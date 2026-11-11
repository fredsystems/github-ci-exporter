//! NixOS channel status, read from `prometheus.nixos.org`.
//!
//! This is the exporter's first non-GitHub origin: a channel-tarball node's
//! upstream (`channels.nixos.org/nixpkgs-unstable`, say) is a NixOS channel,
//! not a GitHub ref, so there is nothing for [`super::upstream`]'s GraphQL
//! query to resolve it against. `prometheus.nixos.org` already tracks every
//! channel's current revision, when it last moved, and whether its `tested`
//! Hydra job is green, so this module fetches and caches that instead of
//! recomputing it -- federating the whole series set would mean polling
//! often enough that Prometheus's 5-minute staleness window never opens,
//! which is not being a good neighbour on an hourly cadence.
//!
//! [`NixosClient`] is deliberately not [`super::super::github::Client`]:
//! same builder settings (rustls, bundled roots, timeouts, a
//! self-identifying `User-Agent`), but no auth header, no `ETag` cache, and
//! none of the GitHub rate-limit machinery, because none of that applies to
//! an anonymous, unauthenticated Prometheus origin.
//!
//! [`apply_tarball_freshness`] is the other half of 3.6: it feeds a
//! channel-tarball node's freshness into the *same* `github_flake_input_*`
//! families [`super::upstream`] already renders, by writing a
//! [`super::upstream::NodeStatus`] directly into its cache through the small
//! seam that module exposes for exactly this -- no new metric families, and
//! no tarball-specific logic added to that module itself.

use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasher,
    time::Duration,
};

use reqwest::{
    StatusCode,
    header::{HeaderMap, HeaderValue, USER_AGENT},
};
use serde::Deserialize;
use tracing::warn;

use super::{
    lock::{NodeKind, TrackedNode},
    upstream::{Freshness, NodeStatus, UpstreamCache},
};
use crate::metrics::{ChannelAttemptLabels, ChannelLabels, ChannelMetrics};

/// `github-ci-exporter` identifies itself here too, even though
/// `prometheus.nixos.org` needs no credential: it is still someone else's
/// infrastructure, and a descriptive `User-Agent` is the minimum courtesy of
/// a client it cannot otherwise push back on.
const UA: &str = concat!("github-ci-exporter/", env!("CARGO_PKG_VERSION"));

/// Backoff before a query's single retry. Short on purpose: this pass must
/// never block the flake pass for long, and a single short pause is enough to
/// ride out a blip without turning a real outage into a slow one.
const RETRY_BACKOFF: Duration = Duration::from_millis(500);

/// A `PromQL` regex alternation matching exactly these channels, for a
/// `channel=~"..."` selector, so `prometheus.nixos.org` is only asked about
/// channels a tracked node references.
///
/// Channel names are `nixos-26.05`-shaped. Anything outside
/// `[A-Za-z0-9._-]` is skipped rather than escaped, so nothing unexpected
/// from a lock file can reach the query; dots are regex-escaped (`\\.`
/// inside the `PromQL` string literal is the regex `\.`).
fn channel_matcher(channels: &[&str]) -> String {
    channels
        .iter()
        .filter(|channel| {
            !channel.is_empty()
                && channel
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
        })
        .map(|channel| channel.replace('.', "\\\\."))
        .collect::<Vec<_>>()
        .join("|")
}

fn query_channel_revision(matcher: &str) -> String {
    format!(r#"channel_revision{{channel=~"{matcher}"}}"#)
}

fn query_channel_update_time(matcher: &str) -> String {
    format!(r#"channel_update_time{{channel=~"{matcher}"}}"#)
}

fn query_tested_failed(matcher: &str) -> String {
    format!(r#"hydra_job_failed{{exported_job=~".*tested",channel=~"{matcher}"}}"#)
}

#[derive(Debug, thiserror::Error)]
pub enum ChannelFetchError {
    #[error("failed to build http client: {0}")]
    Build(#[source] reqwest::Error),
    #[error("http transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("prometheus.nixos.org returned {status} for query `{query}`: {body}")]
    Status {
        status: StatusCode,
        query: String,
        body: String,
    },
    #[error("failed to decode the response for query `{query}`: {source}")]
    Decode {
        query: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("query `{query}` did not report status \"success\" (got \"{status}\")")]
    QueryFailed { query: String, status: String },
}

/// A small, unauthenticated HTTP client for `prometheus.nixos.org`'s
/// instant-query API.
///
/// Shares [`super::super::github::Client`]'s transport hardening -- rustls
/// with Mozilla's bundled roots (so this works in the Nix build sandbox and
/// under a hardened `DynamicUser` unit the same way the GitHub client does)
/// and the same connect/request timeouts -- but none of its machinery that
/// does not apply to a second, anonymous origin: no auth header, no `ETag`
/// cache, no GitHub rate-limit bookkeeping.
#[derive(Debug)]
pub struct NixosClient {
    http: reqwest::Client,
    base_url: String,
}

impl NixosClient {
    /// # Errors
    /// Returns [`ChannelFetchError::Build`] if the underlying HTTP client
    /// cannot be constructed (a TLS backend failure).
    pub fn new(base_url: &str) -> Result<Self, ChannelFetchError> {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(UA));

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            // See `github::client::Client::new`'s identical setup: bundled
            // roots make this independent of the ambient (possibly absent,
            // possibly sandboxed) system trust store.
            .tls_certs_only(
                webpki_root_certs::TLS_SERVER_ROOT_CERTS
                    .iter()
                    .map(|cert| reqwest::Certificate::from_der(cert))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(ChannelFetchError::Build)?,
            )
            .build()
            .map_err(ChannelFetchError::Build)?;

        Ok(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_owned(),
        })
    }

    /// Runs one instant query, retrying once after a short backoff.
    async fn query(&self, promql: &str) -> Result<Vec<Sample>, ChannelFetchError> {
        match self.try_query(promql).await {
            Ok(samples) => Ok(samples),
            Err(first_error) => {
                warn!(query = promql, error = %first_error, "nixos prometheus query failed; retrying once");
                tokio::time::sleep(RETRY_BACKOFF).await;
                self.try_query(promql).await
            }
        }
    }

    async fn try_query(&self, promql: &str) -> Result<Vec<Sample>, ChannelFetchError> {
        let url = format!("{}/api/v1/query", self.base_url);
        let response = self
            .http
            .get(&url)
            .query(&[("query", promql)])
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ChannelFetchError::Status {
                status,
                query: promql.to_owned(),
                body: body.chars().take(300).collect(),
            });
        }

        let body = response.text().await?;
        let parsed: QueryResponse =
            serde_json::from_str(&body).map_err(|source| ChannelFetchError::Decode {
                query: promql.to_owned(),
                source,
            })?;
        if parsed.status != "success" {
            return Err(ChannelFetchError::QueryFailed {
                query: promql.to_owned(),
                status: parsed.status,
            });
        }
        Ok(parsed.data.result)
    }
}

#[derive(Debug, Deserialize)]
struct QueryResponse {
    status: String,
    data: QueryData,
}

#[derive(Debug, Deserialize)]
struct QueryData {
    result: Vec<Sample>,
}

/// One result of a Prometheus instant query: its labels, and its value.
///
/// Only the value *string* is kept -- the sample timestamp Prometheus pairs
/// it with is not meaningful here, since this module reads `channel` /
/// `revision` / `current` / `exported_job` from the label set, and the
/// values this crate cares about (a unix-seconds timestamp, or `"0"`/`"1"`)
/// are exact integers represented as decimal text, not floating point. That
/// is parsed by taking the text ahead of any decimal point rather than
/// through `f64`, so this never needs a lossy float-to-integer cast.
#[derive(Debug, Deserialize)]
struct Sample {
    metric: HashMap<String, String>,
    #[serde(deserialize_with = "value_text")]
    value: String,
}

/// Deserializes a Prometheus API `[timestamp, "value"]` pair, keeping only
/// the value.
fn value_text<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let (_timestamp, value): (serde_json::Value, String) = Deserialize::deserialize(deserializer)?;
    Ok(value)
}

/// Parses a Prometheus sample value representing a unix-seconds timestamp.
///
/// Takes the text ahead of any decimal point, which is exact for the whole
/// numbers these metrics actually hold and avoids a lossy `f64 as i64`
/// conversion entirely.
fn parse_seconds(raw: &str) -> Option<i64> {
    raw.split('.').next()?.parse().ok()
}

/// Whether a NixOS channel is still supported.
///
/// Not a bool: this is read directly off `channel_revision`'s `current`
/// label ("1" or "0"), and naming it after what it means -- rather than
/// carrying the raw label text, or a generic `is_current: bool` -- is what
/// lets `FlakeInputOnEolChannel`-style alerting read as intended at every
/// call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelSupport {
    Current,
    EndOfLife,
}

/// Whether a channel's `tested` Hydra job -- the blocker for every
/// in-channel package -- is currently green.
///
/// `Unknown` is distinct from `Passing`: the `hydra_job_failed{exported_job=~
/// ".*tested"}` query simply has no series for a channel it has never
/// evaluated, and reading silence as "passing" would hide exactly the
/// nixos-unstable-sat-still incident this metric exists to catch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestedState {
    Passing,
    Failing,
    Unknown,
}

/// One channel's status, as of the last successful fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelStatus {
    pub revision: String,
    pub current: ChannelSupport,
    /// `None` when `channel_update_time` had no sample for this channel and
    /// no earlier value for the same revision is known: unknown, never 0
    /// (the epoch would read as "has not moved in decades").
    pub update_time: Option<i64>,
    pub tested: TestedState,
}

/// Cross-cycle state for the channel-status pass.
#[derive(Debug, Default)]
pub struct ChannelCache {
    statuses: HashMap<String, ChannelStatus>,
    /// The tarball statuses [`apply_tarball_freshness`] set on the last pass
    /// whose channel fetch succeeded, keyed by `(repo, path)`. Re-applied
    /// during an outage only to a node whose identity is unchanged, since
    /// [`super::upstream`]'s pass removes every tarball status each time.
    tarball_statuses: HashMap<(String, String), NodeStatus>,
    /// `None` before the first attempt is ever made -- gating
    /// `nixos_channel_status_up`'s absence until then, since no node has
    /// ever referenced a channel is a materially different state from "the
    /// last fetch failed".
    last_fetch_ok: Option<bool>,
}

impl ChannelCache {
    #[must_use]
    pub fn status_for(&self, channel: &str) -> Option<&ChannelStatus> {
        self.statuses.get(channel)
    }

    /// Every channel currently cached, for [`record_metrics`].
    pub fn statuses(&self) -> impl Iterator<Item = (&String, &ChannelStatus)> {
        self.statuses.iter()
    }

    /// Whether the last fetch attempt succeeded; `None` before the first one.
    #[must_use]
    pub const fn last_fetch_ok(&self) -> Option<bool> {
        self.last_fetch_ok
    }
}

/// Runs the three instant queries and caches the channels referenced by any
/// tracked node.
///
/// Does nothing -- not even recording an attempt -- when `channels` is
/// empty: no tracked node anywhere locks a NixOS channel, so there is
/// nothing to ask `prometheus.nixos.org` about, and attempting anyway would
/// keep publishing `nixos_channel_status_up` for a fleet that has never
/// needed it.
///
/// A failure in any of the three queries (transport, a non-success status, a
/// decode failure, or a non-"success" response body) abandons the whole
/// attempt: the previously cached channel data is kept, a warning is logged,
/// and [`ChannelCache::last_fetch_ok`] reports the failure.
pub async fn run_pass<S: BuildHasher + Sync>(
    client: &NixosClient,
    channels: &HashSet<String, S>,
    cache: &mut ChannelCache,
) {
    // Channels no longer referenced by any tracked node are dropped first, so
    // they stop being republished even if this pass's fetch then fails. With
    // none referenced at all, the channel state is reset entirely: there is
    // nothing to report on, including the last fetch's outcome.
    cache
        .statuses
        .retain(|channel, _| channels.contains(channel));
    if channels.is_empty() {
        cache.last_fetch_ok = None;
        cache.tarball_statuses.clear();
        return;
    }

    let mut sorted: Vec<&str> = channels.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    let matcher = channel_matcher(&sorted);
    if matcher.is_empty() {
        warn!("no referenced NixOS channel has a queryable name; skipping the channel fetch");
        return;
    }

    match fetch_all(client, &matcher).await {
        Ok(fetched) => {
            let previous = std::mem::take(&mut cache.statuses);
            cache.statuses = fetched
                .into_iter()
                .filter(|(channel, _)| channels.contains(channel))
                .map(|(channel, mut status)| {
                    // A missing update time keeps the last known one, but only
                    // for the same revision: a new revision's update time is
                    // unknown until reported.
                    if status.update_time.is_none()
                        && let Some(prior) = previous.get(&channel)
                        && prior.revision == status.revision
                    {
                        status.update_time = prior.update_time;
                    }
                    (channel, status)
                })
                .collect();
            cache.last_fetch_ok = Some(true);
        }
        Err(error) => {
            warn!(%error, "failed to fetch NixOS channel status; keeping previously cached values");
            cache.last_fetch_ok = Some(false);
        }
    }
}

/// What is known about one channel while the three queries' results are
/// being combined by [`fetch_all`]. Fields are filled in independently by
/// each query, since a channel can legitimately be absent from one of them
/// (no `tested`-suffixed Hydra job has ever evaluated it, say).
#[derive(Debug, Default)]
struct PartialStatus {
    revision: Option<String>,
    current: Option<ChannelSupport>,
    update_time: Option<i64>,
    has_tested_data: bool,
    any_tested_job_failing: bool,
}

async fn fetch_all(
    client: &NixosClient,
    matcher: &str,
) -> Result<HashMap<String, ChannelStatus>, ChannelFetchError> {
    let revision = client.query(&query_channel_revision(matcher)).await?;
    let update_time = client.query(&query_channel_update_time(matcher)).await?;
    let tested = client.query(&query_tested_failed(matcher)).await?;

    let mut partials: HashMap<String, PartialStatus> = HashMap::new();

    for sample in revision {
        let Some(channel) = sample.metric.get("channel").cloned() else {
            continue;
        };
        let Some(rev) = sample.metric.get("revision").cloned() else {
            continue;
        };
        let current = match sample.metric.get("current").map(String::as_str) {
            Some("1") => ChannelSupport::Current,
            _ => ChannelSupport::EndOfLife,
        };
        let entry = partials.entry(channel).or_default();
        entry.revision = Some(rev);
        entry.current = Some(current);
    }

    for sample in update_time {
        let Some(channel) = sample.metric.get("channel").cloned() else {
            continue;
        };
        let Some(seconds) = parse_seconds(&sample.value) else {
            continue;
        };
        partials.entry(channel).or_default().update_time = Some(seconds);
    }

    for sample in tested {
        let Some(channel) = sample.metric.get("channel").cloned() else {
            continue;
        };
        let entry = partials.entry(channel).or_default();
        entry.has_tested_data = true;
        entry.any_tested_job_failing = entry.any_tested_job_failing || sample.value == "1";
    }

    Ok(partials
        .into_iter()
        .filter_map(|(channel, partial)| {
            let revision = partial.revision?;
            let current = partial.current.unwrap_or(ChannelSupport::EndOfLife);
            let tested = if partial.has_tested_data {
                if partial.any_tested_job_failing {
                    TestedState::Failing
                } else {
                    TestedState::Passing
                }
            } else {
                TestedState::Unknown
            };
            Some((
                channel,
                ChannelStatus {
                    revision,
                    current,
                    update_time: partial.update_time,
                    tested,
                },
            ))
        })
        .collect())
}

/// Feeds every channel-tarball node's freshness into [`super::upstream`]'s
/// own `github_flake_input_*` families.
///
/// Runs after [`super::upstream::run_pass`], which removes every tarball
/// node's status (none of them has a GitHub lookup), so this re-establishes
/// them each pass:
///
/// * When this pass's channel fetch succeeded, each tarball node is compared
///   against its channel's revision. A node whose channel has no data, whose
///   channel has no known update time, or whose own `locked_rev` could not be
///   extracted (see `super::lock`) is left unresolved this pass.
/// * When the fetch failed (an outage), nothing is recomputed: comparing a
///   lock that changed during the outage against the stale cached revision
///   could report an up-to-date node as behind and start a false
///   outdated-since clock. The status from the last successful pass is
///   re-applied only to a node whose identity (kind, locked revision, ...) is
///   unchanged; a changed node stays unresolved until a fetch succeeds.
///
/// `outdated_since` is measured from the node's own locked revision, not the
/// channel's: the channel's revision advances independently of whether this
/// node's lock has moved, and keying the clock to it would restart
/// "how long has this been outdated" every time the channel moves while the
/// node stays exactly as behind as before.
pub fn apply_tarball_freshness<S: BuildHasher>(
    repos: &[String],
    nodes_by_repo: &HashMap<String, Vec<TrackedNode>, S>,
    channels: &mut ChannelCache,
    upstream: &mut UpstreamCache,
    now: i64,
) {
    let fetch_succeeded = channels.last_fetch_ok == Some(true);
    let mut applied = HashMap::new();

    for repo in repos {
        let Some(nodes) = nodes_by_repo.get(repo) else {
            continue;
        };
        for node in nodes {
            if node.kind != NodeKind::ChannelTarball {
                continue;
            }
            let key = (repo.clone(), node.path.clone());

            if !fetch_succeeded {
                if let Some(previous) = channels.tarball_statuses.get(&key)
                    && previous.is_for(node)
                {
                    upstream.set_status(repo, &node.path, previous.clone());
                    applied.insert(key, previous.clone());
                }
                continue;
            }

            let Some(channel_name) = node.channel.as_deref() else {
                continue;
            };
            let Some(channel_status) = channels.status_for(channel_name) else {
                continue;
            };
            let Some(update_time) = channel_status.update_time else {
                continue;
            };
            let Some(locked) = node.locked_rev.as_deref() else {
                continue;
            };

            let freshness = if channel_status.revision.starts_with(locked) {
                Freshness::UpToDate
            } else {
                Freshness::Behind
            };

            let mut status = NodeStatus::for_node(
                node,
                channel_status.revision.clone(),
                None,
                update_time,
                freshness,
            );

            let identity = tarball_first_seen_identity(repo, &node.path, locked);
            match freshness {
                Freshness::Behind => {
                    status.outdated_since = Some(upstream.observe_behind(&identity, now));
                }
                Freshness::UpToDate => upstream.clear_behind(&identity),
                Freshness::Unknown => {}
            }

            upstream.set_status(repo, &node.path, status.clone());
            applied.insert(key, status);
        }
    }

    // Only nodes still tracked (and, during an outage, still identical) carry
    // over, so a removed or changed tarball node never resurfaces later.
    channels.tarball_statuses = applied;
}

/// The identity a channel-tarball node's first-seen entry is stored under:
/// `<repo>|<path>|<locked short revision>`. Keyed on the node's own lock
/// rather than the channel's current revision, so the clock survives the
/// channel moving further ahead while the node stays exactly as behind as it
/// already was, and only resets once the lock itself changes or the node
/// catches up.
fn tarball_first_seen_identity(repo: &str, path: &str, locked_rev: &str) -> String {
    format!("{repo}|{path}|{locked_rev}")
}

/// Publishes every `nixos_channel_*` family for the currently cached state.
///
/// Called every cycle, independent of whether this pass itself ran -- the
/// channel registry is rebuilt from scratch each cycle exactly like the
/// `github`-prefixed one, so cached data not republished here simply
/// disappears.
pub fn record_metrics(metrics: &ChannelMetrics, cache: &ChannelCache) {
    for (channel, status) in cache.statuses() {
        let labels = ChannelLabels {
            channel: channel.clone(),
        };
        if let Some(update_time) = status.update_time {
            metrics
                .update_timestamp
                .get_or_create(&labels)
                .set(update_time);
        }
        metrics
            .current
            .get_or_create(&labels)
            .set(match status.current {
                ChannelSupport::Current => 1,
                ChannelSupport::EndOfLife => 0,
            });
        match status.tested {
            TestedState::Passing => {
                metrics.tested_failed.get_or_create(&labels).set(0);
            }
            TestedState::Failing => {
                metrics.tested_failed.get_or_create(&labels).set(1);
            }
            TestedState::Unknown => {}
        }
    }

    if let Some(ok) = cache.last_fetch_ok() {
        metrics
            .status_up
            .get_or_create(&ChannelAttemptLabels::default())
            .set(i64::from(ok));
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "panicking is how a test reports failure"
)]
mod tests {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::*;
    use crate::{flake::lock::TrackedNode, metrics::Publisher};

    fn client_for(server: &MockServer) -> NixosClient {
        NixosClient::new(&server.uri()).expect("client builds")
    }

    // Both helpers take their JSON by value for a readable call site --
    // `mock_query(QUERY, json!({...}))` inline, rather than naming a binding
    // just to borrow it.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "owned value makes every call site a one-line inline json!(...) literal"
    )]
    fn mock_query(query: &str, result: serde_json::Value) -> wiremock::Mock {
        Mock::given(method("GET"))
            .and(path("/api/v1/query"))
            .and(query_param("query", query))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "success",
                "data": { "resultType": "vector", "result": result },
            })))
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "owned value makes every call site a one-line inline json!(...) literal"
    )]
    fn sample(labels: serde_json::Value, value: &str) -> serde_json::Value {
        serde_json::json!({ "metric": labels, "value": [1_700_000_000, value] })
    }

    async fn mount_all(
        server: &MockServer,
        revision: serde_json::Value,
        update_time: serde_json::Value,
        tested: serde_json::Value,
    ) {
        let matcher = channel_matcher(&["nixos-unstable"]);
        mock_query(&query_channel_revision(&matcher), revision)
            .mount(server)
            .await;
        mock_query(&query_channel_update_time(&matcher), update_time)
            .mount(server)
            .await;
        mock_query(&query_tested_failed(&matcher), tested)
            .mount(server)
            .await;
    }

    fn one_channel() -> HashSet<String> {
        HashSet::from(["nixos-unstable".to_owned()])
    }

    #[tokio::test]
    async fn parses_all_three_queries_ignoring_extra_labels() {
        let server = MockServer::start().await;
        mount_all(
            &server,
            serde_json::json!([sample(
                serde_json::json!({
                    "__name__": "channel_revision",
                    "channel": "nixos-unstable",
                    "revision": "abcdef0123456789",
                    "current": "1",
                    "extra_label": "ignored",
                }),
                "1"
            )]),
            serde_json::json!([sample(
                serde_json::json!({ "channel": "nixos-unstable" }),
                "1700000100"
            )]),
            serde_json::json!([sample(
                serde_json::json!({ "channel": "nixos-unstable", "exported_job": "tested" }),
                "0"
            )]),
        )
        .await;

        let client = client_for(&server);
        let mut cache = ChannelCache::default();
        run_pass(&client, &one_channel(), &mut cache).await;

        let status = cache.status_for("nixos-unstable").expect("cached");
        assert_eq!(status.revision, "abcdef0123456789");
        assert_eq!(status.current, ChannelSupport::Current);
        assert_eq!(status.update_time, Some(1_700_000_100));
        assert_eq!(status.tested, TestedState::Passing);
        assert_eq!(cache.last_fetch_ok(), Some(true));
    }

    #[tokio::test]
    async fn channels_not_referenced_by_any_tracked_node_are_filtered_out() {
        let server = MockServer::start().await;
        mount_all(
            &server,
            serde_json::json!([
                sample(
                    serde_json::json!({ "channel": "nixos-unstable", "revision": "a", "current": "1" }),
                    "1"
                ),
                sample(
                    serde_json::json!({ "channel": "nixos-26.05", "revision": "b", "current": "1" }),
                    "1"
                ),
            ]),
            serde_json::json!([]),
            serde_json::json!([]),
        )
        .await;

        let client = client_for(&server);
        let mut cache = ChannelCache::default();
        run_pass(&client, &one_channel(), &mut cache).await;

        assert!(cache.status_for("nixos-unstable").is_some());
        assert!(
            cache.status_for("nixos-26.05").is_none(),
            "a channel no tracked node references must not be cached"
        );
    }

    #[tokio::test]
    async fn current_zero_is_end_of_life() {
        let server = MockServer::start().await;
        mount_all(
            &server,
            serde_json::json!([sample(
                serde_json::json!({ "channel": "nixos-unstable", "revision": "a", "current": "0" }),
                "1"
            )]),
            serde_json::json!([]),
            serde_json::json!([]),
        )
        .await;

        let client = client_for(&server);
        let mut cache = ChannelCache::default();
        run_pass(&client, &one_channel(), &mut cache).await;

        assert_eq!(
            cache.status_for("nixos-unstable").expect("cached").current,
            ChannelSupport::EndOfLife
        );
    }

    #[tokio::test]
    async fn any_failing_tested_job_marks_the_channel_failing() {
        let server = MockServer::start().await;
        mount_all(
            &server,
            serde_json::json!([sample(
                serde_json::json!({ "channel": "nixos-unstable", "revision": "a", "current": "1" }),
                "1"
            )]),
            serde_json::json!([]),
            serde_json::json!([
                sample(
                    serde_json::json!({ "channel": "nixos-unstable", "exported_job": "tested" }),
                    "0"
                ),
                sample(
                    serde_json::json!({ "channel": "nixos-unstable", "exported_job": "nixpkgs-tested" }),
                    "1"
                ),
            ]),
        )
        .await;

        let client = client_for(&server);
        let mut cache = ChannelCache::default();
        run_pass(&client, &one_channel(), &mut cache).await;

        assert_eq!(
            cache.status_for("nixos-unstable").expect("cached").tested,
            TestedState::Failing,
            "any failing *tested job must mark the channel failing"
        );
    }

    #[tokio::test]
    async fn no_tested_data_is_unknown_not_passing() {
        let server = MockServer::start().await;
        mount_all(
            &server,
            serde_json::json!([sample(
                serde_json::json!({ "channel": "nixos-unstable", "revision": "a", "current": "1" }),
                "1"
            )]),
            serde_json::json!([]),
            serde_json::json!([]),
        )
        .await;

        let client = client_for(&server);
        let mut cache = ChannelCache::default();
        run_pass(&client, &one_channel(), &mut cache).await;

        assert_eq!(
            cache.status_for("nixos-unstable").expect("cached").tested,
            TestedState::Unknown
        );
    }

    #[tokio::test]
    async fn an_outage_keeps_cached_values_and_reports_the_failure() {
        let server = MockServer::start().await;
        mount_all(
            &server,
            serde_json::json!([sample(
                serde_json::json!({ "channel": "nixos-unstable", "revision": "a", "current": "1" }),
                "1"
            )]),
            serde_json::json!([]),
            serde_json::json!([]),
        )
        .await;

        let client = client_for(&server);
        let mut cache = ChannelCache::default();
        run_pass(&client, &one_channel(), &mut cache).await;
        assert_eq!(cache.last_fetch_ok(), Some(true));

        // Tear down and replace with a failing server, retried once per the
        // single-retry policy, both attempts failing.
        drop(server);
        let broken = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/query"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&broken)
            .await;
        let broken_client = client_for(&broken);

        run_pass(&broken_client, &one_channel(), &mut cache).await;

        assert_eq!(cache.last_fetch_ok(), Some(false));
        assert_eq!(
            cache.status_for("nixos-unstable").expect("cached").revision,
            "a",
            "a failed fetch must keep the previously cached revision"
        );

        let (channel_metrics, channel_registry) = ChannelMetrics::new();
        record_metrics(&channel_metrics, &cache);
        let (gh_metrics, gh_registry) = crate::metrics::Metrics::new();
        let rendered =
            Publisher::new_with_channels(gh_metrics, gh_registry, channel_registry).render();
        assert!(rendered.contains("nixos_channel_status_up"));
    }

    #[tokio::test]
    async fn no_channel_fetch_when_no_node_has_a_channel() {
        let server = MockServer::start().await;
        // No mocks at all: any request would panic wiremock's "no matching
        // mock" path, which is exactly how this asserts nothing was sent.
        let client = client_for(&server);
        let mut cache = ChannelCache::default();

        run_pass(&client, &HashSet::new(), &mut cache).await;

        assert_eq!(
            cache.last_fetch_ok(),
            None,
            "no node ever referenced a channel, so no attempt -- and therefore no status -- is recorded"
        );
    }

    #[test]
    fn a_tarball_node_behind_its_channel_gets_a_commits_behind_free_status() {
        let mut channels = ChannelCache {
            last_fetch_ok: Some(true),
            ..ChannelCache::default()
        };
        channels.statuses.insert(
            "nixos-unstable".to_owned(),
            ChannelStatus {
                revision: "e62746b0b472753ef019baceab6f1e2e52014980".to_owned(),
                current: ChannelSupport::Current,
                update_time: Some(1_700_000_200),
                tested: TestedState::Passing,
            },
        );

        let node = tarball_node("catppuccin/nixpkgs", "nixos-unstable", "oldrev000000");
        let mut upstream = UpstreamCache::default();
        apply_tarball_freshness(
            &["fredsystems/nixos".to_owned()],
            &nodes_by_repo(node.clone()),
            &mut channels,
            &mut upstream,
            1_700_000_300,
        );

        let status = upstream
            .status_for("fredsystems/nixos", &node)
            .expect("status recorded");
        assert_eq!(status.freshness, Freshness::Behind);
        assert_eq!(status.outdated_since, Some(1_700_000_300));
        assert_eq!(status.upstream_committed_at, 1_700_000_200);
    }

    #[test]
    fn a_tarball_node_matching_the_channel_prefix_is_up_to_date() {
        let mut channels = ChannelCache {
            last_fetch_ok: Some(true),
            ..ChannelCache::default()
        };
        channels.statuses.insert(
            "nixos-unstable".to_owned(),
            ChannelStatus {
                revision: "e62746b0b472753ef019baceab6f1e2e52014980".to_owned(),
                current: ChannelSupport::Current,
                update_time: Some(1_700_000_200),
                tested: TestedState::Passing,
            },
        );

        let node = tarball_node("catppuccin/nixpkgs", "nixos-unstable", "e62746b0b472");
        let mut upstream = UpstreamCache::default();
        apply_tarball_freshness(
            &["fredsystems/nixos".to_owned()],
            &nodes_by_repo(node.clone()),
            &mut channels,
            &mut upstream,
            1_700_000_300,
        );

        let status = upstream
            .status_for("fredsystems/nixos", &node)
            .expect("status recorded");
        assert_eq!(status.freshness, Freshness::UpToDate);
        assert_eq!(status.outdated_since, None);
    }

    #[test]
    fn outdated_since_is_stable_across_channel_revisions_while_the_lock_is_unchanged() {
        // The regression this exists to prevent: using the channel's own
        // revision as the first-seen identity would restart the clock every
        // time the channel advances, even though the node is exactly as
        // behind as it was a moment ago.
        let node = tarball_node("catppuccin/nixpkgs", "nixos-unstable", "oldrev000000");
        let mut upstream = UpstreamCache::default();

        let mut channels = ChannelCache {
            last_fetch_ok: Some(true),
            ..ChannelCache::default()
        };
        channels.statuses.insert(
            "nixos-unstable".to_owned(),
            ChannelStatus {
                revision: "revisionone00000000000000000000000000000".to_owned(),
                current: ChannelSupport::Current,
                update_time: Some(1_700_000_200),
                tested: TestedState::Passing,
            },
        );
        apply_tarball_freshness(
            &["fredsystems/nixos".to_owned()],
            &nodes_by_repo(node.clone()),
            &mut channels,
            &mut upstream,
            1_700_000_300,
        );
        let first = upstream
            .status_for("fredsystems/nixos", &node)
            .expect("status recorded")
            .outdated_since;
        assert_eq!(first, Some(1_700_000_300));

        // The channel advances to a new revision; the node's lock has not
        // changed, so it is still behind -- just as behind as before.
        channels.statuses.insert(
            "nixos-unstable".to_owned(),
            ChannelStatus {
                revision: "revisiontwo00000000000000000000000000000".to_owned(),
                current: ChannelSupport::Current,
                update_time: Some(1_700_000_900),
                tested: TestedState::Passing,
            },
        );
        apply_tarball_freshness(
            &["fredsystems/nixos".to_owned()],
            &nodes_by_repo(node.clone()),
            &mut channels,
            &mut upstream,
            1_700_001_000,
        );
        let second = upstream
            .status_for("fredsystems/nixos", &node)
            .expect("status recorded")
            .outdated_since;
        assert_eq!(
            second, first,
            "the clock must not reset just because the channel moved while the node stayed behind"
        );
    }

    #[test]
    fn catching_up_moves_the_lock_and_reads_up_to_date() {
        // Catching up happens by `nix flake update` moving the *lock* to a
        // new revision, not by the channel somehow un-advancing back to the
        // old one -- so this, unlike a tag-pinned node settling on the exact
        // release it was already compared against, always changes the
        // first-seen identity. The old identity's entry is deliberately left
        // behind rather than hunted down and cleared: it is never consulted
        // again once nothing is locked to it, and tag-pinned nodes already
        // accept the identical trade-off when a newer release supersedes the
        // one they were compared against (see `super::upstream`'s own
        // documentation on that).
        let node = tarball_node("catppuccin/nixpkgs", "nixos-unstable", "oldrev000000");
        let mut upstream = UpstreamCache::default();
        let mut channels = ChannelCache {
            last_fetch_ok: Some(true),
            ..ChannelCache::default()
        };
        channels.statuses.insert(
            "nixos-unstable".to_owned(),
            ChannelStatus {
                revision: "revisionone00000000000000000000000000000".to_owned(),
                current: ChannelSupport::Current,
                update_time: Some(1_700_000_200),
                tested: TestedState::Passing,
            },
        );
        apply_tarball_freshness(
            &["fredsystems/nixos".to_owned()],
            &nodes_by_repo(node),
            &mut channels,
            &mut upstream,
            1_700_000_300,
        );
        let locked_node = tarball_node("catppuccin/nixpkgs", "nixos-unstable", "oldrev000000");
        assert_eq!(
            upstream
                .status_for("fredsystems/nixos", &locked_node)
                .expect("status recorded")
                .freshness,
            Freshness::Behind
        );

        // `nix flake update` moves the lock to the channel's current short
        // revision.
        let caught_up = tarball_node("catppuccin/nixpkgs", "nixos-unstable", "revisionone0");
        apply_tarball_freshness(
            &["fredsystems/nixos".to_owned()],
            &nodes_by_repo(caught_up.clone()),
            &mut channels,
            &mut upstream,
            1_700_000_400,
        );
        let status = upstream
            .status_for("fredsystems/nixos", &caught_up)
            .expect("status recorded");
        assert_eq!(status.freshness, Freshness::UpToDate);
        assert_eq!(status.outdated_since, None);
    }

    #[test]
    fn an_unresolvable_channel_leaves_the_tarball_node_unpublished() {
        let node = tarball_node("catppuccin/nixpkgs", "nixos-unstable", "oldrev000000");
        let mut upstream = UpstreamCache::default();
        // A successful fetch that simply has no data for this channel.
        let mut channels = ChannelCache {
            last_fetch_ok: Some(true),
            ..ChannelCache::default()
        };

        apply_tarball_freshness(
            &["fredsystems/nixos".to_owned()],
            &nodes_by_repo(node.clone()),
            &mut channels,
            &mut upstream,
            1_700_000_300,
        );

        assert!(
            upstream.status_for("fredsystems/nixos", &node).is_none(),
            "no cached channel data this pass must publish no series, not a guess"
        );
    }

    #[test]
    fn queries_select_only_the_referenced_channels_with_dots_escaped() {
        let matcher = channel_matcher(&["nixos-26.05", "nixos-unstable", "bad\"name"]);
        assert_eq!(
            query_channel_revision(&matcher),
            r#"channel_revision{channel=~"nixos-26\\.05|nixos-unstable"}"#
        );
        assert_eq!(
            query_tested_failed(&matcher),
            r#"hydra_job_failed{exported_job=~".*tested",channel=~"nixos-26\\.05|nixos-unstable"}"#
        );
    }

    fn cached_status(revision: &str, update_time: Option<i64>) -> ChannelStatus {
        ChannelStatus {
            revision: revision.to_owned(),
            current: ChannelSupport::Current,
            update_time,
            tested: TestedState::Passing,
        }
    }

    #[tokio::test]
    async fn no_referenced_channel_resets_the_channel_state() {
        let server = MockServer::start().await;
        let client = client_for(&server);
        let mut cache = ChannelCache {
            last_fetch_ok: Some(true),
            ..ChannelCache::default()
        };
        cache
            .statuses
            .insert("nixos-unstable".to_owned(), cached_status("abc", Some(1)));

        run_pass(&client, &HashSet::new(), &mut cache).await;

        assert_eq!(cache.statuses().count(), 0);
        assert_eq!(cache.last_fetch_ok(), None);
    }

    #[tokio::test]
    async fn an_unreferenced_channel_is_pruned_even_when_the_fetch_fails() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let client = client_for(&server);
        let mut cache = ChannelCache::default();
        cache
            .statuses
            .insert("nixos-unstable".to_owned(), cached_status("abc", Some(1)));
        cache
            .statuses
            .insert("nixos-25.11".to_owned(), cached_status("def", Some(1)));

        run_pass(&client, &one_channel(), &mut cache).await;

        assert!(cache.status_for("nixos-unstable").is_some());
        assert!(cache.status_for("nixos-25.11").is_none());
        assert_eq!(cache.last_fetch_ok(), Some(false));
    }

    #[tokio::test]
    async fn a_missing_update_time_is_unknown_or_kept_for_the_same_revision() {
        let revision = serde_json::json!([sample(
            serde_json::json!({ "channel": "nixos-unstable", "revision": "abc", "current": "1" }),
            "1"
        )]);
        let tested = serde_json::json!([]);

        // No earlier value: unknown, not 0, and no timestamp series.
        let server = MockServer::start().await;
        mount_all(
            &server,
            revision.clone(),
            serde_json::json!([]),
            tested.clone(),
        )
        .await;
        let client = client_for(&server);
        let mut cache = ChannelCache::default();
        run_pass(&client, &one_channel(), &mut cache).await;
        assert_eq!(
            cache
                .status_for("nixos-unstable")
                .expect("cached")
                .update_time,
            None
        );
        let (channel_metrics, channel_registry) = ChannelMetrics::new();
        record_metrics(&channel_metrics, &cache);
        let (gh_metrics, gh_registry) = crate::metrics::Metrics::new();
        let rendered =
            Publisher::new_with_channels(gh_metrics, gh_registry, channel_registry).render();
        assert!(!rendered.contains("nixos_channel_update_timestamp_seconds{"));

        // An earlier value for the same revision is kept.
        cache
            .statuses
            .insert("nixos-unstable".to_owned(), cached_status("abc", Some(42)));
        run_pass(&client, &one_channel(), &mut cache).await;
        assert_eq!(
            cache
                .status_for("nixos-unstable")
                .expect("cached")
                .update_time,
            Some(42)
        );
    }

    #[test]
    fn an_outage_never_compares_a_changed_lock_against_the_cached_revision() {
        let mut channels = ChannelCache {
            last_fetch_ok: Some(true),
            ..ChannelCache::default()
        };
        channels.statuses.insert(
            "nixos-unstable".to_owned(),
            cached_status(
                "aaaaaaaaaaaa000000000000000000000000000",
                Some(1_700_000_200),
            ),
        );
        let repos = ["fredsystems/nixos".to_owned()];
        let mut upstream = UpstreamCache::default();

        // Pass one, fetch fine: lock A matches channel A.
        let lock_a = tarball_node("catppuccin/nixpkgs", "nixos-unstable", "aaaaaaaaaaaa");
        apply_tarball_freshness(
            &repos,
            &nodes_by_repo(lock_a.clone()),
            &mut channels,
            &mut upstream,
            1,
        );

        // Pass two, outage, lock unchanged: the previous status is kept.
        channels.last_fetch_ok = Some(false);
        let mut upstream = UpstreamCache::default();
        apply_tarball_freshness(
            &repos,
            &nodes_by_repo(lock_a.clone()),
            &mut channels,
            &mut upstream,
            2,
        );
        assert_eq!(
            upstream
                .status_for("fredsystems/nixos", &lock_a)
                .expect("kept")
                .freshness,
            Freshness::UpToDate
        );

        // Pass three, outage continues, lock moved to B: unresolved, not a
        // false Behind against the stale cached revision.
        let lock_b = tarball_node("catppuccin/nixpkgs", "nixos-unstable", "bbbbbbbbbbbb");
        let mut upstream = UpstreamCache::default();
        apply_tarball_freshness(
            &repos,
            &nodes_by_repo(lock_b.clone()),
            &mut channels,
            &mut upstream,
            3,
        );
        assert!(upstream.status_for("fredsystems/nixos", &lock_b).is_none());
    }

    fn tarball_node(path: &str, channel: &str, locked_rev: &str) -> TrackedNode {
        let input = path.split('/').next().unwrap_or(path).to_owned();
        TrackedNode {
            input,
            path: path.to_owned(),
            kind: NodeKind::ChannelTarball,
            reference: None,
            channel: Some(channel.to_owned()),
            locked_rev: Some(locked_rev.to_owned()),
            locked_last_modified: Some(1_700_000_000),
            owner: None,
            repo: None,
        }
    }

    fn nodes_by_repo(node: TrackedNode) -> HashMap<String, Vec<TrackedNode>> {
        HashMap::from([("fredsystems/nixos".to_owned(), vec![node])])
    }
}
