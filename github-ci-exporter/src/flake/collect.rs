//! Orchestrates the whole flake pass.
//!
//! Fetches each configured repository's `flake.lock`, then hands the
//! resulting tracked node set to [`super::upstream`] (upstream heads,
//! freshness, update PRs), [`super::channels`] (NixOS channel status, and a
//! channel-tarball node's freshness against it), and [`super::status`] (the
//! flake-watch Action's impact status file), and republishes everything
//! cached every cycle regardless of whether the pass itself ran this one.

use std::{
    collections::{HashMap, HashSet},
    path::Path,
    time::{Duration, Instant},
};

use reqwest::StatusCode;
use serde::Deserialize;
use tracing::{info, warn};

use super::{
    channels::{self, ChannelCache, NixosClient},
    lock::{self, TrackedNode},
    status::{self, StatusCache},
    upstream::{self, UpstreamCache},
};
use crate::{
    github::{Client, ClientError, rest},
    metrics::{FlakeInputLabels, Metrics},
};

/// Cross-cycle state for the flake-lock pass: the most recently observed
/// tracked node set per repository, and when the pass last ran.
///
/// Mirrors [`crate::collector::WorkflowCache`]: held across cycles so a
/// repository's nodes survive a cycle in which the pass does not run (the
/// interval has not elapsed) or fails (a fetch error), and so every cycle can
/// republish them regardless -- the metrics registry is rebuilt from scratch
/// each cycle, so anything not republished simply disappears.
#[derive(Debug, Default)]
pub struct FlakeCache {
    /// `owner/name` -> tracked nodes, as of the last successful fetch.
    nodes: HashMap<String, Vec<TrackedNode>>,
    /// When the flake pass last ran; `None` before the first one.
    last_run: Option<Instant>,
    /// Cross-cycle state for upstream heads, update PRs, and freshness -- see
    /// [`upstream`].
    upstream: UpstreamCache,
    /// Cross-cycle state for the flake-watch Action's impact status file --
    /// see [`status`].
    status: StatusCache,
    /// Cross-cycle state for NixOS channel status -- see [`channels`].
    channels: ChannelCache,
}

impl FlakeCache {
    /// The currently cached NixOS channel status, for
    /// [`channels::record_metrics`].
    #[must_use]
    pub const fn channels(&self) -> &ChannelCache {
        &self.channels
    }

    /// Every channel referenced by any currently tracked node, across every
    /// repository -- what [`channels::run_pass`] fetches, and the only
    /// channels it keeps.
    fn referenced_channels(&self) -> HashSet<String> {
        self.nodes
            .values()
            .flatten()
            .filter_map(|node| node.channel.clone())
            .collect()
    }

    /// Whether the flake pass is due: it has never run, or `interval` has
    /// elapsed since it last did.
    #[must_use]
    pub fn should_run(&self, interval: Duration) -> bool {
        self.last_run.is_none_or(|at| at.elapsed() >= interval)
    }

    /// The tracked nodes currently cached for `repo`, if any.
    #[must_use]
    pub fn nodes_for(&self, repo: &str) -> &[TrackedNode] {
        self.nodes.get(repo).map_or(&[], Vec::as_slice)
    }

    fn set_nodes(&mut self, repo: &str, nodes: Vec<TrackedNode>) {
        self.nodes.insert(repo.to_owned(), nodes);
    }

    fn clear_nodes(&mut self, repo: &str) {
        self.nodes.remove(repo);
    }

    fn mark_ran(&mut self) {
        self.last_run = Some(Instant::now());
    }

    /// Requests a due pass should budget for, estimated from the *previous*
    /// pass's observed state -- this has to be knowable before any of this
    /// cycle's own fetching happens, since it feeds the pre-flight check that
    /// decides whether the cycle runs at all.
    ///
    /// Returns `(core, graphql_batches)`. The lock fetch itself costs one
    /// core request per repository; `upstream` folds in its own compare and
    /// batch estimate, and the status-file fetch costs one core request per
    /// repository too.
    #[must_use]
    pub fn estimate_requests(&self, repos: &[String]) -> (u64, u64) {
        let lock_cost = u64::try_from(repos.len()).unwrap_or(u64::MAX);
        let status_cost = u64::try_from(repos.len()).unwrap_or(u64::MAX);
        let (upstream_core, upstream_graphql) = self.upstream.estimate_requests(repos, &self.nodes);
        (
            lock_cost
                .saturating_add(upstream_core)
                .saturating_add(status_cost),
            upstream_graphql,
        )
    }
}

/// Runs the flake pass for every repository in `repos`.
///
/// Fetches `flake.lock`, then resolves every tracked node's upstream and
/// open update PR, fetches NixOS channel status for the channels any
/// tracked node references and feeds a channel-tarball node's freshness
/// from it, then reads the flake-watch Action's impact status file.
///
/// A fetch failure for one repository is logged and its previous tracked
/// nodes are kept, so one broken repository cannot blind the exporter to the
/// others -- the same contract [`crate::collector::collect`] applies to every
/// other per-repository fetch. A repository with no `flake.lock` (a 404) is
/// not a failure: its cache entry is cleared, so nothing is published for it,
/// and the absence is logged once per pass at `info`.
///
/// The pass is marked as run -- which resets [`FlakeCache::should_run`]'s
/// interval -- only if `repos` is empty or at least one repository's
/// `flake.lock` fetch actually produced [`FetchOutcome::Fetched`] or
/// [`FetchOutcome::NotFound`]. If every repository's lock fetch errored (a
/// transport failure, or the budget check inside [`Client::get_cached_fallible`]
/// refusing the request), marking the pass as run regardless would start the
/// next `flake_interval`-long wait from a pass that observed nothing at all,
/// silently extending an outage into a much longer one. The upstream,
/// channel, and status stages keep their own previous values on failure
/// independently of this and do not affect whether the pass is marked as run.
///
/// The upstream, channel, and status stages run against this cycle's
/// freshly-fetched node set, so a repository whose `flake.lock` fetch failed
/// this cycle is still resolved against whatever nodes the previous cycle
/// observed for it.
pub async fn run_pass(
    client: &Client,
    nixos_client: &NixosClient,
    repos: &[String],
    state_dir: &Path,
    cache: &mut FlakeCache,
) {
    let mut any_observed = false;
    for repo in repos {
        match fetch_tracked_nodes(client, repo).await {
            Ok(FetchOutcome::Fetched(nodes)) => {
                cache.set_nodes(repo, nodes);
                any_observed = true;
            }
            Ok(FetchOutcome::NotFound) => {
                info!(
                    repo,
                    "no flake.lock on the default branch; nothing to track"
                );
                cache.clear_nodes(repo);
                any_observed = true;
            }
            Err(error) => {
                warn!(
                    repo,
                    %error,
                    "failed to fetch flake.lock; keeping the previously observed tracked nodes"
                );
            }
        }
    }

    let now = chrono::Utc::now().timestamp();
    upstream::run_pass(
        client,
        repos,
        &cache.nodes,
        &mut cache.upstream,
        state_dir,
        now,
    )
    .await;

    channels::run_pass(
        nixos_client,
        &cache.referenced_channels(),
        &mut cache.channels,
    )
    .await;
    channels::apply_tarball_freshness(
        repos,
        &cache.nodes,
        &cache.channels,
        &mut cache.upstream,
        now,
    );

    status::run_pass(client, repos, &mut cache.status).await;

    if repos.is_empty() || any_observed {
        cache.mark_ran();
    }
}

/// Publishes the locked-timestamp gauge for every node currently cached for
/// `repo`.
///
/// Called every cycle, independent of whether the flake pass itself ran this
/// cycle: the metrics registry is rebuilt from scratch each cycle, so cached
/// data that is not republished here simply vanishes between hourly passes.
pub fn record_metrics(metrics: &Metrics, repo: &str, nodes: &[TrackedNode]) {
    for node in nodes {
        let Some(locked_at) = node.locked_last_modified else {
            continue;
        };
        metrics
            .flake_input_locked_timestamp
            .get_or_create(&FlakeInputLabels {
                repo: repo.to_owned(),
                input: node.input.clone(),
                path: node.path.clone(),
                r#ref: node.reference.clone().unwrap_or_default(),
                channel: node.channel.clone().unwrap_or_default(),
            })
            .set(locked_at);
    }
}

/// Publishes every flake-input metric family for `repo`'s currently cached
/// state: the lock-derived timestamp, upstream freshness, and impact.
///
/// Called every cycle, independent of whether the flake pass itself ran this
/// cycle -- see [`record_metrics`]'s own documentation for why.
pub fn record_all_metrics(metrics: &Metrics, repo: &str, cache: &FlakeCache) {
    let nodes = cache.nodes_for(repo);
    record_metrics(metrics, repo, nodes);
    upstream::record_metrics(metrics, repo, nodes, &cache.upstream);
    status::record_metrics(metrics, repo, nodes, &cache.status);
}

enum FetchOutcome {
    Fetched(Vec<TrackedNode>),
    NotFound,
}

/// Fetches `repo`'s `flake.lock` from its default branch through the `ETag`
/// cache and parses it into the tracked node set.
///
/// Uses the same Contents API path and `ETag` machinery as
/// `github::rest::fetch_workflow_definition`: no `ref` query parameter, so
/// GitHub resolves the default branch itself, and a `304` is free. The cache
/// stores the parsed [`TrackedNode`] projection rather than the raw lock,
/// following the same rule every other cached endpoint in this crate does --
/// see `github::client`'s module documentation.
///
/// Fetched through [`Client::get_cached_fallible`] rather than
/// [`Client::get_cached`]: unlike a workflow file's crons and triggers, an
/// empty tracked-node set is not a safe default for an unparsable
/// `flake.lock` -- see [`project_flake_lock`]'s documentation.
///
/// No existing `CACHE_FORMAT_VERSION` bump is needed for this: the cache is a
/// flat `url -> (etag, body)` map with no shared schema across endpoints, and
/// this is a brand new URL with a brand new projection type. A version bump
/// is for when an *existing* cached projection's shape or meaning changes out
/// from under entries already on disk, which does not apply to introducing a
/// new one.
async fn fetch_tracked_nodes(client: &Client, repo: &str) -> Result<FetchOutcome, ClientError> {
    let path = format!("/repos/{repo}/contents/flake.lock");
    match client.get_cached_fallible(&path, project_flake_lock).await {
        Ok((nodes, _outcome)) => Ok(FetchOutcome::Fetched(nodes)),
        Err(ClientError::Status { status, .. }) if status == StatusCode::NOT_FOUND => {
            Ok(FetchOutcome::NotFound)
        }
        Err(error) => Err(error),
    }
}

#[derive(Debug, Deserialize)]
struct ContentResponse {
    content: String,
    encoding: String,
}

/// Why [`project_flake_lock`] could not turn a Contents API response into a
/// tracked node set.
#[derive(Debug, thiserror::Error)]
enum ProjectionError {
    #[error("flake.lock contents response used an unexpected encoding: {0}")]
    UnexpectedEncoding(String),
    #[error(transparent)]
    Lock(#[from] lock::LockError),
}

/// Decodes and parses a Contents API response into the tracked node set.
///
/// A failure here -- an unexpected encoding, or anything [`lock::tracked_nodes`]
/// rejects -- is returned as an error rather than degrading to an empty set.
/// Unlike `github::rest::fetch_workflow_definition`'s lenient default for an
/// unparsable workflow file, an empty *result* is not a safe stand-in for a
/// failed *parse* here: it is handed straight to [`crate::collector::collect`]
/// through [`Client::get_cached_fallible`], which does not cache a rejected
/// projection, so the caller's existing failed-fetch handling in [`run_pass`]
/// keeps the previously observed tracked nodes instead -- an unparsable lock
/// must not be able to replace good cached data with nothing, nor get stuck
/// replaying an empty set from under a still-valid `ETag`.
fn project_flake_lock(response: ContentResponse) -> Result<Vec<TrackedNode>, ProjectionError> {
    // Destructured rather than accessed through `response.field`, so this
    // function genuinely consumes its argument instead of only borrowing it.
    let ContentResponse { content, encoding } = response;
    if encoding != "base64" {
        return Err(ProjectionError::UnexpectedEncoding(encoding));
    }
    let raw = rest::decode_base64(&content);
    Ok(lock::tracked_nodes(&raw)?)
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
        matchers::{method, path},
    };

    use super::*;
    use crate::metrics::Metrics;

    /// Base64-encodes `content` the way GitHub's Contents API does, so tests
    /// can build a response without taking a `base64` crate dependency just
    /// for test fixtures -- this crate's only base64 handling is the decoder
    /// these tests exercise.
    fn github_base64(content: &str) -> String {
        const TABLE: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let bytes = content.as_bytes();
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b0 = chunk[0];
            let b1 = chunk.get(1).copied();
            let b2 = chunk.get(2).copied();
            out.push(TABLE[usize::from(b0 >> 2)] as char);
            out.push(
                TABLE[usize::from(((b0 & 0b0000_0011) << 4) | (b1.unwrap_or(0) >> 4))] as char,
            );
            out.push(b1.map_or('=', |b1| {
                TABLE[usize::from(((b1 & 0b0000_1111) << 2) | (b2.unwrap_or(0) >> 6))] as char
            }));
            out.push(b2.map_or('=', |b2| TABLE[usize::from(b2 & 0b0011_1111)] as char));
        }
        out
    }

    fn client_for(server: &MockServer) -> Client {
        Client::new("t", &server.uri(), &format!("{}/graphql", server.uri()))
            .expect("client builds")
    }

    fn nixos_client_for(server: &MockServer) -> channels::NixosClient {
        channels::NixosClient::new(&server.uri()).expect("nixos client builds")
    }

    /// The `nixpkgs` channel-tarball node currently tracked for `o/r`, as
    /// `cache.upstream.status_for` needs it to look up a tarball node's
    /// status by identity rather than by path alone.
    fn tarball_node(cache: &FlakeCache) -> TrackedNode {
        cache
            .nodes_for("o/r")
            .iter()
            .find(|node| node.path == "nixpkgs")
            .expect("the tarball node is tracked")
            .clone()
    }

    /// `sample_lock`'s `nixpkgs` node tracks `nixos-unstable`, so every test
    /// below that runs the flake pass against it triggers a NixOS channel
    /// fetch (see `FlakeCache::referenced_channels`). This stubs all three
    /// queries with an empty-but-successful result, which is all these
    /// tests need: none of them assert on channel status.
    async fn mock_empty_channel_queries(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/api/v1/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "success",
                "data": { "resultType": "vector", "result": [] },
            })))
            .mount(server)
            .await;
    }

    /// A minimal, valid `flake.lock` with one root input, `nixpkgs`, locked to
    /// a branch.
    fn sample_lock() -> String {
        serde_json::json!({
            "version": 7,
            "root": "root",
            "nodes": {
                "root": { "inputs": { "nixpkgs": "nixpkgs" } },
                "nixpkgs": {
                    "inputs": {},
                    "original": {
                        "type": "github", "owner": "nixos", "repo": "nixpkgs", "ref": "nixos-unstable",
                    },
                    "locked": {
                        "type": "github", "owner": "nixos", "repo": "nixpkgs",
                        "rev": "abc123", "lastModified": 1_700_000_000_i64,
                    },
                },
            },
        })
        .to_string()
    }

    #[tokio::test]
    async fn a_fetched_lock_is_cached_and_its_metric_published() {
        let server = MockServer::start().await;
        mock_empty_channel_queries(&server).await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r/contents/flake.lock"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v1\"")
                    .set_body_json(serde_json::json!({
                        "content": github_base64(&sample_lock()),
                        "encoding": "base64",
                    })),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server);
        let nixos_client = nixos_client_for(&server);
        let mut cache = FlakeCache::default();
        let repos = vec!["o/r".to_owned()];
        let dir = tempfile::tempdir().expect("tempdir");
        run_pass(&client, &nixos_client, &repos, dir.path(), &mut cache).await;

        let nodes = cache.nodes_for("o/r");
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].path, "nixpkgs");

        let (metrics, registry) = Metrics::new();
        record_metrics(&metrics, "o/r", nodes);
        let rendered = crate::metrics::Publisher::new(metrics, registry).render();

        assert!(
            rendered.contains("github_flake_input_locked_timestamp_seconds"),
            "{rendered}"
        );
        assert!(rendered.contains(r#"repo="o/r""#));
        assert!(rendered.contains(r#"input="nixpkgs""#));
        assert!(rendered.contains(r#"path="nixpkgs""#));
        assert!(rendered.contains(r#"channel="nixos-unstable""#));
        assert!(rendered.contains("1700000000"));
    }

    #[tokio::test]
    async fn a_304_keeps_the_cached_set_and_it_is_still_published() {
        let server = MockServer::start().await;
        mock_empty_channel_queries(&server).await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r/contents/flake.lock"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v1\"")
                    .set_body_json(serde_json::json!({
                        "content": github_base64(&sample_lock()),
                        "encoding": "base64",
                    })),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r/contents/flake.lock"))
            .respond_with(ResponseTemplate::new(304))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let nixos_client = nixos_client_for(&server);
        let mut cache = FlakeCache::default();
        let repos = vec!["o/r".to_owned()];
        let dir = tempfile::tempdir().expect("tempdir");

        run_pass(&client, &nixos_client, &repos, dir.path(), &mut cache).await;
        let first = cache.nodes_for("o/r").to_vec();
        assert_eq!(first.len(), 1);

        run_pass(&client, &nixos_client, &repos, dir.path(), &mut cache).await;
        let second = cache.nodes_for("o/r");
        assert_eq!(second, first.as_slice(), "a 304 must replay the cached set");

        let (metrics, registry) = Metrics::new();
        record_metrics(&metrics, "o/r", second);
        let rendered = crate::metrics::Publisher::new(metrics, registry).render();
        assert!(rendered.contains(r#"repo="o/r""#));
    }

    #[tokio::test]
    async fn the_pass_is_skipped_before_the_interval_elapses() {
        let server = MockServer::start().await;
        mock_empty_channel_queries(&server).await;
        // Exactly one request: a second pass issued before the interval has
        // elapsed would be a bug in the orchestrator, not in this module, but
        // the cache's own `should_run` is what the orchestrator must obey --
        // this asserts the contract it provides.
        Mock::given(method("GET"))
            .and(path("/repos/o/r/contents/flake.lock"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v1\"")
                    .set_body_json(serde_json::json!({
                        "content": github_base64(&sample_lock()),
                        "encoding": "base64",
                    })),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server);
        let nixos_client = nixos_client_for(&server);
        let mut cache = FlakeCache::default();
        let repos = vec!["o/r".to_owned()];
        let dir = tempfile::tempdir().expect("tempdir");

        assert!(
            cache.should_run(Duration::from_secs(3600)),
            "the pass has never run, so it is always due"
        );
        run_pass(&client, &nixos_client, &repos, dir.path(), &mut cache).await;

        assert!(
            !cache.should_run(Duration::from_secs(3600)),
            "an hour has not elapsed since the pass that just ran"
        );
    }

    #[tokio::test]
    async fn a_missing_flake_lock_publishes_nothing_for_that_repository() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/o/no-flake/contents/flake.lock"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let nixos_client = nixos_client_for(&server);
        let mut cache = FlakeCache::default();
        let repos = vec!["o/no-flake".to_owned()];
        let dir = tempfile::tempdir().expect("tempdir");
        run_pass(&client, &nixos_client, &repos, dir.path(), &mut cache).await;

        assert_eq!(cache.nodes_for("o/no-flake"), []);

        let (metrics, registry) = Metrics::new();
        record_metrics(&metrics, "o/no-flake", cache.nodes_for("o/no-flake"));
        let rendered = crate::metrics::Publisher::new(metrics, registry).render();
        assert!(!rendered.contains("o/no-flake"));
    }

    #[tokio::test]
    async fn a_fetch_failure_keeps_the_previous_tracked_nodes() {
        let server = MockServer::start().await;
        mock_empty_channel_queries(&server).await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r/contents/flake.lock"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v1\"")
                    .set_body_json(serde_json::json!({
                        "content": github_base64(&sample_lock()),
                        "encoding": "base64",
                    })),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // The second pass hits a transient server error. `classify_retry`
        // retries 500s, so use 403 with no `retry-after` header, which the
        // client treats as a hard failure rather than a retryable one.
        Mock::given(method("GET"))
            .and(path("/repos/o/r/contents/flake.lock"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let nixos_client = nixos_client_for(&server);
        let mut cache = FlakeCache::default();
        let repos = vec!["o/r".to_owned()];
        let dir = tempfile::tempdir().expect("tempdir");

        run_pass(&client, &nixos_client, &repos, dir.path(), &mut cache).await;
        assert_eq!(cache.nodes_for("o/r").len(), 1);

        run_pass(&client, &nixos_client, &repos, dir.path(), &mut cache).await;
        assert_eq!(
            cache.nodes_for("o/r").len(),
            1,
            "a fetch failure must keep the previously observed nodes"
        );
    }

    #[tokio::test]
    async fn an_unparsable_lock_after_a_good_one_keeps_the_previous_nodes_and_metrics() {
        // Regression guard: a `flake.lock` that fails to parse -- an
        // unsupported version, here -- used to degrade to an empty tracked
        // node set rather than an error, which replaced the previous cycle's
        // good data with nothing and cached that nothing under the response's
        // ETag.
        let server = MockServer::start().await;
        mock_empty_channel_queries(&server).await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r/contents/flake.lock"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v1\"")
                    .set_body_json(serde_json::json!({
                        "content": github_base64(&sample_lock()),
                        "encoding": "base64",
                    })),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // The second fetch is a 200 with a decodable body that nonetheless
        // fails to parse as a `flake.lock`, rather than a transport error.
        let unparsable_lock = serde_json::json!({
            "version": 6,
            "root": "root",
            "nodes": { "root": { "inputs": {} } },
        })
        .to_string();
        Mock::given(method("GET"))
            .and(path("/repos/o/r/contents/flake.lock"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v2-bad\"")
                    .set_body_json(serde_json::json!({
                        "content": github_base64(&unparsable_lock),
                        "encoding": "base64",
                    })),
            )
            .mount(&server)
            .await;

        let client = client_for(&server);
        let nixos_client = nixos_client_for(&server);
        let mut cache = FlakeCache::default();
        let repos = vec!["o/r".to_owned()];
        let dir = tempfile::tempdir().expect("tempdir");

        run_pass(&client, &nixos_client, &repos, dir.path(), &mut cache).await;
        assert_eq!(cache.nodes_for("o/r").len(), 1, "cycle one parses cleanly");

        run_pass(&client, &nixos_client, &repos, dir.path(), &mut cache).await;
        assert_eq!(
            cache.nodes_for("o/r").len(),
            1,
            "an unparsable lock must keep the previously observed nodes"
        );

        let (metrics, registry) = Metrics::new();
        record_metrics(&metrics, "o/r", cache.nodes_for("o/r"));
        let rendered = crate::metrics::Publisher::new(metrics, registry).render();
        assert!(
            rendered.contains("1700000000"),
            "the retained metrics must still publish:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn a_pass_where_every_repository_fails_is_not_marked_as_run() {
        // Regression guard: marking the pass as run unconditionally meant a
        // pass where every repository errored -- a transport failure, or
        // every request refused for budget -- still reset the interval,
        // silently extending an outage into a much longer one before the
        // next retry.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/o/down/contents/flake.lock"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let nixos_client = nixos_client_for(&server);
        let mut cache = FlakeCache::default();
        let repos = vec!["o/down".to_owned()];
        let dir = tempfile::tempdir().expect("tempdir");

        run_pass(&client, &nixos_client, &repos, dir.path(), &mut cache).await;

        assert!(
            cache.should_run(Duration::from_secs(3600)),
            "a pass where every repository errored must not consume the interval"
        );
    }

    #[tokio::test]
    async fn a_pass_with_one_successful_repository_is_marked_as_run() {
        let server = MockServer::start().await;
        mock_empty_channel_queries(&server).await;
        Mock::given(method("GET"))
            .and(path("/repos/o/down/contents/flake.lock"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/o/up/contents/flake.lock"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v1\"")
                    .set_body_json(serde_json::json!({
                        "content": github_base64(&sample_lock()),
                        "encoding": "base64",
                    })),
            )
            .mount(&server)
            .await;

        let client = client_for(&server);
        let nixos_client = nixos_client_for(&server);
        let mut cache = FlakeCache::default();
        let repos = vec!["o/down".to_owned(), "o/up".to_owned()];
        let dir = tempfile::tempdir().expect("tempdir");

        run_pass(&client, &nixos_client, &repos, dir.path(), &mut cache).await;

        assert!(
            !cache.should_run(Duration::from_secs(3600)),
            "one successful repository is enough for the pass to count as having run"
        );
    }

    /// A `flake.lock` whose only root input is a `channels.nixos.org`
    /// tarball locked to `short_rev`, tracking `nixpkgs-unstable`.
    fn channel_tarball_lock(short_rev: &str) -> String {
        serde_json::json!({
            "version": 7,
            "root": "root",
            "nodes": {
                "root": { "inputs": { "nixpkgs": "nixpkgs" } },
                "nixpkgs": {
                    "inputs": {},
                    "original": {
                        "type": "tarball",
                        "url": "https://channels.nixos.org/nixpkgs-unstable/nixexprs.tar.zst",
                    },
                    "locked": {
                        "type": "tarball",
                        "url": format!(
                            "https://releases.nixos.org/nixpkgs/nixpkgs-26.11pre0.{short_rev}/nixexprs.tar.zst"
                        ),
                        "lastModified": 1_700_000_000_i64,
                    },
                },
            },
        })
        .to_string()
    }

    fn mock_channel_revision(revision: &str) -> wiremock::Mock {
        Mock::given(method("GET"))
            .and(path("/api/v1/query"))
            .and(wiremock::matchers::query_param("query", "channel_revision"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [{
                        "metric": {
                            "channel": "nixpkgs-unstable",
                            "revision": revision,
                            "current": "1",
                        },
                        "value": [1_700_000_000, "1"],
                    }],
                },
            })))
    }

    #[tokio::test]
    async fn a_channel_tarball_node_stays_behind_across_channel_moves_then_catches_up() {
        // Regression guard for the corrected design: a channel-tarball
        // node's `outdated_since` must be keyed to its own locked revision,
        // not the channel's -- otherwise the channel moving on (while the
        // node's lock stays exactly as behind as before) would restart the
        // clock on every pass.
        let server = MockServer::start().await;
        mock_channel_revision("channelrevA00000000000000000000000000000")
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // Pass two and three both see this revision: the channel moved once,
        // then held still while the lock caught up to it.
        mock_channel_revision("channelrevB11111100000000000000000000000")
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v1/query"))
            .and(wiremock::matchers::query_param(
                "query",
                "channel_update_time",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [{
                        "metric": { "channel": "nixpkgs-unstable" },
                        "value": [1_700_000_000, "1700000500"],
                    }],
                },
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v1/query"))
            .and(wiremock::matchers::query_param(
                "query",
                r#"hydra_job_failed{exported_job=~".*tested"}"#,
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "success",
                "data": { "resultType": "vector", "result": [] },
            })))
            .mount(&server)
            .await;

        // Passes one and two: the lock stays on `oldrev000000`, behind both
        // channel revisions above.
        Mock::given(method("GET"))
            .and(path("/repos/o/r/contents/flake.lock"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "content": github_base64(&channel_tarball_lock("oldrev000000")),
                "encoding": "base64",
            })))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        // Pass three: `nix flake update` moves the lock to the channel's
        // current revision.
        Mock::given(method("GET"))
            .and(path("/repos/o/r/contents/flake.lock"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "content": github_base64(&channel_tarball_lock("channelrevB1")),
                "encoding": "base64",
            })))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let nixos_client = nixos_client_for(&server);
        let mut cache = FlakeCache::default();
        let repos = vec!["o/r".to_owned()];
        let dir = tempfile::tempdir().expect("tempdir");

        run_pass(&client, &nixos_client, &repos, dir.path(), &mut cache).await;
        let first = cache
            .upstream
            .status_for("o/r", &tarball_node(&cache))
            .expect("status recorded")
            .clone();
        assert_eq!(first.freshness, crate::flake::Freshness::Behind);
        let first_outdated_since = first.outdated_since.expect("behind nodes publish this");

        run_pass(&client, &nixos_client, &repos, dir.path(), &mut cache).await;
        let second = cache
            .upstream
            .status_for("o/r", &tarball_node(&cache))
            .expect("status recorded")
            .clone();
        assert_eq!(second.freshness, crate::flake::Freshness::Behind);
        assert_eq!(
            second.outdated_since,
            Some(first_outdated_since),
            "the channel moving on must not reset the clock while the lock is unchanged"
        );
        assert_ne!(
            second.upstream_oid, first.upstream_oid,
            "the channel's revision must actually have moved between these two passes"
        );

        run_pass(&client, &nixos_client, &repos, dir.path(), &mut cache).await;
        let third = cache
            .upstream
            .status_for("o/r", &tarball_node(&cache))
            .expect("status recorded")
            .clone();
        assert_eq!(
            third.freshness,
            crate::flake::Freshness::UpToDate,
            "the lock now matches the channel's current revision"
        );
        assert_eq!(third.outdated_since, None);
    }

    #[tokio::test]
    async fn a_channel_fetch_failure_keeps_the_previous_tarball_freshness_published() {
        // `upstream::run_pass` drops a channel-tarball node's cached status
        // every pass -- it has no GitHub lookup of its own, see finding D in
        // `super::upstream` -- and relies on `channels::apply_tarball_freshness`
        // running after it to set it back from `ChannelCache`'s own data. If
        // this pass's channel fetch fails, `ChannelCache` keeps whatever it
        // fetched last (`channels::run_pass`'s own "keep cached values on
        // failure" contract), so the tarball status published this pass must
        // still be pass one's, not absent.
        let server = MockServer::start().await;
        mock_channel_revision("channelrev0000000000000000000000000000000")
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v1/query"))
            .and(wiremock::matchers::query_param("query", "channel_revision"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v1/query"))
            .and(wiremock::matchers::query_param(
                "query",
                "channel_update_time",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [{
                        "metric": { "channel": "nixpkgs-unstable" },
                        "value": [1_700_000_000, "1700000500"],
                    }],
                },
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v1/query"))
            .and(wiremock::matchers::query_param(
                "query",
                r#"hydra_job_failed{exported_job=~".*tested"}"#,
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "success",
                "data": { "resultType": "vector", "result": [] },
            })))
            .mount(&server)
            .await;

        // The lock never changes across either pass: only the channel fetch
        // fails on pass two.
        Mock::given(method("GET"))
            .and(path("/repos/o/r/contents/flake.lock"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "content": github_base64(&channel_tarball_lock("oldrev000000")),
                "encoding": "base64",
            })))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let nixos_client = nixos_client_for(&server);
        let mut cache = FlakeCache::default();
        let repos = vec!["o/r".to_owned()];
        let dir = tempfile::tempdir().expect("tempdir");

        run_pass(&client, &nixos_client, &repos, dir.path(), &mut cache).await;
        assert_eq!(cache.channels().last_fetch_ok(), Some(true));
        let first = cache
            .upstream
            .status_for("o/r", &tarball_node(&cache))
            .expect("status recorded")
            .clone();
        assert_eq!(first.freshness, crate::flake::Freshness::Behind);

        run_pass(&client, &nixos_client, &repos, dir.path(), &mut cache).await;
        assert_eq!(
            cache.channels().last_fetch_ok(),
            Some(false),
            "pass two's channel fetch failed"
        );
        let second = cache
            .upstream
            .status_for("o/r", &tarball_node(&cache))
            .expect("a channel fetch failure must not leave the tarball node's status unpublished")
            .clone();
        assert_eq!(second.freshness, first.freshness);
        assert_eq!(
            second.outdated_since, first.outdated_since,
            "pass one's freshness, including its outdated-since clock, must still be published \
             through the outage"
        );
    }

    #[test]
    fn nodes_without_a_locked_timestamp_are_skipped() {
        let node = crate::flake::lock::TrackedNode {
            input: "x".to_owned(),
            path: "x".to_owned(),
            kind: crate::flake::lock::NodeKind::Untracked,
            reference: None,
            channel: None,
            locked_rev: None,
            locked_last_modified: None,
            owner: None,
            repo: None,
        };
        let (metrics, registry) = Metrics::new();
        record_metrics(&metrics, "o/r", &[node]);
        let rendered = crate::metrics::Publisher::new(metrics, registry).render();
        assert!(!rendered.contains("flake_input_locked_timestamp"));
    }
}
