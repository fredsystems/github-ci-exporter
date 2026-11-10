//! Reads the flake-watch Action's status file and publishes impact.
//!
//! `status/flake-inputs.json`, committed to the `flake-watch` branch by the
//! watch Action (subtask 3.10, in the `nixos` repo), records -- per
//! configured repository and root input -- whether a moved input would
//! change anything a host actually ships, classified by evaluation rather
//! than by which files changed. A branch or file that does not exist yet (a
//! `404`) is not a collection failure: the watch Action has simply never run
//! for that repository, so this pass publishes no impact series for it and
//! logs once at `info`, the same "absence is not an error" contract
//! `super::collect` already applies to a repository with no `flake.lock`.

use std::collections::HashMap;

use reqwest::StatusCode;
use serde::Deserialize;
use tracing::{info, warn};

use crate::{
    github::{Client, ClientError, rest},
    metrics::{FlakeInputImpactLabels, Metrics},
};

/// The only `status/flake-inputs.json` schema version this parser
/// understands. An unrecognised version is ignored wholesale -- the schema
/// changed meaning, not just shape, so guessing at it is worse than
/// publishing nothing until the exporter is updated.
const SUPPORTED_VERSION: u64 = 1;

/// The branch the watch Action commits its status file to.
const STATUS_REF: &str = "flake-watch";

/// Kinds the watch Action may classify a moved input as -- see
/// `PLAN_03_FLAKE_INPUT_TRACKING.md`'s "Impact is decided by evaluation, not
/// by file names".
const KNOWN_KINDS: [&str; 3] = ["none", "rebuild", "source"];

/// One root input's recorded impact, by operating system (`linux`, `darwin`).
/// Either key may be absent; only recognised kinds are kept.
///
/// Serialisable because this is the projection the `ETag` cache stores.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InputImpact {
    pub kinds: HashMap<String, String>,
}

/// Cross-cycle state for the impact pass: the most recently observed status
/// file contents per repository.
#[derive(Debug, Default)]
pub struct StatusCache {
    /// `owner/name` -> input -> its recorded impact.
    impact: HashMap<String, HashMap<String, InputImpact>>,
}

impl StatusCache {
    #[must_use]
    pub fn impact_for(&self, repo: &str) -> Option<&HashMap<String, InputImpact>> {
        self.impact.get(repo)
    }

    fn set(&mut self, repo: &str, impact: HashMap<String, InputImpact>) {
        self.impact.insert(repo.to_owned(), impact);
    }

    fn clear(&mut self, repo: &str) {
        self.impact.remove(repo);
    }
}

/// Fetches and parses the status file for every configured repository.
///
/// A fetch failure (anything but a `404`) is logged and the previous impact
/// is kept, matching every other per-repository fetch in this crate: one
/// broken repository must not blind the exporter to the others.
pub async fn run_pass(client: &Client, repos: &[String], cache: &mut StatusCache) {
    for repo in repos {
        match fetch_status(client, repo).await {
            Ok(Some(impact)) => cache.set(repo, impact),
            Ok(None) => {
                info!(
                    repo,
                    "no flake-watch status file yet; publishing no impact for this repository"
                );
                cache.clear(repo);
            }
            Err(error) => {
                warn!(repo, %error, "failed to fetch flake-watch status file; keeping previous impact");
            }
        }
    }
}

async fn fetch_status(
    client: &Client,
    repo: &str,
) -> Result<Option<HashMap<String, InputImpact>>, ClientError> {
    let path = format!("/repos/{repo}/contents/status/flake-inputs.json?ref={STATUS_REF}");
    match client.get_cached(&path, project_status).await {
        Ok((impact, _outcome)) => Ok(Some(impact)),
        Err(ClientError::Status { status, .. }) if status == StatusCode::NOT_FOUND => Ok(None),
        Err(error) => Err(error),
    }
}

#[derive(Debug, Deserialize)]
struct ContentResponse {
    content: String,
    encoding: String,
}

#[derive(Debug, Deserialize)]
struct StatusFile {
    version: u64,
    #[serde(default)]
    inputs: HashMap<String, StatusInput>,
}

#[derive(Debug, Deserialize)]
struct StatusInput {
    #[serde(default)]
    kinds: HashMap<String, String>,
}

/// Decodes and parses a status-file Contents API response.
///
/// Every failure mode here degrades to an empty map rather than propagating,
/// the same lenient-default idiom `flake::collect::project_flake_lock` uses:
/// this is the projection the `ETag` cache stores and replays on a `304`, so
/// there is no later point at which a parse failure could be retried.
fn project_status(response: ContentResponse) -> HashMap<String, InputImpact> {
    let ContentResponse { content, encoding } = response;
    if encoding != "base64" {
        warn!(
            encoding,
            "flake-inputs.json used an unexpected encoding; publishing no impact"
        );
        return HashMap::new();
    }
    let raw = rest::decode_base64(&content);
    let file: StatusFile = match serde_json::from_str(&raw) {
        Ok(file) => file,
        Err(error) => {
            warn!(%error, "failed to parse flake-inputs.json; publishing no impact");
            return HashMap::new();
        }
    };
    if file.version != SUPPORTED_VERSION {
        warn!(
            version = file.version,
            expected = SUPPORTED_VERSION,
            "unsupported flake-inputs.json version; publishing no impact"
        );
        return HashMap::new();
    }

    file.inputs
        .into_iter()
        .map(|(input, raw_input)| {
            let kinds = raw_input
                .kinds
                .into_iter()
                .filter(|(os, kind)| {
                    let known = KNOWN_KINDS.contains(&kind.as_str());
                    if !known {
                        warn!(
                            input,
                            os, kind, "unrecognised flake-inputs.json kind; ignoring entry"
                        );
                    }
                    known
                })
                .collect();
            (input, InputImpact { kinds })
        })
        .collect()
}

/// Publishes `flake_input_impact` for `repo`'s currently cached status.
///
/// Called every cycle, independent of whether this pass ran this cycle --
/// the registry is rebuilt from scratch each cycle, so cached data not
/// republished here simply disappears.
pub fn record_metrics(metrics: &Metrics, repo: &str, cache: &StatusCache) {
    let Some(impact) = cache.impact_for(repo) else {
        return;
    };
    for (input, input_impact) in impact {
        for (os, kind) in &input_impact.kinds {
            metrics
                .flake_input_impact
                .get_or_create(&FlakeInputImpactLabels {
                    repo: repo.to_owned(),
                    input: input.clone(),
                    os: os.clone(),
                    kind: kind.clone(),
                })
                .set(1);
        }
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
    use crate::metrics::{Metrics, Publisher};

    fn client_for(server: &MockServer) -> Client {
        Client::new("t", &server.uri(), &format!("{}/graphql", server.uri()))
            .expect("client builds")
    }

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

    #[tokio::test]
    async fn a_404_publishes_no_impact_series() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r/contents/status/flake-inputs.json"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let mut cache = StatusCache::default();
        run_pass(&client, &["o/r".to_owned()], &mut cache).await;

        assert!(cache.impact_for("o/r").is_none());
        let (metrics, registry) = Metrics::new();
        record_metrics(&metrics, "o/r", &cache);
        assert!(
            !Publisher::new(metrics, registry)
                .render()
                .contains("flake_input_impact")
        );
    }

    #[tokio::test]
    async fn a_valid_file_publishes_impact_with_os_and_kind() {
        let body = serde_json::json!({
            "version": 1,
            "inputs": { "niri": { "kinds": { "linux": "source" }, "upstream_rev": "abc" } }
        })
        .to_string();

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r/contents/status/flake-inputs.json"))
            .and(query_param("ref", "flake-watch"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "content": github_base64(&body),
                "encoding": "base64",
            })))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let mut cache = StatusCache::default();
        run_pass(&client, &["o/r".to_owned()], &mut cache).await;

        let (metrics, registry) = Metrics::new();
        record_metrics(&metrics, "o/r", &cache);
        let rendered = Publisher::new(metrics, registry).render();
        assert!(rendered.contains("github_flake_input_impact"));
        assert!(rendered.contains(r#"input="niri""#));
        assert!(rendered.contains(r#"os="linux""#));
        assert!(rendered.contains(r#"kind="source""#));
    }

    #[test]
    fn an_unrecognised_version_is_ignored() {
        let body = serde_json::json!({
            "version": 2,
            "inputs": { "niri": { "kinds": { "linux": "source" } } }
        })
        .to_string();
        let impact = project_status(ContentResponse {
            content: github_base64(&body),
            encoding: "base64".to_owned(),
        });
        assert!(impact.is_empty());
    }

    #[test]
    fn an_unrecognised_kind_is_ignored_but_the_rest_of_the_input_survives() {
        let body = serde_json::json!({
            "version": 1,
            "inputs": { "niri": { "kinds": { "linux": "source", "darwin": "explodes" } } }
        })
        .to_string();
        let impact = project_status(ContentResponse {
            content: github_base64(&body),
            encoding: "base64".to_owned(),
        });
        let niri = impact.get("niri").expect("input present");
        assert_eq!(niri.kinds.get("linux").map(String::as_str), Some("source"));
        assert_eq!(niri.kinds.get("darwin"), None);
    }
}
