//! REST collectors for GitHub Actions.
//!
//! Actions state is read over REST rather than GraphQL because the commit
//! `statusCheckRollup` is unusable here: most workflows in these
//! organisations are `schedule` or `workflow_dispatch` triggered and are
//! therefore never attached to the head commit. Measured against the live
//! API, the rollup was null for 30 of 31 sampled repositories.
//!
//! One `GET /repos/{o}/{r}/actions/runs?per_page=100` per repository, with the
//! latest run per workflow selected client-side, costs one request per repo
//! instead of one per workflow (61 vs 239 for the current fleet).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::client::{Client, ClientError};
use crate::model::{Repo, RunConclusion};

/// Runs fetched per repository. 100 is the API maximum and comfortably covers
/// the most recent run of every workflow for these repositories.
const RUNS_PER_PAGE: usize = 100;

#[derive(Debug, Deserialize)]
struct WorkflowsResponse {
    workflows: Vec<WorkflowEntry>,
}

#[derive(Debug, Deserialize)]
struct WorkflowEntry {
    name: String,
    path: String,
    state: String,
}

/// A workflow definition that exists in the repository.
///
/// Serialisable because this is what gets stored in the `ETag` cache, rather
/// than the full API response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Workflow {
    pub name: String,
    pub path: String,
}

/// Lists active workflows defined by files in `.github/workflows`.
///
/// GitHub also reports "dynamic" workflows (Dependabot updates, Copilot
/// reviewers) that have no file in the repository. Those are excluded: they
/// are not the repository's CI and their presence would make every repo look
/// like it has CI.
///
/// # Errors
/// Returns [`ClientError`] if the request fails.
pub async fn list_workflows(client: &Client, repo: &Repo) -> Result<Vec<Workflow>, ClientError> {
    let path = format!(
        "/repos/{}/{}/actions/workflows?per_page=100",
        repo.owner, repo.name
    );
    let (workflows, _) = client
        .get_cached(&path, |response: WorkflowsResponse| {
            response
                .workflows
                .into_iter()
                .filter(|w| w.state == "active" && w.path.starts_with(".github/workflows"))
                .map(|w| Workflow {
                    name: w.name,
                    path: w.path,
                })
                .collect::<Vec<_>>()
        })
        .await?;

    Ok(workflows)
}

#[derive(Debug, Deserialize)]
struct RunsResponse {
    workflow_runs: Vec<RunEntry>,
}

#[derive(Debug, Deserialize)]
struct RunEntry {
    name: Option<String>,
    #[serde(default)]
    path: Option<String>,
    status: String,
    conclusion: Option<String>,
    event: String,
    created_at: DateTime<Utc>,
    html_url: String,
}

/// The most recent run of a single workflow on the default branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatestRun {
    pub workflow: String,
    pub conclusion: RunConclusion,
    pub event: String,
    pub created_at: DateTime<Utc>,
    pub html_url: String,
}

/// Most recent run per workflow, plus the most recent *successful* run.
///
/// This reduced form is what the `ETag` cache stores. The raw runs listing is
/// ~1.5 MB per repository and is discarded immediately after reduction.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RepoRuns {
    pub latest: Vec<LatestRun>,
    /// Last success per workflow, used to express "how long has this been
    /// broken" and to detect stalled scheduled workflows.
    pub last_success: HashMap<String, DateTime<Utc>>,
}

/// Fetches recent runs on the default branch and reduces them to the latest
/// run per workflow.
///
/// # Errors
/// Returns [`ClientError`] if the request fails.
pub async fn fetch_runs(client: &Client, repo: &Repo) -> Result<RepoRuns, ClientError> {
    let path = format!(
        "/repos/{}/{}/actions/runs?per_page={RUNS_PER_PAGE}&branch={}",
        repo.owner, repo.name, repo.default_branch
    );
    let (runs, outcome) = client
        .get_cached(&path, |response: RunsResponse| {
            reduce_runs(response.workflow_runs)
        })
        .await?;
    debug!(repo = %repo, ?outcome, workflows = runs.latest.len(), "fetched runs");
    Ok(runs)
}

/// Reduces a run list to the newest run per workflow.
///
/// Dependabot's security-update runs are excluded: each one has a unique
/// generated name (`npm_and_yarn in /. for ...`), so keeping them would
/// produce unbounded metric cardinality.
fn reduce_runs(runs: Vec<RunEntry>) -> RepoRuns {
    let mut latest: HashMap<String, LatestRun> = HashMap::new();
    let mut last_success: HashMap<String, DateTime<Utc>> = HashMap::new();

    for run in runs {
        if run.event == "dynamic" {
            continue;
        }
        // A run whose workflow file was deleted still appears in history;
        // without a path there is no stable identity to key on.
        let Some(name) = run.name.filter(|n| !n.is_empty()) else {
            continue;
        };
        if run
            .path
            .as_ref()
            .is_some_and(|p| !p.starts_with(".github/workflows"))
        {
            continue;
        }

        let conclusion = RunConclusion::from_api(&run.status, run.conclusion.as_deref());

        if conclusion == RunConclusion::Success {
            last_success
                .entry(name.clone())
                .and_modify(|existing| {
                    if run.created_at > *existing {
                        *existing = run.created_at;
                    }
                })
                .or_insert(run.created_at);
        }

        match latest.get_mut(&name) {
            Some(existing) if run.created_at <= existing.created_at => {}
            Some(existing) => {
                *existing = LatestRun {
                    workflow: name,
                    conclusion,
                    event: run.event,
                    created_at: run.created_at,
                    html_url: run.html_url,
                };
            }
            None => {
                latest.insert(
                    name.clone(),
                    LatestRun {
                        workflow: name,
                        conclusion,
                        event: run.event,
                        created_at: run.created_at,
                        html_url: run.html_url,
                    },
                );
            }
        }
    }

    let mut latest: Vec<LatestRun> = latest.into_values().collect();
    latest.sort_by(|a, b| a.workflow.cmp(&b.workflow));
    RepoRuns {
        latest,
        last_success,
    }
}

#[derive(Debug, Deserialize)]
struct ContentResponse {
    content: String,
    encoding: String,
}

/// Fetches a workflow file and extracts its `schedule:` cron expressions.
///
/// The REST API does not expose a workflow's schedule, so the file itself must
/// be parsed. Results change rarely and are `ETag`-revalidated, so this is
/// nearly free after the first sweep.
///
/// # Errors
/// Returns [`ClientError`] if the file cannot be fetched.
pub async fn fetch_workflow_crons(
    client: &Client,
    repo: &Repo,
    workflow_path: &str,
) -> Result<Vec<String>, ClientError> {
    let path = format!(
        "/repos/{}/{}/contents/{workflow_path}?ref={}",
        repo.owner, repo.name, repo.default_branch
    );
    let (crons, _) = client
        .get_cached(&path, |response: ContentResponse| {
            if response.encoding == "base64" {
                parse_crons(&decode_base64(&response.content))
            } else {
                Vec::new()
            }
        })
        .await?;
    Ok(crons)
}

/// Decodes GitHub's line-wrapped base64 content payloads.
fn decode_base64(input: &str) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut lookup = [255u8; 256];
    let mut i = 0;
    while i < 64 {
        lookup[TABLE[i] as usize] = u8::try_from(i).unwrap_or(0);
        i += 1;
    }

    let mut out = Vec::new();
    let mut buffer = 0u32;
    let mut bits = 0u32;
    for byte in input.bytes() {
        let value = lookup[byte as usize];
        if value == 255 {
            continue;
        }
        buffer = (buffer << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            let shifted = (buffer >> bits) & 0xFF;
            // Masked to a single byte above, so this cannot truncate.
            #[allow(clippy::cast_possible_truncation)]
            out.push(shifted as u8);
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Extracts cron expressions from a workflow's `on.schedule` block.
fn parse_crons(yaml: &str) -> Vec<String> {
    let Ok(document) = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(yaml) else {
        return Vec::new();
    };
    // `on` is a YAML 1.1 boolean, so some parsers surface this key as `true`.
    let triggers = document
        .get("on")
        .or_else(|| document.get(serde_yaml_ng::Value::Bool(true)));
    let Some(schedule) = triggers.and_then(|t| t.get("schedule")) else {
        return Vec::new();
    };
    let Some(entries) = schedule.as_sequence() else {
        return Vec::new();
    };

    entries
        .iter()
        .filter_map(|entry| entry.get("cron")?.as_str().map(str::to_owned))
        .collect()
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "panicking is how a test reports failure"
)]
mod tests {
    use super::*;

    fn run(
        name: &str,
        status: &str,
        conclusion: Option<&str>,
        event: &str,
        created: &str,
    ) -> RunEntry {
        RunEntry {
            name: Some(name.to_owned()),
            path: Some(".github/workflows/x.yaml".to_owned()),
            status: status.to_owned(),
            conclusion: conclusion.map(str::to_owned),
            event: event.to_owned(),
            created_at: created.parse().expect("valid timestamp"),
            html_url: format!("https://github.com/o/r/actions/runs/{name}"),
        }
    }

    #[test]
    fn keeps_only_the_newest_run_per_workflow() {
        let runs = vec![
            run(
                "CI",
                "completed",
                Some("failure"),
                "push",
                "2026-08-01T00:00:00Z",
            ),
            run(
                "CI",
                "completed",
                Some("success"),
                "push",
                "2026-08-09T00:00:00Z",
            ),
            run(
                "Deploy",
                "completed",
                Some("success"),
                "push",
                "2026-08-05T00:00:00Z",
            ),
        ];
        let reduced = reduce_runs(runs);

        assert_eq!(reduced.latest.len(), 2);
        let ci = reduced
            .latest
            .iter()
            .find(|r| r.workflow == "CI")
            .expect("CI present");
        assert_eq!(ci.conclusion, RunConclusion::Success);
        assert_eq!(
            ci.created_at.to_rfc3339(),
            "2026-08-09T00:00:00+00:00",
            "newest run must win regardless of input order"
        );
    }

    #[test]
    fn out_of_order_input_still_selects_newest() {
        let runs = vec![
            run(
                "CI",
                "completed",
                Some("success"),
                "push",
                "2026-08-09T00:00:00Z",
            ),
            run(
                "CI",
                "completed",
                Some("failure"),
                "push",
                "2026-08-01T00:00:00Z",
            ),
        ];
        let reduced = reduce_runs(runs);
        assert_eq!(reduced.latest[0].conclusion, RunConclusion::Success);
    }

    #[test]
    fn tracks_last_success_separately_from_latest() {
        let runs = vec![
            run(
                "CI",
                "completed",
                Some("success"),
                "push",
                "2026-08-01T00:00:00Z",
            ),
            run(
                "CI",
                "completed",
                Some("failure"),
                "push",
                "2026-08-09T00:00:00Z",
            ),
        ];
        let reduced = reduce_runs(runs);

        assert_eq!(reduced.latest[0].conclusion, RunConclusion::Failure);
        assert_eq!(
            reduced.last_success.get("CI").map(DateTime::to_rfc3339),
            Some("2026-08-01T00:00:00+00:00".to_owned()),
            "a later failure must not erase the last known success"
        );
    }

    #[test]
    fn excludes_dependabot_dynamic_runs() {
        // These have unique generated names and would explode cardinality.
        let runs = vec![
            run(
                "npm_and_yarn in /. for esbuild, tmp - Update #1483936572",
                "completed",
                Some("success"),
                "dynamic",
                "2026-07-26T21:05:23Z",
            ),
            run(
                "CI",
                "completed",
                Some("success"),
                "push",
                "2026-08-09T00:00:00Z",
            ),
        ];
        let reduced = reduce_runs(runs);

        assert_eq!(reduced.latest.len(), 1);
        assert_eq!(reduced.latest[0].workflow, "CI");
    }

    #[test]
    fn in_flight_run_does_not_clear_a_failure_state() {
        let runs = vec![run(
            "CI",
            "in_progress",
            None,
            "push",
            "2026-08-09T00:00:00Z",
        )];
        let reduced = reduce_runs(runs);
        assert_eq!(reduced.latest[0].conclusion, RunConclusion::Running);
        assert!(!reduced.latest[0].conclusion.is_failure());
    }

    #[test]
    fn parses_cron_from_workflow_yaml() {
        let yaml = r#"
name: Check container software versions
on:
  workflow_dispatch:
  schedule:
    - cron: "0 12 * * *"
jobs:
  build:
    runs-on: ubuntu-24.04
"#;
        assert_eq!(parse_crons(yaml), ["0 12 * * *"]);
    }

    #[test]
    fn parses_multiple_crons() {
        let yaml = r#"
on:
  schedule:
    - cron: "0 0 * * 1"
    - cron: "0 12 * * 4"
"#;
        assert_eq!(parse_crons(yaml), ["0 0 * * 1", "0 12 * * 4"]);
    }

    #[test]
    fn workflow_without_schedule_yields_no_crons() {
        let yaml = r"
on:
  pull_request:
  merge_group:
";
        assert!(parse_crons(yaml).is_empty());
    }

    #[test]
    fn malformed_yaml_does_not_panic() {
        assert!(parse_crons("this: is: not: valid: yaml:").is_empty());
        assert!(parse_crons("").is_empty());
    }

    #[test]
    fn decodes_github_wrapped_base64() {
        // GitHub wraps contents payloads at 60 chars with embedded newlines.
        let encoded = "b246CiAgc2NoZWR1bGU6CiAgICAtIGNyb246ICIwIDEyICogKiAqIgo=";
        let decoded = decode_base64(encoded);
        assert!(decoded.contains("cron"));
        assert_eq!(parse_crons(&decoded), ["0 12 * * *"]);
    }

    #[test]
    fn decodes_base64_with_embedded_newlines() {
        let decoded = decode_base64("b246CiAgc2NoZWR1\nbGU6CiAgICAtIGNyb246ICIwIDEyICogKiAqIgo=");
        assert_eq!(parse_crons(&decoded), ["0 12 * * *"]);
    }
}
