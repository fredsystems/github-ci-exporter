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

/// Age beyond which a run no longer describes the current code.
///
/// Some workflows only fire on `pull_request`, so their newest branch-state
/// run can be many months old while the workflow itself is healthy. Reporting
/// that ancient conclusion produces an alert nobody can clear without an
/// artificial push. Past this age the run is marked stale and stops
/// contributing a conclusion.
pub const STALE_RUN_AGE: chrono::TimeDelta = chrono::TimeDelta::days(90);

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

/// Whether GitHub will currently run a workflow.
///
/// The distinction between the two disabled states is the point: GitHub
/// automatically disables scheduled workflows in a repository with no activity
/// for 60 days, which silently stops CI. That is a fault worth alerting on,
/// whereas a manually disabled workflow is a deliberate choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowState {
    Active,
    /// Auto-disabled by GitHub after 60 days of repository inactivity.
    DisabledInactivity,
    /// Switched off by a human, or disabled because the repo is a fork.
    DisabledManually,
}

impl WorkflowState {
    fn from_api(state: &str) -> Self {
        match state {
            "active" => Self::Active,
            "disabled_inactivity" => Self::DisabledInactivity,
            _ => Self::DisabledManually,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::DisabledInactivity => "disabled_inactivity",
            Self::DisabledManually => "disabled_manually",
        }
    }
}

/// A workflow definition that exists in the repository.
///
/// Serialisable because this is what gets stored in the `ETag` cache, rather
/// than the full API response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Workflow {
    pub name: String,
    pub path: String,
    pub state: WorkflowState,
}

/// Lists workflows defined by files in `.github/workflows`.
///
/// Disabled workflows are **included**, with their state, because a workflow
/// auto-disabled for inactivity still has meaningful run history and its
/// disablement is itself worth reporting. Filtering them out here caused a
/// failing `update-flakes` to disappear from the metrics entirely.
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
                .filter(|w| w.path.starts_with(".github/workflows"))
                .map(|w| Workflow {
                    name: w.name,
                    state: WorkflowState::from_api(&w.state),
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
    // The display name is deliberately not read: it is unreliable (older runs
    // report the file path instead) and the authoritative name comes from the
    // live workflow list, keyed by `path`.
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

impl LatestRun {
    /// Whether this run is too old to describe the current code.
    #[must_use]
    pub fn is_stale(&self, now: DateTime<Utc>) -> bool {
        now - self.created_at > STALE_RUN_AGE
    }
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
/// `live` is the current workflow set from [`list_workflows`]; runs belonging
/// to workflows absent from it are discarded as orphaned history.
///
/// # Errors
/// Returns [`ClientError`] if the request fails.
pub async fn fetch_runs(
    client: &Client,
    repo: &Repo,
    live: &[Workflow],
) -> Result<RepoRuns, ClientError> {
    let path = format!(
        "/repos/{}/{}/actions/runs?per_page={RUNS_PER_PAGE}&branch={}",
        repo.owner, repo.name, repo.default_branch
    );
    let (runs, outcome) = client
        .get_cached(&path, |response: RunsResponse| {
            reduce_runs(response.workflow_runs, live)
        })
        .await?;
    debug!(repo = %repo, ?outcome, workflows = runs.latest.len(), "fetched runs");
    Ok(runs)
}

/// Whether a run's trigger reflects the state of the default branch.
///
/// Only these events describe "is the branch healthy right now":
///
/// * `push` -- code landed on the branch.
/// * `schedule` / `workflow_dispatch` -- ran against the branch as it stands.
///
/// `pull_request` runs are excluded even when the API's `branch=` filter
/// matches them, because that filter matches the PR's *head* branch. A merged
/// PR's last pre-merge failure would otherwise be reported as the branch's
/// current CI state forever, which was observed on both
/// `fredsystems/pre-commit-checks` and `sdr-enthusiasts/docker-planefence`.
/// Post-merge health is covered by the `push` run that merging produces.
///
/// `dynamic` is Dependabot's generated security-update runs, each with a
/// unique name; keeping them would make cardinality unbounded.
fn is_branch_state_event(event: &str) -> bool {
    matches!(event, "push" | "schedule" | "workflow_dispatch")
}

/// Reduces a run list to the newest run per workflow, keeping only workflows
/// that still exist in the repository.
///
/// Two classes of stale data must be discarded, both observed in the wild:
///
/// * **Orphaned runs.** Run history outlives the workflow file. A deleted
///   `Update pre-commit hooks` workflow kept reporting its final failure
///   indefinitely -- 18 of 38 observed failures were this. Runs are therefore
///   intersected with the live workflow list.
/// * **Path-named runs.** Runs created before a workflow gained a `name:`
///   field report the file path where the name should be, so `CI` and
///   `.github/workflows/ci.yml` appear as two distinct workflows. Keying on
///   `path` and resolving the display name from the live workflow list
///   collapses them.
///
/// Dependabot's security-update runs are excluded: each has a unique generated
/// name, which would make cardinality unbounded.
fn reduce_runs(runs: Vec<RunEntry>, live: &[Workflow]) -> RepoRuns {
    // Workflow identity is the file path, not the display name: a run created
    // before the workflow gained a `name:` reports the path in the name field,
    // and a renamed workflow would otherwise split into two series.
    let live_by_path: HashMap<&str, &Workflow> =
        live.iter().map(|w| (w.path.as_str(), w)).collect();

    let mut latest: HashMap<String, LatestRun> = HashMap::new();
    let mut last_success: HashMap<String, DateTime<Utc>> = HashMap::new();

    for run in runs {
        if !is_branch_state_event(&run.event) {
            continue;
        }
        // Runs of a since-deleted workflow linger in history forever. Only
        // workflows still present in the repository are reported.
        let Some(path) = run.path.as_deref() else {
            continue;
        };
        let Some(workflow) = live_by_path.get(path) else {
            continue;
        };
        let name = workflow.name.clone();

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

    /// A run of a workflow whose file is `.github/workflows/<name>.yml`.
    fn run(
        name: &str,
        status: &str,
        conclusion: Option<&str>,
        event: &str,
        created: &str,
    ) -> RunEntry {
        run_at(
            name,
            &format!(".github/workflows/{name}.yml"),
            status,
            conclusion,
            event,
            created,
        )
    }

    fn run_at(
        name: &str,
        path: &str,
        status: &str,
        conclusion: Option<&str>,
        event: &str,
        created: &str,
    ) -> RunEntry {
        RunEntry {
            path: Some(path.to_owned()),
            status: status.to_owned(),
            conclusion: conclusion.map(str::to_owned),
            event: event.to_owned(),
            created_at: created.parse().expect("valid timestamp"),
            html_url: format!("https://github.com/o/r/actions/runs/{name}"),
        }
    }

    /// Declares workflows as currently present in the repository.
    fn live(names: &[&str]) -> Vec<Workflow> {
        names
            .iter()
            .map(|n| Workflow {
                name: (*n).to_owned(),
                path: format!(".github/workflows/{n}.yml"),
                state: WorkflowState::Active,
            })
            .collect()
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
        let reduced = reduce_runs(runs, &live(&["CI", "Deploy"]));

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
        let reduced = reduce_runs(runs, &live(&["CI", "Deploy"]));
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
        let reduced = reduce_runs(runs, &live(&["CI", "Deploy"]));

        assert_eq!(reduced.latest[0].conclusion, RunConclusion::Failure);
        assert_eq!(
            reduced.last_success.get("CI").map(DateTime::to_rfc3339),
            Some("2026-08-01T00:00:00+00:00".to_owned()),
            "a later failure must not erase the last known success"
        );
    }

    #[test]
    fn excludes_pull_request_runs() {
        // Regression guard: the API's `branch=` filter matches a PR's HEAD
        // branch, so PR runs leak through. A merged PR's failing pre-merge run
        // was being reported as the default branch's current CI state.
        let runs = vec![
            run(
                "Lint",
                "completed",
                Some("failure"),
                "pull_request",
                "2026-08-03T16:51:03Z",
            ),
            run(
                "Lint",
                "completed",
                Some("success"),
                "push",
                "2026-08-01T00:00:00Z",
            ),
        ];
        let reduced = reduce_runs(runs, &live(&["Lint"]));

        assert_eq!(reduced.latest.len(), 1);
        assert_eq!(
            reduced.latest[0].conclusion,
            RunConclusion::Success,
            "a newer pull_request run must not override the branch's push state"
        );
    }

    #[test]
    fn keeps_schedule_and_dispatch_runs() {
        for event in ["push", "schedule", "workflow_dispatch"] {
            let runs = vec![run(
                "CI",
                "completed",
                Some("failure"),
                event,
                "2026-08-09T00:00:00Z",
            )];
            let reduced = reduce_runs(runs, &live(&["CI"]));
            assert_eq!(reduced.latest.len(), 1, "{event} must be kept");
        }
        for event in ["pull_request", "dynamic", "pull_request_target"] {
            let runs = vec![run(
                "CI",
                "completed",
                Some("failure"),
                event,
                "2026-08-09T00:00:00Z",
            )];
            let reduced = reduce_runs(runs, &live(&["CI"]));
            assert!(reduced.latest.is_empty(), "{event} must be excluded");
        }
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
        let reduced = reduce_runs(runs, &live(&["CI", "Deploy"]));

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
        let reduced = reduce_runs(runs, &live(&["CI", "Deploy"]));
        assert_eq!(reduced.latest[0].conclusion, RunConclusion::Running);
        assert!(!reduced.latest[0].conclusion.is_failure());
    }

    #[test]
    fn discards_runs_of_deleted_workflows() {
        // Regression guard: GitHub keeps run history after a workflow file is
        // removed. Observed in the wild as a long-deleted "Update pre-commit
        // hooks" reporting a permanent failure -- 18 of 38 failures were this.
        let runs = vec![
            run(
                "CI",
                "completed",
                Some("success"),
                "push",
                "2026-08-09T00:00:00Z",
            ),
            run(
                "Update pre-commit hooks",
                "completed",
                Some("failure"),
                "schedule",
                "2025-12-14T00:54:24Z",
            ),
        ];
        let reduced = reduce_runs(runs, &live(&["CI"]));

        assert_eq!(reduced.latest.len(), 1);
        assert_eq!(reduced.latest[0].workflow, "CI");
        assert!(
            !reduced.latest.iter().any(|r| r.conclusion.is_failure()),
            "a deleted workflow must not report a failure"
        );
    }

    #[test]
    fn collapses_path_named_runs_onto_the_workflow_name() {
        // Runs created before a workflow gained a `name:` report the file path
        // in the name field. Keying on path prevents "CI" and
        // ".github/workflows/ci.yml" becoming two series for one workflow.
        let runs = vec![
            run_at(
                ".github/workflows/CI.yml",
                ".github/workflows/CI.yml",
                "completed",
                Some("failure"),
                "push",
                "2026-07-08T03:36:04Z",
            ),
            run(
                "CI",
                "completed",
                Some("success"),
                "push",
                "2026-08-09T00:00:00Z",
            ),
        ];
        let reduced = reduce_runs(runs, &live(&["CI"]));

        assert_eq!(reduced.latest.len(), 1, "one workflow, one series");
        assert_eq!(reduced.latest[0].workflow, "CI");
        assert_eq!(
            reduced.latest[0].conclusion,
            RunConclusion::Success,
            "newest run wins after collapsing"
        );
    }

    #[test]
    fn keeps_workflows_regardless_of_yml_or_yaml_extension() {
        // Both spellings are in active use across the fleet.
        let workflows = vec![
            Workflow {
                name: "Deploy".to_owned(),
                path: ".github/workflows/deploy.yml".to_owned(),
                state: WorkflowState::Active,
            },
            Workflow {
                name: "Lint".to_owned(),
                path: ".github/workflows/lint.yaml".to_owned(),
                state: WorkflowState::Active,
            },
        ];
        let runs = vec![
            run_at(
                "Deploy",
                ".github/workflows/deploy.yml",
                "completed",
                Some("success"),
                "push",
                "2026-08-09T00:00:00Z",
            ),
            run_at(
                "Lint",
                ".github/workflows/lint.yaml",
                "completed",
                Some("failure"),
                "push",
                "2026-08-09T00:00:00Z",
            ),
        ];
        let reduced = reduce_runs(runs, &workflows);

        assert_eq!(reduced.latest.len(), 2, "both extensions must be kept");
    }

    #[test]
    fn disabled_workflows_still_report_their_runs() {
        // Regression guard: filtering to state=="active" made a failing
        // `update-flakes` vanish from the metrics after GitHub auto-disabled
        // it for repository inactivity -- the exact condition worth alerting
        // on was the one being hidden.
        let workflows = vec![Workflow {
            name: "update-flakes".to_owned(),
            path: ".github/workflows/update-flakes.yaml".to_owned(),
            state: WorkflowState::DisabledInactivity,
        }];
        let runs = vec![run_at(
            "update-flakes",
            ".github/workflows/update-flakes.yaml",
            "completed",
            Some("failure"),
            "schedule",
            "2026-08-01T00:47:26Z",
        )];
        let reduced = reduce_runs(runs, &workflows);

        assert_eq!(reduced.latest.len(), 1);
        assert!(reduced.latest[0].conclusion.is_failure());
    }

    #[test]
    fn workflow_state_maps_the_two_disabled_kinds_apart() {
        assert_eq!(WorkflowState::from_api("active"), WorkflowState::Active);
        assert_eq!(
            WorkflowState::from_api("disabled_inactivity"),
            WorkflowState::DisabledInactivity
        );
        assert_eq!(
            WorkflowState::from_api("disabled_manually"),
            WorkflowState::DisabledManually
        );
        // Unknown future states are treated as a deliberate disable rather
        // than as an inactivity fault, so they cannot cause a false page.
        assert_eq!(
            WorkflowState::from_api("something_new"),
            WorkflowState::DisabledManually
        );
    }

    #[test]
    fn ancient_runs_are_marked_stale() {
        // Regression guard: `pre-commit-checks` Lint last ran on a push to
        // main in Dec 2025 and failed; every run since has been a passing
        // pull_request. Reporting that failure forever is not actionable.
        let run = run(
            "Lint",
            "completed",
            Some("failure"),
            "push",
            "2025-12-13T13:42:49Z",
        );
        let reduced = reduce_runs(vec![run], &live(&["Lint"]));
        let latest = reduced.latest.first().expect("run retained");

        let now: DateTime<Utc> = "2026-08-10T00:00:00Z".parse().expect("timestamp");
        assert!(latest.is_stale(now), "an 8-month-old run must be stale");
    }

    #[test]
    fn recent_runs_are_not_stale() {
        let run = run(
            "CI",
            "completed",
            Some("failure"),
            "push",
            "2026-08-01T00:00:00Z",
        );
        let reduced = reduce_runs(vec![run], &live(&["CI"]));
        let latest = reduced.latest.first().expect("run retained");

        let now: DateTime<Utc> = "2026-08-10T00:00:00Z".parse().expect("timestamp");
        assert!(!latest.is_stale(now), "a 9-day-old failure is actionable");
    }

    #[test]
    fn staleness_boundary_is_exclusive() {
        let run = run(
            "CI",
            "completed",
            Some("failure"),
            "push",
            "2026-05-12T00:00:00Z",
        );
        let reduced = reduce_runs(vec![run], &live(&["CI"]));
        let latest = reduced.latest.first().expect("run retained");

        // Exactly 90 days is not yet stale; a second past it is.
        let at_horizon = latest.created_at + STALE_RUN_AGE;
        assert!(!latest.is_stale(at_horizon));
        assert!(latest.is_stale(at_horizon + chrono::TimeDelta::seconds(1)));
    }

    #[test]
    fn run_without_a_path_is_discarded() {
        let mut orphan = run(
            "CI",
            "completed",
            Some("failure"),
            "push",
            "2026-08-09T00:00:00Z",
        );
        orphan.path = None;
        assert!(reduce_runs(vec![orphan], &live(&["CI"])).latest.is_empty());
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
