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
//!
//! # Why no server-side filters
//!
//! Every query parameter that *filters* this endpoint -- `branch=`, `event=`
//! -- is answered from an index that is only eventually consistent with the
//! run history. It intermittently returns a **partial, stale, or empty** result
//! set, as a perfectly well-formed `200` with a `total_count` to match, so
//! there is nothing in the response that distinguishes it from a complete one.
//!
//! Measured against `fredsystems/freminal`'s `nightly.yml`, whose true newest
//! run was known independently, 20 samples per query shape:
//!
//! | Query shape                        | Correct | Wrong answers                |
//! | ---------------------------------- | ------- | ---------------------------- |
//! | `?branch=main`                     | 16/20   | runs up to 6 days stale      |
//! | `?event=schedule`                  | 13/20   | stale runs, and 3 empty      |
//! | `?per_page=100` (no filter)        | 20/20   | --                           |
//! | `/workflows/{id}/runs` (no filter) | 20/20   | --                           |
//!
//! And on `sdr-enthusiasts/docker-beast-splitter`, whose `?branch=main`
//! `total_count` is truly 264, fifty samples returned: 0, 23, 30, 92, 111,
//! 133, 135, 190, 192, 206, 264. Roughly one in ten was empty. The unfiltered
//! listing returned 341 on every sample of every repository tried.
//!
//! This was not a cosmetic problem. A truncated page that drops a workflow's
//! recent runs but keeps an older failing one makes that failure the newest run
//! the reducer can see, so the exporter published `conclusion="failure"` for a
//! green workflow -- `update-flakes` on `docker-beast-splitter` really ran
//! green on 07-26, 08-02, 08-09 and 08-16, and a truncated page kept only the
//! 07-12 failure. At 38 days that is inside [`STALE_RUN_AGE`], so it was not
//! even masked as `stale`; it paged. The same truncation regresses
//! `workflow_run_timestamp_seconds`, which fires the staleness alert, and a
//! page that omits a workflow altogether makes its series vanish for a cycle.
//!
//! So: **fetch unfiltered, filter here.** The branch and the event are decided
//! by [`reduce_runs`] against data the API is willing to report consistently.
//! [`RepoRuns::reconciled_with`] then closes the residual gap, because
//! "unfiltered is exact in every sample" is not the same as a guarantee.

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
    id: u64,
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
    /// GitHub's numeric workflow id.
    ///
    /// Carried so the per-workflow runs listing can be addressed unambiguously
    /// when [`fetch_runs`]'s single repository page does not reach far enough
    /// back to cover this workflow. The endpoint also accepts the file's
    /// basename, but an id needs no path handling and no URL escaping.
    ///
    /// Deliberately *not* `#[serde(default)]`: an entry persisted before this
    /// field existed must fail to decode and be refetched rather than
    /// deserialise to id 0 and 404 on every top-up.
    pub id: u64,
    pub name: String,
    pub path: String,
    pub state: WorkflowState,
    /// Events the workflow file currently declares under `on:`.
    ///
    /// Empty means unknown (not yet fetched, or unparsable), which is treated
    /// as "accept any event" so a lookup failure cannot blank a repository.
    #[serde(default)]
    pub triggers: Vec<String>,
}

impl Workflow {
    /// Whether this workflow currently declares `event` as a trigger.
    ///
    /// Note that `merge_group` never reaches this check. Merge-queue runs
    /// execute on a `gh-readonly-queue/...` branch, so the runs request's
    /// `branch=` filter excludes them before reduction -- verified against
    /// nixos and freminal, where none of their `merge_group` runs appear under
    /// `branch=main`. The post-merge `push` run is what reports branch state.
    fn declares(&self, event: &str) -> bool {
        if self.triggers.is_empty() {
            return true;
        }
        self.triggers.iter().any(|t| t == event)
    }

    /// What this workflow's runs can say about the default branch.
    ///
    /// Derived from the `on:` block, because the three cases need different
    /// treatment and nothing else distinguishes them. See
    /// [`DefaultBranchSignal`].
    ///
    /// An empty trigger list means the definition lookup failed. That is
    /// reported as [`DefaultBranchSignal::Cadenced`] -- the most permissive
    /// answer -- for the same reason [`Self::declares`] returns true: a
    /// transient contents error must not blank a repository's CI.
    #[must_use]
    pub fn default_branch_signal(&self) -> DefaultBranchSignal {
        if self.triggers.is_empty() {
            return DefaultBranchSignal::Cadenced;
        }
        let has = |event: &str| self.triggers.iter().any(|t| t == event);

        // Checked first: a workflow with `push` or `schedule` has a real
        // cadence regardless of what else it declares.
        if has("push") || has("schedule") {
            return DefaultBranchSignal::Cadenced;
        }
        // Checked before the dispatch events, and that order is the crux. A
        // workflow declaring both `pull_request` and `workflow_dispatch` does
        // its real work on pull requests; a dispatch against the default
        // branch is an operator action that nothing repeats. Treating it as
        // on-demand would keep the fossil.
        if has("pull_request") || has("pull_request_target") || has("merge_group") {
            return DefaultBranchSignal::None;
        }
        if has("workflow_dispatch") || has("repository_dispatch") {
            return DefaultBranchSignal::OnDemand;
        }
        // `workflow_call` and anything unrecognised. Permissive, as above.
        DefaultBranchSignal::Cadenced
    }
}

/// What a workflow's runs can say about the state of the default branch.
///
/// Collapsing these three into one was the cause of two long-standing false
/// positives, in opposite directions, and they cannot be told apart from run
/// history alone -- only from the `on:` block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefaultBranchSignal {
    /// GitHub runs it against the default branch unprompted, via `push` or
    /// `schedule`. Both its conclusion and the gap since its last run are
    /// meaningful, so the staleness horizon applies.
    Cadenced,

    /// It runs against the default branch, but only when asked --
    /// `workflow_dispatch` or `repository_dispatch`. Its conclusion is
    /// meaningful; the time since the last run is not, because there is no
    /// cadence to be late against.
    ///
    /// This is how the fleet actually deploys: 36 workflows in
    /// `sdr-enthusiasts`, including every `Deploy`, are dispatched by another
    /// repository through the API. Reporting one as stale because nobody has
    /// released in three months is a category error.
    OnDemand,

    /// It never runs against the default branch: `pull_request` and
    /// `pull_request_target` run on the PR's head, `merge_group` on a
    /// `gh-readonly-queue/...` branch. It has no default-branch state at all,
    /// and its health is already covered by the pull-request series.
    ///
    /// Such a workflow acquires default-branch history only if someone
    /// dispatches it once, and nothing ever supersedes that run. Observed on
    /// `sdr-enthusiasts/sdr-e-base-repo-setup`'s `Lint`: one dispatch against
    /// `main` in December, 37 minutes after the file was created and five
    /// edits ago, reported ever since as that workflow's CI state even though
    /// it runs and passes on every pull request. It could not have passed
    /// either -- it runs `pre-commit run --all-files`, whose
    /// `no-commit-to-branch` hook fails by construction on a protected branch.
    ///
    /// 89 workflows in `sdr-enthusiasts` are in this bucket and 86 of them
    /// declare `workflow_dispatch`, so this is a latent trap rather than one
    /// repository's accident.
    None,
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
                    id: w.id,
                    name: w.name,
                    state: WorkflowState::from_api(&w.state),
                    path: w.path,
                    // Populated by `resolve_definitions`, which reads the file.
                    triggers: Vec::new(),
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
    /// Branch the run executed against.
    ///
    /// Read because the branch is now selected here rather than by the API; see
    /// this module's "Why no server-side filters". Nullable in the API, and a
    /// run with no branch cannot be attributed to the default branch.
    #[serde(default)]
    head_branch: Option<String>,
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

impl RepoRuns {
    /// Whether this reduction reported anything for the workflow named `name`.
    ///
    /// "Nothing" is genuinely ambiguous: either the workflow has never run
    /// against the default branch, or the page simply did not reach back far
    /// enough. The caller resolves it with a per-workflow fetch, which cannot
    /// be diluted by sibling workflows and so distinguishes the two.
    #[must_use]
    pub fn covers(&self, name: &str) -> bool {
        self.latest.iter().any(|run| run.workflow == name)
    }

    /// Folds `self` over the previous cycle's reduction, keeping whichever
    /// observation of each workflow is newer.
    ///
    /// Run history is append-only: a workflow's newest run can move forward in
    /// time or stay put, but it cannot move backwards, and it cannot cease to
    /// exist while the workflow file does. Nothing in a single response asserts
    /// either of those, so a response that violates them is not news -- it is
    /// evidence that the response is incomplete, and the previous observation is
    /// the better one.
    ///
    /// That distinction is the whole point. GitHub's runs listing is only
    /// eventually consistent (see this module's "Why no server-side filters"),
    /// and an incomplete page is indistinguishable from a complete one on its
    /// own terms. Comparing across cycles is the only signal available, and it
    /// converts every truncation into a lost update -- one cycle of staleness --
    /// rather than a fabricated regression that pages.
    ///
    /// Two rules, both gated on the workflow still being live so that deleting a
    /// workflow still retires its series:
    ///
    /// * A workflow present in both keeps the observation with the later
    ///   `created_at`. Ties go to `self`, because a re-run of an existing run
    ///   keeps its original `created_at` while its conclusion changes, and that
    ///   is a real update that must not be discarded.
    /// * A workflow present only in `previous` is carried forward.
    ///
    /// `last_success` is merged the same way, per workflow, keeping the later
    /// timestamp.
    #[must_use]
    pub fn reconciled_with(mut self, previous: &Self, live: &[Workflow]) -> Self {
        let live_names: std::collections::HashSet<&str> =
            live.iter().map(|w| w.name.as_str()).collect();

        for old in &previous.latest {
            if !live_names.contains(old.workflow.as_str()) {
                continue;
            }
            match self
                .latest
                .iter_mut()
                .find(|run| run.workflow == old.workflow)
            {
                Some(fresh) if fresh.created_at < old.created_at => {
                    debug!(
                        workflow = old.workflow,
                        fresh = %fresh.created_at,
                        retained = %old.created_at,
                        "discarding a run older than one already observed; the response is incomplete"
                    );
                    *fresh = old.clone();
                }
                Some(_) => {}
                None => self.latest.push(old.clone()),
            }
        }

        for (workflow, at) in &previous.last_success {
            if !live_names.contains(workflow.as_str()) {
                continue;
            }
            self.last_success
                .entry(workflow.clone())
                .and_modify(|existing| {
                    if at > existing {
                        *existing = *at;
                    }
                })
                .or_insert(*at);
        }

        self.latest.sort_by(|a, b| a.workflow.cmp(&b.workflow));
        self
    }
}

/// Fetches one page of the repository's run history and reduces it to the
/// latest default-branch run per workflow.
///
/// `live` is the current workflow set from [`list_workflows`]; runs belonging
/// to workflows absent from it are discarded as orphaned history.
///
/// The request carries **no filters** -- see this module's "Why no server-side
/// filters" for the measurements behind that. The consequence is that this one
/// page is shared between the default branch and every pull request, so on a
/// busy repository it can be dominated by PR runs and fail to reach back far
/// enough to cover an infrequent scheduled workflow. That is what
/// [`fetch_workflow_runs`] is for; [`RepoRuns::covers`] reports which workflows
/// this page actually spoke for.
///
/// # Errors
/// Returns [`ClientError`] if the request fails.
pub async fn fetch_runs(
    client: &Client,
    repo: &Repo,
    live: &[Workflow],
) -> Result<RepoRuns, ClientError> {
    let path = format!(
        "/repos/{}/{}/actions/runs?per_page={RUNS_PER_PAGE}",
        repo.owner, repo.name
    );
    let (runs, outcome) = fetch_reduced(client, repo, &path, live).await?;
    debug!(repo = %repo, ?outcome, workflows = runs.latest.len(), "fetched runs");
    Ok(runs)
}

/// Fetches one workflow's own run history and reduces it the same way.
///
/// Addresses `/actions/workflows/{id}/runs`, again unfiltered. Used to top up
/// the workflows [`fetch_runs`]'s shared page did not reach. Because a
/// workflow's own history is not diluted by its siblings, one page of it always
/// covers the workflow's newest default-branch run if one exists at all.
///
/// # Errors
/// Returns [`ClientError`] if the request fails.
pub async fn fetch_workflow_runs(
    client: &Client,
    repo: &Repo,
    workflow: &Workflow,
) -> Result<RepoRuns, ClientError> {
    let path = format!(
        "/repos/{}/{}/actions/workflows/{}/runs?per_page={RUNS_PER_PAGE}",
        repo.owner, repo.name, workflow.id
    );
    let live = std::slice::from_ref(workflow);
    let (runs, outcome) = fetch_reduced(client, repo, &path, live).await?;
    debug!(
        repo = %repo,
        workflow = workflow.path,
        ?outcome,
        found = runs.latest.len(),
        "topped up runs for a workflow the repository page did not cover"
    );
    Ok(runs)
}

/// Everything a run reduction depends on besides the API response itself.
///
/// A reduction is only meaningful under the inputs that produced it, and it is
/// retained in two places: the `ETag` cache, and the collector's last-known-good
/// state for [`RepoRuns::reconciled_with`]. **Both** have to be invalidated when
/// these change, and for the same reason in each case -- a retained reduction
/// that outlives its inputs is a fossil that the current code would never have
/// produced.
///
/// * The **workflow fingerprint** covers path, display name, and triggers.
///   Deleting, renaming, or retriggering a workflow does not change the runs
///   listing, so the request would answer `304` and replay a reduction computed
///   against the old workflow set -- reviving the orphaned-run and
///   superseded-trigger bugs the reduction exists to prevent.
/// * The **branch** is a reduction input now rather than a request parameter, so
///   a repository that renames its default branch must not go on reporting runs
///   selected against the old one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReductionInputs {
    branch: String,
    workflows: u64,
}

impl ReductionInputs {
    /// The inputs a reduction of `repo`'s runs against `live` depends on.
    #[must_use]
    pub fn of(repo: &Repo, live: &[Workflow]) -> Self {
        Self {
            branch: repo.default_branch.clone(),
            workflows: fingerprint_workflows(live),
        }
    }

    /// A cache key for `path` that changes whenever these inputs do.
    fn cache_key(&self, path: &str) -> String {
        format!("{path}#branch={}#wf={}", self.branch, self.workflows)
    }
}

/// Issues a runs request and reduces it, with the cache keyed by every input
/// the reduction reads.
async fn fetch_reduced(
    client: &Client,
    repo: &Repo,
    path: &str,
    live: &[Workflow],
) -> Result<(RepoRuns, super::client::CacheOutcome), ClientError> {
    let cache_key = ReductionInputs::of(repo, live).cache_key(path);
    let branch = repo.default_branch.clone();
    client
        .get_cached_as(&cache_key, path, move |response: RunsResponse| {
            reduce_runs(response.workflow_runs, live, &branch)
        })
        .await
}

/// A stable fingerprint of the workflow set's identities and display names.
///
/// Order-independent so an API reordering cannot spuriously invalidate the
/// cache, and it covers names as well as paths because a rename must change
/// the reduction's output labels.
fn fingerprint_workflows(live: &[Workflow]) -> u64 {
    use std::hash::{Hash as _, Hasher as _};

    // Must cover every input the reduction reads: path and name decide
    // identity and labels, and triggers decide which runs survive. Omitting
    // triggers would let a trigger-only change keep the same cache key, so the
    // 304 on an unchanged runs listing would replay a reduction computed
    // against the old trigger set -- the same stale-reduction bug the
    // fingerprint exists to prevent.
    let mut entries: Vec<(&str, &str, Vec<&str>)> = live
        .iter()
        .map(|w| {
            let mut triggers: Vec<&str> = w.triggers.iter().map(String::as_str).collect();
            // Sorted so an API or file reordering cannot spuriously invalidate.
            triggers.sort_unstable();
            (w.path.as_str(), w.name.as_str(), triggers)
        })
        .collect();
    entries.sort_unstable();

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for (path, name, triggers) in entries {
        path.hash(&mut hasher);
        name.hash(&mut hasher);
        triggers.hash(&mut hasher);
    }
    hasher.finish()
}

/// Whether a run's trigger reflects the state of the default branch.
///
/// Only these events describe "is the branch healthy right now":
///
/// * `push` -- code landed on the branch.
/// * `schedule` / `workflow_dispatch` -- ran against the branch as it stands.
///
/// `pull_request` runs are excluded even though their `head_branch` can equal
/// the default branch -- that happens whenever the pull request is opened from
/// a fork's own default branch. A merged PR's last pre-merge failure would
/// otherwise be reported as the branch's current CI state forever, which was
/// observed on both `fredsystems/pre-commit-checks` and
/// `sdr-enthusiasts/docker-planefence`. Post-merge health is covered by the
/// `push` run that merging produces.
///
/// This filter is also what makes matching on `head_branch` alone safe. Of the
/// three events kept here, none can originate from another repository: a `push`
/// and a `schedule` are raised by the repository itself, and a
/// `workflow_dispatch` targets a ref within it. So a surviving run whose
/// `head_branch` is the default branch really did run against the default
/// branch of *this* repository.
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
///
/// Note that while *lookup* is by path, the output is keyed by display name,
/// because that is the label operators recognise on a dashboard. Two workflow
/// files declaring the same `name:` therefore collapse into one series. That
/// is accepted: it does not occur in the monitored organisations, and keying
/// metrics by file path would make every dashboard and alert harder to read.
fn reduce_runs(runs: Vec<RunEntry>, live: &[Workflow], default_branch: &str) -> RepoRuns {
    // Workflow identity is the file path, not the display name: a run created
    // before the workflow gained a `name:` reports the path in the name field,
    // and a renamed workflow would otherwise split into two series.
    let live_by_path: HashMap<&str, &Workflow> =
        live.iter().map(|w| (w.path.as_str(), w)).collect();

    let mut latest: HashMap<String, LatestRun> = HashMap::new();
    let mut last_success: HashMap<String, DateTime<Utc>> = HashMap::new();

    for run in runs {
        // The branch is selected here, not by the API. See this module's "Why
        // no server-side filters": `?branch=` answers from an index that
        // intermittently omits recent runs, which turned an old failure into
        // "the newest run" and paged for a green workflow.
        if run.head_branch.as_deref() != Some(default_branch) {
            continue;
        }
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
        // Run history outlives a trigger change. A workflow that used to run
        // on push and now runs only on pull_request keeps its old push runs
        // forever; reporting them describes a configuration that no longer
        // exists. Observed on frext and bike-fitter-1000, both of which showed
        // months-old push failures for a ci.yml that declares only
        // pull_request.
        if !workflow.declares(&run.event) {
            continue;
        }
        // A workflow GitHub never runs against the default branch has no
        // default-branch state, so a one-off dispatch must not become one.
        if workflow.default_branch_signal() == DefaultBranchSignal::None {
            continue;
        }
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
pub async fn fetch_workflow_definition(
    client: &Client,
    repo: &Repo,
    workflow_path: &str,
) -> Result<WorkflowDefinition, ClientError> {
    let path = format!(
        "/repos/{}/{}/contents/{workflow_path}?ref={}",
        repo.owner, repo.name, repo.default_branch
    );
    let (definition, _) = client
        .get_cached(&path, |response: ContentResponse| {
            if response.encoding == "base64" {
                let yaml = decode_base64(&response.content);
                WorkflowDefinition {
                    crons: parse_crons(&yaml),
                    triggers: parse_triggers(&yaml),
                }
            } else {
                WorkflowDefinition::default()
            }
        })
        .await?;
    Ok(definition)
}

/// What a workflow file currently declares.
///
/// `triggers` exists because run history outlives a trigger change. `frext`
/// and `bike-fitter-1000` both had failing `push` runs from a period when
/// their `ci.yml` ran on push; the file now declares only `pull_request`, but
/// GitHub keeps the old runs forever. Reporting them describes a configuration
/// that no longer exists.
///
/// An empty `triggers` set means the file could not be parsed, and is treated
/// as "accept any event" so a parse failure degrades to the previous
/// behaviour rather than blanking a repository's CI.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    pub crons: Vec<String>,
    pub triggers: Vec<String>,
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

/// Extracts the event names from a workflow's `on:` block.
///
/// Handles all three forms GitHub accepts:
///
/// ```yaml
/// on: push                      # scalar
/// on: [push, pull_request]       # sequence
/// on:                           # mapping
///   push:
///     branches: [main]
/// ```
fn parse_triggers(yaml: &str) -> Vec<String> {
    let Ok(document) = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(yaml) else {
        return Vec::new();
    };
    // `on` is a YAML 1.1 boolean, so some parsers surface this key as `true`.
    let Some(triggers) = document
        .get("on")
        .or_else(|| document.get(serde_yaml_ng::Value::Bool(true)))
    else {
        return Vec::new();
    };

    if let Some(one) = triggers.as_str() {
        return vec![one.to_owned()];
    }
    if let Some(seq) = triggers.as_sequence() {
        return seq
            .iter()
            .filter_map(|v| v.as_str().map(str::to_owned))
            .collect();
    }
    if let Some(map) = triggers.as_mapping() {
        return map
            .keys()
            .filter_map(|k| k.as_str().map(str::to_owned))
            .collect();
    }
    Vec::new()
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
            head_branch: Some(DEFAULT_BRANCH.to_owned()),
            status: status.to_owned(),
            conclusion: conclusion.map(str::to_owned),
            event: event.to_owned(),
            created_at: created.parse().expect("valid timestamp"),
            html_url: format!("https://github.com/o/r/actions/runs/{name}"),
        }
    }

    /// The default branch every test reduces against.
    const DEFAULT_BRANCH: &str = "main";

    /// [`reduce_runs`] against [`DEFAULT_BRANCH`].
    ///
    /// The branch is now a reduction input rather than a request parameter, and
    /// threading a literal through every case would say nothing.
    fn reduce(runs: Vec<RunEntry>, live: &[Workflow]) -> RepoRuns {
        reduce_runs(runs, live, DEFAULT_BRANCH)
    }

    /// Declares workflows as currently present in the repository.
    fn live(names: &[&str]) -> Vec<Workflow> {
        names
            .iter()
            .enumerate()
            .map(|(i, n)| Workflow {
                id: u64::try_from(i).unwrap_or(0) + 1,
                name: (*n).to_owned(),
                path: format!(".github/workflows/{n}.yml"),
                state: WorkflowState::Active,
                // Empty = accept any event, so existing tests are unaffected.
                triggers: Vec::new(),
            })
            .collect()
    }

    /// One workflow with an explicit trigger list.
    fn live_with_triggers(name: &str, triggers: &[&str]) -> Vec<Workflow> {
        vec![Workflow {
            id: 1,
            name: name.to_owned(),
            path: format!(".github/workflows/{name}.yml"),
            state: WorkflowState::Active,
            triggers: triggers.iter().map(|t| (*t).to_owned()).collect(),
        }]
    }

    fn signal_of(triggers: &[&str]) -> DefaultBranchSignal {
        live_with_triggers("W", triggers)[0].default_branch_signal()
    }

    #[test]
    fn cadence_bearing_triggers_are_classified_cadenced() {
        for t in [
            vec!["push"],
            vec!["schedule"],
            vec!["push", "pull_request"],
            vec!["schedule", "workflow_dispatch"],
        ] {
            assert_eq!(
                signal_of(&t),
                DefaultBranchSignal::Cadenced,
                "{t:?} has a real cadence"
            );
        }
    }

    #[test]
    fn dispatch_only_triggers_are_classified_on_demand() {
        // The fleet's deployment mechanism: dispatched through the API by
        // another repository, so the conclusion matters but the gap does not.
        for t in [
            vec!["workflow_dispatch"],
            vec!["repository_dispatch"],
            vec!["workflow_dispatch", "repository_dispatch"],
        ] {
            assert_eq!(signal_of(&t), DefaultBranchSignal::OnDemand, "{t:?}");
        }
    }

    #[test]
    fn pull_request_triggers_beat_dispatch_and_yield_no_signal() {
        // The ordering is the crux. `Lint` declares merge_group,
        // pull_request, and workflow_dispatch; classifying it as on-demand
        // because of the dispatch would keep its fossil run alive.
        for t in [
            vec!["pull_request"],
            vec!["merge_group"],
            vec!["merge_group", "pull_request", "workflow_dispatch"],
            vec!["pull_request", "workflow_dispatch"],
        ] {
            assert_eq!(signal_of(&t), DefaultBranchSignal::None, "{t:?}");
        }
    }

    #[test]
    fn an_unknown_trigger_list_is_permissive() {
        // Empty means the definition lookup failed. Anything other than the
        // most permissive answer would let a transient error blank a repo.
        assert_eq!(signal_of(&[]), DefaultBranchSignal::Cadenced);
        assert_eq!(signal_of(&["workflow_call"]), DefaultBranchSignal::Cadenced);
    }

    #[test]
    fn a_workflow_that_never_runs_on_the_default_branch_reports_nothing() {
        // Regression guard, from sdr-e-base-repo-setup's `Lint`: it runs and
        // passes on every pull request, but one dispatch against main in
        // December failed and was reported as its CI state for 241 days.
        let runs = vec![run(
            "Lint",
            "completed",
            Some("failure"),
            "workflow_dispatch",
            "2025-12-13T15:55:22Z",
        )];

        let reduced = reduce(
            runs,
            &live_with_triggers(
                "Lint",
                &["merge_group", "pull_request", "workflow_dispatch"],
            ),
        );

        assert!(
            reduced.latest.is_empty(),
            "a PR-only workflow has no default-branch state: {:?}",
            reduced.latest
        );
    }

    #[test]
    fn a_dispatch_driven_workflow_keeps_its_runs() {
        // The other direction, and the one that must not regress: every
        // `Deploy` in the fleet is dispatch-only, and dropping these would
        // blind 36 workflows.
        let runs = vec![run(
            "Deploy",
            "completed",
            Some("success"),
            "workflow_dispatch",
            "2026-05-04T20:10:34Z",
        )];

        let reduced = reduce(runs, &live_with_triggers("Deploy", &["workflow_dispatch"]));

        assert_eq!(reduced.latest.len(), 1, "dispatch-driven CI is still CI");
        assert_eq!(reduced.latest[0].conclusion, RunConclusion::Success);
    }

    /// A run on a branch other than the default one.
    fn run_on_branch(name: &str, branch: Option<&str>, created: &str) -> RunEntry {
        let mut entry = run(name, "completed", Some("failure"), "push", created);
        entry.head_branch = branch.map(str::to_owned);
        entry
    }

    #[test]
    fn discards_runs_from_other_branches() {
        // The branch is selected here now, not by the API, because `?branch=`
        // is answered from an index that intermittently omits recent runs.
        // Every run the unfiltered listing returns therefore arrives, including
        // every feature branch's.
        let runs = vec![
            run_on_branch("CI", Some("feature/x"), "2026-08-09T00:00:00Z"),
            run_on_branch(
                "CI",
                Some("gh-readonly-queue/main/pr-1"),
                "2026-08-08T00:00:00Z",
            ),
        ];
        assert!(
            reduce(runs, &live(&["CI"])).latest.is_empty(),
            "only the default branch describes default-branch state"
        );
    }

    #[test]
    fn a_run_with_no_branch_is_discarded() {
        // `head_branch` is nullable, and a run that cannot be attributed to a
        // branch cannot be attributed to the default one.
        let runs = vec![run_on_branch("CI", None, "2026-08-09T00:00:00Z")];
        assert!(reduce(runs, &live(&["CI"])).latest.is_empty());
    }

    #[test]
    fn a_newer_run_on_another_branch_does_not_mask_the_default_branch() {
        // The shape that matters: the unfiltered listing is dominated by branch
        // activity, so the newest run of a workflow is usually *not* on the
        // default branch. Selecting per workflow before filtering by branch
        // would report nothing at all for a healthy repository.
        let runs = vec![
            run_on_branch("CI", Some("feature/x"), "2026-08-19T00:00:00Z"),
            run(
                "CI",
                "completed",
                Some("success"),
                "push",
                "2026-08-09T00:00:00Z",
            ),
        ];
        let reduced = reduce(runs, &live(&["CI"]));

        assert_eq!(reduced.latest.len(), 1);
        assert_eq!(reduced.latest[0].conclusion, RunConclusion::Success);
        assert_eq!(
            reduced.latest[0].created_at.to_rfc3339(),
            "2026-08-09T00:00:00+00:00"
        );
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
        let reduced = reduce(runs, &live(&["CI", "Deploy"]));

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
        let reduced = reduce(runs, &live(&["CI", "Deploy"]));
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
        let reduced = reduce(runs, &live(&["CI", "Deploy"]));

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
        let reduced = reduce(runs, &live(&["Lint"]));

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
            let reduced = reduce(runs, &live(&["CI"]));
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
            let reduced = reduce(runs, &live(&["CI"]));
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
        let reduced = reduce(runs, &live(&["CI", "Deploy"]));

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
        let reduced = reduce(runs, &live(&["CI", "Deploy"]));
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
        let reduced = reduce(runs, &live(&["CI"]));

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
        let reduced = reduce(runs, &live(&["CI"]));

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
                id: 1,
                name: "Deploy".to_owned(),
                path: ".github/workflows/deploy.yml".to_owned(),
                state: WorkflowState::Active,
                triggers: Vec::new(),
            },
            Workflow {
                id: 1,
                name: "Lint".to_owned(),
                path: ".github/workflows/lint.yaml".to_owned(),
                state: WorkflowState::Active,
                triggers: Vec::new(),
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
        let reduced = reduce(runs, &workflows);

        assert_eq!(reduced.latest.len(), 2, "both extensions must be kept");
    }

    #[test]
    fn disabled_workflows_still_report_their_runs() {
        // Regression guard: filtering to state=="active" made a failing
        // `update-flakes` vanish from the metrics after GitHub auto-disabled
        // it for repository inactivity -- the exact condition worth alerting
        // on was the one being hidden.
        let workflows = vec![Workflow {
            id: 1,
            name: "update-flakes".to_owned(),
            path: ".github/workflows/update-flakes.yaml".to_owned(),
            state: WorkflowState::DisabledInactivity,
            triggers: Vec::new(),
        }];
        let runs = vec![run_at(
            "update-flakes",
            ".github/workflows/update-flakes.yaml",
            "completed",
            Some("failure"),
            "schedule",
            "2026-08-01T00:47:26Z",
        )];
        let reduced = reduce(runs, &workflows);

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
        let reduced = reduce(vec![run], &live(&["Lint"]));
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
        let reduced = reduce(vec![run], &live(&["CI"]));
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
        let reduced = reduce(vec![run], &live(&["CI"]));
        let latest = reduced.latest.first().expect("run retained");

        // Exactly 90 days is not yet stale; a second past it is.
        let at_horizon = latest.created_at + STALE_RUN_AGE;
        assert!(!latest.is_stale(at_horizon));
        assert!(latest.is_stale(at_horizon + chrono::TimeDelta::seconds(1)));
    }

    #[test]
    fn fingerprint_changes_when_a_workflow_is_deleted() {
        // Regression guard: deleting a workflow does not change the runs
        // listing, so the request answers 304. Without the fingerprint in the
        // cache key the stale reduction is replayed and the deleted
        // workflow's runs come back.
        let before = live(&["CI", "Deploy"]);
        let after = live(&["CI"]);
        assert_ne!(
            fingerprint_workflows(&before),
            fingerprint_workflows(&after)
        );
    }

    #[test]
    fn fingerprint_changes_when_a_workflow_is_renamed() {
        let before = vec![Workflow {
            id: 1,
            name: "CI".to_owned(),
            path: ".github/workflows/ci.yml".to_owned(),
            state: WorkflowState::Active,
            triggers: Vec::new(),
        }];
        let after = vec![Workflow {
            id: 1,
            name: "Build".to_owned(),
            path: ".github/workflows/ci.yml".to_owned(),
            state: WorkflowState::Active,
            triggers: Vec::new(),
        }];
        assert_ne!(
            fingerprint_workflows(&before),
            fingerprint_workflows(&after),
            "a rename changes the output labels and must invalidate the cache"
        );
    }

    #[test]
    fn fingerprint_changes_when_only_triggers_change() {
        // Regression guard: reduce_runs filters on triggers, so they are part
        // of the reduction's input. A trigger-only edit leaves the runs
        // listing unchanged, so the request answers 304; without triggers in
        // the key the stale reduction would be replayed and runs from the old
        // trigger set would reappear.
        let before = wf("CI", &["push", "pull_request"]);
        let after = wf("CI", &["pull_request"]);
        assert_ne!(
            fingerprint_workflows(&before),
            fingerprint_workflows(&after)
        );
    }

    #[test]
    fn fingerprint_ignores_trigger_ordering() {
        // The `on:` mapping order is incidental; reordering it must not force
        // an uncached refetch of every repository.
        let a = wf("CI", &["push", "pull_request", "merge_group"]);
        let b = wf("CI", &["merge_group", "push", "pull_request"]);
        assert_eq!(fingerprint_workflows(&a), fingerprint_workflows(&b));
    }

    #[test]
    fn fingerprint_distinguishes_empty_from_populated_triggers() {
        // Empty means "unknown, accept anything", which is a materially
        // different reduction from an explicit single trigger.
        assert_ne!(
            fingerprint_workflows(&wf("CI", &[])),
            fingerprint_workflows(&wf("CI", &["push"]))
        );
    }

    #[test]
    fn fingerprint_is_order_independent() {
        // An API reordering must not spuriously invalidate the cache and
        // force a full uncached sweep.
        let a = live(&["CI", "Deploy", "Lint"]);
        let mut b = a.clone();
        b.reverse();
        assert_eq!(fingerprint_workflows(&a), fingerprint_workflows(&b));
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
        assert!(reduce(vec![orphan], &live(&["CI"])).latest.is_empty());
    }

    fn wf(name: &str, triggers: &[&str]) -> Vec<Workflow> {
        vec![Workflow {
            id: 1,
            name: name.to_owned(),
            path: format!(".github/workflows/{name}.yml"),
            state: WorkflowState::Active,
            triggers: triggers.iter().map(|t| (*t).to_owned()).collect(),
        }]
    }

    #[test]
    fn discards_runs_from_a_superseded_trigger() {
        // Regression guard: frext and bike-fitter-1000 both showed failing
        // `push` runs for a ci.yml that declares only `pull_request`. The
        // workflow used to run on push; GitHub keeps those runs forever, so
        // reporting them describes a configuration that no longer exists.
        let runs = vec![run(
            "CI",
            "completed",
            Some("failure"),
            "push",
            "2026-06-29T18:23:03Z",
        )];
        let reduced = reduce(runs, &wf("CI", &["pull_request", "merge_group"]));
        assert!(
            reduced.latest.is_empty(),
            "a push run must be dropped when the workflow no longer runs on push"
        );
    }

    #[test]
    fn keeps_runs_whose_event_is_still_declared() {
        let runs = vec![run(
            "CI",
            "completed",
            Some("failure"),
            "push",
            "2026-08-09T00:00:00Z",
        )];
        let reduced = reduce(runs, &wf("CI", &["push", "pull_request"]));
        assert_eq!(reduced.latest.len(), 1);
    }

    #[test]
    fn unknown_triggers_accept_any_event() {
        // An empty trigger list means the file could not be fetched or parsed.
        // That must degrade to the previous behaviour, not blank the repo.
        let runs = vec![run(
            "CI",
            "completed",
            Some("failure"),
            "push",
            "2026-08-09T00:00:00Z",
        )];
        let reduced = reduce(runs, &wf("CI", &[]));
        assert_eq!(reduced.latest.len(), 1);
    }

    #[test]
    fn parses_triggers_from_all_three_yaml_forms() {
        assert_eq!(parse_triggers("on: push\njobs: {}\n"), ["push"]);
        assert_eq!(
            parse_triggers("on: [push, pull_request]\njobs: {}\n"),
            ["push", "pull_request"]
        );
        let mapping = r"
on:
  pull_request:
  merge_group:
jobs: {}
";
        assert_eq!(parse_triggers(mapping), ["pull_request", "merge_group"]);
    }

    #[test]
    fn parses_triggers_from_a_real_frext_style_workflow() {
        // The exact shape that caused the false positive.
        let yaml = r"
name: CI
on:
  pull_request:

permissions:
  contents: read
jobs:
  build:
    runs-on: ubuntu-latest
";
        let triggers = parse_triggers(yaml);
        assert_eq!(triggers, ["pull_request"]);
        assert!(!triggers.iter().any(|t| t == "push"));
    }

    #[test]
    fn malformed_yaml_yields_no_triggers() {
        // Which means "accept anything", the safe direction.
        assert!(parse_triggers("this: is: not: valid: yaml:").is_empty());
        assert!(parse_triggers("").is_empty());
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

    // ---- coverage and reconciliation -------------------------------------
    //
    // Guarding the defect these two exist for: GitHub's runs listing is only
    // eventually consistent, and an incomplete page is a well-formed 200 that
    // looks exactly like a complete one.

    /// A reduction asserting `workflow`'s newest run.
    fn reduction(workflow: &str, conclusion: RunConclusion, created: &str) -> RepoRuns {
        let created_at: DateTime<Utc> = created.parse().expect("valid timestamp");
        RepoRuns {
            latest: vec![LatestRun {
                workflow: workflow.to_owned(),
                conclusion,
                event: "schedule".to_owned(),
                created_at,
                html_url: String::new(),
            }],
            last_success: HashMap::new(),
        }
    }

    fn newest_of(runs: &RepoRuns, workflow: &str) -> Option<(RunConclusion, String)> {
        runs.latest
            .iter()
            .find(|r| r.workflow == workflow)
            .map(|r| (r.conclusion, r.created_at.to_rfc3339()))
    }

    #[test]
    fn covers_reports_whether_a_workflow_was_spoken_for() {
        let runs = reduction(
            "update-flakes",
            RunConclusion::Success,
            "2026-08-16T05:31:31Z",
        );
        assert!(runs.covers("update-flakes"));
        assert!(
            !runs.covers("Deploy"),
            "a workflow the page never mentioned is not covered"
        );
    }

    #[test]
    fn a_truncated_response_cannot_resurrect_an_older_failure() {
        // THE regression guard for this module.
        //
        // `update-flakes` on sdr-enthusiasts/docker-beast-splitter ran green on
        // main on 07-26, 08-02, 08-09 and 08-16. A `?branch=main` page that
        // dropped all four but kept the 07-12 failure made that failure the
        // newest run the reducer could see, so the exporter published
        // conclusion="failure" for a green workflow -- and at 38 days it was
        // inside STALE_RUN_AGE, so it was not even masked as `stale`. It paged.
        //
        // Run history is append-only, so a newest run that moved *backwards* is
        // not news; it is proof the response was incomplete.
        let known = reduction(
            "update-flakes",
            RunConclusion::Success,
            "2026-08-16T05:31:31Z",
        );
        let truncated = reduction(
            "update-flakes",
            RunConclusion::Failure,
            "2026-07-12T07:30:11Z",
        );

        let reconciled = truncated.reconciled_with(&known, &live(&["update-flakes"]));

        assert_eq!(
            newest_of(&reconciled, "update-flakes"),
            Some((
                RunConclusion::Success,
                "2026-08-16T05:31:31+00:00".to_owned()
            )),
            "an older observation must not overwrite a newer one"
        );
    }

    #[test]
    fn a_workflow_missing_from_a_truncated_response_is_carried_forward() {
        // Observed live: a `?branch=main` page with total_count=23 that
        // contained no update-flakes runs at all. Because each cycle publishes a
        // fresh registry, an omitted workflow silently loses its series -- which
        // is the "workflows appear for a round or two and then vanish" shape.
        let known = reduction(
            "update-flakes",
            RunConclusion::Success,
            "2026-08-16T05:31:31Z",
        );
        let empty = RepoRuns::default();

        let reconciled = empty.reconciled_with(&known, &live(&["update-flakes"]));

        assert_eq!(
            newest_of(&reconciled, "update-flakes"),
            Some((
                RunConclusion::Success,
                "2026-08-16T05:31:31+00:00".to_owned()
            )),
            "an empty response is a lost update, not a deletion"
        );
    }

    #[test]
    fn a_genuinely_newer_run_still_wins() {
        // The guard must not freeze state. This is the direction that carries
        // every real CI transition, so getting it wrong would be worse than the
        // bug being fixed.
        let known = reduction("CI", RunConclusion::Success, "2026-08-09T00:00:00Z");
        let fresh = reduction("CI", RunConclusion::Failure, "2026-08-19T00:00:00Z");

        let reconciled = fresh.reconciled_with(&known, &live(&["CI"]));

        assert_eq!(
            newest_of(&reconciled, "CI"),
            Some((
                RunConclusion::Failure,
                "2026-08-19T00:00:00+00:00".to_owned()
            )),
            "a real failure must still be published"
        );
    }

    #[test]
    fn a_rerun_at_the_same_timestamp_updates_the_conclusion() {
        // A re-run keeps the original run's `created_at` and changes only its
        // conclusion, so ties must go to the fresh observation. Comparing with
        // `>=` instead of `>` would pin a re-run's outcome to the failure it was
        // re-run to clear, and no later cycle could ever revise it.
        let known = reduction("CI", RunConclusion::Failure, "2026-08-19T00:00:00Z");
        let rerun = reduction("CI", RunConclusion::Success, "2026-08-19T00:00:00Z");

        let reconciled = rerun.reconciled_with(&known, &live(&["CI"]));

        assert_eq!(
            newest_of(&reconciled, "CI").map(|(c, _)| c),
            Some(RunConclusion::Success),
            "a re-run's new conclusion must land"
        );
    }

    #[test]
    fn a_deleted_workflow_is_not_carried_forward() {
        // Otherwise the guard would become a way to keep a removed workflow's
        // final failure alive forever -- the orphaned-run bug, reintroduced
        // through the back door.
        let known = reduction(
            "Update pre-commit hooks",
            RunConclusion::Failure,
            "2025-12-14T00:54:24Z",
        );

        let reconciled = RepoRuns::default().reconciled_with(&known, &live(&["CI"]));

        assert!(
            reconciled.latest.is_empty(),
            "a workflow whose file is gone must retire: {:?}",
            reconciled.latest
        );
    }

    #[test]
    fn reconciliation_keeps_the_later_last_success() {
        // `last_success` feeds "how long has this been broken", so a truncated
        // page must not move it backwards either.
        let mut known = RepoRuns::default();
        known.last_success.insert(
            "CI".to_owned(),
            "2026-08-16T00:00:00Z".parse().expect("timestamp"),
        );
        let mut truncated = RepoRuns::default();
        truncated.last_success.insert(
            "CI".to_owned(),
            "2026-07-01T00:00:00Z".parse().expect("timestamp"),
        );

        let reconciled = truncated.reconciled_with(&known, &live(&["CI"]));

        assert_eq!(
            reconciled.last_success.get("CI").map(DateTime::to_rfc3339),
            Some("2026-08-16T00:00:00+00:00".to_owned())
        );
    }

    #[test]
    fn reconciliation_accepts_a_newer_last_success() {
        let mut known = RepoRuns::default();
        known.last_success.insert(
            "CI".to_owned(),
            "2026-07-01T00:00:00Z".parse().expect("timestamp"),
        );
        let mut fresh = RepoRuns::default();
        fresh.last_success.insert(
            "CI".to_owned(),
            "2026-08-16T00:00:00Z".parse().expect("timestamp"),
        );

        let reconciled = fresh.reconciled_with(&known, &live(&["CI"]));

        assert_eq!(
            reconciled.last_success.get("CI").map(DateTime::to_rfc3339),
            Some("2026-08-16T00:00:00+00:00".to_owned())
        );
    }

    // ---- request shape ---------------------------------------------------

    /// A repository whose default branch is [`DEFAULT_BRANCH`].
    fn repo() -> Repo {
        Repo {
            owner: "sdr-enthusiasts".to_owned(),
            name: "docker-beast-splitter".to_owned(),
            default_branch: DEFAULT_BRANCH.to_owned(),
        }
    }

    /// A runs payload holding one green `schedule` run of `update-flakes`.
    fn runs_payload() -> serde_json::Value {
        serde_json::json!({
            "workflow_runs": [{
                "path": ".github/workflows/update-flakes.yaml",
                "head_branch": DEFAULT_BRANCH,
                "status": "completed",
                "conclusion": "success",
                "event": "schedule",
                "created_at": "2026-08-16T05:31:31Z",
                "html_url": "https://github.com/o/r/actions/runs/1",
            }]
        })
    }

    fn update_flakes() -> Vec<Workflow> {
        vec![Workflow {
            id: 311_284_543,
            name: "update-flakes".to_owned(),
            path: ".github/workflows/update-flakes.yaml".to_owned(),
            state: WorkflowState::Active,
            triggers: vec!["schedule".to_owned(), "workflow_dispatch".to_owned()],
        }]
    }

    #[tokio::test]
    async fn the_runs_request_carries_no_server_side_filters() {
        // THE guard against reintroducing this module's defect. Every filtering
        // parameter on this endpoint -- `branch=`, `event=` -- is answered from
        // an eventually-consistent index that intermittently returns a partial
        // or empty page as a well-formed 200. Measured on
        // sdr-enthusiasts/docker-beast-splitter, whose true `?branch=main`
        // total_count is 264: fifty samples returned 0, 23, 30, 92, 111, 133,
        // 135, 190, 192, 206 and 264, roughly one in ten of them empty. The
        // unfiltered listing returned the same complete answer every time.
        //
        // So the query string must stay unfiltered. `per_page` is a page size,
        // not a filter, and does not select which runs exist.
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(
                "/repos/sdr-enthusiasts/docker-beast-splitter/actions/runs",
            ))
            .and(wiremock::matchers::query_param(
                "per_page",
                RUNS_PER_PAGE.to_string(),
            ))
            .and(wiremock::matchers::query_param_is_missing("branch"))
            .and(wiremock::matchers::query_param_is_missing("event"))
            .and(wiremock::matchers::query_param_is_missing("status"))
            .and(wiremock::matchers::query_param_is_missing("created"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(runs_payload()))
            .expect(1)
            .mount(&server)
            .await;

        let client = Client::new("t", &server.uri(), &format!("{}/graphql", server.uri()))
            .expect("client builds");
        let runs = fetch_runs(&client, &repo(), &update_flakes())
            .await
            .expect("request succeeds");

        assert_eq!(runs.latest.len(), 1);
        assert_eq!(runs.latest[0].conclusion, RunConclusion::Success);
    }

    #[tokio::test]
    async fn the_top_up_addresses_the_workflow_by_numeric_id_and_is_unfiltered() {
        // The top-up exists because dropping `branch=` leaves the shared page
        // diluted by pull-request runs: one page of fredsystems/nixos spans
        // about a day and speaks for 5 of its 14 workflows. It must not reach
        // for the flaky filter to compensate.
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(
                "/repos/sdr-enthusiasts/docker-beast-splitter/actions/workflows/311284543/runs",
            ))
            .and(wiremock::matchers::query_param_is_missing("branch"))
            .and(wiremock::matchers::query_param_is_missing("event"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(runs_payload()))
            .expect(1)
            .mount(&server)
            .await;

        let client = Client::new("t", &server.uri(), &format!("{}/graphql", server.uri()))
            .expect("client builds");
        let workflows = update_flakes();
        let runs = fetch_workflow_runs(&client, &repo(), &workflows[0])
            .await
            .expect("request succeeds");

        assert_eq!(runs.latest.len(), 1);
        assert_eq!(runs.latest[0].workflow, "update-flakes");
    }

    #[tokio::test]
    async fn renaming_the_default_branch_invalidates_the_cached_reduction() {
        // The branch is a reduction input now rather than a request parameter,
        // so it has to be in the cache key. Without it the unchanged URL would
        // answer 304 and replay a reduction computed against the old branch --
        // the same stale-reduction trap the workflow fingerprint exists for.
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(
                "/repos/sdr-enthusiasts/docker-beast-splitter/actions/runs",
            ))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .insert_header("etag", "\"unchanged\"")
                    .set_body_json(runs_payload()),
            )
            .mount(&server)
            .await;

        let client = Client::new("t", &server.uri(), &format!("{}/graphql", server.uri()))
            .expect("client builds");
        let workflows = update_flakes();

        let on_main = fetch_runs(&client, &repo(), &workflows)
            .await
            .expect("first fetch");
        assert_eq!(on_main.latest.len(), 1, "main is the run's branch");

        let mut renamed = repo();
        renamed.default_branch = "trunk".to_owned();
        let on_trunk = fetch_runs(&client, &renamed, &workflows)
            .await
            .expect("second fetch");

        assert!(
            on_trunk.latest.is_empty(),
            "the reduction must be recomputed for the new branch, not replayed"
        );
    }

    #[test]
    fn reconciliation_leaves_output_sorted_by_workflow() {
        // `record_runs` iterates this, and a stable order keeps a diff of the
        // exposed text meaningful.
        let known = reduction("Zebra", RunConclusion::Success, "2026-08-16T00:00:00Z");
        let fresh = reduction("Alpha", RunConclusion::Success, "2026-08-16T00:00:00Z");

        let reconciled = fresh.reconciled_with(&known, &live(&["Alpha", "Zebra"]));

        let order: Vec<&str> = reconciled
            .latest
            .iter()
            .map(|r| r.workflow.as_str())
            .collect();
        assert_eq!(order, ["Alpha", "Zebra"]);
    }
}
