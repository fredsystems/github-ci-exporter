//! Orchestrates one collection cycle.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use tracing::{debug, error, info, warn};

use crate::{
    config::Config,
    github::{Client, client::RateLimitResource, graphql, rest},
    metrics::{
        AuthorLabels, Metrics, Publisher, PullLabels, RepoLabels, ResourceLabels, SkipLabels,
        WorkflowEnabledLabels, WorkflowLabels, WorkflowStateLabels, author_label,
    },
    model::{PullRef, Repo, SkipReason},
};

/// GraphQL requests a full cycle needs: discovery pages plus batched activity
/// queries. Deliberately generous; the cost is a slightly early skip, not a
/// wrong result.
const ESTIMATED_GRAPHQL_REQUESTS: u64 = 8;

/// Core (REST) requests a cycle needs when the repository count is not yet
/// known, i.e. the very first cycle after start.
const ASSUMED_FIRST_CYCLE_REPOS: u64 = 80;

/// Workflow files assumed present before the first cycle has counted them.
///
/// The fleet runs about four per repository; rounding up keeps the first
/// cycle's estimate pessimistic, which is the safe direction.
const ASSUMED_WORKFLOWS_PER_REPO: u64 = 5;

/// Estimates the REST budget a cycle will consume.
///
/// Per repository: the workflow list, plus the shared runs page. Per workflow:
/// the definition lookup, plus at most one top-up runs request for a workflow
/// the shared page did not cover.
///
/// The per-workflow terms used to be folded into a flat `+50` of headroom,
/// which understated a cold sweep of the current fleet by roughly a factor of
/// four -- 239 workflows cost 239 definition lookups whatever the repository
/// count says. In practice almost all of them answer `304`, which GitHub does
/// not charge, so this is a cold-cache worst case rather than the steady state.
const fn estimate_core_requests(monitored: u64, workflows: u64) -> u64 {
    let repos = if monitored == 0 {
        ASSUMED_FIRST_CYCLE_REPOS
    } else {
        monitored
    };
    let workflows = if workflows == 0 {
        repos.saturating_mul(ASSUMED_WORKFLOWS_PER_REPO)
    } else {
        workflows
    };
    repos
        .saturating_mul(2)
        .saturating_add(workflows.saturating_mul(2))
        .saturating_add(50)
}

/// Cached per-repository workflow metadata.
///
/// Workflow definitions change far less often than run state, so the cron
/// schedules are resolved once and refreshed lazily rather than every cycle.
#[derive(Debug, Default)]
pub struct WorkflowCache {
    /// `owner/name` -> workflow **file path** -> expected interval in seconds.
    ///
    /// Keyed by path, not display name, for the same reason the run reducer is:
    /// the path is the workflow's identity. Keying by name meant a rename plus
    /// a failed contents lookup dropped the cached interval, because the old
    /// name was no longer in the live set.
    intervals: HashMap<String, HashMap<String, i64>>,
    /// `owner/name` -> the workflow files currently present in the repo.
    ///
    /// Retained so run history can be intersected against it: GitHub keeps
    /// runs of deleted workflows forever, and reporting them shows failures
    /// for CI that no longer exists.
    workflows: HashMap<String, Vec<rest::Workflow>>,
    /// Last-known-good run reduction per `owner/name`.
    ///
    /// Retained because GitHub's runs listing is only eventually consistent, so
    /// a single response can be silently incomplete. Reconciling each cycle
    /// against the previous one is the only way to tell an incomplete page from
    /// a real change -- see [`rest::RepoRuns::reconciled_with`].
    ///
    /// Deliberately not persisted with the `ETag` cache. A restart therefore
    /// starts with no high-water mark and trusts its first response, which is
    /// the correct trade: the alternative is carrying a possibly-wrong retained
    /// value across a deploy with no way to ever revise it downwards.
    runs: HashMap<String, rest::RepoRuns>,
    /// Repositories monitored by the previous cycle, used to size the
    /// budget pre-flight check.
    monitored_count: u64,
    /// Workflow files seen across all monitored repositories last cycle.
    ///
    /// The per-workflow requests -- the definition lookup, and the top-up in
    /// [`fetch_repo_runs`] -- scale with this rather than with the repository
    /// count, so the budget estimate needs it to be anything but a guess.
    workflow_count: u64,
    /// Pull requests the operator has declared unactionable, parsed once.
    ///
    /// The raw strings are validated at startup, so re-parsing them every
    /// cycle would be pure repeated work for a set that cannot change while
    /// the process runs.
    ignored_pulls: Option<HashSet<PullRef>>,
}

impl WorkflowCache {
    /// Repositories monitored last cycle; 0 before the first completes.
    #[must_use]
    pub const fn monitored_count(&self) -> u64 {
        self.monitored_count
    }

    /// Workflow files seen last cycle; 0 before the first completes.
    #[must_use]
    pub const fn workflow_count(&self) -> u64 {
        self.workflow_count
    }

    /// The parsed ignore list, resolved on first use.
    ///
    /// A parse failure here is impossible in practice: [`Config::validate`]
    /// rejects a malformed entry before the poll loop starts. Falling back to
    /// an empty set is the safe reading if one ever slips through -- it
    /// suppresses nothing rather than silently suppressing the wrong thing.
    fn ignored_pulls(&mut self, config: &Config) -> &HashSet<PullRef> {
        self.ignored_pulls
            .get_or_insert_with(|| config.ignored_pulls().unwrap_or_default())
    }
}

/// Why a cycle did not run to completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CycleOutcome {
    /// Every stage ran.
    Complete,
    /// Skipped before issuing any request because the remaining API budget
    /// could not cover a full sweep.
    BypassedLowBudget,
}

/// Runs a single collection cycle and publishes its results atomically.
///
/// The cycle builds an entirely fresh [`Metrics`] and only hands it to
/// `publisher` once every stage has run. Nothing it does is observable through
/// `/metrics` until that point, which is what keeps a scrape from catching a
/// half-rebuilt registry — see the [`crate::metrics`] module documentation for
/// what that cost when the live families were cleared and refilled instead.
///
/// A failure for one repository is logged and skipped rather than aborting the
/// cycle: one broken repository must not blind the operator to the other
/// sixty.
///
/// # Errors
/// Returns an error only when discovery fails for every configured owner,
/// which means the cycle produced no usable data at all.
pub async fn collect(
    client: &Client,
    config: &Config,
    publisher: &Publisher,
    cache: &mut WorkflowCache,
) -> anyhow::Result<CycleOutcome> {
    let started = std::time::Instant::now();
    let now = Utc::now();

    // Built off to the side and published at the end. A fresh registry starts
    // empty, so repositories, pull requests, and workflows that no longer
    // exist are absent by construction rather than by an explicit clear.
    let (cycle_metrics, registry) = Metrics::new();
    let metrics = &cycle_metrics;

    // Pre-flight budget check. A cycle that runs out of quota halfway leaves
    // a partially-rebuilt registry: some repositories updated, others cleared
    // and never refilled, which reads as "CI vanished" on the dashboard. It is
    // strictly better to skip the cycle whole and keep the previous values.
    let estimated_core = estimate_core_requests(cache.monitored_count(), cache.workflow_count());
    if !client.can_afford(RateLimitResource::Core, estimated_core)
        || !client.can_afford(RateLimitResource::GraphQl, ESTIMATED_GRAPHQL_REQUESTS)
    {
        let core = client.rate_limit(RateLimitResource::Core);
        let graphql_budget = client.rate_limit(RateLimitResource::GraphQl);
        warn!(
            core_remaining = core.remaining,
            graphql_remaining = graphql_budget.remaining,
            reserve = client.reserve(),
            estimated_core,
            reset_in_secs = core.reset_in_secs(now.timestamp()),
            "skipping collection cycle: insufficient API budget"
        );
        client.record_skipped(estimated_core);
        // Annotates the published set rather than replacing it: the point of
        // skipping is to keep the previous cycle's data, so the fresh registry
        // built above is discarded unused. One `annotate` call, so the three
        // updates land together.
        publisher.annotate(|published| {
            record_budget_metrics(client, published);
            published.cycles_bypassed.inc();
            published.budget_exhausted.set(1);
        });
        return Ok(CycleOutcome::BypassedLowBudget);
    }
    metrics.budget_exhausted.set(0);

    // Discovery
    let mut discovered = Vec::new();
    let mut discovery_failures = 0;
    for org in &config.orgs {
        match graphql::discover_owner(client, org).await {
            Ok(repos) => {
                debug!(owner = org, count = repos.len(), "discovered repositories");
                discovered.extend(repos);
            }
            Err(error) => {
                discovery_failures += 1;
                error!(owner = org, %error, "failed to discover owner");
            }
        }
    }
    if discovery_failures == config.orgs.len() {
        anyhow::bail!("discovery failed for every configured owner");
    }

    let max_age = config
        .max_repo_age
        .and_then(|d| chrono::Duration::from_std(d).ok());
    let (candidates, mut skipped) =
        graphql::partition_repos(discovered, &|name| config.is_denylisted(name), max_age, now);

    let monitored = resolve_monitored(client, config, cache, candidates, &mut skipped).await;

    info!(
        monitored = monitored.len(),
        skipped = skipped.len(),
        "resolved repository set"
    );

    record_repo_inventory(metrics, &monitored, &skipped);

    // Issues and pull requests, batched.
    match graphql::fetch_activity(client, &monitored).await {
        Ok(activity) => {
            record_activity(metrics, &monitored, &activity, cache.ignored_pulls(config));
        }
        Err(error) => error!(%error, "failed to fetch issue/PR activity"),
    }

    // Actions runs.
    let mut workflow_count = 0u64;
    for repo in &monitored {
        let live = cache
            .workflows
            .get(&repo.full_name())
            .cloned()
            .unwrap_or_default();
        // Without a workflow set every run would be discarded as orphaned,
        // publishing no series at all and making a listing failure look like
        // "this repository's CI vanished". Skip the fetch rather than spend a
        // request that cannot produce a result.
        if live.is_empty() {
            warn!(
                repo = %repo,
                "no workflow set available; skipping run fetch for this cycle"
            );
            continue;
        }
        workflow_count = workflow_count.saturating_add(u64::try_from(live.len()).unwrap_or(0));
        record_workflow_states(metrics, repo, &live);
        if let Some(runs) = fetch_repo_runs(client, repo, &live, cache).await {
            record_runs(metrics, repo, &runs, cache, now);
        }
    }

    // Self-monitoring.
    cache.monitored_count = u64::try_from(monitored.len()).unwrap_or(u64::MAX);
    cache.workflow_count = workflow_count;
    record_budget_metrics(client, metrics);
    metrics.scrape_duration.set(started.elapsed().as_secs_f64());
    metrics.scrape_success.set(1);
    metrics.last_success_timestamp.set(now.timestamp());

    // The single point at which any of this becomes visible to a scrape.
    publisher.publish(cycle_metrics, registry);

    if let Err(error) = client.persist_cache() {
        warn!(%error, "failed to persist etag cache");
    }

    Ok(CycleOutcome::Complete)
}

/// Collects one repository's default-branch run state.
///
/// Three stages, each closing a hole the previous one leaves:
///
/// 1. **One shared page** of the repository's run history
///    ([`rest::fetch_runs`]). Unfiltered, because every filter GitHub offers on
///    that endpoint is answered from an eventually-consistent index -- see the
///    [`rest`] module's "Why no server-side filters".
/// 2. **A top-up per uncovered workflow** ([`rest::fetch_workflow_runs`]).
///    Dropping the `branch=` filter means that shared page is diluted by pull
///    request runs, so on a busy repository it no longer reaches back far
///    enough to cover an infrequent scheduled workflow: one page of
///    `fredsystems/nixos` spans about a day and speaks for 5 of its 14
///    workflows. A workflow's own history is not diluted, so one page of it
///    settles the question.
/// 3. **Reconciliation against the previous cycle**
///    ([`rest::RepoRuns::reconciled_with`]), which is what makes an incomplete
///    response cost one cycle of freshness instead of publishing a regression.
///
/// Workflows GitHub never runs against the default branch are excluded before
/// any of this. The reduction already discards their runs, so fetching on their
/// behalf could only ever produce nothing -- and they are the majority of
/// PR-only workflows, which is what keeps the top-up affordable.
///
/// Returns `None` when the shared page could not be fetched at all, which is
/// the one case with nothing trustworthy to publish.
async fn fetch_repo_runs(
    client: &Client,
    repo: &Repo,
    live: &[rest::Workflow],
    cache: &mut WorkflowCache,
) -> Option<rest::RepoRuns> {
    let full_name = repo.full_name();

    let fresh = match rest::fetch_runs(client, repo, live).await {
        Ok(runs) => runs,
        Err(error) => {
            warn!(repo = %repo, %error, "failed to fetch workflow runs");
            // Deliberately not falling back to the retained value. A fetch
            // failure is already reported by `scrape_success`, and republishing
            // last cycle's numbers as though they were fresh is how a GitHub
            // outage comes to look like a healthy fleet.
            return None;
        }
    };

    // Resolved against the shared page before any top-up runs, because a top-up
    // for one workflow can only ever report on that workflow -- so re-checking
    // coverage as they land could not change the answer for any of the others.
    let uncovered: Vec<&rest::Workflow> = live
        .iter()
        .filter(|w| w.default_branch_signal() != rest::DefaultBranchSignal::None)
        .filter(|w| !fresh.covers(&w.name))
        .collect();

    let mut runs = fresh;
    for workflow in uncovered {
        match rest::fetch_workflow_runs(client, repo, workflow).await {
            Ok(topped_up) => runs = topped_up.reconciled_with(&runs, live),
            // One workflow's top-up failing leaves that workflow reported from
            // whatever the shared page and the retained state already say,
            // which is strictly better than discarding the repository.
            Err(error) => {
                warn!(repo = %repo, workflow = workflow.path, %error, "failed to top up workflow runs");
            }
        }
    }

    if let Some(previous) = cache.runs.get(&full_name) {
        runs = runs.reconciled_with(previous, live);
    }
    cache.runs.insert(full_name, runs.clone());
    Some(runs)
}

/// Publishes rate-limit and request-accounting metrics.
///
/// Called on both the normal and the budget-bypassed path, so a skipped cycle
/// still reports why it was skipped.
fn record_budget_metrics(client: &Client, metrics: &Metrics) {
    for resource in [RateLimitResource::Core, RateLimitResource::GraphQl] {
        let limit = client.rate_limit(resource);
        let labels = ResourceLabels {
            resource: resource.as_str().to_owned(),
        };
        metrics
            .rate_limit_remaining
            .get_or_create(&labels)
            .set(i64::try_from(limit.remaining).unwrap_or(i64::MAX));
        metrics
            .rate_limit_limit
            .get_or_create(&labels)
            .set(i64::try_from(limit.limit).unwrap_or(i64::MAX));
        metrics
            .rate_limit_reset
            .get_or_create(&labels)
            .set(limit.reset_at);
    }
    metrics
        .rate_limit_reserve
        .set(i64::try_from(client.reserve()).unwrap_or(i64::MAX));
    metrics
        .api_requests_total
        .set(i64::try_from(client.requests_total()).unwrap_or(i64::MAX));
    metrics
        .api_not_modified_total
        .set(i64::try_from(client.not_modified_total()).unwrap_or(i64::MAX));
    metrics
        .api_requests_skipped
        .set(i64::try_from(client.skipped_total()).unwrap_or(i64::MAX));
}

/// Publishes which repositories are monitored and why others were skipped.
fn record_repo_inventory(metrics: &Metrics, monitored: &[Repo], skipped: &[(Repo, SkipReason)]) {
    let mut skip_counts: HashMap<&'static str, i64> = HashMap::new();
    for (repo, reason) in skipped {
        debug!(repo = %repo, reason = reason.as_str(), "skipping repository");
        *skip_counts.entry(reason.as_str()).or_insert(0) += 1;
    }

    // Publish a zero for every reason so a category dropping to none is
    // visible rather than the series simply vanishing.
    for reason in [
        SkipReason::Archived,
        SkipReason::NoWorkflows,
        SkipReason::Denylisted,
        SkipReason::Inactive,
    ] {
        metrics
            .repos_skipped
            .get_or_create(&SkipLabels {
                reason: reason.as_str().to_owned(),
            })
            .set(skip_counts.get(reason.as_str()).copied().unwrap_or(0));
    }

    for repo in monitored {
        metrics
            .repo_monitored
            .get_or_create(&RepoLabels {
                org: repo.owner.clone(),
                repo: repo.name.clone(),
            })
            .set(1);
    }
}

/// Determines which candidates actually have CI, caching their workflow sets.
///
/// Repositories with no workflow files are content-hosting repos with no CI
/// signal; they are dropped so the dashboard is not padded with permanently
/// empty rows.
async fn resolve_monitored(
    client: &Client,
    config: &Config,
    cache: &mut WorkflowCache,
    candidates: Vec<Repo>,
    skipped: &mut Vec<(Repo, SkipReason)>,
) -> Vec<Repo> {
    let mut monitored = Vec::with_capacity(candidates.len());

    for repo in candidates {
        let key = repo.full_name();
        let mut workflows = match rest::list_workflows(client, &repo).await {
            Ok(workflows) => workflows,
            Err(error) => {
                // A listing failure is not evidence of absent CI, so the
                // repository is kept rather than silently dropped.
                warn!(repo = %repo, %error, "failed to list workflows; keeping repository");
                monitored.push(repo);
                continue;
            }
        };

        if workflows.is_empty() && config.skip_repos_without_workflows {
            cache.workflows.remove(&key);
            // Dropped together: a repository that no longer has CI must not
            // leave a retained reduction behind for `reconciled_with` to
            // resurrect if it later regains a workflow file.
            cache.runs.remove(&key);
            skipped.push((repo, SkipReason::NoWorkflows));
            continue;
        }
        // Trigger lists must be resolved before the workflow set is cached,
        // because the run reducer reads them from the cache.
        let resolved = resolve_definitions(client, &repo, &mut workflows).await;

        if let Some(previous) = cache.workflows.get(&key) {
            carry_forward_triggers(&mut workflows, previous);
        }

        let entry = cache.intervals.entry(key).or_default();
        merge_intervals(entry, resolved, &workflows);

        cache.workflows.insert(repo.full_name(), workflows);

        monitored.push(repo);
    }

    monitored
}

/// Restores trigger lists that this cycle failed to fetch.
///
/// `resolve_definitions` leaves `triggers` empty when a file lookup fails, and
/// an empty list means "accept any event". Without this, a transient contents
/// error would silently re-admit runs from a superseded trigger configuration
/// -- reintroducing the frext/bike-fitter false positive for one cycle.
fn carry_forward_triggers(current: &mut [rest::Workflow], previous: &[rest::Workflow]) {
    for workflow in current {
        if !workflow.triggers.is_empty() {
            continue;
        }
        if let Some(old) = previous
            .iter()
            .find(|w| w.path == workflow.path && !w.triggers.is_empty())
        {
            workflow.triggers.clone_from(&old.triggers);
        }
    }
}

/// Folds newly-resolved cron intervals into the cached set.
///
/// Merges rather than replaces: `resolve_definitions` omits any workflow whose
/// file lookup failed, so replacing wholesale would drop a previously-known
/// interval. That would make `workflow_expected_interval_seconds` disappear and
/// reappear, which in turn flaps `GitHubScheduledWorkflowStale` because that
/// rule compares against it.
///
/// Intervals for workflows that no longer exist are dropped, so a deleted cron
/// workflow stops being expected to run.
fn merge_intervals(
    cached: &mut HashMap<String, i64>,
    resolved: HashMap<String, i64>,
    live: &[rest::Workflow],
) {
    cached.extend(resolved);
    // Retained by path, so renaming a workflow does not orphan its interval.
    let live_paths: std::collections::HashSet<&str> =
        live.iter().map(|w| w.path.as_str()).collect();
    cached.retain(|path, _| live_paths.contains(path.as_str()));
}

/// Publishes whether GitHub will actually run each workflow.
///
/// A workflow auto-disabled for inactivity has silently stopped running; the
/// `state` label lets an alert distinguish that from a deliberate manual
/// disable.
fn record_workflow_states(metrics: &Metrics, repo: &Repo, workflows: &[rest::Workflow]) {
    for workflow in workflows {
        metrics
            .workflow_enabled
            .get_or_create(&WorkflowEnabledLabels {
                org: repo.owner.clone(),
                repo: repo.name.clone(),
                workflow: workflow.name.clone(),
                state: workflow.state.as_str().to_owned(),
            })
            .set(i64::from(workflow.state == rest::WorkflowState::Active));
    }
}

/// Reads each workflow file to recover its cron schedule and its trigger list.
///
/// Both come from the same fetch, which is `ETag`-revalidated and therefore
/// nearly free after the first sweep. `workflows` is updated in place with the
/// triggers, because the run reducer needs them to discard runs from a
/// superseded trigger configuration.
async fn resolve_definitions(
    client: &Client,
    repo: &Repo,
    workflows: &mut [rest::Workflow],
) -> HashMap<String, i64> {
    let mut intervals = HashMap::new();
    for workflow in workflows.iter_mut() {
        match rest::fetch_workflow_definition(client, repo, &workflow.path).await {
            Ok(definition) => {
                if let Some(interval) = expected_interval_seconds(&definition.crons) {
                    intervals.insert(workflow.path.clone(), interval);
                }
                workflow.triggers = definition.triggers;
            }
            Err(error) => {
                // Leaving `triggers` empty means "accept any event", so a
                // failed lookup degrades to the previous behaviour rather than
                // hiding the repository's CI.
                debug!(repo = %repo, path = workflow.path, %error, "workflow definition lookup failed");
            }
        }
    }
    intervals
}

/// Publishes issue and pull-request state.
///
/// `ignored` names pull requests the operator has declared unactionable. They
/// are dropped from the per-PR series -- so no alert can fire on them and they
/// leave the dashboard's PR tables -- but they remain in the aggregate
/// `repo_pulls_open` count, because the repository really does still have that
/// PR open. The number suppressed is published as `repo_pulls_ignored` so the
/// discrepancy is explainable rather than looking like a collection bug.
fn record_activity(
    metrics: &Metrics,
    monitored: &[Repo],
    activity: &std::collections::BTreeMap<String, graphql::RepoActivity>,
    ignored: &HashSet<PullRef>,
) {
    for repo in monitored {
        let full_name = repo.full_name();
        let Some(entry) = activity.get(&full_name) else {
            continue;
        };

        for (kind, count) in &entry.issues {
            metrics
                .issues_open
                .get_or_create(&AuthorLabels {
                    org: repo.owner.clone(),
                    repo: repo.name.clone(),
                    author_kind: author_label(*kind),
                })
                .set(i64::try_from(*count).unwrap_or(i64::MAX));
        }
        for (kind, count) in &entry.pulls {
            metrics
                .pulls_open
                .get_or_create(&AuthorLabels {
                    org: repo.owner.clone(),
                    repo: repo.name.clone(),
                    author_kind: author_label(*kind),
                })
                .set(i64::try_from(*count).unwrap_or(i64::MAX));
        }
        for (kind, count) in &entry.draft_pulls {
            metrics
                .pulls_draft
                .get_or_create(&AuthorLabels {
                    org: repo.owner.clone(),
                    repo: repo.name.clone(),
                    author_kind: author_label(*kind),
                })
                .set(i64::try_from(*count).unwrap_or(i64::MAX));
        }

        let mut ignored_count = 0i64;
        for pull in &entry.open_pulls {
            if ignored.contains(&PullRef::new(&full_name, pull.number)) {
                ignored_count += 1;
                debug!(
                    repo = %repo,
                    number = pull.number,
                    "suppressing pull request by operator configuration"
                );
                continue;
            }
            let labels = PullLabels {
                org: repo.owner.clone(),
                repo: repo.name.clone(),
                number: pull.number.to_string(),
                author: pull.author.clone(),
                author_kind: author_label(pull.author_kind),
                draft: pull.is_draft.to_string(),
                checks: pull.checks.as_str().to_owned(),
                mergeable: pull.mergeable.as_str().to_owned(),
                auto_merge: pull.auto_merge.to_string(),
            };
            metrics
                .pull_created_timestamp
                .get_or_create(&labels)
                .set(pull.created_at.timestamp());
            metrics
                .pull_needs_attention
                .get_or_create(&labels)
                .set(i64::from(pull.needs_attention()));
            metrics
                .pull_ready_to_merge
                .get_or_create(&labels)
                .set(i64::from(pull.is_ready_to_merge()));
        }

        // Published unconditionally, including as a zero, so "nothing is being
        // hidden on this repository" is an assertion the dashboard can make
        // rather than an absence it has to assume.
        metrics
            .pulls_ignored
            .get_or_create(&RepoLabels {
                org: repo.owner.clone(),
                repo: repo.name.clone(),
            })
            .set(ignored_count);
    }
}

fn record_runs(
    metrics: &Metrics,
    repo: &Repo,
    runs: &rest::RepoRuns,
    cache: &WorkflowCache,
    now: DateTime<Utc>,
) {
    let full_name = repo.full_name();
    let intervals = cache.intervals.get(&full_name);
    // Runs are labelled by display name, while intervals are keyed by path, so
    // the workflow set provides the mapping between the two.
    let path_for_name: HashMap<&str, &str> = cache
        .workflows
        .get(&full_name)
        .map(|ws| {
            ws.iter()
                .map(|w| (w.name.as_str(), w.path.as_str()))
                .collect()
        })
        .unwrap_or_default();

    // Whether a gap since the last run means anything depends on how the
    // workflow is triggered, which only the workflow set knows.
    let signal_for_name: HashMap<&str, rest::DefaultBranchSignal> = cache
        .workflows
        .get(&full_name)
        .map(|ws| {
            ws.iter()
                .map(|w| (w.name.as_str(), w.default_branch_signal()))
                .collect()
        })
        .unwrap_or_default();

    // Unknown workflows default to cadenced, matching how an unresolved
    // trigger list is treated everywhere else: permissive, so a failed
    // definition lookup cannot blank a repository.
    let signal_of = |workflow: &str| {
        signal_for_name
            .get(workflow)
            .copied()
            .unwrap_or(rest::DefaultBranchSignal::Cadenced)
    };

    for run in &runs.latest {
        let signal = signal_of(&run.workflow);

        // `reduce_runs` already drops these, so reaching here means the
        // reduction did not come from the current code -- in practice, a
        // cached projection replayed on a `304`. Enforcing the rule at the
        // point of publication as well makes it hold regardless of a
        // reduction's provenance, which is the difference between a stale
        // cache costing one sweep of accuracy and it publishing a fossil
        // failure that pages.
        if signal == rest::DefaultBranchSignal::None {
            debug!(
                repo = %repo,
                workflow = run.workflow,
                "discarding a run for a workflow with no default-branch state"
            );
            continue;
        }

        // A run older than the staleness horizon says nothing about the
        // current code. Reporting `stale` instead of its original conclusion
        // keeps an ancient failure from producing an alert that cannot be
        // cleared without an artificial push. `workflow_run_stale` carries the
        // fact separately so it stays visible on the dashboard.
        //
        // Only for a cadenced workflow, though. An on-demand one has no
        // cadence to be late against, so the age of its last run is not a
        // fault and must not mask what that run actually concluded -- that is
        // what reported every `Deploy` the fleet had not released in three
        // months as though its CI had gone quiet.
        let stale = signal == rest::DefaultBranchSignal::Cadenced && run.is_stale(now);
        metrics
            .workflow_run_stale
            .get_or_create(&WorkflowLabels {
                org: repo.owner.clone(),
                repo: repo.name.clone(),
                workflow: run.workflow.clone(),
            })
            .set(i64::from(stale));

        let conclusion = if stale {
            "stale"
        } else {
            run.conclusion.as_str()
        };
        metrics
            .workflow_run_status
            .get_or_create(&WorkflowStateLabels {
                org: repo.owner.clone(),
                repo: repo.name.clone(),
                workflow: run.workflow.clone(),
                event: run.event.clone(),
                conclusion: conclusion.to_owned(),
            })
            .set(1);

        metrics
            .workflow_run_timestamp
            .get_or_create(&WorkflowLabels {
                org: repo.owner.clone(),
                repo: repo.name.clone(),
                workflow: run.workflow.clone(),
            })
            .set(run.created_at.timestamp());

        if let Some(interval) = path_for_name
            .get(run.workflow.as_str())
            .and_then(|path| intervals.and_then(|m| m.get(*path)))
        {
            metrics
                .workflow_expected_interval
                .get_or_create(&WorkflowLabels {
                    org: repo.owner.clone(),
                    repo: repo.name.clone(),
                    workflow: run.workflow.clone(),
                })
                .set(*interval);
        }
    }

    for (workflow, at) in &runs.last_success {
        // Same gate as above, and for the same reason: a replayed pre-filter
        // reduction carries `last_success` entries too, and publishing "this
        // workflow last passed on the default branch" for one that never runs
        // against the default branch is exactly the claim being retracted.
        if signal_of(workflow) == rest::DefaultBranchSignal::None {
            continue;
        }
        metrics
            .workflow_last_success_timestamp
            .get_or_create(&WorkflowLabels {
                org: repo.owner.clone(),
                repo: repo.name.clone(),
                workflow: workflow.clone(),
            })
            .set(at.timestamp());
    }
}

/// Days of schedule to walk when measuring a cron expression's longest gap.
///
/// Four years so the window always contains a February, a leap February, and a
/// 31-day month, which is what makes a monthly expression resolve to its true
/// worst case rather than to whichever month the walk happened to start in.
const GAP_SCAN_HORIZON_DAYS: i64 = 4 * 366;

/// Span the walk must cover before [`GAP_SCAN_SOFT_SAMPLES`] may stop it.
///
/// A dense schedule confined to a calendar window hides its longest gap behind
/// a large number of short ones: `*/5 * * 1 *` fires 8,928 times inside
/// January before the eleven-month wait, so a sample cap alone reports 300
/// seconds and the staleness alert then fires for the rest of the year. The
/// walk therefore may not stop on sample count until it has seen a full year.
const GAP_SCAN_SETTLED_DAYS: i64 = 366;

/// Occurrences after which the walk may stop, once it has also covered
/// [`GAP_SCAN_SETTLED_DAYS`].
const GAP_SCAN_SOFT_SAMPLES: usize = 2_000;

/// Occurrences after which the walk stops unconditionally.
///
/// Bounds the cost of a sub-hourly schedule, which would otherwise enumerate
/// ~420k occurrences before reaching the settled span. Any schedule dense
/// enough to hit this ceiling has already shown its gap pattern many times
/// over.
const GAP_SCAN_HARD_SAMPLES: usize = 50_000;

/// Derives the longest interval that may legitimately pass between two runs of
/// a workflow, in seconds.
///
/// Used to detect scheduled workflows that have silently stopped running —
/// GitHub disables cron triggers on repositories with no activity for 60 days.
/// The staleness alert downstream compares elapsed time against this value, so
/// it has to be the worst legitimate gap: anything smaller reports a healthy
/// workflow as late.
///
/// Two subtleties, both of which produced wrong values in the field:
///
/// - A single expression does not necessarily have a constant period. Monthly
///   fires are 28..=31 days apart and a weekday-only schedule skips the
///   weekend, so each expression contributes its *longest* gap rather than its
///   first.
/// - Across several expressions a workflow runs whenever any of them fires, so
///   the most frequent schedule bounds the gap and the *minimum* wins.
#[must_use]
pub fn expected_interval_seconds(crons: &[String]) -> Option<i64> {
    crons
        .iter()
        .filter_map(|expression| longest_gap_seconds(expression))
        .min()
}

/// Walks a single cron expression and returns its longest gap, in seconds.
///
/// Returns `None` if the expression does not parse or fires fewer than twice
/// in the scan window; a caller with several expressions still resolves from
/// the others.
fn longest_gap_seconds(expression: &str) -> Option<i64> {
    let schedules = posix_schedules(&normalise_day_of_week(expression)?)?;

    let anchor = DateTime::<Utc>::from_timestamp(0, 0)?;
    let horizon = anchor.checked_add_signed(chrono::TimeDelta::days(GAP_SCAN_HORIZON_DAYS))?;
    let settled = anchor.checked_add_signed(chrono::TimeDelta::days(GAP_SCAN_SETTLED_DAYS))?;

    let mut streams: Vec<_> = schedules
        .iter()
        .map(|schedule| schedule.after(&anchor).peekable())
        .collect();

    let mut previous: Option<DateTime<Utc>> = None;
    let mut longest = 0_i64;
    let mut samples = 0_usize;

    while let Some(occurrence) = streams.iter_mut().filter_map(|s| s.peek().copied()).min() {
        // Advance every stream sitting on this instant, so a moment produced by
        // both halves of a day-of-month/day-of-week union counts once.
        for stream in &mut streams {
            if stream.peek() == Some(&occurrence) {
                stream.next();
            }
        }
        if occurrence > horizon {
            break;
        }
        if let Some(previous) = previous {
            longest = longest.max((occurrence - previous).num_seconds());
        }
        previous = Some(occurrence);

        samples += 1;
        if samples >= GAP_SCAN_HARD_SAMPLES
            || (samples >= GAP_SCAN_SOFT_SAMPLES && occurrence > settled)
        {
            break;
        }
    }

    (longest > 0).then_some(longest)
}

/// Builds the schedules whose union reproduces POSIX day semantics.
///
/// When a cron expression restricts *both* day-of-month and day-of-week, POSIX
/// fires if either matches. The `cron` crate intersects them instead, so
/// `0 0 1 * 1` means "the 1st, and every Monday" to GitHub but "the 1st, but
/// only when it falls on a Monday" to the crate — 273 days between fires
/// rather than 7, which is long enough to suppress a genuinely dead workflow.
///
/// Splitting into one schedule per day field and walking them as a union
/// restores the POSIX reading. When either field is `*` the crate's
/// intersection already agrees, and a single schedule is used.
fn posix_schedules(expression: &str) -> Option<Vec<cron::Schedule>> {
    use std::str::FromStr as _;

    let fields: Vec<&str> = expression.split_whitespace().collect();
    let [minute, hour, day_of_month, month, day_of_week] = fields.as_slice() else {
        return None;
    };

    // GitHub uses 5-field POSIX cron; the `cron` crate expects a leading
    // seconds field, so one is prepended.
    let build = |day_of_month: &str, day_of_week: &str| {
        cron::Schedule::from_str(&format!(
            "0 {minute} {hour} {day_of_month} {month} {day_of_week}"
        ))
        .ok()
    };

    if *day_of_month == "*" || *day_of_week == "*" {
        return build(day_of_month, day_of_week).map(|schedule| vec![schedule]);
    }
    Some(vec![build(day_of_month, "*")?, build("*", day_of_week)?])
}

/// Rewrites a cron expression's day-of-week field from GitHub's numbering into
/// the numbering the `cron` crate accepts.
///
/// GitHub follows POSIX: `0..=6` for Sunday..Saturday, with `7` also accepted
/// for Sunday. The `cron` crate uses `1..=7` for Sunday..Saturday and rejects
/// `0` outright. Left unnormalised, an ordinary Sunday schedule written
/// `0 5 * * 0` fails to parse and is discarded silently, leaving the workflow
/// with no cadence at all — or, when it shares a workflow with a second
/// expression, with the wrong one. Observed live on fredsystems/nixos, whose
/// weekly `0 5 * * 0` vanished and left a monthly baseline behind.
///
/// Returns `None` for anything that is not a 5-field expression, so malformed
/// input is rejected here rather than reaching the parser.
fn normalise_day_of_week(expression: &str) -> Option<String> {
    let fields: Vec<&str> = expression.split_whitespace().collect();
    let [minute, hour, day_of_month, month, day_of_week] = fields.as_slice() else {
        return None;
    };
    let day_of_week = remap_day_of_week_field(day_of_week);
    Some(format!(
        "{minute} {hour} {day_of_month} {month} {day_of_week}"
    ))
}

/// Remaps every numeric term in a day-of-week field.
///
/// Named days (`SUN`) and wildcards pass through untouched: the parser
/// understands those, and a bare `*/n` selects the same days under either
/// numbering because both start counting at their own Sunday.
fn remap_day_of_week_field(field: &str) -> String {
    field
        .split(',')
        .map(remap_day_of_week_term)
        .collect::<Vec<_>>()
        .join(",")
}

/// Remaps one term of a day-of-week field, expanding ranges to an explicit
/// list.
///
/// A range cannot simply have both endpoints remapped, because the two
/// numberings disagree about where the week starts and any range spanning
/// Sunday comes out inverted or collapsed. `0-7` (every day) would become
/// `1-1`, Sunday alone, and `1-7` would become the empty range `2-1`.
/// Enumerating the days first and emitting a list sidesteps the ordering.
fn remap_day_of_week_term(term: &str) -> String {
    let (range, step) = match term.split_once('/') {
        Some((range, step)) => match step.parse::<u8>() {
            Ok(step) if step > 0 => (range, step),
            _ => return term.to_owned(),
        },
        None => (term, 1),
    };

    let Some(days) = posix_days_of_week(range, step) else {
        return term.to_owned();
    };

    let mut remapped: Vec<u8> = days.iter().map(|day| day % 7 + 1).collect();
    remapped.sort_unstable();
    remapped.dedup();

    remapped
        .iter()
        .map(u8::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

/// Enumerates the POSIX day-of-week numbers a single numeric term selects.
///
/// Returns `None` for wildcards and named days, which are left for the parser.
/// Ranges may wrap through Sunday (`5-1` for Friday..Monday), which a plain
/// endpoint comparison would read as empty.
fn posix_days_of_week(range: &str, step: u8) -> Option<Vec<u8>> {
    let (start, end) = if let Some((start, end)) = range.split_once('-') {
        (start.parse::<u8>().ok()?, end.parse::<u8>().ok()?)
    } else {
        let single = range.parse::<u8>().ok()?;
        (single, single)
    };
    if start > 7 || end > 7 {
        return None;
    }

    // Both 0 and 7 mean Sunday, so the span is measured modulo the week.
    let span = if start <= end {
        end - start
    } else {
        7 - start + end
    };
    Some(
        (0..=span)
            .step_by(usize::from(step))
            .map(|offset| (start + offset) % 7)
            .collect(),
    )
}

/// Marks a cycle as failed without clearing the previous values.
///
/// Retaining the last-known-good samples means a transient GitHub outage does
/// not resolve a genuine CI-failure alert; `scrape_success` going to 0 is what
/// signals the staleness. Annotates the published set in place, because
/// publishing anything new is exactly what must not happen here.
///
/// `budget_exhausted` is cleared as well. This path is reached only for errors
/// that are *not* budget exhaustion -- a bypassed cycle returns
/// [`CycleOutcome::BypassedLowBudget`] rather than an error -- so reaching here
/// proves the pre-flight check passed and the budget was affordable. Left set
/// from an earlier bypass it would survive indefinitely, reporting a budget
/// skip for every subsequent unrelated failure.
pub fn record_failure(publisher: &Publisher) {
    publisher.annotate(|published| {
        published.scrape_success.set(0);
        published.budget_exhausted.set(0);
    });
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

    #[test]
    fn daily_cron_yields_86400_seconds() {
        let interval = expected_interval_seconds(&["0 12 * * *".to_owned()]).expect("interval");
        assert_eq!(interval, 86_400);
    }

    #[test]
    fn weekly_cron_yields_seven_days() {
        let interval = expected_interval_seconds(&["0 0 * * 1".to_owned()]).expect("interval");
        assert_eq!(interval, 604_800);
    }

    #[test]
    fn shortest_wins_across_multiple_crons() {
        let interval = expected_interval_seconds(&[
            "0 0 * * 1".to_owned(),  // weekly
            "0 12 * * *".to_owned(), // daily
        ])
        .expect("interval");
        assert_eq!(interval, 86_400);
    }

    #[test]
    fn invalid_cron_is_ignored_not_fatal() {
        assert!(expected_interval_seconds(&["not a cron".to_owned()]).is_none());
        assert!(expected_interval_seconds(&[]).is_none());
    }

    #[test]
    fn invalid_cron_does_not_hide_a_valid_one() {
        let interval = expected_interval_seconds(&["nonsense".to_owned(), "0 12 * * *".to_owned()])
            .expect("valid expression should still resolve");
        assert_eq!(interval, 86_400);
    }

    // GitHub uses POSIX day-of-week numbering, where 0 and 7 both mean Sunday.
    // The `cron` crate uses 1..=7 with 1 = Sunday, so an unnormalised
    // `0 5 * * 0` expression fails to parse and the schedule is silently
    // dropped. Observed live: fredsystems/nixos declares `0 5 * * 0` and the
    // exporter reported no weekly cadence for it at all.
    #[test]
    fn sunday_as_zero_is_a_valid_weekly_cron() {
        let interval = expected_interval_seconds(&["0 5 * * 0".to_owned()])
            .expect("Sunday-as-zero must parse");
        assert_eq!(interval, 604_800);
    }

    #[test]
    fn sunday_as_seven_is_a_valid_weekly_cron() {
        let interval = expected_interval_seconds(&["0 5 * * 7".to_owned()])
            .expect("Sunday-as-seven must parse");
        assert_eq!(interval, 604_800);
    }

    // Every POSIX day-of-week number must round-trip to a weekly cadence.
    #[test]
    fn every_posix_weekday_yields_seven_days() {
        for dow in 0..=7 {
            let interval = expected_interval_seconds(&[format!("0 5 * * {dow}")])
                .unwrap_or_else(|| panic!("day-of-week {dow} must parse"));
            assert_eq!(interval, 604_800, "day-of-week {dow}");
        }
    }

    // A monthly cron has no single interval: consecutive fires are 28..=31
    // days apart. The staleness alert downstream compares elapsed time against
    // this value, so it must be the LONGEST legitimate gap. Anchoring on the
    // Unix epoch previously returned the length of whichever 1970 month the
    // first two fires landed in -- 28 days for `0 0 1 * *`, which made every
    // monthly workflow look permanently three days late.
    #[test]
    fn monthly_cron_uses_the_longest_calendar_month() {
        let interval = expected_interval_seconds(&["0 0 1 * *".to_owned()])
            .expect("monthly cron must resolve");
        assert_eq!(interval, 31 * 86_400);
    }

    #[test]
    fn monthly_cron_interval_is_independent_of_the_hour_field() {
        let midnight = expected_interval_seconds(&["0 0 1 * *".to_owned()])
            .expect("monthly cron must resolve");
        let morning = expected_interval_seconds(&["0 6 1 * *".to_owned()])
            .expect("monthly cron must resolve");
        assert_eq!(midnight, morning);
    }

    // The combination that broke fredsystems/nixos: a weekly Sunday cron and a
    // monthly cron on the same workflow. The weekly one failed to parse, so
    // the workflow was monitored against a ~monthly baseline despite running
    // every week.
    #[test]
    fn weekly_sunday_cron_wins_over_a_monthly_cron() {
        let interval = expected_interval_seconds(&["0 6 1 * *".to_owned(), "0 5 * * 0".to_owned()])
            .expect("interval");
        assert_eq!(interval, 604_800);
    }

    // Mon-Fri fires five times a week, but the gap that matters for staleness
    // is Friday -> Monday.
    #[test]
    fn weekday_only_cron_uses_the_weekend_gap() {
        let interval = expected_interval_seconds(&["0 5 * * 1-5".to_owned()]).expect("interval");
        assert_eq!(interval, 3 * 86_400);
    }

    // A day-of-week range cannot be remapped endpoint-wise: the numberings
    // disagree about where the week starts, so anything spanning Sunday came
    // out inverted (`1-7` -> `2-1`, an empty range that failed to parse) or
    // collapsed (`0-7` -> `1-1`, Sunday alone rather than every day).
    #[test]
    fn day_of_week_ranges_spanning_sunday_keep_their_coverage() {
        for range in ["0-6", "0-7", "1-7"] {
            let interval = expected_interval_seconds(&[format!("0 5 * * {range}")])
                .unwrap_or_else(|| panic!("range {range} must resolve"));
            assert_eq!(interval, 86_400, "range {range} covers every day");
        }
    }

    #[test]
    fn a_weekend_range_wraps_through_sunday() {
        // Saturday and Sunday only: Sunday -> Saturday is six days.
        let interval = expected_interval_seconds(&["0 5 * * 6-7".to_owned()]).expect("interval");
        assert_eq!(interval, 6 * 86_400);
    }

    #[test]
    fn a_day_of_week_range_may_wrap_past_the_end_of_the_week() {
        // Friday..Monday. The longest gap is Monday -> Friday.
        let interval = expected_interval_seconds(&["0 5 * * 5-1".to_owned()]).expect("interval");
        assert_eq!(interval, 4 * 86_400);
    }

    #[test]
    fn a_stepped_day_of_week_range_is_remapped() {
        // Mon, Wed, Fri. The longest gap is Friday -> Monday.
        let interval = expected_interval_seconds(&["0 5 * * 1-5/2".to_owned()]).expect("interval");
        assert_eq!(interval, 3 * 86_400);
    }

    #[test]
    fn named_days_and_bare_steps_are_left_alone() {
        // `*/2` selects Sun/Tue/Thu/Sat under either numbering, since both
        // start counting at their own Sunday.
        let stepped = expected_interval_seconds(&["0 5 * * */2".to_owned()]).expect("interval");
        assert_eq!(stepped, 2 * 86_400);
        assert!(expected_interval_seconds(&["0 5 * * MON".to_owned()]).is_some());
    }

    // POSIX fires when EITHER day field matches, whenever both are restricted.
    // The `cron` crate intersects them, which read `0 0 1 * 1` as "the 1st,
    // but only when it is a Monday" -- 273 days between fires instead of 7,
    // long enough to hide a genuinely dead workflow.
    #[test]
    fn a_restricted_day_of_month_and_day_of_week_are_unioned() {
        let interval = expected_interval_seconds(&["0 0 1 * 1".to_owned()]).expect("interval");
        assert_eq!(interval, 7 * 86_400);
    }

    #[test]
    fn a_wildcard_day_field_still_resolves_from_a_single_schedule() {
        let by_month_day = expected_interval_seconds(&["0 0 1 * *".to_owned()]).expect("interval");
        assert_eq!(by_month_day, 31 * 86_400);
        let by_week_day = expected_interval_seconds(&["0 0 * * 1".to_owned()]).expect("interval");
        assert_eq!(by_week_day, 7 * 86_400);
    }

    // A dense schedule confined to a calendar window hides its longest gap
    // behind thousands of short ones. `*/5 * * 1 *` fires 8,928 times inside
    // January; a plain sample cap stopped the walk after about seven days and
    // reported 300 seconds, which would fire the staleness alert for the
    // remaining eleven months.
    #[test]
    fn a_dense_but_windowed_schedule_still_finds_its_long_gap() {
        let interval = expected_interval_seconds(&["*/5 * * 1 *".to_owned()]).expect("interval");
        assert!(
            interval > 300 * 86_400,
            "expected the eleven-month gap, got {interval}s"
        );
    }

    #[test]
    fn a_dense_unbounded_schedule_terminates_with_a_uniform_gap() {
        let interval = expected_interval_seconds(&["*/5 * * * *".to_owned()]).expect("interval");
        assert_eq!(interval, 300);
    }

    fn wf(name: &str, triggers: &[&str]) -> rest::Workflow {
        rest::Workflow {
            id: 1,
            name: name.to_owned(),
            path: format!(".github/workflows/{name}.yml"),
            state: rest::WorkflowState::Active,
            triggers: triggers.iter().map(|t| (*t).to_owned()).collect(),
        }
    }

    #[test]
    fn triggers_are_carried_forward_when_a_lookup_fails() {
        // A failed contents fetch leaves triggers empty, which means "accept
        // any event". Without carry-forward that silently re-admits runs from
        // a superseded trigger for a cycle.
        let previous = vec![wf("CI", &["pull_request"])];
        let mut current = vec![wf("CI", &[])];
        carry_forward_triggers(&mut current, &previous);
        assert_eq!(current[0].triggers, ["pull_request"]);
    }

    #[test]
    fn freshly_resolved_triggers_win_over_cached_ones() {
        let previous = vec![wf("CI", &["push"])];
        let mut current = vec![wf("CI", &["pull_request"])];
        carry_forward_triggers(&mut current, &previous);
        assert_eq!(
            current[0].triggers,
            ["pull_request"],
            "a real change must not be reverted by the cache"
        );
    }

    #[test]
    fn carry_forward_matches_on_path_not_name() {
        // A renamed workflow keeps its file path, so its triggers still apply.
        let previous = vec![wf("CI", &["pull_request"])];
        let mut renamed = wf("Build", &[]);
        renamed.path = ".github/workflows/CI.yml".to_owned();
        let mut current = vec![renamed];
        carry_forward_triggers(&mut current, &previous);
        assert_eq!(current[0].triggers, ["pull_request"]);
    }

    #[test]
    fn a_failed_lookup_does_not_drop_a_known_interval() {
        // Regression guard: replacing the interval map wholesale made
        // workflow_expected_interval_seconds vanish whenever a contents fetch
        // failed, which flaps GitHubScheduledWorkflowStale.
        let mut cached = HashMap::from([(
            ".github/workflows/update-flakes.yml".to_owned(),
            604_800_i64,
        )]);
        let live = vec![wf("update-flakes", &["schedule"])];

        merge_intervals(&mut cached, HashMap::new(), &live);

        assert_eq!(
            cached.get(".github/workflows/update-flakes.yml"),
            Some(&604_800)
        );
    }

    #[test]
    fn a_new_interval_overwrites_the_cached_one() {
        let mut cached = HashMap::from([(
            ".github/workflows/update-flakes.yml".to_owned(),
            604_800_i64,
        )]);
        let live = vec![wf("update-flakes", &["schedule"])];

        merge_intervals(
            &mut cached,
            HashMap::from([(".github/workflows/update-flakes.yml".to_owned(), 86_400_i64)]),
            &live,
        );

        assert_eq!(
            cached.get(".github/workflows/update-flakes.yml"),
            Some(&86_400)
        );
    }

    #[test]
    fn intervals_for_deleted_workflows_are_dropped() {
        // Otherwise a removed cron workflow stays "expected to run" forever.
        let mut cached = HashMap::from([
            (
                ".github/workflows/update-flakes.yml".to_owned(),
                604_800_i64,
            ),
            (".github/workflows/gone.yml".to_owned(), 86_400_i64),
        ]);
        let live = vec![wf("update-flakes", &["schedule"])];

        merge_intervals(&mut cached, HashMap::new(), &live);

        assert_eq!(cached.len(), 1);
        assert!(!cached.contains_key(".github/workflows/gone.yml"));
    }

    #[test]
    fn a_rename_plus_failed_lookup_keeps_the_interval() {
        // Regression guard: intervals were keyed by display name while
        // identity is the file path. Renaming a workflow whose contents
        // lookup then failed orphaned the cached interval, so
        // workflow_expected_interval_seconds vanished for that cycle and
        // flapped GitHubScheduledWorkflowStale.
        let mut cached = HashMap::from([(".github/workflows/CI.yml".to_owned(), 86_400_i64)]);

        // Same file, new display name, and no freshly resolved interval.
        let mut renamed = wf("Build", &["schedule"]);
        renamed.path = ".github/workflows/CI.yml".to_owned();

        merge_intervals(&mut cached, HashMap::new(), &[renamed]);

        assert_eq!(
            cached.get(".github/workflows/CI.yml"),
            Some(&86_400),
            "a rename must not orphan the cached interval"
        );
    }

    #[test]
    fn first_cycle_estimate_assumes_a_full_fleet() {
        // Before any cycle completes neither count is known; the estimate must
        // be pessimistic rather than zero, on both terms.
        let repos = ASSUMED_FIRST_CYCLE_REPOS;
        let workflows = repos * ASSUMED_WORKFLOWS_PER_REPO;
        assert_eq!(estimate_core_requests(0, 0), repos * 2 + workflows * 2 + 50);
    }

    #[test]
    fn estimate_scales_with_monitored_repositories() {
        assert_eq!(estimate_core_requests(61, 239), 650);
        assert!(estimate_core_requests(61, 239) < estimate_core_requests(100, 239));
    }

    #[test]
    fn estimate_scales_with_workflow_count() {
        // Regression guard: the per-workflow requests -- one definition lookup
        // each, plus at most one top-up -- were folded into a flat `+50`, which
        // understated a cold sweep of the fleet roughly fourfold. A repository
        // count alone cannot price them.
        assert!(estimate_core_requests(61, 239) > estimate_core_requests(61, 61));
    }

    #[test]
    fn estimate_assumes_workflows_when_only_the_repo_count_is_known() {
        // A cycle that saw repositories but counted no workflows has not
        // completed its run stage, so the workflow term must still be assumed.
        assert_eq!(
            estimate_core_requests(10, 0),
            10 * 2 + 10 * ASSUMED_WORKFLOWS_PER_REPO * 2 + 50
        );
    }

    #[test]
    fn estimate_cannot_overflow() {
        assert_eq!(estimate_core_requests(u64::MAX, u64::MAX), u64::MAX);
        assert_eq!(estimate_core_requests(u64::MAX, 0), u64::MAX);
    }

    /// One repository with one open, non-draft, conflicting PR -- i.e. one that
    /// `needs_attention` and would alert.
    fn activity_with_one_stuck_pull(
        number: u64,
    ) -> std::collections::BTreeMap<String, graphql::RepoActivity> {
        use crate::model::{AuthorKind, ChecksState, MergeableState};

        let mut activity = graphql::RepoActivity::default();
        activity.pulls.insert(AuthorKind::Human, 1);
        activity.open_pulls.push(graphql::OpenPull {
            number,
            author: "kx1t".to_owned(),
            author_kind: AuthorKind::Human,
            is_draft: false,
            created_at: "2025-01-01T00:00:00Z".parse().expect("timestamp"),
            checks: ChecksState::None,
            mergeable: MergeableState::Conflicting,
            auto_merge: false,
        });
        std::collections::BTreeMap::from([(
            "sdr-enthusiasts/docker-vesselalert".to_owned(),
            activity,
        )])
    }

    fn vesselalert() -> Repo {
        Repo {
            owner: "sdr-enthusiasts".to_owned(),
            name: "docker-vesselalert".to_owned(),
            default_branch: "main".to_owned(),
        }
    }

    #[test]
    fn an_ignored_pull_leaves_the_per_pull_series() {
        // The motivating case: docker-vesselalert#32 is conflicting and open
        // against someone else's repository. It is not ours to close or draft,
        // and the maintainer has not engaged, so it must stop alerting.
        let (metrics, registry) = Metrics::new();
        let monitored = vec![vesselalert()];
        let ignored = HashSet::from([PullRef::new("sdr-enthusiasts/docker-vesselalert", 32)]);

        record_activity(
            &metrics,
            &monitored,
            &activity_with_one_stuck_pull(32),
            &ignored,
        );

        let rendered = crate::metrics::Publisher::new(metrics, registry).render();
        assert!(
            !rendered.contains("github_pull_needs_attention"),
            "an ignored PR must not be able to fire an alert:\n{rendered}"
        );
        assert!(
            !rendered.contains(r#"number="32""#),
            "an ignored PR must leave the per-PR series entirely"
        );
    }

    #[test]
    fn an_ignored_pull_still_counts_as_open() {
        // Suppression is about actionability, not about lying: the repository
        // really does have an open PR, and repo_pulls_open must keep matching
        // GitHub's own count.
        let (metrics, registry) = Metrics::new();
        let ignored = HashSet::from([PullRef::new("sdr-enthusiasts/docker-vesselalert", 32)]);

        record_activity(
            &metrics,
            &[vesselalert()],
            &activity_with_one_stuck_pull(32),
            &ignored,
        );

        let rendered = crate::metrics::Publisher::new(metrics, registry).render();
        assert!(
            rendered.contains(r#"github_repo_pulls_open{org="sdr-enthusiasts",repo="docker-vesselalert",author_kind="human"} 1"#),
            "the aggregate count must be unchanged:\n{rendered}"
        );
        assert!(
            rendered.contains(
                r#"github_repo_pulls_ignored{org="sdr-enthusiasts",repo="docker-vesselalert"} 1"#
            ),
            "the suppression must be visible, not silent:\n{rendered}"
        );
    }

    #[test]
    fn a_pull_not_on_the_ignore_list_is_unaffected() {
        let (metrics, registry) = Metrics::new();
        let ignored = HashSet::from([PullRef::new("sdr-enthusiasts/docker-vesselalert", 32)]);

        // Same repository, different PR number.
        record_activity(
            &metrics,
            &[vesselalert()],
            &activity_with_one_stuck_pull(33),
            &ignored,
        );

        let rendered = crate::metrics::Publisher::new(metrics, registry).render();
        assert!(
            rendered.contains(r#"number="33""#),
            "ignoring #32 must not suppress #33:\n{rendered}"
        );
        assert!(rendered.contains(
            r#"github_repo_pulls_ignored{org="sdr-enthusiasts",repo="docker-vesselalert"} 0"#
        ));
    }

    #[test]
    fn an_ignore_entry_for_another_repo_does_not_match() {
        let (metrics, registry) = Metrics::new();
        // Same PR number, different repository.
        let ignored = HashSet::from([PullRef::new("fredsystems/nixos", 32)]);

        record_activity(
            &metrics,
            &[vesselalert()],
            &activity_with_one_stuck_pull(32),
            &ignored,
        );

        let rendered = crate::metrics::Publisher::new(metrics, registry).render();
        assert!(
            rendered.contains(r#"number="32""#),
            "the ignore list must be scoped per repository:\n{rendered}"
        );
    }

    #[test]
    fn zero_ignored_is_published_for_every_repository() {
        // A published zero means "nothing is hidden here" is an assertion the
        // dashboard can make, rather than an absence it must assume.
        let (metrics, registry) = Metrics::new();
        record_activity(
            &metrics,
            &[vesselalert()],
            &activity_with_one_stuck_pull(32),
            &HashSet::new(),
        );

        let rendered = crate::metrics::Publisher::new(metrics, registry).render();
        assert!(rendered.contains(
            r#"github_repo_pulls_ignored{org="sdr-enthusiasts",repo="docker-vesselalert"} 0"#
        ));
    }

    #[test]
    fn failure_does_not_clear_existing_samples() {
        let (metrics, registry) = Metrics::new();
        metrics
            .repo_monitored
            .get_or_create(&RepoLabels {
                org: "o".into(),
                repo: "r".into(),
            })
            .set(1);
        metrics.scrape_success.set(1);
        let publisher = Publisher::new(metrics, registry);

        record_failure(&publisher);

        let rendered = publisher.render();
        assert!(rendered.contains("github_exporter_scrape_success 0"));
        assert!(
            rendered.contains(r#"repo="r""#),
            "last-known-good data must survive a failed cycle"
        );
    }

    /// A cache entry from before `reduce_runs` learned to drop PR-only
    /// workflows: the run is present in the reduction even though the current
    /// reducer would never emit it.
    ///
    /// Carries a `last_success` entry as well, because that map is published
    /// by a separate loop and a replayed reduction populates both.
    fn replayed_reduction_containing(workflow: &str) -> rest::RepoRuns {
        rest::RepoRuns {
            latest: vec![rest::LatestRun {
                workflow: workflow.to_owned(),
                conclusion: crate::model::RunConclusion::Failure,
                event: "workflow_dispatch".to_owned(),
                created_at: "2025-12-13T15:55:22Z".parse().expect("timestamp"),
                html_url: "https://github.com/o/r/actions/runs/1".to_owned(),
            }],
            last_success: HashMap::from([(
                workflow.to_owned(),
                "2025-12-01T00:00:00Z".parse().expect("timestamp"),
            )]),
        }
    }

    /// Whether any rendered sample mentions `workflow`.
    ///
    /// Line-wise rather than a substring search over the whole document, so an
    /// unrelated series carrying the same conclusion cannot make an assertion
    /// pass or fail by accident.
    fn mentions_workflow(rendered: &str, workflow: &str) -> bool {
        rendered
            .lines()
            .any(|line| line.contains(&format!(r#"workflow="{workflow}""#)))
    }

    fn cache_with_workflow(repo: &Repo, name: &str, triggers: &[&str]) -> WorkflowCache {
        let mut cache = WorkflowCache::default();
        cache.workflows.insert(
            repo.full_name(),
            vec![rest::Workflow {
                id: 1,
                name: name.to_owned(),
                path: format!(".github/workflows/{name}.yml"),
                state: rest::WorkflowState::Active,
                triggers: triggers.iter().map(|t| (*t).to_owned()).collect(),
            }],
        );
        cache
    }

    #[test]
    fn a_replayed_reduction_cannot_resurrect_a_pr_only_workflow() {
        // The regression that reached production. The runs cache stores a
        // *reduction*, keyed by a fingerprint of path, name, and triggers --
        // none of which changed when the reducer learned to drop workflows
        // with no default-branch state. So `304` replayed a pre-filter
        // reduction, the fossil run came back, and because such a workflow is
        // no longer masked as stale it published as an outright `failure`.
        // Bumping the cache version fixes the cause; this asserts the rule
        // holds at publication regardless of a reduction's provenance.
        let (metrics, registry) = Metrics::new();
        let repo = Repo {
            owner: "sdr-enthusiasts".to_owned(),
            name: "sdr-e-base-repo-setup".to_owned(),
            default_branch: "main".to_owned(),
        };
        let cache = cache_with_workflow(
            &repo,
            "Lint",
            &["merge_group", "pull_request", "workflow_dispatch"],
        );

        record_runs(
            &metrics,
            &repo,
            &replayed_reduction_containing("Lint"),
            &cache,
            Utc::now(),
        );

        let rendered = Publisher::new(metrics, registry).render();
        assert!(
            !mentions_workflow(&rendered, "Lint"),
            "a PR-only workflow must publish nothing from a stale cache -- not a \
             run status, not a stale flag, and not a last-success timestamp:\n{rendered}"
        );
        // Named explicitly, because this is the series that paged.
        assert!(
            !rendered
                .lines()
                .any(|l| l.contains(r#"workflow="Lint""#) && l.contains(r#"conclusion="failure""#)),
            "and must certainly not publish a pageable failure:\n{rendered}"
        );
    }

    #[test]
    fn a_replayed_reduction_still_publishes_a_cadenced_workflow() {
        // The guard must be surgical: a cadenced workflow's old failure is
        // real history and still belongs in the output, masked as stale.
        let (metrics, registry) = Metrics::new();
        let repo = Repo {
            owner: "sdr-enthusiasts".to_owned(),
            name: "docker-jaero".to_owned(),
            default_branch: "main".to_owned(),
        };
        let cache = cache_with_workflow(&repo, "Deploy", &["push"]);

        record_runs(
            &metrics,
            &repo,
            &replayed_reduction_containing("Deploy"),
            &cache,
            Utc::now(),
        );

        let rendered = Publisher::new(metrics, registry).render();
        assert!(mentions_workflow(&rendered, "Deploy"));
        assert!(
            rendered
                .lines()
                .any(|l| l.contains(r#"workflow="Deploy""#) && l.contains(r#"conclusion="stale""#)),
            "an ancient cadenced failure is masked, not dropped:\n{rendered}"
        );
        assert!(
            rendered.contains("github_workflow_last_success_timestamp_seconds"),
            "and its last-success timestamp is still published:\n{rendered}"
        );
    }

    #[test]
    fn a_failure_clears_a_stale_budget_bypass_flag() {
        // Only non-budget errors reach record_failure -- a bypassed cycle
        // returns BypassedLowBudget rather than erroring -- so the budget was
        // affordable. Left set from an earlier bypass, the flag would survive
        // indefinitely and report a budget skip for every later failure.
        let (metrics, registry) = Metrics::new();
        metrics.budget_exhausted.set(1);
        let publisher = Publisher::new(metrics, registry);

        record_failure(&publisher);

        let rendered = publisher.render();
        assert!(
            rendered.contains("github_exporter_budget_exhausted 0"),
            "a non-budget failure must not claim the budget was exhausted:\n{rendered}"
        );
        assert!(rendered.contains("github_exporter_scrape_success 0"));
    }

    // ---- fetch_repo_runs -------------------------------------------------
    //
    // The three-stage collection: one shared unfiltered page, a per-workflow
    // top-up for what it did not cover, and reconciliation against the previous
    // cycle. Driven through a mock API because the interesting behaviour is
    // which requests get issued, not just how a response reduces.

    /// A workflow set of one scheduled workflow, addressed by id 42.
    fn scheduled_workflow() -> Vec<rest::Workflow> {
        vec![rest::Workflow {
            id: 42,
            name: "update-flakes".to_owned(),
            path: ".github/workflows/update-flakes.yaml".to_owned(),
            state: rest::WorkflowState::Active,
            triggers: vec!["schedule".to_owned(), "workflow_dispatch".to_owned()],
        }]
    }

    /// A PR-only workflow, which GitHub never runs against the default branch.
    fn pr_only_workflow() -> rest::Workflow {
        rest::Workflow {
            id: 99,
            name: "Lint".to_owned(),
            path: ".github/workflows/lint.yaml".to_owned(),
            state: rest::WorkflowState::Active,
            triggers: vec!["pull_request".to_owned(), "merge_group".to_owned()],
        }
    }

    fn test_repo() -> Repo {
        Repo {
            owner: "sdr-enthusiasts".to_owned(),
            name: "docker-beast-splitter".to_owned(),
            default_branch: "main".to_owned(),
        }
    }

    /// A runs listing holding one `schedule` run of `update-flakes`.
    fn runs_body(conclusion: &str, created: &str) -> serde_json::Value {
        serde_json::json!({
            "workflow_runs": [{
                "path": ".github/workflows/update-flakes.yaml",
                "head_branch": "main",
                "status": "completed",
                "conclusion": conclusion,
                "event": "schedule",
                "created_at": created,
                "html_url": "https://github.com/o/r/actions/runs/1",
            }]
        })
    }

    fn client_for(server: &wiremock::MockServer) -> Client {
        Client::new("t", &server.uri(), &format!("{}/graphql", server.uri()))
            .expect("client builds")
    }

    #[tokio::test]
    async fn a_workflow_the_shared_page_missed_is_topped_up() {
        // Dropping the `branch=` filter means the shared page is diluted by
        // pull-request runs, so on a busy repository it no longer reaches back
        // far enough to cover an infrequent scheduled workflow -- one page of
        // fredsystems/nixos spans about a day and speaks for 5 of its 14. The
        // workflow's own history is not diluted, so it settles the question.
        let server = wiremock::MockServer::start().await;
        // The shared page: real runs, but none of them this workflow's.
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(
                "/repos/sdr-enthusiasts/docker-beast-splitter/actions/runs",
            ))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "workflow_runs": [] })),
            )
            .expect(1)
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(
                "/repos/sdr-enthusiasts/docker-beast-splitter/actions/workflows/42/runs",
            ))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(runs_body("success", "2026-08-16T05:31:31Z")),
            )
            .expect(1)
            .mount(&server)
            .await;

        let mut cache = WorkflowCache::default();
        let runs = fetch_repo_runs(
            &client_for(&server),
            &test_repo(),
            &scheduled_workflow(),
            &mut cache,
        )
        .await
        .expect("the shared page succeeded");

        assert_eq!(runs.latest.len(), 1, "the top-up must supply the workflow");
        assert_eq!(runs.latest[0].workflow, "update-flakes");
    }

    #[tokio::test]
    async fn no_top_up_is_issued_for_a_workflow_the_page_covered() {
        // The top-up is per uncovered workflow; spending a request for one the
        // shared page already spoke for would multiply the fleet's cost for
        // nothing. `.expect(0)` on the per-workflow route is the assertion.
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(
                "/repos/sdr-enthusiasts/docker-beast-splitter/actions/runs",
            ))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(runs_body("success", "2026-08-16T05:31:31Z")),
            )
            .expect(1)
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(
                "/repos/sdr-enthusiasts/docker-beast-splitter/actions/workflows/42/runs",
            ))
            .respond_with(wiremock::ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let mut cache = WorkflowCache::default();
        let runs = fetch_repo_runs(
            &client_for(&server),
            &test_repo(),
            &scheduled_workflow(),
            &mut cache,
        )
        .await
        .expect("fetch succeeds");
        assert_eq!(runs.latest.len(), 1);
    }

    #[tokio::test]
    async fn a_pr_only_workflow_is_never_topped_up() {
        // Its runs are discarded by the reduction anyway, so a top-up could
        // only ever return something unusable. These are the majority of the
        // fleet's uncovered workflows, so skipping them is what keeps the
        // top-up affordable at all.
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(
                "/repos/sdr-enthusiasts/docker-beast-splitter/actions/runs",
            ))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "workflow_runs": [] })),
            )
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(
                "/repos/sdr-enthusiasts/docker-beast-splitter/actions/workflows/99/runs",
            ))
            .respond_with(wiremock::ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let mut cache = WorkflowCache::default();
        let runs = fetch_repo_runs(
            &client_for(&server),
            &test_repo(),
            &[pr_only_workflow()],
            &mut cache,
        )
        .await
        .expect("fetch succeeds");
        assert!(runs.latest.is_empty());
    }

    #[tokio::test]
    async fn a_later_truncated_cycle_cannot_regress_a_published_run() {
        // The end-to-end shape of the alert that paged. Cycle one observes the
        // 08-16 success. Cycle two gets a truncated page whose newest
        // update-flakes run is the 07-12 failure -- 38 days old, so inside
        // STALE_RUN_AGE and therefore published as an outright `failure` rather
        // than masked as stale. The retained observation must win.
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(
                "/repos/sdr-enthusiasts/docker-beast-splitter/actions/runs",
            ))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(runs_body("success", "2026-08-16T05:31:31Z")),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // Cycle two: the truncated page, with the fossil failure as its newest.
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(
                "/repos/sdr-enthusiasts/docker-beast-splitter/actions/runs",
            ))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(runs_body("failure", "2026-07-12T07:30:11Z")),
            )
            .mount(&server)
            .await;

        let client = client_for(&server);
        let repo = test_repo();
        let live = scheduled_workflow();
        let mut cache = WorkflowCache::default();

        let first = fetch_repo_runs(&client, &repo, &live, &mut cache)
            .await
            .expect("cycle one");
        assert_eq!(
            first.latest[0].conclusion,
            crate::model::RunConclusion::Success
        );

        let second = fetch_repo_runs(&client, &repo, &live, &mut cache)
            .await
            .expect("cycle two");
        assert_eq!(
            second.latest[0].conclusion,
            crate::model::RunConclusion::Success,
            "a truncated page must not publish a fossil failure"
        );
        assert_eq!(
            second.latest[0].created_at.to_rfc3339(),
            "2026-08-16T05:31:31+00:00",
            "nor regress the timestamp the staleness alert reads"
        );
    }

    #[tokio::test]
    async fn a_failed_shared_page_publishes_nothing_rather_than_stale_data() {
        // `scrape_success` already reports the failure. Republishing the
        // retained reduction as though it were fresh is how a GitHub outage
        // comes to look like a healthy fleet.
        //
        // 404 rather than 500 deliberately: a 5xx is retryable, so it would
        // spend this test in `classify_retry`'s exponential backoff -- 14
        // seconds of real sleeping for an assertion that has nothing to do with
        // retrying.
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(
                "/repos/sdr-enthusiasts/docker-beast-splitter/actions/runs",
            ))
            .respond_with(wiremock::ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let mut cache = WorkflowCache::default();
        assert!(
            fetch_repo_runs(
                &client_for(&server),
                &test_repo(),
                &scheduled_workflow(),
                &mut cache,
            )
            .await
            .is_none()
        );
    }

    #[tokio::test]
    async fn a_repository_losing_its_workflows_drops_its_retained_runs() {
        // Otherwise a repository that goes quiet keeps a retained reduction
        // that `reconciled_with` would resurrect if it ever regained a
        // workflow file.
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(
                "/repos/sdr-enthusiasts/docker-beast-splitter/actions/runs",
            ))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(runs_body("failure", "2026-07-12T07:30:11Z")),
            )
            .mount(&server)
            .await;
        // An empty workflow list, which is what retires the repository.
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(
                "/repos/sdr-enthusiasts/docker-beast-splitter/actions/workflows",
            ))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "workflows": [] })),
            )
            .mount(&server)
            .await;

        let client = client_for(&server);
        let repo = test_repo();
        let mut cache = WorkflowCache::default();
        fetch_repo_runs(&client, &repo, &scheduled_workflow(), &mut cache)
            .await
            .expect("first cycle");
        assert!(cache.runs.contains_key(&repo.full_name()));

        let config = Config::default();
        let mut skipped = Vec::new();
        let monitored = resolve_monitored(
            &client,
            &config,
            &mut cache,
            vec![repo.clone()],
            &mut skipped,
        )
        .await;

        assert!(monitored.is_empty(), "a repository with no CI is skipped");
        assert!(
            !cache.runs.contains_key(&repo.full_name()),
            "and must not leave a retained reduction behind"
        );
    }
}
