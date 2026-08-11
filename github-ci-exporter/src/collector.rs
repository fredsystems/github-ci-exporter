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

/// Estimates the REST budget a cycle will consume.
///
/// Two requests per repository (workflow list + runs list), plus headroom for
/// the cron-schedule lookups performed for newly-seen repositories.
const fn estimate_core_requests(monitored: u64) -> u64 {
    let repos = if monitored == 0 {
        ASSUMED_FIRST_CYCLE_REPOS
    } else {
        monitored
    };
    repos.saturating_mul(2).saturating_add(50)
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
    /// Repositories monitored by the previous cycle, used to size the
    /// budget pre-flight check.
    monitored_count: u64,
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
    let estimated_core = estimate_core_requests(cache.monitored_count());
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

    // Actions runs, one request per repository.
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
        record_workflow_states(metrics, repo, &live);
        match rest::fetch_runs(client, repo, &live).await {
            Ok(runs) => record_runs(metrics, repo, &runs, cache, now),
            Err(error) => warn!(repo = %repo, %error, "failed to fetch workflow runs"),
        }
    }

    // Self-monitoring.
    cache.monitored_count = u64::try_from(monitored.len()).unwrap_or(u64::MAX);
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
                if let Some(interval) = shortest_cron_interval(&definition.crons) {
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

    for run in &runs.latest {
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
        let cadenced = signal_for_name
            .get(run.workflow.as_str())
            .copied()
            .unwrap_or(rest::DefaultBranchSignal::Cadenced)
            == rest::DefaultBranchSignal::Cadenced;
        let stale = cadenced && run.is_stale(now);
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

/// Derives the shortest interval between fires across a set of cron
/// expressions, in seconds.
///
/// Used to detect scheduled workflows that have silently stopped running —
/// GitHub disables cron triggers on repositories with no activity for 60 days.
#[must_use]
pub fn shortest_cron_interval(crons: &[String]) -> Option<i64> {
    use std::str::FromStr as _;

    let mut shortest: Option<i64> = None;
    for expression in crons {
        // GitHub uses 5-field POSIX cron; the `cron` crate expects a seconds
        // field, so one is prepended.
        let normalised = format!("0 {expression}");
        let Ok(schedule) = cron::Schedule::from_str(&normalised) else {
            continue;
        };
        let base = DateTime::<Utc>::from_timestamp(0, 0)?;
        let mut upcoming = schedule.after(&base);
        let (Some(first), Some(second)) = (upcoming.next(), upcoming.next()) else {
            continue;
        };
        let delta = (second - first).num_seconds();
        if delta > 0 {
            shortest = Some(shortest.map_or(delta, |current: i64| current.min(delta)));
        }
    }
    shortest
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
        let interval = shortest_cron_interval(&["0 12 * * *".to_owned()]).expect("interval");
        assert_eq!(interval, 86_400);
    }

    #[test]
    fn weekly_cron_yields_seven_days() {
        let interval = shortest_cron_interval(&["0 0 * * 1".to_owned()]).expect("interval");
        assert_eq!(interval, 604_800);
    }

    #[test]
    fn shortest_wins_across_multiple_crons() {
        let interval = shortest_cron_interval(&[
            "0 0 * * 1".to_owned(),  // weekly
            "0 12 * * *".to_owned(), // daily
        ])
        .expect("interval");
        assert_eq!(interval, 86_400);
    }

    #[test]
    fn invalid_cron_is_ignored_not_fatal() {
        assert!(shortest_cron_interval(&["not a cron".to_owned()]).is_none());
        assert!(shortest_cron_interval(&[]).is_none());
    }

    #[test]
    fn invalid_cron_does_not_hide_a_valid_one() {
        let interval = shortest_cron_interval(&["nonsense".to_owned(), "0 12 * * *".to_owned()])
            .expect("valid expression should still resolve");
        assert_eq!(interval, 86_400);
    }

    fn wf(name: &str, triggers: &[&str]) -> rest::Workflow {
        rest::Workflow {
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
        // Before any cycle completes the repo count is unknown; the estimate
        // must be pessimistic rather than zero.
        assert_eq!(
            estimate_core_requests(0),
            ASSUMED_FIRST_CYCLE_REPOS * 2 + 50
        );
    }

    #[test]
    fn estimate_scales_with_monitored_repositories() {
        assert_eq!(estimate_core_requests(61), 172);
        assert!(estimate_core_requests(61) < estimate_core_requests(100));
    }

    #[test]
    fn estimate_cannot_overflow() {
        assert_eq!(estimate_core_requests(u64::MAX), u64::MAX);
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
}
