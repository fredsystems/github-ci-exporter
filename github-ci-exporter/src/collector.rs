//! Orchestrates one collection cycle.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use tracing::{debug, error, info, warn};

use crate::{
    config::Config,
    github::{Client, client::RateLimitResource, graphql, rest},
    metrics::{
        AuthorLabels, Metrics, PullLabels, RepoLabels, ResourceLabels, SkipLabels,
        WorkflowEnabledLabels, WorkflowLabels, WorkflowStateLabels, author_label,
    },
    model::{Repo, SkipReason},
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
    /// `owner/name` -> workflow name -> expected interval in seconds.
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
}

impl WorkflowCache {
    /// Repositories monitored last cycle; 0 before the first completes.
    #[must_use]
    pub const fn monitored_count(&self) -> u64 {
        self.monitored_count
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

/// Runs a single collection cycle, updating `metrics` in place.
///
/// A failure for one repository is logged and skipped rather than aborting the
/// cycle: one broken repository must not blind the operator to the other
/// sixty.
///
/// # Errors
/// Returns an error only when discovery fails for every configured
/// organisation, which means the cycle produced no usable data at all.
pub async fn collect(
    client: &Client,
    config: &Config,
    metrics: &Metrics,
    cache: &mut WorkflowCache,
) -> anyhow::Result<CycleOutcome> {
    let started = std::time::Instant::now();
    let now = Utc::now();

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
        record_budget_metrics(client, metrics);
        metrics.cycles_bypassed.inc();
        metrics.budget_exhausted.set(1);
        return Ok(CycleOutcome::BypassedLowBudget);
    }
    metrics.budget_exhausted.set(0);

    // Discovery
    let mut discovered = Vec::new();
    let mut discovery_failures = 0;
    for org in &config.orgs {
        match graphql::discover_org(client, org).await {
            Ok(repos) => {
                debug!(org, count = repos.len(), "discovered repositories");
                discovered.extend(repos);
            }
            Err(error) => {
                discovery_failures += 1;
                error!(org, %error, "failed to discover organisation");
            }
        }
    }
    if discovery_failures == config.orgs.len() {
        anyhow::bail!("discovery failed for every configured organisation");
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

    // Rebuild all per-repository series from scratch.
    metrics.clear_repo_series();

    record_repo_inventory(metrics, &monitored, &skipped);

    // Issues and pull requests, batched.
    match graphql::fetch_activity(client, &monitored).await {
        Ok(activity) => record_activity(metrics, &monitored, &activity),
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
        let intervals = resolve_definitions(client, &repo, &mut workflows).await;
        cache.workflows.insert(key.clone(), workflows);
        cache.intervals.insert(key, intervals);

        monitored.push(repo);
    }

    monitored
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
                    intervals.insert(workflow.name.clone(), interval);
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

fn record_activity(
    metrics: &Metrics,
    monitored: &[Repo],
    activity: &std::collections::BTreeMap<String, graphql::RepoActivity>,
) {
    for repo in monitored {
        let Some(entry) = activity.get(&repo.full_name()) else {
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

        for pull in &entry.open_pulls {
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
    }
}

fn record_runs(
    metrics: &Metrics,
    repo: &Repo,
    runs: &rest::RepoRuns,
    cache: &WorkflowCache,
    now: DateTime<Utc>,
) {
    let intervals = cache.intervals.get(&repo.full_name());

    for run in &runs.latest {
        // A run older than the staleness horizon says nothing about the
        // current code. Reporting `stale` instead of its original conclusion
        // keeps an ancient failure from producing an alert that cannot be
        // cleared without an artificial push. `workflow_run_stale` carries the
        // fact separately so it stays visible on the dashboard.
        let stale = run.is_stale(now);
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

        if let Some(interval) = intervals.and_then(|m| m.get(&run.workflow)) {
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
/// signals the staleness.
pub fn record_failure(metrics: &Metrics) {
    metrics.scrape_success.set(0);
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

        record_failure(&metrics);

        let rendered = crate::metrics::SharedRegistry::new(registry).render();
        assert!(rendered.contains("github_exporter_scrape_success 0"));
        assert!(
            rendered.contains(r#"repo="r""#),
            "last-known-good data must survive a failed cycle"
        );
    }
}
