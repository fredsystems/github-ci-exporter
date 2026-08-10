//! Prometheus metric definitions and rendering.
//!
//! The registry is rebuilt from scratch on every successful poll rather than
//! mutated in place. A repository that disappears, or a workflow that is
//! deleted, must stop producing samples immediately; incrementally updating a
//! long-lived registry would leave those series exposed forever and keep a
//! resolved alert firing.

use std::sync::{Arc, Mutex};

use prometheus_client::{
    encoding::{EncodeLabelSet, text::encode},
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};

use crate::model::AuthorKind;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RepoLabels {
    pub org: String,
    pub repo: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct AuthorLabels {
    pub org: String,
    pub repo: String,
    pub author_kind: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PullLabels {
    pub org: String,
    pub repo: String,
    pub number: String,
    pub author: String,
    pub author_kind: String,
    pub draft: String,
    /// Rollup state of the head commit's checks: success, failure, pending,
    /// none, or unknown.
    pub checks: String,
    pub mergeable: String,
    /// Whether auto-merge is armed. A green, mergeable PR with this "false"
    /// is waiting on a human.
    pub auto_merge: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct WorkflowLabels {
    pub org: String,
    pub repo: String,
    pub workflow: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct WorkflowEnabledLabels {
    pub org: String,
    pub repo: String,
    pub workflow: String,
    pub state: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct WorkflowStateLabels {
    pub org: String,
    pub repo: String,
    pub workflow: String,
    pub event: String,
    pub conclusion: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SkipLabels {
    pub reason: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ResourceLabels {
    pub resource: String,
}

/// Every metric family the exporter publishes.
#[derive(Debug)]
pub struct Metrics {
    pub issues_open: Family<AuthorLabels, Gauge>,
    pub pulls_open: Family<AuthorLabels, Gauge>,
    pub pulls_draft: Family<AuthorLabels, Gauge>,
    pub pull_created_timestamp: Family<PullLabels, Gauge>,
    pub pull_needs_attention: Family<PullLabels, Gauge>,
    pub pull_ready_to_merge: Family<PullLabels, Gauge>,
    pub workflow_run_status: Family<WorkflowStateLabels, Gauge>,
    pub workflow_run_timestamp: Family<WorkflowLabels, Gauge>,
    pub workflow_last_success_timestamp: Family<WorkflowLabels, Gauge>,
    pub workflow_expected_interval: Family<WorkflowLabels, Gauge>,
    pub workflow_enabled: Family<WorkflowEnabledLabels, Gauge>,
    pub workflow_run_stale: Family<WorkflowLabels, Gauge>,
    pub repo_monitored: Family<RepoLabels, Gauge>,
    pub repos_skipped: Family<SkipLabels, Gauge>,
    pub rate_limit_remaining: Family<ResourceLabels, Gauge>,
    pub rate_limit_limit: Family<ResourceLabels, Gauge>,
    pub rate_limit_reset: Family<ResourceLabels, Gauge>,
    pub rate_limit_reserve: Gauge,
    pub budget_exhausted: Gauge,
    pub cycles_bypassed: Counter,
    pub api_requests_skipped: Gauge,
    pub scrape_success: Gauge,
    pub scrape_duration: Gauge<f64, std::sync::atomic::AtomicU64>,
    pub last_success_timestamp: Gauge,
    pub api_requests_total: Gauge,
    pub api_not_modified_total: Gauge,
}

impl Metrics {
    /// Builds a registry with every family registered.
    #[must_use]
    #[allow(clippy::too_many_lines)] // A flat list of metric registrations.
    pub fn new() -> (Self, Registry) {
        let mut registry = Registry::with_prefix("github");

        let issues_open = Family::<AuthorLabels, Gauge>::default();
        registry.register(
            "repo_issues_open",
            "Open issues by author kind (human or bot)",
            issues_open.clone(),
        );

        let pulls_open = Family::<AuthorLabels, Gauge>::default();
        registry.register(
            "repo_pulls_open",
            "Open pull requests by author kind (human or bot)",
            pulls_open.clone(),
        );

        let pulls_draft = Family::<AuthorLabels, Gauge>::default();
        registry.register(
            "repo_pulls_draft",
            "Open draft pull requests by author kind",
            pulls_draft.clone(),
        );

        let pull_created_timestamp = Family::<PullLabels, Gauge>::default();
        registry.register(
            "pull_created_timestamp_seconds",
            "Creation time of each open pull request",
            pull_created_timestamp.clone(),
        );

        let pull_needs_attention = Family::<PullLabels, Gauge>::default();
        registry.register(
            "pull_needs_attention",
            "1 for an open non-draft PR that is failing checks, conflicting, or green and awaiting a manual merge",
            pull_needs_attention.clone(),
        );

        let pull_ready_to_merge = Family::<PullLabels, Gauge>::default();
        registry.register(
            "pull_ready_to_merge",
            "1 for an open non-draft PR whose checks pass and which is mergeable, but has no auto-merge armed",
            pull_ready_to_merge.clone(),
        );

        let workflow_run_status = Family::<WorkflowStateLabels, Gauge>::default();
        registry.register(
            "workflow_run_status",
            "Current conclusion of the latest run per workflow on the default branch (1 = active)",
            workflow_run_status.clone(),
        );

        let workflow_run_timestamp = Family::<WorkflowLabels, Gauge>::default();
        registry.register(
            "workflow_run_timestamp_seconds",
            "Start time of the latest run per workflow",
            workflow_run_timestamp.clone(),
        );

        let workflow_last_success_timestamp = Family::<WorkflowLabels, Gauge>::default();
        registry.register(
            "workflow_last_success_timestamp_seconds",
            "Start time of the most recent successful run per workflow",
            workflow_last_success_timestamp.clone(),
        );

        let workflow_expected_interval = Family::<WorkflowLabels, Gauge>::default();
        registry.register(
            "workflow_expected_interval_seconds",
            "Expected interval between runs, derived from the workflow's cron schedule",
            workflow_expected_interval.clone(),
        );

        let workflow_enabled = Family::<WorkflowEnabledLabels, Gauge>::default();
        registry.register(
            "workflow_enabled",
            "1 if GitHub will run this workflow; state distinguishes an inactivity auto-disable from a manual one",
            workflow_enabled.clone(),
        );

        let workflow_run_stale = Family::<WorkflowLabels, Gauge>::default();
        registry.register(
            "workflow_run_stale",
            "1 if the latest branch-state run is too old to describe current code",
            workflow_run_stale.clone(),
        );

        let repo_monitored = Family::<RepoLabels, Gauge>::default();
        registry.register(
            "repo_monitored",
            "1 for each repository currently being monitored",
            repo_monitored.clone(),
        );

        let repos_skipped = Family::<SkipLabels, Gauge>::default();
        registry.register(
            "repos_skipped",
            "Repositories excluded from monitoring, by reason",
            repos_skipped.clone(),
        );

        let rate_limit_remaining = Family::<ResourceLabels, Gauge>::default();
        registry.register(
            "exporter_rate_limit_remaining",
            "Remaining GitHub API rate-limit budget",
            rate_limit_remaining.clone(),
        );

        let rate_limit_limit = Family::<ResourceLabels, Gauge>::default();
        registry.register(
            "exporter_rate_limit_limit",
            "Total GitHub API rate-limit budget",
            rate_limit_limit.clone(),
        );

        let rate_limit_reset = Family::<ResourceLabels, Gauge>::default();
        registry.register(
            "exporter_rate_limit_reset_timestamp_seconds",
            "Time at which the rate-limit budget resets",
            rate_limit_reset.clone(),
        );

        let rate_limit_reserve = Gauge::default();
        registry.register(
            "exporter_rate_limit_reserve",
            "Requests deliberately left unspent in each bucket",
            rate_limit_reserve.clone(),
        );

        let budget_exhausted = Gauge::default();
        registry.register(
            "exporter_budget_exhausted",
            "1 if the last cycle was skipped because the API budget was too low",
            budget_exhausted.clone(),
        );

        let cycles_bypassed = Counter::default();
        registry.register(
            "exporter_cycles_bypassed",
            "Collection cycles skipped entirely due to insufficient API budget",
            cycles_bypassed.clone(),
        );

        let api_requests_skipped = Gauge::default();
        registry.register(
            "exporter_api_requests_skipped",
            "Requests not attempted because the rate-limit budget was too low",
            api_requests_skipped.clone(),
        );

        let scrape_success = Gauge::default();
        registry.register(
            "exporter_scrape_success",
            "1 if the most recent collection cycle completed without error",
            scrape_success.clone(),
        );

        let scrape_duration = Gauge::<f64, std::sync::atomic::AtomicU64>::default();
        registry.register(
            "exporter_scrape_duration_seconds",
            "Duration of the most recent collection cycle",
            scrape_duration.clone(),
        );

        let last_success_timestamp = Gauge::default();
        registry.register(
            "exporter_last_success_timestamp_seconds",
            "Completion time of the most recent successful collection cycle",
            last_success_timestamp.clone(),
        );

        let api_requests_total = Gauge::default();
        registry.register(
            "exporter_api_requests",
            "Total GitHub API requests issued since start",
            api_requests_total.clone(),
        );

        let api_not_modified_total = Gauge::default();
        registry.register(
            "exporter_api_not_modified",
            "Requests answered 304 Not Modified, which are not rate-limited",
            api_not_modified_total.clone(),
        );

        (
            Self {
                issues_open,
                pulls_open,
                pulls_draft,
                pull_created_timestamp,
                pull_needs_attention,
                pull_ready_to_merge,
                workflow_run_status,
                workflow_run_timestamp,
                workflow_last_success_timestamp,
                workflow_expected_interval,
                workflow_enabled,
                workflow_run_stale,
                repo_monitored,
                repos_skipped,
                rate_limit_remaining,
                rate_limit_limit,
                rate_limit_reset,
                rate_limit_reserve,
                budget_exhausted,
                cycles_bypassed,
                api_requests_skipped,
                scrape_success,
                scrape_duration,
                last_success_timestamp,
                api_requests_total,
                api_not_modified_total,
            },
            registry,
        )
    }

    /// Clears every per-repository family.
    ///
    /// Called at the start of each cycle so deleted repositories, closed pull
    /// requests, and removed workflows stop being reported.
    pub fn clear_repo_series(&self) {
        self.issues_open.clear();
        self.pulls_open.clear();
        self.pulls_draft.clear();
        self.pull_created_timestamp.clear();
        self.pull_needs_attention.clear();
        self.pull_ready_to_merge.clear();
        self.workflow_run_status.clear();
        self.workflow_run_timestamp.clear();
        self.workflow_last_success_timestamp.clear();
        self.workflow_expected_interval.clear();
        self.workflow_enabled.clear();
        self.workflow_run_stale.clear();
        self.repo_monitored.clear();
        self.repos_skipped.clear();
    }
}

/// Shared registry guarded for the HTTP handler.
#[derive(Clone)]
pub struct SharedRegistry(Arc<Mutex<Registry>>);

impl std::fmt::Debug for SharedRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SharedRegistry")
    }
}

impl SharedRegistry {
    #[must_use]
    pub fn new(registry: Registry) -> Self {
        Self(Arc::new(Mutex::new(registry)))
    }

    /// Renders the registry in the Prometheus text exposition format.
    #[must_use]
    pub fn render(&self) -> String {
        let mut buffer = String::new();
        if let Ok(registry) = self.0.lock() {
            // Writing into a String is infallible for these metric types.
            let _: Result<(), std::fmt::Error> = encode(&mut buffer, &registry);
        }
        buffer
    }
}

/// Renders an [`AuthorKind`] as a label value.
#[must_use]
pub fn author_label(kind: AuthorKind) -> String {
    kind.as_str().to_owned()
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
    fn registry_uses_github_prefix_and_exposes_families() {
        let (metrics, registry) = Metrics::new();
        metrics
            .issues_open
            .get_or_create(&AuthorLabels {
                org: "fredsystems".into(),
                repo: "nixos".into(),
                author_kind: "human".into(),
            })
            .set(4);

        let shared = SharedRegistry::new(registry);
        let rendered = shared.render();

        assert!(
            rendered.contains("github_repo_issues_open"),
            "expected prefixed metric name, got:\n{rendered}"
        );
        assert!(rendered.contains(r#"author_kind="human""#));
        assert!(rendered.contains(r#"repo="nixos""#));
    }

    #[test]
    fn clearing_removes_stale_repo_series() {
        let (metrics, registry) = Metrics::new();
        metrics
            .repo_monitored
            .get_or_create(&RepoLabels {
                org: "o".into(),
                repo: "gone".into(),
            })
            .set(1);

        let shared = SharedRegistry::new(registry);
        assert!(shared.render().contains(r#"repo="gone""#));

        metrics.clear_repo_series();
        assert!(
            !shared.render().contains(r#"repo="gone""#),
            "a deleted repository must stop producing samples"
        );
    }

    #[test]
    fn self_monitoring_metrics_survive_repo_clear() {
        // Clearing repo series must not wipe exporter health, or a failing
        // scrape would look like a healthy empty one.
        let (metrics, registry) = Metrics::new();
        metrics.scrape_success.set(1);
        metrics.clear_repo_series();

        let shared = SharedRegistry::new(registry);
        assert!(shared.render().contains("github_exporter_scrape_success 1"));
    }

    #[test]
    fn workflow_status_encodes_conclusion_label() {
        let (metrics, registry) = Metrics::new();
        metrics
            .workflow_run_status
            .get_or_create(&WorkflowStateLabels {
                org: "sdr-enthusiasts".into(),
                repo: "docker-tar1090".into(),
                workflow: "Deploy".into(),
                event: "schedule".into(),
                conclusion: "failure".into(),
            })
            .set(1);

        let rendered = SharedRegistry::new(registry).render();
        assert!(rendered.contains(r#"conclusion="failure""#));
        assert!(rendered.contains(r#"workflow="Deploy""#));
    }
}
