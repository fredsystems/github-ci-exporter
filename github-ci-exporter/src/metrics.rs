//! Prometheus metric definitions and rendering.
//!
//! A repository that disappears, or a workflow that is deleted, must stop
//! producing samples immediately; incrementally updating a long-lived registry
//! would leave those series exposed forever and keep a resolved alert firing.
//!
//! So each cycle builds a **new** [`Metrics`] and [`Registry`] and hands them
//! to [`Publisher::publish`], which swaps them in under one lock. A scrape
//! therefore sees either the previous cycle in full or the new one in full,
//! never a mixture.
//!
//! That atomicity is the whole point, and it is not a refinement. Clearing the
//! live families and refilling them in place — which is what this did
//! originally — publishes a partially-rebuilt registry for as long as the
//! cycle takes to run. Measured against `sdr-enthusiasts`, that was an 81
//! second window in which `workflow_run_status` climbed from 0 to 93 series
//! while `/metrics` served every intermediate state. With a 15s scrape
//! interval, roughly six scrapes per cycle landed in the gap, and any absent
//! sample resets an alert's `for:` timer. A `for: 15m` rule covering a
//! repository late in the rebuild order could never fire at all: the gap
//! recurred every cycle, well inside the 15 minutes it needed to accumulate.
//! A genuinely failing workflow went unalerted for its entire lifetime.
//!
//! The per-repository families are consequently never cleared. A fresh
//! registry starts empty, which makes "stop reporting what no longer exists"
//! and "never expose a half-built scrape" the same mechanism.

use std::sync::{Arc, Mutex};

use prometheus_client::{
    encoding::{
        EncodeLabelSet,
        text::{encode_eof, encode_registry},
    },
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};

use crate::model::{AuthorKind, RepoIndex};

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

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct FlakeInputLabels {
    pub repo: String,
    /// The root input this node belongs to -- itself for a root input, or the
    /// owning root input for a nested owned-nixpkgs node.
    pub input: String,
    /// The full input path, e.g. `"niri"` or `"niri/nixpkgs"`.
    pub path: String,
    /// The `ref` this node is locked to. Empty for the default branch and
    /// for anything not locked through `github`.
    pub r#ref: String,
    /// The NixOS channel this node tracks. Empty if it does not track one.
    pub channel: String,
}

/// Labels for `flake_input_commits_behind`.
///
/// No `channel` label: it is published only for a `Branch` node that is
/// behind, and a commit count means nothing for a tag-pinned or
/// channel-tracking comparison.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct FlakeInputCommitsBehindLabels {
    pub repo: String,
    pub input: String,
    pub path: String,
    pub r#ref: String,
}

/// Labels for `flake_input_update_pr_open`. `number` matches the label on the
/// existing `github_pull_*` series, so an update PR joins to its age and
/// check state.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct FlakeInputPrLabels {
    pub repo: String,
    pub input: String,
    pub number: String,
}

/// Labels for `flake_input_impact`, read from the flake-watch Action's status
/// file.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct FlakeInputImpactLabels {
    pub repo: String,
    pub input: String,
    /// `linux` or `darwin`.
    pub os: String,
    /// `none`, `rebuild`, or `source` -- see
    /// `PLAN_03_FLAKE_INPUT_TRACKING.md`'s "Impact is decided by evaluation,
    /// not by file names".
    pub kind: String,
}

/// Labels for the `nixos_channel_*` family -- see [`ChannelMetrics`].
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ChannelLabels {
    pub channel: String,
}

/// The (empty) label set for `nixos_channel_status_up`.
///
/// A plain [`Gauge`] always encodes, even at its default value, so one would
/// make `nixos_channel_status_up 0` appear before the exporter has ever
/// attempted a fetch. Wrapping it in a [`Family`] with no labels lets it stay
/// genuinely absent -- an empty family is omitted entirely -- until
/// [`crate::flake::channels::record_metrics`] calls `get_or_create` for the
/// first time, which only happens once an attempt has actually been made.
#[derive(Clone, Debug, Default, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ChannelAttemptLabels {}

/// NixOS channel status -- see `crate::flake::channels`.
///
/// Deliberately a separate struct with its own [`Registry`], not more fields
/// on [`Metrics`]: these metrics are rendered **unprefixed**
/// (`nixos_channel_*`), alongside the exporter's own `github_`-prefixed
/// registry in the same `/metrics` response, because they describe NixOS
/// infrastructure this exporter merely observes rather than anything of its
/// own. See [`Publisher::publish_with_channels`] for how the two registries
/// are kept in step.
#[derive(Debug)]
pub struct ChannelMetrics {
    pub update_timestamp: Family<ChannelLabels, Gauge>,
    pub current: Family<ChannelLabels, Gauge>,
    pub tested_failed: Family<ChannelLabels, Gauge>,
    pub status_up: Family<ChannelAttemptLabels, Gauge>,
}

impl ChannelMetrics {
    /// Builds the unprefixed `nixos_channel` registry.
    #[must_use]
    pub fn new() -> (Self, Registry) {
        let mut registry = Registry::with_prefix("nixos_channel");

        let update_timestamp = Family::<ChannelLabels, Gauge>::default();
        registry.register(
            "update_timestamp_seconds",
            "When this NixOS channel's revision last changed, per prometheus.nixos.org",
            update_timestamp.clone(),
        );

        let current = Family::<ChannelLabels, Gauge>::default();
        registry.register(
            "current",
            "1 if this NixOS channel is still supported, 0 if it is end of life",
            current.clone(),
        );

        let tested_failed = Family::<ChannelLabels, Gauge>::default();
        registry.register(
            "tested_failed",
            "1 if this channel's tested Hydra job is failing; absent if never evaluated",
            tested_failed.clone(),
        );

        let status_up = Family::<ChannelAttemptLabels, Gauge>::default();
        registry.register(
            "status_up",
            "1 if the last NixOS channel status fetch succeeded, 0 if it failed and cached values are being served; absent before the first attempt",
            status_up.clone(),
        );

        (
            Self {
                update_timestamp,
                current,
                tested_failed,
                status_up,
            },
            registry,
        )
    }
}

/// Every metric family the exporter publishes.
#[derive(Debug)]
pub struct Metrics {
    pub issues_open: Family<AuthorLabels, Gauge>,
    pub pulls_open: Family<AuthorLabels, Gauge>,
    pub pulls_draft: Family<AuthorLabels, Gauge>,
    /// Open pull requests suppressed by the operator's ignore list. Makes the
    /// gap between `pulls_open` and the per-PR series explainable.
    pub pulls_ignored: Family<RepoLabels, Gauge>,
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
    /// Lock time of each tracked flake input. The one metric derivable from
    /// `flake.lock` alone -- see `crate::flake`.
    pub flake_input_locked_timestamp: Family<FlakeInputLabels, Gauge>,
    /// Upstream commit/tag time for each tracked flake input -- see
    /// `crate::flake::upstream`.
    pub flake_input_upstream_timestamp: Family<FlakeInputLabels, Gauge>,
    /// 1 if up to date, 0 if behind; no series when the comparison is
    /// unknown.
    pub flake_input_up_to_date: Family<FlakeInputLabels, Gauge>,
    /// When a node that is behind was first observed that way, measured from
    /// the oldest missing change -- never resets while still behind.
    pub flake_input_outdated_since: Family<FlakeInputLabels, Gauge>,
    /// Commits behind upstream, for a `Branch` node that is behind.
    pub flake_input_commits_behind: Family<FlakeInputCommitsBehindLabels, Gauge>,
    /// 1 for each open `update-<input>` pull request found this pass.
    pub flake_input_update_pr_open: Family<FlakeInputPrLabels, Gauge>,
    /// 1 for each OS/kind the flake-watch Action's status file recorded for
    /// an input.
    pub flake_input_impact: Family<FlakeInputImpactLabels, Gauge>,
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

        let pulls_ignored = Family::<RepoLabels, Gauge>::default();
        registry.register(
            "repo_pulls_ignored",
            "Open pull requests suppressed by the operator's ignore list and therefore absent from the per-PR series",
            pulls_ignored.clone(),
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

        let flake_input_locked_timestamp = Family::<FlakeInputLabels, Gauge>::default();
        registry.register(
            "flake_input_locked_timestamp_seconds",
            "Lock time of each tracked flake input, read from flake.lock alone",
            flake_input_locked_timestamp.clone(),
        );

        let flake_input_upstream_timestamp = Family::<FlakeInputLabels, Gauge>::default();
        registry.register(
            "flake_input_upstream_timestamp_seconds",
            "Upstream commit (branch) or release (tag) time for each tracked flake input",
            flake_input_upstream_timestamp.clone(),
        );

        let flake_input_up_to_date = Family::<FlakeInputLabels, Gauge>::default();
        registry.register(
            "flake_input_up_to_date",
            "1 if a tracked flake input is up to date with its upstream, 0 if behind; absent if unknown",
            flake_input_up_to_date.clone(),
        );

        let flake_input_outdated_since = Family::<FlakeInputLabels, Gauge>::default();
        registry.register(
            "flake_input_outdated_since_timestamp_seconds",
            "When a flake input that is behind was first observed that way, from its oldest missing change",
            flake_input_outdated_since.clone(),
        );

        let flake_input_commits_behind = Family::<FlakeInputCommitsBehindLabels, Gauge>::default();
        registry.register(
            "flake_input_commits_behind",
            "Commits a branch-tracking flake input is behind its upstream",
            flake_input_commits_behind.clone(),
        );

        let flake_input_update_pr_open = Family::<FlakeInputPrLabels, Gauge>::default();
        registry.register(
            "flake_input_update_pr_open",
            "1 for each open update-<input> pull request found for a tracked flake input",
            flake_input_update_pr_open.clone(),
        );

        let flake_input_impact = Family::<FlakeInputImpactLabels, Gauge>::default();
        registry.register(
            "flake_input_impact",
            "1 for each OS/kind the flake-watch Action's status file recorded for an input",
            flake_input_impact.clone(),
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
                pulls_ignored,
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
                flake_input_locked_timestamp,
                flake_input_upstream_timestamp,
                flake_input_up_to_date,
                flake_input_outdated_since,
                flake_input_commits_behind,
                flake_input_update_pr_open,
                flake_input_impact,
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

    /// Counters that must survive a registry swap.
    ///
    /// Everything else is either recomputed from the client each cycle or
    /// describes only the cycle that just ran. A monotonic counter is neither:
    /// restarting it at zero on every swap would read as a process restart and
    /// destroy `increase()` over any window spanning one.
    fn carry_forward_from(&self, previous: &Self) {
        self.cycles_bypassed.inc_by(previous.cycles_bypassed.get());
    }
}

/// The metric set currently being served.
///
/// Holds the [`Metrics`] handles alongside their [`Registry`] so a cycle can
/// replace both together. Keeping the handles is what allows the failure and
/// budget-bypass paths to annotate the *published* set in place instead of
/// swapping in a fresh one, which is how last-known-good data survives a cycle
/// that produced none.
#[derive(Clone)]
pub struct Publisher(Arc<Mutex<Published>>);

struct Published {
    metrics: Arc<Metrics>,
    registry: Registry,
    /// The unprefixed `nixos_channel_*` registry -- see [`ChannelMetrics`].
    /// Defaults to an empty [`Registry`] (nothing registered, so it encodes
    /// to nothing) until the first call to
    /// [`Publisher::publish_with_channels`].
    channel_registry: Registry,
    repo_index: Arc<RepoIndex>,
}

impl std::fmt::Debug for Publisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Publisher")
    }
}

impl Publisher {
    #[must_use]
    pub fn new(metrics: Metrics, registry: Registry) -> Self {
        Self(Arc::new(Mutex::new(Published {
            metrics: Arc::new(metrics),
            registry,
            channel_registry: Registry::default(),
            repo_index: Arc::new(RepoIndex::default()),
        })))
    }

    /// As [`Self::new`], additionally seeding the served `nixos_channel_*`
    /// registry rather than leaving it empty.
    #[must_use]
    pub fn new_with_channels(
        metrics: Metrics,
        registry: Registry,
        channel_registry: Registry,
    ) -> Self {
        Self(Arc::new(Mutex::new(Published {
            metrics: Arc::new(metrics),
            registry,
            channel_registry,
            repo_index: Arc::new(RepoIndex::default()),
        })))
    }

    /// Replaces the served `github`-prefixed metric set with
    /// `metrics`/`registry`, leaving the `nixos_channel_*` registry as it was.
    ///
    /// Monotonic counters are carried across so the swap is invisible to
    /// `rate()` and `increase()`.
    pub fn publish(&self, metrics: Metrics, registry: Registry) {
        if let Ok(mut published) = self.0.lock() {
            metrics.carry_forward_from(&published.metrics);
            published.metrics = Arc::new(metrics);
            published.registry = registry;
        }
    }

    /// As [`Self::publish`], additionally replacing the unprefixed
    /// `nixos_channel_*` registry in the same locked section.
    ///
    /// Used by the real collection cycle, which rebuilds both registries from
    /// scratch every time regardless of whether the flake pass (and
    /// therefore the NixOS channel fetch nested inside it) actually ran this
    /// cycle -- same as every other cached flake metric. Swapping both under
    /// one lock acquisition is what keeps a scrape from ever seeing one
    /// cycle's `github_*` series paired with a different cycle's
    /// `nixos_channel_*` ones.
    pub fn publish_with_channels(
        &self,
        metrics: Metrics,
        registry: Registry,
        channel_registry: Registry,
    ) {
        if let Ok(mut published) = self.0.lock() {
            metrics.carry_forward_from(&published.metrics);
            published.metrics = Arc::new(metrics);
            published.registry = registry;
            published.channel_registry = channel_registry;
        }
    }

    /// Annotates the currently-served set in place, under the lock.
    ///
    /// Used only by the paths that must *not* replace the data: a failed cycle
    /// and a budget-bypassed cycle both report their own state while leaving
    /// the previous cycle's samples intact.
    ///
    /// `update` runs while the mutex is held, so a multi-field annotation is
    /// as indivisible as a swap. Handing the handles back to the caller and
    /// releasing the lock first would reintroduce this module's original bug in
    /// miniature: the bypass path sets several metrics in sequence, and a
    /// scrape landing between them would see `budget_exhausted` still 0 with
    /// the rest applied -- enough to reset the `for:` timer on the very alert
    /// that is supposed to report the skip.
    pub fn annotate(&self, update: impl FnOnce(&Metrics)) {
        if let Ok(published) = self.0.lock() {
            update(&published.metrics);
        }
    }

    /// Replaces the served repository index.
    ///
    /// Separate from [`Publisher::publish`] because the index depends only on
    /// discovery, which runs at the very top of a cycle, while the metric set
    /// is not complete until the REST run-fetching at the bottom has finished.
    /// Tying them together would mean a cycle that discovered every repository
    /// but then failed while collecting Actions state served a stale
    /// repository list, even though the list it held was known-good. The two
    /// have genuinely different validity conditions, so they are published
    /// independently.
    ///
    /// The caller is responsible for only calling this after a *complete*
    /// discovery pass -- see the partial-failure guard in `collector::collect`.
    pub fn publish_repo_index(&self, index: RepoIndex) {
        if let Ok(mut published) = self.0.lock() {
            published.repo_index = Arc::new(index);
        }
    }

    /// The currently-served repository index.
    ///
    /// Returns an [`Arc`] so the caller can serialise it after releasing the
    /// lock; a scrape of `/metrics` must not block behind JSON encoding of a
    /// few hundred repositories.
    #[must_use]
    pub fn repo_index(&self) -> Arc<RepoIndex> {
        self.0.lock().map_or_else(
            |_| Arc::new(RepoIndex::default()),
            |p| Arc::clone(&p.repo_index),
        )
    }

    /// Renders both served registries -- `github`-prefixed, then the
    /// unprefixed `nixos_channel_*` one -- as a single Prometheus text
    /// exposition, with one `# EOF` marker at the end.
    ///
    /// `prometheus-client` has no API for encoding two registries under one
    /// prefix scheme in a single call -- a registry only ever gets *one*
    /// prefix, applied to everything under it -- so an unprefixed sibling has
    /// to be a genuinely separate top-level [`Registry`]. `encode_registry`
    /// is public exactly for this: it writes one registry's body without the
    /// `# EOF` marker, so composing several into one response is calling it
    /// more than once and appending the marker last.
    #[must_use]
    pub fn render(&self) -> String {
        let mut buffer = String::new();
        if let Ok(published) = self.0.lock() {
            // Writing into a String is infallible for these metric types.
            let _: Result<(), std::fmt::Error> = encode_registry(&mut buffer, &published.registry);
            let _: Result<(), std::fmt::Error> =
                encode_registry(&mut buffer, &published.channel_registry);
            let _: Result<(), std::fmt::Error> = encode_eof(&mut buffer);
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

        let published = Publisher::new(metrics, registry);
        let rendered = published.render();

        assert!(
            rendered.contains("github_repo_issues_open"),
            "expected prefixed metric name, got:\n{rendered}"
        );
        assert!(rendered.contains(r#"author_kind="human""#));
        assert!(rendered.contains(r#"repo="nixos""#));
    }

    #[test]
    fn repo_index_starts_empty_and_is_replaced_wholesale() {
        let (metrics, registry) = Metrics::new();
        let publisher = Publisher::new(metrics, registry);

        // Before the first sweep completes there is nothing to serve, and
        // `generated_at` is what tells a client to say so rather than render
        // an empty list as "you own no repositories".
        let before = publisher.repo_index();
        assert_eq!(before.repos, Vec::<crate::model::RepoIndexEntry>::new());
        assert!(before.generated_at.is_none());

        publisher.publish_repo_index(RepoIndex {
            generated_at: Some(chrono::Utc::now()),
            repos: vec![crate::model::RepoIndexEntry {
                owner: "fredsystems".to_owned(),
                name: "nixos".to_owned(),
                description: None,
                archived: false,
                pushed_at: None,
            }],
        });

        let after = publisher.repo_index();
        assert_eq!(after.repos.len(), 1);
        assert_eq!(after.repos[0].name, "nixos");
        assert!(after.generated_at.is_some());
    }

    #[test]
    fn publishing_metrics_leaves_the_repo_index_alone() {
        // The two have different validity conditions on purpose: a metric
        // swap at the end of a cycle must not disturb the index published
        // from discovery at the start of it.
        let publisher = publisher_monitoring("kept");
        publisher.publish_repo_index(RepoIndex {
            generated_at: Some(chrono::Utc::now()),
            repos: vec![crate::model::RepoIndexEntry {
                owner: "fredsystems".to_owned(),
                name: "nixos".to_owned(),
                description: None,
                archived: false,
                pushed_at: None,
            }],
        });

        let (metrics, registry) = Metrics::new();
        publisher.publish(metrics, registry);

        assert_eq!(
            publisher.repo_index().repos.len(),
            1,
            "a metrics swap must not clear the repository index"
        );
    }

    /// A publisher serving one monitored repository named `name`.
    fn publisher_monitoring(name: &str) -> Publisher {
        let (metrics, registry) = Metrics::new();
        metrics
            .repo_monitored
            .get_or_create(&RepoLabels {
                org: "o".into(),
                repo: name.into(),
            })
            .set(1);
        Publisher::new(metrics, registry)
    }

    #[test]
    fn publishing_replaces_stale_repo_series() {
        // A repository that disappears upstream must stop producing samples.
        // A fresh registry starts empty, so this falls out of the swap rather
        // than needing an explicit clear.
        let publisher = publisher_monitoring("gone");
        assert!(publisher.render().contains(r#"repo="gone""#));

        let (next, next_registry) = Metrics::new();
        next.repo_monitored
            .get_or_create(&RepoLabels {
                org: "o".into(),
                repo: "still-here".into(),
            })
            .set(1);
        publisher.publish(next, next_registry);

        let rendered = publisher.render();
        assert!(
            !rendered.contains(r#"repo="gone""#),
            "a deleted repository must stop producing samples:\n{rendered}"
        );
        assert!(rendered.contains(r#"repo="still-here""#));
    }

    #[test]
    fn a_cycle_in_progress_is_never_observable() {
        // THE regression guard for this module's reason to exist.
        //
        // The original design cleared the live families and refilled them as
        // the cycle walked its repositories, so `/metrics` served every
        // intermediate state for as long as a cycle took -- measured at 81
        // seconds. Prometheus scraped the gaps, absent samples reset alert
        // `for:` timers, and a `for: 15m` rule could never fire.
        //
        // Building into a detached set must leave the served output byte-identical
        // until the swap.
        let publisher = publisher_monitoring("previous");
        let before = publisher.render();

        // A cycle starts and populates its own set, one repository at a time.
        let (in_flight, in_flight_registry) = Metrics::new();
        for repo in ["first", "second", "third"] {
            in_flight
                .repo_monitored
                .get_or_create(&RepoLabels {
                    org: "o".into(),
                    repo: repo.into(),
                })
                .set(1);
            assert_eq!(
                publisher.render(),
                before,
                "a partially-built cycle must not reach a scrape"
            );
        }

        publisher.publish(in_flight, in_flight_registry);

        let after = publisher.render();
        assert!(!after.contains(r#"repo="previous""#));
        for repo in ["first", "second", "third"] {
            assert!(
                after.contains(&format!(r#"repo="{repo}""#)),
                "the whole cycle must appear at once:\n{after}"
            );
        }
    }

    #[test]
    fn monotonic_counters_survive_a_swap() {
        // Restarting a counter at zero on every swap reads as a process
        // restart and destroys increase() over any window spanning one.
        let (metrics, registry) = Metrics::new();
        metrics.cycles_bypassed.inc();
        metrics.cycles_bypassed.inc();
        let publisher = Publisher::new(metrics, registry);

        let (next, next_registry) = Metrics::new();
        publisher.publish(next, next_registry);

        assert!(
            publisher
                .render()
                .contains("github_exporter_cycles_bypassed_total 2"),
            "counter must carry forward:\n{}",
            publisher.render()
        );
    }

    #[test]
    fn the_published_set_can_be_annotated_in_place() {
        // How a failed cycle reports itself: last-known-good data stays, and
        // only scrape_success changes.
        let publisher = publisher_monitoring("kept");
        publisher.annotate(|published| {
            published.scrape_success.set(0);
        });

        let rendered = publisher.render();
        assert!(rendered.contains("github_exporter_scrape_success 0"));
        assert!(
            rendered.contains(r#"repo="kept""#),
            "a failed cycle must not discard the previous data:\n{rendered}"
        );
    }

    #[test]
    fn annotation_holds_the_lock_for_its_whole_closure() {
        // A multi-field annotation must be as indivisible as a swap. Returning
        // the handles and releasing the lock first let a scrape land between
        // two updates, which is this module's original bug in miniature.
        //
        // Asserted by observing the lock directly rather than by racing a
        // thread, so there is nothing timing-dependent to go flaky.
        let publisher = publisher_monitoring("kept");
        let mut held = false;
        publisher.annotate(|_| {
            held = publisher.0.try_lock().is_err();
        });
        assert!(
            held,
            "the publisher mutex must be held for the duration of the closure"
        );
    }

    #[test]
    fn flake_input_locked_timestamp_encodes_its_labels() {
        let (metrics, registry) = Metrics::new();
        metrics
            .flake_input_locked_timestamp
            .get_or_create(&FlakeInputLabels {
                repo: "fredsystems/nixos".into(),
                input: "niri".into(),
                path: "niri/nixpkgs".into(),
                r#ref: "nixos-unstable".into(),
                channel: "nixos-unstable".into(),
            })
            .set(1_700_000_000);

        let rendered = Publisher::new(metrics, registry).render();
        assert!(rendered.contains("github_flake_input_locked_timestamp_seconds"));
        assert!(rendered.contains(r#"input="niri""#));
        assert!(rendered.contains(r#"path="niri/nixpkgs""#));
        assert!(rendered.contains(r#"ref="nixos-unstable""#));
        assert!(rendered.contains("1700000000"));
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

        let rendered = Publisher::new(metrics, registry).render();
        assert!(rendered.contains(r#"conclusion="failure""#));
        assert!(rendered.contains(r#"workflow="Deploy""#));
    }

    #[test]
    fn channel_metrics_render_unprefixed_alongside_the_github_registry() {
        let (metrics, registry) = Metrics::new();
        metrics.rate_limit_reserve.set(42);
        let (channel_metrics, channel_registry) = ChannelMetrics::new();
        channel_metrics
            .update_timestamp
            .get_or_create(&ChannelLabels {
                channel: "nixos-unstable".into(),
            })
            .set(1_700_000_000);
        channel_metrics
            .current
            .get_or_create(&ChannelLabels {
                channel: "nixos-unstable".into(),
            })
            .set(1);
        channel_metrics
            .status_up
            .get_or_create(&ChannelAttemptLabels::default())
            .set(1);

        let rendered = Publisher::new_with_channels(metrics, registry, channel_registry).render();

        assert!(
            rendered.contains("github_exporter_rate_limit_reserve 42"),
            "the github-prefixed registry must still render:\n{rendered}"
        );
        assert!(
            rendered.contains("nixos_channel_update_timestamp_seconds"),
            "the channel registry must render unprefixed:\n{rendered}"
        );
        assert!(!rendered.contains("github_nixos_channel"));
        assert!(rendered.contains(r#"channel="nixos-unstable""#));
        assert!(rendered.contains("nixos_channel_status_up"));
        assert_eq!(
            rendered.matches("# EOF").count(),
            1,
            "composing two registries must still produce exactly one EOF marker:\n{rendered}"
        );
    }

    #[test]
    fn status_up_is_absent_until_an_attempt_is_recorded() {
        // A plain `Gauge` always encodes, even at its default value, which
        // would make `nixos_channel_status_up 0` appear before the exporter
        // has ever attempted a fetch. `status_up` is instead a `Family` with
        // no labels specifically so it can stay genuinely absent until the
        // first `get_or_create` call -- this asserts that actually holds.
        let (channel_metrics, channel_registry) = ChannelMetrics::new();
        let (metrics, registry) = Metrics::new();
        let publisher = Publisher::new_with_channels(metrics, registry, channel_registry);
        assert!(
            !publisher.render().contains("nixos_channel_status_up"),
            "status_up must not appear before any fetch has been attempted:\n{}",
            publisher.render()
        );

        channel_metrics
            .status_up
            .get_or_create(&ChannelAttemptLabels::default())
            .set(0);
        assert!(publisher.render().contains("nixos_channel_status_up"));
    }

    #[test]
    fn publish_with_channels_keeps_both_registries_in_step() {
        let (metrics, registry) = Metrics::new();
        let publisher = Publisher::new(metrics, registry);
        assert!(!publisher.render().contains("nixos_channel"));

        let (next_metrics, next_registry) = Metrics::new();
        let (channel_metrics, channel_registry) = ChannelMetrics::new();
        channel_metrics
            .current
            .get_or_create(&ChannelLabels {
                channel: "nixos-26.05".into(),
            })
            .set(0);
        publisher.publish_with_channels(next_metrics, next_registry, channel_registry);

        let rendered = publisher.render();
        assert!(rendered.contains(r#"nixos_channel_current{channel="nixos-26.05"} 0"#));
    }
}
