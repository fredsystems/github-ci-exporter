//! Core domain types shared by the collectors and the metrics registry.
//!
//! These types deliberately model GitHub's data the way the *exporter* needs
//! it, not the way the API returns it. The API shapes live in the collector
//! modules and are converted here at the boundary.

use std::fmt;

/// Whether an issue or pull request was opened by a human or by an automation.
///
/// This is derived from GraphQL's `author.__typename`, which is authoritative:
/// it reports `Bot` for GitHub Apps (renovate, dependabot, github-actions)
/// without needing a hand-maintained list of bot logins. A login denylist was
/// rejected because it silently misclassifies every new bot.
///
/// Carried as a metric label so a single dashboard can show bot activity for
/// visibility while alert rules select only `Human`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AuthorKind {
    Human,
    Bot,
}

impl AuthorKind {
    /// Classifies a GraphQL `__typename` for an actor.
    ///
    /// GitHub returns `User`, `Bot`, `Organization`, or `Mannequin`. Anything
    /// that is not exactly `User` is treated as non-human: a deleted author
    /// comes back as `null` (handled by the caller) and the remaining actor
    /// types are never a person reviewing a PR.
    #[must_use]
    pub fn from_typename(typename: &str) -> Self {
        if typename == "User" {
            Self::Human
        } else {
            Self::Bot
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Human => "human",
            Self::Bot => "bot",
        }
    }
}

impl fmt::Display for AuthorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why a repository was excluded from monitoring.
///
/// Exported as a metric so an unexpectedly-empty dashboard is explainable
/// without reading exporter logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SkipReason {
    /// Archived upstream. Note that archived repos still return open issues
    /// and PRs from the search API, so they must be filtered from the repo
    /// list rather than from query results.
    Archived,
    /// No workflow files under `.github/workflows`. These are content-hosting
    /// repos (assets, websites, `.github` profile repos) with no CI to report.
    NoWorkflows,
    /// Named in the operator's denylist.
    Denylisted,
    /// Not pushed within the configured activity window.
    Inactive,
}

impl SkipReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Archived => "archived",
            Self::NoWorkflows => "no_workflows",
            Self::Denylisted => "denylisted",
            Self::Inactive => "inactive",
        }
    }
}

/// A repository selected for monitoring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Repo {
    pub owner: String,
    pub name: String,
    pub default_branch: String,
}

impl Repo {
    #[must_use]
    pub fn full_name(&self) -> String {
        format!("{}/{}", self.owner, self.name)
    }
}

impl fmt::Display for Repo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.owner, self.name)
    }
}

/// Terminal state of the most recent run of a workflow.
///
/// `Running` is distinct from a conclusion: an in-flight run must not clear a
/// previous failure, otherwise a repeatedly-retriggered broken workflow would
/// never alert.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum RunConclusion {
    Success,
    Failure,
    Cancelled,
    Skipped,
    TimedOut,
    ActionRequired,
    Neutral,
    Stale,
    Running,
    Unknown,
}

impl RunConclusion {
    /// Maps the REST API's `conclusion` field, falling back to `status` when
    /// the run has not concluded.
    #[must_use]
    pub fn from_api(status: &str, conclusion: Option<&str>) -> Self {
        match conclusion {
            Some("success") => Self::Success,
            Some("failure") => Self::Failure,
            Some("cancelled") => Self::Cancelled,
            Some("skipped") => Self::Skipped,
            Some("timed_out") => Self::TimedOut,
            Some("action_required") => Self::ActionRequired,
            Some("neutral") => Self::Neutral,
            Some("stale") => Self::Stale,
            // `queued`, `in_progress`, `waiting`, `requested`, `pending`
            None if status != "completed" => Self::Running,
            // An unrecognised future conclusion, or a completed run with no
            // conclusion at all.
            Some(_) | None => Self::Unknown,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Cancelled => "cancelled",
            Self::Skipped => "skipped",
            Self::TimedOut => "timed_out",
            Self::ActionRequired => "action_required",
            Self::Neutral => "neutral",
            Self::Stale => "stale",
            Self::Running => "running",
            Self::Unknown => "unknown",
        }
    }

    /// Whether this conclusion should be treated as a broken build.
    ///
    /// `Cancelled` and `Skipped` are excluded: both are routine (superseded
    /// pushes, path filters) and paging on them would train the operator to
    /// ignore the alert.
    #[must_use]
    pub const fn is_failure(self) -> bool {
        matches!(self, Self::Failure | Self::TimedOut | Self::ActionRequired)
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
    use super::*;

    #[test]
    fn author_kind_treats_only_user_as_human() {
        assert_eq!(AuthorKind::from_typename("User"), AuthorKind::Human);
        assert_eq!(AuthorKind::from_typename("Bot"), AuthorKind::Bot);
        assert_eq!(AuthorKind::from_typename("Organization"), AuthorKind::Bot);
        assert_eq!(AuthorKind::from_typename("Mannequin"), AuthorKind::Bot);
    }

    #[test]
    fn run_conclusion_maps_completed_states() {
        assert_eq!(
            RunConclusion::from_api("completed", Some("success")),
            RunConclusion::Success
        );
        assert_eq!(
            RunConclusion::from_api("completed", Some("failure")),
            RunConclusion::Failure
        );
        assert_eq!(
            RunConclusion::from_api("completed", Some("timed_out")),
            RunConclusion::TimedOut
        );
    }

    #[test]
    fn run_conclusion_reports_in_flight_runs_as_running() {
        assert_eq!(
            RunConclusion::from_api("in_progress", None),
            RunConclusion::Running
        );
        assert_eq!(
            RunConclusion::from_api("queued", None),
            RunConclusion::Running
        );
    }

    #[test]
    fn run_conclusion_handles_unknown_future_values() {
        assert_eq!(
            RunConclusion::from_api("completed", Some("brand_new_thing")),
            RunConclusion::Unknown
        );
    }

    #[test]
    fn only_real_breakage_counts_as_failure() {
        assert!(RunConclusion::Failure.is_failure());
        assert!(RunConclusion::TimedOut.is_failure());
        assert!(RunConclusion::ActionRequired.is_failure());
        // Routine, must not page.
        assert!(!RunConclusion::Cancelled.is_failure());
        assert!(!RunConclusion::Skipped.is_failure());
        assert!(!RunConclusion::Success.is_failure());
        assert!(!RunConclusion::Running.is_failure());
    }

    #[test]
    fn repo_renders_full_name() {
        let repo = Repo {
            owner: "fredsystems".into(),
            name: "nixos".into(),
            default_branch: "main".into(),
        };
        assert_eq!(repo.full_name(), "fredsystems/nixos");
        assert_eq!(repo.to_string(), "fredsystems/nixos");
    }
}
