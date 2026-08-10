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

/// Aggregate result of the checks on a pull request's head commit.
///
/// This is GitHub's `statusCheckRollup`, and unlike on a default-branch commit
/// it is reliable here: PR-triggered workflows are attached to the head commit
/// by construction. (On the default branch the rollup was null for 30 of 31
/// sampled repositories, because most workflows there run on `schedule`.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChecksState {
    Success,
    Failure,
    Pending,
    /// No checks are attached to the head commit at all. Distinct from
    /// `Pending`: this repository has no PR-triggered CI, so waiting will
    /// never produce a result.
    None,
    Unknown,
}

impl ChecksState {
    /// Maps GraphQL's `StatusState`, treating a null rollup as [`Self::None`].
    #[must_use]
    pub fn from_api(state: Option<&str>) -> Self {
        match state {
            None => Self::None,
            Some("SUCCESS") => Self::Success,
            // ERROR is a failed commit status rather than a failed check run;
            // both mean the same thing to someone deciding whether to merge.
            Some("FAILURE" | "ERROR") => Self::Failure,
            Some("PENDING" | "EXPECTED") => Self::Pending,
            Some(_) => Self::Unknown,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Pending => "pending",
            Self::None => "none",
            Self::Unknown => "unknown",
        }
    }
}

/// Whether a pull request can be merged as it stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MergeableState {
    Mergeable,
    /// Conflicts with the base branch.
    Conflicting,
    /// GitHub has not computed it yet. Deliberately its own state rather than
    /// being folded into `Mergeable`: the value is computed lazily, so a first
    /// query commonly returns `UNKNOWN` for a PR that is in fact conflicting.
    /// Treating it as mergeable would report conflicted PRs as ready to merge.
    Unknown,
}

impl MergeableState {
    #[must_use]
    pub fn from_api(state: Option<&str>) -> Self {
        match state {
            Some("MERGEABLE") => Self::Mergeable,
            Some("CONFLICTING") => Self::Conflicting,
            _ => Self::Unknown,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mergeable => "mergeable",
            Self::Conflicting => "conflicting",
            Self::Unknown => "unknown",
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

/// A reference to one pull request, as `owner/name#number`.
///
/// Exists so the operator's ignore list is parsed and validated once, at
/// startup, rather than string-compared per pull request per cycle. A typo in
/// a configured entry is then a startup error instead of a filter that
/// silently never matches.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PullRef {
    /// Lowercased `owner/name`, since GitHub treats those case-insensitively.
    pub repo: String,
    pub number: u64,
}

impl PullRef {
    /// Builds a reference from a repository full name and PR number.
    #[must_use]
    pub fn new(repo: &str, number: u64) -> Self {
        Self {
            repo: repo.to_ascii_lowercase(),
            number,
        }
    }
}

impl std::str::FromStr for PullRef {
    type Err = PullRefError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let raw = raw.trim();
        let (repo, number) = raw
            .split_once('#')
            .ok_or_else(|| PullRefError(raw.to_owned()))?;
        // `owner/name`, both halves non-empty, no stray extra slash, and no
        // internal whitespace. Every one of those would otherwise parse into a
        // reference that can never match an API-supplied name -- `#32`,
        // `owner#32`, and `owner/name #32` alike -- which is precisely the
        // silent no-op this validation exists to prevent. Note that only the
        // *outer* whitespace is trimmed, so an inner space survives into the
        // comparison.
        let is_full_name = repo.split_once('/').is_some_and(|(owner, name)| {
            !owner.is_empty()
                && !name.is_empty()
                && !name.contains('/')
                && !repo.chars().any(char::is_whitespace)
        });
        if !is_full_name {
            return Err(PullRefError(raw.to_owned()));
        }
        let number: u64 = number.parse().map_err(|_| PullRefError(raw.to_owned()))?;
        if number == 0 {
            return Err(PullRefError(raw.to_owned()));
        }
        Ok(Self::new(repo, number))
    }
}

impl fmt::Display for PullRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.repo, self.number)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("`{0}` is not a pull request reference; expected `owner/name#number`")]
pub struct PullRefError(String);

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
    fn pull_ref_parses_the_owner_repo_number_form() {
        let parsed: PullRef = "sdr-enthusiasts/docker-vesselalert#32"
            .parse()
            .expect("a well-formed reference must parse");
        assert_eq!(parsed.repo, "sdr-enthusiasts/docker-vesselalert");
        assert_eq!(parsed.number, 32);
        assert_eq!(parsed.to_string(), "sdr-enthusiasts/docker-vesselalert#32");
    }

    #[test]
    fn pull_ref_matching_is_case_insensitive() {
        // GitHub treats owner and repository names case-insensitively, and the
        // configured entry is hand-written while the runtime value comes from
        // the API. They must still match.
        let configured: PullRef = "SDR-Enthusiasts/Docker-VesselAlert#32"
            .parse()
            .expect("parse");
        assert_eq!(
            configured,
            PullRef::new("sdr-enthusiasts/docker-vesselalert", 32)
        );
    }

    #[test]
    fn pull_ref_rejects_malformed_entries() {
        // Every one of these would otherwise become a filter that silently
        // never matches, leaving the operator believing a PR is suppressed
        // while the alert keeps firing.
        for bad in [
            "sdr-enthusiasts/docker-vesselalert", // no number
            "#32",                                // no repository
            "docker-vesselalert#32",              // no owner
            "owner/#32",                          // empty name
            "/name#32",                           // empty owner
            "owner/name/extra#32",                // not a full name
            "owner/name#",                        // empty number
            "owner/name#abc",                     // non-numeric
            "owner/name#-1",                      // negative
            "owner/name#0",                       // PR numbers start at 1
            "owner/name #32",                     // internal whitespace
            "owner /name#32",
            "own er/name#32",
            "owner/na me#32",
            "owner/name#3 2",
            "owner/name#\t32",
            "",
        ] {
            assert!(
                bad.parse::<PullRef>().is_err(),
                "`{bad}` must be rejected rather than silently never matching"
            );
        }
    }

    #[test]
    fn pull_ref_tolerates_surrounding_whitespace() {
        let parsed: PullRef = "  owner/name#7  ".parse().expect("parse");
        assert_eq!(parsed, PullRef::new("owner/name", 7));
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
