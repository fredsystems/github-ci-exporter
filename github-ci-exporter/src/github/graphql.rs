//! GraphQL collectors: repository discovery, issues, and pull requests.
//!
//! Everything here is batched. Discovery pages 100 repositories per request
//! and the per-repo issue/PR query aliases many repositories into a single
//! document, keeping a full sweep at a handful of rate-limit points.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::client::{Client, ClientError};
use crate::model::{AuthorKind, ChecksState, MergeableState, Repo, SkipReason};

/// Repositories per discovery page. 100 is GraphQL's maximum page size.
const DISCOVERY_PAGE_SIZE: usize = 100;

/// Repositories per batched issue/PR query.
///
/// GraphQL cost scales with aliased root fields; 50 keeps a single request
/// comfortably under the node limit while still covering ~60 repos in two
/// requests.
const BATCH_SIZE: usize = 50;

/// Repository discovery, resolved through the `RepositoryOwner` interface.
///
/// `repositoryOwner` rather than `organization` because a personal account is
/// a `User`, and `organization(login:)` resolves to null for one — an owner
/// like `fredclausen` would fail discovery every cycle with "not found". Both
/// `User` and `Organization` implement `RepositoryOwner`, and the interface
/// exposes the same `repositories` connection, so one query covers both with
/// no branching.
///
/// `ownerAffiliations: [OWNER]` is **not** redundant. On a `User` the default
/// affiliations include `COLLABORATOR`, so the connection returns repositories
/// in *other* accounts that the user can push to. Measured on `fredclausen`,
/// roughly 40% of the default result was unowned, including `fredsystems/nixos`
/// and `sdr-enthusiasts/docker-acarshub` — repositories already monitored under
/// their own owner. With `[OWNER]` the result matches `gh repo list <owner>`
/// exactly, in both directions, which is the invariant to re-check against
/// rather than any particular count.
///
/// `owner { login }` is selected rather than trusting the queried login,
/// because `nodes.name` is bare. Pairing it with the login that was asked for
/// is what produced `fredclausen/nixos` from `fredsystems/nixos`: a duplicate
/// series under a fabricated `org` label, for a repository that already had
/// one. Reading the owner back from the response makes that unrepresentable
/// regardless of what the connection decides to include.
const DISCOVERY_QUERY: &str = r"
query($owner: String!, $cursor: String) {
  repositoryOwner(login: $owner) {
    repositories(
      first: 100
      after: $cursor
      ownerAffiliations: [OWNER]
      orderBy: {field: NAME, direction: ASC}
    ) {
      pageInfo { hasNextPage endCursor }
      nodes {
        name
        owner { login }
        isArchived
        pushedAt
        defaultBranchRef { name }
      }
    }
  }
}
";

#[derive(Debug, Deserialize)]
struct DiscoveryResponse {
    #[serde(rename = "repositoryOwner")]
    repository_owner: Option<DiscoveryOwner>,
}

#[derive(Debug, Deserialize)]
struct DiscoveryOwner {
    repositories: DiscoveryConnection,
}

#[derive(Debug, Deserialize)]
struct DiscoveryConnection {
    #[serde(rename = "pageInfo")]
    page_info: PageInfo,
    nodes: Vec<DiscoveryNode>,
}

#[derive(Debug, Deserialize)]
struct PageInfo {
    #[serde(rename = "hasNextPage")]
    has_next_page: bool,
    #[serde(rename = "endCursor")]
    end_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DiscoveryNode {
    name: String,
    owner: NodeOwner,
    #[serde(rename = "isArchived")]
    is_archived: bool,
    #[serde(rename = "pushedAt")]
    pushed_at: Option<DateTime<Utc>>,
    #[serde(rename = "defaultBranchRef")]
    default_branch_ref: Option<BranchRef>,
}

#[derive(Debug, Deserialize)]
struct NodeOwner {
    login: String,
}

#[derive(Debug, Deserialize)]
struct BranchRef {
    name: String,
}

/// A repository as returned by discovery, before filtering.
#[derive(Debug, Clone)]
pub struct DiscoveredRepo {
    pub repo: Repo,
    pub is_archived: bool,
    pub pushed_at: Option<DateTime<Utc>>,
}

/// Enumerates every repository owned by `owner`, following pagination.
///
/// `owner` may be an organisation or a personal account; the two are
/// interchangeable here (see [`DISCOVERY_QUERY`]).
///
/// Archived repositories are returned rather than dropped, so the caller can
/// account for *why* each repository was skipped. Note that archived repos
/// still surface open issues and PRs via the search API, so they must be
/// filtered out here at the source.
///
/// # Errors
/// Returns [`ClientError`] if any page fails.
pub async fn discover_owner(
    client: &Client,
    owner: &str,
) -> Result<Vec<DiscoveredRepo>, ClientError> {
    let mut cursor: Option<String> = None;
    let mut found = Vec::with_capacity(DISCOVERY_PAGE_SIZE);

    loop {
        let variables = serde_json::json!({ "owner": owner, "cursor": cursor });
        let response: DiscoveryResponse = client.graphql(DISCOVERY_QUERY, variables).await?;
        let Some(repository_owner) = response.repository_owner else {
            return Err(ClientError::GraphQl(format!(
                "owner `{owner}` not found or not visible to this token"
            )));
        };

        for node in repository_owner.repositories.nodes {
            // A repository with no default branch ref is empty (never had a
            // commit); there is nothing to report CI for.
            let Some(branch) = node.default_branch_ref else {
                continue;
            };
            found.push(DiscoveredRepo {
                repo: Repo {
                    // The response's own owner, never the queried login.
                    owner: node.owner.login,
                    name: node.name,
                    default_branch: branch.name,
                },
                is_archived: node.is_archived,
                pushed_at: node.pushed_at,
            });
        }

        if !repository_owner.repositories.page_info.has_next_page {
            break;
        }
        cursor = repository_owner.repositories.page_info.end_cursor;
        if cursor.is_none() {
            break;
        }
    }

    Ok(found)
}

/// Open issue and pull-request counts for one repository, split by author kind.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoActivity {
    pub issues: BTreeMap<AuthorKind, u64>,
    pub pulls: BTreeMap<AuthorKind, u64>,
    pub draft_pulls: BTreeMap<AuthorKind, u64>,
    /// Open pull requests, for per-PR age metrics.
    pub open_pulls: Vec<OpenPull>,
}

/// A single open pull request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenPull {
    pub number: u64,
    pub author: String,
    pub author_kind: AuthorKind,
    pub is_draft: bool,
    pub created_at: DateTime<Utc>,
    /// Aggregate state of checks on the head commit.
    pub checks: ChecksState,
    pub mergeable: MergeableState,
    /// Whether auto-merge is armed. A green, mergeable PR without it is
    /// waiting on a human to press the button.
    pub auto_merge: bool,
}

impl OpenPull {
    /// Whether this pull request needs someone to act on it.
    ///
    /// Drafts are excluded throughout: an open draft is work in progress.
    #[must_use]
    pub fn needs_attention(&self) -> bool {
        !self.is_draft
            && (self.checks == ChecksState::Failure
                || self.mergeable == MergeableState::Conflicting
                || self.is_ready_to_merge())
    }

    /// Green, conflict-free, and not going to merge itself.
    ///
    /// This is the "all checks passed but automerge is off, so it is sitting
    /// there waiting for a click" case.
    #[must_use]
    pub fn is_ready_to_merge(&self) -> bool {
        !self.is_draft
            && self.checks == ChecksState::Success
            && self.mergeable == MergeableState::Mergeable
            && !self.auto_merge
    }
}

#[derive(Debug, Deserialize)]
struct ActivityRepo {
    issues: IssueConnection,
    #[serde(rename = "pullRequests")]
    pull_requests: PullConnection,
}

#[derive(Debug, Deserialize)]
struct IssueConnection {
    nodes: Vec<IssueNode>,
}

#[derive(Debug, Deserialize)]
struct IssueNode {
    author: Option<Actor>,
}

#[derive(Debug, Deserialize)]
struct PullConnection {
    nodes: Vec<PullNode>,
}

#[derive(Debug, Deserialize)]
struct PullNode {
    number: u64,
    #[serde(rename = "isDraft")]
    is_draft: bool,
    #[serde(rename = "createdAt")]
    created_at: DateTime<Utc>,
    author: Option<Actor>,
    mergeable: Option<String>,
    #[serde(rename = "autoMergeRequest")]
    auto_merge_request: Option<AutoMergeRequest>,
    commits: Option<CommitConnection>,
}

#[derive(Debug, Deserialize)]
struct AutoMergeRequest {
    #[serde(rename = "enabledAt")]
    _enabled_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
struct CommitConnection {
    nodes: Vec<CommitNode>,
}

#[derive(Debug, Deserialize)]
struct CommitNode {
    commit: CommitDetail,
}

#[derive(Debug, Deserialize)]
struct CommitDetail {
    #[serde(rename = "statusCheckRollup")]
    status_check_rollup: Option<StatusCheckRollup>,
}

#[derive(Debug, Deserialize)]
struct StatusCheckRollup {
    state: Option<String>,
}

impl PullNode {
    /// Rollup state of the head commit's checks.
    fn checks_state(&self) -> ChecksState {
        let rollup = self
            .commits
            .as_ref()
            .and_then(|c| c.nodes.first())
            .and_then(|n| n.commit.status_check_rollup.as_ref());
        rollup.map_or(ChecksState::None, |r| {
            ChecksState::from_api(r.state.as_deref())
        })
    }
}

#[derive(Debug, Deserialize)]
struct Actor {
    login: String,
    /// `User`, `Bot`, `Organization`, or `Mannequin`. This is what makes bot
    /// filtering robust without a hand-maintained login list.
    #[serde(rename = "__typename")]
    typename: String,
}

impl Actor {
    fn kind(&self) -> AuthorKind {
        AuthorKind::from_typename(&self.typename)
    }
}

/// Builds a batched query aliasing each repository as `r0`, `r1`, ...
fn build_activity_query(batch: &[&Repo]) -> String {
    use std::fmt::Write as _;

    let mut query = String::from("query {\n");
    for (index, repo) in batch.iter().enumerate() {
        // Owner and name come from the API's own discovery response, so they
        // are already valid GitHub identifiers; quoting is still applied.
        let _ = writeln!(
            query,
            "  r{index}: repository(owner: \"{}\", name: \"{}\") {{ ...A }}",
            repo.owner, repo.name
        );
    }
    query.push_str("}\n");
    query.push_str(
        r"
fragment A on Repository {
  issues(states: OPEN, first: 100) {
    nodes { author { login __typename } }
  }
  pullRequests(states: OPEN, first: 100) {
    nodes {
      number isDraft createdAt
      author { login __typename }
      mergeable
      autoMergeRequest { enabledAt }
      commits(last: 1) {
        nodes { commit { statusCheckRollup { state } } }
      }
    }
  }
}
",
    );
    query
}

/// Fetches open issues and PRs for many repositories in as few requests as
/// possible.
///
/// Returns a map keyed by `owner/name`.
///
/// # Errors
/// Returns [`ClientError`] if a batch request fails.
pub async fn fetch_activity(
    client: &Client,
    repos: &[Repo],
) -> Result<BTreeMap<String, RepoActivity>, ClientError> {
    let mut out = BTreeMap::new();

    for chunk in repos.chunks(BATCH_SIZE) {
        let refs: Vec<&Repo> = chunk.iter().collect();
        let query = build_activity_query(&refs);
        let raw: BTreeMap<String, Option<ActivityRepo>> =
            client.graphql(&query, serde_json::json!({})).await?;

        for (index, repo) in refs.iter().enumerate() {
            let Some(Some(data)) = raw.get(&format!("r{index}")) else {
                // A repo can vanish between discovery and this query.
                continue;
            };

            let mut activity = RepoActivity::default();
            for issue in &data.issues.nodes {
                // A deleted account yields a null author; count it as a bot so
                // it can never trigger a human-scoped alert.
                let kind = issue.author.as_ref().map_or(AuthorKind::Bot, Actor::kind);
                *activity.issues.entry(kind).or_insert(0) += 1;
            }
            for pull in &data.pull_requests.nodes {
                let (login, kind) = pull.author.as_ref().map_or_else(
                    || ("ghost".to_owned(), AuthorKind::Bot),
                    |actor| (actor.login.clone(), actor.kind()),
                );
                *activity.pulls.entry(kind).or_insert(0) += 1;
                if pull.is_draft {
                    *activity.draft_pulls.entry(kind).or_insert(0) += 1;
                }
                activity.open_pulls.push(OpenPull {
                    number: pull.number,
                    author: login,
                    author_kind: kind,
                    is_draft: pull.is_draft,
                    created_at: pull.created_at,
                    checks: pull.checks_state(),
                    mergeable: MergeableState::from_api(pull.mergeable.as_deref()),
                    auto_merge: pull.auto_merge_request.is_some(),
                });
            }

            // Ensure both kinds are present so a repo that drops to zero human
            // issues actively reports 0 rather than letting the old sample go
            // stale.
            for kind in [AuthorKind::Human, AuthorKind::Bot] {
                activity.issues.entry(kind).or_insert(0);
                activity.pulls.entry(kind).or_insert(0);
                activity.draft_pulls.entry(kind).or_insert(0);
            }

            out.insert(repo.full_name(), activity);
        }
    }

    Ok(out)
}

/// Applies the operator's filters to a discovery result.
///
/// Returns the repositories to monitor plus the reason each other repository
/// was dropped.
pub fn partition_repos(
    discovered: Vec<DiscoveredRepo>,
    is_denylisted: &dyn Fn(&str) -> bool,
    max_age: Option<chrono::Duration>,
    now: DateTime<Utc>,
) -> (Vec<Repo>, Vec<(Repo, SkipReason)>) {
    let mut keep = Vec::new();
    let mut skip = Vec::new();

    for entry in discovered {
        let full_name = entry.repo.full_name();
        if entry.is_archived {
            skip.push((entry.repo, SkipReason::Archived));
        } else if is_denylisted(&full_name) {
            skip.push((entry.repo, SkipReason::Denylisted));
        } else if let Some(max_age) = max_age
            && entry.pushed_at.is_some_and(|at| now - at > max_age)
        {
            skip.push((entry.repo, SkipReason::Inactive));
        } else {
            keep.push(entry.repo);
        }
    }

    (keep, skip)
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "panicking is how a test reports failure"
)]
mod tests {
    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;

    fn client_for(server: &MockServer) -> Client {
        Client::new(
            "test-token",
            &server.uri(),
            &format!("{}/graphql", server.uri()),
        )
        .expect("client should build")
    }

    /// A discovery page as GitHub returns it, under the `repositoryOwner` key.
    ///
    /// `owner` is the login the *node* reports, which is not necessarily the
    /// login that was queried — see
    /// `a_collaborator_repository_keeps_its_real_owner`.
    fn discovery_page(owner: &str, name: &str) -> serde_json::Value {
        json!({
            "data": {
                "repositoryOwner": {
                    "repositories": {
                        "pageInfo": { "hasNextPage": false, "endCursor": null },
                        "nodes": [{
                            "name": name,
                            "owner": { "login": owner },
                            "isArchived": false,
                            "pushedAt": "2026-01-01T00:00:00Z",
                            "defaultBranchRef": { "name": "main" }
                        }]
                    }
                }
            }
        })
    }

    #[test]
    fn discovery_resolves_through_the_repository_owner_interface() {
        // Regression guard: `organization(login:)` resolves to null for a
        // personal account, so an owner like `fredclausen` failed discovery
        // every cycle with "not found". `repositoryOwner` is the interface
        // both User and Organization implement.
        assert!(
            DISCOVERY_QUERY.contains("repositoryOwner(login: $owner)"),
            "discovery must not be scoped to organisations"
        );
        assert!(
            !DISCOVERY_QUERY.contains("organization("),
            "an org-only root field excludes personal accounts"
        );
    }

    #[tokio::test]
    async fn discovers_repositories_of_a_personal_account() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(discovery_page("fredclausen", "freminal")),
            )
            .expect(1)
            .mount(&server)
            .await;

        let found = discover_owner(&client_for(&server), "fredclausen")
            .await
            .expect("a user must be discoverable");

        assert_eq!(found.len(), 1);
        assert_eq!(found[0].repo.owner, "fredclausen");
        assert_eq!(found[0].repo.name, "freminal");
        assert_eq!(found[0].repo.default_branch, "main");
    }

    #[tokio::test]
    async fn discovers_repositories_of_an_organisation() {
        // The same code path must keep working for orgs; the interface change
        // is not allowed to regress the original use case.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(discovery_page("sdr-enthusiasts", "docker-tar1090")),
            )
            .mount(&server)
            .await;

        let found = discover_owner(&client_for(&server), "sdr-enthusiasts")
            .await
            .expect("an org must still be discoverable");

        assert_eq!(found.len(), 1);
        assert_eq!(found[0].repo.owner, "sdr-enthusiasts");
    }

    #[test]
    fn discovery_requests_owned_repositories_only() {
        // A `User`'s repositories connection includes COLLABORATOR by default,
        // which pulls in repositories belonging to other accounts.
        assert!(
            DISCOVERY_QUERY.contains("ownerAffiliations: [OWNER]"),
            "collaborator repositories belong to their own owner, not this one"
        );
    }

    #[tokio::test]
    async fn a_collaborator_repository_keeps_its_real_owner() {
        // Regression guard for the bug that shipped alongside the user
        // support: pairing the bare `nodes.name` with the *queried* login
        // turned `fredsystems/nixos`, reachable through fredclausen's
        // collaborator affiliation, into `fredclausen/nixos` -- a second
        // series for an already-monitored repository, under an `org` label
        // that does not exist. `ownerAffiliations` should keep such a node
        // out; if one ever arrives anyway, it must not be misattributed.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(discovery_page("fredsystems", "nixos")),
            )
            .mount(&server)
            .await;

        let found = discover_owner(&client_for(&server), "fredclausen")
            .await
            .expect("discovery succeeds");

        assert_eq!(
            found[0].repo.owner, "fredsystems",
            "the owner must come from the response, not the query"
        );
        assert_eq!(found[0].repo.full_name(), "fredsystems/nixos");
    }

    #[tokio::test]
    async fn an_unknown_owner_is_an_error_not_an_empty_set() {
        // A typo'd owner must be loud. Reporting zero repositories would read
        // as "this owner has no CI", which is indistinguishable from success.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"data": {"repositoryOwner": null}})),
            )
            .mount(&server)
            .await;

        let error = discover_owner(&client_for(&server), "nope")
            .await
            .expect_err("a missing owner must fail");
        assert!(
            error.to_string().contains("nope"),
            "the error must name the owner: {error}"
        );
    }

    fn repo(owner: &str, name: &str) -> Repo {
        Repo {
            owner: owner.to_owned(),
            name: name.to_owned(),
            default_branch: "main".to_owned(),
        }
    }

    fn discovered(name: &str, archived: bool, pushed: Option<DateTime<Utc>>) -> DiscoveredRepo {
        DiscoveredRepo {
            repo: repo("sdr-enthusiasts", name),
            is_archived: archived,
            pushed_at: pushed,
        }
    }

    #[test]
    fn batched_query_aliases_every_repo() {
        let a = repo("fredsystems", "nixos");
        let b = repo("sdr-enthusiasts", "docker-tar1090");
        let query = build_activity_query(&[&a, &b]);

        assert!(query.contains(r#"r0: repository(owner: "fredsystems", name: "nixos")"#));
        assert!(
            query.contains(r#"r1: repository(owner: "sdr-enthusiasts", name: "docker-tar1090")"#)
        );
        assert!(query.contains("fragment A on Repository"));
        assert!(query.contains("__typename"), "bot detection needs typename");
    }

    fn pull(
        checks: ChecksState,
        mergeable: MergeableState,
        auto_merge: bool,
        is_draft: bool,
    ) -> OpenPull {
        OpenPull {
            number: 1,
            author: "someone".to_owned(),
            author_kind: AuthorKind::Human,
            is_draft,
            created_at: Utc::now(),
            checks,
            mergeable,
            auto_merge,
        }
    }

    #[test]
    fn failing_checks_need_attention() {
        let p = pull(
            ChecksState::Failure,
            MergeableState::Mergeable,
            false,
            false,
        );
        assert!(p.needs_attention());
        assert!(!p.is_ready_to_merge());
    }

    #[test]
    fn green_without_automerge_is_ready_to_merge() {
        // The case that motivated this: renovate PRs sitting green because
        // automerge is off on that repo, waiting on a human to click.
        let p = pull(
            ChecksState::Success,
            MergeableState::Mergeable,
            false,
            false,
        );
        assert!(p.is_ready_to_merge());
        assert!(p.needs_attention());
    }

    #[test]
    fn green_with_automerge_needs_nothing() {
        let p = pull(ChecksState::Success, MergeableState::Mergeable, true, false);
        assert!(!p.is_ready_to_merge(), "automerge will land it unattended");
        assert!(!p.needs_attention());
    }

    #[test]
    fn conflicting_needs_attention_even_when_green() {
        let p = pull(
            ChecksState::Success,
            MergeableState::Conflicting,
            true,
            false,
        );
        assert!(p.needs_attention());
        assert!(!p.is_ready_to_merge(), "a conflict is not mergeable");
    }

    #[test]
    fn unknown_mergeable_is_not_treated_as_mergeable() {
        // GitHub computes mergeability lazily, so a first query commonly
        // returns UNKNOWN for a PR that is in fact conflicting -- observed on
        // nixos#1615 and gitbook-adsb-guide#176, both of which resolved to
        // CONFLICTING on re-query. Treating UNKNOWN as mergeable would report
        // conflicted PRs as ready to merge.
        let p = pull(ChecksState::Success, MergeableState::Unknown, false, false);
        assert!(!p.is_ready_to_merge());
    }

    #[test]
    fn pending_checks_need_nothing_yet() {
        let p = pull(
            ChecksState::Pending,
            MergeableState::Mergeable,
            false,
            false,
        );
        assert!(!p.needs_attention(), "CI is still running");
    }

    #[test]
    fn repos_without_pr_ci_are_not_stuck_pending() {
        // A repo with no PR-triggered workflows reports no rollup at all.
        // That must not read as "waiting for CI" forever, nor as ready to
        // merge, since nothing has verified it.
        let p = pull(ChecksState::None, MergeableState::Mergeable, false, false);
        assert!(!p.is_ready_to_merge());
        assert!(!p.needs_attention());
    }

    #[test]
    fn drafts_never_need_attention() {
        for checks in [ChecksState::Failure, ChecksState::Success] {
            let p = pull(checks, MergeableState::Mergeable, false, true);
            assert!(!p.needs_attention(), "an open draft is work in progress");
            assert!(!p.is_ready_to_merge());
        }
    }

    #[test]
    fn checks_state_maps_error_as_failure() {
        // ERROR is a failed commit status rather than a failed check run;
        // both block a merge.
        assert_eq!(ChecksState::from_api(Some("ERROR")), ChecksState::Failure);
        assert_eq!(ChecksState::from_api(Some("FAILURE")), ChecksState::Failure);
        assert_eq!(ChecksState::from_api(Some("SUCCESS")), ChecksState::Success);
        assert_eq!(ChecksState::from_api(Some("PENDING")), ChecksState::Pending);
        assert_eq!(ChecksState::from_api(None), ChecksState::None);
    }

    #[test]
    fn archived_repos_are_skipped() {
        // Regression guard: archived repos still return open PRs from the
        // search API, so they must be dropped during discovery.
        let input = vec![
            discovered("sdre-hub", true, None),
            discovered("docker-tar1090", false, None),
        ];
        let (keep, skip) = partition_repos(input, &|_| false, None, Utc::now());

        assert_eq!(keep.len(), 1);
        assert_eq!(keep[0].name, "docker-tar1090");
        assert_eq!(skip.len(), 1);
        assert_eq!(skip[0].1, SkipReason::Archived);
    }

    #[test]
    fn denylist_is_applied_after_archive_check() {
        let input = vec![
            discovered("docker-tar1090", false, None),
            discovered("sdr-enthusiast-website", false, None),
        ];
        let (keep, skip) = partition_repos(
            input,
            &|name| name == "sdr-enthusiasts/sdr-enthusiast-website",
            None,
            Utc::now(),
        );

        assert_eq!(keep.len(), 1);
        assert_eq!(skip[0].1, SkipReason::Denylisted);
    }

    #[test]
    fn inactive_repos_are_skipped_only_when_a_window_is_set() {
        let now = Utc::now();
        let stale = now - chrono::Duration::days(900);
        let input = vec![discovered("rbfeeder", false, Some(stale))];

        let (keep, _) = partition_repos(input.clone(), &|_| false, None, now);
        assert_eq!(keep.len(), 1, "no window means keep everything");

        let (keep, skip) =
            partition_repos(input, &|_| false, Some(chrono::Duration::days(365)), now);
        assert!(keep.is_empty());
        assert_eq!(skip[0].1, SkipReason::Inactive);
    }

    #[test]
    fn repos_with_unknown_push_date_are_kept() {
        let input = vec![discovered("mystery", false, None)];
        let (keep, _) = partition_repos(
            input,
            &|_| false,
            Some(chrono::Duration::days(30)),
            Utc::now(),
        );
        assert_eq!(keep.len(), 1);
    }
}
