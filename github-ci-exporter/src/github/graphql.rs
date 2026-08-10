//! GraphQL collectors: repository discovery, issues, and pull requests.
//!
//! Everything here is batched. Discovery pages 100 repositories per request
//! and the per-repo issue/PR query aliases many repositories into a single
//! document, keeping a full sweep at a handful of rate-limit points.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::client::{Client, ClientError};
use crate::model::{AuthorKind, Repo, SkipReason};

/// Repositories per discovery page. 100 is GraphQL's maximum page size.
const DISCOVERY_PAGE_SIZE: usize = 100;

/// Repositories per batched issue/PR query.
///
/// GraphQL cost scales with aliased root fields; 50 keeps a single request
/// comfortably under the node limit while still covering ~60 repos in two
/// requests.
const BATCH_SIZE: usize = 50;

const DISCOVERY_QUERY: &str = r"
query($org: String!, $cursor: String) {
  organization(login: $org) {
    repositories(first: 100, after: $cursor, orderBy: {field: NAME, direction: ASC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        name
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
    organization: Option<DiscoveryOrg>,
}

#[derive(Debug, Deserialize)]
struct DiscoveryOrg {
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
    #[serde(rename = "isArchived")]
    is_archived: bool,
    #[serde(rename = "pushedAt")]
    pushed_at: Option<DateTime<Utc>>,
    #[serde(rename = "defaultBranchRef")]
    default_branch_ref: Option<BranchRef>,
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

/// Enumerates every repository in an organisation, following pagination.
///
/// Archived repositories are returned rather than dropped, so the caller can
/// account for *why* each repository was skipped. Note that archived repos
/// still surface open issues and PRs via the search API, so they must be
/// filtered out here at the source.
///
/// # Errors
/// Returns [`ClientError`] if any page fails.
pub async fn discover_org(client: &Client, org: &str) -> Result<Vec<DiscoveredRepo>, ClientError> {
    let mut cursor: Option<String> = None;
    let mut found = Vec::with_capacity(DISCOVERY_PAGE_SIZE);

    loop {
        let variables = serde_json::json!({ "org": org, "cursor": cursor });
        let response: DiscoveryResponse = client.graphql(DISCOVERY_QUERY, variables).await?;
        let Some(organization) = response.organization else {
            return Err(ClientError::GraphQl(format!(
                "organization `{org}` not found or not visible to this token"
            )));
        };

        for node in organization.repositories.nodes {
            // A repository with no default branch ref is empty (never had a
            // commit); there is nothing to report CI for.
            let Some(branch) = node.default_branch_ref else {
                continue;
            };
            found.push(DiscoveredRepo {
                repo: Repo {
                    owner: org.to_owned(),
                    name: node.name,
                    default_branch: branch.name,
                },
                is_archived: node.is_archived,
                pushed_at: node.pushed_at,
            });
        }

        if !organization.repositories.page_info.has_next_page {
            break;
        }
        cursor = organization.repositories.page_info.end_cursor;
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
    nodes { number isDraft createdAt author { login __typename } }
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
    use super::*;

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
