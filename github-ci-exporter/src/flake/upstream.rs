//! Upstream heads, open update pull requests, and freshness for every
//! GitHub-locked flake input.
//!
//! One aliased GraphQL query per flake pass resolves every tracked node's
//! upstream -- a branch head, or the newest release tag -- and every root
//! input's open `update-<input>` pull request, batched to at most
//! [`graphql::BATCH_SIZE`] aliases per request, reusing the aliasing approach
//! [`graphql::build_activity_query`] already uses. A REST compare, fetched
//! only for a branch that is behind, is what measures `commits_behind` and
//! the oldest missing commit; see [`fetch_compare`].
//!
//! Tag-pinned inputs have no compare to measure "how long has this been
//! outdated" from -- there is no "next" tag to diff against -- so that is
//! persisted instead, through [`super::first_seen`].

use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::{
    first_seen::FirstSeenStore,
    lock::{NodeKind, TrackedNode},
};
use crate::{
    github::{Client, ClientError, graphql},
    metrics::{FlakeInputCommitsBehindLabels, FlakeInputLabels, FlakeInputPrLabels, Metrics},
};

/// Whether a tracked node's lock is current against its upstream.
///
/// Not a bool: `Unknown` is a distinct, legitimate outcome -- the upstream
/// lookup failed this pass, or the node's kind has no upstream comparison at
/// all yet (a channel tarball, subtask 3.6, or an untracked lock type) --
/// and collapsing it into "not up to date" would publish a false `Behind` for
/// a node this pass has no evidence about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    UpToDate,
    Behind,
    Unknown,
}

/// What the upstream pass knows about one tracked node, as of the last
/// successful lookup for it.
///
/// Held per `(repo, path)` in [`UpstreamCache`] and survives across cycles in
/// which the pass itself does not run or a particular node's lookup fails,
/// the same contract [`super::collect::FlakeCache`] gives the tracked node
/// set.
#[derive(Debug, Clone)]
pub struct NodeStatus {
    pub upstream_oid: String,
    pub upstream_tag: Option<String>,
    pub upstream_committed_at: i64,
    pub freshness: Freshness,
    pub outdated_since: Option<i64>,
    pub commits_behind: Option<u64>,
}

/// Cross-cycle state for the upstream pass.
#[derive(Debug, Default)]
pub struct UpstreamCache {
    /// `(repo, path)` -> the node's last successfully resolved status.
    statuses: HashMap<(String, String), NodeStatus>,
    /// `(repo, root input)` -> the open `update-<input>` PR's number.
    open_prs: HashMap<(String, String), u64>,
    first_seen: FirstSeenStore,
    first_seen_loaded: bool,
}

impl UpstreamCache {
    fn ensure_first_seen_loaded(&mut self, state_dir: &Path) {
        if self.first_seen_loaded {
            return;
        }
        self.first_seen = FirstSeenStore::load(&first_seen_path(state_dir));
        self.first_seen_loaded = true;
    }

    /// The last known status for `repo`'s node at `path`, if any lookup has
    /// ever resolved it.
    #[must_use]
    pub fn status_for(&self, repo: &str, path: &str) -> Option<&NodeStatus> {
        self.statuses.get(&(repo.to_owned(), path.to_owned()))
    }

    /// The open `update-<input>` PR number for `repo`'s `input`, if any.
    #[must_use]
    pub fn open_pr(&self, repo: &str, input: &str) -> Option<u64> {
        self.open_prs
            .get(&(repo.to_owned(), input.to_owned()))
            .copied()
    }

    /// Requests this pass should budget for, based on the previous pass's
    /// observed state -- see `FlakeCache::estimate_requests`'s caller in
    /// `collector.rs` for why this has to be estimated before any of this
    /// cycle's own fetching has happened.
    ///
    /// Returns `(core, graphql_batches)`. `nodes_by_repo` is the *previous*
    /// cycle's tracked node set.
    #[must_use]
    pub fn estimate_requests(
        &self,
        repos: &[String],
        nodes_by_repo: &HashMap<String, Vec<TrackedNode>>,
    ) -> (u64, u64) {
        let mut lookups: HashSet<LookupKey> = HashSet::new();
        let mut pr_count = 0u64;
        let mut branch_lookups = 0u64;
        let mut compare_count = 0u64;

        for repo in repos {
            let Some(nodes) = nodes_by_repo.get(repo) else {
                continue;
            };
            for node in nodes {
                if node.path == node.input {
                    pr_count = pr_count.saturating_add(1);
                }
                let Some(key) = lookup_key_for(node) else {
                    continue;
                };
                if lookups.insert(key) && node.kind == NodeKind::Branch {
                    branch_lookups = branch_lookups.saturating_add(1);
                }
                if node.kind == NodeKind::Branch
                    && self
                        .status_for(repo, &node.path)
                        .is_some_and(|s| s.freshness == Freshness::Behind)
                {
                    compare_count = compare_count.saturating_add(1);
                }
            }
        }

        // Before any pass has ever completed there is nothing cached to say
        // which branches are behind, so the pessimistic reading -- every
        // branch node might need a compare -- is used instead. This mirrors
        // `estimate_core_requests`'s own first-cycle fallback.
        if self.statuses.is_empty() {
            compare_count = branch_lookups;
        }

        let total_aliases = lookups.len() as u64 + pr_count;
        let batches = if total_aliases == 0 {
            0
        } else {
            total_aliases.div_ceil(graphql::BATCH_SIZE as u64)
        };

        (compare_count, batches)
    }
}

/// A distinct GitHub lookup an upstream pass needs to resolve: one repository
/// at one ref (a branch, the default branch, or "the newest tag").
///
/// Several tracked nodes -- across repositories, or an owned nixpkgs sharing
/// its parent's upstream -- can resolve to the same key, so lookups are
/// deduplicated by it before the query is built.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LookupKey {
    owner: String,
    repo: String,
    kind: LookupKeyKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum LookupKeyKind {
    /// `None` is the default branch.
    Branch(Option<String>),
    Tag,
}

/// The upstream lookup a node needs, or `None` for a node with nothing to
/// look up: it is not locked through `github` (no owner/repo), or its kind
/// has no upstream notion at all (a channel tarball or an untracked lock).
fn lookup_key_for(node: &TrackedNode) -> Option<LookupKey> {
    let owner = node.owner.clone()?;
    let repo = node.repo.clone()?;
    let kind = match node.kind {
        NodeKind::Branch => LookupKeyKind::Branch(node.reference.clone()),
        NodeKind::Tag => LookupKeyKind::Tag,
        NodeKind::ChannelTarball | NodeKind::Untracked => return None,
    };
    Some(LookupKey { owner, repo, kind })
}

/// One root input's open `update-<input>` PR to look up, scoped to the
/// configured repository that owns the input.
#[derive(Debug, Clone)]
struct PrLookup {
    repo: String,
    input: String,
}

/// Runs the upstream pass: resolves every tracked node's upstream, tops up
/// `commits_behind`/`outdated_since` for anything behind, resolves open
/// update PRs, and persists first-seen state.
///
/// A batch that fails outright (transport error, GraphQL error) is logged and
/// skipped; the nodes it would have resolved keep their previous status,
/// following the same "a broken fetch degrades to last-known-good" contract
/// every other collector in this crate applies.
#[allow(clippy::implicit_hasher)]
// `nodes_by_repo` is always `collect::FlakeCache`'s own std-hasher map; this
// is an internal collector, not a library API meant to be generic over it.
pub async fn run_pass(
    client: &Client,
    repos: &[String],
    nodes_by_repo: &HashMap<String, Vec<TrackedNode>>,
    cache: &mut UpstreamCache,
    state_dir: &Path,
    now: i64,
) {
    cache.ensure_first_seen_loaded(state_dir);

    let mut lookup_order: Vec<LookupKey> = Vec::new();
    let mut seen: HashSet<LookupKey> = HashSet::new();
    let mut pr_lookups: Vec<PrLookup> = Vec::new();

    for repo in repos {
        let Some(nodes) = nodes_by_repo.get(repo) else {
            continue;
        };
        for node in nodes {
            if node.path == node.input {
                pr_lookups.push(PrLookup {
                    repo: repo.clone(),
                    input: node.input.clone(),
                });
            }
            if let Some(key) = lookup_key_for(node)
                && seen.insert(key.clone())
            {
                lookup_order.push(key);
            }
        }
    }

    let mut upstream_results: HashMap<LookupKey, Option<ResolvedUpstream>> = HashMap::new();
    let mut pr_results: HashMap<(String, String), Option<u64>> = HashMap::new();

    let mut lookup_idx = 0usize;
    let mut pr_idx = 0usize;
    while lookup_idx < lookup_order.len() || pr_idx < pr_lookups.len() {
        let mut batch_lookups: Vec<&LookupKey> = Vec::new();
        while batch_lookups.len() < graphql::BATCH_SIZE && lookup_idx < lookup_order.len() {
            batch_lookups.push(&lookup_order[lookup_idx]);
            lookup_idx += 1;
        }
        let mut batch_prs: Vec<&PrLookup> = Vec::new();
        while batch_lookups.len() + batch_prs.len() < graphql::BATCH_SIZE
            && pr_idx < pr_lookups.len()
        {
            batch_prs.push(&pr_lookups[pr_idx]);
            pr_idx += 1;
        }

        let query = build_query(&batch_lookups, &batch_prs);
        match client
            .graphql::<HashMap<String, Option<serde_json::Value>>>(&query, serde_json::json!({}))
            .await
        {
            Ok(raw) => {
                for (index, key) in batch_lookups.iter().enumerate() {
                    let value = raw.get(&format!("u{index}")).cloned().flatten();
                    if value.is_none() {
                        warn!(
                            owner = key.owner,
                            repo = key.repo,
                            "github node's repository no longer exists; no upstream for this pass"
                        );
                    }
                    let resolved = value
                        .and_then(|v| serde_json::from_value::<UpstreamNode>(v).ok())
                        .and_then(|node| node.resolve(&key.kind));
                    upstream_results.insert((*key).clone(), resolved);
                }
                for (index, pr) in batch_prs.iter().enumerate() {
                    let number = raw
                        .get(&format!("p{index}"))
                        .cloned()
                        .flatten()
                        .and_then(|v| serde_json::from_value::<PrNode>(v).ok())
                        .and_then(|node| node.pull_requests.nodes.first().map(|n| n.number));
                    pr_results.insert((pr.repo.clone(), pr.input.clone()), number);
                }
            }
            Err(error) => {
                warn!(%error, "flake upstream batch failed; keeping previous values for its nodes");
            }
        }
    }

    for repo in repos {
        let Some(nodes) = nodes_by_repo.get(repo) else {
            continue;
        };
        for node in nodes {
            apply_upstream(client, repo, node, &upstream_results, cache, now).await;
        }
    }

    for ((repo, input), number) in pr_results {
        match number {
            Some(number) => {
                cache.open_prs.insert((repo, input), number);
            }
            None => {
                cache.open_prs.remove(&(repo, input));
            }
        }
    }

    if let Err(error) = cache.first_seen.persist(&first_seen_path(state_dir)) {
        warn!(%error, "failed to persist flake first-seen state");
    }
}

/// Resolves one node's [`NodeStatus`] from this pass's lookup results and
/// updates `cache` in place.
///
/// A node whose lookup did not resolve this pass -- not attempted, or `null`
/// -- is left untouched, keeping whatever status an earlier pass observed.
async fn apply_upstream(
    client: &Client,
    repo: &str,
    node: &TrackedNode,
    upstream_results: &HashMap<LookupKey, Option<ResolvedUpstream>>,
    cache: &mut UpstreamCache,
    now: i64,
) {
    let Some(key) = lookup_key_for(node) else {
        return;
    };
    let Some(Some(resolved)) = upstream_results.get(&key).cloned() else {
        return;
    };

    let freshness = match node.kind {
        NodeKind::Branch => {
            if node.locked_rev.as_deref() == Some(resolved.oid.as_str()) {
                Freshness::UpToDate
            } else {
                Freshness::Behind
            }
        }
        NodeKind::Tag => {
            if node.reference.as_deref() == resolved.tag_name.as_deref() {
                Freshness::UpToDate
            } else {
                Freshness::Behind
            }
        }
        // `lookup_key_for` never yields a key for these, so this is
        // unreachable in practice; kept for exhaustiveness rather than an
        // `unreachable!()` this crate's lints forbid anyway.
        NodeKind::ChannelTarball | NodeKind::Untracked => return,
    };

    let mut status = NodeStatus {
        upstream_oid: resolved.oid.clone(),
        upstream_tag: resolved.tag_name.clone(),
        upstream_committed_at: resolved.committed_at,
        freshness,
        outdated_since: None,
        commits_behind: None,
    };

    match (node.kind, freshness) {
        (NodeKind::Branch, Freshness::Behind) => {
            if let (Some(owner), Some(repo_name), Some(locked)) =
                (&node.owner, &node.repo, &node.locked_rev)
            {
                match fetch_compare(client, owner, repo_name, locked, &resolved.oid).await {
                    Ok(page) => {
                        status.commits_behind = Some(page.ahead_by);
                        status.outdated_since =
                            page.oldest_commit_date.as_deref().and_then(parse_date_secs);
                    }
                    Err(error) => {
                        warn!(
                            repo,
                            path = node.path,
                            %error,
                            "failed to compare branch against its upstream"
                        );
                    }
                }
            }
        }
        (NodeKind::Tag, Freshness::Behind) => {
            if let Some(tag_name) = &resolved.tag_name {
                let identity = first_seen_identity(repo, &node.path, tag_name);
                status.outdated_since = Some(cache.first_seen.observe_behind(&identity, now));
            }
        }
        (NodeKind::Tag, Freshness::UpToDate) => {
            if let Some(tag_name) = &resolved.tag_name {
                cache
                    .first_seen
                    .clear(&first_seen_identity(repo, &node.path, tag_name));
            }
        }
        _ => {}
    }

    cache
        .statuses
        .insert((repo.to_owned(), node.path.clone()), status);
}

/// The identity key a tag node's first-seen entry is stored under:
/// `<repo>|<path>|<upstream tag name>`. Including the upstream tag means a
/// newer release superseding the one this node was already behind starts a
/// fresh clock against the new target, rather than reporting "behind" for
/// longer than the node has actually been behind *that* release.
fn first_seen_identity(repo: &str, path: &str, upstream_identity: &str) -> String {
    format!("{repo}|{path}|{upstream_identity}")
}

fn first_seen_path(state_dir: &Path) -> std::path::PathBuf {
    state_dir.join("flake-first-seen.json")
}

/// Builds one batch's query, aliasing upstream lookups `u0, u1, ...` and PR
/// lookups `p0, p1, ...`, mirroring `graphql::build_activity_query`'s
/// approach.
fn build_query(upstream: &[&LookupKey], prs: &[&PrLookup]) -> String {
    use std::fmt::Write as _;

    let mut query = String::from("query {\n");
    for (index, key) in upstream.iter().enumerate() {
        match &key.kind {
            LookupKeyKind::Branch(Some(reference)) => {
                let _ = writeln!(
                    query,
                    "  u{index}: repository(owner: \"{}\", name: \"{}\") {{ ref(qualifiedName: \"refs/heads/{reference}\") {{ target {{ ... on Commit {{ oid committedDate }} }} }} }}",
                    key.owner, key.repo
                );
            }
            LookupKeyKind::Branch(None) => {
                let _ = writeln!(
                    query,
                    "  u{index}: repository(owner: \"{}\", name: \"{}\") {{ defaultBranchRef {{ target {{ ... on Commit {{ oid committedDate }} }} }} }}",
                    key.owner, key.repo
                );
            }
            LookupKeyKind::Tag => {
                let _ = writeln!(
                    query,
                    "  u{index}: repository(owner: \"{}\", name: \"{}\") {{ latestRelease {{ tagName tagCommit {{ oid committedDate }} }} refs(refPrefix: \"refs/tags/\", first: 1, orderBy: {{field: TAG_COMMIT_DATE, direction: DESC}}) {{ nodes {{ name target {{ __typename ... on Commit {{ oid committedDate }} ... on Tag {{ target {{ ... on Commit {{ oid committedDate }} }} }} }} }} }} }}",
                    key.owner, key.repo
                );
            }
        }
    }
    for (index, pr) in prs.iter().enumerate() {
        let (owner, name) = pr.repo.split_once('/').unwrap_or(("", ""));
        let _ = writeln!(
            query,
            "  p{index}: repository(owner: \"{owner}\", name: \"{name}\") {{ pullRequests(states: OPEN, headRefName: \"update-{}\", first: 1) {{ nodes {{ number }} }} }}",
            pr.input
        );
    }
    query.push_str("}\n");
    query
}

/// One lookup's resolved upstream: the commit it currently points to, and --
/// for a tag lookup -- the name of the newest release tag.
#[derive(Debug, Clone)]
struct ResolvedUpstream {
    oid: String,
    committed_at: i64,
    tag_name: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct UpstreamNode {
    #[serde(rename = "ref")]
    git_ref: Option<RefWrapper>,
    #[serde(rename = "defaultBranchRef")]
    default_branch_ref: Option<RefWrapper>,
    #[serde(rename = "latestRelease")]
    latest_release: Option<LatestRelease>,
    refs: Option<RefsConnection>,
}

impl UpstreamNode {
    fn resolve(&self, kind: &LookupKeyKind) -> Option<ResolvedUpstream> {
        match kind {
            LookupKeyKind::Branch(_) => self.resolve_branch(),
            LookupKeyKind::Tag => self.resolve_tag(),
        }
    }

    fn resolve_branch(&self) -> Option<ResolvedUpstream> {
        let wrapper = self.git_ref.as_ref().or(self.default_branch_ref.as_ref())?;
        let target = wrapper.target.as_ref()?;
        Some(ResolvedUpstream {
            oid: target.oid.clone(),
            committed_at: target.committed_date.timestamp(),
            tag_name: None,
        })
    }

    fn resolve_tag(&self) -> Option<ResolvedUpstream> {
        if let Some(release) = &self.latest_release
            && let Some(commit) = &release.tag_commit
        {
            return Some(ResolvedUpstream {
                oid: commit.oid.clone(),
                committed_at: commit.committed_date.timestamp(),
                tag_name: Some(release.tag_name.clone()),
            });
        }
        // `latestRelease` is only populated for a repository using GitHub
        // Releases; many tag the repo without one, so the newest tag by
        // commit date is the fallback.
        let node = self.refs.as_ref()?.nodes.first()?;
        let commit = match node.target.as_ref()? {
            TagTarget::Commit(commit) => commit,
            TagTarget::Tag { target } => target.as_ref()?,
        };
        Some(ResolvedUpstream {
            oid: commit.oid.clone(),
            committed_at: commit.committed_date.timestamp(),
            tag_name: Some(node.name.clone()),
        })
    }
}

#[derive(Debug, Deserialize)]
struct RefWrapper {
    target: Option<CommitTarget>,
}

#[derive(Debug, Deserialize)]
struct CommitTarget {
    oid: String,
    #[serde(rename = "committedDate")]
    committed_date: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct LatestRelease {
    #[serde(rename = "tagName")]
    tag_name: String,
    #[serde(rename = "tagCommit")]
    tag_commit: Option<CommitTarget>,
}

#[derive(Debug, Deserialize)]
struct RefsConnection {
    nodes: Vec<TagRefNode>,
}

#[derive(Debug, Deserialize)]
struct TagRefNode {
    name: String,
    target: Option<TagTarget>,
}

/// A tag ref's target: either a commit directly (a lightweight tag), or an
/// annotated tag object pointing at one. `__typename` disambiguates the two,
/// since both inline fragments can otherwise look like a valid match for the
/// other's shape once `target` is optional.
#[derive(Debug, Deserialize)]
#[serde(tag = "__typename")]
enum TagTarget {
    Commit(CommitTarget),
    Tag { target: Option<CommitTarget> },
}

#[derive(Debug, Deserialize)]
struct PrNode {
    #[serde(rename = "pullRequests")]
    pull_requests: PrConnection,
}

#[derive(Debug, Deserialize)]
struct PrConnection {
    nodes: Vec<PrNumberNode>,
}

#[derive(Debug, Deserialize)]
struct PrNumberNode {
    number: u64,
}

/// The projection cached for a branch's REST compare: just enough to derive
/// `commits_behind` and the oldest missing commit's timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ComparePage {
    ahead_by: u64,
    /// The committer date of the *first* commit in `compare`'s `commits`
    /// array, which GitHub returns oldest-first. `None` if the array is
    /// empty, which should not happen for a comparison this function is only
    /// called for when the two sides differ, but a REST response is not a
    /// promise.
    oldest_commit_date: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CompareResponse {
    ahead_by: u64,
    commits: Vec<CompareCommit>,
}

#[derive(Debug, Deserialize)]
struct CompareCommit {
    commit: CompareCommitDetail,
}

#[derive(Debug, Deserialize)]
struct CompareCommitDetail {
    committer: CompareCommitter,
}

#[derive(Debug, Deserialize)]
struct CompareCommitter {
    date: String,
}

fn project_compare(response: CompareResponse) -> ComparePage {
    let CompareResponse { ahead_by, commits } = response;
    ComparePage {
        ahead_by,
        oldest_commit_date: commits
            .first()
            .map(|commit| commit.commit.committer.date.clone()),
    }
}

/// Fetches `GET /repos/{owner}/{repo}/compare/{base}...{head}` through the
/// `ETag` cache.
///
/// Safe to revalidate indefinitely: a compare between two fixed commits is
/// deterministic, so an `ETag` for one `base...head` pair never goes stale in
/// a way that matters -- unlike the runs listing, there is no "the server's
/// answer for the same request changed" case to guard against.
async fn fetch_compare(
    client: &Client,
    owner: &str,
    repo: &str,
    base: &str,
    head: &str,
) -> Result<ComparePage, ClientError> {
    let path = format!("/repos/{owner}/{repo}/compare/{base}...{head}");
    let (page, _outcome) = client.get_cached(&path, project_compare).await?;
    Ok(page)
}

fn parse_date_secs(raw: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|dt| dt.timestamp())
}

/// Publishes every metric this module derives, for the nodes currently
/// tracked by `repo`.
///
/// Called every cycle, independent of whether the upstream pass itself ran
/// this cycle -- the same contract [`super::collect::record_metrics`]
/// follows, since the registry is rebuilt from scratch each cycle.
pub fn record_metrics(metrics: &Metrics, repo: &str, nodes: &[TrackedNode], cache: &UpstreamCache) {
    for node in nodes {
        let Some(status) = cache.status_for(repo, &node.path) else {
            continue;
        };
        let labels = FlakeInputLabels {
            repo: repo.to_owned(),
            input: node.input.clone(),
            path: node.path.clone(),
            r#ref: node.reference.clone().unwrap_or_default(),
            channel: node.channel.clone().unwrap_or_default(),
        };
        metrics
            .flake_input_upstream_timestamp
            .get_or_create(&labels)
            .set(status.upstream_committed_at);

        match status.freshness {
            Freshness::UpToDate => {
                metrics.flake_input_up_to_date.get_or_create(&labels).set(1);
            }
            Freshness::Behind => {
                metrics.flake_input_up_to_date.get_or_create(&labels).set(0);
                if let Some(since) = status.outdated_since {
                    metrics
                        .flake_input_outdated_since
                        .get_or_create(&labels)
                        .set(since);
                }
                if let Some(behind) = status.commits_behind {
                    metrics
                        .flake_input_commits_behind
                        .get_or_create(&FlakeInputCommitsBehindLabels {
                            repo: repo.to_owned(),
                            input: node.input.clone(),
                            path: node.path.clone(),
                            r#ref: node.reference.clone().unwrap_or_default(),
                        })
                        .set(i64::try_from(behind).unwrap_or(i64::MAX));
                }
            }
            Freshness::Unknown => {}
        }
    }

    for node in nodes.iter().filter(|node| node.path == node.input) {
        if let Some(number) = cache.open_pr(repo, &node.input) {
            metrics
                .flake_input_update_pr_open
                .get_or_create(&FlakeInputPrLabels {
                    repo: repo.to_owned(),
                    input: node.input.clone(),
                    number: number.to_string(),
                })
                .set(1);
        }
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
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::metrics::{Metrics, Publisher};

    fn client_for(server: &MockServer) -> Client {
        Client::new("t", &server.uri(), &format!("{}/graphql", server.uri()))
            .expect("client builds")
    }

    fn branch_node(
        input: &str,
        owner: &str,
        repo: &str,
        reference: Option<&str>,
        locked_rev: &str,
    ) -> TrackedNode {
        TrackedNode {
            input: input.to_owned(),
            path: input.to_owned(),
            kind: NodeKind::Branch,
            reference: reference.map(str::to_owned),
            channel: None,
            locked_rev: Some(locked_rev.to_owned()),
            locked_last_modified: Some(1_700_000_000),
            owner: Some(owner.to_owned()),
            repo: Some(repo.to_owned()),
        }
    }

    fn tag_node(
        input: &str,
        owner: &str,
        repo: &str,
        reference: &str,
        locked_rev: &str,
    ) -> TrackedNode {
        TrackedNode {
            input: input.to_owned(),
            path: input.to_owned(),
            kind: NodeKind::Tag,
            reference: Some(reference.to_owned()),
            channel: None,
            locked_rev: Some(locked_rev.to_owned()),
            locked_last_modified: Some(1_700_000_000),
            owner: Some(owner.to_owned()),
            repo: Some(repo.to_owned()),
        }
    }

    #[test]
    fn query_construction_covers_every_shape() {
        let keys = [
            LookupKey {
                owner: "o".into(),
                repo: "branch-ref".into(),
                kind: LookupKeyKind::Branch(Some("release-1".into())),
            },
            LookupKey {
                owner: "o".into(),
                repo: "default-branch".into(),
                kind: LookupKeyKind::Branch(None),
            },
            LookupKey {
                owner: "o".into(),
                repo: "tagged".into(),
                kind: LookupKeyKind::Tag,
            },
        ];
        let refs: Vec<&LookupKey> = keys.iter().collect();
        let pr = PrLookup {
            repo: "fredsystems/nixos".into(),
            input: "niri".into(),
        };
        let query = build_query(&refs, &[&pr]);

        assert!(query.contains(
            r#"u0: repository(owner: "o", name: "branch-ref") { ref(qualifiedName: "refs/heads/release-1")"#
        ));
        assert!(
            query.contains(
                r#"u1: repository(owner: "o", name: "default-branch") { defaultBranchRef"#
            )
        );
        assert!(query.contains(r#"u2: repository(owner: "o", name: "tagged") { latestRelease"#));
        assert!(query.contains("refPrefix: \"refs/tags/\""));
        assert!(query.contains("TAG_COMMIT_DATE"));
        assert!(query.contains(
            r#"p0: repository(owner: "fredsystems", name: "nixos") { pullRequests(states: OPEN, headRefName: "update-niri""#
        ));
    }

    #[test]
    fn dedup_collapses_identical_lookups_to_one_alias() {
        let node_a = branch_node("niri", "epireyn", "niri-flake", None, "abc");
        let node_b = branch_node("niri2", "epireyn", "niri-flake", None, "def");
        let key_a = lookup_key_for(&node_a).expect("branch node yields a key");
        let key_b = lookup_key_for(&node_b).expect("branch node yields a key");
        assert_eq!(key_a, key_b, "same owner/repo/ref must collapse to one key");
    }

    #[tokio::test]
    async fn a_branch_node_resolves_up_to_date_or_behind() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "defaultBranchRef": {
                            "target": { "oid": "newoid", "committedDate": "2026-01-01T00:00:00Z" }
                        }
                    }
                }
            })))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let node = branch_node("colmena", "zhaofengli", "colmena", None, "newoid");
        let mut nodes_by_repo = HashMap::new();
        nodes_by_repo.insert("o/r".to_owned(), vec![node]);
        let mut cache = UpstreamCache::default();
        let dir = tempfile::tempdir().expect("tempdir");

        run_pass(
            &client,
            &["o/r".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            1_700_000_500,
        )
        .await;

        let status = cache.status_for("o/r", "colmena").expect("status recorded");
        assert_eq!(status.freshness, Freshness::UpToDate);
        assert_eq!(status.upstream_oid, "newoid");
    }

    #[tokio::test]
    async fn a_tag_pinned_node_on_the_newest_release_reads_up_to_date() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "latestRelease": {
                            "tagName": "v1.2.3",
                            "tagCommit": { "oid": "tagoid", "committedDate": "2026-01-01T00:00:00Z" }
                        }
                    }
                }
            })))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let node = tag_node("opencode", "sst", "opencode", "v1.2.3", "tagoid");
        let mut nodes_by_repo = HashMap::new();
        nodes_by_repo.insert("o/r".to_owned(), vec![node]);
        let mut cache = UpstreamCache::default();
        let dir = tempfile::tempdir().expect("tempdir");

        run_pass(
            &client,
            &["o/r".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            1_700_000_500,
        )
        .await;

        let status = cache
            .status_for("o/r", "opencode")
            .expect("status recorded");
        assert_eq!(status.freshness, Freshness::UpToDate);
    }

    #[tokio::test]
    async fn an_older_tag_reads_behind_with_outdated_since_from_first_seen() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "latestRelease": {
                            "tagName": "v2.0.0",
                            "tagCommit": { "oid": "newtagoid", "committedDate": "2026-02-01T00:00:00Z" }
                        }
                    }
                }
            })))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let node = tag_node("opencode", "sst", "opencode", "v1.2.3", "oldtagoid");
        let mut nodes_by_repo = HashMap::new();
        nodes_by_repo.insert("o/r".to_owned(), vec![node]);
        let mut cache = UpstreamCache::default();
        let dir = tempfile::tempdir().expect("tempdir");

        run_pass(
            &client,
            &["o/r".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            1_700_000_000,
        )
        .await;

        let status = cache
            .status_for("o/r", "opencode")
            .expect("status recorded");
        assert_eq!(status.freshness, Freshness::Behind);
        assert_eq!(status.outdated_since, Some(1_700_000_000));
    }

    #[tokio::test]
    async fn outdated_since_for_a_branch_stays_put_as_upstream_advances() {
        // Two passes: the compare's oldest commit date must not move just
        // because the upstream oid/date advanced between them, because the
        // base (`locked_rev`) has not changed.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "defaultBranchRef": {
                            "target": { "oid": "oid1", "committedDate": "2026-01-01T00:00:00Z" }
                        }
                    }
                }
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/o/niri-flake/compare/locked...oid1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ahead_by": 1,
                "commits": [
                    { "commit": { "committer": { "date": "2025-12-01T00:00:00Z" } } }
                ],
            })))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let node = branch_node("niri", "o", "niri-flake", None, "locked");
        let mut nodes_by_repo = HashMap::new();
        nodes_by_repo.insert("o/r".to_owned(), vec![node.clone()]);
        let mut cache = UpstreamCache::default();
        let dir = tempfile::tempdir().expect("tempdir");

        run_pass(
            &client,
            &["o/r".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            1,
        )
        .await;
        let first = cache
            .status_for("o/r", "niri")
            .expect("status recorded")
            .outdated_since;
        assert_eq!(
            first,
            Some(1_764_547_200),
            "2025-12-01T00:00:00Z in epoch seconds"
        );

        // Second pass: upstream moved to oid2, but the compare base (`locked`)
        // is unchanged, so GitHub's compare for locked...oid2 still starts
        // with the same oldest commit.
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "defaultBranchRef": {
                            "target": { "oid": "oid2", "committedDate": "2026-01-02T00:00:00Z" }
                        }
                    }
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/o/niri-flake/compare/locked...oid2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ahead_by": 2,
                "commits": [
                    { "commit": { "committer": { "date": "2025-12-01T00:00:00Z" } } },
                    { "commit": { "committer": { "date": "2025-12-15T00:00:00Z" } } }
                ],
            })))
            .mount(&server)
            .await;

        run_pass(
            &client,
            &["o/r".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            2,
        )
        .await;
        let second = cache.status_for("o/r", "niri").expect("status recorded");
        assert_eq!(
            second.outdated_since, first,
            "the oldest missing commit is unchanged"
        );
        assert_eq!(second.commits_behind, Some(2));
    }

    #[tokio::test]
    async fn a_deleted_repo_graphql_null_does_not_fail_the_pass() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "data": { "u0": null } })),
            )
            .mount(&server)
            .await;

        let client = client_for(&server);
        let node = branch_node("gone", "o", "gone-repo", None, "abc");
        let mut nodes_by_repo = HashMap::new();
        nodes_by_repo.insert("o/r".to_owned(), vec![node]);
        let mut cache = UpstreamCache::default();
        let dir = tempfile::tempdir().expect("tempdir");

        run_pass(
            &client,
            &["o/r".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            1,
        )
        .await;

        assert!(cache.status_for("o/r", "gone").is_none());
    }

    #[tokio::test]
    async fn an_open_update_pr_is_recorded_with_its_number() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "defaultBranchRef": {
                            "target": { "oid": "abc", "committedDate": "2026-01-01T00:00:00Z" }
                        }
                    },
                    "p0": { "pullRequests": { "nodes": [ { "number": 42 } ] } }
                }
            })))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let node = branch_node("niri", "o", "r", None, "abc");
        let mut nodes_by_repo = HashMap::new();
        nodes_by_repo.insert("fredsystems/nixos".to_owned(), vec![node]);
        let mut cache = UpstreamCache::default();
        let dir = tempfile::tempdir().expect("tempdir");

        run_pass(
            &client,
            &["fredsystems/nixos".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            1,
        )
        .await;

        assert_eq!(cache.open_pr("fredsystems/nixos", "niri"), Some(42));
    }

    #[tokio::test]
    async fn up_to_date_clears_a_stale_first_seen_entry() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "latestRelease": {
                            "tagName": "v1.0.0",
                            "tagCommit": { "oid": "oid", "committedDate": "2026-01-01T00:00:00Z" }
                        }
                    }
                }
            })))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let node = tag_node("opencode", "sst", "opencode", "v1.0.0", "oid");
        let mut nodes_by_repo = HashMap::new();
        nodes_by_repo.insert("o/r".to_owned(), vec![node]);
        let mut cache = UpstreamCache::default();
        cache
            .first_seen
            .observe_behind(&first_seen_identity("o/r", "opencode", "v1.0.0"), 1);
        let dir = tempfile::tempdir().expect("tempdir");

        run_pass(
            &client,
            &["o/r".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            2,
        )
        .await;

        assert_eq!(
            cache
                .first_seen
                .observe_behind(&first_seen_identity("o/r", "opencode", "v1.0.0"), 999),
            999,
            "the stale entry must have been cleared, so this re-observes fresh"
        );
    }

    #[tokio::test]
    async fn budget_exhaustion_keeps_previous_values_published() {
        // A GraphQL failure must not clear what a previous pass already
        // resolved -- the metrics rendered from the cache must still carry
        // the prior status.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "defaultBranchRef": {
                            "target": { "oid": "abc", "committedDate": "2026-01-01T00:00:00Z" }
                        }
                    }
                }
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let node = branch_node("colmena", "o", "r", None, "abc");
        let mut nodes_by_repo = HashMap::new();
        nodes_by_repo.insert("o/r".to_owned(), vec![node.clone()]);
        let mut cache = UpstreamCache::default();
        let dir = tempfile::tempdir().expect("tempdir");

        run_pass(
            &client,
            &["o/r".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            1,
        )
        .await;
        assert_eq!(
            cache.status_for("o/r", "colmena").map(|s| s.freshness),
            Some(Freshness::UpToDate)
        );

        run_pass(
            &client,
            &["o/r".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            2,
        )
        .await;
        assert_eq!(
            cache.status_for("o/r", "colmena").map(|s| s.freshness),
            Some(Freshness::UpToDate),
            "a failed batch must not erase the previous status"
        );

        let (metrics, registry) = Metrics::new();
        record_metrics(&metrics, "o/r", &[node], &cache);
        let rendered = Publisher::new(metrics, registry).render();
        assert!(rendered.contains("github_flake_input_up_to_date"));
    }

    #[test]
    fn example_metrics_output_covers_every_new_family() {
        let (metrics, registry) = Metrics::new();
        let mut cache = UpstreamCache::default();
        cache.statuses.insert(
            ("fredsystems/nixos".to_owned(), "niri".to_owned()),
            NodeStatus {
                upstream_oid: "newoid".to_owned(),
                upstream_tag: None,
                upstream_committed_at: 1_700_000_100,
                freshness: Freshness::Behind,
                outdated_since: Some(1_700_000_000),
                commits_behind: Some(3),
            },
        );
        cache
            .open_prs
            .insert(("fredsystems/nixos".to_owned(), "niri".to_owned()), 42);

        let node = branch_node("niri", "epireyn", "niri-flake", None, "oldoid");
        record_metrics(&metrics, "fredsystems/nixos", &[node], &cache);
        let rendered = Publisher::new(metrics, registry).render();

        assert!(rendered.contains("github_flake_input_upstream_timestamp_seconds"));
        assert!(rendered.contains("github_flake_input_up_to_date"));
        assert!(rendered.contains("github_flake_input_outdated_since_timestamp_seconds"));
        assert!(rendered.contains("github_flake_input_commits_behind"));
        assert!(rendered.contains("github_flake_input_update_pr_open"));
        assert!(rendered.contains(r#"number="42""#));
    }
}
