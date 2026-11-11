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
    github::{Client, ClientError, client::GraphQlError, graphql},
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

/// Whether a `Behind` branch's REST compare (`commits_behind`,
/// `outdated_since`) reflects its current `locked_rev`/upstream pair.
///
/// Not a bool: a failed compare must not silently masquerade as either "no
/// comparison needed" (`Complete` on a node with nothing stale to report) or
/// a successful one, and the two failure sub-cases need different handling
/// -- see [`apply_upstream`]'s `(NodeKind::Branch, Freshness::Behind)` arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonState {
    /// Nothing is stale: not a `Behind` branch (a tag, or a branch that is
    /// up to date), or a `Behind` branch whose REST compare succeeded this
    /// pass or still applies from an earlier one.
    Complete,
    /// A `Behind` branch whose REST compare failed this pass, and whose
    /// previous comparison no longer applies because `locked_rev` or the
    /// upstream `oid` moved since it was computed. `commits_behind` and
    /// `outdated_since` are stale or absent and must not be published as
    /// current; the compare is retried next pass rather than waiting out a
    /// full `flake_interval`.
    Pending,
}

/// The node identity a cached [`NodeStatus`] was computed for.
///
/// Stored alongside the status so a later cycle can tell whether it still
/// describes the node currently at this `(repo, path)` key, or is a fossil
/// left behind by a node that changed shape at the same path while the
/// upstream pass could not refresh it -- repointed to a different `ref` or
/// fork, or swapped from a `github` lock to a channel tarball. A status
/// whose identity no longer matches must not keep publishing a freshness
/// this pass has no evidence for.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeIdentity {
    kind: NodeKind,
    owner: Option<String>,
    repo: Option<String>,
    reference: Option<String>,
    locked_rev: Option<String>,
}

impl NodeIdentity {
    fn of(node: &TrackedNode) -> Self {
        Self {
            kind: node.kind,
            owner: node.owner.clone(),
            repo: node.repo.clone(),
            reference: node.reference.clone(),
            locked_rev: node.locked_rev.clone(),
        }
    }
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
    pub comparison: ComparisonState,
    /// The node this status was computed for -- see [`NodeIdentity`].
    identity: NodeIdentity,
}

impl NodeStatus {
    /// Builds a [`NodeStatus`] for `node` whose freshness is already known
    /// outright, rather than derived from this module's own GitHub
    /// comparison.
    ///
    /// The identity-carrying constructor [`super::channels`] (3.6) needs: a
    /// channel-tarball node's upstream is a NixOS channel, not anything this
    /// module's GraphQL query resolves, so that module computes its own
    /// freshness and uses this to stamp the result with `node`'s
    /// [`NodeIdentity`] -- the same identity [`UpstreamCache::status_for`]
    /// checks at publish time -- without reaching into a private field.
    /// `comparison` is always [`ComparisonState::Complete`]: there is no
    /// REST compare to fail for a node with no GitHub lookup at all, so
    /// nothing here can ever be `Pending`.
    #[must_use]
    pub fn for_node(
        node: &TrackedNode,
        upstream_oid: String,
        upstream_tag: Option<String>,
        upstream_committed_at: i64,
        freshness: Freshness,
    ) -> Self {
        Self {
            upstream_oid,
            upstream_tag,
            upstream_committed_at,
            freshness,
            outdated_since: None,
            commits_behind: None,
            comparison: ComparisonState::Complete,
            identity: NodeIdentity::of(node),
        }
    }
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

    /// The last known status for `repo`'s `node`, if any lookup has ever
    /// resolved it *and* that status's node identity still matches `node`'s
    /// current one.
    ///
    /// A mismatch means the node's shape changed -- a different `ref`,
    /// owner, repository, or kind -- since the status was computed, most
    /// often because a `flake.lock` change landed on a cycle whose upstream
    /// batch itself failed, so the stale entry rode along under the
    /// unchanged `(repo, path)` key. Publishing it would attach a freshness
    /// computed for the old node to labels describing the new one.
    #[must_use]
    pub fn status_for(&self, repo: &str, node: &TrackedNode) -> Option<&NodeStatus> {
        let status = self.statuses.get(&(repo.to_owned(), node.path.clone()))?;
        (status.identity == NodeIdentity::of(node)).then_some(status)
    }

    /// The open `update-<input>` PR number for `repo`'s `input`, if any.
    #[must_use]
    pub fn open_pr(&self, repo: &str, input: &str) -> Option<u64> {
        self.open_prs
            .get(&(repo.to_owned(), input.to_owned()))
            .copied()
    }

    /// Records a tracked node's [`NodeStatus`] directly, bypassing this
    /// pass's own GitHub lookup.
    ///
    /// The other seam `super::channels` (3.6) needs to publish a
    /// channel-tarball node's freshness through the exact same
    /// `github_flake_input_*` families this module already renders: its
    /// upstream is a NixOS channel, not anything this module's own GraphQL
    /// query resolves, so it computes the [`NodeStatus`] itself -- via
    /// [`NodeStatus::for_node`], so it still carries the right
    /// [`NodeIdentity`] -- and stores it here rather than this module
    /// growing a second upstream-lookup strategy.
    pub fn set_status(&mut self, repo: &str, path: &str, status: NodeStatus) {
        self.statuses
            .insert((repo.to_owned(), path.to_owned()), status);
    }

    /// Records `identity` as behind, returning the timestamp it was first
    /// observed that way -- see [`FirstSeenStore::observe_behind`].
    ///
    /// Exposed so [`super::channels`] can measure a channel-tarball node's
    /// `outdated_since` from its own identity (its locked revision, not the
    /// channel's -- see that module's documentation for why) without this
    /// module growing tarball-specific logic of its own.
    pub fn observe_behind(&mut self, identity: &str, now: i64) -> i64 {
        self.first_seen.observe_behind(identity, now)
    }

    /// Clears `identity`'s first-seen entry -- see
    /// [`FirstSeenStore::clear`] and [`Self::observe_behind`].
    pub fn clear_behind(&mut self, identity: &str) {
        self.first_seen.clear(identity);
    }

    /// Requests this pass should budget for, based on the previous pass's
    /// observed state -- see `FlakeCache::estimate_requests`'s caller in
    /// `collector.rs` for why this has to be estimated before any of this
    /// cycle's own fetching has happened.
    ///
    /// Returns `(core, graphql_batches)`. `nodes_by_repo` is the *previous*
    /// cycle's tracked node set.
    ///
    /// The REST-compare reserve is pessimistic on purpose, and deliberately
    /// not deduplicated the way the GraphQL lookup itself is: a compare is
    /// fetched per *node*, not per deduplicated upstream lookup, because
    /// `apply_upstream` diffs each node's own `locked_rev` against the
    /// resolved upstream oid even when several nodes share one lookup (an
    /// owned nixpkgs following its parent's ref). And it is reserved for
    /// every eligible branch node regardless of its last-known freshness,
    /// not only ones previously cached as `Behind`: a node cached as up to
    /// date can flip behind this very pass, and undercounting that is the
    /// budget-exhaustion failure mode this estimate exists to avoid.
    #[must_use]
    pub fn estimate_requests(
        &self,
        repos: &[String],
        nodes_by_repo: &HashMap<String, Vec<TrackedNode>>,
    ) -> (u64, u64) {
        let mut lookups: HashSet<LookupKey> = HashSet::new();
        let mut pr_count = 0u64;
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
                lookups.insert(key);
                if node.kind == NodeKind::Branch {
                    compare_count = compare_count.saturating_add(1);
                }
            }
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

/// What one batch resolved for one [`LookupKey`], for the nodes that share
/// it.
///
/// Absence from the results map (rather than a variant here) is what
/// represents "this pass has no news either way" -- the batch failed
/// outright, or the key was never attempted -- so [`apply_upstream`] keeps
/// whatever was cached. The two variants below are reserved for the batch
/// having *answered*, with two different answers:
#[derive(Debug, Clone)]
enum LookupOutcome {
    /// The repository resolved to an explicit GraphQL `null` -- deleted or
    /// no longer visible to this token -- including a `NOT_FOUND` error
    /// reported for this alias. This is confirmed gone, not merely
    /// unreachable this pass, so any cached status for the nodes sharing
    /// this key is stale and must be dropped rather than kept -- see
    /// finding I in `apply_upstream`.
    ExplicitNull,
    /// Resolved normally.
    Resolved(ResolvedUpstream),
}

/// Applies one batch's GraphQL response into `upstream_results` and
/// `pr_results`. Split out of [`run_pass`] to keep it under this crate's
/// line-count lint.
///
/// Any error that is not a per-alias `NOT_FOUND` fails the whole batch
/// (neither map is touched): this crate has no way to know which, if any,
/// of its aliases that error applies to, so reusing partial data would risk
/// publishing data for a lookup the error actually invalidated.
fn apply_batch_response(
    raw: &HashMap<String, Option<serde_json::Value>>,
    errors: &[GraphQlError],
    batch_lookups: &[&LookupKey],
    batch_prs: &[&PrLookup],
    upstream_results: &mut HashMap<LookupKey, LookupOutcome>,
    pr_results: &mut HashMap<(String, String), Option<u64>>,
) {
    if let Some(error) = errors.iter().find(|error| !error.is_not_found()) {
        warn!(
            message = error.message,
            kind = error.kind.as_deref().unwrap_or("unknown"),
            "flake upstream batch failed; keeping previous values for its nodes"
        );
        return;
    }
    let not_found_aliases: HashSet<&str> = errors.iter().filter_map(GraphQlError::alias).collect();

    for (index, key) in batch_lookups.iter().enumerate() {
        let alias = format!("u{index}");
        let value = raw.get(&alias).cloned().flatten();
        let explicit_null = value.is_none() || not_found_aliases.contains(alias.as_str());
        if explicit_null {
            warn!(
                owner = key.owner,
                repo = key.repo,
                "github node's repository no longer exists; invalidating its cached status"
            );
            upstream_results.insert((*key).clone(), LookupOutcome::ExplicitNull);
            continue;
        }
        // A value that is present but does not parse into a resolved
        // upstream (an unexpected shape, or a ref that itself does not
        // exist within an existing repository) is left out of the map
        // entirely, not inserted as either variant: that is a softer "no
        // news this pass" than a confirmed-gone repository, so it keeps
        // whatever was cached rather than invalidating it.
        if let Some(resolved) = value
            .and_then(|v| serde_json::from_value::<UpstreamNode>(v).ok())
            .and_then(|node| node.resolve(&key.kind))
        {
            upstream_results.insert((*key).clone(), LookupOutcome::Resolved(resolved));
        }
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

/// Runs the upstream pass: resolves every tracked node's upstream, tops up
/// `commits_behind`/`outdated_since` for anything behind, resolves open
/// update PRs, and persists first-seen state.
///
/// A batch that fails outright (transport error, a GraphQL error other than
/// a per-alias `NOT_FOUND`) is logged and skipped; the nodes it would have
/// resolved keep their previous status, following the same "a broken fetch
/// degrades to last-known-good" contract every other collector in this crate
/// applies. A batch that answers with some aliases `null` (deleted
/// repositories, reported as a `NOT_FOUND` error for that alias, or a bare
/// `null`) still applies every other alias normally -- see
/// [`Client::graphql_partial`].
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

    let mut upstream_results: HashMap<LookupKey, LookupOutcome> = HashMap::new();
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

        let (query, variables) = build_query(&batch_lookups, &batch_prs);
        match client
            .graphql_partial::<HashMap<String, Option<serde_json::Value>>>(&query, variables)
            .await
        {
            Ok((raw, errors)) => apply_batch_response(
                &raw,
                &errors,
                &batch_lookups,
                &batch_prs,
                &mut upstream_results,
                &mut pr_results,
            ),
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
/// A node whose lookup was not attempted this pass, or whose batch failed
/// outright, is left untouched, keeping whatever status an earlier pass
/// observed (finding I: a failed batch is not evidence either way). A node
/// whose repository resolved to an explicit `null` is different: that is
/// confirmed gone, so any cached status is dropped. A node with no
/// applicable lookup at all any more (repointed to a channel tarball, or to
/// an untracked lock shape, at the same path) also has its cached status
/// dropped -- finding D.
async fn apply_upstream(
    client: &Client,
    repo: &str,
    node: &TrackedNode,
    upstream_results: &HashMap<LookupKey, LookupOutcome>,
    cache: &mut UpstreamCache,
    now: i64,
) {
    let cache_key = (repo.to_owned(), node.path.clone());

    let Some(key) = lookup_key_for(node) else {
        cache.statuses.remove(&cache_key);
        return;
    };

    let resolved = match upstream_results.get(&key) {
        None => return,
        Some(LookupOutcome::ExplicitNull) => {
            cache.statuses.remove(&cache_key);
            return;
        }
        Some(LookupOutcome::Resolved(resolved)) => resolved,
    };

    let freshness = match node.kind {
        NodeKind::Branch => {
            if node.locked_rev.as_deref() == Some(resolved.oid.as_str()) {
                Freshness::UpToDate
            } else {
                Freshness::Behind
            }
        }
        // Finding J: a locked tag name matching the newest release is not
        // enough on its own -- a tag can be moved to point at a different
        // commit after the fact, which still resolves the same name. Only
        // agreement on *both* the name and the commit it now points to
        // means nothing has changed underneath the lock.
        NodeKind::Tag => {
            if node.reference.as_deref() == resolved.tag_name.as_deref()
                && node.locked_rev.as_deref() == Some(resolved.oid.as_str())
            {
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
        comparison: ComparisonState::Complete,
        identity: NodeIdentity::of(node),
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
                        // Finding E: a failed compare must not silently wipe
                        // a previously complete one. If the prior status was
                        // computed for the same `locked_rev`/upstream `oid`
                        // pair, nothing about the comparison has actually
                        // changed, so its numbers still apply. Otherwise
                        // they no longer describe this pair at all, so they
                        // are dropped and the comparison is marked `Pending`
                        // -- retried next pass rather than published as
                        // current or silently left stale for a whole
                        // `flake_interval`.
                        let previous = cache.statuses.get(&cache_key).cloned();
                        match previous {
                            // The whole identity, not just locked_rev: a node
                            // that was a tag (or another repository) with the
                            // same oids must not lend this branch a comparison
                            // that never succeeded for it.
                            Some(previous)
                                if previous.identity == NodeIdentity::of(node)
                                    && previous.upstream_oid == resolved.oid =>
                            {
                                status.commits_behind = previous.commits_behind;
                                status.outdated_since = previous.outdated_since;
                                status.comparison = previous.comparison;
                            }
                            _ => {
                                status.comparison = ComparisonState::Pending;
                            }
                        }
                    }
                }
            }
        }
        (NodeKind::Tag, Freshness::Behind) => {
            if let Some(locked_tag) = &node.reference {
                let identity = first_seen_identity(repo, node, locked_tag);
                status.outdated_since = Some(cache.first_seen.observe_behind(&identity, now));
            }
        }
        (NodeKind::Tag, Freshness::UpToDate) => {
            if let Some(locked_tag) = &node.reference {
                cache
                    .first_seen
                    .clear(&first_seen_identity(repo, node, locked_tag));
            }
        }
        _ => {}
    }

    cache.statuses.insert(cache_key, status);
}

/// The identity key a node's first-seen entry is stored under: `<repo>|
/// <path>|<the node's own locked identity>` -- the locked tag for a `Tag`
/// node.
///
/// Keyed on the **locked** identity, not the upstream one: an upstream
/// release superseding the one a node is already behind does not mean the
/// node became freshly outdated just now -- it has been outdated since the
/// lock stopped matching the newest tag, however many releases ago that
/// was. An earlier version of this function keyed on the upstream tag name
/// instead, which restarted the clock on every subsequent release,
/// systematically under-reporting how long a tag-pinned input had actually
/// been behind. The clock now starts the first time *this* locked identity
/// is observed behind, survives further upstream releases, and restarts
/// only when the lock itself moves to point at a different one.
///
/// Generic over `locked_identity` rather than hardcoded to a tag name, so a
/// channel-tarball node (a future addition) can key its own clock on its
/// locked short revision through the same function.
/// The node's source (`owner/repo`) is part of the key, so repointing an input
/// at another repository with the same locked tag starts a new clock instead
/// of inheriting the previous source's.
fn first_seen_identity(repo: &str, node: &TrackedNode, locked_identity: &str) -> String {
    let source_owner = node.owner.as_deref().unwrap_or("");
    let source_repo = node.repo.as_deref().unwrap_or("");
    format!(
        "{repo}|{}|{source_owner}/{source_repo}|{locked_identity}",
        node.path
    )
}

fn first_seen_path(state_dir: &Path) -> std::path::PathBuf {
    state_dir.join("flake-first-seen.json")
}

/// Builds one batch's query, aliasing upstream lookups `u0, u1, ...` and PR
/// lookups `p0, p1, ...`, mirroring `graphql::build_activity_query`'s
/// approach -- plus the variables map the query's declarations refer to.
///
/// Finding F: a branch's `ref` and a root input's name both come from data
/// this crate does not control -- a `flake.lock` committed by someone else,
/// in the repositories this exporter watches. Earlier versions of this
/// function interpolated them straight into the query text inside GraphQL
/// string literals. A `ref` or input name containing a `"` would break out
/// of its literal, at best failing the whole batch with a syntax error --
/// taking every *other* alias in it down too -- and at worst letting a
/// crafted value inject additional fields or directives into the query
/// GitHub actually runs. Every value that varies per lookup is therefore
/// passed as a GraphQL variable instead: nothing but the alias/variable
/// names and static field selections are ever written into the query text.
fn build_query(upstream: &[&LookupKey], prs: &[&PrLookup]) -> (String, serde_json::Value) {
    use std::fmt::Write as _;

    let mut declarations: Vec<String> = Vec::new();
    let mut body = String::new();
    let mut variables = serde_json::Map::new();

    for (index, key) in upstream.iter().enumerate() {
        let owner_var = format!("owner{index}");
        let name_var = format!("name{index}");
        declarations.push(format!("${owner_var}: String!"));
        declarations.push(format!("${name_var}: String!"));
        variables.insert(owner_var.clone(), serde_json::json!(key.owner));
        variables.insert(name_var.clone(), serde_json::json!(key.repo));

        match &key.kind {
            LookupKeyKind::Branch(Some(reference)) => {
                let ref_var = format!("ref{index}");
                declarations.push(format!("${ref_var}: String!"));
                variables.insert(
                    ref_var.clone(),
                    serde_json::json!(format!("refs/heads/{reference}")),
                );
                let _ = writeln!(
                    body,
                    "  u{index}: repository(owner: ${owner_var}, name: ${name_var}) {{ ref(qualifiedName: ${ref_var}) {{ target {{ ... on Commit {{ oid committedDate }} }} }} }}",
                );
            }
            LookupKeyKind::Branch(None) => {
                let _ = writeln!(
                    body,
                    "  u{index}: repository(owner: ${owner_var}, name: ${name_var}) {{ defaultBranchRef {{ target {{ ... on Commit {{ oid committedDate }} }} }} }}",
                );
            }
            LookupKeyKind::Tag => {
                let _ = writeln!(
                    body,
                    "  u{index}: repository(owner: ${owner_var}, name: ${name_var}) {{ latestRelease {{ tagName tagCommit {{ oid committedDate }} }} refs(refPrefix: \"refs/tags/\", first: 1, orderBy: {{field: TAG_COMMIT_DATE, direction: DESC}}) {{ nodes {{ name target {{ __typename ... on Commit {{ oid committedDate }} ... on Tag {{ target {{ ... on Commit {{ oid committedDate }} }} }} }} }} }} }}",
                );
            }
        }
    }
    for (index, pr) in prs.iter().enumerate() {
        let (owner, name) = pr.repo.split_once('/').unwrap_or(("", ""));
        let owner_var = format!("prOwner{index}");
        let name_var = format!("prName{index}");
        let head_var = format!("headRef{index}");
        declarations.push(format!("${owner_var}: String!"));
        declarations.push(format!("${name_var}: String!"));
        declarations.push(format!("${head_var}: String!"));
        variables.insert(owner_var.clone(), serde_json::json!(owner));
        variables.insert(name_var.clone(), serde_json::json!(name));
        variables.insert(
            head_var.clone(),
            serde_json::json!(format!("update-{}", pr.input)),
        );
        let _ = writeln!(
            body,
            "  p{index}: repository(owner: ${owner_var}, name: ${name_var}) {{ pullRequests(states: OPEN, headRefName: ${head_var}, first: 1) {{ nodes {{ number }} }} }}",
        );
    }

    let query = format!("query({}) {{\n{body}}}\n", declarations.join(", "));
    (query, serde_json::Value::Object(variables))
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
        let Some(status) = cache.status_for(repo, node) else {
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
                // Finding E: a `Pending` comparison means a failed REST
                // compare could not be reconciled against a previous one --
                // publishing it would either show nothing new (acceptable)
                // or republish numbers already known to describe a
                // different `locked_rev`/upstream pair (not acceptable), so
                // it is skipped entirely rather than risk the latter.
                if status.comparison == ComparisonState::Complete {
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
        let (query, variables) = build_query(&refs, &[&pr]);

        assert!(query.contains("u0: repository(owner: $owner0, name: $name0)"));
        assert!(query.contains("ref(qualifiedName: $ref0)"));
        assert_eq!(variables.get("owner0").and_then(|v| v.as_str()), Some("o"));
        assert_eq!(
            variables.get("ref0").and_then(|v| v.as_str()),
            Some("refs/heads/release-1")
        );
        assert!(query.contains("u1: repository(owner: $owner1, name: $name1) { defaultBranchRef"));
        assert!(query.contains("u2: repository(owner: $owner2, name: $name2) { latestRelease"));
        assert!(query.contains("refPrefix: \"refs/tags/\""));
        assert!(query.contains("TAG_COMMIT_DATE"));
        assert!(query.contains(
            "p0: repository(owner: $prOwner0, name: $prName0) { pullRequests(states: OPEN, headRefName: $headRef0"
        ));
        assert_eq!(
            variables.get("prOwner0").and_then(|v| v.as_str()),
            Some("fredsystems")
        );
        assert_eq!(
            variables.get("headRef0").and_then(|v| v.as_str()),
            Some("update-niri")
        );
    }

    #[test]
    fn ref_and_input_names_are_passed_as_variables_not_interpolated() {
        // Finding F: neither a `ref` nor a PR lookup's input name may ever
        // be spliced into the query text -- not only because a `"` would
        // break out of a string literal, but because nothing stops a
        // crafted value from injecting additional fields or directives once
        // it is.
        let keys = [
            LookupKey {
                owner: "o".into(),
                repo: "evil".into(),
                kind: LookupKeyKind::Branch(Some("release\"next".into())),
            },
            LookupKey {
                owner: "o".into(),
                repo: "fine".into(),
                kind: LookupKeyKind::Branch(None),
            },
        ];
        let refs: Vec<&LookupKey> = keys.iter().collect();
        let pr = PrLookup {
            repo: "fredsystems/nixos".into(),
            input: "weird\"input".into(),
        };
        let (query, variables) = build_query(&refs, &[&pr]);

        assert!(
            !query.contains("release\"next"),
            "a ref must never be interpolated into the query text: {query}"
        );
        assert!(
            !query.contains("weird\"input"),
            "a PR lookup's input name must never be interpolated into the query text: {query}"
        );
        assert_eq!(
            variables.get("ref0").and_then(|v| v.as_str()),
            Some("refs/heads/release\"next"),
            "the raw value must still reach GitHub, just as a variable"
        );
        assert_eq!(
            variables.get("headRef0").and_then(|v| v.as_str()),
            Some("update-weird\"input")
        );
        // The second, unrelated alias must still be well-formed.
        assert!(query.contains("u1: repository(owner: $owner1, name: $name1) { defaultBranchRef"));
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
        nodes_by_repo.insert("o/r".to_owned(), vec![node.clone()]);
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

        let status = cache.status_for("o/r", &node).expect("status recorded");
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
        nodes_by_repo.insert("o/r".to_owned(), vec![node.clone()]);
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

        let status = cache.status_for("o/r", &node).expect("status recorded");
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
        nodes_by_repo.insert("o/r".to_owned(), vec![node.clone()]);
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

        let status = cache.status_for("o/r", &node).expect("status recorded");
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
            .status_for("o/r", &node)
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
        let second = cache.status_for("o/r", &node).expect("status recorded");
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

        assert!(cache.status_for("o/r", &node).is_none());
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
        nodes_by_repo.insert("o/r".to_owned(), vec![node.clone()]);
        let mut cache = UpstreamCache::default();
        cache
            .first_seen
            .observe_behind(&first_seen_identity("o/r", &node, "v1.0.0"), 1);
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
                .observe_behind(&first_seen_identity("o/r", &node, "v1.0.0"), 999),
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
            cache.status_for("o/r", &node).map(|s| s.freshness),
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
            cache.status_for("o/r", &node).map(|s| s.freshness),
            Some(Freshness::UpToDate),
            "a failed batch must not erase the previous status"
        );

        let (metrics, registry) = Metrics::new();
        record_metrics(&metrics, "o/r", &[node], &cache);
        let rendered = Publisher::new(metrics, registry).render();
        assert!(rendered.contains("github_flake_input_up_to_date"));
    }

    #[test]
    fn estimate_reserves_a_compare_for_every_eligible_branch_node() {
        // Finding B: a compare is reserved per branch *node*, not only for
        // nodes previously cached as `Behind`, and not deduplicated by
        // lookup key -- two nodes sharing one upstream lookup still each get
        // their own REST compare, since `apply_upstream` diffs each one's
        // own `locked_rev` independently.
        let up_to_date = branch_node("a", "o", "a", None, "abc");
        let behind = branch_node("b", "o", "shared", None, "def1");
        let behind_sibling = branch_node("b2", "o", "shared", None, "def2");
        let tag = tag_node("c", "o", "c", "v1.0.0", "ghi");

        let mut cache = UpstreamCache::default();
        cache.statuses.insert(
            ("o/r".to_owned(), "a".to_owned()),
            NodeStatus {
                upstream_oid: "abc".to_owned(),
                upstream_tag: None,
                upstream_committed_at: 1,
                freshness: Freshness::UpToDate,
                outdated_since: None,
                commits_behind: None,
                comparison: ComparisonState::Complete,
                identity: NodeIdentity::of(&up_to_date),
            },
        );
        cache.statuses.insert(
            ("o/r".to_owned(), "b".to_owned()),
            NodeStatus {
                upstream_oid: "new".to_owned(),
                upstream_tag: None,
                upstream_committed_at: 1,
                freshness: Freshness::Behind,
                outdated_since: Some(1),
                commits_behind: Some(1),
                comparison: ComparisonState::Complete,
                identity: NodeIdentity::of(&behind),
            },
        );

        let mut nodes_by_repo = HashMap::new();
        nodes_by_repo.insert(
            "o/r".to_owned(),
            vec![up_to_date, behind, behind_sibling, tag],
        );

        let (compare_count, batches) = cache.estimate_requests(&["o/r".to_owned()], &nodes_by_repo);

        assert_eq!(
            compare_count, 3,
            "every eligible branch node reserves a compare: the up-to-date \
             one, the behind one, and its sibling sharing the same lookup"
        );
        assert_eq!(
            batches, 1,
            "the two branch nodes sharing owner/repo/ref still collapse to one GraphQL alias"
        );
    }

    #[tokio::test]
    async fn a_not_found_alias_does_not_fail_the_rest_of_the_batch() {
        // Finding C: a deleted/inaccessible repository behind one alias
        // reports `NOT_FOUND` alongside a `null` for that alias, without
        // touching any other alias's healthy result.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": null,
                    "u1": {
                        "defaultBranchRef": {
                            "target": { "oid": "abc", "committedDate": "2026-01-01T00:00:00Z" }
                        }
                    }
                },
                "errors": [{
                    "type": "NOT_FOUND",
                    "path": ["u0"],
                    "message": "Could not resolve to a Repository with the name 'gone'."
                }]
            })))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let gone = branch_node("gone", "o", "gone-repo", None, "abc");
        let healthy = branch_node("healthy", "o", "healthy-repo", None, "abc");
        let mut nodes_by_repo = HashMap::new();
        nodes_by_repo.insert("o/r".to_owned(), vec![gone.clone(), healthy.clone()]);
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

        assert!(
            cache.status_for("o/r", &healthy).is_some(),
            "a NOT_FOUND for one alias must not discard another alias's healthy result"
        );
        assert!(
            cache.status_for("o/r", &gone).is_none(),
            "the NOT_FOUND alias itself resolves to no status"
        );
    }

    #[tokio::test]
    async fn an_explicit_null_invalidates_a_previously_cached_status() {
        // Finding I: unlike a failed batch (kept -- see
        // `budget_exhaustion_keeps_previous_values_published` above), a
        // repository that resolves to an explicit null this pass (deleted,
        // or `NOT_FOUND`) is confirmed gone, so a previously cached status
        // must not keep publishing.
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
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": { "u0": null },
                "errors": [{
                    "type": "NOT_FOUND",
                    "path": ["u0"],
                    "message": "Could not resolve to a Repository with the name 'now-gone'."
                }]
            })))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let node = branch_node("niri", "o", "r", None, "abc");
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
        assert!(
            cache.status_for("o/r", &node).is_some(),
            "resolves normally first"
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
        assert!(
            cache.status_for("o/r", &node).is_none(),
            "an explicit null this pass must invalidate the previously cached status"
        );
    }

    #[tokio::test]
    async fn a_ref_containing_a_quote_does_not_break_other_aliases_in_the_batch() {
        // Finding F, end to end: a ref with a `"` in it must not corrupt the
        // query text enough to take down every other alias in the batch.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "ref": {
                            "target": { "oid": "evil-oid", "committedDate": "2026-01-01T00:00:00Z" }
                        }
                    },
                    "u1": {
                        "defaultBranchRef": {
                            "target": { "oid": "abc", "committedDate": "2026-01-01T00:00:00Z" }
                        }
                    }
                }
            })))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let evil = branch_node("evil", "o", "r", Some("release\"next"), "evil-oid");
        let safe = branch_node("safe", "o", "r2", None, "abc");
        let mut nodes_by_repo = HashMap::new();
        nodes_by_repo.insert("o/r".to_owned(), vec![evil.clone()]);
        nodes_by_repo.insert("o/r2".to_owned(), vec![safe.clone()]);
        let mut cache = UpstreamCache::default();
        let dir = tempfile::tempdir().expect("tempdir");

        run_pass(
            &client,
            &["o/r".to_owned(), "o/r2".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            1,
        )
        .await;

        assert_eq!(
            cache.status_for("o/r", &evil).map(|s| s.freshness),
            Some(Freshness::UpToDate),
            "a ref containing a quote must not break its own alias"
        );
        assert_eq!(
            cache.status_for("o/r2", &safe).map(|s| s.freshness),
            Some(Freshness::UpToDate),
            "nor any other alias in the same batch"
        );
    }

    #[tokio::test]
    async fn a_node_repointed_to_a_channel_tarball_drops_its_cached_status() {
        // Finding D: a node's shape can change at the same `(repo, path)`
        // key -- here a `github` branch becomes a channel tarball, which has
        // no upstream lookup at all. The status computed for the old shape
        // must not keep publishing a freshness for a node that no longer has
        // one.
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
            .mount(&server)
            .await;

        let client = client_for(&server);
        let branch = branch_node("nixpkgs", "nixos", "nixpkgs", None, "abc");
        let mut nodes_by_repo = HashMap::new();
        nodes_by_repo.insert("o/r".to_owned(), vec![branch.clone()]);
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
        assert!(
            cache.status_for("o/r", &branch).is_some(),
            "the branch resolves normally first"
        );

        let tarball = TrackedNode {
            input: "nixpkgs".to_owned(),
            path: "nixpkgs".to_owned(),
            kind: NodeKind::ChannelTarball,
            reference: None,
            channel: Some("nixpkgs-unstable".to_owned()),
            locked_rev: None,
            locked_last_modified: Some(1),
            owner: None,
            repo: None,
        };
        nodes_by_repo.insert("o/r".to_owned(), vec![tarball.clone()]);

        run_pass(
            &client,
            &["o/r".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            2,
        )
        .await;

        assert!(
            cache.status_for("o/r", &tarball).is_none(),
            "a node with no applicable lookup any more must not keep publishing a stale status"
        );
    }

    #[tokio::test]
    async fn a_changed_ref_drops_the_old_status_at_publish_time() {
        // Finding D: if a `flake.lock` change repoints a node to a different
        // `ref` on a cycle whose upstream batch itself fails outright, the
        // stale status computed for the *old* ref must not keep being read
        // back for the new node, even though it is still sitting in the
        // cache under the same `(repo, path)` key waiting for a successful
        // pass to replace it.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "ref": {
                            "target": { "oid": "main-oid", "committedDate": "2026-01-01T00:00:00Z" }
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
        let on_main = branch_node("niri", "o", "r", Some("main"), "main-oid");
        let mut nodes_by_repo = HashMap::new();
        nodes_by_repo.insert("o/r".to_owned(), vec![on_main.clone()]);
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
        assert!(cache.status_for("o/r", &on_main).is_some());

        let on_release = branch_node("niri", "o", "r", Some("release"), "release-oid");
        nodes_by_repo.insert("o/r".to_owned(), vec![on_release.clone()]);
        run_pass(
            &client,
            &["o/r".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            2,
        )
        .await;

        assert!(
            cache.status_for("o/r", &on_release).is_none(),
            "a status computed for a different ref must not be published for the new one"
        );
    }

    #[test]
    fn repointing_an_input_at_another_repository_starts_a_new_first_seen_key() {
        // Same configured repo, path and locked tag, different source: the
        // previous source's outdated-since clock must not carry over.
        let before = tag_node("opencode", "o", "a", "v1", "oid");
        let after = tag_node("opencode", "o", "b", "v1", "oid");
        assert_ne!(
            first_seen_identity("o/r", &before, "v1"),
            first_seen_identity("o/r", &after, "v1")
        );
    }

    #[tokio::test]
    async fn a_tag_status_does_not_lend_its_comparison_to_a_branch_after_a_failed_compare() {
        // Same path, same locked oid, same upstream oid -- but pass one saw a
        // Tag node and pass two sees a Branch node whose compare fails. The
        // tag's status must not be reused as if a branch comparison had
        // succeeded: the branch's comparison is Pending.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "latestRelease": {
                            "tagName": "v2",
                            "tagCommit": { "oid": "head-oid", "committedDate": "2026-01-01T00:00:00Z" }
                        },
                        "defaultBranchRef": {
                            "target": { "oid": "head-oid", "committedDate": "2026-01-01T00:00:00Z" }
                        }
                    }
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/o/niri-flake/compare/locked...head-oid"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cache = UpstreamCache::default();

        let tag = tag_node("niri", "o", "niri-flake", "v1", "locked");
        let mut nodes_by_repo = HashMap::new();
        nodes_by_repo.insert("o/r".to_owned(), vec![tag]);
        run_pass(
            &client,
            &["o/r".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            1,
        )
        .await;

        let branch = branch_node("niri", "o", "niri-flake", None, "locked");
        nodes_by_repo.insert("o/r".to_owned(), vec![branch.clone()]);
        run_pass(
            &client,
            &["o/r".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            2,
        )
        .await;

        let status = cache
            .status_for("o/r", &branch)
            .expect("branch status recorded");
        assert_eq!(status.comparison, ComparisonState::Pending);
        assert_eq!(status.commits_behind, None);
        assert_eq!(status.outdated_since, None);
    }

    #[tokio::test]
    async fn a_failed_compare_keeps_the_previous_comparison_when_nothing_changed() {
        // Finding E: if the prior status was computed for the same
        // `locked_rev`/upstream `oid` pair, nothing about the comparison has
        // actually changed, so a failed compare this pass must not blank it.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "defaultBranchRef": {
                            "target": { "oid": "head-oid", "committedDate": "2026-01-01T00:00:00Z" }
                        }
                    }
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/o/niri-flake/compare/locked...head-oid"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ahead_by": 2,
                "commits": [
                    { "commit": { "committer": { "date": "2025-12-01T00:00:00Z" } } },
                    { "commit": { "committer": { "date": "2025-12-15T00:00:00Z" } } },
                ],
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/o/niri-flake/compare/locked...head-oid"))
            .respond_with(ResponseTemplate::new(403))
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
            .status_for("o/r", &node)
            .expect("status recorded")
            .clone();
        assert_eq!(first.comparison, ComparisonState::Complete);
        assert_eq!(first.commits_behind, Some(2));

        run_pass(
            &client,
            &["o/r".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            2,
        )
        .await;
        let second = cache.status_for("o/r", &node).expect("status recorded");
        assert_eq!(
            second.comparison,
            ComparisonState::Complete,
            "the same locked_rev/upstream pair means the previous comparison still applies"
        );
        assert_eq!(second.commits_behind, Some(2));
        assert_eq!(second.outdated_since, first.outdated_since);
    }

    #[tokio::test]
    async fn a_failed_compare_marks_pending_when_the_compare_parameters_changed() {
        // Finding E: once the upstream oid has moved, the previous
        // comparison no longer describes the new pair, so a failed compare
        // this pass must be marked `Pending` rather than published as
        // current or silently left stale for a whole `flake_interval`.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "defaultBranchRef": {
                            "target": { "oid": "head-oid-1", "committedDate": "2026-01-01T00:00:00Z" }
                        }
                    }
                }
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/o/niri-flake/compare/locked...head-oid-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ahead_by": 2,
                "commits": [
                    { "commit": { "committer": { "date": "2025-12-01T00:00:00Z" } } },
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
        assert_eq!(
            cache
                .status_for("o/r", &node)
                .expect("status recorded")
                .commits_behind,
            Some(2)
        );

        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "defaultBranchRef": {
                            "target": { "oid": "head-oid-2", "committedDate": "2026-01-02T00:00:00Z" }
                        }
                    }
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/o/niri-flake/compare/locked...head-oid-2"))
            .respond_with(ResponseTemplate::new(403))
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
        let second = cache.status_for("o/r", &node).expect("status recorded");
        assert_eq!(second.freshness, Freshness::Behind);
        assert_eq!(
            second.comparison,
            ComparisonState::Pending,
            "the upstream oid moved, so the previous comparison no longer applies"
        );
        assert_eq!(second.commits_behind, None);
    }

    #[tokio::test]
    async fn a_tag_whose_name_matches_but_commit_moved_reads_behind() {
        // Finding J: a locked tag name equal to the newest tag is not enough
        // on its own -- the tag can have been force-moved to a different
        // commit since the lock was taken.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "latestRelease": {
                            "tagName": "v1.0.0",
                            "tagCommit": { "oid": "moved-oid", "committedDate": "2026-01-01T00:00:00Z" }
                        }
                    }
                }
            })))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let node = tag_node("opencode", "sst", "opencode", "v1.0.0", "original-oid");
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

        let status = cache.status_for("o/r", &node).expect("status recorded");
        assert_eq!(
            status.freshness,
            Freshness::Behind,
            "a matching tag name is not enough if the commit it points to moved"
        );
    }

    #[tokio::test]
    async fn a_tag_behind_across_two_newer_releases_keeps_the_same_outdated_since() {
        // Finding K: the clock is keyed on the node's own locked tag, not
        // the upstream one, so a second newer release landing while the
        // lock is unchanged must not restart it.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "latestRelease": {
                            "tagName": "v2.0.0",
                            "tagCommit": { "oid": "v2-oid", "committedDate": "2026-02-01T00:00:00Z" }
                        }
                    }
                }
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        let client = client_for(&server);
        let node = tag_node("opencode", "sst", "opencode", "v1.0.0", "v1-oid");
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
            100,
        )
        .await;
        let first_since = cache
            .status_for("o/r", &node)
            .expect("status recorded")
            .outdated_since;
        assert_eq!(first_since, Some(100));

        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "u0": {
                        "latestRelease": {
                            "tagName": "v3.0.0",
                            "tagCommit": { "oid": "v3-oid", "committedDate": "2026-03-01T00:00:00Z" }
                        }
                    }
                }
            })))
            .mount(&server)
            .await;

        run_pass(
            &client,
            &["o/r".to_owned()],
            &nodes_by_repo,
            &mut cache,
            dir.path(),
            200,
        )
        .await;
        let second_since = cache
            .status_for("o/r", &node)
            .expect("status recorded")
            .outdated_since;
        assert_eq!(
            second_since, first_since,
            "a newer release superseding the one already behind must not reset the clock"
        );
    }

    #[test]
    fn example_metrics_output_covers_every_new_family() {
        let (metrics, registry) = Metrics::new();
        let mut cache = UpstreamCache::default();
        let node = branch_node("niri", "epireyn", "niri-flake", None, "oldoid");
        cache.statuses.insert(
            ("fredsystems/nixos".to_owned(), "niri".to_owned()),
            NodeStatus {
                upstream_oid: "newoid".to_owned(),
                upstream_tag: None,
                upstream_committed_at: 1_700_000_100,
                freshness: Freshness::Behind,
                outdated_since: Some(1_700_000_000),
                commits_behind: Some(3),
                comparison: ComparisonState::Complete,
                identity: NodeIdentity::of(&node),
            },
        );
        cache
            .open_prs
            .insert(("fredsystems/nixos".to_owned(), "niri".to_owned()), 42);

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
