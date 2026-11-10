//! Pure parsing of `flake.lock` (format version 7) into the tracked node set.
//!
//! This module never makes a request; it only turns a lock file's bytes into
//! [`TrackedNode`]s. Fetching the file and acting on the result live in
//! [`super::collect`].

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// The only `flake.lock` schema version this parser understands.
///
/// Nix has held this steady for years; an actual bump is rare enough that
/// failing loudly and extending the parser is better than guessing a newer
/// schema parses the same way.
const SUPPORTED_VERSION: u64 = 7;

/// What a tracked node is locked as, and therefore how its upstream must be
/// found.
///
/// Deliberately not a pair of bools (is it a tag? is it a channel?): the
/// three locked shapes this crate understands are mutually exclusive, and a
/// node's upstream-lookup strategy (3.5) switches on exactly this value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeKind {
    /// A GitHub `ref` that is a branch, or no `ref` at all (the default
    /// branch). Its upstream is the branch head.
    Branch,
    /// A GitHub `ref` that looks like a release tag (`v2.2.7`, `26.05`). Its
    /// upstream is the newest release tag, never the default branch --
    /// comparing a tag-pinned input against a branch head would read as
    /// permanently outdated.
    Tag,
    /// A `tarball` fetch from `channels.nixos.org`. Its upstream is that
    /// channel's `channel_revision` (NixOS Prometheus, see 3.6).
    ChannelTarball,
    /// Any other locked shape: `path`, `git`, a tarball from somewhere other
    /// than `channels.nixos.org`, or a type this parser does not yet know.
    /// Reported, never guessed at.
    Untracked,
}

/// One flake input this crate watches: a root input of `flake.lock`, or a
/// root input's own `nixpkgs` when that input locks it independently rather
/// than following ours.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrackedNode {
    /// The root input that owns this node -- itself for a root input, or the
    /// root input one level up for an owned nixpkgs (`niri` for
    /// `niri/nixpkgs`).
    pub input: String,
    /// The full input path, e.g. `"niri"` or `"niri/nixpkgs"`.
    pub path: String,
    pub kind: NodeKind,
    /// The `ref` this node is locked to. `None` for the default branch and
    /// for anything that is not a `github` lock.
    pub reference: Option<String>,
    /// The NixOS channel this node tracks, if any -- a `nixos-*` /
    /// `nixpkgs-*` branch of `nixos/nixpkgs`, or a `channels.nixos.org`
    /// tarball's channel path segment.
    pub channel: Option<String>,
    /// `locked.rev`, for `github` nodes. `None` for a tarball, which has no
    /// revision, only a URL and a `lastModified`.
    pub locked_rev: Option<String>,
    pub locked_last_modified: Option<i64>,
}

#[derive(Debug, thiserror::Error)]
pub enum LockError {
    #[error("failed to parse flake.lock: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("flake.lock root node `{0}` is missing")]
    MissingRoot(String),
    #[error("unsupported flake.lock version {0}; expected {SUPPORTED_VERSION}")]
    UnsupportedVersion(u64),
}

/// Parses `flake.lock`'s JSON and derives the tracked node set.
///
/// Tracked, per the root's `inputs`:
///
/// * A root input locked to a **node name** (a plain string) is tracked at
///   its own key as the path.
/// * A root input locked through a **follows path** (an array) is skipped: it
///   resolves to another input's node rather than owning one, so there is
///   nothing here to watch independently.
/// * For each tracked root input whose own `inputs.nixpkgs` is itself a node
///   name rather than a follows path -- i.e. it owns its nixpkgs rather than
///   following ours -- that nested node is also tracked, at `<key>/nixpkgs`.
///   Only this one level of nesting is walked.
///
/// # Errors
/// Returns [`LockError`] for unparsable JSON, a missing root node, or a
/// version other than the one this parser understands. Never panics.
pub fn tracked_nodes(raw: &str) -> Result<Vec<TrackedNode>, LockError> {
    let lock: RawLock = serde_json::from_str(raw)?;
    if lock.version != SUPPORTED_VERSION {
        return Err(LockError::UnsupportedVersion(lock.version));
    }
    let root = lock
        .nodes
        .get(&lock.root)
        .ok_or_else(|| LockError::MissingRoot(lock.root.clone()))?;

    let mut tracked = Vec::new();
    for (key, reference) in &root.inputs {
        let InputRef::Node(node_name) = reference else {
            // A follows path: this input resolves to another node's input
            // rather than owning one, so there is nothing new to watch.
            continue;
        };
        let Some(node) = lock.nodes.get(node_name) else {
            // A dangling reference. Not expected from a real lock file, but
            // nothing sane can be reported for it.
            continue;
        };
        tracked.push(classify(key, key, node));

        if let Some(InputRef::Node(nested_name)) = node.inputs.get("nixpkgs")
            && let Some(nested_node) = lock.nodes.get(nested_name)
        {
            tracked.push(classify(key, &format!("{key}/nixpkgs"), nested_node));
        }
    }

    tracked.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(tracked)
}

/// Classifies one node and builds its [`TrackedNode`].
///
/// `input` is the owning root input's key; `path` is this node's own full
/// path (equal to `input` for a root node, `<input>/nixpkgs` for a nested
/// one).
fn classify(input: &str, path: &str, node: &RawNode) -> TrackedNode {
    // `original` reflects what the flake *asked for*; `locked` is the
    // resolved result. A node's `original` is normally present, but falling
    // back to `locked` keeps this from panicking on the ones that are not.
    let source = node.original.as_ref().or(node.locked.as_ref());
    let kind_str = source.and_then(|r| r.kind.as_deref());

    let (kind, reference) = match kind_str {
        Some("github") => match source.and_then(|r| r.git_ref.clone()) {
            None => (NodeKind::Branch, None),
            Some(r) if is_tag_ref(&r) => (NodeKind::Tag, Some(r)),
            Some(r) => (NodeKind::Branch, Some(r)),
        },
        Some("tarball")
            if source
                .and_then(|r| r.url.as_deref())
                .and_then(channel_of_url)
                .is_some() =>
        {
            (NodeKind::ChannelTarball, None)
        }
        _ => (NodeKind::Untracked, None),
    };

    let channel = match kind {
        NodeKind::Branch => branch_channel(source, reference.as_deref()),
        NodeKind::ChannelTarball => source
            .and_then(|r| r.url.as_deref())
            .and_then(channel_of_url),
        NodeKind::Tag | NodeKind::Untracked => None,
    };

    TrackedNode {
        input: input.to_owned(),
        path: path.to_owned(),
        kind,
        reference,
        channel,
        locked_rev: node.locked.as_ref().and_then(|l| l.rev.clone()),
        locked_last_modified: node.locked.as_ref().and_then(|l| l.last_modified),
    }
}

/// The NixOS channel label for a `Branch` node, if it tracks one.
///
/// Only `nixos/nixpkgs` (case-insensitive owner) branches named `nixos-*` or
/// `nixpkgs-*` count -- those are the channel names `channels.nixos.org` and
/// `prometheus.nixos.org` use, e.g. `nixos-unstable`, `nixos-26.05`.
fn branch_channel(source: Option<&RawRef>, reference: Option<&str>) -> Option<String> {
    let source = source?;
    let owner = source.owner.as_deref()?;
    let repo = source.repo.as_deref()?;
    let reference = reference?;
    let is_nixpkgs = owner.eq_ignore_ascii_case("nixos") && repo.eq_ignore_ascii_case("nixpkgs");
    let is_channel_ref = reference.starts_with("nixos-") || reference.starts_with("nixpkgs-");
    (is_nixpkgs && is_channel_ref).then(|| reference.to_owned())
}

/// Whether `reference` looks like a release tag: an optional leading `v`,
/// then one or more dot-separated runs of digits (`v2.2.7`, `26.05`).
///
/// Anything else -- a plain branch name, `release-26.05`, `nixos-unstable` --
/// is a branch. Note that this means an all-numeric branch name would
/// misclassify as a tag; none of the tracked fleet's inputs use one.
fn is_tag_ref(reference: &str) -> bool {
    let digits = reference.strip_prefix('v').unwrap_or(reference);
    !digits.is_empty()
        && digits
            .split('.')
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
}

/// Extracts the channel name from a `channels.nixos.org` tarball URL, e.g.
/// `https://channels.nixos.org/nixpkgs-unstable/nixexprs.tar.zst` ->
/// `"nixpkgs-unstable"`.
///
/// Returns `None` for any other host, which is also what gates
/// [`NodeKind::ChannelTarball`] classification: a tarball from elsewhere is
/// [`NodeKind::Untracked`] rather than guessed at.
fn channel_of_url(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://")?.1;
    let (host, rest) = after_scheme.split_once('/')?;
    if !host.eq_ignore_ascii_case("channels.nixos.org") {
        return None;
    }
    let first_segment = rest.split('/').next()?;
    (!first_segment.is_empty()).then(|| first_segment.to_owned())
}

#[derive(Debug, Deserialize)]
struct RawLock {
    version: u64,
    root: String,
    nodes: HashMap<String, RawNode>,
}

#[derive(Debug, Default, Deserialize)]
struct RawNode {
    #[serde(default)]
    inputs: HashMap<String, InputRef>,
    original: Option<RawRef>,
    locked: Option<RawRef>,
}

/// One entry of a node's `inputs` map: either the name of another node in
/// `nodes` (this input is locked independently), or a follows path (an array
/// of node names to walk from the root) when it instead resolves to another
/// input's node.
///
/// Nothing here ever reads a follows path's contents -- whether it is present
/// at all is what distinguishes "owns its own lock" from "resolves
/// elsewhere" -- so [`Self::Follows`] carries no data, and `InputRef`
/// implements [`Deserialize`] by hand rather than deriving an untagged enum
/// over a field that would otherwise never be read.
#[derive(Debug, Clone)]
enum InputRef {
    Follows,
    Node(String),
}

impl<'de> Deserialize<'de> for InputRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match serde_json::Value::deserialize(deserializer)? {
            serde_json::Value::String(name) => Ok(Self::Node(name)),
            serde_json::Value::Array(_) => Ok(Self::Follows),
            other => Err(serde::de::Error::custom(format!(
                "expected a node name or a follows path, got {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawRef {
    #[serde(rename = "type")]
    kind: Option<String>,
    owner: Option<String>,
    repo: Option<String>,
    #[serde(rename = "ref")]
    git_ref: Option<String>,
    url: Option<String>,
    rev: Option<String>,
    #[serde(rename = "lastModified", default)]
    last_modified: Option<i64>,
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

    /// Builds a minimal `flake.lock` JSON document from a map of root input
    /// name -> node JSON, plus any extra (non-root-input) nodes needed to
    /// resolve nesting.
    fn lock_json(root_inputs: &serde_json::Value, extra_nodes: &serde_json::Value) -> String {
        let mut nodes = serde_json::Map::new();
        nodes.insert(
            "root".to_owned(),
            serde_json::json!({ "inputs": root_inputs }),
        );
        if let serde_json::Value::Object(map) = extra_nodes {
            for (k, v) in map {
                nodes.insert(k.clone(), v.clone());
            }
        }
        serde_json::json!({
            "version": 7,
            "root": "root",
            "nodes": nodes,
        })
        .to_string()
    }

    fn github_node(
        owner: &str,
        repo: &str,
        r#ref: Option<&str>,
        rev: &str,
        modified: i64,
    ) -> serde_json::Value {
        let mut original = serde_json::json!({ "type": "github", "owner": owner, "repo": repo });
        if let Some(r) = r#ref {
            original["ref"] = serde_json::json!(r);
        }
        serde_json::json!({
            "inputs": {},
            "original": original,
            "locked": {
                "type": "github",
                "owner": owner,
                "repo": repo,
                "rev": rev,
                "lastModified": modified,
            },
        })
    }

    #[test]
    fn an_array_root_input_is_a_follows_path_and_is_skipped() {
        let raw = lock_json(
            &serde_json::json!({ "nixpkgs": ["colmena", "nixpkgs"] }),
            &serde_json::json!({}),
        );
        let tracked = tracked_nodes(&raw).expect("parses");
        assert!(
            tracked.is_empty(),
            "a follows path resolves to another input's node, not one of its own"
        );
    }

    #[test]
    fn a_nested_nixpkgs_follows_path_is_skipped() {
        let raw = lock_json(
            &serde_json::json!({ "niri": "niri" }),
            &serde_json::json!({
                "niri": {
                    "inputs": { "nixpkgs": ["nixpkgs"] },
                    "original": { "type": "github", "owner": "epireyn", "repo": "niri-flake" },
                    "locked": {
                        "type": "github", "owner": "epireyn", "repo": "niri-flake",
                        "rev": "abc", "lastModified": 1,
                    },
                },
            }),
        );
        let tracked = tracked_nodes(&raw).expect("parses");
        assert_eq!(tracked.len(), 1, "only the root input itself is tracked");
        assert_eq!(tracked[0].path, "niri");
    }

    #[test]
    fn a_nested_owned_nixpkgs_is_tracked_one_level_deep() {
        let raw = lock_json(
            &serde_json::json!({ "niri": "niri" }),
            &serde_json::json!({
                "niri": {
                    "inputs": { "nixpkgs": "niri-nixpkgs" },
                    "original": { "type": "github", "owner": "epireyn", "repo": "niri-flake" },
                    "locked": {
                        "type": "github", "owner": "epireyn", "repo": "niri-flake",
                        "rev": "abc", "lastModified": 1,
                    },
                },
                "niri-nixpkgs": github_node("NixOS", "nixpkgs", Some("nixos-unstable"), "def", 2),
            }),
        );
        let tracked = tracked_nodes(&raw).expect("parses");
        assert_eq!(tracked.len(), 2);
        let nested = tracked
            .iter()
            .find(|n| n.path == "niri/nixpkgs")
            .expect("the owned nixpkgs must be tracked");
        assert_eq!(
            nested.input, "niri",
            "nested nodes are owned by the root input"
        );
        assert_eq!(nested.kind, NodeKind::Branch);
        assert_eq!(nested.channel.as_deref(), Some("nixos-unstable"));
    }

    #[test]
    fn a_missing_ref_is_the_default_branch() {
        let raw = lock_json(
            &serde_json::json!({ "colmena": "colmena" }),
            &serde_json::json!({ "colmena": github_node("zhaofengli", "colmena", None, "abc", 1) }),
        );
        let tracked = tracked_nodes(&raw).expect("parses");
        assert_eq!(tracked.len(), 1);
        assert_eq!(tracked[0].kind, NodeKind::Branch);
        assert_eq!(tracked[0].reference, None);
    }

    #[test]
    fn a_dotted_numeric_ref_is_a_tag() {
        for r in ["v2.2.7", "v0.8.0", "26.05"] {
            let raw = lock_json(
                &serde_json::json!({ "input": "input" }),
                &serde_json::json!({ "input": github_node("o", "r", Some(r), "abc", 1) }),
            );
            let tracked = tracked_nodes(&raw).expect("parses");
            assert_eq!(tracked[0].kind, NodeKind::Tag, "{r} must classify as a tag");
            assert_eq!(tracked[0].reference.as_deref(), Some(r));
        }
    }

    #[test]
    fn a_named_ref_is_a_branch() {
        for r in ["release-26.05", "main", "nixos-unstable", "nixos-26.05"] {
            let raw = lock_json(
                &serde_json::json!({ "input": "input" }),
                &serde_json::json!({ "input": github_node("o", "r", Some(r), "abc", 1) }),
            );
            let tracked = tracked_nodes(&raw).expect("parses");
            assert_eq!(
                tracked[0].kind,
                NodeKind::Branch,
                "{r} must classify as a branch, not a tag"
            );
        }
    }

    #[test]
    fn a_nixos_nixpkgs_channel_branch_carries_its_channel_label() {
        let raw = lock_json(
            &serde_json::json!({ "nixpkgs": "nixpkgs" }),
            &serde_json::json!({
                "nixpkgs": github_node("nixos", "nixpkgs", Some("nixos-unstable"), "abc", 1),
            }),
        );
        let tracked = tracked_nodes(&raw).expect("parses");
        assert_eq!(tracked[0].channel.as_deref(), Some("nixos-unstable"));
    }

    #[test]
    fn channel_matching_is_case_insensitive_on_the_owner() {
        let raw = lock_json(
            &serde_json::json!({ "nixpkgs": "nixpkgs" }),
            &serde_json::json!({
                "nixpkgs": github_node("NixOS", "nixpkgs", Some("nixos-26.05"), "abc", 1),
            }),
        );
        let tracked = tracked_nodes(&raw).expect("parses");
        assert_eq!(tracked[0].channel.as_deref(), Some("nixos-26.05"));
    }

    #[test]
    fn a_branch_on_a_different_repo_has_no_channel_even_with_a_matching_ref_prefix() {
        let raw = lock_json(
            &serde_json::json!({ "input": "input" }),
            &serde_json::json!({ "input": github_node("someone", "else", Some("nixos-unstable"), "abc", 1) }),
        );
        let tracked = tracked_nodes(&raw).expect("parses");
        assert_eq!(tracked[0].channel, None);
    }

    #[test]
    fn the_catppuccin_nixpkgs_channel_tarball_fixture_is_classified_correctly() {
        // This repo's `fredsystems/nixos`'s catppuccin nixpkgs node, verbatim.
        let raw = lock_json(
            &serde_json::json!({ "node": "node" }),
            &serde_json::json!({
                "node": {
                    "inputs": {},
                    "original": {
                        "type": "tarball",
                        "url": "https://channels.nixos.org/nixpkgs-unstable/nixexprs.tar.zst",
                    },
                    "locked": {
                        "type": "tarball",
                        "url": "https://releases.nixos.org/nixpkgs/nixpkgs-26.11pre1104102.e62746b0b472/nixexprs.tar.zst",
                        "lastModified": 1_794_000_000_i64,
                    },
                },
            }),
        );
        let tracked = tracked_nodes(&raw).expect("parses");
        assert_eq!(tracked.len(), 1);
        assert_eq!(tracked[0].kind, NodeKind::ChannelTarball);
        assert_eq!(tracked[0].channel.as_deref(), Some("nixpkgs-unstable"));
        assert_eq!(tracked[0].locked_last_modified, Some(1_794_000_000));
        assert_eq!(
            tracked[0].locked_rev, None,
            "a tarball has no rev, only a URL and a modification time"
        );
    }

    #[test]
    fn a_tarball_from_elsewhere_is_untracked() {
        let raw = lock_json(
            &serde_json::json!({ "node": "node" }),
            &serde_json::json!({
                "node": {
                    "inputs": {},
                    "original": {
                        "type": "tarball",
                        "url": "https://example.com/nixpkgs/nixexprs.tar.zst",
                    },
                    "locked": {
                        "type": "tarball",
                        "url": "https://example.com/nixpkgs/nixexprs.tar.zst",
                        "lastModified": 1,
                    },
                },
            }),
        );
        let tracked = tracked_nodes(&raw).expect("parses");
        assert_eq!(tracked[0].kind, NodeKind::Untracked);
        assert_eq!(tracked[0].channel, None);
    }

    #[test]
    fn an_unknown_lock_type_is_untracked() {
        let raw = lock_json(
            &serde_json::json!({ "node": "node" }),
            &serde_json::json!({
                "node": {
                    "inputs": {},
                    "original": { "type": "path", "path": "/nix/store/x" },
                    "locked": { "type": "path", "path": "/nix/store/x" },
                },
            }),
        );
        let tracked = tracked_nodes(&raw).expect("parses");
        assert_eq!(tracked.len(), 1);
        assert_eq!(tracked[0].kind, NodeKind::Untracked);
        assert_eq!(tracked[0].locked_rev, None);
        assert_eq!(tracked[0].locked_last_modified, None);
    }

    #[test]
    fn invalid_json_is_an_error_not_a_panic() {
        assert!(matches!(
            tracked_nodes("not json"),
            Err(LockError::InvalidJson(_))
        ));
    }

    #[test]
    fn a_missing_root_node_is_an_error() {
        let raw = serde_json::json!({
            "version": 7,
            "root": "root",
            "nodes": {},
        })
        .to_string();
        assert!(matches!(
            tracked_nodes(&raw),
            Err(LockError::MissingRoot(name)) if name == "root"
        ));
    }

    #[test]
    fn an_unsupported_version_is_an_error() {
        let raw = serde_json::json!({
            "version": 6,
            "root": "root",
            "nodes": { "root": { "inputs": {} } },
        })
        .to_string();
        assert!(matches!(
            tracked_nodes(&raw),
            Err(LockError::UnsupportedVersion(6))
        ));
    }

    #[test]
    fn the_real_nixos_flake_lock_resolves_the_expected_tracked_set() {
        let raw = include_str!("testdata/nixos-flake.lock");
        let tracked = tracked_nodes(raw).expect("a real flake.lock must parse");

        let by_path = |path: &str| tracked.iter().find(|n| n.path == path);

        let nixpkgs = by_path("nixpkgs").expect("nixpkgs root input");
        assert_eq!(nixpkgs.kind, NodeKind::Branch);
        assert_eq!(nixpkgs.channel.as_deref(), Some("nixos-unstable"));

        let nixpkgs_stable = by_path("nixpkgs-stable").expect("nixpkgs-stable root input");
        assert_eq!(nixpkgs_stable.channel.as_deref(), Some("nixos-26.05"));

        assert_eq!(
            by_path("opencode").expect("opencode root input").kind,
            NodeKind::Tag
        );
        assert_eq!(
            by_path("nix-flatpak").expect("nix-flatpak root input").kind,
            NodeKind::Tag
        );

        let catppuccin_nixpkgs =
            by_path("catppuccin/nixpkgs").expect("catppuccin owns its nixpkgs");
        assert_eq!(catppuccin_nixpkgs.kind, NodeKind::ChannelTarball);
        assert_eq!(catppuccin_nixpkgs.input, "catppuccin");

        assert!(
            by_path("niri/nixpkgs").is_some(),
            "niri owns its nixpkgs and it must be tracked"
        );

        assert!(
            by_path("nixos-needsreboot/nixpkgs").is_none(),
            "nixos-needsreboot follows our nixpkgs and must not be tracked separately"
        );
    }
}
