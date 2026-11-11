//! Flake input tracking.
//!
//! Parses each configured repository's `flake.lock`, resolves every tracked
//! input's upstream and open update pull request, reads the flake-watch
//! Action's impact status file, and -- for a channel-tarball node -- reads
//! its channel's status from `prometheus.nixos.org`.
//!
//! Split by nature rather than by file-per-type: [`lock`] is pure parsing
//! with no I/O; [`upstream`] resolves GitHub heads, tags, and update PRs and
//! derives [`Freshness`]; [`status`] reads the watch Action's status file;
//! [`channels`] fetches and caches NixOS channel status and feeds a
//! channel-tarball node's freshness into the same families [`upstream`]
//! renders; [`first_seen`] is the generic persisted "first observed behind"
//! clock a tag-pinned node or a channel-tarball node needs because neither
//! has a compare to measure its oldest missing change from; and [`collect`]
//! is the per-cycle orchestration that ties all of the above together,
//! fetches, and publishes.

pub mod channels;
pub mod collect;
pub mod first_seen;
pub mod lock;
pub mod status;
pub mod upstream;

pub use channels::NixosClient;
pub use collect::{FlakeCache, record_all_metrics, record_metrics, run_pass};
pub use lock::{LockError, NodeKind, TrackedNode};
pub use upstream::Freshness;
