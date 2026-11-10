//! Flake input tracking.
//!
//! Parses each configured repository's `flake.lock`, resolves every tracked
//! input's upstream and open update pull request, and reads the flake-watch
//! Action's impact status file. NixOS channel status for channel-tarball
//! nodes is later work -- see `PLAN_03_FLAKE_INPUT_TRACKING.md` subtask 3.6
//! in the `nixos` repository.
//!
//! Split by nature rather than by file-per-type: [`lock`] is pure parsing
//! with no I/O; [`upstream`] resolves GitHub heads, tags, and update PRs and
//! derives [`Freshness`]; [`status`] reads the watch Action's status file;
//! [`first_seen`] is the generic persisted "first observed behind" clock a
//! tag-pinned node (and, later, a channel tarball) needs because it has no
//! compare to measure its oldest missing change from; and [`collect`] is the
//! per-cycle orchestration that ties all of the above together, fetches, and
//! publishes.

pub mod collect;
pub mod first_seen;
pub mod lock;
pub mod status;
pub mod upstream;

pub use collect::{FlakeCache, record_all_metrics, record_metrics, run_pass};
pub use lock::{LockError, NodeKind, TrackedNode};
pub use upstream::Freshness;
