//! Flake input tracking.
//!
//! Parses each configured repository's `flake.lock` and publishes the one
//! metric derivable from the lock alone: when each tracked input was last
//! locked. Comparing against upstream, detecting update pull requests, and
//! NixOS channel status are later work -- see `PLAN_03_FLAKE_INPUT_TRACKING.md`
//! subtasks 3.5 and 3.6 in the `nixos` repository.
//!
//! Split from [`collect`] because the two have different natures: [`lock`] is
//! pure parsing with no I/O, while [`collect`] fetches, caches across cycles,
//! and publishes.

pub mod collect;
pub mod lock;

pub use collect::{FlakeCache, record_metrics, run_pass};
pub use lock::{LockError, NodeKind, TrackedNode};
