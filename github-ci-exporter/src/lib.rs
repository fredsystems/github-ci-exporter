//! A Prometheus exporter for GitHub issues, pull requests, and Actions CI
//! state across whole repository owners — organisations or personal accounts.
//!
//! # Design constraints
//!
//! * **The search API is unusable.** It permits 30 requests/hour, which
//!   cannot sustain polling. All data therefore comes from GraphQL
//!   (repository discovery, issues, pull requests) and REST (Actions).
//! * **Commit status rollups are unreliable here.** Most workflows in the
//!   target organisations run on `schedule` or `workflow_dispatch` and are
//!   never attached to the head commit, so `statusCheckRollup` is null for
//!   the overwhelming majority of repositories.
//! * **Bots must be separable from humans.** Author classification uses
//!   GraphQL's `__typename`, not a login denylist, so new automation is
//!   classified correctly without a config change.
//! * **The rate-limit budget is a first-class resource.** `core` and
//!   `graphql` are independent pools; both are tracked, a reserve is withheld,
//!   and a cycle that cannot complete is skipped whole rather than left
//!   half-applied.

pub mod collector;
pub mod config;
pub mod github;
pub mod metrics;
pub mod model;
pub mod server;
