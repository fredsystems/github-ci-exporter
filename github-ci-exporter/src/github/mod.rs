//! GitHub API access.
//!
//! Split by transport because the two APIs have different cost models:
//!
//! * [`graphql`] batches many repositories into one request. Issue and PR
//!   counts for ~60 repos cost about 2 points of the 5000/hour budget.
//! * [`rest`] is used for Actions data, which GraphQL exposes only awkwardly.
//!   Every request carries an `If-None-Match`, and a `304 Not Modified` is
//!   **not** charged against the rate limit — measured against the live API.
//!
//! The search API is deliberately unused: it allows only 30 requests/hour,
//! which cannot support polling at any useful interval.

pub mod client;
pub mod graphql;
pub mod rest;

pub use client::{Client, ClientError, RateLimit, RateLimitResource};
