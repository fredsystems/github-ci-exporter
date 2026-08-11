//! Authenticated HTTP client with `ETag` revalidation and rate-limit tracking.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use reqwest::{
    Method, StatusCode,
    header::{ACCEPT, AUTHORIZATION, ETAG, HeaderMap, HeaderValue, IF_NONE_MATCH, USER_AGENT},
};
use serde::{Serialize, de::DeserializeOwned};
use tracing::{debug, warn};

/// GitHub rejects requests without a User-Agent.
const UA: &str = concat!("github-ci-exporter/", env!("CARGO_PKG_VERSION"));

/// Cap on retries for transient failures (429, 5xx, secondary rate limits).
const MAX_RETRIES: u32 = 3;

/// Requests left unspent in each bucket by default.
///
/// The exporter is not necessarily the only consumer of its token, and a
/// fully-drained bucket would also break ad-hoc `gh` usage and any other
/// tooling sharing the credential. 250 of 5000 is a 5% floor.
const DEFAULT_RESERVE: u64 = 250;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("http transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("failed to build http client: {0}")]
    Build(#[source] reqwest::Error),
    #[error("github returned {status} for {url}: {body}")]
    Status {
        status: StatusCode,
        url: String,
        body: String,
    },
    #[error("authentication failed ({status}); check the token and its scopes")]
    Unauthorized { status: StatusCode },
    #[error(
        "{resource} rate limit too low to proceed ({remaining} remaining, reserve withheld); resets in {reset_in:?}"
    )]
    RateLimited {
        resource: RateLimitResource,
        remaining: u64,
        reset_in: Duration,
    },
    #[error("graphql errors: {0}")]
    GraphQl(String),
    #[error("failed to decode response from {url}: {source}")]
    Decode {
        url: String,
        #[source]
        source: serde_json::Error,
    },
}

/// Snapshot of the API's reported rate-limit state for one bucket.
///
/// GitHub maintains independent pools per resource: `core` (REST) and
/// `graphql` each get their own 5000/hour allowance, reported via
/// `x-ratelimit-resource`. Collapsing them into a single figure would make a
/// nearly-exhausted REST budget invisible behind a healthy GraphQL one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RateLimit {
    pub limit: u64,
    pub remaining: u64,
    pub used: u64,
    /// Unix timestamp at which `remaining` resets to `limit`.
    pub reset_at: i64,
}

impl RateLimit {
    /// Whether this bucket has at least `needed` requests left, keeping
    /// `reserve` in hand for other consumers of the same token.
    #[must_use]
    pub const fn can_afford(&self, needed: u64, reserve: u64) -> bool {
        // An unpopulated bucket (no request made yet) must not block the first
        // cycle, otherwise the exporter could never start.
        if self.limit == 0 {
            return true;
        }
        self.remaining >= needed.saturating_add(reserve)
    }

    /// Seconds until this bucket resets, relative to `now`.
    #[must_use]
    pub const fn reset_in_secs(&self, now: i64) -> i64 {
        let delta = self.reset_at - now;
        if delta < 0 { 0 } else { delta }
    }
}

/// Which rate-limit pool a request draws from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RateLimitResource {
    Core,
    GraphQl,
}

impl RateLimitResource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Core => "core",
            Self::GraphQl => "graphql",
        }
    }
}

impl std::fmt::Display for RateLimitResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Persisted `ETag` entry: the validator plus the payload it validated.
///
/// The body must be stored alongside the `ETag`, because a `304` returns no
/// body and the caller still needs the current value.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
struct CacheEntry {
    etag: String,
    body: String,
}

/// On-disk format version for the `ETag` cache.
///
/// The cache stores *projections*, not raw responses, so its contents are only
/// meaningful to the code that produced them. When a projection's shape
/// changes, every persisted entry for that endpoint becomes undecodable while
/// its `ETag` stays valid -- so GitHub answers `304` and the exporter has a
/// validator it cannot use.
///
/// That is not hypothetical. Adding the workflow trigger list changed
/// `WorkflowDefinition` from a bare `Vec<String>` of crons to a struct, and
/// every cached entry stayed in the old shape across the upgrade. The trigger
/// lookup failed for every workflow, an empty trigger list means "accept any
/// event", and the trigger filter was therefore inert in production for as
/// long as the cache survived -- exactly the frext / bike-fitter-1000 false
/// positives it was written to fix.
///
/// Bump this whenever any cached projection's serialised shape changes. The
/// per-entry decode fallback in [`Client::get_cached_as`] recovers from a
/// missed bump, at the cost of one uncached fetch per affected entry.
///
/// **Bump it when the projection's _meaning_ changes too, not only its
/// shape.** Both existing safeguards key on decodability, so a reduction that
/// still deserialises cleanly but is now computed differently sails through
/// them: the version matches, every entry decodes, and `304` replays a value
/// the current code would never have produced.
///
/// That is also not hypothetical. Version 1 held run reductions computed
/// before `reduce_runs` learned to discard workflows with no default-branch
/// trigger. The shape was byte-identical, so the file was accepted and the
/// filtered-out runs came straight back -- resurrecting the exact fossil run
/// the filter existed to remove, and, because such a workflow is no longer
/// masked as stale, publishing it as an outright `failure` that would page.
/// Worse than before the filter was added.
///
/// Version history:
///
/// * 1 -- initial versioned format.
/// * 2 -- `reduce_runs` drops workflows whose triggers give them no
///   default-branch state. Same shape, different contents.
const CACHE_FORMAT_VERSION: u32 = 2;

/// Versioned envelope for the persisted cache, as read from disk.
///
/// The version is checked before the entries are trusted, so a format change
/// discards the file wholesale instead of yielding a map of undecodable
/// projections.
#[derive(Debug, serde::Deserialize)]
struct CacheFile {
    version: u32,
    entries: HashMap<String, CacheEntry>,
}

/// The same envelope for writing, borrowing the live cache.
///
/// Separate from [`CacheFile`] so persisting does not have to clone every
/// entry while the lock is held.
#[derive(Debug, Serialize)]
struct CacheFileRef<'a> {
    version: u32,
    entries: &'a HashMap<String, CacheEntry>,
}

/// Outcome of a conditional request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheOutcome {
    /// Server returned 200; the payload changed.
    Modified,
    /// Server returned 304; the cached payload is still current and the
    /// request was not charged against the rate limit.
    NotModified,
}

#[derive(Debug)]
pub struct Client {
    http: reqwest::Client,
    api_url: String,
    graphql_url: String,
    /// `ETag` cache keyed by request URL.
    cache: Mutex<HashMap<String, CacheEntry>>,
    cache_path: Option<PathBuf>,
    /// Per-resource rate-limit state, keyed by `x-ratelimit-resource`.
    rate_limits: Mutex<HashMap<RateLimitResource, RateLimit>>,
    /// Requests to keep in reserve so the exporter never fully drains a token
    /// that other tooling may share.
    reserve: u64,
    /// Counts of HTTP responses by status class, for self-monitoring.
    requests_total: AtomicU64,
    not_modified_total: AtomicU64,
    /// Requests not attempted because the budget was too low.
    skipped_total: AtomicU64,
}

impl Client {
    /// Builds a client and restores any previously persisted `ETag` cache.
    ///
    /// # Errors
    /// Returns [`ClientError::Build`] if the underlying HTTP client cannot be
    /// constructed (invalid token characters, TLS backend failure).
    pub fn new(token: &str, api_url: &str, graphql_url: &str) -> Result<Self, ClientError> {
        let mut headers = HeaderMap::new();
        let mut auth = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
            ClientError::Unauthorized {
                status: StatusCode::UNAUTHORIZED,
            }
        })?;
        auth.set_sensitive(true);
        headers.insert(AUTHORIZATION, auth);
        headers.insert(USER_AGENT, HeaderValue::from_static(UA));
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.github+json"),
        );
        headers.insert(
            "X-GitHub-Api-Version",
            HeaderValue::from_static("2022-11-28"),
        );

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            // Trust Mozilla's bundled CA set rather than the system trust
            // store. reqwest 0.13 switched to rustls-platform-verifier, which
            // reads system roots; those are absent in the Nix build sandbox
            // (TLS setup fails with "No CA certificates were loaded from the
            // system") and are not guaranteed under a hardened unit with
            // ProtectSystem=strict. Only api.github.com is contacted, so a
            // fixed public-CA set is sufficient and makes the binary
            // independent of ambient host state.
            .tls_certs_only(
                webpki_root_certs::TLS_SERVER_ROOT_CERTS
                    .iter()
                    .map(|cert| reqwest::Certificate::from_der(cert))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(ClientError::Build)?,
            )
            .build()
            .map_err(ClientError::Build)?;

        Ok(Self {
            http,
            api_url: api_url.trim_end_matches('/').to_owned(),
            graphql_url: graphql_url.to_owned(),
            cache: Mutex::new(HashMap::new()),
            cache_path: None,
            rate_limits: Mutex::new(HashMap::new()),
            reserve: DEFAULT_RESERVE,
            requests_total: AtomicU64::new(0),
            not_modified_total: AtomicU64::new(0),
            skipped_total: AtomicU64::new(0),
        })
    }

    /// Sets how many requests to leave unspent in each bucket.
    #[must_use]
    pub const fn with_reserve(mut self, reserve: u64) -> Self {
        self.reserve = reserve;
        self
    }

    /// Enables on-disk persistence of the `ETag` cache.
    ///
    /// Without this a restart re-fetches every repository at full cost. A
    /// corrupt, unreadable, or stale-format cache is a warning, never fatal:
    /// the exporter simply starts cold.
    #[must_use]
    pub fn with_cache_file(mut self, path: &Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(raw) => match Self::decode_cache_file(&raw) {
                Ok(entries) => {
                    debug!(count = entries.len(), "restored etag cache");
                    if let Ok(mut cache) = self.cache.lock() {
                        *cache = entries;
                    }
                }
                Err(reason) => warn!(reason, "starting with a cold etag cache"),
            },
            // A missing file is the normal first start. Anything else -- a
            // permission problem, an I/O error -- means the cache is silently
            // not working, which shows up only as a persistently high request
            // count, so it is worth a warning.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                debug!(path = %path.display(), "no etag cache to restore");
            }
            Err(error) => {
                warn!(path = %path.display(), %error, "could not read etag cache; starting cold");
            }
        }
        self.cache_path = Some(path.to_path_buf());
        self
    }

    /// Parses a persisted cache file, rejecting anything not written by this
    /// format version.
    ///
    /// A version mismatch is discarded wholesale rather than entry by entry:
    /// the projections are what changed, so none of them can be trusted, and
    /// keeping them would mean serving stale reductions for every endpoint
    /// whose `ETag` is still valid.
    fn decode_cache_file(raw: &str) -> Result<HashMap<String, CacheEntry>, &'static str> {
        let file: CacheFile = serde_json::from_str(raw).map_err(|_| "cache file is unreadable")?;
        if file.version != CACHE_FORMAT_VERSION {
            return Err("cache was written by a different projection format");
        }
        Ok(file.entries)
    }

    /// Writes the `ETag` cache to disk, if persistence is enabled.
    ///
    /// # Errors
    /// Returns an error if the cache directory cannot be created or written.
    pub fn persist_cache(&self) -> std::io::Result<()> {
        let Some(path) = self.cache_path.as_ref() else {
            return Ok(());
        };
        let Ok(cache) = self.cache.lock() else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let encoded = serde_json::to_string(&CacheFileRef {
            version: CACHE_FORMAT_VERSION,
            entries: &cache,
        })?;
        // Write-then-rename so a crash mid-write cannot leave a truncated
        // cache that would be discarded on next start.
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, encoded)?;
        std::fs::rename(&tmp, path)
    }

    /// Current state of one rate-limit bucket.
    #[must_use]
    pub fn rate_limit(&self, resource: RateLimitResource) -> RateLimit {
        self.rate_limits
            .lock()
            .ok()
            .and_then(|guard| guard.get(&resource).copied())
            .unwrap_or_default()
    }

    /// Whether `needed` requests can be spent against `resource` while
    /// respecting the configured reserve.
    #[must_use]
    pub fn can_afford(&self, resource: RateLimitResource, needed: u64) -> bool {
        self.rate_limit(resource).can_afford(needed, self.reserve)
    }

    /// Records that a planned request was abandoned due to budget pressure.
    pub fn record_skipped(&self, count: u64) {
        self.skipped_total.fetch_add(count, Ordering::Relaxed);
    }

    #[must_use]
    pub fn skipped_total(&self) -> u64 {
        self.skipped_total.load(Ordering::Relaxed)
    }

    #[must_use]
    pub const fn reserve(&self) -> u64 {
        self.reserve
    }

    /// Guards a request against budget exhaustion.
    ///
    /// Returns [`ClientError::RateLimited`] rather than issuing a request that
    /// GitHub would reject, so a drained bucket degrades into a clearly
    /// reported skip instead of a burst of 403s.
    fn check_budget(&self, resource: RateLimitResource) -> Result<(), ClientError> {
        if self.can_afford(resource, 1) {
            return Ok(());
        }
        let limit = self.rate_limit(resource);
        let reset_in =
            u64::try_from(limit.reset_in_secs(chrono::Utc::now().timestamp())).unwrap_or(0);
        self.record_skipped(1);
        Err(ClientError::RateLimited {
            resource,
            remaining: limit.remaining,
            reset_in: Duration::from_secs(reset_in),
        })
    }

    #[must_use]
    pub fn requests_total(&self) -> u64 {
        self.requests_total.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn not_modified_total(&self) -> u64 {
        self.not_modified_total.load(Ordering::Relaxed)
    }

    /// Records the rate-limit headers against the bucket the response names.
    ///
    /// `x-ratelimit-resource` is authoritative: REST reports `core` and
    /// GraphQL reports `graphql`, and the two have independent allowances.
    fn record_rate_limit(&self, headers: &HeaderMap, fallback: RateLimitResource) {
        let read = |name: &str| -> Option<u64> { headers.get(name)?.to_str().ok()?.parse().ok() };

        let resource = headers
            .get("x-ratelimit-resource")
            .and_then(|value| value.to_str().ok())
            .map_or(fallback, |value| match value {
                "graphql" => RateLimitResource::GraphQl,
                _ => RateLimitResource::Core,
            });

        if let (Some(limit), Some(remaining)) =
            (read("x-ratelimit-limit"), read("x-ratelimit-remaining"))
            && let Ok(mut guard) = self.rate_limits.lock()
        {
            guard.insert(
                resource,
                RateLimit {
                    limit,
                    remaining,
                    used: read("x-ratelimit-used").unwrap_or(0),
                    reset_at: read("x-ratelimit-reset")
                        .and_then(|v| i64::try_from(v).ok())
                        .unwrap_or(0),
                },
            );
        }
    }

    /// Performs a GET with `ETag` revalidation, caching a *projection* of the
    /// response rather than the response itself.
    ///
    /// A `304` carries no body, so the cache must be able to reproduce the
    /// value. Storing raw payloads is prohibitively large here: the Actions
    /// runs endpoint returns ~1.5 MB per repository, which measured at 67 MB
    /// of cache for 61 repositories. `project` reduces the payload to the few
    /// fields actually exported before it is stored.
    ///
    /// # Errors
    /// Returns [`ClientError`] on transport failure, a non-success status, or
    /// a body that does not match `T`.
    pub async fn get_cached<T, R, F>(
        &self,
        path: &str,
        project: F,
    ) -> Result<(R, CacheOutcome), ClientError>
    where
        T: DeserializeOwned,
        R: Serialize + DeserializeOwned,
        F: FnOnce(T) -> R,
    {
        self.get_cached_as(path, path, project).await
    }

    /// As [`Self::get_cached`], but with the cache key decoupled from the
    /// request path.
    ///
    /// Needed when the projection depends on inputs beyond the response body:
    /// the cached value must be invalidated when those inputs change, even
    /// though the request itself is unchanged.
    ///
    /// # Errors
    /// Returns [`ClientError`] on transport failure, a non-success status, or
    /// a body that does not match `T`.
    pub async fn get_cached_as<T, R, F>(
        &self,
        cache_key: &str,
        path: &str,
        project: F,
    ) -> Result<(R, CacheOutcome), ClientError>
    where
        T: DeserializeOwned,
        R: Serialize + DeserializeOwned,
        F: FnOnce(T) -> R,
    {
        let url = if path.starts_with("http") {
            path.to_owned()
        } else {
            format!("{}{}", self.api_url, path)
        };
        let cache_key = if cache_key == path {
            url.clone()
        } else {
            cache_key.to_owned()
        };

        // Cleared if the cached projection turns out to be unusable, so the
        // retry goes out unconditionally.
        let mut cached_etag = self
            .cache
            .lock()
            .ok()
            .and_then(|cache| cache.get(&cache_key).map(|entry| entry.etag.clone()));

        // A conditional request answered 304 is free, but that is only known
        // after the fact; the budget must be checked as though it will cost.
        self.check_budget(RateLimitResource::Core)?;

        let mut attempt = 0;
        let (body, etag) = loop {
            let mut request = self.http.request(Method::GET, &url);
            if let Some(etag) = cached_etag.as_ref() {
                request = request.header(IF_NONE_MATCH, etag);
            }

            let response = request.send().await?;
            self.requests_total.fetch_add(1, Ordering::Relaxed);
            let status = response.status();
            self.record_rate_limit(response.headers(), RateLimitResource::Core);

            // A 304 answering a request that carried no validator would be a
            // protocol violation, and treating it as another cache miss would
            // spin forever. Fail the request instead; the caller degrades to
            // its own fallback for one cycle.
            if status == StatusCode::NOT_MODIFIED && cached_etag.is_none() {
                return Err(ClientError::Status {
                    status,
                    url: url.clone(),
                    body: "304 returned for an unconditional request".to_owned(),
                });
            }

            if status == StatusCode::NOT_MODIFIED {
                self.not_modified_total.fetch_add(1, Ordering::Relaxed);
                let cached = self
                    .cache
                    .lock()
                    .ok()
                    .and_then(|cache| cache.get(&cache_key).map(|entry| entry.body.clone()));
                // A projection that will not decode is treated as a cache
                // miss, not as an error. The alternative -- propagating the
                // decode failure -- is unrecoverable: the ETag stays valid, so
                // every subsequent cycle gets the same 304 and the same
                // undecodable body, and the caller degrades to its
                // lookup-failed fallback forever. That is how the workflow
                // trigger filter came to be silently inert in production.
                match cached.as_deref().map(serde_json::from_str) {
                    Some(Ok(value)) => return Ok((value, CacheOutcome::NotModified)),
                    Some(Err(error)) => warn!(
                        %url,
                        %error,
                        "cached projection no longer decodes; refetching unconditionally"
                    ),
                    // A 304 with no cached projection should be impossible, but
                    // a pruned entry must not wedge the poll loop.
                    None => warn!(%url, "304 with no cached body; refetching unconditionally"),
                }
                // Drop the unusable entry and go round again without the
                // validator. Re-entering the loop rather than issuing the
                // refetch inline keeps `classify_retry` in the path: the
                // recovery request is as likely to meet a 429 or a 5xx as any
                // other, and bypassing the backoff would turn a transient
                // error into a failed cycle.
                if let Ok(mut cache) = self.cache.lock() {
                    cache.remove(&cache_key);
                }
                cached_etag = None;
                continue;
            }

            if let Some(delay) = Self::classify_retry(status, response.headers(), attempt) {
                attempt += 1;
                warn!(%url, %status, attempt, ?delay, "retrying github request");
                tokio::time::sleep(delay).await;
                continue;
            }

            let etag = response
                .headers()
                .get(ETAG)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            break (self.check_status(status, &url, response).await?, etag);
        };

        let parsed: T = serde_json::from_str(&body).map_err(|source| ClientError::Decode {
            url: url.clone(),
            source,
        })?;
        let projected = project(parsed);

        // Only cache what can be replayed; an unserialisable projection simply
        // means the next cycle refetches.
        if let (Some(etag), Ok(encoded)) = (etag, serde_json::to_string(&projected))
            && let Ok(mut cache) = self.cache.lock()
        {
            cache.insert(
                cache_key,
                CacheEntry {
                    etag,
                    body: encoded,
                },
            );
        }

        Ok((projected, CacheOutcome::Modified))
    }

    /// Executes a GraphQL query.
    ///
    /// GraphQL reports errors with HTTP 200, so the `errors` array must be
    /// inspected explicitly.
    ///
    /// # Errors
    /// Returns [`ClientError::GraphQl`] if the response carries errors, or a
    /// transport/decode error otherwise.
    pub async fn graphql<T: DeserializeOwned>(
        &self,
        query: &str,
        variables: serde_json::Value,
    ) -> Result<T, ClientError> {
        let payload = serde_json::json!({ "query": query, "variables": variables });

        self.check_budget(RateLimitResource::GraphQl)?;

        let mut attempt = 0;
        loop {
            let response = self
                .http
                .post(&self.graphql_url)
                .json(&payload)
                .send()
                .await?;
            self.requests_total.fetch_add(1, Ordering::Relaxed);
            let status = response.status();
            self.record_rate_limit(response.headers(), RateLimitResource::GraphQl);

            if let Some(delay) = Self::classify_retry(status, response.headers(), attempt) {
                attempt += 1;
                warn!(%status, attempt, ?delay, "retrying graphql request");
                tokio::time::sleep(delay).await;
                continue;
            }

            let body = self
                .check_status(status, &self.graphql_url, response)
                .await?;
            let envelope: GraphQlResponse<T> =
                serde_json::from_str(&body).map_err(|source| ClientError::Decode {
                    url: self.graphql_url.clone(),
                    source,
                })?;

            if let Some(errors) = envelope.errors.filter(|e| !e.is_empty()) {
                let joined = errors
                    .iter()
                    .map(|e| e.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ");
                return Err(ClientError::GraphQl(joined));
            }

            return envelope
                .data
                .ok_or_else(|| ClientError::GraphQl("response contained no data".to_owned()));
        }
    }

    /// Decides whether a response warrants a retry, and after how long.
    fn classify_retry(status: StatusCode, headers: &HeaderMap, attempt: u32) -> Option<Duration> {
        if attempt >= MAX_RETRIES {
            return None;
        }
        let retryable = status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::FORBIDDEN && headers.contains_key("retry-after")
            || status.is_server_error();
        if !retryable {
            return None;
        }
        // Honour `Retry-After` when GitHub sends it (secondary rate limits),
        // otherwise exponential backoff.
        let after = headers
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .map_or_else(
                || Duration::from_secs(2u64.saturating_pow(attempt + 1)),
                Duration::from_secs,
            );
        Some(after.min(Duration::from_secs(60)))
    }

    async fn check_status(
        &self,
        status: StatusCode,
        url: &str,
        response: reqwest::Response,
    ) -> Result<String, ClientError> {
        if status == StatusCode::UNAUTHORIZED {
            return Err(ClientError::Unauthorized { status });
        }
        if status.is_success() {
            return Ok(response.text().await?);
        }
        let body = response.text().await.unwrap_or_default();
        Err(ClientError::Status {
            status,
            url: url.to_owned(),
            body: body.chars().take(300).collect(),
        })
    }
}

#[derive(Debug, serde::Deserialize)]
struct GraphQlResponse<T> {
    data: Option<T>,
    errors: Option<Vec<GraphQlError>>,
}

#[derive(Debug, serde::Deserialize)]
struct GraphQlError {
    message: String,
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
        matchers::{header, method, path},
    };

    use super::*;

    /// Identity projection for tests that assert on the raw payload.
    fn identity(value: serde_json::Value) -> serde_json::Value {
        value
    }

    fn client_for(server: &MockServer) -> Client {
        Client::new(
            "test-token",
            &server.uri(),
            &format!("{}/graphql", server.uri()),
        )
        .expect("client should build")
    }

    #[tokio::test]
    async fn sends_authorization_and_user_agent() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/ping"))
            .and(header("authorization", "Bearer test-token"))
            .and(header("user-agent", UA))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server);
        let (value, outcome): (serde_json::Value, _) = client
            .get_cached("/ping", identity)
            .await
            .expect("request succeeds");
        assert_eq!(value["ok"], json!(true));
        assert_eq!(outcome, CacheOutcome::Modified);
    }

    #[tokio::test]
    async fn revalidates_with_etag_and_serves_cached_body_on_304() {
        let server = MockServer::start().await;
        // First call returns a body plus an ETag.
        Mock::given(method("GET"))
            .and(path("/data"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"abc123\"")
                    .set_body_json(json!({"value": 42})),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;

        let client = client_for(&server);
        let (first, outcome): (serde_json::Value, _) = client
            .get_cached("/data", identity)
            .await
            .expect("first call");
        assert_eq!(first["value"], json!(42));
        assert_eq!(outcome, CacheOutcome::Modified);

        // Second call must send If-None-Match and accept an empty 304 body.
        Mock::given(method("GET"))
            .and(path("/data"))
            .and(header("if-none-match", "\"abc123\""))
            .respond_with(ResponseTemplate::new(304))
            .expect(1)
            .mount(&server)
            .await;

        let (second, outcome): (serde_json::Value, _) = client
            .get_cached("/data", identity)
            .await
            .expect("second call");
        assert_eq!(second["value"], json!(42), "cached body must be replayed");
        assert_eq!(outcome, CacheOutcome::NotModified);
        assert_eq!(client.not_modified_total(), 1);
    }

    #[tokio::test]
    async fn caches_the_projection_not_the_raw_payload() {
        // Regression guard: caching raw bodies measured 67 MB for 61 repos,
        // because the Actions runs endpoint returns ~1.5 MB each.
        let server = MockServer::start().await;
        let bulky = json!({
            "items": (0..500).map(|i| json!({"id": i, "noise": "x".repeat(200)}))
                .collect::<Vec<_>>()
        });
        Mock::given(method("GET"))
            .and(path("/bulky"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"bulk\"")
                    .set_body_json(&bulky),
            )
            .mount(&server)
            .await;

        let client = client_for(&server);
        let (count, _): (usize, _) = client
            .get_cached("/bulky", |value: serde_json::Value| {
                value["items"].as_array().map_or(0, Vec::len)
            })
            .await
            .expect("request");
        assert_eq!(count, 500);

        let cached_len = {
            let cache = client.cache.lock().expect("lock");
            cache
                .values()
                .next()
                .map(|entry| entry.body.len())
                .unwrap_or_default()
        };
        assert!(
            cached_len < 50,
            "cache must hold the projection, not the {}-byte payload (got {cached_len})",
            bulky.to_string().len()
        );
    }

    #[tokio::test]
    async fn distinct_cache_keys_do_not_share_an_entry() {
        // A projection that depends on more than the response body must be
        // invalidated when those inputs change, even though the URL is
        // identical and the server would answer 304.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/runs"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"same\"")
                    .set_body_json(json!({"n": 1})),
            )
            .mount(&server)
            .await;

        let client = client_for(&server);
        let (first, _): (String, _) = client
            .get_cached_as("/runs#v1", "/runs", |v: serde_json::Value| {
                format!("v1:{}", v["n"])
            })
            .await
            .expect("first");
        assert_eq!(first, "v1:1");

        // Same URL, different key: must re-project rather than replay.
        let (second, outcome): (String, _) = client
            .get_cached_as("/runs#v2", "/runs", |v: serde_json::Value| {
                format!("v2:{}", v["n"])
            })
            .await
            .expect("second");
        assert_eq!(
            second, "v2:1",
            "a new cache key must not replay the old projection"
        );
        assert_eq!(outcome, CacheOutcome::Modified);
    }

    #[tokio::test]
    async fn custom_cache_key_is_reused_on_repeat() {
        // Regression guard: entries were read by cache key but written by URL,
        // so every `get_cached_as` caller refetched and reprojected. That
        // silently disabled ETag revalidation for the runs endpoint, the
        // single largest consumer of the rate-limit budget.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/runs"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"e1\"")
                    .set_body_json(json!({"n": 7})),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;

        let client = client_for(&server);
        let (first, outcome): (i64, _) = client
            .get_cached_as("/runs#fp=abc", "/runs", |v: serde_json::Value| {
                v["n"].as_i64().unwrap_or_default()
            })
            .await
            .expect("first");
        assert_eq!(first, 7);
        assert_eq!(outcome, CacheOutcome::Modified);

        // The same key must now revalidate and be answered from cache.
        Mock::given(method("GET"))
            .and(path("/runs"))
            .and(header("if-none-match", "\"e1\""))
            .respond_with(ResponseTemplate::new(304))
            .expect(1)
            .mount(&server)
            .await;

        let (second, outcome): (i64, _) = client
            .get_cached_as("/runs#fp=abc", "/runs", |v: serde_json::Value| {
                v["n"].as_i64().unwrap_or_default()
            })
            .await
            .expect("second");
        assert_eq!(second, 7, "cached projection must be replayed");
        assert_eq!(
            outcome,
            CacheOutcome::NotModified,
            "a repeated custom key must revalidate rather than refetch"
        );
    }

    #[tokio::test]
    async fn records_rate_limit_headers() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/rl"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("x-ratelimit-limit", "5000")
                    .insert_header("x-ratelimit-remaining", "4931")
                    .insert_header("x-ratelimit-used", "69")
                    .set_body_json(json!({})),
            )
            .mount(&server)
            .await;

        let client = client_for(&server);
        let _: (serde_json::Value, _) = client.get_cached("/rl", identity).await.expect("request");
        let rl = client.rate_limit(RateLimitResource::Core);
        assert_eq!(rl.limit, 5000);
        assert_eq!(rl.remaining, 4931);
        assert_eq!(rl.used, 69);
    }

    #[tokio::test]
    async fn core_and_graphql_budgets_are_tracked_separately() {
        // Regression guard: these are independent 5000/hour pools. Collapsing
        // them would hide an exhausted REST budget behind a healthy GraphQL one.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/core"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("x-ratelimit-limit", "5000")
                    .insert_header("x-ratelimit-remaining", "10")
                    .insert_header("x-ratelimit-resource", "core")
                    .set_body_json(json!({})),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("x-ratelimit-limit", "5000")
                    .insert_header("x-ratelimit-remaining", "4900")
                    .insert_header("x-ratelimit-resource", "graphql")
                    .set_body_json(json!({"data": {"ok": true}})),
            )
            .mount(&server)
            .await;

        let client = client_for(&server);
        let _: (serde_json::Value, _) = client
            .get_cached("/core", identity)
            .await
            .expect("core request");
        let _: serde_json::Value = client
            .graphql("query {}", json!({}))
            .await
            .expect("graphql request");

        assert_eq!(client.rate_limit(RateLimitResource::Core).remaining, 10);
        assert_eq!(
            client.rate_limit(RateLimitResource::GraphQl).remaining,
            4900
        );
    }

    #[tokio::test]
    async fn refuses_requests_once_budget_falls_below_reserve() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/drain"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("x-ratelimit-limit", "5000")
                    .insert_header("x-ratelimit-remaining", "5")
                    .insert_header("x-ratelimit-resource", "core")
                    .set_body_json(json!({})),
            )
            // Exactly one request: the second must be refused locally.
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server).with_reserve(100);
        let _: (serde_json::Value, _) = client
            .get_cached("/drain", identity)
            .await
            .expect("first request");

        let error = client
            .get_cached::<serde_json::Value, serde_json::Value, _>("/drain", identity)
            .await
            .expect_err("must refuse once below reserve");
        assert!(matches!(
            error,
            ClientError::RateLimited {
                resource: RateLimitResource::Core,
                remaining: 5,
                ..
            }
        ));
        assert_eq!(client.skipped_total(), 1);
    }

    #[test]
    fn unpopulated_budget_never_blocks_the_first_request() {
        let limit = RateLimit::default();
        assert!(
            limit.can_afford(200, 250),
            "a bucket with no observed headers must not wedge startup"
        );
    }

    #[test]
    fn can_afford_accounts_for_the_reserve() {
        let limit = RateLimit {
            limit: 5000,
            remaining: 300,
            used: 4700,
            reset_at: 0,
        };
        assert!(limit.can_afford(49, 250), "300 covers 49 plus 250 reserve");
        assert!(!limit.can_afford(51, 250), "300 cannot cover 51 plus 250");
    }

    #[test]
    fn reset_in_never_goes_negative() {
        let limit = RateLimit {
            limit: 5000,
            remaining: 0,
            used: 5000,
            reset_at: 1_000,
        };
        assert_eq!(limit.reset_in_secs(900), 100);
        assert_eq!(limit.reset_in_secs(5_000), 0, "past resets clamp to zero");
    }

    #[tokio::test]
    async fn surfaces_graphql_errors_despite_http_200() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": null,
                "errors": [{"message": "Could not resolve to a Repository"}]
            })))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let result: Result<serde_json::Value, _> = client.graphql("query {}", json!({})).await;
        let error = result.expect_err("graphql errors must not be silently ignored");
        assert!(matches!(error, ClientError::GraphQl(msg) if msg.contains("Could not resolve")));
    }

    #[tokio::test]
    async fn unauthorized_is_distinguishable() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nope"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let result: Result<(serde_json::Value, _), _> = client.get_cached("/nope", identity).await;
        assert!(matches!(
            result.unwrap_err(),
            ClientError::Unauthorized { .. }
        ));
    }

    #[tokio::test]
    async fn retries_on_server_error_then_succeeds() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/flaky"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/flaky"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": 1})))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let (value, _): (serde_json::Value, _) = client
            .get_cached("/flaky", identity)
            .await
            .expect("retry should recover");
        assert_eq!(value["ok"], json!(1));
    }

    #[test]
    fn cache_round_trips_through_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("etags.json");

        let client = Client::new(
            "t",
            "https://example.invalid",
            "https://example.invalid/graphql",
        )
        .expect("client")
        .with_cache_file(&path);
        client.cache.lock().expect("lock").insert(
            "https://example.invalid/x".to_owned(),
            CacheEntry {
                etag: "\"e1\"".to_owned(),
                body: "{\"a\":1}".to_owned(),
            },
        );
        client.persist_cache().expect("persist");

        let restored = Client::new(
            "t",
            "https://example.invalid",
            "https://example.invalid/graphql",
        )
        .expect("client")
        .with_cache_file(&path);
        let restored_etag = {
            let cache = restored.cache.lock().expect("lock");
            cache
                .get("https://example.invalid/x")
                .map(|e| e.etag.clone())
        };
        assert_eq!(restored_etag.as_deref(), Some("\"e1\""));
    }

    #[tokio::test]
    async fn an_undecodable_cached_projection_refetches_instead_of_failing() {
        // Regression guard, and the highest-value test in this file. Adding
        // the trigger list changed the workflow-definition projection from
        // `Vec<String>` to a struct. Every persisted entry kept the old shape
        // while its ETag stayed valid, so GitHub answered 304 forever and the
        // cached body never decoded. The lookup failed on every cycle, an
        // empty trigger list means "accept any event", and the trigger filter
        // was inert in production -- the frext / bike-fitter-1000 false
        // positives survived the fix that was written to remove them.
        #[derive(Debug, PartialEq, Serialize, serde::Deserialize)]
        struct NewShape {
            crons: Vec<String>,
            triggers: Vec<String>,
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/contents"))
            .and(header("if-none-match", "\"still-valid\""))
            .respond_with(ResponseTemplate::new(304))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // The unconditional refetch that must follow the failed decode.
        Mock::given(method("GET"))
            .and(path("/contents"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"fresh\"")
                    .set_body_json(json!({"on": ["pull_request"]})),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server);
        // Seed the cache the way a pre-upgrade binary would have left it: a
        // valid ETag alongside a body in the *old* projection shape.
        client.cache.lock().expect("lock").insert(
            format!("{}/contents", server.uri()),
            CacheEntry {
                etag: "\"still-valid\"".to_owned(),
                body: "[\"0 12 * * *\"]".to_owned(),
            },
        );

        let (value, outcome): (NewShape, _) = client
            .get_cached("/contents", |body: serde_json::Value| NewShape {
                crons: Vec::new(),
                triggers: body["on"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(str::to_owned))
                            .collect()
                    })
                    .unwrap_or_default(),
            })
            .await
            .expect("an undecodable cache entry must not surface as an error");

        assert_eq!(
            value.triggers,
            ["pull_request"],
            "the refetched value must be projected, not the stale cache replayed"
        );
        assert_eq!(outcome, CacheOutcome::Modified);
    }

    #[tokio::test]
    async fn the_stale_projection_refetch_still_gets_bounded_retries() {
        // The recovery request is as likely to meet a 429 or a 5xx as any
        // other. Issuing it outside the retry loop would turn a transient
        // error into a failed cycle, so it must re-enter the loop rather than
        // bypass `classify_retry`.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/flaky-recovery"))
            .and(header("if-none-match", "\"stale\""))
            .respond_with(ResponseTemplate::new(304))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // The unconditional retry hits a transient server error first...
        Mock::given(method("GET"))
            .and(path("/flaky-recovery"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // ...and must then be retried rather than surfacing as an error.
        Mock::given(method("GET"))
            .and(path("/flaky-recovery"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"fresh\"")
                    .set_body_json(json!({"n": 5})),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server);
        client.cache.lock().expect("lock").insert(
            format!("{}/flaky-recovery", server.uri()),
            CacheEntry {
                etag: "\"stale\"".to_owned(),
                // Will not decode as the i64 the projection produces.
                body: "{\"unexpected\":\"shape\"}".to_owned(),
            },
        );

        let (value, _): (i64, _) = client
            .get_cached("/flaky-recovery", |v: serde_json::Value| {
                v["n"].as_i64().unwrap_or_default()
            })
            .await
            .expect("a 5xx on the recovery request must be retried, not fatal");
        assert_eq!(value, 5);
    }

    #[tokio::test]
    async fn a_304_without_a_validator_is_an_error_not_a_spin() {
        // Guard against the refetch loop: a server answering 304 to a request
        // that carried no If-None-Match would otherwise be treated as another
        // cache miss and retried forever.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/liar"))
            .respond_with(ResponseTemplate::new(304))
            .mount(&server)
            .await;

        let client = client_for(&server);
        let result: Result<(serde_json::Value, _), _> = client.get_cached("/liar", identity).await;
        assert!(
            matches!(
                result.expect_err("must not loop"),
                ClientError::Status {
                    status: StatusCode::NOT_MODIFIED,
                    ..
                }
            ),
            "an unconditional 304 must fail fast"
        );
    }

    #[test]
    fn a_cache_written_by_another_format_version_is_discarded() {
        // Belt to the per-entry fallback's braces: a version bump drops the
        // whole file, so a projection change costs exactly one cold sweep
        // rather than a warning per entry.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("etags.json");
        let stale = serde_json::json!({
            "version": CACHE_FORMAT_VERSION + 1,
            "entries": {
                "https://example.invalid/x": { "etag": "\"e1\"", "body": "{}" }
            }
        });
        std::fs::write(&path, stale.to_string()).expect("write");

        let client = Client::new(
            "t",
            "https://example.invalid",
            "https://example.invalid/graphql",
        )
        .expect("client")
        .with_cache_file(&path);

        assert!(
            client.cache.lock().expect("lock").is_empty(),
            "entries from a different projection format must not be trusted"
        );
    }

    #[test]
    fn a_pre_versioning_cache_file_is_discarded() {
        // The format in production before this envelope existed: a bare map.
        // It must be rejected rather than parsed, because its projections are
        // precisely the ones known to be stale.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("etags.json");
        std::fs::write(
            &path,
            r#"{"https://example.invalid/x":{"etag":"\"e1\"","body":"[]"}}"#,
        )
        .expect("write");

        let client = Client::new(
            "t",
            "https://example.invalid",
            "https://example.invalid/graphql",
        )
        .expect("client")
        .with_cache_file(&path);

        assert!(client.cache.lock().expect("lock").is_empty());
    }

    #[test]
    fn corrupt_cache_file_is_ignored_not_fatal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("etags.json");
        std::fs::write(&path, "this is not json").expect("write");

        let client = Client::new(
            "t",
            "https://example.invalid",
            "https://example.invalid/graphql",
        )
        .expect("client")
        .with_cache_file(&path);
        let is_empty = {
            let cache = client.cache.lock().expect("lock");
            cache.is_empty()
        };
        assert!(is_empty);
    }
}
