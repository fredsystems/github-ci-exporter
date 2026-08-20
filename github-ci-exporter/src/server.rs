//! The exporter's HTTP endpoints: `/metrics`, `/repos.json`, `/health`.

use axum::{
    Router,
    extract::State,
    http::{StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use tracing::{error, info};

use crate::metrics::Publisher;

/// Builds the exporter's HTTP router.
pub fn router(publisher: Publisher) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/repos.json", get(repos_handler))
        .route("/health", get(health_handler))
        .route("/", get(index_handler))
        .with_state(publisher)
}

async fn metrics_handler(State(publisher): State<Publisher>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        publisher.render(),
    )
}

/// Serves the repository index as JSON.
///
/// A whole-snapshot endpoint rather than a `?q=` search endpoint. The consumer
/// is a type-ahead box, so a search endpoint would mean a request per
/// keystroke to filter a list that fits in a few kilobytes -- strictly more
/// latency and more failure modes than shipping it once and matching locally.
/// Clients fetch this on load and filter in memory.
///
/// `axum::Json` is deliberately not used: it would require enabling axum's
/// `json` feature for a response this handler can build with the
/// already-present `serde_json` and one header, matching `metrics_handler`.
async fn repos_handler(State(publisher): State<Publisher>) -> impl IntoResponse {
    let index = publisher.repo_index();
    // `as_ref` rather than `&index`: serde only implements `Serialize` for
    // `Arc<T>` behind its `rc` feature, and enabling that repo-wide to avoid
    // one deref would be the wrong trade.
    match serde_json::to_string(index.as_ref()) {
        Ok(body) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/json; charset=utf-8"),
                // The index only changes once per collection cycle, but a
                // stale jump list is more confusing than a cheap revalidation
                // on a LAN, so caching is left to the client's reload.
                (header::CACHE_CONTROL, "no-cache"),
            ],
            body,
        ),
        Err(error) => {
            // Unreachable for these types, but the policy is no panics in
            // production: a broken index must not take the whole server down,
            // and `/metrics` in particular must keep answering.
            error!(%error, "failed to serialise repository index");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [
                    (header::CONTENT_TYPE, "application/json; charset=utf-8"),
                    (header::CACHE_CONTROL, "no-cache"),
                ],
                String::from(r#"{"error":"failed to serialise repository index"}"#),
            )
        }
    }
}

/// Liveness only. Readiness is expressed through
/// `github_exporter_scrape_success`, because an exporter that is up but
/// failing to collect must still serve metrics saying so.
async fn health_handler() -> impl IntoResponse {
    "ok"
}

async fn index_handler() -> impl IntoResponse {
    axum::response::Html(
        "<html><head><title>github-ci-exporter</title></head>\
         <body><h1>github-ci-exporter</h1>\
         <p><a href=\"/metrics\">/metrics</a></p>\
         <p><a href=\"/repos.json\">/repos.json</a></p></body></html>",
    )
}

/// Serves the exporter until `shutdown` resolves.
///
/// # Errors
/// Returns an error if the listener cannot bind or the server fails.
pub async fn serve(
    listen: std::net::SocketAddr,
    publisher: Publisher,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(listen).await?;
    info!(%listen, "serving metrics");
    axum::serve(listener, router(publisher))
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "panicking is how a test reports failure"
)]
mod tests {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt as _;

    use super::*;
    use crate::{
        metrics::Metrics,
        model::{RepoIndex, RepoIndexEntry},
    };

    #[tokio::test]
    async fn metrics_endpoint_serves_openmetrics() {
        let (metrics, registry) = Metrics::new();
        metrics.scrape_success.set(1);
        let app = router(Publisher::new(metrics, registry));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert!(
            content_type.contains("openmetrics-text"),
            "got {content_type}"
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("github_exporter_scrape_success 1"));
    }

    #[tokio::test]
    async fn repos_endpoint_serves_the_published_index() {
        let (metrics, registry) = Metrics::new();
        let publisher = Publisher::new(metrics, registry);
        publisher.publish_repo_index(RepoIndex {
            generated_at: Some(chrono::Utc::now()),
            repos: vec![
                RepoIndexEntry {
                    owner: "fredsystems".to_owned(),
                    name: "nixos".to_owned(),
                    description: Some("the fleet".to_owned()),
                    archived: false,
                    pushed_at: None,
                },
                RepoIndexEntry {
                    owner: "fredclausen".to_owned(),
                    name: "old-thing".to_owned(),
                    description: None,
                    archived: true,
                    pushed_at: None,
                },
            ],
        });

        let response = router(publisher)
            .oneshot(
                Request::builder()
                    .uri("/repos.json")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert!(
            content_type.contains("application/json"),
            "got {content_type}"
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let parsed: serde_json::Value = serde_json::from_slice(&body).expect("valid json");

        let repos = parsed["repos"].as_array().expect("repos array");
        assert_eq!(repos.len(), 2);
        assert_eq!(repos[0]["owner"], "fredsystems");
        assert_eq!(repos[0]["description"], "the fleet");
        assert_eq!(repos[1]["archived"], true);
        // A description-less repository omits the key rather than sending
        // `null`, so the client has exactly one absent-check to write.
        assert!(
            repos[1].get("description").is_none(),
            "an absent description must not serialise as null"
        );
        assert!(parsed["generated_at"].is_string());
    }

    #[tokio::test]
    async fn repos_endpoint_is_valid_before_the_first_sweep() {
        // Nginx proxies this, and the page fetches it on load; an exporter
        // that has just restarted must answer with a parseable empty index
        // rather than a 404 or a half-written body.
        let (metrics, registry) = Metrics::new();

        let response = router(Publisher::new(metrics, registry))
            .oneshot(
                Request::builder()
                    .uri("/repos.json")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let parsed: serde_json::Value = serde_json::from_slice(&body).expect("valid json");

        assert!(parsed["repos"].as_array().expect("repos array").is_empty());
        assert!(parsed["generated_at"].is_null());
    }

    #[tokio::test]
    async fn health_endpoint_responds() {
        let (metrics, registry) = Metrics::new();
        let app = router(Publisher::new(metrics, registry));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }
}
