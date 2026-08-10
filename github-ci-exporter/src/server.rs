//! The `/metrics` HTTP endpoint.

use axum::{Router, extract::State, http::header, response::IntoResponse, routing::get};
use tracing::info;

use crate::metrics::SharedRegistry;

/// Builds the exporter's HTTP router.
pub fn router(registry: SharedRegistry) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/", get(index_handler))
        .with_state(registry)
}

async fn metrics_handler(State(registry): State<SharedRegistry>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        registry.render(),
    )
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
         <p><a href=\"/metrics\">/metrics</a></p></body></html>",
    )
}

/// Serves the exporter until `shutdown` resolves.
///
/// # Errors
/// Returns an error if the listener cannot bind or the server fails.
pub async fn serve(
    listen: std::net::SocketAddr,
    registry: SharedRegistry,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(listen).await?;
    info!(%listen, "serving metrics");
    axum::serve(listener, router(registry))
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
    use crate::metrics::Metrics;

    #[tokio::test]
    async fn metrics_endpoint_serves_openmetrics() {
        let (metrics, registry) = Metrics::new();
        metrics.scrape_success.set(1);
        let app = router(SharedRegistry::new(registry));

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
    async fn health_endpoint_responds() {
        let (_, registry) = Metrics::new();
        let app = router(SharedRegistry::new(registry));

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
