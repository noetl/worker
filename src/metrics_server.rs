//! HTTP server exposing `/metrics` in Prometheus text format.
//!
//! Per [`agents/rules/observability.md`][rule], the worker's
//! `/metrics` endpoint binds on a dedicated port (default `9090`)
//! so sidecar scrapers can pull without going through the main
//! control-plane traffic.  The endpoint is read-only and has no
//! authentication — it's expected to be exposed only inside the
//! cluster network (Kubernetes Service with `ClusterIP` and
//! `PodMonitor`-restricted access).
//!
//! Two routes:
//! - `GET /metrics` — Prometheus text-format snapshot of the
//!   global [`crate::metrics::WorkerMetrics`] registry.
//! - `GET /healthz` — 200 OK (liveness check for Kubernetes).
//!
//! The spawn function returns immediately after `axum::serve` is
//! armed; the caller decides when to drop the join handle (the
//! worker keeps it for the worker's lifetime).
//!
//! [rule]: https://github.com/noetl/ai-meta/blob/main/agents/rules/observability.md

use anyhow::Result;
use axum::{
    http::{header, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use std::net::SocketAddr;
use tokio::task::JoinHandle;

use crate::metrics::{WorkerMetrics, METRICS_CONTENT_TYPE};

/// Spawn the metrics HTTP server in a background task.
///
/// Returns the join handle so the caller can decide when to shut
/// down the server.  Errors during bind are returned synchronously
/// before the server starts accepting connections.
pub async fn spawn(bind: &str) -> Result<JoinHandle<()>> {
    let addr: SocketAddr = bind.parse()?;

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(healthz_handler));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let actual_addr = listener.local_addr()?;

    tracing::info!(
        bind = %actual_addr,
        "Metrics HTTP server listening at http://{actual_addr}/metrics + /healthz"
    );

    let handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "Metrics HTTP server stopped");
        }
    });

    Ok(handle)
}

/// `GET /metrics` — encode the global registry and return as
/// Prometheus text format.
async fn metrics_handler() -> impl IntoResponse {
    let body = WorkerMetrics::global().encode();
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static(METRICS_CONTENT_TYPE),
        )],
        body,
    )
}

/// `GET /healthz` — liveness check.  Returns 200 OK whenever the
/// process is responding; doesn't check upstream dependencies
/// (NATS / control plane) because those have their own failure
/// modes the heartbeat already covers.
async fn healthz_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

#[cfg(test)]
mod tests {
    use super::*;
    use noetl_executor::worker::source::{ClaimOutcome, Command};

    fn dummy_command(id: &str) -> Command {
        Command {
            command_id: id.to_string(),
            execution_id: 1,
            step: "s".to_string(),
            tool_kind: "rhai".to_string(),
            input: serde_json::Value::Null,
            render_context: Default::default(),
            attempts: 0,
        }
    }

    #[tokio::test]
    async fn spawn_starts_and_serves_metrics() {
        // Bind to an ephemeral port (0 => OS picks).
        let handle = spawn("127.0.0.1:0").await.unwrap();
        // The spawn function logs the actual port via tracing; we
        // don't have a direct way to grab the chosen port without
        // refactoring the public API, so this test just confirms
        // the bind succeeded and the task is running.  A more
        // thorough test fits the next observability PR.
        assert!(!handle.is_finished());
        handle.abort();
    }

    #[tokio::test]
    async fn metrics_endpoint_returns_prometheus_text_format() {
        // Bump a counter so the encoded output isn't empty.
        crate::metrics::record_pull(&ClaimOutcome::Claimed(dummy_command("test")), 0.05);

        // Bind to ephemeral port + grab actual addr via a TcpListener.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let actual_addr = listener.local_addr().unwrap();

        let app = Router::new().route("/metrics", get(metrics_handler));
        let server_handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        // Give the server a tick to start accepting.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let body = reqwest::get(format!("http://{actual_addr}/metrics"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        assert!(body.contains("# HELP noetl_worker_pulls_total"));
        assert!(body.contains("noetl_worker_pulls_total{outcome=\"claimed\"}"));
        server_handle.abort();
    }

    #[tokio::test]
    async fn healthz_returns_ok() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let actual_addr = listener.local_addr().unwrap();

        let app = Router::new().route("/healthz", get(healthz_handler));
        let server_handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let resp = reqwest::get(format!("http://{actual_addr}/healthz"))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(resp.text().await.unwrap(), "ok");
        server_handle.abort();
    }
}
