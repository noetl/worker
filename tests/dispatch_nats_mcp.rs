//! Integration tests for `tool_kind = "nats"` and `tool_kind = "mcp"` dispatch.
//!
//! These tests confirm that:
//!
//! 1. The `noetl-tools` registry (via `create_default_registry`) registers
//!    both `"nats"` and `"mcp"` tool kinds after the 2.15.0 / 2.16.0 bumps.
//! 2. Dispatching a `ToolConfig { kind: "nats", ... }` reaches `NatsTool` and
//!    returns a result (live NATS server gated behind `NOETL_TEST_NATS_URL`).
//! 3. Dispatching a `ToolConfig { kind: "mcp", ... }` reaches `McpTool` and
//!    returns a result (live MCP server gated behind `NOETL_TEST_MCP_ENDPOINT`).
//! 4. `record_dispatch("nats", ...)` + `record_dispatch("mcp", ...)` surface
//!    the expected label series in the Prometheus text output (unit test,
//!    no live server required).
//!
//! ## Running the live-server tests
//!
//! ```sh
//! # NATS KV round-trip
//! NOETL_TEST_NATS_URL=nats://localhost:4222 cargo test --test dispatch_nats_mcp
//!
//! # MCP health probe
//! NOETL_TEST_MCP_ENDPOINT=http://localhost:8080/mcp cargo test --test dispatch_nats_mcp
//! ```
//!
//! CI runs without either env var; both live-server sections skip themselves.

use noetl_tools::context::ExecutionContext;
use noetl_tools::registry::{ToolConfig, ToolRegistry};
use noetl_tools::tools::create_default_registry;

// ---------------------------------------------------------------------------
// Registry registration smoke tests (always run; no live server)
// ---------------------------------------------------------------------------

/// Confirm the default registry includes "nats" after the 2.15.0 bump.
#[test]
fn registry_has_nats_tool_kind() {
    let registry: ToolRegistry = create_default_registry();
    assert!(
        registry.has("nats"),
        "create_default_registry() must register NatsTool (noetl-tools >= 2.15.0)"
    );
}

/// Confirm the default registry includes "mcp" after the 2.16.0 bump.
#[test]
fn registry_has_mcp_tool_kind() {
    let registry: ToolRegistry = create_default_registry();
    assert!(
        registry.has("mcp"),
        "create_default_registry() must register McpTool (noetl-tools >= 2.16.0)"
    );
}

/// Confirm pre-existing tool kinds are still registered (regression guard).
#[test]
fn registry_still_has_pre_existing_tool_kinds() {
    let registry: ToolRegistry = create_default_registry();
    for kind in &["shell", "http", "rhai", "result_fetch", "duckdb", "postgres"] {
        assert!(
            registry.has(kind),
            "create_default_registry() must still register '{kind}'"
        );
    }
}

// ---------------------------------------------------------------------------
// Metrics label tests (always run; no live server)
//
// Per `agents/rules/observability.md` Principle 1: every dispatch boundary
// records `noetl_worker_dispatch_duration_seconds{tool_kind=...}`.  These
// tests drive `record_dispatch` directly and verify the labels surface in the
// Prometheus text output — confirming that the `HistogramVec` accepts
// arbitrary `tool_kind` strings (the metric is dynamic; no per-variant arm).
// ---------------------------------------------------------------------------

/// `record_dispatch("nats", ...)` produces the expected label series.
#[test]
fn dispatch_duration_histogram_accepts_nats_label() {
    noetl_worker::metrics::record_dispatch("nats", 0.042, false);
    let text = String::from_utf8(noetl_worker::metrics::WorkerMetrics::global().encode()).unwrap();
    assert!(
        text.contains("noetl_worker_dispatch_duration_seconds"),
        "dispatch duration histogram must be present in /metrics output"
    );
    // The histogram emits `_sum`, `_count`, and `_bucket` lines for every
    // observed label.  Any of those confirms the label was accepted.
    assert!(
        text.contains("tool_kind=\"nats\""),
        "tool_kind=\"nats\" must appear in /metrics after record_dispatch(\"nats\", ...)"
    );
}

/// `record_dispatch("mcp", ...)` produces the expected label series.
#[test]
fn dispatch_duration_histogram_accepts_mcp_label() {
    noetl_worker::metrics::record_dispatch("mcp", 0.150, false);
    let text = String::from_utf8(noetl_worker::metrics::WorkerMetrics::global().encode()).unwrap();
    assert!(
        text.contains("tool_kind=\"mcp\""),
        "tool_kind=\"mcp\" must appear in /metrics after record_dispatch(\"mcp\", ...)"
    );
}

/// Error path: `record_dispatch("nats", ..., true)` bumps the errors counter.
#[test]
fn dispatch_errors_counter_accepts_nats_label() {
    let m = noetl_worker::metrics::WorkerMetrics::global();
    let before = m
        .dispatch_errors_total
        .with_label_values(&["nats"])
        .get();
    noetl_worker::metrics::record_dispatch("nats", 0.001, true);
    let after = m
        .dispatch_errors_total
        .with_label_values(&["nats"])
        .get();
    assert_eq!(after, before + 1, "error counter must increment for nats");
}

/// Error path: `record_dispatch("mcp", ..., true)` bumps the errors counter.
#[test]
fn dispatch_errors_counter_accepts_mcp_label() {
    let m = noetl_worker::metrics::WorkerMetrics::global();
    let before = m
        .dispatch_errors_total
        .with_label_values(&["mcp"])
        .get();
    noetl_worker::metrics::record_dispatch("mcp", 0.001, true);
    let after = m
        .dispatch_errors_total
        .with_label_values(&["mcp"])
        .get();
    assert_eq!(after, before + 1, "error counter must increment for mcp");
}

// ---------------------------------------------------------------------------
// Live-server integration tests (gated behind env vars)
// ---------------------------------------------------------------------------

/// NATS KV round-trip via the tool registry.
///
/// Set `NOETL_TEST_NATS_URL=nats://localhost:4222` to run.
/// Skips silently when the env var is absent (CI-safe).
#[tokio::test]
async fn nats_dispatch_kv_roundtrip_via_registry() {
    let nats_url = match std::env::var("NOETL_TEST_NATS_URL") {
        Ok(u) => u,
        Err(_) => return, // skip; no live NATS available
    };

    let registry: ToolRegistry = create_default_registry();
    let mut ctx = ExecutionContext::default();
    // Provide the NATS URL as a keychain credential so the tool can resolve it.
    ctx.set_secret(
        "test_nats_cred",
        format!(r#"{{"url":"{}"}}"#, nats_url),
    );

    // Use a unique bucket name to avoid cross-test collisions.
    let bucket = format!("noetl_wkr_test_{}", uuid::Uuid::new_v4().simple());

    // --- kv_put ---
    let put_cfg = ToolConfig {
        kind: "nats".to_string(),
        config: serde_json::json!({
            "auth":      "test_nats_cred",
            "operation": "kv_put",
            "bucket":    bucket,
            "key":       "greeting",
            "value":     "hello-from-worker",
            // Create the bucket on the fly if it does not exist.
            "create_bucket": true,
        }),
        timeout: Some(10),
        retry: None,
        auth: None,
    };
    let put_result = registry
        .execute_from_config(&put_cfg, &ctx)
        .await
        .expect("nats kv_put must succeed");
    assert!(
        put_result.is_success(),
        "kv_put should succeed: {:?}",
        put_result
    );

    // --- kv_get ---
    let get_cfg = ToolConfig {
        kind: "nats".to_string(),
        config: serde_json::json!({
            "auth":      "test_nats_cred",
            "operation": "kv_get",
            "bucket":    bucket,
            "key":       "greeting",
        }),
        timeout: Some(10),
        retry: None,
        auth: None,
    };
    let get_result = registry
        .execute_from_config(&get_cfg, &ctx)
        .await
        .expect("nats kv_get must succeed");
    assert!(get_result.is_success(), "kv_get should succeed");
    let data = get_result.data.as_ref().expect("result must have data");
    assert_eq!(data["value"], "hello-from-worker");

    // Confirm metric label surfaced.
    let text =
        String::from_utf8(noetl_worker::metrics::WorkerMetrics::global().encode()).unwrap();
    assert!(
        text.contains("tool_kind=\"nats\""),
        "dispatch metric must surface nats label after live dispatch"
    );
}

/// MCP health probe via the tool registry.
///
/// Set `NOETL_TEST_MCP_ENDPOINT=http://localhost:8080/mcp` to run.
/// Skips silently when the env var is absent (CI-safe).
#[tokio::test]
async fn mcp_dispatch_health_probe_via_registry() {
    let endpoint = match std::env::var("NOETL_TEST_MCP_ENDPOINT") {
        Ok(ep) => ep,
        Err(_) => return, // skip; no live MCP server available
    };

    let registry: ToolRegistry = create_default_registry();
    let ctx = ExecutionContext::default();

    let cfg = ToolConfig {
        kind: "mcp".to_string(),
        config: serde_json::json!({
            "endpoint": endpoint,
            "server":   "test",
            "method":   "health",
            "timeout":  10,
        }),
        timeout: Some(15),
        retry: None,
        auth: None,
    };
    let result = registry
        .execute_from_config(&cfg, &ctx)
        .await
        .expect("mcp health probe must succeed");
    assert!(
        result.is_success(),
        "mcp health probe should succeed: {:?}",
        result
    );
    let data = result.data.as_ref().expect("result must have data");
    assert_eq!(data["status"], "ok");
    assert_eq!(data["method"], "health");

    // Confirm metric label surfaced.
    let text =
        String::from_utf8(noetl_worker::metrics::WorkerMetrics::global().encode()).unwrap();
    assert!(
        text.contains("tool_kind=\"mcp\""),
        "dispatch metric must surface mcp label after live dispatch"
    );
}
