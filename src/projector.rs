//! The `system/projector` drain loop — noetl/server#203 phase 2b-2.
//!
//! Folds the event feed and advances `noetl.projection_snapshot` via
//! `POST /api/internal/projection/advance`, so the projector owns the read model
//! the orchestrator reads instead of the orchestrator self-writing it.
//!
//! # ⚠ Two ways the issue's spec is stale, and what was built instead
//!
//! The issue describes "the `system/projector` **catalog playbook** — a
//! `noetl_events` **JetStream** batch consumer". Neither half matches reality:
//!
//! 1. Its sibling, phase 2d's `system/event_materializer`, is described in the
//!    identical words and is **not a playbook** — it is a worker background loop
//!    ([`crate::materializer`]), running on the system pool in kind AND prod.
//!    The only genuine `system/*` catalog artifact is `system/orchestrate`, and
//!    that is a WASM plug-in.
//! 2. ⚠⚠ **There is no `noetl_events` stream.** Queried directly: NATS carries
//!    exactly one stream, `NOETL_COMMANDS`. The materializer runs
//!    `NOETL_MATERIALIZER_SOURCE=ehdb` in both environments — the live transport
//!    is the EHDB bus.
//!
//! A projector written literally to spec would subscribe to a stream that is
//! never created and **silently process nothing**, which is the failure class
//! this codebase keeps paying for. So this mirrors the materializer: same source
//! abstraction, so it follows whatever transport is actually live.
//!
//! # Blast radius
//!
//! Zero until two flags are flipped. `NOETL_PROJECTOR_ENABLED` off ⇒ no loop
//! runs. `NOETL_PROJECTOR_OWNS_SNAPSHOT` off (server-side, its own default) ⇒
//! the orchestrator self-writes exactly as today.

use anyhow::{anyhow, Context, Result};
use std::collections::BTreeSet;
use std::time::Duration;

use crate::config::WorkerConfig;

/// How long a projector HTTP call may take before it is abandoned.
///
/// ⚠ Generous on purpose, and bounded on purpose. `advance` recomputes a
/// snapshot per execution, so a large batch is legitimately slow; a timeout
/// below that converts a working-but-slow projector into a failing one. But it
/// must be bounded — `reqwest::Client::new()` has no timeout at all, which is
/// the defect noetl/worker#324 fixed in this very crate.
const PROJECTOR_HTTP_TIMEOUT: Duration = Duration::from_secs(300);

/// True when `NOETL_PROJECTOR_ENABLED` is set to a truthy value.
pub fn enabled() -> bool {
    matches!(
        std::env::var("NOETL_PROJECTOR_ENABLED")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Resolved projector configuration.
#[derive(Debug, Clone)]
pub struct ProjectorConfig {
    pub server_url: String,
    pub internal_token: String,
    pub batch: u32,
    pub idle_sleep: Duration,
    pub error_backoff: Duration,
}

impl ProjectorConfig {
    /// Build from the worker config + env. `Ok(None)` when disabled; `Err` when
    /// enabled but missing a hard requirement, so a misconfigured system pool
    /// fails LOUD instead of silently not projecting — the same posture
    /// [`crate::materializer::MaterializerConfig::from_env`] takes.
    pub fn from_env(worker: &WorkerConfig) -> Result<Option<Self>> {
        if !enabled() {
            return Ok(None);
        }
        let internal_token = std::env::var("NOETL_INTERNAL_API_TOKEN")
            .ok()
            .filter(|t| !t.trim().is_empty())
            .context(
                "NOETL_PROJECTOR_ENABLED is set but NOETL_INTERNAL_API_TOKEN is empty — \
                 the projector needs it to call /api/internal/projection/advance",
            )?;
        Ok(Some(Self {
            server_url: worker.server_url.trim_end_matches('/').to_string(),
            internal_token,
            batch: env_u32("NOETL_PROJECTOR_BATCH", 64).clamp(1, 1000),
            idle_sleep: Duration::from_millis(env_u64("NOETL_PROJECTOR_IDLE_SLEEP_MS", 500)),
            error_backoff: Duration::from_millis(env_u64("NOETL_PROJECTOR_ERROR_BACKOFF_MS", 2000)),
        }))
    }
}

fn env_u32(k: &str, d: u32) -> u32 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(d)
}
fn env_u64(k: &str, d: u64) -> u64 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(d)
}

/// A bounded HTTP client for the projector.
fn projector_http() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(PROJECTOR_HTTP_TIMEOUT)
        .build()
        .map_err(|e| anyhow!("projector: could not build HTTP client: {e}"))
}

/// The distinct execution ids carried by a batch of events.
///
/// ⚠ DISTINCT is load-bearing. A batch routinely holds many events for one
/// execution, and `advance` recomputes that execution's whole snapshot — so
/// sending an id N times does the same expensive work N times for one result.
/// `BTreeSet` also makes the order deterministic, which keeps a failing batch
/// reproducible.
pub(crate) fn execution_ids(events: &[serde_json::Value]) -> Vec<i64> {
    let mut seen = BTreeSet::new();
    for e in events {
        let id = e.get("execution_id").and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        });
        if let Some(id) = id {
            seen.insert(id);
        }
    }
    seen.into_iter().collect()
}

/// Did an `advance` reply report every execution as advanced?
///
/// ⚠⚠ The endpoint returns **200 even when some executions failed** — it
/// collects them into `failed[]` rather than failing the batch. So a 2xx is NOT
/// a success signal, and acking on status alone would silently drop the failed
/// executions' work. Only a reply with an EMPTY `failed[]` may be acked.
///
/// ⚠ Acking only on full success admits redelivery of executions that already
/// advanced. That is safe: `advance` recomputes and saves a snapshot, so
/// applying it twice yields the same snapshot. Duplicated work, never divergence.
pub(crate) fn advance_fully_succeeded(reply: &serde_json::Value) -> bool {
    match reply.get("failed") {
        Some(serde_json::Value::Array(f)) => f.is_empty(),
        // A reply without the field is not evidence of success — an older or
        // unexpected shape must not be read as "nothing failed".
        _ => false,
    }
}

/// Spawn the projector loop.
pub fn spawn(config: ProjectorConfig) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = run_loop(config).await {
            tracing::error!(error = %e, "projector loop exited with error");
        }
    })
}

async fn run_loop(config: ProjectorConfig) -> Result<()> {
    // ⚠⚠ THE DRAIN IS NOT IMPLEMENTED, AND THIS FAILS LOUD RATHER THAN
    // PRETENDING TO RUN.
    //
    // An earlier version of this function was `loop { sleep }` after logging
    // "projector started". That is worse than nothing: with the flag on it
    // reports a healthy projector, advances no snapshot, and — once
    // NOETL_PROJECTOR_OWNS_SNAPSHOT is also on — the orchestrator STOPS
    // self-writing while nothing writes in its place. A silently-idle projector
    // is precisely the "clean result computed over zero rows" failure this
    // codebase keeps paying for.
    //
    // What IS decided and tested here: the enable flag (default off), the
    // config's fail-loud posture, distinct-execution-id extraction, and the
    // ack-only-on-zero-failures rule. What remains is wiring the EHDB source
    // drain + ack disposition, which needs the owner's answer on redelivery
    // policy (noetl/server#203 decision 3).
    let _ = (&config.batch, &config.internal_token, &config.error_backoff);
    let url = format!("{}/api/internal/projection/advance", config.server_url);
    let _client = projector_http()?;
    Err(anyhow!(
        "NOETL_PROJECTOR_ENABLED is set, but the projector drain is not \
         implemented (noetl/server#203 phase 2b-2). Refusing to run as a no-op: \
         a projector that reports healthy and advances nothing would let the \
         orchestrator stop self-writing the snapshot with nothing writing in its \
         place. Unset the flag, or finish the drain. Target endpoint: {url}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// ⭐ Default OFF — the whole blast-radius argument rests on this.
    #[test]
    fn the_projector_is_off_unless_deliberately_enabled() {
        if std::env::var("NOETL_PROJECTOR_ENABLED").is_ok() {
            return; // set in this environment; the default is not observable
        }
        assert!(
            !enabled(),
            "an unset NOETL_PROJECTOR_ENABLED must leave the projector inert, so \
             deploying this image cannot start projecting as a side effect"
        );
    }

    /// ⭐ Distinct ids, deterministic order.
    #[test]
    fn a_batch_yields_distinct_execution_ids() {
        let events = vec![
            json!({"execution_id": 3}),
            json!({"execution_id": 1}),
            json!({"execution_id": 3}),
            json!({"execution_id": "2"}),
            json!({"no_execution_id": true}),
        ];
        assert_eq!(
            execution_ids(&events),
            vec![1, 2, 3],
            "ids must be de-duplicated and ordered — `advance` recomputes a whole \
             snapshot per id, so a repeat is the same expensive work twice"
        );
        assert!(
            execution_ids(&[]).is_empty(),
            "an empty batch yields nothing"
        );
    }

    /// ⚠⚠ A 200 is not success. The endpoint collects per-execution failures.
    #[test]
    fn only_a_reply_with_no_failures_may_be_acked() {
        assert!(advance_fully_succeeded(
            &json!({"advanced": [1, 2], "failed": []})
        ));
        assert!(
            !advance_fully_succeeded(&json!({"advanced": [1], "failed": [{"execution_id": 2}]})),
            "a partially-failed batch must NOT be acked — the endpoint returns 200 \
             for it, so acking on status would silently drop the failed work"
        );
        assert!(
            !advance_fully_succeeded(&json!({"advanced": [1]})),
            "a reply with no `failed` field is not evidence of success"
        );
        assert!(
            !advance_fully_succeeded(&json!("nonsense")),
            "a non-object is not success"
        );
    }
}
