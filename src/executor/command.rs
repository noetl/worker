//! Command executor.
//!
//! R-1.2 PR-2d-2: `CommandExecutor::execute` now takes
//! `&noetl_executor::worker::source::Command` (the executor crate's
//! enriched Command, 0.3.0+) instead of the worker's local
//! `crate::client::Command`.  Field accesses:
//!
//! - `command.execution_id` (i64) — same as before.
//! - `command.step` (String) — was `command.step()` accessor returning `&node_name`.
//! - `command.command_id` (String) — was `command.command_id()`.
//! - `command.tool_kind` (String) — was `command.action`.
//! - `command.render_context` (HashMap) — was `command.render_context()`.
//! - `command.attempts` (u32) — new in 0.3.0; useful for retry decisions.
//! - `command.input` (Value) — carries the worker's full `context` JSON
//!   including `tool_config`, `cases`, `args`, and `render_context`
//!   (the dedicated field is also populated for direct access).
//!   `tool_config` is extracted via `command.input.get("tool_config")`;
//!   `cases` via `command.input.get("cases")`.
//!
//! Per `nats::source::NatsCommandSource::translate`, the executor's
//! Command is a lossless mapping of the worker's Command.

use anyhow::Result;
use noetl_arrow_cache::ArrowIpcSharedMemoryCache;
use noetl_executor::worker::source::Command;
use noetl_tools::context::ExecutionContext;
use noetl_tools::registry::{ToolConfig, ToolRegistry};
use noetl_tools::tools::create_default_registry;
use std::sync::Arc;

use crate::client::{ControlPlaneClient, ExecutorEvent};
use crate::executor::case_evaluator::{CaseAction, CaseEvaluator};
use crate::snowflake::SnowflakeGen;

/// Command executor that runs tools and evaluates cases.
pub struct CommandExecutor {
    /// Tool registry with all available tools.
    tool_registry: ToolRegistry,

    /// Case evaluator for when/then logic.
    case_evaluator: CaseEvaluator,

    /// Control plane client for event emission.
    client: ControlPlaneClient,

    /// Worker ID.
    worker_id: String,

    /// Control-plane base URL.
    server_url: String,

    /// Application-side snowflake generator for `event_id` on every
    /// emitted envelope.  Per `observability.md` Principle 3 — the
    /// id is generated BEFORE the row hits the database so spans /
    /// metrics carry it at span-creation time and retries stay
    /// idempotent.
    snowflake: Arc<SnowflakeGen>,

    /// Same-node Arrow IPC cache for `call.done` results that
    /// exceed the broker's 100KB inline budget.  When a tool
    /// returns a large output (Postgres rowset, HTTP API
    /// response, etc.), the bytes go into shared memory + the
    /// event payload carries an `IpcHint` reference instead.
    /// Per R-2.1 (the `noetl-arrow-cache` crate); partial progress
    /// on noetl/worker#24.
    arrow_cache: Arc<ArrowIpcSharedMemoryCache>,
}

impl CommandExecutor {
    /// Create a new command executor.
    pub fn new(
        client: ControlPlaneClient,
        worker_id: String,
        server_url: String,
        snowflake: Arc<SnowflakeGen>,
        arrow_cache: Arc<ArrowIpcSharedMemoryCache>,
    ) -> Self {
        Self {
            tool_registry: create_default_registry(),
            case_evaluator: CaseEvaluator::new(),
            client,
            worker_id,
            server_url,
            snowflake,
            arrow_cache,
        }
    }

    /// Execute a command.
    ///
    /// Per `observability.md` Principle 1: every boundary call
    /// ships a span.  The `command.execute` span covers the full
    /// dispatch path (tool registry lookup, tool execution, case
    /// evaluation, lifecycle event emission) so downstream
    /// observability tooling (traces, metrics exemplars) can group
    /// every sub-operation under one execution.
    ///
    /// Principle 2 (metrics over logs): dispatch duration recorded
    /// to `noetl_worker_dispatch_duration_seconds{tool_kind=...}`;
    /// errors to `noetl_worker_dispatch_errors_total{tool_kind=...}`.
    /// Both labeled by tool_kind so the dashboard can spot which
    /// tools are slow / failing.
    pub async fn execute(&self, command: &Command) -> Result<()> {
        let span = tracing::info_span!(
            "command.execute",
            execution_id = command.execution_id,
            command_id = %command.command_id,
            step = %command.step,
            tool_kind = %command.tool_kind,
            attempts = command.attempts,
        );
        let _enter = span.enter();

        // Timer captures the full dispatch latency including tool
        // execution + case evaluation + lifecycle events.  Recorded
        // on every exit path (success + error) so the histogram is
        // complete.
        let dispatch_start = std::time::Instant::now();
        let tool_kind = command.tool_kind.clone();
        // Helper to record the dispatch metric on every exit path.
        // Captured by the error-return + success-return code below.
        let record_metric = |error: bool| {
            crate::metrics::record_dispatch(
                &tool_kind,
                dispatch_start.elapsed().as_secs_f64(),
                error,
            );
        };

        // Build execution context
        let mut ctx = ExecutionContext::new(command.execution_id, &command.step, &self.server_url)
            .with_worker_id(&self.worker_id)
            .with_command_id(&command.command_id);

        // Add render context variables from command payload.
        ctx.variables = command.render_context.clone();
        ctx.variables
            .entry("action".to_string())
            .or_insert_with(|| serde_json::json!(command.tool_kind));
        ctx.variables
            .entry("node_name".to_string())
            .or_insert_with(|| serde_json::json!(command.step.clone()));

        // Emit command.started event.  R-1.2 PR-EE-3: `step` +
        // `worker_id` are top-level fields on the `ExecutorEvent`
        // shape, so the context payload only carries the
        // command-specific keys.  The server's `EventRequest` /
        // Python's `EventEmitRequest` both read `step` /
        // `worker_id` from the top level after EE-2 + EE-4.
        self.emit_event(
            "command.started",
            &command.step,
            "STARTED",
            command.execution_id,
            command.attempts,
            serde_json::json!({
                "command_id": command.command_id.clone(),
            }),
        )
        .await?;

        // Reconstruct the ToolConfig the noetl-tools registry expects.
        // `command.input` is the worker's full `context` JSON; the
        // tool-side config lives under `input.tool_config`.  Inject
        // `kind` from the executor `Command.tool_kind` field if the
        // nested config doesn't already carry it (mirrors the worker's
        // pre-PR-2d-2 `Command.tool_config_value()` behaviour).
        let tool_config_value = {
            let mut cfg = command
                .input
                .get("tool_config")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            if !cfg.is_object() {
                cfg = serde_json::json!({});
            }
            if let Some(map) = cfg.as_object_mut() {
                map.entry("kind".to_string())
                    .or_insert_with(|| serde_json::json!(command.tool_kind));
                if !map.contains_key("args") {
                    if let Some(args) = command.input.get("args") {
                        map.insert("args".to_string(), args.clone());
                    }
                }
            }
            cfg
        };
        let tool_config: ToolConfig = serde_json::from_value(tool_config_value)?;

        tracing::debug!(
            execution_id = command.execution_id,
            step = %command.step,
            tool = %tool_config.kind,
            attempts = command.attempts,
            "Executing tool"
        );

        // Execute the tool
        let tool_result = match self
            .tool_registry
            .execute_from_config(&tool_config, &ctx)
            .await
        {
            Ok(result) => {
                // Emit call.done event with a reference-only result
                // payload.  The Python broker's
                // `_validate_reference_only_payload` enforces that
                // `payload.result` only carries `{status, reference,
                // context, command_id}` at the top level — the raw
                // tool fields (`stdout` / `stderr` / `exit_code` /
                // `data` / `duration_ms`) live INSIDE `context` so
                // downstream steps can reference them via Jinja
                // (`step_name.data.rows[N].x`).
                //
                // For results under `NOETL_EVENT_RESULT_CONTEXT_MAX_BYTES`
                // (default 100 KB) the broker persists `context` as-is
                // and downstream Jinja templates can read the tool
                // output.  When the JSON would exceed that, the broker
                // silently drops the context (`_bounded_context`
                // returns None), so we pre-check the size on the Rust
                // side and emit a WARN log so operators can see *why*
                // their large-result step's downstream rendering is
                // empty.  Until the result-store / `noetl-arrow-cache`
                // reference path lands (noetl/worker#24), an
                // over-budget result still ships with just `{status}` —
                // identical behaviour to a silent broker drop, just
                // visible in the worker's logs.
                //
                // Defensive: the broker forbids `_internal_data` at
                // any depth in `result.context`.  Our `ToolResult`
                // doesn't surface that key, so the serialised value
                // round-trips cleanly through the validator.
                let result_context = serde_json::to_value(&result)
                    .unwrap_or_else(|_| serde_json::json!({ "status": result.status.to_string() }));
                let result_obj = match build_call_done_result(
                    &result_context,
                    &result.status.to_string(),
                    command.execution_id,
                    &command.step,
                    self.arrow_cache.as_ref(),
                ) {
                    Ok(obj) => obj,
                    Err(e) => {
                        tracing::warn!(
                            execution_id = command.execution_id,
                            step = %command.step,
                            error = %e,
                            "Failed to serialise tool result for inline context; falling back to status-only payload",
                        );
                        serde_json::json!({ "status": result.status.to_string() })
                    }
                };
                self.emit_event(
                    "call.done",
                    &command.step,
                    "COMPLETED",
                    command.execution_id,
                    command.attempts,
                    serde_json::json!({
                        "command_id": command.command_id.clone(),
                        "call_index": ctx.call_index,
                        "result": result_obj,
                    }),
                )
                .await?;

                result
            }
            Err(e) => {
                // Emit call.error event
                self.emit_event(
                    "call.error",
                    &command.step,
                    "FAILED",
                    command.execution_id,
                    command.attempts,
                    serde_json::json!({
                        "command_id": command.command_id.clone(),
                        "call_index": ctx.call_index,
                        "error": e.to_string(),
                    }),
                )
                .await?;

                // Emit command.failed event
                self.emit_event(
                    "command.failed",
                    &command.step,
                    "FAILED",
                    command.execution_id,
                    command.attempts,
                    serde_json::json!({
                        "command_id": command.command_id.clone(),
                        "error": e.to_string(),
                    }),
                )
                .await?;

                record_metric(true);
                return Err(e.into());
            }
        };

        // Parse cases from command.  The executor's `Command.input`
        // carries the worker's full `context` JSON, so `cases`
        // lives at `command.input.cases` (was `command.context.cases`
        // pre-PR-2d-2).
        let cases: Vec<crate::executor::case_evaluator::Case> = command
            .input
            .get("cases")
            .and_then(|v| v.as_array())
            .map(|list| {
                list.iter()
                    .filter_map(|value| serde_json::from_value(value.clone()).ok())
                    .collect()
            })
            .unwrap_or_default();

        // Evaluate cases
        if !cases.is_empty() {
            if let Some(case_result) =
                self.case_evaluator
                    .evaluate(&cases, &ctx, tool_result.data.as_ref())?
            {
                match case_result.action {
                    CaseAction::Exit { status, data } => {
                        // Emit step.exit event.  `step` is top-level
                        // on the EE shape; the case's status string
                        // becomes the envelope status so the projector
                        // sees the actual case outcome.
                        let exit_status = status.clone();
                        self.emit_event(
                            "step.exit",
                            &command.step,
                            &exit_status,
                            command.execution_id,
                            command.attempts,
                            serde_json::json!({
                                "status": status,
                                "data": data,
                            }),
                        )
                        .await?;
                    }
                    CaseAction::SetVar { name, value } => {
                        // Set variable via API
                        self.client
                            .set_variable(command.execution_id, &name, value)
                            .await?;
                    }
                    CaseAction::Fail { message } => {
                        // Emit command.failed event
                        self.emit_event(
                            "command.failed",
                            &command.step,
                            "FAILED",
                            command.execution_id,
                            command.attempts,
                            serde_json::json!({
                                "command_id": command.command_id.clone(),
                                "error": message,
                            }),
                        )
                        .await?;

                        record_metric(true);
                        return Err(anyhow::anyhow!("Case evaluation failed: {}", message));
                    }
                    CaseAction::Continue | CaseAction::Goto { .. } | CaseAction::Retry { .. } => {
                        // These are handled by the orchestrator
                    }
                }
            }
        }

        // Emit command.completed event.  The tool's terminal status
        // (e.g. `"success"` / `"failure"` from the tool registry)
        // becomes the envelope status — projectors group by status
        // to compute success/failure rates per step.
        let completion_status = tool_result.status.to_string();
        self.emit_event(
            "command.completed",
            &command.step,
            &completion_status,
            command.execution_id,
            command.attempts,
            serde_json::json!({
                "command_id": command.command_id.clone(),
                "status": tool_result.status.to_string(),
            }),
        )
        .await?;

        record_metric(false);
        Ok(())
    }

    /// Emit an event to the control plane.
    ///
    /// R-1.2 PR-EE-3: constructs the shared `ExecutorEvent` shape
    /// (`step` + `status` + `created_at` + `context` at the top
    /// level, plus `worker_id` from the executor's own id).
    ///
    /// Post-EE-3 follow-ups now folded in:
    ///
    /// - `event_id` is stamped from the application-side snowflake
    ///   generator per `observability.md` Principle 3 — the id
    ///   exists at span-creation time + survives retries (which
    ///   used to either create duplicate rows or leave a NULL id
    ///   window).  Closes noetl/worker#12.
    /// - `meta.attempts` carries the executor `Command.attempts`
    ///   counter so retry behaviour rides the event log without
    ///   needing to reach back into the worker's logs.  Closes
    ///   noetl/worker#13.
    ///
    /// Per `observability.md` Principle 2: records the emit latency
    /// to `noetl_worker_event_emit_duration_seconds{event_type=...}`.
    /// The retries counter is incremented only when the underlying
    /// `emit_event_with_retry` actually retried (i.e. the first
    /// attempt failed); the retry count is currently not exposed
    /// by the client, so this MVP records 0 — a follow-up will
    /// thread the actual retry count back from the client.
    async fn emit_event(
        &self,
        event_type: &str,
        step: &str,
        status: &str,
        execution_id: i64,
        attempts: u32,
        context: serde_json::Value,
    ) -> Result<()> {
        let event = ExecutorEvent {
            execution_id,
            event_type: event_type.to_string(),
            step: step.to_string(),
            status: status.to_string(),
            created_at: chrono::Utc::now(),
            context,
            event_id: Some(self.snowflake.next_id()),
            worker_id: Some(self.worker_id.clone()),
            meta: Some(serde_json::json!({ "attempts": attempts })),
        };

        let emit_start = std::time::Instant::now();
        let result = self.client.emit_event_with_retry(event, 3).await;
        crate::metrics::record_event_emit(event_type, emit_start.elapsed().as_secs_f64(), 0);
        result
    }
}

/// Soft upper bound for the JSON-serialised size of
/// `payload.result.context` on `call.done` events.  Matches the
/// Python broker's `NOETL_EVENT_RESULT_CONTEXT_MAX_BYTES` default
/// (the broker's `_bounded_context` returns None and silently
/// drops the field above this threshold; we pre-check Rust-side
/// so operators see a WARN log instead of a silent drop).
const INLINE_CONTEXT_MAX_BYTES: usize = 100 * 1024;

/// Build the `payload.result` object for a `call.done` event,
/// choosing between the inline-context fast path and the
/// `{status}`-only fallback based on the JSON-serialised size of
/// the supplied `context`.
///
/// Returns:
///
/// - `{status, context}` when the serialised context fits under
///   [`INLINE_CONTEXT_MAX_BYTES`].
/// - `{status, reference}` when the context exceeds the inline
///   budget — the bytes go into the `noetl-arrow-cache` shared-
///   memory region + the returned `IpcHint` rides the event as
///   `result.reference`.  Same-node consumers attach via
///   `cache.get(&hint)` and decode based on `hint.media_type`
///   (`application/json` here; future tabular tools will emit
///   `application/vnd.apache.arrow.stream`).
/// - `{status}` only when the context exceeds the budget AND
///   the cache `put` fails (out of space, name collision, etc.).
///   Predictable + visible fallback rather than a silent broker
///   drop.
///
/// Errors only if the serde serialisation itself fails (which
/// shouldn't happen for `serde_json::Value` inputs but the
/// signature stays honest via `serde_json::Error`).
fn build_call_done_result(
    context: &serde_json::Value,
    status: &str,
    execution_id: i64,
    step: &str,
    arrow_cache: &ArrowIpcSharedMemoryCache,
) -> Result<serde_json::Value, serde_json::Error> {
    let serialised = serde_json::to_string(context)?;
    if serialised.len() <= INLINE_CONTEXT_MAX_BYTES {
        return Ok(serde_json::json!({ "status": status, "context": context }));
    }

    // Over-budget: stage the bytes in shared memory + emit a
    // reference.  Use a fixed `schema_digest` of `"json"` since
    // these are opaque JSON bytes, not Arrow IPC streams (the
    // cache's `put_arrow_ipc` API accepts arbitrary bytes; the
    // `schema_digest` field is for consumer-side schema-drift
    // detection and is meaningless for JSON content).  The
    // returned `IpcHint` carries the matching `media_type =
    // application/json` so the consumer knows how to decode.
    let put_result = arrow_cache.put_arrow_ipc(serialised.as_bytes(), "json", None, None);

    match put_result {
        Ok(mut hint) => {
            // Override the default Arrow media type with the JSON
            // marker so consumers know to JSON-decode the bytes.
            // Tabular tool outputs (DuckDB, Postgres) will keep
            // the default Arrow media type once they encode via
            // `noetl_tools::arrow_codec::encode_record_batch`.
            hint.media_type = "application/json".to_string();
            tracing::info!(
                execution_id,
                step,
                context_bytes = serialised.len(),
                shm_name = %hint.shm_name,
                "Tool result exceeds inline context budget; staged in shared-memory cache.",
            );
            // Serialise the hint to a JSON object so it slots
            // into `result.reference` per the broker contract
            // (`reference` MUST be a dict).
            let reference = serde_json::to_value(&hint).unwrap_or_else(|_| {
                serde_json::json!({
                    "kind": "arrow_ipc",
                    "shm_name": hint.shm_name.clone(),
                    "byte_length": hint.byte_length,
                    "media_type": "application/json",
                })
            });
            Ok(serde_json::json!({ "status": status, "reference": reference }))
        }
        Err(e) => {
            tracing::warn!(
                execution_id,
                step,
                context_bytes = serialised.len(),
                inline_budget_bytes = INLINE_CONTEXT_MAX_BYTES,
                error = %e,
                "Tool result exceeds inline context budget and shared-memory cache put failed; emitting status-only result. \
                 Downstream Jinja references will be empty.  Track at noetl/worker#24.",
            );
            Ok(serde_json::json!({ "status": status }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noetl_arrow_cache::CacheConfig;

    /// Build a test-isolated cache with a unique namespace so
    /// concurrent test runs don't collide on shm names.  POSIX shm
    /// names are filesystem-global; the namespace is the prefix
    /// stamped onto every entry the cache produces.
    fn test_cache(namespace: &str) -> Arc<ArrowIpcSharedMemoryCache> {
        let config = CacheConfig {
            namespace: namespace.to_string(),
            budget_bytes: 8 * 1024 * 1024,
            default_lease_seconds: 60.0,
            producer: "test".to_string(),
            node_id: "test-node".to_string(),
        };
        Arc::new(ArrowIpcSharedMemoryCache::with_config(config))
    }

    #[test]
    fn test_command_executor_creation() {
        let client = ControlPlaneClient::new("http://localhost:8082");
        let snowflake = Arc::new(SnowflakeGen::with_node_and_epoch(1, 0));
        let cache = test_cache("wkr-test-ctor");
        let executor = CommandExecutor::new(
            client,
            "worker-1".to_string(),
            "http://localhost:8082".to_string(),
            snowflake,
            cache,
        );

        // Verify tools are registered
        assert!(executor.tool_registry.has("shell"));
        assert!(executor.tool_registry.has("http"));
        assert!(executor.tool_registry.has("rhai"));
    }

    /// Small tool result rides the inline `result.context` path —
    /// the broker accepts it and downstream Jinja templates can
    /// reference fields off it.
    #[test]
    fn build_call_done_result_inlines_small_context() {
        let cache = test_cache("wkr-test-small");
        let context = serde_json::json!({
            "stdout": "hello",
            "exit_code": 0,
            "duration_ms": 12,
        });
        let result =
            build_call_done_result(&context, "COMPLETED", 42, "greet", cache.as_ref()).unwrap();
        assert_eq!(result["status"], "COMPLETED");
        assert_eq!(result["context"]["stdout"], "hello");
        assert_eq!(result["context"]["exit_code"], 0);
        // The structure stays valid against the broker's
        // _STRICT_RESULT_ALLOWED_KEYS = {status, reference,
        // context, command_id} contract.
        let result_keys: Vec<&str> = result
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        for key in &result_keys {
            assert!(
                ["status", "context"].contains(key),
                "unexpected key: {}",
                key,
            );
        }
    }

    /// Large tool result exceeds the inline budget — gets staged in
    /// the same-node shared-memory cache and emitted as
    /// `result.reference` (an `IpcHint` dict).  The bytes are
    /// recoverable by colocated consumers via `cache.get(&hint)`.
    /// This replaces the pre-R-2.1 silent-drop behaviour.
    #[test]
    fn build_call_done_result_stages_oversized_context_via_cache() {
        let cache = test_cache("wkr-test-big");
        let big_string: String = "x".repeat(INLINE_CONTEXT_MAX_BYTES + 1024);
        let context = serde_json::json!({ "stdout": big_string });
        let result =
            build_call_done_result(&context, "COMPLETED", 42, "big_step", cache.as_ref()).unwrap();
        assert_eq!(result["status"], "COMPLETED");
        // Over-budget context must NOT ride inline as `context`...
        assert!(
            result.get("context").is_none(),
            "oversize context must not ride inline: result={}",
            result
        );
        // ...instead it must surface as `result.reference` carrying
        // the `IpcHint` fields the broker contract expects.
        let reference = result
            .get("reference")
            .expect("oversize context must surface as result.reference");
        assert!(reference.is_object(), "reference must be a dict");
        assert_eq!(reference["media_type"], "application/json");
        assert!(
            reference["shm_name"].is_string(),
            "reference.shm_name must be a string"
        );
        assert!(
            reference["byte_length"].as_u64().is_some(),
            "reference.byte_length must be a u64"
        );
        // The cache must now hold exactly one entry — the staged
        // bytes — and `used_bytes` should be > the inline budget.
        assert!(
            cache.used_bytes() > INLINE_CONTEXT_MAX_BYTES as u64,
            "cache must hold the staged bytes: used_bytes={}",
            cache.used_bytes()
        );
    }

    /// The inline-budget threshold is a constant the broker side
    /// is tied to (`NOETL_EVENT_RESULT_CONTEXT_MAX_BYTES` default
    /// 102400 bytes).  Lock the value in so a future tweak to
    /// either side stays in sync.
    #[test]
    fn inline_context_max_bytes_matches_broker_default() {
        assert_eq!(INLINE_CONTEXT_MAX_BYTES, 102_400);
    }

    /// Result sized exactly at the budget is allowed; one byte
    /// over is not.  Boundary check for the comparison — and a
    /// regression guard that the over-budget path takes the
    /// reference branch rather than dropping the context entirely.
    #[test]
    fn build_call_done_result_boundary_check() {
        let cache = test_cache("wkr-test-bound");
        // We can't easily craft a context whose JSON encoding is
        // EXACTLY INLINE_CONTEXT_MAX_BYTES, but we can prove the
        // ">" (strictly greater) semantics by checking a result
        // smaller and a result larger than the threshold.
        let small = serde_json::json!({ "x": "a".repeat(INLINE_CONTEXT_MAX_BYTES - 100) });
        let small_result =
            build_call_done_result(&small, "COMPLETED", 1, "s", cache.as_ref()).unwrap();
        assert!(small_result.get("context").is_some());
        assert!(small_result.get("reference").is_none());

        let large = serde_json::json!({ "x": "a".repeat(INLINE_CONTEXT_MAX_BYTES + 100) });
        let large_result =
            build_call_done_result(&large, "COMPLETED", 1, "l", cache.as_ref()).unwrap();
        assert!(large_result.get("context").is_none());
        assert!(large_result.get("reference").is_some());
    }
}
