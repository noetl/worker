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
}

impl CommandExecutor {
    /// Create a new command executor.
    pub fn new(
        client: ControlPlaneClient,
        worker_id: String,
        server_url: String,
        snowflake: Arc<SnowflakeGen>,
    ) -> Self {
        Self {
            tool_registry: create_default_registry(),
            case_evaluator: CaseEvaluator::new(),
            client,
            worker_id,
            server_url,
            snowflake,
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
                // Emit call.done event
                self.emit_event(
                    "call.done",
                    &command.step,
                    "COMPLETED",
                    command.execution_id,
                    command.attempts,
                    serde_json::json!({
                        "command_id": command.command_id.clone(),
                        "call_index": ctx.call_index,
                        "result": result,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_executor_creation() {
        let client = ControlPlaneClient::new("http://localhost:8082");
        let snowflake = Arc::new(SnowflakeGen::with_node_and_epoch(1, 0));
        let executor = CommandExecutor::new(
            client,
            "worker-1".to_string(),
            "http://localhost:8082".to_string(),
            snowflake,
        );

        // Verify tools are registered
        assert!(executor.tool_registry.has("shell"));
        assert!(executor.tool_registry.has("http"));
        assert!(executor.tool_registry.has("rhai"));
    }
}
