//! Event emitter with retry logic.
//!
//! R-1.2 PR-EE-3: this module emits the shared
//! `noetl_executor::events::ExecutorEvent` shape — `step`, `status`,
//! `created_at`, and `context` at the top level, plus the optional
//! `event_id`, `worker_id`, and `meta` fields — instead of the
//! worker-local `WorkerEvent` it shipped pre-EE.  See the
//! [event-envelope page][ee] on the noetl/server wiki for the
//! field-by-field contract.
//!
//! The retry loop is unchanged from PR-2e — it still records per-
//! attempt latency to the `noetl.performance` log target and the
//! Prometheus event-emit histogram.
//!
//! [ee]: https://github.com/noetl/server/wiki/event-envelope

use anyhow::Result;
use chrono::Utc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::client::{ControlPlaneClient, ExecutorEvent};
use crate::snowflake::SnowflakeGen;

/// Event emitter with automatic retry.
///
/// R-1.2 PR-EE-3: holds the emitting worker's id so each helper
/// can stamp `worker_id` on the outgoing envelope without forcing
/// every call site to thread it through (per `observability.md`
/// Principle 4 — every wire format carries the correlation key).
///
/// Post-EE-3 follow-up (this change): holds an `Arc<SnowflakeGen>`
/// so every emitted envelope carries an application-side `event_id`
/// per `observability.md` Principle 3 — the id exists at span-
/// creation time + retries are idempotent across the same logical
/// event without depending on the server's `gen_snowflake()` DB
/// default firing.  Closes [noetl/worker#12].
pub struct EventEmitter {
    client: ControlPlaneClient,
    worker_id: String,
    snowflake: Arc<SnowflakeGen>,
    max_retries: u32,
    initial_delay: Duration,
    max_delay: Duration,
}

impl EventEmitter {
    /// Create a new event emitter.
    pub fn new(
        client: ControlPlaneClient,
        worker_id: impl Into<String>,
        snowflake: Arc<SnowflakeGen>,
    ) -> Self {
        Self {
            client,
            worker_id: worker_id.into(),
            snowflake,
            max_retries: 3,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(10),
        }
    }

    /// Create an event emitter with custom retry settings.
    pub fn with_retry(
        client: ControlPlaneClient,
        worker_id: impl Into<String>,
        snowflake: Arc<SnowflakeGen>,
        max_retries: u32,
        initial_delay: Duration,
        max_delay: Duration,
    ) -> Self {
        Self {
            client,
            worker_id: worker_id.into(),
            snowflake,
            max_retries,
            initial_delay,
            max_delay,
        }
    }

    /// Build a fresh `ExecutorEvent` stamped with `created_at = now`,
    /// the emitter's `worker_id`, an application-side snowflake
    /// `event_id`, and the per-command `attempts` counter in
    /// `meta`.  Per `observability.md` Principle 3 the id exists
    /// at span-creation time + survives retries; per the EE-3
    /// follow-up [noetl/worker#13] retry behaviour rides the
    /// event log via `meta.attempts` so projectors don't need
    /// to reach back into the worker's logs.
    fn build_event(
        &self,
        event_type: &str,
        execution_id: i64,
        step: &str,
        status: &str,
        attempts: u32,
        context: serde_json::Value,
    ) -> ExecutorEvent {
        ExecutorEvent {
            execution_id,
            event_type: event_type.to_string(),
            step: step.to_string(),
            status: status.to_string(),
            created_at: Utc::now(),
            context,
            event_id: Some(self.snowflake.next_id()),
            worker_id: Some(self.worker_id.clone()),
            meta: Some(serde_json::json!({ "attempts": attempts })),
        }
    }

    /// Emit an event with retry.
    pub async fn emit(&self, event: ExecutorEvent) -> Result<()> {
        let emit_start = Instant::now();
        let mut delay = self.initial_delay;
        let mut total_retry_delay = Duration::ZERO;
        let mut retry_count = 0u32;

        for attempt in 0..=self.max_retries {
            match self.client.emit_event(event.clone()).await {
                Ok(()) => {
                    let total_duration = emit_start.elapsed();
                    // Log performance metrics on success
                    tracing::info!(
                        target: "noetl.performance",
                        execution_id = %event.execution_id,
                        event_type = %event.event_type,
                        step = %event.step,
                        status = %event.status,
                        phase = "event_emit",
                        duration_ms = %total_duration.as_millis(),
                        retry_count = %retry_count,
                        retry_delay_ms = %total_retry_delay.as_millis(),
                        "Event emitted successfully"
                    );
                    return Ok(());
                }
                Err(e) if attempt < self.max_retries => {
                    retry_count += 1;
                    tracing::warn!(
                        target: "noetl.performance",
                        attempt = attempt + 1,
                        max_retries = self.max_retries,
                        error = %e,
                        event_type = %event.event_type,
                        execution_id = %event.execution_id,
                        step = %event.step,
                        delay_ms = %delay.as_millis(),
                        "Event emission failed, retrying"
                    );
                    total_retry_delay += delay;
                    tokio::time::sleep(delay).await;
                    delay = std::cmp::min(delay * 2, self.max_delay);
                }
                Err(e) => {
                    let total_duration = emit_start.elapsed();
                    tracing::error!(
                        target: "noetl.performance",
                        event_type = %event.event_type,
                        execution_id = %event.execution_id,
                        step = %event.step,
                        error = %e,
                        duration_ms = %total_duration.as_millis(),
                        retry_count = %retry_count,
                        retry_delay_ms = %total_retry_delay.as_millis(),
                        "Event emission failed after all retries"
                    );
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Emit a `command.claimed` event.
    pub async fn emit_command_claimed(
        &self,
        execution_id: i64,
        step: &str,
        command_id: &str,
        attempts: u32,
    ) -> Result<()> {
        self.emit(self.build_event(
            "command.claimed",
            execution_id,
            step,
            "STARTED",
            attempts,
            serde_json::json!({ "command_id": command_id }),
        ))
        .await
    }

    /// Emit a `command.started` event.
    pub async fn emit_command_started(
        &self,
        execution_id: i64,
        step: &str,
        command_id: &str,
        attempts: u32,
    ) -> Result<()> {
        self.emit(self.build_event(
            "command.started",
            execution_id,
            step,
            "STARTED",
            attempts,
            serde_json::json!({ "command_id": command_id }),
        ))
        .await
    }

    /// Emit a `call.done` event.
    pub async fn emit_call_done(
        &self,
        execution_id: i64,
        step: &str,
        command_id: &str,
        call_index: usize,
        attempts: u32,
        result: &serde_json::Value,
    ) -> Result<()> {
        self.emit(self.build_event(
            "call.done",
            execution_id,
            step,
            "COMPLETED",
            attempts,
            serde_json::json!({
                "command_id": command_id,
                "call_index": call_index,
                "result": result,
            }),
        ))
        .await
    }

    /// Emit a `call.error` event.
    pub async fn emit_call_error(
        &self,
        execution_id: i64,
        step: &str,
        command_id: &str,
        call_index: usize,
        attempts: u32,
        error: &str,
    ) -> Result<()> {
        self.emit(self.build_event(
            "call.error",
            execution_id,
            step,
            "FAILED",
            attempts,
            serde_json::json!({
                "command_id": command_id,
                "call_index": call_index,
                "error": error,
            }),
        ))
        .await
    }

    /// Emit a `step.exit` event.
    pub async fn emit_step_exit(
        &self,
        execution_id: i64,
        step: &str,
        status: &str,
        attempts: u32,
        data: Option<&serde_json::Value>,
    ) -> Result<()> {
        self.emit(self.build_event(
            "step.exit",
            execution_id,
            step,
            status,
            attempts,
            serde_json::json!({ "data": data }),
        ))
        .await
    }

    /// Emit a `command.completed` event.
    pub async fn emit_command_completed(
        &self,
        execution_id: i64,
        step: &str,
        command_id: &str,
        status: &str,
        attempts: u32,
    ) -> Result<()> {
        self.emit(self.build_event(
            "command.completed",
            execution_id,
            step,
            status,
            attempts,
            serde_json::json!({ "command_id": command_id }),
        ))
        .await
    }

    /// Emit a `command.failed` event.
    pub async fn emit_command_failed(
        &self,
        execution_id: i64,
        step: &str,
        command_id: &str,
        attempts: u32,
        error: &str,
    ) -> Result<()> {
        self.emit(self.build_event(
            "command.failed",
            execution_id,
            step,
            "FAILED",
            attempts,
            serde_json::json!({
                "command_id": command_id,
                "error": error,
            }),
        ))
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snowflake::SnowflakeGen;

    fn snowflake_for_tests() -> Arc<SnowflakeGen> {
        // Pin the node id so tests don't depend on env / hostname.
        Arc::new(SnowflakeGen::with_node_and_epoch(42, 0))
    }

    #[test]
    fn test_event_emitter_creation() {
        let client = ControlPlaneClient::new("http://localhost:8082");
        let emitter = EventEmitter::new(client, "worker-test-1", snowflake_for_tests());

        assert_eq!(emitter.max_retries, 3);
        assert_eq!(emitter.initial_delay, Duration::from_millis(500));
        assert_eq!(emitter.worker_id, "worker-test-1");
    }

    #[test]
    fn test_event_emitter_custom_retry() {
        let client = ControlPlaneClient::new("http://localhost:8082");
        let emitter = EventEmitter::with_retry(
            client,
            "worker-test-2",
            snowflake_for_tests(),
            5,
            Duration::from_millis(100),
            Duration::from_secs(5),
        );

        assert_eq!(emitter.max_retries, 5);
        assert_eq!(emitter.initial_delay, Duration::from_millis(100));
        assert_eq!(emitter.max_delay, Duration::from_secs(5));
        assert_eq!(emitter.worker_id, "worker-test-2");
    }

    /// `build_event` stamps `worker_id` from the emitter, sets
    /// `created_at` to a wall-clock timestamp (so the event log
    /// preserves per-component ordering across server-clock skew),
    /// populates `event_id` from the snowflake generator (per
    /// `observability.md` Principle 3 — the id exists at emit time,
    /// not after the DB round-trip), and carries `meta.attempts`
    /// so retry behaviour rides the event log without forcing
    /// projectors to reach back into the worker's logs.
    #[test]
    fn test_build_event_stamps_worker_id_created_at_event_id_and_attempts() {
        let client = ControlPlaneClient::new("http://localhost:8082");
        let emitter = EventEmitter::new(client, "worker-prod-7", snowflake_for_tests());

        let before = Utc::now();
        let event = emitter.build_event(
            "call.done",
            42,
            "fetch_calendar",
            "COMPLETED",
            2,
            serde_json::json!({ "result": "ok" }),
        );
        let after = Utc::now();

        assert_eq!(event.execution_id, 42);
        assert_eq!(event.event_type, "call.done");
        assert_eq!(event.step, "fetch_calendar");
        assert_eq!(event.status, "COMPLETED");
        assert_eq!(event.worker_id, Some("worker-prod-7".to_string()));
        assert!(event.event_id.is_some());
        let id = event.event_id.unwrap();
        assert!(id > 0, "event_id must be positive: {}", id);
        assert_eq!(
            event.meta,
            Some(serde_json::json!({ "attempts": 2 })),
            "meta.attempts must carry the supplied counter"
        );
        assert!(event.created_at >= before && event.created_at <= after);
    }

    /// First-attempt events carry `meta.attempts = 0`, not `meta: None`
    /// — the projector reads attempts uniformly without a
    /// presence check.
    #[test]
    fn test_build_event_carries_attempts_zero_for_first_try() {
        let client = ControlPlaneClient::new("http://localhost:8082");
        let emitter = EventEmitter::new(client, "worker-prod-7", snowflake_for_tests());
        let event = emitter.build_event(
            "command.started",
            1,
            "step",
            "STARTED",
            0,
            serde_json::json!({}),
        );
        assert_eq!(event.meta, Some(serde_json::json!({ "attempts": 0 })));
    }

    /// Consecutive `build_event` calls from the same emitter
    /// produce monotonically increasing `event_id`s — the contract
    /// downstream tooling relies on for ordering events without
    /// hitting the DB.
    #[test]
    fn test_event_ids_increase_monotonically_within_emitter() {
        let client = ControlPlaneClient::new("http://localhost:8082");
        let emitter = EventEmitter::new(client, "worker-prod-7", snowflake_for_tests());

        let mut prev: i64 = 0;
        for _ in 0..16 {
            let event = emitter.build_event(
                "call.done",
                1,
                "step",
                "COMPLETED",
                0,
                serde_json::json!({}),
            );
            let id = event.event_id.unwrap();
            assert!(id > prev, "id {} not > prev {}", id, prev);
            prev = id;
        }
    }

    /// Locks in the wire shape the worker sends after PR-EE-3 +
    /// the snowflake + attempts follow-ups so a future refactor
    /// can't accidentally drop a field both servers (Rust + Python)
    /// expect.  Mirrors the `tests/api/test_event_emit_request_aliases.py::
    /// TestFullExecutorEnvelopeRoundTrips` test on the Python
    /// side.
    #[test]
    fn test_emitted_event_has_full_ee_wire_shape() {
        let client = ControlPlaneClient::new("http://localhost:8082");
        let emitter = EventEmitter::new(client, "worker-prod-7", snowflake_for_tests());
        let event = emitter.build_event(
            "command.completed",
            478775660589088776,
            "fetch_calendar",
            "COMPLETED",
            3,
            serde_json::json!({ "command_id": "cmd-42" }),
        );

        let json = serde_json::to_value(&event).unwrap();
        for key in [
            "execution_id",
            "event_type",
            "step",
            "status",
            "created_at",
            "context",
            "worker_id",
            "event_id",
            "meta",
        ] {
            assert!(json.get(key).is_some(), "missing top-level field: {}", key);
        }
        // `meta.attempts` carries the supplied counter.
        assert_eq!(json["meta"]["attempts"], 3);
    }
}
