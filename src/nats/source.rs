//! `CommandSource` impl wrapping the worker's NATS subscriber +
//! control-plane HTTP client.
//!
//! Implements [`noetl_executor::worker::source::CommandSource`] (the
//! 0.3.0 trait with ack lifecycle + 4-state [`ClaimOutcome`]) so the
//! worker's main loop can be generic over command sources — concrete
//! NATS impl in production, [`MockSource`] for unit tests.
//!
//! ## Pull flow
//!
//! 1. `subscriber.receive()` returns `Option<(CommandNotification,
//!    Message)>` — the JetStream message is the [`AckHandle`].
//! 2. The notification carries an `event_id`; we use it to claim the
//!    full command via `client.claim_command(event_id, worker_id)`.
//! 3. The claim's [`ClaimResult`] maps 1:1 onto the executor's
//!    [`ClaimOutcome`]:
//!
//!    | `ClaimResult`        | `ClaimOutcome`                  |
//!    | :----                | :----                           |
//!    | `Claimed(worker_cmd)`| `Claimed(executor_cmd)` (translated) |
//!    | `AlreadyClaimed`     | `AlreadyClaimed`                |
//!    | `RetryLater(err)`    | `RetryLater(err)`               |
//!    | `Failed(err)`        | `Failed(err)`                   |
//!
//! 4. The translated [`Pulled<Message>`] is returned; the caller
//!    decides whether to `ack` (commit), `nack` (redeliver), and
//!    whether to dispatch the embedded command.
//!
//! ## Command translation
//!
//! Worker's [`crate::client::Command`] → executor's [`Command`]:
//!
//! | Worker field              | Executor field          | Notes |
//! | :----                     | :----                   | :---- |
//! | `command_id()` (from meta)| `command_id`            | Worker computes a fallback if meta is missing. |
//! | `execution_id`            | `execution_id`          | Already `i64` on both sides since R-1.2 PR-2a. |
//! | `node_name`               | `step`                  | Worker calls this `step()` accessor too. |
//! | `action`                  | `tool_kind`             | E.g. `"http"`, `"postgres"`, `"rhai"`. |
//! | `context` (full JSON)     | `input`                 | Carries `tool_config` + `cases` + `args` + nested config.  Caller extracts what it needs. |
//! | `render_context()`        | `render_context`        | Already `HashMap<String, Value>` on both sides. |
//! | `meta.attempts`           | `attempts`              | Parsed from JSON number; defaults to 0 if missing. |

use anyhow::Result;
use async_nats::jetstream::Message;
// `noetl_executor::worker::source::CommandSource` is declared with
// `#[async_trait::async_trait]`; impl blocks need the same attribute
// so `async fn next/ack/nack` desugar to the expected
// `Box<dyn Future + Send + 'a>` shape.
use async_trait::async_trait;
use noetl_executor::worker::source::{
    ClaimOutcome, Command as ExecutorCommand, CommandSource, Pulled,
};

use crate::client::{ClaimResult, Command as WorkerCommand, ControlPlaneClient};
use crate::nats::subscriber::{CommandNotification, NatsSubscriber};

/// Per-pull notification metadata.  Carried alongside the NATS
/// message handle in the source's [`AckHandle`] so the Worker can
/// log `execution_id` + `command_id` + `step` correlations on the
/// non-Claimed ClaimOutcome variants (AlreadyClaimed / RetryLater /
/// Failed) — where the executor's `ClaimOutcome` doesn't carry
/// command identifiers.
///
/// Per `observability.md` Principle 4: `execution_id` rides every
/// wire format and every structured log/span field on WARN+ERROR.
/// This struct is the worker-side bridge between the NATS
/// notification (which has the ids) and the WARN/ERROR call sites
/// in `Worker::process_commands` (which need them for correlation).
#[derive(Debug)]
pub struct NatsAckHandle {
    pub message: Message,
    pub notification: CommandNotification,
}

/// `CommandSource` implementation backed by NATS JetStream + the
/// control-plane HTTP API.
///
/// Owns the subscriber + client + worker_id so `next()` and `ack` /
/// `nack` can all be called without the caller threading shared
/// state.  `worker_id` is captured at construction since it's used
/// in every `claim_command` call.
pub struct NatsCommandSource {
    subscriber: NatsSubscriber,
    client: ControlPlaneClient,
    worker_id: String,
}

impl NatsCommandSource {
    /// Construct a source from its component dependencies.  The
    /// subscriber must already be connected and bound to the right
    /// stream / consumer; the client must be configured with the
    /// control-plane URL.
    pub fn new(
        subscriber: NatsSubscriber,
        client: ControlPlaneClient,
        worker_id: impl Into<String>,
    ) -> Self {
        Self {
            subscriber,
            client,
            worker_id: worker_id.into(),
        }
    }

    /// Borrow the subscriber.  Useful for callers that need to read
    /// the underlying JetStream consumer state (e.g. cluster-mode
    /// pause / resume).
    pub fn subscriber(&self) -> &NatsSubscriber {
        &self.subscriber
    }
}

/// Translate the worker's local `Command` into the executor's
/// enriched `Command`.  Lossless: every field on the executor side
/// maps to a worker-side accessor or JSON path.
fn translate(worker: WorkerCommand) -> ExecutorCommand {
    let attempts = worker
        .meta
        .get("attempts")
        .and_then(|v| v.as_u64())
        .map(|n| n as u32)
        .unwrap_or(0);

    let render_context = worker.render_context();
    let command_id = worker.command_id();
    let step = worker.step().to_string();
    let execution_id = worker.execution_id;
    let tool_kind = worker.action.clone();

    // The executor's `input` carries the worker's full `context`
    // JSON (tool_config + cases + args + any forward-compat fields).
    // CommandExecutor extracts what it needs from `input.tool_config`
    // and `input.cases`.
    ExecutorCommand {
        command_id,
        execution_id,
        step,
        tool_kind,
        input: worker.context,
        render_context,
        attempts,
    }
}

#[async_trait]
impl CommandSource for NatsCommandSource {
    /// Carries both the NATS message handle (for ack/nack) AND the
    /// original notification metadata so the Worker has
    /// `execution_id` / `command_id` / `step` available for WARN /
    /// ERROR correlation on every ClaimOutcome variant.
    type AckHandle = NatsAckHandle;

    async fn next(&mut self) -> Result<Option<Pulled<Self::AckHandle>>> {
        // Span covers the entire pull (receive + claim).  Per
        // `observability.md` Principle 1, every boundary call ships
        // a span that the metrics + logs hang off of.
        let span = tracing::debug_span!("nats.pull");
        let _enter = span.enter();

        let Some((notification, msg)) = self.subscriber.receive().await? else {
            return Ok(None);
        };

        tracing::debug!(
            execution_id = notification.execution_id,
            command_id = %notification.command_id,
            step = %notification.step,
            event_id = notification.event_id,
            "Pulled command notification from NATS"
        );

        let claim = self
            .client
            .claim_command(notification.event_id, &self.worker_id)
            .await?;

        let outcome = match claim {
            ClaimResult::Claimed(worker_cmd) => ClaimOutcome::Claimed(translate(worker_cmd)),
            ClaimResult::AlreadyClaimed => ClaimOutcome::AlreadyClaimed,
            ClaimResult::RetryLater(err) => ClaimOutcome::RetryLater(err),
            ClaimResult::Failed(err) => ClaimOutcome::Failed(err),
        };

        Ok(Some(Pulled {
            outcome,
            ack: NatsAckHandle { message: msg, notification },
        }))
    }

    async fn ack(&self, handle: Self::AckHandle) -> Result<()> {
        self.subscriber.ack(&handle.message).await
    }

    async fn nack(&self, handle: Self::AckHandle) -> Result<()> {
        self.subscriber.nack(&handle.message).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Build a minimal worker `Command` JSON for translation tests.
    fn worker_command(action: &str, context: serde_json::Value, attempts: Option<u64>) -> WorkerCommand {
        let meta = match attempts {
            Some(n) => json!({"command_id": "cmd-test", "attempts": n}),
            None => json!({"command_id": "cmd-test"}),
        };
        serde_json::from_value(json!({
            "execution_id": 42,
            "node_id": "n1",
            "node_name": "fetch_step",
            "action": action,
            "context": context,
            "meta": meta,
        }))
        .expect("test worker command must deserialize")
    }

    #[test]
    fn translate_carries_command_id_from_meta() {
        let wc = worker_command("http", json!({"tool_config": {"kind": "http"}}), None);
        let ec = translate(wc);
        assert_eq!(ec.command_id, "cmd-test");
    }

    #[test]
    fn translate_uses_node_name_as_step() {
        let wc = worker_command("http", json!({}), None);
        let ec = translate(wc);
        assert_eq!(ec.step, "fetch_step");
    }

    #[test]
    fn translate_uses_action_as_tool_kind() {
        let wc = worker_command("postgres", json!({}), None);
        let ec = translate(wc);
        assert_eq!(ec.tool_kind, "postgres");
    }

    #[test]
    fn translate_preserves_execution_id_as_i64() {
        let wc = worker_command("rhai", json!({}), None);
        let ec = translate(wc);
        assert_eq!(ec.execution_id, 42);
    }

    #[test]
    fn translate_extracts_attempts_from_meta() {
        let wc = worker_command("rhai", json!({}), Some(3));
        let ec = translate(wc);
        assert_eq!(ec.attempts, 3);
    }

    #[test]
    fn translate_defaults_attempts_to_zero_when_missing() {
        let wc = worker_command("rhai", json!({}), None);
        let ec = translate(wc);
        assert_eq!(ec.attempts, 0);
    }

    #[test]
    fn translate_carries_render_context_as_hashmap() {
        let wc = worker_command(
            "http",
            json!({
                "render_context": {
                    "workload.region": "us-east-1",
                    "vars.timeout": 30,
                }
            }),
            None,
        );
        let ec = translate(wc);
        assert_eq!(
            ec.render_context.get("workload.region"),
            Some(&json!("us-east-1"))
        );
        assert_eq!(ec.render_context.get("vars.timeout"), Some(&json!(30)));
    }

    #[test]
    fn translate_carries_full_context_as_input_including_cases() {
        // Critical contract: `input` carries the worker's ENTIRE
        // context JSON (tool_config + cases + args + render_context).
        // CommandExecutor extracts each section separately, so this
        // test locks in that nothing gets dropped at the seam.
        let wc = worker_command(
            "http",
            json!({
                "tool_config": {"kind": "http", "url": "https://example.com"},
                "cases": [{"when": [{"left": "result.status", "op": "eq", "right": "ok"}], "then": "continue"}],
                "args": {"timeout": 10},
                "render_context": {"workload.region": "us-east-1"},
            }),
            None,
        );
        let ec = translate(wc);
        assert!(ec.input.get("tool_config").is_some(), "tool_config must be in input");
        assert!(ec.input.get("cases").is_some(), "cases must be in input");
        assert!(ec.input.get("args").is_some(), "args must be in input");
        assert!(ec.input.get("render_context").is_some(), "render_context must be in input (also surfaced via the dedicated field)");
    }

    #[test]
    fn translate_handles_missing_render_context() {
        let wc = worker_command("http", json!({"tool_config": {"kind": "http"}}), None);
        let ec = translate(wc);
        assert!(ec.render_context.is_empty());
    }
}
