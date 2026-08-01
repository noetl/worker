//! **Command dispatch — the transport-neutral half of the old NATS module.**
//!
//! These three items lived in `src/nats/` and were deleted along with it by
//! reflex once — they are NATS-*named* or NATS-*adjacent*, but every one of them
//! is on the **live EHDB path** (noetl/ai-meta#212, #218):
//!
//! - [`CommandNotification`] is the command payload. The EHDB claim path
//!   decodes into it exactly as the NATS subscriber used to.
//! - [`segment_from_filter`] derives the worker's **EHDB pool** from its filter
//!   subject. Unsetting its input silently put system-pool workers on
//!   `commands.shared.>`, where they claimed nothing and every execution stalled
//!   with no error (noetl/ai-meta#218).
//! - [`claim_outcome`] was always shared by both sources — its own doc said so.
//!   Only *how the notification is obtained* ever differed between transports.
//!
//! Nothing here talks to NATS; the filter subject is just a dotted string.

use anyhow::Result;
use noetl_executor::worker::source::{ClaimOutcome, Command as ExecutorCommand};
use serde::{Deserialize, Serialize};

use crate::client::{ClaimResult, Command as WorkerCommand, ControlPlaneClient};

/// The command notification the control plane publishes onto the bus.
///
/// This is a lightweight notification that triggers command fetching.
///
/// `command_id` is normalised to `String` in memory but the wire
/// format accepts either a JSON string OR a JSON integer — the
/// Python broker switched the `noetl.command.command_id` column to
/// `bigint` snowflake and now serialises it as a JSON number on
/// the publish path.  See `deserialize_command_id` below.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandNotification {
    /// Execution ID this command belongs to.
    pub execution_id: i64,

    /// Event ID containing the full command details.
    pub event_id: i64,

    /// Unique command identifier for atomic claiming.  Accepts
    /// JSON string OR integer on the wire; stored as `String` so
    /// downstream call sites (logging, tracing, executor `Command`)
    /// don't need to handle both shapes.
    #[serde(deserialize_with = "deserialize_command_id")]
    pub command_id: String,

    /// Step name this command is for.
    pub step: String,

    /// Server URL for fetching command details.
    pub server_url: String,

    /// Target worker-pool segment the server routed this command to
    /// (`shared` / `system` / a subscription override), mirroring the NATS
    /// subject `noetl.commands.<segment>.<execution_id>` (noetl/ai-meta#108).
    /// `None` for legacy notifications that predate pool stamping. The worker
    /// uses it to decline commands that aren't for its pool — defence-in-depth
    /// against a JetStream consumer whose `filter_subject` drifted broad and so
    /// delivers another pool's commands.
    #[serde(default)]
    pub execution_pool: Option<String>,
}

/// Parse the pool segment out of the routing filter subject
/// (`noetl.commands.<segment>.>` → `Some("<segment>")`). A bare or wildcard
/// segment (`noetl.commands`, `noetl.commands.>`) yields `None` — that worker
/// accepts every command (the single-pool default, unchanged).
pub fn segment_from_filter(filter: &str) -> Option<String> {
    let parts: Vec<&str> = filter.split('.').collect();
    let idx = parts.iter().position(|&p| p == "commands")?;
    let seg = parts.get(idx + 1)?;
    if *seg == ">" || *seg == "*" || seg.is_empty() {
        None
    } else {
        Some((*seg).to_string())
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

    let mut render_context = worker.render_context();
    // noetl/ai-meta#104 R02b: the executor `Command` carries `render_context`
    // but not the raw `metadata`, so copy the cursor fan-out coordinate
    // (`metadata.cursor.{frame,row}`, stamped on body commands by the
    // orchestrator) into `render_context` under reserved keys. The dispatch
    // site reads them to build the result's collision-free logical URI.
    if let Some(cursor) = worker.meta.get("cursor") {
        if let Some(frame) = cursor.get("frame") {
            render_context.insert("__cursor_frame".to_string(), frame.clone());
        }
        if let Some(row) = cursor.get("row") {
            render_context.insert("__cursor_row".to_string(), row.clone());
        }
    }
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

/// Claim a command from the server that published it and map the result to a
/// [`ClaimOutcome`].  Shared by every [`CommandSource`] (NATS + the EHDB command
/// bus, noetl/ai-meta#194 L1 T4) so the claim + translate correctness lives in
/// **one** place — only how the [`CommandNotification`] is *obtained* differs
/// between sources.  Routes the claim (and downstream calls) to
/// `notification.server_url` (noetl/ai-meta#53 Gap 1).
pub(crate) async fn claim_outcome(
    client: &ControlPlaneClient,
    worker_id: &str,
    notification: &CommandNotification,
) -> Result<ClaimOutcome> {
    let dispatch_client = client.with_server_url(&notification.server_url);
    let claim = dispatch_client
        .claim_command(notification.event_id, worker_id)
        .await?;
    Ok(match claim {
        ClaimResult::Claimed(worker_cmd) => ClaimOutcome::Claimed(translate(worker_cmd)),
        ClaimResult::AlreadyClaimed => ClaimOutcome::AlreadyClaimed,
        ClaimResult::RetryLater(err) => ClaimOutcome::RetryLater(err),
        ClaimResult::Failed(err) => ClaimOutcome::Failed(err),
    })
}

/// Accept either a JSON string OR a JSON integer for `command_id`;
/// stringify the integer form so the in-memory representation is
/// always `String`.  The Python broker now sends `command_id` as a
/// `bigint` snowflake (numeric JSON literal) but the worker wasn't
/// updated to deserialize it — the `invalid type: integer ...,
/// expected a string` error surfaced this during the EE-3 kind
/// validation pass.
fn deserialize_command_id<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{Error, Unexpected};
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(s) => Ok(s),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        other => Err(D::Error::invalid_type(
            match &other {
                serde_json::Value::Null => Unexpected::Unit,
                serde_json::Value::Bool(b) => Unexpected::Bool(*b),
                serde_json::Value::Array(_) => Unexpected::Seq,
                serde_json::Value::Object(_) => Unexpected::Map,
                _ => Unexpected::Other("non-string non-number"),
            },
            &"a JSON string or a JSON integer",
        )),
    }
}
