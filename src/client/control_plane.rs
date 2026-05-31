//! Control plane HTTP client.
//!
//! R-1.2 PR-EE-3: the worker now emits the shared
//! `noetl_executor::events::ExecutorEvent` wire shape on
//! `/api/events`, replacing the worker-local `WorkerEvent` it shipped
//! through R-1.2 PR-2e.  See the [event-envelope wiki page][ee] on
//! the noetl/server wiki for the full envelope contract.
//!
//! [ee]: https://github.com/noetl/server/wiki/event-envelope

use anyhow::Result;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// Re-export the shared envelope so the rest of the worker keeps
// importing it from `crate::client` (callers don't need to know it
// comes from the executor crate).
pub use noetl_executor::events::ExecutorEvent;

/// Response shape from `PUT /api/result/{execution_id}`.
///
/// Mirrors the Python server's `ResultPutResponse` (see
/// `noetl/server/api/result/endpoint.py`).  The `ref` field is the
/// `noetl://execution/<eid>/result/<name>/<id>` URI that downstream
/// consumers resolve via `GET /api/result/resolve?ref=<uri>`.  The
/// other fields are metadata the producer stamps onto the
/// `result.reference` dict it emits with `call.done`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultPutResponse {
    /// `noetl://execution/<eid>/result/<name>/<id>` URI.
    pub r#ref: String,
    /// Storage tier the server chose (e.g. `"disk"`, `"s3"`, `"gcs"`).
    pub store: String,
    /// Lifecycle scope (`"execution"` by default for the worker's path).
    pub scope: String,
    /// Optional ISO-8601 expiry; `None` for permanent scope.
    #[serde(default)]
    pub expires_at: Option<String>,
    /// Size in bytes the server stored.
    #[serde(default)]
    pub bytes: u64,
    /// Optional SHA-256 of the stored bytes.
    #[serde(default)]
    pub sha256: Option<String>,
}

/// Result of claiming a command.
#[derive(Debug, Clone)]
pub enum ClaimResult {
    /// Successfully claimed the command and received details.
    Claimed(Command),
    /// Command already claimed by another worker.
    AlreadyClaimed,
    /// Transient failure (retry later / redelivery).
    RetryLater(String),
    /// Failed to claim (error).
    Failed(String),
}

/// Command payload returned by control-plane endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Command {
    /// Execution ID.
    pub execution_id: i64,

    /// Step/node identifier.
    pub node_id: String,

    /// Step/node name.
    pub node_name: String,

    /// Tool kind/action.
    pub action: String,

    /// Command execution context (tool_config, args, render_context, ...).
    #[serde(default)]
    pub context: serde_json::Value,

    /// Metadata (contains command_id, attempts, etc.).
    #[serde(default)]
    pub meta: serde_json::Value,
}

impl Command {
    /// Extract command_id from metadata (or fallback).
    ///
    /// Accepts the JSON value as either a string OR a number — the
    /// Python broker now emits `command_id` as a `bigint` snowflake
    /// (numeric JSON literal) in its outgoing payloads.  When the
    /// value is missing entirely, falls back to a synthetic id
    /// constructed from `execution_id` + `node_name` for diagnostic
    /// purposes.
    pub fn command_id(&self) -> String {
        self.meta
            .get("command_id")
            .and_then(|v| {
                v.as_str()
                    .map(|s| s.to_string())
                    .or_else(|| v.as_i64().map(|n| n.to_string()))
                    .or_else(|| v.as_u64().map(|n| n.to_string()))
            })
            .unwrap_or_else(|| format!("{}:{}:unknown", self.execution_id, self.node_name))
    }

    /// Get step name.
    pub fn step(&self) -> &str {
        &self.node_name
    }

    /// Build full tool config payload from action + context.tool_config.
    pub fn tool_config_value(&self) -> serde_json::Value {
        let mut cfg = self
            .context
            .get("tool_config")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));

        if !cfg.is_object() {
            cfg = serde_json::json!({});
        }
        if let Some(map) = cfg.as_object_mut() {
            map.entry("kind".to_string())
                .or_insert_with(|| serde_json::json!(self.action));
            if !map.contains_key("args") {
                if let Some(args) = self.context.get("args") {
                    map.insert("args".to_string(), args.clone());
                }
            }
        }
        cfg
    }

    /// Extract render_context map from command context.
    pub fn render_context(&self) -> std::collections::HashMap<String, serde_json::Value> {
        self.context
            .get("render_context")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default()
    }
}

/// HTTP client for control plane API.
#[derive(Clone)]
pub struct ControlPlaneClient {
    client: reqwest::Client,
    server_url: String,
}

impl ControlPlaneClient {
    /// Create a new control plane client.
    pub fn new(server_url: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();

        Self {
            client,
            server_url: server_url.trim_end_matches('/').to_string(),
        }
    }

    /// Atomically claim a command and fetch its details.
    ///
    /// Returns full command on success, semantic statuses for claim contention.
    pub async fn claim_command(&self, event_id: i64, worker_id: &str) -> Result<ClaimResult> {
        let response = self
            .client
            .post(format!(
                "{}/api/commands/{}/claim",
                self.server_url, event_id
            ))
            .json(&serde_json::json!({ "worker_id": worker_id }))
            .send()
            .await?;

        match response.status() {
            StatusCode::OK | StatusCode::CREATED => {
                let command: Command = response.json().await?;
                Ok(ClaimResult::Claimed(command))
            }
            StatusCode::CONFLICT => Ok(ClaimResult::AlreadyClaimed),
            StatusCode::TOO_MANY_REQUESTS
            | StatusCode::REQUEST_TIMEOUT
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::BAD_GATEWAY
            | StatusCode::GATEWAY_TIMEOUT => {
                let body = response.text().await.unwrap_or_default();
                Ok(ClaimResult::RetryLater(body))
            }
            status => {
                let body = response.text().await.unwrap_or_default();
                Ok(ClaimResult::Failed(format!("Status {}: {}", status, body)))
            }
        }
    }

    /// Fetch full command details from the control plane.
    ///
    /// Compatibility fallback when claim endpoint is unavailable.
    pub async fn fetch_command(&self, event_id: i64) -> Result<Command> {
        let response = self
            .client
            .get(format!("{}/api/commands/{}", self.server_url, event_id))
            .send()
            .await?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Failed to fetch command: {}", body);
        }

        let command: Command = response.json().await?;
        Ok(command)
    }

    /// Emit an event to the control plane.
    ///
    /// R-1.2 PR-EE-3: takes `ExecutorEvent` (the shared envelope) so
    /// the wire shape matches what `noetl-server` (Rust + Python) and
    /// `noetl-executor` already produce / consume.  See the
    /// [event-envelope wiki page][ee] for the field-by-field
    /// contract.
    ///
    /// [ee]: https://github.com/noetl/server/wiki/event-envelope
    pub async fn emit_event(&self, event: ExecutorEvent) -> Result<()> {
        let response = self
            .client
            .post(format!("{}/api/events", self.server_url))
            .json(&event)
            .send()
            .await?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Failed to emit event: {}", body);
        }

        Ok(())
    }

    /// Emit an event with retry.
    pub async fn emit_event_with_retry(
        &self,
        event: ExecutorEvent,
        max_retries: u32,
    ) -> Result<()> {
        let mut delay = Duration::from_millis(500);

        for attempt in 0..=max_retries {
            match self.emit_event(event.clone()).await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < max_retries => {
                    tracing::warn!(
                        attempt = attempt + 1,
                        max_retries,
                        error = %e,
                        "Event emission failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    delay = std::cmp::min(delay * 2, Duration::from_secs(10));
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    /// Get a variable value for an execution.
    pub async fn get_variable(
        &self,
        execution_id: i64,
        name: &str,
    ) -> Result<Option<serde_json::Value>> {
        let response = self
            .client
            .get(format!(
                "{}/api/vars/{}/{}",
                self.server_url, execution_id, name
            ))
            .send()
            .await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Failed to get variable: {}", body);
        }

        let value: serde_json::Value = response.json().await?;
        Ok(Some(value))
    }

    /// Set a variable value for an execution.
    pub async fn set_variable(
        &self,
        execution_id: i64,
        name: &str,
        value: serde_json::Value,
    ) -> Result<()> {
        let response = self
            .client
            .post(format!("{}/api/vars/{}", self.server_url, execution_id))
            .json(&serde_json::json!({
                name: value
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Failed to set variable: {}", body);
        }

        Ok(())
    }

    /// Store a result payload in the durable result store.
    ///
    /// Calls `PUT /api/result/{execution_id}` on the Python server
    /// with a `ResultPutRequest`-shaped body and returns the
    /// `ResultRef` the server allocated.  Used by
    /// `CommandExecutor::build_call_done_result` for the cross-node
    /// reference path of `noetl/worker#24` — when a tool's
    /// serialised `result.context` exceeds the broker's inline
    /// budget, the worker stages the bytes here and emits a
    /// `result.reference` carrying the returned URI.
    ///
    /// Per `agents/rules/execution-model.md`: this is platform-
    /// runtime traffic (worker → server, internal control plane),
    /// not a business-logic call into a third-party API — the
    /// server's `default_store` handles tier selection (`disk` /
    /// `s3` / `gcs`) and durable lifecycle.
    ///
    /// Per `observability.md` Principle 4: the caller is expected
    /// to wrap the call in a `result_store.put` span carrying
    /// `execution_id` + `step` so the durable-write latency is
    /// attributable to the playbook run.
    ///
    /// Arguments:
    /// - `execution_id`: the execution this result belongs to;
    ///   propagated to `default_tracker.register_ref` for scope
    ///   cleanup at execution completion.
    /// - `name`: logical name for the result, usually the step name.
    ///   Forms part of the returned `noetl://` URI.
    /// - `data`: arbitrary JSON value to stage.  The server
    ///   measures, hashes, and routes to the right tier.
    /// - `scope`: `"execution"` (default) for normal results;
    ///   `"workflow"` for results that outlive the current
    ///   playbook (nested playbook calls); `"permanent"` for
    ///   results that should never auto-cleanup.
    /// - `source_step`: step that produced the result; informs the
    ///   scope tracker so step-scoped results clean up when that
    ///   step's last consumer reports done.
    pub async fn put_result(
        &self,
        execution_id: i64,
        name: &str,
        data: &serde_json::Value,
        scope: &str,
        source_step: Option<&str>,
    ) -> Result<ResultPutResponse> {
        let mut body = serde_json::json!({
            "name": name,
            "data": data,
            "scope": scope,
        });
        if let Some(step) = source_step {
            if let Some(map) = body.as_object_mut() {
                map.insert(
                    "source_step".to_string(),
                    serde_json::Value::String(step.to_string()),
                );
            }
        }

        let response = self
            .client
            .put(format!("{}/api/result/{}", self.server_url, execution_id))
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("put_result failed: HTTP {} {}", status.as_u16(), body);
        }

        let parsed: ResultPutResponse = response.json().await?;
        Ok(parsed)
    }

    /// Register the worker pool with the control plane.
    ///
    /// Wire shape matches the Python broker's `RuntimeRegistrationRequest`:
    /// `name` (the unique component name; we pass `worker_id`),
    /// `component_type` (`worker_pool`), `runtime` (`rust`),
    /// `status` (`ready`), `capacity` (max-concurrent dispatches),
    /// `hostname`, plus a `labels` map carrying the pool name so
    /// multi-pool deployments can filter on it.
    ///
    /// Pre-fix sent `{worker_id, pool_name, hostname}` which the
    /// broker rejected with `Field required: body.name` — kind
    /// validation surfaced this 2026-05-31.
    pub async fn register_worker(
        &self,
        worker_id: &str,
        pool_name: &str,
        hostname: &str,
    ) -> Result<()> {
        let response = self
            .client
            .post(format!("{}/api/worker/pool/register", self.server_url))
            .json(&serde_json::json!({
                "name": worker_id,
                "component_type": "worker_pool",
                "runtime": "rust",
                "status": "ready",
                "hostname": hostname,
                "labels": {
                    "pool_name": pool_name,
                },
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Failed to register worker: {}", body);
        }

        Ok(())
    }

    /// Send a heartbeat to the control plane.
    ///
    /// Wire shape matches the Python broker's
    /// `RuntimeHeartbeatRequest`: `name` only.  The broker upserts
    /// the heartbeat timestamp keyed by `name`.
    pub async fn heartbeat(&self, worker_id: &str, _pool_name: &str) -> Result<()> {
        let response = self
            .client
            .post(format!("{}/api/worker/pool/heartbeat", self.server_url))
            .json(&serde_json::json!({
                "name": worker_id,
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            tracing::warn!("Heartbeat failed: {}", body);
        }

        Ok(())
    }

    /// Deregister the worker pool.
    ///
    /// Wire shape matches the Python broker's deregister endpoint:
    /// `name` + `component_type`.  POST (not DELETE) — the broker
    /// expects a JSON body with the component name and type.
    pub async fn deregister_worker(&self, worker_id: &str, _pool_name: &str) -> Result<()> {
        let response = self
            .client
            .post(format!("{}/api/worker/pool/deregister", self.server_url))
            .json(&serde_json::json!({
                "name": worker_id,
                "component_type": "worker_pool",
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            tracing::warn!("Deregister failed: {}", body);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    /// The R-1.2 PR-EE-3 wire shape: `ExecutorEvent` with `step` +
    /// `status` + `created_at` at the top level and `context` (was
    /// the worker-local `payload` field).  The optional `event_id`
    /// / `worker_id` / `meta` fields all serialize when present and
    /// drop out via `skip_serializing_if = "Option::is_none"`.
    #[test]
    fn test_executor_event_serialization_matches_ee_wire_format() {
        let event = ExecutorEvent {
            execution_id: 12345,
            event_type: "command.started".to_string(),
            step: "fetch_calendar".to_string(),
            status: "STARTED".to_string(),
            created_at: Utc::now(),
            context: serde_json::json!({ "command_id": "cmd-123" }),
            event_id: None,
            worker_id: Some("worker-prod-7".to_string()),
            meta: None,
        };

        let json = serde_json::to_string(&event).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Top-level shape matches the server's `EventRequest` /
        // Python `EventEmitRequest` after EE-2 + EE-4.
        assert_eq!(parsed["event_type"], "command.started");
        assert_eq!(parsed["execution_id"], 12345);
        assert_eq!(parsed["step"], "fetch_calendar");
        assert_eq!(parsed["status"], "STARTED");
        assert_eq!(parsed["worker_id"], "worker-prod-7");
        assert_eq!(parsed["context"]["command_id"], "cmd-123");

        // Optional fields with `None` value must not appear in the
        // serialised JSON (per `skip_serializing_if = "Option::is_none"`).
        assert!(parsed.get("event_id").is_none());
        assert!(parsed.get("meta").is_none());

        // `created_at` is always populated at emit time.
        assert!(parsed.get("created_at").is_some());
    }

    /// The `payload` alias on `ExecutorEvent.context` (added in
    /// PR-EE-1) means pre-EE producers that still send `payload`
    /// continue to deserialize cleanly.  Locked in here so a
    /// future executor crate change doesn't silently drop the
    /// alias.
    #[test]
    fn test_executor_event_payload_alias_back_compat() {
        let wire = serde_json::json!({
            "execution_id": 1,
            "event_type": "call.done",
            "step": "fetch",
            "status": "COMPLETED",
            "created_at": "2026-05-31T03:14:15Z",
            "payload": { "result": "ok" },
        });
        let event: ExecutorEvent = serde_json::from_value(wire).unwrap();
        assert_eq!(event.context, serde_json::json!({ "result": "ok" }));
    }

    #[test]
    fn test_command_deserialization() {
        let json = serde_json::json!({
            "execution_id": 12345,
            "node_id": "process",
            "node_name": "process",
            "action": "shell",
            "context": {"tool_config": {"command": "echo hello"}},
            "meta": {"command_id": "cmd-abc"}
        });

        let command: Command = serde_json::from_value(json).unwrap();
        assert_eq!(command.execution_id, 12345);
        assert_eq!(command.step(), "process");
        assert_eq!(command.command_id(), "cmd-abc");
    }

    /// `ResultPutResponse` matches the Python server's
    /// `ResultPutResponse` wire shape (noetl/server/api/result/endpoint.py).
    /// Lock the field names in so a future server-side rename
    /// surfaces here at build time.
    #[test]
    fn test_result_put_response_deserialization() {
        let wire = serde_json::json!({
            "ref": "noetl://execution/12345/result/big_step/abcd1234",
            "store": "disk",
            "scope": "execution",
            "expires_at": "2026-06-01T00:00:00Z",
            "bytes": 204_800,
            "sha256": "deadbeef",
        });
        let parsed: ResultPutResponse = serde_json::from_value(wire).unwrap();
        assert_eq!(
            parsed.r#ref,
            "noetl://execution/12345/result/big_step/abcd1234"
        );
        assert_eq!(parsed.store, "disk");
        assert_eq!(parsed.scope, "execution");
        assert_eq!(parsed.bytes, 204_800);
        assert_eq!(parsed.sha256.as_deref(), Some("deadbeef"));

        // `expires_at` is allowed to be missing for permanent scope.
        let wire_no_expiry = serde_json::json!({
            "ref": "noetl://execution/1/result/n/x",
            "store": "memory",
            "scope": "permanent",
            "bytes": 12,
        });
        let parsed: ResultPutResponse = serde_json::from_value(wire_no_expiry).unwrap();
        assert!(parsed.expires_at.is_none());
        assert!(parsed.sha256.is_none());
    }

    #[test]
    fn test_client_creation() {
        let client = ControlPlaneClient::new("http://localhost:8082");
        assert_eq!(client.server_url, "http://localhost:8082");

        let client = ControlPlaneClient::new("http://localhost:8082/");
        assert_eq!(client.server_url, "http://localhost:8082");
    }
}
