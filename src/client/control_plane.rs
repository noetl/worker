//! Control plane HTTP client.

use anyhow::Result;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::time::Duration;

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
    pub fn command_id(&self) -> String {
        self.meta
            .get("command_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
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

/// Event to emit to the control plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerEvent {
    /// Event type (e.g., "command.claimed", "command.started", "command.completed").
    pub event_type: String,

    /// Execution ID.
    pub execution_id: i64,

    /// Event payload.
    pub payload: serde_json::Value,
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
    pub async fn emit_event(&self, event: WorkerEvent) -> Result<()> {
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
    pub async fn emit_event_with_retry(&self, event: WorkerEvent, max_retries: u32) -> Result<()> {
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

    /// Register the worker pool with the control plane.
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
                "worker_id": worker_id,
                "pool_name": pool_name,
                "hostname": hostname,
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
    pub async fn heartbeat(&self, worker_id: &str, pool_name: &str) -> Result<()> {
        let response = self
            .client
            .post(format!("{}/api/worker/pool/heartbeat", self.server_url))
            .json(&serde_json::json!({
                "worker_id": worker_id,
                "pool_name": pool_name,
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
    pub async fn deregister_worker(&self, worker_id: &str, pool_name: &str) -> Result<()> {
        let response = self
            .client
            .delete(format!("{}/api/worker/pool/deregister", self.server_url))
            .json(&serde_json::json!({
                "worker_id": worker_id,
                "pool_name": pool_name,
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

    #[test]
    fn test_worker_event_serialization() {
        let event = WorkerEvent {
            event_type: "command.started".to_string(),
            execution_id: 12345,
            payload: serde_json::json!({"command_id": "cmd-123"}),
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("command.started"));
        assert!(json.contains("12345"));
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

    #[test]
    fn test_client_creation() {
        let client = ControlPlaneClient::new("http://localhost:8082");
        assert_eq!(client.server_url, "http://localhost:8082");

        let client = ControlPlaneClient::new("http://localhost:8082/");
        assert_eq!(client.server_url, "http://localhost:8082");
    }
}
