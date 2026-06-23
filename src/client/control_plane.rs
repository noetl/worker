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
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::Zeroize;

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

/// Credential record returned by `GET /api/credentials/{alias}`.
///
/// Wire shape mirrors the server's `CredentialResponse` model: a
/// `type` field carrying the credential family (`postgres`, `bearer`,
/// `api_key`, `basic`, `gcp_adc`, ...) and a free-form `data` dict
/// whose keys depend on the type.  The worker's
/// `resolve_auth_alias` helper maps the type-specific fields into
/// either the tool's flat connection fields (postgres) or a
/// noetl-tools `AuthConfig` JSON (bearer / api_key / basic).
///
/// See noetl/ai-meta#48 for the regression brief.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credential {
    /// Server-assigned numeric id (snowflake), serialized as string.
    pub id: String,
    /// Alias as referenced by playbook `auth: "<alias>"`.
    pub name: String,
    /// Credential family.  Values seen today: `postgres`, `bearer`,
    /// `api_key`, `basic`, `gcp_adc`, `gcp_oauth`, `hmac`.
    #[serde(rename = "type")]
    pub cred_type: String,
    /// Decrypted credential payload.  Keys depend on `cred_type`.
    /// Always present because `get_credential` always requests
    /// `include_data=true`.
    #[serde(default)]
    pub data: std::collections::HashMap<String, serde_json::Value>,
    /// Optional metadata (environment tags, etc.).  Not consumed by
    /// the resolver today but preserved on the type for forward
    /// compatibility.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Free-form description.
    #[serde(default)]
    pub description: Option<String>,
}

/// A non-success, non-404 HTTP response from a credential fetch.
///
/// Carries the numeric status so callers can classify retryability by
/// code (a 503 is transient; a 500 "Decryption failed" or a 400 is
/// terminal) WITHOUT string-matching the formatted error message.
/// `get_credential` / `get_sealed_credential` return this (wrapped in
/// `anyhow::Error`) instead of an opaque `bail!`, and
/// `auth_alias::classify_fetch_error` downcasts to it.  A 404 never
/// reaches here — it maps to `Ok(None)` upstream.  See
/// [noetl/ai-meta#78](https://github.com/noetl/ai-meta/issues/78).
#[derive(Debug, thiserror::Error)]
#[error("credential fetch for '{alias}' failed: HTTP {status} {body}")]
pub struct CredentialHttpError {
    pub alias: String,
    pub status: u16,
    pub body: String,
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
    /// Long-lived X25519 [`StaticSecret`] generated once at startup
    /// (Secrets Wallet Phase 5c, noetl/ai-meta#61).  Wrapped in `Arc` so the
    /// inner key bytes are shared across [`with_server_url`] dispatch clones
    /// — the recipient identity stays constant for the worker's lifetime, so
    /// every sealed credential the server addresses to this pool unseals
    /// with the same secret.
    sealing_sk: Arc<StaticSecret>,
}

impl ControlPlaneClient {
    /// Return a clone of this client with a different server URL.
    ///
    /// Used by the dispatch path (noetl/ai-meta#53 Gap 1) so each
    /// command's HTTP callbacks go to the server that published
    /// the command (carried as `server_url` on the NATS
    /// notification) rather than the global env-var server URL.
    /// Cheap: `reqwest::Client` is internally reference-counted, so
    /// the inner HTTP client + connection pool are shared.
    pub fn with_server_url(&self, server_url: &str) -> Self {
        Self {
            client: self.client.clone(),
            server_url: server_url.trim_end_matches('/').to_string(),
            sealing_sk: Arc::clone(&self.sealing_sk),
        }
    }

    /// Borrow the server URL this client is currently configured
    /// with.  Useful for log lines that want to surface which
    /// server a particular dispatch is targeting.
    pub fn server_url(&self) -> &str {
        &self.server_url
    }

    /// Create a new control plane client.
    ///
    /// Plain HTTP by default.  When `NOETL_TLS_CLIENT_CERT` +
    /// `NOETL_TLS_CLIENT_KEY` (and optionally `NOETL_TLS_CA`) are set the
    /// client presents a certificate for **mTLS** to the server's TLS
    /// listener (Secrets Wallet Phase 4b, noetl/ai-meta#61).  A TLS
    /// misconfiguration is fatal — a worker that must reach an mTLS server
    /// fails fast rather than silently downgrading to a plain client.
    pub fn new(server_url: &str) -> Self {
        let client = crate::client::tls::build_http_client(Duration::from_secs(30))
            .unwrap_or_else(|e| panic!("control-plane HTTP client init failed: {e:#}"));
        let sealing_sk = StaticSecret::random_from_rng(rand_core::OsRng);

        Self {
            client,
            server_url: server_url.trim_end_matches('/').to_string(),
            sealing_sk: Arc::new(sealing_sk),
        }
    }

    /// Base64-encoded X25519 public key the worker registers as the recipient
    /// for sealed credential responses (Secrets Wallet Phase 5c).  The matching
    /// [`StaticSecret`] stays in-process — only the public half ever leaves.
    pub fn worker_public_key_b64(&self) -> String {
        let pk = PublicKey::from(self.sealing_sk.as_ref());
        B64.encode(pk.as_bytes())
    }

    /// Fetch a credential addressed to this worker as a sealed payload
    /// (Secrets Wallet Phase 5c, server endpoint Phase 5b).
    ///
    /// Calls `GET /api/credentials/{alias}/sealed?worker_id=<worker_id>`,
    /// receives a [`SealedEnvelope`] addressed to this worker's
    /// [`worker_public_key_b64`], unseals it with the long-lived
    /// [`StaticSecret`], and returns the same [`Credential`] shape
    /// [`get_credential`] returns — so the auth-alias resolver can stay
    /// shape-stable across the plaintext / sealed paths.
    ///
    /// **Zeroization.** The intermediate plaintext `Vec<u8>` is wiped with
    /// [`zeroize::Zeroize`] after `serde_json::from_slice` consumes it.  The
    /// returned [`Credential`]'s `data` map still carries the raw secret
    /// (the caller — `auth_alias.rs` — owns that and should zeroize on its
    /// side after the tool dispatch returns).
    pub async fn get_sealed_credential(
        &self,
        alias: &str,
        worker_id: &str,
        execution_id: i64,
    ) -> Result<Option<Credential>> {
        let response = self
            .client
            .get(format!(
                "{}/api/credentials/{}/sealed",
                self.server_url, alias
            ))
            .query(&[
                ("worker_id", worker_id),
                ("execution_id", &execution_id.to_string()),
            ])
            .send()
            .await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            // Typed error so the alias-resolution path can classify
            // retryability by status code (noetl/ai-meta#78).
            return Err(anyhow::Error::new(CredentialHttpError {
                alias: alias.to_string(),
                status: status.as_u16(),
                body: format!("(worker_id='{worker_id}') {body}"),
            }));
        }
        let envelope: crate::client::SealedEnvelope = response.json().await?;
        let mut plaintext = crate::client::sealed_open(&self.sealing_sk, &envelope)?;
        let credential: Credential =
            serde_json::from_slice(&plaintext).map_err(|e| {
                plaintext.zeroize();
                anyhow::anyhow!("get_sealed_credential('{alias}'): decode plaintext: {e}")
            })?;
        // Plaintext bytes wiped — the value is now in `credential.data`,
        // which the caller is responsible for clearing after the tool
        // dispatch consumes it.
        plaintext.zeroize();
        Ok(Some(credential))
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

    /// Resolve a `noetl://` result reference to its stored payload via
    /// `GET /api/result/resolve?ref=<uri>` (references-in-state,
    /// noetl/ai-meta#101 phase 2).  The response body IS the data JSON; `None`
    /// on 404.  Used by the worker's render-time reference resolution so
    /// `{{ step.<bulk_field> }}` templates get the full payload the orchestrator
    /// kept in the store instead of carrying inline.
    pub async fn resolve_ref(&self, uri: &str) -> Result<Option<serde_json::Value>> {
        let response = self
            .client
            .get(format!("{}/api/result/resolve", self.server_url))
            .query(&[("ref", uri)])
            .send()
            .await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Failed to resolve result reference {uri}: {body}");
        }
        let value: serde_json::Value = response.json().await?;
        Ok(Some(value))
    }

    /// Read an object back from the server-mediated object store
    /// (`GET /api/internal/objects/{key}`) — the read half of the Phase B
    /// `object_put`, used by the resolve-by-URN read path (#104 Phase C). Returns
    /// `(bytes, content_type)` or `None` on 404 (so the resolver falls back
    /// fail-safe). The slash/`=`-bearing §7 key rides as the catch-all path,
    /// mirroring `object_put`.
    pub async fn object_get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let response = self
            .client
            .get(format!("{}/api/internal/objects/{}", self.server_url, key))
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("object_get {key} failed: HTTP {} {}", status.as_u16(), body);
        }
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();
        let bytes = response.bytes().await?.to_vec();
        Ok(Some((bytes, content_type)))
    }

    /// Fetch the server-served cell endpoint registry
    /// (`GET /api/internal/cells`, #104 Phase C). Returns the raw JSON; the
    /// resolve-by-URN path deserializes it into its own registry shape.
    pub async fn cell_registry(&self) -> Result<serde_json::Value> {
        let response = self
            .client
            .get(format!("{}/api/internal/cells", self.server_url))
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("cell_registry fetch failed: HTTP {} {}", status.as_u16(), body);
        }
        Ok(response.json().await?)
    }

    /// Resolve a keychain credential alias to its full record.
    ///
    /// Calls `GET /api/credentials/{alias}?include_data=true` on the
    /// server and returns the decrypted payload.  Used by the worker
    /// dispatch path (`executor::command::resolve_auth_alias`) when a
    /// playbook step's `auth:` field is a bare string — see
    /// noetl/ai-meta#48 for the regression brief.
    ///
    /// Per `agents/rules/execution-model.md`'s "Secrets and credentials"
    /// rule the credential body never lives in worker pod env; the
    /// keychain is the source of truth and the worker resolves at
    /// dispatch time.  `execution_id` is forwarded so the server's
    /// keychain cache scopes lookups correctly (and so audit logging
    /// attributes the read to the playbook run).
    ///
    /// Returns `Ok(None)` when the server responds 404 — the caller
    /// converts that into a clear `Credential alias '<name>' not
    /// found in keychain` error so operators see the alias name in
    /// the failure message rather than a serde mismatch.
    pub async fn get_credential(
        &self,
        alias: &str,
        execution_id: i64,
    ) -> Result<Option<Credential>> {
        let response = self
            .client
            .get(format!("{}/api/credentials/{}", self.server_url, alias))
            .query(&[
                ("include_data", "true"),
                ("execution_id", &execution_id.to_string()),
            ])
            .send()
            .await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            // Typed error so the alias-resolution path can classify
            // retryability by status code (noetl/ai-meta#78).
            return Err(anyhow::Error::new(CredentialHttpError {
                alias: alias.to_string(),
                status: status.as_u16(),
                body,
            }));
        }

        let parsed: Credential = response.json().await?;
        Ok(Some(parsed))
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

    /// Write a raw object (Arrow Feather, etc.) to the server's object store at
    /// the §7 physical key — `PUT /api/internal/objects/{key}` (noetl/ai-meta#105).
    /// The server is the digest authority; workers never touch the object store
    /// directly (data-access boundary). `key` may contain slashes (the §7 key);
    /// they pass through to the server's `{*key}` catch-all.
    pub async fn object_put(&self, key: &str, bytes: Vec<u8>, media_type: &str) -> Result<()> {
        let response = self
            .client
            .put(format!("{}/api/internal/objects/{}", self.server_url, key))
            .query(&[("media_type", media_type)])
            .header(reqwest::header::CONTENT_TYPE, media_type)
            .body(bytes)
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("object_put failed: HTTP {} {}", status.as_u16(), body);
        }
        Ok(())
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
        // Phase 5c (noetl/ai-meta#61): include the worker's X25519 sealing
        // public key in the `runtime` JSON blob.  Server's
        // `RuntimeService::get_worker_public_key` reads it from this exact
        // path on a sealed-credential fetch; the field is harmless metadata
        // when the server isn't on the Phase-5b code path.
        let response = self
            .client
            .post(format!("{}/api/worker/pool/register", self.server_url))
            .json(&serde_json::json!({
                "name": worker_id,
                "component_type": "worker_pool",
                "runtime": {
                    "kind": "rust",
                    "worker_public_key": self.worker_public_key_b64(),
                },
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

    // -----------------------------------------------------------------------
    // Subscription continuous runtime (noetl/ai-meta#90 Phase 2)
    // -----------------------------------------------------------------------

    /// Fetch a catalog entry's raw YAML content by path
    /// (`POST /api/catalog/resource`).  The continuous runtime uses this to
    /// load the `kind: Subscription` spec it activates.
    pub async fn get_catalog_content(&self, path: &str) -> Result<String> {
        let response = self
            .client
            .post(format!("{}/api/catalog/resource", self.server_url))
            .json(&serde_json::json!({ "path": path, "version": "latest" }))
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("catalog resource '{path}' fetch failed ({status}): {body}");
        }
        let v: serde_json::Value = response.json().await?;
        v.get("content")
            .and_then(|c| c.as_str())
            .map(str::to_string)
            .ok_or_else(|| anyhow::anyhow!("catalog resource '{path}' has no 'content'"))
    }

    /// Start a per-message execution (`POST /api/execute`), routing the whole
    /// run to `execution_pool` and stamping the W3C `trace` context.  Returns
    /// the new `execution_id`.  This is the continuous runtime's "one
    /// execution per message" call.
    #[allow(clippy::too_many_arguments)]
    pub async fn execute(
        &self,
        path: &str,
        payload: serde_json::Value,
        execution_pool: Option<&str>,
        trace: Option<&serde_json::Value>,
        parent_execution_id: Option<i64>,
        dedup: Option<&serde_json::Value>,
    ) -> Result<i64> {
        let item =
            DispatchItem::new(path, payload, execution_pool, trace, parent_execution_id, dedup);
        let body = item.to_request_body();
        let response = self
            .client
            .post(format!("{}/api/execute", self.server_url))
            .json(&body)
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("execute '{path}' failed ({status}): {text}");
        }
        let v: serde_json::Value = response.json().await?;
        let eid = v
            .get("execution_id")
            .and_then(|e| e.as_str().and_then(|s| s.parse::<i64>().ok()).or_else(|| e.as_i64()))
            .ok_or_else(|| anyhow::anyhow!("execute response missing execution_id: {v}"))?;
        Ok(eid)
    }

    /// Batch-dispatch N executions in one HTTP round-trip
    /// (`POST /api/execute/batch`, noetl/ai-meta#90 Phase 7).
    ///
    /// Each item is a full per-message request (its own path / pool / trace /
    /// dedup), so the directive-resolved routing + trace propagation + opt-in
    /// dedup are preserved exactly as in the per-message path — a batch is N
    /// independent executions in one call, not one shared run.  Returns the
    /// per-item outcomes **in request order**; partial failure is contained
    /// (a bad item is an `error` outcome, the rest still run).
    pub async fn execute_batch(&self, items: &[DispatchItem]) -> Result<Vec<BatchItemOutcome>> {
        let executions: Vec<serde_json::Value> =
            items.iter().map(|i| i.to_request_body()).collect();
        let body = serde_json::json!({ "executions": executions });
        let response = self
            .client
            .post(format!("{}/api/execute/batch", self.server_url))
            .json(&body)
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("execute batch ({} items) failed ({status}): {text}", items.len());
        }
        let resp: BatchExecuteResponse = response.json().await?;
        Ok(resp.results)
    }

    /// Register a `kind: Subscription` and return its lifecycle id + state
    /// (`POST /api/subscriptions/register`).
    pub async fn subscription_register(&self, path: &str) -> Result<SubscriptionStatus> {
        let response = self
            .client
            .post(format!("{}/api/subscriptions/register", self.server_url))
            .json(&serde_json::json!({ "path": path }))
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("subscription register '{path}' failed ({status}): {body}");
        }
        Ok(response.json().await?)
    }

    /// Apply a lifecycle transition (`activate` / `pause` / `resume` /
    /// `drain` / `deactivate`) — `POST /api/subscriptions/{id}/{action}`.
    pub async fn subscription_lifecycle(
        &self,
        subscription_id: i64,
        action: &str,
    ) -> Result<SubscriptionStatus> {
        let response = self
            .client
            .post(format!(
                "{}/api/subscriptions/{}/{}",
                self.server_url, subscription_id, action
            ))
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("subscription {subscription_id} {action} failed ({status}): {body}");
        }
        Ok(response.json().await?)
    }

    /// Read a subscription's current lifecycle state
    /// (`GET /api/subscriptions/{id}`).
    pub async fn subscription_get(&self, subscription_id: i64) -> Result<SubscriptionStatus> {
        let response = self
            .client
            .get(format!(
                "{}/api/subscriptions/{}",
                self.server_url, subscription_id
            ))
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("subscription {subscription_id} get failed ({status}): {body}");
        }
        Ok(response.json().await?)
    }
}

/// One execute request for the single or batch dispatch path
/// (noetl/ai-meta#90 Phase 7).  Owns its fields so a batch can be assembled
/// before the HTTP call.
#[derive(Debug, Clone)]
pub struct DispatchItem {
    pub path: String,
    pub payload: serde_json::Value,
    pub execution_pool: Option<String>,
    pub trace: Option<serde_json::Value>,
    pub parent_execution_id: Option<i64>,
    /// Opt-in server-side dedup block (`{ "key", "window_secs" }`); only set
    /// when the subscription declares `dedup.enabled: true`.
    pub dedup: Option<serde_json::Value>,
}

impl DispatchItem {
    pub fn new(
        path: &str,
        payload: serde_json::Value,
        execution_pool: Option<&str>,
        trace: Option<&serde_json::Value>,
        parent_execution_id: Option<i64>,
        dedup: Option<&serde_json::Value>,
    ) -> Self {
        DispatchItem {
            path: path.to_string(),
            payload,
            execution_pool: execution_pool.map(str::to_string),
            trace: trace.cloned(),
            parent_execution_id,
            dedup: dedup.cloned(),
        }
    }

    /// Render to the `/api/execute` JSON request body (also one element of a
    /// batch `executions` array).
    pub fn to_request_body(&self) -> serde_json::Value {
        let mut body = serde_json::json!({ "path": self.path, "payload": self.payload });
        if let serde_json::Value::Object(ref mut m) = body {
            if let Some(pool) = self.execution_pool.as_ref() {
                m.insert("execution_pool".to_string(), serde_json::json!(pool));
            }
            if let Some(t) = self.trace.as_ref() {
                m.insert("trace".to_string(), t.clone());
            }
            if let Some(parent) = self.parent_execution_id {
                m.insert("parent_execution_id".to_string(), serde_json::json!(parent));
            }
            if let Some(d) = self.dedup.as_ref() {
                m.insert("dedup".to_string(), d.clone());
            }
        }
        body
    }
}

/// One per-item result from `POST /api/execute/batch`.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct BatchItemOutcome {
    pub index: usize,
    pub status: String,
    #[serde(default)]
    pub execution_id: Option<String>,
    #[serde(default)]
    pub commands_generated: i32,
    #[serde(default)]
    pub error: Option<String>,
}

impl BatchItemOutcome {
    /// The created (or deduplicated) execution id, parsed.  `None` for an
    /// `error` item.
    pub fn execution_id_i64(&self) -> Option<i64> {
        self.execution_id.as_ref().and_then(|s| s.parse().ok())
    }

    /// Whether the item created or deduplicated an execution (not an error).
    pub fn is_ok(&self) -> bool {
        self.status != "error"
    }
}

/// The `POST /api/execute/batch` response envelope.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct BatchExecuteResponse {
    #[serde(default)]
    pub count: usize,
    #[serde(default)]
    pub started: usize,
    #[serde(default)]
    pub duplicates: usize,
    #[serde(default)]
    pub failed: usize,
    pub results: Vec<BatchItemOutcome>,
}

/// Subscription lifecycle status returned by the `/api/subscriptions` routes.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SubscriptionStatus {
    pub subscription_id: String,
    pub path: String,
    #[serde(default)]
    pub catalog_id: String,
    pub state: String,
    #[serde(default)]
    pub last_event_type: String,
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

    // -----------------------------------------------------------------
    // `get_credential` integration tests (noetl/ai-meta#48)
    //
    // Use the same axum-based mock-server pattern as the put_result
    // tests above.  Each test spawns a tiny HTTP server on
    // 127.0.0.1:0, exercises the client against it, then asserts.
    // -----------------------------------------------------------------

    use axum::{
        extract::{Path, Query},
        routing::get,
        Json, Router,
    };
    use std::collections::HashMap;
    use tokio::net::TcpListener;

    async fn spawn_credential_server(
        response: Option<serde_json::Value>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let canned = response.clone();
        let app = Router::new().route(
            "/api/credentials/{alias}",
            get(
                move |Path(alias): Path<String>, Query(qs): Query<HashMap<String, String>>| {
                    let canned = canned.clone();
                    async move {
                        // include_data + execution_id are the contract
                        // — assert them so the test fails fast if the
                        // client forgets a query param.
                        assert_eq!(
                            qs.get("include_data").map(String::as_str),
                            Some("true"),
                            "get_credential must request include_data=true"
                        );
                        assert!(
                            qs.contains_key("execution_id"),
                            "get_credential must forward execution_id"
                        );
                        match canned {
                            Some(body) => Ok(Json(body)),
                            None => Err(axum::http::StatusCode::NOT_FOUND),
                        }
                        .map(|json| (axum::http::StatusCode::OK, json))
                        .or_else(|status| {
                            Err::<(axum::http::StatusCode, Json<serde_json::Value>), _>(status)
                        })
                        .map(|(_status, body)| {
                            // Echo the alias so the test can assert
                            // the URL was routed correctly.
                            let mut body = body;
                            if let Some(map) = body.0.as_object_mut() {
                                map.insert(
                                    "_test_routed_alias".to_string(),
                                    serde_json::Value::String(alias.clone()),
                                );
                            }
                            body
                        })
                    }
                },
            ),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let base = format!("http://{}", addr);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("axum serve");
        });
        (base, handle)
    }

    #[tokio::test]
    async fn get_credential_decodes_postgres_credential_response() {
        let (base, _handle) = spawn_credential_server(Some(serde_json::json!({
            "id": "611677533586588198",
            "name": "pg_local",
            "type": "postgres",
            "data": {
                "db_host": "postgres.local",
                "db_port": "5432",
                "db_user": "demo",
                "db_password": "demo_pw",
                "db_name": "demo_noetl"
            },
            "tags": ["dev", "postgres"],
            "description": "Local Postgres for tests"
        })))
        .await;
        let client = ControlPlaneClient::new(&base);

        let cred = client
            .get_credential("pg_local", 42)
            .await
            .expect("get_credential ok")
            .expect("credential present");

        assert_eq!(cred.name, "pg_local");
        assert_eq!(cred.cred_type, "postgres");
        assert_eq!(
            cred.data.get("db_host").and_then(|v| v.as_str()),
            Some("postgres.local")
        );
        assert_eq!(cred.tags, vec!["dev", "postgres"]);
    }

    #[tokio::test]
    async fn get_credential_returns_none_on_404() {
        let (base, _handle) = spawn_credential_server(None).await;
        let client = ControlPlaneClient::new(&base);

        let result = client
            .get_credential("does_not_exist", 1)
            .await
            .expect("get_credential ok");
        assert!(result.is_none(), "404 must map to Ok(None)");
    }

    #[tokio::test]
    async fn get_credential_propagates_unexpected_http_error() {
        // The default route on this router returns 404 for any URL
        // not matching `/api/credentials/{alias}`.  Point the client
        // at an unbound port to force a connection error.
        let client = ControlPlaneClient::new("http://127.0.0.1:1");
        let err = client.get_credential("anything", 1).await.unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("error sending request")
                || msg.contains("Connection refused")
                || msg.contains("os error"),
            "expected a transport-level error, got: {msg}"
        );
    }

    /// noetl/ai-meta#53 Gap 1: `with_server_url` returns a fresh
    /// client pointed at a different server.  The original client's
    /// URL stays untouched (clients are immutable from the outside)
    /// so the global startup client isn't accidentally mutated by
    /// a per-dispatch override.
    #[test]
    fn test_with_server_url_returns_independent_client() {
        let original = ControlPlaneClient::new("http://noetl.noetl.svc:8082");
        assert_eq!(original.server_url(), "http://noetl.noetl.svc:8082");

        let overridden = original.with_server_url("http://noetl-server-rust.noetl.svc:8082");
        assert_eq!(
            overridden.server_url(),
            "http://noetl-server-rust.noetl.svc:8082"
        );

        // Original keeps its URL.
        assert_eq!(original.server_url(), "http://noetl.noetl.svc:8082");
    }

    /// `with_server_url` strips a trailing slash so callers can
    /// pass either form without `format!` doubling up the path
    /// separator at downstream call sites.
    #[test]
    fn test_with_server_url_trims_trailing_slash() {
        let c = ControlPlaneClient::new("http://x/").with_server_url("http://y:8082/");
        assert_eq!(c.server_url(), "http://y:8082");
    }
}
