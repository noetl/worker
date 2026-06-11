//! Continuous subscription runtime (RFC Mode B).
//!
//! Phase 2 of the subscription/listener RFC
//! ([noetl/ai-meta#90](https://github.com/noetl/ai-meta/issues/90)).
//!
//! ### What this is
//!
//! A long-lived run-mode of the worker binary (selected by
//! `WORKER_MODE=subscription`).  Where the ordinary worker *pulls commands and
//! runs blocks*, this runtime *pulls source messages and emits executions*: it
//! holds a `kind: Subscription`'s source open and turns **each received
//! message into one `POST /api/execute`** on the subscription's **dedicated
//! pool segment** (`noetl.commands.<pool>.<eid>`), isolated from the shared
//! command stream.
//!
//! It reuses the Phase-1 [`SourceClient`](noetl_tools::tools::source::SourceClient)
//! `poll` in a loop (`build_source` factory) and the Phase-2 header-directive
//! engine ([`DirectiveSpec`](noetl_tools::tools::source::DirectiveSpec)) to
//! resolve per-message redirect / pool / idempotency / content / W3C-trace
//! directives before dispatch.
//!
//! Crucially the runtime **does not execute playbook logic** and **never
//! touches `noetl.*` tables** — it calls the server API
//! (`agents/rules/data-access-boundary.md`).  It is an ingress producer; the
//! work runs on the dedicated worker pool.
//!
//! ### Lifecycle
//!
//! `register → activate` on startup; the loop honors `pause`/`resume` by
//! polling the subscription's server-side state; `drain → deactivate` on
//! shutdown.  Every transition is event-logged server-side.
//!
//! ### Ack semantics (Phase 2)
//!
//! The Phase-1 bounded `poll` acks per batch, so this runtime uses
//! **ack-on-fetch** (`AckMode::OnSuccess`): the drain acks the batch, then each
//! message is turned into an execution.  At-least-once with ack-after-dispatch
//! and durable store-and-forward (spool) is **Phase 4** (RFC §8, OQ14); a
//! dispatch failure here is logged + counted, and (until the spool lands) the
//! message is not redelivered.  Pull sources back-pressure naturally: the loop
//! fetches a bounded batch and waits when the source is empty.

use std::time::Duration;

use anyhow::{Context, Result};
use noetl_tools::tools::source::{AckMode, DirectiveSpec, DispatchPlan, PollOptions, PolledMessage};
use noetl_tools::tools::{build_source, SubscriptionConfig};
use noetl_tools::ExecutionContext;

use crate::client::ControlPlaneClient;
use crate::config::WorkerConfig;

/// Hard cap on the runtime poll batch, mirroring the bounded-drain tool.
const RUNTIME_BATCH_DEFAULT: u32 = 100;
/// Idle backoff when a poll returns no messages (avoids a busy spin on an
/// empty source beyond the poll's own `timeout_ms`).
const POLL_IDLE_MS: u64 = 500;
/// How often the loop re-reads the subscription's lifecycle state to honor
/// pause/resume/drain without a server round-trip per message.
const STATE_CHECK_INTERVAL: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Parsed spec
// ---------------------------------------------------------------------------

/// The fields the runtime needs from a `kind: Subscription` spec.
#[derive(Debug, Clone)]
pub struct ParsedSpec {
    /// Connection config for [`build_source`] (source + stream/consumer/…).
    pub source_cfg: SubscriptionConfig,
    /// Credential alias for the source (resolved via the server credentials API).
    pub auth_alias: Option<String>,
    /// Default target playbook to run per message.
    pub default_playbook: String,
    /// Which part of the message becomes the playbook body.
    pub payload_from: String,
    /// Default downstream command segment (`dispatch.execution_pool`).
    pub default_pool: Option<String>,
    /// The header-directive allowlist (RFC §7).
    pub directives: DirectiveSpec,
    /// Poll batch size.
    pub batch: u32,
    /// Poll wait.
    pub timeout_ms: Option<u64>,
}

/// Parse a `kind: Subscription` catalog YAML into the runtime's [`ParsedSpec`].
pub fn parse_spec(yaml: &serde_yaml::Value) -> Result<ParsedSpec> {
    let spec = yaml
        .get("spec")
        .context("subscription YAML missing 'spec'")?;

    let source = spec
        .get("source")
        .and_then(|v| v.as_str())
        .context("subscription spec missing 'source'")?
        .to_string();

    let auth_alias = spec
        .get("auth")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    // Connection config: copy the source + every connection key the
    // SubscriptionConfig understands.  build_source resolves the credential
    // alias (set as a secret on the context below) for the secret bits.
    let mut cfg = serde_json::Map::new();
    cfg.insert("source".to_string(), serde_json::json!(source));
    for key in [
        "url",
        "user",
        "password",
        "token",
        "stream",
        "consumer",
        "subscription",
        "endpoint",
        "topic",
        "group",
        "brokers",
    ] {
        if let Some(v) = spec.get(key) {
            cfg.insert(key.to_string(), serde_json::to_value(v)?);
        }
    }
    if let Some(alias) = &auth_alias {
        cfg.insert("auth".to_string(), serde_json::json!(alias));
    }

    // runtime.{batch,timeout_ms} knobs.
    let runtime = spec.get("runtime");
    let batch = runtime
        .and_then(|r| r.get("batch"))
        .and_then(|v| v.as_u64())
        .map(|b| b as u32)
        .unwrap_or(RUNTIME_BATCH_DEFAULT);
    let timeout_ms = runtime
        .and_then(|r| r.get("timeout_ms"))
        .and_then(|v| v.as_u64());

    let source_cfg: SubscriptionConfig = serde_json::from_value(serde_json::Value::Object(cfg))
        .context("subscription spec did not yield a valid source config")?;

    // dispatch block.
    let dispatch = spec
        .get("dispatch")
        .context("subscription spec missing 'dispatch'")?;
    let default_playbook = dispatch
        .get("playbook")
        .and_then(|v| v.as_str())
        .context("subscription spec missing 'dispatch.playbook'")?
        .to_string();
    let payload_from = dispatch
        .get("payload_from")
        .and_then(|v| v.as_str())
        .unwrap_or("message.json")
        .to_string();
    let default_pool = dispatch
        .get("execution_pool")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    // headers (directive allowlist) — optional.
    let directives = match spec.get("headers") {
        Some(h) => {
            let json = serde_json::to_value(h)
                .context("subscription spec 'headers' is not serializable")?;
            DirectiveSpec::parse(&json)
                .map_err(|e| anyhow::anyhow!("invalid subscription 'headers' block: {e}"))?
        }
        None => DirectiveSpec::default(),
    };

    Ok(ParsedSpec {
        source_cfg,
        auth_alias,
        default_playbook,
        payload_from,
        default_pool,
        directives,
        batch,
        timeout_ms,
    })
}

/// Build the per-message execution payload from a polled message, honoring
/// `payload_from` and the resolved directive plan.
///
/// The dispatched playbook always sees the full normalized envelope under
/// `message` (`id` / `data` / `headers` / `attributes` / `metadata`); the
/// `payload_from` selection is merged to the top level (a JSON object body) or
/// placed under `body` (a scalar body), so both `{{ workload.message.data.x }}`
/// and `{{ workload.x }}` resolve.  Idempotency key + content type from
/// directives ride alongside.
pub fn build_payload(
    msg: &PolledMessage,
    payload_from: &str,
    plan: &DispatchPlan,
    subscription_path: &str,
    source: &str,
) -> serde_json::Value {
    let mut payload = serde_json::Map::new();
    payload.insert(
        "message".to_string(),
        serde_json::to_value(msg).unwrap_or(serde_json::Value::Null),
    );
    payload.insert("subscription".to_string(), serde_json::json!(subscription_path));
    payload.insert("source".to_string(), serde_json::json!(source));

    let primary = match payload_from {
        "message.attributes" => msg.attributes.clone(),
        "message.body" => match &msg.data {
            serde_json::Value::String(s) => serde_json::Value::String(s.clone()),
            other => serde_json::Value::String(other.to_string()),
        },
        // "message.json" (default): the decoded body.
        _ => msg.data.clone(),
    };
    match primary {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                payload.entry(k).or_insert(v);
            }
        }
        other => {
            payload.insert("body".to_string(), other);
        }
    }

    if let Some(k) = plan.idempotency_key.as_ref() {
        payload.insert("idempotency_key".to_string(), serde_json::json!(k));
    }
    if let Some(c) = plan.content_type.as_ref() {
        payload.insert("content_type".to_string(), serde_json::json!(c));
    }

    serde_json::Value::Object(payload)
}

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

/// The continuous subscription runtime.
pub struct SubscriptionRuntime {
    client: ControlPlaneClient,
    worker_id: String,
    subscription_path: String,
    metrics_bind: String,
}

impl SubscriptionRuntime {
    /// Build the runtime from worker config + `NOETL_SUBSCRIPTION_PATH`.
    pub fn new(worker_cfg: &WorkerConfig) -> Result<Self> {
        let subscription_path = std::env::var("NOETL_SUBSCRIPTION_PATH").context(
            "WORKER_MODE=subscription requires NOETL_SUBSCRIPTION_PATH (the kind: Subscription \
             catalog path to activate)",
        )?;
        Ok(Self {
            client: ControlPlaneClient::new(&worker_cfg.server_url),
            worker_id: worker_cfg.worker_id.clone(),
            subscription_path,
            metrics_bind: worker_cfg.metrics_bind.clone(),
        })
    }

    /// Run the continuous loop until `shutdown` resolves or the subscription is
    /// deactivated.  Honors pause/resume; drains + deactivates on exit.
    pub async fn run<F>(&self, shutdown: F) -> Result<()>
    where
        F: std::future::Future<Output = ()>,
    {
        // Expose the subscription counters at /metrics (observability.md P2).
        if let Err(e) = crate::metrics_server::spawn(&self.metrics_bind).await {
            tracing::warn!(bind = %self.metrics_bind, error = %e, "metrics server bind failed (continuing)");
        }

        // 1. Load + parse the subscription spec.
        let yaml_str = self
            .client
            .get_catalog_content(&self.subscription_path)
            .await
            .with_context(|| format!("load subscription spec '{}'", self.subscription_path))?;
        let yaml: serde_yaml::Value = serde_yaml::from_str(&yaml_str)
            .with_context(|| format!("parse subscription YAML '{}'", self.subscription_path))?;
        let spec = parse_spec(&yaml)?;

        // 2. Build the source client (resolve the credential alias up front).
        let mut ctx = ExecutionContext::default();
        if let Some(alias) = spec.auth_alias.as_ref() {
            match self.client.get_credential(alias, 0).await {
                Ok(Some(cred)) => {
                    let cred_json = serde_json::to_string(&cred.data).unwrap_or_default();
                    ctx.set_secret(alias.clone(), cred_json);
                }
                Ok(None) => {
                    anyhow::bail!("subscription credential alias '{alias}' not found");
                }
                Err(e) => return Err(e).with_context(|| format!("resolve credential '{alias}'")),
            }
        }
        let source = build_source(&spec.source_cfg, &ctx)
            .map_err(|e| anyhow::anyhow!("build source: {e}"))?;
        let source_name = source.source_name();

        // 3. Register + activate the subscription (event-logged server-side).
        let registered = self
            .client
            .subscription_register(&self.subscription_path)
            .await
            .context("register subscription")?;
        let subscription_id: i64 = registered
            .subscription_id
            .parse()
            .context("server returned a non-numeric subscription_id")?;
        self.client
            .subscription_lifecycle(subscription_id, "activate")
            .await
            .context("activate subscription")?;

        tracing::info!(
            subscription_id,
            path = %self.subscription_path,
            source = source_name,
            playbook = %spec.default_playbook,
            pool = spec.default_pool.as_deref().unwrap_or("(default)"),
            batch = spec.batch,
            "Subscription runtime activated"
        );

        let opts = PollOptions::new(Some(spec.batch), spec.timeout_ms, AckMode::OnSuccess);

        // 4. The loop.
        let result = self
            .run_loop(&*source, source_name, subscription_id, &spec, &opts, shutdown)
            .await;

        // 5. Drain + deactivate on the way out (best-effort).
        if let Err(e) = self.client.subscription_lifecycle(subscription_id, "drain").await {
            tracing::warn!(subscription_id, error = %e, "drain transition failed on shutdown");
        }
        if let Err(e) = self
            .client
            .subscription_lifecycle(subscription_id, "deactivate")
            .await
        {
            tracing::warn!(subscription_id, error = %e, "deactivate transition failed on shutdown");
        }
        tracing::info!(subscription_id, path = %self.subscription_path, "Subscription runtime stopped");

        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_loop<F>(
        &self,
        source: &dyn noetl_tools::tools::source::SourceClient,
        source_name: &str,
        subscription_id: i64,
        spec: &ParsedSpec,
        opts: &PollOptions,
        shutdown: F,
    ) -> Result<()>
    where
        F: std::future::Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        let mut paused = false;
        let mut last_state_check = std::time::Instant::now()
            .checked_sub(STATE_CHECK_INTERVAL)
            .unwrap_or_else(std::time::Instant::now);

        loop {
            // Periodically reconcile lifecycle state (pause/resume/deactivate).
            if last_state_check.elapsed() >= STATE_CHECK_INTERVAL {
                last_state_check = std::time::Instant::now();
                match self.client.subscription_get(subscription_id).await {
                    Ok(status) => match status.state.as_str() {
                        "PAUSED" => {
                            if !paused {
                                tracing::info!(subscription_id, "subscription paused");
                            }
                            paused = true;
                        }
                        "DRAINING" | "DEACTIVATED" => {
                            tracing::info!(subscription_id, state = %status.state, "subscription stopping");
                            break;
                        }
                        _ => {
                            if paused {
                                tracing::info!(subscription_id, "subscription resumed");
                            }
                            paused = false;
                        }
                    },
                    Err(e) => {
                        tracing::warn!(subscription_id, error = %e, "lifecycle state check failed");
                    }
                }
            }

            if paused {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => { tracing::info!(subscription_id, "shutdown signal received"); break; }
                    _ = tokio::time::sleep(Duration::from_millis(POLL_IDLE_MS)) => {}
                }
                continue;
            }

            // One bounded drain, racing the shutdown signal so a `poll`
            // waiting out its `timeout_ms` doesn't delay shutdown.
            let outcome = {
                let poll_span = tracing::info_span!(
                    "subscription.runtime.poll",
                    source = source_name,
                    subscription_id,
                );
                let _guard = poll_span.enter();
                tokio::select! {
                    biased;
                    _ = &mut shutdown => { tracing::info!(subscription_id, "shutdown signal received"); break; }
                    r = source.poll(opts) => r,
                }
            };
            let outcome = match outcome {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(subscription_id, source = source_name, error = %e, "source poll failed");
                    tokio::time::sleep(Duration::from_millis(POLL_IDLE_MS)).await;
                    continue;
                }
            };
            let received = outcome.count() as u64;

            if received == 0 {
                tokio::time::sleep(Duration::from_millis(POLL_IDLE_MS)).await;
                continue;
            }

            let mut dispatched = 0u64;
            let mut errors = 0u64;
            for msg in &outcome.messages {
                match self
                    .dispatch_message(msg, spec, source_name, subscription_id)
                    .await
                {
                    Ok(()) => dispatched += 1,
                    Err(e) => {
                        errors += 1;
                        tracing::warn!(
                            subscription_id,
                            source = source_name,
                            message_id = %msg.id,
                            error = %e,
                            "message dispatch failed (ack-on-fetch; spool is Phase 4)"
                        );
                    }
                }
            }
            crate::metrics::record_subscription_batch(source_name, received, dispatched, errors);
        }
        Ok(())
    }

    /// Resolve directives for one message and POST /api/execute.
    async fn dispatch_message(
        &self,
        msg: &PolledMessage,
        spec: &ParsedSpec,
        source_name: &str,
        subscription_id: i64,
    ) -> Result<()> {
        let plan = spec.directives.resolve(&msg.headers);

        let playbook = plan
            .playbook_override
            .clone()
            .unwrap_or_else(|| spec.default_playbook.clone());
        let pool = plan
            .execution_pool_override
            .clone()
            .or_else(|| spec.default_pool.clone());
        let trace = plan
            .trace
            .as_ref()
            .and_then(|t| serde_json::to_value(t).ok());

        let payload = build_payload(msg, &spec.payload_from, &plan, &self.subscription_path, source_name);

        let exec_span = tracing::info_span!(
            "subscription.dispatch",
            source = source_name,
            subscription_id,
            message_id = %msg.id,
            playbook = %playbook,
            pool = pool.as_deref().unwrap_or("(default)"),
        );
        let _g = exec_span.enter();

        let execution_id = self
            .client
            .execute(
                &playbook,
                payload,
                pool.as_deref(),
                trace.as_ref(),
                // The subscription is the parent execution; per-message runs
                // are its children (carries trace inheritance + audit lineage).
                Some(subscription_id),
            )
            .await?;

        tracing::info!(
            subscription_id,
            execution_id,
            message_id = %msg.id,
            playbook = %playbook,
            "dispatched one execution per message"
        );

        // Audit the applied directives (RFC §7.6) — best-effort.
        if !plan.applied.is_empty() || plan.trace.is_some() {
            for d in &plan.applied {
                crate::metrics::record_subscription_directive(&d.controls);
            }
            self.emit_directives_applied(execution_id, msg, &plan).await;
        }

        Ok(())
    }

    /// Emit a `subscription.message.directives_applied` event (RFC §7.6) keyed
    /// by the child execution id.  Best-effort: a failure here never blocks
    /// dispatch.
    async fn emit_directives_applied(
        &self,
        execution_id: i64,
        msg: &PolledMessage,
        plan: &DispatchPlan,
    ) {
        let context = serde_json::json!({
            "message_id": msg.id,
            "applied": plan.applied,
            "route_override": {
                "playbook": plan.playbook_override,
                "pool": plan.execution_pool_override,
            },
            "trace": plan.trace,
        });
        let event = crate::client::ExecutorEvent {
            execution_id,
            event_type: "subscription.message.directives_applied".to_string(),
            step: "ingress".to_string(),
            status: "APPLIED".to_string(),
            created_at: chrono::Utc::now(),
            context,
            event_id: None,
            worker_id: Some(self.worker_id.clone()),
            meta: Some(serde_json::json!({ "emitter": "subscription_runtime" })),
        };
        if let Err(e) = self.client.emit_event(event).await {
            tracing::debug!(execution_id, error = %e, "directives_applied audit emit failed (non-fatal)");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn yaml(s: &str) -> serde_yaml::Value {
        serde_yaml::from_str(s).unwrap()
    }

    fn msg(data: serde_json::Value, headers: serde_json::Value) -> PolledMessage {
        PolledMessage {
            id: "stream:1".to_string(),
            data,
            headers: headers.as_object().cloned().unwrap_or_default(),
            attributes: json!({}),
            metadata: json!({}),
            ack_id: None,
        }
    }

    #[test]
    fn parse_spec_extracts_dispatch_and_directives() {
        let spec = parse_spec(&yaml(
            r#"
kind: Subscription
spec:
  source: nats
  auth: nats_main
  stream: ORDERS
  consumer: orders-drain
  runtime: { batch: 50, timeout_ms: 3000 }
  dispatch: { playbook: domain/process_order, payload_from: message.json, execution_pool: iot }
  headers:
    directives:
      - header: x-noetl-route
        controls: dispatch.playbook
        allowed: ["domain/a", "domain/b"]
    trace: { propagate: w3c }
"#,
        ))
        .unwrap();
        assert_eq!(spec.source_cfg.source, "nats");
        assert_eq!(spec.auth_alias.as_deref(), Some("nats_main"));
        assert_eq!(spec.default_playbook, "domain/process_order");
        assert_eq!(spec.default_pool.as_deref(), Some("iot"));
        assert_eq!(spec.batch, 50);
        assert_eq!(spec.timeout_ms, Some(3000));
        assert_eq!(spec.source_cfg.stream.as_deref(), Some("ORDERS"));
    }

    #[test]
    fn parse_spec_requires_dispatch_playbook() {
        let err = parse_spec(&yaml(
            "kind: Subscription\nspec:\n  source: nats\n  stream: S\n  consumer: C\n  dispatch: {}\n",
        ))
        .unwrap_err();
        assert!(format!("{err}").contains("dispatch.playbook"));
    }

    #[test]
    fn parse_spec_defaults_directives_off() {
        let spec = parse_spec(&yaml(
            "kind: Subscription\nspec:\n  source: nats\n  stream: S\n  consumer: C\n  dispatch: { playbook: p }\n",
        ))
        .unwrap();
        // No headers block → resolving any headers is a no-op.
        let plan = spec.directives.resolve(&json!({ "x-anything": "v" }).as_object().unwrap().clone());
        assert!(plan.is_noop());
        assert_eq!(spec.payload_from, "message.json");
    }

    #[test]
    fn build_payload_merges_json_body_to_top_level() {
        let spec = parse_spec(&yaml(
            "kind: Subscription\nspec:\n  source: nats\n  stream: S\n  consumer: C\n  dispatch: { playbook: p }\n",
        ))
        .unwrap();
        let m = msg(json!({ "order_id": 42, "amount": 9 }), json!({}));
        let plan = spec.directives.resolve(&m.headers);
        let payload = build_payload(&m, &spec.payload_from, &plan, "subscriptions/orders", "nats");
        // Body merged to top level.
        assert_eq!(payload["order_id"], 42);
        assert_eq!(payload["amount"], 9);
        // Full envelope preserved.
        assert_eq!(payload["message"]["data"]["order_id"], 42);
        assert_eq!(payload["subscription"], "subscriptions/orders");
        assert_eq!(payload["source"], "nats");
    }

    #[test]
    fn build_payload_scalar_body_under_body_key() {
        let m = msg(json!("raw-text"), json!({}));
        let payload = build_payload(&m, "message.json", &DispatchPlan::default(), "p", "nats");
        assert_eq!(payload["body"], "raw-text");
    }

    #[test]
    fn directive_redirect_resolves_target() {
        let spec = parse_spec(&yaml(
            r#"
kind: Subscription
spec:
  source: nats
  stream: S
  consumer: C
  dispatch: { playbook: domain/default, execution_pool: shared }
  headers:
    directives:
      - header: x-noetl-route
        controls: dispatch.playbook
        allowed: ["domain/fraud", "domain/billing"]
      - header: x-noetl-pool
        controls: dispatch.execution_pool
        allowed: ["priority", "iot"]
"#,
        ))
        .unwrap();
        let m = msg(json!({}), json!({ "x-noetl-route": "domain/fraud", "x-noetl-pool": "priority" }));
        let plan = spec.directives.resolve(&m.headers);
        assert_eq!(plan.playbook_override.as_deref(), Some("domain/fraud"));
        assert_eq!(plan.execution_pool_override.as_deref(), Some("priority"));
    }
}
