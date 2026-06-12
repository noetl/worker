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
use noetl_tools::spool::SpoolSpec;
use noetl_tools::tools::source::{AckMode, DirectiveSpec, DispatchPlan, PollOptions, PolledMessage};
use noetl_tools::tools::{build_source, SubscriptionConfig};
use noetl_tools::ExecutionContext;

use crate::client::ControlPlaneClient;
use crate::config::WorkerConfig;
use crate::spool_runtime::{Routing, SpoolRuntime};

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
    /// Store-and-forward spool config (RFC §8, Phase 4). [`SpoolSpec::off`]
    /// when no `spool:` block is declared.
    pub spool: SpoolSpec,
    /// Poll batch size.
    pub batch: u32,
    /// Poll wait.
    pub timeout_ms: Option<u64>,
    /// Dispatch a drained batch via `POST /api/execute/batch` instead of one
    /// HTTP round-trip per message (noetl/ai-meta#90 Phase 7, RFC §10 OQ12).
    /// Default off — the per-message path stays the live-proven default.
    pub batch_dispatch: bool,
    /// Cap on messages per batch HTTP call when `batch_dispatch` is on.
    pub batch_max: u32,
    /// Opt-in exactly-once dedup window (noetl/ai-meta#90 Phase 7, RFC §10
    /// OQ1).  `None` → no dedup (the default).
    pub dedup: Option<DedupCfg>,
    /// Per-subscription rate limit / backpressure caps (RFC §9).
    pub limits: LimitsCfg,
}

/// Opt-in dedup config (noetl/ai-meta#90 Phase 7).  The runtime stamps a
/// `dedup` block onto every `/api/execute` so the server collapses a duplicate
/// delivery (same key within the window) to a single execution.
#[derive(Debug, Clone)]
pub struct DedupCfg {
    /// Bounded dedup window in seconds.
    pub window_secs: u64,
}

/// Per-subscription rate-limit / backpressure caps (noetl/ai-meta#90 Phase 7,
/// RFC §9).  Both default to unlimited (`None`).
#[derive(Debug, Clone, Default)]
pub struct LimitsCfg {
    /// Most un-dispatched messages held at once — clamps the poll batch.
    pub max_in_flight: Option<u32>,
    /// Token-bucket dispatch rate; when exhausted the runtime stops fetching.
    pub max_dispatch_per_sec: Option<u32>,
}

/// Default dedup window when `dedup.enabled` is set without a `window_secs`.
const DEFAULT_DEDUP_WINDOW_SECS: u64 = 300;

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

    // Phase 7 scale knobs (noetl/ai-meta#90).
    let batch_dispatch = dispatch
        .get("batch_dispatch")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let batch_max = dispatch
        .get("batch_max")
        .and_then(|v| v.as_u64())
        .map(|n| n as u32)
        .filter(|n| *n > 0)
        .unwrap_or(batch);

    // dedup block (opt-in exactly-once window, RFC §10 OQ1) — only honored
    // when `enabled: true`.
    let dedup = match spec.get("dedup") {
        Some(d) if d.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false) => {
            let window_secs = d
                .get("window_secs")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(DEFAULT_DEDUP_WINDOW_SECS);
            Some(DedupCfg { window_secs })
        }
        _ => None,
    };

    // limits block (rate limit / backpressure, RFC §9) — both optional.
    let limits = match spec.get("limits") {
        Some(l) => LimitsCfg {
            max_in_flight: l
                .get("max_in_flight")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .map(|n| n as u32),
            max_dispatch_per_sec: l
                .get("max_dispatch_per_sec")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .map(|n| n as u32),
        },
        None => LimitsCfg::default(),
    };

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

    // spool block (RFC §8, Phase 4) — optional; absent → off.
    let spool = match spec.get("spool") {
        Some(s) => {
            let json = serde_json::to_value(s)
                .context("subscription spec 'spool' is not serializable")?;
            SpoolSpec::parse(Some(&json))
                .map_err(|e| anyhow::anyhow!("invalid subscription 'spool' block: {e}"))?
        }
        None => SpoolSpec::off(),
    };

    Ok(ParsedSpec {
        source_cfg,
        auth_alias,
        default_playbook,
        payload_from,
        default_pool,
        directives,
        spool,
        batch,
        timeout_ms,
        batch_dispatch,
        batch_max,
        dedup,
        limits,
    })
}

/// Resolve the dedup key for a message: the `idempotency_key` header directive
/// wins, falling back to the source `message_id` (RFC §10 OQ8).  Returns the
/// `dedup` block to stamp on the execute request, or `None` when the
/// subscription hasn't opted in.
fn dedup_block(
    dedup: &Option<DedupCfg>,
    plan: &DispatchPlan,
    msg: &PolledMessage,
) -> Option<serde_json::Value> {
    dedup.as_ref().map(|cfg| {
        let key = plan
            .idempotency_key
            .clone()
            .unwrap_or_else(|| msg.id.clone());
        serde_json::json!({ "key": key, "window_secs": cfg.window_secs })
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
    /// Platform NATS URL — the spool's `nats_object` backend + circuit KV
    /// connect here (a runtime credential, direct access allowed).
    nats_url: String,
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
            nats_url: worker_cfg.nats_url.clone(),
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

        // 3b. Build the store-and-forward spool runtime (RFC §8, Phase 4) —
        // `None` when the spec declares no buffering (`spool.mode: off`).
        let mut spool = SpoolRuntime::build(
            &spec.spool,
            &self.nats_url,
            self.client.clone(),
            self.worker_id.clone(),
            self.subscription_path.clone(),
            subscription_id,
            source_name.to_string(),
            spec.default_playbook.clone(),
            spec.default_pool.clone(),
        )
        .await
        .context("build spool runtime")?;
        if let Some(s) = &spool {
            let _ = s; // built; the loop drives it.
            tracing::info!(subscription_id, "store-and-forward spool enabled");
        }

        // 4. The loop.
        let result = self
            .run_loop(
                &*source,
                source_name,
                subscription_id,
                &spec,
                spool.as_mut(),
                shutdown,
            )
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
        mut spool: Option<&mut SpoolRuntime>,
        shutdown: F,
    ) -> Result<()>
    where
        F: std::future::Future<Output = ()>,
    {
        use crate::ratelimit::{FetchPlan, RateGovernor};
        tokio::pin!(shutdown);
        let mut paused = false;
        let mut last_state_check = std::time::Instant::now()
            .checked_sub(STATE_CHECK_INTERVAL)
            .unwrap_or_else(std::time::Instant::now);

        // Per-subscription rate-limit / backpressure governor (RFC §9).  Both
        // caps default to unlimited; when set, they throttle the fetch side so
        // an over-limit subscription stops pulling (source keeps the backlog,
        // no loss) instead of flooding the control plane.
        let mut governor = RateGovernor::new(
            spec.limits.max_in_flight,
            spec.limits.max_dispatch_per_sec,
            std::time::Instant::now(),
        );
        if !governor.is_unlimited() {
            tracing::info!(
                subscription_id,
                max_in_flight = ?spec.limits.max_in_flight,
                max_dispatch_per_sec = ?spec.limits.max_dispatch_per_sec,
                "per-subscription rate limits engaged"
            );
        }

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

            // Spool maintenance (RFC §8, Phase 4): probe declared downstreams
            // on the configured cadence; when one recovers (circuit closes),
            // drain its backlog in order before resuming live (or interleaved
            // per `drain.on_recovery`).
            if let Some(s) = spool.as_deref_mut() {
                let recovered = s.maybe_probe().await;
                if !recovered.is_empty() && s.drain_before_live() {
                    if let Err(e) = s.drain(&spec.payload_from).await {
                        tracing::warn!(subscription_id, error = %e, "spool drain failed (will retry)");
                    }
                }
            }

            // Rate-limit / backpressure planning (RFC §9).  The governor decides
            // how deep the next poll may be; when the dispatch-rate budget is
            // exhausted it returns Throttle and we skip the poll entirely so the
            // unfetched messages stay in the source (no loss, source redelivers).
            let fetch_batch = match governor.plan_fetch(spec.batch, std::time::Instant::now()) {
                FetchPlan::Throttle { wait, newly_limited } => {
                    if newly_limited {
                        crate::metrics::record_subscription_rate_limited(source_name, "dispatch_rate");
                        self.emit_rate_limited(subscription_id, "dispatch_rate", &spec.limits)
                            .await;
                        tracing::info!(
                            subscription_id,
                            source = source_name,
                            wait_ms = wait.as_millis() as u64,
                            "rate limit engaged — pausing fetch (source retains backlog, no loss)"
                        );
                    }
                    tokio::select! {
                        biased;
                        _ = &mut shutdown => { tracing::info!(subscription_id, "shutdown signal received"); break; }
                        _ = tokio::time::sleep(wait) => {}
                    }
                    continue;
                }
                FetchPlan::Fetch { batch, newly_limited } => {
                    if newly_limited {
                        crate::metrics::record_subscription_rate_limited(source_name, "max_in_flight");
                        self.emit_rate_limited(subscription_id, "max_in_flight", &spec.limits)
                            .await;
                        tracing::info!(
                            subscription_id,
                            source = source_name,
                            batch,
                            "in-flight cap clamped the fetch batch (backpressure)"
                        );
                    }
                    batch
                }
            };
            let opts = PollOptions::new(Some(fetch_batch), spec.timeout_ms, AckMode::OnSuccess);

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
                    r = source.poll(&opts) => r,
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
            // Charge the rate budget for what was actually pulled (RFC §9).
            governor.record_fetched(received as u32, std::time::Instant::now());

            if received == 0 {
                tokio::time::sleep(Duration::from_millis(POLL_IDLE_MS)).await;
                continue;
            }

            let mut dispatched = 0u64;
            let mut errors = 0u64;
            let mut spooled = 0u64;

            // Phase 1 — resolve each message's directive plan and route it
            // through the spool/circuit.  A message whose downstream circuit is
            // open is durably buffered here (already acked by the poll → no
            // loss); the rest are collected as dispatch-eligible.
            let mut eligible: Vec<(usize, DispatchPlan)> = Vec::with_capacity(outcome.messages.len());
            for (idx, msg) in outcome.messages.iter().enumerate() {
                let plan = spec.directives.resolve(&msg.headers);
                if let Some(s) = spool.as_deref_mut() {
                    match s.route_message(msg, &plan).await {
                        Routing::Spooled => {
                            spooled += 1;
                            continue;
                        }
                        Routing::Dropped => {
                            errors += 1;
                            continue;
                        }
                        Routing::Dispatch => {}
                    }
                }
                eligible.push((idx, plan));
            }

            // Phase 2 — dispatch the eligible messages.  When batch_dispatch is
            // on and there's more than one, collapse them into
            // `POST /api/execute/batch` calls of up to `batch_max`; each item
            // still carries its own directive-resolved playbook/pool/trace/dedup
            // so per-message traceability is intact.  Otherwise fall back to the
            // per-message path (the live-proven default).
            if spec.batch_dispatch && eligible.len() > 1 {
                for chunk in eligible.chunks(spec.batch_max.max(1) as usize) {
                    let (d, e) = self
                        .dispatch_chunk(
                            &outcome.messages,
                            chunk,
                            spec,
                            source_name,
                            subscription_id,
                            spool.as_deref_mut(),
                        )
                        .await;
                    dispatched += d;
                    errors += e;
                }
            } else {
                for (idx, plan) in &eligible {
                    let msg = &outcome.messages[*idx];
                    let res = self
                        .dispatch_message(msg, plan, spec, source_name, subscription_id)
                        .await;
                    if let Some(s) = spool.as_deref_mut() {
                        s.report_dispatch(plan, msg, res.is_ok()).await;
                    }
                    match res {
                        Ok(()) => dispatched += 1,
                        Err(e) => {
                            errors += 1;
                            tracing::warn!(
                                subscription_id,
                                source = source_name,
                                message_id = %msg.id,
                                error = %e,
                                "message dispatch failed"
                            );
                        }
                    }
                }
            }
            crate::metrics::record_subscription_batch(source_name, received, dispatched, errors);
            if spooled > 0 {
                tracing::info!(subscription_id, source = source_name, spooled, "messages buffered to spool (circuit open)");
            }
        }
        Ok(())
    }

    /// Dispatch a chunk of eligible messages via `POST /api/execute/batch`
    /// (noetl/ai-meta#90 Phase 7).  Builds one [`DispatchItem`] per message —
    /// each with its directive-resolved playbook / pool / trace / dedup — so a
    /// batch is N independent executions in one HTTP round-trip with per-message
    /// traceability intact.  Returns `(dispatched, errors)`.  A whole-batch HTTP
    /// failure reports every item failed to the circuit breaker (and, when a
    /// spool is configured, that signal is what trips it for the next round);
    /// per-item failures inside a 200 response are contained.
    async fn dispatch_chunk(
        &self,
        messages: &[PolledMessage],
        chunk: &[(usize, DispatchPlan)],
        spec: &ParsedSpec,
        source_name: &str,
        subscription_id: i64,
        mut spool: Option<&mut SpoolRuntime>,
    ) -> (u64, u64) {
        let items: Vec<crate::client::DispatchItem> = chunk
            .iter()
            .map(|(idx, plan)| {
                let msg = &messages[*idx];
                let playbook = plan
                    .playbook_override
                    .clone()
                    .unwrap_or_else(|| spec.default_playbook.clone());
                let pool = plan
                    .execution_pool_override
                    .clone()
                    .or_else(|| spec.default_pool.clone());
                let trace = plan.trace.as_ref().and_then(|t| serde_json::to_value(t).ok());
                let dedup = dedup_block(&spec.dedup, plan, msg);
                let payload =
                    build_payload(msg, &spec.payload_from, plan, &self.subscription_path, source_name);
                crate::client::DispatchItem::new(
                    &playbook,
                    payload,
                    pool.as_deref(),
                    trace.as_ref(),
                    Some(subscription_id),
                    dedup.as_ref(),
                )
            })
            .collect();

        let batch_span = tracing::info_span!(
            "subscription.dispatch.batch",
            source = source_name,
            subscription_id,
            batch_size = items.len(),
        );
        let _g = batch_span.enter();

        crate::metrics::record_subscription_batch_dispatch(source_name, items.len() as u64);

        match self.client.execute_batch(&items).await {
            Ok(results) => {
                let mut dispatched = 0u64;
                let mut errors = 0u64;
                for (slot, (idx, plan)) in chunk.iter().enumerate() {
                    let msg = &messages[*idx];
                    // Results are returned in request order; correlate by slot.
                    let ok = results.get(slot).map(|r| r.is_ok()).unwrap_or(false);
                    if let Some(s) = spool.as_deref_mut() {
                        s.report_dispatch(plan, msg, ok).await;
                    }
                    match results.get(slot) {
                        Some(r) if r.is_ok() => {
                            dispatched += 1;
                            if let Some(eid) = r.execution_id_i64() {
                                self.audit_directives(eid, msg, plan).await;
                            }
                        }
                        Some(r) => {
                            errors += 1;
                            tracing::warn!(
                                subscription_id,
                                source = source_name,
                                message_id = %msg.id,
                                error = r.error.as_deref().unwrap_or("unknown"),
                                "batch item failed"
                            );
                        }
                        None => {
                            errors += 1;
                            tracing::warn!(
                                subscription_id,
                                source = source_name,
                                message_id = %msg.id,
                                "batch response missing item for slot {slot}"
                            );
                        }
                    }
                }
                tracing::info!(
                    subscription_id,
                    source = source_name,
                    dispatched,
                    errors,
                    "batch dispatch complete"
                );
                (dispatched, errors)
            }
            Err(e) => {
                // Whole-batch HTTP failure: report every item failed so the
                // circuit breaker sees the downstream as down for next round.
                tracing::warn!(
                    subscription_id,
                    source = source_name,
                    batch_size = chunk.len(),
                    error = %e,
                    "batch dispatch failed (whole chunk)"
                );
                for (idx, plan) in chunk {
                    let msg = &messages[*idx];
                    if let Some(s) = spool.as_deref_mut() {
                        s.report_dispatch(plan, msg, false).await;
                    }
                }
                (0, chunk.len() as u64)
            }
        }
    }

    /// POST /api/execute for one message using a pre-resolved directive
    /// [`DispatchPlan`] (the caller resolves it once so the spool can route
    /// on the same plan before deciding dispatch-vs-spool).
    async fn dispatch_message(
        &self,
        msg: &PolledMessage,
        plan: &DispatchPlan,
        spec: &ParsedSpec,
        source_name: &str,
        subscription_id: i64,
    ) -> Result<()> {
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

        // Opt-in exactly-once dedup block (RFC §10 OQ1) — only stamped when the
        // subscription declared `dedup.enabled: true`.
        let dedup = dedup_block(&spec.dedup, plan, msg);

        let payload = build_payload(msg, &spec.payload_from, plan, &self.subscription_path, source_name);

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
                dedup.as_ref(),
            )
            .await?;

        tracing::info!(
            subscription_id,
            execution_id,
            message_id = %msg.id,
            playbook = %playbook,
            "dispatched one execution per message"
        );

        self.audit_directives(execution_id, msg, plan).await;

        Ok(())
    }

    /// Record directive metrics + emit the `directives_applied` audit event
    /// (RFC §7.6) for one dispatched execution.  Shared by the per-message and
    /// batch dispatch paths; best-effort.
    async fn audit_directives(&self, execution_id: i64, msg: &PolledMessage, plan: &DispatchPlan) {
        if plan.applied.is_empty() && plan.trace.is_none() {
            return;
        }
        for d in &plan.applied {
            crate::metrics::record_subscription_directive(&d.controls);
        }
        self.emit_directives_applied(execution_id, msg, plan).await;
    }

    /// Emit a `subscription.rate_limited` event (RFC §9) on the subscription's
    /// lifecycle log when a per-subscription limit engages.  Emitted on the
    /// off→on edge only (not per message) so it's an auditable signal without
    /// flooding the event log.  Best-effort.
    async fn emit_rate_limited(&self, subscription_id: i64, reason: &str, limits: &LimitsCfg) {
        let context = serde_json::json!({
            "reason": reason,
            "max_in_flight": limits.max_in_flight,
            "max_dispatch_per_sec": limits.max_dispatch_per_sec,
            "action": "stopped_fetching",
            "loss_safe": true,
        });
        let event = crate::client::ExecutorEvent {
            execution_id: subscription_id,
            event_type: "subscription.rate_limited".to_string(),
            step: "ingress".to_string(),
            status: "RATE_LIMITED".to_string(),
            created_at: chrono::Utc::now(),
            context,
            event_id: None,
            worker_id: Some(self.worker_id.clone()),
            meta: Some(serde_json::json!({ "emitter": "subscription_runtime" })),
        };
        if let Err(e) = self.client.emit_event(event).await {
            tracing::debug!(subscription_id, error = %e, "rate_limited audit emit failed (non-fatal)");
        }
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
    fn parse_spec_defaults_spool_off_when_absent() {
        let spec = parse_spec(&yaml(
            "kind: Subscription\nspec:\n  source: nats\n  stream: S\n  consumer: C\n  dispatch: { playbook: p }\n",
        ))
        .unwrap();
        assert!(!spec.spool.buffers());
    }

    #[test]
    fn parse_spec_extracts_spool_block() {
        let spec = parse_spec(&yaml(
            r#"
kind: Subscription
spec:
  source: nats
  stream: IOT
  consumer: iot-drain
  dispatch: { playbook: domain/ingest, execution_pool: iot }
  spool:
    mode: buffer_and_ack
    backend: nats_object
    bucket: noetl_spool_iot
    ordering: per_key
    ordering_key: device_id
    circuit:
      trip_after: 3
      probe_after_ms: 5000
      probe_interval_ms: 2000
      downstream:
        - { name: warehouse, type: http, target: "http://warehouse.svc/health" }
    retention: { max_bytes: 1048576, on_full: drop_to_dlq }
    drain: { max_replay_attempts: 4, on_recovery: ordered_then_live }
"#,
        ))
        .unwrap();
        assert!(spec.spool.buffers());
        assert_eq!(spec.spool.mode.as_str(), "buffer_and_ack");
        assert_eq!(spec.spool.backend.as_str(), "nats_object");
        assert_eq!(spec.spool.bucket.as_deref(), Some("noetl_spool_iot"));
        assert_eq!(spec.spool.ordering.as_str(), "per_key");
        assert_eq!(spec.spool.ordering_key.as_deref(), Some("device_id"));
        assert_eq!(spec.spool.circuit.trip_after, 3);
        assert_eq!(spec.spool.circuit.downstream.len(), 1);
        assert_eq!(spec.spool.circuit.downstream[0].name, "warehouse");
        assert_eq!(spec.spool.drain.max_replay_attempts, 4);
    }

    #[test]
    fn parse_spec_rejects_invalid_spool() {
        // buffer_and_ack with nats_object but no bucket → reject.
        let err = parse_spec(&yaml(
            "kind: Subscription\nspec:\n  source: nats\n  stream: S\n  consumer: C\n  dispatch: { playbook: p }\n  spool: { mode: buffer_and_ack, backend: nats_object }\n",
        ))
        .unwrap_err();
        assert!(format!("{err}").contains("spool"));
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

    // -----------------------------------------------------------------------
    // Phase 7 — batch dispatch / dedup / limits parsing + dedup key resolution
    // -----------------------------------------------------------------------

    #[test]
    fn parse_spec_defaults_phase7_off() {
        let spec = parse_spec(&yaml(
            "kind: Subscription\nspec:\n  source: nats\n  stream: S\n  consumer: C\n  dispatch: { playbook: p }\n",
        ))
        .unwrap();
        assert!(!spec.batch_dispatch);
        assert_eq!(spec.batch_max, spec.batch); // defaults to runtime batch
        assert!(spec.dedup.is_none());
        assert!(spec.limits.max_in_flight.is_none());
        assert!(spec.limits.max_dispatch_per_sec.is_none());
    }

    #[test]
    fn parse_spec_extracts_phase7_knobs() {
        let spec = parse_spec(&yaml(
            r#"
kind: Subscription
spec:
  source: nats
  stream: ORDERS
  consumer: orders-drain
  runtime: { batch: 500 }
  dispatch: { playbook: domain/order, batch_dispatch: true, batch_max: 100 }
  dedup: { enabled: true, window_secs: 600 }
  limits: { max_in_flight: 1000, max_dispatch_per_sec: 200 }
"#,
        ))
        .unwrap();
        assert!(spec.batch_dispatch);
        assert_eq!(spec.batch_max, 100);
        assert_eq!(spec.dedup.as_ref().unwrap().window_secs, 600);
        assert_eq!(spec.limits.max_in_flight, Some(1000));
        assert_eq!(spec.limits.max_dispatch_per_sec, Some(200));
    }

    #[test]
    fn parse_spec_dedup_disabled_when_enabled_false() {
        let spec = parse_spec(&yaml(
            "kind: Subscription\nspec:\n  source: nats\n  stream: S\n  consumer: C\n  dispatch: { playbook: p }\n  dedup: { enabled: false, window_secs: 99 }\n",
        ))
        .unwrap();
        assert!(spec.dedup.is_none(), "enabled: false → no dedup");
    }

    #[test]
    fn parse_spec_dedup_default_window() {
        let spec = parse_spec(&yaml(
            "kind: Subscription\nspec:\n  source: nats\n  stream: S\n  consumer: C\n  dispatch: { playbook: p }\n  dedup: { enabled: true }\n",
        ))
        .unwrap();
        assert_eq!(spec.dedup.as_ref().unwrap().window_secs, DEFAULT_DEDUP_WINDOW_SECS);
    }

    #[test]
    fn dedup_block_prefers_idempotency_key_over_message_id() {
        let cfg = Some(DedupCfg { window_secs: 120 });
        let m = msg(json!({}), json!({}));
        // With an idempotency_key on the plan → that key wins.
        let plan = DispatchPlan {
            idempotency_key: Some("idem-123".to_string()),
            ..Default::default()
        };
        let block = dedup_block(&cfg, &plan, &m).unwrap();
        assert_eq!(block["key"], "idem-123");
        assert_eq!(block["window_secs"], 120);
        // Without one → falls back to message_id (msg.id).
        let block = dedup_block(&cfg, &DispatchPlan::default(), &m).unwrap();
        assert_eq!(block["key"], m.id);
    }

    #[test]
    fn dedup_block_none_when_not_opted_in() {
        let m = msg(json!({}), json!({}));
        assert!(dedup_block(&None, &DispatchPlan::default(), &m).is_none());
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
