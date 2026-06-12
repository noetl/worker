//! Store-and-forward spool wiring for the subscription runtime — Phase 4 of
//! the subscription/listener RFC
//! ([noetl/ai-meta#90](https://github.com/noetl/ai-meta/issues/90), RFC §8).
//!
//! The spool *engine* + *circuit breaker* logic lives in
//! [`noetl_tools::spool`] (pure, unit-tested). This module is the runtime
//! glue: it stands up the engine over a backend, persists circuit state in
//! NATS KV, drives the active downstream probes, routes each message
//! through the breaker (dispatch / spool), drains on recovery, and emits the
//! event-log trail so an entire outage is replayable.
//!
//! ### The contract that makes it loss-safe
//!
//! 1. The bounded `poll` already **acked** the batch on fetch
//!    (`AckMode::OnSuccess`), so a message in hand is no longer on the
//!    source. In `buffer_and_ack` mode we **durably store it in the spool
//!    before doing anything else** — so a down downstream never loses it.
//! 2. The circuit **only drains after the active probe confirms the
//!    downstream is reachable** — so replay (which dispatches asynchronously
//!    and can't itself observe the playbook's downstream write) happens into
//!    a downstream that is up. The probe gates the drain.
//! 3. Ordering / idempotency / dead-letter / retention are enforced by
//!    [`noetl_tools::spool::SpoolEngine`] — the unit-tested core.
//!
//! ### Scope (Phase 4)
//!
//! `buffer_and_ack` (the push default) and `hybrid` are wired loss-safe
//! here; `hybrid` currently buffers whenever the circuit is open (the
//! cost-optimised "stop-ack short blips first" escalation needs the
//! ack-after-dispatch poll-model change, RFC OQ14, and is tracked). `off`
//! means spool disabled (the Phase-2 ack-on-fetch behaviour). Backends wired
//! in the in-cluster runtime: `nats_object` (default) + `local_disk`;
//! `gcs`/`s3` are implemented as the same trait and tracked for the
//! Cloud-Run/tenant-bucket path.

use anyhow::{Context, Result};
use noetl_tools::spool::{
    probe_downstream, Admission, CircuitDecision, CircuitRegistry, GcsBackend, LocalDiskBackend,
    NatsObjectBackend, SpoolBackend, SpoolBackendKind, SpoolEngine, SpoolItem, SpoolMode, SpoolSpec,
};
use noetl_tools::tools::source::{DispatchPlan, PolledMessage};

use crate::client::{ControlPlaneClient, ExecutorEvent};

/// Wall-clock epoch millis. The spool/circuit logic takes `now_ms` as an
/// argument so the core stays deterministic + testable; the runtime feeds
/// the real clock here.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// What the runtime should do with a message after the spool routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Routing {
    /// Circuit closed (or probe) — dispatch the message normally.
    Dispatch,
    /// Circuit open — the message was spooled (already durable); skip
    /// dispatch.
    Spooled,
    /// `on_full: stop_acking` and the ceiling is hit — the message could not
    /// be spooled; the source must redeliver. (Best-effort: the poll already
    /// acked, so this is surfaced as a WARN + a dropped event; the durable
    /// guarantee is `max_bytes` is set high enough, or `on_full: drop_to_dlq`.)
    Dropped,
}

/// Per-subscription spool + circuit runtime.
pub struct SpoolRuntime {
    engine: SpoolEngine,
    circuits: CircuitRegistry,
    kv: Option<async_nats::jetstream::kv::Store>,
    kv_key: String,
    client: ControlPlaneClient,
    worker_id: String,
    subscription_path: String,
    subscription_id: i64,
    source_name: String,
    default_playbook: String,
    default_pool: Option<String>,
    probe_interval_ms: u64,
    last_probe_ms: u64,
    recv_seq: u64,
}

impl SpoolRuntime {
    /// Build the spool runtime for a subscription, or `None` when the spec
    /// declares no buffering (`spool.mode: off` / absent).
    #[allow(clippy::too_many_arguments)]
    pub async fn build(
        spec: &SpoolSpec,
        nats_url: &str,
        client: ControlPlaneClient,
        worker_id: String,
        subscription_path: String,
        subscription_id: i64,
        source_name: String,
        default_playbook: String,
        default_pool: Option<String>,
    ) -> Result<Option<Self>> {
        if !spec.buffers() {
            return Ok(None);
        }

        // Build the durable backend + a dead-letter sibling.
        let (backend, dlq, kv): (
            Box<dyn SpoolBackend>,
            Box<dyn SpoolBackend>,
            Option<async_nats::jetstream::kv::Store>,
        ) = match spec.backend {
            SpoolBackendKind::NatsObject => {
                let js = connect_jetstream(nats_url).await?;
                let bucket = spec
                    .bucket
                    .clone()
                    .context("spool.backend nats_object requires a bucket")?;
                let backend = NatsObjectBackend::open(&js, &bucket).await.map_err(de)?;
                let dlq = NatsObjectBackend::open(&js, &format!("{bucket}_dlq"))
                    .await
                    .map_err(de)?;
                let kv = open_circuit_kv(&js, subscription_id).await;
                (Box::new(backend), Box::new(dlq), kv)
            }
            SpoolBackendKind::LocalDisk => {
                let path = spec
                    .path
                    .clone()
                    .context("spool.backend local_disk requires a path")?;
                let backend = LocalDiskBackend::open(&path).await.map_err(de)?;
                let dlq = LocalDiskBackend::open(format!("{path}/dlq"))
                    .await
                    .map_err(de)?;
                // local_disk circuit state lives next to the spool, not KV.
                (Box::new(backend), Box::new(dlq), None)
            }
            SpoolBackendKind::Gcs => {
                // The out-of-cluster (Cloud Run) spool backend, RFC #90 Phase 5.
                // Authenticates with ADC — the runtime service account via
                // Workload Identity on Cloud Run, or the gcloud ADC file
                // locally ("already-in-place trust", execution-model.md). One
                // bucket holds both the live spool and the dead-letter sibling,
                // separated by prefix; `recv_seq`-keyed objects list in receive
                // order for `ordering: global`.
                let bucket = spec
                    .bucket
                    .clone()
                    .context("spool.backend gcs requires a bucket")?;
                let prefix = format!("{subscription_path}/spool");
                let dlq_prefix = format!("{subscription_path}/dlq");
                let backend = GcsBackend::open(&bucket, &prefix).await.map_err(de)?;
                let dlq = GcsBackend::open(&bucket, &dlq_prefix).await.map_err(de)?;
                // Circuit state is in-memory for the Cloud Run runtime: there
                // is no in-cluster NATS to reach for a KV bucket, and the
                // service holds the subscription for its lifetime. A restart
                // mid-outage re-probes from `closed` and re-opens on the next
                // failure — correct, just without persisted breaker phase.
                // Persisting circuit state to a server KV endpoint is tracked
                // for the Cloud-Run hardening pass (RFC §8.6).
                (Box::new(backend), Box::new(dlq), None)
            }
            other => {
                anyhow::bail!(
                    "spool.backend '{}' is implemented as a SpoolBackend but not yet wired in the \
                     runtime (s3 backend tracked: tenant-bucket path); \
                     use nats_object, local_disk, or gcs",
                    other.as_str()
                );
            }
        };

        let mut circuits = CircuitRegistry::new(&spec.circuit);
        let kv_key = format!("circuit.{subscription_id}");
        // Rehydrate breaker state from KV (survives a runtime restart
        // mid-outage, RFC §8.1).
        if let Some(store) = &kv {
            if let Ok(Some(entry)) = store.get(&kv_key).await {
                if let Ok(snapshot) = serde_json::from_slice(&entry) {
                    circuits.restore(&snapshot);
                    tracing::info!(subscription_id, "restored circuit state from KV");
                }
            }
        }

        let probe_interval_ms = spec.circuit.probe_interval_ms;
        let engine = SpoolEngine::new(spec.clone(), backend, dlq);

        tracing::info!(
            subscription_id,
            mode = spec.mode.as_str(),
            backend = spec.backend.as_str(),
            ordering = spec.ordering.as_str(),
            downstreams = circuits.downstreams().count(),
            "spool runtime active"
        );

        Ok(Some(Self {
            engine,
            circuits,
            kv,
            kv_key,
            client,
            worker_id,
            subscription_path,
            subscription_id,
            source_name,
            default_playbook,
            default_pool,
            probe_interval_ms,
            last_probe_ms: 0,
            recv_seq: 0,
        }))
    }

    /// Resolve a message to its downstream + circuit decision. The resolved
    /// target (directive pool override, else the default pool) is matched to
    /// a declared downstream (OQ7: decide on the *resolved* target).
    fn route(&mut self, plan: &DispatchPlan) -> (String, CircuitDecision) {
        let resolved = plan
            .execution_pool_override
            .as_deref()
            .or(self.default_pool.as_deref());
        let downstream = self.circuits.route(resolved).to_string();
        let now = now_ms();
        let decision = self.circuits.breaker_mut(&downstream).decide(now);
        (downstream, decision)
    }

    /// Route one message: dispatch when closed, spool when open. Returns the
    /// [`Routing`] the caller acts on (the caller dispatches on
    /// [`Routing::Dispatch`]).
    pub async fn route_message(
        &mut self,
        msg: &PolledMessage,
        plan: &DispatchPlan,
    ) -> Routing {
        let (downstream, decision) = self.route(plan);
        match decision {
            // Closed → dispatch; HalfOpen probe is also a dispatch attempt
            // (the caller reports the outcome via `report_dispatch`).
            CircuitDecision::Dispatch | CircuitDecision::Probe => Routing::Dispatch,
            CircuitDecision::Spool => self.spool(msg, plan, &downstream, "circuit_open").await,
        }
    }

    /// Spool one message durably + emit `subscription.message.spooled`.
    async fn spool(
        &mut self,
        msg: &PolledMessage,
        plan: &DispatchPlan,
        downstream: &str,
        reason: &str,
    ) -> Routing {
        let now = now_ms();
        self.recv_seq += 1;
        let ordering_key = self.resolve_ordering_key(msg);
        let item = SpoolItem::new(
            self.subscription_path.clone(),
            self.source_name.clone(),
            msg.clone(),
            plan.idempotency_key.clone(),
            self.recv_seq,
            ordering_key,
            downstream.to_string(),
            reason,
            now,
        );

        // Retention ceiling (the cost bound, OQ3).
        let incoming = item.to_bytes().len() as u64;
        match self.engine.admit(now, incoming).await {
            Ok(Admission::Accept) => {}
            Ok(Admission::AcceptWithAlert { spool_bytes }) => {
                self.emit(
                    self.subscription_id,
                    "subscription.spool.alert",
                    "ALERT",
                    serde_json::json!({ "downstream": downstream, "spool_bytes": spool_bytes }),
                )
                .await;
            }
            Ok(Admission::AcceptAfterEvict(evicted)) => {
                for d in evicted {
                    self.emit_dead_letter(&d).await;
                }
            }
            Ok(Admission::RejectStopAck) => {
                tracing::warn!(
                    subscription_id = self.subscription_id,
                    downstream,
                    message_id = %msg.id,
                    "spool at retention ceiling (on_full=stop_acking); message not buffered"
                );
                self.emit(
                    self.subscription_id,
                    "subscription.message.dropped",
                    "DROPPED",
                    serde_json::json!({ "message_id": msg.id, "downstream": downstream, "reason": "retention_full" }),
                )
                .await;
                return Routing::Dropped;
            }
            Err(e) => {
                tracing::error!(subscription_id = self.subscription_id, error = %e, "spool admit failed");
                return Routing::Dropped;
            }
        }

        match self.engine.spool(&item).await {
            Ok(spooled) => {
                crate::metrics::record_subscription_spooled(&self.source_name);
                self.update_spool_gauge().await;
                self.emit(
                    self.subscription_id,
                    "subscription.message.spooled",
                    "SPOOLED",
                    serde_json::json!({
                        "message_id": msg.id,
                        "recv_seq": spooled.recv_seq,
                        "spool_ref": spooled.spool_ref,
                        "sha256": spooled.sha256,
                        "downstream": downstream,
                        "reason": reason,
                    }),
                )
                .await;
                Routing::Spooled
            }
            Err(e) => {
                tracing::error!(
                    subscription_id = self.subscription_id,
                    message_id = %msg.id,
                    error = %e,
                    "spool write failed — message NOT durable"
                );
                Routing::Dropped
            }
        }
    }

    /// Report a live-dispatch outcome to the breaker (passive feed). A
    /// dispatch-call failure (server unreachable / 5xx) for the routed
    /// downstream increments the breaker; success records the dedup key so a
    /// later spooled duplicate is deduped on drain.
    pub async fn report_dispatch(&mut self, plan: &DispatchPlan, msg: &PolledMessage, ok: bool) {
        let resolved = plan
            .execution_pool_override
            .as_deref()
            .or(self.default_pool.as_deref());
        let downstream = self.circuits.route(resolved).to_string();
        let now = now_ms();
        if ok {
            let dedup = plan
                .idempotency_key
                .clone()
                .unwrap_or_else(|| msg.id.clone());
            self.engine.mark_dispatched(&dedup);
            let closed = self.circuits.breaker_mut(&downstream).record_success(now);
            if closed {
                self.on_circuit_closed(&downstream).await;
            }
        } else {
            let opened = self.circuits.breaker_mut(&downstream).record_failure(now);
            if opened {
                self.on_circuit_opened(&downstream).await;
            }
        }
    }

    /// Run the active downstream probes if the interval elapsed. Probes the
    /// declared downstreams, feeds the breakers, and emits circuit
    /// transitions. Returns the downstreams that just closed (recovered) so
    /// the caller can trigger a drain.
    pub async fn maybe_probe(&mut self) -> Vec<String> {
        let now = now_ms();
        if now.saturating_sub(self.last_probe_ms) < self.probe_interval_ms {
            return Vec::new();
        }
        self.last_probe_ms = now;

        let specs: Vec<_> = self.circuits.downstreams().cloned().collect();
        let mut recovered = Vec::new();
        for ds in specs {
            let Some(up) = probe_downstream(&ds).await else {
                continue; // passive — no active probe signal
            };
            let breaker = self.circuits.breaker_mut(&ds.name);
            if up {
                if breaker.record_success(now) {
                    self.on_circuit_closed(&ds.name).await;
                    recovered.push(ds.name.clone());
                }
            } else if breaker.record_failure(now) {
                self.on_circuit_opened(&ds.name).await;
            }
        }
        self.persist_circuit().await;
        recovered
    }

    /// Drain the spool for a recovered downstream: replay each item in
    /// order (idempotency + dead-letter via the engine), POSTing
    /// `/api/execute` and emitting `subscription.message.replayed` per item.
    pub async fn drain(&mut self, payload_from: &str) -> Result<()> {
        let pending = self.engine.len().await.unwrap_or(0);
        if pending == 0 {
            return Ok(());
        }
        self.emit(
            self.subscription_id,
            "subscription.spool.draining",
            "DRAINING",
            serde_json::json!({ "pending": pending }),
        )
        .await;

        // Capture what the per-item dispatch closure needs (it can't borrow
        // `self` while `self.engine` is borrowed mutably by `drain`).
        let client = self.client.clone();
        let worker_id = self.worker_id.clone();
        let subscription_id = self.subscription_id;
        let subscription_path = self.subscription_path.clone();
        let source_name = self.source_name.clone();
        let default_playbook = self.default_playbook.clone();
        let default_pool = self.default_pool.clone();
        let payload_from = payload_from.to_string();

        let report = self
            .engine
            .drain(|item: SpoolItem| {
                let client = client.clone();
                let worker_id = worker_id.clone();
                let subscription_path = subscription_path.clone();
                let source_name = source_name.clone();
                let default_playbook = default_playbook.clone();
                let default_pool = default_pool.clone();
                let payload_from = payload_from.clone();
                async move {
                    // Re-resolve the directive plan for the replayed message
                    // so routing/trace match the live path.
                    let plan = DispatchPlan::default();
                    let playbook = item
                        .message
                        .headers
                        .get("x-noetl-route")
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                        .unwrap_or_else(|| default_playbook.clone());
                    let payload = crate::subscription::build_payload(
                        &item.message,
                        &payload_from,
                        &plan,
                        &subscription_path,
                        &source_name,
                    );
                    let execution_id = client
                        .execute(
                            &playbook,
                            payload,
                            default_pool.as_deref(),
                            None,
                            Some(subscription_id),
                            None,
                        )
                        .await
                        .map_err(|e| {
                            noetl_tools::ToolError::ExecutionFailed(format!("replay execute: {e}"))
                        })?;
                    // Per-item replayed audit (best-effort).
                    let event = ExecutorEvent {
                        execution_id,
                        event_type: "subscription.message.replayed".to_string(),
                        step: "ingress".to_string(),
                        status: "REPLAYED".to_string(),
                        created_at: chrono::Utc::now(),
                        context: serde_json::json!({
                            "message_id": item.message_id,
                            "recv_seq": item.recv_seq,
                            "spool_ref": item.spool_ref(),
                            "execution_id": execution_id,
                        }),
                        event_id: None,
                        worker_id: Some(worker_id.clone()),
                        meta: Some(serde_json::json!({ "emitter": "spool_drain" })),
                    };
                    let _ = client.emit_event(event).await;
                    Ok(())
                }
            })
            .await
            .map_err(de)?;

        for d in &report.dead_lettered {
            self.emit_dead_letter(d).await;
        }
        self.update_spool_gauge().await;
        tracing::info!(
            subscription_id = self.subscription_id,
            replayed = report.replayed,
            deduped = report.deduped,
            dead_lettered = report.dead_lettered.len(),
            remaining = report.remaining,
            fully_drained = report.fully_drained,
            "spool drain pass complete"
        );
        Ok(())
    }

    /// Whether the runtime should drain backlog before resuming live (RFC
    /// `drain.on_recovery: ordered_then_live`).
    pub fn drain_before_live(&self) -> bool {
        self.engine.drain_before_live()
    }

    /// True when the hybrid mode should always-buffer at this point (circuit
    /// open beyond the escalation window). For `buffer_and_ack` the answer is
    /// always "buffer when open"; `hybrid` collapses to the same loss-safe
    /// behaviour in Phase 4 (the stop-ack-blip optimisation is OQ14, tracked).
    pub fn always_buffers_when_open(&self) -> bool {
        matches!(
            self.engine.spec().mode,
            SpoolMode::BufferAndAck | SpoolMode::Hybrid
        )
    }

    fn resolve_ordering_key(&self, msg: &PolledMessage) -> Option<String> {
        let key_name = self.engine.spec().ordering_key.as_deref()?;
        msg.headers
            .get(key_name)
            .and_then(|v| v.as_str())
            .map(str::to_string)
    }

    async fn on_circuit_opened(&mut self, downstream: &str) {
        let state = self
            .circuits
            .breaker(downstream)
            .map(|b| b.state().clone())
            .unwrap_or_default();
        tracing::warn!(
            subscription_id = self.subscription_id,
            downstream,
            trips = state.trips,
            "circuit opened — buffering to spool"
        );
        crate::metrics::record_subscription_circuit(downstream, "opened");
        self.emit(
            self.subscription_id,
            "subscription.circuit.opened",
            "OPEN",
            serde_json::json!({
                "downstream": downstream,
                "consecutive_failures": state.consecutive_failures,
                "trips": state.trips,
            }),
        )
        .await;
        self.persist_circuit().await;
    }

    async fn on_circuit_closed(&mut self, downstream: &str) {
        tracing::info!(
            subscription_id = self.subscription_id,
            downstream,
            "circuit closed — downstream recovered"
        );
        crate::metrics::record_subscription_circuit(downstream, "closed");
        self.emit(
            self.subscription_id,
            "subscription.circuit.closed",
            "CLOSED",
            serde_json::json!({ "downstream": downstream }),
        )
        .await;
        self.persist_circuit().await;
    }

    async fn emit_dead_letter(&self, d: &noetl_tools::spool::DeadLetter) {
        crate::metrics::record_subscription_dead_lettered(&self.source_name);
        self.emit(
            self.subscription_id,
            "subscription.message.dead_lettered",
            "DEAD_LETTERED",
            serde_json::json!({
                "message_id": d.message_id,
                "recv_seq": d.recv_seq,
                "spool_ref": d.spool_ref,
                "attempts": d.attempts,
                "reason": d.reason,
            }),
        )
        .await;
    }

    async fn update_spool_gauge(&self) {
        if let Ok(bytes) = self.engine.spool_bytes().await {
            crate::metrics::set_subscription_spool_bytes(&self.source_name, bytes);
        }
    }

    async fn persist_circuit(&self) {
        let Some(store) = &self.kv else { return };
        let snapshot = self.circuits.snapshot();
        if let Ok(bytes) = serde_json::to_vec(&snapshot) {
            if let Err(e) = store.put(&self.kv_key, bytes.into()).await {
                tracing::debug!(error = %e, "circuit KV persist failed (non-fatal)");
            }
        }
    }

    async fn emit(&self, execution_id: i64, event_type: &str, status: &str, context: serde_json::Value) {
        let event = ExecutorEvent {
            execution_id,
            event_type: event_type.to_string(),
            step: "ingress".to_string(),
            status: status.to_string(),
            created_at: chrono::Utc::now(),
            context,
            event_id: None,
            worker_id: Some(self.worker_id.clone()),
            meta: Some(serde_json::json!({ "emitter": "spool_runtime" })),
        };
        if let Err(e) = self.client.emit_event(event).await {
            tracing::debug!(execution_id, event_type, error = %e, "spool event emit failed (non-fatal)");
        }
    }
}

/// Map a `ToolError` into `anyhow` with context.
fn de(e: noetl_tools::ToolError) -> anyhow::Error {
    anyhow::anyhow!("spool: {e}")
}

/// Connect a JetStream context to the platform NATS (a runtime credential,
/// allowed direct per `execution-model.md`). Reuses the `NATS_USER` /
/// `NATS_PASSWORD` env convention the worker's command consumer uses.
async fn connect_jetstream(nats_url: &str) -> Result<async_nats::jetstream::Context> {
    let mut opts = async_nats::ConnectOptions::new();
    if let (Ok(user), Ok(pass)) = (std::env::var("NATS_USER"), std::env::var("NATS_PASSWORD")) {
        if !user.is_empty() {
            opts = opts.user_and_password(user, pass);
        }
    } else if let Ok(token) = std::env::var("NATS_TOKEN") {
        if !token.is_empty() {
            opts = opts.token(token);
        }
    }
    let client = opts
        .connect(nats_url)
        .await
        .with_context(|| format!("spool NATS connect to '{nats_url}'"))?;
    Ok(async_nats::jetstream::new(client))
}

/// Open (creating if absent) the per-subscription circuit-state KV bucket.
/// Best-effort — a KV failure degrades to in-memory circuit state (still
/// correct within the runtime's lifetime, just not restart-durable).
async fn open_circuit_kv(
    js: &async_nats::jetstream::Context,
    subscription_id: i64,
) -> Option<async_nats::jetstream::kv::Store> {
    let bucket = "noetl_subscription_circuit";
    match js.get_key_value(bucket).await {
        Ok(s) => Some(s),
        Err(_) => js
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: bucket.to_string(),
                description: "NoETL subscription circuit-breaker state (RFC #90 Phase 4)"
                    .to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                tracing::warn!(subscription_id, error = %e, "circuit KV open/create failed; in-memory only");
            })
            .ok(),
    }
}
