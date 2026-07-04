//! Prometheus metrics for the worker.
//!
//! Per [`agents/rules/observability.md`][rule] Principle 2 ("metrics
//! over logs"), every boundary call ships at least one metric.  This
//! module defines the worker's `noetl_worker_*` counter / histogram /
//! gauge inventory, lazy-initialised under a single global registry,
//! and exposed via the [`metrics_server`][crate::metrics_server]
//! `/metrics` endpoint.
//!
//! ## Inventory
//!
//! | Metric | Type | Labels | Purpose |
//! | :---- | :---- | :---- | :---- |
//! | `noetl_worker_pulls_total` | counter | `outcome` ∈ {claimed, already_claimed, retry_later, failed} | Pull rate + outcome distribution |
//! | `noetl_worker_pull_duration_seconds` | histogram | — | NATS pull + claim round-trip latency |
//! | `noetl_worker_dispatch_duration_seconds` | histogram | `tool_kind` | Per-tool-kind dispatch latency (where bottlenecks hide) |
//! | `noetl_worker_dispatch_errors_total` | counter | `tool_kind` | Per-tool failure rate |
//! | `noetl_worker_event_emit_duration_seconds` | histogram | `event_type` | Event-log write latency to the control plane |
//! | `noetl_worker_event_emit_retries_total` | counter | `event_type` | Retry rate on flaky control-plane writes |
//! | `noetl_worker_concurrent_dispatches` | gauge | — | Live count of in-flight dispatches (semaphore depth) |
//! | `noetl_worker_nats_consumer_pending` | gauge | `stream`, `consumer` | JetStream messages not yet delivered to a consumer (backlog the worker hasn't seen yet) |
//! | `noetl_worker_nats_consumer_ack_pending` | gauge | `stream`, `consumer` | Messages delivered but not yet ack'd (live in-flight work) |
//! | `noetl_worker_result_store_put_duration_seconds` | histogram | — | Durable result-store PUT latency (the cross-node reference path on `call.done` events) |
//! | `noetl_worker_result_store_put_bytes_total` | counter | — | Total bytes staged in the durable result store |
//! | `noetl_worker_result_store_put_errors_total` | counter | — | Durable result-store PUT failures (fall back to shm-cache-only or status-only) |
//! | `noetl_worker_call_done_skipped_pending_callback_total` | counter | `tool_kind` | Times the worker skipped its own `call.done` emit because the tool set `ToolResult.pending_callback = Some(true)` (the terminal event arrives via an async callback path; today only `Tool::Container` sets this — see noetl/ai-meta#43 Round 4) |
//!
//! `pending` + `ack_pending` together is the queue-depth signal KEDA
//! and the dashboard read to decide whether to scale the worker pool.
//! The gauge labels are stable (`stream`, `consumer`) so a multi-
//! consumer deployment gets one series per consumer without label
//! cardinality blow-up.
//!
//! ## Why a thin facade
//!
//! `lazy_static!`-style global state for metrics is the Prometheus
//! Rust crate's intended pattern.  Wrapping each metric in a typed
//! function (`record_pull(outcome, duration)`,
//! `record_dispatch(tool_kind, duration, error)`) keeps call sites
//! tidy and makes label-typo regressions impossible — `outcome` is
//! an enum, not a free-form string.
//!
//! [rule]: https://github.com/noetl/ai-meta/blob/main/agents/rules/observability.md

use prometheus::{
    CounterVec, Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, IntGaugeVec, Registry, TextEncoder,
};
use std::sync::OnceLock;

use noetl_executor::worker::source::ClaimOutcome;

/// The Prometheus text-format MIME type — what `/metrics` returns.
pub const METRICS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Outcome label values for `noetl_worker_pulls_total`.  Enum so the
/// label is typo-proof — `outcome_label(ClaimOutcome::Claimed)`
/// returns `"claimed"`, etc.
pub fn outcome_label(outcome: &ClaimOutcome) -> &'static str {
    match outcome {
        ClaimOutcome::Claimed(_) => "claimed",
        ClaimOutcome::AlreadyClaimed => "already_claimed",
        ClaimOutcome::RetryLater(_) => "retry_later",
        ClaimOutcome::Failed(_) => "failed",
    }
}

/// Holds every metric the worker exports.  Single-init via
/// [`WorkerMetrics::global`].
pub struct WorkerMetrics {
    pub registry: Registry,
    pub pulls_total: IntCounterVec,
    /// Execution-affinity routing decisions (noetl/ai-meta#166 Phase 4),
    /// partitioned by `decision` ∈ {owned, redirected, forced_local}. Only
    /// drive commands under a multi-shard, affinity-enabled pool are
    /// recorded; `owned` is the affinity-hit numerator, `redirected` +
    /// `forced_local` the miss/steer counts.
    pub affinity_decisions_total: IntCounterVec,
    pub pull_duration_seconds: Histogram,
    pub dispatch_duration_seconds: HistogramVec,
    pub dispatch_errors_total: IntCounterVec,
    pub event_emit_duration_seconds: HistogramVec,
    pub event_emit_retries_total: IntCounterVec,
    pub concurrent_dispatches: IntGauge,
    pub nats_consumer_pending: IntGaugeVec,
    pub nats_consumer_ack_pending: IntGaugeVec,
    pub result_store_put_duration_seconds: Histogram,
    pub result_store_put_bytes_total: IntCounter,
    pub result_store_put_errors_total: IntCounter,
    pub call_done_skipped_pending_callback_total: IntCounterVec,
    /// noetl/ai-meta#145 G2 — container poll fallback.  Pollers started,
    /// by namespace.
    pub container_poll_started_total: IntCounterVec,
    /// Container poll fallback terminal outcomes, by resolved state
    /// (`succeeded` / `failed` / `poll_timeout` / `error`).
    pub container_poll_terminal_total: IntCounterVec,
    /// Wall-clock a container poll fallback spent watching a Job to its
    /// terminal state.
    pub container_poll_duration_seconds: Histogram,
    /// Messages received by the continuous subscription runtime, by source.
    pub subscription_messages_received_total: IntCounterVec,
    /// Per-message executions the runtime dispatched, by source + outcome
    /// (`dispatched` / `error`).
    pub subscription_executions_total: IntCounterVec,
    /// Header directives the runtime applied, by control kind.
    pub subscription_directives_applied_total: IntCounterVec,
    /// Messages written to the store-and-forward spool, by source
    /// (RFC #90 Phase 4 §8).
    pub subscription_spooled_total: IntCounterVec,
    /// Circuit-breaker transitions, by downstream + transition
    /// (`opened` / `closed`).
    pub subscription_circuit_transitions_total: IntCounterVec,
    /// Messages dead-lettered (poison / evicted / expired), by source.
    pub subscription_dead_lettered_total: IntCounterVec,
    /// Live spool size in bytes, by source — the cost ceiling gauge (OQ3).
    pub subscription_spool_bytes: IntGaugeVec,
    /// Batch dispatches (`POST /api/execute/batch`) issued, by source
    /// (noetl/ai-meta#90 Phase 7).
    pub subscription_batch_dispatch_total: IntCounterVec,
    /// Messages dispatched inside a batch, by source — divided by
    /// `subscription_batch_dispatch_total` gives average batch depth.
    pub subscription_batch_messages_total: IntCounterVec,
    /// Times a per-subscription rate limit engaged, by source + reason
    /// (`dispatch_rate` / `max_in_flight`) — RFC §9 backpressure.
    pub subscription_rate_limited_total: IntCounterVec,

    // --- CQRS event materializer (noetl/ai-meta#103) -------------------------
    /// Events drained from `noetl_events` by the materializer consume-loop.
    pub materializer_drained_total: IntCounter,
    /// Events durably inserted into `noetl.event` (events/project `projected`).
    pub materializer_projected_total: IntCounter,
    /// Events that collided with an already-materialized row (idempotent
    /// redelivery path — events/project `duplicates`).
    pub materializer_duplicates_total: IntCounter,
    /// Ack handles disposed (positive ack) after a successful project — the
    /// ack-after-materialize commit point.
    pub materializer_acked_total: IntCounter,
    /// Project failures: the batch was NOT acked and will redeliver. This is
    /// the durability-event counter — the metric that proves no silent loss.
    pub materializer_project_errors_total: IntCounter,
    /// One materializer drain→project→ack cycle latency.
    pub materializer_cycle_duration_seconds: Histogram,

    // --- Shadow result materializer (noetl/ai-meta#104 Phase B) --------------
    /// Events drained from `noetl_events` by the result materializer's separate
    /// consumer.
    pub result_materializer_drained_total: IntCounter,
    /// Over-budget result references the result materializer wrote to object
    /// store, by tier (`feather` for tabular, `json` for non-tabular).
    pub result_materializer_writes_total: IntCounterVec,
    /// Events the result materializer skipped (inline/small, un-addressable
    /// reference, or payload not found) — the no-op surface.
    pub result_materializer_skipped_total: IntCounter,
    /// Shadow write/fetch failures — counted, never failing the event (the
    /// batch is acked regardless; idempotent keys make redelivery safe).
    pub result_materializer_errors_total: IntCounter,
    /// One result-materializer drain→classify→write→ack cycle latency.
    pub result_materializer_cycle_duration_seconds: Histogram,

    // --- Resolve-by-URN read path (noetl/ai-meta#104 Phase C) ----------------
    /// Resolve-by-URN attempts on the consume path, by outcome
    /// (`resolved_feather` / `resolved_json` for a hit; `fallback_*` for a
    /// fail-safe fall-through to the legacy `resolve_ref`). Flag-off it never
    /// moves; flag-on its `resolved_*` delta is the proof the read path served
    /// from the object-store tier instead of `noetl.result_store`.
    pub result_resolve_total: IntCounterVec,
    /// One resolve-by-URN attempt latency (registry + object fetch + decode).
    pub result_resolve_duration_seconds: Histogram,
    /// Consume-side resolutions while the Phase D minting flip is on
    /// (`NOETL_RESULT_MINT_AUTHORITATIVE`), by `path`
    /// (noetl/ai-meta#104 Phase D):
    /// - `tier` — the authoritative URN → Feather/GCS tier served the payload.
    /// - `legacy_fallback` — the tier missed / could not be addressed, so the
    ///   dual-written `noetl.result_store` served it (rollback safety).
    ///
    /// Flag-off it never moves; flag-on `tier` proves the tier is authoritative
    /// and `legacy_fallback` proves the reversible fallback path is intact.
    pub result_mint_authoritative_total: IntCounterVec,

    /// Side-effect durability barrier outcomes by `outcome` + `tool` label
    /// (noetl/ai-meta#104 Phase E).
    ///
    /// `outcome=skipped` — a side-effecting cycle whose durable result URN
    /// already existed; re-execution was skipped and the recorded result
    /// adopted (the side effect fired exactly once across the re-drive).
    /// `outcome=executed` — a side-effecting cycle with no durable result yet;
    /// dispatched normally. Flag-off it never moves; flag-on `skipped` is the
    /// positive proof the barrier prevented a duplicate side effect.
    pub side_effect_barrier_total: IntCounterVec,

    /// Result-tier DR re-derive outcomes by `outcome` (noetl/ai-meta#104 Phase
    /// F), recorded by the materializer's verify-and-repair mode
    /// (`NOETL_RESULT_TIER_DR`):
    /// - `present` — the durable object existed and was byte-identical to the
    ///   re-derivation; no rewrite needed.
    /// - `rederived` — the object was missing or byte-divergent (corrupt) and was
    ///   reconstructed from its source.
    /// - `source_gone` — the authoritative payload source was absent, so the
    ///   object could not be re-derived.
    /// - `error` — a fetch/encode/write failure.
    ///
    /// Flag-off it never moves; flag-on `rederived` is the positive proof a
    /// missing/corrupt tier object was rebuilt from the WAL-derivable source.
    pub result_tier_dr_total: IntCounterVec,

    /// Producer-staged result tier outcomes by `outcome` (noetl/ai-meta#104 OQ5
    /// Option A), gated on `NOETL_RESULT_PRODUCER_STAGE`:
    /// - `staged_feather` / `staged_json` — the producing worker wrote the tier
    ///   object at emit time (the write that decouples the tier from
    ///   `result_store`, the prerequisite to retiring it).
    /// - `skip_parse_uri` — no canonical URI on the reference (cannot key).
    /// - `skip_registry` — the cell registry was unavailable (declined to guess
    ///   a key; the materializer still covers the tier).
    /// - `error` — an `object_put` failure (best-effort; the materializer covers it).
    /// - `materializer_skip_exists` — the materializer found a producer-staged
    ///   object already present and skipped its `result_store` fetch (the OQ5
    ///   "no result_store read" proof).
    ///
    /// Flag-off it never moves; flag-on `staged_*` + `materializer_skip_exists`
    /// together prove the producer populates the tier and the materializer needs
    /// no `result_store` read for it.
    pub result_producer_stage_total: IntCounterVec,

    // --- Off-server state builder (noetl/ai-meta#115 Phase 4) ----------------
    /// Events the off-server state builder consumed from the `noetl_events`
    /// **WAL** stream and indexed. Positive evidence the builder reads the WAL
    /// (RFC tenet 5), not the materialized `noetl.event` table.
    pub state_builder_wal_events_total: IntCounter,
    /// `noetl.event` table scans the builder issued — the no-scan proof (RFC
    /// tenet 3). The builder NEVER touches `noetl.event`, so this stays **0**
    /// for the lifetime of the process; registering it makes the invariant
    /// observable on `/metrics` rather than implicit.
    pub state_builder_event_scans_total: IntCounter,
    /// State-builds by outcome: `cache_hit` (head unchanged), `incremental`
    /// (only the new tail walked), `cold_rebuild` (full walk from the head, e.g.
    /// cache miss / restart), `incomplete` (a chain gap / non-genesis → the real
    /// builder falls back to the server). The cache-effectiveness + correctness
    /// surface for Phase 4.
    pub state_builder_builds_total: IntCounterVec,
    /// Chain-walk depth (events on the spine) per cold rebuild — the analogue of
    /// the server's `noetl_state_build_chain_hops` (server#245), now off-server.
    pub state_builder_chain_hops: Histogram,
    /// Off-server **drive** builds by outcome (RFC #115 Phase 4 drive cutover):
    /// `served` — the drive obtained its state from the WAL spine (the wasm `run`
    /// from_events entry); `fallback_incomplete` — the WAL chain was incomplete
    /// (lag / cold) so the drive used the server-built `run_state` state carried
    /// on the same command; `fallback_disabled` — the worker's builder isn't
    /// authoritative so it used the server-built state.  The proof that the WAL
    /// build is authoritative is `served` dominating in steady state.
    pub state_builder_drive_builds_total: IntCounterVec,
    /// Off-server DRIVE build-retry waits by outcome — `woken` (the drain's
    /// append signal fired, noetl/ai-meta#130) vs `timeout` (the per-wait cap
    /// elapsed).  A healthy event-signalled drive shows `woken` dominating with a
    /// low absolute count (one or two wakes per hop), not a fixed-grid poll.
    pub state_builder_drive_wait_total: IntCounterVec,
    /// Off-server drive **tail-attach** accounting (noetl/ai-meta#156).  `kind`
    /// = `attached` (events the server shipped on the dispatch so the worker can
    /// advance its WAL index drain-independently) vs `applied_new` (of those, the
    /// ones new to the pool-side index — the rest were already drained, an
    /// idempotent overwrite).  A healthy accelerated hop shows `attached` small
    /// (O(few events)) and `applied_new` ≥ 1 (the new tail the build needed),
    /// confirming the per-hop cost is O(tail), not O(global-stream).
    pub state_builder_tail_total: IntCounterVec,
    /// Executions currently held in the pool-side WAL index — the index-coverage
    /// gauge (noetl/ai-meta#119).  The #119 stall was an index starved to **0**
    /// after a worker restart (the durable consumer cursor outran the rebuilt
    /// in-memory index), so `build_spine_to(expected_head)` was permanently
    /// `Incomplete` and off-server executions never completed.  The authoritative
    /// drain now rebuilds the full index from the retained `noetl_events` WAL on
    /// every boot; this gauge going **> 0** after a restart is the rehydration
    /// proof.
    pub state_builder_indexed_executions: IntGauge,
    /// Total events resident across all chains in the pool-side WAL index
    /// (noetl/ai-meta#166).  The `654 executions × ~27 events` headline of the
    /// system-pool OOM: this is the `× events` factor.
    pub state_builder_index_events: IntGauge,
    /// Approximate resident bytes the pool-side WAL index holds
    /// (noetl/ai-meta#166 §5.1) — the bounded-cache byte ledger the
    /// `NOETL_STATE_INDEX_MAX_BYTES` ceiling holds down.  Before this work the
    /// index grew `O(all non-terminal event history × full-envelope-size)` to
    /// ~1.28 GiB at idle; this gauge makes the resident set observable and the
    /// ceiling's effect measurable.
    pub state_builder_index_bytes: IntGauge,
    /// Bounded-cache evictions by `reason` (noetl/ai-meta#166 §5.1): `ttl` (idle
    /// non-terminal chain swept — the stuck/abandoned-execution class terminal
    /// eviction misses), `max_executions` (LRU over the concurrent-chain cap),
    /// `byte_ceiling` (LRU under the hard resident-byte ceiling).  A rising `ttl`
    /// rate is the cure for the OOM treadmill firing.
    pub state_builder_evictions_total: IntCounterVec,
    /// Cold-rebuild-on-miss outcomes (noetl/ai-meta#166 §5.2): `served` (re-read
    /// the missed execution from the retained WAL and the drive then built its
    /// state), `incomplete` (re-indexed events but the chain still couldn't reach
    /// genesis — fell back), `empty` (no events for it in the retained window),
    /// `throttled` (the concurrency cap was saturated — fell back).  The safety
    /// net that makes eviction wedge-safe with tail-attach off.
    pub state_builder_rehydrate_total: IntCounterVec,
    /// Cold-load-from-shard outcomes (noetl/ai-meta#166 Phase 3): `hit` — the
    /// Feather state shard was read + decoded + the reconstructed chain served
    /// the drive; `miss` — no shard object existed (both `sealed`/`open` 404 →
    /// fell through to the WAL-replay path); `fallback` — a shard existed but the
    /// reconstructed chain was still incomplete (stale open shard, tail beyond) or
    /// undecodable, so the WAL-replay path ran.  The payoff metric: `hit` is one
    /// object read (~tens of ms) replacing a retained-WAL scan (≤ the rehydrate
    /// deadline).
    pub state_shard_reads_total: IntCounterVec,
    /// Wall-clock of one cold-load-from-shard attempt — the `object_get` +
    /// Feather-decode + chain-apply round-trip (noetl/ai-meta#166 Phase 3).  The
    /// number that proves the latency payoff vs the WAL-replay miss cost.
    pub state_shard_read_duration_seconds: Histogram,
    /// Equivalence-guard tripwire (noetl/ai-meta#166 Phase 3): incremented when a
    /// shard-reconstructed spine did NOT byte-match the WAL-replay spine under the
    /// `NOETL_STATE_SHARD_READ_VERIFY` dual-build check.  MUST stay 0 — any
    /// increment means the shard served divergent state and the drive fell back to
    /// the WAL build (never serves the wrong state).
    pub state_equivalence_mismatch_total: IntCounter,
    /// Per-phase latency of loading a wasm plug-in module
    /// (noetl/ai-meta#130 cold-start): `fetch` — the HTTP GET of the module
    /// bytes from the server catalog; `compile` — the Cranelift JIT compile
    /// (`Module::new`).  The compile dominates the first-hop cold-start
    /// (~1.6MB `system/orchestrate` module → ~0.2s on a fast host, multiples of
    /// that on a constrained worker node); boot-time warmup moves it off the
    /// first real drive.
    pub plugin_load_seconds: HistogramVec,
    /// Boot-time plug-in warmup outcome (noetl/ai-meta#130): `warmed` — the
    /// module compiled + cached during startup so the first dispatch is a cache
    /// hit; `skipped` — warmup disabled or feature off; `error` — the warm
    /// fetch/compile failed (non-fatal; the first real dispatch falls back to
    /// the lazy load path).  `duration_seconds` on the warmup span is the total
    /// boot-warm cost the readiness gate hides.
    pub plugin_warm_total: IntCounterVec,
    /// Worker readiness (noetl/ai-meta#130): `1` once boot warmup completed and
    /// the pull loop is eligible to claim, `0` during startup.  The `/readyz`
    /// probe reads this so Kubernetes only routes / completes a rollout once the
    /// worker is warm.
    pub worker_ready: IntGauge,
    /// State-builder drain health (noetl/ai-meta#161): `1` while the authoritative
    /// WAL drain is connected and serving, `0` when it has been continuously
    /// erroring against a likely-orphaned JetStream consumer for longer than
    /// `NOETL_STATE_BUILDER_UNHEALTHY_SECS`.  The `/livez` probe reads this so
    /// Kubernetes auto-restarts a pod whose `state_builder` wedged after a NATS
    /// server bounce (the 503/"no responders" storm that drove orchestrate
    /// `commands=0` and locked out every off-server drive).  Defaults to `1` so
    /// workers that don't run the builder (mode `Off`, e.g. the request pool)
    /// always report alive.
    pub state_builder_healthy: IntGauge,
    /// Count of state-builder consumer/connection rebuilds (noetl/ai-meta#161),
    /// partitioned by `reason`: `connect_error` — initial connect / create_consumer
    /// failed and is being retried with backoff; `drain_dead` — a live consumer
    /// started returning the dead-consumer signature (503 / no-responders /
    /// consumer-not-found) past the rebuild threshold and was torn down + recreated.
    /// A rising `drain_dead` rate is the self-heal firing — the worker recovering
    /// from a NATS bounce on its own instead of wedging until a manual restart.
    pub state_builder_consumer_recreate_total: IntCounterVec,

    // --- State materializer (noetl/ai-meta#166 Phase 2 — shadow state-shard tier) ---
    /// Events drained from `noetl_events` by the shadow state materializer.
    pub state_materializer_drained_total: IntCounter,
    /// Slim-chain rows projected (events accepted into an open shard).
    pub state_materializer_rows_total: IntCounter,
    /// State shards written to object store, partitioned by `seal`
    /// (`open` / `sealed`).
    pub state_materializer_shards_written_total: IntCounterVec,
    /// Total bytes of Feather state-shard objects written.
    pub state_materializer_shard_bytes_total: IntCounter,
    /// Shadow state-materializer encode/write failures (counted, never failing
    /// the event — the shard tier never wedges its own consumer).
    pub state_materializer_errors_total: IntCounter,
    /// Open shards evicted before sealing, partitioned by `reason`
    /// (`idle` / `max_open`) — the abandoned-execution backstop.
    pub state_materializer_evicted_total: IntCounterVec,
    /// Resident open (un-sealed) shards — the writer's working-set gauge; the
    /// signal that it stays `O(live executions)`, not `O(history)`.
    pub state_materializer_open_shards: IntGauge,
    /// Latency of one state-materializer drain→project→write→ack cycle.
    pub state_materializer_cycle_duration_seconds: Histogram,
}

impl WorkerMetrics {
    fn new() -> Self {
        let registry = Registry::new();

        let pulls_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_pulls_total",
                "Total commands pulled from the source, partitioned by claim outcome.",
            ),
            &["outcome"],
        )
        .expect("pulls_total metric");
        registry
            .register(Box::new(pulls_total.clone()))
            .expect("register pulls_total");

        let affinity_decisions_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_affinity_decisions_total",
                "Execution-affinity routing decisions for drive commands \
                 (noetl/ai-meta#166 Phase 4), partitioned by decision.",
            ),
            &["decision"],
        )
        .expect("affinity_decisions_total metric");
        registry
            .register(Box::new(affinity_decisions_total.clone()))
            .expect("register affinity_decisions_total");

        let pull_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "noetl_worker_pull_duration_seconds",
                "Latency of one pull (NATS receive + control-plane claim).",
            )
            .buckets(vec![
                0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0,
            ]),
        )
        .expect("pull_duration_seconds metric");
        registry
            .register(Box::new(pull_duration_seconds.clone()))
            .expect("register pull_duration_seconds");

        let dispatch_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "noetl_worker_dispatch_duration_seconds",
                "Latency of one command dispatch (tool execution + lifecycle events).",
            )
            .buckets(vec![
                0.010, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
            ]),
            &["tool_kind"],
        )
        .expect("dispatch_duration_seconds metric");
        registry
            .register(Box::new(dispatch_duration_seconds.clone()))
            .expect("register dispatch_duration_seconds");

        let dispatch_errors_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_dispatch_errors_total",
                "Total command dispatches that failed, by tool kind.",
            ),
            &["tool_kind"],
        )
        .expect("dispatch_errors_total metric");
        registry
            .register(Box::new(dispatch_errors_total.clone()))
            .expect("register dispatch_errors_total");

        let event_emit_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "noetl_worker_event_emit_duration_seconds",
                "Latency of one event emission to the control plane, by event type.",
            )
            .buckets(vec![
                0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0,
            ]),
            &["event_type"],
        )
        .expect("event_emit_duration_seconds metric");
        registry
            .register(Box::new(event_emit_duration_seconds.clone()))
            .expect("register event_emit_duration_seconds");

        let event_emit_retries_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_event_emit_retries_total",
                "Total event-emission retries triggered by transient failures.",
            ),
            &["event_type"],
        )
        .expect("event_emit_retries_total metric");
        registry
            .register(Box::new(event_emit_retries_total.clone()))
            .expect("register event_emit_retries_total");

        let concurrent_dispatches = IntGauge::new(
            "noetl_worker_concurrent_dispatches",
            "Number of dispatches currently in flight (semaphore depth).",
        )
        .expect("concurrent_dispatches metric");
        registry
            .register(Box::new(concurrent_dispatches.clone()))
            .expect("register concurrent_dispatches");

        // NATS consumer-lag gauges — populated by a periodic poll task
        // (see `crate::nats::lag_poller`).  `pending` is the backlog
        // the worker hasn't seen yet; `ack_pending` is live in-flight
        // work.  Together they're the queue-depth signal KEDA reads
        // to decide whether to scale.
        let nats_consumer_pending = IntGaugeVec::new(
            prometheus::Opts::new(
                "noetl_worker_nats_consumer_pending",
                "JetStream messages not yet delivered to a consumer.",
            ),
            &["stream", "consumer"],
        )
        .expect("nats_consumer_pending metric");
        registry
            .register(Box::new(nats_consumer_pending.clone()))
            .expect("register nats_consumer_pending");

        let nats_consumer_ack_pending = IntGaugeVec::new(
            prometheus::Opts::new(
                "noetl_worker_nats_consumer_ack_pending",
                "JetStream messages delivered to a consumer but not yet ack'd.",
            ),
            &["stream", "consumer"],
        )
        .expect("nats_consumer_ack_pending metric");
        registry
            .register(Box::new(nats_consumer_ack_pending.clone()))
            .expect("register nats_consumer_ack_pending");

        // Durable result-store metrics — populated on the over-budget
        // `call.done` path inside `executor::command::build_call_done_result`.
        // Histogram covers PUT round-trip; counters track total bytes
        // staged + total errors so operators can spot a network outage
        // or sudden bandwidth spike.  No labels — the worker only has
        // one durable store endpoint (the control plane) so the labels
        // would all collapse to a single series.
        let result_store_put_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "noetl_worker_result_store_put_duration_seconds",
                "Latency of one durable result-store PUT (control-plane round-trip).",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
        )
        .expect("result_store_put_duration_seconds metric");
        registry
            .register(Box::new(result_store_put_duration_seconds.clone()))
            .expect("register result_store_put_duration_seconds");

        let result_store_put_bytes_total = IntCounter::new(
            "noetl_worker_result_store_put_bytes_total",
            "Total bytes staged in the durable result store across all successful PUTs.",
        )
        .expect("result_store_put_bytes_total metric");
        registry
            .register(Box::new(result_store_put_bytes_total.clone()))
            .expect("register result_store_put_bytes_total");

        let result_store_put_errors_total = IntCounter::new(
            "noetl_worker_result_store_put_errors_total",
            "Total durable result-store PUT failures (fall back to shm-cache-only or status-only).",
        )
        .expect("result_store_put_errors_total metric");
        registry
            .register(Box::new(result_store_put_errors_total.clone()))
            .expect("register result_store_put_errors_total");

        // noetl/ai-meta#43 Round 4 — pending_callback adoption.  When a
        // tool sets `ToolResult.pending_callback = Some(true)` the
        // worker skips its own `call.done` emit because the terminal
        // event arrives asynchronously via a callback (e.g. the K8s
        // watcher → `POST /api/internal/container-callback/...` path
        // for `Tool::Container`).  Counted per `tool_kind` so the
        // dashboard can pair this with the server-side
        // `noetl_container_callback_total{state}` and
        // `noetl_container_callback_stale_total{state}` counters —
        // healthy steady state is `skipped_total ≈ container_callback_total`
        // with `container_callback_stale_total` near zero.
        let call_done_skipped_pending_callback_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_call_done_skipped_pending_callback_total",
                "Times the worker skipped its own call.done emit because the tool set ToolResult.pending_callback (the terminal event arrives via an async callback path).",
            ),
            &["tool_kind"],
        )
        .expect("call_done_skipped_pending_callback_total metric");
        registry
            .register(Box::new(call_done_skipped_pending_callback_total.clone()))
            .expect("register call_done_skipped_pending_callback_total");

        // noetl/ai-meta#145 G2 — container poll fallback observability.
        // The poller runs in a detached task (the dispatch slot is already
        // freed), so these are the only signal an operator has that a
        // long-running Job is being watched + how it resolved.  Pair
        // `container_poll_terminal_total{state}` with the server-side
        // `noetl_container_callback_total` to confirm exactly one of the
        // two completion paths fired per Job (poll vs watcher).
        let container_poll_started_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_container_poll_started_total",
                "Container poll-fallback watchers started, by namespace.",
            ),
            &["namespace"],
        )
        .expect("container_poll_started_total metric");
        registry
            .register(Box::new(container_poll_started_total.clone()))
            .expect("register container_poll_started_total");

        let container_poll_terminal_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_container_poll_terminal_total",
                "Container poll-fallback terminal outcomes, by resolved state (succeeded/failed/poll_timeout/error).",
            ),
            &["state"],
        )
        .expect("container_poll_terminal_total metric");
        registry
            .register(Box::new(container_poll_terminal_total.clone()))
            .expect("register container_poll_terminal_total");

        let container_poll_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "noetl_worker_container_poll_duration_seconds",
                "Wall-clock a container poll fallback spent watching a Job to terminal state.",
            )
            // Jobs run seconds → hours; buckets span that range.
            .buckets(vec![
                1.0, 5.0, 15.0, 60.0, 300.0, 900.0, 1800.0, 3600.0, 7200.0, 21600.0,
            ]),
        )
        .expect("container_poll_duration_seconds metric");
        registry
            .register(Box::new(container_poll_duration_seconds.clone()))
            .expect("register container_poll_duration_seconds");

        let subscription_messages_received_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_subscription_messages_received_total",
                "Messages received by the continuous subscription runtime, by source.",
            ),
            &["source"],
        )
        .expect("subscription_messages_received_total metric");
        registry
            .register(Box::new(subscription_messages_received_total.clone()))
            .expect("register subscription_messages_received_total");

        let subscription_executions_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_subscription_executions_total",
                "Per-message executions dispatched by the subscription runtime, by source + outcome.",
            ),
            &["source", "outcome"],
        )
        .expect("subscription_executions_total metric");
        registry
            .register(Box::new(subscription_executions_total.clone()))
            .expect("register subscription_executions_total");

        let subscription_directives_applied_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_subscription_directives_applied_total",
                "Header directives applied by the subscription runtime, by control kind.",
            ),
            &["controls"],
        )
        .expect("subscription_directives_applied_total metric");
        registry
            .register(Box::new(subscription_directives_applied_total.clone()))
            .expect("register subscription_directives_applied_total");

        let subscription_spooled_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_subscription_spooled_total",
                "Messages written to the store-and-forward spool, by source (RFC #90 Phase 4).",
            ),
            &["source"],
        )
        .expect("subscription_spooled_total metric");
        registry
            .register(Box::new(subscription_spooled_total.clone()))
            .expect("register subscription_spooled_total");

        let subscription_circuit_transitions_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_subscription_circuit_transitions_total",
                "Circuit-breaker transitions, by downstream + transition.",
            ),
            &["downstream", "transition"],
        )
        .expect("subscription_circuit_transitions_total metric");
        registry
            .register(Box::new(subscription_circuit_transitions_total.clone()))
            .expect("register subscription_circuit_transitions_total");

        let subscription_dead_lettered_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_subscription_dead_lettered_total",
                "Messages dead-lettered from the spool (poison / evicted / expired), by source.",
            ),
            &["source"],
        )
        .expect("subscription_dead_lettered_total metric");
        registry
            .register(Box::new(subscription_dead_lettered_total.clone()))
            .expect("register subscription_dead_lettered_total");

        let subscription_spool_bytes = IntGaugeVec::new(
            prometheus::Opts::new(
                "noetl_subscription_spool_bytes",
                "Live store-and-forward spool size in bytes, by source — the cost ceiling gauge.",
            ),
            &["source"],
        )
        .expect("subscription_spool_bytes metric");
        registry
            .register(Box::new(subscription_spool_bytes.clone()))
            .expect("register subscription_spool_bytes");

        let subscription_batch_dispatch_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_subscription_batch_dispatch_total",
                "Batch dispatches (POST /api/execute/batch) issued by the subscription runtime, by source (RFC #90 Phase 7).",
            ),
            &["source"],
        )
        .expect("subscription_batch_dispatch_total metric");
        registry
            .register(Box::new(subscription_batch_dispatch_total.clone()))
            .expect("register subscription_batch_dispatch_total");

        let subscription_batch_messages_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_subscription_batch_messages_total",
                "Messages dispatched inside a batch, by source (RFC #90 Phase 7).",
            ),
            &["source"],
        )
        .expect("subscription_batch_messages_total metric");
        registry
            .register(Box::new(subscription_batch_messages_total.clone()))
            .expect("register subscription_batch_messages_total");

        let subscription_rate_limited_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_subscription_rate_limited_total",
                "Times a per-subscription rate limit engaged, by source + reason (RFC #90 Phase 7 §9).",
            ),
            &["source", "reason"],
        )
        .expect("subscription_rate_limited_total metric");
        registry
            .register(Box::new(subscription_rate_limited_total.clone()))
            .expect("register subscription_rate_limited_total");

        let materializer_drained_total = IntCounter::new(
            "noetl_worker_materializer_drained_total",
            "Events drained from noetl_events by the CQRS materializer (noetl/ai-meta#103).",
        )
        .expect("materializer_drained_total metric");
        registry
            .register(Box::new(materializer_drained_total.clone()))
            .expect("register materializer_drained_total");

        let materializer_projected_total = IntCounter::new(
            "noetl_worker_materializer_projected_total",
            "Events durably inserted into noetl.event by the materializer (events/project projected).",
        )
        .expect("materializer_projected_total metric");
        registry
            .register(Box::new(materializer_projected_total.clone()))
            .expect("register materializer_projected_total");

        let materializer_duplicates_total = IntCounter::new(
            "noetl_worker_materializer_duplicates_total",
            "Events that collided with an already-materialized row (idempotent redelivery path).",
        )
        .expect("materializer_duplicates_total metric");
        registry
            .register(Box::new(materializer_duplicates_total.clone()))
            .expect("register materializer_duplicates_total");

        let materializer_acked_total = IntCounter::new(
            "noetl_worker_materializer_acked_total",
            "Ack handles disposed after a successful project — the ack-after-materialize commit point.",
        )
        .expect("materializer_acked_total metric");
        registry
            .register(Box::new(materializer_acked_total.clone()))
            .expect("register materializer_acked_total");

        let materializer_project_errors_total = IntCounter::new(
            "noetl_worker_materializer_project_errors_total",
            "Project failures: the batch was NOT acked and will redeliver (no silent loss).",
        )
        .expect("materializer_project_errors_total metric");
        registry
            .register(Box::new(materializer_project_errors_total.clone()))
            .expect("register materializer_project_errors_total");

        let materializer_cycle_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            "noetl_worker_materializer_cycle_duration_seconds",
            "Latency of one materializer drain→project→ack cycle.",
        ))
        .expect("materializer_cycle_duration_seconds metric");
        registry
            .register(Box::new(materializer_cycle_duration_seconds.clone()))
            .expect("register materializer_cycle_duration_seconds");

        let result_materializer_drained_total = IntCounter::new(
            "noetl_worker_result_materializer_drained_total",
            "Events drained from noetl_events by the shadow result materializer (noetl/ai-meta#104 Phase B).",
        )
        .expect("result_materializer_drained_total metric");
        registry
            .register(Box::new(result_materializer_drained_total.clone()))
            .expect("register result_materializer_drained_total");

        let result_materializer_writes_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_result_materializer_writes_total",
                "Over-budget result references written to object store by the shadow result materializer, by tier.",
            ),
            &["tier"],
        )
        .expect("result_materializer_writes_total metric");
        registry
            .register(Box::new(result_materializer_writes_total.clone()))
            .expect("register result_materializer_writes_total");

        let result_materializer_skipped_total = IntCounter::new(
            "noetl_worker_result_materializer_skipped_total",
            "Events the shadow result materializer skipped (inline/un-addressable/payload-missing).",
        )
        .expect("result_materializer_skipped_total metric");
        registry
            .register(Box::new(result_materializer_skipped_total.clone()))
            .expect("register result_materializer_skipped_total");

        let result_materializer_errors_total = IntCounter::new(
            "noetl_worker_result_materializer_errors_total",
            "Shadow result-materializer fetch/write failures (counted, never failing the event).",
        )
        .expect("result_materializer_errors_total metric");
        registry
            .register(Box::new(result_materializer_errors_total.clone()))
            .expect("register result_materializer_errors_total");

        let result_materializer_cycle_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            "noetl_worker_result_materializer_cycle_duration_seconds",
            "Latency of one shadow result-materializer drain→classify→write→ack cycle.",
        ))
        .expect("result_materializer_cycle_duration_seconds metric");
        registry
            .register(Box::new(result_materializer_cycle_duration_seconds.clone()))
            .expect("register result_materializer_cycle_duration_seconds");

        let result_resolve_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_result_resolve_total",
                "Resolve-by-URN read-path attempts by outcome (noetl/ai-meta#104 Phase C).",
            ),
            &["outcome"],
        )
        .expect("result_resolve_total metric");
        registry
            .register(Box::new(result_resolve_total.clone()))
            .expect("register result_resolve_total");

        let result_resolve_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            "noetl_worker_result_resolve_duration_seconds",
            "Latency of one resolve-by-URN attempt (registry + object fetch + decode).",
        ))
        .expect("result_resolve_duration_seconds metric");
        registry
            .register(Box::new(result_resolve_duration_seconds.clone()))
            .expect("register result_resolve_duration_seconds");

        let result_mint_authoritative_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_result_mint_authoritative_total",
                "Consume-side resolutions under the Phase D minting flip by path \
                 (tier | legacy_fallback) (noetl/ai-meta#104 Phase D).",
            ),
            &["path"],
        )
        .expect("result_mint_authoritative_total metric");
        registry
            .register(Box::new(result_mint_authoritative_total.clone()))
            .expect("register result_mint_authoritative_total");

        let side_effect_barrier_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_side_effect_barrier_total",
                "Side-effect durability barrier outcomes by outcome \
                 (skipped | executed) + tool kind (noetl/ai-meta#104 Phase E).",
            ),
            &["outcome", "tool"],
        )
        .expect("side_effect_barrier_total metric");
        registry
            .register(Box::new(side_effect_barrier_total.clone()))
            .expect("register side_effect_barrier_total");

        let result_tier_dr_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_result_tier_dr_total",
                "Result-tier DR re-derive outcomes by outcome \
                 (present | rederived | source_gone | error) (noetl/ai-meta#104 Phase F).",
            ),
            &["outcome"],
        )
        .expect("result_tier_dr_total metric");
        registry
            .register(Box::new(result_tier_dr_total.clone()))
            .expect("register result_tier_dr_total");

        let result_producer_stage_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_result_producer_stage_total",
                "Producer-staged result tier outcomes by outcome \
                 (staged_feather | staged_json | skip_parse_uri | skip_registry | \
                 error | materializer_skip_exists) (noetl/ai-meta#104 OQ5 Option A).",
            ),
            &["outcome"],
        )
        .expect("result_producer_stage_total metric");
        registry
            .register(Box::new(result_producer_stage_total.clone()))
            .expect("register result_producer_stage_total");

        let state_builder_wal_events_total = IntCounter::new(
            "noetl_worker_state_builder_wal_events_total",
            "Events the off-server state builder consumed from the noetl_events WAL stream (RFC #115 Phase 4).",
        )
        .expect("state_builder_wal_events_total metric");
        registry
            .register(Box::new(state_builder_wal_events_total.clone()))
            .expect("register state_builder_wal_events_total");

        let state_builder_indexed_executions = IntGauge::new(
            "noetl_worker_state_builder_indexed_executions",
            "Executions currently held in the pool-side WAL index (noetl/ai-meta#119 rehydration proof; >0 after a restart means the index rebuilt from the retained WAL).",
        )
        .expect("state_builder_indexed_executions metric");
        registry
            .register(Box::new(state_builder_indexed_executions.clone()))
            .expect("register state_builder_indexed_executions");

        let state_builder_index_events = IntGauge::new(
            "noetl_worker_state_builder_index_events",
            "Total events resident across all chains in the pool-side WAL index (noetl/ai-meta#166).",
        )
        .expect("state_builder_index_events metric");
        registry
            .register(Box::new(state_builder_index_events.clone()))
            .expect("register state_builder_index_events");

        let state_builder_index_bytes = IntGauge::new(
            "noetl_worker_state_builder_index_bytes",
            "Approximate resident bytes held by the pool-side WAL index — the bounded-cache byte ledger NOETL_STATE_INDEX_MAX_BYTES holds down (noetl/ai-meta#166).",
        )
        .expect("state_builder_index_bytes metric");
        registry
            .register(Box::new(state_builder_index_bytes.clone()))
            .expect("register state_builder_index_bytes");

        let state_builder_evictions_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_state_builder_evictions_total",
                "Bounded-cache evictions by reason — ttl / max_executions / byte_ceiling (noetl/ai-meta#166).",
            ),
            &["reason"],
        )
        .expect("state_builder_evictions_total metric");
        registry
            .register(Box::new(state_builder_evictions_total.clone()))
            .expect("register state_builder_evictions_total");

        let state_builder_rehydrate_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_state_builder_rehydrate_total",
                "Cold-rebuild-on-miss outcomes — served / incomplete / empty / throttled (noetl/ai-meta#166).",
            ),
            &["outcome"],
        )
        .expect("state_builder_rehydrate_total metric");
        registry
            .register(Box::new(state_builder_rehydrate_total.clone()))
            .expect("register state_builder_rehydrate_total");

        let state_shard_reads_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_state_shard_reads_total",
                "Cold-load-from-shard outcomes — hit / miss / fallback (noetl/ai-meta#166 Phase 3).",
            ),
            &["outcome"],
        )
        .expect("state_shard_reads_total metric");
        registry
            .register(Box::new(state_shard_reads_total.clone()))
            .expect("register state_shard_reads_total");

        let state_shard_read_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "noetl_worker_state_shard_read_duration_seconds",
                "Cold-load-from-shard latency — object_get + Feather decode + chain apply (noetl/ai-meta#166 Phase 3).",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0,
            ]),
        )
        .expect("state_shard_read_duration_seconds metric");
        registry
            .register(Box::new(state_shard_read_duration_seconds.clone()))
            .expect("register state_shard_read_duration_seconds");

        let state_equivalence_mismatch_total = IntCounter::new(
            "noetl_worker_state_equivalence_mismatch_total",
            "Shard-vs-WAL spine byte-divergence under NOETL_STATE_SHARD_READ_VERIFY (noetl/ai-meta#166 Phase 3; MUST stay 0).",
        )
        .expect("state_equivalence_mismatch_total metric");
        registry
            .register(Box::new(state_equivalence_mismatch_total.clone()))
            .expect("register state_equivalence_mismatch_total");

        let state_builder_event_scans_total = IntCounter::new(
            "noetl_worker_state_builder_event_scans_total",
            "noetl.event scans the off-server state builder issued (RFC #115 tenet 3 no-scan proof; stays 0).",
        )
        .expect("state_builder_event_scans_total metric");
        registry
            .register(Box::new(state_builder_event_scans_total.clone()))
            .expect("register state_builder_event_scans_total");

        let state_builder_builds_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_state_builder_builds_total",
                "Off-server state builds by outcome (RFC #115 Phase 4).",
            ),
            &["outcome"],
        )
        .expect("state_builder_builds_total metric");
        registry
            .register(Box::new(state_builder_builds_total.clone()))
            .expect("register state_builder_builds_total");

        let state_builder_chain_hops = Histogram::with_opts(
            HistogramOpts::new(
                "noetl_worker_state_builder_chain_hops",
                "Chain-walk depth (spine length) per off-server cold rebuild (RFC #115 Phase 4).",
            )
            .buckets(vec![
                1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0,
            ]),
        )
        .expect("state_builder_chain_hops metric");
        registry
            .register(Box::new(state_builder_chain_hops.clone()))
            .expect("register state_builder_chain_hops");

        let state_builder_drive_builds_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_state_builder_drive_builds_total",
                "Off-server DRIVE builds by outcome — served (WAL spine) vs fallback (RFC #115 Phase 4 cutover).",
            ),
            &["outcome"],
        )
        .expect("state_builder_drive_builds_total metric");
        registry
            .register(Box::new(state_builder_drive_builds_total.clone()))
            .expect("register state_builder_drive_builds_total");

        let state_builder_drive_wait_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_state_builder_drive_wait_total",
                "Off-server DRIVE build-retry waits by outcome — woken (drain append signal) vs timeout (per-wait cap); noetl/ai-meta#130 event-signalled drive.",
            ),
            &["outcome"],
        )
        .expect("state_builder_drive_wait_total metric");
        registry
            .register(Box::new(state_builder_drive_wait_total.clone()))
            .expect("register state_builder_drive_wait_total");

        let state_builder_tail_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_state_builder_tail_total",
                "Off-server drive tail-attach events by kind — attached (shipped on dispatch) vs applied_new (new to the pool-side WAL index); noetl/ai-meta#156.",
            ),
            &["kind"],
        )
        .expect("state_builder_tail_total metric");
        registry
            .register(Box::new(state_builder_tail_total.clone()))
            .expect("register state_builder_tail_total");

        let plugin_load_seconds = HistogramVec::new(
            HistogramOpts::new(
                "noetl_worker_plugin_load_seconds",
                "Per-phase latency of loading a wasm plug-in module (fetch vs Cranelift compile); noetl/ai-meta#130 cold-start attribution.",
            )
            .buckets(vec![
                0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.0, 5.0,
            ]),
            &["phase"],
        )
        .expect("plugin_load_seconds metric");
        registry
            .register(Box::new(plugin_load_seconds.clone()))
            .expect("register plugin_load_seconds");

        let plugin_warm_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_plugin_warm_total",
                "Boot-time plug-in warmup outcome — warmed / skipped / error; noetl/ai-meta#130.",
            ),
            &["outcome"],
        )
        .expect("plugin_warm_total metric");
        registry
            .register(Box::new(plugin_warm_total.clone()))
            .expect("register plugin_warm_total");

        let worker_ready = IntGauge::new(
            "noetl_worker_ready",
            "Worker readiness — 1 once boot warmup completed; the /readyz probe reads this (noetl/ai-meta#130).",
        )
        .expect("worker_ready metric");
        registry
            .register(Box::new(worker_ready.clone()))
            .expect("register worker_ready");

        let state_builder_healthy = IntGauge::new(
            "noetl_worker_state_builder_healthy",
            "State-builder drain health — 1 connected/serving, 0 wedged on a dead NATS consumer; the /livez probe reads this (noetl/ai-meta#161).",
        )
        .expect("state_builder_healthy metric");
        // Default healthy: a worker that never runs the authoritative drain
        // (mode Off — the request pool) must report alive, and the drain itself
        // is healthy until it has been erroring past the unhealthy threshold.
        state_builder_healthy.set(1);
        registry
            .register(Box::new(state_builder_healthy.clone()))
            .expect("register state_builder_healthy");

        let state_builder_consumer_recreate_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_state_builder_consumer_recreate_total",
                "State-builder consumer/connection rebuilds — reason connect_error / drain_dead; the self-heal firing (noetl/ai-meta#161).",
            ),
            &["reason"],
        )
        .expect("state_builder_consumer_recreate_total metric");
        registry
            .register(Box::new(state_builder_consumer_recreate_total.clone()))
            .expect("register state_builder_consumer_recreate_total");

        // --- State materializer (noetl/ai-meta#166 Phase 2) ---
        let state_materializer_drained_total = IntCounter::new(
            "noetl_worker_state_materializer_drained_total",
            "Events drained from noetl_events by the shadow state materializer (noetl/ai-meta#166 Phase 2).",
        )
        .expect("state_materializer_drained_total metric");
        registry
            .register(Box::new(state_materializer_drained_total.clone()))
            .expect("register state_materializer_drained_total");

        let state_materializer_rows_total = IntCounter::new(
            "noetl_worker_state_materializer_rows_total",
            "Slim-chain rows projected into open state shards by the shadow state materializer.",
        )
        .expect("state_materializer_rows_total metric");
        registry
            .register(Box::new(state_materializer_rows_total.clone()))
            .expect("register state_materializer_rows_total");

        let state_materializer_shards_written_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_state_materializer_shards_written_total",
                "Feather state shards written to object store by the shadow state materializer, by seal state (open/sealed).",
            ),
            &["seal"],
        )
        .expect("state_materializer_shards_written_total metric");
        registry
            .register(Box::new(state_materializer_shards_written_total.clone()))
            .expect("register state_materializer_shards_written_total");

        let state_materializer_shard_bytes_total = IntCounter::new(
            "noetl_worker_state_materializer_shard_bytes_total",
            "Total bytes of Feather state-shard objects written by the shadow state materializer.",
        )
        .expect("state_materializer_shard_bytes_total metric");
        registry
            .register(Box::new(state_materializer_shard_bytes_total.clone()))
            .expect("register state_materializer_shard_bytes_total");

        let state_materializer_errors_total = IntCounter::new(
            "noetl_worker_state_materializer_errors_total",
            "Shadow state-materializer encode/write failures (counted, never failing the event).",
        )
        .expect("state_materializer_errors_total metric");
        registry
            .register(Box::new(state_materializer_errors_total.clone()))
            .expect("register state_materializer_errors_total");

        let state_materializer_evicted_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_state_materializer_evicted_total",
                "Open state shards evicted before sealing — reason idle / max_open (abandoned-execution backstop).",
            ),
            &["reason"],
        )
        .expect("state_materializer_evicted_total metric");
        registry
            .register(Box::new(state_materializer_evicted_total.clone()))
            .expect("register state_materializer_evicted_total");

        let state_materializer_open_shards = IntGauge::new(
            "noetl_worker_state_materializer_open_shards",
            "Resident open (un-sealed) state shards — the writer's working set (O(live executions)).",
        )
        .expect("state_materializer_open_shards metric");
        registry
            .register(Box::new(state_materializer_open_shards.clone()))
            .expect("register state_materializer_open_shards");

        let state_materializer_cycle_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            "noetl_worker_state_materializer_cycle_duration_seconds",
            "Latency of one shadow state-materializer drain→project→write→ack cycle.",
        ))
        .expect("state_materializer_cycle_duration_seconds metric");
        registry
            .register(Box::new(state_materializer_cycle_duration_seconds.clone()))
            .expect("register state_materializer_cycle_duration_seconds");

        Self {
            registry,
            pulls_total,
            affinity_decisions_total,
            pull_duration_seconds,
            dispatch_duration_seconds,
            dispatch_errors_total,
            event_emit_duration_seconds,
            event_emit_retries_total,
            concurrent_dispatches,
            nats_consumer_pending,
            nats_consumer_ack_pending,
            result_store_put_duration_seconds,
            result_store_put_bytes_total,
            result_store_put_errors_total,
            call_done_skipped_pending_callback_total,
            container_poll_started_total,
            container_poll_terminal_total,
            container_poll_duration_seconds,
            subscription_messages_received_total,
            subscription_executions_total,
            subscription_spooled_total,
            subscription_circuit_transitions_total,
            subscription_dead_lettered_total,
            subscription_spool_bytes,
            subscription_directives_applied_total,
            subscription_batch_dispatch_total,
            subscription_batch_messages_total,
            subscription_rate_limited_total,
            materializer_drained_total,
            materializer_projected_total,
            materializer_duplicates_total,
            materializer_acked_total,
            materializer_project_errors_total,
            materializer_cycle_duration_seconds,
            result_materializer_drained_total,
            result_materializer_writes_total,
            result_materializer_skipped_total,
            result_materializer_errors_total,
            result_materializer_cycle_duration_seconds,
            result_resolve_total,
            result_mint_authoritative_total,
            side_effect_barrier_total,
            result_tier_dr_total,
            result_producer_stage_total,
            result_resolve_duration_seconds,
            state_builder_wal_events_total,
            state_builder_event_scans_total,
            state_builder_builds_total,
            state_builder_chain_hops,
            state_builder_drive_builds_total,
            state_builder_drive_wait_total,
            state_builder_tail_total,
            state_builder_indexed_executions,
            state_builder_index_events,
            state_builder_index_bytes,
            state_builder_evictions_total,
            state_builder_rehydrate_total,
            state_shard_reads_total,
            state_shard_read_duration_seconds,
            state_equivalence_mismatch_total,
            plugin_load_seconds,
            plugin_warm_total,
            worker_ready,
            state_builder_healthy,
            state_builder_consumer_recreate_total,
            state_materializer_drained_total,
            state_materializer_rows_total,
            state_materializer_shards_written_total,
            state_materializer_shard_bytes_total,
            state_materializer_errors_total,
            state_materializer_evicted_total,
            state_materializer_open_shards,
            state_materializer_cycle_duration_seconds,
        }
    }

    /// Lazily-initialised global metrics instance.
    pub fn global() -> &'static Self {
        static GLOBAL: OnceLock<WorkerMetrics> = OnceLock::new();
        GLOBAL.get_or_init(Self::new)
    }

    /// Encode the registry's current snapshot in Prometheus text
    /// format.  Called by the `/metrics` HTTP handler.
    pub fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder
            .encode(&metric_families, &mut buffer)
            .expect("encode metrics");
        buffer
    }
}

// ---------------------------------------------------------------------------
// Public helpers — call-site-friendly façade over the global metrics.
// ---------------------------------------------------------------------------

/// Record one completed pull (any `ClaimOutcome`).
pub fn record_pull(outcome: &ClaimOutcome, duration_seconds: f64) {
    let m = WorkerMetrics::global();
    m.pulls_total
        .with_label_values(&[outcome_label(outcome)])
        .inc();
    m.pull_duration_seconds.observe(duration_seconds);
}

/// Record an execution-affinity routing decision (noetl/ai-meta#166 Phase 4).
/// `decision` is one of `owned` / `redirected` / `forced_local`
/// ([`crate::sharding::AffinityDecision::metric_label`]); the not-applicable
/// case is not recorded (it is every tool command and would swamp the
/// counter).
pub fn record_affinity_decision(decision: &str) {
    WorkerMetrics::global()
        .affinity_decisions_total
        .with_label_values(&[decision])
        .inc();
}

/// Record one completed dispatch.  `error` is `true` if the tool
/// returned an error (so the errors counter increments alongside the
/// duration histogram).
pub fn record_dispatch(tool_kind: &str, duration_seconds: f64, error: bool) {
    let m = WorkerMetrics::global();
    m.dispatch_duration_seconds
        .with_label_values(&[tool_kind])
        .observe(duration_seconds);
    if error {
        m.dispatch_errors_total
            .with_label_values(&[tool_kind])
            .inc();
    }
}

/// Record one event emission to the control plane.
pub fn record_event_emit(event_type: &str, duration_seconds: f64, retries: u32) {
    let m = WorkerMetrics::global();
    m.event_emit_duration_seconds
        .with_label_values(&[event_type])
        .observe(duration_seconds);
    if retries > 0 {
        m.event_emit_retries_total
            .with_label_values(&[event_type])
            .inc_by(retries as u64);
    }
}

/// Record subscription-runtime activity for one poll batch
/// (noetl/ai-meta#90 Phase 2).  `received` messages, of which
/// `dispatched` turned into executions and `errors` failed to dispatch.
pub fn record_subscription_batch(source: &str, received: u64, dispatched: u64, errors: u64) {
    let m = WorkerMetrics::global();
    if received > 0 {
        m.subscription_messages_received_total
            .with_label_values(&[source])
            .inc_by(received);
    }
    if dispatched > 0 {
        m.subscription_executions_total
            .with_label_values(&[source, "dispatched"])
            .inc_by(dispatched);
    }
    if errors > 0 {
        m.subscription_executions_total
            .with_label_values(&[source, "error"])
            .inc_by(errors);
    }
}

/// Record one batch dispatch (`POST /api/execute/batch`) of `count` messages
/// (noetl/ai-meta#90 Phase 7).
pub fn record_subscription_batch_dispatch(source: &str, count: u64) {
    let m = WorkerMetrics::global();
    m.subscription_batch_dispatch_total
        .with_label_values(&[source])
        .inc();
    if count > 0 {
        m.subscription_batch_messages_total
            .with_label_values(&[source])
            .inc_by(count);
    }
}

/// Record that a per-subscription rate limit engaged, by `reason`
/// (`dispatch_rate` / `max_in_flight`) — RFC §9 backpressure.
pub fn record_subscription_rate_limited(source: &str, reason: &str) {
    WorkerMetrics::global()
        .subscription_rate_limited_total
        .with_label_values(&[source, reason])
        .inc();
}

/// Record one applied header directive, by control kind.
pub fn record_subscription_directive(controls: &str) {
    WorkerMetrics::global()
        .subscription_directives_applied_total
        .with_label_values(&[controls])
        .inc();
}

/// Record one message written to the spool (RFC #90 Phase 4 §8).
pub fn record_subscription_spooled(source: &str) {
    WorkerMetrics::global()
        .subscription_spooled_total
        .with_label_values(&[source])
        .inc();
}

/// Record a circuit-breaker transition (`opened` / `closed`) for a downstream.
pub fn record_subscription_circuit(downstream: &str, transition: &str) {
    WorkerMetrics::global()
        .subscription_circuit_transitions_total
        .with_label_values(&[downstream, transition])
        .inc();
}

/// Record one dead-lettered message (poison / evicted / expired).
pub fn record_subscription_dead_lettered(source: &str) {
    WorkerMetrics::global()
        .subscription_dead_lettered_total
        .with_label_values(&[source])
        .inc();
}

/// Set the live spool byte total for a source — the cost-ceiling gauge.
pub fn set_subscription_spool_bytes(source: &str, bytes: u64) {
    WorkerMetrics::global()
        .subscription_spool_bytes
        .with_label_values(&[source])
        .set(bytes as i64);
}

/// Bump the in-flight dispatches gauge when a permit is acquired.
pub fn inc_concurrent_dispatches() {
    WorkerMetrics::global().concurrent_dispatches.inc();
}

/// Drop the in-flight dispatches gauge when a permit is released.
pub fn dec_concurrent_dispatches() {
    WorkerMetrics::global().concurrent_dispatches.dec();
}

/// Update the NATS consumer-lag gauges for one (`stream`, `consumer`)
/// pair.  Called by the periodic lag poller after fetching consumer
/// info from JetStream.  Both values are `i64` because the underlying
/// `IntGaugeVec` takes signed values; the JetStream API returns
/// `u64` so this is a `try_into` away in the caller.
pub fn record_nats_consumer_lag(stream: &str, consumer: &str, pending: i64, ack_pending: i64) {
    let m = WorkerMetrics::global();
    m.nats_consumer_pending
        .with_label_values(&[stream, consumer])
        .set(pending);
    m.nats_consumer_ack_pending
        .with_label_values(&[stream, consumer])
        .set(ack_pending);
}

/// Record one successful durable result-store PUT.  `bytes` is the
/// serialised size of the payload that was staged; the helper bumps
/// the bytes counter + observes the duration histogram.  Failures
/// use [`record_result_store_put_error`] which doesn't touch the
/// duration histogram (so percentiles only reflect successful PUTs;
/// the error counter is the separate signal for failure rate).
pub fn record_result_store_put(duration_seconds: f64, bytes: usize, _is_error: bool) {
    let m = WorkerMetrics::global();
    m.result_store_put_duration_seconds
        .observe(duration_seconds);
    m.result_store_put_bytes_total.inc_by(bytes as u64);
}

/// Record one failed durable result-store PUT.  Bumps the error
/// counter; the duration histogram is intentionally not touched so
/// percentiles stay clean (an error path tied up in a 30s reqwest
/// timeout would otherwise skew p99 on an otherwise-healthy worker).
pub fn record_result_store_put_error() {
    WorkerMetrics::global().result_store_put_errors_total.inc();
}

/// Record one skipped `call.done` emit driven by
/// `ToolResult.pending_callback = Some(true)`.  Called from
/// [`crate::executor::command`] on the success path after the tool
/// returns.  The `tool_kind` label is the executor's tool kind
/// string (today only `"container"` sets `pending_callback`, but
/// future tools that dispatch long-running external work — e.g. a
/// future GCP Batch / AWS Batch / Argo Workflow tool — would land
/// on the same counter under their own kind label).
pub fn record_call_done_skipped_pending_callback(tool_kind: &str) {
    WorkerMetrics::global()
        .call_done_skipped_pending_callback_total
        .with_label_values(&[tool_kind])
        .inc();
}

/// noetl/ai-meta#145 G2 — record that a container poll-fallback watcher
/// started for a Job in `namespace`.
pub fn record_container_poll_started(namespace: &str) {
    WorkerMetrics::global()
        .container_poll_started_total
        .with_label_values(&[namespace])
        .inc();
}

/// Record a container poll-fallback terminal outcome (`succeeded` /
/// `failed` / `poll_timeout` / `error`) plus the watch duration.
pub fn record_container_poll_terminal(state: &str, duration_secs: f64) {
    let m = WorkerMetrics::global();
    m.container_poll_terminal_total
        .with_label_values(&[state])
        .inc();
    m.container_poll_duration_seconds.observe(duration_secs);
}

/// Record one materializer drain→project→ack cycle (noetl/ai-meta#103).
/// `drained` messages were pulled; `projected`/`duplicates` came back from
/// events/project; `acked` handles were disposed. Call
/// [`record_materializer_project_error`] instead when the project failed (the
/// batch is left un-acked to redeliver).
pub fn record_materializer_cycle(
    drained: u64,
    projected: u64,
    duplicates: u64,
    acked: u64,
    duration_seconds: f64,
) {
    let m = WorkerMetrics::global();
    if drained > 0 {
        m.materializer_drained_total.inc_by(drained);
    }
    if projected > 0 {
        m.materializer_projected_total.inc_by(projected);
    }
    if duplicates > 0 {
        m.materializer_duplicates_total.inc_by(duplicates);
    }
    if acked > 0 {
        m.materializer_acked_total.inc_by(acked);
    }
    m.materializer_cycle_duration_seconds
        .observe(duration_seconds);
}

/// Record a materializer project failure — the batch is NOT acked and will
/// redeliver after the consumer's ack-wait. This is the no-loss guarantee's
/// observability surface.
pub fn record_materializer_project_error() {
    WorkerMetrics::global()
        .materializer_project_errors_total
        .inc();
}

/// Record one shadow result-materializer cycle (noetl/ai-meta#104 Phase B).
#[allow(clippy::too_many_arguments)]
pub fn record_result_materializer_cycle(
    drained: u64,
    _eligible: u64,
    feather: u64,
    json: u64,
    skipped: u64,
    errors: u64,
    duration_seconds: f64,
) {
    let m = WorkerMetrics::global();
    if drained > 0 {
        m.result_materializer_drained_total.inc_by(drained);
    }
    if feather > 0 {
        m.result_materializer_writes_total
            .with_label_values(&["feather"])
            .inc_by(feather);
    }
    if json > 0 {
        m.result_materializer_writes_total
            .with_label_values(&["json"])
            .inc_by(json);
    }
    if skipped > 0 {
        m.result_materializer_skipped_total.inc_by(skipped);
    }
    if errors > 0 {
        m.result_materializer_errors_total.inc_by(errors);
    }
    m.result_materializer_cycle_duration_seconds
        .observe(duration_seconds);
}

/// Record one resolve-by-URN read-path attempt (noetl/ai-meta#104 Phase C).
/// `outcome` is `resolved_feather` / `resolved_json` on a hit, or one of the
/// `fallback_*` labels when the caller falls back to the legacy `resolve_ref`.
pub fn record_result_resolve(outcome: &str, duration_seconds: f64) {
    let m = WorkerMetrics::global();
    m.result_resolve_total.with_label_values(&[outcome]).inc();
    m.result_resolve_duration_seconds.observe(duration_seconds);
}

/// Record one consume-side resolution under the Phase D minting flip
/// (noetl/ai-meta#104 Phase D). `path` is `tier` (the authoritative tier served)
/// or `legacy_fallback` (the dual-written `result_store` served — rollback
/// safety).
pub fn record_result_mint_authoritative(path: &str) {
    WorkerMetrics::global()
        .result_mint_authoritative_total
        .with_label_values(&[path])
        .inc();
}

/// Record one side-effect durability barrier decision (noetl/ai-meta#104 Phase E).
/// `outcome` is `skipped` (a side-effecting cycle whose durable result already
/// existed → re-execution skipped, recorded result adopted) or `executed` (no
/// durable result yet → dispatched normally). `tool` is the tool kind.
pub fn record_side_effect_barrier(outcome: &str, tool: &str) {
    WorkerMetrics::global()
        .side_effect_barrier_total
        .with_label_values(&[outcome, tool])
        .inc();
}

/// Record one result-tier DR re-derive outcome (noetl/ai-meta#104 Phase F).
/// `outcome` is `present` (durable object existed + byte-identical), `rederived`
/// (missing/corrupt → rebuilt from source), `source_gone` (no source to rebuild
/// from), or `error`.
pub fn record_result_tier_dr(outcome: &str) {
    WorkerMetrics::global()
        .result_tier_dr_total
        .with_label_values(&[outcome])
        .inc();
}

/// Record one producer-staged result tier outcome (noetl/ai-meta#104 OQ5 Option
/// A). `outcome` is `staged_feather` / `staged_json` (the producer wrote the tier
/// at emit time), `skip_parse_uri` / `skip_registry` / `error` (best-effort
/// declines), or `materializer_skip_exists` (the materializer found the
/// producer-staged object and skipped its `result_store` fetch).
pub fn record_result_producer_stage(outcome: &str) {
    WorkerMetrics::global()
        .result_producer_stage_total
        .with_label_values(&[outcome])
        .inc();
}

/// Record `n` events consumed from the `noetl_events` WAL by the off-server
/// state builder (noetl/ai-meta#115 Phase 4).
pub fn record_state_builder_wal_events(n: u64) {
    if n > 0 {
        WorkerMetrics::global()
            .state_builder_wal_events_total
            .inc_by(n);
    }
}

/// Set the count of executions currently held in the pool-side WAL index
/// (noetl/ai-meta#119).  Surfaced each drain batch so a restart that repopulates
/// the index from the retained WAL is observable (the bug was this stuck at 0).
pub fn set_state_builder_indexed_executions(n: i64) {
    WorkerMetrics::global()
        .state_builder_indexed_executions
        .set(n);
}

/// Set the total events resident across all chains in the pool-side WAL index
/// (noetl/ai-meta#166).
pub fn set_state_builder_index_events(n: i64) {
    WorkerMetrics::global().state_builder_index_events.set(n);
}

/// Set the approximate resident bytes the pool-side WAL index holds — the
/// bounded-cache byte ledger (noetl/ai-meta#166).
pub fn set_state_builder_index_bytes(n: i64) {
    WorkerMetrics::global().state_builder_index_bytes.set(n);
}

/// Record `n` bounded-cache evictions for `reason` (`ttl` | `max_executions` |
/// `byte_ceiling`) — noetl/ai-meta#166.  A no-op when `n == 0`.
pub fn record_state_builder_eviction(reason: &str, n: usize) {
    if n > 0 {
        WorkerMetrics::global()
            .state_builder_evictions_total
            .with_label_values(&[reason])
            .inc_by(n as u64);
    }
}

/// Record one cold-rebuild-on-miss outcome (`served` | `incomplete` | `empty` |
/// `throttled`) — noetl/ai-meta#166 §5.2.
pub fn record_state_builder_rehydrate(outcome: &str) {
    WorkerMetrics::global()
        .state_builder_rehydrate_total
        .with_label_values(&[outcome])
        .inc();
}

/// Record one cold-load-from-shard outcome (`hit` | `miss` | `fallback`) —
/// noetl/ai-meta#166 Phase 3.
pub fn record_state_shard_read(outcome: &str) {
    WorkerMetrics::global()
        .state_shard_reads_total
        .with_label_values(&[outcome])
        .inc();
}

/// Observe one cold-load-from-shard latency sample (seconds) — noetl/ai-meta#166
/// Phase 3.  The payoff number vs the WAL-replay miss cost.
pub fn observe_state_shard_read_duration(secs: f64) {
    WorkerMetrics::global()
        .state_shard_read_duration_seconds
        .observe(secs);
}

/// Record one shard-vs-WAL spine divergence (the `NOETL_STATE_SHARD_READ_VERIFY`
/// dual-build tripwire) — noetl/ai-meta#166 Phase 3.  MUST stay 0.
pub fn record_state_equivalence_mismatch() {
    WorkerMetrics::global().state_equivalence_mismatch_total.inc();
}

/// Record one off-server state build outcome (`cache_hit` | `incremental` |
/// `cold_rebuild` | `incomplete`).
pub fn record_state_builder_build(outcome: &str) {
    WorkerMetrics::global()
        .state_builder_builds_total
        .with_label_values(&[outcome])
        .inc();
}

/// Record the chain-walk depth of one off-server cold rebuild.
pub fn record_state_builder_chain_hops(hops: usize) {
    WorkerMetrics::global()
        .state_builder_chain_hops
        .observe(hops as f64);
}

/// Record one off-server DRIVE build outcome (`served` | `fallback_incomplete` |
/// `fallback_disabled`) — RFC #115 Phase 4 cutover.
pub fn record_state_builder_drive(outcome: &str) {
    WorkerMetrics::global()
        .state_builder_drive_builds_total
        .with_label_values(&[outcome])
        .inc();
}

/// Record one off-server DRIVE build-retry wait by outcome (`woken` when the
/// drain's append signal fired, `timeout` when the per-wait cap elapsed).
/// noetl/ai-meta#130 — proof the event-signalled drive wakes on WAL appends
/// rather than polling a fixed grid.
pub fn record_state_builder_drive_wait(outcome: &str) {
    WorkerMetrics::global()
        .state_builder_drive_wait_total
        .with_label_values(&[outcome])
        .inc();
}

/// Record one off-server drive tail-attach (noetl/ai-meta#156): `attached` events
/// the server shipped on the dispatch and `applied_new` of those that were new to
/// the pool-side WAL index (the rest were already drained — an idempotent
/// overwrite).  A no-op when `attached == 0`.
pub fn record_offserver_tail_applied(attached: usize, applied_new: usize) {
    if attached == 0 {
        return;
    }
    let m = WorkerMetrics::global();
    m.state_builder_tail_total
        .with_label_values(&["attached"])
        .inc_by(attached as u64);
    if applied_new > 0 {
        m.state_builder_tail_total
            .with_label_values(&["applied_new"])
            .inc_by(applied_new as u64);
    }
}

/// Record one wasm plug-in load phase latency (`fetch` — HTTP GET of the module
/// bytes; `compile` — Cranelift `Module::new`).  noetl/ai-meta#130 cold-start
/// attribution: the `compile` phase on the first dispatch is the one-time cost
/// boot-warmup removes.
pub fn record_plugin_load(phase: &str, duration_seconds: f64) {
    WorkerMetrics::global()
        .plugin_load_seconds
        .with_label_values(&[phase])
        .observe(duration_seconds);
}

/// Record the boot-time plug-in warmup outcome (`warmed` | `skipped` | `error`).
/// noetl/ai-meta#130.
pub fn record_plugin_warm(outcome: &str) {
    WorkerMetrics::global()
        .plugin_warm_total
        .with_label_values(&[outcome])
        .inc();
}

/// Set the worker-readiness gauge (`true` once boot warmup completed).  The
/// `/readyz` probe reads this so Kubernetes only marks the pod Ready once warm.
/// noetl/ai-meta#130.
pub fn set_worker_ready(ready: bool) {
    WorkerMetrics::global()
        .worker_ready
        .set(if ready { 1 } else { 0 });
}

/// Read the worker-readiness gauge — the `/readyz` handler's source of truth.
pub fn worker_ready() -> bool {
    WorkerMetrics::global().worker_ready.get() == 1
}

/// Set the state-builder health gauge (noetl/ai-meta#161).  `true` while the
/// authoritative WAL drain is connected and serving; `false` once it has been
/// continuously erroring against a dead JetStream consumer past the unhealthy
/// threshold.  The `/livez` probe reads this so a wedged system-pool pod is
/// auto-restarted by Kubernetes as the backstop to the in-process self-heal.
pub fn set_state_builder_healthy(healthy: bool) {
    WorkerMetrics::global()
        .state_builder_healthy
        .set(if healthy { 1 } else { 0 });
}

/// Read the state-builder health gauge — the `/livez` handler's source of truth.
pub fn state_builder_healthy() -> bool {
    WorkerMetrics::global().state_builder_healthy.get() == 1
}

/// Record a state-builder consumer/connection rebuild (noetl/ai-meta#161).
/// `reason` is `connect_error` (initial connect / create_consumer retry) or
/// `drain_dead` (a live consumer hit the dead-consumer signature past threshold
/// and was torn down + recreated — the self-heal firing).
pub fn record_state_builder_consumer_recreate(reason: &str) {
    WorkerMetrics::global()
        .state_builder_consumer_recreate_total
        .with_label_values(&[reason])
        .inc();
}

/// Record one shadow state-materializer drain cycle (noetl/ai-meta#166 Phase 2).
#[allow(clippy::too_many_arguments)]
pub fn record_state_materializer_cycle(
    drained: u64,
    rows: u64,
    shards_written: u64,
    sealed: u64,
    shard_bytes: u64,
    skipped: u64,
    errors: u64,
    duration_seconds: f64,
) {
    let _ = skipped; // counted in the loop's debug line; no dedicated metric.
    let m = WorkerMetrics::global();
    if drained > 0 {
        m.state_materializer_drained_total.inc_by(drained);
    }
    if rows > 0 {
        m.state_materializer_rows_total.inc_by(rows);
    }
    // shards_written counts BOTH open + sealed writes this cycle; `sealed` is the
    // subset that sealed, so the open writes are the difference.
    let open_writes = shards_written.saturating_sub(sealed);
    if open_writes > 0 {
        m.state_materializer_shards_written_total
            .with_label_values(&["open"])
            .inc_by(open_writes);
    }
    if sealed > 0 {
        m.state_materializer_shards_written_total
            .with_label_values(&["sealed"])
            .inc_by(sealed);
    }
    if shard_bytes > 0 {
        m.state_materializer_shard_bytes_total.inc_by(shard_bytes);
    }
    if errors > 0 {
        m.state_materializer_errors_total.inc_by(errors);
    }
    m.state_materializer_cycle_duration_seconds
        .observe(duration_seconds);
}

/// Set the resident-open-shards gauge (noetl/ai-meta#166 Phase 2).
pub fn set_state_materializer_open_shards(n: i64) {
    WorkerMetrics::global().state_materializer_open_shards.set(n);
}

/// Record open state shards evicted before sealing (noetl/ai-meta#166 Phase 2).
/// `reason` is `idle` (TTL sweep) or `max_open` (resident-ceiling backstop).
pub fn record_state_materializer_evicted(reason: &str, n: usize) {
    WorkerMetrics::global()
        .state_materializer_evicted_total
        .with_label_values(&[reason])
        .inc_by(n as u64);
}

// Unused-warning suppression for fields that aren't read directly
// outside the helper functions.  The fields ARE used via the
// registry's encode() output; this just keeps clippy quiet.
#[allow(dead_code)]
const _: () = {
    let _ = &CounterVec::new;
};

#[cfg(test)]
mod tests {
    use super::*;
    use noetl_executor::worker::source::Command;

    fn dummy_command(id: &str) -> Command {
        Command {
            command_id: id.to_string(),
            execution_id: 1,
            step: "s".to_string(),
            tool_kind: "http".to_string(),
            input: serde_json::Value::Null,
            render_context: Default::default(),
            attempts: 0,
        }
    }

    #[test]
    fn outcome_label_returns_distinct_strings() {
        assert_eq!(
            outcome_label(&ClaimOutcome::Claimed(dummy_command("c"))),
            "claimed"
        );
        assert_eq!(
            outcome_label(&ClaimOutcome::AlreadyClaimed),
            "already_claimed"
        );
        assert_eq!(
            outcome_label(&ClaimOutcome::RetryLater("e".into())),
            "retry_later"
        );
        assert_eq!(outcome_label(&ClaimOutcome::Failed("e".into())), "failed");
    }

    #[test]
    fn record_pull_increments_counter_and_histogram() {
        let m = WorkerMetrics::global();
        let before = m.pulls_total.with_label_values(&["claimed"]).get();
        record_pull(&ClaimOutcome::Claimed(dummy_command("c")), 0.012);
        let after = m.pulls_total.with_label_values(&["claimed"]).get();
        assert_eq!(after, before + 1);
        // Histogram sample count must increase too.
        assert!(m.pull_duration_seconds.get_sample_count() > 0);
    }

    #[test]
    fn record_dispatch_separates_errors_from_successes() {
        let m = WorkerMetrics::global();
        let before_errors = m
            .dispatch_errors_total
            .with_label_values(&["postgres"])
            .get();
        record_dispatch("postgres", 0.5, false);
        record_dispatch("postgres", 0.6, true);
        let after_errors = m
            .dispatch_errors_total
            .with_label_values(&["postgres"])
            .get();
        assert_eq!(
            after_errors,
            before_errors + 1,
            "only error path increments errors counter"
        );
    }

    #[test]
    fn record_event_emit_increments_retries_only_when_present() {
        let m = WorkerMetrics::global();
        let before = m
            .event_emit_retries_total
            .with_label_values(&["command.completed"])
            .get();
        record_event_emit("command.completed", 0.020, 0); // no retries
        let mid = m
            .event_emit_retries_total
            .with_label_values(&["command.completed"])
            .get();
        assert_eq!(mid, before, "no retries -> counter unchanged");
        record_event_emit("command.completed", 0.060, 2); // 2 retries
        let after = m
            .event_emit_retries_total
            .with_label_values(&["command.completed"])
            .get();
        assert_eq!(after, mid + 2, "2 retries -> counter += 2");
    }

    #[test]
    fn concurrent_dispatches_gauge_round_trips() {
        let m = WorkerMetrics::global();
        let baseline = m.concurrent_dispatches.get();
        inc_concurrent_dispatches();
        inc_concurrent_dispatches();
        assert_eq!(m.concurrent_dispatches.get(), baseline + 2);
        dec_concurrent_dispatches();
        dec_concurrent_dispatches();
        assert_eq!(m.concurrent_dispatches.get(), baseline);
    }

    #[test]
    fn encode_emits_prometheus_text_format() {
        record_pull(&ClaimOutcome::Claimed(dummy_command("c")), 0.1);
        let bytes = WorkerMetrics::global().encode();
        let text = String::from_utf8(bytes).unwrap();
        // Standard Prometheus text format starts each metric with
        // a `# HELP` / `# TYPE` comment block.
        assert!(text.contains("# HELP noetl_worker_pulls_total"));
        assert!(text.contains("# TYPE noetl_worker_pulls_total counter"));
        // The counter value line must include the outcome label.
        assert!(text.contains("noetl_worker_pulls_total{outcome=\"claimed\"}"));
    }

    /// `record_nats_consumer_lag` is the only path that touches the
    /// new gauges; this test exercises it directly + verifies the
    /// label set is what the dashboard / KEDA expects.
    #[test]
    fn record_nats_consumer_lag_updates_both_gauges() {
        let m = WorkerMetrics::global();
        record_nats_consumer_lag("noetl_commands", "worker-pool", 42, 7);
        let pending = m
            .nats_consumer_pending
            .with_label_values(&["noetl_commands", "worker-pool"])
            .get();
        let ack_pending = m
            .nats_consumer_ack_pending
            .with_label_values(&["noetl_commands", "worker-pool"])
            .get();
        assert_eq!(pending, 42);
        assert_eq!(ack_pending, 7);

        // Re-recording overwrites the previous sample (gauges
        // aren't cumulative).
        record_nats_consumer_lag("noetl_commands", "worker-pool", 100, 3);
        let pending2 = m
            .nats_consumer_pending
            .with_label_values(&["noetl_commands", "worker-pool"])
            .get();
        assert_eq!(pending2, 100);
    }

    /// The two new gauges appear in the encoded Prometheus output
    /// with the `stream` + `consumer` labels.  Locks in the wire
    /// format the KEDA prometheus-trigger scrapes.
    #[test]
    fn nats_consumer_lag_gauges_emit_in_prometheus_text() {
        record_nats_consumer_lag("noetl_commands", "worker-pool", 5, 2);
        let bytes = WorkerMetrics::global().encode();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.contains("# HELP noetl_worker_nats_consumer_pending"));
        assert!(text.contains("# TYPE noetl_worker_nats_consumer_pending gauge"));
        assert!(text.contains(
            "noetl_worker_nats_consumer_pending{consumer=\"worker-pool\",stream=\"noetl_commands\"}"
        ));
        assert!(text.contains("# HELP noetl_worker_nats_consumer_ack_pending"));
        assert!(text.contains("# TYPE noetl_worker_nats_consumer_ack_pending gauge"));
    }

    /// noetl/ai-meta#43 Round 4 — `pending_callback` skip counter.
    /// Verifies the label is `tool_kind`, the counter increments per
    /// call, and the metric surfaces in the encoded Prometheus text.
    #[test]
    fn call_done_skipped_pending_callback_counter_increments_per_tool_kind() {
        let m = WorkerMetrics::global();
        let before_container = m
            .call_done_skipped_pending_callback_total
            .with_label_values(&["container"])
            .get();
        record_call_done_skipped_pending_callback("container");
        record_call_done_skipped_pending_callback("container");
        let after_container = m
            .call_done_skipped_pending_callback_total
            .with_label_values(&["container"])
            .get();
        assert_eq!(
            after_container,
            before_container + 2,
            "two container skips -> counter += 2"
        );

        // Distinct tool_kind labels keep their own series — the
        // dashboard can split by future tools that adopt the marker.
        let before_future = m
            .call_done_skipped_pending_callback_total
            .with_label_values(&["future_async_tool"])
            .get();
        record_call_done_skipped_pending_callback("future_async_tool");
        let after_future = m
            .call_done_skipped_pending_callback_total
            .with_label_values(&["future_async_tool"])
            .get();
        assert_eq!(after_future, before_future + 1);
        // Container series is unchanged by the unrelated label.
        assert_eq!(
            m.call_done_skipped_pending_callback_total
                .with_label_values(&["container"])
                .get(),
            after_container
        );

        let text = String::from_utf8(m.encode()).unwrap();
        assert!(text.contains("# HELP noetl_worker_call_done_skipped_pending_callback_total"));
        assert!(
            text.contains("# TYPE noetl_worker_call_done_skipped_pending_callback_total counter")
        );
        assert!(text.contains(
            "noetl_worker_call_done_skipped_pending_callback_total{tool_kind=\"container\"}"
        ));
    }

    /// `record_result_store_put` observes the duration histogram +
    /// bumps the bytes counter on success; `record_result_store_put_error`
    /// bumps the error counter independently.  Both metrics must
    /// surface in the encoded Prometheus text so dashboards can scrape
    /// them.
    #[test]
    fn result_store_metrics_round_trip_through_encode() {
        let m = WorkerMetrics::global();
        let before_bytes = m.result_store_put_bytes_total.get();
        let before_errors = m.result_store_put_errors_total.get();

        record_result_store_put(0.025, 200 * 1024, false);
        record_result_store_put_error();

        assert_eq!(
            m.result_store_put_bytes_total.get(),
            before_bytes + 200 * 1024
        );
        assert_eq!(m.result_store_put_errors_total.get(), before_errors + 1);

        let text = String::from_utf8(m.encode()).unwrap();
        assert!(text.contains("# HELP noetl_worker_result_store_put_duration_seconds"));
        assert!(text.contains("# TYPE noetl_worker_result_store_put_duration_seconds histogram"));
        assert!(text.contains("# HELP noetl_worker_result_store_put_bytes_total"));
        assert!(text.contains("# TYPE noetl_worker_result_store_put_bytes_total counter"));
        assert!(text.contains("# HELP noetl_worker_result_store_put_errors_total"));
        assert!(text.contains("# TYPE noetl_worker_result_store_put_errors_total counter"));
    }
}
