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
//! | `noetl_worker_event_emit_failed_total` | counter | `event_type` | Emissions ABANDONED after every retry — the event never reached the durable log. |
//! | `noetl_worker_concurrent_dispatches` | gauge | — | Live count of in-flight dispatches (semaphore depth) |
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
    /// Emissions abandoned after exhausting retries (noetl/ai-meta#238).
    pub event_emit_failed_total: IntCounterVec,
    /// Claim-coordinator reconnects on the EHDB command path, by reason.
    ///
    /// The EHDB claim loop is how every command reaches this worker post-T5.
    /// Both of its failure paths retried with a `tracing::warn!` and nothing
    /// else, so "how often is the worker losing its claim connection?" could
    /// only be answered by scraping logs.  Measured on one production pod:
    /// **85 such warnings in 24h**, all invisible to monitoring.
    ///
    /// noetl/ai-meta#208 is the quiet version of this exact failure — a
    /// restarted writer left the claim read parked forever and dispatch stopped
    /// with nothing logged anywhere, for ~2.4 days.  That fix added the log
    /// line; this adds the signal (`agents/rules/observability.md` Principle 2).
    pub ehdb_claim_reconnect_total: IntCounterVec,
    /// Always 1; the `version` label identifies the running binary.
    ///
    /// `Registry::gather` prunes empty metric families, so a labelled metric is
    /// ABSENT from `/metrics` until a child series exists — every counter here
    /// is invisible until it first fires, and absent cannot be told apart from
    /// "this binary is too old to have the metric".  Pinning known label values
    /// settles that per metric, but `event_emit_failed_total{event_type}` takes
    /// a free-form String and cannot be pinned at all.  This gauge settles it
    /// once for the whole process (noetl/ai-meta#238).
    pub build_info: IntGaugeVec,
    pub concurrent_dispatches: IntGauge,
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
    /// Drained messages that carried no `event_id`, so no envelope could be
    /// built and nothing was projected.
    ///
    /// Under `NOETL_EVENT_INGEST_PUBLISH_ONLY` the materializer is the SOLE
    /// writer of `noetl.event`, so a skipped message is an event that never
    /// reaches the durable log — and the batch is acked regardless, so it is
    /// gone from the feed too.  Before this the only trace was a
    /// `tracing::warn!`, the same shape as noetl/ai-meta#208: a real loss
    /// visible only to whoever happened to be reading logs.
    ///
    /// Unlabelled deliberately.  A plain `IntCounter` is always present in
    /// `/metrics` at 0, where a labelled one would be absent until the first
    /// skip — the exact ambiguity being removed everywhere else today.
    ///
    /// Distinct from `noetl_worker_result_materializer_skipped_total`, which
    /// belongs to the RESULT materializer.  The names differ by one word.
    pub materializer_skipped_total: IntCounter,
    /// Materializer ack failures, by the stage they happened at.
    ///
    /// The three stages differ sharply in consequence, which is why they are
    /// separated rather than counted together:
    ///
    /// - `non_event_batch` — the ack that advances past a batch with nothing
    ///   materializable.  Its own comment says the batch "poison-loops forever"
    ///   without it, so a sustained rate here is a STALLED materializer.
    /// - `after_project` — the rows are already durable and only the ack
    ///   failed, so the records redeliver and `events/project` dedupes them by
    ///   event_id.  Costs a repeat, never a row.
    /// - `per_handle` — a partial ack within an otherwise successful batch.
    pub materializer_ack_failed_total: IntCounterVec,
    /// Drain polls that failed outright, after which the loop backs off.
    ///
    /// Unlabelled so it is present at 0 without any activity: under the
    /// publish-only gate a materializer that cannot drain is a durable log that
    /// stops being written, and that must not be one of the metrics you cannot
    /// see.
    pub materializer_drain_failed_total: IntCounter,
    /// Why a cold-rebuild replay loop stopped, by reason.
    ///
    /// Four different conditions `break` out of that loop identically, and only
    /// one of them is a defect: `feed_error` means the feed dropped mid-replay,
    /// so the rebuilt state is INCOMPLETE.  The other three are ordinary
    /// termination.  Without this they are indistinguishable, and the code
    /// comment at that site already cites noetl/ai-meta#227 — stalled
    /// executions whose re-issued command does not advance them.
    pub state_builder_replay_end_total: IntCounterVec,
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
    /// Wall time of one off-server state build, labelled by the SAME outcome as
    /// `state_builder_builds_total` (noetl/ai-meta#156).  The counter says how
    /// often each path is taken; this says what each costs.  Without it the
    /// per-hop drive-build floor is unmeasurable on prod, which is the reason
    /// #156 could quantify latency in kind but not in production.
    pub state_builder_build_duration_seconds: HistogramVec,
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
    /// How many EHDB writer hosts sealed on the graceful shutdown path, and how
    /// many there were to seal (noetl/ai-meta#226).
    ///
    /// A partial seal used to be observable only on the *next* boot, as a
    /// `clamped=true` resume over a log that had come back below its own
    /// persisted cursor — i.e. after the records were already gone. These make
    /// it a scrape: `sealed < hosts` on a terminating pod means an unsealed
    /// tail. The pair is written once, immediately before exit; a pod that
    /// terminates without ever setting them never reached the seal at all.
    /// Reattaches to an events face after its connection was found dead
    /// (noetl/ai-meta#225), partitioned by `face` (`group_claim` = :9104,
    /// `wal` = :9108).
    ///
    /// Before #225 neither face could *detect* a half-open connection, so this
    /// counter could not have moved even while the consumers were wedged and
    /// `noetl.event` had gone 3h24m without a write. A rising count around a
    /// writer restart is the reattach working; a flat count while a group cursor
    /// is not advancing is the wedge.
    pub events_consumer_redials_total: IntCounterVec,
    pub shutdown_hosts_sealed: IntGauge,
    pub shutdown_hosts_total: IntGauge,
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
    /// Platform-automatic sink observations (noetl/ai-meta#199 Slice C), by
    /// `outcome`: `candidate` (a resident execution past the byte threshold whose
    /// context the write slice would auto-sink), `skipped_explicit` (an explicit
    /// sink step already owns it — double-write avoidance), `observed_only` (one
    /// completed observe pass — the first slice writes nothing).
    pub autosink_total: IntCounterVec,
    /// Sink-confirmation-gated eviction outcomes (noetl/ai-meta#198): `marked`
    /// (a chain flagged as holding un-sunk business context), `confirmed` (its
    /// context was sunk to the customer store and the chain dropped), `retained`
    /// (an eviction skipped because the chain's context is not yet sunk).
    pub sink_gate_events_total: IntCounterVec,
    /// Sink-confirmation signals the connector-step wiring emits
    /// (noetl/ai-meta#199 Slice A), labeled by `tool_kind` and `signal`
    /// (`mark` when a declared sink step dispatches, `confirm` when it succeeds).
    /// Distinct from `sink_gate_events_total`, which records what the gate *did*:
    /// this counter records the wiring *firing* even when the gate is off, so the
    /// signal path is observable before an operator opts into eviction.
    pub sink_signal_total: IntCounterVec,
    /// Outcomes of posting a sink-state signal to the SERVER's feed
    /// (noetl/ai-meta#199 Slice A): `action` is `mark` / `confirm`, `outcome` is
    /// `ok` / `http_error` / `error`.
    ///
    /// Separate from `sink_signal_total` on purpose. That one counts the signal
    /// being *produced*; this counts it *reaching the server*. They diverge
    /// exactly when the server GC gate is being starved of input, which is the
    /// failure this wiring exists to prevent and which is otherwise invisible —
    /// a lost post degrades silently to the pre-Slice-A behaviour.
    pub sink_state_post_total: IntCounterVec,
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

    /// Count of MAIN command-loop in-process NATS reconnects (noetl/ai-meta#163),
    /// partitioned by `reason`: `pull` / `ack` / `nack` — a hard NATS disconnect
    /// surfaced on that operation and the loop rebuilt the subscriber in-process
    /// (instead of the pre-#163 behaviour: propagate the error → `exit(1)` → k8s
    /// crash-restart + full WAL replay); `connect_error` — a rebuild attempt itself
    /// failed and is being retried with backoff.  A rising `pull`/`ack`/`nack`
    /// rate with the worker's `restart_count` staying flat is the fix working — the
    /// command loop surviving a `nats-0` bounce without a pod restart.
    pub command_loop_reconnect_total: IntCounterVec,

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

        let event_emit_failed_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_event_emit_failed_total",
                "Event emissions abandoned after every retry — the event never reached the durable log.",
            ),
            &["event_type"],
        )
        .expect("event_emit_failed_total metric");
        registry
            .register(Box::new(event_emit_failed_total.clone()))
            .expect("register event_emit_failed_total");

        let ehdb_claim_reconnect_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_ehdb_claim_reconnect_total",
                "EHDB claim-coordinator reconnects, by feed and reason (noetl/ai-meta#238).",
            ),
            &["feed", "reason"],
        )
        .expect("ehdb_claim_reconnect_total metric");
        registry
            .register(Box::new(ehdb_claim_reconnect_total.clone()))
            .expect("register ehdb_claim_reconnect_total");
        for feed in EHDB_CLAIM_FEEDS {
            for reason in EHDB_CLAIM_RECONNECT_REASONS {
                ehdb_claim_reconnect_total
                    .with_label_values(&[feed, reason])
                    .inc_by(0);
            }
        }

        let build_info = IntGaugeVec::new(
            prometheus::Opts::new(
                "noetl_worker_build_info",
                "Always 1; the version label identifies the running binary (noetl/ai-meta#238).",
            ),
            &["version"],
        )
        .expect("build_info metric");
        registry
            .register(Box::new(build_info.clone()))
            .expect("register build_info");
        // Set here rather than in a startup hook: an empty family is pruned at
        // gather time, so a registered-but-unset gauge is still absent.
        build_info
            .with_label_values(&[env!("CARGO_PKG_VERSION")])
            .set(1);

        let concurrent_dispatches = IntGauge::new(
            "noetl_worker_concurrent_dispatches",
            "Number of dispatches currently in flight (semaphore depth).",
        )
        .expect("concurrent_dispatches metric");
        registry
            .register(Box::new(concurrent_dispatches.clone()))
            .expect("register concurrent_dispatches");

        // The NATS consumer-lag gauges were removed here (noetl/ai-meta#242).
        // They measured JetStream consumer depth, and T5 deleted JetStream —
        // their poller (`crate::nats::lag_poller`) went with it, so the pair
        // could only ever read whatever they held at the moment the poller was
        // removed.  The KEDA queue-depth signal is now `ehdb_events_group_lag`,
        // scraped from the writer.

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

        let materializer_skipped_total = IntCounter::new(
            "noetl_worker_materializer_skipped_total",
            "Drained messages with no event_id — never projected into noetl.event (noetl/ai-meta#238).",
        )
        .expect("materializer_skipped_total metric");
        registry
            .register(Box::new(materializer_skipped_total.clone()))
            .expect("register materializer_skipped_total");

        let materializer_ack_failed_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_materializer_ack_failed_total",
                "Materializer ack failures by stage — non_event_batch means a poison-loop (noetl/ai-meta#238).",
            ),
            &["stage"],
        )
        .expect("materializer_ack_failed_total metric");
        registry
            .register(Box::new(materializer_ack_failed_total.clone()))
            .expect("register materializer_ack_failed_total");
        for stage in MATERIALIZER_ACK_STAGES {
            materializer_ack_failed_total
                .with_label_values(&[stage])
                .inc_by(0);
        }

        let materializer_drain_failed_total = IntCounter::new(
            "noetl_worker_materializer_drain_failed_total",
            "Materializer drain polls that failed outright (noetl/ai-meta#238).",
        )
        .expect("materializer_drain_failed_total metric");
        registry
            .register(Box::new(materializer_drain_failed_total.clone()))
            .expect("register materializer_drain_failed_total");

        let state_builder_replay_end_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_state_builder_replay_end_total",
                "Why a cold-rebuild replay stopped — only feed_error means the state is incomplete (noetl/ai-meta#227).",
            ),
            &["reason"],
        )
        .expect("state_builder_replay_end_total metric");
        registry
            .register(Box::new(state_builder_replay_end_total.clone()))
            .expect("register state_builder_replay_end_total");
        for reason in STATE_BUILDER_REPLAY_END_REASONS {
            state_builder_replay_end_total
                .with_label_values(&[reason])
                .inc_by(0);
        }

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

        let events_consumer_redials_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_events_consumer_redials_total",
                "Reattaches to an events face after its connection was found dead \
                 (noetl/ai-meta#225), partitioned by face.",
            ),
            &["face"],
        )
        .expect("events_consumer_redials_total metric");
        registry
            .register(Box::new(events_consumer_redials_total.clone()))
            .expect("register events_consumer_redials_total");

        let shutdown_hosts_sealed = IntGauge::new(
            "noetl_worker_shutdown_hosts_sealed",
            "EHDB writer hosts that sealed on the graceful shutdown path (noetl/ai-meta#226). Compare against noetl_worker_shutdown_hosts_total: sealed < total means an unsealed tail whose next resume will clamp.",
        )
        .expect("shutdown_hosts_sealed metric");
        registry
            .register(Box::new(shutdown_hosts_sealed.clone()))
            .expect("register shutdown_hosts_sealed");

        let shutdown_hosts_total = IntGauge::new(
            "noetl_worker_shutdown_hosts_total",
            "EHDB writer hosts this process was asked to seal on shutdown (noetl/ai-meta#226).",
        )
        .expect("shutdown_hosts_total metric");
        registry
            .register(Box::new(shutdown_hosts_total.clone()))
            .expect("register shutdown_hosts_total");

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

        let autosink_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_autosink_total",
                "Platform-automatic sink observations — candidate / skipped_explicit / observed_only (noetl/ai-meta#199).",
            ),
            &["outcome"],
        )
        .expect("autosink_total metric");
        registry
            .register(Box::new(autosink_total.clone()))
            .expect("register autosink_total");

        let sink_gate_events_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_sink_gate_events_total",
                "Sink-confirmation-gated eviction outcomes — marked / confirmed / retained (noetl/ai-meta#198).",
            ),
            &["outcome"],
        )
        .expect("sink_gate_events_total metric");
        registry
            .register(Box::new(sink_gate_events_total.clone()))
            .expect("register sink_gate_events_total");

        let sink_signal_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_sink_signal_total",
                "Sink-confirmation signals the connector-step wiring emits — mark / confirm, by tool_kind (noetl/ai-meta#199).",
            ),
            &["tool_kind", "signal"],
        )
        .expect("sink_signal_total metric");
        registry
            .register(Box::new(sink_signal_total.clone()))
            .expect("register sink_signal_total");

        let sink_state_post_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_sink_state_post_total",
                "Sink-state signals posted to the server's noetl.sink_pending feed, by action and outcome (noetl/ai-meta#199 Slice A). Divergence from sink_signal_total means the server GC gate is being starved.",
            ),
            &["action", "outcome"],
        )
        .expect("sink_state_post_total metric");
        registry
            .register(Box::new(sink_state_post_total.clone()))
            .expect("register sink_state_post_total");

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

        // Buckets span the observed range: a cache hit is sub-millisecond, a
        // cold rebuild over a large event log has been seen in the seconds.
        // A single histogram with the outcome label makes the expensive path
        // separable from the cheap one — an aggregate p95 over all outcomes
        // hides exactly the cold-rebuild tail #156 is chasing.
        let state_builder_build_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "noetl_worker_state_builder_build_duration_seconds",
                "Off-server state build wall time by outcome (noetl/ai-meta#156).",
            )
            .buckets(vec![
                0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
            &["outcome"],
        )
        .expect("state_builder_build_duration_seconds metric");
        registry
            .register(Box::new(state_builder_build_duration_seconds.clone()))
            .expect("register state_builder_build_duration_seconds");

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

        // Off-server state-builder outcome series, pinned at 0 for the same
        // reason (noetl/ai-meta#238).  These three specifically, because
        // noetl/ai-meta#227 names `drive_builds{outcome="fallback_incomplete"}`
        // and `drive_wait{outcome="timeout"}` as the signals for root-causing
        // why a re-issued command does not advance a stalled execution — and
        // an absent counter reads the same as a zero one, so neither could be
        // concluded from.
        for outcome in STATE_BUILDER_DRIVE_OUTCOMES {
            state_builder_drive_builds_total
                .with_label_values(&[outcome])
                .inc_by(0);
        }
        for outcome in STATE_BUILDER_DRIVE_WAIT_OUTCOMES {
            state_builder_drive_wait_total
                .with_label_values(&[outcome])
                .inc_by(0);
        }
        for outcome in STATE_BUILDER_BUILD_OUTCOMES {
            state_builder_builds_total
                .with_label_values(&[outcome])
                .inc_by(0);
        }

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

        let command_loop_reconnect_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_command_loop_reconnect_total",
                "Main command-loop in-process NATS reconnects — reason pull / ack / nack (a hard disconnect surfaced on that op and the subscriber was rebuilt in-process) or connect_error (a rebuild attempt failed, retrying) — the noetl/ai-meta#163 fix firing.",
            ),
            &["reason"],
        )
        .expect("command_loop_reconnect_total metric");
        registry
            .register(Box::new(command_loop_reconnect_total.clone()))
            .expect("register command_loop_reconnect_total");

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
            event_emit_failed_total,
            ehdb_claim_reconnect_total,
            build_info,
            concurrent_dispatches,
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
            materializer_skipped_total,
            materializer_ack_failed_total,
            materializer_drain_failed_total,
            state_builder_replay_end_total,
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
            state_builder_build_duration_seconds,
            state_builder_chain_hops,
            state_builder_drive_builds_total,
            state_builder_drive_wait_total,
            state_builder_tail_total,
            events_consumer_redials_total,
            shutdown_hosts_sealed,
            shutdown_hosts_total,
            state_builder_indexed_executions,
            state_builder_index_events,
            state_builder_index_bytes,
            state_builder_evictions_total,
            autosink_total,
            sink_gate_events_total,
            sink_signal_total,
            sink_state_post_total,
            state_builder_rehydrate_total,
            state_shard_reads_total,
            state_shard_read_duration_seconds,
            state_equivalence_mismatch_total,
            plugin_load_seconds,
            plugin_warm_total,
            worker_ready,
            state_builder_healthy,
            state_builder_consumer_recreate_total,
            command_loop_reconnect_total,
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
/// Record an event emission ABANDONED after every retry.
///
/// `record_event_emit` covers the success path and counts retries; nothing
/// counted the give-up.  That distinction matters here more than on most paths:
/// a failed emission means the event never reached the durable log, so the
/// execution's history has a hole and no later read can tell.  Retries rising
/// is a flaky control plane; this rising is data loss.
pub fn record_event_emit_failed(event_type: &str) {
    WorkerMetrics::global()
        .event_emit_failed_total
        .with_label_values(&[event_type])
        .inc();
}

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
/// Every reason a cold-rebuild replay loop terminates.
///
/// `feed_error` is the only one that indicates incomplete state; the rest are
/// ordinary termination and exist so that a rise in `feed_error` is legible
/// against them rather than in isolation.
pub const STATE_BUILDER_REPLAY_END_REASONS: [&str; 4] =
    ["complete", "deadline", "max_messages", "feed_error"];

/// Record why one cold-rebuild replay stopped.
pub fn record_state_builder_replay_end(reason: &str) {
    WorkerMetrics::global()
        .state_builder_replay_end_total
        .with_label_values(&[reason])
        .inc();
}

/// The stages at which a materializer ack can fail.
pub const MATERIALIZER_ACK_STAGES: [&str; 3] =
    ["non_event_batch", "after_project", "per_handle"];

/// Record one materializer ack failure at `stage`.
pub fn record_materializer_ack_failed(stage: &str) {
    WorkerMetrics::global()
        .materializer_ack_failed_total
        .with_label_values(&[stage])
        .inc();
}

/// Record one failed materializer drain poll.
pub fn record_materializer_drain_failed() {
    WorkerMetrics::global().materializer_drain_failed_total.inc();
}

/// Record `n` drained messages that could not be materialised.
pub fn record_materializer_skipped(n: u64) {
    WorkerMetrics::global().materializer_skipped_total.inc_by(n);
}

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
/// Count a reattach to an events face after its connection was found dead
/// (noetl/ai-meta#225). `face` is `group_claim` (:9104) or `wal` (:9108).
pub fn record_events_consumer_redial(face: &str) {
    WorkerMetrics::global()
        .events_consumer_redials_total
        .with_label_values(&[face])
        .inc();
}

/// Record the outcome of the graceful writer seal (noetl/ai-meta#226).
///
/// Written once, immediately before exit. A terminating pod whose
/// `noetl_worker_shutdown_hosts_sealed` is below its
/// `noetl_worker_shutdown_hosts_total` — or which never wrote them at all — left
/// an unsealed tail, and the next incarnation's resume over that log will clamp.
pub fn record_shutdown_seal(sealed: usize, hosts: usize) {
    let m = WorkerMetrics::global();
    m.shutdown_hosts_sealed.set(sealed as i64);
    m.shutdown_hosts_total.set(hosts as i64);
}

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

/// One platform-automatic sink observation (noetl/ai-meta#199 Slice C). `outcome`
/// is `candidate`, `skipped_explicit`, or `observed_only`. The first slice is
/// observe-only — it never writes to any store.
pub fn record_autosink(outcome: &str) {
    WorkerMetrics::global()
        .autosink_total
        .with_label_values(&[outcome])
        .inc();
}

/// A chain was flagged as holding un-sunk business context (noetl/ai-meta#198).
pub fn record_sink_gate_marked() {
    WorkerMetrics::global()
        .sink_gate_events_total
        .with_label_values(&["marked"])
        .inc();
}

/// A chain's context was confirmed sunk to the customer store and dropped.
pub fn record_sink_gate_confirmed() {
    WorkerMetrics::global()
        .sink_gate_events_total
        .with_label_values(&["confirmed"])
        .inc();
}

/// An eviction was skipped because the chain's business context is not yet sunk.
pub fn record_sink_gate_retained() {
    WorkerMetrics::global()
        .sink_gate_events_total
        .with_label_values(&["retained"])
        .inc();
}

/// The connector-step wiring emitted a sink signal (noetl/ai-meta#199 Slice A):
/// `signal` is `mark` (a declared `sink: true` step dispatched) or `confirm`
/// (that step succeeded, so its execution's business context is sunk to the
/// customer store). Fires regardless of whether the eviction gate is enabled, so
/// the wiring is observable before an operator opts in.
pub fn record_sink_state_post(action: &str, outcome: &str) {
    WorkerMetrics::global()
        .sink_state_post_total
        .with_label_values(&[action, outcome])
        .inc();
}

/// The connector-step wiring emitted a sink signal — see the counter docs.
pub fn record_sink_signal(tool_kind: &str, signal: &str) {
    WorkerMetrics::global()
        .sink_signal_total
        .with_label_values(&[tool_kind, signal])
        .inc();
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
    WorkerMetrics::global()
        .state_equivalence_mismatch_total
        .inc();
}

/// Record one off-server state build outcome (`cache_hit` | `incremental` |
/// `cold_rebuild` | `incomplete`).
pub fn record_state_builder_build(outcome: &str) {
    WorkerMetrics::global()
        .state_builder_builds_total
        .with_label_values(&[outcome])
        .inc();
}

/// Record the wall time of one off-server state build, under the same outcome
/// label as [`record_state_builder_build`].
pub fn record_state_builder_build_duration(outcome: &str, secs: f64) {
    WorkerMetrics::global()
        .state_builder_build_duration_seconds
        .with_label_values(&[outcome])
        .observe(secs);
}

/// Record the chain-walk depth of one off-server cold rebuild.
pub fn record_state_builder_chain_hops(hops: usize) {
    WorkerMetrics::global()
        .state_builder_chain_hops
        .observe(hops as f64);
}

/// Every `reason` the EHDB claim path reconnects for, taken from its call sites
/// in `command_bus.rs`.
///
/// `connect_failed` is the coordinator being unreachable when a fresh claim
/// connection is opened; `claim_next_failed` is an established connection
/// dying mid-read, which is the case noetl/ai-meta#208 could not detect at all
/// before keepalive + heartbeat landed.
pub const EHDB_CLAIM_RECONNECT_REASONS: [&str; 2] = ["connect_failed", "claim_next_failed"];

/// The two claim feeds a worker holds.
///
/// Both reconnect identically and both were log-only, but conflating them
/// would hide WHICH feed is flapping — and they fail for different reasons:
/// the command feed stalls dispatch, the events feed stalls the materializer.
pub const EHDB_CLAIM_FEEDS: [&str; 2] = ["commands", "events"];

/// Record one EHDB claim-coordinator reconnect.
pub fn record_ehdb_claim_reconnect(feed: &str, reason: &str) {
    WorkerMetrics::global()
        .ehdb_claim_reconnect_total
        .with_label_values(&[feed, reason])
        .inc();
}

/// Every `outcome` the off-server DRIVE build records, taken from the call
/// sites in `executor/command.rs` rather than from prose.
///
/// The doc here previously listed three (`served`, `fallback_incomplete`,
/// `fallback_disabled`) while the code recorded seven — including
/// `served_shard_mismatch`, which is the interesting one.  Pinning from that
/// comment would have left four outcomes permanently unreadable, so
/// `drive_outcome_literals_are_all_pinned` checks this list against the source.
pub const STATE_BUILDER_DRIVE_OUTCOMES: [&str; 7] = [
    "served",
    "served_rehydrated",
    "served_shard",
    "served_shard_mismatch",
    "stateless_retry",
    "fallback_incomplete",
    "fallback_disabled",
];

/// Every `outcome` the DRIVE build-retry wait records.
pub const STATE_BUILDER_DRIVE_WAIT_OUTCOMES: [&str; 2] = ["woken", "timeout"];

/// Every `outcome` the state-builder build records.
pub const STATE_BUILDER_BUILD_OUTCOMES: [&str; 4] =
    ["cache_hit", "incremental", "cold_rebuild", "incomplete"];

/// Record one off-server DRIVE build outcome.  See
/// [`STATE_BUILDER_DRIVE_OUTCOMES`] for the full set.
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

/// Record a MAIN command-loop in-process NATS reconnect (noetl/ai-meta#163).
/// `reason` is `pull` / `ack` / `nack` (a hard disconnect surfaced on that op and
/// the subscriber was rebuilt in-process) or `connect_error` (a rebuild attempt
/// itself failed and is being retried with backoff).
pub fn record_command_loop_reconnect(reason: &str) {
    WorkerMetrics::global()
        .command_loop_reconnect_total
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
    WorkerMetrics::global()
        .state_materializer_open_shards
        .set(n);
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

    /// noetl/ai-meta#238 — an emission abandoned after every retry must be
    /// COUNTABLE.  `event_emit_retries_total` counts retries, which rise
    /// whenever the control plane is flaky; only this counter distinguishes
    /// "retried and eventually succeeded" from "gave up and the event is gone".
    #[test]
    fn abandoned_event_emissions_are_counted_by_type() {
        record_event_emit_failed("test.emit_failed.alpha");
        record_event_emit_failed("test.emit_failed.alpha");
        record_event_emit_failed("test.emit_failed.beta");

        let text = String::from_utf8(WorkerMetrics::global().encode()).unwrap();
        // Scoped to this metric's own lines so the assertion cannot pass on
        // another metric that happens to carry an event_type label.
        let lines: Vec<&str> = text
            .lines()
            .filter(|l| l.starts_with("noetl_worker_event_emit_failed_total{"))
            .collect();
        assert!(
            lines.iter().any(|l| l.contains("test.emit_failed.alpha") && l.trim_end().ends_with(" 2")),
            "alpha must show 2 abandonments; got {lines:?}"
        );
        assert!(
            lines.iter().any(|l| l.contains("test.emit_failed.beta") && l.trim_end().ends_with(" 1")),
            "beta must show 1 abandonment; got {lines:?}"
        );
    }

    /// Every `break` out of the cold-rebuild replay loop must record a reason.
    ///
    /// The point of the metric is that four conditions leave that loop
    /// identically and only `feed_error` means the state is incomplete.  An
    /// uninstrumented exit does not merely lose a count — it makes the ratio
    /// wrong, so `feed_error` looks rarer than it is.
    #[test]
    fn replay_end_covers_every_loop_exit() {
        let src = include_str!("state_builder.rs");
        let recorded = src.matches("record_state_builder_replay_end(").count();
        assert_eq!(
            recorded, 5,
            "all five replay-loop exits must record; found {recorded}"
        );
        let mut seen: Vec<&str> = Vec::new();
        let call = "record_state_builder_replay_end(";
        let mut rest = src;
        while let Some(i) = rest.find(call) {
            rest = &rest[i + call.len()..];
            if let Some(q1) = rest.find('"') {
                let after = &rest[q1 + 1..];
                if let Some(q2) = after.find('"') {
                    let lit = &after[..q2];
                    if !seen.contains(&lit) {
                        seen.push(lit);
                    }
                }
            }
        }
        for lit in &seen {
            assert!(
                STATE_BUILDER_REPLAY_END_REASONS.contains(lit),
                "{lit:?} is recorded but not pinned"
            );
        }
        assert!(
            seen.contains(&"feed_error"),
            "the incomplete-state reason must be instrumented; got {seen:?}"
        );
        let text = String::from_utf8(WorkerMetrics::global().encode()).unwrap();
        for reason in STATE_BUILDER_REPLAY_END_REASONS {
            assert!(
                text.lines().any(|l| l
                    .starts_with("noetl_worker_state_builder_replay_end_total{")
                    && l.contains(&format!("reason=\"{reason}\""))),
                "{reason} must be pinned at 0"
            );
        }
    }

    /// Every materializer ack/drain failure path must record, and all series be
    /// readable at 0.
    ///
    /// `non_event_batch` is the one that matters most: the code's own comment
    /// says that without that ack the batch "poison-loops forever", so a
    /// sustained rate there is a stalled materializer — and under the
    /// publish-only gate a stalled materializer is a durable log that stops
    /// being written.
    #[test]
    fn materializer_ack_and_drain_failures_are_counted() {
        let src = include_str!("materializer.rs");
        for (needle, stage) in [
            ("materializer ack failed on a non-event batch", "non_event_batch"),
            ("materializer ack failed after a durable project", "after_project"),
            ("materializer ack reported per-handle errors", "per_handle"),
        ] {
            assert!(src.contains(needle), "the {stage} warning should still exist");
            assert!(
                src.contains(&format!("record_materializer_ack_failed(\"{stage}\")")),
                "{stage} must be recorded"
            );
        }
        assert!(
            src.contains("record_materializer_drain_failed()"),
            "a failed drain poll must be recorded"
        );

        let text = String::from_utf8(WorkerMetrics::global().encode()).unwrap();
        for stage in MATERIALIZER_ACK_STAGES {
            assert!(
                text.lines().any(|l| l
                    .starts_with("noetl_worker_materializer_ack_failed_total{")
                    && l.contains(&format!("stage=\"{stage}\""))),
                "{stage} must be pinned at 0"
            );
        }
        assert!(
            text.lines()
                .any(|l| l.starts_with("noetl_worker_materializer_drain_failed_total ")),
            "the unlabelled drain counter must be present at 0"
        );
    }

    /// Both skip sites in the materializer must record, and the counter must be
    /// present at 0 without any activity.
    ///
    /// Under the publish-only gate the materializer is the sole writer of
    /// `noetl.event`, so a skipped message is an event that never reaches the
    /// durable log — and the batch is acked anyway.  Instrumenting only one of
    /// the two sites would halve a loss signal while looking instrumented.
    #[test]
    fn materializer_skipped_is_counted_at_every_skip_site() {
        let src = include_str!("materializer.rs");
        let warn_sites = src.matches("materializer skipped messages with no event_id").count();
        let recorded = src.matches("record_materializer_skipped(").count();
        assert!(warn_sites > 0, "the skip warning should still exist");
        assert_eq!(
            recorded, warn_sites,
            "every skip site must record: {warn_sites} warning(s), {recorded} recorder call(s)"
        );

        let text = String::from_utf8(WorkerMetrics::global().encode()).unwrap();
        assert!(
            text.lines()
                .any(|l| l.starts_with("noetl_worker_materializer_skipped_total ")),
            "an unlabelled counter must be present at 0 with no activity"
        );
        // Name collision guard: the RESULT materializer has its own skipped
        // counter, and the two differ by one word.
        assert!(
            text.contains("noetl_worker_result_materializer_skipped_total"),
            "the result-materializer counter must remain distinct and present"
        );
    }

    /// Both reconnect reasons must be pinned and readable at 0.
    ///
    /// This path is why the metric exists: it retried with a log line and
    /// nothing else, so a production pod emitting 85 of them in 24h was
    /// invisible to monitoring.  A reason that is recorded but unpinned is
    /// absent until it first fires, which on a rare failure path may be never.
    #[test]
    fn ehdb_claim_reconnect_covers_both_feeds() {
        // Both feeds reconnect identically and both were log-only.  Asserting
        // per-feed site counts means instrumenting one feed and not the other
        // fails here, rather than leaving half the signal missing while the
        // metric looks present.
        for (file, feed, want) in [
            (include_str!("command_bus.rs"), "commands", 2usize),
            (include_str!("event_bus.rs"), "events", 2usize),
        ] {
            let n = file
                .matches(&format!("record_ehdb_claim_reconnect(\"{feed}\""))
                .count();
            assert_eq!(n, want, "{feed} feed must record at {want} site(s); found {n}");
        }
        let text = String::from_utf8(WorkerMetrics::global().encode()).unwrap();
        for feed in EHDB_CLAIM_FEEDS {
            for reason in EHDB_CLAIM_RECONNECT_REASONS {
                assert!(
                    text.lines().any(|l| l
                        .starts_with("noetl_worker_ehdb_claim_reconnect_total{")
                        && l.contains(&format!("feed=\"{feed}\""))
                        && l.contains(&format!("reason=\"{reason}\""))),
                    "{feed}/{reason} must be pinned at 0"
                );
            }
        }
    }

    /// Every outcome literal passed at a call site must be pinned.
    ///
    /// The source is embedded with `include_str!`, so this reads the real call
    /// sites at compile time rather than trusting a doc comment — which is the
    /// specific thing that failed here: the recorder's doc listed three
    /// outcomes while the code recorded seven.  Add a literal without adding it
    /// to the const and this test fails, instead of that outcome being absent
    /// from /metrics until it first occurs.
    #[test]
    fn drive_outcome_literals_are_all_pinned() {
        fn literals<'a>(src: &'a str, call: &str) -> Vec<&'a str> {
            let mut out = Vec::new();
            let needle = format!("{call}(\"");
            let mut rest = src;
            while let Some(i) = rest.find(&needle) {
                rest = &rest[i + needle.len()..];
                if let Some(end) = rest.find('"') {
                    out.push(&rest[..end]);
                }
            }
            out
        }

        let command_rs = include_str!("executor/command.rs");
        let state_builder_rs = include_str!("state_builder.rs");

        for (src, call, pinned) in [
            (
                command_rs,
                "record_state_builder_drive",
                &STATE_BUILDER_DRIVE_OUTCOMES[..],
            ),
            (
                command_rs,
                "record_state_builder_drive_wait",
                &STATE_BUILDER_DRIVE_WAIT_OUTCOMES[..],
            ),
            (
                state_builder_rs,
                "record_state_builder_build",
                &STATE_BUILDER_BUILD_OUTCOMES[..],
            ),
        ] {
            let found = literals(src, call);
            assert!(
                !found.is_empty(),
                "{call}: found no literals — the extraction broke, which would make this \
                 test pass vacuously"
            );
            for lit in &found {
                assert!(
                    pinned.contains(lit),
                    "{call}(\"{lit}\") is recorded but not pinned; add it to the const"
                );
            }
        }
    }

    /// The gauge exists to be readable when every other metric is absent, so it
    /// must be present with no prior activity — and `event_emit_failed_total`
    /// above is exactly the case it covers: a free-form `event_type` label that
    /// cannot be pinned, leaving the metric invisible until something fails.
    #[test]
    fn build_info_publishes_the_crate_version() {
        let text = String::from_utf8(WorkerMetrics::global().encode()).unwrap();
        let line = text
            .lines()
            .find(|l| l.starts_with("noetl_worker_build_info{"))
            .expect("build_info series must exist without any prior activity");
        assert!(
            line.contains(&format!("version=\"{}\"", env!("CARGO_PKG_VERSION"))),
            "build_info must carry the crate version; got {line:?}"
        );
        assert!(
            line.trim_end().ends_with(" 1"),
            "build_info must read 1; got {line:?}"
        );
    }
}

#[cfg(test)]
mod state_build_latency_tests {
    use super::*;

    /// noetl/ai-meta#156 acceptance item 1: per-hop build latency must be
    /// observable **labelled by cache outcome**. An aggregate quantile over all
    /// outcomes is useless here — cache hits are sub-millisecond and dominate by
    /// count, so they bury the cold-rebuild tail that is the actual floor.
    #[test]
    fn build_duration_is_recorded_per_outcome() {
        for (o, secs) in [("cache_hit", 0.0004), ("incremental", 0.02), ("cold_rebuild", 3.5)] {
            record_state_builder_build_duration(o, secs);
        }
        let out = String::from_utf8(WorkerMetrics::global().encode()).unwrap();
        assert!(
            out.contains("noetl_worker_state_builder_build_duration_seconds"),
            "the histogram must be exported"
        );
        for o in ["cache_hit", "incremental", "cold_rebuild"] {
            assert!(
                out.contains(&format!("outcome=\"{o}\"")),
                "outcome {o} must appear as a label"
            );
        }
    }

    /// The bucket range must actually span the observed values, or the tail is
    /// recorded as +Inf and the number that matters is unreadable. #156 reports
    /// a kind floor around 265ms and prod hops in the seconds.
    #[test]
    fn buckets_span_sub_millisecond_to_seconds() {
        record_state_builder_build_duration("cold_rebuild", 4.0);
        record_state_builder_build_duration("cache_hit", 0.0006);
        let out = String::from_utf8(WorkerMetrics::global().encode()).unwrap();
        // Scope the assertion to THIS metric's own bucket lines. Asserting on a
        // bare `le="0.001"` passes on any histogram in the registry, so it
        // cannot fail — the first version of this test did exactly that and
        // stayed green when the metric was renamed away.
        let mine: Vec<&str> = out
            .lines()
            .filter(|l| l.starts_with("noetl_worker_state_builder_build_duration_seconds_bucket"))
            .collect();
        assert!(!mine.is_empty(), "the histogram must export bucket lines");
        assert!(
            mine.iter().any(|l| l.contains("le=\"0.001\"")),
            "need a sub-millisecond bucket: a cache hit must not land in the first bucket with everything else"
        );
        assert!(
            mine.iter().any(|l| l.contains("le=\"5\"") || l.contains("le=\"5.0\"")),
            "need a multi-second bucket: a cold rebuild must not collapse into +Inf"
        );
    }
}
