//! Off-server orchestrator state builder — Phase 4 of the decoupled-context /
//! event-chain RFC ([noetl/ai-meta#115](https://github.com/noetl/ai-meta/issues/115)).
//!
//! ## What this is
//!
//! Phase 3 ([server#245], server-side, flagged) moved orchestrator
//! `WorkflowState` reconstruction off the `noetl.event` table scan and onto a
//! **chain walk**: follow the one-level `prev_event_id` link (Phase 2) from the
//! per-execution chain head back to the genesis event, each hop a PK lookup, then
//! feed the collected events to `WorkflowState::from_events`. It still runs *in
//! the server*.
//!
//! Phase 4 moves that construction **off the server onto the system worker
//! pool**. This module is the pool-side kernel:
//!
//! - [`WalEventIndex`] — a per-execution index of events **sourced from the
//!   `noetl_events` JetStream WAL** (not from the materialized `noetl.event`
//!   table), each carrying its `prev_event_id`. Fed by [`WalEventIndex::apply`]
//!   from the WAL drain loop; **never** issues a `SELECT`/scan against
//!   `noetl.event` (RFC tenet 3).
//! - [`ExecutionChain::chain_walk`] — walks the index head→root by
//!   `prev_event_id` and returns the events in `event_id` order (the same order
//!   the server's event-scan applies them), so the resulting state is equivalent
//!   to the server chain-walk / event-scan build (parity by construction — both
//!   feed the SAME `from_events`).
//! - [`ExecutionChain`] cache — the built artefact (the ordered event spine)
//!   keyed by the **immutable chain head** (`(execution_id, head_event_id)`).
//!   Because the chain is append-only and immutable, a cached spine for a given
//!   head is valid forever (RFC §5.2: no staleness, no consistency `COUNT(*)`);
//!   the next trigger **advances only the new tail** ([`AdvanceOutcome`]) instead
//!   of re-walking the whole chain, and a cold miss rebuilds deterministically by
//!   re-walking from the durable head ([`ExecutionChain::cold_rebuild`]).
//!
//! ## What this module deliberately does NOT do yet
//!
//! Wiring this builder into the off-server drive — so the drive obtains its state
//! from here instead of the server building it in-process — is the staged drive
//! cutover (behind `NOETL_STATE_BUILDER=offserver`). The live WAL drain loop
//! ([`spawn`]) runs in **shadow / observation mode** under
//! `NOETL_STATE_BUILDER_SHADOW` (default off): it proves on the running cluster
//! that the builder reads the WAL with zero `noetl.event` scans and that the
//! chain-walk + pool-side cache (hit / incremental tail-advance / cold-rebuild)
//! behave, without touching the drive decision. Default off → every other worker
//! is unaffected; PROD runtime is unchanged.
//!
//! [server#245]: https://github.com/noetl/server/pull/245

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use async_nats::jetstream::{
    self,
    consumer::{pull::Config as PullConfig, AckPolicy, DeliverPolicy},
};
use async_nats::ConnectOptions;
use futures::StreamExt;

/// The genesis event type — the chain root for a complete chain. `playbook_started`
/// is the first event `execute` emits (before any `command.issued`); reaching it
/// means the walk covered the whole execution, not just a post-restart tail. Mirror
/// of the server chain-walk's `chain_has_genesis` guard (server#245).
const GENESIS_EVENT_TYPE: &str = "playbook_started";

/// One indexed event: the chain link + the event type (for the genesis guard) +
/// the raw `noetl_events` payload (the input a `from_events` build consumes).
#[derive(Debug, Clone)]
struct IndexedEvent {
    /// The immediately-previous event in this execution's causal order
    /// (`None` at the chain root). The link the walk follows.
    prev_event_id: Option<i64>,
    event_type: String,
    /// The full event JSON as published to `noetl_events` — kept so a chain walk
    /// can hand the ordered spine to a `from_events` build verbatim.
    raw: serde_json::Value,
}

/// Outcome of advancing one execution's cached spine to the current head — the
/// signal the no-`COUNT` / no-rescan property is observable through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdvanceOutcome {
    /// The cached head already equals the current head — nothing to do (the
    /// cheapest path, the steady state between a drive and its next event).
    CacheHit,
    /// The current head extends the cached head along the chain; only the new
    /// tail was walked and appended. Carries the number of events added.
    Incremental(usize),
    /// No usable cache (cold start, restart, or a pointer-continuity gap that
    /// can't be repaired by a tail walk) — rebuilt the whole spine from the head.
    /// Carries the rebuilt length.
    ColdRebuild(usize),
    /// The chain can't be trusted complete (a `prev_event_id` points at an event
    /// not yet in the index — WAL/materializer ordering — or the walk didn't
    /// reach the genesis). The real builder falls back to the server here; the
    /// shadow records it and retries on the next event.
    Incomplete,
}

/// The built spine cached for one execution, keyed by the immutable chain head.
#[derive(Debug, Clone)]
struct CachedSpine {
    /// The chain head this spine summarizes — the cache key. Immutable: a spine
    /// for a given head is valid forever (append-only chain).
    head_event_id: i64,
    /// The event ids on the spine, ascending (`event_id` order — the order the
    /// server event-scan applies, so `from_events` sees the identical sequence).
    ordered_ids: Vec<i64>,
}

/// Per-execution chain index + cached spine. Pure: no I/O, no clock, no DB — a
/// deterministic function of the events applied to it, which makes the parity,
/// incremental-equals-full, and cold-rebuild properties unit-testable.
#[derive(Debug, Default)]
pub struct ExecutionChain {
    events: HashMap<i64, IndexedEvent>,
    /// The chain head = the latest event appended. Tracked as the max `event_id`
    /// seen: along a single execution's chain the ids are monotonic (snowflake
    /// from the one server replica that owns the execution), so the max id is the
    /// tip of the linear spine the server's `ChainHeads` watermark advances.
    head: Option<i64>,
    cache: Option<CachedSpine>,
}

impl ExecutionChain {
    /// Index one WAL event. Idempotent: re-applying the same `event_id` (a
    /// JetStream redelivery) overwrites with identical data and never double-counts
    /// the head. Returns `true` if this event was new to the index.
    pub fn apply(&mut self, event_id: i64, prev_event_id: Option<i64>, event_type: String, raw: serde_json::Value) -> bool {
        let is_new = !self.events.contains_key(&event_id);
        self.events.insert(
            event_id,
            IndexedEvent { prev_event_id, event_type, raw },
        );
        // Advance the head monotonically — the chain tip is the max id seen.
        if self.head.is_none_or(|h| event_id > h) {
            self.head = Some(event_id);
        }
        is_new
    }

    /// Current chain head (max applied `event_id`), if any.
    pub fn head(&self) -> Option<i64> {
        self.head
    }

    /// The event type of an indexed event, if present.  Used by the stateless
    /// off-server drive (RFC #115 Phase 4 remainder) to resolve
    /// `trigger_event_type` off the WAL from the server-supplied
    /// `trigger_event_id` — so the server need not read `noetl.event` to classify
    /// the trigger.  Returns `None` when the id isn't indexed (the caller defaults
    /// to `command.completed`, the only triggering type).
    pub fn event_type_of(&self, event_id: i64) -> Option<&str> {
        self.events.get(&event_id).map(|e| e.event_type.as_str())
    }

    /// Number of events indexed for this execution.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Walk the chain head→root by `prev_event_id`, returning the spine in
    /// ascending `event_id` order — the same order the server event-scan applies
    /// (`ORDER BY event_id ASC`). Returns `None` when the chain can't be trusted
    /// complete: a hop points at an event not present in the index (WAL ordering /
    /// gap), the walk didn't reach the genesis `playbook_started`, or it's empty.
    /// This is the exact completeness contract the server chain-walk falls back
    /// on (server#245); the off-server builder falls back to the server build the
    /// same way.
    pub fn chain_walk(&self) -> Option<Vec<i64>> {
        let head = self.head?;
        let mut ordered: Vec<i64> = Vec::new();
        let mut cursor = Some(head);
        let mut reached_genesis = false;
        // Bound the walk so a corrupt cycle can't spin (real chains are at most a
        // few thousand events; mirror of the server builder's MAX_WALK guard).
        let mut guard = 0usize;
        const MAX_WALK: usize = 5_000_000;
        while let Some(eid) = cursor {
            guard += 1;
            if guard > MAX_WALK {
                return None;
            }
            let node = self.events.get(&eid)?; // missing hop → incomplete → fall back
            if node.event_type == GENESIS_EVENT_TYPE {
                reached_genesis = true;
            }
            ordered.push(eid);
            cursor = node.prev_event_id;
        }
        if ordered.is_empty() || !reached_genesis {
            return None;
        }
        ordered.sort_unstable();
        Some(ordered)
    }

    /// The ordered event spine as the raw `noetl_events` payloads — the verbatim
    /// input a `from_events` build (server-side or wasm) consumes. `None` under
    /// the same incompleteness conditions as [`Self::chain_walk`].
    pub fn ordered_events(&self) -> Option<Vec<serde_json::Value>> {
        let ids = self.chain_walk()?;
        Some(ids.iter().map(|id| self.events[id].raw.clone()).collect())
    }

    /// Advance the cached spine to the current head, doing the **minimum** work:
    /// a no-op on an unchanged head, a **tail-only** walk when the head extended
    /// (pointer-continuity verified against the cached head — no `COUNT(*)`), or a
    /// full cold rebuild when there's no usable cache or the tail can't reach the
    /// cached head. The advanced spine equals a full rebuild from the same head
    /// (proven in the unit tests).
    pub fn advance(&mut self) -> AdvanceOutcome {
        let Some(head) = self.head else {
            return AdvanceOutcome::Incomplete;
        };
        // Cached head unchanged → hit.
        if let Some(c) = &self.cache {
            if c.head_event_id == head {
                return AdvanceOutcome::CacheHit;
            }
        }
        // Try an incremental tail-advance from the cached head.
        if let Some(c) = &self.cache {
            if head > c.head_event_id {
                if let Some(tail) = self.walk_tail_to(head, c.head_event_id) {
                    let added = tail.len();
                    let mut ordered = c.ordered_ids.clone();
                    ordered.extend(tail);
                    ordered.sort_unstable();
                    self.cache = Some(CachedSpine { head_event_id: head, ordered_ids: ordered });
                    return AdvanceOutcome::Incremental(added);
                }
            }
        }
        // No cache / non-extending head / tail gap → cold rebuild.
        match self.chain_walk() {
            Some(ordered) => {
                let len = ordered.len();
                self.cache = Some(CachedSpine { head_event_id: head, ordered_ids: ordered });
                AdvanceOutcome::ColdRebuild(len)
            }
            None => AdvanceOutcome::Incomplete,
        }
    }

    /// Walk from `head` back to (but not including) `stop_at`, returning the new
    /// tail ids. `None` when the walk hits a missing hop before reaching
    /// `stop_at` (a gap the tail can't bridge → caller cold-rebuilds). This is the
    /// **pointer-continuity** check that replaces #101's O(events) `COUNT(*)`
    /// staleness probe (RFC §5.2).
    fn walk_tail_to(&self, head: i64, stop_at: i64) -> Option<Vec<i64>> {
        let mut tail = Vec::new();
        let mut cursor = Some(head);
        let mut guard = 0usize;
        const MAX_WALK: usize = 5_000_000;
        while let Some(eid) = cursor {
            if eid == stop_at {
                return Some(tail); // reached the cached head → continuous
            }
            guard += 1;
            if guard > MAX_WALK {
                return None;
            }
            let node = self.events.get(&eid)?;
            tail.push(eid);
            cursor = node.prev_event_id;
        }
        // Walked to the root without meeting the cached head → not an extension.
        None
    }

    /// Force a cold rebuild of the cached spine from the current head, discarding
    /// any cached state — the crash-recovery / restart path (RFC §7.3). Equivalent
    /// to the steady-state advance after an eviction.
    pub fn cold_rebuild(&mut self) -> AdvanceOutcome {
        self.cache = None;
        self.advance()
    }

    /// The cached spine length, if a spine is cached.
    pub fn cached_len(&self) -> Option<usize> {
        self.cache.as_ref().map(|c| c.ordered_ids.len())
    }

    /// The cached head, if a spine is cached.
    pub fn cached_head(&self) -> Option<i64> {
        self.cache.as_ref().map(|c| c.head_event_id)
    }
}

/// Pool-side index of all in-flight executions' chains. Holds one
/// [`ExecutionChain`] per `execution_id`; terminal executions are evicted to
/// bound memory (RFC §5.2 — eviction, never staleness invalidation).
#[derive(Debug, Default)]
pub struct WalEventIndex {
    chains: HashMap<i64, ExecutionChain>,
}

/// Event types that put an execution into a terminal state — the eviction signal
/// (mirror of the server's terminal-eviction set). Underscore forms are the
/// emitted shapes (`playbook_completed`, not `playbook.completed`).
const TERMINAL_EVENT_TYPES: &[&str] = &[
    "playbook_completed",
    "playbook_failed",
    "playbook_cancelled",
];

impl WalEventIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Index one WAL event payload (the `noetl_events` shape). Extracts the chain
    /// fields and routes them to the owning execution's [`ExecutionChain`].
    /// Returns the `(execution_id, is_new, is_terminal)` triple, or `None` when the
    /// payload isn't a chainable event (no `event_id`/`execution_id`).
    pub fn apply(&mut self, payload: &serde_json::Value) -> Option<(i64, bool, bool)> {
        let obj = payload.as_object()?;
        let event_id = obj.get("event_id").and_then(|v| v.as_i64())?;
        let execution_id = obj.get("execution_id").and_then(|v| v.as_i64())?;
        let prev_event_id = obj.get("prev_event_id").and_then(|v| v.as_i64());
        let event_type = obj
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let is_terminal = TERMINAL_EVENT_TYPES.contains(&event_type.as_str());
        let chain = self.chains.entry(execution_id).or_default();
        let is_new = chain.apply(event_id, prev_event_id, event_type, payload.clone());
        Some((execution_id, is_new, is_terminal))
    }

    /// Borrow an execution's chain (for advance / walk).
    pub fn chain_mut(&mut self, execution_id: i64) -> Option<&mut ExecutionChain> {
        self.chains.get_mut(&execution_id)
    }

    pub fn chain(&self, execution_id: i64) -> Option<&ExecutionChain> {
        self.chains.get(&execution_id)
    }

    /// Drop a terminal execution's chain — frees memory. Mirrors the server's
    /// orch-cache + chain-head eviction on a terminal event.
    pub fn evict(&mut self, execution_id: i64) {
        self.chains.remove(&execution_id);
    }

    /// Number of executions currently indexed.
    pub fn execution_count(&self) -> usize {
        self.chains.len()
    }

    /// Advance an execution's cached spine (exercising the cache: hit /
    /// incremental / cold-rebuild) and return the **ordered event spine** as the
    /// raw `noetl_events` payloads — the verbatim `OrchestrateInput.events` the
    /// `system/orchestrate` plug-in's `run` (from_events) entry consumes — when
    /// the chain is complete (genesis-rooted, no gap).  `None` when the chain
    /// can't be trusted complete (cold / WAL ordering gap / non-genesis tail) —
    /// the off-server drive then falls back to the server-built state.  Returns
    /// the [`AdvanceOutcome`] alongside so the caller can record the cache metric
    /// even on an incomplete read.
    pub fn build_spine(&mut self, execution_id: i64) -> (AdvanceOutcome, Option<Vec<serde_json::Value>>) {
        match self.chains.get_mut(&execution_id) {
            Some(chain) => {
                let outcome = chain.advance();
                (outcome, chain.ordered_events())
            }
            // No chain indexed for this execution yet (the WAL drain hasn't seen
            // any of its events) — incomplete, fall back.
            None => (AdvanceOutcome::Incomplete, None),
        }
    }
}

// ── Off-server drive build (RFC #115 Phase 4 drive cutover) ──────────────────
//
// The shared, pool-side WAL index is fed by the drain loop ([`spawn_drain`]) and
// read by the worker's `system/orchestrate` command dispatch
// (`executor::command::dispatch_wasm`) when the server marks the command
// `__offserver_build__`.  Under `NOETL_STATE_BUILDER=offserver` the drive
// obtains its `WorkflowState` from HERE (the WAL spine fed to the wasm `run` /
// `from_events` entry) instead of the server-built `run_state` payload — so
// state CONSTRUCTION runs on the pool, off the server, with zero `noetl.event`
// reads (the spine comes from the `noetl_events` WAL).

/// The shared, pool-side WAL index: written by the drain loop, read by the
/// command-dispatch off-server-build path.  A `tokio::Mutex` because both sides
/// are async tasks in the same worker process; critical sections are short
/// (a per-batch apply, or a single chain advance + spine read).
pub type SharedWalIndex = std::sync::Arc<tokio::sync::Mutex<WalEventIndex>>;

/// Where the worker's state builder operates — resolved from env at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuilderMode {
    /// Disabled (default) — no drain loop, no off-server build.  The drive uses
    /// the server-built `run_state` payload exactly as today.
    Off,
    /// Observation-only WAL drain (`NOETL_STATE_BUILDER_SHADOW`) — proves the
    /// chain index + cache mechanics on the running cluster, never touches the
    /// drive.
    Shadow,
    /// Authoritative (`NOETL_STATE_BUILDER=offserver`) — the drain feeds a shared
    /// index the orchestrate-command dispatch reads to build the drive state off
    /// the WAL spine.  A complete spine drives the decision; an incomplete one
    /// falls back to the server-built state carried on the same command, so
    /// progress + correctness never regress below the server-built path.
    Authoritative,
}

/// Resolve the builder mode from env.  `NOETL_STATE_BUILDER=offserver` →
/// authoritative (takes precedence); else `NOETL_STATE_BUILDER_SHADOW` truthy →
/// shadow; else off.  Default off — prod/default unchanged.
pub fn builder_mode() -> BuilderMode {
    let sb = std::env::var("NOETL_STATE_BUILDER")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if sb == "offserver" {
        return BuilderMode::Authoritative;
    }
    if shadow_enabled() {
        return BuilderMode::Shadow;
    }
    BuilderMode::Off
}

/// Build the off-server drive input from the shared WAL index, for one trigger.
/// Advances the cached spine (recording the cache outcome) and, when the chain
/// is complete, returns the JSON `OrchestrateInput { events, playbook,
/// trigger_event_type }` bytes for the wasm `run` (from_events) entry.  Returns
/// `None` when the chain is incomplete (the caller falls back to the
/// server-built `run_state` state).  Records the drive-build outcome metric.
pub async fn build_offserver_input(
    index: &SharedWalIndex,
    execution_id: i64,
    playbook: &serde_json::Value,
    trigger_event_type: Option<&str>,
    trigger_event_id: Option<i64>,
    expected_head: Option<i64>,
) -> Option<Vec<u8>> {
    let (outcome, spine, head, resolved_trigger_type) = {
        let mut idx = index.lock().await;
        let (outcome, spine) = idx.build_spine(execution_id);
        let chain = idx.chain(execution_id);
        let head = chain.and_then(|c| c.head());
        // Stateless off-server drive (RFC #115 Phase 4 remainder): when the
        // server did NOT supply `trigger_event_type` (it no longer reads
        // `noetl.event` to classify the trigger), resolve it off the WAL index
        // from the server-supplied `trigger_event_id`.  Falls back to
        // `command.completed` (the only triggering type) if the id isn't indexed.
        let resolved_trigger_type = trigger_event_type.map(|s| s.to_string()).or_else(|| {
            trigger_event_id
                .and_then(|tid| chain.and_then(|c| c.event_type_of(tid)).map(|s| s.to_string()))
        });
        (outcome, spine, head, resolved_trigger_type)
    };
    // Staleness guard (RFC #115 Phase 4): the worker's WAL drain is an
    // independent consumer that can lag the server's view.  A spine that is
    // internally complete (genesis-rooted, no gaps) can still be STALE — missing
    // the most recent events the server already saw (e.g. a fan-in barrier's
    // just-issued reduce `command.issued`), which would make the drive RE-ISSUE
    // it.  So serve only once the index has caught up to the server's dispatch
    // watermark (`expected_head`); until then report incomplete so the bounded
    // retry waits for the drain (or falls back to the server-built state).  This
    // makes the WAL-built state never staler than the server-built one.
    if let Some(expected) = expected_head {
        if head.is_none_or(|h| h < expected) {
            return None;
        }
    }
    // Record the cache outcome (the same labels the shadow loop records) so the
    // authoritative path's hit/incremental/cold distribution is observable.
    match outcome {
        AdvanceOutcome::CacheHit => crate::metrics::record_state_builder_build("cache_hit"),
        AdvanceOutcome::Incremental(_) => crate::metrics::record_state_builder_build("incremental"),
        AdvanceOutcome::ColdRebuild(hops) => {
            crate::metrics::record_state_builder_build("cold_rebuild");
            crate::metrics::record_state_builder_chain_hops(hops);
        }
        AdvanceOutcome::Incomplete => crate::metrics::record_state_builder_build("incomplete"),
    }
    let events = spine?;
    let trigger_type = resolved_trigger_type
        .as_deref()
        .unwrap_or("command.completed");
    let input = serde_json::json!({
        "events": events,
        "playbook": playbook,
        "trigger_event_type": trigger_type,
    });
    serde_json::to_vec(&input).ok()
}

// ── Live WAL shadow drain loop ───────────────────────────────────────────────
//
// The shadow loop is the on-cluster proof of the Phase-4 mechanics: it consumes
// the `noetl_events` WAL into a pool-side [`WalEventIndex`] and, per touched
// execution, runs [`ExecutionChain::advance`] — exercising the chain walk + the
// cache (hit / incremental / cold-rebuild) and emitting metrics — WITHOUT
// touching the drive. It reads the WAL only (zero `noetl.event` scans). Default
// off (`NOETL_STATE_BUILDER_SHADOW`); the drive cutover that makes the drive
// consume this builder's state is staged behind `NOETL_STATE_BUILDER=offserver`.

/// The `noetl_events` stream the server publishes to (mirror of the server's
/// `EVENT_STREAM` / the materializer's).
pub const EVENT_STREAM: &str = "noetl_events";

/// True when `NOETL_STATE_BUILDER_SHADOW` is set to a truthy value — spawns the
/// observation-only WAL drain loop (system worker pool). Default off.
pub fn shadow_enabled() -> bool {
    matches!(
        std::env::var("NOETL_STATE_BUILDER_SHADOW")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Resolved drain-loop configuration (shadow OR authoritative — RFC #115
/// Phase 4).  The loop drains the `noetl_events` WAL into the shared
/// [`SharedWalIndex`]; the [`BuilderMode`] decides whether that index is
/// observation-only (shadow) or the authoritative source the off-server drive
/// reads (`offserver`).
pub struct DrainConfig {
    pub mode: BuilderMode,
    pub nats_url: String,
    pub nats_user: Option<String>,
    pub nats_password: Option<String>,
    pub stream: String,
    /// Durable consumer name for the authoritative drain — makes the
    /// state-builder a first-class system component whose backlog is observable
    /// (KEDA/VMAlert), mirroring the materializer's durable consumer.  `None`
    /// (shadow) → a fresh ephemeral DeliverAll consumer that cold-replays the
    /// retained stream on each start.
    pub durable: Option<String>,
    /// Bounded pull batch size + wait.
    pub batch: u32,
    pub timeout_ms: u64,
    pub idle_sleep: Duration,
}

/// The durable consumer name the authoritative state-builder uses on the
/// `noetl_events` stream — mirror of the materializer's `noetl_materializer`.
/// Override with `NOETL_STATE_BUILDER_CONSUMER`.
pub const STATE_BUILDER_CONSUMER: &str = "noetl_state_builder";

impl DrainConfig {
    /// Build from the worker config + env, or `None` when the builder is off
    /// (mode `Off`).  Authoritative (`NOETL_STATE_BUILDER=offserver`) takes
    /// precedence over shadow.
    pub fn from_env(nats_url: &str) -> Option<Self> {
        let mode = builder_mode();
        if mode == BuilderMode::Off {
            return None;
        }
        let (nats_url, nats_user, nats_password) = parse_nats_credentials(nats_url);
        // Authoritative: a durable consumer so the backlog is observable and the
        // cursor survives restarts (incremental tail-advance instead of a full
        // cold replay each boot).  Shadow: ephemeral, cold-replay on each start.
        let durable = if mode == BuilderMode::Authoritative {
            Some(
                std::env::var("NOETL_STATE_BUILDER_CONSUMER")
                    .unwrap_or_else(|_| STATE_BUILDER_CONSUMER.to_string()),
            )
        } else {
            None
        };
        Some(Self {
            mode,
            nats_url,
            nats_user,
            nats_password,
            stream: std::env::var("NOETL_STATE_BUILDER_STREAM")
                .unwrap_or_else(|_| EVENT_STREAM.to_string()),
            durable,
            batch: env_u32("NOETL_STATE_BUILDER_BATCH", 200).clamp(1, 1000),
            timeout_ms: env_u64("NOETL_STATE_BUILDER_TIMEOUT_MS", 2_000),
            idle_sleep: Duration::from_millis(env_u64("NOETL_STATE_BUILDER_IDLE_SLEEP_MS", 500)),
        })
    }
}

/// Spawn the drain loop over the shared index, returning the handle so the
/// worker can abort it on shutdown.  The drain writes into `index`; under
/// [`BuilderMode::Authoritative`] the command-dispatch path reads the same
/// `index` to build the off-server drive state.
pub fn spawn_drain(config: DrainConfig, index: SharedWalIndex) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = run_drain_loop(config, index).await {
            tracing::error!(error = %e, "state-builder drain loop exited with error");
        }
    })
}

/// Connect → consumer (durable authoritative, or ephemeral DeliverAll/AckNone
/// shadow) → drain → index → advance.
///
/// The **shadow** consumer is ephemeral `DeliverPolicy::All` + `AckPolicy::None`:
/// it replays the whole retained stream into the index on each start (the
/// cold-rebuild / crash-recovery model — RFC §7.3) and never competes for acks.
/// The **authoritative** consumer is durable (`AckPolicy::Explicit`,
/// `DeliverPolicy::All` on first create) so its backlog is observable and the
/// cursor survives restarts; it acks each drained batch.  Either way the index
/// is the same — the chain walk + cache produce identical state.
async fn run_drain_loop(config: DrainConfig, index: SharedWalIndex) -> Result<()> {
    let client = match (&config.nats_user, &config.nats_password) {
        (Some(u), Some(p)) => {
            ConnectOptions::with_user_and_password(u.clone(), p.clone())
                .connect(&config.nats_url)
                .await
                .context("state-builder shadow NATS connect (user/pass)")?
        }
        _ => async_nats::connect(&config.nats_url)
            .await
            .context("state-builder shadow NATS connect")?,
    };
    let js = jetstream::new(client);
    let stream = js
        .get_stream(&config.stream)
        .await
        .with_context(|| format!("state-builder get_stream {}", config.stream))?;

    // Shadow: ephemeral, all-history, no-ack consumer (own consumer, never the
    // materializer's).  Authoritative: durable, explicit-ack consumer so the
    // backlog is observable + the cursor survives restarts.
    let authoritative = config.mode == BuilderMode::Authoritative;
    let consumer = stream
        .create_consumer(PullConfig {
            durable_name: config.durable.clone(),
            filter_subject: "noetl.events.>".to_string(),
            deliver_policy: DeliverPolicy::All,
            ack_policy: if authoritative {
                AckPolicy::Explicit
            } else {
                AckPolicy::None
            },
            ..Default::default()
        })
        .await
        .context("state-builder create_consumer")?;

    tracing::info!(
        stream = %config.stream,
        durable = ?config.durable,
        mode = ?config.mode,
        batch = config.batch,
        "off-server state-builder drain started (WAL drain, zero noetl.event scans)"
    );

    loop {
        let mut batch = match consumer
            .batch()
            .max_messages(config.batch as usize)
            .expires(Duration::from_millis(config.timeout_ms))
            .messages()
            .await
        {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "state-builder batch failed; backing off");
                tokio::time::sleep(config.idle_sleep).await;
                continue;
            }
        };

        let mut touched: Vec<i64> = Vec::new();
        let mut consumed = 0u64;
        let mut terminals: Vec<i64> = Vec::new();
        // Apply the whole batch into the shared index under one lock, then drop
        // it before the (shadow-only) advance pass — keeps the critical section
        // the command-dispatch off-server-build path contends on short.
        {
            let mut idx = index.lock().await;
            while let Some(msg) = batch.next().await {
                let msg = match msg {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::warn!(error = %e, "state-builder message error");
                        continue;
                    }
                };
                consumed += 1;
                let payload: serde_json::Value = match serde_json::from_slice(&msg.payload) {
                    Ok(v) => v,
                    Err(_) => {
                        // Payload may be a JSON string holding JSON (mirror the
                        // materializer's tolerance).
                        match std::str::from_utf8(&msg.payload)
                            .ok()
                            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                        {
                            Some(v) => v,
                            None => {
                                if authoritative {
                                    let _ = msg.ack().await;
                                }
                                continue;
                            }
                        }
                    }
                };
                if let Some((execution_id, _is_new, is_terminal)) = idx.apply(&payload) {
                    if !touched.contains(&execution_id) {
                        touched.push(execution_id);
                    }
                    if is_terminal {
                        terminals.push(execution_id);
                    }
                }
                // Authoritative durable consumer: ack after the event is indexed
                // (at-least-once; re-apply on redelivery is idempotent — same
                // event_id overwrites identical data).
                if authoritative {
                    let _ = msg.ack().await;
                }
            }
        }

        if consumed == 0 {
            tokio::time::sleep(config.idle_sleep).await;
            continue;
        }
        crate::metrics::record_state_builder_wal_events(consumed);

        // Shadow: advance each touched execution's cached spine here — the cache
        // mechanics under observation.  Authoritative: the command-dispatch
        // off-server-build path ([`build_offserver_input`]) advances on demand
        // when a drive command arrives, so the drain only INDEXES here (and
        // evicts terminals) to keep the on-demand advance a cheap incremental.
        if !authoritative {
            let mut idx = index.lock().await;
            for eid in &touched {
                if let Some(chain) = idx.chain_mut(*eid) {
                    let outcome = chain.advance();
                    match outcome {
                        AdvanceOutcome::CacheHit => {
                            crate::metrics::record_state_builder_build("cache_hit")
                        }
                        AdvanceOutcome::Incremental(_) => {
                            crate::metrics::record_state_builder_build("incremental")
                        }
                        AdvanceOutcome::ColdRebuild(hops) => {
                            crate::metrics::record_state_builder_build("cold_rebuild");
                            crate::metrics::record_state_builder_chain_hops(hops);
                        }
                        AdvanceOutcome::Incomplete => {
                            crate::metrics::record_state_builder_build("incomplete")
                        }
                    }
                    tracing::debug!(
                        execution_id = *eid,
                        indexed = chain.len(),
                        spine = ?chain.cached_len(),
                        head = ?chain.head(),
                        outcome = ?outcome,
                        "state-builder shadow advanced execution (WAL chain walk, no noetl.event scan)"
                    );
                }
            }
        }
        {
            let mut idx = index.lock().await;
            for eid in terminals {
                idx.evict(eid);
            }
        }
    }
}

/// Parse `user:pass` out of a `nats://user:pass@host` URL, returning the URL with
/// userinfo stripped. `NATS_USER`/`NATS_PASSWORD` env take precedence (the
/// worker's command-source convention). Mirror of the materializer helper, kept
/// local so the shadow stays self-contained.
fn parse_nats_credentials(nats_url: &str) -> (String, Option<String>, Option<String>) {
    let env_user = std::env::var("NATS_USER").ok().filter(|s| !s.is_empty());
    let env_pass = std::env::var("NATS_PASSWORD").ok().filter(|s| !s.is_empty());
    if let (Some(u), Some(p)) = (&env_user, &env_pass) {
        return (strip_userinfo(nats_url), Some(u.clone()), Some(p.clone()));
    }
    match url::Url::parse(nats_url) {
        Ok(parsed) if !parsed.username().is_empty() && parsed.password().is_some() => {
            let user = urlencoding::decode(parsed.username())
                .map(|c| c.into_owned())
                .unwrap_or_else(|_| parsed.username().to_string());
            let pass = parsed.password().unwrap_or("");
            let pass = urlencoding::decode(pass)
                .map(|c| c.into_owned())
                .unwrap_or_else(|_| pass.to_string());
            (strip_userinfo(nats_url), Some(user), Some(pass))
        }
        _ => (nats_url.to_string(), None, None),
    }
}

fn strip_userinfo(nats_url: &str) -> String {
    match url::Url::parse(nats_url) {
        Ok(mut u) if !u.username().is_empty() => {
            let _ = u.set_username("");
            let _ = u.set_password(None);
            u.to_string()
        }
        _ => nats_url.to_string(),
    }
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `noetl_events`-shaped payload for one event.
    fn ev(event_id: i64, prev: Option<i64>, event_type: &str) -> serde_json::Value {
        serde_json::json!({
            "event_id": event_id,
            "execution_id": 42,
            "prev_event_id": prev,
            "event_type": event_type,
            "created_at": "2026-06-19T00:00:00Z",
        })
    }

    /// Apply a slice of events (in arbitrary order) to a fresh chain.
    fn chain_from(events: &[serde_json::Value]) -> ExecutionChain {
        let mut idx = WalEventIndex::new();
        for e in events {
            idx.apply(e);
        }
        idx.chains.remove(&42).unwrap()
    }

    /// A linear spine: playbook_started → command.issued → command.completed → …
    fn linear_spine(n: i64) -> Vec<serde_json::Value> {
        let mut out = vec![ev(1, None, "playbook_started")];
        for i in 2..=n {
            let ty = if i % 2 == 0 { "command.issued" } else { "command.completed" };
            out.push(ev(i, Some(i - 1), ty));
        }
        out
    }

    #[test]
    fn chain_walk_matches_sorted_scan_order() {
        // Parity by construction (the server#245 proof, now off the WAL index): the
        // chain walk collects head→root then sorts ascending; the result must equal
        // the event-scan ORDER BY event_id ASC — i.e. the identical sequence
        // from_events sees. Apply in REVERSE (worst case for the walk) to prove the
        // index order doesn't matter.
        let spine = linear_spine(6);
        let mut reversed = spine.clone();
        reversed.reverse();
        let chain = chain_from(&reversed);
        let walk = chain.chain_walk().expect("complete chain");
        let scan: Vec<i64> = (1..=6).collect();
        assert_eq!(walk, scan, "chain-walk order must equal event-scan event_id ASC");
        // And the raw ordered events come back in the same order.
        let ordered = chain.ordered_events().expect("ordered events");
        let ids: Vec<i64> = ordered.iter().map(|e| e["event_id"].as_i64().unwrap()).collect();
        assert_eq!(ids, scan);
    }

    #[test]
    fn chain_walk_requires_genesis() {
        // A restart-spanning tail with no playbook_started must be rejected so the
        // builder falls back rather than building a partial state — the server#245
        // non-genesis guard.
        let tail = vec![
            ev(5, None, "command.completed"),
            ev(6, Some(5), "command.issued"),
        ];
        let chain = chain_from(&tail);
        assert!(chain.chain_walk().is_none(), "non-genesis tail must be incomplete");
    }

    #[test]
    fn chain_walk_incomplete_on_missing_hop() {
        // head=4 → prev 3 → prev 2 → prev 1(genesis), but event 2 hasn't arrived
        // from the WAL yet (materializer/stream ordering). The walk hits a missing
        // hop and reports incomplete → caller falls back, retries next event.
        let mut chain = ExecutionChain::default();
        chain.apply(1, None, "playbook_started".into(), ev(1, None, "playbook_started"));
        chain.apply(3, Some(2), "command.completed".into(), ev(3, Some(2), "command.completed"));
        chain.apply(4, Some(3), "command.issued".into(), ev(4, Some(3), "command.issued"));
        assert!(chain.chain_walk().is_none(), "missing hop 2 must be incomplete");
        // Once 2 arrives, the chain is whole.
        chain.apply(2, Some(1), "command.issued".into(), ev(2, Some(1), "command.issued"));
        assert_eq!(chain.chain_walk().unwrap(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn incremental_tail_advance_equals_full_rebuild() {
        // The core cache property (RFC §5.2): advancing the cached spine by the new
        // tail must produce the byte-identical spine a full cold rebuild produces.
        let spine = linear_spine(4);
        let mut chain = chain_from(&spine);
        // First advance → cold rebuild of the 4-event spine.
        assert_eq!(chain.advance(), AdvanceOutcome::ColdRebuild(4));
        let head_after_4 = chain.cached_head();

        // Re-advance with no new events → pure cache hit (no work).
        assert_eq!(chain.advance(), AdvanceOutcome::CacheHit);

        // Append two new tail events, advance → incremental (only the tail walked).
        chain.apply(5, Some(4), "command.completed".into(), ev(5, Some(4), "command.completed"));
        chain.apply(6, Some(5), "command.issued".into(), ev(6, Some(5), "command.issued"));
        assert_eq!(chain.advance(), AdvanceOutcome::Incremental(2));
        let incremental_spine = chain.cached_len();

        // A from-scratch full rebuild of the same 6 events must match the
        // incrementally-advanced spine exactly.
        let full = chain_from(&linear_spine(6));
        let mut full = full;
        assert_eq!(full.advance(), AdvanceOutcome::ColdRebuild(6));
        assert_eq!(incremental_spine, full.cached_len());
        assert_eq!(chain.cached_head(), full.cached_head());
        // And the ordered spines are identical.
        assert_eq!(chain.chain_walk(), full.chain_walk());
        assert_ne!(chain.cached_head(), head_after_4, "head advanced");
    }

    #[test]
    fn cold_rebuild_on_miss_equals_original() {
        // Cache miss / restart (RFC §7.3): discarding the cache and re-walking from
        // the durable head reproduces the same spine — no truth lived in the cache.
        let spine = linear_spine(5);
        let mut chain = chain_from(&spine);
        assert_eq!(chain.advance(), AdvanceOutcome::ColdRebuild(5));
        let before = chain.chain_walk();
        let head_before = chain.cached_head();
        // Simulate a restart: cold rebuild from scratch.
        assert_eq!(chain.cold_rebuild(), AdvanceOutcome::ColdRebuild(5));
        assert_eq!(chain.chain_walk(), before);
        assert_eq!(chain.cached_head(), head_before);
    }

    #[test]
    fn non_extending_head_cold_rebuilds() {
        // If the cache holds head H and a NEW head H' arrives whose tail does not
        // walk back to H (e.g. a different branch root after a gap), advance must
        // cold-rebuild rather than silently splice a discontinuous spine.
        let mut chain = chain_from(&linear_spine(3));
        assert_eq!(chain.advance(), AdvanceOutcome::ColdRebuild(3));
        // Inject a higher-id event whose prev points at a NOT-yet-present id 9,
        // so the tail walk from the new head can't reach the cached head 3.
        chain.apply(10, Some(9), "command.issued".into(), ev(10, Some(9), "command.issued"));
        // Tail walk 10→9(missing) fails to reach cached head 3 → not incremental.
        // The full walk from head 10 also hits the missing hop → Incomplete.
        assert_eq!(chain.advance(), AdvanceOutcome::Incomplete);
    }

    #[test]
    fn index_routes_and_evicts_per_execution() {
        let mut idx = WalEventIndex::new();
        let a = serde_json::json!({"event_id": 1, "execution_id": 100, "prev_event_id": null, "event_type": "playbook_started"});
        let b = serde_json::json!({"event_id": 2, "execution_id": 200, "prev_event_id": null, "event_type": "playbook_started"});
        let term = serde_json::json!({"event_id": 3, "execution_id": 100, "prev_event_id": 1, "event_type": "playbook_completed"});
        assert_eq!(idx.apply(&a), Some((100, true, false)));
        assert_eq!(idx.apply(&b), Some((200, true, false)));
        assert_eq!(idx.apply(&term), Some((100, true, true)));
        assert_eq!(idx.execution_count(), 2);
        // Re-applying a (redelivery) is not new.
        assert_eq!(idx.apply(&a), Some((100, false, false)));
        idx.evict(100);
        assert_eq!(idx.execution_count(), 1);
        assert!(idx.chain(100).is_none());
        // A payload with no event_id isn't chainable.
        assert!(idx.apply(&serde_json::json!({"execution_id": 1})).is_none());
    }

    #[test]
    fn build_spine_returns_ordered_payloads_or_incomplete() {
        // A complete chain → build_spine returns the raw payloads in event_id
        // order (the OrchestrateInput.events the wasm `run` entry consumes).
        let mut idx = WalEventIndex::new();
        for e in linear_spine(4) {
            idx.apply(&e);
        }
        let (outcome, spine) = idx.build_spine(42);
        assert!(matches!(outcome, AdvanceOutcome::ColdRebuild(4)));
        let spine = spine.expect("complete chain yields a spine");
        let ids: Vec<i64> = spine.iter().map(|e| e["event_id"].as_i64().unwrap()).collect();
        assert_eq!(ids, vec![1, 2, 3, 4], "spine in event_id order");
        // Each payload is the raw WAL event (carries created_at + status), so it
        // deserializes into the orchestrate-core Event the plug-in expects.
        assert!(spine[0].get("created_at").is_some());

        // An unknown execution → Incomplete + no spine (caller falls back).
        let (o2, s2) = idx.build_spine(999);
        assert!(matches!(o2, AdvanceOutcome::Incomplete));
        assert!(s2.is_none());

        // A non-genesis tail → Incomplete (the genesis guard), so the off-server
        // drive falls back rather than building a partial state.
        let mut idx2 = WalEventIndex::new();
        idx2.apply(&ev(5, None, "command.completed"));
        idx2.apply(&ev(6, Some(5), "command.issued"));
        let (o3, s3) = idx2.build_spine(42);
        assert!(matches!(o3, AdvanceOutcome::ColdRebuild(_) | AdvanceOutcome::Incomplete));
        assert!(s3.is_none(), "non-genesis tail must not yield a spine");
    }

    #[tokio::test]
    async fn build_offserver_input_assembles_run_input() {
        // The off-server drive build assembles the exact OrchestrateInput shape
        // the wasm `run` (from_events) entry decodes: { events, playbook,
        // trigger_event_type }.
        let index: SharedWalIndex =
            std::sync::Arc::new(tokio::sync::Mutex::new(WalEventIndex::new()));
        {
            let mut idx = index.lock().await;
            for e in linear_spine(3) {
                idx.apply(&e);
            }
        }
        let playbook = serde_json::json!({ "metadata": { "path": "t" } });
        let bytes =
            build_offserver_input(&index, 42, &playbook, Some("command.completed"), None, Some(3))
                .await
                .expect("complete chain builds input");
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["trigger_event_type"], "command.completed");
        assert_eq!(v["playbook"]["metadata"]["path"], "t");
        let evs = v["events"].as_array().unwrap();
        assert_eq!(evs.len(), 3, "all three spine events");
        assert_eq!(evs[0]["event_id"], 1);

        // Stateless edge (RFC #115 Phase 4 remainder): trigger_event_type=None +
        // a trigger_event_id resolves the type off the WAL index (event 3 is
        // command.completed in the linear spine).
        let bytes2 = build_offserver_input(&index, 42, &playbook, None, Some(3), Some(3))
            .await
            .expect("complete chain builds input (resolved trigger type)");
        let v2: serde_json::Value = serde_json::from_slice(&bytes2).unwrap();
        assert_eq!(
            v2["trigger_event_type"], "command.completed",
            "trigger type resolved off the WAL index from trigger_event_id"
        );
        // An unindexed trigger_event_id defaults to command.completed.
        let bytes3 = build_offserver_input(&index, 42, &playbook, None, Some(999), Some(3))
            .await
            .expect("complete chain builds input (default trigger type)");
        let v3: serde_json::Value = serde_json::from_slice(&bytes3).unwrap();
        assert_eq!(v3["trigger_event_type"], "command.completed");

        // Staleness guard: an expected_head ahead of the index head → None (the
        // drain hasn't caught up to the server's dispatch watermark yet).
        assert!(
            build_offserver_input(&index, 42, &playbook, None, None, Some(99)).await.is_none(),
            "stale index (head < expected_head) must not serve"
        );

        // An unknown execution → None (the caller falls back to run_state).
        assert!(build_offserver_input(&index, 7, &playbook, None, None, None).await.is_none());
    }

    #[test]
    fn fanout_emission_order_is_a_linear_spine() {
        // The event chain (event.prev_event_id) is a single linear spine of ALL
        // events in emission order — the fan-out "shared branch origin" lives on
        // the COMMAND chain, not here (server#244 §4.4). So even with fan-out the
        // head→root walk collects every event, matching the scan. Simulate a
        // fan-out body burst: ids keep climbing along one prev spine.
        let mut spine = vec![ev(1, None, "playbook_started")];
        // start issued + completed
        spine.push(ev(2, Some(1), "command.issued"));
        spine.push(ev(3, Some(2), "call.done"));
        spine.push(ev(4, Some(3), "command.completed"));
        // fan-out: three body issues + completions, all on the linear emission spine
        for i in 5..=10 {
            spine.push(ev(i, Some(i - 1), "command.issued"));
        }
        let chain = chain_from(&spine);
        let walk = chain.chain_walk().expect("complete");
        assert_eq!(walk, (1..=10).collect::<Vec<i64>>());
    }
}
