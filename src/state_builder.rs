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
use std::time::{Duration, Instant};

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

/// Fixed per-event overhead added to the JSON-payload estimate when sizing an
/// [`IndexedEvent`] for the byte ledger (noetl/ai-meta#166 §5.1): the `event_id`
/// HashMap key + the `IndexedEvent` struct (the `prev_event_id`, the
/// `event_type` String header, the `bytes` field) + map-bucket overhead.  Rough;
/// the byte ceiling is a soft bound.
const INDEXED_EVENT_OVERHEAD: usize = 96;

/// One indexed event: the chain link + the event type (for the genesis guard) +
/// the raw `noetl_events` payload (the input a `from_events` build consumes).
#[derive(Debug, Clone)]
struct IndexedEvent {
    /// The immediately-previous event in this execution's causal order
    /// (`None` at the chain root). The link the walk follows.
    prev_event_id: Option<i64>,
    event_type: String,
    /// The event JSON the chain walk hands to a `from_events` build.  By default
    /// the full envelope as published to `noetl_events`; under
    /// `NOETL_STATE_INDEX_SLIM` (noetl/ai-meta#166 §4.1) a **lossless** projection
    /// to only the fields the orchestrate-core `Event` deserializer reads — every
    /// other top-level envelope field is dropped by serde on decode anyway, so the
    /// projection is output-equivalent by construction (the drive sees the same
    /// `Event`); it only stops resident memory carrying bytes `from_events` never
    /// looks at (`node_id`, `node_type`, `duration`, `stack_trace`, `trace_*`, …).
    raw: serde_json::Value,
    /// Approximate resident byte cost of [`Self::raw`] — maintained on apply so
    /// the bounded cache (noetl/ai-meta#166 §5.1) can enforce a byte ceiling
    /// without re-serializing the whole index. An estimate (string lengths +
    /// structural overhead), not an exact `serde_json` byte count; the ceiling is
    /// a soft bound so an approximation is sufficient and far cheaper.
    bytes: usize,
}

/// Approximate the resident byte cost of a JSON value — string content + a small
/// structural constant per node.  Used to maintain the bounded-cache byte ledger
/// (noetl/ai-meta#166 §5.1) cheaply, without re-serializing.  Deliberately rough:
/// the byte ceiling is a soft bound, so an estimate that tracks the real shape
/// (dominated by string payloads) is sufficient.
fn approx_json_bytes(v: &serde_json::Value) -> usize {
    match v {
        serde_json::Value::Null => 4,
        serde_json::Value::Bool(_) => 5,
        serde_json::Value::Number(_) => 8,
        serde_json::Value::String(s) => s.len() + 2,
        serde_json::Value::Array(a) => {
            2 + a.len() + a.iter().map(approx_json_bytes).sum::<usize>()
        }
        serde_json::Value::Object(m) => {
            2 + m
                .iter()
                .map(|(k, val)| k.len() + 4 + approx_json_bytes(val))
                .sum::<usize>()
        }
    }
}

/// The top-level envelope keys the orchestrate-core `Event` deserializer
/// (`repos/server/orchestrate-core/src/event.rs`) actually reads.  The slim
/// projection (noetl/ai-meta#166 §4.1) keeps exactly these; serde drops every
/// other key on decode, so a `from_events` build over the slim payload produces
/// the identical `Event` — and therefore the identical drive decision — as over
/// the full envelope.  `timestamp` AND its `created_at` alias are both kept so
/// the projection works whether the producer stamped the WAL/envelope name
/// (`timestamp`) or the DB column name (`created_at`).
const SLIM_EVENT_KEYS: &[&str] = &[
    "event_id",
    "execution_id",
    "catalog_id",
    "event_type",
    "node_name",
    "status",
    "context",
    "result",
    "meta",
    "timestamp",
    "created_at",
    "parent_execution_id",
    "attempt",
];

/// Project a `noetl_events` envelope down to [`SLIM_EVENT_KEYS`] — the lossless
/// slim-chain transform (noetl/ai-meta#166 §4.1).  A non-object payload (never
/// expected for a chainable event) passes through unchanged.
pub fn slim_event_payload(payload: &serde_json::Value) -> serde_json::Value {
    match payload.as_object() {
        Some(obj) => {
            let mut out = serde_json::Map::with_capacity(SLIM_EVENT_KEYS.len());
            for key in SLIM_EVENT_KEYS {
                if let Some(val) = obj.get(*key) {
                    out.insert((*key).to_string(), val.clone());
                }
            }
            serde_json::Value::Object(out)
        }
        None => payload.clone(),
    }
}

/// The per-event payload the noetl/ai-meta#166 Phase-3 **state shard** stores in
/// its `payload` column: [`slim_event_payload`] PLUS `prev_event_id`.
///
/// [`slim_event_payload`] deliberately omits `prev_event_id` because the
/// in-memory [`WalEventIndex::apply`] extracts the chain link into the
/// [`ExecutionChain`] node from the FULL envelope and stores it *outside* the raw
/// payload — so the resident raw never carries it.  The Phase-3 cold-load reader
/// ([`crate::state_reader`]), however, sees ONLY this stored payload, so the link
/// must ride *inside* it or the reconstructed chain has no edges to walk.
/// `apply` strips `prev_event_id` back out when it stores `raw` (it is not in
/// [`SLIM_EVENT_KEYS`]), so the stored raw remains byte-identical to the
/// WAL-replay path — the shard reconstructs the identical chain AND the identical
/// spine.
pub fn shard_chain_payload(payload: &serde_json::Value) -> serde_json::Value {
    let mut slim = slim_event_payload(payload);
    if let (Some(obj), Some(prev)) = (slim.as_object_mut(), payload.get("prev_event_id")) {
        obj.insert("prev_event_id".to_string(), prev.clone());
    }
    slim
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

/// How the off-server spine is ordered before it's handed to `from_events`
/// (noetl/ai-meta#117).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SpineOrder {
    /// Causal order — walk the `prev_event_id` chain head→root, then reverse to
    /// root→head.  The spine reflects the real causal linkage the Phase-2 chain
    /// encodes, so `from_events` replays in true causal order even when the
    /// `event_id`s are non-monotonic with the chain.  This is the default and the
    /// fix for noetl/ai-meta#117: under high-concurrency fan-out two branch
    /// completions can arrive at the owner reordered relative to their
    /// producer-assigned `event_id`s, linking a higher-id event as the
    /// predecessor of a lower-id one; an `event_id` sort then replays the inverted
    /// pair out of causal order and the fan-in reduce barrier never fires.
    #[default]
    Causal,
    /// Legacy `event_id`-ascending order (the pre-#117 behavior).  Assumes id
    /// order == chain order; correct only when every event's id is monotonic
    /// along the chain.  Kept as an instant revert
    /// (`NOETL_OFFSERVER_SPINE_ORDER=event_id`) — identical to `Causal` for any
    /// chain whose ids ARE monotonic (all linear / loop / sequential-fanout
    /// chains), differs only on the inversion #117 fixes.
    EventId,
}

/// Resolve the spine ordering from env.  `NOETL_OFFSERVER_SPINE_ORDER=event_id`
/// → legacy `event_id` sort (the #117 revert); anything else (incl. unset) →
/// causal order (the default, the #117 fix).  The fix only activates inside the
/// `NOETL_STATE_BUILDER=offserver` path (the only place a spine is built off the
/// server), so PROD — which runs the in-server drive — is untouched regardless.
pub fn spine_order() -> SpineOrder {
    if std::env::var("NOETL_OFFSERVER_SPINE_ORDER")
        .unwrap_or_default()
        .trim()
        .eq_ignore_ascii_case("event_id")
    {
        SpineOrder::EventId
    } else {
        SpineOrder::Causal
    }
}

/// The built spine cached for one execution, keyed by the immutable chain head.
#[derive(Debug, Clone)]
struct CachedSpine {
    /// The chain head this spine summarizes — the cache key. Immutable: a spine
    /// for a given head is valid forever (append-only chain).
    head_event_id: i64,
    /// The event ids on the spine in the configured [`SpineOrder`] — causal
    /// (root→head) by default, so `from_events` replays in true causal order
    /// (noetl/ai-meta#117).  For monotonic chains this is identical to the
    /// `event_id`-ascending order the server event-scan applies.
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
    /// How [`Self::chain_walk`] / [`Self::advance`] order the spine — injected by
    /// the owning [`WalEventIndex`] (noetl/ai-meta#117).  Default `Causal`.
    order: SpineOrder,
    /// Running sum of [`IndexedEvent::bytes`] across this chain's indexed events
    /// — the per-execution slice of the bounded-cache byte ledger
    /// (noetl/ai-meta#166 §5.1).  Maintained incrementally on apply (delta on a
    /// redelivery overwrite) so the index never walks all events to size itself.
    bytes: usize,
}

impl ExecutionChain {
    /// A fresh chain that orders its spine per `order` (noetl/ai-meta#117).
    fn with_order(order: SpineOrder) -> Self {
        Self {
            order,
            ..Default::default()
        }
    }

    /// Index one WAL event. Idempotent: re-applying the same `event_id` (a
    /// JetStream redelivery) overwrites with identical data and never double-counts
    /// the head. Returns `true` if this event was new to the index.
    pub fn apply(&mut self, event_id: i64, prev_event_id: Option<i64>, event_type: String, raw: serde_json::Value) -> bool {
        let bytes = approx_json_bytes(&raw) + INDEXED_EVENT_OVERHEAD;
        // Adjust the byte ledger by the delta: a redelivery overwrite replaces an
        // existing entry's cost, a new event adds its cost.
        let is_new = match self.events.get(&event_id) {
            Some(prior) => {
                self.bytes = self.bytes.saturating_sub(prior.bytes);
                false
            }
            None => true,
        };
        self.bytes += bytes;
        self.events.insert(
            event_id,
            IndexedEvent { prev_event_id, event_type, raw, bytes },
        );
        // Advance the head monotonically — the chain tip is the max id seen.
        if self.head.is_none_or(|h| event_id > h) {
            self.head = Some(event_id);
        }
        is_new
    }

    /// Approximate resident bytes this chain's indexed events occupy
    /// (noetl/ai-meta#166 §5.1) — the per-execution byte-ledger slice the
    /// bounded cache sums to enforce the byte ceiling.
    pub fn bytes(&self) -> usize {
        self.bytes
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

    /// Walk the chain head→root by `prev_event_id`, returning the spine in the
    /// configured [`SpineOrder`] — causal (root→head) by default
    /// (noetl/ai-meta#117), or `event_id`-ascending under the legacy revert.
    /// Returns `None` when the chain can't be trusted complete: a hop points at an
    /// event not present in the index (WAL ordering / gap), the walk didn't reach
    /// the genesis `playbook_started`, or it's empty.  This is the exact
    /// completeness contract the server chain-walk falls back on (server#245); the
    /// off-server builder falls back to the server build the same way.
    pub fn chain_walk(&self) -> Option<Vec<i64>> {
        self.chain_walk_from(self.head?)
    }

    /// Walk the chain from an explicit `start` head→root by `prev_event_id`,
    /// returning the spine in the configured [`SpineOrder`].  Same completeness
    /// contract as [`Self::chain_walk`], but rooted at a caller-supplied tip
    /// rather than the max-id head.
    ///
    /// This is the load-bearing distinction for noetl/ai-meta#117: the
    /// authoritative off-server drive walks from `expected_head` — the server's
    /// `ChainHeads` watermark, which is the **last-arrived** event (`link_batch`
    /// advances the head to `event_ids.last()`, the real causal tip).  Under a
    /// high-concurrency fan-out the last-arrived branch completion can carry a
    /// LOWER producer-assigned `event_id` than its predecessor, so `max(event_id)`
    /// is NOT the tip — a walk from the max-id head would start one branch up and
    /// MISS the inverted tip entirely (the fan-in then never sees that branch's
    /// completion and the reduce never fires).  Starting from `expected_head`
    /// reaches every event on the chain regardless of id monotonicity.
    fn chain_walk_from(&self, start: i64) -> Option<Vec<i64>> {
        let mut ordered: Vec<i64> = Vec::new();
        let mut cursor = Some(start);
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
        match self.order {
            // `ordered` was collected head→root by `prev_event_id`; reverse to
            // root→head so `from_events` replays in true causal order even when
            // the `event_id`s are non-monotonic with the chain (noetl/ai-meta#117).
            SpineOrder::Causal => ordered.reverse(),
            // Legacy revert: `event_id`-ascending (identical to the reverse above
            // for any monotonic chain; wedges fan-in only on the #117 inversion).
            SpineOrder::EventId => ordered.sort_unstable(),
        }
        Some(ordered)
    }

    /// The ordered event spine as the raw `noetl_events` payloads — the verbatim
    /// input a `from_events` build (server-side or wasm) consumes, walked from the
    /// max-id head. `None` under the same incompleteness conditions as
    /// [`Self::chain_walk`].  The authoritative drive instead serves
    /// [`Self::cached_spine_events`] (the spine [`Self::advance_to`] just built
    /// from `expected_head`) so the served order matches the cache.
    pub fn ordered_events(&self) -> Option<Vec<serde_json::Value>> {
        let ids = self.chain_walk()?;
        Some(ids.iter().map(|id| self.events[id].raw.clone()).collect())
    }

    /// The raw `noetl_events` payloads for the currently-cached spine — the exact
    /// artefact the last [`Self::advance`] / [`Self::advance_to`] built, in cache
    /// order.  `None` when no spine is cached.  This is what the off-server drive
    /// serves, so the served spine is the one whose ordering the cache encodes
    /// (rooted at `expected_head` under #117), not a fresh max-id walk.
    fn cached_spine_events(&self) -> Option<Vec<serde_json::Value>> {
        let c = self.cache.as_ref()?;
        Some(c.ordered_ids.iter().map(|id| self.events[id].raw.clone()).collect())
    }

    /// Advance the cached spine to the **max-id head** — the shadow/observation
    /// path.  Delegates to [`Self::advance_to`]; see it for the cache mechanics.
    /// For monotonic chains (every non-fan-out case) the max-id head IS the tip,
    /// so this equals advancing to the real tip.
    pub fn advance(&mut self) -> AdvanceOutcome {
        match self.head {
            Some(head) => self.advance_to(head),
            None => AdvanceOutcome::Incomplete,
        }
    }

    /// Advance the cached spine so its head is `target_head`, doing the
    /// **minimum** work: a no-op on an unchanged cached head, a **tail-only** walk
    /// when `target_head` extends the cached head along the chain (reachability
    /// checked by walking `prev_event_id`, NOT by id comparison — ids are
    /// non-monotonic under a #117 inversion), or a full cold rebuild otherwise.
    /// The advanced spine equals a cold rebuild from the same `target_head`
    /// (proven in the unit tests).
    ///
    /// `Incomplete` when `target_head` isn't indexed yet — this IS the staleness
    /// guard for the off-server drive (the worker's WAL drain lags the server's
    /// view, so serve only once the index has caught up to the server's dispatch
    /// watermark `expected_head`), now expressed as "the tip must be present"
    /// rather than the pre-#117 `max_id >= expected` check, which a fan-out id
    /// inversion could satisfy without the real tip being indexed.
    pub fn advance_to(&mut self, target_head: i64) -> AdvanceOutcome {
        // The tip must be indexed before a spine can be built to it.
        if !self.events.contains_key(&target_head) {
            return AdvanceOutcome::Incomplete;
        }
        // Cached head already at the target → hit.
        if let Some(c) = &self.cache {
            if c.head_event_id == target_head {
                return AdvanceOutcome::CacheHit;
            }
        }
        // Try an incremental tail-advance: walk target_head→cached_head along the
        // chain.  Reachability (walk reaches the cached head) is the only test —
        // an id comparison would be wrong under inversion.
        if let Some(c) = &self.cache {
            if let Some(mut tail) = self.walk_tail_to(target_head, c.head_event_id) {
                let added = tail.len();
                let mut ordered = c.ordered_ids.clone();
                match self.order {
                    // `walk_tail_to` collected target→down; reverse to causal
                    // (cached_head+1 → target_head) and append to the causal cache
                    // so the advanced spine equals a cold rebuild (#117).
                    SpineOrder::Causal => {
                        tail.reverse();
                        ordered.extend(tail);
                    }
                    SpineOrder::EventId => {
                        ordered.extend(tail);
                        ordered.sort_unstable();
                    }
                }
                self.cache = Some(CachedSpine {
                    head_event_id: target_head,
                    ordered_ids: ordered,
                });
                return AdvanceOutcome::Incremental(added);
            }
        }
        // No cache / tail can't reach the cached head → cold rebuild from the tip.
        match self.chain_walk_from(target_head) {
            Some(ordered) => {
                let len = ordered.len();
                self.cache = Some(CachedSpine {
                    head_event_id: target_head,
                    ordered_ids: ordered,
                });
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

/// Bounded-cache eviction policy for the pool-side WAL index
/// (noetl/ai-meta#166 Phase 1, RFC §5.1).  Every knob defaults to **off**
/// (unbounded = today's behaviour) so a worker carrying this code is
/// byte-for-byte behaviour-neutral until an operator sets the env vars — the
/// deploy is safe before the flags are flipped.
///
/// Resolved from env by [`Self::from_env`]:
/// - `NOETL_STATE_INDEX_MAX_BYTES` → [`Self::max_bytes`] (hard resident ceiling;
///   the bounded-memory guarantee — evict LRU until under it).
/// - `NOETL_STATE_INDEX_TTL_SECS` → [`Self::ttl`] (idle TTL; evicts a
///   non-terminal execution not driven / not fed an event for this long — the
///   fix for the stuck/abandoned executions terminal-eviction misses, RFC §1.3).
/// - `NOETL_STATE_INDEX_MAX_EXECUTIONS` → [`Self::max_executions`] (cap on
///   concurrent resident chains).
/// - `NOETL_STATE_INDEX_SLIM` → [`Self::slim`] (store the lossless slim
///   projection instead of the full envelope — RFC §4.1).
#[derive(Debug, Clone, Copy, Default)]
pub struct EvictionPolicy {
    pub max_bytes: Option<usize>,
    pub ttl: Option<Duration>,
    pub max_executions: Option<usize>,
    pub slim: bool,
}

impl EvictionPolicy {
    /// Resolve the policy from env.  Unset / zero / unparseable → that knob stays
    /// off, so the default policy is fully unbounded (today's behaviour).
    pub fn from_env() -> Self {
        let max_bytes = std::env::var("NOETL_STATE_INDEX_MAX_BYTES")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .filter(|&n| n > 0);
        let ttl = std::env::var("NOETL_STATE_INDEX_TTL_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .filter(|&n| n > 0)
            .map(Duration::from_secs);
        let max_executions = std::env::var("NOETL_STATE_INDEX_MAX_EXECUTIONS")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .filter(|&n| n > 0);
        let slim = matches!(
            std::env::var("NOETL_STATE_INDEX_SLIM")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "1" | "true" | "yes" | "on"
        );
        Self {
            max_bytes,
            ttl,
            max_executions,
            slim,
        }
    }

    /// True when any eviction bound (bytes / TTL / max-executions) is set — i.e.
    /// the index is no longer unbounded.  `slim` alone doesn't make the policy
    /// "active" for the periodic sweep; it only changes what's stored.
    pub fn bounds_memory(&self) -> bool {
        self.max_bytes.is_some() || self.ttl.is_some() || self.max_executions.is_some()
    }
}

/// One bounded-cache eviction sweep's accounting (noetl/ai-meta#166 §5.1) — how
/// many chains left by each reason, so the drain can record the
/// `state_builder_evictions_total{reason}` metric.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EvictionStats {
    pub ttl: usize,
    pub max_executions: usize,
    pub byte_ceiling: usize,
}

impl EvictionStats {
    pub fn total(&self) -> usize {
        self.ttl + self.max_executions + self.byte_ceiling
    }
}

/// Pool-side index of all in-flight executions' chains. Holds one
/// [`ExecutionChain`] per `execution_id`; terminal executions are evicted to
/// bound memory (RFC §5.2 — eviction, never staleness invalidation).  Under a
/// non-default [`EvictionPolicy`] (noetl/ai-meta#166 Phase 1) idle/abandoned
/// non-terminal chains are also evicted by TTL and the resident set is held
/// under a hard byte ceiling — so memory is `O(active working set)` rather than
/// `O(all non-terminal event history)`.
#[derive(Debug, Default)]
pub struct WalEventIndex {
    chains: HashMap<i64, ExecutionChain>,
    /// Spine ordering injected into every chain this index creates
    /// (noetl/ai-meta#117).  Default `Causal`.
    order: SpineOrder,
    /// Bounded-cache policy (noetl/ai-meta#166).  Default = unbounded.
    policy: EvictionPolicy,
    /// Last-activity instant per execution — refreshed when an event is applied
    /// for it or when its spine is built (driven).  The LRU/TTL key.  Held here
    /// (not on the clockless [`ExecutionChain`]) so the chain stays a pure,
    /// deterministic function of its events for unit testing.
    access: HashMap<i64, Instant>,
    /// Running sum of every resident chain's [`ExecutionChain::bytes`] — the
    /// bounded-cache byte ledger.  Maintained incrementally on apply / evict so
    /// the byte ceiling never walks the whole index to size it.
    total_bytes: usize,
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

    /// An index whose chains order their spine per `order` (noetl/ai-meta#117).
    /// The worker resolves `order` from env ([`spine_order`]) at startup.
    pub fn with_order(order: SpineOrder) -> Self {
        Self {
            order,
            ..Default::default()
        }
    }

    /// An index with both the spine ordering and the bounded-cache eviction
    /// policy resolved from env (noetl/ai-meta#166) — the worker's startup
    /// constructor.
    pub fn with_order_policy(order: SpineOrder, policy: EvictionPolicy) -> Self {
        Self {
            order,
            policy,
            ..Default::default()
        }
    }

    /// The active eviction policy.
    pub fn policy(&self) -> EvictionPolicy {
        self.policy
    }

    /// Swap the eviction policy — test seam for sizing a ceiling against a
    /// measured resident set.
    #[cfg(test)]
    fn set_policy_for_test(&mut self, policy: EvictionPolicy) {
        self.policy = policy;
    }

    /// Index one WAL event payload (the `noetl_events` shape). Extracts the chain
    /// fields and routes them to the owning execution's [`ExecutionChain`].
    /// Returns the `(execution_id, is_new, is_terminal)` triple, or `None` when the
    /// payload isn't a chainable event (no `event_id`/`execution_id`).
    pub fn apply(&mut self, payload: &serde_json::Value) -> Option<(i64, bool, bool)> {
        self.apply_at(payload, Instant::now())
    }

    /// [`Self::apply`] with an explicit activity instant — the clock is injected
    /// so unit tests can drive TTL eviction deterministically.
    pub fn apply_at(
        &mut self,
        payload: &serde_json::Value,
        now: Instant,
    ) -> Option<(i64, bool, bool)> {
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
        let order = self.order;
        // Slim projection (noetl/ai-meta#166 §4.1): store only the fields the
        // orchestrate-core `Event` deserializer reads.  Lossless w.r.t.
        // `from_events` (serde drops the rest on decode regardless), so the drive
        // decision is identical — it just stops the index carrying bytes the
        // build never looks at.
        let stored = if self.policy.slim {
            slim_event_payload(payload)
        } else {
            payload.clone()
        };
        let chain = self
            .chains
            .entry(execution_id)
            .or_insert_with(|| ExecutionChain::with_order(order));
        let before = chain.bytes();
        let is_new = chain.apply(event_id, prev_event_id, event_type, stored);
        let after = chain.bytes();
        // Maintain the index-wide byte ledger by the chain's delta.
        self.total_bytes = self.total_bytes + after - before;
        // An applied event is activity for this execution — refresh its LRU/TTL
        // stamp so a chain receiving WAL events is never evicted as idle.
        self.access.insert(execution_id, now);
        Some((execution_id, is_new, is_terminal))
    }

    /// Refresh an execution's last-activity stamp (noetl/ai-meta#166) — called on
    /// the drive path when a spine is built, so a *driven* execution counts as
    /// active for LRU/TTL even between WAL events.
    pub fn touch(&mut self, execution_id: i64) {
        self.touch_at(execution_id, Instant::now());
    }

    /// [`Self::touch`] with an explicit instant (test seam).
    pub fn touch_at(&mut self, execution_id: i64, now: Instant) {
        if self.chains.contains_key(&execution_id) {
            self.access.insert(execution_id, now);
        }
    }

    /// Borrow an execution's chain (for advance / walk).
    pub fn chain_mut(&mut self, execution_id: i64) -> Option<&mut ExecutionChain> {
        self.chains.get_mut(&execution_id)
    }

    pub fn chain(&self, execution_id: i64) -> Option<&ExecutionChain> {
        self.chains.get(&execution_id)
    }

    /// Drop a terminal execution's chain — frees memory. Mirrors the server's
    /// orch-cache + chain-head eviction on a terminal event.  Keeps the byte
    /// ledger + access map in step with the removed chain (noetl/ai-meta#166).
    pub fn evict(&mut self, execution_id: i64) {
        if let Some(chain) = self.chains.remove(&execution_id) {
            self.total_bytes = self.total_bytes.saturating_sub(chain.bytes());
        }
        self.access.remove(&execution_id);
    }

    /// Number of executions currently indexed.
    pub fn execution_count(&self) -> usize {
        self.chains.len()
    }

    /// Approximate total resident bytes across all indexed chains (the bounded-
    /// cache byte ledger, noetl/ai-meta#166 §5.1).
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Total indexed events across all chains — the resident-events gauge
    /// (the `654 × 27` headline of the #166 problem statement).
    pub fn event_count(&self) -> usize {
        self.chains.values().map(|c| c.len()).sum()
    }

    /// Enforce the bounded-cache policy (noetl/ai-meta#166 §5.1), evicting chains
    /// in priority order: **TTL** (idle non-terminal — the stuck/abandoned
    /// executions terminal-eviction misses), then **max-executions**, then the
    /// hard **byte ceiling**.  Both count-based passes evict least-recently-active
    /// first (LRU), so the *currently-driven* working set is the last to go and a
    /// sanely-sized ceiling never touches it.  Returns per-reason [`EvictionStats`]
    /// for the drain's metric.  A no-op when the policy is unbounded (today's
    /// behaviour).  `now` is injected for deterministic testing.
    pub fn enforce_limits_at(&mut self, now: Instant) -> EvictionStats {
        let mut stats = EvictionStats::default();
        if !self.policy.bounds_memory() {
            return stats;
        }

        // 1) TTL — evict any chain idle longer than the TTL.  A chain with no
        //    access stamp (shouldn't happen — apply always stamps) is treated as
        //    idle-forever and swept.
        if let Some(ttl) = self.policy.ttl {
            let stale: Vec<i64> = self
                .chains
                .keys()
                .copied()
                .filter(|eid| {
                    self.access
                        .get(eid)
                        .map(|t| now.saturating_duration_since(*t) >= ttl)
                        .unwrap_or(true)
                })
                .collect();
            for eid in stale {
                self.evict(eid);
                stats.ttl += 1;
            }
        }

        // 2) Max-executions — evict LRU until at or under the cap.
        if let Some(max) = self.policy.max_executions {
            while self.chains.len() > max {
                match self.lru_execution() {
                    Some(eid) => {
                        self.evict(eid);
                        stats.max_executions += 1;
                    }
                    None => break,
                }
            }
        }

        // 3) Byte ceiling — the bounded-memory guarantee.  Evict LRU until the
        //    resident set is at or under the ceiling.
        if let Some(max_bytes) = self.policy.max_bytes {
            while self.total_bytes > max_bytes && !self.chains.is_empty() {
                match self.lru_execution() {
                    Some(eid) => {
                        self.evict(eid);
                        stats.byte_ceiling += 1;
                    }
                    None => break,
                }
            }
        }
        stats
    }

    /// [`Self::enforce_limits_at`] at the current instant — the drain-loop entry.
    pub fn enforce_limits(&mut self) -> EvictionStats {
        self.enforce_limits_at(Instant::now())
    }

    /// The least-recently-active resident execution (LRU eviction victim).  A
    /// chain missing from `access` sorts oldest (evicted first).
    fn lru_execution(&self) -> Option<i64> {
        self.chains
            .keys()
            .copied()
            .min_by_key(|eid| self.access.get(eid).copied())
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
                let spine = match outcome {
                    AdvanceOutcome::Incomplete => None,
                    _ => chain.cached_spine_events(),
                };
                (outcome, spine)
            }
            // No chain indexed for this execution yet (the WAL drain hasn't seen
            // any of its events) — incomplete, fall back.
            None => (AdvanceOutcome::Incomplete, None),
        }
    }

    /// Like [`Self::build_spine`] but rooted at an explicit `target_head` — the
    /// server's authoritative chain tip (`expected_head`) — so the served spine
    /// reaches the real causal tip even when it carries a lower `event_id` than
    /// its predecessor (the noetl/ai-meta#117 fan-out inversion).  This is the
    /// path the authoritative off-server drive uses; the staleness guard is
    /// intrinsic ([`ExecutionChain::advance_to`] returns `Incomplete` until the
    /// tip is indexed).  Returns the spine in the cache's [`SpineOrder`].
    pub fn build_spine_to(
        &mut self,
        execution_id: i64,
        target_head: i64,
    ) -> (AdvanceOutcome, Option<Vec<serde_json::Value>>) {
        match self.chains.get_mut(&execution_id) {
            Some(chain) => {
                let outcome = chain.advance_to(target_head);
                let spine = match outcome {
                    AdvanceOutcome::Incomplete => None,
                    _ => chain.cached_spine_events(),
                };
                (outcome, spine)
            }
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
///
/// Alongside the index it carries an [`appended`](SharedWalIndex::appended)
/// [`Notify`](tokio::sync::Notify) the drain pulses every time it applies a
/// non-empty batch (noetl/ai-meta#130).  The off-server drive's build-retry loop
/// parks on that signal instead of polling on a fixed `idle_sleep` grid, so a
/// hop advances the instant the drain indexes the event it needs rather than
/// waiting for the next poll tick / the 8s reconcile poller.  The signal is a
/// liveness hint only — the index under the mutex is the source of truth; a
/// spurious or missed pulse only changes *when* the loop re-checks, never *what*
/// it builds.
#[derive(Clone)]
pub struct SharedWalIndex {
    inner: std::sync::Arc<tokio::sync::Mutex<WalEventIndex>>,
    appended: std::sync::Arc<tokio::sync::Notify>,
}

impl SharedWalIndex {
    /// Wrap a fresh [`WalEventIndex`] with its append signal.
    pub fn new(index: WalEventIndex) -> Self {
        Self {
            inner: std::sync::Arc::new(tokio::sync::Mutex::new(index)),
            appended: std::sync::Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Lock the underlying index.  Named `lock` so existing
    /// `index.lock().await` call sites are unchanged.
    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, WalEventIndex> {
        self.inner.lock().await
    }

    /// A handle to the append signal the drain pulses after each non-empty
    /// apply.  Callers register interest with [`Notify::notified`] *before*
    /// checking the index (the enable-before-check pattern) so an append landing
    /// between the check and the await can't be lost.
    pub fn appended(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.appended.clone()
    }

    /// Wake every waiter parked on the append signal.  Called by the drain loop
    /// after it applies a batch that touched at least one execution.  Cheap and
    /// lock-free; a no-op when nobody is waiting.
    pub fn notify_appended(&self) {
        self.appended.notify_waiters();
    }
}

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
    atomic_item_context: bool,
) -> Option<Vec<u8>> {
    let (outcome, spine, resolved_trigger_type) = {
        let mut idx = index.lock().await;
        // Build the spine rooted at the server's authoritative chain tip
        // (`expected_head`, the `ChainHeads` watermark = the last-arrived event)
        // when supplied — NOT the worker's max-id head.  Under a high-concurrency
        // fan-out the last-arrived branch completion can carry a lower
        // producer-assigned `event_id` than its predecessor, so the max-id head is
        // NOT the causal tip and a max-id walk would miss the inverted branch's
        // completion (the fan-in then never fires) — noetl/ai-meta#117.  Walking
        // from `expected_head` reaches every event regardless of id monotonicity,
        // and the staleness guard is intrinsic: `advance_to` reports `Incomplete`
        // until the tip is indexed (the worker WAL drain lags the server), so the
        // WAL-built state is never staler than the server's view.  When the server
        // doesn't supply a watermark (legacy non-stateless path) fall back to the
        // max-id head — the pre-#117 behavior for that path.
        let (outcome, spine) = match expected_head {
            Some(target) => idx.build_spine_to(execution_id, target),
            None => idx.build_spine(execution_id),
        };
        // Refresh the LRU/TTL stamp: an execution being driven is active even
        // between WAL events, so the bounded cache (noetl/ai-meta#166) keeps it
        // resident and evicts only genuinely idle chains.
        idx.touch(execution_id);
        let chain = idx.chain(execution_id);
        // Stateless off-server drive (RFC #115 Phase 4 remainder): when the
        // server did NOT supply `trigger_event_type` (it no longer reads
        // `noetl.event` to classify the trigger), resolve it off the WAL index
        // from the server-supplied `trigger_event_id`.  Falls back to
        // `command.completed` (the only triggering type) if the id isn't indexed.
        let resolved_trigger_type = trigger_event_type.map(|s| s.to_string()).or_else(|| {
            trigger_event_id
                .and_then(|tid| chain.and_then(|c| c.event_type_of(tid)).map(|s| s.to_string()))
        });
        (outcome, spine, resolved_trigger_type)
    };
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
        // RFC #115 Phase 5: forward the atomic-item-context flag onto the
        // from_events `OrchestrateInput` so the off-server drive narrows each
        // worker-bound command context to its minimal slice.  Default false
        // (the server omits it) → full-context dispatch, unchanged.
        "atomic_item_context": atomic_item_context,
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

/// True when `NOETL_STATE_BUILDER_DURABLE` is truthy — opts the authoritative
/// drain back into a **durable** `noetl_state_builder` consumer (the pre-#119
/// behavior).  Default **off**.
///
/// The default authoritative drain uses an **ephemeral** `DeliverPolicy::All`
/// consumer that rebuilds the full in-memory [`WalEventIndex`] from the retained
/// `noetl_events` WAL on **every boot** — exactly the shadow consumer shape — so
/// a persisted consumer cursor can never outrun the freshly-empty index and
/// strand in-flight executions after a worker restart (noetl/ai-meta#119).  The
/// durable form persists a cursor across restarts while the in-memory index
/// rebuilds empty, so the cursor sits ahead of the events the fresh index needs
/// → `build_spine_to(expected_head)` is permanently `Incomplete` → the off-server
/// drive loops `offserver_retry` and executions never complete.  It is NOT
/// restart-safe until the index is snapshotted alongside the cursor; kept only as
/// an instant revert for the steady-state ack/backlog-observability shape.
pub fn durable_consumer_enabled() -> bool {
    matches!(
        std::env::var("NOETL_STATE_BUILDER_DURABLE")
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
    /// Durable consumer name for the drain, or `None` for an **ephemeral**
    /// `DeliverPolicy::All` consumer that cold-replays the retained stream on each
    /// start.  `None` is the default for BOTH shadow and authoritative
    /// (noetl/ai-meta#119): the ephemeral rebuild-on-boot guarantees the in-memory
    /// index is always repopulated to cover the retained WAL, so no persisted
    /// cursor can outrun a freshly-restarted worker's empty index.  A durable name
    /// is set only when `NOETL_STATE_BUILDER_DURABLE` opts back into the pre-#119
    /// durable consumer (see [`durable_consumer_enabled`]) — which is NOT
    /// restart-safe without an index snapshot.
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
        // noetl/ai-meta#119: the authoritative drain defaults to an **ephemeral**
        // DeliverAll consumer that rebuilds the full in-memory index from the
        // retained `noetl_events` WAL on every boot — so a persisted cursor can
        // never outrun the freshly-empty index and strand in-flight executions
        // after a worker restart.  Shadow is ephemeral too.  Only when
        // `NOETL_STATE_BUILDER_DURABLE` is set does the authoritative drain take a
        // durable consumer (the pre-#119 shape; not restart-safe without an index
        // snapshot) — kept as an instant revert.
        let durable = if mode == BuilderMode::Authoritative && durable_consumer_enabled() {
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
            // noetl/ai-meta#130: the post-empty backoff was 500ms — on an idle
            // cluster a freshly-published event could sit up to that long between
            // an empty long-poll returning and the next `batch()` starting, which
            // (stacked on the old fixed-grid drive retry) was a chunk of the
            // ~1.8s/hop off-server latency.  The `expires`-bounded long-poll below
            // already blocks efficiently while waiting for messages, so this is a
            // tiny re-poll gap, not a busy-loop guard; drop it to 25ms so the drain
            // re-arms its long-poll near-immediately and the append signal fires
            // within milliseconds of an event landing.  Still env-overridable.
            idle_sleep: Duration::from_millis(env_u64("NOETL_STATE_BUILDER_IDLE_SLEEP_MS", 25)),
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

/// Backoff floor/ceiling for the state-builder consumer reconnect+rebuild path
/// (noetl/ai-meta#161).  Starts at 250ms, doubles per failed attempt, caps at
/// 10s — fast enough to recover from a NATS bounce in seconds, slow enough not
/// to hammer a still-down server.
const REBUILD_BACKOFF_MIN: Duration = Duration::from_millis(250);
const REBUILD_BACKOFF_MAX: Duration = Duration::from_secs(10);

/// How long the dead-consumer signature must persist before the drain tears the
/// consumer down and recreates it.  Tolerates a single transient blip; a real
/// orphaned consumer (NATS restart) keeps signalling and crosses this quickly.
/// Env override `NOETL_STATE_BUILDER_REBUILD_SECS` (default 5s).
fn rebuild_after() -> Duration {
    Duration::from_secs(env_u64("NOETL_STATE_BUILDER_REBUILD_SECS", 5))
}

/// How long the drain may be continuously unable to serve before `/livez` flips
/// to failing so Kubernetes restarts the pod as a backstop to the in-process
/// self-heal.  Longer than [`rebuild_after`] so the worker gets to recover on
/// its own first.  Env override `NOETL_STATE_BUILDER_UNHEALTHY_SECS` (default
/// 45s).
fn unhealthy_after() -> Duration {
    Duration::from_secs(env_u64("NOETL_STATE_BUILDER_UNHEALTHY_SECS", 45))
}

/// True when a JetStream pull/connect error indicates the consumer or connection
/// is gone (NATS server restarted, consumer orphaned/deleted) rather than
/// transiently busy — the noetl/ai-meta#161 wedge signature.  The orphaned
/// consumer surfaces as a repeated `503 "no responders"` while pulling; a
/// deleted consumer surfaces as "consumer not found"/"consumer deleted".
fn is_consumer_dead<E: std::fmt::Display>(err: &E) -> bool {
    let s = err.to_string().to_ascii_lowercase();
    s.contains("503")
        || s.contains("no responders")
        || s.contains("no responder")
        || s.contains("consumer not found")
        || s.contains("consumer deleted")
        || s.contains("consumer is gone")
        || s.contains("consumer not active")
        || s.contains("no consumer")
}

/// Decision returned by [`on_dead_signal`].
enum DeadAction {
    /// Dead signal sustained past the rebuild threshold — tear down + recreate.
    Rebuild,
    /// Dead signal not yet sustained — retry the same consumer briefly.
    Retry,
}

/// Fold a dead-consumer observation into the rolling `dead_since` timer and
/// decide whether to rebuild now (sustained past `rebuild_after`) or keep
/// retrying.  Flips the health gauge to unhealthy once the drain has been unable
/// to serve for `unhealthy_after` so the `/livez` backstop can fire.
fn on_dead_signal(
    dead_since: &mut Option<Instant>,
    last_healthy: &Instant,
    rebuild_after: Duration,
    unhealthy_after: Duration,
) -> DeadAction {
    let since = *dead_since.get_or_insert_with(Instant::now);
    if since.elapsed() >= rebuild_after {
        DeadAction::Rebuild
    } else {
        if last_healthy.elapsed() >= unhealthy_after {
            crate::metrics::set_state_builder_healthy(false);
        }
        DeadAction::Retry
    }
}

/// Connect to NATS and create the state-builder drain consumer (noetl/ai-meta
/// #161).  Returns the live client (kept in scope for the consumer's lifetime)
/// plus the consumer.  Factored out of the old inline `run_drain_loop` setup so
/// the reconnect path can call it on every rebuild.
async fn connect_drain_consumer(
    config: &DrainConfig,
) -> Result<(
    async_nats::Client,
    jetstream::consumer::Consumer<PullConfig>,
)> {
    let client = match (&config.nats_user, &config.nats_password) {
        (Some(u), Some(p)) => ConnectOptions::with_user_and_password(u.clone(), p.clone())
            .connect(&config.nats_url)
            .await
            .context("state-builder NATS connect (user/pass)")?,
        _ => async_nats::connect(&config.nats_url)
            .await
            .context("state-builder NATS connect")?,
    };
    let js = jetstream::new(client.clone());
    let stream = js
        .get_stream(&config.stream)
        .await
        .with_context(|| format!("state-builder get_stream {}", config.stream))?;
    let durable = config.durable.is_some();
    let consumer = stream
        .create_consumer(PullConfig {
            durable_name: config.durable.clone(),
            filter_subject: "noetl.events.>".to_string(),
            deliver_policy: DeliverPolicy::All,
            ack_policy: if durable {
                AckPolicy::Explicit
            } else {
                AckPolicy::None
            },
            ..Default::default()
        })
        .await
        .context("state-builder create_consumer")?;
    Ok((client, consumer))
}

/// (Re)connect with exponential backoff until a consumer is established
/// (noetl/ai-meta#161).  Loops on failure forever — a permanently-down NATS
/// keeps the drain trying rather than exiting the task (a dead task would never
/// recover even after NATS returns).  Each failed attempt bumps
/// `connect_error` and, once the drain has been unable to serve for
/// `unhealthy_after`, flips `/livez` so Kubernetes restarts the pod.  Resets
/// `backoff` to the floor on success.
async fn connect_with_backoff(
    config: &DrainConfig,
    backoff: &mut Duration,
    last_healthy: &Instant,
    unhealthy_after: Duration,
) -> (
    async_nats::Client,
    jetstream::consumer::Consumer<PullConfig>,
) {
    loop {
        match connect_drain_consumer(config).await {
            Ok(pair) => {
                *backoff = REBUILD_BACKOFF_MIN;
                return pair;
            }
            Err(e) => {
                crate::metrics::record_state_builder_consumer_recreate("connect_error");
                if last_healthy.elapsed() >= unhealthy_after {
                    crate::metrics::set_state_builder_healthy(false);
                }
                tracing::warn!(
                    error = %e,
                    backoff_ms = backoff.as_millis() as u64,
                    "state-builder connect/create_consumer failed; backing off then retrying (noetl/ai-meta#161)"
                );
                tokio::time::sleep(*backoff).await;
                *backoff = (*backoff * 2).min(REBUILD_BACKOFF_MAX);
            }
        }
    }
}

/// Connect → consumer (durable authoritative, or ephemeral DeliverAll/AckNone
/// shadow) → drain → index → advance.
///
/// Both shadow and the **default** authoritative drain use an ephemeral
/// `DeliverPolicy::All` + `AckPolicy::None` consumer: it replays the whole
/// retained stream into the index on each start (the cold-rebuild / crash-recovery
/// model — RFC §7.3) and never competes for acks.  This is the noetl/ai-meta#119
/// fix — rebuilding the full index from the retained WAL on every boot means a
/// persisted consumer cursor can never outrun a freshly-restarted worker's empty
/// index (the stall: cursor acked past events a fresh index still needs →
/// `build_spine_to(expected_head)` permanently `Incomplete` → off-server drive
/// loops `offserver_retry`).  Only under `NOETL_STATE_BUILDER_DURABLE` does the
/// authoritative drain use a durable (`AckPolicy::Explicit`) consumer (the
/// pre-#119 shape, an instant revert).  Either way the index is the same — the
/// chain walk + cache produce identical state.
async fn run_drain_loop(config: DrainConfig, index: SharedWalIndex) -> Result<()> {
    // `authoritative` (mode) governs the *advance timing* (advance-on-demand in
    // the command dispatch vs. advance-in-loop for shadow) + terminal eviction.
    // `durable` (a consumer name is configured) governs the *ack policy*: only a
    // durable consumer has a cursor to advance, so it acks; the ephemeral
    // DeliverAll consumer (the #119 default) re-delivers the full retained WAL on
    // every boot and never acks.  The two are now independent — a default
    // authoritative drain is `authoritative && !durable`.
    let authoritative = config.mode == BuilderMode::Authoritative;
    let durable = config.durable.is_some();

    // One-shot rehydration breadcrumb: log the first batch that populates the
    // index after boot, so a restart leaves a clear "index rehydrated from the
    // retained WAL" marker (the #119 stall was a permanently-empty index).
    let mut rehydrated = false;
    // noetl/ai-meta#161 self-heal state.  `backoff` and `last_healthy` persist
    // across consumer rebuilds so a flapping reconnect that never recovers still
    // trips `/livez` after the unhealthy window.  `dead_since` tracks how long
    // the *current* consumer has emitted the dead signal — reset on every
    // successful cycle so a single transient blip never forces a rebuild.
    let mut backoff = REBUILD_BACKOFF_MIN;
    let mut last_healthy = Instant::now();
    let mut dead_since: Option<Instant> = None;
    let unhealthy_after = unhealthy_after();
    let rebuild_after = rebuild_after();
    // noetl/ai-meta#166: the bounded-cache TTL/byte sweep runs on a throttled
    // cadence on BOTH the busy and idle paths.  Idle-path sweeping is the
    // load-bearing case: the system pool at idle (62m CPU, few new events) is
    // exactly when the 654 stuck/abandoned chains must be evicted — gating the
    // sweep behind `consumed > 0` would never reclaim them on a quiet stream.
    // Throttled (default 15s) so the 25ms idle re-poll grid (noetl/ai-meta#130)
    // doesn't spin an O(n) sweep + index lock 40×/sec.
    let mut last_sweep = Instant::now();
    let sweep_interval = Duration::from_secs(env_u64("NOETL_STATE_INDEX_SWEEP_SECS", 15));

    // Initial connect: retry with backoff until NATS is reachable and the
    // consumer is created (a NATS bounce at boot must not kill the drain task).
    let (mut _client, mut consumer) =
        connect_with_backoff(&config, &mut backoff, &last_healthy, unhealthy_after).await;
    last_healthy = Instant::now();
    crate::metrics::set_state_builder_healthy(true);

    tracing::info!(
        stream = %config.stream,
        durable = ?config.durable,
        ephemeral_rebuild = !durable,
        mode = ?config.mode,
        batch = config.batch,
        "off-server state-builder drain started (WAL drain, zero noetl.event scans; \
         rebuilds the in-memory index from the retained WAL on boot — noetl/ai-meta#119; \
         self-heals on NATS consumer loss — noetl/ai-meta#161)"
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
                // noetl/ai-meta#161: a NATS server bounce orphans the pull
                // consumer; the old code hot-looped a `503 "no responders"` storm
                // against the dead consumer forever, the index stopped advancing
                // → `__orchestrate__` saw `commands=0` → every off-server drive
                // (incl. auth login) wedged until a manual `rollout restart`.
                // Now: on a sustained dead signal, tear the consumer down and
                // recreate it (reconnecting NATS) with backoff.
                if is_consumer_dead(&e) {
                    if let DeadAction::Rebuild = on_dead_signal(
                        &mut dead_since,
                        &last_healthy,
                        rebuild_after,
                        unhealthy_after,
                    ) {
                        crate::metrics::record_state_builder_consumer_recreate("drain_dead");
                        crate::metrics::set_state_builder_healthy(false);
                        tracing::warn!(
                            error = %e,
                            "state-builder consumer dead (NATS bounce / orphaned consumer); tearing down + recreating (noetl/ai-meta#161)"
                        );
                        let (c, k) = connect_with_backoff(
                            &config,
                            &mut backoff,
                            &last_healthy,
                            unhealthy_after,
                        )
                        .await;
                        _client = c;
                        consumer = k;
                        last_healthy = Instant::now();
                        dead_since = None;
                        crate::metrics::set_state_builder_healthy(true);
                    } else {
                        tracing::warn!(
                            error = %e,
                            "state-builder consumer error (dead signal); will recreate if sustained (noetl/ai-meta#161)"
                        );
                        tokio::time::sleep(REBUILD_BACKOFF_MIN).await;
                    }
                    continue;
                }
                dead_since = None;
                tracing::warn!(error = %e, "state-builder batch failed; backing off");
                tokio::time::sleep(config.idle_sleep).await;
                continue;
            }
        };

        let mut touched: Vec<i64> = Vec::new();
        let mut consumed = 0u64;
        let mut terminals: Vec<i64> = Vec::new();
        // Set when a dead-consumer signal surfaces mid-drain (noetl/ai-meta#161)
        // so the post-batch handler can decide rebuild-vs-retry once the batch
        // stops; the events consumed before it are already indexed.
        let mut dead_msg: Option<String> = None;
        // noetl/ai-meta#130: apply each message under its OWN short lock and
        // release between messages — do NOT hold the index lock across
        // `batch.next().await`.  The pull is `.expires(timeout_ms)`-bounded, so on
        // an idle stream `batch.next()` blocks for the full expiry (default 2s)
        // waiting for the next message after the last one arrives; holding the
        // lock across that wait pinned the index for ~2s per cycle and stalled the
        // off-server drive's `build_offserver_input` (which blocks on the same
        // lock) — the ~1.8–2.3s/hop in #130.  Parsing happens lock-free; the
        // critical section is now a single `idx.apply`.  Each applied event pulses
        // the append signal immediately, so the drive's build-retry loop wakes the
        // instant the event it needs is indexed.
        while let Some(msg) = batch.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    if is_consumer_dead(&e) {
                        // Dead signal mid-drain — stop draining this batch and let
                        // the post-batch handler decide rebuild vs retry
                        // (noetl/ai-meta#161).
                        dead_msg = Some(e.to_string());
                        break;
                    }
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
                            if durable {
                                let _ = msg.ack().await;
                            }
                            continue;
                        }
                    }
                }
            };
            // Short critical section: just the apply, then drop the lock so the
            // off-server build path can read the index while we await the next
            // message.
            let applied = {
                let mut idx = index.lock().await;
                idx.apply(&payload)
            };
            if let Some((execution_id, _is_new, is_terminal)) = applied {
                if !touched.contains(&execution_id) {
                    touched.push(execution_id);
                }
                if is_terminal {
                    terminals.push(execution_id);
                }
                // Wake any drive build-retry loop waiting on this execution the
                // instant the event is indexed — not after the whole batch drains.
                index.notify_appended();
            }
            // Durable consumer: ack after the event is indexed (at-least-once;
            // re-apply on redelivery is idempotent — same event_id overwrites
            // identical data).  The ephemeral DeliverAll consumer (the #119
            // default) has no cursor to advance and never acks.
            if durable {
                let _ = msg.ack().await;
            }
        }

        // noetl/ai-meta#161: a dead-consumer signal surfaced while draining.
        // Recreate the consumer if it has persisted past `rebuild_after`,
        // otherwise fall through and retry — the events consumed so far this
        // cycle are already indexed.
        if let Some(err) = dead_msg.take() {
            if let DeadAction::Rebuild =
                on_dead_signal(&mut dead_since, &last_healthy, rebuild_after, unhealthy_after)
            {
                crate::metrics::record_state_builder_consumer_recreate("drain_dead");
                crate::metrics::set_state_builder_healthy(false);
                tracing::warn!(
                    error = %err,
                    "state-builder consumer dead mid-drain (NATS bounce / orphaned consumer); recreating (noetl/ai-meta#161)"
                );
                let (c, k) =
                    connect_with_backoff(&config, &mut backoff, &last_healthy, unhealthy_after)
                        .await;
                _client = c;
                consumer = k;
                last_healthy = Instant::now();
                dead_since = None;
                crate::metrics::set_state_builder_healthy(true);
            } else {
                tracing::warn!(
                    error = %err,
                    "state-builder consumer error mid-drain (dead signal); will recreate if sustained (noetl/ai-meta#161)"
                );
                tokio::time::sleep(REBUILD_BACKOFF_MIN).await;
            }
            continue;
        }

        // Successful pull cycle (even an idle 0-message expiry): the consumer is
        // alive and serving — clear the dead timer and refresh health so `/livez`
        // stays green and the unhealthy timer resets (noetl/ai-meta#161).
        dead_since = None;
        last_healthy = Instant::now();
        crate::metrics::set_state_builder_healthy(true);

        // noetl/ai-meta#166: throttled bounded-cache sweep — runs on idle AND
        // busy iterations so idle/abandoned chains are reclaimed even when no new
        // events are arriving (the system-pool-at-idle case).  A no-op when the
        // policy is unbounded.  Also refreshes the resident-set gauges so the
        // OOM-trail observability stays live on a quiet stream.
        if last_sweep.elapsed() >= sweep_interval {
            last_sweep = Instant::now();
            let (indexed, events, bytes) = {
                let mut idx = index.lock().await;
                let evicted = idx.enforce_limits();
                if evicted.total() > 0 {
                    crate::metrics::record_state_builder_eviction("ttl", evicted.ttl);
                    crate::metrics::record_state_builder_eviction(
                        "max_executions",
                        evicted.max_executions,
                    );
                    crate::metrics::record_state_builder_eviction(
                        "byte_ceiling",
                        evicted.byte_ceiling,
                    );
                    tracing::debug!(
                        ttl = evicted.ttl,
                        max_executions = evicted.max_executions,
                        byte_ceiling = evicted.byte_ceiling,
                        resident = idx.execution_count(),
                        bytes = idx.total_bytes(),
                        "state-builder bounded-cache eviction sweep (noetl/ai-meta#166)"
                    );
                }
                (idx.execution_count(), idx.event_count(), idx.total_bytes())
            };
            crate::metrics::set_state_builder_indexed_executions(indexed as i64);
            crate::metrics::set_state_builder_index_events(events as i64);
            crate::metrics::set_state_builder_index_bytes(bytes as i64);
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
        let (indexed, bytes) = {
            let mut idx = index.lock().await;
            // Terminal eviction stays per-batch — the cheap O(terminals) fast
            // path that frees a chain the instant it completes.  The TTL/byte
            // bounded-cache sweep is the throttled periodic pass above.
            for eid in terminals {
                idx.evict(eid);
            }
            (idx.execution_count(), idx.total_bytes())
        };
        // noetl/ai-meta#119 rehydration proof: surface the indexed-execution count
        // (the bug was this stuck at 0 after a restart — the durable cursor outran
        // the empty index).  A non-zero count after boot means the index rebuilt
        // from the retained WAL.  Log the first non-empty rebuild once per process.
        // O(1) gauges refreshed every busy batch; the O(n) event_count gauge is
        // refreshed by the throttled sweep above (noetl/ai-meta#166).
        crate::metrics::set_state_builder_indexed_executions(indexed as i64);
        crate::metrics::set_state_builder_index_bytes(bytes as i64);
        if !rehydrated && indexed > 0 {
            rehydrated = true;
            tracing::info!(
                indexed_executions = indexed,
                wal_events = consumed,
                durable,
                "off-server state-builder index rehydrated from retained noetl_events WAL (noetl/ai-meta#119)"
            );
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

// ── Cold-rebuild on cache miss (noetl/ai-meta#166 §5.2) ──────────────────────
//
// When the bounded cache evicts an execution that is later driven again (a
// callback-resumed block, an ownership change), the live drain won't re-deliver
// that execution's already-consumed events, so its chain can't reach genesis and
// the build is permanently `Incomplete` — a wedge, since the #156 tail-attach
// accelerator is OFF in prod and the drive has no server-built fallback on the
// stateless edge.  This path makes eviction wedge-safe: on a miss it re-reads the
// retained `noetl_events` WAL (bounded — 24h, `discard=old`, ~tens of MiB) with a
// one-shot ephemeral consumer and re-indexes ONLY the missed execution's events,
// then the drive re-attempts the build.  Bounded by a deadline + a message cap;
// gated off by default so it's opt-in alongside eviction.

/// True when `NOETL_STATE_INDEX_REHYDRATE_ON_MISS` is truthy — enables the
/// targeted retained-WAL cold-rebuild on a bounded-cache miss (noetl/ai-meta#166
/// §5.2).  Default off.  Pairs with the eviction knobs: enable both together so
/// an evicted-then-resumed execution self-heals instead of wedging.
pub fn rehydrate_on_miss_enabled() -> bool {
    matches!(
        std::env::var("NOETL_STATE_INDEX_REHYDRATE_ON_MISS")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Resolved config for a targeted WAL cold-rebuild (noetl/ai-meta#166 §5.2).
#[derive(Clone)]
pub struct RehydrateConfig {
    pub nats_url: String,
    pub nats_user: Option<String>,
    pub nats_password: Option<String>,
    pub stream: String,
    pub batch: u32,
    /// Per-pull long-poll expiry — short so an empty pull (caught up) returns
    /// quickly.
    pub poll_ms: u64,
    /// Overall wall-clock budget for one rehydrate (caps the rare miss-path cost
    /// so a cold-rebuild can never become a #130/#156-class latency regression).
    pub deadline_ms: u64,
    /// Hard cap on messages scanned in one rehydrate (belt-and-suspenders against
    /// an unexpectedly large retained stream).
    pub max_messages: u64,
}

impl RehydrateConfig {
    pub fn from_env(nats_url: &str) -> Self {
        let (nats_url, nats_user, nats_password) = parse_nats_credentials(nats_url);
        Self {
            nats_url,
            nats_user,
            nats_password,
            stream: std::env::var("NOETL_STATE_BUILDER_STREAM")
                .unwrap_or_else(|_| EVENT_STREAM.to_string()),
            batch: env_u32("NOETL_STATE_INDEX_REHYDRATE_BATCH", 500).clamp(1, 2000),
            poll_ms: env_u64("NOETL_STATE_INDEX_REHYDRATE_POLL_MS", 300).clamp(50, 5_000),
            deadline_ms: env_u64("NOETL_STATE_INDEX_REHYDRATE_DEADLINE_MS", 3_000).clamp(100, 30_000),
            max_messages: env_u64("NOETL_STATE_INDEX_REHYDRATE_MAX_MESSAGES", 100_000),
        }
    }
}

/// Targeted retained-WAL cold-rebuild for ONE execution (noetl/ai-meta#166 §5.2).
/// Opens a one-shot ephemeral `DeliverPolicy::All` / `AckPolicy::None` consumer on
/// the `noetl_events` stream, drains the retained window applying **only**
/// `execution_id`'s events into the shared index, and returns the count applied.
/// Bounded by `deadline_ms` + `max_messages`; any NATS error returns `0` so the
/// caller just falls back exactly as today (the rehydrate can never make a miss
/// worse than the pre-existing fallback — it only ADDS events to the index).
///
/// Filtering to the single execution is what keeps this from re-bloating the
/// index back to the full 24h window: a generic replay would re-index everything
/// the eviction just freed.
pub async fn rehydrate_execution_from_wal(
    cfg: &RehydrateConfig,
    index: &SharedWalIndex,
    execution_id: i64,
) -> usize {
    let deadline = Instant::now() + Duration::from_millis(cfg.deadline_ms);
    let connect = async {
        let client = match (&cfg.nats_user, &cfg.nats_password) {
            (Some(u), Some(p)) => {
                ConnectOptions::with_user_and_password(u.clone(), p.clone())
                    .connect(&cfg.nats_url)
                    .await
            }
            _ => async_nats::connect(&cfg.nats_url).await,
        }?;
        let js = jetstream::new(client.clone());
        let stream = js.get_stream(&cfg.stream).await?;
        let consumer = stream
            .create_consumer(PullConfig {
                durable_name: None,
                filter_subject: "noetl.events.>".to_string(),
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::None,
                ..Default::default()
            })
            .await?;
        Ok::<_, async_nats::Error>((client, consumer))
    };
    let (_client, consumer) = match connect.await {
        Ok(pair) => pair,
        Err(e) => {
            tracing::warn!(execution_id, error = %e, "state-builder cold-rebuild connect failed (noetl/ai-meta#166)");
            return 0;
        }
    };

    let mut applied = 0usize;
    let mut scanned = 0u64;
    while Instant::now() < deadline && scanned < cfg.max_messages {
        let mut batch = match consumer
            .batch()
            .max_messages(cfg.batch as usize)
            .expires(Duration::from_millis(cfg.poll_ms))
            .messages()
            .await
        {
            Ok(b) => b,
            Err(_) => break,
        };
        let mut got = 0u64;
        while let Some(Ok(msg)) = batch.next().await {
            got += 1;
            scanned += 1;
            let payload: serde_json::Value = match serde_json::from_slice(&msg.payload) {
                Ok(v) => v,
                Err(_) => match std::str::from_utf8(&msg.payload)
                    .ok()
                    .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                {
                    Some(v) => v,
                    None => continue,
                },
            };
            // Only the missed execution's events — keep the index from re-growing.
            if payload.get("execution_id").and_then(|v| v.as_i64()) != Some(execution_id) {
                continue;
            }
            {
                let mut idx = index.lock().await;
                idx.apply(&payload);
            }
            applied += 1;
            if scanned >= cfg.max_messages {
                break;
            }
        }
        // An empty pull means the retained window is fully scanned (caught up).
        if got == 0 {
            break;
        }
    }
    applied
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- noetl/ai-meta#161 self-heal helpers ----

    #[test]
    fn is_consumer_dead_matches_orphaned_signatures() {
        // The exact prod fingerprint from the #161 outage.
        assert!(is_consumer_dead(&anyhow::anyhow!(
            "error while processing messages from the stream: 503, None"
        )));
        assert!(is_consumer_dead(&"no responders available for request"));
        assert!(is_consumer_dead(&"consumer not found"));
        assert!(is_consumer_dead(&"consumer deleted"));
        assert!(is_consumer_dead(&"503 Service Unavailable"));
    }

    #[test]
    fn is_consumer_dead_ignores_transient_errors() {
        // A normal idle timeout / transient I/O blip must NOT be treated as a
        // dead consumer — those recover on the next pull without a rebuild.
        assert!(!is_consumer_dead(&"deadline exceeded"));
        assert!(!is_consumer_dead(&"connection reset by peer"));
        assert!(!is_consumer_dead(&"timed out waiting for messages"));
        assert!(!is_consumer_dead(&"broken pipe"));
    }

    #[test]
    fn on_dead_signal_retries_until_sustained_then_rebuilds() {
        let rebuild_after = Duration::from_millis(50);
        let unhealthy_after = Duration::from_secs(60);
        let last_healthy = Instant::now();
        let mut dead_since: Option<Instant> = None;

        // First observation arms the timer and asks to retry, not rebuild.
        assert!(matches!(
            on_dead_signal(&mut dead_since, &last_healthy, rebuild_after, unhealthy_after),
            DeadAction::Retry
        ));
        assert!(dead_since.is_some());

        // After the dead signal has persisted past `rebuild_after`, it rebuilds.
        std::thread::sleep(Duration::from_millis(60));
        assert!(matches!(
            on_dead_signal(&mut dead_since, &last_healthy, rebuild_after, unhealthy_after),
            DeadAction::Rebuild
        ));
    }

    #[test]
    fn on_dead_signal_flips_unhealthy_past_window() {
        // last_healthy already past the unhealthy window → the retry path flips
        // the health gauge to false so /livez fails (backstop restart).
        let rebuild_after = Duration::from_secs(60); // never rebuild in this test
        // Zero window → already past it on the first observation (avoids
        // `Instant - Duration` underflow on freshly-booted hosts).
        let unhealthy_after = Duration::ZERO;
        let last_healthy = Instant::now();
        let mut dead_since: Option<Instant> = None;

        crate::metrics::set_state_builder_healthy(true);
        assert!(matches!(
            on_dead_signal(&mut dead_since, &last_healthy, rebuild_after, unhealthy_after),
            DeadAction::Retry
        ));
        assert!(!crate::metrics::state_builder_healthy());
        // Restore the gauge so other tests in the binary see the default.
        crate::metrics::set_state_builder_healthy(true);
    }

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
        // chain walk collects head→root then reverses to causal (root→head) order
        // (#117).  For a MONOTONIC chain causal order == event-scan ORDER BY
        // event_id ASC, so the result must equal `1..=6` — the identical sequence
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
    fn fresh_index_rebuilds_from_full_replay_after_restart() {
        // noetl/ai-meta#119: a worker restart drops the in-memory index.  The
        // pre-#119 durable consumer cursor persisted PAST the events a fresh index
        // needed, so the empty index was never repopulated → build_spine_to was
        // permanently Incomplete → the off-server drive looped offserver_retry and
        // executions never completed.  The fix re-delivers the FULL retained WAL
        // into a FRESH index on every boot (ephemeral DeliverAll), so the rebuilt
        // index serves the same complete spine the pre-restart index did.
        let retained = linear_spine(5); // playbook_started … command.completed (id 5)
        let tip = 5; // the server's expected_head (ChainHeads watermark)

        // Pre-restart: the index serves the complete spine to the tip.
        let mut before = WalEventIndex::new();
        for e in &retained {
            before.apply(e);
        }
        let (o1, s1) = before.build_spine_to(42, tip);
        assert!(matches!(o1, AdvanceOutcome::ColdRebuild(5)));
        let ids_before: Vec<i64> = s1
            .expect("pre-restart index serves the spine")
            .iter()
            .map(|e| e["event_id"].as_i64().unwrap())
            .collect();

        // Restart: a brand-new empty index can't serve — the #119 stall symptom
        // (build Incomplete, the drive would loop offserver_retry forever).
        let mut after = WalEventIndex::new();
        let (o_empty, s_empty) = after.build_spine_to(42, tip);
        assert!(matches!(o_empty, AdvanceOutcome::Incomplete));
        assert!(s_empty.is_none(), "empty post-restart index stalls (the bug)");

        // Rehydrate: replay the same retained WAL into the fresh index → it serves
        // the identical complete spine again (the fix — full DeliverAll replay).
        for e in &retained {
            after.apply(e);
        }
        let (o2, s2) = after.build_spine_to(42, tip);
        assert!(matches!(o2, AdvanceOutcome::ColdRebuild(5)));
        let ids_after: Vec<i64> = s2
            .expect("rehydrated index serves the spine")
            .iter()
            .map(|e| e["event_id"].as_i64().unwrap())
            .collect();
        assert_eq!(
            ids_before, ids_after,
            "rehydrated index serves the same spine as before the restart"
        );
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
        // A complete chain → build_spine returns the raw payloads in causal order
        // (== event_id order for this monotonic chain) — the
        // OrchestrateInput.events the wasm `run` entry consumes.
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

    /// A fan-out reduce chain carrying a noetl/ai-meta#117 id inversion:
    /// causal order `1 → 2 → 3 → 100 → 50`, where the LAST-arrived branch
    /// completion (`50`, enrich.completed) carries a LOWER producer-assigned
    /// `event_id` than its predecessor (`100`, normalize.completed) — the two
    /// branches' snowflakes come from different workers and arrive at the owner
    /// reordered relative to their ids.  The real causal tip is `50` (the server's
    /// `ChainHeads` watermark, `link_batch`-advanced to the last-arrived id), but
    /// `max(event_id)` is `100`.
    fn inverted_fanout_spine() -> Vec<serde_json::Value> {
        vec![
            ev(1, None, "playbook_started"),
            ev(2, Some(1), "command.issued"),   // normalize dispatch
            ev(3, Some(2), "command.issued"),   // enrich dispatch
            ev(100, Some(3), "command.completed"), // normalize.completed (1st, high id)
            ev(50, Some(100), "command.completed"), // enrich.completed (2nd, LOW id) — inversion
        ]
    }

    #[test]
    fn fanout_id_inversion_walks_from_tip_not_max_id() {
        // The crux of #117: walking from the max-id head MISSES the inverted tip
        // (50 is a child of 100, not on the 100→root path), so from_events never
        // sees enrich.completed and the fan-in reduce never fires — the wedge.
        let chain = chain_from(&inverted_fanout_spine());
        let from_max = chain.chain_walk().expect("genesis-rooted");
        assert_eq!(
            from_max,
            vec![1, 2, 3, 100],
            "max-id walk drops the inverted tip 50"
        );
        assert!(
            !from_max.contains(&50),
            "enrich.completed (50) is invisible to the max-id walk — the wedge"
        );
        // Walking from the real tip (expected_head = 50) reaches every event in
        // true causal order, enrich.completed LAST → both branches now visible.
        let from_tip = chain.chain_walk_from(50).expect("genesis-rooted from tip");
        assert_eq!(
            from_tip,
            vec![1, 2, 3, 100, 50],
            "tip-rooted walk = full causal order, inverted tip included"
        );
    }

    #[test]
    fn build_spine_to_serves_inverted_tip_in_causal_order() {
        // build_spine_to(expected_head) serves the full causal spine through the
        // inversion; build_spine (max-id) drops the tip.
        let mut idx_max = WalEventIndex::new();
        for e in inverted_fanout_spine() {
            idx_max.apply(&e);
        }
        let (_, sp_max) = idx_max.build_spine(42);
        let ids_max: Vec<i64> = sp_max
            .expect("genesis-rooted")
            .iter()
            .map(|e| e["event_id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids_max, vec![1, 2, 3, 100], "max-id build drops the tip");

        let mut idx_tip = WalEventIndex::new();
        for e in inverted_fanout_spine() {
            idx_tip.apply(&e);
        }
        let (outcome, sp_tip) = idx_tip.build_spine_to(42, 50);
        assert!(matches!(
            outcome,
            AdvanceOutcome::ColdRebuild(5) | AdvanceOutcome::Incremental(_)
        ));
        let ids_tip: Vec<i64> = sp_tip
            .expect("tip-rooted spine")
            .iter()
            .map(|e| e["event_id"].as_i64().unwrap())
            .collect();
        assert_eq!(
            ids_tip,
            vec![1, 2, 3, 100, 50],
            "tip-rooted build serves the full causal spine"
        );
    }

    #[test]
    fn advance_to_incomplete_until_tip_indexed() {
        // The staleness guard is intrinsic to advance_to: when the server's
        // watermark names a tip (50) the worker hasn't drained yet, the build is
        // Incomplete (the drive waits / falls back), never serving a spine that's
        // missing the tip.  Pre-#117 the `max_id >= expected` guard would have
        // PASSED here (max_id 100 >= 50) and served a tip-less spine.
        let mut idx = WalEventIndex::new();
        idx.apply(&ev(1, None, "playbook_started"));
        idx.apply(&ev(2, Some(1), "command.issued"));
        idx.apply(&ev(3, Some(2), "command.issued"));
        idx.apply(&ev(100, Some(3), "command.completed"));
        let (outcome, spine) = idx.build_spine_to(42, 50);
        assert!(matches!(outcome, AdvanceOutcome::Incomplete));
        assert!(spine.is_none(), "tip 50 not drained yet → no spine");
        // Once 50 arrives, the tip-rooted build serves the full causal spine.
        idx.apply(&ev(50, Some(100), "command.completed"));
        let (_, spine) = idx.build_spine_to(42, 50);
        let ids: Vec<i64> = spine
            .expect("tip now indexed")
            .iter()
            .map(|e| e["event_id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 100, 50]);
    }

    #[test]
    fn legacy_event_id_order_revert_resorts_inverted_pair() {
        // NOETL_OFFSERVER_SPINE_ORDER=event_id restores the pre-#117 sort: even
        // walking from the real tip, it re-sorts the collected ids ascending, so
        // the inverted pair (100, 50) is replayed as 50-before-100 — enrich before
        // its predecessor normalize — the ordering that wedges fan-in.
        let mut legacy = WalEventIndex::with_order(SpineOrder::EventId);
        for e in inverted_fanout_spine() {
            legacy.apply(&e);
        }
        let (_, sp) = legacy.build_spine_to(42, 50);
        let ids: Vec<i64> = sp
            .expect("genesis-rooted")
            .iter()
            .map(|e| e["event_id"].as_i64().unwrap())
            .collect();
        assert_eq!(
            ids,
            vec![1, 2, 3, 50, 100],
            "legacy event_id sort replays the inverted pair out of causal order"
        );
        // The causal default preserves the chain order (the #117 fix).
        let mut causal = WalEventIndex::with_order(SpineOrder::Causal);
        for e in inverted_fanout_spine() {
            causal.apply(&e);
        }
        let (_, sp2) = causal.build_spine_to(42, 50);
        let ids2: Vec<i64> = sp2
            .expect("genesis-rooted")
            .iter()
            .map(|e| e["event_id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids2, vec![1, 2, 3, 100, 50], "causal order preserves the chain");
    }

    #[test]
    fn incremental_advance_to_equals_cold_rebuild_through_inversion() {
        // The incremental-equals-cold-rebuild invariant (RFC §5.2) holds through a
        // #117 inversion: advancing the cached spine from max-id (100) to the real
        // tip (50) by a tail walk yields the same spine a cold rebuild from 50 does.
        let mut incr = WalEventIndex::new();
        for e in inverted_fanout_spine() {
            incr.apply(&e);
        }
        // Seed the cache at the max-id head (100), then advance to the tip (50).
        let _ = incr.build_spine(42); // cache keyed at 100 → [1,2,3,100]
        let (outcome, sp_incr) = incr.build_spine_to(42, 50);
        assert!(
            matches!(outcome, AdvanceOutcome::Incremental(1)),
            "tip 50 extends cached 100 by one tail hop"
        );
        let ids_incr: Vec<i64> = sp_incr
            .unwrap()
            .iter()
            .map(|e| e["event_id"].as_i64().unwrap())
            .collect();

        let mut cold = WalEventIndex::new();
        for e in inverted_fanout_spine() {
            cold.apply(&e);
        }
        let (cold_outcome, sp_cold) = cold.build_spine_to(42, 50);
        assert!(matches!(cold_outcome, AdvanceOutcome::ColdRebuild(5)));
        let ids_cold: Vec<i64> = sp_cold
            .unwrap()
            .iter()
            .map(|e| e["event_id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids_incr, ids_cold, "incremental == cold rebuild through inversion");
        assert_eq!(ids_incr, vec![1, 2, 3, 100, 50]);
    }

    #[tokio::test]
    async fn build_offserver_input_assembles_run_input() {
        // The off-server drive build assembles the exact OrchestrateInput shape
        // the wasm `run` (from_events) entry decodes: { events, playbook,
        // trigger_event_type }.
        let index: SharedWalIndex = SharedWalIndex::new(WalEventIndex::new());
        {
            let mut idx = index.lock().await;
            for e in linear_spine(3) {
                idx.apply(&e);
            }
        }
        let playbook = serde_json::json!({ "metadata": { "path": "t" } });
        let bytes = build_offserver_input(
            &index,
            42,
            &playbook,
            Some("command.completed"),
            None,
            Some(3),
            true,
        )
        .await
        .expect("complete chain builds input");
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["trigger_event_type"], "command.completed");
        assert_eq!(v["playbook"]["metadata"]["path"], "t");
        // RFC #115 Phase 5: the atomic-item-context flag is forwarded onto the
        // from_events drive input.
        assert_eq!(v["atomic_item_context"], true);
        let evs = v["events"].as_array().unwrap();
        assert_eq!(evs.len(), 3, "all three spine events");
        assert_eq!(evs[0]["event_id"], 1);

        // Stateless edge (RFC #115 Phase 4 remainder): trigger_event_type=None +
        // a trigger_event_id resolves the type off the WAL index (event 3 is
        // command.completed in the linear spine).
        let bytes2 = build_offserver_input(&index, 42, &playbook, None, Some(3), Some(3), false)
            .await
            .expect("complete chain builds input (resolved trigger type)");
        let v2: serde_json::Value = serde_json::from_slice(&bytes2).unwrap();
        assert_eq!(
            v2["trigger_event_type"], "command.completed",
            "trigger type resolved off the WAL index from trigger_event_id"
        );
        // An unindexed trigger_event_id defaults to command.completed.
        let bytes3 = build_offserver_input(&index, 42, &playbook, None, Some(999), Some(3), false)
            .await
            .expect("complete chain builds input (default trigger type)");
        let v3: serde_json::Value = serde_json::from_slice(&bytes3).unwrap();
        assert_eq!(v3["trigger_event_type"], "command.completed");

        // Staleness guard: an expected_head ahead of the index head → None (the
        // drain hasn't caught up to the server's dispatch watermark yet).
        assert!(
            build_offserver_input(&index, 42, &playbook, None, None, Some(99), false).await.is_none(),
            "stale index (head < expected_head) must not serve"
        );

        // An unknown execution → None (the caller falls back to run_state).
        assert!(build_offserver_input(&index, 7, &playbook, None, None, None, false).await.is_none());
    }

    #[tokio::test]
    async fn append_signal_wakes_registered_waiter() {
        // noetl/ai-meta#130: the drain's append signal must wake a waiter that
        // registered interest (enable-before-check) — the primitive the
        // off-server drive's build-retry loop parks on instead of fixed polling.
        let index = SharedWalIndex::new(WalEventIndex::new());
        let appended = index.appended();
        let notified = appended.notified();
        tokio::pin!(notified);
        // Register BEFORE the (would-be) build check.
        notified.as_mut().enable();
        // A pulse from the drain side wakes the registered waiter promptly.
        index.notify_appended();
        tokio::time::timeout(std::time::Duration::from_secs(1), notified)
            .await
            .expect("notify_appended must wake a registered waiter");
    }

    #[tokio::test]
    async fn reader_is_not_starved_while_index_is_being_fed() {
        // noetl/ai-meta#130 regression guard: the off-server build path must be
        // able to acquire the index lock promptly even while the drain is feeding
        // events.  The old drain held the lock across the whole `batch.next()`
        // wait (~2s on an idle stream); this proves a reader interleaves with a
        // feeder that applies + releases per event.  If a regression reintroduces
        // a long-held lock on the feeder side, the reader's `lock()` below would
        // block past the generous timeout and the test fails.
        let index = SharedWalIndex::new(WalEventIndex::new());
        let feeder = index.clone();
        let spine = linear_spine(3);
        let feed = tokio::spawn(async move {
            for e in &spine {
                {
                    let mut idx = feeder.lock().await; // short critical section
                    idx.apply(e);
                }
                feeder.notify_appended();
                // Simulate the idle gap between WAL messages WITHOUT holding the
                // lock (the fixed behaviour); the reader must win the lock here.
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        });
        // While the feeder is mid-stream, the reader acquires the lock repeatedly
        // with a tight timeout — proving it is never pinned out for ~2s.
        for _ in 0..5 {
            tokio::time::timeout(std::time::Duration::from_millis(100), async {
                let _g = index.lock().await;
            })
            .await
            .expect("reader must acquire the index lock without being starved");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        feed.await.unwrap();
    }

    #[tokio::test]
    async fn drive_build_wakes_on_append_signal() {
        // End-to-end at the state-builder layer: a waiter mirroring the
        // off-server drive's build-retry loop (enable → build → park on the
        // append signal) advances the instant the drain indexes the events it
        // needs, NOT on a fixed poll grid.  Staged applies + pulses prove the
        // loop re-checks on each append and only serves once the chain reaches
        // `expected_head` (the staleness guard stays intact).
        let index = SharedWalIndex::new(WalEventIndex::new());
        let playbook = serde_json::json!({ "metadata": { "path": "t" } });
        let spine = linear_spine(3); // events 1,2,3 for execution 42

        let waiter_index = index.clone();
        let waiter_playbook = playbook.clone();
        let waiter = tokio::spawn(async move {
            let appended = waiter_index.appended();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let notified = appended.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Some(bytes) = build_offserver_input(
                    &waiter_index,
                    42,
                    &waiter_playbook,
                    Some("command.completed"),
                    None,
                    Some(3),
                    false,
                )
                .await
                {
                    return bytes;
                }
                assert!(std::time::Instant::now() < deadline, "waiter timed out");
                // Park on the next drain append (cap well above the staging gap).
                let _ = tokio::time::timeout(std::time::Duration::from_secs(1), notified).await;
            }
        });

        // Stage the spine one event at a time, pulsing after each — the waiter
        // wakes on each pulse, finds the chain still short of expected_head=3, and
        // re-parks, until the final event lets the build serve.
        for e in &spine {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            {
                let mut idx = index.lock().await;
                idx.apply(e);
            }
            index.notify_appended();
        }

        let bytes = tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("waiter resolves before the timeout")
            .expect("waiter task did not panic");
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["events"].as_array().unwrap().len(), 3, "served the full spine");
        assert_eq!(v["trigger_event_type"], "command.completed");
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

    // ---- noetl/ai-meta#166 Phase 1: slim chain + bounded cache ----

    /// A fat envelope: the Event-relevant fields PLUS the non-Event fields a real
    /// `noetl_events` envelope carries (`node_id`, `node_type`, `duration`,
    /// `stack_trace`, `trace_component`, `parent_event_id`, an `extra` blob).
    fn fat_event(event_id: i64, prev: Option<i64>, event_type: &str, bulk: usize) -> serde_json::Value {
        serde_json::json!({
            "event_id": event_id,
            "execution_id": 42,
            "prev_event_id": prev,
            "event_type": event_type,
            "node_name": "step_a",
            "status": "completed",
            "created_at": "2026-06-30T00:00:00Z",
            "result": { "data": "r".repeat(bulk) },   // Event field — KEPT (load-bearing)
            "context": { "workload": {"k": "v"} },     // Event field — KEPT
            // Non-Event envelope fields — dropped by from_events' serde anyway:
            "node_id": "n".repeat(bulk),
            "node_type": "task",
            "duration": 1.23,
            "stack_trace": "s".repeat(bulk),
            "trace_component": "worker",
            "parent_event_id": 7,
            "some_extra_blob": "x".repeat(bulk),
        })
    }

    #[test]
    fn slim_projection_keeps_event_fields_drops_the_rest() {
        let full = fat_event(2, Some(1), "command.completed", 100);
        let slim = slim_event_payload(&full);
        let obj = slim.as_object().unwrap();
        // Every Event-deserialized field survives…
        for k in ["event_id", "execution_id", "event_type", "node_name", "status", "result", "context", "created_at"] {
            assert!(obj.contains_key(k), "slim must keep Event field {k}");
        }
        // …and the non-Event envelope fields are gone.
        for k in ["node_id", "node_type", "duration", "stack_trace", "trace_component", "parent_event_id", "some_extra_blob", "prev_event_id"] {
            assert!(!obj.contains_key(k), "slim must drop non-Event field {k}");
        }
        // Output-equivalence by construction: the kept body fields are byte-identical.
        assert_eq!(obj.get("result"), full.get("result"));
        assert_eq!(obj.get("context"), full.get("context"));
        // And the slim payload is strictly smaller.
        assert!(approx_json_bytes(&slim) < approx_json_bytes(&full));
    }

    #[test]
    fn slim_index_builds_identical_spine_to_full() {
        // The drive sees the SAME ordered Event spine whether the index stored the
        // full envelope or the slim projection — output-equivalence at the index
        // boundary (the from_events input is identical for the kept fields).
        let events = vec![
            fat_event(1, None, "playbook_started", 50),
            fat_event(2, Some(1), "command.issued", 50),
            fat_event(3, Some(2), "command.completed", 50),
        ];

        let mut full = WalEventIndex::new();
        for e in &events {
            full.apply(e);
        }
        let (_, full_spine) = full.build_spine_to(42, 3);

        let mut slim = WalEventIndex::with_order_policy(
            SpineOrder::Causal,
            EvictionPolicy { slim: true, ..Default::default() },
        );
        for e in &events {
            slim.apply(e);
        }
        let (_, slim_spine) = slim.build_spine_to(42, 3);

        let full_spine = full_spine.expect("full spine");
        let slim_spine = slim_spine.expect("slim spine");
        // Same ids in the same causal order.
        let ids = |s: &[serde_json::Value]| -> Vec<i64> {
            s.iter().map(|e| e["event_id"].as_i64().unwrap()).collect()
        };
        assert_eq!(ids(&full_spine), ids(&slim_spine));
        // The kept Event fields are byte-identical on the slim spine; the slim
        // index holds strictly fewer bytes.
        for (f, s) in full_spine.iter().zip(slim_spine.iter()) {
            assert_eq!(f["result"], s["result"]);
            assert_eq!(f["context"], s["context"]);
            assert_eq!(f["event_type"], s["event_type"]);
        }
        assert!(slim.total_bytes() < full.total_bytes(), "slim index is smaller");
    }

    #[test]
    fn byte_ledger_tracks_apply_overwrite_and_evict() {
        let mut idx = WalEventIndex::new();
        assert_eq!(idx.total_bytes(), 0);
        idx.apply(&fat_event(1, None, "playbook_started", 100));
        let after_one = idx.total_bytes();
        assert!(after_one > 0);
        // Redelivery (same event_id) is an overwrite, not a double-count.
        idx.apply(&fat_event(1, None, "playbook_started", 100));
        assert_eq!(idx.total_bytes(), after_one, "redelivery must not double-count bytes");
        // A second execution adds bytes; evicting it returns to the prior total.
        idx.apply(&serde_json::json!({"event_id": 9, "execution_id": 77, "event_type": "playbook_started"}));
        assert!(idx.total_bytes() > after_one);
        idx.evict(77);
        assert_eq!(idx.total_bytes(), after_one, "evict must subtract the chain's bytes");
        assert_eq!(idx.execution_count(), 1);
    }

    #[test]
    fn ttl_eviction_sweeps_idle_keeps_active() {
        // The cure for the 654 idle/abandoned executions: TTL evicts a chain not
        // touched within the TTL, while an active (recently-touched) one survives.
        let ttl = Duration::from_secs(900);
        let mut idx = WalEventIndex::with_order_policy(
            SpineOrder::Causal,
            EvictionPolicy { ttl: Some(ttl), ..Default::default() },
        );
        let t0 = Instant::now();
        // Two executions applied at t0.
        idx.apply_at(&serde_json::json!({"event_id": 1, "execution_id": 100, "event_type": "playbook_started"}), t0);
        idx.apply_at(&serde_json::json!({"event_id": 2, "execution_id": 200, "event_type": "playbook_started"}), t0);
        assert_eq!(idx.execution_count(), 2);
        // 200 stays active (driven just before the sweep); 100 goes idle.
        let sweep = t0 + ttl + Duration::from_secs(1);
        idx.touch_at(200, sweep - Duration::from_secs(1));
        let stats = idx.enforce_limits_at(sweep);
        assert_eq!(stats.ttl, 1, "exactly the idle chain evicted");
        assert_eq!(stats.total(), 1);
        assert!(idx.chain(100).is_none(), "idle chain evicted");
        assert!(idx.chain(200).is_some(), "active chain survives TTL");
    }

    #[test]
    fn byte_ceiling_evicts_lru_until_under() {
        // The hard bounded-memory guarantee: evict least-recently-active chains
        // until resident bytes are under the ceiling.
        let mut idx = WalEventIndex::with_order_policy(SpineOrder::Causal, EvictionPolicy::default());
        let t0 = Instant::now();
        // Three executions, applied oldest→newest, each ~same size.
        for (i, eid) in [100, 200, 300].into_iter().enumerate() {
            idx.apply_at(
                &serde_json::json!({"event_id": eid, "execution_id": eid, "event_type": "playbook_started", "result": {"d": "z".repeat(200)}}),
                t0 + Duration::from_secs(i as u64),
            );
        }
        let one = idx.total_bytes() / 3;
        // Set a ceiling that fits ~2 of the 3 chains.
        idx.set_policy_for_test(EvictionPolicy { max_bytes: Some(one * 2 + one / 2), ..Default::default() });
        let stats = idx.enforce_limits_at(t0 + Duration::from_secs(10));
        assert!(stats.byte_ceiling >= 1);
        assert!(idx.total_bytes() <= one * 2 + one / 2, "resident set held under the ceiling");
        // The LRU (oldest, execution 100) was the first evicted.
        assert!(idx.chain(100).is_none(), "LRU victim evicted first");
        assert!(idx.chain(300).is_some(), "most-recently-active survives");
    }

    #[test]
    fn max_executions_evicts_lru() {
        let mut idx = WalEventIndex::with_order_policy(
            SpineOrder::Causal,
            EvictionPolicy { max_executions: Some(2), ..Default::default() },
        );
        let t0 = Instant::now();
        for (i, eid) in [100, 200, 300].into_iter().enumerate() {
            idx.apply_at(
                &serde_json::json!({"event_id": eid, "execution_id": eid, "event_type": "playbook_started"}),
                t0 + Duration::from_secs(i as u64),
            );
        }
        let stats = idx.enforce_limits_at(t0 + Duration::from_secs(10));
        assert_eq!(stats.max_executions, 1);
        assert_eq!(idx.execution_count(), 2);
        assert!(idx.chain(100).is_none(), "oldest evicted to honor the cap");
    }

    #[test]
    fn unbounded_policy_never_evicts() {
        // Behaviour-neutral default: with no bounds set, enforce_limits is a no-op
        // even for ancient chains (today's behaviour — only terminal eviction).
        let mut idx = WalEventIndex::new();
        let t0 = Instant::now();
        idx.apply_at(&serde_json::json!({"event_id": 1, "execution_id": 100, "event_type": "playbook_started"}), t0);
        let stats = idx.enforce_limits_at(t0 + Duration::from_secs(86_400));
        assert_eq!(stats.total(), 0);
        assert_eq!(idx.execution_count(), 1);
    }

    #[test]
    fn cold_rebuild_after_eviction_reindexes_from_replayed_events() {
        // Models cold-rebuild-on-miss (§5.2) at the index layer: an execution is
        // evicted (cache miss), then its events are re-applied (the targeted WAL
        // re-replay) and the spine builds again — identical to before eviction.
        let mut idx = WalEventIndex::new();
        let events = linear_spine(4);
        for e in &events {
            idx.apply(e);
        }
        let (_, before) = idx.build_spine_to(42, 4);
        let before = before.expect("pre-eviction spine");
        // Evict (bounded-cache miss).
        idx.evict(42);
        let (miss, none) = idx.build_spine_to(42, 4);
        assert!(matches!(miss, AdvanceOutcome::Incomplete));
        assert!(none.is_none(), "evicted execution misses");
        // Re-replay the retained events for it (what rehydrate_execution_from_wal
        // does over NATS) → the spine rebuilds identically.
        for e in &events {
            idx.apply(e);
        }
        let (_, after) = idx.build_spine_to(42, 4);
        let after = after.expect("rehydrated spine");
        let ids = |s: &[serde_json::Value]| -> Vec<i64> {
            s.iter().map(|e| e["event_id"].as_i64().unwrap()).collect()
        };
        assert_eq!(ids(&before), ids(&after), "cold-rebuild reproduces the same spine");
    }

    #[test]
    fn policy_from_env_defaults_unbounded() {
        // With none of the knobs set the policy bounds nothing (behaviour-neutral).
        let p = EvictionPolicy::default();
        assert!(!p.bounds_memory());
        assert!(p.max_bytes.is_none() && p.ttl.is_none() && p.max_executions.is_none() && !p.slim);
    }
}
