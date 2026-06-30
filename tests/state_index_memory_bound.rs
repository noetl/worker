//! noetl/ai-meta#166 Phase 1 — memory-bound proof for the off-server WAL index.
//!
//! Reproduces the system-pool OOM shape at scale (many executions × many
//! events × large envelopes) and proves the bounded-cache policy turns the
//! `O(all non-terminal history)` resident set into `O(active working set)`:
//!
//! - **Unbounded** (today's default) — resident bytes grow linearly with the
//!   number of indexed executions; nothing trims non-terminal chains.
//! - **Byte-ceiling** — resident bytes stay at or under the configured ceiling
//!   regardless of how many executions are applied (the bounded-memory
//!   guarantee).
//! - **TTL** — idle/abandoned executions (the 654 pinned at prod idle) are
//!   swept while a recently-driven working set survives.
//!
//! The drive correctness / slim output-equivalence / cold-rebuild properties
//! are covered by the unit tests in `src/state_builder.rs`; this is the
//! resident-memory curve the kind/prod rollout is sized against.

use std::time::{Duration, Instant};

use noetl_worker::state_builder::{EvictionPolicy, SpineOrder, WalEventIndex};

/// A realistic-ish `noetl_events` envelope: the chain fields + a result/context
/// body sized like a planner turn (the events that dominate the ~73 KiB mean).
fn envelope(execution_id: i64, event_id: i64, prev: Option<i64>, ty: &str, body: usize) -> serde_json::Value {
    serde_json::json!({
        "event_id": event_id,
        "execution_id": execution_id,
        "prev_event_id": prev,
        "event_type": ty,
        "node_name": "planner_step",
        "status": "completed",
        "created_at": "2026-06-30T00:00:00Z",
        "result": { "data": "r".repeat(body) },
        "context": { "workload": { "slot_state": "s".repeat(body / 2) } },
        // Non-Event envelope fields the slim projection drops:
        "node_id": "node-abcdef",
        "node_type": "task",
        "duration": 1.5,
        "stack_trace": serde_json::Value::Null,
        "trace_component": "worker",
        "parent_event_id": prev,
    })
}

/// Apply `executions` executions, each a `events_per`-event genesis-rooted chain
/// with `body`-byte result/context bodies, at evenly-spaced access times.
fn load(index: &mut WalEventIndex, executions: i64, events_per: i64, body: usize, base: Instant) {
    for e in 0..executions {
        let exec_id = 1000 + e;
        let t = base + Duration::from_secs(e as u64);
        let mut eid = e * 100_000 + 1;
        index.apply_at(&envelope(exec_id, eid, None, "playbook_started", body), t);
        for _ in 1..events_per {
            let prev = eid;
            eid += 1;
            index.apply_at(&envelope(exec_id, eid, Some(prev), "command.completed", body), t);
        }
    }
}

#[test]
fn unbounded_index_grows_linearly_bounded_index_holds_the_ceiling() {
    const EXECUTIONS: i64 = 500;
    const EVENTS_PER: i64 = 27; // the prod mean
    const BODY: usize = 2_000; // ~few-KiB bodies (kept modest so the test is fast)
    let base = Instant::now();

    // --- BEFORE: unbounded (today's behaviour) — grows O(executions). ---
    let mut unbounded = WalEventIndex::new();
    load(&mut unbounded, EXECUTIONS, EVENTS_PER, BODY, base);
    let unbounded_bytes = unbounded.total_bytes();
    let unbounded_execs = unbounded.execution_count();
    // enforce_limits is a no-op with no policy: nothing is evicted even though
    // every chain is "old".
    let none = unbounded.enforce_limits_at(base + Duration::from_secs(1_000_000));
    assert_eq!(none.total(), 0, "unbounded policy never evicts");
    assert_eq!(unbounded.execution_count(), EXECUTIONS as usize);

    // --- AFTER: a byte ceiling at ~1/5 of the unbounded resident set. ---
    let ceiling = unbounded_bytes / 5;
    let mut bounded = WalEventIndex::with_order_policy(
        SpineOrder::Causal,
        EvictionPolicy { max_bytes: Some(ceiling), ..Default::default() },
    );
    load(&mut bounded, EXECUTIONS, EVENTS_PER, BODY, base);
    let bounded_before_sweep = bounded.total_bytes();
    let stats = bounded.enforce_limits_at(base + Duration::from_secs(EXECUTIONS as u64));
    let bounded_bytes = bounded.total_bytes();

    // --- AFTER: slim projection shrinks per-event resident cost losslessly. ---
    let mut slim = WalEventIndex::with_order_policy(
        SpineOrder::Causal,
        EvictionPolicy { slim: true, ..Default::default() },
    );
    load(&mut slim, EXECUTIONS, EVENTS_PER, BODY, base);
    let slim_bytes = slim.total_bytes();

    eprintln!("=== #166 WAL-index memory bound ({EXECUTIONS} execs × {EVENTS_PER} events × {BODY}B bodies) ===");
    eprintln!("BEFORE  unbounded : {:>12} bytes ({} execs, {} events)", unbounded_bytes, unbounded_execs, unbounded.event_count());
    eprintln!("        slim      : {:>12} bytes (lossless field projection, no eviction)", slim_bytes);
    eprintln!("AFTER   ceiling   : {:>12} bytes  (set {ceiling}); pre-sweep {bounded_before_sweep}", bounded_bytes);
    eprintln!("        evicted   : {} chains by byte_ceiling, {} resident", stats.byte_ceiling, bounded.execution_count());

    // The bounded index honors the ceiling regardless of how many executions
    // were applied — the bounded-memory guarantee.
    assert!(bounded_bytes <= ceiling, "resident set must be held at/under the ceiling");
    assert!(stats.byte_ceiling > 0, "the ceiling must have forced evictions");
    // Bounded resident set is a fraction of the unbounded one.
    assert!(bounded_bytes < unbounded_bytes / 4);
    // Slim is strictly smaller than the full envelope index (lossless win).
    assert!(slim_bytes < unbounded_bytes, "slim projection reduces resident bytes");
}

#[test]
fn ttl_sweep_clears_idle_working_set() {
    // The 654-at-idle shape: apply many executions, advance the clock past TTL
    // without driving them, and prove the TTL sweep clears the lot — while a
    // handful "driven" just before the sweep survive (the active working set).
    const EXECUTIONS: i64 = 200;
    let ttl = Duration::from_secs(900);
    let base = Instant::now();
    let mut index = WalEventIndex::with_order_policy(
        SpineOrder::Causal,
        EvictionPolicy { ttl: Some(ttl), ..Default::default() },
    );
    load(&mut index, EXECUTIONS, 10, 500, base);
    assert_eq!(index.execution_count(), EXECUTIONS as usize);

    // Sweep well past the TTL; keep 5 executions "active" by touching them just
    // before the sweep instant.
    let sweep = base + Duration::from_secs(EXECUTIONS as u64) + ttl + Duration::from_secs(1);
    for e in 0..5 {
        index.touch_at(1000 + e, sweep - Duration::from_secs(1));
    }
    let stats = index.enforce_limits_at(sweep);
    eprintln!(
        "=== #166 TTL sweep === evicted {} idle of {EXECUTIONS}; {} active survive ({} bytes resident)",
        stats.ttl,
        index.execution_count(),
        index.total_bytes()
    );
    assert_eq!(stats.ttl, (EXECUTIONS - 5) as usize, "all idle chains swept");
    assert_eq!(index.execution_count(), 5, "only the active working set remains");
}
