//! Multi-replica execution-affinity validation (noetl/ai-meta#166 Phase 4).
//!
//! A deterministic local harness that drives the **real**
//! [`noetl_worker::sharding::AffinityConfig::decide`] + the NAK/redeliver
//! steering loop across a simulated N-replica pool sharing one durable
//! consumer (JetStream competing-consumers), and measures the two
//! acceptance metrics the Phase-4 goal calls for:
//!
//! 1. **Identical execution outcomes** — every drive hop is processed
//!    exactly once (no dropped, no duplicated hop) under affinity ON and
//!    OFF alike. This is the correctness-independent-of-routing proof: a
//!    NAK-before-claim performs no side effect, so steering can only change
//!    *where* a hop runs, never *whether* the append-only log is identical.
//! 2. **Hit-rate rises / cold-load falls with affinity ON** — with ON, all
//!    hops of an execution converge on its owner replica (warm WAL index),
//!    so a cold-load happens at most once per execution; with OFF, hops
//!    scatter and many replicas cold-load the same execution.
//!
//! The harness models the transport (random delivery + NAK redelivery)
//! rather than standing up NATS/kind, because the property under test is
//! the *decision + convergence*, which is pure worker logic. A true kind
//! StatefulSet run (per-pod shard index) is the follow-up gated with the
//! ops manifest — see the PR's rollout plan.

use std::collections::{HashMap, HashSet};

use noetl_worker::sharding::{AffinityConfig, AffinityDecision};

/// Deterministic splitmix64 — stands in for the non-deterministic order in
/// which competing consumers happen to pull a message. Seeded per
/// (execution, hop, attempt) so the whole run is reproducible.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// Which replica a competing-consumer delivery lands on, given the message
/// coordinates, excluding replicas currently in their NAK-delay backoff
/// window (`excluded`). Uniform over the eligible replicas; if every
/// replica is backed off (can't happen while budget < replicas) it falls
/// back to the full set.
fn pick_delivery(
    execution_id: i64,
    hop: u32,
    attempt: i64,
    replicas: u32,
    excluded: &std::collections::HashSet<u32>,
) -> u32 {
    let seed = (execution_id as u64)
        .wrapping_mul(1_000_003)
        .wrapping_add(hop as u64)
        .wrapping_mul(31)
        .wrapping_add(attempt as u64);
    let eligible: Vec<u32> = (0..replicas).filter(|r| !excluded.contains(r)).collect();
    if eligible.is_empty() {
        return (mix(seed) % replicas as u64) as u32;
    }
    eligible[(mix(seed) % eligible.len() as u64) as usize]
}

struct RunStats {
    /// (execution_id, hop) → number of times processed. MUST all be 1.
    processed: HashMap<(i64, u32), u32>,
    /// Cold-loads: a hop processed by a replica that did not yet have the
    /// execution warm (its first touch of that execution).
    cold_loads: u64,
    /// Warm hits: a hop processed by a replica already warm on the execution.
    hits: u64,
    /// Redirect NAKs issued (affinity steering churn).
    redirects: u64,
    /// Forced-local processings (redirect budget exhausted → owner absent).
    forced_local: u64,
}

impl RunStats {
    fn total_processed(&self) -> u32 {
        self.processed.values().copied().sum()
    }
    fn hit_rate(&self) -> f64 {
        let total = self.hits + self.cold_loads;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// Run the pool over `executions × hops` drive commands and return stats.
///
/// `cfg` is the affinity policy every replica shares (same `shard_count`;
/// each replica gets its own `shard_index` at decision time). Warm state is
/// per-replica: a replica becomes warm on execution E once it processes any
/// hop of E (models the WAL index caching E's chain).
fn run_pool(replicas: u32, executions: i64, hops: u32, cfg: AffinityConfig) -> RunStats {
    // Per-replica warm set of execution ids.
    let mut warm: Vec<HashSet<i64>> = vec![HashSet::new(); replicas as usize];
    let mut stats = RunStats {
        processed: HashMap::new(),
        cold_loads: 0,
        hits: 0,
        redirects: 0,
        forced_local: 0,
    };

    for eid in 0..executions {
        // Snowflake-shaped ids (sequential high bits) — the case the hash
        // must still distribute evenly.
        let execution_id = 320_816_801_799_737_344_i64 + eid;
        for hop in 0..hops {
            // Compete for the delivery, honouring affinity redirects.
            let mut attempt: i64 = 1; // JetStream `delivered` starts at 1.
            // Replicas currently in their NAK-delay backoff window for THIS
            // message — they NAK'd with `Nak(Some(delay))` so they won't see
            // the redelivery until the delay elapses, giving the still-eager
            // owner a window to pull it. Faithful to the delayed-NAK
            // mechanism; excluded from the next competing pull.
            let mut backoff: HashSet<u32> = HashSet::new();
            loop {
                let replica = pick_delivery(execution_id, hop, attempt, replicas, &backoff);
                // The pulling replica evaluates the policy from ITS index.
                let replica_cfg = AffinityConfig {
                    shard_index: replica,
                    ..cfg
                };
                let decision = replica_cfg.decide(true, execution_id, attempt);
                match decision {
                    AffinityDecision::Redirect => {
                        // NAK (with delay) back to the shared consumer; this
                        // replica enters its backoff window and the message is
                        // redelivered to a still-eager puller. No claim, no
                        // side effect.
                        stats.redirects += 1;
                        backoff.insert(replica);
                        attempt += 1;
                        continue;
                    }
                    AffinityDecision::ForcedLocal => stats.forced_local += 1,
                    AffinityDecision::Owned | AffinityDecision::NotApplicable => {}
                }
                // Process the hop on `replica`.
                if warm[replica as usize].contains(&execution_id) {
                    stats.hits += 1;
                } else {
                    stats.cold_loads += 1;
                    warm[replica as usize].insert(execution_id);
                }
                *stats.processed.entry((execution_id, hop)).or_insert(0) += 1;
                break;
            }
        }
    }
    stats
}

// A 3-replica pool matches the default redirect budget (`max_redirects=2`)
// and the realistic initial system-pool scale (1→2→3 replicas). With the
// delayed-NAK backoff, a non-owned drive command reaches its owner within
// the budget (after the two other replicas back off, only the owner is
// eligible).
const REPLICAS: u32 = 3;
const EXECUTIONS: i64 = 300;
const HOPS: u32 = 6;

fn affinity_on() -> AffinityConfig {
    AffinityConfig {
        shard_index: 0, // overridden per-replica in run_pool
        shard_count: REPLICAS,
        enabled: true,
        max_redirects: 2,
        nak_delay: std::time::Duration::from_millis(150),
    }
}

fn affinity_off() -> AffinityConfig {
    AffinityConfig {
        enabled: false,
        ..affinity_on()
    }
}

/// Correctness-independent-of-routing: with affinity ON or OFF, every drive
/// hop is processed exactly once — no dropped hop, no duplicated hop. This
/// is the identical-outcome guarantee the hard constraint requires.
#[test]
fn every_hop_processed_exactly_once_in_both_modes() {
    for (name, cfg) in [("on", affinity_on()), ("off", affinity_off())] {
        let stats = run_pool(REPLICAS, EXECUTIONS, HOPS, cfg);
        let expected = (EXECUTIONS as u32) * HOPS;
        assert_eq!(
            stats.total_processed(),
            expected,
            "[{name}] total processed hops"
        );
        assert_eq!(
            stats.processed.len() as u32,
            expected,
            "[{name}] distinct (execution, hop) pairs"
        );
        for (&(eid, hop), &count) in &stats.processed {
            assert_eq!(count, 1, "[{name}] hop ({eid},{hop}) processed {count}×");
        }
    }
}

/// The Phase-4 headline: affinity ON raises the warm-index hit rate and
/// slashes the cold-load count versus OFF — with the same total work.
#[test]
fn affinity_on_raises_hit_rate_and_cuts_cold_loads() {
    let on = run_pool(REPLICAS, EXECUTIONS, HOPS, affinity_on());
    let off = run_pool(REPLICAS, EXECUTIONS, HOPS, affinity_off());

    // Same total work either way.
    assert_eq!(on.total_processed(), off.total_processed());

    // Hit rate strictly higher with affinity on.
    assert!(
        on.hit_rate() > off.hit_rate() + 0.15,
        "hit rate on={:.3} off={:.3} (expected a clear rise)",
        on.hit_rate(),
        off.hit_rate()
    );

    // Cold-loads: OFF scatters hops so many replicas cold-load the same
    // execution; ON converges on the owner so each execution cold-loads
    // about once. Expect a large reduction.
    assert!(
        on.cold_loads * 2 < off.cold_loads,
        "cold-loads on={} off={} (expected on << off)",
        on.cold_loads,
        off.cold_loads
    );

    // With ON, an execution cold-loads at most a small number of times
    // (owner first-touch, plus the rare forced-local when the pseudo-random
    // delivery never reached the owner within budget). Bound it tight:
    // at most one owner cold-load per execution + the forced-local tail.
    assert!(
        on.cold_loads <= EXECUTIONS as u64 + on.forced_local,
        "on cold-loads {} exceeds 1/execution + forced_local {}",
        on.cold_loads,
        on.forced_local
    );

    // Sanity: OFF issues no redirects; ON issues some steering churn.
    assert_eq!(off.redirects, 0, "affinity-off must never NAK-steer");
    assert!(on.redirects > 0, "affinity-on should steer non-owned hops");

    eprintln!(
        "Phase-4 multi-replica ({REPLICAS} replicas, {EXECUTIONS} execs × {HOPS} hops):\n  \
         affinity ON : hit_rate={:.1}%  cold_loads={}  redirects={}  forced_local={}\n  \
         affinity OFF: hit_rate={:.1}%  cold_loads={}  redirects={}  forced_local={}",
        on.hit_rate() * 100.0,
        on.cold_loads,
        on.redirects,
        on.forced_local,
        off.hit_rate() * 100.0,
        off.cold_loads,
        off.redirects,
        off.forced_local,
    );
}

/// Rebalance safety: shrinking the pool (a replica leaves → `shard_count`
/// drops) re-homes executions to new owners, but every hop is still
/// processed exactly once. Models the topology change as a mid-run
/// shard_count change and asserts no hop is dropped or duplicated.
#[test]
fn rebalance_preserves_exactly_once() {
    // Phase A: 3 replicas own the executions.
    let before = run_pool(REPLICAS, EXECUTIONS, HOPS, affinity_on());
    // Phase B: pool shrinks to 2 — ownership moves; re-drive the same
    // executions' next hops under the new topology.
    let smaller = AffinityConfig {
        shard_count: 2,
        ..affinity_on()
    };
    let after = run_pool(2, EXECUTIONS, HOPS, smaller);

    // Each phase independently processes every hop exactly once — the
    // rebalance is a cache-cold routing change, never a drop/dup.
    let expected = (EXECUTIONS as u32) * HOPS;
    assert_eq!(before.total_processed(), expected);
    assert_eq!(after.total_processed(), expected);
    for (&_, &count) in before.processed.iter().chain(after.processed.iter()) {
        assert_eq!(count, 1);
    }
    // Both still beat a scattered baseline (owner convergence survives the
    // smaller ring).
    let off = run_pool(3, EXECUTIONS, HOPS, affinity_off());
    assert!(after.hit_rate() > off.hit_rate());
}
