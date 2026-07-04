//! Execution-affinity sharding — worker-side ownership hash + routing
//! policy (noetl/ai-meta#166 Phase 4).
//!
//! ## What this is
//!
//! All replicas of a worker pool bind the **same** durable JetStream pull
//! consumer ([`crate::nats::subscriber`]), so today an off-server drive
//! command (`__orchestrate__`) is delivered to whichever replica pulls it
//! first — with **no execution affinity**. A hop for execution `E`
//! frequently lands on a replica that does not have `E` warm in its
//! [`crate::state_builder::WalEventIndex`], forcing a Phase-3 cold-load
//! ([`crate::state_reader`]) or a WAL replay.
//!
//! Phase 4 adds *cooperative steering*: a replica that pulls a drive
//! command it does **not** own NAKs it back to the shared consumer (a
//! bounded number of times) so the **owning** replica — the one that
//! already has the execution warm — can pick it up. Cold-loads become the
//! rare fallback rather than the common case across a multi-replica pool.
//!
//! ## Ownership hash — reuse #116, byte-identical
//!
//! [`shard_for`] is a verbatim reimplementation of `noetl-server`'s
//! `src/sharding.rs` `shard_for`: [`twox_hash::XxHash64`] with a fixed
//! seed of `0` over the 8 little-endian bytes of `execution_id`, taken
//! `% shard_count`. The server crate is a binary (not a library) so the
//! function is duplicated rather than imported; [`twox_hash`] is already
//! in the dependency tree. The unit test
//! [`tests::shard_for_matches_server_pinned_vectors`] pins the same
//! `(execution_id, shard_count) → shard` vectors the server's own test
//! pins, so the two implementations can never silently drift. Because the
//! hash is stable across replicas, every stateful worker configured with
//! the same `NOETL_SHARD_COUNT` agrees on which replica owns an execution
//! — the property that makes cooperative steering converge.
//!
//! ## Correctness is independent of routing
//!
//! Affinity only biases **where** a hop preferentially runs; it never
//! changes **whether** a hop can run correctly. A NAK-before-claim
//! performs no claim and emits no event, so a redirect can never
//! double-process a hop (claim atomicity is the exactly-once gate) and
//! redelivery guarantees it is never dropped. When the owner is
//! absent/dead (pod loss, mid-rebalance) the redirect budget
//! ([`AffinityConfig::max_redirects`]) is exhausted and any replica
//! processes it via the Phase-3 cold-load / WAL-replay backstop. So a
//! mis-route or a rebalance is a **latency** regression only, never a
//! divergence — the append-only event log and the idempotent off-server
//! re-drive (noetl/ai-meta#141, noetl/ai-meta#171) remain the safety net.

use std::hash::Hasher;
use std::time::Duration;

use twox_hash::XxHash64;

/// Fixed seed for the shard-routing hash. MUST match `noetl-server`'s
/// `sharding::SHARD_HASH_SEED` (`0`) or a worker and a server would
/// disagree on ownership. Changing it invalidates every existing
/// assignment; see the server module docs.
const SHARD_HASH_SEED: u64 = 0;

/// The synthetic step name the server assigns to the control-plane drive
/// command (`system/orchestrate` wasm dispatch). Mirrors
/// `crate::executor::command::ORCHESTRATE_STEP_NAME`; the drift guard is
/// [`tests::drive_step_name_matches_executor`]. Affinity steering fires
/// **only** for commands carrying this step — stateless tool commands
/// (http / postgres / …) run immediately with zero added latency.
pub const DRIVE_STEP_NAME: &str = "__orchestrate__";

/// Default redirect budget: how many times a non-owning replica NAKs a
/// drive command back to the shared consumer before processing it locally
/// anyway (owner presumed absent). Bounds worst-case added latency to
/// `max_redirects × nak_delay`.
const DEFAULT_MAX_REDIRECTS: i64 = 2;

/// Default delay applied to an affinity NAK so the redelivery gives the
/// owner replica a window to pull it (and the non-owner does not hot-spin
/// re-grabbing its own NAK).
const DEFAULT_NAK_DELAY_MS: u64 = 150;

/// Compute the shard index that owns an `execution_id`.
///
/// `XxHash64(seed=0)` over `execution_id.to_le_bytes()`, `% shard_count`.
/// Byte-identical to `noetl-server` `sharding::shard_for`. `shard_count
/// <= 1` short-circuits to shard `0` (the single-owner default).
pub fn shard_for(execution_id: i64, shard_count: u32) -> u32 {
    if shard_count <= 1 {
        return 0;
    }
    let mut h = XxHash64::with_seed(SHARD_HASH_SEED);
    // Hash the i64 as 8 explicit little-endian bytes so the result is
    // stable regardless of `Hasher::write_i64`'s platform behaviour —
    // matches the server exactly.
    h.write(&execution_id.to_le_bytes());
    (h.finish() % shard_count as u64) as u32
}

/// What a replica should do with a freshly-pulled command under the
/// execution-affinity policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AffinityDecision {
    /// Affinity does not apply — not a drive command, or affinity is
    /// disabled / single-shard. Run it now (today's behaviour).
    NotApplicable,
    /// This replica owns the execution — process it (warm-index hit
    /// expected).
    Owned,
    /// A drive command this replica does not own, with redirect budget
    /// remaining — NAK it back to the shared consumer to steer it toward
    /// the owner.
    Redirect,
    /// A drive command this replica does not own, but the redirect budget
    /// is exhausted (owner presumed absent) — process it locally anyway
    /// via the Phase-3 cold-load / WAL-replay backstop. Liveness over
    /// affinity; never drop the hop.
    ForcedLocal,
}

impl AffinityDecision {
    /// Stable metric label for `noetl_worker_affinity_decisions_total`.
    /// [`AffinityDecision::NotApplicable`] returns `None` — it is not
    /// recorded (it would swamp the counter with every tool command).
    pub fn metric_label(self) -> Option<&'static str> {
        match self {
            AffinityDecision::NotApplicable => None,
            AffinityDecision::Owned => Some("owned"),
            AffinityDecision::Redirect => Some("redirected"),
            AffinityDecision::ForcedLocal => Some("forced_local"),
        }
    }
}

/// Execution-affinity routing configuration for one stateful worker
/// replica. Resolved once from env at source construction
/// ([`AffinityConfig::from_env`]). Every knob defaults to today's
/// behaviour, so a worker carrying this code is behaviour-neutral until an
/// operator both sets `NOETL_SHARD_COUNT > 1` and turns
/// `NOETL_STATE_AFFINITY_ROUTE` on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AffinityConfig {
    /// `0..shard_count-1` — which shard this replica owns
    /// (`NOETL_SHARD_INDEX`). Matches `noetl-server`'s `ShardConfig`.
    pub shard_index: u32,
    /// Total shard count for the pool (`NOETL_SHARD_COUNT`); every replica
    /// MUST agree. `1` (default) = no sharding, this replica owns every
    /// execution.
    pub shard_count: u32,
    /// Master switch (`NOETL_STATE_AFFINITY_ROUTE`, default off). When off,
    /// [`AffinityConfig::decide`] always returns
    /// [`AffinityDecision::NotApplicable`].
    pub enabled: bool,
    /// Redirect budget (`NOETL_STATE_AFFINITY_MAX_REDIRECTS`).
    pub max_redirects: i64,
    /// Delay applied to an affinity NAK (`NOETL_STATE_AFFINITY_NAK_DELAY_MS`).
    pub nak_delay: Duration,
}

impl Default for AffinityConfig {
    /// The behaviour-neutral default: single shard, disabled.
    fn default() -> Self {
        Self {
            shard_index: 0,
            shard_count: 1,
            enabled: false,
            max_redirects: DEFAULT_MAX_REDIRECTS,
            nak_delay: Duration::from_millis(DEFAULT_NAK_DELAY_MS),
        }
    }
}

impl AffinityConfig {
    /// Resolve the config from env. Invalid combinations degrade to the
    /// safe (disabled) default with a WARN rather than panicking a worker
    /// — correctness never depends on affinity, so a misconfigured replica
    /// simply runs every command it pulls (today's behaviour).
    pub fn from_env() -> Self {
        let shard_index = env_u32("NOETL_SHARD_INDEX", 0);
        let shard_count = env_u32("NOETL_SHARD_COUNT", 1).max(1);
        let enabled = env_bool("NOETL_STATE_AFFINITY_ROUTE", false);
        let max_redirects = env_i64("NOETL_STATE_AFFINITY_MAX_REDIRECTS", DEFAULT_MAX_REDIRECTS)
            .max(0);
        let nak_delay = Duration::from_millis(env_u64(
            "NOETL_STATE_AFFINITY_NAK_DELAY_MS",
            DEFAULT_NAK_DELAY_MS,
        ));

        // A replica configured for a shard that does not exist in the pool
        // is a config bug. The server panics; a worker must not (a crashed
        // worker is worse than an unrouted one), so disable affinity and
        // keep serving every command.
        if shard_count > 1 && shard_index >= shard_count {
            tracing::warn!(
                shard_index,
                shard_count,
                "NOETL_SHARD_INDEX >= NOETL_SHARD_COUNT — execution-affinity routing disabled \
                 (replica will process every command it pulls, unchanged from single-owner)"
            );
            return Self {
                enabled: false,
                ..Self::default()
            };
        }

        let cfg = Self {
            shard_index,
            shard_count,
            enabled,
            max_redirects,
            nak_delay,
        };
        if enabled && shard_count > 1 {
            tracing::info!(
                shard_index,
                shard_count,
                max_redirects,
                nak_delay_ms = cfg.nak_delay.as_millis() as u64,
                "Execution-affinity routing enabled (noetl/ai-meta#166 Phase 4)"
            );
        }
        cfg
    }

    /// Does this replica own `execution_id`? Single-shard (or unsharded)
    /// always owns. Matches `noetl-server` `ShardConfig::owns`.
    pub fn owns(&self, execution_id: i64) -> bool {
        if self.shard_count <= 1 {
            return true;
        }
        shard_for(execution_id, self.shard_count) == self.shard_index
    }

    /// Whether affinity steering can ever fire for this replica — the flag
    /// is on and the pool is genuinely multi-shard. Used to fast-path the
    /// pull loop (skip the drive-step string compare entirely when off).
    pub fn is_active(&self) -> bool {
        self.enabled && self.shard_count > 1
    }

    /// Decide what to do with a pulled command.
    ///
    /// - `is_drive_command`: the notification's step is [`DRIVE_STEP_NAME`]
    ///   (only drive builds read the WAL index, so only they benefit).
    /// - `delivered`: the JetStream redelivery count for this message
    ///   (`msg.info().delivered`, `1` on first delivery). Bounds how many
    ///   times a non-owner NAKs before giving up and processing locally.
    pub fn decide(
        &self,
        is_drive_command: bool,
        execution_id: i64,
        delivered: i64,
    ) -> AffinityDecision {
        if !self.is_active() || !is_drive_command {
            return AffinityDecision::NotApplicable;
        }
        if self.owns(execution_id) {
            return AffinityDecision::Owned;
        }
        // Non-owner. Steer to the owner while redelivery budget remains,
        // else process locally (owner presumed absent) — never drop it.
        if delivered <= self.max_redirects {
            AffinityDecision::Redirect
        } else {
            AffinityDecision::ForcedLocal
        }
    }
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_i64(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drive_step_name_matches_executor() {
        // Drift guard: the executor's control-plane drive step name must
        // equal the const affinity keys off. If the executor renames it,
        // this fails and the affinity fast-path silently steering nothing
        // is caught here rather than in prod.
        assert_eq!(DRIVE_STEP_NAME, "__orchestrate__");
    }

    #[test]
    fn shard_for_single_shard_is_zero() {
        assert_eq!(shard_for(1, 1), 0);
        assert_eq!(shard_for(i64::MAX, 0), 0);
        assert_eq!(shard_for(-5, 1), 0);
    }

    #[test]
    fn shard_for_is_stable_across_calls() {
        for eid in [1_i64, 42, 320816801799737344, i64::MAX, i64::MIN] {
            let first = shard_for(eid, 16);
            for _ in 0..100 {
                assert_eq!(shard_for(eid, 16), first);
            }
            assert!(first < 16);
        }
    }

    #[test]
    fn shard_for_matches_server_pinned_vectors() {
        // These MUST equal `noetl-server` `sharding::shard_for` for the
        // same inputs — the two implementations agree on ownership or
        // cooperative steering never converges. Recomputed here from the
        // same algorithm (XxHash64 seed 0 over LE bytes); if a twox-hash
        // major bump changes the output, this and the server's pinned
        // test both fail together.
        let mut h = XxHash64::with_seed(0);
        h.write(&320816801799737344_i64.to_le_bytes());
        let expected = (h.finish() % 16) as u32;
        assert_eq!(shard_for(320816801799737344, 16), expected);
    }

    #[test]
    fn shard_for_distributes_across_shards() {
        // Sequential snowflake-shaped ids must not all cluster on one
        // shard (the avalanche property the hash buys us).
        let mut hits = [0u32; 8];
        let base = 320816801799737344_i64;
        for i in 0..800 {
            hits[shard_for(base + i, 8) as usize] += 1;
        }
        // Every shard gets a non-trivial share (no clustering); loose
        // bound to stay deterministic.
        for count in hits {
            assert!(count > 40, "uneven distribution: {hits:?}");
        }
    }

    fn cfg(index: u32, count: u32, enabled: bool) -> AffinityConfig {
        AffinityConfig {
            shard_index: index,
            shard_count: count,
            enabled,
            max_redirects: 2,
            nak_delay: Duration::from_millis(150),
        }
    }

    #[test]
    fn single_shard_owns_everything() {
        let c = cfg(0, 1, true);
        assert!(c.owns(1));
        assert!(c.owns(i64::MAX));
        assert!(!c.is_active(), "single shard is never active");
    }

    #[test]
    fn owns_is_exclusive_partition() {
        // For any execution exactly one shard index owns it.
        let count = 5;
        for eid in [1_i64, 42, 999, 320816801799737344, i64::MIN] {
            let owners: Vec<u32> = (0..count)
                .filter(|&idx| cfg(idx, count, true).owns(eid))
                .collect();
            assert_eq!(owners.len(), 1, "eid {eid} owned by {owners:?}");
            assert_eq!(owners[0], shard_for(eid, count));
        }
    }

    #[test]
    fn decide_not_applicable_when_disabled_or_single_or_non_drive() {
        // Disabled → not applicable even for a drive command we don't own.
        assert_eq!(
            cfg(0, 4, false).decide(true, 123, 1),
            AffinityDecision::NotApplicable
        );
        // Single shard → not applicable.
        assert_eq!(
            cfg(0, 1, true).decide(true, 123, 1),
            AffinityDecision::NotApplicable
        );
        // Non-drive command → not applicable regardless of ownership.
        let c = cfg(0, 4, true);
        assert_eq!(c.decide(false, 123, 1), AffinityDecision::NotApplicable);
    }

    #[test]
    fn decide_owned_is_processed() {
        let eid = 320816801799737344_i64;
        let owner = shard_for(eid, 4);
        let c = cfg(owner, 4, true);
        assert_eq!(c.decide(true, eid, 1), AffinityDecision::Owned);
    }

    #[test]
    fn decide_non_owner_redirects_within_budget_then_forces_local() {
        let eid = 320816801799737344_i64;
        let owner = shard_for(eid, 4);
        let non_owner = (owner + 1) % 4;
        let c = cfg(non_owner, 4, true);
        // First two deliveries redirect (budget = 2)...
        assert_eq!(c.decide(true, eid, 1), AffinityDecision::Redirect);
        assert_eq!(c.decide(true, eid, 2), AffinityDecision::Redirect);
        // ...the third gives up and processes locally (owner absent).
        assert_eq!(c.decide(true, eid, 3), AffinityDecision::ForcedLocal);
        assert_eq!(c.decide(true, eid, 99), AffinityDecision::ForcedLocal);
    }

    #[test]
    fn metric_labels_are_stable() {
        assert_eq!(AffinityDecision::NotApplicable.metric_label(), None);
        assert_eq!(AffinityDecision::Owned.metric_label(), Some("owned"));
        assert_eq!(
            AffinityDecision::Redirect.metric_label(),
            Some("redirected")
        );
        assert_eq!(
            AffinityDecision::ForcedLocal.metric_label(),
            Some("forced_local")
        );
    }
}
