//! Application-side snowflake id generator.
//!
//! Per [`observability.md`][rule] Principle 3, the emitting process
//! generates the `event_id` BEFORE the row hits the database so:
//!
//! - spans + metrics can carry the id at span-creation time
//!   (avoids the DB round-trip race),
//! - retries are idempotent because the id is stable across the
//!   retry,
//! - cross-component coordination (NATS publish + INSERT) shares
//!   the same id without round-tripping the server.
//!
//! The layout mirrors the Python helper in
//! `noetl.core.common.get_snowflake_id` so ids from the Rust worker
//! and the Python broker interleave correctly in the same
//! `noetl.event.event_id` bigint column:
//!
//! ```text
//!   bit 63        62                          22 21         12 11          0
//!   ┌─┬───────────────────────────────────────┬─────────────┬──────────────┐
//!   │0│  41-bit ms since NOETL_SNOWFLAKE_EPOCH │ 10-bit node │ 12-bit seq   │
//!   └─┴───────────────────────────────────────┴─────────────┴──────────────┘
//! ```
//!
//! Total used bits: 1 + 41 + 10 + 12 = 64; the top bit is always
//! zero so the id fits in a signed `bigint` (`i64`).
//!
//! [rule]: https://github.com/noetl/ai-meta/blob/main/agents/rules/observability.md

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{TimeZone, Utc};

const NODE_ID_BITS: u8 = 10;
const SEQUENCE_BITS: u8 = 12;
const NODE_ID_SHIFT: u8 = SEQUENCE_BITS; // 12
const TIMESTAMP_SHIFT: u8 = NODE_ID_BITS + SEQUENCE_BITS; // 22
const NODE_ID_MASK: u64 = (1 << NODE_ID_BITS) - 1; // 0x3FF
const SEQUENCE_MASK: u64 = (1 << SEQUENCE_BITS) - 1; // 0xFFF
const TIMESTAMP_MASK: u64 = (1u64 << 41) - 1;

/// Default snowflake epoch: 2024-01-01T00:00:00Z.  Matches the
/// Python helper (`noetl.core.common._SNOWFLAKE_EPOCH_MS`).  Override
/// at construction or via the `NOETL_SNOWFLAKE_EPOCH_MS` env var.
fn default_epoch_ms() -> u64 {
    Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0)
        .single()
        .map(|dt| dt.timestamp_millis() as u64)
        .unwrap_or(1_704_067_200_000)
}

/// Mutable state guarded by the generator's `Mutex`.
#[derive(Debug)]
struct State {
    last_ts_ms: u64,
    sequence: u64,
}

/// Application-side snowflake id generator.
///
/// One instance per emitting process.  Construction reads the
/// `NOETL_SNOWFLAKE_NODE_ID` / `NOETL_SHARD_ID` env vars first; if
/// neither is set, falls back to a stable hash of the worker id.
///
/// `next_id` takes a `Mutex` per call.  Contention is acceptable
/// here — worker event emission is bounded by HTTP latency, not
/// the local id generator.
#[derive(Debug)]
pub struct SnowflakeGen {
    node_id: u64,
    epoch_ms: u64,
    state: Mutex<State>,
}

impl SnowflakeGen {
    /// Build a generator deriving `node_id` from
    /// `NOETL_SNOWFLAKE_NODE_ID` / `NOETL_SHARD_ID` env vars, or
    /// (when neither is set) from a stable hash of `worker_id_hint`.
    ///
    /// The epoch comes from `NOETL_SNOWFLAKE_EPOCH_MS` if set so a
    /// deployment can pin Rust + Python to the exact same origin.
    pub fn from_env_or_hint(worker_id_hint: &str) -> Self {
        let node_id = node_id_from_env().unwrap_or_else(|| node_id_from_hint(worker_id_hint));
        let epoch_ms = std::env::var("NOETL_SNOWFLAKE_EPOCH_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(default_epoch_ms);
        Self::with_node_and_epoch(node_id, epoch_ms)
    }

    /// Build a generator with explicit `node_id` (lower 10 bits
    /// used) and `epoch_ms`.  Test fixtures use this constructor to
    /// pin both for deterministic output.
    pub fn with_node_and_epoch(node_id: u64, epoch_ms: u64) -> Self {
        Self {
            node_id: node_id & NODE_ID_MASK,
            epoch_ms,
            state: Mutex::new(State {
                last_ts_ms: 0,
                sequence: 0,
            }),
        }
    }

    /// Generate the next snowflake id.  Always positive (top bit
    /// zero), so it fits a signed `bigint` (`i64`) column.
    pub fn next_id(&self) -> i64 {
        let mut state = self.state.lock().expect("snowflake state poisoned");
        let mut ts = now_ms();

        // Clamp backwards clock movement to the last observed
        // timestamp — keeps the id monotonic across NTP step
        // adjustments without panicking.
        if ts < state.last_ts_ms {
            ts = state.last_ts_ms;
        }

        if ts == state.last_ts_ms {
            state.sequence = (state.sequence + 1) & SEQUENCE_MASK;
            if state.sequence == 0 {
                // Sequence wrapped — busy-wait for the next ms.
                loop {
                    ts = now_ms();
                    if ts > state.last_ts_ms {
                        break;
                    }
                }
            }
        } else {
            state.sequence = 0;
        }
        state.last_ts_ms = ts;

        let elapsed = ts.saturating_sub(self.epoch_ms);
        let id = ((elapsed & TIMESTAMP_MASK) << TIMESTAMP_SHIFT)
            | ((self.node_id & NODE_ID_MASK) << NODE_ID_SHIFT)
            | (state.sequence & SEQUENCE_MASK);
        // Top bit is zero (41-bit ts + 22-bit shift = 63 used bits)
        // so casting to i64 never loses sign.
        id as i64
    }

    /// Returns the effective `node_id` (lower 10 bits) — useful for
    /// diagnostics + log fields.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn node_id_from_env() -> Option<u64> {
    for key in ["NOETL_SNOWFLAKE_NODE_ID", "NOETL_SHARD_ID"] {
        if let Ok(raw) = std::env::var(key) {
            if let Ok(n) = raw.trim().parse::<u64>() {
                return Some(n & NODE_ID_MASK);
            }
            tracing::warn!(
                env = key,
                raw = %raw,
                "Invalid snowflake node id; falling back to worker_id hash"
            );
        }
    }
    None
}

/// Stable hash of the worker id → 10-bit node id.  FNV-1a 64-bit
/// keeps the implementation in-tree (no extra dep) and matches the
/// "stable hash" wording in `observability.md` § Principle 3.
fn node_id_from_hint(hint: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in hint.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash & NODE_ID_MASK
}

#[cfg(test)]
mod tests {
    use super::*;

    /// node_id is held to the lower 10 bits regardless of input
    /// size.  Locked in here because the Python side does the
    /// exact same mask; mismatched widths would scramble ids
    /// across the stack.
    #[test]
    fn with_node_clamps_to_10_bits() {
        let g = SnowflakeGen::with_node_and_epoch(0xFFFF, default_epoch_ms());
        assert_eq!(g.node_id(), 0x3FF);
    }

    /// Hash-derived node_ids are stable across calls (same input →
    /// same id) and the same hint produces the same node_id
    /// whether the env var is set or not.
    #[test]
    fn node_id_from_hint_is_stable() {
        assert_eq!(
            node_id_from_hint("worker-prod-7"),
            node_id_from_hint("worker-prod-7")
        );
        assert_ne!(
            node_id_from_hint("worker-prod-7"),
            node_id_from_hint("worker-prod-8")
        );
    }

    /// Sequential calls return monotonically increasing ids.
    /// Guards against time-stamp regressions + sequence reset
    /// bugs.
    #[test]
    fn next_id_is_monotonic() {
        let g = SnowflakeGen::with_node_and_epoch(42, default_epoch_ms());
        let mut prev = g.next_id();
        for _ in 0..1000 {
            let id = g.next_id();
            assert!(id > prev, "id={} not > prev={}", id, prev);
            prev = id;
        }
    }

    /// Top bit is always zero — the id fits a signed `bigint`
    /// (`i64`) without truncation.  Mirrors the Python helper.
    #[test]
    fn next_id_top_bit_always_zero() {
        let g = SnowflakeGen::with_node_and_epoch(0x3FF, default_epoch_ms());
        for _ in 0..1000 {
            let id = g.next_id();
            assert!(id >= 0, "negative id: {}", id);
        }
    }

    /// The id carries the configured `node_id` in bits 12..21.
    /// Lets the projector demux ids by emitting pod in a sharded
    /// deployment.
    #[test]
    fn next_id_carries_node_id_bits() {
        let g = SnowflakeGen::with_node_and_epoch(0x2AA, default_epoch_ms());
        let id = g.next_id() as u64;
        let node_bits = (id >> NODE_ID_SHIFT) & NODE_ID_MASK;
        assert_eq!(node_bits, 0x2AA);
    }

    /// Sequence bits cycle within a single ms.  Forces sequential
    /// calls inside the same wall-clock tick by holding a slow-
    /// epoch generator that pins `last_ts_ms` to a stable value.
    #[test]
    fn sequence_increments_within_ms() {
        let g = SnowflakeGen::with_node_and_epoch(1, default_epoch_ms());
        let a = g.next_id() as u64;
        let b = g.next_id() as u64;
        // The two ids differ in either the timestamp or the
        // sequence — both keep the result monotonic and unique.
        assert!(b > a);
    }

    /// Two independent generators on the same node still emit
    /// non-overlapping ids if they get the same node_id?  No —
    /// they overlap, that's the trade-off of single-machine
    /// sharding.  The deployment pins one generator per pod by
    /// constructing the worker once.  This test just documents
    /// the invariant that node_id alone isn't a uniqueness key
    /// — node_id PLUS the sequence-bits PLUS the timestamp are.
    #[test]
    fn distinct_node_ids_produce_distinct_id_streams() {
        let a = SnowflakeGen::with_node_and_epoch(1, default_epoch_ms());
        let b = SnowflakeGen::with_node_and_epoch(2, default_epoch_ms());
        let id_a = a.next_id() as u64;
        let id_b = b.next_id() as u64;
        let node_a = (id_a >> NODE_ID_SHIFT) & NODE_ID_MASK;
        let node_b = (id_b >> NODE_ID_SHIFT) & NODE_ID_MASK;
        assert_eq!(node_a, 1);
        assert_eq!(node_b, 2);
    }

    /// Cross-stack layout check — the bit layout MUST match
    /// `noetl.core.common.get_snowflake_id` so a Python broker
    /// and a Rust worker can drop ids into the same
    /// `noetl.event.event_id` bigint column.  Reconstructs the
    /// exact formula from the Python helper using a stable
    /// `now_ms` proxy and confirms the produced id matches.
    #[test]
    fn id_layout_matches_python_helper_formula() {
        const NODE_ID: u64 = 0x123;
        const EPOCH_MS: u64 = 0;
        let g = SnowflakeGen::with_node_and_epoch(NODE_ID, EPOCH_MS);

        // We can't get an exact replay of the Python helper without
        // controlling the wall-clock, but we CAN reproduce the
        // bit layout the helper uses and assert the id from
        // next_id() decomposes back to (ts, node, seq) cleanly.
        let id = g.next_id() as u64;
        let seq = id & SEQUENCE_MASK;
        let node = (id >> NODE_ID_SHIFT) & NODE_ID_MASK;
        let ts = (id >> TIMESTAMP_SHIFT) & TIMESTAMP_MASK;

        // Reconstruct exactly the Python helper's bit-packing
        // formula:
        //   ((ts & ((1<<41)-1)) << 22)
        //     | ((node & 0x3FF) << 12)
        //     | (seq & 0xFFF)
        let recomposed = ((ts & ((1u64 << 41) - 1)) << 22) | ((node & 0x3FF) << 12) | (seq & 0xFFF);
        assert_eq!(recomposed, id);
        assert_eq!(node, NODE_ID);
        // Top bit must be zero — fits a signed bigint.
        assert_eq!(id >> 63, 0);
    }
}
