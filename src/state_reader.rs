//! State-shard cold-load reader — the **read side** of the object-store state
//! tier ([noetl/ai-meta#166](https://github.com/noetl/ai-meta/issues/166)
//! Phase 3).
//!
//! ## What it is
//!
//! On an off-server drive **cache miss** — the bounded in-memory
//! [`crate::state_builder::WalEventIndex`] holds no (or an evicted) chain for the
//! execution being driven (Phase 1 TTL/byte eviction, or a fresh pod that never
//! indexed it) — this module **cold-loads** the execution's state by reading the
//! Phase-2 Feather **state shard** from object store, decoding the slim chain, and
//! reconstructing the execution's chain in the shared index. It replaces the
//! retained-WAL replay ([`crate::state_builder::rehydrate_execution_from_wal`]) —
//! one keyed `object_get` (~tens of ms) instead of scanning up to the whole
//! retained `noetl_events` window (bounded to the rehydrate deadline).
//!
//! ## Byte-equivalence (the load-bearing correctness property)
//!
//! The drive makes routing decisions (`next.arcs`, `when` templating, loop
//! counters, fan-in barriers) from the reconstructed spine, so the shard-loaded
//! chain MUST be byte-equivalent to what the WAL-replay / in-memory path produces.
//! Phase 2's writer carries the **verbatim slim payload** per event (the exact
//! [`crate::state_builder::slim_event_payload`] the in-memory index stores — the
//! load-bearing `context`/`result`/`meta` bodies included, not dropped), so the
//! reader feeds those payloads straight back into
//! [`crate::state_builder::WalEventIndex::apply`]: the reconstructed chain is
//! byte-identical to the WAL-replay chain **by construction** (same payload → same
//! `apply` → same spine → same drive commands).
//!
//! Correctness never depends on the shard alone:
//! - **Missing shard** (never written / GC'd) → `object_get` 404 → fall back to
//!   the WAL replay (unchanged), the belt-and-suspenders path.
//! - **Stale open shard** (writer cadence lags the WAL tip) → the reconstructed
//!   chain doesn't reach `expected_head` → the drive build's `advance_to`
//!   staleness guard returns `Incomplete` → fall back to the WAL replay, which
//!   supplies the tail.
//! - **`NOETL_STATE_SHARD_READ_VERIFY`** dual-build guard → after a shard build
//!   serves, also run the WAL replay and byte-compare the two spines; on any
//!   divergence increment `state_equivalence_mismatch_total` and serve the WAL
//!   build. Never serves divergent state.
//!
//! Opt-in: the cold-load is attempted only when `NOETL_STATE_SHARD_READ` is
//! truthy (default off → the drive miss path is byte-identical to today's WAL
//! replay). Instant rollback = unset the flag.

use arrow::array::{Array, StringArray};

use crate::client::ControlPlaneClient;
use crate::state_builder::SharedWalIndex;
use crate::state_locator::{ShardSeal, StateCoordinates};
use crate::state_materializer::CellSeed;

/// True when `NOETL_STATE_SHARD_READ` is set to a truthy value — enables the
/// Phase-3 cold-load-from-shard on an off-server drive cache miss. Default off
/// (the miss path stays the WAL replay, byte-identical to today).
pub fn shard_read_enabled() -> bool {
    env_truthy("NOETL_STATE_SHARD_READ")
}

/// True when `NOETL_STATE_SHARD_READ_VERIFY` is truthy — after a shard cold-load
/// serves the drive, also run the WAL replay and byte-compare the two spines
/// (the equivalence tripwire). Default off. A validation/canary knob: it pays
/// BOTH the shard read AND the WAL replay, so it removes the latency payoff —
/// used to PROVE `state_equivalence_mismatch_total == 0` on kind + a prod canary,
/// then turned off for steady-state.
pub fn shard_read_verify_enabled() -> bool {
    env_truthy("NOETL_STATE_SHARD_READ_VERIFY")
}

fn env_truthy(name: &str) -> bool {
    matches!(
        std::env::var(name).unwrap_or_default().trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Outcome of one cold-load-from-shard attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum ColdLoad {
    /// A shard was found + decoded; carries the number of events **new** to the
    /// index (a redelivery/overlap applies idempotently and doesn't count).
    Applied(usize),
    /// A shard was found + decoded but added no new events (every event already
    /// resident) — a soft miss; the caller falls through as if not found.
    Empty,
    /// No shard object exists at either seal key (both `sealed` and `open` 404).
    NotFound,
    /// A shard object exists but the `object_get` errored or the Feather bytes /
    /// `payload` column couldn't be decoded — fall back conservatively.
    Error,
}

/// Cold-load one execution's slim chain from its Feather state shard into the
/// shared WAL index (noetl/ai-meta#166 Phase 3).
///
/// Reconstructs the §7 [`StateCoordinates::physical_key`] deterministically — no
/// carried metadata: the tenant/project default to `default/default` (the worker
/// mints result URNs with no tenant/project → the writer's shard lands under
/// `default/default`), the folder shard + cell placement come from the same
/// `NOETL_RESULT_CELL_*` env the writer read, and the date partition is derived
/// from `execution_id`. Prefers the `sealed` (terminal, complete) shard, falling
/// back to the `open` (in-progress) shard.
///
/// Read-only w.r.t. object store and `noetl.*` (per the data-access-boundary
/// rule): a keyed `GET /api/internal/objects/{key}` and an in-memory index apply,
/// nothing else. Any error path returns without mutating the index beyond the
/// idempotent applies already done, so the caller falls back exactly as today.
pub async fn cold_load_from_shard(
    client: &ControlPlaneClient,
    cell: &CellSeed,
    index: &SharedWalIndex,
    execution_id: i64,
) -> ColdLoad {
    let coords = StateCoordinates::new(None, None, execution_id);
    let placement = cell.placement_for(&coords);
    let date = crate::snowflake::date_partition(execution_id);

    for seal in [ShardSeal::Sealed, ShardSeal::Open] {
        let key = coords.physical_key(&placement, &date, seal, "feather");
        match client.object_get(&key).await {
            Ok(Some((bytes, _ct))) => {
                let payloads = match decode_shard_payloads(&bytes) {
                    Some(p) if !p.is_empty() => p,
                    // Present but undecodable / empty payload column — conservative
                    // fall-back (never serve a partial chain from a broken shard).
                    _ => return ColdLoad::Error,
                };
                let mut applied = 0usize;
                {
                    let mut idx = index.lock().await;
                    for payload in &payloads {
                        if let Some((_eid, is_new, _term)) = idx.apply(payload) {
                            if is_new {
                                applied += 1;
                            }
                        }
                    }
                }
                return if applied > 0 { ColdLoad::Applied(applied) } else { ColdLoad::Empty };
            }
            // 404 for this seal → try the next seal key.
            Ok(None) => continue,
            // Object-store error → conservative fall-back.
            Err(_) => return ColdLoad::Error,
        }
    }
    ColdLoad::NotFound
}

/// Decode a state-shard Feather object into the per-event **verbatim slim
/// payloads** (the `payload` column the Phase-2 writer carries). Each is the
/// exact `slim_event_payload` the in-memory index would store, so applying them
/// reconstructs a byte-equivalent chain. Returns `None` on any decode error or a
/// missing/typed-wrong `payload` column — the caller treats that as
/// [`ColdLoad::Error`] and falls back to the WAL replay.
fn decode_shard_payloads(bytes: &[u8]) -> Option<Vec<serde_json::Value>> {
    let batches = noetl_tools::arrow_codec::decode_record_batches(bytes).ok()?;
    let mut out: Vec<serde_json::Value> = Vec::new();
    for batch in &batches {
        // Address the payload column by NAME (schema-order-independent), so a
        // future column re-order in the writer can't silently mis-read.
        let col_idx = batch.schema().fields().iter().position(|f| f.name() == "payload")?;
        let arr = batch.column(col_idx).as_any().downcast_ref::<StringArray>()?;
        for r in 0..batch.num_rows() {
            if arr.is_null(r) {
                continue;
            }
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(arr.value(r)) {
                out.push(v);
            }
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal chainable slim payload (what the writer's `payload` column
    /// carries) — genesis + one completed hop.
    fn slim(event_id: i64, prev: Option<i64>, etype: &str) -> serde_json::Value {
        serde_json::json!({
            "event_id": event_id,
            "execution_id": 325,
            "prev_event_id": prev,
            "event_type": etype,
            "node_name": "load_offers",
            "status": "completed",
            "result": { "status": "completed", "data": { "rows": 4 } },
        })
    }

    /// Build a state-shard Feather object the way the writer does (columns incl.
    /// `payload`) so the reader's decode is exercised against the real encoder.
    fn encode_shard(payloads: &[serde_json::Value]) -> Vec<u8> {
        let rows: Vec<serde_json::Value> = payloads
            .iter()
            .map(|p| {
                serde_json::json!([
                    p.get("event_id"),
                    p.get("prev_event_id"),
                    p.get("event_type"),
                    p.get("node_name"),
                    p.get("status"),
                    serde_json::Value::Null,          // result_ref
                    serde_json::Value::Null,          // extracted
                    serde_json::to_string(p).unwrap() // payload (verbatim slim)
                ])
            })
            .collect();
        let tabular = serde_json::json!({
            "columns": ["event_id", "prev_event_id", "event_type", "node_name", "status", "result_ref", "extracted", "payload"],
            "rows": rows,
        });
        noetl_tools::arrow_codec::try_encode_tabular_json(&tabular)
            .expect("slim chain → Feather")
            .bytes
    }

    #[test]
    fn decode_round_trips_payload_column() {
        let payloads = vec![
            slim(1, None, "playbook_started"),
            slim(2, Some(1), "command.completed"),
        ];
        let bytes = encode_shard(&payloads);
        let decoded = decode_shard_payloads(&bytes).expect("decodes");
        assert_eq!(decoded.len(), 2);
        // The verbatim payload round-trips byte-for-byte (the equivalence anchor).
        assert_eq!(decoded[0], payloads[0]);
        assert_eq!(decoded[1], payloads[1]);
    }

    #[test]
    fn decode_missing_payload_column_is_none() {
        // A shard without the `payload` column (a hypothetical pre-Phase-3 shape)
        // decodes to None → caller falls back to the WAL replay.
        let tabular = serde_json::json!({
            "columns": ["event_id", "event_type"],
            "rows": [[1, "playbook_started"]],
        });
        let bytes = noetl_tools::arrow_codec::try_encode_tabular_json(&tabular).unwrap().bytes;
        assert!(decode_shard_payloads(&bytes).is_none());
    }

    #[test]
    fn decode_garbage_is_none() {
        assert!(decode_shard_payloads(b"not a feather stream").is_none());
    }

    #[tokio::test]
    async fn cold_load_reconstructs_equivalent_chain_and_serves() {
        use crate::state_builder::{build_offserver_input, SharedWalIndex, WalEventIndex};

        // The playbook the drive build echoes back (opaque to the chain).
        let playbook = serde_json::json!({ "path": "muno/itinerary" });
        let payloads = vec![
            slim(1, None, "playbook_started"),
            slim(2, Some(1), "command.completed"),
        ];

        // Reference: the WAL-replay/in-memory path — apply the payloads directly
        // and build the spine at head=2.
        let wal_index = SharedWalIndex::new(WalEventIndex::new());
        {
            let mut idx = wal_index.lock().await;
            for p in &payloads {
                idx.apply(p);
            }
        }
        let wal_bytes = build_offserver_input(&wal_index, 325, &playbook, None, Some(2), Some(2), false)
            .await
            .expect("WAL path serves");

        // Cold-load: decode the shard payloads into a FRESH index and build the
        // same spine. Byte-identical to the WAL path (equivalence by construction).
        let shard_bytes = encode_shard(&payloads);
        let decoded = decode_shard_payloads(&shard_bytes).unwrap();
        let shard_index = SharedWalIndex::new(WalEventIndex::new());
        {
            let mut idx = shard_index.lock().await;
            for p in &decoded {
                idx.apply(p);
            }
        }
        let shard_out = build_offserver_input(&shard_index, 325, &playbook, None, Some(2), Some(2), false)
            .await
            .expect("shard path serves");

        assert_eq!(
            wal_bytes, shard_out,
            "shard-reconstructed drive input must be byte-identical to the WAL-replay input"
        );
    }

    #[test]
    fn flags_default_off() {
        std::env::remove_var("NOETL_STATE_SHARD_READ");
        std::env::remove_var("NOETL_STATE_SHARD_READ_VERIFY");
        assert!(!shard_read_enabled());
        assert!(!shard_read_verify_enabled());
        std::env::set_var("NOETL_STATE_SHARD_READ", "on");
        assert!(shard_read_enabled());
        std::env::remove_var("NOETL_STATE_SHARD_READ");
    }
}
