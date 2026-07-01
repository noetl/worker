//! noetl/ai-meta#166 Phase 3 — cold-load-from-shard **output-equivalence** proof.
//!
//! ## What this proves
//!
//! On an off-server drive cache miss, Phase 3 reconstructs the execution's chain
//! by reading the Feather **state shard** from object store instead of replaying
//! the retained WAL. The drive makes routing decisions (`next.arcs`, `when`,
//! loop counters, fan-in) from the reconstructed spine, so the shard-loaded chain
//! MUST produce **byte-identical** drive input to the WAL-replay / in-memory path.
//!
//! This harness exercises the real primitives end-to-end over a realistic,
//! load-bearing chain — genesis with `context.workload`, a `step.enter` carrying
//! `iterations_expected`, multiple `command.completed` events with full `result`
//! bodies, a `ctx.updated`, and a terminal `playbook_completed` — and asserts:
//!
//! 1. The Phase-2 writer's slim `payload` column round-trips through the shared
//!    `arrow_codec` Feather encode/decode byte-for-byte.
//! 2. Cold-loading those decoded payloads into a fresh `WalEventIndex` and
//!    building the drive input yields **exactly** the bytes the WAL-apply path
//!    produces over the same events — the equivalence guarantee.
//!
//! Pure + deterministic (no NATS, no object store): the cold-load reader's decode
//! and the index apply are the same code the drive runs. Real cold-load latency
//! vs the WAL-replay miss cost is measured on kind (see the PR).
//!
//! ```text
//! cargo test --test state_shard_cold_load
//! ```

use noetl_worker::state_builder::{
    build_offserver_input, shard_chain_payload, EvictionPolicy, SharedWalIndex, SpineOrder,
    WalEventIndex,
};
use serde_json::{json, Value};

/// A fresh index configured like PROD (`NOETL_STATE_INDEX_SLIM=on`): both the
/// WAL-replay path and the cold-load path store the slim projection, so a
/// shard-reconstructed spine is byte-identical to the WAL-replay spine. (With
/// slim off the shard stores slim while the WAL stores the full envelope — still
/// output-equivalent to the drive, but not byte-identical; shard-read is designed
/// to pair with the slim index, the prod default.)
fn slim_index() -> SharedWalIndex {
    let policy = EvictionPolicy { slim: true, ..Default::default() };
    SharedWalIndex::new(WalEventIndex::with_order_policy(SpineOrder::Causal, policy))
}

const EXEC: i64 = 330390737327759360;

/// A full `noetl_events`-shaped envelope (superset of the slim projection) — the
/// writer slims this before storing, the WAL drain applies it as-is.
fn full_event(event_id: i64, prev: Option<i64>, etype: &str, node: &str, extra: Value) -> Value {
    let mut base = json!({
        "event_id": event_id,
        "execution_id": EXEC,
        "catalog_id": 657533161378677602i64,
        "prev_event_id": prev,
        "event_type": etype,
        "node_name": node,
        "status": "completed",
        "timestamp": "2026-06-30T12:00:00Z",
        // Envelope fields the slim projection deliberately DROPS (node_id, etc.) —
        // present here to prove the projection is lossless w.r.t. the drive.
        "node_id": 7,
        "node_type": "task",
        "duration": 1234,
    });
    if let (Value::Object(b), Value::Object(e)) = (&mut base, &extra) {
        for (k, v) in e {
            b.insert(k.clone(), v.clone());
        }
    }
    base
}

/// A realistic Muno itinerary-planner-shaped chain exercising every load-bearing
/// field the drive reads: `context` (workload), `result` bodies, `meta`,
/// `iterations_expected`, `ctx.updated`.
fn muno_chain() -> Vec<Value> {
    vec![
        full_event(
            1,
            None,
            "playbook_started",
            "start",
            json!({ "context": { "workload": { "event_payload": { "text": "Trip to Paris" } }, "path": "muno/playbooks/itinerary-planner" } }),
        ),
        full_event(
            2,
            Some(1),
            "step.enter",
            "show_places",
            json!({ "result": { "status": "completed", "context": { "iterations_expected": 2 } } }),
        ),
        full_event(
            3,
            Some(2),
            "command.completed",
            "show_places",
            json!({ "result": { "status": "completed", "data": { "places": [{ "name": "Louvre" }, { "name": "Eiffel Tower" }] } }, "meta": { "cursor": { "phase": "body", "frame": 0 } } }),
        ),
        full_event(
            4,
            Some(3),
            "command.completed",
            "show_places",
            json!({ "result": { "status": "completed", "data": { "places": [{ "name": "Notre-Dame" }] } } }),
        ),
        full_event(
            5,
            Some(4),
            "ctx.updated",
            "show_places",
            json!({ "result": { "context": { "step": "show_places", "gen": 5, "values": { "places_seen": true } } } }),
        ),
        full_event(
            6,
            Some(5),
            "playbook_completed",
            "start",
            json!({ "result": { "status": "completed" } }),
        ),
    ]
}

/// Encode a chain the way the Phase-2 writer does: the 8 slim columns, with the
/// `payload` column carrying the verbatim `slim_event_payload` of each envelope.
fn encode_state_shard(events: &[Value]) -> Vec<u8> {
    let rows: Vec<Value> = events
        .iter()
        .map(|e| {
            let payload = serde_json::to_string(&shard_chain_payload(e)).unwrap();
            json!([
                e.get("event_id"),
                e.get("prev_event_id"),
                e.get("event_type"),
                e.get("node_name"),
                e.get("status"),
                Value::Null, // result_ref
                Value::Null, // extracted
                payload,     // verbatim slim payload (Phase 3)
            ])
        })
        .collect();
    let tabular = json!({
        "columns": ["event_id", "prev_event_id", "event_type", "node_name", "status", "result_ref", "extracted", "payload"],
        "rows": rows,
    });
    noetl_tools::arrow_codec::try_encode_tabular_json(&tabular).unwrap().bytes
}

/// Decode the `payload` column back into the per-event slim payloads (mirror of
/// the reader's `decode_shard_payloads`, which is private).
fn decode_payload_column(bytes: &[u8]) -> Vec<Value> {
    use arrow::array::{Array, StringArray};
    let batches = noetl_tools::arrow_codec::decode_record_batches(bytes).unwrap();
    let mut out = Vec::new();
    for batch in &batches {
        let idx = batch.schema().fields().iter().position(|f| f.name() == "payload").unwrap();
        let arr = batch.column(idx).as_any().downcast_ref::<StringArray>().unwrap();
        for r in 0..batch.num_rows() {
            out.push(serde_json::from_str::<Value>(arr.value(r)).unwrap());
        }
    }
    out
}

#[tokio::test]
async fn cold_load_from_shard_is_byte_equivalent_to_wal_replay() {
    let events = muno_chain();
    let playbook = json!({ "path": "muno/playbooks/itinerary-planner", "workflow": [] });
    let head = 6i64;

    // --- WAL-replay / in-memory reference ---------------------------------
    // Apply the full envelopes (as the live drain does) with slim projection on
    // (the prod config) and build the drive input at expected_head.
    let wal_index = slim_index();
    {
        let mut idx = wal_index.lock().await;
        for e in &events {
            idx.apply(e);
        }
    }
    let wal_input = build_offserver_input(&wal_index, EXEC, &playbook, None, Some(head), Some(head), false)
        .await
        .expect("WAL-replay path serves the drive");

    // --- Phase 3 cold-load-from-shard -------------------------------------
    // Encode the shard (writer), decode the payload column (reader), apply into a
    // FRESH index, build the same drive input.
    let shard_bytes = encode_state_shard(&events);
    let decoded = decode_payload_column(&shard_bytes);
    assert_eq!(decoded.len(), events.len(), "every event round-trips through the shard");
    for (e, d) in events.iter().zip(&decoded) {
        assert_eq!(&shard_chain_payload(e), d, "the shard payload column is the verbatim slim chain payload");
    }

    let shard_index = slim_index();
    {
        let mut idx = shard_index.lock().await;
        for p in &decoded {
            idx.apply(p);
        }
    }
    let shard_input = build_offserver_input(&shard_index, EXEC, &playbook, None, Some(head), Some(head), false)
        .await
        .expect("cold-load path serves the drive");

    // --- Equivalence: byte-identical drive input --------------------------
    assert_eq!(
        wal_input, shard_input,
        "cold-load-from-shard MUST produce byte-identical drive input to the WAL replay"
    );
}
