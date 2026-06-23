//! Shared result-locator helpers ([noetl/ai-meta#104](https://github.com/noetl/ai-meta/issues/104)).
//!
//! The write side (the shadow result materializer, [`crate::result_materializer`])
//! and the read side (the resolve-by-URN path, [`crate::result_resolver`]) must
//! derive the **same** §7 physical key from the same logical URI, or the read
//! never finds what the write stored. This module is the single place both call,
//! so the two stay in lockstep:
//!
//! - [`coords_from_uri`] — parse the canonical logical URI
//!   (`noetl://<tenant>/<project>/results/<eid>/<step>/<frame>/<row>/<attempt>`)
//!   into [`ResultCoordinates`].
//! - the `date=` partition is derived from the execution_id snowflake
//!   ([`crate::snowflake::date_partition`]), not the event timestamp — so the
//!   read path reconstructs the key from the URI's `execution_id` alone, with no
//!   carried date (RFC §6.4 derivable-not-carried).

use noetl_tools::locator::ResultCoordinates;
use serde_json::Value;

/// Media type stamped on a Feather (Arrow IPC) result-tier object.
pub const FEATHER_MEDIA: &str = "application/vnd.apache.arrow.feather";
/// Media type stamped on a JSON fallback result-tier object.
pub const JSON_MEDIA: &str = "application/json";

/// Which physical encoding a result tier object takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TierKind {
    /// Tabular rowset → Arrow Feather (`noetl_tools::arrow_codec`).
    Feather,
    /// Anything else → JSON (OQ3 decided: JSON, not Parquet).
    Json,
}

impl TierKind {
    pub fn label(self) -> &'static str {
        match self {
            TierKind::Feather => "feather",
            TierKind::Json => "json",
        }
    }

    /// The §7 physical-key file extension for this tier.
    pub fn ext(self) -> &'static str {
        self.label()
    }
}

/// A decided result tier: the deterministically-encoded bytes plus how to store
/// them. The encode is a **pure function of the payload** (see [`decide_tier`]),
/// which is what makes the result tier *derivable* — the producer-staging path
/// ([`crate::result_producer_stage`], #104 OQ5 Option A) and the materializer
/// ([`crate::result_materializer`]) both call [`decide_tier`], so a producer-
/// staged object is **byte-identical** to what the materializer would write.
pub struct Tier {
    pub kind: TierKind,
    pub bytes: Vec<u8>,
    pub media: &'static str,
}

impl Tier {
    pub fn ext(&self) -> &'static str {
        self.kind.ext()
    }
}

/// Decide the over-budget result tier (OQ3 decided: non-tabular → JSON).
///
/// A tool rowset (DuckDB / Postgres / Snowflake) is the tabular case the Feather
/// tier exists for, but the stored result envelope nests it: the worker stores
/// `result_context = {data: {<tool>: <output>}, status, stdout, …}`. So we look
/// for a tabular rowset in two places, in order:
///
///  1. the payload **itself** is a top-level `{rows…}` / `{data:{rows…}}` rowset
///     (`try_encode_tabular_json`) — the shape a colocated shm consumer sees;
///  2. otherwise, a value under the conventional `data.<tool>` envelope is a
///     rowset — the realistic shape an over-budget DuckDB/Postgres result takes.
///
/// The first match encodes Arrow **Feather** (the rowset only); anything else
/// falls back to **JSON** of the whole payload.
///
/// **Determinism / byte-identity.** The Feather extraction is independent of the
/// outer envelope's key ordering (it pulls `columns`/`rows`), and the JSON
/// fallback is `serde_json::to_vec`, which is key-sorted (the worker does not
/// enable serde_json's `preserve_order`). So `decide_tier` returns the same bytes
/// for an equal `Value` regardless of how it was constructed or round-tripped
/// through the result store — the guarantee the producer-staging path relies on.
pub fn decide_tier(payload: &Value) -> Tier {
    let encode = noetl_tools::arrow_codec::try_encode_tabular_json;
    let tabular = encode(payload).or_else(|| {
        payload
            .get("data")
            .and_then(|d| d.as_object())
            .and_then(|m| m.values().find_map(encode))
    });
    match tabular {
        Some(enc) => Tier {
            kind: TierKind::Feather,
            bytes: enc.bytes,
            media: FEATHER_MEDIA,
        },
        None => Tier {
            kind: TierKind::Json,
            bytes: serde_json::to_vec(payload).unwrap_or_default(),
            media: JSON_MEDIA,
        },
    }
}

/// Parse the canonical
/// `noetl://<tenant>/<project>/results/<eid>/<step>/<frame>/<row>/<attempt>` URI
/// into coordinates. Returns `None` (never panics) for any non-result / too-short
/// / non-numeric-tail shape.
///
/// Transitional local inversion of `ResultCoordinates::logical_uri`, kept in
/// lockstep with the producer's stamp and `noetl_locator::ResultCoordinates::from_locator`.
pub fn coords_from_uri(uri: &str) -> Option<ResultCoordinates> {
    let rest = uri.strip_prefix("noetl://")?;
    let segs: Vec<&str> = rest.split('/').collect();
    // tenant / project / "results" / eid / step… / frame / row / attempt
    if segs.len() < 8 || segs[2] != "results" {
        return None;
    }
    let tenant = segs[0];
    let project = segs[1];
    if tenant.is_empty() || project.is_empty() {
        return None;
    }
    let n = segs.len();
    let execution_id = segs[3].parse::<i64>().ok()?;
    let frame = segs[n - 3].parse::<u64>().ok()?;
    let row = segs[n - 2].parse::<u64>().ok()?;
    let attempt = segs[n - 1].parse::<u32>().ok()?;
    let step = segs[4..n - 3].join("/");
    if step.is_empty() {
        return None;
    }
    Some(ResultCoordinates::new(
        Some(tenant),
        Some(project),
        execution_id,
        step,
        frame,
        row,
        attempt,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coords_from_uri_round_trips_and_rejects() {
        let c = coords_from_uri("noetl://t_acme/p_gen/results/325/load_next/2/4/1").unwrap();
        assert_eq!(c.tenant, "t_acme");
        assert_eq!(c.project, "p_gen");
        assert_eq!(c.execution_id, 325);
        assert_eq!(c.step, "load_next");
        assert_eq!(c.frame, 2);
        assert_eq!(c.row, 4);
        assert_eq!(c.attempt, 1);
        assert_eq!(
            c.logical_uri(),
            "noetl://t_acme/p_gen/results/325/load_next/2/4/1"
        );
        // Wrong kind / too short / non-numeric tail → None (never panics).
        assert!(coords_from_uri("noetl://t/p/datasets/1/s/0/0/1").is_none());
        assert!(coords_from_uri("noetl://t/p/results/1/s/0").is_none());
        assert!(coords_from_uri("noetl://t/p/results/1/s/0/0/x").is_none());
        assert!(coords_from_uri("https://nope").is_none());
    }

    #[test]
    fn step_with_slash_survives() {
        let c = coords_from_uri("noetl://default/default/results/9/a/b/c/3/7/2").unwrap();
        assert_eq!(c.step, "a/b/c");
        assert_eq!(c.frame, 3);
        assert_eq!(c.row, 7);
        assert_eq!(c.attempt, 2);
    }

    // --- Shared result-tier encoding (used by the materializer AND the
    //     producer-staging path; #104 Phase B / OQ5 Option A) ----------------

    #[test]
    fn decide_tier_tabular_is_feather() {
        // Canonical {columns, rows} → Arrow Feather.
        let tabular = serde_json::json!({
            "columns": ["id", "name"],
            "rows": [[1, "a"], [2, "b"]]
        });
        let tier = decide_tier(&tabular);
        assert_eq!(tier.kind, TierKind::Feather);
        assert_eq!(tier.media, FEATHER_MEDIA);
        assert_eq!(tier.ext(), "feather");
        assert!(!tier.bytes.is_empty());
    }

    #[test]
    fn decide_tier_rowset_under_data_envelope_is_feather() {
        // The realistic over-budget tool result: the worker stores
        // `{data: {<tool>: {columns, rows}}, status, stdout, …}`. The rowset is
        // nested under the conventional `data.<tool>` envelope — still Feather.
        let envelope = serde_json::json!({
            "status": "ok",
            "stdout": "",
            "exit_code": 0,
            "data": {
                "run_query": {
                    "columns": ["id", "name"],
                    "rows": [[1, "a"], [2, "b"], [3, "c"]]
                }
            }
        });
        let tier = decide_tier(&envelope);
        assert_eq!(tier.kind, TierKind::Feather, "data.<tool> rowset should tier as Feather");
        assert_eq!(tier.media, FEATHER_MEDIA);
        assert!(!tier.bytes.is_empty());
    }

    #[test]
    fn decide_tier_non_tabular_is_json() {
        // Opaque shape (HTTP JSON / shell stdout) → JSON fallback (OQ3).
        let blob = serde_json::json!({ "stdout": "hello", "code": 0, "nested": { "a": [1, 2, 3] } });
        let tier = decide_tier(&blob);
        assert_eq!(tier.kind, TierKind::Json);
        assert_eq!(tier.media, JSON_MEDIA);
        assert_eq!(tier.ext(), "json");
        // Round-trips back to the same JSON.
        let back: serde_json::Value = serde_json::from_slice(&tier.bytes).unwrap();
        assert_eq!(back, blob);
    }

    #[test]
    fn tier_encode_is_deterministic_byte_identical() {
        // The byte-identical guarantee (DR re-derive AND producer-staging) rests
        // on a deterministic encode: the same payload encodes to the same bytes
        // every time, so a producer-staged object equals what the materializer
        // would write. Tabular (Feather) and non-tabular (JSON) both.
        let tabular = serde_json::json!({ "columns": ["id"], "rows": [[1], [2], [3]] });
        assert_eq!(
            decide_tier(&tabular).bytes,
            decide_tier(&tabular).bytes,
            "Feather encode must be byte-identical across runs"
        );
        let blob = serde_json::json!({ "stdout": "hello", "nested": { "a": [1, 2, 3] } });
        assert_eq!(
            decide_tier(&blob).bytes,
            decide_tier(&blob).bytes,
            "JSON encode must be byte-identical across runs"
        );
    }

    #[test]
    fn json_tier_byte_identical_across_key_reordering() {
        // Byte-identity must survive a result-store round-trip that can reorder
        // the envelope's keys. serde_json::Value is BTreeMap-backed (the worker
        // does not enable `preserve_order`), so `to_vec` is key-sorted: two
        // equal-but-differently-constructed values encode to identical bytes.
        // This is the load-bearing property for producer-vs-materializer JSON
        // byte-identity across the PUT round-trip.
        let a = serde_json::json!({ "stdout": "x", "code": 0, "nested": { "a": 1, "b": 2 } });
        let b = serde_json::json!({ "nested": { "b": 2, "a": 1 }, "code": 0, "stdout": "x" });
        assert_eq!(a, b, "the two values are equal");
        assert_eq!(
            decide_tier(&a).bytes,
            decide_tier(&b).bytes,
            "JSON-tier bytes must be independent of construction/round-trip key order"
        );
    }
}
