//! Resolve-by-URN read path ([noetl/ai-meta#104](https://github.com/noetl/ai-meta/issues/104) Phase C).
//!
//! The first phase that **reads** the Feather/JSON result tier Phase B writes.
//! When a downstream step binds the bulk of an over-budget upstream result, the
//! worker resolves the result's canonical logical URI → cell placement (from the
//! server-served registry, §4.3) → derived §7 physical key → object bytes (from
//! the server-mediated object store, now GCS-backed) → the JSON payload — instead
//! of the legacy `noetl.result_store` fetch.
//!
//! ## Fail-safe (OQ6 resolved)
//!
//! Resolve-by-URN is an **optimization**, never a new failure path. On any
//! registry miss, object miss, object-store error, or decode failure the caller
//! falls back to the authoritative `resolve_ref` (`noetl.result_store` / inline)
//! and increments a fallback metric — never a hard failure, never silent data
//! loss. The authoritative store is still written until the Phase D minting flip,
//! so flag-on is safe and reversible.
//!
//! Gated by `NOETL_RESULT_URI_RESOLVE` (default off). Flag-off → this module is
//! never consulted and the read path is byte-identical to today (legacy
//! `resolve_ref`).

use std::time::Instant;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::OnceCell;

use noetl_tools::locator::{CellPlacement, ResultCoordinates};

use crate::client::ControlPlaneClient;
use crate::result_locator::coords_from_uri;

/// True when `NOETL_RESULT_URI_RESOLVE` is set to a truthy value.
pub fn enabled() -> bool {
    truthy_env("NOETL_RESULT_URI_RESOLVE")
}

/// True when `NOETL_RESULT_MINT_AUTHORITATIVE` is set to a truthy value — the
/// Phase D "minting flip" ([noetl/ai-meta#104](https://github.com/noetl/ai-meta/issues/104)).
///
/// The flip makes the URN → Feather/GCS result tier the **authoritative** result
/// store, with `noetl.result_store` demoted to the transitional **dual-write
/// fallback** for reversibility. One flag turns the whole flip on:
///
/// - **Write side** (system pool): the result materializer is the authoritative
///   tier writer — `mint_authoritative()` enables it even if
///   `NOETL_RESULT_MATERIALIZER_ENABLED` is unset
///   ([`crate::result_materializer`]).
/// - **Read side** (consume): resolve-by-URN is the **primary** path —
///   `mint_authoritative()` enables it even if `NOETL_RESULT_URI_RESOLVE` is
///   unset ([`resolve_context_references`](crate::executor::command)). A tier
///   miss / parse failure still falls back fail-safe to the dual-written
///   `result_store` (rollback safety), recorded on
///   `noetl_worker_result_mint_authoritative_total{path}`.
///
/// Default off → byte-identical to Phase A–C (a true no-op). The dual-write to
/// `result_store` continues until the OQ5-gated retirement decision (NOT Phase
/// D), so flag-off rolls back cleanly to `result_store`-authoritative.
pub fn mint_authoritative() -> bool {
    truthy_env("NOETL_RESULT_MINT_AUTHORITATIVE")
}

fn truthy_env(key: &str) -> bool {
    matches!(
        std::env::var(key)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

// ---------------------------------------------------------------------------
// Cell endpoint registry (read side) — process-cached
// ---------------------------------------------------------------------------

/// One cell's placement (the read-side view of the server's registry entry).
#[derive(Debug, Clone, Deserialize)]
pub struct CellEntry {
    pub cell: String,
    pub env: String,
    pub region: String,
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub endpoint: String,
}

/// The server-served cell endpoint registry (`GET /api/internal/cells`).
#[derive(Debug, Clone, Deserialize)]
pub struct CellRegistry {
    pub shard_count: u32,
    pub default_cell: String,
    pub cells: Vec<CellEntry>,
}

impl CellRegistry {
    /// Resolve a result's placement: derive the shard from the stable shard hash,
    /// home it on the cell the registry names. `None` if no cell can be resolved
    /// (→ fail-safe fallback).
    fn placement_for(&self, coords: &ResultCoordinates) -> Option<CellPlacement> {
        let entry = self
            .cells
            .iter()
            .find(|c| c.cell == self.default_cell)
            .or_else(|| self.cells.first())?;
        let shard = coords.shard_key(self.shard_count.max(1));
        Some(CellPlacement::new(&entry.env, &entry.region, &entry.cell, shard))
    }
}

/// Process-global registry cache. Only **successful** fetches are cached
/// (`get_or_try_init` does not cache the error), so a transient registry outage
/// degrades to fail-safe fallback and recovers on the next resolve.
static REGISTRY: OnceCell<CellRegistry> = OnceCell::const_new();

async fn registry(client: &ControlPlaneClient) -> Option<&'static CellRegistry> {
    REGISTRY
        .get_or_try_init(|| async {
            let raw = client.cell_registry().await?;
            serde_json::from_value::<CellRegistry>(raw).map_err(anyhow::Error::from)
        })
        .await
        .ok()
}

// ---------------------------------------------------------------------------
// Resolve
// ---------------------------------------------------------------------------

/// Resolve an over-budget result by its canonical logical URI. Returns the JSON
/// payload on success (→ caller uses it instead of the legacy fetch), or `None`
/// to signal the caller should fall back to `resolve_ref`. Records the outcome
/// metric in every branch. Never panics.
pub async fn resolve_by_urn(client: &ControlPlaneClient, canonical_uri: &str) -> Option<Value> {
    let t0 = Instant::now();
    match try_resolve(client, canonical_uri).await {
        Ok((value, outcome)) => {
            crate::metrics::record_result_resolve(outcome, t0.elapsed().as_secs_f64());
            Some(value)
        }
        Err(outcome) => {
            crate::metrics::record_result_resolve(outcome, t0.elapsed().as_secs_f64());
            None
        }
    }
}

/// The resolve attempt. `Ok((value, "resolved_*"))` on a hit; `Err("fallback_*")`
/// on any miss/error (the caller then falls back fail-safe).
async fn try_resolve(
    client: &ControlPlaneClient,
    canonical_uri: &str,
) -> Result<(Value, &'static str), &'static str> {
    let coords = coords_from_uri(canonical_uri).ok_or("fallback_parse_uri")?;
    let reg = registry(client).await.ok_or("fallback_registry")?;
    let placement = reg.placement_for(&coords).ok_or("fallback_registry")?;
    let date = crate::snowflake::date_partition(coords.execution_id);

    // Try the Feather tier first, then the JSON tier. The tier (ext) is not
    // carried; at most one extra 404 round-trip distinguishes them.
    let feather_key = coords.physical_key(&placement, &date, "feather");
    match client.object_get(&feather_key).await {
        Ok(Some((bytes, _ct))) => {
            return decode_feather_to_rows_json(&bytes)
                .map(|v| (v, "resolved_feather"))
                .map_err(|_| "fallback_decode");
        }
        Ok(None) => {}
        Err(_) => return Err("fallback_object_error"),
    }

    let json_key = coords.physical_key(&placement, &date, "json");
    match client.object_get(&json_key).await {
        Ok(Some((bytes, _ct))) => serde_json::from_slice::<Value>(&bytes)
            .map(|v| (v, "resolved_json"))
            .map_err(|_| "fallback_decode"),
        Ok(None) => Err("fallback_object_miss"),
        Err(_) => Err("fallback_object_error"),
    }
}

// ---------------------------------------------------------------------------
// Feather → JSON decode
// ---------------------------------------------------------------------------

/// Decode Arrow Feather/IPC bytes back into the canonical `{columns, rows}`
/// rowset JSON — the inverse of `noetl_tools::arrow_codec::try_encode_tabular_json`'s
/// rowset extraction (the encoder only emits Int64 / Float64 / Boolean / Utf8
/// columns + nulls, which is exactly what this reconstructs). Each row is an
/// array of cell values aligned to `columns`, so a `{{ step.rows[i][j] }}`
/// template reads it back identically.
fn decode_feather_to_rows_json(bytes: &[u8]) -> anyhow::Result<Value> {
    let batches = noetl_tools::arrow_codec::decode_record_batches(bytes)?;
    let Some(first) = batches.first() else {
        return Ok(json!({ "columns": [], "rows": [] }));
    };
    let columns: Vec<String> = first
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut rows: Vec<Value> = Vec::new();
    for batch in &batches {
        let ncols = batch.num_columns();
        for r in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(ncols);
            for c in 0..ncols {
                row.push(cell_to_json(batch.column(c).as_ref(), r));
            }
            rows.push(Value::Array(row));
        }
    }
    Ok(json!({ "columns": columns, "rows": rows }))
}

/// One Arrow cell → JSON. Handles the four types the encoder emits; nulls and any
/// unexpected type map to `null` (never panics).
fn cell_to_json(arr: &dyn Array, idx: usize) -> Value {
    if arr.is_null(idx) {
        return Value::Null;
    }
    match arr.data_type() {
        DataType::Int64 => arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| json!(a.value(idx)))
            .unwrap_or(Value::Null),
        DataType::Float64 => arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| json!(a.value(idx)))
            .unwrap_or(Value::Null),
        DataType::Boolean => arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| json!(a.value(idx)))
            .unwrap_or(Value::Null),
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| json!(a.value(idx)))
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabled_defaults_off() {
        std::env::remove_var("NOETL_RESULT_URI_RESOLVE");
        assert!(!enabled());
        std::env::set_var("NOETL_RESULT_URI_RESOLVE", "true");
        assert!(enabled());
        std::env::remove_var("NOETL_RESULT_URI_RESOLVE");
    }

    #[test]
    fn mint_authoritative_defaults_off() {
        // Phase D flag: default off → byte-identical to A–C; truthy values flip it.
        std::env::remove_var("NOETL_RESULT_MINT_AUTHORITATIVE");
        assert!(!mint_authoritative());
        for v in ["1", "true", "yes", "on", "TRUE"] {
            std::env::set_var("NOETL_RESULT_MINT_AUTHORITATIVE", v);
            assert!(mint_authoritative(), "value {v:?} should be truthy");
        }
        std::env::set_var("NOETL_RESULT_MINT_AUTHORITATIVE", "false");
        assert!(!mint_authoritative());
        std::env::remove_var("NOETL_RESULT_MINT_AUTHORITATIVE");
    }

    #[test]
    fn placement_derivation_matches_write_side_seed() {
        // The registry's single-cell seed + the stable shard hash must produce the
        // SAME placement the materializer's CellSeed produced, or keys won't match.
        let reg = CellRegistry {
            shard_count: 256,
            default_cell: "local-0".into(),
            cells: vec![CellEntry {
                cell: "local-0".into(),
                env: "dev".into(),
                region: "local".into(),
                provider: "gcs".into(),
                bucket: "noetl-results".into(),
                endpoint: "http://fake-gcs:4443".into(),
            }],
        };
        let coords = coords_from_uri("noetl://default/default/results/325/load/2/4/1").unwrap();
        let placement = reg.placement_for(&coords).unwrap();
        let key = coords.physical_key(&placement, "2026-06-22", "feather");
        // Same shape the Phase B materializer wrote (env/region/cell from the seed,
        // shard from the stable hash).
        assert!(key.contains("env=dev"));
        assert!(key.contains("region=local"));
        assert!(key.contains("cell=local-0"));
        assert!(key.ends_with("/results/load/2/4/1.feather"));
        // shard segment is the stable hash, formatted s%04
        let shard = coords.shard_key(256);
        assert!(key.contains(&format!("shard=s{shard:04}")));
    }

    #[test]
    fn feather_round_trips_to_rowset_json() {
        // encode (Phase B path) → decode (Phase C path) is structurally identical
        // for the canonical {columns, rows} rowset.
        let rowset = json!({
            "columns": ["id", "name", "score", "ok"],
            "rows": [
                [1, "a", 1.5, true],
                [2, "b", 2.5, false]
            ]
        });
        let enc = noetl_tools::arrow_codec::try_encode_tabular_json(&rowset)
            .expect("rowset encodes to Feather");
        let decoded = decode_feather_to_rows_json(&enc.bytes).expect("decode");
        assert_eq!(decoded["columns"], json!(["id", "name", "score", "ok"]));
        assert_eq!(decoded["rows"][0], json!([1, "a", 1.5, true]));
        assert_eq!(decoded["rows"][1], json!([2, "b", 2.5, false]));
    }

    #[test]
    fn feather_decode_handles_nulls() {
        let rowset = json!({
            "columns": ["id", "name"],
            "rows": [[1, "a"], [null, null]]
        });
        let enc = noetl_tools::arrow_codec::try_encode_tabular_json(&rowset).unwrap();
        let decoded = decode_feather_to_rows_json(&enc.bytes).unwrap();
        assert_eq!(decoded["rows"][1], json!([null, null]));
    }

    #[test]
    fn parse_uri_failure_is_fallback() {
        // A non-canonical URI yields the parse-uri fallback outcome label (no
        // network needed — coords_from_uri rejects it).
        assert!(coords_from_uri("noetl://execution/1/result/s/2").is_none());
    }
}
