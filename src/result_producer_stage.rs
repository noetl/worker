//! Producer-staged result tier ([noetl/ai-meta#104](https://github.com/noetl/ai-meta/issues/104)
//! OQ5, **Option A**).
//!
//! ## What it is
//!
//! When a tool result is **over-budget** the producing worker already builds the
//! durable `result_ref` (dual-write to `noetl.result_store`) and stamps the
//! canonical logical `reference.uri`. This module adds one more thing **at emit
//! time, on the producing worker**: it derives the §7 physical key from the
//! canonical coordinates + the server-served cell registry, encodes the payload
//! with the **same** [`crate::result_locator::decide_tier`] the materializer
//! uses, and `PUT`s the object directly via `PUT /api/internal/objects/{key}`.
//!
//! ## Why (the OQ5 prerequisite to retiring `result_store`)
//!
//! Before this, the **only** writer of the Feather/JSON tier was the materializer
//! ([`crate::result_materializer`]), and it sourced the bytes by **reading
//! `result_store`** (`GET /api/result/resolve`). That read is the hard coupling
//! that blocks retiring `result_store`. Staging the object at the producer
//! decouples the tier write from `result_store`: the tier object exists without
//! the materializer reading the store. The materializer keeps its role (shadow /
//! authoritative / DR), but for producer-staged objects it **skips the
//! `result_store` fetch** (`write_shadow` skip-on-exists) — so once every
//! producer stages, nothing reads `result_store` for the tier and the dual-write
//! can be dropped (still gated on the OQ5 metric/time soak).
//!
//! ## Byte-identical
//!
//! The producer encodes the **same payload** the materializer would fetch back
//! (the scrubbed `result.context` the worker hands to `put_result`), through the
//! **same** deterministic [`decide_tier`], at the **same** §7 key the read path
//! ([`crate::result_resolver::resolve_by_urn`]) reconstructs — so a producer-
//! staged object is byte-identical to a materializer-written one. The §7 key is
//! content-addressed and idempotent, so a producer write + a materializer write
//! (or a DR repair) are safe overwrites of identical bytes.
//!
//! ## Best-effort / never fails the cycle
//!
//! Staging is an **acceleration**, never a new failure path. Any
//! parse/registry/encode/write error is counted on
//! `noetl_worker_result_producer_stage_total{outcome}` and logged with
//! `execution_id`, then swallowed — `result_store` remains the authoritative
//! dual-write, so a staging miss is invisible to the execution (the materializer
//! covers it).
//!
//! Opt-in: active only when `NOETL_RESULT_PRODUCER_STAGE` is truthy. Default off
//! → never called → true no-op (the emit path is byte-identical to today).

use serde_json::Value;

use crate::client::ControlPlaneClient;
use crate::result_locator::{coords_from_uri, decide_tier, TierKind};

/// True when `NOETL_RESULT_PRODUCER_STAGE` is set to a truthy value — the
/// #104 OQ5 Option A producer-staging flag. Default off → byte-identical to the
/// Phase A–F emit path (this module is never consulted).
pub fn enabled() -> bool {
    matches!(
        std::env::var("NOETL_RESULT_PRODUCER_STAGE")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Stage an over-budget result's tier object at emit time (best-effort).
///
/// `canonical_uri` is the same `noetl://…` logical URI stamped onto
/// `reference.uri` (so the §7 key matches the read path), and `payload` is the
/// scrubbed `result.context` the worker also hands to `put_result` (so the bytes
/// match what the materializer would fetch back). Never panics, never propagates
/// — records its outcome and returns.
pub async fn stage(client: &ControlPlaneClient, canonical_uri: &str, payload: &Value) {
    let Some(coords) = coords_from_uri(canonical_uri) else {
        crate::metrics::record_result_producer_stage("skip_parse_uri");
        return;
    };
    let eid = coords.execution_id;

    let Some(placement) = crate::result_resolver::placement_for(client, &coords).await else {
        // No registry → cannot derive a stable key. Decline rather than guess;
        // the materializer (CellSeed-seeded) still writes the tier.
        crate::metrics::record_result_producer_stage("skip_registry");
        tracing::debug!(execution_id = eid, "producer-stage: cell registry unavailable; skipping (materializer covers it)");
        return;
    };

    let tier = decide_tier(payload);
    // The `date=` partition derives from the execution_id snowflake (not wall
    // clock), so the write key == the read key reconstructed from the URI alone.
    let date = crate::snowflake::date_partition(coords.execution_id);
    let key = coords.physical_key(&placement, &date, tier.ext());

    let outcome = match tier.kind {
        TierKind::Feather => "staged_feather",
        TierKind::Json => "staged_json",
    };
    match client.object_put(&key, tier.bytes, tier.media).await {
        Ok(()) => {
            crate::metrics::record_result_producer_stage(outcome);
            tracing::debug!(
                execution_id = eid,
                object_key = %key,
                tier = tier.kind.label(),
                "producer-staged result tier object (#104 OQ5 Option A)"
            );
        }
        Err(e) => {
            crate::metrics::record_result_producer_stage("error");
            tracing::warn!(
                execution_id = eid,
                object_key = %key,
                error = %e,
                "producer-stage object_put failed (best-effort; materializer covers it)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabled_defaults_off_and_truthy_values_arm() {
        std::env::remove_var("NOETL_RESULT_PRODUCER_STAGE");
        assert!(!enabled(), "default off → byte-identical to today");
        for v in ["1", "true", "yes", "on", "TRUE", "On"] {
            std::env::set_var("NOETL_RESULT_PRODUCER_STAGE", v);
            assert!(enabled(), "value {v:?} should be truthy");
        }
        std::env::set_var("NOETL_RESULT_PRODUCER_STAGE", "false");
        assert!(!enabled());
        std::env::remove_var("NOETL_RESULT_PRODUCER_STAGE");
    }

    use axum::{
        body::Bytes,
        extract::Path,
        routing::{get, put},
        Json, Router,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    /// stage() writes the tier object via `PUT /api/internal/objects/{key}` and
    /// NEVER touches `result_store` (`GET /api/result/resolve` / `PUT
    /// /api/result/{eid}`) — Option A's whole point: the tier write is decoupled
    /// from the store. It also proves (a) the staged bytes are byte-identical to
    /// `decide_tier` (the same encoder the materializer uses) and (b) the §7 key
    /// is exactly the one the resolve-by-URN read path reconstructs.
    #[tokio::test]
    async fn stage_writes_object_via_object_put_not_result_store() {
        let put_key: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let put_bytes: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let resolve_hit = Arc::new(AtomicBool::new(false));
        let result_put_hit = Arc::new(AtomicBool::new(false));

        let registry_json = serde_json::json!({
            "shard_count": 256,
            "default_cell": "local-0",
            "cells": [{
                "cell": "local-0", "env": "dev", "region": "local",
                "provider": "gcs", "bucket": "noetl-results", "endpoint": "http://fake-gcs:4443"
            }]
        });

        let pk = Arc::clone(&put_key);
        let pb = Arc::clone(&put_bytes);
        let rh = Arc::clone(&resolve_hit);
        let rph = Arc::clone(&result_put_hit);
        let reg_for_route = registry_json.clone();

        let app = Router::new()
            .route(
                "/api/internal/cells",
                get(move || {
                    let r = reg_for_route.clone();
                    async move { Json(r) }
                }),
            )
            .route(
                "/api/internal/objects/{*key}",
                put(move |Path(key): Path<String>, body: Bytes| {
                    let pk = Arc::clone(&pk);
                    let pb = Arc::clone(&pb);
                    async move {
                        *pk.lock().unwrap() = Some(key);
                        *pb.lock().unwrap() = body.to_vec();
                        axum::http::StatusCode::OK
                    }
                }),
            )
            .route(
                "/api/result/resolve",
                get(move || {
                    let rh = Arc::clone(&rh);
                    async move {
                        rh.store(true, Ordering::SeqCst);
                        Json(serde_json::json!({}))
                    }
                }),
            )
            .route(
                "/api/result/{eid}",
                put(move || {
                    let rph = Arc::clone(&rph);
                    async move {
                        rph.store(true, Ordering::SeqCst);
                        axum::http::StatusCode::OK
                    }
                }),
            );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client = ControlPlaneClient::new(&base);
        let payload = serde_json::json!({ "columns": ["id"], "rows": [[1], [2], [3]] });
        let uri = "noetl://t_acme/p_gen/results/325/load/2/4/1";

        stage(&client, uri, &payload).await;

        // (1) The producer wrote the tier object — `object_put` was called.
        let key = put_key.lock().unwrap().clone().expect("stage must PUT the object");
        assert!(key.ends_with("/results/load/2/4/1.feather"), "key tail: {key}");

        // (2) Byte-identical: the staged bytes equal the shared `decide_tier`
        // encode the materializer would write.
        assert_eq!(
            *put_bytes.lock().unwrap(),
            decide_tier(&payload).bytes,
            "producer-staged bytes must equal the materializer's decide_tier encode"
        );

        // (3) §7 key matches the resolve-by-URN read path: reconstruct the key the
        // resolver derives from the SAME URI + the SAME (now-cached) registry.
        let coords = coords_from_uri(uri).unwrap();
        let placement = crate::result_resolver::placement_for(&client, &coords).await.unwrap();
        let date = crate::snowflake::date_partition(coords.execution_id);
        assert_eq!(
            key,
            coords.physical_key(&placement, &date, "feather"),
            "§7 key must match the resolve-by-URN read path"
        );

        // (4) result_store was NOT touched — the tier write is decoupled.
        assert!(!resolve_hit.load(Ordering::SeqCst), "stage must NOT read result_store");
        assert!(!result_put_hit.load(Ordering::SeqCst), "stage must NOT write result_store");
    }
}
