//! Result materializer — the **shadow** Feather/JSON result tier writer
//! ([noetl/ai-meta#104](https://github.com/noetl/ai-meta/issues/104) Phase B).
//!
//! ## What it is
//!
//! A second system-pool consume-loop on the `noetl_events` JetStream WAL,
//! **sibling** to the event materializer ([`crate::materializer`]) but on its
//! **own** durable consumer (`noetl_result_materializer`). It reads `call.done`
//! events that carry an **over-budget** result reference and writes the result
//! payload to object store at the derivable §7 physical key:
//!
//! - tabular payload → Arrow **Feather** (`noetl_tools::arrow_codec`),
//! - non-tabular payload → **JSON** (OQ3 decided: JSON, not Parquet),
//! - small / inline results never carry a reference → no write (true no-op).
//!
//! ## Why a separate consumer (not folded into the event materializer)
//!
//! Object-store latency must **never** back-pressure the `noetl.event` audit
//! fold that the materializer-lag alert guards (the #103 sole-writer path). A
//! distinct consumer with its own ack cursor isolates the result I/O entirely —
//! a slow/erroring object store stalls only this loop, never the event
//! materialize / off-server drive path.
//!
//! ## Shadow / non-authoritative
//!
//! This is **shadow** (RFC Phase B): it writes the Feather tier *alongside* the
//! authoritative `noetl.result_store` path; nothing READS the Feather tier yet
//! (that is Phase C). Two consequences are load-bearing and tested:
//!
//! 1. **Never alters the authoritative result.** The loop only ever *reads* the
//!    stored payload (`GET /api/result/resolve`) and *writes a new object*
//!    (`PUT /api/internal/objects/{key}`). It never mutates `noetl.result_store`
//!    or the event.
//! 2. **Never fails an event.** A classify/fetch/encode/write error is counted
//!    and WARN-logged (with `execution_id`), then the batch is **acked anyway**
//!    so the shadow tier can never wedge its own consumer or perturb anything.
//!    Object writes are idempotent (content-addressed §7 key, `ON CONFLICT DO
//!    UPDATE`), so an at-least-once redelivery is a safe overwrite.
//!
//! Per [`data-access-boundary.md`](https://github.com/noetl/ai-meta/blob/main/agents/rules/data-access-boundary.md)
//! every data touch is server-mediated — the loop never reaches the object
//! store or `noetl.*` directly.
//!
//! Opt-in: spawned only when `NOETL_RESULT_MATERIALIZER_ENABLED` is truthy
//! (set on the system worker pool). Default off → not spawned → true no-op.

use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use noetl_tools::tools::source::{AckDisposition, AckMode, PollOptions};
use noetl_tools::tools::{build_source, SubscriptionConfig};
use noetl_tools::locator::{CellPlacement, ResultCoordinates, DEFAULT_SHARD_COUNT};
use noetl_tools::ExecutionContext;

use crate::client::ControlPlaneClient;
use crate::config::WorkerConfig;
use crate::materializer::{env_u32, env_u64, parse_nats_credentials, EVENT_STREAM};

/// Durable consumer the result materializer drains — distinct from the event
/// materializer's `noetl_materializer` (the server ensures it at startup).
pub const RESULT_MATERIALIZER_CONSUMER: &str = "noetl_result_materializer";

const DEFAULT_BATCH: u32 = 200;
const DEFAULT_TIMEOUT_MS: u64 = 2_000;
const DEFAULT_IDLE_SLEEP_MS: u64 = 500;
const DEFAULT_ERROR_BACKOFF_MS: u64 = 2_000;

/// Media type stamped on a Feather (Arrow IPC) object.
const FEATHER_MEDIA: &str = "application/vnd.apache.arrow.feather";
/// Media type stamped on a JSON fallback object.
const JSON_MEDIA: &str = "application/json";

/// True when `NOETL_RESULT_MATERIALIZER_ENABLED` is set to a truthy value.
pub fn enabled() -> bool {
    matches!(
        std::env::var("NOETL_RESULT_MATERIALIZER_ENABLED")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// The single resolved cell placement for a single-cell deployment (RFC §4.3
/// "single-cell seed"). Phase B is write-side only: every shard resolves to the
/// one configured cell, so no registry lookup can miss (the open part of OQ6 —
/// server-served registry + multi-cell miss behavior — does not arise with one
/// cell and is deferred to Phase C, where the read path needs it).
///
/// Seeded from env: `NOETL_RESULT_CELL_ENV` / `_REGION` / `_CELL`, with the
/// numeric shard derived from the result's stable `shard_key` so the §7 key's
/// `shard=` segment is meaningful even in the single-cell seed.
#[derive(Clone)]
pub struct CellSeed {
    pub env: String,
    pub region: String,
    pub cell: String,
    pub shard_count: u32,
}

impl CellSeed {
    fn from_env() -> Self {
        Self {
            env: std::env::var("NOETL_RESULT_CELL_ENV").unwrap_or_else(|_| "dev".to_string()),
            region: std::env::var("NOETL_RESULT_CELL_REGION").unwrap_or_else(|_| "local".to_string()),
            cell: std::env::var("NOETL_RESULT_CELL").unwrap_or_else(|_| "local-0".to_string()),
            shard_count: env_u32("NOETL_RESULT_SHARD_COUNT", DEFAULT_SHARD_COUNT).max(1),
        }
    }

    /// Resolve the placement for a result's coordinates: derive the shard from
    /// the stable shard hash, then home it on the one configured cell.
    fn placement_for(&self, coords: &ResultCoordinates) -> CellPlacement {
        let shard = coords.shard_key(self.shard_count);
        CellPlacement::new(&self.env, &self.region, &self.cell, shard)
    }
}

/// Resolved result-materializer configuration.
pub struct ResultMaterializerConfig {
    pub nats_url: String,
    pub nats_user: Option<String>,
    pub nats_password: Option<String>,
    pub stream: String,
    pub consumer: String,
    pub server_url: String,
    pub batch: u32,
    pub timeout_ms: u64,
    pub idle_sleep: Duration,
    pub error_backoff: Duration,
    pub cell: CellSeed,
}

impl ResultMaterializerConfig {
    /// Build from worker config + env. `Ok(None)` when disabled (default).
    pub fn from_env(worker: &WorkerConfig) -> Option<Self> {
        if !enabled() {
            return None;
        }
        let (nats_url, nats_user, nats_password) = parse_nats_credentials(&worker.nats_url);
        Some(Self {
            nats_url,
            nats_user,
            nats_password,
            stream: std::env::var("NOETL_RESULT_MATERIALIZER_STREAM")
                .unwrap_or_else(|_| EVENT_STREAM.to_string()),
            consumer: std::env::var("NOETL_RESULT_MATERIALIZER_CONSUMER")
                .unwrap_or_else(|_| RESULT_MATERIALIZER_CONSUMER.to_string()),
            server_url: worker.server_url.trim_end_matches('/').to_string(),
            batch: env_u32("NOETL_RESULT_MATERIALIZER_BATCH", DEFAULT_BATCH).clamp(1, 1000),
            timeout_ms: env_u64("NOETL_RESULT_MATERIALIZER_TIMEOUT_MS", DEFAULT_TIMEOUT_MS),
            idle_sleep: Duration::from_millis(env_u64(
                "NOETL_RESULT_MATERIALIZER_IDLE_SLEEP_MS",
                DEFAULT_IDLE_SLEEP_MS,
            )),
            error_backoff: Duration::from_millis(env_u64(
                "NOETL_RESULT_MATERIALIZER_ERROR_BACKOFF_MS",
                DEFAULT_ERROR_BACKOFF_MS,
            )),
            cell: CellSeed::from_env(),
        })
    }

    fn source_config(&self) -> Result<SubscriptionConfig> {
        let mut cfg = serde_json::Map::new();
        cfg.insert("source".into(), serde_json::json!("nats"));
        cfg.insert("url".into(), serde_json::json!(self.nats_url));
        if let Some(u) = &self.nats_user {
            cfg.insert("user".into(), serde_json::json!(u));
        }
        if let Some(p) = &self.nats_password {
            cfg.insert("password".into(), serde_json::json!(p));
        }
        cfg.insert("stream".into(), serde_json::json!(self.stream));
        cfg.insert("consumer".into(), serde_json::json!(self.consumer));
        serde_json::from_value(serde_json::Value::Object(cfg))
            .map_err(|e| anyhow!("result materializer source config invalid: {e}"))
    }
}

/// Spawn the result-materializer loop, returning its join handle.
pub fn spawn(config: ResultMaterializerConfig, client: ControlPlaneClient) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = run_loop(config, client).await {
            tracing::error!(error = %e, "result materializer loop exited with error");
        }
    })
}

/// One drain → classify → (fetch → encode → object_put) → ack cycle, forever.
async fn run_loop(config: ResultMaterializerConfig, client: ControlPlaneClient) -> Result<()> {
    let source = build_source(&config.source_config()?, &ExecutionContext::default())
        .map_err(|e| anyhow!("result materializer build_source failed: {e}"))?;

    tracing::info!(
        stream = %config.stream,
        consumer = %config.consumer,
        batch = config.batch,
        cell = %config.cell.cell,
        "result materializer started (SHADOW Feather tier; #104 Phase B)"
    );

    // Deferred ack: we ack the batch ourselves after best-effort shadow writes.
    let opts = PollOptions::new(Some(config.batch), Some(config.timeout_ms), AckMode::Defer);

    loop {
        let cycle_start = Instant::now();
        let outcome = match source.poll(&opts).await {
            Ok(o) => o,
            Err(e) => {
                tracing::warn!(error = %e, "result materializer drain failed; backing off");
                tokio::time::sleep(config.error_backoff).await;
                continue;
            }
        };

        let drained = outcome.messages.len();
        if drained == 0 {
            tokio::time::sleep(config.idle_sleep).await;
            continue;
        }

        let mut tally = CycleTally::default();
        for msg in &outcome.messages {
            let row = match parse_row(&msg.data) {
                Some(r) => r,
                None => {
                    tally.skipped += 1;
                    continue;
                }
            };
            // Classify is a PURE decision (no I/O, no mutation) — eligibility +
            // the derived key. Only an eligible event does any work.
            match classify_event(&row) {
                Classification::Skip => tally.skipped += 1,
                Classification::Eligible { legacy_ref, coords } => {
                    write_shadow(&client, &config.cell, &row, &legacy_ref, &coords, &mut tally).await;
                }
            }
        }

        // SHADOW: always ack — the result tier must never wedge its own consumer
        // nor (it being a separate consumer) the event-materialize path. Failed
        // writes are counted; idempotent object keys make any future re-run safe.
        let report = source
            .ack(&outcome.ack_ids, AckDisposition::Ack)
            .await
            .unwrap_or_default();

        tracing::debug!(
            drained,
            eligible = tally.eligible,
            feather = tally.feather,
            json = tally.json,
            skipped = tally.skipped,
            errors = tally.errors,
            acked = report.disposed,
            "result materializer cycle"
        );
        crate::metrics::record_result_materializer_cycle(
            drained as u64,
            tally.eligible,
            tally.feather,
            tally.json,
            tally.skipped,
            tally.errors,
            cycle_start.elapsed().as_secs_f64(),
        );
    }
}

#[derive(Default)]
struct CycleTally {
    eligible: u64,
    feather: u64,
    json: u64,
    skipped: u64,
    errors: u64,
}

/// The fetch → encode → object_put half (the only I/O). Best-effort: any error
/// increments `errors` and WARN-logs with `execution_id`; never propagates.
async fn write_shadow(
    client: &ControlPlaneClient,
    cell: &CellSeed,
    row: &serde_json::Value,
    legacy_ref: &str,
    coords: &ResultCoordinates,
    tally: &mut CycleTally,
) {
    tally.eligible += 1;
    let eid = coords.execution_id;

    // Fetch the authoritative payload (read-only — never alters it).
    let payload = match client.resolve_ref(legacy_ref).await {
        Ok(Some(p)) => p,
        Ok(None) => {
            // GC'd / not yet durable — a benign skip for shadow, not an error.
            tracing::debug!(execution_id = eid, result_ref = legacy_ref, "result payload not found; shadow skip");
            tally.skipped += 1;
            tally.eligible -= 1;
            return;
        }
        Err(e) => {
            tally.errors += 1;
            tracing::warn!(execution_id = eid, result_ref = legacy_ref, error = %e, "result materializer resolve failed (shadow)");
            return;
        }
    };

    let tier = decide_tier(&payload);
    let date = event_date(row);
    let key = coords.physical_key(&cell.placement_for(coords), &date, tier.ext());

    match client.object_put(&key, tier.bytes, tier.media).await {
        Ok(()) => {
            match tier.kind {
                TierKind::Feather => tally.feather += 1,
                TierKind::Json => tally.json += 1,
            }
            tracing::debug!(execution_id = eid, object_key = %key, tier = tier.kind.label(), "shadow result object written");
        }
        Err(e) => {
            tally.errors += 1;
            tracing::warn!(execution_id = eid, object_key = %key, error = %e, "result materializer object_put failed (shadow)");
        }
    }
}

// ---------------------------------------------------------------------------
// Pure decision layer (unit-tested without any I/O)
// ---------------------------------------------------------------------------

/// Outcome of classifying one event row.
#[derive(Debug, PartialEq, Eq)]
enum Classification {
    /// Inline/small result (no over-budget reference), or a reference we cannot
    /// address (no canonical `uri`) — nothing to materialize.
    Skip,
    /// An over-budget result: the legacy ref to fetch the payload + the parsed
    /// canonical coordinates to derive the physical key.
    Eligible {
        legacy_ref: String,
        coords: ResultCoordinates,
    },
}

/// Parse a drained message payload into a JSON object (it may arrive as a JSON
/// value or a JSON string).
fn parse_row(data: &serde_json::Value) -> Option<serde_json::Value> {
    match data {
        serde_json::Value::Object(_) => Some(data.clone()),
        serde_json::Value::String(s) => serde_json::from_str::<serde_json::Value>(s).ok(),
        _ => None,
    }
}

/// Classify an event row: eligible only if it carries an over-budget result
/// reference (`kind: "result_ref"`) with both a legacy `ref` (to fetch) and a
/// canonical `uri` (to derive the key). Pure — never mutates `row`.
fn classify_event(row: &serde_json::Value) -> Classification {
    let Some(reference) = find_result_ref(row) else {
        return Classification::Skip;
    };
    let legacy_ref = reference
        .get("ref")
        .and_then(|v| v.as_str())
        .filter(|s| s.starts_with("noetl://"));
    let coords = reference
        .get("uri")
        .and_then(|v| v.as_str())
        .and_then(coords_from_uri);
    match (legacy_ref, coords) {
        (Some(r), Some(coords)) => Classification::Eligible {
            legacy_ref: r.to_string(),
            coords,
        },
        // Has a reference but we cannot address it (no canonical uri or no
        // fetchable ref) — skip rather than guess a key.
        _ => Classification::Skip,
    }
}

/// Recursively find the over-budget result-reference object: an object with
/// `"kind": "result_ref"`. Returns the first match in a depth-first walk.
fn find_result_ref(v: &serde_json::Value) -> Option<&serde_json::Map<String, serde_json::Value>> {
    match v {
        serde_json::Value::Object(m) => {
            if m.get("kind").and_then(|k| k.as_str()) == Some("result_ref") {
                return Some(m);
            }
            m.values().find_map(find_result_ref)
        }
        serde_json::Value::Array(a) => a.iter().find_map(find_result_ref),
        _ => None,
    }
}

/// Parse the canonical `noetl://<tenant>/<project>/results/<eid>/<step>/<frame>/<row>/<attempt>`
/// URI into coordinates.
///
/// Transitional: a local inversion of `ResultCoordinates::logical_uri` so Phase
/// B needs no `noetl-tools` release. The durable single source is
/// `noetl_locator::ResultCoordinates::from_locator` (tools, #104 Phase B); a
/// follow-up swaps this for it once the worker bumps `noetl-tools`. Kept in
/// lockstep with that parser + the producer's `logical_uri`.
fn coords_from_uri(uri: &str) -> Option<ResultCoordinates> {
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

enum TierKind {
    Feather,
    Json,
}

impl TierKind {
    fn label(&self) -> &'static str {
        match self {
            TierKind::Feather => "feather",
            TierKind::Json => "json",
        }
    }
}

/// The chosen result tier: encoded bytes + how to store them.
struct Tier {
    kind: TierKind,
    bytes: Vec<u8>,
    media: &'static str,
}

impl Tier {
    fn ext(&self) -> &'static str {
        match self.kind {
            TierKind::Feather => "feather",
            TierKind::Json => "json",
        }
    }
}

/// Decide the over-budget tier (OQ3 decided: non-tabular → JSON).
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
/// falls back to **JSON** of the whole payload. Encoding the rowset (not the
/// envelope) is intentional: the Feather tier holds the columnar payload, and
/// the bounded `extracted` block (carried inline) already covers guard/fan-out.
fn decide_tier(payload: &serde_json::Value) -> Tier {
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

/// The `date=` partition for the §7 key — derived from the event's own
/// timestamp (deterministic on replay, never wall-clock). Falls back to a fixed
/// sentinel when absent so a key is still produced.
fn event_date(row: &serde_json::Value) -> String {
    row.get("timestamp")
        .or_else(|| row.get("created_at"))
        .and_then(|v| v.as_str())
        .map(|s| s.chars().take(10).collect::<String>())
        .filter(|d| d.len() == 10)
        .unwrap_or_else(|| "0000-00-00".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A worker over-budget `call.done` shape (reference nested under the event
    /// result), with both the legacy `ref` and the canonical `uri`.
    fn over_budget_row(uri: &str) -> serde_json::Value {
        serde_json::json!({
            "event_id": 1, "execution_id": 325, "timestamp": "2026-06-22T10:11:12Z",
            "result": {
                "status": "completed",
                "context": { "data": { "_ref": "noetl://execution/325/result/s/9001" } },
                "reference": {
                    "kind": "result_ref",
                    "ref": "noetl://execution/325/result/s/9001",
                    "uri": uri,
                    "extracted": { "rows": 4 }
                }
            }
        })
    }

    #[test]
    fn classify_eligible_extracts_ref_and_coords() {
        let row = over_budget_row("noetl://default/default/results/325/load/2/4/1");
        match classify_event(&row) {
            Classification::Eligible { legacy_ref, coords } => {
                assert_eq!(legacy_ref, "noetl://execution/325/result/s/9001");
                assert_eq!(coords.execution_id, 325);
                assert_eq!(coords.step, "load");
                assert_eq!(coords.frame, 2);
                assert_eq!(coords.row, 4);
                assert_eq!(coords.attempt, 1);
            }
            other => panic!("expected Eligible, got {other:?}"),
        }
    }

    #[test]
    fn classify_inline_result_is_skip_no_mutation() {
        // Small/inline result: no reference → Skip → no write (the no-op).
        let row = serde_json::json!({
            "event_id": 2, "execution_id": 7,
            "result": { "status": "completed", "context": { "x": 1 } }
        });
        let before = row.clone();
        assert_eq!(classify_event(&row), Classification::Skip);
        // PURE: classification never touched the row.
        assert_eq!(row, before);
    }

    #[test]
    fn classify_reference_without_canonical_uri_is_skip() {
        // A result_ref carrying only the legacy `ref` (no `uri`) can't be keyed.
        let row = serde_json::json!({
            "result": { "reference": { "kind": "result_ref", "ref": "noetl://execution/1/result/s/2" } }
        });
        assert_eq!(classify_event(&row), Classification::Skip);
    }

    #[test]
    fn decide_tier_tabular_is_feather() {
        // Canonical {columns, rows} → Arrow Feather.
        let tabular = serde_json::json!({
            "columns": ["id", "name"],
            "rows": [[1, "a"], [2, "b"]]
        });
        let tier = decide_tier(&tabular);
        assert!(matches!(tier.kind, TierKind::Feather));
        assert_eq!(tier.media, FEATHER_MEDIA);
        assert_eq!(tier.ext(), "feather");
        assert!(!tier.bytes.is_empty());
    }

    #[test]
    fn decide_tier_rowset_under_data_envelope_is_feather() {
        // The realistic over-budget tool result: the worker stores
        // `{data: {<tool>: {columns, rows}}, status, stdout, …}`. The rowset is
        // nested under the conventional `data.<tool>` envelope — the materializer
        // still tiers it as Feather (encoding the rowset).
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
        assert!(matches!(tier.kind, TierKind::Feather), "data.<tool> rowset should tier as Feather");
        assert_eq!(tier.media, FEATHER_MEDIA);
        assert!(!tier.bytes.is_empty());
    }

    #[test]
    fn decide_tier_non_tabular_is_json() {
        // Opaque shape (HTTP JSON / shell stdout) → JSON fallback (OQ3).
        let blob = serde_json::json!({ "stdout": "hello", "code": 0, "nested": { "a": [1, 2, 3] } });
        let tier = decide_tier(&blob);
        assert!(matches!(tier.kind, TierKind::Json));
        assert_eq!(tier.media, JSON_MEDIA);
        assert_eq!(tier.ext(), "json");
        // Round-trips back to the same JSON.
        let back: serde_json::Value = serde_json::from_slice(&tier.bytes).unwrap();
        assert_eq!(back, blob);
    }

    #[test]
    fn coords_from_uri_round_trips_and_rejects() {
        let c = coords_from_uri("noetl://t_acme/p_gen/results/325/load_next/2/4/1").unwrap();
        assert_eq!(c.tenant, "t_acme");
        assert_eq!(c.project, "p_gen");
        assert_eq!(c.logical_uri(), "noetl://t_acme/p_gen/results/325/load_next/2/4/1");
        // Wrong kind / too short / non-numeric tail → None (never panics).
        assert!(coords_from_uri("noetl://t/p/datasets/1/s/0/0/1").is_none());
        assert!(coords_from_uri("noetl://t/p/results/1/s/0").is_none());
        assert!(coords_from_uri("noetl://t/p/results/1/s/0/0/x").is_none());
        assert!(coords_from_uri("https://nope").is_none());
    }

    #[test]
    fn keep_every_attempt_distinct_keys() {
        // Keep-every (OQ1): attempt is in the URN, so two attempts of the same
        // (eid, step, frame, row) derive DISTINCT physical keys — never an
        // overwrite of a prior attempt.
        let cell = CellSeed { env: "dev".into(), region: "local".into(), cell: "local-0".into(), shard_count: 256 };
        let a1 = coords_from_uri("noetl://t/p/results/1/s/0/0/1").unwrap();
        let a2 = coords_from_uri("noetl://t/p/results/1/s/0/0/2").unwrap();
        let k1 = a1.physical_key(&cell.placement_for(&a1), "2026-06-22", "feather");
        let k2 = a2.physical_key(&cell.placement_for(&a2), "2026-06-22", "feather");
        assert_ne!(k1, k2);
        assert!(k1.ends_with("/0/0/1.feather"));
        assert!(k2.ends_with("/0/0/2.feather"));
    }

    #[test]
    fn physical_key_tier_extension_matches() {
        let cell = CellSeed { env: "dev".into(), region: "local".into(), cell: "local-0".into(), shard_count: 256 };
        let c = coords_from_uri("noetl://default/default/results/9/s/0/0/1").unwrap();
        let pl = cell.placement_for(&c);
        assert!(c.physical_key(&pl, "2026-06-22", "feather").ends_with(".feather"));
        assert!(c.physical_key(&pl, "2026-06-22", "json").ends_with(".json"));
        // Single-cell seed: env/region/cell come straight from config.
        let k = c.physical_key(&pl, "2026-06-22", "json");
        assert!(k.contains("env=dev"));
        assert!(k.contains("region=local"));
        assert!(k.contains("cell=local-0"));
    }

    #[test]
    fn event_date_from_timestamp_or_fallback() {
        assert_eq!(event_date(&over_budget_row("noetl://t/p/results/1/s/0/0/1")), "2026-06-22");
        assert_eq!(event_date(&serde_json::json!({})), "0000-00-00");
        assert_eq!(
            event_date(&serde_json::json!({ "created_at": "2026-01-02T00:00:00Z" })),
            "2026-01-02"
        );
    }

    #[test]
    fn parse_row_handles_object_and_string() {
        assert!(parse_row(&serde_json::json!({"a": 1})).is_some());
        assert!(parse_row(&serde_json::Value::String("{\"a\":1}".into())).is_some());
        assert!(parse_row(&serde_json::Value::String("not json".into())).is_none());
        assert!(parse_row(&serde_json::json!(42)).is_none());
    }

    #[test]
    fn enabled_default_off_and_from_env_none() {
        std::env::remove_var("NOETL_RESULT_MATERIALIZER_ENABLED");
        assert!(!enabled());
        let cfg = WorkerConfig {
            worker_id: "w".into(),
            pool_name: "p".into(),
            server_url: "http://x".into(),
            nats_url: "nats://h:4222".into(),
            nats_stream: "s".into(),
            nats_consumer: "c".into(),
            nats_subject: "noetl.commands".into(),
            nats_filter_subject: "noetl.commands".into(),
            heartbeat_interval: std::time::Duration::from_secs(5),
            max_concurrent_tasks: 1,
            metrics_bind: "0.0.0.0:9090".into(),
        };
        // Disabled → no config built (true no-op: nothing spawned).
        assert!(ResultMaterializerConfig::from_env(&cfg).is_none());
        std::env::set_var("NOETL_RESULT_MATERIALIZER_ENABLED", "true");
        assert!(enabled());
        assert!(ResultMaterializerConfig::from_env(&cfg).is_some());
        std::env::remove_var("NOETL_RESULT_MATERIALIZER_ENABLED");
    }
}
