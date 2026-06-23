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

// The result-tier encoding (`decide_tier` → Feather/JSON) is shared with the
// producer-staging path (#104 OQ5 Option A) so the two are byte-identical.
use crate::result_locator::{decide_tier, TierKind};

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
    /// Phase D minting flip (noetl/ai-meta#104 Phase D): when the tier is the
    /// **authoritative** result store (not just a shadow copy). True when
    /// `NOETL_RESULT_MINT_AUTHORITATIVE` is set; affects logging/observability
    /// only — the write path (fetch → tier → object_put) is identical, since the
    /// dual-write to `result_store` continues until the OQ5 retirement decision.
    pub authoritative: bool,
    /// Phase F DR re-derive (noetl/ai-meta#104 Phase F): when set
    /// (`NOETL_RESULT_TIER_DR`) the loop runs in **verify-and-repair** mode — for
    /// each over-budget result it derives the object and rewrites it only when the
    /// durable object is missing or byte-divergent (corrupt). A WAL event
    /// re-delivery is then a targeted DR repair. Default off → normal write path.
    pub dr_repair: bool,
}

impl ResultMaterializerConfig {
    /// Build from worker config + env. `None` when disabled.
    ///
    /// Spawns when **either** `NOETL_RESULT_MATERIALIZER_ENABLED` (Phase B
    /// shadow) **or** `NOETL_RESULT_MINT_AUTHORITATIVE` (Phase D: the tier is the
    /// authoritative store, so the materializer is the authoritative writer) is
    /// set. Default off → not spawned (true no-op).
    pub fn from_env(worker: &WorkerConfig) -> Option<Self> {
        let authoritative = crate::result_resolver::mint_authoritative();
        let dr_repair = crate::result_resolver::result_tier_dr();
        // Phase F: the DR re-derive runs on the same consume-loop, so the
        // materializer must spawn when DR is requested even if the Phase B/D
        // write flags are off (DR-only mode: verify-and-repair the existing tier).
        if !enabled() && !authoritative && !dr_repair {
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
            authoritative,
            dr_repair,
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

    // Phase D (#104): when the tier is authoritative the materializer is the
    // authoritative writer; otherwise it is the Phase B shadow copy. The write
    // path is identical either way (dual-write to `result_store` continues).
    let mode = if config.dr_repair {
        "DR verify-and-repair; #104 Phase F"
    } else if config.authoritative {
        "AUTHORITATIVE Feather tier; #104 Phase D"
    } else {
        "SHADOW Feather tier; #104 Phase B"
    };
    tracing::info!(
        stream = %config.stream,
        consumer = %config.consumer,
        batch = config.batch,
        cell = %config.cell.cell,
        authoritative = config.authoritative,
        dr_repair = config.dr_repair,
        "result materializer started ({mode})"
    );

    // #104 OQ5 Option A: read the producer-staging flag once — when on, the
    // write path skips a `result_store` fetch for any result already staged to
    // the tier by its producer (skip-on-exists in `write_shadow`).
    let producer_stage = crate::result_producer_stage::enabled();

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
                    if config.dr_repair {
                        // Phase F: verify-and-repair instead of an unconditional
                        // write — rebuild only a missing/corrupt tier object.
                        rederive_one(&client, &config.cell, &legacy_ref, &coords, &mut tally)
                            .await;
                    } else {
                        write_shadow(&client, &config.cell, producer_stage, &legacy_ref, &coords, &mut tally).await;
                    }
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
    producer_stage: bool,
    legacy_ref: &str,
    coords: &ResultCoordinates,
    tally: &mut CycleTally,
) {
    tally.eligible += 1;
    let eid = coords.execution_id;

    // #104 OQ5 Option A — skip-on-exists. When producer-staging is enabled the
    // producing worker stages the tier object at emit time, so the materializer
    // does NOT need to read `result_store` to (re)write it. Probe the §7 key
    // (both tier extensions, exactly like the resolve-by-URN read path) and skip
    // the `result_store` fetch entirely when the object already exists. The key
    // is content-addressed and `decide_tier` is deterministic, so an existing
    // object is byte-identical to what we'd write — re-writing it is wasted I/O
    // and, more importantly, the avoided `resolve_ref` is the prerequisite that
    // lets `result_store` be retired. Gated on the same flag so default-off is a
    // true no-op (byte-identical to Phase B/D behaviour).
    if producer_stage {
        let date = crate::snowflake::date_partition(coords.execution_id);
        let placement = cell.placement_for(coords);
        let staged = matches!(
            client.object_get(&coords.physical_key(&placement, &date, "feather")).await,
            Ok(Some(_))
        ) || matches!(
            client.object_get(&coords.physical_key(&placement, &date, "json")).await,
            Ok(Some(_))
        );
        if staged {
            tally.skipped += 1;
            tally.eligible -= 1;
            crate::metrics::record_result_producer_stage("materializer_skip_exists");
            tracing::debug!(
                execution_id = eid,
                "result object already producer-staged; materializer skip (no result_store read)"
            );
            return;
        }
    }

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
    // The `date=` partition is derived from the execution_id snowflake, NOT the
    // event timestamp — so the resolve-by-URN read path (#104 Phase C) can
    // reconstruct the same §7 key from the logical URI's execution_id alone,
    // with no carried date. Both sides call `snowflake::date_partition`.
    let date = crate::snowflake::date_partition(coords.execution_id);
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
// Phase F — DR re-derive (verify-and-repair)
// ---------------------------------------------------------------------------

/// The outcome of one DR verify-and-repair pass over an over-budget result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrOutcome {
    /// The durable object existed and matched the re-derivation byte-for-byte.
    Present,
    /// The durable object was missing or byte-divergent (corrupt) and was
    /// reconstructed from its source.
    Rederived,
    /// The authoritative payload source was absent — nothing to re-derive from.
    SourceGone,
    /// A fetch/encode/write failure.
    Error,
}

impl DrOutcome {
    fn label(self) -> &'static str {
        match self {
            DrOutcome::Present => "present",
            DrOutcome::Rederived => "rederived",
            DrOutcome::SourceGone => "source_gone",
            DrOutcome::Error => "error",
        }
    }
}

/// Pure verdict: given the durably-stored object bytes (`None` = missing) and the
/// freshly re-derived bytes, is the durable object **healthy** (present and
/// byte-identical)? Anything else (missing, or present-but-divergent/corrupt)
/// needs a repair. Isolated so the "byte-identical means no rewrite, divergent
/// means rewrite" decision is unit-testable without any I/O.
fn dr_is_healthy(durable: Option<&[u8]>, fresh: &[u8]) -> bool {
    matches!(durable, Some(b) if b == fresh)
}

/// DR core (noetl/ai-meta#104 Phase F): re-derive a single over-budget result's
/// tier object from its authoritative source and repair it if the durable object
/// is missing or corrupt.
///
/// The tier is **derivable** — the object bytes are the deterministic encode of
/// the payload (`decide_tier`, byte-stable for a fixed input) and the key is
/// computed from the logical URI — so the repair writes back exactly the bytes a
/// fresh materialization would. It never alters the authoritative `result_store`
/// source; it only ever (re)writes the derived object.
async fn rederive_one(
    client: &ControlPlaneClient,
    cell: &CellSeed,
    legacy_ref: &str,
    coords: &ResultCoordinates,
    tally: &mut CycleTally,
) {
    tally.eligible += 1;
    let eid = coords.execution_id;

    // 1. The byte source: the authoritative payload (read-only).
    let payload = match client.resolve_ref(legacy_ref).await {
        Ok(Some(p)) => p,
        Ok(None) => {
            // No source → cannot re-derive. Distinct from a benign shadow skip:
            // under DR this means the object is unrecoverable from its source.
            tracing::debug!(execution_id = eid, result_ref = legacy_ref, "DR: result source not found; cannot re-derive");
            tally.eligible -= 1;
            crate::metrics::record_result_tier_dr(DrOutcome::SourceGone.label());
            return;
        }
        Err(e) => {
            tally.errors += 1;
            tracing::warn!(execution_id = eid, result_ref = legacy_ref, error = %e, "DR: result source fetch failed");
            crate::metrics::record_result_tier_dr(DrOutcome::Error.label());
            return;
        }
    };

    // 2. Deterministically re-derive the object bytes + the §7 key.
    let tier = decide_tier(&payload);
    let date = crate::snowflake::date_partition(coords.execution_id);
    let key = coords.physical_key(&cell.placement_for(coords), &date, tier.ext());

    // 3. Verify the durable object against the re-derivation.
    let durable = match client.object_get(&key).await {
        Ok(opt) => opt,
        Err(e) => {
            tally.errors += 1;
            tracing::warn!(execution_id = eid, object_key = %key, error = %e, "DR: durable object check failed");
            crate::metrics::record_result_tier_dr(DrOutcome::Error.label());
            return;
        }
    };

    let outcome = if dr_is_healthy(durable.as_ref().map(|(b, _)| b.as_slice()), &tier.bytes) {
        tracing::debug!(execution_id = eid, object_key = %key, "DR: durable object healthy; no repair");
        DrOutcome::Present
    } else {
        // 4. Repair: rewrite the deterministic re-derivation.
        match client.object_put(&key, tier.bytes, tier.media).await {
            Ok(()) => {
                match tier.kind {
                    TierKind::Feather => tally.feather += 1,
                    TierKind::Json => tally.json += 1,
                }
                tracing::info!(execution_id = eid, object_key = %key, tier = tier.kind.label(), "DR: re-derived missing/corrupt result object (#104 Phase F)");
                DrOutcome::Rederived
            }
            Err(e) => {
                tally.errors += 1;
                tracing::warn!(execution_id = eid, object_key = %key, error = %e, "DR: re-derive object_put failed");
                DrOutcome::Error
            }
        }
    };
    crate::metrics::record_result_tier_dr(outcome.label());
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
        .and_then(crate::result_locator::coords_from_uri);
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

    use crate::result_locator::coords_from_uri;

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
    fn date_partition_is_execution_id_derived_and_deterministic() {
        // The §7 `date=` partition derives from the execution_id snowflake (not
        // the event timestamp), so write + read reconstruct the SAME key.
        let eid = 325i64;
        let d1 = crate::snowflake::date_partition(eid);
        let d2 = crate::snowflake::date_partition(eid);
        assert_eq!(d1, d2, "pure function of the id");
        assert_eq!(d1.len(), 10, "YYYY-MM-DD");
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
        std::env::remove_var("NOETL_RESULT_MINT_AUTHORITATIVE");
        std::env::remove_var("NOETL_RESULT_TIER_DR");
        assert!(ResultMaterializerConfig::from_env(&cfg).is_none());
        std::env::set_var("NOETL_RESULT_MATERIALIZER_ENABLED", "true");
        assert!(enabled());
        let built = ResultMaterializerConfig::from_env(&cfg).expect("enabled → built");
        // Phase B shadow: enabled but not authoritative, not DR.
        assert!(!built.authoritative);
        assert!(!built.dr_repair);
        std::env::remove_var("NOETL_RESULT_MATERIALIZER_ENABLED");

        // Phase F: NOETL_RESULT_TIER_DR alone (no write flags) spawns the
        // materializer in verify-and-repair mode.
        std::env::set_var("NOETL_RESULT_TIER_DR", "true");
        let dr = ResultMaterializerConfig::from_env(&cfg).expect("DR → materializer built");
        assert!(dr.dr_repair, "DR flag must set verify-and-repair mode");
        assert!(!dr.authoritative);
        std::env::remove_var("NOETL_RESULT_TIER_DR");
        // All flags off again → not spawned (true no-op).
        assert!(ResultMaterializerConfig::from_env(&cfg).is_none());
    }

    // --- Phase F: DR re-derive ------------------------------------------------

    #[test]
    fn dr_healthy_only_when_present_and_byte_identical() {
        let fresh = b"\x01\x02\x03feather-bytes".as_slice();
        // Present + identical → healthy (no repair).
        assert!(dr_is_healthy(Some(fresh), fresh));
        // Missing → not healthy (repair).
        assert!(!dr_is_healthy(None, fresh));
        // Present but divergent (corrupt) → not healthy (repair).
        assert!(!dr_is_healthy(Some(b"corrupt".as_slice()), fresh));
        // Present but truncated → not healthy (repair).
        assert!(!dr_is_healthy(Some(b"\x01\x02\x03".as_slice()), fresh));
    }

    #[test]
    fn mint_authoritative_alone_spawns_authoritative_writer() {
        // Phase D (#104): NOETL_RESULT_MINT_AUTHORITATIVE alone (without
        // NOETL_RESULT_MATERIALIZER_ENABLED) spawns the materializer AS the
        // authoritative tier writer.
        std::env::remove_var("NOETL_RESULT_MATERIALIZER_ENABLED");
        std::env::set_var("NOETL_RESULT_MINT_AUTHORITATIVE", "true");
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
        let built = ResultMaterializerConfig::from_env(&cfg)
            .expect("mint-authoritative → materializer built");
        assert!(built.authoritative, "Phase D writer must be authoritative");
        std::env::remove_var("NOETL_RESULT_MINT_AUTHORITATIVE");
    }

    // --- #104 OQ5 Option A: materializer skip-on-exists -----------------------

    use axum::{body::Bytes, routing::get, Json, Router};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::net::TcpListener;

    fn test_cell() -> CellSeed {
        CellSeed { env: "dev".into(), region: "local".into(), cell: "local-0".into(), shard_count: 256 }
    }

    /// When producer-staging is on and the tier object already exists (the
    /// producer staged it at emit time), `write_shadow` SKIPS the `result_store`
    /// fetch entirely — the OQ5 "materializer needs no result_store read" proof.
    #[tokio::test]
    async fn write_shadow_skips_result_store_when_object_already_staged() {
        let resolve_hit = Arc::new(AtomicBool::new(false));
        let rh = Arc::clone(&resolve_hit);

        // object_get → 200 (object EXISTS); resolve → tripwire (must NOT be hit).
        let app = Router::new()
            .route(
                "/api/internal/objects/{*key}",
                get(|| async {
                    (
                        axum::http::StatusCode::OK,
                        [(axum::http::header::CONTENT_TYPE, crate::result_locator::FEATHER_MEDIA)],
                        vec![1u8, 2, 3],
                    )
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
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client = ControlPlaneClient::new(&base);
        let coords = coords_from_uri("noetl://t/p/results/325/load/2/4/1").unwrap();
        let mut tally = CycleTally::default();
        write_shadow(&client, &test_cell(), true, "noetl://execution/325/result/s/9001", &coords, &mut tally).await;

        assert_eq!(tally.skipped, 1, "already-staged object → skipped");
        assert_eq!(tally.eligible, 0, "skip decrements the eligible count");
        assert_eq!(tally.feather, 0, "no rewrite");
        assert_eq!(tally.json, 0);
        assert!(!resolve_hit.load(Ordering::SeqCst), "must NOT read result_store for a producer-staged object");
    }

    /// Producer-staging on but the object is ABSENT (producer didn't stage, or it
    /// was GC'd): `write_shadow` falls through to the normal `result_store` fetch
    /// + write — the materializer remains the safety net.
    #[tokio::test]
    async fn write_shadow_falls_through_when_object_absent() {
        let resolve_hit = Arc::new(AtomicBool::new(false));
        let put_hit = Arc::new(AtomicBool::new(false));
        let rh = Arc::clone(&resolve_hit);
        let ph = Arc::clone(&put_hit);

        let app = Router::new()
            // object_get → 404 (NOT staged) for both feather + json probes.
            .route("/api/internal/objects/{*key}", get(|| async { axum::http::StatusCode::NOT_FOUND })
                .put(move |_: Bytes| {
                    let ph = Arc::clone(&ph);
                    async move { ph.store(true, Ordering::SeqCst); axum::http::StatusCode::OK }
                }))
            // resolve → the authoritative payload (tabular → Feather).
            .route(
                "/api/result/resolve",
                get(move || {
                    let rh = Arc::clone(&rh);
                    async move {
                        rh.store(true, Ordering::SeqCst);
                        Json(serde_json::json!({ "columns": ["id"], "rows": [[1], [2]] }))
                    }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client = ControlPlaneClient::new(&base);
        let coords = coords_from_uri("noetl://t/p/results/325/load/2/4/1").unwrap();
        let mut tally = CycleTally::default();
        write_shadow(&client, &test_cell(), true, "noetl://execution/325/result/s/9001", &coords, &mut tally).await;

        assert!(resolve_hit.load(Ordering::SeqCst), "absent object → materializer fetches result_store (safety net)");
        assert!(put_hit.load(Ordering::SeqCst), "absent object → materializer writes the tier");
        assert_eq!(tally.feather, 1, "wrote the Feather tier");
    }
}
