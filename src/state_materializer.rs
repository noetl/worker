//! State materializer — the **shadow** per-execution state-shard writer
//! ([noetl/ai-meta#166](https://github.com/noetl/ai-meta/issues/166) Phase 2).
//!
//! ## What it is
//!
//! A system-pool consume-loop on the `noetl_events` JetStream WAL, **sibling** to
//! the event materializer ([`crate::materializer`]) and the result materializer
//! ([`crate::result_materializer`]) but on its **own** durable consumer
//! (`noetl_state_materializer`). It projects each execution's events down to a
//! **slim chain** (`event_id`, `prev_event_id`, `event_type`, `execution_id`,
//! `node_name`, `status`, the #104 `result_ref` URN, a bounded `extracted` block)
//! and writes that chain as an Arrow **Feather** shard to object store at the
//! §7 [`StateCoordinates::physical_key`] — append-while-live, seal-on-terminal.
//!
//! ## Why a separate consumer (the #103 sole-writer is preserved)
//!
//! This writer is a **third read-model projector** off the same WAL, exactly like
//! the result materializer. It is **READ-ONLY w.r.t. `noetl.*`**: it never writes
//! the event log, the command queue, or any `noetl.*` table — it only ever reads
//! the WAL stream and `PUT`s a derived Feather object via
//! `PUT /api/internal/objects/{key}` (server-mediated, per the
//! data-access-boundary rule). The #103 materializer stays the sole writer of
//! `noetl.event`.
//!
//! Its own durable consumer + ack cursor isolates the object I/O entirely: a
//! slow/erroring object store stalls only this loop, never the event-materialize
//! fold or the off-server drive path.
//!
//! ## Shadow / non-authoritative
//!
//! This is **shadow** (RFC Phase 2): nothing READS these shards yet (that is
//! Phase 3 — cold-load on cache miss). The drive still serves from the Phase-1
//! bounded in-memory index. Two consequences are load-bearing:
//!
//! 1. **Never alters anything authoritative.** The loop only reads the WAL and
//!    writes a new object. It never mutates `noetl.*`, never touches the drive
//!    index, never blocks the drive.
//! 2. **Never fails an event.** A project/encode/write error is counted and
//!    WARN-logged (with `execution_id`), then the batch is **acked anyway** so the
//!    shadow tier can never wedge its own consumer. The §7 key is deterministic,
//!    so an at-least-once redelivery is a safe idempotent overwrite.
//!
//! Opt-in: spawned only when `NOETL_STATE_SHARD_WRITE` is truthy (set on the
//! system worker pool). Default off → not spawned → true no-op (byte-identical to
//! today). Instant rollback = unset the flag.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::{self, consumer::pull::Config as PullConfig, consumer::AckPolicy};
use async_nats::ConnectOptions;
use noetl_tools::locator::CellPlacement;
use noetl_tools::tools::source::{AckDisposition, AckMode, PollOptions};
use noetl_tools::tools::{build_source, SubscriptionConfig};
use noetl_tools::ExecutionContext;

use crate::client::ControlPlaneClient;
use crate::config::WorkerConfig;
use crate::materializer::{env_u32, env_u64, parse_nats_credentials, EVENT_STREAM};
use crate::result_locator::FEATHER_MEDIA;
use crate::state_locator::{ShardSeal, StateCoordinates};

/// Durable consumer the state materializer drains — distinct from the event
/// materializer's `noetl_materializer` and the result materializer's
/// `noetl_result_materializer`. Self-ensured at startup (see [`ensure_consumer`])
/// so Phase 2 is a worker-only change with no server release: an idle consumer
/// (flag default-off) just sits at the stream tail and costs nothing.
pub const STATE_MATERIALIZER_CONSUMER: &str = "noetl_state_materializer";

const DEFAULT_BATCH: u32 = 200;
const DEFAULT_TIMEOUT_MS: u64 = 2_000;
const DEFAULT_IDLE_SLEEP_MS: u64 = 500;
const DEFAULT_ERROR_BACKOFF_MS: u64 = 2_000;
/// Bound the inline `extracted` snippet kept per event so a pathological block
/// can't bloat the shard (the slim-chain memory lever is the whole point).
const DEFAULT_EXTRACTED_BUDGET: usize = 4_096;
/// Safety ceiling on resident open shards — the writer must never itself OOM the
/// way the unbounded Phase-1 index did. Terminal eviction + the idle sweep keep
/// this far under the cap in steady state; the cap is the abandoned-execution
/// backstop (the #163 stuck-execution class).
const DEFAULT_MAX_OPEN: usize = 8_192;
/// Idle TTL: an open shard not touched within this window is swept (its execution
/// is wedged / abandoned and will never seal). Mirrors the Phase-1 TTL idea.
const DEFAULT_OPEN_TTL_SECS: u64 = 3_600;
/// Minimum interval between successive **open**-shard rewrites for one execution
/// (RFC §4.3 "append on a cadence"). The open shard is rewritten idempotently to
/// the SAME object key; an object-store backend (e.g. GCS) rate-limits mutations
/// of a single object (~1/s), so rewriting it every drain cycle a live multi-hop
/// execution spans amplifies into HTTP 429s. Throttling to ≤1 open write / this
/// interval / execution keeps the rate well under that ceiling. The terminal
/// **sealed** shard (a distinct key) is never throttled. Default 30s → a typical
/// sub-30s turn writes the open shard ~once then seals; longer executions get a
/// periodic open snapshot.
const DEFAULT_OPEN_MIN_INTERVAL_SECS: u64 = 30;

/// Terminal event types — observing one seals the execution's shard and evicts it
/// from the open set. BOTH the dotted (`playbook.completed`) and underscore
/// (`playbook_completed`) spellings exist across the codebase and the WAL (see
/// `repos/server/src/handlers/event_write.rs::is_terminal`), so the writer matches
/// both rather than betting on one — a missed terminal would just leave a shard
/// `open` (eventually idle-swept), never wrong, but matching both seals promptly.
const TERMINAL_EVENT_TYPES: &[&str] = &[
    "playbook.completed",
    "playbook_completed",
    "playbook.failed",
    "playbook_failed",
    "playbook.cancelled",
    "playbook_cancelled",
];

/// True when `NOETL_STATE_SHARD_WRITE` is set to a truthy value.
pub fn enabled() -> bool {
    matches!(
        std::env::var("NOETL_STATE_SHARD_WRITE")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// The resolved cell placement seed. Reads the **same** `NOETL_RESULT_CELL_*`
/// env vars as the result materializer so a state shard co-locates with the same
/// execution's result bytes (same env/region/cell, same `s{fnv:04}` folder).
#[derive(Clone)]
pub struct CellSeed {
    pub env: String,
    pub region: String,
    pub cell: String,
    pub shard_count: u32,
}

impl CellSeed {
    /// Resolve the cell seed from the `NOETL_RESULT_CELL_*` env vars — the SAME
    /// vars the result materializer reads, so a state shard co-locates with the
    /// execution's result bytes.  Public so the Phase-3 cold-load reader
    /// ([`crate::state_reader`]) resolves the identical placement the writer used.
    pub fn from_env() -> Self {
        Self {
            env: std::env::var("NOETL_RESULT_CELL_ENV").unwrap_or_else(|_| "dev".to_string()),
            region: std::env::var("NOETL_RESULT_CELL_REGION").unwrap_or_else(|_| "local".to_string()),
            cell: std::env::var("NOETL_RESULT_CELL").unwrap_or_else(|_| "local-0".to_string()),
            shard_count: env_u32("NOETL_RESULT_SHARD_COUNT", 256).max(1),
        }
    }

    /// Resolve the placement for a state shard: derive the folder shard from the
    /// stable shard hash (== the execution's result-byte folder), home it on the
    /// one configured cell.
    pub fn placement_for(&self, coords: &StateCoordinates) -> CellPlacement {
        CellPlacement::new(&self.env, &self.region, &self.cell, coords.shard_key(self.shard_count))
    }
}

/// Resolved state-materializer configuration.
pub struct StateMaterializerConfig {
    pub nats_url: String,
    pub nats_user: Option<String>,
    pub nats_password: Option<String>,
    pub stream: String,
    pub consumer: String,
    pub batch: u32,
    pub timeout_ms: u64,
    pub idle_sleep: Duration,
    pub error_backoff: Duration,
    pub extracted_budget: usize,
    pub max_open: usize,
    pub open_ttl: Duration,
    /// Minimum interval between open-shard rewrites per execution (write-amplification
    /// throttle — see [`DEFAULT_OPEN_MIN_INTERVAL_SECS`]).
    pub open_min_interval: Duration,
    pub cell: CellSeed,
}

impl StateMaterializerConfig {
    /// Build from worker config + env. `None` when disabled (default → no-op).
    pub fn from_env(worker: &WorkerConfig) -> Option<Self> {
        if !enabled() {
            return None;
        }
        let (nats_url, nats_user, nats_password) = parse_nats_credentials(&worker.nats_url);
        Some(Self {
            nats_url,
            nats_user,
            nats_password,
            stream: std::env::var("NOETL_STATE_SHARD_STREAM").unwrap_or_else(|_| EVENT_STREAM.to_string()),
            consumer: std::env::var("NOETL_STATE_SHARD_CONSUMER")
                .unwrap_or_else(|_| STATE_MATERIALIZER_CONSUMER.to_string()),
            batch: env_u32("NOETL_STATE_SHARD_BATCH", DEFAULT_BATCH).clamp(1, 1000),
            timeout_ms: env_u64("NOETL_STATE_SHARD_TIMEOUT_MS", DEFAULT_TIMEOUT_MS),
            idle_sleep: Duration::from_millis(env_u64("NOETL_STATE_SHARD_IDLE_SLEEP_MS", DEFAULT_IDLE_SLEEP_MS)),
            error_backoff: Duration::from_millis(env_u64("NOETL_STATE_SHARD_ERROR_BACKOFF_MS", DEFAULT_ERROR_BACKOFF_MS)),
            extracted_budget: env_u64("NOETL_STATE_SHARD_EXTRACTED_BUDGET", DEFAULT_EXTRACTED_BUDGET as u64) as usize,
            max_open: env_u64("NOETL_STATE_SHARD_MAX_OPEN", DEFAULT_MAX_OPEN as u64) as usize,
            open_ttl: Duration::from_secs(env_u64("NOETL_STATE_SHARD_OPEN_TTL_SECS", DEFAULT_OPEN_TTL_SECS)),
            open_min_interval: Duration::from_secs(env_u64(
                "NOETL_STATE_SHARD_OPEN_MIN_INTERVAL_SECS",
                DEFAULT_OPEN_MIN_INTERVAL_SECS,
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
            .map_err(|e| anyhow!("state materializer source config invalid: {e}"))
    }
}

/// Idempotently ensure the durable `noetl_state_materializer` pull consumer
/// exists on the event stream. `create_consumer` with a stable durable name +
/// config is a no-op when it already exists, so this is safe to call on every
/// boot. Self-ensuring (rather than relying on the server) keeps Phase 2 a
/// worker-only change. Default `DeliverPolicy::All` + `AckPolicy::Explicit` =
/// resume from the durable cursor across restarts (no repeated 24h replay — the
/// #119 ephemeral-replay problem is avoided by being durable).
async fn ensure_consumer(config: &StateMaterializerConfig) -> Result<()> {
    let client = match (&config.nats_user, &config.nats_password) {
        (Some(u), Some(p)) => ConnectOptions::with_user_and_password(u.clone(), p.clone())
            .connect(&config.nats_url)
            .await
            .context("state materializer NATS connect (user/pass)")?,
        _ => async_nats::connect(&config.nats_url)
            .await
            .context("state materializer NATS connect")?,
    };
    let js = jetstream::new(client);
    let stream = js
        .get_stream(&config.stream)
        .await
        .with_context(|| format!("state materializer get_stream {}", config.stream))?;
    stream
        .create_consumer(PullConfig {
            durable_name: Some(config.consumer.clone()),
            filter_subject: "noetl.events.>".to_string(),
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        })
        .await
        .context("state materializer create_consumer")?;
    tracing::debug!(stream = %config.stream, consumer = %config.consumer, "ensured state-shard pull consumer");
    Ok(())
}

/// Spawn the state-materializer loop, returning its join handle.
pub fn spawn(config: StateMaterializerConfig, client: ControlPlaneClient) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = run_loop(config, client).await {
            tracing::error!(error = %e, "state materializer loop exited with error");
        }
    })
}

/// The output of projecting one `noetl_events` envelope: the routing
/// `execution_id`, the slim row, the tenant/project recovered from the #104
/// result-ref URN (for shard co-location), and whether the event seals the shard.
struct Projected {
    execution_id: i64,
    row: SlimRow,
    tenant: Option<String>,
    project: Option<String>,
    is_terminal: bool,
}

/// One slim-chain row (one event projected to the shard columns).
#[derive(Clone, Debug, PartialEq)]
struct SlimRow {
    event_id: i64,
    prev_event_id: Option<i64>,
    event_type: String,
    node_name: Option<String>,
    status: Option<String>,
    result_ref: Option<String>,
    extracted: Option<String>,
    /// The **verbatim** slim event payload — [`crate::state_builder::slim_event_payload`]
    /// of the full `noetl_events` envelope, serialized compact
    /// (noetl/ai-meta#166 Phase 3).  This is the byte-for-byte input the Phase-3
    /// cold-load reader ([`crate::state_reader`]) feeds back into
    /// `WalEventIndex::apply` so the reconstructed chain is **byte-equivalent** to
    /// the WAL-replay/in-memory path — the drive makes routing decisions
    /// (`next.arcs`/`when`) from `context`/`result`/`meta`, which are load-bearing
    /// and therefore carried here in full rather than dropped.  The slim
    /// projection keeps only the fields the orchestrate-core `Event` deserializer
    /// reads (serde drops the rest on decode regardless), so this is the smallest
    /// payload that still reconstructs the identical `Event`.
    payload: String,
}

/// One execution's in-progress (open) state shard accumulator.
struct OpenShard {
    tenant: Option<String>,
    project: Option<String>,
    /// Slim rows keyed by `event_id` — dedups redelivery and keeps the shard in
    /// stable `event_id`-ascending order (each row carries `prev_event_id`, so a
    /// reader can reconstruct causal order; §4.2 causal re-sort is a Phase-3
    /// read-side concern).
    rows: BTreeMap<i64, SlimRow>,
    last_activity: Instant,
    /// When the open shard was last rewritten to object store — the cadence
    /// throttle anchor (RFC §4.3). `None` until the first open write.
    last_open_write: Option<Instant>,
}

impl OpenShard {
    fn coords(&self, execution_id: i64) -> StateCoordinates {
        StateCoordinates::new(self.tenant.as_deref(), self.project.as_deref(), execution_id)
    }
}

/// Per-cycle tally for the metrics recorder.
#[derive(Default)]
struct CycleTally {
    drained: u64,
    rows: u64,
    shards_written: u64,
    sealed: u64,
    shard_bytes: u64,
    skipped: u64,
    errors: u64,
}

/// The drain → project → (encode → object_put) → ack cycle, forever.
async fn run_loop(config: StateMaterializerConfig, client: ControlPlaneClient) -> Result<()> {
    // Self-ensure the durable consumer before the source's `get_consumer` runs.
    ensure_consumer(&config).await?;

    let source = build_source(&config.source_config()?, &ExecutionContext::default())
        .map_err(|e| anyhow!("state materializer build_source failed: {e}"))?;

    tracing::info!(
        stream = %config.stream,
        consumer = %config.consumer,
        batch = config.batch,
        cell = %config.cell.cell,
        max_open = config.max_open,
        "state materializer started (SHADOW Feather state-shard tier; #166 Phase 2)"
    );

    // Deferred ack: we ack the batch ourselves after best-effort shadow writes.
    let opts = PollOptions::new(Some(config.batch), Some(config.timeout_ms), AckMode::Defer);

    // Per-execution open shards, accumulated across cycles. Bounded by terminal
    // eviction + idle sweep + the max-open cap.
    let mut open: HashMap<i64, OpenShard> = HashMap::new();

    loop {
        let cycle_start = Instant::now();
        let outcome = match source.poll(&opts).await {
            Ok(o) => o,
            Err(e) => {
                tracing::warn!(error = %e, "state materializer drain failed; backing off");
                tokio::time::sleep(config.error_backoff).await;
                continue;
            }
        };

        let drained = outcome.messages.len();
        if drained == 0 {
            // Idle: sweep abandoned open shards so a wedged execution can't pin a
            // shard forever, then sleep.
            sweep_idle(&mut open, config.open_ttl, Instant::now());
            tokio::time::sleep(config.idle_sleep).await;
            continue;
        }

        let mut tally = CycleTally { drained: drained as u64, ..Default::default() };
        // Executions that received new events this cycle (write their shards once
        // at cycle end) and those that sealed (write sealed, then evict).
        let mut touched: Vec<i64> = Vec::new();
        let mut sealed: Vec<i64> = Vec::new();
        let now = Instant::now();

        for msg in &outcome.messages {
            let Some(row) = parse_row(&msg.data) else {
                tally.skipped += 1;
                continue;
            };
            let Some(p) = project_event(&row, config.extracted_budget) else {
                tally.skipped += 1;
                continue;
            };
            let Projected { execution_id, row: slim, tenant, project, is_terminal } = p;
            tally.rows += 1;
            let shard = open.entry(execution_id).or_insert_with(|| OpenShard {
                tenant: None,
                project: None,
                rows: BTreeMap::new(),
                last_activity: now,
                last_open_write: None,
            });
            // Pin tenant/project from the first result_ref URN seen (co-location
            // with that execution's result bytes); default until then.
            if shard.tenant.is_none() {
                if let Some(t) = tenant {
                    shard.tenant = Some(t);
                    shard.project = project;
                }
            }
            shard.rows.insert(slim.event_id, slim);
            shard.last_activity = now;
            if is_terminal {
                if !sealed.contains(&execution_id) {
                    sealed.push(execution_id);
                }
            } else if !touched.contains(&execution_id) {
                touched.push(execution_id);
            }
        }

        // Write open shards for live executions touched this cycle (skip those
        // that also sealed — they get the sealed write below). Throttled per
        // execution to ≤1 open rewrite / open_min_interval (RFC §4.3 cadence):
        // the open shard is rewritten to the SAME object key, which an object
        // store rate-limits (~1 mutation/s/object), so an un-throttled per-cycle
        // rewrite of a live multi-hop execution amplifies into HTTP 429s.
        for eid in &touched {
            if sealed.contains(eid) {
                continue;
            }
            let due = open
                .get(eid)
                .map(|s| s.last_open_write.is_none_or(|t| now.duration_since(t) >= config.open_min_interval))
                .unwrap_or(false);
            if !due {
                continue;
            }
            if let Some(shard) = open.get(eid) {
                write_shard(&client, &config.cell, *eid, shard, ShardSeal::Open, &mut tally).await;
                if let Some(s) = open.get_mut(eid) {
                    s.last_open_write = Some(now);
                }
            }
        }
        // Seal + evict terminal executions.
        for eid in &sealed {
            if let Some(shard) = open.get(eid) {
                write_shard(&client, &config.cell, *eid, shard, ShardSeal::Sealed, &mut tally).await;
                tally.sealed += 1;
            }
            open.remove(eid);
        }

        // Enforce the resident-open ceiling (abandoned-execution backstop): evict
        // the least-recently-active open shards until under the cap.
        enforce_max_open(&mut open, config.max_open);

        // SHADOW: always ack — the state tier must never wedge its own consumer
        // nor perturb the event-materialize path. Failed writes are counted;
        // deterministic keys make any redelivery a safe overwrite.
        let report = source
            .ack(&outcome.ack_ids, AckDisposition::Ack)
            .await
            .unwrap_or_default();

        crate::metrics::set_state_materializer_open_shards(open.len() as i64);
        crate::metrics::record_state_materializer_cycle(
            tally.drained,
            tally.rows,
            tally.shards_written,
            tally.sealed,
            tally.shard_bytes,
            tally.skipped,
            tally.errors,
            cycle_start.elapsed().as_secs_f64(),
        );
        tracing::debug!(
            drained,
            rows = tally.rows,
            shards = tally.shards_written,
            sealed = tally.sealed,
            shard_bytes = tally.shard_bytes,
            skipped = tally.skipped,
            errors = tally.errors,
            open = open.len(),
            acked = report.disposed,
            "state materializer cycle"
        );
    }
}

/// Encode + PUT one execution's slim-chain shard. Best-effort: any error
/// increments `errors` and WARN-logs with `execution_id`; never propagates (the
/// shadow tier never fails an event).
async fn write_shard(
    client: &ControlPlaneClient,
    cell: &CellSeed,
    execution_id: i64,
    shard: &OpenShard,
    seal: ShardSeal,
    tally: &mut CycleTally,
) {
    let coords = shard.coords(execution_id);
    let tabular = encode_slim_chain(shard.rows.values());
    let Some(bytes) = noetl_tools::arrow_codec::try_encode_tabular_json(&tabular).map(|enc| enc.bytes) else {
        // Empty / unencodable (never expected — a shard always has ≥1 row).
        tally.skipped += 1;
        return;
    };
    let date = crate::snowflake::date_partition(execution_id);
    let key = coords.physical_key(&cell.placement_for(&coords), &date, seal, "feather");
    let n = bytes.len() as u64;
    match client.object_put(&key, bytes, FEATHER_MEDIA).await {
        Ok(()) => {
            tally.shards_written += 1;
            tally.shard_bytes += n;
            tracing::debug!(
                execution_id,
                object_key = %key,
                rows = shard.rows.len(),
                bytes = n,
                seal = seal.segment(),
                "state shard written (shadow)"
            );
        }
        Err(e) => {
            tally.errors += 1;
            tracing::warn!(execution_id, object_key = %key, error = %e, "state materializer object_put failed (shadow)");
        }
    }
}

/// Build the `{columns, rows}` tabular JSON the shared `arrow_codec` encodes to a
/// Feather shard. Rows are array-shaped (positional, matching `columns`).
fn encode_slim_chain<'a>(rows: impl Iterator<Item = &'a SlimRow>) -> serde_json::Value {
    let mut out_rows: Vec<serde_json::Value> = Vec::new();
    for r in rows {
        out_rows.push(serde_json::json!([
            r.event_id,
            r.prev_event_id,
            r.event_type,
            r.node_name,
            r.status,
            r.result_ref,
            r.extracted,
            r.payload,
        ]));
    }
    serde_json::json!({
        "columns": ["event_id", "prev_event_id", "event_type", "node_name", "status", "result_ref", "extracted", "payload"],
        "rows": out_rows,
    })
}

/// Sweep open shards idle longer than `ttl` (abandoned / wedged executions that
/// will never seal). Returns the number swept.
fn sweep_idle(open: &mut HashMap<i64, OpenShard>, ttl: Duration, now: Instant) -> usize {
    if ttl.is_zero() {
        return 0;
    }
    let stale: Vec<i64> = open
        .iter()
        .filter(|(_, s)| now.duration_since(s.last_activity) >= ttl)
        .map(|(k, _)| *k)
        .collect();
    for eid in &stale {
        open.remove(eid);
    }
    if !stale.is_empty() {
        crate::metrics::record_state_materializer_evicted("idle", stale.len());
    }
    stale.len()
}

/// Enforce the resident-open ceiling: evict the least-recently-active open shards
/// until at or under `max_open`. The unbounded-memory backstop.
fn enforce_max_open(open: &mut HashMap<i64, OpenShard>, max_open: usize) {
    if max_open == 0 || open.len() <= max_open {
        return;
    }
    let mut by_age: Vec<(i64, Instant)> = open.iter().map(|(k, s)| (*k, s.last_activity)).collect();
    by_age.sort_by_key(|(_, t)| *t); // oldest first
    let evict_n = open.len() - max_open;
    for (eid, _) in by_age.into_iter().take(evict_n) {
        open.remove(&eid);
    }
    crate::metrics::record_state_materializer_evicted("max_open", evict_n);
}

// ---------------------------------------------------------------------------
// Pure projection layer (unit-tested without any I/O)
// ---------------------------------------------------------------------------

/// Parse a drained message payload into a JSON object (value or JSON string).
fn parse_row(data: &serde_json::Value) -> Option<serde_json::Value> {
    match data {
        serde_json::Value::Object(_) => Some(data.clone()),
        serde_json::Value::String(s) => serde_json::from_str::<serde_json::Value>(s).ok(),
        _ => None,
    }
}

/// Project a `noetl_events` envelope to its slim-chain row + routing metadata.
/// Returns `(execution_id, row, tenant, project, is_terminal)`, or `None` when
/// the payload isn't a chainable event (no `event_id` / `execution_id`). Pure —
/// never mutates `row`. `tenant`/`project` are recovered from the #104 result-ref
/// URN when present (so the shard co-locates with the execution's result bytes).
fn project_event(row: &serde_json::Value, extracted_budget: usize) -> Option<Projected> {
    let obj = row.as_object()?;
    let event_id = obj.get("event_id").and_then(|v| v.as_i64())?;
    let execution_id = obj.get("execution_id").and_then(|v| v.as_i64())?;
    let prev_event_id = obj.get("prev_event_id").and_then(|v| v.as_i64());
    let event_type = obj.get("event_type").and_then(|v| v.as_str()).unwrap_or_default().to_string();
    let node_name = obj.get("node_name").and_then(|v| v.as_str()).map(|s| s.to_string());
    let status = obj.get("status").and_then(|v| v.as_str()).map(|s| s.to_string());
    let is_terminal = TERMINAL_EVENT_TYPES.contains(&event_type.as_str());

    // The #104 result reference (`kind: "result_ref"`) carries the canonical URN
    // (`uri`) + an `extracted` predicate block. Recover both, plus tenant/project
    // from the URN for shard co-location.
    let reference = find_result_ref(row);
    let result_ref = reference
        .and_then(|r| r.get("uri"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let extracted = reference
        .and_then(|r| r.get("extracted"))
        .map(|v| bound_json(v, extracted_budget));
    let (tenant, project) = result_ref
        .as_deref()
        .and_then(crate::result_locator::coords_from_uri)
        .map(|c| (Some(c.tenant), Some(c.project)))
        .unwrap_or((None, None));

    // The verbatim slim payload (noetl/ai-meta#166 Phase 3): the exact bytes the
    // in-memory WAL index reconstructs its chain node + stored raw from, so the
    // cold-load reader rebuilds a byte-equivalent chain.  `shard_chain_payload`
    // is the slim projection PLUS `prev_event_id` (the chain link the reader needs
    // in-band; the index strips it back out on apply).  Compact JSON (no
    // whitespace) so the stored bytes are stable across writer runs.
    let payload = serde_json::to_string(&crate::state_builder::shard_chain_payload(row))
        .unwrap_or_default();

    Some(Projected {
        execution_id,
        row: SlimRow {
            event_id,
            prev_event_id,
            event_type,
            node_name,
            status,
            result_ref,
            extracted,
            payload,
        },
        tenant,
        project,
        is_terminal,
    })
}

/// Serialize a JSON value to a compact string bounded to `budget` bytes (a
/// truncated snippet is fine — the shard is shadow / advisory; the authoritative
/// body is always re-fetchable by the `result_ref` URN).
fn bound_json(v: &serde_json::Value, budget: usize) -> String {
    let s = serde_json::to_string(v).unwrap_or_default();
    if budget == 0 || s.len() <= budget {
        s
    } else {
        // Truncate on a char boundary.
        let mut end = budget;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        s[..end].to_string()
    }
}

/// Recursively find the over-budget result-reference object (`kind: "result_ref"`).
/// Returns the first match in a depth-first walk. (Same shape the result
/// materializer keys on.)
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

    /// A worker `command.completed` event carrying an over-budget result ref.
    fn event_with_ref(event_id: i64, prev: Option<i64>, etype: &str, uri: &str) -> serde_json::Value {
        serde_json::json!({
            "event_id": event_id,
            "execution_id": 325,
            "prev_event_id": prev,
            "event_type": etype,
            "node_name": "load_offers",
            "status": "completed",
            "result": {
                "status": "completed",
                "reference": { "kind": "result_ref", "uri": uri, "extracted": { "rows": 4 } }
            }
        })
    }

    #[test]
    fn project_extracts_slim_row_and_tenant_from_uri() {
        let ev = event_with_ref(2, Some(1), "command.completed", "noetl://muno/travel/results/325/load/0/0/1");
        let Projected { execution_id: eid, row, tenant, project, is_terminal: terminal } =
            project_event(&ev, 4096).unwrap();
        assert_eq!(eid, 325);
        assert_eq!(row.event_id, 2);
        assert_eq!(row.prev_event_id, Some(1));
        assert_eq!(row.event_type, "command.completed");
        assert_eq!(row.node_name.as_deref(), Some("load_offers"));
        assert_eq!(row.status.as_deref(), Some("completed"));
        assert_eq!(row.result_ref.as_deref(), Some("noetl://muno/travel/results/325/load/0/0/1"));
        assert_eq!(row.extracted.as_deref(), Some("{\"rows\":4}"));
        // tenant/project recovered from the URN → co-locates with result bytes.
        assert_eq!(tenant.as_deref(), Some("muno"));
        assert_eq!(project.as_deref(), Some("travel"));
        assert!(!terminal);
    }

    #[test]
    fn project_genesis_has_null_prev_and_no_ref() {
        let ev = serde_json::json!({
            "event_id": 1, "execution_id": 325, "prev_event_id": null,
            "event_type": "playbook_started", "node_name": "start", "status": "running"
        });
        let Projected { execution_id: eid, row, tenant, project, is_terminal: terminal } =
            project_event(&ev, 4096).unwrap();
        assert_eq!(eid, 325);
        assert_eq!(row.prev_event_id, None);
        assert_eq!(row.result_ref, None);
        assert_eq!(row.extracted, None);
        assert_eq!(tenant, None);
        assert_eq!(project, None);
        assert!(!terminal);
    }

    #[test]
    fn project_terminal_flagged() {
        // Both spellings seal: dotted (the noetl.event DB shape) + underscore.
        let dotted = serde_json::json!({
            "event_id": 9, "execution_id": 325, "prev_event_id": 8,
            "event_type": "playbook.completed", "status": "completed"
        });
        assert!(project_event(&dotted, 4096).unwrap().is_terminal);
        let underscore = serde_json::json!({
            "event_id": 9, "execution_id": 325, "prev_event_id": 8,
            "event_type": "playbook_failed", "status": "failed"
        });
        assert!(project_event(&underscore, 4096).unwrap().is_terminal);
        // A non-terminal hop is not flagged.
        let hop = serde_json::json!({
            "event_id": 5, "execution_id": 325, "prev_event_id": 4,
            "event_type": "command.completed", "status": "success"
        });
        assert!(!project_event(&hop, 4096).unwrap().is_terminal);
    }

    #[test]
    fn project_non_chainable_is_none() {
        // No event_id → not chainable.
        assert!(project_event(&serde_json::json!({"execution_id": 1}), 4096).is_none());
        // No execution_id → not chainable.
        assert!(project_event(&serde_json::json!({"event_id": 1}), 4096).is_none());
    }

    #[test]
    fn slim_chain_encodes_to_feather_with_ref_column() {
        // The slim chain encodes to a non-empty Feather batch whose rows carry the
        // result_ref URN — the shadow-shard correctness proof.
        let rows = vec![
            SlimRow { event_id: 1, prev_event_id: None, event_type: "playbook_started".into(), node_name: Some("start".into()), status: Some("running".into()), result_ref: None, extracted: None, payload: "{\"event_id\":1,\"event_type\":\"playbook_started\"}".into() },
            SlimRow { event_id: 2, prev_event_id: Some(1), event_type: "command.completed".into(), node_name: Some("load".into()), status: Some("completed".into()), result_ref: Some("noetl://muno/travel/results/325/load/0/0/1".into()), extracted: Some("{\"rows\":4}".into()), payload: "{\"event_id\":2,\"event_type\":\"command.completed\"}".into() },
        ];
        let tabular = encode_slim_chain(rows.iter());
        let enc = noetl_tools::arrow_codec::try_encode_tabular_json(&tabular).expect("slim chain → Feather");
        assert!(!enc.bytes.is_empty());
        assert_eq!(enc.row_count, 2);
        // Round-trips back through the decoder with the result_ref + payload columns.
        let batches = noetl_tools::arrow_codec::decode_record_batches(&enc.bytes).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        // 8 columns now: the 7 slim columns + the Phase-3 verbatim `payload` column.
        assert_eq!(batches[0].num_columns(), 8);
    }

    #[test]
    fn bound_json_truncates_on_budget() {
        let big = serde_json::json!({ "x": "y".repeat(100) });
        let s = bound_json(&big, 16);
        assert!(s.len() <= 16);
        // Zero budget = unbounded.
        let full = bound_json(&big, 0);
        assert!(full.len() > 16);
    }

    #[test]
    fn enforce_max_open_evicts_oldest() {
        let mut open: HashMap<i64, OpenShard> = HashMap::new();
        let base = Instant::now();
        for i in 0..10i64 {
            open.insert(i, OpenShard {
                tenant: None, project: None, rows: BTreeMap::new(),
                last_activity: base + Duration::from_millis(i as u64),
                last_open_write: None,
            });
        }
        enforce_max_open(&mut open, 4);
        assert_eq!(open.len(), 4);
        // The 4 most-recently-active (ids 6..=9) survive.
        for i in 6..10i64 {
            assert!(open.contains_key(&i), "expected recent shard {i} kept");
        }
    }

    #[test]
    fn sweep_idle_drops_stale_only() {
        let mut open: HashMap<i64, OpenShard> = HashMap::new();
        let now = Instant::now();
        // Stale: last activity 2h ago.
        open.insert(1, OpenShard { tenant: None, project: None, rows: BTreeMap::new(), last_activity: now - Duration::from_secs(7200), last_open_write: None });
        // Fresh: just now.
        open.insert(2, OpenShard { tenant: None, project: None, rows: BTreeMap::new(), last_activity: now, last_open_write: None });
        let swept = sweep_idle(&mut open, Duration::from_secs(3600), now);
        assert_eq!(swept, 1);
        assert!(!open.contains_key(&1));
        assert!(open.contains_key(&2));
    }

    #[test]
    fn open_write_cadence_gate() {
        // The write-amplification throttle (RFC §4.3): an open shard is due for a
        // rewrite only on first sight (last_open_write None) or after the interval
        // elapsed — caps same-object mutations under the object-store rate limit.
        let now = Instant::now();
        let interval = Duration::from_secs(30);
        let due = |last: Option<Instant>| last.is_none_or(|t| now.duration_since(t) >= interval);
        assert!(due(None), "first open write is always due");
        assert!(!due(Some(now - Duration::from_secs(5))), "5s after a write → throttled");
        assert!(due(Some(now - Duration::from_secs(31))), "31s after a write → due again");
    }

    #[test]
    fn enabled_default_off_and_from_env_none() {
        std::env::remove_var("NOETL_STATE_SHARD_WRITE");
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
        assert!(StateMaterializerConfig::from_env(&cfg).is_none());
        std::env::set_var("NOETL_STATE_SHARD_WRITE", "true");
        assert!(enabled());
        let built = StateMaterializerConfig::from_env(&cfg).expect("enabled → built");
        assert_eq!(built.consumer, STATE_MATERIALIZER_CONSUMER);
        std::env::remove_var("NOETL_STATE_SHARD_WRITE");
    }
}
