//! Durable store behind the tier service — the first slice where tier data
//! actually moves.
//!
//! **PR 3 of [ai-meta#257](https://github.com/noetl/ai-meta/issues/257).**
//! PR 1 gave the writer a listener; PR 2 gave callers a client. Both were inert
//! by construction because nothing was stored. This module backs the `append`
//! and `read` operations with a real `ehdb_reference` driver over a directory
//! the writer owns, so a remote caller's append is durable and a remote read
//! returns it.
//!
//! # Scope
//!
//! `append` / `read_execution` / `scan`, over the tiers in
//! [`StoreTier`][super::store_tier::StoreTier] — **event-log and projection**
//! ([ai-meta#265](https://github.com/noetl/ai-meta/issues/265) A1). KV, object
//! and vector still have no store here; they gain one in the same change set
//! that gives them a `StoreTier` variant, not before.
//!
//! Each tier is a **separate store file** under the same directory, with its
//! own lock. That is the whole of the genericisation and it is deliberately
//! that small: one engine, one serialised-append critical section, N files. A
//! shared file with a tier discriminator inside each record would put the
//! projection tier's write volume — roughly one append per orchestrator trigger
//! — behind the event log's lock, on the process that is already serving the
//! event-log tier primary in production.
//!
//! The RFC puts `ack` here too; it is held back because `ack` drives segment GC
//! — deleting data on a remote caller's say-so deserves its own PR and its own
//! gate, and bundling it here would put "returns the right bytes" and "removes
//! bytes" in one review.
//!
//! # Store location
//!
//! `NOETL_EHDB_TIER_SERVICE_DIR` names the directory. Unset ⇒ the store is not
//! constructed and every data op answers `unavailable` rather than falling back
//! to a guessed path. A service that silently writes tier data somewhere nobody
//! configured is worse than one that refuses.
//!
//! In production this points **inside the writer's PVC**, which is the whole
//! reason the writer hosts this face: it is the process that owns durable
//! volumes, and prod's volumes are `ReadWriteOnce` so no other pod can mount
//! them.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use ehdb_reference::{
    EventLogAppendRequest, EventLogDriver, EventLogReadExecutionRequest, EventLogScanRequest,
    LocalReferenceEventLogDriver, DEFAULT_LOCAL_REFERENCE_NAMESPACE,
    DEFAULT_LOCAL_REFERENCE_TENANT,
};
use tokio::sync::RwLock;

use super::store_tier::StoreTier;

/// Directory the tier service stores into. Unset ⇒ no store.
pub const TIER_SERVICE_DIR_ENV: &str = "NOETL_EHDB_TIER_SERVICE_DIR";

/// Largest `limit` a caller may request from a scan.
///
/// A remote caller controls this number, and an unbounded scan on a
/// single-replica writer is a denial-of-service with extra steps. Requests above
/// the cap are **clamped, not rejected** — a caller asking for more than we will
/// give should still get the most we will give, and the response carries the
/// count so the truncation is visible rather than silent.
pub const MAX_SCAN_LIMIT: usize = 1_000;

/// Resolved store configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierStoreConfig {
    pub dir: PathBuf,
}

impl TierStoreConfig {
    /// Resolve from the environment. `None` ⇒ no store is configured.
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var(TIER_SERVICE_DIR_ENV).ok()?;
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        Some(Self {
            dir: PathBuf::from(raw),
        })
    }

    /// Path of the JSONL this store appends `tier`'s records to.
    ///
    /// The filename comes from [`StoreTier::file_name`], never from the caller,
    /// so a wire value cannot traverse out of the writer's directory.
    pub fn path_for(&self, tier: StoreTier) -> PathBuf {
        self.dir.join(tier.file_name())
    }

    /// Back-compat alias for the event-log store path.
    pub fn eventlog_path(&self) -> PathBuf {
        self.path_for(StoreTier::EventLog)
    }
}

/// Outcome of a data operation, kept distinct so a caller can tell "there is no
/// store" from "the store has nothing for you". Collapsing those into one empty
/// answer is exactly the absent-vs-broken ambiguity this platform keeps paying
/// for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TierStoreOutcome {
    /// The operation succeeded; payload is the JSON body.
    Ok(String),
    /// No store is configured on this writer.
    Unavailable,
    /// The request was malformed.
    Invalid(String),
    /// The store errored.
    Error(String),
}

/// Per-store lock serialising access to one event-log file.
///
/// **This is the fix for the P0 the serve-ready soak found.** Until the mirror
/// source moved to the server, every worker pod appended to *its own* store
/// through *one* caller, so two appends never overlapped and the store's
/// single-writer assumption held by accident. Pointing every execution's mirror
/// at one writer-fronted store through one relay made concurrent appends the
/// normal case, and the store is not built for them:
///
/// * `LocalJsonlTransactionLog::append_record_to_disk` calls
///   `serde_json::to_writer` straight at the `File` — **unbuffered**, so one
///   record becomes hundreds of small `write(2)` calls. `O_APPEND` makes each
///   of those atomic *individually*, which is precisely the problem: two
///   appenders interleave at write-call granularity and the second record lands
///   **inside** the first one's `payload` byte array. Read-back then hits `{`
///   where a `u8` belongs — `invalid transaction log record at line N: invalid
///   type: map, expected u8`.
/// * the sequence is a read-modify-write. `LocalReferenceEventLogDriver::append`
///   replays the whole log to compute `next = count + 1`, then writes. Two
///   appenders that replay the same state both claim the same
///   `global_sequence`.
///
/// One line of corruption is not one lost record. The replay in
/// `LocalJsonlTransactionLog::open` runs on **every** operation and fails on the
/// first bad line, so a single torn write makes every subsequent append *and*
/// every read fail — which is how the soak got `append` ok 46 / error 302 and
/// `ehdb_unavailable` on all reads.
///
/// The lock closes both holes at once because the replay, the sequence
/// decision and the write all sit inside one critical section.
///
/// **Reads take it too**, shared. A read concurrent with an append would
/// otherwise replay a file whose last line is half-written and report the store
/// broken — the same error, from a store that is fine.
///
/// It is `tokio::sync::RwLock`, not `std::sync`, on purpose: this process also
/// hosts both buses, and N blocked appenders must yield their runtime threads
/// rather than park them. Only the lock holder occupies a thread.
///
/// Keyed by store path so two distinct stores never block each other — which is
/// also what keeps the tests below independent.
///
/// Since #265 the key is the **per-tier** path, so the projection tier's appends
/// and the event log's do not serialise against each other. They are different
/// files; sharing a lock would be pure contention with no invariant behind it.
fn store_lock(cfg: &TierStoreConfig, tier: StoreTier) -> Arc<RwLock<()>> {
    static LOCKS: OnceLock<StdMutex<HashMap<PathBuf, Arc<RwLock<()>>>>> = OnceLock::new();
    let registry = LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
    // Poison-tolerant: the registry guards a map, not an invariant, and a
    // panic while holding it must not take down the writer.
    let mut guard = registry.lock().unwrap_or_else(|e| e.into_inner());
    guard
        .entry(cfg.path_for(tier))
        .or_insert_with(|| Arc::new(RwLock::new(())))
        .clone()
}

/// One engine for every tier.
///
/// `LocalReferenceEventLogDriver` is an append-only sequenced record store; the
/// "event log" in its name is where it was first used, not a constraint on what
/// it can hold. Reusing it for the projection tier means the concurrency fix
/// that #257's soak paid for — the serialised replay → sequence → write window
/// — protects the new tier from its first append rather than being reproduced
/// for it.
/// The producer's `event_id`, lifted out of a mirrored payload.
///
/// The payload is the server's `mirror_payload` JSON, whose first field is
/// `event_id` — a snowflake, minted once and immutable, and the only value in
/// the body that identifies the event across redeliveries (noetl/ai-meta#313).
///
/// ⚠ Extracted HERE rather than inside the driver. The driver's append is the
/// write path of a `primary`-serving tier; a parse per record there would be on
/// that path, and it would silently degrade to "never matches" — a dedupe that
/// stops working while still reporting success — the day the payload shape moved.
/// Doing it at the boundary means a shape change shows up as `event_id: None`,
/// i.e. dedupe simply off, which is the safe direction.
///
/// Accepts both the numeric and string spellings: the server emits `event_id` as
/// a JSON number, but a re-serialised payload can carry it quoted.
fn event_id_from_payload(payload: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(payload).ok()?;
    match v.get("event_id")? {
        serde_json::Value::String(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Seal the tier's active log once it exceeds this many bytes.
///
/// ⚠⚠ WHY THIS EXISTS — measured, not assumed.
///
/// The tier is **not** an L0 engine, so it has none of the part-sealing the
/// command bus and events feed get from `seal_aged_parts`. It is one JSONL file
/// that `LocalReferenceRuntime::open` replays **in full** into an in-memory
/// `ReferenceDatabase`, which the reference-runtime cache then holds for the
/// life of the process. Two consequences, both linear in the record count:
///
/// * **Memory** is proportional to the store. In production the store reached
///   **4.0 GB** and the writer's baseline sat at ~3 GiB.
/// * **Every append costs a copy of that state** — `LocalReferenceRuntime::append`
///   does `self.state.clone()` before applying. Measured on a synthetic store:
///
///   ```text
///   records   ms/append   on_disk
///       400       8.12      0.7MB
///      2000      20.87      3.6MB
///      4000      36.56      7.2MB
///   ```
///
///   A 4.5x rise in per-append cost over a 10x store growth — O(n) per append.
///   At production size that is the observed `"timed out after 4s"`.
///
/// On 2026-09-20 this OOM-killed `noetl-cmdbus-writer-0`, which hosts BOTH
/// buses, repeatedly — at a 4 GiB limit and again at 8 GiB. Raising the limit
/// is a delay; bounding the store is the fix.
///
/// **Unset ⇒ no sealing ⇒ byte-identical to today.** A store that has never
/// sealed has exactly one segment, which is the file that exists now.
pub const TIER_SEAL_MAX_BYTES_ENV: &str = "NOETL_EHDB_TIER_SEAL_MAX_BYTES";

/// Resolve the seal threshold. `None` (unset/unparsable/zero) ⇒ never seal.
///
/// Fail-safe direction is **off**: a typo must not start rotating a
/// primary-serving tier's store behind the operator's back.
pub fn tier_seal_max_bytes() -> Option<u64> {
    parse_seal_max_bytes(std::env::var(TIER_SEAL_MAX_BYTES_ENV).ok().as_deref())
}

/// Pure parse, so the fail-safe direction is testable without the process env.
pub fn parse_seal_max_bytes(raw: Option<&str>) -> Option<u64> {
    raw.and_then(|v| v.trim().parse::<u64>().ok()).filter(|n| *n > 0)
}

/// Sealed segments of `tier`, oldest first.
///
/// Named `<active>.<seq>` beside the active file — same directory, **same JSONL
/// format**, so nothing about the on-disk encoding changes. Sealing renames;
/// it never rewrites and never deletes.
pub fn sealed_segments(cfg: &TierStoreConfig, tier: StoreTier) -> Vec<PathBuf> {
    let active = cfg.path_for(tier);
    let Some(dir) = active.parent() else {
        return Vec::new();
    };
    let Some(stem) = active.file_name().and_then(|s| s.to_str()) else {
        return Vec::new();
    };
    let prefix = format!("{stem}.");
    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    for e in rd.flatten() {
        let p = e.path();
        let Some(name) = p.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if let Some(rest) = name.strip_prefix(&prefix) {
            // ⚠ `parse::<u64>` IS the filter: it rejects "", "bak", "1a" and
            // "-1" on its own. An earlier version also checked every byte was a
            // digit; the mutation battery showed that guard was redundant
            // (removing it changed no behaviour), so it is gone rather than left
            // as a second rule that could drift from the first. A stray
            // `eventlog.jsonl.bak` is still never replayed as tier data.
            if let Ok(n) = rest.parse::<u64>() {
                found.push((n, p));
            }
        }
    }
    found.sort_by_key(|(n, _)| *n);
    found.into_iter().map(|(_, p)| p).collect()
}

/// Seal the active log if it is over the threshold. Returns the sealed path.
///
/// ⚠ Called with the store's WRITE lock held, before an append, so no reader or
/// writer can be mid-operation on the file being renamed.
///
/// ⚠ The rename is the whole operation. There is no copy and no truncate, so
/// there is no window in which data exists in neither file and no way for this
/// to lose a record: either the rename happened or it did not.
fn maybe_seal(cfg: &TierStoreConfig, tier: StoreTier, max: Option<u64>) -> Option<PathBuf> {
    let max = max?;
    let active = cfg.path_for(tier);
    let len = std::fs::metadata(&active).ok()?.len();
    if len < max {
        return None;
    }
    let next = sealed_segments(cfg, tier)
        .last()
        .and_then(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .and_then(|s| s.rsplit('.').next())
                .and_then(|s| s.parse::<u64>().ok())
        })
        .unwrap_or(0)
        + 1;
    let sealed = active.with_file_name(format!(
        "{}.{next}",
        active.file_name().and_then(|s| s.to_str()).unwrap_or("tier")
    ));
    match std::fs::rename(&active, &sealed) {
        Ok(()) => {
            // ⚠ The cached runtime is keyed by path and holds the replayed state
            // of the file that just moved. Dropping it is what actually releases
            // the memory — without this the seal bounds the FILE and not the
            // process, which is the entire point.
            ehdb_reference::forget_runtime(&active);
            tracing::info!(
                tier = tier.as_str(),
                bytes = len,
                sealed = %sealed.display(),
                "EHDB tier store sealed (noetl/ai-meta#332 writer OOM)"
            );
            // ⚠ Indexed in the BACKGROUND, not here: building it replays the
            // whole segment, and this runs under the store's write lock on the
            // append path. Doing it inline would make the append that happens to
            // trigger a seal pay a multi-GB scan.
            spawn_index_build(sealed.clone(), tier);
            Some(sealed)
        }
        Err(e) => {
            tracing::warn!(tier = tier.as_str(), error = %e, "EHDB tier seal failed; continuing unsealed");
            None
        }
    }
}

fn driver(cfg: &TierStoreConfig, tier: StoreTier) -> LocalReferenceEventLogDriver {
    LocalReferenceEventLogDriver::new(
        cfg.path_for(tier),
        DEFAULT_LOCAL_REFERENCE_TENANT.to_string(),
        DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string(),
    )
}

/// Ensure the store directory exists. Called before an append; a missing parent
/// directory is a configuration state, not an error to surface per-request.
fn ensure_dir(cfg: &TierStoreConfig) -> Result<(), String> {
    std::fs::create_dir_all(&cfg.dir).map_err(|e| format!("create {}: {e}", cfg.dir.display()))
}

/// Append one record. `execution_id` and `payload` are required; an empty
/// payload is refused rather than stored, because an empty record is
/// indistinguishable from a read miss later.
pub async fn append(
    cfg: Option<&TierStoreConfig>,
    tier: StoreTier,
    execution_id: &str,
    payload: &str,
) -> TierStoreOutcome {
    let Some(cfg) = cfg else {
        return TierStoreOutcome::Unavailable;
    };
    // Validation is outside the critical section: refusing a malformed request
    // must not wait behind a queue of good ones.
    if execution_id.trim().is_empty() {
        return TierStoreOutcome::Invalid("execution_id is empty".to_string());
    }
    if payload.is_empty() {
        return TierStoreOutcome::Invalid("payload is empty".to_string());
    }
    append_with_seal(Some(cfg), tier, execution_id, payload, tier_seal_max_bytes()).await
}

/// [`append`] with the seal threshold injected.
///
/// ⚠ Exists so tests can arm the seal WITHOUT `std::env::set_var`. They did
/// once: `cargo test` does not serialise tests, the variable is process-global,
/// and it armed sealing inside an unrelated sibling
/// (`concurrent_appends_to_one_tier_stay_readable` went from 24 records to 8).
/// A test that can only be written by mutating global state is a test that will
/// eventually break a different one.
pub async fn append_with_seal(
    cfg: Option<&TierStoreConfig>,
    tier: StoreTier,
    execution_id: &str,
    payload: &str,
    seal_max: Option<u64>,
) -> TierStoreOutcome {
    let Some(cfg) = cfg else {
        return TierStoreOutcome::Unavailable;
    };
    if execution_id.trim().is_empty() {
        return TierStoreOutcome::Invalid("execution_id is empty".to_string());
    }
    if payload.is_empty() {
        return TierStoreOutcome::Invalid("payload is empty".to_string());
    }
    let lock = store_lock(cfg, tier);
    let _exclusive = lock.write().await;
    // ⚠ Under the WRITE lock, before the append: no reader or writer can be
    // mid-operation on the file being renamed. No-op unless a threshold is
    // configured AND the active segment is over it.
    let _ = maybe_seal(cfg, tier, seal_max);
    append_locked(cfg, tier, execution_id, payload)
}

/// Append N records under ONE store-write lock and ONE `fsync`
/// (noetl/ai-meta#155).
///
/// The single-record path pays an `fsync` per record — measured at ~118 ms per
/// record at production payload size, on an empty store, so it is a fixed cost
/// and it dominates mirroring. This is the same work with the per-record cost
/// paid once.
///
/// Durability and ordering are unchanged: the engine writes the records in the
/// order given and returns only after the `fsync` that covers them, so every
/// returned sequence is on disk exactly as it was before.
///
/// Returns one outcome per record, in request order, so a caller can report
/// per-record results exactly as it did when it looped.
pub async fn append_batch(
    cfg: Option<&TierStoreConfig>,
    tier: StoreTier,
    execution_id: &str,
    payloads: &[String],
) -> Vec<TierStoreOutcome> {
    let Some(cfg) = cfg else {
        return payloads
            .iter()
            .map(|_| TierStoreOutcome::Unavailable)
            .collect();
    };
    if payloads.is_empty() {
        return Vec::new();
    }
    // Validation outside the critical section, same as the single path. An
    // invalid batch is refused whole: a partial append would leave the caller
    // unable to say which records landed.
    if execution_id.trim().is_empty() {
        return payloads
            .iter()
            .map(|_| TierStoreOutcome::Invalid("execution_id is empty".to_string()))
            .collect();
    }
    if let Some(index) = payloads.iter().position(|p| p.is_empty()) {
        return payloads
            .iter()
            .map(|_| TierStoreOutcome::Invalid(format!("payload {index} of the batch is empty")))
            .collect();
    }

    let lock = store_lock(cfg, tier);
    let _exclusive = lock.write().await;
    let _ = maybe_seal(cfg, tier, tier_seal_max_bytes());
    append_batch_locked(cfg, tier, execution_id, payloads)
}

fn append_batch_locked(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    execution_id: &str,
    payloads: &[String],
) -> Vec<TierStoreOutcome> {
    if let Err(e) = ensure_dir(cfg) {
        return payloads
            .iter()
            .map(|_| TierStoreOutcome::Error(e.clone()))
            .collect();
    }
    let requests: Vec<EventLogAppendRequest> = payloads
        .iter()
        .map(|payload| EventLogAppendRequest {
            execution_id: execution_id.to_string(),
            transaction_id: super::eventlog::new_transaction_id(),
            event_id: event_id_from_payload(payload),
            payload: payload.clone(),
        })
        .collect();

    match driver(cfg, tier).append_batch(&requests) {
        Ok(outs) => {
            // Record store state once for the batch — the same signal the
            // single path records per append (ai-meta#260), and the last
            // record's sequence is the store's sequence after the batch.
            if let Some(last) = outs.last() {
                super::metrics::record_tier_service_append(
                    tier,
                    last.global_sequence,
                    store_bytes(cfg, tier),
                );
            }
            outs.into_iter()
                .map(|out| {
                    TierStoreOutcome::Ok(
                        serde_json::to_string(&serde_json::json!({
                            "appended": true,
                            "global_sequence": out.global_sequence,
                            "log_record_count": out.log_record_count,
                            // ⚠ The caller checks the reply for strictly
                            // increasing sequences and for
                            // log_record_count == global_sequence.  A dedupe
                            // satisfies NEITHER — it returns the existing
                            // position and does not advance the count — so
                            // without this flag a working dedupe reports as a
                            // parity divergence (noetl/ai-meta#313).
                            "deduplicated": out.deduplicated,
                        }))
                        .unwrap_or_else(|_| "{}".to_string()),
                    )
                })
                .collect()
        }
        // The batch is refused whole, so every record reports the same error
        // rather than the caller guessing a split point.
        Err(e) => payloads
            .iter()
            .map(|_| TierStoreOutcome::Error(e.to_string()))
            .collect(),
    }
}

/// The append itself. Callers reach it only through [`append`], which holds the
/// store's write lock for the whole replay → sequence → write → flush window.
fn append_locked(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    execution_id: &str,
    payload: &str,
) -> TierStoreOutcome {
    if execution_id.trim().is_empty() {
        return TierStoreOutcome::Invalid("execution_id is empty".to_string());
    }
    if payload.is_empty() {
        return TierStoreOutcome::Invalid("payload is empty".to_string());
    }
    if let Err(e) = ensure_dir(cfg) {
        return TierStoreOutcome::Error(e);
    }
    let request = EventLogAppendRequest {
        execution_id: execution_id.to_string(),
        transaction_id: super::eventlog::new_transaction_id(),
        payload: payload.to_string(),
        // Deploy A: inert.  Deploy B populates this from the payload's event_id.
        event_id: event_id_from_payload(payload),
    };
    match driver(cfg, tier).append(&request) {
        Ok(out) => {
            // The store's own state, recorded on the write path (ai-meta#260).
            // Observing it here rather than on a read is what makes the tier
            // checkable without generating traffic against it — the question
            // before a `primary` flip is "does this store hold anything", and
            // answering it by reading would change what is being measured.
            super::metrics::record_tier_service_append(
                tier,
                out.global_sequence,
                store_bytes(cfg, tier),
            );
            // `log_record_count` is what makes the append VERIFIABLE by the
            // caller.  The remote appender cannot open this store, so without the
            // count in the reply the only parity check available to it is
            // ordering — and the serve decision on the service-resolved path
            // (`eventlog::serve_service_append`) needs the same gapless
            // invariant `mirror_event` checks locally: `log_record_count ==
            // global_sequence`.  Additive: a caller that does not read it is
            // unaffected, and one that does degrades to the ordering check alone
            // when talking to a writer that predates the field.
            TierStoreOutcome::Ok(
                serde_json::to_string(&serde_json::json!({
                    "appended": true,
                    "global_sequence": out.global_sequence,
                    "log_record_count": out.log_record_count,
                    // See the batch path: a dedupe fails both parity checks
                    // unless the caller is told it was one.
                    "deduplicated": out.deduplicated,
                }))
                .unwrap_or_else(|_| "{}".to_string()),
            )
        }
        Err(e) => TierStoreOutcome::Error(e.to_string()),
    }
}

/// Size of the backing event-log file, or 0 when it does not exist yet.
///
/// 0-on-error is safe here in a way it usually is not: this is only ever
/// reported alongside `store_appends_total` and `store_sequence`, so a stat that
/// fails shows as "0 bytes holding N records", which is visibly wrong rather
/// than quietly plausible.
pub(crate) fn store_bytes(cfg: &TierStoreConfig, tier: StoreTier) -> u64 {
    std::fs::metadata(cfg.path_for(tier))
        .map(|m| m.len())
        .unwrap_or(0)
}

/// Highest global sequence the store already holds, read once at startup.
///
/// Without this, a writer that restarts in front of a populated store reports
/// `sequence 0` until the next append — which reads exactly like an empty store,
/// on the component being promoted to authoritative.
pub(crate) fn startup_sequence(cfg: &TierStoreConfig, tier: StoreTier) -> u64 {
    match driver(cfg, tier).scan_global(&EventLogScanRequest {
        after: None,
        limit: MAX_SCAN_LIMIT,
    }) {
        Ok(out) => out
            .records
            .iter()
            .map(|r| r.global_sequence)
            .max()
            .unwrap_or(0),
        Err(_) => 0,
    }
}

/// Read every record for one execution.
pub async fn read_execution(
    cfg: Option<&TierStoreConfig>,
    tier: StoreTier,
    execution_id: &str,
) -> TierStoreOutcome {
    let Some(cfg) = cfg else {
        return TierStoreOutcome::Unavailable;
    };
    if execution_id.trim().is_empty() {
        return TierStoreOutcome::Invalid("execution_id is empty".to_string());
    }
    let lock = store_lock(cfg, tier);
    let _shared = lock.read().await;
    read_execution_locked(cfg, tier, execution_id)
}

fn read_execution_locked(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    execution_id: &str,
) -> TierStoreOutcome {
    let request = EventLogReadExecutionRequest {
        execution_id: execution_id.to_string(),
        after: None,
        limit: MAX_SCAN_LIMIT,
    };
    // ⚠⚠ Sealed segments FIRST, then the active one. A read that consulted only
    // the active segment would silently lose every record written before the
    // last seal — turning a memory fix into a data-loss bug, which is strictly
    // worse than the OOM it was meant to cure. `sealed_segments` is empty on a
    // store that has never sealed, so this is exactly today's behaviour there.
    // ⚠ The INDEX decides which sealed segments must be opened. Without it every
    // read replays every sealed segment, which is what broke the tier read path
    // after sealing (2259 "timed out after 2s" in production). A segment with no
    // index is still opened — absence is "unknown", never "absent".
    let (sealed, total_sealed) = segments_possibly_holding(cfg, tier, execution_id);
    if !sealed.is_empty() {
        return read_execution_across_segments(cfg, tier, execution_id, &sealed, &request);
    }
    if total_sealed > 0 {
        // Every sealed segment was ruled out by its index; the active segment
        // alone answers. This is the fast path the index exists to create.
        tracing::debug!(
            tier = tier.as_str(),
            execution_id,
            skipped = total_sealed,
            "tier read skipped sealed segments via index"
        );
    }
    match driver(cfg, tier).read_execution(&request) {
        Ok(out) => {
            let mut v = serde_json::to_value(&out).unwrap_or(serde_json::Value::Null);
            // ⚠ The driver reports `exists: true` for an execution it holds NO
            // records for, so a caller using `exists` to decide hit-vs-miss gets
            // the wrong answer (found by the PR-3 gate, which asserts on record
            // payloads for exactly this reason).  Normalise it at the boundary we
            // own rather than leaving a field that means the opposite of its name:
            // `exists` now tracks whether any record came back.
            if let Some(obj) = v.as_object_mut() {
                let n = obj
                    .get("record_count")
                    .and_then(|c| c.as_u64())
                    .unwrap_or(0);
                obj.insert("exists".to_string(), serde_json::Value::Bool(n > 0));
            }
            TierStoreOutcome::Ok(serde_json::to_string(&v).unwrap_or_else(|_| "{}".to_string()))
        }
        Err(e) => TierStoreOutcome::Error(e.to_string()),
    }
}

/// Build a segment's index off the request path.
///
/// Best-effort by design: a failed index build leaves the segment unindexed,
/// which means "might contain" — correct, just slow. It must never fail an
/// append or a read.
/// Index builds are serialised behind this lock.
///
/// The backfill spawns one build per unindexed segment, and production has two
/// (1.08 GB + 3.3 GB). Serialising bounds the work to one segment scan at a
/// time rather than letting startup contend for CPU across all of them.
static INDEX_BUILD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn spawn_index_build(segment: PathBuf, tier: StoreTier) {
    tokio::spawn(async move {
        let seg = segment.clone();
        let res = tokio::task::spawn_blocking(move || {
            let _serialised = INDEX_BUILD_LOCK.lock();
            build_segment_index(&seg)
        })
        .await;
        match res {
            Ok(Ok(n)) => tracing::info!(
                tier = tier.as_str(), segment = %segment.display(), executions = n,
                "EHDB tier segment indexed"
            ),
            Ok(Err(e)) => tracing::warn!(
                tier = tier.as_str(), segment = %segment.display(), error = %e,
                "EHDB tier segment index build failed; reads will open this segment"
            ),
            Err(e) => tracing::warn!(
                tier = tier.as_str(), segment = %segment.display(), error = %e,
                "EHDB tier segment index task failed; reads will open this segment"
            ),
        }
    });
}

/// Index any sealed segment that has none yet.
///
/// ⚠ Needed because segments sealed by an earlier build have no index, and
/// production already has two of them (1.08 GB and 3.3 GB). Without a backfill
/// the index would only ever help segments sealed in the future, leaving the
/// exact segments that caused the regression unindexed forever.
pub fn backfill_segment_indexes(cfg: &TierStoreConfig) {
    for &tier in StoreTier::ALL.iter() {
        for seg in sealed_segments(cfg, tier) {
            if load_segment_index(&seg).is_none() {
                tracing::info!(
                    tier = tier.as_str(), segment = %seg.display(),
                    "EHDB tier segment has no index; building it"
                );
                spawn_index_build(seg, tier);
            }
        }
    }
}

/// The execution-id index beside a sealed segment: `<segment>.idx`.
///
/// ⚠⚠ WHY THIS EXISTS — a regression I introduced and measured.
///
/// Sealing bounded memory (2.9 GB -> 186 MB in production) but broke the tier's
/// READ path. A merged read must consult every sealed segment, and
/// `read_segment_then_release` forgets each one immediately, so **every** read
/// re-replays the whole sealed file. Production's first sealed segment is
/// 1.08 GB, which cannot be replayed inside the tier client's 2-second read
/// timeout. Measured on prod logs, split at the seal:
///
/// ```text
///   before the seal (22:00-22:30Z):     0 read failures
///   after  the seal (22:35Z onward): 2259 read failures, all "timed out after 2s"
/// ```
///
/// (I had previously called this failure pre-existing. That was wrong: I ran a
/// 100-minute window and asserted it spanned the seal without checking the
/// timestamps. It did not.)
///
/// The index makes a read skip segments that cannot contain the execution.
/// That works because the tier mirrors an execution's events over the execution's
/// lifetime, so an execution's records land in the segment active at the time —
/// almost always exactly one.
///
/// Format: one execution id per line, sorted, newline-terminated. A sidecar, so
/// **the segment's own bytes are untouched** and an older binary that ignores
/// `.idx` files still reads the segment correctly.
///
/// ⚠ A MISSING index means "might contain" — the segment is opened. Absence must
/// never be read as "does not contain", or a read would silently skip records
/// that exist, which is the data-loss-shaped failure this whole change has been
/// avoiding.
fn segment_index_path(segment: &std::path::Path) -> PathBuf {
    let mut s = segment.as_os_str().to_os_string();
    s.push(".idx");
    PathBuf::from(s)
}

/// Load a segment's index. `None` = no index = "might contain, must open".
fn load_segment_index(segment: &std::path::Path) -> Option<std::collections::HashSet<String>> {
    let raw = std::fs::read_to_string(segment_index_path(segment)).ok()?;
    // An index that exists but is EMPTY is a real answer (a segment with no
    // executions), so it is distinguished from a missing file by `Some`.
    Some(
        raw.lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(|l| l.to_string())
            .collect(),
    )
}

/// Build the index for a sealed segment by paging its records once.
///
/// One replay total: the runtime cache serves the pages after the first open,
/// and the runtime is released at the end so the scan does not leave a
/// multi-GB state resident — the point of sealing in the first place.
///
/// Written to a temp file and renamed, so a reader never observes a partial
/// index and mistake it for "this segment contains only these executions".
/// Extract the execution id from one stored segment line.
///
/// The driver derives `EventLogRecordView::execution_id` as
/// `execution_from_subject(record.subject)` (`ehdb-reference/src/eventlog.rs`),
/// i.e. the trailing token after [`ehdb_reference::EVENT_LOG_SUBJECT_PREFIX`].
/// This reads the same field off the stored line, so the index is keyed exactly
/// as a lookup keys it. `streaming_index_equals_replay_index` is the guard on
/// that claim — it builds both ways and asserts set equality.
///
/// Why a targeted scan rather than a full JSON parse: a stored payload is a
/// **numeric byte array**, so it contains no quoted strings and cannot produce a
/// false `"subject":"` match; parsing it as `Value` would allocate one `Value`
/// per payload byte for no gain. Measured on the production segment, 82,906 of
/// 82,906 lines carry the subject.
fn execution_id_from_line(line: &str) -> Option<String> {
    const KEY: &str = "\"subject\":\"";
    let start = line.find(KEY)? + KEY.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    let subject = &rest[..end];
    Some(
        subject
            .strip_prefix(&format!("{}.", ehdb_reference::EVENT_LOG_SUBJECT_PREFIX))
            .unwrap_or(subject)
            .to_string(),
    )
}

/// Build `<segment>.idx` by STREAMING the segment, never replaying it.
///
/// ⚠ This must not go back to a `LocalReferenceEventLogDriver` replay. Replay
/// holds the whole segment in the runtime cache, and RSS was measured at ~2.8x
/// record bytes for this workload — production's two sealed segments are 1.08 GB
/// and 3.3 GB, so a replay-based backfill peaks around 12 GB and OOM-kills the
/// writer on startup, on every restart. Streaming is O(1) in the segment size:
/// one line at a time, and only the distinct ids are retained (969 for the
/// 1.08 GB production segment).
///
/// ⚠ Fail-safe direction: a line whose subject cannot be read ABORTS the build
/// and writes no index. A missing index means "might contain" and the segment is
/// opened — correct but slow. A *partial* index read as complete would silently
/// skip a segment that holds the execution, which is a wrong answer.
pub fn build_segment_index(segment: &std::path::Path) -> Result<usize, String> {
    use std::io::BufRead;
    let f = std::fs::File::open(segment).map_err(|e| e.to_string())?;
    let reader = std::io::BufReader::with_capacity(1 << 20, f);
    let mut ids: std::collections::BTreeSet<String> = Default::default();
    for (n, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| format!("line {}: {e}", n + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        match execution_id_from_line(&line) {
            Some(id) => {
                ids.insert(id);
            }
            None => {
                return Err(format!(
                    "line {} carries no readable subject; refusing to write a partial index",
                    n + 1
                ))
            }
        }
    }

    let body: String = ids.iter().map(|i| format!("{i}\n")).collect();
    let final_path = segment_index_path(segment);
    let tmp = final_path.with_extension("idx.tmp");
    std::fs::write(&tmp, body.as_bytes()).map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, &final_path).map_err(|e| e.to_string())?;
    Ok(ids.len())
}

/// Segments that could hold `execution_id`, oldest first.
///
/// A segment with no index is INCLUDED — absence of an index is "unknown", never
/// "absent".
fn segments_possibly_holding(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    execution_id: &str,
) -> (Vec<PathBuf>, usize) {
    let all = sealed_segments(cfg, tier);
    let total = all.len();
    let kept = all
        .into_iter()
        .filter(|s| match load_segment_index(s) {
            Some(ids) => ids.contains(execution_id),
            None => true,
        })
        .collect();
    (kept, total)
}

/// Read a segment and RELEASE it, so a merge is bounded by one segment at a
/// time rather than by the sum of them.
///
/// ⚠⚠ Without this the merge is a second route to the OOM it was built to cure.
/// `LocalReferenceEventLogDriver` opens through the reference-runtime cache,
/// which replays the file and then holds that state for the life of the
/// process. Production's existing tier store is a single **4.1 GB** file, so
/// the first seal makes it sealed segment #1 — and the first cross-segment read
/// would replay 4.1 GB back into memory and keep it, on top of the active
/// segment's state. Two segments later that is worse than where this started.
///
/// So each SEALED segment is forgotten immediately after it is read. The active
/// segment is deliberately NOT forgotten: it is the one the append path uses on
/// every write, and evicting it would reintroduce a full replay per append —
/// the exact cost the runtime cache exists to avoid.
fn read_segment_then_release<T>(
    path: &PathBuf,
    is_sealed: bool,
    f: impl FnOnce(&LocalReferenceEventLogDriver) -> T,
) -> T {
    let d = LocalReferenceEventLogDriver::new(
        path.clone(),
        DEFAULT_LOCAL_REFERENCE_TENANT.to_string(),
        DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string(),
    );
    let out = f(&d);
    if is_sealed {
        ehdb_reference::forget_runtime(path);
    }
    out
}

/// Global scan across sealed segments + the active one, oldest first.
///
/// ⚠ `after` is applied to the **merged, renumbered** sequence, not to each
/// segment's own numbering. Passing it down per segment would skip a different
/// set of records in every file, because each driver numbers from 1 within its
/// own log.
fn scan_across_segments(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    sealed: &[PathBuf],
    after: Option<u64>,
    limit: usize,
) -> TierStoreOutcome {
    let mut records: Vec<serde_json::Value> = Vec::new();
    let mut paths: Vec<PathBuf> = sealed.to_vec();
    paths.push(cfg.path_for(tier));
    let last = paths.len() - 1;
    for (i, path) in paths.iter().enumerate() {
        let sealed_seg = i < last; // the final entry is the ACTIVE segment
        match read_segment_then_release(path, sealed_seg, |d| {
            d.scan_global(&EventLogScanRequest {
                after: None,
                limit: MAX_SCAN_LIMIT,
            })
        }) {
            Ok(out) => {
                if let Ok(serde_json::Value::Object(mut o)) = serde_json::to_value(&out) {
                    if let Some(serde_json::Value::Array(rs)) = o.remove("records") {
                        records.extend(rs);
                    }
                }
            }
            Err(e) => {
                return TierStoreOutcome::Error(format!(
                    "tier segment {} unreadable: {e}",
                    path.display()
                ))
            }
        }
    }
    let total = records.len();
    for (i, r) in records.iter_mut().enumerate() {
        if let Some(o) = r.as_object_mut() {
            o.insert(
                "global_sequence".to_string(),
                serde_json::Value::from((i + 1) as u64),
            );
        }
    }
    let skip = after.unwrap_or(0) as usize;
    let page: Vec<serde_json::Value> = records.into_iter().skip(skip).take(limit).collect();
    let body = serde_json::json!({
        "action": "eventlog-scan",
        "exists": total > 0,
        "record_count": total,
        "returned": page.len(),
        "records": page,
        "segments": sealed.len() + 1,
    });
    TierStoreOutcome::Ok(serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()))
}

/// Read one execution across sealed segments + the active one, oldest first.
///
/// ⚠ `global_sequence` is **renumbered** over the merged result. Each segment's
/// driver numbers records from 1 within its own file, so the raw values collide
/// across segments. Renumbering keeps the field meaning what a reader expects —
/// a total order over what was returned — instead of handing back several
/// records that all claim to be number 1.
fn read_execution_across_segments(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    execution_id: &str,
    sealed: &[PathBuf],
    request: &EventLogReadExecutionRequest,
) -> TierStoreOutcome {
    let mut records: Vec<serde_json::Value> = Vec::new();
    let mut paths: Vec<PathBuf> = sealed.to_vec();
    paths.push(cfg.path_for(tier));
    let last = paths.len() - 1;
    for (i, path) in paths.iter().enumerate() {
        let sealed_seg = i < last; // the final entry is the ACTIVE segment
        match read_segment_then_release(path, sealed_seg, |d| d.read_execution(request)) {
            Ok(out) => {
                if let Ok(serde_json::Value::Object(mut o)) = serde_json::to_value(&out).map(|v| v)
                {
                    if let Some(serde_json::Value::Array(rs)) = o.remove("records") {
                        records.extend(rs);
                    }
                }
            }
            // A segment that cannot be read is NOT silently skipped: a partial
            // answer presented as complete is the failure this whole change is
            // trying not to cause.
            Err(e) => {
                return TierStoreOutcome::Error(format!(
                    "tier segment {} unreadable: {e}",
                    path.display()
                ))
            }
        }
    }
    for (i, r) in records.iter_mut().enumerate() {
        if let Some(o) = r.as_object_mut() {
            o.insert(
                "global_sequence".to_string(),
                serde_json::Value::from((i + 1) as u64),
            );
        }
    }
    let n = records.len();
    let body = serde_json::json!({
        "action": "eventlog-read-execution",
        "execution_id": execution_id,
        "exists": n > 0,
        "record_count": n,
        "returned": n,
        "records": records,
        "segments": sealed.len() + 1,
    });
    TierStoreOutcome::Ok(serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()))
}

/// Bounded global scan. `limit` is clamped to [`MAX_SCAN_LIMIT`].
pub async fn scan(
    cfg: Option<&TierStoreConfig>,
    tier: StoreTier,
    after: Option<u64>,
    limit: usize,
) -> TierStoreOutcome {
    let Some(cfg) = cfg else {
        return TierStoreOutcome::Unavailable;
    };
    let lock = store_lock(cfg, tier);
    let _shared = lock.read().await;
    scan_locked(cfg, tier, after, limit)
}

fn scan_locked(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    after: Option<u64>,
    limit: usize,
) -> TierStoreOutcome {
    let limit = limit.clamp(1, MAX_SCAN_LIMIT);
    // ⚠⚠ Sealed segments first, same as `read_execution`. This was MISSED in the
    // first cut of the seal — `read_execution` merged and `scan` did not — and a
    // leaked env var in a sibling test is what exposed it: a scan that had been
    // returning 24 records returned 8, the active segment only. Silently
    // returning a suffix of the log is the data-loss failure this seal must not
    // cause, so both readers go through the same merge.
    let sealed = sealed_segments(cfg, tier);
    if !sealed.is_empty() {
        return scan_across_segments(cfg, tier, &sealed, after, limit);
    }
    match driver(cfg, tier).scan_global(&EventLogScanRequest { after, limit }) {
        Ok(out) => TierStoreOutcome::Ok(
            serde_json::to_string(&out).unwrap_or_else(|_| "{}".to_string()),
        ),
        Err(e) => TierStoreOutcome::Error(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // ---- tier seal (noetl/ai-meta#332 writer OOM) --------------------------

    fn seal_cfg(tag: &str) -> TierStoreConfig {
        let mut d = std::env::temp_dir();
        d.push(format!(
            "ehdb-seal-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        TierStoreConfig { dir: d }
    }

    /// ⭐⭐ A merge must be bounded by ONE segment at a time, not by their sum.
    ///
    /// Production's tier store is a single 4.1 GB file, so the first seal makes
    /// it sealed segment #1. If the merge left each segment cached, the first
    /// cross-segment read would replay 4.1 GB back into memory and KEEP it —
    /// recreating the OOM by a second route.
    ///
    /// ⚠ This test SETS `NOETL_EHDB_REFERENCE_RUNTIME_CACHE`, and that is a
    /// deliberate exception to "no `set_var` in tests". The property under test
    /// only exists when the cache is on — with it off nothing is retained and
    /// the assertion is vacuous, which is exactly how an earlier version of
    /// this test passed against a planted defect. The distinction from the seal
    /// threshold (which is injected, never set globally) is that this flag
    /// changes only CACHING; it cannot change what a sibling test stores or
    /// reads back. The precondition is asserted, so if that ever stops being
    /// true the test fails rather than quietly proving nothing.
    #[tokio::test]
    async fn a_merged_read_does_not_leave_sealed_segments_cached() {
        std::env::set_var("NOETL_EHDB_REFERENCE_RUNTIME_CACHE", "true");
        assert!(
            ehdb_reference::runtime_cache_enabled(),
            "the runtime cache must be ON, or nothing is retained and this test \
             proves nothing (it passed against a planted defect that way once)"
        );

        let cfg = seal_cfg("release");
        const N: usize = 30;
        for i in 0..N {
            let _ = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                "exec-rel",
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "q".repeat(120)),
                Some(2048),
            )
            .await;
        }
        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        assert!(
            segs.len() >= 2,
            "need >=2 sealed segments to exercise the merge, got {}",
            segs.len()
        );

        for sp in &segs {
            ehdb_reference::forget_runtime(sp);
        }
        ehdb_reference::forget_runtime(&cfg.path_for(StoreTier::EventLog));

        let got = read_execution(Some(&cfg), StoreTier::EventLog, "exec-rel").await;
        assert!(matches!(got, TierStoreOutcome::Ok(_)), "read failed: {got:?}");

        // The ACTIVE segment stays cached on purpose — the append path uses it
        // on every write, and evicting it would reintroduce a full replay per
        // append. Every SEALED one must be gone.
        for sp in &segs {
            assert!(
                !ehdb_reference::forget_runtime(sp),
                "sealed segment {} was STILL cached after the merge — each one \
                 holds its whole file replayed, so leaving them is the OOM again",
                sp.display()
            );
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⭐⭐ THE ONE THAT MATTERS. An index must never make a read MISS records
    /// that exist. Skipping a segment that does contain the execution is silent
    /// data loss — strictly worse than the slow reads the index exists to cure.
    #[tokio::test]
    async fn an_indexed_read_returns_exactly_what_an_unindexed_read_returns() {
        let cfg = seal_cfg("idx-equiv");
        const N: usize = 36;
        for i in 0..N {
            let _ = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                // Two executions interleaved, so segments genuinely differ in
                // which ids they hold — a fixture where every segment held every
                // execution could not detect a wrong skip.
                if i % 2 == 0 { "exec-even" } else { "exec-odd" },
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "k".repeat(120)),
                Some(2048),
            )
            .await;
        }
        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        assert!(segs.len() >= 2, "need >=2 sealed segments, got {}", segs.len());

        // UNINDEXED answer first — the reference.
        for id in ["exec-even", "exec-odd"] {
            let before = read_execution(Some(&cfg), StoreTier::EventLog, id).await;
            let TierStoreOutcome::Ok(b) = before else { panic!("read failed: {before:?}") };
            let bv: serde_json::Value = serde_json::from_str(&b).unwrap();

            // Now index every sealed segment and read again.
            for sg in &segs {
                build_segment_index(sg).expect("index build");
            }
            let after = read_execution(Some(&cfg), StoreTier::EventLog, id).await;
            let TierStoreOutcome::Ok(a) = after else { panic!("indexed read failed: {after:?}") };
            let av: serde_json::Value = serde_json::from_str(&a).unwrap();

            assert_eq!(
                av["record_count"], bv["record_count"],
                "indexing CHANGED the answer for {id}: {} -> {} — a skipped segment \
                 that holds the execution is silent data loss",
                bv["record_count"], av["record_count"]
            );
            assert!(
                bv["record_count"].as_u64().unwrap() > 0,
                "the fixture returned no records for {id}, so equality is vacuous"
            );
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⭐ The index must actually SKIP something, or it is decorative and the
    /// equality test above would pass against an index nobody consults.
    #[tokio::test]
    async fn streaming_index_equals_replay_index() {
        // The differential that keeps the streaming derivation honest: build the
        // index by streaming, and independently derive the same set from the
        // DRIVER's own `execution_id` (which is what a lookup compares against).
        // If the two ever key differently, a read silently skips a segment that
        // holds the execution — a wrong answer, not a slow one.
        let cfg = seal_cfg("idx-diff");
        for i in 0..40 {
            let exec = if i % 3 == 0 { "exec-alpha" } else { "exec-beta" };
            let _ = append_with_seal(
                Some(&cfg), StoreTier::EventLog, exec,
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "z".repeat(120)), Some(2048),
            ).await;
        }
        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        assert!(segs.len() >= 2, "need several segments, got {}", segs.len());

        let mut checked = 0usize;
        for sg in &segs {
            let n = build_segment_index(sg).expect("index build");
            let streamed = load_segment_index(sg).expect("index present");

            // Independent derivation, via the driver.
            let d = LocalReferenceEventLogDriver::new(
                sg.clone(),
                DEFAULT_LOCAL_REFERENCE_TENANT.to_string(),
                DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string(),
            );
            let out = d
                .scan_global(&EventLogScanRequest { after: None, limit: MAX_SCAN_LIMIT })
                .expect("scan");
            ehdb_reference::forget_runtime(sg);
            let replayed: std::collections::BTreeSet<String> =
                out.records.iter().map(|r| r.execution_id.clone()).collect();

            assert!(!replayed.is_empty(), "fixture segment held no records");
            let streamed_set: std::collections::BTreeSet<String> =
                streamed.iter().cloned().collect();
            assert_eq!(
                streamed_set, replayed,
                "streaming index disagrees with the driver's own execution_id for {}",
                sg.display()
            );
            assert_eq!(n, replayed.len());
            checked += 1;
        }
        assert!(checked >= 2, "compared only {checked} segments");
    }

    #[tokio::test]
    async fn a_line_with_no_subject_refuses_to_write_an_index() {
        // Fail-safe: a partial index read as complete would skip a segment that
        // holds the execution. No index at all means "might contain" -> opened.
        let cfg = seal_cfg("idx-torn");
        for i in 0..24 {
            let _ = append_with_seal(
                Some(&cfg), StoreTier::EventLog, "exec-torn",
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "t".repeat(120)), Some(2048),
            ).await;
        }
        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        let sg = segs.first().expect("a sealed segment").clone();
        // Append a line that carries no subject at all.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(&sg).unwrap();
            writeln!(f, "{{\"sequence\":999,\"mutations\":[]}}").unwrap();
        }
        let err = build_segment_index(&sg).expect_err("must refuse a partial index");
        assert!(
            err.contains("no readable subject"),
            "unexpected error: {err}"
        );
        assert!(
            load_segment_index(&sg).is_none(),
            "a partial index was written anyway"
        );
        // And the segment is therefore still considered for the read.
        let (kept, _) = segments_possibly_holding(&cfg, StoreTier::EventLog, "exec-torn");
        assert!(kept.contains(&sg), "an unindexed segment must still be opened");
    }

    #[tokio::test]
    async fn the_index_actually_rules_segments_out() {
        let cfg = seal_cfg("idx-skip");
        // Fill the early segments with one execution only...
        for i in 0..24 {
            let _ = append_with_seal(
                Some(&cfg), StoreTier::EventLog, "exec-early",
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "k".repeat(120)), Some(2048),
            ).await;
        }
        // ...then a DIFFERENT one, which cannot appear in those early segments.
        for i in 0..24 {
            let _ = append_with_seal(
                Some(&cfg), StoreTier::EventLog, "exec-late",
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "k".repeat(120)), Some(2048),
            ).await;
        }
        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        assert!(segs.len() >= 3, "need several segments, got {}", segs.len());
        for sg in &segs {
            build_segment_index(sg).expect("index build");
        }
        let (kept, total) = segments_possibly_holding(&cfg, StoreTier::EventLog, "exec-late");
        assert_eq!(total, segs.len());
        assert!(
            kept.len() < total,
            "the index ruled out NOTHING ({}/{} kept) — it is not being consulted",
            kept.len(), total
        );
        // And the early execution must still find its own segments.
        let (kept_early, _) = segments_possibly_holding(&cfg, StoreTier::EventLog, "exec-early");
        assert!(!kept_early.is_empty(), "the index ruled out the segments that DO hold exec-early");
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⚠ A MISSING index means "might contain", never "absent".
    #[tokio::test]
    async fn a_segment_without_an_index_is_always_opened() {
        let cfg = seal_cfg("idx-missing");
        for i in 0..24 {
            let _ = append_with_seal(
                Some(&cfg), StoreTier::EventLog, "exec-x",
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "k".repeat(120)), Some(2048),
            ).await;
        }
        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        assert!(!segs.is_empty());
        // No indexes built at all.
        let (kept, total) = segments_possibly_holding(&cfg, StoreTier::EventLog, "anything-at-all");
        assert_eq!(
            kept.len(), total,
            "an unindexed segment was ruled out; absence of an index must mean \
             UNKNOWN, or a read silently skips records that exist"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⭐ The index build must PAGE. `scan_global` is capped at
    /// `MAX_SCAN_LIMIT` (1000), so a build that reads one page indexes only the
    /// first 1000 records — and every execution appearing later is then
    /// silently "not in this segment", which makes reads skip records that
    /// exist.
    ///
    /// ⚠ The other index tests cannot catch this: their segments hold far fewer
    /// than 1000 records, so paging never engages and the fixture cannot exhibit
    /// the failure. This one deliberately builds a segment LARGER than a page.
    #[tokio::test]
    async fn the_index_build_pages_past_the_scan_limit() {
        let cfg = seal_cfg("idx-paging");
        // Seal OFF, so everything lands in ONE segment larger than a page.
        const EARLY: usize = MAX_SCAN_LIMIT + 200;
        for i in 0..EARLY {
            let _ = append_with_seal(
                Some(&cfg), StoreTier::EventLog, "exec-early",
                &format!("{{\"i\":{i}}}"), None,
            ).await;
        }
        // This one appears ONLY after the first page boundary.
        for i in 0..5 {
            let _ = append_with_seal(
                Some(&cfg), StoreTier::EventLog, "exec-past-the-page",
                &format!("{{\"i\":{i}}}"), None,
            ).await;
        }
        // Make it a sealed segment by name, so the index path applies to it.
        let active = cfg.path_for(StoreTier::EventLog);
        let sealed = active.with_file_name("eventlog.jsonl.1");
        std::fs::rename(&active, &sealed).unwrap();
        ehdb_reference::forget_runtime(&active);

        let n = build_segment_index(&sealed).expect("index build");
        let ids = load_segment_index(&sealed).expect("index must exist");
        assert!(
            ids.contains("exec-early"),
            "the index lost the execution on the FIRST page ({n} ids)"
        );
        assert!(
            ids.contains("exec-past-the-page"),
            "the index stopped at the first page: {} records were written but only \
             {n} execution ids indexed, so everything past record {} is invisible \
             and reads would skip it",
            EARLY + 5, MAX_SCAN_LIMIT
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// Fail-safe: the seal is OFF unless explicitly and usably configured.
    #[test]
    fn the_seal_is_off_unless_explicitly_configured() {
        for raw in [None, Some(""), Some("0"), Some("abc"), Some("-1")] {
            assert_eq!(
                parse_seal_max_bytes(raw),
                None,
                "{raw:?} must leave the seal OFF — a typo must not start rotating a \
                 primary-serving tier's store behind the operator's back"
            );
        }
        assert_eq!(parse_seal_max_bytes(Some("1024")), Some(1024));
        assert_eq!(parse_seal_max_bytes(Some(" 1024 ")), Some(1024));
    }

    /// A store that has never sealed looks exactly like today's.
    #[test]
    fn an_unsealed_store_has_no_segments() {
        let cfg = seal_cfg("none");
        assert!(
            sealed_segments(&cfg, StoreTier::EventLog).is_empty(),
            "a fresh store must report no sealed segments"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⚠ Only all-digit suffixes are segments. A stray `eventlog.jsonl.bak`
    /// must never be replayed as if it were tier data.
    #[test]
    fn only_numbered_suffixes_count_as_segments() {
        let cfg = seal_cfg("suffix");
        let active = cfg.path_for(StoreTier::EventLog);
        std::fs::write(&active, b"").unwrap();
        for bad in ["bak", "tmp", "1a", ""] {
            std::fs::write(active.with_file_name(format!("eventlog.jsonl.{bad}")), b"").unwrap();
        }
        std::fs::write(active.with_file_name("eventlog.jsonl.1"), b"").unwrap();
        std::fs::write(active.with_file_name("eventlog.jsonl.2"), b"").unwrap();
        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        assert_eq!(
            segs.len(),
            2,
            "expected exactly the two numbered segments, got {segs:?}"
        );
        // Oldest first — the read path depends on this order.
        assert!(segs[0].to_string_lossy().ends_with(".1"));
        assert!(segs[1].to_string_lossy().ends_with(".2"));
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⭐⭐ THE ONE THAT MATTERS. Sealing must bound the ACTIVE segment while
    /// losing **nothing** — every record written before a seal must still read
    /// back, through BOTH readers.
    ///
    /// A seal that bounded memory by dropping records would be strictly worse
    /// than the OOM it cures, so this asserts counts AND payloads, and asserts
    /// the read actually spanned segments (otherwise it proves nothing about
    /// the merge).
    ///
    /// ⚠ The threshold is INJECTED, never `set_var`. An earlier version set the
    /// process env and armed sealing inside an unrelated sibling test, which is
    /// how the missing `scan` merge was found — but it is not a technique to
    /// keep.
    #[tokio::test]
    async fn sealing_bounds_the_active_segment_and_loses_no_records() {
        let cfg = seal_cfg("roundtrip");
        const N: usize = 40;
        const MAX: u64 = 2048;
        for i in 0..N {
            let out = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                "exec-seal",
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "y".repeat(120)),
                Some(MAX),
            )
            .await;
            assert!(matches!(out, TierStoreOutcome::Ok(_)), "append {i} failed: {out:?}");
        }

        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        assert!(
            !segs.is_empty(),
            "the fixture never crossed the threshold, so it cannot show sealing works"
        );

        // BOUND: the active segment stayed small.
        let active_len = std::fs::metadata(cfg.path_for(StoreTier::EventLog))
            .map(|m| m.len())
            .unwrap_or(0);
        assert!(
            active_len < MAX * 2,
            "active segment is {active_len} bytes against a {MAX}-byte threshold — \
             sealing did not bound it"
        );

        // NO LOSS, reader 1: read_execution.
        let got = read_execution(Some(&cfg), StoreTier::EventLog, "exec-seal").await;
        let TierStoreOutcome::Ok(body) = got else { panic!("read failed: {got:?}") };
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            v["record_count"].as_u64().unwrap() as usize, N,
            "read_execution LOST records: {} of {N} (segments={})", v["record_count"], v["segments"]
        );
        assert!(v["segments"].as_u64().unwrap() > 1, "the read did not span segments");

        // ⚠ Sequences must be renumbered over the MERGE. Each segment's driver
        // numbers from 1 within its own file, so without renumbering the merged
        // reply contains several records all claiming to be number 1 — a total
        // order that is not one. (The battery caught that this was unasserted.)
        let seqs: Vec<u64> = v["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["global_sequence"].as_u64().unwrap_or(0))
            .collect();
        let expected: Vec<u64> = (1..=N as u64).collect();
        assert_eq!(
            seqs, expected,
            "merged global_sequence must be 1..N with no duplicates; got {seqs:?}"
        );

        // Payloads, not just the count — a merge returning N copies of one
        // record satisfies a count assertion.
        let recs = v["records"].as_array().unwrap();
        for i in 0..N {
            let needle = format!("\\\"i\\\":{i},");
            assert!(
                recs.iter().any(|r| r.to_string().contains(&needle)),
                "record {i} missing from the merged read_execution"
            );
        }

        // NO LOSS, reader 2: scan. This is the one the first cut forgot.
        let sc = scan(Some(&cfg), StoreTier::EventLog, None, MAX_SCAN_LIMIT).await;
        let TierStoreOutcome::Ok(sbody) = sc else { panic!("scan failed: {sc:?}") };
        let sv: serde_json::Value = serde_json::from_str(&sbody).unwrap();
        assert_eq!(
            sv["record_count"].as_u64().unwrap() as usize, N,
            "scan LOST records: {} of {N} — a scan that returns only the active \
             segment is silent data loss", sv["record_count"]
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }



    /// Every pre-#265 test addresses the event log, which is also the tier the
    /// wire default resolves to — so these keep asserting exactly what they
    /// asserted before the tier argument existed.
    const EL: StoreTier = StoreTier::EventLog;
    const PROJ: StoreTier = StoreTier::Projection;

    fn tmp_cfg(name: &str) -> TierStoreConfig {
        let mut d = std::env::temp_dir();
        d.push(format!("ehdb-tier-store-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        TierStoreConfig { dir: d }
    }

    #[tokio::test]
    async fn no_store_configured_is_unavailable_not_empty() {
        // The distinction that matters: a caller must be able to tell "no store
        // here" from "the store is empty".
        assert_eq!(append(None, EL, "e1", "{}").await, TierStoreOutcome::Unavailable);
        assert_eq!(read_execution(None, EL, "e1").await, TierStoreOutcome::Unavailable);
        assert_eq!(scan(None, EL, None, 10).await, TierStoreOutcome::Unavailable);
    }

    #[tokio::test]
    async fn append_then_read_returns_the_same_payload() {
        let cfg = tmp_cfg("roundtrip");
        let payload = r#"{"event_type":"probe","n":1}"#;
        match append(Some(&cfg), EL, "exec-1", payload).await {
            TierStoreOutcome::Ok(_) => {}
            other => panic!("append failed: {other:?}"),
        }
        let body = match read_execution(Some(&cfg), EL, "exec-1").await {
            TierStoreOutcome::Ok(b) => b,
            other => panic!("read failed: {other:?}"),
        };
        assert!(
            body.contains("probe"),
            "read must return the appended payload; got {body}"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn reading_an_absent_execution_is_distinguishable_from_a_hit() {
        // The NEGATIVE control.  Without it, "read returned something" proves
        // nothing — a store that returned the same blob for every key would pass
        // the round-trip test above.
        let cfg = tmp_cfg("absent");
        append(Some(&cfg), EL, "present-1", r#"{"marker":"HIT"}"#).await;

        let hit = match read_execution(Some(&cfg), EL, "present-1").await {
            TierStoreOutcome::Ok(b) => b,
            other => panic!("expected a hit: {other:?}"),
        };
        let miss = match read_execution(Some(&cfg), EL, "definitely-not-there").await {
            TierStoreOutcome::Ok(b) => b,
            other => panic!("a miss must still be Ok with an empty result: {other:?}"),
        };
        assert!(hit.contains("HIT"), "hit must carry the marker: {hit}");
        assert!(
            !miss.contains("HIT"),
            "a miss must NOT return another execution's data: {miss}"
        );
        assert_ne!(hit, miss, "hit and miss must be distinguishable");
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn exists_reflects_whether_records_came_back() {
        // Regression guard for the field that meant the opposite of its name:
        // the driver reports exists:true for an execution holding no records.
        let cfg = tmp_cfg("exists");
        append(Some(&cfg), EL, "has-records", r#"{"n":1}"#).await;

        let hit: serde_json::Value = match read_execution(Some(&cfg), EL, "has-records").await {
            TierStoreOutcome::Ok(b) => serde_json::from_str(&b).unwrap(),
            other => panic!("{other:?}"),
        };
        let miss: serde_json::Value = match read_execution(Some(&cfg), EL, "no-such-execution").await {
            TierStoreOutcome::Ok(b) => serde_json::from_str(&b).unwrap(),
            other => panic!("{other:?}"),
        };
        assert_eq!(hit["exists"], serde_json::Value::Bool(true));
        assert_eq!(hit["record_count"], 1);
        assert_eq!(
            miss["exists"],
            serde_json::Value::Bool(false),
            "a miss must report exists:false — the driver says true, which is why we normalise"
        );
        assert_eq!(miss["record_count"], 0);
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn empty_payload_and_empty_id_are_refused() {
        let cfg = tmp_cfg("invalid");
        match append(Some(&cfg), EL, "e", "").await {
            TierStoreOutcome::Invalid(_) => {}
            other => panic!("empty payload must be Invalid, got {other:?}"),
        }
        match append(Some(&cfg), EL, "  ", "{}").await {
            TierStoreOutcome::Invalid(_) => {}
            other => panic!("empty execution_id must be Invalid, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn scan_limit_is_clamped_not_trusted() {
        let cfg = tmp_cfg("clamp");
        for i in 0..5 {
            append(Some(&cfg), EL, &format!("e{i}"), &format!(r#"{{"i":{i}}}"#)).await;
        }
        // A caller asking for far more than the cap is clamped, and still served.
        // Parsed, not substring-matched: the record payload is JSON-ESCAPED
        // inside the response ("payload":"{\\"i\\":0}"), so a naive
        // `contains("\"i\":0")` fails against correct data — which is exactly
        // how a good store gets blamed for a bad assertion.
        match scan(Some(&cfg), EL, None, usize::MAX).await {
            TierStoreOutcome::Ok(b) => {
                let v: serde_json::Value = serde_json::from_str(&b).expect("scan returns JSON");
                assert_eq!(
                    v["record_count"], 5,
                    "clamped scan still serves every record: {b}"
                );
                assert!(v["records"].as_array().is_some_and(|r| r.len() == 5));
            }
            other => panic!("clamped scan must serve: {other:?}"),
        }
        // A zero/absurd low limit is raised to 1 rather than returning nothing.
        match scan(Some(&cfg), EL, None, 0).await {
            TierStoreOutcome::Ok(_) => {}
            other => panic!("limit 0 must clamp to 1, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    // ---------------------------------------------------------------------
    // #265 A1 — per-tier isolation.
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn tiers_do_not_read_each_others_records() {
        // The property the whole genericisation rests on. If these shared a
        // store, a projection append would land in the log that is serving
        // primary in prod, and the event-log comparator would report it as a
        // record with no authoritative counterpart — divergence caused by the
        // mirror rather than found by it.
        let cfg = tmp_cfg("isolation");
        append(Some(&cfg), EL, "exec-9", r#"{"marker":"EVENTLOG-ONLY"}"#).await;
        append(Some(&cfg), PROJ, "exec-9", r#"{"marker":"PROJECTION-ONLY"}"#).await;

        let el = match read_execution(Some(&cfg), EL, "exec-9").await {
            TierStoreOutcome::Ok(b) => b,
            other => panic!("{other:?}"),
        };
        let proj = match read_execution(Some(&cfg), PROJ, "exec-9").await {
            TierStoreOutcome::Ok(b) => b,
            other => panic!("{other:?}"),
        };
        // POSITIVE control first: each tier must actually hold its own record,
        // or "did not see the other one" is satisfied by an empty store.
        assert!(el.contains("EVENTLOG-ONLY"), "event-log tier lost its record: {el}");
        assert!(proj.contains("PROJECTION-ONLY"), "projection tier lost its record: {proj}");
        assert!(
            !el.contains("PROJECTION-ONLY"),
            "the event-log tier can see projection records: {el}"
        );
        assert!(
            !proj.contains("EVENTLOG-ONLY"),
            "the projection tier can see event-log records: {proj}"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn each_tier_sequences_independently_from_one() {
        // A shared sequence would make the projection tier's global_sequence
        // jump with event-log traffic, and the append-side parity check is
        // exactly `log_record_count == global_sequence`. Sharing would fail it
        // on a healthy store.
        let cfg = tmp_cfg("sequence");
        for i in 0..3 {
            append(Some(&cfg), EL, &format!("e{i}"), r#"{"t":"el"}"#).await;
        }
        let body = match append(Some(&cfg), PROJ, "p0", r#"{"t":"proj"}"#).await {
            TierStoreOutcome::Ok(b) => b,
            other => panic!("{other:?}"),
        };
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            v["global_sequence"], 1,
            "the projection tier's first append must be sequence 1, not 4: {body}"
        );
        assert_eq!(v["log_record_count"], 1, "{body}");
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn concurrent_appends_to_one_tier_stay_readable() {
        // Reproduces the shape of the #257 P0 (torn appends under concurrent
        // writers) on the NEW tier, so the projection store inherits the fix
        // rather than being assumed to. Before the serialising lock this test
        // fails on read-back, not on append.
        let cfg = tmp_cfg("concurrent-proj");
        let n = 24;
        let mut set = tokio::task::JoinSet::new();
        for i in 0..n {
            let c = cfg.clone();
            set.spawn(async move {
                append(Some(&c), PROJ, &format!("exec-{i}"), &format!(r#"{{"i":{i}}}"#)).await
            });
        }
        let mut ok = 0;
        while let Some(r) = set.join_next().await {
            if matches!(r.expect("task panicked"), TierStoreOutcome::Ok(_)) {
                ok += 1;
            }
        }
        assert_eq!(ok, n, "every concurrent append must succeed");
        // The real assertion is the READ: one torn line makes the replay fail
        // on every subsequent operation, so a store that tore is unreadable.
        match scan(Some(&cfg), PROJ, None, MAX_SCAN_LIMIT).await {
            TierStoreOutcome::Ok(b) => {
                let v: serde_json::Value = serde_json::from_str(&b).unwrap();
                assert_eq!(v["record_count"], n, "records lost or torn: {b}");
            }
            other => panic!("store unreadable after concurrent appends: {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn store_bytes_and_startup_sequence_are_per_tier() {
        // These two feed the gauges an operator reads before a flip ("does this
        // store hold anything"). Reporting the event log's size under the
        // projection tier would answer that question about the wrong store.
        let cfg = tmp_cfg("gauges");
        for i in 0..4 {
            append(Some(&cfg), EL, &format!("e{i}"), r#"{"t":"el"}"#).await;
        }
        assert_eq!(
            startup_sequence(&cfg, PROJ),
            0,
            "an untouched projection store must report 0, not the event log's tip"
        );
        assert_eq!(store_bytes(&cfg, PROJ), 0);
        assert_eq!(startup_sequence(&cfg, EL), 4);
        assert!(store_bytes(&cfg, EL) > 0);

        append(Some(&cfg), PROJ, "p0", r#"{"t":"proj"}"#).await;
        assert_eq!(startup_sequence(&cfg, PROJ), 1);
        assert_eq!(
            startup_sequence(&cfg, EL),
            4,
            "a projection append must not move the event log's tip"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }
}
