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
    let lock = store_lock(cfg, tier);
    let _exclusive = lock.write().await;
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
