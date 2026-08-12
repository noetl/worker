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
//! # Scope, deliberately narrow
//!
//! **Event-log tier only**, and only `append` / `read_execution` / `scan`. KV,
//! object, projection and vector reuse this shape in a later PR. The RFC puts
//! `ack` here too; it is held back because `ack` drives segment GC — deleting
//! data on a remote caller's say-so deserves its own PR and its own gate, and
//! bundling it here would put "returns the right bytes" and "removes bytes" in
//! one review.
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

    /// Path of the event-log JSONL this store appends to.
    pub fn eventlog_path(&self) -> PathBuf {
        self.dir.join("eventlog.jsonl")
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
fn store_lock(cfg: &TierStoreConfig) -> Arc<RwLock<()>> {
    static LOCKS: OnceLock<StdMutex<HashMap<PathBuf, Arc<RwLock<()>>>>> = OnceLock::new();
    let registry = LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
    // Poison-tolerant: the registry guards a map, not an invariant, and a
    // panic while holding it must not take down the writer.
    let mut guard = registry.lock().unwrap_or_else(|e| e.into_inner());
    guard
        .entry(cfg.eventlog_path())
        .or_insert_with(|| Arc::new(RwLock::new(())))
        .clone()
}

fn driver(cfg: &TierStoreConfig) -> LocalReferenceEventLogDriver {
    LocalReferenceEventLogDriver::new(
        cfg.eventlog_path(),
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
    let lock = store_lock(cfg);
    let _exclusive = lock.write().await;
    append_locked(cfg, execution_id, payload)
}

/// The append itself. Callers reach it only through [`append`], which holds the
/// store's write lock for the whole replay → sequence → write → flush window.
fn append_locked(cfg: &TierStoreConfig, execution_id: &str, payload: &str) -> TierStoreOutcome {
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
    match driver(cfg).append(&request) {
        Ok(out) => {
            // The store's own state, recorded on the write path (ai-meta#260).
            // Observing it here rather than on a read is what makes the tier
            // checkable without generating traffic against it — the question
            // before a `primary` flip is "does this store hold anything", and
            // answering it by reading would change what is being measured.
            super::metrics::record_tier_service_append(out.global_sequence, store_bytes(cfg));
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
pub(crate) fn store_bytes(cfg: &TierStoreConfig) -> u64 {
    std::fs::metadata(cfg.eventlog_path())
        .map(|m| m.len())
        .unwrap_or(0)
}

/// Highest global sequence the store already holds, read once at startup.
///
/// Without this, a writer that restarts in front of a populated store reports
/// `sequence 0` until the next append — which reads exactly like an empty store,
/// on the component being promoted to authoritative.
pub(crate) fn startup_sequence(cfg: &TierStoreConfig) -> u64 {
    match driver(cfg).scan_global(&EventLogScanRequest {
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
    execution_id: &str,
) -> TierStoreOutcome {
    let Some(cfg) = cfg else {
        return TierStoreOutcome::Unavailable;
    };
    if execution_id.trim().is_empty() {
        return TierStoreOutcome::Invalid("execution_id is empty".to_string());
    }
    let lock = store_lock(cfg);
    let _shared = lock.read().await;
    read_execution_locked(cfg, execution_id)
}

fn read_execution_locked(cfg: &TierStoreConfig, execution_id: &str) -> TierStoreOutcome {
    let request = EventLogReadExecutionRequest {
        execution_id: execution_id.to_string(),
        after: None,
        limit: MAX_SCAN_LIMIT,
    };
    match driver(cfg).read_execution(&request) {
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
    after: Option<u64>,
    limit: usize,
) -> TierStoreOutcome {
    let Some(cfg) = cfg else {
        return TierStoreOutcome::Unavailable;
    };
    let lock = store_lock(cfg);
    let _shared = lock.read().await;
    scan_locked(cfg, after, limit)
}

fn scan_locked(cfg: &TierStoreConfig, after: Option<u64>, limit: usize) -> TierStoreOutcome {
    let limit = limit.clamp(1, MAX_SCAN_LIMIT);
    match driver(cfg).scan_global(&EventLogScanRequest { after, limit }) {
        Ok(out) => TierStoreOutcome::Ok(
            serde_json::to_string(&out).unwrap_or_else(|_| "{}".to_string()),
        ),
        Err(e) => TierStoreOutcome::Error(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(append(None, "e1", "{}").await, TierStoreOutcome::Unavailable);
        assert_eq!(read_execution(None, "e1").await, TierStoreOutcome::Unavailable);
        assert_eq!(scan(None, None, 10).await, TierStoreOutcome::Unavailable);
    }

    #[tokio::test]
    async fn append_then_read_returns_the_same_payload() {
        let cfg = tmp_cfg("roundtrip");
        let payload = r#"{"event_type":"probe","n":1}"#;
        match append(Some(&cfg), "exec-1", payload).await {
            TierStoreOutcome::Ok(_) => {}
            other => panic!("append failed: {other:?}"),
        }
        let body = match read_execution(Some(&cfg), "exec-1").await {
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
        append(Some(&cfg), "present-1", r#"{"marker":"HIT"}"#).await;

        let hit = match read_execution(Some(&cfg), "present-1").await {
            TierStoreOutcome::Ok(b) => b,
            other => panic!("expected a hit: {other:?}"),
        };
        let miss = match read_execution(Some(&cfg), "definitely-not-there").await {
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
        append(Some(&cfg), "has-records", r#"{"n":1}"#).await;

        let hit: serde_json::Value = match read_execution(Some(&cfg), "has-records").await {
            TierStoreOutcome::Ok(b) => serde_json::from_str(&b).unwrap(),
            other => panic!("{other:?}"),
        };
        let miss: serde_json::Value = match read_execution(Some(&cfg), "no-such-execution").await {
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
        match append(Some(&cfg), "e", "").await {
            TierStoreOutcome::Invalid(_) => {}
            other => panic!("empty payload must be Invalid, got {other:?}"),
        }
        match append(Some(&cfg), "  ", "{}").await {
            TierStoreOutcome::Invalid(_) => {}
            other => panic!("empty execution_id must be Invalid, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn scan_limit_is_clamped_not_trusted() {
        let cfg = tmp_cfg("clamp");
        for i in 0..5 {
            append(Some(&cfg), &format!("e{i}"), &format!(r#"{{"i":{i}}}"#)).await;
        }
        // A caller asking for far more than the cap is clamped, and still served.
        // Parsed, not substring-matched: the record payload is JSON-ESCAPED
        // inside the response ("payload":"{\\"i\\":0}"), so a naive
        // `contains("\"i\":0")` fails against correct data — which is exactly
        // how a good store gets blamed for a bad assertion.
        match scan(Some(&cfg), None, usize::MAX).await {
            TierStoreOutcome::Ok(b) => {
                let v: serde_json::Value = serde_json::from_str(&b).expect("scan returns JSON");
                assert_eq!(v["record_count"], 5, "clamped scan still serves every record: {b}");
                assert!(v["records"].as_array().is_some_and(|r| r.len() == 5));
            }
            other => panic!("clamped scan must serve: {other:?}"),
        }
        // A zero/absurd low limit is raised to 1 rather than returning nothing.
        match scan(Some(&cfg), None, 0).await {
            TierStoreOutcome::Ok(_) => {}
            other => panic!("limit 0 must clamp to 1, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }
}
