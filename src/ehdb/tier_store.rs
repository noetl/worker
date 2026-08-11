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

use std::path::PathBuf;

use ehdb_reference::{
    EventLogAppendRequest, EventLogDriver, EventLogReadExecutionRequest, EventLogScanRequest,
    LocalReferenceEventLogDriver, DEFAULT_LOCAL_REFERENCE_NAMESPACE,
    DEFAULT_LOCAL_REFERENCE_TENANT,
};

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
pub fn append(cfg: Option<&TierStoreConfig>, execution_id: &str, payload: &str) -> TierStoreOutcome {
    let Some(cfg) = cfg else {
        return TierStoreOutcome::Unavailable;
    };
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
        Ok(out) => TierStoreOutcome::Ok(
            serde_json::to_string(&serde_json::json!({
                "appended": true,
                "global_sequence": out.global_sequence,
            }))
            .unwrap_or_else(|_| "{}".to_string()),
        ),
        Err(e) => TierStoreOutcome::Error(e.to_string()),
    }
}

/// Read every record for one execution.
pub fn read_execution(cfg: Option<&TierStoreConfig>, execution_id: &str) -> TierStoreOutcome {
    let Some(cfg) = cfg else {
        return TierStoreOutcome::Unavailable;
    };
    if execution_id.trim().is_empty() {
        return TierStoreOutcome::Invalid("execution_id is empty".to_string());
    }
    let request = EventLogReadExecutionRequest {
        execution_id: execution_id.to_string(),
        after: None,
        limit: MAX_SCAN_LIMIT,
    };
    match driver(cfg).read_execution(&request) {
        Ok(out) => TierStoreOutcome::Ok(
            serde_json::to_string(&out).unwrap_or_else(|_| "{}".to_string()),
        ),
        Err(e) => TierStoreOutcome::Error(e.to_string()),
    }
}

/// Bounded global scan. `limit` is clamped to [`MAX_SCAN_LIMIT`].
pub fn scan(cfg: Option<&TierStoreConfig>, after: Option<u64>, limit: usize) -> TierStoreOutcome {
    let Some(cfg) = cfg else {
        return TierStoreOutcome::Unavailable;
    };
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

    #[test]
    fn no_store_configured_is_unavailable_not_empty() {
        // The distinction that matters: a caller must be able to tell "no store
        // here" from "the store is empty".
        assert_eq!(append(None, "e1", "{}"), TierStoreOutcome::Unavailable);
        assert_eq!(read_execution(None, "e1"), TierStoreOutcome::Unavailable);
        assert_eq!(scan(None, None, 10), TierStoreOutcome::Unavailable);
    }

    #[test]
    fn append_then_read_returns_the_same_payload() {
        let cfg = tmp_cfg("roundtrip");
        let payload = r#"{"event_type":"probe","n":1}"#;
        match append(Some(&cfg), "exec-1", payload) {
            TierStoreOutcome::Ok(_) => {}
            other => panic!("append failed: {other:?}"),
        }
        let body = match read_execution(Some(&cfg), "exec-1") {
            TierStoreOutcome::Ok(b) => b,
            other => panic!("read failed: {other:?}"),
        };
        assert!(
            body.contains("probe"),
            "read must return the appended payload; got {body}"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[test]
    fn reading_an_absent_execution_is_distinguishable_from_a_hit() {
        // The NEGATIVE control.  Without it, "read returned something" proves
        // nothing — a store that returned the same blob for every key would pass
        // the round-trip test above.
        let cfg = tmp_cfg("absent");
        append(Some(&cfg), "present-1", r#"{"marker":"HIT"}"#);

        let hit = match read_execution(Some(&cfg), "present-1") {
            TierStoreOutcome::Ok(b) => b,
            other => panic!("expected a hit: {other:?}"),
        };
        let miss = match read_execution(Some(&cfg), "definitely-not-there") {
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

    #[test]
    fn empty_payload_and_empty_id_are_refused() {
        let cfg = tmp_cfg("invalid");
        match append(Some(&cfg), "e", "") {
            TierStoreOutcome::Invalid(_) => {}
            other => panic!("empty payload must be Invalid, got {other:?}"),
        }
        match append(Some(&cfg), "  ", "{}") {
            TierStoreOutcome::Invalid(_) => {}
            other => panic!("empty execution_id must be Invalid, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[test]
    fn scan_limit_is_clamped_not_trusted() {
        let cfg = tmp_cfg("clamp");
        for i in 0..5 {
            append(Some(&cfg), &format!("e{i}"), &format!(r#"{{"i":{i}}}"#));
        }
        // A caller asking for far more than the cap is clamped, and still served.
        // Parsed, not substring-matched: the record payload is JSON-ESCAPED
        // inside the response ("payload":"{\\"i\\":0}"), so a naive
        // `contains("\"i\":0")` fails against correct data — which is exactly
        // how a good store gets blamed for a bad assertion.
        match scan(Some(&cfg), None, usize::MAX) {
            TierStoreOutcome::Ok(b) => {
                let v: serde_json::Value = serde_json::from_str(&b).expect("scan returns JSON");
                assert_eq!(v["record_count"], 5, "clamped scan still serves every record: {b}");
                assert!(v["records"].as_array().is_some_and(|r| r.len() == 5));
            }
            other => panic!("clamped scan must serve: {other:?}"),
        }
        // A zero/absurd low limit is raised to 1 rather than returning nothing.
        match scan(Some(&cfg), None, 0) {
            TierStoreOutcome::Ok(_) => {}
            other => panic!("limit 0 must clamp to 1, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }
}
