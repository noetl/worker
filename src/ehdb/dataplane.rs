//! Bounded, stateless worker/playbook/system data-plane ops (EHDB Phase C).
//!
//! `append`/`read` a single domain record on a local-reference stream via the
//! in-process [`ehdb_reference`] helpers.  Every op is:
//!
//! * **Disabled by default** — `Disabled` no-op, records no metric.
//! * **Control-plane guarded** — gateway/api/server are refused before any
//!   runtime is opened.
//! * **Bounded** — payload byte cap (`NOETL_EHDB_DATAPLANE_MAX_PAYLOAD_BYTES`,
//!   default 65536, ceiling 1 MiB) and read-limit cap
//!   (`NOETL_EHDB_DATAPLANE_MAX_READ_LIMIT`, default 1000).  Over-bound requests
//!   are `Rejected` before the helper runs.
//! * **Stateless** — the runtime is opened + dropped per call.

use std::sync::OnceLock;

use ehdb_reference::{
    append_local_reference_domain_record, read_local_reference_domain_records,
    AppendDomainRecordOutcome, AppendDomainRecordRequest, ReadDomainRecordsOutcome,
    ReadDomainRecordsRequest, DEFAULT_LOCAL_REFERENCE_NAMESPACE, DEFAULT_LOCAL_REFERENCE_TENANT,
};

use super::contract::{contract_from_env, EhdbClientRole, EhdbContract, EHDB_ENABLED_ENV};
use super::guard::assert_data_plane_access_allowed;
use super::{metrics, EnvMap};
use crate::snowflake::SnowflakeGen;

pub const MAX_PAYLOAD_BYTES_ENV: &str = "NOETL_EHDB_DATAPLANE_MAX_PAYLOAD_BYTES";
pub const MAX_READ_LIMIT_ENV: &str = "NOETL_EHDB_DATAPLANE_MAX_READ_LIMIT";
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 65536;
const MAX_PAYLOAD_BYTES_CEILING: usize = 1_048_576;
const DEFAULT_MAX_READ_LIMIT: usize = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataPlaneOperation {
    Append,
    Read,
}

impl DataPlaneOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            DataPlaneOperation::Append => "append",
            DataPlaneOperation::Read => "read",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataPlaneOutcome {
    Disabled,
    Appended,
    Read,
    Absent,
    Rejected,
    Unavailable,
    GuardRefused,
    Invalid,
}

impl DataPlaneOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            DataPlaneOutcome::Disabled => "disabled",
            DataPlaneOutcome::Appended => "appended",
            DataPlaneOutcome::Read => "read",
            DataPlaneOutcome::Absent => "absent",
            DataPlaneOutcome::Rejected => "rejected",
            DataPlaneOutcome::Unavailable => "unavailable",
            DataPlaneOutcome::GuardRefused => "guard_refused",
            DataPlaneOutcome::Invalid => "invalid",
        }
    }

    pub fn ok(&self) -> bool {
        matches!(
            self,
            DataPlaneOutcome::Disabled
                | DataPlaneOutcome::Appended
                | DataPlaneOutcome::Read
                | DataPlaneOutcome::Absent
        )
    }

    fn degraded(&self) -> bool {
        matches!(self, DataPlaneOutcome::Unavailable)
    }
}

/// Structured, secret-free result of a bounded data-plane op.
#[derive(Debug, Clone)]
pub struct DataPlaneResult {
    pub operation: DataPlaneOperation,
    pub outcome: DataPlaneOutcome,
    pub role: Option<EhdbClientRole>,
    pub duration_seconds: f64,
    pub detail: Option<String>,
    pub append: Option<AppendDomainRecordOutcome>,
    pub read: Option<ReadDomainRecordsOutcome>,
}

/// Optional overrides for a data-plane op.
#[derive(Debug, Clone, Default)]
pub struct DataPlaneOptions {
    pub tenant: Option<String>,
    pub namespace: Option<String>,
    pub transaction_id: Option<String>,
}

fn txn_gen() -> &'static SnowflakeGen {
    static GEN: OnceLock<SnowflakeGen> = OnceLock::new();
    GEN.get_or_init(|| SnowflakeGen::from_env_or_hint("ehdb"))
}

fn new_transaction_id() -> String {
    format!("ehdbtxn-{}", txn_gen().next_id())
}

fn truthy(env: &EnvMap, key: &str) -> bool {
    matches!(
        env.get(key)
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref(),
        Some("1" | "true" | "yes" | "y" | "on")
    )
}

fn bounded_max_payload_bytes(env: &EnvMap) -> usize {
    let value = env
        .get(MAX_PAYLOAD_BYTES_ENV)
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_PAYLOAD_BYTES);
    value.clamp(1, MAX_PAYLOAD_BYTES_CEILING)
}

fn bounded_read_limit(env: &EnvMap, requested: Option<usize>) -> usize {
    let ceiling = env
        .get(MAX_READ_LIMIT_ENV)
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v >= 1)
        .unwrap_or(DEFAULT_MAX_READ_LIMIT);
    match requested {
        Some(n) => n.clamp(1, ceiling),
        None => ceiling,
    }
}

/// Resolve the contract for a data-plane op.  Returns `Ok(contract)` for a
/// data-plane role, or `Err(result)` carrying the early outcome
/// (disabled/guard_refused/invalid) already classified + metered.
fn resolve_contract(
    env: &EnvMap,
    operation: DataPlaneOperation,
    started: std::time::Instant,
    record_metrics: bool,
) -> Result<EhdbContract, Box<DataPlaneResult>> {
    // The early-exit result is boxed: it is large (carries the crate outcome
    // structs) and this is the cold error path (clippy::result_large_err).
    let finish =
        |outcome: DataPlaneOutcome, role: Option<EhdbClientRole>, detail: Option<String>| {
            Box::new(make_result(
                operation,
                outcome,
                role,
                started,
                detail,
                None,
                None,
                record_metrics,
            ))
        };

    let contract = match contract_from_env(env) {
        Ok(c) => c,
        Err(err) => {
            let role = super::contract::safe_client_role(env);
            let outcome = if role.map(|r| r.is_control_plane()).unwrap_or(false) {
                DataPlaneOutcome::GuardRefused
            } else {
                DataPlaneOutcome::Invalid
            };
            return Err(finish(outcome, role, Some(err.0)));
        }
    };

    if let Err(err) = assert_data_plane_access_allowed(contract.role, operation.as_str()) {
        return Err(finish(
            DataPlaneOutcome::GuardRefused,
            Some(contract.role),
            Some(err.to_string()),
        ));
    }
    if !contract.uses_local_reference_runtime() || contract.local_reference_log.is_none() {
        return Err(finish(
            DataPlaneOutcome::Disabled,
            Some(contract.role),
            None,
        ));
    }
    Ok(contract)
}

#[allow(clippy::too_many_arguments)]
fn make_result(
    operation: DataPlaneOperation,
    outcome: DataPlaneOutcome,
    role: Option<EhdbClientRole>,
    started: std::time::Instant,
    detail: Option<String>,
    append: Option<AppendDomainRecordOutcome>,
    read: Option<ReadDomainRecordsOutcome>,
    record_metrics: bool,
) -> DataPlaneResult {
    let duration_seconds = started.elapsed().as_secs_f64();
    if record_metrics {
        metrics::record_dataplane(
            operation.as_str(),
            outcome.as_str(),
            outcome.ok(),
            outcome.degraded(),
            duration_seconds,
        );
    }
    DataPlaneResult {
        operation,
        outcome,
        role,
        duration_seconds,
        detail,
        append,
        read,
    }
}

/// Append one bounded domain record.  Disabled ⇒ `Disabled` no-op.
pub fn append_domain_record(
    env: &EnvMap,
    stream: &str,
    subject: &str,
    payload: &str,
    opts: &DataPlaneOptions,
    record_metrics: bool,
) -> DataPlaneResult {
    let op = DataPlaneOperation::Append;
    let started = std::time::Instant::now();

    if !truthy(env, EHDB_ENABLED_ENV) {
        return make_result(
            op,
            DataPlaneOutcome::Disabled,
            None,
            started,
            None,
            None,
            None,
            record_metrics,
        );
    }
    let contract = match resolve_contract(env, op, started, record_metrics) {
        Ok(c) => c,
        Err(result) => return *result,
    };

    let max_bytes = bounded_max_payload_bytes(env);
    let payload_bytes = payload.len();
    if payload_bytes == 0 {
        return make_result(
            op,
            DataPlaneOutcome::Rejected,
            Some(contract.role),
            started,
            Some("empty domain-record payload".to_string()),
            None,
            None,
            record_metrics,
        );
    }
    if payload_bytes > max_bytes {
        return make_result(
            op,
            DataPlaneOutcome::Rejected,
            Some(contract.role),
            started,
            Some(format!(
                "payload {payload_bytes} bytes exceeds bound {max_bytes}"
            )),
            None,
            None,
            record_metrics,
        );
    }

    let request = AppendDomainRecordRequest {
        log_path: contract.local_reference_log.clone().expect("log present"),
        tenant: opts
            .tenant
            .clone()
            .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string()),
        namespace: opts
            .namespace
            .clone()
            .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string()),
        stream: stream.to_string(),
        subject: subject.to_string(),
        transaction_id: opts
            .transaction_id
            .clone()
            .unwrap_or_else(new_transaction_id),
        payload: payload.to_string(),
    };

    match append_local_reference_domain_record(request) {
        Ok(outcome) => make_result(
            op,
            DataPlaneOutcome::Appended,
            Some(contract.role),
            started,
            None,
            Some(outcome),
            None,
            record_metrics,
        ),
        Err(err) => make_result(
            op,
            classify_helper_error(&err),
            Some(contract.role),
            started,
            Some(err.to_string()),
            None,
            None,
            record_metrics,
        ),
    }
}

/// Read up to `limit` bounded domain records.  A never-written stream returns
/// `Absent`.  Disabled ⇒ `Disabled` no-op.
pub fn read_domain_records(
    env: &EnvMap,
    stream: &str,
    after: Option<u64>,
    limit: Option<usize>,
    opts: &DataPlaneOptions,
    record_metrics: bool,
) -> DataPlaneResult {
    let op = DataPlaneOperation::Read;
    let started = std::time::Instant::now();

    if !truthy(env, EHDB_ENABLED_ENV) {
        return make_result(
            op,
            DataPlaneOutcome::Disabled,
            None,
            started,
            None,
            None,
            None,
            record_metrics,
        );
    }
    let contract = match resolve_contract(env, op, started, record_metrics) {
        Ok(c) => c,
        Err(result) => return *result,
    };

    let request = ReadDomainRecordsRequest {
        log_path: contract.local_reference_log.clone().expect("log present"),
        tenant: opts
            .tenant
            .clone()
            .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string()),
        namespace: opts
            .namespace
            .clone()
            .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string()),
        stream: stream.to_string(),
        after,
        limit: bounded_read_limit(env, limit),
    };

    match read_local_reference_domain_records(request) {
        Ok(outcome) => {
            let dp_outcome = if outcome.exists {
                DataPlaneOutcome::Read
            } else {
                DataPlaneOutcome::Absent
            };
            make_result(
                op,
                dp_outcome,
                Some(contract.role),
                started,
                None,
                None,
                Some(outcome),
                record_metrics,
            )
        }
        Err(err) => make_result(
            op,
            classify_helper_error(&err),
            Some(contract.role),
            started,
            Some(err.to_string()),
            None,
            None,
            record_metrics,
        ),
    }
}

/// A crate-side identifier validation failure is a caller mistake (`Invalid`);
/// any other runtime error is `Unavailable` (degraded).  Classified by the
/// error's Display string (`EhdbError::InvalidIdentifier` renders as
/// `"invalid identifier: …"`) since the crate does not re-export its error enum.
fn classify_helper_error<E: std::fmt::Display>(err: &E) -> DataPlaneOutcome {
    if err.to_string().starts_with("invalid identifier") {
        DataPlaneOutcome::Invalid
    } else {
        DataPlaneOutcome::Unavailable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> EnvMap {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn worker_env(log: &str) -> EnvMap {
        env(&[
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "worker"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", log),
        ])
    }

    fn tmp_log(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-dp-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        (dir.join("log.jsonl"), dir)
    }

    #[test]
    fn disabled_is_noop() {
        let r = append_domain_record(&env(&[]), "s", "sub", "x", &Default::default(), false);
        assert_eq!(r.outcome, DataPlaneOutcome::Disabled);
        assert!(r.append.is_none());
    }

    #[test]
    fn append_then_read_roundtrip() {
        let (log, dir) = tmp_log("rt");
        let e = worker_env(log.to_str().unwrap());
        let a = append_domain_record(
            &e,
            "orders",
            "orders.new",
            "{\"id\":1}",
            &Default::default(),
            false,
        );
        assert_eq!(a.outcome, DataPlaneOutcome::Appended);
        let r = read_domain_records(&e, "orders", None, Some(10), &Default::default(), false);
        assert_eq!(r.outcome, DataPlaneOutcome::Read);
        let read = r.read.unwrap();
        assert_eq!(read.returned, 1);
        assert_eq!(read.records[0].payload, "{\"id\":1}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_absent_stream() {
        let (log, dir) = tmp_log("absent");
        let e = worker_env(log.to_str().unwrap());
        let r = read_domain_records(&e, "never", None, None, &Default::default(), false);
        assert_eq!(r.outcome, DataPlaneOutcome::Absent);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn oversized_payload_rejected() {
        let (log, dir) = tmp_log("big");
        let mut e = worker_env(log.to_str().unwrap());
        e.insert(MAX_PAYLOAD_BYTES_ENV.to_string(), "8".to_string());
        let r = append_domain_record(&e, "s", "s.a", "0123456789", &Default::default(), false);
        assert_eq!(r.outcome, DataPlaneOutcome::Rejected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn gateway_role_guard_refused() {
        let e = env(&[
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "gateway"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
        ]);
        let r = append_domain_record(&e, "s", "s.a", "x", &Default::default(), false);
        assert_eq!(r.outcome, DataPlaneOutcome::GuardRefused);
        assert!(r.append.is_none());
    }
}
