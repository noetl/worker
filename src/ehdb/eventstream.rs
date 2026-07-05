//! Bounded worker/playbook/system event-stream drain (EHDB Phase D).
//!
//! The NoETL event log (`noetl.event` / NATS JetStream) is the append-only
//! source of truth.  EHDB is a **derived, auxiliary** consumer of
//! already-emitted NoETL events: a worker/playbook step `project`s an event
//! payload into a separate local-reference EHDB stream (the `append` primitive),
//! then drains it through a **durable consumer** with explicit
//! `ack`-after-materialize semantics.
//!
//! ## Event-log-authoritative invariant
//!
//! Nothing here writes back to the NoETL event log.  This module only ever calls
//! the [`ehdb_reference`] crate against the separate EHDB JSONL fabric — it has
//! no import of the worker's event emitter / control-plane client and issues no
//! `POST /api/events`.  A unit test (`no_noetl_event_writer`) asserts this
//! structurally over the module source.  "project" mirrors an
//! already-committed event; it never emits one.
//!
//! Bounds mirror Phase C: payload cap
//! (`NOETL_EHDB_EVENTSTREAM_MAX_PAYLOAD_BYTES`), consume batch cap
//! (`NOETL_EHDB_EVENTSTREAM_MAX_CONSUME_LIMIT`), ack sequence ≥ 1.  Disabled by
//! default; control-plane roles refused.

use std::sync::OnceLock;

use ehdb_reference::{
    ack_local_reference_event_consumer, append_local_reference_domain_record,
    consume_local_reference_event_records, AckEventConsumerOutcome, AckEventConsumerRequest,
    AppendDomainRecordOutcome, AppendDomainRecordRequest, ConsumeEventRecordsOutcome,
    ConsumeEventRecordsRequest, DEFAULT_LOCAL_REFERENCE_NAMESPACE, DEFAULT_LOCAL_REFERENCE_TENANT,
};

use super::contract::{contract_from_env, EhdbClientRole, EhdbContract, EHDB_ENABLED_ENV};
use super::guard::assert_data_plane_access_allowed;
use super::{metrics, EnvMap};
use crate::snowflake::SnowflakeGen;

pub const MAX_PAYLOAD_BYTES_ENV: &str = "NOETL_EHDB_EVENTSTREAM_MAX_PAYLOAD_BYTES";
pub const MAX_CONSUME_LIMIT_ENV: &str = "NOETL_EHDB_EVENTSTREAM_MAX_CONSUME_LIMIT";
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 65536;
const MAX_PAYLOAD_BYTES_CEILING: usize = 1_048_576;
const DEFAULT_MAX_CONSUME_LIMIT: usize = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventStreamOperation {
    Project,
    Consume,
    Ack,
}

impl EventStreamOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventStreamOperation::Project => "project",
            EventStreamOperation::Consume => "consume",
            EventStreamOperation::Ack => "ack",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventStreamOutcome {
    Disabled,
    Projected,
    Consumed,
    Absent,
    Acked,
    Rejected,
    Unavailable,
    GuardRefused,
    Invalid,
}

impl EventStreamOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventStreamOutcome::Disabled => "disabled",
            EventStreamOutcome::Projected => "projected",
            EventStreamOutcome::Consumed => "consumed",
            EventStreamOutcome::Absent => "absent",
            EventStreamOutcome::Acked => "acked",
            EventStreamOutcome::Rejected => "rejected",
            EventStreamOutcome::Unavailable => "unavailable",
            EventStreamOutcome::GuardRefused => "guard_refused",
            EventStreamOutcome::Invalid => "invalid",
        }
    }

    pub fn ok(&self) -> bool {
        matches!(
            self,
            EventStreamOutcome::Disabled
                | EventStreamOutcome::Projected
                | EventStreamOutcome::Consumed
                | EventStreamOutcome::Absent
                | EventStreamOutcome::Acked
        )
    }

    fn degraded(&self) -> bool {
        matches!(self, EventStreamOutcome::Unavailable)
    }
}

/// Structured, secret-free result of a bounded event-stream op.
#[derive(Debug, Clone)]
pub struct EventStreamResult {
    pub operation: EventStreamOperation,
    pub outcome: EventStreamOutcome,
    pub role: Option<EhdbClientRole>,
    pub duration_seconds: f64,
    pub detail: Option<String>,
    pub project: Option<AppendDomainRecordOutcome>,
    pub consume: Option<ConsumeEventRecordsOutcome>,
    pub ack: Option<AckEventConsumerOutcome>,
}

#[derive(Debug, Clone, Default)]
pub struct EventStreamOptions {
    pub tenant: Option<String>,
    pub namespace: Option<String>,
    pub transaction_id: Option<String>,
}

fn txn_gen() -> &'static SnowflakeGen {
    static GEN: OnceLock<SnowflakeGen> = OnceLock::new();
    GEN.get_or_init(|| SnowflakeGen::from_env_or_hint("ehdb-es"))
}

fn new_transaction_id() -> String {
    format!("ehdbes-{}", txn_gen().next_id())
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
    env.get(MAX_PAYLOAD_BYTES_ENV)
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_PAYLOAD_BYTES)
        .clamp(1, MAX_PAYLOAD_BYTES_CEILING)
}

fn bounded_consume_limit(env: &EnvMap, requested: Option<usize>) -> usize {
    let ceiling = env
        .get(MAX_CONSUME_LIMIT_ENV)
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v >= 1)
        .unwrap_or(DEFAULT_MAX_CONSUME_LIMIT);
    match requested {
        Some(n) => n.clamp(1, ceiling),
        None => ceiling,
    }
}

fn resolve_contract(
    env: &EnvMap,
    operation: EventStreamOperation,
    started: std::time::Instant,
    record_metrics: bool,
) -> Result<EhdbContract, Box<EventStreamResult>> {
    // Boxed early-exit result — large Err on the cold path
    // (clippy::result_large_err).
    let finish =
        |outcome: EventStreamOutcome, role: Option<EhdbClientRole>, detail: Option<String>| {
            Box::new(make_result(
                operation,
                outcome,
                role,
                started,
                detail,
                record_metrics,
            ))
        };

    let contract = match contract_from_env(env) {
        Ok(c) => c,
        Err(err) => {
            let role = super::contract::safe_client_role(env);
            let outcome = if role.map(|r| r.is_control_plane()).unwrap_or(false) {
                EventStreamOutcome::GuardRefused
            } else {
                EventStreamOutcome::Invalid
            };
            return Err(finish(outcome, role, Some(err.0)));
        }
    };

    if let Err(err) = assert_data_plane_access_allowed(contract.role, operation.as_str()) {
        return Err(finish(
            EventStreamOutcome::GuardRefused,
            Some(contract.role),
            Some(err.to_string()),
        ));
    }
    if !contract.uses_local_reference_runtime() || contract.local_reference_log.is_none() {
        return Err(finish(
            EventStreamOutcome::Disabled,
            Some(contract.role),
            None,
        ));
    }
    Ok(contract)
}

fn make_result(
    operation: EventStreamOperation,
    outcome: EventStreamOutcome,
    role: Option<EhdbClientRole>,
    started: std::time::Instant,
    detail: Option<String>,
    record_metrics: bool,
) -> EventStreamResult {
    let duration_seconds = started.elapsed().as_secs_f64();
    if record_metrics {
        metrics::record_eventstream(
            operation.as_str(),
            outcome.as_str(),
            outcome.ok(),
            outcome.degraded(),
            duration_seconds,
        );
    }
    EventStreamResult {
        operation,
        outcome,
        role,
        duration_seconds,
        detail,
        project: None,
        consume: None,
        ack: None,
    }
}

/// Classified by the error's Display string since the crate does not re-export
/// its error enum: an identifier validation failure is a caller mistake
/// (`Invalid`); any other runtime error is `Unavailable`.
fn classify_helper_error<E: std::fmt::Display>(err: &E) -> EventStreamOutcome {
    if err.to_string().starts_with("invalid identifier") {
        EventStreamOutcome::Invalid
    } else {
        EventStreamOutcome::Unavailable
    }
}

/// Project one already-committed NoETL event payload into a derived EHDB stream.
/// This is the `append` primitive — it never emits a NoETL event.
pub fn project_event(
    env: &EnvMap,
    stream: &str,
    subject: &str,
    payload: &str,
    opts: &EventStreamOptions,
    record_metrics: bool,
) -> EventStreamResult {
    let op = EventStreamOperation::Project;
    let started = std::time::Instant::now();
    if !truthy(env, EHDB_ENABLED_ENV) {
        return make_result(
            op,
            EventStreamOutcome::Disabled,
            None,
            started,
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
            EventStreamOutcome::Rejected,
            Some(contract.role),
            started,
            Some("empty event payload".to_string()),
            record_metrics,
        );
    }
    if payload_bytes > max_bytes {
        return make_result(
            op,
            EventStreamOutcome::Rejected,
            Some(contract.role),
            started,
            Some(format!(
                "payload {payload_bytes} bytes exceeds bound {max_bytes}"
            )),
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
        Ok(outcome) => {
            let mut r = make_result(
                op,
                EventStreamOutcome::Projected,
                Some(contract.role),
                started,
                None,
                record_metrics,
            );
            r.project = Some(outcome);
            r
        }
        Err(err) => make_result(
            op,
            classify_helper_error(&err),
            Some(contract.role),
            started,
            Some(err.to_string()),
            record_metrics,
        ),
    }
}

/// Pull up to `limit` pending records for a durable consumer WITHOUT moving the
/// ack cursor.  A never-projected stream returns `Absent`.
pub fn consume_events(
    env: &EnvMap,
    stream: &str,
    consumer: &str,
    limit: Option<usize>,
    opts: &EventStreamOptions,
    record_metrics: bool,
) -> EventStreamResult {
    let op = EventStreamOperation::Consume;
    let started = std::time::Instant::now();
    if !truthy(env, EHDB_ENABLED_ENV) {
        return make_result(
            op,
            EventStreamOutcome::Disabled,
            None,
            started,
            None,
            record_metrics,
        );
    }
    let contract = match resolve_contract(env, op, started, record_metrics) {
        Ok(c) => c,
        Err(result) => return *result,
    };

    let request = ConsumeEventRecordsRequest {
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
        consumer: consumer.to_string(),
        transaction_id: opts
            .transaction_id
            .clone()
            .unwrap_or_else(new_transaction_id),
        limit: bounded_consume_limit(env, limit),
    };

    match consume_local_reference_event_records(request) {
        Ok(outcome) => {
            let es_outcome = if outcome.exists {
                EventStreamOutcome::Consumed
            } else {
                EventStreamOutcome::Absent
            };
            let mut r = make_result(
                op,
                es_outcome,
                Some(contract.role),
                started,
                None,
                record_metrics,
            );
            r.consume = Some(outcome);
            r
        }
        Err(err) => make_result(
            op,
            classify_helper_error(&err),
            Some(contract.role),
            started,
            Some(err.to_string()),
            record_metrics,
        ),
    }
}

/// Advance a durable consumer's ack cursor to `sequence` after materialize.
/// `sequence` must be ≥ 1 (a real published record).
pub fn ack_events(
    env: &EnvMap,
    stream: &str,
    consumer: &str,
    sequence: u64,
    opts: &EventStreamOptions,
    record_metrics: bool,
) -> EventStreamResult {
    let op = EventStreamOperation::Ack;
    let started = std::time::Instant::now();
    if !truthy(env, EHDB_ENABLED_ENV) {
        return make_result(
            op,
            EventStreamOutcome::Disabled,
            None,
            started,
            None,
            record_metrics,
        );
    }
    let contract = match resolve_contract(env, op, started, record_metrics) {
        Ok(c) => c,
        Err(result) => return *result,
    };

    if sequence < 1 {
        return make_result(
            op,
            EventStreamOutcome::Rejected,
            Some(contract.role),
            started,
            Some("ack sequence must be >= 1".to_string()),
            record_metrics,
        );
    }

    let request = AckEventConsumerRequest {
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
        consumer: consumer.to_string(),
        transaction_id: opts
            .transaction_id
            .clone()
            .unwrap_or_else(new_transaction_id),
        sequence,
    };

    match ack_local_reference_event_consumer(request) {
        Ok(outcome) => {
            let mut r = make_result(
                op,
                EventStreamOutcome::Acked,
                Some(contract.role),
                started,
                None,
                record_metrics,
            );
            r.ack = Some(outcome);
            r
        }
        Err(err) => make_result(
            op,
            classify_helper_error(&err),
            Some(contract.role),
            started,
            Some(err.to_string()),
            record_metrics,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker_env(log: &str) -> EnvMap {
        [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "worker"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", log),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn tmp_log(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-es-{tag}-{}-{:?}",
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
        let e: EnvMap = EnvMap::new();
        let r = project_event(&e, "s", "s.a", "x", &Default::default(), false);
        assert_eq!(r.outcome, EventStreamOutcome::Disabled);
    }

    #[test]
    fn project_consume_ack_cursor() {
        let (log, dir) = tmp_log("drain");
        let e = worker_env(log.to_str().unwrap());
        // Project two events.
        for i in 0..2 {
            let r = project_event(
                &e,
                "events",
                "events.emitted",
                &format!("evt-{i}"),
                &Default::default(),
                false,
            );
            assert_eq!(r.outcome, EventStreamOutcome::Projected);
        }
        // Consume (does not move cursor): 2 pending.
        let c = consume_events(
            &e,
            "events",
            "drain-1",
            Some(10),
            &Default::default(),
            false,
        );
        assert_eq!(c.outcome, EventStreamOutcome::Consumed);
        let consumed = c.consume.unwrap();
        assert_eq!(consumed.pending_count, 2);
        assert_eq!(consumed.returned, 2);
        // Ack the first.
        let a = ack_events(&e, "events", "drain-1", 1, &Default::default(), false);
        assert_eq!(a.outcome, EventStreamOutcome::Acked);
        // Consume again (fresh process-equivalent: cursor persisted in the log).
        let c2 = consume_events(
            &e,
            "events",
            "drain-1",
            Some(10),
            &Default::default(),
            false,
        );
        assert_eq!(c2.consume.unwrap().pending_count, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn consume_absent_stream() {
        let (log, dir) = tmp_log("absent");
        let e = worker_env(log.to_str().unwrap());
        let c = consume_events(&e, "never", "d", None, &Default::default(), false);
        assert_eq!(c.outcome, EventStreamOutcome::Absent);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn gateway_guard_refused() {
        let e: EnvMap = [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "server"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        // server role in local_reference mode is a config error (control-plane
        // role + data-plane env) → classified as guard refusal, no write.
        let r = project_event(&e, "s", "s.a", "x", &Default::default(), false);
        assert_eq!(r.outcome, EventStreamOutcome::GuardRefused);
        assert!(r.project.is_none());
    }

    /// Event-log-authoritative invariant, asserted structurally: this module
    /// must never reach the NoETL event log.  It may only touch the derived
    /// EHDB fabric via `ehdb_reference`.
    #[test]
    fn no_noetl_event_writer() {
        // Scan only the production portion (before the test module) so the
        // forbidden-token literals in this test don't trip the check.
        let full = include_str!("eventstream.rs");
        let src = full.split("#[cfg(test)]").next().unwrap();
        for forbidden in [
            "crate::events",
            "crate::client",
            "/api/events",
            "ExecutorEvent",
            "emit_event",
        ] {
            // Allow the word inside doc comments describing the invariant, but
            // forbid it in a code position (`use`/path).  A conservative check:
            // the tokens above never legitimately appear in this module's code.
            assert!(
                !code_lines(src).contains(forbidden),
                "forbidden NoETL event-writer reference `{forbidden}` in eventstream.rs"
            );
        }
    }

    /// Strip `//`-comment and doc-comment lines so the structural check only
    /// inspects code positions.
    fn code_lines(src: &str) -> String {
        src.lines()
            .filter(|l| {
                let t = l.trim_start();
                !t.starts_with("//") && !t.starts_with("//!") && !t.starts_with("///")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}
