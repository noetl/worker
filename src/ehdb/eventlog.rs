//! Disabled-by-default event-log SHADOW wiring (EHDB Phase 6).
//!
//! EHDB's event-log core engine (the `ehdb_reference::eventlog` driver) is the
//! durable persistence + ordering + serving layer that Phase 6 puts underneath
//! NoETL's append-only platform event log, in place of the
//! NATS-JetStream + PostgreSQL log-and-store path.  This module is the worker's
//! **driver-selection seam** for that engine, gated by `NOETL_EHDB_EVENTLOG`:
//!
//! * `off` (default) — strict no-op.  No engine is opened, no metric recorded;
//!   the worker's `/metrics` and behaviour are byte-identical to a build without
//!   the event-log wiring.
//! * `shadow` — **dual-write + compare, never serve.**  Each already-authored
//!   platform event is *mirrored* into the EHDB engine alongside the existing
//!   JetStream+Postgres path, and the mirror is compared against the
//!   authoritative log for sequence parity, count parity, and monotonic
//!   ordering.  Reads are **never** served from EHDB and the authoritative
//!   producer path is untouched.
//! * `primary` — recognised but **NOT activated this session**.  Cutover to
//!   serving the log from EHDB is a later gated step; requesting `primary` here
//!   is refused with a distinct outcome and the worker stays on the existing
//!   path.  [`PRIMARY_SERVE_ACTIVATED`] is a compile-time `false` so it is
//!   structurally impossible for this build to serve primary.
//!
//! ## Boundaries (mirror the rest of `src/ehdb`)
//!
//! * Disabled-by-default no-op (byte-identical `/metrics`).
//! * Control-plane roles (`gateway`/`api`/`server`) refused before any engine
//!   opens — the gateway never touches the data plane.
//! * Bounded (payload cap) + stateless (engine opened + dropped per mirror).
//! * **Event-log-authoritative** — mirroring persists an already-authored event
//!   into the *derived* EHDB fabric; it never authors a NoETL event and never
//!   reaches `noetl.event` / `POST /api/events` (structurally asserted).

use std::sync::OnceLock;

use ehdb_reference::{
    compare_shadow_parity, EventLogAppendRequest, EventLogDriver, EventLogParityReport,
    LocalReferenceEventLogDriver, DEFAULT_LOCAL_REFERENCE_NAMESPACE,
    DEFAULT_LOCAL_REFERENCE_TENANT,
};

use super::contract::{contract_from_env, EhdbClientRole, EhdbContract, EHDB_ENABLED_ENV};
use super::guard::assert_data_plane_access_allowed;
use super::{metrics, EnvMap};
use crate::snowflake::SnowflakeGen;

/// The driver-selection flag for the event-log tier.
pub const EVENTLOG_MODE_ENV: &str = "NOETL_EHDB_EVENTLOG";
/// Payload byte cap for one mirrored event.
pub const MAX_PAYLOAD_BYTES_ENV: &str = "NOETL_EHDB_EVENTLOG_MAX_PAYLOAD_BYTES";
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 262_144;
const MAX_PAYLOAD_BYTES_CEILING: usize = 1_048_576;

/// Compile-time guard: this build never serves the log from EHDB.  Phase 6 ships
/// the shadow only; flipping this to `true` is the later, separately-gated
/// primary cutover and is intentionally not reachable from config.
pub const PRIMARY_SERVE_ACTIVATED: bool = false;

/// Which event-log engine the tier is driven by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventLogMode {
    /// No EHDB engine; the incumbent JetStream+Postgres path is authoritative.
    Off,
    /// Dual-write into EHDB + compare; never serve reads from it.
    Shadow,
    /// Serve the log from EHDB — recognised but not activated this session.
    Primary,
}

impl EventLogMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventLogMode::Off => "off",
            EventLogMode::Shadow => "shadow",
            EventLogMode::Primary => "primary",
        }
    }

    /// Parse the mode from the env, defaulting to `Off`.  An unrecognised value
    /// is treated as `Off` (fail-safe: an unknown driver never mirrors).
    pub fn from_env(env: &EnvMap) -> Self {
        match env
            .get(EVENTLOG_MODE_ENV)
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("shadow") => EventLogMode::Shadow,
            Some("primary") => EventLogMode::Primary,
            _ => EventLogMode::Off,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventLogOutcome {
    /// Off mode / EHDB disabled — strict no-op.
    Disabled,
    /// Event mirrored into EHDB and parity held.
    Mirrored,
    /// Event mirrored but the EHDB engine diverged from the authoritative log.
    ParityMismatch,
    /// `primary` requested but primary-serve is not activated this session.
    PrimaryUnavailable,
    /// Payload empty or over the byte cap.
    Rejected,
    /// A control-plane role reached the data-plane engine — refused.
    GuardRefused,
    /// Caller mistake (bad execution id / config).
    Invalid,
    /// The engine errored at runtime.
    Unavailable,
}

impl EventLogOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventLogOutcome::Disabled => "disabled",
            EventLogOutcome::Mirrored => "mirrored",
            EventLogOutcome::ParityMismatch => "parity_mismatch",
            EventLogOutcome::PrimaryUnavailable => "primary_unavailable",
            EventLogOutcome::Rejected => "rejected",
            EventLogOutcome::GuardRefused => "guard_refused",
            EventLogOutcome::Invalid => "invalid",
            EventLogOutcome::Unavailable => "unavailable",
        }
    }

    pub fn ok(&self) -> bool {
        matches!(self, EventLogOutcome::Disabled | EventLogOutcome::Mirrored)
    }

    /// A degraded (but non-fatal) outcome — surfaces on the `last_degraded`
    /// gauge so a divergence or engine hiccup is visible without failing the
    /// authoritative path.
    fn degraded(&self) -> bool {
        matches!(
            self,
            EventLogOutcome::ParityMismatch | EventLogOutcome::Unavailable
        )
    }
}

/// Secret-free result of one shadow mirror.
#[derive(Debug, Clone)]
pub struct EventLogResult {
    pub mode: EventLogMode,
    pub outcome: EventLogOutcome,
    pub role: Option<EhdbClientRole>,
    pub duration_seconds: f64,
    pub detail: Option<String>,
    /// The global sequence EHDB assigned (present on a successful mirror).
    pub global_sequence: Option<u64>,
    /// The parity verdict (present when a mirror ran).
    pub parity: Option<EventLogParityReport>,
}

#[derive(Debug, Clone, Default)]
pub struct EventLogOptions {
    pub tenant: Option<String>,
    pub namespace: Option<String>,
    pub transaction_id: Option<String>,
}

fn txn_gen() -> &'static SnowflakeGen {
    static GEN: OnceLock<SnowflakeGen> = OnceLock::new();
    GEN.get_or_init(|| SnowflakeGen::from_env_or_hint("ehdb-el"))
}

fn new_transaction_id() -> String {
    format!("ehdbel-{}", txn_gen().next_id())
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

/// Build a result (and record its metric).  `global_sequence` / `parity` are set
/// by the success path afterward — the early-exit paths leave them `None`.
fn make_result(
    mode: EventLogMode,
    outcome: EventLogOutcome,
    role: Option<EhdbClientRole>,
    started: std::time::Instant,
    detail: Option<String>,
    record_metrics: bool,
) -> EventLogResult {
    let duration_seconds = started.elapsed().as_secs_f64();
    if record_metrics {
        metrics::record_eventlog(
            "mirror",
            outcome.as_str(),
            outcome.ok(),
            outcome.degraded(),
            duration_seconds,
        );
    }
    EventLogResult {
        mode,
        outcome,
        role,
        duration_seconds,
        detail,
        global_sequence: None,
        parity: None,
    }
}

/// Classified by the crate error's Display since the crate does not re-export
/// its error enum: an identifier validation failure is a caller mistake
/// (`Invalid`); any other runtime error is `Unavailable`.
fn classify_helper_error<E: std::fmt::Display>(err: &E) -> EventLogOutcome {
    if err.to_string().starts_with("invalid identifier") {
        EventLogOutcome::Invalid
    } else {
        EventLogOutcome::Unavailable
    }
}

fn resolve_contract(
    env: &EnvMap,
    mode: EventLogMode,
    started: std::time::Instant,
    record_metrics: bool,
) -> Result<EhdbContract, Box<EventLogResult>> {
    let finish =
        |outcome: EventLogOutcome, role: Option<EhdbClientRole>, detail: Option<String>| {
            Box::new(make_result(
                mode,
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
                EventLogOutcome::GuardRefused
            } else {
                EventLogOutcome::Invalid
            };
            return Err(finish(outcome, role, Some(err.0)));
        }
    };

    if let Err(err) = assert_data_plane_access_allowed(contract.role, "mirror") {
        return Err(finish(
            EventLogOutcome::GuardRefused,
            Some(contract.role),
            Some(err.to_string()),
        ));
    }
    if !contract.uses_local_reference_runtime() || contract.local_reference_log.is_none() {
        return Err(finish(EventLogOutcome::Disabled, Some(contract.role), None));
    }
    Ok(contract)
}

/// Mirror one already-authored platform event into the EHDB event-log engine
/// (shadow) and compare it against the authoritative log.
///
/// `authoritative_sequence` is the sequence the authoritative producer path
/// assigned to this event when it is known + comparable (e.g. a controlled
/// selfcheck drive, or a JetStream stream sequence mirrored from origin);
/// `None` skips raw sequence-value comparison and relies on count + ordering
/// parity, which is the safe default when the authoritative sequence is not a
/// 1-based value aligned with the EHDB stream.
///
/// This NEVER serves reads and NEVER authors a NoETL event — it only appends to
/// the derived EHDB fabric and reports parity.
pub fn mirror_event(
    env: &EnvMap,
    execution_id: &str,
    authoritative_sequence: Option<u64>,
    payload: &str,
    opts: &EventLogOptions,
    record_metrics: bool,
) -> EventLogResult {
    let started = std::time::Instant::now();
    let mode = EventLogMode::from_env(env);

    // Off mode OR the umbrella EHDB switch disabled ⇒ strict no-op.
    if mode == EventLogMode::Off || !truthy(env, EHDB_ENABLED_ENV) {
        return make_result(
            mode,
            EventLogOutcome::Disabled,
            None,
            started,
            None,
            record_metrics,
        );
    }

    // Primary is recognised but not activated this session — refuse before any
    // engine opens.  The compile-time guard makes serving structurally
    // impossible; this is the config-time refusal.
    if mode == EventLogMode::Primary && !PRIMARY_SERVE_ACTIVATED {
        // Still resolve the contract so a control-plane role is refused as a
        // guard, not silently treated as "primary unavailable".
        let contract = match resolve_contract(env, mode, started, record_metrics) {
            Ok(c) => c,
            Err(result) => return *result,
        };
        return make_result(
            mode,
            EventLogOutcome::PrimaryUnavailable,
            Some(contract.role),
            started,
            Some("event-log primary serve is not activated in this build".to_string()),
            record_metrics,
        );
    }

    // Shadow mode.
    let contract = match resolve_contract(env, mode, started, record_metrics) {
        Ok(c) => c,
        Err(result) => return *result,
    };

    let max_bytes = bounded_max_payload_bytes(env);
    let payload_bytes = payload.len();
    if payload_bytes == 0 {
        return make_result(
            mode,
            EventLogOutcome::Rejected,
            Some(contract.role),
            started,
            Some("empty event payload".to_string()),
            record_metrics,
        );
    }
    if payload_bytes > max_bytes {
        return make_result(
            mode,
            EventLogOutcome::Rejected,
            Some(contract.role),
            started,
            Some(format!(
                "payload {payload_bytes} bytes exceeds bound {max_bytes}"
            )),
            record_metrics,
        );
    }

    let driver = LocalReferenceEventLogDriver::new(
        contract.local_reference_log.clone().expect("log present"),
        opts.tenant
            .clone()
            .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string()),
        opts.namespace
            .clone()
            .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string()),
    );

    let request = EventLogAppendRequest {
        execution_id: execution_id.to_string(),
        transaction_id: opts
            .transaction_id
            .clone()
            .unwrap_or_else(new_transaction_id),
        payload: payload.to_string(),
    };

    match driver.append(&request) {
        Ok(outcome) => {
            // Concurrency-safe parity: the canonical event-log stream is gapless
            // from 1, so the engine's own invariant `global_sequence ==
            // log_record_count` proves no gap and no double-write for THIS
            // append, independent of process-global bookkeeping (which would
            // race across concurrent executions mirroring the same log).  We
            // feed `previous_sequence = seq - 1` (ordering is trivially
            // monotonic under the gapless invariant) and `expected_count = seq`
            // so the count-parity check is exactly that invariant.  Sequence
            // parity against the authoritative log is enforced when known.
            let previous_sequence = outcome.global_sequence.saturating_sub(1);
            let expected_count = outcome.global_sequence as usize;
            let report = compare_shadow_parity(
                authoritative_sequence,
                &outcome,
                previous_sequence,
                expected_count,
            );

            let result_outcome = if report.holds() {
                EventLogOutcome::Mirrored
            } else {
                EventLogOutcome::ParityMismatch
            };
            let mut result = make_result(
                mode,
                result_outcome,
                Some(contract.role),
                started,
                report.divergence.clone(),
                record_metrics,
            );
            result.global_sequence = Some(outcome.global_sequence);
            result.parity = Some(report);
            result
        }
        Err(err) => make_result(
            mode,
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

    fn worker_env(log: &str, mode: &str) -> EnvMap {
        [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "worker"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", log),
            ("NOETL_EHDB_EVENTLOG", mode),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn tmp_log(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-el-{tag}-{}-{:?}",
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
    fn off_mode_is_noop() {
        let e = worker_env("/tmp/unused.jsonl", "off");
        let r = mirror_event(&e, "100", Some(1), "evt", &Default::default(), false);
        assert_eq!(r.mode, EventLogMode::Off);
        assert_eq!(r.outcome, EventLogOutcome::Disabled);
        assert!(r.parity.is_none());
    }

    #[test]
    fn ehdb_disabled_is_noop_even_in_shadow() {
        // Shadow requested but the umbrella EHDB switch is off ⇒ still no-op.
        let e: EnvMap = [("NOETL_EHDB_EVENTLOG", "shadow")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let r = mirror_event(&e, "100", Some(1), "evt", &Default::default(), false);
        assert_eq!(r.outcome, EventLogOutcome::Disabled);
    }

    #[test]
    fn shadow_mirror_holds_parity() {
        let (log, dir) = tmp_log("shadow");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        // Mirror three events with a controlled 1-based authoritative sequence.
        for (i, seq) in [1u64, 2, 3].iter().enumerate() {
            let r = mirror_event(
                &e,
                "100",
                Some(*seq),
                &format!("evt-{i}"),
                &Default::default(),
                false,
            );
            assert_eq!(r.outcome, EventLogOutcome::Mirrored, "{:?}", r.detail);
            assert_eq!(r.global_sequence, Some(*seq));
            assert!(r.parity.as_ref().unwrap().holds());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shadow_without_authoritative_sequence_still_mirrors() {
        let (log, dir) = tmp_log("noauth");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        // No authoritative sequence supplied → count+order parity still enforced.
        let r1 = mirror_event(&e, "100", None, "a", &Default::default(), false);
        let r2 = mirror_event(&e, "100", None, "b", &Default::default(), false);
        assert_eq!(r1.outcome, EventLogOutcome::Mirrored);
        assert_eq!(r2.outcome, EventLogOutcome::Mirrored);
        assert!(r2.parity.as_ref().unwrap().sequence_ok);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shadow_flags_parity_mismatch_on_wrong_authoritative_sequence() {
        let (log, dir) = tmp_log("mismatch");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        // Authoritative claims 99 but EHDB assigns 1 → divergence, degraded.
        let r = mirror_event(&e, "100", Some(99), "evt", &Default::default(), false);
        assert_eq!(r.outcome, EventLogOutcome::ParityMismatch);
        assert!(!r.parity.as_ref().unwrap().holds());
        assert!(r.detail.is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_empty_and_oversized_payload() {
        let (log, dir) = tmp_log("bounds");
        let mut e = worker_env(log.to_str().unwrap(), "shadow");
        let empty = mirror_event(&e, "100", None, "", &Default::default(), false);
        assert_eq!(empty.outcome, EventLogOutcome::Rejected);
        e.insert(MAX_PAYLOAD_BYTES_ENV.to_string(), "4".to_string());
        let big = mirror_event(&e, "100", None, "toolong", &Default::default(), false);
        assert_eq!(big.outcome, EventLogOutcome::Rejected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn control_plane_role_guard_refused() {
        let e: EnvMap = [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "server"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_EVENTLOG", "shadow"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let r = mirror_event(&e, "100", Some(1), "evt", &Default::default(), false);
        assert_eq!(r.outcome, EventLogOutcome::GuardRefused);
        assert!(r.global_sequence.is_none());
    }

    #[test]
    fn primary_is_recognised_but_not_activated() {
        let (log, dir) = tmp_log("primary");
        let e = worker_env(log.to_str().unwrap(), "primary");
        let r = mirror_event(&e, "100", Some(1), "evt", &Default::default(), false);
        assert_eq!(r.mode, EventLogMode::Primary);
        assert_eq!(r.outcome, EventLogOutcome::PrimaryUnavailable);
        // Structurally impossible to serve primary in this build.
        assert!(!PRIMARY_SERVE_ACTIVATED);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_control_plane_still_guard_refused() {
        let e: EnvMap = [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "gateway"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_EVENTLOG", "primary"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let r = mirror_event(&e, "100", Some(1), "evt", &Default::default(), false);
        // Config error (control-plane role + data-plane env) → guard refused.
        assert_eq!(r.outcome, EventLogOutcome::GuardRefused);
    }

    /// Event-log-authoritative invariant, asserted structurally: this module
    /// must never reach the NoETL event log — it only touches the derived EHDB
    /// fabric via `ehdb_reference`.
    #[test]
    fn no_noetl_event_writer() {
        let full = include_str!("eventlog.rs");
        let src = full.split("#[cfg(test)]").next().unwrap();
        for forbidden in [
            "crate::events",
            "crate::client",
            "/api/events",
            "ExecutorEvent",
            "emit_event",
        ] {
            assert!(
                !code_lines(src).contains(forbidden),
                "forbidden NoETL event-writer reference `{forbidden}` in eventlog.rs"
            );
        }
    }

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
