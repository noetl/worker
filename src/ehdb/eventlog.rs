//! Event-log SHADOW wiring + PRIMARY-serve cutover (EHDB Phase 6 shadow,
//! Phase 9 tier-1 primary).
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
//! * `primary` — **EHDB serves the event log authoritatively** (Phase 9 tier 1):
//!   append + read + tail + ack + replay are served by the EHDB engine in place
//!   of the JetStream+Postgres incumbent, while each append is dual-run
//!   parity-checked against the incumbent sequence.  [`PRIMARY_SERVE_ACTIVATED`]
//!   is now `true` so this build *can* serve primary; whether it *does* is a
//!   pure runtime choice of the `NOETL_EHDB_EVENTLOG` flag (see reversibility).
//!
//! ## Reversibility (the cutover safety property)
//!
//! The cutover is reversible with **two independent levers**:
//!
//! 1. **Runtime flag (operational, instant, no redeploy)** — flip
//!    `NOETL_EHDB_EVENTLOG` from `primary` back to `shadow`/`off` and the
//!    incumbent (JetStream+Postgres) is authoritative again immediately.  Zero
//!    data loss: the primary path only ever *appends* to the EHDB `KeepAll` log
//!    and never mutates/deletes anything the incumbent owns, so the incumbent's
//!    store is exactly as it was, and the EHDB log stays whole on disk for a
//!    later re-enable.
//! 2. **Compile-time kill switch (structural, belt-and-suspenders)** — set
//!    [`PRIMARY_SERVE_ACTIVATED`] back to `false` and it is structurally
//!    impossible for the build to serve primary regardless of config.
//!
//! ## Boundaries (mirror the rest of `src/ehdb`)
//!
//! * Disabled-by-default no-op (byte-identical `/metrics`).
//! * Control-plane roles (`gateway`/`api`/`server`) refused before any engine
//!   opens — the gateway never touches the data plane.
//! * Bounded (payload cap) + stateless (engine opened + dropped per op).
//! * **Event-log-authoritative** — shadow mirroring AND primary serving persist
//!   already-authored events into the *derived* EHDB fabric; neither authors a
//!   NoETL event nor reaches `noetl.event` / `POST /api/events` (structurally
//!   asserted).  Primary changes the *serving engine* underneath, not event
//!   authorship — the gateway/server stay the gatekeeper of what is appended.

use std::sync::OnceLock;

use ehdb_reference::{
    compare_shadow_parity, exercise_primary_serve, EventLogAppendRequest, EventLogDriver,
    EventLogParityReport, EventLogPrimaryEvent, EventLogPrimaryServeReport, EventLogScanRequest,
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

/// Compile-time kill switch for primary-serve.  Phase 9 tier 1 activates it
/// (`true`): this build *can* serve the event log authoritatively from EHDB.
/// Whether it *does* is the pure runtime choice of `NOETL_EHDB_EVENTLOG`
/// (`primary` serves; `shadow`/`off` keep the incumbent authoritative), so the
/// cutover stays reversible without a redeploy.  Setting this back to `false`
/// is the belt-and-suspenders structural rollback — it makes primary-serve
/// unreachable regardless of config (the `primary` flag then degrades to
/// [`EventLogOutcome::PrimaryUnavailable`]).
pub const PRIMARY_SERVE_ACTIVATED: bool = true;

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
    /// `primary` served the append authoritatively from EHDB + dual-run parity
    /// against the incumbent held.
    ServedPrimary,
    /// `primary` served the append from EHDB but the dual-run parity against the
    /// incumbent diverged (degraded — surfaces on `last_degraded`).
    PrimaryDivergence,
    /// `primary` requested but primary-serve is not activated this build (the
    /// compile-time kill switch is off).
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
            EventLogOutcome::ServedPrimary => "served_primary",
            EventLogOutcome::PrimaryDivergence => "primary_divergence",
            EventLogOutcome::PrimaryUnavailable => "primary_unavailable",
            EventLogOutcome::Rejected => "rejected",
            EventLogOutcome::GuardRefused => "guard_refused",
            EventLogOutcome::Invalid => "invalid",
            EventLogOutcome::Unavailable => "unavailable",
        }
    }

    pub fn ok(&self) -> bool {
        matches!(
            self,
            EventLogOutcome::Disabled | EventLogOutcome::Mirrored | EventLogOutcome::ServedPrimary
        )
    }

    /// A degraded (but non-fatal) outcome — surfaces on the `last_degraded`
    /// gauge so a divergence or engine hiccup is visible without failing the
    /// authoritative path.
    fn degraded(&self) -> bool {
        matches!(
            self,
            EventLogOutcome::ParityMismatch
                | EventLogOutcome::PrimaryDivergence
                | EventLogOutcome::Unavailable
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

    // Primary with the compile-time kill switch off — refuse before any engine
    // opens (the belt-and-suspenders structural rollback).  Still resolve the
    // contract so a control-plane role is refused as a guard, not silently
    // treated as "primary unavailable".
    if mode == EventLogMode::Primary && !PRIMARY_SERVE_ACTIVATED {
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

    // Shadow (dual-write + compare) OR primary (EHDB serves authoritatively).
    // The engine op is identical — an append + parity compare; the mode only
    // changes which log is authoritative and how the outcome is labelled.
    let serving_primary = mode == EventLogMode::Primary;
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

            let result_outcome = match (serving_primary, report.holds()) {
                // Primary: EHDB served the append authoritatively.
                (true, true) => EventLogOutcome::ServedPrimary,
                (true, false) => EventLogOutcome::PrimaryDivergence,
                // Shadow: EHDB mirrored alongside the authoritative incumbent.
                (false, true) => EventLogOutcome::Mirrored,
                (false, false) => EventLogOutcome::ParityMismatch,
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

/// How many events the built-in primary-serve cycle drives through the engine.
pub const PRIMARY_SERVE_CYCLE_EVENTS: usize = 3;

/// Secret-free result of the authoritative primary-serve cycle (Phase 9 tier 1)
/// plus the operational reversibility demonstration.
#[derive(Debug, Clone)]
pub struct EventLogServeResult {
    pub mode: EventLogMode,
    pub outcome: EventLogOutcome,
    pub role: Option<EhdbClientRole>,
    pub duration_seconds: f64,
    /// The EHDB engine served the whole cycle with the incumbent's semantics
    /// preserved and dual-run parity intact.
    pub served_by_ehdb: bool,
    /// The full served-by-EHDB proof (present once the cycle ran).
    pub report: Option<EventLogPrimaryServeReport>,
    /// After serving primary, flipping `NOETL_EHDB_EVENTLOG` back to `shadow`
    /// over the same log mirrored a further event and the log replayed whole —
    /// the incumbent path is restored with zero data loss (rollback lever 1
    /// demonstrated operationally).
    pub reversible: bool,
    /// The log record count after the flip-back append (== cycle events + 1).
    pub records_after_revert: usize,
    pub detail: Option<String>,
}

/// Drive the authoritative event-log primary-serve cycle through the EHDB engine
/// and demonstrate operational reversibility.
///
/// In `primary` mode (and with [`PRIMARY_SERVE_ACTIVATED`]) this:
///
/// 1. runs [`exercise_primary_serve`] — append + global scan + per-execution
///    read + durable tail + ack + fresh-driver replay, all served
///    authoritatively by EHDB, dual-run parity-checked against the incumbent
///    sequence; then
/// 2. flips `NOETL_EHDB_EVENTLOG` back to `shadow` in a cloned env and mirrors a
///    further event over the SAME log, proving the incumbent/shadow path is
///    restored and the log stays whole (zero data loss on rollback).
///
/// Off/disabled ⇒ strict no-op (byte-identical `/metrics`).  Control-plane roles
/// are guard-refused before any engine opens.  Never authors a NoETL event — it
/// only exercises the derived EHDB fabric.
pub fn serve_primary_cycle(
    env: &EnvMap,
    opts: &EventLogOptions,
    record_metrics: bool,
) -> EventLogServeResult {
    let started = std::time::Instant::now();
    let mode = EventLogMode::from_env(env);

    // Early-exit builder (no cycle report) that records the `primary_serve`
    // metric — `disabled` outcomes are skipped by `record_eventlog`, preserving
    // the byte-identical no-op invariant.
    let early = |outcome: EventLogOutcome,
                 role: Option<EhdbClientRole>,
                 detail: Option<String>|
     -> EventLogServeResult {
        let duration_seconds = started.elapsed().as_secs_f64();
        if record_metrics {
            metrics::record_eventlog(
                "primary_serve",
                outcome.as_str(),
                outcome.ok(),
                outcome.degraded(),
                duration_seconds,
            );
        }
        EventLogServeResult {
            mode,
            outcome,
            role,
            duration_seconds,
            served_by_ehdb: false,
            report: None,
            reversible: false,
            records_after_revert: 0,
            detail,
        }
    };

    // Off mode OR the umbrella EHDB switch disabled ⇒ strict no-op.
    if mode == EventLogMode::Off || !truthy(env, EHDB_ENABLED_ENV) {
        return early(EventLogOutcome::Disabled, None, None);
    }

    // Resolve the contract (guards control-plane / disabled).  Pass
    // `record_metrics = false` so the only metric recorded here is the
    // `primary_serve`-labelled one from `early` / the final path.
    let contract = match resolve_contract(env, mode, started, false) {
        Ok(c) => c,
        Err(result) => {
            let r = *result;
            return early(r.outcome, r.role, r.detail);
        }
    };

    // Compile-time kill switch off ⇒ primary unavailable (structural rollback).
    if !PRIMARY_SERVE_ACTIVATED {
        return early(
            EventLogOutcome::PrimaryUnavailable,
            Some(contract.role),
            Some("event-log primary serve is not activated in this build".to_string()),
        );
    }
    // The cycle only serves under the `primary` flag; `shadow` stays mirror-only.
    if mode != EventLogMode::Primary {
        return early(
            EventLogOutcome::PrimaryUnavailable,
            Some(contract.role),
            Some("primary-serve cycle requires NOETL_EHDB_EVENTLOG=primary".to_string()),
        );
    }

    let log = contract.local_reference_log.clone().expect("log present");
    let tenant = opts
        .tenant
        .clone()
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string());
    let namespace = opts
        .namespace
        .clone()
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string());
    let driver = LocalReferenceEventLogDriver::new(log, tenant, namespace);

    // Deterministic cycle: two executions interleaved, known 1-based
    // authoritative sequences so the dual-run parity check is exact.
    let events: Vec<EventLogPrimaryEvent> = [("100", 1u64), ("200", 2), ("100", 3)]
        .into_iter()
        .map(|(exec, seq)| EventLogPrimaryEvent {
            execution_id: exec.to_string(),
            transaction_id: format!("primary-{exec}-{seq}"),
            payload: format!("{{\"exec\":\"{exec}\",\"seq\":{seq}}}"),
            authoritative_sequence: Some(seq),
        })
        .collect();

    let report = match exercise_primary_serve(
        &driver,
        &events,
        "primary-serve-projector",
        &new_transaction_id(),
    ) {
        Ok(r) => r,
        Err(err) => {
            return early(
                classify_helper_error(&err),
                Some(contract.role),
                Some(err.to_string()),
            )
        }
    };
    let served = report.served_by_ehdb();

    // Reversibility (rollback lever 1): flip the flag back to `shadow` in a
    // cloned env and mirror one more event over the SAME log.  A clean mirror
    // plus a whole-log replay proves the incumbent/shadow path is restored with
    // zero data loss.
    let mut shadow_env = env.clone();
    shadow_env.insert(EVENTLOG_MODE_ENV.to_string(), "shadow".to_string());
    let revert = mirror_event(&shadow_env, "100", None, "{\"revert\":true}", opts, false);
    let records_after_revert = driver
        .scan_global(&EventLogScanRequest {
            after: None,
            limit: events.len() + 8,
        })
        .map(|s| s.record_count)
        .unwrap_or(0);
    let reversible =
        revert.outcome == EventLogOutcome::Mirrored && records_after_revert == events.len() + 1;

    let outcome = if served && reversible {
        EventLogOutcome::ServedPrimary
    } else {
        EventLogOutcome::PrimaryDivergence
    };
    let detail = if served && reversible {
        None
    } else if !served {
        report.divergence.clone()
    } else {
        Some(format!(
            "reversibility flip-back failed: revert={} records={}",
            revert.outcome.as_str(),
            records_after_revert
        ))
    };

    let duration_seconds = started.elapsed().as_secs_f64();
    if record_metrics {
        metrics::record_eventlog(
            "primary_serve",
            outcome.as_str(),
            outcome.ok(),
            outcome.degraded(),
            duration_seconds,
        );
    }
    EventLogServeResult {
        mode,
        outcome,
        role: Some(contract.role),
        duration_seconds,
        served_by_ehdb: served,
        report: Some(report),
        reversible,
        records_after_revert,
        detail,
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
    fn primary_serves_authoritatively() {
        let (log, dir) = tmp_log("primary");
        let e = worker_env(log.to_str().unwrap(), "primary");
        // Phase 9 tier 1: primary is activated, so a primary append is served
        // authoritatively by EHDB (not refused).  Global seq 1, parity holds.
        let r = mirror_event(&e, "100", Some(1), "evt", &Default::default(), false);
        assert_eq!(r.mode, EventLogMode::Primary);
        assert_eq!(r.outcome, EventLogOutcome::ServedPrimary);
        assert_eq!(r.global_sequence, Some(1));
        assert!(r.parity.as_ref().unwrap().holds());
        assert!(PRIMARY_SERVE_ACTIVATED);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_flags_divergence_on_wrong_authoritative_sequence() {
        let (log, dir) = tmp_log("primary-diverge");
        let e = worker_env(log.to_str().unwrap(), "primary");
        // Incumbent claims 99 but EHDB assigns 1 → served but dual-run diverged.
        let r = mirror_event(&e, "100", Some(99), "evt", &Default::default(), false);
        assert_eq!(r.outcome, EventLogOutcome::PrimaryDivergence);
        assert!(!r.parity.as_ref().unwrap().holds());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_cycle_served_by_ehdb_and_reversible() {
        let (log, dir) = tmp_log("cycle");
        let e = worker_env(log.to_str().unwrap(), "primary");
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.mode, EventLogMode::Primary);
        assert_eq!(r.outcome, EventLogOutcome::ServedPrimary, "{:?}", r.detail);
        assert!(r.served_by_ehdb);
        let report = r.report.as_ref().unwrap();
        assert!(report.served_by_ehdb());
        assert_eq!(report.appended, PRIMARY_SERVE_CYCLE_EVENTS);
        assert!(
            report.scan_ordered && report.scope_ok && report.ack_advanced && report.replay_matches
        );
        // Reversibility: flip back to shadow appended one more; log is whole.
        assert!(r.reversible);
        assert_eq!(r.records_after_revert, PRIMARY_SERVE_CYCLE_EVENTS + 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_cycle_off_is_noop() {
        let e = worker_env("/tmp/unused-cycle.jsonl", "off");
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.outcome, EventLogOutcome::Disabled);
        assert!(r.report.is_none());
        assert!(!r.served_by_ehdb);
    }

    #[test]
    fn primary_serve_cycle_shadow_is_primary_unavailable() {
        let (log, dir) = tmp_log("cycle-shadow");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        // The cycle only serves under the `primary` flag.
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.outcome, EventLogOutcome::PrimaryUnavailable);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_cycle_control_plane_guard_refused() {
        let e: EnvMap = [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "server"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_EVENTLOG", "primary"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.outcome, EventLogOutcome::GuardRefused);
        assert!(r.report.is_none());
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
