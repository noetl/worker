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
    /// The `durable_segment` backend refused the append because this replica
    /// does not own the execution's shard (execution-affinity single-writer
    /// routing). Correct behaviour, not an error: the owning replica mirrors it.
    /// Never fires on the default `local_reference` backend nor under the
    /// single-owner default.
    RoutedAway,
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
            EventLogOutcome::RoutedAway => "routed_away",
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

/// Shared with the tier store (ai-meta#257 PR 3) so remote appends carry the
/// same transaction-id shape as local ones.
pub(crate) fn new_transaction_id() -> String {
    format!("ehdbel-{}", txn_gen().next_id())
}

/// Flag arming the authoritative-id stamp (noetl/ai-meta#258). Default off.
pub const AUTHORITATIVE_ID_STAMP_ENV: &str = "NOETL_EHDB_AUTHORITATIVE_ID_STAMP";

/// What happened when the mirrored payload's identity was reconciled against
/// the identity the server assigned.
///
/// Every value is a label on `noetl_ehdb_eventlog_ops_total{operation=
/// "authoritative_id"}`, so the distribution is readable from a scrape. That
/// matters most for `absent`: before this existed, a record with no `event_id`
/// was mirrored and counted as a clean shadow op, and the fact that it could
/// never be matched against the authoritative log was invisible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthoritativeIdVerdict {
    /// The caller supplied no authoritative id (selfcheck drive, unit test).
    NotSupplied,
    /// An authoritative id was supplied and the payload already carried the
    /// same one — the producer stamped it and the server honoured it.
    Agreed,
    /// The payload carried no `event_id` and the authoritative one was written
    /// in. This is the common case for five live emit paths (`spool_runtime`
    /// ×2, `subscription` ×2, and the retry path in `control_plane`) which all
    /// send `event_id: None` and let the server assign it.
    Stamped,
    /// The payload carried no `event_id`, an authoritative id was available, and
    /// the stamp is **disarmed** — so the record goes to the tier
    /// unidentifiable. Recorded so the flag-off state is visible rather than
    /// looking like the flag-on one.
    Unstamped,
    /// The payload's `event_id` and the server's disagree. A genuine cross-store
    /// identity divergence, detected at the append site.
    Disagreed,
    /// The payload is not a JSON object, so it has no identity to reconcile.
    /// Only the synthetic selfcheck payloads look like this.
    NotJson,
}

impl AuthoritativeIdVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotSupplied => "not_supplied",
            Self::Agreed => "agreed",
            Self::Stamped => "stamped",
            Self::Unstamped => "unstamped",
            Self::Disagreed => "disagreed",
            Self::NotJson => "not_json",
        }
    }

    /// A disagreement is the only verdict that indicates something wrong.
    fn degraded(self) -> bool {
        matches!(self, Self::Disagreed)
    }
}

/// Reconcile the mirrored payload's `event_id` against the server-assigned one.
///
/// Returns the payload to mirror (rewritten only when a stamp was applied) plus
/// the verdict.
///
/// The rewrite is deliberately narrow: it fills in an `event_id` the producer
/// omitted and never overwrites one it supplied. A stamp that overwrote a
/// producer's id would make the two stores agree by construction, which is the
/// one thing a parity mechanism must not do — it would convert a real
/// divergence into a silent correction.
pub fn reconcile_authoritative_id(
    payload: &str,
    authoritative_event_id: Option<i64>,
    stamp: bool,
) -> (String, AuthoritativeIdVerdict) {
    let Some(auth_id) = authoritative_event_id else {
        return (payload.to_string(), AuthoritativeIdVerdict::NotSupplied);
    };
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(payload) else {
        return (payload.to_string(), AuthoritativeIdVerdict::NotJson);
    };
    let Some(obj) = value.as_object_mut() else {
        return (payload.to_string(), AuthoritativeIdVerdict::NotJson);
    };

    match obj.get("event_id").and_then(|v| match v {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }) {
        Some(producer_id) if producer_id == auth_id => {
            (payload.to_string(), AuthoritativeIdVerdict::Agreed)
        }
        Some(producer_id) => {
            tracing::warn!(
                producer_event_id = producer_id,
                authoritative_event_id = auth_id,
                "EHDB mirror: the producer's event_id and the server-assigned event_id disagree"
            );
            (payload.to_string(), AuthoritativeIdVerdict::Disagreed)
        }
        None if stamp => {
            obj.insert(
                "event_id".to_string(),
                serde_json::Value::Number(auth_id.into()),
            );
            (
                serde_json::to_string(&value).unwrap_or_else(|_| payload.to_string()),
                AuthoritativeIdVerdict::Stamped,
            )
        }
        None => (payload.to_string(), AuthoritativeIdVerdict::Unstamped),
    }
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

    let request = EventLogAppendRequest {
        execution_id: execution_id.to_string(),
        transaction_id: opts
            .transaction_id
            .clone()
            .unwrap_or_else(new_transaction_id),
        payload: payload.to_string(),
        // Deploy A: inert.  Deploy B populates this from the payload's event_id.
        event_id: None,
    };

    // Storage-backend selection (durable event-log backend, slice 4): the
    // default `local_reference` backend appends byte-identically to the JSONL
    // log the worker has always used; `NOETL_EHDB_EVENTLOG_BACKEND=durable_segment`
    // routes the append through the durable segment + execution-affinity
    // single-writer + shared-tier stack instead.  Orthogonal to the mode axis
    // above (mode decides *whether* EHDB serves; backend decides *which* durable
    // engine does the append).  Both backends yield the same
    // `EventLogAppendOutcome` shape, so the parity path below is backend-agnostic.
    let backend = super::eventlog_backend::selected_backend(env);
    match super::eventlog_backend::append_selected(env, &contract, &request, opts, backend) {
        Ok(super::eventlog_backend::AppendDispatch::Served(outcome)) => {
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

            // ai-meta#257 PR 6 — the serve decision now runs through the shared
            // policy (`primary_serve::decide`) instead of being re-derived here.
            //
            // The policy adds the condition this site never checked: a DURABLE
            // SERVICE must be reachable.  Without it `primary` was deciding to
            // "serve authoritatively" from a POD-LOCAL fragment while the
            // incumbent held all history — authoritative in name only.
            //
            // Default-off: with no tier-service address configured,
            // `durable_service_reachable` is false, so `primary` demotes to the
            // incumbent and shadow is untouched.  That is byte-identical to the
            // pre-PR-6 behaviour for every configuration in use today, because
            // the address is set nowhere.
            // ARM-D FIX: ask the MEASURED verdict, not whether an address is set.
            // `config.is_some()` answered "is it configured"; a black-hole address
            // then counted as a durable service and primary served — authoritative
            // in name only.  Reachability is now derived from real tier-service
            // operations (see `reachability`), and "never contacted" is not
            // reachable.
            let durable_service_reachable = super::tier_client::TierClientConfig::from_env()
                .is_some()
                && super::reachability::is_reachable();
            let decision = super::primary_serve::decide(
                serving_primary,
                durable_service_reachable,
                report.holds(),
            );
            let result_outcome = match (serving_primary, report.holds(), decision.served_by_ehdb())
            {
                // Primary AND the policy agreed EHDB may serve.
                (true, true, true) => EventLogOutcome::ServedPrimary,
                // Primary, parity diverged ⇒ demote.  The incumbent's write already
                // happened, so the caller is served correctly; the tier is marked
                // degraded rather than the caller being failed.
                (true, false, _) => EventLogOutcome::PrimaryDivergence,
                // Primary, parity held, but the policy refused — no durable service.
                // Reported as PrimaryUnavailable, NOT ServedPrimary: claiming to have
                // served authoritatively from a pod-local fragment is the lie this
                // whole RFC exists to prevent.
                (true, true, false) => EventLogOutcome::PrimaryUnavailable,
                // Shadow is unchanged by this PR.
                (false, true, _) => EventLogOutcome::Mirrored,
                (false, false, _) => EventLogOutcome::ParityMismatch,
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
        // `durable_segment` refused: this replica does not own the execution's
        // shard (single-writer routing).  Correct behaviour — the owning replica
        // mirrors it — recorded as a neutral (non-ok, non-degraded) outcome.
        Ok(super::eventlog_backend::AppendDispatch::RoutedAway { owner_shard }) => make_result(
            mode,
            EventLogOutcome::RoutedAway,
            Some(contract.role),
            started,
            Some(format!(
                "execution routed to shard {owner_shard} owner (backend={})",
                backend.as_str()
            )),
            record_metrics,
        ),
        Err(err) => make_result(
            mode,
            classify_helper_error(&err),
            Some(contract.role),
            started,
            Some(err),
            record_metrics,
        ),
    }
}

/// Resolve the once-per-process env snapshot that arms the **live event-append
/// hook** (noetl/ehdb#234 runtime integration).  This is the gate the worker's
/// authoritative event path (`ControlPlaneClient::emit_event`) calls exactly
/// once at client construction, so the per-event path does *zero* work when the
/// hook is inactive.
///
/// Returns `Some(env)` — meaning "mirror every live event" — ONLY when all of:
///
/// * the umbrella switch `NOETL_EHDB_ENABLED` is truthy, AND
/// * the event-log tier `NOETL_EHDB_EVENTLOG` is `shadow` (this slice wires the
///   live path for **shadow** only; `off`/`primary` return `None` so a live
///   drive never dual-writes under them — primary live-serve is a separate
///   follow-up, and `off` stays byte-identical), AND
/// * the resolved contract is a data-plane role (`worker`/`playbook`/`system`)
///   running the bounded `local_reference` runtime with a log configured.
///
/// Every other case (disabled, tier off/primary, control-plane role, malformed
/// contract) returns `None` — a strict no-op hook.  The env is snapshotted (the
/// process env is immutable for the worker's lifetime) so the per-event mirror
/// reuses it without re-collecting `std::env::vars()` on the hot path.
pub fn runtime_hook_env(env: &EnvMap) -> Option<EnvMap> {
    // Umbrella switch off ⇒ no hook (byte-identical to a build without EHDB).
    if !truthy(env, EHDB_ENABLED_ENV) {
        return None;
    }
    // Shadow-only for the live path this slice.  `off` and `primary` do not
    // arm the live mirror.
    // `off` ⇒ no hook.  `shadow` AND `primary` both arm the live mirror.
    //
    // `primary` used to return `None` here, so selecting it SILENTLY DISARMED
    // verification: the mirror stopped, and nothing served in its place.
    // Measured in kind on one binary under identical load: `shadow` mirrored 30
    // events, `primary` mirrored 0.  A mode meant to promote the tier instead
    // turned it off, with no signal (noetl/ai-meta#247).
    //
    // Since ai-meta#257 PR 5/6 the event log DOES have a runtime serve path —
    // the append site below runs `primary_serve::decide` — so `primary` here is
    // "mirror, and additionally serve when the policy allows", not "shadow with
    // a warning".  Arming the mirror is monotonic either way: it can only add
    // verification, never remove it.  `primary` keeps parity-checking on every
    // op precisely so that serving cannot silence verification.
    match EventLogMode::from_env(env) {
        EventLogMode::Off => return None,
        EventLogMode::Primary => super::note_primary_selected("eventlog"),
        EventLogMode::Shadow => {}
    }
    // A control-plane role carrying a data-plane env fails contract validation;
    // `.ok()?` drops it (the gateway never mirrors).  Defense-in-depth: also
    // require an explicit data-plane role + a live local-reference log.
    let contract = contract_from_env(env).ok()?;
    if !contract.role.is_data_plane() {
        return None;
    }
    if !contract.uses_local_reference_runtime() || contract.local_reference_log.is_none() {
        return None;
    }
    Some(env.clone())
}

/// Live event-append hook: mirror one already-authored, just-emitted platform
/// event into the EHDB event-log shadow fabric.
///
/// This is the runtime counterpart of the `ehdb-selfcheck mirror-eventlog`
/// drive — it calls the SAME [`mirror_event`] shadow dual-write + parity path,
/// but on the real events the worker emits to the control plane, so a live drive
/// advances the `noetl_ehdb_eventlog_*` metrics instead of only the selfcheck.
///
/// `authoritative_sequence` is passed as `None`, and that stays correct: the
/// authoritative log has no 1-based gapless global sequence to compare EHDB's
/// against, so any value put there would be measured against the wrong scale.
/// What the worker *can* now supply is the authoritative **identity** —
/// `authoritative_event_id`, read back from the `POST /api/events` response and
/// carried on [`EventLogOptions`] — which is what lets the server's cross-store
/// comparator match a mirrored record to its `noetl.event` row at all
/// (noetl/ai-meta#258). Count, ordering and payload parity against the
/// authoritative log are compared **server-side**, because per
/// `data-access-boundary.md` the worker may not read `noetl.*`.
///
/// **Best-effort + isolated.**  Shadow is auxiliary: this NEVER affects the
/// authoritative event path.  Any failure inside the mirror is contained — the
/// engine-error cases already surface as non-`ok` outcomes (recorded to the
/// degraded metric), and an unexpected panic is caught here and returned as
/// [`EventLogOutcome::Unavailable`] rather than unwinding into the caller's
/// event-emit path.  The caller discards the return; the metric carries the
/// signal.
pub fn mirror_live_event(
    env: &EnvMap,
    execution_id: &str,
    payload: &str,
    authoritative_event_id: Option<i64>,
) -> LiveMirror {
    // Reconcile identity BEFORE the append, and return the reconciled payload so
    // every store that receives this event receives the same bytes. That is not
    // a tidiness point: the pod-local log and the writer-fronted tier service
    // are both read by the server's comparator, and a record that is identified
    // in one and anonymous in the other would diverge against itself.
    let (payload, verdict) = reconcile_authoritative_id(
        payload,
        authoritative_event_id,
        truthy(env, AUTHORITATIVE_ID_STAMP_ENV),
    );
    metrics::record_eventlog(
        "authoritative_id",
        verdict.as_str(),
        !verdict.degraded(),
        verdict.degraded(),
        0.0,
    );

    let guarded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        mirror_event(
            env,
            execution_id,
            None,
            &payload,
            &EventLogOptions::default(),
            true,
        )
        .outcome
    }));
    LiveMirror {
        outcome: guarded.unwrap_or(EventLogOutcome::Unavailable),
        payload,
        id_verdict: verdict,
    }
}

/// What [`mirror_live_event`] mirrored, and what it mirrored.
///
/// The payload is returned rather than discarded because the caller has a second
/// store to feed — the writer-fronted tier service — and both must receive the
/// identical record.
#[derive(Debug, Clone)]
pub struct LiveMirror {
    pub outcome: EventLogOutcome,
    /// The bytes actually appended, after any authoritative-id stamp.
    pub payload: String,
    pub id_verdict: AuthoritativeIdVerdict,
}

// ===========================================================================
// The serve site for SERVICE-RESOLVED appends.
//
// ai-meta#257 P0.  `primary_serve::decide` had exactly two runtime call sites,
// and the configuration that makes the event-log tier *correct* at more than one
// replica routes around BOTH of them:
//
// | call site | routed around by |
// | :-- | :-- |
// | `mirror_live_event` (the worker's own emit hook) | `MIRROR_SOURCE=server` disarms the hook in `client/control_plane.rs` before the tier mode is consulted |
// | `mirror_event` from the append handler's `Resolution::Local` branch | `TIER_QUERY_SOURCE=service` takes `Resolution::Service(client)`, which calls `client.append()` and never enters `mirror_event` |
//
// So on `MIRROR_SOURCE=server` + `TIER_QUERY_SOURCE=service` a flip to `primary`
// changed nothing measurable: `served_primary` never recorded, the whole
// `noetl_ehdb_eventlog_ops_total` family stayed absent, and no line was logged.
// Measured in kind at three replicas — 13 events per execution flowed and landed
// correctly the whole time, which is why nothing looked wrong.
//
// This is the third site, and it is the one the composed configuration reaches.
// It is deliberately NOT a new policy: it calls the same
// [`super::primary_serve::decide`] with the same three conditions and maps the
// verdict onto the same outcome vocabulary `mirror_event` uses, so the two sites
// cannot disagree about what `primary` means.  The mapping is written directly
// below `mirror_event`'s for that reason — a reviewer can read them together.
// ===========================================================================

/// What the writer-fronted tier service acknowledged for one append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceAppendAck {
    /// The sequence the remote store assigned this record.
    pub global_sequence: u64,
    /// Total records the remote store holds after this append.
    ///
    /// `None` only when the writer predates the field (a rollout skew — the pool
    /// and the writer run the same image in a coherent deployment).  Absent means
    /// the strong check cannot run, not that it failed; see [`Self::parity`].
    pub log_record_count: Option<u64>,
    /// True when the store ACKNOWLEDGED an existing record rather than writing a
    /// new one (noetl/ai-meta#313).
    ///
    /// Absent on a writer that predates the field, which parses as `false` — the
    /// pre-dedupe behaviour, and the safe direction: an old writer never claims a
    /// dedupe it did not perform.
    pub deduplicated: bool,
}

impl ServiceAppendAck {
    /// Parse the tier service's append reply.
    ///
    /// `None` for every reply that is not an acknowledged append — the service's
    /// typed refusals (`invalid …` / `unavailable …` / `error …`) are plain text,
    /// so they fail the JSON parse, and a JSON body without `appended: true` is
    /// not an append either.  Both mean the record is not in the store, which the
    /// caller must not mistake for a parity question.
    pub fn parse(body: &str) -> Option<Self> {
        let v: serde_json::Value = serde_json::from_str(body).ok()?;
        if !v.get("appended").and_then(|a| a.as_bool()).unwrap_or(false) {
            return None;
        }
        Some(Self {
            global_sequence: v.get("global_sequence").and_then(|s| s.as_u64())?,
            log_record_count: v.get("log_record_count").and_then(|c| c.as_u64()),
            deduplicated: v
                .get("deduplicated")
                .and_then(|d| d.as_bool())
                .unwrap_or(false),
        })
    }

    /// Parity for this append, computed from the REMOTE store's own reply.
    ///
    /// Two checks, and which ones ran is reported rather than assumed:
    ///
    /// * **ordering** — the sequence must be strictly greater than the previous
    ///   one *in this batch*.  Batch-local on purpose: the store has N appenders
    ///   (one per replica) and process-global bookkeeping would race, reporting a
    ///   divergence whenever two batches interleaved.  Within one batch the
    ///   appends are sequential, and the store's sequence only ever grows, so
    ///   strict monotonicity is a real invariant there.  `previous_sequence = 0`
    ///   for a batch's first record makes that check vacuous, which is why it is
    ///   never the only one.
    /// * **gapless count** — `log_record_count == global_sequence`, the same
    ///   invariant [`mirror_event`] checks on the local engine.  Skipped, with the
    ///   skip named in the detail, when the writer did not send the count.
    ///
    /// **This is SELF-consistency, not cross-store parity, and the difference
    /// matters.**  `mirror_event` is in the same position on the live path — it
    /// passes `authoritative_sequence: None`, because the authoritative log has no
    /// 1-based gapless sequence to compare against, so anything put there would be
    /// measured on the wrong scale.  Parity against `noetl.event` is computed
    /// **server-side** by the cross-store comparator, which is the only component
    /// allowed to read `noetl.*` (`data-access-boundary.md`).  What this check
    /// buys is that the tier does not serve from a store that has forgotten or
    /// rewound — and a promoted tier that had would otherwise answer confidently.
    /// `pub(crate)` since #265: the projection tier's serve path reuses this
    /// rather than reimplementing it. One store engine renders one ack body, so
    /// a second parser would be a second chance to disagree about what "the
    /// record landed" means — on exactly the two tiers whose verdicts an
    /// operator compares during a cutover.
    pub(crate) fn parity(&self, previous_sequence: u64) -> Result<Option<&'static str>, String> {
        // ⚠⚠ A deduplicated append satisfies NEITHER check, and both failures are
        // correct readings of the wrong question (noetl/ai-meta#313).
        //
        // The store acknowledged a record that was already there: the sequence is
        // the EXISTING one, so it is not greater than the previous append's, and
        // the record count did not advance, so it no longer equals the sequence.
        // Running either check on a dedupe reports a divergence for a store that
        // behaved exactly as designed.
        //
        // ⚠ The skip is NARROW on purpose. It is taken only when the store itself
        // says `deduplicated`, and it returns a NAMED outcome rather than `Ok(None)`
        // — a silent pass here would let a store that reported dedupe on every
        // append suppress every divergence it ever had. Named, that pattern is
        // visible as an outcome nobody expected to see at volume, instead of as
        // silence. Everything else takes the checks unchanged.
        if self.deduplicated {
            return Ok(Some(
                "tier-service acknowledged an existing record (deduplicated); ordering \
                 and gapless-count are not meaningful for a record that was not written",
            ));
        }
        if self.global_sequence <= previous_sequence {
            return Err(format!(
                "ordering divergence: tier-service sequence {} not > previous {previous_sequence}",
                self.global_sequence
            ));
        }
        match self.log_record_count {
            Some(c) if c != self.global_sequence => Err(format!(
                "count divergence: tier-service record count {c} != sequence {}",
                self.global_sequence
            )),
            Some(_) => Ok(None),
            None => Ok(Some(
                "tier-service reply carries no log_record_count (writer predates the field); \
                 parity verified by ordering alone",
            )),
        }
    }
}

/// One service-resolved append's serve verdict.
#[derive(Debug, Clone)]
pub struct ServiceAppendServe {
    /// The shared policy's verdict.  Carried alongside the outcome so a caller
    /// can report the serve state without re-deriving it from a label.
    pub decision: super::primary_serve::ServeDecision,
    pub outcome: EventLogOutcome,
    pub detail: Option<String>,
    /// The sequence the store assigned, when the record landed.  The caller
    /// threads this into the next record's `previous_sequence`.
    pub sequence: Option<u64>,
}

/// Decide, record and log the serve outcome for ONE record appended through the
/// writer-fronted tier service.
///
/// `reply` is exactly what [`super::tier_client::TierClient::append`] returned:
/// `Ok(body)` for a reply from the service, `Err(e)` for a transport failure.
///
/// The three conditions are unchanged and are all measured, never assumed:
///
/// * **primary mode** — `NOETL_EHDB_EVENTLOG=primary` and the compile-time
///   [`PRIMARY_SERVE_ACTIVATED`] switch.
/// * **durable service reachable** — [`super::reachability::is_reachable`], which
///   only a real successful operation sets and any transport failure clears.  The
///   append that produced `reply` is itself that operation, so this is the
///   post-append verdict rather than a cached poll.
/// * **parity held** — the remote store's own reply, per
///   [`ServiceAppendAck::parity`].
///
/// **Demotion never fails the caller and never serves partial data.**  The
/// server's own write to `noetl.event` has already happened by the time it
/// mirrors here, so a demote costs nothing but the tier's authority; the reply to
/// the server is unchanged by this decision (the append handler still reports
/// what landed).  A record that did NOT land is recorded on the degraded label
/// and demotes — which is what makes a dead tier service visible instead of
/// silently stopping the serve signal.
pub fn serve_service_append(
    env: &EnvMap,
    reply: Result<&str, &str>,
    previous_sequence: u64,
    duration_seconds: f64,
) -> ServiceAppendServe {
    // ARM-D discipline: ask the MEASURED verdict, not whether an address is set.
    // A black-hole address is configured and unreachable, and `primary` must not
    // serve from it. The append that produced `reply` is the operation that set
    // this, so it is a post-append fact rather than a cached poll.
    let durable_service_reachable = super::tier_client::TierClientConfig::from_env().is_some()
        && super::reachability::is_reachable();
    serve_service_append_with(
        env,
        reply,
        previous_sequence,
        duration_seconds,
        durable_service_reachable,
    )
}

/// [`serve_service_append`] with the reachability verdict passed in.
///
/// Split out so the decision is testable without the process env and the
/// process-global reachability latch: `cargo test` does **not** serialise tests,
/// so a test that set `NOETL_EHDB_TIER_SERVICE_ADDR` or drove the latch would race
/// every other test in the binary. The public wrapper above is the only place the
/// two globals are read.
pub(crate) fn serve_service_append_with(
    env: &EnvMap,
    reply: Result<&str, &str>,
    previous_sequence: u64,
    duration_seconds: f64,
    durable_service_reachable: bool,
) -> ServiceAppendServe {
    let mode = EventLogMode::from_env(env);

    // Off / EHDB disabled ⇒ strict no-op, and no metric.  The append handler is
    // only reachable with the tier configured, but the gate stays here so this
    // site can never be the reason the family exists on a disabled build.
    if mode == EventLogMode::Off || !truthy(env, EHDB_ENABLED_ENV) {
        return ServiceAppendServe {
            decision: super::primary_serve::decide(false, false, false),
            outcome: EventLogOutcome::Disabled,
            detail: None,
            sequence: None,
        };
    }

    let is_primary = mode == EventLogMode::Primary && PRIMARY_SERVE_ACTIVATED;

    // Did the record land, and if so did parity hold?
    let (ack, parity_held, mut detail, landed_outcome) = match reply {
        Err(e) => (
            None,
            false,
            Some(format!("tier-service append failed: {e}")),
            Some(EventLogOutcome::Unavailable),
        ),
        Ok(body) => match ServiceAppendAck::parse(body) {
            // A reply the service produced that acknowledges no append: one of
            // its typed refusals.  `invalid` is a caller mistake and NOT
            // degraded — per `reachability`, a service that rejects one record is
            // reachable and healthy, and demoting the whole tier for a poisoned
            // payload would let one record disable authoritative serving.
            // `unavailable` / `error` are the store, and are degraded.
            None => {
                let trimmed = body.trim();
                let outcome = if trimmed.starts_with("invalid") {
                    EventLogOutcome::Rejected
                } else {
                    EventLogOutcome::Unavailable
                };
                (
                    None,
                    false,
                    Some(format!(
                        "tier-service refused the append: {}",
                        &trimmed[..trimmed.len().min(200)]
                    )),
                    Some(outcome),
                )
            }
            Some(ack) => match ack.parity(previous_sequence) {
                Ok(note) => (Some(ack), true, note.map(str::to_string), None),
                Err(divergence) => (Some(ack), false, Some(divergence), None),
            },
        },
    };

    let decision = super::primary_serve::decide(is_primary, durable_service_reachable, parity_held);

    // The SAME mapping `mirror_event` uses.  `landed_outcome` short-circuits it
    // for the records that are not in the store at all: claiming a parity verdict
    // about a record that was never written would be a statement about nothing.
    let outcome = match landed_outcome {
        Some(o) => o,
        None => match (is_primary, parity_held, decision.served_by_ehdb()) {
            (true, true, true) => EventLogOutcome::ServedPrimary,
            (true, false, _) => EventLogOutcome::PrimaryDivergence,
            // Primary, parity held, policy refused — no reachable durable
            // service.  Never `ServedPrimary`: claiming to have served
            // authoritatively is the lie this whole RFC exists to prevent.
            (true, true, false) => EventLogOutcome::PrimaryUnavailable,
            (false, true, _) => EventLogOutcome::Mirrored,
            (false, false, _) => EventLogOutcome::ParityMismatch,
        },
    };

    if detail.is_none() && outcome == EventLogOutcome::PrimaryUnavailable {
        detail = Some(
            "no reachable durable tier service — the event-log tier is primary but has \
             nothing authoritative to serve from; the incumbent answers"
                .to_string(),
        );
    }

    metrics::record_eventlog(
        "mirror",
        outcome.as_str(),
        outcome.ok(),
        outcome.degraded(),
        duration_seconds,
    );
    log_serve_transition(&decision, outcome, detail.as_deref());

    ServiceAppendServe {
        decision,
        outcome,
        detail,
        sequence: ack.map(|a| a.global_sequence),
    }
}

/// The last serve state this process logged, as a code.  `0` is "nothing yet".
///
/// A flip is a once-per-deployment event and an append is a 13-per-execution one,
/// so the line has to be emitted on TRANSITION or it is noise that nobody reads —
/// and a line nobody reads is the same as no line, which is the state this fix
/// found.  Transitions in BOTH directions are logged: a demote that is silent
/// because the promote was already logged would hide the outage.
static LAST_SERVE_STATE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

fn serve_state_code(decision: &super::primary_serve::ServeDecision) -> u8 {
    use super::primary_serve::{DemoteReason, ServeDecision};
    match decision {
        ServeDecision::ServedByEhdb => 1,
        ServeDecision::ServedByIncumbent {
            reason: DemoteReason::NotPrimary,
        } => 2,
        ServeDecision::ServedByIncumbent {
            reason: DemoteReason::NoDurableService,
        } => 3,
        ServeDecision::ServedByIncumbent {
            reason: DemoteReason::ParityDiverged,
        } => 4,
    }
}

/// Log a serve-state change, once per transition.
fn log_serve_transition(
    decision: &super::primary_serve::ServeDecision,
    outcome: EventLogOutcome,
    detail: Option<&str>,
) {
    let code = serve_state_code(decision);
    let prev = LAST_SERVE_STATE.swap(code, std::sync::atomic::Ordering::Relaxed);
    if prev == code {
        return;
    }
    let label = decision.outcome_label();
    if decision.served_by_ehdb() {
        tracing::info!(
            serve_state = label,
            outcome = outcome.as_str(),
            "{}=primary IS SERVING: the event-log tier answered authoritatively through the \
             writer-fronted tier service (mirror_source=server, tier_query_source=service)",
            EVENTLOG_MODE_ENV
        );
    } else if decision.degraded() {
        tracing::warn!(
            serve_state = label,
            outcome = outcome.as_str(),
            detail = detail.unwrap_or("-"),
            "{}=primary is NOT serving — the incumbent answers (demoted: {label})",
            EVENTLOG_MODE_ENV
        );
    } else {
        tracing::info!(
            serve_state = label,
            outcome = outcome.as_str(),
            "event-log tier serve state: {label} (the incumbent is authoritative)"
        );
    }
}

/// The serve state this process last decided, as the label the metric carries.
///
/// `"unknown"` until the first service-resolved append — which is the honest
/// answer, and different from every other value: a process that has not been
/// asked to mirror anything has not decided anything.
pub fn current_serve_state() -> &'static str {
    use super::primary_serve::{DemoteReason, ServeDecision};
    match LAST_SERVE_STATE.load(std::sync::atomic::Ordering::Relaxed) {
        1 => ServeDecision::ServedByEhdb.outcome_label(),
        2 => ServeDecision::ServedByIncumbent {
            reason: DemoteReason::NotPrimary,
        }
        .outcome_label(),
        3 => ServeDecision::ServedByIncumbent {
            reason: DemoteReason::NoDurableService,
        }
        .outcome_label(),
        4 => ServeDecision::ServedByIncumbent {
            reason: DemoteReason::ParityDiverged,
        }
        .outcome_label(),
        _ => "unknown",
    }
}

/// Reset the logged serve state.  Tests only — the transition logger is
/// process-global, so a test that asserts a transition must start from a known
/// state or it depends on which test ran first.
#[cfg(test)]
pub(crate) fn reset_serve_state_for_test() {
    LAST_SERVE_STATE.store(0, std::sync::atomic::Ordering::Relaxed);
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
    fn shadow_mirror_durable_backend_lands_in_segments() {
        // Full mirror_event path with the durable segment backend selected: each
        // event mirrors + holds parity, and the events land in durable segments
        // (not the JSONL log), independently reopened via the backend module.
        let (log, dir) = tmp_log("shadow-durable");
        let mut e = worker_env(log.to_str().unwrap(), "shadow");
        e.insert(
            "NOETL_EHDB_EVENTLOG_BACKEND".to_string(),
            "durable_segment".to_string(),
        );
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
        // The durable segments hold all three; the JSONL log was never written.
        let contract = contract_from_env(&e).unwrap();
        let count = crate::ehdb::eventlog_backend::durable_shard_record_count(&e, &contract, "100")
            .unwrap();
        assert_eq!(
            count, 3,
            "durable segments replay all three appended events"
        );
        assert!(!log.exists(), "durable backend never writes the JSONL log");
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
    fn primary_without_a_durable_service_is_unavailable_not_served() {
        // CONTRACT CHANGE (ai-meta#257 PR 6).  This previously asserted
        // `ServedPrimary`: `primary` + parity was treated as sufficient to serve
        // authoritatively from a POD-LOCAL log while the incumbent held all
        // history — authoritative in name only, and the failure the RFC exists
        // to prevent.  Serving now also requires a reachable durable tier
        // service; none is configured here, so PrimaryUnavailable is correct.
        // The append still happens and parity is still computed — what changed
        // is that EHDB no longer CLAIMS to have served it.  No tier is `primary`
        // in any environment, so no deployed behaviour changes.
        let (log, dir) = tmp_log("primary");
        let e = worker_env(log.to_str().unwrap(), "primary");
        // Phase 9 tier 1: primary is activated, so a primary append is served
        // authoritatively by EHDB (not refused).  Global seq 1, parity holds.
        let r = mirror_event(&e, "100", Some(1), "evt", &Default::default(), false);
        assert_eq!(r.mode, EventLogMode::Primary);
        assert_eq!(r.outcome, EventLogOutcome::PrimaryUnavailable);
        assert_eq!(r.global_sequence, Some(1));
        assert!(r.parity.as_ref().unwrap().holds());
        // Compile-time invariant: ServedPrimary is only reachable with the flag on.
        const _: () = assert!(PRIMARY_SERVE_ACTIVATED);
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

    // --- Live event-append hook (runtime integration, noetl/ehdb#234) ---

    #[test]
    fn runtime_hook_env_arms_only_for_enabled_shadow_data_plane() {
        // Enabled + shadow + worker role + log ⇒ armed.
        let armed = runtime_hook_env(&worker_env("/tmp/hook.jsonl", "shadow"));
        assert!(armed.is_some(), "shadow+enabled worker must arm the hook");
    }

    #[test]
    fn runtime_hook_env_noop_when_disabled() {
        // Umbrella switch off ⇒ no hook even though the tier says shadow.
        let e: EnvMap = [("NOETL_EHDB_EVENTLOG", "shadow")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert!(runtime_hook_env(&e).is_none());
    }

    // ======================================================================
    // The service-resolved serve site (ai-meta#257 P0).
    //
    // These are the permanent guard for the defect: on the composed
    // serve-ready configuration `primary` reached NO caller of
    // `primary_serve::decide`, so the flip was inert AND silent while 13
    // events per execution flowed correctly. Every test below asserts a
    // property of the third call site — the one that configuration reaches.
    // ======================================================================

    /// The tier service's real acknowledgement shape, for one append.
    fn ack_body(seq: u64, count: Option<u64>) -> String {
        match count {
            Some(c) => {
                format!(r#"{{"appended":true,"global_sequence":{seq},"log_record_count":{c}}}"#)
            }
            None => format!(r#"{{"appended":true,"global_sequence":{seq}}}"#),
        }
    }

    #[test]
    fn service_append_serves_primary_when_all_three_conditions_hold() {
        // THE P0, as an assertion. `primary` + a reachable durable service +
        // parity holding must produce `served_primary` on the service-resolved
        // path — the path `MIRROR_SOURCE=server` + `TIER_QUERY_SOURCE=service`
        // takes, and the one that previously recorded nothing at all.
        let e = worker_env("/tmp/svc-serve.jsonl", "primary");
        let body = ack_body(7, Some(7));
        let s = serve_service_append_with(&e, Ok(&body), 6, 0.001, true);
        assert_eq!(
            s.outcome,
            EventLogOutcome::ServedPrimary,
            "the composed configuration must reach the serve decision: {s:?}"
        );
        assert!(s.decision.served_by_ehdb());
        assert!(!s.decision.degraded());
        assert_eq!(s.sequence, Some(7));
        assert_eq!(s.decision.outcome_label(), "served_primary");
    }

    #[test]
    fn service_append_never_serves_without_a_reachable_durable_service() {
        // The failure the whole RFC exists to prevent: a tier claiming to be
        // authoritative with nothing authoritative behind it. Same inputs as
        // above with reachability false.
        let e = worker_env("/tmp/svc-unreach.jsonl", "primary");
        let body = ack_body(7, Some(7));
        let s = serve_service_append_with(&e, Ok(&body), 6, 0.001, false);
        assert_eq!(s.outcome, EventLogOutcome::PrimaryUnavailable);
        assert!(!s.decision.served_by_ehdb(), "must not serve: {s:?}");
        assert!(
            s.decision.degraded(),
            "asking for primary and not getting it is degraded"
        );
        assert_eq!(s.decision.outcome_label(), "no_durable_service");
        assert!(
            s.detail
                .is_some_and(|d| d.contains("nothing authoritative")),
            "the demote must say why"
        );
    }

    #[test]
    fn service_append_demotes_on_divergence_rather_than_serving_or_erroring() {
        let e = worker_env("/tmp/svc-div.jsonl", "primary");
        // Gapless invariant broken: the store holds 4 records but assigned 7.
        let body = ack_body(7, Some(4));
        let s = serve_service_append_with(&e, Ok(&body), 6, 0.001, true);
        assert_eq!(s.outcome, EventLogOutcome::PrimaryDivergence);
        assert!(!s.decision.served_by_ehdb());
        assert!(s.decision.degraded());
        assert_eq!(s.decision.outcome_label(), "parity_diverged");
        assert!(s.detail.is_some_and(|d| d.contains("count divergence")));

        // Ordering is checked too, and batch-locally: a sequence that did not
        // advance within one batch is a divergence.
        let flat = ack_body(6, Some(6));
        let s2 = serve_service_append_with(&e, Ok(&flat), 6, 0.001, true);
        assert_eq!(s2.outcome, EventLogOutcome::PrimaryDivergence);
        assert!(s2.detail.is_some_and(|d| d.contains("ordering divergence")));
    }

    #[test]
    fn service_append_in_shadow_mirrors_and_never_claims_to_serve() {
        // The flip must be the ONLY difference. Same reachable service, same
        // parity, `shadow` instead of `primary`.
        let e = worker_env("/tmp/svc-shadow.jsonl", "shadow");
        let body = ack_body(3, Some(3));
        let s = serve_service_append_with(&e, Ok(&body), 2, 0.001, true);
        assert_eq!(s.outcome, EventLogOutcome::Mirrored);
        assert!(!s.decision.served_by_ehdb());
        assert!(!s.decision.degraded(), "non-primary is not degraded");
        assert_eq!(s.decision.outcome_label(), "not_primary");
    }

    #[test]
    fn service_append_off_and_disabled_record_nothing() {
        // Default-off, byte-identical: this site must never be the reason the
        // event-log family exists.
        let off = worker_env("/tmp/svc-off.jsonl", "off");
        let body = ack_body(1, Some(1));
        assert_eq!(
            serve_service_append_with(&off, Ok(&body), 0, 0.0, true).outcome,
            EventLogOutcome::Disabled
        );

        let mut disabled = worker_env("/tmp/svc-dis.jsonl", "primary");
        disabled.insert("NOETL_EHDB_ENABLED".to_string(), "false".to_string());
        assert_eq!(
            serve_service_append_with(&disabled, Ok(&body), 0, 0.0, true).outcome,
            EventLogOutcome::Disabled,
            "the umbrella switch must dominate the tier mode"
        );
    }

    #[test]
    fn a_record_that_did_not_land_is_degraded_and_demotes_visibly() {
        // Arm E: the tier service dies under `primary`. The serve signal must not
        // simply STOP — that is indistinguishable from no traffic. A transport
        // failure records on a degraded label and demotes.
        let e = worker_env("/tmp/svc-dead.jsonl", "primary");
        let s = serve_service_append_with(&e, Err("connect refused"), 0, 0.05, false);
        assert_eq!(s.outcome, EventLogOutcome::Unavailable);
        assert!(!s.decision.served_by_ehdb());
        assert!(s.decision.degraded());
        assert_eq!(s.sequence, None, "nothing landed, so there is no sequence");
        assert!(s.detail.is_some_and(|d| d.contains("connect refused")));
    }

    #[test]
    fn a_rejected_record_does_not_demote_the_whole_tier() {
        // `reachability`'s distinction, enforced at this site: a service that
        // refuses ONE record is reachable and healthy. Demoting for a poisoned
        // payload would let one record disable authoritative serving platform-wide,
        // so `invalid` is not degraded — while a store fault is.
        let e = worker_env("/tmp/svc-rej.jsonl", "primary");
        let rejected = serve_service_append_with(&e, Ok("invalid payload is empty"), 0, 0.0, true);
        assert_eq!(rejected.outcome, EventLogOutcome::Rejected);
        assert!(
            !rejected.outcome.degraded(),
            "a refused record is not a degraded tier"
        );

        for body in ["unavailable no tier store configured", "error disk full"] {
            let s = serve_service_append_with(&e, Ok(body), 0, 0.0, true);
            assert_eq!(
                s.outcome,
                EventLogOutcome::Unavailable,
                "{body:?} is the store failing, which IS degraded"
            );
            assert!(s.outcome.degraded());
        }
    }

    #[test]
    fn an_ack_without_a_record_count_verifies_by_ordering_alone_and_says_so() {
        // Rollout skew: the writer predates `log_record_count`. Parity degrades to
        // the ordering check rather than being reported as a divergence — an
        // unverifiable check must not be labelled a failed one, and the skip is
        // named in the detail rather than being silent.
        let e = worker_env("/tmp/svc-noc.jsonl", "primary");
        let body = ack_body(9, None);
        let s = serve_service_append_with(&e, Ok(&body), 8, 0.001, true);
        assert_eq!(s.outcome, EventLogOutcome::ServedPrimary);
        assert!(
            s.detail.is_some_and(|d| d.contains("no log_record_count")),
            "which checks ran must be reported, not assumed"
        );
    }

    #[test]
    fn the_serve_site_shares_one_policy_with_the_local_path() {
        // The property that keeps the two sites from drifting: for every one of
        // the eight input combinations, this site's serve verdict is exactly
        // `primary_serve::decide`'s. If someone adds a fourth condition here, or
        // relaxes one, this fails.
        for is_primary in [false, true] {
            for reachable in [false, true] {
                for parity in [false, true] {
                    let e = worker_env(
                        "/tmp/svc-table.jsonl",
                        if is_primary { "primary" } else { "shadow" },
                    );
                    // parity holds ⇒ count == sequence; diverges ⇒ it does not.
                    let body = ack_body(5, Some(if parity { 5 } else { 2 }));
                    let s = serve_service_append_with(&e, Ok(&body), 4, 0.0, reachable);
                    let want = super::super::primary_serve::decide(is_primary, reachable, parity);
                    assert_eq!(
                        s.decision, want,
                        "({is_primary},{reachable},{parity}) diverged from the shared policy"
                    );
                    assert_eq!(
                        s.decision.served_by_ehdb(),
                        is_primary && reachable && parity,
                        "exactly one combination may serve"
                    );
                }
            }
        }
    }

    #[test]
    fn the_flip_is_never_silent_in_either_direction() {
        // The second half of the P0: at flip time the pods emitted NO line about
        // the tier being primary — not even a wrong one. The transition logger is
        // process-global, so the assertion here is on the state it exposes, which
        // is the same value the reply body and the log line carry.
        reset_serve_state_for_test();
        assert_eq!(
            current_serve_state(),
            "unknown",
            "a process that has decided nothing must say so, not report `not_primary`"
        );

        let e = worker_env("/tmp/svc-signal.jsonl", "primary");
        let body = ack_body(2, Some(2));
        serve_service_append_with(&e, Ok(&body), 1, 0.0, true);
        assert_eq!(current_serve_state(), "served_primary");

        // Demote: the state must move back. A demote that stayed silent because
        // the promote was already logged is how an outage hides.
        serve_service_append_with(&e, Err("connect refused"), 2, 0.0, false);
        assert_eq!(current_serve_state(), "no_durable_service");

        // And re-promote on a real success, with no restart and no timer.
        let body2 = ack_body(3, Some(3));
        serve_service_append_with(&e, Ok(&body2), 2, 0.0, true);
        assert_eq!(current_serve_state(), "served_primary");
        reset_serve_state_for_test();
    }

    #[test]
    fn the_pinned_serve_outcomes_cover_every_outcome_this_site_records() {
        // The drift check from `representation-drift.md`: a pinned label set that
        // omits one value reintroduces the absent-series bug on that value alone,
        // while the rest read 0 and look complete. Enumerate what this site can
        // record and require the pin to contain it.
        let recorded = [
            EventLogOutcome::ServedPrimary,
            EventLogOutcome::PrimaryUnavailable,
            EventLogOutcome::PrimaryDivergence,
            EventLogOutcome::Mirrored,
            EventLogOutcome::ParityMismatch,
            EventLogOutcome::Rejected,
            EventLogOutcome::Unavailable,
        ];
        for outcome in recorded {
            assert!(
                super::super::metrics::eventlog_serve_outcome_is_pinned(outcome.as_str()),
                "{} is recorded by serve_service_append but not pinned — it would be ABSENT \
                 until it first fires, which reads as 'this build has no serve path'",
                outcome.as_str()
            );
        }
    }

    #[test]
    fn runtime_hook_env_off_disarms_but_primary_keeps_verifying() {
        // `off` is the only mode that disarms the live mirror.
        assert!(
            runtime_hook_env(&worker_env("/tmp/hook.jsonl", "off")).is_none(),
            "`off` must be a strict no-op"
        );

        // `primary` MUST keep the mirror armed.  It previously returned `None`,
        // which silently disarmed verification while serving nothing in its
        // place — no runtime path calls `serve_primary_cycle` (noetl/ai-meta#247).
        // Selecting a stronger mode must never reduce verification.
        assert!(
            runtime_hook_env(&worker_env("/tmp/hook.jsonl", "primary")).is_some(),
            "`primary` must keep mirroring — it must not silently disarm verification"
        );

        // Control: `shadow` is unchanged by this fix.
        assert!(
            runtime_hook_env(&worker_env("/tmp/hook.jsonl", "shadow")).is_some(),
            "`shadow` behaviour must be unchanged"
        );
    }

    #[test]
    fn runtime_hook_env_skips_control_plane_role() {
        // A control-plane role must never arm the live mirror.
        for role in ["server", "gateway", "api"] {
            let e: EnvMap = [
                ("NOETL_EHDB_ENABLED", "true"),
                ("NOETL_EHDB_MODE", "local_reference"),
                ("NOETL_EHDB_CLIENT_ROLE", role),
                ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
                ("NOETL_EHDB_EVENTLOG", "shadow"),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
            assert!(
                runtime_hook_env(&e).is_none(),
                "control-plane role {role} must not arm the hook"
            );
        }
    }

    #[test]
    fn mirror_live_event_fires_on_shadow_enabled() {
        let (log, dir) = tmp_log("live-fire");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        // A real (long) numeric execution id, mirrored via the runtime hook.
        let outcome = mirror_live_event(
            &e,
            "478775660589088776",
            "{\"event_type\":\"call.done\"}",
            None,
        )
        .outcome;
        assert_eq!(outcome, EventLogOutcome::Mirrored);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // --- noetl/ai-meta#258: authoritative-id reconciliation ------------------

    #[test]
    fn no_authoritative_id_is_reported_not_assumed() {
        let (p, v) = reconcile_authoritative_id("{\"event_type\":\"x\"}", None, true);
        assert_eq!(v, AuthoritativeIdVerdict::NotSupplied);
        assert_eq!(p, "{\"event_type\":\"x\"}", "payload must be untouched");
    }

    #[test]
    fn a_producer_stamped_id_is_never_overwritten() {
        // The load-bearing property. A stamp that overwrote the producer's id
        // would make the stores agree by construction — converting a real
        // divergence into a silent correction, which is the one thing a parity
        // mechanism must not do.
        let (p, v) = reconcile_authoritative_id("{\"event_id\":11}", Some(22), true);
        assert_eq!(v, AuthoritativeIdVerdict::Disagreed);
        let parsed: serde_json::Value = serde_json::from_str(&p).unwrap();
        assert_eq!(parsed["event_id"], 11, "the producer's id must survive");
    }

    #[test]
    fn agreement_is_distinguished_from_a_stamp() {
        let (_, v) = reconcile_authoritative_id("{\"event_id\":7}", Some(7), true);
        assert_eq!(v, AuthoritativeIdVerdict::Agreed);
    }

    #[test]
    fn a_missing_id_is_stamped_only_when_armed() {
        let payload = "{\"event_type\":\"step.enter\"}";

        let (off, v_off) = reconcile_authoritative_id(payload, Some(42), false);
        assert_eq!(v_off, AuthoritativeIdVerdict::Unstamped);
        assert_eq!(off, payload, "flag-off must be byte-identical");

        let (on, v_on) = reconcile_authoritative_id(payload, Some(42), true);
        assert_eq!(v_on, AuthoritativeIdVerdict::Stamped);
        let parsed: serde_json::Value = serde_json::from_str(&on).unwrap();
        assert_eq!(parsed["event_id"], 42);
        assert_eq!(
            parsed["event_type"], "step.enter",
            "stamping must not disturb the rest of the event"
        );
    }

    #[test]
    fn a_stringified_producer_id_is_compared_not_ignored() {
        // The two producers spell event_id differently; reading only one
        // spelling would report every event from the other as a disagreement.
        let (_, v) = reconcile_authoritative_id("{\"event_id\":\"7\"}", Some(7), true);
        assert_eq!(v, AuthoritativeIdVerdict::Agreed);
    }

    #[test]
    fn a_non_json_payload_cannot_be_reconciled_and_says_so() {
        let (p, v) = reconcile_authoritative_id("not json", Some(1), true);
        assert_eq!(v, AuthoritativeIdVerdict::NotJson);
        assert_eq!(p, "not json");
    }

    #[test]
    fn the_stamped_payload_is_what_gets_mirrored() {
        // The returned payload is fed to the tier service as well as the local
        // log, so it has to BE the appended bytes, not a copy of the input.
        let (log, dir) = tmp_log("live-stamp");
        let mut e = worker_env(log.to_str().unwrap(), "shadow");
        e.insert(AUTHORITATIVE_ID_STAMP_ENV.to_string(), "true".to_string());
        let m = mirror_live_event(&e, "100", "{\"event_type\":\"x\"}", Some(4242));
        assert_eq!(m.outcome, EventLogOutcome::Mirrored);
        assert_eq!(m.id_verdict, AuthoritativeIdVerdict::Stamped);
        assert!(
            m.payload.contains("4242"),
            "returned payload must carry the stamp: {}",
            m.payload
        );
        // Read it back through the driver rather than grepping the JSONL: the
        // log stores the payload as a byte array, so a text search over the file
        // would match the digits of an unrelated field and pass without the
        // stamp ever having landed.
        let driver = LocalReferenceEventLogDriver::new(
            log.clone(),
            DEFAULT_LOCAL_REFERENCE_TENANT.to_string(),
            DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string(),
        );
        let out = driver
            .read_execution(&ehdb_reference::EventLogReadExecutionRequest {
                execution_id: "100".to_string(),
                after: None,
                limit: 10,
            })
            .unwrap();
        assert_eq!(out.record_count, 1);
        let stored: serde_json::Value = serde_json::from_str(&out.records[0].payload).unwrap();
        assert_eq!(
            stored["event_id"], 4242,
            "the stamped id must be what reached the store"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn mirror_live_event_is_noop_when_disabled() {
        // No EHDB env at all ⇒ Disabled (records no metric, real path untouched).
        let e: EnvMap = EnvMap::new();
        let outcome = mirror_live_event(&e, "100", "{\"seq\":1}", None).outcome;
        assert_eq!(outcome, EventLogOutcome::Disabled);
    }

    #[test]
    fn mirror_live_event_skipped_for_control_plane_role() {
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
        // Even if the hook were called directly, the guard refuses the write.
        let outcome = mirror_live_event(&e, "100", "{\"seq\":1}", None).outcome;
        assert_eq!(outcome, EventLogOutcome::GuardRefused);
    }

    #[test]
    fn mirror_live_event_isolates_engine_error_without_propagating() {
        // Point the log at a path whose parent is a *file*, so the engine cannot
        // create/append the log.  The mirror must return an outcome (Unavailable)
        // rather than panicking / propagating — proving the real event path is
        // never broken by a mirror failure.
        let (file_as_dir, dir) = tmp_log("iso");
        std::fs::write(&file_as_dir, b"x").unwrap(); // now a regular file
        let bad_log = file_as_dir.join("nested").join("log.jsonl");
        let e = worker_env(bad_log.to_str().unwrap(), "shadow");
        let outcome = mirror_live_event(&e, "100", "{\"seq\":1}", None).outcome;
        assert!(
            matches!(
                outcome,
                EventLogOutcome::Unavailable | EventLogOutcome::Invalid
            ),
            "engine error must be contained as an outcome, got {outcome:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
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

#[cfg(test)]
mod tier_dedupe_parity_tests {
    //! **noetl/ai-meta#313 Deploy B** — the parity skip must fire ONLY for genuine
    //! dedupes, and must never mask a real divergence.
    //!
    //! The tier-append reply is checked for two invariants: strictly increasing
    //! sequences, and `log_record_count == global_sequence`. A deduplicated append
    //! satisfies **neither** — it returns the position of a record already there,
    //! and the count did not advance. So the skip is necessary; and because it
    //! suppresses both checks, it is also the most dangerous thing in this change.
    //! Every test here exists to bound it.
    use super::ServiceAppendAck;

    fn reply(seq: u64, count: Option<u64>, deduplicated: bool) -> String {
        let mut v = serde_json::json!({"appended": true, "global_sequence": seq});
        if let Some(c) = count {
            v["log_record_count"] = serde_json::json!(c);
        }
        if deduplicated {
            v["deduplicated"] = serde_json::json!(true);
        }
        v.to_string()
    }

    fn parse(body: &str) -> ServiceAppendAck {
        ServiceAppendAck::parse(body).expect("an appended reply must parse")
    }

    /// ⭐ A genuine dedupe does not trip parity, and says so by name.
    ///
    /// ⚠ Mutation verified: removing the `if self.deduplicated` early return makes
    /// this fail on the ordering check — the false divergence the skip prevents.
    #[test]
    fn a_deduplicated_reply_does_not_trip_parity_and_is_named() {
        let r = parse(&reply(7, Some(42), true));
        let verdict = r.parity(41).expect("a dedupe must not be a divergence");
        let note = verdict.expect("the skip must be NAMED, never a silent Ok(None)");
        assert!(
            note.contains("deduplicated"),
            "the outcome must say why parity was not applied: {note}"
        );
    }

    /// ⚠⚠ THE NEGATIVE CONTROL — a REAL divergence still trips.
    ///
    /// Without this, a skip that fired unconditionally would pass the test above
    /// perfectly while silencing every divergence the tier could report. That is
    /// strictly worse than the duplicates this removes: a rewound or forgetful
    /// `primary` store would look healthy.
    #[test]
    fn a_real_divergence_still_trips_when_not_deduplicated() {
        let backwards = parse(&reply(7, Some(7), false));
        let err = backwards
            .parity(41)
            .expect_err("a non-deduplicated backwards sequence is a real divergence");
        assert!(err.contains("ordering divergence"), "{err}");

        let gappy = parse(&reply(42, Some(40), false));
        let err = gappy
            .parity(41)
            .expect_err("a non-deduplicated count mismatch is a real divergence");
        assert!(err.contains("count divergence"), "{err}");
    }

    /// ⚠ The skip is keyed on the STORE's claim, never inferred from the numbers.
    ///
    /// A reply that merely *looks* like a dedupe — older sequence, unchanged count
    /// — but carries no flag must still be a divergence. Inferring it from shape
    /// would silence exactly the case the check exists for: a rewound store looks
    /// identical to a dedupe.
    #[test]
    fn a_dedupe_shaped_reply_without_the_flag_is_still_a_divergence() {
        let looks_like_one = parse(&reply(7, Some(42), false));
        looks_like_one
            .parity(41)
            .expect_err("without the flag this is a rewound store, not a dedupe");
    }

    /// A writer predating the field parses as not-deduplicated — the safe
    /// direction. An old writer must never claim a dedupe it did not perform.
    #[test]
    fn a_reply_without_the_field_is_not_deduplicated() {
        let legacy = parse(&reply(42, Some(42), false));
        assert!(!legacy.deduplicated);
        assert!(
            legacy.parity(41).is_ok(),
            "a healthy legacy reply still passes"
        );
    }

    /// A normal append is unaffected: both checks run and pass.
    #[test]
    fn a_normal_append_still_runs_both_checks() {
        let ok = parse(&reply(42, Some(42), false));
        assert_eq!(ok.parity(41).expect("healthy"), None, "no skip, no note");
    }
}
