//! Projection / read-model SHADOW wiring + PRIMARY-serve cutover (EHDB Phase 7
//! shadow, Phase 9 tier-2 primary).
//!
//! EHDB's projection core engine (the `ehdb_reference::projection` driver,
//! ehdb#243) is the read-model builder that Phase 7 puts *on top of* the Phase-6
//! event-log tail, in place of the **PostgreSQL materializer** that today folds
//! `noetl.event` into projected state (the `noetl.event` projection, the
//! per-execution `projection_snapshot`, and the durable consumer offset).  This
//! module is the worker's **driver-selection seam** for that engine, gated by
//! `NOETL_EHDB_PROJECTION`:
//!
//! * `off` (default) — strict no-op.  No engine is opened, no metric recorded;
//!   the worker's `/metrics` and behaviour are byte-identical to a build without
//!   the projection wiring.
//! * `shadow` — **dual-materialize + compare, never serve.**  A batch of events
//!   (typically the Phase-6 event-log tail) is *also* materialized into the EHDB
//!   projection read-models alongside the authoritative Postgres materializer,
//!   and the EHDB read-models are compared against the materializer's observed
//!   output for key / value / ordering parity + checkpoint lag.  Reads are
//!   **never** served from EHDB and the authoritative materializer is untouched.
//! * `primary` — **EHDB serves the read-models authoritatively** (Phase 9 tier
//!   2): the read-model queries the control plane makes (`list_executions`,
//!   per-execution `read_execution_state`, `read_event`) are served by the EHDB
//!   engine in place of the PostgreSQL materializer, while the served read-models
//!   are dual-run parity-checked against the incumbent.  [`PRIMARY_SERVE_ACTIVATED`]
//!   is now `true` so this build *can* serve primary; whether it *does* is a pure
//!   runtime choice of the `NOETL_EHDB_PROJECTION` flag (see reversibility).
//!
//! ## Reversibility (the cutover safety property)
//!
//! The cutover is reversible with **two independent levers**:
//!
//! 1. **Runtime flag (operational, instant, no redeploy)** — flip
//!    `NOETL_EHDB_PROJECTION` from `primary` back to `shadow`/`off` and the
//!    incumbent (PostgreSQL materializer) is the authoritative read path again
//!    immediately.  Zero data loss: the primary path only ever materializes into
//!    the derived EHDB `KeepAll` projection store by consuming already-authored
//!    events and never mutates/deletes anything the incumbent owns, so the
//!    incumbent read-models are exactly as they were, and the EHDB store stays
//!    whole on disk for a later re-enable.
//! 2. **Compile-time kill switch (structural, belt-and-suspenders)** — set
//!    [`PRIMARY_SERVE_ACTIVATED`] back to `false` and it is structurally
//!    impossible for the build to serve primary regardless of config.
//!
//! ## Boundaries (mirror the rest of `src/ehdb`)
//!
//! * Disabled-by-default no-op (byte-identical `/metrics`).
//! * Control-plane roles (`gateway`/`api`/`server`) refused before any engine
//!   opens — the gateway never touches the data plane.
//! * Bounded (apply-batch cap) + stateless (engine opened + dropped per apply).
//! * **Event-log-authoritative / read-model-derived** — a projection is a
//!   *derived read-model* built by consuming already-authored events; this module
//!   never authors a NoETL event, never reaches `noetl.event` /
//!   `POST /api/events`, and never writes to the authoritative Postgres
//!   materializer (structurally asserted — it only touches the derived EHDB
//!   projection fabric via `ehdb_reference`).

use ehdb_reference::projection::exercise_primary_serve;
use ehdb_reference::{
    compare_projection_parity, AuthoritativeExecutionState, ExecutionStateView,
    LocalReferenceProjectionEngine, ProjectionApplyRequest, ProjectionDriver, ProjectionEventInput,
    ProjectionParityReport, ProjectionPrimaryInput, ProjectionPrimaryServeReport,
    DEFAULT_LOCAL_REFERENCE_NAMESPACE, DEFAULT_LOCAL_REFERENCE_TENANT,
};

use super::contract::{contract_from_env, EhdbClientRole, EhdbContract, EHDB_ENABLED_ENV};
use super::guard::assert_data_plane_access_allowed;
use super::{metrics, EnvMap};
use crate::snowflake::SnowflakeGen;
use std::sync::OnceLock;

/// The driver-selection flag for the projection tier.
pub const PROJECTION_MODE_ENV: &str = "NOETL_EHDB_PROJECTION";
/// Event-count cap for one shadow apply batch.
pub const MAX_BATCH_ENV: &str = "NOETL_EHDB_PROJECTION_MAX_BATCH";
const DEFAULT_MAX_BATCH: usize = 1_024;
/// Hard ceiling — the crate engine rejects a batch above `MAX_APPLY_BATCH`
/// (4096), so the worker-side clamp never exceeds it.
const MAX_BATCH_CEILING: usize = 4_096;
/// Default consumer identity for the projector checkpoint.
const DEFAULT_CONSUMER: &str = "noetl-projection-shadow";
/// Bound on how many execution-state rows the shadow reads back for the parity
/// comparison — the projection read is bounded like every other EHDB op.
const READBACK_LIMIT: usize = 4_096;

/// Compile-time kill switch for primary-serve.  Phase 9 tier 2 activates it
/// (`true`): this build *can* serve the projection read-models authoritatively
/// from EHDB.  Whether it *does* is the pure runtime choice of
/// `NOETL_EHDB_PROJECTION` (`primary` serves; `shadow`/`off` keep the PostgreSQL
/// materializer authoritative), so the cutover stays reversible without a
/// redeploy.  Setting this back to `false` is the belt-and-suspenders structural
/// rollback — it makes primary-serve unreachable regardless of config (the
/// `primary` flag then degrades to [`ProjectionOutcome::PrimaryUnavailable`]).
pub const PRIMARY_SERVE_ACTIVATED: bool = true;

/// Which projection engine the tier is driven by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionMode {
    /// No EHDB engine; the incumbent Postgres materializer is authoritative.
    Off,
    /// Dual-materialize into EHDB + compare; never serve reads from it.
    Shadow,
    /// Serve read-models from EHDB — recognised but not activated this session.
    Primary,
}

impl ProjectionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProjectionMode::Off => "off",
            ProjectionMode::Shadow => "shadow",
            ProjectionMode::Primary => "primary",
        }
    }

    /// Parse the mode from the env, defaulting to `Off`.  An unrecognised value
    /// is treated as `Off` (fail-safe: an unknown driver never materializes).
    pub fn from_env(env: &EnvMap) -> Self {
        match env
            .get(PROJECTION_MODE_ENV)
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("shadow") => ProjectionMode::Shadow,
            Some("primary") => ProjectionMode::Primary,
            _ => ProjectionMode::Off,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionOutcome {
    /// Off mode / EHDB disabled — strict no-op.
    Disabled,
    /// Events materialized into the EHDB read-models and parity held.
    Materialized,
    /// Events materialized but the EHDB read-models diverged from the
    /// authoritative materializer.
    ParityMismatch,
    /// `primary` served the read-models authoritatively from EHDB + dual-run
    /// parity against the incumbent materializer held.
    ServedPrimary,
    /// `primary` served the read-models from EHDB but the dual-run parity against
    /// the incumbent diverged (degraded — surfaces on `last_degraded`).
    PrimaryDivergence,
    /// `primary` requested but primary-serve is not activated this build (the
    /// compile-time kill switch is off).
    PrimaryUnavailable,
    /// Empty batch or a batch over the count cap.
    Rejected,
    /// A control-plane role reached the data-plane engine — refused.
    GuardRefused,
    /// Caller mistake (bad execution id / config).
    Invalid,
    /// The engine errored at runtime.
    Unavailable,
}

impl ProjectionOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProjectionOutcome::Disabled => "disabled",
            ProjectionOutcome::Materialized => "materialized",
            ProjectionOutcome::ParityMismatch => "parity_mismatch",
            ProjectionOutcome::ServedPrimary => "served_primary",
            ProjectionOutcome::PrimaryDivergence => "primary_divergence",
            ProjectionOutcome::PrimaryUnavailable => "primary_unavailable",
            ProjectionOutcome::Rejected => "rejected",
            ProjectionOutcome::GuardRefused => "guard_refused",
            ProjectionOutcome::Invalid => "invalid",
            ProjectionOutcome::Unavailable => "unavailable",
        }
    }

    pub fn ok(&self) -> bool {
        matches!(
            self,
            ProjectionOutcome::Disabled
                | ProjectionOutcome::Materialized
                | ProjectionOutcome::ServedPrimary
        )
    }

    /// A degraded (but non-fatal) outcome — surfaces on the `last_degraded`
    /// gauge so a divergence or engine hiccup is visible without failing the
    /// authoritative path.
    fn degraded(&self) -> bool {
        matches!(
            self,
            ProjectionOutcome::ParityMismatch
                | ProjectionOutcome::PrimaryDivergence
                | ProjectionOutcome::Unavailable
        )
    }
}

/// Secret-free result of one shadow projection apply.
#[derive(Debug, Clone)]
pub struct ProjectionResult {
    pub mode: ProjectionMode,
    pub outcome: ProjectionOutcome,
    pub role: Option<EhdbClientRole>,
    pub duration_seconds: f64,
    pub detail: Option<String>,
    /// New read-model rows materialized this apply (present on a successful
    /// materialize).
    pub applied: Option<usize>,
    /// The EHDB projector's applied-through global sequence after this apply.
    pub checkpoint: Option<u64>,
    /// The parity verdict against the authoritative materializer (present when a
    /// shadow apply ran).
    pub parity: Option<ProjectionParityReport>,
}

#[derive(Debug, Clone, Default)]
pub struct ProjectionOptions {
    pub tenant: Option<String>,
    pub namespace: Option<String>,
    pub consumer: Option<String>,
    pub transaction_id: Option<String>,
}

fn txn_gen() -> &'static SnowflakeGen {
    static GEN: OnceLock<SnowflakeGen> = OnceLock::new();
    GEN.get_or_init(|| SnowflakeGen::from_env_or_hint("ehdb-proj"))
}

fn new_transaction_id() -> String {
    format!("ehdbproj-{}", txn_gen().next_id())
}

fn truthy(env: &EnvMap, key: &str) -> bool {
    matches!(
        env.get(key)
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref(),
        Some("1" | "true" | "yes" | "y" | "on")
    )
}

fn bounded_max_batch(env: &EnvMap) -> usize {
    env.get(MAX_BATCH_ENV)
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_BATCH)
        .clamp(1, MAX_BATCH_CEILING)
}

/// Build a result (and record its metric).  `applied` / `checkpoint` / `parity`
/// are set by the success path afterward — the early-exit paths leave them
/// `None`.
fn make_result(
    mode: ProjectionMode,
    outcome: ProjectionOutcome,
    role: Option<EhdbClientRole>,
    started: std::time::Instant,
    detail: Option<String>,
    record_metrics: bool,
) -> ProjectionResult {
    let duration_seconds = started.elapsed().as_secs_f64();
    if record_metrics {
        metrics::record_projection(
            "materialize",
            outcome.as_str(),
            outcome.ok(),
            outcome.degraded(),
            duration_seconds,
        );
    }
    ProjectionResult {
        mode,
        outcome,
        role,
        duration_seconds,
        detail,
        applied: None,
        checkpoint: None,
        parity: None,
    }
}

/// Classified by the crate error's Display since the crate does not re-export its
/// error enum: an identifier validation failure is a caller mistake (`Invalid`),
/// an over-cap batch is a caller `Rejected`, any other runtime error is
/// `Unavailable`.
fn classify_helper_error<E: std::fmt::Display>(err: &E) -> ProjectionOutcome {
    let msg = err.to_string();
    if msg.starts_with("invalid identifier") {
        ProjectionOutcome::Invalid
    } else if msg.contains("exceeds bound") {
        ProjectionOutcome::Rejected
    } else {
        ProjectionOutcome::Unavailable
    }
}

fn resolve_contract(
    env: &EnvMap,
    mode: ProjectionMode,
    started: std::time::Instant,
    record_metrics: bool,
) -> Result<EhdbContract, Box<ProjectionResult>> {
    let finish =
        |outcome: ProjectionOutcome, role: Option<EhdbClientRole>, detail: Option<String>| {
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
                ProjectionOutcome::GuardRefused
            } else {
                ProjectionOutcome::Invalid
            };
            return Err(finish(outcome, role, Some(err.0)));
        }
    };

    if let Err(err) = assert_data_plane_access_allowed(contract.role, "materialize") {
        return Err(finish(
            ProjectionOutcome::GuardRefused,
            Some(contract.role),
            Some(err.to_string()),
        ));
    }
    if !contract.uses_local_reference_runtime() || contract.local_reference_log.is_none() {
        return Err(finish(
            ProjectionOutcome::Disabled,
            Some(contract.role),
            None,
        ));
    }
    Ok(contract)
}

/// Dual-materialize a batch of already-authored events (typically the Phase-6
/// event-log tail) into the EHDB projection read-models (shadow) and compare the
/// result against the authoritative Postgres materializer's observed output.
///
/// `authoritative` are the execution-state rows the incumbent materializer
/// produced for the same events (the shadow's parity ground truth), and
/// `authoritative_offset` is the incumbent's committed offset (highest global
/// sequence materialized) when known; `None` skips the checkpoint-lag check.
///
/// This NEVER serves reads to the control plane and NEVER authors a NoETL event
/// or touches the authoritative materializer — it only materializes into the
/// derived EHDB projection fabric and reports parity.
pub fn shadow_project(
    env: &EnvMap,
    events: &[ProjectionEventInput],
    authoritative: &[AuthoritativeExecutionState],
    authoritative_offset: Option<u64>,
    opts: &ProjectionOptions,
    record_metrics: bool,
) -> ProjectionResult {
    let started = std::time::Instant::now();
    let mode = ProjectionMode::from_env(env);

    // Off mode OR the umbrella EHDB switch disabled ⇒ strict no-op.
    if mode == ProjectionMode::Off || !truthy(env, EHDB_ENABLED_ENV) {
        return make_result(
            mode,
            ProjectionOutcome::Disabled,
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
    if mode == ProjectionMode::Primary && !PRIMARY_SERVE_ACTIVATED {
        let contract = match resolve_contract(env, mode, started, record_metrics) {
            Ok(c) => c,
            Err(result) => return *result,
        };
        return make_result(
            mode,
            ProjectionOutcome::PrimaryUnavailable,
            Some(contract.role),
            started,
            Some("projection primary read-serving is not activated in this build".to_string()),
            record_metrics,
        );
    }

    // Shadow (dual-materialize + compare) OR primary (EHDB serves the read-models
    // authoritatively).  The engine op is identical — an apply + read-back +
    // parity compare; the mode only changes which read path is authoritative and
    // how the outcome is labelled.
    let serving_primary = mode == ProjectionMode::Primary;
    let contract = match resolve_contract(env, mode, started, record_metrics) {
        Ok(c) => c,
        Err(result) => return *result,
    };

    let max_batch = bounded_max_batch(env);
    if events.is_empty() {
        return make_result(
            mode,
            ProjectionOutcome::Rejected,
            Some(contract.role),
            started,
            Some("empty projection batch".to_string()),
            record_metrics,
        );
    }
    if events.len() > max_batch {
        return make_result(
            mode,
            ProjectionOutcome::Rejected,
            Some(contract.role),
            started,
            Some(format!(
                "projection batch {} exceeds bound {max_batch}",
                events.len()
            )),
            record_metrics,
        );
    }

    let engine = LocalReferenceProjectionEngine::new(
        contract.local_reference_log.clone().expect("log present"),
        opts.tenant
            .clone()
            .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string()),
        opts.namespace
            .clone()
            .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string()),
    );

    let consumer = opts
        .consumer
        .clone()
        .unwrap_or_else(|| DEFAULT_CONSUMER.to_string());
    let request = ProjectionApplyRequest {
        consumer: consumer.clone(),
        transaction_id: opts
            .transaction_id
            .clone()
            .unwrap_or_else(new_transaction_id),
        events: events.to_vec(),
    };

    let apply = match engine.apply(&request) {
        Ok(outcome) => outcome,
        Err(err) => {
            return make_result(
                mode,
                classify_helper_error(&err),
                Some(contract.role),
                started,
                Some(err.to_string()),
                record_metrics,
            );
        }
    };

    // Read the EHDB read-models back (bounded) and compare against the
    // authoritative materializer's rows.  The read-back stays inside this module
    // — it is the shadow's parity input, NOT a read served to the control plane.
    let ehdb_states: Vec<ExecutionStateView> = match engine.list_executions(READBACK_LIMIT) {
        Ok(list) => list.states,
        Err(err) => {
            return make_result(
                mode,
                classify_helper_error(&err),
                Some(contract.role),
                started,
                Some(err.to_string()),
                record_metrics,
            );
        }
    };

    let report = compare_projection_parity(
        &ehdb_states,
        authoritative,
        apply.checkpoint.applied_through_sequence,
        authoritative_offset,
    );

    let result_outcome = match (serving_primary, report.holds()) {
        // Primary: EHDB served the read-models authoritatively.
        (true, true) => ProjectionOutcome::ServedPrimary,
        (true, false) => ProjectionOutcome::PrimaryDivergence,
        // Shadow: EHDB materialized alongside the authoritative incumbent.
        (false, true) => ProjectionOutcome::Materialized,
        (false, false) => ProjectionOutcome::ParityMismatch,
    };
    let mut result = make_result(
        mode,
        result_outcome,
        Some(contract.role),
        started,
        report.divergence.clone(),
        record_metrics,
    );
    result.applied = Some(apply.applied);
    result.checkpoint = Some(apply.checkpoint.applied_through_sequence);
    result.parity = Some(report);
    result
}

/// How many events the built-in primary-serve cycle drives through the engine
/// (materializing 2 executions: "100" completed/2, "200" running/1).
pub const PRIMARY_SERVE_CYCLE_EVENTS: usize = 3;
/// Execution rows served after the reversibility flip-back (the 2 cycle
/// executions + the 1 fresh execution the shadow flip-back materializes).
const PRIMARY_SERVE_ROWS_AFTER_REVERT: usize = 3;

fn cycle_event(
    global_sequence: u64,
    event_id: i64,
    exec: &str,
    event_type: &str,
    node: &str,
    status: &str,
) -> ProjectionEventInput {
    ProjectionEventInput {
        global_sequence,
        event_id,
        execution_id: exec.to_string(),
        event_type: event_type.to_string(),
        node_name: Some(node.to_string()),
        status: Some(status.to_string()),
        prev_event_id: None,
    }
}

fn cycle_auth(
    exec: &str,
    status: &str,
    event_count: usize,
    terminal: bool,
) -> AuthoritativeExecutionState {
    AuthoritativeExecutionState {
        execution_id: exec.to_string(),
        status: status.to_string(),
        event_count,
        terminal,
    }
}

/// Secret-free result of the authoritative projection primary-serve cycle
/// (Phase 9 tier 2) plus the operational reversibility demonstration.
#[derive(Debug, Clone)]
pub struct ProjectionServeResult {
    pub mode: ProjectionMode,
    pub outcome: ProjectionOutcome,
    pub role: Option<EhdbClientRole>,
    pub duration_seconds: f64,
    /// The EHDB engine served the whole cycle with the incumbent materializer's
    /// query contracts preserved and dual-run parity intact.
    pub served_by_ehdb: bool,
    /// The full served-by-EHDB proof (present once the cycle ran).
    pub report: Option<ProjectionPrimaryServeReport>,
    /// After serving primary, flipping `NOETL_EHDB_PROJECTION` back to `shadow`
    /// over the same store materialized a further execution and the read-models
    /// replayed whole — the incumbent read path is restored with zero data loss
    /// (rollback lever 1 demonstrated operationally).
    pub reversible: bool,
    /// The execution-row count served after the flip-back (== cycle executions + 1).
    pub rows_after_revert: usize,
    pub detail: Option<String>,
}

/// Drive the authoritative projection primary-serve cycle through the EHDB engine
/// and demonstrate operational reversibility.
///
/// In `primary` mode (and with [`PRIMARY_SERVE_ACTIVATED`]) this:
///
/// 1. runs [`exercise_primary_serve`] — apply (materialize) + the three
///    read-model query contracts (`list_executions`, per-execution
///    `read_execution_state`, `read_event`) + durable checkpoint + idempotent
///    re-apply + fresh-engine replay, all served authoritatively by EHDB,
///    dual-run parity-checked against the incumbent materializer; then
/// 2. flips `NOETL_EHDB_PROJECTION` back to `shadow` in a cloned env and
///    materializes a further execution over the SAME store, proving the
///    incumbent/shadow read path is restored and the store stays whole (zero data
///    loss on rollback).
///
/// Off/disabled ⇒ strict no-op (byte-identical `/metrics`).  Control-plane roles
/// are guard-refused before any engine opens.  Never authors a NoETL event or
/// writes the incumbent materializer — it only exercises the derived EHDB
/// projection fabric.
pub fn serve_primary_cycle(
    env: &EnvMap,
    opts: &ProjectionOptions,
    record_metrics: bool,
) -> ProjectionServeResult {
    let started = std::time::Instant::now();
    let mode = ProjectionMode::from_env(env);

    // Early-exit builder (no cycle report) that records the `primary_serve`
    // metric — `disabled` outcomes are skipped by `record_projection`, preserving
    // the byte-identical no-op invariant.
    let early = |outcome: ProjectionOutcome,
                 role: Option<EhdbClientRole>,
                 detail: Option<String>|
     -> ProjectionServeResult {
        let duration_seconds = started.elapsed().as_secs_f64();
        if record_metrics {
            metrics::record_projection(
                "primary_serve",
                outcome.as_str(),
                outcome.ok(),
                outcome.degraded(),
                duration_seconds,
            );
        }
        ProjectionServeResult {
            mode,
            outcome,
            role,
            duration_seconds,
            served_by_ehdb: false,
            report: None,
            reversible: false,
            rows_after_revert: 0,
            detail,
        }
    };

    // Off mode OR the umbrella EHDB switch disabled ⇒ strict no-op.
    if mode == ProjectionMode::Off || !truthy(env, EHDB_ENABLED_ENV) {
        return early(ProjectionOutcome::Disabled, None, None);
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
            ProjectionOutcome::PrimaryUnavailable,
            Some(contract.role),
            Some("projection primary read-serving is not activated in this build".to_string()),
        );
    }
    // The cycle only serves under the `primary` flag; `shadow` stays materialize-only.
    if mode != ProjectionMode::Primary {
        return early(
            ProjectionOutcome::PrimaryUnavailable,
            Some(contract.role),
            Some("primary-serve cycle requires NOETL_EHDB_PROJECTION=primary".to_string()),
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
    let engine = LocalReferenceProjectionEngine::new(log, tenant, namespace);
    let consumer = opts
        .consumer
        .clone()
        .unwrap_or_else(|| DEFAULT_CONSUMER.to_string());

    // Deterministic cycle: exec "100" runs to a terminal completed (2 events),
    // exec "200" one running event — a scope + fold + parity ground truth with a
    // matching authoritative snapshot so the dual-run parity check is exact.
    let input = ProjectionPrimaryInput {
        events: vec![
            cycle_event(1, 10, "100", "playbook_started", "start", "running"),
            cycle_event(2, 20, "200", "playbook_started", "start", "running"),
            cycle_event(3, 11, "100", "playbook.completed", "finish", "completed"),
        ],
        authoritative: vec![
            cycle_auth("100", "completed", 2, true),
            cycle_auth("200", "running", 1, false),
        ],
        authoritative_offset: Some(3),
    };

    let report = match exercise_primary_serve(&engine, &input, &consumer, &new_transaction_id()) {
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

    // Reversibility (rollback lever 1): flip the flag back to `shadow` in a cloned
    // env and materialize one more execution over the SAME store.  A clean shadow
    // materialize plus a whole-store read-back proves the incumbent/shadow read
    // path is restored with zero data loss and the store grew.
    let mut shadow_env = env.clone();
    shadow_env.insert(PROJECTION_MODE_ENV.to_string(), "shadow".to_string());
    let revert_events = vec![cycle_event(
        4,
        30,
        "300",
        "playbook_started",
        "start",
        "running",
    )];
    let revert_auth = vec![
        cycle_auth("100", "completed", 2, true),
        cycle_auth("200", "running", 1, false),
        cycle_auth("300", "running", 1, false),
    ];
    let revert = shadow_project(
        &shadow_env,
        &revert_events,
        &revert_auth,
        Some(4),
        opts,
        false,
    );
    let rows_after_revert = engine
        .list_executions(PRIMARY_SERVE_CYCLE_EVENTS + 8)
        .map(|l| l.total)
        .unwrap_or(0);
    let reversible = revert.outcome == ProjectionOutcome::Materialized
        && rows_after_revert == PRIMARY_SERVE_ROWS_AFTER_REVERT;

    let outcome = if served && reversible {
        ProjectionOutcome::ServedPrimary
    } else {
        ProjectionOutcome::PrimaryDivergence
    };
    let detail = if served && reversible {
        None
    } else if !served {
        report.divergence.clone()
    } else {
        Some(format!(
            "reversibility flip-back failed: revert={} rows={}",
            revert.outcome.as_str(),
            rows_after_revert
        ))
    };

    let duration_seconds = started.elapsed().as_secs_f64();
    if record_metrics {
        metrics::record_projection(
            "primary_serve",
            outcome.as_str(),
            outcome.ok(),
            outcome.degraded(),
            duration_seconds,
        );
    }
    ProjectionServeResult {
        mode,
        outcome,
        role: Some(contract.role),
        duration_seconds,
        served_by_ehdb: served,
        report: Some(report),
        reversible,
        rows_after_revert,
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
            ("NOETL_EHDB_PROJECTION", mode),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn tmp_log(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-proj-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        (dir.join("log.jsonl"), dir)
    }

    fn ev(
        global_sequence: u64,
        event_id: i64,
        exec: &str,
        event_type: &str,
        node: Option<&str>,
        status: Option<&str>,
    ) -> ProjectionEventInput {
        ProjectionEventInput {
            global_sequence,
            event_id,
            execution_id: exec.to_string(),
            event_type: event_type.to_string(),
            node_name: node.map(|s| s.to_string()),
            status: status.map(|s| s.to_string()),
            prev_event_id: None,
        }
    }

    /// The three-event drive used across the parity tests: exec "100" starts,
    /// runs a command, then completes.  Authoritative fold: one execution,
    /// terminal, completed, 3 events, offset 3.
    fn drive_events() -> Vec<ProjectionEventInput> {
        vec![
            ev(
                1,
                10,
                "100",
                "playbook_started",
                Some("start"),
                Some("running"),
            ),
            ev(
                2,
                11,
                "100",
                "command.completed",
                Some("load"),
                Some("completed"),
            ),
            ev(
                3,
                12,
                "100",
                "playbook.completed",
                Some("finish"),
                Some("completed"),
            ),
        ]
    }

    fn drive_authoritative() -> Vec<AuthoritativeExecutionState> {
        vec![AuthoritativeExecutionState {
            execution_id: "100".to_string(),
            status: "completed".to_string(),
            event_count: 3,
            terminal: true,
        }]
    }

    #[test]
    fn off_mode_is_noop() {
        let e = worker_env("/tmp/unused.jsonl", "off");
        let r = shadow_project(
            &e,
            &drive_events(),
            &drive_authoritative(),
            Some(3),
            &Default::default(),
            false,
        );
        assert_eq!(r.mode, ProjectionMode::Off);
        assert_eq!(r.outcome, ProjectionOutcome::Disabled);
        assert!(r.parity.is_none());
        assert!(r.applied.is_none());
    }

    #[test]
    fn ehdb_disabled_is_noop_even_in_shadow() {
        // Shadow requested but the umbrella EHDB switch is off ⇒ still no-op.
        let e: EnvMap = [("NOETL_EHDB_PROJECTION", "shadow")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let r = shadow_project(
            &e,
            &drive_events(),
            &drive_authoritative(),
            Some(3),
            &Default::default(),
            false,
        );
        assert_eq!(r.outcome, ProjectionOutcome::Disabled);
    }

    #[test]
    fn shadow_materialize_holds_parity() {
        let (log, dir) = tmp_log("shadow");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        let r = shadow_project(
            &e,
            &drive_events(),
            &drive_authoritative(),
            Some(3),
            &Default::default(),
            false,
        );
        assert_eq!(r.outcome, ProjectionOutcome::Materialized, "{:?}", r.detail);
        assert_eq!(r.applied, Some(3));
        assert_eq!(r.checkpoint, Some(3));
        assert!(r.parity.as_ref().unwrap().holds());
        assert_eq!(r.parity.as_ref().unwrap().checkpoint_lag, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shadow_without_authoritative_offset_still_materializes() {
        let (log, dir) = tmp_log("nooffset");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        // No authoritative offset → checkpoint-lag check skipped, key+value parity
        // still enforced.
        let r = shadow_project(
            &e,
            &drive_events(),
            &drive_authoritative(),
            None,
            &Default::default(),
            false,
        );
        assert_eq!(r.outcome, ProjectionOutcome::Materialized);
        assert!(r.parity.as_ref().unwrap().checkpoint_ok);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shadow_flags_parity_mismatch_on_divergent_authoritative() {
        let (log, dir) = tmp_log("mismatch");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        // Authoritative claims the execution is still running with 2 events, but
        // EHDB folds it to completed/3 → value divergence, degraded.
        let auth = vec![AuthoritativeExecutionState {
            execution_id: "100".to_string(),
            status: "running".to_string(),
            event_count: 2,
            terminal: false,
        }];
        let r = shadow_project(
            &e,
            &drive_events(),
            &auth,
            Some(3),
            &Default::default(),
            false,
        );
        assert_eq!(r.outcome, ProjectionOutcome::ParityMismatch);
        assert!(!r.parity.as_ref().unwrap().holds());
        assert!(r.detail.is_some());
        // The read-models were still materialized — the mismatch is a parity
        // verdict, not a materialize failure.
        assert_eq!(r.applied, Some(3));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shadow_flags_checkpoint_lag() {
        let (log, dir) = tmp_log("lag");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        // Authoritative offset claims 9 but EHDB only applied through 3 → lag.
        let r = shadow_project(
            &e,
            &drive_events(),
            &drive_authoritative(),
            Some(9),
            &Default::default(),
            false,
        );
        assert_eq!(r.outcome, ProjectionOutcome::ParityMismatch);
        let parity = r.parity.as_ref().unwrap();
        assert!(!parity.checkpoint_ok);
        assert_eq!(parity.checkpoint_lag, 6);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_empty_and_oversized_batch() {
        let (log, dir) = tmp_log("bounds");
        let mut e = worker_env(log.to_str().unwrap(), "shadow");
        let empty = shadow_project(&e, &[], &[], Some(0), &Default::default(), false);
        assert_eq!(empty.outcome, ProjectionOutcome::Rejected);
        e.insert(MAX_BATCH_ENV.to_string(), "2".to_string());
        let big = shadow_project(
            &e,
            &drive_events(),
            &drive_authoritative(),
            Some(3),
            &Default::default(),
            false,
        );
        assert_eq!(big.outcome, ProjectionOutcome::Rejected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_execution_id_is_invalid() {
        let (log, dir) = tmp_log("badid");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        let bad = vec![ev(1, 10, "bad id!", "playbook_started", None, None)];
        let auth = vec![AuthoritativeExecutionState {
            execution_id: "bad id!".to_string(),
            status: "running".to_string(),
            event_count: 1,
            terminal: false,
        }];
        let r = shadow_project(&e, &bad, &auth, Some(1), &Default::default(), false);
        assert_eq!(r.outcome, ProjectionOutcome::Invalid);
        assert!(r.applied.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn control_plane_role_guard_refused() {
        let e: EnvMap = [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "server"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_PROJECTION", "shadow"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let r = shadow_project(
            &e,
            &drive_events(),
            &drive_authoritative(),
            Some(3),
            &Default::default(),
            false,
        );
        assert_eq!(r.outcome, ProjectionOutcome::GuardRefused);
        assert!(r.applied.is_none());
    }

    #[test]
    fn primary_serves_authoritatively() {
        let (log, dir) = tmp_log("primary");
        let e = worker_env(log.to_str().unwrap(), "primary");
        // Phase 9 tier 2: primary is activated, so a primary apply serves the
        // read-models authoritatively from EHDB (not refused).  Parity holds.
        let r = shadow_project(
            &e,
            &drive_events(),
            &drive_authoritative(),
            Some(3),
            &Default::default(),
            false,
        );
        assert_eq!(r.mode, ProjectionMode::Primary);
        assert_eq!(
            r.outcome,
            ProjectionOutcome::ServedPrimary,
            "{:?}",
            r.detail
        );
        assert_eq!(r.applied, Some(3));
        assert_eq!(r.checkpoint, Some(3));
        assert!(r.parity.as_ref().unwrap().holds());
        // ServedPrimary is only reachable with PRIMARY_SERVE_ACTIVATED == true.
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_flags_divergence_on_divergent_authoritative() {
        let (log, dir) = tmp_log("primary-diverge");
        let e = worker_env(log.to_str().unwrap(), "primary");
        // Incumbent claims running/2 but EHDB folds completed/3 → served but the
        // dual-run parity diverged (degraded).
        let auth = vec![AuthoritativeExecutionState {
            execution_id: "100".to_string(),
            status: "running".to_string(),
            event_count: 2,
            terminal: false,
        }];
        let r = shadow_project(
            &e,
            &drive_events(),
            &auth,
            Some(3),
            &Default::default(),
            false,
        );
        assert_eq!(r.outcome, ProjectionOutcome::PrimaryDivergence);
        assert!(!r.parity.as_ref().unwrap().holds());
        // The read-models were still materialized — divergence is a parity verdict.
        assert_eq!(r.applied, Some(3));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_control_plane_still_guard_refused() {
        let e: EnvMap = [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "gateway"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_PROJECTION", "primary"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let r = shadow_project(
            &e,
            &drive_events(),
            &drive_authoritative(),
            Some(3),
            &Default::default(),
            false,
        );
        // Config error (control-plane role + data-plane env) → guard refused.
        assert_eq!(r.outcome, ProjectionOutcome::GuardRefused);
    }

    #[test]
    fn incremental_shadow_is_idempotent_on_replay() {
        // A second shadow apply of the same events materializes nothing new (the
        // engine's replay guard), and parity still holds against the same
        // authoritative fold.
        let (log, dir) = tmp_log("replay");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        let first = shadow_project(
            &e,
            &drive_events(),
            &drive_authoritative(),
            Some(3),
            &Default::default(),
            false,
        );
        assert_eq!(first.applied, Some(3));
        let second = shadow_project(
            &e,
            &drive_events(),
            &drive_authoritative(),
            Some(3),
            &Default::default(),
            false,
        );
        assert_eq!(second.applied, Some(0));
        assert_eq!(second.outcome, ProjectionOutcome::Materialized);
        assert_eq!(second.checkpoint, Some(3));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_cycle_served_by_ehdb_and_reversible() {
        let (log, dir) = tmp_log("cycle");
        let e = worker_env(log.to_str().unwrap(), "primary");
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.mode, ProjectionMode::Primary);
        assert_eq!(
            r.outcome,
            ProjectionOutcome::ServedPrimary,
            "{:?}",
            r.detail
        );
        assert!(r.served_by_ehdb);
        let report = r.report.as_ref().unwrap();
        assert!(report.served_by_ehdb());
        assert_eq!(report.applied, PRIMARY_SERVE_CYCLE_EVENTS);
        assert!(
            report.list_ok && report.scope_ok && report.read_event_ok && report.replay_idempotent
        );
        assert!(report.replay_matches && report.dual_run_holds);
        // Reversibility: flip back to shadow materialized one more execution; the
        // store is whole and serves the 2 cycle execs + the 1 revert exec.
        assert!(r.reversible);
        assert_eq!(r.rows_after_revert, 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_cycle_off_is_noop() {
        let e = worker_env("/tmp/unused-proj-cycle.jsonl", "off");
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.outcome, ProjectionOutcome::Disabled);
        assert!(r.report.is_none());
        assert!(!r.served_by_ehdb);
    }

    #[test]
    fn primary_serve_cycle_shadow_is_primary_unavailable() {
        let (log, dir) = tmp_log("cycle-shadow");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        // The cycle only serves under the `primary` flag.
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.outcome, ProjectionOutcome::PrimaryUnavailable);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_cycle_control_plane_guard_refused() {
        let e: EnvMap = [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "server"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_PROJECTION", "primary"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.outcome, ProjectionOutcome::GuardRefused);
        assert!(r.report.is_none());
    }

    /// Read-model-derived invariant, asserted structurally: this module must
    /// never author a NoETL event and never write to the authoritative Postgres
    /// materializer — it only touches the derived EHDB projection fabric via
    /// `ehdb_reference`.
    #[test]
    fn no_noetl_event_writer_or_materializer_write() {
        let full = include_str!("projection.rs");
        let src = full.split("#[cfg(test)]").next().unwrap();
        for forbidden in [
            "crate::events",
            "crate::client",
            "/api/events",
            "ExecutorEvent",
            "emit_event",
            "state_materializer",
            "INSERT INTO",
        ] {
            assert!(
                !code_lines(src).contains(forbidden),
                "forbidden write reference `{forbidden}` in projection.rs"
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
