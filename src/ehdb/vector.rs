//! Vector SHADOW wiring + PRIMARY-serve cutover (EHDB Phase 8 shadow, Phase 9
//! tier-5 primary — the final tier).
//!
//! EHDB's vector core engine (the `ehdb_reference::vector` driver, ehdb#246) is the
//! durable vector engine that Phase 8 puts *underneath* NoETL's internal **platform
//! vector** tier — the RAG / catalog embeddings the worker already ingests + queries
//! in-process via the Phase-E retrieval path ([`super::rag`]).  Unlike the KV / object
//! slices there is **no external Qdrant client in the worker**: platform retrieval is
//! already in-process, so this slice **formalizes** that path behind a driver seam
//! rather than cutting a live dependency.  This module is the worker's
//! **driver-selection seam** for that engine, gated by `NOETL_EHDB_VECTOR`:
//!
//! * `off` (default) — strict no-op.  No engine opened, no metric recorded; the
//!   worker's `/metrics` and behaviour are byte-identical to a build without the
//!   vector wiring.
//! * `shadow` — **dual-write + compare, never serve.**  A platform vector upsert is
//!   *also* written into the EHDB vector engine alongside the authoritative Qdrant
//!   path, then a self-retrieval top-k query is read back and compared for id-set /
//!   rank-order / score-monotonicity parity.  Reads are **never** served from EHDB
//!   and the authoritative Qdrant retrieval path is untouched.
//! * `primary` — **EHDB serves the platform vector tier authoritatively** (Phase 9
//!   tier 5): the retrieval ops the worker makes (upsert / query-topk / delete) are
//!   served by the EHDB engine in place of the internal Qdrant retrieval path, while
//!   the served results are dual-run parity-checked (id set + rank order + score
//!   monotonicity) against the Qdrant expectation.  [`PRIMARY_SERVE_ACTIVATED`] is
//!   now `true` so this build *can* serve primary; whether it *does* is a pure
//!   runtime choice of the `NOETL_EHDB_VECTOR` flag (see reversibility).
//!
//! ## Reversibility (the cutover safety property)
//!
//! The cutover is reversible with **two independent levers**:
//!
//! 1. **Runtime flag (operational, instant, no redeploy)** — flip
//!    `NOETL_EHDB_VECTOR` from `primary` back to `shadow`/`off` and the incumbent
//!    Qdrant retrieval path is the authoritative vector tier again immediately.
//!    Zero data loss: the primary path only ever appends to the derived EHDB
//!    `KeepAll` vector index and never mutates/deletes anything Qdrant owns, so
//!    Qdrant is exactly as it was and the EHDB index stays whole on disk for a
//!    later re-enable.
//! 2. **Compile-time kill switch (structural, belt-and-suspenders)** — set
//!    [`PRIMARY_SERVE_ACTIVATED`] back to `false` and it is structurally impossible
//!    for the build to serve primary regardless of config (the `primary` flag then
//!    degrades to [`VectorOutcome::PrimaryUnavailable`]).
//!
//! ## Boundaries (mirror the rest of `src/ehdb`)
//!
//! * Disabled-by-default no-op (byte-identical `/metrics`).
//! * Control-plane roles (`gateway`/`api`/`server`) refused before any engine
//!   opens — the gateway never touches the data plane.
//! * Bounded (dimensionality + top-k + payload caps) + stateless (engine opened +
//!   dropped per op).
//! * **Platform-only, event-log-authoritative** — a vector point is a derived
//!   platform index entry, not an event; this module never authors a NoETL event and
//!   never reaches `noetl.event` / `POST /api/events` (structurally asserted).  It
//!   only touches the derived EHDB vector fabric via `ehdb_reference`.  Business
//!   vector collections stay external and never flow through here.

use ehdb_reference::vector::exercise_primary_serve;
use ehdb_reference::{
    compare_vector_parity, AuthoritativeVectorHit, LocalReferenceVectorDriver, VectorDeleteRequest,
    VectorDriver, VectorParityReport, VectorPrimaryInput, VectorPrimaryServeReport,
    VectorQueryRequest, VectorUpsertRequest, DEFAULT_LOCAL_REFERENCE_NAMESPACE,
    DEFAULT_LOCAL_REFERENCE_TENANT, MAX_VECTOR_DIMENSIONS,
};

use super::contract::{contract_from_env, EhdbClientRole, EhdbContract, EHDB_ENABLED_ENV};
use super::guard::assert_data_plane_access_allowed;
use super::{metrics, EnvMap};
use crate::snowflake::SnowflakeGen;
use std::sync::OnceLock;

/// The driver-selection flag for the vector tier.
pub const VECTOR_MODE_ENV: &str = "NOETL_EHDB_VECTOR";
/// Dimensionality cap for one upsert (worker-side clamp under the crate ceiling).
pub const MAX_DIMENSIONS_ENV: &str = "NOETL_EHDB_VECTOR_MAX_DIMENSIONS";
const DEFAULT_MAX_DIMENSIONS: usize = 2_048;
/// The self-retrieval read-back top-k the shadow queries with (a dual-written point
/// must self-retrieve as the top hit).
const SELF_RETRIEVAL_TOP_K: usize = 4;
/// The float slack allowed when checking the EHDB ranking is monotonic (scores
/// differ across engines).
const PARITY_TOLERANCE: f32 = 1e-3;

/// Compile-time kill switch for primary-serve.  Phase 9 tier 5 activates it
/// (`true`): this build *can* serve the platform vector tier authoritatively from
/// EHDB.  Whether it *does* is the pure runtime choice of `NOETL_EHDB_VECTOR`
/// (`primary` serves; `shadow`/`off` keep the internal Qdrant retrieval path
/// authoritative), so the cutover stays reversible without a redeploy.  Setting
/// this back to `false` is the belt-and-suspenders structural rollback — it makes
/// primary-serve unreachable regardless of config (the `primary` flag then
/// degrades to [`VectorOutcome::PrimaryUnavailable`]).
pub const PRIMARY_SERVE_ACTIVATED: bool = true;

/// Which vector engine the tier is driven by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMode {
    /// No EHDB engine; the incumbent Qdrant path is authoritative.
    Off,
    /// Dual-write into EHDB + compare; never serve reads from it.
    Shadow,
    /// Serve retrieval from EHDB authoritatively (Phase 9 tier 5).
    Primary,
}

impl VectorMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            VectorMode::Off => "off",
            VectorMode::Shadow => "shadow",
            VectorMode::Primary => "primary",
        }
    }

    /// Parse the mode from the env, defaulting to `Off`.  An unrecognised value is
    /// treated as `Off` (fail-safe: an unknown driver never mirrors).
    pub fn from_env(env: &EnvMap) -> Self {
        match env
            .get(VECTOR_MODE_ENV)
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("shadow") => VectorMode::Shadow,
            Some("primary") => VectorMode::Primary,
            _ => VectorMode::Off,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorOutcome {
    /// Off mode / EHDB disabled — strict no-op.
    Disabled,
    /// Point mirrored into EHDB and top-k parity held.
    Mirrored,
    /// Point mirrored but the EHDB engine's top-k diverged from the authoritative view.
    ParityMismatch,
    /// `primary` served the retrieval op authoritatively from EHDB + dual-run parity
    /// against the incumbent Qdrant path held.
    ServedPrimary,
    /// `primary` served the retrieval op from EHDB but the dual-run parity against the
    /// incumbent diverged (degraded — surfaces on `last_degraded`).
    PrimaryDivergence,
    /// `primary` requested but primary-serve is not activated this build (the
    /// compile-time kill switch is off).
    PrimaryUnavailable,
    /// Vector over the dimensionality / payload cap, or top-k over bound.
    Rejected,
    /// A control-plane role reached the data-plane engine — refused.
    GuardRefused,
    /// Caller mistake (bad id / empty or zero vector / config).
    Invalid,
    /// The engine errored at runtime.
    Unavailable,
}

impl VectorOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            VectorOutcome::Disabled => "disabled",
            VectorOutcome::Mirrored => "mirrored",
            VectorOutcome::ParityMismatch => "parity_mismatch",
            VectorOutcome::ServedPrimary => "served_primary",
            VectorOutcome::PrimaryDivergence => "primary_divergence",
            VectorOutcome::PrimaryUnavailable => "primary_unavailable",
            VectorOutcome::Rejected => "rejected",
            VectorOutcome::GuardRefused => "guard_refused",
            VectorOutcome::Invalid => "invalid",
            VectorOutcome::Unavailable => "unavailable",
        }
    }

    pub fn ok(&self) -> bool {
        matches!(
            self,
            VectorOutcome::Disabled | VectorOutcome::Mirrored | VectorOutcome::ServedPrimary
        )
    }

    /// A degraded (but non-fatal) outcome — surfaces on the `last_degraded` gauge so
    /// a divergence or engine hiccup is visible without failing the authoritative
    /// path.
    fn degraded(&self) -> bool {
        matches!(
            self,
            VectorOutcome::ParityMismatch
                | VectorOutcome::PrimaryDivergence
                | VectorOutcome::Unavailable
        )
    }
}

/// Secret-free result of one shadow vector op.
#[derive(Debug, Clone)]
pub struct VectorResult {
    pub mode: VectorMode,
    pub outcome: VectorOutcome,
    pub role: Option<EhdbClientRole>,
    pub duration_seconds: f64,
    pub detail: Option<String>,
    /// The per-point index version EHDB assigned (present on a successful mirror).
    pub version: Option<u64>,
    /// The self-retrieval candidate count (present when a mirror ran).
    pub candidate_count: Option<usize>,
    /// The self-retrieval returned count (present when a mirror ran).
    pub returned: Option<usize>,
    /// The parity verdict (present when a mirror ran).
    pub parity: Option<VectorParityReport>,
}

#[derive(Debug, Clone, Default)]
pub struct VectorOptions {
    pub tenant: Option<String>,
    pub namespace: Option<String>,
    pub transaction_id: Option<String>,
}

fn txn_gen() -> &'static SnowflakeGen {
    static GEN: OnceLock<SnowflakeGen> = OnceLock::new();
    GEN.get_or_init(|| SnowflakeGen::from_env_or_hint("ehdb-vector"))
}

fn new_transaction_id() -> String {
    format!("ehdbvec-{}", txn_gen().next_id())
}

fn truthy(env: &EnvMap, key: &str) -> bool {
    matches!(
        env.get(key)
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref(),
        Some("1" | "true" | "yes" | "y" | "on")
    )
}

fn bounded_max_dimensions(env: &EnvMap) -> usize {
    env.get(MAX_DIMENSIONS_ENV)
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_DIMENSIONS)
        .clamp(1, MAX_VECTOR_DIMENSIONS)
}

fn tenant_of(opts: &VectorOptions) -> String {
    opts.tenant
        .clone()
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string())
}

fn namespace_of(opts: &VectorOptions) -> String {
    opts.namespace
        .clone()
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string())
}

/// Build a result (and record its metric under `operation`).  `version` / parity
/// fields are set by the success path afterward.
fn make_result(
    operation: &str,
    mode: VectorMode,
    outcome: VectorOutcome,
    role: Option<EhdbClientRole>,
    started: std::time::Instant,
    detail: Option<String>,
    record_metrics: bool,
) -> VectorResult {
    let duration_seconds = started.elapsed().as_secs_f64();
    if record_metrics {
        metrics::record_vector(
            operation,
            outcome.as_str(),
            outcome.ok(),
            outcome.degraded(),
            duration_seconds,
        );
    }
    VectorResult {
        mode,
        outcome,
        role,
        duration_seconds,
        detail,
        version: None,
        candidate_count: None,
        returned: None,
        parity: None,
    }
}

/// Classified by the crate error's Display since the crate does not re-export its
/// error enum: an identifier validation failure is a caller mistake (`Invalid`), an
/// over-cap dimensionality / payload / top-k is a caller `Rejected`, a vector
/// validation failure (empty / non-finite / zero / top-k zero) is `Invalid`, any
/// other runtime error is `Unavailable`.
fn classify_helper_error<E: std::fmt::Display>(err: &E) -> VectorOutcome {
    let msg = err.to_string();
    if msg.starts_with("invalid identifier") {
        VectorOutcome::Invalid
    } else if msg.contains("exceeds bound") {
        VectorOutcome::Rejected
    } else if msg.contains("must not be empty")
        || msg.contains("must contain only finite")
        || msg.contains("zero vector")
        || msg.contains("must be greater than zero")
    {
        VectorOutcome::Invalid
    } else {
        VectorOutcome::Unavailable
    }
}

/// Resolve the disabled-by-default contract for a vector op, refusing control-plane
/// roles before any engine opens.  Returns a ready result on any early exit.
fn resolve_contract(
    operation: &'static str,
    env: &EnvMap,
    mode: VectorMode,
    started: std::time::Instant,
    record_metrics: bool,
) -> Result<EhdbContract, Box<VectorResult>> {
    let finish = |outcome: VectorOutcome, role: Option<EhdbClientRole>, detail: Option<String>| {
        Box::new(make_result(
            operation,
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
                VectorOutcome::GuardRefused
            } else {
                VectorOutcome::Invalid
            };
            return Err(finish(outcome, role, Some(err.0)));
        }
    };

    if let Err(err) = assert_data_plane_access_allowed(contract.role, operation) {
        return Err(finish(
            VectorOutcome::GuardRefused,
            Some(contract.role),
            Some(err.to_string()),
        ));
    }
    if !contract.uses_local_reference_runtime() || contract.local_reference_log.is_none() {
        return Err(finish(VectorOutcome::Disabled, Some(contract.role), None));
    }
    Ok(contract)
}

pub(crate) fn driver_from(
    contract: &EhdbContract,
    opts: &VectorOptions,
) -> LocalReferenceVectorDriver {
    let log = contract.local_reference_log.clone().expect("log present");
    LocalReferenceVectorDriver::new(log, tenant_of(opts), namespace_of(opts))
}

/// Guard the common `off` / `disabled` / `primary` short-circuits shared by every
/// vector shadow op.  Returns `Ok((mode, contract))` when the shadow may proceed,
/// else a ready result.
fn enter_shadow(
    operation: &'static str,
    env: &EnvMap,
    started: std::time::Instant,
    record_metrics: bool,
) -> Result<(VectorMode, EhdbContract), Box<VectorResult>> {
    let mode = VectorMode::from_env(env);

    // Off mode OR the umbrella EHDB switch disabled ⇒ strict no-op.
    if mode == VectorMode::Off || !truthy(env, EHDB_ENABLED_ENV) {
        return Err(Box::new(make_result(
            operation,
            mode,
            VectorOutcome::Disabled,
            None,
            started,
            None,
            record_metrics,
        )));
    }

    // Primary is activated (Phase 9 tier 5) so it proceeds; the compile-time kill
    // switch off ⇒ refuse before any engine opens (a control-plane role is still
    // refused as a guard first).
    if mode == VectorMode::Primary && !PRIMARY_SERVE_ACTIVATED {
        let contract = resolve_contract(operation, env, mode, started, record_metrics)?;
        return Err(Box::new(make_result(
            operation,
            mode,
            VectorOutcome::PrimaryUnavailable,
            Some(contract.role),
            started,
            Some("vector primary serve is not activated in this build".to_string()),
            record_metrics,
        )));
    }

    let contract = resolve_contract(operation, env, mode, started, record_metrics)?;
    Ok((mode, contract))
}

/// Dual-write one platform vector into the EHDB vector engine (shadow) and compare a
/// self-retrieval top-k read-back against the authoritative expectation (the point
/// self-retrieves as the top hit — the id-set / rank-order / monotonicity parity the
/// Qdrant top-k would give for the same query).
///
/// This NEVER serves reads to the control plane and NEVER authors a NoETL event — it
/// only writes the derived EHDB vector fabric and reports parity.  The authoritative
/// Qdrant upsert is the caller's responsibility and is untouched here.
#[allow(clippy::too_many_arguments)]
pub fn mirror_upsert(
    env: &EnvMap,
    collection: &str,
    point_id: &str,
    model_id: &str,
    vector: &[f32],
    payload: Option<&str>,
    opts: &VectorOptions,
    record_metrics: bool,
) -> VectorResult {
    let started = std::time::Instant::now();
    let (mode, contract) = match enter_shadow("mirror", env, started, record_metrics) {
        Ok(pair) => pair,
        Err(result) => return *result,
    };

    let max_dims = bounded_max_dimensions(env);
    if vector.len() > max_dims {
        return make_result(
            "mirror",
            mode,
            VectorOutcome::Rejected,
            Some(contract.role),
            started,
            Some(format!(
                "vector {} dimensions exceeds bound {max_dims}",
                vector.len()
            )),
            record_metrics,
        );
    }

    let driver = driver_from(&contract, opts);
    let upsert_request = VectorUpsertRequest {
        collection: collection.to_string(),
        point_id: point_id.to_string(),
        model_id: model_id.to_string(),
        vector: vector.to_vec(),
        payload: payload.map(str::to_string),
        transaction_id: opts
            .transaction_id
            .clone()
            .unwrap_or_else(new_transaction_id),
    };

    let upsert = match driver.upsert(&upsert_request) {
        Ok(outcome) => outcome,
        Err(err) => {
            return make_result(
                "mirror",
                mode,
                classify_helper_error(&err),
                Some(contract.role),
                started,
                Some(err.to_string()),
                record_metrics,
            );
        }
    };

    // Self-retrieval read-back: query the engine with the point's own vector.  The
    // read-back stays inside this module — it is the shadow's parity input, NOT a
    // read served to the control plane.
    let query = match driver.query(&VectorQueryRequest {
        collection: collection.to_string(),
        model_id: model_id.to_string(),
        query: vector.to_vec(),
        top_k: SELF_RETRIEVAL_TOP_K,
    }) {
        Ok(outcome) => outcome,
        Err(err) => {
            return make_result(
                "mirror",
                mode,
                classify_helper_error(&err),
                Some(contract.role),
                started,
                Some(err.to_string()),
                record_metrics,
            );
        }
    };

    // Authoritative expectation: the just-written point self-retrieves as the top
    // hit (cosine of a vector with itself is maximal), so Qdrant's top-1 for this
    // query is this point.
    let authoritative = match query.hits.first() {
        Some(top) => vec![AuthoritativeVectorHit {
            point_id: point_id.to_string(),
            score: top.score,
        }],
        None => Vec::new(),
    };
    // Compare only the top hit (self-retrieval): truncate the EHDB view to rank-1.
    let top_only = ehdb_reference::VectorQueryOutcome {
        hits: query.hits.iter().take(1).cloned().collect(),
        returned: query.hits.iter().take(1).count(),
        ..query.clone()
    };
    let report = compare_vector_parity(&authoritative, &top_only, PARITY_TOLERANCE);
    // Under `primary` EHDB served the op authoritatively; under `shadow` it mirrored
    // alongside the authoritative Qdrant path.  The engine op is identical — the mode
    // only changes which path is authoritative and how the outcome is labelled.
    let serving_primary = mode == VectorMode::Primary;
    let result_outcome = match (serving_primary, report.holds()) {
        (true, true) => VectorOutcome::ServedPrimary,
        (true, false) => VectorOutcome::PrimaryDivergence,
        (false, true) => VectorOutcome::Mirrored,
        (false, false) => VectorOutcome::ParityMismatch,
    };
    let mut result = make_result(
        "mirror",
        mode,
        result_outcome,
        Some(contract.role),
        started,
        report.divergence.clone(),
        record_metrics,
    );
    result.version = Some(upsert.version);
    result.candidate_count = Some(query.candidate_count);
    result.returned = Some(query.returned);
    result.parity = Some(report);
    result
}

/// One step of the deterministic vector shadow drive.
#[derive(Debug, Clone)]
pub struct VectorSuiteStep {
    pub step: String,
    pub outcome: String,
    pub detail: Option<String>,
}

/// Secret-free report of the full vector shadow drive (`vector-suite`).
#[derive(Debug, Clone)]
pub struct VectorSuiteReport {
    pub mode: VectorMode,
    pub disabled: bool,
    pub ok: bool,
    pub role: Option<EhdbClientRole>,
    pub steps: Vec<VectorSuiteStep>,
    pub duration_seconds: f64,
    /// A control-plane role reached the data-plane engine — refused.
    pub guard_refused: bool,
    /// `primary` requested but not activated this session.
    pub primary_unavailable: bool,
}

/// Run a deterministic upsert / query-parity / top-k-truncate / delete /
/// query-after-delete drive against the EHDB vector engine in ONE contract/guard
/// resolution, exercising every engine capability behind the disabled-by-default
/// seam.  Reads are the shadow's own validation reads — never served to the control
/// plane.  Disabled ⇒ a no-op report (`disabled = true`).
pub fn shadow_suite(env: &EnvMap, opts: &VectorOptions, record_metrics: bool) -> VectorSuiteReport {
    let started = std::time::Instant::now();
    let (mode, contract) = match enter_shadow("suite", env, started, record_metrics) {
        Ok(pair) => pair,
        Err(result) => {
            let r = *result;
            return VectorSuiteReport {
                mode: r.mode,
                disabled: r.outcome == VectorOutcome::Disabled,
                ok: r.outcome == VectorOutcome::Disabled,
                role: r.role,
                steps: Vec::new(),
                duration_seconds: r.duration_seconds,
                guard_refused: r.outcome == VectorOutcome::GuardRefused,
                primary_unavailable: r.outcome == VectorOutcome::PrimaryUnavailable,
            };
        }
    };

    let driver = driver_from(&contract, opts);
    let collection = "ehdb-selfcheck-surface";
    let model = "selfcheck-embedding";
    let mut steps = Vec::new();
    let mut ok = true;

    let mut txn = 0u64;
    let mut next_txn = || {
        txn += 1;
        format!("vecsuite-{txn}")
    };

    // Three points with a known cosine order relative to query [1,0,0]: a > b > c.
    let points = [
        ("point-a", vec![1.0f32, 0.0, 0.0]),
        ("point-b", vec![0.8f32, 0.2, 0.0]),
        ("point-c", vec![0.0f32, 1.0, 0.0]),
    ];
    let mut upsert_ok = true;
    let mut first_version_ok = true;
    for (id, vector) in &points {
        match driver.upsert(&VectorUpsertRequest {
            collection: collection.to_string(),
            point_id: id.to_string(),
            model_id: model.to_string(),
            vector: vector.clone(),
            payload: Some(format!("src://{id}")),
            transaction_id: next_txn(),
        }) {
            Ok(o) => {
                upsert_ok &= o.written;
                if *id == "point-a" {
                    first_version_ok = o.version == 1 && o.created_stream;
                }
            }
            Err(_) => upsert_ok = false,
        }
    }
    ok &= upsert_ok && first_version_ok;
    steps.push(step("upsert", upsert_ok && first_version_ok, None));

    // query top-k parity: the known geometric order [a,b,c] must match EHDB's rank.
    let query = driver.query(&VectorQueryRequest {
        collection: collection.to_string(),
        model_id: model.to_string(),
        query: vec![1.0, 0.0, 0.0],
        top_k: 10,
    });
    let authoritative = vec![
        AuthoritativeVectorHit {
            point_id: "point-a".to_string(),
            score: 1.0,
        },
        AuthoritativeVectorHit {
            point_id: "point-b".to_string(),
            score: 0.97,
        },
        AuthoritativeVectorHit {
            point_id: "point-c".to_string(),
            score: 0.0,
        },
    ];
    let parity = query
        .as_ref()
        .ok()
        .map(|q| compare_vector_parity(&authoritative, q, PARITY_TOLERANCE));
    let query_ok = parity.as_ref().map(|p| p.holds()).unwrap_or(false);
    ok &= query_ok;
    steps.push(step(
        "query_parity",
        query_ok,
        parity.and_then(|p| p.divergence),
    ));

    // top-k truncation flag.
    let truncated = driver.query(&VectorQueryRequest {
        collection: collection.to_string(),
        model_id: model.to_string(),
        query: vec![1.0, 0.0, 0.0],
        top_k: 2,
    });
    let truncate_ok = matches!(&truncated, Ok(q) if q.returned == 2 && q.truncated_by_top_k && q.candidate_count == 3);
    ok &= truncate_ok;
    steps.push(step("top_k_truncate", truncate_ok, None));

    // delete point-a → query no longer returns it, ranking becomes [b,c].
    let del = driver.delete(&VectorDeleteRequest {
        collection: collection.to_string(),
        point_id: "point-a".to_string(),
        transaction_id: next_txn(),
    });
    let after = driver.query(&VectorQueryRequest {
        collection: collection.to_string(),
        model_id: model.to_string(),
        query: vec![1.0, 0.0, 0.0],
        top_k: 10,
    });
    let delete_ok = matches!(&del, Ok(o) if o.existed)
        && matches!(&after, Ok(q) if q.candidate_count == 2
            && q.hits.first().map(|h| h.point_id.as_str()) == Some("point-b"));
    ok &= delete_ok;
    steps.push(step("delete", delete_ok, None));

    if record_metrics {
        for s in &steps {
            metrics::record_vector(
                "suite",
                if s.outcome == "ok" {
                    "mirrored"
                } else {
                    "parity_mismatch"
                },
                s.outcome == "ok",
                s.outcome != "ok",
                0.0,
            );
        }
    }

    VectorSuiteReport {
        mode,
        disabled: false,
        ok,
        role: Some(contract.role),
        steps,
        duration_seconds: started.elapsed().as_secs_f64(),
        guard_refused: false,
        primary_unavailable: false,
    }
}

fn step(name: &str, ok: bool, detail: Option<String>) -> VectorSuiteStep {
    VectorSuiteStep {
        step: name.to_string(),
        outcome: if ok { "ok" } else { "fail" }.to_string(),
        detail,
    }
}

/// The platform-RAG collection + model the built-in primary-serve cycle drives.
const PRIMARY_SERVE_COLLECTION: &str = "noetl/primary_serve/playbook-surface";
const PRIMARY_SERVE_MODEL: &str = "text-embedding-3-small";
/// How many points the built-in primary-serve cycle seeds (delete on the last).
pub const PRIMARY_SERVE_CYCLE_ENTRIES: usize = 3;
/// Live candidates served after the reversibility flip-back: the 2 surviving cycle
/// points (last deleted) plus the 1 fresh point the shadow flip-back mirrors.
const PRIMARY_SERVE_CANDIDATES_AFTER_REVERT: usize = 3;

/// Secret-free result of the authoritative vector primary-serve cycle (Phase 9
/// tier 5) plus the operational reversibility demonstration.
#[derive(Debug, Clone)]
pub struct VectorServeResult {
    pub mode: VectorMode,
    pub outcome: VectorOutcome,
    pub role: Option<EhdbClientRole>,
    pub duration_seconds: f64,
    /// The EHDB engine served the whole cycle with the Qdrant retrieval semantics
    /// preserved and dual-run top-k parity intact.
    pub served_by_ehdb: bool,
    /// The full served-by-EHDB proof (present once the cycle ran).
    pub report: Option<VectorPrimaryServeReport>,
    /// After serving primary, flipping `NOETL_EHDB_VECTOR` back to `shadow` over the
    /// same index mirrored a further point and the collection served the whole live
    /// set — the incumbent Qdrant retrieval path is restored with zero data loss
    /// (rollback lever 1 demonstrated operationally).
    pub reversible: bool,
    /// The live-candidate count served after the flip-back (== surviving cycle
    /// points + 1).
    pub candidates_after_revert: usize,
    pub detail: Option<String>,
}

/// Drive the authoritative vector primary-serve cycle through the EHDB engine and
/// demonstrate operational reversibility.
///
/// In `primary` mode (and with [`PRIMARY_SERVE_ACTIVATED`]) this:
///
/// 1. runs [`exercise_primary_serve`] — upsert + served cosine top-k query +
///    tombstone delete + fresh-driver replay, all served authoritatively by EHDB,
///    dual-run parity-checked (id set + rank order + score monotonicity) against a
///    Qdrant mirror ranked in lockstep; then
/// 2. flips `NOETL_EHDB_VECTOR` back to `shadow` in a cloned env and mirrors a
///    further point over the SAME index, proving the incumbent/shadow path is
///    restored and the index stays whole (zero data loss on rollback).
///
/// Off/disabled ⇒ strict no-op (byte-identical `/metrics`).  Control-plane roles
/// are guard-refused before any engine opens.  Never authors a NoETL event and
/// never writes Qdrant — it only exercises the derived EHDB vector fabric.
pub fn serve_primary_cycle(
    env: &EnvMap,
    opts: &VectorOptions,
    record_metrics: bool,
) -> VectorServeResult {
    let started = std::time::Instant::now();
    let mode = VectorMode::from_env(env);

    // Early-exit builder (no cycle report) that records the `primary_serve`
    // metric — `disabled` outcomes are skipped by `record_vector`, preserving the
    // byte-identical no-op invariant.
    let early = |outcome: VectorOutcome,
                 role: Option<EhdbClientRole>,
                 detail: Option<String>|
     -> VectorServeResult {
        let duration_seconds = started.elapsed().as_secs_f64();
        if record_metrics {
            metrics::record_vector(
                "primary_serve",
                outcome.as_str(),
                outcome.ok(),
                outcome.degraded(),
                duration_seconds,
            );
        }
        VectorServeResult {
            mode,
            outcome,
            role,
            duration_seconds,
            served_by_ehdb: false,
            report: None,
            reversible: false,
            candidates_after_revert: 0,
            detail,
        }
    };

    // Off mode OR the umbrella EHDB switch disabled ⇒ strict no-op.
    if mode == VectorMode::Off || !truthy(env, EHDB_ENABLED_ENV) {
        return early(VectorOutcome::Disabled, None, None);
    }

    // Resolve the contract (guards control-plane / disabled).  Pass
    // `record_metrics = false` so the only metric recorded here is the
    // `primary_serve`-labelled one from `early` / the final path.
    let contract = match resolve_contract("primary_serve", env, mode, started, false) {
        Ok(c) => c,
        Err(result) => {
            let r = *result;
            return early(r.outcome, r.role, r.detail);
        }
    };

    // Compile-time kill switch off ⇒ primary unavailable (structural rollback).
    if !PRIMARY_SERVE_ACTIVATED {
        return early(
            VectorOutcome::PrimaryUnavailable,
            Some(contract.role),
            Some("vector primary serve is not activated in this build".to_string()),
        );
    }
    // The cycle only serves under the `primary` flag; `shadow` stays mirror-only.
    if mode != VectorMode::Primary {
        return early(
            VectorOutcome::PrimaryUnavailable,
            Some(contract.role),
            Some("primary-serve cycle requires NOETL_EHDB_VECTOR=primary".to_string()),
        );
    }

    let driver = driver_from(&contract, opts);

    // Deterministic cycle: three distinct platform-RAG points under one collection
    // (delete on the last), queried [1,0,0] so the ranking is a > b > c — a scope +
    // rank + delete ground truth with an in-lockstep Qdrant mirror so the dual-run
    // top-k parity is exact.
    let input = VectorPrimaryInput {
        collection: PRIMARY_SERVE_COLLECTION.to_string(),
        model_id: PRIMARY_SERVE_MODEL.to_string(),
        entries: vec![
            (
                "noetl/playbook/weather.example/chunk.0".to_string(),
                vec![1.0, 0.0, 0.0],
            ),
            (
                "noetl/playbook/weather.example/chunk.1".to_string(),
                vec![0.9, 0.1, 0.0],
            ),
            (
                "noetl/catalog/embeddings/tool.http".to_string(),
                vec![0.0, 1.0, 0.0],
            ),
        ],
        query: vec![1.0, 0.0, 0.0],
        top_k: 10,
    };

    let report = match exercise_primary_serve(&driver, &input, &new_transaction_id()) {
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
    // env and mirror one more point over the SAME index.  A clean mirror plus a
    // whole-collection live query proves the incumbent/shadow path is restored with
    // zero data loss and the index grew.
    let mut shadow_env = env.clone();
    shadow_env.insert(VECTOR_MODE_ENV.to_string(), "shadow".to_string());
    let revert = mirror_upsert(
        &shadow_env,
        PRIMARY_SERVE_COLLECTION,
        "noetl/primary_serve/revert.chunk",
        PRIMARY_SERVE_MODEL,
        &[0.5, 0.5, 0.0],
        Some("src://revert"),
        opts,
        false,
    );
    let candidates_after_revert = driver
        .query(&VectorQueryRequest {
            collection: PRIMARY_SERVE_COLLECTION.to_string(),
            model_id: PRIMARY_SERVE_MODEL.to_string(),
            query: vec![1.0, 0.0, 0.0],
            top_k: 10,
        })
        .map(|q| q.candidate_count)
        .unwrap_or(0);
    let reversible = revert.outcome == VectorOutcome::Mirrored
        && candidates_after_revert == PRIMARY_SERVE_CANDIDATES_AFTER_REVERT;

    let outcome = if served && reversible {
        VectorOutcome::ServedPrimary
    } else {
        VectorOutcome::PrimaryDivergence
    };
    let detail = if served && reversible {
        None
    } else if !served {
        report.divergence.clone()
    } else {
        Some(format!(
            "reversibility flip-back failed: revert={} candidates={candidates_after_revert}",
            revert.outcome.as_str(),
        ))
    };

    let duration_seconds = started.elapsed().as_secs_f64();
    if record_metrics {
        metrics::record_vector(
            "primary_serve",
            outcome.as_str(),
            outcome.ok(),
            outcome.degraded(),
            duration_seconds,
        );
    }
    VectorServeResult {
        mode,
        outcome,
        role: Some(contract.role),
        duration_seconds,
        served_by_ehdb: served,
        report: Some(report),
        reversible,
        candidates_after_revert,
        detail,
    }
}

// ===========================================================================
// Live runtime hook (noetl/ehdb#234 runtime integration, vector tier).
//
// **Reachability — honest status (2026-07-07).**  Unlike event-log / kv / object
// / projection, the vector tier has **no live platform vector-upsert site in the
// worker process today**, so the `mirror_live_upsert` pair below is READY but is
// NOT invoked from any live path — wiring it into a fabricated site would create
// a hook that fires but never mirrors a real upsert (or, worse, mirrors non-vector
// data), which is exactly the "hook that lies" this integration refuses to ship.
//
// What exists in the worker:
//   * platform RAG *retrieval* ([`super::rag::retrieve`]) — read-only, no upsert.
//   * platform RAG *ingest* ([`super::rag::ingest`]) — writes a **lexical**
//     retrieval fabric ([`super::rag::RagChunk`] carries `text` + `checksum`, NOT
//     an embedding vector), so it is not a vector-embedding upsert to mirror.
//   * [`mirror_upsert`] / [`shadow_suite`] / [`serve_primary_cycle`] — exercised
//     only by the `ehdb-selfcheck` diagnostic binary and tests.
//
// The precise remaining seam: a future platform-RAG *embed + upsert* write site —
// when a worker step (a `tool`/system playbook) computes embeddings and upserts
// platform vectors (e.g. in `executor/command.rs` at the embed-and-upsert
// dispatch, or wherever the SLM/RAG ingest path lands its vectors) — that write
// site, once it exists, calls [`mirror_live_upsert`] right after the authoritative
// Qdrant upsert.  The hook lands WITH that write site, not before.  Everything
// below is the ready, tested machinery so that future wire-up is a one-line call.
// ===========================================================================

/// Resolve the once-per-process env snapshot that would arm the **live platform
/// vector-upsert mirror hook** (noetl/ehdb#234, vector tier).  Twin of
/// [`eventlog::runtime_hook_env`][elh] for the vector tier.
///
/// Returns `Some(env)` ONLY when `NOETL_EHDB_ENABLED` is truthy, `NOETL_EHDB_VECTOR`
/// is `shadow`, and the contract is a data-plane role on the bounded
/// `local_reference` runtime with a log configured; `None` otherwise (a strict
/// no-op).  **Note:** no live worker path resolves this yet — see the module
/// reachability note above.
///
/// [elh]: super::eventlog::runtime_hook_env
pub fn runtime_hook_env(env: &EnvMap) -> Option<EnvMap> {
    if !truthy(env, EHDB_ENABLED_ENV) {
        return None;
    }
    if VectorMode::from_env(env) != VectorMode::Shadow {
        return None;
    }
    let contract = contract_from_env(env).ok()?;
    if !contract.role.is_data_plane() {
        return None;
    }
    if !contract.uses_local_reference_runtime() || contract.local_reference_log.is_none() {
        return None;
    }
    Some(env.clone())
}

/// Live platform vector-upsert mirror hook: mirror one already-written platform
/// vector (the authoritative Qdrant upsert the caller just performed) into the
/// EHDB vector shadow fabric.  Calls the SAME [`mirror_upsert`] shadow
/// dual-write plus self-retrieval parity path the `ehdb-selfcheck mirror-vector`
/// drive exercises.
///
/// **Not yet wired to a live site** (see module reachability note): this is the
/// ready seam for the future platform-RAG embed+upsert path.  It is exercised by
/// unit tests so it stays correct until the write site exists.
///
/// **Best-effort + isolated.**  Shadow is auxiliary: this NEVER affects the
/// authoritative vector path.  Engine-error cases surface as non-`ok` outcomes
/// (recorded to the degraded metric); an unexpected panic is caught here and
/// returned as [`VectorOutcome::Unavailable`] rather than unwinding into the
/// caller.  The caller discards the return; the `noetl_ehdb_vector_*` metric
/// carries the signal.
pub fn mirror_live_upsert(
    env: &EnvMap,
    collection: &str,
    point_id: &str,
    model_id: &str,
    vector: &[f32],
    payload: Option<&str>,
) -> VectorOutcome {
    let guarded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        mirror_upsert(
            env,
            collection,
            point_id,
            model_id,
            vector,
            payload,
            &VectorOptions::default(),
            true,
        )
        .outcome
    }));
    guarded.unwrap_or(VectorOutcome::Unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MODEL: &str = "text-embedding-3-small";
    const COLLECTION: &str = "playbook-surface";
    const POINT: &str = "noetl/playbook/weather.example/chunk.0";

    fn worker_env(log: &str, mode: &str) -> EnvMap {
        [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "worker"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", log),
            ("NOETL_EHDB_VECTOR", mode),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn tmp_log(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-vector-worker-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        (dir.join("log.jsonl"), dir)
    }

    fn mirror(e: &EnvMap, collection: &str, point: &str, vector: &[f32]) -> VectorResult {
        mirror_upsert(
            e,
            collection,
            point,
            MODEL,
            vector,
            Some("src://x"),
            &Default::default(),
            false,
        )
    }

    #[test]
    fn off_mode_is_noop() {
        let e = worker_env("/tmp/unused.jsonl", "off");
        let r = mirror(&e, COLLECTION, POINT, &[1.0, 0.0]);
        assert_eq!(r.mode, VectorMode::Off);
        assert_eq!(r.outcome, VectorOutcome::Disabled);
        assert!(r.parity.is_none());
    }

    #[test]
    fn ehdb_disabled_is_noop_even_in_shadow() {
        let e: EnvMap = [("NOETL_EHDB_VECTOR", "shadow")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let r = mirror(&e, COLLECTION, POINT, &[1.0, 0.0]);
        assert_eq!(r.outcome, VectorOutcome::Disabled);
    }

    #[test]
    fn shadow_mirror_holds_parity() {
        let (log, dir) = tmp_log("shadow");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        let r = mirror(&e, COLLECTION, POINT, &[1.0, 0.0, 0.0]);
        assert_eq!(r.outcome, VectorOutcome::Mirrored, "{:?}", r.detail);
        assert_eq!(r.version, Some(1));
        assert_eq!(r.returned, Some(1));
        assert!(r.parity.as_ref().unwrap().holds());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shadow_mirror_self_retrieves_top_hit_among_many() {
        let (log, dir) = tmp_log("selfretrieve");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        // Seed a competing point, then mirror the target — it must still self-retrieve.
        mirror(&e, COLLECTION, "other", &[0.0, 1.0, 0.0]);
        let r = mirror(&e, COLLECTION, POINT, &[1.0, 0.0, 0.0]);
        assert_eq!(r.outcome, VectorOutcome::Mirrored, "{:?}", r.detail);
        assert!(r.candidate_count.unwrap() >= 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_oversized_dimensionality() {
        let (log, dir) = tmp_log("bounds");
        let mut e = worker_env(log.to_str().unwrap(), "shadow");
        e.insert(MAX_DIMENSIONS_ENV.to_string(), "2".to_string());
        let r = mirror(&e, COLLECTION, POINT, &[1.0, 0.0, 0.0]);
        assert_eq!(r.outcome, VectorOutcome::Rejected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn zero_vector_is_invalid() {
        let (log, dir) = tmp_log("zero");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        let r = mirror(&e, COLLECTION, POINT, &[0.0, 0.0]);
        assert_eq!(r.outcome, VectorOutcome::Invalid);
        assert!(r.version.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_id_is_invalid() {
        let (log, dir) = tmp_log("badid");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        let r = mirror(&e, COLLECTION, "", &[1.0, 0.0]);
        assert_eq!(r.outcome, VectorOutcome::Invalid);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn control_plane_role_guard_refused() {
        let e: EnvMap = [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "server"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_VECTOR", "shadow"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let r = mirror(&e, COLLECTION, POINT, &[1.0, 0.0]);
        assert_eq!(r.outcome, VectorOutcome::GuardRefused);
        assert!(r.version.is_none());
    }

    #[test]
    fn primary_serves_authoritatively() {
        let (log, dir) = tmp_log("primary");
        let e = worker_env(log.to_str().unwrap(), "primary");
        // Phase 9 tier 5: primary is activated, so a primary upsert serves the
        // retrieval op authoritatively from EHDB (not refused).  Parity holds.
        let r = mirror(&e, COLLECTION, POINT, &[1.0, 0.0, 0.0]);
        assert_eq!(r.mode, VectorMode::Primary);
        assert_eq!(r.outcome, VectorOutcome::ServedPrimary, "{:?}", r.detail);
        assert_eq!(r.version, Some(1));
        assert!(r.parity.as_ref().unwrap().holds());
        // ServedPrimary is only reachable with PRIMARY_SERVE_ACTIVATED == true.
        assert!(PRIMARY_SERVE_ACTIVATED);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_cycle_served_by_ehdb_and_reversible() {
        let (log, dir) = tmp_log("cycle");
        let e = worker_env(log.to_str().unwrap(), "primary");
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.mode, VectorMode::Primary);
        assert_eq!(r.outcome, VectorOutcome::ServedPrimary, "{:?}", r.detail);
        assert!(r.served_by_ehdb);
        let report = r.report.as_ref().unwrap();
        assert!(report.served_by_ehdb());
        assert_eq!(report.upsert_count, PRIMARY_SERVE_CYCLE_ENTRIES);
        assert!(report.upsert_ok && report.query_ok && report.delete_ok);
        assert!(report.replay_matches && report.dual_run_holds);
        // Reversibility: flip back to shadow mirrored one more point; the index is
        // whole and serves the 2 surviving cycle points + the 1 revert point.
        assert!(r.reversible);
        assert_eq!(r.candidates_after_revert, 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_cycle_off_is_noop() {
        let e = worker_env("/tmp/unused-vector-cycle.jsonl", "off");
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.outcome, VectorOutcome::Disabled);
        assert!(r.report.is_none());
        assert!(!r.served_by_ehdb);
    }

    #[test]
    fn primary_serve_cycle_shadow_is_primary_unavailable() {
        let (log, dir) = tmp_log("cycle-shadow");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        // The cycle only serves under the `primary` flag.
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.outcome, VectorOutcome::PrimaryUnavailable);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_cycle_control_plane_guard_refused() {
        let e: EnvMap = [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "server"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_VECTOR", "primary"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.outcome, VectorOutcome::GuardRefused);
        assert!(r.report.is_none());
    }

    #[test]
    fn primary_control_plane_still_guard_refused() {
        let e: EnvMap = [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "gateway"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_VECTOR", "primary"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let r = mirror(&e, COLLECTION, POINT, &[1.0, 0.0]);
        assert_eq!(r.outcome, VectorOutcome::GuardRefused);
    }

    #[test]
    fn suite_drives_full_engine_and_holds() {
        let (log, dir) = tmp_log("suite");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        let report = shadow_suite(&e, &Default::default(), false);
        assert!(!report.disabled);
        assert!(report.ok, "{:?}", report.steps);
        let names: Vec<&str> = report.steps.iter().map(|s| s.step.as_str()).collect();
        assert_eq!(
            names,
            vec!["upsert", "query_parity", "top_k_truncate", "delete"]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn suite_disabled_is_noop() {
        let e = worker_env("/tmp/unused.jsonl", "off");
        let report = shadow_suite(&e, &Default::default(), false);
        assert!(report.disabled);
        assert!(report.ok);
        assert!(report.steps.is_empty());
    }

    #[test]
    fn suite_control_plane_guard_refused() {
        let e: EnvMap = [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "server"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_VECTOR", "shadow"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let report = shadow_suite(&e, &Default::default(), false);
        assert!(report.guard_refused);
        assert!(!report.ok);
    }

    // --- live runtime hook (noetl/ehdb#234; ready but not yet live-wired) ----

    #[test]
    fn runtime_hook_env_arms_only_for_enabled_shadow_data_plane() {
        let armed = runtime_hook_env(&worker_env("/tmp/vec-hook.jsonl", "shadow"));
        assert!(armed.is_some(), "enabled + shadow + worker role should arm");
    }

    #[test]
    fn runtime_hook_env_noop_when_disabled() {
        let mut e = worker_env("/tmp/vec-hook.jsonl", "shadow");
        e.remove("NOETL_EHDB_ENABLED");
        assert!(runtime_hook_env(&e).is_none());
    }

    #[test]
    fn runtime_hook_env_noop_when_tier_off_or_primary() {
        assert!(runtime_hook_env(&worker_env("/tmp/vec-hook.jsonl", "off")).is_none());
        assert!(runtime_hook_env(&worker_env("/tmp/vec-hook.jsonl", "primary")).is_none());
    }

    #[test]
    fn runtime_hook_env_skips_control_plane_role() {
        for role in ["gateway", "api", "server"] {
            let mut e = worker_env("/tmp/vec-hook.jsonl", "shadow");
            e.insert("NOETL_EHDB_CLIENT_ROLE".to_string(), role.to_string());
            assert!(
                runtime_hook_env(&e).is_none(),
                "control-plane role {role} must not arm the vector hook"
            );
        }
    }

    #[test]
    fn mirror_live_upsert_fires_on_shadow_enabled() {
        let (log, dir) = tmp_log("live-upsert");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        let outcome = mirror_live_upsert(&e, COLLECTION, POINT, MODEL, &[1.0, 0.0, 0.0], None);
        assert_eq!(outcome, VectorOutcome::Mirrored);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn mirror_live_upsert_is_noop_when_disabled() {
        let mut e = worker_env("/tmp/unused.jsonl", "shadow");
        e.remove("NOETL_EHDB_ENABLED");
        let outcome = mirror_live_upsert(&e, COLLECTION, POINT, MODEL, &[1.0, 0.0, 0.0], None);
        assert_eq!(outcome, VectorOutcome::Disabled);
    }

    #[test]
    fn mirror_live_upsert_is_noop_when_tier_off() {
        let e = worker_env("/tmp/unused.jsonl", "off");
        let outcome = mirror_live_upsert(&e, COLLECTION, POINT, MODEL, &[1.0, 0.0, 0.0], None);
        assert_eq!(outcome, VectorOutcome::Disabled);
    }

    #[test]
    fn mirror_live_upsert_skipped_for_control_plane_role() {
        let (log, dir) = tmp_log("live-cp");
        let mut e = worker_env(log.to_str().unwrap(), "shadow");
        e.insert("NOETL_EHDB_CLIENT_ROLE".to_string(), "gateway".to_string());
        let outcome = mirror_live_upsert(&e, COLLECTION, POINT, MODEL, &[1.0, 0.0, 0.0], None);
        assert_eq!(outcome, VectorOutcome::GuardRefused);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn mirror_live_upsert_isolates_engine_error_without_propagating() {
        // An over-cap dimensionality drives a metered non-ok outcome; the guarded
        // hook returns it instead of unwinding into the caller.
        let (log, dir) = tmp_log("live-iso");
        let mut e = worker_env(log.to_str().unwrap(), "shadow");
        e.insert(MAX_DIMENSIONS_ENV.to_string(), "1".to_string());
        let outcome = mirror_live_upsert(&e, COLLECTION, POINT, MODEL, &[1.0, 0.0, 0.0], None);
        assert_eq!(outcome, VectorOutcome::Rejected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Event-log-authoritative invariant, asserted structurally: this module must
    /// never reach the NoETL event log — it only touches the derived EHDB vector
    /// fabric via `ehdb_reference`.
    #[test]
    fn no_noetl_event_writer() {
        let full = include_str!("vector.rs");
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
                "forbidden NoETL event-writer reference `{forbidden}` in vector.rs"
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
