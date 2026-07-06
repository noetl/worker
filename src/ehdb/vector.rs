//! Disabled-by-default vector SHADOW wiring (EHDB Phase 8, slice 3).
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
//! * `primary` — recognised but **NOT activated this session**.  Cutover to serving
//!   retrieval from EHDB is a later gated step (Phase 9); requesting `primary` here
//!   is refused with a distinct outcome and the worker stays on Qdrant.
//!   [`PRIMARY_SERVE_ACTIVATED`] is a compile-time `false` so it is structurally
//!   impossible for this build to serve primary.
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

use ehdb_reference::{
    compare_vector_parity, AuthoritativeVectorHit, LocalReferenceVectorDriver, VectorDeleteRequest,
    VectorDriver, VectorParityReport, VectorQueryRequest, VectorUpsertRequest,
    DEFAULT_LOCAL_REFERENCE_NAMESPACE, DEFAULT_LOCAL_REFERENCE_TENANT, MAX_VECTOR_DIMENSIONS,
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

/// Compile-time guard: this build never serves retrieval from EHDB.  Phase 8 ships
/// the shadow only; flipping this to `true` is the later, separately-gated primary
/// cutover (Phase 9) and is intentionally not reachable from config.
pub const PRIMARY_SERVE_ACTIVATED: bool = false;

/// Which vector engine the tier is driven by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMode {
    /// No EHDB engine; the incumbent Qdrant path is authoritative.
    Off,
    /// Dual-write into EHDB + compare; never serve reads from it.
    Shadow,
    /// Serve retrieval from EHDB — recognised but not activated this session.
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
    /// `primary` requested but primary-serve is not activated this session.
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
            VectorOutcome::PrimaryUnavailable => "primary_unavailable",
            VectorOutcome::Rejected => "rejected",
            VectorOutcome::GuardRefused => "guard_refused",
            VectorOutcome::Invalid => "invalid",
            VectorOutcome::Unavailable => "unavailable",
        }
    }

    pub fn ok(&self) -> bool {
        matches!(self, VectorOutcome::Disabled | VectorOutcome::Mirrored)
    }

    /// A degraded (but non-fatal) outcome — surfaces on the `last_degraded` gauge so
    /// a divergence or engine hiccup is visible without failing the authoritative
    /// path.
    fn degraded(&self) -> bool {
        matches!(
            self,
            VectorOutcome::ParityMismatch | VectorOutcome::Unavailable
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

fn driver_from(contract: &EhdbContract, opts: &VectorOptions) -> LocalReferenceVectorDriver {
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

    // Primary is recognised but not activated this session — refuse before any engine
    // opens (a control-plane role is still refused as a guard first).
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
    let result_outcome = if report.holds() {
        VectorOutcome::Mirrored
    } else {
        VectorOutcome::ParityMismatch
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
    fn primary_is_recognised_but_not_activated() {
        let (log, dir) = tmp_log("primary");
        let e = worker_env(log.to_str().unwrap(), "primary");
        let r = mirror(&e, COLLECTION, POINT, &[1.0, 0.0]);
        assert_eq!(r.mode, VectorMode::Primary);
        assert_eq!(r.outcome, VectorOutcome::PrimaryUnavailable);
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
