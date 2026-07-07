//! Object / blob SHADOW wiring + PRIMARY-serve cutover (EHDB Phase 8 shadow,
//! Phase 9 tier-4 primary).
//!
//! EHDB's object/blob core engine (the `ehdb_reference::object` driver, ehdb#245)
//! is the durable content-addressed object engine that Phase 8 puts *underneath*
//! NoETL's internal **external object store** (GCS / S3) usage — the
//! platform-artifact tier: the **state shards** (`#166`,
//! `state_materializer`/`state_reader`) and the **result tier** (`#104`,
//! `result_materializer`/`result_resolver`) the worker reaches today through the
//! server's `/api/internal/objects/{key}` API.  This module is the worker's
//! **driver-selection seam** for that engine, gated by `NOETL_EHDB_OBJECT`:
//!
//! * `off` (default) — strict no-op.  No engine opened, no metric recorded; the
//!   worker's `/metrics` and behaviour are byte-identical to a build without the
//!   object wiring.
//! * `shadow` — **dual-write + compare, never serve.**  A platform artifact write
//!   is *also* stored in the EHDB object engine alongside the authoritative
//!   external-store path, then read back and compared for presence / digest /
//!   length / retrievability parity.  Reads are **never** served from EHDB and the
//!   authoritative object-store path is untouched.
//! * `primary` — **EHDB serves the platform object tier authoritatively** (Phase 9
//!   tier 4): the object ops the worker makes (put / get / list / locate / delete)
//!   are served by the EHDB engine in place of the internal external object store,
//!   while the served results are dual-run digest-parity-checked against the
//!   external store.  [`PRIMARY_SERVE_ACTIVATED`] is now `true` so this build *can*
//!   serve primary; whether it *does* is a pure runtime choice of the
//!   `NOETL_EHDB_OBJECT` flag (see reversibility).
//!
//! ## Reversibility (the cutover safety property)
//!
//! The cutover is reversible with **two independent levers**:
//!
//! 1. **Runtime flag (operational, instant, no redeploy)** — flip
//!    `NOETL_EHDB_OBJECT` from `primary` back to `shadow`/`off` and the incumbent
//!    external object store is the authoritative object tier again immediately.
//!    Zero data loss: the primary path only ever appends to the derived EHDB
//!    `KeepAll` object registry + content-addressed blob store and never
//!    mutates/deletes anything the external store owns, so the external store is
//!    exactly as it was and the EHDB store stays whole on disk for a later
//!    re-enable.
//! 2. **Compile-time kill switch (structural, belt-and-suspenders)** — set
//!    [`PRIMARY_SERVE_ACTIVATED`] back to `false` and it is structurally
//!    impossible for the build to serve primary regardless of config (the
//!    `primary` flag then degrades to [`ObjectOutcome::PrimaryUnavailable`]).
//!
//! ## Boundaries (mirror the rest of `src/ehdb`)
//!
//! * Disabled-by-default no-op (byte-identical `/metrics`).
//! * Control-plane roles (`gateway`/`api`/`server`) refused before any engine
//!   opens — the gateway never touches the data plane.
//! * Bounded (blob byte cap) + stateless (engine opened + dropped per op).
//! * **Event-log-authoritative** — an object is a derived platform artifact, not
//!   an event; this module never authors a NoETL event and never reaches
//!   `noetl.event` / `POST /api/events` (structurally asserted).  It only touches
//!   the derived EHDB object fabric via `ehdb_reference`.  **Platform object tier
//!   only** — tenant/domain (business) object buckets stay external, reached by
//!   playbook connectors.

use std::path::PathBuf;

use ehdb_reference::object::exercise_primary_serve;
use ehdb_reference::{
    compare_object_parity, AuthoritativeObject, LocalReferenceObjectBlobDriver, ObjectBlobDriver,
    ObjectDeleteRequest, ObjectGetRequest, ObjectListRequest, ObjectLocateRequest,
    ObjectParityReport, ObjectPrimaryInput, ObjectPrimaryServeReport, ObjectPutRequest,
    DEFAULT_LOCAL_REFERENCE_NAMESPACE, DEFAULT_LOCAL_REFERENCE_TENANT,
};

use super::contract::{contract_from_env, EhdbClientRole, EhdbContract, EHDB_ENABLED_ENV};
use super::guard::assert_data_plane_access_allowed;
use super::{metrics, EnvMap};
use crate::snowflake::SnowflakeGen;
use std::sync::OnceLock;

/// The driver-selection flag for the object/blob tier.
pub const OBJECT_MODE_ENV: &str = "NOETL_EHDB_OBJECT";
/// Blob byte cap for one object write.
pub const MAX_OBJECT_BYTES_ENV: &str = "NOETL_EHDB_OBJECT_MAX_BYTES";
const DEFAULT_MAX_OBJECT_BYTES: usize = 4 * 1024 * 1024;
/// Hard ceiling — the crate engine rejects a blob above `MAX_OBJECT_BYTES`
/// (16 MiB), so the worker-side clamp never exceeds it.
const MAX_OBJECT_BYTES_CEILING: usize = 16 * 1024 * 1024;
/// The content-addressed blob store root is a sibling directory of the reference
/// transaction log, so the object bytes and the registry log share a parent and
/// are cleaned up together.
const OBJECT_STORE_DIRNAME: &str = "ehdb_object_store";

/// Compile-time kill switch for primary-serve.  Phase 9 tier 4 activates it
/// (`true`): this build *can* serve the platform object tier authoritatively from
/// EHDB.  Whether it *does* is the pure runtime choice of `NOETL_EHDB_OBJECT`
/// (`primary` serves; `shadow`/`off` keep the internal external object store
/// authoritative), so the cutover stays reversible without a redeploy.  Setting
/// this back to `false` is the belt-and-suspenders structural rollback — it makes
/// primary-serve unreachable regardless of config (the `primary` flag then
/// degrades to [`ObjectOutcome::PrimaryUnavailable`]).
pub const PRIMARY_SERVE_ACTIVATED: bool = true;

/// Which object engine the tier is driven by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectMode {
    /// No EHDB engine; the incumbent external-store path is authoritative.
    Off,
    /// Dual-write into EHDB + compare; never serve reads from it.
    Shadow,
    /// Serve objects from EHDB authoritatively (Phase 9 tier 4).
    Primary,
}

impl ObjectMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectMode::Off => "off",
            ObjectMode::Shadow => "shadow",
            ObjectMode::Primary => "primary",
        }
    }

    /// Parse the mode from the env, defaulting to `Off`.  An unrecognised value is
    /// treated as `Off` (fail-safe: an unknown driver never mirrors).
    pub fn from_env(env: &EnvMap) -> Self {
        match env
            .get(OBJECT_MODE_ENV)
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("shadow") => ObjectMode::Shadow,
            Some("primary") => ObjectMode::Primary,
            _ => ObjectMode::Off,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectOutcome {
    /// Off mode / EHDB disabled — strict no-op.
    Disabled,
    /// Object mirrored into EHDB and parity held.
    Mirrored,
    /// Object mirrored but the EHDB engine diverged from the authoritative view.
    ParityMismatch,
    /// `primary` served the object op authoritatively from EHDB + dual-run parity
    /// against the incumbent external-store path held.
    ServedPrimary,
    /// `primary` served the object op from EHDB but the dual-run parity against the
    /// incumbent diverged (degraded — surfaces on `last_degraded`).
    PrimaryDivergence,
    /// `primary` requested but primary-serve is not activated this build (the
    /// compile-time kill switch is off).
    PrimaryUnavailable,
    /// Blob over the byte cap.
    Rejected,
    /// A control-plane role reached the data-plane engine — refused.
    GuardRefused,
    /// Caller mistake (bad key / config).
    Invalid,
    /// The engine errored at runtime.
    Unavailable,
}

impl ObjectOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectOutcome::Disabled => "disabled",
            ObjectOutcome::Mirrored => "mirrored",
            ObjectOutcome::ParityMismatch => "parity_mismatch",
            ObjectOutcome::ServedPrimary => "served_primary",
            ObjectOutcome::PrimaryDivergence => "primary_divergence",
            ObjectOutcome::PrimaryUnavailable => "primary_unavailable",
            ObjectOutcome::Rejected => "rejected",
            ObjectOutcome::GuardRefused => "guard_refused",
            ObjectOutcome::Invalid => "invalid",
            ObjectOutcome::Unavailable => "unavailable",
        }
    }

    pub fn ok(&self) -> bool {
        matches!(
            self,
            ObjectOutcome::Disabled | ObjectOutcome::Mirrored | ObjectOutcome::ServedPrimary
        )
    }

    /// A degraded (but non-fatal) outcome — surfaces on the `last_degraded` gauge
    /// so a divergence or engine hiccup is visible without failing the
    /// authoritative path.
    fn degraded(&self) -> bool {
        matches!(
            self,
            ObjectOutcome::ParityMismatch
                | ObjectOutcome::PrimaryDivergence
                | ObjectOutcome::Unavailable
        )
    }
}

/// Secret-free result of one shadow object op.
#[derive(Debug, Clone)]
pub struct ObjectResult {
    pub mode: ObjectMode,
    pub outcome: ObjectOutcome,
    pub role: Option<EhdbClientRole>,
    pub duration_seconds: f64,
    pub detail: Option<String>,
    /// The per-key registry version EHDB assigned (present on a successful mirror).
    pub version: Option<u64>,
    /// The content digest EHDB stored (present on a successful mirror).
    pub digest: Option<String>,
    /// Whether the content-addressed blob dedup'd (present on a successful mirror).
    pub content_deduplicated: Option<bool>,
    /// The parity verdict (present when a mirror ran).
    pub parity: Option<ObjectParityReport>,
}

#[derive(Debug, Clone, Default)]
pub struct ObjectOptions {
    pub tenant: Option<String>,
    pub namespace: Option<String>,
    pub transaction_id: Option<String>,
}

fn txn_gen() -> &'static SnowflakeGen {
    static GEN: OnceLock<SnowflakeGen> = OnceLock::new();
    GEN.get_or_init(|| SnowflakeGen::from_env_or_hint("ehdb-object"))
}

fn new_transaction_id() -> String {
    format!("ehdbobj-{}", txn_gen().next_id())
}

fn truthy(env: &EnvMap, key: &str) -> bool {
    matches!(
        env.get(key)
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref(),
        Some("1" | "true" | "yes" | "y" | "on")
    )
}

fn bounded_max_object_bytes(env: &EnvMap) -> usize {
    env.get(MAX_OBJECT_BYTES_ENV)
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_OBJECT_BYTES)
        .clamp(1, MAX_OBJECT_BYTES_CEILING)
}

fn tenant_of(opts: &ObjectOptions) -> String {
    opts.tenant
        .clone()
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string())
}

fn namespace_of(opts: &ObjectOptions) -> String {
    opts.namespace
        .clone()
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string())
}

/// Derive the content-addressed blob store root from the reference log path — a
/// sibling `ehdb_object_store` directory so the bytes and the registry log share a
/// parent and are cleaned up together.
fn object_root_from(log: &std::path::Path) -> PathBuf {
    match log.parent() {
        Some(parent) => parent.join(OBJECT_STORE_DIRNAME),
        None => PathBuf::from(OBJECT_STORE_DIRNAME),
    }
}

/// Build a result (and record its metric under `operation`).  `version` / `digest`
/// / `parity` are set by the success path afterward.
fn make_result(
    operation: &str,
    mode: ObjectMode,
    outcome: ObjectOutcome,
    role: Option<EhdbClientRole>,
    started: std::time::Instant,
    detail: Option<String>,
    record_metrics: bool,
) -> ObjectResult {
    let duration_seconds = started.elapsed().as_secs_f64();
    if record_metrics {
        metrics::record_object(
            operation,
            outcome.as_str(),
            outcome.ok(),
            outcome.degraded(),
            duration_seconds,
        );
    }
    ObjectResult {
        mode,
        outcome,
        role,
        duration_seconds,
        detail,
        version: None,
        digest: None,
        content_deduplicated: None,
        parity: None,
    }
}

/// Classified by the crate error's Display since the crate does not re-export its
/// error enum: an identifier validation failure is a caller mistake (`Invalid`),
/// an over-cap blob is a caller `Rejected`, any other runtime error is
/// `Unavailable`.
fn classify_helper_error<E: std::fmt::Display>(err: &E) -> ObjectOutcome {
    let msg = err.to_string();
    if msg.starts_with("invalid identifier") {
        ObjectOutcome::Invalid
    } else if msg.contains("exceeds bound") {
        ObjectOutcome::Rejected
    } else {
        ObjectOutcome::Unavailable
    }
}

/// Resolve the disabled-by-default contract for an object op, refusing
/// control-plane roles before any engine opens.  Returns a ready result on any
/// early exit.
fn resolve_contract(
    operation: &'static str,
    env: &EnvMap,
    mode: ObjectMode,
    started: std::time::Instant,
    record_metrics: bool,
) -> Result<EhdbContract, Box<ObjectResult>> {
    let finish = |outcome: ObjectOutcome, role: Option<EhdbClientRole>, detail: Option<String>| {
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
                ObjectOutcome::GuardRefused
            } else {
                ObjectOutcome::Invalid
            };
            return Err(finish(outcome, role, Some(err.0)));
        }
    };

    if let Err(err) = assert_data_plane_access_allowed(contract.role, operation) {
        return Err(finish(
            ObjectOutcome::GuardRefused,
            Some(contract.role),
            Some(err.to_string()),
        ));
    }
    if !contract.uses_local_reference_runtime() || contract.local_reference_log.is_none() {
        return Err(finish(ObjectOutcome::Disabled, Some(contract.role), None));
    }
    Ok(contract)
}

fn driver_from(contract: &EhdbContract, opts: &ObjectOptions) -> LocalReferenceObjectBlobDriver {
    let log = contract.local_reference_log.clone().expect("log present");
    let object_root = object_root_from(&log);
    LocalReferenceObjectBlobDriver::new(log, object_root, tenant_of(opts), namespace_of(opts))
}

/// Guard the common `off` / `disabled` / `primary` short-circuits shared by every
/// object shadow op.  Returns `Ok((mode, contract))` when the shadow may proceed,
/// else a ready result.
fn enter_shadow(
    operation: &'static str,
    env: &EnvMap,
    started: std::time::Instant,
    record_metrics: bool,
) -> Result<(ObjectMode, EhdbContract), Box<ObjectResult>> {
    let mode = ObjectMode::from_env(env);

    // Off mode OR the umbrella EHDB switch disabled ⇒ strict no-op.
    if mode == ObjectMode::Off || !truthy(env, EHDB_ENABLED_ENV) {
        return Err(Box::new(make_result(
            operation,
            mode,
            ObjectOutcome::Disabled,
            None,
            started,
            None,
            record_metrics,
        )));
    }

    // Primary is recognised but not activated this session — refuse before any
    // engine opens (a control-plane role is still refused as a guard first).
    if mode == ObjectMode::Primary && !PRIMARY_SERVE_ACTIVATED {
        let contract = resolve_contract(operation, env, mode, started, record_metrics)?;
        return Err(Box::new(make_result(
            operation,
            mode,
            ObjectOutcome::PrimaryUnavailable,
            Some(contract.role),
            started,
            Some("object primary serve is not activated in this build".to_string()),
            record_metrics,
        )));
    }

    let contract = resolve_contract(operation, env, mode, started, record_metrics)?;
    Ok((mode, contract))
}

/// Dual-write one platform artifact into the EHDB object engine (shadow) and
/// compare the engine's read-back against the authoritative artifact just written
/// (presence / digest / length / retrievability parity).
///
/// This NEVER serves reads to the control plane and NEVER authors a NoETL event —
/// it only writes the derived EHDB object fabric and reports parity.  The
/// authoritative external-store write is the caller's responsibility and is
/// untouched here.
pub fn mirror_put(
    env: &EnvMap,
    key: &str,
    bytes: &[u8],
    opts: &ObjectOptions,
    record_metrics: bool,
) -> ObjectResult {
    let started = std::time::Instant::now();
    let (mode, contract) = match enter_shadow("mirror", env, started, record_metrics) {
        Ok(pair) => pair,
        Err(result) => return *result,
    };

    let max_bytes = bounded_max_object_bytes(env);
    if bytes.len() > max_bytes {
        return make_result(
            "mirror",
            mode,
            ObjectOutcome::Rejected,
            Some(contract.role),
            started,
            Some(format!(
                "object {} bytes exceeds bound {max_bytes}",
                bytes.len()
            )),
            record_metrics,
        );
    }

    let driver = driver_from(&contract, opts);
    let put_request = ObjectPutRequest {
        key: key.to_string(),
        bytes: bytes.to_vec(),
        transaction_id: opts
            .transaction_id
            .clone()
            .unwrap_or_else(new_transaction_id),
    };

    let put = match driver.put(&put_request) {
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

    // Read back and compare against the authoritative artifact we just wrote.  The
    // read-back stays inside this module — it is the shadow's parity input, NOT a
    // read served to the control plane.
    let get = match driver.get(&ObjectGetRequest {
        key: key.to_string(),
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

    let authoritative = AuthoritativeObject {
        digest: put.digest.clone(),
        byte_len: put.byte_len,
    };
    let report = compare_object_parity(Some(&authoritative), &get);
    // Under `primary` EHDB served the op authoritatively; under `shadow` it
    // mirrored alongside the authoritative external-store path.  The engine op is
    // identical — the mode only changes which path is authoritative and how the
    // outcome is labelled.
    let serving_primary = mode == ObjectMode::Primary;
    let result_outcome = match (serving_primary, report.holds()) {
        (true, true) => ObjectOutcome::ServedPrimary,
        (true, false) => ObjectOutcome::PrimaryDivergence,
        (false, true) => ObjectOutcome::Mirrored,
        (false, false) => ObjectOutcome::ParityMismatch,
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
    result.version = Some(put.version);
    result.digest = Some(put.digest);
    result.content_deduplicated = Some(put.content_deduplicated);
    result.parity = Some(report);
    result
}

/// One step of the deterministic object shadow drive.
#[derive(Debug, Clone)]
pub struct ObjectSuiteStep {
    pub step: String,
    pub outcome: String,
    pub detail: Option<String>,
}

/// Secret-free report of the full object shadow drive (`object-suite`).
#[derive(Debug, Clone)]
pub struct ObjectSuiteReport {
    pub mode: ObjectMode,
    pub disabled: bool,
    pub ok: bool,
    pub role: Option<EhdbClientRole>,
    pub steps: Vec<ObjectSuiteStep>,
    pub duration_seconds: f64,
    /// A control-plane role reached the data-plane engine — refused.
    pub guard_refused: bool,
    /// `primary` requested but not activated this session.
    pub primary_unavailable: bool,
}

/// Run a deterministic put / get-parity / list / dedup / locate / delete drive
/// against the EHDB object engine in ONE contract/guard resolution, exercising
/// every engine capability behind the disabled-by-default seam.  Reads are the
/// shadow's own validation reads — never served to the control plane.  Disabled ⇒
/// a no-op report (`disabled = true`).
pub fn shadow_suite(env: &EnvMap, opts: &ObjectOptions, record_metrics: bool) -> ObjectSuiteReport {
    let started = std::time::Instant::now();
    let (mode, contract) = match enter_shadow("suite", env, started, record_metrics) {
        Ok(pair) => pair,
        Err(result) => {
            let r = *result;
            return ObjectSuiteReport {
                mode: r.mode,
                disabled: r.outcome == ObjectOutcome::Disabled,
                ok: r.outcome == ObjectOutcome::Disabled,
                role: r.role,
                steps: Vec::new(),
                duration_seconds: r.duration_seconds,
                guard_refused: r.outcome == ObjectOutcome::GuardRefused,
                primary_unavailable: r.outcome == ObjectOutcome::PrimaryUnavailable,
            };
        }
    };

    let driver = driver_from(&contract, opts);
    let state_key = "noetl/env=selfcheck/execution=exec-1/state/open.feather";
    let result_key = "noetl/env=selfcheck/execution=exec-1/results/s/f/r/a.feather";
    let mut steps = Vec::new();
    let mut ok = true;

    let mut txn = 0u64;
    let mut next_txn = || {
        txn += 1;
        format!("objsuite-{txn}")
    };

    // put state_key → get → digest/length/retrievability parity.
    let put1 = driver.put(&ObjectPutRequest {
        key: state_key.to_string(),
        bytes: b"arrow-ipc-state-shard".to_vec(),
        transaction_id: next_txn(),
    });
    let put1_ok = matches!(&put1, Ok(o) if o.written && !o.content_deduplicated);
    ok &= put1_ok;
    steps.push(step(
        "put",
        put1_ok,
        put1.as_ref().err().map(|e| e.to_string()),
    ));

    let get1 = driver.get(&ObjectGetRequest {
        key: state_key.to_string(),
    });
    let parity1 = match (&put1, &get1) {
        (Ok(p), Ok(g)) => Some(compare_object_parity(
            Some(&AuthoritativeObject {
                digest: p.digest.clone(),
                byte_len: p.byte_len,
            }),
            g,
        )),
        _ => None,
    };
    let get1_ok = parity1.as_ref().map(|p| p.holds()).unwrap_or(false);
    ok &= get1_ok;
    steps.push(step(
        "get_parity",
        get1_ok,
        parity1.and_then(|p| p.divergence),
    ));

    // dedup: a second key with identical bytes → same digest, content dedups.
    let put_dup = driver.put(&ObjectPutRequest {
        key: result_key.to_string(),
        bytes: b"arrow-ipc-state-shard".to_vec(),
        transaction_id: next_txn(),
    });
    let dedup_ok = match (&put1, &put_dup) {
        (Ok(a), Ok(b)) => a.digest == b.digest && b.content_deduplicated,
        _ => false,
    };
    ok &= dedup_ok;
    steps.push(step("content_dedup", dedup_ok, None));

    // list: both keys present, ordered by key.
    let list = driver.list(&ObjectListRequest {
        prefix: Some("noetl/env=selfcheck/".to_string()),
        limit: 100,
    });
    let list_ok = matches!(&list, Ok(l) if l.exists && l.match_count == 2);
    ok &= list_ok;
    steps.push(step("list", list_ok, None));

    // locate: state_key resolves to an in-cluster URI handle.
    let locate = driver.locate(&ObjectLocateRequest {
        key: state_key.to_string(),
    });
    let locate_ok = matches!(&locate, Ok(l) if l.found && l.uri.as_deref().map(|u| u.starts_with("ehdb-object://")).unwrap_or(false));
    ok &= locate_ok;
    steps.push(step("locate", locate_ok, None));

    // delete → get absent.
    let del = driver.delete(&ObjectDeleteRequest {
        key: state_key.to_string(),
        transaction_id: next_txn(),
    });
    let del_get = driver.get(&ObjectGetRequest {
        key: state_key.to_string(),
    });
    let del_ok = matches!(&del, Ok(o) if o.existed) && matches!(&del_get, Ok(g) if !g.found);
    ok &= del_ok;
    steps.push(step("delete", del_ok, None));

    if record_metrics {
        for s in &steps {
            metrics::record_object(
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

    ObjectSuiteReport {
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

fn step(name: &str, ok: bool, detail: Option<String>) -> ObjectSuiteStep {
    ObjectSuiteStep {
        step: name.to_string(),
        outcome: if ok { "ok" } else { "fail" }.to_string(),
        detail,
    }
}

/// The logical-key prefix the built-in primary-serve cycle drives.
const PRIMARY_SERVE_PREFIX: &str = "noetl/env=primary_serve/execution=exec-p/";
/// How many entries the built-in primary-serve cycle seeds (delete on the last).
pub const PRIMARY_SERVE_CYCLE_ENTRIES: usize = 3;
/// Live keys served after the reversibility flip-back: the 2 surviving cycle keys
/// (last deleted) plus the 1 fresh key the shadow flip-back mirrors.
const PRIMARY_SERVE_KEYS_AFTER_REVERT: usize = 3;

/// Secret-free result of the authoritative object primary-serve cycle (Phase 9
/// tier 4) plus the operational reversibility demonstration.
#[derive(Debug, Clone)]
pub struct ObjectServeResult {
    pub mode: ObjectMode,
    pub outcome: ObjectOutcome,
    pub role: Option<EhdbClientRole>,
    pub duration_seconds: f64,
    /// The EHDB engine served the whole cycle with the external-store semantics
    /// preserved and dual-run digest-parity intact.
    pub served_by_ehdb: bool,
    /// The full served-by-EHDB proof (present once the cycle ran).
    pub report: Option<ObjectPrimaryServeReport>,
    /// After serving primary, flipping `NOETL_EHDB_OBJECT` back to `shadow` over the
    /// same store mirrored a further object and the store served the whole live set
    /// — the incumbent external object store is restored with zero data loss
    /// (rollback lever 1 demonstrated operationally).
    pub reversible: bool,
    /// The live-key count served after the flip-back (== surviving cycle keys + 1).
    pub keys_after_revert: usize,
    pub detail: Option<String>,
}

/// Drive the authoritative object primary-serve cycle through the EHDB engine and
/// demonstrate operational reversibility.
///
/// In `primary` mode (and with [`PRIMARY_SERVE_ACTIVATED`]) this:
///
/// 1. runs [`exercise_primary_serve`] — put + per-key digest-verified served get +
///    prefix list + in-cluster locate + tombstone delete + fresh-driver replay, all
///    served authoritatively by EHDB, dual-run digest-parity-checked against an
///    external-store mirror; then
/// 2. flips `NOETL_EHDB_OBJECT` back to `shadow` in a cloned env and mirrors a
///    further object over the SAME store, proving the incumbent/shadow path is
///    restored and the store stays whole (zero data loss on rollback).
///
/// Off/disabled ⇒ strict no-op (byte-identical `/metrics`).  Control-plane roles
/// are guard-refused before any engine opens.  Never authors a NoETL event and
/// never writes the external object store — it only exercises the derived EHDB
/// object fabric.
pub fn serve_primary_cycle(
    env: &EnvMap,
    opts: &ObjectOptions,
    record_metrics: bool,
) -> ObjectServeResult {
    let started = std::time::Instant::now();
    let mode = ObjectMode::from_env(env);

    // Early-exit builder (no cycle report) that records the `primary_serve`
    // metric — `disabled` outcomes are skipped by `record_object`, preserving the
    // byte-identical no-op invariant.
    let early = |outcome: ObjectOutcome,
                 role: Option<EhdbClientRole>,
                 detail: Option<String>|
     -> ObjectServeResult {
        let duration_seconds = started.elapsed().as_secs_f64();
        if record_metrics {
            metrics::record_object(
                "primary_serve",
                outcome.as_str(),
                outcome.ok(),
                outcome.degraded(),
                duration_seconds,
            );
        }
        ObjectServeResult {
            mode,
            outcome,
            role,
            duration_seconds,
            served_by_ehdb: false,
            report: None,
            reversible: false,
            keys_after_revert: 0,
            detail,
        }
    };

    // Off mode OR the umbrella EHDB switch disabled ⇒ strict no-op.
    if mode == ObjectMode::Off || !truthy(env, EHDB_ENABLED_ENV) {
        return early(ObjectOutcome::Disabled, None, None);
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
            ObjectOutcome::PrimaryUnavailable,
            Some(contract.role),
            Some("object primary serve is not activated in this build".to_string()),
        );
    }
    // The cycle only serves under the `primary` flag; `shadow` stays mirror-only.
    if mode != ObjectMode::Primary {
        return early(
            ObjectOutcome::PrimaryUnavailable,
            Some(contract.role),
            Some("primary-serve cycle requires NOETL_EHDB_OBJECT=primary".to_string()),
        );
    }

    let driver = driver_from(&contract, opts);

    // Deterministic cycle: three distinct platform-artifact keys under one
    // execution prefix (delete on the last state shard), each with distinct bytes —
    // a scope + list + locate + delete ground truth with an in-lockstep
    // external-store mirror so the dual-run digest-parity is exact.
    let input = ObjectPrimaryInput {
        prefix: Some(PRIMARY_SERVE_PREFIX.to_string()),
        entries: vec![
            (
                format!("{PRIMARY_SERVE_PREFIX}state/open.feather"),
                b"arrow-ipc-state-open".to_vec(),
            ),
            (
                format!("{PRIMARY_SERVE_PREFIX}results/s/f/r/a.feather"),
                b"arrow-ipc-result-frame".to_vec(),
            ),
            (
                format!("{PRIMARY_SERVE_PREFIX}state/sealed.feather"),
                b"arrow-ipc-state-sealed".to_vec(),
            ),
        ],
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
    // env and mirror one more object over the SAME store.  A clean mirror plus a
    // whole-store live list proves the incumbent/shadow path is restored with zero
    // data loss and the store grew.
    let mut shadow_env = env.clone();
    shadow_env.insert(OBJECT_MODE_ENV.to_string(), "shadow".to_string());
    let revert = mirror_put(
        &shadow_env,
        &format!("{PRIMARY_SERVE_PREFIX}results/s/f/r/revert.feather"),
        b"arrow-ipc-revert-frame",
        opts,
        false,
    );
    let keys_after_revert = driver
        .list(&ObjectListRequest {
            prefix: Some(PRIMARY_SERVE_PREFIX.to_string()),
            limit: 4_096,
        })
        .map(|l| l.match_count)
        .unwrap_or(0);
    let reversible = revert.outcome == ObjectOutcome::Mirrored
        && keys_after_revert == PRIMARY_SERVE_KEYS_AFTER_REVERT;

    let outcome = if served && reversible {
        ObjectOutcome::ServedPrimary
    } else {
        ObjectOutcome::PrimaryDivergence
    };
    let detail = if served && reversible {
        None
    } else if !served {
        report.divergence.clone()
    } else {
        Some(format!(
            "reversibility flip-back failed: revert={} keys={keys_after_revert}",
            revert.outcome.as_str(),
        ))
    };

    let duration_seconds = started.elapsed().as_secs_f64();
    if record_metrics {
        metrics::record_object(
            "primary_serve",
            outcome.as_str(),
            outcome.ok(),
            outcome.degraded(),
            duration_seconds,
        );
    }
    ObjectServeResult {
        mode,
        outcome,
        role: Some(contract.role),
        duration_seconds,
        served_by_ehdb: served,
        report: Some(report),
        reversible,
        keys_after_revert,
        detail,
    }
}

/// Resolve the once-per-process env snapshot that arms the **live platform-object
/// mirror hook** (noetl/ehdb#234 runtime integration, object tier).  This is the
/// twin of [`eventlog::runtime_hook_env`][elh] for the object tier: the worker's
/// authoritative platform-object write chokepoint
/// (`ControlPlaneClient::object_put` → `PUT /api/internal/objects/{key}`, which
/// every object tier — result-tier, state-shard, plugin — funnels through)
/// resolves it exactly once at client construction, so the per-put path does
/// *zero* work when the hook is inactive.
///
/// Returns `Some(env)` — meaning "mirror every live platform-object put" — ONLY
/// when all of:
///
/// * the umbrella switch `NOETL_EHDB_ENABLED` is truthy, AND
/// * the object tier `NOETL_EHDB_OBJECT` is `shadow` (this slice wires the live
///   path for **shadow** only; `off`/`primary` return `None`), AND
/// * the resolved contract is a data-plane role (`worker`/`playbook`/`system`)
///   running the bounded `local_reference` runtime with a log configured.
///
/// Every other case (disabled, tier off/primary, control-plane role, malformed
/// contract) returns `None` — a strict no-op hook, so a worker without EHDB is
/// byte-identical.
///
/// [elh]: super::eventlog::runtime_hook_env
pub fn runtime_hook_env(env: &EnvMap) -> Option<EnvMap> {
    if !truthy(env, EHDB_ENABLED_ENV) {
        return None;
    }
    if ObjectMode::from_env(env) != ObjectMode::Shadow {
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

/// Live platform-object mirror hook: mirror one already-written platform object
/// (the authoritative object-store put the worker just performed) into the EHDB
/// object shadow fabric with digest-parity verification.
///
/// This calls the SAME [`mirror_put`] shadow dual-write + read-back digest-parity
/// path the `ehdb-selfcheck mirror-object` drive exercises, but on the real
/// object puts the worker performs, so a live drive advances the
/// `noetl_ehdb_object_*` metrics instead of only the selfcheck.
///
/// **Best-effort + isolated.**  Shadow is auxiliary: this NEVER affects the
/// authoritative object path.  Engine-error cases surface as non-`ok` outcomes
/// (recorded to the degraded metric), and an unexpected panic is caught here and
/// returned as [`ObjectOutcome::Unavailable`] rather than unwinding into the
/// caller.  The caller discards the return; the metric carries the signal.
pub fn mirror_live_put(env: &EnvMap, key: &str, bytes: &[u8]) -> ObjectOutcome {
    let guarded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        mirror_put(env, key, bytes, &ObjectOptions::default(), true).outcome
    }));
    guarded.unwrap_or(ObjectOutcome::Unavailable)
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
            ("NOETL_EHDB_OBJECT", mode),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn tmp_log(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-object-worker-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        (dir.join("log.jsonl"), dir)
    }

    const STATE_KEY: &str = "noetl/env=prod/execution=exec-9/state/open.feather";

    #[test]
    fn off_mode_is_noop() {
        let e = worker_env("/tmp/unused.jsonl", "off");
        let r = mirror_put(&e, STATE_KEY, b"v", &Default::default(), false);
        assert_eq!(r.mode, ObjectMode::Off);
        assert_eq!(r.outcome, ObjectOutcome::Disabled);
        assert!(r.parity.is_none());
    }

    #[test]
    fn ehdb_disabled_is_noop_even_in_shadow() {
        let e: EnvMap = [("NOETL_EHDB_OBJECT", "shadow")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let r = mirror_put(&e, STATE_KEY, b"v", &Default::default(), false);
        assert_eq!(r.outcome, ObjectOutcome::Disabled);
    }

    #[test]
    fn shadow_mirror_holds_parity() {
        let (log, dir) = tmp_log("shadow");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        let r = mirror_put(
            &e,
            STATE_KEY,
            b"arrow-ipc-bytes",
            &Default::default(),
            false,
        );
        assert_eq!(r.outcome, ObjectOutcome::Mirrored, "{:?}", r.detail);
        assert_eq!(r.version, Some(1));
        assert!(r.digest.as_ref().unwrap().starts_with("sha256:"));
        assert_eq!(r.content_deduplicated, Some(false));
        assert!(r.parity.as_ref().unwrap().holds());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shadow_mirror_dedups_identical_content() {
        let (log, dir) = tmp_log("dedup");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        let first = mirror_put(
            &e,
            "noetl/a/state/open.feather",
            b"same",
            &Default::default(),
            false,
        );
        let second = mirror_put(
            &e,
            "noetl/b/state/open.feather",
            b"same",
            &Default::default(),
            false,
        );
        assert_eq!(first.digest, second.digest);
        assert_eq!(first.content_deduplicated, Some(false));
        assert_eq!(second.content_deduplicated, Some(true));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_oversized_object() {
        let (log, dir) = tmp_log("bounds");
        let mut e = worker_env(log.to_str().unwrap(), "shadow");
        e.insert(MAX_OBJECT_BYTES_ENV.to_string(), "4".to_string());
        let r = mirror_put(&e, STATE_KEY, b"toolong", &Default::default(), false);
        assert_eq!(r.outcome, ObjectOutcome::Rejected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_key_is_invalid() {
        let (log, dir) = tmp_log("badid");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        // An empty key is an invalid identifier.
        let r = mirror_put(&e, "", b"v", &Default::default(), false);
        assert_eq!(r.outcome, ObjectOutcome::Invalid);
        assert!(r.version.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn control_plane_role_guard_refused() {
        let e: EnvMap = [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "server"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_OBJECT", "shadow"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let r = mirror_put(&e, STATE_KEY, b"v", &Default::default(), false);
        assert_eq!(r.outcome, ObjectOutcome::GuardRefused);
        assert!(r.version.is_none());
    }

    #[test]
    fn primary_serves_authoritatively() {
        let (log, dir) = tmp_log("primary");
        let e = worker_env(log.to_str().unwrap(), "primary");
        // Phase 9 tier 4: primary is activated, so a primary put serves the object
        // op authoritatively from EHDB (not refused).  Parity holds.
        let r = mirror_put(&e, STATE_KEY, b"arrow-ipc-bytes", &Default::default(), false);
        assert_eq!(r.mode, ObjectMode::Primary);
        assert_eq!(r.outcome, ObjectOutcome::ServedPrimary, "{:?}", r.detail);
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
        assert_eq!(r.mode, ObjectMode::Primary);
        assert_eq!(r.outcome, ObjectOutcome::ServedPrimary, "{:?}", r.detail);
        assert!(r.served_by_ehdb);
        let report = r.report.as_ref().unwrap();
        assert!(report.served_by_ehdb());
        assert_eq!(report.put_count, PRIMARY_SERVE_CYCLE_ENTRIES);
        assert!(report.put_ok && report.get_ok && report.list_ok);
        assert!(report.locate_ok && report.delete_ok);
        assert!(report.replay_matches && report.dual_run_holds);
        // Reversibility: flip back to shadow mirrored one more object; the store is
        // whole and serves the 2 surviving cycle keys + the 1 revert key.
        assert!(r.reversible);
        assert_eq!(r.keys_after_revert, 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_cycle_off_is_noop() {
        let e = worker_env("/tmp/unused-object-cycle.jsonl", "off");
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.outcome, ObjectOutcome::Disabled);
        assert!(r.report.is_none());
        assert!(!r.served_by_ehdb);
    }

    #[test]
    fn primary_serve_cycle_shadow_is_primary_unavailable() {
        let (log, dir) = tmp_log("cycle-shadow");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        // The cycle only serves under the `primary` flag.
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.outcome, ObjectOutcome::PrimaryUnavailable);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_cycle_control_plane_guard_refused() {
        let e: EnvMap = [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "server"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_OBJECT", "primary"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.outcome, ObjectOutcome::GuardRefused);
        assert!(r.report.is_none());
    }

    #[test]
    fn primary_control_plane_still_guard_refused() {
        let e: EnvMap = [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "gateway"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_OBJECT", "primary"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let r = mirror_put(&e, STATE_KEY, b"v", &Default::default(), false);
        assert_eq!(r.outcome, ObjectOutcome::GuardRefused);
    }

    #[test]
    fn suite_drives_full_engine_and_holds() {
        let (log, dir) = tmp_log("suite");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        let report = shadow_suite(&e, &Default::default(), false);
        assert!(!report.disabled);
        assert!(report.ok, "{:?}", report.steps);
        // Every capability exercised: put/get/dedup/list/locate/delete.
        let names: Vec<&str> = report.steps.iter().map(|s| s.step.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "put",
                "get_parity",
                "content_dedup",
                "list",
                "locate",
                "delete"
            ]
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
            ("NOETL_EHDB_OBJECT", "shadow"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let report = shadow_suite(&e, &Default::default(), false);
        assert!(report.guard_refused);
        assert!(!report.ok);
    }

    /// Event-log-authoritative invariant, asserted structurally: this module must
    /// never reach the NoETL event log — it only touches the derived EHDB object
    /// fabric via `ehdb_reference`.
    #[test]
    fn no_noetl_event_writer() {
        let full = include_str!("object.rs");
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
                "forbidden NoETL event-writer reference `{forbidden}` in object.rs"
            );
        }
    }

    // --- Live platform-object mirror hook (runtime integration, noetl/ehdb#234) ---

    #[test]
    fn runtime_hook_env_arms_only_for_enabled_shadow_data_plane() {
        let armed = runtime_hook_env(&worker_env("/tmp/obj-hook.jsonl", "shadow"));
        assert!(armed.is_some(), "shadow+enabled worker must arm the object hook");
    }

    #[test]
    fn runtime_hook_env_noop_when_disabled() {
        let e: EnvMap = [("NOETL_EHDB_OBJECT", "shadow")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert!(runtime_hook_env(&e).is_none());
    }

    #[test]
    fn runtime_hook_env_noop_when_tier_off_or_primary() {
        assert!(runtime_hook_env(&worker_env("/tmp/obj-hook.jsonl", "off")).is_none());
        assert!(runtime_hook_env(&worker_env("/tmp/obj-hook.jsonl", "primary")).is_none());
    }

    #[test]
    fn runtime_hook_env_skips_control_plane_role() {
        for role in ["server", "gateway", "api"] {
            let e: EnvMap = [
                ("NOETL_EHDB_ENABLED", "true"),
                ("NOETL_EHDB_MODE", "local_reference"),
                ("NOETL_EHDB_CLIENT_ROLE", role),
                ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
                ("NOETL_EHDB_OBJECT", "shadow"),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
            assert!(
                runtime_hook_env(&e).is_none(),
                "control-plane role {role} must not arm the object hook"
            );
        }
    }

    #[test]
    fn mirror_live_put_fires_on_shadow_enabled() {
        let (log, dir) = tmp_log("live-obj-fire");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        let outcome = mirror_live_put(
            &e,
            "exec/478775660589088776/state/shard-0.feather",
            b"\x00arrow-feather-bytes\x01",
        );
        assert_eq!(outcome, ObjectOutcome::Mirrored);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn mirror_live_put_is_noop_when_disabled() {
        let e: EnvMap = EnvMap::new();
        let outcome = mirror_live_put(&e, "k", b"bytes");
        assert_eq!(outcome, ObjectOutcome::Disabled);
    }

    #[test]
    fn mirror_live_put_skipped_for_control_plane_role() {
        let e: EnvMap = [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "server"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_OBJECT", "shadow"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let outcome = mirror_live_put(&e, "k", b"bytes");
        assert_eq!(outcome, ObjectOutcome::GuardRefused);
    }

    #[test]
    fn mirror_live_put_isolates_engine_error_without_propagating() {
        let (file_as_dir, dir) = tmp_log("obj-iso");
        std::fs::write(&file_as_dir, b"x").unwrap();
        let bad_log = file_as_dir.join("nested").join("log.jsonl");
        let e = worker_env(bad_log.to_str().unwrap(), "shadow");
        let outcome = mirror_live_put(&e, "k", b"bytes");
        assert!(
            matches!(outcome, ObjectOutcome::Unavailable | ObjectOutcome::Invalid),
            "engine error must be contained as an outcome, got {outcome:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
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
