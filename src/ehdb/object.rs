//! Disabled-by-default object / blob SHADOW wiring (EHDB Phase 8).
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
//! * `primary` — recognised but **NOT activated this session**.  Cutover to
//!   serving objects from EHDB is a later gated step; requesting `primary` here is
//!   refused with a distinct outcome and the worker stays on the external store.
//!   [`PRIMARY_SERVE_ACTIVATED`] is a compile-time `false` so it is structurally
//!   impossible for this build to serve primary.
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
//!   the derived EHDB object fabric via `ehdb_reference`.

use std::path::PathBuf;

use ehdb_reference::{
    compare_object_parity, AuthoritativeObject, LocalReferenceObjectBlobDriver, ObjectBlobDriver,
    ObjectDeleteRequest, ObjectGetRequest, ObjectListRequest, ObjectLocateRequest,
    ObjectParityReport, ObjectPutRequest, DEFAULT_LOCAL_REFERENCE_NAMESPACE,
    DEFAULT_LOCAL_REFERENCE_TENANT,
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

/// Compile-time guard: this build never serves objects from EHDB.  Phase 8 ships
/// the shadow only; flipping this to `true` is the later, separately-gated primary
/// cutover and is intentionally not reachable from config.
pub const PRIMARY_SERVE_ACTIVATED: bool = false;

/// Which object engine the tier is driven by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectMode {
    /// No EHDB engine; the incumbent external-store path is authoritative.
    Off,
    /// Dual-write into EHDB + compare; never serve reads from it.
    Shadow,
    /// Serve objects from EHDB — recognised but not activated this session.
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
    /// `primary` requested but primary-serve is not activated this session.
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
            ObjectOutcome::PrimaryUnavailable => "primary_unavailable",
            ObjectOutcome::Rejected => "rejected",
            ObjectOutcome::GuardRefused => "guard_refused",
            ObjectOutcome::Invalid => "invalid",
            ObjectOutcome::Unavailable => "unavailable",
        }
    }

    pub fn ok(&self) -> bool {
        matches!(self, ObjectOutcome::Disabled | ObjectOutcome::Mirrored)
    }

    /// A degraded (but non-fatal) outcome — surfaces on the `last_degraded` gauge
    /// so a divergence or engine hiccup is visible without failing the
    /// authoritative path.
    fn degraded(&self) -> bool {
        matches!(
            self,
            ObjectOutcome::ParityMismatch | ObjectOutcome::Unavailable
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
    let result_outcome = if report.holds() {
        ObjectOutcome::Mirrored
    } else {
        ObjectOutcome::ParityMismatch
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
    fn primary_is_recognised_but_not_activated() {
        let (log, dir) = tmp_log("primary");
        let e = worker_env(log.to_str().unwrap(), "primary");
        let r = mirror_put(&e, STATE_KEY, b"v", &Default::default(), false);
        assert_eq!(r.mode, ObjectMode::Primary);
        assert_eq!(r.outcome, ObjectOutcome::PrimaryUnavailable);
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
