//! Disabled-by-default KV / platform-state SHADOW wiring (EHDB Phase 8).
//!
//! EHDB's KV/state core engine (the `ehdb_reference::kv` driver, ehdb#244) is the
//! durable key/value engine that Phase 8 puts *underneath* NoETL's internal
//! **NATS JetStream Key-Value** platform tier — the worker's
//! `noetl_subscription_circuit` breaker store today, the #115 program-scale
//! coherence keys as they move off NATS-KV.  This module is the worker's
//! **driver-selection seam** for that engine, gated by `NOETL_EHDB_KV`:
//!
//! * `off` (default) — strict no-op.  No engine opened, no metric recorded; the
//!   worker's `/metrics` and behaviour are byte-identical to a build without the
//!   KV wiring.
//! * `shadow` — **dual-write + compare, never serve.**  A platform KV write is
//!   *also* applied to the EHDB engine alongside the authoritative NATS-KV path,
//!   then read back and compared for presence / value / TTL parity.  Reads are
//!   **never** served from EHDB and the authoritative NATS-KV path is untouched.
//! * `primary` — recognised but **NOT activated this session**.  Cutover to
//!   serving KV from EHDB is a later gated step; requesting `primary` here is
//!   refused with a distinct outcome and the worker stays on NATS-KV.
//!   [`PRIMARY_SERVE_ACTIVATED`] is a compile-time `false` so it is structurally
//!   impossible for this build to serve primary.
//!
//! ## Boundaries (mirror the rest of `src/ehdb`)
//!
//! * Disabled-by-default no-op (byte-identical `/metrics`).
//! * Control-plane roles (`gateway`/`api`/`server`) refused before any engine
//!   opens — the gateway never touches the data plane.
//! * Bounded (value byte cap) + stateless (engine opened + dropped per op).
//! * **Event-log-authoritative** — a KV entry is derived platform state, not an
//!   event; this module never authors a NoETL event and never reaches
//!   `noetl.event` / `POST /api/events` (structurally asserted).  It only touches
//!   the derived EHDB KV fabric via `ehdb_reference`.

use ehdb_reference::{
    compare_kv_parity, AuthoritativeKvEntry, KvCasExpectation, KvDeleteRequest, KvGetRequest,
    KvParityReport, KvPutRequest, KvScanRequest, KvStateDriver, LocalReferenceKvStateDriver,
    DEFAULT_LOCAL_REFERENCE_NAMESPACE, DEFAULT_LOCAL_REFERENCE_TENANT,
};

use super::contract::{contract_from_env, EhdbClientRole, EhdbContract, EHDB_ENABLED_ENV};
use super::guard::assert_data_plane_access_allowed;
use super::{metrics, EnvMap};
use crate::snowflake::SnowflakeGen;
use std::sync::OnceLock;

/// The driver-selection flag for the KV tier.
pub const KV_MODE_ENV: &str = "NOETL_EHDB_KV";
/// Value byte cap for one KV write.
pub const MAX_VALUE_BYTES_ENV: &str = "NOETL_EHDB_KV_MAX_VALUE_BYTES";
const DEFAULT_MAX_VALUE_BYTES: usize = 262_144;
/// Hard ceiling — the crate engine rejects a value above `MAX_KV_VALUE_BYTES`
/// (1 MiB), so the worker-side clamp never exceeds it.
const MAX_VALUE_BYTES_CEILING: usize = 1_048_576;

/// Compile-time guard: this build never serves KV from EHDB.  Phase 8 ships the
/// shadow only; flipping this to `true` is the later, separately-gated primary
/// cutover and is intentionally not reachable from config.
pub const PRIMARY_SERVE_ACTIVATED: bool = false;

/// Which KV engine the tier is driven by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvMode {
    /// No EHDB engine; the incumbent NATS-KV path is authoritative.
    Off,
    /// Dual-write into EHDB + compare; never serve reads from it.
    Shadow,
    /// Serve KV from EHDB — recognised but not activated this session.
    Primary,
}

impl KvMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            KvMode::Off => "off",
            KvMode::Shadow => "shadow",
            KvMode::Primary => "primary",
        }
    }

    /// Parse the mode from the env, defaulting to `Off`.  An unrecognised value
    /// is treated as `Off` (fail-safe: an unknown driver never mirrors).
    pub fn from_env(env: &EnvMap) -> Self {
        match env
            .get(KV_MODE_ENV)
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("shadow") => KvMode::Shadow,
            Some("primary") => KvMode::Primary,
            _ => KvMode::Off,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvOutcome {
    /// Off mode / EHDB disabled — strict no-op.
    Disabled,
    /// Write mirrored into EHDB and parity held.
    Mirrored,
    /// Write mirrored but the EHDB engine diverged from the authoritative view.
    ParityMismatch,
    /// `primary` requested but primary-serve is not activated this session.
    PrimaryUnavailable,
    /// Value over the byte cap.
    Rejected,
    /// A control-plane role reached the data-plane engine — refused.
    GuardRefused,
    /// Caller mistake (bad bucket / key / config).
    Invalid,
    /// The engine errored at runtime.
    Unavailable,
}

impl KvOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            KvOutcome::Disabled => "disabled",
            KvOutcome::Mirrored => "mirrored",
            KvOutcome::ParityMismatch => "parity_mismatch",
            KvOutcome::PrimaryUnavailable => "primary_unavailable",
            KvOutcome::Rejected => "rejected",
            KvOutcome::GuardRefused => "guard_refused",
            KvOutcome::Invalid => "invalid",
            KvOutcome::Unavailable => "unavailable",
        }
    }

    pub fn ok(&self) -> bool {
        matches!(self, KvOutcome::Disabled | KvOutcome::Mirrored)
    }

    /// A degraded (but non-fatal) outcome — surfaces on the `last_degraded` gauge
    /// so a divergence or engine hiccup is visible without failing the
    /// authoritative path.
    fn degraded(&self) -> bool {
        matches!(self, KvOutcome::ParityMismatch | KvOutcome::Unavailable)
    }
}

/// Secret-free result of one shadow KV op.
#[derive(Debug, Clone)]
pub struct KvResult {
    pub mode: KvMode,
    pub outcome: KvOutcome,
    pub role: Option<EhdbClientRole>,
    pub duration_seconds: f64,
    pub detail: Option<String>,
    /// The per-key version EHDB assigned (present on a successful mirror).
    pub version: Option<u64>,
    /// The parity verdict (present when a mirror ran).
    pub parity: Option<KvParityReport>,
}

#[derive(Debug, Clone, Default)]
pub struct KvOptions {
    pub tenant: Option<String>,
    pub namespace: Option<String>,
    pub transaction_id: Option<String>,
}

fn txn_gen() -> &'static SnowflakeGen {
    static GEN: OnceLock<SnowflakeGen> = OnceLock::new();
    GEN.get_or_init(|| SnowflakeGen::from_env_or_hint("ehdb-kv"))
}

fn new_transaction_id() -> String {
    format!("ehdbkv-{}", txn_gen().next_id())
}

fn truthy(env: &EnvMap, key: &str) -> bool {
    matches!(
        env.get(key)
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref(),
        Some("1" | "true" | "yes" | "y" | "on")
    )
}

fn bounded_max_value_bytes(env: &EnvMap) -> usize {
    env.get(MAX_VALUE_BYTES_ENV)
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_VALUE_BYTES)
        .clamp(1, MAX_VALUE_BYTES_CEILING)
}

fn tenant_of(opts: &KvOptions) -> String {
    opts.tenant
        .clone()
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string())
}

fn namespace_of(opts: &KvOptions) -> String {
    opts.namespace
        .clone()
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string())
}

/// Build a result (and record its metric under `operation`).  `version` / `parity`
/// are set by the success path afterward.
fn make_result(
    operation: &str,
    mode: KvMode,
    outcome: KvOutcome,
    role: Option<EhdbClientRole>,
    started: std::time::Instant,
    detail: Option<String>,
    record_metrics: bool,
) -> KvResult {
    let duration_seconds = started.elapsed().as_secs_f64();
    if record_metrics {
        metrics::record_kv(
            operation,
            outcome.as_str(),
            outcome.ok(),
            outcome.degraded(),
            duration_seconds,
        );
    }
    KvResult {
        mode,
        outcome,
        role,
        duration_seconds,
        detail,
        version: None,
        parity: None,
    }
}

/// Classified by the crate error's Display since the crate does not re-export its
/// error enum: an identifier validation failure is a caller mistake (`Invalid`),
/// an over-cap value is a caller `Rejected`, any other runtime error is
/// `Unavailable`.
fn classify_helper_error<E: std::fmt::Display>(err: &E) -> KvOutcome {
    let msg = err.to_string();
    if msg.starts_with("invalid identifier") {
        KvOutcome::Invalid
    } else if msg.contains("exceeds bound") {
        KvOutcome::Rejected
    } else {
        KvOutcome::Unavailable
    }
}

/// Resolve the disabled-by-default contract for a KV op, refusing control-plane
/// roles before any engine opens.  Returns a ready result on any early exit.
fn resolve_contract(
    operation: &'static str,
    env: &EnvMap,
    mode: KvMode,
    started: std::time::Instant,
    record_metrics: bool,
) -> Result<EhdbContract, Box<KvResult>> {
    let finish = |outcome: KvOutcome, role: Option<EhdbClientRole>, detail: Option<String>| {
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
                KvOutcome::GuardRefused
            } else {
                KvOutcome::Invalid
            };
            return Err(finish(outcome, role, Some(err.0)));
        }
    };

    if let Err(err) = assert_data_plane_access_allowed(contract.role, operation) {
        return Err(finish(
            KvOutcome::GuardRefused,
            Some(contract.role),
            Some(err.to_string()),
        ));
    }
    if !contract.uses_local_reference_runtime() || contract.local_reference_log.is_none() {
        return Err(finish(KvOutcome::Disabled, Some(contract.role), None));
    }
    Ok(contract)
}

fn driver_from(contract: &EhdbContract, opts: &KvOptions) -> LocalReferenceKvStateDriver {
    LocalReferenceKvStateDriver::new(
        contract.local_reference_log.clone().expect("log present"),
        tenant_of(opts),
        namespace_of(opts),
    )
}

/// Guard the common `off` / `disabled` / `primary` short-circuits shared by every
/// KV shadow op.  Returns `Ok(contract)` when the shadow may proceed, else a
/// ready result.
fn enter_shadow(
    operation: &'static str,
    env: &EnvMap,
    started: std::time::Instant,
    record_metrics: bool,
) -> Result<(KvMode, EhdbContract), Box<KvResult>> {
    let mode = KvMode::from_env(env);

    // Off mode OR the umbrella EHDB switch disabled ⇒ strict no-op.
    if mode == KvMode::Off || !truthy(env, EHDB_ENABLED_ENV) {
        return Err(Box::new(make_result(
            operation,
            mode,
            KvOutcome::Disabled,
            None,
            started,
            None,
            record_metrics,
        )));
    }

    // Primary is recognised but not activated this session — refuse before any
    // engine opens (a control-plane role is still refused as a guard first).
    if mode == KvMode::Primary && !PRIMARY_SERVE_ACTIVATED {
        let contract = resolve_contract(operation, env, mode, started, record_metrics)?;
        return Err(Box::new(make_result(
            operation,
            mode,
            KvOutcome::PrimaryUnavailable,
            Some(contract.role),
            started,
            Some("kv primary serve is not activated in this build".to_string()),
            record_metrics,
        )));
    }

    let contract = resolve_contract(operation, env, mode, started, record_metrics)?;
    Ok((mode, contract))
}

/// Dual-write one platform KV entry into the EHDB engine (shadow) and compare the
/// engine's read-back against the authoritative value just written (presence /
/// value / TTL parity).
///
/// This NEVER serves reads to the control plane and NEVER authors a NoETL event —
/// it only writes the derived EHDB KV fabric and reports parity.  The
/// authoritative NATS-KV write is the caller's responsibility and is untouched
/// here.
pub fn mirror_put(
    env: &EnvMap,
    bucket: &str,
    key: &str,
    value: &str,
    expires_at_ms: Option<u64>,
    opts: &KvOptions,
    record_metrics: bool,
) -> KvResult {
    let started = std::time::Instant::now();
    let (mode, contract) = match enter_shadow("mirror", env, started, record_metrics) {
        Ok(pair) => pair,
        Err(result) => return *result,
    };

    let max_bytes = bounded_max_value_bytes(env);
    if value.len() > max_bytes {
        return make_result(
            "mirror",
            mode,
            KvOutcome::Rejected,
            Some(contract.role),
            started,
            Some(format!(
                "value {} bytes exceeds bound {max_bytes}",
                value.len()
            )),
            record_metrics,
        );
    }

    let driver = driver_from(&contract, opts);
    let put_request = KvPutRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        value: value.to_string(),
        expires_at_ms,
        cas: None,
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

    // Read back (bounded, `now_ms = None` so TTL metadata is compared rather than
    // filtered) and compare against the authoritative value we just wrote.  The
    // read-back stays inside this module — it is the shadow's parity input, NOT a
    // read served to the control plane.
    let get = match driver.get(&KvGetRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        now_ms: None,
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

    let authoritative = AuthoritativeKvEntry {
        value: value.to_string(),
        expires_at_ms,
    };
    let report = compare_kv_parity(Some(&authoritative), &get);
    let result_outcome = if report.holds() {
        KvOutcome::Mirrored
    } else {
        KvOutcome::ParityMismatch
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
    result.parity = Some(report);
    result
}

/// One step of the deterministic KV shadow drive.
#[derive(Debug, Clone)]
pub struct KvSuiteStep {
    pub step: String,
    pub outcome: String,
    pub detail: Option<String>,
}

/// Secret-free report of the full KV shadow drive (`kv-suite`).
#[derive(Debug, Clone)]
pub struct KvSuiteReport {
    pub mode: KvMode,
    pub disabled: bool,
    pub ok: bool,
    pub role: Option<EhdbClientRole>,
    pub steps: Vec<KvSuiteStep>,
    pub duration_seconds: f64,
    /// A control-plane role reached the data-plane engine — refused.
    pub guard_refused: bool,
    /// `primary` requested but not activated this session.
    pub primary_unavailable: bool,
}

/// Run a deterministic put / get / CAS-conflict / CAS-swap / delete / scan / TTL
/// drive against the EHDB KV engine in ONE contract/guard resolution, exercising
/// every engine capability behind the disabled-by-default seam.  Reads are the
/// shadow's own validation reads — never served to the control plane.  Disabled ⇒
/// a no-op report (`disabled = true`).
pub fn shadow_suite(env: &EnvMap, opts: &KvOptions, record_metrics: bool) -> KvSuiteReport {
    let started = std::time::Instant::now();
    let (mode, contract) = match enter_shadow("suite", env, started, record_metrics) {
        Ok(pair) => pair,
        Err(result) => {
            let r = *result;
            return KvSuiteReport {
                mode: r.mode,
                disabled: r.outcome == KvOutcome::Disabled,
                ok: r.outcome == KvOutcome::Disabled,
                role: r.role,
                steps: Vec::new(),
                duration_seconds: r.duration_seconds,
                guard_refused: r.outcome == KvOutcome::GuardRefused,
                primary_unavailable: r.outcome == KvOutcome::PrimaryUnavailable,
            };
        }
    };

    let driver = driver_from(&contract, opts);
    let bucket = "noetl_kv_selfcheck";
    let mut steps = Vec::new();
    let mut ok = true;

    let mut txn = 0u64;
    let mut next_txn = || {
        txn += 1;
        format!("kvsuite-{txn}")
    };

    // put k1 = v1 → get → parity vs {v1}.
    let put1 = driver.put(&KvPutRequest {
        bucket: bucket.to_string(),
        key: "circuit.1".to_string(),
        value: "{\"phase\":\"closed\"}".to_string(),
        expires_at_ms: None,
        cas: None,
        transaction_id: next_txn(),
    });
    let put1_ok = matches!(&put1, Ok(o) if o.written);
    ok &= put1_ok;
    steps.push(step(
        "put",
        put1_ok,
        put1.as_ref().err().map(|e| e.to_string()),
    ));

    let get1 = driver.get(&KvGetRequest {
        bucket: bucket.to_string(),
        key: "circuit.1".to_string(),
        now_ms: None,
    });
    let parity1 = get1.as_ref().ok().map(|g| {
        compare_kv_parity(
            Some(&AuthoritativeKvEntry {
                value: "{\"phase\":\"closed\"}".to_string(),
                expires_at_ms: None,
            }),
            g,
        )
    });
    let get1_ok = parity1.as_ref().map(|p| p.holds()).unwrap_or(false);
    ok &= get1_ok;
    steps.push(step(
        "get_parity",
        get1_ok,
        parity1.and_then(|p| p.divergence),
    ));

    // CAS Absent on the existing key → conflict.
    let cas_absent = driver.put(&KvPutRequest {
        bucket: bucket.to_string(),
        key: "circuit.1".to_string(),
        value: "x".to_string(),
        expires_at_ms: None,
        cas: Some(KvCasExpectation::Absent),
        transaction_id: next_txn(),
    });
    let cas_absent_ok = matches!(&cas_absent, Ok(o) if o.cas_conflict && !o.written);
    ok &= cas_absent_ok;
    steps.push(step("cas_absent_conflict", cas_absent_ok, None));

    // CAS Version(1) → swap to v2.
    let cas_swap = driver.put(&KvPutRequest {
        bucket: bucket.to_string(),
        key: "circuit.1".to_string(),
        value: "{\"phase\":\"open\"}".to_string(),
        expires_at_ms: None,
        cas: Some(KvCasExpectation::Version(1)),
        transaction_id: next_txn(),
    });
    let cas_swap_ok = matches!(&cas_swap, Ok(o) if o.written && o.version == 2);
    ok &= cas_swap_ok;
    steps.push(step("cas_version_swap", cas_swap_ok, None));

    // delete → get absent.
    let del = driver.delete(&KvDeleteRequest {
        bucket: bucket.to_string(),
        key: "circuit.1".to_string(),
        transaction_id: next_txn(),
    });
    let del_get = driver.get(&KvGetRequest {
        bucket: bucket.to_string(),
        key: "circuit.1".to_string(),
        now_ms: None,
    });
    let del_ok = matches!(&del, Ok(o) if o.existed) && matches!(&del_get, Ok(g) if !g.found);
    ok &= del_ok;
    steps.push(step("delete", del_ok, None));

    // TTL: put with expiry, read before + after.
    let _ = driver.put(&KvPutRequest {
        bucket: bucket.to_string(),
        key: "lease.a".to_string(),
        value: "v".to_string(),
        expires_at_ms: Some(1_000),
        cas: None,
        transaction_id: next_txn(),
    });
    let before = driver.get(&KvGetRequest {
        bucket: bucket.to_string(),
        key: "lease.a".to_string(),
        now_ms: Some(999),
    });
    let after = driver.get(&KvGetRequest {
        bucket: bucket.to_string(),
        key: "lease.a".to_string(),
        now_ms: Some(1_000),
    });
    let ttl_ok =
        matches!(&before, Ok(g) if g.found) && matches!(&after, Ok(g) if !g.found && g.expired);
    ok &= ttl_ok;
    steps.push(step("ttl_expiry", ttl_ok, None));

    // scan: circuit.1 deleted, lease.a TTL-live at now=500 → one live key.
    let scan = driver.scan(&KvScanRequest {
        bucket: bucket.to_string(),
        prefix: None,
        limit: 100,
        now_ms: Some(500),
    });
    let scan_ok = matches!(&scan, Ok(s) if s.exists && s.match_count == 1 && s.entries.iter().all(|e| e.key == "lease.a"));
    ok &= scan_ok;
    steps.push(step("scan", scan_ok, None));

    if record_metrics {
        for s in &steps {
            metrics::record_kv(
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

    KvSuiteReport {
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

fn step(name: &str, ok: bool, detail: Option<String>) -> KvSuiteStep {
    KvSuiteStep {
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
            ("NOETL_EHDB_KV", mode),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn tmp_log(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-kv-worker-{tag}-{}-{:?}",
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
        let r = mirror_put(&e, "b", "k", "v", None, &Default::default(), false);
        assert_eq!(r.mode, KvMode::Off);
        assert_eq!(r.outcome, KvOutcome::Disabled);
        assert!(r.parity.is_none());
    }

    #[test]
    fn ehdb_disabled_is_noop_even_in_shadow() {
        let e: EnvMap = [("NOETL_EHDB_KV", "shadow")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let r = mirror_put(&e, "b", "k", "v", None, &Default::default(), false);
        assert_eq!(r.outcome, KvOutcome::Disabled);
    }

    #[test]
    fn shadow_mirror_holds_parity() {
        let (log, dir) = tmp_log("shadow");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        let r = mirror_put(
            &e,
            "noetl_subscription_circuit",
            "circuit.12345",
            "{\"phase\":\"closed\"}",
            None,
            &Default::default(),
            false,
        );
        assert_eq!(r.outcome, KvOutcome::Mirrored, "{:?}", r.detail);
        assert_eq!(r.version, Some(1));
        assert!(r.parity.as_ref().unwrap().holds());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shadow_mirror_round_trips_ttl_metadata() {
        let (log, dir) = tmp_log("ttl");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        let r = mirror_put(
            &e,
            "b",
            "lease.a",
            "v",
            Some(9_000),
            &Default::default(),
            false,
        );
        assert_eq!(r.outcome, KvOutcome::Mirrored, "{:?}", r.detail);
        assert!(r.parity.as_ref().unwrap().ttl_ok);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_oversized_value() {
        let (log, dir) = tmp_log("bounds");
        let mut e = worker_env(log.to_str().unwrap(), "shadow");
        e.insert(MAX_VALUE_BYTES_ENV.to_string(), "4".to_string());
        let r = mirror_put(&e, "b", "k", "toolong", None, &Default::default(), false);
        assert_eq!(r.outcome, KvOutcome::Rejected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_bucket_is_invalid() {
        let (log, dir) = tmp_log("badid");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        // A dotted bucket splits across subject tokens → invalid identifier.
        let r = mirror_put(&e, "bad.bucket", "k", "v", None, &Default::default(), false);
        assert_eq!(r.outcome, KvOutcome::Invalid);
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
            ("NOETL_EHDB_KV", "shadow"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let r = mirror_put(&e, "b", "k", "v", None, &Default::default(), false);
        assert_eq!(r.outcome, KvOutcome::GuardRefused);
        assert!(r.version.is_none());
    }

    #[test]
    fn primary_is_recognised_but_not_activated() {
        let (log, dir) = tmp_log("primary");
        let e = worker_env(log.to_str().unwrap(), "primary");
        let r = mirror_put(&e, "b", "k", "v", None, &Default::default(), false);
        assert_eq!(r.mode, KvMode::Primary);
        assert_eq!(r.outcome, KvOutcome::PrimaryUnavailable);
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
            ("NOETL_EHDB_KV", "primary"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let r = mirror_put(&e, "b", "k", "v", None, &Default::default(), false);
        assert_eq!(r.outcome, KvOutcome::GuardRefused);
    }

    #[test]
    fn suite_drives_full_engine_and_holds() {
        let (log, dir) = tmp_log("suite");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        let report = shadow_suite(&e, &Default::default(), false);
        assert!(!report.disabled);
        assert!(report.ok, "{:?}", report.steps);
        // Every capability exercised: put/get/CAS-conflict/CAS-swap/delete/TTL/scan.
        let names: Vec<&str> = report.steps.iter().map(|s| s.step.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "put",
                "get_parity",
                "cas_absent_conflict",
                "cas_version_swap",
                "delete",
                "ttl_expiry",
                "scan"
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
            ("NOETL_EHDB_KV", "shadow"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let report = shadow_suite(&e, &Default::default(), false);
        assert!(report.guard_refused);
        assert!(!report.ok);
    }

    /// Event-log-authoritative invariant, asserted structurally: this module must
    /// never reach the NoETL event log — it only touches the derived EHDB KV
    /// fabric via `ehdb_reference`.
    #[test]
    fn no_noetl_event_writer() {
        let full = include_str!("kv.rs");
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
                "forbidden NoETL event-writer reference `{forbidden}` in kv.rs"
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
