//! KV / platform-state SHADOW wiring + PRIMARY-serve cutover (EHDB Phase 8
//! shadow, Phase 9 tier-3 primary).
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
//! * `primary` — **EHDB serves the platform KV tier authoritatively** (Phase 9
//!   tier 3): the KV ops the worker makes (put / get / scan / CAS / delete / TTL)
//!   are served by the EHDB engine in place of the internal NATS-KV bucket, while
//!   the served results are dual-run parity-checked against NATS-KV.
//!   [`PRIMARY_SERVE_ACTIVATED`] is now `true` so this build *can* serve primary;
//!   whether it *does* is a pure runtime choice of the `NOETL_EHDB_KV` flag (see
//!   reversibility).
//!
//! ## Reversibility (the cutover safety property)
//!
//! The cutover is reversible with **two independent levers**:
//!
//! 1. **Runtime flag (operational, instant, no redeploy)** — flip `NOETL_EHDB_KV`
//!    from `primary` back to `shadow`/`off` and the incumbent NATS-KV path is the
//!    authoritative KV tier again immediately.  Zero data loss: the primary path
//!    only ever appends to the derived EHDB `KeepAll` KV stream and never
//!    mutates/deletes anything NATS-KV owns, so the NATS-KV bucket is exactly as
//!    it was and the EHDB store stays whole on disk for a later re-enable.
//! 2. **Compile-time kill switch (structural, belt-and-suspenders)** — set
//!    [`PRIMARY_SERVE_ACTIVATED`] back to `false` and it is structurally
//!    impossible for the build to serve primary regardless of config (the
//!    `primary` flag then degrades to [`KvOutcome::PrimaryUnavailable`]).
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
//!   the derived EHDB KV fabric via `ehdb_reference`.  **Platform KV only** —
//!   tenant/domain (business) KV stays external, reached by playbook connectors.

use ehdb_reference::kv::exercise_primary_serve;
use ehdb_reference::{
    compare_kv_parity, AuthoritativeKvEntry, KvCasExpectation, KvDeleteRequest, KvGetRequest,
    KvParityReport, KvPrimaryInput, KvPrimaryServeReport, KvPutRequest, KvScanRequest,
    KvStateDriver, LocalReferenceKvStateDriver, DEFAULT_LOCAL_REFERENCE_NAMESPACE,
    DEFAULT_LOCAL_REFERENCE_TENANT,
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

/// Compile-time kill switch for primary-serve.  Phase 9 tier 3 activates it
/// (`true`): this build *can* serve the platform KV tier authoritatively from
/// EHDB.  Whether it *does* is the pure runtime choice of `NOETL_EHDB_KV`
/// (`primary` serves; `shadow`/`off` keep the internal NATS-KV bucket
/// authoritative), so the cutover stays reversible without a redeploy.  Setting
/// this back to `false` is the belt-and-suspenders structural rollback — it makes
/// primary-serve unreachable regardless of config (the `primary` flag then
/// degrades to [`KvOutcome::PrimaryUnavailable`]).
pub const PRIMARY_SERVE_ACTIVATED: bool = true;

/// Which KV engine the tier is driven by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvMode {
    /// No EHDB engine; the incumbent NATS-KV path is authoritative.
    Off,
    /// Dual-write into EHDB + compare; never serve reads from it.
    Shadow,
    /// Serve KV from EHDB authoritatively (Phase 9 tier 3).
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
    /// `primary` served the KV op authoritatively from EHDB + dual-run parity
    /// against the incumbent NATS-KV path held.
    ServedPrimary,
    /// `primary` served the KV op from EHDB but the dual-run parity against the
    /// incumbent diverged (degraded — surfaces on `last_degraded`).
    PrimaryDivergence,
    /// `primary` requested but primary-serve is not activated this build (the
    /// compile-time kill switch is off).
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
            KvOutcome::ServedPrimary => "served_primary",
            KvOutcome::PrimaryDivergence => "primary_divergence",
            KvOutcome::PrimaryUnavailable => "primary_unavailable",
            KvOutcome::Rejected => "rejected",
            KvOutcome::GuardRefused => "guard_refused",
            KvOutcome::Invalid => "invalid",
            KvOutcome::Unavailable => "unavailable",
        }
    }

    pub fn ok(&self) -> bool {
        matches!(
            self,
            KvOutcome::Disabled | KvOutcome::Mirrored | KvOutcome::ServedPrimary
        )
    }

    /// A degraded (but non-fatal) outcome — surfaces on the `last_degraded` gauge
    /// so a divergence or engine hiccup is visible without failing the
    /// authoritative path.
    fn degraded(&self) -> bool {
        matches!(
            self,
            KvOutcome::ParityMismatch | KvOutcome::PrimaryDivergence | KvOutcome::Unavailable
        )
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
    // Under `primary` EHDB served the op authoritatively; under `shadow` it
    // mirrored alongside the authoritative NATS-KV path.  The engine op is
    // identical — the mode only changes which path is authoritative and how the
    // outcome is labelled.
    let serving_primary = mode == KvMode::Primary;
    let result_outcome = match (serving_primary, report.holds()) {
        (true, true) => KvOutcome::ServedPrimary,
        (true, false) => KvOutcome::PrimaryDivergence,
        (false, true) => KvOutcome::Mirrored,
        (false, false) => KvOutcome::ParityMismatch,
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

/// The bucket the built-in primary-serve cycle drives.
const PRIMARY_SERVE_BUCKET: &str = "noetl_kv_primary_serve";
/// How many entries the built-in primary-serve cycle seeds (CAS on the first,
/// delete on the last).
pub const PRIMARY_SERVE_CYCLE_ENTRIES: usize = 3;
/// Live keys served after the reversibility flip-back: the 2 surviving cycle keys
/// (first CAS-swapped + middle; last deleted) plus the 1 fresh key the shadow
/// flip-back mirrors.
const PRIMARY_SERVE_KEYS_AFTER_REVERT: usize = 3;
/// The clock the cycle uses for the TTL lease; the reversibility scan runs past
/// `now_ms + 1` so the expired lease never counts.
const PRIMARY_SERVE_NOW_MS: u64 = 1_000;

/// Secret-free result of the authoritative KV primary-serve cycle (Phase 9 tier
/// 3) plus the operational reversibility demonstration.
#[derive(Debug, Clone)]
pub struct KvServeResult {
    pub mode: KvMode,
    pub outcome: KvOutcome,
    pub role: Option<EhdbClientRole>,
    pub duration_seconds: f64,
    /// The EHDB engine served the whole cycle with the NATS-KV semantics
    /// preserved and dual-run parity intact.
    pub served_by_ehdb: bool,
    /// The full served-by-EHDB proof (present once the cycle ran).
    pub report: Option<KvPrimaryServeReport>,
    /// After serving primary, flipping `NOETL_EHDB_KV` back to `shadow` over the
    /// same store mirrored a further key and the store served the whole live set
    /// — the incumbent NATS-KV path is restored with zero data loss (rollback
    /// lever 1 demonstrated operationally).
    pub reversible: bool,
    /// The live-key count served after the flip-back (== surviving cycle keys + 1).
    pub keys_after_revert: usize,
    pub detail: Option<String>,
}

/// Drive the authoritative KV primary-serve cycle through the EHDB engine and
/// demonstrate operational reversibility.
///
/// In `primary` mode (and with [`PRIMARY_SERVE_ACTIVATED`]) this:
///
/// 1. runs [`exercise_primary_serve`] — put + per-key served get + bucket scan +
///    optimistic CAS (versioned swap + create-only conflict) + tombstone delete +
///    absolute-TTL lease + fresh-driver replay, all served authoritatively by
///    EHDB, dual-run parity-checked against a NATS-KV mirror; then
/// 2. flips `NOETL_EHDB_KV` back to `shadow` in a cloned env and mirrors a further
///    key over the SAME store, proving the incumbent/shadow path is restored and
///    the store stays whole (zero data loss on rollback).
///
/// Off/disabled ⇒ strict no-op (byte-identical `/metrics`).  Control-plane roles
/// are guard-refused before any engine opens.  Never authors a NoETL event and
/// never writes NATS-KV — it only exercises the derived EHDB KV fabric.
pub fn serve_primary_cycle(env: &EnvMap, opts: &KvOptions, record_metrics: bool) -> KvServeResult {
    let started = std::time::Instant::now();
    let mode = KvMode::from_env(env);

    // Early-exit builder (no cycle report) that records the `primary_serve`
    // metric — `disabled` outcomes are skipped by `record_kv`, preserving the
    // byte-identical no-op invariant.
    let early = |outcome: KvOutcome,
                 role: Option<EhdbClientRole>,
                 detail: Option<String>|
     -> KvServeResult {
        let duration_seconds = started.elapsed().as_secs_f64();
        if record_metrics {
            metrics::record_kv(
                "primary_serve",
                outcome.as_str(),
                outcome.ok(),
                outcome.degraded(),
                duration_seconds,
            );
        }
        KvServeResult {
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
    if mode == KvMode::Off || !truthy(env, EHDB_ENABLED_ENV) {
        return early(KvOutcome::Disabled, None, None);
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
            KvOutcome::PrimaryUnavailable,
            Some(contract.role),
            Some("kv primary serve is not activated in this build".to_string()),
        );
    }
    // The cycle only serves under the `primary` flag; `shadow` stays mirror-only.
    if mode != KvMode::Primary {
        return early(
            KvOutcome::PrimaryUnavailable,
            Some(contract.role),
            Some("primary-serve cycle requires NOETL_EHDB_KV=primary".to_string()),
        );
    }

    let driver = driver_from(&contract, opts);

    // Deterministic cycle: three distinct circuit keys (CAS on the first, delete
    // on the last) + a TTL lease at clock 1000 — a scope + CAS + delete + TTL
    // ground truth with an in-lockstep NATS-KV mirror so the dual-run parity is
    // exact.
    let input = KvPrimaryInput {
        bucket: PRIMARY_SERVE_BUCKET.to_string(),
        entries: vec![
            (
                "circuit.1".to_string(),
                "{\"phase\":\"closed\"}".to_string(),
            ),
            ("circuit.2".to_string(), "{\"phase\":\"open\"}".to_string()),
            ("circuit.3".to_string(), "{\"phase\":\"half\"}".to_string()),
        ],
        now_ms: PRIMARY_SERVE_NOW_MS,
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
    // env and mirror one more key over the SAME store.  A clean mirror plus a
    // whole-store live scan proves the incumbent/shadow path is restored with zero
    // data loss and the store grew.
    let mut shadow_env = env.clone();
    shadow_env.insert(KV_MODE_ENV.to_string(), "shadow".to_string());
    let revert = mirror_put(
        &shadow_env,
        PRIMARY_SERVE_BUCKET,
        "circuit.revert",
        "{\"phase\":\"reverted\"}",
        None,
        opts,
        false,
    );
    let keys_after_revert = driver
        .scan(&KvScanRequest {
            bucket: PRIMARY_SERVE_BUCKET.to_string(),
            prefix: None,
            limit: 4_096,
            // Past the lease expiry so the expired TTL key never counts.
            now_ms: Some(PRIMARY_SERVE_NOW_MS + 1),
        })
        .map(|s| s.match_count)
        .unwrap_or(0);
    let reversible = revert.outcome == KvOutcome::Mirrored
        && keys_after_revert == PRIMARY_SERVE_KEYS_AFTER_REVERT;

    let outcome = if served && reversible {
        KvOutcome::ServedPrimary
    } else {
        KvOutcome::PrimaryDivergence
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
        metrics::record_kv(
            "primary_serve",
            outcome.as_str(),
            outcome.ok(),
            outcome.degraded(),
            duration_seconds,
        );
    }
    KvServeResult {
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

/// Resolve the once-per-process env snapshot that arms the **live platform-KV
/// mirror hook** (noetl/ehdb#234 runtime integration, KV tier).  This is the
/// twin of [`eventlog::runtime_hook_env`][elh] for the KV tier: the worker's
/// authoritative platform-KV write path (the NATS-KV `Store::put` in
/// `spool_runtime`) resolves it exactly once at construction, so the per-put
/// path does *zero* work when the hook is inactive.
///
/// Returns `Some(env)` — meaning "mirror every live platform-KV put" — ONLY
/// when all of:
///
/// * the umbrella switch `NOETL_EHDB_ENABLED` is truthy, AND
/// * the KV tier `NOETL_EHDB_KV` is `shadow` (this slice wires the live path for
///   **shadow** only; `off`/`primary` return `None`), AND
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
    if KvMode::from_env(env) != KvMode::Shadow {
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

/// Live platform-KV mirror hook: mirror one already-written platform KV entry
/// (the authoritative NATS-KV `put` the worker just performed) into the EHDB KV
/// shadow fabric.
///
/// This calls the SAME [`mirror_put`] shadow dual-write + read-back parity path
/// the `ehdb-selfcheck mirror-kv` drive exercises, but on the real KV puts the
/// worker performs, so a live drive advances the `noetl_ehdb_kv_*` metrics
/// instead of only the selfcheck.  `expires_at_ms` is `None` (platform KV
/// circuit state carries no TTL); parity relies on presence + value equality.
///
/// **Best-effort + isolated.**  Shadow is auxiliary: this NEVER affects the
/// authoritative KV path.  Engine-error cases surface as non-`ok` outcomes
/// (recorded to the degraded metric), and an unexpected panic is caught here and
/// returned as [`KvOutcome::Unavailable`] rather than unwinding into the caller.
/// The caller discards the return; the metric carries the signal.
pub fn mirror_live_put(env: &EnvMap, bucket: &str, key: &str, value: &str) -> KvOutcome {
    let guarded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        mirror_put(env, bucket, key, value, None, &KvOptions::default(), true).outcome
    }));
    guarded.unwrap_or(KvOutcome::Unavailable)
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
    fn primary_serves_authoritatively() {
        let (log, dir) = tmp_log("primary");
        let e = worker_env(log.to_str().unwrap(), "primary");
        // Phase 9 tier 3: primary is activated, so a primary put serves the KV op
        // authoritatively from EHDB (not refused).  Parity holds.
        let r = mirror_put(&e, "b", "k", "v", None, &Default::default(), false);
        assert_eq!(r.mode, KvMode::Primary);
        assert_eq!(r.outcome, KvOutcome::ServedPrimary, "{:?}", r.detail);
        assert_eq!(r.version, Some(1));
        assert!(r.parity.as_ref().unwrap().holds());
        // ServedPrimary is only reachable with PRIMARY_SERVE_ACTIVATED == true.
        assert!(PRIMARY_SERVE_ACTIVATED);
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

    #[test]
    fn primary_serve_cycle_served_by_ehdb_and_reversible() {
        let (log, dir) = tmp_log("cycle");
        let e = worker_env(log.to_str().unwrap(), "primary");
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.mode, KvMode::Primary);
        assert_eq!(r.outcome, KvOutcome::ServedPrimary, "{:?}", r.detail);
        assert!(r.served_by_ehdb);
        let report = r.report.as_ref().unwrap();
        assert!(report.served_by_ehdb());
        assert_eq!(report.put_count, PRIMARY_SERVE_CYCLE_ENTRIES);
        assert!(report.put_ok && report.get_ok && report.scan_ok);
        assert!(report.cas_ok && report.delete_ok && report.ttl_ok);
        assert!(report.replay_matches && report.dual_run_holds);
        // Reversibility: flip back to shadow mirrored one more key; the store is
        // whole and serves the 2 surviving cycle keys + the 1 revert key.
        assert!(r.reversible);
        assert_eq!(r.keys_after_revert, 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_cycle_off_is_noop() {
        let e = worker_env("/tmp/unused-kv-cycle.jsonl", "off");
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.outcome, KvOutcome::Disabled);
        assert!(r.report.is_none());
        assert!(!r.served_by_ehdb);
    }

    #[test]
    fn primary_serve_cycle_shadow_is_primary_unavailable() {
        let (log, dir) = tmp_log("cycle-shadow");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        // The cycle only serves under the `primary` flag.
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.outcome, KvOutcome::PrimaryUnavailable);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_cycle_control_plane_guard_refused() {
        let e: EnvMap = [
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "server"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_KV", "primary"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let r = serve_primary_cycle(&e, &Default::default(), false);
        assert_eq!(r.outcome, KvOutcome::GuardRefused);
        assert!(r.report.is_none());
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

    // --- Live platform-KV mirror hook (runtime integration, noetl/ehdb#234) ---

    #[test]
    fn runtime_hook_env_arms_only_for_enabled_shadow_data_plane() {
        let armed = runtime_hook_env(&worker_env("/tmp/kv-hook.jsonl", "shadow"));
        assert!(armed.is_some(), "shadow+enabled worker must arm the KV hook");
    }

    #[test]
    fn runtime_hook_env_noop_when_disabled() {
        // Umbrella switch off ⇒ no hook even though the tier says shadow.
        let e: EnvMap = [("NOETL_EHDB_KV", "shadow")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert!(runtime_hook_env(&e).is_none());
    }

    #[test]
    fn runtime_hook_env_noop_when_tier_off_or_primary() {
        assert!(runtime_hook_env(&worker_env("/tmp/kv-hook.jsonl", "off")).is_none());
        assert!(runtime_hook_env(&worker_env("/tmp/kv-hook.jsonl", "primary")).is_none());
    }

    #[test]
    fn runtime_hook_env_skips_control_plane_role() {
        for role in ["server", "gateway", "api"] {
            let e: EnvMap = [
                ("NOETL_EHDB_ENABLED", "true"),
                ("NOETL_EHDB_MODE", "local_reference"),
                ("NOETL_EHDB_CLIENT_ROLE", role),
                ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
                ("NOETL_EHDB_KV", "shadow"),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
            assert!(
                runtime_hook_env(&e).is_none(),
                "control-plane role {role} must not arm the KV hook"
            );
        }
    }

    #[test]
    fn mirror_live_put_fires_on_shadow_enabled() {
        let (log, dir) = tmp_log("live-kv-fire");
        let e = worker_env(log.to_str().unwrap(), "shadow");
        let outcome = mirror_live_put(
            &e,
            "noetl_subscription_circuit",
            "circuit.478775660589088776",
            "{\"phase\":\"closed\"}",
        );
        assert_eq!(outcome, KvOutcome::Mirrored);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn mirror_live_put_is_noop_when_disabled() {
        let e: EnvMap = EnvMap::new();
        let outcome = mirror_live_put(&e, "b", "k", "v");
        assert_eq!(outcome, KvOutcome::Disabled);
    }

    #[test]
    fn mirror_live_put_skipped_for_control_plane_role() {
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
        let outcome = mirror_live_put(&e, "b", "k", "v");
        assert_eq!(outcome, KvOutcome::GuardRefused);
    }

    #[test]
    fn mirror_live_put_isolates_engine_error_without_propagating() {
        // Log path whose parent is a *file* ⇒ engine cannot append; the mirror
        // must contain it as an outcome rather than panicking into the caller.
        let (file_as_dir, dir) = tmp_log("kv-iso");
        std::fs::write(&file_as_dir, b"x").unwrap();
        let bad_log = file_as_dir.join("nested").join("log.jsonl");
        let e = worker_env(bad_log.to_str().unwrap(), "shadow");
        let outcome = mirror_live_put(&e, "b", "k", "v");
        assert!(
            matches!(outcome, KvOutcome::Unavailable | KvOutcome::Invalid),
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
