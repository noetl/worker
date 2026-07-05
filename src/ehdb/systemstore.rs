//! Bounded, stateless worker/playbook/system data-plane ops for the EHDB
//! **system WASM library store** (EHDB Phase E, noetl/ehdb#239).
//!
//! EHDB owns the durable catalog side of NoETL's system WASM library model:
//! immutable module manifests (`path`/`revision`/`digest`/`entry`/`target`/
//! object ref / byte-len / host capabilities) plus mutable environment/channel
//! bindings that resolve a logical library to a concrete module for a
//! tenant/namespace/environment/channel.  This module is the worker-side bridge
//! to the [`ehdb_reference`] `*_system_*` helpers:
//!
//! * `publish` — record one immutable module manifest (one atomic commit).
//! * `bind`    — (re)bind a release channel to a published revision (one commit).
//! * `resolve` — read-only replay returning the active module ref for a channel.
//!
//! WASM *execution* stays in the worker/system-pool host: this slice only
//! publishes a manifest, (re)binds a channel, and resolves the module ref the
//! host then loads into its own sandboxed, stateless WASM runtime.  Every op
//! here honours the same boundaries as [`super::dataplane`] /
//! [`super::eventstream`]:
//!
//! * **Disabled by default** — `Disabled` no-op that records no metric, so a
//!   disabled build renders byte-identical `/metrics`.
//! * **Control-plane guarded** — gateway/api/server are refused before any
//!   runtime is opened, so no manifest/binding can be written by a gatekeeper.
//! * **Bounded** — a published manifest's declared object size is capped
//!   (`NOETL_EHDB_SYSTEM_MAX_MODULE_BYTES`, default 16 MiB, ceiling 256 MiB) and
//!   its host-capability count is capped
//!   (`NOETL_EHDB_SYSTEM_MAX_CAPABILITIES`, default 16, ceiling 64).  Over-bound
//!   publishes are `Rejected` before the helper runs — the bound on what the
//!   host will later load into WASM is enforced here at catalog time.
//! * **Stateless** — the local-reference runtime is opened + dropped per call;
//!   no long-lived handle, no per-tenant residency.
//! * **Event-log-authoritative** — this catalog is a *separate* on-disk JSONL
//!   fabric; it NEVER writes back to `noetl.event` (structurally: no NoETL
//!   event-emitter import reaches this module).
//!
//! Bounded RAG retrieval — the other half of the Phase E direction
//! (system-WASM store → RAG) — is deferred: the merged `ehdb-reference` slice
//! (noetl/ehdb#239) exposes no bounded retrieval helper, only the system-store
//! `publish`/`bind`/`resolve`.  Wiring RAG needs an `ehdb-reference` retrieval
//! helper first (a follow-up ehdb slice); see noetl/ehdb#234.

use ehdb_reference::{
    bind_local_reference_system_channel, publish_local_reference_system_module,
    resolve_local_reference_system_module, BindSystemChannelOutcome, BindSystemChannelRequest,
    PublishSystemModuleOutcome, PublishSystemModuleRequest, ResolveSystemModuleOutcome,
    ResolveSystemModuleRequest, DEFAULT_LOCAL_REFERENCE_NAMESPACE, DEFAULT_LOCAL_REFERENCE_TENANT,
};

use std::sync::OnceLock;

use super::contract::{contract_from_env, EhdbClientRole, EhdbContract, EHDB_ENABLED_ENV};
use super::guard::assert_data_plane_access_allowed;
use super::{metrics, EnvMap};
use crate::snowflake::SnowflakeGen;

pub const MAX_MODULE_BYTES_ENV: &str = "NOETL_EHDB_SYSTEM_MAX_MODULE_BYTES";
pub const MAX_CAPABILITIES_ENV: &str = "NOETL_EHDB_SYSTEM_MAX_CAPABILITIES";
const DEFAULT_MAX_MODULE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_MODULE_BYTES_CEILING: u64 = 256 * 1024 * 1024;
const DEFAULT_MAX_CAPABILITIES: usize = 16;
const MAX_CAPABILITIES_CEILING: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemStoreOperation {
    Publish,
    Bind,
    Resolve,
}

impl SystemStoreOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            SystemStoreOperation::Publish => "publish",
            SystemStoreOperation::Bind => "bind",
            SystemStoreOperation::Resolve => "resolve",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemStoreOutcome {
    Disabled,
    Published,
    Bound,
    Resolved,
    /// A resolve on a never-bound / unpublished channel (the absent probe).
    Absent,
    /// A bound violation (empty/over-cap module bytes or capability count, or
    /// an immutable-manifest / missing-manifest conflict from the engine).
    Rejected,
    Unavailable,
    GuardRefused,
    Invalid,
}

impl SystemStoreOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            SystemStoreOutcome::Disabled => "disabled",
            SystemStoreOutcome::Published => "published",
            SystemStoreOutcome::Bound => "bound",
            SystemStoreOutcome::Resolved => "resolved",
            SystemStoreOutcome::Absent => "absent",
            SystemStoreOutcome::Rejected => "rejected",
            SystemStoreOutcome::Unavailable => "unavailable",
            SystemStoreOutcome::GuardRefused => "guard_refused",
            SystemStoreOutcome::Invalid => "invalid",
        }
    }

    pub fn ok(&self) -> bool {
        matches!(
            self,
            SystemStoreOutcome::Disabled
                | SystemStoreOutcome::Published
                | SystemStoreOutcome::Bound
                | SystemStoreOutcome::Resolved
                | SystemStoreOutcome::Absent
        )
    }

    fn degraded(&self) -> bool {
        matches!(self, SystemStoreOutcome::Unavailable)
    }
}

/// Structured, secret-free result of a bounded system-store op.
#[derive(Debug, Clone)]
pub struct SystemStoreResult {
    pub operation: SystemStoreOperation,
    pub outcome: SystemStoreOutcome,
    pub role: Option<EhdbClientRole>,
    pub duration_seconds: f64,
    pub detail: Option<String>,
    pub publish: Option<PublishSystemModuleOutcome>,
    pub bind: Option<BindSystemChannelOutcome>,
    pub resolve: Option<ResolveSystemModuleOutcome>,
}

/// Optional tenant/namespace/transaction overrides for a system-store op.
#[derive(Debug, Clone, Default)]
pub struct SystemStoreOptions {
    pub tenant: Option<String>,
    pub namespace: Option<String>,
    pub transaction_id: Option<String>,
}

/// A published module manifest (the bounded, validated-at-caller shape).
#[derive(Debug, Clone)]
pub struct ModuleManifest {
    pub path: String,
    pub revision: u32,
    pub digest: String,
    pub entry: String,
    pub target: String,
    pub object_path: String,
    pub byte_len: u64,
    pub capabilities: Vec<String>,
}

/// A channel binding (path + release channel + the revision/digest it points at).
#[derive(Debug, Clone)]
pub struct ChannelBinding {
    pub environment: String,
    pub channel: String,
    pub path: String,
    pub revision: u32,
    pub digest: String,
}

fn txn_gen() -> &'static SnowflakeGen {
    static GEN: OnceLock<SnowflakeGen> = OnceLock::new();
    GEN.get_or_init(|| SnowflakeGen::from_env_or_hint("ehdb"))
}

fn new_transaction_id() -> String {
    format!("ehdbtxn-{}", txn_gen().next_id())
}

fn truthy(env: &EnvMap, key: &str) -> bool {
    matches!(
        env.get(key)
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref(),
        Some("1" | "true" | "yes" | "y" | "on")
    )
}

fn bounded_max_module_bytes(env: &EnvMap) -> u64 {
    let value = env
        .get(MAX_MODULE_BYTES_ENV)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_MAX_MODULE_BYTES);
    value.clamp(1, MAX_MODULE_BYTES_CEILING)
}

fn bounded_max_capabilities(env: &EnvMap) -> usize {
    let value = env
        .get(MAX_CAPABILITIES_ENV)
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_CAPABILITIES);
    value.clamp(1, MAX_CAPABILITIES_CEILING)
}

/// Resolve the contract for a system-store op.  Returns `Ok(contract)` for a
/// data-plane role, or `Err(result)` carrying the early outcome
/// (disabled/guard_refused/invalid) already classified + metered.
fn resolve_contract(
    env: &EnvMap,
    operation: SystemStoreOperation,
    started: std::time::Instant,
    record_metrics: bool,
) -> Result<EhdbContract, Box<SystemStoreResult>> {
    // Boxed cold error path — the result carries large crate outcome structs
    // (clippy::result_large_err).
    let finish =
        |outcome: SystemStoreOutcome, role: Option<EhdbClientRole>, detail: Option<String>| {
            Box::new(make_result(
                operation,
                outcome,
                role,
                started,
                detail,
                None,
                None,
                None,
                record_metrics,
            ))
        };

    let contract = match contract_from_env(env) {
        Ok(c) => c,
        Err(err) => {
            let role = super::contract::safe_client_role(env);
            let outcome = if role.map(|r| r.is_control_plane()).unwrap_or(false) {
                SystemStoreOutcome::GuardRefused
            } else {
                SystemStoreOutcome::Invalid
            };
            return Err(finish(outcome, role, Some(err.0)));
        }
    };

    if let Err(err) = assert_data_plane_access_allowed(contract.role, operation.as_str()) {
        return Err(finish(
            SystemStoreOutcome::GuardRefused,
            Some(contract.role),
            Some(err.to_string()),
        ));
    }
    if !contract.uses_local_reference_runtime() || contract.local_reference_log.is_none() {
        return Err(finish(
            SystemStoreOutcome::Disabled,
            Some(contract.role),
            None,
        ));
    }
    Ok(contract)
}

#[allow(clippy::too_many_arguments)]
fn make_result(
    operation: SystemStoreOperation,
    outcome: SystemStoreOutcome,
    role: Option<EhdbClientRole>,
    started: std::time::Instant,
    detail: Option<String>,
    publish: Option<PublishSystemModuleOutcome>,
    bind: Option<BindSystemChannelOutcome>,
    resolve: Option<ResolveSystemModuleOutcome>,
    record_metrics: bool,
) -> SystemStoreResult {
    let duration_seconds = started.elapsed().as_secs_f64();
    if record_metrics {
        metrics::record_systemstore(
            operation.as_str(),
            outcome.as_str(),
            outcome.ok(),
            outcome.degraded(),
            duration_seconds,
        );
    }
    SystemStoreResult {
        operation,
        outcome,
        role,
        duration_seconds,
        detail,
        publish,
        bind,
        resolve,
    }
}

fn tenant_of(opts: &SystemStoreOptions) -> String {
    opts.tenant
        .clone()
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string())
}

fn namespace_of(opts: &SystemStoreOptions) -> String {
    opts.namespace
        .clone()
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string())
}

/// Publish one immutable system WASM library manifest.  Disabled ⇒ `Disabled`
/// no-op.  Over-bound module size / capability count ⇒ `Rejected` before the
/// helper runs.
pub fn publish_module(
    env: &EnvMap,
    manifest: &ModuleManifest,
    opts: &SystemStoreOptions,
    record_metrics: bool,
) -> SystemStoreResult {
    let op = SystemStoreOperation::Publish;
    let started = std::time::Instant::now();

    if !truthy(env, EHDB_ENABLED_ENV) {
        return make_result(
            op,
            SystemStoreOutcome::Disabled,
            None,
            started,
            None,
            None,
            None,
            None,
            record_metrics,
        );
    }
    let contract = match resolve_contract(env, op, started, record_metrics) {
        Ok(c) => c,
        Err(result) => return *result,
    };

    let reject = |detail: String| {
        make_result(
            op,
            SystemStoreOutcome::Rejected,
            Some(contract.role),
            started,
            Some(detail),
            None,
            None,
            None,
            record_metrics,
        )
    };

    if manifest.byte_len == 0 {
        return reject("system module byte_len is zero".to_string());
    }
    let max_bytes = bounded_max_module_bytes(env);
    if manifest.byte_len > max_bytes {
        return reject(format!(
            "system module {} bytes exceeds bound {max_bytes}",
            manifest.byte_len
        ));
    }
    if manifest.capabilities.is_empty() {
        return reject("system module declares no host capabilities".to_string());
    }
    let max_caps = bounded_max_capabilities(env);
    if manifest.capabilities.len() > max_caps {
        return reject(format!(
            "system module declares {} capabilities, exceeds bound {max_caps}",
            manifest.capabilities.len()
        ));
    }

    let request = PublishSystemModuleRequest {
        log_path: contract.local_reference_log.clone().expect("log present"),
        tenant: tenant_of(opts),
        namespace: namespace_of(opts),
        path: manifest.path.clone(),
        revision: manifest.revision,
        digest: manifest.digest.clone(),
        entry: manifest.entry.clone(),
        target: manifest.target.clone(),
        object_path: manifest.object_path.clone(),
        byte_len: manifest.byte_len,
        capabilities: manifest.capabilities.clone(),
        transaction_id: opts
            .transaction_id
            .clone()
            .unwrap_or_else(new_transaction_id),
    };

    match publish_local_reference_system_module(request) {
        Ok(outcome) => make_result(
            op,
            SystemStoreOutcome::Published,
            Some(contract.role),
            started,
            None,
            Some(outcome),
            None,
            None,
            record_metrics,
        ),
        Err(err) => make_result(
            op,
            classify_helper_error(&err),
            Some(contract.role),
            started,
            Some(err.to_string()),
            None,
            None,
            None,
            record_metrics,
        ),
    }
}

/// Bind a release channel to a published revision (hot-replaces the active
/// module; prior manifests stay addressable).  Disabled ⇒ `Disabled` no-op.
pub fn bind_channel(
    env: &EnvMap,
    binding: &ChannelBinding,
    opts: &SystemStoreOptions,
    record_metrics: bool,
) -> SystemStoreResult {
    let op = SystemStoreOperation::Bind;
    let started = std::time::Instant::now();

    if !truthy(env, EHDB_ENABLED_ENV) {
        return make_result(
            op,
            SystemStoreOutcome::Disabled,
            None,
            started,
            None,
            None,
            None,
            None,
            record_metrics,
        );
    }
    let contract = match resolve_contract(env, op, started, record_metrics) {
        Ok(c) => c,
        Err(result) => return *result,
    };

    let request = BindSystemChannelRequest {
        log_path: contract.local_reference_log.clone().expect("log present"),
        tenant: tenant_of(opts),
        namespace: namespace_of(opts),
        environment: binding.environment.clone(),
        channel: binding.channel.clone(),
        path: binding.path.clone(),
        revision: binding.revision,
        digest: binding.digest.clone(),
        transaction_id: opts
            .transaction_id
            .clone()
            .unwrap_or_else(new_transaction_id),
    };

    match bind_local_reference_system_channel(request) {
        Ok(outcome) => make_result(
            op,
            SystemStoreOutcome::Bound,
            Some(contract.role),
            started,
            None,
            None,
            Some(outcome),
            None,
            record_metrics,
        ),
        Err(err) => make_result(
            op,
            classify_helper_error(&err),
            Some(contract.role),
            started,
            Some(err.to_string()),
            None,
            None,
            None,
            record_metrics,
        ),
    }
}

/// Resolve the active module a channel binding points at (read-only).  A
/// never-bound channel resolves to `Absent`.  Disabled ⇒ `Disabled` no-op.
pub fn resolve_module(
    env: &EnvMap,
    environment: &str,
    channel: &str,
    path: &str,
    opts: &SystemStoreOptions,
    record_metrics: bool,
) -> SystemStoreResult {
    let op = SystemStoreOperation::Resolve;
    let started = std::time::Instant::now();

    if !truthy(env, EHDB_ENABLED_ENV) {
        return make_result(
            op,
            SystemStoreOutcome::Disabled,
            None,
            started,
            None,
            None,
            None,
            None,
            record_metrics,
        );
    }
    let contract = match resolve_contract(env, op, started, record_metrics) {
        Ok(c) => c,
        Err(result) => return *result,
    };

    let request = ResolveSystemModuleRequest {
        log_path: contract.local_reference_log.clone().expect("log present"),
        tenant: tenant_of(opts),
        namespace: namespace_of(opts),
        environment: environment.to_string(),
        channel: channel.to_string(),
        path: path.to_string(),
    };

    match resolve_local_reference_system_module(request) {
        Ok(outcome) => {
            let ss_outcome = if outcome.exists {
                SystemStoreOutcome::Resolved
            } else {
                SystemStoreOutcome::Absent
            };
            make_result(
                op,
                ss_outcome,
                Some(contract.role),
                started,
                None,
                None,
                None,
                Some(outcome),
                record_metrics,
            )
        }
        Err(err) => make_result(
            op,
            classify_helper_error(&err),
            Some(contract.role),
            started,
            Some(err.to_string()),
            None,
            None,
            None,
            record_metrics,
        ),
    }
}

/// Classify an `ehdb_reference` error by its Display prefix (the crate does not
/// re-export its error enum):
///
/// * `invalid identifier: …` / `invalid state: …` — a caller mistake
///   (bad id, unsupported wasm target / host capability) ⇒ `Invalid`.
/// * `already exists: …` (re-publishing an immutable manifest) /
///   `not found: …` (binding a channel to an unpublished path) ⇒ `Rejected`.
/// * anything else (storage / IO) ⇒ `Unavailable` (degraded).
fn classify_helper_error<E: std::fmt::Display>(err: &E) -> SystemStoreOutcome {
    let text = err.to_string();
    if text.starts_with("invalid identifier") || text.starts_with("invalid state") {
        SystemStoreOutcome::Invalid
    } else if text.starts_with("already exists") || text.starts_with("not found") {
        SystemStoreOutcome::Rejected
    } else {
        SystemStoreOutcome::Unavailable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> EnvMap {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn worker_env(log: &str) -> EnvMap {
        env(&[
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "worker"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", log),
        ])
    }

    fn tmp_log(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-sys-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        (dir.join("log.jsonl"), dir)
    }

    fn manifest(revision: u32, byte_len: u64) -> ModuleManifest {
        ModuleManifest {
            path: "system/render".to_string(),
            revision,
            digest: format!("sha256:{:064x}", revision),
            entry: "render".to_string(),
            target: "wasm32-wasi-preview1".to_string(),
            object_path: format!("system/render/{revision}.wasm"),
            byte_len,
            capabilities: vec!["event_publish".to_string()],
        }
    }

    #[test]
    fn disabled_is_noop() {
        let r = publish_module(&env(&[]), &manifest(1, 128), &Default::default(), false);
        assert_eq!(r.outcome, SystemStoreOutcome::Disabled);
        assert!(r.publish.is_none());
        let r = resolve_module(
            &env(&[]),
            "prod",
            "stable",
            "system/render",
            &Default::default(),
            false,
        );
        assert_eq!(r.outcome, SystemStoreOutcome::Disabled);
    }

    #[test]
    fn publish_bind_resolve_roundtrip() {
        let (log, dir) = tmp_log("rt");
        let e = worker_env(log.to_str().unwrap());

        // Resolve before any bind is the absent probe, not an error.
        let pre = resolve_module(
            &e,
            "prod",
            "stable",
            "system/render",
            &Default::default(),
            false,
        );
        assert_eq!(pre.outcome, SystemStoreOutcome::Absent);
        assert!(!pre.resolve.unwrap().exists);

        let p = publish_module(&e, &manifest(1, 512), &Default::default(), false);
        assert_eq!(p.outcome, SystemStoreOutcome::Published);

        let b = bind_channel(
            &e,
            &ChannelBinding {
                environment: "prod".to_string(),
                channel: "stable".to_string(),
                path: "system/render".to_string(),
                revision: 1,
                digest: manifest(1, 512).digest,
            },
            &Default::default(),
            false,
        );
        assert_eq!(b.outcome, SystemStoreOutcome::Bound);

        let r = resolve_module(
            &e,
            "prod",
            "stable",
            "system/render",
            &Default::default(),
            false,
        );
        assert_eq!(r.outcome, SystemStoreOutcome::Resolved);
        let resolved = r.resolve.unwrap();
        assert!(resolved.exists);
        assert_eq!(resolved.revision, Some(1));
        assert_eq!(resolved.target.as_deref(), Some("wasm32-wasi-preview1"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rebind_hot_replaces_active_module() {
        let (log, dir) = tmp_log("rebind");
        let e = worker_env(log.to_str().unwrap());

        publish_module(&e, &manifest(1, 256), &Default::default(), false);
        publish_module(&e, &manifest(2, 256), &Default::default(), false);
        bind_channel(
            &e,
            &ChannelBinding {
                environment: "prod".to_string(),
                channel: "stable".to_string(),
                path: "system/render".to_string(),
                revision: 1,
                digest: manifest(1, 256).digest,
            },
            &Default::default(),
            false,
        );
        bind_channel(
            &e,
            &ChannelBinding {
                environment: "prod".to_string(),
                channel: "stable".to_string(),
                path: "system/render".to_string(),
                revision: 2,
                digest: manifest(2, 256).digest,
            },
            &Default::default(),
            false,
        );
        let r = resolve_module(
            &e,
            "prod",
            "stable",
            "system/render",
            &Default::default(),
            false,
        );
        assert_eq!(r.outcome, SystemStoreOutcome::Resolved);
        assert_eq!(r.resolve.unwrap().revision, Some(2));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn oversized_module_rejected() {
        let (log, dir) = tmp_log("big");
        let mut e = worker_env(log.to_str().unwrap());
        e.insert(MAX_MODULE_BYTES_ENV.to_string(), "64".to_string());
        let r = publish_module(&e, &manifest(1, 128), &Default::default(), false);
        assert_eq!(r.outcome, SystemStoreOutcome::Rejected);
        assert!(r.publish.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn too_many_capabilities_rejected() {
        let (log, dir) = tmp_log("caps");
        let mut e = worker_env(log.to_str().unwrap());
        e.insert(MAX_CAPABILITIES_ENV.to_string(), "1".to_string());
        let mut m = manifest(1, 128);
        m.capabilities = vec!["event_publish".to_string(), "object_put".to_string()];
        let r = publish_module(&e, &m, &Default::default(), false);
        assert_eq!(r.outcome, SystemStoreOutcome::Rejected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn zero_byte_module_rejected() {
        let (log, dir) = tmp_log("zero");
        let e = worker_env(log.to_str().unwrap());
        let r = publish_module(&e, &manifest(1, 0), &Default::default(), false);
        assert_eq!(r.outcome, SystemStoreOutcome::Rejected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn republish_same_revision_rejected() {
        let (log, dir) = tmp_log("dup");
        let e = worker_env(log.to_str().unwrap());
        let p1 = publish_module(&e, &manifest(1, 128), &Default::default(), false);
        assert_eq!(p1.outcome, SystemStoreOutcome::Published);
        // Immutable manifest: re-publishing (path, revision) is an engine
        // `already exists` ⇒ Rejected.
        let p2 = publish_module(&e, &manifest(1, 128), &Default::default(), false);
        assert_eq!(p2.outcome, SystemStoreOutcome::Rejected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn unsupported_target_invalid() {
        let (log, dir) = tmp_log("target");
        let e = worker_env(log.to_str().unwrap());
        let mut m = manifest(1, 128);
        m.target = "x86_64-unknown-linux".to_string();
        let r = publish_module(&e, &m, &Default::default(), false);
        assert_eq!(r.outcome, SystemStoreOutcome::Invalid);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn control_plane_role_guard_refused() {
        for role in ["gateway", "api", "server"] {
            let e = env(&[
                ("NOETL_EHDB_ENABLED", "true"),
                ("NOETL_EHDB_MODE", "local_reference"),
                ("NOETL_EHDB_CLIENT_ROLE", role),
                ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ]);
            let p = publish_module(&e, &manifest(1, 128), &Default::default(), false);
            assert_eq!(p.outcome, SystemStoreOutcome::GuardRefused);
            assert!(p.publish.is_none());
            let r = resolve_module(
                &e,
                "prod",
                "stable",
                "system/render",
                &Default::default(),
                false,
            );
            assert_eq!(r.outcome, SystemStoreOutcome::GuardRefused);
            assert!(r.resolve.is_none());
        }
    }
}
