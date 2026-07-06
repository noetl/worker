//! EHDB integration contract — the disabled-by-default env boundary.
//!
//! This is the Rust twin of the (now retired) Python `noetl.core.ehdb_contract`
//! module.  It parses and validates the `NOETL_EHDB_*` environment the ops Helm
//! chart renders (see `automation/helm/*/templates/_ehdb.tpl`) into a typed
//! contract, and hard-codes the control-plane vs data-plane boundary:
//!
//! * **Control-plane roles** (`gateway` / `api` / `server`) may only run in
//!   `control_plane` mode with the single `control_plane` capability and no
//!   local-reference log.  They NEVER touch EHDB data.
//! * **Data-plane roles** (`worker` / `playbook` / `system`) run in
//!   `local_reference` mode with a bounded on-disk JSONL log.  These are the
//!   only roles the worker's in-process EHDB integration serves.
//!
//! Everything is disabled by default: when `NOETL_EHDB_ENABLED` is not truthy
//! the contract is `Disabled` and no EHDB code path runs, so the worker behaves
//! byte-identically to a build without EHDB.

use std::collections::BTreeSet;
use std::path::PathBuf;

use super::EnvMap;

pub const EHDB_ENABLED_ENV: &str = "NOETL_EHDB_ENABLED";
pub const EHDB_MODE_ENV: &str = "NOETL_EHDB_MODE";
pub const EHDB_CLIENT_ROLE_ENV: &str = "NOETL_EHDB_CLIENT_ROLE";
pub const EHDB_CAPABILITIES_ENV: &str = "NOETL_EHDB_CAPABILITIES";
pub const EHDB_LOCAL_REFERENCE_LOG_ENV: &str = "NOETL_EHDB_LOCAL_REFERENCE_LOG";
pub const NOETL_RUN_MODE_ENV: &str = "NOETL_RUN_MODE";

/// The NoETL client role an EHDB-enabled process runs as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EhdbClientRole {
    Gateway,
    Api,
    Server,
    Worker,
    Playbook,
    System,
}

impl EhdbClientRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            EhdbClientRole::Gateway => "gateway",
            EhdbClientRole::Api => "api",
            EhdbClientRole::Server => "server",
            EhdbClientRole::Worker => "worker",
            EhdbClientRole::Playbook => "playbook",
            EhdbClientRole::System => "system",
        }
    }

    /// Control-plane roles are gatekeepers — they never touch EHDB data.
    pub fn is_control_plane(&self) -> bool {
        matches!(
            self,
            EhdbClientRole::Gateway | EhdbClientRole::Api | EhdbClientRole::Server
        )
    }

    /// Data-plane roles are the only roles the in-process integration serves.
    pub fn is_data_plane(&self) -> bool {
        matches!(
            self,
            EhdbClientRole::Worker | EhdbClientRole::Playbook | EhdbClientRole::System
        )
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "gateway" => Some(EhdbClientRole::Gateway),
            "api" => Some(EhdbClientRole::Api),
            "server" => Some(EhdbClientRole::Server),
            "worker" => Some(EhdbClientRole::Worker),
            "playbook" => Some(EhdbClientRole::Playbook),
            "system" => Some(EhdbClientRole::System),
            _ => None,
        }
    }
}

/// EHDB integration mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EhdbIntegrationMode {
    Disabled,
    ControlPlane,
    LocalReference,
}

impl EhdbIntegrationMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            EhdbIntegrationMode::Disabled => "disabled",
            EhdbIntegrationMode::ControlPlane => "control_plane",
            EhdbIntegrationMode::LocalReference => "local_reference",
        }
    }
}

/// The 12 capability tokens the NoETL contract recognises.  Modelled only so an
/// unsupported token is rejected as a misconfiguration (`InvalidConfig`) rather
/// than silently accepted; the worker's behaviour is role-driven, not
/// capability-driven.
const KNOWN_CAPABILITIES: &[&str] = &[
    "control_plane",
    "catalog_read",
    "catalog_write",
    "transaction_append",
    "stream_append",
    "stream_consume",
    "object_read",
    "object_write",
    "retrieval_read",
    "retrieval_write",
    "replication_plan",
    "system_library_resolve",
];

/// A parsed, validated EHDB integration contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EhdbContract {
    pub enabled: bool,
    pub mode: EhdbIntegrationMode,
    pub role: EhdbClientRole,
    pub capabilities: BTreeSet<String>,
    pub local_reference_log: Option<PathBuf>,
}

impl EhdbContract {
    /// Whether the process should run the bounded local-reference data-plane
    /// runtime (enabled + local_reference mode).
    pub fn uses_local_reference_runtime(&self) -> bool {
        self.enabled && self.mode == EhdbIntegrationMode::LocalReference
    }
}

/// A misconfiguration of the EHDB env contract (non-guard).  Distinct from a
/// [`super::guard::EhdbGuardError`] so readiness can classify a control-plane
/// role carrying a data-plane env as a guard refusal rather than an invalid
/// config.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid EHDB configuration: {0}")]
pub struct EhdbConfigError(pub String);

fn truthy(value: Option<&String>) -> bool {
    matches!(
        value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "y" | "on")
    )
}

fn non_empty<'a>(env: &'a EnvMap, key: &str) -> Option<&'a String> {
    env.get(key).filter(|v| !v.trim().is_empty())
}

/// Whether the umbrella EHDB integration is enabled (`NOETL_EHDB_ENABLED`
/// truthy).  The single umbrella gate every tier sits under; the Phase-10
/// backend-config resolver reads it to reject `shadow`/`primary` tiers while the
/// integration is disabled.
pub fn enabled_from_env(env: &EnvMap) -> bool {
    truthy(env.get(EHDB_ENABLED_ENV))
}

/// Resolve the client role from the env, tolerating an unknown value (used by
/// readiness to classify).  `None` = unset/unknown.
pub fn safe_client_role(env: &EnvMap) -> Option<EhdbClientRole> {
    let raw = non_empty(env, EHDB_CLIENT_ROLE_ENV)
        .or_else(|| non_empty(env, NOETL_RUN_MODE_ENV))
        .map(|s| s.as_str())
        .unwrap_or("worker");
    EhdbClientRole::parse(raw)
}

fn client_role(env: &EnvMap) -> Result<EhdbClientRole, EhdbConfigError> {
    let raw = non_empty(env, EHDB_CLIENT_ROLE_ENV)
        .or_else(|| non_empty(env, NOETL_RUN_MODE_ENV))
        .map(|s| s.as_str())
        .unwrap_or("worker");
    EhdbClientRole::parse(raw)
        .ok_or_else(|| EhdbConfigError(format!("unsupported EHDB client role: {raw}")))
}

fn integration_mode(env: &EnvMap, enabled: bool) -> Result<EhdbIntegrationMode, EhdbConfigError> {
    match non_empty(env, EHDB_MODE_ENV).map(|s| s.trim().to_ascii_lowercase()) {
        None => Ok(if enabled {
            EhdbIntegrationMode::LocalReference
        } else {
            EhdbIntegrationMode::Disabled
        }),
        Some(raw) => match raw.as_str() {
            "disabled" => Ok(EhdbIntegrationMode::Disabled),
            "control_plane" => Ok(EhdbIntegrationMode::ControlPlane),
            "local_reference" => Ok(EhdbIntegrationMode::LocalReference),
            other => Err(EhdbConfigError(format!(
                "unsupported EHDB integration mode: {other}"
            ))),
        },
    }
}

fn capabilities(
    env: &EnvMap,
    enabled: bool,
    mode: EhdbIntegrationMode,
) -> Result<BTreeSet<String>, EhdbConfigError> {
    let raw = non_empty(env, EHDB_CAPABILITIES_ENV);
    if !enabled {
        if raw.is_some() {
            return Err(EhdbConfigError(
                "disabled EHDB integration must not declare capabilities".to_string(),
            ));
        }
        return Ok(BTreeSet::new());
    }
    let Some(raw) = raw else {
        return Ok(match mode {
            EhdbIntegrationMode::ControlPlane => BTreeSet::from(["control_plane".to_string()]),
            EhdbIntegrationMode::LocalReference => KNOWN_CAPABILITIES
                .iter()
                .filter(|c| **c != "control_plane")
                .map(|c| c.to_string())
                .collect(),
            EhdbIntegrationMode::Disabled => BTreeSet::new(),
        });
    };
    let mut set = BTreeSet::new();
    for token in raw.split(',') {
        let normalized = token.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Err(EhdbConfigError("empty EHDB capability token".to_string()));
        }
        if !KNOWN_CAPABILITIES.contains(&normalized.as_str()) {
            return Err(EhdbConfigError(format!(
                "unsupported EHDB capability: {token}"
            )));
        }
        set.insert(normalized);
    }
    Ok(set)
}

/// Build and validate the disabled-by-default EHDB contract from the env.
pub fn contract_from_env(env: &EnvMap) -> Result<EhdbContract, EhdbConfigError> {
    let enabled = truthy(env.get(EHDB_ENABLED_ENV));
    let role = client_role(env)?;
    let mode = integration_mode(env, enabled)?;
    let caps = capabilities(env, enabled, mode)?;
    let local_reference_log =
        non_empty(env, EHDB_LOCAL_REFERENCE_LOG_ENV).map(|s| PathBuf::from(s.trim()));

    let contract = EhdbContract {
        enabled,
        mode,
        role,
        capabilities: caps,
        local_reference_log,
    };
    validate(&contract)?;
    Ok(contract)
}

fn validate(contract: &EhdbContract) -> Result<(), EhdbConfigError> {
    if !contract.enabled {
        if contract.mode != EhdbIntegrationMode::Disabled {
            return Err(EhdbConfigError(
                "disabled EHDB integration must use disabled mode".to_string(),
            ));
        }
        if !contract.capabilities.is_empty() {
            return Err(EhdbConfigError(
                "disabled EHDB integration must not declare capabilities".to_string(),
            ));
        }
        return Ok(());
    }

    if contract.mode == EhdbIntegrationMode::ControlPlane {
        if !contract.role.is_control_plane() {
            return Err(EhdbConfigError(
                "EHDB control-plane embedding is only supported for gateway/api/server roles"
                    .to_string(),
            ));
        }
        if contract.capabilities.len() != 1 || !contract.capabilities.contains("control_plane") {
            return Err(EhdbConfigError(
                "EHDB control-plane embedding only allows control_plane capability".to_string(),
            ));
        }
        if contract.local_reference_log.is_some() {
            return Err(EhdbConfigError(
                "EHDB control-plane embedding must not define NOETL_EHDB_LOCAL_REFERENCE_LOG"
                    .to_string(),
            ));
        }
        return Ok(());
    }

    if contract.role.is_control_plane() {
        return Err(EhdbConfigError(
            "EHDB data-plane integration may not run in gateway/api/server roles; gateway \
             remains a gatekeeper and must not touch data directly"
                .to_string(),
        ));
    }

    if contract.mode != EhdbIntegrationMode::LocalReference {
        return Err(EhdbConfigError(
            "enabled EHDB integration currently supports control_plane or local_reference mode only"
                .to_string(),
        ));
    }

    if !contract.role.is_data_plane() {
        return Err(EhdbConfigError(
            "EHDB local_reference mode requires worker/playbook/system role".to_string(),
        ));
    }

    if contract.capabilities.is_empty() || contract.capabilities.contains("control_plane") {
        return Err(EhdbConfigError(
            "EHDB local_reference mode requires data-plane capabilities".to_string(),
        ));
    }

    if contract.local_reference_log.is_none() {
        return Err(EhdbConfigError(
            "NOETL_EHDB_LOCAL_REFERENCE_LOG is required for local_reference mode".to_string(),
        ));
    }

    Ok(())
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

    #[test]
    fn disabled_by_default() {
        let c = contract_from_env(&env(&[])).unwrap();
        assert!(!c.enabled);
        assert_eq!(c.mode, EhdbIntegrationMode::Disabled);
        assert!(!c.uses_local_reference_runtime());
    }

    #[test]
    fn worker_local_reference_valid() {
        let c = contract_from_env(&env(&[
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "worker"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
        ]))
        .unwrap();
        assert!(c.uses_local_reference_runtime());
        assert_eq!(c.role, EhdbClientRole::Worker);
        assert!(c.local_reference_log.is_some());
    }

    #[test]
    fn control_plane_role_rejects_data_plane_env() {
        // A gateway role handed a local-reference log is a hard error — the
        // control-plane boundary in config form.
        let err = contract_from_env(&env(&[
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "gateway"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
        ]))
        .unwrap_err();
        assert!(err.0.contains("gateway/api/server"));
    }

    #[test]
    fn control_plane_mode_valid_for_server() {
        let c = contract_from_env(&env(&[
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "control_plane"),
            ("NOETL_EHDB_CLIENT_ROLE", "server"),
            ("NOETL_EHDB_CAPABILITIES", "control_plane"),
        ]))
        .unwrap();
        assert_eq!(c.mode, EhdbIntegrationMode::ControlPlane);
        assert!(!c.uses_local_reference_runtime());
    }

    #[test]
    fn local_reference_requires_log() {
        let err = contract_from_env(&env(&[
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "worker"),
        ]))
        .unwrap_err();
        assert!(err.0.contains("NOETL_EHDB_LOCAL_REFERENCE_LOG"));
    }
}
