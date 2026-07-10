//! Dedicated **external Flight SQL data-plane endpoint** ([noetl/ai-meta#184]).
//!
//! This is the deployed half of the external EHDB driver MVP: a Flight SQL
//! listener, co-located with the worker data plane, that lets applications
//! *outside* the NoETL platform run a bounded read-only `SELECT` against the
//! projection read-model tier and get real rows. The reusable Flight SQL
//! surface lives in `ehdb-service` (`ehdb_service::flight_sql`); this module
//! wires it to a real projection read model, a real token verifier, and the
//! worker's shutdown lifecycle.
//!
//! ## Fail-safe, off by default
//!
//! [`FlightSqlConfig::from_env`] returns `None` — so the listener never even
//! binds — unless **all** of these hold:
//! - `NOETL_EHDB_FLIGHT_SQL` is truthy (the opt-in),
//! - a resolvable **data-plane** EHDB contract with a local-reference log
//!   (the projection store to read),
//! - an auth mode is satisfiable: either `NOETL_EHDB_FLIGHT_SQL_TOKEN_ALIAS`
//!   names a keychain credential (the external shape), **or**
//!   `NOETL_EHDB_FLIGHT_SQL_ALLOW_LOOPBACK_UNAUTH` is truthy *and* the bind is
//!   loopback (local-reference harness only).
//!
//! An external (non-loopback) bind with no token alias is **refused** —
//! fail-closed, never an open external read surface.
//!
//! ## Read-only, committed-only, secret-free
//!
//! The endpoint reaches the projection tier only through the read-only
//! [`ProjectionReadModel`] seam ([`ProjectionDriverReadModel`] over the
//! [`LocalReferenceProjectionEngine`]), so it cannot write. Responses carry
//! only the projected, secret-free read-model columns. The scoped read token
//! is resolved from the platform keychain (the control-plane credential API,
//! which resolves keychain → GSM) at startup, held in a zeroizing buffer, and
//! compared in constant time — **no token value is put on the wire response or
//! logged.**
//!
//! ## Integration seam with #178
//!
//! The projection read is served **on top of** the #178 read contract:
//! [`ProjectionDriverReadModel`] adapts the same `ProjectionDriver` the
//! `/api/ehdb/tiers/{tier}` handler resolves. When the #178 worker-side query
//! handler lands, swap the model construction below for a handler-backed
//! [`ProjectionReadModel`] — nothing else here changes.
//!
//! [noetl/ai-meta#184]: https://github.com/noetl/ai-meta/issues/184

use std::net::SocketAddr;
use std::sync::Arc;

use ehdb_reference::{
    LocalReferenceProjectionEngine, DEFAULT_LOCAL_REFERENCE_NAMESPACE,
    DEFAULT_LOCAL_REFERENCE_TENANT,
};
use ehdb_service::flight_sql::{
    serve, FlightSqlProjectionService, ProjectionDriverReadModel, ProjectionReadModel,
    ReadTokenVerifier,
};
use tokio::task::JoinHandle;
use zeroize::Zeroizing;

use super::contract::{contract_from_env, EhdbContract};
use super::guard::assert_data_plane_access_allowed;
use super::{process_env, EnvMap};
use crate::client::ControlPlaneClient;

/// Opt-in flag. Truthy ⇒ the endpoint may start (subject to the other gates).
pub const ENABLE_ENV: &str = "NOETL_EHDB_FLIGHT_SQL";
/// Bind address for the Flight SQL listener (default [`DEFAULT_BIND`]).
pub const BIND_ENV: &str = "NOETL_EHDB_FLIGHT_SQL_BIND";
/// Keychain credential alias whose token grants external read access.
pub const TOKEN_ALIAS_ENV: &str = "NOETL_EHDB_FLIGHT_SQL_TOKEN_ALIAS";
/// Which field of the resolved credential carries the token (default `token`).
pub const TOKEN_FIELD_ENV: &str = "NOETL_EHDB_FLIGHT_SQL_TOKEN_FIELD";
/// Escape hatch: run without auth **only** on a loopback bind (harness use).
pub const ALLOW_LOOPBACK_UNAUTH_ENV: &str = "NOETL_EHDB_FLIGHT_SQL_ALLOW_LOOPBACK_UNAUTH";
/// Tenant scope for the projection engine (default [`DEFAULT_LOCAL_REFERENCE_TENANT`]).
pub const TENANT_ENV: &str = "NOETL_EHDB_TENANT";
/// Namespace scope for the projection engine (default [`DEFAULT_LOCAL_REFERENCE_NAMESPACE`]).
pub const NAMESPACE_ENV: &str = "NOETL_EHDB_NAMESPACE";

/// Default listener address — all interfaces, port 8092.
pub const DEFAULT_BIND: &str = "0.0.0.0:8092";
/// Sentinel execution id for the startup credential resolve (no execution
/// scopes a startup token fetch; the server uses this only for cache/audit).
const STARTUP_EXECUTION_ID: i64 = 0;

fn truthy(value: Option<&String>) -> bool {
    matches!(
        value.map(|s| s.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

/// How the endpoint authenticates external callers.
enum AuthMode {
    /// Resolve the expected token from a keychain credential alias.
    Token { alias: String, field: String },
    /// No auth — valid only on a loopback bind (harness).
    LoopbackUnauth,
}

/// Fully-resolved endpoint config. Constructed only when every gate in
/// [`FlightSqlConfig::from_env`] is satisfied.
pub struct FlightSqlConfig {
    bind: SocketAddr,
    contract: EhdbContract,
    tenant: String,
    namespace: String,
    auth: AuthMode,
}

impl FlightSqlConfig {
    /// Resolve the endpoint config from the process environment, or `None` when
    /// the endpoint is not fully opted-in. `None` ⇒ the listener never binds.
    pub fn from_env() -> Option<Self> {
        Self::from_env_map(process_env())
    }

    fn from_env_map(env: EnvMap) -> Option<Self> {
        if !truthy(env.get(ENABLE_ENV)) {
            return None;
        }
        let bind_raw = env
            .get(BIND_ENV)
            .map(String::as_str)
            .unwrap_or(DEFAULT_BIND);
        let bind: SocketAddr = match bind_raw.parse() {
            Ok(addr) => addr,
            Err(err) => {
                tracing::error!(bind = %bind_raw, error = %err, "invalid NOETL_EHDB_FLIGHT_SQL_BIND; endpoint not started");
                return None;
            }
        };

        let contract = match contract_from_env(&env) {
            Ok(contract) => contract,
            Err(err) => {
                tracing::warn!(error = %err, "EHDB contract unresolved; Flight SQL endpoint not started");
                return None;
            }
        };
        if !contract.role.is_data_plane() {
            tracing::warn!(role = %contract.role.as_str(), "Flight SQL endpoint requires a data-plane role; not started");
            return None;
        }
        if !contract.uses_local_reference_runtime() {
            tracing::warn!("Flight SQL endpoint needs a local-reference projection store (NOETL_EHDB_LOCAL_REFERENCE_LOG); not started");
            return None;
        }

        let tenant = env
            .get(TENANT_ENV)
            .cloned()
            .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string());
        let namespace = env
            .get(NAMESPACE_ENV)
            .cloned()
            .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string());

        let auth = match env
            .get(TOKEN_ALIAS_ENV)
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            Some(alias) => AuthMode::Token {
                alias: alias.to_string(),
                field: env
                    .get(TOKEN_FIELD_ENV)
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "token".to_string()),
            },
            None => {
                if truthy(env.get(ALLOW_LOOPBACK_UNAUTH_ENV)) && bind.ip().is_loopback() {
                    AuthMode::LoopbackUnauth
                } else {
                    tracing::error!(bind = %bind, "Flight SQL endpoint refuses to start: a non-loopback bind requires NOETL_EHDB_FLIGHT_SQL_TOKEN_ALIAS (fail-closed)");
                    return None;
                }
            }
        };

        Some(Self {
            bind,
            contract,
            tenant,
            namespace,
            auth,
        })
    }
}

/// Constant-time byte comparison for token verification. Returns `false`
/// immediately on a length mismatch (token length is not secret), else folds
/// the XOR of every byte so the compare time does not depend on the position
/// of the first differing byte.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// A [`ReadTokenVerifier`] holding the expected scoped read token resolved from
/// the platform keychain. The value is zeroized on drop and never logged or
/// surfaced.
struct KeychainReadTokenVerifier {
    expected: Zeroizing<Vec<u8>>,
}

impl ReadTokenVerifier for KeychainReadTokenVerifier {
    fn verify(&self, presented_token: &str) -> bool {
        constant_time_eq(self.expected.as_slice(), presented_token.as_bytes())
    }
}

/// Spawn the Flight SQL endpoint task. The task resolves the scoped read token
/// (for the authenticated mode), builds the read-only projection service, and
/// serves until the returned handle is aborted (worker shutdown).
pub fn spawn(
    config: FlightSqlConfig,
    client: ControlPlaneClient,
    worker_id: String,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Defense-in-depth: the endpoint is a data-plane surface; refuse a
        // control-plane role even though from_env already checked it.
        if let Err(err) = assert_data_plane_access_allowed(config.contract.role, "flight_sql_serve")
        {
            tracing::error!(error = %err, "Flight SQL endpoint denied by data-plane guard");
            return;
        }

        let log_path = match config.contract.local_reference_log.clone() {
            Some(path) => path,
            None => {
                tracing::error!("Flight SQL endpoint has no local-reference log; not serving");
                return;
            }
        };
        let engine = LocalReferenceProjectionEngine::new(
            log_path,
            config.tenant.clone(),
            config.namespace.clone(),
        );
        let model: Arc<dyn ProjectionReadModel> = Arc::new(ProjectionDriverReadModel::new(engine));

        let service = match &config.auth {
            AuthMode::LoopbackUnauth => {
                tracing::warn!(
                    bind = %config.bind,
                    "Flight SQL endpoint serving WITHOUT auth (loopback local-reference only)"
                );
                FlightSqlProjectionService::new_local_reference(model)
            }
            AuthMode::Token { alias, field } => {
                match resolve_token(&client, alias, field, &worker_id).await {
                    Some(token) => {
                        let verifier: Arc<dyn ReadTokenVerifier> =
                            Arc::new(KeychainReadTokenVerifier { expected: token });
                        FlightSqlProjectionService::new(model, verifier)
                    }
                    None => {
                        // resolve_token logged the specific reason.
                        return;
                    }
                }
            }
        };

        tracing::info!(
            bind = %config.bind,
            role = %config.contract.role.as_str(),
            "starting external Flight SQL projection endpoint (read-only, projection tier)"
        );
        // Runs until the JoinHandle is aborted on worker shutdown.
        let shutdown = std::future::pending::<()>();
        if let Err(err) = serve(service, config.bind, shutdown).await {
            tracing::error!(error = %err, "Flight SQL endpoint terminated with error");
        }
    })
}

/// Resolve the expected read token from the platform keychain (control-plane
/// credential API → keychain → GSM). Returns the token in a zeroizing buffer,
/// or `None` (logging the reason) when it cannot be resolved — the endpoint
/// then fails closed and does not serve.
async fn resolve_token(
    client: &ControlPlaneClient,
    alias: &str,
    field: &str,
    _worker_id: &str,
) -> Option<Zeroizing<Vec<u8>>> {
    match client.get_credential(alias, STARTUP_EXECUTION_ID).await {
        Ok(Some(credential)) => match credential.data.get(field).and_then(|v| v.as_str()) {
            Some(token) if !token.is_empty() => Some(Zeroizing::new(token.as_bytes().to_vec())),
            _ => {
                tracing::error!(
                    alias = %alias,
                    field = %field,
                    "Flight SQL token field missing/empty in credential; endpoint not started"
                );
                None
            }
        },
        Ok(None) => {
            tracing::error!(alias = %alias, "Flight SQL token credential alias not found; endpoint not started");
            None
        }
        Err(err) => {
            tracing::error!(alias = %alias, error = %err, "resolving Flight SQL token failed; endpoint not started");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_of(pairs: &[(&str, &str)]) -> EnvMap {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn disabled_by_default() {
        assert!(FlightSqlConfig::from_env_map(EnvMap::new()).is_none());
    }

    #[test]
    fn enabled_but_no_contract_is_none() {
        // Truthy flag but no EHDB contract env → not started.
        let env = env_of(&[(ENABLE_ENV, "true")]);
        assert!(FlightSqlConfig::from_env_map(env).is_none());
    }

    #[test]
    fn external_bind_without_token_alias_is_refused() {
        // A full data-plane contract, external bind, but no token alias and no
        // loopback-unauth opt-in → fail closed.
        let env = env_of(&[
            (ENABLE_ENV, "true"),
            (BIND_ENV, "0.0.0.0:8092"),
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "worker"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/ehdb/proj.jsonl"),
        ]);
        assert!(FlightSqlConfig::from_env_map(env).is_none());
    }

    #[test]
    fn control_plane_role_is_refused() {
        let env = env_of(&[
            (ENABLE_ENV, "true"),
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "server"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/ehdb/proj.jsonl"),
            (TOKEN_ALIAS_ENV, "ehdb-flight-read"),
        ]);
        assert!(FlightSqlConfig::from_env_map(env).is_none());
    }

    #[test]
    fn constant_time_eq_matches_and_rejects() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secreT"));
        assert!(!constant_time_eq(b"secret", b"secret-longer"));
        assert!(!constant_time_eq(b"", b"x"));
    }

    #[test]
    fn keychain_verifier_gate() {
        let v = KeychainReadTokenVerifier {
            expected: Zeroizing::new(b"good-token".to_vec()),
        };
        assert!(v.verify("good-token"));
        assert!(!v.verify("bad-token"));
        assert!(!v.verify(""));
    }
}
