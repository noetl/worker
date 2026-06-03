//! Resolve playbook `auth: "<alias>"` strings into concrete tool config fields.
//!
//! See [noetl/ai-meta#48](https://github.com/noetl/ai-meta/issues/48)
//! for the regression brief.  The Python worker accepts a bare string
//! in `auth:` and treats it as a keychain alias — looks up the
//! credential at dispatch time and merges the type-specific fields
//! into the tool config.  The Rust worker's noetl-tools registry
//! expects either an `AuthConfig` struct (for `http`, `result_fetch`,
//! ...) or the tool's flat connection fields (for `postgres`,
//! `duckdb`, ...).  Until this helper landed, a string in `auth:`
//! caused serde to fail with `expected struct AuthConfig`.
//!
//! This module pre-processes the tool config JSON **before** it
//! reaches `serde_json::from_value(...)`.  When `auth` is a string:
//!
//! 1. Fetch the credential via [`ControlPlaneClient::get_credential`]
//!    (`GET /api/credentials/{alias}?include_data=true`).
//! 2. Branch on the credential's `type` field:
//!    - `postgres` → strip `auth`; merge `db_host`/`db_port`/`db_user`/
//!      `db_password`/`db_name` into the tool's flat connection fields
//!      (`host`/`port`/`user`/`password`/`database`).
//!    - `bearer` / `bearer_token` → replace `auth` with the noetl-tools
//!      `AuthConfig` shape `{type: bearer, credential: <alias>}` so
//!      the existing `AuthResolver.resolve_bearer` keychain lookup
//!      fires.  The bearer value lives in `data.token` and gets
//!      copied into `ExecutionContext.secrets` so the resolver finds
//!      it.
//!    - `api_key` → `{type: api_key, credential: <alias>}` + secret
//!      copy.
//!    - `basic` → `{type: basic, credential: <alias>, username:
//!      data.username}` + secret copy of the password.
//!    - Anything else → return a clear error that names the type.
//! 3. If the alias isn't in the keychain (server returns 404) → emit
//!    a clear `Credential alias '<name>' not found in keychain`
//!    error rather than the cryptic serde mismatch the worker used
//!    to surface.
//!
//! The helper is idempotent: if `auth` is already a struct (or
//! absent), it's a no-op and the helper returns immediately.

use std::collections::HashMap;

use anyhow::{Context as _, Result};
use serde_json::Value;

use crate::client::{Credential, ControlPlaneClient};

/// Mapping from credential `data` keys to the postgres tool's flat
/// connection-config keys.  Mirrors the Python normalization in
/// `noetl/core/auth/postgres.py` so the Rust path produces an
/// identical connection string for the same keychain alias.
const POSTGRES_FIELD_MAP: &[(&str, &str)] = &[
    ("db_host", "host"),
    ("db_port", "port"),
    ("db_user", "user"),
    ("db_password", "password"),
    ("db_name", "database"),
    // Some operator-imported credentials use the unprefixed names —
    // accept both shapes so a hand-edited keychain row still works.
    ("host", "host"),
    ("port", "port"),
    ("user", "user"),
    ("password", "password"),
    ("database", "database"),
];

/// Resolve a string `auth:` value into concrete tool config fields.
///
/// Mutates `tool_config_value` in place.  Returns the secret name
/// (alias) → value pairs the caller must inject into the
/// `ExecutionContext.secrets` map so the noetl-tools `AuthResolver`
/// can lift them at request time.  Returns an empty map when no
/// secret seeding is required (postgres path mutates fields directly).
///
/// No-op if `tool_config_value` is not an object or its `auth` field
/// is missing / already a struct.
pub async fn resolve_auth_alias(
    tool_config_value: &mut Value,
    client: &ControlPlaneClient,
    execution_id: i64,
) -> Result<HashMap<String, String>> {
    let map = match tool_config_value.as_object_mut() {
        Some(m) => m,
        None => return Ok(HashMap::new()),
    };

    // Pop the `auth` slot only if it's a string — leave struct /
    // mapping values untouched so existing playbooks (and the
    // noetl-tools `AuthConfig` deserializer) keep working unchanged.
    let alias = match map.get("auth") {
        Some(Value::String(s)) => s.clone(),
        _ => return Ok(HashMap::new()),
    };

    let credential = client
        .get_credential(&alias, execution_id)
        .await
        .with_context(|| format!("looking up credential alias '{alias}' in keychain"))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Credential alias '{}' not found in keychain (server returned 404 for /api/credentials/{})",
                alias,
                alias
            )
        })?;

    apply_credential(map, &alias, &credential)
}

/// Apply a resolved credential to a tool-config JSON object.
///
/// Separated from [`resolve_auth_alias`] so unit tests can drive the
/// resolution path with stub `Credential` values without standing up
/// a mock HTTP server.
fn apply_credential(
    map: &mut serde_json::Map<String, Value>,
    alias: &str,
    credential: &Credential,
) -> Result<HashMap<String, String>> {
    let cred_type = credential.cred_type.to_lowercase();
    match cred_type.as_str() {
        "postgres" => apply_postgres(map, &credential.data),
        "bearer" | "bearer_token" => Ok(apply_bearer(map, alias, &credential.data)),
        "api_key" => Ok(apply_api_key(map, alias, &credential.data)),
        "basic" => Ok(apply_basic(map, alias, &credential.data)),
        other => Err(anyhow::anyhow!(
            "Credential alias '{}' has unsupported type '{}'.  Supported types: postgres, bearer, api_key, basic.  File an issue if your tool needs another type.",
            alias,
            other
        )),
    }
}

fn apply_postgres(
    map: &mut serde_json::Map<String, Value>,
    data: &HashMap<String, Value>,
) -> Result<HashMap<String, String>> {
    map.remove("auth");

    for (src, dst) in POSTGRES_FIELD_MAP {
        let Some(value) = data.get(*src) else { continue };
        // Don't clobber explicit playbook overrides — operator wrote
        // `port: 6543` to override the keychain default, keep that.
        if map.contains_key(*dst) {
            continue;
        }

        // The postgres tool config has typed fields (port: u16,
        // database: string, etc.).  Coerce strings to the right
        // shape where it matters — keychain payloads typically store
        // everything as strings.
        let coerced = if *dst == "port" {
            coerce_port(value)?
        } else {
            value.clone()
        };
        map.insert((*dst).to_string(), coerced);
    }

    Ok(HashMap::new())
}

fn coerce_port(value: &Value) -> Result<Value> {
    match value {
        Value::Number(_) => Ok(value.clone()),
        Value::String(s) => {
            let parsed: u16 = s
                .parse()
                .with_context(|| format!("postgres credential 'db_port' is not a u16: {s:?}"))?;
            Ok(serde_json::json!(parsed))
        }
        _ => Err(anyhow::anyhow!(
            "postgres credential 'db_port' has unsupported JSON shape: {value:?}"
        )),
    }
}

fn apply_bearer(
    map: &mut serde_json::Map<String, Value>,
    alias: &str,
    data: &HashMap<String, Value>,
) -> HashMap<String, String> {
    let token = data
        .get("token")
        .or_else(|| data.get("access_token"))
        .or_else(|| data.get("bearer_token"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    map.insert(
        "auth".to_string(),
        serde_json::json!({
            "type": "bearer",
            "credential": alias,
        }),
    );

    let mut secrets = HashMap::new();
    secrets.insert(alias.to_string(), token);
    secrets
}

fn apply_api_key(
    map: &mut serde_json::Map<String, Value>,
    alias: &str,
    data: &HashMap<String, Value>,
) -> HashMap<String, String> {
    let token = data
        .get("api_key")
        .or_else(|| data.get("key"))
        .or_else(|| data.get("token"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let header = data
        .get("header")
        .and_then(|v| v.as_str())
        .unwrap_or("X-API-Key")
        .to_string();

    map.insert(
        "auth".to_string(),
        serde_json::json!({
            "type": "api_key",
            "credential": alias,
            "header": header,
        }),
    );

    let mut secrets = HashMap::new();
    secrets.insert(alias.to_string(), token);
    secrets
}

fn apply_basic(
    map: &mut serde_json::Map<String, Value>,
    alias: &str,
    data: &HashMap<String, Value>,
) -> HashMap<String, String> {
    let username = data
        .get("username")
        .or_else(|| data.get("user"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let password = data
        .get("password")
        .or_else(|| data.get("db_password"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    map.insert(
        "auth".to_string(),
        serde_json::json!({
            "type": "basic",
            "credential": alias,
            "username": username,
        }),
    );

    let mut secrets = HashMap::new();
    secrets.insert(alias.to_string(), password);
    secrets
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cred(name: &str, cred_type: &str, data: serde_json::Value) -> Credential {
        Credential {
            id: "1".to_string(),
            name: name.to_string(),
            cred_type: cred_type.to_string(),
            data: serde_json::from_value(data).unwrap(),
            tags: Vec::new(),
            description: None,
        }
    }

    #[test]
    fn postgres_alias_merges_db_fields_into_flat_config() {
        let mut cfg = serde_json::json!({
            "kind": "postgres",
            "auth": "pg_local",
            "command": "SELECT 1",
        });
        let c = cred(
            "pg_local",
            "postgres",
            serde_json::json!({
                "db_host": "postgres.local",
                "db_port": "5432",
                "db_user": "demo",
                "db_password": "demo_pw",
                "db_name": "demo_noetl",
            }),
        );

        let map = cfg.as_object_mut().unwrap();
        let secrets = apply_credential(map, "pg_local", &c).unwrap();

        assert!(secrets.is_empty(), "postgres path does not seed secrets");
        assert!(!map.contains_key("auth"), "auth field stripped");
        assert_eq!(map.get("host").unwrap(), "postgres.local");
        assert_eq!(map.get("port").unwrap(), 5432);
        assert_eq!(map.get("user").unwrap(), "demo");
        assert_eq!(map.get("password").unwrap(), "demo_pw");
        assert_eq!(map.get("database").unwrap(), "demo_noetl");
        // Tool-specific fields preserved.
        assert_eq!(map.get("kind").unwrap(), "postgres");
        assert_eq!(map.get("command").unwrap(), "SELECT 1");
    }

    #[test]
    fn postgres_alias_does_not_clobber_explicit_playbook_overrides() {
        let mut cfg = serde_json::json!({
            "kind": "postgres",
            "auth": "pg_local",
            // Playbook overrides the credential's port — keep it.
            "port": 6543,
        });
        let c = cred(
            "pg_local",
            "postgres",
            serde_json::json!({
                "db_host": "postgres.local",
                "db_port": "5432",
            }),
        );

        let map = cfg.as_object_mut().unwrap();
        apply_credential(map, "pg_local", &c).unwrap();

        assert_eq!(map.get("port").unwrap(), 6543);
        assert_eq!(map.get("host").unwrap(), "postgres.local");
    }

    #[test]
    fn postgres_alias_accepts_unprefixed_keys_too() {
        // Some operator-imported credentials store host/port/etc.
        // without the `db_` prefix.
        let mut cfg = serde_json::json!({
            "kind": "postgres",
            "auth": "pg_legacy",
        });
        let c = cred(
            "pg_legacy",
            "postgres",
            serde_json::json!({
                "host": "legacy.host",
                "port": 5432,
                "user": "legacy",
                "password": "legacy_pw",
                "database": "legacy_db",
            }),
        );

        let map = cfg.as_object_mut().unwrap();
        apply_credential(map, "pg_legacy", &c).unwrap();

        assert_eq!(map.get("host").unwrap(), "legacy.host");
        assert_eq!(map.get("port").unwrap(), 5432);
    }

    #[test]
    fn bearer_alias_produces_auth_config_and_seeds_secret() {
        let mut cfg = serde_json::json!({
            "kind": "http",
            "auth": "api_token",
            "url": "https://example.com/api",
        });
        let c = cred(
            "api_token",
            "bearer",
            serde_json::json!({"token": "abcd1234"}),
        );

        let map = cfg.as_object_mut().unwrap();
        let secrets = apply_credential(map, "api_token", &c).unwrap();

        let auth = map.get("auth").unwrap();
        assert_eq!(auth["type"], "bearer");
        assert_eq!(auth["credential"], "api_token");
        assert_eq!(secrets.get("api_token").unwrap(), "abcd1234");
    }

    #[test]
    fn api_key_alias_carries_custom_header_when_present() {
        let mut cfg = serde_json::json!({
            "kind": "http",
            "auth": "duffel",
            "url": "https://api.duffel.com",
        });
        let c = cred(
            "duffel",
            "api_key",
            serde_json::json!({
                "api_key": "secret",
                "header": "X-Duffel-Key",
            }),
        );

        let map = cfg.as_object_mut().unwrap();
        let secrets = apply_credential(map, "duffel", &c).unwrap();

        let auth = map.get("auth").unwrap();
        assert_eq!(auth["type"], "api_key");
        assert_eq!(auth["header"], "X-Duffel-Key");
        assert_eq!(secrets.get("duffel").unwrap(), "secret");
    }

    #[test]
    fn api_key_alias_defaults_header_when_missing() {
        let mut cfg = serde_json::json!({
            "kind": "http",
            "auth": "duffel",
        });
        let c = cred(
            "duffel",
            "api_key",
            serde_json::json!({"api_key": "secret"}),
        );

        let map = cfg.as_object_mut().unwrap();
        apply_credential(map, "duffel", &c).unwrap();

        let auth = map.get("auth").unwrap();
        assert_eq!(auth["header"], "X-API-Key");
    }

    #[test]
    fn basic_alias_produces_auth_config_and_seeds_password_secret() {
        let mut cfg = serde_json::json!({
            "kind": "http",
            "auth": "service_basic",
        });
        let c = cred(
            "service_basic",
            "basic",
            serde_json::json!({"username": "svc", "password": "svc_pw"}),
        );

        let map = cfg.as_object_mut().unwrap();
        let secrets = apply_credential(map, "service_basic", &c).unwrap();

        let auth = map.get("auth").unwrap();
        assert_eq!(auth["type"], "basic");
        assert_eq!(auth["username"], "svc");
        assert_eq!(secrets.get("service_basic").unwrap(), "svc_pw");
    }

    #[test]
    fn unsupported_type_returns_named_error() {
        let mut cfg = serde_json::json!({"kind": "http", "auth": "weird"});
        let c = cred("weird", "exotic", serde_json::json!({}));

        let map = cfg.as_object_mut().unwrap();
        let err = apply_credential(map, "weird", &c).unwrap_err().to_string();

        assert!(
            err.contains("unsupported type 'exotic'"),
            "error message must name the offending type, got: {err}"
        );
    }

    #[tokio::test]
    async fn no_op_when_auth_field_is_already_a_struct() {
        // No HTTP call should be made when auth is already a struct.
        // We pass a non-functional client and rely on the early return.
        let client = ControlPlaneClient::new("http://0.0.0.0:0");
        let mut cfg = serde_json::json!({
            "kind": "http",
            "auth": {"type": "bearer", "credential": "explicit"},
        });

        let secrets = resolve_auth_alias(&mut cfg, &client, 1).await.unwrap();
        assert!(secrets.is_empty());
        assert_eq!(cfg["auth"]["type"], "bearer");
        assert_eq!(cfg["auth"]["credential"], "explicit");
    }

    #[tokio::test]
    async fn no_op_when_tool_config_has_no_auth_field() {
        let client = ControlPlaneClient::new("http://0.0.0.0:0");
        let mut cfg = serde_json::json!({
            "kind": "python",
            "code": "result = {'status': 'ok'}",
        });

        let secrets = resolve_auth_alias(&mut cfg, &client, 1).await.unwrap();
        assert!(secrets.is_empty());
        assert!(cfg.get("auth").is_none());
    }

    #[tokio::test]
    async fn no_op_when_tool_config_is_not_an_object() {
        let client = ControlPlaneClient::new("http://0.0.0.0:0");
        let mut cfg = serde_json::json!("not an object");

        let secrets = resolve_auth_alias(&mut cfg, &client, 1).await.unwrap();
        assert!(secrets.is_empty());
    }
}
