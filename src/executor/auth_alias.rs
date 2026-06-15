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

use crate::client::{ControlPlaneClient, Credential};

/// Classified failure from credential-alias resolution.
///
/// Distinguishes a **terminal** failure — a clean 404 from the keychain
/// (the alias isn't bound), an unsupported credential type, or a
/// malformed credential shape — from a **retryable** transport error
/// where the keychain HTTP call itself failed and a later attempt might
/// succeed.  None of the terminal cases get fixed by retrying; the
/// transport case might.
///
/// The command executor branches on
/// [`CredentialResolutionError::is_terminal`] to decide whether to emit
/// a terminal `call.error` (so the execution fails cleanly instead of
/// hanging at `command.started`) or to leave the command path's
/// retry/redelivery semantics in place.  Classifying with a typed error
/// keeps that decision off fragile `anyhow`-message string matching.
/// See [noetl/ai-meta#78](https://github.com/noetl/ai-meta/issues/78).
#[derive(Debug, thiserror::Error)]
pub enum CredentialResolutionError {
    /// The keychain returned a clean 404 for `/api/keychain/<alias>` —
    /// the alias is not bound.  **Terminal**: the binding won't appear
    /// on a retry, so the execution should fail cleanly rather than
    /// hang.  (The credential *record* may still exist in the separate
    /// `/api/credentials/<alias>` store; this error is specifically the
    /// keychain-binding lookup the worker performs at dispatch time.)
    #[error("Credential alias '{alias}' not found in keychain (server returned 404 for /api/credentials/{alias})")]
    AliasNotFound { alias: String },

    /// The keychain HTTP call failed at the transport layer (connection
    /// refused, timeout, 5xx, TLS error, ...).  **Retryable**: a later
    /// attempt may reach a healthy keychain.  The command executor does
    /// NOT emit a terminal `call.error` for this on a fresh command —
    /// only once the command's attempt counter is exhausted.
    #[error("transient error looking up credential alias '{alias}' in keychain")]
    Transient {
        alias: String,
        #[source]
        source: anyhow::Error,
    },

    /// The credential resolved but its type/shape can't be applied
    /// (unsupported credential type, malformed `db_port`, ...).
    /// **Terminal**: the same bytes deserialize to the same error on a
    /// retry.
    #[error(transparent)]
    Invalid(#[from] anyhow::Error),
}

impl CredentialResolutionError {
    /// True when the failure will never succeed on retry.  The command
    /// executor emits a terminal `call.error` for these immediately;
    /// retryable (transient) failures are escalated to terminal only
    /// after the command's attempt counter is exhausted.
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Self::Transient { .. })
    }
}

/// HTTP statuses worth retrying — transient server / infrastructure
/// conditions where a later attempt may succeed.  Everything else
/// (400 / 401 / 403, a deterministic 500 like the keychain's
/// "Decryption failed: aead::Error", etc.) is treated as terminal so
/// the execution fails cleanly instead of hanging.  See
/// noetl/ai-meta#78.
fn is_retryable_status(status: u16) -> bool {
    matches!(
        status,
        408 /* Request Timeout */
        | 429 /* Too Many Requests */
        | 502 /* Bad Gateway */
        | 503 /* Service Unavailable */
        | 504 /* Gateway Timeout */
    )
}

/// Classify an error from the credential-fetch HTTP call into terminal
/// (`AliasNotFound` is handled by the caller; this returns `Invalid`)
/// vs retryable (`Transient`).
///
/// Inspects typed errors rather than string-matching messages
/// (noetl/ai-meta#78):
///
/// - A [`crate::client::CredentialHttpError`] carries the HTTP status —
///   retryable only for [`is_retryable_status`] codes; every other
///   status (incl. the keychain's 500 "Decryption failed") is terminal.
/// - A transport-layer [`reqwest::Error`] is retryable when it's a
///   connect/timeout/request failure, but terminal when it's a
///   decode/body failure (the response shape is wrong and won't change
///   on retry).
/// - Anything else (e.g. a sealed-envelope open/decrypt failure) is a
///   deterministic local error → terminal.
fn classify_fetch_error(alias: &str, err: anyhow::Error) -> CredentialResolutionError {
    if let Some(http) = err.downcast_ref::<crate::client::CredentialHttpError>() {
        if is_retryable_status(http.status) {
            return CredentialResolutionError::Transient {
                alias: alias.to_string(),
                source: err,
            };
        }
        return CredentialResolutionError::Invalid(err);
    }
    if let Some(re) = err.downcast_ref::<reqwest::Error>() {
        if re.is_decode() || re.is_body() {
            return CredentialResolutionError::Invalid(err);
        }
        return CredentialResolutionError::Transient {
            alias: alias.to_string(),
            source: err,
        };
    }
    CredentialResolutionError::Invalid(err)
}

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

/// Field aliases for message-source credentials (`nats` / `pubsub` / `kafka`).
///
/// Mirrors [`POSTGRES_FIELD_MAP`]: keychain rows commonly store the connection
/// under a tool-prefixed name (`nats_url`), but the `nats` / `subscription`
/// tool config deserializes the flat name (`url`).  Without this mapping the
/// prefixed fields land in the config as serde-unknown keys and are silently
/// dropped, so `resolve_nats_conn` sees no `url` and fails with
/// "NATS connection requires 'url' …".  Map the known prefixed shapes to the
/// flat names so a shipped `nats`-type credential resolves without the
/// operator hand-editing field names.  Unprefixed fields pass through the
/// verbatim loop in `apply_source_credential`.
const SOURCE_FIELD_MAP: &[(&str, &str)] = &[
    ("nats_url", "url"),
    ("nats_user", "user"),
    ("nats_username", "user"),
    ("nats_password", "password"),
    ("nats_token", "token"),
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
) -> Result<HashMap<String, String>, CredentialResolutionError> {
    // task_sequence pipelines carry the keychain alias on each
    // SUB-TASK (`credential:` / `auth:` inside a pipeline entry), not
    // on the outer envelope.  The task_sequence tool dispatches its
    // sub-tasks through noetl-tools' registry, which has no
    // ControlPlaneClient and so can't resolve aliases — every nested
    // postgres/http step therefore got no connection fields and fell
    // back to a default (unreachable) connection.  Pre-resolve each
    // pipeline task's inner spec here, in the worker, before the
    // task_sequence runs.  See noetl/worker#47.
    //
    // Detect the task_sequence shape without holding a borrow across
    // the await points below.
    let is_task_sequence = tool_config_value
        .as_object()
        .and_then(|m| m.get("kind"))
        .and_then(|v| v.as_str())
        == Some("task_sequence");

    if is_task_sequence {
        let mut all_secrets = HashMap::new();
        if let Some(Value::Array(tasks)) = tool_config_value
            .as_object_mut()
            .and_then(|m| m.get_mut("tool_config"))
        {
            // Each pipeline entry is a single-key `{label: spec}` map;
            // resolve the alias (if any) on each task's inner spec.
            for task in tasks.iter_mut() {
                if let Some(task_obj) = task.as_object_mut() {
                    for (_label, spec) in task_obj.iter_mut() {
                        let secrets = resolve_single_tool_alias(spec, client, execution_id).await?;
                        all_secrets.extend(secrets);
                    }
                }
            }
        }
        return Ok(all_secrets);
    }

    resolve_single_tool_alias(tool_config_value, client, execution_id).await
}

/// Resolve a keychain alias on a single (non-pipeline) tool config.
///
/// Looks for the alias under `auth` or `credential`, fetches the
/// credential from the keychain via the control-plane API, and
/// applies it (injecting flat connection fields for postgres, an
/// `AuthConfig` struct for bearer/api_key/basic).  Returns the
/// secrets to seed into the execution context (empty for postgres).
async fn resolve_single_tool_alias(
    tool_config_value: &mut Value,
    client: &ControlPlaneClient,
    execution_id: i64,
) -> Result<HashMap<String, String>, CredentialResolutionError> {
    let map = match tool_config_value.as_object_mut() {
        Some(m) => m,
        None => return Ok(HashMap::new()),
    };

    // Pop the alias slot only if it's a string — leave struct /
    // mapping values untouched so existing playbooks (and the
    // noetl-tools `AuthConfig` deserializer) keep working unchanged.
    //
    // The canonical v10 playbook YAML writes the keychain alias under
    // `credential:` (e.g. `credential: "{{ pg_auth }}"`); older
    // fixtures + the noetl/ai-meta#48 path use `auth:`.  Accept both
    // — check `auth` first, then `credential`.  Without the
    // `credential` fallback every v10 postgres/http step that
    // references a keychain alias got no connection fields injected
    // and the tool fell back to a default (unreachable) connection.
    let alias = match map.get("auth").or_else(|| map.get("credential")) {
        Some(Value::String(s)) => s.clone(),
        _ => return Ok(HashMap::new()),
    };

    // Classify the fetch outcome for the executor's terminal-vs-retryable
    // decision (noetl/ai-meta#78):
    //   * `Ok(None)` — the keychain returned a clean 404, the alias isn't
    //     bound → `AliasNotFound` (terminal).
    //   * `Err(_)` — a transport error or a non-success HTTP status;
    //     `classify_fetch_error` decides terminal vs retryable by
    //     inspecting the typed error (HTTP status code / reqwest
    //     predicates), NOT by string-matching the message.
    let credential = fetch_credential_maybe_sealed(client, &alias, execution_id)
        .await
        .map_err(|source| classify_fetch_error(&alias, source))?
        .ok_or_else(|| CredentialResolutionError::AliasNotFound {
            alias: alias.clone(),
        })?;

    let mut credential = credential;
    // `apply_credential` returns `anyhow::Error` for unsupported types /
    // malformed shapes; the `#[from]` arm classifies those as terminal
    // `Invalid`.
    let injected_secrets = apply_credential(map, &alias, &credential)?;
    // Phase 5c (noetl/ai-meta#61): zeroize the credential payload after the
    // dispatcher has copied what it needs into the tool config / context.
    // `apply_credential` reads `credential.data` by reference; once it
    // returns we're done with the resolved-secret bytes here.  The fields
    // landed in `tool_config_value` + `injected_secrets` are the caller's
    // to manage.
    use zeroize::Zeroize;
    for v in credential.data.values_mut() {
        if let Value::String(s) = v {
            s.zeroize();
        }
    }
    Ok(injected_secrets)
}

/// Route the credential fetch through the sealed endpoint when
/// `NOETL_SEALED_CREDENTIALS=true` (or `1`) is set and the pod identifies
/// itself via `WORKER_ID` (matching what the worker passes to
/// `register_worker`).
///
/// Defense-in-depth on top of Phase-4 mTLS: with sealing on, the resolved
/// secret travels as a [`ControlPlaneClient::get_sealed_credential`]
/// SealedEnvelope; the cleartext exists only briefly inside the worker
/// process after unseal, never inside the server's HTTP response body.
///
/// Defaults off — workers that don't opt in keep using the plaintext path
/// they always used, so this round can land before the deployment manifests
/// flip the flag.
async fn fetch_credential_maybe_sealed(
    client: &ControlPlaneClient,
    alias: &str,
    execution_id: i64,
) -> Result<Option<Credential>> {
    let sealed_enabled = std::env::var("NOETL_SEALED_CREDENTIALS")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
        .unwrap_or(false);
    if sealed_enabled {
        if let Ok(worker_id) = std::env::var("WORKER_ID") {
            if !worker_id.is_empty() {
                return client
                    .get_sealed_credential(alias, &worker_id, execution_id)
                    .await;
            }
        }
        tracing::warn!(
            alias = %alias,
            "NOETL_SEALED_CREDENTIALS=true but WORKER_ID is unset; falling back to plaintext credential fetch",
        );
    }
    client.get_credential(alias, execution_id).await
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
    // Strip BOTH alias keys so neither leaks into the tool config as
    // a stray string (the alias was carried under `auth` or
    // `credential`; the type-specific appliers below inject the real
    // connection fields / auth struct).
    map.remove("auth");
    map.remove("credential");

    let cred_type = credential.cred_type.to_lowercase();
    match cred_type.as_str() {
        "postgres" => apply_postgres(map, &credential.data),
        "bearer" | "bearer_token" => Ok(apply_bearer(map, alias, &credential.data)),
        "api_key" => Ok(apply_api_key(map, alias, &credential.data)),
        "basic" => Ok(apply_basic(map, alias, &credential.data)),
        // Message-source credentials (the `nats` tool + the `subscription`
        // tool's nats/pubsub/kafka backends) carry their connection in the
        // credential data (`url` / `user` / `password` / `token` / etc.).
        // Inject those fields directly into the tool config — the same shape
        // `apply_postgres` uses — so the tool reads them as explicit config.
        // The `auth` alias was already stripped by `apply_credential`, which
        // also avoids colliding with the outer `ToolConfig.auth`
        // (`Option<AuthConfig>`, which can't hold a bare alias string).
        // noetl/ai-meta#90 Phase 1 surfaced this during the in-cluster
        // subscription-tool E2E.
        "nats" | "pubsub" | "kafka" => Ok(apply_source_credential(map, &credential.data)),
        // Snowflake credentials carry sf_*-prefixed connection fields; map them
        // to the flat names the `snowflake` tool reads (account / user / ...),
        // same shape as `apply_postgres`.
        "snowflake" => apply_snowflake(map, &credential.data),
        other => Err(anyhow::anyhow!(
            "Credential alias '{}' has unsupported type '{}'.  Supported types: postgres, snowflake, bearer, api_key, basic, nats, pubsub, kafka.  File an issue if your tool needs another type.",
            alias,
            other
        )),
    }
}

/// Field aliases for `snowflake`-type credentials → the flat names the
/// `snowflake` tool config deserializes.  Mirrors [`POSTGRES_FIELD_MAP`].
const SNOWFLAKE_FIELD_MAP: &[(&str, &str)] = &[
    ("sf_account", "account"),
    ("sf_user", "user"),
    ("sf_password", "password"),
    ("sf_private_key", "private_key"),
    ("sf_private_key_passphrase", "private_key_passphrase"),
    ("sf_warehouse", "warehouse"),
    ("sf_database", "database"),
    ("sf_schema", "schema"),
    ("sf_role", "role"),
    // accept unprefixed shapes too (hand-edited keychain rows).
    ("account", "account"),
    ("user", "user"),
    ("password", "password"),
    ("private_key", "private_key"),
    ("warehouse", "warehouse"),
    ("database", "database"),
    ("schema", "schema"),
    ("role", "role"),
];

fn apply_snowflake(
    map: &mut serde_json::Map<String, Value>,
    data: &HashMap<String, Value>,
) -> Result<HashMap<String, String>> {
    for (src, dst) in SNOWFLAKE_FIELD_MAP {
        let Some(value) = data.get(*src) else {
            continue;
        };
        // Don't clobber an explicit playbook override.
        map.entry((*dst).to_string())
            .or_insert_with(|| value.clone());
    }
    Ok(HashMap::new())
}

fn apply_postgres(
    map: &mut serde_json::Map<String, Value>,
    data: &HashMap<String, Value>,
) -> Result<HashMap<String, String>> {
    // `auth` / `credential` already stripped by `apply_credential`.
    for (src, dst) in POSTGRES_FIELD_MAP {
        let Some(value) = data.get(*src) else {
            continue;
        };
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

/// Inject a message-source credential's data fields directly into the tool
/// config map, for the `nats` tool and the `subscription` tool's
/// nats/pubsub/kafka backends.
///
/// These tools resolve their connection from explicit config fields
/// (`url` / `user` / `password` / `token` / …) rather than a typed `auth:`
/// struct.  Like [`apply_postgres`], this merges the credential data into the
/// config (without clobbering explicit playbook overrides) and seeds no
/// secrets — and it deliberately does NOT re-attach `auth`, which the outer
/// `ToolConfig.auth` (`Option<AuthConfig>`) would reject as a bare string.
fn apply_source_credential(
    map: &mut serde_json::Map<String, Value>,
    data: &HashMap<String, Value>,
) -> HashMap<String, String> {
    // First, map known tool-prefixed connection fields (`nats_url` → `url`)
    // to the flat names the tool config deserializes.  Don't clobber an
    // explicit playbook override.
    for (src, dst) in SOURCE_FIELD_MAP {
        if let Some(value) = data.get(*src) {
            map.entry((*dst).to_string())
                .or_insert_with(|| value.clone());
        }
    }
    // Then inject any remaining fields verbatim — covers explicit flat names
    // (`url` / `user` / …) and pubsub/kafka-specific keys.  Leftover prefixed
    // keys (`nats_url`) are harmless: the tool config ignores serde-unknown
    // fields.  Still don't clobber an explicit playbook override.
    for (key, value) in data {
        map.entry(key.clone()).or_insert_with(|| value.clone());
    }
    HashMap::new()
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
    fn nats_alias_injects_connection_fields_and_strips_auth() {
        // noetl/ai-meta#90 Phase 1: a type-`nats` credential must merge its
        // connection fields into the tool config (so the `subscription` /
        // `nats` tool reads explicit `url`/`user`/`password`) and strip the
        // `auth` alias (the outer ToolConfig.auth can't hold a bare string),
        // rather than erroring as an unsupported type.
        let mut cfg = serde_json::json!({
            "kind": "subscription",
            "auth": "nats_e2e",
            "source": "nats",
            "operation": "poll",
            "stream": "ORDERS",
            "consumer": "orders-drain",
        });
        let c = cred(
            "nats_e2e",
            "nats",
            serde_json::json!({
                "url": "nats://nats.nats.svc.cluster.local:4222",
                "user": "noetl",
                "password": "noetl",
            }),
        );

        let map = cfg.as_object_mut().unwrap();
        let secrets = apply_credential(map, "nats_e2e", &c).unwrap();

        assert!(secrets.is_empty(), "source path seeds no secrets");
        assert!(!map.contains_key("auth"), "auth alias stripped");
        // Connection fields injected directly.
        assert_eq!(
            map.get("url").unwrap(),
            "nats://nats.nats.svc.cluster.local:4222"
        );
        assert_eq!(map.get("user").unwrap(), "noetl");
        assert_eq!(map.get("password").unwrap(), "noetl");
        // Tool-specific fields untouched.
        assert_eq!(map.get("source").unwrap(), "nats");
        assert_eq!(map.get("consumer").unwrap(), "orders-drain");
    }

    #[test]
    fn nats_alias_maps_prefixed_connection_fields_to_flat_names() {
        // The shipped `nats_credential` (repos/e2e/fixtures/credentials/
        // nats_credential.json) stores its connection under tool-prefixed
        // names (`nats_url` / `nats_user` / `nats_password`).  Those must be
        // mapped to the flat `url` / `user` / `password` the NatsConfig
        // deserializes — otherwise they're dropped as serde-unknown keys and
        // resolve_nats_conn fails with "NATS connection requires 'url' …".
        // Regression for the auth0_login cache_and_callback prod failure
        // (noetl/ai-meta#49 Phase F R5 cutover).
        let mut cfg = serde_json::json!({
            "kind": "nats",
            "auth": "nats_credential",
            "operation": "kv_put",
            "bucket": "sessions",
        });
        let c = cred(
            "nats_credential",
            "nats",
            serde_json::json!({
                "nats_url": "nats://noetl:noetl@nats.nats.svc.cluster.local:4222",
                "nats_user": "noetl",
                "nats_password": "noetl",
            }),
        );

        let map = cfg.as_object_mut().unwrap();
        let secrets = apply_credential(map, "nats_credential", &c).unwrap();

        assert!(secrets.is_empty(), "source path seeds no secrets");
        assert!(!map.contains_key("auth"), "auth alias stripped");
        // Prefixed fields mapped to the flat names the tool reads.
        assert_eq!(
            map.get("url").unwrap(),
            "nats://noetl:noetl@nats.nats.svc.cluster.local:4222"
        );
        assert_eq!(map.get("user").unwrap(), "noetl");
        assert_eq!(map.get("password").unwrap(), "noetl");
        // Tool-specific fields untouched.
        assert_eq!(map.get("operation").unwrap(), "kv_put");
        assert_eq!(map.get("bucket").unwrap(), "sessions");
    }

    #[test]
    fn snowflake_alias_maps_sf_fields_to_flat_config() {
        // sf_test ships sf_*-prefixed fields; map them to the flat names the
        // snowflake tool deserializes (account/user/warehouse/...).  Regression
        // for "Credential alias 'sf_test' has unsupported type 'snowflake'".
        let mut cfg = serde_json::json!({
            "kind": "snowflake",
            "auth": "sf_test",
            "command": "SELECT 1",
        });
        let c = cred(
            "sf_test",
            "snowflake",
            serde_json::json!({
                "sf_account": "abc-xy123",
                "sf_user": "noetl",
                "sf_password": "pw",
                "sf_warehouse": "WH",
                "sf_database": "DB",
                "sf_schema": "PUBLIC",
                "sf_role": "SYSADMIN",
            }),
        );
        let map = cfg.as_object_mut().unwrap();
        let secrets = apply_credential(map, "sf_test", &c).unwrap();
        assert!(secrets.is_empty());
        assert!(!map.contains_key("auth"), "auth alias stripped");
        assert_eq!(map.get("account").unwrap(), "abc-xy123");
        assert_eq!(map.get("user").unwrap(), "noetl");
        assert_eq!(map.get("warehouse").unwrap(), "WH");
        assert_eq!(map.get("schema").unwrap(), "PUBLIC");
        assert_eq!(map.get("role").unwrap(), "SYSADMIN");
        assert_eq!(map.get("command").unwrap(), "SELECT 1");
    }

    #[test]
    fn source_credential_does_not_clobber_explicit_override() {
        // An explicit playbook `url:` wins over the credential's url.
        let mut cfg = serde_json::json!({
            "kind": "nats",
            "auth": "nats_e2e",
            "url": "nats://override:4222",
        });
        let c = cred(
            "nats_e2e",
            "nats",
            serde_json::json!({ "url": "nats://cred:4222", "user": "noetl" }),
        );
        let map = cfg.as_object_mut().unwrap();
        apply_credential(map, "nats_e2e", &c).unwrap();
        assert_eq!(map.get("url").unwrap(), "nats://override:4222");
        assert_eq!(map.get("user").unwrap(), "noetl");
    }

    #[test]
    fn postgres_alias_under_credential_key_resolves_and_strips() {
        // Canonical v10 playbook YAML carries the keychain alias under
        // `credential:` (not `auth:`).  apply_credential must strip
        // BOTH alias keys and inject the flat connection fields.
        // Regression for noetl/ai-meta#54 Phase F R5 — postgres
        // fixtures stalled with "error connecting to server" because
        // the alias under `credential:` was never resolved.
        let mut cfg = serde_json::json!({
            "kind": "postgres",
            "credential": "pg_k8s",
            "command": "CREATE TABLE t (id int)",
        });
        let c = cred(
            "pg_k8s",
            "postgres",
            serde_json::json!({
                "db_host": "postgres.postgres.svc.cluster.local",
                "db_port": "5432",
                "db_user": "demo",
                "db_password": "demo_pw",
                "db_name": "demo_noetl",
            }),
        );

        let map = cfg.as_object_mut().unwrap();
        let secrets = apply_credential(map, "pg_k8s", &c).unwrap();

        assert!(secrets.is_empty());
        assert!(!map.contains_key("auth"), "auth stripped");
        assert!(
            !map.contains_key("credential"),
            "credential key stripped so it doesn't leak into PostgresConfig"
        );
        assert_eq!(
            map.get("host").unwrap(),
            "postgres.postgres.svc.cluster.local"
        );
        assert_eq!(map.get("port").unwrap(), 5432);
        assert_eq!(map.get("user").unwrap(), "demo");
        assert_eq!(map.get("password").unwrap(), "demo_pw");
        assert_eq!(map.get("database").unwrap(), "demo_noetl");
        assert_eq!(map.get("command").unwrap(), "CREATE TABLE t (id int)");
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

    // ----------------------------------------------------------------
    // Terminal-vs-retryable classification (noetl/ai-meta#78)
    //
    // The command executor branches on `is_terminal()` to decide
    // whether a pre-dispatch credential-alias failure emits a terminal
    // `call.error` (so the execution fails cleanly instead of hanging
    // at `command.started`) or stays retryable.  These tests pin the
    // classification at the boundary the executor reads.
    // ----------------------------------------------------------------

    use axum::{extract::Path, http::StatusCode, routing::get, Json, Router};
    use tokio::net::TcpListener;

    /// Spawn a mock keychain that returns `response` (200) for every
    /// `/api/credentials/{alias}` GET, or 404 when `response` is `None`.
    /// Returns `(base_url, server_handle)`; drop the handle to stop it.
    async fn spawn_keychain(
        response: Option<serde_json::Value>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let canned = response;
        let app = Router::new().route(
            "/api/credentials/{alias}",
            get(move |Path(_alias): Path<String>| {
                let canned = canned.clone();
                async move {
                    match canned {
                        Some(body) => Ok(Json(body)),
                        None => Err(StatusCode::NOT_FOUND),
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let base = format!("http://{}", addr);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("axum serve");
        });
        (base, handle)
    }

    /// A clean 404 from the keychain (the alias isn't bound) is the
    /// live-repro fixture from noetl/ai-meta#78 (`pg_noetl_k8s`).  It
    /// must classify as terminal `AliasNotFound` so the executor emits
    /// `call.error` rather than hanging.
    #[tokio::test]
    async fn alias_404_classifies_as_terminal_alias_not_found() {
        let (base, handle) = spawn_keychain(None).await;
        let client = ControlPlaneClient::new(&base);
        let mut cfg = serde_json::json!({
            "kind": "postgres",
            "auth": "pg_noetl_k8s",
            "command": "SELECT 1",
        });

        let err = resolve_auth_alias(&mut cfg, &client, 1)
            .await
            .expect_err("missing alias must error");

        assert!(
            matches!(err, CredentialResolutionError::AliasNotFound { ref alias } if alias == "pg_noetl_k8s"),
            "clean 404 must classify as AliasNotFound, got: {err:?}"
        );
        assert!(err.is_terminal(), "alias-404 must be terminal");
        handle.abort();
    }

    /// A transport error talking to the keychain (connection refused)
    /// must classify as retryable `Transient` — the executor leaves the
    /// command path's retry/redelivery in place rather than emitting a
    /// terminal `call.error` on the first failure.
    #[tokio::test]
    async fn transport_error_classifies_as_retryable_transient() {
        // Port 1 is unbound — the keychain HTTP call fails at the
        // transport layer.
        let client = ControlPlaneClient::new("http://127.0.0.1:1");
        let mut cfg = serde_json::json!({
            "kind": "postgres",
            "auth": "pg_noetl_k8s",
            "command": "SELECT 1",
        });

        let err = resolve_auth_alias(&mut cfg, &client, 1)
            .await
            .expect_err("transport failure must error");

        assert!(
            matches!(err, CredentialResolutionError::Transient { ref alias, .. } if alias == "pg_noetl_k8s"),
            "transport error must classify as Transient, got: {err:?}"
        );
        assert!(
            !err.is_terminal(),
            "transient transport error must stay retryable"
        );
    }

    /// HTTP 500 "Decryption failed: aead::Error" is the ACTUAL live
    /// repro from noetl/ai-meta#78 (the credential record exists but its
    /// stored ciphertext can't be decrypted server-side — sealing is
    /// off, so the worker hits `/api/credentials/pg_noetl_k8s` and gets
    /// a deterministic 500, NOT a 404).  It must classify as terminal
    /// `Invalid` so the executor emits `call.error` rather than treating
    /// a permanent decryption failure as retryable and hanging.
    #[tokio::test]
    async fn http_500_decryption_classifies_as_terminal_invalid() {
        let app = Router::new().route(
            "/api/credentials/{alias}",
            get(|Path(_): Path<String>| async {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": "Decryption failed: aead::Error",
                        "status": 500,
                    })),
                )
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let base = format!("http://{}", addr);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("axum serve");
        });
        let client = ControlPlaneClient::new(&base);
        let mut cfg = serde_json::json!({
            "kind": "postgres",
            "auth": "pg_noetl_k8s",
        });

        let err = resolve_auth_alias(&mut cfg, &client, 1)
            .await
            .expect_err("500 must error");
        assert!(
            matches!(err, CredentialResolutionError::Invalid(_)),
            "deterministic 500 must classify as terminal Invalid, got: {err:?}"
        );
        assert!(
            err.is_terminal(),
            "a 500 decryption failure must be terminal so the execution fails cleanly"
        );
        handle.abort();
    }

    /// A genuinely transient HTTP status (503 Service Unavailable)
    /// surfaces from `get_credential` as an `Err` and must classify as
    /// retryable `Transient` — a later attempt may reach a healthy
    /// keychain.  This is the half of the split that must NOT emit a
    /// terminal `call.error` on the first failure.
    #[tokio::test]
    async fn http_503_classifies_as_retryable_transient() {
        let app = Router::new().route(
            "/api/credentials/{alias}",
            get(|Path(_): Path<String>| async { StatusCode::SERVICE_UNAVAILABLE }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let base = format!("http://{}", addr);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("axum serve");
        });
        let client = ControlPlaneClient::new(&base);
        let mut cfg = serde_json::json!({
            "kind": "postgres",
            "auth": "pg_noetl_k8s",
        });

        let err = resolve_auth_alias(&mut cfg, &client, 1)
            .await
            .expect_err("5xx must error");
        assert!(
            matches!(err, CredentialResolutionError::Transient { .. }),
            "5xx must classify as Transient, got: {err:?}"
        );
        assert!(!err.is_terminal());
        handle.abort();
    }

    /// A credential that resolves but carries an unsupported type can
    /// never be applied — it must classify as terminal `Invalid`.
    #[tokio::test]
    async fn unsupported_credential_type_classifies_as_terminal_invalid() {
        let (base, handle) = spawn_keychain(Some(serde_json::json!({
            "id": "1",
            "name": "weird",
            "type": "exotic",
            "data": {},
            "tags": [],
            "description": null,
        })))
        .await;
        let client = ControlPlaneClient::new(&base);
        let mut cfg = serde_json::json!({
            "kind": "http",
            "auth": "weird",
        });

        let err = resolve_auth_alias(&mut cfg, &client, 1)
            .await
            .expect_err("unsupported type must error");
        assert!(
            matches!(err, CredentialResolutionError::Invalid(_)),
            "unsupported type must classify as Invalid, got: {err:?}"
        );
        assert!(
            err.is_terminal(),
            "unsupported credential type must be terminal"
        );
        assert!(
            err.to_string().contains("unsupported type 'exotic'"),
            "Invalid error must name the offending type, got: {err}"
        );
        handle.abort();
    }
}
