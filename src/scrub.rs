//! Producer-side credential scrubbing for `call.done` payloads.
//!
//! Per [`agents/rules/execution-model.md`][rule]:
//!
//! > HTTP responses that surface execution state, variables, events,
//! > or result payloads must mask resolved credential values before
//! > they leave the server.
//!
//! The Python server already calls
//! [`noetl.core.credential_refs.producer_scrub_payload`][py] on every
//! `PUT /api/result/{execution_id}` body before durable storage, so a
//! worker that doesn't scrub still ends up with redacted values in
//! the durable result store.  This module shortens the **wire-transit
//! window**: a third party watching the worker → server HTTP traffic
//! (mitmproxy on the cluster network, malicious sidecar) sees redacted
//! values too, not just the persisted ones.
//!
//! It also covers the inline `result.context` path and the shared-
//! memory cache stage — neither of those rides through Python's
//! server-side scrubber, so without this module a tool that
//! accidentally surfaces a credential in its stdout / JSON output
//! would leak it into both the event log AND the colocated consumer's
//! shm read.
//!
//! ## What gets redacted
//!
//! 1. **Values whose keys match a known-sensitive name.**  Mirrors
//!    Python's `SENSITIVE_KEYS` set in `noetl/core/sanitize.py`.
//!    Comparison is case-insensitive on the lower-snake form
//!    (`Authorization` → `authorization`, `api-key` → `api_key`).
//! 2. **Values whose string content matches a known-secret pattern.**
//!    Bearer tokens, Basic-auth headers, JWTs, private-key blocks,
//!    common API-key prefixes (`sk-`, `ghp_`, etc.).
//!
//! Both checks recurse into nested objects + arrays.  The replacement
//! string is [`REDACTED`].
//!
//! ## What does NOT get redacted
//!
//! - **Keychain-manifest-driven values** (Python's `_keychain_manifest`
//!   path).  The Rust worker doesn't currently propagate the manifest
//!   into `Command.input.render_context`; once it does, we'll extend
//!   the scrubber to honor it.  Until then the Python server's
//!   manifest-driven scrub at the `PUT /api/result` boundary catches
//!   these for the durable path.
//! - **Arbitrary "long alphanumeric string" values** that COULD be
//!   secrets but also could be execution ids, document ids, etc.
//!   Python ships a separate `_is_response_secret_value` for the
//!   response boundary; we don't run that broader scan here because
//!   it's prone to false positives on production execution ids
//!   (`noetl_event_id` is also a long string).
//!
//! ## Why scrub in place
//!
//! `scrub_in_place` mutates a `serde_json::Value` rather than allocating
//! a fresh tree.  Tool outputs can be > 100 KB; cloning to scrub would
//! double the over-budget memory pressure right where we want it
//! tightest.  Callers that need to keep the original around clone
//! before calling.
//!
//! [rule]: https://github.com/noetl/ai-meta/blob/main/agents/rules/execution-model.md
//! [py]: https://github.com/noetl/noetl/blob/main/noetl/core/credential_refs.py

/// Replacement string for redacted values.  Matches Python's
/// `noetl.core.sanitize.REDACTED` so a credential leak that gets
/// scrubbed on one side and surfaces on the other side via replay
/// produces the same observable string in the event log.
pub const REDACTED: &str = "[REDACTED]";

/// Lowercase + underscored key names that ALWAYS get their value
/// redacted.  Mirrors Python's `noetl/core/sanitize.py` `SENSITIVE_KEYS`
/// set — keep in sync when adding new entries.
///
/// The matching strategy: lowercase the input key, replace `-` with
/// `_`, then check both exact match and substring match (so `db_password`
/// matches against `password` even though the full key isn't in the
/// set).  This is Python's `_is_sensitive_key` shape.
const SENSITIVE_KEY_TOKENS: &[&str] = &[
    // Authentication
    "password",
    "passwd",
    "pwd",
    "secret",
    "token",
    "bearer",
    "api_key",
    "apikey",
    "access_token",
    "refresh_token",
    "auth_token",
    "authorization",
    "auth",
    "credential",
    "credentials",
    "private_key",
    "privatekey",
    "secret_key",
    "secretkey",
    "client_secret",
    "clientsecret",
    // Database
    "connection_string",
    "connectionstring",
    "db_password",
    "database_password",
    // Cloud
    "aws_secret",
    "gcp_key",
    "azure_key",
    // SSH/TLS
    "ssh_key",
    "sshkey",
    "passphrase",
    "pem",
    "cert",
    "certificate",
    // OAuth
    "oauth_token",
    "id_token",
    // Encryption
    "encryption_key",
    "decrypt_key",
    "master_key",
    // Snowflake specific
    "snowflake_password",
    "snowflake_token",
    "private_key_passphrase",
    // Response-boundary additions (Python's RESPONSE_SENSITIVE_KEYS).
    "keychain",
    "api_secret",
    "apisecret",
    "auth0_token",
    "auth0_id_token",
    "auth0_refresh_token",
    "idtoken",
    "oauth",
    "jwt",
];

/// Return `true` if the key name matches any sensitive token (exact
/// or substring).  Case- and separator-insensitive.
pub fn is_sensitive_key(key: &str) -> bool {
    let normalised = key.to_ascii_lowercase().replace('-', "_");
    for token in SENSITIVE_KEY_TOKENS {
        if normalised == *token || normalised.contains(token) {
            return true;
        }
    }
    false
}

/// Return `true` if the string content matches a known-secret pattern
/// regardless of where it lives in the JSON tree.  Catches credentials
/// that leak through a non-sensitive key name (`stdout: "Bearer ..."`,
/// `error: "...private key..."`, etc.).
///
/// Heuristics — keep them tight to avoid false positives on execution
/// ids, document ids, hashes:
///
/// - `Bearer <anything>` (auth header content).
/// - `Basic <base64>` (basic-auth header content).
/// - `eyJ<base64>.eyJ<base64>.<base64>` (compact-JWT format).
/// - `-----BEGIN ... PRIVATE KEY-----` (PEM block).
/// - `sk-<chars>` / `sk-ant-<chars>` (OpenAI / Anthropic API keys).
/// - `AIza<chars>` (Google API keys).
/// - `ya29.<chars>` (Google OAuth tokens).
/// - `ghp_<chars>` / `ghs_<chars>` / `gho_<chars>` / `ghu_<chars>` /
///   `ghr_<chars>` / `github_pat_<chars>` (GitHub tokens).
/// - `xoxb-`, `xoxa-`, `xoxp-`, `xoxr-`, `xoxs-` (Slack tokens).
pub fn looks_like_secret_value(s: &str) -> bool {
    if s.len() < 8 {
        // Tokens shorter than 8 chars are too noisy to scrub (an
        // execution id would trip them).
        return false;
    }

    if s.starts_with("-----BEGIN ") && s.contains("PRIVATE KEY-----") {
        return true;
    }

    // Bearer / Basic — case-insensitive prefix check.
    let lower = s.to_ascii_lowercase();
    if (lower.starts_with("bearer ") || lower.starts_with("basic "))
        && s.split_whitespace().count() >= 2
    {
        return true;
    }

    // Compact JWT — three base64url segments separated by dots,
    // first segment starts with `eyJ` (the base64 for `{"`).
    if s.starts_with("eyJ") {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() == 3 && parts[1].starts_with("eyJ") && parts.iter().all(|p| !p.is_empty()) {
            return true;
        }
    }

    // Vendor-prefixed tokens.  Match anywhere in the string —
    // tokens often surface embedded in error messages or shell
    // stdout (`"failed to auth with sk-ant-..."`), not just as
    // standalone values.  Python's `SECRET_VALUE_PATTERNS` apply
    // the same "anywhere with separator" check.
    static VENDOR_PREFIXES: &[&str] = &[
        "sk-ant-",
        "sk-",
        "AIza",
        "ya29.",
        "ghp_",
        "ghs_",
        "gho_",
        "ghu_",
        "ghr_",
        "github_pat_",
        "xoxb-",
        "xoxa-",
        "xoxp-",
        "xoxr-",
        "xoxs-",
    ];
    for prefix in VENDOR_PREFIXES {
        let mut search_from = 0;
        while let Some(rel) = s[search_from..].find(prefix) {
            let abs = search_from + rel;
            // Require the prefix to be at the start of the string
            // OR preceded by a separator — guards against the
            // sub-prefix landing inside a longer harmless token.
            let at_boundary = abs == 0
                || matches!(
                    s.as_bytes()[abs - 1],
                    b' ' | b'\t' | b'\n' | b'\r' | b'"' | b'\'' | b':' | b'=' | b','
                );
            if at_boundary {
                let after = &s[abs + prefix.len()..];
                // Take the contiguous run of token-y chars after
                // the prefix; if it's ≥ 12 chars it's almost
                // certainly a real credential.
                let run: String = after
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
                    .collect();
                if run.len() >= 12 {
                    return true;
                }
            }
            search_from = abs + prefix.len();
            if search_from >= s.len() {
                break;
            }
        }
    }

    false
}

/// Walk a JSON value in place and redact:
///
/// - Any value whose key matches [`is_sensitive_key`].
/// - Any string value whose content matches [`looks_like_secret_value`].
///
/// Recurses into nested objects + arrays.  Replaces the matched value
/// with [`REDACTED`] (a string) — same shape Python ships.
pub fn scrub_in_place(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if is_sensitive_key(key) {
                    // Replace the whole subtree with the REDACTED
                    // marker — mirrors Python's behaviour on
                    // sensitive keys (the value is opaque from the
                    // consumer's perspective).
                    *child = serde_json::Value::String(REDACTED.to_string());
                } else {
                    scrub_in_place(child);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items.iter_mut() {
                scrub_in_place(item);
            }
        }
        serde_json::Value::String(s) => {
            if looks_like_secret_value(s) {
                *s = REDACTED.to_string();
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}

/// Convenience wrapper: clone the input, scrub the clone, return it.
/// Used when the caller needs to keep the original around (logging,
/// metrics, etc.) AND still emit a scrubbed copy.  Prefer
/// [`scrub_in_place`] when the original is no longer needed.
pub fn scrub_cloned(value: &serde_json::Value) -> serde_json::Value {
    let mut clone = value.clone();
    scrub_in_place(&mut clone);
    clone
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- is_sensitive_key ---

    #[test]
    fn sensitive_keys_match_case_and_separator_insensitively() {
        for key in [
            "password",
            "Password",
            "PASSWORD",
            "api_key",
            "api-key",
            "API-Key",
            "Authorization",
            "authorization",
            "client_secret",
            "Client-Secret",
            "DB_PASSWORD",
        ] {
            assert!(
                is_sensitive_key(key),
                "key `{}` must be flagged as sensitive",
                key
            );
        }
    }

    #[test]
    fn sensitive_keys_match_substrings() {
        // `db_password` includes `password`; `oauth_id_token` includes
        // `oauth` + `id_token`; etc.
        for key in [
            "db_password",
            "user_password",
            "snowflake_token",
            "header_authorization",
            "step_credentials",
        ] {
            assert!(is_sensitive_key(key), "substring key `{}`", key);
        }
    }

    #[test]
    fn non_sensitive_keys_are_not_flagged() {
        for key in [
            "stdout",
            "exit_code",
            "duration_ms",
            "user_name",
            "row_count",
            "rows",
            "columns",
            "step",
            "node_name",
            "execution_id",
            "event_type",
        ] {
            assert!(!is_sensitive_key(key), "key `{}` must NOT be flagged", key);
        }
    }

    // --- looks_like_secret_value ---

    #[test]
    fn looks_like_secret_recognises_bearer_and_basic() {
        assert!(looks_like_secret_value("Bearer abc123token"));
        assert!(looks_like_secret_value("bearer abc123token"));
        assert!(looks_like_secret_value("Basic dXNlcjpwYXNzd29yZA=="));
        // Bare "Bearer" with nothing after must NOT match (no token).
        assert!(!looks_like_secret_value("Bearer"));
    }

    #[test]
    fn looks_like_secret_recognises_jwt() {
        let jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3In0.abcd1234efgh";
        assert!(looks_like_secret_value(jwt));
        // Missing one segment → not a JWT.
        let two_parts = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3In0";
        assert!(!looks_like_secret_value(two_parts));
    }

    #[test]
    fn looks_like_secret_recognises_private_key_block() {
        let pem = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA...\n-----END";
        assert!(looks_like_secret_value(pem));
        let openssh = "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAA...\n";
        assert!(looks_like_secret_value(openssh));
    }

    #[test]
    fn looks_like_secret_recognises_vendor_prefixes() {
        // OpenAI / Anthropic-style API keys.
        assert!(looks_like_secret_value("sk-1234567890ab"));
        assert!(looks_like_secret_value("sk-ant-1234567890ab"));
        // Google API key.
        assert!(looks_like_secret_value(
            "AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI"
        ));
        // GitHub PAT.
        assert!(looks_like_secret_value(
            "ghp_16C7e42F292c6912E7710c838347Ae178B4a"
        ));
        // Slack bot token.
        assert!(looks_like_secret_value("xoxb-12345-67890-abcdefghijklmnop"));
    }

    #[test]
    fn looks_like_secret_does_not_false_positive_on_execution_ids() {
        // Execution / event / command ids are bigint snowflakes;
        // they can be 18+ digits and would trip a naive "long
        // alphanumeric" rule.  We intentionally don't run that rule.
        assert!(!looks_like_secret_value("639085882888160059"));
        assert!(!looks_like_secret_value(
            "noetl_event_id_639085882888160059"
        ));
        // Document ids, request ids, names.
        assert!(!looks_like_secret_value("user-12345"));
        assert!(!looks_like_secret_value("hello world"));
        assert!(!looks_like_secret_value("short"));
        // Random base64 that's not a JWT (no leading `eyJ`).
        assert!(!looks_like_secret_value(
            "YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXo="
        ));
    }

    // --- scrub_in_place ---

    #[test]
    fn scrub_in_place_redacts_sensitive_keys_recursively() {
        let mut value = serde_json::json!({
            "user": "alice",
            "password": "hunter2",
            "headers": {
                "Authorization": "Bearer xyz",
                "Accept": "application/json"
            },
            "nested": {
                "credentials": {
                    "api_key": "AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI",
                    "user": "bob"
                }
            }
        });
        scrub_in_place(&mut value);
        assert_eq!(value["user"], "alice");
        assert_eq!(value["password"], REDACTED);
        assert_eq!(value["headers"]["Authorization"], REDACTED);
        assert_eq!(value["headers"]["Accept"], "application/json");
        // `credentials` key is itself sensitive → whole subtree
        // redacted in one shot, not recursed into.
        assert_eq!(value["nested"]["credentials"], REDACTED);
    }

    #[test]
    fn scrub_in_place_recurses_through_non_sensitive_dict_keys() {
        // `nested.api_keys.primary` — `nested` is not sensitive
        // (recurse), `api_keys` IS sensitive (matches `api_key`
        // substring) so the whole subtree gets redacted.
        let mut value = serde_json::json!({
            "nested": {
                "api_keys": {
                    "primary": "AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI",
                    "secondary": "another-key"
                },
                "user": "alice"
            }
        });
        scrub_in_place(&mut value);
        assert_eq!(value["nested"]["api_keys"], REDACTED);
        assert_eq!(value["nested"]["user"], "alice");
    }

    #[test]
    fn scrub_in_place_redacts_secret_values_in_non_sensitive_keys() {
        let mut value = serde_json::json!({
            "stdout": "Bearer leaked-token-12345",
            "exit_code": 0,
            "error": "could not authenticate with sk-ant-1234567890abcdef"
        });
        scrub_in_place(&mut value);
        assert_eq!(value["stdout"], REDACTED);
        assert_eq!(value["exit_code"], 0);
        // The whole `error` string is replaced — we don't do partial
        // redaction of substrings.  Tighter than necessary but
        // doesn't leak the embedded token.
        assert_eq!(value["error"], REDACTED);
    }

    #[test]
    fn scrub_in_place_recurses_into_arrays() {
        let mut value = serde_json::json!({
            "rows": [
                { "user": "alice", "password": "hunter2" },
                { "user": "bob", "password": "qwerty" }
            ]
        });
        scrub_in_place(&mut value);
        assert_eq!(value["rows"][0]["user"], "alice");
        assert_eq!(value["rows"][0]["password"], REDACTED);
        assert_eq!(value["rows"][1]["password"], REDACTED);
    }

    #[test]
    fn scrub_in_place_preserves_non_string_non_sensitive_values() {
        let mut value = serde_json::json!({
            "row_count": 1234,
            "active": true,
            "duration_ms": 42,
            "ratio": 0.95,
            "null_field": null,
            "tags": ["a", "b", "c"]
        });
        let original = value.clone();
        scrub_in_place(&mut value);
        assert_eq!(value, original);
    }

    #[test]
    fn scrub_cloned_does_not_mutate_input() {
        let original = serde_json::json!({
            "password": "hunter2",
            "user": "alice"
        });
        let scrubbed = scrub_cloned(&original);
        assert_eq!(original["password"], "hunter2");
        assert_eq!(scrubbed["password"], REDACTED);
    }

    // --- DuckDB-shape rowset round-trip ---

    /// Tool outputs that contain credential-bearing rows (e.g. a
    /// SELECT against a users table) must have those rows redacted
    /// before they ride into shm / durable storage.  Locks in the
    /// behaviour for the tabular path that R-2.2 ships through.
    #[test]
    fn scrub_in_place_handles_tabular_rowset() {
        let mut value = serde_json::json!({
            "status": "Success",
            "data": {
                "columns": ["user", "password", "role"],
                "rows": [
                    {"user": "alice", "password": "hunter2", "role": "admin"},
                    {"user": "bob", "password": "qwerty", "role": "viewer"}
                ],
                "row_count": 2
            }
        });
        scrub_in_place(&mut value);
        let row0 = &value["data"]["rows"][0];
        assert_eq!(row0["user"], "alice");
        assert_eq!(row0["password"], REDACTED);
        assert_eq!(row0["role"], "admin");
    }
}
