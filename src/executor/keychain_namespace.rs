//! Populate the `keychain.<alias>.<field>` template namespace on the
//! worker render context (noetl/ai-meta#151).
//!
//! Playbooks reference resolved keychain entries via a bare
//! `{{ keychain.<alias>.<field> }}` template — e.g.
//! `Authorization: "Bearer {{ keychain.openai_token.api_key }}"` or an
//! HTTP payload field `client_secret: "{{ keychain.auth0_credentials.client_secret }}"`.
//!
//! The orchestrator (the pure drive core) does not populate this
//! namespace: keychain resolution is a boundary call (secret-manager /
//! credential-store fetch) that the deterministic core can't make.  So
//! absent this step the `keychain.*` reference renders to an **empty
//! string** and the downstream HTTP/auth step sends an empty credential
//! → `401`.  This was the root cause of noetl/ai-meta#151.
//!
//! The fix mirrors the `auth:` alias path ([`super::auth_alias`]): the
//! worker resolves every distinct `keychain.<alias>` referenced by the
//! command's templates through the control-plane credentials API — the
//! same server boundary that already resolves `provider:`-backed
//! (`kind: secrets`) and, per the companion server fix, `kind: credential`
//! keychain entries — and injects
//! `variables["keychain"][alias] = <resolved data object>` before the
//! tool renders its templates.  Honours the data-access boundary
//! (`agents/rules/data-access-boundary.md`): the worker never reads the
//! secret store directly; it asks the server.

use std::collections::{BTreeSet, HashMap};

use serde_json::Value;

use crate::client::ControlPlaneClient;

/// `NOETL_KEYCHAIN_STRICT` — fail the command when a referenced
/// `keychain.<alias>` cannot be resolved, instead of leaving it undefined.
///
/// **Default off**, so today's behaviour is unchanged.
///
/// Why the lenient default is a real problem (noetl/ai-meta#151): an
/// unresolved alias leaves the template undefined, which renders to an EMPTY
/// STRING. A step then sends `Authorization: Bearer ` and the third party
/// answers `401 invalid_authorization_header` — a failure that surfaces as a
/// remote auth error, in a different component, with the actual cause (an
/// alias that did not resolve) visible only in a worker WARN the playbook
/// author never sees. That is the precise shape #151 was filed for.
///
/// Why it is nonetheless off by default: turning it on converts a silent
/// degradation into a hard step failure, and a playbook that references an
/// alias it does not strictly need would start failing. That is the right
/// end state, but it is a behaviour change that wants a deliberate flip
/// rather than arriving with an image bump.
pub const STRICT_ENV: &str = "NOETL_KEYCHAIN_STRICT";

/// Is strict keychain resolution enabled?
pub fn strict_enabled() -> bool {
    std::env::var(STRICT_ENV)
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            v == "1" || v == "true" || v == "yes" || v == "on"
        })
        .unwrap_or(false)
}

/// Why a referenced alias did not make it into the namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnresolvedAlias {
    /// The server has no such credential (404).
    NotFound(String),
    /// The fetch itself failed (transport, auth, 5xx).
    FetchFailed(String, String),
}

impl UnresolvedAlias {
    pub fn alias(&self) -> &str {
        match self {
            UnresolvedAlias::NotFound(a) => a,
            UnresolvedAlias::FetchFailed(a, _) => a,
        }
    }
}

/// Render the operator-facing explanation for a set of unresolved aliases.
///
/// Says what will happen (the empty-string render and its downstream 401)
/// rather than only what failed — the message exists because the symptom
/// otherwise appears in a different system.
pub fn unresolved_message(unresolved: &[UnresolvedAlias]) -> String {
    let mut parts: Vec<String> = Vec::new();
    for u in unresolved {
        match u {
            UnresolvedAlias::NotFound(a) => {
                parts.push(format!("`{a}` (no such credential on the server)"))
            }
            UnresolvedAlias::FetchFailed(a, e) => parts.push(format!("`{a}` (fetch failed: {e})")),
        }
    }
    format!(
        "keychain alias(es) referenced by this step could not be resolved: {}. \
         A `{{{{ keychain.<alias>.<field> }}}}` reference to an unresolved alias renders to an \
         EMPTY STRING, so the step would send an empty credential (typically producing a 401 \
         from the remote service, not an error here). Check the alias name and that the \
         execution's credentials grant access to it.",
        parts.join(", ")
    )
}

/// Scan `template_src` for the distinct keychain aliases referenced as
/// `keychain.<alias>` (the second path segment after the `keychain`
/// namespace).  Deliberately dependency-free (no regex crate): the
/// template surface is small and this runs once per command.
///
/// A match requires `keychain` to be a standalone token — the char
/// immediately before it must not be an identifier char — so
/// `mykeychain.foo` (a user variable that merely ends in `keychain`)
/// does not falsely match.
pub fn referenced_aliases(template_src: &str) -> BTreeSet<String> {
    const NEEDLE: &str = "keychain.";
    let bytes = template_src.as_bytes();
    let mut out = BTreeSet::new();
    let mut search_from = 0usize;

    while let Some(rel) = template_src[search_from..].find(NEEDLE) {
        let start = search_from + rel;
        // Advance the cursor past this needle for the next iteration
        // regardless of whether it turns out to be a real reference.
        search_from = start + NEEDLE.len();

        // Guard: `keychain` must be a standalone token — reject when the
        // preceding byte is an identifier char (`foo_keychain.bar`).
        if start > 0 {
            let prev = bytes[start - 1];
            if prev == b'_' || prev.is_ascii_alphanumeric() {
                continue;
            }
        }

        // Read the alias identifier following `keychain.`.
        let alias_start = search_from;
        let mut alias_end = alias_start;
        while alias_end < bytes.len() {
            let c = bytes[alias_end];
            if c == b'_' || c.is_ascii_alphanumeric() {
                alias_end += 1;
            } else {
                break;
            }
        }
        if alias_end > alias_start {
            out.insert(template_src[alias_start..alias_end].to_string());
        }
    }

    out
}

/// Resolve every `keychain.<alias>` referenced by `template_src` and
/// inject the results under the `keychain` namespace of `variables`.
///
/// Best-effort by design: a referenced alias that the server can't
/// resolve (a clean 404 or a fetch error) is logged as a **WARN naming
/// the alias** — never a silent empty token (noetl/ai-meta#151
/// acceptance) — and simply left absent, so `{{ keychain.X.Y is defined }}`
/// guard patterns keep working.  The downstream auth step still fails
/// with a clear `401`/error if the missing credential was load-bearing;
/// the WARN is the breadcrumb that names which alias didn't resolve.
///
/// No-op (and no HTTP calls) when `template_src` references no keychain
/// aliases — the common case.
pub async fn inject_keychain_namespace(
    variables: &mut HashMap<String, Value>,
    template_src: &str,
    client: &ControlPlaneClient,
    execution_id: i64,
) -> Vec<UnresolvedAlias> {
    let aliases = referenced_aliases(template_src);
    if aliases.is_empty() {
        return Vec::new();
    }

    let mut unresolved: Vec<UnresolvedAlias> = Vec::new();
    let mut resolved = serde_json::Map::new();
    let mut resolved_names: Vec<String> = Vec::new();
    for alias in &aliases {
        match client.get_credential(alias, execution_id).await {
            Ok(Some(cred)) => {
                let data: serde_json::Map<String, Value> = cred.data.into_iter().collect();
                resolved.insert(alias.clone(), Value::Object(data));
                resolved_names.push(alias.clone());
            }
            Ok(None) => {
                tracing::warn!(
                    execution_id,
                    alias = %alias,
                    "keychain.namespace: alias referenced by a `keychain.*` template did not \
                     resolve (server 404); leaving it undefined",
                );
                unresolved.push(UnresolvedAlias::NotFound(alias.clone()));
            }
            Err(e) => {
                tracing::warn!(
                    execution_id,
                    alias = %alias,
                    error = %e,
                    "keychain.namespace: fetch for a referenced `keychain.*` alias failed; \
                     leaving it undefined",
                );
                unresolved.push(UnresolvedAlias::FetchFailed(alias.clone(), e.to_string()));
            }
        }
    }

    if resolved.is_empty() {
        return unresolved;
    }

    // Per observability.md Principle 1: log the alias NAMES resolved into
    // the namespace (never values) so operators can trace credential
    // exposure in the worker log alongside the dispatch span.
    tracing::info!(
        execution_id,
        aliases = ?resolved_names,
        "Injected keychain.* namespace for tool template rendering"
    );

    // Merge into any pre-existing `keychain` object rather than clobber
    // it (idempotent if a future server build starts pre-seeding it).
    match variables.get_mut("keychain") {
        Some(Value::Object(existing)) => {
            for (k, v) in resolved {
                existing.entry(k).or_insert(v);
            }
        }
        _ => {
            variables.insert("keychain".to_string(), Value::Object(resolved));
        }
    }

    unresolved
}

/// Resolve the deferred `{{ keychain.<alias>.<field>… }}` templates in a tool
/// config `Value` against a resolved `keychain` namespace, in place.
///
/// The orchestrator DEFERS `keychain.*` templates through the drive (see
/// `noetl-orchestrate-core` `render_value_deferring_keychain`) so the secret is
/// never rendered into the persisted command; this resolves them transiently at
/// dispatch, right before the tool runs.  Dependency-free: it substitutes plain
/// `{{ keychain.<dotted.path> }}` expressions (the shape playbooks use for
/// credential fields) by walking the namespace.  An expression it can't resolve
/// to a scalar (a filter, an unknown path) is left verbatim — never collapsed to
/// empty — so the failure stays visible rather than becoming a silent bad token.
///
/// `keychain_ns` is the object injected under `variables["keychain"]` by
/// [`inject_keychain_namespace`] — `{ alias: { field: value, … }, … }`.
pub fn render_keychain_in_config(config: &mut Value, keychain_ns: &serde_json::Map<String, Value>) {
    match config {
        Value::String(s) => {
            if s.contains("keychain.") {
                *s = substitute_keychain(s, keychain_ns);
            }
        }
        Value::Object(map) => {
            for v in map.values_mut() {
                render_keychain_in_config(v, keychain_ns);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                render_keychain_in_config(v, keychain_ns);
            }
        }
        _ => {}
    }
}

/// Substitute every plain `{{ keychain.<path> }}` expression in `s` with its
/// resolved scalar from `ns`.  Non-keychain `{{ … }}` and unresolvable keychain
/// expressions are left verbatim.
fn substitute_keychain(s: &str, ns: &serde_json::Map<String, Value>) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let after_open = &rest[open + 2..];
        let Some(close) = after_open.find("}}") else {
            // Unbalanced — emit the remainder verbatim and stop.
            out.push_str(&rest[open..]);
            return out;
        };
        let expr = after_open[..close].trim();
        let replaced = resolve_keychain_path(expr, ns);
        match replaced {
            Some(val) => out.push_str(&val),
            None => {
                // Not a resolvable plain keychain path — keep the tag verbatim.
                out.push_str("{{");
                out.push_str(&after_open[..close]);
                out.push_str("}}");
            }
        }
        rest = &after_open[close + 2..];
    }
    out.push_str(rest);
    out
}

/// Resolve a `keychain.<alias>.<field>…` expression to a scalar string from the
/// namespace.  Returns `None` for non-keychain expressions, expressions
/// carrying filters/operators, or paths that don't resolve to a scalar — the
/// caller then leaves the template verbatim.
fn resolve_keychain_path(expr: &str, ns: &serde_json::Map<String, Value>) -> Option<String> {
    // Only accept a bare dotted path (identifiers + dots).  A filter (`|`),
    // operator, or space inside means it isn't a plain path — defer to leaving
    // it verbatim rather than mis-resolving.
    let rest = expr.strip_prefix("keychain.")?;
    if !rest
        .chars()
        .all(|c| c == '_' || c == '.' || c.is_ascii_alphanumeric())
    {
        return None;
    }
    let mut cur: &Value = ns.get(rest.split('.').next()?)?;
    for seg in rest.split('.').skip(1) {
        cur = cur.as_object()?.get(seg)?;
    }
    match cur {
        Value::String(v) => Some(v.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_distinct_aliases() {
        let src = r#"{"headers":{"Authorization":"Bearer {{ keychain.openai_token.api_key }}"},
                      "payload":{"client_secret":"{{ keychain.auth0_credentials.client_secret }}",
                                 "again":"{{ keychain.openai_token.api_key }}"}}"#;
        let aliases = referenced_aliases(src);
        assert_eq!(aliases.len(), 2);
        assert!(aliases.contains("openai_token"));
        assert!(aliases.contains("auth0_credentials"));
    }

    #[test]
    fn ignores_non_token_keychain_substring() {
        // `mykeychain.foo` must NOT match — `keychain` is not standalone.
        let src = "{{ mykeychain.foo }} and {{ keychain.real_alias.k }}";
        let aliases = referenced_aliases(src);
        assert_eq!(aliases.len(), 1);
        assert!(aliases.contains("real_alias"));
    }

    #[test]
    fn no_keychain_reference_is_empty() {
        let src = r#"{"code":"result = {'ok': True}","url":"https://example.com"}"#;
        assert!(referenced_aliases(src).is_empty());
    }

    #[test]
    fn handles_bare_keychain_without_field() {
        // `keychain.alias` with no trailing field still yields the alias.
        let src = "{{ keychain.solo }}";
        let aliases = referenced_aliases(src);
        assert_eq!(aliases.len(), 1);
        assert!(aliases.contains("solo"));
    }

    #[test]
    fn substitutes_plain_keychain_refs_in_config() {
        let ns: serde_json::Map<String, Value> = serde_json::from_value(serde_json::json!({
            "openai_token": {"api_key": "sk-live-XYZ"},
            "amadeus_credentials": {"client_id": "CID", "client_secret": "CSEC"},
        }))
        .unwrap();
        let mut cfg = serde_json::json!({
            "headers": {"Authorization": "Bearer {{ keychain.openai_token.api_key }}"},
            "form": {
                "client_id": "{{ keychain.amadeus_credentials.client_id }}",
                "client_secret": "{{ keychain.amadeus_credentials.client_secret }}",
                "grant": "client_credentials"
            }
        });
        render_keychain_in_config(&mut cfg, &ns);
        assert_eq!(cfg["headers"]["Authorization"], "Bearer sk-live-XYZ");
        assert_eq!(cfg["form"]["client_id"], "CID");
        assert_eq!(cfg["form"]["client_secret"], "CSEC");
        assert_eq!(cfg["form"]["grant"], "client_credentials");
    }

    #[test]
    fn leaves_unresolvable_keychain_ref_verbatim() {
        let ns: serde_json::Map<String, Value> =
            serde_json::from_value(serde_json::json!({"present": {"k": "v"}})).unwrap();
        // Unknown alias, and a filtered expression — both left verbatim, not emptied.
        let mut cfg = serde_json::json!({
            "a": "Bearer {{ keychain.missing.k }}",
            "b": "{{ keychain.present.k | upper }}",
            "c": "{{ keychain.present.k }}"
        });
        render_keychain_in_config(&mut cfg, &ns);
        assert_eq!(cfg["a"], "Bearer {{ keychain.missing.k }}");
        assert_eq!(cfg["b"], "{{ keychain.present.k | upper }}");
        assert_eq!(cfg["c"], "v");
    }

    #[tokio::test]
    async fn no_reference_injects_nothing_and_makes_no_calls() {
        // A bogus server URL would error on any HTTP call; the early
        // return means we never make one.
        let client = ControlPlaneClient::new("http://127.0.0.1:1/unused");
        let mut vars: HashMap<String, Value> = HashMap::new();
        inject_keychain_namespace(&mut vars, "no templates here", &client, 42).await;
        assert!(!vars.contains_key("keychain"));
    }
}

#[cfg(test)]
mod strict_mode_tests {
    use super::*;

    /// noetl/ai-meta#151's third acceptance criterion. An unresolved alias
    /// renders to an EMPTY STRING, so the step sends an empty credential and the
    /// failure surfaces as a 401 from the remote service — in a different
    /// component, with the real cause only in a worker WARN the playbook author
    /// never reads. The message has to say that, not just "not found".
    #[test]
    fn the_message_explains_the_empty_render_not_just_the_miss() {
        let msg = unresolved_message(&[UnresolvedAlias::NotFound("duffel_token".into())]);
        assert!(msg.contains("duffel_token"), "must name the alias");
        assert!(
            msg.contains("EMPTY STRING"),
            "must say the reference renders empty — that is the non-obvious part"
        );
        assert!(
            msg.contains("401"),
            "must connect it to the symptom the author will actually see"
        );
    }

    /// A fetch failure and a 404 are different operator actions — a missing
    /// credential is an authoring/provisioning fix, a fetch failure is an
    /// availability problem. The message must not collapse them.
    #[test]
    fn fetch_failure_and_not_found_read_differently() {
        let nf = unresolved_message(&[UnresolvedAlias::NotFound("a".into())]);
        let ff = unresolved_message(&[UnresolvedAlias::FetchFailed("a".into(), "503".into())]);
        assert!(nf.contains("no such credential"));
        assert!(ff.contains("fetch failed") && ff.contains("503"));
        assert_ne!(nf, ff);
    }

    /// Every unresolved alias is named — reporting only the first would hide
    /// work from an operator fixing them in one pass.
    #[test]
    fn all_unresolved_aliases_are_named() {
        let msg = unresolved_message(&[
            UnresolvedAlias::NotFound("alpha".into()),
            UnresolvedAlias::FetchFailed("beta".into(), "timeout".into()),
        ]);
        assert!(msg.contains("alpha") && msg.contains("beta"));
    }

    /// The flag must default OFF, or an image bump silently converts a
    /// degradation into a hard failure across every playbook that references a
    /// keychain alias.
    #[test]
    fn strict_is_off_by_default_and_accepts_the_usual_spellings() {
        // Not set -> off. (Serialised via a unique var name per assertion to
        // avoid cross-test env races.)
        std::env::remove_var(STRICT_ENV);
        assert!(!strict_enabled(), "default MUST be off");

        for on in ["1", "true", "TRUE", "yes", "on", " On "] {
            std::env::set_var(STRICT_ENV, on);
            assert!(strict_enabled(), "{on:?} should enable strict");
        }
        for off in ["0", "false", "no", "off", ""] {
            std::env::set_var(STRICT_ENV, off);
            assert!(!strict_enabled(), "{off:?} should NOT enable strict");
        }
        std::env::remove_var(STRICT_ENV);
    }

    /// `alias()` is what the ERROR log reports; it must work for both variants.
    #[test]
    fn alias_accessor_covers_both_variants() {
        assert_eq!(UnresolvedAlias::NotFound("x".into()).alias(), "x");
        assert_eq!(
            UnresolvedAlias::FetchFailed("y".into(), "e".into()).alias(),
            "y"
        );
    }
}
