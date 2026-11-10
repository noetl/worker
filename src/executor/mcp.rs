//! `kind: mcp` on the worker: catalog-resolved servers, auth headers, and a
//! per-step provider-error policy (RFC `interop-topology-and-ai-mcp-playbooks.md`
//! Phase 2, noetl/ai-meta#539).
//!
//! Since noetl-tools 4.5 (ai-meta#330 lifted the `=4.0.1` hold) the MCP client,
//! `auth:` and `headers:` live in noetl-tools' `McpTool`, shared with the CLI local
//! runtime and daemon. This file keeps what only a cluster worker can do.
//!
//! # Three additions, each inert unless a step asks for it
//!
//! 1. **`server: mcp/<path>`** ([`resolve_catalog_server`]). When an `mcp` step names
//!    no endpoint and its `server` is a catalog path under `mcp/`, the worker fetches
//!    that catalog entry, requires it to be a `kind: Mcp` resource, and injects its
//!    `spec.url` as `endpoint`. A step with an explicit endpoint, or a plain server
//!    name (`server: kubernetes` → `NOETL_MCP_KUBERNETES_ENDPOINT`), is untouched.
//!    Resolution runs at dispatch, on the worker, so it covers commands the server
//!    built and commands the off-server orchestrate drive built.
//! 2. **Auth headers**. `auth:` takes the shapes every other worker tool takes: a
//!    keychain alias string (resolved before dispatch by `auth_alias`, exactly as
//!    for `http`), or an inline `{type: bearer|basic|api_key|gcp_adc, ...}` block.
//!    `headers:` adds literal headers, and can carry `{{ keychain.<alias>.<field> }}`
//!    templates. Both are sent by noetl-tools' `McpTool` ([`WorkerMcpTool`] only
//!    delegates).
//! 3. **`on_provider_error: fail | continue`** ([`take_provider_error_policy`]). A
//!    per-step override of the process-wide `NOETL_PROVIDER_ERROR_TERMINATES`
//!    (default off, ai-meta#246). With `fail`, a result that carries the MCP
//!    `isError: true` marker, or an `mcp` step whose transport failed (`status:
//!    "error"`, which noetl-tools returns as a *successful* `ToolResult`), fails the
//!    step instead of completing it. The key is removed from the config before the
//!    tool sees it.
//!
//! # Secrets
//!
//! Header values and resolved credentials are never logged and never echoed into the
//! result. The result names the header *keys* the call sent (`sent_headers`), so an
//! operator can tell an unauthenticated call from an authenticated one in the event
//! log without the event log holding the credential.

use async_trait::async_trait;
use noetl_tools::registry::{Tool, ToolConfig};
use noetl_tools::tools::McpTool;
use noetl_tools::{ExecutionContext, ToolError, ToolResult};
use serde_json::Value;

/// The catalog path prefix that marks `server:` as a catalog reference.
pub(crate) const CATALOG_PREFIX: &str = "mcp/";

/// The per-step policy key. Removed from the config before dispatch.
pub(crate) const POLICY_KEY: &str = "on_provider_error";


// ===========================================================================
// 1. server: mcp/<path>
// ===========================================================================

/// Why a `server: mcp/<path>` reference could not be resolved.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CatalogResolveError {
    /// The catalog has no entry at that path. Terminal.
    #[error("mcp server '{path}' is not registered in the catalog")]
    NotFound { path: String },
    /// The entry exists but is not a usable `kind: Mcp` resource. Terminal.
    #[error("mcp server '{path}': {reason}")]
    Invalid { path: String, reason: String },
    /// The catalog could not be reached. Retryable.
    #[error("mcp server '{path}': catalog lookup failed: {source}")]
    Transport {
        path: String,
        #[source]
        source: anyhow::Error,
    },
}

impl CatalogResolveError {
    /// Terminal failures do not get better on a retry.
    pub(crate) fn is_terminal(&self) -> bool {
        !matches!(self, CatalogResolveError::Transport { .. })
    }
}

/// The catalog path this config asks to resolve, if any.
///
/// `None` unless the config is an `mcp` tool, names no endpoint (under any of the
/// aliases noetl-tools accepts), and its `server` (or `name`) starts with `mcp/`.
pub(crate) fn catalog_path(cfg: &Value) -> Option<String> {
    let map = cfg.as_object()?;
    if map.get("kind").and_then(Value::as_str) != Some("mcp") {
        return None;
    }
    let has_endpoint = ["endpoint", "url", "server_url", "base_url"]
        .iter()
        .any(|k| {
            map.get(*k)
                .and_then(Value::as_str)
                .is_some_and(|s| !s.trim().is_empty())
        });
    if has_endpoint {
        return None;
    }
    let server = map
        .get("server")
        .or_else(|| map.get("name"))
        .and_then(Value::as_str)?
        .trim();
    let rest = server.strip_prefix(CATALOG_PREFIX)?;
    if rest.is_empty() {
        return None;
    }
    Some(server.to_string())
}

/// Read the endpoint out of a `kind: Mcp` catalog document.
///
/// The URL is `spec.url` (the shape the ops templates and the GUI wizard write),
/// with `spec.endpoint` accepted as an alias. It must be `http` or `https`.
pub(crate) fn endpoint_from_resource(
    path: &str,
    content: &str,
) -> Result<String, CatalogResolveError> {
    let invalid = |reason: String| CatalogResolveError::Invalid {
        path: path.to_string(),
        reason,
    };
    let doc: Value = serde_yaml::from_str(content)
        .map_err(|e| invalid(format!("catalog content is not YAML: {e}")))?;
    let kind = doc.get("kind").and_then(Value::as_str).unwrap_or("");
    if !kind.eq_ignore_ascii_case("mcp") {
        return Err(invalid(format!(
            "catalog entry is kind '{kind}', not 'Mcp'"
        )));
    }
    let spec = doc.get("spec");
    let url = spec
        .and_then(|s| s.get("url").or_else(|| s.get("endpoint")))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| invalid("kind: Mcp resource has no spec.url".to_string()))?;
    let parsed = reqwest::Url::parse(url)
        .map_err(|e| invalid(format!("spec.url '{url}' is not a URL: {e}")))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(invalid(format!(
            "spec.url scheme '{}' is not http or https",
            parsed.scheme()
        )));
    }
    Ok(url.trim_end_matches('/').to_string())
}

/// Resolve `server: mcp/<path>` into an `endpoint`, in place.
///
/// `fetch` returns the catalog entry's content, `Ok(None)` for a clean 404. Returns
/// the path that was resolved, or `None` when the config did not ask for it (the
/// default, which makes no call at all).
pub(crate) async fn resolve_catalog_server<F, Fut>(
    cfg: &mut Value,
    fetch: F,
) -> Result<Option<(String, String)>, CatalogResolveError>
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Option<String>>>,
{
    let Some(path) = catalog_path(cfg) else {
        return Ok(None);
    };
    let content = fetch(path.clone())
        .await
        .map_err(|source| CatalogResolveError::Transport {
            path: path.clone(),
            source,
        })?
        .ok_or_else(|| CatalogResolveError::NotFound { path: path.clone() })?;
    let endpoint = endpoint_from_resource(&path, &content)?;
    if let Some(map) = cfg.as_object_mut() {
        map.insert("endpoint".to_string(), Value::String(endpoint.clone()));
    }
    Ok(Some((path, endpoint)))
}

// ===========================================================================
// 3. on_provider_error
// ===========================================================================

/// A step's explicit provider-error policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProviderErrorPolicy {
    /// Fail the step on a provider error.
    Fail,
    /// Complete the step; the error stays in the result for the playbook to read.
    Continue,
}

/// Remove `on_provider_error` from a tool config and parse it.
///
/// `Ok(None)` when the step does not set it (the process-wide flag decides). An
/// unrecognised value is an error: a typo must not silently mean "continue".
pub(crate) fn take_provider_error_policy(
    cfg: &mut Value,
) -> Result<Option<ProviderErrorPolicy>, String> {
    let Some(map) = cfg.as_object_mut() else {
        return Ok(None);
    };
    match map.remove(POLICY_KEY) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => match s.trim() {
            "fail" => Ok(Some(ProviderErrorPolicy::Fail)),
            "continue" => Ok(Some(ProviderErrorPolicy::Continue)),
            other => Err(format!(
                "{POLICY_KEY} must be 'fail' or 'continue', got '{other}'"
            )),
        },
        Some(other) => Err(format!(
            "{POLICY_KEY} must be 'fail' or 'continue', got {other}"
        )),
    }
}

/// Does a provider error terminate this step? The step's policy wins; without one,
/// the process-wide default decides.
pub(crate) fn terminates(policy: Option<ProviderErrorPolicy>, process_default: bool) -> bool {
    match policy {
        Some(ProviderErrorPolicy::Fail) => true,
        Some(ProviderErrorPolicy::Continue) => false,
        None => process_default,
    }
}

/// An `mcp` step whose call failed at the transport, JSON-RPC or envelope layer.
///
/// noetl-tools reports those as a successful `ToolResult` whose data is `{status:
/// "error", error: "..."}` (mirroring the Python tool), so `isError` never appears.
/// Only consulted when the step opted in with `on_provider_error: fail`: the
/// `status: "error"` spelling is too common in business payloads to count globally.
pub(crate) fn mcp_transport_error(kind: &str, data: Option<&Value>) -> Option<String> {
    if kind != "mcp" {
        return None;
    }
    let data = data?;
    if data.get("status").and_then(Value::as_str) != Some("error") {
        return None;
    }
    let msg = data
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("mcp call failed (no error text)");
    Some(msg.chars().take(300).collect())
}

// ===========================================================================
// 2. WorkerMcpTool
// ===========================================================================

/// The worker's `mcp` tool: noetl-tools' `McpTool` (which sends `auth:` / `headers:`).
pub struct WorkerMcpTool {
    inner: McpTool,
}

impl WorkerMcpTool {
    pub fn new() -> Self {
        Self { inner: McpTool::new() }
    }
}

impl Default for WorkerMcpTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for WorkerMcpTool {
    fn name(&self) -> &'static str {
        "mcp"
    }

    fn side_effecting(&self) -> bool {
        self.inner.side_effecting()
    }

    /// noetl-tools' `McpTool` sends `auth:` and `headers:` itself since 4.5
    /// (noetl/tools#118), with the same reserved-header, sensitive-value and
    /// `sent_headers` rules this file used to implement. One implementation
    /// now serves the worker, the CLI local runtime and the daemon. What stays
    /// here is what only a cluster worker can do: catalog `server: mcp/<path>`
    /// resolution and the process-wide provider-error default, both applied
    /// before dispatch.
    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        self.inner.execute(config, ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use axum::http::HeaderMap as AxHeaders;
    use axum::routing::post;
    use axum::{Json, Router};
    use std::sync::{Arc, Mutex};

    // ---- catalog resolution ------------------------------------------------

    fn mcp(cfg: Value) -> Value {
        let mut c = cfg;
        c["kind"] = json!("mcp");
        c
    }

    #[test]
    fn only_an_mcp_step_with_a_catalog_server_and_no_endpoint_resolves() {
        assert_eq!(
            catalog_path(&mcp(json!({"server": "mcp/noetl-cluster"}))).as_deref(),
            Some("mcp/noetl-cluster")
        );
        assert_eq!(
            catalog_path(&mcp(json!({"name": "mcp/a/b"}))).as_deref(),
            Some("mcp/a/b")
        );
        // explicit endpoint (any alias) wins
        for k in ["endpoint", "url", "server_url", "base_url"] {
            let mut c = mcp(json!({"server": "mcp/x"}));
            c[k] = json!("http://h/mcp");
            assert_eq!(catalog_path(&c), None, "{k}");
        }
        // a blank endpoint does not count
        assert!(catalog_path(&mcp(json!({"server": "mcp/x", "endpoint": "  "}))).is_some());
        // a plain server name keeps the env-var path
        assert_eq!(catalog_path(&mcp(json!({"server": "kubernetes"}))), None);
        assert_eq!(catalog_path(&mcp(json!({"server": "mcp/"}))), None);
        // other kinds are never touched
        assert_eq!(
            catalog_path(&json!({"kind": "http", "server": "mcp/x"})),
            None
        );
    }

    const RESOURCE: &str = "apiVersion: noetl.io/v2\nkind: Mcp\nmetadata:\n  name: noetl-cluster\n  path: mcp/noetl-cluster\nspec:\n  url: http://noetl-server-rust.noetl.svc.cluster.local:8082/ehgraph/noetl-cluster/mcp/\n";

    #[test]
    fn a_kind_mcp_resource_yields_its_spec_url() {
        assert_eq!(
            endpoint_from_resource("mcp/noetl-cluster", RESOURCE).unwrap(),
            "http://noetl-server-rust.noetl.svc.cluster.local:8082/ehgraph/noetl-cluster/mcp"
        );
        let alias = "kind: mcp\nspec:\n  endpoint: https://h/mcp\n";
        assert_eq!(
            endpoint_from_resource("mcp/a", alias).unwrap(),
            "https://h/mcp"
        );
    }

    #[test]
    fn a_resource_that_is_not_a_usable_mcp_server_is_terminal_and_says_why() {
        for (content, needle) in [
            ("kind: Playbook\nspec:\n  url: http://h\n", "not 'Mcp'"),
            ("kind: Mcp\nspec: {}\n", "no spec.url"),
            ("kind: Mcp\nspec:\n  url: file:///etc/passwd\n", "not http"),
            ("kind: Mcp\nspec:\n  url: not a url\n", "not a URL"),
            (": : :", "not YAML"),
        ] {
            let e = endpoint_from_resource("mcp/a", content).unwrap_err();
            assert!(e.is_terminal());
            assert!(e.to_string().contains(needle), "{content:?} -> {e}");
        }
    }

    #[tokio::test]
    async fn resolution_injects_the_endpoint_and_classifies_failures() {
        let mut cfg = mcp(json!({"server": "mcp/noetl-cluster", "tool": "graph.instance.info"}));
        let got = resolve_catalog_server(&mut cfg, |p| async move {
            assert_eq!(p, "mcp/noetl-cluster");
            Ok(Some(RESOURCE.to_string()))
        })
        .await
        .unwrap();
        assert_eq!(got.unwrap().0, "mcp/noetl-cluster");
        assert!(cfg["endpoint"]
            .as_str()
            .unwrap()
            .ends_with("/ehgraph/noetl-cluster/mcp"));
        assert_eq!(
            cfg["server"], "mcp/noetl-cluster",
            "server name kept for the result"
        );

        let mut none = mcp(json!({"server": "kubernetes"}));
        let called = resolve_catalog_server(&mut none, |_| async {
            panic!("no call without a catalog server")
        })
        .await;
        assert!(matches!(called, Ok(None)));

        let mut c = mcp(json!({"server": "mcp/missing"}));
        let e = resolve_catalog_server(&mut c, |_| async { Ok(None) })
            .await
            .unwrap_err();
        assert!(e.is_terminal() && e.to_string().contains("not registered"));

        let mut c = mcp(json!({"server": "mcp/x"}));
        let e = resolve_catalog_server(&mut c, |_| async {
            Err(anyhow::anyhow!("connection refused"))
        })
        .await
        .unwrap_err();
        assert!(!e.is_terminal(), "a transport failure is retryable");
        assert!(c.get("endpoint").is_none());
    }

    // ---- on_provider_error -------------------------------------------------

    #[test]
    fn the_policy_is_parsed_strictly_and_removed_from_the_config() {
        let mut c = json!({"kind": "mcp", "on_provider_error": "fail"});
        assert_eq!(
            take_provider_error_policy(&mut c),
            Ok(Some(ProviderErrorPolicy::Fail))
        );
        assert!(c.get(POLICY_KEY).is_none(), "the tool never sees the key");
        let mut c = json!({"on_provider_error": "continue"});
        assert_eq!(
            take_provider_error_policy(&mut c),
            Ok(Some(ProviderErrorPolicy::Continue))
        );
        let mut c = json!({"kind": "http"});
        assert_eq!(take_provider_error_policy(&mut c), Ok(None));
        for bad in [json!("FAIL"), json!("terminate"), json!(true), json!(1)] {
            let mut c = json!({ "on_provider_error": bad });
            assert!(take_provider_error_policy(&mut c).is_err(), "{c}");
        }
    }

    #[test]
    fn the_step_policy_overrides_the_process_default_both_ways() {
        assert!(terminates(Some(ProviderErrorPolicy::Fail), false));
        assert!(!terminates(Some(ProviderErrorPolicy::Continue), true));
        assert!(terminates(None, true));
        assert!(!terminates(None, false));
    }

    #[test]
    fn an_mcp_transport_error_is_recognised_only_for_mcp() {
        let d = json!({"status": "error", "error": "MCP HTTP error: HTTP status 401 Unauthorized"});
        assert!(mcp_transport_error("mcp", Some(&d))
            .unwrap()
            .contains("401"));
        assert_eq!(mcp_transport_error("http", Some(&d)), None);
        assert_eq!(
            mcp_transport_error("mcp", Some(&json!({"status": "ok"}))),
            None
        );
        assert_eq!(mcp_transport_error("mcp", None), None);
    }

    // ---- WorkerMcpTool against a real HTTP MCP server -----------------------

    /// (method, Authorization, X-Tenant) per request.
    type Request = (String, Option<String>, Option<String>);

    #[derive(Clone, Default)]
    struct Seen(Arc<Mutex<Vec<Request>>>);

    /// A minimal MCP server: like ehgraph, `initialize` is open and `tools/call`
    /// requires `Authorization: Bearer good`, answering `isError: true` otherwise.
    async fn spawn_server(seen: Seen) -> String {
        let app = Router::new().route(
            "/mcp",
            post(move |headers: AxHeaders, Json(req): Json<Value>| {
                let seen = seen.clone();
                async move {
                    let auth = headers.get("authorization").and_then(|v| v.to_str().ok()).map(str::to_string);
                    let extra = headers.get("x-tenant").and_then(|v| v.to_str().ok()).map(str::to_string);
                    let method = req["method"].as_str().unwrap_or("").to_string();
                    seen.0.lock().unwrap().push((method.clone(), auth.clone(), extra));
                    let id = req["id"].clone();
                    let result = match method.as_str() {
                        "initialize" => json!({"protocolVersion": "2024-11-05", "capabilities": {}}),
                        "tools/call" if auth.as_deref() == Some("Bearer good") => json!({
                            "content": [{"type": "text", "text": format!("hello {}", req["params"]["arguments"]["who"])}],
                            "isError": false,
                        }),
                        "tools/call" => json!({"content": [{"type": "text", "text": "unauthorized"}], "isError": true}),
                        _ => return Json(json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32601, "message": "method not found"}})),
                    };
                    Json(json!({"jsonrpc": "2.0", "id": id, "result": result}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}/mcp")
    }

    fn tool_config(cfg: Value) -> ToolConfig {
        let mut c = cfg;
        c["kind"] = json!("mcp");
        serde_json::from_value(c).unwrap()
    }

    fn ctx() -> ExecutionContext {
        let mut c = ExecutionContext::new(1, "call", "http://unused");
        c.set_secret("ehgraph_mcp", "good");
        c
    }

    #[tokio::test]
    async fn a_keychain_bearer_alias_reaches_the_server_as_an_authorization_header() {
        let seen = Seen::default();
        let ep = spawn_server(seen.clone()).await;
        // The shape auth_alias leaves behind for `auth: ehgraph_mcp` (a bearer credential).
        let cfg = tool_config(json!({
            "endpoint": ep, "server": "mcp/noetl-cluster", "tool": "greet", "arguments": {"who": "ionq"},
            "auth": {"type": "bearer", "credential": "ehgraph_mcp"},
        }));
        let r = WorkerMcpTool::new().execute(&cfg, &ctx()).await.unwrap();
        let d = r.data.unwrap();
        assert_eq!(d["status"], "ok");
        assert_eq!(d["text"], "hello \"ionq\"");
        assert_eq!(d["result"]["isError"], false);
        assert_eq!(d["sent_headers"], json!(["authorization"]));
        assert_eq!(d["server"], "mcp/noetl-cluster");
        assert!(
            !d.to_string().contains("good"),
            "the credential is never echoed: {d}"
        );
        let calls = seen.0.lock().unwrap().clone();
        assert_eq!(calls.len(), 2);
        assert!(
            calls
                .iter()
                .all(|(_, a, _)| a.as_deref() == Some("Bearer good")),
            "{calls:?}"
        );
    }

    #[tokio::test]
    async fn literal_headers_are_sent_and_reserved_ones_are_refused() {
        let seen = Seen::default();
        let ep = spawn_server(seen.clone()).await;
        let cfg = tool_config(json!({
            "endpoint": ep, "tool": "greet", "arguments": {"who": "x"},
            "headers": {"Authorization": "Bearer good", "X-Tenant": "ionq", "X-Empty": ""},
        }));
        let d = WorkerMcpTool::new()
            .execute(&cfg, &ctx())
            .await
            .unwrap()
            .data
            .unwrap();
        assert_eq!(d["result"]["isError"], false);
        assert_eq!(
            d["sent_headers"],
            json!(["authorization", "x-tenant"]),
            "an empty header is dropped"
        );
        assert_eq!(seen.0.lock().unwrap()[1].2.as_deref(), Some("ionq"));

        let bad =
            tool_config(json!({"endpoint": ep, "tool": "t", "headers": {"Mcp-Session-Id": "x"}}));
        let e = WorkerMcpTool::new()
            .execute(&bad, &ctx())
            .await
            .unwrap_err();
        assert!(e.to_string().contains("cannot be overridden"), "{e}");
    }

    #[tokio::test]
    async fn without_auth_or_headers_the_upstream_tool_runs_unchanged() {
        let seen = Seen::default();
        let ep = spawn_server(seen.clone()).await;
        let cfg = tool_config(json!({"endpoint": ep, "tool": "greet", "arguments": {"who": "x"}}));
        let d = WorkerMcpTool::new()
            .execute(&cfg, &ctx())
            .await
            .unwrap()
            .data
            .unwrap();
        assert!(
            d.get("sent_headers").is_none(),
            "delegated to noetl-tools McpTool: {d}"
        );
        assert_eq!(
            d["result"]["isError"], true,
            "the server refused the unauthenticated call"
        );
        assert!(seen.0.lock().unwrap().iter().all(|(_, a, _)| a.is_none()));
    }

    #[tokio::test]
    async fn a_transport_failure_is_a_structured_error_without_the_credential() {
        let cfg = tool_config(json!({
            "endpoint": "http://127.0.0.1:9/mcp", "tool": "t", "timeout": 2,
            "auth": {"type": "bearer", "credential": "ehgraph_mcp"},
        }));
        let d = WorkerMcpTool::new()
            .execute(&cfg, &ctx())
            .await
            .unwrap()
            .data
            .unwrap();
        assert_eq!(d["status"], "error");
        assert!(d["error"].as_str().unwrap().starts_with("MCP HTTP error"));
        assert!(!d.to_string().contains("good"));
        assert!(mcp_transport_error("mcp", Some(&d)).is_some());
    }

}
