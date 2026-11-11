//! Playbook-bound agent tools on the cluster pools (build plan item 8,
//! interop RFC §3.2 Stage B).
//!
//! `noetl_tools::tools::AgentTool` (the registry `kind: agent`) refuses an agent
//! whose `tools:` entries are bound to playbooks: a playbook has to run between
//! model turns, and that tool has no way to start one. This module is the worker
//! hosting that loop when `NOETL_WORKER_AGENT_PLAYBOOK_TOOLS=true`:
//!
//! 1. The worker drives `AgentSession` turn by turn, as the CLI local runtime
//!    does.
//! 2. A `mcp_servers:` / `agents:` call is answered by the session itself, the
//!    same as before.
//! 3. A `tools:` call bound to `playbook: <catalog path>` (optionally with
//!    `result_step: <step>`, the child step whose result is the answer; without
//!    it the child's last step result is used) starts a **child
//!    execution** through the control plane (`kind: playbook`,
//!    `return_result: true`, parent = this execution). The child is routed and
//!    claimed like any other execution, so it runs on the shared or system
//!    pool its playbook targets, not inside this worker. Its result becomes
//!    the `tool_result` the model sees next turn. A failed child is an
//!    `is_error` tool result, as in the CLI, so the model can recover.
//! 4. Every turn and every tool call is recorded as an event on this execution
//!    (`agent.turn`, `agent.tool_call`, `agent.final`). Events go through the
//!    normal emit path, so they land in `noetl.event` and, when the worker's
//!    EHDB event-log hook is armed, in the EHDB tier, keyed by execution.
//!
//! Budgets the model cannot override: `max_turns` (the session's own),
//! `NOETL_AGENT_MAX_TOOL_CALLS` per step (default 32), and
//! `NOETL_AGENT_TOOL_TIMEOUT_SECS` per child (default 180).
//!
//! What this is not: the loop still lives in one command on one worker. If
//! that worker dies mid-loop, the step is retried from the first turn, and
//! children that already ran are run again. An orchestrator-driven loop, with
//! one command per turn and resume from the recorded turns, is TARGET.

use std::time::Instant;

use async_trait::async_trait;
use noetl_tools::context::ExecutionContext;
use noetl_tools::error::ToolError;
use noetl_tools::registry::{ToolConfig, ToolRegistry};
use noetl_tools::result::ToolResult;
use noetl_tools::tools::{AgentSession, AgentToolCall, AgentToolOutput, AgentTurn};
use serde_json::{json, Value};

/// Arms the hosted loop. Strict `true`; anything else keeps the refusal.
pub(crate) const PLAYBOOK_TOOLS_ENV: &str = "NOETL_WORKER_AGENT_PLAYBOOK_TOOLS";
const MAX_TOOL_CALLS_ENV: &str = "NOETL_AGENT_MAX_TOOL_CALLS";
const TOOL_TIMEOUT_ENV: &str = "NOETL_AGENT_TOOL_TIMEOUT_SECS";
const DEFAULT_MAX_TOOL_CALLS: u32 = 32;
const DEFAULT_TOOL_TIMEOUT_SECS: u64 = 180;
/// Bytes of a tool's input / result kept on its `agent.tool_call` record.
const RECORD_PREVIEW_BYTES: usize = 2048;

pub(crate) fn playbook_tools_enabled() -> bool {
    std::env::var(PLAYBOOK_TOOLS_ENV).ok().as_deref() == Some("true")
}

/// True when any `tools:` entry carries a `playbook:` binding: the case the
/// registry agent refuses and this host runs.
pub(crate) fn has_playbook_tools(config: &Value) -> bool {
    config
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|ts| ts.iter().any(|t| t.get("playbook").is_some()))
}

/// Limits the orchestrating host enforces, whatever the model asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AgentBudget {
    pub max_tool_calls: u32,
    pub tool_timeout_secs: u64,
}

impl Default for AgentBudget {
    fn default() -> Self {
        Self {
            max_tool_calls: DEFAULT_MAX_TOOL_CALLS,
            tool_timeout_secs: DEFAULT_TOOL_TIMEOUT_SECS,
        }
    }
}

impl AgentBudget {
    pub(crate) fn from_env() -> Self {
        let num = |k: &str| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .filter(|n| *n > 0)
        };
        let d = Self::default();
        Self {
            max_tool_calls: num(MAX_TOOL_CALLS_ENV)
                .map(|n| n.min(u32::MAX as u64) as u32)
                .unwrap_or(d.max_tool_calls),
            tool_timeout_secs: num(TOOL_TIMEOUT_ENV).unwrap_or(d.tool_timeout_secs),
        }
    }
}

/// Where the per-turn records go. The worker's impl emits events on the
/// execution; tests collect them.
#[async_trait]
pub(crate) trait TurnSink: Send + Sync {
    async fn record(&self, event_type: &str, context: Value);
}

fn preview(v: &Value) -> Value {
    let s = match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    if s.len() <= RECORD_PREVIEW_BYTES {
        return Value::String(s);
    }
    let mut end = RECORD_PREVIEW_BYTES;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    json!({ "preview": &s[..end], "truncated_bytes": s.len() - end })
}

/// Run one tool call bound to a playbook as a child execution, through the
/// registry's `kind: playbook` (start + await + extract result).
async fn run_playbook_call(
    registry: &ToolRegistry,
    ctx: &ExecutionContext,
    call: &AgentToolCall,
    path: &str,
    result_step: Option<&str>,
    budget: &AgentBudget,
) -> (AgentToolOutput, Option<String>) {
    // The tool input becomes the child's workload, with JSON types kept (the
    // CLI flattens values to strings; the cluster passes them through).
    let payload = match &call.input {
        Value::Object(_) => call.input.clone(),
        Value::Null => json!({}),
        other => json!({ "input": other }),
    };
    let mut config = json!({
        "path": path,
        "payload": payload,
        "return_result": true,
        "timeout": budget.tool_timeout_secs,
    });
    // Without it `kind: playbook` returns the child's LAST step result, which
    // for most playbooks is a closing `end` noop rather than the answer.
    if let Some(step) = result_step {
        config["result_step"] = json!(step);
    }
    let cfg = ToolConfig {
        kind: "playbook".into(),
        config,
        timeout: Some(30),
        retry: None,
        auth: None,
    };
    match registry.execute_from_config(&cfg, ctx).await {
        Ok(r) => {
            let child = r.child_execution_id.clone();
            let failed = r.error.is_some() && r.data.is_none();
            let content = match (&r.data, &r.error) {
                (Some(Value::String(s)), _) => s.clone(),
                (Some(d), _) => d.to_string(),
                (None, Some(e)) => e.clone(),
                (None, None) => "null".into(),
            };
            (
                AgentToolOutput {
                    tool_use_id: call.id.clone(),
                    content,
                    is_error: failed,
                },
                child,
            )
        }
        Err(e) => (
            AgentToolOutput {
                tool_use_id: call.id.clone(),
                content: format!("tool `{}` ({path}) failed: {e}", call.name),
                is_error: true,
            },
            child_id_from_error(&e.to_string()),
        ),
    }
}

/// `kind: playbook` names the child in its failure text ("child playbook P (ID): …").
fn child_id_from_error(msg: &str) -> Option<String> {
    let start = msg.find(" (")? + 2;
    let end = start + msg[start..].find(')')?;
    let id = &msg[start..end];
    (!id.is_empty() && id.chars().all(|c| c.is_ascii_digit())).then(|| id.to_string())
}

/// Drive an agent whose tools include playbook bindings to completion.
pub(crate) async fn run_hosted(
    config: &ToolConfig,
    ctx: &ExecutionContext,
    registry: &ToolRegistry,
    sink: &dyn TurnSink,
    budget: AgentBudget,
) -> Result<ToolResult, ToolError> {
    let started = Instant::now();
    if config.config.get("framework").and_then(Value::as_str) == Some("claude-code") {
        return Err(ToolError::Configuration(
            "framework claude-code runs a local CLI under its signed-in account and is not available on this runtime; use framework claude or remote"
                .to_string(),
        ));
    }
    let rendered = ToolConfig {
        config: noetl_tools::template::TemplateEngine::new()
            .render_value(&config.config, &ctx.to_template_context())?,
        ..config.clone()
    };
    let mut session = AgentSession::start(&rendered, ctx).await?;
    let mut tool_calls = 0u32;
    let mut children: Vec<String> = Vec::new();

    loop {
        let turn = session.next_turn().await?;
        let n = session.turns();
        match turn {
            AgentTurn::Final(value) => {
                sink.record(
                    "agent.turn",
                    json!({ "turn": n, "outcome": "final", "tool_calls": [] }),
                )
                .await;
                sink.record(
                    "agent.final",
                    json!({
                        "turns": n,
                        "tool_calls": tool_calls,
                        "child_execution_ids": children,
                        "duration_ms": started.elapsed().as_millis() as u64,
                    }),
                )
                .await;
                return Ok(
                    ToolResult::success(value).with_duration(started.elapsed().as_millis() as u64)
                );
            }
            AgentTurn::ToolCalls(calls) => {
                sink.record(
                    "agent.turn",
                    json!({
                        "turn": n,
                        "outcome": "tool_calls",
                        "tool_calls": calls.iter().map(|c| json!({"id": c.id, "name": c.name})).collect::<Vec<_>>(),
                    }),
                )
                .await;
                let mut outputs = Vec::with_capacity(calls.len());
                for call in &calls {
                    tool_calls += 1;
                    if tool_calls > budget.max_tool_calls {
                        sink.record(
                            "agent.final",
                            json!({ "turns": n, "tool_calls": tool_calls - 1, "child_execution_ids": children, "refused": "tool_call_budget" }),
                        )
                        .await;
                        return Err(ToolError::ExecutionFailed(format!(
                            "agent exceeded its tool-call budget of {} ({MAX_TOOL_CALLS_ENV})",
                            budget.max_tool_calls
                        )));
                    }
                    let t = Instant::now();
                    let (output, binding, path, child) =
                        if let Some(out) = session.run_source_tool(call).await {
                            (out, "source", None, None)
                        } else {
                            let spec = session.tool_spec(&call.name);
                            let path = spec
                                .and_then(|s| s.binding.get("playbook"))
                                .and_then(Value::as_str)
                                .map(str::to_string);
                            let result_step = spec
                                .and_then(|s| s.binding.get("result_step"))
                                .and_then(Value::as_str)
                                .map(str::to_string);
                            match path {
                                Some(p) => {
                                    let (out, child) = run_playbook_call(
                                        registry,
                                        ctx,
                                        call,
                                        &p,
                                        result_step.as_deref(),
                                        &budget,
                                    )
                                    .await;
                                    (out, "playbook", Some(p), child)
                                }
                                None => {
                                    return Err(ToolError::ExecutionFailed(format!(
                                        "model requested undeclared tool `{}`",
                                        call.name
                                    )))
                                }
                            }
                        };
                    if let Some(c) = &child {
                        children.push(c.clone());
                    }
                    sink.record(
                        "agent.tool_call",
                        json!({
                            "turn": n,
                            "tool_use_id": call.id,
                            "tool": call.name,
                            "binding": binding,
                            "playbook": path,
                            "child_execution_id": child,
                            "is_error": output.is_error,
                            "duration_ms": t.elapsed().as_millis() as u64,
                            "input": preview(&call.input),
                            "result": preview(&Value::String(output.content.clone())),
                        }),
                    )
                    .await;
                    outputs.push(output);
                }
                session.submit_tool_results(outputs);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noetl_tools::registry::Tool;
    use std::sync::{Arc, Mutex};

    #[derive(Default, Clone)]
    struct Collect(Arc<Mutex<Vec<(String, Value)>>>);
    #[async_trait]
    impl TurnSink for Collect {
        async fn record(&self, event_type: &str, context: Value) {
            self.0.lock().unwrap().push((event_type.into(), context));
        }
    }

    /// Stands in for `kind: playbook`: answers from a map, records the configs.
    struct FakeChild {
        seen: Arc<Mutex<Vec<Value>>>,
    }
    #[async_trait]
    impl Tool for FakeChild {
        fn name(&self) -> &'static str {
            "playbook"
        }
        async fn execute(
            &self,
            config: &ToolConfig,
            _ctx: &ExecutionContext,
        ) -> Result<ToolResult, ToolError> {
            self.seen.lock().unwrap().push(config.config.clone());
            match config.config["path"].as_str() {
                Some("tests/ok") => Ok(ToolResult::success(
                    json!({"greeting": format!("hello {}", config.config["payload"]["who"].as_str().unwrap_or("?"))}),
                )
                .with_child_execution_id("4242")),
                _ => Err(ToolError::ExecutionFailed(
                    "child playbook tests/broken (4343): boom".into(),
                )),
            }
        }
    }

    /// A model that calls every offered tool on turn 1 and echoes the results on turn 2.
    async fn stub_model() -> String {
        use axum::{routing::post, Json, Router};
        async fn reply(Json(body): Json<Value>) -> Json<Value> {
            let last = &body["messages"].as_array().unwrap().last().unwrap()["content"];
            if let Some(blocks) = last.as_array() {
                if blocks.first().and_then(|b| b["type"].as_str()) == Some("tool_result") {
                    let texts: Vec<String> = blocks
                        .iter()
                        .map(|b| {
                            format!(
                                "{}{}",
                                if b["is_error"] == json!(true) {
                                    "ERR:"
                                } else {
                                    ""
                                },
                                b["content"].as_str().unwrap_or("")
                            )
                        })
                        .collect();
                    return Json(
                        json!({"content":[{"type":"text","text": format!("model saw: {}", texts.join(" | "))}],
                        "stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}),
                    );
                }
            }
            let calls: Vec<Value> = body["tools"].as_array().cloned().unwrap_or_default().iter().enumerate()
                .map(|(i, t)| json!({"type":"tool_use","id": format!("tu_{i}"),"name": t["name"],"input":{"who":"model"}}))
                .collect();
            Json(
                json!({"content": calls, "stop_reason":"tool_use","usage":{"input_tokens":1,"output_tokens":1}}),
            )
        }
        let app = Router::new().route("/v1/messages", post(reply));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        format!("http://{addr}")
    }

    fn agent_cfg(endpoint: &str, tools: Value) -> ToolConfig {
        serde_json::from_value(json!({
            "kind": "agent",
            "framework": "claude", "model": "stub", "prompt": "hi", "endpoint": endpoint,
            "tools": tools,
            "auth": { "type": "api_key", "credential": "model" },
        }))
        .unwrap()
    }

    fn ctx() -> ExecutionContext {
        let mut c = ExecutionContext::new(7, "ask", "http://unused");
        c.set_secret("model", "k");
        c
    }

    fn registry(seen: Arc<Mutex<Vec<Value>>>) -> ToolRegistry {
        let mut r = ToolRegistry::new();
        r.register(FakeChild { seen });
        r
    }

    #[test]
    fn detects_playbook_bindings_only() {
        assert!(has_playbook_tools(
            &json!({"tools":[{"name":"a","playbook":"x"}]})
        ));
        assert!(!has_playbook_tools(&json!({"tools":[{"name":"a"}]})));
        assert!(!has_playbook_tools(&json!({"prompt":"x"})));
    }

    #[test]
    fn child_id_is_read_from_the_playbook_error() {
        assert_eq!(
            child_id_from_error("child playbook a/b (123): x").as_deref(),
            Some("123")
        );
        assert_eq!(child_id_from_error("http 500"), None);
    }

    #[tokio::test]
    async fn playbook_tools_run_as_children_and_every_turn_is_recorded() {
        let ep = stub_model().await;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Collect::default();
        let cfg = agent_cfg(
            &ep,
            json!([
                {"name":"greet","description":"g","input_schema":{"type":"object","properties":{"who":{"type":"string"}}},"playbook":"tests/ok","result_step":"greet"},
                {"name":"broken","description":"b","playbook":"tests/broken"}
            ]),
        );
        let out = run_hosted(
            &cfg,
            &ctx(),
            &registry(seen.clone()),
            &sink,
            AgentBudget::default(),
        )
        .await
        .unwrap();
        let text = out.data.unwrap().to_string();
        assert!(text.contains("hello model"), "{text}");
        assert!(
            text.contains("ERR:tool `broken` (tests/broken) failed"),
            "a failed child is an is_error tool result, not a failed step: {text}"
        );

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0]["return_result"], json!(true));
        assert_eq!(seen[0]["payload"], json!({"who":"model"}));
        assert_eq!(seen[0]["timeout"], json!(180));
        assert_eq!(
            seen[0]["result_step"],
            json!("greet"),
            "the binding's result_step is passed through"
        );
        assert!(seen[1].get("result_step").is_none());

        let rec = sink.0.lock().unwrap();
        let types: Vec<&str> = rec.iter().map(|(t, _)| t.as_str()).collect();
        assert_eq!(
            types,
            [
                "agent.turn",
                "agent.tool_call",
                "agent.tool_call",
                "agent.turn",
                "agent.final"
            ]
        );
        assert_eq!(rec[1].1["child_execution_id"], json!("4242"));
        assert_eq!(rec[1].1["binding"], json!("playbook"));
        assert_eq!(rec[2].1["child_execution_id"], json!("4343"));
        assert_eq!(rec[2].1["is_error"], json!(true));
        assert_eq!(rec[4].1["child_execution_ids"], json!(["4242", "4343"]));
    }

    #[tokio::test]
    async fn the_tool_call_budget_is_enforced_by_the_host() {
        let ep = stub_model().await;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Collect::default();
        let cfg = agent_cfg(
            &ep,
            json!([
                {"name":"a","playbook":"tests/ok"},
                {"name":"b","playbook":"tests/ok"},
                {"name":"c","playbook":"tests/ok"}
            ]),
        );
        let budget = AgentBudget {
            max_tool_calls: 2,
            tool_timeout_secs: 5,
        };
        let err = run_hosted(&cfg, &ctx(), &registry(seen.clone()), &sink, budget)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("tool-call budget of 2"), "{err}");
        assert_eq!(
            seen.lock().unwrap().len(),
            2,
            "the third child never started"
        );
        assert_eq!(
            sink.0.lock().unwrap().last().unwrap().1["refused"],
            json!("tool_call_budget")
        );
    }

    #[tokio::test]
    async fn claude_code_stays_refused() {
        let mut cfg = agent_cfg("http://x", json!([{"name":"a","playbook":"p"}]));
        cfg.config["framework"] = json!("claude-code");
        let err = run_hosted(
            &cfg,
            &ctx(),
            &ToolRegistry::new(),
            &Collect::default(),
            AgentBudget::default(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("claude-code"), "{err}");
    }
}
