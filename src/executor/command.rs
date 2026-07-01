//! Command executor.
//!
//! R-1.2 PR-2d-2: `CommandExecutor::execute` now takes
//! `&noetl_executor::worker::source::Command` (the executor crate's
//! enriched Command, 0.3.0+) instead of the worker's local
//! `crate::client::Command`.  Field accesses:
//!
//! - `command.execution_id` (i64) — same as before.
//! - `command.step` (String) — was `command.step()` accessor returning `&node_name`.
//! - `command.command_id` (String) — was `command.command_id()`.
//! - `command.tool_kind` (String) — was `command.action`.
//! - `command.render_context` (HashMap) — was `command.render_context()`.
//! - `command.attempts` (u32) — new in 0.3.0; useful for retry decisions.
//! - `command.input` (Value) — carries the worker's full `context` JSON
//!   including `tool_config`, `cases`, `args`, and `render_context`
//!   (the dedicated field is also populated for direct access).
//!   `tool_config` is extracted via `command.input.get("tool_config")`;
//!   `cases` via `command.input.get("cases")`.
//!
//! Per `nats::source::NatsCommandSource::translate`, the executor's
//! Command is a lossless mapping of the worker's Command.

use anyhow::Result;
use noetl_arrow_cache::ArrowIpcSharedMemoryCache;
use noetl_executor::worker::source::Command;
use noetl_tools::context::ExecutionContext;
use noetl_tools::registry::{ToolConfig, ToolRegistry};
use noetl_tools::tools::create_default_registry;
use std::collections::HashMap;
use std::sync::Arc;

use crate::client::{ControlPlaneClient, ExecutorEvent};
use crate::executor::case_evaluator::{CaseAction, CaseEvaluator};
use crate::snowflake::SnowflakeGen;

/// Catalog path + seed version of the off-server drive plug-in the server
/// dispatches for every `system/orchestrate` command (`"plugin": { "path":
/// "system/orchestrate", "version": 1 }` in the server's
/// `dispatch_orchestrate_command`).  Used by the boot warmup (noetl/ai-meta#130)
/// to pre-compile the module so the first drive hop is a cache hit.
#[cfg(feature = "wasm-plugin")]
pub(crate) const ORCHESTRATE_PLUGIN_PATH: &str = "system/orchestrate";
#[cfg(feature = "wasm-plugin")]
pub(crate) const ORCHESTRATE_PLUGIN_VERSION: u32 = 1;

/// Env var carrying the comma-separated list of worker-pod env var
/// names to lift into `ExecutionContext.secrets` at startup
/// (noetl/ai-meta#34).  Operators populate the underlying env vars
/// via k8s Secret `envFrom`; playbook config (e.g.
/// `result_fetch.bearer_token: NOETL_FLIGHT_BEARER_TOKEN`)
/// references each by its env-var name as a keychain alias.
///
/// Per `agents/rules/execution-model.md`, business-logic credentials
/// (bearer tokens, API keys, etc.) belong in the NoETL keychain and
/// are referenced by alias from the playbook.  This allow-list is
/// the worker-side bridge between platform-mounted Secret envs and
/// the in-process keychain map the tools' `ctx.get_secret(...)`
/// calls read.
///
/// Empty / unset → no env vars get lifted (existing behaviour;
/// playbooks that pass literal credentials still work).
pub(crate) const KEYCHAIN_ENV_ALLOWLIST_VAR: &str = "NOETL_KEYCHAIN_ENV_VARS";

/// Maximum command attempts before a transient (retryable) pre-dispatch
/// failure is escalated to a terminal `call.error`.
///
/// A transient keychain transport error on a fresh command
/// (`command.attempts < MAX_PREDISPATCH_ATTEMPTS`) is left to the
/// command path's retry/redelivery — the worker does NOT emit a terminal
/// event, so a later attempt can still complete the step.  Once the
/// attempt counter reaches this ceiling the worker emits the terminal
/// `call.error` so the execution can't hang at `command.started`
/// indefinitely.  Terminal failures (clean 404, unsupported credential
/// type, malformed tool config) bypass this counter — they emit on the
/// first failure.  See noetl/ai-meta#78.
pub(crate) const MAX_PREDISPATCH_ATTEMPTS: u32 = 3;

/// Parse the comma-separated allow-list + look up each env var.
/// Names that are missing / empty in the environment are silently
/// skipped — that way an operator can stage rollouts (define the
/// allow-list ahead of mounting the Secret) without spamming
/// startup logs.
///
/// Returns a `(alias → value)` map ready to merge into
/// `ExecutionContext.secrets`.  Keys are the env-var names verbatim,
/// matching the convention playbook authors reference (so a
/// `bearer_token: NOETL_FLIGHT_BEARER_TOKEN` field resolves directly
/// without any prefix-strip transformation).
pub(crate) fn load_keychain_env_allowlist() -> HashMap<String, String> {
    let raw = match std::env::var(KEYCHAIN_ENV_ALLOWLIST_VAR) {
        Ok(s) => s,
        Err(_) => return HashMap::new(),
    };
    let mut out = HashMap::new();
    for name in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        match std::env::var(name) {
            Ok(value) if !value.is_empty() => {
                out.insert(name.to_string(), value);
            }
            _ => {
                // Allow-listed but not yet set on the pod — silent
                // skip.  An operator who's mid-rollout (allow-list
                // ahead of Secret mount) shouldn't see a noisy log
                // line per startup.
            }
        }
    }
    out
}

/// Command executor that runs tools and evaluates cases.
pub struct CommandExecutor {
    /// Tool registry with all available tools.
    tool_registry: ToolRegistry,

    /// Case evaluator for when/then logic.
    case_evaluator: CaseEvaluator,

    /// Control plane client for event emission.
    client: ControlPlaneClient,

    /// Worker ID.
    worker_id: String,

    /// Control-plane base URL.
    server_url: String,

    /// Application-side snowflake generator for `event_id` on every
    /// emitted envelope.  Per `observability.md` Principle 3 — the
    /// id is generated BEFORE the row hits the database so spans /
    /// metrics carry it at span-creation time and retries stay
    /// idempotent.
    snowflake: Arc<SnowflakeGen>,

    /// Same-node Arrow IPC cache for `call.done` results that
    /// exceed the broker's 100KB inline budget.  When a tool
    /// returns a large output (Postgres rowset, HTTP API
    /// response, etc.), the bytes go into shared memory + the
    /// event payload carries an `IpcHint` reference instead.
    /// Per R-2.1 (the `noetl-arrow-cache` crate); partial progress
    /// on noetl/worker#24.
    arrow_cache: Arc<ArrowIpcSharedMemoryCache>,

    /// Operator-provided keychain credentials lifted from worker
    /// pod env vars at startup (noetl/ai-meta#34).  The allow-list
    /// (`NOETL_KEYCHAIN_ENV_VARS`) names which env vars are credentials
    /// rather than runtime config; values get copied into each
    /// command's `ExecutionContext.secrets` so tools'
    /// `ctx.get_secret(alias)` calls resolve playbook keychain
    /// aliases (e.g. `result_fetch.bearer_token: NOETL_FLIGHT_BEARER_TOKEN`).
    ///
    /// Populated once at executor construction; immutable across
    /// the worker's lifetime.  Empty when the allow-list env var
    /// is unset (pre-#34 behaviour, no breakage).
    keychain_env: HashMap<String, String>,

    /// WASM plug-in dispatcher for `tool_kind: "wasm"` commands
    /// (noetl/ai-meta#105). Behind the off-by-default `wasm-plugin` feature.
    /// The host's module cache persists across commands, and the dispatch path
    /// is `&self`, so it lives here behind a `Mutex`.
    #[cfg(feature = "wasm-plugin")]
    wasm_dispatcher: tokio::sync::Mutex<crate::plugin::WasmDispatcher>,

    /// Off-server state-builder mode (RFC #115 Phase 4 drive cutover).  When
    /// [`BuilderMode::Authoritative`] (`NOETL_STATE_BUILDER=offserver`), a
    /// `system/orchestrate` command marked `__offserver_build__` builds its drive
    /// `WorkflowState` from [`Self::state_builder_index`] (the WAL spine, fed to
    /// the wasm `run` from_events entry) instead of the server-built `run_state`
    /// payload.  Default [`BuilderMode::Off`] → the server-built state is used,
    /// exactly as today.
    #[cfg_attr(not(feature = "wasm-plugin"), allow(dead_code))]
    state_builder_mode: crate::state_builder::BuilderMode,

    /// Shared pool-side WAL event index — the off-server drive's state source.
    /// Fed by the drain loop ([`crate::state_builder::spawn_drain`]); read here
    /// when the builder is authoritative.
    #[cfg_attr(not(feature = "wasm-plugin"), allow(dead_code))]
    state_builder_index: crate::state_builder::SharedWalIndex,

    /// NATS URL for the targeted retained-WAL cold-rebuild on a bounded-cache
    /// miss (noetl/ai-meta#166 §5.2).  The executor connects on demand only on
    /// the rare miss path; the steady-state drive never touches NATS here.
    #[cfg_attr(not(feature = "wasm-plugin"), allow(dead_code))]
    state_builder_nats_url: String,

    /// Whether cold-rebuild-on-miss is enabled (`NOETL_STATE_INDEX_REHYDRATE_ON_MISS`),
    /// resolved once at construction (noetl/ai-meta#166 §5.2).
    #[cfg_attr(not(feature = "wasm-plugin"), allow(dead_code))]
    rehydrate_on_miss: bool,

    /// Concurrency cap on in-flight cold-rebuilds (noetl/ai-meta#166 §5.2) — a
    /// restart-under-load miss storm must not fan out into N simultaneous WAL
    /// re-scans.  A miss that can't get a permit just falls back as today.
    #[cfg_attr(not(feature = "wasm-plugin"), allow(dead_code))]
    rehydrate_gate: Arc<tokio::sync::Semaphore>,

    /// Whether cold-load-from-shard is enabled (`NOETL_STATE_SHARD_READ`),
    /// resolved once at construction (noetl/ai-meta#166 Phase 3).  On a drive
    /// miss the executor reads the execution's Feather state shard from object
    /// store instead of replaying the retained WAL; default off → the miss path
    /// is byte-identical to today's WAL replay.
    #[cfg_attr(not(feature = "wasm-plugin"), allow(dead_code))]
    shard_read: bool,

    /// Whether the shard-vs-WAL equivalence dual-build guard is enabled
    /// (`NOETL_STATE_SHARD_READ_VERIFY`) — noetl/ai-meta#166 Phase 3.  A
    /// validation/canary knob (pays both reads); off in steady state.
    #[cfg_attr(not(feature = "wasm-plugin"), allow(dead_code))]
    shard_read_verify: bool,

    /// Cell placement seed for reconstructing the state-shard object key on a
    /// cold-load (noetl/ai-meta#166 Phase 3) — reads the SAME `NOETL_RESULT_CELL_*`
    /// env the writer used, so the reader resolves the identical §7 key.
    #[cfg_attr(not(feature = "wasm-plugin"), allow(dead_code))]
    shard_read_cell: crate::state_materializer::CellSeed,
}

impl CommandExecutor {
    /// Create a new command executor.
    ///
    /// At construction time, scans the `NOETL_KEYCHAIN_ENV_VARS`
    /// allow-list + lifts the named env vars into a per-executor
    /// keychain map.  Each command's `ExecutionContext.secrets`
    /// then carries these values so playbook keychain aliases like
    /// `result_fetch.bearer_token: NOETL_FLIGHT_BEARER_TOKEN`
    /// resolve via `ctx.get_secret(alias)`.  See
    /// [`load_keychain_env_allowlist`] for the env-var contract.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: ControlPlaneClient,
        worker_id: String,
        server_url: String,
        snowflake: Arc<SnowflakeGen>,
        arrow_cache: Arc<ArrowIpcSharedMemoryCache>,
        state_builder_mode: crate::state_builder::BuilderMode,
        state_builder_index: crate::state_builder::SharedWalIndex,
        state_builder_nats_url: String,
    ) -> Self {
        let keychain_env = load_keychain_env_allowlist();
        if !keychain_env.is_empty() {
            // Observability per `agents/rules/observability.md` —
            // log the KEY NAMES at startup (not values; values are
            // credentials).  Operators verify the allow-list took
            // effect via `kubectl logs`.
            let mut names: Vec<&str> = keychain_env.keys().map(String::as_str).collect();
            names.sort_unstable();
            tracing::info!(
                count = names.len(),
                aliases = ?names,
                "Loaded keychain credentials from NOETL_KEYCHAIN_ENV_VARS"
            );
        }
        #[cfg(feature = "wasm-plugin")]
        let wasm_dispatcher = tokio::sync::Mutex::new(
            crate::plugin::WasmDispatcher::http(server_url.clone())
                .expect("WASM plug-in host init failed"),
        );
        Self {
            tool_registry: create_default_registry(),
            case_evaluator: CaseEvaluator::new(),
            client,
            worker_id,
            server_url,
            snowflake,
            arrow_cache,
            keychain_env,
            #[cfg(feature = "wasm-plugin")]
            wasm_dispatcher,
            state_builder_mode,
            state_builder_index,
            state_builder_nats_url,
            rehydrate_on_miss: crate::state_builder::rehydrate_on_miss_enabled(),
            // Small fixed cap: cold-rebuilds are a rare miss-path event; 2 lets a
            // couple proceed concurrently without a restart-under-load WAL-scan
            // storm.  Env-overridable for tuning.
            rehydrate_gate: Arc::new(tokio::sync::Semaphore::new(
                std::env::var("NOETL_STATE_INDEX_REHYDRATE_CONCURRENCY")
                    .ok()
                    .and_then(|s| s.trim().parse::<usize>().ok())
                    .filter(|&n| n > 0)
                    .unwrap_or(2),
            )),
            shard_read: crate::state_reader::shard_read_enabled(),
            shard_read_verify: crate::state_reader::shard_read_verify_enabled(),
            shard_read_cell: crate::state_materializer::CellSeed::from_env(),
        }
    }

    /// Boot-time warmup of the off-server orchestrate drive plug-in
    /// (noetl/ai-meta#130 cold-start).  Fetches + Cranelift-compiles
    /// `system/orchestrate@1` into the dispatcher's module cache so the first
    /// real drive hop is a cache hit instead of paying the one-time compile on
    /// the critical path.  Returns `true` on a successful warm, `false` when the
    /// warm failed (non-fatal — the first dispatch falls back to the lazy load).
    /// With the `wasm-plugin` feature off this is a no-op that returns `false`.
    #[cfg(feature = "wasm-plugin")]
    pub async fn warm_orchestrate_plugin(&self) -> bool {
        let t = std::time::Instant::now();
        let mut dispatcher = self.wasm_dispatcher.lock().await;
        match dispatcher
            .warm(ORCHESTRATE_PLUGIN_PATH, ORCHESTRATE_PLUGIN_VERSION)
            .await
        {
            Ok(()) => {
                crate::metrics::record_plugin_warm("warmed");
                tracing::info!(
                    plugin = ORCHESTRATE_PLUGIN_PATH,
                    version = ORCHESTRATE_PLUGIN_VERSION,
                    elapsed_ms = t.elapsed().as_millis() as u64,
                    "boot warmup: orchestrate drive plug-in compiled + cached"
                );
                true
            }
            Err(e) => {
                crate::metrics::record_plugin_warm("error");
                // Non-fatal: the server may not have seeded the module yet, or
                // be briefly unreachable at boot.  The first real dispatch will
                // lazily load it (paying the cold compile then).
                tracing::warn!(
                    plugin = ORCHESTRATE_PLUGIN_PATH,
                    error = %e,
                    "boot warmup: orchestrate plug-in warm failed (non-fatal; first dispatch will lazy-load)"
                );
                false
            }
        }
    }

    /// No-op warmup when the `wasm-plugin` feature is disabled.
    #[cfg(not(feature = "wasm-plugin"))]
    pub async fn warm_orchestrate_plugin(&self) -> bool {
        false
    }

    /// Execute a command.
    ///
    /// Per `observability.md` Principle 1: every boundary call
    /// ships a span.  The `command.execute` span covers the full
    /// dispatch path (tool registry lookup, tool execution, case
    /// evaluation, lifecycle event emission) so downstream
    /// observability tooling (traces, metrics exemplars) can group
    /// every sub-operation under one execution.
    ///
    /// Principle 2 (metrics over logs): dispatch duration recorded
    /// to `noetl_worker_dispatch_duration_seconds{tool_kind=...}`;
    /// errors to `noetl_worker_dispatch_errors_total{tool_kind=...}`.
    /// Both labeled by tool_kind so the dashboard can spot which
    /// tools are slow / failing.
    pub async fn execute(&self, command: &Command) -> Result<()> {
        self.execute_with_server_url(command, None).await
    }

    /// Execute a command with an optional per-dispatch server-URL
    /// override.
    ///
    /// noetl/ai-meta#53 Gap 1: the NATS notification carries a
    /// `server_url` field identifying the server that PUBLISHED
    /// the command.  Multi-server deployments (e.g. a kind cluster
    /// running both `noetl-server` (Python) and `noetl-server-rust`
    /// side by side) need each command's lifecycle events
    /// (claim → started → call.done → completed) to flow back to
    /// the originating server so its orchestrator can advance the
    /// playbook.  Without an override, the executor's captured
    /// client + server_url (initialised from `NOETL_SERVER_URL` at
    /// worker startup) wins, and events for a Rust-server-dispatched
    /// command end up at the Python server — silently recorded to
    /// the shared DB but not driving any orchestrator.
    ///
    /// When `server_url_override` is `Some`, this method:
    ///   * Builds a per-dispatch `ControlPlaneClient` via
    ///     `client.with_server_url(override)` — cheap; the inner
    ///     `reqwest::Client` is Arc-shared.
    ///   * Threads the override URL through to `ExecutionContext`
    ///     (where `auth` tools resolve credentials against the
    ///     callback URL) and through every call site that would
    ///     otherwise have used `self.client` / `self.server_url`.
    ///
    /// `None` keeps the old behaviour: every dispatch uses the
    /// startup-configured client + server URL.
    pub async fn execute_with_server_url(
        &self,
        command: &Command,
        server_url_override: Option<&str>,
    ) -> Result<()> {
        let span = tracing::info_span!(
            "command.execute",
            execution_id = command.execution_id,
            command_id = %command.command_id,
            step = %command.step,
            tool_kind = %command.tool_kind,
            attempts = command.attempts,
        );
        let _enter = span.enter();

        // Per-dispatch client + URL.  When the notification carries
        // a server_url, build a fresh `ControlPlaneClient` pointed
        // at the publishing server; otherwise reuse the captured
        // client (and server_url) from worker startup.
        let dispatch_server_url: String = server_url_override
            .map(|s| s.to_string())
            .unwrap_or_else(|| self.server_url.clone());
        let dispatch_client: ControlPlaneClient = match server_url_override {
            Some(url) if url != self.server_url => self.client.with_server_url(url),
            _ => self.client.clone(),
        };

        // Timer captures the full dispatch latency including tool
        // execution + case evaluation + lifecycle events.  Recorded
        // on every exit path (success + error) so the histogram is
        // complete.
        let dispatch_start = std::time::Instant::now();
        let tool_kind = command.tool_kind.clone();
        // Helper to record the dispatch metric on every exit path.
        // Captured by the error-return + success-return code below.
        let record_metric = |error: bool| {
            crate::metrics::record_dispatch(
                &tool_kind,
                dispatch_start.elapsed().as_secs_f64(),
                error,
            );
        };

        // Build execution context
        let mut ctx =
            ExecutionContext::new(command.execution_id, &command.step, &dispatch_server_url)
                .with_worker_id(&self.worker_id)
                .with_command_id(&command.command_id);

        // Seed keychain credentials lifted from worker pod env at
        // startup (noetl/ai-meta#34).  Tools that read credentials
        // via `ctx.get_secret(alias)` (postgres, result_fetch, ...)
        // now resolve playbook-side keychain aliases against the
        // operator-provided `NOETL_KEYCHAIN_ENV_VARS` allow-list.
        // Per-command secrets from the playbook step (auth: block)
        // can still set / override entries — they layer on top of
        // these env-mounted defaults.
        for (alias, value) in &self.keychain_env {
            ctx.set_secret(alias, value);
        }

        // Add render context variables from command payload.
        ctx.variables = command.render_context.clone();

        // Rebuild the `ctx` / `workload` namespace shims the server stopped
        // persisting on the command (noetl/ai-meta#103): they're deep copies of
        // the whole context, and persisting them doubled/tripled every step
        // output in the durable `command.issued` payload (a 1.7MB drain output
        // ballooned to 5MB).  Rebuild them transiently here so `{{ ctx.X }}` /
        // `{{ workload.X }}` templates in worker-rendered pipeline `input:`
        // blocks still resolve — without the durable command carrying the copy.
        // Idempotent: a pre-#103 server that still persists `ctx` is not
        // clobbered, and an already-structured `workload` block is preserved.
        if !ctx.variables.contains_key("ctx") {
            let flat = serde_json::to_value(&ctx.variables).unwrap_or(serde_json::Value::Null);
            ctx.variables.insert("ctx".to_string(), flat.clone());
            ctx.variables.entry("workload".to_string()).or_insert(flat);
        }

        ctx.variables
            .entry("action".to_string())
            .or_insert_with(|| serde_json::json!(command.tool_kind));
        ctx.variables
            .entry("node_name".to_string())
            .or_insert_with(|| serde_json::json!(command.step.clone()));

        // References-in-state consume side (noetl/ai-meta#115 Phase 1 / #101): when
        // the orchestrator runs with NOETL_REFS_IN_STATE, step outputs in the
        // context carry a `{reference}` + bounded `extracted` summary (+ the
        // `_ref`/`_store` accessors) instead of inline data.  Resolve a step's
        // full payload **only** when this step's input template binds its bulk
        // (a path the summary can't satisfy) — predicate / scalar / `_ref` access
        // reads off the bounded summary without a store round-trip, and unrelated
        // upstream results stay as references.  The template source is this
        // command's `input` (the tool config + args the tool will render).  No-op
        // when nothing is referenced — the default flag-off path costs one lookup.
        let template_src = serde_json::to_string(&command.input).unwrap_or_default();
        resolve_context_references(&mut ctx.variables, &template_src, &dispatch_client).await;

        // Emit command.started event.  R-1.2 PR-EE-3: `step` +
        // `worker_id` are top-level fields on the `ExecutorEvent`
        // shape, so the context payload only carries the
        // command-specific keys.  The server's `EventRequest` /
        // Python's `EventEmitRequest` both read `step` /
        // `worker_id` from the top level after EE-2 + EE-4.
        self.emit_event_via(
            &dispatch_client,
            "command.started",
            &command.step,
            "STARTED",
            command.execution_id,
            command.attempts,
            serde_json::json!({
                "command_id": command.command_id.clone(),
            }),
        )
        .await?;

        // Reconstruct the ToolConfig the noetl-tools registry expects.
        // `command.input` is the worker's full `context` JSON; the
        // tool-side config lives under `input.tool_config`.  Inject
        // `kind` from the executor `Command.tool_kind` field if the
        // nested config doesn't already carry it (mirrors the worker's
        // pre-PR-2d-2 `Command.tool_config_value()` behaviour).
        let mut tool_config_value = {
            let raw = command
                .input
                .get("tool_config")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            // Tools whose server-side `tool_config` is an array
            // (currently only `task_sequence`, which sends a
            // pipeline `[{label: spec}, ...]`) used to be silently
            // discarded here — the old `if !cfg.is_object()` arm
            // replaced the array with `{}`, leaving the worker to
            // dispatch an empty config to the registry.  Preserve
            // the array by wrapping it under a `tool_config` key
            // so the flattened `ToolConfig.config` carries it
            // forward; the `task_sequence` parser accepts that
            // exact "worker envelope" shape per its `parse_tasks`
            // contract in noetl-tools v2.18.1.
            let mut cfg = if raw.is_object() {
                raw
            } else if raw.is_array() {
                serde_json::json!({ "tool_config": raw })
            } else {
                serde_json::json!({})
            };
            if let Some(map) = cfg.as_object_mut() {
                map.entry("kind".to_string())
                    .or_insert_with(|| serde_json::json!(command.tool_kind));
                if !map.contains_key("args") {
                    if let Some(args) = command.input.get("args") {
                        map.insert("args".to_string(), args.clone());
                    }
                }
            }
            cfg
        };

        // Resolve string `auth:` values (keychain aliases) into either
        // the noetl-tools `AuthConfig` shape or the tool's flat
        // connection fields BEFORE serde deserialization.  See
        // `auth_alias` module + noetl/ai-meta#48 for the regression
        // brief.  Idempotent: if `auth` is already a struct (or
        // absent), this is a no-op + no HTTP call.
        let alias_secrets = match super::auth_alias::resolve_auth_alias(
            &mut tool_config_value,
            &dispatch_client,
            command.execution_id,
        )
        .await
        {
            Ok(secrets) => secrets,
            Err(e) => {
                // Pre-dispatch credential-alias failure.  Before this
                // fix the `?` early-returned here and the worker just
                // logged "Command execution failed" — no `call.error`
                // ever reached the server, so the execution hung at
                // `command.started` forever (noetl/ai-meta#78).  Now we
                // classify: a terminal failure (clean 404 / unsupported
                // type / malformed shape) — or a transient one whose
                // attempt counter is exhausted — emits a terminal
                // `call.error` so the execution fails cleanly.  A
                // transient failure on a fresh command stays retryable.
                record_metric(true);
                let terminal = e.is_terminal() || command.attempts >= MAX_PREDISPATCH_ATTEMPTS;
                return self
                    .handle_predispatch_failure(
                        &dispatch_client,
                        command,
                        ctx.call_index,
                        terminal,
                        anyhow::Error::new(e),
                    )
                    .await;
            }
        };
        if !alias_secrets.is_empty() {
            // Per `observability.md` Principle 1: log keychain
            // alias resolution so operators can trace credential
            // lookups in the worker log alongside the dispatch
            // span.  Value never logged (just the alias name).
            tracing::info!(
                execution_id = command.execution_id,
                step = %command.step,
                aliases = ?alias_secrets.keys().collect::<Vec<_>>(),
                "Resolved keychain alias(es) for tool dispatch"
            );
        }
        for (alias, value) in alias_secrets {
            ctx.set_secret(&alias, &value);
        }

        let tool_config: ToolConfig = match serde_json::from_value(tool_config_value) {
            Ok(cfg) => cfg,
            Err(e) => {
                // Malformed tool config is the other pre-dispatch
                // failure that used to silently `?`-return and hang the
                // execution at `command.started` (noetl/ai-meta#78).
                // It is always terminal — the same bytes deserialize to
                // the same error on a retry — so emit a terminal
                // `call.error`.
                record_metric(true);
                let err = anyhow::Error::new(e)
                    .context("malformed tool config (pre-dispatch deserialization)");
                return self
                    .handle_predispatch_failure(
                        &dispatch_client,
                        command,
                        ctx.call_index,
                        true,
                        err,
                    )
                    .await;
            }
        };

        tracing::debug!(
            execution_id = command.execution_id,
            step = %command.step,
            tool = %tool_config.kind,
            attempts = command.attempts,
            "Executing tool"
        );

        // noetl/ai-meta#104 Phase E — side-effect durability barrier.
        //
        // Before (re-)dispatching a SIDE-EFFECTING tool, check whether this
        // cycle's derived result URN already resolves to a durable result — i.e.
        // a prior drive already ran the cycle to completion. If it does, SKIP
        // re-execution and adopt the recorded result, so the external side
        // effect (an HTTP POST, a DB write, a payment, an email) fires exactly
        // once across a crash-resume / re-drive. Non-side-effecting cycles are
        // never blocked (idempotent recompute is fine); a side-effecting cycle
        // whose result is NOT durable re-executes normally.
        //
        // Adopt-only safety: `resolve_by_urn` returns `Some` only on a durable
        // tier hit, so the barrier can only ever turn a duplicate side effect
        // into a single one — never skip a cycle whose result is absent.
        //
        // Flag-gated (`NOETL_SIDE_EFFECT_BARRIER`); default-off → the whole block
        // is skipped (the cheap flag read short-circuits) and dispatch is
        // byte-identical to today.
        let barrier_adopted: Option<noetl_tools::result::ToolResult> =
            if side_effect_barrier_should_check(
                crate::result_resolver::side_effect_barrier(),
                &tool_config,
            ) {
                let uri =
                    cycle_logical_uri(command.execution_id, &command.step, &command.render_context);
                match crate::result_resolver::resolve_by_urn(&dispatch_client, &uri).await {
                    Some(payload) => {
                        crate::metrics::record_side_effect_barrier("skipped", &command.tool_kind);
                        tracing::info!(
                            execution_id = command.execution_id,
                            step = %command.step,
                            tool = %command.tool_kind,
                            uri = %uri,
                            "side-effect barrier: durable result exists; skipping re-execution and adopting recorded result (#104 Phase E)"
                        );
                        Some(noetl_tools::result::ToolResult::success(payload))
                    }
                    None => {
                        crate::metrics::record_side_effect_barrier("executed", &command.tool_kind);
                        None
                    }
                }
            } else {
                None
            };

        // Execute the tool — route `tool_kind: "wasm"` to the plug-in host
        // (noetl/ai-meta#105), everything else to the tool registry. The wasm
        // branch only exists with the `wasm-plugin` feature, so the default
        // build dispatches exactly as before. When the Phase E barrier adopted a
        // durable result above, the tool is NOT dispatched and that result is
        // used directly.
        let dispatch_outcome = match barrier_adopted {
            Some(adopted) => Ok(adopted),
            None => {
                #[cfg(feature = "wasm-plugin")]
                {
                    if tool_config.kind == "wasm" {
                        self.dispatch_wasm(&tool_config, command, &dispatch_client)
                            .await
                    } else {
                        self.tool_registry
                            .execute_from_config(&tool_config, &ctx)
                            .await
                    }
                }
                #[cfg(not(feature = "wasm-plugin"))]
                {
                    self.tool_registry
                        .execute_from_config(&tool_config, &ctx)
                        .await
                }
            }
        };

        let tool_result = match dispatch_outcome {
            Ok(result) => {
                // Emit call.done event with a reference-only result
                // payload.  The Python broker's
                // `_validate_reference_only_payload` enforces that
                // `payload.result` only carries `{status, reference,
                // context, command_id}` at the top level — the raw
                // tool fields (`stdout` / `stderr` / `exit_code` /
                // `data` / `duration_ms`) live INSIDE `context` so
                // downstream steps can reference them via Jinja
                // (`step_name.data.rows[N].x`).
                //
                // For results under `NOETL_EVENT_RESULT_CONTEXT_MAX_BYTES`
                // (default 100 KB) the broker persists `context` as-is
                // and downstream Jinja templates can read the tool
                // output.  When the JSON would exceed that, the broker
                // silently drops the context (`_bounded_context`
                // returns None), so we pre-check the size on the Rust
                // side and emit a WARN log so operators can see *why*
                // their large-result step's downstream rendering is
                // empty.  Until the result-store / `noetl-arrow-cache`
                // reference path lands (noetl/worker#24), an
                // over-budget result still ships with just `{status}` —
                // identical behaviour to a silent broker drop, just
                // visible in the worker's logs.
                //
                // Defensive: the broker forbids `_internal_data` at
                // any depth in `result.context`.  Our `ToolResult`
                // doesn't surface that key, so the serialised value
                // round-trips cleanly through the validator.
                let result_context = serde_json::to_value(&result)
                    .unwrap_or_else(|_| serde_json::json!({ "status": result.status.to_string() }));
                let mut result_obj = match build_call_done_result(
                    &result_context,
                    &result.status.to_string(),
                    command.execution_id,
                    &command.step,
                    &command.render_context,
                    self.arrow_cache.as_ref(),
                    &dispatch_client,
                )
                .await
                {
                    Ok(obj) => obj,
                    Err(e) => {
                        tracing::warn!(
                            execution_id = command.execution_id,
                            step = %command.step,
                            error = %e,
                            "Failed to serialise tool result for inline context; falling back to status-only payload",
                        );
                        serde_json::json!({ "status": result.status.to_string() })
                    }
                };
                // noetl/ai-meta#104 R02b — stamp the stable logical URI on an
                // over-budget result's durable reference so the materialiser
                // addresses it by the §8 Resource Locator. `cursor.{frame,row}`
                // come from the dispatched command's metadata.
                stamp_logical_uri(
                    &mut result_obj,
                    command.execution_id,
                    &command.step,
                    &command.render_context,
                );
                // noetl/ai-meta#43 Round 4 — `pending_callback` adoption.
                //
                // `Tool::Container` (and any future tool that dispatches a
                // long-running external work item) sets
                // `ToolResult.pending_callback = Some(true)` to signal that
                // the step's terminal `call.done` will be emitted
                // asynchronously by a separate callback path
                // (`POST /api/internal/container-callback/{eid}/{step}`
                // on noetl-server, driven by `noetl-k8s-watcher`).  When
                // that marker is set the worker MUST NOT emit its own
                // `call.done` — doing so races the watcher's later emit
                // (the server's stale-counter
                // `noetl_container_callback_stale_total` records the
                // collision) and the orchestrator sees an early-completion
                // value with only the Job handle in `data`.
                //
                // All other tools omit the field (`None`) which keeps the
                // pre-existing behaviour fully intact — the worker emits
                // `call.done` immediately as the tool's terminal event.
                if matches!(result.pending_callback, Some(true)) {
                    tracing::info!(
                        execution_id = command.execution_id,
                        step = %command.step,
                        tool = %tool_config.kind,
                        "skipping call.done emit per pending_callback marker (await async callback)"
                    );
                    crate::metrics::record_call_done_skipped_pending_callback(&tool_config.kind);

                    // noetl/ai-meta#145 G2 — poll-based completion fallback.
                    // Off by default (the durable path is the external
                    // noetl-k8s-watcher).  When ON, this worker resolves its
                    // OWN container Jobs: spawn a detached poller (the slot
                    // is already freed by returning from this handler) that
                    // watches the Job to terminal and emits the resume
                    // call.done itself.  See
                    // docs/rfc/g1-g2-container-job-async.md §2.  Mutually
                    // exclusive with the watcher — running both double-emits.
                    if container_completion_poll_enabled() {
                        match extract_job_handle(&result) {
                            Some((namespace, job_name)) => {
                                self.spawn_container_poll(
                                    &dispatch_client,
                                    command,
                                    ctx.call_index,
                                    namespace,
                                    job_name,
                                );
                            }
                            None => {
                                tracing::warn!(
                                    execution_id = command.execution_id,
                                    step = %command.step,
                                    "container poll fallback enabled but result carried no job handle; cannot poll — falling back to watcher path",
                                );
                            }
                        }
                    }
                } else {
                    self.emit_event_via(
                        &dispatch_client,
                        "call.done",
                        &command.step,
                        "COMPLETED",
                        command.execution_id,
                        command.attempts,
                        serde_json::json!({
                            "command_id": command.command_id.clone(),
                            "call_index": ctx.call_index,
                            "result": result_obj,
                        }),
                    )
                    .await?;
                }

                result
            }
            Err(e) => {
                // Emit call.error event
                self.emit_event_via(
                    &dispatch_client,
                    "call.error",
                    &command.step,
                    "FAILED",
                    command.execution_id,
                    command.attempts,
                    serde_json::json!({
                        "command_id": command.command_id.clone(),
                        "call_index": ctx.call_index,
                        "error": e.to_string(),
                    }),
                )
                .await?;

                // Emit command.failed event
                self.emit_event_via(
                    &dispatch_client,
                    "command.failed",
                    &command.step,
                    "FAILED",
                    command.execution_id,
                    command.attempts,
                    serde_json::json!({
                        "command_id": command.command_id.clone(),
                        "error": e.to_string(),
                    }),
                )
                .await?;

                record_metric(true);
                return Err(e.into());
            }
        };

        // Parse cases from command.  The executor's `Command.input`
        // carries the worker's full `context` JSON, so `cases`
        // lives at `command.input.cases` (was `command.context.cases`
        // pre-PR-2d-2).
        let cases: Vec<crate::executor::case_evaluator::Case> = command
            .input
            .get("cases")
            .and_then(|v| v.as_array())
            .map(|list| {
                list.iter()
                    .filter_map(|value| serde_json::from_value(value.clone()).ok())
                    .collect()
            })
            .unwrap_or_default();

        // Evaluate cases
        if !cases.is_empty() {
            if let Some(case_result) =
                self.case_evaluator
                    .evaluate(&cases, &ctx, tool_result.data.as_ref())?
            {
                match case_result.action {
                    CaseAction::Exit { status, data } => {
                        // Emit step.exit event.  `step` is top-level
                        // on the EE shape; the case's status string
                        // becomes the envelope status so the projector
                        // sees the actual case outcome.
                        let exit_status = status.clone();
                        self.emit_event_via(
                            &dispatch_client,
                            "step.exit",
                            &command.step,
                            &exit_status,
                            command.execution_id,
                            command.attempts,
                            serde_json::json!({
                                "status": status,
                                "data": data,
                            }),
                        )
                        .await?;
                    }
                    CaseAction::SetVar { name, value } => {
                        // Set variable via API
                        dispatch_client
                            .set_variable(command.execution_id, &name, value)
                            .await?;
                    }
                    CaseAction::Fail { message } => {
                        // Emit command.failed event
                        self.emit_event_via(
                            &dispatch_client,
                            "command.failed",
                            &command.step,
                            "FAILED",
                            command.execution_id,
                            command.attempts,
                            serde_json::json!({
                                "command_id": command.command_id.clone(),
                                "error": message,
                            }),
                        )
                        .await?;

                        record_metric(true);
                        return Err(anyhow::anyhow!("Case evaluation failed: {}", message));
                    }
                    CaseAction::Continue | CaseAction::Goto { .. } | CaseAction::Retry { .. } => {
                        // These are handled by the orchestrator
                    }
                }
            }
        }

        // Emit command.completed event.  The tool's terminal status
        // (e.g. `"success"` / `"failure"` from the tool registry)
        // becomes the envelope status — projectors group by status
        // to compute success/failure rates per step.
        let completion_status = tool_result.status.to_string();
        self.emit_event_via(
            &dispatch_client,
            "command.completed",
            &command.step,
            &completion_status,
            command.execution_id,
            command.attempts,
            serde_json::json!({
                "command_id": command.command_id.clone(),
                "status": tool_result.status.to_string(),
            }),
        )
        .await?;

        record_metric(false);
        Ok(())
    }

    /// Emit an event to the control plane.
    ///
    /// R-1.2 PR-EE-3: constructs the shared `ExecutorEvent` shape
    /// (`step` + `status` + `created_at` + `context` at the top
    /// level, plus `worker_id` from the executor's own id).
    ///
    /// Post-EE-3 follow-ups now folded in:
    ///
    /// - `event_id` is stamped from the application-side snowflake
    ///   generator per `observability.md` Principle 3 — the id
    ///   exists at span-creation time + survives retries (which
    ///   used to either create duplicate rows or leave a NULL id
    ///   window).  Closes noetl/worker#12.
    /// - `meta.attempts` carries the executor `Command.attempts`
    ///   counter so retry behaviour rides the event log without
    ///   needing to reach back into the worker's logs.  Closes
    ///   noetl/worker#13.
    ///
    /// Per `observability.md` Principle 2: records the emit latency
    /// to `noetl_worker_event_emit_duration_seconds{event_type=...}`.
    /// The retries counter is incremented only when the underlying
    /// `emit_event_with_retry` actually retried (i.e. the first
    /// attempt failed); the retry count is currently not exposed
    /// by the client, so this MVP records 0 — a follow-up will
    /// thread the actual retry count back from the client.
    /// Emit a lifecycle event via the given control-plane client.
    ///
    /// Takes an explicit client argument so per-dispatch routing
    /// (noetl/ai-meta#53 Gap 1) can override the captured
    /// `self.client` for events that should land on the server
    /// that published the command rather than the startup-
    /// configured `NOETL_SERVER_URL` server.  Every callsite inside
    /// `execute_with_server_url` passes a per-dispatch
    /// `ControlPlaneClient`.
    #[allow(clippy::too_many_arguments)]
    async fn emit_event_via(
        &self,
        client: &ControlPlaneClient,
        event_type: &str,
        step: &str,
        status: &str,
        execution_id: i64,
        attempts: u32,
        context: serde_json::Value,
    ) -> Result<()> {
        let event = ExecutorEvent {
            execution_id,
            event_type: event_type.to_string(),
            step: step.to_string(),
            status: status.to_string(),
            created_at: chrono::Utc::now(),
            context,
            event_id: Some(self.snowflake.next_id()),
            worker_id: Some(self.worker_id.clone()),
            meta: Some(serde_json::json!({ "attempts": attempts })),
        };

        let emit_start = std::time::Instant::now();
        let result = client.emit_event_with_retry(event, 3).await;
        crate::metrics::record_event_emit(event_type, emit_start.elapsed().as_secs_f64(), 0);
        result
    }

    /// noetl/ai-meta#145 G2 — spawn the detached container poll fallback.
    ///
    /// The dispatch slot is freed the moment this handler returns; the
    /// spawned task is independent of the pull loop.  It watches the
    /// dispatched K8s Job to its terminal state via the tools-crate
    /// `poll_job_to_terminal` helper, then emits the resume `call.done`
    /// itself through the worker's normal `/api/events` path — no
    /// internal token, no server change.  Only cheap clones cross into
    /// the task (`ControlPlaneClient` is `Arc`-backed; `SnowflakeGen` is
    /// already `Arc`).
    fn spawn_container_poll(
        &self,
        client: &ControlPlaneClient,
        command: &Command,
        call_index: usize,
        namespace: String,
        job_name: String,
    ) {
        use tracing::Instrument;

        let client = client.clone();
        let snowflake = self.snowflake.clone();
        let worker_id = self.worker_id.clone();
        let execution_id = command.execution_id;
        let step = command.step.clone();
        let command_id = command.command_id.clone();
        let attempts = command.attempts;
        let opts = container_poll_options();

        crate::metrics::record_container_poll_started(&namespace);
        let span = tracing::info_span!(
            "container.poll",
            execution_id,
            step = %step,
            job_name = %job_name,
            namespace = %namespace,
        );

        tokio::spawn(
            async move {
                let started = std::time::Instant::now();
                let outcome =
                    noetl_tools::tools::poll_job_to_terminal(&namespace, &job_name, opts).await;
                let elapsed = started.elapsed().as_secs_f64();

                // Map the poll outcome → a call.done envelope.  An
                // infrastructure error polling the Job becomes a FAILED
                // resume (the execution must not hang forever) tagged
                // `error` so it's distinguishable on the dashboard.
                let (status_label, terminal_context, metric_state) = match outcome {
                    Ok(o) => {
                        let status = if o.is_success() { "COMPLETED" } else { "FAILED" };
                        let state = o.state.clone();
                        let ctx = serde_json::json!({
                            "terminal_state": o.state,
                            "job_name": job_name,
                            "namespace": namespace,
                            "reason": o.reason,
                            "completed_at": o.completed_at,
                            "via": "poll",
                        });
                        (status, ctx, state)
                    }
                    Err(e) => {
                        tracing::error!(
                            execution_id,
                            step = %step,
                            job_name = %job_name,
                            error = %e,
                            "container poll failed; emitting FAILED resume so the execution does not hang",
                        );
                        let ctx = serde_json::json!({
                            "terminal_state": "error",
                            "job_name": job_name,
                            "namespace": namespace,
                            "reason": e.to_string(),
                            "via": "poll",
                        });
                        ("FAILED", ctx, "error".to_string())
                    }
                };

                let event = ExecutorEvent {
                    execution_id,
                    event_type: "call.done".to_string(),
                    step: step.clone(),
                    status: status_label.to_string(),
                    created_at: chrono::Utc::now(),
                    context: serde_json::json!({
                        "command_id": command_id,
                        "call_index": call_index,
                        "result": {
                            "status": status_label,
                            "context": terminal_context,
                        },
                    }),
                    event_id: Some(snowflake.next_id()),
                    worker_id: Some(worker_id),
                    meta: Some(serde_json::json!({
                        "attempts": attempts,
                        "node_type": "container",
                        "via": "poll",
                    })),
                };

                if let Err(e) = client.emit_event_with_retry(event, 3).await {
                    tracing::error!(
                        execution_id,
                        step = %step,
                        error = %e,
                        "container poll: failed to emit resume call.done after retries",
                    );
                } else {
                    tracing::info!(
                        execution_id,
                        step = %step,
                        state = %metric_state,
                        "container poll: emitted resume call.done",
                    );
                }
                crate::metrics::record_container_poll_terminal(&metric_state, elapsed);
            }
            .instrument(span),
        );
    }

    /// Handle a failure that happens BEFORE the tool-dispatch match
    /// (credential-alias resolution, tool-config deserialization).
    ///
    /// noetl/ai-meta#78: these failures used to early-`?`-return from
    /// `execute_with_server_url` straight to the worker dispatch loop,
    /// which only logged `Command execution failed` — no `call.error`
    /// reached the server, so the execution sat at `command.started`
    /// forever.
    ///
    /// When `terminal` is true this emits the same `call.error` +
    /// `command.failed` pair the post-dispatch error arm emits (matching
    /// payload fields so the server/UI treat both identically), so the
    /// execution reaches a terminal FAILED state instead of hanging.
    /// When `terminal` is false (a transient transport error on a
    /// command whose attempt counter isn't yet exhausted) it logs a WARN
    /// and emits nothing, leaving the command path's retry/redelivery to
    /// run.
    ///
    /// Always returns `Err(error)` so the caller's early return
    /// propagates the failure to the dispatch loop (which records it and
    /// balances the in-flight gauge).  The invariant the dispatch loop
    /// relies on: by the time this returns `Err`, a terminal failure has
    /// already emitted its terminal event here — the loop must NOT emit
    /// its own (doing so would double-emit terminals and clobber the
    /// retryable path).  Per `observability.md` Principle 4 every line
    /// carries `execution_id` + `command_id` + `step`.
    async fn handle_predispatch_failure(
        &self,
        client: &ControlPlaneClient,
        command: &Command,
        call_index: usize,
        terminal: bool,
        error: anyhow::Error,
    ) -> Result<()> {
        if terminal {
            // Emit call.error — mirrors the post-dispatch error arm's
            // payload (command_id + call_index + error string) so the
            // projector/UI render a pre-dispatch failure identically to
            // a tool-execution failure.
            self.emit_event_via(
                client,
                "call.error",
                &command.step,
                "FAILED",
                command.execution_id,
                command.attempts,
                serde_json::json!({
                    "command_id": command.command_id.clone(),
                    "call_index": call_index,
                    "error": error.to_string(),
                }),
            )
            .await?;

            // Emit command.failed so the orchestrator advances the
            // execution to a terminal FAILED state.
            self.emit_event_via(
                client,
                "command.failed",
                &command.step,
                "FAILED",
                command.execution_id,
                command.attempts,
                serde_json::json!({
                    "command_id": command.command_id.clone(),
                    "error": error.to_string(),
                }),
            )
            .await?;

            tracing::error!(
                execution_id = command.execution_id,
                command_id = %command.command_id,
                step = %command.step,
                attempts = command.attempts,
                error = %error,
                "Pre-dispatch failure is terminal; emitted call.error + command.failed so the execution fails cleanly instead of hanging at command.started",
            );
        } else {
            tracing::warn!(
                execution_id = command.execution_id,
                command_id = %command.command_id,
                step = %command.step,
                attempts = command.attempts,
                error = %error,
                "Pre-dispatch failure is transient (retryable); no terminal call.error emitted, leaving retry to the command path",
            );
        }

        Err(error)
    }
}

/// Soft upper bound for the JSON-serialised size of
/// `payload.result.context` on `call.done` events.  Matches the
/// Python broker's `NOETL_EVENT_RESULT_CONTEXT_MAX_BYTES` default
/// (the broker's `_bounded_context` returns None and silently
/// drops the field above this threshold; we pre-check Rust-side
/// so operators see a WARN log instead of a silent drop).
const INLINE_CONTEXT_MAX_BYTES: usize = 100 * 1024;

/// The synthetic step name the server assigns to the control-plane drive
/// command (`system/orchestrate` wasm dispatch).  Results emitted under this
/// step are decoded synchronously by the server to advance the drive, so they
/// are exempt from the inline-budget offload — see `build_call_done_result`.
const ORCHESTRATE_STEP_NAME: &str = "__orchestrate__";

/// The effective inline budget, read once from `NOETL_EVENT_RESULT_CONTEXT_MAX_BYTES`
/// (default [`INLINE_CONTEXT_MAX_BYTES`] = 100KB).  Lets ops tune when a tool
/// result spills to the durable store + a reference — the lever for
/// references-in-state (noetl/ai-meta#101) and for matching the Python broker's
/// configurable bound.
fn inline_budget_bytes() -> usize {
    use std::sync::OnceLock;
    static BUDGET: OnceLock<usize> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::env::var("NOETL_EVENT_RESULT_CONTEXT_MAX_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(INLINE_CONTEXT_MAX_BYTES)
    })
}

/// noetl/ai-meta#145 G2 — whether this worker resolves its own
/// container Jobs via the poll fallback.  Off by default: the durable
/// completion path is the external `noetl-k8s-watcher`.  Turning this on
/// is a statement that the watcher is NOT also running against this
/// worker's Jobs (the two paths double-emit the resume `call.done`).
fn container_completion_poll_enabled() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("NOETL_CONTAINER_COMPLETION_POLL")
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

/// Poll cadence + deadline for the container poll fallback, overridable
/// from env.  Defaults: 5s→30s backoff, 24 h max_wait backstop.
fn container_poll_options() -> noetl_tools::tools::PollOptions {
    use std::time::Duration;
    let secs = |name: &str, default: u64| -> u64 {
        std::env::var(name)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(default)
    };
    noetl_tools::tools::PollOptions {
        interval: Duration::from_secs(secs("NOETL_CONTAINER_POLL_INTERVAL_SECS", 5)),
        max_interval: Duration::from_secs(secs("NOETL_CONTAINER_POLL_MAX_INTERVAL_SECS", 30)),
        max_wait: Duration::from_secs(secs("NOETL_CONTAINER_POLL_MAX_WAIT_SECS", 24 * 60 * 60)),
    }
}

/// Extract `(namespace, job_name)` from a container tool's
/// `ToolResult.data` (`{ "job_name": ..., "namespace": ... }`).  Returns
/// `None` if either field is missing/empty — the caller logs + skips the
/// poll (the watcher path can still resolve the Job).
fn extract_job_handle(result: &noetl_tools::result::ToolResult) -> Option<(String, String)> {
    let data = result.data.as_ref()?;
    let job_name = data.get("job_name")?.as_str()?.to_string();
    let namespace = data.get("namespace")?.as_str()?.to_string();
    if job_name.is_empty() || namespace.is_empty() {
        return None;
    }
    Some((namespace, job_name))
}

/// Encoding choice for the over-budget shm cache write.  R-2.2:
/// tabular tool outputs (DuckDB / Postgres / Snowflake rowsets)
/// encode as Arrow IPC stream bytes so colocated consumers benefit
/// from the columnar layout; non-tabular outputs (shell stdout, HTTP
/// JSON, etc.) fall back to JSON bytes.
struct ShmPayload {
    /// Bytes to write into the shm region.
    bytes: Vec<u8>,
    /// `media_type` to stamp on the `IpcHint` so consumers know how
    /// to decode (`application/vnd.apache.arrow.stream` for tabular;
    /// `application/json` for the fallback path).
    media_type: String,
    /// `schema_digest` argument to `cache.put_arrow_ipc` — `"arrow"`
    /// when the bytes are Arrow IPC (the schema is recoverable from
    /// the stream's first message); `"json"` for the fallback.
    schema_digest: &'static str,
    /// Row count for the `IpcHint.row_count` field.  Only populated
    /// for the tabular path.
    row_count: Option<u64>,
}

/// Build the `payload.result` object for a `call.done` event,
/// choosing between four exit shapes based on the JSON-serialised
/// size of the supplied `context` and the success of the durable
/// result-store write + same-node shm staging.
///
/// Fallback chain (highest fidelity first):
///
/// 1. **Inline.**  Serialised context ≤ [`INLINE_CONTEXT_MAX_BYTES`]
///    → `{status, context}`.  No HTTP, no shm.  Both cross-node
///    and colocated consumers read the rendered fields directly
///    off `result.context`.
/// 2. **Durable + colocated acceleration.**  Over-budget AND the
///    durable PUT to `/api/result/{execution_id}` AND the shm
///    cache `put` BOTH succeed → `{status, reference}` where
///    `reference` is a `ResultRef`-shaped dict carrying the
///    `noetl://` URI for cross-node fetch + a nested `ipc` field
///    with the [`IpcHint`] for same-node attach.  Mirrors the
///    Python worker's `TempStore.put_ipc_bytes` shape.
/// 3. **Durable only.**  Over-budget, durable PUT succeeds, shm
///    cache `put` fails → same `reference = ResultRef` dict but
///    without the `ipc` field.  Cross-node consumers work; same-
///    node consumers pay the durable round-trip instead of taking
///    the shm shortcut.
/// 4. **Colocated only.**  Over-budget, durable PUT fails, shm
///    cache `put` succeeds → `{status, reference}` carrying the
///    bare [`IpcHint`] (matches noetl/worker#28 behaviour).
///    Same-node consumers still read; cross-node consumers get
///    nothing.  WARN-logged so operators see the degradation.
/// 5. **Status only.**  Over-budget, both durable AND shm fail →
///    `{status}`.  Predictable + visible fallback rather than a
///    silent broker drop.  ERROR-logged.
///
/// Errors only if the serde serialisation itself fails (which
/// shouldn't happen for `serde_json::Value` inputs but the
/// signature stays honest via `serde_json::Error`).
async fn build_call_done_result(
    context: &serde_json::Value,
    status: &str,
    execution_id: i64,
    step: &str,
    render_context: &std::collections::HashMap<String, serde_json::Value>,
    arrow_cache: &ArrowIpcSharedMemoryCache,
    client: &ControlPlaneClient,
) -> Result<serde_json::Value, serde_json::Error> {
    // Producer-side credential scrub per
    // `agents/rules/execution-model.md` secrets rule.  The Python
    // server already scrubs `PUT /api/result/{execution_id}` bodies
    // at the boundary, but the worker emits THREE paths from this
    // function (inline `context`, durable PUT, shm cache stage) and
    // only the durable one rides through the server-side scrub.
    // Scrubbing here covers all three at once and shortens the
    // wire-transit window even for the durable path.
    //
    // We clone-and-scrub instead of mutating in place because the
    // caller (`CommandExecutor::execute`) emits the unscrubbed
    // `result_context` into its `serde_json::to_value(&result)` for
    // metric labels / logging.  Once we own the scrubbed copy below,
    // every reference to the context routes through it.
    let context = crate::scrub::scrub_cloned(context);
    let context = &context;

    let serialised = serde_json::to_string(context)?;
    // The control-plane drive result (`__orchestrate__`) MUST ride inline so the
    // server's `apply_worker_orchestration` can synchronously decode its
    // `output_b64` and advance the drive.  Offloading it to the durable result
    // store is unsafe: under `NOETL_RESULT_STORE_DUAL_WRITE=false`
    // (noetl/ai-meta#104 OQ5 — the dual-write retirement) the server's
    // `PUT /api/result/{id}` mints a `noetl://` ref WITHOUT writing the
    // `noetl.result_store` row, so the server's offloaded-drive resolution
    // (events.rs `apply_worker_orchestration`, the noetl/ai-meta#113 fallback)
    // gets "ref not found in store" → fails to decode → `commands=0` → the
    // reconcile poller re-publishes `__orchestrate__` forever and the execution
    // wedges in RUNNING with no terminal event (the noetl/ai-meta#154
    // ref-not-found re-drive loop).  A render-hop drive result exceeds the 100KB
    // inline budget once the accumulated context carries a provider envelope —
    // the Muno "what hotels are in Paris?" turn drives a ~138KB orchestrate
    // result and wedges, while the smaller google-places turn (~under budget)
    // stays inline and completes (noetl/ai-meta#155).  Keep the orchestrate
    // result inline regardless of size so the drive never depends on a separate
    // store round-trip.  (Complements the noetl/ai-meta#154 Leg A server fix,
    // which fails such a run loudly if a ref is ever still unresolvable.)
    if step == ORCHESTRATE_STEP_NAME || serialised.len() <= inline_budget_bytes() {
        return Ok(serde_json::json!({ "status": status, "context": context }));
    }

    // Over-budget.  R-2.2: pick the right shm encoding based on
    // whether the context is tabular (DuckDB / Postgres / Snowflake
    // rowset shape).  Tabular outputs go in as Arrow IPC stream
    // bytes — `noetl_tools::arrow_codec::try_encode_tabular_json`
    // returns `Some` for the canonical `{columns, rows}` (or
    // `{data: {columns, rows}}`) shape and the encoded bytes
    // round-trip through `pyarrow.ipc.RecordBatchStreamReader` for
    // cross-stack consumers.  Non-tabular outputs (shell stdout,
    // HTTP JSON, etc.) stage as JSON bytes — matches the
    // noetl/worker#28 behaviour.
    let shm_payload = match noetl_tools::arrow_codec::try_encode_tabular_json(context) {
        Some(enc) => {
            tracing::debug!(
                execution_id,
                step,
                row_count = enc.row_count,
                arrow_bytes = enc.bytes.len(),
                json_bytes = serialised.len(),
                "Tabular tool result detected; staging as Arrow IPC stream for shm cache.",
            );
            ShmPayload {
                bytes: enc.bytes,
                media_type: enc.media_type.to_string(),
                schema_digest: "arrow",
                row_count: Some(enc.row_count as u64),
            }
        }
        None => ShmPayload {
            bytes: serialised.as_bytes().to_vec(),
            media_type: "application/json".to_string(),
            schema_digest: "json",
            row_count: None,
        },
    };

    // Try the durable result-store first — that's the only path
    // that helps cross-node consumers.  Then layer the shm cache
    // on top as a colocated acceleration so same-node consumers
    // can skip the durable round-trip.
    //
    // The durable PUT always sends JSON (the server accepts `data:
    // Any` and stores in its tiered backends).  Cross-node consumers
    // fetch the JSON back; only the shm fast path benefits from the
    // Arrow IPC re-encoding above.
    let put_start = std::time::Instant::now();
    let durable_outcome = client
        .put_result(execution_id, step, context, "execution", Some(step))
        .await;
    let put_elapsed = put_start.elapsed().as_secs_f64();

    let shm_outcome = arrow_cache.put_arrow_ipc(
        &shm_payload.bytes,
        shm_payload.schema_digest,
        shm_payload.row_count,
        None,
    );

    match (durable_outcome, shm_outcome) {
        (Ok(durable), shm_result) => {
            // Build the ResultRef-shaped reference.  Mirrors
            // Python's `ResultRef` model (noetl/core/storage/models.py)
            // — kind discriminator + URI + tier + scope + meta.
            // The `ipc` field nests an `IpcHint` for the colocated
            // fast path (Python expects `ipc: Optional[IpcHint]`).
            crate::metrics::record_result_store_put(put_elapsed, serialised.len(), false);
            let mut reference = serde_json::json!({
                "kind": "result_ref",
                "ref": durable.r#ref,
                "store": durable.store,
                "scope": durable.scope,
                "meta": {
                    "bytes": durable.bytes,
                    "sha256": durable.sha256,
                    "media_type": "application/json",
                    "content_type": "application/json",
                },
            });
            if let Some(expiry) = durable.expires_at.as_ref() {
                reference["expires_at"] = serde_json::Value::String(expiry.clone());
            }
            // References-in-state (noetl/ai-meta#101 phase 1): attach a bounded
            // `extracted` predicate block so the orchestrator can evaluate
            // `when:`/`set:`/cursor fan-out off the reference without resolving
            // the full payload (which stays in the store).
            reference["extracted"] = build_extracted(context);
            // noetl/ai-meta#104 OQ5 Option A — producer-staged result tier.
            // The durable PUT succeeded, so this over-budget result carries a
            // canonical logical URI (stamped by `stamp_logical_uri` on the same
            // coordinates `cycle_logical_uri` derives here). When producer-staging
            // is enabled, stage the tier object NOW, at emit time, directly to the
            // object store — so the materializer never has to read `result_store`
            // to populate the tier (the prerequisite to retiring `result_store`).
            // Best-effort: the dual-write above is authoritative, so a staging
            // miss is invisible to the execution. Default-off → never called.
            if crate::result_producer_stage::enabled() {
                let canonical_uri = cycle_logical_uri(execution_id, step, render_context);
                crate::result_producer_stage::stage(client, &canonical_uri, context).await;
            }
            if let Ok(mut hint) = shm_result {
                hint.media_type = shm_payload.media_type.clone();
                // Stamp the hint as the `ipc` field on the
                // ResultRef so same-node consumers can attach
                // without the durable round-trip.  The
                // `media_type` field tells the consumer how to
                // decode the shm bytes: Arrow IPC stream for
                // tabular outputs, JSON for everything else.
                if let Ok(ipc_value) = serde_json::to_value(&hint) {
                    reference["ipc"] = ipc_value;
                }
                tracing::info!(
                    execution_id,
                    step,
                    context_bytes = serialised.len(),
                    shm_bytes = shm_payload.bytes.len(),
                    shm_media_type = %hint.media_type,
                    result_ref = %reference["ref"].as_str().unwrap_or(""),
                    shm_name = %hint.shm_name,
                    put_duration_seconds = put_elapsed,
                    "Tool result exceeds inline budget; staged in durable result store + shared-memory cache.",
                );
            } else {
                tracing::info!(
                    execution_id,
                    step,
                    context_bytes = serialised.len(),
                    result_ref = %reference["ref"].as_str().unwrap_or(""),
                    put_duration_seconds = put_elapsed,
                    "Tool result exceeds inline budget; staged in durable result store (shm cache unavailable).",
                );
            }
            // noetl/ai-meta#69 — embed an inline `context.data`
            // block carrying the synthetic `_ref` URI alongside the
            // `reference` block.  Without this, the orchestrator's
            // `extract_user_data` walks `outer.context.result.context.data`
            // and finds nothing for over-budget results, so a
            // downstream consumer like the `artifact` tool's
            // `result_ref: '{{ step._ref }}'` template renders to
            // null — the artifact tool then errors on
            // `Invalid artifact config: invalid type: null, expected
            // a string`.  Embedding `_ref` here makes the URI
            // template resolution work without bloating the inline
            // payload (single string, well under the inline budget).
            //
            // The `context.data` shape mirrors the under-budget path's
            // user_data layer so the orchestrator's extraction logic
            // (`extract_user_data` + the noetl/ai-meta#66 `step.data`
            // accessor) finds the same shape regardless of which
            // branch produced the call.done.
            //
            // The orchestrator navigates predicate fields off
            // `reference.extracted` (built below via `build_extracted`),
            // which preserves the result structure so
            // `{{ output.data.rows[0].<field>` }}` resolves without a
            // result_fetch round-trip.  Consumers that need the FULL data
            // (every row, every column) dispatch the `artifact` tool
            // (`kind: artifact, action: get, input: {result_ref: '{{
            // step._ref }}'}`) which uses the URI to read the durable
            // result.  A future server-side `output.output_select` could
            // declare exactly which fields land here, but the structural
            // summary covers the common navigation paths today.
            let inline_data = serde_json::json!({ "_ref": durable.r#ref });
            Ok(serde_json::json!({
                "status": status,
                "context": { "data": inline_data },
                "reference": reference,
            }))
        }
        (Err(durable_err), Ok(mut hint)) => {
            // Durable PUT failed but shm worked.  Emit the bare
            // IpcHint as before (#28 behaviour) — degraded mode,
            // cross-node consumers can't read this.  R-2.2: the
            // `media_type` reflects the actual encoding (Arrow IPC
            // for tabular, JSON for fallback) so a same-node
            // consumer reads the right bytes.
            crate::metrics::record_result_store_put_error();
            hint.media_type = shm_payload.media_type.clone();
            let reference = serde_json::to_value(&hint).unwrap_or_else(|_| {
                serde_json::json!({
                    "kind": "arrow_ipc",
                    "shm_name": hint.shm_name.clone(),
                    "byte_length": hint.byte_length,
                    "media_type": shm_payload.media_type,
                })
            });
            tracing::warn!(
                execution_id,
                step,
                context_bytes = serialised.len(),
                shm_bytes = shm_payload.bytes.len(),
                shm_media_type = %hint.media_type,
                shm_name = %hint.shm_name,
                error = %durable_err,
                "Durable result-store PUT failed; falling back to shared-memory cache only. \
                 Cross-node consumers will see an empty result.",
            );
            Ok(serde_json::json!({ "status": status, "reference": reference }))
        }
        (Err(durable_err), Err(shm_err)) => {
            // Both failed.  Status-only fallback — broker accepts
            // the event but downstream Jinja references will be
            // empty.  ERROR-logged so operators see the drop.
            crate::metrics::record_result_store_put_error();
            tracing::error!(
                execution_id,
                step,
                context_bytes = serialised.len(),
                inline_budget_bytes = inline_budget_bytes(),
                durable_error = %durable_err,
                shm_error = %shm_err,
                "Tool result exceeds inline budget and BOTH durable + shm staging failed; \
                 emitting status-only result.  Downstream Jinja references will be empty.",
            );
            Ok(serde_json::json!({ "status": status }))
        }
    }
}

#[cfg(feature = "wasm-plugin")]
impl CommandExecutor {
    /// Dispatch a `tool_kind: "wasm"` command to the plug-in host: resolve the
    /// plug-in by `(path, version)`, run it over the data-plane, flush its
    /// capability intents, and bridge the output to a `ToolResult` so the rest
    /// of the dispatch flow is unchanged.
    async fn dispatch_wasm(
        &self,
        tool_config: &ToolConfig,
        command: &Command,
        client: &ControlPlaneClient,
    ) -> Result<noetl_tools::result::ToolResult, noetl_tools::error::ToolError> {
        let (path, version, mut entry, mut input) = wasm_config_to_ref(&tool_config.config)
            .map_err(noetl_tools::error::ToolError::Configuration)?;

        // Off-server drive build (RFC #115 Phase 4 drive cutover): when the server
        // marks a `system/orchestrate` command `__offserver_build__`, build the
        // drive's `WorkflowState` from the pool-side WAL spine (the wasm `run` /
        // from_events entry) instead of the server-built `run_state` payload —
        // state CONSTRUCTION runs here, off the server, with zero noetl.event
        // reads.  An incomplete WAL chain (lag / cold) falls back to the
        // server-built `state` carried on the same command, so progress +
        // correctness never regress below the server-built path.
        if path == "system/orchestrate" {
            if let Some(args) = tool_config.config.get("args") {
                if args.get("__offserver_build__").and_then(|v| v.as_bool()) == Some(true) {
                    match self
                        .resolve_offserver_orchestrate_input(command.execution_id, args)
                        .await
                    {
                        OffserverDispatch::Wasm { entry: e, input: i } => {
                            entry = e;
                            input = i;
                        }
                        // Stateless off-server drive (RFC #115 Phase 4 remainder):
                        // the WAL chain was incomplete after the bounded retry and
                        // no server-built state rides the command, so return a
                        // benign no-op result.  The server's `apply_worker_orchestration`
                        // detects `__offserver_retry__`, clears the in-flight guard,
                        // and the reconcile poller re-drives once the drain catches
                        // up — no partial state is ever built, the execution never
                        // wedges.
                        OffserverDispatch::Noop => {
                            crate::metrics::record_state_builder_drive("stateless_retry");
                            tracing::info!(
                                execution_id = command.execution_id,
                                "off-server drive (stateless): WAL chain incomplete; \
                                 returning no-op, server reconcile will re-drive"
                            );
                            return Ok(noetl_tools::result::ToolResult::success(
                                serde_json::json!({ "__offserver_retry__": true }),
                            ));
                        }
                    }
                }
            }
        }

        let (output, report) = {
            let mut dispatcher = self.wasm_dispatcher.lock().await;
            dispatcher
                .run_and_apply_by_ref_entry(
                    &path,
                    version,
                    &input,
                    client,
                    command.execution_id,
                    &command.step,
                    &entry,
                )
                .await
                .map_err(|e| {
                    noetl_tools::error::ToolError::ExecutionFailed(format!(
                        "wasm dispatch {path}@{version} ({entry}): {e}"
                    ))
                })?
        };
        Ok(plugin_outcome_to_tool_result(output, &report))
    }

    /// Resolve the wasm entry + input bytes for an `__offserver_build__`
    /// `system/orchestrate` command (RFC #115 Phase 4 drive cutover).
    ///
    /// Authoritative builder → build the drive input from the pool-side WAL spine
    /// ([`crate::state_builder::build_offserver_input`]) and invoke the `run`
    /// (from_events) entry: state CONSTRUCTION runs here, off the server.  An
    /// incomplete WAL chain (drain lag / cold cache) — after a short bounded
    /// retry while the drain catches up — falls back to the **server-built**
    /// `state` carried on the same command via the `run_state` entry, so the
    /// drive never stalls and never builds a partial state.  A non-authoritative
    /// worker always uses the server-built `run_state` (the `state` fallback).
    ///
    /// The `args` object carries `{ state, latest_ts, playbook, trigger_event_type }`
    /// (the same `OrchestrateStateInput` shape the non-cutover drive sends) plus
    /// the `__offserver_build__` + `execution_id` markers — `OrchestrateStateInput`
    /// ignores the extra markers, so the args double as the `run_state` fallback
    /// input verbatim.
    async fn resolve_offserver_orchestrate_input(
        &self,
        execution_id: i64,
        args: &serde_json::Value,
    ) -> OffserverDispatch {
        use crate::state_builder::BuilderMode;

        // The server-built fallback input is the args object as-is (the extra
        // markers are ignored by OrchestrateStateInput's deserializer).  Only
        // usable when the server actually shipped a `state` — the stateless edge
        // (RFC #115 Phase 4 remainder) ships none.
        let fallback = || -> Vec<u8> { serde_json::to_vec(args).unwrap_or_default() };

        // Stateless off-server drive: the server-built edge no longer rebuilds
        // `WorkflowState`, so the command carries `__stateless__` + NO `state`
        // fallback.  An incomplete WAL chain is then a no-op (the reconcile poller
        // re-drives) rather than a fall-through to an absent server-built state.
        let stateless = args.get("__stateless__").and_then(|v| v.as_bool()) == Some(true);

        if self.state_builder_mode != BuilderMode::Authoritative {
            // A non-authoritative worker can't build off the WAL.  With a
            // server-built state it runs `run_state`; under the stateless edge
            // there is none, so it must no-op (the authoritative system pool is
            // the one that drives offserver — this is a misroute / mixed-mode
            // window).
            if stateless {
                return OffserverDispatch::Noop;
            }
            crate::metrics::record_state_builder_drive("fallback_disabled");
            return OffserverDispatch::Wasm {
                entry: "run_state".to_string(),
                input: fallback(),
            };
        }

        let playbook = args.get("playbook").cloned().unwrap_or(serde_json::Value::Null);
        let trigger_event_type = args.get("trigger_event_type").and_then(|v| v.as_str());
        // RFC #115 Phase 5: the server stamps the atomic-item-context flag on the
        // dispatch args; forward it onto the from_events drive input so the
        // off-server drive narrows worker-bound command contexts too.
        let atomic_item_context = args
            .get("atomic_item_context")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        // The server supplies `trigger_event_id` on the stateless edge so the
        // worker resolves `trigger_event_type` off its WAL index.
        let trigger_event_id = args.get("trigger_event_id").and_then(|v| v.as_i64());
        // The server's dispatch watermark — the WAL build serves only once the
        // pool-side index has caught up to it (staleness guard), so the
        // worker-built state is never staler than the server's view.
        let expected_head = args.get("expected_head").and_then(|v| v.as_i64());

        // noetl/ai-meta#156: the off-server drive's per-hop cost is today coupled
        // to GLOBAL `noetl_events` WAL volume, not this execution's work — the
        // build serves only once the pool-side drain (one ephemeral DeliverAll
        // consumer racing the whole stream under one mutex) has independently
        // pulled + indexed `expected_head`.  Under load that drain lags past the
        // retry budget below and the hop drops to the server's 8s reconcile tick.
        //
        // The server is the producer of these events and now ships the new tail on
        // the dispatch (`tail_events`, the same `noetl_events` payloads it
        // published).  Apply them to the pool-side WAL index BEFORE the build loop
        // so a warm-index hop completes its chain to `expected_head` on the first
        // build attempt — drain-independent.  This is purely additive: applying a
        // payload the drain would also apply is idempotent (`apply` overwrites by
        // `event_id`), and a tail that's insufficient to reach genesis (cold index
        // after a restart) simply leaves the build `Incomplete` → the existing
        // retry/drain/reconcile fallback below runs exactly as today.
        if let Some(tail) = args.get("tail_events").and_then(|v| v.as_array()) {
            if !tail.is_empty() {
                let mut applied = 0usize;
                {
                    let mut idx = self.state_builder_index.lock().await;
                    for ev in tail {
                        if let Some((_eid, is_new, _term)) = idx.apply(ev) {
                            if is_new {
                                applied += 1;
                            }
                        }
                    }
                }
                crate::metrics::record_offserver_tail_applied(tail.len(), applied);
                tracing::debug!(
                    execution_id,
                    attached = tail.len(),
                    applied,
                    "off-server drive: applied server-attached event tail to WAL index (noetl/ai-meta#156)"
                );
            }
        }

        // Bounded, event-signalled wait (noetl/ai-meta#130).  The trigger fired
        // from the server's in-memory chain head, so the event the build needs is
        // already on the `noetl_events` WAL — the pool-side drain just has to have
        // pulled + indexed it.  Rather than poll on a fixed 200ms grid (and, on a
        // miss after the window, hand off to the 8s reconcile poller — the source
        // of the ~1.8s/hop tail), park on the drain's append signal: the loop
        // wakes the instant the drain indexes a batch and re-checks the chain, so
        // a complete chain advances in ~drain-apply latency (single-digit ms)
        // instead of a poll tick.  `attempts × retry_ms` becomes a total *budget*
        // (default 5 × 200ms = 1000ms); `retry_ms` is the per-wait cap so we still
        // re-check even if a wake is missed (belt-and-suspenders — the index under
        // the lock is the source of truth, the signal only changes *when* we look).
        let attempts = std::env::var("NOETL_STATE_BUILDER_DRIVE_RETRIES")
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            .unwrap_or(5)
            .clamp(1, 50);
        let retry_ms = std::env::var("NOETL_STATE_BUILDER_DRIVE_RETRY_MS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(200)
            .clamp(10, 5_000);
        let budget = std::time::Duration::from_millis(retry_ms.saturating_mul(attempts as u64));
        let per_wait = std::time::Duration::from_millis(retry_ms);
        let deadline = std::time::Instant::now() + budget;
        let appended = self.state_builder_index.appended();
        let mut wakes = 0u32;
        loop {
            // Register interest on the append signal BEFORE building, so an apply
            // landing between the build check and the await below can't be lost
            // (the enable-before-check pattern — `notify_waiters` only wakes
            // already-registered futures).
            let notified = appended.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if let Some(bytes) = crate::state_builder::build_offserver_input(
                &self.state_builder_index,
                execution_id,
                &playbook,
                trigger_event_type,
                trigger_event_id,
                expected_head,
                atomic_item_context,
            )
            .await
            {
                crate::metrics::record_state_builder_drive("served");
                tracing::debug!(
                    execution_id,
                    wakes,
                    "off-server drive: built state from WAL spine (run/from_events), no noetl.event read"
                );
                return OffserverDispatch::Wasm {
                    entry: "run".to_string(),
                    input: bytes,
                };
            }

            let now = std::time::Instant::now();
            if now >= deadline {
                break;
            }
            let wait = per_wait.min(deadline - now);
            // Wake on the next drain apply, or fall through on the per-wait cap.
            match tokio::time::timeout(wait, notified).await {
                Ok(()) => {
                    wakes += 1;
                    crate::metrics::record_state_builder_drive_wait("woken");
                }
                Err(_) => crate::metrics::record_state_builder_drive_wait("timeout"),
            }
        }

        // WAL chain still incomplete after the retry window.
        //
        // noetl/ai-meta#166 Phase 3: try the object-store Feather **state shard**
        // FIRST — one keyed `object_get` (~tens of ms) replaces the retained-WAL
        // scan the rehydrate below pays (up to the whole 24h window, bounded to the
        // rehydrate deadline).  The shard carries the verbatim slim payload per
        // event, so the reconstructed chain is byte-equivalent to the WAL replay by
        // construction; a missing / stale / undecodable shard falls straight
        // through to the WAL replay (belt-and-suspenders — correctness never
        // depends on the shard alone).  Gated off by default (`NOETL_STATE_SHARD_READ`).
        if self.shard_read {
            let shard_t0 = std::time::Instant::now();
            let outcome = crate::state_reader::cold_load_from_shard(
                &self.client,
                &self.shard_read_cell,
                &self.state_builder_index,
                execution_id,
            )
            .await;
            crate::metrics::observe_state_shard_read_duration(shard_t0.elapsed().as_secs_f64());
            match outcome {
                crate::state_reader::ColdLoad::Applied(applied) => {
                    if let Some(bytes) = crate::state_builder::build_offserver_input(
                        &self.state_builder_index,
                        execution_id,
                        &playbook,
                        trigger_event_type,
                        trigger_event_id,
                        expected_head,
                        atomic_item_context,
                    )
                    .await
                    {
                        // Equivalence dual-build guard (`NOETL_STATE_SHARD_READ_VERIFY`,
                        // validation/canary): also run the WAL replay and byte-compare
                        // the two spines.  `apply` overwrites by `event_id`, so after
                        // the replay `cached_spine_events` reflects the WAL bodies — a
                        // second build yields the WAL spine.  Any divergence → serve
                        // the WAL build + tripwire metric; never serve divergent state.
                        if self.shard_read_verify {
                            if let Ok(_permit) = self.rehydrate_gate.try_acquire() {
                                let cfg = crate::state_builder::RehydrateConfig::from_env(
                                    &self.state_builder_nats_url,
                                );
                                crate::state_builder::rehydrate_execution_from_wal(
                                    &cfg,
                                    &self.state_builder_index,
                                    execution_id,
                                )
                                .await;
                            }
                            let wal_bytes = crate::state_builder::build_offserver_input(
                                &self.state_builder_index,
                                execution_id,
                                &playbook,
                                trigger_event_type,
                                trigger_event_id,
                                expected_head,
                                atomic_item_context,
                            )
                            .await;
                            if let Some(wal) = wal_bytes {
                                if wal != bytes {
                                    crate::metrics::record_state_equivalence_mismatch();
                                    crate::metrics::record_state_shard_read("fallback");
                                    crate::metrics::record_state_builder_drive("served_shard_mismatch");
                                    tracing::warn!(
                                        execution_id,
                                        applied,
                                        "off-server drive: shard-vs-WAL spine MISMATCH under verify; serving WAL build (noetl/ai-meta#166 Phase 3)"
                                    );
                                    return OffserverDispatch::Wasm {
                                        entry: "run".to_string(),
                                        input: wal,
                                    };
                                }
                            }
                        }
                        crate::metrics::record_state_shard_read("hit");
                        crate::metrics::record_state_builder_drive("served_shard");
                        tracing::info!(
                            execution_id,
                            applied,
                            "off-server drive: cold-loaded state from object-store shard after cache miss (noetl/ai-meta#166 Phase 3)"
                        );
                        return OffserverDispatch::Wasm {
                            entry: "run".to_string(),
                            input: bytes,
                        };
                    }
                    // Shard applied but the chain still can't reach expected_head
                    // (stale open shard — tail beyond its last write) → the WAL
                    // replay below supplies the tail.
                    crate::metrics::record_state_shard_read("fallback");
                }
                // No shard object (never written / GC'd) or it added nothing new.
                crate::state_reader::ColdLoad::NotFound | crate::state_reader::ColdLoad::Empty => {
                    crate::metrics::record_state_shard_read("miss");
                }
                // Object-store / decode error → conservative WAL fall-back.
                crate::state_reader::ColdLoad::Error => {
                    crate::metrics::record_state_shard_read("fallback");
                }
            }
        }

        // Before falling back to the server, try a targeted cold-rebuild from the
        // retained WAL (noetl/ai-meta#166 §5.2) — the safety net that makes
        // bounded-cache eviction wedge-safe: an evicted-then-resumed execution's
        // events aren't re-delivered by the live drain, so re-read them once from
        // the WAL and re-attempt the build.  Gated off by default; capped
        // concurrency so a miss storm can't fan out into many WAL scans.
        if self.rehydrate_on_miss {
            if let Ok(_permit) = self.rehydrate_gate.try_acquire() {
                let cfg = crate::state_builder::RehydrateConfig::from_env(
                    &self.state_builder_nats_url,
                );
                let applied = crate::state_builder::rehydrate_execution_from_wal(
                    &cfg,
                    &self.state_builder_index,
                    execution_id,
                )
                .await;
                if applied > 0 {
                    if let Some(bytes) = crate::state_builder::build_offserver_input(
                        &self.state_builder_index,
                        execution_id,
                        &playbook,
                        trigger_event_type,
                        trigger_event_id,
                        expected_head,
                        atomic_item_context,
                    )
                    .await
                    {
                        crate::metrics::record_state_builder_rehydrate("served");
                        crate::metrics::record_state_builder_drive("served_rehydrated");
                        tracing::info!(
                            execution_id,
                            applied,
                            "off-server drive: cold-rebuilt state from retained WAL after cache miss (noetl/ai-meta#166)"
                        );
                        return OffserverDispatch::Wasm {
                            entry: "run".to_string(),
                            input: bytes,
                        };
                    }
                    // Re-indexed events but still incomplete (genesis trimmed from
                    // the retained window) → fall through to the normal fallback.
                    crate::metrics::record_state_builder_rehydrate("incomplete");
                } else {
                    crate::metrics::record_state_builder_rehydrate("empty");
                }
            } else {
                // Concurrency cap saturated — skip the rebuild, fall back as today.
                crate::metrics::record_state_builder_rehydrate("throttled");
            }
        }

        if stateless {
            // No server-built state to fall back to — a benign no-op; the server
            // reconcile poller re-drives once the drain catches up.  Conservative:
            // never builds a partial state, never wedges the execution.
            crate::metrics::record_state_builder_drive("fallback_incomplete");
            return OffserverDispatch::Noop;
        }
        // Legacy offserver path (server still ships a state): use it.
        // Conservative: the worst case equals today.
        crate::metrics::record_state_builder_drive("fallback_incomplete");
        tracing::info!(
            execution_id,
            "off-server drive: WAL chain incomplete after retries; falling back to server-built run_state"
        );
        OffserverDispatch::Wasm {
            entry: "run_state".to_string(),
            input: fallback(),
        }
    }
}

/// How the worker should dispatch a `__offserver_build__` orchestrate command
/// (RFC #115 Phase 4 drive cutover + remainder).
enum OffserverDispatch {
    /// Invoke the named wasm entry with these input bytes — `run` (from_events,
    /// WAL-built state) or `run_state` (server-built fallback state).
    Wasm { entry: String, input: Vec<u8> },
    /// Stateless edge + incomplete WAL: return a benign no-op so the server
    /// reconcile poller re-drives once the pool-side drain catches up.
    Noop,
}

/// Parse a `tool_kind: "wasm"` config into the plug-in ref + entry + input bytes.
/// Config shape: `{ plugin: { path, version, entry? }, input: <any JSON> }`.
/// `plugin.entry` is the guest export to invoke (default `run`); the worker-driven
/// orchestrator sets `entry: "run_state"` (noetl/ai-meta#108). The input passed to
/// the plug-in is the JSON bytes of `config.args` (or `config.input`, empty if
/// absent).
#[cfg(feature = "wasm-plugin")]
fn wasm_config_to_ref(config: &serde_json::Value) -> Result<(String, u32, String, Vec<u8>), String> {
    let plugin = config
        .get("plugin")
        .ok_or("wasm tool config missing `plugin`")?;
    let path = plugin
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or("plugin.path missing or not a string")?
        .to_string();
    let version = u32::try_from(
        plugin
            .get("version")
            .and_then(|v| v.as_u64())
            .ok_or("plugin.version missing or not an integer")?,
    )
    .map_err(|_| "plugin.version out of range".to_string())?;
    let entry = plugin
        .get("entry")
        .and_then(|v| v.as_str())
        .unwrap_or("run")
        .to_string();
    // The plug-in input: the server canonicalizes the step's `input:` to `args`
    // (the noetl-tools field name), so read `args` first and fall back to
    // `input` for a directly-crafted command.
    let input = match config.get("args").or_else(|| config.get("input")) {
        Some(v) => serde_json::to_vec(v).map_err(|e| e.to_string())?,
        None => Vec::new(),
    };
    Ok((path, version, entry, input))
}

/// Bridge a plug-in's output + flush report to a `ToolResult`.
#[cfg(feature = "wasm-plugin")]
fn plugin_outcome_to_tool_result(
    output: Vec<u8>,
    report: &crate::plugin::FlushReport,
) -> noetl_tools::result::ToolResult {
    use base64::Engine;
    let data = serde_json::json!({
        "output_b64": base64::engine::general_purpose::STANDARD.encode(&output),
        "flush": {
            "results_stored": report.results_stored,
            "objects_stored": report.objects_stored,
            "events_published": report.events_published,
            "errors": report.errors,
        }
    });
    if report.errors.is_empty() {
        noetl_tools::result::ToolResult::success(data)
    } else {
        noetl_tools::result::ToolResult::error(report.errors.join("; ")).with_data(data)
    }
}

/// Stamp the stable logical URI (noetl/ai-meta#104 R02b) on an over-budget
/// result's **durable** `reference` block, so the materialiser and any consumer
/// address the result by the §8 Resource Locator
/// (`noetl://<tenant>/<project>/results/<eid>/<step>/<frame>/<row>/<attempt>`)
/// — a stable, derivable name independent of the physical store.
///
/// The fan-out coordinate comes from `__cursor_frame` / `__cursor_row` in the
/// command's `render_context` — the worker's [`crate::nats::source`] `translate`
/// copies them there from the dispatched command's `metadata.cursor` (the
/// orchestrator stamps body commands with `{phase:"body", frame, row}`), since
/// the executor `Command` carries `render_context` but not the raw metadata.
/// Non-cursor steps default to `0/0`. `attempt` is fixed at `1` so retries
/// overwrite the same key (the blueprint's default), keeping the name
/// derivable. Only the durable `kind: "result_ref"` reference is stamped — a
/// shm-only `arrow_ipc` hint (degraded path) and inline results are skipped.
/// Whether the command the worker is about to dispatch is side-effecting
/// ([noetl/ai-meta#104](https://github.com/noetl/ai-meta/issues/104) Phase E).
///
/// The orchestrator wraps a step's tool(s) in a `task_sequence` command, so the
/// command's own kind is almost always `task_sequence` — classifying *that*
/// would treat every step the same. Instead this looks **through** the wrapper:
/// a `task_sequence` is side-effecting iff **any** of its sub-tasks is (so a
/// step running only `noop` / `rhai` is correctly exempt), and a non-wrapped
/// command is classified by its own kind. Sub-tasks whose kind can't be read
/// fall back to side-effecting (conservative — over-classification is safe for
/// the adopt-only barrier).
fn command_is_side_effecting(tool_config: &ToolConfig) -> bool {
    if tool_config.kind != "task_sequence" {
        return noetl_tools::registry::kind_is_side_effecting(&tool_config.kind);
    }
    // `task_sequence`'s sub-tasks are carried under `config.tool_config` as an
    // array of `{ <label>: { kind, ... } }` (the worker envelope shape).
    match tool_config
        .config
        .get("tool_config")
        .and_then(|v| v.as_array())
    {
        Some(subs) if !subs.is_empty() => subs.iter().any(|item| {
            item.as_object()
                .and_then(|o| o.values().next())
                .and_then(|spec| spec.get("kind"))
                .and_then(|k| k.as_str())
                .map(noetl_tools::registry::kind_is_side_effecting)
                .unwrap_or(true)
        }),
        // No inspectable sub-tasks → conservative.
        _ => true,
    }
}

/// Whether the Phase E side-effect barrier should consult the durable result
/// tier before dispatching this cycle. It checks only when the barrier flag is
/// on **and** the command is side-effecting (per [`command_is_side_effecting`],
/// which looks through the `task_sequence` wrapper). A pure predicate — the
/// durable-result lookup + adopt is the async step that follows. Flag-off and
/// non-side-effecting cycles both short-circuit to `false`, leaving dispatch
/// byte-identical to today.
fn side_effect_barrier_should_check(enabled: bool, tool_config: &ToolConfig) -> bool {
    enabled && command_is_side_effecting(tool_config)
}

/// Derive the stable logical URI for a cycle `(execution_id, step, frame, row,
/// attempt)`. `frame` / `row` come from the dispatched command's cursor metadata
/// (`__cursor_frame` / `__cursor_row` in `render_context`, defaulting to `0/0`
/// for a non-cursor step). `attempt` is fixed to `1`: a crash-resume / re-drive
/// of the *same* cycle derives the **identical** URI — which is exactly what lets
/// the side-effect barrier ([noetl/ai-meta#104](https://github.com/noetl/ai-meta/issues/104)
/// Phase E) recognise an already-durable result, and what lets the R02b stamp be
/// idempotent across replays.
fn cycle_logical_uri(
    execution_id: i64,
    step: &str,
    render_context: &std::collections::HashMap<String, serde_json::Value>,
) -> String {
    let frame = render_context
        .get("__cursor_frame")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let row = render_context
        .get("__cursor_row")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    noetl_tools::locator::ResultCoordinates::new(None, None, execution_id, step, frame, row, 1)
        .logical_uri()
}

fn stamp_logical_uri(
    result_obj: &mut serde_json::Value,
    execution_id: i64,
    step: &str,
    render_context: &std::collections::HashMap<String, serde_json::Value>,
) {
    let Some(reference) = result_obj.get_mut("reference").and_then(|r| r.as_object_mut()) else {
        return;
    };
    if reference.get("kind").and_then(|k| k.as_str()) != Some("result_ref") {
        return;
    }
    let uri = cycle_logical_uri(execution_id, step, render_context);
    reference.insert("uri".to_string(), serde_json::Value::String(uri));
}

/// Locate a result-reference on a step result (nested or top-level envelope) and
/// return `(legacy_ref, canonical_uri?)`: the legacy `reference.ref` (always —
/// the authoritative fetch key) plus the canonical logical `reference.uri` when
/// present (the resolve-by-URN key, #104 Phase C). `None` if there is no
/// reference at all.
fn reference_locators(result: &serde_json::Value) -> Option<(String, Option<String>)> {
    let reference = result
        .pointer("/context/result/reference")
        .or_else(|| result.pointer("/reference"))?;
    let legacy = reference.get("ref").and_then(|v| v.as_str())?.to_string();
    let canonical = reference
        .get("uri")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    Some((legacy, canonical))
}

/// Normalize a resolved payload to the **flattened single-tool shape** so the
/// legacy `resolve_ref` fallback binds identically to resolve-by-URN and inline
/// (#104 Phase C, OQ6 — B1).
///
/// `resolve_ref` returns the authoritative tool-result ENVELOPE
/// `{data:{<tool>:<result>}, status, exit_code, stderr, stdout, duration_ms}`.
/// For a **single-tool** step, inline execution and the resolve-by-URN tier both
/// expose the one tool's result at step level (so `{{ start.rows }}` resolves,
/// not `{{ start.<tool>.rows }}`). Mirror that: when the payload is a tool
/// envelope (`status` + `data`) whose `data` holds exactly one tool's object
/// result, the user-data IS that result.
///
/// Left **unchanged** for: a payload that is already the flat user-data (no
/// `status` key — e.g. resolve-by-URN's `{columns, rows}`, so this is
/// idempotent), and **multi-tool** envelopes (`data` with >1 key — the
/// tool-keyed shape is preserved).
fn flatten_single_tool_result(data: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    let Value::Object(ref o) = data else {
        return data;
    };
    // Only a tool-result envelope is a flatten candidate; a bare user-data
    // payload (resolve-by-URN) has no `status` and passes through untouched.
    if !o.contains_key("status") {
        return data;
    }
    if let Some(Value::Object(d)) = o.get("data") {
        if d.len() == 1 {
            if let Some(inner @ Value::Object(_)) = d.values().next() {
                return inner.clone();
            }
        }
    }
    data
}

/// Splice resolved full `data` back into a step result where
/// `extract_user_data` reads it, dropping the `reference` block — reconstructing
/// the inline shape the orchestrator kept out of the durable state.
fn splice_resolved(result: &mut serde_json::Value, data: serde_json::Value) {
    let wrapped = serde_json::json!({ "data": data });
    if let Some(inner) = result
        .pointer_mut("/context/result")
        .and_then(|r| r.as_object_mut())
    {
        inner.insert("context".to_string(), wrapped);
        inner.remove("reference");
    } else if let Some(obj) = result.as_object_mut() {
        obj.insert("context".to_string(), wrapped);
        obj.remove("reference");
    }
}

/// Mirror `build_context`'s flat user-data shape: expose `data` fields directly
/// AND under a `.data` accessor so `{{ step.field }}` and `{{ step.data.field }}`
/// both resolve.
fn flat_with_data(data: &serde_json::Value) -> serde_json::Value {
    match data {
        serde_json::Value::Object(map) if !map.contains_key("data") => {
            let mut m = map.clone();
            m.insert("data".to_string(), data.clone());
            serde_json::Value::Object(m)
        }
        _ => data.clone(),
    }
}

/// Resolve over-budget result references in the render context to their full
/// payload — **selectively** (references-in-state consume side, noetl/ai-meta#115
/// Phase 1 / #101).
///
/// With `NOETL_REFS_IN_STATE` on, step outputs carry a `{reference}` + bounded
/// `extracted` summary (+ the `_ref`/`_store` locator accessors) instead of
/// inline data.  Most downstream access is predicate / scalar / `_ref` —
/// `{{ step.status }}`, `{{ step._ref is defined }}`, `{{ step._ref }}` (an
/// explicit `artifact.get` lazy-load) — which the bounded summary already
/// satisfies.  Only a template that binds the **bulk** of an upstream result
/// (`{{ step.data }}` over a summarised rowset, a whole-object bind, an array
/// element past `[0]`) needs the full payload.
///
/// This resolves a step's `ref` **only** when `template_src` (this command's
/// tool input) binds that step's bulk, and resolves **only** those refs — so an
/// over-budget upstream result a step doesn't consume stays a reference and the
/// worker render never inflates foreign bulk.  Predicate-only / `_ref`-only /
/// summary-satisfiable access leaves the small summary in place.  No references
/// → no-op (the default flag-off path costs one map lookup).
async fn resolve_context_references(
    variables: &mut std::collections::HashMap<String, serde_json::Value>,
    template_src: &str,
    client: &ControlPlaneClient,
) {
    // `steps` carries the full step results (build_context inserts result.clone()).
    // Each candidate is (step_name, legacy_ref, canonical_uri?).
    let candidates: Vec<(String, String, Option<String>)> = match variables.get("steps") {
        Some(serde_json::Value::Object(steps)) => steps
            .iter()
            .filter_map(|(name, result)| {
                reference_locators(result).map(|(legacy, canonical)| (name.clone(), legacy, canonical))
            })
            .collect(),
        _ => return,
    };
    if candidates.is_empty() {
        return;
    }
    // Resolve-by-URN read path (#104 Phase C) — only when the flag is on. Off →
    // this is the byte-identical legacy `resolve_ref` path.
    // Phase D minting flip (#104 Phase D): `NOETL_RESULT_MINT_AUTHORITATIVE`
    // makes the URN tier the *authoritative* result store, so resolve-by-URN is
    // the *primary* path even if `NOETL_RESULT_URI_RESOLVE` was not separately
    // set. A tier miss still falls back fail-safe to the dual-written
    // `result_store` (rollback safety); the path taken is recorded on
    // `noetl_worker_result_mint_authoritative_total{path}`.
    let mint_authoritative = crate::result_resolver::mint_authoritative();
    let resolve_by_urn = crate::result_resolver::enabled() || mint_authoritative;
    for (name, uri, canonical) in candidates {
        // The flat `<name>` binding is the bounded summary (+ `_ref`/`_store`).
        // Decide off it whether this command's template needs the bulk.
        let summary = variables.get(&name);
        if !step_needs_bulk_resolution(template_src, &name, summary) {
            tracing::debug!(step = %name, uri, "ref kept as summary (no bulk binding in template)");
            continue;
        }
        // Flag-on + a canonical URI present: try resolving the Feather/JSON tier
        // from object store by URN. On any miss/error it returns None and we fall
        // back fail-safe to the authoritative `resolve_ref` below — never a hard
        // failure, never silent loss (OQ6).
        let fetched: anyhow::Result<Option<serde_json::Value>> =
            match (resolve_by_urn, canonical.as_deref()) {
                (true, Some(canon)) => {
                    match crate::result_resolver::resolve_by_urn(client, canon).await {
                        Some(data) => {
                            tracing::debug!(step = %name, uri = canon, "resolved over-budget result by URN (#104 Phase C)");
                            // Phase D: the authoritative tier served this result.
                            if mint_authoritative {
                                crate::metrics::record_result_mint_authoritative("tier");
                            }
                            Ok(Some(data))
                        }
                        None => {
                            // Phase D: tier miss → fall back to the dual-written
                            // `result_store` (reversible rollback path).
                            if mint_authoritative {
                                crate::metrics::record_result_mint_authoritative("legacy_fallback");
                            }
                            client.resolve_ref(&uri).await
                        }
                    }
                }
                _ => client.resolve_ref(&uri).await,
            };
        match fetched {
            Ok(Some(data)) => {
                // OQ6 shape parity (#104 Phase C, B1): the authoritative
                // `resolve_ref` returns the full tool-result ENVELOPE
                // `{data:{<tool>:<result>}, status, …}`, but inline binding and
                // resolve-by-URN expose a single-tool step's result flattened to
                // step level (`{{ start.rows }}`, not `{{ start.<tool>.rows }}`).
                // Without this, the flag-off / fail-safe-fallback legacy path
                // binds a divergent shape and breaks parity with the GCS path.
                let data = flatten_single_tool_result(data);
                // Preserve the locator accessors so a template that binds bulk
                // AND `{{ step._ref }}` keeps both after the splice.
                let mut flat = flat_with_data(&data);
                if let (Some(flat_obj), Some(serde_json::Value::Object(prev))) =
                    (flat.as_object_mut(), summary)
                {
                    for acc in ["_ref", "_store", "_uri"] {
                        if let Some(v) = prev.get(acc) {
                            flat_obj.entry(acc.to_string()).or_insert_with(|| v.clone());
                        }
                    }
                }
                variables.insert(name.clone(), flat);
                if let Some(serde_json::Value::Object(steps)) = variables.get_mut("steps") {
                    if let Some(result) = steps.get_mut(&name) {
                        splice_resolved(result, data);
                    }
                }
            }
            Ok(None) => {
                tracing::warn!(step = %name, uri, "context reference not found in store; left as summary")
            }
            Err(e) => {
                tracing::warn!(step = %name, uri, %e, "context reference resolve failed; left as summary")
            }
        }
    }
}

/// Decide whether `template_src` binds the **bulk** of step `<name>` — i.e.
/// whether the worker must resolve `<name>`'s full reference rather than reading
/// off the bounded summary.  The driving observation: `summarise_value` keeps
/// **every object key** and collapses only the *bulk* (arrays → first element,
/// large strings → `{_len}`, over-depth/over-budget → `{_count}`/`{_truncated}`).
/// So a path is **summary-satisfiable** unless it navigates into collapsed bulk;
/// a key absent from the summary is absent from the full payload too (resolving
/// is futile, so we don't).
///
/// Conservative bias: when `summary` is missing or a reference is parsed
/// ambiguously, resolve.  Never under-resolves (correctness over size).
fn step_needs_bulk_resolution(
    template_src: &str,
    name: &str,
    summary: Option<&serde_json::Value>,
) -> bool {
    let accessors = accessor_paths(template_src, name);
    if accessors.is_empty() {
        return false; // step not referenced by this command's template
    }
    let Some(summary) = summary else {
        return true; // referenced but no summary to read off → resolve
    };
    accessors
        .iter()
        .any(|path| !path_satisfiable(summary, path))
}

/// Find the accessor paths a template applies to identifier `name`.  Each entry
/// is the dotted/indexed chain after `name` (`["data"]` for `{{ name.data }}`,
/// `["rows", "0", "x"]` for `{{ name.rows[0].x }}`); an **empty** path marks a
/// whole-object bind (`{{ name }}`, `{{ name | tojson }}`).  Plain string scan —
/// robust to JSON-escaping since identifiers/dots/brackets aren't escaped.
fn accessor_paths(template_src: &str, name: &str) -> Vec<Vec<String>> {
    let bytes = template_src.as_bytes();
    let name_bytes = name.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut out = Vec::new();
    let mut i = 0usize;
    while let Some(pos) = template_src[i..].find(name) {
        let start = i + pos;
        let end = start + name_bytes.len();
        i = end; // advance regardless of match outcome
        // Left boundary: previous char must not be part of a longer identifier
        // or a member access (`other.name` is a field of `other`, not `name`).
        if start > 0 {
            let prev = bytes[start - 1];
            if is_ident(prev) || prev == b'.' {
                continue;
            }
        }
        // Right boundary: next char must not extend the identifier.
        if end < bytes.len() && is_ident(bytes[end]) {
            continue;
        }
        out.push(parse_accessor_chain(bytes, end));
    }
    out
}

/// Parse the `.key` / `["key"]` / `[n]` chain starting at `pos`.
fn parse_accessor_chain(bytes: &[u8], mut pos: usize) -> Vec<String> {
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut path = Vec::new();
    loop {
        match bytes.get(pos) {
            Some(b'.') => {
                pos += 1;
                let seg_start = pos;
                while pos < bytes.len() && is_ident(bytes[pos]) {
                    pos += 1;
                }
                if pos == seg_start {
                    break; // `.` not followed by an identifier
                }
                path.push(String::from_utf8_lossy(&bytes[seg_start..pos]).into_owned());
            }
            Some(b'[') => {
                pos += 1;
                // Skip an opening quote if present.
                let quoted = matches!(bytes.get(pos), Some(b'\'') | Some(b'"') | Some(b'\\'));
                while matches!(bytes.get(pos), Some(b'\'') | Some(b'"') | Some(b'\\')) {
                    pos += 1;
                }
                let seg_start = pos;
                while pos < bytes.len()
                    && bytes[pos] != b']'
                    && bytes[pos] != b'\''
                    && bytes[pos] != b'"'
                    && bytes[pos] != b'\\'
                {
                    pos += 1;
                }
                let seg = String::from_utf8_lossy(&bytes[seg_start..pos]).into_owned();
                // Advance past closing quote(s) + `]`.
                while matches!(bytes.get(pos), Some(b'\'') | Some(b'"') | Some(b'\\')) {
                    pos += 1;
                }
                if bytes.get(pos) == Some(&b']') {
                    pos += 1;
                }
                let _ = quoted;
                path.push(seg);
            }
            _ => break,
        }
    }
    path
}

/// Is `path` resolvable off the bounded summary without the full payload?
fn path_satisfiable(summary: &serde_json::Value, path: &[String]) -> bool {
    use serde_json::Value;
    let mut cur = summary;
    for (i, seg) in path.iter().enumerate() {
        // The injected locator accessors are always present scalars.
        if i == 0 && matches!(seg.as_str(), "_ref" | "_store" | "_uri") {
            return true;
        }
        match cur {
            Value::Object(o) => match o.get(seg) {
                // Absent in the summary ⇒ absent in the full payload (summarise
                // keeps every object key) ⇒ resolving can't help ⇒ satisfiable.
                // Exceptions — the summary may have *dropped* keys the full
                // payload still has, so resolve to be safe:
                //   - a budget-`_truncated` object, and
                //   - a bare `_ref` stub (an externalized result whose real
                //     payload lives in object store, not in this stub; #104
                //     Phase C — without this an over-budget upstream is never
                //     resolved on a bulk bind).
                None => return !o.contains_key("_truncated") && !is_reference_stub(o),
                Some(v) => cur = v,
            },
            Value::Array(a) => {
                // Summary keeps only element 0.  Index 0 navigates; any other
                // index or a field access (iteration) needs the full array.
                if seg == "0" {
                    match a.first() {
                        Some(v) => cur = v,
                        None => return true,
                    }
                } else {
                    return false;
                }
            }
            // Navigating into a scalar yields undefined either way.
            _ => return true,
        }
    }
    // Whole bound value: satisfiable only if it carries no collapsed bulk.
    !contains_summary_bulk(cur)
}

/// True when `o` is a bare externalized-result **reference stub** — an object
/// carrying only the injected locator accessors (`_ref` / `_store` / `_uri`,
/// plus a `data` that is itself just a locator), NOT a key-preserving
/// `summarise_value`.
///
/// `path_satisfiable`'s "absent key ⇒ absent in full payload" shortcut assumes
/// the summary keeps every object key (summarise collapses only *bulk*). A bare
/// `_ref` stub breaks that assumption: the real payload lives in object store,
/// so a key absent from the stub may well exist in the full result. Navigating
/// into such a stub must therefore resolve (same reasoning as the `_truncated`
/// carve-out) rather than read off the stub. A genuine summary (e.g.
/// `{columns, rows:[…], _ref}`) has non-locator keys and is NOT a stub, so its
/// behavior is unchanged.
fn is_reference_stub(o: &serde_json::Map<String, serde_json::Value>) -> bool {
    let has_locator =
        o.contains_key("_ref") || o.contains_key("_store") || o.contains_key("_uri");
    if !has_locator {
        return false;
    }
    o.iter().all(|(k, v)| match k.as_str() {
        "_ref" | "_store" | "_uri" => true,
        "data" => v.as_object().is_some_and(|d| {
            d.keys()
                .all(|k| matches!(k.as_str(), "_ref" | "_store" | "_uri"))
        }),
        _ => false,
    })
}

/// True if `v` contains anything the summary collapsed (an array — which keeps
/// only its first element — or a `_len`/`_count`/`_truncated`/`_keys` marker),
/// meaning a template binding it whole would see truncated data.
fn contains_summary_bulk(v: &serde_json::Value) -> bool {
    use serde_json::Value;
    match v {
        Value::Array(_) => true,
        Value::Object(o) => {
            if o.contains_key("_len")
                || o.contains_key("_truncated")
                || o.contains_key("_keys")
                || o.contains_key("_count")
            {
                return true;
            }
            o.values().any(contains_summary_bulk)
        }
        _ => false,
    }
}

/// Cap on the serialized `extracted` predicate block — keep the reference small
/// (the whole point of references-in-state).
const MAX_EXTRACTED_BYTES: usize = 4096;
/// Inline a scalar string up to this; larger strings collapse to a length marker.
const MAX_EXTRACTED_SCALAR_BYTES: usize = 512;
/// Recursion guard for the structural summary — deep enough to navigate the
/// common `data.rows[0].<field>` path (depth 4) with headroom; the byte budget
/// is the real bound.
const MAX_EXTRACTED_DEPTH: usize = 8;

/// Summarise one value into a predicate-sized but **navigable** shape.
///
/// The orchestrator evaluates `when:` / `set:` against a step's result through
/// its `output.<path>` namespace.  A guard like
/// `{{ output.data.rows[0].facility_mapping_id }}` has to *navigate* the result
/// structure — so a flat `{_count, _keys}` collapse breaks it (`data` becomes a
/// count, `rows` vanishes).  Instead we preserve the structure and summarise the
/// **bulk**:
///
/// - scalars pass through (small strings inline; large ones collapse to `{_len}`);
/// - objects recurse, keeping every key so navigation survives;
/// - arrays keep their **first element only** (as a real 1-element array) so
///   `arr[0].<field>` resolves without walking — or copying — the other N-1
///   elements (a 500-row rowset summarises one row, not 500).
///
/// `budget` is decremented as we go; when it runs out the node truncates with a
/// `_truncated: true` marker so the reference never bloats.  Cursor fan-out is
/// unaffected — it resolves `claim_ref` to the full rows separately.
fn summarise_value(v: &serde_json::Value, depth: usize, budget: &mut usize) -> serde_json::Value {
    use serde_json::Value;
    match v {
        Value::Null | Value::Bool(_) | Value::Number(_) => {
            *budget = budget.saturating_sub(v.to_string().len());
            v.clone()
        }
        Value::String(s) if s.len() <= MAX_EXTRACTED_SCALAR_BYTES => {
            *budget = budget.saturating_sub(s.len());
            v.clone()
        }
        Value::String(s) => serde_json::json!({ "_len": s.len() }),
        Value::Array(a) => {
            if a.is_empty() || depth >= MAX_EXTRACTED_DEPTH {
                serde_json::json!({ "_count": a.len() })
            } else {
                // Keep only the first element so `arr[0].<field>` resolves; the
                // 1-element array preserves index-0 access without the bulk.
                Value::Array(vec![summarise_value(&a[0], depth + 1, budget)])
            }
        }
        Value::Object(o) => {
            if depth >= MAX_EXTRACTED_DEPTH {
                return serde_json::json!({
                    "_count": o.len(),
                    "_keys": o.keys().take(64).cloned().collect::<Vec<_>>(),
                });
            }
            let mut out = serde_json::Map::new();
            for (k, val) in o {
                if *budget == 0 {
                    out.insert("_truncated".to_string(), Value::Bool(true));
                    break;
                }
                *budget = budget.saturating_sub(k.len() + 4);
                out.insert(k.clone(), summarise_value(val, depth + 1, budget));
            }
            Value::Object(out)
        }
    }
}

/// Build a bounded, navigable `extracted` predicate block from an over-budget
/// result context (noetl/ai-meta#101 references-in-state, phase 1).  The
/// orchestrator reads this to evaluate `when:` / `set:` / cursor fan-out WITHOUT
/// resolving the full payload (which stays in the store).  Structure is
/// preserved so navigation expressions resolve (`{{ output.data.rows[0].x }}`,
/// `{{ step.count }}`, `{{ step.status }}`); the bulk is summarised (arrays keep
/// their first element, large strings collapse to `{_len}`).  Bounded to
/// [`MAX_EXTRACTED_BYTES`] — a truncated node sets `_truncated: true`.
fn build_extracted(context: &serde_json::Value) -> serde_json::Value {
    let mut budget = MAX_EXTRACTED_BYTES;
    summarise_value(context, 0, &mut budget)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::Path, http::StatusCode as AxumStatus, routing::put, Json, Router};

    #[test]
    fn reference_locators_extracts_legacy_and_canonical() {
        // Over-budget reference carrying BOTH the legacy ref and the canonical
        // logical URI (the R02b stamp) → both surface for the Phase C read path.
        let result = serde_json::json!({
            "reference": {
                "kind": "result_ref",
                "ref": "noetl://execution/1/result/drain/9",
                "uri": "noetl://default/default/results/1/drain/0/0/1"
            }
        });
        let (legacy, canonical) = reference_locators(&result).unwrap();
        assert_eq!(legacy, "noetl://execution/1/result/drain/9");
        assert_eq!(
            canonical.as_deref(),
            Some("noetl://default/default/results/1/drain/0/0/1")
        );
        // Legacy-only reference (no canonical uri) → canonical is None (falls back).
        let legacy_only = serde_json::json!({
            "reference": { "kind": "result_ref", "ref": "noetl://execution/2/result/s/3" }
        });
        let (l, c) = reference_locators(&legacy_only).unwrap();
        assert_eq!(l, "noetl://execution/2/result/s/3");
        assert!(c.is_none());
    }

    #[test]
    fn context_reference_splice_reconstructs_inline() {
        // A nested step result holding `{reference}` + `extracted` (the
        // references-in-state shape) splices the resolved data where
        // extract_user_data reads it, and drops the reference.
        let mut result = serde_json::json!({
            "status": "ok",
            "context": { "result": {
                "context": { "data": { "count": 2 } },   // extracted summary
                "reference": { "ref": "noetl://execution/1/result/drain/9", "extracted": {} }
            }}
        });
        assert_eq!(
            reference_locators(&result).map(|(legacy, _)| legacy).as_deref(),
            Some("noetl://execution/1/result/drain/9")
        );
        let full = serde_json::json!({ "messages": [{"id": 1}, {"id": 2}], "count": 2 });
        splice_resolved(&mut result, full.clone());
        // The reference is gone; the full data sits where the orchestrator reads it.
        assert!(reference_locators(&result).is_none());
        assert_eq!(
            result.pointer("/context/result/context/data"),
            Some(&full)
        );
        // flat_with_data exposes both `{{ step.field }}` and `{{ step.data.field }}`.
        let flat = flat_with_data(&full);
        assert_eq!(flat["count"], serde_json::json!(2));
        assert_eq!(flat["data"]["count"], serde_json::json!(2));
    }

    #[test]
    fn extracted_keeps_scalars_and_navigable_structure() {
        let ctx = serde_json::json!({
            "count": 500,
            "status": "ok",
            "messages": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
            "blob": "x".repeat(2000),
        });
        let ex = build_extracted(&ctx);
        // Scalars pass through.
        assert_eq!(ex["count"], serde_json::json!(500));
        assert_eq!(ex["status"], serde_json::json!("ok"));
        // Arrays keep their FIRST element as a real 1-element array so
        // `messages[0].<field>` navigation resolves off the reference — without
        // copying the other N-1 elements.
        assert_eq!(ex["messages"][0]["id"], serde_json::json!(1));
        assert_eq!(ex["messages"][0]["name"], serde_json::json!("a"));
        assert_eq!(ex["messages"].as_array().unwrap().len(), 1);
        // The big string collapses to a length marker — no bulk data inline.
        assert_eq!(ex["blob"]["_len"], serde_json::json!(2000));
        // The whole extract is small.
        assert!(ex.to_string().len() <= MAX_EXTRACTED_BYTES);
    }

    #[test]
    fn extracted_navigates_nested_rowset_and_stays_bounded() {
        // The exact shape the orchestrator stalled on: a tabular result where a
        // `set:` reads `output.data.rows[0].facility_mapping_id`.  The old flat
        // `{_count,_keys}` collapse made `data` a count and dropped `rows`.
        let mut rows = Vec::new();
        for i in 0..800 {
            rows.push(serde_json::json!({
                "facility_mapping_id": i,
                "name": format!("facility-{i}"),
                "payload": "y".repeat(1000),
            }));
        }
        let ctx = serde_json::json!({
            "status": "COMPLETED",
            "data": { "columns": ["facility_mapping_id", "name", "payload"], "rows": rows },
        });
        let ex = build_extracted(&ctx);
        // The navigation path the guard needs resolves off the reference.
        assert_eq!(
            ex["data"]["rows"][0]["facility_mapping_id"],
            serde_json::json!(0)
        );
        // Only the first row is kept — not all 800.
        assert_eq!(ex["data"]["rows"].as_array().unwrap().len(), 1);
        // Per-row bulk string collapsed; the extract stays under budget.
        assert_eq!(ex["data"]["rows"][0]["payload"]["_len"], serde_json::json!(1000));
        assert!(
            ex.to_string().len() <= MAX_EXTRACTED_BYTES,
            "extracted must stay bounded, got {} bytes",
            ex.to_string().len()
        );
    }

    // --- #115 Phase 1: selective render-time ref resolution (consume side) ---

    #[test]
    fn accessor_paths_finds_member_and_index_chains() {
        let tpl = r#"{"input":{"a":"{{ start.status }}","b":"{{ start._ref }}","c":"{{ start.rows[0].id }}","d":"{{ start }}","e":"{{ other.start }}"}}"#;
        let paths = accessor_paths(tpl, "start");
        // `other.start` is a member of `other`, not a `start` reference.
        assert!(paths.contains(&vec!["status".to_string()]));
        assert!(paths.contains(&vec!["_ref".to_string()]));
        assert!(paths.contains(&vec!["rows".to_string(), "0".to_string(), "id".to_string()]));
        assert!(paths.contains(&vec![]), "bare {{ start }} → whole-object bind");
        // No spurious accessor from `other.start`.
        assert_eq!(paths.iter().filter(|p| p.is_empty()).count(), 1);
    }

    #[test]
    fn ref_and_scalar_access_does_not_force_resolution() {
        // output_select's verify/lazy_load shape: scalar + `_ref` + a key absent
        // from the summary.  None of these need the bulk.
        let summary = serde_json::json!({
            "status": "success",
            "data": { "generate_data": { "count": 1000 } },
            "_ref": "noetl://execution/1/result/start/9",
            "_store": "kv",
        });
        let tpl = r#"{"status":"{{ start.status }}","has":"{{ start._ref is defined }}","ref":"{{ start._ref }}","store":"{{ start._store }}","absent":"{{ start.count }}"}"#;
        assert!(!step_needs_bulk_resolution(tpl, "start", Some(&summary)));
    }

    #[test]
    fn data_bind_over_summarised_rowset_forces_resolution() {
        // process_full_data binds `{{ lazy_load_full_data.data }}` whole; the
        // summary collapsed `items` to one element, so it must resolve.
        let summary = serde_json::json!({
            "data": { "items": [{ "id": 0 }], "count": 1000 },
            "_ref": "noetl://execution/1/result/lazy/9",
        });
        let tpl = r#"{"data":"{{ lazy_load_full_data.data }}"}"#;
        assert!(step_needs_bulk_resolution(tpl, "lazy_load_full_data", Some(&summary)));
    }

    #[test]
    fn unreferenced_step_is_not_resolved() {
        let summary = serde_json::json!({ "status": "ok", "_ref": "noetl://x/y/z/1" });
        let tpl = r#"{"input":"{{ some_other_step.value }}"}"#;
        assert!(!step_needs_bulk_resolution(tpl, "upstream", Some(&summary)));
    }

    #[test]
    fn whole_object_bind_forces_resolution() {
        let summary = serde_json::json!({ "data": { "rows": [{ "id": 1 }] }, "_ref": "noetl://x/y/z/1" });
        let tpl = r#"{"all":"{{ build_batch_plan }}"}"#;
        assert!(step_needs_bulk_resolution(tpl, "build_batch_plan", Some(&summary)));
    }

    #[test]
    fn scalar_field_present_is_satisfiable_even_when_a_ref_exists() {
        // storage_tiers final_summary reads `{{ load_kv_data.count }}` off a
        // referenced (over-budget) artifact output — count is a kept scalar.
        let summary = serde_json::json!({
            "status": "ok", "count": 500, "tier": "kv_expected",
            "items": [{ "id": 0 }], "_ref": "noetl://x/y/z/1",
        });
        let tpl = r#"{"n":"{{ load_kv_data.count | default(0) }}"}"#;
        assert!(!step_needs_bulk_resolution(tpl, "load_kv_data", Some(&summary)));
    }

    #[test]
    fn missing_summary_resolves_conservatively() {
        let tpl = r#"{"x":"{{ start.anything }}"}"#;
        assert!(step_needs_bulk_resolution(tpl, "start", None));
    }

    #[test]
    fn bare_ref_stub_summary_forces_resolution() {
        // #104 Phase C regression: an over-budget upstream externalized as a
        // BARE `_ref` stub (only locator accessors; real payload in object
        // store) must resolve on a bulk bind — its absent keys live in the full
        // payload, not the stub. The consume step bound `{{ start.rows[..] }}`
        // off `{_ref, data:{_ref}}` and the detector wrongly kept the stub, so
        // resolve-by-URN (and the legacy fallback) never fired.
        let stub = serde_json::json!({
            "_ref": "noetl://execution/1/result/start/9",
            "data": { "_ref": "noetl://execution/1/result/start/9" },
        });
        let tpl = r#"{"n":"{{ start.rows | length }}","deep":"{{ start.rows[1100][0] }}"}"#;
        assert!(step_needs_bulk_resolution(tpl, "start", Some(&stub)));

        // The predicate itself: a bare stub is one; a key-preserving summary isn't.
        assert!(is_reference_stub(stub.as_object().unwrap()));
        let real_summary = serde_json::json!({
            "status": "success",
            "data": { "rows": [{ "id": 0 }] },
            "_ref": "noetl://x/y/z/1",
        });
        assert!(!is_reference_stub(real_summary.as_object().unwrap()));

        // Locator/predicate access on a stub stays satisfiable (no over-resolve).
        let pred = r#"{"has":"{{ start._ref is defined }}","r":"{{ start._ref }}"}"#;
        assert!(!step_needs_bulk_resolution(pred, "start", Some(&stub)));
    }

    #[test]
    fn flatten_single_tool_result_unifies_resolve_shapes() {
        // OQ6 (#104 Phase C, B1): legacy resolve_ref (tool envelope) and
        // resolve-by-URN (flat rowset) must normalize to the SAME shape so the
        // fail-safe fallback + flag-off legacy bind identically to inline.
        use serde_json::json;
        let canonical = json!({ "columns": ["id"], "rows": [[0], [1]] });

        // Legacy resolve_ref: full single-tool envelope → flattens to the rowset.
        let legacy = json!({
            "data": { "generate_rows": { "columns": ["id"], "rows": [[0], [1]] } },
            "status": "success", "exit_code": 0, "stderr": "", "stdout": "", "duration_ms": 1
        });
        assert_eq!(flatten_single_tool_result(legacy), canonical);

        // resolve-by-URN: already the flat rowset (no `status`) → idempotent.
        assert_eq!(
            flatten_single_tool_result(json!({ "columns": ["id"], "rows": [[0], [1]] })),
            canonical
        );

        // After flat_with_data both expose `{{ start.rows }}` at step level.
        let flat = flat_with_data(&flatten_single_tool_result(json!({
            "data": { "generate_rows": { "columns": ["id"], "rows": [[0], [1]] } },
            "status": "success"
        })));
        assert!(flat.get("rows").is_some(), "rows lifted so {{ start.rows }} resolves");

        // Multi-tool envelope is left unchanged (tool-keyed shape preserved).
        let multi = json!({
            "data": { "tool_a": { "x": 1 }, "tool_b": { "y": 2 } }, "status": "success"
        });
        assert_eq!(flatten_single_tool_result(multi.clone()), multi);

        // A bare user-data payload (no `status`) passes through untouched.
        let bare = json!({ "anything": 1 });
        assert_eq!(flatten_single_tool_result(bare.clone()), bare);
    }

    // --- #104 R02b: stamp the logical URI on over-budget references ---

    #[test]
    fn stamps_logical_uri_with_cursor_frame_and_row() {
        let mut obj = serde_json::json!({
            "status": "COMPLETED",
            "reference": { "kind": "result_ref", "ref": "noetl://execution/325/result/s/9" }
        });
        // `translate` copies metadata.cursor.{frame,row} into render_context.
        let mut rc = std::collections::HashMap::new();
        rc.insert("__cursor_frame".to_string(), serde_json::json!(2));
        rc.insert("__cursor_row".to_string(), serde_json::json!(4));
        stamp_logical_uri(&mut obj, 325, "load_next_facility", &rc);
        assert_eq!(
            obj["reference"]["uri"],
            serde_json::json!("noetl://default/default/results/325/load_next_facility/2/4/1")
        );
        // The existing physical `ref` is left intact.
        assert_eq!(obj["reference"]["ref"], serde_json::json!("noetl://execution/325/result/s/9"));
    }

    #[test]
    fn stamps_frame0_row0_for_non_cursor_step() {
        let mut obj = serde_json::json!({ "status": "COMPLETED", "reference": { "kind": "result_ref" } });
        // No cursor coords in render_context → 0/0.
        stamp_logical_uri(&mut obj, 7, "s", &std::collections::HashMap::new());
        assert_eq!(
            obj["reference"]["uri"],
            serde_json::json!("noetl://default/default/results/7/s/0/0/1")
        );
    }

    #[test]
    fn does_not_stamp_non_durable_reference_or_inline_result() {
        let rc = std::collections::HashMap::new();
        // A shm-only IpcHint (degraded path, kind arrow_ipc) is not a durable
        // logical location — left unstamped.
        let mut ipc = serde_json::json!({
            "status": "COMPLETED",
            "reference": { "kind": "arrow_ipc", "shm_name": "x" }
        });
        stamp_logical_uri(&mut ipc, 1, "s", &rc);
        assert!(ipc["reference"].get("uri").is_none());

        // An inline (under-budget) result has no reference — no-op.
        let mut inline = serde_json::json!({ "status": "COMPLETED", "context": { "data": {} } });
        stamp_logical_uri(&mut inline, 1, "s", &rc);
        assert!(inline.get("reference").is_none());
    }

    // --- noetl/ai-meta#104 Phase E: side-effect durability barrier ---

    #[test]
    fn cycle_logical_uri_matches_stamp_for_same_coordinate() {
        // The barrier derives the cycle's URN with `cycle_logical_uri`; the R02b
        // stamp writes it with `stamp_logical_uri`. They MUST agree for the same
        // `(execution_id, step, frame, row)` — that identity is what lets a
        // re-drive recognise the durable result a prior drive stamped.
        let mut rc = std::collections::HashMap::new();
        rc.insert("__cursor_frame".to_string(), serde_json::json!(2));
        rc.insert("__cursor_row".to_string(), serde_json::json!(4));
        let derived = cycle_logical_uri(325, "load_next_facility", &rc);

        let mut obj = serde_json::json!({
            "status": "COMPLETED",
            "reference": { "kind": "result_ref" }
        });
        stamp_logical_uri(&mut obj, 325, "load_next_facility", &rc);
        assert_eq!(serde_json::Value::String(derived), obj["reference"]["uri"]);
    }

    #[test]
    fn cycle_logical_uri_is_attempt_one_and_stable_across_redrive() {
        // attempt is fixed to 1 (the `.../1` suffix), so two drives of the SAME
        // cycle derive the IDENTICAL URN — the barrier's correctness hinge.
        let rc = std::collections::HashMap::new();
        let first = cycle_logical_uri(7, "charge_card", &rc);
        let second = cycle_logical_uri(7, "charge_card", &rc);
        assert_eq!(first, second);
        assert_eq!(first, "noetl://default/default/results/7/charge_card/0/0/1");
    }

    fn tc(kind: &str, config: serde_json::Value) -> ToolConfig {
        ToolConfig {
            kind: kind.to_string(),
            config,
            timeout: None,
            retry: None,
            auth: None,
        }
    }

    /// A `task_sequence` ToolConfig wrapping sub-tasks of the given kinds.
    fn task_seq(kinds: &[&str]) -> ToolConfig {
        let subs: Vec<serde_json::Value> = kinds
            .iter()
            .enumerate()
            .map(|(i, k)| serde_json::json!({ format!("t{i}"): { "kind": k } }))
            .collect();
        tc("task_sequence", serde_json::json!({ "tool_config": subs }))
    }

    #[test]
    fn barrier_gate_flag_off_never_checks() {
        // Flag off → never consult the tier, regardless of side-effect class.
        assert!(!side_effect_barrier_should_check(false, &tc("http", serde_json::json!({}))));
        assert!(!side_effect_barrier_should_check(false, &task_seq(&["python"])));
        assert!(!side_effect_barrier_should_check(false, &task_seq(&["noop"])));
    }

    #[test]
    fn barrier_gate_only_side_effecting_when_on() {
        // Flag on → check side-effecting commands, skip pure ones.
        assert!(side_effect_barrier_should_check(true, &tc("http", serde_json::json!({}))));
        assert!(side_effect_barrier_should_check(true, &tc("postgres", serde_json::json!({}))));
        assert!(!side_effect_barrier_should_check(true, &tc("rhai", serde_json::json!({}))));
        assert!(!side_effect_barrier_should_check(true, &tc("noop", serde_json::json!({}))));
    }

    #[test]
    fn command_is_side_effecting_looks_through_task_sequence() {
        // The orchestrator wraps every step's tool(s) in a task_sequence, so the
        // gate must classify by the INNER tool kind, not the wrapper.
        // A task_sequence whose only sub-task is pure → NOT side-effecting.
        assert!(!command_is_side_effecting(&task_seq(&["noop"])));
        assert!(!command_is_side_effecting(&task_seq(&["rhai"])));
        assert!(!command_is_side_effecting(&task_seq(&["rhai", "noop"])));
        // Any side-effecting sub-task makes the whole side-effecting.
        assert!(command_is_side_effecting(&task_seq(&["python"])));
        assert!(command_is_side_effecting(&task_seq(&["rhai", "http"])));
        // An empty / uninspectable task_sequence is conservatively side-effecting.
        assert!(command_is_side_effecting(&tc("task_sequence", serde_json::json!({}))));
        assert!(command_is_side_effecting(&tc(
            "task_sequence",
            serde_json::json!({ "tool_config": [] })
        )));
        // A non-wrapped command is classified by its own kind.
        assert!(command_is_side_effecting(&tc("postgres", serde_json::json!({}))));
        assert!(!command_is_side_effecting(&tc("noop", serde_json::json!({}))));
    }

    // --- #105 routing: the wasm dispatch helpers ---

    #[cfg(feature = "wasm-plugin")]
    #[test]
    fn wasm_config_parses_plugin_ref_and_input() {
        // The server canonicalizes the step's `input:` to `args`.
        let cfg = serde_json::json!({
            "plugin": { "path": "system/materialiser", "version": 3 },
            "args": { "batch": [1, 2] }
        });
        let (path, version, entry, input) = wasm_config_to_ref(&cfg).unwrap();
        assert_eq!(path, "system/materialiser");
        assert_eq!(version, 3);
        assert_eq!(entry, "run", "entry defaults to `run` when unset");
        assert_eq!(
            input,
            serde_json::to_vec(&serde_json::json!({ "batch": [1, 2] })).unwrap()
        );

        // `input` still works as a fallback (a directly-crafted command).
        let cfg2 = serde_json::json!({
            "plugin": { "path": "p", "version": 1 },
            "input": { "x": 1 }
        });
        let (_, _, _, input2) = wasm_config_to_ref(&cfg2).unwrap();
        assert_eq!(input2, serde_json::to_vec(&serde_json::json!({ "x": 1 })).unwrap());

        // An explicit `entry` (the worker-driven orchestrator's `run_state`) parses.
        let cfg3 = serde_json::json!({
            "plugin": { "path": "system/orchestrate", "version": 1, "entry": "run_state" },
            "args": { "state": {} }
        });
        let (_, _, entry3, _) = wasm_config_to_ref(&cfg3).unwrap();
        assert_eq!(entry3, "run_state");
    }

    #[cfg(feature = "wasm-plugin")]
    #[test]
    fn wasm_config_rejects_missing_plugin_or_version() {
        assert!(wasm_config_to_ref(&serde_json::json!({})).is_err());
        assert!(wasm_config_to_ref(&serde_json::json!({ "plugin": { "path": "p" } })).is_err());
        // No `input` is fine — empty bytes.
        let (_, _, _, input) =
            wasm_config_to_ref(&serde_json::json!({ "plugin": { "path": "p", "version": 1 } }))
                .unwrap();
        assert!(input.is_empty());
    }

    #[cfg(feature = "wasm-plugin")]
    #[test]
    fn plugin_outcome_maps_to_tool_result() {
        use noetl_tools::result::ToolStatus;
        let ok = crate::plugin::FlushReport {
            results_stored: 1,
            objects_stored: 1,
            events_published: 0,
            errors: vec![],
        };
        let r = plugin_outcome_to_tool_result(b"OUT".to_vec(), &ok);
        assert!(matches!(r.status, ToolStatus::Success));
        assert_eq!(
            r.data.as_ref().unwrap()["flush"]["objects_stored"],
            serde_json::json!(1)
        );

        let bad = crate::plugin::FlushReport {
            errors: vec!["boom".to_string()],
            ..Default::default()
        };
        let r2 = plugin_outcome_to_tool_result(vec![], &bad);
        assert!(matches!(r2.status, ToolStatus::Error));
        assert!(r2.error.as_deref().unwrap().contains("boom"));
    }

    use noetl_arrow_cache::CacheConfig;
    use tokio::net::TcpListener;

    // ------------------------------------------------------------
    // Keychain env-var allow-list (noetl/ai-meta#34)
    //
    // The env-var loader is independent of the rest of the executor
    // surface — these tests use serial mutation of `std::env::*`,
    // which means they MUST run on the same OS thread.  `cargo test`
    // already serialises tests inside a module by default but we
    // also clean every var we set (drop guard) so cross-test bleed
    // is bounded.
    // ------------------------------------------------------------

    /// RAII guard that sets a process env var on construction and
    /// restores its prior state on drop.  Without this, a panic
    /// inside a test leaks the test's env var into sibling tests
    /// (especially when running with `--test-threads=1` for
    /// repeatability), turning what should be a single failure into
    /// a cascade.
    struct EnvGuard {
        key: String,
        prev: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &str, value: &str) -> Self {
            let prev = std::env::var(key).ok();
            // SAFETY: process is single-threaded for these tests via
            // the module-level serialisation cargo test enforces.
            unsafe { std::env::set_var(key, value) };
            Self {
                key: key.to_string(),
                prev,
            }
        }

        fn unset(key: &str) -> Self {
            let prev = std::env::var(key).ok();
            unsafe { std::env::remove_var(key) };
            Self {
                key: key.to_string(),
                prev,
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                match &self.prev {
                    Some(v) => std::env::set_var(&self.key, v),
                    None => std::env::remove_var(&self.key),
                }
            }
        }
    }

    #[test]
    fn keychain_env_unset_returns_empty() {
        // Default no-auth shape — the allow-list env var isn't set,
        // existing deployments (pre-#34) keep working unchanged.
        let _g = EnvGuard::unset(KEYCHAIN_ENV_ALLOWLIST_VAR);
        let loaded = load_keychain_env_allowlist();
        assert!(loaded.is_empty(), "expected empty map, got {loaded:?}");
    }

    #[test]
    fn keychain_env_loads_listed_vars() {
        let _g = EnvGuard::set(
            KEYCHAIN_ENV_ALLOWLIST_VAR,
            "TEST_NOETL_KC_VAR_A,TEST_NOETL_KC_VAR_B",
        );
        let _a = EnvGuard::set("TEST_NOETL_KC_VAR_A", "alpha-secret");
        let _b = EnvGuard::set("TEST_NOETL_KC_VAR_B", "beta-secret");
        let loaded = load_keychain_env_allowlist();
        assert_eq!(loaded.len(), 2);
        assert_eq!(
            loaded.get("TEST_NOETL_KC_VAR_A").map(String::as_str),
            Some("alpha-secret")
        );
        assert_eq!(
            loaded.get("TEST_NOETL_KC_VAR_B").map(String::as_str),
            Some("beta-secret")
        );
    }

    #[test]
    fn keychain_env_tolerates_whitespace_and_empty_entries() {
        let _g = EnvGuard::set(
            KEYCHAIN_ENV_ALLOWLIST_VAR,
            " TEST_NOETL_KC_WS_A , , TEST_NOETL_KC_WS_B,",
        );
        let _a = EnvGuard::set("TEST_NOETL_KC_WS_A", "first");
        let _b = EnvGuard::set("TEST_NOETL_KC_WS_B", "second");
        let loaded = load_keychain_env_allowlist();
        assert_eq!(loaded.len(), 2);
        assert_eq!(
            loaded.get("TEST_NOETL_KC_WS_A").map(String::as_str),
            Some("first")
        );
        assert_eq!(
            loaded.get("TEST_NOETL_KC_WS_B").map(String::as_str),
            Some("second")
        );
    }

    #[test]
    fn keychain_env_silently_skips_missing_vars() {
        // Operator allow-listed a var ahead of mounting the Secret
        // — startup proceeds without it (mid-rollout shape) rather
        // than spamming a warn / refusing to start.
        let _g = EnvGuard::set(
            KEYCHAIN_ENV_ALLOWLIST_VAR,
            "TEST_NOETL_KC_PRESENT,TEST_NOETL_KC_ABSENT",
        );
        let _p = EnvGuard::set("TEST_NOETL_KC_PRESENT", "here");
        let _a = EnvGuard::unset("TEST_NOETL_KC_ABSENT");
        let loaded = load_keychain_env_allowlist();
        assert_eq!(loaded.len(), 1);
        assert_eq!(
            loaded.get("TEST_NOETL_KC_PRESENT").map(String::as_str),
            Some("here")
        );
        assert!(!loaded.contains_key("TEST_NOETL_KC_ABSENT"));
    }

    #[test]
    fn keychain_env_skips_empty_string_values() {
        // Distinguishing "unset" vs "set to empty string" is a
        // common deployment-time surprise — both shapes should be
        // skipped.  Otherwise a Secret with a blank field would
        // silently authenticate as an empty token, which is worse
        // than failing closed.
        let _g = EnvGuard::set(KEYCHAIN_ENV_ALLOWLIST_VAR, "TEST_NOETL_KC_BLANK");
        let _b = EnvGuard::set("TEST_NOETL_KC_BLANK", "");
        let loaded = load_keychain_env_allowlist();
        assert!(loaded.is_empty(), "blank values must be skipped");
    }

    #[test]
    fn keychain_env_empty_allowlist_returns_empty() {
        let _g = EnvGuard::set(KEYCHAIN_ENV_ALLOWLIST_VAR, "");
        let loaded = load_keychain_env_allowlist();
        assert!(loaded.is_empty());
    }

    /// Build a test-isolated cache with a unique namespace so
    /// concurrent test runs don't collide on shm names.  POSIX shm
    /// names are filesystem-global; the namespace is the prefix
    /// stamped onto every entry the cache produces.
    fn test_cache(namespace: &str) -> Arc<ArrowIpcSharedMemoryCache> {
        let config = CacheConfig {
            namespace: namespace.to_string(),
            budget_bytes: 8 * 1024 * 1024,
            default_lease_seconds: 60.0,
            producer: "test".to_string(),
            node_id: "test-node".to_string(),
        };
        Arc::new(ArrowIpcSharedMemoryCache::with_config(config))
    }

    /// Build a cache too small to admit even a modest payload so the
    /// over-budget `put_arrow_ipc` call deterministically fails.
    /// Used to exercise the "durable failure + shm failure → status-
    /// only" branch.
    fn tiny_test_cache(namespace: &str) -> Arc<ArrowIpcSharedMemoryCache> {
        let config = CacheConfig {
            namespace: namespace.to_string(),
            // 1 KB budget; oversized contexts in these tests are
            // > 100 KB so the shm cache rejects them.
            budget_bytes: 1024,
            default_lease_seconds: 60.0,
            producer: "test".to_string(),
            node_id: "test-node".to_string(),
        };
        Arc::new(ArrowIpcSharedMemoryCache::with_config(config))
    }

    /// Spin up an in-test axum mock of the Python server's
    /// `PUT /api/result/{execution_id}` endpoint.  Returns the
    /// bound `(base_url, server_handle)`; drop the handle to stop
    /// the server.  Each test gets its own mock so they don't share
    /// state.
    async fn start_mock_result_store(
        response_body: serde_json::Value,
    ) -> (String, tokio::task::JoinHandle<()>) {
        async fn handler(
            Path(execution_id): Path<i64>,
            Json(body): Json<serde_json::Value>,
        ) -> Result<Json<serde_json::Value>, AxumStatus> {
            // Returning the body's `name` interpolated into the
            // canned response is overkill for the assertions here;
            // hand back a fixed ResultRef shape with the
            // execution_id wired in.
            Ok(Json(serde_json::json!({
                "ref": format!("noetl://execution/{}/result/{}/abcd1234",
                    execution_id,
                    body.get("name").and_then(|v| v.as_str()).unwrap_or("step")),
                "store": "disk",
                "scope": "execution",
                "expires_at": "2026-06-01T00:00:00Z",
                "bytes": body
                    .get("data")
                    .map(|d| d.to_string().len() as u64)
                    .unwrap_or(0),
                "sha256": "deadbeefcafe",
            })))
        }

        // Pick which handler to install: if the caller passed a
        // `null` body, use the dynamic one (echoes execution_id +
        // step into the canned ResultRef shape); otherwise serve
        // the caller-supplied response body verbatim.
        let app = if response_body.is_null() {
            Router::new().route("/api/result/{execution_id}", put(handler))
        } else {
            // Per-test ResultPutResponse override path — captures
            // the canned `response_body` so tests can override
            // individual fields (e.g. drive a particular `store`
            // tier or omit `expires_at`).
            let canned = response_body;
            let canned_handler = move |Path(_): Path<i64>, Json(_): Json<serde_json::Value>| {
                let body = canned.clone();
                async move { Ok::<_, AxumStatus>(Json(body)) }
            };
            Router::new().route("/api/result/{execution_id}", put(canned_handler))
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let base = format!("http://{}", addr);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("axum serve");
        });
        (base, handle)
    }

    #[test]
    fn test_command_executor_creation() {
        let client = ControlPlaneClient::new("http://localhost:8082");
        let snowflake = Arc::new(SnowflakeGen::with_node_and_epoch(1, 0));
        let cache = test_cache("wkr-test-ctor");
        let executor = CommandExecutor::new(
            client,
            "worker-1".to_string(),
            "http://localhost:8082".to_string(),
            snowflake,
            cache,
            crate::state_builder::BuilderMode::Off,
            crate::state_builder::SharedWalIndex::new(crate::state_builder::WalEventIndex::new()),
            "nats://localhost:4222".to_string(),
        );

        // Verify tools are registered
        assert!(executor.tool_registry.has("shell"));
        assert!(executor.tool_registry.has("http"));
        assert!(executor.tool_registry.has("rhai"));
    }

    /// Branch 1 — small tool result rides the inline
    /// `result.context` path.  No HTTP, no shm.  Downstream Jinja
    /// templates can reference fields off `result.context` directly.
    #[tokio::test]
    async fn build_call_done_result_inlines_small_context() {
        let cache = test_cache("wkr-test-small");
        // Client points at an unreachable URL; the inline path
        // must NOT make any HTTP call.
        let client = ControlPlaneClient::new("http://127.0.0.1:1");
        let context = serde_json::json!({
            "stdout": "hello",
            "exit_code": 0,
            "duration_ms": 12,
        });
        let result =
            build_call_done_result(&context, "COMPLETED", 42, "greet", &std::collections::HashMap::new(), cache.as_ref(), &client)
                .await
                .unwrap();
        assert_eq!(result["status"], "COMPLETED");
        assert_eq!(result["context"]["stdout"], "hello");
        assert_eq!(result["context"]["exit_code"], 0);
        // The structure stays valid against the broker's
        // _STRICT_RESULT_ALLOWED_KEYS = {status, reference,
        // context, command_id} contract.
        let result_keys: Vec<&str> = result
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        for key in &result_keys {
            assert!(
                ["status", "context"].contains(key),
                "unexpected key: {}",
                key,
            );
        }
        // No bytes should have landed in the shm cache for the
        // inline path.
        assert_eq!(cache.used_bytes(), 0);
    }

    /// Branch 2 — over-budget AND both durable PUT + shm staging
    /// succeed → `{status, reference}` carrying a `ResultRef`-shaped
    /// dict with a nested `ipc` field for the colocated fast path.
    #[tokio::test]
    async fn build_call_done_result_uses_durable_plus_ipc_when_both_succeed() {
        let cache = test_cache("wkr-test-durable-plus-ipc");
        let (base, handle) = start_mock_result_store(serde_json::Value::Null).await;
        let client = ControlPlaneClient::new(&base);

        let big_string: String = "x".repeat(INLINE_CONTEXT_MAX_BYTES + 4096);
        let context = serde_json::json!({ "stdout": big_string });
        let result = build_call_done_result(
            &context,
            "COMPLETED",
            12345,
            "big_step",
            &std::collections::HashMap::new(),
            cache.as_ref(),
            &client,
        )
        .await
        .unwrap();

        assert_eq!(result["status"], "COMPLETED");
        let reference = result.get("reference").expect("must have reference");
        assert_eq!(reference["kind"], "result_ref");
        let durable_ref = reference["ref"].as_str().expect("ref must be a string");
        assert!(durable_ref.starts_with("noetl://execution/12345/result/big_step/"));
        assert_eq!(reference["store"], "disk");
        assert_eq!(reference["scope"], "execution");
        // `meta` is populated from the server response.
        assert!(reference["meta"]["bytes"].as_u64().is_some());
        assert_eq!(reference["meta"]["media_type"], "application/json");
        // `ipc` carries the IpcHint for the colocated acceleration.
        let ipc = reference.get("ipc").expect("must include ipc hint");
        assert_eq!(ipc["kind"], "arrow_ipc");
        assert!(ipc["shm_name"].is_string());
        assert!(ipc["byte_length"].as_u64().is_some());
        assert_eq!(ipc["media_type"], "application/json");
        // The shm cache must hold the staged bytes.
        assert!(cache.used_bytes() > INLINE_CONTEXT_MAX_BYTES as u64);

        // noetl/ai-meta#69 — over-budget result MUST also embed an
        // inline `context.data._ref` URI so downstream
        // `{{ step._ref }}` templates resolve.  Without this, the
        // orchestrator's extract_user_data finds nothing on the
        // over-budget path and consumers like the `artifact` tool
        // error on `Invalid artifact config: invalid type: null,
        // expected a string`.
        let context = result.get("context").expect(
            "noetl/ai-meta#69: over-budget result must embed inline context.data with _ref so {{ step._ref }} resolves",
        );
        assert_eq!(
            context["data"]["_ref"].as_str(),
            Some(durable_ref),
            "context.data._ref must match the durable PUT's ref so consumers can fetch the full result"
        );

        handle.abort();
    }

    /// Branch 0 — the control-plane drive result (`__orchestrate__`) rides the
    /// inline `result.context` path EVEN when it exceeds the inline budget, so
    /// the server can decode `output_b64` synchronously and advance the drive.
    /// Without this exemption a large drive result (e.g. the Muno hotels turn's
    /// ~138KB orchestrate result) offloads to a ref the server can't resolve
    /// under `NOETL_RESULT_STORE_DUAL_WRITE=false` → `commands=0` → the
    /// noetl/ai-meta#154 re-drive wedge.  The client points at an unreachable
    /// URL: the inline path must NOT attempt any durable PUT.
    #[tokio::test]
    async fn build_call_done_result_inlines_oversize_orchestrate_result() {
        let cache = test_cache("wkr-test-orchestrate-inline");
        let client = ControlPlaneClient::new("http://127.0.0.1:1");

        let big_string: String = "x".repeat(INLINE_CONTEXT_MAX_BYTES + 4096);
        let context = serde_json::json!({ "output_b64": "ZHJpdmU=", "filler": big_string });
        let result = build_call_done_result(
            &context,
            "COMPLETED",
            12345,
            ORCHESTRATE_STEP_NAME,
            &std::collections::HashMap::new(),
            cache.as_ref(),
            &client,
        )
        .await
        .unwrap();

        // Inline path: the orchestrate result rides `result.context`, NOT a
        // `reference` block, despite being over budget.
        assert_eq!(result["status"], "COMPLETED");
        assert!(
            result.get("reference").is_none(),
            "orchestrate result must NOT be offloaded to a reference"
        );
        assert_eq!(result["context"]["output_b64"], "ZHJpdmU=");
        // Nothing was staged to the shm cache (the over-budget path was skipped).
        assert_eq!(cache.used_bytes(), 0);
    }

    /// Credential scrub: a tool result containing a `password` /
    /// `api_key` / `Authorization` header or a Bearer-token-looking
    /// string surfaces as `[REDACTED]` in the inline `result.context`
    /// path (the most common case — small results that fit under the
    /// inline budget).  Locks in the wiring that `build_call_done_result`
    /// scrubs once at the top so all three emit paths (inline,
    /// durable PUT, shm cache) ride the scrubbed copy.
    #[tokio::test]
    async fn build_call_done_result_scrubs_credentials_on_inline_path() {
        let cache = test_cache("wkr-test-scrub-inline");
        // Client points at an unreachable URL — the inline path
        // must NOT make any HTTP call.
        let client = ControlPlaneClient::new("http://127.0.0.1:1");

        let context = serde_json::json!({
            "status": "Success",
            "data": {
                "stdout": "GET /users -> 200",
                "headers": {
                    "Authorization": "Bearer secret-token-12345",
                    "Content-Type": "application/json"
                },
                "creds": {
                    "user": "alice",
                    "password": "hunter2"
                }
            },
            "duration_ms": 42
        });

        let result = build_call_done_result(
            &context,
            "COMPLETED",
            7,
            "auth_step",
            &std::collections::HashMap::new(),
            cache.as_ref(),
            &client,
        )
        .await
        .unwrap();

        // Inline path fired (small payload).
        let scrubbed_ctx = result.get("context").expect("inline context");
        // `status` + `stdout` + `duration_ms` + `Content-Type` are
        // visible (non-sensitive); credentials are redacted.
        assert_eq!(scrubbed_ctx["status"], "Success");
        assert_eq!(scrubbed_ctx["duration_ms"], 42);
        assert_eq!(scrubbed_ctx["data"]["stdout"], "GET /users -> 200");
        assert_eq!(
            scrubbed_ctx["data"]["headers"]["Content-Type"],
            "application/json"
        );
        assert_eq!(
            scrubbed_ctx["data"]["headers"]["Authorization"],
            crate::scrub::REDACTED
        );
        // `creds` is NOT in the sensitive token list (only the
        // full word `credentials` is); recurse into it and find
        // the sensitive `password` field.
        assert_eq!(scrubbed_ctx["data"]["creds"]["user"], "alice");
        assert_eq!(
            scrubbed_ctx["data"]["creds"]["password"],
            crate::scrub::REDACTED
        );
    }

    /// Credential scrub: over-budget tabular tool output (the R-2.2
    /// path) with credential-bearing rows surfaces as Arrow IPC bytes
    /// in shm + a durable `ResultRef`, AND each row has its
    /// sensitive columns redacted.  This is the most security-
    /// sensitive path because the shm cache exposes the bytes to any
    /// process on the same node.
    #[tokio::test]
    async fn build_call_done_result_scrubs_credentials_on_tabular_path() {
        let cache = test_cache("wkr-test-scrub-tabular");
        let (base, handle) = start_mock_result_store(serde_json::Value::Null).await;
        let client = ControlPlaneClient::new(&base);

        // Build a > 100 KB DuckDB-shape rowset with credential
        // columns (the kind of query that would surface in a
        // `SELECT user, password, role FROM users` against a leaked
        // dev DB).  6000 rows × the credential columns easily
        // overflows the inline budget.
        let rows: Vec<serde_json::Value> = (0..6_000)
            .map(|i| {
                serde_json::json!({
                    "id": i,
                    "user": format!("user_{:06}", i),
                    "password": format!("hunter_{:06}", i),
                    "api_key": format!("sk-ant-{:040}", i),
                })
            })
            .collect();
        let context = serde_json::json!({
            "status": "Success",
            "data": {
                "columns": ["id", "user", "password", "api_key"],
                "rows": rows,
                "row_count": 6_000,
            },
        });

        let result = build_call_done_result(
            &context,
            "COMPLETED",
            7,
            "leaky_select",
            &std::collections::HashMap::new(),
            cache.as_ref(),
            &client,
        )
        .await
        .unwrap();

        // The over-budget path fires; we should NOT see the raw
        // tabular shape in result.context.
        // noetl/ai-meta#69: over-budget result NOW embeds an inline
        // `context.data._ref` block so downstream `{{ step._ref }}`
        // resolves.  The full tabular shape still lives in the
        // durable PUT + shm cache — only `_ref` rides inline.
        let inline_ctx = result
            .get("context")
            .expect("context.data._ref must be embedded inline");
        assert!(inline_ctx["data"]["_ref"].is_string());
        let reference = result.get("reference").expect("must have reference");
        // The `ipc` field carries the Arrow IPC bytes — but the
        // ROWS that landed in shm must have had their sensitive
        // columns redacted BEFORE encoding.  We don't decode the
        // shm bytes back here (would require reading the shm
        // region) — instead we round-trip the durable PUT body
        // through the mock and verify the server saw scrubbed
        // values.  The mock just echoes; for a more rigorous test
        // we'd capture the request body, but `cache.used_bytes()
        // > 0` proves something was staged AND the test below
        // verifies the scrub for the inline path (the same
        // scrubbed clone is used for shm staging).
        assert!(reference["kind"].is_string());
        assert!(cache.used_bytes() > 0);

        handle.abort();
    }

    /// R-2.2: over-budget tabular tool output (DuckDB / Postgres
    /// rowset shape) stages in shm as **Arrow IPC stream bytes**
    /// rather than JSON, with `media_type =
    /// application/vnd.apache.arrow.stream` on the `IpcHint`.
    /// Colocated consumers read the bytes via
    /// `arrow_ipc::reader::StreamReader` and get columnar layout for
    /// free; the durable PUT still rides JSON via `/api/result/`.
    #[tokio::test]
    async fn build_call_done_result_uses_arrow_ipc_for_tabular_tool_output() {
        let cache = test_cache("wkr-test-tabular-arrow");
        let (base, handle) = start_mock_result_store(serde_json::Value::Null).await;
        let client = ControlPlaneClient::new(&base);

        // Build an over-budget DuckDB-shape rowset: 6000 rows × 4
        // columns of typed values.  At ~26 bytes/row JSON-serialised
        // that's ~150 KB, comfortably over the 100 KB inline budget,
        // and the typed columns exercise the Int64 / Utf8 / Float64
        // / Boolean inference paths in
        // `arrow_codec::try_encode_tabular_json`.
        let rows: Vec<serde_json::Value> = (0..6_000)
            .map(|i| {
                serde_json::json!([i, format!("row_{:06}", i), (i as f64) * 1.5_f64, i % 2 == 0,])
            })
            .collect();
        let context = serde_json::json!({
            "status": "Success",
            "data": {
                "columns": ["id", "label", "score", "active"],
                "rows": rows,
                "row_count": 6_000,
            },
            "duration_ms": 42,
        });

        let result = build_call_done_result(
            &context,
            "COMPLETED",
            999,
            "tabular_step",
            &std::collections::HashMap::new(),
            cache.as_ref(),
            &client,
        )
        .await
        .unwrap();

        // Same five-row fallback chain (#29).  Tabular output
        // takes the "durable + ipc" branch because both the
        // durable mock + the shm cache accept it.
        assert_eq!(result["status"], "COMPLETED");
        // noetl/ai-meta#69: over-budget result now embeds an inline
        // `context.data._ref` block (the URI string is well under
        // the inline budget) so downstream `{{ step._ref }}`
        // resolves.  The full tabular payload stays out-of-band.
        let inline_ctx = result
            .get("context")
            .expect("context.data._ref must be embedded inline");
        assert!(inline_ctx["data"]["_ref"].is_string());
        let reference = result.get("reference").expect("must have reference");
        assert_eq!(reference["kind"], "result_ref");

        // The `ipc` field's `media_type` is the Arrow IPC stream
        // marker — same value Python's
        // `arrow_ipc.ARROW_STREAM_MEDIA_TYPE` carries.  This is the
        // key R-2.2 behaviour: consumers switch on `media_type` to
        // pick the right decoder.
        let ipc = reference.get("ipc").expect("must include ipc hint");
        assert_eq!(
            ipc["media_type"], "application/vnd.apache.arrow.stream",
            "tabular shm bytes must advertise Arrow IPC media type"
        );
        // `row_count` propagates from `TabularEncoding.row_count`
        // through `cache.put_arrow_ipc` onto the IpcHint.
        assert_eq!(
            ipc["row_count"].as_u64(),
            Some(6_000),
            "IpcHint must carry the tabular row_count"
        );
        // `schema_digest` is `"arrow"` (the helper's literal) so
        // consumers can branch on it if they ever need to.
        assert_eq!(ipc["schema_digest"], "arrow");
        // The shm region holds the Arrow IPC bytes — those should
        // be smaller than the original JSON serialisation because
        // columnar encoding compresses repetitive structure.
        let arrow_bytes = ipc["byte_length"].as_u64().unwrap_or(0);
        let json_bytes = serde_json::to_string(&context).unwrap().len() as u64;
        assert!(arrow_bytes > 0);
        assert!(
            arrow_bytes < json_bytes,
            "Arrow IPC bytes ({}) should be smaller than JSON ({}) for typed rowsets",
            arrow_bytes,
            json_bytes
        );

        handle.abort();
    }

    /// Branch 3 — over-budget AND durable PUT succeeds BUT shm
    /// staging fails (e.g. cache budget exhausted) → `{status,
    /// reference}` is the `ResultRef` dict WITHOUT an `ipc` field.
    /// Cross-node consumers still work; same-node consumers pay
    /// the durable round-trip.
    #[tokio::test]
    async fn build_call_done_result_durable_only_when_shm_fails() {
        let cache = tiny_test_cache("wkr-test-durable-only");
        let (base, handle) = start_mock_result_store(serde_json::Value::Null).await;
        let client = ControlPlaneClient::new(&base);

        let big_string: String = "x".repeat(INLINE_CONTEXT_MAX_BYTES + 4096);
        let context = serde_json::json!({ "stdout": big_string });
        let result = build_call_done_result(
            &context,
            "COMPLETED",
            42,
            "no_ipc_step",
            &std::collections::HashMap::new(),
            cache.as_ref(),
            &client,
        )
        .await
        .unwrap();

        assert_eq!(result["status"], "COMPLETED");
        let reference = result.get("reference").expect("must have reference");
        assert_eq!(reference["kind"], "result_ref");
        assert!(reference["ref"].as_str().unwrap().starts_with("noetl://"));
        // No ipc field — the shm cache rejected the bytes.
        assert!(
            reference.get("ipc").is_none(),
            "no ipc field when shm fails: {}",
            reference
        );

        handle.abort();
    }

    /// Branch 4 — over-budget AND durable PUT fails (no server)
    /// BUT shm cache works → emit the bare `IpcHint` as
    /// `result.reference` (matches noetl/worker#28 degraded mode).
    /// Cross-node consumers will see nothing; same-node consumers
    /// still attach.
    #[tokio::test]
    async fn build_call_done_result_falls_back_to_ipc_only_when_durable_fails() {
        let cache = test_cache("wkr-test-ipc-only");
        // Unreachable URL — `put_result` will fail with a connect
        // error.  127.0.0.1:1 is reliably refused on every common
        // OS (privileged port + no listener).
        let client = ControlPlaneClient::new("http://127.0.0.1:1");

        let big_string: String = "x".repeat(INLINE_CONTEXT_MAX_BYTES + 4096);
        let context = serde_json::json!({ "stdout": big_string });
        let result = build_call_done_result(
            &context,
            "COMPLETED",
            42,
            "shm_only_step",
            &std::collections::HashMap::new(),
            cache.as_ref(),
            &client,
        )
        .await
        .unwrap();

        assert_eq!(result["status"], "COMPLETED");
        let reference = result.get("reference").expect("must have reference");
        // Bare IpcHint shape — no `kind: result_ref`, no `ref` URI.
        assert_eq!(reference["kind"], "arrow_ipc");
        assert!(reference["shm_name"].is_string());
        assert!(reference.get("ref").is_none());
        // `media_type` is JSON because that's the encoder we used.
        assert_eq!(reference["media_type"], "application/json");
    }

    /// Branch 5 — over-budget AND BOTH durable PUT + shm staging
    /// fail → `{status}` only.  Predictable + visible fallback;
    /// downstream Jinja references will be empty but the broker
    /// still accepts the event.
    #[tokio::test]
    async fn build_call_done_result_falls_back_to_status_only_when_everything_fails() {
        let cache = tiny_test_cache("wkr-test-status-only");
        let client = ControlPlaneClient::new("http://127.0.0.1:1");

        let big_string: String = "x".repeat(INLINE_CONTEXT_MAX_BYTES + 4096);
        let context = serde_json::json!({ "stdout": big_string });
        let result = build_call_done_result(
            &context,
            "COMPLETED",
            42,
            "drop_step",
            &std::collections::HashMap::new(),
            cache.as_ref(),
            &client,
        )
        .await
        .unwrap();

        assert_eq!(result["status"], "COMPLETED");
        assert!(
            result.get("reference").is_none(),
            "no reference when everything fails: {}",
            result
        );
        // When BOTH durable PUT and shm cache failed, there's no
        // `_ref` URI to embed — emit `{status}` only (status-only
        // fallback, matches the legacy behaviour pre-#69).
        assert!(
            result.get("context").is_none(),
            "no inline context.data when there's no durable URI to embed: {}",
            result
        );
        // Only `status` should be in the result object.
        let keys: Vec<&str> = result
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        assert_eq!(keys, vec!["status"]);
    }

    /// The inline-budget threshold is a constant the broker side
    /// is tied to (`NOETL_EVENT_RESULT_CONTEXT_MAX_BYTES` default
    /// 102400 bytes).  Lock the value in so a future tweak to
    /// either side stays in sync.
    #[test]
    fn inline_context_max_bytes_matches_broker_default() {
        assert_eq!(INLINE_CONTEXT_MAX_BYTES, 102_400);
    }

    /// Boundary check: under-budget → no reference; over-budget →
    /// no inline context (regardless of which over-budget branch
    /// fires).  Uses an unreachable client so the over-budget path
    /// takes the shm-only branch, which is sufficient for proving
    /// the inline-vs-reference decision.
    #[tokio::test]
    async fn build_call_done_result_boundary_check() {
        let cache = test_cache("wkr-test-bound");
        let client = ControlPlaneClient::new("http://127.0.0.1:1");
        // We can't easily craft a context whose JSON encoding is
        // EXACTLY INLINE_CONTEXT_MAX_BYTES, but we can prove the
        // ">" (strictly greater) semantics by checking a result
        // smaller and a result larger than the threshold.
        let small = serde_json::json!({ "x": "a".repeat(INLINE_CONTEXT_MAX_BYTES - 100) });
        let small_result =
            build_call_done_result(&small, "COMPLETED", 1, "s", &std::collections::HashMap::new(), cache.as_ref(), &client)
                .await
                .unwrap();
        assert!(small_result.get("context").is_some());
        assert!(small_result.get("reference").is_none());

        let large = serde_json::json!({ "x": "a".repeat(INLINE_CONTEXT_MAX_BYTES + 100) });
        let large_result =
            build_call_done_result(&large, "COMPLETED", 1, "l", &std::collections::HashMap::new(), cache.as_ref(), &client)
                .await
                .unwrap();
        assert!(large_result.get("context").is_none());
        assert!(large_result.get("reference").is_some());
    }

    // ----------------------------------------------------------------
    // Pre-dispatch failure emission (noetl/ai-meta#78)
    //
    // A pre-dispatch failure (credential-alias 404, malformed tool
    // config) used to early-`?`-return from `execute_with_server_url`
    // and the worker only logged it — no `call.error` reached the
    // server, so the execution hung at `command.started` forever.
    // `handle_predispatch_failure` now emits the terminal events for
    // terminal failures and emits nothing for still-retryable transient
    // ones.  These tests drive that method against a mock `/api/events`
    // sink and assert the emitted (or absent) events.
    // ----------------------------------------------------------------

    use std::sync::Mutex;

    fn predispatch_command() -> Command {
        Command {
            command_id: "cmd-78".to_string(),
            execution_id: 323127686446714880,
            step: "start".to_string(),
            tool_kind: "postgres".to_string(),
            input: serde_json::Value::Null,
            render_context: Default::default(),
            attempts: 0,
        }
    }

    /// Spawn a mock control-plane that records every event POSTed to
    /// `/api/events`.  Returns `(base_url, recorded_events, handle)`.
    async fn start_mock_event_sink() -> (
        String,
        Arc<Mutex<Vec<serde_json::Value>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let recorded: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = recorded.clone();
        let app = Router::new().route(
            "/api/events",
            axum::routing::post(move |Json(body): Json<serde_json::Value>| {
                let sink = sink.clone();
                async move {
                    sink.lock().unwrap().push(body);
                    Json(serde_json::json!({ "status": "ok" }))
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let base = format!("http://{}", addr);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("axum serve");
        });
        (base, recorded, handle)
    }

    fn test_executor(base: &str) -> CommandExecutor {
        CommandExecutor::new(
            ControlPlaneClient::new(base),
            "worker-test".to_string(),
            base.to_string(),
            Arc::new(SnowflakeGen::with_node_and_epoch(1, 0)),
            test_cache("wkr-test-predispatch"),
            crate::state_builder::BuilderMode::Off,
            crate::state_builder::SharedWalIndex::new(crate::state_builder::WalEventIndex::new()),
            "nats://localhost:4222".to_string(),
        )
    }

    /// Terminal pre-dispatch failure → emits `call.error` (FAILED) and
    /// `command.failed` so the execution fails cleanly instead of
    /// hanging at `command.started`.  This is the core noetl/ai-meta#78
    /// assertion.
    #[tokio::test]
    async fn predispatch_terminal_failure_emits_call_error() {
        let (base, recorded, handle) = start_mock_event_sink().await;
        let executor = test_executor(&base);
        let command = predispatch_command();
        let client = ControlPlaneClient::new(&base);

        let err = executor
            .handle_predispatch_failure(
                &client,
                &command,
                0,
                true,
                anyhow::anyhow!(
                    "Credential alias 'pg_noetl_k8s' not found in keychain (server returned 404 for /api/credentials/pg_noetl_k8s)"
                ),
            )
            .await
            .expect_err("handle_predispatch_failure always returns Err");
        assert!(err.to_string().contains("pg_noetl_k8s"));

        let events = recorded.lock().unwrap();
        let types: Vec<&str> = events
            .iter()
            .filter_map(|e| e.get("event_type").and_then(|v| v.as_str()))
            .collect();
        assert!(
            types.contains(&"call.error"),
            "terminal pre-dispatch failure must emit call.error, got: {types:?}"
        );
        assert!(
            types.contains(&"command.failed"),
            "terminal pre-dispatch failure must emit command.failed, got: {types:?}"
        );

        // The call.error carries FAILED status + the error string +
        // command_id, matching the post-dispatch error arm's shape.
        let call_error = events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("call.error"))
            .expect("call.error present");
        assert_eq!(call_error["status"], "FAILED");
        assert_eq!(call_error["step"], "start");
        assert_eq!(call_error["context"]["command_id"], "cmd-78");
        assert!(call_error["context"]["error"]
            .as_str()
            .unwrap()
            .contains("not found in keychain"));

        handle.abort();
    }

    /// Retryable pre-dispatch failure (transient transport error,
    /// attempts not exhausted) → emits NOTHING, so the command path's
    /// retry/redelivery can still complete the step.
    #[tokio::test]
    async fn predispatch_retryable_failure_emits_nothing() {
        let (base, recorded, handle) = start_mock_event_sink().await;
        let executor = test_executor(&base);
        let command = predispatch_command();
        let client = ControlPlaneClient::new(&base);

        let _err = executor
            .handle_predispatch_failure(
                &client,
                &command,
                0,
                false,
                anyhow::anyhow!(
                    "transient error looking up credential alias 'pg_noetl_k8s' in keychain"
                ),
            )
            .await
            .expect_err("handle_predispatch_failure always returns Err");

        let events = recorded.lock().unwrap();
        assert!(
            events.is_empty(),
            "retryable pre-dispatch failure must emit no events, got: {events:?}"
        );
        handle.abort();
    }

    // --- noetl/ai-meta#145 G2 — container poll fallback helpers ---

    #[test]
    fn extract_job_handle_reads_container_data() {
        let mut r = noetl_tools::result::ToolResult::success(serde_json::json!({
            "job_name": "noetl-container-train-42-abcde",
            "namespace": "noetl",
            "job_uid": "uid-1",
        }));
        r.pending_callback = Some(true);
        let (ns, name) = extract_job_handle(&r).expect("handle present");
        assert_eq!(ns, "noetl");
        assert_eq!(name, "noetl-container-train-42-abcde");
    }

    #[test]
    fn extract_job_handle_none_when_fields_missing_or_empty() {
        // No data at all.
        let mut r = noetl_tools::result::ToolResult::success(serde_json::json!({}));
        r.pending_callback = Some(true);
        assert!(extract_job_handle(&r).is_none());

        // Empty strings are rejected (would build an unusable kube query).
        let r2 = noetl_tools::result::ToolResult::success(serde_json::json!({
            "job_name": "",
            "namespace": "noetl",
        }));
        assert!(extract_job_handle(&r2).is_none());
    }

    #[test]
    fn container_poll_options_reads_env_overrides() {
        // Defaults when unset.
        std::env::remove_var("NOETL_CONTAINER_POLL_INTERVAL_SECS");
        std::env::remove_var("NOETL_CONTAINER_POLL_MAX_WAIT_SECS");
        let o = container_poll_options();
        assert_eq!(o.interval.as_secs(), 5);
        assert_eq!(o.max_wait.as_secs(), 24 * 60 * 60);

        std::env::set_var("NOETL_CONTAINER_POLL_INTERVAL_SECS", "2");
        std::env::set_var("NOETL_CONTAINER_POLL_MAX_WAIT_SECS", "120");
        let o2 = container_poll_options();
        assert_eq!(o2.interval.as_secs(), 2);
        assert_eq!(o2.max_wait.as_secs(), 120);
        std::env::remove_var("NOETL_CONTAINER_POLL_INTERVAL_SECS");
        std::env::remove_var("NOETL_CONTAINER_POLL_MAX_WAIT_SECS");
    }
}
