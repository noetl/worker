//! In-process EHDB (Event Horizon Database) integration for the Rust worker.
//!
//! EHDB is NoETL-specialized storage.  The worker/playbook/system **data-plane**
//! roles own the EHDB integration *in process*: this module calls the
//! [`ehdb_reference`] crate's bounded local-reference helpers directly — there is
//! no subprocess to the `ehdb-local-reference` binary and no parallel Python
//! implementation (the `noetl.core.ehdb_*` path is retired).  Because prod runs
//! worker-rust, this is the only EHDB integration path that actually executes.
//!
//! ## Boundaries (all enforced in code + tested)
//!
//! * **Disabled by default** — when `NOETL_EHDB_ENABLED` is not truthy every
//!   entry point is a strict no-op that records no metric, so the worker's
//!   `/metrics` output and behaviour are byte-identical to a build without EHDB.
//! * **Control-plane-only refusal** — `gateway` / `api` / `server` roles are
//!   refused any data-plane / event-stream op by [`guard`]; only
//!   `worker` / `playbook` / `system` reach the data plane.
//! * **Bounded + stateless** — payloads, read/consume batches, and ack
//!   sequences are capped; the local-reference runtime is opened, used, and
//!   dropped per call (no long-lived handle, no per-tenant residency).
//! * **Event-log-authoritative** — the NoETL event log stays the append-only
//!   source of truth.  [`eventstream`] is a *derived* consumer of already-emitted
//!   events into a separate on-disk JSONL fabric; it NEVER writes back to
//!   `noetl.event` (structurally asserted — no NoETL event-emitter import).
//!
//! The env contract this reads is rendered by the ops Helm charts
//! (`automation/helm/*/templates/_ehdb.tpl`, disabled by default) and mirrored
//! by [`contract`].
//!
//! ## Live runtime-hook wiring status (noetl/ehdb#234)
//!
//! Each tier's shadow/mirror engine exists in this module; a tier is
//! *live-wired* once its `mirror_live_*` hook is invoked from the worker's real
//! runtime path (not just `ehdb-selfcheck`).  Every hook is env-armed
//! (`NOETL_EHDB_ENABLED` + `NOETL_EHDB_<TIER>=shadow` + data-plane role), a
//! strict no-op otherwise, and error-isolated so the authoritative path is never
//! affected.
//!
//! | Tier | Live-wired | Seam |
//! | :-- | :-- | :-- |
//! | eventlog | ✅ | [`client::ControlPlaneClient::emit_event`][ee] → [`eventlog::mirror_live_event`] |
//! | kv | ✅ | `spool_runtime::SpoolRuntime::persist_circuit` (NATS-KV circuit put) → [`kv::mirror_live_put`] |
//! | object | ✅ | [`client::ControlPlaneClient::object_put`][op] (all object tiers funnel through it) → [`object::mirror_live_put`] |
//! | projection | ⏳ deferred | see below |
//! | vector | ⏳ deferred | see below |
//!
//! **projection — deferred (no clean per-event live seam).**
//! [`projection::shadow_project`] is a *batch* materialize that reads back the
//! **whole accumulating** projection log ([`ehdb_reference`]'s
//! `list_executions`) and compares it against a **full** authoritative
//! execution-state fold + committed offset.  A long-running worker's log
//! accumulates unboundedly, so a naive per-event hook (at `emit_event` or the
//! off-server state builder's `WalEventIndex::apply`, `state_builder.rs`) would
//! report persistent false key-divergence — that is "a hook that fires but lies",
//! not a shadow.  The faithful seam is a bounded, windowed batch materialization
//! that also supplies the incumbent materializer's fold for the same window
//! (either a scheduled tail-window drive, or a per-execution-scoped readback on
//! the engine side) — a larger change than a call-site hook.  Exact remaining
//! seam: `state_builder.rs` `run_drain_loop` after `idx.apply(&payload)` (where
//! `payload` + `execution_id` are in hand), feeding the `WalEventIndex`
//! per-execution fold as `shadow_project`'s `authoritative` argument, once the
//! engine can scope its readback to the touched executions.
//!
//! **vector — deferred (no live write site exists yet).**
//! There is no platform vector-upsert in the worker's live loop today: platform
//! RAG retrieval is in-process and read-only ([`rag`]), and
//! [`vector::mirror_upsert`] is exercised only by `ehdb-selfcheck`.  Wiring a
//! hook now would create one that never fires.  Exact remaining seam: a future
//! platform-RAG *ingest/embed* path (would live in `executor/command.rs` when a
//! step embeds + upserts platform vectors), which does not exist yet — the hook
//! lands with that write site, not before.
//!
//! [ee]: crate::client::ControlPlaneClient::emit_event
//! [op]: crate::client::ControlPlaneClient::object_put

use std::collections::HashMap;

pub mod backends;
pub mod contract;
pub mod dataplane;
pub mod eventlog;
pub mod eventstream;
pub mod guard;
pub mod kv;
pub mod metrics;
pub mod object;
pub mod projection;
pub mod rag;
pub mod readiness;
pub mod systemstore;
pub mod vector;

/// A snapshot of environment variables.  Functions take an explicit `&EnvMap`
/// (rather than reading `std::env` directly) so tests can inject a fixed env
/// without racing the process environment.
pub type EnvMap = HashMap<String, String>;

/// Snapshot the current process environment into an [`EnvMap`].
pub fn process_env() -> EnvMap {
    std::env::vars().collect()
}

/// Default tenant / namespace for NoETL worker/playbook bounded ops (mirrors the
/// `ehdb_reference` crate constants).
pub use ehdb_reference::{DEFAULT_LOCAL_REFERENCE_NAMESPACE, DEFAULT_LOCAL_REFERENCE_TENANT};

/// Render a Prometheus label block (`{k="v",...}`) with keys sorted, values
/// escaped.  Shared by the readiness / dataplane / eventstream metric
/// renderers so they emit identical formatting.  Returns `""` for no labels.
pub(crate) fn format_labels(labels: &[(&str, String)]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let mut sorted: Vec<&(&str, String)> = labels.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    let rendered = sorted
        .iter()
        .map(|(k, v)| format!("{k}=\"{}\"", escape_label(v)))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{rendered}}}")
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}
