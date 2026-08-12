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
//! | projection | ✅ | `state_builder::run_drain_loop` post-batch checkpoint → [`projection::mirror_live_window`] (windowed, not per-event) |
//! | vector | ⏳ ready, no live site | [`vector::mirror_live_upsert`] exists + tested; no live platform vector-upsert path exists in the worker to call it — see below |
//!
//! **projection — live-wired via a windowed cadence hook (not per-event).**
//! [`projection::shadow_project`] is a *batch* materialize that reads back the
//! projection log ([`ehdb_reference`]'s `list_executions`) and compares it
//! against a **full** authoritative execution-state fold + committed offset.  A
//! naive per-event hook against the long-lived, unboundedly-accumulating
//! projection store would report persistent false key-divergence (the store
//! keeps every execution `KeepAll` while a per-event authoritative names only the
//! touched one) — "a hook that fires but lies", not a shadow.  The faithful seam
//! shipped here: the off-server state builder's drain loop
//! (`state_builder.rs::run_drain_loop`) buffers each drained batch's real events
//! and, at the natural post-batch checkpoint, calls
//! [`projection::mirror_live_window`] once per batch.  That windows the batch into
//! a **fresh, throwaway per-window projection store** (unique temp log) so the
//! read-back sees exactly the window's executions — no cross-window accumulation
//! ⇒ no false key-divergence — and parity-checks the EHDB engine's fold against an
//! **independent worker-side fold** of the same window
//! ([`projection::fold_window_authoritative`]).  Bounded + stateless (store
//! opened, used, removed per window); runs on a blocking thread so the sync engine
//! I/O never stalls the drain reactor; best-effort + isolated.  Cross-window
//! persistence / replay is proven separately by `ehdb-selfcheck`'s primary-serve
//! cycle.
//!
//! **vector — ready hook, no live write site (documented-unreachable).**
//! There is **no platform vector-upsert in the worker's live loop today**:
//! platform RAG retrieval is in-process and read-only ([`rag`]); platform RAG
//! *ingest* ([`rag::ingest`]) writes a **lexical** retrieval fabric
//! ([`rag::RagChunk`] carries `text` + `checksum`, not an embedding vector), so it
//! is not a vector-embedding upsert to mirror; and [`vector::mirror_upsert`] is
//! exercised only by `ehdb-selfcheck`.  The ready-but-unwired
//! [`vector::mirror_live_upsert`] + [`vector::runtime_hook_env`] pair (tested to
//! the same discipline as the other tiers) is provided so the future wire-up is a
//! one-line call, but it is deliberately **not** invoked from any live path —
//! fabricating a call site would create a hook that never mirrors a real upsert.
//! Exact remaining seam: a future platform-RAG *embed + upsert* write site (would
//! live in `executor/command.rs` when a step computes embeddings and upserts
//! platform vectors) calls `mirror_live_upsert` right after the authoritative
//! Qdrant upsert — the hook lands with that write site, not before.
//!
//! [ee]: crate::client::ControlPlaneClient::emit_event
//! [op]: crate::client::ControlPlaneClient::object_put

use std::collections::HashMap;

pub mod backends;
pub mod contract;
pub mod dataplane;
pub mod eventlog;
pub mod eventlog_backend;
pub mod eventlog_gc;
pub mod eventstream;
pub mod flight_sql_endpoint;
pub mod guard;
pub mod kv;
pub mod metrics;
pub mod object;
pub mod primary_serve;
pub mod projection;
pub mod query;
pub mod rag;
pub mod reachability;
pub mod readiness;
pub mod systemstore;
pub mod tier_client;
pub mod tier_query_source;
pub mod tier_service;
pub mod tier_store;
pub mod vector;

/// A snapshot of environment variables.  Functions take an explicit `&EnvMap`
/// (rather than reading `std::env` directly) so tests can inject a fixed env
/// without racing the process environment.
pub type EnvMap = HashMap<String, String>;

/// Snapshot the current process environment into an [`EnvMap`].
/// Report — once per tier per process — what selecting `primary` on this tier
/// actually did, at the moment an operator selects it.
///
/// Three distinct conditions, because they call for three different operator
/// responses.  Getting this wrong is not cosmetic: the line fires exactly when
/// someone is running a cutover, and a false "nothing happened" invites them to
/// flip again or to walk away from a tier that is live
/// ([ai-meta#259](https://github.com/noetl/ai-meta/issues/259)).
///
/// | condition | signal | what the operator should do |
/// | :-- | :-- | :-- |
/// | tier has no runtime serve path ([`primary_serve::SERVE_WIRED_TIERS`]) | WARN, `outcome="primary_not_wired"` | nothing was promoted; the tier stays a shadow. Flipping again will not help |
/// | serve path exists, no tier-service address configured | WARN, `outcome="primary_no_tier_service"` | set `NOETL_EHDB_TIER_SERVICE_ADDR`; until then every op demotes to the incumbent |
/// | serve path exists and a tier service is configured | INFO, `outcome="primary_armed"` | watch `served_primary` vs `parity_diverged` / `no_durable_service` |
///
/// `primary_armed` is deliberately *not* a claim that the tier is serving.
/// Serving needs three conditions ([`primary_serve::decide`]) and two of them —
/// measured reachability and per-op parity — are only knowable per operation.
/// This says the flip was accepted and the serve path is armed; the per-op
/// outcome counters say what it then did.
///
/// `primary` is accepted rather than rejected in every case, because refusing
/// to start on a config a previous build accepted would turn a misconfiguration
/// into an outage.
///
/// Rate-limited to one line per tier because this sits on the per-event hot
/// path — `logging.md` forbids per-event INFO/WARN volume.
pub fn note_primary_selected(tier: &str) {
    use std::collections::HashSet;
    use std::sync::Mutex;
    static WARNED: Mutex<Option<HashSet<String>>> = Mutex::new(None);
    let mut g = match WARNED.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let set = g.get_or_insert_with(HashSet::new);
    if !set.insert(tier.to_string()) {
        return;
    }
    let upper = tier.to_uppercase();
    if !primary_serve::tier_serves_primary(tier) {
        crate::ehdb::metrics::record_runtime_hook(tier, "primary_not_wired");
        tracing::warn!(
            tier = tier,
            "NOETL_EHDB_{upper} is set to `primary`, but the {tier} tier has no \
             runtime serve path in this build — its `serve_primary_cycle` is \
             reachable only from bin/ehdb-selfcheck, so no caller can receive an \
             EHDB answer for it.  The tier keeps mirroring and parity-checking \
             and the incumbent remains authoritative; flipping again will not \
             change that.  The event-log tier is the only tier wired to serve \
             today.  See noetl/ai-meta#257.",
        );
        return;
    }
    if tier_client::TierClientConfig::from_env().is_none() {
        crate::ehdb::metrics::record_runtime_hook(tier, "primary_no_tier_service");
        tracing::warn!(
            tier = tier,
            "NOETL_EHDB_{upper} is set to `primary` and the {tier} tier does have \
             a serve path, but NOETL_EHDB_TIER_SERVICE_ADDR is unset — there is \
             no durable store to serve FROM, so every operation demotes to the \
             incumbent (outcome=\"no_durable_service\").  Set the tier-service \
             address to complete the cutover.  See noetl/ai-meta#257.",
        );
        return;
    }
    crate::ehdb::metrics::record_runtime_hook(tier, "primary_armed");
    tracing::info!(
        tier = tier,
        "NOETL_EHDB_{upper} is set to `primary` and the {tier} serve path is \
         armed.  Whether an individual operation is served by EHDB still depends \
         on a REACHED tier service and on parity holding for that operation; \
         divergence or unreachability demotes to the incumbent rather than \
         failing the caller.  Watch outcome=\"served_primary\" against \
         \"parity_diverged\" / \"no_durable_service\".  See noetl/ai-meta#257.",
    );
}

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
