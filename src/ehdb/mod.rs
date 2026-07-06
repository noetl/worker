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

use std::collections::HashMap;

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
