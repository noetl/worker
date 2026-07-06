//! Secret-free, process-local EHDB metric accumulators.
//!
//! These are deliberately NOT registered in the worker's Prometheus
//! [`crate::metrics::WorkerMetrics`] registry.  A registered zero-valued metric
//! still renders a line, which would break the "disabled ⇒ byte-identical
//! `/metrics`" invariant.  Instead the accumulators start empty and are only
//! ever touched by a *non-disabled* EHDB op; [`render_lines`] returns nothing
//! until then, and the worker's `/metrics` handler appends its output verbatim.
//! Mirrors the retired Python `render_ehdb_*_metrics` renderers, including their
//! metric names, so dashboards carry over unchanged.
//!
//! Only aggregate counters + last-op gauges are exported — no log path, payload,
//! stream/subject, or error text ever reaches a label.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::OnceLock;

use super::format_labels;

#[derive(Default)]
struct FamilyState {
    /// Counter keyed by an ordered label tuple → count.
    counts: BTreeMap<Vec<(String, String)>, u64>,
    last_a: i64, // ready / ok (1|0)
    last_degraded: i64,
    last_duration_seconds: f64,
}

impl FamilyState {
    fn record(&mut self, labels: Vec<(String, String)>, a: bool, degraded: bool, duration: f64) {
        *self.counts.entry(labels).or_insert(0) += 1;
        self.last_a = i64::from(a);
        self.last_degraded = i64::from(degraded);
        self.last_duration_seconds = duration;
    }

    fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }
}

#[derive(Default)]
struct EhdbMetricsState {
    readiness: FamilyState,
    dataplane: FamilyState,
    eventstream: FamilyState,
    systemstore: FamilyState,
    rag: FamilyState,
    eventlog: FamilyState,
    projection: FamilyState,
    kv: FamilyState,
    object: FamilyState,
    vector: FamilyState,
}

fn state() -> &'static Mutex<EhdbMetricsState> {
    static STATE: OnceLock<Mutex<EhdbMetricsState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(EhdbMetricsState::default()))
}

/// Record one readiness evaluation.  `disabled` outcomes are intentionally NOT
/// recorded so a disabled build renders byte-identical `/metrics`.
pub fn record_readiness(outcome: &str, ready: bool, degraded: bool, duration_seconds: f64) {
    if outcome == "disabled" {
        return;
    }
    let mut s = state().lock().expect("ehdb metrics lock");
    s.readiness.record(
        vec![("outcome".to_string(), outcome.to_string())],
        ready,
        degraded,
        duration_seconds,
    );
}

/// Record one bounded data-plane op.  `disabled` outcomes are not recorded.
pub fn record_dataplane(
    operation: &str,
    outcome: &str,
    ok: bool,
    degraded: bool,
    duration_seconds: f64,
) {
    if outcome == "disabled" {
        return;
    }
    let mut s = state().lock().expect("ehdb metrics lock");
    s.dataplane.record(
        vec![
            ("operation".to_string(), operation.to_string()),
            ("outcome".to_string(), outcome.to_string()),
        ],
        ok,
        degraded,
        duration_seconds,
    );
}

/// Record one bounded event-stream op.  `disabled` outcomes are not recorded.
pub fn record_eventstream(
    operation: &str,
    outcome: &str,
    ok: bool,
    degraded: bool,
    duration_seconds: f64,
) {
    if outcome == "disabled" {
        return;
    }
    let mut s = state().lock().expect("ehdb metrics lock");
    s.eventstream.record(
        vec![
            ("operation".to_string(), operation.to_string()),
            ("outcome".to_string(), outcome.to_string()),
        ],
        ok,
        degraded,
        duration_seconds,
    );
}

/// Record one bounded system-store op (EHDB Phase E).  `disabled` outcomes are
/// not recorded, preserving the byte-identical `/metrics` invariant.
pub fn record_systemstore(
    operation: &str,
    outcome: &str,
    ok: bool,
    degraded: bool,
    duration_seconds: f64,
) {
    if outcome == "disabled" {
        return;
    }
    let mut s = state().lock().expect("ehdb metrics lock");
    s.systemstore.record(
        vec![
            ("operation".to_string(), operation.to_string()),
            ("outcome".to_string(), outcome.to_string()),
        ],
        ok,
        degraded,
        duration_seconds,
    );
}

/// Record one bounded RAG retrieval/ingest op (EHDB Phase E).  `disabled`
/// outcomes are not recorded, preserving the byte-identical `/metrics` invariant.
pub fn record_rag(operation: &str, outcome: &str, ok: bool, degraded: bool, duration_seconds: f64) {
    if outcome == "disabled" {
        return;
    }
    let mut s = state().lock().expect("ehdb metrics lock");
    s.rag.record(
        vec![
            ("operation".to_string(), operation.to_string()),
            ("outcome".to_string(), outcome.to_string()),
        ],
        ok,
        degraded,
        duration_seconds,
    );
}

/// Record one event-log shadow op (EHDB Phase 6).  `disabled` outcomes are not
/// recorded, preserving the byte-identical `/metrics` invariant.
pub fn record_eventlog(
    operation: &str,
    outcome: &str,
    ok: bool,
    degraded: bool,
    duration_seconds: f64,
) {
    if outcome == "disabled" {
        return;
    }
    let mut s = state().lock().expect("ehdb metrics lock");
    s.eventlog.record(
        vec![
            ("operation".to_string(), operation.to_string()),
            ("outcome".to_string(), outcome.to_string()),
        ],
        ok,
        degraded,
        duration_seconds,
    );
}

/// Record one projection shadow op (EHDB Phase 7).  `disabled` outcomes are not
/// recorded, preserving the byte-identical `/metrics` invariant.
pub fn record_projection(
    operation: &str,
    outcome: &str,
    ok: bool,
    degraded: bool,
    duration_seconds: f64,
) {
    if outcome == "disabled" {
        return;
    }
    let mut s = state().lock().expect("ehdb metrics lock");
    s.projection.record(
        vec![
            ("operation".to_string(), operation.to_string()),
            ("outcome".to_string(), outcome.to_string()),
        ],
        ok,
        degraded,
        duration_seconds,
    );
}

/// Record one KV shadow op (EHDB Phase 8).  `disabled` outcomes are not
/// recorded, preserving the byte-identical `/metrics` invariant.
pub fn record_kv(operation: &str, outcome: &str, ok: bool, degraded: bool, duration_seconds: f64) {
    if outcome == "disabled" {
        return;
    }
    let mut s = state().lock().expect("ehdb metrics lock");
    s.kv.record(
        vec![
            ("operation".to_string(), operation.to_string()),
            ("outcome".to_string(), outcome.to_string()),
        ],
        ok,
        degraded,
        duration_seconds,
    );
}

/// Record one object/blob shadow op (EHDB Phase 8).  `disabled` outcomes are not
/// recorded, preserving the byte-identical `/metrics` invariant.
pub fn record_object(
    operation: &str,
    outcome: &str,
    ok: bool,
    degraded: bool,
    duration_seconds: f64,
) {
    if outcome == "disabled" {
        return;
    }
    let mut s = state().lock().expect("ehdb metrics lock");
    s.object.record(
        vec![
            ("operation".to_string(), operation.to_string()),
            ("outcome".to_string(), outcome.to_string()),
        ],
        ok,
        degraded,
        duration_seconds,
    );
}

/// Record one vector shadow op (EHDB Phase 8).  `disabled` outcomes are not
/// recorded, preserving the byte-identical `/metrics` invariant.
pub fn record_vector(
    operation: &str,
    outcome: &str,
    ok: bool,
    degraded: bool,
    duration_seconds: f64,
) {
    if outcome == "disabled" {
        return;
    }
    let mut s = state().lock().expect("ehdb metrics lock");
    s.vector.record(
        vec![
            ("operation".to_string(), operation.to_string()),
            ("outcome".to_string(), outcome.to_string()),
        ],
        ok,
        degraded,
        duration_seconds,
    );
}

/// Render all EHDB metric families as Prometheus text lines.  Returns an empty
/// vec when no non-disabled EHDB op has run (the disabled/no-op case), so the
/// worker `/metrics` output stays byte-identical.
pub fn render_lines() -> Vec<String> {
    let s = state().lock().expect("ehdb metrics lock");
    let mut lines = Vec::new();

    if !s.readiness.is_empty() {
        lines.push(
            "# HELP noetl_ehdb_readiness_checks_total EHDB readiness checks by outcome".to_string(),
        );
        lines.push("# TYPE noetl_ehdb_readiness_checks_total counter".to_string());
        for (labels, count) in &s.readiness.counts {
            lines.push(format!(
                "noetl_ehdb_readiness_checks_total{} {count}",
                render_labels(labels)
            ));
        }
        lines.push(
            "# HELP noetl_ehdb_readiness_ready Last EHDB readiness gate result (1=ready)"
                .to_string(),
        );
        lines.push("# TYPE noetl_ehdb_readiness_ready gauge".to_string());
        lines.push(format!("noetl_ehdb_readiness_ready {}", s.readiness.last_a));
        lines.push(
            "# HELP noetl_ehdb_readiness_degraded Last EHDB readiness degraded flag".to_string(),
        );
        lines.push("# TYPE noetl_ehdb_readiness_degraded gauge".to_string());
        lines.push(format!(
            "noetl_ehdb_readiness_degraded {}",
            s.readiness.last_degraded
        ));
        lines.push(
            "# HELP noetl_ehdb_readiness_last_duration_seconds Last EHDB readiness duration"
                .to_string(),
        );
        lines.push("# TYPE noetl_ehdb_readiness_last_duration_seconds gauge".to_string());
        lines.push(format!(
            "noetl_ehdb_readiness_last_duration_seconds {:.6}",
            s.readiness.last_duration_seconds
        ));
    }

    render_op_family(
        &mut lines,
        &s.dataplane,
        "dataplane",
        "EHDB data-plane operations by operation and outcome",
    );
    render_op_family(
        &mut lines,
        &s.eventstream,
        "eventstream",
        "EHDB event-stream operations by operation and outcome",
    );
    render_op_family(
        &mut lines,
        &s.systemstore,
        "systemstore",
        "EHDB system WASM library store operations by operation and outcome",
    );
    render_op_family(
        &mut lines,
        &s.rag,
        "rag",
        "EHDB RAG retrieval/ingest operations by operation and outcome",
    );
    render_op_family(
        &mut lines,
        &s.eventlog,
        "eventlog",
        "EHDB event-log operations (shadow mirror + Phase-9 primary serve) by operation and outcome",
    );
    render_op_family(
        &mut lines,
        &s.projection,
        "projection",
        "EHDB projection read-model shadow operations by operation and outcome",
    );
    render_op_family(
        &mut lines,
        &s.kv,
        "kv",
        "EHDB KV/state shadow operations by operation and outcome",
    );
    render_op_family(
        &mut lines,
        &s.object,
        "object",
        "EHDB object/blob shadow operations by operation and outcome",
    );
    render_op_family(
        &mut lines,
        &s.vector,
        "vector",
        "EHDB vector shadow operations by operation and outcome",
    );

    lines
}

fn render_op_family(lines: &mut Vec<String>, family: &FamilyState, name: &str, help: &str) {
    if family.is_empty() {
        return;
    }
    lines.push(format!("# HELP noetl_ehdb_{name}_ops_total {help}"));
    lines.push(format!("# TYPE noetl_ehdb_{name}_ops_total counter"));
    for (labels, count) in &family.counts {
        lines.push(format!(
            "noetl_ehdb_{name}_ops_total{} {count}",
            render_labels(labels)
        ));
    }
    lines.push(format!(
        "# HELP noetl_ehdb_{name}_last_ok Last EHDB {name} op result (1=ok)"
    ));
    lines.push(format!("# TYPE noetl_ehdb_{name}_last_ok gauge"));
    lines.push(format!("noetl_ehdb_{name}_last_ok {}", family.last_a));
    lines.push(format!(
        "# HELP noetl_ehdb_{name}_last_degraded Last EHDB {name} degraded flag"
    ));
    lines.push(format!("# TYPE noetl_ehdb_{name}_last_degraded gauge"));
    lines.push(format!(
        "noetl_ehdb_{name}_last_degraded {}",
        family.last_degraded
    ));
    lines.push(format!(
        "# HELP noetl_ehdb_{name}_last_duration_seconds Last EHDB {name} op duration"
    ));
    lines.push(format!(
        "# TYPE noetl_ehdb_{name}_last_duration_seconds gauge"
    ));
    lines.push(format!(
        "noetl_ehdb_{name}_last_duration_seconds {:.6}",
        family.last_duration_seconds
    ));
}

fn render_labels(labels: &[(String, String)]) -> String {
    let refs: Vec<(&str, String)> = labels
        .iter()
        .map(|(k, v)| (k.as_str(), v.clone()))
        .collect();
    format_labels(&refs)
}

/// Reset the process-local accumulators (test helper only).
#[cfg(test)]
pub fn reset() {
    let mut s = state().lock().expect("ehdb metrics lock");
    *s = EhdbMetricsState::default();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_records_nothing() {
        reset();
        record_readiness("disabled", true, false, 0.0);
        record_dataplane("append", "disabled", true, false, 0.0);
        record_eventstream("project", "disabled", true, false, 0.0);
        record_systemstore("publish", "disabled", true, false, 0.0);
        record_rag("retrieve", "disabled", true, false, 0.0);
        record_eventlog("mirror", "disabled", true, false, 0.0);
        record_projection("materialize", "disabled", true, false, 0.0);
        record_kv("mirror", "disabled", true, false, 0.0);
        record_object("mirror", "disabled", true, false, 0.0);
        record_vector("mirror", "disabled", true, false, 0.0);
        assert!(render_lines().is_empty());
    }

    #[test]
    fn readiness_render_shape() {
        reset();
        record_readiness("ready", true, false, 0.001234);
        let text = render_lines().join("\n");
        assert!(text.contains("noetl_ehdb_readiness_checks_total{outcome=\"ready\"} 1"));
        assert!(text.contains("noetl_ehdb_readiness_ready 1"));
        reset();
    }
}
