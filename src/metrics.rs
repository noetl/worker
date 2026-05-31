//! Prometheus metrics for the worker.
//!
//! Per [`agents/rules/observability.md`][rule] Principle 2 ("metrics
//! over logs"), every boundary call ships at least one metric.  This
//! module defines the worker's `noetl_worker_*` counter / histogram /
//! gauge inventory, lazy-initialised under a single global registry,
//! and exposed via the [`metrics_server`][crate::metrics_server]
//! `/metrics` endpoint.
//!
//! ## Inventory
//!
//! | Metric | Type | Labels | Purpose |
//! | :---- | :---- | :---- | :---- |
//! | `noetl_worker_pulls_total` | counter | `outcome` ∈ {claimed, already_claimed, retry_later, failed} | Pull rate + outcome distribution |
//! | `noetl_worker_pull_duration_seconds` | histogram | — | NATS pull + claim round-trip latency |
//! | `noetl_worker_dispatch_duration_seconds` | histogram | `tool_kind` | Per-tool-kind dispatch latency (where bottlenecks hide) |
//! | `noetl_worker_dispatch_errors_total` | counter | `tool_kind` | Per-tool failure rate |
//! | `noetl_worker_event_emit_duration_seconds` | histogram | `event_type` | Event-log write latency to the control plane |
//! | `noetl_worker_event_emit_retries_total` | counter | `event_type` | Retry rate on flaky control-plane writes |
//! | `noetl_worker_concurrent_dispatches` | gauge | — | Live count of in-flight dispatches (semaphore depth) |
//!
//! NATS consumer lag is a follow-up — it requires a periodic poll
//! against the JetStream consumer info API which isn't yet wired in.
//!
//! ## Why a thin facade
//!
//! `lazy_static!`-style global state for metrics is the Prometheus
//! Rust crate's intended pattern.  Wrapping each metric in a typed
//! function (`record_pull(outcome, duration)`,
//! `record_dispatch(tool_kind, duration, error)`) keeps call sites
//! tidy and makes label-typo regressions impossible — `outcome` is
//! an enum, not a free-form string.
//!
//! [rule]: https://github.com/noetl/ai-meta/blob/main/agents/rules/observability.md

use prometheus::{
    CounterVec, Encoder, Histogram, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Registry,
    TextEncoder,
};
use std::sync::OnceLock;

use noetl_executor::worker::source::ClaimOutcome;

/// The Prometheus text-format MIME type — what `/metrics` returns.
pub const METRICS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Outcome label values for `noetl_worker_pulls_total`.  Enum so the
/// label is typo-proof — `outcome_label(ClaimOutcome::Claimed)`
/// returns `"claimed"`, etc.
pub fn outcome_label(outcome: &ClaimOutcome) -> &'static str {
    match outcome {
        ClaimOutcome::Claimed(_) => "claimed",
        ClaimOutcome::AlreadyClaimed => "already_claimed",
        ClaimOutcome::RetryLater(_) => "retry_later",
        ClaimOutcome::Failed(_) => "failed",
    }
}

/// Holds every metric the worker exports.  Single-init via
/// [`WorkerMetrics::global`].
pub struct WorkerMetrics {
    pub registry: Registry,
    pub pulls_total: IntCounterVec,
    pub pull_duration_seconds: Histogram,
    pub dispatch_duration_seconds: HistogramVec,
    pub dispatch_errors_total: IntCounterVec,
    pub event_emit_duration_seconds: HistogramVec,
    pub event_emit_retries_total: IntCounterVec,
    pub concurrent_dispatches: IntGauge,
}

impl WorkerMetrics {
    fn new() -> Self {
        let registry = Registry::new();

        let pulls_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_pulls_total",
                "Total commands pulled from the source, partitioned by claim outcome.",
            ),
            &["outcome"],
        )
        .expect("pulls_total metric");
        registry
            .register(Box::new(pulls_total.clone()))
            .expect("register pulls_total");

        let pull_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "noetl_worker_pull_duration_seconds",
                "Latency of one pull (NATS receive + control-plane claim).",
            )
            .buckets(vec![
                0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0,
            ]),
        )
        .expect("pull_duration_seconds metric");
        registry
            .register(Box::new(pull_duration_seconds.clone()))
            .expect("register pull_duration_seconds");

        let dispatch_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "noetl_worker_dispatch_duration_seconds",
                "Latency of one command dispatch (tool execution + lifecycle events).",
            )
            .buckets(vec![
                0.010, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
            ]),
            &["tool_kind"],
        )
        .expect("dispatch_duration_seconds metric");
        registry
            .register(Box::new(dispatch_duration_seconds.clone()))
            .expect("register dispatch_duration_seconds");

        let dispatch_errors_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_dispatch_errors_total",
                "Total command dispatches that failed, by tool kind.",
            ),
            &["tool_kind"],
        )
        .expect("dispatch_errors_total metric");
        registry
            .register(Box::new(dispatch_errors_total.clone()))
            .expect("register dispatch_errors_total");

        let event_emit_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "noetl_worker_event_emit_duration_seconds",
                "Latency of one event emission to the control plane, by event type.",
            )
            .buckets(vec![
                0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0,
            ]),
            &["event_type"],
        )
        .expect("event_emit_duration_seconds metric");
        registry
            .register(Box::new(event_emit_duration_seconds.clone()))
            .expect("register event_emit_duration_seconds");

        let event_emit_retries_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_event_emit_retries_total",
                "Total event-emission retries triggered by transient failures.",
            ),
            &["event_type"],
        )
        .expect("event_emit_retries_total metric");
        registry
            .register(Box::new(event_emit_retries_total.clone()))
            .expect("register event_emit_retries_total");

        let concurrent_dispatches = IntGauge::new(
            "noetl_worker_concurrent_dispatches",
            "Number of dispatches currently in flight (semaphore depth).",
        )
        .expect("concurrent_dispatches metric");
        registry
            .register(Box::new(concurrent_dispatches.clone()))
            .expect("register concurrent_dispatches");

        Self {
            registry,
            pulls_total,
            pull_duration_seconds,
            dispatch_duration_seconds,
            dispatch_errors_total,
            event_emit_duration_seconds,
            event_emit_retries_total,
            concurrent_dispatches,
        }
    }

    /// Lazily-initialised global metrics instance.
    pub fn global() -> &'static Self {
        static GLOBAL: OnceLock<WorkerMetrics> = OnceLock::new();
        GLOBAL.get_or_init(Self::new)
    }

    /// Encode the registry's current snapshot in Prometheus text
    /// format.  Called by the `/metrics` HTTP handler.
    pub fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder
            .encode(&metric_families, &mut buffer)
            .expect("encode metrics");
        buffer
    }
}

// ---------------------------------------------------------------------------
// Public helpers — call-site-friendly façade over the global metrics.
// ---------------------------------------------------------------------------

/// Record one completed pull (any `ClaimOutcome`).
pub fn record_pull(outcome: &ClaimOutcome, duration_seconds: f64) {
    let m = WorkerMetrics::global();
    m.pulls_total
        .with_label_values(&[outcome_label(outcome)])
        .inc();
    m.pull_duration_seconds.observe(duration_seconds);
}

/// Record one completed dispatch.  `error` is `true` if the tool
/// returned an error (so the errors counter increments alongside the
/// duration histogram).
pub fn record_dispatch(tool_kind: &str, duration_seconds: f64, error: bool) {
    let m = WorkerMetrics::global();
    m.dispatch_duration_seconds
        .with_label_values(&[tool_kind])
        .observe(duration_seconds);
    if error {
        m.dispatch_errors_total
            .with_label_values(&[tool_kind])
            .inc();
    }
}

/// Record one event emission to the control plane.
pub fn record_event_emit(event_type: &str, duration_seconds: f64, retries: u32) {
    let m = WorkerMetrics::global();
    m.event_emit_duration_seconds
        .with_label_values(&[event_type])
        .observe(duration_seconds);
    if retries > 0 {
        m.event_emit_retries_total
            .with_label_values(&[event_type])
            .inc_by(retries as u64);
    }
}

/// Bump the in-flight dispatches gauge when a permit is acquired.
pub fn inc_concurrent_dispatches() {
    WorkerMetrics::global().concurrent_dispatches.inc();
}

/// Drop the in-flight dispatches gauge when a permit is released.
pub fn dec_concurrent_dispatches() {
    WorkerMetrics::global().concurrent_dispatches.dec();
}

// Unused-warning suppression for fields that aren't read directly
// outside the helper functions.  The fields ARE used via the
// registry's encode() output; this just keeps clippy quiet.
#[allow(dead_code)]
const _: () = {
    let _ = &CounterVec::new;
};

#[cfg(test)]
mod tests {
    use super::*;
    use noetl_executor::worker::source::Command;

    fn dummy_command(id: &str) -> Command {
        Command {
            command_id: id.to_string(),
            execution_id: 1,
            step: "s".to_string(),
            tool_kind: "http".to_string(),
            input: serde_json::Value::Null,
            render_context: Default::default(),
            attempts: 0,
        }
    }

    #[test]
    fn outcome_label_returns_distinct_strings() {
        assert_eq!(
            outcome_label(&ClaimOutcome::Claimed(dummy_command("c"))),
            "claimed"
        );
        assert_eq!(
            outcome_label(&ClaimOutcome::AlreadyClaimed),
            "already_claimed"
        );
        assert_eq!(
            outcome_label(&ClaimOutcome::RetryLater("e".into())),
            "retry_later"
        );
        assert_eq!(outcome_label(&ClaimOutcome::Failed("e".into())), "failed");
    }

    #[test]
    fn record_pull_increments_counter_and_histogram() {
        let m = WorkerMetrics::global();
        let before = m.pulls_total.with_label_values(&["claimed"]).get();
        record_pull(&ClaimOutcome::Claimed(dummy_command("c")), 0.012);
        let after = m.pulls_total.with_label_values(&["claimed"]).get();
        assert_eq!(after, before + 1);
        // Histogram sample count must increase too.
        assert!(m.pull_duration_seconds.get_sample_count() > 0);
    }

    #[test]
    fn record_dispatch_separates_errors_from_successes() {
        let m = WorkerMetrics::global();
        let before_errors = m
            .dispatch_errors_total
            .with_label_values(&["postgres"])
            .get();
        record_dispatch("postgres", 0.5, false);
        record_dispatch("postgres", 0.6, true);
        let after_errors = m
            .dispatch_errors_total
            .with_label_values(&["postgres"])
            .get();
        assert_eq!(
            after_errors,
            before_errors + 1,
            "only error path increments errors counter"
        );
    }

    #[test]
    fn record_event_emit_increments_retries_only_when_present() {
        let m = WorkerMetrics::global();
        let before = m
            .event_emit_retries_total
            .with_label_values(&["command.completed"])
            .get();
        record_event_emit("command.completed", 0.020, 0); // no retries
        let mid = m
            .event_emit_retries_total
            .with_label_values(&["command.completed"])
            .get();
        assert_eq!(mid, before, "no retries -> counter unchanged");
        record_event_emit("command.completed", 0.060, 2); // 2 retries
        let after = m
            .event_emit_retries_total
            .with_label_values(&["command.completed"])
            .get();
        assert_eq!(after, mid + 2, "2 retries -> counter += 2");
    }

    #[test]
    fn concurrent_dispatches_gauge_round_trips() {
        let m = WorkerMetrics::global();
        let baseline = m.concurrent_dispatches.get();
        inc_concurrent_dispatches();
        inc_concurrent_dispatches();
        assert_eq!(m.concurrent_dispatches.get(), baseline + 2);
        dec_concurrent_dispatches();
        dec_concurrent_dispatches();
        assert_eq!(m.concurrent_dispatches.get(), baseline);
    }

    #[test]
    fn encode_emits_prometheus_text_format() {
        record_pull(&ClaimOutcome::Claimed(dummy_command("c")), 0.1);
        let bytes = WorkerMetrics::global().encode();
        let text = String::from_utf8(bytes).unwrap();
        // Standard Prometheus text format starts each metric with
        // a `# HELP` / `# TYPE` comment block.
        assert!(text.contains("# HELP noetl_worker_pulls_total"));
        assert!(text.contains("# TYPE noetl_worker_pulls_total counter"));
        // The counter value line must include the outcome label.
        assert!(text.contains("noetl_worker_pulls_total{outcome=\"claimed\"}"));
    }
}
