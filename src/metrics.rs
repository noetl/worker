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
//! | `noetl_worker_nats_consumer_pending` | gauge | `stream`, `consumer` | JetStream messages not yet delivered to a consumer (backlog the worker hasn't seen yet) |
//! | `noetl_worker_nats_consumer_ack_pending` | gauge | `stream`, `consumer` | Messages delivered but not yet ack'd (live in-flight work) |
//! | `noetl_worker_result_store_put_duration_seconds` | histogram | — | Durable result-store PUT latency (the cross-node reference path on `call.done` events) |
//! | `noetl_worker_result_store_put_bytes_total` | counter | — | Total bytes staged in the durable result store |
//! | `noetl_worker_result_store_put_errors_total` | counter | — | Durable result-store PUT failures (fall back to shm-cache-only or status-only) |
//! | `noetl_worker_call_done_skipped_pending_callback_total` | counter | `tool_kind` | Times the worker skipped its own `call.done` emit because the tool set `ToolResult.pending_callback = Some(true)` (the terminal event arrives via an async callback path; today only `Tool::Container` sets this — see noetl/ai-meta#43 Round 4) |
//!
//! `pending` + `ack_pending` together is the queue-depth signal KEDA
//! and the dashboard read to decide whether to scale the worker pool.
//! The gauge labels are stable (`stream`, `consumer`) so a multi-
//! consumer deployment gets one series per consumer without label
//! cardinality blow-up.
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
    CounterVec, Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, IntGaugeVec, Registry, TextEncoder,
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
    pub nats_consumer_pending: IntGaugeVec,
    pub nats_consumer_ack_pending: IntGaugeVec,
    pub result_store_put_duration_seconds: Histogram,
    pub result_store_put_bytes_total: IntCounter,
    pub result_store_put_errors_total: IntCounter,
    pub call_done_skipped_pending_callback_total: IntCounterVec,
    /// Messages received by the continuous subscription runtime, by source.
    pub subscription_messages_received_total: IntCounterVec,
    /// Per-message executions the runtime dispatched, by source + outcome
    /// (`dispatched` / `error`).
    pub subscription_executions_total: IntCounterVec,
    /// Header directives the runtime applied, by control kind.
    pub subscription_directives_applied_total: IntCounterVec,
    /// Messages written to the store-and-forward spool, by source
    /// (RFC #90 Phase 4 §8).
    pub subscription_spooled_total: IntCounterVec,
    /// Circuit-breaker transitions, by downstream + transition
    /// (`opened` / `closed`).
    pub subscription_circuit_transitions_total: IntCounterVec,
    /// Messages dead-lettered (poison / evicted / expired), by source.
    pub subscription_dead_lettered_total: IntCounterVec,
    /// Live spool size in bytes, by source — the cost ceiling gauge (OQ3).
    pub subscription_spool_bytes: IntGaugeVec,
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

        // NATS consumer-lag gauges — populated by a periodic poll task
        // (see `crate::nats::lag_poller`).  `pending` is the backlog
        // the worker hasn't seen yet; `ack_pending` is live in-flight
        // work.  Together they're the queue-depth signal KEDA reads
        // to decide whether to scale.
        let nats_consumer_pending = IntGaugeVec::new(
            prometheus::Opts::new(
                "noetl_worker_nats_consumer_pending",
                "JetStream messages not yet delivered to a consumer.",
            ),
            &["stream", "consumer"],
        )
        .expect("nats_consumer_pending metric");
        registry
            .register(Box::new(nats_consumer_pending.clone()))
            .expect("register nats_consumer_pending");

        let nats_consumer_ack_pending = IntGaugeVec::new(
            prometheus::Opts::new(
                "noetl_worker_nats_consumer_ack_pending",
                "JetStream messages delivered to a consumer but not yet ack'd.",
            ),
            &["stream", "consumer"],
        )
        .expect("nats_consumer_ack_pending metric");
        registry
            .register(Box::new(nats_consumer_ack_pending.clone()))
            .expect("register nats_consumer_ack_pending");

        // Durable result-store metrics — populated on the over-budget
        // `call.done` path inside `executor::command::build_call_done_result`.
        // Histogram covers PUT round-trip; counters track total bytes
        // staged + total errors so operators can spot a network outage
        // or sudden bandwidth spike.  No labels — the worker only has
        // one durable store endpoint (the control plane) so the labels
        // would all collapse to a single series.
        let result_store_put_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "noetl_worker_result_store_put_duration_seconds",
                "Latency of one durable result-store PUT (control-plane round-trip).",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
        )
        .expect("result_store_put_duration_seconds metric");
        registry
            .register(Box::new(result_store_put_duration_seconds.clone()))
            .expect("register result_store_put_duration_seconds");

        let result_store_put_bytes_total = IntCounter::new(
            "noetl_worker_result_store_put_bytes_total",
            "Total bytes staged in the durable result store across all successful PUTs.",
        )
        .expect("result_store_put_bytes_total metric");
        registry
            .register(Box::new(result_store_put_bytes_total.clone()))
            .expect("register result_store_put_bytes_total");

        let result_store_put_errors_total = IntCounter::new(
            "noetl_worker_result_store_put_errors_total",
            "Total durable result-store PUT failures (fall back to shm-cache-only or status-only).",
        )
        .expect("result_store_put_errors_total metric");
        registry
            .register(Box::new(result_store_put_errors_total.clone()))
            .expect("register result_store_put_errors_total");

        // noetl/ai-meta#43 Round 4 — pending_callback adoption.  When a
        // tool sets `ToolResult.pending_callback = Some(true)` the
        // worker skips its own `call.done` emit because the terminal
        // event arrives asynchronously via a callback (e.g. the K8s
        // watcher → `POST /api/internal/container-callback/...` path
        // for `Tool::Container`).  Counted per `tool_kind` so the
        // dashboard can pair this with the server-side
        // `noetl_container_callback_total{state}` and
        // `noetl_container_callback_stale_total{state}` counters —
        // healthy steady state is `skipped_total ≈ container_callback_total`
        // with `container_callback_stale_total` near zero.
        let call_done_skipped_pending_callback_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_worker_call_done_skipped_pending_callback_total",
                "Times the worker skipped its own call.done emit because the tool set ToolResult.pending_callback (the terminal event arrives via an async callback path).",
            ),
            &["tool_kind"],
        )
        .expect("call_done_skipped_pending_callback_total metric");
        registry
            .register(Box::new(call_done_skipped_pending_callback_total.clone()))
            .expect("register call_done_skipped_pending_callback_total");

        let subscription_messages_received_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_subscription_messages_received_total",
                "Messages received by the continuous subscription runtime, by source.",
            ),
            &["source"],
        )
        .expect("subscription_messages_received_total metric");
        registry
            .register(Box::new(subscription_messages_received_total.clone()))
            .expect("register subscription_messages_received_total");

        let subscription_executions_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_subscription_executions_total",
                "Per-message executions dispatched by the subscription runtime, by source + outcome.",
            ),
            &["source", "outcome"],
        )
        .expect("subscription_executions_total metric");
        registry
            .register(Box::new(subscription_executions_total.clone()))
            .expect("register subscription_executions_total");

        let subscription_directives_applied_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_subscription_directives_applied_total",
                "Header directives applied by the subscription runtime, by control kind.",
            ),
            &["controls"],
        )
        .expect("subscription_directives_applied_total metric");
        registry
            .register(Box::new(subscription_directives_applied_total.clone()))
            .expect("register subscription_directives_applied_total");

        let subscription_spooled_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_subscription_spooled_total",
                "Messages written to the store-and-forward spool, by source (RFC #90 Phase 4).",
            ),
            &["source"],
        )
        .expect("subscription_spooled_total metric");
        registry
            .register(Box::new(subscription_spooled_total.clone()))
            .expect("register subscription_spooled_total");

        let subscription_circuit_transitions_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_subscription_circuit_transitions_total",
                "Circuit-breaker transitions, by downstream + transition.",
            ),
            &["downstream", "transition"],
        )
        .expect("subscription_circuit_transitions_total metric");
        registry
            .register(Box::new(subscription_circuit_transitions_total.clone()))
            .expect("register subscription_circuit_transitions_total");

        let subscription_dead_lettered_total = IntCounterVec::new(
            prometheus::Opts::new(
                "noetl_subscription_dead_lettered_total",
                "Messages dead-lettered from the spool (poison / evicted / expired), by source.",
            ),
            &["source"],
        )
        .expect("subscription_dead_lettered_total metric");
        registry
            .register(Box::new(subscription_dead_lettered_total.clone()))
            .expect("register subscription_dead_lettered_total");

        let subscription_spool_bytes = IntGaugeVec::new(
            prometheus::Opts::new(
                "noetl_subscription_spool_bytes",
                "Live store-and-forward spool size in bytes, by source — the cost ceiling gauge.",
            ),
            &["source"],
        )
        .expect("subscription_spool_bytes metric");
        registry
            .register(Box::new(subscription_spool_bytes.clone()))
            .expect("register subscription_spool_bytes");

        Self {
            registry,
            pulls_total,
            pull_duration_seconds,
            dispatch_duration_seconds,
            dispatch_errors_total,
            event_emit_duration_seconds,
            event_emit_retries_total,
            concurrent_dispatches,
            nats_consumer_pending,
            nats_consumer_ack_pending,
            result_store_put_duration_seconds,
            result_store_put_bytes_total,
            result_store_put_errors_total,
            call_done_skipped_pending_callback_total,
            subscription_messages_received_total,
            subscription_executions_total,
            subscription_spooled_total,
            subscription_circuit_transitions_total,
            subscription_dead_lettered_total,
            subscription_spool_bytes,
            subscription_directives_applied_total,
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

/// Record subscription-runtime activity for one poll batch
/// (noetl/ai-meta#90 Phase 2).  `received` messages, of which
/// `dispatched` turned into executions and `errors` failed to dispatch.
pub fn record_subscription_batch(source: &str, received: u64, dispatched: u64, errors: u64) {
    let m = WorkerMetrics::global();
    if received > 0 {
        m.subscription_messages_received_total
            .with_label_values(&[source])
            .inc_by(received);
    }
    if dispatched > 0 {
        m.subscription_executions_total
            .with_label_values(&[source, "dispatched"])
            .inc_by(dispatched);
    }
    if errors > 0 {
        m.subscription_executions_total
            .with_label_values(&[source, "error"])
            .inc_by(errors);
    }
}

/// Record one applied header directive, by control kind.
pub fn record_subscription_directive(controls: &str) {
    WorkerMetrics::global()
        .subscription_directives_applied_total
        .with_label_values(&[controls])
        .inc();
}

/// Record one message written to the spool (RFC #90 Phase 4 §8).
pub fn record_subscription_spooled(source: &str) {
    WorkerMetrics::global()
        .subscription_spooled_total
        .with_label_values(&[source])
        .inc();
}

/// Record a circuit-breaker transition (`opened` / `closed`) for a downstream.
pub fn record_subscription_circuit(downstream: &str, transition: &str) {
    WorkerMetrics::global()
        .subscription_circuit_transitions_total
        .with_label_values(&[downstream, transition])
        .inc();
}

/// Record one dead-lettered message (poison / evicted / expired).
pub fn record_subscription_dead_lettered(source: &str) {
    WorkerMetrics::global()
        .subscription_dead_lettered_total
        .with_label_values(&[source])
        .inc();
}

/// Set the live spool byte total for a source — the cost-ceiling gauge.
pub fn set_subscription_spool_bytes(source: &str, bytes: u64) {
    WorkerMetrics::global()
        .subscription_spool_bytes
        .with_label_values(&[source])
        .set(bytes as i64);
}

/// Bump the in-flight dispatches gauge when a permit is acquired.
pub fn inc_concurrent_dispatches() {
    WorkerMetrics::global().concurrent_dispatches.inc();
}

/// Drop the in-flight dispatches gauge when a permit is released.
pub fn dec_concurrent_dispatches() {
    WorkerMetrics::global().concurrent_dispatches.dec();
}

/// Update the NATS consumer-lag gauges for one (`stream`, `consumer`)
/// pair.  Called by the periodic lag poller after fetching consumer
/// info from JetStream.  Both values are `i64` because the underlying
/// `IntGaugeVec` takes signed values; the JetStream API returns
/// `u64` so this is a `try_into` away in the caller.
pub fn record_nats_consumer_lag(stream: &str, consumer: &str, pending: i64, ack_pending: i64) {
    let m = WorkerMetrics::global();
    m.nats_consumer_pending
        .with_label_values(&[stream, consumer])
        .set(pending);
    m.nats_consumer_ack_pending
        .with_label_values(&[stream, consumer])
        .set(ack_pending);
}

/// Record one successful durable result-store PUT.  `bytes` is the
/// serialised size of the payload that was staged; the helper bumps
/// the bytes counter + observes the duration histogram.  Failures
/// use [`record_result_store_put_error`] which doesn't touch the
/// duration histogram (so percentiles only reflect successful PUTs;
/// the error counter is the separate signal for failure rate).
pub fn record_result_store_put(duration_seconds: f64, bytes: usize, _is_error: bool) {
    let m = WorkerMetrics::global();
    m.result_store_put_duration_seconds
        .observe(duration_seconds);
    m.result_store_put_bytes_total.inc_by(bytes as u64);
}

/// Record one failed durable result-store PUT.  Bumps the error
/// counter; the duration histogram is intentionally not touched so
/// percentiles stay clean (an error path tied up in a 30s reqwest
/// timeout would otherwise skew p99 on an otherwise-healthy worker).
pub fn record_result_store_put_error() {
    WorkerMetrics::global().result_store_put_errors_total.inc();
}

/// Record one skipped `call.done` emit driven by
/// `ToolResult.pending_callback = Some(true)`.  Called from
/// [`crate::executor::command`] on the success path after the tool
/// returns.  The `tool_kind` label is the executor's tool kind
/// string (today only `"container"` sets `pending_callback`, but
/// future tools that dispatch long-running external work — e.g. a
/// future GCP Batch / AWS Batch / Argo Workflow tool — would land
/// on the same counter under their own kind label).
pub fn record_call_done_skipped_pending_callback(tool_kind: &str) {
    WorkerMetrics::global()
        .call_done_skipped_pending_callback_total
        .with_label_values(&[tool_kind])
        .inc();
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

    /// `record_nats_consumer_lag` is the only path that touches the
    /// new gauges; this test exercises it directly + verifies the
    /// label set is what the dashboard / KEDA expects.
    #[test]
    fn record_nats_consumer_lag_updates_both_gauges() {
        let m = WorkerMetrics::global();
        record_nats_consumer_lag("noetl_commands", "worker-pool", 42, 7);
        let pending = m
            .nats_consumer_pending
            .with_label_values(&["noetl_commands", "worker-pool"])
            .get();
        let ack_pending = m
            .nats_consumer_ack_pending
            .with_label_values(&["noetl_commands", "worker-pool"])
            .get();
        assert_eq!(pending, 42);
        assert_eq!(ack_pending, 7);

        // Re-recording overwrites the previous sample (gauges
        // aren't cumulative).
        record_nats_consumer_lag("noetl_commands", "worker-pool", 100, 3);
        let pending2 = m
            .nats_consumer_pending
            .with_label_values(&["noetl_commands", "worker-pool"])
            .get();
        assert_eq!(pending2, 100);
    }

    /// The two new gauges appear in the encoded Prometheus output
    /// with the `stream` + `consumer` labels.  Locks in the wire
    /// format the KEDA prometheus-trigger scrapes.
    #[test]
    fn nats_consumer_lag_gauges_emit_in_prometheus_text() {
        record_nats_consumer_lag("noetl_commands", "worker-pool", 5, 2);
        let bytes = WorkerMetrics::global().encode();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.contains("# HELP noetl_worker_nats_consumer_pending"));
        assert!(text.contains("# TYPE noetl_worker_nats_consumer_pending gauge"));
        assert!(text.contains(
            "noetl_worker_nats_consumer_pending{consumer=\"worker-pool\",stream=\"noetl_commands\"}"
        ));
        assert!(text.contains("# HELP noetl_worker_nats_consumer_ack_pending"));
        assert!(text.contains("# TYPE noetl_worker_nats_consumer_ack_pending gauge"));
    }

    /// noetl/ai-meta#43 Round 4 — `pending_callback` skip counter.
    /// Verifies the label is `tool_kind`, the counter increments per
    /// call, and the metric surfaces in the encoded Prometheus text.
    #[test]
    fn call_done_skipped_pending_callback_counter_increments_per_tool_kind() {
        let m = WorkerMetrics::global();
        let before_container = m
            .call_done_skipped_pending_callback_total
            .with_label_values(&["container"])
            .get();
        record_call_done_skipped_pending_callback("container");
        record_call_done_skipped_pending_callback("container");
        let after_container = m
            .call_done_skipped_pending_callback_total
            .with_label_values(&["container"])
            .get();
        assert_eq!(
            after_container,
            before_container + 2,
            "two container skips -> counter += 2"
        );

        // Distinct tool_kind labels keep their own series — the
        // dashboard can split by future tools that adopt the marker.
        let before_future = m
            .call_done_skipped_pending_callback_total
            .with_label_values(&["future_async_tool"])
            .get();
        record_call_done_skipped_pending_callback("future_async_tool");
        let after_future = m
            .call_done_skipped_pending_callback_total
            .with_label_values(&["future_async_tool"])
            .get();
        assert_eq!(after_future, before_future + 1);
        // Container series is unchanged by the unrelated label.
        assert_eq!(
            m.call_done_skipped_pending_callback_total
                .with_label_values(&["container"])
                .get(),
            after_container
        );

        let text = String::from_utf8(m.encode()).unwrap();
        assert!(text.contains("# HELP noetl_worker_call_done_skipped_pending_callback_total"));
        assert!(text.contains("# TYPE noetl_worker_call_done_skipped_pending_callback_total counter"));
        assert!(text.contains(
            "noetl_worker_call_done_skipped_pending_callback_total{tool_kind=\"container\"}"
        ));
    }

    /// `record_result_store_put` observes the duration histogram +
    /// bumps the bytes counter on success; `record_result_store_put_error`
    /// bumps the error counter independently.  Both metrics must
    /// surface in the encoded Prometheus text so dashboards can scrape
    /// them.
    #[test]
    fn result_store_metrics_round_trip_through_encode() {
        let m = WorkerMetrics::global();
        let before_bytes = m.result_store_put_bytes_total.get();
        let before_errors = m.result_store_put_errors_total.get();

        record_result_store_put(0.025, 200 * 1024, false);
        record_result_store_put_error();

        assert_eq!(
            m.result_store_put_bytes_total.get(),
            before_bytes + 200 * 1024
        );
        assert_eq!(m.result_store_put_errors_total.get(), before_errors + 1);

        let text = String::from_utf8(m.encode()).unwrap();
        assert!(text.contains("# HELP noetl_worker_result_store_put_duration_seconds"));
        assert!(text.contains("# TYPE noetl_worker_result_store_put_duration_seconds histogram"));
        assert!(text.contains("# HELP noetl_worker_result_store_put_bytes_total"));
        assert!(text.contains("# TYPE noetl_worker_result_store_put_bytes_total counter"));
        assert!(text.contains("# HELP noetl_worker_result_store_put_errors_total"));
        assert!(text.contains("# TYPE noetl_worker_result_store_put_errors_total counter"));
    }
}
