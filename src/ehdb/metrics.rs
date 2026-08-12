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

    /// Create a label tuple at 0 without counting an operation.
    ///
    /// This is what makes a zero reading mean "no traffic" rather than "metric
    /// absent".  Prometheus' registry prunes empty families and this renderer
    /// has the same property by construction (see [`render_lines`]), so a
    /// labelled series does not exist until something touches it — and an absent
    /// series is indistinguishable from a broken exporter, the wrong port, or a
    /// pod that predates the metric.
    ///
    /// Deliberately does NOT touch `last_a` / `last_degraded` /
    /// `last_duration_seconds`: a pin is not an operation, and letting it write
    /// the last-op gauges would report a result nothing produced.
    fn pin(&mut self, labels: Vec<(String, String)>) {
        self.counts.entry(labels).or_insert(0);
    }

    fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }
}

/// Upper bounds for the tier-service latency histogram, in seconds.
///
/// Tuned for a loopback/in-cluster TCP request against a local JSONL store: the
/// interesting range is sub-millisecond to a few tens of milliseconds, and the
/// top bucket exists to catch a store that has started to stall rather than to
/// resolve how badly. `+Inf` is emitted by the renderer, not listed here.
const TIER_SERVICE_BUCKETS: [f64; 11] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
];

/// One operation's latency distribution. Bucket counts are stored **raw**
/// (per-bucket, not cumulative) and accumulated at render time — a cumulative
/// representation is easy to increment wrongly and the error is invisible in the
/// output, since any monotonic series looks like a valid histogram.
#[derive(Clone)]
struct HistSeries {
    buckets: [u64; TIER_SERVICE_BUCKETS.len()],
    /// Observations above the largest bound. Rendered only into `+Inf`.
    overflow: u64,
    sum: f64,
    count: u64,
}

impl Default for HistSeries {
    fn default() -> Self {
        Self {
            buckets: [0; TIER_SERVICE_BUCKETS.len()],
            overflow: 0,
            sum: 0.0,
            count: 0,
        }
    }
}

impl HistSeries {
    fn observe(&mut self, seconds: f64) {
        // A negative or NaN duration cannot come from a monotonic Instant, but
        // clamping is cheaper than reasoning about what it would do to `sum`.
        let seconds = if seconds.is_finite() && seconds > 0.0 {
            seconds
        } else {
            0.0
        };
        match TIER_SERVICE_BUCKETS.iter().position(|b| seconds <= *b) {
            Some(i) => self.buckets[i] += 1,
            None => self.overflow += 1,
        }
        self.sum += seconds;
        self.count += 1;
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
    eventlog_gc: FamilyState,
    projection: FamilyState,
    kv: FamilyState,
    object: FamilyState,
    vector: FamilyState,
    query: FamilyState,
    /// Tier-service request latency, keyed by operation (ai-meta#260).
    tier_service_latency: BTreeMap<String, HistSeries>,
    /// Whether [`pin_tier_service_series`] has run — i.e. whether the listener
    /// exists in this process. Gates the store gauges, which would otherwise
    /// render `0` on every worker that has no tier service at all and make the
    /// family look present-but-empty everywhere.
    tier_service_up: bool,
    /// Appends this process has served. A counter, so a restart is visible as a
    /// reset rather than as a plateau.
    tier_service_appends: u64,
    /// Highest `global_sequence` the store has reported — "how many records does
    /// the thing I am about to promote actually hold". Sampled at pin time from
    /// the store file too, so it is meaningful before the first append.
    tier_service_sequence: u64,
    /// Size of the backing store file in bytes, sampled at pin time and after
    /// each successful append.
    tier_service_store_bytes: u64,
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
/// Record that a tier was configured `primary` while this build has no
/// authoritative serve path for it (noetl/ai-meta#247).
///
/// Routed into the tier's own existing family with a distinct
/// `operation="runtime_hook", outcome="primary_not_wired"` label pair rather
/// than a new family, so it renders through the same path and needs no new
/// registry surface.
///
/// Recorded once per tier per process, alongside the WARN — a log line alone is
/// not observable from a dashboard, and the whole failure mode this guards
/// against was "a counter quietly stopped moving and nobody saw it".
pub fn record_primary_not_wired(tier: &str) {
    let (op, outcome) = ("runtime_hook", "primary_not_wired");
    match tier {
        "eventlog" => record_eventlog(op, outcome, true, false, 0.0),
        "projection" => record_projection(op, outcome, true, false, 0.0),
        "kv" => record_kv(op, outcome, true, false, 0.0),
        "object" => record_object(op, outcome, true, false, 0.0),
        "vector" => record_vector(op, outcome, true, false, 0.0),
        _ => {}
    }
}

/// Record one tier-service **client** operation (ai-meta#257 PR 2).
///
/// Routed into the existing `dataplane` family with an explicit
/// `operation="tier_client.<op>"` label rather than a new family, so it renders
/// through the same path and adds no registry surface.  Absent entirely until
/// the client is configured, which is the inertness property PR 2 asserts.
pub fn record_tier_client(op: &str, outcome: &str, ok: bool, degraded: bool, duration_seconds: f64) {
    record_dataplane(
        &format!("tier_client.{op}"),
        outcome,
        ok,
        degraded,
        duration_seconds,
    );
}

// ---------------------------------------------------------------------------
// Tier service — the SERVER half (noetl/ai-meta#260)
//
// Before this, `tier_service.rs` recorded nothing at all: its entire
// observability was four `tracing` lines, three of them on failure paths. That
// is the component `primary` promotes to authoritative, so the only prod-side
// signal for "the tier store is refusing, slowing, or dropping connections" was
// a demotion counter recorded by the CLIENT — which cannot separate "the service
// is unhealthy" from "the network is".
//
// Design, and why it is this shape:
//
//   * **Same family as the client.** `noetl_ehdb_dataplane_ops_total` with
//     `operation="tier_service.<op>"`, mirroring the existing
//     `operation="tier_client.<op>"`. One family, one renderer, no new registry
//     surface, and a dashboard can put the two halves of one request side by
//     side by matching on the prefix.
//   * **One call does both halves.** [`record_tier_service`] increments the
//     counter AND observes the histogram. Two functions would let a call site
//     move one and not the other, which is a defect that renders as a perfectly
//     plausible dashboard.
//   * **Latency is labelled by operation only.** `outcome` is deliberately not a
//     histogram label: it multiplies series for a question ("how slow was the
//     rejection?") nobody asks, and the counter already carries the taxonomy.
// ---------------------------------------------------------------------------

/// Record one handled tier-service request: counter + latency, in one call.
///
/// `op` is the bare operation (`health`, `append`, `read_execution`, `scan`,
/// `unsupported`, `conn`); the `tier_service.` prefix is added here so no call
/// site can spell it differently.
pub fn record_tier_service(
    op: &str,
    outcome: &str,
    ok: bool,
    degraded: bool,
    duration_seconds: f64,
) {
    record_dataplane(
        &format!("tier_service.{op}"),
        outcome,
        ok,
        degraded,
        duration_seconds,
    );
    let mut s = state().lock().expect("ehdb metrics lock");
    s.tier_service_latency
        .entry(op.to_string())
        .or_default()
        .observe(duration_seconds);
}

/// Every (operation, outcome) pair the tier service can produce.
///
/// Enumerated rather than built as a cross-product: a cross-product would invent
/// series that cannot happen (`health` can never be `miss`), and a series that
/// is pinned at 0 forever is a worse lie than an absent one — it asserts that a
/// thing is being watched when nothing can ever move it.
const TIER_SERVICE_SERIES: &[(&str, &str)] = &[
    // Liveness handshake. Carries no tier data and cannot fail server-side.
    ("health", "ok"),
    // Writes. `invalid` is the store refusing a malformed request, `unavailable`
    // is no store configured on this writer, `error` is the store itself.
    ("append", "ok"),
    ("append", "invalid"),
    ("append", "unavailable"),
    ("append", "error"),
    // Reads. hit/miss is the distinction that matters most on a promoted tier:
    // an all-miss read stream means the store is serving, and empty.
    ("read_execution", "hit"),
    ("read_execution", "miss"),
    ("read_execution", "invalid"),
    ("read_execution", "unavailable"),
    ("read_execution", "error"),
    // Scans clamp their limit rather than rejecting it, so there is no `invalid`.
    ("scan", "hit"),
    ("scan", "miss"),
    ("scan", "unavailable"),
    ("scan", "error"),
    // A frame this build does not implement — answered, never dropped.
    ("unsupported", "unsupported"),
    // Connection lifecycle, not a request. Distinguishes a peer that hung up
    // cleanly from one that spoke a protocol this writer does not understand
    // from a listener that is failing to accept at all.
    ("conn", "accepted"),
    ("conn", "closed"),
    ("conn", "protocol_error"),
    ("conn", "write_error"),
    ("conn", "accept_error"),
];

/// Operations that get a latency series pinned at count 0.
const TIER_SERVICE_OPS: &[&str] = &["health", "append", "read_execution", "scan", "unsupported"];

/// Create every tier-service series at 0, once, when the listener comes up.
///
/// **Call this from the bind site and from nowhere else.** The condition is the
/// existence of the component, not the state of any feature flag: if the tier
/// service is running then all of these are things that can happen, so all of
/// them must read 0 rather than be absent. In particular the pin is NOT gated on
/// a store being configured, on the tier being `primary`, or on `NOETL_EHDB_*`
/// — that mistake is [server#315](https://github.com/noetl/server/pull/315),
/// which pinned its publish-skip reasons inside `if publishes_ehdb()` and so
/// left them absent on exactly the configuration whose skip reason someone would
/// be reading.
///
/// The inverse is deliberate too: with no listener, nothing is pinned and the
/// worker's `/metrics` stays byte-identical to a build without this module. That
/// is correct because absence then means "this process has no tier service",
/// which is a true and useful statement — and `noetl_worker_build_info{version}`
/// already answers "is this binary too old to have the metric".
///
/// `store_bytes` / `sequence` are the store's state at startup, so a writer that
/// restarts in front of an existing store reports its real size before serving
/// anything.
pub fn pin_tier_service_series(store_bytes: u64, sequence: u64) {
    let mut s = state().lock().expect("ehdb metrics lock");
    s.tier_service_up = true;
    s.tier_service_store_bytes = store_bytes;
    s.tier_service_sequence = sequence;
    for (op, outcome) in TIER_SERVICE_SERIES {
        s.dataplane.pin(vec![
            ("operation".to_string(), format!("tier_service.{op}")),
            ("outcome".to_string(), outcome.to_string()),
        ]);
    }
    for op in TIER_SERVICE_OPS {
        s.tier_service_latency.entry((*op).to_string()).or_default();
    }
}

/// Record that the durable store grew.
///
/// Separate from [`record_tier_service`] because it describes the **store**, not
/// the request: #260 asks for the store to be observable independently of
/// whether anyone is reading it, so that a tier about to be promoted can be
/// checked for "does it actually hold anything" without generating traffic.
pub fn record_tier_service_append(sequence: u64, store_bytes: u64) {
    let mut s = state().lock().expect("ehdb metrics lock");
    s.tier_service_appends += 1;
    // `max`, not assignment: the sequence must never appear to go backwards on a
    // reordered or concurrent append.
    s.tier_service_sequence = s.tier_service_sequence.max(sequence);
    s.tier_service_store_bytes = store_bytes;
}

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

/// Record one durable event-log **segment-GC** pass (the periodic reclaim).
/// `outcome` is `reclaimed` (segments/objects freed), `noop` (nothing eligible),
/// or `error`. Never recorded when GC is disabled, preserving the byte-identical
/// `/metrics` invariant. Aggregate + last-op only — no shard id, path, or error
/// text reaches a label.
pub fn record_eventlog_gc(outcome: &str, ok: bool, degraded: bool, duration_seconds: f64) {
    let mut s = state().lock().expect("ehdb metrics lock");
    s.eventlog_gc.record(
        vec![
            ("operation".to_string(), "reclaim".to_string()),
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

/// Record one read-only tier **query** op (EHDB Data Query Interface,
/// noetl/ai-meta#178).  `disabled` outcomes are not recorded, preserving the
/// byte-identical `/metrics` invariant.  Labelled by tier + operation + outcome;
/// `execution_id` is deliberately NOT a label (cardinality) — it rides the query
/// span instead.
pub fn record_query(
    tier: &str,
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
    s.query.record(
        vec![
            ("tier".to_string(), tier.to_string()),
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
        &s.eventlog_gc,
        "eventlog_gc",
        "EHDB durable event-log segment-GC passes by outcome (reclaimed/noop/error)",
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

    // Tier-service latency + store state (ai-meta#260). Rendered only when the
    // listener exists in this process — see `pin_tier_service_series` for why
    // that is the right condition and why it is not a feature flag.
    if s.tier_service_up {
        lines.push(
            "# HELP noetl_ehdb_tier_service_duration_seconds EHDB tier-service request latency by operation"
                .to_string(),
        );
        lines.push("# TYPE noetl_ehdb_tier_service_duration_seconds histogram".to_string());
        for (op, h) in &s.tier_service_latency {
            let op_label = format_labels(&[("operation", op.clone())]);
            // Prometheus histogram buckets are CUMULATIVE. Accumulate here from
            // the raw per-bucket counts rather than maintaining a cumulative
            // representation at observe time, where an off-by-one is invisible
            // in the output: any monotonic series still parses as a histogram.
            let mut cumulative = 0u64;
            for (i, bound) in TIER_SERVICE_BUCKETS.iter().enumerate() {
                cumulative += h.buckets[i];
                lines.push(format!(
                    "noetl_ehdb_tier_service_duration_seconds_bucket{} {cumulative}",
                    format_labels(&[("operation", op.clone()), ("le", format!("{bound}"))])
                ));
            }
            cumulative += h.overflow;
            lines.push(format!(
                "noetl_ehdb_tier_service_duration_seconds_bucket{} {cumulative}",
                format_labels(&[("operation", op.clone()), ("le", "+Inf".to_string())])
            ));
            lines.push(format!(
                "noetl_ehdb_tier_service_duration_seconds_sum{op_label} {:.6}",
                h.sum
            ));
            lines.push(format!(
                "noetl_ehdb_tier_service_duration_seconds_count{op_label} {}",
                h.count
            ));
        }

        lines.push(
            "# HELP noetl_ehdb_tier_service_store_appends_total Records appended to the tier-service durable store by this process"
                .to_string(),
        );
        lines.push("# TYPE noetl_ehdb_tier_service_store_appends_total counter".to_string());
        lines.push(format!(
            "noetl_ehdb_tier_service_store_appends_total {}",
            s.tier_service_appends
        ));
        lines.push(
            "# HELP noetl_ehdb_tier_service_store_sequence Highest global sequence the tier-service store holds"
                .to_string(),
        );
        lines.push("# TYPE noetl_ehdb_tier_service_store_sequence gauge".to_string());
        lines.push(format!(
            "noetl_ehdb_tier_service_store_sequence {}",
            s.tier_service_sequence
        ));
        lines.push(
            "# HELP noetl_ehdb_tier_service_store_bytes Size on disk of the tier-service durable store"
                .to_string(),
        );
        lines.push("# TYPE noetl_ehdb_tier_service_store_bytes gauge".to_string());
        lines.push(format!(
            "noetl_ehdb_tier_service_store_bytes {}",
            s.tier_service_store_bytes
        ));
    }

    // The read-only tier-query family uses the `noetl_worker_ehdb_query_*` name
    // shape (worker-scoped, distinct from the shadow-mirror `noetl_ehdb_*`
    // families), so it renders with a bespoke block rather than
    // `render_op_family`. Emits the ops counter + last-op gauges, including the
    // `noetl_worker_ehdb_query_duration_seconds` gauge the query interface
    // advertises (observability.md Principle 1).
    if !s.query.is_empty() {
        lines.push(
            "# HELP noetl_worker_ehdb_query_ops_total EHDB read-only tier queries by tier, operation, outcome"
                .to_string(),
        );
        lines.push("# TYPE noetl_worker_ehdb_query_ops_total counter".to_string());
        for (labels, count) in &s.query.counts {
            lines.push(format!(
                "noetl_worker_ehdb_query_ops_total{} {count}",
                render_labels(labels)
            ));
        }
        lines.push(
            "# HELP noetl_worker_ehdb_query_last_ok Last EHDB tier-query result (1=ok)".to_string(),
        );
        lines.push("# TYPE noetl_worker_ehdb_query_last_ok gauge".to_string());
        lines.push(format!(
            "noetl_worker_ehdb_query_last_ok {}",
            s.query.last_a
        ));
        lines.push(
            "# HELP noetl_worker_ehdb_query_last_degraded Last EHDB tier-query degraded flag"
                .to_string(),
        );
        lines.push("# TYPE noetl_worker_ehdb_query_last_degraded gauge".to_string());
        lines.push(format!(
            "noetl_worker_ehdb_query_last_degraded {}",
            s.query.last_degraded
        ));
        lines.push(
            "# HELP noetl_worker_ehdb_query_duration_seconds Last EHDB tier-query duration"
                .to_string(),
        );
        lines.push("# TYPE noetl_worker_ehdb_query_duration_seconds gauge".to_string());
        lines.push(format!(
            "noetl_worker_ehdb_query_duration_seconds {:.6}",
            s.query.last_duration_seconds
        ));
    }

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

/// Serialise any test that drives the process-global accumulator.
///
/// `cargo test` does **not** serialise tests within a binary, and since
/// ai-meta#260 the tier-service request path records metrics — so a
/// `tier_service` test serving one health frame can land between another test's
/// [`reset`] and its assertion. That is a genuine cross-module race, not a
/// theoretical one: the same shape (an `EnvGuard` SAFETY note in this crate that
/// claimed `cargo test` serialised) hid a defect that failed 1-5 runs in 15
/// under a narrow filter while the full suite stayed green on scheduling luck.
///
/// Lives at module level rather than inside `mod tests` so `tier_service`'s and
/// `tier_store`'s tests can take the SAME lock. Two locks would not serialise
/// anything.
///
/// Poison-tolerant: one panicking test must not cascade into every other.
#[cfg(test)]
pub(crate) fn test_guard() -> std::sync::MutexGuard<'static, ()> {
    static TEST_LOCK: Mutex<()> = Mutex::new(());
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
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

    /// The tests below drive the same process-global accumulator: they
    /// `reset()` it, record into it, then assert on `render_lines()`.  Cargo
    /// runs tests in parallel threads inside one process, so without this lock
    /// `readiness_render_shape` can record between the other test's `reset()`
    /// and its `assert!(render_lines().is_empty())` — which failed roughly one
    /// run in six, and passed on the retry that anyone would reach for first.
    ///
    /// The state mutex inside `state()` does not help: each individual call is
    /// atomic, but the reset-then-assert *sequence* is not.
    ///
    /// The lock itself moved to [`super::test_guard`] with ai-meta#260, because
    /// the tier-service request path now records too — so tests in OTHER modules
    /// have to take the same one.
    fn serialised() -> std::sync::MutexGuard<'static, ()> {
        super::test_guard()
    }

    #[test]
    fn disabled_records_nothing() {
        let _guard = serialised();
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
        let _guard = serialised();
        reset();
        record_readiness("ready", true, false, 0.001234);
        let text = render_lines().join("\n");
        assert!(text.contains("noetl_ehdb_readiness_checks_total{outcome=\"ready\"} 1"));
        assert!(text.contains("noetl_ehdb_readiness_ready 1"));
        reset();
    }

    // --- tier service (noetl/ai-meta#260) ---

    /// The property the whole pin exists for: with the listener up and no
    /// traffic, every series READS 0 rather than being absent.
    ///
    /// This is the in-binary half of the gate's positive control. Without it,
    /// `rate(...) == 0` on a dashboard is ambiguous between "nothing is calling
    /// the tier service" and "this pod predates the metric / is not the pod you
    /// think / is not being scraped".
    #[test]
    fn pin_creates_every_series_at_zero() {
        let _guard = serialised();
        reset();
        pin_tier_service_series(0, 0);
        let text = render_lines().join("\n");

        for (op, outcome) in TIER_SERVICE_SERIES {
            let want = format!(
                "noetl_ehdb_dataplane_ops_total{{operation=\"tier_service.{op}\",outcome=\"{outcome}\"}} 0"
            );
            assert!(text.contains(&want), "missing pinned series: {want}\n{text}");
        }
        for op in TIER_SERVICE_OPS {
            let want =
                format!("noetl_ehdb_tier_service_duration_seconds_count{{operation=\"{op}\"}} 0");
            assert!(text.contains(&want), "missing pinned histogram: {want}");
        }
        assert!(text.contains("noetl_ehdb_tier_service_store_appends_total 0"));
        assert!(text.contains("noetl_ehdb_tier_service_store_bytes 0"));
        assert!(text.contains("noetl_ehdb_tier_service_store_sequence 0"));
        reset();
    }

    /// The inverse, and the invariant this module was built around: no listener
    /// ⇒ nothing rendered at all. If the pin ever leaks into a process without a
    /// tier service, every worker in the fleet grows 20 series that can never
    /// move.
    #[test]
    fn without_a_listener_the_tier_service_renders_nothing() {
        let _guard = serialised();
        reset();
        assert!(
            render_lines().is_empty(),
            "an unpinned process must render no tier-service lines"
        );
        reset();
    }

    /// A pin must not fabricate a result. Pinning writes 0s into the counter
    /// only — if it also wrote the last-op gauges, `last_ok` would report a
    /// success that never happened.
    #[test]
    fn pin_does_not_write_the_last_op_gauges() {
        let _guard = serialised();
        reset();
        pin_tier_service_series(0, 0);
        let text = render_lines().join("\n");
        assert!(text.contains("noetl_ehdb_dataplane_last_ok 0"), "{text}");
        assert!(
            text.contains("noetl_ehdb_dataplane_last_duration_seconds 0.000000"),
            "{text}"
        );
        reset();
    }

    /// Recording moves exactly the series named, and the pinned neighbours stay
    /// at 0 — the discrimination the gate's mutation arm depends on.
    #[test]
    fn recording_moves_only_the_named_series() {
        let _guard = serialised();
        reset();
        pin_tier_service_series(0, 0);
        record_tier_service("append", "ok", true, false, 0.003);
        record_tier_service("read_execution", "hit", true, false, 0.001);
        record_tier_service("read_execution", "miss", true, false, 0.001);
        record_tier_service("read_execution", "miss", true, false, 0.002);
        let text = render_lines().join("\n");

        let has = |s: &str| text.contains(s);
        assert!(has(
            "noetl_ehdb_dataplane_ops_total{operation=\"tier_service.append\",outcome=\"ok\"} 1"
        ));
        assert!(has(
            "noetl_ehdb_dataplane_ops_total{operation=\"tier_service.read_execution\",outcome=\"hit\"} 1"
        ));
        assert!(has(
            "noetl_ehdb_dataplane_ops_total{operation=\"tier_service.read_execution\",outcome=\"miss\"} 2"
        ));
        // The negative half: untouched pinned series must still read 0, not be
        // absent and not have drifted.
        assert!(has(
            "noetl_ehdb_dataplane_ops_total{operation=\"tier_service.append\",outcome=\"error\"} 0"
        ));
        assert!(has(
            "noetl_ehdb_dataplane_ops_total{operation=\"tier_service.health\",outcome=\"ok\"} 0"
        ));
        // The counter and the histogram move together, by construction.
        assert!(has(
            "noetl_ehdb_tier_service_duration_seconds_count{operation=\"read_execution\"} 3"
        ));
        assert!(has(
            "noetl_ehdb_tier_service_duration_seconds_count{operation=\"append\"} 1"
        ));
        reset();
    }

    /// Buckets must be CUMULATIVE and must end at `+Inf` == count. A histogram
    /// whose buckets are per-bucket rather than cumulative still parses, still
    /// renders, and produces silently wrong quantiles — there is no error
    /// anywhere, which is why this is asserted rather than eyeballed.
    #[test]
    fn histogram_buckets_are_cumulative_and_total_at_inf() {
        let _guard = serialised();
        reset();
        pin_tier_service_series(0, 0);
        // One observation in a low bucket, one mid, one above the top bound.
        record_tier_service("scan", "hit", true, false, 0.0001);
        record_tier_service("scan", "hit", true, false, 0.03);
        record_tier_service("scan", "hit", true, false, 5.0);
        let text = render_lines().join("\n");

        let bucket = |le: &str| -> u64 {
            let needle =
                format!("noetl_ehdb_tier_service_duration_seconds_bucket{{le=\"{le}\",operation=\"scan\"}} ");
            text.lines()
                .find_map(|l| l.strip_prefix(&needle))
                .unwrap_or_else(|| panic!("no bucket le={le} in:\n{text}"))
                .parse()
                .expect("bucket is a number")
        };

        assert_eq!(bucket("0.0005"), 1, "the 0.1ms observation");
        assert_eq!(bucket("0.001"), 1, "cumulative: still just the first");
        assert_eq!(bucket("0.025"), 1, "30ms has not been reached yet");
        assert_eq!(bucket("0.05"), 2, "cumulative: now includes the 30ms one");
        assert_eq!(bucket("1"), 2, "the 5s observation is above every bound");
        assert_eq!(bucket("+Inf"), 3, "+Inf must equal the total count");
        assert!(text
            .contains("noetl_ehdb_tier_service_duration_seconds_count{operation=\"scan\"} 3"));
        reset();
    }

    /// Store state is observable without anyone reading the tier — the question
    /// asked before a `primary` flip.
    #[test]
    fn store_state_tracks_appends_and_never_goes_backwards() {
        let _guard = serialised();
        reset();
        pin_tier_service_series(0, 0);
        record_tier_service_append(7, 512);
        record_tier_service_append(9, 900);
        // An out-of-order sequence must not rewind the gauge.
        record_tier_service_append(4, 950);
        let text = render_lines().join("\n");
        assert!(text.contains("noetl_ehdb_tier_service_store_appends_total 3"), "{text}");
        assert!(text.contains("noetl_ehdb_tier_service_store_sequence 9"), "{text}");
        assert!(text.contains("noetl_ehdb_tier_service_store_bytes 950"), "{text}");
        reset();
    }
}
