//! Serving heartbeats and the readiness predicate (noetl/ai-meta#470).
//!
//! # The blind spot this closes
//!
//! The writer's readiness and liveness probes are `tcpSocket` on `cmdbus-claim`.
//! **A port that accepts a connection and then answers nothing passes a TCP probe
//! forever.** That is not hypothetical: noetl/ai-meta#469 had the writer accept
//! TCP on :9090/:9102/:9106 and never send HTTP headers for over a day across two
//! pod generations, while the pod reported `Ready`, `restarts=0`, every listener
//! logged "up", and there were zero ERROR lines. The tier service has the same
//! shape recorded in its own source: it "went **deaf**" under saturation because
//! it never reached `accept()`, and "the kernel still completed" the handshake.
//!
//! A boolean is the wrong instrument for this. `worker_ready()` (noetl/ai-meta#130)
//! answers "did boot warmup finish" — it is set once and never falsified, so it
//! cannot represent a loop that was serving and stopped.
//!
//! # The instrument
//!
//! Each serving task publishes a **monotonic, instance-stamped heartbeat**, bumped
//! on a timer while the task is still being polled. The value is packed
//! Snowflake-style — `(timestamp_ms << SEQ_BITS) | sequence` — so it is strictly
//! increasing within a process even when two bumps land in the same millisecond,
//! and so a reader can recover the wall-clock component without a second series.
//!
//! The instance id is carried as a **label**, not packed into the integer: it is a
//! uuid and would not fit. A changed `instance_id` is how an external reader tells
//! "this counter reset because the process restarted" from "this counter stopped
//! advancing because the loop wedged" — two facts a bare reset cannot distinguish.
//!
//! # Why the bump proves something
//!
//! The bump is not placed after `accept()`. A loop parked in `accept()` with no
//! traffic is *healthy*, and bumping only on connections would make an idle writer
//! go stale and drop out of rotation. It is also not a free-running timer task,
//! which would prove only that *some* task runs.
//!
//! It is a timer `select!`ed against the serving future **in the same task**, with
//! the future pinned so it survives across iterations:
//!
//! ```ignore
//! tokio::pin!(serving);
//! loop {
//!     tokio::select! {
//!         r = &mut serving => break r,
//!         _ = ticker.tick() => hb.bump(),
//!     }
//! }
//! ```
//!
//! So a bump means *this task was polled recently*. A task that blocks its thread
//! — the tier service's deaf-under-saturation bug, a handler holding the engine
//! lock, a starved runtime — stops bumping and goes stale.
//!
//! ⚠ What it does **not** catch: a loop that is progressing normally but returning
//! wrong answers. This instrument measures **progress**, which is the #469/#470
//! failure class, and nothing more. Saying so is the point — a readiness signal
//! that is believed to mean more than it does is how the `tcpSocket` probe got
//! trusted in the first place.
//!
//! # Readiness, not liveness
//!
//! Only `readinessProbe` consumes this. Liveness stays `tcpSocket`: a freshness
//! gate on liveness would let a slow bus bind **kill** the writer, and this writer
//! has looped fatally on restart before. A `startupProbe` covers the bind window.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Bits reserved for the intra-millisecond sequence.
///
/// 12 bits = 4096 bumps per millisecond before the sequence would wrap. Ticks are
/// milliseconds apart at the fastest, so this is headroom rather than a budget;
/// it exists so two bumps in one millisecond still produce increasing values.
pub const SEQ_BITS: u32 = 12;
const SEQ_MASK: u64 = (1 << SEQ_BITS) - 1;

/// The serving loops a heartbeat is published for.
///
/// ⚠ A fixed enum rather than free-form strings, for the reason every label set in
/// this fleet is fixed: an open label set cannot be pinned, and an unpinned
/// labelled metric is **absent** until it first fires — indistinguishable from a
/// loop that is not running. `ALL` is the denominator the exposition iterates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ServingLoop {
    /// Command-bus ingest (:9100) — where dispatch enters.
    CommandIngest,
    /// Command-bus claim (:9101) — where workers take work out.
    CommandClaim,
    /// Event-bus ingest (:9103) — where events enter.
    EventIngest,
    /// Event-bus claim (:9104) — where event consumers take work out.
    EventClaim,
    /// The durable tier service (:9110).
    TierService,
}

impl ServingLoop {
    pub const ALL: [ServingLoop; 5] = [
        Self::CommandIngest,
        Self::CommandClaim,
        Self::EventIngest,
        Self::EventClaim,
        Self::TierService,
    ];

    /// The DEFAULT loops `/readyz` requires to be fresh.
    ///
    /// ⚠⚠ Two faces, not four, and the reason is a measurement rather than a
    /// preference. Bind times on the prod writer, from pod start 2026-10-16
    /// 17:04:01Z (restarts=0, so these are its real startup logs):
    ///
    /// | face | bound at |
    /// | :-- | --: |
    /// | `:9090` metrics — serves `/readyz` itself | 73.6 s |
    /// | `:9100` command ingest | 74.6 s |
    /// | `:9101` command claim | 74.6 s |
    /// | `:9103` event ingest | 128.0 s |
    /// | `:9110` tier service | 132.5 s |
    /// | `:9104` event claim | **380.4 s** |
    ///
    /// Today's `tcpSocket` on cmdbus-claim turns the pod Ready at ~75 s. A
    /// NotReady pod is removed from EVERY Service that selects it, so requiring
    /// a face that binds later does not merely delay that face — it makes the
    /// faces that ARE up unreachable for the difference. Requiring event ingest
    /// costs +53 s of fleet-wide bus unreachability per writer restart;
    /// requiring event claim costs **+306 s**, which is the same arithmetic as
    /// the C3 enforce trade that was rejected for being too expensive per
    /// restart.
    ///
    /// So the default is the two faces that are already up when the current
    /// probe passes. That makes this change **purely additive**: identical Ready
    /// timing to today, plus wedge detection on the dispatch path the existing
    /// probe pretends to cover. Widening is one env var and is a cost decision,
    /// not a code change — see [`required_loops`].
    pub const DEFAULT_REQUIRED: [ServingLoop; 2] = [Self::CommandIngest, Self::CommandClaim];

    pub fn index(self) -> usize {
        match self {
            Self::CommandIngest => 0,
            Self::CommandClaim => 1,
            Self::EventIngest => 2,
            Self::EventClaim => 3,
            Self::TierService => 4,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::CommandIngest => "command_ingest",
            Self::CommandClaim => "command_claim",
            Self::EventIngest => "event_ingest",
            Self::EventClaim => "event_claim",
            Self::TierService => "tier_service",
        }
    }

    pub fn is_required(self) -> bool {
        required_loops().contains(&self)
    }
}

/// The faces `/readyz` requires, from `NOETL_SERVING_REQUIRED` — a comma-separated
/// list of loop names ([`ServingLoop::as_str`]), or `all`.
///
/// ⚠ Unparseable or unknown names fall back to [`ServingLoop::DEFAULT_REQUIRED`]
/// and log once, rather than silently requiring nothing. A typo that quietly
/// emptied the required set would make `/readyz` answer 200 unconditionally — a
/// readiness probe that cannot fail, which is the defect class this whole module
/// exists to remove.
pub fn required_loops() -> Vec<ServingLoop> {
    static RESOLVED: std::sync::OnceLock<Vec<ServingLoop>> = std::sync::OnceLock::new();
    RESOLVED
        .get_or_init(|| parse_required(std::env::var("NOETL_SERVING_REQUIRED").ok().as_deref()))
        .clone()
}

/// The parse, separated from the env read and the memoization so it is testable
/// without either. `required_loops` is a `OnceLock`, so a test driving it through
/// the environment would depend on which test initialised it first — and
/// `cargo test` does not serialise tests.
pub fn parse_required(raw: Option<&str>) -> Vec<ServingLoop> {
    let raw = match raw {
        Some(v) if !v.trim().is_empty() => v,
        _ => return ServingLoop::DEFAULT_REQUIRED.to_vec(),
    };
    if raw.trim().eq_ignore_ascii_case("all") {
        return ServingLoop::ALL.to_vec();
    }
    let mut out = Vec::new();
    for name in raw.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        match ServingLoop::ALL.iter().find(|l| l.as_str() == name) {
            Some(l) if !out.contains(l) => out.push(*l),
            Some(_) => {}
            None => {
                tracing::warn!(
                    name,
                    raw,
                    "NOETL_SERVING_REQUIRED names an unknown serving loop; falling \
                     back to the default required set rather than requiring nothing"
                );
                return ServingLoop::DEFAULT_REQUIRED.to_vec();
            }
        }
    }
    if out.is_empty() {
        return ServingLoop::DEFAULT_REQUIRED.to_vec();
    }
    out
}

/// `0` means "never bumped", which is distinct from any real packed value because
/// a real one always carries a non-zero millisecond timestamp.
static BEATS: [AtomicU64; 5] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

/// One bit per loop: its listener is bound.
static BOUND: AtomicU64 = AtomicU64::new(0);

/// One bit per loop: this process is CONFIGURED to host that face.
///
/// ⚠⚠ Load-bearing, and the reason the predicate is not simply "all of
/// [`ServingLoop::DEFAULT_REQUIRED`] must be fresh".
///
/// Only the writer host binds the bus faces. A user-pool or drive-pool worker
/// binds none of them — so a predicate that required them unconditionally would
/// hold every other pool permanently NotReady, and `/readyz` is exactly the
/// endpoint noetl/ai-meta#130 added for those pools' cold-start warmup. Gating on
/// what the process was configured to host makes this change **strictly
/// additive**: a process that hosts no buses answers precisely what it answered
/// before, which is the warmup boolean alone.
static EXPECTED: AtomicU64 = AtomicU64::new(0);

/// The wall-clock millisecond component of a packed heartbeat.
pub fn timestamp_of(packed: u64) -> u64 {
    packed >> SEQ_BITS
}

/// The intra-millisecond sequence component of a packed heartbeat.
pub fn sequence_of(packed: u64) -> u64 {
    packed & SEQ_MASK
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The per-process instance identity, memoized.
///
/// ⚠ Reuses the election's `<hostname>#<uuid>` identity deliberately, so an
/// operator correlating a stale heartbeat against the C2 lease holder sees the
/// SAME string rather than two uuids for one process. `election::identity()` is
/// called exactly once today, so memoizing it is behaviour-preserving — and it
/// closes a latent hazard: a second call would have minted a different uuid and,
/// had anything ever made one, the writer would have thrashed its own lease.
pub fn instance_id() -> &'static str {
    crate::ehdb::election::identity()
}

/// Bump `which`'s heartbeat. Monotonic within the process.
pub fn bump(which: ServingLoop) -> u64 {
    let slot = &BEATS[which.index()];
    let ms = now_ms();
    loop {
        let prev = slot.load(Ordering::Acquire);
        let prev_ms = prev >> SEQ_BITS;
        // Same millisecond (or a clock that went backwards): advance the sequence
        // instead, so the value never stalls or regresses. A non-monotonic series
        // would make "did it advance" unanswerable, which is the whole signal.
        let next = if ms > prev_ms {
            ms << SEQ_BITS
        } else {
            prev.saturating_add(1)
        };
        if slot
            .compare_exchange_weak(prev, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return next;
        }
    }
}

/// The raw packed value, or `None` if never bumped.
pub fn beat(which: ServingLoop) -> Option<u64> {
    match BEATS[which.index()].load(Ordering::Acquire) {
        0 => None,
        v => Some(v),
    }
}

/// Milliseconds since `which` last bumped, or `None` if it never has.
pub fn age_ms(which: ServingLoop) -> Option<u64> {
    beat(which).map(|v| now_ms().saturating_sub(v >> SEQ_BITS))
}

/// Record that this process is configured to host `which`.
///
/// Called from the host wiring BEFORE the bind, so a face that was configured and
/// failed (or has not yet finished) binding reads `expected && !bound` — which is
/// a 503, and is the window the `startupProbe` covers.
pub fn mark_expected(which: ServingLoop) {
    EXPECTED.fetch_or(1 << which.index(), Ordering::AcqRel);
}

pub fn is_expected(which: ServingLoop) -> bool {
    EXPECTED.load(Ordering::Acquire) & (1 << which.index()) != 0
}

/// Record that `which`'s listener is bound. Bumps once so a bound-but-never-polled
/// loop is distinguishable from an unbound one.
pub fn mark_bound(which: ServingLoop) {
    BOUND.fetch_or(1 << which.index(), Ordering::AcqRel);
    bump(which);
}

pub fn is_bound(which: ServingLoop) -> bool {
    BOUND.load(Ordering::Acquire) & (1 << which.index()) != 0
}

/// Freshness threshold in milliseconds.
///
/// ⚠ PROPOSED. Derived from the tick interval rather than picked: the bump runs
/// every [`TICK_MS`], so the threshold must exceed normal scheduling jitter under
/// load (several ticks) while staying well under how long a wedged loop should be
/// allowed to stay in rotation. Four ticks is the proposal; the PR asks for a
/// decision. Overridable with `NOETL_SERVING_FRESHNESS_MS` so it can be tuned
/// without a rebuild.
pub const TICK_MS: u64 = 1_000;
pub const DEFAULT_FRESHNESS_MS: u64 = 4_000;

/// The threshold must leave room for scheduling jitter under load, or a
/// busy-but-healthy writer flaps out of rotation. Asserted at COMPILE time
/// rather than in a test, so tuning the constants cannot produce a build that
/// flaps — a runtime test would only have caught it once someone ran the suite.
const _: () = assert!(DEFAULT_FRESHNESS_MS >= 3 * TICK_MS);

pub fn freshness_threshold_ms() -> u64 {
    std::env::var("NOETL_SERVING_FRESHNESS_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_FRESHNESS_MS)
}

/// Why readiness was refused — returned in the probe body so an operator does not
/// have to guess which of three conditions failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotReady {
    Warming,
    Unbound(ServingLoop),
    NeverServed(ServingLoop),
    Stale {
        which: ServingLoop,
        age_ms: u64,
        threshold_ms: u64,
    },
}

impl std::fmt::Display for NotReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Warming => write!(f, "warming: boot warmup has not completed"),
            Self::Unbound(l) => write!(f, "unbound: {} has no listener", l.as_str()),
            Self::NeverServed(l) => write!(f, "never-served: {} has never ticked", l.as_str()),
            Self::Stale {
                which,
                age_ms,
                threshold_ms,
            } => write!(
                f,
                "stale: {} last ticked {age_ms}ms ago, threshold {threshold_ms}ms",
                which.as_str()
            ),
        }
    }
}

/// An immutable sample of everything the predicate reads.
///
/// ⚠ The predicate is pure over this struct rather than reading the globals
/// directly, for a testing reason that is a correctness reason: `cargo test` does
/// **not** serialise tests, so a predicate that mutated global warmup/heartbeat
/// state to set up a case would race every other case in the binary. Separating
/// the sample from the judgement makes every branch below reachable from a pure
/// test — which is what makes a mutation run over them meaningful.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub warm: bool,
    pub now_ms: u64,
    pub threshold_ms: u64,
    /// Packed heartbeat per loop, indexed by [`ServingLoop::index`]; `0` = never.
    pub beats: [u64; 5],
    /// Bitset of bound listeners, indexed by [`ServingLoop::index`].
    pub bound: u64,
    /// Bitset of faces this process is configured to host.
    pub expected: u64,
    /// The faces this sample must judge — captured in the snapshot so the
    /// predicate stays pure and a test can vary it without touching env.
    pub required: Vec<ServingLoop>,
}

/// Sample the live state.
pub fn snapshot() -> Snapshot {
    Snapshot {
        warm: crate::metrics::worker_ready(),
        now_ms: now_ms(),
        threshold_ms: freshness_threshold_ms(),
        beats: [
            BEATS[0].load(Ordering::Acquire),
            BEATS[1].load(Ordering::Acquire),
            BEATS[2].load(Ordering::Acquire),
            BEATS[3].load(Ordering::Acquire),
            BEATS[4].load(Ordering::Acquire),
        ],
        bound: BOUND.load(Ordering::Acquire),
        expected: EXPECTED.load(Ordering::Acquire),
        required: required_loops(),
    }
}

/// The readiness predicate: warmup done, every REQUIRED listener bound, and every
/// REQUIRED loop fresh.
///
/// The freshness comparison happens in-process because a Kubernetes probe is
/// stateless — it cannot compare two scrapes, so it cannot notice that a number
/// stopped moving. The process does the comparison and answers with a status code.
pub fn evaluate(s: &Snapshot) -> Result<(), NotReady> {
    if !s.warm {
        return Err(NotReady::Warming);
    }
    for which in &s.required {
        let which = *which;
        let i = which.index();
        // Not configured on this process — not its job, so not its readiness.
        if s.expected & (1 << i) == 0 {
            continue;
        }
        if s.bound & (1 << i) == 0 {
            return Err(NotReady::Unbound(which));
        }
        let packed = s.beats[i];
        if packed == 0 {
            return Err(NotReady::NeverServed(which));
        }
        let age = s.now_ms.saturating_sub(packed >> SEQ_BITS);
        if age > s.threshold_ms {
            return Err(NotReady::Stale {
                which,
                age_ms: age,
                threshold_ms: s.threshold_ms,
            });
        }
    }
    Ok(())
}

/// [`evaluate`] against the live sample — what `/readyz` answers.
pub fn readiness() -> Result<(), NotReady> {
    evaluate(&snapshot())
}

/// Prometheus exposition for the heartbeats.
///
/// ⚠ Every loop is emitted unconditionally, including ones that have never ticked
/// (as `0`), so an absent series means "this build does not have the metric"
/// rather than "this loop is not running" — two states an operator must be able to
/// tell apart. `instance_id` is a label so a reader can distinguish a restart
/// (id changed, value reset) from a wedge (id same, value frozen).
pub fn render_prometheus() -> String {
    let id = escape_label(instance_id());
    let mut out = String::new();
    out.push_str(
        "# HELP noetl_worker_serving_heartbeat Monotonic (timestamp_ms << 12 | seq) \
         serving-loop heartbeat; frozen value with an unchanged instance_id means the \
         loop stopped making progress.\n# TYPE noetl_worker_serving_heartbeat gauge\n",
    );
    for l in ServingLoop::ALL {
        out.push_str(&format!(
            "noetl_worker_serving_heartbeat{{loop=\"{}\",instance_id=\"{}\",required=\"{}\"}} {}\n",
            l.as_str(),
            id,
            l.is_required(),
            beat(l).unwrap_or(0)
        ));
    }
    out.push_str(
        "# HELP noetl_worker_serving_heartbeat_age_ms Milliseconds since the loop last \
         ticked; -1 when it has never ticked.\n# TYPE noetl_worker_serving_heartbeat_age_ms gauge\n",
    );
    for l in ServingLoop::ALL {
        let age = age_ms(l).map(|v| v as i64).unwrap_or(-1);
        out.push_str(&format!(
            "noetl_worker_serving_heartbeat_age_ms{{loop=\"{}\"}} {}\n",
            l.as_str(),
            age
        ));
    }
    out.push_str(
        "# HELP noetl_worker_serving_listener_bound 1 when the loop's listener is bound.\n\
         # TYPE noetl_worker_serving_listener_bound gauge\n",
    );
    for l in ServingLoop::ALL {
        out.push_str(&format!(
            "noetl_worker_serving_listener_bound{{loop=\"{}\"}} {}\n",
            l.as_str(),
            u8::from(is_bound(l))
        ));
    }
    // ⚠ Emitted so a reader can tell "this pool does not host that face" from
    // "it should and does not" — the two states the predicate treats oppositely
    // and which a bound=0 reading alone cannot distinguish.
    out.push_str(
        "# HELP noetl_worker_serving_listener_expected 1 when this process is \
         configured to host the loop.\n\
         # TYPE noetl_worker_serving_listener_expected gauge\n",
    );
    for l in ServingLoop::ALL {
        out.push_str(&format!(
            "noetl_worker_serving_listener_expected{{loop=\"{}\"}} {}\n",
            l.as_str(),
            u8::from(is_expected(l))
        ));
    }
    out.push_str(
        "# HELP noetl_worker_serving_ready 1 when the readiness predicate holds \
         (what /readyz answers).\n# TYPE noetl_worker_serving_ready gauge\n",
    );
    out.push_str(&format!(
        "noetl_worker_serving_ready {}\n",
        u8::from(readiness().is_ok())
    ));
    out
}

fn escape_label(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', " ")
}

/// Wrap a serving future so it publishes a heartbeat while it is being polled.
///
/// Composes inside [`crate::graceful::until_stopped`]: that pins this future and
/// selects it against the stop signal, and this one pins the inner serving future
/// and selects it against a ticker. The inner future is **pinned across
/// iterations**, so a tick does not cancel the serve — a `select!` that dropped
/// and rebuilt it would restart the accept loop every tick.
///
/// ⚠ The bump is here rather than after `accept()` on purpose. An idle accept
/// loop is healthy; bumping per connection would make a quiet writer look wedged
/// and drop it out of rotation. What a bump asserts is narrower and more useful:
/// **this task was polled**. A task that blocks its thread — the tier service's
/// deaf-under-saturation shape, a handler holding the engine lock, a starved
/// runtime — stops being polled and goes stale.
///
/// Generic over the serving future's output so it composes with both shapes in
/// this crate: the faces under [`crate::graceful::until_stopped`] return
/// `io::Result<()>`, and the tier service's own accept loop returns `()`.
pub async fn beating<F>(which: ServingLoop, serve: F) -> F::Output
where
    F: std::future::Future,
{
    beating_every(which, std::time::Duration::from_millis(TICK_MS), serve).await
}

/// [`beating`] with an explicit tick interval.
///
/// Separate so the tick is injectable rather than a baked constant. Tests drive
/// it at millisecond resolution against the REAL clock — `tokio`'s paused clock
/// cannot be used here, because the heartbeat's timestamp component comes from
/// `SystemTime::now()`, which paused time does not advance. A test under
/// `start_paused` can therefore only ever observe the sequence moving, and would
/// pass against a build whose wall-clock component is frozen.
pub async fn beating_every<F>(which: ServingLoop, tick: std::time::Duration, serve: F) -> F::Output
where
    F: std::future::Future,
{
    mark_expected(which);
    mark_bound(which);
    let mut ticker = tokio::time::interval(tick);
    // Delay rather than burst on missed ticks — an EFFICIENCY choice, not a
    // correctness one, and worth saying so because the obvious reading is wrong.
    //
    // A burst cannot manufacture freshness: every bump reads the clock at bump
    // time, so N catch-up ticks in a row all stamp the same `now` and produce
    // exactly the freshness one tick would. (A mutation run removing this line
    // survived the suite, which is the correct outcome — there is no behaviour
    // here to pin. The first version of this comment claimed the burst would
    // "manufacture freshness the loop did not earn"; it would not.)
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut serve = std::pin::pin!(serve);
    loop {
        tokio::select! {
            result = &mut serve => return result,
            _ = ticker.tick() => {
                bump(which);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A snapshot where everything required is bound and just ticked.
    fn healthy(now_ms: u64) -> Snapshot {
        let packed = now_ms << SEQ_BITS;
        let mut bound = 0u64;
        let mut beats = [0u64; 5];
        for l in ServingLoop::ALL {
            bound |= 1 << l.index();
            beats[l.index()] = packed;
        }
        Snapshot {
            warm: true,
            now_ms,
            threshold_ms: DEFAULT_FRESHNESS_MS,
            beats,
            bound,
            expected: bound,
            required: ServingLoop::DEFAULT_REQUIRED.to_vec(),
        }
    }

    // ---- the positive control -------------------------------------------
    //
    // ⚠ First, and load-bearing. Every negative case below asserts a 503; if the
    // baseline were already 503 for some unrelated reason, all of them would pass
    // while testing nothing. This asserts the fixture can read READY.

    #[test]
    fn fresh_and_bound_is_ready() {
        assert_eq!(evaluate(&healthy(1_000_000)), Ok(()));
    }

    // ---- the three refusal paths ----------------------------------------

    #[test]
    fn not_warm_is_not_ready() {
        let mut s = healthy(1_000_000);
        s.warm = false;
        assert_eq!(evaluate(&s), Err(NotReady::Warming));
    }

    #[test]
    fn unbound_required_listener_is_not_ready() {
        for l in ServingLoop::DEFAULT_REQUIRED {
            let mut s = healthy(1_000_000);
            s.bound &= !(1 << l.index());
            assert_eq!(
                evaluate(&s),
                Err(NotReady::Unbound(l)),
                "unbinding {} must refuse readiness",
                l.as_str()
            );
        }
    }

    #[test]
    fn never_served_required_loop_is_not_ready() {
        for l in ServingLoop::DEFAULT_REQUIRED {
            let mut s = healthy(1_000_000);
            s.beats[l.index()] = 0;
            assert_eq!(
                evaluate(&s),
                Err(NotReady::NeverServed(l)),
                "a never-ticked {} must refuse readiness",
                l.as_str()
            );
        }
    }

    /// ⭐ The case the whole issue exists for: the listener is bound (so a
    /// `tcpSocket` probe reads Ready) and the loop stopped making progress.
    #[test]
    fn stale_required_loop_is_not_ready_though_bound() {
        for l in ServingLoop::DEFAULT_REQUIRED {
            let now = 1_000_000u64;
            let mut s = healthy(now);
            // Bound — a tcpSocket probe would still succeed here.
            assert!(s.bound & (1 << l.index()) != 0);
            s.beats[l.index()] = (now - DEFAULT_FRESHNESS_MS - 1) << SEQ_BITS;
            assert_eq!(
                evaluate(&s),
                Err(NotReady::Stale {
                    which: l,
                    age_ms: DEFAULT_FRESHNESS_MS + 1,
                    threshold_ms: DEFAULT_FRESHNESS_MS,
                }),
                "a wedged-but-bound {} must refuse readiness",
                l.as_str()
            );
        }
    }

    /// The boundary is `>`, not `>=`: a loop exactly at the threshold is fresh.
    /// Pinned because an off-by-one here silently halves or doubles how long a
    /// wedged writer stays in rotation.
    #[test]
    fn threshold_boundary_is_exclusive() {
        let now = 1_000_000u64;
        let l = ServingLoop::CommandClaim;

        let mut at = healthy(now);
        at.beats[l.index()] = (now - DEFAULT_FRESHNESS_MS) << SEQ_BITS;
        assert_eq!(evaluate(&at), Ok(()), "age == threshold must stay ready");

        let mut over = healthy(now);
        over.beats[l.index()] = (now - DEFAULT_FRESHNESS_MS - 1) << SEQ_BITS;
        assert!(over.beats != at.beats, "fixture must actually differ");
        assert!(
            matches!(evaluate(&over), Err(NotReady::Stale { .. })),
            "age == threshold + 1 must refuse"
        );
    }

    /// ⚠ A deliberate asymmetry, pinned so it cannot be "tidied" away: a stale
    /// NON-required loop must NOT take the writer out of rotation. Without this
    /// test, widening REQUIRED to all five would pass every other test here.
    #[test]
    fn stale_optional_loop_stays_ready() {
        let now = 1_000_000u64;
        let mut s = healthy(now);
        assert!(
            !ServingLoop::TierService.is_required(),
            "fixture assumes the tier service is optional"
        );
        s.beats[ServingLoop::TierService.index()] = 1;
        assert_eq!(
            evaluate(&s),
            Ok(()),
            "a stale OPTIONAL face must not refuse readiness"
        );
    }

    /// A clock that jumps backwards must not be read as freshness.
    #[test]
    fn future_dated_beat_is_not_stale() {
        let now = 1_000_000u64;
        let mut s = healthy(now);
        s.beats[ServingLoop::CommandIngest.index()] = (now + 5_000) << SEQ_BITS;
        assert_eq!(
            evaluate(&s),
            Ok(()),
            "saturating_sub must floor the age at 0"
        );
    }

    /// ⚠⚠ The regression this guards, and it is not hypothetical: a predicate
    /// that required the bus faces UNCONDITIONALLY would hold every non-writer
    /// pool permanently NotReady, because only the writer host binds them — and
    /// `/readyz` is the endpoint noetl/ai-meta#130 added for those pools'
    /// cold-start warmup. A worker hosting no buses must answer exactly what it
    /// answered before this module existed: the warmup boolean alone.
    #[test]
    fn process_hosting_no_buses_is_ready_on_warmup_alone() {
        let s = Snapshot {
            warm: true,
            now_ms: 1_000_000,
            threshold_ms: DEFAULT_FRESHNESS_MS,
            beats: [0; 5],
            bound: 0,
            expected: 0,
            required: ServingLoop::DEFAULT_REQUIRED.to_vec(),
        };
        assert_eq!(
            evaluate(&s),
            Ok(()),
            "a pool that hosts no bus face must stay ready"
        );

        let cold = Snapshot { warm: false, ..s };
        assert_eq!(
            evaluate(&cold),
            Err(NotReady::Warming),
            "and must still refuse while warming"
        );
    }

    /// Expected-but-unbound is a refusal: that is the bind window, and the
    /// `startupProbe` is what keeps it from killing the pod.
    #[test]
    fn expected_but_unbound_refuses() {
        let l = ServingLoop::CommandClaim;
        let mut s = healthy(1_000_000);
        s.bound &= !(1 << l.index());
        assert!(s.expected & (1 << l.index()) != 0, "fixture must expect it");
        assert_eq!(evaluate(&s), Err(NotReady::Unbound(l)));
    }

    // ---- the configurable required set ----------------------------------

    #[test]
    fn parse_required_defaults_when_unset_or_blank() {
        for raw in [None, Some(""), Some("   "), Some(",, ,")] {
            assert_eq!(
                parse_required(raw),
                ServingLoop::DEFAULT_REQUIRED.to_vec(),
                "{raw:?} must fall back to the default set"
            );
        }
    }

    #[test]
    fn parse_required_accepts_all_and_explicit_lists() {
        assert_eq!(parse_required(Some("all")), ServingLoop::ALL.to_vec());
        assert_eq!(parse_required(Some("ALL")), ServingLoop::ALL.to_vec());
        assert_eq!(
            parse_required(Some("command_claim, event_ingest")),
            vec![ServingLoop::CommandClaim, ServingLoop::EventIngest]
        );
        // Duplicates collapse — a repeated name must not be judged twice.
        assert_eq!(
            parse_required(Some("event_claim,event_claim")),
            vec![ServingLoop::EventClaim]
        );
    }

    /// ⚠⚠ The dangerous failure mode, pinned: a typo must NOT empty the required
    /// set. An empty set makes `evaluate` return Ok unconditionally — a readiness
    /// probe that cannot fail, which is the exact defect class this module exists
    /// to remove.
    #[test]
    fn parse_required_falls_back_rather_than_requiring_nothing() {
        for raw in ["command_clam", "event_ingest,nonsense", "9101"] {
            let got = parse_required(Some(raw));
            assert_eq!(
                got,
                ServingLoop::DEFAULT_REQUIRED.to_vec(),
                "{raw:?} must fall back, not empty the set"
            );
            assert!(!got.is_empty(), "the required set must never be empty");
        }
    }

    /// Widening must actually bite — otherwise the env var is decorative.
    #[test]
    fn widening_required_catches_a_face_the_default_ignores() {
        let now = 1_000_000u64;
        let l = ServingLoop::EventClaim;

        let mut lenient = healthy(now);
        lenient.beats[l.index()] = 1; // ancient
        assert_eq!(
            evaluate(&lenient),
            Ok(()),
            "the default set must ignore a stale event_claim (it binds at ~380 s)"
        );

        let strict = Snapshot {
            required: ServingLoop::ALL.to_vec(),
            ..lenient
        };
        assert!(
            matches!(evaluate(&strict), Err(NotReady::Stale { which, .. }) if which == l),
            "NOETL_SERVING_REQUIRED=all must refuse the same snapshot"
        );
    }

    // ---- the packed value ------------------------------------------------

    #[test]
    fn bump_is_monotonic_across_many_calls() {
        // Exercises the same-millisecond path hard: thousands of bumps inside a
        // few ms, where the timestamp cannot be what makes the value increase.
        let mut last = 0u64;
        let mut same_ms = 0usize;
        for _ in 0..5_000 {
            let v = bump(ServingLoop::TierService);
            assert!(v > last, "heartbeat went backwards: {last} -> {v}");
            if timestamp_of(v) == timestamp_of(last) {
                same_ms += 1;
            }
            last = v;
        }
        // The denominator: if every bump landed in its own millisecond, the
        // sequence path never ran and this test proved nothing about it.
        assert!(
            same_ms > 0,
            "no two bumps shared a millisecond — the sequence path was never \
             exercised (same_ms={same_ms}/5000)"
        );
    }

    #[test]
    fn pack_and_unpack_round_trip() {
        let packed = (1_700_000_000_000u64 << SEQ_BITS) | 7;
        assert_eq!(timestamp_of(packed), 1_700_000_000_000);
        assert_eq!(sequence_of(packed), 7);
    }

    // ---- identity --------------------------------------------------------

    #[test]
    fn instance_id_is_stable_within_the_process() {
        let a = instance_id();
        let b = instance_id();
        assert_eq!(a, b, "instance id must be memoized per process");
        // The shape the lease holder carries: `<host>#<uuid>`.
        let (_host, uuid) = a.rsplit_once('#').expect("instance id must be host#uuid");
        assert!(
            uuid::Uuid::parse_str(uuid).is_ok(),
            "instance id suffix must be a uuid, got {uuid:?}"
        );
    }

    // ---- exposition ------------------------------------------------------

    #[test]
    fn exposition_emits_every_loop_unconditionally() {
        let body = render_prometheus();
        for l in ServingLoop::ALL {
            let needle = format!(
                "noetl_worker_serving_heartbeat_age_ms{{loop=\"{}\"}}",
                l.as_str()
            );
            assert!(
                body.contains(&needle),
                "every loop must be emitted even when it has never ticked, \
                 missing: {needle}"
            );
        }
        // The denominator, asserted rather than assumed: all five, plus the two
        // other families and the ready gauge.
        assert_eq!(
            body.matches("noetl_worker_serving_heartbeat{").count(),
            ServingLoop::ALL.len(),
            "exposition must carry one heartbeat series per loop"
        );
        assert!(
            body.contains("instance_id=\""),
            "heartbeat must be instance-stamped"
        );
        assert!(body.contains("noetl_worker_serving_ready "));
    }

    #[test]
    fn required_is_a_strict_subset_of_all() {
        assert!(ServingLoop::DEFAULT_REQUIRED.len() < ServingLoop::ALL.len());
        for l in ServingLoop::DEFAULT_REQUIRED {
            assert!(ServingLoop::ALL.contains(&l));
            assert!(l.is_required());
        }
        // Names must be unique or two loops would share a metric series.
        let mut names: Vec<_> = ServingLoop::ALL.iter().map(|l| l.as_str()).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(names.len(), before, "loop names must be unique");
        // Indices must be unique and dense, or the bitset and array would alias.
        let mut idx: Vec<_> = ServingLoop::ALL.iter().map(|l| l.index()).collect();
        idx.sort_unstable();
        assert_eq!(idx, (0..ServingLoop::ALL.len()).collect::<Vec<_>>());
    }

    // ---- the wrapper actually ticks --------------------------------------

    /// ⚠ The integration-shaped case: prove [`beating`] bumps while the inner
    /// serving future is still pending, and that it does not cancel it.
    #[tokio::test]
    async fn beating_ticks_while_serve_is_pending_and_does_not_cancel_it() {
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;

        let l = ServingLoop::TierService;
        let polled = Arc::new(AtomicBool::new(false));
        let p = polled.clone();

        let before = beat(l).unwrap_or(0);
        let serve = async move {
            p.store(true, Ordering::Release);
            // Never completes: the shape of a real accept loop.
            std::future::pending::<()>().await
        };
        // 2 ms ticks against the real clock (see `beating_every`), so the two
        // observation windows below each span many ticks and several
        // milliseconds of wall clock.
        const T: std::time::Duration = std::time::Duration::from_millis(2);
        let task = tokio::spawn(beating_every(l, T, serve));

        tokio::time::sleep(std::time::Duration::from_millis(40)).await;

        assert!(polled.load(Ordering::Acquire), "inner serve must be polled");
        assert!(
            !task.is_finished(),
            "a tick must NOT cancel the serving future"
        );
        assert!(is_bound(l), "beating must mark the face bound");

        let first = beat(l).expect("beating must publish a heartbeat");
        assert!(
            first > before,
            "beating must advance the heartbeat while serving is pending \
             ({before} -> {first})"
        );

        // ⚠⚠ TWO observations, not one, and this is the point of the test.
        //
        // `mark_bound` bumps once on entry, so a single `after > before` check
        // passes even if the TICKER NEVER FIRES — the exact mutant that survived
        // the first battery. A wedged loop that bumped once at bind and never
        // again is the failure this module exists to detect, so the test has to
        // be able to see the difference between "ticking" and "ticked once".
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        let second = beat(l).expect("heartbeat must still be published");
        assert!(
            second > first,
            "the TICKER must keep advancing the heartbeat, not just the initial \
             mark_bound ({first} -> {second})"
        );
        assert!(
            timestamp_of(second) > timestamp_of(first),
            "successive ticks must advance the wall-clock component, not only \
             the sequence ({} -> {})",
            timestamp_of(first),
            timestamp_of(second)
        );
        task.abort();
    }
}
