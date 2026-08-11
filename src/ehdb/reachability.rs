//! Durable-service reachability, measured by the operation that depends on it.
//!
//! **Fixes the arm-D defect** ([ai-meta#257](https://github.com/noetl/ai-meta/issues/257)).
//! The append path previously fed the serve policy
//! `TierClientConfig::from_env().is_some()`, which measures **configured**, not
//! **reachable** — so a black-hole address was treated as a durable service and
//! `primary` served. The policy was right; its input was a lie.
//!
//! # Option 2: the append is the probe
//!
//! Reachability is derived from real tier-service operations rather than from a
//! separate poll. That is the only measurement that cannot drift: a cached
//! probe with a TTL is a claim of authority with a timer on it, and the window
//! in which it is stale is exactly the window in which the tier lies about being
//! authoritative.
//!
//! The accepted cost is a **one-operation lag** — the first append after the
//! service dies pays a timeout before demoting. That request is *slower*, not
//! wrong: the incumbent still answers it.
//!
//! # Rejected is not unreachable
//!
//! The distinction this module exists to get right. A service that **rejects a
//! record** (malformed, over-cap, no store configured) is reachable and healthy;
//! demoting the whole tier for one bad record would let a single poisoned
//! payload disable authoritative serving platform-wide. Only transport failures
//! — connect refused, timeout, truncated frame — mean "unreachable".
//!
//! # Cached negative
//!
//! Once known-down the verdict is cached for [`DOWN_CACHE_MS`], so an outage
//! costs **one** slow request rather than one per append. After the window the
//! next append attempts again and re-promotes on success, so recovery needs no
//! operator action.
//!
//! # The cooldown gates RETRY, never BELIEF
//!
//! This is the distinction the module got wrong once, and the one it now exists
//! to hold. Reachability was stored as a *deadline* — `DOWN_UNTIL_MS` — and
//! `is_reachable()` read it as `now >= until`. So the passage of time alone
//! restored belief: five seconds after a service died, with nothing having
//! contacted it, the tier decided it was authoritative again. That is a probe
//! TTL wearing a different name, and it fails in the same direction — the
//! window in which it is stale is exactly the window in which the tier lies.
//!
//! Belief now lives in its own flag, [`VERIFIED_REACHABLE`], which **only a
//! real successful operation sets** and **any transport failure clears**. The
//! deadline keeps its one honest job: suppressing repeat *attempts* while an
//! outage is known to be in progress ([`is_cached_down`]). Window expiry
//! therefore permits the next append to be *tried*; that append must
//! **succeed** before anything is served authoritatively again.
//!
//! The two questions are deliberately answered by two different pieces of
//! state, because they are different questions:
//!
//! | question | answered by | keyed on |
//! | :-- | :-- | :-- |
//! | may I *try* the service? | [`is_cached_down`] | the clock |
//! | may I *serve from* it? | [`is_reachable`] | the last observed outcome |

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// How long a known-down verdict is cached, in milliseconds.
///
/// Short enough that recovery is quick, long enough that a sustained outage does
/// not pay a timeout per append. This is the only tunable, and unlike a probe
/// TTL it can never cause a **false positive**: while cached-down the tier
/// demotes, which is the safe direction.
pub const DOWN_CACHE_MS: u64 = 5_000;

/// Epoch-millis until which the service is considered down. `0` ⇒ not down.
static DOWN_UNTIL_MS: AtomicU64 = AtomicU64::new(0);

/// Whether any successful operation has ever been observed. Until one has, the
/// service is **not** assumed reachable — "never tried" must not read as
/// "healthy", which is the assume-good failure the arm-D defect was made of.
///
/// This is a latch: it never returns to 0 once set. It answers "has this
/// process ever reached the service", which is a different question from
/// "is it reachable now" — see [`VERIFIED_REACHABLE`].
static EVER_SUCCEEDED: AtomicU64 = AtomicU64::new(0);

/// Whether the **most recent** evidence says the service is reachable.
///
/// Set to 1 only by an operation that actually round-tripped; cleared to 0 by
/// any transport failure. Nothing else writes it — in particular **no clock
/// does**. This is the flag that makes re-promotion evidence-based: after a
/// demotion [`is_reachable`] stays false until a real op succeeds, however long
/// the cooldown has been over.
static VERIFIED_REACHABLE: AtomicU64 = AtomicU64::new(0);

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// What one tier-service operation says about reachability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpVerdict {
    /// The service answered. Reachable.
    Reached,
    /// Transport failure — connect refused, timeout, truncated frame.
    Unreachable,
    /// The service answered and refused the record. **Reachable**; this is a
    /// data problem, not a reachability one.
    Rejected,
}

impl OpVerdict {
    /// Metric-label form. Bounded set, safe as a label value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Reached => "reached",
            Self::Unreachable => "unreachable",
            Self::Rejected => "rejected",
        }
    }
}

/// Classify a tier-client result into a reachability verdict.
///
/// `Ok(body)` still needs inspection: the service answers its typed refusals
/// (`unavailable` / `invalid` / `error`) as a normal reply, so a successful
/// round-trip carrying a refusal is `Rejected`, not `Reached`. Treating every
/// `Ok` as `Reached` would be right for reachability but would hide that the
/// store is refusing writes; treating it as `Unreachable` would demote for a
/// data problem. It is neither, and the enum says so.
pub fn classify(result: &Result<String, String>) -> OpVerdict {
    match result {
        Err(_) => OpVerdict::Unreachable,
        Ok(body) => {
            let b = body.trim_start();
            if b.starts_with("unavailable") || b.starts_with("invalid") || b.starts_with("error") {
                OpVerdict::Rejected
            } else {
                OpVerdict::Reached
            }
        }
    }
}

/// Record what an operation observed.
///
/// The only writer of belief. Every branch is deliberate:
///
/// * **Reached** — evidence of reachability. Promotes, and clears the cooldown
///   so the next append is not needlessly suppressed.
/// * **Unreachable** — clears belief *and* re-arms the cooldown. Both, always:
///   arming the timer without clearing belief is the bug that let a dead
///   service keep being served from, and clearing belief without arming the
///   timer would pay a full timeout on every event of an outage.
/// * **Rejected** — the service *answered*. It is reachable; the record was
///   refused. Promotes for exactly the same reason `Reached` does, because the
///   round trip happened. Demoting here would let one poisoned payload disable
///   authoritative serving platform-wide.
pub fn record(verdict: OpVerdict) {
    match verdict {
        OpVerdict::Reached | OpVerdict::Rejected => {
            EVER_SUCCEEDED.store(1, Ordering::Relaxed);
            VERIFIED_REACHABLE.store(1, Ordering::Relaxed);
            DOWN_UNTIL_MS.store(0, Ordering::Relaxed); // self-heal
        }
        OpVerdict::Unreachable => {
            VERIFIED_REACHABLE.store(0, Ordering::Relaxed);
            DOWN_UNTIL_MS.store(now_ms().saturating_add(DOWN_CACHE_MS), Ordering::Relaxed);
        }
    }
    // Observability for the decision's *input*. Only reachable from a
    // configured tier client, so the flag-off path still renders a
    // byte-identical `/metrics` (the accumulators stay empty). Deliberately NOT
    // pinned at 0 for that reason: here, absence means "no tier op has ever
    // run", which is exactly what it should mean.
    super::metrics::record_dataplane(
        "reachability",
        verdict.as_str(),
        verdict != OpVerdict::Unreachable,
        verdict == OpVerdict::Unreachable,
        0.0,
    );
}

/// Whether a durable service is currently believed reachable.
///
/// Reads **only** the last observed outcome. No clock is consulted, and that
/// absence is the point: a cooldown lapsing means "you may try again", never
/// "it is back". After a demotion this stays false until [`record`] sees a real
/// success — see the module note on retry-versus-belief.
pub fn is_reachable() -> bool {
    EVER_SUCCEEDED.load(Ordering::Relaxed) == 1 && VERIFIED_REACHABLE.load(Ordering::Relaxed) == 1
}

/// Whether the cached-negative window is currently suppressing attempts.
///
/// This one *is* keyed on the clock, which is correct: it answers "would
/// another attempt right now just buy the same timeout again?". When it goes
/// false the next op is attempted, and that op — not this function — decides
/// whether belief returns.
pub fn is_cached_down() -> bool {
    let until = DOWN_UNTIL_MS.load(Ordering::Relaxed);
    until != 0 && now_ms() < until
}

#[cfg(test)]
mod tests {
    use super::*;

    /// These share process-global state, so they run as ONE test: `cargo test`
    /// does not serialise tests within a binary, and split tests would race each
    /// other's atomics — a lesson this crate has already paid for.
    #[test]
    fn reachability_is_measured_not_assumed() {
        // --- never contacted ⇒ NOT reachable (the arm-D fix) ---
        EVER_SUCCEEDED.store(0, Ordering::Relaxed);
        VERIFIED_REACHABLE.store(0, Ordering::Relaxed);
        DOWN_UNTIL_MS.store(0, Ordering::Relaxed);
        assert!(
            !is_reachable(),
            "configured-but-never-contacted must NOT count as reachable"
        );

        // --- a success promotes ---
        record(OpVerdict::Reached);
        assert!(is_reachable());

        // --- a transport failure demotes, and caches the negative ---
        record(OpVerdict::Unreachable);
        assert!(!is_reachable(), "an unreachable op must demote");
        assert!(is_cached_down(), "the negative must be cached to skip repeat timeouts");

        // --- a success self-heals immediately ---
        record(OpVerdict::Reached);
        assert!(is_reachable(), "recovery needs no operator action");
        assert!(!is_cached_down());

        // --- REJECTION MUST NOT DEMOTE ---
        // One poisoned payload must not disable authoritative serving platform-wide.
        record(OpVerdict::Rejected);
        assert!(
            is_reachable(),
            "a rejected record is a DATA problem; the service is reachable"
        );
        assert!(!is_cached_down());

        lapsed_cooldown_permits_a_retry_but_never_restores_belief();
    }

    /// The guard for the re-promotion defect: **a lapsed cooldown must not
    /// restore belief.**
    ///
    /// Called from [`reachability_is_measured_not_assumed`] rather than carrying
    /// its own `#[test]`, for the reason stated there: these atomics are
    /// process-global and `cargo test` does not serialise tests within a binary.
    /// As a separate test this raced the other one and failed on its very first
    /// assertion — the same class of bug the module is about, arriving via the
    /// test harness.
    ///
    /// Two mutations must both break this test, and were both checked by hand
    /// against a rebuilt image as well as here:
    ///
    /// 1. `is_reachable()` consulting the clock again (`until == 0 || now >=
    ///    until`) — the original bug. Caught by the first assertion below.
    /// 2. `record(Unreachable)` leaving `VERIFIED_REACHABLE` set — a failed op
    ///    that still promotes. Caught by the demotion assertion.
    ///
    /// The lapse is simulated by rewinding the deadline rather than sleeping
    /// [`DOWN_CACHE_MS`], so the test stays fast and, more importantly,
    /// deterministic — a sleep-based version would pass on a slow machine for
    /// the wrong reason.
    fn lapsed_cooldown_permits_a_retry_but_never_restores_belief() {
        EVER_SUCCEEDED.store(0, Ordering::Relaxed);
        VERIFIED_REACHABLE.store(0, Ordering::Relaxed);
        DOWN_UNTIL_MS.store(0, Ordering::Relaxed);

        // Establish belief the only way it can be established.
        record(OpVerdict::Reached);
        assert!(is_reachable());

        // The service dies.
        record(OpVerdict::Unreachable);
        assert!(!is_reachable(), "a transport failure must demote");
        assert!(is_cached_down(), "and must arm the cooldown");

        // The cooldown lapses with NOTHING having contacted the service.
        DOWN_UNTIL_MS.store(now_ms().saturating_sub(1), Ordering::Relaxed);
        assert!(
            !is_cached_down(),
            "the window is over, so a retry is now permitted"
        );
        assert!(
            !is_reachable(),
            "THE FIX: time passing is not evidence.  The retry is permitted; \
             belief is not restored until that retry SUCCEEDS"
        );

        // EVER_SUCCEEDED is a latch and must not be what gates this — if it
        // were, the demotion above would have been invisible.
        assert_eq!(
            EVER_SUCCEEDED.load(Ordering::Relaxed),
            1,
            "the latch remembers that we once reached it, and that is not the \
             same claim as 'it is reachable now'"
        );

        // Only the successful retry re-promotes.
        record(OpVerdict::Reached);
        assert!(is_reachable(), "a real success re-promotes, with no operator action");
    }

    #[test]
    fn classify_separates_transport_failure_from_refusal() {
        assert_eq!(
            classify(&Err("connect: Connection refused".to_string())),
            OpVerdict::Unreachable
        );
        assert_eq!(
            classify(&Err("timed out after 2s".to_string())),
            OpVerdict::Unreachable
        );
        // The service ANSWERED — these are refusals, not transport failures.
        assert_eq!(
            classify(&Ok("unavailable no tier store configured".to_string())),
            OpVerdict::Rejected
        );
        assert_eq!(
            classify(&Ok("invalid payload is empty".to_string())),
            OpVerdict::Rejected
        );
        assert_eq!(
            classify(&Ok("error disk full".to_string())),
            OpVerdict::Rejected
        );
        // A real body is a real success.
        assert_eq!(
            classify(&Ok(r#"{"appended":true,"global_sequence":7}"#.to_string())),
            OpVerdict::Reached
        );
    }
}
