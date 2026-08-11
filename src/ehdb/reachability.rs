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

/// Whether any successful operation has been observed. Until one has, the
/// service is **not** assumed reachable — "never tried" must not read as
/// "healthy", which is the assume-good failure the arm-D defect was made of.
static EVER_SUCCEEDED: AtomicU64 = AtomicU64::new(0);

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
pub fn record(verdict: OpVerdict) {
    match verdict {
        OpVerdict::Reached => {
            EVER_SUCCEEDED.store(1, Ordering::Relaxed);
            DOWN_UNTIL_MS.store(0, Ordering::Relaxed); // self-heal
        }
        OpVerdict::Unreachable => {
            DOWN_UNTIL_MS.store(now_ms().saturating_add(DOWN_CACHE_MS), Ordering::Relaxed);
        }
        // Reachable — deliberately does NOT change the verdict.
        OpVerdict::Rejected => {
            EVER_SUCCEEDED.store(1, Ordering::Relaxed);
        }
    }
}

/// Whether a durable service is currently believed reachable.
///
/// Requires a prior success: "configured but never contacted" is **not**
/// reachable. That is the arm-D fix in one line.
pub fn is_reachable() -> bool {
    if EVER_SUCCEEDED.load(Ordering::Relaxed) == 0 {
        return false;
    }
    let until = DOWN_UNTIL_MS.load(Ordering::Relaxed);
    until == 0 || now_ms() >= until
}

/// Whether the cached-negative window is currently suppressing attempts.
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
