//! Per-subscription rate limiting / backpressure safety valves
//! (noetl/ai-meta#90 Phase 7, RFC §9).
//!
//! One firehose subscription must not be able to starve the shared control
//! plane or a dedicated pool.  Two configurable caps, both enforced on the
//! runtime's **fetch side** so an over-limit subscription *stops fetching*
//! rather than dropping — the unfetched messages stay in the source (the
//! durable buffer built for them) and are redelivered later.  This is the
//! existing "stop fetching → source redelivers" backpressure model (RFC §9);
//! the limiter never acks-then-drops, so no message is lost.
//!
//! - `max_in_flight` — the most un-dispatched messages the runtime will hold
//!   at once.  It clamps the effective poll batch, so the runtime never pulls
//!   a deeper batch than it is allowed to have outstanding (RFC §9 "caps
//!   outstanding un-acked/un-spooled messages; runtime stops fetching at the
//!   cap").
//! - `max_dispatch_per_sec` — a token bucket over dispatch rate.  Before each
//!   poll the runtime asks how many tokens are available and fetches at most
//!   that many; when the budget is exhausted it doesn't poll at all (the
//!   source keeps the backlog).  This bounds the rate at which the
//!   subscription can hand work to the control plane.
//!
//! The token bucket takes the current `Instant` as a parameter on every
//! method so it is deterministic in tests (no wall-clock dependency).

use std::time::{Duration, Instant};

/// A simple token bucket.  Capacity == one second of tokens (a 1 s burst),
/// refilling continuously at `rate_per_sec`.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    capacity: f64,
    tokens: f64,
    rate_per_sec: f64,
    last: Instant,
}

impl TokenBucket {
    /// Build a bucket that refills `rate_per_sec` tokens per second, starting
    /// full.  `now` seeds the refill clock.
    pub fn new(rate_per_sec: u32, now: Instant) -> Self {
        let cap = rate_per_sec.max(1) as f64;
        TokenBucket {
            capacity: cap,
            tokens: cap,
            rate_per_sec: cap,
            last: now,
        }
    }

    /// Refill tokens for the elapsed time since the last touch, capped at
    /// capacity.
    fn refill(&mut self, now: Instant) {
        if now <= self.last {
            return;
        }
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate_per_sec).min(self.capacity);
        self.last = now;
    }

    /// How many whole tokens are available right now (after refilling).
    pub fn available_at(&mut self, now: Instant) -> u32 {
        self.refill(now);
        self.tokens.floor().max(0.0) as u32
    }

    /// Consume `n` tokens (saturating at zero).  Call after fetching `n`
    /// messages so the budget reflects what was actually pulled.
    pub fn consume_at(&mut self, n: u32, now: Instant) {
        self.refill(now);
        self.tokens = (self.tokens - n as f64).max(0.0);
    }

    /// How long until at least one token is available (zero if already).
    pub fn wait_for_one(&mut self, now: Instant) -> Duration {
        self.refill(now);
        if self.tokens >= 1.0 {
            return Duration::ZERO;
        }
        let deficit = 1.0 - self.tokens;
        Duration::from_secs_f64(deficit / self.rate_per_sec)
    }
}

/// The fetch-side governor combining `max_in_flight` (a static clamp) and
/// `max_dispatch_per_sec` (the token bucket).  Tracks whether the limit is
/// currently engaged so the runtime can event-log the transition once instead
/// of per message.
#[derive(Debug)]
pub struct RateGovernor {
    max_in_flight: Option<u32>,
    bucket: Option<TokenBucket>,
    /// Whether a limit was engaged on the last `plan_fetch` (so we log on the
    /// off→on edge, not every loop).
    engaged: bool,
}

/// What the governor decided for the next poll.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchPlan {
    /// Fetch up to this many messages (always ≥ 1).
    Fetch {
        batch: u32,
        /// True on the off→on edge of a limit engaging (log it once).
        newly_limited: bool,
    },
    /// No budget — don't poll this tick; sleep `wait` then re-plan.  The
    /// source retains the backlog (backpressure, no loss).  `newly_limited`
    /// flags the off→on edge.
    Throttle { wait: Duration, newly_limited: bool },
}

impl RateGovernor {
    /// Build from the optional caps.  `now` seeds the token bucket.
    pub fn new(max_in_flight: Option<u32>, max_dispatch_per_sec: Option<u32>, now: Instant) -> Self {
        RateGovernor {
            max_in_flight,
            bucket: max_dispatch_per_sec.map(|r| TokenBucket::new(r, now)),
            engaged: false,
        }
    }

    /// True when no caps are configured (the common, unlimited case).
    pub fn is_unlimited(&self) -> bool {
        self.max_in_flight.is_none() && self.bucket.is_none()
    }

    /// Decide how deep the next poll may be, given the configured `base` batch.
    ///
    /// `max_in_flight` clamps the batch; the token bucket caps it to the
    /// available budget and, when the budget is zero, returns
    /// [`FetchPlan::Throttle`] so the runtime skips the poll.
    pub fn plan_fetch(&mut self, base: u32, now: Instant) -> FetchPlan {
        let mut batch = base.max(1);
        let mut limiting = false;

        if let Some(cap) = self.max_in_flight {
            if cap < batch {
                batch = cap.max(1);
                limiting = true;
            }
        }

        if let Some(bucket) = self.bucket.as_mut() {
            let budget = bucket.available_at(now);
            if budget == 0 {
                let wait = bucket.wait_for_one(now);
                let newly_limited = !self.engaged;
                self.engaged = true;
                return FetchPlan::Throttle {
                    // Don't busy-spin: wait at least a short floor.
                    wait: wait.max(Duration::from_millis(20)),
                    newly_limited,
                };
            }
            if budget < batch {
                batch = budget;
                limiting = true;
            }
        }

        let newly_limited = limiting && !self.engaged;
        self.engaged = limiting;
        FetchPlan::Fetch {
            batch,
            newly_limited,
        }
    }

    /// Record that `n` messages were actually fetched (consumes tokens).
    pub fn record_fetched(&mut self, n: u32, now: Instant) {
        if let Some(bucket) = self.bucket.as_mut() {
            bucket.consume_at(n, now);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> Instant {
        Instant::now()
    }

    #[test]
    fn token_bucket_starts_full_and_refills() {
        let now = t0();
        let mut b = TokenBucket::new(10, now);
        assert_eq!(b.available_at(now), 10);
        b.consume_at(10, now);
        assert_eq!(b.available_at(now), 0);
        // After 0.5s, ~5 tokens back.
        let later = now + Duration::from_millis(500);
        assert_eq!(b.available_at(later), 5);
        // After a full second from empty, capped at capacity (10).
        let full = now + Duration::from_secs(5);
        assert_eq!(b.available_at(full), 10);
    }

    #[test]
    fn token_bucket_wait_for_one() {
        let now = t0();
        let mut b = TokenBucket::new(4, now); // 4/sec → 0.25s per token
        b.consume_at(4, now);
        let w = b.wait_for_one(now);
        // need 1 token at 4/sec → 250ms
        assert!(w >= Duration::from_millis(240) && w <= Duration::from_millis(260), "{w:?}");
    }

    #[test]
    fn governor_unlimited_passes_full_batch() {
        let now = t0();
        let mut g = RateGovernor::new(None, None, now);
        assert!(g.is_unlimited());
        assert_eq!(
            g.plan_fetch(100, now),
            FetchPlan::Fetch {
                batch: 100,
                newly_limited: false
            }
        );
    }

    #[test]
    fn governor_max_in_flight_clamps_batch() {
        let now = t0();
        let mut g = RateGovernor::new(Some(10), None, now);
        match g.plan_fetch(100, now) {
            FetchPlan::Fetch { batch, newly_limited } => {
                assert_eq!(batch, 10);
                assert!(newly_limited, "first clamp is the off->on edge");
            }
            other => panic!("expected Fetch, got {other:?}"),
        }
        // Second call still clamped but no longer "newly" limited.
        match g.plan_fetch(100, now) {
            FetchPlan::Fetch { batch, newly_limited } => {
                assert_eq!(batch, 10);
                assert!(!newly_limited);
            }
            other => panic!("expected Fetch, got {other:?}"),
        }
    }

    #[test]
    fn governor_rate_limit_throttles_then_recovers_no_loss() {
        let now = t0();
        // base batch == rate (5) so the first poll fetches the full batch
        // without clamping; the *throttle* is then the clean off→on edge.
        let mut g = RateGovernor::new(None, Some(5), now);
        match g.plan_fetch(5, now) {
            FetchPlan::Fetch { batch, newly_limited } => {
                assert_eq!(batch, 5);
                assert!(!newly_limited, "full batch fetched, not yet limited");
            }
            other => panic!("expected Fetch, got {other:?}"),
        }
        g.record_fetched(5, now);
        // Budget exhausted → throttle (backpressure, source keeps backlog).
        match g.plan_fetch(5, now) {
            FetchPlan::Throttle { wait, newly_limited } => {
                assert!(wait > Duration::ZERO);
                assert!(newly_limited, "throttle is the off->on edge here");
            }
            other => panic!("expected Throttle, got {other:?}"),
        }
        // Still throttled a moment later, but no longer a *new* engagement.
        match g.plan_fetch(5, now) {
            FetchPlan::Throttle { newly_limited, .. } => assert!(!newly_limited),
            other => panic!("expected Throttle, got {other:?}"),
        }
        // After 1s the bucket has refilled → fetch resumes (no loss; the
        // unfetched messages were never pulled, so they're still in source).
        let later = now + Duration::from_secs(1);
        match g.plan_fetch(5, later) {
            FetchPlan::Fetch { batch, .. } => assert_eq!(batch, 5),
            other => panic!("expected Fetch, got {other:?}"),
        }
    }

    #[test]
    fn governor_combines_in_flight_and_rate() {
        let now = t0();
        // in-flight cap 3, rate 100/sec → the tighter cap (3) wins.
        let mut g = RateGovernor::new(Some(3), Some(100), now);
        match g.plan_fetch(50, now) {
            FetchPlan::Fetch { batch, .. } => assert_eq!(batch, 3),
            other => panic!("expected Fetch, got {other:?}"),
        }
    }
}
