//! Platform-automatic sink — **observe-only first slice** (noetl/ai-meta#199
//! Slice C, write-behind-cache boundary, RFC `docs/rfc/ehdb-layered-platform.md`
//! §0.3).
//!
//! The explicit-connector sink (Slice A) requires a playbook author to write a
//! `sink: true` step. The platform should *also* be able to auto-sink bounded
//! transient business context that no explicit step claimed, so cache pressure
//! never forces a choice between dropping un-sunk context and unbounded growth.
//!
//! ## What this first slice does — and deliberately does NOT do
//!
//! This slice ships the **loop + config + the pure eligibility policy + candidate
//! metrics**, and **writes nothing to any store** (the platform's
//! shadow-before-cutover pattern). It selects the executions a future write slice
//! *would* sink and records them; the connector write + `confirm_sunk` is the
//! next slice, gated on an explicitly configured target
//! (`NOETL_AUTOSINK_TARGET`). Two independent guards keep it safe:
//!
//! 1. **Default off.** [`AutoSinkConfig::from_env`] returns `None` unless
//!    `NOETL_AUTOSINK` is truthy and `NOETL_AUTOSINK_INTERVAL_SECS > 0`, so the
//!    task never spawns on a default worker.
//! 2. **No target ⇒ no write, ever.** Even enabled, the loop is observe-only in
//!    this slice; and by contract the write slice will refuse to write without an
//!    explicitly configured customer-store target — the platform never picks a
//!    destination on the author's behalf.
//!
//! ## Double-write avoidance
//!
//! An execution an explicit sink step already handles (Slice A marks it in the
//! [`SharedWalIndex`] sink gate) is **skipped** — the explicit step owns its
//! sink. The write slice will additionally address the customer store by a
//! deterministic `execution=<eid>/…` key so a later explicit sink is an
//! idempotent overwrite, never a duplicate.

use std::time::Duration;

use tokio::task::JoinHandle;

use crate::state_builder::SharedWalIndex;

/// Master switch. Truthy ⇒ the auto-sink task is eligible to spawn (still needs a
/// positive interval). Default off.
pub const ENABLED_ENV: &str = "NOETL_AUTOSINK";
/// Observe cadence in whole seconds. `0` / unset / unparsable ⇒ disabled.
pub const INTERVAL_ENV: &str = "NOETL_AUTOSINK_INTERVAL_SECS";
/// Minimum resident cache footprint (bytes) for an execution to be a candidate.
pub const MIN_BYTES_ENV: &str = "NOETL_AUTOSINK_MIN_BYTES";
/// The customer-store connector target (a keychain alias / connector config).
/// **Required for the future write slice to write; absent ⇒ observe-only.** The
/// first slice never writes regardless.
pub const TARGET_ENV: &str = "NOETL_AUTOSINK_TARGET";

/// Default candidate threshold: 64 KiB of resident cache footprint.
const DEFAULT_MIN_BYTES: usize = 64 * 1024;

/// Fully-resolved auto-sink config. Constructed only when the operator has opted
/// in (`NOETL_AUTOSINK` truthy + a positive interval).
#[derive(Debug, Clone)]
pub struct AutoSinkConfig {
    interval: Duration,
    min_bytes: usize,
    /// The configured customer-store target, if any. `None` ⇒ observe-only (and
    /// the first slice is observe-only regardless).
    target: Option<String>,
}

impl AutoSinkConfig {
    /// Resolve from the process environment, or `None` when auto-sink is not
    /// opted-in. `None` ⇒ the task never spawns (default-worker byte-identical).
    pub fn from_env() -> Option<Self> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// Env-resolution seam for tests.
    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Option<Self> {
        if !truthy(get(ENABLED_ENV).as_deref()) {
            return None;
        }
        let secs = get(INTERVAL_ENV)
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);
        if secs == 0 {
            return None;
        }
        let min_bytes = get(MIN_BYTES_ENV)
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(DEFAULT_MIN_BYTES);
        let target = get(TARGET_ENV).filter(|s| !s.trim().is_empty());
        Some(Self {
            interval: Duration::from_secs(secs.max(1)),
            min_bytes,
            target,
        })
    }

    /// The resolved observe cadence.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Whether a customer-store target is configured. The first slice never
    /// writes; the future write slice writes only when this is true.
    pub fn has_target(&self) -> bool {
        self.target.is_some()
    }
}

/// The pure eligibility policy (noetl/ai-meta#199 Slice C). An execution is an
/// auto-sink **candidate** iff its resident cache footprint is at or above the
/// threshold AND no explicit sink step is already handling it (`sink_blocked`).
/// Pure + total so the selection is unit-testable in isolation from the loop.
pub fn is_candidate(resident_bytes: usize, sink_blocked: bool, min_bytes: usize) -> bool {
    resident_bytes >= min_bytes && !sink_blocked
}

/// One observe pass's accounting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObservePass {
    /// Executions the write slice would auto-sink.
    pub candidates: usize,
    /// Executions skipped because an explicit sink step already owns them.
    pub skipped_explicit: usize,
}

/// Spawn the observe-only auto-sink task. Ticks every [`AutoSinkConfig::interval`];
/// each tick classifies the resident set and records candidate metrics — it
/// **writes nothing**. Returns the join handle so the caller can `abort()` it.
pub fn spawn(cfg: AutoSinkConfig, worker_id: String, index: SharedWalIndex) -> JoinHandle<()> {
    tokio::spawn(async move {
        tracing::info!(
            worker_id,
            interval_secs = cfg.interval.as_secs(),
            min_bytes = cfg.min_bytes,
            has_target = cfg.has_target(),
            "platform-automatic sink task started (observe-only first slice; writes nothing)"
        );
        let mut ticker = tokio::time::interval(cfg.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip the immediate first tick so a fresh pod doesn't observe an empty
        // index the instant it boots.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            run_once(&cfg, &worker_id, &index).await;
        }
    })
}

/// Run one observe pass: snapshot the resident set, classify each execution by
/// the pure policy, record metrics. **Never writes.** Returns the pass accounting
/// (also for the unit test / a future selfcheck verb).
pub async fn run_once(
    cfg: &AutoSinkConfig,
    worker_id: &str,
    index: &SharedWalIndex,
) -> ObservePass {
    let snapshot = index.resident_snapshot().await;
    let mut pass = ObservePass::default();
    for (_eid, bytes, sink_blocked) in &snapshot {
        if is_candidate(*bytes, *sink_blocked, cfg.min_bytes) {
            pass.candidates += 1;
            crate::metrics::record_autosink("candidate");
        } else if *sink_blocked && *bytes >= cfg.min_bytes {
            // Over threshold but an explicit sink step owns it — double-write
            // avoidance.
            pass.skipped_explicit += 1;
            crate::metrics::record_autosink("skipped_explicit");
        }
    }
    crate::metrics::record_autosink("observed_only");
    if pass.candidates > 0 {
        tracing::debug!(
            worker_id,
            candidates = pass.candidates,
            skipped_explicit = pass.skipped_explicit,
            "auto-sink observe pass: candidate executions identified (observe-only, no write)"
        );
    }
    pass
}

/// Truthy env parse: `1` / `true` / `yes` / `on` (case-insensitive).
fn truthy(v: Option<&str>) -> bool {
    matches!(
        v.unwrap_or_default().trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_builder::WalEventIndex;

    #[test]
    fn disabled_by_default() {
        // Nothing set → no task.
        assert!(AutoSinkConfig::from_lookup(|_| None).is_none());
        // Enabled but no interval → no task.
        let only_enabled = |k: &str| (k == ENABLED_ENV).then(|| "true".to_string());
        assert!(AutoSinkConfig::from_lookup(only_enabled).is_none());
        // Interval but not enabled → no task.
        let only_interval = |k: &str| (k == INTERVAL_ENV).then(|| "60".to_string());
        assert!(AutoSinkConfig::from_lookup(only_interval).is_none());
    }

    #[test]
    fn enabled_resolves_config_and_defaults() {
        let env = |k: &str| match k {
            ENABLED_ENV => Some("on".to_string()),
            INTERVAL_ENV => Some("30".to_string()),
            _ => None,
        };
        let cfg = AutoSinkConfig::from_lookup(env).expect("opted in");
        assert_eq!(cfg.interval(), Duration::from_secs(30));
        assert_eq!(cfg.min_bytes, DEFAULT_MIN_BYTES);
        assert!(!cfg.has_target(), "no target ⇒ observe-only");

        // A configured target is surfaced (still observe-only in this slice).
        let with_target = |k: &str| match k {
            ENABLED_ENV => Some("1".to_string()),
            INTERVAL_ENV => Some("30".to_string()),
            MIN_BYTES_ENV => Some("1024".to_string()),
            TARGET_ENV => Some("customer_pg".to_string()),
            _ => None,
        };
        let cfg = AutoSinkConfig::from_lookup(with_target).expect("opted in");
        assert_eq!(cfg.min_bytes, 1024);
        assert!(cfg.has_target());
        // A blank target is treated as unset.
        let blank = |k: &str| match k {
            ENABLED_ENV => Some("yes".to_string()),
            INTERVAL_ENV => Some("30".to_string()),
            TARGET_ENV => Some("   ".to_string()),
            _ => None,
        };
        assert!(!AutoSinkConfig::from_lookup(blank).unwrap().has_target());
    }

    #[test]
    fn candidate_policy_is_threshold_and_not_explicit() {
        // Over threshold + not explicitly handled → candidate.
        assert!(is_candidate(2048, false, 1024));
        // At the threshold boundary → candidate (>=).
        assert!(is_candidate(1024, false, 1024));
        // Below threshold → not a candidate.
        assert!(!is_candidate(512, false, 1024));
        // Over threshold but an explicit sink step owns it → NOT a candidate
        // (double-write avoidance).
        assert!(!is_candidate(2048, true, 1024));
    }

    #[tokio::test]
    async fn observe_pass_classifies_without_writing() {
        // Two large executions: one explicitly-sunk (skipped), one a candidate;
        // plus a small one below threshold (neither).
        let index = SharedWalIndex::new(WalEventIndex::new());
        {
            let mut idx = index.lock().await;
            idx.enable_sink_gate_for_test();
            // Big enough chains: apply several events so bytes clear a tiny
            // threshold. (Payload bytes are the chain's cache footprint.)
            for eid in [100_i64, 200, 300] {
                for ev in 1..=3 {
                    idx.apply(&serde_json::json!({
                        "event_id": eid * 10 + ev,
                        "execution_id": eid,
                        "event_type": "playbook_started",
                        "context": { "pad": "x".repeat(64) },
                    }));
                }
            }
            // 200 is claimed by an explicit sink step.
            idx.mark_pending_sink(200);
        }
        let cfg = AutoSinkConfig {
            interval: Duration::from_secs(1),
            min_bytes: 1, // every resident chain clears it
            target: None, // observe-only
        };
        let pass = run_once(&cfg, "test-worker", &index).await;
        assert_eq!(pass.candidates, 2, "100 and 300 are candidates");
        assert_eq!(
            pass.skipped_explicit, 1,
            "200 is skipped (explicit sink owns it)"
        );
        // The index is untouched — observe-only writes nothing anywhere.
        assert_eq!(
            index.resident_snapshot().await.len(),
            3,
            "no execution dropped"
        );
    }
}
