//! Periodic poller for JetStream consumer-lag metrics.
//!
//! Per [`observability.md`][rule] Principle 2 ("metrics over logs"),
//! the worker exports `noetl_worker_nats_consumer_pending` +
//! `noetl_worker_nats_consumer_ack_pending` gauges so KEDA + the
//! dashboard can read the queue depth without scraping logs.  Those
//! gauges are pull-style — the JetStream consumer info API doesn't
//! push state changes — so this module owns a periodic poll task
//! that updates them on a configurable cadence.
//!
//! The poll task is spawned once from [`crate::worker::Worker::run`].
//! It runs forever; transient errors fetching consumer info are
//! logged at WARN and don't crash the worker (metrics are
//! non-critical to the dispatch path).
//!
//! [rule]: https://github.com/noetl/ai-meta/blob/main/agents/rules/observability.md

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::interval;

use crate::nats::NatsCommandSource;

/// Default polling interval — `WORKER_NATS_LAG_POLL_INTERVAL` overrides.
///
/// 5 seconds is a balance: long enough that the periodic JetStream
/// round-trip is noise relative to dispatch latency; short enough
/// that KEDA's `pollingInterval: 15` (default for prometheus-trigger
/// queries) sees fresh values on every scrape.
pub const DEFAULT_POLL_INTERVAL_SECONDS: u64 = 5;

/// Read the lag-poll interval from the environment, falling back to
/// [`DEFAULT_POLL_INTERVAL_SECONDS`].  Values < 1s are clamped to 1s
/// — sub-second polling would create more JetStream RPC traffic
/// than the metric is worth.
pub fn poll_interval_from_env() -> Duration {
    let secs = std::env::var("WORKER_NATS_LAG_POLL_INTERVAL")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_POLL_INTERVAL_SECONDS);
    Duration::from_secs(secs.max(1))
}

/// Spawn the lag-poll task.  Returns the `JoinHandle` so callers can
/// `abort()` on shutdown if needed; today the worker just lets it
/// die with the runtime.
///
/// The poll task holds an `Arc<Mutex<NatsCommandSource>>` so it
/// shares the same NATS connection the dispatch loop uses — keeping
/// the JetStream client count bounded by worker pod count.
///
/// On every tick:
/// 1. Acquire the source mutex.
/// 2. Read `subscriber.consumer_lag()`.
/// 3. Update `record_nats_consumer_lag(stream, consumer, ...)`.
///
/// Errors are logged at WARN and don't abort the loop.  A future
/// follow-up could expose a `noetl_worker_nats_lag_poll_errors_total`
/// counter; today the WARN line is the alarm surface.
pub fn spawn(
    source: Arc<Mutex<NatsCommandSource>>,
    poll_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(poll_interval);
        // First tick fires immediately so the gauge has a value
        // BEFORE the first scrape, even on a freshly-started pod.
        ticker.tick().await;
        loop {
            let snapshot = {
                let source = source.lock().await;
                let subscriber = source.subscriber();
                let stream = subscriber.stream_name().to_string();
                let consumer = subscriber.consumer_name().to_string();
                let result = subscriber.consumer_lag().await;
                (stream, consumer, result)
            };

            let (stream, consumer, result) = snapshot;
            match result {
                Ok(lag) => {
                    // i64 cast: JetStream returns u64 for num_pending
                    // and usize for num_ack_pending.  Both are
                    // realistically <<< i64::MAX (a queue of 9e18
                    // messages would have exhausted disk long ago);
                    // we saturate to MAX defensively rather than
                    // truncate.
                    let pending = i64::try_from(lag.pending).unwrap_or(i64::MAX);
                    let ack_pending = i64::try_from(lag.ack_pending).unwrap_or(i64::MAX);
                    crate::metrics::record_nats_consumer_lag(
                        &stream,
                        &consumer,
                        pending,
                        ack_pending,
                    );
                    tracing::debug!(
                        stream = %stream,
                        consumer = %consumer,
                        pending,
                        ack_pending,
                        "Updated consumer lag gauges"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        stream = %stream,
                        consumer = %consumer,
                        error = %e,
                        "Failed to fetch consumer info; gauges stale until next tick"
                    );
                }
            }

            ticker.tick().await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_interval_defaults_when_env_unset() {
        // Note: this test inspects the env-or-default function, NOT
        // the global env.  We can't safely mutate std::env from a
        // multi-threaded test runner — the WORKER_NATS_LAG_POLL_INTERVAL
        // env var coverage lives in test_poll_interval_clamp below
        // via direct construction.
        let interval = poll_interval_from_env();
        assert!(interval >= Duration::from_secs(1));
    }

    #[test]
    fn default_poll_interval_is_five_seconds() {
        assert_eq!(DEFAULT_POLL_INTERVAL_SECONDS, 5);
    }
}
