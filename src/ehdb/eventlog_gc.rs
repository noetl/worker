//! Periodic **segment-GC** for the durable event-log backend
//! ([noetl/ehdb#254]) — the operational piece that turns the reclamation
//! mechanism into a running, self-bounding store in a real cluster.
//!
//! The durable segment store grows without bound unless consumed sealed segments
//! are reclaimed (the runbook §C residual-risk R1 / D11 gap). The engine ships
//! the reclamation ([`SharedTierEventLog::reclaim_shard`], off by default); this
//! module invokes it **periodically** so an operator who opts in gets a
//! self-bounding store with no manual step.
//!
//! ## Who triggers, and back-pressure
//!
//! Each worker replica runs this task for the shards **it owns** — only a
//! shard's single writer may reclaim it (the execution-affinity invariant), so
//! GC is inherently per-owner and needs no central coordinator. A reclaim pass
//! acquires the per-shard advisory lock
//! ([`crate::ehdb::eventlog_backend::reclaim_owned_shards`]) so it never
//! interleaves with a durable append on the same shard, and runs on a
//! [`tokio::task::spawn_blocking`] thread so its `fsync`/unlink I/O never stalls
//! the async runtime. A pass on one shard never blocks appends on another.
//!
//! ## Fail-safe, off by default
//!
//! [`GcConfig::from_env`] returns `None` unless **all** of these hold, so the
//! task never even spawns until an operator opts in on every axis:
//! - `NOETL_EHDB_EVENTLOG_BACKEND=durable_segment` (the durable backend),
//! - `NOETL_EHDB_EVENTLOG_GC=consumer_ack` (the reclaim policy enabled),
//! - `NOETL_EHDB_EVENTLOG_GC_INTERVAL_SECS>0` (a cadence),
//! - a resolvable data-plane EHDB contract with a durable store.
//!
//! [noetl/ehdb#254]: https://github.com/noetl/ehdb/issues/254

use std::time::{Duration, Instant};

use ehdb_reference::{EventLogStorageBackend, SegmentGcPolicy};
use tokio::task::JoinHandle;

use super::contract::{contract_from_env, EhdbContract};
use super::{eventlog_backend, metrics, process_env, EnvMap};

/// The cadence env var: the periodic reclaim interval in whole seconds. `0` (or
/// unset / unparsable) disables the periodic task — the fail-safe default.
pub const GC_INTERVAL_ENV: &str = "NOETL_EHDB_EVENTLOG_GC_INTERVAL_SECS";

/// The fully-resolved config for the periodic GC task. Constructed only when the
/// operator has opted in on every axis (see the module docs).
#[derive(Debug, Clone)]
pub struct GcConfig {
    interval: Duration,
    policy: SegmentGcPolicy,
    env: EnvMap,
    contract: EhdbContract,
}

impl GcConfig {
    /// Resolve the periodic-GC config from the process environment, or `None`
    /// when GC is not fully opted-in (backend, policy, cadence, and a data-plane
    /// durable contract all required). `None` ⇒ the task never spawns.
    pub fn from_env() -> Option<Self> {
        Self::from_env_map(process_env())
    }

    fn from_env_map(env: EnvMap) -> Option<Self> {
        // 1. The durable segment backend must be selected.
        if eventlog_backend::selected_backend(&env) != EventLogStorageBackend::DurableSegment {
            return None;
        }
        // 2. The reclaim policy must be enabled (fail-safe parse). The optional
        //    limits-based retention knob (MAX_RETAINED) lets a store with no
        //    durable consumer — e.g. a shadow mirror — self-bound; unset ⇒
        //    interest-only (the pre-retention behaviour).
        let policy = SegmentGcPolicy::from_raw(
            env.get(SegmentGcPolicy::ENV_VAR).map(|s| s.as_str()),
            env.get(SegmentGcPolicy::MIN_RETAINED_ENV_VAR)
                .map(|s| s.as_str()),
            env.get(SegmentGcPolicy::MAX_RETAINED_ENV_VAR)
                .map(|s| s.as_str()),
        );
        if !policy.enabled {
            return None;
        }
        // 3. A positive cadence.
        let secs = env
            .get(GC_INTERVAL_ENV)
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);
        if secs == 0 {
            return None;
        }
        // 4. A resolvable data-plane contract with a durable store (the durable
        //    paths are derived from the local_reference log path). A control-plane
        //    role never writes the event log, so it never GCs it.
        let contract = contract_from_env(&env).ok()?;
        if !contract.role.is_data_plane()
            || !contract.uses_local_reference_runtime()
            || contract.local_reference_log.is_none()
        {
            return None;
        }
        Some(GcConfig {
            interval: Duration::from_secs(secs.max(1)),
            policy,
            env,
            contract,
        })
    }

    /// The resolved reclaim interval.
    pub fn interval(&self) -> Duration {
        self.interval
    }
}

/// Spawn the periodic segment-GC task. Ticks every [`GcConfig::interval`]; each
/// tick reclaims every owned shard on a blocking thread and records the outcome
/// to the EHDB metric family `noetl_ehdb_eventlog_gc_*`. Returns the join handle
/// so the caller can `abort()` it on shutdown. The task is best-effort: a
/// per-shard error is recorded + logged, never fatal.
pub fn spawn(cfg: GcConfig, worker_id: String) -> JoinHandle<()> {
    tokio::spawn(async move {
        tracing::info!(
            worker_id,
            interval_secs = cfg.interval.as_secs(),
            min_retained = cfg.policy.min_retained_segments,
            "EHDB durable event-log segment-GC task started"
        );
        let mut ticker = tokio::time::interval(cfg.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // interval() fires immediately on the first tick; skip it so we do not GC
        // the instant the pod boots (let a first drive accumulate).
        ticker.tick().await;
        loop {
            ticker.tick().await;
            run_once(&cfg, &worker_id).await;
        }
    })
}

/// Run one reclaim pass across owned shards (a blocking op off the async
/// runtime), classify + record the outcome. Public for the `ehdb-selfcheck` GC
/// verb, which drives a single pass for kind validation.
pub async fn run_once(cfg: &GcConfig, worker_id: &str) {
    let env = cfg.env.clone();
    let contract = cfg.contract.clone();
    let policy = cfg.policy;
    let start = Instant::now();
    let joined = tokio::task::spawn_blocking(move || {
        eventlog_backend::reclaim_owned_shards(&env, &contract, &policy)
    })
    .await;
    let duration = start.elapsed().as_secs_f64();
    match joined {
        Ok(results) => {
            let mut segments = 0usize;
            let mut objects = 0usize;
            let mut errored = false;
            for r in &results {
                match r {
                    Ok(o) => {
                        segments += o.local_segments_reclaimed;
                        objects += o.shared_objects_deleted;
                    }
                    Err(detail) => {
                        errored = true;
                        tracing::warn!(worker_id, ehdb_detail = %detail, "EHDB segment-GC pass shard error");
                    }
                }
            }
            let reclaimed = segments > 0 || objects > 0;
            let outcome = if errored {
                "error"
            } else if reclaimed {
                "reclaimed"
            } else {
                "noop"
            };
            metrics::record_eventlog_gc(outcome, !errored, errored, duration);
            if reclaimed {
                // Low frequency (interval-driven) + only on a real reclamation —
                // safe to log at INFO per logging.md.
                tracing::info!(
                    worker_id,
                    segments_reclaimed = segments,
                    shared_objects_deleted = objects,
                    duration_seconds = duration,
                    "EHDB segment-GC reclaimed durable segments"
                );
            }
        }
        Err(join_err) => {
            metrics::record_eventlog_gc("error", false, true, duration);
            tracing::warn!(worker_id, ehdb_detail = %join_err, "EHDB segment-GC task join error");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_env(pairs: &[(&str, &str)]) -> EnvMap {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn from_env_disabled_by_default() {
        // Nothing set → no task.
        assert!(GcConfig::from_env_map(base_env(&[])).is_none());
        // Backend durable but policy off → no task.
        assert!(GcConfig::from_env_map(base_env(&[(
            "NOETL_EHDB_EVENTLOG_BACKEND",
            "durable_segment"
        )]))
        .is_none());
        // Policy on but no cadence → no task.
        assert!(GcConfig::from_env_map(base_env(&[
            ("NOETL_EHDB_EVENTLOG_BACKEND", "durable_segment"),
            ("NOETL_EHDB_EVENTLOG_GC", "consumer_ack"),
        ]))
        .is_none());
        // Cadence but backend is the default local_reference → no task.
        assert!(GcConfig::from_env_map(base_env(&[
            ("NOETL_EHDB_EVENTLOG_GC", "consumer_ack"),
            ("NOETL_EHDB_EVENTLOG_GC_INTERVAL_SECS", "60"),
        ]))
        .is_none());
    }
}
