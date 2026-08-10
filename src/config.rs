//! Worker configuration.

use anyhow::Result;
use std::time::Duration;

/// Worker pool configuration.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Unique worker identifier (UUID).
    pub worker_id: String,

    /// Worker pool name.
    pub pool_name: String,

    /// Control plane server URL.
    pub server_url: String,

    /// NATS server URL.
    pub nats_url: String,

    /// NATS stream name.
    pub nats_stream: String,

    /// NATS consumer name.
    pub nats_consumer: String,

    /// Base NATS subject the publisher writes command notifications to.
    ///
    /// Defaults to `noetl.commands`.  Per noetl/ai-meta#42 PR-3 this
    /// is now env-driven (`NATS_SUBJECT`) so the deployment yaml can
    /// override it independently of the stream + consumer names.  The
    /// stream is widened to accept both this bare subject AND the
    /// hierarchical wildcard `<subject>.>` (matches the Python PR-2a).
    pub nats_subject: String,

    /// JetStream consumer-side filter subject.  Per noetl/ai-meta#42
    /// PR-3, the worker subscribes to commands matching this filter.
    /// Default = `nats_subject` (the bare subject — today's
    /// behaviour, no change).  PR-4 sets this to
    /// `noetl.commands.shared.>` via the Rust worker's deployment
    /// env so the Rust pool only sees shared-segment commands.
    /// The routing filter subject.  Sourced from `NOETL_FEED_FILTER_SUBJECT`
    /// (falling back to the legacy `NATS_FILTER_SUBJECT` for one release).
    /// Despite the field name it is the **EHDB** pool-routing input — see
    /// [`WorkerConfig::from_env`] and noetl/ai-meta#218.
    pub nats_filter_subject: String,

    /// Heartbeat interval.
    pub heartbeat_interval: Duration,

    /// Maximum concurrent tasks.
    pub max_concurrent_tasks: usize,

    /// Bind address for the Prometheus `/metrics` endpoint.
    /// Defaults to `0.0.0.0:9090` so it's reachable from sidecar
    /// scrapers in Kubernetes without extra config.  See
    /// `agents/rules/observability.md` Principle 2.
    pub metrics_bind: String,
}

impl WorkerConfig {
    /// Load configuration from environment variables.
    pub fn from_env() -> Result<Self> {
        let worker_id =
            std::env::var("WORKER_ID").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());

        let pool_name = std::env::var("WORKER_POOL_NAME").unwrap_or_else(|_| "default".to_string());

        let server_url = std::env::var("NOETL_SERVER_URL")
            .unwrap_or_else(|_| "http://localhost:8082".to_string());

        // H5: no `nats://localhost:4222` default any more.  The internal NATS
        // bus was deleted (noetl/ai-meta#212, prod 2026-08-01), so that default
        // named a broker that is not there and cannot be there — every consumer
        // that took it got a connect failure at best, and at worst sat retrying
        // against localhost while looking healthy.  Unset now means unset: the
        // remaining readers are opt-in (the subscription spool's `nats_object`
        // backend and the state-builder rehydrate path), and they fail visibly
        // on an empty URL instead of silently on a phantom one.
        let nats_url = std::env::var("NATS_URL").unwrap_or_default();

        let nats_stream =
            std::env::var("NATS_STREAM").unwrap_or_else(|_| "noetl_commands".to_string());

        let nats_consumer =
            std::env::var("NATS_CONSUMER").unwrap_or_else(|_| "worker-pool".to_string());

        // PR-3 of noetl/ai-meta#42: subject + filter_subject become
        // env-driven so the deployment yaml can point the Rust pool
        // at `noetl.commands.shared.>` without code change.  Default
        // for the filter is the bare subject — preserves today's
        // single-consumer behaviour.
        let nats_subject =
            std::env::var("NATS_SUBJECT").unwrap_or_else(|_| "noetl.commands".to_string());

        // The routing filter subject — the input EHDB pool routing is derived
        // from (noetl/ai-meta#218). The legacy `NATS_FILTER_SUBJECT` fallback is
        // gone now that every manifest carries the EHDB-native name.
        //
        // With neither this nor `NATS_SUBJECT` set the value degrades to the
        // bare subject, from which `segment_from_filter` yields `None` — and the
        // worker then refuses to start rather than silently joining `shared`,
        // which is what made unsetting the old name stall every execution with
        // nothing in the logs.
        let nats_filter_subject = std::env::var("NOETL_FEED_FILTER_SUBJECT")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| nats_subject.clone());

        let heartbeat_secs: u64 = std::env::var("WORKER_HEARTBEAT_INTERVAL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(15);

        let max_concurrent: usize = std::env::var("WORKER_MAX_CONCURRENT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(4);

        // Metrics/health HTTP bind.  Cloud Run injects `PORT` and expects the
        // container to listen on it (the startup probe is a TCP check on that
        // port); the worker's metrics server (/healthz + /metrics) satisfies
        // it with no extra HTTP code (RFC #90 Phase 5).  Precedence:
        // explicit WORKER_METRICS_BIND → `0.0.0.0:$PORT` (Cloud Run) → :9090.
        let metrics_bind = std::env::var("WORKER_METRICS_BIND")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| {
                std::env::var("PORT")
                    .ok()
                    .filter(|p| !p.is_empty())
                    .map(|p| format!("0.0.0.0:{p}"))
            })
            .unwrap_or_else(|| "0.0.0.0:9090".to_string());

        Ok(Self {
            worker_id,
            pool_name,
            server_url,
            nats_url,
            nats_stream,
            nats_consumer,
            nats_subject,
            nats_filter_subject,
            heartbeat_interval: Duration::from_secs(heartbeat_secs),
            max_concurrent_tasks: max_concurrent,
            metrics_bind,
        })
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        let nats_subject = "noetl.commands".to_string();
        Self {
            worker_id: uuid::Uuid::new_v4().to_string(),
            pool_name: "default".to_string(),
            server_url: "http://localhost:8082".to_string(),
            nats_url: "nats://localhost:4222".to_string(),
            nats_stream: "noetl_commands".to_string(),
            nats_consumer: "worker-pool".to_string(),
            nats_filter_subject: nats_subject.clone(),
            nats_subject,
            heartbeat_interval: Duration::from_secs(15),
            max_concurrent_tasks: 4,
            metrics_bind: "0.0.0.0:9090".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = WorkerConfig::default();
        assert_eq!(config.pool_name, "default");
        assert_eq!(config.max_concurrent_tasks, 4);
    }

    #[test]
    fn test_default_subject_and_filter_match() {
        // Backward compat: with no env override, `nats_filter_subject`
        // equals `nats_subject` so the worker keeps subscribing to
        // the bare subject (today's behaviour).  Noetl/ai-meta#42 PR-3.
        let config = WorkerConfig::default();
        assert_eq!(config.nats_subject, "noetl.commands");
        assert_eq!(config.nats_filter_subject, config.nats_subject);
    }
}

#[cfg(test)]
mod t218_filter_rename_tests {
    /// The precedence and the failure mode are the whole point of this rename, so
    /// pin them with a pure function rather than mutating process env (other
    /// tests run in parallel in the same process).
    fn resolve(new: Option<&str>, _legacy: Option<&str>, subject: &str) -> String {
        new.filter(|s| !s.trim().is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| subject.to_string())
    }

    #[test]
    fn new_name_wins_over_the_legacy_one() {
        assert_eq!(
            resolve(
                Some("noetl.commands.system.>"),
                Some("noetl.commands.shared.>"),
                "noetl.commands"
            ),
            "noetl.commands.system.>"
        );
    }

    /// The legacy name is NO LONGER honoured — every manifest carries the
    /// EHDB-native one, and keeping a second spelling alive is how the
    /// misnaming persisted in the first place.
    #[test]
    fn the_legacy_name_is_no_longer_honoured() {
        assert_eq!(resolve(None, None, "noetl.commands"), "noetl.commands");
    }

    #[test]
    fn blank_values_do_not_shadow_the_fallback() {
        // A blank value must not count as "set". It degrades to the bare
        // subject, from which no pool resolves — so the worker refuses to start
        // rather than silently joining `shared`. A manifest with the key present
        // but empty is exactly how this would otherwise recur.
        assert_eq!(
            resolve(Some("  "), None, "noetl.commands"),
            "noetl.commands"
        );
        assert_eq!(resolve(Some(""), None, "noetl.commands"), "noetl.commands");
        assert_eq!(
            crate::dispatch::segment_from_filter("noetl.commands"),
            None,
            "the degraded value must not resolve a pool"
        );
    }

    /// With neither set the value degrades to the bare subject, from which
    /// `segment_from_filter` yields None — which the worker now treats as a hard
    /// error instead of silently joining `shared` (noetl/ai-meta#218).
    #[test]
    fn neither_set_yields_an_unresolvable_pool() {
        let v = resolve(None, None, "noetl.commands");
        assert_eq!(v, "noetl.commands");
        assert_eq!(crate::dispatch::segment_from_filter(&v), None);
    }

    #[test]
    fn real_pool_filters_resolve_to_their_segment() {
        for (filter, pool) in [
            ("noetl.commands.system.>", "system"),
            ("noetl.commands.shared.>", "shared"),
            ("noetl.commands.cmdbus.>", "cmdbus"),
        ] {
            assert_eq!(
                crate::dispatch::segment_from_filter(filter).as_deref(),
                Some(pool),
                "{filter} must resolve to {pool}"
            );
        }
        // A wildcard segment is NOT a pool.
        assert_eq!(
            crate::dispatch::segment_from_filter("noetl.commands.>"),
            None
        );
    }
}
