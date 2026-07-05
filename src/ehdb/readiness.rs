//! Bounded, stateless EHDB readiness preflight.
//!
//! A worker/playbook/system process runs [`evaluate`] once at bootstrap to
//! confirm the EHDB local-reference log is reachable, using the in-process
//! [`ehdb_reference::summarize_local_reference`] helper (no subprocess).  The
//! preflight is wired non-fatally into the worker bootstrap: EHDB is auxiliary,
//! so a degraded / unavailable EHDB must never block the worker from starting.
//!
//! Disabled by default: when `NOETL_EHDB_ENABLED` is not truthy the evaluation
//! is `Disabled` and records no metric, so the worker is byte-identical to a
//! build without EHDB.

use std::time::Instant;

use super::contract::{contract_from_env, safe_client_role, EhdbClientRole, EHDB_ENABLED_ENV};
use super::guard::assert_data_plane_access_allowed;
use super::{metrics, EnvMap};

pub const READINESS_TIMEOUT_ENV: &str = "NOETL_EHDB_READINESS_TIMEOUT_SECONDS";
const DEFAULT_TIMEOUT_SECONDS: f64 = 5.0;
const MIN_TIMEOUT_SECONDS: f64 = 0.1;
const MAX_TIMEOUT_SECONDS: f64 = 30.0;

/// Terminal classification of a single readiness evaluation.  Mirrors the
/// retired Python `EhdbReadinessOutcome`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadinessOutcome {
    /// EHDB off — no-op, byte-identical.
    Disabled,
    /// Control-plane role — no data-plane read.
    ControlPlane,
    /// Summary read, at least one non-zero count.
    Ready,
    /// Summary read, all counts zero (fresh log).
    Empty,
    /// Bounded time cap tripped — degraded read.
    Truncated,
    /// Helper errored (missing / unreadable log) — degraded.
    Unavailable,
    /// Control-plane role handed a data-plane env.
    GuardRefused,
    /// Misconfigured EHDB env (non-guard).
    Invalid,
}

impl ReadinessOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReadinessOutcome::Disabled => "disabled",
            ReadinessOutcome::ControlPlane => "control_plane",
            ReadinessOutcome::Ready => "ready",
            ReadinessOutcome::Empty => "empty",
            ReadinessOutcome::Truncated => "truncated",
            ReadinessOutcome::Unavailable => "unavailable",
            ReadinessOutcome::GuardRefused => "guard_refused",
            ReadinessOutcome::Invalid => "invalid",
        }
    }

    /// Whether the process may proceed (everything but guard/invalid).
    pub fn ready(&self) -> bool {
        !matches!(
            self,
            ReadinessOutcome::GuardRefused | ReadinessOutcome::Invalid
        )
    }

    /// Whether the outcome, while allowing the process to run, signals a
    /// suboptimal (degraded) EHDB.
    pub fn degraded(&self) -> bool {
        matches!(
            self,
            ReadinessOutcome::Truncated | ReadinessOutcome::Unavailable
        )
    }
}

/// Structured, secret-free result of a readiness evaluation.
#[derive(Debug, Clone)]
pub struct ReadinessResult {
    pub outcome: ReadinessOutcome,
    pub role: Option<EhdbClientRole>,
    pub duration_seconds: f64,
    /// Total record/entity count across the summary (0 ⇒ empty log).
    pub total_count: u64,
    pub detail: Option<String>,
}

fn bounded_timeout(env: &EnvMap) -> f64 {
    let raw = env
        .get(READINESS_TIMEOUT_ENV)
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(DEFAULT_TIMEOUT_SECONDS);
    raw.clamp(MIN_TIMEOUT_SECONDS, MAX_TIMEOUT_SECONDS)
}

fn truthy(env: &EnvMap, key: &str) -> bool {
    matches!(
        env.get(key)
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref(),
        Some("1" | "true" | "yes" | "y" | "on")
    )
}

/// Evaluate EHDB local-reference readiness for the current role.
///
/// `record_metrics` gates the process-local metric side effect (tests pass
/// `false` to keep the accumulators clean).
pub fn evaluate(env: &EnvMap, record_metrics: bool) -> ReadinessResult {
    let started = Instant::now();

    let finish = |outcome: ReadinessOutcome,
                  role: Option<EhdbClientRole>,
                  total_count: u64,
                  detail: Option<String>| {
        let result = ReadinessResult {
            outcome,
            role,
            duration_seconds: started.elapsed().as_secs_f64(),
            total_count,
            detail,
        };
        if record_metrics {
            metrics::record_readiness(
                outcome.as_str(),
                outcome.ready(),
                outcome.degraded(),
                result.duration_seconds,
            );
        }
        result
    };

    // 1. Disabled fast path — strict no-op (no read, no metric side effect).
    if !truthy(env, EHDB_ENABLED_ENV) {
        return finish(ReadinessOutcome::Disabled, None, 0, None);
    }

    // 2. Build the contract.  A control-plane role carrying a data-plane env is
    //    a guard refusal; any other config error is Invalid.
    let contract = match contract_from_env(env) {
        Ok(c) => c,
        Err(err) => {
            let role = safe_client_role(env);
            let outcome = if role.map(|r| r.is_control_plane()).unwrap_or(false) {
                ReadinessOutcome::GuardRefused
            } else {
                ReadinessOutcome::Invalid
            };
            return finish(outcome, role, 0, Some(err.0));
        }
    };

    // 3. Control-plane roles never perform a data-plane read.
    if contract.role.is_control_plane() {
        return finish(ReadinessOutcome::ControlPlane, Some(contract.role), 0, None);
    }

    // 4. Data-plane role: enforce the guard, then run the bounded read.
    if let Err(err) = assert_data_plane_access_allowed(contract.role, "readiness") {
        return finish(
            ReadinessOutcome::GuardRefused,
            Some(contract.role),
            0,
            Some(err.to_string()),
        );
    }

    let Some(log_path) = contract.local_reference_log.clone() else {
        // Contract validation guarantees a log in local_reference mode; treat a
        // missing one defensively as disabled rather than inventing readiness.
        return finish(ReadinessOutcome::Disabled, Some(contract.role), 0, None);
    };

    let cap = bounded_timeout(env);
    match ehdb_reference::summarize_local_reference(&log_path) {
        Ok(summary) => {
            let total = summary_total(&summary);
            let elapsed = started.elapsed().as_secs_f64();
            if elapsed > cap {
                return finish(
                    ReadinessOutcome::Truncated,
                    Some(contract.role),
                    total,
                    Some(format!("summary read exceeded {cap:.3}s cap")),
                );
            }
            let outcome = if total == 0 {
                ReadinessOutcome::Empty
            } else {
                ReadinessOutcome::Ready
            };
            finish(outcome, Some(contract.role), total, None)
        }
        Err(err) => finish(
            ReadinessOutcome::Unavailable,
            Some(contract.role),
            0,
            Some(err.to_string()),
        ),
    }
}

fn summary_total(summary: &ehdb_reference::LocalReferenceSummary) -> u64 {
    (summary.transaction_count
        + summary.table_count
        + summary.snapshot_count
        + summary.scan_grant_count
        + summary.stream_count
        + summary.stream_record_count
        + summary.stream_consumer_count
        + summary.retrieval_document_count
        + summary.retrieval_chunk_count
        + summary.retrieval_embedding_count
        + summary.system_library_count
        + summary.system_binding_count
        + summary.storage_object_count
        + summary.storage_replica_count) as u64
}

/// Non-fatal bootstrap preflight.  Reads the process env, evaluates readiness,
/// logs a single structured line, and records the metric (when enabled).  Never
/// returns an error — EHDB is auxiliary and must not block worker startup.
pub fn run_preflight(worker_id: &str) {
    let env = super::process_env();
    let result = evaluate(&env, true);
    match result.outcome {
        ReadinessOutcome::Disabled => {
            // Silent: byte-identical to a build without EHDB.
        }
        ReadinessOutcome::GuardRefused | ReadinessOutcome::Invalid => {
            tracing::error!(
                worker_id,
                ehdb_outcome = result.outcome.as_str(),
                ehdb_role = result.role.map(|r| r.as_str()).unwrap_or("unknown"),
                ehdb_detail = result.detail.as_deref().unwrap_or(""),
                "EHDB readiness preflight refused"
            );
        }
        ReadinessOutcome::Truncated | ReadinessOutcome::Unavailable => {
            tracing::warn!(
                worker_id,
                ehdb_outcome = result.outcome.as_str(),
                ehdb_role = result.role.map(|r| r.as_str()).unwrap_or("unknown"),
                ehdb_detail = result.detail.as_deref().unwrap_or(""),
                "EHDB readiness preflight degraded"
            );
        }
        _ => {
            tracing::info!(
                worker_id,
                ehdb_outcome = result.outcome.as_str(),
                ehdb_role = result.role.map(|r| r.as_str()).unwrap_or("unknown"),
                ehdb_total_count = result.total_count,
                "EHDB readiness preflight ok"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> EnvMap {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn disabled_is_noop() {
        let r = evaluate(&env(&[]), false);
        assert_eq!(r.outcome, ReadinessOutcome::Disabled);
    }

    #[test]
    fn control_plane_role_no_read() {
        let r = evaluate(
            &env(&[
                ("NOETL_EHDB_ENABLED", "true"),
                ("NOETL_EHDB_MODE", "control_plane"),
                ("NOETL_EHDB_CLIENT_ROLE", "server"),
                ("NOETL_EHDB_CAPABILITIES", "control_plane"),
            ]),
            false,
        );
        assert_eq!(r.outcome, ReadinessOutcome::ControlPlane);
    }

    #[test]
    fn gateway_data_plane_env_is_guard_refused() {
        let r = evaluate(
            &env(&[
                ("NOETL_EHDB_ENABLED", "true"),
                ("NOETL_EHDB_MODE", "local_reference"),
                ("NOETL_EHDB_CLIENT_ROLE", "gateway"),
                ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ]),
            false,
        );
        assert_eq!(r.outcome, ReadinessOutcome::GuardRefused);
    }

    #[test]
    fn worker_fresh_log_is_empty() {
        let dir = std::env::temp_dir().join(format!("ehdb-readiness-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let log = dir.join("ready.jsonl");
        let r = evaluate(
            &env(&[
                ("NOETL_EHDB_ENABLED", "true"),
                ("NOETL_EHDB_MODE", "local_reference"),
                ("NOETL_EHDB_CLIENT_ROLE", "worker"),
                ("NOETL_EHDB_LOCAL_REFERENCE_LOG", log.to_str().unwrap()),
            ]),
            false,
        );
        assert_eq!(r.outcome, ReadinessOutcome::Empty);
        assert_eq!(r.total_count, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
