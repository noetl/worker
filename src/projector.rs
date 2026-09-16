//! The `system/projector` drain loop — noetl/server#203 phase 2b-2.
//!
//! Folds the event feed and advances `noetl.projection_snapshot` via
//! `POST /api/internal/projection/advance`, so the projector owns the read model
//! the orchestrator reads instead of the orchestrator self-writing it.
//!
//! # ⚠ Two ways the issue's spec is stale, and what was built instead
//!
//! The issue describes "the `system/projector` **catalog playbook** — a
//! `noetl_events` **JetStream** batch consumer". Neither half matches reality:
//!
//! 1. Its sibling, phase 2d's `system/event_materializer`, is described in the
//!    identical words and is **not a playbook** — it is a worker background loop
//!    ([`crate::materializer`]), running on the system pool in kind AND prod.
//!    The only genuine `system/*` catalog artifact is `system/orchestrate`, and
//!    that is a WASM plug-in.
//! 2. ⚠⚠ **There is no `noetl_events` stream.** Queried directly: NATS carries
//!    exactly one stream, `NOETL_COMMANDS`. The materializer runs
//!    `NOETL_MATERIALIZER_SOURCE=ehdb` in both environments — the live transport
//!    is the EHDB bus.
//!
//! A projector written literally to spec would subscribe to a stream that is
//! never created and **silently process nothing**, which is the failure class
//! this codebase keeps paying for. So this mirrors the materializer: same source
//! abstraction, so it follows whatever transport is actually live.
//!
//! # Blast radius
//!
//! Zero until two flags are flipped. `NOETL_PROJECTOR_ENABLED` off ⇒ no loop
//! runs. `NOETL_PROJECTOR_OWNS_SNAPSHOT` off (server-side, its own default) ⇒
//! the orchestrator self-writes exactly as today.

use anyhow::{anyhow, Context, Result};
use std::collections::BTreeSet;
use std::time::Duration;

use crate::config::WorkerConfig;

/// How long a projector HTTP call may take before it is abandoned.
///
/// ⚠ Generous on purpose, and bounded on purpose. `advance` recomputes a
/// snapshot per execution, so a large batch is legitimately slow; a timeout
/// below that converts a working-but-slow projector into a failing one. But it
/// must be bounded — `reqwest::Client::new()` has no timeout at all, which is
/// the defect noetl/worker#324 fixed in this very crate.
const PROJECTOR_HTTP_TIMEOUT: Duration = Duration::from_secs(300);

/// The feed consumer group the projector owns. Deliberately not the
/// materializer's — see [`ProjectorConfig::group`].
pub const DEFAULT_PROJECTOR_GROUP: &str = "noetl_projector";

/// True when `NOETL_PROJECTOR_ENABLED` is set to a truthy value.
pub fn enabled() -> bool {
    matches!(
        std::env::var("NOETL_PROJECTOR_ENABLED")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Resolved projector configuration.
#[derive(Debug, Clone)]
pub struct ProjectorConfig {
    pub server_url: String,
    pub internal_token: String,
    pub batch: u32,
    pub idle_sleep: Duration,
    pub error_backoff: Duration,
    /// `NOETL_EVENT_BUS_CLAIM_ADDR` — the events feed's group-claim address.
    pub claim_addr: String,
    /// The feed consumer group this projector owns.
    ///
    /// ⚠ MUST NOT be the materializer's group. Two consumers sharing a group
    /// split the feed between them, so each would see roughly half the events
    /// and neither would fail — the projector would advance snapshots for half
    /// the executions and look healthy doing it.
    pub group: String,
    /// How long one poll waits for the batch to fill.
    pub poll_timeout: Duration,
}

impl ProjectorConfig {
    /// Build from the worker config + env. `Ok(None)` when disabled; `Err` when
    /// enabled but missing a hard requirement, so a misconfigured system pool
    /// fails LOUD instead of silently not projecting — the same posture
    /// [`crate::materializer::MaterializerConfig::from_env`] takes.
    pub fn from_env(worker: &WorkerConfig) -> Result<Option<Self>> {
        if !enabled() {
            return Ok(None);
        }
        let internal_token = std::env::var("NOETL_INTERNAL_API_TOKEN")
            .ok()
            .filter(|t| !t.trim().is_empty())
            .context(
                "NOETL_PROJECTOR_ENABLED is set but NOETL_INTERNAL_API_TOKEN is empty — \
                 the projector needs it to call /api/internal/projection/advance",
            )?;
        let claim_addr = std::env::var("NOETL_EVENT_BUS_CLAIM_ADDR")
            .ok()
            .filter(|a| !a.trim().is_empty())
            .context(
                "NOETL_PROJECTOR_ENABLED is set but NOETL_EVENT_BUS_CLAIM_ADDR is empty — \
                 the projector drains the EHDB events feed and has nothing to connect to",
            )?;
        let group = std::env::var("NOETL_PROJECTOR_GROUP")
            .ok()
            .map(|g| g.trim().to_string())
            .filter(|g| !g.is_empty())
            .unwrap_or_else(|| DEFAULT_PROJECTOR_GROUP.to_string());
        if group == crate::materializer::MATERIALIZER_CONSUMER {
            return Err(anyhow!(
                "NOETL_PROJECTOR_GROUP is {group:?}, which is the materializer's group. \
                 Sharing a group splits the feed between the two consumers, so each would \
                 see about half the events and NEITHER would report an error — the \
                 projector would advance half the executions and look healthy. Use a \
                 distinct group."
            ));
        }
        Ok(Some(Self {
            server_url: worker.server_url.trim_end_matches('/').to_string(),
            internal_token,
            batch: env_u32("NOETL_PROJECTOR_BATCH", 64).clamp(1, 1000),
            idle_sleep: Duration::from_millis(env_u64("NOETL_PROJECTOR_IDLE_SLEEP_MS", 500)),
            error_backoff: Duration::from_millis(env_u64("NOETL_PROJECTOR_ERROR_BACKOFF_MS", 2000)),
            claim_addr,
            group,
            poll_timeout: Duration::from_millis(env_u64("NOETL_PROJECTOR_TIMEOUT_MS", 1000)),
        }))
    }
}

fn env_u32(k: &str, d: u32) -> u32 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(d)
}
fn env_u64(k: &str, d: u64) -> u64 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(d)
}

/// A bounded HTTP client for the projector.
fn projector_http() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(PROJECTOR_HTTP_TIMEOUT)
        .build()
        .map_err(|e| anyhow!("projector: could not build HTTP client: {e}"))
}

/// The distinct execution ids carried by a batch of events.
///
/// ⚠ DISTINCT is load-bearing. A batch routinely holds many events for one
/// execution, and `advance` recomputes that execution's whole snapshot — so
/// sending an id N times does the same expensive work N times for one result.
/// `BTreeSet` also makes the order deterministic, which keeps a failing batch
/// reproducible.
pub(crate) fn execution_ids(events: &[serde_json::Value]) -> Vec<i64> {
    let mut seen = BTreeSet::new();
    for e in events {
        let id = e.get("execution_id").and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        });
        if let Some(id) = id {
            seen.insert(id);
        }
    }
    seen.into_iter().collect()
}

/// Did an `advance` reply report every execution as advanced?
///
/// ⚠⚠ The endpoint returns **200 even when some executions failed** — it
/// collects them into `failed[]` rather than failing the batch. So a 2xx is NOT
/// a success signal, and acking on status alone would silently drop the failed
/// executions' work. Only a reply with an EMPTY `failed[]` may be acked.
///
/// ⚠ Acking only on full success admits redelivery of executions that already
/// advanced. That is safe: `advance` recomputes and saves a snapshot, so
/// applying it twice yields the same snapshot. Duplicated work, never divergence.
pub(crate) fn advance_fully_succeeded(reply: &serde_json::Value) -> bool {
    match reply.get("failed") {
        Some(serde_json::Value::Array(f)) => f.is_empty(),
        // A reply without the field is not evidence of success — an older or
        // unexpected shape must not be read as "nothing failed".
        _ => false,
    }
}

/// Spawn the projector loop.
pub fn spawn(config: ProjectorConfig) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = run_loop(config).await {
            tracing::error!(error = %e, "projector loop exited with error");
        }
    })
}

/// Split a drained batch's ack tokens by whether their execution advanced.
///
/// ⚠⚠ THIS IS THE REDELIVERY POLICY, AND IT IS PER-EXECUTION ON PURPOSE
/// (noetl/server#203 decision 3 — nack failures).
///
/// The simple policies are both wrong here:
///
/// * **Ack the whole batch on a 200.** `projection/advance` returns 200 with a
///   populated `failed[]`, so this silently drops the failed executions' work —
///   their events are gone from the feed and their snapshot never advances.
/// * **Hold the whole batch when anything failed.** Correct about loss, but one
///   permanently-broken execution then blocks every other execution in the feed
///   forever. The projector stalls completely, and the stall looks like
///   idleness.
///
/// So: ack the events whose execution advanced, hold the ones whose execution
/// did not. Held events redeliver, nothing is dropped, and a stuck execution
/// blocks only itself while the rest of the feed keeps moving.
///
/// Redelivery of an already-advanced execution is safe: `advance` recomputes
/// and monotonically upserts a snapshot, so re-applying is a no-op or a forward
/// move. Duplicated work, never divergence.
///
/// Events with **no** `execution_id` can never advance, so holding them would
/// poison-loop the cursor. They are acked — and returned separately so the
/// caller counts them, because an ack that did nothing must not be silent.
pub(crate) fn partition_acks(
    events: &[(u64, Option<i64>)],
    advanced: &BTreeSet<i64>,
) -> (Vec<u64>, Vec<u64>, u64) {
    let mut ack = Vec::new();
    let mut held = Vec::new();
    let mut unaddressable = 0u64;
    for (sort_key, execution_id) in events {
        match execution_id {
            None => {
                unaddressable += 1;
                ack.push(*sort_key);
            }
            Some(id) if advanced.contains(id) => ack.push(*sort_key),
            Some(_) => held.push(*sort_key),
        }
    }
    (ack, held, unaddressable)
}

/// The `execution_id` carried by one event payload, if it has a usable one.
pub(crate) fn event_execution_id(e: &serde_json::Value) -> Option<i64> {
    e.get("execution_id").and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    })
}

/// The executions `projection/advance` reported as advanced.
pub(crate) fn advanced_ids(reply: &serde_json::Value) -> BTreeSet<i64> {
    reply
        .get("advanced")
        .and_then(|v| v.as_array())
        .map(|rows| {
            rows.iter()
                .filter_map(|r| r.get("execution_id").and_then(|v| v.as_i64()))
                .collect()
        })
        .unwrap_or_default()
}

/// The executions `projection/advance` reported as failed, with their errors.
pub(crate) fn failed_ids(reply: &serde_json::Value) -> Vec<(i64, String)> {
    reply
        .get("failed")
        .and_then(|v| v.as_array())
        .map(|rows| {
            rows.iter()
                .filter_map(|r| {
                    let id = r.get("execution_id").and_then(|v| v.as_i64())?;
                    let err = r
                        .get("error")
                        .and_then(|v| v.as_str())
                        .unwrap_or("<no error reported>")
                        .to_string();
                    Some((id, err))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// POST the deduped execution ids to `projection/advance`.
async fn advance(
    http: &reqwest::Client,
    url: &str,
    token: &str,
    execution_ids: &[i64],
) -> Result<serde_json::Value> {
    let resp = http
        .post(url)
        .bearer_auth(token)
        .json(&serde_json::json!({ "execution_ids": execution_ids }))
        .send()
        .await
        .context("projection/advance request failed")?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "projection/advance HTTP {}: {}",
            status.as_u16(),
            body
        ));
    }
    resp.json().await.context("projection/advance decode")
}

/// The drain → advance → ack loop.
async fn run_loop(config: ProjectorConfig) -> Result<()> {
    let source = crate::event_bus::EhdbGroupSource::connect(
        config.claim_addr.clone(),
        config.group.clone(),
        crate::event_bus::ALL_EVENTS_FILTER.to_string(),
        member_id(&config.group),
        (config.batch as usize).max(1),
    )
    .await?;
    let http = projector_http()?;
    let url = format!("{}/api/internal/projection/advance", config.server_url);

    tracing::info!(
        claim_addr = %config.claim_addr,
        group = %config.group,
        batch = config.batch,
        "CQRS projector started on the EHDB events feed (ack only what advanced)"
    );

    loop {
        let cycle_start = std::time::Instant::now();
        let drained_batch = source.poll(config.batch as usize, config.poll_timeout).await;
        let drained = drained_batch.len();
        if drained == 0 {
            if source.is_finished() {
                // The claim task is gone. Returning parks nothing and hides
                // nothing: `spawn` logs the error and the loop is over, which is
                // the honest outcome. Silently sleeping here would leave a
                // projector that reports healthy and drains nothing.
                return Err(anyhow!(
                    "events-feed claim task exited; projector cannot drain"
                ));
            }
            tokio::time::sleep(config.idle_sleep).await;
            continue;
        }

        let keyed: Vec<(u64, Option<i64>)> = drained_batch
            .iter()
            .map(|d| (d.sort_key, event_execution_id(&d.payload)))
            .collect();
        let payloads: Vec<serde_json::Value> =
            drained_batch.iter().map(|d| d.payload.clone()).collect();
        let ids = execution_ids(&payloads);

        if ids.is_empty() {
            // Nothing in this batch can ever advance. Ack so the cursor moves —
            // holding would poison-loop — and COUNT it, because an ack that
            // advanced nothing must never be silent.
            let keys: Vec<u64> = keyed.iter().map(|(k, _)| *k).collect();
            crate::metrics::record_projector_unaddressable(keys.len() as u64);
            tracing::warn!(
                drained,
                "projector: batch carried no usable execution_id; acked to keep the cursor moving"
            );
            if let Err(error) = source.ack(&keys).await {
                crate::metrics::record_projector_error("ack");
                tracing::warn!(%error, "projector ack failed on an unaddressable batch");
            }
            continue;
        }

        match advance(&http, &url, &config.internal_token, &ids).await {
            Ok(reply) => {
                let advanced = advanced_ids(&reply);
                let failed = failed_ids(&reply);
                let (ack_keys, held_keys, unaddressable) = partition_acks(&keyed, &advanced);

                if !advance_fully_succeeded(&reply) {
                    // ⚠ A 200 that is not a success. Deliberately asked via
                    // `advance_fully_succeeded` rather than `!failed.is_empty()`:
                    // the two agree on every well-formed reply, but they differ
                    // on a reply with NO `failed` field at all — an older or
                    // unexpected server shape. `!failed.is_empty()` would read
                    // that as "nothing failed"; this reads it as "not evidence
                    // of success", which is the posture that does not invent
                    // good news from a shape it does not recognise.
                    crate::metrics::record_projector_error("partial");
                    for (execution_id, error) in &failed {
                        tracing::warn!(
                            execution_id,
                            %error,
                            "projector: execution did NOT advance; its events are held un-acked and will redeliver"
                        );
                    }
                }
                crate::metrics::record_projector_unaddressable(unaddressable);

                let acked = match source.ack(&ack_keys).await {
                    Ok(n) => n,
                    Err(error) => {
                        // The snapshots ARE saved; only the ack failed. Those
                        // events redeliver and `advance` is idempotent, so this
                        // costs repeated work, never a lost advance.
                        crate::metrics::record_projector_error("ack");
                        tracing::warn!(%error, "projector ack failed after a durable advance");
                        0
                    }
                };

                crate::metrics::record_projector_cycle(
                    drained as u64,
                    advanced.len() as u64,
                    acked as u64,
                    held_keys.len() as u64,
                    cycle_start.elapsed().as_secs_f64(),
                );
                tracing::debug!(
                    drained,
                    executions = ids.len(),
                    advanced = advanced.len(),
                    failed = failed.len(),
                    acked,
                    held = held_keys.len(),
                    "projector cycle: drained → advanced → acked"
                );

                if !held_keys.is_empty() {
                    tokio::time::sleep(config.error_backoff).await;
                }
            }
            Err(e) => {
                // Transport or non-2xx: ack NOTHING. The whole batch redelivers.
                crate::metrics::record_projector_error("http");
                crate::metrics::record_projector_cycle(
                    drained as u64,
                    0,
                    0,
                    drained as u64,
                    cycle_start.elapsed().as_secs_f64(),
                );
                tracing::warn!(
                    drained,
                    error = %e,
                    "projector advance failed; batch NOT acked, will redeliver"
                );
                tokio::time::sleep(config.error_backoff).await;
            }
        }
    }
}

/// Stable non-zero member id for a group member, derived from the group name —
/// the same derivation the materializer uses.
fn member_id(name: &str) -> u32 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut h);
    ((h.finish() as u32) | 1).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// ⭐ Default OFF — the whole blast-radius argument rests on this.
    #[test]
    fn the_projector_is_off_unless_deliberately_enabled() {
        if std::env::var("NOETL_PROJECTOR_ENABLED").is_ok() {
            return; // set in this environment; the default is not observable
        }
        assert!(
            !enabled(),
            "an unset NOETL_PROJECTOR_ENABLED must leave the projector inert, so \
             deploying this image cannot start projecting as a side effect"
        );
    }

    /// ⭐ Distinct ids, deterministic order.
    #[test]
    fn a_batch_yields_distinct_execution_ids() {
        let events = vec![
            json!({"execution_id": 3}),
            json!({"execution_id": 1}),
            json!({"execution_id": 3}),
            json!({"execution_id": "2"}),
            json!({"no_execution_id": true}),
        ];
        assert_eq!(
            execution_ids(&events),
            vec![1, 2, 3],
            "ids must be de-duplicated and ordered — `advance` recomputes a whole \
             snapshot per id, so a repeat is the same expensive work twice"
        );
        assert!(
            execution_ids(&[]).is_empty(),
            "an empty batch yields nothing"
        );
    }

    /// ⭐⭐ THE REDELIVERY POLICY (noetl/server#203 decision 3 — nack failures).
    ///
    /// One batch, two executions, one of them failed. The failing execution's
    /// events must be HELD (un-acked, so they redeliver); the succeeding one's
    /// must be ACKED (so a permanently-broken execution cannot stall the feed
    /// for everyone else).
    #[test]
    fn a_failed_execution_holds_only_its_own_events() {
        let advanced: BTreeSet<i64> = [7].into_iter().collect();
        let batch = vec![
            (100, Some(7)),  // advanced
            (101, Some(9)),  // failed
            (102, Some(7)),  // advanced
            (103, Some(9)),  // failed
        ];
        let (ack, held, unaddressable) = partition_acks(&batch, &advanced);
        assert_eq!(
            ack,
            vec![100, 102],
            "events of an execution that advanced are acked — otherwise one \
             broken execution stalls the whole feed"
        );
        assert_eq!(
            held,
            vec![101, 103],
            "events of an execution that did NOT advance are held un-acked so \
             they redeliver — acking them would silently drop their work"
        );
        assert_eq!(unaddressable, 0);
    }

    /// ⚠ An HTTP failure means NOTHING advanced, so nothing may be acked.
    #[test]
    fn nothing_advanced_means_nothing_acked() {
        let batch = vec![(1, Some(5)), (2, Some(6))];
        let (ack, held, _) = partition_acks(&batch, &BTreeSet::new());
        assert!(
            ack.is_empty(),
            "with an empty advanced set every event must be held"
        );
        assert_eq!(held, vec![1, 2]);
    }

    /// Events that can never advance are acked — holding them poison-loops the
    /// cursor — but they are COUNTED, so the ack is never silent.
    #[test]
    fn unaddressable_events_are_acked_but_counted() {
        let advanced: BTreeSet<i64> = [4].into_iter().collect();
        let batch = vec![(10, None), (11, Some(4)), (12, None)];
        let (ack, held, unaddressable) = partition_acks(&batch, &advanced);
        assert_eq!(
            ack,
            vec![10, 11, 12],
            "an event with no execution_id can never advance; holding it would \
             block the cursor forever"
        );
        assert!(held.is_empty());
        assert_eq!(
            unaddressable, 2,
            "acked-without-advancing must be counted, never silent"
        );
    }

    /// The reply parsers read the shape the server actually returns.
    #[test]
    fn advanced_and_failed_are_read_from_the_real_reply_shape() {
        let reply = json!({
            "advanced": [
                {"execution_id": 7, "version": 42, "events": 9},
                {"execution_id": 8, "version": 11, "events": 3}
            ],
            "failed": [{"execution_id": 9, "error": "boom"}]
        });
        assert_eq!(advanced_ids(&reply), [7, 8].into_iter().collect());
        assert_eq!(failed_ids(&reply), vec![(9, "boom".to_string())]);
        assert!(!advance_fully_succeeded(&reply));

        let clean = json!({"advanced": [{"execution_id": 1, "version": 2, "events": 3}], "failed": []});
        assert!(advance_fully_succeeded(&clean));
        assert_eq!(advanced_ids(&clean), [1].into_iter().collect());
        assert!(failed_ids(&clean).is_empty());
    }

    /// ⭐ The projector must not share the materializer's consumer group.
    ///
    /// Sharing one splits the feed between the two consumers, so each sees
    /// roughly half the events and NEITHER errors — the projector would advance
    /// half the executions and look healthy doing it.
    #[test]
    fn the_projector_group_is_not_the_materializers() {
        assert_ne!(
            DEFAULT_PROJECTOR_GROUP,
            crate::materializer::MATERIALIZER_CONSUMER,
            "a shared group silently halves the feed each consumer sees"
        );
    }

    /// ⚠⚠ A projector nobody spawns is a projector that silently does nothing.
    ///
    /// This module's whole contract is "with the flag on, snapshots advance".
    /// It was previously wired nowhere: `worker.rs` spawned the materializer and
    /// its three siblings and never mentioned this one, so flipping the flag
    /// would have produced exactly the silent no-op the module header warns
    /// about. A source guard, because the property is about a call site in
    /// another file that no unit test here can reach.
    #[test]
    fn the_projector_is_actually_spawned_by_the_worker() {
        let worker_rs = include_str!("worker.rs");
        let calls: Vec<&str> = worker_rs
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.starts_with("//"))
            .filter(|l| l.contains("projector::spawn"))
            .collect();
        assert_eq!(
            calls.len(),
            1,
            "expected exactly one non-comment `projector::spawn` call site in \
             worker.rs, found {}: {:?}. Comments do not spawn tasks.",
            calls.len(),
            calls
        );
    }

    /// ⚠⚠ A 200 is not success. The endpoint collects per-execution failures.
    #[test]
    fn only_a_reply_with_no_failures_may_be_acked() {
        assert!(advance_fully_succeeded(
            &json!({"advanced": [1, 2], "failed": []})
        ));
        assert!(
            !advance_fully_succeeded(&json!({"advanced": [1], "failed": [{"execution_id": 2}]})),
            "a partially-failed batch must NOT be acked — the endpoint returns 200 \
             for it, so acking on status would silently drop the failed work"
        );
        assert!(
            !advance_fully_succeeded(&json!({"advanced": [1]})),
            "a reply with no `failed` field is not evidence of success"
        );
        assert!(
            !advance_fully_succeeded(&json!("nonsense")),
            "a non-object is not success"
        );
    }
}
