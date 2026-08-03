//! CQRS event materializer — the durable `noetl.event` writer
//! ([noetl/ai-meta#103](https://github.com/noetl/ai-meta/issues/103)).
//!
//! ## Why this is a worker consume-loop and not a playbook
//!
//! Under `NOETL_EVENT_INGEST_PUBLISH_ONLY` the server stops writing
//! `noetl.event` synchronously and publishes every event to the
//! `noetl_events` JetStream stream instead; a drainer becomes the **sole**
//! writer. The drainer must do **ack-after-materialize**: ack a stream
//! message only AFTER its row is durably in `noetl.event`, so a transient
//! failure between drain and write redelivers the batch instead of losing it.
//!
//! The original drainer was the `system/event_materializer` playbook, which
//! acked **on fetch** (`ack: on_success`) — the durability hole this module
//! closes. The playbook step model can't hold an ack handle across the
//! drain→build→project steps cleanly: the handles (one per message) would
//! ride through playbook state across atomic blocks on different pods, where a
//! batch over the inline-context budget gets staged to the result store as a
//! `_ref` (the documented `{{ drain_events.count }}` stall), and concurrent
//! cron-triggered drains split batches. A single in-process loop has none of
//! that: it owns the consumer, drains a bounded batch with **deferred ack**
//! ([`AckMode::Defer`]), POSTs `events/project`, and acks **only on 2xx**.
//! On any failure it leaves the batch un-acked → JetStream redelivers after
//! the consumer's ack-wait. Serial by construction, so ordering holds and no
//! batch is split; idempotent `events/project` (`ON CONFLICT`) makes the
//! redelivery path a no-op double-write, never a duplicate row.
//!
//! Per [`data-access-boundary.md`](https://github.com/noetl/ai-meta/blob/main/agents/rules/data-access-boundary.md)
//! the loop never touches `noetl.*` directly — it drains a NATS stream and
//! writes through the server's `POST /api/internal/events/project` API.
//!
//! Opt-in: spawned only when `NOETL_MATERIALIZER_ENABLED` is truthy (set on
//! the system worker pool). Default off — every other worker is unaffected.

use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use noetl_tools::tools::source::{AckDisposition, AckMode, PollOptions, SourceClient};
use noetl_tools::tools::{build_source, SubscriptionConfig};
use noetl_tools::ExecutionContext;

use crate::config::WorkerConfig;

/// JetStream stream the server publishes events to (mirror of the server's
/// `EVENT_STREAM`).
pub const EVENT_STREAM: &str = "noetl_events";

/// Durable pull consumer the materializer drains. The server ensures this
/// consumer (ack-explicit) at startup; the loop is a pure consumer of it.
pub const MATERIALIZER_CONSUMER: &str = "noetl_materializer";

/// Default bounded-drain batch. Kept well under the source cap; ack-after
/// -materialize means a larger batch only widens the redelivery blast radius
/// on a failure, so we stay modest.
const DEFAULT_BATCH: u32 = 200;
/// Default bounded-drain wait.
const DEFAULT_TIMEOUT_MS: u64 = 2_000;
/// Sleep when a drain comes back empty — keeps the idle loop off the CPU
/// without adding meaningful materialization latency.
const DEFAULT_IDLE_SLEEP_MS: u64 = 500;
/// Backoff after a project failure before the next drain attempt. The real
/// redelivery delay is the consumer's ack-wait; this just avoids hot-looping
/// against a down server.
const DEFAULT_ERROR_BACKOFF_MS: u64 = 2_000;

/// Resolved materializer configuration.
pub struct MaterializerConfig {
    /// NATS connection (creds parsed out of the worker's `NATS_URL`).
    pub nats_url: String,
    pub nats_user: Option<String>,
    pub nats_password: Option<String>,
    /// Stream + durable consumer to drain.
    pub stream: String,
    pub consumer: String,
    /// Control-plane base URL for `events/project`.
    pub server_url: String,
    /// Bearer for the internal API (`NOETL_INTERNAL_API_TOKEN`).
    pub internal_token: String,
    pub batch: u32,
    pub timeout_ms: u64,
    pub idle_sleep: Duration,
    pub error_backoff: Duration,
    /// noetl/ai-meta#212 T3 — which transport this materializer drains.
    /// `NOETL_MATERIALIZER_SOURCE`: `nats` (default) or `ehdb`. Independent of
    /// the server's `NOETL_EVENT_BUS` publish mode on purpose, so a consumer can
    /// be cut over one at a time while publish stays in `shadow`.
    pub source_mode: crate::event_bus::EventSourceMode,
    /// `NOETL_EVENT_BUS_CLAIM_ADDR` — the events feed's group-claim address.
    pub event_claim_addr: Option<String>,

    /// Chaos / validation knob: fail (skip the POST + leave the batch
    /// un-acked) the first N non-empty cycles, to exercise the redelivery
    /// path deterministically. `NOETL_MATERIALIZER_FAULT_FAIL_FIRST`, default
    /// 0 (disabled). Never set in production.
    pub fault_fail_first: u32,
}

/// True when `NOETL_MATERIALIZER_ENABLED` is set to a truthy value.
pub fn enabled() -> bool {
    matches!(
        std::env::var("NOETL_MATERIALIZER_ENABLED")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

impl MaterializerConfig {
    /// Build the config from the worker config + env. Returns `Ok(None)` when
    /// the materializer is disabled; `Err` when it's enabled but missing a
    /// hard requirement (the internal token), so a misconfigured system pool
    /// fails loud instead of silently not materializing.
    pub fn from_env(worker: &WorkerConfig) -> Result<Option<Self>> {
        if !enabled() {
            return Ok(None);
        }

        let internal_token = std::env::var("NOETL_INTERNAL_API_TOKEN")
            .ok()
            .filter(|t| !t.trim().is_empty())
            .context(
                "NOETL_MATERIALIZER_ENABLED is set but NOETL_INTERNAL_API_TOKEN is empty — \
                 the materializer needs it to call /api/internal/events/project",
            )?;

        let (nats_url, nats_user, nats_password) = parse_nats_credentials(&worker.nats_url);

        let batch = env_u32("NOETL_MATERIALIZER_BATCH", DEFAULT_BATCH).clamp(1, 1000);
        let timeout_ms = env_u64("NOETL_MATERIALIZER_TIMEOUT_MS", DEFAULT_TIMEOUT_MS);
        let idle_sleep = Duration::from_millis(env_u64(
            "NOETL_MATERIALIZER_IDLE_SLEEP_MS",
            DEFAULT_IDLE_SLEEP_MS,
        ));
        let error_backoff = Duration::from_millis(env_u64(
            "NOETL_MATERIALIZER_ERROR_BACKOFF_MS",
            DEFAULT_ERROR_BACKOFF_MS,
        ));
        let fault_fail_first = env_u32("NOETL_MATERIALIZER_FAULT_FAIL_FIRST", 0);

        Ok(Some(Self {
            nats_url,
            nats_user,
            nats_password,
            stream: std::env::var("NOETL_MATERIALIZER_STREAM")
                .unwrap_or_else(|_| EVENT_STREAM.to_string()),
            consumer: std::env::var("NOETL_MATERIALIZER_CONSUMER")
                .unwrap_or_else(|_| MATERIALIZER_CONSUMER.to_string()),
            server_url: worker.server_url.trim_end_matches('/').to_string(),
            internal_token,
            batch,
            timeout_ms,
            idle_sleep,
            error_backoff,
            fault_fail_first,
            source_mode: crate::event_bus::EventSourceMode::from_env_strict(
                "NOETL_MATERIALIZER_SOURCE",
            )?,
            event_claim_addr: std::env::var("NOETL_EVENT_BUS_CLAIM_ADDR")
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|s| !s.is_empty()),
        }))
    }

    /// The `SubscriptionConfig` the `noetl_tools` NATS source is built from.
    fn source_config(&self) -> Result<SubscriptionConfig> {
        let mut cfg = serde_json::Map::new();
        cfg.insert("source".into(), serde_json::json!("nats"));
        cfg.insert("url".into(), serde_json::json!(self.nats_url));
        if let Some(u) = &self.nats_user {
            cfg.insert("user".into(), serde_json::json!(u));
        }
        if let Some(p) = &self.nats_password {
            cfg.insert("password".into(), serde_json::json!(p));
        }
        cfg.insert("stream".into(), serde_json::json!(self.stream));
        cfg.insert("consumer".into(), serde_json::json!(self.consumer));
        serde_json::from_value(serde_json::Value::Object(cfg))
            .context("materializer source config invalid")
    }
}

/// Spawn the materializer loop, returning the join handle so the worker can
/// `abort()` it on shutdown.
pub fn spawn(config: MaterializerConfig) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = run_loop(config).await {
            tracing::error!(error = %e, "materializer loop exited with error");
        }
    })
}

/// The drain → project → ack loop. Runs forever; the worker aborts it on
/// shutdown.
async fn run_loop(config: MaterializerConfig) -> Result<()> {
    if config.source_mode.is_ehdb() {
        return run_loop_ehdb(config).await;
    }
    run_loop_nats(config).await
}

/// The EHDB-sourced drain: same project → ack commit point as the NATS loop,
/// over a named durable group on the events feed instead of a JetStream
/// consumer.
///
/// Deliberately a sibling function rather than a generic over the two sources.
/// The loops share their *contract* (drain → project → ack only on 2xx) but not
/// their mechanics — poll options, ack handles and error taxonomy all differ —
/// and threading a trait through would have obscured the one line that actually
/// matters here: the batch is acked ONLY after `events/project` returns 2xx, so
/// a crash mid-batch redelivers rather than losing rows from the durable log.
async fn run_loop_ehdb(config: MaterializerConfig) -> Result<()> {
    let claim_addr = config.event_claim_addr.clone().ok_or_else(|| {
        anyhow!("NOETL_MATERIALIZER_SOURCE=ehdb requires NOETL_EVENT_BUS_CLAIM_ADDR")
    })?;
    let member = member_id(&config.consumer);
    let source = crate::event_bus::EhdbGroupSource::connect(
        claim_addr.clone(),
        config.consumer.clone(),
        crate::event_bus::ALL_EVENTS_FILTER.to_string(),
        member,
        (config.batch as usize).max(1),
    )
    .await?;
    let http = reqwest::Client::new();
    let project_url = format!("{}/api/internal/events/project", config.server_url);

    tracing::info!(
        %claim_addr,
        group = %config.consumer,
        batch = config.batch,
        "CQRS event materializer started on the EHDB events feed (ack-after-materialize)"
    );

    let poll_timeout = Duration::from_millis(config.timeout_ms);
    let mut faults_remaining = config.fault_fail_first;

    loop {
        let cycle_start = Instant::now();
        let drained_batch = source.poll(config.batch as usize, poll_timeout).await;
        let drained = drained_batch.len();
        if drained == 0 {
            if source.is_finished() {
                return Err(anyhow!(
                    "events-feed claim task exited; materializer cannot drain"
                ));
            }
            tokio::time::sleep(config.idle_sleep).await;
            continue;
        }

        let payloads: Vec<serde_json::Value> =
            drained_batch.iter().map(|d| d.payload.clone()).collect();
        let sort_keys: Vec<u64> = drained_batch.iter().map(|d| d.sort_key).collect();
        let (events, skipped) = build_envelopes_from_payloads(&payloads);
        if skipped > 0 {
            tracing::warn!(
                skipped,
                drained,
                "materializer skipped messages with no event_id (not materializable)"
            );
        }

        if faults_remaining > 0 {
            faults_remaining -= 1;
            crate::metrics::record_materializer_project_error();
            tracing::warn!(
                drained,
                faults_remaining,
                "materializer FAULT-INJECT: skipping project + ack; batch will redeliver"
            );
            tokio::time::sleep(config.error_backoff).await;
            continue;
        }

        if events.is_empty() {
            // Nothing materializable — ack to advance the cursor, else the same
            // batch poison-loops forever.
            if let Err(error) = source.ack(&sort_keys).await {
                tracing::warn!(%error, "materializer ack failed on a non-event batch");
            }
            continue;
        }

        match project(&http, &project_url, &config.internal_token, &events).await {
            Ok((projected, duplicates)) => {
                let acked = match source.ack(&sort_keys).await {
                    Ok(n) => n,
                    Err(error) => {
                        // The rows ARE durable; only the ack failed. Those
                        // records redeliver and `events/project` dedupes them by
                        // event_id, so this costs a repeat, never a row.
                        tracing::warn!(%error, "materializer ack failed after a durable project");
                        0
                    }
                };
                tracing::debug!(
                    drained,
                    projected,
                    duplicates,
                    acked,
                    executions = distinct_execution_ids(&events).len(),
                    "materializer cycle (ehdb): drained → projected → acked"
                );
                crate::metrics::record_materializer_cycle(
                    drained as u64,
                    projected as u64,
                    duplicates as u64,
                    acked as u64,
                    cycle_start.elapsed().as_secs_f64(),
                );
            }
            Err(e) => {
                // DO NOT ack — the batch redelivers after ack_wait. No row lost.
                crate::metrics::record_materializer_project_error();
                tracing::warn!(
                    drained,
                    error = %e,
                    "materializer project failed; batch NOT acked, will redeliver"
                );
                tokio::time::sleep(config.error_backoff).await;
            }
        }
    }
}

/// Stable non-zero member id for a group member, derived from the group name.
fn member_id(name: &str) -> u32 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut h);
    (h.finish() as u32) | 1
}

/// The NATS-sourced drain — unchanged.
async fn run_loop_nats(config: MaterializerConfig) -> Result<()> {
    let source = build_source(&config.source_config()?, &ExecutionContext::default())
        .map_err(|e| anyhow!("materializer build_source failed: {e}"))?;
    let http = reqwest::Client::new();
    let project_url = format!("{}/api/internal/events/project", config.server_url);

    tracing::info!(
        stream = %config.stream,
        consumer = %config.consumer,
        batch = config.batch,
        server_url = %config.server_url,
        fault_fail_first = config.fault_fail_first,
        "CQRS event materializer started (ack-after-materialize, deferred ack)"
    );

    // Deferred ack: poll does NOT ack; we ack only after events/project 2xx.
    let opts = PollOptions::new(Some(config.batch), Some(config.timeout_ms), AckMode::Defer);
    let mut faults_remaining = config.fault_fail_first;

    loop {
        let cycle_start = Instant::now();

        let outcome = match source.poll(&opts).await {
            Ok(o) => o,
            Err(e) => {
                tracing::warn!(error = %e, "materializer drain failed; backing off");
                tokio::time::sleep(config.error_backoff).await;
                continue;
            }
        };

        let drained = outcome.messages.len();
        if drained == 0 {
            tokio::time::sleep(config.idle_sleep).await;
            continue;
        }

        let (events, skipped) = build_envelopes(&outcome.messages);
        if skipped > 0 {
            tracing::warn!(
                skipped,
                drained,
                "materializer skipped messages with no event_id (not materializable)"
            );
        }

        // Chaos hook: simulate a transient project failure BEFORE the ack so
        // the redelivery path can be exercised deterministically. The batch is
        // left un-acked → JetStream redelivers after ack-wait.
        if faults_remaining > 0 {
            faults_remaining -= 1;
            crate::metrics::record_materializer_project_error();
            tracing::warn!(
                drained,
                faults_remaining,
                "materializer FAULT-INJECT: skipping project + ack; batch will redeliver"
            );
            tokio::time::sleep(config.error_backoff).await;
            continue;
        }

        if events.is_empty() {
            // Nothing materializable in this batch — ack to advance the cursor
            // (leaving them un-acked would poison-loop forever).
            dispose(
                &*source,
                &outcome.ack_ids,
                AckDisposition::Ack,
                "non-event batch",
            )
            .await;
            continue;
        }

        match project(&http, &project_url, &config.internal_token, &events).await {
            Ok((projected, duplicates)) => {
                // Ack ONLY now that the rows are durable. This is the
                // ack-after-materialize commit point.
                let report = source
                    .ack(&outcome.ack_ids, AckDisposition::Ack)
                    .await
                    .unwrap_or_default();
                if !report.is_clean() {
                    tracing::warn!(
                        errors = ?report.errors,
                        "materializer ack reported per-handle errors"
                    );
                }
                let execution_ids = distinct_execution_ids(&events);
                tracing::debug!(
                    drained,
                    projected,
                    duplicates,
                    acked = report.disposed,
                    executions = execution_ids.len(),
                    "materializer cycle: drained → projected → acked"
                );
                crate::metrics::record_materializer_cycle(
                    drained as u64,
                    projected as u64,
                    duplicates as u64,
                    report.disposed as u64,
                    cycle_start.elapsed().as_secs_f64(),
                );
            }
            Err(e) => {
                // DO NOT ack — the batch stays in-flight and redelivers after
                // the consumer's ack-wait. No event is lost.
                crate::metrics::record_materializer_project_error();
                tracing::warn!(
                    drained,
                    error = %e,
                    "materializer project failed; batch NOT acked, will redeliver"
                );
                tokio::time::sleep(config.error_backoff).await;
            }
        }
    }
}

/// Map drained stream messages to `events/project` envelopes.
///
/// Each `noetl_events` payload is the published `to_jsonb(event_row)` shape;
/// the envelope reads typed fields and flattens the rest. The only transform
/// is `created_at → timestamp` (the envelope's typed time field) so the
/// materialized row keeps its original time. Messages whose payload isn't an
/// object or carries no `event_id` are not materializable and are dropped
/// (counted in the returned `skipped`).
fn build_envelopes(
    messages: &[noetl_tools::tools::source::PolledMessage],
) -> (Vec<serde_json::Value>, usize) {
    let payloads: Vec<serde_json::Value> = messages.iter().map(|m| m.data.clone()).collect();
    build_envelopes_from_payloads(&payloads)
}

/// The transport-independent half of [`build_envelopes`]: map raw payloads to
/// `events/project` envelopes.
///
/// Split out so the EHDB feed drain shares the EXACT same envelope construction
/// as the NATS drain. That identity is what makes the shadow comparison
/// meaningful — if the two transports built envelopes differently, a parity
/// check would be comparing the adapters, not the buses.
fn build_envelopes_from_payloads(
    payloads: &[serde_json::Value],
) -> (Vec<serde_json::Value>, usize) {
    let mut events = Vec::with_capacity(payloads.len());
    let mut skipped = 0usize;
    for data in payloads {
        // `data` may already be a JSON object, or a string holding JSON.
        let obj = match data {
            serde_json::Value::Object(_) => Some(data.clone()),
            serde_json::Value::String(s) => serde_json::from_str::<serde_json::Value>(s).ok(),
            _ => None,
        };
        let Some(serde_json::Value::Object(mut map)) = obj else {
            skipped += 1;
            continue;
        };
        if map.get("event_id").map(|v| v.is_null()).unwrap_or(true) {
            skipped += 1;
            continue;
        }
        if !map.contains_key("timestamp") {
            if let Some(created) = map.remove("created_at") {
                map.insert("timestamp".into(), created);
            }
        }
        events.push(serde_json::Value::Object(map));
    }
    (events, skipped)
}

/// Distinct `execution_id`s in a batch — for the debug correlation line.
fn distinct_execution_ids(events: &[serde_json::Value]) -> Vec<i64> {
    let mut ids: Vec<i64> = events
        .iter()
        .filter_map(|e| e.get("execution_id").and_then(|v| v.as_i64()))
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// POST one batch to `events/project`. Returns `(projected, duplicates)`.
async fn project(
    http: &reqwest::Client,
    url: &str,
    token: &str,
    events: &[serde_json::Value],
) -> Result<(i64, i64)> {
    let resp = http
        .post(url)
        .bearer_auth(token)
        .json(&serde_json::json!({ "events": events }))
        .send()
        .await
        .context("events/project request failed")?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("events/project HTTP {}: {}", status.as_u16(), body));
    }
    let parsed: serde_json::Value = resp.json().await.context("events/project decode")?;
    let projected = parsed
        .get("projected")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let duplicates = parsed
        .get("duplicates")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    Ok((projected, duplicates))
}

/// Dispose a set of handles, logging (not failing the loop) on error.
async fn dispose(
    source: &dyn SourceClient,
    ack_ids: &[String],
    disposition: AckDisposition,
    reason: &str,
) {
    if ack_ids.is_empty() {
        return;
    }
    match source.ack(ack_ids, disposition).await {
        Ok(report) if report.is_clean() => {}
        Ok(report) => {
            tracing::warn!(reason, errors = ?report.errors, "materializer dispose partial")
        }
        Err(e) => tracing::warn!(reason, error = %e, "materializer dispose failed"),
    }
}

/// Parse user/password out of a `nats://user:pass@host` URL, returning the
/// URL with the userinfo stripped (`async_nats::connect` ignores inline creds,
/// so they must be passed explicitly). Env `NATS_USER`/`NATS_PASSWORD` take
/// precedence (matching the worker's command-source convention).
pub(crate) fn parse_nats_credentials(nats_url: &str) -> (String, Option<String>, Option<String>) {
    let env_user = std::env::var("NATS_USER").ok().filter(|s| !s.is_empty());
    let env_pass = std::env::var("NATS_PASSWORD")
        .ok()
        .filter(|s| !s.is_empty());
    if let (Some(u), Some(p)) = (&env_user, &env_pass) {
        let clean = strip_userinfo(nats_url);
        return (clean, Some(u.clone()), Some(p.clone()));
    }
    match url::Url::parse(nats_url) {
        Ok(parsed) if !parsed.username().is_empty() && parsed.password().is_some() => {
            let user = urlencoding::decode(parsed.username())
                .map(|c| c.into_owned())
                .unwrap_or_else(|_| parsed.username().to_string());
            let pass = parsed.password().unwrap_or("");
            let pass = urlencoding::decode(pass)
                .map(|c| c.into_owned())
                .unwrap_or_else(|_| pass.to_string());
            (strip_userinfo(nats_url), Some(user), Some(pass))
        }
        _ => (nats_url.to_string(), None, None),
    }
}

/// Drop the `user:pass@` portion of a NATS URL.
pub(crate) fn strip_userinfo(nats_url: &str) -> String {
    match url::Url::parse(nats_url) {
        Ok(mut u) if !u.username().is_empty() => {
            let _ = u.set_username("");
            let _ = u.set_password(None);
            u.to_string()
        }
        _ => nats_url.to_string(),
    }
}

pub(crate) fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
        .unwrap_or(default)
}

pub(crate) fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use noetl_tools::tools::source::PolledMessage;

    fn msg(data: serde_json::Value) -> PolledMessage {
        PolledMessage {
            id: "1".into(),
            data,
            headers: serde_json::Map::new(),
            attributes: serde_json::Value::Null,
            metadata: serde_json::Value::Null,
            ack_id: Some("$JS.ACK.x".into()),
        }
    }

    #[test]
    fn build_envelopes_maps_created_at_and_drops_invalid() {
        let messages = vec![
            msg(
                serde_json::json!({"event_id": 1, "execution_id": 9, "created_at": "2026-06-18T00:00:00Z"}),
            ),
            // string-encoded JSON payload
            msg(serde_json::Value::String(
                r#"{"event_id": 2, "execution_id": 9, "timestamp": "2026-06-18T00:00:01Z"}"#.into(),
            )),
            // no event_id → dropped
            msg(serde_json::json!({"execution_id": 9})),
            // not an object → dropped
            msg(serde_json::json!(42)),
        ];
        let (events, skipped) = build_envelopes(&messages);
        assert_eq!(events.len(), 2);
        assert_eq!(skipped, 2);
        // created_at renamed to timestamp; no created_at remains.
        assert_eq!(events[0]["timestamp"], "2026-06-18T00:00:00Z");
        assert!(events[0].get("created_at").is_none());
        // existing timestamp preserved.
        assert_eq!(events[1]["timestamp"], "2026-06-18T00:00:01Z");
    }

    #[test]
    fn distinct_execution_ids_dedup_sorted() {
        let events = vec![
            serde_json::json!({"execution_id": 5}),
            serde_json::json!({"execution_id": 3}),
            serde_json::json!({"execution_id": 5}),
            serde_json::json!({"no_exec": true}),
        ];
        assert_eq!(distinct_execution_ids(&events), vec![3, 5]);
    }

    #[test]
    fn strip_userinfo_removes_creds() {
        assert_eq!(
            strip_userinfo("nats://noetl:noetl@host:4222"),
            "nats://host:4222"
        );
        assert_eq!(strip_userinfo("nats://host:4222"), "nats://host:4222");
    }

    #[test]
    fn parse_creds_from_url() {
        // env not set in test → falls back to URL userinfo
        std::env::remove_var("NATS_USER");
        std::env::remove_var("NATS_PASSWORD");
        let (clean, u, p) = parse_nats_credentials("nats://alice:secret@h:4222");
        assert_eq!(clean, "nats://h:4222");
        assert_eq!(u.as_deref(), Some("alice"));
        assert_eq!(p.as_deref(), Some("secret"));
    }

    #[test]
    fn enabled_reads_truthy() {
        std::env::set_var("NOETL_MATERIALIZER_ENABLED", "true");
        assert!(enabled());
        std::env::set_var("NOETL_MATERIALIZER_ENABLED", "0");
        assert!(!enabled());
        std::env::remove_var("NOETL_MATERIALIZER_ENABLED");
        assert!(!enabled());
    }
}

#[cfg(test)]
mod t3_source_tests {
    use super::*;

    /// The load-bearing property for shadow parity: the SAME payload must build
    /// the SAME `events/project` envelope whether it arrived over NATS or the
    /// EHDB feed.  If the two adapters diverged, a parity check would be
    /// comparing adapters rather than buses.
    #[test]
    fn both_transports_build_identical_envelopes() {
        let payload = serde_json::json!({
            "event_id": 42,
            "execution_id": 7,
            "event_type": "action_started",
            "created_at": "2026-07-31T00:00:00Z",
            "status": "STARTED",
        });

        // The NATS adapter path wraps the payload in a PolledMessage; the EHDB
        // adapter passes the payload directly.  Both must land on one envelope.
        let (from_payload, skipped_p) = build_envelopes_from_payloads(&[payload.clone()]);
        assert_eq!(skipped_p, 0);

        // Same input, expressed as the string form NATS sometimes carries.
        let as_string = serde_json::Value::String(payload.to_string());
        let (from_string, skipped_s) = build_envelopes_from_payloads(&[as_string]);
        assert_eq!(skipped_s, 0);

        assert_eq!(
            from_payload, from_string,
            "object and string payloads must produce identical envelopes"
        );
        // created_at is renamed to timestamp exactly once, on both paths.
        assert_eq!(from_payload[0]["timestamp"], "2026-07-31T00:00:00Z");
        assert!(from_payload[0].get("created_at").is_none());
        assert_eq!(from_payload[0]["event_id"], 42);
    }

    /// A payload with no `event_id` is not materializable and must be skipped
    /// rather than posted — identical on both transports.
    #[test]
    fn payloads_without_event_id_are_skipped() {
        let (events, skipped) = build_envelopes_from_payloads(&[
            serde_json::json!({"execution_id": 1}),
            serde_json::json!({"event_id": null, "execution_id": 1}),
            serde_json::Value::String("not json".into()),
            serde_json::json!(5),
        ]);
        assert!(events.is_empty());
        assert_eq!(skipped, 4);
    }

    /// H5 — the inversion of the old rule.
    ///
    /// While NATS existed, falling back to `nats` on an unset or garbage value
    /// was the safe direction: a typo could not move the sole writer of the
    /// durable event log onto an unproven transport.  Since the internal NATS
    /// bus was deleted (noetl/ai-meta#212) that fall-through points a
    /// materializer at a transport that is not there, and the failure is
    /// **silent** — the worker registers, heartbeats, reports ready, and its
    /// group cursor never moves while executions keep completing.
    ///
    /// So unset and unrecognised are now hard errors.  Explicit `nats` still
    /// parses: written out in a manifest it is an auditable operator choice,
    /// not a default nobody chose.
    #[test]
    fn an_unset_or_typod_source_is_a_hard_error() {
        use crate::event_bus::EventSourceMode as M;
        const V: &str = "NOETL_MATERIALIZER_SOURCE";

        // The whole point: unset no longer silently means NATS.
        let unset = M::parse_strict(V, "").unwrap_err().to_string();
        assert!(unset.contains(V), "the error must name the var: {unset}");
        assert!(
            unset.contains("unset"),
            "the error must say what is wrong: {unset}"
        );

        // A one-character typo must not resolve to anything at all.
        let typo = M::parse_strict(V, "ehbd").unwrap_err().to_string();
        assert!(
            typo.contains("ehbd"),
            "the error must echo the offending value: {typo}"
        );
        // Whitespace-only is the same as unset, not a valid value.
        assert!(M::parse_strict(V, "   ").is_err());

        // Valid values still resolve, case- and whitespace-insensitively.
        assert_eq!(M::parse_strict(V, "ehdb").unwrap(), M::Ehdb);
        assert_eq!(M::parse_strict(V, " EHDB ").unwrap(), M::Ehdb);
        assert_eq!(M::parse_strict(V, "nats").unwrap(), M::Nats);
        assert!(M::Ehdb.is_ehdb() && !M::Nats.is_ehdb());
    }

    /// The strict resolver must reach the config builder, so a misconfigured
    /// materializer fails on the startup path rather than inside its spawned
    /// task.  Uses the real env var, hence `serial` semantics via a unique
    /// value set and cleared within the test.
    #[test]
    fn from_env_strict_reads_the_real_var() {
        use crate::event_bus::EventSourceMode as M;
        // A var name nothing else in the test binary touches.
        const V: &str = "NOETL_TEST_H5_SOURCE_PROBE";
        assert!(
            M::from_env_strict(V).is_err(),
            "an unset var must be an error, not a default"
        );
        std::env::set_var(V, "ehdb");
        assert_eq!(M::from_env_strict(V).unwrap(), M::Ehdb);
        std::env::remove_var(V);
    }

    /// Each materializer group gets a distinct, non-zero member id, so the two
    /// system-pool pods compete correctly within a group and the three groups
    /// never collide.
    #[test]
    fn member_ids_are_distinct_and_non_zero() {
        let ids: Vec<u32> = [
            "noetl_materializer",
            "noetl_result_materializer",
            "noetl_state_materializer",
        ]
        .iter()
        .map(|g| member_id(g))
        .collect();
        assert!(ids.iter().all(|i| *i != 0), "member id must be non-zero");
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            3,
            "group member ids must not collide: {ids:?}"
        );
    }
}
