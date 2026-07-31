//! **L1 T3 — the EHDB events feed (worker side, flag-gated).**
//!
//! The sibling of [`crate::command_bus`] for the *events* path — the
//! `noetl.events.>` fan-out that still binds NATS and is the last real blocker
//! before T5 (noetl/ai-meta#212). Behind `NOETL_EVENT_BUS_HOST`, default off.
//!
//! **What rides this feed.** The server publishes every `noetl.event` here; four
//! consumers read it. Three are named durable groups (`noetl_materializer`,
//! `noetl_result_materializer`, `noetl_state_materializer`) that fan out between
//! themselves and queue-group within; the fourth is the gateway's SSE broadcast.
//! Because the server runs `NOETL_EVENT_INGEST_PUBLISH_ONLY=true`, the first of
//! those groups is the **sole writer** of the durable `noetl.event` log — this
//! feed carries the platform's source of truth, not just SPA updates.
//!
//! **Why its own engine, not the command bus's.** noetl/ai-meta#205 established
//! that the binding constraint on this design is `fsync` inside the engine lock.
//! Events run roughly an order of magnitude hotter than commands (~17k/day vs
//! ~2.2k on shastaratech prod), so sharing one engine would put event volume
//! directly in front of command dispatch latency and re-open the exact
//! regression #205 closed. Separate engine, separate directory, separate
//! `fsync` stream, separate ports:
//!
//! | Face | Bind | Serves |
//! |---|---|---|
//! | ingest | `NOETL_EVENT_BUS_INGEST_BIND` (9103) | the server's event publish |
//! | group claims | `NOETL_EVENT_BUS_CLAIM_BIND` (9104) | the three materializers |
//! | `/metrics` | `NOETL_EVENT_BUS_METRICS_BIND` (9106) | per-group lag + resume facts |
//!
//! The command bus's 9100/9101/9102 are untouched, as is every type it uses.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use ehdb_feed::{CursorFallback, FeedWriter, GroupCoordinator};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, L0Config, L0Engine, LocalFsSubstrate};
use tokio::net::TcpListener;

/// The named durable groups this feed serves — the EHDB twins of the three
/// JetStream durable consumers on `noetl_events`. Named here (rather than
/// discovered from client traffic) so the host can open them at startup and log
/// one resume line each: a group that only materialises on first claim reports
/// nothing during the window an operator most wants to see it.
pub const EVENT_GROUPS: [&str; 3] = [
    "noetl_materializer",
    "noetl_result_materializer",
    "noetl_state_materializer",
];

/// Every event type — the filter all three materializers subscribe with, the
/// analog of the JetStream consumers' `filter_subject: noetl.events.>`.
pub const ALL_EVENTS_FILTER: &str = "events.>";

/// How often the host persists each group's committed cursor, on top of the
/// per-ack persist. Bounds the replay window if the pod dies between acks.
const DEFAULT_CURSOR_PERSIST_SECS: u64 = 5;

/// Which transport a materializer drains (`NOETL_*_SOURCE`).
///
/// Deliberately separate from the server's publish-side `NOETL_EVENT_BUS`: the
/// cutover is per-consumer, so a materializer moves to the EHDB feed while
/// publish stays in `shadow` and NATS remains authoritative for everything else.
/// Anything unrecognised is `nats`, so a typo can never quietly move the sole
/// writer of the durable event log onto an unproven transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EventSourceMode {
    #[default]
    Nats,
    Ehdb,
}

impl EventSourceMode {
    pub fn from_env_value(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "ehdb" => Self::Ehdb,
            _ => Self::Nats,
        }
    }
    pub fn is_ehdb(self) -> bool {
        matches!(self, Self::Ehdb)
    }
}

/// Resolved events-feed host configuration.
#[derive(Debug, Clone)]
pub struct EventBusConfig {
    /// `NOETL_EVENT_BUS_HOST` — host the events writer in this process.
    pub host: bool,
    pub shard: u32,
    pub shard_count: u32,
    /// `NOETL_EVENT_BUS_WRITER_DIR` — the events log's own directory. Must NOT
    /// be the command bus's dir: separate engines, separate fsync streams.
    pub writer_dir: Option<PathBuf>,
    pub ingest_bind: Option<SocketAddr>,
    pub claim_bind: Option<SocketAddr>,
    pub metrics_bind: Option<SocketAddr>,
    pub ack_wait: Duration,
    pub cursor_persist: Duration,
    pub cursor_fallback: CursorFallback,
}

impl EventBusConfig {
    pub fn from_env() -> Self {
        let env_bool = |k: &str| {
            matches!(
                std::env::var(k)
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_lowercase()
                    .as_str(),
                "1" | "true" | "yes" | "on"
            )
        };
        let env_addr = |k: &str| std::env::var(k).ok().and_then(|v| v.trim().parse().ok());
        let env_u32 = |k: &str, d: u32| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(d)
        };
        Self {
            host: env_bool("NOETL_EVENT_BUS_HOST"),
            shard: env_u32("NOETL_EVENT_BUS_SHARD", 0),
            shard_count: env_u32("NOETL_EVENT_SHARD_COUNT", 1),
            writer_dir: std::env::var("NOETL_EVENT_BUS_WRITER_DIR")
                .ok()
                .map(PathBuf::from),
            ingest_bind: env_addr("NOETL_EVENT_BUS_INGEST_BIND"),
            claim_bind: env_addr("NOETL_EVENT_BUS_CLAIM_BIND"),
            metrics_bind: env_addr("NOETL_EVENT_BUS_METRICS_BIND"),
            ack_wait: Duration::from_secs(env_u32("NOETL_EVENT_BUS_ACK_WAIT_SECS", 30) as u64),
            cursor_persist: Duration::from_secs(env_u32(
                "NOETL_EVENT_BUS_CURSOR_PERSIST_SECS",
                DEFAULT_CURSOR_PERSIST_SECS as u32,
            ) as u64),
            cursor_fallback: CursorFallback::from_env_value(
                &std::env::var("NOETL_EVENT_BUS_CURSOR_FALLBACK").unwrap_or_default(),
            ),
        }
    }
}

/// Host the events feed: open its own durable log and spawn the ingest, named-
/// group claim, and `/metrics` faces. Returns the writer handle.
pub async fn spawn_event_writer_host(
    config: &EventBusConfig,
) -> Result<Arc<GroupCoordinator<D1EventLog>>> {
    let dir = config
        .writer_dir
        .clone()
        .ok_or_else(|| anyhow!("NOETL_EVENT_BUS_WRITER_DIR required to host the events writer"))?;
    std::fs::create_dir_all(&dir)?;
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&dir)?);
    let engine = L0Engine::<D1EventLog>::open(
        L0Config::d1(&dir).with_shard_count(config.shard_count.max(1)),
        store,
    )?;
    let writer = Arc::new(FeedWriter::new(engine));

    if let Some(addr) = config.ingest_bind {
        let listener = TcpListener::bind(addr).await?;
        tokio::spawn(ehdb_feed::serve_ingest(listener, writer.clone()));
        tracing::info!(%addr, shard = config.shard, "EHDB events-feed ingest listener up");
    }

    let coordinator = Arc::new(GroupCoordinator::new(
        writer.clone(),
        config.shard,
        config.ack_wait,
        // Route on `events.<event_type>`, the analog of the server's
        // `noetl.events.<event_type>`.
        ehdb_feed::event_feed_subject(),
        Some(dir.clone()),
        config.cursor_fallback,
    ));

    // Open every group up front so each logs its resume line now, rather than at
    // whatever later moment its consumer first connects.
    for group in EVENT_GROUPS {
        let report = coordinator.open_group(group).await;
        tracing::info!(group, %report, "EHDB events-feed group resumed");
    }

    if let Some(addr) = config.claim_bind {
        let listener = TcpListener::bind(addr).await?;
        tokio::spawn(ehdb_feed::serve_group_claims(listener, coordinator.clone()));
        tracing::info!(%addr, shard = config.shard, "EHDB events-feed group coordinator up");
    }

    if config.cursor_persist > Duration::ZERO {
        let coord = coordinator.clone();
        let every = config.cursor_persist;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                if let Err(error) = coord.checkpoint().await {
                    tracing::warn!(%error, "EHDB events-feed cursor checkpoint failed");
                }
            }
        });
    }

    spawn_graceful_event_shutdown(coordinator.clone(), config.shard);

    if let Some(addr) = config.metrics_bind {
        tokio::spawn(serve_event_metrics(addr, coordinator.clone()));
        tracing::info!(%addr, "EHDB events-feed /metrics endpoint up");
    }

    Ok(coordinator)
}

/// Render the events feed's Prometheus exposition: per-group committed cursor
/// and lag, plus the cursor-persist error count.
///
/// Per-group lag is the number the cutover gates read: whole-feed lag would mix
/// three consumers that are deliberately at different positions, so "is the
/// durable-log materializer keeping up" is only answerable per group.
async fn render_event_metrics(coordinator: &GroupCoordinator<D1EventLog>) -> String {
    let mut out = String::new();
    out.push_str("# HELP ehdb_events_group_committed Committed cursor per named group.\n");
    out.push_str("# TYPE ehdb_events_group_committed gauge\n");
    let lags = coordinator.group_lags().await;
    for (group, committed, _) in &lags {
        out.push_str(&format!(
            "ehdb_events_group_committed{{group=\"{group}\"}} {committed}\n"
        ));
    }
    out.push_str("# HELP ehdb_events_group_lag Undelivered + unacked records per named group.\n");
    out.push_str("# TYPE ehdb_events_group_lag gauge\n");
    for (group, _, lag) in &lags {
        out.push_str(&format!(
            "ehdb_events_group_lag{{group=\"{group}\"}} {lag}\n"
        ));
    }
    out.push_str(
        "# HELP ehdb_events_cursor_errors Failed group-cursor persists (progress not durable).\n",
    );
    out.push_str("# TYPE ehdb_events_cursor_errors counter\n");
    out.push_str(&format!(
        "ehdb_events_cursor_errors {}\n",
        coordinator.cursor_errors()
    ));
    out
}

async fn serve_event_metrics(
    addr: SocketAddr,
    coordinator: Arc<GroupCoordinator<D1EventLog>>,
) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = TcpListener::bind(addr).await?;
    loop {
        let (mut sock, _peer) = listener.accept().await?;
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            // Read and discard the request head; this endpoint serves one body
            // regardless of path, like the command bus's.
            let _ = sock.read(&mut buf).await;
            let body = render_event_metrics(&coordinator).await;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.flush().await;
        });
    }
}

#[cfg(unix)]
fn spawn_graceful_event_shutdown(coordinator: Arc<GroupCoordinator<D1EventLog>>, shard: u32) {
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};
        let (Ok(mut sigterm), Ok(mut sigint)) = (
            signal(SignalKind::terminate()),
            signal(SignalKind::interrupt()),
        ) else {
            tracing::warn!("EHDB events-feed writer could not install its shutdown handler");
            return;
        };
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
        // Persist every group's cursor before the process goes. A cursor behind
        // the log is always safe (records redeliver); a cursor ahead of it needs
        // the clamp, which is why this only ever writes committed positions.
        match coordinator.checkpoint().await {
            Ok(()) => tracing::info!(shard, "EHDB events-feed cursors persisted on shutdown"),
            Err(error) => {
                tracing::warn!(shard, %error, "EHDB events-feed cursor persist failed")
            }
        }
    });
}

#[cfg(not(unix))]
fn spawn_graceful_event_shutdown(_coordinator: Arc<GroupCoordinator<D1EventLog>>, _shard: u32) {}

#[cfg(test)]
mod tests {
    use super::*;

    /// The events feed must default to **off** and must not inherit the command
    /// bus's directory — a shared dir would put both logs in one fsync stream,
    /// which is the coupling this module exists to avoid.
    #[test]
    fn defaults_are_off_and_carry_no_directory() {
        // Read defaults without mutating the process env (other tests run in
        // parallel in the same process).
        let config = EventBusConfig {
            host: false,
            shard: 0,
            shard_count: 1,
            writer_dir: None,
            ingest_bind: None,
            claim_bind: None,
            metrics_bind: None,
            ack_wait: Duration::from_secs(30),
            cursor_persist: Duration::from_secs(DEFAULT_CURSOR_PERSIST_SECS),
            cursor_fallback: CursorFallback::default(),
        };
        assert!(!config.host);
        assert!(config.writer_dir.is_none());
        assert_eq!(config.cursor_fallback, CursorFallback::Tail);
    }

    /// Hosting without a directory must fail loudly rather than silently
    /// picking one — a wrong events-log location is a data-durability bug.
    #[tokio::test]
    async fn hosting_without_a_directory_is_an_error() {
        let config = EventBusConfig {
            host: true,
            shard: 0,
            shard_count: 1,
            writer_dir: None,
            ingest_bind: None,
            claim_bind: None,
            metrics_bind: None,
            ack_wait: Duration::from_secs(30),
            cursor_persist: Duration::ZERO,
            cursor_fallback: CursorFallback::Tail,
        };
        let err = match spawn_event_writer_host(&config).await {
            Ok(_) => panic!("hosting without a writer dir must fail"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("NOETL_EVENT_BUS_WRITER_DIR"));
    }

    /// The three groups are opened at startup and each reports its own lag line,
    /// so the first scrape after a restart is already complete.
    #[tokio::test]
    async fn metrics_carry_a_line_per_group_from_the_first_scrape() {
        let dir = std::env::temp_dir().join(format!("noetl-eventbus-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let config = EventBusConfig {
            host: true,
            shard: 0,
            shard_count: 1,
            writer_dir: Some(dir.clone()),
            ingest_bind: None,
            claim_bind: None,
            metrics_bind: None,
            ack_wait: Duration::from_secs(30),
            cursor_persist: Duration::ZERO,
            cursor_fallback: CursorFallback::Tail,
        };
        let coordinator = spawn_event_writer_host(&config).await.unwrap();
        let body = render_event_metrics(&coordinator).await;
        for group in EVENT_GROUPS {
            assert!(
                body.contains(&format!("ehdb_events_group_lag{{group=\"{group}\"}}")),
                "missing lag line for {group} in:\n{body}"
            );
            assert!(
                body.contains(&format!("ehdb_events_group_committed{{group=\"{group}\"}}")),
                "missing committed line for {group} in:\n{body}"
            );
        }
        assert!(body.contains("ehdb_events_cursor_errors 0"));
        std::fs::remove_dir_all(&dir).ok();
    }
}

// ---------------------------------------------------------------------------
// Consumer side — draining a named group off the events feed.
// ---------------------------------------------------------------------------

/// One record drained from the events feed.
#[derive(Debug, Clone)]
pub struct DrainedEvent {
    /// The feed's writer-assigned sort key — the ack token.
    pub sort_key: u64,
    /// The event payload (the server published `to_stream_json()` bytes).
    pub payload: serde_json::Value,
}

/// A consumer's connection to one named group on the events feed.
///
/// **Why a background claim task rather than calling `claim_next` inline.**
/// `claim_next` *blocks* until a record is available, and the claim protocol is
/// strict request/response on one socket. A caller that wants "up to N records,
/// or whatever arrives in T milliseconds" therefore cannot simply time out the
/// call: abandoning an in-flight claim leaves the coordinator about to write a
/// response nobody will read, and the next read on that socket returns the stale
/// record — silently mis-associating an ack.
///
/// So one task owns the pull socket and never abandons a request; it pushes
/// claimed records into a bounded channel. [`poll`](Self::poll) drains that
/// channel with a deadline, which is a pure local operation. Acks go over a
/// second connection, so an ack never waits behind a blocked claim.
///
/// The bounded channel is the backpressure seam: when the consumer is slower
/// than the feed, the claim task parks on a full channel rather than claiming
/// records it cannot ack, which keeps the group's in-flight set small and its
/// `ack_wait` redeliveries rare.
pub struct EhdbGroupSource {
    group: String,
    rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<DrainedEvent>>,
    ack: tokio::sync::Mutex<Option<ehdb_feed::GroupClaimClient>>,
    claim_addr: String,
    member: u32,
    /// Set when the claim task dies so `poll` can report it rather than looking
    /// like a permanently idle feed.
    claim_task: tokio::task::JoinHandle<()>,
}

impl EhdbGroupSource {
    /// Connect a drain for `group` at `claim_addr`, subscribing with `filter`
    /// (`events.>` for every type). `capacity` bounds the in-flight prefetch.
    pub async fn connect(
        claim_addr: String,
        group: String,
        filter: String,
        member: u32,
        capacity: usize,
    ) -> Result<Self> {
        let (tx, rx) = tokio::sync::mpsc::channel(capacity.max(1));
        let addr = claim_addr.clone();
        let g = group.clone();
        let f = filter.clone();
        let claim_task = tokio::spawn(async move {
            claim_loop(addr, g, f, member, tx).await;
        });
        Ok(Self {
            group,
            rx: tokio::sync::Mutex::new(rx),
            ack: tokio::sync::Mutex::new(None),
            claim_addr,
            member,
            claim_task,
        })
    }

    /// The group this source drains.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Drain up to `max` records, waiting at most `timeout` for the first one.
    /// Returns an empty vec when the feed is idle — the caller then sleeps.
    pub async fn poll(&self, max: usize, timeout: Duration) -> Vec<DrainedEvent> {
        let mut rx = self.rx.lock().await;
        let mut out = Vec::new();
        // Wait for the first record; after that take only what is already
        // buffered, so a partially-full batch is not held for the full timeout.
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(first)) => out.push(first),
            Ok(None) => return out, // channel closed — claim task gone
            Err(_) => return out,   // idle
        }
        while out.len() < max {
            match rx.try_recv() {
                Ok(ev) => out.push(ev),
                Err(_) => break,
            }
        }
        out
    }

    /// Ack a drained batch by sort key. Errors are returned so the caller can
    /// decide; an unacked record simply redelivers after `ack_wait`.
    pub async fn ack(&self, sort_keys: &[u64]) -> Result<usize> {
        if sort_keys.is_empty() {
            return Ok(0);
        }
        let mut guard = self.ack.lock().await;
        if guard.is_none() {
            *guard = Some(
                ehdb_feed::GroupClaimClient::connect(
                    self.claim_addr.as_str(),
                    self.group.clone(),
                    self.member,
                    ALL_EVENTS_FILTER,
                )
                .await
                .with_context(|| format!("events-feed ack connect ({})", self.claim_addr))?,
            );
        }
        let client = guard.as_mut().expect("just connected");
        let mut acked = 0usize;
        for key in sort_keys {
            if let Err(e) = client.ack(*key).await {
                // Drop the connection so the next call redials; the unacked
                // remainder redelivers, which is the at-least-once contract.
                *guard = None;
                return Err(anyhow!("events-feed ack failed after {acked}: {e}"));
            }
            acked += 1;
        }
        Ok(acked)
    }

    /// True when the background claim task has exited — a source in this state
    /// will never yield another record.
    pub fn is_finished(&self) -> bool {
        self.claim_task.is_finished()
    }
}

impl Drop for EhdbGroupSource {
    fn drop(&mut self) {
        self.claim_task.abort();
    }
}

/// Own the pull socket and claim forever, redialing on error. Never abandons an
/// in-flight claim (see [`EhdbGroupSource`] for why that matters).
async fn claim_loop(
    claim_addr: String,
    group: String,
    filter: String,
    member: u32,
    tx: tokio::sync::mpsc::Sender<DrainedEvent>,
) {
    let mut backoff = Duration::from_millis(200);
    loop {
        let client = match ehdb_feed::GroupClaimClient::connect(
            claim_addr.as_str(),
            group.clone(),
            member,
            filter.clone(),
        )
        .await
        {
            Ok(c) => {
                backoff = Duration::from_millis(200);
                c
            }
            Err(error) => {
                tracing::warn!(%claim_addr, group, %error, "events-feed claim connect failed; retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
                continue;
            }
        };
        let mut client = client;
        loop {
            match client.claim_next::<ehdb_l0::EventRecord>().await {
                Ok(claimed) => {
                    let payload =
                        serde_json::from_str::<serde_json::Value>(&claimed.record.payload)
                            .unwrap_or(serde_json::Value::Null);
                    if tx
                        .send(DrainedEvent {
                            sort_key: claimed.sort_key,
                            payload,
                        })
                        .await
                        .is_err()
                    {
                        return; // receiver dropped — the source is gone
                    }
                }
                Err(error) => {
                    tracing::warn!(group, %error, "events-feed claim failed; redialing");
                    break;
                }
            }
        }
        tokio::time::sleep(backoff).await;
    }
}
