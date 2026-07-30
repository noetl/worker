//! **L1 T4 — the EHDB command bus (worker side, flag-gated).**
//!
//! Behind `NOETL_COMMAND_BUS` (default `nats`, unchanged). Two responsibilities,
//! both opt-in:
//!
//! - **Host** (the system-pool worker that owns a shard, `NOETL_COMMAND_BUS_HOST`):
//!   opens the shard's durable command-log `FeedWriter` and spawns its three
//!   faces — `serve_ingest` (the server publishes commands here), `serve_claims`
//!   (worker replicas compete for commands here, path A), and a Prometheus
//!   `/metrics` lag endpoint (the KEDA signal).
//! - **Consume** (`ehdb` mode): the command source claims via the **network**
//!   `claim_next`/`ack`/`nack` against its shard's coordinator — a shared,
//!   competing consumer across replicas (NOT a local in-process group), so each
//!   command goes to exactly one worker. Reuses the shared [`claim_outcome`] path.
//!
//! Consuming uses **two** claim connections: one for the blocking `claim_next`
//! pull (`&mut self` in `next`), one behind a mutex for `ack`/`nack` (`&self`),
//! since ack is by global sort key against the shared coordinator and must not
//! stall a blocked pull. Lazy-connected + drop-on-error redial, so a worker never
//! hard-depends on the host being up at boot.
//!
//! **Surviving a writer restart (noetl/ai-meta#208).** Restarting only the writer
//! pod used to stop dispatch outright. Two changes here close it:
//!
//! - The claim connection can now *notice*: `ehdb-feed` arms TCP keepalive and a
//!   coordinator heartbeat, so a vanished writer surfaces as an `Err` and the redial
//!   below actually runs. Previously the read parked forever on a half-open socket
//!   and no error was ever logged.
//! - The hosted coordinator **resumes from its committed cursor** (persisted on the
//!   writer's volume) instead of replaying the shard from 0, and seals the log on
//!   SIGTERM so the reopened engine recovers the tail.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use ehdb_feed::scaler::ShardLag;
use ehdb_feed::{ClaimClient, ClaimCoordinator, CursorFallback, CursorStore, FeedWriter};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate};
use noetl_executor::worker::source::{CommandSource, Pulled};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::client::ControlPlaneClient;
use crate::nats::{claim_outcome, CommandNotification, NatsAckHandle, NatsCommandSource};

/// Which transport carries commands (`NOETL_COMMAND_BUS`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CommandBusMode {
    #[default]
    Nats,
    Ehdb,
    Shadow,
}

impl CommandBusMode {
    pub fn from_env_value(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "ehdb" => Self::Ehdb,
            "shadow" => Self::Shadow,
            _ => Self::Nats,
        }
    }
    /// The worker consumes from the EHDB bus (only in pure `ehdb`; in `shadow`
    /// NATS stays authoritative and the worker keeps consuming NATS).
    pub fn consumes_ehdb(self) -> bool {
        matches!(self, Self::Ehdb)
    }
    /// The EHDB writer should exist (to receive the server's publishes) — `ehdb`
    /// or `shadow`.
    pub fn hosts_relevant(self) -> bool {
        matches!(self, Self::Ehdb | Self::Shadow)
    }
}

/// Worker command-bus configuration (env `NOETL_COMMAND_BUS_*`).
#[derive(Debug, Clone)]
pub struct CommandBusConfig {
    pub mode: CommandBusMode,
    pub host: bool,
    pub shard: u32,
    pub shard_count: u32,
    pub writer_dir: Option<PathBuf>,
    pub ingest_bind: Option<SocketAddr>,
    pub claim_bind: Option<SocketAddr>,
    pub metrics_bind: Option<SocketAddr>,
    /// The claim coordinator's address as a `host:port` string — a DNS service
    /// name (resolved at connect time) or `ip:port`. Not a parsed `SocketAddr`,
    /// so a Kubernetes service name works directly (finding #2, noetl/ai-meta#194).
    pub claim_addr: Option<String>,
    pub ack_wait: Duration,
    /// How often the hosted coordinator persists its committed cursor to the
    /// writer's volume (`NOETL_COMMAND_BUS_CURSOR_PERSIST_MS`, default 1000).
    /// `0` disables the ticker; the shutdown persist still runs.
    pub cursor_persist: Duration,
    /// Where a restarted coordinator starts when nothing has been persisted yet
    /// (`NOETL_COMMAND_BUS_CURSOR_FALLBACK`, default `tail`).
    pub cursor_fallback: CursorFallback,
}

impl CommandBusConfig {
    pub fn from_env() -> Self {
        let mode =
            CommandBusMode::from_env_value(&std::env::var("NOETL_COMMAND_BUS").unwrap_or_default());
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
            mode,
            host: env_bool("NOETL_COMMAND_BUS_HOST"),
            shard: env_u32("NOETL_COMMAND_BUS_SHARD", 0),
            shard_count: env_u32("NOETL_COMMAND_SHARD_COUNT", 1),
            writer_dir: std::env::var("NOETL_COMMAND_BUS_WRITER_DIR")
                .ok()
                .map(PathBuf::from),
            ingest_bind: env_addr("NOETL_COMMAND_BUS_INGEST_BIND"),
            claim_bind: env_addr("NOETL_COMMAND_BUS_CLAIM_BIND"),
            metrics_bind: env_addr("NOETL_COMMAND_BUS_METRICS_BIND"),
            claim_addr: std::env::var("NOETL_COMMAND_BUS_CLAIM_ADDR")
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|s| !s.is_empty()),
            ack_wait: Duration::from_secs(env_u32("NOETL_COMMAND_BUS_ACK_WAIT_SECS", 30) as u64),
            cursor_persist: Duration::from_millis(env_u32(
                "NOETL_COMMAND_BUS_CURSOR_PERSIST_MS",
                1_000,
            ) as u64),
            cursor_fallback: CursorFallback::from_env_value(
                &std::env::var("NOETL_COMMAND_BUS_CURSOR_FALLBACK").unwrap_or_default(),
            ),
        }
    }
}

/// Host the shard's writer: open the durable command-log engine and spawn its
/// ingest (publish-in), claim (compete-out), and `/metrics` (lag) faces. Returns
/// the writer handle. Idempotent per process; call once when `config.host`.
pub async fn spawn_writer_host(config: &CommandBusConfig) -> Result<Arc<FeedWriter<D1EventLog>>> {
    let dir = config
        .writer_dir
        .clone()
        .ok_or_else(|| anyhow!("NOETL_COMMAND_BUS_WRITER_DIR required to host the writer"))?;
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
        tracing::info!(%addr, shard = config.shard, "EHDB command-bus ingest listener up");
    }

    // **Resume, don't replay** (noetl/ai-meta#208 defect 2). This used to pass
    // `from_cursor = 0`, so every restart re-served the shard's whole log —
    // 2738 long-completed commands in kind, each costing a control-plane
    // round-trip to learn it was already claimed, with fresh commands queued
    // behind them. The committed cursor now lives on the writer's own volume
    // beside the log and the coordinator picks up where the last one left off.
    let coordinator = Arc::new(ClaimCoordinator::resume(
        writer.clone(),
        config.shard,
        config.ack_wait,
        // Derive each command's routing subject (`commands.<pool>.shard.<n>`)
        // from the notification — so a member claims only within its subscribed
        // subjects (pool + shard isolation, noetl/ai-meta#194 finding #1, the
        // general NATS-subject mechanism).
        ehdb_feed::d1_command_subject(config.shard_count),
        CursorStore::open(&dir, config.shard)?,
        config.cursor_fallback,
    )?);
    let (from_cursor, origin) = coordinator.started_from();
    tracing::info!(
        shard = config.shard,
        from_cursor,
        origin = origin.as_str(),
        "EHDB command-bus claim coordinator resumed"
    );
    if config.cursor_persist > Duration::ZERO {
        coordinator
            .clone()
            .spawn_cursor_persister(config.cursor_persist);
    }
    spawn_graceful_writer_shutdown(writer.clone(), coordinator.clone(), config.shard);

    if let Some(addr) = config.claim_bind {
        let listener = TcpListener::bind(addr).await?;
        tokio::spawn(ehdb_feed::serve_claims(listener, coordinator.clone()));
        tracing::info!(%addr, shard = config.shard, "EHDB command-bus claim coordinator up");
    }

    if let Some(addr) = config.metrics_bind {
        // The scaler provider is sync; publish the async lag into an atomic that a
        // background sampler refreshes.
        let gauge = Arc::new(AtomicU64::new(0));
        // The committed cursor rides the same sampler: it is the value a restart
        // resumes from, so an operator watching a restart can see it advance
        // (noetl/ai-meta#208) instead of the hardcoded 0 reported before.
        let committed = Arc::new(AtomicU64::new(0));
        let sampler = gauge.clone();
        let committed_sampler = committed.clone();
        let coord = coordinator.clone();
        tokio::spawn(async move {
            loop {
                sampler.store(coord.lag().await, Ordering::Relaxed);
                committed_sampler.store(coord.committed_cursor().await, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
        let shard = config.shard;
        let read = gauge.clone();
        tokio::spawn(ehdb_feed::scaler::bind_and_serve(addr, move || {
            vec![ShardLag {
                shard,
                committed: committed.load(Ordering::Relaxed),
                lag: read.load(Ordering::Relaxed),
            }]
        }));
        tracing::info!(%addr, "EHDB command-bus /metrics lag endpoint up");
    }

    Ok(writer)
}

/// **Seal the command log and persist the cursor on SIGTERM** — the other half of
/// surviving a writer restart (noetl/ai-meta#208).
///
/// The L0 engine reopens from its **durable manifest**, so records still sitting in
/// an unsealed active part are invisible to the restarted process even though every
/// append `fsync`ed them. A rollout restart would therefore drop the tail of the
/// command log (up to `seal_max_records`, 1024 by default) *and* leave the
/// persisted cursor up to one persist-interval stale. Sealing + flushing uploads on
/// the termination signal closes both: the reopened writer recovers everything, and
/// the coordinator resumes from an exact cursor.
///
/// Best-effort by nature — this races the process exit, and a SIGKILL / OOM skips it
/// entirely. That crash path is why the resume cursor is clamped to the reopened
/// log's tip (`ClaimCoordinator::resume`): worst case the bus redelivers, it never
/// goes dark. Commands genuinely lost with an unsealed part are re-issued by the
/// control plane's orphaned-command guardrail (noetl/ai-meta#171).
#[cfg(unix)]
fn spawn_graceful_writer_shutdown(
    writer: Arc<FeedWriter<D1EventLog>>,
    coordinator: Arc<ClaimCoordinator<D1EventLog>>,
    shard: u32,
) {
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};
        let (Ok(mut sigterm), Ok(mut sigint)) = (
            signal(SignalKind::terminate()),
            signal(SignalKind::interrupt()),
        ) else {
            tracing::warn!("EHDB command-bus writer could not install its shutdown handler");
            return;
        };
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
        // Cursor first: it is cheap, and a cursor that is behind the log is always
        // safe while a log that is behind the cursor needs the clamp.
        match coordinator.persist_cursor().await {
            Ok(Some(cursor)) => {
                tracing::info!(
                    shard,
                    cursor,
                    "EHDB command-bus cursor persisted on shutdown"
                )
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(shard, error = %e, "EHDB command-bus cursor persist failed"),
        }
        // `flush_and_wait_uploads` blocks on the uploader, so keep it off the async
        // worker threads.
        let sealed = tokio::task::spawn_blocking(move || {
            writer.engine().lock().unwrap().flush_and_wait_uploads()
        })
        .await;
        match sealed {
            Ok(Ok(())) => tracing::info!(shard, "EHDB command-bus log sealed on shutdown"),
            Ok(Err(e)) => tracing::warn!(shard, error = %e, "EHDB command-bus seal failed"),
            Err(e) => tracing::warn!(shard, error = %e, "EHDB command-bus seal task failed"),
        }
    });
}

#[cfg(not(unix))]
fn spawn_graceful_writer_shutdown(
    _writer: Arc<FeedWriter<D1EventLog>>,
    _coordinator: Arc<ClaimCoordinator<D1EventLog>>,
    _shard: u32,
) {
}

fn member_id(worker_id: &str) -> u32 {
    let mut h = DefaultHasher::new();
    worker_id.hash(&mut h);
    (h.finish() as u32) | 1 // non-zero
}

/// The EHDB command source: claims commands over the network from its shard's
/// coordinator (competing with the pool's other replicas), then runs the shared
/// claim → `ClaimOutcome` path. The ack handle carries the global sort key.
pub struct EhdbCommandSource {
    claim_addr: String,
    /// This worker's subject subscription filter (e.g. `commands.shared.>` for
    /// the shared pool on any shard), derived from its `NATS_FILTER_SUBJECT`
    /// segment. The coordinator only ever hands it a command whose subject
    /// matches — strict pool + shard isolation (noetl/ai-meta#194 finding #1).
    filter: String,
    member: u32,
    worker_id: String,
    client: ControlPlaneClient,
    pull: Option<ClaimClient>,
    ack_conn: Mutex<Option<ClaimClient>>,
}

/// EHDB ack handle: the claimed command's global sort key + the notification
/// metadata (`execution_id` / `command_id` / … for WARN/ERROR correlation, per
/// `observability.md` Principle 4 — the EHDB twin of [`NatsAckHandle`]).
#[derive(Debug, Clone)]
pub struct EhdbAckHandle {
    pub sort_key: u64,
    pub notification: CommandNotification,
}

impl EhdbCommandSource {
    pub fn new(
        claim_addr: String,
        filter: String,
        worker_id: String,
        client: ControlPlaneClient,
    ) -> Self {
        let member = member_id(&worker_id);
        Self {
            claim_addr,
            filter,
            member,
            worker_id,
            client,
            pull: None,
            ack_conn: Mutex::new(None),
        }
    }

    async fn ack_client(&self) -> Result<tokio::sync::MutexGuard<'_, Option<ClaimClient>>> {
        let mut guard = self.ack_conn.lock().await;
        if guard.is_none() {
            *guard = Some(
                ClaimClient::connect(&self.claim_addr, self.member, self.filter.clone()).await?,
            );
        }
        Ok(guard)
    }

    /// Ack a claimed command by its global sort key (the wrapper's ack path).
    pub async fn ack_sort_key(&self, sort_key: u64) -> Result<()> {
        let mut guard = self.ack_client().await?;
        match guard.as_mut().unwrap().ack(sort_key).await {
            Ok(()) => Ok(()),
            Err(e) => {
                *guard = None; // redial next time
                Err(anyhow!("EHDB ack failed: {e}"))
            }
        }
    }

    /// Nack a claimed command by its global sort key (redeliver after ack_wait).
    pub async fn nack_sort_key(&self, sort_key: u64) -> Result<()> {
        let mut guard = self.ack_client().await?;
        match guard.as_mut().unwrap().nack(sort_key).await {
            Ok(()) => Ok(()),
            Err(e) => {
                *guard = None;
                Err(anyhow!("EHDB nack failed: {e}"))
            }
        }
    }
}

#[async_trait]
impl CommandSource for EhdbCommandSource {
    type AckHandle = EhdbAckHandle;

    async fn next(&mut self) -> Result<Option<Pulled<Self::AckHandle>>> {
        loop {
            if self.pull.is_none() {
                match ClaimClient::connect(&self.claim_addr, self.member, self.filter.clone()).await
                {
                    Ok(c) => self.pull = Some(c),
                    Err(e) => {
                        tracing::warn!(claim_addr = %self.claim_addr, error = %e, "EHDB claim connect failed; retrying");
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        continue;
                    }
                }
            }
            let claimed = match self
                .pull
                .as_mut()
                .unwrap()
                .claim_next::<EventRecord>()
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    // Reachable at all only because the claim connection can now
                    // detect a dead coordinator (keepalive + heartbeat,
                    // noetl/ai-meta#208 defect 1). Before that a restarted writer
                    // left this read parked forever, so dispatch stopped with
                    // nothing logged anywhere.
                    tracing::warn!(
                        claim_addr = %self.claim_addr,
                        member = self.member,
                        filter = %self.filter,
                        error = %e,
                        "EHDB claim_next failed; reconnecting to the claim coordinator"
                    );
                    self.pull = None;
                    continue;
                }
            };
            let notification: CommandNotification =
                serde_json::from_str(&claimed.record.payload)
                    .map_err(|e| anyhow!("EHDB command notification decode: {e}"))?;
            let outcome = claim_outcome(&self.client, &self.worker_id, &notification).await?;
            return Ok(Some(Pulled {
                outcome,
                ack: EhdbAckHandle {
                    sort_key: claimed.sort_key,
                    notification,
                },
            }));
        }
    }

    async fn ack(&self, handle: Self::AckHandle) -> Result<()> {
        self.ack_sort_key(handle.sort_key).await
    }

    async fn nack(&self, handle: Self::AckHandle) -> Result<()> {
        self.nack_sort_key(handle.sort_key).await
    }
}

/// The worker's active command source — NATS (default) or the EHDB bus. Keeps
/// `Worker` non-generic while dispatching the trait + the NATS-only reconnect.
pub enum WorkerCommandSource {
    // Both boxed so neither large source inflates the enum (large_enum_variant).
    Nats(Box<NatsCommandSource>),
    Ehdb(Box<EhdbCommandSource>),
}

/// The ack mechanism for a claimed command (kept separate from the notification
/// metadata so `Worker::process_commands` reads `handle.notification` uniformly).
enum WorkerAckInner {
    /// The NATS message handle (its `ack`/`nack` go through JetStream). Boxed:
    /// the message dwarfs the EHDB sort key.
    Nats(Box<NatsAckHandle>),
    /// The EHDB claim's global sort key (ack/nack go through the coordinator).
    Ehdb(u64),
}

/// Ack handle for either source. `notification` is exposed uniformly (both
/// sources carry it) so the correlation call sites don't branch on transport.
pub struct WorkerAckHandle {
    pub notification: CommandNotification,
    inner: WorkerAckInner,
}

impl WorkerCommandSource {
    /// Reconnect the NATS subscriber in-process (noetl/ai-meta#163 self-heal).
    /// A no-op for the EHDB source (its claim client redials on error).
    pub fn replace_subscriber(&mut self, subscriber: crate::nats::NatsSubscriber) {
        if let Self::Nats(s) = self {
            s.replace_subscriber(subscriber);
        }
    }

    /// The NATS subscriber, when this is the NATS source — `None` on the EHDB
    /// bus (whose lag is exported by the writer `/metrics`, not a NATS consumer).
    pub fn nats_subscriber(&self) -> Option<&crate::nats::NatsSubscriber> {
        match self {
            Self::Nats(s) => Some(s.subscriber()),
            Self::Ehdb(_) => None,
        }
    }
}

#[async_trait]
impl CommandSource for WorkerCommandSource {
    type AckHandle = WorkerAckHandle;

    async fn next(&mut self) -> Result<Option<Pulled<Self::AckHandle>>> {
        match self {
            Self::Nats(s) => Ok(s.next().await?.map(|p| Pulled {
                outcome: p.outcome,
                ack: WorkerAckHandle {
                    notification: p.ack.notification.clone(),
                    inner: WorkerAckInner::Nats(Box::new(p.ack)),
                },
            })),
            Self::Ehdb(s) => Ok(s.next().await?.map(|p| Pulled {
                outcome: p.outcome,
                ack: WorkerAckHandle {
                    notification: p.ack.notification.clone(),
                    inner: WorkerAckInner::Ehdb(p.ack.sort_key),
                },
            })),
        }
    }

    async fn ack(&self, handle: Self::AckHandle) -> Result<()> {
        match (self, handle.inner) {
            (Self::Nats(s), WorkerAckInner::Nats(h)) => s.ack(*h).await,
            (Self::Ehdb(s), WorkerAckInner::Ehdb(sort_key)) => s.ack_sort_key(sort_key).await,
            _ => Err(anyhow!("command-source / ack-handle mismatch")),
        }
    }

    async fn nack(&self, handle: Self::AckHandle) -> Result<()> {
        match (self, handle.inner) {
            (Self::Nats(s), WorkerAckInner::Nats(h)) => s.nack(*h).await,
            (Self::Ehdb(s), WorkerAckInner::Ehdb(sort_key)) => s.nack_sort_key(sort_key).await,
            _ => Err(anyhow!("command-source / ack-handle mismatch")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_parsing_defaults_to_nats() {
        assert_eq!(CommandBusMode::from_env_value("ehdb"), CommandBusMode::Ehdb);
        assert_eq!(
            CommandBusMode::from_env_value(" SHADOW "),
            CommandBusMode::Shadow
        );
        assert_eq!(CommandBusMode::from_env_value("nats"), CommandBusMode::Nats);
        assert_eq!(
            CommandBusMode::from_env_value("garbage"),
            CommandBusMode::Nats
        );
        assert_eq!(CommandBusMode::default(), CommandBusMode::Nats);
        // ehdb consumes EHDB; shadow keeps consuming NATS (authoritative).
        assert!(CommandBusMode::Ehdb.consumes_ehdb() && !CommandBusMode::Shadow.consumes_ehdb());
        // both ehdb + shadow want the writer to exist; nats does not.
        assert!(CommandBusMode::Ehdb.hosts_relevant() && CommandBusMode::Shadow.hosts_relevant());
        assert!(!CommandBusMode::Nats.hosts_relevant());
    }

    #[test]
    fn member_id_is_nonzero_and_stable() {
        assert_ne!(member_id("worker-a"), 0);
        assert_eq!(member_id("worker-a"), member_id("worker-a"));
        assert_ne!(member_id("worker-a"), member_id("worker-b"));
    }
}
