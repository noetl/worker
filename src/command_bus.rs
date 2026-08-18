//! **L1 T4 — the EHDB command bus (worker side, flag-gated).**
//!
//! Behind `NOETL_COMMAND_BUS`.
//!
//! The cutover is done: every prod workload sets `NOETL_COMMAND_BUS=ehdb`
//! explicitly and NATS is deleted (noetl/ai-meta#194 T5). The flag is therefore
//! **required with no default** — an unset or unrecognised value is a startup
//! error, not a guess at a dead transport (noetl/ai-meta#243). Two
//! responsibilities, both opt-in:
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
use crate::dispatch::{claim_outcome, CommandNotification};
use crate::graceful::WriterShutdown;

/// Which transport carries commands (`NOETL_COMMAND_BUS`).
///
/// There is deliberately **no `Default`** (noetl/ai-meta#243). A default here
/// would have to name a transport, and since T5 deleted NATS the only transport
/// that exists is EHDB — so defaulting means guessing, and the guess this type
/// used to make was `Nats`.
///
/// That made "unset the flag to roll back" an **outage** rather than a
/// rollback: every prod workload sets `NOETL_COMMAND_BUS=ehdb` explicitly, so
/// the dead default was load-bearing only in the sense that nothing exercised
/// it. Worse, the fall-through laundered *unset* and *typo* into the string
/// `Nats`, so the error an operator finally saw named a value nothing had set
/// — sending them to grep for a `nats` that was not there.
///
/// The `Nats` variant survives so a stale `NOETL_COMMAND_BUS=nats` still gets a
/// specific, actionable error instead of a generic parse failure. It is not
/// selectable. Mirrors the sibling [`crate::event_bus::EventSourceMode`] and
/// the server's `CommandBusMode`, which took this same fix first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandBusMode {
    /// ⚠ Not selectable — NATS was deleted at T5 (noetl/ai-meta#212). Retained
    /// only so an explicit `nats` names itself in the error.
    Nats,
    Ehdb,
    Shadow,
}

impl CommandBusMode {
    /// Resolve `NOETL_COMMAND_BUS`, failing loud. Required — there is no default.
    pub fn from_env_strict() -> Result<Self> {
        Self::parse_strict(&std::env::var("NOETL_COMMAND_BUS").unwrap_or_default())
    }

    /// The pure half of [`Self::from_env_strict`], split out so the behaviour is
    /// testable without mutating process-global env from parallel tests (the
    /// `EnvGuard` SAFETY note claiming `cargo test` serialises tests is false).
    pub fn parse_strict(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "ehdb" => Ok(Self::Ehdb),
            "shadow" => Ok(Self::Shadow),
            "nats" => anyhow::bail!(
                "NOETL_COMMAND_BUS=nats selects a transport that no longer exists — NATS was \
                 removed at T5 (noetl/ai-meta#212). Set NOETL_COMMAND_BUS=ehdb."
            ),
            "" => anyhow::bail!(
                "NOETL_COMMAND_BUS is required and unset. There is no default: the only \
                 transport is EHDB, and guessing would start a worker that hosts no writer \
                 and claims no commands while registering, heartbeating, and reporting \
                 ready. Set NOETL_COMMAND_BUS=ehdb (noetl/ai-meta#243)."
            ),
            other => anyhow::bail!(
                "NOETL_COMMAND_BUS={other:?} is not a known transport. Valid values: ehdb, \
                 shadow. (nats was removed at T5.) Refusing to fall back, because a typo \
                 would silently select a dead transport (noetl/ai-meta#243)."
            ),
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
    /// Build from env. `Err` when `NOETL_COMMAND_BUS` is unset or unrecognised —
    /// see [`CommandBusMode::parse_strict`] (noetl/ai-meta#243).
    pub fn from_env() -> Result<Self> {
        let mode = CommandBusMode::from_env_strict()?;
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
        Ok(Self {
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
        })
    }
}

/// Host the shard's writer: open the durable command-log engine and spawn its
/// ingest (publish-in), claim (compete-out), and `/metrics` (lag) faces.
///
/// Returns the writer handle **and** the [`WriterShutdown`] the caller must
/// hold and await on SIGTERM (noetl/ai-meta#209). Dropping the shutdown handle
/// is what the old code effectively did, and it is what let the seal race both
/// in-flight ingest and process exit.
///
/// Idempotent per process; call once when `config.host`.
pub async fn spawn_writer_host(
    config: &CommandBusConfig,
) -> Result<(Arc<FeedWriter<D1EventLog>>, WriterShutdown)> {
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
    // noetl/ai-meta#209 — say, at startup, whether the engine replayed an
    // unsealed active part left by a crash.
    //
    // This exists because the metric alone could not answer it. A live writer
    // reported `ehdb_l0_recovered_active_records = 0` after three hard kills
    // with a verified 18-frame unsealed part, while the records demonstrably
    // survived (a new sealed part covering exactly the missing range appeared on
    // restart). The same engine code recovers that same file's real bytes 18/18
    // in a test. So either the path does not run in this binary or something
    // else replays the tail — and with no log line there was no way to tell
    // which, only more inference.
    //
    // A counter that reads 0 cannot distinguish "did not run" from "ran and
    // found nothing"; a line that always prints can.
    let recovered_at_open = engine.metrics().snapshot().recovered_active_records;
    tracing::info!(
        shard = config.shard,
        dir = %dir.display(),
        recovered_active_records = recovered_at_open,
        "EHDB command-bus engine opened (recovered_active_records>0 means an unsealed part was replayed after an unclean exit)"
    );
    let writer = Arc::new(FeedWriter::new(engine));

    // noetl/ai-meta#209: the host owns the ingest face's lifetime now. The
    // acceptor runs under a `select!` against `stop_ingest`, so shutdown can
    // close the listener *before* it seals instead of sealing underneath a
    // still-accepting publisher.
    let stop_ingest = crate::graceful::StopSignal::new();
    let mut ingest_stop_handle = None;
    if let Some(addr) = config.ingest_bind {
        let listener = TcpListener::bind(addr).await?;
        // Register before spawning, so the shutdown barrier counts this face
        // whether or not its task has been polled yet.
        tokio::spawn(crate::graceful::until_stopped(
            stop_ingest.register("command-bus ingest"),
            ehdb_feed::serve_ingest(listener, writer.clone()),
        ));
        ingest_stop_handle = Some(stop_ingest.clone());
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
    // noetl/ai-meta#209: built here, run by `Worker::shutdown` from `main`'s
    // signal branch. The old detached SIGTERM handler is gone — a second,
    // independent handler racing `main`'s was the reason the seal usually lost.
    let shutdown = {
        let coordinator = coordinator.clone();
        WriterShutdown::new(
            "command-bus",
            config.shard,
            ingest_stop_handle,
            Box::new(move || {
                let coordinator = coordinator.clone();
                Box::pin(async move {
                    coordinator.persist_cursor().await?;
                    Ok(())
                })
            }),
            vec![Arc::new(crate::graceful::EngineSeal::new(writer.clone()))],
        )
    };

    if let Some(addr) = config.claim_bind {
        let listener = TcpListener::bind(addr).await?;
        tokio::spawn(crate::graceful::supervised(
            "command-bus claim",
            ehdb_feed::serve_claims(listener, coordinator.clone()),
        ));
        tracing::info!(%addr, shard = config.shard, "EHDB command-bus claim coordinator up");
    }

    if let Some(addr) = config.metrics_bind {
        // Seed the reported subject label set from the existing log **before** the
        // endpoint is up, so the very first scrape after a restart already carries
        // a line for every pool this shard has ever routed to. Without it a
        // freshly-restarted writer reports an empty subject set until each pool's
        // next command arrives — and to KEDA a `valueLocation` that matches no
        // line is a *scaler error*, not a backlog of 0 (noetl/ai-meta#194).
        coordinator.seed_subjects().await;

        // The scaler provider is sync; publish the async lag into an atomic that a
        // background sampler refreshes.
        let gauge = Arc::new(AtomicU64::new(0));
        // The committed cursor rides the same sampler: it is the value a restart
        // resumes from, so an operator watching a restart can see it advance
        // (noetl/ai-meta#208) instead of the hardcoded 0 reported before.
        let committed = Arc::new(AtomicU64::new(0));
        // The per-pool split — the value the user pool's ScaledObject actually
        // triggers on. Whole-shard lag mixes the pools that share this shard, so a
        // stuck system-pool command would pin it high and hold the user pool at
        // maxReplicaCount forever (noetl/ai-meta#194, noetl/ai-meta#210).
        let subjects = Arc::new(std::sync::Mutex::new(Vec::<ehdb_feed::SubjectLag>::new()));
        let sampler = gauge.clone();
        let committed_sampler = committed.clone();
        let subject_sampler = subjects.clone();
        let coord = coordinator.clone();
        tokio::spawn(async move {
            loop {
                sampler.store(coord.lag().await, Ordering::Relaxed);
                committed_sampler.store(coord.committed_cursor().await, Ordering::Relaxed);
                let split = coord.subject_lags().await;
                if let Ok(mut guard) = subject_sampler.lock() {
                    *guard = split;
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
        let shard = config.shard;
        let read = gauge.clone();
        // Serve the lag snapshot, the resume facts, **and** the append-integrity
        // counters from one endpoint. Each answers a different question an
        // operator asks about this bus, and a writer that binds only one of them
        // drops the others silently (noetl/ai-meta#208 follow-up).
        let resume_reports = coordinator.resume_report().into_iter().collect::<Vec<_>>();
        let integrity_engine = writer.clone();
        tokio::spawn(serve_writer_metrics(
            addr,
            resume_reports,
            move || ehdb_feed::LagSnapshot {
                shards: vec![ShardLag {
                    shard,
                    committed: committed.load(Ordering::Relaxed),
                    lag: read.load(Ordering::Relaxed),
                }],
                subjects: subjects.lock().map(|g| g.clone()).unwrap_or_default(),
            },
            move || {
                integrity_engine
                    .engine()
                    .lock()
                    .ok()
                    .map(|e| e.metrics().snapshot())
            },
        ));
        tracing::info!(%addr, "EHDB command-bus /metrics lag + resume + integrity endpoint up");
    }

    Ok((writer, shutdown))
}

/// Render the writer's **append-integrity** counters (noetl/ai-meta#206).
///
/// `out_of_order_appends` is the detector for the noetl/ai-meta#203 loss class:
/// the writer assigns each command's feed ordering key from its own
/// `global_sequence`, and `L0Engine` counts any append whose key fails to advance
/// past the previous one. A non-zero value means a command could be dropped from
/// a subscriber's view — the exact failure that stalled 23 of 40 commands before
/// #203 was fixed.
///
/// It was asserted in ehdb's tests from the start but never exposed on the
/// writer, so **in production the invariant was unobservable** — you could only
/// infer it after the fact from delivery accounting in `noetl.event`. That is not
/// good enough for a bus with no NATS behind it: T5 removes the fallback, so this
/// has to be a gauge a soak can watch, not a postmortem query.
///
/// `appends` rides along as the denominator — a rate of zero out-of-order appends
/// is only meaningful next to how many appends actually happened.
fn render_integrity(m: &ehdb_l0::metrics::L0MetricsSnapshot) -> String {
    let mut out = String::new();
    out.push_str(
        "# HELP ehdb_l0_out_of_order_appends Appends whose assigned sort key did not advance — the noetl/ai-meta#203 delivery-loss detector. Must stay 0.\n",
    );
    out.push_str("# TYPE ehdb_l0_out_of_order_appends counter\n");
    out.push_str(&format!(
        "ehdb_l0_out_of_order_appends {}\n",
        m.out_of_order_appends
    ));
    out.push_str("# HELP ehdb_l0_appends Total appends to this writer's log — the denominator for the counter above.\n");
    out.push_str("# TYPE ehdb_l0_appends counter\n");
    out.push_str(&format!("ehdb_l0_appends {}\n", m.appends));
    // noetl/ai-meta#209 — records replayed from an unsealed active part left by
    // a crash.  Exposed for the same reason as the counter above: the recovery
    // exists in the engine and is asserted in ehdb's tests, but until it is on
    // this endpoint it is unobservable in production, and an end-to-end SIGKILL
    // test can only report "the metric is absent" — which is indistinguishable
    // from "nothing was recovered".  That is exactly how the first post-fix run
    // of `sigkill-writer.sh` failed to prove anything.
    //
    // A NON-ZERO value is not an error: it means the process did not exit
    // cleanly (a clean shutdown seals, leaving nothing to replay), and the count
    // is how many acked records would have been LOST before the fix.  Zero after
    // a graceful roll is the expected reading.
    out.push_str("# HELP ehdb_l0_recovered_active_records Records replayed from an unsealed active part after a hard kill (noetl/ai-meta#209). Non-zero means the process did not exit cleanly; the count is what would previously have been lost.\n");
    out.push_str("# TYPE ehdb_l0_recovered_active_records counter\n");
    out.push_str(&format!(
        "ehdb_l0_recovered_active_records {}\n",
        m.recovered_active_records
    ));
    out
}

/// The writer's `/metrics`: lag snapshot + resume facts + append integrity.
///
/// Composed here rather than in `ehdb-feed` because the integrity counters come
/// off the **L0 engine**, not the feed's scaler surface, and because keeping the
/// composition worker-side means adding a series does not require an ehdb
/// revision bump (which would invalidate the image's dependency layer).
///
/// The lag + resume halves are rendered by ehdb's own public renderers, so their
/// byte shape — which KEDA prefix-matches — stays owned by the crate that tests
/// it.
async fn serve_writer_metrics<L, I>(
    addr: SocketAddr,
    reports: Vec<ehdb_feed::ResumeReport>,
    lag: L,
    integrity: I,
) -> std::io::Result<()>
where
    L: Fn() -> ehdb_feed::LagSnapshot + Send + Sync + 'static,
    I: Fn() -> Option<ehdb_l0::metrics::L0MetricsSnapshot> + Send + Sync + 'static,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = TcpListener::bind(addr).await?;
    let resume = Arc::new(ehdb_feed::render_resume(&reports));
    let lag = Arc::new(lag);
    let integrity = Arc::new(integrity);
    loop {
        let (mut sock, _peer) = listener.accept().await?;
        let (lag, resume, integrity) = (lag.clone(), resume.clone(), integrity.clone());
        tokio::spawn(async move {
            let mut scratch = [0u8; 1024];
            let _ = sock.read(&mut scratch).await;
            let mut body = ehdb_feed::render_snapshot(&lag());
            body.push_str(&resume);
            // Best-effort: a contended engine lock must not fail the scrape, or
            // the autoscaler's lag series would vanish with it.
            if let Some(m) = integrity() {
                body.push_str(&render_integrity(&m));
            }
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.flush().await;
        });
    }
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
/// `observability.md` Principle 4 — the EHDB twin of the old `NatsAckHandle`,
/// which was deleted with `src/nats/` at T5).
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
                        crate::metrics::record_ehdb_claim_reconnect("commands", "connect_failed");
                        tracing::warn!(claim_addr = %self.claim_addr, error = %e, "EHDB claim connect failed; retrying");
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        continue;
                    }
                }
            }
            // noetl/ai-meta#155: split ONE pickup into park vs outcome.  The
            // gap from "server published" to "worker began work" is ~510ms p50
            // and is 79% of a turn, while every bus primitive under it is
            // microseconds-to-milliseconds.  Timing the two halves separately
            // is the only way to tell a slow hand-over from a slow claim ack.
            let park_started = std::time::Instant::now();
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
                    crate::metrics::record_ehdb_claim_reconnect("commands", "claim_next_failed");
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
            let park_secs = park_started.elapsed().as_secs_f64();
            crate::metrics::record_command_pickup_phase("park", park_secs);
            let notification: CommandNotification =
                serde_json::from_str(&claimed.record.payload)
                    .map_err(|e| anyhow!("EHDB command notification decode: {e}"))?;
            let outcome_started = std::time::Instant::now();
            let outcome = claim_outcome(&self.client, &self.worker_id, &notification).await?;
            let outcome_secs = outcome_started.elapsed().as_secs_f64();
            crate::metrics::record_command_pickup_phase("outcome", outcome_secs);
            tracing::debug!(
                park_ms = park_secs * 1000.0,
                outcome_ms = outcome_secs * 1000.0,
                sort_key = claimed.sort_key,
                "command pickup phases"
            );
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

/// The worker's command source. Only the EHDB bus remains (noetl/ai-meta#212);
/// the enum is kept so `Worker` stays non-generic and a second transport can be
/// added back without threading a type parameter through the pull loop.
pub enum WorkerCommandSource {
    // Boxed so the source does not inflate the enum (large_enum_variant).
    Ehdb(Box<EhdbCommandSource>),
}

/// The ack mechanism for a claimed command (kept separate from the notification
/// metadata so `Worker::process_commands` reads `handle.notification` uniformly).
enum WorkerAckInner {
    /// The EHDB claim's global sort key (ack/nack go through the coordinator).
    Ehdb(u64),
}

/// Ack handle for either source. `notification` is exposed uniformly (both
/// sources carry it) so the correlation call sites don't branch on transport.
pub struct WorkerAckHandle {
    pub notification: CommandNotification,
    inner: WorkerAckInner,
}

#[async_trait]
impl CommandSource for WorkerCommandSource {
    type AckHandle = WorkerAckHandle;

    async fn next(&mut self) -> Result<Option<Pulled<Self::AckHandle>>> {
        match self {
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
        let Self::Ehdb(s) = self;
        let WorkerAckInner::Ehdb(sort_key) = handle.inner;
        s.ack_sort_key(sort_key).await
    }

    async fn nack(&self, handle: Self::AckHandle) -> Result<()> {
        let Self::Ehdb(s) = self;
        let WorkerAckInner::Ehdb(sort_key) = handle.inner;
        s.nack_sort_key(sort_key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// noetl/ai-meta#243 — the regression guard.
    ///
    /// The predecessor of this test asserted the bug: it was named
    /// `mode_parsing_defaults_to_nats` and checked that `""`, `"garbage"`, and
    /// `CommandBusMode::default()` all resolved to `Nats`. That is exactly the
    /// behaviour that turned "unset the flag to roll back" into an outage, and
    /// a green test spelling it out is why it survived T5.
    ///
    /// The load-bearing assertion is the negative one: **no input silently
    /// yields `Nats`.** Note this is checked over the error path, because there
    /// is no longer any way to *obtain* a `Nats` from parsing.
    #[test]
    fn unset_or_invalid_never_silently_selects_nats() {
        // Valid values still resolve, case- and whitespace-insensitively.
        assert_eq!(CommandBusMode::parse_strict("ehdb").unwrap(), CommandBusMode::Ehdb);
        assert_eq!(
            CommandBusMode::parse_strict(" SHADOW ").unwrap(),
            CommandBusMode::Shadow
        );

        // The regression itself: every input that used to fall through to the
        // dead transport must now be an error.
        for bad in ["", "   ", "garbage", "ehbd", "NATS", "nats"] {
            let err = CommandBusMode::parse_strict(bad);
            assert!(
                err.is_err(),
                "{bad:?} must not resolve — it used to become Nats silently"
            );
        }

        // Each failure mode names itself, so the operator is not sent looking
        // for a `nats` that nothing set.
        let unset = CommandBusMode::parse_strict("").unwrap_err().to_string();
        assert!(unset.contains("NOETL_COMMAND_BUS"), "must name the var: {unset}");
        assert!(unset.contains("unset"), "must say what is wrong: {unset}");

        let stale = CommandBusMode::parse_strict("nats").unwrap_err().to_string();
        assert!(
            stale.contains("no longer exists"),
            "an explicit stale `nats` gets its own actionable error: {stale}"
        );

        let typo = CommandBusMode::parse_strict("ehbd").unwrap_err().to_string();
        assert!(typo.contains("ehbd"), "must echo the offending value: {typo}");

        // ehdb consumes EHDB; shadow keeps consuming NATS (authoritative).
        assert!(CommandBusMode::Ehdb.consumes_ehdb() && !CommandBusMode::Shadow.consumes_ehdb());
        // both ehdb + shadow want the writer to exist; nats does not.
        assert!(CommandBusMode::Ehdb.hosts_relevant() && CommandBusMode::Shadow.hosts_relevant());
        assert!(!CommandBusMode::Nats.hosts_relevant());
    }

    /// The strict resolver must reach the config builder, so a misconfigured
    /// worker fails on the startup path rather than deep inside `Worker::new`
    /// with a mode it invented. Mirrors `EventSourceMode`'s equivalent test.
    ///
    /// Uses the real var, so it sets and clears within the test. `cargo test`
    /// does NOT serialise tests; this is safe only because no other test in
    /// this binary reads `NOETL_COMMAND_BUS`.
    #[test]
    fn from_env_strict_reads_the_real_var() {
        std::env::remove_var("NOETL_COMMAND_BUS");
        assert!(
            CommandBusMode::from_env_strict().is_err(),
            "an unset var must be an error, not a default"
        );
        assert!(
            CommandBusConfig::from_env().is_err(),
            "the error must reach the config builder, not be swallowed"
        );
        std::env::set_var("NOETL_COMMAND_BUS", "ehdb");
        assert_eq!(CommandBusMode::from_env_strict().unwrap(), CommandBusMode::Ehdb);
        assert_eq!(CommandBusConfig::from_env().unwrap().mode, CommandBusMode::Ehdb);
        std::env::remove_var("NOETL_COMMAND_BUS");
    }

    #[test]
    fn member_id_is_nonzero_and_stable() {
        assert_ne!(member_id("worker-a"), 0);
        assert_eq!(member_id("worker-a"), member_id("worker-a"));
        assert_ne!(member_id("worker-a"), member_id("worker-b"));
    }

    /// noetl/ai-meta#209 — the recovery counter has to be ON this endpoint.
    ///
    /// It existed in the engine and was asserted in ehdb's own tests, but was
    /// never rendered here, so in production it was unobservable: an end-to-end
    /// SIGKILL run could only report "the metric is absent", which is
    /// indistinguishable from "nothing was recovered". That is exactly how the
    /// first post-fix run of `sigkill-writer.sh` failed to prove anything.
    #[test]
    fn integrity_exposition_carries_the_crash_recovery_counter() {
        let mut m = ehdb_l0::metrics::L0Metrics::new().snapshot();
        m.recovered_active_records = 7;
        m.out_of_order_appends = 0;
        m.appends = 41;
        let out = render_integrity(&m);

        assert!(
            out.contains("ehdb_l0_recovered_active_records 7"),
            "the recovered-records series must be exposed with its value:\n{out}"
        );
        assert!(
            out.contains("# TYPE ehdb_l0_recovered_active_records counter"),
            "a series without a TYPE line is not scrapeable as a counter:\n{out}"
        );
        // The pre-existing series must survive alongside it.
        assert!(out.contains("ehdb_l0_out_of_order_appends 0"));
        assert!(out.contains("ehdb_l0_appends 41"));
    }

    /// Zero must still be rendered. A counter that only appears once non-zero
    /// cannot distinguish "clean shutdown, nothing to recover" from "this build
    /// does not have the metric" — which is the ambiguity that cost a test run.
    #[test]
    fn the_recovery_counter_is_rendered_even_at_zero() {
        let m = ehdb_l0::metrics::L0Metrics::new().snapshot();
        assert_eq!(m.recovered_active_records, 0);
        assert!(render_integrity(&m).contains("ehdb_l0_recovered_active_records 0"));
    }
}
