//! **L1 T3 — the EHDB events feed (worker side, flag-gated).**
//!
//! The sibling of [`crate::command_bus`] for the *events* path. It replaced the
//! `noetl.events.>` fan-out, which was the last consumer binding NATS
//! (noetl/ai-meta#212) — T5 has since removed NATS entirely
//! (noetl/ai-meta#194). Behind `NOETL_EVENT_BUS_HOST`, default off: prod runs
//! the writer as its own StatefulSet rather than in-process.
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
//! | SSE | `NOETL_EVENT_BUS_SSE_BIND` (9105) | the gateway's live SPA feed |
//! | `/metrics` | `NOETL_EVENT_BUS_METRICS_BIND` (9106) | per-group lag + resume facts |
//! | KV | `NOETL_EVENT_BUS_KV_BIND` (9107) | the gateway's session + request stores |
//! | WAL fan-out | `NOETL_EVENT_BUS_WAL_BIND` (9108) | the off-server state builder's WAL replay |
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

use crate::graceful::WriterShutdown;

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

/// The logical KV buckets the gateway uses — the two NATS KV buckets this face
/// replaces. Named so the sweeper knows what to reclaim.
pub const KV_BUCKETS: [&str; 2] = ["sessions", "requests"];

/// How often lapsed KV entries are reclaimed. Generous, because it is a space
/// concern only: an expired key is already invisible to readers.
const KV_SWEEP: Duration = Duration::from_secs(60);

/// Which transport a materializer drains (`NOETL_*_SOURCE`).
///
/// Deliberately separate from the server's publish-side `NOETL_EVENT_BUS`: the
/// cutover was per-consumer, so a materializer could move to the EHDB feed
/// while publish stayed in `shadow`.
///
/// **There is no default any more (H5).**  While NATS existed, falling back to
/// `nats` on an unrecognised value was the safe direction — a typo could not
/// quietly move the sole writer of the durable event log onto an unproven
/// transport.  Since the internal NATS bus was deleted (noetl/ai-meta#212, prod
/// 2026-08-01) that same fall-through inverted into the dangerous direction: it
/// points a materializer at a transport that is not there.  The failure is
/// silent — the group's cursor simply never advances while executions keep
/// completing — so the mode is now resolved with [`Self::from_env_strict`] at
/// worker startup and an unset or unrecognised value is a hard error.
///
/// The `Default` impl is gone on purpose: `EventSourceMode::default()` was a
/// third silent path to `Nats`, and there is no longer a defensible default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSourceMode {
    Nats,
    Ehdb,
}

impl EventSourceMode {
    /// Permissive parse, kept for callers that already hold a value and want
    /// the historical fall-through.  Prefer [`Self::from_env_strict`] for
    /// anything that decides which transport a consumer actually drains.
    pub fn from_env_value(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "ehdb" => Self::Ehdb,
            _ => Self::Nats,
        }
    }

    /// Resolve the internal event-bus source for `var`, failing loud.
    ///
    /// Unset, empty, or unrecognised is an error rather than a fall-through.
    /// This matches the command bus (`src/worker.rs`, noetl/ai-meta#212) and
    /// pool routing (noetl/ai-meta#218): a worker that starts pointed at a
    /// transport that no longer exists looks perfectly healthy — it registers,
    /// heartbeats, and reports ready — while its group cursor sits flat. A
    /// crashloop an operator sees in seconds beats a stall nobody sees at all.
    ///
    /// `nats` is still accepted when written explicitly: that is an auditable
    /// choice in a manifest, not a silent default, and the NATS drain code is
    /// still present for the user-facing carve-outs.
    pub fn from_env_strict(var: &str) -> Result<Self> {
        Self::parse_strict(var, &std::env::var(var).unwrap_or_default())
    }

    /// The pure half of [`Self::from_env_strict`], split out so the behaviour is
    /// testable without mutating process-global env from parallel tests.
    pub fn parse_strict(var: &str, value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "ehdb" => Ok(Self::Ehdb),
            "nats" => Ok(Self::Nats),
            "" => anyhow::bail!(
                "{var} is unset — refusing to guess the internal event-bus source. \
                 The internal NATS bus was removed (noetl/ai-meta#212); set {var}=ehdb. \
                 Defaulting here would start a materializer against a dead transport, \
                 which fails silently (flat group cursor, executions still complete)."
            ),
            other => anyhow::bail!(
                "{var}={other:?} is not a recognised internal event-bus source — \
                 valid values are `ehdb` (and `nats`, which no longer exists internally). \
                 Refusing to fall back, because a typo would silently point this \
                 materializer at a dead transport (noetl/ai-meta#212)."
            ),
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
    /// `NOETL_EVENT_BUS_SSE_BIND` — the broadcast face.  Separate from the group
    /// claim face because the gateway is a fundamentally different consumer: it
    /// wants every event with no ack and no competing peer, and `Last-Event-ID`
    /// maps straight onto the feed cursor.  Serving it through a claim group
    /// would make SPA clients compete for events, which is exactly wrong.
    pub sse_bind: Option<SocketAddr>,
    /// `NOETL_EVENT_BUS_KV_BIND` — the networked KV face that replaces the two
    /// NATS KV buckets (`sessions`, `requests`).  Its own D4 engine in its own
    /// directory: KV is a random-access fold, the events log is an append-only
    /// stream, and putting session churn in front of the event log's fsync would
    /// repeat the coupling the separate events engine exists to avoid.
    pub kv_bind: Option<SocketAddr>,
    /// `NOETL_EVENT_BUS_KV_DIR` — the KV store's directory.
    pub kv_dir: Option<PathBuf>,
    /// `NOETL_EVENT_BUS_WAL_BIND` — the raw fan-out face.  The off-server state
    /// builder replays the retained WAL from cursor 0 on every boot and never
    /// acks, which is a *subscription*, not a consumer group: giving it a group
    /// would persist a cursor that could outrun a freshly-restarted worker's
    /// empty index — precisely the noetl/ai-meta#119 stall.
    pub wal_bind: Option<SocketAddr>,
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
            sse_bind: env_addr("NOETL_EVENT_BUS_SSE_BIND"),
            kv_bind: env_addr("NOETL_EVENT_BUS_KV_BIND"),
            wal_bind: env_addr("NOETL_EVENT_BUS_WAL_BIND"),
            kv_dir: std::env::var("NOETL_EVENT_BUS_KV_DIR")
                .ok()
                .map(PathBuf::from),
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
/// group claim, SSE, WAL fan-out, KV and `/metrics` faces.
///
/// Returns the group coordinator **and** the [`WriterShutdown`] the caller must
/// hold and await on SIGTERM (noetl/ai-meta#209).
///
/// Note this host previously **never sealed its log**: its shutdown handler
/// only checkpointed group cursors, so every restart dropped the unsealed tail
/// (up to `seal_max_records`, 1024) of the sole writer of the durable
/// `noetl.event` log. The seal is now part of the same sequenced shutdown the
/// command bus uses.
pub async fn spawn_event_writer_host(
    config: &EventBusConfig,
) -> Result<(Arc<GroupCoordinator<D1EventLog>>, WriterShutdown)> {
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
    // noetl/ai-meta#209 — same startup visibility for the EVENTS engine; the
    // command bus is not the only log a crash can leave unsealed.
    let recovered_at_open = engine.metrics().snapshot().recovered_active_records;
    tracing::info!(
        shard = config.shard,
        recovered_active_records = recovered_at_open,
        "EHDB events-feed engine opened (recovered_active_records>0 means an unsealed part was replayed after an unclean exit)"
    );
    let writer = Arc::new(FeedWriter::new(engine));

    // noetl/ai-meta#209: the host owns the ingest face's lifetime, so shutdown
    // can close the listener before it seals rather than sealing underneath a
    // still-accepting publisher.
    // One signal, shared: this host runs several faces (ingest, group-claim,
    // SSE, KV, WAL). Only ingest appends to — and therefore acks into — the log
    // this shutdown seals, so only ingest is registered here. The signal is a
    // `watch`, which broadcasts, so registering the others later needs no change
    // to the mechanism; a permit-storing `notify_one` would have woken exactly
    // one of them. See `graceful`'s module docs.
    let stop_ingest = crate::graceful::StopSignal::new();
    let mut ingest_stop_handle = None;
    if let Some(addr) = config.ingest_bind {
        let listener = TcpListener::bind(addr).await?;
        tokio::spawn(crate::graceful::until_stopped(
            stop_ingest.register("events ingest"),
            ehdb_feed::serve_ingest(listener, writer.clone()),
        ));
        ingest_stop_handle = Some(stop_ingest.clone());
        tracing::info!(%addr, shard = config.shard, "EHDB events-feed ingest listener up");
    }

    // EHDB tier-service CLIENT probe (ai-meta#257 PR 2).  Checks reachability
    // once at startup when NOETL_EHDB_TIER_SERVICE_ADDR is set, and does nothing
    // at all when it is not.  Spawned rather than awaited so an unreachable
    // endpoint delays no other face coming up.
    tokio::spawn(crate::ehdb::tier_client::probe_at_startup());

    // EHDB tier service (ai-meta#257 PR 1) — the writer-fronted face for the
    // storage tiers.  Hosted here because this is the process that owns the
    // durable volumes and already fronts both buses.
    //
    // Skeleton only: it answers `health` and serves no tier data yet.  Absent
    // unless NOETL_EHDB_TIER_SERVICE_BIND is set, so a build with the flag unset
    // opens no socket and is byte-identical to one without this face.
    if let Some(tier_cfg) = crate::ehdb::tier_service::TierServiceConfig::from_env() {
        let listener = TcpListener::bind(tier_cfg.bind).await?;
        let addr = tier_cfg.bind;
        tokio::spawn(crate::ehdb::tier_service::serve_tier(listener));
        tracing::info!(
            %addr,
            shard = config.shard,
            protocol = crate::ehdb::tier_service::PROTOCOL_VERSION,
            "EHDB tier service listener up (skeleton: health only, serves no tier data)"
        );
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
        tokio::spawn(crate::graceful::supervised(
            "events group-claim",
            ehdb_feed::serve_group_claims(listener, coordinator.clone()),
        ));
        tracing::info!(%addr, shard = config.shard, "EHDB events-feed group coordinator up");
    }

    if let Some(addr) = config.sse_bind {
        let listener = TcpListener::bind(addr).await?;
        tokio::spawn(crate::graceful::supervised(
            "events SSE",
            ehdb_feed::sse::serve_sse(listener, writer.clone()),
        ));
        tracing::info!(%addr, shard = config.shard, "EHDB events-feed SSE face up");
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

    // noetl/ai-meta#209: built here, run by `Worker::shutdown` from `main`'s
    // signal branch. Replaces the detached SIGTERM handler that raced `main`'s
    // own — and that only checkpointed cursors, never sealing the log.
    let mut shutdown = {
        let coordinator = coordinator.clone();
        WriterShutdown::new(
            "events-feed",
            config.shard,
            ingest_stop_handle,
            Box::new(move || {
                let coordinator = coordinator.clone();
                Box::pin(async move {
                    coordinator.checkpoint().await?;
                    Ok(())
                })
            }),
            vec![Arc::new(crate::graceful::EngineSeal::new(writer.clone()))],
        )
    };
    tokio::spawn(watch_cursor_errors(coordinator.clone()));

    // The networked KV face — the NATS-KV replacement for the gateway's
    // `sessions` + `requests` buckets (noetl/ai-meta#214, #215).  Its own D4
    // engine in its own directory: KV is a random-access fold while the events
    // log is an append-only stream, and putting session churn in front of the
    // events log's fsync would repeat exactly the coupling the separate events
    // engine exists to avoid.
    if let Some(addr) = config.wal_bind {
        let listener = TcpListener::bind(addr).await?;
        // The most fragile face of the lot: `ehdb_feed::serve` handshakes inside
        // its accept loop, so ONE malformed connection kills it permanently.
        // See `graceful::supervised`.
        tokio::spawn(crate::graceful::supervised(
            "events WAL fan-out",
            ehdb_feed::serve(writer.clone(), listener),
        ));
        tracing::info!(%addr, shard = config.shard, "EHDB events-feed WAL fan-out face up");
    }

    if let Some(addr) = config.kv_bind {
        let kv_dir = config
            .kv_dir
            .clone()
            .ok_or_else(|| anyhow!("NOETL_EVENT_BUS_KV_BIND requires NOETL_EVENT_BUS_KV_DIR"))?;
        std::fs::create_dir_all(&kv_dir)?;
        let kv_substrate: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&kv_dir)?);
        let store = ehdb_l0::KvStore::open(ehdb_l0::KvStore::config(&kv_dir), kv_substrate)?;
        // noetl/ai-meta#209 — the KV face is the THIRD L0 engine in this pod and
        // the last one without startup visibility. It holds sessions and request
        // state, so an unsealed part it replays after an unclean exit is exactly
        // the "sessions survived a crash" question an operator will ask, and
        // until now nothing answered it either way.
        tracing::info!(
            dir = %kv_dir.display(),
            recovered_active_records = store.engine().metrics().snapshot().recovered_active_records,
            "EHDB KV engine opened (recovered_active_records>0 means an unsealed part was replayed after an unclean exit)"
        );
        let kv = Arc::new(ehdb_feed::KvCoordinator::new(store));
        // noetl/ai-meta#209 — the KV face joins the shutdown sequence. It is
        // constructed after the `WriterShutdown` above (the feed writer binds
        // first), which is why the shutdown takes a late registration rather
        // than the whole set up front.
        shutdown.push_sealable(Arc::new(crate::graceful::KvSeal::new(kv.clone())));
        // Reclaim lapsed entries; correctness already comes from the read-side
        // TTL filter, so this only bounds the log's growth.
        kv.clone()
            .spawn_sweeper(KV_BUCKETS.iter().map(|s| s.to_string()).collect(), KV_SWEEP);
        let listener = TcpListener::bind(addr).await?;
        tokio::spawn(crate::graceful::supervised(
            "events KV",
            ehdb_feed::serve_kv(listener, kv),
        ));
        tracing::info!(%addr, dir = %kv_dir.display(), "EHDB KV face up");
    }

    if let Some(addr) = config.metrics_bind {
        tokio::spawn(serve_event_metrics(addr, coordinator.clone()));
        tracing::info!(%addr, "EHDB events-feed /metrics endpoint up");
    }

    Ok((coordinator, shutdown))
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
    // noetl/ai-meta#230 — how far the FEED has got, independent of consumption.
    //
    // `group_lag == 0` is the gate in the T3/T4 cutover work and in every
    // paired-evidence check since, and it reads **identically** whether the
    // consumers drained a real burst or nothing was ever published. There was no
    // series that could tell those apart, so a gate run during a quiet window
    // passed having verified nothing.
    //
    // That is not hypothetical: `should_publish` excludes system-pool playbooks
    // by design (they drain the stream), and prod's steady state is an hourly
    // `system/scheduled_cleanup` plus a 3-minute watchdog — so long stretches
    // produce zero feed movement, entirely correctly. A check sampled in one of
    // those windows sees `0 0 0` and looks green. It cost a wrongly-filed bug
    // (#229) before this series existed.
    //
    // The tip is `committed + lag` for any group: every group consumes the same
    // feed, so they agree, and `max` is taken only to be robust to a torn read
    // across the two values. Being a monotonic gauge, a gate can assert "tip
    // advanced by N **and** lag returned to 0" — which an idle window cannot
    // satisfy.
    let tip = lags
        .iter()
        .map(|(_, committed, lag)| committed.saturating_add(*lag))
        .max()
        .unwrap_or(0);
    out.push_str("# HELP ehdb_events_feed_tip Records appended to the events feed, independent of consumption — advances on publish even when every group is at lag 0. Assert tip-advance AND lag-0 together; lag-0 alone cannot distinguish a drained feed from an empty one (noetl/ai-meta#230).\n");
    out.push_str("# TYPE ehdb_events_feed_tip gauge\n");
    out.push_str(&format!("ehdb_events_feed_tip {tip}\n"));
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

/// Log any new cursor-persist failure the coordinator has recorded.
///
/// `ehdb-feed` carries no logging dependency, so it keeps the last failure and
/// the host emits it. Without this the counter was a number with no way to learn
/// why (noetl/ai-meta#216). Logged only when the count moves, so a persistent
/// fault does not spam — the counter carries the rate, the log carries the
/// reason.
async fn watch_cursor_errors(coordinator: Arc<GroupCoordinator<D1EventLog>>) {
    let mut seen = 0u64;
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let now = coordinator.cursor_errors();
        if now > seen {
            match coordinator.last_cursor_error() {
                Some((group, error)) => tracing::warn!(
                    group,
                    %error,
                    failures = now,
                    new = now - seen,
                    "EHDB events-feed cursor persist failed — group progress is not \
                     durable, so a writer restart will replay from an older cursor \
                     (records are re-delivered, never lost)"
                ),
                None => tracing::warn!(
                    failures = now,
                    "EHDB events-feed cursor persist failed (no detail recorded)"
                ),
            }
            seen = now;
        }
    }
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
            sse_bind: None,
            kv_bind: None,
            kv_dir: None,
            wal_bind: None,
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
            sse_bind: None,
            kv_bind: None,
            kv_dir: None,
            wal_bind: None,
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
            sse_bind: None,
            kv_bind: None,
            kv_dir: None,
            wal_bind: None,
            metrics_bind: None,
            ack_wait: Duration::from_secs(30),
            cursor_persist: Duration::ZERO,
            cursor_fallback: CursorFallback::Tail,
        };
        let (coordinator, _shutdown) = spawn_event_writer_host(&config).await.unwrap();
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
        // noetl/ai-meta#230 — the feed tip must be on the FIRST scrape, at zero.
        // A series that only materialises once non-zero cannot distinguish "no
        // appends yet" from "this build lacks the metric", which is the same
        // ambiguity that made lag-0 unusable as a gate in the first place.
        assert!(
            body.contains("ehdb_events_feed_tip 0"),
            "the feed tip must be exposed from the first scrape, at 0, in:\n{body}"
        );
        assert!(
            body.contains("# TYPE ehdb_events_feed_tip gauge"),
            "a series without a TYPE line is not scrapeable in:\n{body}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// noetl/ai-meta#230 — the tip is what makes a paired-evidence gate
    /// falsifiable: it advances on publish even while every group sits at lag 0,
    /// so "tip advanced by N AND lag 0" cannot be satisfied by an idle window
    /// the way bare "lag 0" can.
    ///
    /// Asserted on the arithmetic rather than a live feed, because the property
    /// under test is that the tip is derived from `committed + lag` and is
    /// therefore consumption-independent.
    #[test]
    fn the_feed_tip_is_consumption_independent() {
        // Three groups on one feed at different consumption points; the feed has
        // carried 100 records in every case.
        let lags: Vec<(String, u64, u64)> = vec![
            ("noetl_materializer".into(), 100, 0),        // fully drained
            ("noetl_result_materializer".into(), 60, 40), // mid-drain
            ("noetl_state_materializer".into(), 0, 100),  // untouched
        ];
        let tip = lags
            .iter()
            .map(|(_, c, l)| c.saturating_add(*l))
            .max()
            .unwrap_or(0);
        assert_eq!(
            tip, 100,
            "the tip must report what the feed carried, not what was consumed"
        );

        // The case the gate has to catch: everything at lag 0 because nothing
        // was ever published. Bare lag-0 is indistinguishable from the first row
        // above; the tip is not.
        let empty: Vec<(String, u64, u64)> = vec![
            ("noetl_materializer".into(), 0, 0),
            ("noetl_result_materializer".into(), 0, 0),
            ("noetl_state_materializer".into(), 0, 0),
        ];
        let empty_tip = empty
            .iter()
            .map(|(_, c, l)| c.saturating_add(*l))
            .max()
            .unwrap_or(0);
        assert_eq!(empty_tip, 0);
        assert_ne!(
            tip, empty_tip,
            "a drained feed and an empty feed must not read the same"
        );
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
                crate::metrics::record_ehdb_claim_reconnect("events", "connect_failed");
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
                    // noetl/ai-meta#225: before the events face carried keepalive
                    // and a negotiated heartbeat this arm was **unreachable** on
                    // a half-open socket — the read never returned, so the
                    // consumer parked silently while `noetl.event` stopped being
                    // written and every health signal stayed green. Reaching it
                    // is the fix working; the counter is how that is visible in
                    // prod, where the group cursor's freeze was the only symptom.
                    crate::metrics::record_events_consumer_redial("group_claim");
                    crate::metrics::record_ehdb_claim_reconnect("events", "claim_next_failed");
                    tracing::warn!(group, %error, "events-feed claim failed; redialing");
                    break;
                }
            }
        }
        tokio::time::sleep(backoff).await;
    }
}

/// A uniform drain over either transport, so a materializer's loop does not
/// care which bus it is on.
///
/// The three materializers had one NATS drain each; adding an EHDB path to all
/// of them by copy-paste would have produced three near-identical loops that
/// drift. This carries the difference — poll shape, ack token type — in one
/// place, and each loop keeps its own *policy* (what it does with a batch,
/// when it acks), which is what actually differs between them.
#[allow(clippy::large_enum_variant)] // EhdbGroupSource is the common case and
                                     // is constructed once per materializer; boxing it would add an indirection on
                                     // every poll to save a few words on a long-lived value.
pub enum MaterializerFeed {
    Nats(Box<dyn noetl_tools::tools::source::SourceClient>),
    Ehdb(EhdbGroupSource),
}

/// One drained batch: the payloads plus whatever this transport acks with.
pub struct DrainedBatch {
    pub payloads: Vec<serde_json::Value>,
    nats_acks: Vec<String>,
    ehdb_acks: Vec<u64>,
}

impl DrainedBatch {
    pub fn len(&self) -> usize {
        self.payloads.len()
    }
    pub fn is_empty(&self) -> bool {
        self.payloads.is_empty()
    }
}

impl MaterializerFeed {
    /// Open the feed selected by `mode`. `group` names both the JetStream
    /// durable consumer and the EHDB named group — they are deliberately the
    /// same string, so an operator reads one name in both worlds.
    pub async fn open(
        mode: EventSourceMode,
        group: &str,
        claim_addr: Option<&str>,
        nats_source: impl FnOnce() -> Result<Box<dyn noetl_tools::tools::source::SourceClient>>,
        batch: u32,
    ) -> Result<Self> {
        if mode.is_ehdb() {
            let addr = claim_addr.ok_or_else(|| {
                anyhow!("EHDB materializer source requires NOETL_EVENT_BUS_CLAIM_ADDR")
            })?;
            let member = group_member_id(group);
            return Ok(Self::Ehdb(
                EhdbGroupSource::connect(
                    addr.to_string(),
                    group.to_string(),
                    ALL_EVENTS_FILTER.to_string(),
                    member,
                    (batch as usize).max(1),
                )
                .await?,
            ));
        }
        Ok(Self::Nats(nats_source()?))
    }

    /// Drain up to `batch`, waiting at most `timeout` for the first record.
    pub async fn poll(&self, batch: u32, timeout: Duration) -> Result<DrainedBatch> {
        match self {
            Self::Nats(source) => {
                use noetl_tools::tools::source::{AckMode, PollOptions};
                let opts = PollOptions::new(
                    Some(batch),
                    Some(timeout.as_millis() as u64),
                    AckMode::Defer,
                );
                let outcome = source
                    .poll(&opts)
                    .await
                    .map_err(|e| anyhow!("nats drain failed: {e}"))?;
                Ok(DrainedBatch {
                    payloads: outcome.messages.iter().map(|m| m.data.clone()).collect(),
                    nats_acks: outcome.ack_ids,
                    ehdb_acks: Vec::new(),
                })
            }
            Self::Ehdb(source) => {
                if source.is_finished() {
                    return Err(anyhow!("events-feed claim task exited"));
                }
                let drained = source.poll(batch as usize, timeout).await;
                Ok(DrainedBatch {
                    payloads: drained.iter().map(|d| d.payload.clone()).collect(),
                    nats_acks: Vec::new(),
                    ehdb_acks: drained.iter().map(|d| d.sort_key).collect(),
                })
            }
        }
    }

    /// Ack a drained batch. Returns how many were acked.
    pub async fn ack(&self, batch: &DrainedBatch) -> usize {
        match self {
            Self::Nats(source) => {
                use noetl_tools::tools::source::AckDisposition;
                source
                    .ack(&batch.nats_acks, AckDisposition::Ack)
                    .await
                    .map(|r| r.disposed)
                    .unwrap_or(0)
            }
            Self::Ehdb(source) => source.ack(&batch.ehdb_acks).await.unwrap_or(0),
        }
    }
}

/// Stable non-zero member id for a group member.
pub fn group_member_id(name: &str) -> u32 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut h);
    (h.finish() as u32) | 1
}
