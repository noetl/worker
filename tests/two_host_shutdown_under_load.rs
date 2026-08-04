//! **Both writer hosts must seal, under load (noetl/ai-meta#226).**
//!
//! v5.92.0 closed the graceful half of noetl/ai-meta#209 and was released on the
//! strength of an *idle* SIGTERM that sealed both hosts in 568 ms. Taken under
//! load in prod, the same code logged
//!
//! ```text
//!   sealing EHDB writer hosts before exit   hosts=2
//!   EHDB command-bus ingest listener closed
//!   EHDB command-bus cursor persisted on shutdown
//!   EHDB command-bus log sealed on shutdown
//!   <end of stream — pod gone>
//! ```
//!
//! and stopped. No events seal. No `Worker stopped`. Not even the expiry line
//! from the 15 s `tokio::time::timeout` wrapped around the whole sequence. The
//! reopened events log came back **390 records below its persisted cursor** and
//! all three consumer groups resumed `clamped=true`.
//!
//! Two causes, and the existing suite could not see either, because every test
//! in `cmdbus_writer_graceful_shutdown.rs` drives **one** host with **nothing in
//! flight**:
//!
//! 1. The seal held the engine's `MutexGuard` through exit. Every appender then
//!    parks on that mutex *blocking, from inside an async task*, so under load
//!    the tokio worker threads are consumed and the shutdown future is never
//!    polled again. Covered at the engine level by ehdb-feed's
//!    `seal_and_close.rs`, with the starvation reproduced as a negative control.
//! 2. The sequence ran host-at-a-time, so host 2 kept accepting and appending
//!    for the whole time host 1 took, then started its own phases from scratch
//!    against a shared budget.
//!
//! So the bar these tests hold is the one prod failed: with a **backlog in
//! flight at SIGTERM**, both logs seal and both reopen at exactly their
//! persisted cursors — `clamped=false`, zero records below cursor.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use noetl_worker::command_bus::{spawn_writer_host, CommandBusConfig, CommandBusMode};
use noetl_worker::event_bus::{spawn_event_writer_host, EventBusConfig};
use noetl_worker::graceful::seal_all;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "noetl-two-host-shutdown-{tag}-{}-{n}",
        std::process::id()
    ))
}

async fn free_addr() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

fn cmd_config(dir: &std::path::Path, ingest: Option<SocketAddr>) -> CommandBusConfig {
    CommandBusConfig {
        mode: CommandBusMode::Ehdb,
        host: true,
        shard: 0,
        shard_count: 1,
        writer_dir: Some(dir.to_path_buf()),
        ingest_bind: ingest,
        claim_bind: None,
        metrics_bind: None,
        claim_addr: None,
        ack_wait: Duration::from_secs(30),
        cursor_persist: Duration::ZERO,
        cursor_fallback: Default::default(),
    }
}

fn event_config(dir: &std::path::Path, ingest: Option<SocketAddr>) -> EventBusConfig {
    EventBusConfig {
        host: true,
        shard: 0,
        shard_count: 1,
        writer_dir: Some(dir.to_path_buf()),
        ingest_bind: ingest,
        claim_bind: None,
        sse_bind: None,
        kv_bind: None,
        kv_dir: None,
        wal_bind: None,
        metrics_bind: None,
        ack_wait: Duration::from_secs(30),
        cursor_persist: Duration::ZERO,
        cursor_fallback: Default::default(),
    }
}

fn rec(seq: u64, kind: &str) -> ehdb_l0::EventRecord {
    ehdb_l0::EventRecord::new(
        seq,
        format!("exec-{seq}"),
        kind,
        format!(r#"{{"event_type":"action_started","seq":{seq}}}"#),
    )
}

/// What a *freshly reopened* engine can see on `dir` — exactly what the
/// replacement pod does. It reads the manifest, so anything left in an unsealed
/// active part is invisible to it by construction.
async fn cmd_tip_after_reopen(dir: &std::path::Path) -> u64 {
    let (writer, _s) = spawn_writer_host(&cmd_config(dir, None)).await.unwrap();
    *writer.tip_receiver().borrow()
}

/// The reopened events log's tip, plus whether any group's resume had to clamp
/// its persisted cursor down to it. `clamped == true` is the prod symptom: the
/// log recovered *less* than the cursor covered.
async fn events_resume_after_reopen(dir: &std::path::Path) -> (u64, bool, u64) {
    let (coordinator, _s) = spawn_event_writer_host(&event_config(dir, None))
        .await
        .unwrap();
    let report = coordinator.open_group("noetl_materializer").await;
    let below = report.stored_cursor.unwrap_or(0).saturating_sub(report.tip);
    (report.tip, report.clamped(), below)
}

/// Append to both writers from many concurrent tasks until told to stop, so
/// SIGTERM lands with real work in flight rather than on a quiet writer. This is
/// the whole difference between this file and the idle suite that passed while
/// prod was losing records.
struct Backlog {
    stop: Arc<AtomicBool>,
    tasks: Vec<tokio::task::JoinHandle<(u64, u64)>>,
}

impl Backlog {
    fn spawn(
        cmd: Arc<ehdb_feed::FeedWriter<ehdb_l0::D1EventLog>>,
        events: Arc<ehdb_feed::FeedWriter<ehdb_l0::D1EventLog>>,
        writers: u64,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let tasks = (0..writers)
            .map(|w| {
                let (cmd, events, stop) = (cmd.clone(), events.clone(), Arc::clone(&stop));
                tokio::spawn(async move {
                    let (mut c, mut e) = (0u64, 0u64);
                    let mut seq = w * 100_000;
                    while !stop.load(Ordering::SeqCst) {
                        seq += 1;
                        // Both logs are loaded at once — the prod shape, where
                        // the command bus and the events feed are busy together.
                        if cmd.append(rec(seq, "command.issued")).is_ok() {
                            c += 1;
                        }
                        if events.append(rec(seq, "action_started")).is_ok() {
                            e += 1;
                        }
                        tokio::task::yield_now().await;
                    }
                    (c, e)
                })
            })
            .collect();
        Self { stop, tasks }
    }

    /// Stop appending and report how many records each log **acked**. Counted
    /// from the appenders' own return values, so a record only counts if its
    /// `append` returned `Ok` — which is the definition of acked, and therefore
    /// the definition of what must survive.
    async fn acked(self) -> (u64, u64) {
        self.stop.store(true, Ordering::SeqCst);
        let mut totals = (0u64, 0u64);
        for task in self.tasks {
            let (c, e) = tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .expect("an appender never returned — it is parked on the seal")
                .unwrap();
            totals.0 += c;
            totals.1 += e;
        }
        totals
    }
}

/// **The regression.** A backlog is in flight when shutdown runs; both hosts
/// must seal and both logs must reopen at exactly what they acked.
///
/// Sized under `seal_max_records` (1024) per log so nothing auto-seals along the
/// way — every record is sitting in the active part when shutdown runs, which
/// makes the whole batch precisely the tail that vanished in prod.
///
/// # Why the shutdown is watched from outside the runtime
///
/// Reintroduce the v5.92.0 seal and this does not merely fail — it **hangs**,
/// and a `tokio::time::timeout` around `seal_all` does not save it. Worker
/// threads drive the timer as well as the tasks, so appenders parked on the
/// leaked engine guard stop the timeout from ever firing. That is not a
/// hypothetical: it was measured on this test with the old seal restored, and it
/// is the same mechanism that swallowed `main`'s 15 s budget in prod, where the
/// expiry line never appeared in the log.
///
/// So completion is observed over a `std` channel from the test thread, which is
/// not a runtime worker. A regression then fails in 30 s with a message instead
/// of wedging CI — the diagnosis, rather than a timeout nobody reads.
#[test]
fn both_hosts_seal_with_a_backlog_in_flight() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let cmd_dir = unique_dir("cmd");
    let events_dir = unique_dir("events");

    let (backlog, hosts) = rt.block_on(async {
        let (cmd_writer, cmd_shutdown) = spawn_writer_host(&cmd_config(&cmd_dir, None))
            .await
            .unwrap();
        let (events_coord, events_shutdown) =
            spawn_event_writer_host(&event_config(&events_dir, None))
                .await
                .unwrap();
        let backlog = Backlog::spawn(cmd_writer.clone(), events_coord.writer(), 8);
        // Let a real backlog accumulate, then seal underneath it.
        tokio::time::sleep(Duration::from_millis(200)).await;
        (backlog, vec![cmd_shutdown, events_shutdown])
    });

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    rt.spawn(async move {
        let _ = done_tx.send(seal_all(&hosts).await);
    });
    let report = match done_rx.recv_timeout(Duration::from_secs(30)) {
        Ok(report) => report,
        Err(_) => {
            // The runtime is wedged; dropping it would hang the harness too.
            std::mem::forget(rt);
            panic!(
                "the shutdown sequence never finished. This is the noetl/ai-meta#226 \
                 failure exactly: under load the seal starved the runtime, the \
                 second host was never reached, and the budget meant to bound it \
                 could not fire because the same starvation stopped the clock"
            );
        }
    };

    assert_eq!(
        (report.sealed, report.hosts),
        (2, 2),
        "BOTH hosts must seal. Prod sealed 1 of 2 and said nothing: the command \
         bus sealed, the events feed never did, and the loss only surfaced on the \
         next boot as clamped=true"
    );

    let (cmd_acked, events_acked) = rt.block_on(backlog.acked());

    assert!(
        cmd_acked > 0 && events_acked > 0,
        "the harness must actually load both logs (cmd={cmd_acked}, events={events_acked})"
    );
    assert!(
        cmd_acked < 1024 && events_acked < 1024,
        "sized to stay under seal_max_records so nothing auto-sealed and the \
         whole batch is the unsealed tail (cmd={cmd_acked}, events={events_acked})"
    );

    let cmd_tip = rt.block_on(cmd_tip_after_reopen(&cmd_dir));
    assert_eq!(
        cmd_tip, cmd_acked,
        "the reopened command log saw {cmd_tip} of {cmd_acked} acked records"
    );

    let (events_tip, clamped, below) = rt.block_on(events_resume_after_reopen(&events_dir));
    assert_eq!(
        events_tip, events_acked,
        "the reopened events log saw {events_tip} of {events_acked} acked records \
         — prod's came back 390 short"
    );
    assert!(
        !clamped,
        "the events group resumed clamped: the log came back below its own \
         persisted cursor, which is the noetl/ai-meta#226 signature"
    );
    assert_eq!(below, 0, "records below the persisted cursor must be 0");

    let _ = std::fs::remove_dir_all(&cmd_dir);
    let _ = std::fs::remove_dir_all(&events_dir);
}

/// **Negative control for the sequencing half.** Sealing host-at-a-time — the
/// v5.92.0 shape — leaves host 2 accepting and appending for the whole of host
/// 1's sequence.
///
/// Measured, not asserted by construction: records land in the events log
/// *after* the command host's own sequence has completely finished. In prod
/// those are the records that had nowhere safe to go. If this ever stops
/// reproducing, host-at-a-time has stopped being observably different from
/// `seal_all` and the ordering half of the fix is no longer being measured.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn negative_control_host_at_a_time_lets_the_second_host_keep_appending() {
    let cmd_dir = unique_dir("seq-cmd");
    let events_dir = unique_dir("seq-events");

    let (cmd_writer, cmd_shutdown) = spawn_writer_host(&cmd_config(&cmd_dir, None))
        .await
        .unwrap();
    let (events_coord, events_shutdown) = spawn_event_writer_host(&event_config(&events_dir, None))
        .await
        .unwrap();
    let events_writer = events_coord.writer();
    // Subscribe **before** anything appends. `FeedWriter::append` publishes the
    // tip with `watch::Sender::send`, which is a no-op while no receiver is
    // alive, so a receiver created after the fact reads the value the writer was
    // seeded with at open — 0 — no matter how much has landed since.
    let tip = events_writer.tip_receiver();

    let backlog = Backlog::spawn(cmd_writer.clone(), events_writer.clone(), 8);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The old shape: run host 1's *entire* sequence first.
    let events_tip_before = *tip.borrow();
    cmd_shutdown.run().await;
    let events_tip_after = *tip.borrow();

    assert!(
        events_tip_after > events_tip_before,
        "host-at-a-time must let the events log keep growing through the command \
         host's whole sequence ({events_tip_before} -> {events_tip_after}); if it \
         does not, this control no longer measures the ordering defect"
    );

    // Clean up: seal the second host so the appenders are refused and return.
    let hosts = vec![events_shutdown];
    seal_all(&hosts).await;
    backlog.acked().await;

    let _ = std::fs::remove_dir_all(&cmd_dir);
    let _ = std::fs::remove_dir_all(&events_dir);
}

/// The idle path must stay exactly as green as it was — this is the case
/// v5.92.0 got right and the fix must not regress.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn both_hosts_still_seal_when_idle() {
    let cmd_dir = unique_dir("idle-cmd");
    let events_dir = unique_dir("idle-events");
    const RECORDS: u64 = 300;

    {
        let (cmd_writer, cmd_shutdown) = spawn_writer_host(&cmd_config(&cmd_dir, None))
            .await
            .unwrap();
        let (events_coord, events_shutdown) =
            spawn_event_writer_host(&event_config(&events_dir, None))
                .await
                .unwrap();
        let events_writer = events_coord.writer();

        for seq in 1..=RECORDS {
            cmd_writer.append(rec(seq, "command.issued")).unwrap();
            events_writer.append(rec(seq, "action_started")).unwrap();
        }

        let hosts = vec![cmd_shutdown, events_shutdown];
        let report = seal_all(&hosts).await;
        assert!(report.complete(), "idle: {report:?}");
    }

    assert_eq!(cmd_tip_after_reopen(&cmd_dir).await, RECORDS);
    let (tip, clamped, below) = events_resume_after_reopen(&events_dir).await;
    assert_eq!(tip, RECORDS);
    assert!(!clamped);
    assert_eq!(below, 0);

    let _ = std::fs::remove_dir_all(&cmd_dir);
    let _ = std::fs::remove_dir_all(&events_dir);
}

/// Ingest faces on both hosts: every listener must be closed before any engine
/// is sealed, so nothing can be accepted — and therefore acked — into a part
/// opened after the seal. The old sequence only guaranteed this for host 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_hosts_ingest_listener_closes_before_any_seal() {
    let cmd_dir = unique_dir("ports-cmd");
    let events_dir = unique_dir("ports-events");
    let (cmd_ingest, events_ingest) = (free_addr().await, free_addr().await);

    let (_cw, cmd_shutdown) = spawn_writer_host(&cmd_config(&cmd_dir, Some(cmd_ingest)))
        .await
        .unwrap();
    let (_ec, events_shutdown) =
        spawn_event_writer_host(&event_config(&events_dir, Some(events_ingest)))
            .await
            .unwrap();

    let hosts = vec![cmd_shutdown, events_shutdown];
    let report = seal_all(&hosts).await;
    assert!(report.complete(), "{report:?}");

    // Re-bind, not connect: a connect only reports that *someone* is listening.
    for (addr, which) in [(cmd_ingest, "command-bus"), (events_ingest, "events-feed")] {
        assert!(
            tokio::net::TcpListener::bind(addr).await.is_ok(),
            "the {which} ingest listener at {addr} was still open after shutdown \
             returned — a publisher could still be accepted and acked into a \
             post-seal part"
        );
    }

    let _ = std::fs::remove_dir_all(&cmd_dir);
    let _ = std::fs::remove_dir_all(&events_dir);
}
