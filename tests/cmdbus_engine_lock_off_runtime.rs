//! The command-bus writer must keep answering while the engine lock is held
//! (noetl/ai-meta#501).
//!
//! worker#385 moved the startup subject scan off the runtime and released the
//! engine `std::sync::Mutex` between 20k-record chunks. Prod (v6.10.0,
//! 2026-10-26 19:36, 2-CPU limit, TOKIO_WORKER_THREADS=8) still answered only
//! 12/25 1 Hz probes on :9090 and 21/25 on :9106 during the ~49 s scan: every
//! other face — the claims the system and user pools poll, the 2 s lag sampler,
//! the ingest committers, the age-seal sweep — still took the same Mutex
//! *inline on a runtime worker*, so each waited out every chunk with a worker
//! pinned.
//!
//! This holds the engine lock from a plain thread for seconds — the stand-in for
//! a scan chunk or any other long holder — while every face of a real writer
//! host runs into it, on a TWO-worker runtime, and requires:
//!
//! * no runtime worker is parked (rendezvous probe), and
//! * the writer's HTTP face answers every 1 s probe throughout;
//!
//! then that everything published during the hold is claimed once it ends.
//! Reverting the ehdb pin to v0.14.1 (the faces take the lock inline) fails it.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use ehdb_l0::EventRecord;
use noetl_worker::command_bus::{spawn_writer_host, CommandBusConfig, CommandBusMode};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: AtomicUsize = AtomicUsize::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("noetl-cmdbus-501-{tag}-{}-{n}", std::process::id()))
}

async fn free_addr() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

fn config_at(dir: &std::path::Path) -> CommandBusConfig {
    CommandBusConfig {
        mode: CommandBusMode::Ehdb,
        host: true,
        shard: 0,
        shard_count: 1,
        writer_dir: Some(dir.to_path_buf()),
        ingest_bind: None,
        claim_bind: None,
        metrics_bind: None,
        claim_addr: None,
        ack_wait: Duration::from_secs(30),
        cursor_persist: Duration::ZERO,
        cursor_fallback: Default::default(),
    }
}

fn cmd(i: u64, pool: &str) -> EventRecord {
    EventRecord::new(
        i,
        format!("exec-{i}"),
        "command.issued",
        format!("{{\"execution_pool\":\"{pool}\"}}"),
    )
}

/// Can BOTH runtime workers run a task at the same time, right now? Two tasks
/// rendezvous, spinning without yielding; a parked worker cannot pick up the
/// second one.
async fn both_workers_free(timeout: Duration) -> bool {
    let arrived = Arc::new(AtomicUsize::new(0));
    let spawn = |arrived: Arc<AtomicUsize>| {
        tokio::spawn(async move {
            arrived.fetch_add(1, Ordering::SeqCst);
            let t = Instant::now();
            while arrived.load(Ordering::SeqCst) < 2 {
                if t.elapsed() > timeout {
                    return false;
                }
                std::hint::spin_loop();
            }
            true
        })
    };
    let a = spawn(arrived.clone());
    let b = spawn(arrived.clone());
    a.await.unwrap_or(false) & b.await.unwrap_or(false)
}

/// One `GET /metrics` with a hard 1 s budget, from outside the runtime — the
/// prod `wget -T1` probe.
fn http_probe(addr: SocketAddr) -> bool {
    use std::io::{Read, Write};
    let Ok(mut s) = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(1)) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(1)));
    let _ = s.set_write_timeout(Some(Duration::from_secs(1)));
    if s.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut buf = [0u8; 64];
    matches!(s.read(&mut buf), Ok(n) if n > 0 && buf.starts_with(b"HTTP/1.1 200"))
}

/// Hold the engine lock on a plain thread for `hold`; returns once held.
fn hold_engine(
    writer: &Arc<ehdb_feed::FeedWriter<ehdb_l0::D1EventLog>>,
    hold: Duration,
) -> std::thread::JoinHandle<()> {
    let engine = writer.engine();
    let (tx, rx) = mpsc::channel();
    let t = std::thread::spawn(move || {
        let _g = engine.read();
        tx.send(()).unwrap();
        std::thread::sleep(hold);
    });
    rx.recv().unwrap();
    t
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_engine_lock_parks_no_runtime_worker_and_the_writer_keeps_answering() {
    let dir = unique_dir("held");
    let (metrics, claim, ingest) = (free_addr().await, free_addr().await, free_addr().await);
    let mut cfg = config_at(&dir);
    cfg.metrics_bind = Some(metrics);
    cfg.claim_bind = Some(claim);
    cfg.ingest_bind = Some(ingest);
    let (writer, shutdown) = spawn_writer_host(&cfg).await.unwrap();

    // The metrics face binds after the (empty) seed; wait for it.
    let deadline = Instant::now() + Duration::from_secs(30);
    while !http_probe(metrics) {
        assert!(Instant::now() < deadline, "writer /metrics never came up");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The prod claim load: several members, two pools, every one parked in a
    // claim — each re-polls the engine every 250 ms while idle.
    let pools = ["system", "shared", "shared", "system"];
    let mut claimers = Vec::new();
    for (m, pool) in pools.iter().enumerate() {
        let filter = format!("commands.{pool}.>");
        let mut c = ehdb_feed::ClaimClient::connect(claim, m as u32 + 1, filter)
            .await
            .expect("claim face reachable");
        claimers.push(tokio::spawn(async move {
            let got = c.claim_next::<EventRecord>().await.unwrap();
            c.ack(got.sort_key).await.unwrap();
            got.sort_key
        }));
    }
    // Two publishers, connected before the hold, publishing during it.
    let mut publishers = Vec::new();
    for _ in 0..2 {
        publishers.push(ehdb_feed::PublishClient::connect(ingest).await.unwrap());
    }
    // Let the claims and the 2 s sampler settle into their loops.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let hold = Duration::from_millis(4_500);
    let holder = hold_engine(&writer, hold);
    let mut published = Vec::new();
    for (i, mut p) in publishers.into_iter().enumerate() {
        published.push(tokio::spawn(async move {
            // Two per publisher: one per pool, so every claimer pool is fed.
            for (j, pool) in ["system", "shared"].iter().enumerate() {
                p.publish(&cmd(10 + (i * 2 + j) as u64, pool))
                    .await
                    .unwrap();
            }
            for (j, pool) in ["system", "shared"].iter().enumerate() {
                p.publish(&cmd(20 + (i * 2 + j) as u64, pool))
                    .await
                    .unwrap();
            }
        }));
    }

    // HTTP probe from a plain thread, 10 Hz, for the whole hold.
    let (answered, total, stop) = (
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicBool::new(false)),
    );
    let prober = {
        let (answered, total, stop) = (answered.clone(), total.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                total.fetch_add(1, Ordering::SeqCst);
                if http_probe(metrics) {
                    answered.fetch_add(1, Ordering::SeqCst);
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        })
    };

    // Runtime probe, same window.
    let (mut probes, mut held) = (0u32, 0u32);
    tokio::time::sleep(Duration::from_millis(150)).await;
    while !holder.is_finished() {
        if !both_workers_free(Duration::from_millis(400)).await {
            held += 1;
        }
        probes += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stop.store(true, Ordering::SeqCst);
    prober.join().unwrap();
    holder.join().unwrap();

    // Everything waiting on the lock completes once it is released.
    for p in published {
        tokio::time::timeout(Duration::from_secs(15), p)
            .await
            .expect("publishes complete after the hold")
            .unwrap();
    }
    for c in claimers {
        tokio::time::timeout(Duration::from_secs(15), c)
            .await
            .expect("every parked claimer is served after the hold")
            .unwrap();
    }

    let (a, n) = (
        answered.load(Ordering::SeqCst),
        total.load(Ordering::SeqCst),
    );
    eprintln!(
        "  {hold:?} engine-lock hold: runtime probes {probes} (held {held}); \
         HTTP /metrics answered {a}/{n} within 1 s"
    );
    assert!(probes >= 5 && n >= 10, "too few probes ({probes}, {n})");
    assert_eq!(
        held, 0,
        "{held}/{probes} probes found a runtime worker parked on the engine Mutex"
    );
    assert_eq!(
        a,
        n,
        "the writer's HTTP face missed {} of {n} 1 s probes while the engine lock was held",
        n - a
    );

    shutdown.run().await;
    let _ = std::fs::remove_dir_all(&dir);
}
