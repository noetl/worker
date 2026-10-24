//! The command-bus subject seed must not freeze the writer's runtime
//! (prod 2026-10-24, the writer restart at 01:27:44Z).
//!
//! The seed enumerates every routing subject in the retained command log so the
//! first `/metrics` scrape carries every pool at 0 (noetl/ai-meta#194 — a missing
//! label is a KEDA scaler error, not a backlog of 0). It used to be
//! `ClaimCoordinator::seed_subjects().await`, which reads the whole partition in
//! ONE call while holding the engine `std::sync::Mutex`, synchronously on a Tokio
//! worker thread. The writer runs two worker threads. The first claim then parked
//! the second thread on the same Mutex, and the whole runtime stopped for the
//! scan: on prod the log went silent from +12.5 s to +53.7 s at ≤16 % CPU, the
//! events-feed WAL fan-out (a different, uncontended engine) could not deliver,
//! and the system pools' resume record arrived 59 s late.
//!
//! These tests run on exactly that shape — a multi-thread runtime with TWO
//! workers — over a log big enough that the scan costs seconds, and pin three
//! things that each fail against a specific revert:
//!
//! * no runtime worker is held for the scan (fails if the seed goes back onto a
//!   runtime thread, chunked or not);
//! * a command published mid-seed is claimed promptly (fails if the scan goes
//!   back to one unbounded read under the lock, wherever it runs);
//! * the first scrape still names every seeded subject at 0 (fails if the seed
//!   is dropped or truncated).

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ehdb_l0::EventRecord;
use noetl_worker::command_bus::{spawn_writer_host, CommandBusConfig, CommandBusMode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: AtomicUsize = AtomicUsize::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "noetl-cmdbus-seed-{tag}-{}-{n}",
        std::process::id()
    ))
}

async fn free_addr() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

/// Scrape `/metrics`, retrying the connect until the face is bound (it binds
/// only after the seed). Gives up after `within`.
async fn scrape(addr: SocketAddr, within: Duration) -> String {
    let deadline = Instant::now() + within;
    let mut sock = loop {
        match TcpStream::connect(addr).await {
            Ok(s) => break s,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await
            }
            Err(e) => panic!("writer /metrics never bound within {within:?}: {e}"),
        }
    };
    sock.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let mut resp = String::new();
    sock.read_to_string(&mut resp).await.unwrap();
    resp
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

/// Seeded pools: present all through the log.
const POOLS: [&str; 4] = ["pool-a", "pool-b", "pool-c", "pool-d"];
/// Routed by exactly ONE record, the last one — a scan that stops early (a
/// cursor bug, a truncated read) loses it.
const RARE_POOL: &str = "pool-rare";

/// Build a retained command log of `n` records and close it cleanly. The
/// claim cursor is removed afterwards so the next open resumes at the TAIL —
/// the prod shape, where the committed cursor sits near the tip and the seed is
/// the only thing that reads the whole log.
async fn build_log(dir: &std::path::Path, n: u64) {
    let (writer, shutdown) = spawn_writer_host(&config_at(dir)).await.unwrap();
    let pad = "x".repeat(160);
    let w = writer.clone();
    // Batched: one lock + one durable write per 5k records, not per record.
    tokio::task::spawn_blocking(move || {
        let mut batch = Vec::with_capacity(5_000);
        for i in 0..n {
            let pool = if i + 1 == n {
                RARE_POOL
            } else {
                POOLS[(i % POOLS.len() as u64) as usize]
            };
            batch.push(EventRecord::new(
                i + 1,
                format!("exec-{i}"),
                "command.issued",
                format!("{{\"execution_pool\":\"{pool}\",\"pad\":\"{pad}\"}}"),
            ));
            if batch.len() == 5_000 || i + 1 == n {
                w.append_batch(std::mem::take(&mut batch)).unwrap();
            }
        }
    })
    .await
    .unwrap();
    shutdown.run().await;
    let _ = std::fs::remove_file(dir.join("claim-cursor.shard-0.json"));
}

/// Records in the test log. Sized so the full scan costs seconds on a laptop
/// and in CI; the test asserts that it did (the denominator) rather than trust
/// this number.
fn log_len() -> u64 {
    std::env::var("NOETL_TEST_SEED_LOG_LEN")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(250_000)
}

/// Can BOTH runtime workers run a task at the same time, right now?
///
/// Two tasks rendezvous: each checks in, then spins (it must not yield — a
/// yielding task would let one thread run both) until the other has checked in.
/// If a worker is held — by a synchronous scan, or parked on a `std` Mutex —
/// the second task cannot start and the first times out.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_subject_seed_holds_no_runtime_worker_and_does_not_block_claims() {
    let dir = unique_dir("off-runtime");
    build_log(&dir, log_len()).await;

    let metrics = free_addr().await;
    let claim = free_addr().await;
    let mut cfg = config_at(&dir);
    cfg.metrics_bind = Some(metrics);
    cfg.claim_bind = Some(claim);

    // ── Probe the runtime from before the seed starts until the face binds.
    let done = Arc::new(AtomicBool::new(false));
    let probe = {
        let done = done.clone();
        tokio::spawn(async move {
            let (mut probes, mut held, mut max_gap) = (0u32, 0u32, Duration::ZERO);
            let mut last = Instant::now();
            while !done.load(Ordering::SeqCst) {
                if !both_workers_free(Duration::from_millis(750)).await {
                    held += 1;
                }
                probes += 1;
                tokio::time::sleep(Duration::from_millis(25)).await;
                max_gap = max_gap.max(last.elapsed());
                last = Instant::now();
            }
            (probes, held, max_gap)
        })
    };

    let t0 = Instant::now();
    let (writer, shutdown) = spawn_writer_host(&cfg).await.unwrap();

    // ── Mid-seed: publish one command and time how long until it is claimed.
    // The append runs off the runtime (it takes the engine lock); the claim
    // is the real client over the real face.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let mut client = ehdb_feed::ClaimClient::connect(claim, 1, "commands.live.>")
        .await
        .expect("claim face reachable");
    let published = Instant::now();
    let seed_running_at_publish = TcpStream::connect(metrics).await.is_err();
    {
        let w = writer.clone();
        tokio::task::spawn_blocking(move || {
            w.append(EventRecord::new(
                log_len() + 1,
                "exec-live",
                "command.issued",
                "{\"execution_pool\":\"live\"}",
            ))
            .unwrap();
        });
    }
    let got = tokio::time::timeout(Duration::from_secs(120), client.claim_next::<EventRecord>())
        .await
        .expect("the mid-seed command was claimed at all")
        .expect("claim succeeded");
    let claim_latency = published.elapsed();
    client.ack(got.sort_key).await.unwrap();

    // ── The face binds once the seed is done: that is the seed's cost.
    let body = scrape(metrics, Duration::from_secs(180)).await;
    let seed_cost = t0.elapsed();
    done.store(true, Ordering::SeqCst);
    let (probes, held, max_gap) = probe.await.unwrap();

    eprintln!(
        "  seed ~{seed_cost:?}; mid-seed claim in {claim_latency:?}; \
         runtime probes {probes} (held {held}), max probe gap {max_gap:?}"
    );

    // ⚠ The denominator. On a log the scan reads in milliseconds every
    // assertion below passes against the regression, so require that the seed
    // genuinely took long, and that the command really landed while it ran.
    assert!(
        seed_cost >= Duration::from_millis(1_500),
        "the seed must cost seconds for this test to mean anything — raise \
         NOETL_TEST_SEED_LOG_LEN (seed took {seed_cost:?})"
    );
    assert!(
        seed_running_at_publish,
        "the live command must be published while the seed is still running"
    );

    // ⭐ 1. No runtime worker held. Every probe across the seed saw both
    // workers free, and none of them waited behind a frozen runtime.
    assert_eq!(
        held, 0,
        "{held}/{probes} probes found a runtime worker held during the seed — \
         the scan (or a claim parked on the engine Mutex) is on a runtime thread"
    );
    assert!(
        max_gap * 4 < seed_cost,
        "the runtime stalled for {max_gap:?} during a {seed_cost:?} seed"
    );

    // ⭐ 2. The claim interleaved with the scan instead of waiting it out.
    assert!(
        claim_latency * 4 < seed_cost,
        "a command published mid-seed took {claim_latency:?} to claim against a \
         {seed_cost:?} seed — the scan holds the engine lock for the whole log"
    );

    // ⭐ 3. And the #194 contract: the first scrape names every pool at 0.
    for pool in POOLS.iter().chain([&RARE_POOL]) {
        let line = format!("ehdb_feed_subject_lag{{subject=\"commands.{pool}.shard.0\"}} 0\n");
        assert!(
            body.contains(&line),
            "the first scrape after restart is missing `{}` — KEDA reads an \
             absent label as a scaler error. body:\n{body}",
            line.trim_end()
        );
    }

    drop(client);
    shutdown.run().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// The seeded set is exact: every subject the log ever routed, spanning many
/// scan chunks, including one that appears only in the final record — and the
/// sampler's later ticks keep them (the coordinator itself is no longer seeded,
/// so a tick that dropped the merge would erase the labels 2 s after boot).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_routed_subject_survives_past_the_first_sampler_tick() {
    let dir = unique_dir("complete");
    build_log(&dir, 25_000).await;

    let metrics = free_addr().await;
    let mut cfg = config_at(&dir);
    cfg.metrics_bind = Some(metrics);
    let (_writer, shutdown) = spawn_writer_host(&cfg).await.unwrap();

    let first = scrape(metrics, Duration::from_secs(60)).await;
    // The sampler refreshes every 2 s; wait past two ticks.
    tokio::time::sleep(Duration::from_millis(4_500)).await;
    let later = scrape(metrics, Duration::from_secs(5)).await;

    for (when, body) in [("first", &first), ("after the sampler ticked", &later)] {
        for pool in POOLS.iter().chain([&RARE_POOL]) {
            let line = format!("ehdb_feed_subject_lag{{subject=\"commands.{pool}.shard.0\"}} 0\n");
            assert!(
                body.contains(&line),
                "{when} scrape is missing `{}`. body:\n{body}",
                line.trim_end()
            );
        }
    }

    shutdown.run().await;
    let _ = std::fs::remove_dir_all(&dir);
}
