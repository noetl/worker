//! The **deployed** writer's `/metrics` surface — noetl/ai-meta#194 / #210.
//!
//! The per-subject lag family is not a nice-to-have gauge: it is the value the
//! user pool's KEDA ScaledObject triggers on after the EHDB command-bus cutover,
//! and KEDA's `metrics-api` scaler in `format: prometheus` has no label selector
//! — it prefix-matches `valueLocation` against the whole `name{labels}` token and
//! takes the first hit. Two consequences this test pins, both of which are
//! silent failures rather than errors:
//!
//! 1. A writer that binds a lag-only endpoint serves no `ehdb_feed_subject_lag`
//!    line at all, and a `valueLocation` matching nothing is a KEDA *scaler
//!    error*, not a backlog of 0 — the pool falls back instead of scaling.
//! 2. A freshly-restarted writer that has not seeded its subject set reports an
//!    empty label set until each pool's next command arrives — the same failure,
//!    in exactly the window after a restart when the autoscaler most needs a
//!    reading.
//!
//! The unit-level shape lives in noetl/ehdb (`subject_lag.rs`); this drives the
//! real `spawn_writer_host` wiring the pod actually runs.

use std::net::SocketAddr;
use std::time::Duration;

use noetl_worker::command_bus::{spawn_writer_host, CommandBusConfig, CommandBusMode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "noetl-cmdbus-metrics-{tag}-{}-{n}",
        std::process::id()
    ))
}

async fn free_addr() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

async fn scrape(addr: SocketAddr) -> String {
    let mut sock = {
        let mut attempt = None;
        for _ in 0..200 {
            match TcpStream::connect(addr).await {
                Ok(s) => {
                    attempt = Some(s);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
        attempt.expect("writer /metrics endpoint accepted a connection")
    };
    sock.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    sock.flush().await.unwrap();
    let mut resp = String::new();
    sock.read_to_string(&mut resp).await.unwrap();
    resp
}

fn config_at(dir: &std::path::Path, metrics: SocketAddr) -> CommandBusConfig {
    CommandBusConfig {
        mode: CommandBusMode::Ehdb,
        host: true,
        shard: 0,
        shard_count: 1,
        writer_dir: Some(dir.to_path_buf()),
        ingest_bind: None,
        claim_bind: None,
        metrics_bind: Some(metrics),
        claim_addr: None,
        ack_wait: Duration::from_secs(30),
        cursor_persist: Duration::ZERO,
        cursor_fallback: Default::default(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_writer_metrics_endpoint_serves_the_per_pool_trigger_and_the_resume_facts() {
    let dir = unique_dir("families");
    let addr = free_addr().await;
    let _writer = spawn_writer_host(&config_at(&dir, addr)).await.unwrap();

    let resp = scrape(addr).await;
    assert!(resp.starts_with("HTTP/1.1 200 OK"), "got: {resp}");

    // The per-pool autoscaler trigger family must be present, with its HELP/TYPE
    // headers, even on a writer that has never carried a command.
    assert!(
        resp.contains("# TYPE ehdb_feed_subject_lag gauge"),
        "per-subject family missing from the deployed writer endpoint: {resp}"
    );
    // The restart verdict (noetl/ai-meta#208 follow-up) rides the same scrape.
    assert!(
        resp.contains("ehdb_feed_shard_resume_replay_records{shard=\"0\"}"),
        "resume facts missing from the deployed writer endpoint: {resp}"
    );
    // And the families the existing ScaledObject + runbooks already read.
    assert!(resp.contains("ehdb_feed_total_lag "), "{resp}");
    assert!(
        resp.contains("ehdb_feed_shard_committed{shard=\"0\"}"),
        "{resp}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_writer_reports_a_drained_pool_as_zero_rather_than_omitting_it() {
    // The regression that would strand the user pool: restart the writer on a log
    // that already carries both pools' traffic, and the first scrape must already
    // name each subject at 0 — not wait for the next command of each pool. An
    // omitted series is a scaler error, so "absent" and "0" are opposite outcomes
    // for the ScaledObject.
    let dir = unique_dir("seeded");

    // First incarnation: route one command to each pool, then let it go away.
    {
        let addr = free_addr().await;
        let writer = spawn_writer_host(&config_at(&dir, addr)).await.unwrap();
        for (seq, pool) in [(1u64, "shared"), (2, "system")] {
            writer
                .append(ehdb_l0::EventRecord::new(
                    seq,
                    format!("exec-{pool}"),
                    "command.issued",
                    format!("{{\"pool\":\"{pool}\"}}"),
                ))
                .unwrap();
        }
        // Seal, so the second incarnation's engine recovers the records from its
        // durable manifest — the same thing the SIGTERM path does in the pod.
        writer
            .engine()
            .lock()
            .unwrap()
            .flush_and_wait_uploads()
            .unwrap();
    }

    // Second incarnation: same directory, fresh process-equivalent wiring.
    let addr = free_addr().await;
    let _writer = spawn_writer_host(&config_at(&dir, addr)).await.unwrap();
    let resp = scrape(addr).await;

    assert!(
        resp.contains("ehdb_feed_subject_lag{subject=\"commands."),
        "a restarted writer reported no subjects at all — the autoscaler would see \
         a scaler error, not a backlog: {resp}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
