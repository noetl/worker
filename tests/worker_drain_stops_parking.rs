//! A SIGTERM'd worker stops taking commands without dropping one it already
//! claimed (noetl/ai-meta#511).
//!
//! Prod, 2026-11-03: a KEDA scale-in killed a worker 3.4 s into an LLM call and
//! the orphan guardrail failed the execution ~3.5 min later. The fix drains: the
//! pull loop stops claiming and the running commands finish first. That needs
//! the command source to give up an idle park on the drain signal — the bus
//! park is unbounded on an idle bus, so without it the drain waits forever on a
//! claim that will never come — while never cancelling the server-side claim.
//!
//! These run the production `EhdbCommandSource` against a real command-bus
//! writer host. The control test (no drain wired) is what gives the second one
//! its power: an idle park that returned anyway would make it pass vacuously.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use noetl_executor::worker::source::CommandSource;
use noetl_worker::client::ControlPlaneClient;
use noetl_worker::command_bus::{
    spawn_writer_host, CommandBusConfig, CommandBusMode, EhdbCommandSource,
};
use tokio::net::TcpListener;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("noetl-drain-{tag}-{}-{n}", std::process::id()))
}

async fn free_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

/// A real, idle command-bus writer host with its claim face on `claim`.
async fn writer(tag: &str, claim: SocketAddr) -> noetl_worker::graceful::WriterShutdown {
    let cfg = CommandBusConfig {
        mode: CommandBusMode::Ehdb,
        host: true,
        shard: 0,
        shard_count: 1,
        writer_dir: Some(unique_dir(tag)),
        ingest_bind: None,
        claim_bind: Some(claim),
        metrics_bind: None,
        claim_addr: None,
        ack_wait: Duration::from_secs(30),
        cursor_persist: Duration::ZERO,
        cursor_fallback: Default::default(),
    };
    let (_writer, shutdown) = spawn_writer_host(&cfg).await.unwrap();
    shutdown
}

fn source(addr: SocketAddr) -> EhdbCommandSource {
    EhdbCommandSource::new(
        addr.to_string(),
        "commands.>".to_string(),
        "drain-test".to_string(),
        ControlPlaneClient::new("http://127.0.0.1:1"),
    )
}

/// Draining before the pull: nothing new is taken, and the source does not even
/// dial — the address here has nothing listening, so a dial would spin in the
/// connect-retry loop instead of returning.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_draining_source_takes_nothing_and_does_not_dial() {
    let _g = SERIAL.lock().await;
    let (tx, rx) = tokio::sync::watch::channel(true);
    let mut src = source(free_addr().await).with_drain(rx);
    let got = tokio::time::timeout(Duration::from_secs(2), src.next())
        .await
        .expect("a draining source must answer at once, not dial");
    assert!(
        got.unwrap().is_none(),
        "a draining source must take nothing new"
    );
    drop(tx);
}

/// ⭐ The case that wedges a drain: parked on an idle bus when SIGTERM arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_drain_signal_ends_an_idle_park() {
    let _g = SERIAL.lock().await;
    let claim = free_addr().await;
    let _w = writer("idle-park", claim).await;
    let (tx, rx) = tokio::sync::watch::channel(false);
    let mut src = source(claim).with_drain(rx);

    let pulled = tokio::spawn(async move { src.next().await });
    // Let it connect and park.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !pulled.is_finished(),
        "an idle bus must park, or this test has no power"
    );

    let signalled = Instant::now();
    tx.send_replace(true);
    let got = tokio::time::timeout(Duration::from_secs(3), pulled)
        .await
        .expect(
            "the drain signal must end an idle park — otherwise a SIGTERM'd worker \
             waits on a bus that may never deliver again (noetl/ai-meta#511)",
        )
        .unwrap();
    assert!(got.unwrap().is_none(), "an interrupted park takes nothing");
    assert!(signalled.elapsed() < Duration::from_secs(1));
}

/// Control: with no drain wired the same idle park does NOT return. Without
/// this, the test above could pass because idle parks return on their own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_a_drain_an_idle_park_keeps_parking() {
    let _g = SERIAL.lock().await;
    let claim = free_addr().await;
    let _w = writer("control", claim).await;
    let mut src = source(claim);
    let parked = tokio::time::timeout(Duration::from_secs(3), src.next()).await;
    assert!(
        parked.is_err(),
        "an idle park must not return without a drain signal"
    );
}
