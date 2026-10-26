//! The command-bus claim loop recovers from a writer restart even when the bus
//! is idle — and still reports a frozen writer as wedged.
//!
//! Kind, 2026-10-23, with the state-builder fix already in: deleting the writer
//! pod still killed the system pool, now on the claim-loop predicate. The loop
//! records a failure per refused connect while the writer is down; once it is
//! back the loop reconnects and parks on an idle bus, and since only a CLAIM
//! counted as progress (#297) it stayed "wedged" for
//! `NOETL_CLAIM_LOOP_UNHEALTHY_SECS` and `/livez` failed until the kubelet
//! killed it. The fix bounds the first post-failure park and asks the client
//! whether the coordinator heartbeated: alive-and-idle clears the streak,
//! silence (a frozen writer) stays a failure.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use noetl_executor::worker::source::CommandSource;
use noetl_worker::client::ControlPlaneClient;
use noetl_worker::command_bus::{
    spawn_writer_host, CommandBusConfig, CommandBusMode, EhdbCommandSource,
};
use noetl_worker::metrics;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const WINDOW: u64 = 180;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "noetl-claim-restart-{tag}-{}-{n}",
        std::process::id()
    ))
}

async fn free_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

/// A real command-bus writer host with its claim face on `claim`.
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

/// Test-controlled proxy. `blackhole` = accept connections but never forward
/// a byte: a frozen writer whose kernel still completes the handshake.
struct Proxy {
    addr: SocketAddr,
    upstream: SocketAddr,
    blackhole: Arc<AtomicBool>,
    conns: Arc<Mutex<Vec<JoinHandle<()>>>>,
    acceptor: Mutex<Option<JoinHandle<()>>>,
}

impl Proxy {
    async fn start(upstream: SocketAddr) -> Self {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p = Proxy {
            addr: l.local_addr().unwrap(),
            upstream,
            blackhole: Arc::new(AtomicBool::new(false)),
            conns: Arc::new(Mutex::new(Vec::new())),
            acceptor: Mutex::new(None),
        };
        p.spawn_acceptor(l);
        p
    }
    fn spawn_acceptor(&self, l: TcpListener) {
        let (upstream, blackhole, conns) =
            (self.upstream, self.blackhole.clone(), self.conns.clone());
        let h = tokio::spawn(async move {
            loop {
                let Ok((mut client, _)) = l.accept().await else {
                    return;
                };
                let hole = blackhole.load(Ordering::SeqCst);
                let h = tokio::spawn(async move {
                    if hole {
                        // Hold the socket open and say nothing, forever.
                        let mut buf = [0u8; 1024];
                        loop {
                            match tokio::io::AsyncReadExt::read(&mut client, &mut buf).await {
                                Ok(0) | Err(_) => return,
                                Ok(_) => continue,
                            }
                        }
                    }
                    if let Ok(mut server) = TcpStream::connect(upstream).await {
                        let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                    }
                });
                conns.lock().unwrap().push(h);
            }
        });
        *self.acceptor.lock().unwrap() = Some(h);
    }
    fn outage(&self) {
        if let Some(a) = self.acceptor.lock().unwrap().take() {
            a.abort();
        }
        for c in self.conns.lock().unwrap().drain(..) {
            c.abort();
        }
    }
    async fn restore(&self, blackhole: bool) {
        self.blackhole.store(blackhole, Ordering::SeqCst);
        let l = loop {
            match TcpListener::bind(self.addr).await {
                Ok(l) => break l,
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        };
        self.spawn_acceptor(l);
    }
}

fn set_env() {
    unsafe {
        std::env::set_var("NOETL_CLAIM_PROBE_SECS", "4");
        std::env::set_var("NOETL_EHDB_CONNECT_TIMEOUT_MS", "1000");
    }
}

/// Run the production claim loop forever against `addr` (it parks on an idle
/// bus; `next()` only returns on a claim).
fn spawn_claim_loop(addr: SocketAddr) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut src = EhdbCommandSource::new(
            addr.to_string(),
            "commands.>".to_string(),
            "claim-restart-test".to_string(),
            ControlPlaneClient::new("http://127.0.0.1:1"),
        );
        loop {
            let _ = src.next().await;
        }
    })
}

/// Drive the loop into a failure streak with a writer outage.
async fn outage_until_wedged(proxy: &Proxy) {
    proxy.outage();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !metrics::claim_loop_wedged(WINDOW) {
        assert!(
            Instant::now() < deadline,
            "an outage must put the claim loop in a failure streak"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// ⭐ Writer back, bus idle: the loop proves the coordinator alive by its
/// heartbeats and stops reporting wedged — no claim required.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_claim_loop_recovers_after_a_writer_restart() {
    let _g = SERIAL.lock().await;
    set_env();
    let claim = free_addr().await;
    let _w = writer("idle", claim).await;
    let proxy = Proxy::start(claim).await;
    let lp = spawn_claim_loop(proxy.addr);
    tokio::time::sleep(Duration::from_millis(500)).await;

    outage_until_wedged(&proxy).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    proxy.restore(false).await;

    // probe window (4 s) + a heartbeat interval + slack.
    let deadline = Instant::now() + Duration::from_secs(15);
    while metrics::claim_loop_wedged(WINDOW) {
        assert!(
            Instant::now() < deadline,
            "the writer is back and heartbeating, but the idle claim loop still reports \
             wedged — /livez would kill a healthy pool on every writer roll"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    lp.abort();
}

/// The #297 guard is intact: a writer that accepts connections but never
/// heartbeats (frozen) must keep the loop wedged — the probe must not mistake
/// silence for health.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_frozen_writer_still_reads_as_wedged() {
    let _g = SERIAL.lock().await;
    set_env();
    let claim = free_addr().await;
    let _w = writer("frozen", claim).await;
    let proxy = Proxy::start(claim).await;
    let lp = spawn_claim_loop(proxy.addr);
    tokio::time::sleep(Duration::from_millis(500)).await;

    outage_until_wedged(&proxy).await;
    proxy.restore(true).await; // comes back as a black hole

    // Two full probe windows of silence: still wedged throughout.
    let until = Instant::now() + Duration::from_secs(10);
    while Instant::now() < until {
        assert!(
            metrics::claim_loop_wedged(WINDOW),
            "a frozen (accepting, silent) writer must stay wedged — the probe treated \
             silence as health"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    lp.abort();
}
