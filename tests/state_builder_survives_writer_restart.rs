//! The off-server state builder survives a writer restart without a cold
//! replay, and without failing `/livez`.
//!
//! Prod 2026-10-22 and kind both showed the old behaviour: on a writer pod
//! replacement the EHDB WAL drain flipped `state_builder_healthy` to 0 at the
//! first dropped read, then sat in an unbounded TCP connect to the dead pod's IP;
//! the kubelet killed the pool on `/livez` (kind: restart 34→35) and it replayed
//! the whole log from 0 (~660k events, ~10 min of `WAL chain incomplete`).
//!
//! The writer here is a real `FeedWriter` + `ehdb_feed::serve`; a test-owned TCP
//! proxy sits in front of it so the test can drop every connection and refuse new
//! ones (a writer restart), or repoint at a different writer (a regressed log).
//! The drain under test is the production `run_drain_loop_ehdb`.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ehdb_feed::FeedWriter;
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate};
use noetl_worker::event_bus::EventSourceMode;
use noetl_worker::metrics;
use noetl_worker::state_builder::{
    run_drain_loop_ehdb, BuilderMode, DrainConfig, SharedWalIndex, WalEventIndex,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// The metrics are process-global; the scenarios run one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("noetl-sb-restart-{tag}-{}-{n}", std::process::id()))
}

/// A real events writer serving its WAL feed on a loopback port.
struct Writer {
    feed: Arc<FeedWriter<D1EventLog>>,
    addr: SocketAddr,
    _serve: JoinHandle<()>,
}

async fn writer(tag: &str) -> Writer {
    let dir = unique_dir(tag);
    std::fs::create_dir_all(&dir).unwrap();
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&dir).unwrap());
    let engine =
        L0Engine::<D1EventLog>::open(L0Config::d1(&dir).with_shard_count(1), store).unwrap();
    let feed = Arc::new(FeedWriter::new(engine));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let f = feed.clone();
    let _serve = tokio::spawn(async move {
        let _ = ehdb_feed::serve(f, listener).await;
    });
    Writer { feed, addr, _serve }
}

/// Append `n` index-able events for `execution_id`, event ids from `first_event`.
fn append(w: &Writer, execution_id: i64, first_event: i64, n: i64, txn_tag: &str) {
    for i in 0..n {
        let eid = first_event + i;
        let payload = format!(
            r#"{{"event_id":{eid},"execution_id":{execution_id},"event_type":"step.enter"}}"#
        );
        w.feed
            .append(EventRecord::new(
                0,
                execution_id.to_string(),
                format!("{txn_tag}-{eid}"),
                payload,
            ))
            .unwrap();
    }
}

/// A TCP proxy the test controls: `outage()` drops every live connection and
/// refuses new ones (a writer pod going away); `restore()` serves again;
/// `repoint()` sends new connections to a different writer.
struct Proxy {
    addr: SocketAddr,
    upstream: Arc<Mutex<SocketAddr>>,
    up: Arc<AtomicBool>,
    /// Hold the writer->drain direction of each NEW connection this long before
    /// forwarding: a freshly restarted writer that is slow to serve its first
    /// read (the prod 2026-10-23 shape). Heartbeats are held too.
    hold_first: Arc<Mutex<Duration>>,
    /// When the last connection was accepted (to sample health mid-wait).
    last_accept: Arc<Mutex<Option<Instant>>>,
    conns: Arc<Mutex<Vec<JoinHandle<()>>>>,
    acceptor: Mutex<Option<JoinHandle<()>>>,
}

impl Proxy {
    async fn start(upstream: SocketAddr) -> Self {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let p = Proxy {
            addr,
            upstream: Arc::new(Mutex::new(upstream)),
            up: Arc::new(AtomicBool::new(true)),
            hold_first: Arc::new(Mutex::new(Duration::ZERO)),
            last_accept: Arc::new(Mutex::new(None)),
            conns: Arc::new(Mutex::new(Vec::new())),
            acceptor: Mutex::new(None),
        };
        p.spawn_acceptor(l);
        p
    }

    fn spawn_acceptor(&self, l: TcpListener) {
        let upstream = self.upstream.clone();
        let conns = self.conns.clone();
        let hold_first = self.hold_first.clone();
        let last_accept = self.last_accept.clone();
        let h = tokio::spawn(async move {
            loop {
                let Ok((client, _)) = l.accept().await else {
                    return;
                };
                *last_accept.lock().unwrap() = Some(Instant::now());
                let target = *upstream.lock().unwrap();
                let hold = *hold_first.lock().unwrap();
                let h = tokio::spawn(async move {
                    if let Ok(server) = TcpStream::connect(target).await {
                        let (mut cr, mut cw) = client.into_split();
                        let (mut sr, mut sw) = server.into_split();
                        let up = tokio::spawn(async move {
                            let _ = tokio::io::copy(&mut cr, &mut sw).await;
                        });
                        tokio::time::sleep(hold).await;
                        let _ = tokio::io::copy(&mut sr, &mut cw).await;
                        up.abort();
                    }
                });
                conns.lock().unwrap().push(h);
            }
        });
        *self.acceptor.lock().unwrap() = Some(h);
    }

    /// Drop every connection and close the port: connects are refused.
    fn outage(&self) {
        self.up.store(false, Ordering::SeqCst);
        if let Some(a) = self.acceptor.lock().unwrap().take() {
            a.abort();
        }
        for c in self.conns.lock().unwrap().drain(..) {
            c.abort();
        }
    }

    /// Serve again on the SAME port, as a replaced writer pod does.
    async fn restore(&self) {
        let l = loop {
            match TcpListener::bind(self.addr).await {
                Ok(l) => break l,
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        };
        self.up.store(true, Ordering::SeqCst);
        self.spawn_acceptor(l);
    }

    fn repoint(&self, upstream: SocketAddr) {
        *self.upstream.lock().unwrap() = upstream;
    }

    fn hold_first_delivery(&self, d: Duration) {
        *self.hold_first.lock().unwrap() = d;
    }

    fn last_accept(&self) -> Option<Instant> {
        *self.last_accept.lock().unwrap()
    }
}

fn drain_config(addr: SocketAddr) -> DrainConfig {
    DrainConfig {
        mode: BuilderMode::Shadow,
        nats_url: String::new(),
        nats_user: None,
        nats_password: None,
        stream: "noetl_events".to_string(),
        durable: None,
        batch: 500,
        timeout_ms: 200,
        idle_sleep: Duration::from_millis(10),
        source: EventSourceMode::Ehdb,
        wal_addr: Some(addr.to_string()),
    }
}

async fn event_count(index: &SharedWalIndex) -> usize {
    index.lock().await.event_count()
}

async fn wait_for(index: &SharedWalIndex, want: usize, within: Duration) -> usize {
    let deadline = Instant::now() + within;
    loop {
        let n = event_count(index).await;
        if n >= want || Instant::now() >= deadline {
            return n;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn set_test_env() {
    // Short bounds so the scenarios run in seconds; the grace stays the
    // production default (45 s), which is the point of the outage test.
    unsafe {
        std::env::set_var("NOETL_EHDB_CONNECT_TIMEOUT_MS", "1000");
        std::env::set_var("NOETL_STATE_BUILDER_RESUME_VERIFY_MS", "1500");
    }
}

struct Counts {
    cold: u64,
    resume: u64,
    regression: u64,
    wal: u64,
}
fn counts() -> Counts {
    Counts {
        cold: metrics::state_builder_wal_resubscribes("cold"),
        resume: metrics::state_builder_wal_resubscribes("resume"),
        regression: metrics::state_builder_wal_resubscribes("regression_replay"),
        wal: metrics::state_builder_wal_events(),
    }
}

/// ⭐ A short writer outage: the drain resumes from its last record — no second
/// full replay — and `state_builder_healthy` never drops, so `/livez` stays green.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_restart_resumes_without_a_replay_and_stays_healthy() {
    let _g = SERIAL.lock().await;
    set_test_env();
    let w = writer("resume").await;
    let proxy = Proxy::start(w.addr).await;
    let index = SharedWalIndex::new(WalEventIndex::new());
    let before = counts();

    const N: i64 = 400;
    const M: i64 = 150;
    append(&w, 7, 1, N, "a");
    let drain = tokio::spawn(run_drain_loop_ehdb(drain_config(proxy.addr), index.clone()));
    assert_eq!(
        wait_for(&index, N as usize, Duration::from_secs(10)).await,
        N as usize,
        "the initial replay must index all {N} events"
    );
    let after_cold = counts();
    assert_eq!(
        after_cold.cold - before.cold,
        1,
        "exactly one cold (replay-from-0) subscribe at start"
    );

    // The outage: every connection dropped, the port refusing, for ~3 s — the
    // shape of a writer pod replacement. Appends continue on the writer.
    proxy.outage();
    let mut healthy_throughout = true;
    let outage_until = Instant::now() + Duration::from_secs(3);
    let mut appended_during = false;
    while Instant::now() < outage_until {
        if !appended_during {
            append(&w, 7, 1 + N, M, "a");
            appended_during = true;
        }
        healthy_throughout &= metrics::state_builder_healthy();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    proxy.restore().await;

    let got = wait_for(&index, (N + M) as usize, Duration::from_secs(15)).await;
    for _ in 0..20 {
        healthy_throughout &= metrics::state_builder_healthy();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let end = counts();
    drain.abort();

    assert_eq!(
        got,
        (N + M) as usize,
        "no event lost and none double-counted across the restart"
    );
    assert!(
        healthy_throughout,
        "state_builder_healthy dropped during a 3 s outage — /livez would fail on every writer roll"
    );
    assert!(
        end.resume - after_cold.resume >= 1,
        "the reconnect must be an in-process resume"
    );
    assert_eq!(
        end.cold - after_cold.cold,
        0,
        "the reconnect must NOT be another cold replay"
    );
    assert_eq!(
        end.regression - before.regression,
        0,
        "an intact log must not trigger a replay"
    );
    // Records consumed after the outage: the M new ones plus the one-record
    // overlap, nowhere near another N.
    let consumed_after = end.wal - after_cold.wal;
    assert!(
        consumed_after <= (M + 5) as u64,
        "the resume re-read {consumed_after} records; a replay from 0 would re-read ~{}",
        N + M
    );
}

/// A writer whose log no longer agrees with ours (different record at our last
/// sequence) must NOT be resumed from: the drain falls back to a full replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rewritten_log_is_detected_and_replayed_from_zero() {
    let _g = SERIAL.lock().await;
    set_test_env();
    let w1 = writer("rewritten-a").await;
    let w2 = writer("rewritten-b").await;
    append(&w1, 11, 1, 50, "orig");
    // Same length and beyond, different transactions: our last sequence exists
    // on w2 but holds a different record.
    append(&w2, 12, 1001, 80, "other");
    let proxy = Proxy::start(w1.addr).await;
    let index = SharedWalIndex::new(WalEventIndex::new());
    let before = counts();
    let drain = tokio::spawn(run_drain_loop_ehdb(drain_config(proxy.addr), index.clone()));
    assert_eq!(wait_for(&index, 50, Duration::from_secs(10)).await, 50);

    proxy.outage();
    proxy.repoint(w2.addr);
    proxy.restore().await;
    let got = wait_for(&index, 50 + 80, Duration::from_secs(15)).await;
    let end = counts();
    drain.abort();

    assert!(
        end.regression - before.regression >= 1,
        "a log holding a different record at our cursor must be detected"
    );
    assert_eq!(
        got, 130,
        "the fallback replay must index the new log in full"
    );
}

/// A writer whose log is SHORTER than our cursor (a lost tail) delivers nothing
/// at our overlap point: the verify window elapses and the drain replays from 0
/// rather than waiting forever or skipping what the new log re-sequenced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shorter_log_times_out_the_check_and_replays_from_zero() {
    let _g = SERIAL.lock().await;
    set_test_env();
    let w1 = writer("short-a").await;
    let w2 = writer("short-b").await;
    append(&w1, 21, 1, 60, "long");
    append(&w2, 22, 5001, 5, "short");
    let proxy = Proxy::start(w1.addr).await;
    let index = SharedWalIndex::new(WalEventIndex::new());
    let before = counts();
    let drain = tokio::spawn(run_drain_loop_ehdb(drain_config(proxy.addr), index.clone()));
    assert_eq!(wait_for(&index, 60, Duration::from_secs(10)).await, 60);

    proxy.outage();
    proxy.repoint(w2.addr);
    proxy.restore().await;
    let got = wait_for(&index, 65, Duration::from_secs(15)).await;
    let end = counts();
    drain.abort();

    assert!(
        end.regression - before.regression >= 1,
        "a shorter log must be detected"
    );
    assert_eq!(
        got, 65,
        "the re-sequenced records of the shorter log must not be skipped"
    );
}

/// ⭐ noetl/ai-meta prod 2026-10-23: the restarted writer is slow to serve its
/// first read. With the OLD 10 s window both pools gave up (`found=None`) and
/// replayed ~369k events from 0 although the record was there. On the DEFAULT
/// window the drain must wait it out and RESUME — no replay, nothing lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_first_delivery_after_a_restart_still_resumes_on_the_default_window() {
    let _g = SERIAL.lock().await;
    set_test_env();
    // The production default, not the test override: this asserts the default.
    unsafe { std::env::remove_var("NOETL_STATE_BUILDER_RESUME_VERIFY_MS") };
    let w = writer("slowfirst").await;
    let proxy = Proxy::start(w.addr).await;
    let index = SharedWalIndex::new(WalEventIndex::new());
    const N: i64 = 300;
    const M: i64 = 40;
    append(&w, 31, 1, N, "s");
    let drain = tokio::spawn(run_drain_loop_ehdb(drain_config(proxy.addr), index.clone()));
    assert_eq!(
        wait_for(&index, N as usize, Duration::from_secs(10)).await,
        N as usize
    );
    let after_cold = counts();

    proxy.outage();
    append(&w, 31, 1 + N, M, "s");
    // Longer than the old 10 s window, well inside the new default.
    proxy.hold_first_delivery(Duration::from_secs(12));
    proxy.restore().await;
    let got = wait_for(&index, (N + M) as usize, Duration::from_secs(40)).await;
    let end = counts();
    drain.abort();
    set_test_env();

    assert_eq!(
        got,
        (N + M) as usize,
        "no event lost and none double-counted"
    );
    assert_eq!(
        end.regression - after_cold.regression,
        0,
        "a slow-but-intact writer was treated as a regressed log and replayed from 0"
    );
    assert_eq!(end.cold - after_cold.cold, 0, "no second cold replay");
    assert!(
        end.resume - after_cold.resume >= 1,
        "the reconnect must resume"
    );
    assert!(
        end.wal - after_cold.wal <= (M + 5) as u64,
        "re-read {} records after the restart; a replay would re-read ~{}",
        end.wal - after_cold.wal,
        N + M
    );
}

/// ⭐ Once connected, the drain is healthy WHILE it waits for the resume to
/// verify — even after an outage long enough to have flipped it unhealthy. The
/// longer window must never turn a slow first read into a `/livez` kill.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_drain_is_healthy_while_connected_and_waiting_to_verify() {
    let _g = SERIAL.lock().await;
    set_test_env();
    unsafe {
        std::env::set_var("NOETL_STATE_BUILDER_RESUME_VERIFY_MS", "20000");
        // A 2 s grace so the outage below provably flips health first.
        std::env::set_var("NOETL_STATE_BUILDER_UNHEALTHY_SECS", "2");
    }
    let w = writer("healthwait").await;
    let proxy = Proxy::start(w.addr).await;
    let index = SharedWalIndex::new(WalEventIndex::new());
    append(&w, 41, 1, 50, "h");
    let drain = tokio::spawn(run_drain_loop_ehdb(drain_config(proxy.addr), index.clone()));
    assert_eq!(wait_for(&index, 50, Duration::from_secs(10)).await, 50);

    proxy.outage();
    let until = Instant::now() + Duration::from_secs(5);
    let mut went_unhealthy = false;
    while Instant::now() < until {
        went_unhealthy |= !metrics::state_builder_healthy();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Positive control: a 5 s outage against a 2 s grace DOES flip health.
    assert!(
        went_unhealthy,
        "the outage never flipped health — the check below proves nothing"
    );

    let mark = proxy.last_accept();
    proxy.hold_first_delivery(Duration::from_secs(6));
    proxy.restore().await;
    // Wait for the drain to reconnect, then sample health during the held wait.
    let deadline = Instant::now() + Duration::from_secs(10);
    while proxy.last_accept() == mark && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_ne!(proxy.last_accept(), mark, "the drain never reconnected");
    tokio::time::sleep(Duration::from_millis(700)).await;
    let healthy_mid_wait = metrics::state_builder_healthy();
    let got = wait_for(&index, 50, Duration::from_secs(15)).await;
    drain.abort();
    unsafe { std::env::remove_var("NOETL_STATE_BUILDER_UNHEALTHY_SECS") };
    set_test_env();

    assert!(
        healthy_mid_wait,
        "connected but still unhealthy while waiting to verify the resume: a slow first \
         read after a writer restart would fail /livez"
    );
    assert_eq!(got, 50);
}
