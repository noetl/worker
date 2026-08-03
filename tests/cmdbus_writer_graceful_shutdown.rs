//! **The graceful half of noetl/ai-meta#209** — SIGTERM must seal the log with
//! no loss of anything the writer already acked.
//!
//! The failure this pins is the nastiest shape in the EHDB migration: it is
//! *silent*, and it looks like success. The L0 engine reopens from its durable
//! **manifest**, so records sitting in an unsealed active part are invisible to
//! a restarted process even though every append `fsync`ed them and the publisher
//! got an ack. A restart therefore drops the tail of the log while every health
//! signal stays green — the writer comes up, reports `ehdb_feed_shard_lag 0`,
//! and the missing commands simply never dispatch until the orphaned-command
//! guardrail (noetl/ai-meta#171) re-issues them ~30s later.
//!
//! Two defects produced that, both fixed by the sequenced shutdown in
//! `noetl_worker::graceful`:
//!
//! 1. The seal raced **process exit**. `main`'s shutdown branch logged one line
//!    and returned, dropping the runtime while the writer host's own detached
//!    SIGTERM handler was still inside `flush_and_wait_uploads`.
//! 2. The seal raced **in-flight ingest**. `serve_ingest` kept accepting and
//!    appending during the seal, so a command published in that instant was
//!    acked and then landed in a fresh active part opened *after* the seal.
//!
//! Defect 2 took two attempts. The first sequenced the steps but signalled the
//! acceptor with `Notify::notify_waiters()`, which wakes only waiters already
//! registered — and the acceptor registers on its first poll, so shutdown firing
//! before that poll lost the signal outright and `serve_ingest` accepted
//! straight through the seal. `graceful::StopSignal` replaced it with a `watch`
//! value plus a stop acknowledgement; see that module's docs.
//!
//! These tests drive the real `spawn_writer_host` wiring, so they fail if either
//! the sequencing or the seal regresses. Note which test covers which: the two
//! `..._the_active_part_...` tests below drive `FeedWriter::append` in-process
//! and never touch the ingest face, which is exactly why they stayed green
//! 300/300 while defect 2 was live. The stop-ingest ordering is covered by
//! `shutdown_that_fires_before_the_acceptor_is_polled_still_closes_the_listener`
//! (with its yield-first control) and by the `..._over_the_wire_...` test, which
//! publishes through the acceptor with the real `PublishClient`.

use std::net::SocketAddr;
use std::time::Duration;

use noetl_worker::command_bus::{spawn_writer_host, CommandBusConfig, CommandBusMode};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "noetl-cmdbus-graceful-{tag}-{}-{n}",
        std::process::id()
    ))
}

/// A kernel-assigned ephemeral port, released so the writer can bind it.
///
/// Inherently a race: the port is free between the `drop` and the writer's
/// bind, and free again after the writer releases it. Nothing here can close
/// that — `CommandBusConfig::ingest_bind` is a `SocketAddr`, so the writer must
/// do its own binding. Assertions built on this address must therefore never
/// treat "something is listening on that port" as "the writer is listening on
/// that port". See `shutdown_closes_the_ingest_listener_before_sealing`.
async fn free_addr() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

/// Can we take `addr` back? This is the only probe that answers "did **this**
/// writer drop its listener", and it is why the assertions below re-bind rather
/// than probe with a connect: a connect only reports that *someone* is
/// listening, and `free_addr` hands out ports from the same ephemeral range the
/// rest of the suite binds from in concurrently-run test binaries.
///
/// A short bounded retry, not a long one: after the fix `shutdown.run()` does
/// not return until the acceptor future has been dropped, so the first attempt
/// is expected to succeed. The retry only absorbs kernel-side close scheduling —
/// it cannot rescue a *lost* stop signal, which never closes the listener at all.
async fn rebindable(addr: SocketAddr) -> bool {
    for _ in 0..10 {
        if tokio::net::TcpListener::bind(addr).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

fn config_at(dir: &std::path::Path, ingest: Option<SocketAddr>) -> CommandBusConfig {
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

/// Count the records a *freshly reopened* engine can see on `dir`. This is the
/// only honest measure of durability here: it is exactly what the replacement
/// writer pod does, and it reads the manifest, so anything left in an unsealed
/// active part is invisible to it by construction.
async fn records_visible_after_reopen(dir: &std::path::Path) -> u64 {
    let (writer, _shutdown) = spawn_writer_host(&config_at(dir, None)).await.unwrap();
    let tip = *writer.tip_receiver().borrow();
    tip
}

/// The headline: every record acked before SIGTERM survives the restart.
///
/// Deliberately writes 300 records — comfortably under `seal_max_records`
/// (1024), so **none** of them auto-sealed along the way. Every single one is
/// sitting in the active part when shutdown runs, which means the whole batch is
/// exactly the tail that used to vanish. Without the seal this reads back 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_seals_the_active_part_with_no_acked_record_lost() {
    let dir = unique_dir("seal");
    const RECORDS: u64 = 300;

    {
        let (writer, shutdown) = spawn_writer_host(&config_at(&dir, None)).await.unwrap();
        for seq in 1..=RECORDS {
            writer
                .append(ehdb_l0::EventRecord::new(
                    seq,
                    format!("exec-{seq}"),
                    "command.issued",
                    format!("{{\"seq\":{seq}}}"),
                ))
                .expect("append acked");
        }
        // The test is only meaningful below `seal_max_records`: above it the
        // engine auto-sealed along the way and we would be proving nothing.
        const _: () = assert!(RECORDS < 1024);

        // The graceful path, in full: stop ingest -> quiesce -> cursor -> seal.
        shutdown.run().await;
    }

    let visible = records_visible_after_reopen(&dir).await;
    assert_eq!(
        visible, RECORDS,
        "the reopened engine saw {visible} of {RECORDS} acked records — the \
         unsealed tail was lost despite every append being fsynced and acked"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The quantified counterfactual: the *same* writes, with no seal, are gone.
///
/// This is what a SIGKILL / OOM still costs today — the hard-kill half of
/// noetl/ai-meta#209, which needs L0-level replay of the local active part on
/// open and is out of scope here. Pinning it as a test rather than a comment
/// means the day someone closes that half, this test fails and tells them the
/// exposure is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_the_seal_the_unsealed_tail_is_lost_the_hard_kill_exposure() {
    let dir = unique_dir("nokill");
    const RECORDS: u64 = 300;

    {
        let (writer, _shutdown) = spawn_writer_host(&config_at(&dir, None)).await.unwrap();
        for seq in 1..=RECORDS {
            writer
                .append(ehdb_l0::EventRecord::new(
                    seq,
                    format!("exec-{seq}"),
                    "command.issued",
                    format!("{{\"seq\":{seq}}}"),
                ))
                .expect("append acked");
        }
        // No `shutdown.run()` — this models SIGKILL, where nothing seals.
    }

    let visible = records_visible_after_reopen(&dir).await;
    assert_eq!(
        visible, 0,
        "expected the whole unsealed tail to be lost on a hard kill (that is the \
         residual exposure #209 documents); saw {visible} of {RECORDS} survive. \
         If the L0 active-part replay landed, this exposure is closed — update \
         the issue and delete this test."
    );
    // The loss is bounded by `seal_max_records` — that bound is the number the
    // soak's hard-kill measurement is checked against.
    const _: () = assert!(RECORDS <= 1024);

    let _ = std::fs::remove_dir_all(&dir);
}

/// The invariant defect 2 is about, stated end to end over the real publish
/// protocol: **everything the writer acked is visible to the next incarnation,
/// and nothing can be acked once shutdown has returned.**
///
/// The earlier tests here drive `FeedWriter::append` in-process, which never
/// touches the ingest face at all — that is exactly why they passed 300/300
/// while the stop-ingest step was broken. This one publishes over the wire with
/// `PublishClient`, the same client noetl-server uses, so the acceptor is in the
/// path.
///
/// The post-shutdown probe accepts either outcome that means "not acked": a
/// refused connect (the expected one), or a connect that never yields a sort
/// key. It cannot simply require a refusal — `free_addr` hands out an ephemeral
/// port that a concurrently-running test binary may take the moment the writer
/// releases it, and a stranger answering the connect says nothing about this
/// writer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nothing_the_writer_acked_over_the_wire_is_lost_and_nothing_is_acked_after_shutdown() {
    let dir = unique_dir("wire");
    let addr = free_addr().await;
    const RECORDS: u64 = 200;
    const _: () = assert!(RECORDS < 1024); // below seal_max_records: nothing auto-sealed

    let (_writer, shutdown) = spawn_writer_host(&config_at(&dir, Some(addr)))
        .await
        .unwrap();

    let mut publisher = ehdb_feed::PublishClient::connect(addr)
        .await
        .expect("the ingest face accepts publishers while the writer serves");
    let mut acked = 0u64;
    for seq in 1..=RECORDS {
        publisher
            .publish(&ehdb_l0::EventRecord::new(
                seq,
                format!("exec-{seq}"),
                "command.issued",
                format!("{{\"seq\":{seq}}}"),
            ))
            .await
            .expect("durable ack");
        acked += 1;
    }
    drop(publisher);

    shutdown.run().await;

    // Nothing new can earn an ack now: the listener is closed, and anything that
    // slipped past it blocks forever on the held engine lock rather than landing
    // in a post-seal part.
    let late = tokio::time::timeout(Duration::from_millis(500), async {
        let mut client = ehdb_feed::PublishClient::connect(addr).await.ok()?;
        client
            .publish(&ehdb_l0::EventRecord::new(
                RECORDS + 1,
                "exec-late".to_string(),
                "command.issued",
                "{\"late\":true}".to_string(),
            ))
            .await
            .ok()
    })
    .await;
    assert!(
        !matches!(late, Ok(Some(_))),
        "a publisher was acked after shutdown returned — that record lands in a \
         part the next incarnation cannot see (noetl/ai-meta#209 defect 2)"
    );

    let visible = records_visible_after_reopen(&dir).await;
    assert_eq!(
        visible, acked,
        "the reopened engine saw {visible} of {acked} records acked over the \
         wire — an acked record was lost across the graceful restart"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The deterministic form of the lost-wakeup defect, with the ordering the
/// scheduler only sometimes produces **forced**.
///
/// `#[tokio::test]` without a flavor is a *current-thread* runtime, and
/// `tokio::spawn` on it never polls the new task inline — it queues it until the
/// spawning task yields. `spawn_writer_host` performs no `await` between
/// spawning the ingest acceptor and returning (with `ingest_bind` set and
/// nothing else bound), so at the point `shutdown.run()` is called below the
/// acceptor task is **guaranteed** never to have been polled.
///
/// That is exactly the ordering that loses a `Notify::notify_waiters()` signal:
/// it wakes only waiters already registered and stores no permit, while the
/// acceptor registers its waiter on its first poll. This test therefore fails
/// 100% of the time against a registration-order-dependent signal, and is the
/// deterministic anchor for the fix — no load, no contention, no retries.
///
/// See `graceful::StopSignal` for the mechanism that makes it pass.
#[tokio::test]
async fn shutdown_that_fires_before_the_acceptor_is_polled_still_closes_the_listener() {
    let dir = unique_dir("forced");
    let addr = free_addr().await;
    let (_writer, shutdown) = spawn_writer_host(&config_at(&dir, Some(addr)))
        .await
        .unwrap();

    // NO yield here. Anything that awaits — a sleep, a `yield_now`, a connect —
    // hands the runtime to the acceptor and destroys the forced ordering. The
    // control test below is the same body *with* that yield.
    shutdown.run().await;

    assert!(
        rebindable(addr).await,
        "the ingest listener was still holding {addr} after shutdown fired \
         before the acceptor's first poll — the stop signal was lost, so \
         `serve_ingest` accepts straight through the seal (noetl/ai-meta#209 \
         defect 2)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The control for the test above: identical, except the acceptor is polled
/// first. This passes both before and after the fix — which is what makes the
/// forced-ordering case above *evidence* rather than noise.
#[tokio::test]
async fn shutdown_closes_the_listener_when_the_acceptor_registered_first_control() {
    let dir = unique_dir("control");
    let addr = free_addr().await;
    let (_writer, shutdown) = spawn_writer_host(&config_at(&dir, Some(addr)))
        .await
        .unwrap();

    // The only difference from the forced case: let the acceptor task run, so a
    // registration-order-dependent signal has a waiter to wake.
    tokio::task::yield_now().await;

    shutdown.run().await;

    assert!(
        rebindable(addr).await,
        "the ingest listener was still holding {addr} after shutdown, even with \
         the acceptor polled first — this is not the lost-wakeup ordering, so \
         something else on the shutdown path is failing to close the listener"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Shutdown must close the ingest listener, so a publisher cannot connect and
/// get an ack for a record that would land after the seal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_closes_the_ingest_listener_before_sealing() {
    let dir = unique_dir("ingest");
    let addr = free_addr().await;
    let (_writer, shutdown) = spawn_writer_host(&config_at(&dir, Some(addr)))
        .await
        .unwrap();

    // The face is up: a publisher can reach it.
    tokio::net::TcpStream::connect(addr)
        .await
        .expect("ingest face accepts connections while the writer is serving");

    shutdown.run().await;

    // ...and the port is ours again afterwards, so nothing new can be acked
    // post-seal. See `rebindable` for why the probe re-binds instead of
    // expecting a refused connect.
    //
    // This assertion used to fail under load — 24 of 30 full-suite runs under
    // contention, 0 of 15 idle — and the failure was real, not harness noise:
    // `lsof` at the moment of failure showed the port still held in LISTEN by
    // this very test process, with the writer host's own acceptor on it, after
    // `shutdown.run()` had returned. The cause was a lost wakeup in
    // `WriterShutdown::run`, now fixed; the deterministic form of that ordering
    // is pinned above in
    // `shutdown_that_fires_before_the_acceptor_is_polled_still_closes_the_listener`.
    //
    // Note the `connect` above never disproved it: the socket is bound and
    // listening before the acceptor task is spawned, so the kernel completes
    // handshakes into the backlog whether or not that task is ever polled.
    assert!(
        rebindable(addr).await,
        "the ingest listener was still holding {addr} after shutdown — a \
         publisher could be acked for a record landing in a part opened after \
         the seal, which is precisely the acked-and-lost window of \
         noetl/ai-meta#209"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
