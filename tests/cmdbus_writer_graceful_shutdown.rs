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
//! These tests drive the real `spawn_writer_host` wiring, so they fail if either
//! the sequencing or the seal regresses.

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

async fn free_addr() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
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

    // ...and is refused afterwards, so nothing new can be acked post-seal.
    // Retry briefly: closing the listener is asynchronous (the acceptor future
    // is dropped), so a connect racing that drop may still succeed once.
    let mut refused = false;
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_err() {
            refused = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        refused,
        "the ingest listener was still accepting after shutdown — a publisher \
         could be acked for a record landing in a part opened after the seal, \
         which is precisely the acked-and-lost window of noetl/ai-meta#209"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
