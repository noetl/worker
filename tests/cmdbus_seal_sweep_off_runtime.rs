//! The age-seal sweep must not park a runtime worker on the engine lock
//! (noetl/ai-meta#501).
//!
//! With `NOETL_EHDB_SEAL_MAX_AGE_MS` set the writer takes the engine lock every
//! sweep interval to seal aged parts. It used to do that inline in an async
//! task, so every tick that met another holder (a startup scan chunk, an append)
//! pinned a runtime worker for the wait plus the seal. Its own binary because it
//! sets process env.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use noetl_worker::command_bus::{spawn_writer_host, CommandBusConfig, CommandBusMode};

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
async fn the_age_seal_sweep_waits_for_a_held_engine_lock_off_the_runtime() {
    // SAFETY: the only test in this binary; set before any thread reads them.
    unsafe {
        std::env::set_var("NOETL_EHDB_SEAL_MAX_AGE_MS", "60000");
        std::env::set_var("NOETL_EHDB_SEAL_SWEEP_INTERVAL_MS", "50");
    }
    let dir = std::env::temp_dir().join(format!("noetl-seal-sweep-501-{}", std::process::id()));
    let cfg = CommandBusConfig {
        mode: CommandBusMode::Ehdb,
        host: true,
        shard: 0,
        shard_count: 1,
        writer_dir: Some(dir.clone()),
        ingest_bind: None,
        claim_bind: None,
        metrics_bind: None,
        claim_addr: None,
        ack_wait: Duration::from_secs(30),
        cursor_persist: Duration::ZERO,
        cursor_fallback: Default::default(),
    };
    let (writer, shutdown) = spawn_writer_host(&cfg).await.unwrap();

    let engine = writer.engine();
    let (tx, rx) = mpsc::channel();
    let holder = std::thread::spawn(move || {
        let _g = engine.read();
        tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(2_500));
    });
    rx.recv().unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (mut probes, mut held) = (0u32, 0u32);
    while !holder.is_finished() {
        if !both_workers_free(Duration::from_millis(400)).await {
            held += 1;
        }
        probes += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    holder.join().unwrap();
    eprintln!("  seal sweep vs a 2.5 s hold: {probes} runtime probes, {held} held");
    assert!(probes >= 3, "too few probes ({probes})");
    assert_eq!(
        held, 0,
        "{held}/{probes} probes found a runtime worker parked — the age-seal sweep \
         takes the engine Mutex on a Tokio worker"
    );

    shutdown.run().await;
    let _ = std::fs::remove_dir_all(&dir);
}
