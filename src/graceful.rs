//! Ordered, *awaited* shutdown for the EHDB writer hosts (noetl/ai-meta#209).
//!
//! # What was wrong
//!
//! Both writer hosts used to install their own detached SIGTERM handler and
//! spawn every face — ingest, claim, SSE, WAL, KV, metrics — with the
//! `JoinHandle` dropped on the floor. `main`'s own SIGTERM handler resolved on
//! the same signal, logged one line, dropped the `worker.run()` future and
//! returned from `main`, which drops the runtime and exits the process.
//!
//! So the seal was not sequenced against anything. Three distinct losses fell
//! out of that:
//!
//! 1. **The seal raced process exit.** `main` frequently returned before the
//!    detached handler's `spawn_blocking(flush_and_wait_uploads)` finished, so
//!    the seal simply did not happen.
//! 2. **The seal raced in-flight ingest.** `serve_ingest` kept accepting and
//!    appending *while* the shutdown handler sealed. A command published in
//!    that window was acked to the server and then landed in a fresh active
//!    part opened after the seal — acked and lost. Observed once per writer
//!    restart in kind, reproducibly.
//! 3. **The events writer never sealed at all.** Its handler only checkpointed
//!    group cursors; there was no `flush_and_wait_uploads` anywhere on the
//!    events path. That is the *sole writer of the durable `noetl.event` log*,
//!    so it was the larger of the two loss surfaces.
//!
//! # The shape here
//!
//! A host returns a [`WriterShutdown`] the `Worker` owns and `main` **awaits**
//! before exiting. The sequence is:
//!
//! ```text
//!   stop accepting ingest  ->  quiesce  ->  persist cursor  ->  seal and hold
//! ```
//!
//! ## Stop accepting, then quiesce
//!
//! `ehdb_feed::serve_ingest` has no cancellation parameter (it is a bare
//! `accept()` loop), so the host wraps it in a `select!` against a `Notify`.
//! Firing the notify drops the acceptor future, which closes the listener: no
//! *new* publisher connections. Connections already accepted live in tasks
//! spawned inside `ehdb-feed` that hold their own `Arc<FeedWriter>` clones and
//! cannot be reached from here — hence the short quiesce window, which lets
//! their in-flight appends land and be acked *before* the seal rather than
//! after it.
//!
//! ## Seal and hold
//!
//! The quiesce window is a mitigation, not a guarantee, so the seal itself
//! closes the hole: [`Sealable::seal_and_hold`] seals the active part and then
//! **deliberately leaks the engine mutex guard**. `FeedWriter::append_batch`
//! takes that same mutex, so any publisher that arrives after the seal blocks
//! forever and is *never acked*. An un-acked publish is exactly the case the
//! contract already handles — the server's publish redial-retry republishes it
//! to the replacement writer (noetl/server#290). The alternative, releasing the
//! lock, lets a post-seal append open a new active part that the next
//! incarnation cannot see: acked and lost, which is the bug.
//!
//! Leaking a lock is only sound because this runs on the terminal path, tens of
//! milliseconds before the process exits. [`WriterShutdown::run`] must never be
//! called on a worker that intends to keep serving.
//!
//! # What this does NOT fix
//!
//! The hard-kill half of noetl/ai-meta#209. SIGKILL / OOM / node loss skips all
//! of the above and still loses the unsealed tail — up to `seal_max_records`
//! (1024 by default) per shard — despite every record having been `fsync`ed and
//! acked. Closing that needs L0-level replay of the local active part on open,
//! an `ehdb-l0` change tracked separately on the same issue.
//!
//! The KV face is also still unsealed: `ehdb_feed::KvCoordinator` keeps its
//! `KvStore` private with no public flush, so the worker cannot seal it without
//! an ehdb-side accessor. Sessions/requests written in the last unsealed part
//! are lost on restart. Tracked as a follow-up.

use anyhow::{anyhow, Result};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

use ehdb_l0::{Dataset, L0Engine};

/// How long to let already-accepted publishers finish after the listener
/// closes, before sealing. Short: the seal-and-hold below is the actual
/// guarantee, so this only buys a cleaner ack ledger, and every millisecond
/// here is spent inside Kubernetes' termination grace period.
const DEFAULT_QUIESCE: Duration = Duration::from_millis(250);

/// `NOETL_EHDB_SHUTDOWN_QUIESCE_MS` — override for the quiesce window.
fn quiesce() -> Duration {
    std::env::var("NOETL_EHDB_SHUTDOWN_QUIESCE_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_QUIESCE)
}

type BoxFut = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

/// An L0 engine that must be sealed before the process exits.
pub trait Sealable: Send + Sync + 'static {
    /// Seal the active part, wait for its upload, and then keep the engine
    /// locked so nothing can append (and therefore nothing can be *acked*)
    /// afterwards. See the module docs for why the lock is never released.
    fn seal_and_hold(&self) -> Result<()>;
}

/// [`Sealable`] over a `FeedWriter`'s engine handle.
pub struct EngineSeal<D: Dataset> {
    engine: Arc<Mutex<L0Engine<D>>>,
}

impl<D: Dataset> EngineSeal<D> {
    pub fn new(engine: Arc<Mutex<L0Engine<D>>>) -> Self {
        Self { engine }
    }
}

impl<D> Sealable for EngineSeal<D>
where
    D: Dataset + Send + Sync + 'static,
{
    fn seal_and_hold(&self) -> Result<()> {
        let mut guard = self
            .engine
            .lock()
            .map_err(|_| anyhow!("L0 engine mutex poisoned before the shutdown seal"))?;
        guard.flush_and_wait_uploads()?;
        // Deliberate: hold the engine lock through process exit so no append
        // can be acked after the seal. See the module docs.
        std::mem::forget(guard);
        Ok(())
    }
}

/// The shutdown handle a writer host hands back to the `Worker`.
pub struct WriterShutdown {
    /// `command-bus` / `events-feed` — used only for log lines.
    label: &'static str,
    shard: u32,
    /// Fired to close the ingest listener. `None` when the host never bound an
    /// ingest face (tests, and pods that only consume).
    stop_ingest: Option<Arc<Notify>>,
    /// Persist the claim/group cursor. Boxed because the two hosts carry
    /// different coordinator types.
    persist_cursor: Box<dyn Fn() -> BoxFut + Send + Sync>,
    /// Engines to seal, in order.
    sealables: Vec<Arc<dyn Sealable>>,
}

impl WriterShutdown {
    pub fn new(
        label: &'static str,
        shard: u32,
        stop_ingest: Option<Arc<Notify>>,
        persist_cursor: Box<dyn Fn() -> BoxFut + Send + Sync>,
        sealables: Vec<Arc<dyn Sealable>>,
    ) -> Self {
        Self {
            label,
            shard,
            stop_ingest,
            persist_cursor,
            sealables,
        }
    }

    /// Run the full sequence. Never panics: a shutdown path that unwinds is
    /// worse than one that logs and continues to the next step, because the
    /// steps are independent and the later ones matter more.
    pub async fn run(&self) {
        let (label, shard) = (self.label, self.shard);

        // 1. Close the ingest listener: no new publisher connections.
        if let Some(stop) = &self.stop_ingest {
            stop.notify_waiters();
            tracing::info!(label, shard, "EHDB {label} ingest listener closing");

            // 2. Let already-accepted publishers land and get acked.
            tokio::time::sleep(quiesce()).await;
        }

        // 3. Cursor first: it is cheap, and a cursor behind the log is always
        //    safe while a log behind the cursor needs the resume clamp.
        match (self.persist_cursor)().await {
            Ok(()) => tracing::info!(label, shard, "EHDB {label} cursor persisted on shutdown"),
            Err(error) => {
                tracing::warn!(label, shard, %error, "EHDB {label} cursor persist failed")
            }
        }

        // 4. Seal, and hold the engine closed through exit.
        for sealable in &self.sealables {
            let sealable = Arc::clone(sealable);
            // `flush_and_wait_uploads` blocks on the uploader's condvar, so keep
            // it off the async worker threads.
            match tokio::task::spawn_blocking(move || sealable.seal_and_hold()).await {
                Ok(Ok(())) => tracing::info!(label, shard, "EHDB {label} log sealed on shutdown"),
                Ok(Err(error)) => tracing::warn!(label, shard, %error, "EHDB {label} seal failed"),
                Err(error) => {
                    tracing::warn!(label, shard, %error, "EHDB {label} seal task failed")
                }
            }
        }
    }
}

/// Wrap a face's `serve_*` future so firing `stop` closes its listener.
///
/// Dropping the future is what closes the listener; the already-accepted
/// connections are owned by tasks inside `ehdb-feed` and outlive this.
pub async fn until_stopped<F>(stop: Arc<Notify>, face: &'static str, serve: F)
where
    F: Future<Output = std::io::Result<()>> + Send + 'static,
{
    let stopped = stop.notified();
    tokio::pin!(stopped);
    tokio::select! {
        result = serve => report_face_exit(face, result),
        _ = &mut stopped => {}
    }
}

/// Supervise a face that has no shutdown hook, purely so its exit is *visible*.
///
/// Every face used to be spawned as a bare `tokio::spawn(serve_x(..))`, which
/// discards the returned `io::Result`. A face that dies therefore takes its
/// listener down with it and says nothing: the startup line still claims the
/// face is "up", `/metrics` carries no signal for it, and the only symptom is
/// consumers logging `Connection refused` somewhere else entirely.
///
/// That is not hypothetical. `ehdb_feed::serve` — the WAL fan-out face — runs
/// its subscribe handshake *inside* the accept loop, so `read_frame(..)?` and
/// `serde_json::from_slice(..)?` propagate out of the whole function. **One**
/// connection that closes early or sends a non-`SubscribeReq` frame (a port
/// scan, a stray `curl`, an HTTP health probe pointed at the wrong port)
/// permanently kills the face for the rest of the process's life. Reproduced
/// deterministically in kind: listener present, one malformed frame, listener
/// gone, no log line anywhere.
///
/// This does not fix that — the fix belongs in `ehdb-feed`, moving the
/// handshake into the per-connection task and not letting a per-connection
/// error escape the accept loop. It makes it *loud*, which is the part the
/// worker owns.
pub async fn supervised<F>(face: &'static str, serve: F)
where
    F: Future<Output = std::io::Result<()>> + Send + 'static,
{
    report_face_exit(face, serve.await);
}

fn report_face_exit(face: &'static str, result: std::io::Result<()>) {
    match result {
        // A face returning at all means its listener is gone and the face is
        // dead until the pod restarts. Never silent, error either way.
        Ok(()) => tracing::error!(
            face,
            "EHDB {face} face accept loop ended — the listener is closed and this \
             face is dead for the life of the process"
        ),
        Err(error) => tracing::error!(
            face,
            %error,
            "EHDB {face} face accept loop failed — the listener is closed and this \
             face is dead for the life of the process"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A `Sealable` that records how many times it sealed.
    struct CountingSeal(Arc<AtomicUsize>);
    impl Sealable for CountingSeal {
        fn seal_and_hold(&self) -> Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// The ordering is the whole fix: ingest must be told to stop *before* the
    /// cursor is persisted and the log sealed. A seal that runs while ingest is
    /// still accepting is the acked-and-lost window of noetl/ai-meta#209.
    #[tokio::test]
    async fn shutdown_stops_ingest_before_it_persists_and_seals() {
        std::env::set_var("NOETL_EHDB_SHUTDOWN_QUIESCE_MS", "10");
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let stop = Arc::new(Notify::new());
        let seals = Arc::new(AtomicUsize::new(0));

        // A stand-in ingest face: records that it stopped when the notify fires.
        let ingest_order = Arc::clone(&order);
        let ingest_stop = Arc::clone(&stop);
        let ingest = tokio::spawn(async move {
            ingest_stop.notified().await;
            ingest_order.lock().unwrap().push("ingest-stopped");
        });
        // Let the fake face reach its `notified()` await before we fire.
        tokio::time::sleep(Duration::from_millis(20)).await;

        let cursor_order = Arc::clone(&order);
        let shutdown = WriterShutdown::new(
            "test-bus",
            0,
            Some(Arc::clone(&stop)),
            Box::new(move || {
                let order = Arc::clone(&cursor_order);
                Box::pin(async move {
                    order.lock().unwrap().push("cursor-persisted");
                    Ok(())
                })
            }),
            vec![Arc::new(CountingSeal(Arc::clone(&seals)))],
        );

        shutdown.run().await;
        ingest.await.unwrap();

        let seen = order.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec!["ingest-stopped", "cursor-persisted"],
            "ingest must stop before the cursor persist + seal, not alongside them"
        );
        assert_eq!(
            seals.load(Ordering::SeqCst),
            1,
            "the log must be sealed exactly once on the graceful path"
        );
        std::env::remove_var("NOETL_EHDB_SHUTDOWN_QUIESCE_MS");
    }

    /// A host with no ingest face (a consume-only pod, and every unit test)
    /// must still persist its cursor and seal.
    #[tokio::test]
    async fn a_host_without_an_ingest_face_still_seals() {
        let seals = Arc::new(AtomicUsize::new(0));
        let persisted = Arc::new(AtomicUsize::new(0));
        let p = Arc::clone(&persisted);
        let shutdown = WriterShutdown::new(
            "test-bus",
            0,
            None,
            Box::new(move || {
                let p = Arc::clone(&p);
                Box::pin(async move {
                    p.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }),
            vec![Arc::new(CountingSeal(Arc::clone(&seals)))],
        );
        shutdown.run().await;
        assert_eq!(persisted.load(Ordering::SeqCst), 1);
        assert_eq!(seals.load(Ordering::SeqCst), 1);
    }

    /// A cursor-persist failure must not skip the seal. The seal is the part
    /// that prevents data loss; the cursor only affects redelivery.
    #[tokio::test]
    async fn a_cursor_failure_still_seals() {
        let seals = Arc::new(AtomicUsize::new(0));
        let shutdown = WriterShutdown::new(
            "test-bus",
            0,
            None,
            Box::new(|| Box::pin(async { Err(anyhow!("cursor store unavailable")) })),
            vec![Arc::new(CountingSeal(Arc::clone(&seals)))],
        );
        shutdown.run().await;
        assert_eq!(
            seals.load(Ordering::SeqCst),
            1,
            "a cursor failure must not cost us the seal"
        );
    }
}
