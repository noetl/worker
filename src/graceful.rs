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
//! `accept()` loop), so the host wraps it in a `select!` against a
//! [`StopSignal`]. Losing that race drops the acceptor future, which closes the
//! listener: no *new* publisher connections. Connections already accepted live
//! in tasks spawned inside `ehdb-feed` that hold their own `Arc<FeedWriter>`
//! clones and cannot be reached from here — hence the short quiesce window,
//! which lets their in-flight appends land and be acked *before* the seal rather
//! than after it.
//!
//! ### Why the signal is a `watch`, and why it is acknowledged
//!
//! This step was written first with a `tokio::sync::Notify` fired by
//! `notify_waiters()`, and it did not work. `notify_waiters` wakes the waiters
//! **registered at that instant** and stores no permit, while the acceptor runs
//! under `tokio::spawn(until_stopped(..))` and registers its waiter on its
//! **first poll**. Shutdown firing before that first poll loses the signal
//! permanently: `until_stopped` then parks on a notification that will never
//! come again and `serve_ingest` accepts straight through the seal. That is
//! load-dependent, not rare — 24 of 30 full-suite runs under contention, 0 of 15
//! idle — which is why it read as a flaky test for as long as it did.
//!
//! [`StopSignal`] carries a `watch` **value** instead. A receiver polled for the
//! first time after the signal fired still observes `true`
//! (`Receiver::wait_for` evaluates its predicate against the current value
//! before ever parking), so no ordering between "shutdown fires" and "the face
//! task is first polled" can lose it.
//!
//! A permit-storing `Notify::notify_one` would fix the single-face command-bus
//! host and quietly break the events host, which is the one that matters more:
//! one stored permit wakes exactly **one** waiter, so the first face to be
//! polled consumes it and every other face registered against the same handle
//! parks forever. `watch` broadcasts — every receiver of the same channel
//! observes the same value — so one `StopSignal` fans out to as many faces as a
//! host cares to register. Registering before spawning would also close the
//! specific hole, but it is a rule about call-site ordering that nothing
//! enforces; `watch` makes the ordering irrelevant.
//!
//! Firing is also not enough on its own. Dropping the acceptor future is what
//! closes the listener, and that happens inside the face's task, so a
//! `run()` that returns as soon as it has *sent* the signal can still seal
//! underneath a listener that is open. Every face therefore acknowledges: it
//! drops its `serve` future and only then reports back, and
//! [`StopSignal::stop`] does not return until every registered face has reported
//! (or the bounded deadline expires, which is logged). "Stop accepting" is a
//! completed step by the time the sequence moves on, not a request in flight.
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
use tokio::sync::{mpsc, watch};

use ehdb_l0::{Dataset, L0Engine};

/// How long to let already-accepted publishers finish after the listener
/// closes, before sealing. Short: the seal-and-hold below is the actual
/// guarantee, so this only buys a cleaner ack ledger, and every millisecond
/// here is spent inside Kubernetes' termination grace period.
const DEFAULT_QUIESCE: Duration = Duration::from_millis(250);

/// How long to wait for the faces to actually stop before giving up and sealing
/// anyway. A face that will not stop is a bug, but sealing late is worse than
/// sealing with a listener still open — the seal is what prevents loss, and
/// `main`'s own 15 s budget is what stands between us and a SIGKILL.
const DEFAULT_STOP_DEADLINE: Duration = Duration::from_millis(2000);

/// Bounded so one face's ack cannot block another's. Far above the handful of
/// faces any host registers, so `try_send` in the drop path never fails.
const ACK_CAPACITY: usize = 16;

/// `NOETL_EHDB_SHUTDOWN_QUIESCE_MS` — override for the quiesce window.
fn quiesce() -> Duration {
    env_millis("NOETL_EHDB_SHUTDOWN_QUIESCE_MS").unwrap_or(DEFAULT_QUIESCE)
}

/// `NOETL_EHDB_SHUTDOWN_STOP_TIMEOUT_MS` — override for the stop-ingest deadline.
fn stop_deadline() -> Duration {
    env_millis("NOETL_EHDB_SHUTDOWN_STOP_TIMEOUT_MS").unwrap_or(DEFAULT_STOP_DEADLINE)
}

fn env_millis(key: &str) -> Option<Duration> {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_millis)
}

/// The stop signal a host shares with its cancellable faces.
///
/// Two properties the previous `Arc<Notify>` did not have, both load-bearing —
/// see the module docs for the failure each one closes:
///
/// 1. **Registration-order independence.** The signal is a `watch` *value*, so a
///    face first polled after the signal fired still observes it. There is no
///    ordering between "shutdown fires" and "the spawned face is first polled"
///    that can lose it.
/// 2. **Acknowledgement.** [`stop`](Self::stop) returns only once every
///    registered face has dropped its `serve` future — i.e. once the listeners
///    are actually closed — so "stop accepting" is a completed step rather than
///    a request in flight.
///
/// One signal fans out to any number of faces: `watch` broadcasts to every
/// receiver. That is the property the events writer host needs and the reason
/// this is not a permit-storing `Notify::notify_one`, which would wake exactly
/// one of them.
pub struct StopSignal {
    stop: watch::Sender<bool>,
    /// The template ack sender, cloned into each handle. [`stop`](Self::stop)
    /// takes it so that the last surviving handle closing the channel is what
    /// tells the receiver every face has reported.
    ack: Mutex<Option<mpsc::Sender<&'static str>>>,
    acks: tokio::sync::Mutex<mpsc::Receiver<&'static str>>,
}

impl StopSignal {
    pub fn new() -> Arc<Self> {
        let (stop, _) = watch::channel(false);
        let (ack, acks) = mpsc::channel(ACK_CAPACITY);
        Arc::new(Self {
            stop,
            ack: Mutex::new(Some(ack)),
            acks: tokio::sync::Mutex::new(acks),
        })
    }

    /// Register a face and get the handle [`until_stopped`] consumes.
    ///
    /// Call this **before** spawning the face's task — the handle is what makes
    /// the face's stop observable, and `stop()` waits for exactly the faces
    /// registered when it runs. Correctness does not depend on it (that is the
    /// point of the `watch`); the ack barrier's completeness does.
    ///
    /// Registering after `stop()` has already fired yields a handle that
    /// observes the stop immediately and is not counted by the (already
    /// finished) barrier, so a late face still shuts itself down and cannot
    /// wedge shutdown.
    pub fn register(self: &Arc<Self>, face: &'static str) -> StopHandle {
        StopHandle {
            face,
            stop: self.stop.subscribe(),
            ack: self.ack.lock().ok().and_then(|slot| slot.clone()),
        }
    }

    /// Fire the signal and wait until every registered face has stopped.
    ///
    /// Bounded by `deadline`; a timeout is reported, not fatal. Idempotent — a
    /// second call finds the barrier already drained and returns immediately.
    pub async fn stop(&self, deadline: Duration) -> StopReport {
        let _ = self.stop.send(true);
        // Drop our own template sender: the ack channel now closes exactly when
        // the last registered handle is dropped.
        if let Ok(mut slot) = self.ack.lock() {
            slot.take();
        }

        let mut report = StopReport::default();
        let mut acks = self.acks.lock().await;
        let drain = async {
            while let Some(face) = acks.recv().await {
                report.stopped.push(face);
            }
        };
        if tokio::time::timeout(deadline, drain).await.is_err() {
            report.timed_out = true;
        }
        report
    }
}

/// What [`StopSignal::stop`] observed.
#[derive(Debug, Default)]
pub struct StopReport {
    /// Faces that reported their listener closed, in the order they reported.
    pub stopped: Vec<&'static str>,
    /// A face did not report within the deadline. The sequence continues — the
    /// seal matters more — but the ordering guarantee is not proven for this run.
    pub timed_out: bool,
}

/// One face's end of a [`StopSignal`].
pub struct StopHandle {
    face: &'static str,
    stop: watch::Receiver<bool>,
    /// `None` only for a face registered after the barrier already completed.
    ack: Option<mpsc::Sender<&'static str>>,
}

impl StopHandle {
    pub fn face(&self) -> &'static str {
        self.face
    }

    /// Resolves once shutdown has been signalled — including when it was
    /// signalled *before* this handle was ever polled, which is the whole
    /// reason this is a `watch` and not a `Notify`.
    ///
    /// A dropped sender (the host is gone) also resolves it: stopping is the
    /// safe direction to fail in.
    async fn signalled(&mut self) {
        let _ = self.stop.wait_for(|stopped| *stopped).await;
    }

    /// Report that this face's listener is closed. Consumes the handle so the
    /// ack cannot be sent while the `serve` future is still alive.
    fn report_stopped(self) {
        if let Some(ack) = &self.ack {
            let _ = ack.try_send(self.face);
        }
    }
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
    /// Fired to close the ingest listener, and awaited until it is actually
    /// closed. `None` when the host never bound an ingest face (tests, and pods
    /// that only consume).
    stop_ingest: Option<Arc<StopSignal>>,
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
        stop_ingest: Option<Arc<StopSignal>>,
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

        // 1. Close the ingest listener: no new publisher connections. This
        //    **awaits** the faces reporting their listeners closed, so the seal
        //    below cannot run underneath a still-accepting acceptor. Returning
        //    from a bare `send` here is what left the ordering unproven, and
        //    with `notify_waiters` the signal could be lost outright.
        if let Some(stop) = &self.stop_ingest {
            let report = stop.stop(stop_deadline()).await;
            if report.timed_out {
                tracing::warn!(
                    label,
                    shard,
                    stopped = ?report.stopped,
                    timeout_ms = stop_deadline().as_millis() as u64,
                    "EHDB {label} face(s) did not confirm their listener closed before \
                     the deadline — sealing anyway, but a publisher may still be \
                     accepted during the seal"
                );
            } else {
                tracing::info!(
                    label,
                    shard,
                    faces = ?report.stopped,
                    "EHDB {label} ingest listener closed"
                );
            }

            // 2. Let already-accepted publishers land and get acked. Their
            //    connections are owned by tasks inside `ehdb-feed` and cannot be
            //    reached from here, so this window is what lets their appends
            //    reach the *pre-seal* part. Anything that misses it blocks on the
            //    held engine lock below and is never acked.
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

/// Wrap a face's `serve_*` future so firing its [`StopSignal`] closes its
/// listener, and report back once it has.
///
/// Dropping the future is what closes the listener; the already-accepted
/// connections are owned by tasks inside `ehdb-feed` and outlive this. The drop
/// is explicit below rather than left to the end of the `select!` expression,
/// because the ack that follows it is only meaningful if the listener is
/// already gone — the whole point of the handshake is that
/// [`WriterShutdown::run`] can treat "stop accepting" as done.
pub async fn until_stopped<F>(mut handle: StopHandle, serve: F)
where
    F: Future<Output = std::io::Result<()>> + Send + 'static,
{
    let face = handle.face();
    let mut serve = Box::pin(serve);
    let exited = tokio::select! {
        result = &mut serve => Some(result),
        _ = handle.signalled() => None,
    };
    // The listener's fd is released here, before the ack below.
    drop(serve);
    match exited {
        Some(result) => report_face_exit(face, result),
        None => tracing::info!(face, "EHDB {face} face stopped — listener closed"),
    }
    handle.report_stopped();
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

    /// A face that never finishes on its own, so the only way it can stop is the
    /// stop signal. Returning `Ok(())` would look like the face *exiting*, which
    /// is a different (and loudly reported) outcome.
    async fn never_returns() -> std::io::Result<()> {
        std::future::pending::<()>().await;
        Ok(())
    }

    /// Join a face task with a deadline. A lost stop signal leaves the face
    /// parked forever; without this the tests below would *hang* instead of
    /// failing, which in CI is the difference between a diagnosis and a
    /// timeout nobody reads.
    async fn join_stopped(task: tokio::task::JoinHandle<()>, face: &str) {
        match tokio::time::timeout(Duration::from_secs(5), task).await {
            Ok(joined) => joined.unwrap(),
            Err(_) => panic!(
                "the {face} face never stopped — it is parked on a stop signal it \
                 will never observe (noetl/ai-meta#209 defect 2)"
            ),
        }
    }

    /// The ordering is the whole fix: ingest must be told to stop *before* the
    /// cursor is persisted and the log sealed. A seal that runs while ingest is
    /// still accepting is the acked-and-lost window of noetl/ai-meta#209.
    ///
    /// Deliberately **no** sleep between spawning the face and running shutdown.
    /// The version of this test that predated the fix slept 20 ms first, to
    /// "let the fake face reach its `notified()` await" — which is precisely the
    /// ordering the bug needs to *avoid*, so the test could never see it. The
    /// spawn below is on a current-thread runtime and is therefore guaranteed
    /// not to have been polled when `run()` starts.
    #[tokio::test]
    async fn shutdown_stops_ingest_before_it_persists_and_seals() {
        no_quiesce();
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let stop = StopSignal::new();
        let seals = Arc::new(AtomicUsize::new(0));

        let ingest_order = Arc::clone(&order);
        let handle = stop.register("test-ingest");
        let ingest = tokio::spawn(async move {
            until_stopped(handle, never_returns()).await;
            ingest_order.lock().unwrap().push("ingest-stopped");
        });

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
        join_stopped(ingest, "ingest").await;

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
    }

    /// The signal must survive being fired before the face is ever polled.
    ///
    /// This is the unit-level twin of the integration repro. `tokio::spawn` on a
    /// current-thread runtime queues the task without polling it, so at the
    /// `stop()` below the face has definitely not registered anything. A
    /// `Notify::notify_waiters()` loses that signal permanently and this hangs
    /// until the deadline, reporting `timed_out`; the `watch` value does not.
    #[tokio::test]
    async fn the_stop_signal_survives_firing_before_the_face_is_first_polled() {
        let stop = StopSignal::new();
        let handle = stop.register("late-face");
        let face = tokio::spawn(until_stopped(handle, never_returns()));

        // No yield: the face task has never run.
        let report = stop.stop(Duration::from_secs(5)).await;

        assert!(
            !report.timed_out,
            "the stop signal was lost because the face had not registered yet — \
             this is the noetl/ai-meta#209 defect-2 lost wakeup"
        );
        assert_eq!(report.stopped, vec!["late-face"]);
        join_stopped(face, "late-face").await;
    }

    /// One signal, several faces — the events writer host's shape.
    ///
    /// This is why the signal is a `watch` and not a permit-storing
    /// `Notify::notify_one`: a single stored permit wakes exactly one waiter, so
    /// with `notify_one` the first face polled would consume it and the other
    /// two would park forever. Fired before any of them is polled, for the same
    /// reason as above.
    #[tokio::test]
    async fn one_signal_stops_every_registered_face() {
        let stop = StopSignal::new();
        let faces = ["ingest", "group-claim", "sse"];
        let tasks: Vec<_> = faces
            .iter()
            .map(|face| tokio::spawn(until_stopped(stop.register(face), never_returns())))
            .collect();

        let report = stop.stop(Duration::from_secs(5)).await;

        assert!(!report.timed_out, "not every face observed the stop signal");
        let mut stopped = report.stopped.clone();
        stopped.sort_unstable();
        assert_eq!(
            stopped,
            vec!["group-claim", "ingest", "sse"],
            "one signal must fan out to every registered face"
        );
        for (task, face) in tasks.into_iter().zip(faces) {
            join_stopped(task, face).await;
        }
    }

    /// Collapse the quiesce window for this binary, so the ordering below is
    /// proven by the stop handshake alone.
    ///
    /// With the default 250 ms quiesce, a `run()` that merely *fires* the signal
    /// and returns still usually seals after the listener closed — the sleep
    /// hides the missing barrier. Zero makes the seal follow the stop
    /// acknowledgement immediately, which is the property under test. Set once
    /// and never unset: a shorter quiesce is strictly stricter for every test in
    /// this binary, so there is nothing for a parallel test to race against.
    fn no_quiesce() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| std::env::set_var("NOETL_EHDB_SHUTDOWN_QUIESCE_MS", "0"));
    }

    /// The ordering guarantee stated in bytes on the wire: by the time the seal
    /// runs, the face's listener is **gone**, so nothing can be accepted — and
    /// therefore nothing can be acked — into a part opened after the seal.
    ///
    /// The probe is a re-bind of the face's own address from inside
    /// `seal_and_hold`. A connect probe could not do this: it only reports that
    /// *someone* is listening. A re-bind succeeds only if this face really
    /// dropped its listener.
    ///
    /// The face here is deliberately **slow to release** its listener, and the
    /// runtime is multi-threaded. Both are needed for the test to have any
    /// power: a face that releases instantly on a runtime that happens to
    /// schedule it first passes even with no barrier at all, so the assertion
    /// would be measuring the scheduler's goodwill rather than the sequence.
    /// A real acceptor has teardown work; this makes that cost explicit and
    /// large enough to observe.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_seal_runs_only_after_the_listener_is_closed() {
        no_quiesce();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        /// A face that never completes and is slow to give its listener back.
        ///
        /// Implemented as a hand-written `Future` rather than an `async` block
        /// on purpose: an async block only constructs its locals on its first
        /// poll, so a block dropped before it ever ran would release the
        /// listener instantly and the delay below would never arm. This owns
        /// the listener outright, so the cost is paid however it is dropped.
        struct SlowFace(Option<tokio::net::TcpListener>);
        impl Future for SlowFace {
            type Output = std::io::Result<()>;
            fn poll(
                self: Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                std::task::Poll::Pending
            }
        }
        impl Drop for SlowFace {
            fn drop(&mut self) {
                std::thread::sleep(Duration::from_millis(200));
                drop(self.0.take());
            }
        }

        let stop = StopSignal::new();
        let handle = stop.register("probe-ingest");
        let face = tokio::spawn(until_stopped(handle, SlowFace(Some(listener))));

        /// Records whether the face's port was free at the instant of the seal.
        struct PortProbe {
            addr: std::net::SocketAddr,
            free_at_seal: Arc<Mutex<Option<bool>>>,
        }
        impl Sealable for PortProbe {
            fn seal_and_hold(&self) -> Result<()> {
                let free = std::net::TcpListener::bind(self.addr).is_ok();
                *self.free_at_seal.lock().unwrap() = Some(free);
                Ok(())
            }
        }

        let free_at_seal = Arc::new(Mutex::new(None));
        let shutdown = WriterShutdown::new(
            "test-bus",
            0,
            Some(Arc::clone(&stop)),
            Box::new(|| Box::pin(async { Ok(()) })),
            vec![Arc::new(PortProbe {
                addr,
                free_at_seal: Arc::clone(&free_at_seal),
            })],
        );

        shutdown.run().await;
        join_stopped(face, "probe-ingest").await;

        assert_eq!(
            *free_at_seal.lock().unwrap(),
            Some(true),
            "the seal ran while {addr} was still accepting — a publisher could be \
             accepted and acked for a record landing in a part opened after the \
             seal, which is the acked-and-lost window of noetl/ai-meta#209"
        );
    }

    /// A face that will not stop must not stall the seal past the deadline. The
    /// seal is what prevents loss; `main`'s own budget is what stands between a
    /// slow shutdown and the SIGKILL that loses the tail outright.
    #[tokio::test]
    async fn a_face_that_will_not_stop_is_reported_and_the_seal_still_runs() {
        let stop = StopSignal::new();
        // Registered but never spawned: nothing will ever acknowledge it.
        let _stuck = stop.register("stuck-face");
        let seals = Arc::new(AtomicUsize::new(0));

        let started = std::time::Instant::now();
        let report = stop.stop(Duration::from_millis(50)).await;
        assert!(report.timed_out, "an unacknowledged face must be reported");
        assert!(started.elapsed() < Duration::from_secs(2));

        let shutdown = WriterShutdown::new(
            "test-bus",
            0,
            None,
            Box::new(|| Box::pin(async { Ok(()) })),
            vec![Arc::new(CountingSeal(Arc::clone(&seals)))],
        );
        shutdown.run().await;
        assert_eq!(seals.load(Ordering::SeqCst), 1);
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
