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
//!   stop accepting ingest  ->  quiesce  ->  persist cursor  ->  seal and close
//! ```
//!
//! A process that hosts more than one writer runs those phases **across every
//! host at once** — every ingest stops, then one quiesce, then every cursor,
//! then every seal — rather than finishing one host before starting the next.
//! See [`seal_all`] for why host-at-a-time is not merely slower but lossy.
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
//! ## Seal and close
//!
//! The quiesce window is a mitigation, not a guarantee, so the seal itself
//! closes the hole: [`Sealable::seal_and_hold`] seals the active part and closes
//! the writer, after which every append fails. A refused publish is exactly the
//! case the contract already handles — the server's publish redial-retry
//! republishes it to the replacement writer (noetl/server#290). The alternative,
//! doing nothing, lets a post-seal append open a new active part that the next
//! incarnation cannot see: acked and lost, which is the bug.
//!
//! ### This used to leak the engine mutex guard, and it cost us the events log
//!
//! The first version of this closed the same hole by `std::mem::forget`ing the
//! guard so the mutex was never released — any post-seal appender blocked on it
//! forever and could never be acked. Correct on paper, and it passed every idle
//! test. Under load it lost data (noetl/ai-meta#226).
//!
//! `FeedWriter::append_batch` takes a **`std::sync::Mutex`**, *blocking*, from
//! inside an async task. Parking appenders on it therefore parks tokio worker
//! threads, and there are only as many of those as CPUs — two, on the prod
//! worker. With a backlog in flight at SIGTERM the runtime had no threads left,
//! so [`seal_all`] never reached the second host and the events log went out
//! unsealed: the reopened log came back 390 records below its persisted cursor
//! and every consumer group resumed `clamped=true`. Worker threads also drive
//! the timer, so `main`'s 15 s budget could not fire either — the shutdown ate
//! the clock that was supposed to bound it, which is why the prod log ends
//! mid-sequence with no expiry line and no `Worker stopped`.
//!
//! `FeedWriter::seal_and_close` keeps the guarantee with a flag set before the
//! lock is taken and re-checked under it, and releases the lock. Nothing blocks;
//! a post-seal publisher gets an error it can act on instead of a hang.
//!
//! This still runs only on the terminal path. [`seal_all`] must never be called
//! on a worker that intends to keep serving — the writers do not reopen.
//!
//! # What this does NOT fix
//!
//! The hard-kill half of noetl/ai-meta#209. SIGKILL / OOM / node loss skips all
//! of the above and still loses the unsealed tail — up to `seal_max_records`
//! (1024 by default) per shard — despite every record having been `fsync`ed and
//! acked. Closing that needs L0-level replay of the local active part on open,
//! an `ehdb-l0` change tracked separately on the same issue.
//!
//! The KV face **is** now sealed ([`KvSeal`]): `ehdb_feed::KvCoordinator`
//! exposes `flush_and_wait`, and the events host registers it via
//! [`WriterShutdown::push_sealable`] (late, because the KV store is constructed
//! after the shutdown is built). Since the L0 active-part recovery in the same
//! issue an unsealed KV part is also *replayed* on the next open rather than
//! destroyed, so the crash path no longer loses sessions/requests either.

use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

use ehdb_feed::FeedWriter;
use ehdb_l0::Dataset;
use serde::de::DeserializeOwned;
use serde::Serialize;

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

/// One host's seal work, detached from its borrowed [`WriterShutdown`] so the
/// whole set can be moved onto a single blocking thread.
type HostSeal = (&'static str, u32, Vec<Arc<dyn Sealable>>);

/// An L0 engine that must be sealed before the process exits.
pub trait Sealable: Send + Sync + 'static {
    /// Seal the active part, wait for its upload, and close the writer so
    /// nothing can append — and therefore nothing can be *acked* — afterwards.
    ///
    /// Must **not** hold a lock past its own return. See
    /// [`EngineSeal::seal_and_hold`] for the incident that rule comes from.
    fn seal_and_hold(&self) -> Result<()>;
}

/// [`Sealable`] over an `ehdb_feed::KvCoordinator` (noetl/ai-meta#209).
///
/// The KV face was the last unsealed engine on the events host: the coordinator
/// held its `KvStore` privately with no flush, so a host could seal its feed
/// writers on SIGTERM and still leave sessions and request state sitting in an
/// unsealed part. `KvCoordinator::flush_and_wait` now exposes it.
///
/// Since the L0 active-part recovery in the same issue an unsealed KV part is
/// *replayed* on the next open rather than destroyed, so this is no longer the
/// difference between kept and lost. It is still the difference between durable
/// in the object store and recoverable by any replica, versus local-only on a
/// volume that may not come back.
pub struct KvSeal {
    kv: Arc<ehdb_feed::KvCoordinator>,
}

impl KvSeal {
    pub fn new(kv: Arc<ehdb_feed::KvCoordinator>) -> Self {
        Self { kv }
    }
}

impl Sealable for KvSeal {
    /// `Handle::block_on` is correct here and `block_in_place` is not:
    /// [`seal_all`] runs every sealable inside `spawn_blocking`, which is a
    /// blocking thread and **not** an async context, so blocking on the handle
    /// is permitted and cannot stall a runtime worker.
    ///
    /// Holds no lock past its own return — the guard lives inside
    /// `flush_and_wait` — so it does not repeat the leaked-`MutexGuard` incident
    /// documented on [`EngineSeal::seal_and_hold`].
    fn seal_and_hold(&self) -> Result<()> {
        let kv = Arc::clone(&self.kv);
        tokio::runtime::Handle::current().block_on(async move { kv.flush_and_wait().await })?;
        Ok(())
    }
}

/// [`Sealable`] over a `FeedWriter`.
pub struct EngineSeal<D: Dataset> {
    writer: Arc<FeedWriter<D>>,
}

impl<D> EngineSeal<D>
where
    D: Dataset,
    D::Record: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    pub fn new(writer: Arc<FeedWriter<D>>) -> Self {
        Self { writer }
    }
}

impl<D> Sealable for EngineSeal<D>
where
    D: Dataset + Send + Sync + 'static,
    D::Record: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// **This used to leak the engine's `MutexGuard`, and that cost us the
    /// events log (noetl/ai-meta#226).**
    ///
    /// The old shape was `engine.lock()` → `flush_and_wait_uploads()` →
    /// `std::mem::forget(guard)`, holding the lock through process exit so no
    /// append could be acked after the seal. It does stop appends — by parking
    /// every appender on a mutex that is never released. Every append path in
    /// `ehdb-feed` runs inside an async task and takes that `std::sync::Mutex`
    /// *blocking*, so each parked appender burns a whole tokio worker thread.
    /// Under load there are more parked appenders than worker threads, the
    /// runtime starves, and [`seal_all`] never gets to the **second** host —
    /// which is exactly what prod's shutdown log shows: `hosts=2`, one seal,
    /// then silence. Worker threads also drive the timer, so `main`'s 15 s
    /// budget could not fire either.
    ///
    /// `FeedWriter::seal_and_close` keeps the same guarantee with a flag
    /// re-checked under the lock, and releases the lock. A post-seal append now
    /// fails fast — a better contract than hanging, since the publisher retries
    /// against the replacement writer.
    fn seal_and_hold(&self) -> Result<()> {
        self.writer.seal_and_close().map_err(Into::into)
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

    /// Register another engine to seal on shutdown (noetl/ai-meta#209).
    ///
    /// Exists because the KV face is constructed *after* the host builds its
    /// `WriterShutdown` — the events host binds its feed writer, builds the
    /// shutdown, and only then opens the KV store. Without this the KV engine
    /// simply could not be reached from the shutdown sequence, whatever the
    /// coordinator exposed.
    pub fn push_sealable(&mut self, sealable: Arc<dyn Sealable>) {
        self.sealables.push(sealable);
    }

    /// Phase 1 — close this host's ingest listener: no new publisher
    /// connections. **Awaits** the faces reporting their listeners closed, so
    /// the seal cannot run underneath a still-accepting acceptor. Returning from
    /// a bare `send` here is what left the ordering unproven, and with
    /// `notify_waiters` the signal could be lost outright.
    async fn stop_ingest(&self) {
        let (label, shard) = (self.label, self.shard);
        let Some(stop) = &self.stop_ingest else {
            return;
        };
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
    }

    /// Phase 3 — persist the claim/group cursor. Cheap, and a cursor behind the
    /// log is always safe while a log behind the cursor needs the resume clamp.
    async fn persist_cursor(&self) {
        let (label, shard) = (self.label, self.shard);
        match (self.persist_cursor)().await {
            Ok(()) => tracing::info!(label, shard, "EHDB {label} cursor persisted on shutdown"),
            Err(error) => {
                tracing::warn!(label, shard, %error, "EHDB {label} cursor persist failed")
            }
        }
    }

    /// Run the full sequence for this host alone. Kept for single-host callers
    /// and tests; multi-host processes must use [`seal_all`], which interleaves
    /// the phases across hosts rather than finishing one host before starting
    /// the next.
    pub async fn run(&self) {
        seal_all(std::slice::from_ref(self)).await;
    }
}

/// What [`seal_all`] achieved. Logged, and returned so a caller can assert on it.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SealReport {
    /// Hosts whose every engine sealed cleanly.
    pub sealed: usize,
    /// Hosts the sequence was asked to seal.
    pub hosts: usize,
}

impl SealReport {
    /// Did every host seal? The only outcome that makes the next incarnation's
    /// resume exact.
    pub fn complete(&self) -> bool {
        self.sealed == self.hosts
    }
}

/// Seal **every** writer host this process owns, before the process exits
/// (noetl/ai-meta#209, #226).
///
/// # Why the phases interleave across hosts
///
/// The obvious shape — `for host in hosts { host.run().await }` — is what
/// shipped in v5.92.0, and under load it sealed the command bus and never
/// reached the events feed. Two independent reasons, both fixed here:
///
/// 1. **The old seal held a lock through exit** and starved the runtime, so the
///    loop's own continuation was never polled again. That is fixed in
///    [`EngineSeal::seal_and_hold`], which no longer holds anything.
/// 2. **Even with a well-behaved seal, host-at-a-time is the wrong order.**
///    Host 1's whole sequence — including its `stop_deadline` and its quiesce —
///    runs before host 2's ingest is so much as told to stop, so host 2 keeps
///    accepting and appending for the entire time host 1 takes, and then spends
///    its own budget from a standing start. `main`'s 15 s budget is shared, so a
///    slow first host can consume the budget the second one needed. Stopping
///    every ingest first also means the single quiesce window covers all hosts
///    at once instead of being paid per host.
///
/// So the order is: stop **every** ingest → quiesce **once** → persist **every**
/// cursor → seal **every** engine.
///
/// The seal phase runs on **one** blocking thread for all hosts. `spawn_blocking`
/// per host would put a scheduler round-trip between them, which is precisely
/// the gap the old code died in; one task means that once sealing starts,
/// nothing outside it has to be scheduled for it to finish.
pub async fn seal_all(hosts: &[WriterShutdown]) -> SealReport {
    if hosts.is_empty() {
        return SealReport::default();
    }
    tracing::info!(hosts = hosts.len(), "sealing EHDB writer hosts before exit");

    // 1. Every ingest listener closes before any of them quiesces or seals.
    futures::future::join_all(hosts.iter().map(|h| h.stop_ingest())).await;

    // 2. One quiesce for all of them: let already-accepted publishers land and
    //    get acked. Their connections are owned by tasks inside `ehdb-feed` and
    //    cannot be reached from here, so this window is what lets their appends
    //    reach the *pre-seal* part. Anything that misses it is refused by the
    //    closed writer below and republished to the replacement.
    //
    //    Skipped when no host bound an ingest face — there is nothing in flight.
    if hosts.iter().any(|h| h.stop_ingest.is_some()) {
        tokio::time::sleep(quiesce()).await;
    }

    // 3. Cursors, concurrently — independent per host, and all cheap.
    futures::future::join_all(hosts.iter().map(|h| h.persist_cursor())).await;

    // 4. Seal every host on one blocking thread. `flush_and_wait_uploads` blocks
    //    on the uploader's condvar, so it must not run on an async worker.
    let hosts_len = hosts.len();
    let sealables: Vec<HostSeal> = hosts
        .iter()
        .map(|h| (h.label, h.shard, h.sealables.clone()))
        .collect();
    let sealed = tokio::task::spawn_blocking(move || {
        sealables
            .into_iter()
            .filter(|(label, shard, sealables)| {
                let mut all = true;
                for sealable in sealables {
                    match sealable.seal_and_hold() {
                        Ok(()) => {
                            tracing::info!(label, shard, "EHDB {label} log sealed on shutdown")
                        }
                        Err(error) => {
                            all = false;
                            tracing::error!(label, shard, %error, "EHDB {label} seal failed");
                        }
                    }
                }
                all
            })
            .count()
    })
    .await
    .unwrap_or_else(|error| {
        tracing::error!(%error, "EHDB seal task panicked — no host is known to have sealed");
        0
    });

    let report = SealReport {
        sealed,
        hosts: hosts_len,
    };
    // A partial seal used to be invisible until the *next* boot reported
    // `clamped=true` on a log below its own cursor. Say it here, at ERROR, while
    // the operator is still watching the pod terminate (noetl/ai-meta#226).
    if report.complete() {
        tracing::info!(
            hosts = report.hosts,
            "EHDB writer hosts all sealed — shutdown complete"
        );
    } else {
        tracing::error!(
            sealed = report.sealed,
            hosts = report.hosts,
            "EHDB writer hosts did NOT all seal — the unsealed tail of the \
             unsealed host(s) will be lost and their next resume will clamp"
        );
    }
    crate::metrics::record_shutdown_seal(report.sealed, report.hosts);
    report
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
    use anyhow::anyhow;
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

    /// noetl/ai-meta#209 — a late registration is sealed like any other engine.
    ///
    /// The KV face is constructed *after* its host builds the `WriterShutdown`,
    /// so without `push_sealable` it could not be reached from the shutdown
    /// sequence at all. This asserts the registration actually takes part in the
    /// seal rather than being accepted and dropped.
    #[tokio::test]
    async fn a_late_registered_sealable_is_sealed_too() {
        no_quiesce();
        let early = Arc::new(AtomicUsize::new(0));
        let late = Arc::new(AtomicUsize::new(0));

        let mut host = WriterShutdown::new(
            "test-host",
            0,
            None,
            Box::new(|| Box::pin(async { Ok(()) })),
            vec![Arc::new(CountingSeal(Arc::clone(&early)))],
        );
        host.push_sealable(Arc::new(CountingSeal(Arc::clone(&late))));

        let report = seal_all(&[host]).await;
        assert!(report.complete(), "host must report as sealed");
        assert_eq!(early.load(Ordering::SeqCst), 1, "the engine bound up front");
        assert_eq!(
            late.load(Ordering::SeqCst),
            1,
            "the late registration must be sealed too, or the KV face is silently skipped"
        );
    }

    /// A late registration that FAILS must fail the host, not be swallowed —
    /// otherwise an unsealed KV face would report as a clean shutdown.
    #[tokio::test]
    async fn a_failing_late_sealable_fails_the_host() {
        no_quiesce();
        struct Failing;
        impl Sealable for Failing {
            fn seal_and_hold(&self) -> Result<()> {
                Err(anyhow!("kv seal failed"))
            }
        }
        let mut host = WriterShutdown::new(
            "test-host",
            0,
            None,
            Box::new(|| Box::pin(async { Ok(()) })),
            vec![],
        );
        host.push_sealable(Arc::new(Failing));

        let report = seal_all(&[host]).await;
        assert!(
            !report.complete(),
            "a failed seal must be visible, not reported as a clean shutdown"
        );
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
