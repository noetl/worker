//! Worker-local drive (noetl/ai-meta#558, Stage 1 of the latency-reduction plan,
//! RFC #556).
//!
//! Today every completed step pays two hand-offs before the next one starts: the
//! server dispatches `__orchestrate__` to the system pool, a system worker builds
//! the state from the WAL spine and runs `system/orchestrate`, the server applies
//! the result and publishes the next command, and a user worker claims it off the
//! feed. With `NOETL_WORKER_LOCAL_DRIVE=true` the worker that completed the step
//! runs that same drive itself, against the server's
//! `POST /api/executions/{id}/advance` (server `NOETL_SERVER_ADVANCE_API`):
//!
//! 1. its `command.completed` carries `x-noetl-local-drive: 1` (the server then
//!    suppresses the system-pool dispatch for that event);
//! 2. `begin` returns the SAME `__orchestrate__` args the system pool would get,
//!    with the execution's event tail;
//! 3. the worker builds the drive input with the system pool's own builder
//!    ([`crate::state_builder::build_offserver_input`]) on an execution-scoped
//!    index, and runs `system/orchestrate` in its own WASM host;
//! 4. `apply` returns the commands the result issued; a same-pool one is claimed
//!    directly (the normal claim endpoint, so the feed delivery that follows finds
//!    it claimed and is acked) and run on the SAME slot.
//!
//! Anything that cannot be done locally — no `begin` (flag-off server: 404), an
//! incomplete spine, a WASM error — ends in `abort` or a refused `begin`, and the
//! server dispatches the normal drive at once. Default OFF; with the flag off the
//! worker never sets the header and never calls the endpoint.
//!
//! **Memory.** The index holds only executions this worker is chaining right now:
//! an execution is evicted when its chain leaves this worker (no same-pool next
//! command was taken here), so residency is bounded by the slot count, not by the
//! cluster's active executions. Prod's user pool has no WAL index at all; this
//! does not add one.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};

/// `NOETL_WORKER_LOCAL_DRIVE` (default off).
pub fn enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("NOETL_WORKER_LOCAL_DRIVE")
            .map(|v| matches!(v.trim(), "1" | "true" | "TRUE" | "True"))
            .unwrap_or(false)
    }) && !DISABLED.load(Ordering::Relaxed)
}

/// Set when the server answered `begin` with 404 (its advance API is off): stop
/// marking completions for the rest of the process, so a flag-on worker against
/// a flag-off server costs one probe, not one per step.
static DISABLED: AtomicBool = AtomicBool::new(false);

pub fn disable_for_process(reason: &str) {
    if !DISABLED.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            reason,
            "worker-local drive disabled for this process: the server does not serve \
             POST /api/executions/{{id}}/advance (NOETL_SERVER_ADVANCE_API off) — \
             every step takes today's path (noetl/ai-meta#558)"
        );
        crate::metrics::record_local_drive("disabled_no_server_api");
    }
}

/// The header the server keys the claim on.
pub const HEADER: &str = "x-noetl-local-drive";

/// What the executor recorded about the completion it just emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Completed {
    /// The `command.completed` event id (producer-stamped).
    pub trigger_event_id: i64,
}

#[derive(Default)]
struct Slot {
    /// The event id to send the header on.
    mark: Option<i64>,
    completed: Option<Completed>,
}

tokio::task_local! {
    static SLOT: RefCell<Slot>;
}

/// Run `fut` with a drive slot: inside it the executor marks its
/// `command.completed` and records it. Returns the future's output and what was
/// recorded (None when the command did not end in a plain `command.completed`).
pub async fn with_slot<F, T>(fut: F) -> (T, Option<Completed>)
where
    F: std::future::Future<Output = T>,
{
    SLOT.scope(RefCell::new(Slot::default()), async move {
        let out = fut.await;
        let done = SLOT.with(|s| s.borrow().completed);
        (out, done)
    })
    .await
}

/// True inside [`with_slot`] (the executor then marks its completion).
pub fn requested() -> bool {
    SLOT.try_with(|_| ()).is_ok()
}

/// Mark `event_id` as the one to carry [`HEADER`] and record it as the
/// completion to drive from.
pub fn mark_completed(event_id: i64) {
    let _ = SLOT.try_with(|s| {
        let mut s = s.borrow_mut();
        s.mark = Some(event_id);
        s.completed = Some(Completed {
            trigger_event_id: event_id,
        });
    });
}

/// Undo [`mark_completed`] (the emit failed; nothing to drive).
pub fn clear_completed() {
    let _ = SLOT.try_with(|s| {
        let mut s = s.borrow_mut();
        s.mark = None;
        s.completed = None;
    });
}

/// Whether the event being emitted must carry [`HEADER`].
pub fn header_for(event_id: Option<i64>) -> bool {
    match event_id {
        Some(id) => SLOT
            .try_with(|s| s.borrow().mark == Some(id))
            .unwrap_or(false),
        None => false,
    }
}

/// Command event ids this process has claimed (bounded; oldest dropped).
fn claimed() -> &'static std::sync::Mutex<(std::collections::HashSet<i64>, std::collections::VecDeque<i64>)> {
    static C: std::sync::OnceLock<
        std::sync::Mutex<(std::collections::HashSet<i64>, std::collections::VecDeque<i64>)>,
    > = std::sync::OnceLock::new();
    C.get_or_init(Default::default)
}
const CLAIMED_CAP: usize = 8192;

/// Record a claim attempt for `event_id`; false when this process already
/// claimed it (the second delivery must not run it again).
pub fn begin_claim(event_id: i64) -> bool {
    let mut c = claimed().lock().unwrap();
    if !c.0.insert(event_id) {
        return false;
    }
    c.1.push_back(event_id);
    while c.1.len() > CLAIMED_CAP {
        if let Some(old) = c.1.pop_front() {
            c.0.remove(&old);
        }
    }
    true
}

/// Forget a claim attempt that did not claim (so a redelivery may retry it).
pub fn release_claim(event_id: i64) {
    let mut c = claimed().lock().unwrap();
    c.0.remove(&event_id);
    c.1.retain(|e| *e != event_id);
}

/// NATS-style subject match: `*` is one token, `>` the rest (one or more).
pub fn subject_matches(filter: &str, subject: &str) -> bool {
    let f: Vec<&str> = filter.split('.').collect();
    let s: Vec<&str> = subject.split('.').collect();
    for (i, tok) in f.iter().enumerate() {
        match *tok {
            ">" => return s.len() > i,
            "*" => {
                if i >= s.len() {
                    return false;
                }
            }
            t => {
                if s.get(i) != Some(&t) {
                    return false;
                }
            }
        }
    }
    f.len() == s.len()
}

/// This worker's feed filter (`NOETL_FEED_FILTER_SUBJECT`), the pool it serves.
pub fn own_filter_subject() -> Option<String> {
    std::env::var("NOETL_FEED_FILTER_SUBJECT")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// The index of executions this worker is chaining (see the module doc).
pub fn new_index() -> crate::state_builder::SharedWalIndex {
    crate::state_builder::SharedWalIndex::new(crate::state_builder::WalEventIndex::with_order_policy(
        crate::state_builder::spine_order(),
        crate::state_builder::EvictionPolicy::from_env(),
    ))
}

/// The outcome of one local drive.
#[derive(Debug)]
pub enum Outcome {
    /// The next command, already claimed for this worker.
    Next(Box<noetl_executor::worker::source::Command>),
    /// Driven and applied; nothing for this worker to run next.
    Applied,
    /// Not driven here; the server took today's path.
    Fallback(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_matching() {
        assert!(subject_matches("noetl.commands.shared.>", "noetl.commands.shared.7.x"));
        assert!(subject_matches("noetl.commands.shared.>", "noetl.commands.shared.x"));
        assert!(!subject_matches("noetl.commands.shared.>", "noetl.commands.shared"));
        assert!(!subject_matches("noetl.commands.shared.>", "noetl.commands.system.x"));
        assert!(subject_matches("noetl.commands.*.x", "noetl.commands.shared.x"));
        assert!(!subject_matches("noetl.commands.*.x", "noetl.commands.shared.y"));
        assert!(subject_matches("a.b", "a.b"));
        assert!(!subject_matches("a.b", "a.b.c"));
    }

    #[test]
    fn a_command_is_claimed_once_per_process() {
        assert!(begin_claim(-77));
        assert!(!begin_claim(-77), "the second delivery is refused");
        release_claim(-77);
        assert!(begin_claim(-77), "a released attempt may be retried");
        release_claim(-77);
    }

    #[tokio::test]
    async fn slot_marks_only_inside_scope() {
        assert!(!requested());
        mark_completed(5); // no slot: a no-op
        assert!(!header_for(Some(5)));
        let ((), done) = with_slot(async {
            assert!(requested());
            mark_completed(42);
            assert!(header_for(Some(42)));
            assert!(!header_for(Some(41)));
            assert!(!header_for(None));
        })
        .await;
        assert_eq!(done, Some(Completed { trigger_event_id: 42 }));
        let ((), none) = with_slot(async {
            mark_completed(1);
            clear_completed();
        })
        .await;
        assert_eq!(none, None);
    }
}
