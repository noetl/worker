//! The per-test metric isolation must keep holding — noetl/worker#299, #302.
//!
//! # What broke
//!
//! `ehdb::metrics` state was process-wide. `metrics::test_guard()` serialised
//! access, but two test modules record into that state **without taking it**:
//! `reachability.rs` (7 recording calls, 0 guard takes) and parts of
//! `tier_client.rs`. So a guarded test's window was never actually exclusive and
//! a sibling's counts landed inside it:
//!
//! ```text
//! an unserved op must read 0, not be absent:
//!   noetl_ehdb_dataplane_ops_total{operation="tier_service.append",outcome="ok"} 1
//!   noetl_ehdb_dataplane_ops_total{operation="reachability",outcome="reached"} 1
//! ```
//!
//! ⚠ **The obvious fix was tried and reverted, twice.** Guarding all of
//! `tier_client`'s tests DEADLOCKS — `test_guard` returns a non-reentrant
//! `std::sync::MutexGuard` and those are `#[tokio::test]`s holding it across
//! `.await`. Guarding `reachability`'s took the suite from 12s to over 200s.
//!
//! # What was done
//!
//! `test_guard()` now also hands the calling thread its own `EhdbMetricsState`.
//! Tests on one thread run sequentially, so a thread-private state cannot race:
//! nothing is serialised, nothing is held across an `.await`, and both reverted
//! attempts' failure modes are structurally impossible.
//!
//! # ⚠ What this file guards, and why it is not a style rule
//!
//! An earlier version of this file banned exact-value assertions on metric text
//! outright. That rule was **measured to be wrong** and is not what is enforced
//! here: planting `assert_eq!(series_value(&text, health), Some(1))` and running
//! the suite 8/8 times never failed, because that series is written by exactly
//! one test. Banning it would have forbidden a safe pattern and, worse, implied
//! the unsafe ones were merely stylistic.
//!
//! What the isolation actually rests on is one load-bearing assumption, so that
//! is what is checked: **every `#[tokio::test]` under `src/ehdb/` runs on a
//! CURRENT-THREAD runtime.** A `flavor = "multi_thread"` test would run its
//! spawned tasks on other threads, those tasks would record into the global
//! state instead of the scope, and the flake would return — silently, as a
//! missing count rather than an error.

const EHDB_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/ehdb");

/// Source with comment lines stripped.
///
/// ⚠ Not cosmetic. The first version of this check FAILED against a clean tree,
/// because `metrics.rs`'s own doc comment explains the multi-thread hazard and
/// therefore contains the string it warns about. A matcher that reads comments
/// as code reports the documentation as the defect.
fn code_only(src: &str) -> String {
    src.lines()
        .filter(|l| {
            let t = l.trim_start();
            !t.starts_with("//") && !t.starts_with("/*") && !t.starts_with('*')
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn ehdb_sources() -> Vec<(String, String)> {
    std::fs::read_dir(EHDB_DIR)
        .expect("src/ehdb must exist")
        .filter_map(|e| {
            let p = e.ok()?.path();
            if p.extension()? != "rs" {
                return None;
            }
            Some((
                p.file_name()?.to_string_lossy().into_owned(),
                std::fs::read_to_string(&p).ok()?,
            ))
        })
        .collect()
}

/// ⭐ The assumption the whole isolation rests on.
#[test]
fn no_ehdb_test_runs_on_a_multi_thread_runtime() {
    let offenders: Vec<String> = ehdb_sources()
        .into_iter()
        .filter(|(_, src)| code_only(src).contains("multi_thread"))
        .map(|(name, _)| name)
        .collect();

    assert!(
        offenders.is_empty(),
        "these EHDB modules ask for a multi-thread tokio runtime: {offenders:?}\n\n\
         `metrics::test_guard()` isolates tests by giving each THREAD its own \
         metric state (noetl/worker#302). Under a multi-thread runtime a \
         spawned task runs on a different thread, so its records go to the \
         PROCESS-WIDE state instead of this test's scope.\n\n\
         That does not fail loudly — the test sees a count that never arrived, \
         and a sibling module's counts reappear in the shared state. It is \
         noetl/worker#299 returning as a missing number rather than an error.\n\n\
         If a multi-thread runtime is genuinely needed, the scope must move \
         from a thread-local to a task-local first."
    );
}

/// The scope must actually be consulted. A `state()` that ignored it would leave
/// every test above passing while isolating nothing.
#[test]
fn the_metric_accessor_consults_the_per_thread_scope() {
    let metrics = std::fs::read_to_string(format!("{EHDB_DIR}/metrics.rs")).unwrap();
    let accessor = metrics
        .split_once("fn state() -> &'static Mutex<EhdbMetricsState> {")
        .expect("the single metric state accessor")
        .1;
    let body = &accessor[..accessor.find("\n}").expect("accessor body")];

    assert!(
        body.contains("SCOPED_STATE"),
        "`state()` no longer consults the per-thread scope, so \
         `metrics::test_guard()` isolates nothing and noetl/worker#299 is back. \
         Body was:\n{body}"
    );
    assert!(
        metrics.contains("fn test_guard() -> TestGuard"),
        "`test_guard()` is what claims the thread's state; if its shape changed, \
         check it still calls `claim_thread_state()`"
    );
    assert!(
        metrics
            .split_once("pub(crate) fn test_guard() -> TestGuard {")
            .expect("test_guard")
            .1
            .contains("claim_thread_state()"),
        "`test_guard()` stopped claiming this thread's state — tests would fall \
         back to the process-wide one and race again"
    );
}

/// The positive control: this file's checks must be able to FAIL, or a green
/// run means "the matcher is broken", not "the code is correct".
#[test]
fn the_checks_can_actually_fail() {
    let planted = "#[tokio::test(flavor = \"multi_thread\")]\nasync fn x() {}";
    assert!(
        code_only(planted).contains("multi_thread"),
        "the multi-thread matcher cannot see the thing it looks for"
    );

    // …and it must NOT see a comment that merely discusses it. `metrics.rs`
    // does exactly this, and it made the first run of this check red.
    let discussed = "/// no `flavor = \"multi_thread\"` anywhere under src/ehdb\nfn x() {}";
    assert!(
        !code_only(discussed).contains("multi_thread"),
        "the matcher reads comments as code — it would report documentation as \
         the defect, which is exactly how this check first failed"
    );

    let gutted = "fn state() -> &'static Mutex<EhdbMetricsState> {\n    STATE.get()\n}";
    let body = gutted.split_once("{").unwrap().1;
    assert!(
        !body.contains("SCOPED_STATE"),
        "the accessor matcher would pass a state() that ignores the scope"
    );
}
