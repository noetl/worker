//! The shard-election **driver** — C2 of [ai-meta#460].
//!
//! [`ShardElection`] (noetl/ehdb#331) and the Kubernetes [`LeaseStore`]
//! ([`super::lease_store`]) were both implemented, tested and merged before this
//! module existed — and neither had a call site. Every writer's epoch was
//! therefore `0`, and `render_election()` published a hard-coded `0` with a
//! comment saying so. That is this codebase's recurring defect, not a gap in
//! coverage: a mechanism that exists, is registered, is documented, and never
//! runs. This module is the call site.
//!
//! ## What it does
//!
//! One OS thread per writer process. Every `renew` interval it calls
//! [`ShardElection::try_acquire`] (when it holds nothing) or
//! [`ShardElection::renew`] (when it does), and publishes the result through
//! [`render`] so the epoch is readable on `/metrics` instead of inferable from
//! the source.
//!
//! ## ⚠ Shadow only — this issues a token, it does not gate a write
//!
//! Nothing in the write path consults [`ElectionState::epoch`]. Flipping fencing
//! to `enforce` is C3 and a separate, owner-gated decision, because it makes the
//! API server a dependency of the NoETL write path. The non-negotiable order is
//! that this must be live and **minting** before enforce is armed: a fence
//! enforces *against* an epoch, so arming it while nothing mints fails every
//! writer at once.
//!
//! ## ⚠ Why the unit of exclusion is the pod ordinal, not the bus
//!
//! A writer pod hosts **two** L0 engines (command bus and event bus), and the
//! entrypoint derives both shards from the same StatefulSet ordinal. The thing
//! being excluded is *two processes owning one ordinal's volumes*, not two buses
//! — both engines are backed by the same pod's PVCs. So one lease per ordinal is
//! the right granularity, and a per-bus lease would be two names for one
//! mutual-exclusion problem.
//!
//! ## ⚠⚠ Why the identity carries a per-process suffix
//!
//! StatefulSet pod names are **stable**: the writer is always
//! `noetl-cmdbus-writer-0`. An identity of just the pod name would make a
//! *restarted* writer look like the same holder to its predecessor's lease —
//! [`ShardElection::renew`] would succeed and the new process would silently
//! inherit the old epoch. Fencing could then not distinguish the restarted
//! writer from a zombie predecessor, which is the split-brain the epoch exists
//! to catch. The identity is `<hostname>#<uuid>`, minted once per process, so a
//! restart is a different holder: it waits out the old lease and acquires with
//! `transitions + 1`.
//!
//! The cost is explicit and is the failover window: a restarted writer cannot
//! acquire until the previous lease **expires**, so it is unelected for up to
//! `lease_secs` (default 15 s).
//!
//! ## ⚠ Why its own OS thread rather than a tokio task
//!
//! [`KubeLeaseStore`] bridges the sync `LeaseStore` trait onto `reqwest` by
//! calling `block_on` on a runtime of its own, and tokio panics when `block_on`
//! runs on a thread that is already driving tasks. A plain `tokio::spawn` here
//! would be a panic on the first tick — in the writer, in prod, behind a flag
//! that had looked harmless. A dedicated thread also means a stalled API server
//! cannot stall the writer's reactor: the worst case is a stale epoch, not
//! back-pressure on dispatch.
//!
//! ## ⚠ Fail-soft: wrong RBAC must degrade, not crash-loop
//!
//! Every error is counted, logged (rate-limited) and otherwise ignored; the loop
//! never exits and never panics. This is deliberate and load-bearing. The
//! prerequisite for enabling this is a Role binding that does not exist yet, so
//! the *first* thing a misconfiguration produces is `403 Forbidden` on every
//! tick — and a writer that exits on that would be a crash-loop on a
//! StatefulSet whose restart behaviour is the riskiest thing about this change.
//!
//! What is **not** fail-soft is the claim to hold a token.
//! [`ElectionState::token_valid`] is `holds_lease && contact is fresher than the
//! lease duration`, so once contact has been lost for longer than the lease
//! could survive, this writer stops claiming a usable token — without needing to
//! reach the API server to find that out. A lease we cannot renew is a lease
//! another node is entitled to take.
//!
//! [ai-meta#460]: https://github.com/noetl/ai-meta/issues/460

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ehdb_reference::election::{
    Clock, ElectionOutcome, LeaseStore, ShardElection, SystemClock, DEFAULT_LEASE_DURATION_SECS,
    DEFAULT_RENEW_INTERVAL_SECS,
};
use ehdb_reference::EhdbError;

use super::lease_store::{election_enabled, KubeLeaseStore};

/// Lease duration override. Longer = fewer renewals and more tolerance of a
/// slow API server, but a proportionally longer failover window.
pub const LEASE_SECS_ENV: &str = "NOETL_EHDB_ELECTION_LEASE_SECS";
/// Renewal interval override.
pub const RENEW_SECS_ENV: &str = "NOETL_EHDB_ELECTION_RENEW_SECS";

/// Error reasons, as a **closed** label set.
///
/// ⚠ Pinned at 0 unconditionally by [`render`]. A labelled metric is absent from
/// a Prometheus scrape until something increments it, so an unpinned
/// `reason="forbidden"` would be missing both when RBAC is correct and when this
/// build predates the label — two states an operator must be able to tell apart.
pub const ERROR_REASONS: [&str; 4] = ["forbidden", "unauthorized", "unreachable", "other"];

const FORBIDDEN: usize = 0;
const UNAUTHORIZED: usize = 1;
const UNREACHABLE: usize = 2;
const OTHER: usize = 3;

/// Everything the scrape needs, written by the driver thread and read by
/// `/metrics`.
#[derive(Debug, Default)]
pub struct ElectionState {
    shard: AtomicU64,
    epoch: AtomicU64,
    holds: AtomicBool,
    /// Wall-clock millis of the last **successful** store call. `0` = never.
    last_contact_millis: AtomicU64,
    lease_duration_secs: AtomicU64,
    renew_interval_secs: AtomicU64,
    acquired_total: AtomicU64,
    renewed_total: AtomicU64,
    lost_total: AtomicU64,
    held_by_other_total: AtomicU64,
    errors: [AtomicU64; 4],
}

impl ElectionState {
    /// Whether this writer holds a token it could legitimately act on.
    ///
    /// ⚠ Not the same as `holds`. `holds` is what the last successful call said;
    /// this additionally requires that the call was recent enough that the lease
    /// cannot have lapsed since. Fencing in `enforce` mode must consult *this*,
    /// not `holds`, or a writer partitioned from the API server would keep
    /// asserting a token that another node has already taken over.
    pub fn token_valid(&self, now_millis: u64) -> bool {
        if !self.holds.load(Ordering::Relaxed) {
            return false;
        }
        let last = self.last_contact_millis.load(Ordering::Relaxed);
        if last == 0 {
            return false;
        }
        let window = self
            .lease_duration_secs
            .load(Ordering::Relaxed)
            .saturating_mul(1000);
        now_millis.saturating_sub(last) < window
    }

    fn note_contact(&self, now_millis: u64) {
        // Never move the marker backwards: a clock that steps back would
        // otherwise read as fresher contact than we have.
        self.last_contact_millis
            .fetch_max(now_millis, Ordering::Relaxed);
    }

    fn record(&self, outcome: ElectionOutcome, now_millis: u64) {
        self.note_contact(now_millis);
        match outcome {
            ElectionOutcome::Acquired { epoch } => {
                self.epoch.store(epoch, Ordering::Relaxed);
                self.holds.store(true, Ordering::Relaxed);
                self.acquired_total.fetch_add(1, Ordering::Relaxed);
            }
            ElectionOutcome::Renewed { epoch } => {
                self.epoch.store(epoch, Ordering::Relaxed);
                self.holds.store(true, Ordering::Relaxed);
                self.renewed_total.fetch_add(1, Ordering::Relaxed);
            }
            ElectionOutcome::HeldByOther { .. } => {
                // ⚠ The epoch is NOT cleared. It is the last token this writer
                // held, and an operator reading a non-zero epoch next to
                // holds_lease=0 is reading the truth: we were elected and are
                // not now. `token_valid` is the field that says whether it may
                // be acted on.
                self.holds.store(false, Ordering::Relaxed);
                self.held_by_other_total.fetch_add(1, Ordering::Relaxed);
            }
            ElectionOutcome::Lost => {
                self.holds.store(false, Ordering::Relaxed);
                self.lost_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn record_error(&self, err: &EhdbError) {
        self.errors[classify(err)].fetch_add(1, Ordering::Relaxed);
        // ⚠ `holds` is deliberately left alone. A failed renew does not prove
        // the lease is gone — it proves we cannot see it. Clearing the flag here
        // would report a handover that may not have happened; leaving it lets
        // `token_valid` expire it on the clock instead, which is the bound we
        // can actually defend.
    }
}

/// Which `reason` label an error counts under.
///
/// ⚠ Substring classification over [`KubeLeaseStore`]'s message, because the
/// trait returns `EhdbError` and the HTTP status does not survive as structure.
/// `classification_matches_the_adapters_wording` pins the adapter's literals so
/// this cannot rot silently into everything being `other`.
fn classify(err: &EhdbError) -> usize {
    let m = err.to_string();
    if m.contains("403 Forbidden") {
        FORBIDDEN
    } else if m.contains("401 Unauthorized") {
        UNAUTHORIZED
    } else if m.contains("k8s request failed") {
        UNREACHABLE
    } else {
        OTHER
    }
}

static STATE: OnceLock<Arc<ElectionState>> = OnceLock::new();

/// The live state, or `None` when no election is running in this process.
pub fn state() -> Option<&'static Arc<ElectionState>> {
    STATE.get()
}

/// One iteration. Split out from the thread so the loop's behaviour is testable
/// against [`ehdb_reference::election::InMemoryLeaseStore`] and against a store
/// that only fails — without threads, sleeps, or an API server.
pub fn tick<S: LeaseStore, C: Clock>(
    election: &ShardElection<S, C>,
    state: &ElectionState,
    now_millis: u64,
) {
    let result = if election.holds_lease() {
        election.renew()
    } else {
        election.try_acquire()
    };
    match result {
        Ok(outcome) => state.record(outcome, now_millis),
        Err(e) => state.record_error(&e),
    }
}

fn env_secs(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

/// Lease duration and renew interval, with the renew interval clamped so a
/// misconfiguration cannot guarantee lease loss.
///
/// ⚠ Clamped rather than rejected. Refusing to start on a value a previous build
/// accepted turns a misconfiguration into an outage — the same reasoning
/// `note_primary_selected` applies to a `primary` flip on an unwired tier.
pub fn intervals() -> (u64, u64) {
    let lease = env_secs(LEASE_SECS_ENV, DEFAULT_LEASE_DURATION_SECS);
    let want = env_secs(RENEW_SECS_ENV, DEFAULT_RENEW_INTERVAL_SECS);
    (lease, clamp_renew(lease, want))
}

/// The renew interval that will actually be used.
///
/// Three renewals must fit inside one lease, so two may be lost before it
/// lapses. Pure, so the clamp is tested without mutating the process
/// environment — `cargo test` does not serialise tests, and an env-mutating
/// test of this would race every other reader.
pub fn clamp_renew(lease_secs: u64, want_secs: u64) -> u64 {
    want_secs.min((lease_secs / 3).max(1)).max(1)
}

/// The per-process holder identity: `<hostname>#<uuid>`.
///
/// See the module note — the uuid is what makes a restarted writer a *different*
/// holder on a StatefulSet's stable pod name.
fn identity() -> String {
    let host = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "unknown-host".to_string());
    format!("{host}#{}", uuid::Uuid::new_v4())
}

/// Start the election for `shard`, if enabled. Returns whether it started.
///
/// A strict no-op when `NOETL_EHDB_ELECTION` is unset: no thread, no state, no
/// ServiceAccount token read, no request. Idempotent per process.
pub fn spawn(shard: u32) -> bool {
    if !election_enabled() {
        return false;
    }
    if STATE.get().is_some() {
        return false;
    }
    let store = match KubeLeaseStore::from_env_if_enabled() {
        Ok(Some(s)) => s,
        Ok(None) => return false,
        Err(e) => {
            // ⚠ Not fatal. The flag is on and the environment cannot support it
            // — that is a configuration problem, and a writer that refuses to
            // boot over it is a worse outcome than one that boots unelected.
            tracing::error!(
                shard,
                error = %e,
                "EHDB shard election is enabled but its Kubernetes lease store \
                 could not be built — continuing WITHOUT an election. Every epoch \
                 stays 0 and single-writer rests on StatefulSet replicas:1 alone; \
                 fencing must NOT be flipped to enforce in this state."
            );
            return false;
        }
    };

    let (lease_secs, renew_secs) = intervals();
    let id = identity();
    let st = Arc::new(ElectionState::default());
    st.shard.store(u64::from(shard), Ordering::Relaxed);
    st.lease_duration_secs.store(lease_secs, Ordering::Relaxed);
    st.renew_interval_secs.store(renew_secs, Ordering::Relaxed);
    if STATE.set(st.clone()).is_err() {
        return false;
    }

    let election =
        ShardElection::new(store, SystemClock, id.clone(), shard).with_duration_secs(lease_secs);
    tracing::info!(
        shard,
        identity = %id,
        lease_name = %election.lease_name(),
        lease_secs,
        renew_secs,
        "EHDB shard election starting in SHADOW — it mints fencing tokens and \
         nothing gates writes on them. Watch ehdb_election_epoch; fencing stays \
         in its configured mode until C3."
    );

    // ⚠ A dedicated OS thread, not `tokio::spawn` — see the module note on
    // `block_on`. Daemon by construction: the lease carries its own expiry, so
    // there is nothing to clean up on exit and the failover window is bounded by
    // `lease_secs` whether this process exits politely or is SIGKILLed.
    let builder = std::thread::Builder::new().name(format!("ehdb-election-{shard}"));
    let spawned = builder.spawn(move || {
        let clock = SystemClock;
        // Log the first failure, then every 60th. A threshold rather than a
        // modulo: the count can jump by more than one between reads, and
        // `errs % 60 == 0` would then skip the line entirely.
        let mut next_log_at: u64 = 1;
        let mut last_errors: u64 = 0;
        loop {
            tick(&election, &st, clock.now_millis());
            let errs: u64 = st.errors.iter().map(|c| c.load(Ordering::Relaxed)).sum();
            if errs > last_errors {
                // An election that cannot reach the API server would otherwise
                // log every renew interval forever — `logging.md` forbids that
                // volume, and the later lines carry the count the first could
                // not.
                if errs >= next_log_at {
                    tracing::warn!(
                        shard,
                        errors_total = errs,
                        forbidden = st.errors[FORBIDDEN].load(Ordering::Relaxed),
                        unauthorized = st.errors[UNAUTHORIZED].load(Ordering::Relaxed),
                        unreachable = st.errors[UNREACHABLE].load(Ordering::Relaxed),
                        "EHDB shard election could not reach its lease store. \
                         forbidden>0 means the ehdb-shard-election Role is not \
                         bound to this pod's ServiceAccount. The writer keeps \
                         running unelected; epochs stop advancing."
                    );
                    next_log_at = errs.saturating_add(60);
                }
                last_errors = errs;
            }
            std::thread::sleep(Duration::from_secs(renew_secs));
        }
    });
    if let Err(e) = spawned {
        tracing::error!(shard, error = %e, "could not spawn the EHDB election thread");
        return false;
    }
    true
}

fn now_millis() -> u64 {
    SystemClock.now_millis()
}

/// The election block of the writer's `/metrics`.
///
/// Every series renders in **both** states — election running and not — so the
/// scrape's shape does not depend on the flag. A series that appears only when
/// enabled cannot be distinguished from one this build does not have.
pub fn render() -> String {
    render_from(state().map(|s| &**s), now_millis())
}

/// The renderer, with the state and the clock injected.
///
/// `render()` is the thin wrapper over the process-global. Taking the state as
/// an argument is what lets a test render a populated state without calling
/// `STATE.set`, which would leak into every other test in the binary and make
/// the flag-off shape test order-dependent.
pub fn render_from(st: Option<&ElectionState>, now: u64) -> String {
    let (active, epoch, holds, valid, lease, renew, age) = match st {
        None => (0, 0, 0, 0, 0u64, 0u64, -1.0f64),
        Some(s) => {
            let last = s.last_contact_millis.load(Ordering::Relaxed);
            let age = if last == 0 {
                -1.0
            } else {
                (now.saturating_sub(last) as f64) / 1000.0
            };
            (
                1,
                s.epoch.load(Ordering::Relaxed),
                u8::from(s.holds.load(Ordering::Relaxed)),
                u8::from(s.token_valid(now)),
                s.lease_duration_secs.load(Ordering::Relaxed),
                s.renew_interval_secs.load(Ordering::Relaxed),
                age,
            )
        }
    };

    let mut out = String::new();
    out.push_str("# HELP ehdb_election_active Whether a shard-lease election is running and issuing fencing tokens. 0 means single-writer rests on StatefulSet replicas:1 alone, and fencing enforce would be an outage.\n");
    out.push_str("# TYPE ehdb_election_active gauge\n");
    out.push_str(&format!("ehdb_election_active {active}\n"));
    out.push_str("# HELP ehdb_election_epoch The fencing token this writer holds. 0 means no token has been issued.\n");
    out.push_str("# TYPE ehdb_election_epoch gauge\n");
    out.push_str(&format!("ehdb_election_epoch {epoch}\n"));
    out.push_str("# HELP ehdb_election_holds_lease Whether the last successful lease call left this writer as the holder.\n");
    out.push_str("# TYPE ehdb_election_holds_lease gauge\n");
    out.push_str(&format!("ehdb_election_holds_lease {holds}\n"));
    out.push_str("# HELP ehdb_election_token_valid Whether the held token may be acted on: holder AND last contact fresher than the lease duration. This, not holds_lease, is what fencing must consult.\n");
    out.push_str("# TYPE ehdb_election_token_valid gauge\n");
    out.push_str(&format!("ehdb_election_token_valid {valid}\n"));
    out.push_str("# HELP ehdb_election_last_contact_age_seconds Seconds since the last successful lease call. -1 means never — distinct from 0, which would claim contact just now.\n");
    out.push_str("# TYPE ehdb_election_last_contact_age_seconds gauge\n");
    out.push_str(&format!(
        "ehdb_election_last_contact_age_seconds {age:.3}\n"
    ));
    out.push_str("# HELP ehdb_election_lease_duration_seconds Lease duration; the upper bound on the failover window.\n");
    out.push_str("# TYPE ehdb_election_lease_duration_seconds gauge\n");
    out.push_str(&format!("ehdb_election_lease_duration_seconds {lease}\n"));
    out.push_str("# HELP ehdb_election_renew_interval_seconds Renewal interval.\n");
    out.push_str("# TYPE ehdb_election_renew_interval_seconds gauge\n");
    out.push_str(&format!("ehdb_election_renew_interval_seconds {renew}\n"));

    out.push_str("# HELP ehdb_election_outcomes_total Election outcomes by kind.\n");
    out.push_str("# TYPE ehdb_election_outcomes_total counter\n");
    let (a, r, l, o) = match st {
        None => (0, 0, 0, 0),
        Some(s) => (
            s.acquired_total.load(Ordering::Relaxed),
            s.renewed_total.load(Ordering::Relaxed),
            s.lost_total.load(Ordering::Relaxed),
            s.held_by_other_total.load(Ordering::Relaxed),
        ),
    };
    for (kind, v) in [
        ("acquired", a),
        ("renewed", r),
        ("lost", l),
        ("held_by_other", o),
    ] {
        out.push_str(&format!(
            "ehdb_election_outcomes_total{{outcome=\"{kind}\"}} {v}\n"
        ));
    }

    out.push_str("# HELP ehdb_election_errors_total Lease-store failures by reason. forbidden>0 means the ehdb-shard-election Role is not bound to this pod's ServiceAccount.\n");
    out.push_str("# TYPE ehdb_election_errors_total counter\n");
    for (i, reason) in ERROR_REASONS.iter().enumerate() {
        let v = st.map(|s| s.errors[i].load(Ordering::Relaxed)).unwrap_or(0);
        out.push_str(&format!(
            "ehdb_election_errors_total{{reason=\"{reason}\"}} {v}\n"
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use ehdb_reference::election::{InMemoryLeaseStore, LeaseRecord, ManualClock};
    use std::sync::atomic::AtomicBool;

    /// A clock the test can advance while the election holds it.
    struct Shared(Arc<ManualClock>);
    impl Clock for Shared {
        fn now_millis(&self) -> u64 {
            self.0.now_millis()
        }
    }

    const LEASE: u64 = 15;

    fn election(
        store: Arc<InMemoryLeaseStore>,
        clock: Arc<ManualClock>,
        id: &str,
    ) -> ShardElection<Arc<InMemoryLeaseStore>, Shared> {
        ShardElection::new(store, Shared(clock), id, 7).with_duration_secs(LEASE)
    }

    /// Delegates until `fail` is set, then returns `err` from every call.
    struct Flaky {
        inner: InMemoryLeaseStore,
        fail: AtomicBool,
        err: String,
    }
    impl Flaky {
        fn new(err: &str) -> Self {
            Self {
                inner: InMemoryLeaseStore::new(),
                fail: AtomicBool::new(false),
                err: err.to_string(),
            }
        }
        fn boom(&self) -> EhdbError {
            EhdbError::Storage(self.err.clone())
        }
    }
    impl LeaseStore for Flaky {
        fn read(&self, n: &str) -> ehdb_reference::Result<Option<LeaseRecord>> {
            if self.fail.load(Ordering::Relaxed) {
                return Err(self.boom());
            }
            self.inner.read(n)
        }
        fn create(&self, n: &str, r: &LeaseRecord) -> ehdb_reference::Result<bool> {
            if self.fail.load(Ordering::Relaxed) {
                return Err(self.boom());
            }
            self.inner.create(n, r)
        }
        fn compare_and_swap(
            &self,
            n: &str,
            v: u64,
            r: &LeaseRecord,
        ) -> ehdb_reference::Result<bool> {
            if self.fail.load(Ordering::Relaxed) {
                return Err(self.boom());
            }
            self.inner.compare_and_swap(n, v, r)
        }
    }

    #[test]
    fn the_flag_is_off_by_default_so_spawn_does_nothing() {
        // Reads the ambient env rather than setting it: `cargo test` does not
        // serialise, so a test that set NOETL_EHDB_ELECTION would race every
        // other reader in the binary.
        assert!(
            !election_enabled(),
            "NOETL_EHDB_ELECTION is set in this test env"
        );
        assert!(!spawn(0), "spawn must be a no-op with the flag off");
        assert!(
            state().is_none(),
            "no state may be published with the flag off"
        );
    }

    #[test]
    fn classification_tracks_reqwests_own_status_rendering() {
        // ⚠ Built with reqwest's `StatusCode` Display, exactly as the adapter
        // formats it. If reqwest ever renders 403 as something other than
        // "403 Forbidden", this fails instead of silently reclassifying every
        // RBAC failure as `other` — which is the one reason an operator most
        // needs to see named.
        let forbidden = EhdbError::Storage(format!(
            "GET lease ehdb-shard-00000000: {} {{}}",
            reqwest::StatusCode::FORBIDDEN
        ));
        assert_eq!(classify(&forbidden), FORBIDDEN, "{forbidden}");
        let unauth = EhdbError::Storage(format!(
            "GET lease x: {} {{}}",
            reqwest::StatusCode::UNAUTHORIZED
        ));
        assert_eq!(classify(&unauth), UNAUTHORIZED, "{unauth}");
        let down = EhdbError::Storage("k8s request failed: connection refused".into());
        assert_eq!(classify(&down), UNREACHABLE);
        assert_eq!(classify(&EhdbError::NotFound("lease x".into())), OTHER);
    }

    #[test]
    fn the_adapter_still_embeds_the_status_and_transport_literals() {
        // The other half of the classifier's correctness: it can only see what
        // the adapter puts in the message. Counts CODE, not prose.
        let src = include_str!("lease_store.rs");
        let code: String = src
            .lines()
            .map(str::trim_start)
            .filter(|l| !l.starts_with("//") && !l.starts_with('*'))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            code.contains("{status}"),
            "the adapter no longer interpolates the HTTP status into its error \
             message — classify() can no longer tell 403 from anything else"
        );
        assert!(
            code.contains("k8s request failed"),
            "the adapter's transport-failure literal changed — the `unreachable` \
             reason now counts as `other`"
        );
    }

    #[test]
    fn clamp_renew_keeps_three_renewals_inside_one_lease() {
        assert_eq!(clamp_renew(15, 5), 5, "the default pair is already valid");
        // ⚠ The misconfiguration that matters: a renew interval at or beyond the
        // lease guarantees the lease lapses between renewals, so the writer
        // would flap between elected and not forever.
        assert_eq!(clamp_renew(15, 15), 5);
        assert_eq!(clamp_renew(15, 60), 5);
        assert_eq!(clamp_renew(60, 60), 20);
        // Never zero: a 0-second sleep would spin the API server.
        assert_eq!(clamp_renew(1, 1), 1);
        assert_eq!(clamp_renew(2, 0), 1);
    }

    #[test]
    fn tick_acquires_then_renews_and_the_epoch_is_stable() {
        let store = Arc::new(InMemoryLeaseStore::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let el = election(store, clock.clone(), "pod-0#aaaa");
        let st = ElectionState::default();
        st.lease_duration_secs.store(LEASE, Ordering::Relaxed);

        tick(&el, &st, clock.now_millis());
        assert_eq!(
            st.epoch.load(Ordering::Relaxed),
            1,
            "first tick must mint 1"
        );
        assert!(st.holds.load(Ordering::Relaxed));
        assert_eq!(st.acquired_total.load(Ordering::Relaxed), 1);
        assert!(st.token_valid(clock.now_millis()));

        clock.advance_millis(5_000);
        tick(&el, &st, clock.now_millis());
        // ⚠ Renewal must NOT advance the epoch. A token minted per renewal would
        // make the store fence the holder against itself.
        assert_eq!(st.epoch.load(Ordering::Relaxed), 1);
        assert_eq!(st.renewed_total.load(Ordering::Relaxed), 1);
        assert_eq!(st.acquired_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn tick_is_fail_soft_keeps_the_last_epoch_and_counts_the_reason() {
        let flaky = Arc::new(Flaky::new(&format!(
            "GET lease ehdb-shard-00000007: {} forbidden",
            reqwest::StatusCode::FORBIDDEN
        )));
        let clock = Arc::new(ManualClock::new(1_000));
        let el = ShardElection::new(flaky.clone(), Shared(clock.clone()), "pod-0#aaaa", 7)
            .with_duration_secs(LEASE);
        let st = ElectionState::default();
        st.lease_duration_secs.store(LEASE, Ordering::Relaxed);

        tick(&el, &st, clock.now_millis());
        assert_eq!(st.epoch.load(Ordering::Relaxed), 1);

        // RBAC is revoked (or was never granted).
        flaky.fail.store(true, Ordering::Relaxed);
        for i in 1..=5 {
            clock.advance_millis(1_000);
            tick(&el, &st, clock.now_millis());
            assert_eq!(
                st.errors[FORBIDDEN].load(Ordering::Relaxed),
                i,
                "every failure must be counted under its own reason"
            );
        }
        // Fail-soft: no panic, the loop would continue, and the last known epoch
        // is retained rather than being reported as a handover that may not have
        // happened.
        assert_eq!(st.epoch.load(Ordering::Relaxed), 1);
        assert!(st.holds.load(Ordering::Relaxed));
        assert_eq!(st.lost_total.load(Ordering::Relaxed), 0);
        for other in [UNAUTHORIZED, UNREACHABLE, OTHER] {
            assert_eq!(st.errors[other].load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn token_valid_expires_on_the_clock_without_reaching_the_api_server() {
        let st = ElectionState::default();
        st.lease_duration_secs.store(LEASE, Ordering::Relaxed);
        st.holds.store(true, Ordering::Relaxed);
        st.epoch.store(4, Ordering::Relaxed);

        // Never contacted: holding is not enough.
        assert!(
            !st.token_valid(0),
            "no contact must not read as a valid token"
        );

        st.note_contact(100_000);
        assert!(st.token_valid(100_000));
        assert!(st.token_valid(100_000 + LEASE * 1000 - 1));
        // ⚠ The bound that matters. Once contact is older than the lease could
        // survive, this writer stops claiming a usable token — decided locally,
        // with no API server call, which is the only way the claim can be made
        // while partitioned.
        assert!(!st.token_valid(100_000 + LEASE * 1000));
        assert!(!st.token_valid(100_000 + LEASE * 1000 + 60_000));
        // The epoch is still readable — "was elected, is not now" is the truth.
        assert_eq!(st.epoch.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn a_restarted_writer_inherits_the_epoch_unless_identity_is_per_process() {
        // ⚠⚠ The reason `identity()` carries a uuid. A StatefulSet pod name is
        // stable, so this is what a restart looks like to the lease.
        let store = Arc::new(InMemoryLeaseStore::new());
        let clock = Arc::new(ManualClock::new(1_000));

        let first = election(store.clone(), clock.clone(), "writer-0#aaaa");
        let st = ElectionState::default();
        st.lease_duration_secs.store(LEASE, Ordering::Relaxed);
        tick(&first, &st, clock.now_millis());
        assert_eq!(st.epoch.load(Ordering::Relaxed), 1);

        clock.advance_millis(1_000); // lease still live

        // A fresh process reusing ONLY the pod name renews its predecessor's
        // lease and silently keeps epoch 1 — indistinguishable from a zombie.
        let same_name = election(store.clone(), clock.clone(), "writer-0#aaaa");
        let st_same = ElectionState::default();
        st_same.lease_duration_secs.store(LEASE, Ordering::Relaxed);
        tick(&same_name, &st_same, clock.now_millis());
        assert_eq!(
            st_same.epoch.load(Ordering::Relaxed),
            1,
            "identity without a per-process suffix INHERITS the epoch — this is \
             the defect the uuid prevents"
        );

        // With a per-process suffix it is a different holder: it must wait the
        // lease out rather than inherit.
        let restarted = election(store.clone(), clock.clone(), "writer-0#bbbb");
        let st_new = ElectionState::default();
        st_new.lease_duration_secs.store(LEASE, Ordering::Relaxed);
        tick(&restarted, &st_new, clock.now_millis());
        assert_eq!(
            st_new.epoch.load(Ordering::Relaxed),
            0,
            "must not acquire early"
        );
        assert!(!st_new.holds.load(Ordering::Relaxed));
        assert_eq!(st_new.held_by_other_total.load(Ordering::Relaxed), 1);

        // ⚠ The exact edge of the failover window. `LeaseRecord::is_expired` is
        // a strict `>`, so at `renewed_at + duration` the lease is still live and
        // the restarted writer must NOT take it. Asserted because this boundary
        // is the number the C3 failover bound is computed from — my first
        // version of this test advanced to exactly here and expected an
        // acquisition.
        clock.advance_millis(LEASE * 1000);
        tick(&restarted, &st_new, clock.now_millis());
        assert_eq!(
            st_new.epoch.load(Ordering::Relaxed),
            0,
            "the lease is still live at exactly renewed_at + duration"
        );

        // One millisecond later it lapses, and the acquisition mints a NEW
        // token, so a fence can tell the two processes apart.
        clock.advance_millis(1);
        tick(&restarted, &st_new, clock.now_millis());
        assert_eq!(st_new.epoch.load(Ordering::Relaxed), 2);
        assert!(st_new.holds.load(Ordering::Relaxed));
    }

    #[test]
    fn render_from_reports_live_state() {
        let st = ElectionState::default();
        st.shard.store(7, Ordering::Relaxed);
        st.epoch.store(3, Ordering::Relaxed);
        st.holds.store(true, Ordering::Relaxed);
        st.lease_duration_secs.store(LEASE, Ordering::Relaxed);
        st.renew_interval_secs.store(5, Ordering::Relaxed);
        st.note_contact(100_000);
        st.acquired_total.store(1, Ordering::Relaxed);
        st.renewed_total.store(12, Ordering::Relaxed);
        st.errors[FORBIDDEN].store(2, Ordering::Relaxed);

        let text = render_from(Some(&st), 102_500);
        for want in [
            "ehdb_election_active 1\n",
            "ehdb_election_epoch 3\n",
            "ehdb_election_holds_lease 1\n",
            "ehdb_election_token_valid 1\n",
            "ehdb_election_last_contact_age_seconds 2.500\n",
            "ehdb_election_lease_duration_seconds 15\n",
            "ehdb_election_renew_interval_seconds 5\n",
            "ehdb_election_outcomes_total{outcome=\"renewed\"} 12\n",
            "ehdb_election_errors_total{reason=\"forbidden\"} 2\n",
            "ehdb_election_errors_total{reason=\"other\"} 0\n",
        ] {
            assert!(text.contains(want), "missing {want:?} in:\n{text}");
        }
        // Stale contact flips token_valid while holds_lease stays 1 — the two
        // series exist precisely to be read apart.
        let stale = render_from(Some(&st), 100_000 + LEASE * 1000 + 1);
        assert!(stale.contains("ehdb_election_holds_lease 1\n"), "{stale}");
        assert!(stale.contains("ehdb_election_token_valid 0\n"), "{stale}");
    }
}
