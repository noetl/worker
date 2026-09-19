//! Driving [`ShardElection`] — the M5 token issuer, on a ladder.
//!
//! # The ordering hazard this ladder exists for
//!
//! **VERIFIED** by `ehdb-reference`'s own
//! `enforcing_before_any_election_fences_every_writer`: with no election every
//! writer's epoch is `0`, and **all-zero is self-consistent, so writes
//! succeed**. The danger is not "enforce without an election" — it is **MIXED**
//! epochs. The moment ONE node mints epoch 1 and advances the shard marker,
//! every writer still on epoch 0 is refused, including one that is legitimately
//! the single writer for its own shard.
//!
//! So the transition that must never be partial is `observe -> authoritative`,
//! and that is precisely what a rolling update makes partial. Hence three rungs
//! rather than a boolean:
//!
//! | `NOETL_EHDB_ELECTION` | thread | epoch published to metrics | epoch applied to the write path |
//! | :-- | :-- | :-- | :-- |
//! | `off` (default) | no | — | no (stays 0) |
//! | `observe` | yes | yes | **no** |
//! | `authoritative` | yes | yes | yes |
//!
//! `observe` is the load-bearing rung: it proves the lease, the CAS and the
//! failover work **in the real cluster** while the write path still sees the
//! all-zero state it sees today. Nothing about the bytes written changes until
//! `authoritative`.
//!
//! ⚠⚠ Flipping to `authoritative` on a multi-writer pool must be simultaneous,
//! not rolling. On the `cmdbus-writer` StatefulSet (`replicas: 1`) there is one
//! writer per shard and the question does not arise; on a pool it does.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use super::EnvMap;

/// `NOETL_EHDB_ELECTION` — `off` (default) | `observe` | `authoritative`.
pub const ELECTION_ENV: &str = "NOETL_EHDB_ELECTION";
/// `NOETL_EHDB_ELECTION_IDENTITY` — defaults to `HOSTNAME`, i.e. the pod name.
pub const ELECTION_IDENTITY_ENV: &str = "NOETL_EHDB_ELECTION_IDENTITY";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionSetting {
    Off,
    Observe,
    Authoritative,
}

impl ElectionSetting {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Observe => "observe",
            Self::Authoritative => "authoritative",
        }
    }

    /// Anything unrecognised — including a typo — is `Off`, which is today's
    /// behaviour. A typo must never silently start applying fencing tokens.
    pub fn parse(raw: Option<&str>) -> Self {
        match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
            Some("observe") => Self::Observe,
            Some("authoritative") => Self::Authoritative,
            _ => Self::Off,
        }
    }

    pub fn from_env(env: &EnvMap) -> Self {
        Self::parse(env.get(ELECTION_ENV).map(|s| s.as_str()))
    }

    /// Whether the epoch this process holds may reach the WRITE path.
    pub fn applies_to_writes(self) -> bool {
        matches!(self, Self::Authoritative)
    }

    /// Whether an election loop should run at all.
    pub fn runs_loop(self) -> bool {
        !matches!(self, Self::Off)
    }
}

/// What the election currently believes, readable from any thread.
#[derive(Debug, Default)]
pub struct ElectionState {
    /// An election loop is running AND last contacted the store successfully.
    active: AtomicBool,
    /// The epoch this process holds. `0` = no token.
    epoch: AtomicU64,
    /// Successful acquire/renew round trips — the POSITIVE CONTROL for
    /// `active`. Without it, "active and epoch 0" and "loop wedged" look alike.
    rounds: AtomicU64,
    /// Rounds that failed to reach the store.
    errors: AtomicU64,
}

impl ElectionState {
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }
    pub fn rounds(&self) -> u64 {
        self.rounds.load(Ordering::Relaxed)
    }
    pub fn errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }
    fn set_held(&self, epoch: u64) {
        self.epoch.store(epoch, Ordering::Relaxed);
        self.active.store(true, Ordering::Relaxed);
        self.rounds.fetch_add(1, Ordering::Relaxed);
    }
    fn set_not_held(&self) {
        // ⚠ Dropping the token must drop the EPOCH too. A process that keeps
        // publishing an epoch it no longer holds is exactly the split-brain
        // writer fencing exists to refuse.
        self.epoch.store(0, Ordering::Relaxed);
        self.active.store(true, Ordering::Relaxed);
        self.rounds.fetch_add(1, Ordering::Relaxed);
    }
    fn set_unreachable(&self) {
        self.epoch.store(0, Ordering::Relaxed);
        self.active.store(false, Ordering::Relaxed);
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
}

/// The process-wide election state. One per process because one process is one
/// lease holder.
pub static ELECTION: std::sync::LazyLock<Arc<ElectionState>> =
    std::sync::LazyLock::new(|| Arc::new(ElectionState::default()));

/// The epoch the WRITE path should present.
///
/// `0` unless the setting is `authoritative` **and** a token is held. Under
/// `observe` the election runs and publishes to metrics while the write path
/// keeps seeing the all-zero state — which is what makes `observe` safe to turn
/// on everywhere before anything is enforced.
pub fn epoch_for_write(setting: ElectionSetting) -> u64 {
    epoch_for_write_from(setting, ELECTION.epoch())
}

/// [`epoch_for_write`] as a **pure function of the held epoch**.
///
/// ⚠ Split from the process-global read for the reason this codebase keeps
/// rediscovering: `cargo test` does **not** serialise tests, so two tests
/// driving `ELECTION` race each other. (They did — the first version of these
/// tests failed intermittently with `left: 7, right: 4`.) The decision is the
/// part worth testing; the global is just where the number lives.
pub fn epoch_for_write_from(setting: ElectionSetting, held: u64) -> u64 {
    if setting.applies_to_writes() {
        held
    } else {
        0
    }
}

/// Start the election loop on a **dedicated OS thread**.
///
/// ⚠ Dedicated on purpose: `KubeLeaseStore` uses `reqwest::blocking`, which
/// panics if called from inside a Tokio runtime thread. This is a 5-second
/// renew timer, not a hot path, so an OS thread is the cheap correct answer.
///
/// No-op under `off`, and a no-op when not running in a cluster — the latter
/// reads as "no election available", never as "elected".
pub fn spawn(env: &EnvMap) {
    let setting = ElectionSetting::from_env(env);
    if !setting.runs_loop() {
        return;
    }
    let Some(cfg) = super::kube_lease::KubeConfig::in_cluster() else {
        tracing::warn!(
            setting = setting.as_str(),
            "{ELECTION_ENV} is set but this process is not in a Kubernetes cluster \
             (no ServiceAccount) — no election will run and the epoch stays 0",
        );
        return;
    };
    let store = match super::kube_lease::KubeLeaseStore::new(cfg) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(%e, "could not build the Kubernetes lease store; no election will run");
            return;
        }
    };
    let identity = env
        .get(ELECTION_IDENTITY_ENV)
        .cloned()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "unknown".to_string());
    let shard = super::eventlog_backend::writer_shard_index(env);

    std::thread::Builder::new()
        .name("ehdb-election".into())
        .spawn(move || run_loop(store, identity, shard, setting))
        .map(|_| ())
        .unwrap_or_else(|e| tracing::error!(%e, "could not spawn the election thread"));
}

fn run_loop(
    store: super::kube_lease::KubeLeaseStore,
    identity: String,
    shard: u32,
    setting: ElectionSetting,
) {
    use ehdb_reference::election::{
        ElectionOutcome, ShardElection, SystemClock, DEFAULT_RENEW_INTERVAL_SECS,
    };
    let election = ShardElection::new(store, SystemClock, identity.clone(), shard);
    tracing::info!(
        shard,
        identity = %identity,
        setting = setting.as_str(),
        lease = %election.lease_name(),
        "EHDB shard election starting"
    );
    let mut last: Option<u64> = None;
    loop {
        match election.try_acquire() {
            // `Renewed` is the steady state — `try_acquire` delegates to
            // `renew` when this process already holds the lease — and it must
            // keep the SAME epoch. Minting a new token per renewal would make
            // the store fence the holder against itself.
            Ok(ElectionOutcome::Renewed { epoch }) | Ok(ElectionOutcome::Acquired { epoch }) => {
                ELECTION.set_held(epoch);
                if last != Some(epoch) {
                    // Transition-only: a per-tick line at 5s would be noise, and
                    // a line nobody reads is the same as no line.
                    tracing::info!(
                        shard, epoch, identity = %identity, setting = setting.as_str(),
                        applies_to_writes = setting.applies_to_writes(),
                        "EHDB shard lease ACQUIRED"
                    );
                    last = Some(epoch);
                }
            }
            Ok(ElectionOutcome::Lost) => {
                ELECTION.set_not_held();
                if last.is_some() {
                    tracing::warn!(
                        shard, identity = %identity,
                        "EHDB shard lease LOST on renew — the token is dropped"
                    );
                    last = None;
                }
            }
            Ok(ElectionOutcome::HeldByOther { holder_epoch }) => {
                ELECTION.set_not_held();
                if last.is_some() {
                    tracing::warn!(
                        shard, holder_epoch, identity = %identity,
                        "EHDB shard lease LOST — this process no longer holds a token"
                    );
                    last = None;
                }
            }
            Err(e) => {
                ELECTION.set_unreachable();
                tracing::warn!(shard, %e, "EHDB shard election could not reach the lease store");
                last = None;
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(DEFAULT_RENEW_INTERVAL_SECS));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> EnvMap {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn unset_and_typos_are_off() {
        // A typo must never silently start applying fencing tokens.
        assert_eq!(ElectionSetting::from_env(&env(&[])), ElectionSetting::Off);
        for bad in ["", "obsrve", "auth", "true", "1", "on"] {
            assert_eq!(
                ElectionSetting::parse(Some(bad)),
                ElectionSetting::Off,
                "{bad:?} must be Off"
            );
        }
    }

    #[test]
    fn the_three_rungs_differ_in_exactly_the_right_way() {
        // `observe` is the whole point of the ladder: the loop runs, so the
        // lease and CAS are proven in the real cluster, while the WRITE path
        // still sees the all-zero state it sees today.
        assert!(!ElectionSetting::Off.runs_loop());
        assert!(!ElectionSetting::Off.applies_to_writes());

        assert!(ElectionSetting::Observe.runs_loop());
        assert!(
            !ElectionSetting::Observe.applies_to_writes(),
            "observe must NOT reach the write path, or it is just authoritative \
             under a safer-sounding name"
        );

        assert!(ElectionSetting::Authoritative.runs_loop());
        assert!(ElectionSetting::Authoritative.applies_to_writes());
    }

    #[test]
    fn only_authoritative_lets_an_epoch_reach_the_write_path() {
        // Pure — no process-global, so this cannot race the test below.
        assert_eq!(epoch_for_write_from(ElectionSetting::Off, 7), 0);
        assert_eq!(epoch_for_write_from(ElectionSetting::Observe, 7), 0);
        assert_eq!(
            epoch_for_write_from(ElectionSetting::Authoritative, 7),
            7,
            "authoritative must present the held epoch, or the ladder has no top rung"
        );
        // POSITIVE CONTROL: a non-zero epoch really can come through, so the
        // two zeros above are a decision rather than an empty variable.
        assert_ne!(epoch_for_write_from(ElectionSetting::Authoritative, 7), 0);
    }

    #[test]
    fn losing_the_lease_drops_the_epoch() {
        // A process that keeps publishing an epoch it no longer holds is the
        // split-brain writer fencing exists to refuse. Driven on a LOCAL state
        // so it cannot race any other test.
        let st = ElectionState::default();
        st.set_held(4);
        assert_eq!(epoch_for_write_from(ElectionSetting::Authoritative, st.epoch()), 4);
        assert_eq!(st.rounds(), 1);

        st.set_not_held();
        assert_eq!(epoch_for_write_from(ElectionSetting::Authoritative, st.epoch()), 0);
        assert!(st.is_active(), "still talking to the store");

        st.set_unreachable();
        assert_eq!(st.epoch(), 0);
        assert!(
            !st.is_active(),
            "an unreachable store is NOT an active election — reporting it as one \
             would make a wedged loop look like a healthy un-elected process"
        );
        assert_eq!(st.errors(), 1);
        // The round counter is the positive control for `active`: it must have
        // MOVED, or 'active' could be true on a loop that never ran.
        assert_eq!(st.rounds(), 2);
    }

    /// Strip `//` comments so a guard counts CODE, not prose about code.
    fn code_only(src: &str) -> String {
        src.lines()
            .map(|l| match l.find("//") {
                Some(i) => &l[..i],
                None => l,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// ⚠⚠ THE REACHABILITY GUARD — the defect class M5 exists to fix.
    ///
    /// `ShardElection` was implemented, tested and merged with **no call sites
    /// anywhere**, so every writer's epoch stayed 0 and single-writer rested
    /// entirely on `StatefulSet replicas: 1`. Shipping this module without a
    /// caller would reproduce that exactly — a module that compiles is not a
    /// module that runs.
    ///
    /// Counted in CODE with comments stripped, because the comment above this
    /// test names the function and would otherwise satisfy it — and, worse, the
    /// count could be satisfied by DELETING a comment while removing the call.
    #[test]
    fn the_election_is_actually_spawned_by_the_worker() {
        let src = code_only(include_str!("../worker.rs"));
        assert!(
            src.contains("election::spawn("),
            "nothing calls `election::spawn` — the election would be another \
             implemented-and-unreachable feature, which is the exact state \
             (`ehdb_election_active 0`, epoch 0 everywhere) that M5 exists to end"
        );
    }

    /// The epoch must reach the fenced backend, and only through the ladder.
    #[test]
    fn the_write_path_takes_its_epoch_from_the_ladder() {
        let src = code_only(include_str!("eventlog_backend.rs"));
        assert!(
            src.contains("set_epoch("),
            "the fenced backend never has its epoch set, so the election's token \
             cannot reach the write path no matter what the flag says"
        );
        assert!(
            src.contains("election::epoch_for_write("),
            "the epoch must come from `epoch_for_write`, which is what keeps \
             `observe` off the write path; reading `ELECTION.epoch()` directly \
             would make observe and authoritative the same thing"
        );
    }
}
