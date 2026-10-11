//! **The election is wired, and gated by a flag that defaults off.**
//!
//! ⚠⚠ This file previously asserted the opposite. As shipped in noetl/worker#352
//! the adapter was *deliberately* inert and said so, and its third test pinned
//! that nothing on the startup path constructed it — with a comment saying that
//! test was the one to delete, on purpose, once the election was driven.
//!
//! It is now driven: `ehdb::election` spawns a `ShardElection` over the
//! Kubernetes lease store from the writer's boot path. So the inertness claim
//! would be a **passing test that lies** — the sharpest version of this
//! codebase's recurring defect, since a green guard asserting a false claim is
//! worse than no guard. The claim it is replaced with is narrower and still
//! load-bearing: the mechanism exists and is reachable, but it does nothing
//! until an operator sets the flag.
//!
//! The three halves pinned here:
//!
//! 1. the flag defaults **off**, so nothing happens by accident;
//! 2. with the flag off nothing is constructed and the environment is not even
//!    probed — so a dev box, a CI runner and a pod without RBAC behave alike;
//! 3. with the flag **on** in an environment that cannot support it, the driver
//!    degrades and returns rather than failing the boot. That is the property
//!    the whole rollout rests on: the prerequisite Role binding does not exist
//!    yet, so the first thing a premature flip produces is `403 Forbidden` on
//!    every tick, and a writer that exits over it would crash-loop the one
//!    StatefulSet whose restart behaviour is the riskiest part of this change.
//!
//! C2 going live still needs three owner actions on prod (a dedicated
//! ServiceAccount for the cmdbus-writer, `serviceAccountName` set on the
//! StatefulSet, and the `ehdb-shard-election` Role bound to it). Nothing here
//! pre-empts them; with the flag off this code is reached and declines.

use noetl_worker::ehdb::lease_store::election_enabled;

static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// ⚠ `cargo test` does not serialise tests, so two tests mutating the same env
/// var race and fail intermittently rather than loudly.
fn serialised() -> std::sync::MutexGuard<'static, ()> {
    match ENV_LOCK.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

fn with_flag<T>(v: Option<&str>, f: impl FnOnce() -> T) -> T {
    let _g = serialised();
    let key = "NOETL_EHDB_ELECTION";
    let prev = std::env::var(key).ok();
    unsafe {
        match v {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
    }
    let out = f();
    unsafe {
        match prev {
            Some(p) => std::env::set_var(key, p),
            None => std::env::remove_var(key),
        }
    }
    out
}

#[test]
fn the_election_flag_defaults_off() {
    assert!(
        !with_flag(None, election_enabled),
        "absent must mean OFF — the election must not start because a deployment forgot to \
         set something"
    );
    assert!(!with_flag(Some("false"), election_enabled));
    assert!(!with_flag(Some("0"), election_enabled));
    assert!(with_flag(Some("true"), election_enabled));
    assert!(with_flag(Some("1"), election_enabled));
}

#[test]
fn from_env_if_enabled_builds_nothing_while_the_flag_is_off() {
    // ⚠ Must not even attempt to read the ServiceAccount token: in-cluster
    // paths do not exist on a dev box, and an eager read would make this fail
    // for the wrong reason — or, worse, succeed in-cluster and start an
    // election nobody asked for.
    let got = with_flag(None, || {
        noetl_worker::ehdb::lease_store::KubeLeaseStore::from_env_if_enabled()
    });
    match got {
        Ok(None) => {}
        Ok(Some(_)) => panic!("built a store with the flag off"),
        Err(e) => panic!("must return Ok(None), not probe the environment and fail: {e}"),
    }
}

/// ⚠⚠ The gate, both halves: the call site EXISTS, and it declines while the
/// flag is off. Pinning only one half would miss the two opposite failures —
/// a driver nothing spawns, and a driver that spawns unasked.
#[test]
fn the_driver_is_reachable_from_the_boot_path_and_declines_while_the_flag_is_off() {
    // Half one: reachable. Counts CODE, not prose, and refuses to run against a
    // suspiciously small slice — three checks in an earlier session "passed"
    // against a truncated region and printed green computed from zero bytes.
    let worker_rs = include_str!("../src/worker.rs");
    assert!(
        worker_rs.len() > 1000,
        "worker.rs read as {} bytes — the check below would be vacuous",
        worker_rs.len()
    );
    let code: String = worker_rs
        .lines()
        .map(str::trim_start)
        .filter(|l| !l.starts_with("//") && !l.starts_with('*'))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        code.contains("election::spawn"),
        "⚠ nothing on the writer's boot path spawns the election. The gauges \
         would keep reporting while no token is ever minted — and C3 must NEVER \
         be armed in that state, because a fence enforces against an epoch."
    );

    // Half two: declines. With the flag off this is reached and returns false,
    // publishing no state at all.
    // ⚠ No `serialised()` here: `with_flag` takes ENV_LOCK itself, and
    // `std::sync::Mutex` is not reentrant — taking it twice on one thread
    // deadlocks. Both of these tests hung for exactly that reason, and across
    // three background runs the hang read as a slow compile rather than as a
    // failure. A test that never finishes is not a test that passes.
    with_flag(None, || {
        assert!(
            !noetl_worker::ehdb::election::spawn(0),
            "spawn must decline while the flag is off"
        );
        assert!(
            noetl_worker::ehdb::election::state().is_none(),
            "no state may be published while the flag is off"
        );
    });
}

/// ⚠ The fail-soft boot property, exercised rather than asserted in a comment.
///
/// The flag is on and this process is not in a cluster: there is no
/// ServiceAccount token to read. The driver must log and return — not panic, not
/// propagate, not abort the worker's construction.
#[test]
fn the_flag_on_without_a_cluster_degrades_instead_of_failing_the_boot() {
    // ⚠ Refuse to run this one IN a cluster. With the flag on and a real
    // ServiceAccount token present, the driver would succeed and start a thread
    // that campaigns for a live lease — a test that quietly elects itself. The
    // skip is explicit rather than silent so the reason is readable.
    if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() || std::env::var("NOETL_K8S_API").is_ok() {
        eprintln!(
            "skipped: a Kubernetes API endpoint is reachable, so this test \
                   would start a real election rather than exercise the decline path"
        );
        return;
    }
    // ⚠ No `serialised()` here: `with_flag` takes ENV_LOCK itself, and
    // `std::sync::Mutex` is not reentrant — taking it twice on one thread
    // deadlocks. Both of these tests hung for exactly that reason, and across
    // three background runs the hang read as a slow compile rather than as a
    // failure. A test that never finishes is not a test that passes.
    with_flag(Some("true"), || {
        assert!(
            !noetl_worker::ehdb::election::spawn(0),
            "a flag-on election with no kube environment must decline, not start"
        );
        assert!(
            noetl_worker::ehdb::election::state().is_none(),
            "a failed election must publish no state — ehdb_election_active would \
             otherwise read 1 while no token is ever minted"
        );
    });
}
