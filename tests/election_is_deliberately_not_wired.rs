//! **The election adapter is built, inert, and LABELLED as inert.**
//!
//! An adapter nothing calls is a fence-shaped object, and this codebase has
//! shipped several: a recorder with no caller, a guard with no runner, a
//! `#[test]`-less test fn. The distinction that makes this one acceptable is
//! that it is *deliberately* unreachable and says so — ehdb's own framing:
//! inert **and labelled** is a shipped shadow slice, not drift.
//!
//! So these tests pin the two halves of that claim:
//!
//! 1. the flag defaults **off**, so nothing happens by accident;
//! 2. nothing in `main.rs` constructs it — C2 going live needs three owner
//!    actions on prod (a dedicated ServiceAccount for the cmdbus-writer,
//!    `serviceAccountName` set on the StatefulSet, and the `ehdb-shard-election`
//!    Role bound to it), and arming it here would quietly pre-empt that
//!    decision.
//!
//! ⚠ When the owner does take those actions, test 2 is the one to delete — and
//! deleting it should be a conscious act, which is why it carries this comment
//! rather than being a silent absence.

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

/// ⚠⚠ The deliberate-inertness guard. See the module docs: C2 going live is
/// three owner actions on prod, and arming it from here would pre-empt them.
#[test]
fn nothing_constructs_the_election_on_the_startup_path() {
    let main_rs = include_str!("../src/main.rs");
    assert!(
        main_rs.len() > 1000,
        "main.rs read as {} bytes — the check below would be vacuous",
        main_rs.len()
    );
    for needle in ["KubeLeaseStore", "from_env_if_enabled", "ShardElection"] {
        assert!(
            !main_rs.contains(needle),
            "⚠ main.rs references `{needle}`. The election adapter is deliberately not wired: \
             going live needs a dedicated ServiceAccount for the cmdbus-writer, \
             `serviceAccountName` set on the StatefulSet, and the ehdb-shard-election Role \
             bound to it — all owner actions. If those have now happened, delete this test \
             on purpose rather than letting it be the thing that failed."
        );
    }
}
