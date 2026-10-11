//! **The spec's first promotion gate, satisfied: the LeaseStore conformance
//! suite run against a real API server.**
//!
//! `ehdb/docs/spec/lease-election-k8s-binding.md`:
//!
//! > - [ ] Adapter conformance: the same suite `InMemoryLeaseStore` passes, run
//! >       against a real API server in kind.
//! > - [ ] A forced holder change (delete the pod, not the lease) advances
//! >       `leaseTransitions` by exactly 1.
//!
//! ⭐ **"The same suite"** is literal here: `ehdb_reference::election::conformance::run`
//! is the identical function the in-memory store is checked with. Writing fresh
//! assertions for the adapter would prove it satisfies *those*, not the contract
//! the election depends on.
//!
//! ⚠ `#[ignore]`d and env-driven: it needs a real API server and a token. A test
//! that silently passed without one would be worse than no test — it would
//! report the gate satisfied having checked nothing. The env is asserted
//! present rather than defaulted.
//!
//! ```text
//! kubectl --context kind-noetl create namespace ehdb-election-test
//! # + ServiceAccount/Role/RoleBinding (leases: get,list,watch,create,update — NO delete)
//! NOETL_K8S_API=$(kubectl --context kind-noetl config view --minify \
//!      -o jsonpath='{.clusters[0].cluster.server}') \
//! NOETL_K8S_TOKEN_FILE=/tmp/kindauth/token \
//! NOETL_K8S_CA_FILE=/tmp/kindauth/ca.crt \
//! NOETL_K8S_NAMESPACE=ehdb-election-test \
//!   cargo test --test kube_lease_store_conformance -- --ignored --nocapture
//! ```

use ehdb_reference::election::conformance;
use ehdb_reference::election::{LeaseStore, ManualClock, ShardElection};
use noetl_worker::ehdb::lease_store::{KubeLeaseConfig, KubeLeaseStore};

fn store() -> KubeLeaseStore {
    // ⚠ Fail loudly when the env is absent. The whole point of the gate is that
    // it ran against a real server.
    for k in [
        "NOETL_K8S_API",
        "NOETL_K8S_TOKEN_FILE",
        "NOETL_K8S_NAMESPACE",
    ] {
        assert!(
            std::env::var(k).is_ok(),
            "{k} must be set — this test must not pass without a real API server"
        );
    }
    let cfg = KubeLeaseConfig::from_env().expect("k8s config from env");
    eprintln!(
        "GATE  api={} ns={} ca={}",
        cfg.api_base,
        cfg.namespace,
        cfg.ca_pem.as_ref().map(|p| p.len()).unwrap_or(0)
    );
    KubeLeaseStore::new(cfg).expect("adapter builds")
}

fn prefix(tag: &str) -> String {
    // k8s names: lowercase alphanumeric and '-'.
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("conf-{tag}-{n}")
}

/// ⭐⭐ **Gate 1.** The identical suite, against the kind API server.
#[test]
#[ignore = "needs a real API server: see the module docs"]
fn the_same_conformance_suite_passes_against_a_real_api_server() {
    let s = store();
    let p = prefix("kube");
    eprintln!("GATE  running conformance::run with prefix {p}");
    let violations = conformance::run(&s, &p);
    for v in &violations {
        eprintln!("GATE  VIOLATION {} — {}", v.clause, v.detail);
    }
    assert!(
        violations.is_empty(),
        "{} clause(s) failed against the real API server: {violations:#?}",
        violations.len()
    );
    eprintln!("GATE  conformance: 0 violations");
}

/// ⚠ The suite must be meaningful *here*, not just pass. Against the real
/// server, re-running with the same prefix must still be caught — otherwise the
/// pass above could be an artifact of the clauses not really exercising the
/// server's conditional writes.
#[test]
#[ignore = "needs a real API server"]
fn reusing_a_prefix_is_caught_against_the_real_server_too() {
    let s = store();
    let p = prefix("rerun");
    assert!(conformance::run(&s, &p).is_empty(), "first run");
    let second = conformance::run(&s, &p);
    assert!(
        !second.is_empty(),
        "a second run on the same prefix must be caught — if it passes, the create-on-absent \
         clause is not actually reaching the API server"
    );
}

/// ⭐⭐ **Gate 2.** A forced holder change advances `leaseTransitions` by
/// **exactly 1** — measured through the real API server.
///
/// The spec frames this as "delete the pod, not the lease". There is no pod
/// here, so the equivalent is driven directly: holder A acquires, its lease is
/// allowed to lapse on a `ManualClock`, holder B acquires. What is being tested
/// is not the state machine (ehdb covers that against the in-memory store) but
/// that **the adapter carries `spec.leaseTransitions` faithfully across a real
/// holder change** — an adapter that dropped or mis-set it would let two nodes
/// mint the same epoch.
///
/// ⚠ Asserted as a **delta**, not an absolute: the lease name comes from
/// `shard_lease_name(shard)` and so persists across runs. An absolute assertion
/// would pass once and fail forever after.
#[test]
#[ignore = "needs a real API server"]
fn a_forced_holder_change_advances_transitions_by_exactly_one() {
    use std::sync::Arc;
    let s = Arc::new(store());
    // ⚠ A UNIQUE shard per run, because `shard_lease_name(shard)` is derived
    // from the shard alone and the Lease persists on a real API server. A fixed
    // shard made the first run pass (lease absent ⇒ A acquires, B takes over,
    // delta 1) and the second run meaningless: A saw a live lease held by the
    // previous run's holder-b and reported `HeldByOther`, B reported `Renewed`,
    // delta 0. Exactly the staleness the conformance suite's `prefix` exists to
    // avoid — which I built into the suite and then failed to apply here.
    let shard = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        % 100_000) as u32
        + 900_000;
    let name = ehdb_reference::election::shard_lease_name(shard);

    let before = s.read(&name).expect("read lease").map(|r| r.transitions);
    eprintln!("GATE  shard={shard} lease={name} transitions before: {before:?}");
    assert!(
        before.is_none(),
        "the unique shard must give a fresh lease; got transitions={before:?} — if this \
         fires, the shard is colliding and the delta below would be measured against \
         another run's state"
    );

    let clock_a = ManualClock::new(1_000_000);
    let a = ShardElection::new(Arc::clone(&s), clock_a, "holder-a", shard).with_duration_secs(5);
    let out_a = a.try_acquire().expect("A acquires");
    eprintln!("GATE  A -> {out_a:?} epoch={:?}", a.epoch());

    let after_a = s
        .read(&name)
        .expect("read")
        .map(|r| r.transitions)
        .expect("lease exists after A");

    // Let A's lease lapse, then B takes it.
    let clock_b = ManualClock::new(1_000_000 + 60_000);
    let b = ShardElection::new(Arc::clone(&s), clock_b, "holder-b", shard).with_duration_secs(5);
    let out_b = b.try_acquire().expect("B acquires the lapsed lease");
    eprintln!("GATE  B -> {out_b:?} epoch={:?}", b.epoch());

    let after_b = s
        .read(&name)
        .expect("read")
        .map(|r| r.transitions)
        .expect("lease exists after B");

    eprintln!(
        "GATE  transitions: after_A={after_a} after_B={after_b} delta={}",
        after_b - after_a
    );
    assert_eq!(
        after_b - after_a,
        1,
        "a holder change must advance leaseTransitions by EXACTLY 1 (A={after_a}, B={after_b}); \
         0 means the adapter is not carrying spec.leaseTransitions and two nodes could mint \
         the same epoch, >1 means it is being double-incremented"
    );
    assert_eq!(
        b.epoch(),
        Some(after_b),
        "the issued token must be the lease's transitions value"
    );
    // And the holder actually changed at the server.
    let holder = s.read(&name).expect("read").map(|r| r.holder);
    assert_eq!(holder.as_deref(), Some("holder-b"), "server-side holder");
}

/// ⚠ The adapter must NOT retry a 409. The spec: "an adapter that retries a 409
/// by re-reading and writing again has thrown the guarantee away."
///
/// Proven at the server: take the current version, let a competing write bump
/// it, then CAS on the now-stale version. It must come back `Ok(false)` — once —
/// and must not have changed the holder.
#[test]
#[ignore = "needs a real API server"]
fn a_stale_cas_is_refused_by_the_api_server_and_not_retried() {
    let s = store();
    let name = prefix("stale");
    let rec = |holder: &str, t: u64| ehdb_reference::election::LeaseRecord {
        holder: holder.to_string(),
        transitions: t,
        renewed_at_millis: 1_700_000_000_123,
        duration_secs: 15,
        version: 0,
    };
    assert!(s.create(&name, &rec("first", 1)).expect("create"));
    let v1 = s.read(&name).expect("read").expect("exists").version;

    // A competing write advances the resourceVersion.
    assert!(
        s.compare_and_swap(&name, v1, &rec("second", 2))
            .expect("cas"),
        "the current-version CAS should succeed"
    );
    let v2 = s.read(&name).expect("read").expect("exists").version;
    assert!(v2 > v1, "resourceVersion must advance: {v1} -> {v2}");

    // Now the stale one.
    let refused = s
        .compare_and_swap(&name, v1, &rec("third", 3))
        .expect("stale cas returns Ok(false), not an error");
    assert!(
        !refused,
        "⭐ THE MUTUAL EXCLUSION: the API server must refuse a stale resourceVersion"
    );
    let holder = s.read(&name).expect("read").expect("exists").holder;
    assert_eq!(
        holder, "second",
        "a refused CAS must not have changed the holder — if it did, the 409 was retried"
    );
    eprintln!("GATE  stale CAS refused at the server; holder still {holder}");
}
