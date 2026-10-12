//! The engine fence reaches the real writer, and is off by default.
//!
//! ⚠ These guards exist because the defect being fixed was *exactly* a fence
//! that was built, registered, documented and never on the path: 624 prod
//! appends with `writes_checked` at 0 (noetl/ai-meta#460). A fence whose wiring
//! is unasserted is the same bug waiting to recur.

/// Non-test code only, comments stripped — a guard that matches its own prose
/// is not a guard.
fn code(src: &str) -> String {
    assert!(
        src.len() > 1000,
        "source read as {} bytes — this guard would be vacuous",
        src.len()
    );
    let body: Vec<&str> = src
        .split("#[cfg(test)]")
        .next()
        .unwrap_or("")
        .lines()
        .map(str::trim_start)
        .filter(|l| !l.starts_with("//") && !l.starts_with('*'))
        .collect();
    assert!(!body.is_empty(), "non-test slice came out empty");
    body.join("\n")
}

#[test]
fn both_writer_hosts_construct_a_fenced_writer() {
    // ⚠ Split needles so this cannot match itself.
    let fenced = concat!("new_", "fenced");
    let build = concat!("build_engine_", "fence");
    for (name, src) in [
        (
            "command_bus.rs",
            code(include_str!("../src/command_bus.rs")),
        ),
        ("event_bus.rs", code(include_str!("../src/event_bus.rs"))),
    ] {
        assert!(
            src.contains(fenced),
            "{name} does not construct a fenced writer — the fence would exist \
             and never be on the append path, which is the defect this fixes"
        );
        assert!(
            src.contains(build),
            "{name} does not build a fence, so nothing supplies a mode or ledger"
        );
    }
}

#[test]
fn the_election_pushes_its_token_into_the_registered_fences() {
    // Without this the fence holds epoch 0 forever: Fresh against an empty
    // ledger, never advancing the marker — an enforcing fence that cannot
    // refuse, reporting healthy.
    let src = code(include_str!("../src/ehdb/election.rs"));
    let push = concat!("push_epoch_to_", "fences");
    assert!(
        src.matches(push).count() >= 2,
        "the token push must be both defined AND called from the tick loop"
    );
    let reg = concat!("register_", "fence");
    assert!(src.contains(reg), "no fence registry");
}

#[test]
fn the_fence_block_is_on_the_scrape() {
    // ⭐ The C3 gate reads `epoch_advances` off a served endpoint. If the block
    // is not rendered, the gate cannot be satisfied at all — and absence would
    // be indistinguishable from an inert fence.
    let src = code(include_str!("../src/metrics_server.rs"));
    assert!(
        src.contains(concat!("render_engine_", "fences")),
        "the engine-fence block is not rendered, so the C3 gate is unreadable"
    );
}

#[test]
fn the_fence_is_off_unless_asked_for() {
    // ⚠ Reads the ambient env rather than setting it: `cargo test` does not
    // serialise, and a test that set NOETL_EHDB_FENCING would race every other
    // reader in this binary.
    assert!(
        std::env::var("NOETL_EHDB_FENCING").is_err(),
        "NOETL_EHDB_FENCING is set in this test environment"
    );
    assert_eq!(
        ehdb_core::fence::FenceMode::from_str_or_off(""),
        ehdb_core::fence::FenceMode::Off
    );
    // Anything unrecognised is Off, never a partial arm.
    for v in ["", "on", "true", "1", "yes", "ENFORCE_LATER", "shadowy"] {
        assert_eq!(
            ehdb_core::fence::FenceMode::from_str_or_off(v),
            ehdb_core::fence::FenceMode::Off,
            "{v:?} must not arm the fence"
        );
    }
    assert_eq!(
        ehdb_core::fence::FenceMode::from_str_or_off("shadow"),
        ehdb_core::fence::FenceMode::Shadow
    );
    assert_eq!(
        ehdb_core::fence::FenceMode::from_str_or_off("Enforce"),
        ehdb_core::fence::FenceMode::Enforce
    );
}

#[test]
fn nothing_is_registered_in_a_process_that_hosts_no_writer() {
    // The renderer must emit NOTHING rather than a zeroed block, because there
    // is no useful zero for a component that does not exist in this process.
    assert!(
        noetl_worker::ehdb::election::render_engine_fences().is_empty(),
        "a process with no writer must render no fence block"
    );
}
