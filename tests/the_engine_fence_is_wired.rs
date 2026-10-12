//! The engine fence reaches the real writer, and is off by default.
//!
//! ⚠ These guards exist because the defect being fixed was *exactly* a fence
//! that was built, registered, documented and never on the path: 624 prod
//! appends with `writes_checked` at 0 (noetl/ai-meta#460). A fence whose wiring
//! is unasserted is the same bug waiting to recur.

static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// ⚠ `cargo test` does not serialise tests, so two tests mutating the same env
/// var race and fail intermittently rather than loudly.
fn serialised() -> std::sync::MutexGuard<'static, ()> {
    match ENV_LOCK.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

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

/// ⚠⚠ The C3 gate must be readable through a restart, so the election and
/// engine-fence blocks must NOT sit behind unrelated state.
///
/// They used to live inside `if !ehdb_lines.is_empty()`, and a prod scrape 87 s
/// after a restart came back 30 KB with both blocks at **zero lines** while the
/// older `ehdb_fencing_*` block was present. `epoch_advances` is the one series
/// that distinguishes an inert fence from a live one; gating it behind the EHDB
/// data-plane metrics made it absent during exactly the window it covers.
#[test]
fn the_election_and_fence_blocks_are_not_gated_on_unrelated_state() {
    let src = code(include_str!("../src/metrics_server.rs"));
    let gate = concat!("ehdb_lines.is_", "empty()");
    let elect = concat!("render_", "election");
    let fences = concat!("render_engine_", "fences");

    // Find the LAST occurrence of the gate, and require both renderers to be
    // called after it at top level — i.e. not nested inside it.
    let gate_at = src
        .rfind(gate)
        .expect("the gate literal vanished; rewrite this guard");
    let elect_at = src
        .rfind(elect)
        .expect("render_election is no longer called");
    let fences_at = src
        .rfind(fences)
        .expect("render_engine_fences is no longer called");
    assert!(
        elect_at > gate_at && fences_at > gate_at,
        "the election/fence blocks are emitted before or inside the          `ehdb_lines.is_empty()` gate — the C3 safety gate would read ABSENT          during the post-restart window it exists to cover"
    );

    // And structurally: the gate's block must not contain either call. The body
    // between the last gate and its closing brace is short by construction.
    let after = &src[gate_at..];
    let block_end = after.find("\n}").map(|i| i + gate_at).unwrap_or(src.len());
    let block = &src[gate_at..block_end];
    assert!(
        !block.contains(elect) && !block.contains(fences),
        "a renderer is still nested inside the ehdb_lines gate"
    );
}

/// ⭐ The pre-arm state: fencing configured, no fence built yet.
#[test]
fn a_configured_but_unarmed_fence_reports_registered_zero_rather_than_silence() {
    let _g = serialised();
    let key = "NOETL_EHDB_FENCING";
    let prev = std::env::var(key).ok();

    // Unconfigured and no writer → nothing at all. There is no useful zero for
    // a component this process does not have.
    unsafe { std::env::remove_var(key) };
    assert!(
        noetl_worker::ehdb::election::render_engine_fences().is_empty(),
        "a process with no writer and no fencing config must render nothing"
    );

    // Configured but not yet armed → readable, with the discriminator.
    unsafe { std::env::set_var(key, "shadow") };
    let text = noetl_worker::ehdb::election::render_engine_fences();
    assert!(
        text.contains("ehdb_engine_fence_registered 0"),
        "⚠ configured-but-unarmed must be readable as registered 0, not absent:\n{text}"
    );

    // ⚠ An unrecognised value must NOT arm the block either — it is Off.
    unsafe { std::env::set_var(key, "enforce-later") };
    assert!(
        noetl_worker::ehdb::election::render_engine_fences().is_empty(),
        "an unrecognised flag value must not render the block"
    );

    match prev {
        Some(v) => unsafe { std::env::set_var(key, v) },
        None => unsafe { std::env::remove_var(key) },
    }
}
