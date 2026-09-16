//! The failure gate must be WIRED, not merely correct — noetl/server#434.
//!
//! ⚠ This file exists because of a mistake made on noetl/ai-meta#343. The
//! resolve-fallback there was written correctly, unit-tested, and wired into
//! only the HTTP handler — so the kind run showed four successful fallbacks
//! while the parent step still received zero items. The function was right and
//! the system was unchanged.
//!
//! `terminal_event_for` has the same shape of risk: it is a pure function whose
//! tests pass whether or not anything calls it. If the emission site goes back
//! to the string literal, every unit test in `executor::command` still passes
//! and a failed step silently advances the DAG again.

const COMMAND_RS: &str = include_str!("../src/executor/command.rs");

/// The body of `execute_command`'s terminal emission, from the decision to the
/// `emit_event_via` that sends it.
fn terminal_emission() -> &'static str {
    let from = COMMAND_RS
        .find("let terminal_event = terminal_event_for(")
        .expect(
            "the terminal event is no longer chosen by `terminal_event_for` — \
             noetl/server#434's gate has been removed or renamed",
        );
    let rest = &COMMAND_RS[from..];
    let to = rest
        .find("record_metric(false);")
        .expect("the end of the terminal emission");
    &rest[..to]
}

/// ⭐ The decision must reach the wire.
#[test]
fn the_emitted_terminal_event_comes_from_the_gate_not_a_literal() {
    let block = terminal_emission();

    assert!(
        block.contains("            terminal_event,"),
        "`emit_event_via` is not being passed `terminal_event`. The gate \
         computes the right answer and then throws it away, so a failed step \
         still emits `command.completed` and the DAG still advances past it — \
         which is the whole defect in noetl/server#434.\n\nBlock was:\n{block}"
    );

    assert!(
        !block.contains("\"command.completed\","),
        "the terminal emission passes the literal \"command.completed\" — the \
         gate has been bypassed.\n\nBlock was:\n{block}"
    );
}

/// The envelope status has to agree with the event, or a `command.failed`
/// arrives labelled `error` and the server's own `has_errored_step` query —
/// which matches on `lower(status) IN ('failed','error')` — reads it
/// inconsistently with every other failure path.
#[test]
fn a_failed_command_is_labelled_failed() {
    let block = terminal_emission();
    assert!(
        block.contains("\"FAILED\".to_string()"),
        "a `command.failed` must carry the FAILED envelope status, matching the \
         pre-dispatch and call.error paths.\n\nBlock was:\n{block}"
    );
}

/// The positive control: these checks must be able to fail.
#[test]
fn the_checks_can_actually_fail() {
    let bypassed = "let terminal_event = terminal_event_for(a, b, c);\n \
                    emit_event_via(&c, \"command.completed\",\n record_metric(false);";
    assert!(
        bypassed.contains("\"command.completed\","),
        "the literal-bypass matcher cannot see a hardcoded event name"
    );
    assert!(
        !bypassed.contains("            terminal_event,"),
        "the wiring matcher would pass a call site that never forwards the gate"
    );
}
