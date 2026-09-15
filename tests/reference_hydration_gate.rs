//! **Externalised-result hydration gate** — the durable guard for a bug that
//! passed every unit test while being completely dead in production.
//!
//! ## What happened, and why the existing tests did not catch it
//!
//! A `kind: playbook` child whose result crosses the externalisation byte budget
//! is stored out of line; the consumer receives a locator instead of the payload.
//! On 2026-09-15 `muno/playbooks/hotel-cards` returned **0 hotels on four
//! consecutive prod runs** while the child had really fetched 5 hotels / 509
//! images / 67 rates (214,805 bytes). Silently: `status: success`, empty list,
//! no error.
//!
//! The consume path is a chain:
//!
//! ```text
//!   resolve_context_references
//!     └─ reference_locators          <-- THE GATE. No candidate => silent return.
//!          └─ step_needs_bulk_resolution
//!               └─ path_satisfiable
//!                    └─ contains_summary_bulk
//! ```
//!
//! noetl/worker#315 fixed `contains_summary_bulk` and `is_reference_stub`, with
//! four unit tests and a verified negative control. It shipped to prod as
//! v5.132.2 and **changed nothing** — hotel-cards execution
//! `358309183893807104` still returned `count: 0`, on an image that
//! demonstrably contained the fix.
//!
//! The reason: `reference_locators` recognised only a nested `reference`
//! OBJECT, while a child result actually arrives as **flat accessors**
//! (`{"_ref": …, "data": {"_ref": …}}` — measured on prod, probe execution
//! `358317531129192448`). The gate produced no candidate, so everything below it
//! never ran. **Every unit test below the gate passed while the path was dead.**
//!
//! ⚠ The sharpest detail: `{_ref, data:{_ref}}` is the *exact* shape the
//! existing `bare_ref_stub_summary_forces_resolution` unit test uses. That test
//! exercises `step_needs_bulk_resolution` — downstream. The shape was well
//! covered at the predicate and completely uncovered at the gate in front of it.
//!
//! **The lesson this file encodes: a unit test that starts below a gate cannot
//! tell you the gate is shut.** These assertions are deliberately anchored at
//! the gate and on the real wire shapes, not on the predicate.
//!
//! Companion coverage: the unit tests in `src/executor/command.rs`
//! (`reference_locators_accepts_the_flat_accessor_shape`,
//! `whole_object_bind_of_a_reference_resolves`,
//! `truncated_sample_is_never_treated_as_complete`) call the private functions
//! directly. This file is the source-level guard that they stay wired, which is
//! the half that was missing.

const COMMAND_RS: &str = include_str!("../src/executor/command.rs");

/// Source of `command.rs` with comments stripped and the test module removed.
///
/// Both exclusions matter and both have burned this repo before: a doc comment
/// naming a symbol satisfied an earlier reachability check, and a needle written
/// inside `#[cfg(test)]` matched itself.
fn production_source() -> String {
    let cut = COMMAND_RS.find("#[cfg(test)]").unwrap_or(COMMAND_RS.len());
    // ⚠ Indentation is PRESERVED deliberately. An earlier cut of this helper
    // trimmed every line, which flattened a NESTED `fn` to column 0 — so
    // `fn_body` stopped slicing there and never saw the rest of the function.
    // That produced a false failure on a correct implementation, which is the
    // reassuring-direction error this file exists to avoid. Comments are still
    // stripped (a doc comment naming a symbol has satisfied a guard here before).
    COMMAND_RS[..cut]
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Slice the body of a named top-level `fn`, so an assertion about one function
/// cannot be satisfied by text somewhere else in the file.
fn fn_body(src: &str, name: &str) -> String {
    // Anchor at a TOP-LEVEL definition (column 0), accepting both `fn` and
    // `async fn`, so a nested helper of the same shape cannot truncate the slice
    // and an async function is not silently missed.
    let start = [format!("\nfn {name}("), format!("\nasync fn {name}(")]
        .iter()
        .filter_map(|sig| src.find(sig.as_str()))
        .min()
        .unwrap_or_else(|| {
            panic!(
                "top-level function `{name}` not found — was it renamed or made \
                 non-top-level? This guard must be updated deliberately, not deleted."
            )
        });
    let rest = &src[start + 1..];
    // Terminate at the next TOP-LEVEL definition. A nested `fn` is indented and
    // therefore does not match.
    let end = ["\nfn ", "\nasync fn "]
        .iter()
        .filter_map(|m| rest[1..].find(m).map(|i| i + 1))
        .min()
        .unwrap_or(rest.len());
    rest[..end].to_string()
}

#[test]
fn the_gate_accepts_the_flat_accessor_shape_the_runtime_actually_emits() {
    // THE REGRESSION. `reference_locators` must recognise `_ref` and not only a
    // nested `reference` object, because the flat form is what a kind: playbook
    // child result arrives as. If this stops being true, every downstream fix is
    // dead again and no unit test will notice.
    let body = fn_body(&production_source(), "reference_locators");
    assert!(
        body.contains("_ref"),
        "reference_locators no longer reads the flat `_ref` accessor.\n\
         A kind: playbook child result arrives as {{\"_ref\": …, \"data\": {{\"_ref\": …}}}}\n\
         (prod probe 358317531129192448). Without this the gate yields no candidate,\n\
         resolve_context_references returns silently, and an over-budget upstream\n\
         is delivered to the step as a bare reference — the noetl/worker#315 bug."
    );
    assert!(
        body.contains("reference"),
        "reference_locators must still handle the nested `reference` object shape too."
    );
    // The needle above is necessary but weak. What actually broke twice is
    // DEPTH: the locator in a real parent `steps` entry sits at
    // `/context/result/context/data/_ref` (prod execution 358323454170112000),
    // deeper than any fixed path. Require a structural search, not positions.
    assert!(
        body.contains("find_locator") || body.contains("depth"),
        "reference_locators looks position-based again.\n\
         Fixed paths have now failed TWICE against real envelope nesting\n\
         (#317 checked top level, `data` and `/context/result` — the real shape\n\
         is one level deeper still). It must search by STRUCTURE, depth-capped."
    );
}

#[test]
fn the_predicate_still_treats_a_locator_as_collapsed_bulk() {
    // noetl/worker#315. A whole-object bind takes the EMPTY accessor path
    // straight to contains_summary_bulk; if that stops treating a locator as
    // bulk, an over-budget upstream reads as "satisfiable" and is never resolved.
    let body = fn_body(&production_source(), "contains_summary_bulk");
    assert!(
        body.contains("object_has_locator") || body.contains("_ref"),
        "contains_summary_bulk no longer treats a locator as collapsed bulk.\n\
         A whole-object bind ({{{{ step }}}}, {{{{ step | default({{}}) }}}}) then reads as\n\
         satisfiable and the payload is never fetched — silently."
    );
}

#[test]
fn a_truncated_sample_is_never_mistaken_for_the_payload() {
    // The API returns the reference plus `extracted` metadata flagged
    // `_truncated`. It is a SAMPLE. An early cut of the playbook-side guard used
    // it and reported 1 hotel as though it were the whole answer — silently
    // wrong, which is strictly worse than empty, and it would have passed a
    // smoke test.
    let src = production_source();
    let stub = fn_body(&src, "is_reference_stub");
    assert!(
        stub.contains("extracted") || stub.contains("_truncated"),
        "is_reference_stub no longer recognises reference METADATA keys.\n\
         A container like {{_ref, extracted:{{_truncated, …}}}} is not a\n\
         key-preserving summary, so \"absent in the summary => absent in the payload\"\n\
         does not hold for it. Without this, `{{{{ step.data.rows }}}}` is answered\n\
         from the truncated sample."
    );
}

#[test]
fn the_gate_is_actually_called_by_the_consume_path() {
    // Reachability, in the spirit of `reachability_guard.rs`: a gate nothing
    // calls is a gate that cannot fail open, and this whole class of bug is
    // "implemented but unreachable".
    let src = production_source();
    let caller = fn_body(&src, "resolve_context_references");
    assert!(
        caller.contains("reference_locators"),
        "resolve_context_references no longer calls reference_locators — the\n\
         consume-side hydration path is unwired."
    );
    assert!(
        caller.contains("step_needs_bulk_resolution"),
        "resolve_context_references no longer consults step_needs_bulk_resolution."
    );
    assert!(
        src.contains("resolve_context_references(&mut ctx.variables"),
        "resolve_context_references has no call site in the command executor —\n\
         the entire hydration path is dead. This is exactly the failure mode\n\
         reachability_guard.rs exists to catch."
    );
}
