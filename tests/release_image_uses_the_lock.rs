//! **The release image must build against the committed `Cargo.lock`.**
//!
//! On 2026-10-10 `release-worker` failed for v6.2.2 with `E0308` in
//! `src/spool_runtime.rs` after a 47-minute build, on a commit whose
//! `cargo test --all-targets --locked` was green. The two are consistent
//! because they were building **different dependency graphs**:
//!
//! - `noetl-tools = "~4.0"` floats. The lock says **4.0.1** (which needs
//!   `async-nats 0.38`); the image build resolved **4.0.6** (which needs
//!   `async-nats 0.47`), and the worker's own `async-nats = "0.38"` then put
//!   two incompatible `Context` types in one graph.
//! - The last successful release was 2026-10-02, so a new `noetl-tools` was
//!   published in between and the break was waiting for whoever released next.
//!
//! ⚠ Which means the shipped artifact's dependency graph was never the one any
//! test exercised, and the divergence appears only when the registry index
//! moves — so it passed locally, passed on 2026-10-02, and failed today. This
//! is the same class as noetl/worker#183, where a caret range silently dropped
//! DuckDB from the shipped binary: **a floating range is a decision made later
//! by a resolver**, and the resolver was not running anywhere anyone was
//! looking.
//!
//! `--locked` removes the dependence on index state. This test keeps it there.

const DOCKERFILE: &str = include_str!("../Dockerfile");

#[test]
fn every_cargo_invocation_in_the_release_image_is_locked() {
    // ⚠ Assert the extraction before asserting about it.
    assert!(
        DOCKERFILE.len() > 500,
        "Dockerfile read as {} bytes — the checks below would be vacuous",
        DOCKERFILE.len()
    );

    let cargo_lines: Vec<&str> = DOCKERFILE
        .lines()
        .map(str::trim)
        .filter(|l| !l.starts_with('#'))
        .filter(|l| l.contains("cargo build") || l.contains("cargo chef cook"))
        .collect();

    // The denominator: if this is 0 the loop below proves nothing.
    assert!(
        cargo_lines.len() >= 2,
        "expected at least the chef-cook and the app build; found {}: {cargo_lines:?}",
        cargo_lines.len()
    );

    for line in &cargo_lines {
        assert!(
            line.contains("--locked"),
            "⚠ this cargo invocation in the release image does not pass --locked, so the \
             shipped artifact's dependency graph is whatever the registry index happens to \
             offer at build time rather than the graph CI tested:\n  {line}"
        );
    }
}

#[test]
fn the_cook_stage_has_a_lock_to_honour() {
    // `cargo chef cook --locked` fails outright without a Cargo.lock in the
    // build directory, and the builder stage copies only recipe.json. So the
    // lock must be copied explicitly — relying on cargo-chef to carry it inside
    // the recipe would make this silently resolution-dependent again, one layer
    // earlier where it is harder to see.
    let cook_at = DOCKERFILE
        .find("cargo chef cook")
        .expect("the cook stage exists");
    let before = &DOCKERFILE[..cook_at];
    assert!(
        before.contains("COPY Cargo.lock"),
        "the cook stage runs with --locked but nothing copies Cargo.lock before it"
    );
}

#[test]
fn the_lock_is_tracked_and_not_dockerignored() {
    // Both are required for the above to mean anything: a lock that is not in
    // the build context cannot be honoured, and the failure would look like a
    // resolution problem rather than a missing file.
    let ignore = include_str!("../.dockerignore");
    for pat in ["Cargo.lock", "*.lock", "/Cargo.lock"] {
        assert!(
            !ignore
                .lines()
                .map(str::trim)
                .any(|l| l == pat),
            ".dockerignore excludes {pat}, so the build context has no lock"
        );
    }
}
