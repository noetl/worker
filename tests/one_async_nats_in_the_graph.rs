//! **The `async-nats` version must be unique in the dependency graph, and that
//! requirement cannot be expressed as a range.**
//!
//! This crate pins `async-nats = "0.38"`. `noetl-tools` 4.0.6 depends on
//! async-nats **0.47**, so any resolution that picks 4.0.6 puts two
//! incompatible `async_nats::jetstream::Context` types in one graph and
//! `src/spool_runtime.rs` fails with `E0308`.
//!
//! ⚠⚠ Two release runs (v6.2.2, v6.2.3) failed exactly that way after
//! ~45-minute builds, on commits whose `cargo test --locked` was **green** —
//! because the test job resolved from the committed lock and the image never
//! did. `cargo chef cook` works from `recipe.json`, a skeleton reconstruction
//! of the manifests, and writes its own `Cargo.lock` over whatever is in the
//! build directory. So `~4.0` plus a lock plus `--locked` was still not enough:
//! the cook stage resolved 4.0.6 regardless.
//!
//! An **exact** pin removes the resolver from the decision. This test keeps it
//! exact, because the next person to "tidy up" a `=` into a `^` would not see
//! a failure for ~45 minutes and would see it in a stage no local command runs.

const CARGO_TOML: &str = include_str!("../Cargo.toml");

#[test]
fn noetl_tools_is_pinned_exactly() {
    assert!(
        CARGO_TOML.len() > 1000,
        "Cargo.toml read as {} bytes — the check below would be vacuous",
        CARGO_TOML.len()
    );
    let line = CARGO_TOML
        .lines()
        .map(str::trim)
        .find(|l| l.starts_with("noetl-tools = "))
        .expect("noetl-tools is a dependency");
    assert!(
        line.contains("\"=") || line.contains("= \"="),
        "⚠ noetl-tools must be pinned EXACTLY (`=x.y.z`). A range delegates the \
         async-nats version to whichever resolver runs last, and the image build's \
         cook stage does not honour the lock — so a range here fails the release \
         ~45 minutes in, in a stage no local command exercises. Found:\n  {line}"
    );
}

/// The invariant the pin exists to protect, asserted against the resolved lock
/// rather than against the manifest.
#[test]
fn exactly_one_async_nats_version_is_resolved() {
    let lock = include_str!("../Cargo.lock");
    let n = lock.matches("name = \"async-nats\"").count();
    assert_eq!(
        n, 1,
        "⚠ {n} async-nats versions in Cargo.lock. Two means `spool_runtime.rs` will fail \
         with E0308 on `async_nats::jetstream::Context` — the shipped build's failure mode, \
         not a tidiness issue."
    );
    let tools = lock.matches("name = \"noetl-tools\"").count();
    assert_eq!(tools, 1, "{tools} noetl-tools versions in Cargo.lock");
}
