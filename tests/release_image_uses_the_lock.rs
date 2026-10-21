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
            !ignore.lines().map(str::trim).any(|l| l == pat),
            ".dockerignore excludes {pat}, so the build context has no lock"
        );
    }
}

// ---- noetl/ai-meta#494: the stamp step must not undo `--locked` ----
//
// `--locked` above is only as good as the lock in the build context, and the
// release workflow runs `ci/stamp-version.sh` BEFORE the image build. That
// script used to run `cargo generate-lockfile`, which re-resolved the whole
// graph at release time — so `--locked` faithfully honoured a lock regenerated
// minutes earlier, and the 2026-10-10 fix above was defeated one step upstream.

const STAMP: &str = include_str!("../ci/stamp-version.sh");

#[test]
fn the_stamp_step_does_not_re_resolve_the_graph() {
    assert!(STAMP.len() > 300, "stamp script read as {} bytes", STAMP.len());
    for line in STAMP.lines().map(str::trim).filter(|l| !l.starts_with('#')) {
        for forbidden in ["generate-lockfile", "cargo update"] {
            assert!(
                !line.contains(forbidden),
                "⚠ ci/stamp-version.sh runs `{forbidden}`, which re-resolves Cargo.lock at \
                 release time — the image would then build a graph CI never tested:\n  {line}"
            );
        }
    }
}

fn stamp_in(dir: &std::path::Path, version: &str) -> std::process::Output {
    std::process::Command::new("bash")
        .arg(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("ci/stamp-version.sh"))
        .arg(version)
        .current_dir(dir)
        .output()
        .expect("bash runs")
}

fn scratch(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("stamp-494-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for f in ["Cargo.toml", "Cargo.lock"] {
        std::fs::copy(root.join(f), d.join(f)).unwrap();
    }
    d
}

/// The real script, on a copy: Cargo.lock changes in EXACTLY one line — the
/// root package's version — and Cargo.toml carries the stamp.
#[test]
fn stamping_changes_only_the_root_version_line_of_the_lock() {
    let d = scratch("ok");
    let before = std::fs::read_to_string(d.join("Cargo.lock")).unwrap();
    let out = stamp_in(&d, "9.8.7");
    assert!(
        out.status.success(),
        "stamp failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let after = std::fs::read_to_string(d.join("Cargo.lock")).unwrap();
    let changed: Vec<(&str, &str)> = before
        .lines()
        .zip(after.lines())
        .filter(|(a, b)| a != b)
        .collect();
    assert_eq!(
        before.lines().count(),
        after.lines().count(),
        "the lock gained or lost lines — something re-resolved"
    );
    assert_eq!(changed.len(), 1, "lock lines changed: {changed:?}");
    assert_eq!(changed[0].1, r#"version = "9.8.7""#);
    let toml = std::fs::read_to_string(d.join("Cargo.toml")).unwrap();
    assert!(toml.contains("version = \"9.8.7\""), "Cargo.toml not stamped");
    let _ = std::fs::remove_dir_all(&d);
}

/// A lock with no entry for this package cannot be made consistent by a text
/// stamp; the release must stop rather than ship an unverified lock.
#[test]
fn a_lock_without_the_root_entry_is_refused() {
    let d = scratch("noroot");
    let lock = std::fs::read_to_string(d.join("Cargo.lock")).unwrap();
    std::fs::write(
        d.join("Cargo.lock"),
        lock.replace("name = \"noetl-worker\"\n", "name = \"not-this-package\"\n"),
    )
    .unwrap();
    let out = stamp_in(&d, "9.8.7");
    assert!(!out.status.success(), "a lock with no root entry was accepted");
    let _ = std::fs::remove_dir_all(&d);
}
