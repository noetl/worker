//! The fence-coverage guard — [noetl/ai-meta#460](https://github.com/noetl/ai-meta/issues/460) C1.
//!
//! ## What this prevents, and why it is a test rather than a review note
//!
//! On 2026-09 the M5 fencing guard was **"ACTIVE, ENFORCING" and still served a superseded
//! writer**, because the check sat on `append_segment` and the publish path skipped it. The
//! algorithm was fine; the **placement** was not. The rule that followed —
//! *"a stale writer must be rejected by the store, not asked to check first"* — is honoured
//! today by a **decorator**: `build_durable_stack` wraps the plain backend when
//! `NOETL_EHDB_FENCING` is `shadow`/`enforce`, and a decorator cannot be skipped by a
//! caller who forgot to check.
//!
//! ⚠⚠ But a decorator *can* be bypassed one way: by **constructing the store somewhere
//! else** and never passing it through the wrap. That is the M5 shape reappearing one level
//! up, and it is invisible to every runtime test — a second construction site works
//! perfectly, and simply is not fenced.
//!
//! This guard fails when that happens. It changes no runtime behaviour.
//!
//! ⚠ Scope, stated so a green result is not over-read: this covers the **shared
//! event-log backend** that the fencing decorator wraps. `tier_store`'s JSONL segment path
//! is a **separate store** the decorator has no relationship to — see
//! `the_tier_store_mutation_paths_are_registered` below, which guards that set by
//! enumeration instead.

/// Production source of a worker file — every `#[cfg(test)]` block removed by brace
/// matching.
///
/// ⚠⚠ Not "truncate at the first `#[cfg(test)]`". This repo has shipped a `non_test()`
/// helper that cut at the first occurrence and silently discarded thousands of lines of
/// production code, so every check downstream of it ran on an empty slice and passed.
/// `eventlog_backend.rs` has **two** separate test modules (the first closes before the
/// second opens), which is exactly the shape that trap needs.
fn production_src(path: &str) -> String {
    let raw = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let lines: Vec<&str> = raw.lines().collect();
    let mut out: Vec<&str> = Vec::new();
    let mut i = 0usize;
    while i < lines.len() {
        if lines[i].trim_start().starts_with("#[cfg(test)]") {
            // Skip to the end of the attached item by brace matching.
            let mut depth = 0i32;
            let mut opened = false;
            while i < lines.len() {
                depth += lines[i].matches('{').count() as i32;
                depth -= lines[i].matches('}').count() as i32;
                if lines[i].contains('{') {
                    opened = true;
                }
                i += 1;
                if opened && depth <= 0 {
                    break;
                }
            }
            continue;
        }
        out.push(lines[i]);
        i += 1;
    }
    let kept = out.join("\n");
    // ⭐ Assert the extraction BEFORE asserting anything about it. A slice that came back
    // implausibly small is the failure mode that makes every check below vacuous.
    assert!(
        kept.len() > raw.len() / 4,
        "production slice of {path} is {} bytes of {} — the test-stripper ate production \
         code, and every assertion below would be vacuous",
        kept.len(),
        raw.len()
    );
    kept
}

const BACKEND: &str = "src/ehdb/eventlog_backend.rs";

/// ⭐⭐ THE GUARD. The shared backend must be constructed in exactly ONE production place,
/// and that place must be the function that decides whether to wrap it.
///
/// A second `FilesystemSharedBackend::open` in production code is a store that no fencing
/// setting can ever reach. It would pass every runtime test.
#[test]
fn the_shared_backend_is_constructed_in_exactly_one_production_place() {
    let src = production_src(BACKEND);
    let sites: Vec<usize> = src
        .lines()
        .enumerate()
        .filter(|(_, l)| l.contains("FilesystemSharedBackend::open"))
        .map(|(n, _)| n + 1)
        .collect();
    assert_eq!(
        sites.len(),
        1,
        "⚠⚠ {} production construction sites for the shared backend (lines {sites:?}). \
         Exactly one may exist, inside `build_durable_stack`, because that is the only \
         place that consults NOETL_EHDB_FENCING. A second site is a store the fence can \
         never wrap — the M5 shape one level up, and invisible to every runtime test.",
        sites.len()
    );

    // ...and it must be inside `build_durable_stack`.
    let fn_at = src
        .find("pub fn build_durable_stack(")
        .expect("build_durable_stack exists");
    let site_at = src
        .find("FilesystemSharedBackend::open")
        .expect("the construction exists");
    assert!(
        site_at > fn_at,
        "the construction is not inside build_durable_stack"
    );
}

// ---------------------------------------------------------------------------
// ⚠⚠ A test that is DELIBERATELY ABSENT, and why.
//
// I wrote a guard asserting that the stack is built from the wrapped handle and that the
// fencing setting is read before the wrap. Then I tried to RED-prove it, twice, and
// could not — for a good reason both times:
//
// * **"Build the stack from `plain`"** cannot be written in compiling Rust. `plain` is
//   *moved* in both match arms (`Arc::new(plain)` and `FencedSharedBackend::new(plain,
//   …)`), so it does not exist after the match, and `open_with_segment_size` takes
//   `Arc<dyn SharedSegmentBackend>` rather than the concrete type. **Move semantics and
//   the type system guarantee it.**
// * **"Read the setting after the wrap"** cannot happen either: the wrap is
//   `match setting { … }`, a data dependency the compiler enforces.
//
// My mutants for both failed to compile or were self-defeating — and a mutant that does
// not compile is not a caught mutant. So a structural test for either would be a check
// that cannot fire for the reason it claims, which this codebase has enough of. It is
// recorded here instead: **within `build_durable_stack` the bypass is impossible by
// construction.** The residual risks are the two the tests below DO catch — a second
// construction site, and a new unregistered append path.
// ---------------------------------------------------------------------------

/// Every production path that mutates durable tier state, enumerated.
///
/// ⚠ A registry, not a pattern match: these five have different shapes and no single idiom
/// finds them. A new one must be added here deliberately, which is the point — the review
/// question "does this new append need fencing?" gets asked by a failing test rather than
/// by someone remembering.
///
/// ⚠⚠ `tier_store`'s JSONL segment store is NOT wrapped by the fencing decorator — it is a
/// separate store. Registering its paths here does not claim they are fenced; it claims
/// the **set is known**, so a sixth one cannot appear unnoticed while #460 C2/C3 are still
/// open decisions.
const REGISTERED_MUTATION_PATHS: [(&str, &str); 6] = [
    ("src/ehdb/tier_store.rs", "pub async fn append("),
    ("src/ehdb/tier_store.rs", "pub async fn append_with_seal("),
    ("src/ehdb/tier_store.rs", "pub async fn append_batch("),
    ("src/ehdb/tier_shadow.rs", "pub async fn append("),
    ("src/ehdb/dataplane.rs", "pub fn append_domain_record("),
    ("src/ehdb/eventlog_backend.rs", "pub fn append_selected("),
];

#[test]
fn every_registered_mutation_path_still_exists() {
    // ⭐ A registry whose entries have been renamed away is worse than no registry: it
    // reports a complete set while covering nothing.
    let mut missing = Vec::new();
    for (file, sig) in REGISTERED_MUTATION_PATHS {
        if !production_src(file).contains(sig) {
            missing.push(format!("{file}: {sig}"));
        }
    }
    assert!(
        missing.is_empty(),
        "registered mutation paths no longer exist (renamed or removed): {missing:?} — \
         update the registry deliberately rather than letting it rot into a set that \
         covers nothing"
    );
}

/// ⭐⭐ The completeness half: a NEW public append in these modules must be registered.
#[test]
fn no_unregistered_public_append_path_exists() {
    let files = [
        "src/ehdb/tier_store.rs",
        "src/ehdb/tier_shadow.rs",
        "src/ehdb/dataplane.rs",
        "src/ehdb/eventlog_backend.rs",
    ];
    let mut scanned = 0usize;
    let mut unregistered = Vec::new();
    for file in files {
        let src = production_src(file);
        for line in src.lines() {
            let t = line.trim();
            // Public append-shaped entry points. Narrow on purpose: `pub fn append…` is the
            // idiom all six registered paths share, and a wider net would flag helpers.
            let is_append_fn = (t.starts_with("pub fn append") || t.starts_with("pub async fn append"))
                && t.contains('(');
            if !is_append_fn {
                continue;
            }
            scanned += 1;
            // Normalise to the registry's form: everything up to and including the paren.
            let sig = match t.find('(') {
                Some(i) => &t[..=i],
                None => continue,
            };
            let known = REGISTERED_MUTATION_PATHS
                .iter()
                .any(|(f, s)| *f == file && *s == sig);
            if !known {
                unregistered.push(format!("{file}: {sig}"));
            }
        }
    }
    // ⭐ The denominator. A pass over zero candidates is consistent with a broken scan, and
    // this guard's whole value is that it looked at something.
    println!("  scanned {scanned} public append entry points across {} files", files.len());
    assert!(
        scanned >= REGISTERED_MUTATION_PATHS.len(),
        "the scan found {scanned} append entry points but {} are registered — the scan is \
         broken, not the code",
        REGISTERED_MUTATION_PATHS.len()
    );
    assert!(
        unregistered.is_empty(),
        "⚠⚠ UNREGISTERED durable-state mutation path(s): {unregistered:?}\n\n\
         A new append has appeared in a module that writes durable state. Add it to \
         REGISTERED_MUTATION_PATHS — and while doing so, answer the question this guard \
         exists to force: does it need to pass through the fencing decorator? The M5 \
         incident was a fence that was active, enforcing, and skipped by one path."
    );
}

/// The fencing setting must keep its fail-safe direction: anything unrecognised is OFF.
///
/// ⚠ A typo must not silently *enable* a mode that changes write behaviour on a
/// primary-serving store. (The inverse of the data-deleting-flag rule: here the unsafe
/// direction is "on", so unrecognised means off.)
#[test]
fn an_unrecognised_fencing_value_is_off_not_enforce() {
    let src = production_src(BACKEND);
    let at = src
        .find("impl FencingSetting")
        .or_else(|| src.find("enum FencingSetting"))
        .expect("FencingSetting exists");
    let window = &src[at..(at + 1600).min(src.len())];
    assert!(
        window.contains("Off"),
        "FencingSetting must have an Off variant: {window}"
    );
    // The documented contract, kept adjacent to the code so it cannot drift away silently.
    assert!(
        src.contains("the default, and any unrecognised value) does not wrap"),
        "the fail-safe-off contract must stay documented at the setting: an unrecognised \
         value enabling Enforce on a primary-serving store would be an outage"
    );
}
