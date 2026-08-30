//! **Merged-but-unreferenced guard** — the durable fix for today's repeated
//! finding (noetl/ehdb#324).
//!
//! Five features were merged, tested and green while having **no call sites at
//! all**: fencing, the election, the age-seal knob, and both halves of the
//! failure-domain check. Each silently voided a gate's verify-before, because a
//! precondition that cannot be evaluated is not a precondition.
//!
//! Existence is what a naive grep measures. This asserts **reachability**, and
//! fails the build when a feature that is supposed to be wired stops being
//! wired — or when one that is supposed to be dead quietly comes alive.
//!
//! ## ⚠ The traps this had to be built around
//!
//! Getting a guard like this wrong is easy, and every failure mode is a false
//! *negative* — the reassuring direction:
//!
//! * **It counts its own source.** `include_str!` yields the whole file
//!   including this module, so a needle written here matches itself. Everything
//!   below scans only the portion **before** `#[cfg(test)]`, and this file keeps
//!   its needles in a table rather than inline.
//! * **Comments count as callers.** A doc comment naming a symbol cleared an
//!   earlier check on this platform. Comment lines are stripped.
//! * **The defining file is the caller.** Excluding it hid three intra-module
//!   callers and made a live path read as dead, so nothing is excluded by name
//!   here — the registry names the file the *use* must appear in.

/// One feature: where its use must (or must not) appear, and why.
struct Expect {
    feature: &'static str,
    /// The usage token. ⚠ Must be usage syntax (`::`, `<`, `(`), never a bare
    /// name — a bare name matches prose.
    needle: &'static str,
    /// File that must contain the use, with its non-test source.
    source: &'static str,
    /// `true` = must be reached. `false` = must stay unreached, deliberately.
    reachable: bool,
    /// What breaks if this flips. Printed on failure.
    why: &'static str,
}

const SOURCES: &[(&str, &str)] = &[
    ("eventlog_backend", include_str!("../src/ehdb/eventlog_backend.rs")),
    ("command_bus", include_str!("../src/command_bus.rs")),
    ("event_bus", include_str!("../src/event_bus.rs")),
    ("metrics_server", include_str!("../src/metrics_server.rs")),
];

const EXPECTATIONS: &[Expect] = &[
    Expect {
        feature: "F2 fencing decorator",
        needle: "FencedSharedBackend::new",
        source: "eventlog_backend",
        reachable: true,
        why: "G2's verify-before reads ehdb_fencing_stale_observed_total. Unwired, \
              that counter can never move and a zero means nothing.",
    },
    Expect {
        feature: "F3 seal-age knob",
        needle: "seal_max_age_from_env(",
        source: "command_bus",
        reachable: true,
        why: "Unwired, setting NOETL_EHDB_SEAL_MAX_AGE_MS on prod does NOTHING \
              silently, and G1b looks taken while changing nothing.",
    },
    Expect {
        feature: "F3 seal-age knob (event bus)",
        needle: "seal_max_age_from_env(",
        source: "event_bus",
        reachable: true,
        why: "The event log is the primary tier; wiring only the command bus \
              bounds the wrong log.",
    },
    Expect {
        feature: "F4 durability window (command bus)",
        needle: "unreplicated_snapshot(",
        source: "command_bus",
        reachable: true,
        why: "The window is the instrument the whole durability story reads from.",
    },
    Expect {
        feature: "F4 durability window (event bus)",
        needle: "unreplicated_snapshot(",
        source: "event_bus",
        reachable: true,
        why: "This is the PRIMARY tier. A previous release wired only the command \
              bus and shipped a window for the wrong log while looking complete.",
    },
    Expect {
        feature: "F5 replica-domain observation",
        needle: "REPLICA_DOMAINS",
        source: "eventlog_backend",
        reachable: true,
        why: "G4's verify-before asks whether the live paths violate the domain \
              check. Unwired, nothing computes it against the live paths.",
    },
    Expect {
        feature: "F1 election",
        needle: "ShardElection",
        source: "eventlog_backend",
        reachable: false,
        why: "Deliberately unwired: the K8s LeaseStore adapter needs an HTTP/kube \
              dependency decision. If this now has a call site, render_election() \
              must stop hard-coding 0 and ehdb#331's gate needs revisiting.",
    },
];

/// Non-comment lines of a source, excluding everything from `#[cfg(test)]` on.
fn production_lines(src: &str) -> impl Iterator<Item = &str> {
    src.split("#[cfg(test)]")
        .next()
        .unwrap_or("")
        .lines()
        .map(str::trim_start)
        .filter(|l| !l.starts_with("//") && !l.starts_with("/*") && !l.starts_with('*'))
}

fn uses(source: &str, needle: &str) -> usize {
    SOURCES
        .iter()
        .find(|(name, _)| *name == source)
        .map(|(_, src)| production_lines(src).filter(|l| l.contains(needle)).count())
        .unwrap_or_else(|| panic!("registry names unknown source {source:?}"))
}

#[test]
fn every_feature_that_must_be_reachable_still_is() {
    let mut broken = Vec::new();
    for e in EXPECTATIONS.iter().filter(|e| e.reachable) {
        if uses(e.source, e.needle) == 0 {
            broken.push(format!(
                "\n  {} — `{}` no longer appears in {}.\n     {}",
                e.feature, e.needle, e.source, e.why
            ));
        }
    }
    assert!(
        broken.is_empty(),
        "MERGED-BUT-UNREFERENCED: {} feature(s) became unreachable.{}\n\n\
         A feature nothing calls is indistinguishable from one that is broken, \
         and it silently voids whatever gate depends on its signal.",
        broken.len(),
        broken.join("")
    );
}

#[test]
fn features_that_are_deliberately_dead_have_not_quietly_come_alive() {
    for e in EXPECTATIONS.iter().filter(|e| !e.reachable) {
        assert_eq!(
            uses(e.source, e.needle),
            0,
            "{} is now referenced in {}.\n  {}",
            e.feature,
            e.source,
            e.why
        );
    }
}

// ---------------------------------------------------------------------------
// Controls. ⚠ A guard that has never been shown to fail is indistinguishable
// from one that cannot fail, and every trap above produced a false NEGATIVE.
// ---------------------------------------------------------------------------

#[test]
fn the_counter_finds_a_real_use_and_ignores_prose() {
    // Positive: a token that genuinely appears in production code.
    assert!(
        uses("metrics_server", "render_fencing(") > 0,
        "the counter must see a real call"
    );
    // Negative: a token that appears nowhere.
    assert_eq!(uses("command_bus", "NoSuchSymbolAnywhereXYZ("), 0);
}

#[test]
fn a_comment_naming_a_symbol_does_not_count_as_a_use() {
    // ⚠ This is the trap that cleared an earlier check on this platform.
    let src = "// FencedSharedBackend::new is mentioned here\n/// and here too\nfn x() {}\n";
    assert_eq!(
        production_lines(src)
            .filter(|l| l.contains("FencedSharedBackend::new"))
            .count(),
        0,
        "comments must not count as callers"
    );
}

#[test]
fn test_modules_do_not_count_as_production_use() {
    // ⚠ The self-reference trap: a use inside #[cfg(test)] is not wiring, and a
    // guard that counted it would pass on a feature only its own tests call.
    let src = "fn prod() {}\n#[cfg(test)]\nmod t { fn u() { FencedSharedBackend::new(); } }\n";
    assert_eq!(
        production_lines(src)
            .filter(|l| l.contains("FencedSharedBackend::new"))
            .count(),
        0,
        "a call from a test module is not production wiring"
    );
    // And the same source WITH a production call is counted.
    let wired = format!("fn prod() {{ FencedSharedBackend::new(); }}\n{src}");
    assert_eq!(
        production_lines(&wired)
            .filter(|l| l.contains("FencedSharedBackend::new"))
            .count(),
        1
    );
}

#[test]
fn the_registry_covers_every_flagged_feature() {
    // Cheap completeness check: if someone adds a flagged feature and forgets to
    // register it here, the count below drifts and the omission is visible in
    // review rather than silent.
    assert_eq!(
        EXPECTATIONS.len(),
        7,
        "add the new feature to EXPECTATIONS and bump this count — an \
         unregistered feature is exactly what this guard exists to catch"
    );
}
