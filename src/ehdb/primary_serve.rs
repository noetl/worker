//! Primary serve policy: what a tier is allowed to serve, and what happens when
//! it disagrees with the incumbent.
//!
//! **PR 5 of [ai-meta#257](https://github.com/noetl/ai-meta/issues/257)** — the
//! first slice where a tier could serve authoritatively. Because of that, this
//! module is deliberately a *decision function* with no I/O: the policy that
//! decides "serve EHDB's answer / fall back to the incumbent / refuse" is the
//! part that must be exhaustively testable, and mixing it with sockets and
//! drivers would make it testable only through them.
//!
//! # The safety property
//!
//! **Divergence demotes; it never serves a wrong answer and never fails the
//! caller.** When EHDB and the incumbent disagree, the incumbent's answer is
//! returned and the tier is marked degraded. Three reasons, in order:
//!
//! 1. The incumbent is the store with history. A divergence means EHDB is the
//!    one we cannot trust yet.
//! 2. Failing the caller would convert a *verification* problem into an
//!    *availability* problem — the tier is being trialled, and a trial must not
//!    be able to take the platform down.
//! 3. It preserves the rollback story: primary only ever **appends**, so
//!    demoting loses nothing.
//!
//! # Why this is not just `if primary { serve }`
//!
//! Serving requires **three** conditions, not one: the tier is `primary`, a
//! durable service is actually reachable, and parity held on this operation.
//! Any of them missing means the incumbent answers. Collapsing that into the
//! flag alone is precisely how [worker#251](https://github.com/noetl/worker/pull/251)
//! shipped a `primary` that turned verification off — the flag was treated as
//! sufficient when it was only necessary.

/// Which store's answer the caller receives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServeDecision {
    /// EHDB served it authoritatively; parity held.
    ServedByEhdb,
    /// The incumbent answered. EHDB may still have mirrored — verification is
    /// unaffected by this decision.
    ServedByIncumbent { reason: DemoteReason },
}

/// Why the incumbent answered instead of EHDB. Distinct variants because they
/// call for different operator responses: a divergence is a correctness signal,
/// an unreachable service is an infrastructure one, and "not primary" is normal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DemoteReason {
    /// The tier is not in `primary` mode. The ordinary case.
    NotPrimary,
    /// `primary` was requested but no durable tier service is reachable, so
    /// there is nothing authoritative to serve FROM.
    NoDurableService,
    /// EHDB answered but disagreed with the incumbent.
    ParityDiverged,
}

impl DemoteReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NotPrimary => "not_primary",
            Self::NoDurableService => "no_durable_service",
            Self::ParityDiverged => "parity_diverged",
        }
    }

    /// Whether this reason should mark the tier degraded.
    ///
    /// `NotPrimary` is not degraded — it is the default state of every tier and
    /// flagging it would make "degraded" mean nothing. The other two are.
    pub fn is_degraded(&self) -> bool {
        !matches!(self, Self::NotPrimary)
    }
}

/// Decide who answers.
///
/// * `is_primary` — the tier's configured mode is `primary`.
/// * `durable_service_reachable` — a writer-fronted store is available. Without
///   it, `primary` has no single authoritative store to serve from; serving a
///   pod-local fragment while claiming to be authoritative is worse than not
///   serving at all.
/// * `parity_held` — EHDB and the incumbent agreed on this operation.
pub fn decide(
    is_primary: bool,
    durable_service_reachable: bool,
    parity_held: bool,
) -> ServeDecision {
    if !is_primary {
        return ServeDecision::ServedByIncumbent {
            reason: DemoteReason::NotPrimary,
        };
    }
    if !durable_service_reachable {
        return ServeDecision::ServedByIncumbent {
            reason: DemoteReason::NoDurableService,
        };
    }
    if !parity_held {
        return ServeDecision::ServedByIncumbent {
            reason: DemoteReason::ParityDiverged,
        };
    }
    ServeDecision::ServedByEhdb
}

/// The tiers whose `primary` mode is wired to a runtime serve decision — i.e.
/// the tiers where a live worker path calls [`decide`] and can therefore answer
/// authoritatively.
///
/// **Today that is the event-log tier and the projection tier.**  `kv`,
/// `object` and `vector` have a `serve_primary_cycle`, but its only caller is
/// `bin/ehdb-selfcheck` — a conformance drive that never authors a NoETL event —
/// so selecting `primary` on those tiers cannot change what any caller receives.
///
/// The projection tier joined in
/// [ai-meta#265](https://github.com/noetl/ai-meta/issues/265) A2, and what it
/// means there is narrower than for the event log, so it is worth stating
/// rather than leaving to be inferred from membership: the projection tier's
/// **write** path consults [`decide`] and reports the verdict, and no reader
/// resolves projections from EHDB yet — `orch_snapshot::load_latest` still
/// answers from `noetl.projection_snapshot`. Read-serve is #265 phase B1.
/// Membership here means "the flag is not inert", which is exactly the question
/// #259 was about; it does not mean "the incumbent has been replaced".
///
/// This list exists because the *previous* way of knowing which tiers were
/// wired was a sentence in a doc comment, and it went stale the moment PR 5
/// wired the event log: the flip-time warning kept telling operators the tier
/// was inert on a pod that was serving 48 ops as primary
/// ([ai-meta#259](https://github.com/noetl/ai-meta/issues/259)).  The guard test
/// below re-derives the list from the tier sources on every `cargo test`, so a
/// tier that gains (or loses) a serve path and is not listed here fails the
/// build rather than quietly mis-signalling during a cutover.
pub const SERVE_WIRED_TIERS: &[&str] = &["eventlog", "projection"];

/// Whether `tier` has a runtime serve path — see [`SERVE_WIRED_TIERS`].
pub fn tier_serves_primary(tier: &str) -> bool {
    SERVE_WIRED_TIERS.contains(&tier)
}

impl ServeDecision {
    pub fn served_by_ehdb(&self) -> bool {
        matches!(self, Self::ServedByEhdb)
    }
    pub fn degraded(&self) -> bool {
        match self {
            Self::ServedByEhdb => false,
            Self::ServedByIncumbent { reason } => reason.is_degraded(),
        }
    }
    pub fn outcome_label(&self) -> &'static str {
        match self {
            Self::ServedByEhdb => "served_primary",
            Self::ServedByIncumbent { reason } => reason.as_str(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every tier module, paired with its source text at compile time.
    ///
    /// `include_str!` resolves relative to this file, so these are the real
    /// sibling modules — not a copy that can drift.
    const TIER_SOURCES: &[(&str, &str)] = &[
        ("eventlog", include_str!("eventlog.rs")),
        ("kv", include_str!("kv.rs")),
        ("object", include_str!("object.rs")),
        ("projection", include_str!("projection.rs")),
        ("vector", include_str!("vector.rs")),
    ];

    /// The guard for [ai-meta#259](https://github.com/noetl/ai-meta/issues/259):
    /// [`SERVE_WIRED_TIERS`] must equal the set of tiers that actually call
    /// [`decide`].
    ///
    /// The #259 defect was a *description* that stopped tracking the code — the
    /// flip-time warning claimed no serve path existed on a build that served.
    /// A doc comment cannot be wrong loudly; this can.  Matching on
    /// `primary_serve::decide(` (with the paren) means a doc comment naming the
    /// function does not count as a caller — comments passing for callers is one
    /// of the ways the earlier reachability scans returned confident zeros.
    #[test]
    fn serve_wired_tiers_matches_the_tiers_that_call_decide() {
        // Positive control: the match string must find SOMETHING, otherwise a
        // renamed call form would make every tier read as unwired and the test
        // would pass by finding nothing.
        let wired_in_source: Vec<&str> = TIER_SOURCES
            .iter()
            .filter(|(_, src)| src.contains("primary_serve::decide("))
            .map(|(tier, _)| *tier)
            .collect();
        assert!(
            !wired_in_source.is_empty(),
            "no tier module calls `primary_serve::decide(` — either the serve \
             path was removed or this test's match string is stale; a zero here \
             is not evidence"
        );
        let mut declared: Vec<&str> = SERVE_WIRED_TIERS.to_vec();
        declared.sort_unstable();
        let mut found = wired_in_source;
        found.sort_unstable();
        assert_eq!(
            declared, found,
            "SERVE_WIRED_TIERS disagrees with the tier sources.  A tier that \
             gained a serve path must be added (else its flip-time signal tells \
             operators it is inert while it serves); a tier that lost one must \
             be removed."
        );
    }

    #[test]
    fn unwired_tiers_do_not_claim_a_serve_path() {
        for tier in ["kv", "object", "vector"] {
            assert!(
                !tier_serves_primary(tier),
                "{tier} is listed as serve-wired; if that is now true, update \
                 the cutover order in the RFC and the operator runbook too"
            );
        }
        assert!(tier_serves_primary("eventlog"));
        assert!(tier_serves_primary("projection"));
        assert!(!tier_serves_primary("nonexistent"));
    }

    /// Exhaustive over all 8 input combinations. A policy this consequential
    /// should not have an untested corner, and there are only eight.
    #[test]
    fn decision_table_is_exhaustive_and_conservative() {
        let cases = [
            // (primary, reachable, parity, expected_serves_ehdb, expected_degraded)
            (false, false, false, false, false),
            (false, false, true, false, false),
            (false, true, false, false, false),
            (false, true, true, false, false),
            (true, false, false, false, true),
            (true, false, true, false, true),
            (true, true, false, false, true),
            (true, true, true, true, false),
        ];
        for (p, r, par, serves, degraded) in cases {
            let d = decide(p, r, par);
            assert_eq!(
                d.served_by_ehdb(),
                serves,
                "decide({p},{r},{par}) serve mismatch: {d:?}"
            );
            assert_eq!(
                d.degraded(),
                degraded,
                "decide({p},{r},{par}) degraded mismatch: {d:?}"
            );
        }
    }

    #[test]
    fn ehdb_serves_only_when_all_three_hold() {
        // Exactly ONE of eight combinations serves.  The flag alone is
        // necessary, never sufficient — the mistake worker#251 fixed.
        let serving: Vec<_> = [false, true]
            .iter()
            .flat_map(|&p| [false, true].iter().map(move |&r| (p, r)))
            .flat_map(|(p, r)| [false, true].iter().map(move |&par| (p, r, par)))
            .filter(|&(p, r, par)| decide(p, r, par).served_by_ehdb())
            .collect();
        assert_eq!(serving, vec![(true, true, true)]);
    }

    #[test]
    fn divergence_demotes_rather_than_failing_or_serving_wrong_data() {
        let d = decide(true, true, false);
        assert!(!d.served_by_ehdb(), "a diverged tier must NOT serve its answer");
        assert!(d.degraded(), "divergence must mark the tier degraded");
        assert_eq!(
            d,
            ServeDecision::ServedByIncumbent {
                reason: DemoteReason::ParityDiverged
            },
            "the incumbent answers — divergence is never an error to the caller"
        );
    }

    #[test]
    fn primary_without_a_durable_service_does_not_serve_a_local_fragment() {
        // The failure this whole RFC exists to prevent: a tier claiming to be
        // authoritative while answering from one pod's fragment.
        let d = decide(true, false, true);
        assert!(!d.served_by_ehdb());
        assert_eq!(
            d,
            ServeDecision::ServedByIncumbent {
                reason: DemoteReason::NoDurableService
            }
        );
        assert!(d.degraded(), "asking for primary and not getting it is degraded");
    }

    #[test]
    fn not_primary_is_not_degraded() {
        // Every tier is non-primary by default; if that counted as degraded the
        // signal would be meaningless.
        for (r, par) in [(false, false), (false, true), (true, false), (true, true)] {
            let d = decide(false, r, par);
            assert!(!d.degraded(), "non-primary must not be degraded: {d:?}");
            assert_eq!(d.outcome_label(), "not_primary");
        }
    }
}
