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
