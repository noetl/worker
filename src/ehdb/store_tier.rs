//! Which tier a tier-service operation addresses.
//!
//! **[ai-meta#265](https://github.com/noetl/ai-meta/issues/265) A1.** Until now
//! the tier service spoke about exactly one tier and never said so: `append`,
//! `read_execution` and `scan` were event-log operations, the store was
//! `eventlog.jsonl`, and the wire frames carried no tier at all. That was
//! honest while the event log was the only tier with a durable store behind the
//! service ([#257](https://github.com/noetl/ai-meta/issues/257) PR 3 says so in
//! its own doc comment), and it stops being honest the moment a second tier
//! needs one.
//!
//! # Why an enum rather than a string
//!
//! The tier arrives from the wire, so it is caller-controlled. Two consequences,
//! and the enum is what closes both:
//!
//! * A caller-controlled string must never reach a **metric label** — that is
//!   unbounded cardinality, and it is the reason `TierRequest::Unsupported`
//!   already keeps the unknown op name out of `operation`. Parsing to a closed
//!   set at the boundary means every label downstream comes from
//!   [`StoreTier::as_str`], which has two possible values.
//! * A caller-controlled string must never reach a **path**. `dir.join(tier)`
//!   with `tier = "../../etc/passwd"` is a directory traversal on the one
//!   process that holds the durable PVCs. Parsing first means the filename
//!   comes from this file, not from the network.
//!
//! # Deliberately only two variants
//!
//! `kv`, `object` and `vector` are **not** here. They have tier drivers and
//! shadow paths, but nothing behind the tier service, and adding a variant for
//! a tier with no store would let a caller address something that answers
//! `unavailable` in a way indistinguishable from a misconfigured writer. A tier
//! gains a variant in the same change set that gives it a store — not before.
//!
//! This is the same discipline as
//! [`primary_serve::SERVE_WIRED_TIERS`][super::primary_serve::SERVE_WIRED_TIERS]:
//! the list says what is true, and a test re-derives it rather than a doc
//! comment asserting it.

/// A tier with a durable store behind the tier service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoreTier {
    /// The authoritative event log's mirror (`noetl.event`).
    Eventlog,
    /// The orchestrator read-model mirror (`noetl.projection_snapshot`).
    Projection,
}

impl StoreTier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Eventlog => "eventlog",
            Self::Projection => "projection",
        }
    }

    /// The store filename for this tier.
    ///
    /// `eventlog` keeps `eventlog.jsonl` **exactly**: a writer that rolls onto
    /// this build must find the log it already holds, and renaming it would
    /// present a populated store as an empty one — on the component that is
    /// serving primary in production.
    pub fn file_name(self) -> &'static str {
        match self {
            Self::Eventlog => "eventlog.jsonl",
            Self::Projection => "projection.jsonl",
        }
    }

    /// Parse a wire value. `None` for anything not in the closed set.
    ///
    /// Case- and space-insensitive because the value crosses an HTTP path
    /// segment and a JSON field on the way here, and rejecting `"Projection"`
    /// would be a refusal an operator has to debug rather than read.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "eventlog" => Some(Self::Eventlog),
            "projection" => Some(Self::Projection),
            _ => None,
        }
    }

    /// Parse a wire value, defaulting to [`StoreTier::Eventlog`] when the field
    /// is **absent**.
    ///
    /// The default is what keeps a pre-#265 client working against a #265
    /// writer: every frame it sends omits `tier`, and every one of them means
    /// the event log. A protocol that broke its own previous version during a
    /// rolling upgrade would be a self-inflicted outage on the tier that is
    /// already primary in prod.
    ///
    /// Note the asymmetry, which is intentional: **absent** defaults, but
    /// **present and unrecognised** does not. A typo'd tier must be refused, not
    /// silently written into the event log — that would be a wrong-tier append
    /// scored as a correct one.
    pub fn parse_or_default(raw: Option<&str>) -> Result<Self, String> {
        match raw.map(str::trim).filter(|s| !s.is_empty()) {
            None => Ok(Self::Eventlog),
            Some(v) => Self::parse(v).ok_or_else(|| {
                format!(
                    "unknown tier {:?} — the tier service stores {}",
                    v.chars().take(40).collect::<String>(),
                    Self::ALL
                        .iter()
                        .map(|t| t.as_str())
                        .collect::<Vec<_>>()
                        .join(" and ")
                )
            }),
        }
    }

    /// Every tier with a store. Used by the pin sites and by the tests that
    /// assert per-tier isolation, so adding a variant cannot leave one behind.
    pub const ALL: &'static [StoreTier] = &[StoreTier::Eventlog, StoreTier::Projection];
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_tier_is_the_event_log() {
        // The rolling-upgrade property: a pre-#265 frame carries no tier.
        assert_eq!(StoreTier::parse_or_default(None), Ok(StoreTier::Eventlog));
        assert_eq!(StoreTier::parse_or_default(Some("")), Ok(StoreTier::Eventlog));
        assert_eq!(
            StoreTier::parse_or_default(Some("   ")),
            Ok(StoreTier::Eventlog)
        );
    }

    #[test]
    fn a_typo_is_refused_rather_than_written_to_the_event_log() {
        // The asymmetry that matters: absent defaults, wrong does not. Silently
        // treating `projction` as `eventlog` would append projection records
        // into the log that is primary in prod.
        let err = StoreTier::parse_or_default(Some("projction")).unwrap_err();
        assert!(err.contains("projction"), "the error must name the value: {err}");
        assert!(
            err.contains("eventlog") && err.contains("projection"),
            "the error must name what IS accepted: {err}"
        );
        for bad in ["kv", "object", "vector", "../../etc/passwd", "eventlog "] {
            if bad.trim() == "eventlog" {
                continue;
            }
            assert!(
                StoreTier::parse(bad).is_none(),
                "{bad:?} must not parse — it has no store behind the tier service"
            );
        }
    }

    #[test]
    fn case_and_space_are_tolerated() {
        for v in ["projection", "PROJECTION", " Projection "] {
            assert_eq!(StoreTier::parse(v), Some(StoreTier::Projection), "{v:?}");
        }
    }

    #[test]
    fn file_names_are_distinct_and_the_event_log_keeps_its_name() {
        // A rename here would present the writer's populated, primary-serving
        // event-log store as empty.
        assert_eq!(StoreTier::Eventlog.file_name(), "eventlog.jsonl");
        let mut names: Vec<&str> = StoreTier::ALL.iter().map(|t| t.file_name()).collect();
        names.sort_unstable();
        let n = names.len();
        names.dedup();
        assert_eq!(names.len(), n, "two tiers share a store file: {names:?}");
    }

    #[test]
    fn file_names_carry_no_path_separator() {
        // The traversal guard: these strings are joined onto the writer's PVC
        // directory, so they must be bare filenames whatever the wire said.
        for t in StoreTier::ALL {
            let n = t.file_name();
            assert!(
                !n.contains('/') && !n.contains('\\') && !n.contains(".."),
                "{n:?} is not a bare filename"
            );
        }
    }

    #[test]
    fn as_str_round_trips_through_parse() {
        for t in StoreTier::ALL {
            assert_eq!(StoreTier::parse(t.as_str()), Some(*t));
        }
    }
}
