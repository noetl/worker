//! Which component mirrors events into the event-log tier.
//!
//! The tier's mirror hook has always sat on the **worker's** emit chokepoint
//! (`ControlPlaneClient::emit_event`). That placement bounds what the tier can
//! ever hold: the server authors events itself — `playbook_started`,
//! `command.issued`, `step.enter`, `playbook.completed`, and `command.claimed`
//! inside the claim transaction — and none of those pass through a worker. The
//! server's cross-store comparator measured the consequence in kind: **6 of 13**
//! events per execution reach the tier, and the other 7 are reported as
//! `unmirrored_by_design` (noetl/ai-meta#258).
//!
//! That is a structural bar on promoting the event-log tier to `primary`: a tier
//! holding half the log cannot serve the log (noetl/ai-meta#257 §3.4).
//!
//! The closure is to move the mirror to the **server's** write chokepoint
//! (`handlers::event_write::emit_events`), which every authoritative event
//! passes through — the worker's own events included, because a worker event
//! reaches `noetl.event` only by way of `POST /api/events`. This enum is the
//! switch, and both components read the same variable so they cannot disagree
//! about who is mirroring.
//!
//! | value | worker mirrors | server mirrors | tier holds |
//! | :-- | :-- | :-- | :-- |
//! | `worker` (default) | yes | no | the worker-emitted subset |
//! | `server` | **no** | yes | the whole authoritative set |
//!
//! The worker half of `server` mode is a **disarm**, and it is not optional:
//! with both halves mirroring, every worker-emitted event is appended twice and
//! the comparator reports a count divergence. That failure is loud by
//! construction, which is the direction to be wrong in — a silent double-append
//! would inflate the tier and still read as agreement on membership.

use super::EnvMap;

/// `NOETL_EHDB_EVENTLOG_MIRROR_SOURCE` — `worker` (default) or `server`.
pub const MIRROR_SOURCE_ENV: &str = "NOETL_EHDB_EVENTLOG_MIRROR_SOURCE";

/// `NOETL_EHDB_PROJECTION_MIRROR_SOURCE` — the projection tier's twin
/// ([ai-meta#265](https://github.com/noetl/ai-meta/issues/265)).
///
/// **A separate variable, not a shared one.** The two tiers cut over
/// independently — the event log is already primary in prod and the projection
/// tier is not — so one variable would make arming the projection mirror a
/// change to the event log's configuration. That is the class of coupling that
/// turns a tier-2 experiment into a tier-1 incident.
///
/// The projection tier has no `worker` mode that means anything: the worker
/// cannot read `noetl.projection_snapshot` (`data-access-boundary.md`), so it
/// has nothing to mirror. `worker` here means "the projection mirror is off",
/// and that is the default.
pub const PROJECTION_MIRROR_SOURCE_ENV: &str = "NOETL_EHDB_PROJECTION_MIRROR_SOURCE";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorSource {
    /// Today's behaviour: the worker's emit chokepoint mirrors what it emits.
    Worker,
    /// The server's write chokepoint mirrors the complete authoritative set;
    /// the worker's event-log mirror disarms.
    Server,
}

impl MirrorSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Server => "server",
        }
    }

    /// Resolve from an env map.
    ///
    /// Anything unrecognised — including an empty value — is `Worker`. The
    /// default has to be the pre-change behaviour: an operator who typos the
    /// value must get the old mirror, not no mirror. A typo that disarmed both
    /// halves would leave the tier silently empty while `NOETL_EHDB_EVENTLOG`
    /// still said `shadow`.
    pub fn from_env(env: &EnvMap) -> Self {
        Self::from_env_key(env, MIRROR_SOURCE_ENV)
    }

    /// Resolve the mirror source for one tier's own variable.
    ///
    /// [`StoreTier::Eventlog`] reads [`MIRROR_SOURCE_ENV`],
    /// [`StoreTier::Projection`] reads [`PROJECTION_MIRROR_SOURCE_ENV`]. The
    /// match is exhaustive rather than a fallback, so a tier added without a
    /// variable fails the build instead of silently inheriting the event log's.
    pub fn for_tier(env: &EnvMap, tier: super::store_tier::StoreTier) -> Self {
        use super::store_tier::StoreTier;
        let key = match tier {
            StoreTier::Eventlog => MIRROR_SOURCE_ENV,
            StoreTier::Projection => PROJECTION_MIRROR_SOURCE_ENV,
        };
        Self::from_env_key(env, key)
    }

    /// The variable `tier`'s mirror source is read from — for error messages, so
    /// an operator is told which variable to set rather than a generic one.
    pub fn env_key_for(tier: super::store_tier::StoreTier) -> &'static str {
        use super::store_tier::StoreTier;
        match tier {
            StoreTier::Eventlog => MIRROR_SOURCE_ENV,
            StoreTier::Projection => PROJECTION_MIRROR_SOURCE_ENV,
        }
    }

    fn from_env_key(env: &EnvMap, key: &str) -> Self {
        match env
            .get(key)
            .map(|s| s.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("server") => Self::Server,
            _ => Self::Worker,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> EnvMap {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn default_and_unrecognised_are_worker() {
        assert_eq!(MirrorSource::from_env(&env(&[])), MirrorSource::Worker);
        assert_eq!(
            MirrorSource::from_env(&env(&[(MIRROR_SOURCE_ENV, "")])),
            MirrorSource::Worker
        );
        assert_eq!(
            MirrorSource::from_env(&env(&[(MIRROR_SOURCE_ENV, "srever")])),
            MirrorSource::Worker
        );
    }

    #[test]
    fn server_is_recognised_case_and_space_insensitively() {
        for v in ["server", "SERVER", " Server "] {
            assert_eq!(
                MirrorSource::from_env(&env(&[(MIRROR_SOURCE_ENV, v)])),
                MirrorSource::Server,
                "{v:?} must resolve to Server"
            );
        }
    }

    #[test]
    fn the_two_tiers_have_independent_variables() {
        use super::super::store_tier::StoreTier;
        // The property that keeps a tier-2 experiment from being a tier-1
        // change: arming the projection mirror must not arm the event log's,
        // and vice versa.
        let proj_only = env(&[(PROJECTION_MIRROR_SOURCE_ENV, "server")]);
        assert_eq!(
            MirrorSource::for_tier(&proj_only, StoreTier::Projection),
            MirrorSource::Server
        );
        assert_eq!(
            MirrorSource::for_tier(&proj_only, StoreTier::Eventlog),
            MirrorSource::Worker,
            "arming the projection mirror must not arm the event log's"
        );

        let el_only = env(&[(MIRROR_SOURCE_ENV, "server")]);
        assert_eq!(
            MirrorSource::for_tier(&el_only, StoreTier::Eventlog),
            MirrorSource::Server
        );
        assert_eq!(
            MirrorSource::for_tier(&el_only, StoreTier::Projection),
            MirrorSource::Worker,
            "the event log's mirror source must not arm the projection tier — prod \
             sets it TODAY, so inheriting it would arm tier 2 on the next rollout"
        );
        assert_ne!(
            MirrorSource::env_key_for(StoreTier::Eventlog),
            MirrorSource::env_key_for(StoreTier::Projection)
        );
    }
}
