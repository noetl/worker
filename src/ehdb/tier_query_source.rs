//! Where a tier query resolves from: this pod's local store, or the
//! writer-fronted tier service.
//!
//! **PR 4 of [ai-meta#257](https://github.com/noetl/ai-meta/issues/257).**
//!
//! # The gap this closes
//!
//! The read chain already existed end to end:
//!
//! ```text
//! server /api/ehdb/tiers/{tier}
//!   -> TierRelayState (NOETL_EHDB_WORKER_QUERY_URL)
//!     -> worker /ehdb/tiers/{tier}      (metrics_server.rs)
//!       -> run_query(&process_env, ..)  <- resolves POD-LOCAL
//! ```
//!
//! Only the last hop was wrong. Each worker answered from *its own* store, so a
//! read landed on whichever replica the request happened to reach and saw a
//! fragment of the tier. This module lets that hop resolve against the writer's
//! durable store instead — the one place the data actually accumulates.
//!
//! **No server change is involved.** The server keeps its control-plane guard and
//! still never opens tier storage; it relays, exactly as before.
//!
//! # Default-off
//!
//! `NOETL_EHDB_TIER_QUERY_SOURCE` defaults to `local`, which is byte-identical to
//! the behaviour before this module existed. `service` additionally requires
//! `NOETL_EHDB_TIER_SERVICE_ADDR` (PR 2); without it the source falls back to
//! `local` **with a WARN**, because silently answering from a different store than
//! the operator asked for is the failure mode this whole effort exists to remove.

use super::EnvMap;

/// Env var selecting the query source.
pub const TIER_QUERY_SOURCE_ENV: &str = "NOETL_EHDB_TIER_QUERY_SOURCE";

/// Where a tier read resolves from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TierQuerySource {
    /// This pod's own store (the pre-PR-4 behaviour, and the default).
    Local,
    /// The writer-fronted tier service.
    Service,
}

impl TierQuerySource {
    /// Resolve from an env map.
    ///
    /// Anything unrecognised is `Local`. Fail-safe rather than fail-loud is the
    /// right call here specifically because the unrecognised case includes
    /// "an older config on a newer binary during a rollout", and a read path
    /// must not start erroring mid-deploy.
    pub fn from_env(env: &EnvMap) -> Self {
        match env
            .get(TIER_QUERY_SOURCE_ENV)
            .map(|s| s.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("service") => Self::Service,
            _ => Self::Local,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Service => "service",
        }
    }
}

/// Resolve the *effective* source, downgrading `service` to `local` when no
/// service address is configured.
///
/// Returns `(effective, downgraded)`. The caller records the downgrade rather
/// than this function logging it, so a per-request path does not emit a per-request
/// WARN.
pub fn effective_source(env: &EnvMap) -> (TierQuerySource, bool) {
    let requested = TierQuerySource::from_env(env);
    if requested == TierQuerySource::Service
        && env
            .get(super::tier_client::TIER_SERVICE_ADDR_ENV)
            .map(|s| s.trim().is_empty())
            .unwrap_or(true)
    {
        return (TierQuerySource::Local, true);
    }
    (requested, false)
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
    fn default_is_local_and_unrecognised_is_local() {
        assert_eq!(TierQuerySource::from_env(&env(&[])), TierQuerySource::Local);
        assert_eq!(
            TierQuerySource::from_env(&env(&[(TIER_QUERY_SOURCE_ENV, "")])),
            TierQuerySource::Local
        );
        // An older config on a newer binary must not start erroring mid-rollout.
        assert_eq!(
            TierQuerySource::from_env(&env(&[(TIER_QUERY_SOURCE_ENV, "bogus")])),
            TierQuerySource::Local
        );
        assert_eq!(
            TierQuerySource::from_env(&env(&[(TIER_QUERY_SOURCE_ENV, "SERVICE")])),
            TierQuerySource::Service,
            "case-insensitive"
        );
    }

    #[test]
    fn service_without_an_address_downgrades_visibly() {
        // The property that matters: asking for `service` with no address must
        // NOT silently answer from a different store.
        let (src, downgraded) = effective_source(&env(&[(TIER_QUERY_SOURCE_ENV, "service")]));
        assert_eq!(src, TierQuerySource::Local);
        assert!(downgraded, "the downgrade must be reported, not hidden");

        let (src, downgraded) = effective_source(&env(&[
            (TIER_QUERY_SOURCE_ENV, "service"),
            (super::super::tier_client::TIER_SERVICE_ADDR_ENV, "127.0.0.1:9110"),
        ]));
        assert_eq!(src, TierQuerySource::Service);
        assert!(!downgraded);
    }

    #[test]
    fn local_is_never_reported_as_downgraded() {
        let (src, downgraded) = effective_source(&env(&[]));
        assert_eq!(src, TierQuerySource::Local);
        assert!(!downgraded, "plain local is not a downgrade");
    }
}
