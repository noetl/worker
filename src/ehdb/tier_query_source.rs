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
//! `NOETL_EHDB_TIER_SERVICE_ADDR` (PR 2). Silently answering from a different
//! store than the operator asked for is the failure mode this whole effort
//! exists to remove, so the two ways of not having a service are kept apart:
//!
//! | state | behaviour |
//! | :-- | :-- |
//! | no address at all | fall back to `local`, **with a WARN** — this is the mid-rollout case (new binary, old config) and a read path must not start erroring during a deploy |
//! | an address that cannot be used | **fail loud** (503), never fall back. The operator made a positive statement about which store answers; honouring a typo by reading a different one is invisible in the reply |
//!
//! # Which store answered is in the reply
//!
//! Every reply from the tier query route carries `tier_query_source`
//! (`local` / `downgraded_local` / `service`), and `tier_service_addr` when
//! there is one. Without it the two arms of this flag are indistinguishable from
//! outside the process — the bodies have the same shape — so no gate could tell
//! a working service path from a silent fall-back to local. That is precisely
//! what has to be provable before an event-log flip, because with more than one
//! worker replica the local answer is a *fragment* and reads as a full one.

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

/// What a caller should actually do, with the client already built.
///
/// One value rather than a `(source, downgraded)` pair plus a separate
/// `TierClient::from_env()` at each call site. The pair shape had a hole: a call
/// site that got `Service` and then failed to build a client had nothing to do
/// but fall through to the local read — silently, because the fall-through was
/// the next statement rather than a case. Making "asked for service, cannot have
/// it" a *variant* means a caller must handle it, and the compiler says so.
#[derive(Debug)]
pub enum Resolution {
    /// Read this pod's own store. The default, and the pre-PR-4 behaviour.
    Local,
    /// `service` was asked for and no address is configured at all. Falls back
    /// to local, deliberately: the unconfigured case includes "an older config
    /// on a newer binary mid-rollout", and a read path must not start erroring
    /// during a deploy. Reported, never silent.
    DowngradedToLocal,
    /// Read the writer-fronted tier service.
    Service(super::tier_client::TierClient),
    /// `service` was asked for, an address **is** set, and it cannot be used.
    ///
    /// This one fails loud and does **not** fall back. An operator who set an
    /// address made a positive statement about which store answers; answering
    /// from a different one because their address had a typo is the exact
    /// failure mode this whole effort exists to remove, and it is invisible in
    /// the reply. A 503 is recoverable in a way a wrong answer is not.
    Misconfigured(String),
}

impl Resolution {
    /// Label for the reply body and the metric. A closed set of four.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::DowngradedToLocal => "downgraded_local",
            Self::Service(_) => "service",
            Self::Misconfigured(_) => "misconfigured",
        }
    }

    /// The service address, when there is one to name.
    pub fn addr(&self) -> Option<&str> {
        match self {
            Self::Service(c) => Some(c.addr()),
            _ => None,
        }
    }
}

/// Resolve what a tier read or append should do, from the environment.
///
/// The client is built here rather than by the caller so that "requested
/// service" and "has a usable service client" cannot come apart.
pub fn resolve(env: &EnvMap) -> Resolution {
    if TierQuerySource::from_env(env) == TierQuerySource::Local {
        return Resolution::Local;
    }
    let raw = env
        .get(super::tier_client::TIER_SERVICE_ADDR_ENV)
        .map(|s| s.trim())
        .unwrap_or("");
    if raw.is_empty() {
        return Resolution::DowngradedToLocal;
    }
    match super::tier_client::TierClient::from_map(env) {
        Some(c) => Resolution::Service(c),
        // `TierClientConfig::from_env` already WARNed with the offending value;
        // carry the reason so the HTTP reply names it too. A log line the caller
        // cannot see is not an answer to "why did this 503".
        None => Resolution::Misconfigured(format!(
            "{}={raw} is not a usable host:port",
            super::tier_client::TIER_SERVICE_ADDR_ENV
        )),
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

    const ADDR: &str = super::super::tier_client::TIER_SERVICE_ADDR_ENV;

    #[test]
    fn service_without_an_address_downgrades_visibly() {
        // The property that matters: asking for `service` with no address must
        // NOT silently answer from a different store.
        assert!(matches!(
            resolve(&env(&[(TIER_QUERY_SOURCE_ENV, "service")])),
            Resolution::DowngradedToLocal
        ));
        assert_eq!(
            resolve(&env(&[(TIER_QUERY_SOURCE_ENV, "service"), (ADDR, "  ")])).label(),
            "downgraded_local",
            "whitespace is not an address"
        );
    }

    #[test]
    fn service_with_an_address_resolves_to_the_service() {
        let r = resolve(&env(&[
            (TIER_QUERY_SOURCE_ENV, "service"),
            (ADDR, "noetl-cmdbus-writer-0.noetl.svc.cluster.local:9110"),
        ]));
        assert_eq!(r.label(), "service");
        assert_eq!(
            r.addr(),
            Some("noetl-cmdbus-writer-0.noetl.svc.cluster.local:9110"),
            "a DNS name must survive resolution — an earlier build parsed this \
             as a SocketAddr and silently fell back to local for every \
             Kubernetes name"
        );
    }

    #[test]
    fn a_bad_address_fails_loud_rather_than_reading_the_wrong_store() {
        // THE regression this variant exists for. `service` + an address that
        // cannot be dialled must NOT resolve to `Local`: the pod-local store
        // would answer, the reply would look entirely normal, and the operator
        // would be reading a different store than the one they configured.
        for bad in ["nonsense", "host:", ":9110", "host:0", "host:notaport"] {
            let r = resolve(&env(&[(TIER_QUERY_SOURCE_ENV, "service"), (ADDR, bad)]));
            assert_eq!(
                r.label(),
                "misconfigured",
                "{bad} must fail loud, got {r:?}"
            );
            assert!(
                matches!(&r, Resolution::Misconfigured(m) if m.contains(bad)),
                "the reason must name the offending value: {r:?}"
            );
        }
    }

    #[test]
    fn local_is_never_reported_as_downgraded() {
        assert_eq!(resolve(&env(&[])).label(), "local");
        // An address set while the source is `local` is not a request for the
        // service — the writer's own pods carry the address for the mirror.
        assert_eq!(resolve(&env(&[(ADDR, "127.0.0.1:9110")])).label(), "local");
    }
}
