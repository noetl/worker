//! Client for the writer-fronted EHDB tier service.
//!
//! **PR 2 of [ai-meta#257](https://github.com/noetl/ai-meta/issues/257).**
//! Speaks the length-framed protocol that [`super::tier_service`] serves, and
//! reuses that module's frame codec rather than reimplementing it — two
//! implementations of one wire format is how a protocol drifts apart.
//!
//! # What this PR deliberately does NOT do
//!
//! It does **not** redirect the shadow mirror at the service. The RFC groups
//! "client" and "mirror can target it" together, but the service has no storage
//! until PR 3: pointing live mirror traffic at it now would send verification
//! writes somewhere that cannot keep them. The client is therefore usable and
//! tested, and the only thing wired to it is an opt-in **connectivity probe**
//! that proves reachability without moving any tier data.
//!
//! # Inertness
//!
//! With `NOETL_EHDB_TIER_SERVICE_ADDR` unset, [`TierClientConfig::from_env`]
//! yields `None`, nothing connects, and no metric family gains a child. Exactly
//! as in PR 1, there is **no default address** — a client that dials a guessed
//! host because a variable was forgotten is worse than one that does nothing.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;

use super::tier_service::{read_frame, write_frame, PROTOCOL_VERSION};

/// Env var naming the tier service to talk to. Unset ⇒ no client.
pub const TIER_SERVICE_ADDR_ENV: &str = "NOETL_EHDB_TIER_SERVICE_ADDR";

/// Env var overriding the connect/request timeout, in milliseconds.
pub const TIER_SERVICE_TIMEOUT_MS_ENV: &str = "NOETL_EHDB_TIER_SERVICE_TIMEOUT_MS";

/// Default timeout. Short on purpose: this is an auxiliary verification path,
/// and it must never become a latency contributor on the caller's hot path.
pub const DEFAULT_TIMEOUT_MS: u64 = 2_000;

/// Resolved client configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TierClientConfig {
    pub addr: SocketAddr,
    pub timeout: Duration,
}

impl TierClientConfig {
    /// Resolve from the process environment. `None` ⇒ no client is configured.
    ///
    /// An unparseable address fails closed with a WARN, matching the service
    /// side: a typo must leave the client absent and say so, never dial a
    /// default and never panic a process that hosts the buses.
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var(TIER_SERVICE_ADDR_ENV).ok()?;
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        let addr = match raw.parse::<SocketAddr>() {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(
                    var = TIER_SERVICE_ADDR_ENV,
                    error = %e,
                    "EHDB tier service address is unparseable; no tier client will be created"
                );
                return None;
            }
        };
        // An unparseable timeout falls back to the default rather than
        // disabling the client: the address is the load-bearing setting, and a
        // bad timeout should not silently remove a configured capability.
        let timeout_ms = std::env::var(TIER_SERVICE_TIMEOUT_MS_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_TIMEOUT_MS);
        Some(Self {
            addr,
            timeout: Duration::from_millis(timeout_ms),
        })
    }
}

/// What a probe found. Distinguishing these matters: "unreachable" and "spoke a
/// protocol I do not understand" need different operator responses, and folding
/// both into a bare failure is how a version mismatch gets misdiagnosed as a
/// network problem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TierProbe {
    /// Connected, and the peer answered a well-formed health reply.
    Healthy { version: u16 },
    /// Connected and got a reply that is not a health reply.
    Unexpected(String),
    /// Could not connect, or the exchange timed out.
    Unreachable(String),
}

/// A client for one tier service endpoint.
#[derive(Debug, Clone, Copy)]
pub struct TierClient {
    cfg: TierClientConfig,
}

impl TierClient {
    pub fn new(cfg: TierClientConfig) -> Self {
        Self { cfg }
    }

    /// Build from the environment. `None` ⇒ not configured.
    pub fn from_env() -> Option<Self> {
        TierClientConfig::from_env().map(Self::new)
    }

    pub fn addr(&self) -> SocketAddr {
        self.cfg.addr
    }

    /// Send one request frame and read one reply frame.
    ///
    /// Every step is bounded by the configured timeout — connect, write, and
    /// read alike. An unbounded read here would let a wedged writer hold a
    /// caller forever, which is precisely the shape of stall this platform has
    /// been bitten by before.
    pub async fn request(&self, payload: &[u8]) -> Result<Vec<u8>, String> {
        let fut = async {
            let mut s = TcpStream::connect(self.cfg.addr)
                .await
                .map_err(|e| format!("connect: {e}"))?;
            write_frame(&mut s, payload)
                .await
                .map_err(|e| format!("write: {e}"))?;
            match read_frame(&mut s).await {
                Ok(Some(v)) => Ok(v),
                Ok(None) => Err("peer closed without a reply".to_string()),
                Err(e) => Err(format!("read: {e}")),
            }
        };
        match tokio::time::timeout(self.cfg.timeout, fut).await {
            Ok(r) => r,
            Err(_) => Err(format!("timed out after {:?}", self.cfg.timeout)),
        }
    }

    /// Append one record to the remote event-log tier.
    pub async fn append(&self, execution_id: &str, payload: &str) -> Result<String, String> {
        let req = serde_json::json!({"op":"append","execution_id":execution_id,"payload":payload});
        let body = self.request(req.to_string().as_bytes()).await?;
        Ok(String::from_utf8_lossy(&body).to_string())
    }

    /// Read every record the remote tier holds for one execution.
    pub async fn read_execution(&self, execution_id: &str) -> Result<String, String> {
        let req = serde_json::json!({"op":"read_execution","execution_id":execution_id});
        let body = self.request(req.to_string().as_bytes()).await?;
        Ok(String::from_utf8_lossy(&body).to_string())
    }

    /// Bounded global scan of the remote tier.
    pub async fn scan(&self, after: Option<u64>, limit: usize) -> Result<String, String> {
        let req = serde_json::json!({"op":"scan","after":after,"limit":limit});
        let body = self.request(req.to_string().as_bytes()).await?;
        Ok(String::from_utf8_lossy(&body).to_string())
    }

    /// Probe the endpoint's health. Never returns an error — the classification
    /// *is* the result, and a caller deciding what to log should not also have
    /// to unwrap.
    pub async fn probe(&self) -> TierProbe {
        match self.request(b"health").await {
            Err(e) => TierProbe::Unreachable(e),
            Ok(body) => {
                let text = String::from_utf8_lossy(&body).to_string();
                match parse_health(&text) {
                    Some(version) => TierProbe::Healthy { version },
                    None => TierProbe::Unexpected(text),
                }
            }
        }
    }
}

/// Parse `ok tier-service v<N>` into its version.
pub fn parse_health(text: &str) -> Option<u16> {
    let rest = text.trim().strip_prefix("ok tier-service v")?;
    rest.parse::<u16>().ok()
}

/// Probe once at startup when a client is configured, and record what was found.
///
/// Reachability is checked **once, at startup, off the hot path**. It is a
/// deployment question ("did I point this at the right host?"), not a
/// per-request one, and polling it would add load to a single-replica writer for
/// no new information.
pub async fn probe_at_startup() {
    let Some(client) = TierClient::from_env() else {
        return; // not configured — strict no-op
    };
    let addr = client.addr();
    match client.probe().await {
        TierProbe::Healthy { version } => {
            super::metrics::record_tier_client("probe", "healthy", true, false, 0.0);
            if version == PROTOCOL_VERSION {
                tracing::info!(%addr, version, "EHDB tier service reachable");
            } else {
                // Not an error yet — nothing depends on the service in this PR —
                // but a version skew must be visible BEFORE a later PR starts
                // relying on it.
                super::metrics::record_tier_client("probe", "version_skew", true, true, 0.0);
                tracing::warn!(
                    %addr, peer_version = version, local_version = PROTOCOL_VERSION,
                    "EHDB tier service speaks a different protocol version"
                );
            }
        }
        TierProbe::Unexpected(body) => {
            super::metrics::record_tier_client("probe", "unexpected_reply", false, true, 0.0);
            tracing::warn!(%addr, reply = %body, "EHDB tier service gave an unexpected health reply");
        }
        TierProbe::Unreachable(err) => {
            super::metrics::record_tier_client("probe", "unreachable", false, true, 0.0);
            tracing::warn!(%addr, error = %err, "EHDB tier service is not reachable");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ehdb::tier_service::serve_tier;
    use tokio::net::TcpListener;

    #[test]
    fn parses_a_health_reply() {
        assert_eq!(parse_health("ok tier-service v1"), Some(1));
        assert_eq!(parse_health("  ok tier-service v7  "), Some(7));
        assert_eq!(parse_health("unsupported append"), None);
        assert_eq!(parse_health("ok tier-service vX"), None);
    }

    #[test]
    fn config_is_absent_unless_explicitly_set() {
        let prev = std::env::var(TIER_SERVICE_ADDR_ENV).ok();

        std::env::remove_var(TIER_SERVICE_ADDR_ENV);
        assert!(TierClientConfig::from_env().is_none(), "unset ⇒ no client");

        std::env::set_var(TIER_SERVICE_ADDR_ENV, "");
        assert!(TierClientConfig::from_env().is_none(), "empty ⇒ no client");

        std::env::set_var(TIER_SERVICE_ADDR_ENV, "nonsense");
        assert!(
            TierClientConfig::from_env().is_none(),
            "unparseable ⇒ fail closed, never a default address"
        );

        std::env::set_var(TIER_SERVICE_ADDR_ENV, "127.0.0.1:9110");
        let cfg = TierClientConfig::from_env().expect("valid ⇒ client");
        assert_eq!(cfg.timeout, Duration::from_millis(DEFAULT_TIMEOUT_MS));

        // A bad timeout must not remove a configured capability.
        std::env::set_var(TIER_SERVICE_TIMEOUT_MS_ENV, "abc");
        assert!(TierClientConfig::from_env().is_some());
        std::env::set_var(TIER_SERVICE_TIMEOUT_MS_ENV, "0");
        assert_eq!(
            TierClientConfig::from_env().unwrap().timeout,
            Duration::from_millis(DEFAULT_TIMEOUT_MS),
            "0 is not a usable timeout; fall back to the default"
        );
        std::env::set_var(TIER_SERVICE_TIMEOUT_MS_ENV, "500");
        assert_eq!(
            TierClientConfig::from_env().unwrap().timeout,
            Duration::from_millis(500)
        );

        std::env::remove_var(TIER_SERVICE_TIMEOUT_MS_ENV);
        match prev {
            Some(v) => std::env::set_var(TIER_SERVICE_ADDR_ENV, v),
            None => std::env::remove_var(TIER_SERVICE_ADDR_ENV),
        }
    }

    /// The load-bearing test: the client talks to a REAL PR-1 listener over the
    /// real wire format, not to a mock of it. A mock would happily agree with a
    /// client that had drifted from the server.
    #[tokio::test]
    async fn client_probes_a_real_listener() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_tier(listener));

        let client = TierClient::new(TierClientConfig {
            addr,
            timeout: Duration::from_millis(2_000),
        });
        assert_eq!(
            client.probe().await,
            TierProbe::Healthy {
                version: PROTOCOL_VERSION
            }
        );
    }

    #[tokio::test]
    async fn unsupported_op_round_trips_as_a_reply_not_an_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_tier(listener));

        let client = TierClient::new(TierClientConfig {
            addr,
            timeout: Duration::from_millis(2_000),
        });
        let body = client.request(b"append").await.expect("a reply, not an error");
        assert_eq!(String::from_utf8(body).unwrap(), "unsupported append");
    }

    /// END-TO-END, the PR-3 property: a record appended THROUGH THE CLIENT, over
    /// the real wire format, to a REAL listener backed by a REAL store, comes
    /// back on a subsequent read.
    ///
    /// Includes the negative control in the same test, because "read returned
    /// something" proves nothing on its own: a store that returned one blob for
    /// every key would satisfy the positive half.
    #[tokio::test]
    async fn append_then_read_round_trips_through_the_wire() {
        let mut dir = std::env::temp_dir();
        dir.push(format!("ehdb-tier-e2e-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::env::set_var(
            crate::ehdb::tier_store::TIER_SERVICE_DIR_ENV,
            dir.to_str().unwrap(),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_tier(listener));
        let client = TierClient::new(TierClientConfig { addr, timeout: Duration::from_millis(3_000) });

        let appended = client
            .append("e2e-exec-1", r#"{"marker":"E2E-HIT"}"#)
            .await
            .expect("append over the wire");
        assert!(appended.contains("appended"), "append reply: {appended}");

        let hit = client.read_execution("e2e-exec-1").await.expect("read over the wire");
        assert!(hit.contains("E2E-HIT"), "the appended payload must come back: {hit}");

        // NEGATIVE CONTROL — a different key must NOT return that payload.
        let miss = client.read_execution("e2e-absent").await.expect("read over the wire");
        assert!(
            !miss.contains("E2E-HIT"),
            "a miss must not return another execution's data: {miss}"
        );
        assert_ne!(hit, miss, "hit and miss must be distinguishable over the wire");

        std::env::remove_var(crate::ehdb::tier_store::TIER_SERVICE_DIR_ENV);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn unreachable_endpoint_is_classified_not_hung() {
        // Port 1 on loopback: nothing listens, so connect fails fast. The point
        // is that the caller gets a classified answer rather than a hang.
        let client = TierClient::new(TierClientConfig {
            addr: "127.0.0.1:1".parse().unwrap(),
            timeout: Duration::from_millis(500),
        });
        match client.probe().await {
            TierProbe::Unreachable(_) => {}
            other => panic!("expected Unreachable, got {other:?}"),
        }
    }
}
