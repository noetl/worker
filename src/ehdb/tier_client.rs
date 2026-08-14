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

use std::time::Duration;

use tokio::net::TcpStream;

use super::store_tier::StoreTier;

use super::tier_service::{read_frame, write_frame, PROTOCOL_VERSION};

/// Env var naming the tier service to talk to. Unset ⇒ no client.
pub const TIER_SERVICE_ADDR_ENV: &str = "NOETL_EHDB_TIER_SERVICE_ADDR";

/// Env var overriding the connect/request timeout, in milliseconds.
pub const TIER_SERVICE_TIMEOUT_MS_ENV: &str = "NOETL_EHDB_TIER_SERVICE_TIMEOUT_MS";

/// Default timeout. Short on purpose: this is an auxiliary verification path,
/// and it must never become a latency contributor on the caller's hot path.
pub const DEFAULT_TIMEOUT_MS: u64 = 2_000;

/// Split a `host:port` authority, accepting DNS names as well as IP literals.
///
/// Returns the normalised authority, or `None` when the shape is wrong. `[::1]:9110`
/// style bracketed IPv6 is accepted by splitting on the LAST colon.
pub fn split_authority(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let (host, port) = raw.rsplit_once(':')?;
    if host.is_empty() || port.is_empty() {
        return None;
    }
    let port: u16 = port.parse().ok()?;
    if port == 0 {
        return None;
    }
    Some(format!("{host}:{port}"))
}

/// Resolved client configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierClientConfig {
    /// The raw `host:port` authority, NOT a resolved address.
    ///
    /// Kept unresolved on purpose.  The first version parsed this as a
    /// `SocketAddr`, which accepts only literal IPs — so every Kubernetes DNS
    /// name was rejected and the client fell back to local resolution. Its unit
    /// tests all used `127.0.0.1`, so they passed while the client could not
    /// address a real service; the kind gate is what caught it.
    ///
    /// Holding the name also gives **re-resolution on every connect** for free,
    /// which matters because pod IPs move: a cached address would keep dialling
    /// a pod that no longer exists after a writer restart.
    pub addr: String,
    pub timeout: Duration,
}

impl TierClientConfig {
    /// Resolve from the process environment. `None` ⇒ no client is configured.
    ///
    /// An unparseable address fails closed with a WARN, matching the service
    /// side: a typo must leave the client absent and say so, never dial a
    /// default and never panic a process that hosts the buses.
    pub fn from_env() -> Option<Self> {
        Self::build(
            std::env::var(TIER_SERVICE_ADDR_ENV).ok().as_deref(),
            std::env::var(TIER_SERVICE_TIMEOUT_MS_ENV).ok().as_deref(),
        )
    }

    /// Resolve from an explicit [`EnvMap`] rather than the live process env.
    ///
    /// Same parse, same refusals — [`Self::from_env`] is this function over a
    /// snapshot. It exists because the request paths already hold an `EnvMap`
    /// ([`crate::ehdb::process_env`]) and reading the address from a *second*
    /// place would let "which source did I ask for" and "which address do I
    /// dial" disagree, which is unobservable at the call site.
    pub fn from_map(env: &super::EnvMap) -> Option<Self> {
        Self::build(
            env.get(TIER_SERVICE_ADDR_ENV).map(|s| s.as_str()),
            env.get(TIER_SERVICE_TIMEOUT_MS_ENV).map(|s| s.as_str()),
        )
    }

    fn build(raw: Option<&str>, raw_timeout: Option<&str>) -> Option<Self> {
        let raw = raw?.trim();
        if raw.is_empty() {
            return None;
        }
        // Validate the SHAPE (host:port with a numeric port), not resolvability.
        // Resolution belongs at connect time — a name that does not resolve at
        // startup may resolve later (the writer may simply not be up yet), and
        // refusing the config for that would be wrong.
        let addr = match split_authority(raw) {
            Some(a) => a,
            None => {
                tracing::warn!(
                    var = TIER_SERVICE_ADDR_ENV,
                    value = raw,
                    "EHDB tier service address is not host:port; no tier client will be created"
                );
                return None;
            }
        };
        // An unparseable timeout falls back to the default rather than
        // disabling the client: the address is the load-bearing setting, and a
        // bad timeout should not silently remove a configured capability.
        let timeout_ms = raw_timeout
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
#[derive(Debug, Clone)]
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

    /// Build from an explicit [`EnvMap`]. See [`TierClientConfig::from_map`].
    pub fn from_map(env: &super::EnvMap) -> Option<Self> {
        TierClientConfig::from_map(env).map(Self::new)
    }

    pub fn addr(&self) -> &str {
        &self.cfg.addr
    }

    /// Send one request frame and read one reply frame.
    ///
    /// Every step is bounded by the configured timeout — connect, write, and
    /// read alike. An unbounded read here would let a wedged writer hold a
    /// caller forever, which is precisely the shape of stall this platform has
    /// been bitten by before.
    pub async fn request(&self, payload: &[u8]) -> Result<Vec<u8>, String> {
        let fut = async {
            // Connect by NAME: tokio resolves it per call, so a moved pod is
            // picked up on the next attempt rather than cached forever.
            let mut s = TcpStream::connect(self.cfg.addr.as_str())
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

    /// Append one record to the remote **event-log** tier.
    ///
    /// Kept as the bare name because it is what every existing caller means and
    /// what the wire default resolves to. Tier-addressed callers use
    /// [`TierClient::append_tier`].
    pub async fn append(&self, execution_id: &str, payload: &str) -> Result<String, String> {
        self.append_tier(StoreTier::Eventlog, execution_id, payload)
            .await
    }

    /// Append one record to `tier` on the remote writer (#265).
    pub async fn append_tier(
        &self,
        tier: StoreTier,
        execution_id: &str,
        payload: &str,
    ) -> Result<String, String> {
        let req = serde_json::json!({
            "op": "append",
            "tier": tier.as_str(),
            "execution_id": execution_id,
            "payload": payload,
        });
        // THE APPEND IS THE PROBE.  Reachability is measured by the operation
        // that depends on it, so it cannot drift the way a cached poll does.
        //
        // Reachability stays PROCESS-WIDE rather than per-tier on purpose: it
        // answers "is the writer's tier service answering me", which is a fact
        // about one TCP endpoint, not about which file it wrote. Splitting it
        // per tier would let a tier that happens to be idle read as unreachable
        // and demote a healthy one.
        let out = self
            .request(req.to_string().as_bytes())
            .await
            .map(|b| String::from_utf8_lossy(&b).to_string());
        super::reachability::record(super::reachability::classify(&out));
        out
    }

    /// Read every record the remote **event-log** tier holds for one execution.
    pub async fn read_execution(&self, execution_id: &str) -> Result<String, String> {
        self.read_execution_tier(StoreTier::Eventlog, execution_id)
            .await
    }

    /// Read every record `tier` holds for one execution (#265).
    pub async fn read_execution_tier(
        &self,
        tier: StoreTier,
        execution_id: &str,
    ) -> Result<String, String> {
        let req = serde_json::json!({
            "op": "read_execution",
            "tier": tier.as_str(),
            "execution_id": execution_id,
        });
        let body = self.request(req.to_string().as_bytes()).await?;
        Ok(String::from_utf8_lossy(&body).to_string())
    }

    /// Bounded global scan of the remote **event-log** tier.
    pub async fn scan(&self, after: Option<u64>, limit: usize) -> Result<String, String> {
        self.scan_tier(StoreTier::Eventlog, after, limit).await
    }

    /// Bounded global scan of `tier` (#265).
    pub async fn scan_tier(
        &self,
        tier: StoreTier,
        after: Option<u64>,
        limit: usize,
    ) -> Result<String, String> {
        let req = serde_json::json!({
            "op": "scan",
            "tier": tier.as_str(),
            "after": after,
            "limit": limit,
        });
        let body = self.request(req.to_string().as_bytes()).await?;
        Ok(String::from_utf8_lossy(&body).to_string())
    }

    /// Probe the endpoint's health. Never returns an error — the classification
    /// *is* the result, and a caller deciding what to log should not also have
    /// to unwrap.
    pub async fn probe(&self) -> TierProbe {
        let raw = self.request(b"health").await;
        super::reachability::record(super::reachability::classify(&raw.clone().map(|b| String::from_utf8_lossy(&b).to_string())));
        match raw {
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

/// How many times the startup probe is attempted before giving up.
///
/// Bounded, and small. This is a boot-race smoother, not a supervisor: if the
/// service is still absent after the last attempt, the append path's own
/// cached-negative retry takes over and will promote the moment a real append
/// succeeds. Nothing here is load-bearing for recovery.
const STARTUP_PROBE_ATTEMPTS: u32 = 5;

/// Base backoff between startup probe attempts; doubles each time.
/// 250ms + 500 + 1s + 2s ≈ 3.75s of total patience across the 5 attempts.
const STARTUP_PROBE_BACKOFF_MS: u64 = 250;

/// Probe at startup when a client is configured, and record what was found.
///
/// Reachability is checked **at startup, off the hot path**. It is a deployment
/// question ("did I point this at the right host?"), not a per-request one, and
/// polling it would add load to a single-replica writer for no new information.
///
/// # Why this retries
///
/// A single attempt makes worker boot a race against writer restart. The writer
/// is a single-replica StatefulSet; a rolling restart of both leaves a window of
/// a few seconds where the worker is up and the tier face is not. One failed
/// probe would leave the worker unpromoted — not *permanently*, since the append
/// path re-probes, but for as long as it takes the next event to arrive, which
/// on an idle pool is unbounded. A handful of backed-off attempts closes that
/// window for the cost of a few seconds of a background task.
///
/// Only a **transport failure** is retried. A service that answers — even to
/// refuse, or with a version we do not speak — has been reached, and asking it
/// again would tell us nothing new.
pub async fn probe_at_startup() {
    let Some(client) = TierClient::from_env() else {
        return; // not configured — strict no-op
    };
    let addr = client.addr();
    let mut outcome = client.probe().await;
    let mut attempt = 1;
    while attempt < STARTUP_PROBE_ATTEMPTS && matches!(outcome, TierProbe::Unreachable(_)) {
        let backoff = STARTUP_PROBE_BACKOFF_MS << (attempt - 1);
        tracing::debug!(
            %addr, attempt, backoff_ms = backoff,
            "EHDB tier service not reachable yet; retrying the startup probe"
        );
        tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
        outcome = client.probe().await;
        attempt += 1;
    }
    match outcome {
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
            tracing::warn!(
                %addr, error = %err, attempts = attempt,
                "EHDB tier service is not reachable after the bounded startup probe; \
                 the append path will re-probe and promote on the first success"
            );
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
            addr: addr.to_string(),
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
            addr: addr.to_string(),
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
        let client = TierClient::new(TierClientConfig { addr: addr.to_string(), timeout: Duration::from_millis(3_000) });

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

    #[test]
    fn a_dns_hostname_is_accepted_not_rejected() {
        // THE GAP THAT HID THE BUG.  Every earlier config test used
        // `127.0.0.1:9110`, which parses as a SocketAddr — so the tests passed
        // while the client rejected every Kubernetes DNS name and silently fell
        // back to local resolution.  A gate against a real writer caught it.
        for host in [
            "noetl-cmdbus-writer-0.noetl-cmdbus-writer-headless.noetl.svc.cluster.local:9110",
            "writer:9110",
            "example.internal:1",
            "127.0.0.1:9110",
            "[::1]:9110",
        ] {
            assert!(
                split_authority(host).is_some(),
                "must accept host:port, including DNS names: {host}"
            );
        }
        // Shape errors are still refused — accepting names must not mean
        // accepting anything.
        for bad in ["", "nocolon", "host:", ":9110", "host:abc", "host:0", "host:99999"] {
            assert!(split_authority(bad).is_none(), "must reject: {bad:?}");
        }
    }

    /// Connect to a real listener via a resolvable HOSTNAME rather than a literal
    /// IP, exercising the resolution path the SocketAddr parse used to block.
    #[tokio::test]
    async fn client_connects_through_a_hostname() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(serve_tier(listener));

        // `localhost` is a genuine name lookup, not a literal-IP parse.
        let client = TierClient::new(TierClientConfig {
            addr: format!("localhost:{port}"),
            timeout: Duration::from_millis(3_000),
        });
        assert_eq!(
            client.probe().await,
            TierProbe::Healthy { version: PROTOCOL_VERSION },
            "a hostname must resolve and connect"
        );
    }

    /// PERMANENT GUARD for the arm-D defect the PR-6 kind gate found
    /// (ai-meta#257).  Ignored, not deleted: it asserts the behaviour we WANT,
    /// fails today, and flips to a live regression guard the moment the
    /// reachability signal is fixed.  Run with `cargo test -- --ignored`.
    ///
    /// The defect: the append path feeds the serve policy
    ///
    ///     durable_service_reachable = TierClientConfig::from_env().is_some()
    ///
    /// which measures CONFIGURED, not REACHABLE.  Point the address at a black
    /// hole and the policy is told the service is reachable and serves — the
    /// exact "authoritative in name only" failure the RFC exists to prevent.
    /// The policy itself is correct; its input is a lie.
    ///
    /// Same class as the DNS bug two PRs earlier: a variable whose NAME states a
    /// property its VALUE does not measure.
    #[tokio::test]
    async fn a_configured_but_unreachable_service_must_not_count_as_reachable() {
        use crate::ehdb::reachability;

        // 10.255.255.1 is a black hole: configured, never reachable.
        let client = TierClient::new(TierClientConfig {
            addr: "10.255.255.1:9110".to_string(),
            timeout: Duration::from_millis(400),
        });

        // An append against it fails at the transport, which demotes.
        let _ = client.append("armd", r#"{"x":1}"#).await;
        assert!(
            !reachability::is_reachable(),
            "a configured black hole must NOT count as a durable service — the arm-D defect"
        );
        assert!(
            reachability::is_cached_down(),
            "the negative is cached so an outage costs one slow request, not one per append"
        );

        // POSITIVE CONTROL: a real listener promotes, proving the guard is not
        // simply stuck at 'unreachable'.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_tier(listener));
        let good = TierClient::new(TierClientConfig {
            addr: format!("127.0.0.1:{}", addr.port()),
            timeout: Duration::from_millis(2_000),
        });
        assert_eq!(good.probe().await, TierProbe::Healthy { version: PROTOCOL_VERSION });
        assert!(
            reachability::is_reachable(),
            "a reachable service must promote — self-healing, no operator action"
        );
    }

    #[tokio::test]
    async fn unreachable_endpoint_is_classified_not_hung() {
        // Port 1 on loopback: nothing listens, so connect fails fast. The point
        // is that the caller gets a classified answer rather than a hang.
        let client = TierClient::new(TierClientConfig {
            addr: "127.0.0.1:1".to_string(),
            timeout: Duration::from_millis(500),
        });
        match client.probe().await {
            TierProbe::Unreachable(_) => {}
            other => panic!("expected Unreachable, got {other:?}"),
        }
    }
}
