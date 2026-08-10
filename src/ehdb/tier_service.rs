//! EHDB tier service — the writer-fronted face for the storage tiers.
//!
//! **PR 1 of [ai-meta#257](https://github.com/noetl/ai-meta/issues/257): skeleton
//! and protocol only.** This module binds a listener and answers `health`. It
//! does not read, append, or serve any tier. That is deliberate — the RFC's
//! phase 1 is "a listener that does not exist unless a flag is set", so the
//! risky part (tier data actually moving) lands in a later PR with its own gate.
//!
//! # Why this exists
//!
//! EHDB's storage tiers are **pod-local**: every worker mirrors into its own
//! `NOETL_EHDB_LOCAL_REFERENCE_LOG`, and the server's `/api/ehdb/*` resolves
//! from the *server's own* location. Prod's PVCs are all `ReadWriteOnce`, so the
//! shared mount `NOETL_EHDB_EVENTLOG_SHARED_DIR` anticipates cannot be mounted
//! by two pods. The tier is therefore **N disjoint stores**, and no flag can
//! make one of them authoritative.
//!
//! The fix is to give the durable store an owner that other processes can talk
//! to. The writer already is that owner — it holds the durable volumes and
//! already fronts both buses — so this is a third face on a process built to
//! host faces, not a new component.
//!
//! # Wire format
//!
//! Length-framed binary, mirroring `ehdb_feed`'s ingest face so the two are
//! debuggable with the same tools:
//!
//! ```text
//!   u32 big-endian length  ||  <length> bytes of payload
//! ```
//!
//! A frame larger than [`MAX_FRAME_BYTES`] is refused and the connection closed,
//! rather than trusting a length prefix to size an allocation.
//!
//! # Inertness
//!
//! With `NOETL_EHDB_TIER_SERVICE_BIND` unset, [`TierServiceConfig::from_env`]
//! yields `None`, no socket is opened, no task is spawned, and no metric family
//! gains a child — so `/metrics` is byte-identical to a build without this
//! module. That is the property PR 1's gate asserts.

use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Env var naming the bind address for the tier service. Unset ⇒ the face does
/// not exist. There is no default: a service that silently binds a port because
/// someone forgot to set a variable is the opposite of what this PR is for.
pub const TIER_SERVICE_BIND_ENV: &str = "NOETL_EHDB_TIER_SERVICE_BIND";

/// Largest frame accepted, in bytes (1 MiB).
///
/// The length prefix is attacker- (or bug-) controlled, and the natural
/// implementation — read a u32, allocate that many bytes — lets one bad frame
/// ask for 4 GiB. The cap is checked *before* any allocation.
pub const MAX_FRAME_BYTES: u32 = 1024 * 1024;

/// Protocol version, sent in every `health` reply so a client can refuse to talk
/// to a writer it does not understand rather than misparse its frames.
pub const PROTOCOL_VERSION: u16 = 1;

/// Resolved tier-service configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TierServiceConfig {
    pub bind: SocketAddr,
}

impl TierServiceConfig {
    /// Resolve from the process environment. `None` ⇒ the face is not enabled.
    ///
    /// An unparseable value is **fail-closed with a WARN**, not a panic and not
    /// a silent default: a typo in a bind address should leave the face absent
    /// and say so, not take the writer's whole process down — the writer hosts
    /// both buses, so panicking here would convert a typo into a platform
    /// outage.
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var(TIER_SERVICE_BIND_ENV).ok()?;
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        match raw.parse::<SocketAddr>() {
            Ok(bind) => Some(Self { bind }),
            Err(e) => {
                tracing::warn!(
                    var = TIER_SERVICE_BIND_ENV,
                    error = %e,
                    "EHDB tier service bind address is unparseable; the tier face will NOT be started"
                );
                None
            }
        }
    }
}

/// One decoded request. PR 1 knows exactly one operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TierRequest {
    /// Liveness + protocol handshake. Carries no tier data.
    Health,
    /// A frame this build does not implement. Carried as a value rather than an
    /// error so the server can answer `unsupported` — a client talking to an
    /// older writer must get a clear reply, not a dropped connection.
    Unsupported(String),
}

/// Decode a request frame payload.
///
/// The payload is a bare ASCII op name in PR 1. It is deliberately not JSON:
/// the ops that carry real data land in PR 2/3 and will define their own
/// encoding then, and inventing a schema now would freeze a guess.
pub fn decode_request(payload: &[u8]) -> TierRequest {
    match std::str::from_utf8(payload).map(str::trim) {
        Ok("health") => TierRequest::Health,
        Ok(other) => TierRequest::Unsupported(other.to_string()),
        Err(_) => TierRequest::Unsupported("<non-utf8>".to_string()),
    }
}

/// Encode the reply for a request.
pub fn encode_response(req: &TierRequest) -> Vec<u8> {
    match req {
        TierRequest::Health => format!("ok tier-service v{PROTOCOL_VERSION}").into_bytes(),
        TierRequest::Unsupported(op) => format!("unsupported {op}").into_bytes(),
    }
}

/// Read one length-framed message.
///
/// Returns `Ok(None)` on a clean EOF at a frame boundary (the peer hung up
/// between requests, which is normal), and an error for a truncated frame or an
/// over-long one — those are protocol violations and must not be silently
/// treated as "no more work".
/// Shared with the client (`tier_client`) so both ends use ONE codec.  Two
/// implementations of the same wire format is how a protocol drifts.
pub(crate) async fn read_frame(stream: &mut TcpStream) -> std::io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("tier-service frame of {len} bytes exceeds the {MAX_FRAME_BYTES}-byte cap"),
        ));
    }
    let mut payload = vec![0u8; len as usize];
    stream.read_exact(&mut payload).await?;
    Ok(Some(payload))
}

/// Write one length-framed message.
/// Shared with the client — see [`read_frame`].
pub(crate) async fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> std::io::Result<()> {
    let len = u32::try_from(payload.len()).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "response exceeds u32 length")
    })?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(payload).await?;
    stream.flush().await
}

/// Serve one connection until the peer hangs up or violates the protocol.
async fn serve_conn(mut stream: TcpStream) {
    loop {
        match read_frame(&mut stream).await {
            Ok(None) => return,
            Ok(Some(payload)) => {
                let req = decode_request(&payload);
                let resp = encode_response(&req);
                if let Err(e) = write_frame(&mut stream, &resp).await {
                    tracing::debug!(error = %e, "EHDB tier service: write failed; closing connection");
                    return;
                }
            }
            Err(e) => {
                // WARN, not silence: a malformed frame means a client is talking
                // a protocol this writer does not speak, which is exactly the
                // thing an operator needs to see during a rollout.
                tracing::warn!(error = %e, "EHDB tier service: protocol error; closing connection");
                return;
            }
        }
    }
}

/// Accept loop. Runs until the task is dropped.
pub async fn serve_tier(listener: TcpListener) {
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                tokio::spawn(serve_conn(stream));
            }
            Err(e) => {
                tracing::warn!(error = %e, "EHDB tier service: accept failed");
                // Yield rather than spin if the listener is in a bad state.
                tokio::task::yield_now().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_round_trips() {
        let req = decode_request(b"health");
        assert_eq!(req, TierRequest::Health);
        let resp = String::from_utf8(encode_response(&req)).unwrap();
        assert!(resp.starts_with("ok tier-service v"), "got {resp}");
    }

    #[test]
    fn unknown_op_is_answered_not_dropped() {
        // A client on a newer protocol must get a reply it can act on, rather
        // than a closed socket it has to guess about.
        let req = decode_request(b"append");
        assert_eq!(req, TierRequest::Unsupported("append".to_string()));
        assert_eq!(encode_response(&req), b"unsupported append".to_vec());
    }

    #[test]
    fn non_utf8_does_not_panic() {
        assert_eq!(
            decode_request(&[0xff, 0xfe]),
            TierRequest::Unsupported("<non-utf8>".to_string())
        );
    }

    // --- config: the inertness property this PR exists to establish ---
    //
    // These mutate process env, so they are one test: `cargo test` does NOT
    // serialise tests within a binary, and a sibling test reading the same var
    // concurrently would flake. (Learned the hard way — an EnvGuard SAFETY note
    // in this crate claimed the opposite and its tests raced.)
    #[test]
    fn from_env_is_absent_unless_explicitly_set() {
        let prev = std::env::var(TIER_SERVICE_BIND_ENV).ok();

        std::env::remove_var(TIER_SERVICE_BIND_ENV);
        assert!(
            TierServiceConfig::from_env().is_none(),
            "unset must mean the face does not exist — there is no default port"
        );

        std::env::set_var(TIER_SERVICE_BIND_ENV, "");
        assert!(
            TierServiceConfig::from_env().is_none(),
            "empty must be treated as unset, not as a parse error"
        );

        std::env::set_var(TIER_SERVICE_BIND_ENV, "not-an-address");
        assert!(
            TierServiceConfig::from_env().is_none(),
            "unparseable must fail closed (WARN + no face), never panic the writer"
        );

        std::env::set_var(TIER_SERVICE_BIND_ENV, "0.0.0.0:9110");
        let cfg = TierServiceConfig::from_env().expect("a valid address must enable the face");
        assert_eq!(cfg.bind, "0.0.0.0:9110".parse::<SocketAddr>().unwrap());

        match prev {
            Some(v) => std::env::set_var(TIER_SERVICE_BIND_ENV, v),
            None => std::env::remove_var(TIER_SERVICE_BIND_ENV),
        }
    }

    #[tokio::test]
    async fn listener_answers_health_over_the_wire() {
        // Behaviour, not a call site: bind an ephemeral port, speak the actual
        // frame format, and assert on the bytes that come back.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_tier(listener));

        let mut c = TcpStream::connect(addr).await.unwrap();
        write_frame(&mut c, b"health").await.unwrap();
        let reply = read_frame(&mut c).await.unwrap().expect("a reply frame");
        let reply = String::from_utf8(reply).unwrap();
        assert!(reply.starts_with("ok tier-service v"), "got {reply}");
    }

    #[tokio::test]
    async fn oversized_length_prefix_is_refused_before_allocating() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_tier(listener));

        let mut c = TcpStream::connect(addr).await.unwrap();
        // Claim a frame far larger than the cap and send nothing after it.
        c.write_all(&(MAX_FRAME_BYTES + 1).to_be_bytes()).await.unwrap();
        c.flush().await.unwrap();
        // The server must close rather than wait on (or allocate for) the body.
        let mut buf = [0u8; 1];
        let n = c.read(&mut buf).await.unwrap_or(0);
        assert_eq!(n, 0, "server must close the connection on an over-long frame");
    }
}
