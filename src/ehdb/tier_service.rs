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
    /// Append one record to the event-log tier (ai-meta#257 PR 3).
    Append {
        execution_id: String,
        payload: String,
    },
    /// Append N records under one store lock and ONE `fsync`
    /// (noetl/ai-meta#155).  Semantically N `Append`s in order; the reply
    /// carries one result per record so a caller reports exactly what it did
    /// when it looped.
    AppendBatch {
        execution_id: String,
        payloads: Vec<String>,
    },
    /// Read every record for one execution.
    ReadExecution { execution_id: String },
    /// Bounded global scan.
    Scan { after: Option<u64>, limit: usize },
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
    let Ok(text) = std::str::from_utf8(payload) else {
        return TierRequest::Unsupported("<non-utf8>".to_string());
    };
    let text = text.trim();
    // PR 1 spoke bare op names.  `health` stays bare so a PR-1 client keeps
    // working against a PR-3 writer — a protocol that breaks its own previous
    // version during a rolling upgrade is a self-inflicted outage.
    if text == "health" {
        return TierRequest::Health;
    }
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return TierRequest::Unsupported(text.chars().take(40).collect());
    };
    match v.get("op").and_then(|o| o.as_str()) {
        Some("append") => TierRequest::Append {
            execution_id: v
                .get("execution_id")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            payload: v
                .get("payload")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
        },
        Some("append_batch") => TierRequest::AppendBatch {
            execution_id: v
                .get("execution_id")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            payloads: v
                .get("payloads")
                .and_then(|x| x.as_array())
                .map(|a| {
                    a.iter()
                        .map(|p| p.as_str().unwrap_or("").to_string())
                        .collect()
                })
                .unwrap_or_default(),
        },
        Some("read_execution") => TierRequest::ReadExecution {
            execution_id: v
                .get("execution_id")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
        },
        Some("scan") => TierRequest::Scan {
            after: v.get("after").and_then(|x| x.as_u64()),
            limit: v.get("limit").and_then(|x| x.as_u64()).unwrap_or(100) as usize,
        },
        Some(other) => TierRequest::Unsupported(other.to_string()),
        None => TierRequest::Unsupported("<no op>".to_string()),
    }
}

/// How one handled request is classified for metrics: the bare operation name,
/// the outcome, and the two health bits the `noetl_ehdb_*` families carry.
///
/// `degraded` means **this writer is not able to serve**, which is a different
/// question from `ok`. A malformed request is `ok = false, degraded = false` —
/// the caller got a correct refusal and the service is fine. No store and a
/// store error are both `degraded`, because in each case a tier promoted to
/// primary here would be unable to answer. That split is the whole point: an
/// alert on `degraded` must not fire because someone sent a bad frame.
pub(crate) struct Observed {
    pub op: &'static str,
    pub outcome: &'static str,
    pub ok: bool,
    pub degraded: bool,
}

/// Encode the reply for a request.
pub async fn encode_response(req: &TierRequest) -> Vec<u8> {
    encode_response_observed(req).await.0
}

/// Encode the reply and classify it in one pass.
///
/// One function, not two, because the classification depends on the store's
/// answer — a `read_execution` is a hit or a miss according to what came back,
/// and re-deriving that from the encoded bytes afterwards would be a second
/// implementation of the same decision, free to disagree with the first.
pub(crate) async fn encode_response_observed(req: &TierRequest) -> (Vec<u8>, Observed) {
    use super::tier_store::{self, TierStoreOutcome};
    let cfg = tier_store::TierStoreConfig::from_env();

    // Did a read actually return records? Parsed from the body the store just
    // produced. A parse failure counts as a miss rather than panicking: this is
    // a metric label, and a malformed body is already going to surface as a
    // client-side error.
    let has_records = |body: &str| -> bool {
        serde_json::from_str::<serde_json::Value>(body)
            .ok()
            .and_then(|v| v.get("record_count").and_then(|c| c.as_u64()))
            .is_some_and(|n| n > 0)
    };

    let render = |op: &'static str, o: TierStoreOutcome| -> (Vec<u8>, Observed) {
        match o {
            TierStoreOutcome::Ok(body) => {
                // Reads distinguish hit from miss; a write is simply `ok`.
                let outcome = if op == "append" {
                    "ok"
                } else if has_records(&body) {
                    "hit"
                } else {
                    "miss"
                };
                (
                    body.into_bytes(),
                    Observed {
                        op,
                        outcome,
                        ok: true,
                        degraded: false,
                    },
                )
            }
            // Each failure keeps its own shape.  A caller must be able to tell
            // "this writer has no store" from "your request was malformed" from
            // "the store broke" — collapsing them into one error is how an
            // operator spends an afternoon on the wrong hypothesis.
            TierStoreOutcome::Unavailable => (
                b"unavailable no tier store configured".to_vec(),
                Observed {
                    op,
                    outcome: "unavailable",
                    ok: false,
                    degraded: true,
                },
            ),
            TierStoreOutcome::Invalid(e) => (
                format!("invalid {e}").into_bytes(),
                Observed {
                    op,
                    outcome: "invalid",
                    ok: false,
                    degraded: false,
                },
            ),
            TierStoreOutcome::Error(e) => (
                format!("error {e}").into_bytes(),
                Observed {
                    op,
                    outcome: "error",
                    ok: false,
                    degraded: true,
                },
            ),
        }
    };

    match req {
        TierRequest::Health => (
            format!("ok tier-service v{PROTOCOL_VERSION}").into_bytes(),
            Observed {
                op: "health",
                outcome: "ok",
                ok: true,
                degraded: false,
            },
        ),
        TierRequest::Append {
            execution_id,
            payload,
        } => render(
            "append",
            tier_store::append(cfg.as_ref(), execution_id, payload).await,
        ),
        TierRequest::AppendBatch {
            execution_id,
            payloads,
        } => {
            let outs = tier_store::append_batch(cfg.as_ref(), execution_id, payloads).await;
            // One reply carrying one result per record, in order. The batch is
            // refused whole on error, so `ok` is a property of the batch — a
            // caller must not have to guess a split point.
            let results: Vec<serde_json::Value> = outs
                .iter()
                .map(|o| match o {
                    tier_store::TierStoreOutcome::Ok(body) => serde_json::json!({
                        "ok": true,
                        "body": serde_json::from_str::<serde_json::Value>(body)
                            .unwrap_or(serde_json::Value::Null),
                    }),
                    other => serde_json::json!({ "ok": false, "error": format!("{other:?}") }),
                })
                .collect();
            let all_ok = outs
                .iter()
                .all(|o| matches!(o, tier_store::TierStoreOutcome::Ok(_)));
            let body = serde_json::json!({
                "action": "ehdb.tier.append_batch",
                "outcome": if all_ok { "ok" } else { "error" },
                "appended": results.iter().filter(|r| r["ok"] == true).count(),
                "requested": payloads.len(),
                "results": results,
            })
            .to_string();
            (
                body.into_bytes(),
                Observed {
                    op: "append_batch",
                    outcome: if all_ok { "ok" } else { "error" },
                    ok: all_ok,
                    degraded: false,
                },
            )
        }
        TierRequest::ReadExecution { execution_id } => render(
            "read_execution",
            tier_store::read_execution(cfg.as_ref(), execution_id).await,
        ),
        TierRequest::Scan { after, limit } => {
            render("scan", tier_store::scan(cfg.as_ref(), *after, *limit).await)
        }
        // `op` is NOT the label — the unknown op name is caller-controlled and
        // would make `operation` unbounded-cardinality. The name stays in the
        // reply, where the caller can read it.
        TierRequest::Unsupported(op) => (
            format!("unsupported {op}").into_bytes(),
            Observed {
                op: "unsupported",
                outcome: "unsupported",
                ok: false,
                degraded: false,
            },
        ),
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
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "response exceeds u32 length",
        )
    })?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(payload).await?;
    stream.flush().await
}

/// Record one connection-lifecycle event (ai-meta#260).
///
/// Connection events carry no latency — the duration of "a peer hung up" is not
/// a quantity — so they pass 0.0 and are excluded from the pinned latency
/// series.
fn record_conn(outcome: &str, ok: bool, degraded: bool) {
    super::metrics::record_tier_service("conn", outcome, ok, degraded, 0.0);
}

/// Serve one connection until the peer hangs up or violates the protocol.
async fn serve_conn(mut stream: TcpStream) {
    record_conn("accepted", true, false);
    loop {
        match read_frame(&mut stream).await {
            Ok(None) => {
                record_conn("closed", true, false);
                return;
            }
            Ok(Some(payload)) => {
                // The measured window is decode → store → encode: everything
                // this service is responsible for. It deliberately excludes the
                // frame read (which is dominated by how long the client took to
                // send) and the write (which is dominated by the client's
                // receive window). Including either would make the tier store
                // look slow whenever a caller was.
                let started = std::time::Instant::now();
                let req = decode_request(&payload);
                let (resp, obs) = encode_response_observed(&req).await;
                let elapsed = started.elapsed().as_secs_f64();
                super::metrics::record_tier_service(
                    obs.op,
                    obs.outcome,
                    obs.ok,
                    obs.degraded,
                    elapsed,
                );
                if let Err(e) = write_frame(&mut stream, &resp).await {
                    // Degraded: the request was served and the answer was lost.
                    // From the caller's side this is indistinguishable from the
                    // service being down, so it must not read as healthy here.
                    record_conn("write_error", false, true);
                    tracing::debug!(error = %e, "EHDB tier service: write failed; closing connection");
                    return;
                }
            }
            Err(e) => {
                // WARN, not silence: a malformed frame means a client is talking
                // a protocol this writer does not speak, which is exactly the
                // thing an operator needs to see during a rollout.
                record_conn("protocol_error", false, false);
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
                record_conn("accept_error", false, true);
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

    #[tokio::test]
    async fn health_round_trips() {
        let req = decode_request(b"health");
        assert_eq!(req, TierRequest::Health);
        let resp = String::from_utf8(encode_response(&req).await).unwrap();
        assert!(resp.starts_with("ok tier-service v"), "got {resp}");
    }

    #[tokio::test]
    async fn unknown_op_is_answered_not_dropped() {
        // A client on a newer protocol must get a reply it can act on, rather
        // than a closed socket it has to guess about.
        let req = decode_request(b"append");
        assert_eq!(req, TierRequest::Unsupported("append".to_string()));
        assert_eq!(encode_response(&req).await, b"unsupported append".to_vec());
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

    // Every test that serves a frame now writes the process-global metric
    // accumulator (ai-meta#260), so each takes the shared metrics test lock.
    // Without it, serving one health frame here can land between another
    // module's `reset()` and its "renders nothing" assertion.
    use super::super::metrics;

    #[tokio::test]
    async fn listener_answers_health_over_the_wire() {
        let _guard = metrics::test_guard();
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
        let _guard = metrics::test_guard();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_tier(listener));

        let mut c = TcpStream::connect(addr).await.unwrap();
        // Claim a frame far larger than the cap and send nothing after it.
        c.write_all(&(MAX_FRAME_BYTES + 1).to_be_bytes())
            .await
            .unwrap();
        c.flush().await.unwrap();
        // The server must close rather than wait on (or allocate for) the body.
        let mut buf = [0u8; 1];
        let n = c.read(&mut buf).await.unwrap_or(0);
        assert_eq!(
            n, 0,
            "server must close the connection on an over-long frame"
        );
    }

    /// Serving a real frame over a real socket must move the counter AND the
    /// histogram (ai-meta#260).
    ///
    /// Driven end-to-end through the accept loop rather than by calling
    /// `record_tier_service` directly: the defect #260 describes is not "the
    /// recorder is wrong", it is "the serve path never calls one". A test that
    /// invoked the recorder itself would pass against the un-instrumented
    /// module this replaces.
    #[tokio::test]
    async fn serving_a_frame_moves_the_counter_and_the_histogram() {
        let _guard = metrics::test_guard();
        metrics::reset();
        metrics::pin_tier_service_series(0, 0);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_tier(listener));

        let mut c = TcpStream::connect(addr).await.unwrap();
        write_frame(&mut c, b"health").await.unwrap();
        let _ = read_frame(&mut c).await.unwrap().expect("a reply frame");

        let text = metrics::render_lines().join("\n");
        assert!(
            text.contains(
                "noetl_ehdb_dataplane_ops_total{operation=\"tier_service.health\",outcome=\"ok\"} 1"
            ),
            "health must be counted:\n{text}"
        );
        assert!(
            text.contains("noetl_ehdb_tier_service_duration_seconds_count{operation=\"health\"} 1"),
            "health must be timed:\n{text}"
        );
        assert!(
            text.contains(
                "noetl_ehdb_dataplane_ops_total{operation=\"tier_service.conn\",outcome=\"accepted\"} 1"
            ),
            "the connection must be counted:\n{text}"
        );
        // The negative half: an operation nobody performed stays pinned at 0.
        assert!(
            text.contains(
                "noetl_ehdb_dataplane_ops_total{operation=\"tier_service.append\",outcome=\"ok\"} 0"
            ),
            "an unserved op must read 0, not be absent:\n{text}"
        );
        metrics::reset();
    }

    /// A protocol violation is counted as one, and NOT as a served request.
    /// Folding it into the request counters would make a client speaking the
    /// wrong protocol look like healthy traffic.
    #[tokio::test]
    async fn a_protocol_error_is_counted_separately_from_a_request() {
        let _guard = metrics::test_guard();
        metrics::reset();
        metrics::pin_tier_service_series(0, 0);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_tier(listener));

        let mut c = TcpStream::connect(addr).await.unwrap();
        c.write_all(&(MAX_FRAME_BYTES + 1).to_be_bytes())
            .await
            .unwrap();
        c.flush().await.unwrap();
        let mut buf = [0u8; 1];
        let _ = c.read(&mut buf).await.unwrap_or(0);

        let text = metrics::render_lines().join("\n");
        assert!(
            text.contains(
                "noetl_ehdb_dataplane_ops_total{operation=\"tier_service.conn\",outcome=\"protocol_error\"} 1"
            ),
            "the protocol error must be counted:\n{text}"
        );
        assert!(
            text.contains("noetl_ehdb_tier_service_duration_seconds_count{operation=\"health\"} 0"),
            "a rejected frame is not a served request:\n{text}"
        );
        metrics::reset();
    }

    /// The taxonomy that alerting depends on: a malformed request is `ok=false`
    /// but NOT `degraded`, while an absent store IS degraded. An alert on
    /// degraded must not fire because a caller sent a bad frame.
    #[tokio::test]
    async fn a_bad_request_is_not_a_degraded_service() {
        let bad = encode_response_observed(&TierRequest::Append {
            execution_id: String::new(),
            payload: "{}".to_string(),
        })
        .await
        .1;
        // With no store configured the append cannot even reach validation, so
        // assert on whichever of the two failure shapes this environment yields
        // — both must agree on the invariant under test.
        assert!(!bad.ok, "an empty execution_id must not read as success");
        match bad.outcome {
            "invalid" => assert!(!bad.degraded, "a caller error is not a service degradation"),
            "unavailable" => assert!(bad.degraded, "no store means this writer cannot serve"),
            other => panic!("unexpected outcome {other}"),
        }

        let unsupported =
            encode_response_observed(&TierRequest::Unsupported("nonsense".to_string()))
                .await
                .1;
        assert_eq!(unsupported.op, "unsupported");
        assert!(
            !unsupported.degraded,
            "an unknown op is a client/version issue, not a sick service"
        );
    }
}
