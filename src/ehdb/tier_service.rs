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

use super::store_tier::StoreTier;

/// Env var naming the bind address for the tier service. Unset ⇒ the face does
/// not exist. There is no default: a service that silently binds a port because
/// someone forgot to set a variable is the opposite of what this PR is for.
pub const TIER_SERVICE_BIND_ENV: &str = "NOETL_EHDB_TIER_SERVICE_BIND";

/// Largest **request** frame accepted, in bytes (1 MiB).
///
/// The length prefix is attacker- (or bug-) controlled, and the natural
/// implementation — read a u32, allocate that many bytes — lets one bad frame
/// ask for 4 GiB. The cap is checked *before* any allocation.
///
/// ⚠ This bounds what an untrusted peer may send us. It is deliberately NOT
/// the bound on replies we read back from our own writer — see
/// [`MAX_REPLY_BYTES`]. Using one constant for both is what caused
/// noetl/ai-meta#343 blocker 2.
pub const MAX_FRAME_BYTES: u32 = 1024 * 1024;

/// Largest **reply** frame a client will read back, in bytes (16 MiB).
///
/// # Why this is a different number
///
/// The codec is shared by both ends, and until noetl/ai-meta#343 the cap was
/// too: a client did `write_frame` (which enforced **no** cap) and then
/// `read_frame` (which enforced 1 MiB). So the tier service would happily
/// serialise a query reply that its own client structurally could not read —
/// not intermittently, but for **every** execution whose payload exceeded
/// 1 MiB. Prod symptom, 2026-09-12:
///
/// ```text
/// read: tier-service frame of 1174874 bytes exceeds the 1048576-byte cap
/// ```
///
/// Three `muno/playbooks/itinerary-planner` executions in a 40-execution
/// sample were unreadable this way. They were not reported as divergent —
/// they could not be compared **at all**, so they left the parity denominator
/// entirely. A cliff that removes rows from the population is worse than one
/// that fails loudly.
///
/// # Why 16 MiB
///
/// The threat model differs by direction. A request arrives from a peer whose
/// length prefix we must not trust. A reply arrives from the writer we just
/// connected to, in answer to a query we issued — the allocation is bounded by
/// what we asked for, not by what a stranger claims. 16 MiB matches
/// `object::MAX_OBJECT_BYTES_CEILING`, the largest single object this worker
/// already accepts, so the transport stops being the narrower limit.
///
/// # This is a ceiling, not a solution
///
/// It converts a cliff at 1 MiB into a cliff at 16 MiB. What makes it safe is
/// that the cliff is now **visible and explicit**: the service refuses to emit
/// a frame the peer cannot read and answers with a structured error that fits,
/// counted on `tier_service.reply_too_large`. Paging the query reply is the
/// real fix and is tracked separately; this stops the silent denominator loss.
pub const MAX_REPLY_BYTES: u32 = 16 * 1024 * 1024;

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
    /// Append one record to `tier` (ai-meta#257 PR 3; tier-addressed since #265).
    Append {
        tier: StoreTier,
        execution_id: String,
        payload: String,
    },
    /// Append N records to `tier` under one store lock and ONE `fsync`
    /// (noetl/ai-meta#155).  Semantically N `Append`s in order; the reply
    /// carries one result per record so a caller reports exactly what it did
    /// when it looped.
    AppendBatch {
        tier: StoreTier,
        execution_id: String,
        payloads: Vec<String>,
    },
    /// Read every record `tier` holds for one execution.
    ReadExecution {
        tier: StoreTier,
        execution_id: String,
    },
    /// Bounded global scan of one tier.
    Scan {
        tier: StoreTier,
        after: Option<u64>,
        limit: usize,
    },
    /// A frame naming a tier this writer has no store for. Distinct from
    /// [`TierRequest::Unsupported`] because the two need different operator
    /// responses: an unknown *op* is a version skew, an unknown *tier* is a
    /// caller addressing a store that does not exist. Folding them together
    /// would answer "your writer is old" to someone who typed `projction`.
    UnknownTier(String),
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
///
/// **Tier addressing (#265).** Data ops carry an optional `tier`. Absent means
/// `eventlog`, so every frame a pre-#265 client sends decodes to exactly what
/// it meant — the rolling-upgrade property, on the tier that is already primary
/// in prod. Present-but-unrecognised is [`TierRequest::UnknownTier`], never a
/// silent fall-back to the event log.
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
    let op = v.get("op").and_then(|o| o.as_str());
    // Resolve the tier ONCE, before dispatching on the op: every data op takes
    // the same field with the same meaning, and three copies of this would be
    // three chances for one of them to default differently.
    let tier = match StoreTier::parse_or_default(v.get("tier").and_then(|x| x.as_str())) {
        Ok(t) => t,
        Err(e) => return TierRequest::UnknownTier(e),
    };
    match op {
        Some("append") => TierRequest::Append {
            tier,
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
            tier,
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
            tier,
            execution_id: v
                .get("execution_id")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
        },
        Some("scan") => TierRequest::Scan {
            tier,
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
            tier,
            execution_id,
            payload,
        } => render(
            "append",
            tier_store::append(cfg.as_ref(), *tier, execution_id, payload).await,
        ),
        TierRequest::AppendBatch {
            tier,
            execution_id,
            payloads,
        } => {
            let outs = tier_store::append_batch(cfg.as_ref(), *tier, execution_id, payloads).await;
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
        TierRequest::ReadExecution { tier, execution_id } => render(
            "read_execution",
            tier_store::read_execution(cfg.as_ref(), *tier, execution_id).await,
        ),
        TierRequest::Scan { tier, after, limit } => render(
            "scan",
            tier_store::scan(cfg.as_ref(), *tier, *after, *limit).await,
        ),
        // A caller mistake, so `ok:false, degraded:false` — the writer is fine
        // and an alert on `degraded` must not fire because someone typed a tier
        // name wrong. `invalid ` prefix so the client's existing refusal parser
        // classifies it as a rejection rather than an outage.
        TierRequest::UnknownTier(reason) => (
            format!("invalid {reason}").into_bytes(),
            Observed {
                op: "unknown_tier",
                outcome: "invalid",
                ok: false,
                degraded: false,
            },
        ),
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
pub(crate) async fn read_frame(stream: &mut TcpStream, cap: u32) -> std::io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > cap {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("tier-service frame of {len} bytes exceeds the {cap}-byte cap"),
        ));
    }
    let mut payload = vec![0u8; len as usize];
    stream.read_exact(&mut payload).await?;
    Ok(Some(payload))
}

/// Write one length-framed message.
/// Shared with the client — see [`read_frame`].
/// Write one length-framed **reply** (service → client), bounded by
/// [`MAX_REPLY_BYTES`].
pub(crate) async fn write_reply_frame(
    stream: &mut TcpStream,
    payload: &[u8],
) -> std::io::Result<()> {
    write_frame_capped(stream, payload, MAX_REPLY_BYTES, "reply")
        .await
}

/// Write one length-framed **request** (client → service), bounded by
/// [`MAX_FRAME_BYTES`] — the cap the service actually reads requests at.
///
/// ⚠⚠ This split exists because noetl/worker#311 got it wrong in the other
/// direction. That PR raised the REPLY cap to 16 MiB and made `write_frame`
/// validate against it — for **both** directions. So a client could write a
/// 1.25 MB *request*, pass the local check at 16 MiB, and have the service
/// refuse it at its 1 MiB request cap and close the socket. Prod, 2026-09-13:
///
/// ```text
/// relay:  write: Connection reset by peer (os error 104)
/// writer: protocol error; closing connection
///         error=tier-service frame of 1251646 bytes exceeds the 1048576-byte cap
/// ```
///
/// 48 occurrences, and the mirror dropped the batch permanently — no retry can
/// fix a frame the peer structurally cannot read. The failure surfaced at the
/// far end as an unattributable reset, with nothing counted on the side that
/// caused it. **That is the exact defect #311 was written to remove, recreated
/// by #311's own guard**: one cap for two directions is the same mistake as one
/// cap for two roles.
///
/// A writer must validate against the cap its READER uses, which means the
/// direction has to be part of the call.
pub(crate) async fn write_request_frame(
    stream: &mut TcpStream,
    payload: &[u8],
) -> std::io::Result<()> {
    write_frame_capped(stream, payload, MAX_FRAME_BYTES, "request")
        .await
}

async fn write_frame_capped(
    stream: &mut TcpStream,
    payload: &[u8],
    cap: u32,
    what: &str,
) -> std::io::Result<()> {
    if payload.len() > cap as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "refusing to write a {}-byte {what} frame; the reader cap is {cap} bytes",
                payload.len()
            ),
        ));
    }
    write_frame(stream, payload).await
}

pub(crate) async fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> std::io::Result<()> {
    // ⚠ The write side used to check only u32 overflow, so it would emit frames
    // no reader on either end would accept (noetl/ai-meta#343). A codec whose
    // writer can produce what its reader must refuse is not one protocol, it is
    // two — and the failure surfaces at the far end as an unattributable
    // `read:` error with no counter on the side that caused it.
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
        match read_frame(&mut stream, MAX_FRAME_BYTES).await {
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
                // An over-large reply is answered, not dropped. Before
                // noetl/ai-meta#343 the service wrote the frame anyway and the
                // client refused it with `read: … exceeds the … cap`, which
                // named no execution, incremented nothing here, and removed the
                // execution from every parity denominator instead of marking it
                // unreadable. Now the caller gets a reply that FITS and says so.
                let resp = if resp.len() > MAX_REPLY_BYTES as usize {
                    record_conn("reply_too_large", false, true);
                    tracing::warn!(
                        bytes = resp.len(),
                        cap = MAX_REPLY_BYTES,
                        op = obs.op,
                        "EHDB tier service: reply exceeds the frame cap; answering with an \
                         explicit error so the caller can count it as unreadable rather than \
                         losing it"
                    );
                    format!(
                        "err reply of {} bytes exceeds the {MAX_REPLY_BYTES}-byte frame cap; \
                         the execution is UNREADABLE, not absent — page the query",
                        resp.len()
                    )
                    .into_bytes()
                } else {
                    resp
                };
                if let Err(e) = write_reply_frame(&mut stream, &resp).await {
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

/// Cap on tier requests served concurrently.
///
/// ⚠⚠ This exists because the accept loop used to be an **unbounded fan-in**:
/// one `tokio::spawn` per accepted connection, with nothing limiting how many
/// ran at once. That is survivable when each request is cheap. It is not
/// survivable here, because a tier append costs a copy of the tier's entire
/// in-memory state (`LocalReferenceRuntime::append` clones `self.state`), so N
/// concurrent appends cost N copies of a state that is proportional to the
/// store.
///
/// Measured: per-append cost rises **linearly** with record count — 8.1 ms at
/// 400 records, 36.6 ms at 4,000, against a store that reached 4.0 GB in
/// production. On 2026-09-20 a KEDA reconnect of the worker pool (it scales
/// 1→20) drove `noetl-cmdbus-writer-0` past an 8 GiB limit nine seconds after
/// the roll began, OOM-killing the process that hosts BOTH buses.
///
/// So the bound is backpressure, not a queue: a permit is taken **before**
/// `accept`, so when the writer is saturated the connection simply waits in the
/// kernel backlog rather than becoming another in-flight state copy. A queue
/// here would convert a memory spike into an unbounded task list, which is the
/// same failure wearing a different hat.
pub const TIER_MAX_INFLIGHT_ENV: &str = "NOETL_EHDB_TIER_MAX_INFLIGHT";

/// Default concurrent tier requests.
///
/// 4, not 1: serialising completely would make a slow append block unrelated
/// reads, and reads are cheap. 4 bounds the worst case at four state copies
/// while leaving the service responsive. Override for a writer with more
/// headroom; `0` is rejected (it would wedge the service) and falls back here.
pub const TIER_MAX_INFLIGHT_DEFAULT: usize = 4;

/// Resolve the in-flight cap. Unparsable or zero ⇒ the default, because a typo
/// must not silently remove the bound this exists to impose.
pub fn tier_max_inflight() -> usize {
    parse_max_inflight(std::env::var(TIER_MAX_INFLIGHT_ENV).ok().as_deref())
}

/// [`tier_max_inflight`] as a **pure function of the raw value**.
///
/// ⚠ Split from the env read on purpose. The first version of the fail-safe
/// test re-implemented this parse inline instead of calling it, so deleting
/// `.filter(|n| *n > 0)` from the real function left the test green — a
/// decorative test that proved only that I can write the same expression
/// twice. `cargo test` does not serialise tests, so exercising the real
/// function meant making the decision testable without the process env.
pub fn parse_max_inflight(raw: Option<&str>) -> usize {
    raw.and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(TIER_MAX_INFLIGHT_DEFAULT)
}

/// Accept loop. Runs until the task is dropped.
pub async fn serve_tier(listener: TcpListener) {
    let limit = tier_max_inflight();
    let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(limit));
    tracing::info!(
        max_inflight = limit,
        "EHDB tier service: concurrency bounded (noetl/ai-meta#332 writer OOM)"
    );
    loop {
        // ⚠ Acquired BEFORE accept, deliberately. Acquiring after would accept
        // every connection and then block — the connections still exist, the
        // tasks still exist, and the only thing bounded would be how many run
        // at once. Taking the permit first leaves excess clients in the kernel
        // accept backlog, which is where backpressure belongs.
        let permit = match std::sync::Arc::clone(&permits).acquire_owned().await {
            Ok(p) => p,
            // The semaphore is never closed; if that changes, stop rather than
            // silently reverting to an unbounded loop.
            Err(_) => {
                tracing::error!("EHDB tier service: permit source closed; accept loop stopping");
                return;
            }
        };
        match listener.accept().await {
            Ok((stream, _peer)) => {
                tokio::spawn(async move {
                    let _permit = permit;
                    serve_conn(stream).await;
                });
            }
            Err(e) => {
                drop(permit);
                record_conn("accept_error", false, true);
                tracing::warn!(error = %e, "EHDB tier service: accept failed");
                // Yield rather than spin if the listener is in a bad state.
                tokio::task::yield_now().await;
            }
        }
    }
}

/// The reconnect-burst bound, proven rather than asserted.
///
/// These drive the REAL accept loop over a real socket, because the bound lives
/// in the accept loop and a test that called `serve_conn` directly would prove
/// nothing about it.
#[cfg(test)]
mod reconnect_burst_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A stand-in for the tier service's accept loop with the same bound, used
    /// to observe concurrency directly.
    ///
    /// ⚠ It mirrors `serve_tier`'s shape — permit acquired BEFORE accept — and
    /// `the_real_accept_loop_takes_its_permit_before_accepting` holds the two in
    /// agreement, so this cannot drift into testing a different algorithm than
    /// the one that ships.
    async fn bounded_accept_loop(
        listener: tokio::net::TcpListener,
        limit: usize,
        live: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        hold: std::time::Duration,
    ) {
        let permits = Arc::new(tokio::sync::Semaphore::new(limit));
        loop {
            let permit = match Arc::clone(&permits).acquire_owned().await {
                Ok(p) => p,
                Err(_) => return,
            };
            match listener.accept().await {
                Ok((mut stream, _)) => {
                    let live = Arc::clone(&live);
                    let peak = Arc::clone(&peak);
                    tokio::spawn(async move {
                        let _permit = permit;
                        let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        // Stand in for an append: the expensive thing whose
                        // concurrency is what must be bounded.
                        tokio::time::sleep(hold).await;
                        let _ = stream.write_all(b"ok").await;
                        let _ = stream.shutdown().await;
                        live.fetch_sub(1, Ordering::SeqCst);
                    });
                }
                Err(_) => {
                    drop(permit);
                    return;
                }
            }
        }
    }

    /// ⭐ Reproduces the production trigger: KEDA scales the worker pool 1→20 and
    /// every worker reconnects at once. Before the bound, that was 20 concurrent
    /// appends, each costing a copy of the tier's entire in-memory state.
    #[tokio::test]
    async fn a_mass_reconnect_cannot_exceed_the_inflight_bound() {
        const LIMIT: usize = 4;
        const CLIENTS: usize = 20; // the real KEDA ceiling
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn(bounded_accept_loop(
            listener,
            LIMIT,
            Arc::clone(&live),
            Arc::clone(&peak),
            std::time::Duration::from_millis(60),
        ));

        let mut clients = Vec::new();
        for _ in 0..CLIENTS {
            clients.push(tokio::spawn(async move {
                let mut s = tokio::net::TcpStream::connect(addr).await.expect("connect");
                let mut buf = Vec::new();
                let _ = s.read_to_end(&mut buf).await;
                buf.len()
            }));
        }
        let mut served = 0usize;
        for c in clients {
            if c.await.unwrap() > 0 {
                served += 1;
            }
        }
        server.abort();

        let observed = peak.load(Ordering::SeqCst);
        // The load-bearing half: every client was SERVED. A bound that works by
        // dropping connections is not backpressure, it is an outage.
        assert_eq!(
            served, CLIENTS,
            "backpressure must delay clients, not drop them: {served}/{CLIENTS} served"
        );
        assert!(
            observed <= LIMIT,
            "concurrency exceeded the bound: peak={observed} limit={LIMIT}"
        );
        // ⭐ And the burst must actually have been a burst — if the clients
        // arrived one at a time, peak would be 1 and this test would pass
        // against a completely unbounded server.
        assert!(
            observed > 1,
            "peak concurrency was {observed}; the clients did not overlap, so this \
             test could not have detected an unbounded loop"
        );
    }

    /// ⭐ POSITIVE CONTROL — the same burst with the bound REMOVED must exceed it.
    ///
    /// Without this, the test above passes on a machine that simply never runs
    /// the clients concurrently, and the bound would be untested.
    #[tokio::test]
    async fn without_the_bound_the_same_burst_blows_past_it() {
        const CLIENTS: usize = 20;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        // LIMIT = CLIENTS is "effectively unbounded" for this burst — the shape
        // the accept loop had before this change.
        let server = tokio::spawn(bounded_accept_loop(
            listener,
            CLIENTS,
            Arc::clone(&live),
            Arc::clone(&peak),
            std::time::Duration::from_millis(60),
        ));
        let mut clients = Vec::new();
        for _ in 0..CLIENTS {
            clients.push(tokio::spawn(async move {
                let mut s = tokio::net::TcpStream::connect(addr).await.expect("connect");
                let mut buf = Vec::new();
                let _ = s.read_to_end(&mut buf).await;
            }));
        }
        for c in clients {
            c.await.unwrap();
        }
        server.abort();
        let observed = peak.load(Ordering::SeqCst);
        assert!(
            observed > 4,
            "the unbounded arm only reached {observed} concurrent — the fixture \
             cannot produce a burst, so the bounded arm proves nothing"
        );
    }

    /// The cap is fail-safe: a typo must not silently remove the bound.
    /// ⚠ Drives the REAL `parse_max_inflight`. An earlier version re-implemented
    /// the parse inline and therefore survived deleting the fail-safe filter
    /// from the shipped function (battery arm B2). Calling the real thing is
    /// the whole point.
    #[test]
    fn an_unusable_cap_falls_back_to_the_default_rather_than_unbounded() {
        for raw in [Some(""), Some("0"), Some("abc"), Some("-1"), Some("1.5"), None] {
            assert_eq!(
                parse_max_inflight(raw),
                TIER_MAX_INFLIGHT_DEFAULT,
                "{raw:?} must fall back to the default cap, never widen the bound"
            );
        }
        assert_eq!(parse_max_inflight(Some("8")), 8, "a usable override must be honoured");
        assert_eq!(parse_max_inflight(Some(" 8 ")), 8, "whitespace must not defeat the override");
        assert!(
            TIER_MAX_INFLIGHT_DEFAULT > 0,
            "the default must itself be a usable bound — 0 would wedge the service"
        );
    }

    /// ⚠ Holds the shipped loop and the test double in agreement on the one
    /// property that matters: the permit is taken BEFORE `accept`. Acquiring
    /// after would accept everything and bound only how many run at once, which
    /// leaves the connection and task count unbounded — the same failure.
    #[test]
    fn the_real_accept_loop_takes_its_permit_before_accepting() {
        let src = include_str!("tier_service.rs");
        let body = src
            .split("pub async fn serve_tier(")
            .nth(1)
            .expect("serve_tier not found — this guard is anchored to a renamed fn");
        let accept = body.find("listener.accept()").expect("no accept in serve_tier");
        // ⚠ The AWAITED form specifically. `try_acquire_owned()` contains the
        // substring `acquire_owned`, so an earlier version of this guard was
        // satisfied by a non-blocking acquisition that silently drops the bound
        // (battery arm B1): `try_` returns Err when saturated rather than
        // waiting, which under this loop means falling through unbounded.
        let acquire = body
            .find(".acquire_owned().await")
            .expect("serve_tier must AWAIT its permit; a non-blocking acquire is not backpressure");
        assert!(
            !body[..accept].contains("try_acquire"),
            "serve_tier uses a non-blocking try_acquire before accept; that does \
             not wait when saturated, so it bounds nothing"
        );
        assert!(
            acquire < accept,
            "serve_tier acquires its permit AFTER accept; backpressure must happen \
             before the connection is taken off the backlog"
        );
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
        let reply = read_frame(&mut c, MAX_REPLY_BYTES).await.unwrap().expect("a reply frame");
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
        // ⚠ Bounded — see the note in the request-cap test. An unbounded read
        // here turns "the cap was widened" from a failure into a hang.
        let n = tokio::time::timeout(std::time::Duration::from_secs(5), c.read(&mut buf))
            .await
            .expect(
                "the server neither closed nor answered — it accepted a length prefix above \
                 the request cap and is now waiting for a body that will never arrive. \
                 Unbounded, this read HANGS the suite instead of failing it.",
            )
            .unwrap_or(0);
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
        metrics::pin_tier_service_series(&[]);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_tier(listener));

        let mut c = TcpStream::connect(addr).await.unwrap();
        write_frame(&mut c, b"health").await.unwrap();
        let _ = read_frame(&mut c, MAX_REPLY_BYTES).await.unwrap().expect("a reply frame");

        let text = metrics::render_lines().join("\n");
        // What this test performed must be counted AT LEAST once. See
        // `series_present` for why these are not exact-value assertions.
        let health = "noetl_ehdb_dataplane_ops_total{operation=\"tier_service.health\",outcome=\"ok\"}";
        assert!(
            series_value(&text, health).unwrap_or(0) >= 1,
            "health must be counted:\n{text}"
        );
        assert!(
            series_value(
                &text,
                "noetl_ehdb_tier_service_duration_seconds_count{operation=\"health\"}"
            )
            .unwrap_or(0)
                >= 1,
            "health must be timed:\n{text}"
        );
        assert!(
            series_value(
                &text,
                "noetl_ehdb_dataplane_ops_total{operation=\"tier_service.conn\",outcome=\"accepted\"}"
            )
            .unwrap_or(0)
                >= 1,
            "the connection must be counted:\n{text}"
        );
        // The negative half, restored to the exact value it was written as.
        // It was weakened to a presence check by noetl/worker#299, because a
        // sibling test module recording into the shared state could land a
        // count inside this window. `metrics::test_guard` now gives each test
        // thread its OWN state, so nothing else can contribute here and the
        // stronger property — an operation nobody performed reads exactly 0,
        // rather than being absent — is assertable again. That property is the
        // entire reason `pin_tier_service_series` exists.
        assert_eq!(
            series_value(
                &text,
                "noetl_ehdb_dataplane_ops_total{operation=\"tier_service.append\",outcome=\"ok\"}"
            ),
            Some(0),
            "an unserved op must read 0, not be absent:\n{text}"
        );
        metrics::reset();
    }

    /// Whether a metric series is PRESENT in the exposition, at any value.
    ///
    /// Kept for the checks that genuinely only care about presence. The
    /// weakened-assertion workaround this used to carry is gone:
    /// `metrics::test_guard` now hands each test thread its own state
    /// (noetl/worker#302), so exact values are attributable again and the
    /// assertions above say `== 0` / `>= 1` as they were written to.
    #[allow(dead_code)]
    fn series_present(text: &str, series: &str) -> bool {
        text.lines().any(|l| l.starts_with(series))
    }

    /// The value of a series, if present.
    fn series_value(text: &str, series: &str) -> Option<u64> {
        text.lines()
            .find(|l| l.starts_with(series))
            .and_then(|l| l.rsplit(' ').next())
            .and_then(|v| v.parse().ok())
    }

    /// A protocol violation is counted as one, and NOT as a served request.
    /// Folding it into the request counters would make a client speaking the
    /// wrong protocol look like healthy traffic.
    #[tokio::test]
    async fn a_protocol_error_is_counted_separately_from_a_request() {
        let _guard = metrics::test_guard();
        metrics::reset();
        metrics::pin_tier_service_series(&[]);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_tier(listener));

        let mut c = TcpStream::connect(addr).await.unwrap();
        c.write_all(&(MAX_FRAME_BYTES + 1).to_be_bytes())
            .await
            .unwrap();
        c.flush().await.unwrap();
        let mut buf = [0u8; 1];
        // ⚠ Bounded, same reason.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), c.read(&mut buf))
            .await
            .expect(
                "the server neither closed nor answered — it accepted a length prefix above \
                 the request cap and is now waiting for a body that will never arrive. \
                 Unbounded, this read HANGS the suite instead of failing it.",
            )
            .unwrap_or(0);

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
            tier: StoreTier::EventLog,
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

    // ---------------------------------------------------------------------
    // noetl/ai-meta#343 blocker 2 — the codec must not be asymmetric.
    // ---------------------------------------------------------------------

    /// ⭐⭐ **The property #311 got wrong: a writer must validate against the cap
    /// its READER uses — which depends on DIRECTION.**
    ///
    /// #311 raised the reply cap to 16 MiB and made `write_frame` validate
    /// against it for both directions. A client could then write a 1.25 MB
    /// *request*, pass the local check, and have the service refuse it at its
    /// 1 MiB request cap and close the socket. Prod, 2026-09-13: 48
    /// `protocol error … frame of 1251646 bytes exceeds the 1048576-byte cap`,
    /// and the mirror dropped those batches **permanently** — no retry can fix a
    /// frame the peer structurally cannot read.
    ///
    /// One cap for two directions is the same mistake as one cap for two roles.
    #[tokio::test]
    async fn a_request_is_bounded_by_the_request_cap_not_the_reply_cap() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut c = TcpStream::connect(addr).await.unwrap();

        // Between the two caps: legal as a reply, ILLEGAL as a request.
        let between = vec![b'x'; (MAX_FRAME_BYTES as usize) + 1];
        assert!(
            between.len() < MAX_REPLY_BYTES as usize,
            "fixture must sit BETWEEN the caps or it cannot tell them apart"
        );

        let err = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            write_request_frame(&mut c, &between),
        )
        .await
        .expect("write_request_frame blocked instead of refusing")
        .expect_err(
            "a request above the REQUEST cap was accepted locally — it will be \
             refused by the service as a connection reset, dropped permanently, \
             and counted nowhere",
        );
        assert!(
            err.to_string().contains("request frame"),
            "the error must name the direction; got {err}"
        );

        // Positive control: the SAME payload is a legal reply.
        //
        // ⚠ It needs a DRAINING reader. A >1 MB write to a socket nobody reads
        // fills the kernel buffer and blocks, so without this the control times
        // out and "proves" the payload is illegal in both directions — which
        // would make the whole test about size rather than direction.
        let (mut srv, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let mut sink = vec![0u8; 1 << 16];
            while srv.read(&mut sink).await.unwrap_or(0) > 0 {}
        });
        let mut c2 = TcpStream::connect(addr).await.unwrap();
        let (mut srv2, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let mut sink = vec![0u8; 1 << 16];
            while srv2.read(&mut sink).await.unwrap_or(0) > 0 {}
        });
        let ok = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            write_reply_frame(&mut c2, &between),
        )
        .await
        .expect("write_reply_frame blocked even with a draining reader");
        assert!(
            ok.is_ok(),
            "a payload between the caps must be a LEGAL reply — otherwise this \
             test proves nothing about direction, only about size: {ok:?}"
        );
    }

    /// The client must use the request writer, and the service the reply writer.
    ///
    /// The behavioural test above covers the helpers; this covers the CALL
    /// SITES, which is where #311's mistake actually lived. A mutation swapping
    /// them back survives everything else.
    #[test]
    fn each_side_writes_with_its_own_direction() {
        let code_only = |src: &str| -> String {
            src.lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let client = code_only(include_str!("tier_client.rs"));
        assert!(
            client.contains("write_request_frame(&mut s, payload)"),
            "the client does not write REQUEST frames — it will validate against \
             the reply cap and emit frames the service refuses"
        );
        assert!(
            !client.contains("write_frame(&mut s, payload)"),
            "the client still calls the uncapped/reply-capped writer"
        );

        let svc = include_str!("tier_service.rs");
        let svc_code = code_only(svc.split("\n#[cfg(test)]").next().unwrap_or(svc));
        assert!(
            svc_code.contains("write_reply_frame(&mut stream, &resp)"),
            "serve_conn does not write REPLY frames"
        );
    }

    /// ⭐ **The defect, stated as a property.** Anything this codec can WRITE,
    /// it must be able to READ.
    ///
    /// Until #343 the write side checked only u32 overflow while the read side
    /// enforced 1 MiB, so the tier service would serialise query replies its own
    /// client structurally could not read — deterministically, for every
    /// execution over the cap, not intermittently. Prod surfaced it as
    /// `read: tier-service frame of 1174874 bytes exceeds the 1048576-byte cap`,
    /// and the affected executions left the parity denominator rather than
    /// being reported as divergent.
    ///
    /// A round-trip is the honest test: a unit test asserting two constants are
    /// equal would pass against a codec where the writer ignored its own cap.
    #[tokio::test]
    async fn anything_the_codec_can_write_it_can_read_back() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Echo one frame straight back, so only the codec is under test.
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let f = read_frame(&mut s, MAX_REPLY_BYTES).await.unwrap().unwrap();
            write_frame(&mut s, &f).await.unwrap();
        });

        // Larger than the OLD shared cap, which is the size prod actually hit.
        let payload = vec![b'x'; 1_174_874];
        assert!(
            payload.len() > MAX_FRAME_BYTES as usize,
            "the fixture must exceed the old cap or it proves nothing"
        );
        let mut c = TcpStream::connect(addr).await.unwrap();
        write_frame(&mut c, &payload).await.unwrap();
        let back = read_frame(&mut c, MAX_REPLY_BYTES)
            .await
            .expect("a 1.17MB frame must round-trip — this is the #343 payload size")
            .expect("a reply frame");
        assert_eq!(back.len(), payload.len());
    }

    /// The request cap is NOT raised. It bounds an untrusted length prefix, and
    /// widening it to match the reply cap would let one bad frame ask for 16 MiB
    /// before a byte of body has arrived.
    #[tokio::test]
    async fn the_request_cap_stays_narrow_even_though_replies_may_be_large() {
        // ⚠ This test drives `serve_tier`, which records `conn` outcomes into
        // the PROCESS-WIDE metrics state. Without the guard it races
        // `a_protocol_error_is_counted_separately_from_a_request`, which
        // asserts `protocol_error == 1` exactly — and since this test also
        // produces a protocol error, that assertion saw 2.
        //
        // Omitting it made the suite fail 3 runs in 5. A 60%-red baseline makes
        // EVERY mutant read CAUGHT, which is the specific way two earlier
        // mutation batteries in this programme were thrown away.
        let _guard = metrics::test_guard();
        assert!(
            MAX_REPLY_BYTES > MAX_FRAME_BYTES,
            "replies must be allowed to exceed requests, or #343 is not fixed"
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_tier(listener));

        let mut c = TcpStream::connect(addr).await.unwrap();
        // A length prefix between the two caps: legal as a reply, illegal as a
        // request. The server must still refuse it.
        c.write_all(&(MAX_FRAME_BYTES + 1).to_be_bytes())
            .await
            .unwrap();
        c.flush().await.unwrap();
        // ⚠ Bounded. If the server accepts the over-long prefix it then blocks
        // in `read_exact` waiting for a body that never arrives, and this read
        // blocks with it — the mutation would HANG the suite instead of failing
        // it, which is not a catch.
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(std::time::Duration::from_secs(5), c.read(&mut buf))
            .await
            .expect(
                "the server neither closed nor answered — it accepted a length prefix above \
                 the request cap and is waiting for the body, which is the widening this \
                 test exists to refuse",
            )
            .unwrap_or(0);
        assert_eq!(
            n, 0,
            "the request cap was widened along with the reply cap — an untrusted \
             length prefix can now claim {MAX_REPLY_BYTES} bytes"
        );
    }

    /// A frame over even the reply cap must be REFUSED at the writer, not
    /// emitted for the far end to choke on.
    ///
    /// This is what turns the remaining cliff from silent into countable: the
    /// side that caused the failure is the side that records it.
    #[tokio::test]
    async fn the_writer_refuses_to_emit_a_frame_no_reader_would_accept() {
        // ⚠ Deliberately never accepted, and the listener is kept in scope
        // rather than moved into a task. `write_frame` must refuse BEFORE any
        // I/O, so the peer's behaviour is irrelevant to what is under test.
        //
        // The first draft leaked the accepted socket with `std::mem::forget`.
        // A forgotten `TcpStream` stays registered with the tokio reactor, and
        // the runtime's shutdown then blocks waiting for a resource nothing
        // will ever release — so this test PASSED when run alone and HUNG under
        // `cargo test`, holding the whole mutation battery at 0% CPU for twenty
        // minutes while reading, from the outside, exactly like a slow compile.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut c = TcpStream::connect(addr).await.unwrap();
        let too_big = vec![b'x'; MAX_REPLY_BYTES as usize + 1];
        // ⚠ Bounded. The refusal happens BEFORE any I/O, so a correct
        // `write_frame` returns instantly — but a broken one blocks forever
        // filling a socket nobody drains. Without this bound the mutation that
        // deletes the guard makes the test HANG rather than fail, which is not
        // a catch: it stalled a mutation battery at 0% CPU for twenty minutes,
        // indistinguishable from a slow compile. A mutant must produce a
        // FAILURE, not a stall.
        let err = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            write_reply_frame(&mut c, &too_big),
        )
        .await
        .expect("write_reply_frame blocked on a frame it should have refused outright")
        .expect_err("writing an unreadable frame must fail at the writer");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("refusing to write"),
            "the error must name the cause; got {err}"
        );
        drop(listener);
    }

    /// Absence and zero must not read alike for the new outcome either.
    ///
    /// A mutation removing `reply_too_large` from the pinned list SURVIVED the
    /// first battery — the counter existed and nothing asserted it was visible
    /// before it first fired. On a healthy writer it never fires, which is
    /// precisely when an operator needs to see it sitting at 0 rather than
    /// wonder whether the build has it at all.
    #[tokio::test]
    async fn the_oversized_reply_outcome_is_pinned_at_zero() {
        let _guard = metrics::test_guard();
        metrics::reset();
        metrics::pin_tier_service_series(&[]);
        let text = metrics::render_lines().join("\n");
        assert!(
            text.contains(
                "noetl_ehdb_dataplane_ops_total{operation=\"tier_service.conn\",outcome=\"reply_too_large\"} 0"
            ),
            "reply_too_large is not pinned — it will be ABSENT from /metrics until \
             the first over-large reply, and absent reads as zero:\n{text}"
        );
        // Negative control: an outcome nobody defined must not appear, or the
        // assertion above would pass against any text at all.
        assert!(
            !text.contains("outcome=\"nonsense_outcome\""),
            "an undefined outcome is present — this check cannot distinguish \
             pinned from unpinned"
        );
        metrics::reset();
    }

    /// An over-large reply is ANSWERED, not dropped.
    ///
    /// The caller has to be able to tell "this execution is unreadable" from
    /// "this execution is absent". Before #343 it could not: the client saw a
    /// `read:` protocol error naming no execution, nothing was counted on this
    /// side, and the execution silently left every parity denominator. The
    /// distinction is the whole point — an absent execution is a mirror gap to
    /// repair, an unreadable one is a transport limit to page around.
    #[test]
    fn an_oversized_reply_is_answered_with_an_error_that_fits() {
        // The substituted reply must itself be under the cap, or the fix
        // reproduces the bug it fixes.
        let resp = vec![b'x'; MAX_REPLY_BYTES as usize + 1];
        let substitute = format!(
            "err reply of {} bytes exceeds the {MAX_REPLY_BYTES}-byte frame cap; \
             the execution is UNREADABLE, not absent — page the query",
            resp.len()
        );
        assert!(
            substitute.len() < MAX_REPLY_BYTES as usize,
            "the too-large reply is itself too large"
        );
        assert!(
            substitute.starts_with("err "),
            "the substitute must be an error reply the client already parses"
        );
        assert!(
            substitute.contains("UNREADABLE, not absent"),
            "the reply must distinguish unreadable from absent, which is the \
             distinction that was lost"
        );

        // …and the serve path must actually install it. Structural, because
        // driving a >16MiB reply through a real socket in a unit test is slower
        // than the property is worth.
        let src = include_str!("tier_service.rs");
        let code = src.split("\n#[cfg(test)]").next().unwrap_or(src);
        let start = code.find("async fn serve_conn").expect("serve_conn not found");
        let body = &code[start..];
        let end = body.find("\n}\n").map(|i| i + 3).unwrap_or(body.len());
        let body: String = body[..end]
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            body.contains("resp.len() > MAX_REPLY_BYTES as usize"),
            "serve_conn does not check the reply size before writing it"
        );
        assert!(
            body.contains("reply_too_large"),
            "serve_conn does not record the over-large reply — an invisible \
             cliff is the defect, not the size limit itself"
        );
    }
}
