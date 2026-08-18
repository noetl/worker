//! HTTP server exposing `/metrics` in Prometheus text format.
//!
//! Per [`agents/rules/observability.md`][rule], the worker's
//! `/metrics` endpoint binds on a dedicated port (default `9090`)
//! so sidecar scrapers can pull without going through the main
//! control-plane traffic.  The endpoint is read-only and has no
//! authentication — it's expected to be exposed only inside the
//! cluster network (Kubernetes Service with `ClusterIP` and
//! `PodMonitor`-restricted access).
//!
//! Two routes:
//! - `GET /metrics` — Prometheus text-format snapshot of the
//!   global [`crate::metrics::WorkerMetrics`] registry.
//! - `GET /healthz` — 200 OK (liveness check for Kubernetes).
//!
//! The spawn function returns immediately after `axum::serve` is
//! armed; the caller decides when to drop the join handle (the
//! worker keeps it for the worker's lifetime).
//!
//! [rule]: https://github.com/noetl/ai-meta/blob/main/agents/rules/observability.md

use anyhow::Result;
use axum::{
    extract::{Path, Query},
    http::{header, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::task::JoinHandle;

use crate::ehdb::query::{run_query, QueryParams, QueryTier};
use crate::metrics::{WorkerMetrics, METRICS_CONTENT_TYPE};

/// Spawn the metrics HTTP server in a background task.
///
/// Returns the join handle so the caller can decide when to shut
/// down the server.  Errors during bind are returned synchronously
/// before the server starts accepting connections.
pub async fn spawn(bind: &str) -> Result<JoinHandle<()>> {
    let addr: SocketAddr = bind.parse()?;

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(healthz_handler))
        .route("/readyz", get(readyz_handler))
        .route("/livez", get(livez_handler))
        // EHDB Data Query Interface — the data-plane read handler the server
        // relays raw tier queries to (noetl/ai-meta#178).  Read-only, disabled by
        // default (no-op until `NOETL_EHDB_ENABLED`), reachable in-cluster via
        // the existing worker metrics/query service on :9090.
        // GET reads the tier; POST appends to it (noetl/ai-meta#258).  Both on
        // the SAME route deliberately — see `ehdb_tier_append_handler` for why
        // the write must resolve its store exactly the way the read does.
        .route(
            "/ehdb/tiers/{tier}",
            get(ehdb_tier_query_handler).post(ehdb_tier_append_handler),
        );

    // ai-meta#257 P0 — pin the serve-decision series at the bind site of the
    // route that carries server-authored appends, so `served_primary 0` is
    // readable instead of the series being absent. A pod that comes up under
    // `primary` while the tier service is down would otherwise render nothing at
    // all, which is exactly what made the inert flip look like a missing metric.
    // Gated so a disabled build's `/metrics` stays byte-identical; both `shadow`
    // and `primary` pin, so the flip changes values and not which series exist.
    {
        use crate::ehdb::eventlog::EventLogMode;
        let env = crate::ehdb::process_env();
        // `enabled_from_env`, not a second copy of the truthiness rule — two
        // readers of one flag are two chances to disagree about it.
        if crate::ehdb::contract::enabled_from_env(&env)
            && EventLogMode::from_env(&env) != EventLogMode::Off
        {
            crate::ehdb::metrics::pin_eventlog_serve_series();
        }
    }

    // noetl/ai-meta#155 — pin the closed pickup-phase label set so both series
    // read 0 on a worker that has not yet claimed, instead of being absent and
    // indistinguishable from a binary that lacks the metric.
    crate::metrics::pin_command_pickup_phases();

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let actual_addr = listener.local_addr()?;

    tracing::info!(
        bind = %actual_addr,
        "Metrics HTTP server listening at http://{actual_addr}/metrics + /healthz + /readyz"
    );

    let handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "Metrics HTTP server stopped");
        }
    });

    Ok(handle)
}

/// `GET /metrics` — encode the global registry and return as
/// Prometheus text format.
///
/// The EHDB integration's process-local metric families
/// ([`crate::ehdb::metrics`]) are appended after the registry snapshot.  They
/// render nothing until a non-disabled EHDB op has run, so a disabled EHDB
/// build (the default) produces byte-identical output.
async fn metrics_handler() -> impl IntoResponse {
    let mut body = WorkerMetrics::global().encode();
    let ehdb_lines = crate::ehdb::metrics::render_lines();
    if !ehdb_lines.is_empty() {
        body.extend_from_slice(ehdb_lines.join("\n").as_bytes());
        body.push(b'\n');
    }
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static(METRICS_CONTENT_TYPE),
        )],
        body,
    )
}

/// `GET /ehdb/tiers/{tier}` — read-only EHDB data-plane tier query.
///
/// The worker-side half of the EHDB Data Query Interface (noetl/ai-meta#178).
/// The server (control plane) makes a synchronous read request straight here
/// rather than enqueuing on the NATS drive — a query is a data-plane read, not a
/// unit of playbook work.  The handler resolves + guards the data-plane contract
/// from the process env, dispatches to the tier driver's read method, and returns
/// the tier `*Outcome` (already `Serialize` + secret-free).  Disabled by default:
/// with `NOETL_EHDB_ENABLED` unset it returns a `disabled` no-op body.
async fn ehdb_tier_query_handler(
    Path(tier): Path<String>,
    Query(raw): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let Some(tier) = QueryTier::parse(&tier) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "action": "ehdb.tier.query",
                "error": "unknown tier",
                "known_tiers": ["eventlog", "kv", "object", "vector"],
            })),
        );
    };
    let params = QueryParams::from_pairs(raw.iter());

    // ai-meta#257 PR 4 — resolve against the writer-fronted tier service when the
    // operator asked for it.  Default (`local`) is byte-identical to the previous
    // behaviour: this pod's own store.
    //
    // The server is NOT involved: it keeps its control-plane guard and still only
    // relays to this route.  All that changes is which store this hop reads.
    let env = crate::ehdb::process_env();
    let resolution = crate::ehdb::tier_query_source::resolve(&env);
    let source_label = resolution.label();
    let source_addr = resolution.addr().map(str::to_string);
    crate::ehdb::metrics::record_tier_query_source("read", source_label);
    // ai-meta#257 P0 — the event-log serve state, for the reply. Only for the
    // event-log tier: it is that tier's state, and a bare `serve_state` on a `kv`
    // body would describe the wrong thing to a reader who has no way to tell.
    let serve_state = matches!(tier, QueryTier::Eventlog)
        .then(crate::ehdb::eventlog::current_serve_state);

    use crate::ehdb::tier_query_source::Resolution;
    match &resolution {
        Resolution::DowngradedToLocal => {
            // Asking for `service` and silently answering from a different store is
            // the exact failure this effort exists to remove, so say so.
            tracing::warn!(
                "NOETL_EHDB_TIER_QUERY_SOURCE=service but no tier-service address is \
                 configured; falling back to the pod-local store"
            );
        }
        Resolution::Misconfigured(reason) => {
            // Fail loud.  Falling through to the local read here would answer
            // from a store the operator did not ask for, in a body that looks
            // exactly like a correct one — and with N replicas that body is a
            // fragment of the tier, not the tier.
            tracing::error!(%reason, "EHDB tier query: refusing to answer from a different store");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(stamp_source(
                    serde_json::json!({
                        "action": "ehdb.tier.query",
                        "outcome": "unavailable",
                        "error": reason,
                    }),
                    source_label,
                    source_addr.as_deref(),
                    serve_state,
                )),
            );
        }
        Resolution::Service(client) => {
            // The remote store backs the event log only (`tier_store`), so any
            // other tier would be answered with event-log records under a `kv` /
            // `object` / `vector` label.  Refuse instead: a wrong-tier answer is
            // worse than no answer, and it would be scored as data.
            if !matches!(tier, QueryTier::Eventlog) {
                return (
                    StatusCode::NOT_IMPLEMENTED,
                    Json(stamp_source(
                        serde_json::json!({
                            "action": "ehdb.tier.query",
                            "outcome": "unsupported_tier",
                            "error": format!(
                                "the tier service serves the eventlog tier only; the {} \
                                 tier must be read with NOETL_EHDB_TIER_QUERY_SOURCE=local",
                                tier.as_str()
                            ),
                        }),
                        source_label,
                        source_addr.as_deref(),
                        serve_state,
                    )),
                );
            }
            // `execution`, NOT `execution_id`: the latter is documented in
            // QueryParams as a tracing correlation id, and reading by it would
            // silently query the wrong key.
            let reply = match params.execution.as_deref() {
                Some(eid) => client.read_execution(eid).await,
                None => client.scan(None, 100).await,
            };
            return match reply {
                Ok(body) => match serde_json::from_str::<serde_json::Value>(&body) {
                    Ok(v) => (
                        StatusCode::OK,
                        Json(stamp_source(v, source_label, source_addr.as_deref(), serve_state)),
                    ),
                    // A non-JSON reply is one of the service's typed refusals
                    // (`unavailable` / `invalid` / `error`).  Surface it as-is
                    // rather than as an empty 200, which would read as "no data".
                    Err(_) => (
                        StatusCode::BAD_GATEWAY,
                        Json(stamp_source(
                            serde_json::json!({
                                "action": "ehdb.tier.query",
                                "outcome": "unavailable",
                                "error": body,
                            }),
                            source_label,
                            source_addr.as_deref(),
                            serve_state,
                        )),
                    ),
                },
                Err(e) => (
                    StatusCode::BAD_GATEWAY,
                    Json(stamp_source(
                        serde_json::json!({
                            "action": "ehdb.tier.query",
                            "outcome": "unavailable",
                            "error": e,
                        }),
                        source_label,
                        source_addr.as_deref(),
                        serve_state,
                    )),
                ),
            };
        }
        Resolution::Local => {}
    }

    // The driver reads are synchronous, bounded, filesystem-backed opens; run
    // them on the blocking pool so a scan never stalls the metrics reactor.
    let result = tokio::task::spawn_blocking(move || run_query(&env, tier, &params))
        .await
        .map(|r| (r.outcome.http_status(), r.body))
        .unwrap_or_else(|e| {
            (
                500,
                serde_json::json!({
                    "action": "ehdb.tier.query",
                    "outcome": "unavailable",
                    "error": format!("query task join error: {e}"),
                }),
            )
        });
    let status = StatusCode::from_u16(result.0).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (
        status,
        Json(stamp_source(result.1, source_label, source_addr.as_deref(), serve_state)),
    )
}

/// Stamp which store answered onto a tier reply.
///
/// **This is what makes the flag observable from outside the process.** The
/// local and the service bodies are the same shape, so without this field a
/// reader cannot tell a working service path from a silent fall-back to local —
/// and with more than one worker replica the local answer is a *fragment* that
/// reads exactly like a complete one. Every gate for
/// [ai-meta#257](https://github.com/noetl/ai-meta/issues/257) PR 4 discriminates
/// on this field.
///
/// A non-object body is wrapped rather than dropped: losing a reply to keep a
/// label would be the wrong trade, and the wrapper keeps the original under
/// `body` where it is still readable.
fn stamp_source(
    v: serde_json::Value,
    source: &str,
    addr: Option<&str>,
    serve_state: Option<&str>,
) -> serde_json::Value {
    let mut v = match v {
        serde_json::Value::Object(_) => v,
        other => serde_json::json!({ "body": other }),
    };
    if let Some(obj) = v.as_object_mut() {
        obj.insert(
            "tier_query_source".to_string(),
            serde_json::Value::String(source.to_string()),
        );
        // The serve state, on the READ, at the endpoint level (ai-meta#257 P0).
        //
        // The append decides it; the read is where a gate — or an operator — can
        // see it per replica without scraping a metric. That matters more than it
        // sounds: the P0 was found by a gate asserting a metric delta, and the
        // family turned out to be ABSENT, which reads the same as a build that
        // never had it. A field in the body cannot be absent for that reason.
        //
        // `unknown` until this pod has decided anything, which is a different
        // statement from any of the four decisions and must not be confused with
        // `not_primary`.
        //
        // `None` for every tier that is not the event log, and that is not
        // fastidiousness: the serve state is the EVENT LOG's, and stamping it onto
        // a `kv` or `object` reply would put a field on a body it does not
        // describe. A reader has no way to tell which tier a bare `serve_state`
        // belongs to, which is the whole class of defect this playbook is about.
        if let Some(state) = serve_state {
            obj.insert(
                "serve_state".to_string(),
                serde_json::Value::String(state.to_string()),
            );
        }
        if let Some(a) = addr {
            obj.insert(
                "tier_service_addr".to_string(),
                serde_json::Value::String(a.to_string()),
            );
        }
    }
    v
}

/// `POST /ehdb/tiers/{tier}` — append records the **server** authored.
///
/// The write half of the closure in noetl/ai-meta#258. The event-log tier could
/// only ever hold the worker-emitted subset of the log, because the mirror hook
/// sits on the worker's emit chokepoint and the server authors the rest itself.
/// This is where the server's chokepoint puts the events it writes.
///
/// **It shares a route with the read on purpose, and that is a correctness
/// property rather than a tidiness one.** The GET handler resolves which store
/// answers — this pod's own log, or the writer-fronted tier service — from
/// `NOETL_EHDB_TIER_QUERY_SOURCE`. A write that resolved its store any other way
/// could land in a store the comparator does not read, and the comparator would
/// then report every server-authored event missing: a total divergence that is
/// an artefact of two different stores rather than a fact about either. Routing
/// both through one handler makes "written where it will be read" true by
/// construction instead of by matching two env vars.
///
/// The control-plane guard is untouched. The server still never opens tier
/// storage; it makes the same HTTP hop it already makes to read.
///
/// Inert by default: without `NOETL_EHDB_EVENTLOG_MIRROR_SOURCE=server` this
/// answers 501 and appends nothing, so a build carrying it behaves exactly as
/// the build before it.
async fn ehdb_tier_append_handler(
    Path(tier): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    // Only the event log. The other four tiers hold derived data with no
    // authoritative counterpart for a server to author, so accepting an append
    // for them would invent records rather than mirror any.
    if tier != "eventlog" {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "action": "ehdb.tier.append",
                "error": "append is supported for the eventlog tier only",
                "tier": tier,
            })),
        );
    }

    let env = crate::ehdb::process_env();
    let source = crate::ehdb::mirror_source::MirrorSource::from_env(&env);
    if source != crate::ehdb::mirror_source::MirrorSource::Server {
        // 501, not 403: the surface exists and the caller is entitled to it —
        // this deployment simply has not been asked to mirror server-side. The
        // server's mirror treats this as "unconfigured" and says so, the same
        // vocabulary the comparator uses for a tier it cannot read.
        return (
            StatusCode::NOT_IMPLEMENTED,
            Json(serde_json::json!({
                "action": "ehdb.tier.append",
                "outcome": "unconfigured",
                "error": format!(
                    "{}={} — this worker is not configured to accept server-authored appends",
                    crate::ehdb::mirror_source::MIRROR_SOURCE_ENV,
                    source.as_str()
                ),
            })),
        );
    }

    let execution_id = match body.get("execution_id").and_then(|v| match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }) {
        Some(e) => e,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "action": "ehdb.tier.append",
                    "error": "execution_id is required",
                })),
            )
        }
    };

    // Records arrive as already-serialised event payloads, in the order the
    // server intends them to sit in the tier. Preserving that order across this
    // hop is the whole point of sending a batch rather than N requests: the
    // comparator checks that the tier's records sit in the same relative order
    // as the authoritative log, and N concurrent requests would not.
    let records: Vec<String> = match body.get("records").and_then(|v| v.as_array()) {
        Some(arr) => arr
            .iter()
            .filter_map(|v| match v {
                serde_json::Value::String(s) => Some(s.clone()),
                other => Some(other.to_string()),
            })
            .collect(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "action": "ehdb.tier.append",
                    "error": "records must be an array",
                })),
            )
        }
    };
    if records.is_empty() {
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "action": "ehdb.tier.append",
                "outcome": "ok",
                "appended": 0,
            })),
        );
    }

    use crate::ehdb::tier_query_source::Resolution;
    let resolution = crate::ehdb::tier_query_source::resolve(&env);
    crate::ehdb::metrics::record_tier_query_source("append", resolution.label());

    let mut appended = 0usize;
    let mut failures: Vec<String> = Vec::new();

    match &resolution {
        Resolution::DowngradedToLocal => {
            tracing::warn!(
                "NOETL_EHDB_TIER_QUERY_SOURCE=service but no tier-service address is \
                 configured; server-authored appends are landing in the pod-local store"
            );
        }
        Resolution::Misconfigured(reason) => {
            // Symmetric with the read.  Writing to the pod-local store while the
            // operator believes the service holds the tier is how the two ends
            // of this route come apart — and the comparator would then report
            // every server-authored event missing, which is a true statement
            // about the wrong store.
            tracing::error!(%reason, "EHDB tier append: refusing to write to a different store");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "action": "ehdb.tier.append",
                    "outcome": "unavailable",
                    "tier_query_source": resolution.label(),
                    "error": reason,
                })),
            );
        }
        Resolution::Service(client) => {
            // ai-meta#257 P0 — THE SERVE DECISION RUNS HERE.
            //
            // This branch is the event-log tier's append chokepoint on the only
            // configuration that makes the tier correct at more than one replica
            // (`MIRROR_SOURCE=server` + `TIER_QUERY_SOURCE=service`), and before
            // this it was the one append path that never consulted
            // `primary_serve::decide`: the worker's own mirror hook is disarmed by
            // `MIRROR_SOURCE=server`, and `Resolution::Service` never enters
            // `mirror_event`, which is where the other call site lives.  A flip to
            // `primary` was therefore inert AND silent — measured in kind at three
            // replicas with 13 events per execution flowing correctly throughout.
            //
            // `serve_service_append` is not a second policy: it calls the same
            // `decide` with the same three conditions and the same outcome
            // vocabulary.  It records on every record, landed or not, so a dead
            // tier service shows as a demote rather than as a serve signal that
            // quietly stopped.
            let mut previous_sequence = 0u64;
            let mut serve_state = crate::ehdb::eventlog::current_serve_state();
            for payload in &records {
                let started = std::time::Instant::now();
                let out = client.append(&execution_id, payload).await;
                let serve = crate::ehdb::eventlog::serve_service_append(
                    &env,
                    out.as_deref().map_err(String::as_str),
                    previous_sequence,
                    started.elapsed().as_secs_f64(),
                );
                if let Some(seq) = serve.sequence {
                    previous_sequence = seq;
                }
                serve_state = serve.decision.outcome_label();
                match out {
                    Ok(_) => appended += 1,
                    Err(e) => failures.push(e),
                }
            }
            return append_reply(
                resolution.label(),
                appended,
                records.len(),
                failures,
                serve_state,
            );
        }
        Resolution::Local => {}
    }

    // Pod-local store. `mirror_event` is the same append the worker's own mirror
    // uses, so a server-authored record and a worker-authored one are written by
    // identical code into identical storage — there is no second write path that
    // could diverge in format or in sequence assignment.
    let exec = execution_id.clone();
    let recs = records.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        let env = crate::ehdb::process_env();
        let mut ok = 0usize;
        let mut errs: Vec<String> = Vec::new();
        for payload in &recs {
            let r = crate::ehdb::eventlog::mirror_event(
                &env,
                &exec,
                None,
                payload,
                &crate::ehdb::eventlog::EventLogOptions::default(),
                true,
            );
            // "Appended" means the record is in the store. `ParityMismatch` and
            // `PrimaryDivergence` are appends that landed and then disagreed
            // with something — the record exists, and the tier's own parity
            // metric already carries the disagreement, so counting them as
            // failures here would double-report one fault as two.
            //
            // `RoutedAway` is deliberately NOT in that set. It means "the owning
            // replica mirrors it", which is true of a worker-originated append
            // and false of this one: the server sent this record here and will
            // not send it anywhere else, so treating it as success would drop an
            // event silently.
            match r.outcome {
                crate::ehdb::eventlog::EventLogOutcome::Mirrored
                | crate::ehdb::eventlog::EventLogOutcome::ServedPrimary
                | crate::ehdb::eventlog::EventLogOutcome::ParityMismatch
                | crate::ehdb::eventlog::EventLogOutcome::PrimaryDivergence => ok += 1,
                other => errs.push(format!("{other:?}")),
            }
        }
        (ok, errs)
    })
    .await;

    match outcome {
        Ok((ok, errs)) => {
            appended = ok;
            failures = errs;
        }
        Err(e) => failures.push(format!("append task join error: {e}")),
    }

    append_reply(
        resolution.label(),
        appended,
        records.len(),
        failures,
        crate::ehdb::eventlog::current_serve_state(),
    )
}

/// One reply shape for both stores.
///
/// A partial append is reported as a partial append. Coercing "3 of 5 landed"
/// into a 200 with no detail would let the comparator's later `count` verdict be
/// the first anyone hears of it, at which point the cause is a store away.
///
/// `serve_state` is the serve decision's own label (`served_primary`,
/// `no_durable_service`, `parity_diverged`, `not_primary`, or `unknown` before
/// the first append). It is in the body because a serve decision that is only
/// visible as a metric delta cannot be asserted at the endpoint level — and the
/// P0 this closes was found by a gate reading a metric family that turned out to
/// be absent, which reads identically to a build that does not have it.
fn append_reply(
    source: &str,
    appended: usize,
    requested: usize,
    failures: Vec<String>,
    serve_state: &str,
) -> (StatusCode, Json<serde_json::Value>) {
    if failures.is_empty() {
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "action": "ehdb.tier.append",
                "outcome": "ok",
                "tier_query_source": source,
                "appended": appended,
                "serve_state": serve_state,
            })),
        );
    }
    tracing::warn!(
        source,
        appended,
        requested,
        serve_state,
        failures = failures.len(),
        "server-authored tier append did not land in full"
    );
    (
        StatusCode::BAD_GATEWAY,
        Json(serde_json::json!({
            "action": "ehdb.tier.append",
            "outcome": "degraded",
            "tier_query_source": source,
            "appended": appended,
            "requested": requested,
            "serve_state": serve_state,
            "errors": failures,
        })),
    )
}

/// `GET /healthz` — liveness check.  Returns 200 OK whenever the
/// process is responding; doesn't check upstream dependencies
/// (NATS / control plane) because those have their own failure
/// modes the heartbeat already covers.
async fn healthz_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// `GET /readyz` — readiness check (noetl/ai-meta#130 cold-start).  Returns 200
/// once boot warmup has completed (the orchestrate drive plug-in is compiled +
/// cached on the drive pool); 503 while still warming.  Kubernetes routes /
/// completes a rollout only on 200, so the one-time warm latency is hidden from
/// the first real request.  Liveness (`/healthz`) stays 200 throughout so a slow
/// warm never trips a restart.
async fn readyz_handler() -> impl IntoResponse {
    if crate::metrics::worker_ready() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "warming")
    }
}

/// `GET /livez` — liveness check for the state-builder drain (noetl/ai-meta#161).
/// Returns 200 while the authoritative WAL drain is connected and serving, 503
/// once it has been continuously erroring against a likely-orphaned JetStream
/// consumer past `NOETL_STATE_BUILDER_UNHEALTHY_SECS`.  Wiring this as the
/// system-pool deployment's `livenessProbe` makes Kubernetes auto-restart a pod
/// whose `state_builder` wedged after a NATS server bounce — the backstop to the
/// in-process self-heal (consumer recreate), which handles the common case
/// without a restart.  Workers that don't run the drive (mode `Off` — the
/// request pool) keep the gauge at its default `1`, so this stays 200 for them
/// and the probe is safe to apply fleet-wide.
async fn livez_handler() -> impl IntoResponse {
    if crate::metrics::state_builder_healthy() {
        (StatusCode::OK, "alive")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "state_builder wedged")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noetl_executor::worker::source::{ClaimOutcome, Command};

    /// ai-meta#257 PR 4. The reply must name the store that answered, on BOTH
    /// arms — a marker present only on the service arm is not a discriminator,
    /// because its absence would then be ambiguous between "local answered" and
    /// "this binary predates the marker".
    #[test]
    fn every_tier_reply_names_the_store_that_answered() {
        let local = stamp_source(
            serde_json::json!({"action": "ehdb.tier.query", "result": {"records": []}}),
            "local",
            None,
            None,
        );
        assert_eq!(local["tier_query_source"], "local");
        assert!(
            local.get("tier_service_addr").is_none(),
            "local has no service address to name"
        );
        // The payload must survive being labelled.
        assert!(local["result"]["records"].is_array());

        let svc = stamp_source(
            serde_json::json!({"record_count": 13, "records": []}),
            "service",
            Some("writer:9110"),
            Some("served_primary"),
        );
        assert_eq!(svc["tier_query_source"], "service");
        assert_eq!(
            svc["serve_state"], "served_primary",
            "the serve decision must be readable on the reply, not only as a metric"
        );
        assert_eq!(svc["tier_service_addr"], "writer:9110");
        assert_eq!(svc["record_count"], 13);
    }

    #[test]
    fn a_non_object_reply_is_labelled_rather_than_dropped() {
        // A reply that is not an object still has to carry the label, and it
        // must not lose its body doing so.
        let v = stamp_source(serde_json::json!([1, 2, 3]), "service", Some("w:9110"), None);
        assert_eq!(v["tier_query_source"], "service");
        assert_eq!(v["body"], serde_json::json!([1, 2, 3]));
    }

    fn dummy_command(id: &str) -> Command {
        Command {
            command_id: id.to_string(),
            execution_id: 1,
            step: "s".to_string(),
            tool_kind: "rhai".to_string(),
            input: serde_json::Value::Null,
            render_context: Default::default(),
            attempts: 0,
        }
    }

    #[tokio::test]
    async fn spawn_starts_and_serves_metrics() {
        // Bind to an ephemeral port (0 => OS picks).
        let handle = spawn("127.0.0.1:0").await.unwrap();
        // The spawn function logs the actual port via tracing; we
        // don't have a direct way to grab the chosen port without
        // refactoring the public API, so this test just confirms
        // the bind succeeded and the task is running.  A more
        // thorough test fits the next observability PR.
        assert!(!handle.is_finished());
        handle.abort();
    }

    #[tokio::test]
    async fn metrics_endpoint_returns_prometheus_text_format() {
        // Bump a counter so the encoded output isn't empty.
        crate::metrics::record_pull(&ClaimOutcome::Claimed(dummy_command("test")), 0.05);

        // Bind to ephemeral port + grab actual addr via a TcpListener.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let actual_addr = listener.local_addr().unwrap();

        let app = Router::new().route("/metrics", get(metrics_handler));
        let server_handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        // Give the server a tick to start accepting.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let body = reqwest::get(format!("http://{actual_addr}/metrics"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        assert!(body.contains("# HELP noetl_worker_pulls_total"));
        assert!(body.contains("noetl_worker_pulls_total{outcome=\"claimed\"}"));
        server_handle.abort();
    }

    #[tokio::test]
    async fn healthz_returns_ok() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let actual_addr = listener.local_addr().unwrap();

        let app = Router::new().route("/healthz", get(healthz_handler));
        let server_handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let resp = reqwest::get(format!("http://{actual_addr}/healthz"))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(resp.text().await.unwrap(), "ok");
        server_handle.abort();
    }
}
