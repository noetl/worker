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
    spawn_with_index(bind, None).await
}

/// As [`spawn`], but with the off-server WAL chain index attached so the
/// state-spine route can serve it (ai-meta#265 Phase 2).
///
/// `None` keeps every existing caller byte-identical: the route is registered
/// either way, and without an index it answers `unavailable` — which is a
/// different answer from "this execution has no events", and the server's fold
/// treats the two differently.
pub async fn spawn_with_index(
    bind: &str,
    index: Option<crate::state_builder::SharedWalIndex>,
) -> Result<JoinHandle<()>> {
    let addr: SocketAddr = bind.parse()?;

    let spine_state = SpineState { index };
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
        )
        // ai-meta#265 Phase 2 — the WAL spine, served for one execution.
        //
        // The worker holds the log and does NOT fold: `WorkflowState` lives in
        // the server's `orchestrate-core`, and the drive folds inside the wasm
        // plug-in. So the split is deliberate — the worker serves the ordered
        // verbatim slim payloads, the server folds and digests them. The
        // alternative (teach the worker to fold) would mean a second
        // implementation of what an execution's state IS, which is the one
        // thing an event-sourced read model must not have.
        .route("/ehdb/state-spine", get(state_spine_handler))
        .with_state(spine_state);

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
            // noetl/ai-meta#155 — same site, same reasoning, for the tier-append
            // store path. This is the bind site of the route that RECEIVES the
            // appends, so pinning here means the counters exist on every worker
            // that can serve one — including the ones that forward to a writer
            // elsewhere and therefore never set `tier_service_up`.
            //
            // Those are the workers whose ratio anyone reads, and the first
            // version of this metric was invisible on exactly them.
            crate::ehdb::metrics::pin_tier_append_series();
            crate::ehdb::metrics::pin_projection_serve_series();
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
    // #265 — the projection tier is served by the writer-fronted tier service,
    // not by a pod-local shadow driver, so it is handled before `QueryTier` and
    // deliberately NOT added to that enum. `QueryTier` is the shadow-driver read
    // surface; adding a variant there would promise a local read path that does
    // not exist, and a promise a route cannot keep is how a caller ends up
    // scoring an error body as data.
    // Both of these are served by the writer-fronted tier service rather than a
    // pod-local shadow driver, so they are handled before `QueryTier` and
    // deliberately NOT added to that enum — `QueryTier` is the shadow-driver read
    // surface, and a variant there would promise a local read path that does not
    // exist.
    match crate::ehdb::store_tier::StoreTier::parse(&tier) {
        Some(t @ crate::ehdb::store_tier::StoreTier::Projection)
        | Some(t @ crate::ehdb::store_tier::StoreTier::Catalog) => {
            return ehdb_service_tier_query(t, &raw).await;
        }
        _ => {}
    }
    let Some(tier) = QueryTier::parse(&tier) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "action": "ehdb.tier.query",
                "error": "unknown tier",
                "known_tiers": ["eventlog", "projection", "catalog", "kv", "object", "vector"],
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
    let serve_state =
        matches!(tier, QueryTier::Eventlog).then(crate::ehdb::eventlog::current_serve_state);

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
                        Json(stamp_source(
                            v,
                            source_label,
                            source_addr.as_deref(),
                            serve_state,
                        )),
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
        Json(stamp_source(
            result.1,
            source_label,
            source_addr.as_deref(),
            serve_state,
        )),
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

/// The serve state of `tier`, as the label its own metric carries (#265).
///
/// Dispatched rather than shared: each tier keeps its own transition atomic, so
/// a demote on one cannot be reported as the other's state.
fn current_serve_state_for(tier: crate::ehdb::store_tier::StoreTier) -> &'static str {
    match tier {
        crate::ehdb::store_tier::StoreTier::Eventlog => {
            crate::ehdb::eventlog::current_serve_state()
        }
        crate::ehdb::store_tier::StoreTier::Projection => {
            crate::ehdb::projection::current_serve_state()
        }
        // The catalog log is append-and-read only; it has no serve path and is
        // deliberately absent from SERVE_WIRED_TIERS. Reporting a serve state
        // for it would publish a label describing a decision nothing makes.
        crate::ehdb::store_tier::StoreTier::Catalog => {
            crate::ehdb::store_tier::CATALOG_SERVE_STATE
        }
    }
}

/// `GET /ehdb/tiers/{projection|catalog}` — read a **service-resolved** tier.
///
/// Service-resolved **only**, for the same reason the append is: a pod-local
/// store would be one replica's fragment, and answering a comparator from a
/// fragment produces a confident divergence report about the wrong store.
/// `local` is refused with a reason rather than silently answered.
///
/// Parameterised over the tier rather than duplicated per tier: two copies of
/// this would be two refusal postures and two places for the `execution` vs
/// `execution_id` trap below to be got wrong.
async fn ehdb_service_tier_query(
    tier: crate::ehdb::store_tier::StoreTier,
    raw: &HashMap<String, String>,
) -> (StatusCode, Json<serde_json::Value>) {
    use crate::ehdb::tier_query_source::Resolution;

    let params = QueryParams::from_pairs(raw.iter());
    let env = crate::ehdb::process_env();
    let resolution = crate::ehdb::tier_query_source::resolve(&env);
    let source_label = resolution.label();
    let source_addr = resolution.addr().map(str::to_string);
    crate::ehdb::metrics::record_tier_query_source("read", source_label);
    let serve_state = Some(current_serve_state_for(tier));

    let refuse = |reason: String| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(stamp_source(
                serde_json::json!({
                    "action": "ehdb.tier.query",
                    "tier": tier.as_str(),
                    "outcome": "unavailable",
                    "error": reason,
                }),
                source_label,
                source_addr.as_deref(),
                serve_state,
            )),
        )
    };

    let client = match &resolution {
        Resolution::Service(c) => c,
        Resolution::Misconfigured(reason) => {
            tracing::error!(%reason, tier = tier.as_str(), "EHDB tier query: refusing to answer");
            return refuse(reason.clone());
        }
        Resolution::Local | Resolution::DowngradedToLocal => {
            return refuse(
                format!(
                    "the {} tier is served only by the writer-fronted tier service; \
                     set NOETL_EHDB_TIER_QUERY_SOURCE=service and NOETL_EHDB_TIER_SERVICE_ADDR",
                    tier.as_str()
                ),
            );
        }
    };

    // `execution`, NOT `execution_id` — the latter is a tracing correlation id
    // in QueryParams, and reading by it would silently query the wrong key.
    let reply = match params.execution.as_deref() {
        Some(eid) => client.read_execution_tier(tier, eid).await,
        None => client.scan_tier(tier, None, 100).await,
    };
    match reply {
        Ok(body) => match serde_json::from_str::<serde_json::Value>(&body) {
            Ok(v) => (
                StatusCode::OK,
                Json(stamp_source(v, source_label, source_addr.as_deref(), serve_state)),
            ),
            // A non-JSON reply is one of the service's typed refusals. Surface it
            // as-is rather than as an empty 200, which would read as "no data".
            Err(_) => (
                StatusCode::BAD_GATEWAY,
                Json(stamp_source(
                    serde_json::json!({
                        "action": "ehdb.tier.query",
                        "tier": tier.as_str(),
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
                    "tier": tier.as_str(),
                    "outcome": "unavailable",
                    "error": e,
                }),
                source_label,
                source_addr.as_deref(),
                serve_state,
            )),
        ),
    }
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
    // The tiers with a durable store behind the tier service (#265, #311): the
    // event log mirroring `noetl.event`, the projection tier mirroring
    // `noetl.projection_snapshot`, and the catalog log. `kv` / `object` /
    // `vector` hold derived data with no authoritative counterpart for a server
    // to author, so accepting an append for them would invent records rather
    // than mirror any.
    let Some(store_tier) = crate::ehdb::store_tier::StoreTier::parse(&tier) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                // Derived from the enum rather than spelled out: the previous
                // wording said "eventlog and projection only" and would have
                // gone stale the moment a tier was added — telling an operator
                // their valid tier is unsupported.
                "error": format!(
                    "append is supported for these tiers only: {}",
                    crate::ehdb::store_tier::StoreTier::ALL
                        .iter()
                        .map(|t| t.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                "action": "ehdb.tier.append",
                "tier": tier,
            })),
        );
    };

    let env = crate::ehdb::process_env();
    // Per-tier, so arming the projection mirror is not a change to the event
    // log's configuration — prod sets the event log's TODAY.
    let source = crate::ehdb::mirror_source::MirrorSource::for_tier(&env, store_tier);
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
                "tier": store_tier.as_str(),
                "error": format!(
                    "{}={} — this worker is not configured to accept server-authored {} appends",
                    crate::ehdb::mirror_source::MirrorSource::env_key_for(store_tier),
                    source.as_str(),
                    store_tier.as_str()
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
            //
            // #265 — the same shape for the projection tier. The dispatch is on
            // the tier rather than a shared function because the two tiers keep
            // their OWN mode flag, their OWN outcome vocabulary and their OWN
            // transition log: a shared serve state would let a projection demote
            // silence an event-log promote, on the tier that is primary in prod.
            //
            // noetl/ai-meta#155 Option 2 — one request, one store lock, one
            // `fsync` for the whole batch instead of one per record. Measured:
            // the per-record `fsync` is ~118ms at production payload size even
            // on an empty store, and the tier appends one record per mirrored
            // event.
            //
            // Off unless `NOETL_EHDB_TIER_APPEND_BATCH` is truthy — the loop
            // below stays the default, so this is per-deployment and instantly
            // revertible.
            //
            // The serve decision is NOT skipped: the same per-tier decision runs
            // per record against the batch's per-record results, because a batch
            // that landed is still N records the serve policy must see. Skipping
            // it here would recreate ai-meta#257's inert-serve-decision bug on a
            // new path — and #265 would inherit it for the projection tier.
            if batch_appends_enabled() && records.len() > 1 {
                crate::ehdb::metrics::record_tier_append_path(true, records.len());
                let started = std::time::Instant::now();
                let raw = client
                    .append_batch_tier(store_tier, &execution_id, &records)
                    .await;
                let elapsed = started.elapsed().as_secs_f64() / records.len() as f64;
                let mut previous_sequence = 0u64;
                let mut serve_state = current_serve_state_for(store_tier);
                let per_record = batch_results(&raw, records.len());
                for out in &per_record {
                    let reply = out.as_deref().map_err(String::as_str);
                    let (seq, label) = match store_tier {
                        crate::ehdb::store_tier::StoreTier::Eventlog => {
                            let serve = crate::ehdb::eventlog::serve_service_append(
                                &env,
                                reply,
                                previous_sequence,
                                elapsed,
                            );
                            (serve.sequence, serve.decision.outcome_label())
                        }
                        crate::ehdb::store_tier::StoreTier::Projection => {
                            let serve = crate::ehdb::projection::serve_service_append(
                                &env,
                                reply,
                                previous_sequence,
                                elapsed,
                            );
                            (serve.sequence, serve.decision.outcome_label())
                        }
                        // The catalog log has no primary-serve path (it is absent
                        // from SERVE_WIRED_TIERS on purpose: nothing reads catalog
                        // rows from EHDB yet). Appends are recorded, not scored —
                        // a serve decision here would be a verdict about a read
                        // path that does not exist.
                        crate::ehdb::store_tier::StoreTier::Catalog => {
                            (None, crate::ehdb::store_tier::catalog_append_label(reply))
                        }
                    };
                    if let Some(s) = seq {
                        previous_sequence = s;
                    }
                    serve_state = label;
                    match out {
                        Ok(_) => appended += 1,
                        Err(e) => failures.push(e.clone()),
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

            crate::ehdb::metrics::record_tier_append_path(false, records.len());
            let mut previous_sequence = 0u64;
            let mut serve_state = current_serve_state_for(store_tier);
            for payload in &records {
                let started = std::time::Instant::now();
                let out = client.append_tier(store_tier, &execution_id, payload).await;
                let elapsed = started.elapsed().as_secs_f64();
                let reply = out.as_deref().map_err(String::as_str);
                let (seq, label) = match store_tier {
                    crate::ehdb::store_tier::StoreTier::Eventlog => {
                        let serve = crate::ehdb::eventlog::serve_service_append(
                            &env,
                            reply,
                            previous_sequence,
                            elapsed,
                        );
                        (serve.sequence, serve.decision.outcome_label())
                    }
                    crate::ehdb::store_tier::StoreTier::Projection => {
                        let serve = crate::ehdb::projection::serve_service_append(
                            &env,
                            reply,
                            previous_sequence,
                            elapsed,
                        );
                        (serve.sequence, serve.decision.outcome_label())
                    }
                    // See the batch path above: catalog appends are recorded,
                    // not scored.
                    crate::ehdb::store_tier::StoreTier::Catalog => {
                        (None, crate::ehdb::store_tier::catalog_append_label(reply))
                    }
                };
                if let Some(s) = seq {
                    previous_sequence = s;
                }
                serve_state = label;
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

    // #265 — the projection tier has NO pod-local write path, and that is a
    // decision rather than an omission.
    //
    // The event log has one because it predates the tier service: a pod-local
    // mirror already existed and had to keep working. The projection tier has no
    // such history, so giving it one would be building the exact defect #257
    // §1.3 exists to describe — N disjoint pod-local fragments, each of which a
    // `primary` flip would promote while the incumbent holds everything. The
    // comparator would then read one replica's fragment and report the other
    // replicas' snapshots missing: a true statement about the wrong store.
    //
    // Refused with 503 and a reason, never written somewhere else.
    if store_tier == crate::ehdb::store_tier::StoreTier::Projection {
        tracing::error!(
            tier_query_source = resolution.label(),
            "EHDB projection tier append: refusing a pod-local write — the projection tier is \
             served only by the writer-fronted tier service (set \
             NOETL_EHDB_TIER_QUERY_SOURCE=service and NOETL_EHDB_TIER_SERVICE_ADDR)"
        );
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "action": "ehdb.tier.append",
                "outcome": "unavailable",
                "tier": store_tier.as_str(),
                "tier_query_source": resolution.label(),
                "error": "the projection tier has no pod-local store; it requires \
                          NOETL_EHDB_TIER_QUERY_SOURCE=service",
            })),
        );
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
/// `NOETL_EHDB_TIER_APPEND_BATCH` — send a multi-record tier append as one
/// batch (noetl/ai-meta#155 Option 2).
///
/// Default **false**: the per-record loop stays the shipped behaviour, so
/// enabling and reverting are both per-deployment and immediate.
fn batch_appends_enabled() -> bool {
    matches!(
        std::env::var("NOETL_EHDB_TIER_APPEND_BATCH")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Split a batch reply into one result per record, in request order.
///
/// A transport failure, an unparseable reply, or a reply whose `results` array
/// does not have one entry per requested record all collapse to "every record
/// failed, with the reason". That is the safe direction: a batch reported as
/// partially landed when the count does not line up would let the serve
/// decision and the caller's `appended` tally disagree with the store.
fn batch_results(raw: &Result<String, String>, expected: usize) -> Vec<Result<String, String>> {
    let body = match raw {
        Ok(b) => b,
        Err(e) => return (0..expected).map(|_| Err(e.clone())).collect(),
    };
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => {
            let msg = format!("tier append_batch reply is not JSON: {e}");
            return (0..expected).map(|_| Err(msg.clone())).collect();
        }
    };
    let results = parsed.get("results").and_then(|r| r.as_array());
    match results {
        Some(list) if list.len() == expected => list
            .iter()
            .map(|r| {
                if r.get("ok").and_then(|o| o.as_bool()).unwrap_or(false) {
                    Ok(r.get("body").map(|b| b.to_string()).unwrap_or_default())
                } else {
                    Err(r
                        .get("error")
                        .and_then(|e| e.as_str())
                        .unwrap_or("tier append_batch: record refused")
                        .to_string())
                }
            })
            .collect(),
        Some(list) => {
            let msg = format!(
                "tier append_batch returned {} results for {expected} records",
                list.len()
            );
            (0..expected).map(|_| Err(msg.clone())).collect()
        }
        None => {
            let msg = "tier append_batch reply carried no results array".to_string();
            (0..expected).map(|_| Err(msg.clone())).collect()
        }
    }
}

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
    if !crate::metrics::state_builder_healthy() {
        return (StatusCode::SERVICE_UNAVAILABLE, "state_builder wedged");
    }
    // noetl/ai-meta#297 — the claim-loop backstop.  A pool whose claim loop
    // parked stayed Running 1/1 with every probe green for ~36h because nothing
    // observed claim liveness.  A loop that has made no progress past the bound
    // is wedged; failing here makes Kubernetes restart it, exactly as the
    // state-builder backstop above already does for the drain.
    //
    // `None` means the loop has never run in this process (no claim loop at all),
    // which must stay 200 so the probe is safe fleet-wide.
    if crate::metrics::claim_loop_wedged(claim_loop_unhealthy_after()) {
        return (StatusCode::SERVICE_UNAVAILABLE, "claim_loop wedged");
    }
    (StatusCode::OK, "alive")
}

/// How long the claim loop may make no progress before `/livez` calls it wedged.
/// `NOETL_CLAIM_LOOP_UNHEALTHY_SECS`, default 180.
///
/// Deliberately well above the ehdb read ceiling (`EHDB_READ_HARD_CEILING_MS`,
/// 300s default is the *read*; a redial follows immediately) so the in-process
/// self-heal gets to run first and a restart is genuinely the last resort — the
/// same ordering the state-builder backstop uses.
fn claim_loop_unhealthy_after() -> u64 {
    std::env::var("NOETL_CLAIM_LOOP_UNHEALTHY_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(180)
}


/// Router state carrying the off-server WAL chain index (ai-meta#265 Phase 2).
#[derive(Clone)]
struct SpineState {
    index: Option<crate::state_builder::SharedWalIndex>,
}

/// `GET /ehdb/state-spine?execution=<id>[&head=<expected_head>]`
///
/// Serve the ordered event spine for one execution, exactly as the off-server
/// drive builds it — same `build_spine` / `build_spine_to`, same completeness
/// contract, same payloads.
///
/// # Fail-closed by construction
///
/// `AdvanceOutcome::Incomplete` means the chain does not reach the requested
/// head: the WAL drain has not caught up, or a link is missing. The drive's own
/// answer to that is a benign no-op the reconciler re-drives, and this route
/// gives the same answer — **`complete: false` and NO events**. It never serves
/// a partial spine, because a fold over a gapped spine is a different
/// execution's history, and a caller cannot tell the difference from the events
/// alone.
///
/// Read-only: it advances the cached chain (the same work a drive would do) and
/// touches the LRU stamp, but writes nothing durable.
async fn state_spine_handler(
    axum::extract::State(st): axum::extract::State<SpineState>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let execution_id = match params.get("execution").and_then(|v| v.parse::<i64>().ok()) {
        Some(v) => v,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "action": "ehdb.state.spine",
                    "outcome": "invalid",
                    "error": "execution is required and must be an integer",
                })),
            )
        }
    };
    let Some(index) = st.index.as_ref() else {
        // Not "no events" — this process has no index at all. The distinction is
        // the whole reason the route reports an outcome rather than an empty
        // list: a fold that read `unavailable` as `empty` would build a state
        // from nothing and call it correct.
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "action": "ehdb.state.spine",
                "outcome": "unavailable",
                "error": "this worker runs no off-server state-builder index",
            })),
        );
    };
    let head = params.get("head").and_then(|v| v.parse::<i64>().ok());

    let (outcome, spine) = {
        let mut idx = index.lock().await;
        let out = match head {
            Some(target) => idx.build_spine_to(execution_id, target),
            None => idx.build_spine(execution_id),
        };
        idx.touch(execution_id);
        out
    };

    let label = match outcome {
        crate::state_builder::AdvanceOutcome::CacheHit => "cache_hit",
        crate::state_builder::AdvanceOutcome::Incremental(_) => "incremental",
        crate::state_builder::AdvanceOutcome::ColdRebuild(_) => "cold_rebuild",
        crate::state_builder::AdvanceOutcome::Incomplete => "incomplete",
    };
    let complete = !matches!(
        outcome,
        crate::state_builder::AdvanceOutcome::Incomplete
    );
    let events = if complete { spine.unwrap_or_default() } else { Vec::new() };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "action": "ehdb.state.spine",
            "outcome": if complete { "ok" } else { "incomplete" },
            "build": label,
            "execution_id": execution_id.to_string(),
            "requested_head": head,
            "complete": complete,
            "count": events.len(),
            "events": events,
        })),
    )
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
        let v = stamp_source(
            serde_json::json!([1, 2, 3]),
            "service",
            Some("w:9110"),
            None,
        );
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

#[cfg(test)]
mod tier_append_batch_tests {

    /// Both store paths must be instrumented, and instrumented once each.
    ///
    /// `append_batch` (ehdb#317 + worker#281) shipped behind a flag whose whole
    /// justification is "the async mirror will produce multi-record batches"
    /// (noetl/ai-meta#155) — and for one release nothing on `/metrics` could say
    /// whether it ever ran. Counting the call sites rather than naming them: a
    /// third append path added later is the failure this catches, and a test
    /// that lists the two it knows about cannot.
    #[test]
    fn every_tier_append_path_is_counted() {
        // Scan the CODE half only. A guard that reads the whole file counts its
        // own search literals and reports a number that is off by exactly the
        // number of patterns it uses — which is how this test failed the first
        // time it ran, and the same way the noetl/ai-meta#263 INSERT counter
        // once counted its own doc comment.
        let whole = include_str!("metrics_server.rs");
        let src = &whole[..whole
            .find("mod tier_append_batch_tests {")
            .expect("the test module must still be the tail of this file")];
        // Counted by PREFIX, not by full name. The first version of this guard
        // matched `client.append_batch(&execution_id` and `client.append(&execution_id`
        // literally, and noetl/ai-meta#265 then renamed both call sites to their
        // tier-addressed forms — which a name-listing guard reports as "zero
        // store calls, zero recorded", i.e. as agreement. A prefix count cannot
        // be satisfied by renaming, and it still catches the case the guard is
        // for: a third append path added later with no counter.
        // Two normalisations before counting, each for a zero this guard already
        // produced:
        //
        // 1. Strip `//` comments. A comment naming a call site counts as one
        //    (noetl/ai-meta#263 counted its own doc comment) — and worse, the
        //    count could be SATISFIED by deleting a comment while adding a real
        //    uncounted path.
        // 2. Strip whitespace. rustfmt is free to break
        //    `client\n    .append_batch_tier(...)` across lines, and a literal
        //    count then reads 0 — the same silent zero, arrived at through
        //    formatting instead of renaming.
        let code: String = src
            .lines()
            .map(|l| match l.find("//") {
                Some(i) => &l[..i],
                None => l,
            })
            .collect::<Vec<_>>()
            .join("\n");
        let flat: String = code.chars().filter(|c| !c.is_whitespace()).collect();
        let store_calls = flat.matches("client.append").count();
        let batch_calls = flat.matches("client.append_batch_tier(").count();
        let single_calls = flat.matches("client.append_tier(").count();
        let recorded = flat.matches("record_tier_append_path(").count();
        // Positive control for the strippers: if either ate real code, the
        // handler's own route literal would vanish too and every count above
        // would be a meaningless zero that still compared equal.
        assert!(
            flat.contains("\"/ehdb/tiers/{tier}\""),
            "the comment/whitespace strippers ate real code; every count below is \
             meaningless and would still compare equal"
        );
        assert_eq!(
            store_calls,
            recorded,
            "the tier-append handler makes {store_calls} store call(s) but has \
             {recorded} record_tier_append_path call(s). An uncounted path makes \
             the batch substrate unfalsifiable from /metrics, which is the state \
             noetl/ai-meta#155 found it in."
        );
        assert_eq!(
            batch_calls + single_calls,
            store_calls,
            "{store_calls} store call(s) but only {batch_calls} batch + {single_calls} \
             single recognised — a path this guard does not know about was added; \
             name it here rather than letting the prefix count absorb it"
        );
        assert!(
            batch_calls >= 1 && single_calls >= 1,
            "both paths must still exist; if one was removed, delete this guard and say so"
        );
    }
    use super::{batch_appends_enabled, batch_results};

    /// A batch reply must split into exactly one result per record, in order.
    #[test]
    fn well_formed_reply_splits_per_record() {
        let raw = Ok(serde_json::json!({
            "action": "ehdb.tier.append_batch",
            "outcome": "ok",
            "results": [
                {"ok": true,  "body": {"global_sequence": 7}},
                {"ok": false, "error": "refused"},
            ],
        })
        .to_string());
        let out = batch_results(&raw, 2);
        assert_eq!(out.len(), 2);
        assert!(out[0].is_ok(), "first record landed");
        assert_eq!(out[1].as_ref().unwrap_err(), "refused");
    }

    /// Every failure shape must collapse to "all records failed".
    ///
    /// This is the safe direction and it is the point of the test: a batch
    /// reported as partially landed when the reply does not line up would let
    /// the caller's `appended` tally and the serve decision disagree with what
    /// the store actually holds — the ai-meta#263 shape, where a tier reported
    /// completeness it did not have.
    #[test]
    fn every_malformed_reply_fails_all_records() {
        let expected = 3;

        let transport_err: Result<String, String> = Err("connect refused".to_string());
        let out = batch_results(&transport_err, expected);
        assert_eq!(out.len(), expected);
        assert!(
            out.iter().all(|r| r.is_err()),
            "transport failure fails all"
        );

        let not_json = Ok("<html>502</html>".to_string());
        assert!(
            batch_results(&not_json, expected)
                .iter()
                .all(|r| r.is_err()),
            "unparseable reply fails all"
        );

        let no_array = Ok(serde_json::json!({"outcome": "ok"}).to_string());
        assert!(
            batch_results(&no_array, expected)
                .iter()
                .all(|r| r.is_err()),
            "missing results array fails all"
        );

        // The one that matters most: a reply that looks fine but is SHORT.
        let short = Ok(serde_json::json!({
            "results": [{"ok": true, "body": {}}, {"ok": true, "body": {}}]
        })
        .to_string());
        let out = batch_results(&short, expected);
        assert_eq!(out.len(), expected, "count must match what was requested");
        assert!(
            out.iter().all(|r| r.is_err()),
            "a short results array must not be read as a partial success"
        );
    }

    /// Default off — the per-record loop stays the shipped behaviour.
    #[test]
    fn batch_is_opt_in() {
        std::env::remove_var("NOETL_EHDB_TIER_APPEND_BATCH");
        assert!(!batch_appends_enabled(), "must default to the loop");
    }
}

#[cfg(test)]
mod state_spine_tests {
    /// An `Incomplete` build must serve NO events.
    ///
    /// This is the fail-closed property the server's fold depends on: a spine
    /// that does not reach the requested head is a gapped history, and folding
    /// it produces a state for an execution that never existed. The drive's own
    /// answer is a benign no-op the reconciler re-drives; this route must give
    /// the same one rather than a shorter list the caller cannot distinguish
    /// from a genuinely shorter execution.
    ///
    /// Asserted on the ROUTE'S OWN CODE rather than by standing up an index,
    /// because the property is a branch in the handler and the branch is what
    /// can regress. Comment-stripped so prose cannot satisfy it, with a
    /// positive control that the stripper left the real code.
    #[test]
    fn an_incomplete_spine_serves_no_events() {
        let whole = include_str!("metrics_server.rs");
        let code: String = whole
            .lines()
            .map(|l| match l.find("//") {
                Some(i) => &l[..i],
                None => l,
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            code.contains("async fn state_spine_handler"),
            "the comment stripper ate the handler; this guard proves nothing"
        );
        assert!(
            code.contains("let events = if complete { spine.unwrap_or_default() } else { Vec::new() };"),
            "the spine route must serve an EMPTY event list when the build is \
             Incomplete. Serving the partial spine would let the server fold a \
             gapped history and digest it as if it were the execution's state."
        );
    }

    /// The route must distinguish "no index in this process" from "no events".
    ///
    /// Two zeros that mean opposite things: `unavailable` is a worker that
    /// cannot answer, `ok` with an empty list is an execution the index knows
    /// nothing about. A fold that collapsed them would build a state from
    /// nothing on a misconfigured worker and report it as correct.
    #[test]
    fn no_index_is_unavailable_not_empty() {
        let whole = include_str!("metrics_server.rs");
        let code: String = whole
            .lines()
            .map(|l| match l.find("//") {
                Some(i) => &l[..i],
                None => l,
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            code.contains("StatusCode::SERVICE_UNAVAILABLE")
                && code.contains("\"unavailable\""),
            "a worker with no state-builder index must answer `unavailable`, \
             never an empty spine"
        );
    }
}

#[cfg(test)]
mod claim_loop_backstop_tests {
    /// These tests mutate PROCESS-GLOBAL gauges.  `cargo test` does **not**
    /// serialise tests — they run on a thread pool — so without this they race
    /// and the "defaults to connected" arm fails whenever the mutation arm has
    /// just flipped it.  Learned the hard way on noetl/ai-meta#265, where an
    /// `unsafe set_var` carried a SAFETY note claiming cargo serialises.
    static GAUGE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn guard() -> std::sync::MutexGuard<'static, ()> {
        GAUGE_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// noetl/ai-meta#297 — the backstop must stay 200 for a worker that runs no
    /// claim loop, or applying the probe fleet-wide restarts the request pool
    /// forever.  This is the arm that makes the probe safe to apply.
    #[test]
    fn a_worker_with_no_claim_loop_is_never_called_wedged() {
        let _g = guard();
        // Asserted through the pure decision, not the process-global gauges:
        // this binary runs 690+ other tests and a sibling arm legitimately
        // leaves the gauges flipped. The property is about the CONTRACT — a
        // process that never claims records no failures — not about whatever
        // the shared gauge happens to hold when this test is scheduled.
        //
        // Getting this arm wrong restarts the request pool forever, which is why
        // it is asserted rather than assumed.
        assert!(
            !crate::metrics::wedged_from(0, 3, None, super::claim_loop_unhealthy_after()),
            "a worker that runs no claim loop must never read wedged"
        );
    }

    /// The gauge must default to healthy for the same reason.
    #[test]
    fn the_claim_loop_gauge_defaults_to_connected() {
        let _g = guard();
        // The property under test is the registered default.  Another test in
        // this binary may already have flipped it, so assert the contract the
        // ctor establishes rather than whatever the gauge happens to hold.
        crate::metrics::record_claim_loop_progress();
        assert!(
            crate::metrics::claim_loop_connected(),
            "a worker that has made progress must read connected; the registered \
             default is 1 for the same reason — a default of 0 would fail /livez \
             on every worker that runs no claim loop"
        );
    }

    /// Progress must both stamp the timestamp and mark connected — a boolean
    /// with no timestamp is what made the stall invisible.
    #[test]
    fn progress_stamps_the_timestamp_and_marks_connected() {
        let _g = guard();
        crate::metrics::set_claim_loop_connected(false);
        assert!(!crate::metrics::claim_loop_connected());
        crate::metrics::record_claim_loop_progress();
        assert!(crate::metrics::claim_loop_connected());
        let stale = crate::metrics::claim_loop_stale_secs();
        assert!(stale.is_some(), "progress must stamp a readable timestamp");
        assert!(stale.unwrap() < 5, "a fresh stamp must read as fresh");
    }

    /// The defect the kind fault arm found: a frozen peer still completes the TCP
    /// handshake, so a loop redialling a stuck writer "connects" every time. When
    /// a connect counted as progress the gauge read healthy through an 8-failure
    /// streak. Only a claim may clear it.
    #[test]
    fn a_reconnect_to_a_stuck_peer_is_not_progress() {
        let _g = guard();
        crate::metrics::record_claim_loop_progress(); // clean slate
        assert!(crate::metrics::claim_loop_connected());
        for _ in 0..8 {
            crate::metrics::record_claim_loop_failure();
        }
        assert!(
            !crate::metrics::claim_loop_connected(),
            "a failure streak must flip the gauge; connects must not clear it"
        );
        assert!(crate::metrics::claim_loop_consecutive_failures() >= 8);
        // Only a claim recovers it.
        crate::metrics::record_claim_loop_progress();
        assert!(crate::metrics::claim_loop_connected());
        assert_eq!(crate::metrics::claim_loop_consecutive_failures(), 0);
    }

    /// A healthy idle bus produces zero failures, so idleness can never be
    /// mistaken for a stall — the trap a naive "no claims recently" rule falls
    /// into, and the reason the discriminator is a failure streak.
    #[test]
    fn an_idle_bus_is_never_called_wedged() {
        let _g = guard();
        crate::metrics::record_claim_loop_progress();
        assert!(
            !crate::metrics::claim_loop_wedged(super::claim_loop_unhealthy_after()),
            "an idle bus parks — it must never read wedged"
        );
    }

    /// The SECOND defect the kind arm found, and the more dangerous one: after a
    /// stall clears, the system pool is legitimately idle — it has nothing to
    /// claim. A rule keyed on claim recency left the gauge at 0 and /livez at 503
    /// indefinitely, which would restart-loop a healthy pool forever. Recovery
    /// must be provable WITHOUT a claim.
    #[test]
    fn a_recovered_but_idle_pool_stops_reading_wedged() {
        let _g = guard();
        crate::metrics::record_claim_loop_progress();
        for _ in 0..5 {
            crate::metrics::record_claim_loop_failure();
        }
        assert!(
            crate::metrics::claim_loop_wedged(3600),
            "while failures are recent it is wedged"
        );
        // Failures stop. No claim arrives — the pool is simply idle. Once the
        // last failure is older than the window it must read healthy, and that
        // has to hold WITHOUT a claim ever arriving.
        let streak = 5;
        assert!(
            crate::metrics::wedged_from(streak, 3, Some(10), 180),
            "control: a failure 10s ago inside a 180s window is wedged"
        );
        assert!(
            !crate::metrics::wedged_from(streak, 3, Some(600), 180),
            "once failures stop past the window, an idle pool must not stay wedged \
             — otherwise /livez restart-loops a healthy worker forever"
        );
        assert!(
            !crate::metrics::wedged_from(0, 3, Some(1), 180),
            "a single blip below the streak is never wedged"
        );
        assert!(
            !crate::metrics::wedged_from(99, 3, None, 180),
            "no failure ever recorded cannot be wedged"
        );
    }

    /// The bound must be positive and overridable; 0 must not disable the
    /// backstop by making everything instantly wedged.
    #[test]
    fn the_unhealthy_bound_is_sane() {
        let _g = guard();
        assert_eq!(super::claim_loop_unhealthy_after(), 180);
        // SAFETY: restored below; this test owns the var for its duration.
        unsafe { std::env::set_var("NOETL_CLAIM_LOOP_UNHEALTHY_SECS", "0") };
        assert_eq!(
            super::claim_loop_unhealthy_after(),
            180,
            "0 must fall back to the default, not make every worker instantly wedged"
        );
        unsafe { std::env::set_var("NOETL_CLAIM_LOOP_UNHEALTHY_SECS", "45") };
        assert_eq!(super::claim_loop_unhealthy_after(), 45);
        unsafe { std::env::remove_var("NOETL_CLAIM_LOOP_UNHEALTHY_SECS") };
    }
}
