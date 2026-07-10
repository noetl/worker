//! EHDB Data Query Interface — the worker-side **read** handler (noetl/ai-meta#178).
//!
//! This is the data-plane half of the EHDB query interface. The server stays a
//! control-plane gatekeeper: it serves projection / read-model queries directly
//! from Postgres and, for the **raw data-plane tiers** (event-log raw scan, KV,
//! object, vector), makes a synchronous read request straight to this handler
//! over the existing worker metrics/query port (`worker-service:9090`). Reads do
//! **not** ride the NATS drive/command bus — a query is a data-plane read, not a
//! unit of playbook work, so it takes the direct data-plane path.
//!
//! ## Boundaries (identical discipline to the rest of `src/ehdb`)
//!
//! * **Disabled by default** — when `NOETL_EHDB_ENABLED` is not truthy every
//!   tier query is a strict [`QueryOutcome::Disabled`] no-op that reads no store
//!   and records no metric, so a disabled worker (the prod default) is
//!   byte-identical to a build without this handler.
//! * **Control-plane refusal** — [`assert_data_plane_access_allowed`] refuses
//!   `gateway`/`api`/`server` roles before any store is opened. The worker
//!   process is always a data-plane role, so this is defense-in-depth: even if
//!   the query port were somehow reached by a control-plane-roled process it
//!   could not read tier storage.
//! * **Read-only** — every entry point opens a tier driver and calls its *read*
//!   method (`scan_global` / `read_execution` / `get` / `scan` / `list` /
//!   `locate` / `query`). No append/put/upsert/delete path is reachable here.
//! * **Bounded** — every list/scan clamps `limit` to [`MAX_QUERY_LIMIT`]; the
//!   vector `top_k` is clamped the same way. Over-bound requests are truncated,
//!   never unbounded.
//! * **Stateless** — the tier driver (or per-shard durable store) is opened,
//!   read, and dropped per call, matching the mirror write path.
//!
//! ## What it reads — the same stores the live mirrors write through
//!
//! The event-log / KV / object / vector mirrors ([`super::eventlog`],
//! [`super::kv`], [`super::object`], [`super::vector`]) write to the bounded
//! `local_reference` log (and, for the durable event-log backend, the per-shard
//! segment stores). This handler opens the **same** drivers over the **same**
//! contract log, so a query returns exactly what the mirror persisted — no second
//! copy, no divergent path.
//!
//! ## Secret-free by construction
//!
//! Every tier `*Outcome` returned here is already the crate's secret-free
//! projection (`#[serde(deny_unknown_fields)]` view structs — sequence ids, keys,
//! digests, versions, scores; never a credential surface). The handler relays the
//! `Serialize` outcome verbatim; there is nothing to scrub.

use std::time::Instant;

use ehdb_reference::{
    DurableSegmentStore, EventLogDriver, EventLogReadExecutionRequest, EventLogRecordView,
    EventLogScanOutcome, EventLogScanRequest, EventLogStorageBackend, KvGetRequest, KvScanRequest,
    KvStateDriver, LocalReferenceEventLogDriver, ObjectBlobDriver, ObjectGetRequest,
    ObjectListRequest, ObjectLocateRequest, VectorDriver, VectorQueryRequest,
    DEFAULT_LOCAL_REFERENCE_NAMESPACE, DEFAULT_LOCAL_REFERENCE_TENANT,
};
use serde_json::{json, Value};

use super::contract::{contract_from_env, safe_client_role, EhdbContract, EHDB_ENABLED_ENV};
use super::eventlog_backend::{owned_shards, ownership_from_env, selected_backend, DurablePaths};
use super::guard::assert_data_plane_access_allowed;
use super::{metrics, EnvMap};

/// Hard ceiling on rows any single tier query returns (matches the server-side
/// `MAX_EHDB_LIMIT` and the data-plane read cap). A request may ask for less; it
/// can never ask for more.
pub const MAX_QUERY_LIMIT: usize = 1000;
/// Default page size when a query omits `limit`.
const DEFAULT_QUERY_LIMIT: usize = 100;

/// The four raw data-plane tiers this handler serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryTier {
    Eventlog,
    Kv,
    Object,
    Vector,
}

impl QueryTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            QueryTier::Eventlog => "eventlog",
            QueryTier::Kv => "kv",
            QueryTier::Object => "object",
            QueryTier::Vector => "vector",
        }
    }

    /// Parse a tier token (case-insensitive). `None` = unknown tier.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "eventlog" => Some(QueryTier::Eventlog),
            "kv" => Some(QueryTier::Kv),
            "object" => Some(QueryTier::Object),
            "vector" => Some(QueryTier::Vector),
            _ => None,
        }
    }
}

/// Parsed, tier-agnostic query parameters (the HTTP query string, decoded once).
/// Each tier reads the subset it needs; irrelevant fields are ignored.
#[derive(Debug, Clone, Default)]
pub struct QueryParams {
    // Common.
    pub tenant: Option<String>,
    pub namespace: Option<String>,
    pub limit: Option<usize>,
    pub after: Option<u64>,
    /// Correlation id for tracing (never a metric label — cardinality).
    pub execution_id: Option<String>,
    // Event-log per-execution read.
    pub execution: Option<String>,
    // KV.
    pub bucket: Option<String>,
    pub key: Option<String>,
    pub prefix: Option<String>,
    // Object op selector (`get` | `list` | `locate`); defaults inferred from key.
    pub op: Option<String>,
    // Vector.
    pub collection: Option<String>,
    pub model_id: Option<String>,
    pub top_k: Option<usize>,
    pub vector: Option<Vec<f32>>,
}

impl QueryParams {
    /// Build from a flat key→value map (the decoded HTTP query string). Numeric /
    /// vector fields that fail to parse are dropped (treated as absent) rather
    /// than erroring — the tier dispatch reports a precise `Rejected` when a
    /// required field is missing.
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut p = QueryParams::default();
        for (k, v) in pairs {
            let key = k.as_ref();
            let val = v.as_ref().to_string();
            match key {
                "tenant" => p.tenant = non_empty(val),
                "namespace" => p.namespace = non_empty(val),
                "limit" => p.limit = val.trim().parse().ok(),
                "after" => p.after = val.trim().parse().ok(),
                "execution_id" => p.execution_id = non_empty(val),
                "execution" => p.execution = non_empty(val),
                "bucket" => p.bucket = non_empty(val),
                "key" => p.key = non_empty(val),
                "prefix" => p.prefix = non_empty(val),
                "op" => p.op = non_empty(val),
                "collection" => p.collection = non_empty(val),
                "model_id" => p.model_id = non_empty(val),
                "top_k" => p.top_k = val.trim().parse().ok(),
                "vector" => p.vector = parse_vector(&val),
                _ => {}
            }
        }
        p
    }

    fn tenant_or_default(&self) -> String {
        self.tenant
            .clone()
            .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string())
    }

    fn namespace_or_default(&self) -> String {
        self.namespace
            .clone()
            .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string())
    }

    fn bounded_limit(&self) -> usize {
        self.limit.unwrap_or(DEFAULT_QUERY_LIMIT).clamp(1, MAX_QUERY_LIMIT)
    }
}

fn non_empty(v: String) -> Option<String> {
    let t = v.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

/// Parse a comma-separated float list (`0.1,0.2,-0.3`) into a query embedding.
/// Any malformed component makes the whole vector absent so the tier reports a
/// clear `Rejected` rather than silently querying a truncated embedding.
fn parse_vector(raw: &str) -> Option<Vec<f32>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut out = Vec::new();
    for part in trimmed.split(',') {
        match part.trim().parse::<f32>() {
            Ok(f) => out.push(f),
            Err(_) => return None,
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

/// Classified outcome of a tier query — the metric + HTTP-status discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOutcome {
    /// EHDB disabled (default) — strict no-op, no store opened, no metric.
    Disabled,
    /// A read that returned live records / a found entry.
    Served,
    /// A well-formed read whose target does not exist yet (empty log, absent key).
    Absent,
    /// A caller mistake — missing required field or a malformed identifier.
    Rejected,
    /// The engine was reachable but the read failed (I/O, corruption) — degraded.
    Unavailable,
    /// A control-plane role reached the data-plane query — refused by the guard.
    GuardRefused,
    /// A config error resolving the contract (not a control-plane refusal).
    Invalid,
}

impl QueryOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            QueryOutcome::Disabled => "disabled",
            QueryOutcome::Served => "served",
            QueryOutcome::Absent => "absent",
            QueryOutcome::Rejected => "rejected",
            QueryOutcome::Unavailable => "unavailable",
            QueryOutcome::GuardRefused => "guard_refused",
            QueryOutcome::Invalid => "invalid",
        }
    }

    fn ok(&self) -> bool {
        matches!(self, QueryOutcome::Served | QueryOutcome::Absent)
    }

    fn degraded(&self) -> bool {
        matches!(self, QueryOutcome::Unavailable)
    }

    /// The HTTP status the worker's query route returns for this outcome. The
    /// server relays it verbatim.
    pub fn http_status(&self) -> u16 {
        match self {
            QueryOutcome::Served | QueryOutcome::Absent => 200,
            QueryOutcome::Disabled => 200,
            QueryOutcome::Rejected | QueryOutcome::Invalid => 400,
            QueryOutcome::GuardRefused => 403,
            QueryOutcome::Unavailable => 503,
        }
    }
}

/// The full result of a tier query: the classified outcome + the JSON body the
/// worker returns (and the server relays).
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub outcome: QueryOutcome,
    pub body: Value,
}

/// A read error from a tier driver classified into the outcome taxonomy: an
/// `invalid identifier` Display prefix is a caller mistake (`Rejected`); anything
/// else is a degraded engine failure (`Unavailable`). Mirrors
/// `dataplane::classify_helper_error`.
fn classify_read_error<E: std::fmt::Display>(err: &E) -> QueryOutcome {
    if err.to_string().starts_with("invalid identifier") {
        QueryOutcome::Rejected
    } else {
        QueryOutcome::Unavailable
    }
}

/// Run one read-only tier query against the worker's in-process EHDB drivers.
///
/// The single entry point the query HTTP route calls. Resolves + guards the
/// data-plane contract, dispatches to the tier's read method, records the
/// `noetl_worker_ehdb_query_*` metric, and returns the JSON body + outcome.
pub fn run_query(env: &EnvMap, tier: QueryTier, params: &QueryParams) -> QueryResult {
    let started = Instant::now();

    // Disabled by default — no store opened, no metric recorded.
    if !truthy(env, EHDB_ENABLED_ENV) {
        return finish(
            tier,
            "query",
            QueryOutcome::Disabled,
            started,
            json!({ "status": "disabled", "reason": "NOETL_EHDB_ENABLED not set" }),
        );
    }

    // Resolve + guard the contract before any store is opened.
    let contract = match contract_from_env(env) {
        Ok(c) => c,
        Err(err) => {
            let role = safe_client_role(env);
            let outcome = if role.map(|r| r.is_control_plane()).unwrap_or(false) {
                QueryOutcome::GuardRefused
            } else {
                QueryOutcome::Invalid
            };
            return finish(
                tier,
                "query",
                outcome,
                started,
                json!({ "status": outcome.as_str(), "reason": err.0 }),
            );
        }
    };
    if let Err(err) = assert_data_plane_access_allowed(contract.role, "query") {
        return finish(
            tier,
            "query",
            QueryOutcome::GuardRefused,
            started,
            json!({ "status": "guard_refused", "reason": err.to_string() }),
        );
    }
    if !contract.uses_local_reference_runtime() || contract.local_reference_log.is_none() {
        return finish(
            tier,
            "query",
            QueryOutcome::Disabled,
            started,
            json!({ "status": "disabled", "reason": "no local_reference runtime for this role" }),
        );
    }

    let span = tracing::info_span!(
        "ehdb.query",
        tier = tier.as_str(),
        execution_id = params.execution_id.as_deref().unwrap_or("")
    );
    let _e = span.enter();

    let (operation, outcome, body) = match tier {
        QueryTier::Eventlog => query_eventlog(env, &contract, params),
        QueryTier::Kv => query_kv(&contract, params),
        QueryTier::Object => query_object(&contract, params),
        QueryTier::Vector => query_vector(&contract, params),
    };
    finish(tier, operation, outcome, started, body)
}

fn truthy(env: &EnvMap, key: &str) -> bool {
    matches!(
        env.get(key).map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "y" | "on")
    )
}

/// Record the metric + wrap the body in the standard envelope.
fn finish(
    tier: QueryTier,
    operation: &str,
    outcome: QueryOutcome,
    started: Instant,
    inner: Value,
) -> QueryResult {
    let duration = started.elapsed().as_secs_f64();
    if outcome != QueryOutcome::Disabled {
        metrics::record_query(
            tier.as_str(),
            operation,
            outcome.as_str(),
            outcome.ok(),
            outcome.degraded(),
            duration,
        );
    }
    QueryResult {
        outcome,
        body: json!({
            "action": "ehdb.tier.query",
            "tier": tier.as_str(),
            "op": operation,
            "outcome": outcome.as_str(),
            "result": inner,
        }),
    }
}

// ---------------------------------------------------------------------------
// Event-log tier
// ---------------------------------------------------------------------------

/// Event-log read: a per-execution ordered read when `execution` is supplied,
/// else a global ordered scan. The scan honours the *selected backend* — the
/// durable segment stack fans a k-way merge across this replica's owned shards;
/// the default local-reference backend scans the single JSONL log.
fn query_eventlog(
    env: &EnvMap,
    contract: &EhdbContract,
    params: &QueryParams,
) -> (&'static str, QueryOutcome, Value) {
    let tenant = params.tenant_or_default();
    let namespace = params.namespace_or_default();
    let limit = params.bounded_limit();
    let backend = selected_backend(env);

    if let Some(execution) = params.execution.as_deref() {
        return eventlog_read_execution(env, contract, backend, execution, params.after, limit, &tenant, &namespace);
    }

    match backend {
        EventLogStorageBackend::LocalReference => {
            let driver = LocalReferenceEventLogDriver::new(
                contract.local_reference_log.clone().expect("log present"),
                tenant,
                namespace,
            );
            match driver.scan_global(&EventLogScanRequest { after: params.after, limit }) {
                Ok(out) => {
                    let outcome = if out.returned > 0 {
                        QueryOutcome::Served
                    } else {
                        QueryOutcome::Absent
                    };
                    ("scan", outcome, serde_json::to_value(out).unwrap_or(Value::Null))
                }
                Err(e) => ("scan", classify_read_error(&e), json!({ "error": e.to_string() })),
            }
        }
        EventLogStorageBackend::DurableSegment => {
            match durable_merged_scan(env, contract, params.after, limit) {
                Ok(out) => {
                    let outcome = if out.returned > 0 {
                        QueryOutcome::Served
                    } else {
                        QueryOutcome::Absent
                    };
                    ("scan_merged", outcome, serde_json::to_value(out).unwrap_or(Value::Null))
                }
                Err(e) => ("scan_merged", QueryOutcome::Unavailable, json!({ "error": e })),
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn eventlog_read_execution(
    env: &EnvMap,
    contract: &EhdbContract,
    backend: EventLogStorageBackend,
    execution: &str,
    after: Option<u64>,
    limit: usize,
    tenant: &str,
    namespace: &str,
) -> (&'static str, QueryOutcome, Value) {
    let request = EventLogReadExecutionRequest {
        execution_id: execution.to_string(),
        after,
        limit,
    };
    match backend {
        EventLogStorageBackend::LocalReference => {
            let driver = LocalReferenceEventLogDriver::new(
                contract.local_reference_log.clone().expect("log present"),
                tenant.to_string(),
                namespace.to_string(),
            );
            match driver.read_execution(&request) {
                Ok(out) => {
                    let outcome = if out.exists {
                        QueryOutcome::Served
                    } else {
                        QueryOutcome::Absent
                    };
                    ("read", outcome, serde_json::to_value(out).unwrap_or(Value::Null))
                }
                Err(e) => ("read", classify_read_error(&e), json!({ "error": e.to_string() })),
            }
        }
        EventLogStorageBackend::DurableSegment => {
            // Read from the shard that owns this execution (single-writer =
            // single-reader-of-truth); a fresh read-only open replays the
            // segments (crash-recovery correctness).
            let paths = DurablePaths::resolve(env, contract);
            let shard = ownership_from_env(env).shard_of(execution);
            let dir = paths.local_root.join(format!("shard-{shard:04}"));
            if !dir.exists() {
                return (
                    "read",
                    QueryOutcome::Absent,
                    json!({ "exists": false, "execution_id": execution, "returned": 0 }),
                );
            }
            match DurableSegmentStore::open_read_only(&dir)
                .and_then(|mut s| s.read_execution(&request))
            {
                Ok(out) => {
                    let outcome = if out.exists {
                        QueryOutcome::Served
                    } else {
                        QueryOutcome::Absent
                    };
                    ("read", outcome, serde_json::to_value(out).unwrap_or(Value::Null))
                }
                Err(e) => ("read", classify_read_error(&e), json!({ "error": e.to_string() })),
            }
        }
    }
}

/// Fan a bounded ordered scan across every durable shard this replica owns and
/// k-way merge the per-shard record streams into one global-sequence-ordered
/// view. Degrades to a single store when the pool is unsharded (one owned shard).
///
/// Each per-shard store already applied `after` + `limit` over its own gapless
/// sequence space; the merge re-orders by `global_sequence` (ties broken by
/// `execution_id`) and re-applies `limit` so the merged page is coherent and
/// bounded. A missing shard directory (never written) contributes nothing.
fn durable_merged_scan(
    env: &EnvMap,
    contract: &EhdbContract,
    after: Option<u64>,
    limit: usize,
) -> Result<EventLogScanOutcome, String> {
    let paths = DurablePaths::resolve(env, contract);
    let mut per_shard: Vec<Vec<EventLogRecordView>> = Vec::new();
    let mut exists = false;
    let mut total_after_cursor = 0usize;
    for shard in owned_shards(env) {
        let dir = paths.local_root.join(format!("shard-{shard:04}"));
        if !dir.exists() {
            continue;
        }
        let mut store = DurableSegmentStore::open_read_only(&dir).map_err(|e| e.to_string())?;
        let scan = store
            .scan_global(&EventLogScanRequest { after, limit })
            .map_err(|e| e.to_string())?;
        if scan.exists {
            exists = true;
        }
        total_after_cursor += scan.record_count;
        per_shard.push(scan.records);
    }
    let merged = merge_shard_records(per_shard, limit);
    let returned = merged.len();
    Ok(EventLogScanOutcome {
        action: "eventlog-scan-merged".to_string(),
        exists,
        // Total records-after-cursor across shards (pre-`limit`); the merged view
        // returns at most `limit` of them.
        record_count: total_after_cursor,
        returned,
        records: merged,
    })
}

/// Pure k-way merge of per-shard record streams (the scan-merge hot path). Orders
/// by `global_sequence`, breaking ties on `execution_id`, and truncates to
/// `limit`. Kept dependency-free + `&mut`-free so it is unit-testable and
/// benchable in isolation.
pub fn merge_shard_records(
    per_shard: Vec<Vec<EventLogRecordView>>,
    limit: usize,
) -> Vec<EventLogRecordView> {
    let mut all: Vec<EventLogRecordView> = per_shard.into_iter().flatten().collect();
    all.sort_by(|a, b| {
        a.global_sequence
            .cmp(&b.global_sequence)
            .then_with(|| a.execution_id.cmp(&b.execution_id))
    });
    all.truncate(limit);
    all
}

// ---------------------------------------------------------------------------
// KV tier
// ---------------------------------------------------------------------------

/// KV read: a single-key `get` when `key` is supplied, else a bounded bucket
/// `scan` (optionally prefix-filtered). `now_ms: None` disables TTL filtering so
/// a diagnostic read surfaces the latest live value regardless of TTL clock skew.
fn query_kv(contract: &EhdbContract, params: &QueryParams) -> (&'static str, QueryOutcome, Value) {
    let Some(bucket) = params.bucket.clone() else {
        return (
            "get",
            QueryOutcome::Rejected,
            json!({ "error": "kv query requires ?bucket=<bucket>" }),
        );
    };
    let opts = super::kv::KvOptions {
        tenant: params.tenant.clone(),
        namespace: params.namespace.clone(),
        transaction_id: None,
    };
    let driver = super::kv::driver_from(contract, &opts);

    if let Some(key) = params.key.clone() {
        match driver.get(&KvGetRequest { bucket, key, now_ms: None }) {
            Ok(out) => {
                let outcome = if out.found {
                    QueryOutcome::Served
                } else {
                    QueryOutcome::Absent
                };
                ("get", outcome, serde_json::to_value(out).unwrap_or(Value::Null))
            }
            Err(e) => ("get", classify_read_error(&e), json!({ "error": e.to_string() })),
        }
    } else {
        let req = KvScanRequest {
            bucket,
            prefix: params.prefix.clone(),
            limit: params.bounded_limit(),
            now_ms: None,
        };
        match driver.scan(&req) {
            Ok(out) => {
                let outcome = if out.returned > 0 {
                    QueryOutcome::Served
                } else {
                    QueryOutcome::Absent
                };
                ("scan", outcome, serde_json::to_value(out).unwrap_or(Value::Null))
            }
            Err(e) => ("scan", classify_read_error(&e), json!({ "error": e.to_string() })),
        }
    }
}

// ---------------------------------------------------------------------------
// Object tier
// ---------------------------------------------------------------------------

/// Object read: `locate` (presign-equivalent handle) or `get` (metadata +
/// digest-verify) for a single key, else a bounded prefix `list`. The op is
/// `?op=get|list|locate`, defaulting to `get` when a key is present and `list`
/// otherwise. Bytes are never surfaced — the object tier is metadata-only here.
fn query_object(
    contract: &EhdbContract,
    params: &QueryParams,
) -> (&'static str, QueryOutcome, Value) {
    let opts = super::object::ObjectOptions {
        tenant: params.tenant.clone(),
        namespace: params.namespace.clone(),
        transaction_id: None,
    };
    let driver = super::object::driver_from(contract, &opts);
    let op = params
        .op
        .as_deref()
        .map(|s| s.trim().to_ascii_lowercase())
        .unwrap_or_else(|| if params.key.is_some() { "get".to_string() } else { "list".to_string() });

    match op.as_str() {
        "locate" => {
            let Some(key) = params.key.clone() else {
                return ("locate", QueryOutcome::Rejected, json!({ "error": "object locate requires ?key=<key>" }));
            };
            match driver.locate(&ObjectLocateRequest { key }) {
                Ok(out) => {
                    let outcome = if out.found { QueryOutcome::Served } else { QueryOutcome::Absent };
                    ("locate", outcome, serde_json::to_value(out).unwrap_or(Value::Null))
                }
                Err(e) => ("locate", classify_read_error(&e), json!({ "error": e.to_string() })),
            }
        }
        "list" => {
            let req = ObjectListRequest {
                prefix: params.prefix.clone(),
                limit: params.bounded_limit(),
            };
            match driver.list(&req) {
                Ok(out) => {
                    let outcome = if out.returned > 0 { QueryOutcome::Served } else { QueryOutcome::Absent };
                    ("list", outcome, serde_json::to_value(out).unwrap_or(Value::Null))
                }
                Err(e) => ("list", classify_read_error(&e), json!({ "error": e.to_string() })),
            }
        }
        _ => {
            // "get" (default) — any other token folds here.
            let Some(key) = params.key.clone() else {
                return ("get", QueryOutcome::Rejected, json!({ "error": "object get requires ?key=<key>" }));
            };
            match driver.get(&ObjectGetRequest { key }) {
                Ok(out) => {
                    let outcome = if out.found { QueryOutcome::Served } else { QueryOutcome::Absent };
                    ("get", outcome, serde_json::to_value(out).unwrap_or(Value::Null))
                }
                Err(e) => ("get", classify_read_error(&e), json!({ "error": e.to_string() })),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Vector tier
// ---------------------------------------------------------------------------

/// Vector read: a bounded cosine top-k `query` over a collection. Requires
/// `collection`, `model_id`, and a `vector` embedding (comma-separated floats);
/// `top_k` defaults to the bounded limit. Returns hits (point id + score) only —
/// never the vectors or payloads.
fn query_vector(
    contract: &EhdbContract,
    params: &QueryParams,
) -> (&'static str, QueryOutcome, Value) {
    let (Some(collection), Some(model_id), Some(vector)) =
        (params.collection.clone(), params.model_id.clone(), params.vector.clone())
    else {
        return (
            "query",
            QueryOutcome::Rejected,
            json!({ "error": "vector query requires ?collection=&model_id=&vector=<f,f,...>" }),
        );
    };
    let top_k = params.top_k.unwrap_or(DEFAULT_QUERY_LIMIT).clamp(1, MAX_QUERY_LIMIT);
    let opts = super::vector::VectorOptions {
        tenant: params.tenant.clone(),
        namespace: params.namespace.clone(),
        transaction_id: None,
    };
    let driver = super::vector::driver_from(contract, &opts);
    match driver.query(&VectorQueryRequest {
        collection,
        model_id,
        query: vector,
        top_k,
    }) {
        Ok(out) => {
            let outcome = if out.returned > 0 { QueryOutcome::Served } else { QueryOutcome::Absent };
            ("query", outcome, serde_json::to_value(out).unwrap_or(Value::Null))
        }
        Err(e) => ("query", classify_read_error(&e), json!({ "error": e.to_string() })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> EnvMap {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn worker_env(log: &str) -> EnvMap {
        env(&[
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "worker"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", log),
        ])
    }

    fn tmp_log(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-query-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        (dir.join("log.jsonl"), dir)
    }

    #[test]
    fn tier_parse_is_case_insensitive() {
        assert_eq!(QueryTier::parse("EventLog"), Some(QueryTier::Eventlog));
        assert_eq!(QueryTier::parse(" kv "), Some(QueryTier::Kv));
        assert_eq!(QueryTier::parse("object"), Some(QueryTier::Object));
        assert_eq!(QueryTier::parse("vector"), Some(QueryTier::Vector));
        assert_eq!(QueryTier::parse("bogus"), None);
    }

    #[test]
    fn params_parse_vector_and_numbers() {
        let p = QueryParams::from_pairs([
            ("limit", "50"),
            ("after", "7"),
            ("vector", "0.1,0.2,-0.3"),
            ("top_k", "3"),
        ]);
        assert_eq!(p.limit, Some(50));
        assert_eq!(p.after, Some(7));
        assert_eq!(p.vector, Some(vec![0.1, 0.2, -0.3]));
        assert_eq!(p.top_k, Some(3));
        // Malformed vector component → whole vector dropped.
        let bad = QueryParams::from_pairs([("vector", "0.1,oops")]);
        assert_eq!(bad.vector, None);
    }

    #[test]
    fn limit_is_bounded() {
        let p = QueryParams { limit: Some(10_000), ..Default::default() };
        assert_eq!(p.bounded_limit(), MAX_QUERY_LIMIT);
        let p0 = QueryParams { limit: Some(0), ..Default::default() };
        assert_eq!(p0.bounded_limit(), 1);
        let none = QueryParams::default();
        assert_eq!(none.bounded_limit(), DEFAULT_QUERY_LIMIT);
    }

    #[test]
    fn disabled_by_default_is_noop() {
        // EHDB unset ⇒ strict Disabled no-op (no store opened). `finish()` skips
        // `record_query` for the Disabled outcome; the metrics module's own
        // `disabled_records_nothing` test covers the "no line emitted" property
        // without racing the process-global metrics state that parallel tests
        // share.
        let r = run_query(&env(&[]), QueryTier::Eventlog, &QueryParams::default());
        assert_eq!(r.outcome, QueryOutcome::Disabled);
        assert_eq!(r.body["outcome"], "disabled");
    }

    #[test]
    fn control_plane_role_guard_refused() {
        let e = env(&[
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "control_plane"),
            ("NOETL_EHDB_CLIENT_ROLE", "server"),
            ("NOETL_EHDB_CAPABILITIES", "control_plane"),
        ]);
        let r = run_query(&e, QueryTier::Kv, &QueryParams::default());
        // control_plane mode has no local_reference runtime → Disabled before the
        // guard even fires; a data-plane env with a control-plane role is the
        // GuardRefused path, exercised in the contract/guard unit tests.
        assert!(matches!(r.outcome, QueryOutcome::Disabled | QueryOutcome::GuardRefused));
    }

    #[test]
    fn eventlog_scan_empty_is_absent() {
        let (log, dir) = tmp_log("el-empty");
        let e = worker_env(log.to_str().unwrap());
        let r = run_query(&e, QueryTier::Eventlog, &QueryParams::default());
        assert_eq!(r.outcome, QueryOutcome::Absent);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kv_scan_requires_bucket() {
        let (log, dir) = tmp_log("kv-nobucket");
        let e = worker_env(log.to_str().unwrap());
        let r = run_query(&e, QueryTier::Kv, &QueryParams::default());
        assert_eq!(r.outcome, QueryOutcome::Rejected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn vector_query_requires_fields() {
        let (log, dir) = tmp_log("vec-missing");
        let e = worker_env(log.to_str().unwrap());
        let r = run_query(&e, QueryTier::Vector, &QueryParams::default());
        assert_eq!(r.outcome, QueryOutcome::Rejected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn merge_shard_records_orders_by_global_sequence() {
        let rec = |seq: u64, exec: &str| EventLogRecordView {
            global_sequence: seq,
            execution_id: exec.to_string(),
            transaction_id: format!("txn-{seq}"),
            byte_len: 3,
            payload: "{}".to_string(),
        };
        // Two shards, interleaved sequences.
        let shard_a = vec![rec(1, "100"), rec(3, "100"), rec(5, "100")];
        let shard_b = vec![rec(2, "200"), rec(4, "200")];
        let merged = merge_shard_records(vec![shard_a, shard_b], 100);
        let seqs: Vec<u64> = merged.iter().map(|r| r.global_sequence).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
        // Tie on sequence → broken by execution_id.
        let tie = merge_shard_records(
            vec![vec![rec(1, "200")], vec![rec(1, "100")]],
            100,
        );
        assert_eq!(tie[0].execution_id, "100");
        assert_eq!(tie[1].execution_id, "200");
        // Truncation to limit.
        let capped = merge_shard_records(vec![vec![rec(1, "1"), rec(2, "1"), rec(3, "1")]], 2);
        assert_eq!(capped.len(), 2);
    }

    /// Micro-bench on the scan-merge hot path. `#[ignore]`d so it never runs in
    /// the default `cargo test` gate; run explicitly with
    /// `cargo test --release ehdb::query::tests::bench_merge_shard_records -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn bench_merge_shard_records() {
        let shards = 8usize;
        let per_shard = 4096usize;
        let build = || {
            (0..shards)
                .map(|s| {
                    (0..per_shard)
                        .map(|i| EventLogRecordView {
                            // Interleave sequences across shards so the merge does real work.
                            global_sequence: (i * shards + s) as u64,
                            execution_id: format!("{s}"),
                            transaction_id: format!("txn-{s}-{i}"),
                            byte_len: 8,
                            payload: "{\"k\":1}".to_string(),
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        let iters = 200u32;
        let start = std::time::Instant::now();
        let mut sink = 0usize;
        for _ in 0..iters {
            let merged = merge_shard_records(build(), MAX_QUERY_LIMIT);
            sink = sink.wrapping_add(merged.len());
        }
        let elapsed = start.elapsed();
        let total_records = (shards * per_shard) as u64 * iters as u64;
        eprintln!(
            "merge_shard_records: {iters} iters, {shards}x{per_shard} records/iter, \
             {:.3} ms/iter, {:.1} M records/s (sink={sink})",
            elapsed.as_secs_f64() * 1000.0 / iters as f64,
            total_records as f64 / elapsed.as_secs_f64() / 1e6,
        );
    }
}
