//! Bounded, stateless worker/playbook/system data-plane ops for **EHDB RAG
//! retrieval** (EHDB Phase E, noetl/ehdb#234).
//!
//! This is the second half of the Phase E direction (system-WASM store → RAG).
//! The system-WASM store slice ([`super::systemstore`]) publishes/binds/resolves
//! immutable WASM library manifests; this slice ingests bounded retrieval
//! documents into the derived EHDB fabric and runs bounded, read-only retrieval
//! over them.  It is the worker-side bridge to the `ehdb_reference`
//! retrieve/ingest helpers:
//!
//! * `ingest`   — write one document + its chunks (one atomic commit).
//! * `retrieve` — read-only bounded text search over the derived fabric.
//!
//! Every op honours the same boundaries as [`super::dataplane`] /
//! [`super::eventstream`] / [`super::systemstore`]:
//!
//! * **Disabled by default** — `Disabled` no-op that records no metric, so a
//!   disabled build renders byte-identical `/metrics`.
//! * **Control-plane guarded** — gateway/api/server are refused before any
//!   runtime is opened, so no gatekeeper can ingest or retrieve.
//! * **Bounded** — retrieval enforces three caps in the `ehdb_reference` helper:
//!   a top-k cap (`NOETL_EHDB_RAG_TOP_K`, default 8, ceiling 64), a per-hit
//!   result-size cap (`NOETL_EHDB_RAG_MAX_CHUNK_BYTES`, default 4 KiB, ceiling
//!   64 KiB), and a wall-clock budget (`NOETL_EHDB_RAG_TIME_BUDGET_MS`, default
//!   5 s, ceiling 60 s).  An over-ceiling cap is `Rejected`; an empty query is
//!   `Invalid` — both classified before any search.
//! * **Stateless** — the local-reference runtime is opened + dropped per call.
//! * **Event-log-authoritative** — retrieval is read-only; ingest writes only
//!   the separate on-disk JSONL fabric, NEVER `noetl.event` (structurally: no
//!   NoETL event-emitter import reaches this module).

use ehdb_reference::{
    ingest_local_reference_retrieval_document, retrieve_local_reference_context, IngestChunkInput,
    IngestRetrievalDocumentOutcome, IngestRetrievalDocumentRequest, RetrievalOutcome,
    RetrieveContextOutcome, RetrieveContextRequest, DEFAULT_LOCAL_REFERENCE_NAMESPACE,
    DEFAULT_LOCAL_REFERENCE_TENANT, MAX_RETRIEVAL_MAX_CHUNK_BYTES, MAX_RETRIEVAL_TIME_BUDGET_MS,
    MAX_RETRIEVAL_TOP_K,
};

use std::sync::OnceLock;

use super::contract::{contract_from_env, EhdbClientRole, EhdbContract, EHDB_ENABLED_ENV};
use super::guard::assert_data_plane_access_allowed;
use super::{metrics, EnvMap};
use crate::snowflake::SnowflakeGen;

pub const TOP_K_ENV: &str = "NOETL_EHDB_RAG_TOP_K";
pub const MAX_CHUNK_BYTES_ENV: &str = "NOETL_EHDB_RAG_MAX_CHUNK_BYTES";
pub const TIME_BUDGET_MS_ENV: &str = "NOETL_EHDB_RAG_TIME_BUDGET_MS";
const DEFAULT_TOP_K: usize = 8;
const DEFAULT_MAX_CHUNK_BYTES: usize = 4_096;
const DEFAULT_TIME_BUDGET_MS: u64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RagOperation {
    Ingest,
    Retrieve,
}

impl RagOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            RagOperation::Ingest => "ingest",
            RagOperation::Retrieve => "retrieve",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RagOutcome {
    Disabled,
    /// An ingest committed the document + chunks.
    Ingested,
    /// A retrieval returned at least one hit.
    Hit,
    /// A retrieval ran but matched nothing (or no docs are in scope).
    Empty,
    /// An over-ceiling cap (top-k / size / time) or an ingest bound violation.
    Rejected,
    /// A degraded IO / storage failure under the helper.
    Unavailable,
    /// A control-plane role was refused (the boundary).
    GuardRefused,
    /// A caller mistake (empty query, bad identifier).
    Invalid,
}

impl RagOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            RagOutcome::Disabled => "disabled",
            RagOutcome::Ingested => "ingested",
            RagOutcome::Hit => "hit",
            RagOutcome::Empty => "empty",
            RagOutcome::Rejected => "rejected",
            RagOutcome::Unavailable => "unavailable",
            RagOutcome::GuardRefused => "guard_refused",
            RagOutcome::Invalid => "invalid",
        }
    }

    pub fn ok(&self) -> bool {
        matches!(
            self,
            RagOutcome::Disabled | RagOutcome::Ingested | RagOutcome::Hit | RagOutcome::Empty
        )
    }

    fn degraded(&self) -> bool {
        matches!(self, RagOutcome::Unavailable)
    }
}

/// Structured, secret-free result of a bounded RAG op.
#[derive(Debug, Clone)]
pub struct RagResult {
    pub operation: RagOperation,
    pub outcome: RagOutcome,
    pub role: Option<EhdbClientRole>,
    pub duration_seconds: f64,
    pub detail: Option<String>,
    pub retrieve: Option<RetrieveContextOutcome>,
    pub ingest: Option<IngestRetrievalDocumentOutcome>,
}

/// Optional tenant/namespace/transaction overrides for a RAG op.
#[derive(Debug, Clone, Default)]
pub struct RagOptions {
    pub tenant: Option<String>,
    pub namespace: Option<String>,
    pub transaction_id: Option<String>,
}

/// A chunk to ingest (the bounded, validated-at-caller shape).
#[derive(Debug, Clone)]
pub struct RagChunk {
    pub chunk_id: String,
    pub ordinal: u32,
    pub text: String,
    pub checksum: String,
}

/// A document + its chunks to ingest.
#[derive(Debug, Clone)]
pub struct RagDocument {
    pub document_id: String,
    pub source_uri: String,
    pub content_type: String,
    pub chunks: Vec<RagChunk>,
}

/// Bounded retrieval parameters.  A `0` cap is resolved to the env-configured
/// default inside the `ehdb_reference` helper; an over-ceiling cap is `Rejected`.
#[derive(Debug, Clone)]
pub struct RagQuery {
    pub query: String,
    pub top_k: usize,
    pub max_chunk_bytes: usize,
    pub time_budget_ms: u64,
}

fn txn_gen() -> &'static SnowflakeGen {
    static GEN: OnceLock<SnowflakeGen> = OnceLock::new();
    GEN.get_or_init(|| SnowflakeGen::from_env_or_hint("ehdb"))
}

fn new_transaction_id() -> String {
    format!("ehdbtxn-{}", txn_gen().next_id())
}

fn truthy(env: &EnvMap, key: &str) -> bool {
    matches!(
        env.get(key)
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref(),
        Some("1" | "true" | "yes" | "y" | "on")
    )
}

fn bounded_top_k(env: &EnvMap) -> usize {
    let value = env
        .get(TOP_K_ENV)
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_TOP_K);
    value.clamp(1, MAX_RETRIEVAL_TOP_K)
}

fn bounded_max_chunk_bytes(env: &EnvMap) -> usize {
    let value = env
        .get(MAX_CHUNK_BYTES_ENV)
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_CHUNK_BYTES);
    value.clamp(1, MAX_RETRIEVAL_MAX_CHUNK_BYTES)
}

fn bounded_time_budget_ms(env: &EnvMap) -> u64 {
    let value = env
        .get(TIME_BUDGET_MS_ENV)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_TIME_BUDGET_MS);
    value.clamp(1, MAX_RETRIEVAL_TIME_BUDGET_MS)
}

/// Resolve the contract for a RAG op.  Returns `Ok(contract)` for a data-plane
/// role, or `Err(result)` carrying the early outcome
/// (disabled/guard_refused/invalid) already classified + metered.
fn resolve_contract(
    env: &EnvMap,
    operation: RagOperation,
    started: std::time::Instant,
    record_metrics: bool,
) -> Result<EhdbContract, Box<RagResult>> {
    // Boxed cold error path — the result carries large crate outcome structs
    // (clippy::result_large_err).
    let finish = |outcome: RagOutcome, role: Option<EhdbClientRole>, detail: Option<String>| {
        Box::new(make_result(
            operation,
            outcome,
            role,
            started,
            detail,
            None,
            None,
            record_metrics,
        ))
    };

    let contract = match contract_from_env(env) {
        Ok(c) => c,
        Err(err) => {
            let role = super::contract::safe_client_role(env);
            let outcome = if role.map(|r| r.is_control_plane()).unwrap_or(false) {
                RagOutcome::GuardRefused
            } else {
                RagOutcome::Invalid
            };
            return Err(finish(outcome, role, Some(err.0)));
        }
    };

    if let Err(err) = assert_data_plane_access_allowed(contract.role, operation.as_str()) {
        return Err(finish(
            RagOutcome::GuardRefused,
            Some(contract.role),
            Some(err.to_string()),
        ));
    }
    if !contract.uses_local_reference_runtime() || contract.local_reference_log.is_none() {
        return Err(finish(RagOutcome::Disabled, Some(contract.role), None));
    }
    Ok(contract)
}

#[allow(clippy::too_many_arguments)]
fn make_result(
    operation: RagOperation,
    outcome: RagOutcome,
    role: Option<EhdbClientRole>,
    started: std::time::Instant,
    detail: Option<String>,
    retrieve: Option<RetrieveContextOutcome>,
    ingest: Option<IngestRetrievalDocumentOutcome>,
    record_metrics: bool,
) -> RagResult {
    let duration_seconds = started.elapsed().as_secs_f64();
    if record_metrics {
        metrics::record_rag(
            operation.as_str(),
            outcome.as_str(),
            outcome.ok(),
            outcome.degraded(),
            duration_seconds,
        );
    }
    RagResult {
        operation,
        outcome,
        role,
        duration_seconds,
        detail,
        retrieve,
        ingest,
    }
}

fn tenant_of(opts: &RagOptions) -> String {
    opts.tenant
        .clone()
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string())
}

fn namespace_of(opts: &RagOptions) -> String {
    opts.namespace
        .clone()
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string())
}

/// Ingest one document + its chunks into the derived retrieval fabric.  Disabled
/// ⇒ `Disabled` no-op.  A bound violation (empty/oversized chunk, duplicate id)
/// ⇒ `Rejected`; a bad identifier ⇒ `Invalid`.
///
/// ## Boundary (noetl/ai-meta#197, EHDB write-behind-cache §0.2 Slice C)
///
/// This ingest surface is **platform-only** — system docs / catalog embeddings.
/// **Never wire a playbook / user-facing `tool:` to `rag::ingest`.** User-document
/// RAG goes to the **user's own vector store via a connector**, never the platform
/// vector/RAG tier (D6) — otherwise user business data lands in EHDB, crossing the
/// boundary.  The surface is role-permissive by design (the platform selfcheck
/// drives it), so the guard is a **discipline**, not a type: the only caller today
/// is the diagnostic `ehdb-selfcheck` binary, and the
/// `no_playbook_tool_path_reaches_rag_ingest` guard test in this module fails if a
/// future change adds any other call site.
pub fn ingest(
    env: &EnvMap,
    document: &RagDocument,
    opts: &RagOptions,
    record_metrics: bool,
) -> RagResult {
    let op = RagOperation::Ingest;
    let started = std::time::Instant::now();

    if !truthy(env, EHDB_ENABLED_ENV) {
        return make_result(
            op,
            RagOutcome::Disabled,
            None,
            started,
            None,
            None,
            None,
            record_metrics,
        );
    }
    let contract = match resolve_contract(env, op, started, record_metrics) {
        Ok(c) => c,
        Err(result) => return *result,
    };

    let chunks = document
        .chunks
        .iter()
        .map(|c| IngestChunkInput {
            chunk_id: c.chunk_id.clone(),
            ordinal: c.ordinal,
            text: c.text.clone(),
            checksum: c.checksum.clone(),
        })
        .collect();

    let request = IngestRetrievalDocumentRequest {
        log_path: contract.local_reference_log.clone().expect("log present"),
        tenant: tenant_of(opts),
        namespace: namespace_of(opts),
        document_id: document.document_id.clone(),
        source_uri: document.source_uri.clone(),
        content_type: document.content_type.clone(),
        transaction_id: opts
            .transaction_id
            .clone()
            .unwrap_or_else(new_transaction_id),
        chunks,
    };

    match ingest_local_reference_retrieval_document(request) {
        Ok(outcome) => make_result(
            op,
            RagOutcome::Ingested,
            Some(contract.role),
            started,
            None,
            None,
            Some(outcome),
            record_metrics,
        ),
        Err(err) => make_result(
            op,
            classify_ingest_error(&err),
            Some(contract.role),
            started,
            Some(err.to_string()),
            None,
            None,
            record_metrics,
        ),
    }
}

/// Bounded, read-only retrieval over the derived retrieval fabric.  Disabled ⇒
/// `Disabled` no-op.  The `ehdb_reference` helper classifies over-ceiling caps as
/// `Rejected` and an empty query as `Invalid`; a hit/empty search is `Hit`/`Empty`.
pub fn retrieve(
    env: &EnvMap,
    query: &RagQuery,
    opts: &RagOptions,
    record_metrics: bool,
) -> RagResult {
    let op = RagOperation::Retrieve;
    let started = std::time::Instant::now();

    if !truthy(env, EHDB_ENABLED_ENV) {
        return make_result(
            op,
            RagOutcome::Disabled,
            None,
            started,
            None,
            None,
            None,
            record_metrics,
        );
    }
    let contract = match resolve_contract(env, op, started, record_metrics) {
        Ok(c) => c,
        Err(result) => return *result,
    };

    // A 0 cap ⇒ the env-configured default; the helper re-checks the ceiling.
    let top_k = if query.top_k == 0 {
        bounded_top_k(env)
    } else {
        query.top_k
    };
    let max_chunk_bytes = if query.max_chunk_bytes == 0 {
        bounded_max_chunk_bytes(env)
    } else {
        query.max_chunk_bytes
    };
    let time_budget_ms = if query.time_budget_ms == 0 {
        bounded_time_budget_ms(env)
    } else {
        query.time_budget_ms
    };

    let request = RetrieveContextRequest {
        log_path: contract.local_reference_log.clone().expect("log present"),
        tenant: tenant_of(opts),
        namespace: namespace_of(opts),
        query: query.query.clone(),
        top_k,
        max_chunk_bytes,
        time_budget_ms,
    };

    match retrieve_local_reference_context(request) {
        Ok(outcome) => {
            let rag_outcome = match outcome.outcome {
                RetrievalOutcome::Hit => RagOutcome::Hit,
                RetrievalOutcome::Empty => RagOutcome::Empty,
                RetrievalOutcome::Rejected => RagOutcome::Rejected,
                RetrievalOutcome::Invalid => RagOutcome::Invalid,
            };
            let detail = outcome.detail.clone();
            make_result(
                op,
                rag_outcome,
                Some(contract.role),
                started,
                detail,
                Some(outcome),
                None,
                record_metrics,
            )
        }
        Err(err) => make_result(
            op,
            RagOutcome::Unavailable,
            Some(contract.role),
            started,
            Some(err.to_string()),
            None,
            None,
            record_metrics,
        ),
    }
}

/// Classify an ingest error by its `ehdb_reference` Display prefix:
///
/// * `invalid identifier: …` — a bad document/chunk id ⇒ `Invalid`.
/// * `invalid state: … exceeds bound …` — an over-cap chunk count / byte len ⇒
///   `Rejected`.
/// * `invalid state: …` (e.g. "requires at least one chunk") ⇒ `Invalid`.
/// * `already exists: …` (a duplicate document/chunk id) ⇒ `Rejected`.
/// * anything else (storage / IO) ⇒ `Unavailable` (degraded).
fn classify_ingest_error<E: std::fmt::Display>(err: &E) -> RagOutcome {
    let text = err.to_string();
    if text.starts_with("invalid identifier") {
        RagOutcome::Invalid
    } else if text.starts_with("invalid state") {
        if text.contains("exceeds bound") {
            RagOutcome::Rejected
        } else {
            RagOutcome::Invalid
        }
    } else if text.starts_with("already exists") {
        RagOutcome::Rejected
    } else {
        RagOutcome::Unavailable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Boundary guard (noetl/ai-meta#197, EHDB write-behind-cache §0.2 Slice C).
    ///
    /// The platform vector/RAG tier (D6) must stay **platform-only** — system
    /// docs / catalog embeddings.  No user-facing / playbook `tool:` path may
    /// reach [`ingest`]: user-document RAG goes to the user's own vector store
    /// via a connector, never EHDB.  The ingest surface is role-permissive by
    /// design (the platform selfcheck drives it), so this is enforced as a
    /// discipline, not a type — this test scans the whole worker source tree and
    /// fails if `rag::ingest` is referenced anywhere but the diagnostic
    /// `ehdb-selfcheck` binary (the sole sanctioned caller) and the module that
    /// defines it. A future change that wires a user tool to the platform RAG
    /// ingest surface trips this test.
    #[test]
    fn no_playbook_tool_path_reaches_rag_ingest() {
        // Files allowed to reference `rag::ingest` — the diagnostic binary (the
        // only sanctioned caller) and the defining module itself.
        const ALLOWED_SUFFIXES: &[&str] = &["src/bin/ehdb-selfcheck.rs", "src/ehdb/rag.rs"];

        fn collect_rs_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            let entries = match std::fs::read_dir(dir) {
                Ok(e) => e,
                Err(_) => return,
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    collect_rs_files(&path, out);
                } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                    out.push(path);
                }
            }
        }

        let src_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        collect_rs_files(&src_root, &mut files);
        assert!(
            !files.is_empty(),
            "guard scan found no .rs files under {} — path resolution broke",
            src_root.display()
        );

        let mut offenders: Vec<String> = Vec::new();
        let mut sanctioned_hits = 0usize;
        for file in &files {
            let rel = file
                .strip_prefix(env!("CARGO_MANIFEST_DIR"))
                .unwrap_or(file)
                .to_string_lossy()
                .replace('\\', "/");
            let allowed = ALLOWED_SUFFIXES.iter().any(|s| rel.ends_with(s));
            let contents = std::fs::read_to_string(file).unwrap_or_default();
            for (i, line) in contents.lines().enumerate() {
                let trimmed = line.trim_start();
                // Skip comment / doc lines — the boundary is about live call
                // sites and `use` imports, not the prose that documents them.
                if trimmed.starts_with("//") || trimmed.starts_with('*') {
                    continue;
                }
                if line.contains("rag::ingest") {
                    if allowed {
                        sanctioned_hits += 1;
                    } else {
                        offenders.push(format!("{}:{}: {}", rel, i + 1, trimmed));
                    }
                }
            }
        }

        assert!(
            offenders.is_empty(),
            "user-facing/playbook code path reaches the platform RAG ingest surface \
             (noetl/ai-meta#197) — user-document RAG must go to the user's own vector \
             store via a connector, never `rag::ingest`:\n  {}",
            offenders.join("\n  ")
        );
        // Sanity: the scan actually resolved + matched the sanctioned caller, so
        // a future rename that hides the surface can't make this a silent no-op.
        assert!(
            sanctioned_hits > 0,
            "guard scan matched no `rag::ingest` reference even in the sanctioned \
             files — the scan or the surface moved; re-point the guard"
        );
    }

    fn env(pairs: &[(&str, &str)]) -> EnvMap {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
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
            "ehdb-rag-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        (dir.join("log.jsonl"), dir)
    }

    fn doc(id: &str, chunks: &[(&str, u32, &str)]) -> RagDocument {
        RagDocument {
            document_id: id.to_string(),
            source_uri: format!("artifact://{id}/source.md"),
            content_type: "text/markdown".to_string(),
            chunks: chunks
                .iter()
                .map(|(cid, ord, text)| RagChunk {
                    chunk_id: cid.to_string(),
                    ordinal: *ord,
                    text: text.to_string(),
                    checksum: format!("len-{}", text.len()),
                })
                .collect(),
        }
    }

    fn query(q: &str, top_k: usize, max_chunk_bytes: usize) -> RagQuery {
        RagQuery {
            query: q.to_string(),
            top_k,
            max_chunk_bytes,
            time_budget_ms: 0,
        }
    }

    #[test]
    fn disabled_is_noop() {
        let r = retrieve(
            &env(&[]),
            &query("retrieval", 8, 0),
            &Default::default(),
            false,
        );
        assert_eq!(r.outcome, RagOutcome::Disabled);
        assert!(r.retrieve.is_none());
        let i = ingest(
            &env(&[]),
            &doc("doc-x", &[("doc-x-0", 0, "text")]),
            &Default::default(),
            false,
        );
        assert_eq!(i.outcome, RagOutcome::Disabled);
        assert!(i.ingest.is_none());
    }

    #[test]
    fn ingest_then_retrieve_hit() {
        let (log, dir) = tmp_log("hit");
        let e = worker_env(log.to_str().unwrap());

        let i = ingest(
            &e,
            &doc(
                "doc-rag",
                &[
                    ("doc-rag-0", 0, "NoETL retrieval lineage lives with EHDB"),
                    ("doc-rag-1", 1, "weather report"),
                ],
            ),
            &Default::default(),
            false,
        );
        assert_eq!(i.outcome, RagOutcome::Ingested);
        assert_eq!(i.ingest.as_ref().unwrap().chunk_count, 2);

        let r = retrieve(&e, &query("retrieval", 8, 0), &Default::default(), false);
        assert_eq!(r.outcome, RagOutcome::Hit);
        let ro = r.retrieve.unwrap();
        assert_eq!(ro.returned, 1);
        assert_eq!(ro.hits[0].chunk_id, "doc-rag-0");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retrieve_empty_when_no_match() {
        let (log, dir) = tmp_log("empty");
        let e = worker_env(log.to_str().unwrap());
        ingest(
            &e,
            &doc("doc-e", &[("doc-e-0", 0, "only lineage")]),
            &Default::default(),
            false,
        );
        let r = retrieve(&e, &query("nomatchxyz", 8, 0), &Default::default(), false);
        assert_eq!(r.outcome, RagOutcome::Empty);
        assert_eq!(r.retrieve.unwrap().returned, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn over_limit_top_k_rejected() {
        let (log, dir) = tmp_log("reject");
        let e = worker_env(log.to_str().unwrap());
        ingest(
            &e,
            &doc("doc-r", &[("doc-r-0", 0, "retrieval content")]),
            &Default::default(),
            false,
        );
        let r = retrieve(
            &e,
            &query("retrieval", MAX_RETRIEVAL_TOP_K + 1, 0),
            &Default::default(),
            false,
        );
        assert_eq!(r.outcome, RagOutcome::Rejected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn empty_query_invalid() {
        let (log, dir) = tmp_log("invalid");
        let e = worker_env(log.to_str().unwrap());
        let r = retrieve(&e, &query("   ", 8, 0), &Default::default(), false);
        assert_eq!(r.outcome, RagOutcome::Invalid);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ingest_oversized_chunk_rejected() {
        let (log, dir) = tmp_log("ingest-reject");
        let e = worker_env(log.to_str().unwrap());
        let huge = "x".repeat(70_000);
        let r = ingest(
            &e,
            &doc("doc-big", &[("doc-big-0", 0, &huge)]),
            &Default::default(),
            false,
        );
        assert_eq!(r.outcome, RagOutcome::Rejected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retrieve_is_read_only() {
        let (log, dir) = tmp_log("readonly");
        let e = worker_env(log.to_str().unwrap());
        ingest(
            &e,
            &doc("doc-ro", &[("doc-ro-0", 0, "retrieval read only")]),
            &Default::default(),
            false,
        );
        for _ in 0..3 {
            let r = retrieve(&e, &query("retrieval", 8, 0), &Default::default(), false);
            assert_eq!(r.outcome, RagOutcome::Hit);
        }
        // Only the single ingest wrote a transaction.
        let follow = retrieve(&e, &query("retrieval", 8, 0), &Default::default(), false);
        assert_eq!(follow.retrieve.unwrap().returned, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn control_plane_role_guard_refused() {
        for role in ["gateway", "api", "server"] {
            let e = env(&[
                ("NOETL_EHDB_ENABLED", "true"),
                ("NOETL_EHDB_MODE", "local_reference"),
                ("NOETL_EHDB_CLIENT_ROLE", role),
                ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ]);
            let r = retrieve(&e, &query("retrieval", 8, 0), &Default::default(), false);
            assert_eq!(r.outcome, RagOutcome::GuardRefused);
            assert!(r.retrieve.is_none());
            let i = ingest(
                &e,
                &doc("doc-x", &[("doc-x-0", 0, "text")]),
                &Default::default(),
                false,
            );
            assert_eq!(i.outcome, RagOutcome::GuardRefused);
            assert!(i.ingest.is_none());
        }
    }
}
