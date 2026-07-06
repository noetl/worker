//! `ehdb-selfcheck` — a worker/playbook-local driver for the in-process EHDB
//! integration (noetl/ehdb#234), shipped in the same image as `noetl-worker`.
//!
//! It exercises the exact `noetl_worker::ehdb` code the worker links — readiness,
//! data-plane append/read, event-stream project/consume/ack, the control-plane
//! guard, and the secret-free `/metrics` render — reading the same
//! `NOETL_EHDB_*` env the ops Helm chart renders.  This is the Rust-only
//! replacement for the retired Python `scripts/ehdb_*_step.py` / smoke scripts,
//! and the artifact the kind validation runs against the worker-rust image.
//!
//! It is NOT a server endpoint and NOT part of the worker's request path.
//!
//! Exit codes convey the terminal outcome so a shell harness can assert:
//!   0 = ok / disabled no-op / control_plane        3 = rejected (bound)
//!   4 = guard_refused / invalid (control-plane boundary or misconfig)
//!   5 = unavailable / truncated (degraded)         2 = usage error

use std::collections::HashMap;
use std::process::ExitCode;

use noetl_worker::ehdb::{
    self, dataplane, eventlog, eventstream, kv, object, projection, rag, readiness, systemstore,
    vector,
};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flags = match parse_flags(&args) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("{e}\n{}", usage());
            return ExitCode::from(2);
        }
    };
    let env = ehdb::process_env();

    match args.first().map(String::as_str) {
        Some("readiness") => run_readiness(&env),
        Some("append") => run_append(&env, &flags),
        Some("read") => run_read(&env, &flags),
        Some("project") => run_project(&env, &flags),
        Some("consume") => run_consume(&env, &flags),
        Some("ack") => run_ack(&env, &flags),
        Some("suite") => run_suite(&env, &flags),
        Some("publish-system") => run_publish_system(&env, &flags),
        Some("bind-system") => run_bind_system(&env, &flags),
        Some("resolve-system") => run_resolve_system(&env, &flags),
        Some("system-suite") => run_system_suite(&env, &flags),
        Some("ingest-rag") => run_ingest_rag(&env, &flags),
        Some("retrieve-rag") => run_retrieve_rag(&env, &flags),
        Some("rag-suite") => run_rag_suite(&env, &flags),
        Some("mirror-eventlog") => run_mirror_eventlog(&env, &flags),
        Some("eventlog-suite") => run_eventlog_suite(&env, &flags),
        Some("eventlog-primary-serve") => run_eventlog_primary_serve(&env, &flags),
        Some("mirror-projection") => run_mirror_projection(&env, &flags),
        Some("projection-suite") => run_projection_suite(&env, &flags),
        Some("projection-primary-serve") => run_projection_primary_serve(&env, &flags),
        Some("mirror-kv") => run_mirror_kv(&env, &flags),
        Some("kv-suite") => run_kv_suite(&env, &flags),
        Some("kv-primary-serve") => run_kv_primary_serve(&env, &flags),
        Some("mirror-object") => run_mirror_object(&env, &flags),
        Some("object-suite") => run_object_suite(&env, &flags),
        Some("mirror-vector") => run_mirror_vector(&env, &flags),
        Some("vector-suite") => run_vector_suite(&env, &flags),
        Some("metrics") => {
            print!("{}", render_metrics());
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!("{}", usage());
            ExitCode::from(2)
        }
    }
}

fn run_readiness(env: &ehdb::EnvMap) -> ExitCode {
    let r = readiness::evaluate(env, true);
    println!(
        "{}",
        serde_json::json!({
            "op": "readiness",
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "ready": r.outcome.ready(),
            "degraded": r.outcome.degraded(),
            "total_count": r.total_count,
            "duration_seconds": round6(r.duration_seconds),
            "detail": r.detail,
        })
    );
    print_metrics_footer();
    readiness_exit(r.outcome)
}

fn run_append(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let stream = flags.req("stream");
    let subject = flags.req("subject");
    let payload = flags.req("payload");
    let (stream, subject, payload) = match (stream, subject, payload) {
        (Some(a), Some(b), Some(c)) => (a, b, c),
        _ => return usage_exit(),
    };
    let r =
        dataplane::append_domain_record(env, &stream, &subject, &payload, &dp_opts(flags), true);
    println!(
        "{}",
        serde_json::json!({
            "op": "append",
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "detail": r.detail,
            "append": r.append,
        })
    );
    print_metrics_footer();
    dataplane_exit(r.outcome)
}

fn run_read(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let Some(stream) = flags.req("stream") else {
        return usage_exit();
    };
    let limit = flags.parse_usize("limit");
    let after = flags.parse_u64("after");
    let r = dataplane::read_domain_records(env, &stream, after, limit, &dp_opts(flags), true);
    println!(
        "{}",
        serde_json::json!({
            "op": "read",
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "detail": r.detail,
            "read": r.read,
        })
    );
    print_metrics_footer();
    dataplane_exit(r.outcome)
}

fn run_project(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let (Some(stream), Some(subject), Some(payload)) = (
        flags.req("stream"),
        flags.req("subject"),
        flags.req("payload"),
    ) else {
        return usage_exit();
    };
    let r = eventstream::project_event(env, &stream, &subject, &payload, &es_opts(flags), true);
    println!(
        "{}",
        serde_json::json!({
            "op": "project",
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "detail": r.detail,
            "project": r.project,
        })
    );
    print_metrics_footer();
    eventstream_exit(r.outcome)
}

fn run_consume(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let (Some(stream), Some(consumer)) = (flags.req("stream"), flags.req("consumer")) else {
        return usage_exit();
    };
    let limit = flags.parse_usize("limit");
    let r = eventstream::consume_events(env, &stream, &consumer, limit, &es_opts(flags), true);
    println!(
        "{}",
        serde_json::json!({
            "op": "consume",
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "detail": r.detail,
            "consume": r.consume,
        })
    );
    print_metrics_footer();
    eventstream_exit(r.outcome)
}

fn run_ack(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let (Some(stream), Some(consumer), Some(sequence)) = (
        flags.req("stream"),
        flags.req("consumer"),
        flags.parse_u64("sequence"),
    ) else {
        return usage_exit();
    };
    let r = eventstream::ack_events(env, &stream, &consumer, sequence, &es_opts(flags), true);
    println!(
        "{}",
        serde_json::json!({
            "op": "ack",
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "detail": r.detail,
            "ack": r.ack,
        })
    );
    print_metrics_footer();
    eventstream_exit(r.outcome)
}

/// Run a full deterministic drive in ONE process: readiness → append → read →
/// project → consume → ack → consume-again.  Prints a JSON report + the rendered
/// EHDB metrics.  Exit 0 only when the whole sequence hit its expected outcomes
/// (or EHDB is disabled, in which case it proves the byte-identical no-op:
/// readiness=disabled + empty metrics).
fn run_suite(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let stream = flags
        .get("stream")
        .unwrap_or_else(|| "selfcheck_events".to_string());
    let consumer = flags
        .get("consumer")
        .unwrap_or_else(|| "selfcheck_drain".to_string());
    let subject = "noetl.events.selfcheck".to_string();

    let mut steps = Vec::new();
    let mut ok = true;

    let rd = readiness::evaluate(env, true);
    steps.push(serde_json::json!({"step":"readiness","outcome": rd.outcome.as_str()}));

    if rd.outcome == readiness::ReadinessOutcome::Disabled {
        // Disabled no-op: metrics must be empty (byte-identical proof).
        let metrics = render_metrics();
        println!(
            "{}",
            serde_json::json!({
                "suite":"ehdb-selfcheck",
                "ehdb":"disabled",
                "steps": steps,
                "metrics_empty": metrics.is_empty(),
            })
        );
        print!("{metrics}");
        return if metrics.is_empty() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    }

    let a = dataplane::append_domain_record(
        env,
        &stream,
        &subject,
        "{\"seq\":1}",
        &dp_opts(flags),
        true,
    );
    ok &= a.outcome == dataplane::DataPlaneOutcome::Appended;
    steps.push(serde_json::json!({"step":"append","outcome": a.outcome.as_str()}));

    let rr = dataplane::read_domain_records(env, &stream, None, Some(10), &dp_opts(flags), true);
    ok &= rr.outcome == dataplane::DataPlaneOutcome::Read
        && rr.read.as_ref().map(|x| x.returned).unwrap_or(0) >= 1;
    steps.push(
        serde_json::json!({"step":"read","outcome": rr.outcome.as_str(),
        "returned": rr.read.as_ref().map(|x| x.returned)}),
    );

    let p =
        eventstream::project_event(env, &stream, &subject, "{\"seq\":2}", &es_opts(flags), true);
    ok &= p.outcome == eventstream::EventStreamOutcome::Projected;
    steps.push(serde_json::json!({"step":"project","outcome": p.outcome.as_str()}));

    let c = eventstream::consume_events(env, &stream, &consumer, Some(10), &es_opts(flags), true);
    let pending_before = c.consume.as_ref().map(|x| x.pending_count).unwrap_or(0);
    ok &= c.outcome == eventstream::EventStreamOutcome::Consumed && pending_before >= 1;
    steps.push(
        serde_json::json!({"step":"consume","outcome": c.outcome.as_str(),
        "pending": pending_before}),
    );

    let ak = eventstream::ack_events(env, &stream, &consumer, 1, &es_opts(flags), true);
    ok &= ak.outcome == eventstream::EventStreamOutcome::Acked;
    steps.push(serde_json::json!({"step":"ack","outcome": ak.outcome.as_str()}));

    let c2 = eventstream::consume_events(env, &stream, &consumer, Some(10), &es_opts(flags), true);
    let pending_after = c2.consume.as_ref().map(|x| x.pending_count).unwrap_or(0);
    // The ack advanced the cursor, so the second consume sees one fewer pending.
    ok &= pending_after + 1 == pending_before;
    steps.push(
        serde_json::json!({"step":"consume_after_ack","outcome": c2.outcome.as_str(),
        "pending": pending_after}),
    );

    let metrics = render_metrics();
    println!(
        "{}",
        serde_json::json!({
            "suite":"ehdb-selfcheck",
            "ehdb":"enabled",
            "role": rd.role.map(|x| x.as_str()),
            "ok": ok,
            "steps": steps,
            "metrics_secret_free": metrics_is_secret_free(&metrics),
        })
    );
    print!("{metrics}");

    if ok && metrics_is_secret_free(&metrics) {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

// --- system WASM library store (EHDB Phase E) ------------------------------

fn ss_opts(flags: &Flags) -> systemstore::SystemStoreOptions {
    systemstore::SystemStoreOptions {
        tenant: flags.get("tenant"),
        namespace: flags.get("namespace"),
        transaction_id: flags.get("transaction-id"),
    }
}

fn manifest_from_flags(flags: &Flags) -> Option<systemstore::ModuleManifest> {
    Some(systemstore::ModuleManifest {
        path: flags.req("path")?,
        revision: flags.parse_u64("revision")? as u32,
        digest: flags.req("digest")?,
        entry: flags.req("entry")?,
        target: flags.req("target")?,
        object_path: flags.req("object-path")?,
        byte_len: flags.parse_u64("byte-len")?,
        capabilities: flags
            .req("capabilities")?
            .split(',')
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty())
            .collect(),
    })
}

fn run_publish_system(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let Some(manifest) = manifest_from_flags(flags) else {
        return usage_exit();
    };
    let r = systemstore::publish_module(env, &manifest, &ss_opts(flags), true);
    println!(
        "{}",
        serde_json::json!({
            "op": "publish-system",
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "detail": r.detail,
            "publish": r.publish,
        })
    );
    print_metrics_footer();
    systemstore_exit(r.outcome)
}

fn run_bind_system(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let (Some(environment), Some(channel), Some(path), Some(revision), Some(digest)) = (
        flags.req("environment"),
        flags.req("channel"),
        flags.req("path"),
        flags.parse_u64("revision"),
        flags.req("digest"),
    ) else {
        return usage_exit();
    };
    let r = systemstore::bind_channel(
        env,
        &systemstore::ChannelBinding {
            environment,
            channel,
            path,
            revision: revision as u32,
            digest,
        },
        &ss_opts(flags),
        true,
    );
    println!(
        "{}",
        serde_json::json!({
            "op": "bind-system",
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "detail": r.detail,
            "bind": r.bind,
        })
    );
    print_metrics_footer();
    systemstore_exit(r.outcome)
}

fn run_resolve_system(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let (Some(environment), Some(channel), Some(path)) = (
        flags.req("environment"),
        flags.req("channel"),
        flags.req("path"),
    ) else {
        return usage_exit();
    };
    let r = systemstore::resolve_module(env, &environment, &channel, &path, &ss_opts(flags), true);
    println!(
        "{}",
        serde_json::json!({
            "op": "resolve-system",
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "detail": r.detail,
            "resolve": r.resolve,
        })
    );
    print_metrics_footer();
    systemstore_exit(r.outcome)
}

/// Run a full deterministic Phase-E drive in ONE process:
/// resolve(absent) → publish rev1 → bind → resolve(rev1) → publish rev2 →
/// rebind rev2 → resolve(rev2).  Proves publish/bind/resolve, the absent probe,
/// and the hot-replace-on-rebind semantic.  Disabled ⇒ byte-identical no-op
/// proof (resolve=disabled + empty metrics), same shape as `suite`.
fn run_system_suite(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let path = flags
        .get("path")
        .unwrap_or_else(|| "system/selfcheck_render".to_string());
    let environment = flags
        .get("environment")
        .unwrap_or_else(|| "prod".to_string());
    let channel = flags.get("channel").unwrap_or_else(|| "stable".to_string());
    let digest1 = format!("sha256:{:064x}", 1);
    let digest2 = format!("sha256:{:064x}", 2);
    let mk = |revision: u32, digest: &str| systemstore::ModuleManifest {
        path: path.clone(),
        revision,
        digest: digest.to_string(),
        entry: "render".to_string(),
        target: "wasm32-wasi-preview1".to_string(),
        object_path: format!("{path}/{revision}.wasm"),
        byte_len: 512,
        capabilities: vec!["event_publish".to_string()],
    };

    // Disabled short-circuit: prove the byte-identical no-op.
    let pre =
        systemstore::resolve_module(env, &environment, &channel, &path, &ss_opts(flags), true);
    if pre.outcome == systemstore::SystemStoreOutcome::Disabled {
        let metrics = render_metrics();
        println!(
            "{}",
            serde_json::json!({
                "suite":"ehdb-system-selfcheck",
                "ehdb":"disabled",
                "metrics_empty": metrics.is_empty(),
            })
        );
        print!("{metrics}");
        return if metrics.is_empty() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    }

    let mut steps = Vec::new();
    let mut ok = true;

    // Resolve before any bind is the absent probe.
    ok &= pre.outcome == systemstore::SystemStoreOutcome::Absent;
    steps.push(serde_json::json!({"step":"resolve_absent","outcome": pre.outcome.as_str()}));

    let p1 = systemstore::publish_module(env, &mk(1, &digest1), &ss_opts(flags), true);
    ok &= p1.outcome == systemstore::SystemStoreOutcome::Published;
    steps.push(serde_json::json!({"step":"publish_rev1","outcome": p1.outcome.as_str()}));

    let b1 = systemstore::bind_channel(
        env,
        &systemstore::ChannelBinding {
            environment: environment.clone(),
            channel: channel.clone(),
            path: path.clone(),
            revision: 1,
            digest: digest1.clone(),
        },
        &ss_opts(flags),
        true,
    );
    ok &= b1.outcome == systemstore::SystemStoreOutcome::Bound;
    steps.push(serde_json::json!({"step":"bind_rev1","outcome": b1.outcome.as_str()}));

    let r1 = systemstore::resolve_module(env, &environment, &channel, &path, &ss_opts(flags), true);
    let rev1 = r1.resolve.as_ref().and_then(|x| x.revision);
    ok &= r1.outcome == systemstore::SystemStoreOutcome::Resolved && rev1 == Some(1);
    steps.push(
        serde_json::json!({"step":"resolve_rev1","outcome": r1.outcome.as_str(),"revision": rev1}),
    );

    let p2 = systemstore::publish_module(env, &mk(2, &digest2), &ss_opts(flags), true);
    ok &= p2.outcome == systemstore::SystemStoreOutcome::Published;
    steps.push(serde_json::json!({"step":"publish_rev2","outcome": p2.outcome.as_str()}));

    let b2 = systemstore::bind_channel(
        env,
        &systemstore::ChannelBinding {
            environment: environment.clone(),
            channel: channel.clone(),
            path: path.clone(),
            revision: 2,
            digest: digest2.clone(),
        },
        &ss_opts(flags),
        true,
    );
    ok &= b2.outcome == systemstore::SystemStoreOutcome::Bound;
    steps.push(serde_json::json!({"step":"rebind_rev2","outcome": b2.outcome.as_str()}));

    // Rebind hot-replaces the active module: resolve now returns rev2.
    let r2 = systemstore::resolve_module(env, &environment, &channel, &path, &ss_opts(flags), true);
    let rev2 = r2.resolve.as_ref().and_then(|x| x.revision);
    ok &= r2.outcome == systemstore::SystemStoreOutcome::Resolved && rev2 == Some(2);
    steps.push(
        serde_json::json!({"step":"resolve_rev2","outcome": r2.outcome.as_str(),"revision": rev2}),
    );

    let metrics = render_metrics();
    println!(
        "{}",
        serde_json::json!({
            "suite":"ehdb-system-selfcheck",
            "ehdb":"enabled",
            "role": pre.role.map(|x| x.as_str()),
            "ok": ok,
            "steps": steps,
            "metrics_secret_free": metrics_is_secret_free(&metrics),
        })
    );
    print!("{metrics}");

    if ok && metrics_is_secret_free(&metrics) {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

// --- bounded RAG retrieval (EHDB Phase E) ----------------------------------

fn rag_opts(flags: &Flags) -> rag::RagOptions {
    rag::RagOptions {
        tenant: flags.get("tenant"),
        namespace: flags.get("namespace"),
        transaction_id: flags.get("transaction-id"),
    }
}

/// Build a document from `--document-id` + a `||`-joined `--chunks` string;
/// ordinals + chunk ids are assigned positionally.
fn document_from_flags(flags: &Flags) -> Option<rag::RagDocument> {
    let document_id = flags.req("document-id")?;
    let chunks_raw = flags.req("chunks")?;
    let chunks = chunks_raw
        .split("||")
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .enumerate()
        .map(|(index, text)| rag::RagChunk {
            chunk_id: format!("{document_id}-{index}"),
            ordinal: index as u32,
            text: text.to_string(),
            checksum: format!("len-{}", text.len()),
        })
        .collect();
    Some(rag::RagDocument {
        document_id,
        source_uri: flags
            .get("source-uri")
            .unwrap_or_else(|| "artifact://selfcheck/source.md".to_string()),
        content_type: flags
            .get("content-type")
            .unwrap_or_else(|| "text/plain".to_string()),
        chunks,
    })
}

fn query_from_flags(flags: &Flags) -> Option<rag::RagQuery> {
    Some(rag::RagQuery {
        query: flags.req("query")?,
        top_k: flags.parse_usize("top-k").unwrap_or(0),
        max_chunk_bytes: flags.parse_usize("max-chunk-bytes").unwrap_or(0),
        time_budget_ms: flags.parse_u64("time-budget-ms").unwrap_or(0),
    })
}

fn run_ingest_rag(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let Some(document) = document_from_flags(flags) else {
        return usage_exit();
    };
    let r = rag::ingest(env, &document, &rag_opts(flags), true);
    println!(
        "{}",
        serde_json::json!({
            "op": "ingest-rag",
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "detail": r.detail,
            "ingest": r.ingest,
        })
    );
    print_metrics_footer();
    rag_exit(r.outcome)
}

fn run_retrieve_rag(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let Some(query) = query_from_flags(flags) else {
        return usage_exit();
    };
    let r = rag::retrieve(env, &query, &rag_opts(flags), true);
    println!(
        "{}",
        serde_json::json!({
            "op": "retrieve-rag",
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "detail": r.detail,
            "retrieve": r.retrieve,
        })
    );
    print_metrics_footer();
    rag_exit(r.outcome)
}

/// Run a full deterministic RAG drive in ONE process: ingest a 3-chunk document
/// → retrieve (top-k truncation) → retrieve (empty) → retrieve (over-limit
/// rejected).  Proves ingest + bounded retrieval + the top-k / reject bounds.
/// Disabled ⇒ byte-identical no-op proof (retrieve=disabled + empty metrics),
/// same shape as `suite` / `system-suite`.
fn run_rag_suite(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let document_id = flags
        .get("document-id")
        .unwrap_or_else(|| "rag_selfcheck".to_string());

    // Disabled short-circuit: prove the byte-identical no-op.
    let pre = rag::retrieve(
        env,
        &rag::RagQuery {
            query: "retrieval".to_string(),
            top_k: 0,
            max_chunk_bytes: 0,
            time_budget_ms: 0,
        },
        &rag_opts(flags),
        true,
    );
    if pre.outcome == rag::RagOutcome::Disabled {
        let metrics = render_metrics();
        println!(
            "{}",
            serde_json::json!({
                "suite":"ehdb-rag-selfcheck",
                "ehdb":"disabled",
                "metrics_empty": metrics.is_empty(),
            })
        );
        print!("{metrics}");
        return if metrics.is_empty() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    }

    let mut steps = Vec::new();
    let mut ok = true;

    let document = rag::RagDocument {
        document_id: document_id.clone(),
        source_uri: format!("artifact://{document_id}/source.md"),
        content_type: "text/markdown".to_string(),
        chunks: vec![
            rag::RagChunk {
                chunk_id: format!("{document_id}-0"),
                ordinal: 0,
                text: "retrieval retrieval retrieval alpha lineage".to_string(),
                checksum: "c0".to_string(),
            },
            rag::RagChunk {
                chunk_id: format!("{document_id}-1"),
                ordinal: 1,
                text: "retrieval retrieval beta".to_string(),
                checksum: "c1".to_string(),
            },
            rag::RagChunk {
                chunk_id: format!("{document_id}-2"),
                ordinal: 2,
                text: "retrieval gamma".to_string(),
                checksum: "c2".to_string(),
            },
        ],
    };

    let ing = rag::ingest(env, &document, &rag_opts(flags), true);
    ok &= ing.outcome == rag::RagOutcome::Ingested;
    steps.push(
        serde_json::json!({"step":"ingest","outcome": ing.outcome.as_str(),
        "chunks": ing.ingest.as_ref().map(|x| x.chunk_count)}),
    );

    // Retrieve with a small top-k to exercise the top-k truncation bound.
    let hit = rag::retrieve(
        env,
        &rag::RagQuery {
            query: "retrieval".to_string(),
            top_k: 2,
            max_chunk_bytes: 0,
            time_budget_ms: 0,
        },
        &rag_opts(flags),
        true,
    );
    let hit_ro = hit.retrieve.as_ref();
    ok &= hit.outcome == rag::RagOutcome::Hit
        && hit_ro.map(|x| x.returned).unwrap_or(0) == 2
        && hit_ro.map(|x| x.truncated_by_top_k).unwrap_or(false);
    steps.push(
        serde_json::json!({"step":"retrieve_hit","outcome": hit.outcome.as_str(),
        "returned": hit_ro.map(|x| x.returned),
        "candidate_count": hit_ro.map(|x| x.candidate_count),
        "truncated_by_top_k": hit_ro.map(|x| x.truncated_by_top_k)}),
    );

    let empty = rag::retrieve(
        env,
        &rag::RagQuery {
            query: "nonexistentterm".to_string(),
            top_k: 0,
            max_chunk_bytes: 0,
            time_budget_ms: 0,
        },
        &rag_opts(flags),
        true,
    );
    ok &= empty.outcome == rag::RagOutcome::Empty;
    steps.push(serde_json::json!({"step":"retrieve_empty","outcome": empty.outcome.as_str()}));

    // Over-ceiling top-k ⇒ Rejected (no search).
    let rejected = rag::retrieve(
        env,
        &rag::RagQuery {
            query: "retrieval".to_string(),
            top_k: 65,
            max_chunk_bytes: 0,
            time_budget_ms: 0,
        },
        &rag_opts(flags),
        true,
    );
    ok &= rejected.outcome == rag::RagOutcome::Rejected;
    steps
        .push(serde_json::json!({"step":"retrieve_rejected","outcome": rejected.outcome.as_str()}));

    let metrics = render_metrics();
    println!(
        "{}",
        serde_json::json!({
            "suite":"ehdb-rag-selfcheck",
            "ehdb":"enabled",
            "role": pre.role.map(|x| x.as_str()),
            "ok": ok,
            "steps": steps,
            "metrics_secret_free": metrics_is_secret_free(&metrics),
        })
    );
    print!("{metrics}");

    if ok && metrics_is_secret_free(&metrics) {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

// --- event-log shadow (EHDB Phase 6) ---------------------------------------

fn el_opts(flags: &Flags) -> eventlog::EventLogOptions {
    eventlog::EventLogOptions {
        tenant: flags.get("tenant"),
        namespace: flags.get("namespace"),
        transaction_id: flags.get("transaction-id"),
    }
}

fn run_mirror_eventlog(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let (Some(execution_id), Some(payload)) = (flags.req("execution-id"), flags.req("payload"))
    else {
        return usage_exit();
    };
    let authoritative = flags.parse_u64("authoritative-sequence");
    let r = eventlog::mirror_event(
        env,
        &execution_id,
        authoritative,
        &payload,
        &el_opts(flags),
        true,
    );
    println!(
        "{}",
        serde_json::json!({
            "op": "mirror-eventlog",
            "mode": r.mode.as_str(),
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "detail": r.detail,
            "global_sequence": r.global_sequence,
            "parity": r.parity,
        })
    );
    print_metrics_footer();
    eventlog_exit(r.outcome)
}

/// Deterministic one-process event-log shadow drive: mirror three events into a
/// fresh log with a controlled 1-based authoritative sequence.  Off / disabled
/// ⇒ byte-identical no-op proof (mirror=disabled + empty metrics), same shape as
/// `suite` / `rag-suite`.  Enabled ⇒ every mirror holds parity
/// (`global_sequence == log_record_count`, and == the authoritative sequence).
fn run_eventlog_suite(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let execution_id = flags
        .get("execution-id")
        .unwrap_or_else(|| "100".to_string());

    // First mirror doubles as the disabled probe: in off/disabled mode it does
    // not append; in shadow mode it is event seq 1.
    let first = eventlog::mirror_event(
        env,
        &execution_id,
        Some(1),
        "{\"seq\":1}",
        &el_opts(flags),
        true,
    );
    if first.outcome == eventlog::EventLogOutcome::Disabled {
        let metrics = render_metrics();
        println!(
            "{}",
            serde_json::json!({
                "suite": "ehdb-eventlog-selfcheck",
                "ehdb": "disabled",
                "mode": first.mode.as_str(),
                "metrics_empty": metrics.is_empty(),
            })
        );
        print!("{metrics}");
        return if metrics.is_empty() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    }

    let mut steps = Vec::new();
    let mut ok = first.outcome == eventlog::EventLogOutcome::Mirrored;
    steps.push(
        serde_json::json!({"step":"mirror_1","outcome": first.outcome.as_str(),
        "global_sequence": first.global_sequence}),
    );

    for seq in [2u64, 3] {
        let r = eventlog::mirror_event(
            env,
            &execution_id,
            Some(seq),
            &format!("{{\"seq\":{seq}}}"),
            &el_opts(flags),
            true,
        );
        ok &= r.outcome == eventlog::EventLogOutcome::Mirrored
            && r.parity.as_ref().map(|p| p.holds()).unwrap_or(false);
        steps.push(serde_json::json!({"step": format!("mirror_{seq}"),
            "outcome": r.outcome.as_str(), "global_sequence": r.global_sequence,
            "parity_holds": r.parity.as_ref().map(|p| p.holds())}));
    }

    let metrics = render_metrics();
    println!(
        "{}",
        serde_json::json!({
            "suite": "ehdb-eventlog-selfcheck",
            "ehdb": "enabled",
            "mode": first.mode.as_str(),
            "role": first.role.map(|x| x.as_str()),
            "ok": ok,
            "steps": steps,
            "metrics_secret_free": metrics_is_secret_free(&metrics),
        })
    );
    print!("{metrics}");

    if ok && metrics_is_secret_free(&metrics) {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn eventlog_exit(outcome: eventlog::EventLogOutcome) -> ExitCode {
    use eventlog::EventLogOutcome as O;
    match outcome {
        O::GuardRefused | O::Invalid | O::PrimaryUnavailable => ExitCode::from(4),
        O::Rejected => ExitCode::from(3),
        O::Unavailable | O::ParityMismatch | O::PrimaryDivergence => ExitCode::from(5),
        _ => ExitCode::SUCCESS,
    }
}

/// Phase 9 tier-1 primary cutover: serve the platform event log authoritatively
/// from EHDB and prove it in one process.  Off / disabled ⇒ byte-identical no-op
/// (served=false + empty metrics).  Enabled + `NOETL_EHDB_EVENTLOG=primary` ⇒ the
/// full authoritative cycle (append → scan → read → tail → ack → replay) is
/// served-by-EHDB with dual-run parity, AND flipping back to `shadow` restores
/// the incumbent path over the same log with zero data loss (reversibility).
/// Exit 0 only when served-by-EHDB AND reversible AND metrics stay secret-free.
fn run_eventlog_primary_serve(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let r = eventlog::serve_primary_cycle(env, &el_opts(flags), true);

    // Off / disabled probe: byte-identical no-op, same shape as `eventlog-suite`.
    if r.outcome == eventlog::EventLogOutcome::Disabled {
        let metrics = render_metrics();
        println!(
            "{}",
            serde_json::json!({
                "suite": "ehdb-eventlog-primary-serve",
                "ehdb": "disabled",
                "mode": r.mode.as_str(),
                "metrics_empty": metrics.is_empty(),
            })
        );
        print!("{metrics}");
        return if metrics.is_empty() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    }

    let metrics = render_metrics();
    let secret_free = metrics_is_secret_free(&metrics);
    println!(
        "{}",
        serde_json::json!({
            "suite": "ehdb-eventlog-primary-serve",
            "ehdb": "enabled",
            "op": "eventlog-primary-serve",
            "mode": r.mode.as_str(),
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "served_by_ehdb": r.served_by_ehdb,
            "reversible": r.reversible,
            "records_after_revert": r.records_after_revert,
            "duration_seconds": round6(r.duration_seconds),
            "detail": r.detail,
            "report": r.report,
            "metrics_secret_free": secret_free,
        })
    );
    print!("{metrics}");

    if r.outcome == eventlog::EventLogOutcome::ServedPrimary
        && r.served_by_ehdb
        && r.reversible
        && secret_free
    {
        ExitCode::SUCCESS
    } else {
        eventlog_exit(r.outcome)
    }
}

// --- projection read-model shadow (EHDB Phase 7) ---------------------------

fn pr_opts(flags: &Flags) -> projection::ProjectionOptions {
    projection::ProjectionOptions {
        tenant: flags.get("tenant"),
        namespace: flags.get("namespace"),
        consumer: flags.get("consumer"),
        transaction_id: flags.get("transaction-id"),
    }
}

/// Build a deterministic `count`-event drive for one execution: event 1 starts
/// (running), the middle events are `command.completed`, the last is
/// `playbook.completed` (terminal).  Returns the events plus the authoritative
/// materializer fold the shadow must match: one completed/terminal execution
/// with `event_count == count` and offset `count`.
fn projection_drive(
    execution_id: &str,
    count: u64,
) -> (
    Vec<ehdb_reference::ProjectionEventInput>,
    Vec<ehdb_reference::AuthoritativeExecutionState>,
    u64,
) {
    let count = count.max(1);
    let events = (1..=count)
        .map(|seq| {
            let (event_type, node, status) = if seq == count {
                ("playbook.completed", "finish", "completed")
            } else if seq == 1 {
                ("playbook_started", "start", "running")
            } else {
                ("command.completed", "load", "completed")
            };
            ehdb_reference::ProjectionEventInput {
                global_sequence: seq,
                event_id: 10 + seq as i64,
                execution_id: execution_id.to_string(),
                event_type: event_type.to_string(),
                node_name: Some(node.to_string()),
                status: Some(status.to_string()),
                prev_event_id: None,
            }
        })
        .collect();
    let authoritative = vec![ehdb_reference::AuthoritativeExecutionState {
        execution_id: execution_id.to_string(),
        status: "completed".to_string(),
        event_count: count as usize,
        terminal: true,
    }];
    (events, authoritative, count)
}

fn run_mirror_projection(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let Some(execution_id) = flags.req("execution-id") else {
        return usage_exit();
    };
    let count = flags.parse_u64("events").unwrap_or(3);
    let (events, authoritative, offset) = projection_drive(&execution_id, count);
    let r = projection::shadow_project(
        env,
        &events,
        &authoritative,
        Some(offset),
        &pr_opts(flags),
        true,
    );
    println!(
        "{}",
        serde_json::json!({
            "op": "mirror-projection",
            "mode": r.mode.as_str(),
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "detail": r.detail,
            "applied": r.applied,
            "checkpoint": r.checkpoint,
            "parity": r.parity,
        })
    );
    print_metrics_footer();
    projection_exit(r.outcome)
}

/// Deterministic one-process projection shadow drive: materialize a three-event
/// drive into a fresh projection store and compare against the authoritative
/// fold.  Off / disabled ⇒ byte-identical no-op proof (materialize=disabled +
/// empty metrics), same shape as `eventlog-suite`.  Enabled ⇒ the read-models
/// hold parity against the authoritative materializer and the checkpoint catches
/// up (lag 0); a second apply is an idempotent replay (applied 0, parity holds).
fn run_projection_suite(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let execution_id = flags
        .get("execution-id")
        .unwrap_or_else(|| "100".to_string());
    let (events, authoritative, offset) = projection_drive(&execution_id, 3);

    // First apply doubles as the disabled probe: in off/disabled mode it does not
    // materialize; in shadow mode it materializes the whole drive.
    let first = projection::shadow_project(
        env,
        &events,
        &authoritative,
        Some(offset),
        &pr_opts(flags),
        true,
    );
    if first.outcome == projection::ProjectionOutcome::Disabled {
        let metrics = render_metrics();
        println!(
            "{}",
            serde_json::json!({
                "suite": "ehdb-projection-selfcheck",
                "ehdb": "disabled",
                "mode": first.mode.as_str(),
                "metrics_empty": metrics.is_empty(),
            })
        );
        print!("{metrics}");
        return if metrics.is_empty() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    }

    let first_ok = first.outcome == projection::ProjectionOutcome::Materialized
        && first.parity.as_ref().map(|p| p.holds()).unwrap_or(false)
        && first.applied == Some(3);

    // Second apply of the same drive → idempotent replay: nothing new applied,
    // parity still holds.
    let second = projection::shadow_project(
        env,
        &events,
        &authoritative,
        Some(offset),
        &pr_opts(flags),
        true,
    );
    let second_ok = second.outcome == projection::ProjectionOutcome::Materialized
        && second.applied == Some(0)
        && second.parity.as_ref().map(|p| p.holds()).unwrap_or(false);

    let ok = first_ok && second_ok;
    let metrics = render_metrics();
    println!(
        "{}",
        serde_json::json!({
            "suite": "ehdb-projection-selfcheck",
            "ehdb": "enabled",
            "mode": first.mode.as_str(),
            "role": first.role.map(|x| x.as_str()),
            "ok": ok,
            "steps": [
                {"step": "apply_1", "outcome": first.outcome.as_str(),
                 "applied": first.applied, "checkpoint": first.checkpoint,
                 "parity_holds": first.parity.as_ref().map(|p| p.holds())},
                {"step": "apply_2_replay", "outcome": second.outcome.as_str(),
                 "applied": second.applied, "checkpoint": second.checkpoint,
                 "parity_holds": second.parity.as_ref().map(|p| p.holds())},
            ],
            "metrics_secret_free": metrics_is_secret_free(&metrics),
        })
    );
    print!("{metrics}");

    if ok && metrics_is_secret_free(&metrics) {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn projection_exit(outcome: projection::ProjectionOutcome) -> ExitCode {
    use projection::ProjectionOutcome as O;
    match outcome {
        O::GuardRefused | O::Invalid | O::PrimaryUnavailable => ExitCode::from(4),
        O::Rejected => ExitCode::from(3),
        O::Unavailable | O::ParityMismatch | O::PrimaryDivergence => ExitCode::from(5),
        _ => ExitCode::SUCCESS,
    }
}

/// Phase 9 tier-2 primary cutover: serve the projection read-models
/// authoritatively from EHDB and prove it in one process.  Off / disabled ⇒
/// byte-identical no-op (served=false + empty metrics).  Enabled +
/// `NOETL_EHDB_PROJECTION=primary` ⇒ the full authoritative cycle (apply → list →
/// per-execution read → event lookup → checkpoint → idempotent re-apply → replay)
/// is served-by-EHDB with dual-run parity, AND flipping back to `shadow` restores
/// the incumbent materializer read path over the same store with zero data loss
/// (reversibility).  Exit 0 only when served-by-EHDB AND reversible AND metrics
/// stay secret-free.
fn run_projection_primary_serve(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let r = projection::serve_primary_cycle(env, &pr_opts(flags), true);

    // Off / disabled probe: byte-identical no-op, same shape as `projection-suite`.
    if r.outcome == projection::ProjectionOutcome::Disabled {
        let metrics = render_metrics();
        println!(
            "{}",
            serde_json::json!({
                "suite": "ehdb-projection-primary-serve",
                "ehdb": "disabled",
                "mode": r.mode.as_str(),
                "metrics_empty": metrics.is_empty(),
            })
        );
        print!("{metrics}");
        return if metrics.is_empty() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    }

    let metrics = render_metrics();
    let secret_free = metrics_is_secret_free(&metrics);
    println!(
        "{}",
        serde_json::json!({
            "suite": "ehdb-projection-primary-serve",
            "ehdb": "enabled",
            "op": "projection-primary-serve",
            "mode": r.mode.as_str(),
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "served_by_ehdb": r.served_by_ehdb,
            "reversible": r.reversible,
            "rows_after_revert": r.rows_after_revert,
            "duration_seconds": round6(r.duration_seconds),
            "detail": r.detail,
            "report": r.report,
            "metrics_secret_free": secret_free,
        })
    );
    print!("{metrics}");

    if r.outcome == projection::ProjectionOutcome::ServedPrimary
        && r.served_by_ehdb
        && r.reversible
        && secret_free
    {
        ExitCode::SUCCESS
    } else {
        projection_exit(r.outcome)
    }
}

// --- KV / platform-state shadow (EHDB Phase 8) -----------------------------

fn kv_opts(flags: &Flags) -> kv::KvOptions {
    kv::KvOptions {
        tenant: flags.get("tenant"),
        namespace: flags.get("namespace"),
        transaction_id: flags.get("transaction-id"),
    }
}

fn run_mirror_kv(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let (Some(bucket), Some(key), Some(value)) =
        (flags.req("bucket"), flags.req("key"), flags.req("value"))
    else {
        return usage_exit();
    };
    let expires_at_ms = flags.parse_u64("expires-at-ms");
    let r = kv::mirror_put(
        env,
        &bucket,
        &key,
        &value,
        expires_at_ms,
        &kv_opts(flags),
        true,
    );
    println!(
        "{}",
        serde_json::json!({
            "op": "mirror-kv",
            "mode": r.mode.as_str(),
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "detail": r.detail,
            "version": r.version,
            "parity": r.parity,
        })
    );
    print_metrics_footer();
    kv_exit(r.outcome)
}

/// Deterministic one-process KV shadow drive: put / get-parity / CAS-conflict /
/// CAS-swap / delete / TTL-expiry / scan against a fresh log.  Off / disabled ⇒
/// byte-identical no-op proof (disabled + empty metrics), same shape as
/// `eventlog-suite` / `projection-suite`.  Enabled ⇒ every engine capability
/// holds.
fn run_kv_suite(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let report = kv::shadow_suite(env, &kv_opts(flags), true);
    if report.disabled {
        let metrics = render_metrics();
        println!(
            "{}",
            serde_json::json!({
                "suite": "ehdb-kv-selfcheck",
                "ehdb": "disabled",
                "mode": report.mode.as_str(),
                "metrics_empty": metrics.is_empty(),
            })
        );
        print!("{metrics}");
        return if metrics.is_empty() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    }

    let metrics = render_metrics();
    println!(
        "{}",
        serde_json::json!({
            "suite": "ehdb-kv-selfcheck",
            "ehdb": "enabled",
            "mode": report.mode.as_str(),
            "role": report.role.map(|x| x.as_str()),
            "ok": report.ok,
            "guard_refused": report.guard_refused,
            "primary_unavailable": report.primary_unavailable,
            "steps": report.steps.iter().map(|s| serde_json::json!({
                "step": s.step, "outcome": s.outcome, "detail": s.detail,
            })).collect::<Vec<_>>(),
            "metrics_secret_free": metrics_is_secret_free(&metrics),
        })
    );
    print!("{metrics}");

    if report.guard_refused || report.primary_unavailable {
        return ExitCode::from(4);
    }
    if report.ok && metrics_is_secret_free(&metrics) {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn kv_exit(outcome: kv::KvOutcome) -> ExitCode {
    use kv::KvOutcome as O;
    match outcome {
        O::GuardRefused | O::Invalid | O::PrimaryUnavailable => ExitCode::from(4),
        O::Rejected => ExitCode::from(3),
        O::Unavailable | O::ParityMismatch | O::PrimaryDivergence => ExitCode::from(5),
        _ => ExitCode::SUCCESS,
    }
}

/// Phase 9 tier-3 primary cutover: serve the platform KV tier authoritatively
/// from EHDB and prove it in one process.  Off / disabled ⇒ byte-identical no-op
/// (served=false + empty metrics).  Enabled + `NOETL_EHDB_KV=primary` ⇒ the full
/// authoritative cycle (put → get → scan → CAS → delete → TTL → replay) is
/// served-by-EHDB with dual-run parity, AND flipping back to `shadow` restores the
/// incumbent NATS-KV path over the same store with zero data loss (reversibility).
/// Exit 0 only when served-by-EHDB AND reversible AND metrics stay secret-free.
fn run_kv_primary_serve(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let r = kv::serve_primary_cycle(env, &kv_opts(flags), true);

    // Off / disabled probe: byte-identical no-op, same shape as `kv-suite`.
    if r.outcome == kv::KvOutcome::Disabled {
        let metrics = render_metrics();
        println!(
            "{}",
            serde_json::json!({
                "suite": "ehdb-kv-primary-serve",
                "ehdb": "disabled",
                "mode": r.mode.as_str(),
                "metrics_empty": metrics.is_empty(),
            })
        );
        print!("{metrics}");
        return if metrics.is_empty() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    }

    let metrics = render_metrics();
    let secret_free = metrics_is_secret_free(&metrics);
    println!(
        "{}",
        serde_json::json!({
            "suite": "ehdb-kv-primary-serve",
            "ehdb": "enabled",
            "op": "kv-primary-serve",
            "mode": r.mode.as_str(),
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "served_by_ehdb": r.served_by_ehdb,
            "reversible": r.reversible,
            "keys_after_revert": r.keys_after_revert,
            "duration_seconds": round6(r.duration_seconds),
            "detail": r.detail,
            "report": r.report,
            "metrics_secret_free": secret_free,
        })
    );
    print!("{metrics}");

    if r.outcome == kv::KvOutcome::ServedPrimary && r.served_by_ehdb && r.reversible && secret_free
    {
        ExitCode::SUCCESS
    } else {
        kv_exit(r.outcome)
    }
}

// --- object / blob shadow (EHDB Phase 8) -----------------------------------

fn object_opts(flags: &Flags) -> object::ObjectOptions {
    object::ObjectOptions {
        tenant: flags.get("tenant"),
        namespace: flags.get("namespace"),
        transaction_id: flags.get("transaction-id"),
    }
}

fn run_mirror_object(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let (Some(key), Some(value)) = (flags.req("key"), flags.req("value")) else {
        return usage_exit();
    };
    let r = object::mirror_put(env, &key, value.as_bytes(), &object_opts(flags), true);
    println!(
        "{}",
        serde_json::json!({
            "op": "mirror-object",
            "mode": r.mode.as_str(),
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "detail": r.detail,
            "version": r.version,
            "digest": r.digest,
            "content_deduplicated": r.content_deduplicated,
            "parity": r.parity,
        })
    );
    print_metrics_footer();
    object_exit(r.outcome)
}

/// Deterministic one-process object shadow drive: put / get-parity / content-dedup
/// / list / locate / delete against a fresh registry log + blob store.  Off /
/// disabled ⇒ byte-identical no-op proof (disabled + empty metrics), same shape as
/// `kv-suite`.  Enabled ⇒ every engine capability holds.
fn run_object_suite(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let report = object::shadow_suite(env, &object_opts(flags), true);
    if report.disabled {
        let metrics = render_metrics();
        println!(
            "{}",
            serde_json::json!({
                "suite": "ehdb-object-selfcheck",
                "ehdb": "disabled",
                "mode": report.mode.as_str(),
                "metrics_empty": metrics.is_empty(),
            })
        );
        print!("{metrics}");
        return if metrics.is_empty() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    }

    let metrics = render_metrics();
    println!(
        "{}",
        serde_json::json!({
            "suite": "ehdb-object-selfcheck",
            "ehdb": "enabled",
            "mode": report.mode.as_str(),
            "role": report.role.map(|x| x.as_str()),
            "ok": report.ok,
            "guard_refused": report.guard_refused,
            "primary_unavailable": report.primary_unavailable,
            "steps": report.steps.iter().map(|s| serde_json::json!({
                "step": s.step, "outcome": s.outcome, "detail": s.detail,
            })).collect::<Vec<_>>(),
            "metrics_secret_free": metrics_is_secret_free(&metrics),
        })
    );
    print!("{metrics}");

    if report.guard_refused || report.primary_unavailable {
        return ExitCode::from(4);
    }
    if report.ok && metrics_is_secret_free(&metrics) {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn object_exit(outcome: object::ObjectOutcome) -> ExitCode {
    use object::ObjectOutcome as O;
    match outcome {
        O::GuardRefused | O::Invalid | O::PrimaryUnavailable => ExitCode::from(4),
        O::Rejected => ExitCode::from(3),
        O::Unavailable | O::ParityMismatch => ExitCode::from(5),
        _ => ExitCode::SUCCESS,
    }
}

// --- vector shadow (EHDB Phase 8, slice 3) ---------------------------------

fn vector_opts(flags: &Flags) -> vector::VectorOptions {
    vector::VectorOptions {
        tenant: flags.get("tenant"),
        namespace: flags.get("namespace"),
        transaction_id: flags.get("transaction-id"),
    }
}

/// Parse a `--vector` flag of comma-separated floats (e.g. `1.0,0.0,0.5`).
fn parse_vector(flags: &Flags) -> Option<Vec<f32>> {
    let raw = flags.get("vector")?;
    raw.split(',')
        .map(|t| t.trim().parse::<f32>().ok())
        .collect()
}

fn run_mirror_vector(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let (Some(collection), Some(point), Some(model), Some(vec)) = (
        flags.req("collection"),
        flags.req("point-id"),
        flags.req("model-id"),
        parse_vector(flags),
    ) else {
        return usage_exit();
    };
    let r = vector::mirror_upsert(
        env,
        &collection,
        &point,
        &model,
        &vec,
        flags.get("payload").as_deref(),
        &vector_opts(flags),
        true,
    );
    println!(
        "{}",
        serde_json::json!({
            "op": "mirror-vector",
            "mode": r.mode.as_str(),
            "outcome": r.outcome.as_str(),
            "role": r.role.map(|x| x.as_str()),
            "detail": r.detail,
            "version": r.version,
            "candidate_count": r.candidate_count,
            "returned": r.returned,
            "parity": r.parity,
        })
    );
    print_metrics_footer();
    vector_exit(r.outcome)
}

/// Deterministic one-process vector shadow drive: upsert / query-parity /
/// top-k-truncate / delete against a fresh index log.  Off / disabled ⇒
/// byte-identical no-op proof (disabled + empty metrics), same shape as
/// `object-suite`.  Enabled ⇒ every engine capability holds.
fn run_vector_suite(env: &ehdb::EnvMap, flags: &Flags) -> ExitCode {
    let report = vector::shadow_suite(env, &vector_opts(flags), true);
    if report.disabled {
        let metrics = render_metrics();
        println!(
            "{}",
            serde_json::json!({
                "suite": "ehdb-vector-selfcheck",
                "ehdb": "disabled",
                "mode": report.mode.as_str(),
                "metrics_empty": metrics.is_empty(),
            })
        );
        print!("{metrics}");
        return if metrics.is_empty() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    }

    let metrics = render_metrics();
    println!(
        "{}",
        serde_json::json!({
            "suite": "ehdb-vector-selfcheck",
            "ehdb": "enabled",
            "mode": report.mode.as_str(),
            "role": report.role.map(|x| x.as_str()),
            "ok": report.ok,
            "guard_refused": report.guard_refused,
            "primary_unavailable": report.primary_unavailable,
            "steps": report.steps.iter().map(|s| serde_json::json!({
                "step": s.step, "outcome": s.outcome, "detail": s.detail,
            })).collect::<Vec<_>>(),
            "metrics_secret_free": metrics_is_secret_free(&metrics),
        })
    );
    print!("{metrics}");

    if report.guard_refused || report.primary_unavailable {
        return ExitCode::from(4);
    }
    if report.ok && metrics_is_secret_free(&metrics) {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn vector_exit(outcome: vector::VectorOutcome) -> ExitCode {
    use vector::VectorOutcome as O;
    match outcome {
        O::GuardRefused | O::Invalid | O::PrimaryUnavailable => ExitCode::from(4),
        O::Rejected => ExitCode::from(3),
        O::Unavailable | O::ParityMismatch => ExitCode::from(5),
        _ => ExitCode::SUCCESS,
    }
}

// --- helpers ---------------------------------------------------------------

fn dp_opts(flags: &Flags) -> dataplane::DataPlaneOptions {
    dataplane::DataPlaneOptions {
        tenant: flags.get("tenant"),
        namespace: flags.get("namespace"),
        transaction_id: flags.get("transaction-id"),
    }
}

fn es_opts(flags: &Flags) -> eventstream::EventStreamOptions {
    eventstream::EventStreamOptions {
        tenant: flags.get("tenant"),
        namespace: flags.get("namespace"),
        transaction_id: flags.get("transaction-id"),
    }
}

fn render_metrics() -> String {
    let lines = ehdb::metrics::render_lines();
    if lines.is_empty() {
        String::new()
    } else {
        format!("{}\n", lines.join("\n"))
    }
}

fn print_metrics_footer() {
    let m = render_metrics();
    if !m.is_empty() {
        eprintln!("--- ehdb /metrics ---\n{m}");
    }
}

/// A secret-free render carries only known metric names + the `operation` /
/// `outcome` label keys — never a log path, payload, stream, or subject.
fn metrics_is_secret_free(metrics: &str) -> bool {
    let forbidden = [
        "log_path",
        ".jsonl",
        "payload",
        "/opt/noetl",
        "subject=",
        "stream=",
    ];
    !forbidden.iter().any(|f| metrics.contains(f))
}

fn round6(v: f64) -> f64 {
    (v * 1_000_000.0).round() / 1_000_000.0
}

fn readiness_exit(outcome: readiness::ReadinessOutcome) -> ExitCode {
    use readiness::ReadinessOutcome as O;
    match outcome {
        O::GuardRefused | O::Invalid => ExitCode::from(4),
        O::Truncated | O::Unavailable => ExitCode::from(5),
        _ => ExitCode::SUCCESS,
    }
}

fn dataplane_exit(outcome: dataplane::DataPlaneOutcome) -> ExitCode {
    use dataplane::DataPlaneOutcome as O;
    match outcome {
        O::GuardRefused | O::Invalid => ExitCode::from(4),
        O::Rejected => ExitCode::from(3),
        O::Unavailable => ExitCode::from(5),
        _ => ExitCode::SUCCESS,
    }
}

fn eventstream_exit(outcome: eventstream::EventStreamOutcome) -> ExitCode {
    use eventstream::EventStreamOutcome as O;
    match outcome {
        O::GuardRefused | O::Invalid => ExitCode::from(4),
        O::Rejected => ExitCode::from(3),
        O::Unavailable => ExitCode::from(5),
        _ => ExitCode::SUCCESS,
    }
}

fn systemstore_exit(outcome: systemstore::SystemStoreOutcome) -> ExitCode {
    use systemstore::SystemStoreOutcome as O;
    match outcome {
        O::GuardRefused | O::Invalid => ExitCode::from(4),
        O::Rejected => ExitCode::from(3),
        O::Unavailable => ExitCode::from(5),
        _ => ExitCode::SUCCESS,
    }
}

fn rag_exit(outcome: rag::RagOutcome) -> ExitCode {
    use rag::RagOutcome as O;
    match outcome {
        O::GuardRefused | O::Invalid => ExitCode::from(4),
        O::Rejected => ExitCode::from(3),
        O::Unavailable => ExitCode::from(5),
        _ => ExitCode::SUCCESS,
    }
}

fn usage_exit() -> ExitCode {
    eprintln!("{}", usage());
    ExitCode::from(2)
}

fn usage() -> &'static str {
    "usage: ehdb-selfcheck <readiness|append|read|project|consume|ack|suite|\
     publish-system|bind-system|resolve-system|system-suite|metrics> [--flag value ...]\n  \
     append          --stream <s> --subject <sub> --payload <text>\n  \
     read            --stream <s> [--limit <n>] [--after <seq>]\n  \
     project         --stream <s> --subject <sub> --payload <text>\n  \
     consume         --stream <s> --consumer <c> [--limit <n>]\n  \
     ack             --stream <s> --consumer <c> --sequence <seq>\n  \
     suite           [--stream <s>] [--consumer <c>]\n  \
     publish-system  --path <lib> --revision <n> --digest <sha256:..> --entry <e> \
     --target <wasm32-unknown-unknown|wasm32-wasi-preview1> --object-path <p> \
     --byte-len <n> --capabilities <c1,c2,..>\n  \
     bind-system     --environment <env> --channel <chan> --path <lib> --revision <n> --digest <sha256:..>\n  \
     resolve-system  --environment <env> --channel <chan> --path <lib>\n  \
     system-suite    [--path <lib>] [--environment <env>] [--channel <chan>]\n  \
     ingest-rag      --document-id <id> --chunks <text1||text2||...> [--source-uri <uri>] [--content-type <ct>]\n  \
     retrieve-rag    --query <text> [--top-k <n>] [--max-chunk-bytes <n>] [--time-budget-ms <n>]\n  \
     rag-suite       [--document-id <id>]\n  \
     mirror-eventlog --execution-id <id> --payload <text> [--authoritative-sequence <n>]\n  \
     eventlog-suite  [--execution-id <id>]\n  \
     eventlog-primary-serve  (Phase 9 tier 1: serve log from EHDB + reversibility)\n  \
     mirror-projection --execution-id <id> [--events <n>] [--consumer <c>]\n  \
     projection-suite  [--execution-id <id>] [--consumer <c>]\n  \
     projection-primary-serve  (Phase 9 tier 2: serve read-models from EHDB + reversibility)\n  \
     mirror-kv       --bucket <b> --key <k> --value <text> [--expires-at-ms <n>]\n  \
     kv-suite\n  \
     kv-primary-serve  (Phase 9 tier 3: serve platform KV from EHDB + reversibility)\n  \
     mirror-object   --key <k> --value <text>\n  \
     object-suite\n  \
     mirror-vector   --collection <c> --point-id <id> --model-id <m> --vector <f1,f2,..> [--payload <text>]\n  \
     vector-suite\n  \
     common:  [--tenant <t>] [--namespace <n>] [--transaction-id <id>]"
}

struct Flags(HashMap<String, String>);

impl Flags {
    fn get(&self, key: &str) -> Option<String> {
        self.0.get(key).cloned()
    }
    fn req(&self, key: &str) -> Option<String> {
        self.0.get(key).cloned()
    }
    fn parse_usize(&self, key: &str) -> Option<usize> {
        self.0.get(key).and_then(|v| v.parse().ok())
    }
    fn parse_u64(&self, key: &str) -> Option<u64> {
        self.0.get(key).and_then(|v| v.parse().ok())
    }
}

fn parse_flags(args: &[String]) -> Result<Flags, String> {
    let mut map = HashMap::new();
    let mut iter = args.iter().skip(1); // skip subcommand
    while let Some(token) = iter.next() {
        let key = token
            .strip_prefix("--")
            .ok_or_else(|| format!("unexpected argument: {token}"))?;
        if key.is_empty() {
            return Err("empty flag name".to_string());
        }
        let value = iter
            .next()
            .ok_or_else(|| format!("flag --{key} is missing a value"))?;
        map.insert(key.to_string(), value.clone());
    }
    Ok(Flags(map))
}
