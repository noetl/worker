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

use noetl_worker::ehdb::{self, dataplane, eventstream, readiness};

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

fn usage_exit() -> ExitCode {
    eprintln!("{}", usage());
    ExitCode::from(2)
}

fn usage() -> &'static str {
    "usage: ehdb-selfcheck <readiness|append|read|project|consume|ack|suite|metrics> [--flag value ...]\n  \
     append   --stream <s> --subject <sub> --payload <text>\n  \
     read     --stream <s> [--limit <n>] [--after <seq>]\n  \
     project  --stream <s> --subject <sub> --payload <text>\n  \
     consume  --stream <s> --consumer <c> [--limit <n>]\n  \
     ack      --stream <s> --consumer <c> --sequence <seq>\n  \
     suite    [--stream <s>] [--consumer <c>]\n  \
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
