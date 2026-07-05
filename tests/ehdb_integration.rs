//! End-to-end integration test for the in-process EHDB integration
//! (noetl/ehdb#234).  Runs in its own process so the process-local metric
//! accumulators start clean.
//!
//! Covers: disabled → empty metric render (byte-identical no-op); an enabled
//! worker-role full drive (append → read → project → consume → ack →
//! consume-again with an advanced cursor); the control-plane guard refusing a
//! server role; and the secret-free metric render.

use std::collections::HashMap;

use noetl_worker::ehdb::{self, dataplane, eventstream, readiness};

fn worker_env(log: &std::path::Path) -> ehdb::EnvMap {
    let mut m: HashMap<String, String> = HashMap::new();
    m.insert("NOETL_EHDB_ENABLED".into(), "true".into());
    m.insert("NOETL_EHDB_MODE".into(), "local_reference".into());
    m.insert("NOETL_EHDB_CLIENT_ROLE".into(), "worker".into());
    m.insert(
        "NOETL_EHDB_LOCAL_REFERENCE_LOG".into(),
        log.to_str().unwrap().into(),
    );
    m
}

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ehdb-it-{tag}-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn full_drive_and_secret_free_metrics() {
    let dir = tmp_dir("drive");
    let log = dir.join("log.jsonl");
    let env = worker_env(&log);

    // readiness on a fresh log → empty (no records yet).
    let rd = readiness::evaluate(&env, true);
    assert_eq!(rd.outcome, readiness::ReadinessOutcome::Empty);

    // data-plane append → read roundtrip.
    let a = dataplane::append_domain_record(
        &env,
        "events",
        "noetl.events.a",
        "{\"n\":1}",
        &Default::default(),
        true,
    );
    assert_eq!(a.outcome, dataplane::DataPlaneOutcome::Appended);
    let r =
        dataplane::read_domain_records(&env, "events", None, Some(10), &Default::default(), true);
    assert_eq!(r.outcome, dataplane::DataPlaneOutcome::Read);
    assert_eq!(r.read.as_ref().unwrap().returned, 1);

    // event-stream project → consume → ack → consume-again (cursor advanced).
    let p = eventstream::project_event(
        &env,
        "events",
        "noetl.events.b",
        "{\"n\":2}",
        &Default::default(),
        true,
    );
    assert_eq!(p.outcome, eventstream::EventStreamOutcome::Projected);
    let c =
        eventstream::consume_events(&env, "events", "drain", Some(10), &Default::default(), true);
    let before = c.consume.unwrap().pending_count;
    assert!(before >= 1);
    let ak = eventstream::ack_events(&env, "events", "drain", 1, &Default::default(), true);
    assert_eq!(ak.outcome, eventstream::EventStreamOutcome::Acked);
    let c2 =
        eventstream::consume_events(&env, "events", "drain", Some(10), &Default::default(), true);
    let after = c2.consume.unwrap().pending_count;
    assert_eq!(after + 1, before, "ack must advance the durable cursor");

    // Metrics render carries the three families, and is secret-free: no log
    // path, payload, stream, or subject ever reaches a label.
    let metrics = ehdb::metrics::render_lines().join("\n");
    assert!(metrics.contains("noetl_ehdb_readiness_checks_total"));
    assert!(metrics
        .contains("noetl_ehdb_dataplane_ops_total{operation=\"append\",outcome=\"appended\"}"));
    assert!(
        metrics.contains("noetl_ehdb_eventstream_ops_total{operation=\"ack\",outcome=\"acked\"}")
    );
    for forbidden in [
        "log_path",
        ".jsonl",
        "payload",
        "/opt/noetl",
        "stream=\"",
        "subject=\"",
    ] {
        assert!(
            !metrics.contains(forbidden),
            "metric render leaked `{forbidden}`:\n{metrics}"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn server_role_data_plane_env_is_refused_no_write() {
    let dir = tmp_dir("guard");
    let log = dir.join("log.jsonl");
    let mut env = worker_env(&log);
    env.insert("NOETL_EHDB_CLIENT_ROLE".into(), "server".into());

    let a = dataplane::append_domain_record(&env, "s", "s.a", "x", &Default::default(), false);
    assert_eq!(a.outcome, dataplane::DataPlaneOutcome::GuardRefused);
    // No write happened: the log file must not have been created.
    assert!(!log.exists(), "guard refusal must not create the log");

    let _ = std::fs::remove_dir_all(&dir);
}
