//! Concurrency gate for the EHDB tier store's append path — the P0 the
//! serve-ready prod soak found (ai-meta#257).
//!
//! # What broke, and why nothing caught it before
//!
//! With `NOETL_EHDB_EVENTLOG_MIRROR_SOURCE=server` +
//! `NOETL_EHDB_TIER_QUERY_SOURCE=service`, the **server** authors every
//! execution's mirror appends into **one** writer-fronted store through **one**
//! relay. Before that config, each worker pod owned its own store and drove it
//! from a single caller, so two appends never overlapped — the store's
//! single-writer assumption held by accident, and every prior gate ran under it.
//!
//! The store is a JSONL transaction log whose writer calls
//! `serde_json::to_writer` straight at the `File`, unbuffered. One record is
//! hundreds of small `write(2)` calls; `O_APPEND` makes each atomic
//! *individually*, so two appenders interleave mid-record and the second lands
//! **inside** the first's `payload` byte array. Read-back hits `{` where a `u8`
//! belongs:
//!
//! ```text
//! invalid transaction log record at line N: invalid type: map, expected u8
//! ```
//!
//! and because the replay runs on *every* operation and fails on the first bad
//! line, one torn write bricks the whole store — the soak saw append ok 46 /
//! error 302 and `ehdb_unavailable` on every read.
//!
//! # Why this file is an integration test, not a unit test
//!
//! It drives the **real listener** over the **real length-framed protocol**
//! with the **real client** — no assertion here reads source or reaches past
//! the wire. It lives in its own test binary, and is **one test function with
//! three phases** rather than three tests, because the service resolves
//! `NOETL_EHDB_TIER_SERVICE_DIR` per request and `cargo test` does not
//! serialise tests within a binary: three `#[tokio::test]`s would each point
//! the shared service at their own directory and trample each other. (Observed,
//! not theorised — the first draft did exactly that and the fixture phase
//! failed against an empty store while a sibling held the env.)
//!
//! # The mutation check
//!
//! Delete the `store_lock` write guard in `src/ehdb/tier_store.rs::append` and
//! phase 1 must fail. It is a real race, so it is driven hard enough to lose
//! reliably rather than occasionally: see `CLIENTS` / `APPENDS_PER_CLIENT`
//! below.  Measured on the pre-fix path: **184 of 192 appends refused**, the
//! same shape as the soak's ok-46 / error-302.

use std::collections::BTreeSet;

use noetl_worker::ehdb::tier_client::{TierClient, TierClientConfig};
use noetl_worker::ehdb::tier_service::serve_tier;
use noetl_worker::ehdb::tier_store::TIER_SERVICE_DIR_ENV;

/// Concurrent connections hammering the one store.
const CLIENTS: usize = 16;
/// Appends each client issues, back to back, on its own connection.
const APPENDS_PER_CLIENT: usize = 12;
/// Total records the store must hold afterwards.
const TOTAL: usize = CLIENTS * APPENDS_PER_CLIENT;

/// A payload big enough that serialising it is many `write(2)` calls.
///
/// This is what makes the race *reliable* rather than lucky. The torn window is
/// proportional to how many syscalls one record takes, and the record's
/// `payload` is serialised as a JSON array of individual `u8` numbers — so a
/// long payload is a long splice target. A 20-byte payload can pass the unfixed
/// code by luck; this one does not.
fn payload_for(client: usize, n: usize) -> String {
    let filler = "0123456789abcdef".repeat(64);
    format!(r#"{{"client":{client},"n":{n},"filler":"{filler}"}}"#)
}

/// Bring up a real tier service over a real socket, on an ephemeral port.
async fn start_service(dir: &std::path::Path) -> TierClient {
    // SAFETY: this test binary is this process, and no other test in it reads
    // or writes this variable.
    unsafe { std::env::set_var(TIER_SERVICE_DIR_ENV, dir) };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(serve_tier(listener));

    TierClient::new(TierClientConfig {
        addr: addr.to_string(),
        timeout: std::time::Duration::from_secs(30),
        append_timeout: std::time::Duration::from_secs(30),
    })
}

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let mut d = std::env::temp_dir();
    d.push(format!("ehdb-tier-concurrency-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// **The gate.** Hammer one store with concurrent appends through the service,
/// then read the whole log back and assert it is well formed.
///
/// Four assertions, and the reason each is here:
///
/// 1. **every append succeeded** — the unfixed code fails most of them once the
///    first torn line lands, because the replay that precedes every append
///    trips over it. This alone reproduces the soak's ok-46/error-302 shape.
/// 2. **the log reads back at all** — a `scan` on a corrupt store returns
///    `error ... invalid transaction log record at line N`, which is the
///    `ehdb_unavailable` the soak saw on every read.
/// 3. **every record deserialises and the count matches** — this is the
///    *positive control*. Without it "zero torn records" is satisfiable by an
///    empty log, and a store that silently dropped everything would pass.
/// 4. **sequences are unique and contiguous** — the second half of the defect.
///    The sequence is a read-modify-write (`replay` → `count + 1` → write), so
///    two appenders that replay the same state both claim the same number even
///    when neither write tears.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn tier_store_append_is_safe_under_concurrent_writers() {
    phase_concurrent_appends_produce_a_readable_log().await;
    phase_reads_never_observe_a_half_written_record().await;
    phase_a_corrupt_store_is_refused_not_misread().await;
    phase_the_preserved_prod_artifact_is_refused().await;
}

async fn phase_concurrent_appends_produce_a_readable_log() {
    let dir = tmp_dir("gate");
    let client = start_service(&dir).await;

    // Every client races on its own connection. `join_all` on spawned tasks,
    // not sequential awaits — a sequential driver would serialise the very
    // thing under test and pass against the broken code.
    let mut tasks = Vec::with_capacity(CLIENTS);
    for c in 0..CLIENTS {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            let mut failures = Vec::new();
            for n in 0..APPENDS_PER_CLIENT {
                let exec = format!("exec-{c}-{n}");
                match client.append(&exec, &payload_for(c, n)).await {
                    Ok(body) if body.contains("\"appended\":true") => {}
                    Ok(body) => failures.push(format!("{exec}: refused: {body}")),
                    Err(e) => failures.push(format!("{exec}: transport: {e}")),
                }
            }
            failures
        }));
    }

    let mut failures = Vec::new();
    for t in tasks {
        failures.extend(t.await.expect("append task panicked"));
    }

    // (1) Every append succeeded.
    assert!(
        failures.is_empty(),
        "{} of {TOTAL} concurrent appends failed — the store tore under \
         concurrency.  First few:\n{}",
        failures.len(),
        failures.iter().take(5).cloned().collect::<Vec<_>>().join("\n")
    );

    // (2) The whole log reads back.  A corrupt store answers `error ...` here.
    let body = client
        .scan(None, 5_000)
        .await
        .unwrap_or_else(|e| panic!("scan transport failed: {e}"));
    assert!(
        !body.starts_with("error") && !body.starts_with("unavailable"),
        "the log did not read back — this is the read-side of the corruption:\n{body}"
    );

    // (3) Parsed with a parser, not substring-matched.  Structured, fail-loud.
    let v: serde_json::Value = serde_json::from_str(&body)
        .unwrap_or_else(|e| panic!("scan body is not JSON ({e}):\n{body}"));
    let records = v["records"]
        .as_array()
        .unwrap_or_else(|| panic!("scan body carries no records array:\n{body}"));

    // Positive control: the log is NON-EMPTY and holds exactly what we wrote.
    // Without this, every assertion below is vacuously true on an empty log.
    assert_eq!(
        records.len(),
        TOTAL,
        "expected {TOTAL} records, got {} — records were lost, not just torn",
        records.len()
    );
    assert_eq!(
        v["record_count"].as_u64(),
        Some(TOTAL as u64),
        "the count the store reports must match what it returns"
    );

    // (4) Sequences: unique and contiguous 1..=TOTAL.
    let seqs: Vec<u64> = records
        .iter()
        .map(|r| {
            r["global_sequence"]
                .as_u64()
                .unwrap_or_else(|| panic!("record has no global_sequence: {r}"))
        })
        .collect();
    let unique: BTreeSet<u64> = seqs.iter().copied().collect();
    assert_eq!(
        unique.len(),
        TOTAL,
        "global_sequence collided — {} distinct values across {TOTAL} records. \
         The sequence is a read-modify-write; concurrent appenders claimed the same one.",
        unique.len()
    );
    assert_eq!(
        (*unique.first().unwrap(), *unique.last().unwrap()),
        (1, TOTAL as u64),
        "sequences must be contiguous 1..={TOTAL}, got {:?}..={:?}",
        unique.first(),
        unique.last()
    );

    // Every payload we wrote is present exactly once — the records are not just
    // well-formed, they are OUR records.
    let mut seen = BTreeSet::new();
    for r in records {
        let p = r["payload"].as_str().unwrap_or("");
        let parsed: serde_json::Value = serde_json::from_str(p)
            .unwrap_or_else(|e| panic!("record payload is not the JSON we appended ({e}): {p}"));
        seen.insert((
            parsed["client"].as_u64().unwrap_or(u64::MAX),
            parsed["n"].as_u64().unwrap_or(u64::MAX),
        ));
    }
    assert_eq!(seen.len(), TOTAL, "duplicate or mangled payloads round-tripped");

    let _ = std::fs::remove_dir_all(&dir);
}

/// The **real** corrupt store from the prod soak, when it is available.
///
/// `EHDB_TORN_FIXTURE` points at a directory holding the preserved
/// `eventlog.jsonl` (ai-meta#261 keeps one; it is deliberately not committed to
/// a public repo). Absent ⇒ this phase reports that it did not run.
///
/// It is **opt-in rather than skip-if-missing-silently** on purpose: a test that
/// quietly passes when its fixture is absent is worse than no test, because the
/// green tick then means "the file was not there".  The synthetic splice in
/// [`phase_a_corrupt_store_is_refused_not_misread`] is the portable assertion;
/// this one confirms the synthetic shape matches the real bytes.
async fn phase_the_preserved_prod_artifact_is_refused() {
    let Ok(src) = std::env::var("EHDB_TORN_FIXTURE") else {
        eprintln!("phase 4 SKIPPED: set EHDB_TORN_FIXTURE=<dir with eventlog.jsonl> to run it");
        return;
    };
    let src = std::path::Path::new(&src).join("eventlog.jsonl");
    let corrupt = std::fs::read(&src)
        .unwrap_or_else(|e| panic!("EHDB_TORN_FIXTURE is set but unreadable ({e}): {}", src.display()));

    // COPY it. The service writes into whatever directory it is pointed at, and
    // a preserved incident artifact must not be mutated by the thing examining it.
    let dir = tmp_dir("artifact");
    std::fs::create_dir_all(&dir).expect("create store dir");
    std::fs::write(dir.join("eventlog.jsonl"), &corrupt).expect("seed the corrupt log");

    let client = start_service(&dir).await;
    let body = client.scan(None, 1_000).await.expect("scan transport");

    assert!(
        body.starts_with("error"),
        "the preserved corrupt store must be REFUSED, not served:\n{body}"
    );
    assert!(
        body.contains("invalid type: map, expected u8"),
        "the real artifact must reproduce the fingerprint this fix was written \
         against:\n{body}"
    );
    eprintln!("phase 4: preserved artifact refused fail-loud -> {body}");

    let _ = std::fs::remove_dir_all(&dir);
}

/// A read racing an append must not be reported as a broken store.
///
/// Without the shared read side of the lock, a `read_execution` that replays
/// while an append is mid-write sees a half-written final line and answers
/// `error ... invalid transaction log record` — the same message a genuinely
/// corrupt store gives, from a store that is fine. An operator cannot tell
/// those apart, so the read side is part of the fix, not a nicety.
async fn phase_reads_never_observe_a_half_written_record() {
    let dir = tmp_dir("readrace");
    let client = start_service(&dir).await;

    let writer = {
        let client = client.clone();
        tokio::spawn(async move {
            for n in 0..TOTAL {
                let _ = client.append(&format!("exec-{n}"), &payload_for(0, n)).await;
            }
        })
    };

    let reader = {
        let client = client.clone();
        tokio::spawn(async move {
            let mut bad = Vec::new();
            for _ in 0..60 {
                match client.scan(None, 5_000).await {
                    Ok(b) if b.starts_with("error") => bad.push(b),
                    Ok(_) => {}
                    Err(e) => bad.push(format!("transport: {e}")),
                }
                tokio::task::yield_now().await;
            }
            bad
        })
    };

    writer.await.expect("writer panicked");
    let bad = reader.await.expect("reader panicked");
    assert!(
        bad.is_empty(),
        "{} reads observed a store that looked corrupt while it was only being \
         written.  First:\n{}",
        bad.len(),
        bad[0]
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A genuinely corrupt store must **fail loud**, not mis-deserialise.
///
/// This is the fixture half of the gate: it reconstructs the exact splice the
/// soak produced — record B's opening `{` landing inside record A's `payload`
/// byte array — and asserts the reader refuses it with the recognisable
/// message, rather than returning a short or wrong record set that a caller
/// would treat as data.
///
/// It also fixes the shape of the corruption in a test, so the fingerprint
/// stays greppable after the live repro stops reproducing.
async fn phase_a_corrupt_store_is_refused_not_misread() {
    let dir = tmp_dir("corrupt");
    let client = start_service(&dir).await;

    // Two clean records first, so the corruption is not the whole file — a
    // reader that simply returns nothing would otherwise look correct.
    for n in 0..2 {
        client
            .append(&format!("exec-{n}"), &payload_for(9, n))
            .await
            .expect("seed append");
    }

    // Splice a second record's opening brace into the first one's payload
    // array, byte for byte the way two interleaved `write(2)` calls do it.
    //
    // The splice point matters and is not arbitrary: it goes **at an element
    // boundary** — immediately after a comma inside the byte array — because
    // that is where an interleaved `write(2)` lands when the interrupted
    // serialiser had just finished emitting one `u8`.  Splicing mid-number
    // instead yields `expected ',' or ']'`, a *different* message about the
    // same file, and the point of this phase is to pin the exact fingerprint
    // prod reported.
    let path = dir.join("eventlog.jsonl");
    let text = std::fs::read_to_string(&path).expect("read seeded log");
    let first = text.lines().next().expect("at least one record").to_string();
    let marker = "\"payload\":[";
    let array_at = first
        .find(marker)
        .map(|i| i + marker.len())
        .expect("records carry a payload byte array");
    let at = array_at
        + first[array_at..]
            .find(',')
            .expect("the payload array has more than one element")
        + 1;
    let torn = format!("{}{}{}", &first[..at], r#"{"transaction_id":"#, &first[at..]);
    std::fs::write(&path, format!("{torn}\n{text}")).expect("write torn log");

    // The reader must refuse, with the message an operator can search for.
    let body = client.scan(None, 5_000).await.expect("scan transport");
    assert!(
        body.starts_with("error"),
        "a torn log must be refused, not served:\n{body}"
    );
    assert!(
        body.contains("invalid transaction log record"),
        "the refusal must name the corruption so it is greppable:\n{body}"
    );
    // The exact fingerprint the prod soak reported, pinned so it stays
    // greppable after the live repro stops reproducing.  This is also a
    // negative control on the assertion above: it must be the *corruption*
    // being reported, not merely any error string.
    assert!(
        body.contains("invalid type: map, expected u8"),
        "the refusal must carry the deserialiser's reason — the prod fingerprint \
         is `invalid type: map, expected u8`:\n{body}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
