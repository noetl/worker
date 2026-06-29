//! noetl/ai-meta#156 — off-server drive per-hop latency: tail-attach vs
//! drain-only, measured under synthetic global event-log load.
//!
//! ## What this proves
//!
//! The off-server drive serves a hop only once the pool-side WAL index holds the
//! chain up to the server's `expected_head`.  Today that index is fed solely by
//! the drain — one ephemeral `DeliverAll` consumer that replays the **global**
//! `noetl_events` stream serially under one mutex — so the time to serve a hop
//! scales with how far back in the global backlog this execution's new tip sits,
//! NOT with this execution's own work.  Under load the drain lags past the
//! worker's ~1s drive-retry budget and the hop drops to the server's 8s reconcile
//! tick (`events.rs RECONCILE_INTERVAL`): the ~8–13s per-hop variance #156 pins.
//!
//! The #156 fix attaches the new tail (the events the server just published) to
//! the dispatch, and the worker applies them to its index BEFORE building.  A
//! warm-index hop then serves on the first build attempt regardless of drain lag.
//!
//! This harness exercises the **real** primitives — [`SharedWalIndex`],
//! [`build_offserver_input`], and the same append-signal wait loop
//! `executor::command::resolve_offserver_orchestrate_input` runs — under a
//! synthetic drain whose backlog `B` models global event-log volume.  It measures
//! the per-hop time-to-serve distribution (p50/p95/max/stdev) and the miss-rate
//! (each miss = an 8s server reconcile cliff) for both arms.
//!
//! Run explicitly (it is `#[ignore]` so it stays out of the default CI gate):
//!
//! ```text
//! cargo test --test offserver_tail_latency -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};

use noetl_worker::state_builder::{build_offserver_input, SharedWalIndex, WalEventIndex};
use serde_json::json;

const EXEC: i64 = 42;
/// The worker's real default drive-retry budget (5 × 200ms) — `command.rs`.
const RETRY_ATTEMPTS: u32 = 5;
const RETRY_MS: u64 = 200;
/// Events the hop appends to the chain (the new tail). Per the #156 attribution
/// the per-hop tail is usually 1–3 events; use 2.
const TAIL: i64 = 2;
/// Per-event drain cost — models the serial global-stream replay latency (pull
/// batch + apply under the shared mutex). The drain must clear `B` backlog events
/// before it reaches this hop's tip, so time-to-tip ≈ `B × DRAIN_PER_EVENT`.
const DRAIN_PER_EVENT: Duration = Duration::from_micros(80);

/// A `noetl_events`-shaped payload.
fn ev(execution_id: i64, event_id: i64, prev: Option<i64>, etype: &str) -> serde_json::Value {
    json!({
        "event_id": event_id,
        "execution_id": execution_id,
        "prev_event_id": prev,
        "event_type": etype,
    })
}

/// Build execution `EXEC`'s chain payloads `1..=n`: genesis `playbook_started`
/// (id 1) then a linear `prev_event_id` chain. `expected_head` for the hop = `n`.
fn chain_payloads(n: i64) -> Vec<serde_json::Value> {
    (1..=n)
        .map(|id| {
            let etype = if id == 1 {
                "playbook_started"
            } else if id % 2 == 0 {
                "command.issued"
            } else {
                "command.completed"
            };
            ev(EXEC, id, if id == 1 { None } else { Some(id - 1) }, etype)
        })
        .collect()
}

/// The same enable-before-check append-signal wait loop the worker runs in
/// `resolve_offserver_orchestrate_input`. Returns the time to a served build, or
/// `None` if the budget elapsed first (→ the server's 8s reconcile cliff).
async fn time_to_serve(index: &SharedWalIndex, expected_head: i64, playbook: &serde_json::Value) -> Option<Duration> {
    let budget = Duration::from_millis(RETRY_MS * RETRY_ATTEMPTS as u64);
    let per_wait = Duration::from_millis(RETRY_MS);
    let start = Instant::now();
    let deadline = start + budget;
    let appended = index.appended();
    loop {
        let notified = appended.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        if build_offserver_input(
            index,
            EXEC,
            playbook,
            Some("command.completed"),
            Some(expected_head),
            Some(expected_head),
            false,
        )
        .await
        .is_some()
        {
            return Some(start.elapsed());
        }
        let now = Instant::now();
        if now >= deadline {
            return None;
        }
        let wait = per_wait.min(deadline - now);
        let _ = tokio::time::timeout(wait, notified).await;
    }
}

/// The serial global-stream drain: it must clear the whole `B`-event backlog
/// before it reaches this hop's tip. Modeled as one accurate aggregate delay
/// (`B × DRAIN_PER_EVENT`) — coarse-grained to avoid tokio's ~1ms per-sleep timer
/// floor inflating many tiny sleeps — after which the hop's tail is indexed and
/// the append signal pulsed. A drain-only hop cannot serve until this fires.
async fn spawn_drain(index: SharedWalIndex, b: usize, tail: Vec<serde_json::Value>) {
    tokio::spawn(async move {
        let drain_time = DRAIN_PER_EVENT * b as u32;
        if !drain_time.is_zero() {
            tokio::time::sleep(drain_time).await;
        }
        let mut idx = index.lock().await;
        for p in &tail {
            idx.apply(p);
        }
        drop(idx);
        index.notify_appended();
    });
}

fn pctl(sorted: &[Duration], q: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn stats(mut xs: Vec<Duration>) -> (Duration, Duration, Duration, f64) {
    xs.sort();
    let p50 = pctl(&xs, 0.50);
    let p95 = pctl(&xs, 0.95);
    let max = *xs.last().unwrap_or(&Duration::ZERO);
    let mean = xs.iter().map(|d| d.as_secs_f64()).sum::<f64>() / xs.len().max(1) as f64;
    let var = xs.iter().map(|d| (d.as_secs_f64() - mean).powi(2)).sum::<f64>() / xs.len().max(1) as f64;
    (p50, p95, max, var.sqrt())
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Deterministic mechanism proof (no timing, runs in the default gate): a warm
/// index missing only the hop's tail CANNOT serve `expected_head` (the drain
/// hasn't delivered the tip); applying the server-attached tail makes the build
/// serve immediately — proving the #156 hot-path change removes the dependence on
/// the drain catching up, with zero drain involvement.
#[tokio::test]
async fn tail_attach_serves_without_drain() {
    let playbook = json!({ "name": "harness" });
    let head = 40i64;
    let warm_to = head - TAIL;
    let all = chain_payloads(head);
    let index = SharedWalIndex::new(WalEventIndex::new());
    {
        let mut g = index.lock().await;
        for p in all.iter().take(warm_to as usize) {
            g.apply(p);
        }
    }

    // Drain has NOT delivered the tip → the build cannot complete the chain to
    // `expected_head`, exactly as today under drain lag.
    let before = build_offserver_input(
        &index, EXEC, &playbook, Some("command.completed"), Some(head), Some(head), false,
    )
    .await;
    assert!(before.is_none(), "warm-but-short index must not serve the new head");

    // The server-attached tail (the #156 worker hot path) — apply it up front.
    {
        let mut g = index.lock().await;
        for p in all.iter().skip(warm_to as usize) {
            g.apply(p);
        }
    }
    let after = build_offserver_input(
        &index, EXEC, &playbook, Some("command.completed"), Some(head), Some(head), false,
    )
    .await;
    assert!(after.is_some(), "after applying the attached tail the build must serve, drain-independent");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "latency harness; run explicitly with --ignored --nocapture"]
async fn offserver_tail_attach_collapses_per_hop_variance() {
    let playbook = json!({ "name": "harness" });
    // expected_head after the hop; warm index covers everything but the tail.
    let head = 40i64;
    let warm_to = head - TAIL;
    let all = chain_payloads(head);
    let warm: Vec<_> = all.iter().take(warm_to as usize).cloned().collect();
    let tail: Vec<_> = all.iter().skip(warm_to as usize).cloned().collect();
    assert_eq!(tail.len() as i64, TAIL);

    // Sweep of global event-log backlog sizes (proxy for total stream volume).
    let sweep = [0usize, 1_000, 4_000, 8_000, 16_000, 24_000];
    let trials = 10usize;

    println!("\n#156 off-server per-hop time-to-serve — drain-only (BEFORE) vs tail-attach (AFTER)");
    println!("  warm index to id {warm_to}, hop tail = {TAIL} events, drive budget = {}ms, drain {}µs/event",
        RETRY_MS * RETRY_ATTEMPTS as u64, DRAIN_PER_EVENT.as_micros());
    println!("  (a 'miss' = budget elapsed before serve → server falls to the 8s reconcile tick)\n");
    println!("  {:>8} | {:^42} | {:^28}", "global", "BEFORE (drain-only)", "AFTER (tail-attach)");
    println!("  {:>8} | {:>7} {:>7} {:>7} {:>6} {:>5} | {:>7} {:>7} {:>5} {:>5}",
        "backlog", "p50ms", "p95ms", "maxms", "sd", "miss", "p50ms", "p95ms", "sd", "miss");
    println!("  {:-<8}-+-{:-<42}-+-{:-<28}", "", "", "");

    for &b in &sweep {
        let mut before = Vec::new();
        let mut before_miss = 0usize;
        let mut after = Vec::new();

        for _ in 0..trials {
            // BEFORE: index fed ONLY by the drain; the hop's tip sits behind the
            // whole global backlog.
            let idx = SharedWalIndex::new(WalEventIndex::new());
            {
                let mut g = idx.lock().await;
                for p in &warm {
                    g.apply(p);
                }
            }
            spawn_drain(idx.clone(), b, tail.clone()).await;
            match time_to_serve(&idx, head, &playbook).await {
                Some(d) => before.push(d),
                None => {
                    before_miss += 1;
                    // Record the cliff so percentiles reflect it: 8s reconcile +
                    // the budget already spent.
                    before.push(Duration::from_millis(8_000 + RETRY_MS * RETRY_ATTEMPTS as u64));
                }
            }

            // AFTER: the server attached the tail; the worker applies it to the
            // index up front, so the build serves regardless of the drain. The
            // same global backlog drain runs concurrently but is now irrelevant.
            let idx2 = SharedWalIndex::new(WalEventIndex::new());
            {
                let mut g = idx2.lock().await;
                for p in &warm {
                    g.apply(p);
                }
            }
            spawn_drain(idx2.clone(), b, tail.clone()).await;
            {
                // worker's #156 tail-apply (the new hot-path lines in command.rs).
                let mut g = idx2.lock().await;
                for p in &tail {
                    g.apply(p);
                }
            }
            after.push(time_to_serve(&idx2, head, &playbook).await.expect("tail-attach always serves"));
        }

        let (bp50, bp95, bmax, bsd) = stats(before);
        let (ap50, ap95, _amax, asd) = stats(after);
        println!("  {:>8} | {:>7.2} {:>7.2} {:>7.1} {:>6.1} {:>4}/{:<2} | {:>7.3} {:>7.3} {:>5.3} {:>4}/{:<2}",
            b, ms(bp50), ms(bp95), ms(bmax), ms(Duration::from_secs_f64(bsd)),
            before_miss, trials,
            ms(ap50), ms(ap95), ms(Duration::from_secs_f64(asd)), 0, trials);

        // The result: tail-attach serves with zero misses and p95 far below the
        // drain-only arm once the backlog is non-trivial.
        if b >= 16_000 {
            assert!(before_miss > 0, "high global load must push the drain-only arm past budget (the cliff)");
        }
        assert!(ap95 < bp95 || b == 0, "tail-attach p95 must not exceed drain-only p95 under load");
    }
    println!();
}
