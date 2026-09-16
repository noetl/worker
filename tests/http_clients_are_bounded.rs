//! Every HTTP client in the worker must have a timeout.
//!
//! ⚠ `reqwest::Client::new()` has **no timeout at all** — not a long one, none.
//! A server that accepts the connection and then stalls parks the caller
//! forever: no completion, no error, no retry. On a background drain loop that
//! is invisible, because nothing is waiting on it to report.
//!
//! Three such clients existed: two in `materializer.rs`'s drain loops and one
//! in `plugin.rs`'s module fetch. They were found while ruling out causes for
//! noetl/worker#316, whose symptom profile is exactly this shape — a span entry
//! followed by silence.
//!
//! ⚠⚠ The bounds are deliberately GENEROUS (300s / 120s) rather than matching
//! the control-plane client's 30s. `project` posts a whole batch, and a timeout
//! shorter than a legitimate slow batch converts a working-but-slow system into
//! a failing one. The goal is to replace "hangs forever" with "fails and
//! retries", not to police latency.

const SOURCES: &[(&str, &str)] = &[
    ("materializer.rs", include_str!("../src/materializer.rs")),
    ("plugin.rs", include_str!("../src/plugin.rs")),
    ("worker.rs", include_str!("../src/worker.rs")),
    (
        "client/control_plane.rs",
        include_str!("../src/client/control_plane.rs"),
    ),
    ("client/tls.rs", include_str!("../src/client/tls.rs")),
];

/// Lines constructing an unbounded client, ignoring comments.
///
/// ⚠ Comments are excluded because this crate documents the hazard by name in
/// several places, and a matcher that reads prose as code reports the warning as
/// the defect — a false reading this repo has produced before.
fn unbounded_constructions(src: &str) -> Vec<(usize, String)> {
    src.lines()
        .enumerate()
        .filter(|(_, l)| {
            let t = l.trim_start();
            !t.starts_with("//") && !t.starts_with("///") && !t.starts_with('*')
        })
        .filter(|(_, l)| l.contains("reqwest::Client::new()"))
        .map(|(i, l)| (i + 1, l.trim().to_string()))
        .collect()
}

/// Is this occurrence an explicitly-logged fallback rather than a plain default?
///
/// The one permitted use is the `unwrap_or_else` arm of a builder that already
/// set a timeout: unreachable in practice, and it shouts rather than degrading
/// silently. The marker is the word UNBOUNDED in its own log line.
fn is_logged_fallback(src: &str, line_no: usize) -> bool {
    let lines: Vec<&str> = src.lines().collect();
    let lo = line_no.saturating_sub(10);
    lines[lo..line_no.min(lines.len())]
        .iter()
        .any(|l| l.contains("UNBOUNDED"))
}

/// ⭐ No unbounded HTTP client may be constructed.
#[test]
fn no_worker_http_client_is_built_without_a_timeout() {
    let mut offenders = Vec::new();
    for (name, src) in SOURCES {
        for (line_no, text) in unbounded_constructions(src) {
            if is_logged_fallback(src, line_no) {
                continue;
            }
            offenders.push(format!("{name}:{line_no}  {text}"));
        }
    }
    assert!(
        offenders.is_empty(),
        "these construct an HTTP client with NO timeout:\n  {}\n\n\
         `reqwest::Client::new()` has no default timeout. A peer that accepts \
         the connection and stalls will park the caller indefinitely — no \
         completion, no error, no retry. On a background loop nothing reports \
         it, and the work simply stops.\n\n\
         Use `reqwest::Client::builder().timeout(..).build()`. Prefer a bound \
         comfortably above the slowest legitimate call: the aim is to turn an \
         unbounded hang into a failure that retries, not to police latency.",
        offenders.join("\n  ")
    );
}

/// The positive control: the matcher must find what it looks for, and must not
/// fire on prose describing it.
#[test]
fn the_matcher_can_actually_find_an_unbounded_client() {
    let planted = "fn f() {\n    let c = reqwest::Client::new();\n}";
    assert_eq!(
        unbounded_constructions(planted).len(),
        1,
        "the matcher cannot see a plain unbounded construction"
    );

    let discussed = "/// never use reqwest::Client::new() here\nfn f() {}";
    assert!(
        unbounded_constructions(discussed).is_empty(),
        "the matcher counts comments as code — it would report the warning as \
         the defect"
    );

    // And the fallback exemption must require the marker, not just proximity.
    let unmarked = "let c = builder.build().unwrap_or_else(|_| reqwest::Client::new());";
    assert!(
        !is_logged_fallback(unmarked, 1),
        "an unmarked fallback must NOT be exempt, or the exemption swallows the rule"
    );
}
