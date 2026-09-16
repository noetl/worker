//! The oversize-refusal signal must be wired — noetl/worker#326.
//!
//! ⚠ A latch nothing sets, or a promotion log that does not consult it, leaves
//! the original defect exactly in place: a WARN saying an append was refused,
//! followed milliseconds later by an INFO saying the tier "answered
//! authoritatively" — and the INFO is the line visible at default level.

const EVENTLOG: &str = include_str!("../src/ehdb/eventlog.rs");

/// Production code only — test modules stripped by brace matching.
fn production_code(src: &str) -> String {
    let code: String = src
        .lines()
        .filter(|l| {
            let t = l.trim_start();
            !t.starts_with("//") && !t.starts_with("///") && !t.starts_with('*')
        })
        .collect::<Vec<_>>()
        .join("\n");
    let mut out = String::with_capacity(code.len());
    let mut rest = code.as_str();
    while let Some(i) = rest.find("#[cfg(test)]") {
        out.push_str(&rest[..i]);
        let after = &rest[i..];
        let Some(open) = after.find('{') else { break };
        let (mut depth, mut end) = (0usize, None);
        for (off, ch) in after[open..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(open + off + 1);
                        break;
                    }
                }
                _ => {}
            }
        }
        match end {
            Some(e) => rest = &after[e..],
            None => break,
        }
    }
    out.push_str(rest);
    out
}

/// ⭐ The refusal must be recorded where appends fail.
#[test]
fn the_refusal_is_recorded_on_the_failure_path() {
    let code = production_code(EVENTLOG);
    assert!(
        code.contains("note_oversize_refusal("),
        "nothing records an oversize refusal, so the latch never sets and the \
         promotion line keeps claiming the tier answered authoritatively"
    );
    assert!(
        code.contains("refusing to write") && code.contains("exceeds the"),
        "the classifier must match BOTH directions of the writer's own wording — \
         a refused request frame and an over-cap reply frame"
    );
}

/// ⭐ And cleared where they land, or the tier is described as incomplete forever.
#[test]
fn the_latch_is_cleared_when_an_append_lands() {
    let code = production_code(EVENTLOG);
    assert!(
        code.contains("note_append_landed()"),
        "nothing clears the latch — one refusal would suppress the authoritative \
         claim permanently, which is its own kind of wrong answer"
    );
}

/// ⭐⭐ The promotion log must consult the latch. This is the defect itself.
#[test]
fn the_authoritative_claim_consults_the_latch() {
    let code = production_code(EVENTLOG);
    let from = code
        .find("fn log_serve_transition")
        .expect("the serve-transition logger");
    let body = &code[from..];
    let body = &body[..body.find("\n}").map(|i| i + 2).unwrap_or(body.len())];
    assert!(
        body.contains("oversize_refusal_outstanding()"),
        "the serve-transition logger does not consult the refusal latch, so it \
         still emits \"IS SERVING … answered authoritatively\" while an append is \
         known refused:\n{body}"
    );
}

/// Positive control: the matchers must be able to fail, and must ignore prose.
#[test]
fn the_matchers_can_actually_fail() {
    let gutted = "fn log_serve_transition() {\n    info!(\"IS SERVING\");\n}\n";
    assert!(
        !production_code(gutted).contains("oversize_refusal_outstanding()"),
        "a logger that ignores the latch must not satisfy the matcher"
    );
    let discussed = "/// calls note_oversize_refusal( here\nfn f() {}";
    assert!(
        !production_code(discussed).contains("note_oversize_refusal("),
        "the matcher counts comments as code"
    );
    // Interleaved test modules must not hide production code.
    let inter = "fn a() { note_append_landed(); }\n#[cfg(test)]\nmod t { fn f() {} }\nfn b() { note_oversize_refusal(\"x\"); }";
    let p = production_code(inter);
    assert!(
        p.contains("note_append_landed()") && p.contains("note_oversize_refusal("),
        "production code on both sides of a test module must survive"
    );
}
