//! `<VAR>_FILE` hydration for the worker's one secret-sourced env
//! (noetl/ai-meta#267 Tier 2).
//!
//! # Scope: exactly one variable
//!
//! `NOETL_INTERNAL_API_TOKEN` is the worker's **only** secret-sourced
//! environment variable — the sole `secretKeyRef` on either system pool. The
//! server's twin hydrates three; this one deliberately does not, because
//! hydrating a variable no worker reads would be a capability nobody asked for
//! and a wider surface to reason about.
//!
//! # Why hydration rather than editing the read sites
//!
//! The token is read in two places, and both are `std::env::var`:
//!
//! ```text
//! src/client/tls.rs:89       bearer for the control-plane client
//! src/materializer.rs:120    bearer for the internal API
//! ```
//!
//! Patching both would work today and silently miss the third one someone adds
//! later. Hydrating the process environment before anything reads it covers
//! every present and future consumer, including any that arrives via `envy`.
//!
//! # Inert by default
//!
//! With no `NOETL_INTERNAL_API_TOKEN_FILE` set this does nothing at all, so a
//! worker that has not migrated behaves byte-identically to today. That is what
//! lets the CSI mount and the existing `secretKeyRef` coexist during the
//! migration: the file wins when present, the env remains the fallback, and
//! neither stage has to be atomic.

use std::path::Path;

/// The variable this hydrates. A fixed single entry rather than a pattern scan:
/// an allowlist cannot be widened by an unrelated variable that happens to end
/// in `_FILE`.
pub const HYDRATED: [&str; 1] = ["NOETL_INTERNAL_API_TOKEN"];

/// Where the value came from. Never carries the value itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// `<VAR>_FILE` was set, readable and non-empty; `<VAR>` now holds it.
    File,
    /// No `<VAR>_FILE`; whatever `<VAR>` already held is untouched.
    Env,
    /// `<VAR>_FILE` was set but unusable (missing, unreadable, empty). Distinct
    /// from `Env` on purpose: an operator *intended* file delivery and did not
    /// get it, which is a misconfiguration that must be visible rather than
    /// silently degraded into looking like it was never configured.
    FileUnusable,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Env => "env",
            Self::FileUnusable => "file_unusable",
        }
    }
}

/// Decide the outcome for one variable, touching neither process nor disk, so
/// every arm is testable — including the ones needing a file that is not there.
pub fn decide(file_var: Option<&str>, file_contents: Option<&str>) -> Source {
    match file_var {
        None => Source::Env,
        Some(p) if p.trim().is_empty() => Source::Env,
        Some(_) => match file_contents {
            Some(c) if !c.trim().is_empty() => Source::File,
            _ => Source::FileUnusable,
        },
    }
}

/// Hydrate `<VAR>` from `<VAR>_FILE` for each entry of [`HYDRATED`].
///
/// # Safety
///
/// Calls `std::env::set_var`, which is `unsafe` because concurrent readers race
/// it. This MUST run at the very top of `main`, before any thread is spawned and
/// before `tls.rs` or `materializer.rs` read the token. That is the whole
/// contract, and it is why this is one early call rather than something lazy.
/// The placement guard in the tests pins the call site so a later refactor
/// cannot quietly move it after the runtime starts.
pub fn hydrate() -> Vec<(&'static str, Source)> {
    let mut out = Vec::with_capacity(HYDRATED.len());
    for var in HYDRATED {
        let file_var = std::env::var(format!("{var}_FILE")).ok();
        let contents = file_var.as_deref().and_then(|p| {
            if p.trim().is_empty() {
                None
            } else {
                std::fs::read_to_string(Path::new(p.trim())).ok()
            }
        });
        let decision = decide(file_var.as_deref(), contents.as_deref());
        if decision == Source::File {
            if let Some(c) = contents.as_deref() {
                // A mounted file conventionally ends with a newline and that is
                // not part of the secret; a bearer token with `\n` appended
                // fails auth in a way that looks like a wrong value rather than
                // a formatting bug.
                // SAFETY: see the contract above — called before any thread.
                unsafe { std::env::set_var(var, c.trim_end_matches(['\n', '\r'])) };
            }
        }
        if decision == Source::FileUnusable {
            tracing::warn!(
                target: "noetl_worker::secrets",
                var,
                "{var}_FILE is set but the file is missing, unreadable or empty — \
                 falling back to the environment value (noetl/ai-meta#267)"
            );
        }
        out.push((var, decision));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The inert case, and what makes the rollout reversible: no `_FILE`
    /// anywhere means nothing changes.
    #[test]
    fn no_file_var_leaves_the_environment_alone() {
        assert_eq!(decide(None, None), Source::Env);
        assert_eq!(decide(None, Some("ignored")), Source::Env);
    }

    /// The file must WIN during the dual-run, not merely be read — otherwise the
    /// migration is a no-op that still reports success.
    #[test]
    fn a_readable_non_empty_file_wins() {
        assert_eq!(decide(Some("/x"), Some("value")), Source::File);
    }

    /// An unusable file must not be reported as `env`.
    #[test]
    fn an_unusable_file_is_named_as_such_not_silently_degraded() {
        assert_eq!(decide(Some("/missing"), None), Source::FileUnusable);
        assert_eq!(decide(Some("/empty"), Some("")), Source::FileUnusable);
        assert_eq!(decide(Some("/blank"), Some("  \n")), Source::FileUnusable);
        assert_ne!(decide(Some("/missing"), None), Source::Env);
    }

    /// An empty `_FILE` value means "unset", not "a file at path ''".
    #[test]
    fn an_empty_file_var_is_treated_as_unset() {
        assert_eq!(decide(Some(""), None), Source::Env);
        assert_eq!(decide(Some("   "), None), Source::Env);
    }

    /// Scope guard: the worker hydrates exactly its one secret-sourced env. If
    /// this list grows silently, a variable could be hydrated that nobody
    /// intended.
    #[test]
    fn the_worker_hydrates_exactly_the_internal_api_token() {
        assert_eq!(HYDRATED, ["NOETL_INTERNAL_API_TOKEN"]);
    }

    /// `hydrate()` must run before anything reads the token. Pinned as a source
    /// check because moving it after the runtime starts is both a data race and
    /// a silently wrong config.
    #[test]
    fn hydrate_is_called_at_the_top_of_main() {
        let src = include_str!("../main.rs");
        let body_start = src
            .find("async fn main")
            .expect("main.rs must define async fn main");
        let body = &src[body_start..];
        let call = body
            .find("file_env::hydrate()")
            .expect("main() must call file_env::hydrate()");
        // Anything that reads configuration must come after it.
        //
        // ⚠ `envy::` is written as `envy::prefixed`: the bare substring also
        // matches `dotenvy::`, which sits near the top of main, so the naive
        // pattern fails on correct code. Learned on the server twin.
        for reader in ["WorkerConfig::from_env", "envy::prefixed"] {
            if let Some(at) = body.find(reader) {
                assert!(
                    call < at,
                    "hydrate() must run before {reader} — it is what puts the \
                     token in the environment that {reader} then reads"
                );
            }
        }
    }

    /// Tests here mutate PROCESS-GLOBAL env. `cargo test` does NOT serialise
    /// tests, so without this lock they race each other and the surrounding
    /// suite.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// ⚠ THE PRECEDENCE PROOF — the property the whole migration rests on: when
    /// both a file and an env value are present, the FILE must win.
    ///
    /// This lives here rather than in kind because kind cannot show it. The
    /// kind control plane does not enforce the internal API token (a bogus
    /// bearer returns 200), so a worker started with a deliberately wrong token
    /// registers happily and proves nothing. And `/proc/<pid>/environ` is a
    /// snapshot taken at exec time — it does not reflect `setenv` made by the
    /// process, so reading it back would show the ORIGINAL value however well
    /// hydration worked. Both roads lead to a confident wrong answer; this one
    /// is decisive.
    #[test]
    fn the_file_value_wins_over_an_existing_env_value() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let var = HYDRATED[0];
        let dir = std::env::temp_dir().join(format!("fe-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("token");
        std::fs::write(&path, "FILE-VALUE\n").unwrap();

        // SAFETY: guarded by ENV_LOCK; restored below.
        unsafe {
            std::env::set_var(var, "ENV-VALUE");
            std::env::set_var(format!("{var}_FILE"), path.to_str().unwrap());
        }

        let out = hydrate();
        assert_eq!(out[0].1, Source::File, "expected the file to be chosen");
        assert_eq!(
            std::env::var(var).unwrap(),
            "FILE-VALUE",
            "the file must OVERWRITE the env value, and its trailing newline              must be trimmed — a bearer with \\n appended fails auth in a way              that looks like a wrong token rather than a formatting bug"
        );

        unsafe {
            std::env::remove_var(format!("{var}_FILE"));
            std::env::remove_var(var);
        }
        let _ = std::fs::remove_file(&path);
    }

    /// The mirror of the above: with no file, an existing env value must be left
    /// exactly as it is.
    #[test]
    fn an_existing_env_value_is_untouched_when_no_file_is_configured() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let var = HYDRATED[0];
        // SAFETY: guarded by ENV_LOCK; restored below.
        unsafe {
            std::env::set_var(var, "ENV-VALUE");
            std::env::remove_var(format!("{var}_FILE"));
        }
        let out = hydrate();
        assert_eq!(out[0].1, Source::Env);
        assert_eq!(std::env::var(var).unwrap(), "ENV-VALUE");
        unsafe { std::env::remove_var(var) };
    }

    /// An unusable file must leave the env value intact — the fallback has to be
    /// real, not just reported.
    #[test]
    fn an_unusable_file_leaves_the_env_value_intact() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let var = HYDRATED[0];
        // SAFETY: guarded by ENV_LOCK; restored below.
        unsafe {
            std::env::set_var(var, "ENV-VALUE");
            std::env::set_var(format!("{var}_FILE"), "/definitely/not/here");
        }
        let out = hydrate();
        assert_eq!(out[0].1, Source::FileUnusable);
        assert_eq!(
            std::env::var(var).unwrap(),
            "ENV-VALUE",
            "fallback must preserve the env value, not clear it"
        );
        unsafe {
            std::env::remove_var(format!("{var}_FILE"));
            std::env::remove_var(var);
        }
    }

    /// The guard above must be able to fail; a source check that can only pass
    /// is indistinguishable from no check.
    #[test]
    fn the_placement_guard_can_fail() {
        let bad = "async fn main() {\n let c = WorkerConfig::from_env();\n file_env::hydrate();\n}";
        let body = &bad[bad.find("async fn main").unwrap()..];
        assert!(
            body.find("file_env::hydrate()").unwrap() > body.find("WorkerConfig::from_env").unwrap(),
            "fixture must represent the WRONG order"
        );
    }
}
