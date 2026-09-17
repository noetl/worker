//! NoETL Worker Pool
//!
//! Executes workflow commands received from the control plane over the EHDB
//! command bus. NATS was the original transport and was deleted at T5
//! (noetl/ai-meta#194); nothing here talks to it any more.
//!
//! This crate provides:
//! - EHDB command-bus consumer for command notifications
//! - Control plane HTTP client for command fetching and event emission
//! - Command executor with tool dispatch
//! - Case/when/then evaluation

pub mod autosink;
pub mod client;
pub mod command_bus;
pub mod config;
pub mod dispatch;
pub mod ehdb;
pub mod event_bus;
pub mod events;
pub mod executor;
pub mod graceful;
pub mod materializer;
pub mod metrics;
pub mod secrets;
pub mod metrics_server;
/// WASM plug-in host for the system worker pool (noetl/ai-meta#105). Gated
/// behind the `wasm-plugin` feature while it is an unwired skeleton.
#[cfg(feature = "wasm-plugin")]
pub mod plugin;
pub mod ratelimit;
pub mod result_locator;
pub mod result_materializer;
pub mod result_producer_stage;
pub mod result_resolver;
pub mod scrub;
pub mod sharding;
pub mod snowflake;
pub mod spool_runtime;
pub mod state_builder;
pub mod state_locator;
pub mod state_materializer;
pub mod state_reader;
pub mod subscription;
pub mod worker;

pub use config::WorkerConfig;
pub use subscription::SubscriptionRuntime;
pub use worker::Worker;

/// Release-pipeline guards (noetl/ai-meta#330).
///
/// Nothing here runs at runtime — these assert facts about the release
/// workflows that no unit test could otherwise reach.
#[cfg(test)]
mod release_pipeline {
    /// ⚠⚠ **Every job that builds an artifact MUST stamp the version first.**
    ///
    /// The bot no longer pushes a version bump to `main` — a required status
    /// check rejects that push, because the commit is `[skip ci]` so the check
    /// never runs on it and stays "expected" forever. The tag is authoritative
    /// now, and the checked-out tree carries a stale floor in `Cargo.toml`.
    ///
    /// `CARGO_PKG_VERSION` is compiled into the binary and reported as
    /// `noetl_worker_build_info{version="..."}`. A build job that skips the
    /// stamp still compiles, still pushes an image, still deploys, still passes
    /// every health check — and reports the PREVIOUS version forever. There is
    /// no failure to notice. That is exactly the shape this repo keeps paying
    /// for, so it gets a guard rather than a comment.
    #[test]
    fn every_artifact_build_job_stamps_the_version() {
        let wf = include_str!("../.github/workflows/release.yml");

        // Split into top-level jobs: a line with exactly 2 spaces of indent
        // ending in `:` starts one.
        let mut jobs: Vec<(String, String)> = Vec::new();
        let mut name = String::new();
        let mut body = String::new();
        for line in wf.lines() {
            let is_job_header = line.starts_with("  ")
                && !line.starts_with("   ")
                && line.trim_end().ends_with(':')
                && !line.trim_start().starts_with('#');
            if is_job_header {
                if !name.is_empty() {
                    jobs.push((name.clone(), std::mem::take(&mut body)));
                }
                name = line.trim().trim_end_matches(':').to_string();
            } else if !name.is_empty() {
                body.push_str(line);
                body.push('\n');
            }
        }
        if !name.is_empty() {
            jobs.push((name, body));
        }
        assert!(
            jobs.len() >= 4,
            "job extraction broke — found {} jobs, so this guard is not \
             inspecting what it claims to",
            jobs.len()
        );

        // A job builds a shipped artifact if it invokes a container build.
        let builders: Vec<&(String, String)> = jobs
            .iter()
            .filter(|(_, b)| {
                b.contains("docker/build-push-action") || b.contains("gcloud builds submit")
            })
            .collect();
        assert!(
            !builders.is_empty(),
            "no artifact-building job found in release.yml — the detector is \
             matching nothing, which would make this guard vacuously green"
        );

        for (job, b) in builders {
            assert!(
                b.contains("ci/stamp-version.sh"),
                "release.yml job `{job}` builds an artifact but never runs \
                 ci/stamp-version.sh. The image would ship reporting the \
                 version in the committed Cargo.toml floor, which is stale by \
                 design since the bot stopped pushing to main."
            );
        }
    }

    /// ⚠ The release must not reintroduce a push to `main`.
    ///
    /// `@semantic-release/git` is the plugin that committed the version bump
    /// and pushed it. Re-adding it re-breaks the release the moment a required
    /// status check is on `main` — which is the whole reason this shape exists.
    #[test]
    fn semantic_release_does_not_push_to_main() {
        let rc = include_str!("../.releaserc.json");
        // ⚠ Match the QUOTED name. `@semantic-release/git` is a prefix of
        // `@semantic-release/github`, which is still in use — a plain
        // `contains` here reported the plugin as present when it was not. The
        // first version of this guard did exactly that and failed on a correct
        // config, which is the same substring-matching class as the mutation
        // noetl/ai-meta#330's own guard nearly shipped.
        assert!(
            !rc.contains("\"@semantic-release/git\""),
            "@semantic-release/git is back in .releaserc.json. It pushes the \
             version bump to `main`, which a required status check rejects \
             (GH006) — the commit is [skip ci], so the check never runs on it \
             and stays `expected` forever."
        );
    }
}

/// Dependency-range guards.
///
/// Nothing here runs at runtime — these assert facts about `Cargo.toml` that a
/// resolver would otherwise be free to decide differently on a future release.
#[cfg(test)]
mod dependency_ranges {
    /// ⚠⚠ **A caret range is a decision made later by a resolver.**
    ///
    /// `noetl-tools = "3.19.1"` means `^3.19.1`, so a release could — and did —
    /// resolve **3.27.0**, which added a required `child_execution_id` field to
    /// `ToolResult`. `noetl-executor 0.5.0`, which this crate also depends on,
    /// constructs `ToolResult` without it, so the workspace stopped compiling.
    ///
    /// It broke on the `chore(release): version 5.131.2` commit **itself** —
    /// that is where the lock is re-resolved — so every PR's CI had passed
    /// against the older lock and nothing failed until `main` was already
    /// tagged. That is the shape worth guarding: the breakage appears after the
    /// last gate that could have caught it.
    ///
    /// This is the second time in this crate (worker#183 silently dropped
    /// DuckDB from the shipped image the same way).
    ///
    /// Lift the hold only together with `noetl-executor`. The two move as a
    /// pair, and they did: `~3.26.3` + `0.5` became `~4.0` + `0.10`
    /// (noetl/ai-meta#330, noetl/cli#89).
    ///
    /// ⚠ The hold SURVIVED that lift, deliberately. noetl-tools 4.0 seals
    /// `ToolResult` with `#[non_exhaustive]`, which removes the exact mechanism
    /// described above — a field added to it can no longer break a downstream
    /// literal, because downstream can no longer write one. That retires this
    /// specific failure, not the class. A new enum variant elsewhere, or a
    /// changed default, breaks a resolve the same way and still does it on the
    /// release commit. So the range stays `~`, and going to `^` is a decision
    /// someone makes with evidence rather than one a resolver makes for them.
    #[test]
    fn noetl_tools_is_held_not_caret_ranged() {
        let manifest = include_str!("../Cargo.toml");
        let line = manifest
            .lines()
            .find(|l| l.trim_start().starts_with("noetl-tools = "))
            .expect("noetl-tools dependency line not found — extraction broke");
        assert!(
            line.contains('~') || line.contains('=') && line.contains("\"="),
            "noetl-tools is caret-ranged ({}). A resolver may pick a minor \
             release with a breaking change, and it will do so on the RELEASE \
             commit, after every PR gate has passed. Hold it, and lift the hold \
             together with noetl-executor.",
            line.trim()
        );
        assert!(
            !line.contains("\"3.19.1\""),
            "the bare ^3.19.1 range is back — this is exactly what resolved to \
             3.27.0 and broke the build"
        );
    }

    /// The pair that must move together.
    ///
    /// Without this, someone lifts the `noetl-tools` hold on its own, the
    /// resolver takes 3.27.0 again, and `noetl-executor 0.5.0` breaks again —
    /// the same failure, reintroduced by the fix for it.
    #[test]
    fn lifting_the_tools_hold_requires_moving_the_executor_too() {
        let manifest = include_str!("../Cargo.toml");
        let tools_held = manifest
            .lines()
            .any(|l| l.trim_start().starts_with("noetl-tools = ") && l.contains('~'));
        let executor_05 = manifest
            .lines()
            .any(|l| l.trim_start().starts_with("noetl-executor = ") && l.contains("0.5"));
        assert!(
            tools_held || !executor_05,
            "noetl-tools is unheld while noetl-executor is still on 0.5.x. \
             0.5.0 cannot build against noetl-tools >= 3.27.0; move both or \
             neither."
        );
    }
}
