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
    /// Lift the hold only together with `noetl-executor` — 0.9.x is published
    /// and presumably carries the new field. The two move as a pair.
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
