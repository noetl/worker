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
