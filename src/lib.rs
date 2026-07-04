//! NoETL Worker Pool
//!
//! Executes workflow commands received from the control plane via NATS.
//!
//! This crate provides:
//! - NATS JetStream subscriber for command notifications
//! - Control plane HTTP client for command fetching and event emission
//! - Command executor with tool dispatch
//! - Case/when/then evaluation

pub mod client;
pub mod config;
pub mod events;
pub mod executor;
pub mod materializer;
pub mod result_locator;
pub mod result_materializer;
pub mod result_producer_stage;
pub mod result_resolver;
pub mod metrics;
pub mod state_builder;
pub mod state_locator;
pub mod state_materializer;
pub mod state_reader;
pub mod metrics_server;
pub mod nats;
/// WASM plug-in host for the system worker pool (noetl/ai-meta#105). Gated
/// behind the `wasm-plugin` feature while it is an unwired skeleton.
#[cfg(feature = "wasm-plugin")]
pub mod plugin;
pub mod ratelimit;
pub mod scrub;
pub mod sharding;
pub mod snowflake;
pub mod spool_runtime;
pub mod subscription;
pub mod worker;

pub use config::WorkerConfig;
pub use subscription::SubscriptionRuntime;
pub use worker::Worker;
