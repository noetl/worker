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
pub mod metrics;
pub mod metrics_server;
pub mod nats;
pub mod ratelimit;
pub mod scrub;
pub mod snowflake;
pub mod spool_runtime;
pub mod subscription;
pub mod worker;

pub use config::WorkerConfig;
pub use subscription::SubscriptionRuntime;
pub use worker::Worker;
