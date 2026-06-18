//! NATS JetStream subscriber module.

pub mod lag_poller;
mod source;
mod subscriber;

pub use source::{segment_from_filter, NatsCommandSource};
pub use subscriber::{CommandNotification, ConsumerLag, NatsSubscriber};
