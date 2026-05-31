//! NATS JetStream subscriber module.

pub mod lag_poller;
mod source;
mod subscriber;

pub use source::NatsCommandSource;
pub use subscriber::{CommandNotification, ConsumerLag, NatsSubscriber};
