//! NATS JetStream subscriber module.

pub mod lag_poller;
mod source;
mod subscriber;

pub use source::{segment_from_filter, NatsCommandSource};
pub use subscriber::{CommandNotification, ConsumerLag, NatsSubscriber};

// Shared claim path + ack handle reused by the EHDB command source
// (noetl/ai-meta#194 L1 T4).
pub(crate) use source::{claim_outcome, NatsAckHandle};
