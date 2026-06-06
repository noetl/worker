//! Control plane HTTP client module.
//!
//! R-1.2 PR-EE-3: `WorkerEvent` is gone; `ExecutorEvent` (re-exported
//! from `noetl_executor::events`) is the shared event-envelope shape.

mod control_plane;
pub mod sealed;
pub mod tls;

pub use control_plane::{ClaimResult, Command, ControlPlaneClient, Credential, ExecutorEvent};
pub use sealed::{open as sealed_open, SealedEnvelope, SEAL_ALG, SEAL_V};
