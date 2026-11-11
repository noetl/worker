//! Command execution module.

pub(crate) mod agent_host;
mod auth_alias;
mod case_evaluator;
pub(crate) mod command;
mod keychain_namespace;
pub(crate) mod mcp;

pub use case_evaluator::CaseEvaluator;
pub use command::CommandExecutor;
