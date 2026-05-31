//! Worker lifecycle management.
//!
//! R-1.2 PR-2d-2: command pulling is now driven through the
//! `noetl_executor::worker::source::CommandSource` trait via
//! [`crate::nats::NatsCommandSource`].  `Worker::process_commands`
//! is generic over the trait's `next()` + `ack()` / `nack()`
//! lifecycle, so unit tests can swap in
//! `noetl_executor::worker::source::tests::MockSource` to drive
//! the dispatcher with synthetic outcomes.

use anyhow::Result;
use noetl_executor::worker::source::{ClaimOutcome, CommandSource, Pulled};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

use crate::client::ControlPlaneClient;
use crate::config::WorkerConfig;
use crate::executor::CommandExecutor;
use crate::nats::{NatsCommandSource, NatsSubscriber};

/// Worker pool that processes commands.
pub struct Worker {
    /// Worker configuration.
    config: WorkerConfig,

    /// Pull-model command source backed by NATS JetStream + the
    /// control-plane HTTP API.  Behind a `Mutex` so
    /// `process_commands` (which takes `&self`) can call `next()`
    /// (which takes `&mut self`).
    source: Arc<Mutex<NatsCommandSource>>,

    /// Control plane HTTP client — used by Worker for register /
    /// deregister / heartbeat / set_variable paths that aren't part
    /// of the pull-loop.  `NatsCommandSource` owns its own clone for
    /// the `claim_command` calls inside `next()`.
    client: ControlPlaneClient,

    /// Command executor.
    executor: Arc<CommandExecutor>,

    /// Semaphore for concurrency control.
    semaphore: Arc<Semaphore>,
}

impl Worker {
    /// Create a new worker.
    pub async fn new(config: WorkerConfig) -> Result<Self> {
        // Connect to NATS
        let subscriber =
            NatsSubscriber::connect(&config.nats_url, &config.nats_stream, &config.nats_consumer)
                .await?;

        // Create HTTP client
        let client = ControlPlaneClient::new(&config.server_url);

        // Wrap subscriber + client into the trait-conformant
        // command source.  The source owns its own clone of the
        // client for `claim_command` calls; Worker keeps a
        // separate clone for register / deregister / heartbeat.
        let source = Arc::new(Mutex::new(NatsCommandSource::new(
            subscriber,
            client.clone(),
            config.worker_id.clone(),
        )));

        // Create executor
        let executor = Arc::new(CommandExecutor::new(
            client.clone(),
            config.worker_id.clone(),
            config.server_url.clone(),
        ));

        // Create semaphore for concurrency control
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_tasks));

        Ok(Self {
            config,
            source,
            client,
            executor,
            semaphore,
        })
    }

    /// Run the worker.
    pub async fn run(&self) -> Result<()> {
        // Register worker
        self.register().await?;

        // Start heartbeat task
        let heartbeat_handle = self.start_heartbeat();

        // Process commands
        let result = self.process_commands().await;

        // Stop heartbeat
        heartbeat_handle.abort();

        // Deregister worker
        self.deregister().await?;

        result
    }

    /// Register the worker with the control plane.
    async fn register(&self) -> Result<()> {
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        self.client
            .register_worker(&self.config.worker_id, &self.config.pool_name, &hostname)
            .await?;

        tracing::info!(
            worker_id = %self.config.worker_id,
            pool_name = %self.config.pool_name,
            hostname = %hostname,
            "Worker registered"
        );

        Ok(())
    }

    /// Deregister the worker.
    async fn deregister(&self) -> Result<()> {
        self.client
            .deregister_worker(&self.config.worker_id, &self.config.pool_name)
            .await?;

        tracing::info!(
            worker_id = %self.config.worker_id,
            "Worker deregistered"
        );

        Ok(())
    }

    /// Start the heartbeat background task.
    fn start_heartbeat(&self) -> tokio::task::JoinHandle<()> {
        let client = self.client.clone();
        let worker_id = self.config.worker_id.clone();
        let pool_name = self.config.pool_name.clone();
        let interval = self.config.heartbeat_interval;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // Skip first immediate tick

            loop {
                ticker.tick().await;

                if let Err(e) = client.heartbeat(&worker_id, &pool_name).await {
                    tracing::warn!(error = %e, "Heartbeat failed");
                } else {
                    tracing::trace!("Heartbeat sent");
                }
            }
        })
    }

    /// Process commands from the configured `CommandSource`.
    ///
    /// R-1.2 PR-2d-2: rewritten to drive through the
    /// `noetl_executor::worker::source::CommandSource` trait
    /// (`source.next()` + `source.ack()` / `source.nack()`) instead of
    /// inline `subscriber.receive()` + `client.claim_command()` +
    /// `subscriber.ack()` calls.  The four `ClaimOutcome` variants
    /// map 1:1 onto the worker's pre-PR-2d-2 control flow.
    async fn process_commands(&self) -> Result<()> {
        loop {
            // Wait for available slot
            let permit = self.semaphore.clone().acquire_owned().await?;

            // Pull one item from the source.  The Mutex is held only
            // for the duration of `next()` + the corresponding ack /
            // nack; dispatch happens after the lock is released.
            let pulled = {
                let mut source = self.source.lock().await;
                source.next().await?
            };

            let Some(Pulled { outcome, ack }) = pulled else {
                // Source exhausted (local-mode playbook complete);
                // long-running NATS source never returns None in
                // normal operation but we tolerate it for testability
                // and the brief gap when no messages are queued.
                drop(permit);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            };

            match outcome {
                ClaimOutcome::Claimed(command) => {
                    tracing::debug!(
                        command_id = %command.command_id,
                        execution_id = command.execution_id,
                        step = %command.step,
                        "Command claimed"
                    );

                    // Ack the source handle now that we own the
                    // command; dispatch happens off the pull loop.
                    {
                        let source = self.source.lock().await;
                        source.ack(ack).await?;
                    }

                    // Spawn task to process command
                    let executor = self.executor.clone();
                    let command_id = command.command_id.clone();

                    tokio::spawn(async move {
                        // Keep permit until done
                        let _permit = permit;

                        if let Err(e) = executor.execute(&command).await {
                            tracing::error!(
                                command_id = %command_id,
                                error = %e,
                                "Command execution failed"
                            );
                        }
                    });
                }
                ClaimOutcome::AlreadyClaimed => {
                    tracing::debug!("Command already claimed by another worker");

                    // Ack — another worker has it, no redelivery.
                    {
                        let source = self.source.lock().await;
                        source.ack(ack).await?;
                    }

                    // Release permit immediately
                    drop(permit);
                }
                ClaimOutcome::RetryLater(error) => {
                    tracing::warn!(
                        error = %error,
                        "Transient claim failure, requesting redelivery"
                    );

                    // Nack for redelivery on transient overload /
                    // contention.
                    {
                        let source = self.source.lock().await;
                        source.nack(ack).await?;
                    }
                    drop(permit);
                }
                ClaimOutcome::Failed(error) => {
                    tracing::error!(
                        error = %error,
                        "Failed to claim command"
                    );

                    // Nack for redelivery.
                    {
                        let source = self.source.lock().await;
                        source.nack(ack).await?;
                    }
                    drop(permit);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_config() {
        let config = WorkerConfig::default();
        assert!(!config.worker_id.is_empty());
        assert_eq!(config.pool_name, "default");
    }
}
