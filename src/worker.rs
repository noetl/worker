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
use noetl_arrow_cache::ArrowIpcSharedMemoryCache;
use noetl_executor::worker::source::{ClaimOutcome, CommandSource, Pulled};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

use crate::client::ControlPlaneClient;
use crate::config::WorkerConfig;
use crate::executor::CommandExecutor;
use crate::nats::{NatsCommandSource, NatsSubscriber};
use crate::snowflake::SnowflakeGen;

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

        // One snowflake generator per worker process — populates the
        // application-side `event_id` on every emitted envelope per
        // `observability.md` Principle 3.  Node id derives from the
        // `NOETL_SNOWFLAKE_NODE_ID` / `NOETL_SHARD_ID` env vars when
        // set (matches the Python broker convention), else from a
        // stable hash of the worker id.
        let snowflake = Arc::new(SnowflakeGen::from_env_or_hint(&config.worker_id));
        tracing::info!(
            worker_id = %config.worker_id,
            snowflake_node_id = snowflake.node_id(),
            "Snowflake generator initialised"
        );

        // One Arrow IPC shared-memory cache per worker process — same-
        // node zero-copy reference path for `call.done` results that
        // exceed the broker's 100KB inline budget.  Reads
        // `NOETL_IPC_CACHE_BUDGET_BYTES` (default 256 MB) and the
        // node-id env chain (`NOETL_NODE_ID` / `NODE_NAME` /
        // `K8S_NODE_NAME` / `HOSTNAME`) from the same conventions
        // Python's `ArrowIpcSharedMemoryCache` reads — so a hint
        // produced by either stack round-trips against the other.
        // Per Appendix H R-2.1; partial progress on noetl/worker#24.
        let arrow_cache = Arc::new(ArrowIpcSharedMemoryCache::new());
        tracing::info!(
            node_id = %arrow_cache.config().node_id,
            budget_bytes = arrow_cache.config().budget_bytes,
            "Arrow IPC shared-memory cache initialised"
        );

        // Create executor
        let executor = Arc::new(CommandExecutor::new(
            client.clone(),
            config.worker_id.clone(),
            config.server_url.clone(),
            snowflake.clone(),
            arrow_cache.clone(),
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

        // Start metrics HTTP server.  Bind failures are
        // immediately surfaced; the server runs in the background
        // for the worker's lifetime.  Per
        // `agents/rules/observability.md` Principle 2.
        let metrics_handle = crate::metrics_server::spawn(&self.config.metrics_bind).await?;

        // Start NATS consumer-lag poller.  Periodically queries
        // JetStream consumer info and updates the
        // `noetl_worker_nats_consumer_pending` +
        // `noetl_worker_nats_consumer_ack_pending` gauges so KEDA
        // and the dashboard can read queue depth without scraping
        // logs.  Cadence is `WORKER_NATS_LAG_POLL_INTERVAL` env (s),
        // default 5s.  Per `observability.md` Principle 2.
        let lag_poll_interval = crate::nats::lag_poller::poll_interval_from_env();
        let lag_handle = crate::nats::lag_poller::spawn(self.source.clone(), lag_poll_interval);
        tracing::info!(
            interval_secs = lag_poll_interval.as_secs(),
            "NATS consumer-lag poller started"
        );

        // Process commands
        let result = self.process_commands().await;

        // Stop heartbeat + metrics server + lag poller
        heartbeat_handle.abort();
        metrics_handle.abort();
        lag_handle.abort();

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
                    let execution_id = command.execution_id;
                    let step = command.step.clone();

                    // Bump the in-flight dispatches gauge for the
                    // duration of the spawn.  The matching dec is
                    // in the spawned task's exit path so the
                    // counter is balanced even on error returns.
                    // Per `observability.md` Principle 2.
                    crate::metrics::inc_concurrent_dispatches();

                    tokio::spawn(async move {
                        // Keep permit until done
                        let _permit = permit;

                        if let Err(e) = executor.execute(&command).await {
                            // Per `observability.md` Principle 4:
                            // structured execution_id on every
                            // ERROR.  Includes `step` for the
                            // playbook-level correlation when
                            // looking at a single execution's
                            // trace.
                            tracing::error!(
                                execution_id,
                                command_id = %command_id,
                                step = %step,
                                error = %e,
                                "Command execution failed"
                            );
                        }
                        crate::metrics::dec_concurrent_dispatches();
                    });
                }
                ClaimOutcome::AlreadyClaimed => {
                    // Per `observability.md` Principle 4: every
                    // WARN/ERROR carries `execution_id` as a
                    // structured field.  `ack.notification` gives
                    // us the ids without requiring the ClaimOutcome
                    // variant to carry them.
                    tracing::debug!(
                        execution_id = ack.notification.execution_id,
                        command_id = %ack.notification.command_id,
                        step = %ack.notification.step,
                        "Command already claimed by another worker"
                    );

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
                        execution_id = ack.notification.execution_id,
                        command_id = %ack.notification.command_id,
                        step = %ack.notification.step,
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
                        execution_id = ack.notification.execution_id,
                        command_id = %ack.notification.command_id,
                        step = %ack.notification.step,
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
