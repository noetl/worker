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

    /// Shared pool-side WAL event index (RFC #115 Phase 4).  The drain loop
    /// ([`crate::state_builder::spawn_drain`]) feeds it from the `noetl_events`
    /// WAL; the executor reads it to build off-server drive state under
    /// `NOETL_STATE_BUILDER=offserver`.  Always constructed (cheap, empty); the
    /// drain loop spawns only when the builder is enabled.
    state_builder_index: crate::state_builder::SharedWalIndex,
}

impl Worker {
    /// Create a new worker.
    pub async fn new(config: WorkerConfig) -> Result<Self> {
        // Connect to NATS.  `nats_subject` is the base for the
        // stream config; `nats_filter_subject` is the consumer-side
        // filter — defaults to the bare subject in `WorkerConfig`
        // unless `NATS_FILTER_SUBJECT` is set by the deployment env
        // (PR-4 of noetl/ai-meta#42 ships the manifest change).
        let subscriber = NatsSubscriber::connect(
            &config.nats_url,
            &config.nats_stream,
            &config.nats_consumer,
            &config.nats_subject,
            &config.nats_filter_subject,
        )
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
            crate::nats::segment_from_filter(&config.nats_filter_subject),
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

        // Shared pool-side WAL event index (RFC #115 Phase 4).  Built here so
        // the executor and the drain loop share one index; the drain loop
        // (spawned in `run` when the builder is enabled) feeds it from the WAL.
        // The spine ordering (noetl/ai-meta#117) is resolved from env once: causal
        // (`prev_event_id` chain) order by default, so fan-in survives an
        // `event_id`-vs-chain inversion under high-concurrency fan-out.
        let state_builder_index: crate::state_builder::SharedWalIndex =
            crate::state_builder::SharedWalIndex::new(
                crate::state_builder::WalEventIndex::with_order(crate::state_builder::spine_order()),
            );

        // Create executor.  Under `NOETL_STATE_BUILDER=offserver` it builds the
        // orchestrate drive's state from `state_builder_index` (the WAL spine)
        // instead of the server-built `run_state` payload.
        let executor = Arc::new(CommandExecutor::new(
            client.clone(),
            config.worker_id.clone(),
            config.server_url.clone(),
            snowflake.clone(),
            arrow_cache.clone(),
            crate::state_builder::builder_mode(),
            state_builder_index.clone(),
        ));

        // Create semaphore for concurrency control
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_tasks));

        Ok(Self {
            config,
            source,
            client,
            executor,
            semaphore,
            state_builder_index,
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
        // When this worker runs the materializer (system pool, under
        // NOETL_MATERIALIZER_ENABLED), the lag poller also tracks the
        // noetl_events/noetl_materializer consumer backlog — the
        // earliest signal that events are piling up un-materialized
        // under the PUBLISH_ONLY gate (noetl/ai-meta#103 flip
        // guardrail).  An independent task is what catches a stalled
        // or dead materializer loop, which can't report its own lag.
        let materializer_lag_target = if crate::materializer::enabled() {
            let stream = std::env::var("NOETL_MATERIALIZER_STREAM")
                .unwrap_or_else(|_| crate::materializer::EVENT_STREAM.to_string());
            let consumer = std::env::var("NOETL_MATERIALIZER_CONSUMER")
                .unwrap_or_else(|_| crate::materializer::MATERIALIZER_CONSUMER.to_string());
            Some((stream, consumer))
        } else {
            None
        };
        let lag_handle = crate::nats::lag_poller::spawn(
            self.source.clone(),
            lag_poll_interval,
            materializer_lag_target.clone(),
        );
        tracing::info!(
            interval_secs = lag_poll_interval.as_secs(),
            materializer_lag = ?materializer_lag_target,
            "NATS consumer-lag poller started"
        );

        // Start the CQRS event materializer (noetl/ai-meta#103) when enabled
        // (system worker pool only).  It drains noetl_events with deferred ack
        // and is the sole noetl.event writer under PUBLISH_ONLY — acking each
        // batch only after events/project durably inserts it, so a transient
        // failure redelivers instead of losing events.  Default off.
        let materializer_handle = match crate::materializer::MaterializerConfig::from_env(&self.config)
        {
            Ok(Some(cfg)) => Some(crate::materializer::spawn(cfg)),
            Ok(None) => None,
            Err(e) => {
                // Enabled-but-misconfigured: fail loud rather than silently
                // not materializing under the sole-writer gate.
                return Err(e);
            }
        };

        // Start the result materializer (noetl/ai-meta#104 Phase B/D) when
        // enabled (system worker pool only).  A SEPARATE noetl_events consumer
        // (noetl_result_materializer) writes the over-budget Feather/JSON result
        // tier to object store at the derived §7 key — isolated from the event
        // materialize path so object-store latency never back-pressures the audit
        // fold.  Under NOETL_RESULT_MATERIALIZER_ENABLED it is the Phase B shadow
        // copy; under NOETL_RESULT_MINT_AUTHORITATIVE (Phase D) it is the
        // authoritative tier writer (the consume path resolves from it, with the
        // dual-written result_store as the reversible fallback).  Default off.
        let result_materializer_handle =
            crate::result_materializer::ResultMaterializerConfig::from_env(&self.config)
                .map(|cfg| crate::result_materializer::spawn(cfg, self.client.clone()));

        // Off-server state-builder drain (noetl/ai-meta#115 Phase 4): drain the
        // noetl_events WAL into the shared pool-side chain index (system worker
        // pool).  Under NOETL_STATE_BUILDER_SHADOW it's observation-only (exercises
        // the chain-walk + cache, no drive impact); under NOETL_STATE_BUILDER=offserver
        // it's authoritative — a durable consumer feeds the index the orchestrate
        // command dispatch reads to build drive state off the WAL spine. Zero
        // noetl.event scans either way. Default off.
        let state_builder_handle =
            crate::state_builder::DrainConfig::from_env(&self.config.nats_url)
                .map(|cfg| crate::state_builder::spawn_drain(cfg, self.state_builder_index.clone()));

        // Boot warmup of the off-server orchestrate drive plug-in
        // (noetl/ai-meta#130 cold-start).  The first orchestrate hop otherwise
        // pays a one-time Cranelift compile of the ~1.6MB `system/orchestrate`
        // module on the critical path (~2.7s observed on a constrained kind
        // node).  Compile it once here — overlapping the state-builder drain
        // rehydrate above — so the first real drive is a cache hit.  Gated to
        // the drive pool (system pool) by default; NOETL_WARM_ORCHESTRATE_PLUGIN
        // forces on/off.  The warmup completes BEFORE process_commands starts
        // claiming, so the first claimed orchestrate command finds a warm cache
        // even independent of the /readyz gate.
        if warm_orchestrate_enabled() {
            self.executor.warm_orchestrate_plugin().await;
        } else {
            crate::metrics::record_plugin_warm("skipped");
        }
        // Mark ready regardless of warm outcome — the worker is functional even
        // cold; the readiness gate exists to hide the warm latency from a
        // rollout, not to fail-closed when a warm misses (e.g. server briefly
        // unreachable at boot).
        crate::metrics::set_worker_ready(true);

        // Process commands
        let result = self.process_commands().await;

        // Stop heartbeat + metrics server + lag poller + materializer
        heartbeat_handle.abort();
        metrics_handle.abort();
        lag_handle.abort();
        if let Some(h) = materializer_handle {
            h.abort();
        }
        if let Some(h) = result_materializer_handle {
            h.abort();
        }
        if let Some(h) = state_builder_handle {
            h.abort();
        }

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

                    // noetl/ai-meta#53 Gap 1: extract the publishing
                    // server's URL from the NATS notification BEFORE
                    // we hand the ack back to the source (which owns
                    // the handle).  The dispatch task uses this to
                    // route lifecycle events back to the server that
                    // published the command, not the global env-var
                    // server URL.  Empty string (which the
                    // notification never carries in practice) falls
                    // back to the captured client.
                    let dispatch_server_url = ack.notification.server_url.clone();

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

                        let override_url = if dispatch_server_url.is_empty() {
                            None
                        } else {
                            Some(dispatch_server_url.as_str())
                        };
                        if let Err(e) = executor
                            .execute_with_server_url(&command, override_url)
                            .await
                        {
                            // Per `observability.md` Principle 4:
                            // structured execution_id on every
                            // ERROR.  Includes `step` for the
                            // playbook-level correlation when
                            // looking at a single execution's
                            // trace.
                            //
                            // Invariant (noetl/ai-meta#78): by the time
                            // `execute_with_server_url` returns `Err`,
                            // it has ALREADY emitted the step's terminal
                            // events for every terminal failure —
                            // tool-execution errors (the post-dispatch
                            // error arm) and pre-dispatch errors
                            // (credential-alias 404, malformed tool
                            // config, etc.) both emit `call.error` +
                            // `command.failed` before returning.  The
                            // only `Err` that reaches here WITHOUT a
                            // terminal event is a transient (retryable)
                            // pre-dispatch failure that hasn't exhausted
                            // its attempt counter — that is deliberate,
                            // so the command path's retry/redelivery can
                            // still complete the step.  Therefore this
                            // arm must NOT emit a blanket terminal event
                            // as a "safety net": doing so would
                            // double-emit terminals for the terminal
                            // case and defeat the retry for the
                            // transient case.  It logs only.
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

/// Whether this worker should boot-warm the orchestrate drive plug-in
/// (noetl/ai-meta#130).  `NOETL_WARM_ORCHESTRATE_PLUGIN` forces the decision
/// (`1`/`true`/`yes`/`on` vs `0`/`false`/`no`/`off`); unset, it defaults to the
/// pool that actually drives orchestrate — the system pool, identified by
/// running the off-server state-builder drain (`NOETL_STATE_BUILDER` set) or the
/// CQRS materializer (`NOETL_MATERIALIZER_ENABLED`).  A leaf pool that only runs
/// tools never receives `system/orchestrate`, so warming there would waste
/// startup time + memory.
fn warm_orchestrate_enabled() -> bool {
    match std::env::var("NOETL_WARM_ORCHESTRATE_PLUGIN") {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => {
            crate::state_builder::builder_mode() != crate::state_builder::BuilderMode::Off
                || crate::materializer::enabled()
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

    #[test]
    fn warm_orchestrate_env_override_wins() {
        // Explicit override is honored regardless of pool role.
        std::env::set_var("NOETL_WARM_ORCHESTRATE_PLUGIN", "1");
        assert!(warm_orchestrate_enabled());
        std::env::set_var("NOETL_WARM_ORCHESTRATE_PLUGIN", "off");
        assert!(!warm_orchestrate_enabled());
        std::env::remove_var("NOETL_WARM_ORCHESTRATE_PLUGIN");
    }
}
