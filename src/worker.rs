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

    /// Pull-model command source — NATS JetStream (default) or the EHDB command
    /// bus (`NOETL_COMMAND_BUS=ehdb`, noetl/ai-meta#194 L1 T4).  Behind a `Mutex`
    /// so `process_commands` (which takes `&self`) can call `next()` (which takes
    /// `&mut self`).
    source: Arc<Mutex<crate::command_bus::WorkerCommandSource>>,

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
        // Create HTTP client
        let client = ControlPlaneClient::new(&config.server_url);

        // noetl/ai-meta#194 L1 T4 — the command-bus transport (default `nats`,
        // path below unchanged). When `ehdb`/`shadow` AND this worker hosts its
        // shard (the system-pool writer), open the shard's command-log writer and
        // spawn its ingest (server publishes) + claim (replicas compete) +
        // `/metrics` (lag) faces.
        let cmdbus = crate::command_bus::CommandBusConfig::from_env();
        if cmdbus.host && cmdbus.mode.hosts_relevant() {
            crate::command_bus::spawn_writer_host(&cmdbus).await?;
        }

        // Select the command source by mode.  `ehdb` claims over the network from
        // the shard coordinator (competing across replicas) and creates NO NATS
        // subscriber (supports the NATS-deleted end state); every other mode is
        // the unchanged NATS durable pull consumer.
        let source = if cmdbus.mode.consumes_ehdb() {
            let claim_addr = cmdbus.claim_addr.ok_or_else(|| {
                anyhow::anyhow!("NOETL_COMMAND_BUS=ehdb requires NOETL_COMMAND_BUS_CLAIM_ADDR")
            })?;
            // This worker's pool segment — the same `NATS_FILTER_SUBJECT`
            // derivation the NATS path uses (default `shared`). The coordinator
            // only ever hands it a command whose `execution_pool` matches, so a
            // system command never reaches a shared worker (noetl/ai-meta#194 #1).
            let pool = crate::nats::segment_from_filter(&config.nats_filter_subject)
                .unwrap_or_else(|| ehdb_feed::DEFAULT_POOL.to_string());
            tracing::info!(%claim_addr, %pool, "worker consuming commands from the EHDB bus");
            crate::command_bus::WorkerCommandSource::Ehdb(Box::new(
                crate::command_bus::EhdbCommandSource::new(
                    claim_addr,
                    pool,
                    config.worker_id.clone(),
                    client.clone(),
                ),
            ))
        } else {
            // Default NATS path.  `nats_subject` is the base for the stream
            // config; `nats_filter_subject` is the consumer-side filter.
            let subscriber = NatsSubscriber::connect(
                &config.nats_url,
                &config.nats_stream,
                &config.nats_consumer,
                &config.nats_subject,
                &config.nats_filter_subject,
            )
            .await?;
            // noetl/ai-meta#166 Phase 4: execution-affinity routing policy (env).
            // Behaviour-neutral by default (single-shard, flag off).
            let affinity = crate::sharding::AffinityConfig::from_env();
            crate::command_bus::WorkerCommandSource::Nats(Box::new(NatsCommandSource::new(
                subscriber,
                client.clone(),
                config.worker_id.clone(),
                crate::nats::segment_from_filter(&config.nats_filter_subject),
                affinity,
            )))
        };
        let source = Arc::new(Mutex::new(source));

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
        // noetl/ai-meta#166 Phase 1: the index now carries a bounded-cache
        // eviction policy resolved from env (TTL / byte-ceiling / max-executions
        // + the slim-chain projection).  Every knob defaults to off (unbounded =
        // today's behaviour), so a worker carrying this code is behaviour-neutral
        // until an operator sets the env vars.
        let state_builder_index: crate::state_builder::SharedWalIndex =
            crate::state_builder::SharedWalIndex::new(
                crate::state_builder::WalEventIndex::with_order_policy(
                    crate::state_builder::spine_order(),
                    crate::state_builder::EvictionPolicy::from_env(),
                ),
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
            config.nats_url.clone(),
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
        // When this worker runs the state materializer (system pool, under
        // NOETL_STATE_SHARD_WRITE), the lag poller also tracks the
        // noetl_state_materializer consumer backlog — the writer-lag health
        // signal (noetl/ai-meta#166 Phase 2 §9 writer-lag risk).
        let state_materializer_lag_target = if crate::state_materializer::enabled() {
            let stream = std::env::var("NOETL_STATE_SHARD_STREAM")
                .unwrap_or_else(|_| crate::materializer::EVENT_STREAM.to_string());
            let consumer = std::env::var("NOETL_STATE_SHARD_CONSUMER").unwrap_or_else(|_| {
                crate::state_materializer::STATE_MATERIALIZER_CONSUMER.to_string()
            });
            Some((stream, consumer))
        } else {
            None
        };
        let lag_handle = crate::nats::lag_poller::spawn(
            self.source.clone(),
            lag_poll_interval,
            materializer_lag_target.clone(),
            state_materializer_lag_target.clone(),
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
        let materializer_handle =
            match crate::materializer::MaterializerConfig::from_env(&self.config) {
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

        // Start the state materializer (noetl/ai-meta#166 Phase 2) when enabled
        // (system worker pool only).  Yet ANOTHER separate noetl_events consumer
        // (noetl_state_materializer, self-ensured) that projects each execution's
        // slim event-chain into a per-execution Feather state shard on object
        // store — SHADOW: nothing reads the shards yet (Phase 3), and it is
        // READ-ONLY w.r.t. noetl.* (object PUT only), so it can neither perturb
        // the drive nor the #103 sole-writer.  Default off (NOETL_STATE_SHARD_WRITE).
        let state_materializer_handle =
            crate::state_materializer::StateMaterializerConfig::from_env(&self.config)
                .map(|cfg| crate::state_materializer::spawn(cfg, self.client.clone()));

        // Off-server state-builder drain (noetl/ai-meta#115 Phase 4): drain the
        // noetl_events WAL into the shared pool-side chain index (system worker
        // pool).  Under NOETL_STATE_BUILDER_SHADOW it's observation-only (exercises
        // the chain-walk + cache, no drive impact); under NOETL_STATE_BUILDER=offserver
        // it's authoritative — a durable consumer feeds the index the orchestrate
        // command dispatch reads to build drive state off the WAL spine. Zero
        // noetl.event scans either way. Default off.
        let state_builder_handle =
            crate::state_builder::DrainConfig::from_env(&self.config.nats_url).map(|cfg| {
                crate::state_builder::spawn_drain(cfg, self.state_builder_index.clone())
            });

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

        // EHDB in-process readiness preflight (noetl/ehdb#234).  Bounded,
        // stateless, and NON-FATAL: EHDB is auxiliary storage, so a degraded or
        // unavailable EHDB must never block worker startup.  When EHDB is
        // disabled (the default) this is a strict no-op that records no metric,
        // so the worker's behaviour + `/metrics` output are byte-identical to a
        // build without EHDB.  Control-plane roles never perform the read.
        crate::ehdb::readiness::run_preflight(&self.config.worker_id);

        // EHDB durable event-log periodic segment-GC (noetl/ehdb#254).  Spawns
        // ONLY when the operator has opted in on every axis (durable_segment
        // backend + GC policy enabled + a positive interval + a data-plane
        // durable contract); otherwise `from_env` returns None and nothing is
        // spawned, so a default worker is byte-identical.  Reclaims each owned
        // shard's consumed sealed segments (local + shared) on a blocking thread,
        // serialized against appends per shard.
        let eventlog_gc_handle = crate::ehdb::eventlog_gc::GcConfig::from_env()
            .map(|cfg| crate::ehdb::eventlog_gc::spawn(cfg, self.config.worker_id.clone()));

        // Dedicated external Flight SQL data-plane endpoint (noetl/ai-meta#184).
        // Off unless NOETL_EHDB_FLIGHT_SQL is truthy AND a data-plane
        // local-reference contract + an auth mode resolve; serves the
        // projection tier read-only to external Flight SQL clients.
        let flight_sql_handle =
            crate::ehdb::flight_sql_endpoint::FlightSqlConfig::from_env().map(|cfg| {
                crate::ehdb::flight_sql_endpoint::spawn(
                    cfg,
                    self.client.clone(),
                    self.config.worker_id.clone(),
                )
            });

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
        if let Some(h) = state_materializer_handle {
            h.abort();
        }
        if let Some(h) = state_builder_handle {
            h.abort();
        }
        if let Some(h) = eventlog_gc_handle {
            h.abort();
        }
        if let Some(h) = flight_sql_handle {
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
        // noetl/ai-meta#163: bounded exponential backoff shared across the loop's
        // in-process NATS reconnects.  Reset to the floor after every healthy pull
        // so a single recovered blip doesn't leave the next reconnect starting from
        // a stretched backoff, but a *flapping* reconnect that never recovers still
        // walks the backoff up to the 10s ceiling.
        let mut reconnect_backoff = crate::state_builder::REBUILD_BACKOFF_MIN;

        loop {
            // Wait for available slot
            let permit = self.semaphore.clone().acquire_owned().await?;

            // Pull one item from the source.  The Mutex is held only
            // for the duration of `next()` + the corresponding ack /
            // nack; dispatch happens after the lock is released.
            //
            // noetl/ai-meta#163: a pull error is NO LONGER propagated (which used
            // to `exit(1)` the worker and rely on a k8s crash-restart + full WAL
            // replay).  A hard NATS disconnect (e.g. `nats-0` pod delete) is
            // detected here and healed in-process by rebuilding the subscriber with
            // backoff; a non-disconnect blip takes a brief backoff and retries.
            // Either way the loop keeps running — the durable consumer's server-side
            // cursor is untouched, so nothing is replayed or skipped.
            let pulled = {
                let mut source = self.source.lock().await;
                source.next().await
            };
            let pulled = match pulled {
                Ok(p) => {
                    // Healthy pull → the connection is good; reset the backoff.
                    reconnect_backoff = crate::state_builder::REBUILD_BACKOFF_MIN;
                    p
                }
                Err(e) => {
                    drop(permit);
                    self.on_loop_error("pull", &e, &mut reconnect_backoff).await;
                    continue;
                }
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

                    // Ack the source handle now that we own the command; dispatch
                    // happens off the pull loop.  noetl/ai-meta#163: the claim (in
                    // `next()`) is the execution commitment, not the NATS ack — so
                    // even if the ack fails (connection dropped between claim and
                    // ack) we STILL dispatch the claimed command below, then heal
                    // the connection.  A failed ack leaves the notification
                    // unacked → JetStream redelivers it → re-claim returns
                    // `AlreadyClaimed` → ack + skip (no double execution; the DB
                    // claim is the exactly-once gate).
                    let ack_err = {
                        let source = self.source.lock().await;
                        source.ack(ack).await.err()
                    };

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

                    // The claimed command is now dispatching regardless; if its ack
                    // failed the connection is suspect — heal it in-process
                    // (noetl/ai-meta#163) so the next pull works.
                    if let Some(e) = ack_err {
                        self.on_loop_error("ack", &e, &mut reconnect_backoff).await;
                    }
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
                    let ack_err = {
                        let source = self.source.lock().await;
                        source.ack(ack).await.err()
                    };

                    // Release permit immediately
                    drop(permit);

                    // noetl/ai-meta#163: a failed ack here just leaves the
                    // notification to redeliver (harmless — it re-claims to
                    // `AlreadyClaimed` again); heal the connection so pulls resume.
                    if let Some(e) = ack_err {
                        self.on_loop_error("ack", &e, &mut reconnect_backoff).await;
                    }
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
                    let nack_err = {
                        let source = self.source.lock().await;
                        source.nack(ack).await.err()
                    };
                    drop(permit);
                    if let Some(e) = nack_err {
                        self.on_loop_error("nack", &e, &mut reconnect_backoff).await;
                    }
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
                    let nack_err = {
                        let source = self.source.lock().await;
                        source.nack(ack).await.err()
                    };
                    drop(permit);
                    if let Some(e) = nack_err {
                        self.on_loop_error("nack", &e, &mut reconnect_backoff).await;
                    }
                }
            }
        }
    }

    /// React to an error on the command loop's NATS path (noetl/ai-meta#163).
    ///
    /// `op` is the operation that failed (`pull` / `ack` / `nack`) — used as the
    /// reconnect metric's `reason` label.  A hard-disconnect-class error rebuilds
    /// the subscriber in-process with bounded backoff (recovering from a `nats-0`
    /// bounce without a pod restart); a non-disconnect transient error takes a
    /// brief backoff and lets the caller retry (the durable consumer redelivers
    /// any unacked notification, so nothing is lost).  Never returns an error —
    /// the loop must not `exit(1)` on a NATS blip.
    async fn on_loop_error<E: std::fmt::Display>(
        &self,
        op: &str,
        err: &E,
        backoff: &mut std::time::Duration,
    ) {
        match classify_loop_error(err) {
            LoopAction::Reconnect => {
                tracing::warn!(
                    op,
                    error = %err,
                    "command-loop NATS disconnect; rebuilding subscriber in-process (noetl/ai-meta#163)"
                );
                self.reconnect_command_source(op, backoff).await;
            }
            LoopAction::Backoff => {
                // Not a disconnect (e.g. a transient control-plane claim blip).
                // Don't churn the NATS connection; brief backoff and retry.
                tracing::warn!(
                    op,
                    error = %err,
                    "command-loop transient error (non-disconnect); backing off then retrying"
                );
                tokio::time::sleep(crate::state_builder::REBUILD_BACKOFF_MIN).await;
            }
        }
    }

    /// Rebuild the NATS command subscriber in-process with bounded exponential
    /// backoff and swap it into the source (noetl/ai-meta#163).  Loops until a
    /// fresh connect + durable-consumer bind succeeds — a permanently-down NATS
    /// keeps the loop retrying (as the pre-#163 `exit(1)` + crash-restart also
    /// couldn't make progress against a down NATS, but without the full-WAL-replay
    /// outage on every attempt).  The rebuilt subscriber re-binds the SAME durable
    /// consumer by name, so its server-side cursor is unchanged — no command is
    /// replayed or skipped across the reconnect.
    async fn reconnect_command_source(&self, reason: &str, backoff: &mut std::time::Duration) {
        crate::metrics::record_command_loop_reconnect(reason);
        loop {
            match NatsSubscriber::connect(
                &self.config.nats_url,
                &self.config.nats_stream,
                &self.config.nats_consumer,
                &self.config.nats_subject,
                &self.config.nats_filter_subject,
            )
            .await
            {
                Ok(subscriber) => {
                    self.source.lock().await.replace_subscriber(subscriber);
                    tracing::info!(
                        reason,
                        "command-loop NATS reconnected in-process; resuming consume (noetl/ai-meta#163)"
                    );
                    *backoff = crate::state_builder::REBUILD_BACKOFF_MIN;
                    return;
                }
                Err(e) => {
                    crate::metrics::record_command_loop_reconnect("connect_error");
                    tracing::warn!(
                        error = %e,
                        backoff_ms = backoff.as_millis() as u64,
                        reason,
                        "command-loop NATS reconnect failed; backing off then retrying (noetl/ai-meta#163)"
                    );
                    tokio::time::sleep(*backoff).await;
                    *backoff = (*backoff * 2).min(crate::state_builder::REBUILD_BACKOFF_MAX);
                }
            }
        }
    }
}

/// Control-loop reaction to a NATS-path error (noetl/ai-meta#163).
#[derive(Debug, PartialEq, Eq)]
enum LoopAction {
    /// Hard disconnect — rebuild the subscriber in-process with backoff.
    Reconnect,
    /// Transient non-disconnect blip — brief backoff, then retry (no rebuild).
    Backoff,
}

/// True when a command-loop pull/ack/nack error indicates the NATS connection or
/// consumer is gone (hard disconnect: `nats-0` pod delete, connection reset,
/// orphaned consumer) rather than a transient application-level failure (e.g. a
/// control-plane HTTP claim blip).  Reuses the state-builder's consumer-dead
/// signatures (noetl/ai-meta#161) and adds the subscriber's own NATS-layer error
/// wrappers plus raw connection-loss signatures a full client disconnect surfaces.
///
/// Deliberately does NOT match bare control-plane HTTP failures ("error sending
/// request", "connection refused" to the server) so a server-side outage does not
/// trigger a NATS reconnect storm — those take the brief-backoff path and let the
/// durable consumer redeliver.
fn is_nats_disconnect<E: std::fmt::Display>(err: &E) -> bool {
    if crate::state_builder::is_consumer_dead(err) {
        return true;
    }
    let s = err.to_string().to_ascii_lowercase();
    // The subscriber wraps every NATS-layer failure with these prefixes; a hard
    // disconnect on the pull / receive / ack / nack path surfaces here.
    s.contains("failed to pull command")
        || s.contains("failed to receive message")
        || s.contains("failed to ack message")
        || s.contains("failed to nack")
        // Raw connection-loss signatures (belt-and-braces for errors that reach us
        // unwrapped).
        || s.contains("connection reset")
        || s.contains("connection closed")
        || s.contains("connection lost")
        || s.contains("connection aborted")
        || s.contains("broken pipe")
        || s.contains("disconnected")
}

/// Decide how the command loop should react to a NATS-path error
/// (noetl/ai-meta#163): rebuild the subscriber on a hard disconnect, else a brief
/// backoff-and-retry.  Pure so the reconnect decision is unit-tested directly.
fn classify_loop_error<E: std::fmt::Display>(err: &E) -> LoopAction {
    if is_nats_disconnect(err) {
        LoopAction::Reconnect
    } else {
        LoopAction::Backoff
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

    // --- noetl/ai-meta#163: main command-loop in-process NATS reconnect ---

    #[test]
    fn is_nats_disconnect_matches_hard_disconnect_signatures() {
        // Subscriber's own NATS-layer error wrappers (the strings that actually
        // reach the loop through `next()` / `ack()` / `nack()`).
        assert!(is_nats_disconnect(
            &"Failed to pull command: connection reset by peer"
        ));
        assert!(is_nats_disconnect(
            &"Failed to receive message: broken pipe"
        ));
        assert!(is_nats_disconnect(&"Failed to ack message: disconnected"));
        assert!(is_nats_disconnect(&"Failed to nack message: timed out"));
        // Consumer-dead signatures shared with the state-builder self-heal (#161).
        assert!(is_nats_disconnect(
            &"503 no responders available for request"
        ));
        assert!(is_nats_disconnect(&"consumer not found"));
        assert!(is_nats_disconnect(&"consumer deleted"));
        // Raw connection-loss signatures.
        assert!(is_nats_disconnect(&"connection closed"));
        assert!(is_nats_disconnect(&"connection aborted"));
    }

    #[test]
    fn is_nats_disconnect_ignores_control_plane_http_failures() {
        // A control-plane (server) HTTP outage must NOT trigger a NATS reconnect
        // storm — these take the brief-backoff path so the durable consumer just
        // redelivers.  (reqwest's connect-refused shape names the SERVER, not NATS.)
        assert!(!is_nats_disconnect(
            &"error sending request for url (http://noetl-server:8082/api/commands/claim): error trying to connect: tcp connect error: Connection refused (os error 111)"
        ));
        assert!(!is_nats_disconnect(&"500 Internal Server Error"));
        assert!(!is_nats_disconnect(&"claim rejected: catalog mismatch"));
    }

    #[test]
    fn classify_loop_error_reconnects_on_disconnect_backs_off_otherwise() {
        assert_eq!(
            classify_loop_error(&"Failed to pull command: broken pipe"),
            LoopAction::Reconnect
        );
        assert_eq!(
            classify_loop_error(&"500 Internal Server Error"),
            LoopAction::Backoff
        );
    }

    #[test]
    fn reconnect_backoff_doubles_with_ceiling() {
        // Mirrors the doubling in `reconnect_command_source`: floor → double per
        // failed attempt → clamp at the 10s ceiling (never regressing the cursor
        // or hammering a still-down NATS).
        let mut b = crate::state_builder::REBUILD_BACKOFF_MIN;
        assert_eq!(b, std::time::Duration::from_millis(250));
        for _ in 0..10 {
            b = (b * 2).min(crate::state_builder::REBUILD_BACKOFF_MAX);
        }
        assert_eq!(b, crate::state_builder::REBUILD_BACKOFF_MAX);
        assert_eq!(b, std::time::Duration::from_secs(10));
    }

    // A scriptable in-memory `CommandSource` that can inject a hard-disconnect
    // error mid-stream, then (once "reconnected") resume yielding commands — the
    // unit-level simulation of a `nats-0` bounce.  Its ack log lets the test
    // assert every command is acked exactly once and in order (cursor intact, no
    // duplicate or lost command across the reconnect).
    enum Scripted {
        Yield(ClaimOutcome),
        Disconnect,
        End,
    }

    #[derive(Clone)]
    struct MockAck {
        command_id: String,
    }

    struct FlakySource {
        script: std::collections::VecDeque<Scripted>,
        acked: Arc<Mutex<Vec<String>>>,
        nacked: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl CommandSource for FlakySource {
        type AckHandle = MockAck;

        async fn next(&mut self) -> Result<Option<Pulled<MockAck>>> {
            match self.script.pop_front() {
                None | Some(Scripted::End) => Ok(None),
                Some(Scripted::Disconnect) => {
                    // The exact shape a hard `nats-0` delete surfaces through the
                    // subscriber's pull wrapper.
                    Err(anyhow::anyhow!(
                        "Failed to pull command: connection reset by peer"
                    ))
                }
                Some(Scripted::Yield(outcome)) => {
                    let command_id = match &outcome {
                        ClaimOutcome::Claimed(c) => c.command_id.clone(),
                        _ => "n/a".to_string(),
                    };
                    Ok(Some(Pulled {
                        outcome,
                        ack: MockAck { command_id },
                    }))
                }
            }
        }

        async fn ack(&self, handle: MockAck) -> Result<()> {
            self.acked.lock().await.push(handle.command_id);
            Ok(())
        }

        async fn nack(&self, handle: MockAck) -> Result<()> {
            self.nacked.lock().await.push(handle.command_id);
            Ok(())
        }
    }

    fn claimed(id: &str) -> ClaimOutcome {
        ClaimOutcome::Claimed(noetl_executor::worker::source::Command {
            command_id: id.to_string(),
            execution_id: 1,
            step: "s".to_string(),
            tool_kind: "rhai".to_string(),
            input: serde_json::Value::Null,
            render_context: Default::default(),
            attempts: 0,
        })
    }

    /// Faithful model of `process_commands`' reconnect control flow (it shares the
    /// real `classify_loop_error` decision fn): pull → on a disconnect error,
    /// classify + "reconnect" (here: bump a counter; the script models the healed
    /// stream) without propagating → on a claimed command, dispatch + ack.  Proves
    /// the loop SURVIVES a hard disconnect (returns Ok, never `exit(1)`), fires the
    /// reconnect exactly once, and processes/acks every command exactly once and in
    /// order across the reconnect (no cursor regression, no dup/lost command).
    async fn drive_flaky(source: FlakySource) -> (Vec<String>, Vec<String>, u32) {
        let acked = source.acked.clone();
        let nacked = source.nacked.clone();
        let source = Arc::new(Mutex::new(source));
        let mut processed = Vec::new();
        let mut reconnects = 0u32;
        let mut backoff = crate::state_builder::REBUILD_BACKOFF_MIN;

        loop {
            let pulled = {
                let mut s = source.lock().await;
                s.next().await
            };
            let pulled = match pulled {
                Ok(p) => {
                    backoff = crate::state_builder::REBUILD_BACKOFF_MIN;
                    p
                }
                Err(e) => {
                    // The load-bearing assertion: a hard disconnect is NOT
                    // propagated (no `?`, no `exit(1)`) — it is classified and healed.
                    match classify_loop_error(&e) {
                        LoopAction::Reconnect => {
                            reconnects += 1;
                            // Model the in-process rebuild's backoff bump.
                            backoff = (backoff * 2).min(crate::state_builder::REBUILD_BACKOFF_MAX);
                        }
                        LoopAction::Backoff => {}
                    }
                    continue;
                }
            };
            let Some(Pulled { outcome, ack }) = pulled else {
                break; // drained
            };
            match outcome {
                ClaimOutcome::Claimed(cmd) => {
                    processed.push(cmd.command_id.clone());
                    source.lock().await.ack(ack).await.unwrap();
                }
                ClaimOutcome::AlreadyClaimed => {
                    source.lock().await.ack(ack).await.unwrap();
                }
                ClaimOutcome::RetryLater(_) | ClaimOutcome::Failed(_) => {
                    source.lock().await.nack(ack).await.unwrap();
                }
            }
        }

        let acked = acked.lock().await.clone();
        let _ = nacked; // (unused in the claimed-only script)
        (processed, acked, reconnects)
    }

    #[tokio::test]
    async fn reconnect_survives_hard_disconnect_and_preserves_cursor() {
        // Two commands, a hard disconnect, then two more — the unit-level shape of
        // a `nats-0` pod delete mid-stream.
        let source = FlakySource {
            script: vec![
                Scripted::Yield(claimed("c1")),
                Scripted::Yield(claimed("c2")),
                Scripted::Disconnect,
                Scripted::Yield(claimed("c3")),
                Scripted::Yield(claimed("c4")),
                Scripted::End,
            ]
            .into(),
            acked: Arc::new(Mutex::new(Vec::new())),
            nacked: Arc::new(Mutex::new(Vec::new())),
        };

        let (processed, acked, reconnects) = drive_flaky(source).await;

        // Survived the disconnect in-process (drive returned; no exit(1)).
        assert_eq!(reconnects, 1, "exactly one in-process reconnect fired");
        // Every command processed exactly once, in order — no cursor regression,
        // no command lost or replayed across the reconnect boundary.
        assert_eq!(processed, vec!["c1", "c2", "c3", "c4"]);
        // Every processed command acked exactly once (no duplicate acks).
        assert_eq!(acked, vec!["c1", "c2", "c3", "c4"]);
    }
}
