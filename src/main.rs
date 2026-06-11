//! NoETL Worker Pool binary.
//!
//! Runs a worker that receives commands via NATS and executes tools.

use anyhow::Result;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use noetl_worker::{SubscriptionRuntime, Worker, WorkerConfig};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,worker_pool=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load environment variables
    dotenvy::dotenv().ok();

    // Load configuration
    let config = WorkerConfig::from_env()?;

    // Handle shutdown signals
    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C handler");
        tracing::info!("Shutdown signal received");
    };

    // Run-mode selection (noetl/ai-meta#90 Phase 2).  `WORKER_MODE=subscription`
    // turns the binary into the continuous subscription runtime (Mode B);
    // anything else (default) is the ordinary command-pull worker pool.  Both
    // are the same binary with distinct configuration — the system-pool
    // philosophy, no new compiled artifact.
    let mode = std::env::var("WORKER_MODE").unwrap_or_else(|_| "command".to_string());
    if mode == "subscription" {
        tracing::info!(
            worker_id = %config.worker_id,
            server_url = %config.server_url,
            subscription_path = %std::env::var("NOETL_SUBSCRIPTION_PATH").unwrap_or_default(),
            "Starting NoETL Subscription Runtime (Mode B)"
        );
        let runtime = SubscriptionRuntime::new(&config)?;
        if let Err(e) = runtime.run(shutdown).await {
            tracing::error!(error = %e, "Subscription runtime error");
            return Err(e);
        }
        tracing::info!("Subscription runtime stopped");
        return Ok(());
    }

    tracing::info!(
        worker_id = %config.worker_id,
        pool_name = %config.pool_name,
        server_url = %config.server_url,
        "Starting NoETL Worker Pool"
    );

    // Create and run worker
    let worker = Worker::new(config).await?;

    tokio::select! {
        result = worker.run() => {
            if let Err(e) = result {
                tracing::error!(error = %e, "Worker error");
                return Err(e);
            }
        }
        _ = shutdown => {
            tracing::info!("Shutting down worker");
        }
    }

    tracing::info!("Worker stopped");
    Ok(())
}
