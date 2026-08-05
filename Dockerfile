# Chef stage for dependency analysis
# https://crates.io/crates/cargo-chef/0.1.73
FROM lukemathwalker/cargo-chef:0.1.73-rust-1.91.1-alpine3.22 AS chef
WORKDIR /app
RUN apk update && \
    apk add --no-cache clang lld llvm musl-dev make pkgconfig openssl-dev openssl-libs-static g++ libc-dev

# Planner stage - analyzes dependencies
FROM chef AS planner
COPY . .
# Compute a lock-like file for dependency installation
RUN cargo chef prepare --recipe-path recipe.json

# Builder stage - caches dependencies
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this layer is cached as long as Cargo.toml/Cargo.lock don't change.
# `--features duckdb-integration` keeps the DuckDB engine in the shipped image:
# the worker pool executes `duckdb` / `ducklake` tool steps (noetl-tools gates
# the DuckDB C++ engine behind that non-default feature — noetl/ai-meta#185).
# Cook with the same feature set as the app build so the libduckdb-sys layer is
# cached, not rebuilt in the app stage.
RUN cargo chef cook --release --features duckdb-integration --recipe-path recipe.json

# Build the application.  `ehdb-selfcheck` ships alongside the worker so the
# in-process EHDB integration (noetl/ehdb#234) can be exercised inside the
# deployed image (kind validation + operator preflight); it is not on the
# worker's request path.
COPY . .
# `--features duckdb-integration`: ship the DuckDB engine (see the cook stage +
# noetl/ai-meta#185).  The worker pool runs `duckdb` / `ducklake` steps.
RUN cargo build --release --features duckdb-integration --bin noetl-worker --bin ehdb-selfcheck

# Runtime stage
FROM alpine:3.22.2 AS runtime

WORKDIR /app

# Install necessary runtime dependencies
RUN apk add --no-cache libgcc libxslt ca-certificates openssl python3 py3-pip

# Copy the compiled binaries
COPY --from=builder /app/target/release/noetl-worker ./noetl-worker
COPY --from=builder /app/target/release/ehdb-selfcheck ./ehdb-selfcheck

# Default environment variables
ENV WORKER_POOL_NAME=worker-rust-pool \
    NOETL_SERVER_URL=http://noetl.noetl.svc.cluster.local:8082 \
    NATS_URL=nats://nats.nats.svc.cluster.local:4222 \
    NATS_STREAM=NOETL_COMMANDS \
    NATS_CONSUMER=noetl_worker_pool \
    WORKER_HEARTBEAT_INTERVAL=15 \
    WORKER_MAX_CONCURRENT=4 \
    RUST_LOG=info,worker_pool=debug

ENTRYPOINT ["./noetl-worker"]
