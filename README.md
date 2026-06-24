# NoETL Worker (`noetl-worker`)

Worker executable that consumes command events and executes tools.

## Distribution Channels

- **Crates.io**: `noetl-worker`
- **Container image**: recommended primary runtime channel (GHCR/GCR)
- **Cloud Build**: recommended for image builds and GKE deploy pipelines

## Dependency Policy

- Crates.io dependencies for shared libraries:
  - `noetl-tools = "2.16"` (or matching release version) — the
    tool registry (HTTP, DuckDB, Postgres, shell, Rhai, nats, mcp, …).
  - `noetl-executor = "0.3"` — the shared execution core
    (R-1.2 PR-2c onwards).  Hosts the structured-condition
    surface (`Condition`, `Operator`,
    `evaluate_structured_condition`) and the `CommandSource`
    trait the worker's NATS source implements; ships in the
    [`noetl/cli` workspace][cli] as a workspace-member crate.
- Observability deps (R-1.2 PR-2e):
  - `prometheus = "0.14"` + `axum = "0.8"` for the `/metrics`
    endpoint per [`agents/rules/observability.md`][obs] Principle 2.
- Release order must publish `noetl-tools` and `noetl-executor`
  before `noetl-worker`.

## Observability

`noetl-worker` exposes Prometheus metrics on a dedicated port
(default `0.0.0.0:9090`, configurable via `WORKER_METRICS_BIND`).

### Endpoints

- `GET /metrics` — Prometheus text-format snapshot.
- `GET /healthz` — liveness check.

### Metric inventory

| Metric | Type | Labels | Purpose |
| :---- | :---- | :---- | :---- |
| `noetl_worker_pulls_total` | counter | `outcome` | Pull rate + outcome distribution |
| `noetl_worker_pull_duration_seconds` | histogram | — | NATS pull + claim latency |
| `noetl_worker_dispatch_duration_seconds` | histogram | `tool_kind` | Per-tool-kind dispatch latency |
| `noetl_worker_dispatch_errors_total` | counter | `tool_kind` | Per-tool failure rate |
| `noetl_worker_event_emit_duration_seconds` | histogram | `event_type` | Event-log write latency |
| `noetl_worker_event_emit_retries_total` | counter | `event_type` | Retry rate |
| `noetl_worker_concurrent_dispatches` | gauge | — | Live in-flight dispatches |

NATS consumer lag is a planned follow-up — requires a periodic
poll against the JetStream consumer info API.

Under `NOETL_STATE_BUILDER=offserver` the drive is event-signalled
and releases the WAL index lock per applied message, so per-hop
transition latency stays sub-second instead of stalling on the
idle-stream WAL drain (see noetl/ai-meta#130).

## Release Checklist

1. Ensure `noetl-tools` + `noetl-executor` target versions are
   available on crates.io.
2. Bump `version` in `Cargo.toml`.
3. Build and verify:
   - `cargo build --release`
4. Publish crate:
   - `cargo publish`
5. Build and push container image (`worker`).
6. Roll out worker deployment and validate command throughput.

[cli]: https://github.com/noetl/cli
[obs]: https://github.com/noetl/ai-meta/blob/main/agents/rules/observability.md
