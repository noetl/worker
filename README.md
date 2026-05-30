# NoETL Worker (`noetl-worker`)

Worker executable that consumes command events and executes tools.

## Distribution Channels

- **Crates.io**: `noetl-worker`
- **Container image**: recommended primary runtime channel (GHCR/GCR)
- **Cloud Build**: recommended for image builds and GKE deploy pipelines

## Dependency Policy

- Crates.io dependencies for shared libraries:
  - `noetl-tools = "2.8.7"` (or matching release version) — the
    tool registry (HTTP, DuckDB, Postgres, shell, Rhai, …).
  - `noetl-executor = "0.2"` — the shared execution core
    (R-1.2 PR-2c onwards).  Hosts the structured-condition
    surface (`Condition`, `Operator`,
    `evaluate_structured_condition`) the worker's case dispatcher
    delegates to; ships in the [`noetl/cli` workspace][cli] as a
    workspace-member crate.
- Release order must publish `noetl-tools` and `noetl-executor`
  before `noetl-worker`.

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
