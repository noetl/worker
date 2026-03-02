# NoETL Worker (`noetl-worker`)

Worker executable that consumes command events and executes tools.

## Distribution Channels

- **Crates.io**: `noetl-worker`
- **Container image**: recommended primary runtime channel (GHCR/GCR)
- **Cloud Build**: recommended for image builds and GKE deploy pipelines

## Dependency Policy

- Crates.io dependency for shared library:
  - `noetl-tools = "2.8.7"` (or matching release version)
- Release order must publish `noetl-tools` before `noetl-worker`.

## Release Checklist

1. Ensure `noetl-tools` target version is available on crates.io.
2. Bump `version` in `Cargo.toml`.
3. Build and verify:
   - `cargo build --release`
4. Publish crate:
   - `cargo publish`
5. Build and push container image (`worker`).
6. Roll out worker deployment and validate command throughput.
