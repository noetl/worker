# Plug-in test fixtures

`reference_materializer.wasm` — the hand-written Rust reference plug-in
(`plugins/reference-materializer`) compiled to `wasm32-unknown-unknown`.
Rebuild with:

    cd plugins/reference-materializer
    cargo build --release --target wasm32-unknown-unknown
    cp target/wasm32-unknown-unknown/release/noetl_reference_materializer.wasm \
       ../../tests/fixtures/reference_materializer.wasm

Exercised by `plugin::tests::loads_and_runs_a_real_rust_compiled_plugin`
(noetl/ai-meta#105 Round 5) — proves a real compiled plug-in (not just WAT)
runs on the host through the data-plane ABI + the `noetl.object_put` capability.
