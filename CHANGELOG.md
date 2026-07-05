# Changelog

All notable changes to this project will be documented in this file.

## [5.57.0](https://github.com/noetl/worker/compare/v5.56.0...v5.57.0) (2026-07-05)

### Features

* **ehdb:** disabled-by-default projection read-model SHADOW wiring (Phase 7) ([#157](https://github.com/noetl/worker/issues/157)) ([eadc3a5](https://github.com/noetl/worker/commit/eadc3a5bd995f3d5f0aa7c38b960434dd12cb84b)), closes [noetl/ehdb#241](https://github.com/noetl/ehdb/issues/241) [ehdb#243](https://github.com/noetl/ehdb/issues/243) [ehdb#243](https://github.com/noetl/ehdb/issues/243) [#243](https://github.com/noetl/worker/issues/243) [noetl/ehdb#241](https://github.com/noetl/ehdb/issues/241)

## [5.56.0](https://github.com/noetl/worker/compare/v5.55.0...v5.56.0) (2026-07-05)

### Features

* **ehdb:** disabled-by-default event-log SHADOW wiring (Phase 6) ([#156](https://github.com/noetl/worker/issues/156)) ([43c8f0f](https://github.com/noetl/worker/commit/43c8f0f733e76de4ea3364e28c90054719d844d8)), closes [noetl/ehdb#242](https://github.com/noetl/ehdb/issues/242) [noetl/ehdb#241](https://github.com/noetl/ehdb/issues/241)

## [5.55.0](https://github.com/noetl/worker/compare/v5.54.0...v5.55.0) (2026-07-05)

### Features

* **ehdb:** in-process bounded RAG retrieval (Phase E) ([#234](https://github.com/noetl/worker/issues/234)) ([#155](https://github.com/noetl/worker/issues/155)) ([d1ebaf2](https://github.com/noetl/worker/commit/d1ebaf262bd7bf41fa86d8420c235491cd8dc828)), closes [noetl/ehdb#240](https://github.com/noetl/ehdb/issues/240)

## [5.54.0](https://github.com/noetl/worker/compare/v5.53.0...v5.54.0) (2026-07-05)

### Features

* **ehdb:** in-process system WASM library store (Phase E) ([#154](https://github.com/noetl/worker/issues/154)) ([3162b1d](https://github.com/noetl/worker/commit/3162b1d88659eab99e371c98bd3c03e3908731e5)), closes [noetl/ehdb#239](https://github.com/noetl/ehdb/issues/239) [noetl/ehdb#234](https://github.com/noetl/ehdb/issues/234) [noetl/ehdb#234](https://github.com/noetl/ehdb/issues/234) [noetl/ai-meta#238](https://github.com/noetl/ai-meta/issues/238)

## [5.53.0](https://github.com/noetl/worker/compare/v5.52.0...v5.53.0) (2026-07-05)

### Features

* **ehdb:** in-process EHDB integration (readiness + data-plane + event-stream) ([#153](https://github.com/noetl/worker/issues/153)) ([d6226a2](https://github.com/noetl/worker/commit/d6226a2be071ea6f75b9f952a528e36439db3ec1)), closes [noetl/ehdb#234](https://github.com/noetl/ehdb/issues/234) [noetl/ehdb#234](https://github.com/noetl/ehdb/issues/234)

## [5.52.0](https://github.com/noetl/worker/compare/v5.51.0...v5.52.0) (2026-07-04)

### Features

* **state_builder:** execution-affinity routing for off-server drive cache ([#166](https://github.com/noetl/worker/issues/166) Phase 4) ([#152](https://github.com/noetl/worker/issues/152)) ([8728bfd](https://github.com/noetl/worker/commit/8728bfdd13884cc1e0148ea507cf48b9a70f6c9d)), closes [#116](https://github.com/noetl/worker/issues/116) [noetl/worker#151](https://github.com/noetl/worker/issues/151)

## [5.51.0](https://github.com/noetl/worker/compare/v5.50.1...v5.51.0) (2026-07-03)

### Features

* **state_reader:** cold-load execution state from object-store shard on drive miss ([#166](https://github.com/noetl/worker/issues/166) Phase 3) ([853c1bb](https://github.com/noetl/worker/commit/853c1bbb8a2dbd6e7d19cba0b0280bbf4162ad70)), closes [noetl/worker#150](https://github.com/noetl/worker/issues/150) [noetl/ai-meta#155](https://github.com/noetl/ai-meta/issues/155)

## [5.50.1](https://github.com/noetl/worker/compare/v5.50.0...v5.50.1) (2026-06-30)

### Bug Fixes

* **state_materializer:** throttle open-shard rewrites per execution ([#166](https://github.com/noetl/worker/issues/166) Phase 2) ([d7fc79d](https://github.com/noetl/worker/commit/d7fc79dc959dfe8ab612ff52f341c1d3300f04ac))

## [5.50.0](https://github.com/noetl/worker/compare/v5.49.0...v5.50.0) (2026-06-30)

### Features

* **state_materializer:** shadow object-store state-shard writer ([#166](https://github.com/noetl/worker/issues/166) Phase 2) ([f160f3e](https://github.com/noetl/worker/commit/f160f3e681a44ea4acf4f9babc65a05eb9b541ac)), closes [noetl/worker#146](https://github.com/noetl/worker/issues/146)

## [5.49.0](https://github.com/noetl/worker/compare/v5.48.1...v5.49.0) (2026-06-30)

### Features

* **state_builder:** bound the off-server WAL index — slim chain + LRU/TTL/byte-ceiling eviction + cold-rebuild-on-miss ([#166](https://github.com/noetl/worker/issues/166) Phase 1) ([149a78d](https://github.com/noetl/worker/commit/149a78d45988e43a0b11ac7b171d7cb2b90a3e3d)), closes [#156](https://github.com/noetl/worker/issues/156) [noetl/ai-meta#163](https://github.com/noetl/ai-meta/issues/163) [noetl/ai-meta#155](https://github.com/noetl/ai-meta/issues/155)

## [5.48.1](https://github.com/noetl/worker/compare/v5.48.0...v5.48.1) (2026-06-30)

### Bug Fixes

* **state_builder:** self-heal NATS consumer loss + /livez backstop ([#143](https://github.com/noetl/worker/issues/143)) ([cc9ae22](https://github.com/noetl/worker/commit/cc9ae224e2b67b732f4d0cb15902b7dda0c182a9)), closes [noetl/ai-meta#163](https://github.com/noetl/ai-meta/issues/163) [noetl/ai-meta#163](https://github.com/noetl/ai-meta/issues/163) [noetl/ai-meta#163](https://github.com/noetl/ai-meta/issues/163) [noetl/ai-meta#156](https://github.com/noetl/ai-meta/issues/156) [noetl/ai-meta#155](https://github.com/noetl/ai-meta/issues/155)

## [5.48.0](https://github.com/noetl/worker/compare/v5.47.3...v5.48.0) (2026-06-29)

### Features

* **executor:** apply server-attached event tail to off-server drive ([#156](https://github.com/noetl/worker/issues/156)) ([#142](https://github.com/noetl/worker/issues/142)) ([614fb8e](https://github.com/noetl/worker/commit/614fb8eaa665631dc09b3ce47f3e8f35b02dc132))

## [5.47.3](https://github.com/noetl/worker/compare/v5.47.2...v5.47.3) (2026-06-28)

### Bug Fixes

* **executor:** never offload the __orchestrate__ control-plane drive result ([#141](https://github.com/noetl/worker/issues/141)) ([f8f4d12](https://github.com/noetl/worker/commit/f8f4d12cb449cfcd52c35b4361c364413aa01c58)), closes [noetl/ai-meta#104](https://github.com/noetl/ai-meta/issues/104) [noetl/ai-meta#113](https://github.com/noetl/ai-meta/issues/113) [noetl/ai-meta#154](https://github.com/noetl/ai-meta/issues/154) [noetl/ai-meta#155](https://github.com/noetl/ai-meta/issues/155) [noetl/ai-meta#154](https://github.com/noetl/ai-meta/issues/154) [noetl/ai-meta#155](https://github.com/noetl/ai-meta/issues/155)

## [5.47.2](https://github.com/noetl/worker/compare/v5.47.1...v5.47.2) (2026-06-28)

### Bug Fixes

* **nats:** reuse NATS connection in drain loops — bump noetl-tools 3.19.0->3.19.1 ([#140](https://github.com/noetl/worker/issues/140)) ([9702469](https://github.com/noetl/worker/commit/9702469338b1d1a9dd8dd418361fda2fd25305d6)), closes [tools#79](https://github.com/noetl/tools/issues/79)

## [5.47.1](https://github.com/noetl/worker/compare/v5.47.0...v5.47.1) (2026-06-27)

### Performance Improvements

* **coldstart:** readiness-gated boot warmup of orchestrate drive plug-in ([#136](https://github.com/noetl/worker/issues/136)) ([fb67483](https://github.com/noetl/worker/commit/fb67483603451034b02d6ca3e1b272bd3942a30d)), closes [noetl/ai-meta#130](https://github.com/noetl/ai-meta/issues/130)
* **nats:** blocking command claim + cached consumer handle ([#135](https://github.com/noetl/worker/issues/135)) ([4450406](https://github.com/noetl/worker/commit/44504060b06d4b6a186a976d41a58b5731e7fc12)), closes [noetl/ai-meta#130](https://github.com/noetl/ai-meta/issues/130)

## [5.47.0](https://github.com/noetl/worker/compare/v5.46.3...v5.47.0) (2026-06-26)

### Features

* **container:** G2 poll-based completion fallback for long-running Jobs (SLM platform [#145](https://github.com/noetl/worker/issues/145)) ([#139](https://github.com/noetl/worker/issues/139)) ([66bf007](https://github.com/noetl/worker/commit/66bf007195b25e4b7fd0a3e810e652c89a2b2485))

## [5.46.3](https://github.com/noetl/worker/compare/v5.46.2...v5.46.3) (2026-06-25)

### Bug Fixes

* **deps:** bump noetl-tools 3.18 -> 3.18.1 (playbook payload precedence, [#136](https://github.com/noetl/worker/issues/136)) ([#138](https://github.com/noetl/worker/issues/138)) ([f5f1774](https://github.com/noetl/worker/commit/f5f17740eff4b30302c36dae0304d4d0c2d7a96f))

## [5.46.2](https://github.com/noetl/worker/compare/v5.46.1...v5.46.2) (2026-06-25)

### Bug Fixes

* **deps:** bump noetl-tools 3.17 -> 3.18 (playbook return_result, [#136](https://github.com/noetl/worker/issues/136)) ([#137](https://github.com/noetl/worker/issues/137)) ([934fb61](https://github.com/noetl/worker/commit/934fb6109b66e552907195ca356f682aaa1a3d0e)), closes [tools#80](https://github.com/noetl/tools/issues/80)

## [5.46.1](https://github.com/noetl/worker/compare/v5.46.0...v5.46.1) (2026-06-24)

### Bug Fixes

* **offserver:** event-signalled drive + release index lock per WAL apply ([#133](https://github.com/noetl/worker/issues/133)) ([402b26e](https://github.com/noetl/worker/commit/402b26e8c87210e56f28044073ff0b5cc7c62603)), closes [noetl/ai-meta#130](https://github.com/noetl/ai-meta/issues/130) [#115](https://github.com/noetl/worker/issues/115) [#103](https://github.com/noetl/worker/issues/103) [noetl/ai-meta#130](https://github.com/noetl/ai-meta/issues/130)

## [5.46.0](https://github.com/noetl/worker/compare/v5.45.0...v5.46.0) (2026-06-23)

### Features

* **result:** producer-staged result tier, flag-gated ([#104](https://github.com/noetl/worker/issues/104) OQ5 Option A) ([#132](https://github.com/noetl/worker/issues/132)) ([0d9ca18](https://github.com/noetl/worker/commit/0d9ca1804c9b6def1132e18419bc03444da770b3))

## [5.45.0](https://github.com/noetl/worker/compare/v5.44.0...v5.45.0) (2026-06-23)

### Features

* **result:** DR re-derive verify-and-repair, byte-identical ([#104](https://github.com/noetl/worker/issues/104) Phase F) ([#131](https://github.com/noetl/worker/issues/131)) ([99cde36](https://github.com/noetl/worker/commit/99cde3632597164234199c609ef0eeb19023ac10))

## [5.44.0](https://github.com/noetl/worker/compare/v5.43.0...v5.44.0) (2026-06-23)

### Features

* **barrier:** side-effect durability barrier, flag-gated ([#104](https://github.com/noetl/worker/issues/104) Phase E) ([#130](https://github.com/noetl/worker/issues/130)) ([c3ba8c7](https://github.com/noetl/worker/commit/c3ba8c78f416ee77f43c854161d389b2e54aee64)), closes [#125](https://github.com/noetl/worker/issues/125)

## [5.43.0](https://github.com/noetl/worker/compare/v5.42.0...v5.43.0) (2026-06-23)

### Features

* **result:** authoritative tier writer + tier-primary consume + rollback fallback ([#104](https://github.com/noetl/worker/issues/104) Phase D) ([419ad5f](https://github.com/noetl/worker/commit/419ad5f39c920f10de938759b8244ae2deb32038))

## [5.42.0](https://github.com/noetl/worker/compare/v5.41.0...v5.42.0) (2026-06-23)

### Features

* **result:** resolve-by-URN read path + refs-in-state bulk-bind fixes ([#104](https://github.com/noetl/worker/issues/104) Phase C) ([379bf31](https://github.com/noetl/worker/commit/379bf317c697c6b41c205567c167d5dcf8dc29e9)), closes [noetl/server#262](https://github.com/noetl/server/issues/262)

## [5.41.0](https://github.com/noetl/worker/compare/v5.40.5...v5.41.0) (2026-06-22)

### Features

* **result-materializer:** shadow Feather result tier ([#104](https://github.com/noetl/worker/issues/104) Phase B) ([c1adb7f](https://github.com/noetl/worker/commit/c1adb7fe806f5541b0d877156280884aa344287a))

## [5.40.5](https://github.com/noetl/worker/compare/v5.40.4...v5.40.5) (2026-06-22)

### Bug Fixes

* **deps:** bump noetl-tools 3.14.1 -> 3.14.2 (postgres temporal/identity serialization) ([#126](https://github.com/noetl/worker/issues/126)) ([60a849d](https://github.com/noetl/worker/commit/60a849df4cb3b559a5cc703017bef8733a270e4c)), closes [tools#75](https://github.com/noetl/tools/issues/75) [noetl/ai-meta#95](https://github.com/noetl/ai-meta/issues/95)

## [5.40.4](https://github.com/noetl/worker/compare/v5.40.3...v5.40.4) (2026-06-22)

### Bug Fixes

* **deps:** bump noetl-tools 3.14 -> 3.14.1 (task_sequence per-sub-task CPU opt) ([#127](https://github.com/noetl/worker/issues/127)) ([#125](https://github.com/noetl/worker/issues/125)) ([1a10a73](https://github.com/noetl/worker/commit/1a10a730d469a00a30c4ab9f2d782c365b2eb84e)), closes [tools#74](https://github.com/noetl/tools/issues/74)

## [5.40.3](https://github.com/noetl/worker/compare/v5.40.2...v5.40.3) (2026-06-21)

### Bug Fixes

* **deps:** bump noetl-tools 3.13 -> 3.14 (task_sequence control flow + http data-shape) ([#124](https://github.com/noetl/worker/issues/124)) ([87b85e8](https://github.com/noetl/worker/commit/87b85e883bd9a93cd0415f75a34d45896dc9493f)), closes [tools#72](https://github.com/noetl/tools/issues/72) [tools#73](https://github.com/noetl/tools/issues/73) [noetl/ai-meta#125](https://github.com/noetl/ai-meta/issues/125) [noetl/ai-meta#126](https://github.com/noetl/ai-meta/issues/126)

## [5.40.2](https://github.com/noetl/worker/compare/v5.40.1...v5.40.2) (2026-06-20)

### Bug Fixes

* **state-builder:** rebuild WAL index from retained stream on boot ([#119](https://github.com/noetl/worker/issues/119)) ([#123](https://github.com/noetl/worker/issues/123)) ([b382ef7](https://github.com/noetl/worker/commit/b382ef74f121e9e6fe18d1218d2e9da66539fb8e)), closes [#115](https://github.com/noetl/worker/issues/115) [#116](https://github.com/noetl/worker/issues/116) [#117](https://github.com/noetl/worker/issues/117)

## [5.40.1](https://github.com/noetl/worker/compare/v5.40.0...v5.40.1) (2026-06-20)

### Bug Fixes

* **state-builder:** order off-server spine by prev_event_id chain, walk from real tip ([#117](https://github.com/noetl/worker/issues/117)) ([#122](https://github.com/noetl/worker/issues/122)) ([cbe749e](https://github.com/noetl/worker/commit/cbe749ed476707c88d3a490b982f01c410b6e067))

## [5.40.0](https://github.com/noetl/worker/compare/v5.39.0...v5.40.0) (2026-06-20)

### Features

* **state-builder:** forward atomic-item-context flag onto the off-server drive input (RFC [#115](https://github.com/noetl/worker/issues/115) Phase 5) ([27047bf](https://github.com/noetl/worker/commit/27047bf1ad8569aacebe0faa268ac51576f7a7af)), closes [noetl/ai-meta#107](https://github.com/noetl/ai-meta/issues/107)

## [5.39.0](https://github.com/noetl/worker/compare/v5.38.0...v5.39.0) (2026-06-20)

### Features

* **state-builder:** stateless off-server drive — resolve trigger type off the WAL + no-op on incomplete chain (RFC [#115](https://github.com/noetl/worker/issues/115) Phase 4 remainder) ([#120](https://github.com/noetl/worker/issues/120)) ([3296d45](https://github.com/noetl/worker/commit/3296d4559f3f8bcbbb5d3088a1beb40e89c75fc2)), closes [noetl/ai-meta#107](https://github.com/noetl/ai-meta/issues/107)

## [5.38.0](https://github.com/noetl/worker/compare/v5.37.0...v5.38.0) (2026-06-20)

### Features

* **state-builder:** off-server WAL drive cutover — authoritative build via wasm run/from_events (RFC [#115](https://github.com/noetl/worker/issues/115) Phase 4) ([d5acc6f](https://github.com/noetl/worker/commit/d5acc6f5ce1a4257dff2e390b86569e1df802511))

### Bug Fixes

* **state-builder:** staleness guard — serve the WAL build only after catching up to the server's dispatch head (RFC [#115](https://github.com/noetl/worker/issues/115) Phase 4) ([57214a9](https://github.com/noetl/worker/commit/57214a97f5ffa35b72ccb495f6f998f33d8f6e88))

## [5.37.0](https://github.com/noetl/worker/compare/v5.36.0...v5.37.0) (2026-06-20)

### Features

* **state-builder:** off-server WorkflowState builder kernel + WAL shadow loop (RFC [#115](https://github.com/noetl/worker/issues/115) Phase 4) ([e0f9441](https://github.com/noetl/worker/commit/e0f94410238e64183d843b17819f566a4c2d02f2)), closes [server#245](https://github.com/noetl/server/issues/245)

## [5.36.0](https://github.com/noetl/worker/compare/v5.35.0...v5.36.0) (2026-06-19)

### Features

* **executor:** selective render-time ref resolution (refs-in-state consume side) ([#117](https://github.com/noetl/worker/issues/117)) ([10d2721](https://github.com/noetl/worker/commit/10d272163ade85e0c21e25ffb70a0c69b6055154)), closes [noetl/ai-meta#115](https://github.com/noetl/ai-meta/issues/115) [#101](https://github.com/noetl/worker/issues/101) [noetl/ai-meta#115](https://github.com/noetl/ai-meta/issues/115) [noetl/ai-meta#101](https://github.com/noetl/ai-meta/issues/101) [noetl/ai-meta#113](https://github.com/noetl/ai-meta/issues/113)

## [5.35.0](https://github.com/noetl/worker/compare/v5.34.0...v5.35.0) (2026-06-19)

### Features

* **materializer:** expose materializer-consumer lag gauge (CQRS PUBLISH_ONLY flip guardrail) ([#116](https://github.com/noetl/worker/issues/116)) ([bbd2dd9](https://github.com/noetl/worker/commit/bbd2dd95f671b4956a60e8800bce1cc115e990c3)), closes [noetl/ai-meta#103](https://github.com/noetl/ai-meta/issues/103) [noetl/ai-meta#103](https://github.com/noetl/ai-meta/issues/103)

## [5.34.0](https://github.com/noetl/worker/compare/v5.33.0...v5.34.0) (2026-06-19)

### Features

* **materializer:** in-process CQRS event materializer (ack-after-materialize) ([#115](https://github.com/noetl/worker/issues/115)) ([af34a92](https://github.com/noetl/worker/commit/af34a92ce8ea57f21e8318c896ad6a9ee00e0505)), closes [noetl/ai-meta#103](https://github.com/noetl/ai-meta/issues/103) [noetl/ai-meta#103](https://github.com/noetl/ai-meta/issues/103) [noetl/ai-meta#104](https://github.com/noetl/ai-meta/issues/104)

## [5.33.0](https://github.com/noetl/worker/compare/v5.32.0...v5.33.0) (2026-06-18)

### Features

* **nats:** pool-affinity — decline command notifications not for this worker's pool ([#114](https://github.com/noetl/worker/issues/114)) ([e2162b7](https://github.com/noetl/worker/commit/e2162b742bd64306d9cb837d6ec989d97d57e0d5)), closes [noetl/ai-meta#108](https://github.com/noetl/ai-meta/issues/108) [noetl/ai-meta#108](https://github.com/noetl/ai-meta/issues/108)

## [5.32.0](https://github.com/noetl/worker/compare/v5.31.2...v5.32.0) (2026-06-18)

### Features

* **plugin:** configurable guest entry export (run_state) for wasm dispatch ([#113](https://github.com/noetl/worker/issues/113)) ([04420d0](https://github.com/noetl/worker/commit/04420d0048ff86316f11f4e69b991ce206a56a8a)), closes [noetl/ai-meta#108](https://github.com/noetl/ai-meta/issues/108) [#105](https://github.com/noetl/worker/issues/105) [noetl/ai-meta#108](https://github.com/noetl/ai-meta/issues/108)

## [5.31.2](https://github.com/noetl/worker/compare/v5.31.1...v5.31.2) (2026-06-17)

### Performance Improvements

* **orch:** rebuild ctx/workload shims at render (paired with server dedup) ([#90](https://github.com/noetl/worker/issues/90)) ([516d172](https://github.com/noetl/worker/commit/516d172957fb68b3d0e521febd38482de9d36939)), closes [noetl/ai-meta#103](https://github.com/noetl/ai-meta/issues/103) [noetl/ai-meta#103](https://github.com/noetl/ai-meta/issues/103)

## [5.31.1](https://github.com/noetl/worker/compare/v5.31.0...v5.31.1) (2026-06-17)

### Bug Fixes

* **plugin:** read wasm plug-in input from `args` (the server's canonical field) ([#110](https://github.com/noetl/worker/issues/110)) ([c03648f](https://github.com/noetl/worker/commit/c03648f3f945f1a021aefe6cc0f1f0d6f83617e0)), closes [noetl/ai-meta#105](https://github.com/noetl/ai-meta/issues/105) [noetl/ai-meta#105](https://github.com/noetl/ai-meta/issues/105)

## [5.31.0](https://github.com/noetl/worker/compare/v5.30.0...v5.31.0) (2026-06-17)

### Features

* **plugin:** flip wasm-plugin into default features ([#105](https://github.com/noetl/worker/issues/105) Round 5 routing 3) ([#108](https://github.com/noetl/worker/issues/108)) ([83e2c32](https://github.com/noetl/worker/commit/83e2c3255167f6a4cad4d47a06deccfbfda32a3f))

## [5.30.0](https://github.com/noetl/worker/compare/v5.29.0...v5.30.0) (2026-06-17)

### Features

* **executor:** route tool_kind "wasm" to the plug-in host ([#105](https://github.com/noetl/worker/issues/105) Round 5 routing) ([#107](https://github.com/noetl/worker/issues/107)) ([3c480f3](https://github.com/noetl/worker/commit/3c480f3e10fb75f3368ebb39f2a492c63a7d585d)), closes [noetl/worker#106](https://github.com/noetl/worker/issues/106)

## [5.29.0](https://github.com/noetl/worker/compare/v5.28.0...v5.29.0) (2026-06-17)

### Features

* **plugin:** digest resolution at dispatch — load a plug-in by (path, version) ([#105](https://github.com/noetl/worker/issues/105) Round 5 routing) ([#105](https://github.com/noetl/worker/issues/105)) ([45a5f43](https://github.com/noetl/worker/commit/45a5f431922fbc85dce62955692deffc147c07f4)), closes [noetl/worker#104](https://github.com/noetl/worker/issues/104)

## [5.28.0](https://github.com/noetl/worker/compare/v5.27.0...v5.28.0) (2026-06-17)

### Features

* **plugin:** repoint object_put to the object-store endpoint ([#105](https://github.com/noetl/worker/issues/105) Round 5) ([#103](https://github.com/noetl/worker/issues/103)) ([fe40cb4](https://github.com/noetl/worker/commit/fe40cb437ba54623eacf9698ec9576c61ad477ab)), closes [noetl/server#212](https://github.com/noetl/server/issues/212) [noetl/worker#102](https://github.com/noetl/worker/issues/102)

## [5.27.0](https://github.com/noetl/worker/compare/v5.26.0...v5.27.0) (2026-06-17)

### Features

* **plugin:** WASM dispatcher core — load from catalog, run, collect intents ([#101](https://github.com/noetl/worker/issues/101)) ([da6dec5](https://github.com/noetl/worker/commit/da6dec529b9334dfe55650b4d20f216bcb5de6c3)), closes [#105](https://github.com/noetl/worker/issues/105) [noetl/ai-meta#105](https://github.com/noetl/ai-meta/issues/105) [#105](https://github.com/noetl/worker/issues/105) [noetl/ai-meta#105](https://github.com/noetl/ai-meta/issues/105)

## [5.26.0](https://github.com/noetl/worker/compare/v5.25.0...v5.26.0) (2026-06-17)

### Features

* **executor:** stamp the logical URI on over-budget result references ([#104](https://github.com/noetl/worker/issues/104) R02b) ([#99](https://github.com/noetl/worker/issues/99)) ([961797e](https://github.com/noetl/worker/commit/961797e3ffc791e648392618006ca4635c306dec)), closes [noetl/worker#98](https://github.com/noetl/worker/issues/98)

## [5.25.0](https://github.com/noetl/worker/compare/v5.24.0...v5.25.0) (2026-06-17)

### Features

* **plugin:** reference Rust→wasm system plug-in + host end-to-end test ([#105](https://github.com/noetl/worker/issues/105) Round 5) ([#97](https://github.com/noetl/worker/issues/97)) ([7298ee7](https://github.com/noetl/worker/commit/7298ee79e87bb611d74cc9ec3fc5a8bd9818f0ce))

## [5.24.0](https://github.com/noetl/worker/compare/v5.23.0...v5.24.0) (2026-06-17)

### Features

* **plugin:** HTTP PluginSource — fetch modules from the server registry ([#105](https://github.com/noetl/worker/issues/105) Round 4b) ([#95](https://github.com/noetl/worker/issues/95)) ([581c9c3](https://github.com/noetl/worker/commit/581c9c311ca3622ba6e566581896b52b999a5f60)), closes [noetl/server#210](https://github.com/noetl/server/issues/210)

## [5.23.0](https://github.com/noetl/worker/compare/v5.22.0...v5.23.0) (2026-06-17)

### Features

* **plugin:** wasmtime host skeleton for system-pool plug-ins (v5.23.0) ([#93](https://github.com/noetl/worker/issues/93)) ([fcfef01](https://github.com/noetl/worker/commit/fcfef01ac70e4d146b4ab355b84322e62933f2ae)), closes [noetl/ai-meta#101](https://github.com/noetl/ai-meta/issues/101) [#13](https://github.com/noetl/worker/issues/13) [noetl/ai-meta#101](https://github.com/noetl/ai-meta/issues/101) [noetl/ai-meta#101](https://github.com/noetl/ai-meta/issues/101) [server#208](https://github.com/noetl/server/issues/208) [noetl/ai-meta#101](https://github.com/noetl/ai-meta/issues/101) [noetl/ai-meta#101](https://github.com/noetl/ai-meta/issues/101) [noetl/ai-meta#101](https://github.com/noetl/ai-meta/issues/101) [noetl/ai-meta#101](https://github.com/noetl/ai-meta/issues/101) [noetl/ai-meta#105](https://github.com/noetl/ai-meta/issues/105) [noetl/ai-meta#105](https://github.com/noetl/ai-meta/issues/105) [#105](https://github.com/noetl/worker/issues/105) [noetl/ai-meta#105](https://github.com/noetl/ai-meta/issues/105) [#105](https://github.com/noetl/worker/issues/105) [noetl/ai-meta#105](https://github.com/noetl/ai-meta/issues/105)

## [5.22.0](https://github.com/noetl/worker/compare/v5.21.0...v5.22.0) (2026-06-15)

### Features

* **auth:** resolve transfer source/target credential aliases + noetl-tools 3.10 ([#87](https://github.com/noetl/worker/issues/87)) ([0e57e78](https://github.com/noetl/worker/commit/0e57e78f0159342c96d1a067e2292df35f963489)), closes [noetl/tools#65](https://github.com/noetl/tools/issues/65) [noetl/ai-meta#99](https://github.com/noetl/ai-meta/issues/99)

## [5.21.0](https://github.com/noetl/worker/compare/v5.20.2...v5.21.0) (2026-06-15)

### Features

* **auth:** map sf_public_key -> public_key for Snowflake keypair JWT ([#83](https://github.com/noetl/worker/issues/83)) ([b79afcb](https://github.com/noetl/worker/commit/b79afcb7b857736475cb83e9f35047df20c7ba1c)), closes [noetl/tools#62](https://github.com/noetl/tools/issues/62) [noetl/ai-meta#98](https://github.com/noetl/ai-meta/issues/98)

## [5.20.2](https://github.com/noetl/worker/compare/v5.20.1...v5.20.2) (2026-06-15)

### Bug Fixes

* **auth:** support snowflake credential type (sf_* field mapping) ([#82](https://github.com/noetl/worker/issues/82)) ([446468e](https://github.com/noetl/worker/commit/446468ec7d43fceed0f36f187f03b8310848612f)), closes [noetl/ai-meta#98](https://github.com/noetl/ai-meta/issues/98) [noetl/ai-meta#98](https://github.com/noetl/ai-meta/issues/98)

## [5.20.1](https://github.com/noetl/worker/compare/v5.20.0...v5.20.1) (2026-06-14)

### Bug Fixes

* **auth:** map nats_url/nats_user/nats_password credential fields to flat tool config names ([#81](https://github.com/noetl/worker/issues/81)) ([9ce4d6d](https://github.com/noetl/worker/commit/9ce4d6dd2951233e4fcb53da2b4de8805c762568)), closes [noetl/ai-meta#49](https://github.com/noetl/ai-meta/issues/49) [noetl/ai-meta#49](https://github.com/noetl/ai-meta/issues/49)

## [5.20.0](https://github.com/noetl/worker/compare/v5.19.0...v5.20.0) (2026-06-12)

### Features

* wire s3 spool backend + cross-restart spool drain recovery ([c441813](https://github.com/noetl/worker/commit/c4418132a98ef2fe866eb6f435871e828c8d6f50)), closes [noetl/ai-meta#94](https://github.com/noetl/ai-meta/issues/94) [noetl/ai-meta#93](https://github.com/noetl/ai-meta/issues/93) [noetl/ai-meta#94](https://github.com/noetl/ai-meta/issues/94) [noetl/ai-meta#93](https://github.com/noetl/ai-meta/issues/93)

## [5.19.0](https://github.com/noetl/worker/compare/v5.18.0...v5.19.0) (2026-06-12)

### Features

* batch dispatch + dedup opt-in + per-subscription rate limits ([#79](https://github.com/noetl/worker/issues/79)) ([83d4d2a](https://github.com/noetl/worker/commit/83d4d2ac21f3cf9eab2eed910fb720a38dc1cdb0)), closes [noetl/ai-meta#90](https://github.com/noetl/ai-meta/issues/90) [noetl/worker#78](https://github.com/noetl/worker/issues/78)

## [5.18.0](https://github.com/noetl/worker/compare/v5.17.0...v5.18.0) (2026-06-12)

### Features

* **subscription:** Cloud Run parity — gcs spool + bearer auth + $PORT bind ([f36ba68](https://github.com/noetl/worker/commit/f36ba68e8436067fc0cc056e2f9c6c6ea46ea4eb)), closes [noetl/worker#76](https://github.com/noetl/worker/issues/76) [noetl/ai-meta#90](https://github.com/noetl/ai-meta/issues/90)

## [5.17.0](https://github.com/noetl/worker/compare/v5.16.0...v5.17.0) (2026-06-12)

### Features

* wire store-and-forward spool + circuit breaker into subscription runtime ([#90](https://github.com/noetl/worker/issues/90) Phase 4) ([#75](https://github.com/noetl/worker/issues/75)) ([c612c8a](https://github.com/noetl/worker/commit/c612c8aef8bd11cb029cf15f321cb1b3a66c5922))

## [5.16.0](https://github.com/noetl/worker/compare/v5.15.2...v5.16.0) (2026-06-12)

### Features

* continuous subscription runtime (Mode B) run-mode ([#90](https://github.com/noetl/worker/issues/90) Phase 2) ([#73](https://github.com/noetl/worker/issues/73)) ([d7370b3](https://github.com/noetl/worker/commit/d7370b3bb05c5cb7d64355be0fa4d6c840d97bb2))

## [5.15.2](https://github.com/noetl/worker/compare/v5.15.1...v5.15.2) (2026-06-11)

### Bug Fixes

* **auth:** resolve nats/pubsub/kafka credential aliases into tool config ([#71](https://github.com/noetl/worker/issues/71)) ([ca606b2](https://github.com/noetl/worker/commit/ca606b224c3aaadb78e6c6b3511789b91a52e8da)), closes [noetl/ai-meta#90](https://github.com/noetl/ai-meta/issues/90) [noetl/ai-meta#90](https://github.com/noetl/ai-meta/issues/90)

## [5.15.1](https://github.com/noetl/worker/compare/v5.15.0...v5.15.1) (2026-06-10)

### Bug Fixes

* emit terminal call.error on pre-dispatch failures instead of hanging ([#68](https://github.com/noetl/worker/issues/68)) ([99e2c66](https://github.com/noetl/worker/commit/99e2c668bbf0c9d9979bfa92b64db6ca32606b28)), closes [noetl/worker#67](https://github.com/noetl/worker/issues/67) [noetl/ai-meta#78](https://github.com/noetl/ai-meta/issues/78)

## [5.15.0](https://github.com/noetl/worker/compare/v5.14.0...v5.15.0) (2026-06-08)

### Features

* **executor:** embed inline `context.data._ref` on over-budget call.done ([55e5ef6](https://github.com/noetl/worker/commit/55e5ef63f2d30c91f7c4f4195649c28a6719b3e7)), closes [noetl/ai-meta#69](https://github.com/noetl/ai-meta/issues/69) [#68](https://github.com/noetl/worker/issues/68) [noetl/ai-meta#69](https://github.com/noetl/ai-meta/issues/69)

## [5.14.0](https://github.com/noetl/worker/compare/v5.13.0...v5.14.0) (2026-06-07)

### Features

* **executor:** skip call.done emit when ToolResult.pending_callback is Some(true) ([41a98f4](https://github.com/noetl/worker/commit/41a98f441c5e9325bee86c30932f2eee0792f601)), closes [noetl/ai-meta#43](https://github.com/noetl/ai-meta/issues/43) [noetl/tools#37](https://github.com/noetl/tools/issues/37) [noetl/cli#56](https://github.com/noetl/cli/issues/56) [noetl/worker#59](https://github.com/noetl/worker/issues/59) [noetl/ai-meta#43](https://github.com/noetl/ai-meta/issues/43)

## [5.13.0](https://github.com/noetl/worker/compare/v5.12.0...v5.13.0) (2026-06-06)

### Features

* **client:** sealed credential delivery + worker keypair + zeroize (Phase 5c) ([218a5a5](https://github.com/noetl/worker/commit/218a5a522db618b273a36dcab43f6d344f9234bb)), closes [noetl/ai-meta#61](https://github.com/noetl/ai-meta/issues/61) [server#107](https://github.com/noetl/server/issues/107) [server#109](https://github.com/noetl/server/issues/109) [#57](https://github.com/noetl/worker/issues/57) [noetl/ai-meta#61](https://github.com/noetl/ai-meta/issues/61)

## [5.12.0](https://github.com/noetl/worker/compare/v5.11.3...v5.12.0) (2026-06-06)

### Features

* **tls:** worker control-plane mTLS client (Secrets Wallet Phase 4b) ([3b70c17](https://github.com/noetl/worker/commit/3b70c17c908391198d96eae0d85f2dc66e9dd202)), closes [noetl/ai-meta#61](https://github.com/noetl/ai-meta/issues/61) [server#103](https://github.com/noetl/server/issues/103) [noetl/ai-meta#61](https://github.com/noetl/ai-meta/issues/61)

## [5.11.3](https://github.com/noetl/worker/compare/v5.11.2...v5.11.3) (2026-06-05)

### Bug Fixes

* **auth_alias:** resolve keychain aliases on task_sequence sub-tasks ([ec17624](https://github.com/noetl/worker/commit/ec176243eab28c17b9e6d7eb1585204ae03161d2)), closes [noetl/ai-meta#54](https://github.com/noetl/ai-meta/issues/54) [noetl/worker#47](https://github.com/noetl/worker/issues/47) [noetl/ai-meta#54](https://github.com/noetl/ai-meta/issues/54)

## [5.11.2](https://github.com/noetl/worker/compare/v5.11.1...v5.11.2) (2026-06-05)

### Bug Fixes

* **auth_alias:** resolve keychain alias under the v10 credential: key ([7f2d118](https://github.com/noetl/worker/commit/7f2d118885cdf09eb6369e436e955acaf90671ee)), closes [noetl/ai-meta#54](https://github.com/noetl/ai-meta/issues/54) [noetl/worker#45](https://github.com/noetl/worker/issues/45) [noetl/ai-meta#54](https://github.com/noetl/ai-meta/issues/54)

## [5.11.1](https://github.com/noetl/worker/compare/v5.11.0...v5.11.1) (2026-06-05)

### Bug Fixes

* **command:** preserve array tool_config for task_sequence ([91434ab](https://github.com/noetl/worker/commit/91434ab5fe0b93a2153cb8800f77493d265f9988)), closes [noetl/ai-meta#54](https://github.com/noetl/ai-meta/issues/54) [noetl/worker#43](https://github.com/noetl/worker/issues/43) [noetl/ai-meta#54](https://github.com/noetl/ai-meta/issues/54)

## [5.11.0](https://github.com/noetl/worker/compare/v5.10.0...v5.11.0) (2026-06-03)

### Features

* **dispatch:** honor server_url from NATS command notification ([e972d1b](https://github.com/noetl/worker/commit/e972d1bab9028d159a2700aa63c4daed3e45ddf8)), closes [noetl/ai-meta#53](https://github.com/noetl/ai-meta/issues/53) [noetl/ai-meta#49](https://github.com/noetl/ai-meta/issues/49) [#35](https://github.com/noetl/worker/issues/35) [noetl/server#33](https://github.com/noetl/server/issues/33) [noetl/server#34](https://github.com/noetl/server/issues/34) [noetl/ai-meta#53](https://github.com/noetl/ai-meta/issues/53) [noetl/ai-meta#49](https://github.com/noetl/ai-meta/issues/49)

## [5.10.0](https://github.com/noetl/worker/compare/v5.9.0...v5.10.0) (2026-06-03)

### Features

* **executor:** resolve credential aliases in tool config dispatch ([2867bdc](https://github.com/noetl/worker/commit/2867bdce8dcb0914334e59fe6d442fcb49f6f0d8)), closes [noetl/ai-meta#48](https://github.com/noetl/ai-meta/issues/48) [noetl/ai-meta#42](https://github.com/noetl/ai-meta/issues/42) [noetl/ai-meta#48](https://github.com/noetl/ai-meta/issues/48)

## [5.9.0](https://github.com/noetl/worker/compare/v5.8.0...v5.9.0) (2026-06-02)

### Features

* **routing:** env-driven NATS subject + filter_subject for per-pool routing (PR-3 of 6) ([e5068f4](https://github.com/noetl/worker/commit/e5068f43119f189e37f2f77d520e9a0df919f660)), closes [noetl/ai-meta#42](https://github.com/noetl/ai-meta/issues/42) [noetl/noetl#655](https://github.com/noetl/noetl/issues/655)

## [5.8.0](https://github.com/noetl/worker/compare/v5.7.0...v5.8.0) (2026-06-02)

### Features

* **deps:** bump noetl-tools 2.11 → 2.16 + add nats/mcp dispatch tests ([4c93f49](https://github.com/noetl/worker/commit/4c93f4959e9a09fa1b36ce62d3a94be833c98702)), closes [noetl/tools#12](https://github.com/noetl/tools/issues/12) [noetl/tools#13](https://github.com/noetl/tools/issues/13) [noetl/ai-meta#40](https://github.com/noetl/ai-meta/issues/40)

## [5.7.0](https://github.com/noetl/worker/compare/v5.6.0...v5.7.0) (2026-06-01)

### Features

* **executor:** keychain env-var allow-list (noetl/ai-meta[#34](https://github.com/noetl/worker/issues/34)) ([19a76b7](https://github.com/noetl/worker/commit/19a76b758df8366f9291ed21b9aec6dce863077a)), closes [noetl/ops#133](https://github.com/noetl/ops/issues/133)

## [5.6.0](https://github.com/noetl/worker/compare/v5.5.0...v5.6.0) (2026-06-01)

### Features

* **scrub:** producer-side credential scrubbing in build_call_done_result ([a82f294](https://github.com/noetl/worker/commit/a82f294ee14de49932b30b8d9f4b8dcee7125049)), closes [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [5.5.0](https://github.com/noetl/worker/compare/v5.4.0...v5.5.0) (2026-06-01)

### Features

* **executor:** stage tabular tool outputs as Arrow IPC bytes in shm cache (R-2.2) ([69dff28](https://github.com/noetl/worker/commit/69dff28961f55d6d94358eebd3df952913f62476)), closes [noetl/tools#7](https://github.com/noetl/tools/issues/7) [#29](https://github.com/noetl/worker/issues/29) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [5.4.0](https://github.com/noetl/worker/compare/v5.3.0...v5.4.0) (2026-05-31)

### Features

* **executor:** durable result-store reference path for cross-node consumers ([73d1dd7](https://github.com/noetl/worker/commit/73d1dd7dd41ef15acae87bd32fe8963ab3673d5f)), closes [noetl/worker#24](https://github.com/noetl/worker/issues/24) [#26](https://github.com/noetl/worker/issues/26) [#28](https://github.com/noetl/worker/issues/28) [noetl/worker#24](https://github.com/noetl/worker/issues/24) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [5.3.0](https://github.com/noetl/worker/compare/v5.2.1...v5.3.0) (2026-05-31)

### Features

* **executor:** stage over-budget call.done context in shared-memory cache ([d42be16](https://github.com/noetl/worker/commit/d42be16c1a1f411d732446a13d49aab746b1044f)), closes [noetl/worker#24](https://github.com/noetl/worker/issues/24) [noetl/worker#24](https://github.com/noetl/worker/issues/24) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [5.2.1](https://github.com/noetl/worker/compare/v5.2.0...v5.2.1) (2026-05-31)

### Bug Fixes

* **executor:** pre-check call.done context size against broker budget ([cb35b48](https://github.com/noetl/worker/commit/cb35b480d9edd27459e57a59b0c254a7843ef7cf)), closes [noetl/worker#24](https://github.com/noetl/worker/issues/24) [#26](https://github.com/noetl/worker/issues/26) [noetl/worker#24](https://github.com/noetl/worker/issues/24) [noetl/worker#24](https://github.com/noetl/worker/issues/24) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30) [noetl/worker#24](https://github.com/noetl/worker/issues/24)

## [5.2.0](https://github.com/noetl/worker/compare/v5.1.3...v5.2.0) (2026-05-31)

### Features

* **executor:** emit tool output in result.context for data-flow ([689e005](https://github.com/noetl/worker/commit/689e005559d713945f6a0ef2ea4cf8a7702b0246)), closes [noetl/worker#25](https://github.com/noetl/worker/issues/25) [noetl/worker#24](https://github.com/noetl/worker/issues/24) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30) [noetl/worker#24](https://github.com/noetl/worker/issues/24)

## [5.1.3](https://github.com/noetl/worker/compare/v5.1.2...v5.1.3) (2026-05-31)

### Bug Fixes

* **executor:** emit reference-only payload for call.done per broker contract ([2b652ff](https://github.com/noetl/worker/commit/2b652ffcf6a63e1a14e11d8693fec54657a086a2)), closes [noetl/cli#39](https://github.com/noetl/cli/issues/39) [noetl/worker#24](https://github.com/noetl/worker/issues/24) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30) [noetl/worker#24](https://github.com/noetl/worker/issues/24)

## [5.1.2](https://github.com/noetl/worker/compare/v5.1.1...v5.1.2) (2026-05-31)

### Bug Fixes

* **client:** align worker registration / heartbeat / deregister with broker ([403fd13](https://github.com/noetl/worker/commit/403fd139f97606032163045838d8a3d21955e6d8)), closes [noetl/worker#19](https://github.com/noetl/worker/issues/19) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)
* **nats:** accept numeric command_id in CommandNotification + Command meta ([71b9acf](https://github.com/noetl/worker/commit/71b9acf54874368f0e69909f48ac5fef6ec02469)), closes [noetl/worker#19](https://github.com/noetl/worker/issues/19) [noetl/worker#21](https://github.com/noetl/worker/issues/21) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [5.1.1](https://github.com/noetl/worker/compare/v5.1.0...v5.1.1) (2026-05-31)

### Bug Fixes

* **nats:** honor user:pass URL credentials + NATS_USER/NATS_PASSWORD env ([fdfb588](https://github.com/noetl/worker/commit/fdfb588a985f4d3c52df5668ca03499432536068)), closes [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [5.1.0](https://github.com/noetl/worker/compare/v5.0.0...v5.1.0) (2026-05-31)

### Features

* NATS consumer-lag metric (PR-2e follow-up) ([cbe9f61](https://github.com/noetl/worker/commit/cbe9f6111da23200fe10dca4fc9929ad2f20dc22)), closes [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [5.0.0](https://github.com/noetl/worker/compare/v4.0.0...v5.0.0) (2026-05-31)

### ⚠ BREAKING CHANGES

* `EventEmitter`'s emit_* helpers and
`CommandExecutor::emit_event` now take an `attempts: u32`
parameter so the per-command retry counter rides every emitted
envelope via `meta.attempts`.  Callers pass the executor
`Command.attempts` value (or `0` when not in a command
lifecycle context).

### Features

* propagate Command.attempts through ExecutorEvent.meta on emit ([579a974](https://github.com/noetl/worker/commit/579a97439d393dd9c88134324d5f704ab4491523)), closes [noetl/worker#13](https://github.com/noetl/worker/issues/13) [#14](https://github.com/noetl/worker/issues/14) [noetl/worker#13](https://github.com/noetl/worker/issues/13) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [4.0.0](https://github.com/noetl/worker/compare/v3.0.0...v4.0.0) (2026-05-31)

### ⚠ BREAKING CHANGES

* CommandExecutor::new and EventEmitter::new /
EventEmitter::with_retry now take an Arc<SnowflakeGen>
parameter so the application-side event_id can be stamped at
emit time per observability.md Principle 3.  Callers that
constructed these types directly need to pass
SnowflakeGen::from_env_or_hint(worker_id_string).into() (or
the explicit with_node_and_epoch constructor for tests).

### Features

* app-side snowflake event_id (observability.md Principle 3) ([8f92167](https://github.com/noetl/worker/commit/8f9216742fcf4ae5a6ed66ac735b5181cad6d3f2)), closes [noetl/worker#12](https://github.com/noetl/worker/issues/12) [noetl/worker#12](https://github.com/noetl/worker/issues/12) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [3.0.0](https://github.com/noetl/worker/compare/v2.1.0...v3.0.0) (2026-05-31)

### ⚠ BREAKING CHANGES

* PR-EE-3 — adopt ExecutorEvent as wire shape on /api/events

### Features

* PR-EE-3 — adopt ExecutorEvent as wire shape on /api/events ([d8f04cf](https://github.com/noetl/worker/commit/d8f04cf1b35cba200f197d27bfdca8165f825a46)), closes [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [2.1.0](https://github.com/noetl/worker/compare/v2.0.0...v2.1.0) (2026-05-31)

### Features

* **observability:** Prometheus metrics harness + /metrics endpoint (R-1.2 PR-2e) ([b1c55ee](https://github.com/noetl/worker/commit/b1c55eee877b4b0bcd7b35dcf82dae2ba1136e6f)), closes [noetl/ai-meta#32](https://github.com/noetl/ai-meta/issues/32) [noetl/ai-meta#32](https://github.com/noetl/ai-meta/issues/32) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30) [noetl/ai-meta#32](https://github.com/noetl/ai-meta/issues/32)

## [2.0.0](https://github.com/noetl/worker/compare/v1.1.2...v2.0.0) (2026-05-31)

### ⚠ BREAKING CHANGES

* **worker:** adopt noetl-executor CommandSource 0.3.0 (R-1.2 PR-2d-2)

### Features

* **observability:** spans + execution_id correlation per observability.md ([e2b6d57](https://github.com/noetl/worker/commit/e2b6d57e30b79fa0f660ee9976900237215e325e)), closes [#6](https://github.com/noetl/worker/issues/6) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)
* **worker:** adopt noetl-executor CommandSource 0.3.0 (R-1.2 PR-2d-2) ([4836048](https://github.com/noetl/worker/commit/4836048b015f8d99e543e41b8cbb8d8645de655b)), closes [noetl/cli#35](https://github.com/noetl/cli/issues/35)

## [1.1.2](https://github.com/noetl/worker/compare/v1.1.1...v1.1.2) (2026-05-30)

### Bug Fixes

* **ci:** add actions/issues/pull-requests write permissions to semantic-release.yml ([68b410e](https://github.com/noetl/worker/commit/68b410e4df4ec3e73983355b1ed373879379d920)), closes [#4](https://github.com/noetl/worker/issues/4) [#4](https://github.com/noetl/worker/issues/4) [#4](https://github.com/noetl/worker/issues/4) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30) [noetl/worker#4](https://github.com/noetl/worker/issues/4)

## [1.1.1](https://github.com/noetl/worker/compare/v1.1.0...v1.1.1) (2026-05-30)

### Bug Fixes

* **ci:** trigger release-worker after semantic-release tags a version ([aac4f25](https://github.com/noetl/worker/commit/aac4f25de350a06bd61e5d710bf3baa8a18f0c16)), closes [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [1.1.0](https://github.com/noetl/worker/compare/v1.0.0...v1.1.0) (2026-05-30)

### Features

* **executor:** adopt noetl-executor structured condition surface (R-1.2 PR-2c) ([282d18d](https://github.com/noetl/worker/commit/282d18d7e0122dc18ec63d3f8706c1583d161bf0)), closes [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## 1.0.0 (2026-03-02)

### Bug Fixes

* harden release workflow and docker build context ([a62dc6b](https://github.com/noetl/worker/commit/a62dc6b6d0c5777aa69a88ddd73d4e4a53777a12))
* make release input parsing event-safe ([88c625f](https://github.com/noetl/worker/commit/88c625f44433ca2fdc65ed30a04da9da0c53c85f))
* release workflows on push and semantic auth ([a552a8b](https://github.com/noetl/worker/commit/a552a8b27e4272a88b4a58ac807ea99364d43dd8))
* remove secret expressions from workflow conditions ([9d3f7f0](https://github.com/noetl/worker/commit/9d3f7f0e391d70292acb38a6285cf6ece5fdd4bb))
