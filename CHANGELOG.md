# Changelog

All notable changes to this project will be documented in this file.

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
