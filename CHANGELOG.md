# Changelog

All notable changes to this project will be documented in this file.

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
