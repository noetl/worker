# Changelog

All notable changes to this project will be documented in this file.

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
