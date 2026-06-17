//! WASM plug-in host for the system worker pool (noetl/ai-meta#105, Round 1).
//!
//! Phase 4 of the
//! [System Worker Pool + WASM Plug-in ADR](https://github.com/noetl/docs/blob/main/docs/architecture/system_pool_and_wasm_plugins.md):
//! system-service logic is authored as playbooks on the system worker pool and,
//! when a hot loop earns it, compiled to a WASM module that the worker loads,
//! invokes, hot-reloads, and discards — without restarting the pool, with the
//! catalog as the managed, replaceable plug-in library.
//!
//! This Round-1 skeleton proves the four load-bearing mechanics with a `wasmtime`
//! engine and an inline WAT reference plug-in; it is **not yet wired into command
//! dispatch** (gated behind the `wasm-plugin` cargo feature, off by default):
//!
//! 1. **Load + compile** a module (WAT or wasm bytes) and cache it by
//!    `(path, version, digest)` — the catalog identity.
//! 2. **Capability-based imports** — a plug-in may import only the host functions
//!    the [`WasmPluginHost`]'s `Linker` registers. An ungranted import fails
//!    instantiation, so the capability ring is enforced by construction, not by a
//!    runtime check we could forget.
//! 3. **Invoke** the module's `run` export, observing both its return value and
//!    the side effects it produced through granted capabilities.
//! 4. **Hot-reload** — a catalog version bump is a new cache key; the new module
//!    compiles and the old version evicts, a clean swap with no process restart.
//!
//! The lowering model that produces real plug-in modules from playbooks (hybrid:
//! a hand-written Rust reference plug-in first, then a playbook→WASM lowering
//! pass) lands in later rounds. Round 1 fixes the host contract those modules
//! target.

use std::collections::HashMap;

use wasmtime::{Caller, Engine, Linker, Module, Store};

/// The canonical reference plug-in, in WebAssembly text. It imports the single
/// granted capability (`noetl.emit`), calls it once as an observable side
/// effect, and returns its input doubled — the smallest program that exercises
/// every host mechanic. Real system plug-ins replace this; the import/export
/// shape is the contract they honor.
pub const REFERENCE_PLUGIN_WAT: &str = r#"
(module
  ;; Capability import — the host grants exactly this and nothing else.
  (import "noetl" "emit" (func $emit (param i32)))
  ;; Entry point the host invokes per claim.
  (func (export "run") (param $input i32) (result i32)
    (call $emit (i32.const 42))                       ;; observable capability effect
    (i32.mul (local.get $input) (i32.const 2))))
"#;

/// Catalog identity of one plug-in version — the hot-reload cache key. A version
/// bump (or a digest change at the same version) is a distinct key, so the new
/// module compiles fresh and the old one can evict.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PluginKey {
    /// Catalog path of the plug-in playbook, e.g. `system/materialiser`.
    pub path: String,
    /// Catalog version — monotonically bumped on replacement.
    pub version: u32,
    /// Content digest of the compiled module — guards against a stale cache when
    /// a module is republished without a version bump.
    pub digest: String,
}

impl PluginKey {
    pub fn new(path: impl Into<String>, version: u32, digest: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            version,
            digest: digest.into(),
        }
    }
}

/// Host-side state a plug-in invocation can affect through its granted capability
/// imports. Round 1 grants a single `noetl.emit` capability that records the
/// values a plug-in emits; later rounds widen this to the real system-pool
/// capability set (event publish, result-store write, object-store put), each
/// added to the [`WasmPluginHost`] `Linker`.
#[derive(Default)]
pub struct HostState {
    /// Values the plug-in emitted through `noetl.emit`, in call order.
    pub emitted: Vec<i32>,
}

/// The outcome of one plug-in invocation.
#[derive(Debug, PartialEq, Eq)]
pub struct PluginOutcome {
    /// The `run` export's return value.
    pub output: i32,
    /// Values emitted through granted capabilities during the call.
    pub emitted: Vec<i32>,
}

/// Errors loading or invoking a plug-in.
#[derive(Debug, thiserror::Error)]
pub enum PluginError {
    #[error("compile failed for {path}: {source}")]
    Compile {
        path: String,
        #[source]
        source: anyhow::Error,
    },
    #[error("instantiate failed for {path} (likely an ungranted capability import): {source}")]
    Instantiate {
        path: String,
        #[source]
        source: anyhow::Error,
    },
    #[error("module {0} is not loaded")]
    NotLoaded(String),
    #[error("module is missing the required `{0}` export")]
    MissingExport(String),
    #[error("invocation trapped: {0}")]
    Invoke(#[source] anyhow::Error),
}

/// The wasmtime host that loads, caches, hot-reloads, and invokes system
/// plug-in modules. One host serves the whole system worker pool process;
/// modules are shared across claims, instances are per-invocation.
pub struct WasmPluginHost {
    engine: Engine,
    linker: Linker<HostState>,
    cache: HashMap<PluginKey, Module>,
    compiles: u64,
}

impl WasmPluginHost {
    /// Build a host with the Round-1 capability ring (`noetl.emit` only).
    pub fn new() -> Result<Self, PluginError> {
        let engine = Engine::default();
        let mut linker = Linker::new(&engine);
        // The capability ring. wasmtime resolves a module's imports against the
        // names registered here; an import the host did NOT register fails
        // `instantiate`, so a plug-in cannot reach a capability it was not
        // granted — enforced structurally, not by an auditable runtime check.
        linker
            .func_wrap(
                "noetl",
                "emit",
                |mut caller: Caller<'_, HostState>, value: i32| {
                    caller.data_mut().emitted.push(value);
                },
            )
            .map_err(|e| PluginError::Instantiate {
                path: "<linker:noetl.emit>".into(),
                source: e,
            })?;
        Ok(Self {
            engine,
            linker,
            cache: HashMap::new(),
            compiles: 0,
        })
    }

    /// Number of compilations performed. A cache hit does not increment it — the
    /// observable that proves repeated claims of an unchanged plug-in reuse the
    /// compiled module.
    pub fn compiles(&self) -> u64 {
        self.compiles
    }

    /// `true` if `key` is loaded.
    pub fn is_loaded(&self, key: &PluginKey) -> bool {
        self.cache.contains_key(key)
    }

    /// Load and compile a plug-in if not already cached. `source` is WAT text or
    /// wasm bytes (in production, the catalog-stored compiled module for `key`).
    /// Idempotent: a second load of the same key is a no-op.
    pub fn load(&mut self, key: &PluginKey, source: impl AsRef<[u8]>) -> Result<(), PluginError> {
        if self.cache.contains_key(key) {
            return Ok(());
        }
        let module = Module::new(&self.engine, source).map_err(|e| PluginError::Compile {
            path: key.path.clone(),
            source: e,
        })?;
        self.cache.insert(key.clone(), module);
        self.compiles += 1;
        Ok(())
    }

    /// Evict every cached version of `path` except `keep` — the hot-reload step
    /// after a catalog version bump installs the new module.
    pub fn evict_other_versions(&mut self, path: &str, keep: &PluginKey) {
        self.cache.retain(|k, _| k.path != path || k == keep);
    }

    /// Number of cached modules (test/observability aid).
    pub fn cached_len(&self) -> usize {
        self.cache.len()
    }

    /// Invoke the plug-in's `run(i32) -> i32` export in a fresh store, returning
    /// its result plus whatever it emitted through granted capabilities. A new
    /// [`Store`] per call keeps invocations isolated — no state leaks between
    /// claims.
    pub fn invoke(&self, key: &PluginKey, input: i32) -> Result<PluginOutcome, PluginError> {
        let module = self
            .cache
            .get(key)
            .ok_or_else(|| PluginError::NotLoaded(format!("{}@{}", key.path, key.version)))?;
        let mut store = Store::new(&self.engine, HostState::default());
        let instance =
            self.linker
                .instantiate(&mut store, module)
                .map_err(|e| PluginError::Instantiate {
                    path: key.path.clone(),
                    source: e,
                })?;
        let run = instance
            .get_typed_func::<i32, i32>(&mut store, "run")
            .map_err(|_| PluginError::MissingExport("run".into()))?;
        let output = run.call(&mut store, input).map_err(PluginError::Invoke)?;
        let emitted = std::mem::take(&mut store.data_mut().emitted);
        Ok(PluginOutcome { output, emitted })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host() -> WasmPluginHost {
        WasmPluginHost::new().expect("host builds")
    }

    #[test]
    fn loads_and_invokes_reference_plugin() {
        let mut h = host();
        let key = PluginKey::new("system/reference", 1, "sha-1");
        h.load(&key, REFERENCE_PLUGIN_WAT).unwrap();
        let out = h.invoke(&key, 21).unwrap();
        // run returns input*2 ...
        assert_eq!(out.output, 42);
        // ... and emitted 42 through the granted capability.
        assert_eq!(out.emitted, vec![42]);
    }

    #[test]
    fn cache_hit_does_not_recompile() {
        let mut h = host();
        let key = PluginKey::new("system/reference", 1, "sha-1");
        h.load(&key, REFERENCE_PLUGIN_WAT).unwrap();
        h.load(&key, REFERENCE_PLUGIN_WAT).unwrap();
        h.load(&key, REFERENCE_PLUGIN_WAT).unwrap();
        assert_eq!(h.compiles(), 1, "same key must compile once");
        // The cached module still invokes.
        assert_eq!(h.invoke(&key, 5).unwrap().output, 10);
    }

    #[test]
    fn version_bump_hot_swaps_behavior() {
        // v2 emits a different value and triples instead of doubles.
        const V2_WAT: &str = r#"
            (module
              (import "noetl" "emit" (func $emit (param i32)))
              (func (export "run") (param $x i32) (result i32)
                (call $emit (i32.const 99))
                (i32.mul (local.get $x) (i32.const 3))))
        "#;
        let mut h = host();
        let v1 = PluginKey::new("system/reference", 1, "sha-1");
        let v2 = PluginKey::new("system/reference", 2, "sha-2");

        h.load(&v1, REFERENCE_PLUGIN_WAT).unwrap();
        h.load(&v2, V2_WAT).unwrap();
        assert_eq!(h.compiles(), 2);
        assert_eq!(h.cached_len(), 2);

        // Both versions coexist and behave per their own code.
        assert_eq!(h.invoke(&v1, 10).unwrap(), PluginOutcome { output: 20, emitted: vec![42] });
        assert_eq!(h.invoke(&v2, 10).unwrap(), PluginOutcome { output: 30, emitted: vec![99] });

        // Hot-reload: install v2, evict v1 — clean swap, no restart.
        h.evict_other_versions("system/reference", &v2);
        assert!(!h.is_loaded(&v1));
        assert!(h.is_loaded(&v2));
        assert_eq!(h.cached_len(), 1);
        assert!(matches!(h.invoke(&v1, 1), Err(PluginError::NotLoaded(_))));
    }

    #[test]
    fn ungranted_capability_import_is_rejected() {
        // A plug-in that imports a host function the ring does NOT grant.
        const ROGUE_WAT: &str = r#"
            (module
              (import "noetl" "exfiltrate" (func $x (param i32)))
              (func (export "run") (param i32) (result i32)
                (call $x (i32.const 1)) (i32.const 0)))
        "#;
        let mut h = host();
        let key = PluginKey::new("system/rogue", 1, "sha-r");
        // It compiles fine (well-formed wasm) ...
        h.load(&key, ROGUE_WAT).unwrap();
        // ... but cannot instantiate: the ungranted import is unresolved, so the
        // capability ring blocks it by construction.
        assert!(matches!(h.invoke(&key, 0), Err(PluginError::Instantiate { .. })));
    }

    #[test]
    fn missing_run_export_is_reported() {
        const NO_RUN_WAT: &str = r#"(module (func (export "other") (result i32) (i32.const 0)))"#;
        let mut h = host();
        let key = PluginKey::new("system/no_run", 1, "sha-n");
        h.load(&key, NO_RUN_WAT).unwrap();
        assert!(matches!(h.invoke(&key, 0), Err(PluginError::MissingExport(_))));
    }

    #[test]
    fn invoking_unloaded_plugin_errors() {
        let h = host();
        let key = PluginKey::new("system/absent", 1, "sha-a");
        assert!(matches!(h.invoke(&key, 0), Err(PluginError::NotLoaded(_))));
    }
}
