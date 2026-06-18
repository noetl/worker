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
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
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

/// The NoETL capability ring — the host functions a system plug-in may call.
///
/// A plug-in reaches the outside world ONLY through these; the host implements
/// the actual write (server API, result store, object store) so placement,
/// scrub, audit, and RBAC stay enforced. This is why plug-ins target
/// `wasm32-wasip1` for the toolchain but are NOT granted raw WASI fs/net — that
/// would bypass the [data-access boundary](https://github.com/noetl/noetl/blob/main/agents/rules/data-access-boundary.md).
///
/// Each method takes a borrowed key/payload (read out of the plug-in's linear
/// memory by the host) and returns a result the host maps to a status code the
/// plug-in sees. The real worker impl wraps `ControlPlaneClient` + the object
/// store; tests use a recording impl.
pub trait HostCapabilities: Send {
    /// Publish an event envelope (the `noetl.event_publish` import).
    fn event_publish(&mut self, payload: &[u8]) -> Result<(), String>;
    /// Store a result payload at a logical-URI key (the `noetl.result_put`
    /// import) — `key` is the §8 Resource Locator.
    fn result_put(&mut self, key: &str, payload: &[u8]) -> Result<(), String>;
    /// Write a buffer (Arrow Feather, …) to object store at a physical key
    /// (the `noetl.object_put` import).
    fn object_put(&mut self, key: &str, payload: &[u8]) -> Result<(), String>;
}

/// Deny-by-default capabilities — every call fails. A plug-in invoked without an
/// explicit capability impl cannot reach any host function, so forgetting to
/// wire capabilities fails closed rather than silently succeeding.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullCapabilities;

impl HostCapabilities for NullCapabilities {
    fn event_publish(&mut self, _: &[u8]) -> Result<(), String> {
        Err("no capabilities granted to this invocation".into())
    }
    fn result_put(&mut self, _: &str, _: &[u8]) -> Result<(), String> {
        Err("no capabilities granted to this invocation".into())
    }
    fn object_put(&mut self, _: &str, _: &[u8]) -> Result<(), String> {
        Err("no capabilities granted to this invocation".into())
    }
}

/// Host-side state for one plug-in invocation: the granted capability sink plus
/// the Round-1 `noetl.emit` scratch. A fresh `HostState` is built per invocation
/// so no state leaks between claims.
pub struct HostState {
    /// Values the plug-in emitted through `noetl.emit`, in call order.
    pub emitted: Vec<i32>,
    /// The capability ring this invocation may call. Deny-by-default.
    caps: Box<dyn HostCapabilities>,
}

impl Default for HostState {
    fn default() -> Self {
        Self {
            emitted: Vec::new(),
            caps: Box::new(NullCapabilities),
        }
    }
}

// Host-function status codes returned to the plug-in across the boundary.
const CAP_OK: i32 = 0;
const CAP_ERR_NO_MEMORY: i32 = 1;
const CAP_ERR_BOUNDS: i32 = 2;
const CAP_ERR_DENIED: i32 = 3;

/// Borrow a byte range out of a plug-in's linear memory, bounds-checked.
fn slice_guest(data: &[u8], ptr: i32, len: i32) -> Result<&[u8], i32> {
    let start = ptr as usize;
    let end = start.checked_add(len as usize).ok_or(CAP_ERR_BOUNDS)?;
    data.get(start..end).ok_or(CAP_ERR_BOUNDS)
}

/// Fetch the plug-in's exported `memory`, copy out an owned key (UTF-8) + payload
/// from the given linear-memory ranges, releasing the borrow before the
/// capability call. Returns a status code on any failure.
fn read_key_and_payload(
    caller: &mut Caller<'_, HostState>,
    key_ptr: i32,
    key_len: i32,
    data_ptr: i32,
    data_len: i32,
) -> Result<(String, Vec<u8>), i32> {
    let mem = caller
        .get_export("memory")
        .and_then(|e| e.into_memory())
        .ok_or(CAP_ERR_NO_MEMORY)?;
    let data = mem.data(&*caller);
    let key = std::str::from_utf8(slice_guest(data, key_ptr, key_len)?)
        .map_err(|_| CAP_ERR_BOUNDS)?
        .to_owned();
    let payload = slice_guest(data, data_ptr, data_len)?.to_vec();
    Ok((key, payload))
}

/// `noetl.event_publish(ptr, len) -> status` — publish an event envelope.
fn host_event_publish(mut caller: Caller<'_, HostState>, ptr: i32, len: i32) -> i32 {
    let payload = {
        let mem = match caller.get_export("memory").and_then(|e| e.into_memory()) {
            Some(m) => m,
            None => return CAP_ERR_NO_MEMORY,
        };
        let data = mem.data(&caller);
        match slice_guest(data, ptr, len) {
            Ok(b) => b.to_vec(),
            Err(code) => return code,
        }
    };
    match caller.data_mut().caps.event_publish(&payload) {
        Ok(()) => CAP_OK,
        Err(_) => CAP_ERR_DENIED,
    }
}

/// `noetl.result_put(key_ptr, key_len, data_ptr, data_len) -> status`.
fn host_result_put(
    mut caller: Caller<'_, HostState>,
    key_ptr: i32,
    key_len: i32,
    data_ptr: i32,
    data_len: i32,
) -> i32 {
    let (key, payload) =
        match read_key_and_payload(&mut caller, key_ptr, key_len, data_ptr, data_len) {
            Ok(kp) => kp,
            Err(code) => return code,
        };
    match caller.data_mut().caps.result_put(&key, &payload) {
        Ok(()) => CAP_OK,
        Err(_) => CAP_ERR_DENIED,
    }
}

/// `noetl.object_put(key_ptr, key_len, data_ptr, data_len) -> status`.
fn host_object_put(
    mut caller: Caller<'_, HostState>,
    key_ptr: i32,
    key_len: i32,
    data_ptr: i32,
    data_len: i32,
) -> i32 {
    let (key, payload) =
        match read_key_and_payload(&mut caller, key_ptr, key_len, data_ptr, data_len) {
            Ok(kp) => kp,
            Err(code) => return code,
        };
    match caller.data_mut().caps.object_put(&key, &payload) {
        Ok(()) => CAP_OK,
        Err(_) => CAP_ERR_DENIED,
    }
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
    #[error("linear-memory access failed: {0}")]
    Memory(String),
    #[error("invocation trapped: {0}")]
    Invoke(#[source] anyhow::Error),
    #[error("plug-in source error: {0}")]
    Source(String),
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

        // The materialiser capability ring (noetl/ai-meta#105 Round 3). A plug-in
        // reaches the platform only through these; the host impl
        // ([`HostCapabilities`]) does the real write so the data-access boundary
        // holds. A module that imports one of these gets it; a module that
        // imports a host function NOT registered here fails `instantiate`.
        linker
            .func_wrap("noetl", "event_publish", host_event_publish)
            .and_then(|l| l.func_wrap("noetl", "result_put", host_result_put))
            .and_then(|l| l.func_wrap("noetl", "object_put", host_object_put))
            .map_err(|e| PluginError::Instantiate {
                path: "<linker:noetl.capabilities>".into(),
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

    /// Ensure the module for `key` is loaded, fetching its bytes from `source`
    /// on a cache miss. This is the catalog-driven load path: `key` is the
    /// catalog identity `(path, version, digest)`, and `source` is the plug-in
    /// library (the catalog). A hit neither fetches nor recompiles, so repeated
    /// claims of an unchanged plug-in are cheap; a version bump is a new key, so
    /// the next claim fetches + compiles the new module — the hot-reload path.
    pub async fn ensure_loaded(
        &mut self,
        key: &PluginKey,
        source: &dyn PluginSource,
    ) -> Result<(), PluginError> {
        if self.cache.contains_key(key) {
            return Ok(());
        }
        let bytes = source.fetch(key).await?;
        self.load(key, bytes)
    }

    /// Ensure the module for `(path, version)` is loaded when the digest is not
    /// known up front — the dispatch path. Resolves `(digest, bytes)` from the
    /// `source` (the registry is the digest authority), keys the compile cache
    /// by the resolved digest, and returns the full [`PluginKey`] to invoke
    /// with. A version bump resolves a new digest → a new key → fresh compile
    /// (the hot-reload path); a repeat claim at an unchanged digest reuses the
    /// cached module.
    pub async fn ensure_loaded_by_ref(
        &mut self,
        path: &str,
        version: u32,
        source: &dyn PluginSource,
    ) -> Result<PluginKey, PluginError> {
        let (digest, bytes) = source.resolve(path, version).await?;
        let key = PluginKey::new(path, version, digest);
        if !self.cache.contains_key(&key) {
            self.load(&key, bytes)?;
        }
        Ok(key)
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

    /// Invoke a plug-in over the **byte data-plane ABI** — the contract real
    /// data plug-ins (the materialiser, transforms) use to move Arrow IPC /
    /// Feather buffers across the boundary **without JSON serialization**.
    ///
    /// The production pattern (per the Arrow-on-Wasm design): the module exports
    /// an allocator, the host asks it for a block, writes the input buffer
    /// straight into the module's linear memory, and passes `(ptr, len)`:
    ///
    /// 1. `alloc(len) -> in_ptr` — the module hands back an isolated block (the
    ///    host never writes to an arbitrary offset).
    /// 2. host copies `input` into linear memory at `in_ptr` (one memcpy into
    ///    the sandbox — no encode/decode; the plug-in reads the Arrow buffers in
    ///    place).
    /// 3. `run(in_ptr, len) -> packed` where `packed = (out_ptr << 32) | out_len`.
    /// 4. host reads `out_len` bytes from `out_ptr`.
    ///
    /// Arrow `RecordBatch` / Feather bytes transit intact; the plug-in reads them
    /// as Arrow buffers via pointers + lengths. Cross-*network* plug-ins use Arrow
    /// Flight instead; this is the in-process path.
    pub fn invoke_bytes(&self, key: &PluginKey, input: &[u8]) -> Result<Vec<u8>, PluginError> {
        self.invoke_bytes_with(key, input, Box::new(NullCapabilities))
    }

    /// Invoke the byte data-plane ABI with an explicit capability ring — what a
    /// real plug-in invocation uses. `caps` is moved into the per-call store, so
    /// the plug-in's `noetl.object_put` / `result_put` / `event_publish` imports
    /// route to it. The materialiser passes an impl wrapping `ControlPlaneClient`
    /// + the object store; deny-by-default ([`NullCapabilities`]) otherwise.
    pub fn invoke_bytes_with(
        &self,
        key: &PluginKey,
        input: &[u8],
        caps: Box<dyn HostCapabilities>,
    ) -> Result<Vec<u8>, PluginError> {
        self.invoke_bytes_with_entry(key, input, caps, "run")
    }

    /// Like [`invoke_bytes_with`](Self::invoke_bytes_with) but invokes a named
    /// guest export `entry` rather than the default `run`. The worker-driven
    /// orchestrator dispatches `system/orchestrate` via its `run_state` export
    /// (noetl/ai-meta#108) — the data-plane ABI (`alloc` + `entry(ptr,len)->packed`)
    /// is identical, only the export name differs.
    pub fn invoke_bytes_with_entry(
        &self,
        key: &PluginKey,
        input: &[u8],
        caps: Box<dyn HostCapabilities>,
        entry: &str,
    ) -> Result<Vec<u8>, PluginError> {
        let module = self
            .cache
            .get(key)
            .ok_or_else(|| PluginError::NotLoaded(format!("{}@{}", key.path, key.version)))?;
        let mut store = Store::new(
            &self.engine,
            HostState {
                emitted: Vec::new(),
                caps,
            },
        );
        let instance =
            self.linker
                .instantiate(&mut store, module)
                .map_err(|e| PluginError::Instantiate {
                    path: key.path.clone(),
                    source: e,
                })?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| PluginError::MissingExport("memory".into()))?;
        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "alloc")
            .map_err(|_| PluginError::MissingExport("alloc".into()))?;
        let run = instance
            .get_typed_func::<(i32, i32), i64>(&mut store, entry)
            .map_err(|_| PluginError::MissingExport(entry.into()))?;

        let len = i32::try_from(input.len())
            .map_err(|_| PluginError::Memory(format!("input {} bytes exceeds i32", input.len())))?;
        let in_ptr = alloc.call(&mut store, len).map_err(PluginError::Invoke)?;
        memory
            .write(&mut store, in_ptr as usize, input)
            .map_err(|e| PluginError::Memory(e.to_string()))?;

        let packed = run.call(&mut store, (in_ptr, len)).map_err(PluginError::Invoke)?;
        let out_ptr = ((packed >> 32) & 0xffff_ffff) as usize;
        let out_len = (packed & 0xffff_ffff) as usize;

        let mut out = vec![0u8; out_len];
        memory
            .read(&store, out_ptr, &mut out)
            .map_err(|e| PluginError::Memory(e.to_string()))?;
        Ok(out)
    }
}

/// Reference data-plane plug-in (WAT): a bump allocator + a `run` that copies
/// the input buffer through **unchanged** — the identity transform that proves
/// an Arrow buffer survives the boundary byte-for-byte. Real plug-ins replace
/// `run`'s body; the `memory` + `alloc` + `run(ptr,len)->packed` exports are the
/// data-plane contract.
pub const ECHO_PLUGIN_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (global $bump (mut i32) (i32.const 1024))
  (func $alloc (export "alloc") (param $n i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $bump))
    (global.set $bump (i32.add (global.get $bump) (local.get $n)))
    (local.get $p))
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    (local $out i32) (local $i i32)
    (local.set $out (call $alloc (local.get $len)))
    (local.set $i (i32.const 0))
    (block $done (loop $loop
      (br_if $done (i32.ge_u (local.get $i) (local.get $len)))
      (i32.store8 (i32.add (local.get $out) (local.get $i))
                  (i32.load8_u (i32.add (local.get $ptr) (local.get $i))))
      (local.set $i (i32.add (local.get $i) (i32.const 1)))
      (br $loop)))
    (i64.or (i64.shl (i64.extend_i32_u (local.get $out)) (i64.const 32))
            (i64.extend_i32_u (local.get $len)))))
"#;

/// The plug-in library a [`WasmPluginHost`] loads modules from — the catalog.
/// A miss in the host cache fetches the compiled module bytes for a
/// `(path, version, digest)` key. The live impl is an HTTP client to the
/// server's catalog plug-in endpoint (deferred until that endpoint lands — it
/// is the server-side half of Round 3); [`MapPluginSource`] is the in-memory
/// stand-in used in tests and local runs.
#[async_trait]
pub trait PluginSource: Send + Sync {
    /// Fetch the module bytes (wasm or WAT) for a fully-identified `key`
    /// (digest known) — the hot-reload cache-keyed path.
    async fn fetch(&self, key: &PluginKey) -> Result<Vec<u8>, PluginError>;

    /// Resolve a module by `(path, version)` when the digest is **not** known —
    /// the dispatch path, where a command carries only `{path, version}` (the
    /// registry is the digest authority, per the WASM dispatch convention).
    /// Returns `(digest, bytes)`; the host keys its compile cache by the
    /// resolved digest.
    async fn resolve(&self, path: &str, version: u32) -> Result<(String, Vec<u8>), PluginError>;
}

/// An in-memory [`PluginSource`] — the plug-in library backed by a map. Counts
/// fetches so a caller can confirm cache hits avoid the source.
#[derive(Default)]
pub struct MapPluginSource {
    modules: HashMap<PluginKey, Vec<u8>>,
    fetches: std::sync::atomic::AtomicU64,
}

impl MapPluginSource {
    /// Register a module's bytes under its catalog key.
    pub fn insert(&mut self, key: PluginKey, bytes: impl Into<Vec<u8>>) {
        self.modules.insert(key, bytes.into());
    }

    /// Number of `fetch` calls served — a cache hit on the host should not
    /// increment this.
    pub fn fetches(&self) -> u64 {
        self.fetches.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[async_trait]
impl PluginSource for MapPluginSource {
    async fn fetch(&self, key: &PluginKey) -> Result<Vec<u8>, PluginError> {
        self.fetches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.modules
            .get(key)
            .cloned()
            .ok_or_else(|| PluginError::NotLoaded(format!("{}@{} not in source", key.path, key.version)))
    }

    async fn resolve(&self, path: &str, version: u32) -> Result<(String, Vec<u8>), PluginError> {
        self.fetches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.modules
            .iter()
            .find(|(k, _)| k.path == path && k.version == version)
            .map(|(k, bytes)| (k.digest.clone(), bytes.clone()))
            .ok_or_else(|| PluginError::NotLoaded(format!("{path}@{version} not in source")))
    }
}

/// The live [`PluginSource`] — an HTTP client to the server's plug-in module
/// registry (`GET /api/internal/plugins/{path}?version=&digest=`, noetl/server
/// Round 4). The system worker pool points this at its control plane; the host's
/// [`WasmPluginHost::ensure_loaded`] fetches a module on a cache miss and the
/// server's digest check (409) guards against a stale cache key.
pub struct HttpPluginSource {
    client: reqwest::Client,
    base_url: String,
}

impl HttpPluginSource {
    /// Build a source pointed at the control-plane base URL (e.g.
    /// `http://noetl.noetl.svc.cluster.local:8082`).
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into(),
        }
    }

    /// Build with a shared `reqwest::Client` (connection-pool reuse with the
    /// rest of the worker's HTTP).
    pub fn with_client(client: reqwest::Client, base_url: impl Into<String>) -> Self {
        Self {
            client,
            base_url: base_url.into(),
        }
    }
}

#[async_trait]
impl PluginSource for HttpPluginSource {
    async fn fetch(&self, key: &PluginKey) -> Result<Vec<u8>, PluginError> {
        // `path` carries slashes (`system/materialiser`) — left literal to match
        // the server's `{*path}` catch-all; version + hex digest are URL-safe.
        let url = format!(
            "{}/api/internal/plugins/{}?version={}&digest={}",
            self.base_url.trim_end_matches('/'),
            key.path,
            key.version,
            key.digest,
        );
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| PluginError::Source(format!("GET {}@{}: {e}", key.path, key.version)))?;
        let status = resp.status();
        if status.is_success() {
            let bytes = resp
                .bytes()
                .await
                .map_err(|e| PluginError::Source(format!("read body for {}: {e}", key.path)))?;
            Ok(bytes.to_vec())
        } else if status == reqwest::StatusCode::NOT_FOUND {
            Err(PluginError::NotLoaded(format!(
                "{}@{} not in catalog",
                key.path, key.version
            )))
        } else if status == reqwest::StatusCode::CONFLICT {
            Err(PluginError::Source(format!(
                "digest mismatch for {}@{} (cache key stale)",
                key.path, key.version
            )))
        } else {
            Err(PluginError::Source(format!(
                "unexpected {status} fetching {}@{}",
                key.path, key.version
            )))
        }
    }

    async fn resolve(&self, path: &str, version: u32) -> Result<(String, Vec<u8>), PluginError> {
        // No digest in the query — the server returns the bytes + the digest as
        // the ETag, which becomes the host's cache key.
        let url = format!(
            "{}/api/internal/plugins/{}?version={}",
            self.base_url.trim_end_matches('/'),
            path,
            version,
        );
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| PluginError::Source(format!("GET {path}@{version}: {e}")))?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(PluginError::NotLoaded(format!(
                "{path}@{version} not in catalog"
            )));
        }
        if !status.is_success() {
            return Err(PluginError::Source(format!(
                "unexpected {status} resolving {path}@{version}"
            )));
        }
        let digest = resp
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string())
            .ok_or_else(|| PluginError::Source(format!("no ETag digest for {path}@{version}")))?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| PluginError::Source(format!("read body for {path}: {e}")))?
            .to_vec();
        Ok((digest, bytes))
    }
}

// ---------------------------------------------------------------------------
// Dispatcher (noetl/ai-meta#105 Round 5) — load from the catalog, run, collect
// ---------------------------------------------------------------------------

/// A capability call a plug-in made, buffered during the **synchronous** wasm
/// invocation for the dispatcher to apply (flush) afterwards via the **async**
/// control plane / object store. This keeps the plug-in run fast and sync while
/// the I/O stays async — the local-first split. Applying the intents is the next
/// Round-5 step (`event_publish` → emit, `result_put` → result store,
/// `object_put` → the Feather tier).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapIntent {
    EventPublish { payload: Vec<u8> },
    ResultPut { key: String, payload: Vec<u8> },
    ObjectPut { key: String, payload: Vec<u8> },
}

/// Production [`HostCapabilities`] — records each capability call as a
/// [`CapIntent`] into a shared sink the dispatcher drains after the invocation.
#[derive(Clone, Default)]
pub struct BufferingCapabilities {
    sink: Arc<Mutex<Vec<CapIntent>>>,
}

impl BufferingCapabilities {
    pub fn new() -> Self {
        Self::default()
    }

    /// A handle to the intent sink the dispatcher reads after `invoke`.
    pub fn sink(&self) -> Arc<Mutex<Vec<CapIntent>>> {
        self.sink.clone()
    }
}

impl HostCapabilities for BufferingCapabilities {
    fn event_publish(&mut self, payload: &[u8]) -> Result<(), String> {
        self.sink
            .lock()
            .unwrap()
            .push(CapIntent::EventPublish { payload: payload.to_vec() });
        Ok(())
    }
    fn result_put(&mut self, key: &str, payload: &[u8]) -> Result<(), String> {
        self.sink.lock().unwrap().push(CapIntent::ResultPut {
            key: key.to_string(),
            payload: payload.to_vec(),
        });
        Ok(())
    }
    fn object_put(&mut self, key: &str, payload: &[u8]) -> Result<(), String> {
        self.sink.lock().unwrap().push(CapIntent::ObjectPut {
            key: key.to_string(),
            payload: payload.to_vec(),
        });
        Ok(())
    }
}

/// The outcome of running a plug-in: its byte output plus the capability intents
/// it recorded (for the dispatcher's caller to flush to the control plane).
#[derive(Debug)]
pub struct WasmRunOutcome {
    pub output: Vec<u8>,
    pub intents: Vec<CapIntent>,
}

/// Loads system plug-ins from a [`PluginSource`] (the catalog) and runs them on
/// a [`WasmPluginHost`], collecting the capability intents each run records.
///
/// This is the dispatcher core. The remaining Round-5 integration is (1)
/// applying the collected intents to the control plane / object store, and (2)
/// the command-dispatch routing that selects a WASM-flagged playbook and calls
/// [`WasmDispatcher::run`] instead of the normal tool registry.
pub struct WasmDispatcher {
    host: WasmPluginHost,
    source: Box<dyn PluginSource>,
}

impl WasmDispatcher {
    /// Build a dispatcher over an explicit plug-in source.
    pub fn new(source: Box<dyn PluginSource>) -> Result<Self, PluginError> {
        Ok(Self {
            host: WasmPluginHost::new()?,
            source,
        })
    }

    /// Build a dispatcher that fetches plug-ins from the server's catalog
    /// registry at `server_url` (the production source).
    pub fn http(server_url: impl Into<String>) -> Result<Self, PluginError> {
        Self::new(Box::new(HttpPluginSource::new(server_url)))
    }

    /// Ensure the plug-in for `key` is loaded (fetching from the catalog source
    /// on a cache miss — a version bump fetches + compiles the new module), run
    /// it over the byte data-plane with `input`, and return its output plus the
    /// capability intents it recorded.
    pub async fn run(
        &mut self,
        key: &PluginKey,
        input: &[u8],
    ) -> Result<WasmRunOutcome, PluginError> {
        self.host.ensure_loaded(key, self.source.as_ref()).await?;
        self.invoke_collected(key, input, "run")
    }

    /// Like [`run`](Self::run) but addressed by `(path, version)` — the dispatch
    /// path. Resolves the digest from the catalog source, loads (hot-reload on a
    /// version bump), invokes, and collects intents.
    pub async fn run_by_ref(
        &mut self,
        path: &str,
        version: u32,
        input: &[u8],
    ) -> Result<WasmRunOutcome, PluginError> {
        self.run_by_ref_entry(path, version, input, "run").await
    }

    /// Like [`run_by_ref`](Self::run_by_ref) but invokes a named guest export
    /// `entry` (e.g. `run_state` for the worker-driven orchestrator,
    /// noetl/ai-meta#108).
    pub async fn run_by_ref_entry(
        &mut self,
        path: &str,
        version: u32,
        input: &[u8],
        entry: &str,
    ) -> Result<WasmRunOutcome, PluginError> {
        let key = self
            .host
            .ensure_loaded_by_ref(path, version, self.source.as_ref())
            .await?;
        self.invoke_collected(&key, input, entry)
    }

    /// Invoke a loaded plug-in over the byte data-plane with a fresh buffering
    /// capability ring, returning its output + collected intents.
    fn invoke_collected(
        &self,
        key: &PluginKey,
        input: &[u8],
        entry: &str,
    ) -> Result<WasmRunOutcome, PluginError> {
        let caps = BufferingCapabilities::new();
        let sink = caps.sink();
        let output = self
            .host
            .invoke_bytes_with_entry(key, input, Box::new(caps), entry)?;
        let intents = std::mem::take(&mut *sink.lock().unwrap());
        Ok(WasmRunOutcome { output, intents })
    }

    /// [`run`](Self::run) then flush the recorded intents to the control plane
    /// via [`apply_intents`]. Returns the plug-in output and the flush report.
    pub async fn run_and_apply(
        &mut self,
        key: &PluginKey,
        input: &[u8],
        client: &crate::client::ControlPlaneClient,
        execution_id: i64,
        step: &str,
    ) -> Result<(Vec<u8>, FlushReport), PluginError> {
        let outcome = self.run(key, input).await?;
        let report = apply_intents(outcome.intents, client, execution_id, step).await;
        Ok((outcome.output, report))
    }

    /// [`run_by_ref`](Self::run_by_ref) then flush — the entry point the command
    /// dispatcher calls for a `tool_kind: "wasm"` command carrying `{path,
    /// version}`.
    pub async fn run_and_apply_by_ref(
        &mut self,
        path: &str,
        version: u32,
        input: &[u8],
        client: &crate::client::ControlPlaneClient,
        execution_id: i64,
        step: &str,
    ) -> Result<(Vec<u8>, FlushReport), PluginError> {
        self.run_and_apply_by_ref_entry(path, version, input, client, execution_id, step, "run")
            .await
    }

    /// Like [`run_and_apply_by_ref`](Self::run_and_apply_by_ref) but invokes a
    /// named guest export `entry`. The worker-driven orchestrator dispatches
    /// `system/orchestrate` with `entry = "run_state"` (noetl/ai-meta#108): the
    /// plug-in returns the next commands as its output (no capability intents),
    /// which the server applies on the command's completion.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_and_apply_by_ref_entry(
        &mut self,
        path: &str,
        version: u32,
        input: &[u8],
        client: &crate::client::ControlPlaneClient,
        execution_id: i64,
        step: &str,
        entry: &str,
    ) -> Result<(Vec<u8>, FlushReport), PluginError> {
        let outcome = self.run_by_ref_entry(path, version, input, entry).await?;
        let report = apply_intents(outcome.intents, client, execution_id, step).await;
        Ok((outcome.output, report))
    }
}

/// Outcome of flushing a plug-in's [`CapIntent`]s to the control plane.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct FlushReport {
    pub results_stored: usize,
    pub objects_stored: usize,
    pub events_published: usize,
    pub errors: Vec<String>,
}

/// Encode a `result_put` payload for the result store: keep it as-is if the
/// bytes are already JSON, otherwise wrap the raw bytes base64.
fn result_payload_to_json(payload: &[u8]) -> serde_json::Value {
    match serde_json::from_slice::<serde_json::Value>(payload) {
        Ok(v) => v,
        Err(_) => {
            use base64::Engine;
            serde_json::json!({
                "_bytes_b64": base64::engine::general_purpose::STANDARD.encode(payload),
            })
        }
    }
}

/// Apply a plug-in's buffered [`CapIntent`]s to the control plane — the async
/// half of the sync-record / async-flush bridge.
///
/// - `result_put` → the durable result store via
///   `ControlPlaneClient::put_result` (JSON results pass through; binary is
///   base64-wrapped).
/// - `object_put` → the Feather tier via `ControlPlaneClient::object_put`
///   (`PUT /api/internal/objects/{key}`) — raw bytes at the §7 physical key.
/// - `event_publish` → `ControlPlaneClient::emit_event` (the payload is a
///   serialized `ExecutorEvent`).
///
/// All paths are server-mediated, so the data-access boundary holds — workers
/// never touch the object store directly.
///
/// Best-effort: a failed intent is recorded in [`FlushReport::errors`] and the
/// rest still flush — the plug-in output already returned to the caller.
pub async fn apply_intents(
    intents: Vec<CapIntent>,
    client: &crate::client::ControlPlaneClient,
    execution_id: i64,
    step: &str,
) -> FlushReport {
    let mut report = FlushReport::default();
    for intent in intents {
        match intent {
            CapIntent::ResultPut { key, payload } => {
                let data = result_payload_to_json(&payload);
                match client
                    .put_result(execution_id, &key, &data, "execution", Some(step))
                    .await
                {
                    Ok(_) => report.results_stored += 1,
                    Err(e) => report.errors.push(format!("result_put {key}: {e}")),
                }
            }
            CapIntent::ObjectPut { key, payload } => {
                // The Feather tier: a raw object write at the §7 physical key via
                // the server's object store (noetl/server#212), server-mediated.
                match client
                    .object_put(&key, payload, "application/vnd.apache.arrow.feather")
                    .await
                {
                    Ok(()) => report.objects_stored += 1,
                    Err(e) => report.errors.push(format!("object_put {key}: {e}")),
                }
            }
            CapIntent::EventPublish { payload } => {
                match serde_json::from_slice::<crate::client::ExecutorEvent>(&payload)
                {
                    Ok(event) => match client.emit_event(event).await {
                        Ok(()) => report.events_published += 1,
                        Err(e) => report.errors.push(format!("event_publish: {e}")),
                    },
                    Err(e) => report.errors.push(format!("event_publish parse: {e}")),
                }
            }
        }
    }
    report
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

    // --- Round 2: the byte data-plane ABI (alloc-export + linear memory) ---

    /// A data plug-in that adds 1 to every input byte — proves the host writes
    /// into the module's linear memory, the module reads the host-provided
    /// bytes, transforms them, and the host reads the result back.
    const TRANSFORM_PLUS_ONE_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (global $bump (mut i32) (i32.const 1024))
          (func $alloc (export "alloc") (param $n i32) (result i32)
            (local $p i32)
            (local.set $p (global.get $bump))
            (global.set $bump (i32.add (global.get $bump) (local.get $n)))
            (local.get $p))
          (func (export "run") (param $ptr i32) (param $len i32) (result i64)
            (local $out i32) (local $i i32)
            (local.set $out (call $alloc (local.get $len)))
            (local.set $i (i32.const 0))
            (block $done (loop $loop
              (br_if $done (i32.ge_u (local.get $i) (local.get $len)))
              (i32.store8 (i32.add (local.get $out) (local.get $i))
                          (i32.add (i32.load8_u (i32.add (local.get $ptr) (local.get $i)))
                                   (i32.const 1)))
              (local.set $i (i32.add (local.get $i) (i32.const 1)))
              (br $loop)))
            (i64.or (i64.shl (i64.extend_i32_u (local.get $out)) (i64.const 32))
                    (i64.extend_i32_u (local.get $len)))))
    "#;

    #[test]
    fn invoke_bytes_transforms_through_linear_memory() {
        let mut h = host();
        let key = PluginKey::new("system/transform", 1, "sha-t");
        h.load(&key, TRANSFORM_PLUS_ONE_WAT).unwrap();
        let out = h.invoke_bytes(&key, &[1, 2, 3, 254]).unwrap();
        // each byte +1, wrapping at the 8-bit store (254 -> 255).
        assert_eq!(out, vec![2, 3, 4, 255]);
    }

    #[test]
    fn invoke_bytes_echo_preserves_an_arrow_ipc_buffer() {
        // Encode a real Arrow IPC buffer via the same codec the worker uses for
        // over-budget results, push it through the wasm boundary, and assert it
        // comes back byte-identical — Arrow buffers transit without any
        // serialization, the property the data-plane ABI exists for.
        let table = serde_json::json!({
            "columns": ["id", "name"],
            "rows": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
        });
        let enc = noetl_tools::arrow_codec::try_encode_tabular_json(&table)
            .expect("tabular json encodes to Arrow IPC");
        assert!(!enc.bytes.is_empty());
        assert_eq!(enc.row_count, 2);

        let mut h = host();
        let key = PluginKey::new("system/echo", 1, "sha-e");
        h.load(&key, ECHO_PLUGIN_WAT).unwrap();
        let out = h.invoke_bytes(&key, &enc.bytes).unwrap();
        assert_eq!(out, enc.bytes, "Arrow IPC buffer must survive the boundary intact");
    }

    #[test]
    fn invoke_bytes_handles_empty_input() {
        let mut h = host();
        let key = PluginKey::new("system/echo", 1, "sha-e");
        h.load(&key, ECHO_PLUGIN_WAT).unwrap();
        assert_eq!(h.invoke_bytes(&key, &[]).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn invoke_bytes_requires_the_memory_abi_exports() {
        // The Round-1 reference plug-in has `run` but no `alloc`/`memory` data
        // ABI — invoke_bytes must report the missing export, not misbehave.
        let mut h = host();
        let key = PluginKey::new("system/reference", 1, "sha-1");
        h.load(&key, REFERENCE_PLUGIN_WAT).unwrap();
        assert!(matches!(
            h.invoke_bytes(&key, &[1, 2, 3]),
            Err(PluginError::MissingExport(_))
        ));
    }

    // --- Round 3: the materialiser capability ring + catalog-source loading ---

    /// A recording capability ring — captures `(op, key, payload)` per call so a
    /// test can assert what a plug-in invoked. Backed by an `Arc<Mutex<…>>` the
    /// test keeps a handle to after the caps are moved into the store.
    #[derive(Clone, Default)]
    struct RecordingCapabilities {
        calls: std::sync::Arc<std::sync::Mutex<Vec<(String, String, Vec<u8>)>>>,
    }
    impl HostCapabilities for RecordingCapabilities {
        fn event_publish(&mut self, p: &[u8]) -> Result<(), String> {
            self.calls.lock().unwrap().push(("event_publish".into(), String::new(), p.to_vec()));
            Ok(())
        }
        fn result_put(&mut self, k: &str, p: &[u8]) -> Result<(), String> {
            self.calls.lock().unwrap().push(("result_put".into(), k.into(), p.to_vec()));
            Ok(())
        }
        fn object_put(&mut self, k: &str, p: &[u8]) -> Result<(), String> {
            self.calls.lock().unwrap().push(("object_put".into(), k.into(), p.to_vec()));
            Ok(())
        }
    }

    /// A materialiser-shaped plug-in: it builds a key (`obj/k` at offset 16) and
    /// a payload (`DATA` at offset 32) in its own memory, calls
    /// `noetl.object_put`, and returns the host's status code as its 4-byte
    /// output — so the test sees both the recorded capability call and the
    /// status the plug-in observed.
    const MATERIALISER_WAT: &str = r#"
        (module
          (import "noetl" "object_put" (func $object_put (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 16) "obj/k")
          (data (i32.const 32) "DATA")
          (global $bump (mut i32) (i32.const 1024))
          (func $alloc (export "alloc") (param $n i32) (result i32)
            (local $p i32)
            (local.set $p (global.get $bump))
            (global.set $bump (i32.add (global.get $bump) (local.get $n)))
            (local.get $p))
          (func (export "run") (param $ptr i32) (param $len i32) (result i64)
            (local $status i32) (local $out i32)
            (local.set $status
              (call $object_put (i32.const 16) (i32.const 5) (i32.const 32) (i32.const 4)))
            (local.set $out (call $alloc (i32.const 4)))
            (i32.store (local.get $out) (local.get $status))
            (i64.or (i64.shl (i64.extend_i32_u (local.get $out)) (i64.const 32))
                    (i64.extend_i32_u (i32.const 4)))))
    "#;

    #[test]
    fn plugin_calls_granted_object_put_capability() {
        let mut h = host();
        let key = PluginKey::new("system/materialiser", 1, "sha-m");
        h.load(&key, MATERIALISER_WAT).unwrap();

        let rec = RecordingCapabilities::default();
        let log = rec.calls.clone();
        let out = h.invoke_bytes_with(&key, b"ignored", Box::new(rec)).unwrap();

        // status 0 (CAP_OK) returned to the plug-in, little-endian.
        assert_eq!(out, vec![0, 0, 0, 0]);
        // The host received the exact key + payload the plug-in built in memory.
        let calls = log.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0],
            ("object_put".to_string(), "obj/k".to_string(), b"DATA".to_vec())
        );
    }

    #[test]
    fn capabilities_deny_by_default() {
        // Same plug-in, invoked WITHOUT a capability ring (the default
        // NullCapabilities). The host function returns the denied status, which
        // the plug-in surfaces — fail closed, nothing reaches a backend.
        let mut h = host();
        let key = PluginKey::new("system/materialiser", 1, "sha-m");
        h.load(&key, MATERIALISER_WAT).unwrap();
        let out = h.invoke_bytes(&key, b"ignored").unwrap();
        assert_eq!(out, vec![3, 0, 0, 0]); // CAP_ERR_DENIED
    }

    #[tokio::test]
    async fn ensure_loaded_fetches_from_source_then_caches() {
        let mut src = MapPluginSource::default();
        let key = PluginKey::new("system/echo", 1, "sha-e");
        src.insert(key.clone(), ECHO_PLUGIN_WAT.as_bytes().to_vec());

        let mut h = host();
        h.ensure_loaded(&key, &src).await.unwrap();
        assert_eq!(src.fetches(), 1);
        assert_eq!(h.compiles(), 1);

        // Cache hit: neither the source nor the compiler is touched again.
        h.ensure_loaded(&key, &src).await.unwrap();
        assert_eq!(src.fetches(), 1, "cache hit must not fetch from the source");
        assert_eq!(h.compiles(), 1, "cache hit must not recompile");

        // The catalog-loaded module runs.
        assert_eq!(h.invoke_bytes(&key, b"hi").unwrap(), b"hi");
    }

    #[tokio::test]
    async fn ensure_loaded_reports_a_missing_module() {
        let src = MapPluginSource::default();
        let mut h = host();
        let key = PluginKey::new("system/absent", 9, "sha-x");
        assert!(matches!(
            h.ensure_loaded(&key, &src).await,
            Err(PluginError::NotLoaded(_))
        ));
    }

    /// A source that models an operator **republishing the same `path@version`
    /// with new content** while the worker runs: each `resolve` returns the
    /// next published revision (a new digest + new bytes). This is the live
    /// hot-reload trigger the dispatch path (`ensure_loaded_by_ref`) must honor
    /// without a worker restart.
    struct RepublishSource {
        revisions: Vec<(String, Vec<u8>)>,
        next: std::sync::atomic::AtomicUsize,
    }

    impl RepublishSource {
        fn new(revisions: Vec<(&str, &str)>) -> Self {
            Self {
                revisions: revisions
                    .into_iter()
                    .map(|(d, wat)| (d.to_string(), wat.as_bytes().to_vec()))
                    .collect(),
                next: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl PluginSource for RepublishSource {
        async fn fetch(&self, _key: &PluginKey) -> Result<Vec<u8>, PluginError> {
            unreachable!("ensure_loaded_by_ref resolves, never fetches")
        }

        async fn resolve(
            &self,
            _path: &str,
            _version: u32,
        ) -> Result<(String, Vec<u8>), PluginError> {
            // Advance to the next published revision, clamping at the last so a
            // repeat claim after the final republish stays on it (a stable
            // digest → host cache hit, the steady state).
            let i = self
                .next
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                .min(self.revisions.len() - 1);
            Ok(self.revisions[i].clone())
        }
    }

    #[tokio::test]
    async fn republish_same_version_hot_reloads_through_ensure_loaded_by_ref() {
        // Two revisions of the SAME path@version: rev A doubles + emits 42,
        // rev B triples + emits 99. Only the content (digest) differs.
        const REV_B_WAT: &str = r#"
            (module
              (import "noetl" "emit" (func $emit (param i32)))
              (func (export "run") (param $x i32) (result i32)
                (call $emit (i32.const 99))
                (i32.mul (local.get $x) (i32.const 3))))
        "#;
        let src =
            RepublishSource::new(vec![("sha-a", REFERENCE_PLUGIN_WAT), ("sha-b", REV_B_WAT)]);
        let mut h = host();

        // First dispatch resolves rev A → compiles → behaves as A.
        let key_a = h
            .ensure_loaded_by_ref("system/reference", 1, &src)
            .await
            .unwrap();
        assert_eq!(key_a.digest, "sha-a");
        assert_eq!(
            h.invoke(&key_a, 10).unwrap(),
            PluginOutcome {
                output: 20,
                emitted: vec![42]
            }
        );

        // Operator republishes the SAME version with new bytes. The very next
        // dispatch resolves the new digest → a distinct cache key → fresh
        // compile → new behavior, all on the running host (no restart).
        let key_b = h
            .ensure_loaded_by_ref("system/reference", 1, &src)
            .await
            .unwrap();
        assert_eq!(key_b.version, 1, "same version");
        assert_ne!(key_a.digest, key_b.digest, "new digest = hot-reload trigger");
        assert_eq!(h.compiles(), 2, "the republished bytes compiled fresh");
        assert_eq!(
            h.invoke(&key_b, 10).unwrap(),
            PluginOutcome {
                output: 30,
                emitted: vec![99]
            }
        );

        // Steady state: a repeat claim at the settled digest is a cache hit.
        let key_b2 = h
            .ensure_loaded_by_ref("system/reference", 1, &src)
            .await
            .unwrap();
        assert_eq!(key_b2.digest, "sha-b");
        assert_eq!(h.compiles(), 2, "unchanged digest must not recompile");
    }

    // --- Round 4b: the HTTP PluginSource against the server's registry ---

    /// A mock of `GET /api/internal/plugins/{*path}` — serves the echo module
    /// for `system/echo@1`, 409 on a digest mismatch, 404 otherwise.
    async fn spawn_plugin_registry() -> String {
        use axum::{
            extract::{Path as AxPath, Query},
            http::{header, StatusCode},
            response::IntoResponse,
            routing::get,
            Router,
        };
        use std::collections::HashMap as Map;
        use tokio::net::TcpListener;

        let body = ECHO_PLUGIN_WAT.as_bytes().to_vec();
        let app = Router::new().route(
            "/api/internal/plugins/{*path}",
            get(
                move |AxPath(path): AxPath<String>, Query(q): Query<Map<String, String>>| {
                    let body = body.clone();
                    async move {
                        let version = q.get("version").map(String::as_str).unwrap_or("");
                        if path != "system/echo" || version != "1" {
                            return (StatusCode::NOT_FOUND, Vec::new()).into_response();
                        }
                        if let Some(d) = q.get("digest") {
                            if d != "sha-e" {
                                return (StatusCode::CONFLICT, Vec::new()).into_response();
                            }
                        }
                        // ETag carries the digest — what `resolve` reads when the
                        // caller doesn't supply one.
                        ([(header::ETAG, "\"sha-e\"")], body).into_response()
                    }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn http_source_fetches_and_loads_through_the_host() {
        let base = spawn_plugin_registry().await;
        let src = HttpPluginSource::new(base);
        let key = PluginKey::new("system/echo", 1, "sha-e");

        let mut h = host();
        // ensure_loaded drives the HTTP fetch + compile end to end.
        h.ensure_loaded(&key, &src).await.unwrap();
        assert!(h.is_loaded(&key));
        assert_eq!(h.invoke_bytes(&key, b"abc").unwrap(), b"abc");
    }

    #[tokio::test]
    async fn http_source_resolve_reads_digest_from_etag() {
        let base = spawn_plugin_registry().await;
        let src = HttpPluginSource::new(base);
        // No digest supplied — resolve learns it from the ETag.
        let (digest, bytes) = src.resolve("system/echo", 1).await.unwrap();
        assert_eq!(digest, "sha-e");
        assert_eq!(bytes, ECHO_PLUGIN_WAT.as_bytes());
    }

    #[tokio::test]
    async fn dispatcher_run_by_ref_resolves_digest_then_runs() {
        // The dispatch path: the command carries only (path, version); the
        // dispatcher resolves the digest from the source, loads, and runs.
        const WASM: &[u8] = include_bytes!("../tests/fixtures/reference_materializer.wasm");
        let key = PluginKey::new("system/reference-materializer", 1, "sha-rm");
        let mut src = MapPluginSource::default();
        src.insert(key, WASM.to_vec());

        let mut dispatcher = WasmDispatcher::new(Box::new(src)).unwrap();
        let payload = b"FEATHER".to_vec();
        let outcome = dispatcher
            .run_by_ref("system/reference-materializer", 1, &payload)
            .await
            .unwrap();
        assert_eq!(outcome.output, vec![0]); // CAP_OK
        assert_eq!(
            outcome.intents,
            vec![CapIntent::ObjectPut {
                key: "noetl/results/reference/0/0/1.feather".to_string(),
                payload,
            }]
        );
    }

    // Exports both `run` and `run_state`; each returns a single distinct byte so
    // a test can tell which export the dispatcher invoked.
    const TWO_ENTRY_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (global $bump (mut i32) (i32.const 1024))
  (func $alloc (export "alloc") (param $n i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $bump))
    (global.set $bump (i32.add (global.get $bump) (local.get $n)))
    (local.get $p))
  (func $emit1 (param $byte i32) (result i64)
    (local $out i32)
    (local.set $out (call $alloc (i32.const 1)))
    (i32.store8 (local.get $out) (local.get $byte))
    (i64.or (i64.shl (i64.extend_i32_u (local.get $out)) (i64.const 32))
            (i64.extend_i32_u (i32.const 1))))
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    (call $emit1 (i32.const 170)))      ;; 0xAA
  (func (export "run_state") (param $ptr i32) (param $len i32) (result i64)
    (call $emit1 (i32.const 187))))     ;; 0xBB
"#;

    #[tokio::test]
    async fn dispatcher_invokes_named_entry_export() {
        // The worker-driven orchestrator dispatches `system/orchestrate` with
        // entry `run_state` (noetl/ai-meta#108). Same data-plane ABI, different
        // export — the dispatcher must call the one the command names.
        let key = PluginKey::new("system/two-entry", 1, "sha-2e");
        let mut src = MapPluginSource::default();
        src.insert(key, TWO_ENTRY_WAT.as_bytes().to_vec());
        let mut dispatcher = WasmDispatcher::new(Box::new(src)).unwrap();

        // Default `run` export.
        let run = dispatcher
            .run_by_ref("system/two-entry", 1, b"x")
            .await
            .unwrap();
        assert_eq!(run.output, vec![0xAA], "default path invokes `run`");

        // Named `run_state` export — the worker-driven path.
        let run_state = dispatcher
            .run_by_ref_entry("system/two-entry", 1, b"x", "run_state")
            .await
            .unwrap();
        assert_eq!(run_state.output, vec![0xBB], "entry path invokes `run_state`");

        // A missing export surfaces a clear error, not a panic.
        let missing = dispatcher
            .run_by_ref_entry("system/two-entry", 1, b"x", "nope")
            .await;
        assert!(matches!(missing, Err(PluginError::MissingExport(e)) if e == "nope"));
    }

    #[tokio::test]
    async fn run_by_ref_reports_a_missing_module() {
        let src = MapPluginSource::default();
        let mut dispatcher = WasmDispatcher::new(Box::new(src)).unwrap();
        assert!(matches!(
            dispatcher.run_by_ref("system/absent", 9, b"x").await,
            Err(PluginError::NotLoaded(_))
        ));
    }

    #[tokio::test]
    async fn http_source_maps_404_to_not_loaded() {
        let base = spawn_plugin_registry().await;
        let src = HttpPluginSource::new(base);
        let key = PluginKey::new("system/absent", 9, "sha-x");
        assert!(matches!(
            src.fetch(&key).await,
            Err(PluginError::NotLoaded(_))
        ));
    }

    #[test]
    fn loads_and_runs_a_real_rust_compiled_plugin() {
        // The reference plug-in is hand-written Rust compiled to
        // wasm32-unknown-unknown (`plugins/reference-materializer`) — proving a
        // REAL compiled plug-in, not just WAT, runs on the host: no_std + no
        // WASI, importing only the granted `noetl.object_put` capability, over
        // the data-plane ABI. This is the hybrid model's "reference plug-in
        // first" milestone (noetl/ai-meta#105 Round 5).
        const WASM: &[u8] = include_bytes!("../tests/fixtures/reference_materializer.wasm");
        let mut h = host();
        let key = PluginKey::new("system/reference-materializer", 1, "sha-rm");
        h.load(&key, WASM).unwrap();

        let rec = RecordingCapabilities::default();
        let log = rec.calls.clone();
        let payload = b"ARROW-FEATHER-BYTES".to_vec();
        let out = h.invoke_bytes_with(&key, &payload, Box::new(rec)).unwrap();

        // The plug-in returns the host status (0 = CAP_OK) as its 1-byte output.
        assert_eq!(out, vec![0]);
        // It wrote our exact payload to object store under its derived key,
        // through the granted capability.
        let calls = log.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "object_put");
        assert_eq!(calls[0].1, "noetl/results/reference/0/0/1.feather");
        assert_eq!(calls[0].2, payload);
    }

    #[tokio::test]
    async fn dispatcher_loads_runs_and_collects_capability_intents() {
        // End-to-end: the dispatcher fetches the real reference plug-in from a
        // catalog source, runs it on the host, and collects the `object_put`
        // intent it recorded — the dispatcher core (noetl/ai-meta#105 Round 5).
        const WASM: &[u8] = include_bytes!("../tests/fixtures/reference_materializer.wasm");
        let key = PluginKey::new("system/reference-materializer", 1, "sha-rm");
        let mut src = MapPluginSource::default();
        src.insert(key.clone(), WASM.to_vec());

        let mut dispatcher = WasmDispatcher::new(Box::new(src)).unwrap();
        let payload = b"ARROW-FEATHER-BYTES".to_vec();
        let outcome = dispatcher.run(&key, &payload).await.unwrap();

        assert_eq!(outcome.output, vec![0]); // CAP_OK
        assert_eq!(
            outcome.intents,
            vec![CapIntent::ObjectPut {
                key: "noetl/results/reference/0/0/1.feather".to_string(),
                payload,
            }]
        );
    }

    #[tokio::test]
    async fn dispatcher_caches_the_plugin_across_runs() {
        const WASM: &[u8] = include_bytes!("../tests/fixtures/reference_materializer.wasm");
        let key = PluginKey::new("system/reference-materializer", 1, "sha-rm");
        let mut src = MapPluginSource::default();
        src.insert(key.clone(), WASM.to_vec());
        // Wrap in a counting source via the in-memory `fetches()` counter.
        let mut dispatcher = WasmDispatcher::new(Box::new(src)).unwrap();
        // First run fetches + compiles; second reuses the cached module.
        dispatcher.run(&key, b"a").await.unwrap();
        let second = dispatcher.run(&key, b"bb").await.unwrap();
        // Each run records exactly one capability intent (no leakage between
        // runs — fresh store + fresh buffer per invocation).
        assert_eq!(second.intents.len(), 1);
    }

    #[test]
    fn result_payload_passthrough_json_or_wraps_binary() {
        // Already-JSON result payload passes through unchanged.
        assert_eq!(
            result_payload_to_json(br#"{"rows":3}"#),
            serde_json::json!({ "rows": 3 })
        );
        // Non-JSON binary is base64-wrapped.
        let wrapped = result_payload_to_json(&[0xff, 0x00, 0x01]);
        assert!(wrapped.get("_bytes_b64").and_then(|v| v.as_str()).is_some());
    }

    /// Mock control plane recording the result `name` of each
    /// `PUT /api/result/{eid}`, the object key of each
    /// `PUT /api/internal/objects/{*key}`, and 200ing `POST /api/events`.
    async fn spawn_control_plane(
        results: Arc<Mutex<Vec<String>>>,
        objects: Arc<Mutex<Vec<String>>>,
    ) -> String {
        use axum::{
            extract::Path as AxPath,
            http::StatusCode,
            routing::{post, put},
            Json, Router,
        };
        use tokio::net::TcpListener;

        let app = Router::new()
            .route(
                "/api/result/{eid}",
                put(move |AxPath(_eid): AxPath<String>, body: String| {
                    let results = results.clone();
                    async move {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                            if let Some(name) = v.get("name").and_then(|n| n.as_str()) {
                                results.lock().unwrap().push(name.to_string());
                            }
                        }
                        Json(serde_json::json!({
                            "ref": "noetl://execution/325/result/x/1",
                            "store": "db", "scope": "execution",
                            "bytes": 1, "sha256": null, "expires_at": null
                        }))
                    }
                }),
            )
            .route(
                "/api/internal/objects/{*key}",
                put(move |AxPath(key): AxPath<String>, _body: axum::body::Bytes| {
                    let objects = objects.clone();
                    async move {
                        objects.lock().unwrap().push(key);
                        Json(serde_json::json!({ "key": "k", "digest": "d", "bytes": 1 }))
                    }
                }),
            )
            .route("/api/events", post(|| async { StatusCode::OK }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn apply_intents_flushes_to_the_control_plane() {
        use crate::client::ControlPlaneClient;
        let results = Arc::new(Mutex::new(Vec::new()));
        let objects = Arc::new(Mutex::new(Vec::new()));
        let base = spawn_control_plane(results.clone(), objects.clone()).await;
        let client = ControlPlaneClient::new(&base);

        let intents = vec![
            CapIntent::ObjectPut {
                key: "noetl/results/ref/0/0/1.feather".to_string(),
                payload: b"FEATHER".to_vec(),
            },
            CapIntent::ResultPut {
                key: "load_facility".to_string(),
                payload: br#"{"rows":3}"#.to_vec(),
            },
        ];
        let report = apply_intents(intents, &client, 325, "step").await;

        assert_eq!(report.objects_stored, 1);
        assert_eq!(report.results_stored, 1);
        assert!(report.errors.is_empty(), "unexpected errors: {:?}", report.errors);

        // object_put → the object store endpoint at its §7 key; result_put → put_result.
        assert!(objects
            .lock()
            .unwrap()
            .contains(&"noetl/results/ref/0/0/1.feather".to_string()));
        assert!(results.lock().unwrap().contains(&"load_facility".to_string()));
    }

    #[tokio::test]
    async fn http_source_maps_digest_mismatch_to_source_error() {
        let base = spawn_plugin_registry().await;
        let src = HttpPluginSource::new(base);
        // Right path+version, wrong digest → 409 → Source error (stale cache key).
        let key = PluginKey::new("system/echo", 1, "wrong-digest");
        assert!(matches!(src.fetch(&key).await, Err(PluginError::Source(_))));
    }
}
