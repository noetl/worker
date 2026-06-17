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
            .get_typed_func::<(i32, i32), i64>(&mut store, "run")
            .map_err(|_| PluginError::MissingExport("run".into()))?;

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
    /// Fetch the module bytes (wasm or WAT) for `key`, or error if absent.
    async fn fetch(&self, key: &PluginKey) -> Result<Vec<u8>, PluginError>;
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

    // --- Round 4b: the HTTP PluginSource against the server's registry ---

    /// A mock of `GET /api/internal/plugins/{*path}` — serves the echo module
    /// for `system/echo@1`, 409 on a digest mismatch, 404 otherwise.
    async fn spawn_plugin_registry() -> String {
        use axum::{
            extract::{Path as AxPath, Query},
            http::StatusCode,
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
                        (StatusCode::OK, body).into_response()
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
    async fn http_source_maps_digest_mismatch_to_source_error() {
        let base = spawn_plugin_registry().await;
        let src = HttpPluginSource::new(base);
        // Right path+version, wrong digest → 409 → Source error (stale cache key).
        let key = PluginKey::new("system/echo", 1, "wrong-digest");
        assert!(matches!(src.fetch(&key).await, Err(PluginError::Source(_))));
    }
}
