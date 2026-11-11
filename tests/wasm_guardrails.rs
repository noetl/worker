//! WASM host guardrails (noetl/ai-meta#548 follow-up): a plug-in that loops,
//! spins or balloons is stopped by the host, the failure says which limit
//! tripped, and the host keeps serving the next invocation.
#![cfg(feature = "wasm-plugin")]

use std::time::{Duration, Instant};

use noetl_worker::plugin::{
    MapPluginSource, PluginError, PluginKey, PluginLimits, WasmDispatcher, WasmPluginHost,
    DEFAULT_PLUGIN_FUEL, DEFAULT_PLUGIN_MAX_MEMORY, DEFAULT_PLUGIN_TIMEOUT, ECHO_PLUGIN_WAT,
};

/// Byte-ABI module whose `run` never returns.
const SPIN_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "run") (param i32 i32) (result i64)
    (loop $forever (br $forever))
    (i64.const 0)))
"#;

/// Byte-ABI module whose start function never returns: the module hangs
/// during instantiation, before any export is called.
const SPIN_AT_START_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (func $start (loop $forever (br $forever)))
  (start $start)
  (func (export "alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "run") (param i32 i32) (result i64) (i64.const 0)))
"#;

/// Byte-ABI module that grows memory one page at a time until refused, then
/// traps (what a Rust guest's allocator does on OOM).
const BALLOON_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "run") (param i32 i32) (result i64)
    (loop $grow
      (if (i32.eq (memory.grow (i32.const 1)) (i32.const -1))
        (then unreachable))
      (br $grow))
    (i64.const 0)))
"#;

/// The i32 ABI: `run(i32) -> i32` spinning forever.
const SPIN_I32_WAT: &str = r#"
(module
  (func (export "run") (param i32) (result i32)
    (loop $forever (br $forever))
    (i32.const 0)))
"#;

fn key(path: &str) -> PluginKey {
    PluginKey::new(path, 1, format!("sha-{path}"))
}

fn host(limits: PluginLimits, mods: &[(&str, &str)]) -> WasmPluginHost {
    let mut h = WasmPluginHost::with_limits(limits).expect("host");
    for (p, wat) in mods {
        h.load(&key(p), wat).expect("compile");
    }
    h
}

#[test]
fn defaults_are_on() {
    let d = PluginLimits::default();
    assert_eq!(d.fuel, Some(DEFAULT_PLUGIN_FUEL));
    assert_eq!(d.timeout, Some(DEFAULT_PLUGIN_TIMEOUT));
    assert_eq!(d.max_memory_bytes, Some(DEFAULT_PLUGIN_MAX_MEMORY));
    assert_eq!(
        PluginLimits::from_lookup(|_| None),
        d,
        "unset env = safe defaults"
    );
}

#[test]
fn env_overrides_disables_and_typos() {
    let l = PluginLimits::from_lookup(|k| match k {
        "NOETL_WASM_FUEL" => Some("1_000_000".into()),
        "NOETL_WASM_TIMEOUT_MS" => Some("0".into()),
        "NOETL_WASM_MAX_MEMORY_MB" => Some("64".into()),
        _ => None,
    });
    assert_eq!(l.fuel, Some(1_000_000));
    assert_eq!(l.timeout, None, "0 disables the deadline");
    assert_eq!(l.max_memory_bytes, Some(64 << 20));

    let typo = PluginLimits::from_lookup(|k| match k {
        "NOETL_WASM_FUEL" => Some("lots".into()),
        "NOETL_WASM_TIMEOUT_MS" => Some("off".into()),
        _ => None,
    });
    assert_eq!(
        typo.fuel,
        Some(DEFAULT_PLUGIN_FUEL),
        "a typo must not remove the guardrail"
    );
    assert_eq!(typo.timeout, None);
}

#[test]
fn a_spinning_module_runs_out_of_fuel() {
    let limits = PluginLimits {
        fuel: Some(50_000_000),
        timeout: None,
        max_memory_bytes: None,
    };
    let h = host(limits, &[("spin", SPIN_WAT)]);
    let t = Instant::now();
    let err = h.invoke_bytes(&key("spin"), b"{}").unwrap_err();
    assert!(
        matches!(
            err,
            PluginError::FuelExhausted {
                fuel: 50_000_000,
                ..
            }
        ),
        "{err}"
    );
    assert!(
        t.elapsed() < Duration::from_secs(10),
        "stopped in {:?}",
        t.elapsed()
    );
    assert!(err.to_string().contains("NOETL_WASM_FUEL"), "{err}");
}

#[test]
fn a_spinning_module_hits_the_wall_clock_deadline_with_fuel_off() {
    let limits = PluginLimits {
        fuel: None,
        timeout: Some(Duration::from_millis(300)),
        max_memory_bytes: None,
    };
    let h = host(limits, &[("spin", SPIN_WAT)]);
    let t = Instant::now();
    let err = h.invoke_bytes(&key("spin"), b"{}").unwrap_err();
    let took = t.elapsed();
    assert!(matches!(err, PluginError::Timeout { ms: 300, .. }), "{err}");
    assert!(
        took >= Duration::from_millis(250) && took < Duration::from_secs(5),
        "deadline 300ms, stopped after {took:?}"
    );
}

#[test]
fn the_i32_abi_is_guarded_too() {
    let limits = PluginLimits {
        fuel: None,
        timeout: Some(Duration::from_millis(200)),
        max_memory_bytes: None,
    };
    let h = host(limits, &[("spin32", SPIN_I32_WAT)]);
    let err = h.invoke(&key("spin32"), 1).unwrap_err();
    assert!(matches!(err, PluginError::Timeout { .. }), "{err}");
}

#[test]
fn a_module_that_hangs_in_its_start_function_is_stopped_at_instantiation() {
    let limits = PluginLimits {
        fuel: Some(20_000_000),
        timeout: Some(Duration::from_secs(5)),
        max_memory_bytes: None,
    };
    let h = host(limits, &[("start", SPIN_AT_START_WAT)]);
    let err = h.invoke_bytes(&key("start"), b"").unwrap_err();
    assert!(matches!(err, PluginError::FuelExhausted { .. }), "{err}");
}

#[test]
fn a_ballooning_module_hits_the_memory_cap() {
    let limits = PluginLimits {
        fuel: Some(DEFAULT_PLUGIN_FUEL),
        timeout: Some(Duration::from_secs(10)),
        max_memory_bytes: Some(4 << 20),
    };
    let h = host(limits, &[("balloon", BALLOON_WAT)]);
    let err = h.invoke_bytes(&key("balloon"), b"{}").unwrap_err();
    assert!(
        matches!(
            err,
            PluginError::MemoryLimit {
                limit_bytes: 4194304,
                ..
            }
        ),
        "{err}"
    );
}

#[test]
fn the_host_keeps_serving_after_a_trip_and_each_call_gets_a_fresh_budget() {
    // A budget the echo module fits in comfortably but the spinner cannot.
    let limits = PluginLimits {
        fuel: Some(5_000_000),
        timeout: Some(Duration::from_secs(5)),
        max_memory_bytes: Some(16 << 20),
    };
    let h = host(limits, &[("spin", SPIN_WAT), ("echo", ECHO_PLUGIN_WAT)]);
    let payload = vec![7u8; 4096];
    for round in 0..3 {
        let err = h.invoke_bytes(&key("spin"), b"x").unwrap_err();
        assert!(
            matches!(err, PluginError::FuelExhausted { .. }),
            "round {round}: {err}"
        );
        // If budgets leaked between invocations, the echo would starve here.
        assert_eq!(
            h.invoke_bytes(&key("echo"), &payload).unwrap(),
            payload,
            "round {round}"
        );
    }
}

#[test]
fn well_behaved_modules_run_unchanged_under_the_defaults() {
    let h = host(PluginLimits::default(), &[("echo", ECHO_PLUGIN_WAT)]);
    // ECHO has one 64 KiB page and copies the input once: keep in + out under it.
    let payload: Vec<u8> = (0..=255u8).cycle().take(16 * 1024).collect();
    assert_eq!(h.invoke_bytes(&key("echo"), &payload).unwrap(), payload);
}

#[tokio::test]
async fn the_dispatch_path_surfaces_the_trip_as_an_error() {
    let mut src = MapPluginSource::default();
    src.insert(key("system/runaway"), SPIN_WAT.as_bytes().to_vec());
    let limits = PluginLimits {
        fuel: None,
        timeout: Some(Duration::from_millis(200)),
        max_memory_bytes: None,
    };
    let mut d = WasmDispatcher::with_limits(Box::new(src), limits).unwrap();
    assert_eq!(d.limits(), limits);
    let t = Instant::now();
    let err = d.run(&key("system/runaway"), b"{}").await.unwrap_err();
    assert!(matches!(err, PluginError::Timeout { .. }), "{err}");
    assert!(t.elapsed() < Duration::from_secs(5));
}
