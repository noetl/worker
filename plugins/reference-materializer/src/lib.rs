//! Reference system plug-in — the worked example a real `system/materialiser`
//! plug-in follows (noetl/ai-meta#105 Round 5, hybrid model).
//!
//! Compiled to `wasm32-unknown-unknown`: **no_std + no WASI**, so it imports
//! ONLY the granted `noetl.*` capability host functions — never raw WASI
//! fs/net — keeping the data-access boundary intact. It honors the host's
//! data-plane ABI (`memory` + `alloc` + `run(ptr,len)->packed`) and the
//! capability ring (`noetl.object_put`).
#![no_std]

use core::panic::PanicInfo;

#[panic_handler]
fn panic(_: &PanicInfo) -> ! {
    loop {}
}

// The granted capability — the host registers `noetl.object_put` on its Linker.
// An import the host did NOT grant would fail instantiation (capability ring).
#[link(wasm_import_module = "noetl")]
extern "C" {
    fn object_put(key_ptr: *const u8, key_len: usize, data_ptr: *const u8, data_len: usize) -> i32;
}

// A bump allocator over a static arena — no global allocator / `alloc` crate
// needed. The host calls `alloc` for an isolated block, writes the input buffer
// in, then calls `run`.
const ARENA: usize = 1 << 16;
static mut HEAP: [u8; ARENA] = [0; ARENA];
static mut BUMP: usize = 0;

/// Data-plane ABI: hand back an isolated block in linear memory.
#[no_mangle]
pub extern "C" fn alloc(size: usize) -> *mut u8 {
    unsafe {
        let offset = BUMP;
        BUMP += size;
        core::ptr::addr_of_mut!(HEAP).cast::<u8>().add(offset)
    }
}

/// Materialiser-shaped entry: write the input buffer (an Arrow/Feather payload
/// the host copied into linear memory) to object store under a derived key via
/// the granted capability, and return the host's status code as the 1-byte
/// output — `packed = (out_ptr << 32) | out_len`.
#[no_mangle]
pub extern "C" fn run(input_ptr: *const u8, input_len: usize) -> i64 {
    const KEY: &[u8] = b"noetl/results/reference/0/0/1.feather";
    let status = unsafe { object_put(KEY.as_ptr(), KEY.len(), input_ptr, input_len) };
    let out = alloc(1);
    unsafe {
        *out = status as u8;
    }
    ((out as i64) << 32) | 1
}
