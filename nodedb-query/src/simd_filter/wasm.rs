// ---------------------------------------------------------------------------
// WASM SIMD128 — 4 u32 per instruction (f64/i64 use scalar)
// ---------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
use super::bitmask::words_for;
#[cfg(target_arch = "wasm32")]
use super::scalar::{scalar_eq_u32, scalar_ne_u32};

#[cfg(target_arch = "wasm32")]
#[target_feature(enable = "simd128")]
unsafe fn wasm_eq_u32_inner(values: &[u32], target: u32) -> Vec<u64> {
    use core::arch::wasm32::*;
    let mut mask = vec![0u64; words_for(values.len())];
    let target_vec = u32x4_splat(target);
    let chunks = values.len() / 4;
    for chunk in 0..chunks {
        let vals = v128_load(values.as_ptr().add(chunk * 4) as *const v128);
        let cmp = u32x4_eq(vals, target_vec);
        let base = chunk * 4;
        for lane in 0..4u32 {
            if u32x4_extract_lane::<{ lane as usize }>(cmp) != 0 {
                let idx = base + lane as usize;
                mask[idx / 64] |= 1u64 << (idx % 64);
            }
        }
    }
    for i in (chunks * 4)..values.len() {
        if values[i] == target {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

// WASM SIMD128 extraction requires const generic lane indices, which makes
// a generic loop awkward. Use scalar for WASM — LLVM auto-vectorizes well.
#[cfg(target_arch = "wasm32")]
pub(super) fn wasm_eq_u32(values: &[u32], target: u32) -> Vec<u64> {
    scalar_eq_u32(values, target)
}

#[cfg(target_arch = "wasm32")]
pub(super) fn wasm_ne_u32(values: &[u32], target: u32) -> Vec<u64> {
    scalar_ne_u32(values, target)
}
