// ---------------------------------------------------------------------------
// AVX-512 (x86_64) filter kernels
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
use super::bitmask::words_for;
#[cfg(target_arch = "x86_64")]
use super::scalar::{
    CmpOp, scalar_cmp_f64, scalar_cmp_i64, scalar_eq_u32, scalar_ne_u32, scalar_range_i64,
};

// ---------------------------------------------------------------------------
// AVX-512 — u32
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
pub(super) fn avx512_eq_u32(values: &[u32], target: u32) -> Vec<u64> {
    if values.len() < 32 {
        return scalar_eq_u32(values, target);
    }
    unsafe { avx512_eq_u32_inner(values, target) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn avx512_eq_u32_inner(values: &[u32], target: u32) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let mut mask = vec![0u64; words_for(values.len())];
        let target_vec = _mm512_set1_epi32(target as i32);
        let chunks = values.len() / 16;
        let ptr = values.as_ptr() as *const i32;

        for chunk in 0..chunks {
            let vals = _mm512_loadu_si512(ptr.add(chunk * 16).cast());
            let cmp: u16 = _mm512_cmpeq_epi32_mask(vals, target_vec);
            // Each bit in cmp corresponds to one of the 16 u32 elements.
            let base_bit = chunk * 16;
            let word_idx = base_bit / 64;
            let bit_offset = base_bit % 64;
            mask[word_idx] |= (cmp as u64) << bit_offset;
            // Handle overflow into next word when bit_offset + 16 > 64.
            if bit_offset > 48 {
                mask[word_idx + 1] |= (cmp as u64) >> (64 - bit_offset);
            }
        }

        // Scalar tail.
        for i in (chunks * 16)..values.len() {
            if values[i] == target {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }

        mask
    }
}

#[cfg(target_arch = "x86_64")]
pub(super) fn avx512_ne_u32(values: &[u32], target: u32) -> Vec<u64> {
    if values.len() < 32 {
        return scalar_ne_u32(values, target);
    }
    unsafe { avx512_ne_u32_inner(values, target) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn avx512_ne_u32_inner(values: &[u32], target: u32) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let mut mask = vec![0u64; words_for(values.len())];
        let target_vec = _mm512_set1_epi32(target as i32);
        let chunks = values.len() / 16;
        let ptr = values.as_ptr() as *const i32;

        for chunk in 0..chunks {
            let vals = _mm512_loadu_si512(ptr.add(chunk * 16).cast());
            let cmp: u16 = _mm512_cmpneq_epi32_mask(vals, target_vec);
            let base_bit = chunk * 16;
            let word_idx = base_bit / 64;
            let bit_offset = base_bit % 64;
            mask[word_idx] |= (cmp as u64) << bit_offset;
            if bit_offset > 48 {
                mask[word_idx + 1] |= (cmp as u64) >> (64 - bit_offset);
            }
        }

        for i in (chunks * 16)..values.len() {
            if values[i] != target {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }

        mask
    }
}

// ---------------------------------------------------------------------------
// AVX-512 — f64
//
// Each AVX-512 chunk processes 8 f64 lanes.
// _mm512_cmp_pd_mask produces a u8 where bit k is set iff lane k passes.
// ---------------------------------------------------------------------------

// Immediate constants for _mm512_cmp_pd_mask.
#[cfg(target_arch = "x86_64")]
const _CMP_GT_OQ: i32 = 30;
#[cfg(target_arch = "x86_64")]
const _CMP_GE_OQ: i32 = 29;
#[cfg(target_arch = "x86_64")]
const _CMP_LT_OQ: i32 = 17;
#[cfg(target_arch = "x86_64")]
const _CMP_LE_OQ: i32 = 18;

#[cfg(target_arch = "x86_64")]
pub(super) fn avx512_gt_f64(values: &[f64], threshold: f64) -> Vec<u64> {
    if values.len() < 16 {
        return scalar_cmp_f64(values, threshold, CmpOp::Gt);
    }
    unsafe { avx512_cmp_f64_inner::<{ _CMP_GT_OQ }>(values, threshold) }
}

#[cfg(target_arch = "x86_64")]
pub(super) fn avx512_gte_f64(values: &[f64], threshold: f64) -> Vec<u64> {
    if values.len() < 16 {
        return scalar_cmp_f64(values, threshold, CmpOp::Gte);
    }
    unsafe { avx512_cmp_f64_inner::<{ _CMP_GE_OQ }>(values, threshold) }
}

#[cfg(target_arch = "x86_64")]
pub(super) fn avx512_lt_f64(values: &[f64], threshold: f64) -> Vec<u64> {
    if values.len() < 16 {
        return scalar_cmp_f64(values, threshold, CmpOp::Lt);
    }
    unsafe { avx512_cmp_f64_inner::<{ _CMP_LT_OQ }>(values, threshold) }
}

#[cfg(target_arch = "x86_64")]
pub(super) fn avx512_lte_f64(values: &[f64], threshold: f64) -> Vec<u64> {
    if values.len() < 16 {
        return scalar_cmp_f64(values, threshold, CmpOp::Lte);
    }
    unsafe { avx512_cmp_f64_inner::<{ _CMP_LE_OQ }>(values, threshold) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn avx512_cmp_f64_inner<const IMM: i32>(values: &[f64], threshold: f64) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let mut mask = vec![0u64; words_for(values.len())];
        let thresh_vec = _mm512_set1_pd(threshold);
        let chunks = values.len() / 8;
        let ptr = values.as_ptr();

        for chunk in 0..chunks {
            let vals = _mm512_loadu_pd(ptr.add(chunk * 8));
            // u8 mask: bit k set if lane k passes the comparison.
            let cmp: u8 = _mm512_cmp_pd_mask::<IMM>(vals, thresh_vec);
            let base_bit = chunk * 8;
            let word_idx = base_bit / 64;
            let bit_offset = base_bit % 64;
            mask[word_idx] |= (cmp as u64) << bit_offset;
            // Overflow: bit_offset + 8 > 64, i.e. bit_offset > 56.
            if bit_offset > 56 {
                mask[word_idx + 1] |= (cmp as u64) >> (64 - bit_offset);
            }
        }

        // Scalar tail.
        let scalar_op = match IMM {
            _CMP_GT_OQ => CmpOp::Gt,
            _CMP_GE_OQ => CmpOp::Gte,
            _CMP_LT_OQ => CmpOp::Lt,
            _ => CmpOp::Lte,
        };
        for i in (chunks * 8)..values.len() {
            let pass = match scalar_op {
                CmpOp::Gt => values[i] > threshold,
                CmpOp::Gte => values[i] >= threshold,
                CmpOp::Lt => values[i] < threshold,
                CmpOp::Lte => values[i] <= threshold,
            };
            if pass {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }

        mask
    }
}

// ---------------------------------------------------------------------------
// AVX-512 — i64
//
// Each AVX-512 chunk processes 8 i64 lanes.
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
pub(super) fn avx512_gt_i64(values: &[i64], threshold: i64) -> Vec<u64> {
    if values.len() < 16 {
        return scalar_cmp_i64(values, threshold, CmpOp::Gt);
    }
    unsafe { avx512_gt_i64_inner(values, threshold) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn avx512_gt_i64_inner(values: &[i64], threshold: i64) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let mut mask = vec![0u64; words_for(values.len())];
        let thresh_vec = _mm512_set1_epi64(threshold);
        let chunks = values.len() / 8;
        let ptr = values.as_ptr();

        for chunk in 0..chunks {
            let vals = _mm512_loadu_si512(ptr.add(chunk * 8).cast());
            let cmp: u8 = _mm512_cmpgt_epi64_mask(vals, thresh_vec);
            let base_bit = chunk * 8;
            let word_idx = base_bit / 64;
            let bit_offset = base_bit % 64;
            mask[word_idx] |= (cmp as u64) << bit_offset;
            if bit_offset > 56 {
                mask[word_idx + 1] |= (cmp as u64) >> (64 - bit_offset);
            }
        }

        for i in (chunks * 8)..values.len() {
            if values[i] > threshold {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }

        mask
    }
}

#[cfg(target_arch = "x86_64")]
pub(super) fn avx512_gte_i64(values: &[i64], threshold: i64) -> Vec<u64> {
    if values.len() < 16 {
        return scalar_cmp_i64(values, threshold, CmpOp::Gte);
    }
    unsafe { avx512_gte_i64_inner(values, threshold) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn avx512_gte_i64_inner(values: &[i64], threshold: i64) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let mut mask = vec![0u64; words_for(values.len())];
        let thresh_vec = _mm512_set1_epi64(threshold);
        let chunks = values.len() / 8;
        let ptr = values.as_ptr();

        for chunk in 0..chunks {
            let vals = _mm512_loadu_si512(ptr.add(chunk * 8).cast());
            // AVX-512 has a direct >= mask intrinsic.
            let cmp: u8 = _mm512_cmpge_epi64_mask(vals, thresh_vec);
            let base_bit = chunk * 8;
            let word_idx = base_bit / 64;
            let bit_offset = base_bit % 64;
            mask[word_idx] |= (cmp as u64) << bit_offset;
            if bit_offset > 56 {
                mask[word_idx + 1] |= (cmp as u64) >> (64 - bit_offset);
            }
        }

        for i in (chunks * 8)..values.len() {
            if values[i] >= threshold {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }

        mask
    }
}

#[cfg(target_arch = "x86_64")]
pub(super) fn avx512_lt_i64(values: &[i64], threshold: i64) -> Vec<u64> {
    if values.len() < 16 {
        return scalar_cmp_i64(values, threshold, CmpOp::Lt);
    }
    unsafe { avx512_lt_i64_inner(values, threshold) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn avx512_lt_i64_inner(values: &[i64], threshold: i64) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let mut mask = vec![0u64; words_for(values.len())];
        let thresh_vec = _mm512_set1_epi64(threshold);
        let chunks = values.len() / 8;
        let ptr = values.as_ptr();

        for chunk in 0..chunks {
            let vals = _mm512_loadu_si512(ptr.add(chunk * 8).cast());
            let cmp: u8 = _mm512_cmplt_epi64_mask(vals, thresh_vec);
            let base_bit = chunk * 8;
            let word_idx = base_bit / 64;
            let bit_offset = base_bit % 64;
            mask[word_idx] |= (cmp as u64) << bit_offset;
            if bit_offset > 56 {
                mask[word_idx + 1] |= (cmp as u64) >> (64 - bit_offset);
            }
        }

        for i in (chunks * 8)..values.len() {
            if values[i] < threshold {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }

        mask
    }
}

#[cfg(target_arch = "x86_64")]
pub(super) fn avx512_lte_i64(values: &[i64], threshold: i64) -> Vec<u64> {
    if values.len() < 16 {
        return scalar_cmp_i64(values, threshold, CmpOp::Lte);
    }
    unsafe { avx512_lte_i64_inner(values, threshold) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn avx512_lte_i64_inner(values: &[i64], threshold: i64) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let mut mask = vec![0u64; words_for(values.len())];
        let thresh_vec = _mm512_set1_epi64(threshold);
        let chunks = values.len() / 8;
        let ptr = values.as_ptr();

        for chunk in 0..chunks {
            let vals = _mm512_loadu_si512(ptr.add(chunk * 8).cast());
            let cmp: u8 = _mm512_cmple_epi64_mask(vals, thresh_vec);
            let base_bit = chunk * 8;
            let word_idx = base_bit / 64;
            let bit_offset = base_bit % 64;
            mask[word_idx] |= (cmp as u64) << bit_offset;
            if bit_offset > 56 {
                mask[word_idx + 1] |= (cmp as u64) >> (64 - bit_offset);
            }
        }

        for i in (chunks * 8)..values.len() {
            if values[i] <= threshold {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }

        mask
    }
}

/// Fused AVX-512 range: `min <= value <= max` in one pass (load once, two compares, AND masks).
#[cfg(target_arch = "x86_64")]
pub(super) fn avx512_range_i64(values: &[i64], min: i64, max: i64) -> Vec<u64> {
    if values.len() < 16 {
        return scalar_range_i64(values, min, max);
    }
    unsafe { avx512_range_i64_inner(values, min, max) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn avx512_range_i64_inner(values: &[i64], min: i64, max: i64) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let mut mask = vec![0u64; words_for(values.len())];
        let min_vec = _mm512_set1_epi64(min);
        let max_vec = _mm512_set1_epi64(max);
        let chunks = values.len() / 8;
        let ptr = values.as_ptr();

        for chunk in 0..chunks {
            let vals = _mm512_loadu_si512(ptr.add(chunk * 8).cast());
            // value >= min AND value <= max.
            let gte: u8 = _mm512_cmpge_epi64_mask(vals, min_vec);
            let lte: u8 = _mm512_cmple_epi64_mask(vals, max_vec);
            let cmp: u8 = gte & lte;
            let base_bit = chunk * 8;
            let word_idx = base_bit / 64;
            let bit_offset = base_bit % 64;
            mask[word_idx] |= (cmp as u64) << bit_offset;
            if bit_offset > 56 {
                mask[word_idx + 1] |= (cmp as u64) >> (64 - bit_offset);
            }
        }

        for i in (chunks * 8)..values.len() {
            if values[i] >= min && values[i] <= max {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }

        mask
    }
}
