// ---------------------------------------------------------------------------
// AVX2 (x86_64) filter kernels
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
use super::bitmask::words_for;
#[cfg(target_arch = "x86_64")]
use super::scalar::{
    CmpOp, scalar_cmp_f64, scalar_cmp_i64, scalar_eq_u32, scalar_ne_u32, scalar_range_i64,
};

// ---------------------------------------------------------------------------
// AVX2 — u32
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
pub(super) fn avx2_eq_u32(values: &[u32], target: u32) -> Vec<u64> {
    if values.len() < 16 {
        return scalar_eq_u32(values, target);
    }
    unsafe { avx2_eq_u32_inner(values, target) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_eq_u32_inner(values: &[u32], target: u32) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let mut mask = vec![0u64; words_for(values.len())];
        let target_vec = _mm256_set1_epi32(target as i32);
        let chunks = values.len() / 8;
        let ptr = values.as_ptr() as *const i32;

        for chunk in 0..chunks {
            let vals = _mm256_loadu_si256(ptr.add(chunk * 8).cast());
            let cmp = _mm256_cmpeq_epi32(vals, target_vec);
            // movemask extracts the sign bit of each 8-bit element (32 bits).
            // We want one bit per 32-bit lane, so use movemask on bytes and
            // extract every 4th bit.
            let raw_mask = _mm256_movemask_epi8(cmp) as u32;
            // raw_mask has 32 bits (one per byte), we want 8 bits (one per
            // i32 lane). Take every 4th bit: bits 3,7,11,15,19,23,27,31.
            let mut bits: u8 = 0;
            for lane in 0..8u32 {
                if (raw_mask >> (lane * 4 + 3)) & 1 == 1 {
                    bits |= 1 << lane;
                }
            }
            let base_bit = chunk * 8;
            let word_idx = base_bit / 64;
            let bit_offset = base_bit % 64;
            mask[word_idx] |= (bits as u64) << bit_offset;
            if bit_offset > 56 {
                mask[word_idx + 1] |= (bits as u64) >> (64 - bit_offset);
            }
        }

        for i in (chunks * 8)..values.len() {
            if values[i] == target {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }

        mask
    }
}

#[cfg(target_arch = "x86_64")]
pub(super) fn avx2_ne_u32(values: &[u32], target: u32) -> Vec<u64> {
    if values.len() < 16 {
        return scalar_ne_u32(values, target);
    }
    unsafe { avx2_ne_u32_inner(values, target) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_ne_u32_inner(values: &[u32], target: u32) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let mut mask = vec![0u64; words_for(values.len())];
        let target_vec = _mm256_set1_epi32(target as i32);
        let chunks = values.len() / 8;
        let ptr = values.as_ptr() as *const i32;

        for chunk in 0..chunks {
            let vals = _mm256_loadu_si256(ptr.add(chunk * 8).cast());
            let eq = _mm256_cmpeq_epi32(vals, target_vec);
            // Invert: XOR with all-ones for NOT-equal.
            let ne = _mm256_xor_si256(eq, _mm256_set1_epi32(-1));
            let raw_mask = _mm256_movemask_epi8(ne) as u32;
            let mut bits: u8 = 0;
            for lane in 0..8u32 {
                if (raw_mask >> (lane * 4 + 3)) & 1 == 1 {
                    bits |= 1 << lane;
                }
            }
            let base_bit = chunk * 8;
            let word_idx = base_bit / 64;
            let bit_offset = base_bit % 64;
            mask[word_idx] |= (bits as u64) << bit_offset;
            if bit_offset > 56 {
                mask[word_idx + 1] |= (bits as u64) >> (64 - bit_offset);
            }
        }

        for i in (chunks * 8)..values.len() {
            if values[i] != target {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }

        mask
    }
}

// ---------------------------------------------------------------------------
// AVX2 — f64
//
// Each AVX2 chunk processes 4 f64 lanes.
// _mm256_cmp_pd produces a 256-bit result; _mm256_movemask_pd extracts
// the sign bits of each 64-bit lane as a 4-bit integer.
// ---------------------------------------------------------------------------

// Immediate constants for _mm256_cmp_pd (VEX-encoded, same encoding as SSE4.2).
#[cfg(target_arch = "x86_64")]
const _CMP_GT_OQ_AVX2: i32 = 30;
#[cfg(target_arch = "x86_64")]
const _CMP_GE_OQ_AVX2: i32 = 29;
#[cfg(target_arch = "x86_64")]
const _CMP_LT_OQ_AVX2: i32 = 17;
#[cfg(target_arch = "x86_64")]
const _CMP_LE_OQ_AVX2: i32 = 18;

#[cfg(target_arch = "x86_64")]
pub(super) fn avx2_gt_f64(values: &[f64], threshold: f64) -> Vec<u64> {
    if values.len() < 8 {
        return scalar_cmp_f64(values, threshold, CmpOp::Gt);
    }
    unsafe { avx2_cmp_f64_inner::<{ _CMP_GT_OQ_AVX2 }>(values, threshold) }
}

#[cfg(target_arch = "x86_64")]
pub(super) fn avx2_gte_f64(values: &[f64], threshold: f64) -> Vec<u64> {
    if values.len() < 8 {
        return scalar_cmp_f64(values, threshold, CmpOp::Gte);
    }
    unsafe { avx2_cmp_f64_inner::<{ _CMP_GE_OQ_AVX2 }>(values, threshold) }
}

#[cfg(target_arch = "x86_64")]
pub(super) fn avx2_lt_f64(values: &[f64], threshold: f64) -> Vec<u64> {
    if values.len() < 8 {
        return scalar_cmp_f64(values, threshold, CmpOp::Lt);
    }
    unsafe { avx2_cmp_f64_inner::<{ _CMP_LT_OQ_AVX2 }>(values, threshold) }
}

#[cfg(target_arch = "x86_64")]
pub(super) fn avx2_lte_f64(values: &[f64], threshold: f64) -> Vec<u64> {
    if values.len() < 8 {
        return scalar_cmp_f64(values, threshold, CmpOp::Lte);
    }
    unsafe { avx2_cmp_f64_inner::<{ _CMP_LE_OQ_AVX2 }>(values, threshold) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_cmp_f64_inner<const IMM: i32>(values: &[f64], threshold: f64) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let mut mask = vec![0u64; words_for(values.len())];
        let thresh_vec = _mm256_set1_pd(threshold);
        let chunks = values.len() / 4;
        let ptr = values.as_ptr();

        for chunk in 0..chunks {
            let vals = _mm256_loadu_pd(ptr.add(chunk * 4));
            let cmp = _mm256_cmp_pd::<IMM>(vals, thresh_vec);
            // movemask_pd: 4-bit result, one bit per 64-bit lane.
            let bits = _mm256_movemask_pd(cmp) as u8;
            let base_bit = chunk * 4;
            let word_idx = base_bit / 64;
            let bit_offset = base_bit % 64;
            mask[word_idx] |= (bits as u64) << bit_offset;
            // Overflow: bit_offset + 4 > 64, i.e. bit_offset > 60.
            if bit_offset > 60 {
                mask[word_idx + 1] |= (bits as u64) >> (64 - bit_offset);
            }
        }

        // Scalar tail.
        let scalar_op = match IMM {
            _CMP_GT_OQ_AVX2 => CmpOp::Gt,
            _CMP_GE_OQ_AVX2 => CmpOp::Gte,
            _CMP_LT_OQ_AVX2 => CmpOp::Lt,
            _ => CmpOp::Lte,
        };
        for i in (chunks * 4)..values.len() {
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
// AVX2 — i64
//
// Each AVX2 chunk processes 4 i64 lanes.
// AVX2 has _mm256_cmpgt_epi64 but no cmpge/cmple/cmplt directly.
// - gt:  _mm256_cmpgt_epi64(vals, thresh)
// - lt:  _mm256_cmpgt_epi64(thresh, vals)   (swap operands)
// - gte: gt OR eq  (cmpgt | cmpeq)
// - lte: lt OR eq  (swap-cmpgt | cmpeq)
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
pub(super) fn avx2_gt_i64(values: &[i64], threshold: i64) -> Vec<u64> {
    if values.len() < 8 {
        return scalar_cmp_i64(values, threshold, CmpOp::Gt);
    }
    unsafe { avx2_gt_i64_inner(values, threshold) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_gt_i64_inner(values: &[i64], threshold: i64) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let mut mask = vec![0u64; words_for(values.len())];
        let thresh_vec = _mm256_set1_epi64x(threshold);
        let chunks = values.len() / 4;
        let ptr = values.as_ptr();

        for chunk in 0..chunks {
            let vals = _mm256_loadu_si256(ptr.add(chunk * 4).cast());
            let cmp = _mm256_cmpgt_epi64(vals, thresh_vec);
            // Cast to f64 vector to use movemask_pd (extracts sign bits of 64-bit lanes).
            let bits = _mm256_movemask_pd(_mm256_castsi256_pd(cmp)) as u8;
            let base_bit = chunk * 4;
            let word_idx = base_bit / 64;
            let bit_offset = base_bit % 64;
            mask[word_idx] |= (bits as u64) << bit_offset;
            if bit_offset > 60 {
                mask[word_idx + 1] |= (bits as u64) >> (64 - bit_offset);
            }
        }

        for i in (chunks * 4)..values.len() {
            if values[i] > threshold {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }

        mask
    }
}

#[cfg(target_arch = "x86_64")]
pub(super) fn avx2_gte_i64(values: &[i64], threshold: i64) -> Vec<u64> {
    if values.len() < 8 {
        return scalar_cmp_i64(values, threshold, CmpOp::Gte);
    }
    unsafe { avx2_gte_i64_inner(values, threshold) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_gte_i64_inner(values: &[i64], threshold: i64) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let mut mask = vec![0u64; words_for(values.len())];
        let thresh_vec = _mm256_set1_epi64x(threshold);
        let chunks = values.len() / 4;
        let ptr = values.as_ptr();

        for chunk in 0..chunks {
            let vals = _mm256_loadu_si256(ptr.add(chunk * 4).cast());
            // gte = (vals > thresh) OR (vals == thresh).
            let gt = _mm256_cmpgt_epi64(vals, thresh_vec);
            let eq = _mm256_cmpeq_epi64(vals, thresh_vec);
            let cmp = _mm256_or_si256(gt, eq);
            let bits = _mm256_movemask_pd(_mm256_castsi256_pd(cmp)) as u8;
            let base_bit = chunk * 4;
            let word_idx = base_bit / 64;
            let bit_offset = base_bit % 64;
            mask[word_idx] |= (bits as u64) << bit_offset;
            if bit_offset > 60 {
                mask[word_idx + 1] |= (bits as u64) >> (64 - bit_offset);
            }
        }

        for i in (chunks * 4)..values.len() {
            if values[i] >= threshold {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }

        mask
    }
}

#[cfg(target_arch = "x86_64")]
pub(super) fn avx2_lt_i64(values: &[i64], threshold: i64) -> Vec<u64> {
    if values.len() < 8 {
        return scalar_cmp_i64(values, threshold, CmpOp::Lt);
    }
    unsafe { avx2_lt_i64_inner(values, threshold) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_lt_i64_inner(values: &[i64], threshold: i64) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let mut mask = vec![0u64; words_for(values.len())];
        let thresh_vec = _mm256_set1_epi64x(threshold);
        let chunks = values.len() / 4;
        let ptr = values.as_ptr();

        for chunk in 0..chunks {
            let vals = _mm256_loadu_si256(ptr.add(chunk * 4).cast());
            // lt = thresh > vals  (swap operands to cmpgt).
            let cmp = _mm256_cmpgt_epi64(thresh_vec, vals);
            let bits = _mm256_movemask_pd(_mm256_castsi256_pd(cmp)) as u8;
            let base_bit = chunk * 4;
            let word_idx = base_bit / 64;
            let bit_offset = base_bit % 64;
            mask[word_idx] |= (bits as u64) << bit_offset;
            if bit_offset > 60 {
                mask[word_idx + 1] |= (bits as u64) >> (64 - bit_offset);
            }
        }

        for i in (chunks * 4)..values.len() {
            if values[i] < threshold {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }

        mask
    }
}

#[cfg(target_arch = "x86_64")]
pub(super) fn avx2_lte_i64(values: &[i64], threshold: i64) -> Vec<u64> {
    if values.len() < 8 {
        return scalar_cmp_i64(values, threshold, CmpOp::Lte);
    }
    unsafe { avx2_lte_i64_inner(values, threshold) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_lte_i64_inner(values: &[i64], threshold: i64) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let mut mask = vec![0u64; words_for(values.len())];
        let thresh_vec = _mm256_set1_epi64x(threshold);
        let chunks = values.len() / 4;
        let ptr = values.as_ptr();

        for chunk in 0..chunks {
            let vals = _mm256_loadu_si256(ptr.add(chunk * 4).cast());
            // lte = (thresh > vals) OR (vals == thresh).
            let lt = _mm256_cmpgt_epi64(thresh_vec, vals);
            let eq = _mm256_cmpeq_epi64(vals, thresh_vec);
            let cmp = _mm256_or_si256(lt, eq);
            let bits = _mm256_movemask_pd(_mm256_castsi256_pd(cmp)) as u8;
            let base_bit = chunk * 4;
            let word_idx = base_bit / 64;
            let bit_offset = base_bit % 64;
            mask[word_idx] |= (bits as u64) << bit_offset;
            if bit_offset > 60 {
                mask[word_idx + 1] |= (bits as u64) >> (64 - bit_offset);
            }
        }

        for i in (chunks * 4)..values.len() {
            if values[i] <= threshold {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }

        mask
    }
}

/// Fused AVX2 range: `min <= value <= max` in one pass.
#[cfg(target_arch = "x86_64")]
pub(super) fn avx2_range_i64(values: &[i64], min: i64, max: i64) -> Vec<u64> {
    if values.len() < 8 {
        return scalar_range_i64(values, min, max);
    }
    unsafe { avx2_range_i64_inner(values, min, max) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_range_i64_inner(values: &[i64], min: i64, max: i64) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let mut mask = vec![0u64; words_for(values.len())];
        let min_vec = _mm256_set1_epi64x(min);
        let max_vec = _mm256_set1_epi64x(max);
        let chunks = values.len() / 4;
        let ptr = values.as_ptr();

        for chunk in 0..chunks {
            let vals = _mm256_loadu_si256(ptr.add(chunk * 4).cast());

            // gte_min = (vals > min) OR (vals == min)
            let gt_min = _mm256_cmpgt_epi64(vals, min_vec);
            let eq_min = _mm256_cmpeq_epi64(vals, min_vec);
            let gte_min = _mm256_or_si256(gt_min, eq_min);

            // lte_max = (max > vals) OR (vals == max)
            let lt_max = _mm256_cmpgt_epi64(max_vec, vals);
            let eq_max = _mm256_cmpeq_epi64(vals, max_vec);
            let lte_max = _mm256_or_si256(lt_max, eq_max);

            let cmp = _mm256_and_si256(gte_min, lte_max);
            let bits = _mm256_movemask_pd(_mm256_castsi256_pd(cmp)) as u8;
            let base_bit = chunk * 4;
            let word_idx = base_bit / 64;
            let bit_offset = base_bit % 64;
            mask[word_idx] |= (bits as u64) << bit_offset;
            if bit_offset > 60 {
                mask[word_idx + 1] |= (bits as u64) >> (64 - bit_offset);
            }
        }

        for i in (chunks * 4)..values.len() {
            if values[i] >= min && values[i] <= max {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }

        mask
    }
}
