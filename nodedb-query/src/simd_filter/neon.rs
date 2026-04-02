// ---------------------------------------------------------------------------
// NEON (AArch64) filter kernels — 4 u32 / 2 f64 / 2 i64 per instruction
// ---------------------------------------------------------------------------

#[cfg(target_arch = "aarch64")]
use super::bitmask::words_for;
#[cfg(target_arch = "aarch64")]
use super::scalar::{
    CmpOp, scalar_cmp_f64, scalar_cmp_i64, scalar_eq_u32, scalar_ne_u32, scalar_range_i64,
};

#[cfg(target_arch = "aarch64")]
pub(super) fn neon_eq_u32(values: &[u32], target: u32) -> Vec<u64> {
    use std::arch::aarch64::*;
    if values.len() < 8 {
        return scalar_eq_u32(values, target);
    }
    let mut mask = vec![0u64; words_for(values.len())];
    let target_vec = unsafe { vdupq_n_u32(target) };
    let chunks = values.len() / 4;
    for chunk in 0..chunks {
        let vals = unsafe { vld1q_u32(values.as_ptr().add(chunk * 4)) };
        let cmp = unsafe { vceqq_u32(vals, target_vec) };
        // Extract comparison result: each lane is 0xFFFFFFFF or 0x00000000
        let mut buf = [0u32; 4];
        unsafe { vst1q_u32(buf.as_mut_ptr(), cmp) };
        let base = chunk * 4;
        for (lane, &v) in buf.iter().enumerate() {
            if v != 0 {
                let idx = base + lane;
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

#[cfg(target_arch = "aarch64")]
pub(super) fn neon_ne_u32(values: &[u32], target: u32) -> Vec<u64> {
    use std::arch::aarch64::*;
    if values.len() < 8 {
        return scalar_ne_u32(values, target);
    }
    let mut mask = vec![0u64; words_for(values.len())];
    let target_vec = unsafe { vdupq_n_u32(target) };
    let chunks = values.len() / 4;
    for chunk in 0..chunks {
        let vals = unsafe { vld1q_u32(values.as_ptr().add(chunk * 4)) };
        let eq = unsafe { vceqq_u32(vals, target_vec) };
        let ne = unsafe { vmvnq_u32(eq) };
        let mut buf = [0u32; 4];
        unsafe { vst1q_u32(buf.as_mut_ptr(), ne) };
        let base = chunk * 4;
        for (lane, &v) in buf.iter().enumerate() {
            if v != 0 {
                let idx = base + lane;
                mask[idx / 64] |= 1u64 << (idx % 64);
            }
        }
    }
    for i in (chunks * 4)..values.len() {
        if values[i] != target {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

#[cfg(target_arch = "aarch64")]
pub(super) fn neon_gt_f64(values: &[f64], threshold: f64) -> Vec<u64> {
    use std::arch::aarch64::*;
    if values.len() < 4 {
        return scalar_cmp_f64(values, threshold, CmpOp::Gt);
    }
    let mut mask = vec![0u64; words_for(values.len())];
    let thresh = unsafe { vdupq_n_f64(threshold) };
    let chunks = values.len() / 2;
    for chunk in 0..chunks {
        let vals = unsafe { vld1q_f64(values.as_ptr().add(chunk * 2)) };
        let cmp = unsafe { vcgtq_f64(vals, thresh) };
        let mut buf = [0u64; 2];
        unsafe { vst1q_u64(buf.as_mut_ptr(), cmp) };
        let base = chunk * 2;
        for (lane, &v) in buf.iter().enumerate() {
            if v != 0 {
                let idx = base + lane;
                mask[idx / 64] |= 1u64 << (idx % 64);
            }
        }
    }
    for i in (chunks * 2)..values.len() {
        if values[i] > threshold {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

#[cfg(target_arch = "aarch64")]
pub(super) fn neon_gte_f64(values: &[f64], threshold: f64) -> Vec<u64> {
    use std::arch::aarch64::*;
    if values.len() < 4 {
        return scalar_cmp_f64(values, threshold, CmpOp::Gte);
    }
    let mut mask = vec![0u64; words_for(values.len())];
    let thresh = unsafe { vdupq_n_f64(threshold) };
    let chunks = values.len() / 2;
    for chunk in 0..chunks {
        let vals = unsafe { vld1q_f64(values.as_ptr().add(chunk * 2)) };
        let cmp = unsafe { vcgeq_f64(vals, thresh) };
        let mut buf = [0u64; 2];
        unsafe { vst1q_u64(buf.as_mut_ptr(), cmp) };
        let base = chunk * 2;
        for (lane, &v) in buf.iter().enumerate() {
            if v != 0 {
                let idx = base + lane;
                mask[idx / 64] |= 1u64 << (idx % 64);
            }
        }
    }
    for i in (chunks * 2)..values.len() {
        if values[i] >= threshold {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

#[cfg(target_arch = "aarch64")]
pub(super) fn neon_lt_f64(values: &[f64], threshold: f64) -> Vec<u64> {
    use std::arch::aarch64::*;
    if values.len() < 4 {
        return scalar_cmp_f64(values, threshold, CmpOp::Lt);
    }
    let mut mask = vec![0u64; words_for(values.len())];
    let thresh = unsafe { vdupq_n_f64(threshold) };
    let chunks = values.len() / 2;
    for chunk in 0..chunks {
        let vals = unsafe { vld1q_f64(values.as_ptr().add(chunk * 2)) };
        let cmp = unsafe { vcltq_f64(vals, thresh) };
        let mut buf = [0u64; 2];
        unsafe { vst1q_u64(buf.as_mut_ptr(), cmp) };
        let base = chunk * 2;
        for (lane, &v) in buf.iter().enumerate() {
            if v != 0 {
                let idx = base + lane;
                mask[idx / 64] |= 1u64 << (idx % 64);
            }
        }
    }
    for i in (chunks * 2)..values.len() {
        if values[i] < threshold {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

#[cfg(target_arch = "aarch64")]
pub(super) fn neon_lte_f64(values: &[f64], threshold: f64) -> Vec<u64> {
    use std::arch::aarch64::*;
    if values.len() < 4 {
        return scalar_cmp_f64(values, threshold, CmpOp::Lte);
    }
    let mut mask = vec![0u64; words_for(values.len())];
    let thresh = unsafe { vdupq_n_f64(threshold) };
    let chunks = values.len() / 2;
    for chunk in 0..chunks {
        let vals = unsafe { vld1q_f64(values.as_ptr().add(chunk * 2)) };
        let cmp = unsafe { vcleq_f64(vals, thresh) };
        let mut buf = [0u64; 2];
        unsafe { vst1q_u64(buf.as_mut_ptr(), cmp) };
        let base = chunk * 2;
        for (lane, &v) in buf.iter().enumerate() {
            if v != 0 {
                let idx = base + lane;
                mask[idx / 64] |= 1u64 << (idx % 64);
            }
        }
    }
    for i in (chunks * 2)..values.len() {
        if values[i] <= threshold {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

#[cfg(target_arch = "aarch64")]
pub(super) fn neon_gt_i64(values: &[i64], threshold: i64) -> Vec<u64> {
    use std::arch::aarch64::*;
    if values.len() < 4 {
        return scalar_cmp_i64(values, threshold, CmpOp::Gt);
    }
    let mut mask = vec![0u64; words_for(values.len())];
    let thresh = unsafe { vdupq_n_s64(threshold) };
    let chunks = values.len() / 2;
    for chunk in 0..chunks {
        let vals = unsafe { vld1q_s64(values.as_ptr().add(chunk * 2)) };
        let cmp = unsafe { vcgtq_s64(vals, thresh) };
        let mut buf = [0u64; 2];
        unsafe { vst1q_u64(buf.as_mut_ptr(), cmp) };
        let base = chunk * 2;
        for (lane, &v) in buf.iter().enumerate() {
            if v != 0 {
                let idx = base + lane;
                mask[idx / 64] |= 1u64 << (idx % 64);
            }
        }
    }
    for i in (chunks * 2)..values.len() {
        if values[i] > threshold {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

#[cfg(target_arch = "aarch64")]
pub(super) fn neon_gte_i64(values: &[i64], threshold: i64) -> Vec<u64> {
    use std::arch::aarch64::*;
    if values.len() < 4 {
        return scalar_cmp_i64(values, threshold, CmpOp::Gte);
    }
    let mut mask = vec![0u64; words_for(values.len())];
    let thresh = unsafe { vdupq_n_s64(threshold) };
    let chunks = values.len() / 2;
    for chunk in 0..chunks {
        let vals = unsafe { vld1q_s64(values.as_ptr().add(chunk * 2)) };
        let cmp = unsafe { vcgeq_s64(vals, thresh) };
        let mut buf = [0u64; 2];
        unsafe { vst1q_u64(buf.as_mut_ptr(), cmp) };
        let base = chunk * 2;
        for (lane, &v) in buf.iter().enumerate() {
            if v != 0 {
                let idx = base + lane;
                mask[idx / 64] |= 1u64 << (idx % 64);
            }
        }
    }
    for i in (chunks * 2)..values.len() {
        if values[i] >= threshold {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

#[cfg(target_arch = "aarch64")]
pub(super) fn neon_lt_i64(values: &[i64], threshold: i64) -> Vec<u64> {
    use std::arch::aarch64::*;
    if values.len() < 4 {
        return scalar_cmp_i64(values, threshold, CmpOp::Lt);
    }
    let mut mask = vec![0u64; words_for(values.len())];
    let thresh = unsafe { vdupq_n_s64(threshold) };
    let chunks = values.len() / 2;
    for chunk in 0..chunks {
        let vals = unsafe { vld1q_s64(values.as_ptr().add(chunk * 2)) };
        let cmp = unsafe { vcltq_s64(vals, thresh) };
        let mut buf = [0u64; 2];
        unsafe { vst1q_u64(buf.as_mut_ptr(), cmp) };
        let base = chunk * 2;
        for (lane, &v) in buf.iter().enumerate() {
            if v != 0 {
                let idx = base + lane;
                mask[idx / 64] |= 1u64 << (idx % 64);
            }
        }
    }
    for i in (chunks * 2)..values.len() {
        if values[i] < threshold {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

#[cfg(target_arch = "aarch64")]
pub(super) fn neon_lte_i64(values: &[i64], threshold: i64) -> Vec<u64> {
    use std::arch::aarch64::*;
    if values.len() < 4 {
        return scalar_cmp_i64(values, threshold, CmpOp::Lte);
    }
    let mut mask = vec![0u64; words_for(values.len())];
    let thresh = unsafe { vdupq_n_s64(threshold) };
    let chunks = values.len() / 2;
    for chunk in 0..chunks {
        let vals = unsafe { vld1q_s64(values.as_ptr().add(chunk * 2)) };
        let cmp = unsafe { vcleq_s64(vals, thresh) };
        let mut buf = [0u64; 2];
        unsafe { vst1q_u64(buf.as_mut_ptr(), cmp) };
        let base = chunk * 2;
        for (lane, &v) in buf.iter().enumerate() {
            if v != 0 {
                let idx = base + lane;
                mask[idx / 64] |= 1u64 << (idx % 64);
            }
        }
    }
    for i in (chunks * 2)..values.len() {
        if values[i] <= threshold {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

#[cfg(target_arch = "aarch64")]
pub(super) fn neon_range_i64(values: &[i64], min: i64, max: i64) -> Vec<u64> {
    use std::arch::aarch64::*;
    if values.len() < 4 {
        return scalar_range_i64(values, min, max);
    }
    let mut mask = vec![0u64; words_for(values.len())];
    let min_vec = unsafe { vdupq_n_s64(min) };
    let max_vec = unsafe { vdupq_n_s64(max) };
    let chunks = values.len() / 2;
    for chunk in 0..chunks {
        let vals = unsafe { vld1q_s64(values.as_ptr().add(chunk * 2)) };
        let gte_min = unsafe { vcgeq_s64(vals, min_vec) };
        let lte_max = unsafe { vcleq_s64(vals, max_vec) };
        let both = unsafe { vandq_u64(gte_min, lte_max) };
        let mut buf = [0u64; 2];
        unsafe { vst1q_u64(buf.as_mut_ptr(), both) };
        let base = chunk * 2;
        for (lane, &v) in buf.iter().enumerate() {
            if v != 0 {
                let idx = base + lane;
                mask[idx / 64] |= 1u64 << (idx % 64);
            }
        }
    }
    for i in (chunks * 2)..values.len() {
        if values[i] >= min && values[i] <= max {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}
