// ---------------------------------------------------------------------------
// Comparison op enum (for scalar generic dispatch)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
pub(crate) enum CmpOp {
    Gt,
    Gte,
    Lt,
    Lte,
}

// ---------------------------------------------------------------------------
// Scalar fallback
// ---------------------------------------------------------------------------

pub(crate) fn scalar_eq_u32(values: &[u32], target: u32) -> Vec<u64> {
    let mut mask = vec![0u64; super::bitmask::words_for(values.len())];
    for (i, &v) in values.iter().enumerate() {
        if v == target {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

pub(crate) fn scalar_ne_u32(values: &[u32], target: u32) -> Vec<u64> {
    let mut mask = vec![0u64; super::bitmask::words_for(values.len())];
    for (i, &v) in values.iter().enumerate() {
        if v != target {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

pub(crate) fn scalar_cmp_f64(values: &[f64], threshold: f64, op: CmpOp) -> Vec<u64> {
    let mut mask = vec![0u64; super::bitmask::words_for(values.len())];
    for (i, &v) in values.iter().enumerate() {
        let pass = match op {
            CmpOp::Gt => v > threshold,
            CmpOp::Gte => v >= threshold,
            CmpOp::Lt => v < threshold,
            CmpOp::Lte => v <= threshold,
        };
        if pass {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

pub(crate) fn scalar_cmp_i64(values: &[i64], threshold: i64, op: CmpOp) -> Vec<u64> {
    let mut mask = vec![0u64; super::bitmask::words_for(values.len())];
    for (i, &v) in values.iter().enumerate() {
        let pass = match op {
            CmpOp::Gt => v > threshold,
            CmpOp::Gte => v >= threshold,
            CmpOp::Lt => v < threshold,
            CmpOp::Lte => v <= threshold,
        };
        if pass {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

pub(crate) fn scalar_range_i64(values: &[i64], min: i64, max: i64) -> Vec<u64> {
    let mut mask = vec![0u64; super::bitmask::words_for(values.len())];
    for (i, &v) in values.iter().enumerate() {
        if v >= min && v <= max {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}
