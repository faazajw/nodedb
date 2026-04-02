// ---------------------------------------------------------------------------
// Runtime dispatch
// ---------------------------------------------------------------------------

use super::scalar::{
    CmpOp, scalar_cmp_f64, scalar_cmp_i64, scalar_eq_u32, scalar_ne_u32, scalar_range_i64,
};

/// SIMD runtime for filter-to-bitmask operations.
pub struct FilterSimdRuntime {
    /// `values[i] == target` → bit i set.
    pub eq_u32: fn(&[u32], u32) -> Vec<u64>,
    /// `values[i] != target` → bit i set.
    pub ne_u32: fn(&[u32], u32) -> Vec<u64>,
    /// `values[i] > threshold` → bit i set.
    pub gt_f64: fn(&[f64], f64) -> Vec<u64>,
    /// `values[i] >= threshold` → bit i set.
    pub gte_f64: fn(&[f64], f64) -> Vec<u64>,
    /// `values[i] < threshold` → bit i set.
    pub lt_f64: fn(&[f64], f64) -> Vec<u64>,
    /// `values[i] <= threshold` → bit i set.
    pub lte_f64: fn(&[f64], f64) -> Vec<u64>,
    /// `values[i] > threshold` → bit i set (i64).
    pub gt_i64: fn(&[i64], i64) -> Vec<u64>,
    /// `values[i] >= threshold` → bit i set (i64).
    pub gte_i64: fn(&[i64], i64) -> Vec<u64>,
    /// `values[i] < threshold` → bit i set (i64).
    pub lt_i64: fn(&[i64], i64) -> Vec<u64>,
    /// `values[i] <= threshold` → bit i set (i64).
    pub lte_i64: fn(&[i64], i64) -> Vec<u64>,
    /// `min <= values[i] <= max` → bit i set.
    pub range_i64: fn(&[i64], i64, i64) -> Vec<u64>,
    pub name: &'static str,
}

impl FilterSimdRuntime {
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512f") {
                return Self {
                    eq_u32: super::avx512::avx512_eq_u32,
                    ne_u32: super::avx512::avx512_ne_u32,
                    gt_f64: super::avx512::avx512_gt_f64,
                    gte_f64: super::avx512::avx512_gte_f64,
                    lt_f64: super::avx512::avx512_lt_f64,
                    lte_f64: super::avx512::avx512_lte_f64,
                    gt_i64: super::avx512::avx512_gt_i64,
                    gte_i64: super::avx512::avx512_gte_i64,
                    lt_i64: super::avx512::avx512_lt_i64,
                    lte_i64: super::avx512::avx512_lte_i64,
                    range_i64: super::avx512::avx512_range_i64,
                    name: "avx512",
                };
            }
            if std::is_x86_feature_detected!("avx2") {
                return Self {
                    eq_u32: super::avx2::avx2_eq_u32,
                    ne_u32: super::avx2::avx2_ne_u32,
                    gt_f64: super::avx2::avx2_gt_f64,
                    gte_f64: super::avx2::avx2_gte_f64,
                    lt_f64: super::avx2::avx2_lt_f64,
                    lte_f64: super::avx2::avx2_lte_f64,
                    gt_i64: super::avx2::avx2_gt_i64,
                    gte_i64: super::avx2::avx2_gte_i64,
                    lt_i64: super::avx2::avx2_lt_i64,
                    lte_i64: super::avx2::avx2_lte_i64,
                    range_i64: super::avx2::avx2_range_i64,
                    name: "avx2",
                };
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            return Self {
                eq_u32: super::neon::neon_eq_u32,
                ne_u32: super::neon::neon_ne_u32,
                gt_f64: super::neon::neon_gt_f64,
                gte_f64: super::neon::neon_gte_f64,
                lt_f64: super::neon::neon_lt_f64,
                lte_f64: super::neon::neon_lte_f64,
                gt_i64: super::neon::neon_gt_i64,
                gte_i64: super::neon::neon_gte_i64,
                lt_i64: super::neon::neon_lt_i64,
                lte_i64: super::neon::neon_lte_i64,
                range_i64: super::neon::neon_range_i64,
                name: "neon",
            };
        }
        #[cfg(target_arch = "wasm32")]
        {
            return Self {
                eq_u32: super::wasm::wasm_eq_u32,
                ne_u32: super::wasm::wasm_ne_u32,
                gt_f64: |v, t| scalar_cmp_f64(v, t, CmpOp::Gt),
                gte_f64: |v, t| scalar_cmp_f64(v, t, CmpOp::Gte),
                lt_f64: |v, t| scalar_cmp_f64(v, t, CmpOp::Lt),
                lte_f64: |v, t| scalar_cmp_f64(v, t, CmpOp::Lte),
                gt_i64: |v, t| scalar_cmp_i64(v, t, CmpOp::Gt),
                gte_i64: |v, t| scalar_cmp_i64(v, t, CmpOp::Gte),
                lt_i64: |v, t| scalar_cmp_i64(v, t, CmpOp::Lt),
                lte_i64: |v, t| scalar_cmp_i64(v, t, CmpOp::Lte),
                range_i64: scalar_range_i64,
                name: "wasm-simd128",
            };
        }
        #[allow(unreachable_code)]
        Self {
            eq_u32: scalar_eq_u32,
            ne_u32: scalar_ne_u32,
            gt_f64: |v, t| scalar_cmp_f64(v, t, CmpOp::Gt),
            gte_f64: |v, t| scalar_cmp_f64(v, t, CmpOp::Gte),
            lt_f64: |v, t| scalar_cmp_f64(v, t, CmpOp::Lt),
            lte_f64: |v, t| scalar_cmp_f64(v, t, CmpOp::Lte),
            gt_i64: |v, t| scalar_cmp_i64(v, t, CmpOp::Gt),
            gte_i64: |v, t| scalar_cmp_i64(v, t, CmpOp::Gte),
            lt_i64: |v, t| scalar_cmp_i64(v, t, CmpOp::Lt),
            lte_i64: |v, t| scalar_cmp_i64(v, t, CmpOp::Lte),
            range_i64: scalar_range_i64,
            name: "scalar",
        }
    }
}

static FILTER_RUNTIME: std::sync::OnceLock<FilterSimdRuntime> = std::sync::OnceLock::new();

/// Get the global filter SIMD runtime.
pub fn filter_runtime() -> &'static FilterSimdRuntime {
    FILTER_RUNTIME.get_or_init(FilterSimdRuntime::detect)
}
