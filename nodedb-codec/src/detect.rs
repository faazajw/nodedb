//! Codec auto-detection from column type and data distribution.
//!
//! Analyzes up to the first 1000 values of a column to select the optimal
//! codec. Called at flush time when `ColumnCodec::Auto` is configured.

use crate::{ColumnCodec, ColumnTypeHint};

/// Maximum number of values to sample for codec detection.
const SAMPLE_SIZE: usize = 1000;

/// Detect the optimal codec for a column based on its type and data.
///
/// When `codec` is not `Auto`, returns it unchanged. When `Auto`,
/// analyzes the column type hint and optionally the raw data to select
/// the best codec.
pub fn detect_codec(codec: ColumnCodec, type_hint: ColumnTypeHint) -> ColumnCodec {
    if codec != ColumnCodec::Auto {
        return codec;
    }

    match type_hint {
        ColumnTypeHint::Timestamp => ColumnCodec::DoubleDelta,
        ColumnTypeHint::Float64 => ColumnCodec::Gorilla,
        ColumnTypeHint::Int64 => ColumnCodec::Delta,
        ColumnTypeHint::Symbol => ColumnCodec::Raw,
        ColumnTypeHint::String => ColumnCodec::Lz4,
    }
}

/// Detect the optimal codec for an i64 column by analyzing the data.
///
/// Checks if the column is monotonic (use Delta) or non-monotonic
/// (use Gorilla). Falls back to `Delta` if the data is mostly monotonic
/// (>90% of deltas are non-negative).
pub fn detect_i64_codec(values: &[i64]) -> ColumnCodec {
    if values.len() < 2 {
        return ColumnCodec::Delta;
    }

    let sample_end = values.len().min(SAMPLE_SIZE);
    let sample = &values[..sample_end];

    let mut positive_deltas = 0usize;
    let mut zero_dod_count = 0usize;
    let mut prev_delta: Option<i64> = None;

    for i in 1..sample.len() {
        let delta = sample[i] - sample[i - 1];
        if delta >= 0 {
            positive_deltas += 1;
        }
        if let Some(pd) = prev_delta
            && delta == pd
        {
            zero_dod_count += 1;
        }
        prev_delta = Some(delta);
    }

    let total_deltas = sample.len() - 1;
    let monotonic_ratio = positive_deltas as f64 / total_deltas as f64;
    let constant_rate_ratio = zero_dod_count as f64 / total_deltas.max(1) as f64;

    // If >80% of delta-of-deltas are zero, this is timestamp-like → DoubleDelta.
    if constant_rate_ratio > 0.8 {
        return ColumnCodec::DoubleDelta;
    }

    // If >90% of deltas are non-negative, it's a counter → Delta.
    if monotonic_ratio > 0.9 {
        return ColumnCodec::Delta;
    }

    // Non-monotonic integer → Delta still works but with less compression.
    // Gorilla can work too for i64 via f64 cast, but Delta with zigzag
    // handles small fluctuations better than Gorilla for integers.
    ColumnCodec::Delta
}

/// Detect the optimal codec for an f64 column by analyzing the data.
///
/// Almost always returns Gorilla, which is the best general-purpose
/// codec for floating-point metrics. Could return DoubleDelta if the
/// values are actually integers stored as f64 with constant deltas,
/// but that's rare enough not to optimize for.
pub fn detect_f64_codec(values: &[f64]) -> ColumnCodec {
    if values.len() < 2 {
        return ColumnCodec::Gorilla;
    }

    // Check if all values are identical (common for status/flag columns
    // stored as f64). Gorilla compresses these to 1 bit per value.
    let sample_end = values.len().min(SAMPLE_SIZE);
    let sample = &values[..sample_end];

    let all_same = sample.iter().all(|&v| v.to_bits() == sample[0].to_bits());
    if all_same {
        // Gorilla handles identical values extremely well (1 bit each).
        return ColumnCodec::Gorilla;
    }

    // Default: Gorilla is the best general-purpose f64 codec.
    ColumnCodec::Gorilla
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_codec_passthrough() {
        assert_eq!(
            detect_codec(ColumnCodec::Lz4, ColumnTypeHint::Timestamp),
            ColumnCodec::Lz4
        );
        assert_eq!(
            detect_codec(ColumnCodec::Zstd, ColumnTypeHint::Float64),
            ColumnCodec::Zstd
        );
    }

    #[test]
    fn auto_timestamp() {
        assert_eq!(
            detect_codec(ColumnCodec::Auto, ColumnTypeHint::Timestamp),
            ColumnCodec::DoubleDelta
        );
    }

    #[test]
    fn auto_float64() {
        assert_eq!(
            detect_codec(ColumnCodec::Auto, ColumnTypeHint::Float64),
            ColumnCodec::Gorilla
        );
    }

    #[test]
    fn auto_int64() {
        assert_eq!(
            detect_codec(ColumnCodec::Auto, ColumnTypeHint::Int64),
            ColumnCodec::Delta
        );
    }

    #[test]
    fn auto_symbol() {
        assert_eq!(
            detect_codec(ColumnCodec::Auto, ColumnTypeHint::Symbol),
            ColumnCodec::Raw
        );
    }

    #[test]
    fn auto_string() {
        assert_eq!(
            detect_codec(ColumnCodec::Auto, ColumnTypeHint::String),
            ColumnCodec::Lz4
        );
    }

    #[test]
    fn detect_constant_increment_counter() {
        // Constant increment (all dod=0) → DoubleDelta is optimal.
        let values: Vec<i64> = (0..1000).map(|i| i * 100).collect();
        assert_eq!(detect_i64_codec(&values), ColumnCodec::DoubleDelta);
    }

    #[test]
    fn detect_varying_increment_counter() {
        // Monotonic but varying increments → Delta.
        let mut values = Vec::with_capacity(1000);
        let mut v = 0i64;
        let mut rng: u64 = 42;
        for _ in 0..1000 {
            values.push(v);
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            v += ((rng >> 33) as i64 % 100) + 1; // always positive, varying
        }
        assert_eq!(detect_i64_codec(&values), ColumnCodec::Delta);
    }

    #[test]
    fn detect_constant_rate_timestamps() {
        let values: Vec<i64> = (0..1000).map(|i| 1_700_000_000_000 + i * 10_000).collect();
        assert_eq!(detect_i64_codec(&values), ColumnCodec::DoubleDelta);
    }

    #[test]
    fn detect_fluctuating_gauge() {
        // Values that go up and down — still Delta.
        let mut values = Vec::with_capacity(1000);
        let mut v = 50i64;
        let mut rng: u64 = 42;
        for _ in 0..1000 {
            values.push(v);
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            v += ((rng >> 33) as i64 % 11) - 5;
        }
        let codec = detect_i64_codec(&values);
        assert_eq!(codec, ColumnCodec::Delta);
    }

    #[test]
    fn detect_f64_default() {
        let values: Vec<f64> = (0..100).map(|i| 50.0 + i as f64 * 0.1).collect();
        assert_eq!(detect_f64_codec(&values), ColumnCodec::Gorilla);
    }

    #[test]
    fn detect_f64_identical() {
        let values = vec![42.0f64; 100];
        assert_eq!(detect_f64_codec(&values), ColumnCodec::Gorilla);
    }

    #[test]
    fn small_sample() {
        assert_eq!(detect_i64_codec(&[]), ColumnCodec::Delta);
        assert_eq!(detect_i64_codec(&[42]), ColumnCodec::Delta);
        assert_eq!(detect_f64_codec(&[]), ColumnCodec::Gorilla);
    }
}
