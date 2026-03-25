//! Compression codecs for NodeDB timeseries columnar storage.
//!
//! Provides per-column codec selection: each column in a partition can use
//! a different compression algorithm optimized for its data distribution.
//!
//! Shared by Origin and Lite. Compiles to WASM (lz4_flex is pure Rust,
//! Zstd uses `ruzstd` on WASM targets).

pub mod delta;
pub mod detect;
pub mod double_delta;
pub mod error;
pub mod gorilla;
pub mod lz4;
pub mod raw;
pub mod zstd_codec;

pub use delta::{DeltaDecoder, DeltaEncoder};
pub use detect::detect_codec;
pub use double_delta::{DoubleDeltaDecoder, DoubleDeltaEncoder};
pub use error::CodecError;
pub use gorilla::{GorillaDecoder, GorillaEncoder};
pub use lz4::{Lz4Decoder, Lz4Encoder};
pub use raw::{RawDecoder, RawEncoder};
pub use zstd_codec::{ZstdDecoder, ZstdEncoder};

use serde::{Deserialize, Serialize};

/// Codec identifier for per-column compression selection.
///
/// Stored in partition schema metadata so the reader knows which decoder
/// to use for each column file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnCodec {
    /// Engine selects codec automatically based on column type and data
    /// distribution (analyzed at flush time).
    Auto,
    /// Gorilla XOR encoding — best for floating-point metrics.
    Gorilla,
    /// DoubleDelta — best for monotonic timestamps with near-constant intervals.
    DoubleDelta,
    /// Delta + varint — best for monotonic counters.
    Delta,
    /// LZ4 block compression — best for string/log columns.
    Lz4,
    /// Zstd — best for cold/archived partitions.
    Zstd,
    /// No compression — for symbol columns (already dictionary-encoded).
    Raw,
}

impl ColumnCodec {
    pub fn is_compressed(&self) -> bool {
        !matches!(self, Self::Raw | Self::Auto)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Gorilla => "gorilla",
            Self::DoubleDelta => "double_delta",
            Self::Delta => "delta",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
            Self::Raw => "raw",
        }
    }
}

impl std::fmt::Display for ColumnCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Column data type hint for codec auto-detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnTypeHint {
    Timestamp,
    Float64,
    Int64,
    Symbol,
    String,
}

/// Per-column statistics computed at flush time.
///
/// Stored in partition metadata for predicate pushdown and approximate
/// query answers without decompression.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Codec used for this column in this partition.
    pub codec: ColumnCodec,
    /// Number of non-null values.
    pub count: u64,
    /// Minimum value (as f64 for numeric columns, 0.0 for non-numeric).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    /// Maximum value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    /// Sum of values (for numeric columns).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sum: Option<f64>,
    /// Number of distinct values (for symbol/tag columns).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cardinality: Option<u32>,
    /// Compressed size in bytes for this column.
    pub compressed_bytes: u64,
    /// Uncompressed size in bytes.
    pub uncompressed_bytes: u64,
}

impl ColumnStatistics {
    /// Create empty statistics with just the codec.
    pub fn new(codec: ColumnCodec) -> Self {
        Self {
            codec,
            count: 0,
            min: None,
            max: None,
            sum: None,
            cardinality: None,
            compressed_bytes: 0,
            uncompressed_bytes: 0,
        }
    }

    /// Compute statistics for an i64 column.
    pub fn from_i64(values: &[i64], codec: ColumnCodec, compressed_bytes: u64) -> Self {
        if values.is_empty() {
            return Self::new(codec);
        }

        let mut min = values[0];
        let mut max = values[0];
        let mut sum: i128 = 0;

        for &v in values {
            if v < min {
                min = v;
            }
            if v > max {
                max = v;
            }
            sum += v as i128;
        }

        Self {
            codec,
            count: values.len() as u64,
            min: Some(min as f64),
            max: Some(max as f64),
            sum: Some(sum as f64),
            cardinality: None,
            compressed_bytes,
            uncompressed_bytes: (values.len() * 8) as u64,
        }
    }

    /// Compute statistics for an f64 column.
    pub fn from_f64(values: &[f64], codec: ColumnCodec, compressed_bytes: u64) -> Self {
        if values.is_empty() {
            return Self::new(codec);
        }

        let mut min = values[0];
        let mut max = values[0];
        let mut sum: f64 = 0.0;

        for &v in values {
            if v < min {
                min = v;
            }
            if v > max {
                max = v;
            }
            sum += v;
        }

        Self {
            codec,
            count: values.len() as u64,
            min: Some(min),
            max: Some(max),
            sum: Some(sum),
            cardinality: None,
            compressed_bytes,
            uncompressed_bytes: (values.len() * 8) as u64,
        }
    }

    /// Compute statistics for a symbol column.
    pub fn from_symbols(
        values: &[u32],
        cardinality: u32,
        codec: ColumnCodec,
        compressed_bytes: u64,
    ) -> Self {
        Self {
            codec,
            count: values.len() as u64,
            min: None,
            max: None,
            sum: None,
            cardinality: Some(cardinality),
            compressed_bytes,
            uncompressed_bytes: (values.len() * 4) as u64,
        }
    }

    /// Compression ratio (uncompressed / compressed). Returns 1.0 if no data.
    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_bytes == 0 {
            return 1.0;
        }
        self.uncompressed_bytes as f64 / self.compressed_bytes as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_codec_serde_roundtrip() {
        for codec in [
            ColumnCodec::Auto,
            ColumnCodec::Gorilla,
            ColumnCodec::DoubleDelta,
            ColumnCodec::Delta,
            ColumnCodec::Lz4,
            ColumnCodec::Zstd,
            ColumnCodec::Raw,
        ] {
            let json = serde_json::to_string(&codec).unwrap();
            let back: ColumnCodec = serde_json::from_str(&json).unwrap();
            assert_eq!(codec, back, "serde roundtrip failed for {codec}");
        }
    }

    #[test]
    fn column_statistics_i64() {
        let values = vec![10i64, 20, 30, 40, 50];
        let stats = ColumnStatistics::from_i64(&values, ColumnCodec::Delta, 12);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.min, Some(10.0));
        assert_eq!(stats.max, Some(50.0));
        assert_eq!(stats.sum, Some(150.0));
        assert_eq!(stats.uncompressed_bytes, 40);
        assert_eq!(stats.compressed_bytes, 12);
    }

    #[test]
    fn column_statistics_f64() {
        let values = vec![1.5f64, 2.5, 3.5];
        let stats = ColumnStatistics::from_f64(&values, ColumnCodec::Gorilla, 8);
        assert_eq!(stats.count, 3);
        assert_eq!(stats.min, Some(1.5));
        assert_eq!(stats.max, Some(3.5));
        assert_eq!(stats.sum, Some(7.5));
    }

    #[test]
    fn column_statistics_symbols() {
        let values = vec![0u32, 1, 2, 0, 1];
        let stats = ColumnStatistics::from_symbols(&values, 3, ColumnCodec::Raw, 20);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.cardinality, Some(3));
        assert!(stats.min.is_none());
    }

    #[test]
    fn compression_ratio_calculation() {
        let stats = ColumnStatistics {
            codec: ColumnCodec::Delta,
            count: 100,
            min: None,
            max: None,
            sum: None,
            cardinality: None,
            compressed_bytes: 200,
            uncompressed_bytes: 800,
        };
        assert!((stats.compression_ratio() - 4.0).abs() < f64::EPSILON);
    }
}
