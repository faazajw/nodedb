//! Segment reader for L1 timeseries data.
//!
//! Reads and decodes L1 segment files (Gorilla-compressed metrics,
//! Zstd-compressed logs) for the query path.
//!
//! ## Segment Formats
//!
//! ### Metric Segment
//! ```text
//! [magic:4 "TSEG"] [kind:1 0x01] [sample_count:8] [block_len:4] [gorilla_block:N]
//! ```
//!
//! ### Log Segment
//! ```text
//! [magic:4 "TSEG"] [kind:1 0x02] [entry_count:4] [compressed_len:4] [compressed_block:N]
//! ```

use std::path::Path;

use super::compress::{DictionaryRegistry, decompress_log};
use super::gorilla::GorillaDecoder;
use nodedb_types::timeseries::LogEntry;

const SEGMENT_MAGIC: &[u8; 4] = b"TSEG";
const KIND_METRIC: u8 = 0x01;
const KIND_LOG: u8 = 0x02;

/// A decoded metric segment: all samples decompressed.
#[derive(Debug)]
pub struct MetricSegmentData {
    /// Decompressed (timestamp_ms, value) pairs, in order.
    pub samples: Vec<(i64, f64)>,
}

/// A decoded log segment: all entries decompressed.
#[derive(Debug)]
pub struct LogSegmentData {
    /// Decompressed log entries, in order.
    pub entries: Vec<LogEntry>,
}

/// What kind of data a segment contains.
#[derive(Debug)]
pub enum SegmentData {
    Metric(MetricSegmentData),
    Log(LogSegmentData),
}

/// Errors from segment reading.
#[derive(Debug, thiserror::Error)]
pub enum SegmentReadError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("segment too small: {size} bytes")]
    TooSmall { size: usize },
    #[error("invalid segment magic")]
    InvalidMagic,
    #[error("unknown segment kind: {kind:#x}")]
    UnknownKind { kind: u8 },
    #[error("decompression error: {detail}")]
    Decompression { detail: String },
}

/// Read and decode a metric segment from disk.
///
/// Returns all (timestamp_ms, value) pairs.
pub fn read_metric_segment(path: &Path) -> Result<MetricSegmentData, SegmentReadError> {
    let data = std::fs::read(path)?;

    // Minimum: magic(4) + kind(1) + sample_count(8) + block_len(4) = 17 bytes
    if data.len() < 17 {
        return Err(SegmentReadError::TooSmall { size: data.len() });
    }
    if &data[0..4] != SEGMENT_MAGIC {
        return Err(SegmentReadError::InvalidMagic);
    }
    if data[4] != KIND_METRIC {
        return Err(SegmentReadError::UnknownKind { kind: data[4] });
    }

    let block_len = u32::from_le_bytes(data[13..17].try_into().unwrap_or([0; 4])) as usize;

    if data.len() < 17 + block_len {
        return Err(SegmentReadError::TooSmall { size: data.len() });
    }

    let gorilla_block = &data[17..17 + block_len];
    let mut decoder = GorillaDecoder::new(gorilla_block);
    let samples = decoder.decode_all();

    Ok(MetricSegmentData { samples })
}

/// Read and decode a log segment from disk.
///
/// Returns all log entries. Requires a dictionary registry for
/// Zstd dictionary decompression.
pub fn read_log_segment(
    path: &Path,
    registry: &DictionaryRegistry,
) -> Result<LogSegmentData, SegmentReadError> {
    let data = std::fs::read(path)?;

    // Minimum: magic(4) + kind(1) + entry_count(4) + compressed_len(4) = 13 bytes
    if data.len() < 13 {
        return Err(SegmentReadError::TooSmall { size: data.len() });
    }
    if &data[0..4] != SEGMENT_MAGIC {
        return Err(SegmentReadError::InvalidMagic);
    }
    if data[4] != KIND_LOG {
        return Err(SegmentReadError::UnknownKind { kind: data[4] });
    }

    let entry_count = u32::from_le_bytes(data[5..9].try_into().unwrap_or([0; 4])) as usize;

    let compressed_len = u32::from_le_bytes(data[9..13].try_into().unwrap_or([0; 4])) as usize;

    if data.len() < 13 + compressed_len {
        return Err(SegmentReadError::TooSmall { size: data.len() });
    }

    let compressed_block = &data[13..13 + compressed_len];

    // The compressed block includes the ZL header from compress_log.
    let raw = decompress_log(compressed_block, registry).map_err(|e| {
        SegmentReadError::Decompression {
            detail: e.to_string(),
        }
    })?;

    // Parse raw bytes back into log entries.
    // Format per entry: [timestamp_ms:8] [data_len:4] [data:N]
    let mut entries = Vec::with_capacity(entry_count);
    let mut offset = 0;

    while offset + 12 <= raw.len() && entries.len() < entry_count {
        let timestamp_ms = i64::from_le_bytes(raw[offset..offset + 8].try_into().unwrap_or([0; 8]));
        let data_len =
            u32::from_le_bytes(raw[offset + 8..offset + 12].try_into().unwrap_or([0; 4])) as usize;

        offset += 12;
        if offset + data_len > raw.len() {
            break;
        }

        entries.push(LogEntry {
            timestamp_ms,
            data: raw[offset..offset + data_len].to_vec(),
        });
        offset += data_len;
    }

    Ok(LogSegmentData { entries })
}

/// Read a segment file, auto-detecting its type.
pub fn read_segment(
    path: &Path,
    registry: &DictionaryRegistry,
) -> Result<SegmentData, SegmentReadError> {
    let data = std::fs::read(path)?;

    if data.len() < 5 {
        return Err(SegmentReadError::TooSmall { size: data.len() });
    }
    if &data[0..4] != SEGMENT_MAGIC {
        return Err(SegmentReadError::InvalidMagic);
    }

    match data[4] {
        KIND_METRIC => read_metric_segment(path).map(SegmentData::Metric),
        KIND_LOG => read_log_segment(path, registry).map(SegmentData::Log),
        kind => Err(SegmentReadError::UnknownKind { kind }),
    }
}

/// Aggregation functions for metric samples.
#[derive(Debug, Clone, Copy)]
pub struct MetricAggregation {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub first_ts: i64,
    pub last_ts: i64,
}

impl MetricAggregation {
    /// Compute aggregation over a slice of (timestamp, value) pairs.
    pub fn compute(samples: &[(i64, f64)]) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }

        let mut agg = Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            first_ts: samples[0].0,
            last_ts: samples[0].0,
        };

        for &(ts, val) in samples {
            agg.count += 1;
            agg.sum += val;
            if val < agg.min {
                agg.min = val;
            }
            if val > agg.max {
                agg.max = val;
            }
            if ts < agg.first_ts {
                agg.first_ts = ts;
            }
            if ts > agg.last_ts {
                agg.last_ts = ts;
            }
        }

        Some(agg)
    }

    /// Average value.
    pub fn avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    /// Merge two aggregations (for combining results from multiple segments).
    pub fn merge(&self, other: &Self) -> Self {
        Self {
            count: self.count + other.count,
            sum: self.sum + other.sum,
            min: self.min.min(other.min),
            max: self.max.max(other.max),
            first_ts: self.first_ts.min(other.first_ts),
            last_ts: self.last_ts.max(other.last_ts),
        }
    }
}

/// Downsample metric samples by averaging within fixed time windows.
///
/// Given samples sorted by timestamp and a window size (in ms), returns
/// one (timestamp, avg_value) per window. The timestamp is the start
/// of the window.
pub fn downsample(samples: &[(i64, f64)], window_ms: i64) -> Vec<(i64, f64)> {
    if samples.is_empty() || window_ms <= 0 {
        return Vec::new();
    }

    let mut result = Vec::new();
    let mut window_start = (samples[0].0 / window_ms) * window_ms;
    let mut window_sum = 0.0;
    let mut window_count = 0u64;

    for &(ts, val) in samples {
        let this_window = (ts / window_ms) * window_ms;
        if this_window != window_start {
            // Emit previous window.
            if window_count > 0 {
                result.push((window_start, window_sum / window_count as f64));
            }
            window_start = this_window;
            window_sum = 0.0;
            window_count = 0;
        }
        window_sum += val;
        window_count += 1;
    }

    // Emit last window.
    if window_count > 0 {
        result.push((window_start, window_sum / window_count as f64));
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::gorilla::GorillaEncoder;
    use tempfile::TempDir;

    fn write_test_metric_segment(dir: &Path, samples: &[(i64, f64)]) -> std::path::PathBuf {
        let mut encoder = GorillaEncoder::new();
        for &(ts, val) in samples {
            encoder.encode(ts, val);
        }
        let gorilla_block = encoder.finish();

        let path = dir.join("test-metric.seg");
        let mut buf = Vec::new();
        buf.extend_from_slice(b"TSEG");
        buf.push(0x01);
        buf.extend_from_slice(&(samples.len() as u64).to_le_bytes());
        buf.extend_from_slice(&(gorilla_block.len() as u32).to_le_bytes());
        buf.extend_from_slice(&gorilla_block);
        std::fs::write(&path, &buf).unwrap();
        path
    }

    #[test]
    fn read_metric_segment_roundtrip() {
        let dir = TempDir::new().unwrap();
        let samples = vec![(1000i64, 42.0f64), (2000, 43.5), (3000, 41.0), (4000, 44.2)];
        let path = write_test_metric_segment(dir.path(), &samples);

        let data = read_metric_segment(&path).unwrap();
        assert_eq!(data.samples.len(), 4);
        assert_eq!(data.samples[0].0, 1000);
        assert!((data.samples[0].1 - 42.0).abs() < f64::EPSILON);
        assert_eq!(data.samples[3].0, 4000);
    }

    #[test]
    fn aggregation_basic() {
        let samples = vec![(1000i64, 10.0f64), (2000, 20.0), (3000, 30.0), (4000, 40.0)];
        let agg = MetricAggregation::compute(&samples).unwrap();
        assert_eq!(agg.count, 4);
        assert!((agg.sum - 100.0).abs() < f64::EPSILON);
        assert!((agg.avg() - 25.0).abs() < f64::EPSILON);
        assert!((agg.min - 10.0).abs() < f64::EPSILON);
        assert!((agg.max - 40.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregation_empty() {
        assert!(MetricAggregation::compute(&[]).is_none());
    }

    #[test]
    fn aggregation_merge() {
        let a = MetricAggregation {
            count: 2,
            sum: 30.0,
            min: 10.0,
            max: 20.0,
            first_ts: 1000,
            last_ts: 2000,
        };
        let b = MetricAggregation {
            count: 2,
            sum: 70.0,
            min: 30.0,
            max: 40.0,
            first_ts: 3000,
            last_ts: 4000,
        };
        let merged = a.merge(&b);
        assert_eq!(merged.count, 4);
        assert!((merged.sum - 100.0).abs() < f64::EPSILON);
        assert!((merged.min - 10.0).abs() < f64::EPSILON);
        assert!((merged.max - 40.0).abs() < f64::EPSILON);
        assert_eq!(merged.first_ts, 1000);
        assert_eq!(merged.last_ts, 4000);
    }

    #[test]
    fn downsample_basic() {
        let samples: Vec<(i64, f64)> = (0..100).map(|i| (i * 100, i as f64)).collect();

        // 10 windows of 1000ms each.
        let downsampled = downsample(&samples, 1000);
        assert_eq!(downsampled.len(), 10);

        // First window: samples 0-9, avg = 4.5
        assert_eq!(downsampled[0].0, 0);
        assert!((downsampled[0].1 - 4.5).abs() < f64::EPSILON);

        // Last window: samples 90-99, avg = 94.5
        assert_eq!(downsampled[9].0, 9000);
        assert!((downsampled[9].1 - 94.5).abs() < f64::EPSILON);
    }

    #[test]
    fn downsample_empty() {
        assert!(downsample(&[], 1000).is_empty());
    }

    #[test]
    fn invalid_segment_errors() {
        let dir = TempDir::new().unwrap();

        // Too small.
        let path = dir.path().join("tiny.seg");
        std::fs::write(&path, [0u8; 3]).unwrap();
        assert!(matches!(
            read_metric_segment(&path),
            Err(SegmentReadError::TooSmall { .. })
        ));

        // Bad magic.
        let path = dir.path().join("bad_magic.seg");
        std::fs::write(
            &path,
            b"XXXX\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        .unwrap();
        assert!(matches!(
            read_metric_segment(&path),
            Err(SegmentReadError::InvalidMagic)
        ));
    }
}
