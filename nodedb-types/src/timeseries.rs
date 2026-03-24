//! Shared timeseries types for multi-model database engines.
//!
//! Used by both `nodedb` (server) and `nodedb-lite` (embedded) for
//! timeseries ingest, storage, and query. Edge devices record sensor
//! telemetry, event logs, and metrics using these types.

/// Unique identifier for a timeseries (metric name + tag set hash).
pub type SeriesId = u64;

/// A single metric sample (timestamp + scalar value).
#[derive(Debug, Clone, Copy)]
pub struct MetricSample {
    pub timestamp_ms: i64,
    pub value: f64,
}

/// A single log entry (timestamp + arbitrary bytes).
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp_ms: i64,
    pub data: Vec<u8>,
}

/// Result of an ingest operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestResult {
    /// Write accepted, memtable healthy.
    Ok,
    /// Write accepted, but memtable should be flushed (memory pressure).
    FlushNeeded,
    /// Write rejected — memory budget exhausted and cannot evict further.
    /// Caller should apply backpressure.
    Rejected,
}

impl IngestResult {
    pub fn is_flush_needed(&self) -> bool {
        matches!(self, Self::FlushNeeded)
    }

    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected)
    }
}

/// Time range for queries (inclusive on both ends).
#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    pub start_ms: i64,
    pub end_ms: i64,
}

impl TimeRange {
    pub fn new(start_ms: i64, end_ms: i64) -> Self {
        Self { start_ms, end_ms }
    }

    pub fn contains(&self, ts: i64) -> bool {
        ts >= self.start_ms && ts <= self.end_ms
    }
}

/// Data from a single series after memtable drain.
#[derive(Debug)]
pub struct FlushedSeries {
    pub series_id: SeriesId,
    pub kind: FlushedKind,
    pub min_ts: i64,
    pub max_ts: i64,
}

/// Type-specific flushed data.
#[derive(Debug)]
pub enum FlushedKind {
    Metric {
        /// Gorilla-compressed block.
        gorilla_block: Vec<u8>,
        sample_count: u64,
    },
    Log {
        entries: Vec<LogEntry>,
        total_bytes: usize,
    },
}

/// Segment file reference for the L1/L2 index.
#[derive(Debug, Clone)]
pub struct SegmentRef {
    pub path: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub kind: SegmentKind,
    /// On-disk size in bytes.
    pub size_bytes: u64,
    /// Timestamp when segment was created (for retention).
    pub created_at_ms: i64,
}

/// Whether a segment contains metrics or logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentKind {
    Metric,
    Log,
}
