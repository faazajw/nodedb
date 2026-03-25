//! Sparse primary index for block-level skip within partitions.
//!
//! Stores `(min_ts, max_ts, per-column min/max)` every N rows (default 1024,
//! aligned with FastLanes block size). Enables:
//!
//! 1. **Time-range block skip**: binary search to find blocks overlapping a
//!    query's time range. For a 1-hour query within a 1-day partition (86M rows),
//!    reads ~3600 blocks instead of scanning 86M rows.
//!
//! 2. **Predicate pushdown**: `WHERE cpu > 90` → skip blocks where
//!    `max(cpu) <= 90`. Evaluates predicates against block-level statistics
//!    without decompressing any data.
//!
//! 3. **Metadata-only queries**: `COUNT(*)` → `row_count` from partition meta.
//!    `MIN/MAX(ts)` → from column stats. `SUM(value)` → from column stats sum.
//!    Zero decompression.
//!
//! The sparse index is built at flush time from raw (pre-compression) column
//! data and stored as `sparse_index.bin` in the partition directory.
//!
//! File format:
//! ```text
//! [4 bytes] version (LE u32, currently 1)
//! [4 bytes] block_size (LE u32, rows per block)
//! [4 bytes] block_count (LE u32)
//! [4 bytes] column_count (LE u32)
//! [column_count × (2 + name_len) bytes] column names (LE u16 length + UTF-8)
//! [block_count × BlockEntry bytes] block entries
//!
//! BlockEntry:
//!   [4 bytes] row_offset (LE u32)
//!   [4 bytes] row_count (LE u32)
//!   [8 bytes] min_ts (LE i64)
//!   [8 bytes] max_ts (LE i64)
//!   [column_count × 16 bytes] per-column stats (min: f64 LE, max: f64 LE)
//! ```

use super::columnar_memtable::{ColumnData, ColumnType, ColumnarSchema};

/// Default block size: 1024 rows, aligned with FastLanes block size.
pub const DEFAULT_BLOCK_SIZE: usize = 1024;

/// Current file format version.
const FORMAT_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// Sparse primary index for a single partition.
///
/// Enables sub-partition block-level skip during query.
#[derive(Debug, Clone)]
pub struct SparseIndex {
    /// Rows per block (default 1024).
    pub block_size: u32,
    /// Column names in order (matches the schema at flush time).
    pub column_names: Vec<String>,
    /// Per-block metadata, sorted by row_offset.
    pub blocks: Vec<BlockEntry>,
}

/// Metadata for a single block within a partition.
#[derive(Debug, Clone)]
pub struct BlockEntry {
    /// Starting row index of this block.
    pub row_offset: u32,
    /// Number of rows in this block (last block may be smaller).
    pub row_count: u32,
    /// Minimum timestamp in this block.
    pub min_ts: i64,
    /// Maximum timestamp in this block.
    pub max_ts: i64,
    /// Per-column min/max statistics. Indexed by column position.
    /// For non-numeric columns (Symbol), values are NaN.
    pub column_stats: Vec<BlockColumnStats>,
}

/// Min/max statistics for a single column within a block.
#[derive(Debug, Clone, Copy)]
pub struct BlockColumnStats {
    pub min: f64,
    pub max: f64,
}

impl BlockColumnStats {
    /// No-data sentinel (for symbol columns or empty blocks).
    pub fn none() -> Self {
        Self {
            min: f64::NAN,
            max: f64::NAN,
        }
    }

    /// Whether this stat has valid numeric data (not NaN).
    pub fn is_valid(&self) -> bool {
        !self.min.is_nan() && !self.max.is_nan()
    }
}

// ---------------------------------------------------------------------------
// Predicate types for block-level pushdown
// ---------------------------------------------------------------------------

/// A simple predicate for block-level pushdown.
#[derive(Debug, Clone)]
pub enum BlockPredicate {
    /// Column value > threshold.
    GreaterThan { column_idx: usize, threshold: f64 },
    /// Column value >= threshold.
    GreaterThanOrEqual { column_idx: usize, threshold: f64 },
    /// Column value < threshold.
    LessThan { column_idx: usize, threshold: f64 },
    /// Column value <= threshold.
    LessThanOrEqual { column_idx: usize, threshold: f64 },
    /// Column value between [low, high] inclusive.
    Between {
        column_idx: usize,
        low: f64,
        high: f64,
    },
}

impl BlockPredicate {
    /// Check if a block could possibly contain rows matching this predicate.
    ///
    /// Returns `true` if the block cannot be skipped (might contain matches).
    /// Returns `false` if the block can definitely be skipped.
    pub fn might_match(&self, stats: &BlockColumnStats) -> bool {
        if !stats.is_valid() {
            return true; // Can't skip blocks with no stats.
        }
        match self {
            Self::GreaterThan { threshold, .. } => stats.max > *threshold,
            Self::GreaterThanOrEqual { threshold, .. } => stats.max >= *threshold,
            Self::LessThan { threshold, .. } => stats.min < *threshold,
            Self::LessThanOrEqual { threshold, .. } => stats.min <= *threshold,
            Self::Between { low, high, .. } => stats.max >= *low && stats.min <= *high,
        }
    }

    /// Column index this predicate applies to.
    pub fn column_idx(&self) -> usize {
        match self {
            Self::GreaterThan { column_idx, .. }
            | Self::GreaterThanOrEqual { column_idx, .. }
            | Self::LessThan { column_idx, .. }
            | Self::LessThanOrEqual { column_idx, .. }
            | Self::Between { column_idx, .. } => *column_idx,
        }
    }
}

// ---------------------------------------------------------------------------
// SparseIndex construction (from raw column data)
// ---------------------------------------------------------------------------

impl SparseIndex {
    /// Build a sparse index from raw column data at flush time.
    ///
    /// Scans the column data in blocks of `block_size` rows, computing
    /// per-block timestamp ranges and per-column min/max statistics.
    pub fn build(
        columns: &[ColumnData],
        schema: &ColumnarSchema,
        row_count: u64,
        block_size: usize,
    ) -> Self {
        let block_size = block_size.max(64); // minimum 64 rows per block
        let total_rows = row_count as usize;
        let block_count = if total_rows == 0 {
            0
        } else {
            total_rows.div_ceil(block_size)
        };

        let column_names: Vec<String> = schema.columns.iter().map(|(n, _)| n.clone()).collect();
        let ts_idx = schema.timestamp_idx;

        let mut blocks = Vec::with_capacity(block_count);

        for block_idx in 0..block_count {
            let row_start = block_idx * block_size;
            let row_end = (row_start + block_size).min(total_rows);
            let count = row_end - row_start;

            // Timestamp range for this block.
            let (min_ts, max_ts) = if ts_idx < columns.len() {
                compute_ts_range(&columns[ts_idx], row_start, row_end)
            } else {
                (i64::MIN, i64::MAX)
            };

            // Per-column stats.
            let column_stats: Vec<BlockColumnStats> = columns
                .iter()
                .zip(schema.columns.iter())
                .map(|(col, (_, col_type))| compute_block_stats(col, *col_type, row_start, row_end))
                .collect();

            blocks.push(BlockEntry {
                row_offset: row_start as u32,
                row_count: count as u32,
                min_ts,
                max_ts,
                column_stats,
            });
        }

        Self {
            block_size: block_size as u32,
            column_names,
            blocks,
        }
    }

    // -- Query methods --

    /// Find blocks whose timestamp range overlaps `[start_ms, end_ms]`.
    ///
    /// Returns indices into `self.blocks`. Uses binary search for the start
    /// and scans forward until blocks no longer overlap.
    pub fn blocks_in_time_range(&self, start_ms: i64, end_ms: i64) -> Vec<usize> {
        if self.blocks.is_empty() {
            return Vec::new();
        }

        // Binary search for the first block whose max_ts >= start_ms.
        let first = self.blocks.partition_point(|b| b.max_ts < start_ms);

        let mut result = Vec::new();
        for i in first..self.blocks.len() {
            let block = &self.blocks[i];
            if block.min_ts > end_ms {
                break; // No more overlapping blocks.
            }
            result.push(i);
        }
        result
    }

    /// Filter blocks by time range AND predicates.
    ///
    /// Returns indices of blocks that might contain matching rows.
    /// Blocks are skipped if their timestamp range doesn't overlap OR
    /// if any predicate's min/max check rules them out.
    pub fn filter_blocks(
        &self,
        start_ms: i64,
        end_ms: i64,
        predicates: &[BlockPredicate],
    ) -> Vec<usize> {
        let time_blocks = self.blocks_in_time_range(start_ms, end_ms);

        if predicates.is_empty() {
            return time_blocks;
        }

        time_blocks
            .into_iter()
            .filter(|&block_idx| {
                let block = &self.blocks[block_idx];
                predicates.iter().all(|pred| {
                    let col_idx = pred.column_idx();
                    if col_idx < block.column_stats.len() {
                        pred.might_match(&block.column_stats[col_idx])
                    } else {
                        true // Unknown column — can't skip.
                    }
                })
            })
            .collect()
    }

    /// Total row count across all blocks.
    pub fn total_rows(&self) -> u64 {
        self.blocks.iter().map(|b| b.row_count as u64).sum()
    }

    /// Number of blocks.
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// Row range for a specific block.
    pub fn block_row_range(&self, block_idx: usize) -> (usize, usize) {
        let block = &self.blocks[block_idx];
        let start = block.row_offset as usize;
        let end = start + block.row_count as usize;
        (start, end)
    }

    /// Find column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.column_names.iter().position(|n| n == name)
    }

    // -- Serialization --

    /// Serialize to binary format.
    pub fn to_bytes(&self) -> Vec<u8> {
        let col_count = self.column_names.len();
        // Estimate size.
        let mut buf =
            Vec::with_capacity(16 + col_count * 32 + self.blocks.len() * (24 + col_count * 16));

        // Header.
        buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&self.block_size.to_le_bytes());
        buf.extend_from_slice(&(self.blocks.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(col_count as u32).to_le_bytes());

        // Column names.
        for name in &self.column_names {
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(name_bytes);
        }

        // Block entries.
        for block in &self.blocks {
            buf.extend_from_slice(&block.row_offset.to_le_bytes());
            buf.extend_from_slice(&block.row_count.to_le_bytes());
            buf.extend_from_slice(&block.min_ts.to_le_bytes());
            buf.extend_from_slice(&block.max_ts.to_le_bytes());
            for stats in &block.column_stats {
                buf.extend_from_slice(&stats.min.to_le_bytes());
                buf.extend_from_slice(&stats.max.to_le_bytes());
            }
        }

        buf
    }

    /// Deserialize from binary format.
    pub fn from_bytes(data: &[u8]) -> Result<Self, SparseIndexError> {
        if data.len() < 16 {
            return Err(SparseIndexError::Truncated);
        }

        let version = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        if version != FORMAT_VERSION {
            return Err(SparseIndexError::UnsupportedVersion(version));
        }

        let block_size = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        let block_count = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;
        let col_count = u32::from_le_bytes([data[12], data[13], data[14], data[15]]) as usize;

        let mut pos = 16;

        // Read column names.
        let mut column_names = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            if pos + 2 > data.len() {
                return Err(SparseIndexError::Truncated);
            }
            let name_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            if pos + name_len > data.len() {
                return Err(SparseIndexError::Truncated);
            }
            let name = std::str::from_utf8(&data[pos..pos + name_len])
                .map_err(|_| SparseIndexError::Corrupt("invalid UTF-8 column name".into()))?
                .to_string();
            pos += name_len;
            column_names.push(name);
        }

        // Read block entries.
        let entry_size = 24 + col_count * 16; // 4+4+8+8 + cols*(8+8)
        let mut blocks = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            if pos + entry_size > data.len() {
                return Err(SparseIndexError::Truncated);
            }

            let row_offset =
                u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            let row_count =
                u32::from_le_bytes([data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7]]);
            let min_ts = i64::from_le_bytes([
                data[pos + 8],
                data[pos + 9],
                data[pos + 10],
                data[pos + 11],
                data[pos + 12],
                data[pos + 13],
                data[pos + 14],
                data[pos + 15],
            ]);
            let max_ts = i64::from_le_bytes([
                data[pos + 16],
                data[pos + 17],
                data[pos + 18],
                data[pos + 19],
                data[pos + 20],
                data[pos + 21],
                data[pos + 22],
                data[pos + 23],
            ]);
            pos += 24;

            let mut column_stats = Vec::with_capacity(col_count);
            for _ in 0..col_count {
                let min = f64::from_le_bytes([
                    data[pos],
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                    data[pos + 5],
                    data[pos + 6],
                    data[pos + 7],
                ]);
                let max = f64::from_le_bytes([
                    data[pos + 8],
                    data[pos + 9],
                    data[pos + 10],
                    data[pos + 11],
                    data[pos + 12],
                    data[pos + 13],
                    data[pos + 14],
                    data[pos + 15],
                ]);
                pos += 16;
                column_stats.push(BlockColumnStats { min, max });
            }

            blocks.push(BlockEntry {
                row_offset,
                row_count,
                min_ts,
                max_ts,
                column_stats,
            });
        }

        Ok(Self {
            block_size,
            column_names,
            blocks,
        })
    }
}

// ---------------------------------------------------------------------------
// Helper functions for building stats from column data
// ---------------------------------------------------------------------------

fn compute_ts_range(col: &ColumnData, row_start: usize, row_end: usize) -> (i64, i64) {
    match col {
        ColumnData::Timestamp(v) => {
            let slice = &v[row_start..row_end];
            if slice.is_empty() {
                return (i64::MAX, i64::MIN);
            }
            let mut min = slice[0];
            let mut max = slice[0];
            for &ts in &slice[1..] {
                if ts < min {
                    min = ts;
                }
                if ts > max {
                    max = ts;
                }
            }
            (min, max)
        }
        _ => (i64::MIN, i64::MAX),
    }
}

fn compute_block_stats(
    col: &ColumnData,
    col_type: ColumnType,
    row_start: usize,
    row_end: usize,
) -> BlockColumnStats {
    match (col, col_type) {
        (ColumnData::Timestamp(v), ColumnType::Timestamp) => {
            let slice = &v[row_start..row_end];
            if slice.is_empty() {
                return BlockColumnStats::none();
            }
            let mut min = slice[0];
            let mut max = slice[0];
            for &val in &slice[1..] {
                if val < min {
                    min = val;
                }
                if val > max {
                    max = val;
                }
            }
            BlockColumnStats {
                min: min as f64,
                max: max as f64,
            }
        }
        (ColumnData::Float64(v), ColumnType::Float64) => {
            let slice = &v[row_start..row_end];
            if slice.is_empty() {
                return BlockColumnStats::none();
            }
            let mut min = slice[0];
            let mut max = slice[0];
            for &val in &slice[1..] {
                if val < min {
                    min = val;
                }
                if val > max {
                    max = val;
                }
            }
            BlockColumnStats { min, max }
        }
        (ColumnData::Int64(v), ColumnType::Int64) => {
            let slice = &v[row_start..row_end];
            if slice.is_empty() {
                return BlockColumnStats::none();
            }
            let mut min = slice[0];
            let mut max = slice[0];
            for &val in &slice[1..] {
                if val < min {
                    min = val;
                }
                if val > max {
                    max = val;
                }
            }
            BlockColumnStats {
                min: min as f64,
                max: max as f64,
            }
        }
        (ColumnData::Symbol(_), ColumnType::Symbol) => {
            // Symbol columns don't have meaningful numeric stats.
            BlockColumnStats::none()
        }
        _ => BlockColumnStats::none(),
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum SparseIndexError {
    Truncated,
    UnsupportedVersion(u32),
    Corrupt(String),
}

impl std::fmt::Display for SparseIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(f, "sparse index data truncated"),
            Self::UnsupportedVersion(v) => {
                write!(f, "sparse index unsupported version: {v}")
            }
            Self::Corrupt(msg) => write!(f, "sparse index corrupt: {msg}"),
        }
    }
}

impl std::error::Error for SparseIndexError {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_columns(row_count: usize) -> (Vec<ColumnData>, ColumnarSchema) {
        let timestamps: Vec<i64> = (0..row_count as i64)
            .map(|i| 1_700_000_000_000 + i * 10_000)
            .collect();
        let values: Vec<f64> = (0..row_count).map(|i| (i % 100) as f64).collect();

        let columns = vec![
            ColumnData::Timestamp(timestamps),
            ColumnData::Float64(values),
        ];
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("cpu".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 2],
        };
        (columns, schema)
    }

    #[test]
    fn build_empty() {
        let columns = vec![ColumnData::Timestamp(vec![]), ColumnData::Float64(vec![])];
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("cpu".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 2],
        };
        let idx = SparseIndex::build(&columns, &schema, 0, 1024);
        assert_eq!(idx.block_count(), 0);
        assert_eq!(idx.total_rows(), 0);
    }

    #[test]
    fn build_single_block() {
        let (columns, schema) = make_test_columns(500);
        let idx = SparseIndex::build(&columns, &schema, 500, 1024);
        assert_eq!(idx.block_count(), 1);
        assert_eq!(idx.total_rows(), 500);
        assert_eq!(idx.blocks[0].row_offset, 0);
        assert_eq!(idx.blocks[0].row_count, 500);
        assert_eq!(idx.blocks[0].min_ts, 1_700_000_000_000);
        assert_eq!(idx.blocks[0].max_ts, 1_700_000_000_000 + 499 * 10_000);
    }

    #[test]
    fn build_multiple_blocks() {
        let (columns, schema) = make_test_columns(3000);
        let idx = SparseIndex::build(&columns, &schema, 3000, 1024);
        // 3000 / 1024 = 2 full + 1 partial = 3 blocks.
        assert_eq!(idx.block_count(), 3);
        assert_eq!(idx.total_rows(), 3000);

        assert_eq!(idx.blocks[0].row_count, 1024);
        assert_eq!(idx.blocks[1].row_count, 1024);
        assert_eq!(idx.blocks[2].row_count, 952);
    }

    #[test]
    fn block_level_stats() {
        let (columns, schema) = make_test_columns(2048);
        let idx = SparseIndex::build(&columns, &schema, 2048, 1024);
        assert_eq!(idx.block_count(), 2);

        // CPU values cycle 0..100, block 0 has rows 0..1024 → values 0..99 cycle.
        let cpu_stats_0 = &idx.blocks[0].column_stats[1];
        assert_eq!(cpu_stats_0.min, 0.0);
        assert_eq!(cpu_stats_0.max, 99.0);

        // Block 1 has rows 1024..2048 → same cycle.
        let cpu_stats_1 = &idx.blocks[1].column_stats[1];
        assert_eq!(cpu_stats_1.min, 0.0);
        assert_eq!(cpu_stats_1.max, 99.0);
    }

    #[test]
    fn time_range_query() {
        let (columns, schema) = make_test_columns(10_000);
        let idx = SparseIndex::build(&columns, &schema, 10_000, 1024);
        assert_eq!(idx.block_count(), 10); // 10000 / 1024 = 9.77 → 10 blocks

        // Query the middle: rows ~5000-6000.
        let ts_start = 1_700_000_000_000 + 5000 * 10_000;
        let ts_end = 1_700_000_000_000 + 6000 * 10_000;
        let matching = idx.blocks_in_time_range(ts_start, ts_end);

        // Should match 2-3 blocks around rows 5000-6000.
        assert!(!matching.is_empty());
        assert!(matching.len() <= 3);

        // Should NOT match block 0 (rows 0..1024, timestamps too early).
        assert!(!matching.contains(&0));
    }

    #[test]
    fn time_range_no_overlap() {
        let (columns, schema) = make_test_columns(1000);
        let idx = SparseIndex::build(&columns, &schema, 1000, 1024);

        // Query after all data.
        let matching = idx.blocks_in_time_range(i64::MAX - 1, i64::MAX);
        assert!(matching.is_empty());

        // Query before all data.
        let matching = idx.blocks_in_time_range(0, 1);
        assert!(matching.is_empty());
    }

    #[test]
    fn predicate_pushdown() {
        // Create data where block 0 has cpu in [0, 49] and block 1 has cpu in [50, 99].
        let timestamps: Vec<i64> = (0..2048).map(|i| 1_700_000_000_000 + i * 10_000).collect();
        let values: Vec<f64> = (0..2048)
            .map(|i| {
                if i < 1024 {
                    (i % 50) as f64
                } else {
                    50.0 + (i % 50) as f64
                }
            })
            .collect();
        let columns = vec![
            ColumnData::Timestamp(timestamps),
            ColumnData::Float64(values),
        ];
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("cpu".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 2],
        };
        let idx = SparseIndex::build(&columns, &schema, 2048, 1024);
        assert_eq!(idx.block_count(), 2);

        // Block 0: cpu min=0, max=49. Block 1: cpu min=50, max=99.
        assert_eq!(idx.blocks[0].column_stats[1].max, 49.0);
        assert_eq!(idx.blocks[1].column_stats[1].min, 50.0);

        // Predicate: WHERE cpu > 60 → skip block 0 (max=49 <= 60).
        let preds = vec![BlockPredicate::GreaterThan {
            column_idx: 1,
            threshold: 60.0,
        }];
        let matching = idx.filter_blocks(i64::MIN, i64::MAX, &preds);
        assert_eq!(matching, vec![1]); // Only block 1.

        // Predicate: WHERE cpu < 10 → skip block 1 (min=50 >= 10).
        let preds = vec![BlockPredicate::LessThan {
            column_idx: 1,
            threshold: 10.0,
        }];
        let matching = idx.filter_blocks(i64::MIN, i64::MAX, &preds);
        assert_eq!(matching, vec![0]); // Only block 0.

        // Predicate: WHERE cpu BETWEEN 45 AND 55 → both blocks match.
        let preds = vec![BlockPredicate::Between {
            column_idx: 1,
            low: 45.0,
            high: 55.0,
        }];
        let matching = idx.filter_blocks(i64::MIN, i64::MAX, &preds);
        assert_eq!(matching, vec![0, 1]);
    }

    #[test]
    fn combined_time_and_predicate() {
        let (columns, schema) = make_test_columns(10_000);
        let idx = SparseIndex::build(&columns, &schema, 10_000, 1024);

        // Time range: only last 2 blocks. Predicate: cpu > 50.
        let ts_start = 1_700_000_000_000 + 8000 * 10_000;
        let ts_end = 1_700_000_000_000 + 9999 * 10_000;
        let preds = vec![BlockPredicate::GreaterThan {
            column_idx: 1,
            threshold: 50.0,
        }];
        let matching = idx.filter_blocks(ts_start, ts_end, &preds);

        // Should return block indices for the last ~2 blocks.
        assert!(!matching.is_empty());
        assert!(matching.len() <= 3);
        // First few blocks should NOT be included.
        for &bi in &matching {
            assert!(bi >= 7, "block {bi} should not be in range");
        }
    }

    #[test]
    fn serialization_roundtrip() {
        let (columns, schema) = make_test_columns(5000);
        let idx = SparseIndex::build(&columns, &schema, 5000, 1024);
        let bytes = idx.to_bytes();
        let recovered = SparseIndex::from_bytes(&bytes).unwrap();

        assert_eq!(recovered.block_size, idx.block_size);
        assert_eq!(recovered.column_names, idx.column_names);
        assert_eq!(recovered.blocks.len(), idx.blocks.len());

        for (a, b) in idx.blocks.iter().zip(recovered.blocks.iter()) {
            assert_eq!(a.row_offset, b.row_offset);
            assert_eq!(a.row_count, b.row_count);
            assert_eq!(a.min_ts, b.min_ts);
            assert_eq!(a.max_ts, b.max_ts);
            for (sa, sb) in a.column_stats.iter().zip(b.column_stats.iter()) {
                assert_eq!(sa.min.to_bits(), sb.min.to_bits());
                assert_eq!(sa.max.to_bits(), sb.max.to_bits());
            }
        }
    }

    #[test]
    fn serialization_empty() {
        let idx = SparseIndex {
            block_size: 1024,
            column_names: vec!["ts".into(), "val".into()],
            blocks: vec![],
        };
        let bytes = idx.to_bytes();
        let recovered = SparseIndex::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.block_count(), 0);
        assert_eq!(recovered.column_names.len(), 2);
    }

    #[test]
    fn large_partition_skip_rate() {
        // 86M rows (1-day at 1000 samples/sec) → 83984 blocks.
        // Query 1 hour → should match ~3515 blocks → ~4.2% of partition.
        let row_count = 86_400_000usize;
        let block_size = 1024;
        let block_count = row_count.div_ceil(block_size);

        // Simulate: each block covers ~10.24 seconds of data.
        let blocks: Vec<BlockEntry> = (0..block_count)
            .map(|i| {
                let row_start = i * block_size;
                let row_end = (row_start + block_size).min(row_count);
                let count = row_end - row_start;
                let min_ts = 1_700_000_000_000 + (row_start as i64); // 1ms per row
                let max_ts = min_ts + count as i64 - 1;
                BlockEntry {
                    row_offset: row_start as u32,
                    row_count: count as u32,
                    min_ts,
                    max_ts,
                    column_stats: vec![],
                }
            })
            .collect();

        let idx = SparseIndex {
            block_size: block_size as u32,
            column_names: vec!["ts".into()],
            blocks,
        };

        // Query 1 hour = 3_600_000 ms.
        let query_start = 1_700_000_000_000 + 40_000_000; // ~11 hours in
        let query_end = query_start + 3_600_000;
        let matching = idx.blocks_in_time_range(query_start, query_end);

        // Should match about 3600000/1024 ≈ 3516 blocks.
        let skip_rate = 1.0 - (matching.len() as f64 / idx.block_count() as f64);
        assert!(
            skip_rate > 0.95,
            "expected >95% skip rate, got {:.1}% (matched {} of {} blocks)",
            skip_rate * 100.0,
            matching.len(),
            idx.block_count()
        );
    }

    #[test]
    fn column_index_lookup() {
        let (columns, schema) = make_test_columns(100);
        let idx = SparseIndex::build(&columns, &schema, 100, 1024);
        assert_eq!(idx.column_index("timestamp"), Some(0));
        assert_eq!(idx.column_index("cpu"), Some(1));
        assert_eq!(idx.column_index("nonexistent"), None);
    }
}
