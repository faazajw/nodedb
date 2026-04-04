//! Level-based compaction for the FTS LSM engine.
//!
//! Uses tiered compaction with configurable levels and segments-per-level.
//! When a level fills (exceeds `max_segments_per_level`), all segments at
//! that level are merged into a single segment at the next level.

use crate::backend::FtsBackend;

use super::merge;
use super::segment::{reader::SegmentReader, writer};

/// Compaction configuration.
#[derive(Debug, Clone, Copy)]
pub struct CompactionConfig {
    /// Maximum number of levels.
    pub max_levels: usize,
    /// Maximum segments per level before triggering compaction.
    pub max_segments_per_level: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            max_levels: 8,
            max_segments_per_level: 8,
        }
    }
}

/// Segment metadata for level tracking.
#[derive(Debug, Clone)]
pub struct SegmentMeta {
    /// Storage key for this segment.
    pub key: String,
    /// Compaction level (0 = freshly flushed from memtable).
    pub level: u32,
    /// Byte size of the segment.
    pub size: u64,
}

/// Check which level needs compaction and return the level number, or None.
pub fn needs_compaction(segments: &[SegmentMeta], config: &CompactionConfig) -> Option<u32> {
    // Count segments per level.
    let mut counts = vec![0usize; config.max_levels];
    for seg in segments {
        let level = seg.level as usize;
        if level < config.max_levels {
            counts[level] += 1;
        }
    }

    // Find the lowest level that exceeds the threshold.
    for (level, &count) in counts.iter().enumerate() {
        if count >= config.max_segments_per_level && level + 1 < config.max_levels {
            return Some(level as u32);
        }
    }
    None
}

/// Result of a compaction: new segment bytes and keys of merged (to-remove) segments.
pub type CompactionResult = (Vec<u8>, Vec<String>);

/// Perform compaction: merge all segments at `level` into one segment at `level + 1`.
///
/// Returns the merged segment bytes and the keys of segments that were merged
/// (which should be removed from storage after the new segment is written).
pub fn compact_level<B: FtsBackend>(
    backend: &B,
    _collection: &str,
    segments: &[SegmentMeta],
    level: u32,
) -> Result<Option<CompactionResult>, B::Error> {
    let to_merge: Vec<&SegmentMeta> = segments.iter().filter(|s| s.level == level).collect();
    if to_merge.len() < 2 {
        return Ok(None);
    }

    // Load segment data and open readers.
    let mut readers = Vec::with_capacity(to_merge.len());
    let mut merged_keys = Vec::with_capacity(to_merge.len());

    for meta in &to_merge {
        if let Some(data) = backend.read_segment(&meta.key)?
            && let Some(reader) = SegmentReader::open(data)
        {
            readers.push(reader);
            merged_keys.push(meta.key.clone());
        }
    }

    if readers.len() < 2 {
        return Ok(None);
    }

    // N-way merge.
    let merged_term_blocks = merge::merge_segments(&readers);
    let new_segment = writer::build_from_blocks(&merged_term_blocks);

    Ok(Some((new_segment, merged_keys)))
}

/// Generate a segment storage key.
pub fn segment_key(collection: &str, segment_id: u64, level: u32) -> String {
    format!("{collection}:seg:L{level}:{segment_id:016x}")
}

/// Parse the level from a segment key. Returns 0 if unparseable.
pub fn parse_level(key: &str) -> u32 {
    // Format: "{collection}:seg:L{level}:{id}"
    key.split(":seg:L")
        .nth(1)
        .and_then(|s| s.split(':').next())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// Parse the segment ID from a segment key.
pub fn parse_segment_id(key: &str) -> u64 {
    key.rsplit(':')
        .next()
        .and_then(|s| u64::from_str_radix(s, 16).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meta(key: &str, level: u32) -> SegmentMeta {
        SegmentMeta {
            key: key.to_string(),
            level,
            size: 1000,
        }
    }

    #[test]
    fn needs_compaction_basic() {
        let config = CompactionConfig {
            max_levels: 4,
            max_segments_per_level: 3,
        };

        let segments = vec![
            make_meta("s1", 0),
            make_meta("s2", 0),
            make_meta("s3", 0), // 3 at level 0 → trigger
        ];
        assert_eq!(needs_compaction(&segments, &config), Some(0));
    }

    #[test]
    fn no_compaction_under_threshold() {
        let config = CompactionConfig {
            max_levels: 4,
            max_segments_per_level: 3,
        };
        let segments = vec![make_meta("s1", 0), make_meta("s2", 0)];
        assert_eq!(needs_compaction(&segments, &config), None);
    }

    #[test]
    fn compaction_at_higher_level() {
        let config = CompactionConfig {
            max_levels: 4,
            max_segments_per_level: 2,
        };
        let segments = vec![
            make_meta("s1", 0),
            make_meta("s2", 1),
            make_meta("s3", 1), // 2 at level 1 → trigger
        ];
        assert_eq!(needs_compaction(&segments, &config), Some(1));
    }

    #[test]
    fn segment_key_format() {
        let key = segment_key("docs", 42, 0);
        assert!(key.starts_with("docs:seg:L0:"));
        assert_eq!(parse_level(&key), 0);
        assert_eq!(parse_segment_id(&key), 42);
    }

    #[test]
    fn parse_level_and_id() {
        assert_eq!(parse_level("col:seg:L2:000000000000002a"), 2);
        assert_eq!(parse_segment_id("col:seg:L2:000000000000002a"), 42);
    }
}
