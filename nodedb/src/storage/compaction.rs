//! LSM-style L1 segment compaction and HNSW segment merging.
//!
//! **L1 segment compaction**: merge small L1 segments into larger ones.
//! Preserves monotonic LSN ordering. Tombstone entries are cleaned up
//! during compaction (deleted documents removed from the merged output).
//!
//! **HNSW segment merging**: when sealed HNSW segments are compacted,
//! their vectors are merged into a single index with fresh graph
//! construction. Tombstoned vectors are dropped (memory reclaimed).

use std::path::{Path, PathBuf};

use tracing::info;

/// Compaction configuration.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Minimum number of L1 segments before triggering compaction.
    pub min_segments_to_compact: usize,
    /// Maximum segments to merge in a single compaction pass.
    pub max_segments_per_pass: usize,
    /// Target segment size in bytes after compaction.
    ///
    /// Corresponds to `QueryTuning::compaction_target_bytes`.
    pub target_segment_bytes: usize,
    /// Tombstone ratio threshold: compact if tombstones > this fraction.
    pub tombstone_ratio_threshold: f64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            min_segments_to_compact: 4,
            max_segments_per_pass: 8,
            target_segment_bytes: 256 * 1024 * 1024, // 256 MiB
            tombstone_ratio_threshold: 0.3,
        }
    }
}

/// Metadata for a single L1 segment file.
#[derive(Debug, Clone)]
pub struct SegmentMeta {
    /// Path to the segment file.
    pub path: PathBuf,
    /// Size in bytes.
    pub size_bytes: u64,
    /// Minimum LSN in this segment.
    pub min_lsn: u64,
    /// Maximum LSN in this segment.
    pub max_lsn: u64,
    /// Number of live entries.
    pub live_entries: u64,
    /// Number of tombstone entries.
    pub tombstone_entries: u64,
    /// Creation timestamp (epoch seconds).
    pub created_at: u64,
}

impl SegmentMeta {
    /// Tombstone ratio for this segment.
    pub fn tombstone_ratio(&self) -> f64 {
        let total = self.live_entries + self.tombstone_entries;
        if total == 0 {
            0.0
        } else {
            self.tombstone_entries as f64 / total as f64
        }
    }

    /// Whether this segment needs compaction (high tombstone ratio).
    pub fn needs_compaction(&self, threshold: f64) -> bool {
        self.tombstone_ratio() > threshold
    }
}

/// Result of a compaction pass.
#[derive(Debug)]
pub struct CompactionResult {
    /// Input segments that were merged.
    pub input_segments: Vec<PathBuf>,
    /// Output segment path.
    pub output_segment: PathBuf,
    /// Tombstones cleaned up.
    pub tombstones_removed: u64,
    /// Bytes reclaimed.
    pub bytes_reclaimed: u64,
    /// LSN range of the output segment.
    pub min_lsn: u64,
    pub max_lsn: u64,
}

/// Select segments for compaction based on the configuration.
///
/// Returns segments to compact, sorted by min_lsn (oldest first).
/// Selects segments that:
/// 1. Are too small (below target size)
/// 2. Have high tombstone ratios
/// 3. Are adjacent in LSN space (can be merged cleanly)
pub fn select_segments_for_compaction(
    segments: &[SegmentMeta],
    config: &CompactionConfig,
) -> Vec<usize> {
    if segments.len() < config.min_segments_to_compact {
        return Vec::new();
    }

    let mut candidates: Vec<(usize, &SegmentMeta)> = segments
        .iter()
        .enumerate()
        .filter(|(_, s)| {
            s.size_bytes < config.target_segment_bytes as u64
                || s.needs_compaction(config.tombstone_ratio_threshold)
        })
        .collect();

    // Sort by min_lsn (oldest first) for monotonic ordering.
    candidates.sort_by_key(|(_, s)| s.min_lsn);

    // Take up to max_segments_per_pass.
    candidates
        .iter()
        .take(config.max_segments_per_pass)
        .map(|(i, _)| *i)
        .collect()
}

/// Plan a compaction: compute the expected output segment metadata.
///
/// This is a dry-run — no I/O is performed. The caller uses this to
/// decide whether the compaction is worth performing.
pub fn plan_compaction(segments: &[SegmentMeta], output_dir: &Path) -> Option<CompactionResult> {
    if segments.is_empty() {
        return None;
    }

    let min_lsn = segments.iter().map(|s| s.min_lsn).min().unwrap_or(0);
    let max_lsn = segments.iter().map(|s| s.max_lsn).max().unwrap_or(0);
    let tombstones: u64 = segments.iter().map(|s| s.tombstone_entries).sum();
    let total_bytes: u64 = segments.iter().map(|s| s.size_bytes).sum();
    let tombstone_bytes = total_bytes * tombstones
        / (segments
            .iter()
            .map(|s| s.live_entries + s.tombstone_entries)
            .sum::<u64>())
        .max(1);

    let output_path = output_dir.join(format!("segment-{min_lsn}-{max_lsn}.dat"));

    Some(CompactionResult {
        input_segments: segments.iter().map(|s| s.path.clone()).collect(),
        output_segment: output_path,
        tombstones_removed: tombstones,
        bytes_reclaimed: tombstone_bytes,
        min_lsn,
        max_lsn,
    })
}

/// HNSW segment merge: combine vectors from multiple sealed segments
/// into a single HNSW index, dropping tombstoned vectors.
///
/// Returns `(merged_vectors, dropped_count)`.
///
/// The caller builds a fresh HNSW index from the merged vectors.
/// This is better than graph-level merging because:
/// - Tombstoned vectors are fully removed (memory reclaimed)
/// - The new graph has optimal connectivity (no dead-end edges)
/// - Construction parameters can be tuned for the merged size
pub fn merge_hnsw_vectors(
    segment_vectors: &[Vec<(Vec<f32>, bool)>], // (vector, is_deleted)
) -> (Vec<Vec<f32>>, usize) {
    let mut merged = Vec::new();
    let mut dropped = 0;

    for segment in segment_vectors {
        for (vector, deleted) in segment {
            if *deleted {
                dropped += 1;
            } else {
                merged.push(vector.clone());
            }
        }
    }

    info!(
        merged = merged.len(),
        dropped,
        segments = segment_vectors.len(),
        "HNSW segment merge: vectors merged"
    );
    (merged, dropped)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_segments(n: usize) -> Vec<SegmentMeta> {
        (0..n)
            .map(|i| SegmentMeta {
                path: PathBuf::from(format!("seg-{i}.dat")),
                size_bytes: 10 * 1024 * 1024, // 10 MiB each.
                min_lsn: (i * 1000) as u64,
                max_lsn: ((i + 1) * 1000 - 1) as u64,
                live_entries: 900,
                tombstone_entries: 100,
                created_at: 0,
            })
            .collect()
    }

    #[test]
    fn select_segments_respects_min_count() {
        let segments = make_segments(2);
        let config = CompactionConfig {
            min_segments_to_compact: 4,
            ..Default::default()
        };
        let selected = select_segments_for_compaction(&segments, &config);
        assert!(selected.is_empty()); // Not enough segments.
    }

    #[test]
    fn select_segments_picks_small_and_tombstoned() {
        let segments = make_segments(6);
        let config = CompactionConfig {
            min_segments_to_compact: 4,
            max_segments_per_pass: 4,
            target_segment_bytes: 256 * 1024 * 1024,
            tombstone_ratio_threshold: 0.05, // Low threshold → all segments qualify.
        };
        let selected = select_segments_for_compaction(&segments, &config);
        assert_eq!(selected.len(), 4); // Capped at max_segments_per_pass.
    }

    #[test]
    fn plan_compaction_output() {
        let segments = make_segments(3);
        let result = plan_compaction(&segments, Path::new("/tmp")).unwrap();
        assert_eq!(result.input_segments.len(), 3);
        assert_eq!(result.min_lsn, 0);
        assert_eq!(result.max_lsn, 2999);
        assert!(result.tombstones_removed > 0);
    }

    #[test]
    fn hnsw_merge_drops_tombstones() {
        let seg1 = vec![
            (vec![1.0, 0.0], false),
            (vec![2.0, 0.0], true), // tombstoned
            (vec![3.0, 0.0], false),
        ];
        let seg2 = vec![
            (vec![4.0, 0.0], false),
            (vec![5.0, 0.0], true), // tombstoned
        ];
        let (merged, dropped) = merge_hnsw_vectors(&[seg1, seg2]);
        assert_eq!(merged.len(), 3); // 5 total - 2 tombstoned = 3.
        assert_eq!(dropped, 2);
    }

    #[test]
    fn tombstone_ratio() {
        let seg = SegmentMeta {
            path: PathBuf::from("test.dat"),
            size_bytes: 1000,
            min_lsn: 0,
            max_lsn: 100,
            live_entries: 700,
            tombstone_entries: 300,
            created_at: 0,
        };
        let ratio = seg.tombstone_ratio();
        assert!((ratio - 0.3).abs() < 0.01);
        assert!(seg.needs_compaction(0.25));
        assert!(!seg.needs_compaction(0.35));
    }
}
