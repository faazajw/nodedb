//! Compaction handler: periodic and on-demand engine compaction.
//!
//! Compaction removes tombstoned vectors from HNSW indexes, compacts CSR
//! write buffers into dense arrays, and sweeps dangling edges from deleted
//! nodes. All operations run on the Data Plane (single-core, no locks).
//!
//! ## What gets compacted
//!
//! - **Vector engine**: `HnswIndex::compact()` on each sealed segment.
//!   Rebuilds the node array with only live nodes, remaps neighbor IDs,
//!   reclaims jemalloc arena memory. At 768-dim FP32 (~3 KiB/vector),
//!   compacting 1M tombstones reclaims ~3 GB.
//!
//! - **CSR index**: `CsrIndex::compact()` merges the mutable write buffer
//!   into the dense adjacency arrays. Eliminates per-node buffer overhead
//!   and restores cache-friendly sequential access.
//!
//! - **Dangling edges**: Removes edges whose source or destination was
//!   deleted (present in `deleted_nodes` set). Cleans both the in-memory
//!   CSR and the persistent redb edge store.
//!
//! ## Triggering
//!
//! - **Periodic**: The runtime event loop calls `run_maintenance()` every
//!   `COMPACTION_INTERVAL` (default 10 minutes). Only compacts collections
//!   with tombstone ratio above the threshold.
//!
//! - **On-demand**: `PhysicalPlan::Compact` dispatched from the Control
//!   Plane. Forces compaction regardless of tombstone ratio (for operator
//!   use, e.g., after a bulk delete).

use tracing::info;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Execute an on-demand compaction request.
    ///
    /// Forces compaction of all vector collections (regardless of tombstone
    /// ratio), CSR compaction, and dangling edge sweep. Returns a summary
    /// payload with compaction statistics.
    pub(in crate::data::executor) fn execute_compact(&mut self, task: &ExecutionTask) -> Response {
        let result = self.run_compaction(true);
        let payload = match rmp_serde::to_vec_named(&result) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "failed to encode compaction stats");
                Vec::new()
            }
        };
        self.response_with_payload(task, payload)
    }

    /// Run all maintenance/compaction tasks.
    ///
    /// Called periodically from the runtime event loop (idle maintenance)
    /// and on-demand via `PhysicalPlan::Compact`.
    ///
    /// When `force` is false (periodic), only compacts collections whose
    /// tombstone ratio exceeds the threshold. When `force` is true
    /// (on-demand), compacts everything.
    pub fn run_compaction(&mut self, force: bool) -> CompactionStats {
        let mut stats = CompactionStats::default();

        // 1. Vector compaction: remove tombstoned nodes from HNSW indexes.
        for (key, collection) in &mut self.vector_collections {
            // Check tombstone ratio across all sealed segments.
            let total_tombstones: usize = collection
                .sealed_segments()
                .iter()
                .map(|seg| seg.index.tombstone_count())
                .sum();
            let total_nodes: usize = collection
                .sealed_segments()
                .iter()
                .map(|seg| seg.index.len())
                .sum();

            if total_tombstones == 0 {
                continue;
            }

            let ratio = if total_nodes > 0 {
                total_tombstones as f64 / total_nodes as f64
            } else {
                0.0
            };

            if !force && ratio < self.compaction_tombstone_threshold {
                continue;
            }

            let removed = collection.compact();
            if removed > 0 {
                info!(
                    core = self.core_id,
                    collection = %key,
                    removed,
                    ratio = format!("{ratio:.2}"),
                    "vector compaction: tombstones removed"
                );
                stats.vectors_compacted += removed;
                stats.collections_compacted += 1;
            }
        }

        // 2. CSR compaction: merge write buffers into dense arrays.
        self.csr.compact();
        stats.csr_compacted = true;

        // 3. Dangling edge sweep.
        stats.edges_swept = self.sweep_dangling_edges();

        if stats.vectors_compacted > 0 || stats.edges_swept > 0 {
            info!(
                core = self.core_id,
                vectors_compacted = stats.vectors_compacted,
                collections_compacted = stats.collections_compacted,
                edges_swept = stats.edges_swept,
                "compaction cycle complete"
            );
        }

        stats
    }

    /// Run maintenance tasks if enough time has elapsed.
    ///
    /// Called from the runtime event loop on every idle wake. Tracks the
    /// last maintenance time internally and skips if the interval hasn't
    /// elapsed. Returns `true` if maintenance was executed.
    pub fn maybe_run_maintenance(&mut self) -> bool {
        let now = std::time::Instant::now();
        if let Some(last) = self.last_maintenance
            && now.duration_since(last) < self.compaction_interval
        {
            return false;
        }
        self.last_maintenance = Some(now);
        self.run_compaction(false);
        true
    }
}

/// Statistics from a compaction cycle.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct CompactionStats {
    /// Number of tombstoned vectors removed across all collections.
    pub vectors_compacted: usize,
    /// Number of collections that had tombstones compacted.
    pub collections_compacted: usize,
    /// Whether CSR write buffers were compacted.
    pub csr_compacted: bool,
    /// Number of dangling edges swept.
    pub edges_swept: usize,
}

#[cfg(test)]
mod tests {
    use crate::engine::vector::hnsw::graph::HnswParams;

    #[test]
    fn compaction_removes_tombstones() {
        // Test HNSW compaction directly (sealed segment tombstone removal).
        let mut idx = crate::engine::vector::hnsw::graph::HnswIndex::new(4, HnswParams::default());
        for i in 0..20u32 {
            idx.insert(vec![i as f32; 4]);
        }
        for i in 0..10u32 {
            idx.delete(i);
        }
        assert_eq!(idx.tombstone_count(), 10);
        assert_eq!(idx.live_count(), 10);

        let removed = idx.compact();
        assert_eq!(removed, 10);
        assert_eq!(idx.live_count(), 10);
        assert_eq!(idx.tombstone_count(), 0);
    }

    #[test]
    fn maintenance_respects_interval() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _req_tx, _resp_rx) =
            crate::data::executor::core_loop::tests::make_core_with_dir(dir.path());

        // First call should run.
        assert!(core.maybe_run_maintenance());

        // Immediate second call should skip.
        assert!(!core.maybe_run_maintenance());
    }

    #[test]
    fn forced_compaction_ignores_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _req_tx, _resp_rx) =
            crate::data::executor::core_loop::tests::make_core_with_dir(dir.path());

        // Force compaction with no data — should succeed without error.
        let stats = core.run_compaction(true);
        assert_eq!(stats.vectors_compacted, 0);
        assert!(stats.csr_compacted);
    }
}
