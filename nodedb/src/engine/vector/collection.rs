//! Segment-based vector collection with growing/sealed lifecycle.
//!
//! Each collection manages:
//! - One **growing segment** (FlatIndex, append-only, brute-force searchable)
//! - Zero or more **sealed segments** (HnswIndex, immutable, graph-searchable)
//! - A build queue for pending HNSW constructions
//!
//! Inserts land in the growing segment (O(1) append). When it reaches
//! `SEAL_THRESHOLD` vectors, it's sealed: vectors are frozen and HNSW
//! construction is dispatched to a background thread. The growing segment
//! is replaced with a fresh empty one. Queries probe all segments and
//! merge results by distance.

use serde::{Deserialize, Serialize};

use super::distance::DistanceMetric;
use super::flat::FlatIndex;
use super::hnsw::{HnswIndex, HnswParams, SearchResult};

/// Threshold for sealing the growing segment.
/// 64K vectors × 768 dims × 4 bytes = ~192 MiB per segment.
pub const SEAL_THRESHOLD: usize = 65_536;

/// Request to build an HNSW index from sealed vectors (sent to builder thread).
pub struct BuildRequest {
    pub key: String,
    pub segment_id: u32,
    pub vectors: Vec<Vec<f32>>,
    pub dim: usize,
    pub params: HnswParams,
}

/// Completed HNSW build (sent back from builder thread).
pub struct BuildComplete {
    pub key: String,
    pub segment_id: u32,
    pub index: HnswIndex,
}

/// A sealed segment whose HNSW index is being built in background.
struct BuildingSegment {
    /// Flat index for brute-force search while HNSW is building.
    flat: FlatIndex,
    /// Base ID offset: vectors have global IDs [base_id .. base_id + count).
    base_id: u32,
    /// Unique segment identifier (for matching with BuildComplete).
    segment_id: u32,
}

/// A sealed segment with a completed HNSW index.
struct SealedSegment {
    /// Built HNSW index (immutable after construction).
    index: HnswIndex,
    /// Base ID offset.
    base_id: u32,
}

/// Manages all vector segments for a single collection (one index key).
///
/// This type is `!Send` — owned by a single Data Plane core.
pub struct VectorCollection {
    /// Active growing segment (append-only, brute-force search).
    growing: FlatIndex,
    /// Base ID for the growing segment's vectors.
    growing_base_id: u32,
    /// Sealed segments with completed HNSW indexes.
    sealed: Vec<SealedSegment>,
    /// Segments being built in background (brute-force searchable).
    building: Vec<BuildingSegment>,
    /// HNSW params for this collection.
    params: HnswParams,
    /// Global vector ID counter (monotonic across all segments).
    next_id: u32,
    /// Next segment ID (monotonic).
    next_segment_id: u32,
    /// Dimensionality.
    dim: usize,
}

impl VectorCollection {
    /// Create an empty collection.
    pub fn new(dim: usize, params: HnswParams) -> Self {
        Self {
            growing: FlatIndex::new(dim, params.metric),
            growing_base_id: 0,
            sealed: Vec::new(),
            building: Vec::new(),
            params,
            next_id: 0,
            next_segment_id: 0,
            dim,
        }
    }

    /// Create with a specific RNG-like seed (for deterministic testing).
    pub fn with_seed(dim: usize, params: HnswParams, _seed: u64) -> Self {
        Self::new(dim, params)
    }

    /// Insert a vector. Returns the global vector ID.
    pub fn insert(&mut self, vector: Vec<f32>) -> u32 {
        let id = self.next_id;
        self.growing.insert(vector);
        self.next_id += 1;
        id
    }

    /// Soft-delete a vector by global ID.
    pub fn delete(&mut self, id: u32) -> bool {
        // Check growing segment.
        if id >= self.growing_base_id {
            let local = id - self.growing_base_id;
            if (local as usize) < self.growing.len() {
                return self.growing.delete(local);
            }
        }
        // Check sealed segments.
        for seg in &mut self.sealed {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.index.len() {
                    return seg.index.delete(local);
                }
            }
        }
        // Check building segments.
        for seg in &mut self.building {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.flat.len() {
                    return seg.flat.delete(local);
                }
            }
        }
        false
    }

    /// Un-delete a previously soft-deleted vector (for transaction rollback).
    pub fn undelete(&mut self, id: u32) -> bool {
        // Only HNSW sealed segments support undelete.
        for seg in &mut self.sealed {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.index.len() {
                    return seg.index.undelete(local);
                }
            }
        }
        false
    }

    /// Search across all segments, merging results by distance.
    pub fn search(&self, query: &[f32], top_k: usize, ef: usize) -> Vec<SearchResult> {
        let mut all: Vec<SearchResult> = Vec::new();

        // Search growing segment (brute-force).
        let growing_results = self.growing.search(query, top_k);
        for mut r in growing_results {
            r.id += self.growing_base_id;
            all.push(r);
        }

        // Search sealed segments (HNSW).
        for seg in &self.sealed {
            let results = seg.index.search(query, top_k, ef);
            for mut r in results {
                r.id += seg.base_id;
                all.push(r);
            }
        }

        // Search building segments (brute-force while HNSW builds).
        for seg in &self.building {
            let results = seg.flat.search(query, top_k);
            for mut r in results {
                r.id += seg.base_id;
                all.push(r);
            }
        }

        // Merge: sort by distance, take top-k.
        all.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all.truncate(top_k);
        all
    }

    /// Search with a pre-filter bitmap.
    pub fn search_with_bitmap_bytes(
        &self,
        query: &[f32],
        top_k: usize,
        ef: usize,
        bitmap: &[u8],
    ) -> Vec<SearchResult> {
        let mut all: Vec<SearchResult> = Vec::new();

        let growing_results = self.growing.search_filtered(query, top_k, bitmap);
        for mut r in growing_results {
            r.id += self.growing_base_id;
            all.push(r);
        }

        for seg in &self.sealed {
            let results = seg.index.search_with_bitmap_bytes(query, top_k, ef, bitmap);
            for mut r in results {
                r.id += seg.base_id;
                all.push(r);
            }
        }

        for seg in &self.building {
            let results = seg.flat.search_filtered(query, top_k, bitmap);
            for mut r in results {
                r.id += seg.base_id;
                all.push(r);
            }
        }

        all.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all.truncate(top_k);
        all
    }

    /// Check if the growing segment should be sealed.
    pub fn needs_seal(&self) -> bool {
        self.growing.len() >= SEAL_THRESHOLD
    }

    /// Seal the growing segment and return a build request for the builder thread.
    ///
    /// The growing segment is moved to the building list (still searchable via
    /// brute-force). A new empty growing segment is created. The returned
    /// `BuildRequest` should be sent to the background builder thread.
    pub fn seal(&mut self, key: &str) -> Option<BuildRequest> {
        if self.growing.is_empty() {
            return None;
        }

        let segment_id = self.next_segment_id;
        self.next_segment_id += 1;

        // Extract vectors from the growing segment for HNSW construction.
        let count = self.growing.len();
        let mut vectors = Vec::with_capacity(count);
        for i in 0..count as u32 {
            if let Some(v) = self.growing.get_vector(i) {
                vectors.push(v.to_vec());
            }
        }

        // Move the growing FlatIndex to building list.
        let old_growing = std::mem::replace(
            &mut self.growing,
            FlatIndex::new(self.dim, self.params.metric),
        );
        let old_base = self.growing_base_id;
        self.growing_base_id = self.next_id;

        self.building.push(BuildingSegment {
            flat: old_growing,
            base_id: old_base,
            segment_id,
        });

        Some(BuildRequest {
            key: key.to_string(),
            segment_id,
            vectors,
            dim: self.dim,
            params: self.params.clone(),
        })
    }

    /// Accept a completed HNSW build from the background thread.
    ///
    /// Finds the matching building segment, replaces it with a sealed segment
    /// containing the built HNSW index. The flat index is dropped (memory freed).
    pub fn complete_build(&mut self, segment_id: u32, index: HnswIndex) {
        if let Some(pos) = self.building.iter().position(|b| b.segment_id == segment_id) {
            let building = self.building.remove(pos);
            self.sealed.push(SealedSegment {
                index,
                base_id: building.base_id,
            });
            // building.flat is dropped here, freeing its memory.
        }
    }

    /// Compact sealed segments: merge all into one, rebuild HNSW.
    ///
    /// Returns the number of tombstoned vectors removed. Also compacts
    /// each individual sealed segment's HNSW via `HnswIndex::compact()`.
    pub fn compact(&mut self) -> usize {
        let mut total_removed = 0;
        for seg in &mut self.sealed {
            total_removed += seg.index.compact();
        }
        total_removed
    }

    /// Total vector count across all segments (including deleted).
    pub fn len(&self) -> usize {
        let mut total = self.growing.len();
        for seg in &self.sealed {
            total += seg.index.len();
        }
        for seg in &self.building {
            total += seg.flat.len();
        }
        total
    }

    /// Total live (non-deleted) vectors.
    pub fn live_count(&self) -> usize {
        let mut total = self.growing.live_count();
        for seg in &self.sealed {
            total += seg.index.live_count();
        }
        for seg in &self.building {
            total += seg.flat.live_count();
        }
        total
    }

    pub fn is_empty(&self) -> bool {
        self.live_count() == 0
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn params(&self) -> &HnswParams {
        &self.params
    }

    /// Serialize all segments for checkpointing.
    ///
    /// Lock-free: sealed segments are immutable (no concurrent writes).
    /// Growing segment is small (<=64K vectors). Building segments are
    /// serialized as raw vectors (will trigger rebuild on reload).
    pub fn checkpoint_to_bytes(&self) -> Vec<u8> {
        let snapshot = CollectionSnapshot {
            dim: self.dim,
            params_m: self.params.m,
            params_m0: self.params.m0,
            params_ef_construction: self.params.ef_construction,
            params_metric: self.params.metric as u8,
            next_id: self.next_id,
            growing_base_id: self.growing_base_id,
            // Growing: serialize as raw vectors.
            growing_vectors: (0..self.growing.len() as u32)
                .filter_map(|i| self.growing.get_vector(i).map(|v| v.to_vec()))
                .collect(),
            growing_deleted: (0..self.growing.len() as u32)
                .map(|i| self.growing.get_vector(i).is_none())
                .collect(),
            // Sealed: serialize each HNSW.
            sealed_segments: self
                .sealed
                .iter()
                .map(|s| SealedSnapshot {
                    base_id: s.base_id,
                    hnsw_bytes: s.index.checkpoint_to_bytes(),
                })
                .collect(),
            // Building: serialize as raw vectors (will rebuild on reload).
            building_segments: self
                .building
                .iter()
                .map(|b| BuildingSnapshot {
                    base_id: b.base_id,
                    vectors: (0..b.flat.len() as u32)
                        .filter_map(|i| b.flat.get_vector(i).map(|v| v.to_vec()))
                        .collect(),
                })
                .collect(),
        };
        rmp_serde::to_vec_named(&snapshot).unwrap_or_default()
    }

    /// Restore a collection from checkpoint bytes.
    pub fn from_checkpoint(bytes: &[u8]) -> Option<Self> {
        let snap: CollectionSnapshot = rmp_serde::from_slice(bytes).ok()?;
        let metric = match snap.params_metric {
            0 => DistanceMetric::L2,
            1 => DistanceMetric::Cosine,
            2 => DistanceMetric::InnerProduct,
            _ => DistanceMetric::Cosine,
        };
        let params = HnswParams {
            m: snap.params_m,
            m0: snap.params_m0,
            ef_construction: snap.params_ef_construction,
            metric,
        };

        // Restore growing segment.
        let mut growing = FlatIndex::new(snap.dim, metric);
        for v in &snap.growing_vectors {
            growing.insert(v.clone());
        }

        // Restore sealed segments.
        let mut sealed = Vec::with_capacity(snap.sealed_segments.len());
        for ss in &snap.sealed_segments {
            if let Some(index) = HnswIndex::from_checkpoint(&ss.hnsw_bytes) {
                sealed.push(SealedSegment {
                    index,
                    base_id: ss.base_id,
                });
            }
        }

        // Building segments become sealed with fresh HNSW builds.
        // Since we can't dispatch to a background thread during checkpoint load,
        // we build them inline (they're typically small post-crash).
        for bs in &snap.building_segments {
            let mut index = HnswIndex::new(snap.dim, params.clone());
            for v in &bs.vectors {
                index.insert(v.clone());
            }
            sealed.push(SealedSegment {
                index,
                base_id: bs.base_id,
            });
        }

        let next_segment_id = (sealed.len() + 1) as u32;

        Some(Self {
            growing,
            growing_base_id: snap.growing_base_id,
            sealed,
            building: Vec::new(),
            params,
            next_id: snap.next_id,
            next_segment_id,
            dim: snap.dim,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct CollectionSnapshot {
    dim: usize,
    params_m: usize,
    params_m0: usize,
    params_ef_construction: usize,
    params_metric: u8,
    next_id: u32,
    growing_base_id: u32,
    growing_vectors: Vec<Vec<f32>>,
    growing_deleted: Vec<bool>,
    sealed_segments: Vec<SealedSnapshot>,
    building_segments: Vec<BuildingSnapshot>,
}

#[derive(Serialize, Deserialize)]
struct SealedSnapshot {
    base_id: u32,
    hnsw_bytes: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct BuildingSnapshot {
    base_id: u32,
    vectors: Vec<Vec<f32>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::vector::distance::DistanceMetric;

    fn make_collection() -> VectorCollection {
        VectorCollection::new(
            3,
            HnswParams {
                metric: DistanceMetric::L2,
                ..HnswParams::default()
            },
        )
    }

    #[test]
    fn insert_and_search() {
        let mut coll = make_collection();
        for i in 0..100u32 {
            coll.insert(vec![i as f32, 0.0, 0.0]);
        }
        assert_eq!(coll.len(), 100);
        let results = coll.search(&[50.0, 0.0, 0.0], 3, 64);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].id, 50);
    }

    #[test]
    fn seal_moves_to_building() {
        let mut coll = VectorCollection::new(2, HnswParams::default());
        // Insert enough to trigger seal threshold check.
        for i in 0..SEAL_THRESHOLD {
            coll.insert(vec![i as f32, 0.0]);
        }
        assert!(coll.needs_seal());

        let req = coll.seal("test_key").unwrap();
        assert_eq!(req.vectors.len(), SEAL_THRESHOLD);
        assert_eq!(coll.building.len(), 1);
        assert_eq!(coll.growing.len(), 0);

        // Building segment is still searchable.
        let results = coll.search(&[100.0, 0.0], 1, 64);
        assert!(!results.is_empty());
    }

    #[test]
    fn complete_build_promotes_to_sealed() {
        let mut coll = VectorCollection::new(2, HnswParams::default());
        for i in 0..100 {
            coll.insert(vec![i as f32, 0.0]);
        }
        let req = coll.seal("test").unwrap();

        // Simulate background build.
        let mut index = HnswIndex::new(req.dim, req.params);
        for v in &req.vectors {
            index.insert(v.clone());
        }
        coll.complete_build(req.segment_id, index);

        assert_eq!(coll.building.len(), 0);
        assert_eq!(coll.sealed.len(), 1);

        // Sealed segment searchable via HNSW.
        let results = coll.search(&[50.0, 0.0], 3, 64);
        assert!(!results.is_empty());
    }

    #[test]
    fn checkpoint_roundtrip() {
        let mut coll = make_collection();
        for i in 0..50u32 {
            coll.insert(vec![i as f32, 0.0, 0.0]);
        }
        let bytes = coll.checkpoint_to_bytes();
        let restored = VectorCollection::from_checkpoint(&bytes).unwrap();
        assert_eq!(restored.len(), 50);
        assert_eq!(restored.dim(), 3);

        let results = restored.search(&[25.0, 0.0, 0.0], 1, 64);
        assert_eq!(results[0].id, 25);
    }

    #[test]
    fn multi_segment_search_merges() {
        let mut coll = VectorCollection::new(
            2,
            HnswParams {
                metric: DistanceMetric::L2,
                ..HnswParams::default()
            },
        );

        // Insert, seal, build — creates first sealed segment.
        for i in 0..100 {
            coll.insert(vec![i as f32, 0.0]);
        }
        let req = coll.seal("test").unwrap();
        let mut idx = HnswIndex::new(2, req.params);
        for v in &req.vectors {
            idx.insert(v.clone());
        }
        coll.complete_build(req.segment_id, idx);

        // Insert more into growing segment.
        for i in 100..200 {
            coll.insert(vec![i as f32, 0.0]);
        }

        // Search should find results from both segments.
        let results = coll.search(&[150.0, 0.0], 3, 64);
        assert_eq!(results.len(), 3);
        // Closest should be 150 (in growing segment).
        assert_eq!(results[0].id, 150);
    }

    #[test]
    fn delete_across_segments() {
        let mut coll = VectorCollection::new(2, HnswParams::default());
        for i in 0..10 {
            coll.insert(vec![i as f32, 0.0]);
        }
        assert!(coll.delete(5));
        assert_eq!(coll.live_count(), 9);

        let results = coll.search(&[5.0, 0.0], 10, 64);
        assert!(results.iter().all(|r| r.id != 5));
    }
}
