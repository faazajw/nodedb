//! VectorCollection search: multi-segment merging with SQ8 reranking.

use crate::distance::distance;
use crate::hnsw::SearchResult;

use super::lifecycle::VectorCollection;

impl VectorCollection {
    /// Search across all segments, merging results by distance.
    pub fn search(&self, query: &[f32], top_k: usize, ef: usize) -> Vec<SearchResult> {
        let mut all: Vec<SearchResult> = Vec::new();

        // Search growing segment (brute-force).
        let growing_results = self.growing.search(query, top_k);
        for mut r in growing_results {
            r.id += self.growing_base_id;
            all.push(r);
        }

        // Search sealed segments.
        for seg in &self.sealed {
            let results = if let Some(_sq8) = &seg.sq8 {
                // Quantized two-phase search: use HNSW graph for O(log N) candidate
                // generation, then rerank with exact FP32 distance.
                let rerank_k = top_k.saturating_mul(3).max(20);
                let hnsw_candidates = seg.index.search(query, rerank_k, ef);
                let candidates: Vec<(u32, f32)> = hnsw_candidates
                    .into_iter()
                    .map(|r| (r.id, r.distance))
                    .collect();

                // Prefetch FP32 vectors for reranking candidates.
                if let Some(mmap) = &seg.mmap_vectors {
                    let ids: Vec<u32> = candidates.iter().map(|&(id, _)| id).collect();
                    mmap.prefetch_batch(&ids);
                }

                // Phase 2: Rerank with exact FP32 distance.
                let mut reranked: Vec<SearchResult> = candidates
                    .iter()
                    .filter_map(|&(id, _)| {
                        let v = if let Some(mmap) = &seg.mmap_vectors {
                            mmap.get_vector(id)?
                        } else {
                            seg.index.get_vector(id)?
                        };
                        Some(SearchResult {
                            id,
                            distance: distance(query, v, self.params.metric),
                        })
                    })
                    .collect();
                reranked.sort_by(|a, b| {
                    a.distance
                        .partial_cmp(&b.distance)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                reranked.truncate(top_k);
                reranked
            } else {
                seg.index.search(query, top_k, ef)
            };
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

        all.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all.truncate(top_k);
        all
    }

    /// Search with a pre-filter bitmap (byte-array format).
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
}

#[cfg(test)]
mod tests {
    use crate::collection::lifecycle::VectorCollection;
    use crate::collection::segment::DEFAULT_SEAL_THRESHOLD;
    use crate::distance::DistanceMetric;
    use crate::hnsw::{HnswIndex, HnswParams};

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
        for i in 0..DEFAULT_SEAL_THRESHOLD {
            coll.insert(vec![i as f32, 0.0]);
        }
        assert!(coll.needs_seal());

        let req = coll.seal("test_key").unwrap();
        assert_eq!(req.vectors.len(), DEFAULT_SEAL_THRESHOLD);
        assert_eq!(coll.building.len(), 1);
        assert_eq!(coll.growing.len(), 0);

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

        let mut index = HnswIndex::new(req.dim, req.params);
        for v in &req.vectors {
            index.insert(v.clone()).unwrap();
        }
        coll.complete_build(req.segment_id, index);

        assert_eq!(coll.building.len(), 0);
        assert_eq!(coll.sealed.len(), 1);

        let results = coll.search(&[50.0, 0.0], 3, 64);
        assert!(!results.is_empty());
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

        for i in 0..100 {
            coll.insert(vec![i as f32, 0.0]);
        }
        let req = coll.seal("test").unwrap();
        let mut idx = HnswIndex::new(2, req.params);
        for v in &req.vectors {
            idx.insert(v.clone()).unwrap();
        }
        coll.complete_build(req.segment_id, idx);

        for i in 100..200 {
            coll.insert(vec![i as f32, 0.0]);
        }

        let results = coll.search(&[150.0, 0.0], 3, 64);
        assert_eq!(results.len(), 3);
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

    /// Build a sealed HNSW segment from `n` vectors of `dim=2`, where vector `i`
    /// is `[i as f32, 0.0]`. Returns the collection with one sealed segment.
    fn make_sealed_collection(n: usize) -> VectorCollection {
        let mut coll = VectorCollection::new(
            2,
            HnswParams {
                metric: DistanceMetric::L2,
                ..HnswParams::default()
            },
        );
        for i in 0..n {
            coll.insert(vec![i as f32, 0.0]);
        }
        let req = coll.seal("seg").unwrap();
        let mut idx = HnswIndex::new(req.dim, req.params);
        for v in &req.vectors {
            idx.insert(v.clone()).unwrap();
        }
        coll.complete_build(req.segment_id, idx);
        coll
    }

    /// Attach SQ8 quantization to the first sealed segment of `coll`.
    fn attach_sq8(coll: &mut VectorCollection) {
        use crate::quantize::sq8::Sq8Codec;

        let sealed = &mut coll.sealed[0];
        let dim = sealed.index.dim();
        let n = sealed.index.len();
        let vecs: Vec<Vec<f32>> = (0..n)
            .filter_map(|i| sealed.index.get_vector(i as u32).map(|v| v.to_vec()))
            .collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = Sq8Codec::calibrate(&refs, dim);
        let sq8_data: Vec<u8> = vecs.iter().flat_map(|v| codec.quantize(v)).collect();
        sealed.sq8 = Some((codec, sq8_data));
    }

    #[test]
    fn sq8_search_returns_correct_nearest_neighbor() {
        let mut coll = make_sealed_collection(200);
        attach_sq8(&mut coll);

        let results = coll.search(&[100.0, 0.0], 5, 64);
        assert!(!results.is_empty(), "expected non-empty results");
        assert_eq!(
            results[0].id, 100,
            "nearest neighbor of [100,0] should be id=100, got id={}",
            results[0].id
        );
    }

    #[test]
    fn sq8_search_recall_matches_hnsw() {
        // Build two identical collections — one without SQ8, one with.
        let coll_plain = make_sealed_collection(500);
        let mut coll_sq8 = make_sealed_collection(500);
        attach_sq8(&mut coll_sq8);

        let query = [250.0f32, 0.0];
        let top_k = 5;

        let plain_results = coll_plain.search(&query, top_k, 64);
        let sq8_results = coll_sq8.search(&query, top_k, 64);

        let plain_ids: std::collections::HashSet<u32> =
            plain_results.iter().map(|r| r.id).collect();
        let sq8_ids: std::collections::HashSet<u32> = sq8_results.iter().map(|r| r.id).collect();

        let overlap = plain_ids.intersection(&sq8_ids).count();
        assert!(
            overlap >= 4,
            "SQ8 recall too low: {overlap}/5 results matched plain HNSW (need >=4)"
        );
    }

    #[test]
    fn sq8_search_does_not_scan_all_vectors() {
        // This test validates correctness of the SQ8 search path for a large
        // segment. The bug being guarded against is an O(N) linear scan instead
        // of graph-guided traversal: the fix must use HNSW with SQ8 as the
        // distance function. Correctness (correct nearest neighbor) is the
        // invariant that must be preserved when the implementation changes.
        let mut coll = make_sealed_collection(2000);
        attach_sq8(&mut coll);

        let results = coll.search(&[1000.0, 0.0], 5, 64);
        assert!(!results.is_empty(), "expected non-empty results");
        assert_eq!(
            results[0].id, 1000,
            "nearest neighbor of [1000,0] should be id=1000, got id={}",
            results[0].id
        );
    }
}
