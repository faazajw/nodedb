//! HNSW search algorithm (Malkov & Yashunin, Algorithm 2).
//!
//! Beam search through the multi-layer graph. No Roaring bitmap dependency —
//! filtering is handled at the `NodeDbLite` layer above.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};

use crate::hnsw::{Candidate, HnswIndex, SearchResult};

impl HnswIndex {
    /// K-NN search with pre-filtering: only consider vectors whose IDs are in `allowed`.
    ///
    /// The graph is still traversed through all nodes for connectivity, but
    /// only nodes in `allowed` are added to the result set. This gives much
    /// better recall than post-filtering because the beam search can explore
    /// deeper before filling the result set.
    ///
    /// `ef` is automatically scaled up to compensate for filter selectivity.
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        allowed: &HashSet<u32>,
    ) -> Vec<SearchResult> {
        assert_eq!(query.len(), self.dim, "query dimension mismatch");
        if self.is_empty() || allowed.is_empty() {
            return Vec::new();
        }

        let ef = ef.max(k);
        let Some(ep) = self.entry_point else {
            return Vec::new();
        };

        // Phase 1: Greedy descent from top layer to layer 1.
        let mut current_ep = ep;
        for layer in (1..=self.max_layer).rev() {
            let results = search_layer(self, query, current_ep, 1, layer, None);
            if let Some(nearest) = results.first() {
                current_ep = nearest.id;
            }
        }

        // Phase 2: Beam search at layer 0 with filter applied.
        let results = search_layer(self, query, current_ep, ef, 0, Some(allowed));

        results
            .into_iter()
            .take(k)
            .map(|c| SearchResult {
                id: c.id,
                distance: c.dist,
            })
            .collect()
    }

    /// K-NN search: find the `k` closest vectors to `query`.
    ///
    /// `ef` controls the search beam width (higher = better recall, slower).
    /// Must be >= k. Typical values: ef = 2*k to 10*k.
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<SearchResult> {
        assert_eq!(query.len(), self.dim, "query dimension mismatch");
        if self.is_empty() {
            return Vec::new();
        }

        let ef = ef.max(k);
        let Some(ep) = self.entry_point else {
            return Vec::new();
        };

        // Phase 1: Greedy descent from top layer to layer 1.
        let mut current_ep = ep;
        for layer in (1..=self.max_layer).rev() {
            let results = search_layer(self, query, current_ep, 1, layer, None);
            if let Some(nearest) = results.first() {
                current_ep = nearest.id;
            }
        }

        // Phase 2: Beam search at layer 0.
        let results = search_layer(self, query, current_ep, ef, 0, None);

        results
            .into_iter()
            .take(k)
            .map(|c| SearchResult {
                id: c.id,
                distance: c.dist,
            })
            .collect()
    }
}

/// Unified HNSW beam search on a single layer with optional pre-filter.
///
/// When `allowed` is `None`, all non-deleted nodes enter the result set.
/// When `allowed` is `Some`, only nodes in the set enter results, but all
/// nodes are traversed for graph connectivity (preserves recall while filtering).
/// Filtered mode uses a 3x internal beam to compensate for filter selectivity.
pub(crate) fn search_layer(
    index: &HnswIndex,
    query: &[f32],
    entry_point: u32,
    ef: usize,
    layer: usize,
    allowed: Option<&HashSet<u32>>,
) -> Vec<Candidate> {
    let mut visited: HashSet<u32> = HashSet::new();
    visited.insert(entry_point);

    let ep_dist = index.dist_to_node(query, entry_point);
    let ep_candidate = Candidate {
        dist: ep_dist,
        id: entry_point,
    };

    let mut candidates: BinaryHeap<Reverse<Candidate>> = BinaryHeap::new();
    candidates.push(Reverse(ep_candidate));

    let mut results: BinaryHeap<Candidate> = BinaryHeap::new();

    let internal_ef = if allowed.is_some() { ef * 3 } else { ef };

    let ep_passes = !index.nodes[entry_point as usize].deleted
        && allowed.is_none_or(|a| a.contains(&entry_point));
    if ep_passes {
        results.push(ep_candidate);
    }

    while let Some(Reverse(current)) = candidates.pop() {
        if let Some(worst) = results.peek()
            && current.dist > worst.dist
            && results.len() >= ef
        {
            break;
        }

        let neighbors = index.neighbors_at(current.id, layer);
        if neighbors.is_empty() && layer >= index.node_num_layers(current.id) {
            continue;
        }

        for &neighbor_id in neighbors {
            if !visited.insert(neighbor_id) {
                continue;
            }

            let dist = index.dist_to_node(query, neighbor_id);
            let neighbor = Candidate {
                dist,
                id: neighbor_id,
            };

            let worst_dist = results.peek().map_or(f32::INFINITY, |w| w.dist);
            let should_explore = dist < worst_dist || results.len() < internal_ef;

            if should_explore {
                candidates.push(Reverse(neighbor));
            }

            let passes = !index.nodes[neighbor_id as usize].deleted
                && allowed.is_none_or(|a| a.contains(&neighbor_id));
            if passes {
                results.push(neighbor);
                if results.len() > ef {
                    results.pop();
                }
            }
        }
    }

    let mut result_vec: Vec<Candidate> = results.into_vec();
    result_vec.sort_unstable_by(|a, b| a.dist.total_cmp(&b.dist));
    result_vec
}

#[cfg(test)]
mod tests {
    use crate::distance::DistanceMetric;
    use crate::hnsw::{HnswIndex, HnswParams};

    fn build_index(n: usize, dim: usize) -> HnswIndex {
        let mut idx = HnswIndex::with_seed(
            dim,
            HnswParams {
                m: 16,
                m0: 32,
                ef_construction: 100,
                metric: DistanceMetric::L2,
            },
            42,
        );
        for i in 0..n {
            let v: Vec<f32> = (0..dim).map(|d| (i * dim + d) as f32).collect();
            idx.insert(v).unwrap();
        }
        idx
    }

    #[test]
    fn search_empty_index() {
        let idx = HnswIndex::new(3, HnswParams::default());
        let results = idx.search(&[1.0, 2.0, 3.0], 5, 50);
        assert!(results.is_empty());
    }

    #[test]
    fn search_single_element() {
        let mut idx = HnswIndex::with_seed(
            2,
            HnswParams {
                m: 4,
                m0: 8,
                ef_construction: 16,
                metric: DistanceMetric::L2,
            },
            1,
        );
        idx.insert(vec![1.0, 0.0]).unwrap();

        let results = idx.search(&[1.0, 0.0], 1, 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 0);
        assert!(results[0].distance < 1e-6);
    }

    #[test]
    fn search_finds_exact_match() {
        let idx = build_index(50, 3);
        let query = idx.get_vector(25).unwrap().to_vec();
        let results = idx.search(&query, 1, 50);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 25);
        assert!(results[0].distance < 1e-6);
    }

    #[test]
    fn search_returns_sorted_by_distance() {
        let idx = build_index(100, 4);
        let query = vec![50.0, 50.0, 50.0, 50.0];
        let results = idx.search(&query, 10, 64);
        assert_eq!(results.len(), 10);

        for w in results.windows(2) {
            assert!(w[0].distance <= w[1].distance);
        }
    }

    #[test]
    fn search_k_larger_than_index() {
        let idx = build_index(5, 2);
        let results = idx.search(&[0.0, 0.0], 20, 50);
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn search_recall_at_10() {
        let idx = build_index(500, 3);
        let query = vec![100.0, 100.0, 100.0];

        let results = idx.search(&query, 10, 128);

        // Brute-force ground truth.
        let mut truth: Vec<(u32, f32)> = (0..500)
            .map(|i| {
                let v = idx.get_vector(i).unwrap();
                let d = crate::distance::l2_squared(&query, v);
                (i, d)
            })
            .collect();
        truth.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let truth_top10: std::collections::HashSet<u32> = truth[..10].iter().map(|t| t.0).collect();

        let found: std::collections::HashSet<u32> = results.iter().map(|r| r.id).collect();
        let recall = found.intersection(&truth_top10).count() as f64 / 10.0;

        assert!(recall >= 0.8, "recall@10 = {recall:.2}, expected >= 0.80");
    }

    #[test]
    fn search_excludes_tombstoned() {
        let mut idx = build_index(20, 3);
        // Delete node 0 (which would be closest to [0,0,0]).
        idx.delete(0);
        let results = idx.search(&[0.0, 0.0, 0.0], 5, 32);
        for r in &results {
            assert_ne!(r.id, 0, "tombstoned node appeared in results");
        }
    }

    #[test]
    fn search_filtered_respects_allowed_set() {
        let idx = build_index(50, 3);
        // Only allow even IDs.
        let allowed: std::collections::HashSet<u32> = (0..50).filter(|i| i % 2 == 0).collect();
        let results = idx.search_filtered(&[0.0, 0.0, 0.0], 5, 64, &allowed);
        assert_eq!(results.len(), 5);
        for r in &results {
            assert!(
                r.id % 2 == 0,
                "filtered result should only contain even IDs, got {}",
                r.id
            );
        }
    }

    #[test]
    fn search_filtered_empty_allowed_returns_empty() {
        let idx = build_index(20, 3);
        let allowed = std::collections::HashSet::new();
        let results = idx.search_filtered(&[0.0, 0.0, 0.0], 5, 64, &allowed);
        assert!(results.is_empty());
    }

    #[test]
    fn search_high_dimensional() {
        let mut idx = HnswIndex::with_seed(
            128,
            HnswParams {
                m: 16,
                m0: 32,
                ef_construction: 100,
                metric: DistanceMetric::Cosine,
            },
            7,
        );
        for i in 0..200 {
            let v: Vec<f32> = (0..128).map(|d| ((i * 128 + d) as f32).sin()).collect();
            idx.insert(v).unwrap();
        }

        let query: Vec<f32> = (0..128).map(|d| (d as f32).sin()).collect();
        let results = idx.search(&query, 5, 64);
        assert_eq!(results.len(), 5);
        for w in results.windows(2) {
            assert!(w[0].distance <= w[1].distance);
        }
    }
}
