//! Batch distance computation for HNSW neighbor selection.
//!
//! Instead of computing distances one-at-a-time in a loop, collects
//! candidate vectors and computes distances in bulk. This improves
//! cache utilization and enables SIMD-friendly memory access patterns.
//!
//! Used by `select_neighbors_heuristic` in build.rs to accelerate
//! the diversity check during HNSW graph construction.

use crate::distance::{DistanceMetric, distance};

/// Compute distances from a query vector to multiple candidate vectors.
///
/// Returns a Vec of distances, one per candidate, in the same order.
/// Processes candidates in batches for better cache behavior.
pub fn batch_distances(query: &[f32], candidates: &[&[f32]], metric: DistanceMetric) -> Vec<f32> {
    candidates
        .iter()
        .map(|candidate| distance(query, candidate, metric))
        .collect()
}

/// Precompute all pairwise distances between selected neighbors and a candidate.
///
/// For the diversity heuristic: given a candidate and the currently selected
/// set, compute `distance(candidate, selected[i])` for all i.
/// Returns true if the candidate is "diverse" (closer to query than to
/// every selected neighbor).
pub fn is_diverse_batched(
    candidate_vec: &[f32],
    candidate_dist_to_query: f32,
    selected_vecs: &[&[f32]],
    metric: DistanceMetric,
) -> bool {
    for selected in selected_vecs {
        let dist_to_selected = distance(candidate_vec, selected, metric);
        if candidate_dist_to_query > dist_to_selected {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_distances_correctness() {
        let query = [1.0, 0.0, 0.0];
        let c1 = [0.0, 1.0, 0.0];
        let c2 = [1.0, 0.0, 0.0];
        let c3 = [0.0, 0.0, 1.0];

        let dists = batch_distances(&query, &[&c1, &c2, &c3], DistanceMetric::L2);
        assert_eq!(dists.len(), 3);
        // c2 is identical to query → distance 0.
        assert_eq!(dists[1], 0.0);
        // c1 and c3 are equidistant from query.
        assert_eq!(dists[0], dists[2]);
    }

    #[test]
    fn diversity_check() {
        let candidate = [1.0, 0.0];
        let selected1 = [0.9, 0.1]; // Close to candidate.

        // candidate_dist_to_query = 0.5 (arbitrary).
        // dist(candidate, selected1) = sqrt(0.01 + 0.01) = 0.141...
        // Since 0.5 > 0.141, candidate is NOT diverse (farther from query than from selected).
        assert!(!is_diverse_batched(
            &candidate,
            0.5,
            &[&selected1],
            DistanceMetric::L2,
        ));

        // With dist_to_query = 0.01 — candidate is closer to query than to selected.
        assert!(is_diverse_batched(
            &candidate,
            0.01,
            &[&selected1],
            DistanceMetric::L2,
        ));
    }
}
