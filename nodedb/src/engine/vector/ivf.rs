//! IVF-PQ index for billion-scale datasets.
//!
//! Inverted File with Product Quantization: partition vectors into Voronoi
//! cells using k-means centroids, PQ-compress within cells. At query time,
//! only probe the `nprobe` closest cells instead of scanning all vectors.
//!
//! Trade-offs vs HNSW:
//! - Memory: ~16 bytes/vector (M=8 PQ subvectors + cell ID) vs ~200+ bytes for HNSW
//! - Build: O(N) single-pass assignment vs O(N log N) HNSW construction
//! - Query: O(nprobe × cell_size / N × M) vs O(log N × M) for HNSW
//! - Recall: 85-95% at nprobe=32 vs 95-99% for HNSW
//!
//! Best for: >10M vectors where HNSW memory overhead is too high.

use super::distance::{DistanceMetric, distance};
use super::hnsw::SearchResult;
use super::quantize::pq::PqCodec;

/// IVF-PQ index configuration.
#[derive(Clone)]
pub struct IvfPqParams {
    /// Number of Voronoi cells (partitions). Typical: sqrt(N).
    pub n_cells: usize,
    /// Number of PQ subvectors. Must divide dimension evenly.
    pub pq_m: usize,
    /// Centroids per PQ subvector (fixed at 256 for u8 encoding).
    pub pq_k: usize,
    /// Number of cells to probe at query time. Higher = better recall.
    pub nprobe: usize,
    /// Distance metric.
    pub metric: DistanceMetric,
}

impl Default for IvfPqParams {
    fn default() -> Self {
        Self {
            n_cells: 256,
            pq_m: 8,
            pq_k: 256,
            nprobe: 16,
            metric: DistanceMetric::L2,
        }
    }
}

/// IVF-PQ index: inverted file with product quantization.
pub struct IvfPqIndex {
    dim: usize,
    params: IvfPqParams,
    /// Coarse centroids: `n_cells` × `dim` FP32 vectors.
    centroids: Vec<Vec<f32>>,
    /// PQ codec trained on the dataset.
    pq: Option<PqCodec>,
    /// Per-cell inverted lists: `cells[cell_id]` = list of (vector_id, pq_code).
    cells: Vec<Vec<(u32, Vec<u8>)>>,
    /// Total vectors indexed.
    count: u32,
}

impl IvfPqIndex {
    /// Create an empty IVF-PQ index.
    pub fn new(dim: usize, params: IvfPqParams) -> Self {
        Self {
            dim,
            params,
            centroids: Vec::new(),
            pq: None,
            cells: Vec::new(),
            count: 0,
        }
    }

    /// Train the index from a set of vectors.
    ///
    /// Two-phase training:
    /// 1. K-means on full vectors → coarse centroids (Voronoi cells)
    /// 2. PQ training on residuals (vector - nearest centroid)
    ///
    /// Must be called before `add()`. Typically trained on a representative
    /// sample (10K-100K vectors), then all vectors are added.
    pub fn train(&mut self, vectors: &[&[f32]]) {
        assert!(!vectors.is_empty());
        assert!(self.dim > 0);
        assert!(
            self.dim.is_multiple_of(self.params.pq_m),
            "dim {} must be divisible by pq_m {}",
            self.dim,
            self.params.pq_m
        );

        let n_cells = self.params.n_cells.min(vectors.len());

        // Phase 1: K-means for coarse centroids.
        self.centroids = kmeans_centroids(vectors, self.dim, n_cells, 20);
        self.cells = vec![Vec::new(); self.centroids.len()];

        // Phase 2: Compute residuals and train PQ.
        let mut residuals: Vec<Vec<f32>> = Vec::with_capacity(vectors.len());
        for v in vectors {
            let cell = self.nearest_centroid(v);
            let res: Vec<f32> = v
                .iter()
                .zip(&self.centroids[cell])
                .map(|(a, b)| a - b)
                .collect();
            residuals.push(res);
        }
        let res_refs: Vec<&[f32]> = residuals.iter().map(|r| r.as_slice()).collect();
        self.pq = Some(PqCodec::train(
            &res_refs,
            self.dim,
            self.params.pq_m,
            self.params.pq_k,
            20,
        ));
    }

    /// Add a vector to the index. Returns the assigned ID.
    ///
    /// The index must be trained first. The vector is assigned to the
    /// nearest coarse centroid, then PQ-encoded and stored in that cell.
    pub fn add(&mut self, vector: &[f32]) -> u32 {
        assert_eq!(vector.len(), self.dim);
        let pq = self
            .pq
            .as_ref()
            .expect("index must be trained before add()");

        let cell = self.nearest_centroid(vector);
        let residual: Vec<f32> = vector
            .iter()
            .zip(&self.centroids[cell])
            .map(|(a, b)| a - b)
            .collect();
        let code = pq.encode(&residual);
        let id = self.count;
        self.cells[cell].push((id, code));
        self.count += 1;
        id
    }

    /// Batch add vectors.
    pub fn add_batch(&mut self, vectors: &[&[f32]]) {
        for v in vectors {
            self.add(v);
        }
    }

    /// Search: find top-k nearest neighbors.
    ///
    /// 1. Find `nprobe` closest coarse centroids to query
    /// 2. For each probed cell, compute asymmetric PQ distance to all vectors
    /// 3. Merge and return top-k
    pub fn search(&self, query: &[f32], top_k: usize) -> Vec<SearchResult> {
        assert_eq!(query.len(), self.dim);
        if self.centroids.is_empty() || self.count == 0 {
            return Vec::new();
        }

        let pq = match &self.pq {
            Some(p) => p,
            None => return Vec::new(),
        };

        // Find nprobe closest centroids.
        let nprobe = self.params.nprobe.min(self.centroids.len());
        let mut centroid_dists: Vec<(usize, f32)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(i, c)| (i, distance(query, c, self.params.metric)))
            .collect();
        centroid_dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Build PQ distance table for each probed cell's residual space.
        let mut candidates: Vec<SearchResult> = Vec::new();

        for &(cell_idx, _) in centroid_dists.iter().take(nprobe) {
            // Residual query = query - centroid.
            let residual_query: Vec<f32> = query
                .iter()
                .zip(&self.centroids[cell_idx])
                .map(|(q, c)| q - c)
                .collect();
            let table = pq.build_distance_table(&residual_query);

            for (id, code) in &self.cells[cell_idx] {
                let dist = pq.asymmetric_distance(&table, code);
                candidates.push(SearchResult {
                    id: *id,
                    distance: dist,
                });
            }
        }

        // Top-k selection.
        if candidates.len() > top_k {
            candidates.select_nth_unstable_by(top_k, |a, b| {
                a.distance
                    .partial_cmp(&b.distance)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            candidates.truncate(top_k);
        }
        candidates.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        candidates
    }

    /// Find the nearest coarse centroid for a vector.
    fn nearest_centroid(&self, vector: &[f32]) -> usize {
        let mut best = 0;
        let mut best_dist = f32::MAX;
        for (i, c) in self.centroids.iter().enumerate() {
            let d = distance(vector, c, self.params.metric);
            if d < best_dist {
                best_dist = d;
                best = i;
            }
        }
        best
    }

    pub fn len(&self) -> usize {
        self.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn n_cells(&self) -> usize {
        self.centroids.len()
    }
}

/// Simple k-means for coarse centroid training.
fn kmeans_centroids(data: &[&[f32]], dim: usize, k: usize, max_iter: usize) -> Vec<Vec<f32>> {
    let n = data.len();
    let k = k.min(n);
    if k == 0 {
        return Vec::new();
    }

    // K-means++ init: pick first centroid, then furthest-first.
    let mut centroids: Vec<Vec<f32>> = vec![data[0].to_vec()];
    let mut min_dists = vec![f32::MAX; n];

    for _ in 1..k {
        let last = centroids.last().unwrap();
        for (i, point) in data.iter().enumerate() {
            let d = distance(point, last, DistanceMetric::L2);
            if d < min_dists[i] {
                min_dists[i] = d;
            }
        }
        let best = min_dists
            .iter()
            .enumerate()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(i, _)| i)
            .unwrap_or(0);
        centroids.push(data[best].to_vec());
    }

    // K-means iterations.
    let mut assignments = vec![0usize; n];
    for _ in 0..max_iter {
        let mut changed = false;
        for (i, point) in data.iter().enumerate() {
            let mut best = 0;
            let mut best_d = f32::MAX;
            for (c, centroid) in centroids.iter().enumerate() {
                let d = distance(point, centroid, DistanceMetric::L2);
                if d < best_d {
                    best_d = d;
                    best = c;
                }
            }
            if assignments[i] != best {
                assignments[i] = best;
                changed = true;
            }
        }
        if !changed {
            break;
        }
        let mut sums = vec![vec![0.0f32; dim]; k];
        let mut counts = vec![0usize; k];
        for (i, point) in data.iter().enumerate() {
            let c = assignments[i];
            counts[c] += 1;
            for d in 0..dim {
                sums[c][d] += point[d];
            }
        }
        for c in 0..k {
            if counts[c] > 0 {
                for d in 0..dim {
                    centroids[c][d] = sums[c][d] / counts[c] as f32;
                }
            }
        }
    }
    centroids
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_vectors(n: usize, dim: usize) -> Vec<Vec<f32>> {
        (0..n)
            .map(|i| (0..dim).map(|d| ((i * dim + d) as f32) * 0.01).collect())
            .collect()
    }

    #[test]
    fn train_and_search() {
        let vecs = make_vectors(1000, 16);
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();

        let mut idx = IvfPqIndex::new(
            16,
            IvfPqParams {
                n_cells: 32,
                pq_m: 4,
                pq_k: 32,
                nprobe: 8,
                metric: DistanceMetric::L2,
            },
        );
        idx.train(&refs);
        idx.add_batch(&refs);

        assert_eq!(idx.len(), 1000);

        let query = &vecs[500];
        let results = idx.search(query, 5);
        assert_eq!(results.len(), 5);
        // The exact match (id=500) should be in top results.
        assert!(
            results.iter().any(|r| r.id == 500),
            "exact match not found in top-5: {:?}",
            results.iter().map(|r| r.id).collect::<Vec<_>>()
        );
    }

    #[test]
    fn empty_index() {
        let idx = IvfPqIndex::new(8, IvfPqParams::default());
        assert!(idx.search(&[0.0; 8], 5).is_empty());
    }

    #[test]
    fn recall_check() {
        let vecs = make_vectors(5000, 32);
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();

        let mut idx = IvfPqIndex::new(
            32,
            IvfPqParams {
                n_cells: 64,
                pq_m: 8,
                pq_k: 256,
                nprobe: 16,
                metric: DistanceMetric::L2,
            },
        );
        idx.train(&refs);
        idx.add_batch(&refs);

        // Brute-force ground truth.
        let query = &vecs[2500];
        let mut truth: Vec<(u32, f32)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u32, distance(query, v, DistanceMetric::L2)))
            .collect();
        truth.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let truth_top10: std::collections::HashSet<u32> = truth[..10].iter().map(|t| t.0).collect();

        let results = idx.search(query, 10);
        let found: std::collections::HashSet<u32> = results.iter().map(|r| r.id).collect();
        let recall = found.intersection(&truth_top10).count() as f64 / 10.0;
        assert!(
            recall >= 0.5,
            "IVF-PQ recall@10 = {recall:.2}, expected >= 0.50"
        );
    }
}
