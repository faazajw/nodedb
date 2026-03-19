use super::super::distance::{DistanceMetric, distance};

/// HNSW index parameters.
#[derive(Debug, Clone)]
pub struct HnswParams {
    /// Max bidirectional connections per node at layers > 0.
    pub m: usize,
    /// Max connections at layer 0 (typically 2*M for denser base layer).
    pub m0: usize,
    /// Dynamic candidate list size during construction. Higher = better
    /// recall at the cost of slower inserts.
    pub ef_construction: usize,
    /// Distance metric for similarity computation.
    pub metric: DistanceMetric,
}

impl Default for HnswParams {
    fn default() -> Self {
        Self {
            m: 16,
            m0: 32,
            ef_construction: 200,
            metric: DistanceMetric::Cosine,
        }
    }
}

/// Result of a k-NN search.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Internal node identifier (insertion order).
    pub id: u32,
    /// Distance from the query vector under the configured metric.
    pub distance: f32,
}

/// A node in the HNSW graph.
///
/// Stores the full-precision vector (FP32) for structural integrity during
/// construction, and per-layer neighbor lists.
pub(super) struct Node {
    /// Full-precision vector data.
    pub vector: Vec<f32>,
    /// Neighbors at each layer this node participates in.
    /// `neighbors[layer]` is the list of neighbor node IDs at that layer.
    pub neighbors: Vec<Vec<u32>>,
    /// Tombstone flag. Soft-deleted nodes are excluded from search results
    /// and neighbor selection, but remain in the graph for navigation.
    /// This preserves graph connectivity without expensive restructuring.
    pub deleted: bool,
}

/// Hierarchical Navigable Small World graph index.
///
/// Production implementation per Malkov & Yashunin (2018):
/// - Multi-layer graph with exponential layer assignment
/// - FP32 construction for structural integrity
/// - Heuristic neighbor selection (Algorithm 4) for diverse connectivity
/// - Beam search with configurable ef parameter
/// - Roaring bitmap pre-filtering for HNSW traversal
///
/// This type is intentionally `!Send` — owned by a single Data Plane core.
pub struct HnswIndex {
    pub(super) params: HnswParams,
    pub(super) dim: usize,
    pub(super) nodes: Vec<Node>,
    pub(super) entry_point: Option<u32>,
    pub(super) max_layer: usize,
    pub(super) rng: Xorshift64,
}

/// Lightweight xorshift64 PRNG for layer assignment. No external dependency.
pub(super) struct Xorshift64(u64);

impl Xorshift64 {
    pub fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    pub fn next_f64(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 as f64) / (u64::MAX as f64)
    }
}

/// Ordered candidate used in priority queues during search and construction.
///
/// Implements `Ord` by distance (ascending), then by id for stability.
/// Rust's `BinaryHeap` is a max-heap, so use `Reverse<Candidate>` for min-heap.
#[derive(Clone, Copy, PartialEq)]
pub(super) struct Candidate {
    pub dist: f32,
    pub id: u32,
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.dist
            .partial_cmp(&other.dist)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(self.id.cmp(&other.id))
    }
}

impl HnswIndex {
    /// Create a new empty HNSW index for vectors of the given dimensionality.
    pub fn new(dim: usize, params: HnswParams) -> Self {
        Self {
            dim,
            nodes: Vec::new(),
            entry_point: None,
            max_layer: 0,
            rng: Xorshift64::new(42),
            params,
        }
    }

    /// Create with a specific RNG seed (for deterministic testing).
    pub fn with_seed(dim: usize, params: HnswParams, seed: u64) -> Self {
        Self {
            dim,
            nodes: Vec::new(),
            entry_point: None,
            max_layer: 0,
            rng: Xorshift64::new(seed),
            params,
        }
    }

    /// Return the total number of nodes (including tombstoned).
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Return the number of live (non-deleted) vectors.
    pub fn live_count(&self) -> usize {
        self.nodes.len() - self.tombstone_count()
    }

    /// Return the number of tombstoned (soft-deleted) vectors.
    pub fn tombstone_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.deleted).count()
    }

    /// Tombstone ratio: fraction of nodes that are deleted.
    /// High ratio (>0.3) indicates the index needs compaction.
    pub fn tombstone_ratio(&self) -> f64 {
        if self.nodes.is_empty() {
            0.0
        } else {
            self.tombstone_count() as f64 / self.nodes.len() as f64
        }
    }

    /// Check whether the index contains no live vectors.
    pub fn is_empty(&self) -> bool {
        self.live_count() == 0
    }

    /// Soft-delete a vector by internal node ID.
    ///
    /// The node is marked as tombstoned: it is excluded from search results
    /// and future neighbor selection, but its position in the graph is
    /// preserved for navigation. This is O(1) — no graph restructuring.
    ///
    /// Returns `true` if the node was found and deleted, `false` if the ID
    /// is out of range or already deleted.
    pub fn delete(&mut self, id: u32) -> bool {
        if let Some(node) = self.nodes.get_mut(id as usize) {
            if node.deleted {
                return false;
            }
            node.deleted = true;
            true
        } else {
            false
        }
    }

    /// Check whether a node is tombstoned.
    pub fn is_deleted(&self, id: u32) -> bool {
        self.nodes.get(id as usize).is_none_or(|n| n.deleted)
    }

    /// Return the vector dimensionality this index was created for.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Retrieve a stored vector by node ID.
    pub fn get_vector(&self, id: u32) -> Option<&[f32]> {
        self.nodes.get(id as usize).map(|n| n.vector.as_slice())
    }

    /// Access the index parameters.
    pub fn params(&self) -> &HnswParams {
        &self.params
    }

    /// Current entry point node ID.
    pub fn entry_point(&self) -> Option<u32> {
        self.entry_point
    }

    /// Highest layer in the graph.
    pub fn max_layer(&self) -> usize {
        self.max_layer
    }

    /// Current RNG state (for snapshot reproducibility).
    pub fn rng_state(&self) -> u64 {
        self.rng.0
    }

    /// Export all vectors for snapshot transfer.
    pub fn export_vectors(&self) -> Vec<Vec<f32>> {
        self.nodes.iter().map(|n| n.vector.clone()).collect()
    }

    /// Export all neighbor lists for snapshot transfer.
    pub fn export_neighbors(&self) -> Vec<Vec<Vec<u32>>> {
        self.nodes.iter().map(|n| n.neighbors.clone()).collect()
    }

    /// Assign a random layer for a new node using the exponential distribution.
    /// layer = floor(-ln(uniform()) * m_L) where m_L = 1 / ln(M).
    pub(super) fn random_layer(&mut self) -> usize {
        let ml = 1.0 / (self.params.m as f64).ln();
        let r = self.rng.next_f64().max(f64::MIN_POSITIVE);
        (-r.ln() * ml).floor() as usize
    }

    /// Compute distance between a query vector and a stored node.
    pub(super) fn dist_to_node(&self, query: &[f32], node_id: u32) -> f32 {
        distance(
            query,
            &self.nodes[node_id as usize].vector,
            self.params.metric,
        )
    }

    /// Max neighbors allowed at a given layer.
    pub(super) fn max_neighbors(&self, layer: usize) -> usize {
        if layer == 0 {
            self.params.m0
        } else {
            self.params.m
        }
    }
}
