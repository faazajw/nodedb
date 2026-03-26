// Re-export shared vector engine from nodedb-vector crate.
// The core HNSW implementation lives in the shared crate.
// Origin extends with SIMD distance + quantization.
pub use nodedb_vector::build;
pub use nodedb_vector::distance;
pub use nodedb_vector::hnsw as graph;
pub use nodedb_vector::search;

pub use nodedb_vector::{DistanceMetric, HnswIndex, HnswParams, SearchResult};
