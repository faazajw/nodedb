//! Memory governance tuning.

use serde::{Deserialize, Serialize};

/// Memory governance tuning (overflow region, document cache).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryTuning {
    /// Overflow region initial mmap size in bytes.
    /// See `nodedb_mem::overflow::OverflowRegion::DEFAULT_INITIAL_CAPACITY`.
    #[serde(default = "default_overflow_initial_bytes")]
    pub overflow_initial_bytes: usize,
    /// Overflow region maximum capacity in bytes.
    /// See `nodedb_mem::overflow::OverflowRegion::DEFAULT_MAX_CAPACITY`.
    #[serde(default = "default_overflow_max_bytes")]
    pub overflow_max_bytes: usize,
    /// Per-core LRU document cache size (number of entries).
    /// See `QueryTuning::doc_cache_entries` for the active config value.
    #[serde(default = "default_doc_cache_entries")]
    pub doc_cache_entries: usize,
}

impl Default for MemoryTuning {
    fn default() -> Self {
        Self {
            overflow_initial_bytes: default_overflow_initial_bytes(),
            overflow_max_bytes: default_overflow_max_bytes(),
            doc_cache_entries: default_doc_cache_entries(),
        }
    }
}

fn default_overflow_initial_bytes() -> usize {
    64 * 1024 * 1024
}
fn default_overflow_max_bytes() -> usize {
    1024 * 1024 * 1024
}
fn default_doc_cache_entries() -> usize {
    4096
}
