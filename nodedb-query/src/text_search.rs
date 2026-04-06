//! Full-text search re-exports from `nodedb-fts`.
//!
//! Query-layer code that needs BM25 scoring, analyzers, or fuzzy matching
//! imports from here. The implementation lives in the shared `nodedb-fts` crate.
//!
//! Lite: uses `engine::fts::FtsCollectionManager` (wraps `FtsIndex<MemoryBackend>`).
//! Origin: uses `engine::sparse::fts_redb` (wraps `FtsIndex<RedbBackend>`).

pub use nodedb_fts::FtsIndex;
pub use nodedb_fts::analyzer::pipeline::analyze;
pub use nodedb_fts::backend::FtsBackend;
pub use nodedb_fts::backend::memory::MemoryBackend;
pub use nodedb_fts::posting::{Bm25Params, MatchOffset, Posting, QueryMode, TextSearchResult};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analyze_basic() {
        let tokens = analyze("The quick brown fox jumps over the lazy dog");
        assert!(!tokens.is_empty());
        assert!(tokens.iter().all(|t| t != "the"));
    }
}
