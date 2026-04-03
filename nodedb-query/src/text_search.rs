//! In-memory BM25 text search engine.
//!
//! Thin wrapper around `nodedb_fts::FtsIndex<MemoryBackend>`, providing
//! the same API that Lite currently uses. All scoring, tokenization, and
//! fuzzy logic is now in `nodedb-fts`.

pub use nodedb_fts::analyzer::pipeline::analyze;
pub use nodedb_fts::posting::Bm25Params;
pub use nodedb_fts::posting::QueryMode;

use nodedb_fts::backend::FtsBackend;
use nodedb_fts::backend::memory::MemoryBackend;
use nodedb_fts::index::FtsIndex;
use nodedb_fts::posting::TextSearchResult as FtsResult;

/// A single search result with BM25 score.
#[derive(Debug, Clone)]
pub struct TextSearchResult {
    pub doc_id: String,
    pub score: f64,
}

/// In-memory inverted index with BM25 scoring.
///
/// Wraps `nodedb_fts::FtsIndex<MemoryBackend>`. Uses a single implicit
/// collection key so callers don't need to pass one (backward compat).
pub struct InvertedIndex {
    inner: FtsIndex<MemoryBackend>,
}

impl Default for InvertedIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self {
            inner: FtsIndex::new(MemoryBackend::new()),
        }
    }

    /// Index a document's text content.
    ///
    /// `doc_id` is the unique identifier. `text` is analyzed into tokens.
    /// Calling again with the same `doc_id` replaces the previous entry.
    pub fn index_document(&mut self, doc_id: &str, text: &str) {
        // Remove old entry if re-indexing.
        self.remove_document(doc_id);
        // MemoryBackend::Error is infallible in practice.
        let _ = self.inner.index_document("_", doc_id, text);
    }

    /// Remove a document from the index.
    pub fn remove_document(&mut self, doc_id: &str) {
        let _ = self.inner.remove_document("_", doc_id);
    }

    /// Search with BM25 scoring.
    ///
    /// Returns results sorted by descending score, limited to `top_k`.
    pub fn search(
        &self,
        query: &str,
        top_k: usize,
        mode: QueryMode,
        _params: Bm25Params,
    ) -> Vec<TextSearchResult> {
        let fts_mode = match mode {
            QueryMode::And => nodedb_fts::posting::QueryMode::And,
            QueryMode::Or => nodedb_fts::posting::QueryMode::Or,
        };
        let results = self
            .inner
            .search_with_mode("_", query, top_k, false, fts_mode)
            .unwrap_or_default();
        results.into_iter().map(Self::convert_result).collect()
    }

    /// Fuzzy search: find documents matching query terms within Levenshtein distance.
    pub fn search_fuzzy(
        &self,
        query: &str,
        _max_distance: usize,
        top_k: usize,
        _params: Bm25Params,
    ) -> Vec<TextSearchResult> {
        let results = self
            .inner
            .search_with_mode("_", query, top_k, true, nodedb_fts::posting::QueryMode::Or)
            .unwrap_or_default();
        results.into_iter().map(Self::convert_result).collect()
    }

    pub fn doc_count(&self) -> u32 {
        self.inner.index_stats("_").map(|(c, _)| c).unwrap_or(0)
    }

    pub fn token_count(&self) -> usize {
        self.inner
            .backend()
            .collection_terms("_")
            .map(|t: Vec<String>| t.len())
            .unwrap_or(0)
    }

    fn convert_result(r: FtsResult) -> TextSearchResult {
        TextSearchResult {
            doc_id: r.doc_id,
            score: r.score as f64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analyze_basic() {
        let tokens = analyze("The quick brown fox jumps over the lazy dog");
        assert!(!tokens.is_empty());
        assert!(tokens.iter().all(|t| t != "the"));
    }

    #[test]
    fn index_and_search() {
        let mut idx = InvertedIndex::new();
        idx.index_document("d1", "Rust is a systems programming language");
        idx.index_document("d2", "Python is great for machine learning");
        idx.index_document("d3", "Rust and Python are both great languages");

        let results = idx.search("rust programming", 10, QueryMode::Or, Bm25Params::default());
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, "d1");
    }

    #[test]
    fn and_mode() {
        let mut idx = InvertedIndex::new();
        idx.index_document("d1", "Rust programming language");
        idx.index_document("d2", "Python programming language");

        let results = idx.search(
            "rust programming",
            10,
            QueryMode::And,
            Bm25Params::default(),
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "d1");
    }

    #[test]
    fn remove_document() {
        let mut idx = InvertedIndex::new();
        idx.index_document("d1", "hello world");
        assert_eq!(idx.doc_count(), 1);

        idx.remove_document("d1");
        assert_eq!(idx.doc_count(), 0);

        let results = idx.search("hello", 10, QueryMode::Or, Bm25Params::default());
        assert!(results.is_empty());
    }

    #[test]
    fn reindex_replaces() {
        let mut idx = InvertedIndex::new();
        idx.index_document("d1", "old content");
        idx.index_document("d1", "new content");
        assert_eq!(idx.doc_count(), 1);

        let results = idx.search("old", 10, QueryMode::Or, Bm25Params::default());
        assert!(results.is_empty());
    }
}
