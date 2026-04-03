//! BM25 search over the FtsIndex.

use std::collections::HashMap;

use crate::analyzer::pipeline::analyze;
use crate::backend::FtsBackend;
use crate::bm25::bm25_score;
use crate::index::FtsIndex;
use crate::posting::{QueryMode, TextSearchResult};

impl<B: FtsBackend> FtsIndex<B> {
    /// Search the index using BM25 scoring.
    ///
    /// Analyzes the query, retrieves posting lists, scores each document,
    /// and returns the top-k results sorted by descending score.
    pub fn search(
        &self,
        collection: &str,
        query: &str,
        top_k: usize,
        fuzzy_enabled: bool,
    ) -> Result<Vec<TextSearchResult>, B::Error> {
        self.search_with_mode(collection, query, top_k, fuzzy_enabled, QueryMode::And)
    }

    /// Search with explicit boolean mode (AND or OR).
    pub fn search_with_mode(
        &self,
        collection: &str,
        query: &str,
        top_k: usize,
        fuzzy_enabled: bool,
        mode: QueryMode,
    ) -> Result<Vec<TextSearchResult>, B::Error> {
        let query_tokens = analyze(query);
        if query_tokens.is_empty() {
            return Ok(Vec::new());
        }
        let num_query_terms = query_tokens.len();

        let (total_docs, avg_doc_len) = self.index_stats(collection)?;
        if total_docs == 0 {
            return Ok(Vec::new());
        }

        // (score, fuzzy_flag, term_match_count)
        let mut doc_scores: HashMap<String, (f32, bool, usize)> = HashMap::new();

        for token in &query_tokens {
            let (postings, is_fuzzy) = {
                let exact = self.backend.read_postings(collection, token)?;
                if !exact.is_empty() {
                    (exact, false)
                } else if fuzzy_enabled {
                    self.fuzzy_lookup(collection, token)?
                } else {
                    (Vec::new(), false)
                }
            };

            if postings.is_empty() {
                continue;
            }

            let df = postings.len() as u32;

            for posting in &postings {
                let doc_len = self
                    .backend
                    .read_doc_length(collection, &posting.doc_id)?
                    .unwrap_or(1);

                let mut score = bm25_score(
                    posting.term_freq,
                    df,
                    doc_len,
                    total_docs,
                    avg_doc_len,
                    &self.bm25_params,
                );

                if is_fuzzy {
                    score *= crate::fuzzy::fuzzy_discount(1);
                }

                let entry = doc_scores
                    .entry(posting.doc_id.clone())
                    .or_insert((0.0, false, 0));
                entry.0 += score;
                if is_fuzzy {
                    entry.1 = true;
                }
                entry.2 += 1;
            }
        }

        // AND mode: keep only docs matching all query terms.
        if mode == QueryMode::And && num_query_terms > 1 {
            doc_scores.retain(|_, (_, _, match_count)| *match_count >= num_query_terms);
        }

        let mut results: Vec<TextSearchResult> = doc_scores
            .into_iter()
            .map(|(doc_id, (score, fuzzy_flag, _))| TextSearchResult {
                doc_id,
                score,
                fuzzy: fuzzy_flag,
            })
            .collect();
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::memory::MemoryBackend;
    use crate::index::FtsIndex;
    use crate::posting::QueryMode;

    fn make_index() -> FtsIndex<MemoryBackend> {
        let mut idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("docs", "d1", "The quick brown fox jumps over the lazy dog")
            .unwrap();
        idx.index_document("docs", "d2", "A fast brown dog runs across the field")
            .unwrap();
        idx.index_document("docs", "d3", "Rust programming language for systems")
            .unwrap();
        idx
    }

    #[test]
    fn basic_search() {
        let idx = make_index();
        let results = idx.search("docs", "brown fox", 10, false).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, "d1");
    }

    #[test]
    fn search_with_stemming() {
        let mut idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("docs", "d1", "running distributed databases")
            .unwrap();
        idx.index_document("docs", "d2", "the cat sat on a mat")
            .unwrap();

        let results = idx
            .search("docs", "database distribution", 10, false)
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, "d1");
    }

    #[test]
    fn or_mode() {
        let idx = make_index();
        let results = idx
            .search_with_mode("docs", "brown fox", 10, false, QueryMode::Or)
            .unwrap();
        // OR mode should return docs matching either "brown" or "fox".
        assert!(results.len() >= 2);
    }

    #[test]
    fn and_mode_filters() {
        let mut idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("docs", "d1", "Rust programming language")
            .unwrap();
        idx.index_document("docs", "d2", "Python programming language")
            .unwrap();

        let results = idx
            .search_with_mode("docs", "rust programming", 10, false, QueryMode::And)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "d1");
    }

    #[test]
    fn empty_query() {
        let idx = make_index();
        let results = idx.search("docs", "the a is", 10, false).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn collections_isolated() {
        let mut idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("col_a", "d1", "alpha bravo charlie")
            .unwrap();
        idx.index_document("col_b", "d1", "delta echo foxtrot")
            .unwrap();

        let results = idx.search("col_a", "alpha", 10, false).unwrap();
        assert_eq!(results.len(), 1);

        let results = idx.search("col_b", "alpha", 10, false).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn fuzzy_search() {
        let mut idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("docs", "d1", "distributed database systems")
            .unwrap();

        let results = idx.search("docs", "databse", 10, true).unwrap();
        assert!(!results.is_empty());
        assert!(results[0].fuzzy);
    }
}
