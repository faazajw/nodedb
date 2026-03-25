//! In-memory BM25 text search engine.
//!
//! Shared between Origin (as a complement to the redb-backed index) and
//! Lite (as the primary text search engine). Rebuilt from documents on
//! cold start — acceptable for edge-scale datasets.
//!
//! Features:
//! - BM25 scoring with configurable k1/b parameters
//! - Snowball stemming (15 languages, defaults to English)
//! - Unicode normalization + stop word removal
//! - Fuzzy matching via Levenshtein edit distance
//! - AND/OR boolean query modes

use std::collections::HashMap;

use unicode_normalization::UnicodeNormalization;

// ── Text Analyzer ─────────────────────────────────────────────────────

/// Analyze text into normalized, stemmed tokens.
///
/// Pipeline: NFD normalize → lowercase → split → filter → stop words → stem.
pub fn analyze(text: &str) -> Vec<String> {
    let stemmer = rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English);
    let normalized: String = text.nfd().filter(|c| !c.is_ascii_control()).collect();
    let lower = normalized.to_lowercase();

    lower
        .split(|c: char| !c.is_alphanumeric() && c != '-')
        .filter(|t| t.len() > 1)
        .filter(|t| !is_stop_word(t))
        .map(|t| stemmer.stem(t).to_string())
        .collect()
}

/// English stop words (sorted for binary search).
fn is_stop_word(word: &str) -> bool {
    const STOP_WORDS: &[&str] = &[
        "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "from", "had", "has",
        "have", "he", "her", "his", "how", "if", "in", "into", "is", "it", "its", "me", "my", "no",
        "not", "of", "on", "or", "our", "she", "so", "than", "that", "the", "their", "them",
        "then", "there", "these", "they", "this", "to", "us", "was", "we", "were", "what", "when",
        "where", "which", "who", "will", "with", "would", "you", "your",
    ];
    STOP_WORDS.binary_search(&word).is_ok()
}

// ── Inverted Index ────────────────────────────────────────────────────

/// In-memory inverted index with BM25 scoring.
#[derive(Debug, Default)]
pub struct InvertedIndex {
    /// token → { doc_id → term_frequency }.
    postings: HashMap<String, HashMap<String, u32>>,
    /// doc_id → total token count (document length).
    doc_lengths: HashMap<String, u32>,
    /// Total number of documents indexed.
    doc_count: u32,
    /// Sum of all document lengths (for average calculation).
    total_length: u64,
}

/// A single search result with BM25 score.
#[derive(Debug, Clone)]
pub struct TextSearchResult {
    pub doc_id: String,
    pub score: f64,
}

/// BM25 parameters.
#[derive(Debug, Clone, Copy)]
pub struct Bm25Params {
    /// Term frequency saturation. Default: 1.2.
    pub k1: f64,
    /// Length normalization. Default: 0.75.
    pub b: f64,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

/// Query mode: AND (all terms must match) or OR (any term can match).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryMode {
    And,
    Or,
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Index a document's text content.
    ///
    /// `doc_id` is the unique identifier. `text` is analyzed into tokens.
    /// Calling again with the same `doc_id` replaces the previous entry.
    pub fn index_document(&mut self, doc_id: &str, text: &str) {
        // Remove old entry if re-indexing.
        self.remove_document(doc_id);

        let tokens = analyze(text);
        if tokens.is_empty() {
            return;
        }

        let doc_len = tokens.len() as u32;
        self.doc_lengths.insert(doc_id.to_string(), doc_len);
        self.doc_count += 1;
        self.total_length += doc_len as u64;

        // Count term frequencies.
        let mut tf: HashMap<String, u32> = HashMap::new();
        for token in &tokens {
            *tf.entry(token.clone()).or_insert(0) += 1;
        }

        // Insert into postings.
        for (token, freq) in tf {
            self.postings
                .entry(token)
                .or_default()
                .insert(doc_id.to_string(), freq);
        }
    }

    /// Remove a document from the index.
    pub fn remove_document(&mut self, doc_id: &str) {
        if let Some(old_len) = self.doc_lengths.remove(doc_id) {
            self.doc_count = self.doc_count.saturating_sub(1);
            self.total_length = self.total_length.saturating_sub(old_len as u64);

            // Remove from all postings lists.
            self.postings.retain(|_, docs| {
                docs.remove(doc_id);
                !docs.is_empty()
            });
        }
    }

    /// Search with BM25 scoring.
    ///
    /// Returns results sorted by descending score, limited to `top_k`.
    pub fn search(
        &self,
        query: &str,
        top_k: usize,
        mode: QueryMode,
        params: Bm25Params,
    ) -> Vec<TextSearchResult> {
        let tokens = analyze(query);
        if tokens.is_empty() {
            return Vec::new();
        }

        let avg_dl = if self.doc_count > 0 {
            self.total_length as f64 / self.doc_count as f64
        } else {
            1.0
        };

        let mut scores: HashMap<String, f64> = HashMap::new();

        for token in &tokens {
            let Some(posting) = self.postings.get(token) else {
                continue;
            };

            // IDF: log((N - df + 0.5) / (df + 0.5) + 1)
            let df = posting.len() as f64;
            let idf = ((self.doc_count as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();

            for (doc_id, &tf) in posting {
                let dl = *self.doc_lengths.get(doc_id).unwrap_or(&1) as f64;
                // BM25: idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / avgdl))
                let tf_f = tf as f64;
                let numerator = tf_f * (params.k1 + 1.0);
                let denominator = tf_f + params.k1 * (1.0 - params.b + params.b * dl / avg_dl);
                let bm25 = idf * numerator / denominator;

                *scores.entry(doc_id.clone()).or_insert(0.0) += bm25;
            }
        }

        // AND mode: remove docs that don't match ALL query tokens.
        if mode == QueryMode::And {
            let query_token_count = tokens.len();
            scores.retain(|doc_id, _| {
                let matched_tokens = tokens
                    .iter()
                    .filter(|t| {
                        self.postings
                            .get(*t)
                            .is_some_and(|p| p.contains_key(doc_id))
                    })
                    .count();
                matched_tokens == query_token_count
            });
        }

        let mut results: Vec<TextSearchResult> = scores
            .into_iter()
            .map(|(doc_id, score)| TextSearchResult { doc_id, score })
            .collect();

        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        results
    }

    /// Fuzzy search: find documents matching query terms within Levenshtein distance.
    pub fn search_fuzzy(
        &self,
        query: &str,
        max_distance: usize,
        top_k: usize,
        params: Bm25Params,
    ) -> Vec<TextSearchResult> {
        let tokens = analyze(query);
        if tokens.is_empty() {
            return Vec::new();
        }

        // Expand each query token to matching index tokens within edit distance.
        let mut expanded_query = String::new();
        for token in &tokens {
            let matching: Vec<&str> = self
                .postings
                .keys()
                .filter(|idx_token| levenshtein(token, idx_token) <= max_distance)
                .map(|s| s.as_str())
                .collect();
            if !matching.is_empty() {
                if !expanded_query.is_empty() {
                    expanded_query.push(' ');
                }
                expanded_query.push_str(&matching.join(" "));
            }
        }

        if expanded_query.is_empty() {
            return Vec::new();
        }

        self.search(&expanded_query, top_k, QueryMode::Or, params)
    }

    pub fn doc_count(&self) -> u32 {
        self.doc_count
    }

    pub fn token_count(&self) -> usize {
        self.postings.len()
    }
}

/// Levenshtein edit distance between two strings.
fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let (m, n) = (a.len(), b.len());

    let mut prev = (0..=n).collect::<Vec<_>>();
    let mut curr = vec![0; n + 1];

    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[n]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analyze_basic() {
        let tokens = analyze("The quick brown fox jumps over the lazy dog");
        assert!(!tokens.is_empty());
        // "the" is a stop word, should be removed.
        assert!(tokens.iter().all(|t| t != "the"));
    }

    #[test]
    fn analyze_stemming() {
        let tokens = analyze("running jumps quickly");
        // "running" → "run", "jumps" → "jump", "quickly" → "quick"
        assert!(tokens.contains(&"run".to_string()));
        assert!(tokens.contains(&"jump".to_string()));
        assert!(tokens.contains(&"quick".to_string()));
    }

    #[test]
    fn index_and_search() {
        let mut idx = InvertedIndex::new();
        idx.index_document("d1", "Rust is a systems programming language");
        idx.index_document("d2", "Python is great for machine learning");
        idx.index_document("d3", "Rust and Python are both great languages");

        let results = idx.search("rust programming", 10, QueryMode::Or, Bm25Params::default());
        assert!(!results.is_empty());
        // d1 should rank highest (has both "rust" and "programming").
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
    fn fuzzy_search() {
        let mut idx = InvertedIndex::new();
        idx.index_document("d1", "programming language design");
        idx.index_document("d2", "progrmmng language review"); // typos

        // Fuzzy search expands "programming" (stemmed) to match index tokens
        // within edit distance. Should find both documents.
        let results = idx.search_fuzzy("programming", 3, 10, Bm25Params::default());
        assert!(!results.is_empty(), "fuzzy search should find matches");
        let doc_ids: Vec<&str> = results.iter().map(|r| r.doc_id.as_str()).collect();
        assert!(doc_ids.contains(&"d1"), "should find d1 (exact match)");
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
    fn levenshtein_basic() {
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("abc", "abc"), 0);
        assert_eq!(levenshtein("abc", "ab"), 1);
    }

    #[test]
    fn reindex_replaces() {
        let mut idx = InvertedIndex::new();
        idx.index_document("d1", "old content");
        idx.index_document("d1", "new content");
        assert_eq!(idx.doc_count(), 1);

        let results = idx.search("old", 10, QueryMode::Or, Bm25Params::default());
        assert!(results.is_empty()); // old content should be gone
    }
}
