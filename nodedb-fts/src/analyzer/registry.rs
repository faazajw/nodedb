//! Per-collection analyzer and synonym registry.

use std::collections::HashMap;

use super::language::LanguageAnalyzer;
use super::ngram::{EdgeNgramAnalyzer, NgramAnalyzer};
use super::pipeline::{TextAnalyzer, analyze};
use super::standard::{KeywordAnalyzer, SimpleAnalyzer, StandardAnalyzer};
use super::synonym::SynonymMap;

/// Registry of named text analyzers and synonym maps per collection.
///
/// Collections can configure their analyzer via:
/// `ALTER COLLECTION articles SET text_analyzer = 'german'`
///
/// If no analyzer is set, the standard English analyzer is used.
pub struct AnalyzerRegistry {
    /// Per-collection analyzer override: collection → analyzer instance.
    overrides: HashMap<String, Box<dyn TextAnalyzer>>,
    /// Per-collection synonym maps: collection → SynonymMap.
    synonyms: HashMap<String, SynonymMap>,
}

impl AnalyzerRegistry {
    pub fn new() -> Self {
        Self {
            overrides: HashMap::new(),
            synonyms: HashMap::new(),
        }
    }

    /// Set the analyzer for a collection.
    ///
    /// Supported names: "standard", "simple", "keyword", "ngram", "edge_ngram",
    /// or any Snowball language ("english", "german", "french", etc.).
    ///
    /// N-gram analyzers accept optional parameters: "ngram:2:4" (min:max).
    pub fn set_analyzer(&mut self, collection: &str, analyzer_name: &str) -> bool {
        let analyzer: Box<dyn TextAnalyzer> = match analyzer_name {
            "standard" => Box::new(StandardAnalyzer),
            "simple" => Box::new(SimpleAnalyzer),
            "keyword" => Box::new(KeywordAnalyzer),
            "ngram" => Box::new(NgramAnalyzer::new(3, 4)),
            "edge_ngram" => Box::new(EdgeNgramAnalyzer::new(2, 5)),
            name if name.starts_with("ngram:") => {
                let parts: Vec<&str> = name.splitn(3, ':').collect();
                let min = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(3);
                let max = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);
                Box::new(NgramAnalyzer::new(min, max))
            }
            name if name.starts_with("edge_ngram:") => {
                let parts: Vec<&str> = name.splitn(3, ':').collect();
                let min = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(2);
                let max = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(5);
                Box::new(EdgeNgramAnalyzer::new(min, max))
            }
            lang => match LanguageAnalyzer::new(lang) {
                Some(a) => Box::new(a),
                None => return false,
            },
        };
        self.overrides.insert(collection.to_string(), analyzer);
        true
    }

    /// Add a synonym for a collection. Both term and synonyms are lowercased.
    pub fn add_synonym(&mut self, collection: &str, term: &str, synonyms: &[&str]) {
        self.synonyms
            .entry(collection.to_string())
            .or_default()
            .add(term, synonyms);
    }

    /// Get the synonym map for a collection (if any).
    pub fn get_synonyms(&self, collection: &str) -> Option<&SynonymMap> {
        self.synonyms.get(collection)
    }

    /// Analyze text for a collection, applying synonym expansion at query time.
    ///
    /// For indexing, call `analyze_for_index()` (no synonym expansion).
    /// For querying, call `analyze()` (with synonym expansion).
    pub fn analyze(&self, collection: &str, text: &str) -> Vec<String> {
        let tokens = match self.overrides.get(collection) {
            Some(analyzer) => analyzer.analyze(text),
            None => analyze(text),
        };
        match self.synonyms.get(collection) {
            Some(syn_map) if !syn_map.is_empty() => syn_map.expand(&tokens),
            _ => tokens,
        }
    }

    /// Analyze text for indexing (no synonym expansion).
    pub fn analyze_for_index(&self, collection: &str, text: &str) -> Vec<String> {
        match self.overrides.get(collection) {
            Some(analyzer) => analyzer.analyze(text),
            None => analyze(text),
        }
    }
}

impl Default for AnalyzerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analyzer_registry_with_synonyms() {
        let mut registry = AnalyzerRegistry::new();
        registry.add_synonym("docs", "db", &["databas"]);

        let tokens = registry.analyze("docs", "db query");
        assert!(tokens.contains(&"databas".to_string()));

        let index_tokens = registry.analyze_for_index("docs", "db query");
        assert!(!index_tokens.contains(&"databas".to_string()));
    }

    #[test]
    fn registry_ngram_with_params() {
        let mut registry = AnalyzerRegistry::new();
        assert!(registry.set_analyzer("col", "ngram:2:3"));
        let tokens = registry.analyze_for_index("col", "hello");
        assert_eq!(tokens.len(), 7);
        assert!(tokens.contains(&"he".to_string()));
    }

    #[test]
    fn registry_edge_ngram() {
        let mut registry = AnalyzerRegistry::new();
        assert!(registry.set_analyzer("col", "edge_ngram:1:3"));
        let tokens = registry.analyze_for_index("col", "test");
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0], "t");
        assert_eq!(tokens[2], "tes");
    }

    #[test]
    fn unknown_analyzer_returns_false() {
        let mut registry = AnalyzerRegistry::new();
        assert!(!registry.set_analyzer("col", "klingon"));
    }
}
