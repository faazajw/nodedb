//! Standard, Simple, and Keyword analyzers.

use super::pipeline::{TextAnalyzer, analyze};

/// Standard English text analyzer (default).
///
/// Pipeline: NFD normalize → lowercase → split → filter → stop words → Snowball stem.
pub struct StandardAnalyzer;

impl TextAnalyzer for StandardAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        analyze(text)
    }

    fn name(&self) -> &str {
        "standard"
    }
}

/// Simple analyzer: lowercase + split on whitespace. No stemming or stop words.
///
/// Useful for exact-match fields (email addresses, tags, identifiers).
pub struct SimpleAnalyzer;

impl TextAnalyzer for SimpleAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split_whitespace()
            .filter(|w| w.len() > 1)
            .map(|w| w.to_string())
            .collect()
    }

    fn name(&self) -> &str {
        "simple"
    }
}

/// Keyword analyzer: treats entire input as a single token (lowercase).
///
/// Used for fields where the entire value is the token (status fields,
/// enum-like values, exact-match tags).
pub struct KeywordAnalyzer;

impl TextAnalyzer for KeywordAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        let trimmed = text.trim().to_lowercase();
        if trimmed.is_empty() {
            Vec::new()
        } else {
            vec![trimmed]
        }
    }

    fn name(&self) -> &str {
        "keyword"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_analyzer() {
        let analyzer = SimpleAnalyzer;
        let tokens = analyzer.analyze("Hello World foo");
        assert_eq!(tokens, vec!["hello", "world", "foo"]);
    }

    #[test]
    fn keyword_analyzer() {
        let analyzer = KeywordAnalyzer;
        let tokens = analyzer.analyze("Active Status");
        assert_eq!(tokens, vec!["active status"]);
    }

    #[test]
    fn keyword_empty() {
        let analyzer = KeywordAnalyzer;
        assert!(analyzer.analyze("  ").is_empty());
    }
}
