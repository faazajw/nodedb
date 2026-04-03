//! N-gram and edge n-gram analyzers for substring and prefix matching.

use super::pipeline::TextAnalyzer;

/// N-gram analyzer: generates all character n-grams of sizes min..=max for each token.
///
/// Useful for substring matching and partial-word search (e.g., autocomplete).
/// Example: "database" with min=3, max=4 → ["dat", "ata", "tab", "aba", "bas", "ase", "data", "atab", "taba", "abas", "base"]
pub struct NgramAnalyzer {
    min: usize,
    max: usize,
}

impl NgramAnalyzer {
    pub fn new(min: usize, max: usize) -> Self {
        Self {
            min: min.max(1),
            max: max.max(min.max(1)),
        }
    }
}

impl TextAnalyzer for NgramAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        let lower = text.to_lowercase();
        let mut ngrams = Vec::new();
        for word in lower.split(|c: char| !c.is_alphanumeric()) {
            if word.is_empty() {
                continue;
            }
            let chars: Vec<char> = word.chars().collect();
            for n in self.min..=self.max {
                if n > chars.len() {
                    break;
                }
                for window in chars.windows(n) {
                    ngrams.push(window.iter().collect());
                }
            }
        }
        ngrams
    }

    fn name(&self) -> &str {
        "ngram"
    }
}

/// Edge n-gram analyzer: generates n-grams anchored to the start of each token.
///
/// Useful for prefix/autocomplete search.
/// Example: "database" with min=2, max=5 → ["da", "dat", "data", "datab"]
pub struct EdgeNgramAnalyzer {
    min: usize,
    max: usize,
}

impl EdgeNgramAnalyzer {
    pub fn new(min: usize, max: usize) -> Self {
        Self {
            min: min.max(1),
            max: max.max(min.max(1)),
        }
    }
}

impl TextAnalyzer for EdgeNgramAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        let lower = text.to_lowercase();
        let mut ngrams = Vec::new();
        for word in lower.split(|c: char| !c.is_alphanumeric()) {
            if word.is_empty() {
                continue;
            }
            let chars: Vec<char> = word.chars().collect();
            for n in self.min..=self.max.min(chars.len()) {
                ngrams.push(chars[..n].iter().collect());
            }
        }
        ngrams
    }

    fn name(&self) -> &str {
        "edge_ngram"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ngram_analyzer() {
        let analyzer = NgramAnalyzer::new(3, 4);
        let tokens = analyzer.analyze("hello");
        assert_eq!(tokens.len(), 5);
        assert!(tokens.contains(&"hel".to_string()));
        assert!(tokens.contains(&"ell".to_string()));
        assert!(tokens.contains(&"llo".to_string()));
        assert!(tokens.contains(&"hell".to_string()));
        assert!(tokens.contains(&"ello".to_string()));
    }

    #[test]
    fn ngram_short_word() {
        let analyzer = NgramAnalyzer::new(3, 5);
        let tokens = analyzer.analyze("ab");
        assert!(tokens.is_empty());
    }

    #[test]
    fn edge_ngram_analyzer() {
        let analyzer = EdgeNgramAnalyzer::new(2, 5);
        let tokens = analyzer.analyze("database");
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0], "da");
        assert_eq!(tokens[1], "dat");
        assert_eq!(tokens[2], "data");
        assert_eq!(tokens[3], "datab");
    }

    #[test]
    fn edge_ngram_multiple_words() {
        let analyzer = EdgeNgramAnalyzer::new(2, 3);
        let tokens = analyzer.analyze("foo bar");
        assert_eq!(tokens.len(), 4);
        assert!(tokens.contains(&"fo".to_string()));
        assert!(tokens.contains(&"foo".to_string()));
        assert!(tokens.contains(&"ba".to_string()));
        assert!(tokens.contains(&"bar".to_string()));
    }
}
