//! Core text analysis trait and the default English analysis pipeline.
//!
//! Pipeline stages (applied at both index time and query time):
//! 1. Unicode NFD normalization + strip combining marks
//! 2. Lowercase
//! 3. Split on non-alphanumeric boundaries (preserving hyphens within words)
//! 4. Filter empty tokens and single characters
//! 5. Remove stop words (language-specific)
//! 6. Snowball stemming

use rust_stemmers::{Algorithm, Stemmer};
use unicode_normalization::UnicodeNormalization;

use super::language::stop_words::is_stop_word_en;

/// Text analyzer trait: transforms raw text into searchable tokens.
///
/// Implementations must produce the same tokens for equivalent text at
/// both index time and query time (deterministic).
pub trait TextAnalyzer: Send + Sync {
    /// Analyze text into tokens.
    fn analyze(&self, text: &str) -> Vec<String>;

    /// Analyzer name (for serialization and config).
    fn name(&self) -> &str;
}

/// Analyze text into searchable tokens using the standard English analyzer.
///
/// Applies the full pipeline: normalize → lowercase → split → filter →
/// stop words → stem. Used for both indexing and querying.
pub fn analyze(text: &str) -> Vec<String> {
    let stemmer = Stemmer::create(Algorithm::English);
    tokenize_with_stemmer(text, &stemmer, is_stop_word_en)
}

/// Shared tokenization pipeline used by both the standard `analyze()` function
/// and `LanguageAnalyzer`. Normalizes Unicode (NFD + strip combining marks +
/// lowercase), splits on word boundaries, removes stop words, and stems.
pub(crate) fn tokenize_with_stemmer(
    text: &str,
    stemmer: &Stemmer,
    stop_check: fn(&str) -> bool,
) -> Vec<String> {
    // Stage 1-2: NFD normalize, strip combining marks, lowercase.
    let normalized: String = text
        .nfd()
        .filter(|c| !c.is_ascii() || !unicode_normalization::char::is_combining_mark(*c))
        .flat_map(char::to_lowercase)
        .collect();

    let mut tokens = Vec::new();

    // Stage 3: Split on non-alphanumeric boundaries.
    // Keep hyphens and underscores within words (e.g., "e-mail" stays together).
    for word in normalized.split(|c: char| !c.is_alphanumeric() && c != '-' && c != '_') {
        let trimmed = word.trim_matches(|c: char| c == '-' || c == '_');
        if trimmed.is_empty() {
            continue;
        }

        // Stage 4: Filter single characters.
        if trimmed.len() <= 1 {
            continue;
        }

        // Stage 5: Stop word removal.
        if stop_check(trimmed) {
            continue;
        }

        // Stage 6: Snowball stemming.
        let stemmed = stemmer.stem(trimmed);
        if !stemmed.is_empty() {
            tokens.push(stemmed.into_owned());
        }
    }

    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_analysis() {
        let tokens = analyze("The quick Brown FOX jumped over the lazy dog");
        assert!(tokens.contains(&"quick".to_string()));
        assert!(tokens.contains(&"brown".to_string()));
        assert!(tokens.contains(&"fox".to_string()));
        assert!(tokens.contains(&"jump".to_string())); // stemmed
        assert!(tokens.contains(&"lazi".to_string())); // stemmed
        assert!(tokens.contains(&"dog".to_string()));
        assert!(!tokens.contains(&"the".to_string())); // stop word
    }

    #[test]
    fn stop_words_removed() {
        let tokens = analyze("this is a test of the system");
        assert_eq!(tokens, vec!["test", "system"]);
    }

    #[test]
    fn stemming_works() {
        let tokens = analyze("running databases distributed systems");
        assert!(tokens.contains(&"run".to_string()));
        assert!(tokens.contains(&"databas".to_string()));
        assert!(tokens.contains(&"distribut".to_string()));
        assert!(tokens.contains(&"system".to_string()));
    }

    #[test]
    fn unicode_normalization() {
        let tokens = analyze("cafe\u{0301}"); // "café" with combining acute
        assert_eq!(tokens, vec!["cafe"]);
    }

    #[test]
    fn hyphenated_words_preserved() {
        let tokens = analyze("e-mail real-time");
        assert!(tokens.contains(&"e-mail".to_string()) || tokens.contains(&"email".to_string()));
        assert!(
            tokens.contains(&"real-tim".to_string()) || tokens.contains(&"real-time".to_string())
        );
    }

    #[test]
    fn empty_and_single_char_filtered() {
        let tokens = analyze("I a x  ");
        assert!(tokens.is_empty());
    }
}
