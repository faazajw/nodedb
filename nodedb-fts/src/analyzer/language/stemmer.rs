//! Language-specific analyzer: Snowball stemming for 15 languages.

use rust_stemmers::{Algorithm, Stemmer};

use crate::analyzer::pipeline::{TextAnalyzer, tokenize_with_stemmer};

use super::stop_words::is_stop_word_en;

/// Language-specific analyzer using Snowball stemming.
pub struct LanguageAnalyzer {
    algorithm: Algorithm,
    lang_name: String,
}

impl LanguageAnalyzer {
    pub fn new(language: &str) -> Option<Self> {
        let algorithm = match language.to_lowercase().as_str() {
            "english" | "en" => Algorithm::English,
            "german" | "de" => Algorithm::German,
            "french" | "fr" => Algorithm::French,
            "spanish" | "es" => Algorithm::Spanish,
            "italian" | "it" => Algorithm::Italian,
            "portuguese" | "pt" => Algorithm::Portuguese,
            "dutch" | "nl" => Algorithm::Dutch,
            "swedish" | "sv" => Algorithm::Swedish,
            "norwegian" | "no" => Algorithm::Norwegian,
            "danish" | "da" => Algorithm::Danish,
            "finnish" | "fi" => Algorithm::Finnish,
            "russian" | "ru" => Algorithm::Russian,
            "turkish" | "tr" => Algorithm::Turkish,
            "hungarian" | "hu" => Algorithm::Hungarian,
            "romanian" | "ro" => Algorithm::Romanian,
            _ => return None,
        };
        Some(Self {
            algorithm,
            lang_name: language.to_lowercase(),
        })
    }
}

impl TextAnalyzer for LanguageAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        let stemmer = Stemmer::create(self.algorithm);
        // TODO: use per-language stop words once multi-language stop word lists are added
        tokenize_with_stemmer(text, &stemmer, is_stop_word_en)
    }

    fn name(&self) -> &str {
        &self.lang_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn language_analyzer_german() {
        let analyzer = LanguageAnalyzer::new("german").unwrap();
        let tokens = analyzer.analyze("Die Datenbanken sind schnell");
        assert!(!tokens.is_empty());
        assert!(tokens.iter().all(|t| t == &t.to_lowercase()));
    }

    #[test]
    fn unknown_language_returns_none() {
        assert!(LanguageAnalyzer::new("klingon").is_none());
    }

    #[test]
    fn language_codes_work() {
        assert!(LanguageAnalyzer::new("en").is_some());
        assert!(LanguageAnalyzer::new("de").is_some());
        assert!(LanguageAnalyzer::new("fr").is_some());
    }
}
