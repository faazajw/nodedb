pub mod fuzzy;
pub mod text_analyzer;

pub use fuzzy::{fuzzy_discount, fuzzy_match, levenshtein, max_distance_for_length};
pub use text_analyzer::{
    AnalyzerRegistry, EdgeNgramAnalyzer, KeywordAnalyzer, LanguageAnalyzer, NgramAnalyzer,
    SimpleAnalyzer, StandardAnalyzer, TextAnalyzer, analyze,
};
