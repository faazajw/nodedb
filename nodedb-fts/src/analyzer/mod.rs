pub mod language;
pub mod ngram;
pub mod pipeline;
pub mod registry;
pub mod standard;
pub mod synonym;

pub use language::LanguageAnalyzer;
pub use ngram::{EdgeNgramAnalyzer, NgramAnalyzer};
pub use pipeline::{TextAnalyzer, analyze};
pub use registry::AnalyzerRegistry;
pub use standard::{KeywordAnalyzer, SimpleAnalyzer, StandardAnalyzer};
pub use synonym::SynonymMap;
