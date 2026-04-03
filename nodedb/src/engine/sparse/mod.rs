pub mod btree;
pub mod btree_scan;
pub mod doc_cache;
pub mod fts_redb;
pub mod gsi;
pub mod inverted;
pub mod sparse_vector;
pub mod stats;

// Re-export shared text analysis and fuzzy matching from nodedb-fts.
pub use nodedb_fts::analyzer as text_analyzer;
pub use nodedb_fts::fuzzy;
