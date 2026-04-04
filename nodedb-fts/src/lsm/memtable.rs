//! In-memory memtable for the FTS LSM engine.
//!
//! Accumulates postings in a HashMap until a spill threshold is reached.
//! Serves queries from memory. Thread-safe via interior mutability.

use std::cell::RefCell;
use std::collections::HashMap;

use crate::block::CompactPosting;
use crate::codec::smallfloat;

/// Spill threshold: flush memtable when total posting entries exceed this.
pub const DEFAULT_SPILL_POSTINGS: usize = 32 * 1024 * 1024; // 32M entries

/// Spill threshold: flush when unique terms exceed this.
pub const DEFAULT_SPILL_TERMS: usize = 100_000;

/// Configuration for memtable spill thresholds.
#[derive(Debug, Clone, Copy)]
pub struct MemtableConfig {
    pub max_postings: usize,
    pub max_terms: usize,
}

impl Default for MemtableConfig {
    fn default() -> Self {
        Self {
            max_postings: DEFAULT_SPILL_POSTINGS,
            max_terms: DEFAULT_SPILL_TERMS,
        }
    }
}

/// In-memory accumulator for FTS postings.
///
/// Stores per-term posting lists using u32 doc IDs (via DocIdMap).
/// Maintains incremental corpus stats.
pub struct Memtable {
    /// term → sorted list of compact postings.
    postings: RefCell<HashMap<String, Vec<CompactPosting>>>,
    /// Total number of posting entries across all terms.
    total_postings: RefCell<usize>,
    /// Incremental stats: (doc_count, total_token_sum).
    stats: RefCell<(u32, u64)>,
    /// Fieldnorm array: doc_id → SmallFloat-encoded length.
    fieldnorms: RefCell<Vec<u8>>,
    /// Spill configuration.
    config: MemtableConfig,
}

impl Memtable {
    pub fn new(config: MemtableConfig) -> Self {
        Self {
            postings: RefCell::new(HashMap::new()),
            total_postings: RefCell::new(0),
            stats: RefCell::new((0, 0)),
            fieldnorms: RefCell::new(Vec::new()),
            config,
        }
    }

    /// Insert a posting for a term. Doc ID must already be assigned via DocIdMap.
    pub fn insert(&self, term: &str, posting: CompactPosting) {
        let mut map = self.postings.borrow_mut();
        map.entry(term.to_string()).or_default().push(posting);
        *self.total_postings.borrow_mut() += 1;
    }

    /// Record a document's stats (call once per indexed document).
    pub fn record_doc(&self, doc_id: u32, doc_len: u32) {
        let mut stats = self.stats.borrow_mut();
        stats.0 += 1;
        stats.1 += doc_len as u64;

        let mut norms = self.fieldnorms.borrow_mut();
        let idx = doc_id as usize;
        if idx >= norms.len() {
            norms.resize(idx + 1, 0);
        }
        norms[idx] = smallfloat::encode(doc_len);
    }

    /// Remove a document's postings from all terms.
    pub fn remove_doc(&self, doc_id: u32) {
        let mut map = self.postings.borrow_mut();
        let mut removed = 0usize;
        map.retain(|_, postings| {
            let before = postings.len();
            postings.retain(|p| p.doc_id != doc_id);
            removed += before - postings.len();
            !postings.is_empty()
        });
        *self.total_postings.borrow_mut() -= removed;

        // Decrement stats using fieldnorm to recover doc length.
        let norms = self.fieldnorms.borrow();
        if let Some(&norm) = norms.get(doc_id as usize) {
            let doc_len = smallfloat::decode(norm);
            let mut stats = self.stats.borrow_mut();
            stats.0 = stats.0.saturating_sub(1);
            stats.1 = stats.1.saturating_sub(doc_len as u64);
        }
    }

    /// Check if the memtable should be flushed (spill threshold reached).
    pub fn should_flush(&self) -> bool {
        let tp = *self.total_postings.borrow();
        let terms = self.postings.borrow().len();
        tp >= self.config.max_postings || terms >= self.config.max_terms
    }

    /// Get the posting list for a term. Returns empty vec if not found.
    pub fn get_postings(&self, term: &str) -> Vec<CompactPosting> {
        self.postings
            .borrow()
            .get(term)
            .cloned()
            .unwrap_or_default()
    }

    /// Get all term names in the memtable.
    pub fn terms(&self) -> Vec<String> {
        self.postings.borrow().keys().cloned().collect()
    }

    /// Get corpus stats: (doc_count, total_token_sum).
    pub fn stats(&self) -> (u32, u64) {
        *self.stats.borrow()
    }

    /// Get the fieldnorm array (SmallFloat-encoded doc lengths).
    pub fn fieldnorms(&self) -> Vec<u8> {
        self.fieldnorms.borrow().clone()
    }

    /// Drain all postings from the memtable (for flush).
    /// Returns the term→postings map and resets the memtable to empty,
    /// including stats and fieldnorms.
    pub fn drain(&self) -> HashMap<String, Vec<CompactPosting>> {
        let mut map = self.postings.borrow_mut();
        *self.total_postings.borrow_mut() = 0;
        *self.stats.borrow_mut() = (0, 0);
        self.fieldnorms.borrow_mut().clear();
        std::mem::take(&mut *map)
    }

    /// Drain only postings matching a collection prefix.
    /// Removes scoped terms like "{collection}:*" and resets stats/fieldnorms.
    pub fn drain_collection(&self, collection: &str) {
        let prefix = format!("{collection}:");
        let mut map = self.postings.borrow_mut();
        let mut removed = 0usize;
        map.retain(|k, v| {
            if k.starts_with(&prefix) {
                removed += v.len();
                false
            } else {
                true
            }
        });
        *self.total_postings.borrow_mut() -= removed;
        // Stats and fieldnorms are collection-scoped in the backend,
        // but the memtable tracks them globally. Reset to be safe.
        *self.stats.borrow_mut() = (0, 0);
        self.fieldnorms.borrow_mut().clear();
    }

    /// Number of unique terms.
    pub fn term_count(&self) -> usize {
        self.postings.borrow().len()
    }

    /// Total posting entries.
    pub fn posting_count(&self) -> usize {
        *self.total_postings.borrow()
    }

    /// Whether the memtable is empty.
    pub fn is_empty(&self) -> bool {
        self.postings.borrow().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_posting(doc_id: u32, tf: u32) -> CompactPosting {
        CompactPosting {
            doc_id,
            term_freq: tf,
            fieldnorm: smallfloat::encode(100),
            positions: vec![0],
        }
    }

    #[test]
    fn insert_and_query() {
        let mt = Memtable::new(MemtableConfig::default());
        mt.insert("hello", make_posting(0, 1));
        mt.insert("hello", make_posting(1, 2));
        mt.insert("world", make_posting(0, 1));

        assert_eq!(mt.get_postings("hello").len(), 2);
        assert_eq!(mt.get_postings("world").len(), 1);
        assert!(mt.get_postings("missing").is_empty());
        assert_eq!(mt.term_count(), 2);
        assert_eq!(mt.posting_count(), 3);
    }

    #[test]
    fn remove_doc() {
        let mt = Memtable::new(MemtableConfig::default());
        mt.insert("hello", make_posting(0, 1));
        mt.insert("hello", make_posting(1, 2));
        mt.record_doc(0, 100);
        mt.record_doc(1, 50);

        mt.remove_doc(0);

        assert_eq!(mt.get_postings("hello").len(), 1);
        assert_eq!(mt.get_postings("hello")[0].doc_id, 1);
        assert_eq!(mt.stats().0, 1); // Only doc 1 remains.
    }

    #[test]
    fn drain_resets_everything() {
        let mt = Memtable::new(MemtableConfig::default());
        mt.insert("hello", make_posting(0, 1));
        mt.insert("world", make_posting(1, 1));
        mt.record_doc(0, 100);
        mt.record_doc(1, 50);

        let drained = mt.drain();
        assert_eq!(drained.len(), 2);
        assert!(mt.is_empty());
        assert_eq!(mt.posting_count(), 0);
        assert_eq!(mt.stats(), (0, 0));
        assert!(mt.fieldnorms().is_empty());
    }

    #[test]
    fn drain_collection_selective() {
        let mt = Memtable::new(MemtableConfig::default());
        mt.insert("col_a:hello", make_posting(0, 1));
        mt.insert("col_a:world", make_posting(1, 1));
        mt.insert("col_b:rust", make_posting(2, 1));

        mt.drain_collection("col_a");

        assert!(mt.get_postings("col_a:hello").is_empty());
        assert!(mt.get_postings("col_a:world").is_empty());
        assert_eq!(mt.get_postings("col_b:rust").len(), 1);
        assert_eq!(mt.posting_count(), 1);
    }

    #[test]
    fn spill_threshold() {
        let config = MemtableConfig {
            max_postings: 5,
            max_terms: 100,
        };
        let mt = Memtable::new(config);
        for i in 0..4 {
            mt.insert("term", make_posting(i, 1));
        }
        assert!(!mt.should_flush());
        mt.insert("term", make_posting(4, 1));
        assert!(mt.should_flush());
    }

    #[test]
    fn fieldnorms_recorded() {
        let mt = Memtable::new(MemtableConfig::default());
        mt.record_doc(0, 100);
        mt.record_doc(5, 50);

        let norms = mt.fieldnorms();
        assert_eq!(norms.len(), 6); // 0..=5
        assert_eq!(
            smallfloat::decode(norms[0]),
            smallfloat::decode(smallfloat::encode(100))
        );
        assert_eq!(
            smallfloat::decode(norms[5]),
            smallfloat::decode(smallfloat::encode(50))
        );
    }
}
