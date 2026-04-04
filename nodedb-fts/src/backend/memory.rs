//! In-memory FTS backend for Lite and WASM deployments.
//!
//! All data lives in HashMaps behind `RefCell` for interior mutability,
//! matching the `&self` trait signature. Rebuilt from documents on cold
//! start — acceptable for edge-scale datasets.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;

use crate::backend::FtsBackend;
use crate::posting::Posting;

/// In-memory backend error (infallible in practice, but trait requires it).
#[derive(Debug)]
pub struct MemoryError(String);

impl fmt::Display for MemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "memory backend: {}", self.0)
    }
}

/// In-memory FTS backend backed by HashMaps.
///
/// Keys are stored as `"{collection}:{term}"` for postings and
/// `"{collection}:{doc_id}"` for document lengths, matching the
/// scoping pattern used by the redb backend.
///
/// Uses `RefCell` for interior mutability so the `FtsBackend` trait
/// can use `&self` uniformly (redb has its own transactional isolation).
#[derive(Debug, Default)]
pub struct MemoryBackend {
    /// Scoped key "{collection}:{term}" → posting list.
    postings: RefCell<HashMap<String, Vec<Posting>>>,
    /// Scoped key "{collection}:{doc_id}" → token count.
    doc_lengths: RefCell<HashMap<String, u32>>,
    /// Per-collection incremental stats: collection → (doc_count, total_token_sum).
    stats: RefCell<HashMap<String, (u32, u64)>>,
    /// Generic metadata blobs (DocIdMap, fieldnorms, etc.).
    meta: RefCell<HashMap<String, Vec<u8>>>,
    /// Segment blobs: key → compressed segment bytes.
    segments: RefCell<HashMap<String, Vec<u8>>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

impl FtsBackend for MemoryBackend {
    type Error = MemoryError;

    fn read_postings(&self, collection: &str, term: &str) -> Result<Vec<Posting>, Self::Error> {
        let key = format!("{collection}:{term}");
        Ok(self
            .postings
            .borrow()
            .get(&key)
            .cloned()
            .unwrap_or_default())
    }

    fn write_postings(
        &self,
        collection: &str,
        term: &str,
        postings: &[Posting],
    ) -> Result<(), Self::Error> {
        let key = format!("{collection}:{term}");
        let mut map = self.postings.borrow_mut();
        if postings.is_empty() {
            map.remove(&key);
        } else {
            map.insert(key, postings.to_vec());
        }
        Ok(())
    }

    fn remove_postings(&self, collection: &str, term: &str) -> Result<(), Self::Error> {
        let key = format!("{collection}:{term}");
        self.postings.borrow_mut().remove(&key);
        Ok(())
    }

    fn read_doc_length(&self, collection: &str, doc_id: &str) -> Result<Option<u32>, Self::Error> {
        let key = format!("{collection}:{doc_id}");
        Ok(self.doc_lengths.borrow().get(&key).copied())
    }

    fn write_doc_length(
        &self,
        collection: &str,
        doc_id: &str,
        length: u32,
    ) -> Result<(), Self::Error> {
        let key = format!("{collection}:{doc_id}");
        self.doc_lengths.borrow_mut().insert(key, length);
        Ok(())
    }

    fn remove_doc_length(&self, collection: &str, doc_id: &str) -> Result<(), Self::Error> {
        let key = format!("{collection}:{doc_id}");
        self.doc_lengths.borrow_mut().remove(&key);
        Ok(())
    }

    fn collection_terms(&self, collection: &str) -> Result<Vec<String>, Self::Error> {
        let prefix = format!("{collection}:");
        Ok(self
            .postings
            .borrow()
            .keys()
            .filter_map(|k| k.strip_prefix(&prefix).map(String::from))
            .collect())
    }

    fn collection_stats(&self, collection: &str) -> Result<(u32, u64), Self::Error> {
        Ok(self
            .stats
            .borrow()
            .get(collection)
            .copied()
            .unwrap_or((0, 0)))
    }

    fn increment_stats(&self, collection: &str, doc_len: u32) -> Result<(), Self::Error> {
        let mut stats = self.stats.borrow_mut();
        let entry = stats.entry(collection.to_string()).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += doc_len as u64;
        Ok(())
    }

    fn decrement_stats(&self, collection: &str, doc_len: u32) -> Result<(), Self::Error> {
        let mut stats = self.stats.borrow_mut();
        let entry = stats.entry(collection.to_string()).or_insert((0, 0));
        entry.0 = entry.0.saturating_sub(1);
        entry.1 = entry.1.saturating_sub(doc_len as u64);
        Ok(())
    }

    fn read_meta(&self, key: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.meta.borrow().get(key).cloned())
    }

    fn write_meta(&self, key: &str, value: &[u8]) -> Result<(), Self::Error> {
        self.meta
            .borrow_mut()
            .insert(key.to_string(), value.to_vec());
        Ok(())
    }

    fn write_segment(&self, key: &str, data: &[u8]) -> Result<(), Self::Error> {
        self.segments
            .borrow_mut()
            .insert(key.to_string(), data.to_vec());
        Ok(())
    }

    fn read_segment(&self, key: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.segments.borrow().get(key).cloned())
    }

    fn list_segments(&self, collection: &str) -> Result<Vec<String>, Self::Error> {
        let prefix = format!("{collection}:seg:");
        Ok(self
            .segments
            .borrow()
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect())
    }

    fn remove_segment(&self, key: &str) -> Result<(), Self::Error> {
        self.segments.borrow_mut().remove(key);
        Ok(())
    }

    fn purge_collection(&self, collection: &str) -> Result<usize, Self::Error> {
        let prefix = format!("{collection}:");
        let mut postings = self.postings.borrow_mut();
        let mut doc_lengths = self.doc_lengths.borrow_mut();
        let before = postings.len() + doc_lengths.len();
        postings.retain(|k, _| !k.starts_with(&prefix));
        doc_lengths.retain(|k, _| !k.starts_with(&prefix));
        self.stats.borrow_mut().remove(collection);
        let meta_prefix = format!("{collection}:");
        self.meta
            .borrow_mut()
            .retain(|k, _| !k.starts_with(&meta_prefix));
        self.segments
            .borrow_mut()
            .retain(|k, _| !k.starts_with(&prefix));
        let after = postings.len() + doc_lengths.len();
        Ok(before - after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_postings() {
        let backend = MemoryBackend::new();
        let postings = vec![Posting {
            doc_id: "d1".into(),
            term_freq: 2,
            positions: vec![0, 5],
        }];
        backend.write_postings("col", "hello", &postings).unwrap();

        let read = backend.read_postings("col", "hello").unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].doc_id, "d1");
    }

    #[test]
    fn roundtrip_doc_lengths() {
        let backend = MemoryBackend::new();
        backend.write_doc_length("col", "d1", 42).unwrap();
        assert_eq!(backend.read_doc_length("col", "d1").unwrap(), Some(42));

        backend.remove_doc_length("col", "d1").unwrap();
        assert_eq!(backend.read_doc_length("col", "d1").unwrap(), None);
    }

    #[test]
    fn incremental_stats() {
        let backend = MemoryBackend::new();
        backend.increment_stats("col", 10).unwrap();
        backend.increment_stats("col", 20).unwrap();
        assert_eq!(backend.collection_stats("col").unwrap(), (2, 30));

        backend.decrement_stats("col", 10).unwrap();
        assert_eq!(backend.collection_stats("col").unwrap(), (1, 20));
    }

    #[test]
    fn stats_saturating_sub() {
        let backend = MemoryBackend::new();
        backend.decrement_stats("col", 100).unwrap();
        assert_eq!(backend.collection_stats("col").unwrap(), (0, 0));
    }

    #[test]
    fn purge_clears_stats_and_isolates_collections() {
        let backend = MemoryBackend::new();
        // Set up two collections.
        backend.increment_stats("col", 10).unwrap();
        backend.write_doc_length("col", "d1", 10).unwrap();
        backend
            .write_postings(
                "col",
                "hello",
                &[Posting {
                    doc_id: "d1".into(),
                    term_freq: 1,
                    positions: vec![0],
                }],
            )
            .unwrap();

        backend.increment_stats("other", 7).unwrap();
        backend.write_doc_length("other", "d1", 7).unwrap();
        backend
            .write_postings(
                "other",
                "world",
                &[Posting {
                    doc_id: "d1".into(),
                    term_freq: 1,
                    positions: vec![0],
                }],
            )
            .unwrap();

        // Purge only "col".
        backend.purge_collection("col").unwrap();
        assert_eq!(backend.collection_stats("col").unwrap(), (0, 0));
        assert!(backend.read_postings("col", "hello").unwrap().is_empty());
        assert_eq!(backend.read_doc_length("col", "d1").unwrap(), None);

        // "other" must be completely unaffected.
        assert_eq!(backend.collection_stats("other").unwrap(), (1, 7));
        assert_eq!(backend.read_postings("other", "world").unwrap().len(), 1);
        assert_eq!(backend.read_doc_length("other", "d1").unwrap(), Some(7));
    }

    #[test]
    fn collection_terms() {
        let backend = MemoryBackend::new();
        backend
            .write_postings(
                "col",
                "hello",
                &[Posting {
                    doc_id: "d1".into(),
                    term_freq: 1,
                    positions: vec![0],
                }],
            )
            .unwrap();
        backend
            .write_postings(
                "col",
                "world",
                &[Posting {
                    doc_id: "d1".into(),
                    term_freq: 1,
                    positions: vec![1],
                }],
            )
            .unwrap();

        let mut terms = backend.collection_terms("col").unwrap();
        terms.sort();
        assert_eq!(terms, vec!["hello", "world"]);
    }

    #[test]
    fn segment_roundtrip() {
        let backend = MemoryBackend::new();
        let data = b"compressed segment bytes";
        backend.write_segment("col:seg:id1", data).unwrap();
        assert_eq!(
            backend.read_segment("col:seg:id1").unwrap(),
            Some(data.to_vec())
        );
        assert_eq!(backend.read_segment("col:seg:missing").unwrap(), None);
    }

    #[test]
    fn segment_list_filters_by_collection() {
        let backend = MemoryBackend::new();
        backend.write_segment("col:seg:a", b"a").unwrap();
        backend.write_segment("col:seg:b", b"b").unwrap();
        backend.write_segment("other:seg:c", b"c").unwrap();

        let mut segs = backend.list_segments("col").unwrap();
        segs.sort();
        assert_eq!(segs, vec!["col:seg:a", "col:seg:b"]);

        let other = backend.list_segments("other").unwrap();
        assert_eq!(other, vec!["other:seg:c"]);
    }

    #[test]
    fn segment_remove() {
        let backend = MemoryBackend::new();
        backend.write_segment("col:seg:id1", b"data").unwrap();
        backend.remove_segment("col:seg:id1").unwrap();
        assert_eq!(backend.read_segment("col:seg:id1").unwrap(), None);
    }

    #[test]
    fn purge_clears_segments() {
        let backend = MemoryBackend::new();
        backend.write_segment("col:seg:a", b"a").unwrap();
        backend.write_segment("other:seg:b", b"b").unwrap();

        backend.purge_collection("col").unwrap();
        assert!(backend.list_segments("col").unwrap().is_empty());
        assert_eq!(backend.list_segments("other").unwrap().len(), 1);
    }
}
