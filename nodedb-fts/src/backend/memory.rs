//! In-memory FTS backend for Lite and WASM deployments.
//!
//! All data lives in HashMaps. Rebuilt from documents on cold start —
//! acceptable for edge-scale datasets.

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
#[derive(Debug, Default)]
pub struct MemoryBackend {
    /// Scoped key "{collection}:{term}" → posting list.
    postings: HashMap<String, Vec<Posting>>,
    /// Scoped key "{collection}:{doc_id}" → token count.
    doc_lengths: HashMap<String, u32>,
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
        Ok(self.postings.get(&key).cloned().unwrap_or_default())
    }

    fn write_postings(
        &mut self,
        collection: &str,
        term: &str,
        postings: &[Posting],
    ) -> Result<(), Self::Error> {
        let key = format!("{collection}:{term}");
        if postings.is_empty() {
            self.postings.remove(&key);
        } else {
            self.postings.insert(key, postings.to_vec());
        }
        Ok(())
    }

    fn remove_postings(&mut self, collection: &str, term: &str) -> Result<(), Self::Error> {
        let key = format!("{collection}:{term}");
        self.postings.remove(&key);
        Ok(())
    }

    fn read_doc_length(&self, collection: &str, doc_id: &str) -> Result<Option<u32>, Self::Error> {
        let key = format!("{collection}:{doc_id}");
        Ok(self.doc_lengths.get(&key).copied())
    }

    fn write_doc_length(
        &mut self,
        collection: &str,
        doc_id: &str,
        length: u32,
    ) -> Result<(), Self::Error> {
        let key = format!("{collection}:{doc_id}");
        self.doc_lengths.insert(key, length);
        Ok(())
    }

    fn remove_doc_length(&mut self, collection: &str, doc_id: &str) -> Result<(), Self::Error> {
        let key = format!("{collection}:{doc_id}");
        self.doc_lengths.remove(&key);
        Ok(())
    }

    fn collection_terms(&self, collection: &str) -> Result<Vec<String>, Self::Error> {
        let prefix = format!("{collection}:");
        Ok(self
            .postings
            .keys()
            .filter_map(|k| k.strip_prefix(&prefix).map(String::from))
            .collect())
    }

    fn collection_stats(&self, collection: &str) -> Result<(u32, u64), Self::Error> {
        let prefix = format!("{collection}:");
        let mut count = 0u32;
        let mut total = 0u64;
        for (key, &len) in &self.doc_lengths {
            if key.starts_with(&prefix) {
                count += 1;
                total += len as u64;
            }
        }
        Ok((count, total))
    }

    fn purge_collection(&mut self, collection: &str) -> Result<usize, Self::Error> {
        let prefix = format!("{collection}:");
        let before = self.postings.len() + self.doc_lengths.len();
        self.postings.retain(|k, _| !k.starts_with(&prefix));
        self.doc_lengths.retain(|k, _| !k.starts_with(&prefix));
        let after = self.postings.len() + self.doc_lengths.len();
        Ok(before - after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_postings() {
        let mut backend = MemoryBackend::new();
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
        let mut backend = MemoryBackend::new();
        backend.write_doc_length("col", "d1", 42).unwrap();
        assert_eq!(backend.read_doc_length("col", "d1").unwrap(), Some(42));

        backend.remove_doc_length("col", "d1").unwrap();
        assert_eq!(backend.read_doc_length("col", "d1").unwrap(), None);
    }

    #[test]
    fn collection_stats() {
        let mut backend = MemoryBackend::new();
        backend.write_doc_length("col", "d1", 10).unwrap();
        backend.write_doc_length("col", "d2", 20).unwrap();
        backend.write_doc_length("other", "d1", 5).unwrap();

        let (count, total) = backend.collection_stats("col").unwrap();
        assert_eq!(count, 2);
        assert_eq!(total, 30);
    }

    #[test]
    fn purge_collection() {
        let mut backend = MemoryBackend::new();
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
        backend.write_doc_length("other", "d1", 5).unwrap();

        let removed = backend.purge_collection("col").unwrap();
        assert_eq!(removed, 2);
        assert_eq!(backend.collection_stats("col").unwrap(), (0, 0));
        assert_eq!(backend.collection_stats("other").unwrap(), (1, 5));
    }

    #[test]
    fn collection_terms() {
        let mut backend = MemoryBackend::new();
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
}
