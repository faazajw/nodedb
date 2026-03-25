//! Bulk update and delete by predicate (ScanFilter-based, not SQL string).

use std::collections::HashMap;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;

use super::super::convert::value_to_loro;
use super::super::{LockExt, NodeDbLite};
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Bulk update documents matching a predicate.
    ///
    /// Scans all documents in the collection, evaluates each `ScanFilter`
    /// against the document's JSON representation, and applies `updates`
    /// to matching documents via CRDT upsert.
    ///
    /// Returns the number of documents updated.
    pub fn bulk_update(
        &self,
        collection: &str,
        filters: &[nodedb_query::ScanFilter],
        updates: &HashMap<String, Value>,
    ) -> NodeDbResult<u64> {
        let crdt = self.crdt.lock_or_recover();
        let ids = crdt.list_ids(collection);

        // Identify matching documents.
        let matching_ids: Vec<String> = ids
            .iter()
            .filter(|id| {
                crdt.read(collection, id)
                    .map(|loro_val| {
                        let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                        let json = serde_json::to_value(&doc.fields).unwrap_or_default();
                        filters.is_empty() || filters.iter().all(|f| f.matches(&json))
                    })
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        drop(crdt);

        // Apply updates to each matching document.
        let mut crdt = self.crdt.lock_or_recover();
        let mut count = 0u64;
        for id in &matching_ids {
            let fields: Vec<(&str, loro::LoroValue)> = updates
                .iter()
                .map(|(k, v)| (k.as_str(), value_to_loro(v)))
                .collect();
            crdt.upsert(collection, id, &fields)
                .map_err(NodeDbError::storage)?;
            count += 1;
        }
        drop(crdt);

        // Update text index for modified documents.
        for id in &matching_ids {
            let crdt = self.crdt.lock_or_recover();
            if let Some(loro_val) = crdt.read(collection, id) {
                let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                drop(crdt);
                self.index_document_text(collection, id, &doc.fields);
            }
        }

        Ok(count)
    }

    /// Bulk delete documents matching a predicate.
    ///
    /// Returns the number of documents deleted.
    pub fn bulk_delete(
        &self,
        collection: &str,
        filters: &[nodedb_query::ScanFilter],
    ) -> NodeDbResult<u64> {
        let crdt = self.crdt.lock_or_recover();
        let ids = crdt.list_ids(collection);

        let matching_ids: Vec<String> = ids
            .iter()
            .filter(|id| {
                crdt.read(collection, id)
                    .map(|loro_val| {
                        let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                        let json = serde_json::to_value(&doc.fields).unwrap_or_default();
                        filters.is_empty() || filters.iter().all(|f| f.matches(&json))
                    })
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        drop(crdt);

        let mut crdt = self.crdt.lock_or_recover();
        let mut count = 0u64;
        for id in &matching_ids {
            crdt.delete(collection, id).map_err(NodeDbError::storage)?;
            count += 1;
        }
        drop(crdt);

        // Remove from text index.
        for id in &matching_ids {
            self.remove_document_text(collection, id);
        }

        Ok(count)
    }
}
