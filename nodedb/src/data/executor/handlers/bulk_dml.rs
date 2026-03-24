//! Bulk DML handlers: BulkUpdate, BulkDelete, Upsert.
//!
//! These operate on document sets matching ScanFilter predicates,
//! unlike PointUpdate/PointDelete which require `WHERE id = 'x'`.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Scan documents in a collection matching the given filters.
    ///
    /// Returns document IDs of all matching documents.
    fn scan_matching_documents(
        &self,
        tid: u32,
        collection: &str,
        filters: &[ScanFilter],
    ) -> crate::Result<Vec<String>> {
        let prefix = format!("{tid}:{collection}:");
        let end = format!("{tid}:{collection}:\u{ffff}");

        let read_txn = self
            .sparse
            .db()
            .begin_read()
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("read txn: {e}"),
            })?;
        let table = read_txn
            .open_table(crate::engine::sparse::btree::DOCUMENTS)
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("open table: {e}"),
            })?;

        let mut ids = Vec::new();
        if let Ok(range) = table.range(prefix.as_str()..end.as_str()) {
            for entry in range.flatten() {
                let key = entry.0.value();
                let value_bytes = entry.1.value();
                if let Some(doc) = super::super::doc_format::decode_document(value_bytes)
                    && filters.iter().all(|f| f.matches(&doc))
                    && let Some(doc_id) = key.strip_prefix(&prefix)
                {
                    ids.push(doc_id.to_string());
                }
            }
        }
        Ok(ids)
    }

    /// Bulk update: scan documents matching filters, apply field updates.
    ///
    /// Returns affected row count as JSON payload: `{"affected": N}`.
    pub(in crate::data::executor) fn execute_bulk_update(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        filter_bytes: &[u8],
        updates: &[(String, Vec<u8>)],
    ) -> Response {
        debug!(core = self.core_id, %collection, "bulk update");

        let filters: Vec<ScanFilter> = match rmp_serde::from_slice(filter_bytes) {
            Ok(f) => f,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("deserialize filters: {e}"),
                    },
                );
            }
        };

        let matching_ids = match self.scan_matching_documents(tid, collection, &filters) {
            Ok(ids) => ids,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Apply updates to each matching document.
        let mut affected = 0u64;
        for doc_id in &matching_ids {
            match self.sparse.get(tid, collection, doc_id) {
                Ok(Some(current_bytes)) => {
                    let mut doc = match super::super::doc_format::decode_document(&current_bytes) {
                        Some(v) => v,
                        None => continue,
                    };
                    if let Some(obj) = doc.as_object_mut() {
                        for (field, value_bytes) in updates {
                            let val: serde_json::Value = match serde_json::from_slice(value_bytes) {
                                Ok(v) => v,
                                Err(_) => serde_json::Value::String(
                                    String::from_utf8_lossy(value_bytes).into_owned(),
                                ),
                            };
                            obj.insert(field.clone(), val);
                        }
                    }
                    let updated_bytes = super::super::doc_format::encode_to_msgpack(&doc);
                    if self
                        .sparse
                        .put(tid, collection, doc_id, &updated_bytes)
                        .is_ok()
                    {
                        self.doc_cache.put(tid, collection, doc_id, &updated_bytes);
                        affected += 1;
                    }
                }
                _ => continue,
            }
        }

        debug!(core = self.core_id, %collection, affected, "bulk update complete");
        let payload = serde_json::json!({ "affected": affected });
        self.response_with_payload(task, serde_json::to_vec(&payload).unwrap_or_default())
    }

    /// Bulk delete: scan documents matching filters, delete all matches.
    ///
    /// Cascades to inverted index, secondary indexes, and graph edges.
    /// Returns affected row count as JSON payload: `{"affected": N}`.
    pub(in crate::data::executor) fn execute_bulk_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        filter_bytes: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, "bulk delete");

        let filters: Vec<ScanFilter> = match rmp_serde::from_slice(filter_bytes) {
            Ok(f) => f,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("deserialize filters: {e}"),
                    },
                );
            }
        };

        let matching_ids = match self.scan_matching_documents(tid, collection, &filters) {
            Ok(ids) => ids,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Delete each matching document with full cascade.
        let mut affected = 0u64;
        for doc_id in &matching_ids {
            if self.sparse.delete(tid, collection, doc_id).unwrap_or(false) {
                // Cascade: inverted index.
                if let Err(e) = self.inverted.remove_document(collection, doc_id) {
                    warn!(core = self.core_id, %collection, %doc_id, error = %e, "bulk delete: inverted index removal failed");
                }
                // Cascade: secondary indexes.
                if let Err(e) = self
                    .sparse
                    .delete_indexes_for_document(tid, collection, doc_id)
                {
                    warn!(core = self.core_id, %collection, %doc_id, error = %e, "bulk delete: secondary index cascade failed");
                }
                // Cascade: graph edges.
                let edges_removed = self.csr.remove_node_edges(doc_id);
                if edges_removed > 0
                    && let Err(e) = self.edge_store.delete_edges_for_node(doc_id)
                {
                    warn!(core = self.core_id, %doc_id, error = %e, "bulk delete: edge cascade failed");
                }
                self.deleted_nodes.insert(doc_id.to_string());
                self.doc_cache.invalidate(tid, collection, doc_id);
                affected += 1;
            }
        }

        debug!(core = self.core_id, %collection, affected, "bulk delete complete");
        let payload = serde_json::json!({ "affected": affected });
        self.response_with_payload(task, serde_json::to_vec(&payload).unwrap_or_default())
    }

    /// Upsert: insert if absent, merge fields if present.
    ///
    /// If a document with `document_id` exists, merges `value` fields into the
    /// existing document (preserving fields not in `value`). If it doesn't exist,
    /// inserts as a new document (identical to PointPut).
    pub(in crate::data::executor) fn execute_upsert(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "upsert");

        // Check if document already exists.
        let existing = self.sparse.get(tid, collection, document_id);

        match existing {
            Ok(Some(current_bytes)) => {
                // Merge: read existing doc, overlay new fields.
                let mut doc = match super::super::doc_format::decode_document(&current_bytes) {
                    Some(v) => v,
                    None => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "failed to parse existing document for upsert".into(),
                            },
                        );
                    }
                };

                // Parse incoming value as JSON.
                let new_fields: serde_json::Value = match serde_json::from_slice(value) {
                    Ok(v) => v,
                    Err(_) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "failed to parse upsert value as JSON".into(),
                            },
                        );
                    }
                };

                // Merge new fields into existing document.
                if let (Some(existing_obj), Some(new_obj)) =
                    (doc.as_object_mut(), new_fields.as_object())
                {
                    for (k, v) in new_obj {
                        existing_obj.insert(k.clone(), v.clone());
                    }
                }

                let merged_bytes = super::super::doc_format::encode_to_msgpack(&doc);
                match self.sparse.put(tid, collection, document_id, &merged_bytes) {
                    Ok(()) => {
                        self.doc_cache
                            .put(tid, collection, document_id, &merged_bytes);
                        self.response_ok(task)
                    }
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Ok(None) => {
                // Insert: document doesn't exist, create new (same as PointPut).
                // Use unified transaction for document + inverted index + stats.
                let txn = match self.sparse.begin_write() {
                    Ok(t) => t,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        );
                    }
                };

                if let Err(e) = self.apply_point_put(&txn, tid, collection, document_id, value) {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }

                if let Err(e) = txn.commit() {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("commit: {e}"),
                        },
                    );
                }

                self.response_ok(task)
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
