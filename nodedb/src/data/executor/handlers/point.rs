//! Point operation handlers: PointGet, PointPut, PointDelete, PointUpdate.

use redb::WriteTransaction;
use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_get(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "point get");
        match self.sparse.get(tid, collection, document_id) {
            Ok(Some(data)) => {
                // Document is stored as MessagePack (or legacy JSON).
                // Pass through directly — the Control Plane's
                // decode_payload_to_json() handles format conversion.
                self.response_with_payload(task, data)
            }
            Ok(None) => self.response_error(task, ErrorCode::NotFound),
            Err(e) => {
                tracing::warn!(core = self.core_id, error = %e, "sparse get failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    pub(in crate::data::executor) fn execute_point_put(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "point put");
        let stored = super::super::doc_format::json_to_msgpack(value);
        let doc = super::super::doc_format::decode_document(value);

        // Unified write transaction: document + inverted index + stats in one commit.
        // This reduces N+2 separate transactions (each with its own fsync) to 1.
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

        // 1. Write document.
        if let Err(e) = self
            .sparse
            .put_in_txn(&txn, tid, collection, document_id, &stored)
        {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            );
        }

        // 2. Auto-index text fields for full-text search.
        if let Some(ref doc) = doc {
            if let Some(obj) = doc.as_object() {
                let text_content: String = obj
                    .values()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(" ");
                if !text_content.is_empty()
                    && let Err(e) = self.inverted.index_document_in_txn(
                        &txn,
                        collection,
                        document_id,
                        &text_content,
                    )
                {
                    warn!(core = self.core_id, %collection, %document_id, error = %e, "inverted index update failed");
                }
            }

            // 3. Update column statistics for CBO.
            if let Err(e) = self
                .stats_store
                .observe_document_in_txn(&txn, tid, collection, doc)
            {
                warn!(core = self.core_id, %collection, error = %e, "column stats update failed");
            }
        }

        // Single commit for document + index + stats.
        if let Err(e) = txn.commit() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("commit: {e}"),
                },
            );
        }

        // Invalidate aggregate cache for this collection (in-memory only, no txn needed).
        if doc.is_some() {
            let cache_prefix = format!("{tid}:{collection}\0");
            self.aggregate_cache
                .retain(|k, _| !k.starts_with(&cache_prefix));
        }

        self.response_ok(task)
    }

    pub(in crate::data::executor) fn execute_point_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "point delete");
        match self.sparse.delete(tid, collection, document_id) {
            Ok(_) => {
                // Cascade 1: Remove from full-text inverted index.
                if let Err(e) = self.inverted.remove_document(collection, document_id) {
                    warn!(core = self.core_id, %collection, %document_id, error = %e, "inverted index removal failed");
                }

                // Cascade 2: Remove secondary index entries for this document.
                // Secondary indexes use key format "{tenant}:{collection}:{field}:{value}:{doc_id}".
                // We scan and delete all entries ending with this doc_id.
                if let Err(e) =
                    self.sparse
                        .delete_indexes_for_document(tid, collection, document_id)
                {
                    warn!(core = self.core_id, %collection, %document_id, error = %e, "secondary index cascade failed");
                }

                // Cascade 3: Remove graph edges where this document is src or dst.
                let edges_removed = self.csr.remove_node_edges(document_id);
                if edges_removed > 0 {
                    // Also remove from persistent edge store.
                    if let Err(e) = self.edge_store.delete_edges_for_node(document_id) {
                        warn!(core = self.core_id, %document_id, error = %e, "edge cascade failed");
                    }
                    tracing::trace!(core = self.core_id, %document_id, edges_removed, "EDGE_CASCADE_DELETE");
                }

                // Record deletion for edge referential integrity.
                self.deleted_nodes.insert(document_id.to_string());

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

    pub(in crate::data::executor) fn execute_point_update(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
        updates: &[(String, Vec<u8>)],
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, fields = updates.len(), "point update");
        match self.sparse.get(tid, collection, document_id) {
            Ok(Some(current_bytes)) => {
                let mut doc = match super::super::doc_format::decode_document(&current_bytes) {
                    Some(v) => v,
                    None => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "failed to parse document for update".into(),
                            },
                        );
                    }
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
                match self
                    .sparse
                    .put(tid, collection, document_id, &updated_bytes)
                {
                    Ok(()) => self.response_ok(task),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Ok(None) => self.response_error(task, ErrorCode::NotFound),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Execute a PointPut within an externally-owned WriteTransaction.
    ///
    /// Does NOT open or commit a transaction — the caller is responsible for
    /// committing the shared transaction after all batched writes complete.
    /// Returns `Ok(())` on success, `Err(Response)` on failure.
    fn point_put_in_txn(
        &mut self,
        txn: &WriteTransaction,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) -> Result<(), Response> {
        let stored = super::super::doc_format::json_to_msgpack(value);

        if let Err(e) = self
            .sparse
            .put_in_txn(txn, tid, collection, document_id, &stored)
        {
            return Err(self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ));
        }

        if let Some(doc) = super::super::doc_format::decode_document(value) {
            if let Some(obj) = doc.as_object() {
                let text_content: String = obj
                    .values()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(" ");
                if !text_content.is_empty()
                    && let Err(e) = self.inverted.index_document_in_txn(
                        txn,
                        collection,
                        document_id,
                        &text_content,
                    )
                {
                    warn!(core = self.core_id, %collection, %document_id, error = %e, "inverted index update failed (batch)");
                }
            }

            if let Err(e) = self
                .stats_store
                .observe_document_in_txn(txn, tid, collection, &doc)
            {
                warn!(core = self.core_id, %collection, error = %e, "column stats update failed (batch)");
            }

            let cache_prefix = format!("{tid}:{collection}\0");
            self.aggregate_cache
                .retain(|k, _| !k.starts_with(&cache_prefix));
        }

        Ok(())
    }

    /// Batch-coalesce consecutive PointPut tasks from the front of the task queue.
    ///
    /// Opens ONE redb WriteTransaction, executes all PointPuts within it,
    /// commits once, and sends individual responses. This amortizes the
    /// fsync cost across N writes instead of paying it per-write.
    ///
    /// Returns the number of tasks processed (0 if the front of the queue
    /// is not a batchable PointPut, in which case the caller should fall
    /// back to `poll_one`).
    pub fn poll_write_batch(&mut self) -> usize {
        // Check if the front of the queue is a non-expired PointPut.
        let front_is_put = self
            .task_queue
            .front()
            .is_some_and(|t| matches!(t.plan(), PhysicalPlan::PointPut { .. }) && !t.is_expired());
        if !front_is_put {
            return 0;
        }

        // Collect consecutive non-expired PointPuts (max 64).
        let mut batch: Vec<ExecutionTask> = Vec::with_capacity(64);
        while batch.len() < 64 {
            let is_put = self.task_queue.front().is_some_and(|t| {
                matches!(t.plan(), PhysicalPlan::PointPut { .. }) && !t.is_expired()
            });
            if !is_put {
                break;
            }
            batch.push(self.task_queue.pop_front().unwrap());
        }

        // Single write: no batching benefit, let poll_one handle it
        // (poll_one also handles idempotency cache and other bookkeeping).
        if batch.len() <= 1 {
            for t in batch.into_iter().rev() {
                self.task_queue.push_front(t);
            }
            return 0;
        }

        // Open ONE transaction for the entire batch.
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(_) => {
                // Can't open txn — put tasks back, let poll_one handle individually.
                for t in batch.into_iter().rev() {
                    self.task_queue.push_front(t);
                }
                return 0;
            }
        };

        // Execute each PointPut within the shared transaction.
        // Track per-task success/failure for individual responses.
        let mut results: Vec<Result<(), Response>> = Vec::with_capacity(batch.len());
        for task in &batch {
            let PhysicalPlan::PointPut {
                collection,
                document_id,
                value,
            } = task.plan()
            else {
                unreachable!("batch only contains PointPut");
            };
            let tid = task.request.tenant_id.as_u32();
            results.push(self.point_put_in_txn(&txn, task, tid, collection, document_id, value));
        }

        // If ANY write failed hard (document put error), abort the batch.
        let any_hard_failure = results.iter().any(|r| r.is_err());
        if any_hard_failure {
            // Don't commit — transaction is dropped (implicit rollback).
            // Send error responses for failed tasks, put successful ones back.
            let count = batch.len();
            for (task, result) in batch.into_iter().zip(results) {
                let response = match result {
                    Err(err_response) => err_response,
                    Ok(()) => self.response_error(
                        &task,
                        ErrorCode::Internal {
                            detail: "batch aborted due to sibling failure".into(),
                        },
                    ),
                };
                let _ = self
                    .response_tx
                    .try_push(crate::bridge::dispatch::BridgeResponse { inner: response });
            }
            return count;
        }

        // Commit once for all writes.
        let commit_result = txn.commit();

        let count = batch.len();
        for task in &batch {
            let response = match &commit_result {
                Ok(()) => self.response_ok(task),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("batch commit: {e}"),
                    },
                ),
            };

            // Record idempotency key.
            if let Some(key) = task.request.idempotency_key {
                let succeeded = response.status == crate::bridge::envelope::Status::Ok;
                if self.idempotency_cache.len() >= 16_384
                    && let Some(oldest) = self.idempotency_order.pop_front()
                {
                    self.idempotency_cache.remove(&oldest);
                }
                self.idempotency_cache.insert(key, succeeded);
                self.idempotency_order.push_back(key);
            }

            let _ = self
                .response_tx
                .try_push(crate::bridge::dispatch::BridgeResponse { inner: response });
        }

        if commit_result.is_ok() {
            debug!(core = self.core_id, count, "write batch committed");
        }

        count
    }
}
