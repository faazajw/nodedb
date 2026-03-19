//! DocumentScan execution handler for the Data Plane CoreLoop.
//!
//! Extracted from `aggregate.rs` to keep that file under the 500-line limit.
//! Handles `PhysicalPlan::DocumentScan` with filter, sort, distinct, offset, and limit.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};

use super::core_loop::CoreLoop;
use super::scan_filter::{ScanFilter, compare_json_values};
use super::task::ExecutionTask;

impl CoreLoop {
    /// Execute a DocumentScan: full-collection scan with optional filter,
    /// sort, distinct deduplication, offset, and limit.
    ///
    /// Fetches extra documents to account for filtering and offset, then
    /// applies predicates, sorts by `sort_keys`, deduplicates if `distinct`,
    /// and returns the paginated slice.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_document_scan(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        limit: usize,
        offset: usize,
        sort_keys: &[(String, bool)],
        filters: &[u8],
        distinct: bool,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            limit,
            offset,
            sort_fields = sort_keys.len(),
            "document scan"
        );

        // Fetch extra documents to account for filtering + offset.
        let fetch_limit = (limit + offset).saturating_mul(2).max(1000);
        match self.sparse.scan_documents(tid, collection, fetch_limit) {
            Ok(docs) => {
                // Parse filters if present.
                let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
                    Vec::new()
                } else {
                    match serde_json::from_slice(filters) {
                        Ok(f) => f,
                        Err(e) => {
                            warn!(core = self.core_id, error = %e, "failed to parse scan filters");
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!("malformed scan filters: {e}"),
                                },
                            );
                        }
                    }
                };

                // Apply filters.
                let filtered: Vec<_> = if filter_predicates.is_empty() {
                    docs
                } else {
                    docs.into_iter()
                        .filter(|(_, value)| {
                            let doc: serde_json::Value = match serde_json::from_slice(value) {
                                Ok(v) => v,
                                Err(_) => return false,
                            };
                            filter_predicates.iter().all(|f| f.matches(&doc))
                        })
                        .collect()
                };

                // Multi-field sort: compare by each key in order,
                // breaking ties with subsequent keys.
                let mut sorted = filtered;
                if !sort_keys.is_empty() {
                    sorted.sort_by(|(_, a_bytes), (_, b_bytes)| {
                        let a_doc: serde_json::Value =
                            serde_json::from_slice(a_bytes).unwrap_or(serde_json::Value::Null);
                        let b_doc: serde_json::Value =
                            serde_json::from_slice(b_bytes).unwrap_or(serde_json::Value::Null);

                        for (field, asc) in sort_keys {
                            let a_val = a_doc.get(field.as_str());
                            let b_val = b_doc.get(field.as_str());
                            let cmp = compare_json_values(a_val, b_val);
                            let ordered = if *asc { cmp } else { cmp.reverse() };
                            if ordered != std::cmp::Ordering::Equal {
                                return ordered;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }

                // Deduplicate if DISTINCT.
                let deduped = if distinct {
                    let mut seen = std::collections::HashSet::new();
                    sorted
                        .into_iter()
                        .filter(|(_, value)| seen.insert(value.clone()))
                        .collect()
                } else {
                    sorted
                };

                // Apply offset + limit.
                let result: Vec<_> = deduped
                    .into_iter()
                    .skip(offset)
                    .take(limit)
                    .map(|(doc_id, value)| {
                        let data: serde_json::Value = serde_json::from_slice(&value)
                            .unwrap_or_else(|e| {
                                warn!(error = %e, "corrupted document bytes");
                                serde_json::Value::Null
                            });
                        serde_json::json!({"id": doc_id, "data": data})
                    })
                    .collect();

                match serde_json::to_vec(&result) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
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
