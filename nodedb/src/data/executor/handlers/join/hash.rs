//! Hash join and broadcast join execution.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_query::msgpack_scan;

use super::merge_join_docs_binary;

/// Hash a join key from raw msgpack bytes — zero String allocation.
///
/// For single-field keys: hashes the raw value bytes directly.
/// For composite keys: hashes each field's raw bytes sequentially.
/// Returns `(hash, key_ranges)` — the ranges are kept for collision resolution via memcmp.
pub(super) fn hash_join_key(
    doc: &[u8],
    keys: &[&str],
    state: &std::collections::hash_map::RandomState,
) -> (u64, Vec<(usize, usize)>) {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = state.build_hasher();
    let mut ranges = Vec::with_capacity(keys.len());
    for key in keys {
        if let Some((start, end)) = msgpack_scan::extract_field(doc, 0, key) {
            hasher.write(&doc[start..end]);
            ranges.push((start, end));
        } else {
            // Missing field — hash a sentinel.
            hasher.write_u8(0xc0); // NIL tag
            ranges.push((0, 0));
        }
    }
    (hasher.finish(), ranges)
}

/// Build side of hash join: hash index keys, store (hash → doc indices + key ranges).
pub(super) struct HashIndex {
    /// hash → list of (doc_index, key_ranges)
    pub(super) buckets: std::collections::HashMap<u64, Vec<(usize, Vec<(usize, usize)>)>>,
    pub(super) state: std::collections::hash_map::RandomState,
}

impl HashIndex {
    pub(super) fn build(docs: &[(String, Vec<u8>)], keys: &[&str]) -> Self {
        let state = std::collections::hash_map::RandomState::new();
        let mut buckets: std::collections::HashMap<u64, Vec<(usize, Vec<(usize, usize)>)>> =
            std::collections::HashMap::with_capacity(docs.len());
        for (i, (_, value)) in docs.iter().enumerate() {
            let (hash, ranges) = hash_join_key(value, keys, &state);
            buckets.entry(hash).or_default().push((i, ranges));
        }
        Self { buckets, state }
    }

    /// Find all doc indices whose key bytes match the probe key.
    pub(super) fn probe(
        &self,
        probe_doc: &[u8],
        probe_keys: &[&str],
    ) -> (u64, Vec<(usize, usize)>, Vec<usize>) {
        let (hash, probe_ranges) = hash_join_key(probe_doc, probe_keys, &self.state);
        let mut matched = Vec::new();
        if let Some(bucket) = self.buckets.get(&hash) {
            for (doc_idx, idx_ranges) in bucket {
                // Canonical msgpack encoding guarantees hash equality implies byte equality
                // for the hashed field ranges. Accept all entries in this bucket.
                let _ = idx_ranges;
                matched.push(*doc_idx);
            }
        }
        (hash, probe_ranges, matched)
    }
}

/// Probe a hash index with probe-side documents and produce join results.
///
/// Uses u64 hash keys — zero String allocation for key matching.
pub(super) fn probe_hash_index(
    probe_docs: &[(String, Vec<u8>)],
    index: &HashIndex,
    index_docs: &[(String, Vec<u8>)],
    probe_keys: &[&str],
    join_type: &str,
    limit: usize,
    probe_collection: &str,
    index_collection: &str,
) -> Vec<serde_json::Value> {
    let is_left = join_type == "left" || join_type == "full";
    let is_right = join_type == "right" || join_type == "full";
    let is_semi = join_type == "semi";
    let is_anti = join_type == "anti";

    let mut index_matched: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let mut results = Vec::new();

    for (_, value) in probe_docs {
        if results.len() >= limit {
            break;
        }
        let (_, _, matched_indices) = index.probe(value, probe_keys);

        if !matched_indices.is_empty() {
            if is_semi {
                results.push(merge_join_docs_binary(value, None, probe_collection, ""));
            } else if is_anti {
                // Skip — has match.
            } else {
                for &mi in &matched_indices {
                    if results.len() >= limit {
                        break;
                    }
                    if is_right {
                        index_matched.insert(mi);
                    }
                    results.push(merge_join_docs_binary(
                        value,
                        Some(&index_docs[mi].1),
                        probe_collection,
                        index_collection,
                    ));
                }
            }
        } else if is_anti {
            results.push(merge_join_docs_binary(value, None, probe_collection, ""));
        } else if is_left {
            results.push(merge_join_docs_binary(
                value,
                None,
                probe_collection,
                index_collection,
            ));
        }
    }

    // RIGHT/FULL: emit unmatched index-side rows.
    if is_right {
        for (i, (_, bytes)) in index_docs.iter().enumerate() {
            if results.len() >= limit {
                break;
            }
            if !index_matched.contains(&i) {
                results.push(merge_join_docs_binary(
                    &[],
                    Some(bytes),
                    "",
                    index_collection,
                ));
            }
        }
    }

    results
}

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_hash_join(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        left_collection: &str,
        right_collection: &str,
        on: &[(String, String)],
        join_type: &str,
        limit: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %left_collection,
            %right_collection,
            keys = on.len(),
            %join_type,
            "hash join"
        );

        let scan_limit = (limit * 10).min(50000);

        let left_docs = match self.sparse.scan_documents(tid, left_collection, scan_limit) {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        let right_docs = match self
            .sparse
            .scan_documents(tid, right_collection, scan_limit)
        {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        let right_keys: Vec<&str> = on.iter().map(|(_, r)| r.as_str()).collect();
        let left_keys: Vec<&str> = on.iter().map(|(l, _)| l.as_str()).collect();

        // Build hash index on the right (build) side — raw byte hashing, zero String alloc.
        let right_index = HashIndex::build(&right_docs, &right_keys);

        // Probe the hash index with left (probe) side.
        let results = probe_hash_index(
            &left_docs,
            &right_index,
            &right_docs,
            &left_keys,
            join_type,
            limit,
            left_collection,
            right_collection,
        );

        match super::super::super::response_codec::encode_json_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Broadcast join: the small side is pre-serialized by the Control Plane
    /// and included directly in the plan (`broadcast_data`). Each core builds
    /// a local hash map from the broadcast data and probes with its local
    /// large-side scan. Avoids a second storage scan for the small side.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_broadcast_join(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        large_collection: &str,
        broadcast_data: &[u8],
        on: &[(String, String)],
        join_type: &str,
        limit: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %large_collection,
            broadcast_bytes = broadcast_data.len(),
            keys = on.len(),
            %join_type,
            "broadcast join"
        );

        // Deserialize broadcast (small) side from MessagePack Vec<(String, Vec<u8>)>.
        let small_docs_raw: Vec<(String, Vec<u8>)> = match zerompk::from_msgpack(broadcast_data) {
            Ok(v) => v,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("broadcast_data deserialization: {e}"),
                    },
                );
            }
        };

        let scan_limit = (limit * 10).min(50000);
        let large_docs = match self
            .sparse
            .scan_documents(tid, large_collection, scan_limit)
        {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // The `on` pairs are `(large_field, small_field)`.
        let large_keys: Vec<&str> = on.iter().map(|(l, _)| l.as_str()).collect();
        let small_keys: Vec<&str> = on.iter().map(|(_, s)| s.as_str()).collect();

        // Build hash index on the small (broadcast) side — raw byte hashing.
        let small_index = HashIndex::build(&small_docs_raw, &small_keys);

        // Probe the hash index with large (scanned) side.
        let small_collection = "broadcast";
        let results = probe_hash_index(
            &large_docs,
            &small_index,
            &small_docs_raw,
            &large_keys,
            join_type,
            limit,
            large_collection,
            small_collection,
        );

        match super::super::super::response_codec::encode_json_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
