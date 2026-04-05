//! Join execution handlers — hash, sort-merge, broadcast, and nested-loop.

pub mod hash;
pub mod nested_loop;
pub mod sort_merge;

use nodedb_query::msgpack_scan;

/// Merge a left and optional right document into a single JSON object,
/// prefixing each key with its source collection name.
///
/// Uses binary scan to iterate map entries — avoids full rmpv decode.
/// Values are read directly into `serde_json::Value` via `read_value`
/// (scalars) or `json_from_msgpack` (complex types).
pub(super) fn merge_join_docs_binary(
    left_bytes: &[u8],
    right_bytes: Option<&[u8]>,
    left_collection: &str,
    right_collection: &str,
) -> serde_json::Value {
    let mut merged = serde_json::Map::new();
    merge_map_entries(&mut merged, left_bytes, left_collection);
    if let Some(rb) = right_bytes {
        merge_map_entries(&mut merged, rb, right_collection);
    }
    serde_json::Value::Object(merged)
}

/// Iterate msgpack map entries using binary scan and insert into merged map.
pub(super) fn merge_map_entries(
    merged: &mut serde_json::Map<String, serde_json::Value>,
    bytes: &[u8],
    prefix: &str,
) {
    let Some((count, mut pos)) = msgpack_scan::map_header(bytes, 0) else {
        return;
    };
    for _ in 0..count {
        let key = msgpack_scan::read_str(bytes, pos).map(|s| format!("{prefix}.{s}"));
        pos = match msgpack_scan::skip_value(bytes, pos) {
            Some(p) => p,
            None => return,
        };
        let value_start = pos;
        let value_end = match msgpack_scan::skip_value(bytes, pos) {
            Some(p) => p,
            None => return,
        };
        if let Some(k) = key {
            // Fast path: scalars directly.
            let val = if let Some(v) = msgpack_scan::read_value(bytes, value_start) {
                serde_json::Value::from(v)
            } else {
                // Complex types (array, map) — decode slice.
                nodedb_types::json_msgpack::json_from_msgpack(&bytes[value_start..value_end])
                    .unwrap_or(serde_json::Value::Null)
            };
            merged.insert(k, val);
        }
        pos = value_end;
    }
}

/// Compare two documents using pre-extracted key byte ranges.
/// `a_ranges`/`b_ranges` are `(start, end)` byte slices into the respective docs.
pub(super) fn compare_preextracted(
    a_doc: &[u8],
    a_ranges: &[(usize, usize)],
    b_doc: &[u8],
    b_ranges: &[(usize, usize)],
) -> std::cmp::Ordering {
    use nodedb_query::msgpack_scan::compare_field_bytes;
    for (a_range, b_range) in a_ranges.iter().zip(b_ranges.iter()) {
        let ord = compare_field_bytes(a_doc, *a_range, b_doc, *b_range);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}
