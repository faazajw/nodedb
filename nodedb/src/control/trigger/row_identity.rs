//! Shared utility for injecting row identity fields into trigger payloads.

use std::collections::HashMap;

use nodedb_types::Value;

/// Inject `id` and `document_id` fields into a trigger row's field map.
///
/// If the row already contains these fields, the existing values are preserved.
/// No-op when `row_id` is empty.
pub(crate) fn inject_row_identity(fields: &mut HashMap<String, Value>, row_id: &str) {
    if row_id.is_empty() {
        return;
    }

    let value = Value::String(row_id.to_string());
    fields
        .entry("id".to_string())
        .or_insert_with(|| value.clone());
    fields.entry("document_id".to_string()).or_insert(value);
}
