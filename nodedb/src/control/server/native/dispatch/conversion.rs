//! Shared conversion helpers for native protocol dispatch.

use nodedb_types::Value;
use nodedb_types::conversion::json_to_value_display;
use nodedb_types::protocol::NativeResponse;
use sonic_rs;

/// Convert a crate-level error into a NativeResponse.
pub(crate) fn error_to_native(seq: u64, e: &crate::Error) -> NativeResponse {
    let (code, message) = match e {
        crate::Error::BadRequest { detail } => ("42601", detail.clone()),
        crate::Error::RejectedAuthz { resource, .. } => ("42501", resource.clone()),
        crate::Error::DeadlineExceeded { .. } => ("57014", "query cancelled due to timeout".into()),
        crate::Error::CollectionNotFound { collection, .. } => {
            ("42P01", format!("collection '{collection}' not found"))
        }
        other => ("XX000", format!("{other}")),
    };
    NativeResponse::error(seq, code, message)
}

/// Parse a JSON string (from the Data Plane) into proper columns and rows.
///
/// The Data Plane returns JSON in several formats:
/// - Array of objects: `[{"id":"1","name":"Alice"}, ...]` → extract keys as columns
/// - Single object: `{"id":"1","name":"Alice"}` → one row
/// - Scalar/string: just wrap as a single "result" column
pub(crate) fn parse_json_to_columns_rows(json_text: &str) -> (Vec<String>, Vec<Vec<Value>>) {
    if let Ok(val) = sonic_rs::from_str::<serde_json::Value>(json_text) {
        match val {
            serde_json::Value::Array(arr) if !arr.is_empty() => {
                if let Some(first) = arr.first().and_then(|v| v.as_object()) {
                    let columns: Vec<String> = first.keys().cloned().collect();
                    let mut rows = Vec::with_capacity(arr.len());
                    for item in &arr {
                        if let Some(obj) = item.as_object() {
                            let row: Vec<Value> = columns
                                .iter()
                                .map(|col| {
                                    obj.get(col)
                                        .map(json_to_value_display)
                                        .unwrap_or(Value::Null)
                                })
                                .collect();
                            rows.push(row);
                        }
                    }
                    return (columns, rows);
                }
                let rows: Vec<Vec<Value>> =
                    arr.iter().map(|v| vec![json_to_value_display(v)]).collect();
                return (vec!["value".into()], rows);
            }
            serde_json::Value::Object(obj) => {
                let columns: Vec<String> = obj.keys().cloned().collect();
                let row: Vec<Value> = columns
                    .iter()
                    .map(|col| {
                        obj.get(col)
                            .map(json_to_value_display)
                            .unwrap_or(Value::Null)
                    })
                    .collect();
                return (columns, vec![row]);
            }
            _ => {}
        }
    }

    (
        vec!["result".into()],
        vec![vec![Value::String(json_text.to_string())]],
    )
}
