//! pgwire-neutral dispatch outcome used by both pgwire adapter and procedural executor.

/// Result of a unified SQL dispatch call.
///
/// For procedural callers that do not consume rows, `rows` is always empty.
#[derive(Debug, Default)]
pub struct DispatchOutcome {
    /// Number of rows affected by the statement (0 for DDL / PUBLISH).
    pub rows_affected: u64,
    /// Result rows as JSON values (empty for write-only statements).
    pub rows: Vec<serde_json::Value>,
}
