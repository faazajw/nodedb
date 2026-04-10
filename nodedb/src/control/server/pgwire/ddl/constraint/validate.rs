//! DDL-time validation for CHECK constraint expressions.

use pgwire::error::PgWireResult;

use super::err;

/// Validate that a subquery CHECK expression uses a supported pattern.
///
/// Supported patterns:
/// - `expr IN (SELECT col FROM tbl [WHERE ...])`
/// - `expr NOT IN (SELECT col FROM tbl [WHERE ...])`
pub(super) fn validate_subquery_pattern(check_sql: &str) -> PgWireResult<()> {
    let upper = check_sql.to_uppercase();

    if upper.contains(" IN (SELECT ") || upper.contains(" IN(SELECT ") {
        return Ok(());
    }

    Err(err(
        "0A000",
        &format!(
            "unsupported subquery CHECK pattern. \
             Supported: `expr IN (SELECT col FROM tbl)`, \
             `expr NOT IN (SELECT col FROM tbl)`. \
             Got: {check_sql}"
        ),
    ))
}

/// Strip `NEW.` prefix for validation parsing.
pub(super) fn strip_new_prefix_for_validation(sql: &str) -> String {
    let upper = sql.to_uppercase();
    let bytes = sql.as_bytes();
    let mut result = String::with_capacity(sql.len());
    let mut i = 0;
    while i < bytes.len() {
        if i + 4 <= bytes.len() && upper[i..].starts_with("NEW.") {
            if i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_') {
                result.push(bytes[i] as char);
                i += 1;
                continue;
            }
            i += 4;
            continue;
        }
        result.push(bytes[i] as char);
        i += 1;
    }
    result
}
