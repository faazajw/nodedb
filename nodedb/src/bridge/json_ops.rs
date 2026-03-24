//! Shared JSON value operations: comparison, coercion, truthiness.
//!
//! Used by both `expr_eval` (computed projections) and `scan_filter`
//! (WHERE predicate evaluation on the Data Plane).

use std::cmp::Ordering;

/// Coerce a JSON value to f64.
///
/// - Numbers: `as_f64()` directly
/// - Strings: parse as f64 (`"5"` → `5.0`)
/// - Booleans: `true` → `1.0`, `false` → `0.0` (when `coerce_bool` is true)
/// - Other types: `None`
pub fn json_to_f64(v: &serde_json::Value, coerce_bool: bool) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        serde_json::Value::Bool(b) if coerce_bool => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

/// Compare two JSON values with type coercion.
///
/// Tries numeric comparison first (with bool coercion), then falls
/// back to string comparison.
pub fn compare_json(a: &serde_json::Value, b: &serde_json::Value) -> Ordering {
    // Try numeric comparison with bool coercion.
    if let (Some(na), Some(nb)) = (json_to_f64(a, true), json_to_f64(b, true)) {
        return na.partial_cmp(&nb).unwrap_or(Ordering::Equal);
    }
    // Fallback: string comparison.
    let sa = json_to_display_string(a);
    let sb = json_to_display_string(b);
    sa.cmp(&sb)
}

/// Compare two optional JSON values (for scan_filter compatibility).
pub fn compare_json_optional(
    a: Option<&serde_json::Value>,
    b: Option<&serde_json::Value>,
) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(a), Some(b)) => compare_json(a, b),
    }
}

/// Check equality with type coercion.
///
/// Handles `"5" == 5` by coercing both sides to f64 when one is a
/// number and the other is a numeric string.
pub fn coerced_eq(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    if a == b {
        return true;
    }
    if let (Some(af), Some(bf)) = (json_to_f64(a, true), json_to_f64(b, true)) {
        return (af - bf).abs() < f64::EPSILON;
    }
    false
}

/// Check if a JSON value is truthy (for boolean contexts).
///
/// - `true` → true, `false` → false
/// - `null` → false
/// - Numbers: non-zero → true
/// - Strings: non-empty → true
/// - Arrays/Objects: always true
pub fn is_truthy(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::Null => false,
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0) != 0.0,
        serde_json::Value::String(s) => !s.is_empty(),
        _ => true,
    }
}

/// Convert a JSON value to a display string.
///
/// - Strings: returned as-is (no quotes)
/// - Null: empty string
/// - Numbers/Bools: `.to_string()`
/// - Objects/Arrays: JSON serialization
pub fn json_to_display_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

/// Convert a f64 to a JSON number, preferring integer representation.
///
/// Returns `Null` for NaN/Infinity.
pub fn to_json_number(n: f64) -> serde_json::Value {
    if n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
        serde_json::Value::Number(serde_json::Number::from(n as i64))
    } else {
        serde_json::Number::from_f64(n)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn coerced_eq_mixed_types() {
        assert!(coerced_eq(&json!(5), &json!("5")));
        assert!(coerced_eq(&json!(3.15), &json!("3.15")));
        assert!(!coerced_eq(&json!(5), &json!("6")));
    }

    #[test]
    fn coerced_eq_bool_numeric() {
        assert!(coerced_eq(&json!(true), &json!(1)));
        assert!(coerced_eq(&json!(false), &json!(0)));
        assert!(!coerced_eq(&json!(true), &json!(0)));
    }

    #[test]
    fn compare_numeric_coercion() {
        assert_eq!(compare_json(&json!(5), &json!("4")), Ordering::Greater);
        assert_eq!(compare_json(&json!("10"), &json!(9)), Ordering::Greater);
    }

    #[test]
    fn truthiness() {
        assert!(is_truthy(&json!(true)));
        assert!(!is_truthy(&json!(false)));
        assert!(!is_truthy(&json!(null)));
        assert!(is_truthy(&json!(1)));
        assert!(!is_truthy(&json!(0)));
        assert!(is_truthy(&json!("hello")));
        assert!(!is_truthy(&json!("")));
    }

    #[test]
    fn to_json_number_nan() {
        assert_eq!(to_json_number(f64::NAN), serde_json::Value::Null);
    }

    #[test]
    fn to_json_number_integer() {
        assert_eq!(to_json_number(42.0), json!(42));
    }

    #[test]
    fn to_json_number_float() {
        assert_eq!(to_json_number(3.15), json!(3.15));
    }
}
