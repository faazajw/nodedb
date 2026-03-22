//! Post-scan filter evaluation for DocumentScan.
//!
//! `ScanFilter` represents a single filter predicate deserialized from the
//! `filters` bytes in a `PhysicalPlan::DocumentScan`. `compare_json_values`
//! provides total ordering for JSON values used in sort and range comparisons.
//!
//! This module lives in the bridge layer so both the Control Plane (which
//! constructs `ScanFilter` values during query planning) and the Data Plane
//! (which evaluates them during physical execution) can share the type without
//! violating plane separation.

/// A single filter predicate for DocumentScan post-scan evaluation.
///
/// Supports simple comparison operators (eq, ne, gt, gte, lt, lte, contains,
/// is_null, is_not_null) and disjunctive groups via the `"or"` operator.
///
/// OR representation: `{"op": "or", "clauses": [[filter1, filter2], [filter3]]}`
/// means `(filter1 AND filter2) OR filter3`. Each clause is an AND-group;
/// the document matches if ANY clause group fully matches.
#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct ScanFilter {
    #[serde(default)]
    pub field: String,
    pub op: String,
    #[serde(default)]
    pub value: serde_json::Value,
    /// Disjunctive clause groups for OR predicates.
    /// Each inner Vec is an AND-group. The document matches if ANY group matches.
    #[serde(default)]
    pub clauses: Vec<Vec<ScanFilter>>,
}

impl ScanFilter {
    /// Evaluate this filter against a JSON document.
    pub fn matches(&self, doc: &serde_json::Value) -> bool {
        // OR predicate: document matches if ANY clause group fully matches.
        if self.op == "or" {
            return self
                .clauses
                .iter()
                .any(|clause| clause.iter().all(|f| f.matches(doc)));
        }

        let field_val = match doc.get(&self.field) {
            Some(v) => v,
            None => return self.op == "is_null",
        };

        match self.op.as_str() {
            "eq" => field_val == &self.value,
            "ne" | "neq" => field_val != &self.value,
            "gt" => {
                compare_json_values(Some(field_val), Some(&self.value))
                    == std::cmp::Ordering::Greater
            }
            "gte" | "ge" => {
                let cmp = compare_json_values(Some(field_val), Some(&self.value));
                cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal
            }
            "lt" => {
                compare_json_values(Some(field_val), Some(&self.value)) == std::cmp::Ordering::Less
            }
            "lte" | "le" => {
                let cmp = compare_json_values(Some(field_val), Some(&self.value));
                cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal
            }
            "contains" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    s.contains(pattern)
                } else {
                    false
                }
            }
            "like" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    sql_like_match(s, pattern, false)
                } else {
                    false
                }
            }
            "not_like" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    !sql_like_match(s, pattern, false)
                } else {
                    false
                }
            }
            "ilike" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    sql_like_match(s, pattern, true)
                } else {
                    false
                }
            }
            "not_ilike" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    !sql_like_match(s, pattern, true)
                } else {
                    false
                }
            }
            "in" => {
                if let Some(arr) = self.value.as_array() {
                    arr.iter().any(|v| field_val == v)
                } else {
                    false
                }
            }
            "not_in" => {
                if let Some(arr) = self.value.as_array() {
                    !arr.iter().any(|v| field_val == v)
                } else {
                    true
                }
            }
            "is_null" => field_val.is_null(),
            "is_not_null" => !field_val.is_null(),
            _ => false,
        }
    }
}

/// SQL LIKE pattern matching.
///
/// Supports the standard SQL wildcards:
/// - `%` matches zero or more characters
/// - `_` matches exactly one character
///
/// No escape character support yet (future: `LIKE 'a\%b' ESCAPE '\'`).
///
/// When `case_insensitive` is true, both the input and pattern are lowercased
/// before matching (ILIKE behavior).
fn sql_like_match(input: &str, pattern: &str, case_insensitive: bool) -> bool {
    let (input, pattern) = if case_insensitive {
        (input.to_lowercase(), pattern.to_lowercase())
    } else {
        (input.to_string(), pattern.to_string())
    };

    let input = input.as_bytes();
    let pattern = pattern.as_bytes();

    // DP-free two-pointer matching (same algorithm as `fnmatch` but for SQL LIKE).
    // Tracks the last `%` position for backtracking.
    let (mut i, mut j) = (0usize, 0usize);
    let (mut star_j, mut star_i) = (usize::MAX, 0usize);

    while i < input.len() {
        if j < pattern.len() && (pattern[j] == b'_' || pattern[j] == input[i]) {
            // Exact match or single-char wildcard.
            i += 1;
            j += 1;
        } else if j < pattern.len() && pattern[j] == b'%' {
            // Multi-char wildcard: remember position for backtracking.
            star_j = j;
            star_i = i;
            j += 1;
        } else if star_j != usize::MAX {
            // Backtrack: advance the input position matched by the last `%`.
            star_i += 1;
            i = star_i;
            j = star_j + 1;
        } else {
            return false;
        }
    }

    // Consume trailing `%` wildcards in the pattern.
    while j < pattern.len() && pattern[j] == b'%' {
        j += 1;
    }

    j == pattern.len()
}

/// Compare two optional JSON values for sorting.
pub fn compare_json_values(
    a: Option<&serde_json::Value>,
    b: Option<&serde_json::Value>,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(a), Some(b)) => {
            // Try numeric comparison first.
            if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
                return af.partial_cmp(&bf).unwrap_or(Ordering::Equal);
            }
            // Try integer comparison.
            if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                return ai.cmp(&bi);
            }
            // Fall back to string comparison.
            let a_str = a
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{a}"));
            let b_str = b
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{b}"));
            a_str.cmp(&b_str)
        }
    }
}

/// Compute an aggregate function over a group of JSON documents.
///
/// Supported operations: count, sum, avg, min, max.
pub fn compute_aggregate(op: &str, field: &str, docs: &[serde_json::Value]) -> serde_json::Value {
    match op {
        "count" => serde_json::json!(docs.len()),

        "sum" => {
            let total: f64 = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .sum();
            serde_json::json!(total)
        }

        "avg" => {
            let values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .collect();
            if values.is_empty() {
                serde_json::Value::Null
            } else {
                let avg = values.iter().sum::<f64>() / values.len() as f64;
                serde_json::json!(avg)
            }
        }

        "min" => {
            let min = docs
                .iter()
                .filter_map(|d| d.get(field))
                .min_by(|a, b| compare_json_values(Some(a), Some(b)));
            match min {
                Some(v) => v.clone(),
                None => serde_json::Value::Null,
            }
        }

        "max" => {
            let max = docs
                .iter()
                .filter_map(|d| d.get(field))
                .max_by(|a, b| compare_json_values(Some(a), Some(b)));
            match max {
                Some(v) => v.clone(),
                None => serde_json::Value::Null,
            }
        }

        _ => serde_json::Value::Null,
    }
}
