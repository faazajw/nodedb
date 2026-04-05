//! Binary filter evaluation on raw MessagePack documents.
//!
//! `ScanFilter::matches_binary(doc: &[u8])` evaluates a filter predicate
//! directly on msgpack bytes without decoding to `serde_json::Value`.

use std::cmp::Ordering;

use crate::msgpack_scan::field::extract_field;
use crate::msgpack_scan::reader::{
    array_header, read_bool, read_f64, read_i64, read_null, read_str, skip_value,
};
use crate::scan_filter::like::sql_like_match;
use crate::scan_filter::{FilterOp, ScanFilter};

impl ScanFilter {
    /// Evaluate this filter against a raw MessagePack document.
    ///
    /// Zero deserialization — extracts only the needed field bytes.
    pub fn matches_binary(&self, doc: &[u8]) -> bool {
        match self.op {
            FilterOp::MatchAll | FilterOp::Exists | FilterOp::NotExists => return true,
            FilterOp::Or => {
                return self
                    .clauses
                    .iter()
                    .any(|clause| clause.iter().all(|f| f.matches_binary(doc)));
            }
            _ => {}
        }

        let (start, end) = match extract_field(doc, 0, &self.field) {
            Some(r) => r,
            None => return self.op == FilterOp::IsNull,
        };

        match self.op {
            FilterOp::IsNull => read_null(doc, start),
            FilterOp::IsNotNull => !read_null(doc, start),
            FilterOp::Eq => eq_value_binary(&self.value, doc, start),
            FilterOp::Ne => !eq_value_binary(&self.value, doc, start),
            FilterOp::Gt => cmp_value_binary(&self.value, doc, start) == Ordering::Less,
            FilterOp::Gte => {
                let cmp = cmp_value_binary(&self.value, doc, start);
                cmp == Ordering::Less || cmp == Ordering::Equal
            }
            FilterOp::Lt => cmp_value_binary(&self.value, doc, start) == Ordering::Greater,
            FilterOp::Lte => {
                let cmp = cmp_value_binary(&self.value, doc, start);
                cmp == Ordering::Greater || cmp == Ordering::Equal
            }
            FilterOp::Contains => {
                if let (Some(s), Some(pattern)) = (read_str(doc, start), self.value.as_str()) {
                    s.contains(pattern)
                } else {
                    false
                }
            }
            FilterOp::Like => {
                if let (Some(s), Some(pattern)) = (read_str(doc, start), self.value.as_str()) {
                    sql_like_match(s, pattern, false)
                } else {
                    false
                }
            }
            FilterOp::NotLike => {
                if let (Some(s), Some(pattern)) = (read_str(doc, start), self.value.as_str()) {
                    !sql_like_match(s, pattern, false)
                } else {
                    false
                }
            }
            FilterOp::Ilike => {
                if let (Some(s), Some(pattern)) = (read_str(doc, start), self.value.as_str()) {
                    sql_like_match(s, pattern, true)
                } else {
                    false
                }
            }
            FilterOp::NotIlike => {
                if let (Some(s), Some(pattern)) = (read_str(doc, start), self.value.as_str()) {
                    !sql_like_match(s, pattern, true)
                } else {
                    false
                }
            }
            FilterOp::In => {
                if let Some(mut iter) = self.value.as_array_iter() {
                    iter.any(|v| eq_value_binary(v, doc, start))
                } else {
                    false
                }
            }
            FilterOp::NotIn => {
                if let Some(mut iter) = self.value.as_array_iter() {
                    !iter.any(|v| eq_value_binary(v, doc, start))
                } else {
                    true
                }
            }
            FilterOp::ArrayContains => array_any(doc, start, end, |elem_start| {
                eq_value_binary(&self.value, doc, elem_start)
            }),
            FilterOp::ArrayContainsAll => {
                if let Some(mut needles) = self.value.as_array_iter() {
                    needles.all(|needle| {
                        array_any(doc, start, end, |elem_start| {
                            eq_value_binary(needle, doc, elem_start)
                        })
                    })
                } else {
                    false
                }
            }
            FilterOp::ArrayOverlap => {
                if let Some(mut needles) = self.value.as_array_iter() {
                    needles.any(|needle| {
                        array_any(doc, start, end, |elem_start| {
                            eq_value_binary(needle, doc, elem_start)
                        })
                    })
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Compare `nodedb_types::Value` for equality against a msgpack value at offset.
/// Mirrors `Value::eq_json` with type coercion.
fn eq_value_binary(filter_val: &nodedb_types::Value, buf: &[u8], offset: usize) -> bool {
    use nodedb_types::Value;

    if read_null(buf, offset) {
        return matches!(filter_val, Value::Null);
    }

    match filter_val {
        Value::Null => read_null(buf, offset),
        Value::Bool(a) => read_bool(buf, offset).is_some_and(|b| *a == b),
        Value::Integer(a) => {
            if let Some(b) = read_i64(buf, offset) {
                *a == b
            } else if let Some(b) = read_f64(buf, offset) {
                *a as f64 == b
            } else if let Some(s) = read_str(buf, offset) {
                // Coercion: integer filter vs string field
                s.parse::<i64>().is_ok_and(|n| *a == n)
                    || s.parse::<f64>().is_ok_and(|n| *a as f64 == n)
            } else {
                false
            }
        }
        Value::Float(a) => {
            if let Some(b) = read_f64(buf, offset) {
                *a == b
            } else if let Some(s) = read_str(buf, offset) {
                s.parse::<f64>().is_ok_and(|n| *a == n)
            } else {
                false
            }
        }
        Value::String(a) => {
            if let Some(b) = read_str(buf, offset) {
                a == b
            } else if let Some(n) = read_i64(buf, offset) {
                // Coercion: string filter vs number field
                a.parse::<i64>().is_ok_and(|ai| ai == n)
            } else if let Some(n) = read_f64(buf, offset) {
                a.parse::<f64>().is_ok_and(|af| af == n)
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Compare `nodedb_types::Value` against a msgpack value at offset for ordering.
/// Mirrors `Value::cmp_json` with numeric coercion.
fn cmp_value_binary(filter_val: &nodedb_types::Value, buf: &[u8], offset: usize) -> Ordering {
    use nodedb_types::Value;

    let self_f64 = match filter_val {
        Value::Integer(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    };

    let other_f64 = if let Some(n) = read_f64(buf, offset) {
        Some(n)
    } else if let Some(s) = read_str(buf, offset) {
        s.parse::<f64>().ok()
    } else {
        None
    };

    if let (Some(a), Some(b)) = (self_f64, other_f64) {
        return a.partial_cmp(&b).unwrap_or(Ordering::Equal);
    }

    // String comparison fallback.
    let a_str = match filter_val {
        Value::String(s) => s.as_str(),
        _ => return Ordering::Equal,
    };
    let b_str = match read_str(buf, offset) {
        Some(s) => s,
        None => return Ordering::Equal,
    };
    a_str.cmp(b_str)
}

/// Iterate over a msgpack array at `(start, end)` and return true if any
/// element satisfies the predicate.
fn array_any(buf: &[u8], start: usize, _end: usize, mut pred: impl FnMut(usize) -> bool) -> bool {
    let Some((count, mut pos)) = array_header(buf, start) else {
        return false;
    };
    for _ in 0..count {
        if pred(pos) {
            return true;
        }
        let Some(next) = skip_value(buf, pos) else {
            return false;
        };
        pos = next;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    fn filter(field: &str, op: &str, value: nodedb_types::Value) -> ScanFilter {
        ScanFilter {
            field: field.into(),
            op: op.into(),
            value,
            clauses: vec![],
        }
    }

    #[test]
    fn eq_integer() {
        let doc = encode(&json!({"age": 25}));
        assert!(filter("age", "eq", nodedb_types::Value::Integer(25)).matches_binary(&doc));
        assert!(!filter("age", "eq", nodedb_types::Value::Integer(30)).matches_binary(&doc));
    }

    #[test]
    fn eq_string() {
        let doc = encode(&json!({"name": "alice"}));
        assert!(
            filter("name", "eq", nodedb_types::Value::String("alice".into())).matches_binary(&doc)
        );
        assert!(
            !filter("name", "eq", nodedb_types::Value::String("bob".into())).matches_binary(&doc)
        );
    }

    #[test]
    fn eq_coercion_int_vs_string() {
        let doc = encode(&json!({"age": 25}));
        assert!(filter("age", "eq", nodedb_types::Value::String("25".into())).matches_binary(&doc));
    }

    #[test]
    fn eq_coercion_string_vs_int() {
        let doc = encode(&json!({"score": "90"}));
        assert!(filter("score", "eq", nodedb_types::Value::Integer(90)).matches_binary(&doc));
    }

    #[test]
    fn ne() {
        let doc = encode(&json!({"x": 1}));
        assert!(filter("x", "ne", nodedb_types::Value::Integer(2)).matches_binary(&doc));
        assert!(!filter("x", "ne", nodedb_types::Value::Integer(1)).matches_binary(&doc));
    }

    #[test]
    fn gt_lt() {
        let doc = encode(&json!({"v": 10}));
        assert!(filter("v", "gt", nodedb_types::Value::Integer(5)).matches_binary(&doc));
        assert!(!filter("v", "gt", nodedb_types::Value::Integer(15)).matches_binary(&doc));
        assert!(filter("v", "lt", nodedb_types::Value::Integer(15)).matches_binary(&doc));
        assert!(!filter("v", "lt", nodedb_types::Value::Integer(5)).matches_binary(&doc));
    }

    #[test]
    fn gte_lte() {
        let doc = encode(&json!({"v": 10}));
        assert!(filter("v", "gte", nodedb_types::Value::Integer(10)).matches_binary(&doc));
        assert!(filter("v", "gte", nodedb_types::Value::Integer(5)).matches_binary(&doc));
        assert!(!filter("v", "gte", nodedb_types::Value::Integer(15)).matches_binary(&doc));
        assert!(filter("v", "lte", nodedb_types::Value::Integer(10)).matches_binary(&doc));
        assert!(filter("v", "lte", nodedb_types::Value::Integer(15)).matches_binary(&doc));
        assert!(!filter("v", "lte", nodedb_types::Value::Integer(5)).matches_binary(&doc));
    }

    #[test]
    fn is_null_not_null() {
        let doc = encode(&json!({"a": null, "b": 1}));
        assert!(filter("a", "is_null", nodedb_types::Value::Null).matches_binary(&doc));
        assert!(!filter("b", "is_null", nodedb_types::Value::Null).matches_binary(&doc));
        assert!(!filter("a", "is_not_null", nodedb_types::Value::Null).matches_binary(&doc));
        assert!(filter("b", "is_not_null", nodedb_types::Value::Null).matches_binary(&doc));
    }

    #[test]
    fn missing_field_is_null() {
        let doc = encode(&json!({"x": 1}));
        assert!(filter("missing", "is_null", nodedb_types::Value::Null).matches_binary(&doc));
        assert!(!filter("missing", "eq", nodedb_types::Value::Integer(1)).matches_binary(&doc));
    }

    #[test]
    fn contains_str() {
        let doc = encode(&json!({"msg": "hello world"}));
        assert!(
            filter(
                "msg",
                "contains",
                nodedb_types::Value::String("world".into())
            )
            .matches_binary(&doc)
        );
        assert!(
            !filter("msg", "contains", nodedb_types::Value::String("xyz".into()))
                .matches_binary(&doc)
        );
    }

    #[test]
    fn like_ilike() {
        let doc = encode(&json!({"name": "Alice"}));
        assert!(
            filter("name", "like", nodedb_types::Value::String("Ali%".into())).matches_binary(&doc)
        );
        assert!(
            !filter("name", "like", nodedb_types::Value::String("ali%".into()))
                .matches_binary(&doc)
        );
        assert!(
            filter("name", "ilike", nodedb_types::Value::String("ali%".into()))
                .matches_binary(&doc)
        );
        assert!(
            filter(
                "name",
                "not_like",
                nodedb_types::Value::String("Bob%".into())
            )
            .matches_binary(&doc)
        );
        assert!(
            filter(
                "name",
                "not_ilike",
                nodedb_types::Value::String("bob%".into())
            )
            .matches_binary(&doc)
        );
    }

    #[test]
    fn in_not_in() {
        let doc = encode(&json!({"status": "active"}));
        let vals = nodedb_types::Value::Array(vec![
            nodedb_types::Value::String("active".into()),
            nodedb_types::Value::String("pending".into()),
        ]);
        assert!(
            ScanFilter {
                field: "status".into(),
                op: "in".into(),
                value: vals.clone(),
                clauses: vec![]
            }
            .matches_binary(&doc)
        );

        let doc2 = encode(&json!({"status": "deleted"}));
        assert!(
            ScanFilter {
                field: "status".into(),
                op: "not_in".into(),
                value: vals,
                clauses: vec![]
            }
            .matches_binary(&doc2)
        );
    }

    #[test]
    fn array_contains() {
        let doc = encode(&json!({"tags": ["rust", "db", "fast"]}));
        assert!(
            filter(
                "tags",
                "array_contains",
                nodedb_types::Value::String("rust".into())
            )
            .matches_binary(&doc)
        );
        assert!(
            !filter(
                "tags",
                "array_contains",
                nodedb_types::Value::String("slow".into())
            )
            .matches_binary(&doc)
        );
    }

    #[test]
    fn array_contains_all() {
        let doc = encode(&json!({"tags": ["a", "b", "c"]}));
        let needles = nodedb_types::Value::Array(vec![
            nodedb_types::Value::String("a".into()),
            nodedb_types::Value::String("c".into()),
        ]);
        assert!(
            ScanFilter {
                field: "tags".into(),
                op: "array_contains_all".into(),
                value: needles,
                clauses: vec![]
            }
            .matches_binary(&doc)
        );
    }

    #[test]
    fn array_overlap() {
        let doc = encode(&json!({"tags": ["x", "y"]}));
        let needles = nodedb_types::Value::Array(vec![
            nodedb_types::Value::String("y".into()),
            nodedb_types::Value::String("z".into()),
        ]);
        assert!(
            ScanFilter {
                field: "tags".into(),
                op: "array_overlap".into(),
                value: needles,
                clauses: vec![]
            }
            .matches_binary(&doc)
        );
    }

    #[test]
    fn or_clauses() {
        let doc = encode(&json!({"x": 5}));
        let f = ScanFilter {
            field: String::new(),
            op: "or".into(),
            value: nodedb_types::Value::Null,
            clauses: vec![
                vec![filter("x", "eq", nodedb_types::Value::Integer(10))],
                vec![filter("x", "eq", nodedb_types::Value::Integer(5))],
            ],
        };
        assert!(f.matches_binary(&doc));
    }

    #[test]
    fn match_all() {
        let doc = encode(&json!({"any": "thing"}));
        assert!(filter("", "match_all", nodedb_types::Value::Null).matches_binary(&doc));
    }

    #[test]
    fn float_comparison() {
        let doc = encode(&json!({"temp": 36.6}));
        assert!(filter("temp", "gt", nodedb_types::Value::Float(30.0)).matches_binary(&doc));
        assert!(filter("temp", "lt", nodedb_types::Value::Float(40.0)).matches_binary(&doc));
    }

    #[test]
    fn bool_eq() {
        let doc = encode(&json!({"active": true}));
        assert!(filter("active", "eq", nodedb_types::Value::Bool(true)).matches_binary(&doc));
        assert!(!filter("active", "eq", nodedb_types::Value::Bool(false)).matches_binary(&doc));
    }

    #[test]
    fn gt_coercion_string_field() {
        let doc = encode(&json!({"score": "90"}));
        assert!(filter("score", "gt", nodedb_types::Value::Integer(80)).matches_binary(&doc));
    }
}
