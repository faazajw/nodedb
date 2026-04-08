//! Conditional scalar functions: coalesce, nullif, greatest, least.

use crate::value_ops::compare_values;
use nodedb_types::Value;

pub(super) fn try_eval(name: &str, args: &[Value]) -> Option<Value> {
    let v = match name {
        "coalesce" => {
            for arg in args {
                if !arg.is_null() {
                    return Some(arg.clone());
                }
            }
            Value::Null
        }
        "nullif" => {
            if args.len() >= 2 && args[0] == args[1] {
                Value::Null
            } else {
                args.first().cloned().unwrap_or(Value::Null)
            }
        }
        "greatest" => args
            .iter()
            .filter(|v| !v.is_null())
            .max_by(|a, b| compare_values(a, b))
            .cloned()
            .unwrap_or(Value::Null),
        "least" => args
            .iter()
            .filter(|v| !v.is_null())
            .min_by(|a, b| compare_values(a, b))
            .cloned()
            .unwrap_or(Value::Null),
        _ => return None,
    };
    Some(v)
}
