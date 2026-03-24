//! CAST evaluation for SqlExpr.

use crate::bridge::json_ops::{is_truthy, json_to_display_string};

use super::eval::CastType;

pub(super) fn eval_cast(val: &serde_json::Value, to_type: &CastType) -> serde_json::Value {
    match to_type {
        CastType::Int => match val {
            serde_json::Value::Number(n) => {
                let i = n.as_i64().unwrap_or(n.as_f64().unwrap_or(0.0) as i64);
                serde_json::Value::Number(i.into())
            }
            serde_json::Value::String(s) => s
                .parse::<i64>()
                .map(|n| serde_json::Value::Number(n.into()))
                .unwrap_or(serde_json::Value::Null),
            serde_json::Value::Bool(b) => serde_json::Value::Number((*b as i64).into()),
            _ => serde_json::Value::Null,
        },
        CastType::Float => match val {
            serde_json::Value::Number(n) => n
                .as_f64()
                .and_then(serde_json::Number::from_f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            serde_json::Value::String(s) => s
                .parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            _ => serde_json::Value::Null,
        },
        CastType::String => serde_json::Value::String(json_to_display_string(val)),
        CastType::Bool => serde_json::Value::Bool(is_truthy(val)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cast_string_to_int() {
        assert_eq!(eval_cast(&json!("42"), &CastType::Int), json!(42));
    }

    #[test]
    fn cast_float_to_int() {
        assert_eq!(eval_cast(&json!(3.7), &CastType::Int), json!(3));
    }

    #[test]
    fn cast_int_to_string() {
        assert_eq!(eval_cast(&json!(42), &CastType::String), json!("42"));
    }

    #[test]
    fn cast_to_bool() {
        assert_eq!(eval_cast(&json!(1), &CastType::Bool), json!(true));
        assert_eq!(eval_cast(&json!(0), &CastType::Bool), json!(false));
        assert_eq!(eval_cast(&json!(""), &CastType::Bool), json!(false));
        assert_eq!(eval_cast(&json!("x"), &CastType::Bool), json!(true));
    }
}
