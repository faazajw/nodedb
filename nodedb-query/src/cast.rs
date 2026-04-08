//! CAST evaluation for SqlExpr.

use crate::value_ops::{is_truthy, value_to_display_string};
use nodedb_types::Value;

pub fn eval_cast(val: &Value, to_type: &crate::expr::CastType) -> Value {
    use crate::expr::CastType;
    match to_type {
        CastType::Int => match val {
            Value::Integer(_) => val.clone(),
            Value::Float(f) => Value::Integer(*f as i64),
            Value::String(s) => s.parse::<i64>().map(Value::Integer).unwrap_or(Value::Null),
            Value::Bool(b) => Value::Integer(*b as i64),
            Value::Decimal(d) => {
                use rust_decimal::prelude::ToPrimitive;
                d.to_i64().map(Value::Integer).unwrap_or(Value::Null)
            }
            _ => Value::Null,
        },
        CastType::Float => match val {
            Value::Float(_) => val.clone(),
            Value::Integer(i) => Value::Float(*i as f64),
            Value::String(s) => s
                .parse::<f64>()
                .map(Value::Float)
                .ok()
                .unwrap_or(Value::Null),
            Value::Decimal(d) => {
                use rust_decimal::prelude::ToPrimitive;
                d.to_f64().map(Value::Float).unwrap_or(Value::Null)
            }
            _ => Value::Null,
        },
        CastType::String => Value::String(value_to_display_string(val)),
        CastType::Bool => Value::Bool(is_truthy(val)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::CastType;

    #[test]
    fn cast_string_to_int() {
        assert_eq!(
            eval_cast(&Value::String("42".into()), &CastType::Int),
            Value::Integer(42)
        );
    }

    #[test]
    fn cast_float_to_int() {
        assert_eq!(
            eval_cast(&Value::Float(3.7), &CastType::Int),
            Value::Integer(3)
        );
    }

    #[test]
    fn cast_int_to_string() {
        assert_eq!(
            eval_cast(&Value::Integer(42), &CastType::String),
            Value::String("42".into())
        );
    }

    #[test]
    fn cast_to_bool() {
        assert_eq!(
            eval_cast(&Value::Integer(1), &CastType::Bool),
            Value::Bool(true)
        );
        assert_eq!(
            eval_cast(&Value::Integer(0), &CastType::Bool),
            Value::Bool(false)
        );
        assert_eq!(
            eval_cast(&Value::String(String::new()), &CastType::Bool),
            Value::Bool(false)
        );
        assert_eq!(
            eval_cast(&Value::String("x".into()), &CastType::Bool),
            Value::Bool(true)
        );
    }
}
