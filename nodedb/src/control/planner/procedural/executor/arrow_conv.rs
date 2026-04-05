//! Arrow scalar to `nodedb_types::Value` conversion.
//!
//! Used by the statement executor to convert DataFusion evaluation results
//! into typed values for ASSIGN and OUT parameter handling.
//! Uses stack-friendly `nodedb_types::Value` instead of heap-heavy `serde_json::Value`.

use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;
use nodedb_types::Value;

/// Extract a single scalar value from an Arrow array at the given row index.
///
/// Returns `nodedb_types::Value` which uses stack-allocated variants for
/// integers and floats (no heap allocation). Previously used `serde_json::json!()`
/// which allocated via `Number::from()` for every scalar.
pub fn arrow_scalar_to_value(col: &Arc<dyn Array>, row: usize) -> Value {
    if col.is_null(row) {
        return Value::Null;
    }

    match col.data_type() {
        DataType::Boolean => Value::Bool(
            col.as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row),
        ),
        DataType::Int8 => {
            Value::Integer(col.as_any().downcast_ref::<Int8Array>().unwrap().value(row) as i64)
        }
        DataType::Int16 => Value::Integer(
            col.as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::Int32 => Value::Integer(
            col.as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::Int64 => Value::Integer(
            col.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        ),
        DataType::UInt8 => Value::Integer(
            col.as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::UInt16 => Value::Integer(
            col.as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::UInt32 => Value::Integer(
            col.as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::UInt64 => Value::Integer(
            col.as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::Float32 => Value::Float(
            col.as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row) as f64,
        ),
        DataType::Float64 => Value::Float(
            col.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(arr.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Value::String(arr.value(row).to_string())
        }
        _ => {
            // Fallback: format as string via ScalarValue.
            let scalar = datafusion::common::ScalarValue::try_from_array(col, row);
            match scalar {
                Ok(s) => Value::String(s.to_string()),
                Err(_) => Value::Null,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn null_value() {
        let arr: Arc<dyn Array> = Arc::new(Int32Array::from(vec![None]));
        assert_eq!(arrow_scalar_to_value(&arr, 0), Value::Null);
    }

    #[test]
    fn int32_value() {
        let arr: Arc<dyn Array> = Arc::new(Int32Array::from(vec![42]));
        assert_eq!(arrow_scalar_to_value(&arr, 0), Value::Integer(42));
    }

    #[test]
    fn float64_value() {
        let arr: Arc<dyn Array> = Arc::new(Float64Array::from(vec![1.5]));
        assert_eq!(arrow_scalar_to_value(&arr, 0), Value::Float(1.5));
    }

    #[test]
    fn string_value() {
        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["hello"]));
        assert_eq!(
            arrow_scalar_to_value(&arr, 0),
            Value::String("hello".into())
        );
    }

    #[test]
    fn boolean_value() {
        let arr: Arc<dyn Array> = Arc::new(BooleanArray::from(vec![true]));
        assert_eq!(arrow_scalar_to_value(&arr, 0), Value::Bool(true));
    }
}
