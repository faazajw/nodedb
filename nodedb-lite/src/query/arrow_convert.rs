//! Arrow value extraction helper for query result conversion.

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use nodedb_types::value::Value;

/// Extract a single value from an Arrow array at the given row index.
///
/// Returns `Err` if the Arrow array type doesn't match the expected downcast.
pub(crate) fn arrow_value_at(
    col: &dyn Array,
    row: usize,
) -> Result<Value, crate::error::LiteError> {
    use datafusion::arrow::array::*;

    if col.is_null(row) {
        return Ok(Value::Null);
    }

    /// Downcast helper that returns a proper error instead of panicking.
    macro_rules! downcast {
        ($col:expr, $arr_type:ty, $type_name:expr) => {
            $col.as_any().downcast_ref::<$arr_type>().ok_or_else(|| {
                crate::error::LiteError::ArrowTypeConversion {
                    expected: $type_name.into(),
                    got: format!("{:?}", $col.data_type()),
                }
            })?
        };
    }

    match col.data_type() {
        DataType::Utf8 => Ok(Value::String(
            downcast!(col, StringArray, "StringArray")
                .value(row)
                .to_string(),
        )),
        DataType::LargeUtf8 => Ok(Value::String(
            downcast!(col, LargeStringArray, "LargeStringArray")
                .value(row)
                .to_string(),
        )),
        DataType::Int8 => Ok(Value::Integer(
            downcast!(col, Int8Array, "Int8Array").value(row) as i64,
        )),
        DataType::Int16 => Ok(Value::Integer(
            downcast!(col, Int16Array, "Int16Array").value(row) as i64,
        )),
        DataType::Int32 => Ok(Value::Integer(
            downcast!(col, Int32Array, "Int32Array").value(row) as i64,
        )),
        DataType::Int64 => Ok(Value::Integer(
            downcast!(col, Int64Array, "Int64Array").value(row),
        )),
        DataType::UInt8 => Ok(Value::Integer(
            downcast!(col, UInt8Array, "UInt8Array").value(row) as i64,
        )),
        DataType::UInt16 => Ok(Value::Integer(
            downcast!(col, UInt16Array, "UInt16Array").value(row) as i64,
        )),
        DataType::UInt32 => Ok(Value::Integer(
            downcast!(col, UInt32Array, "UInt32Array").value(row) as i64,
        )),
        DataType::UInt64 => Ok(Value::Integer(
            downcast!(col, UInt64Array, "UInt64Array").value(row) as i64,
        )),
        DataType::Float32 => Ok(Value::Float(
            downcast!(col, Float32Array, "Float32Array").value(row) as f64,
        )),
        DataType::Float64 => Ok(Value::Float(
            downcast!(col, Float64Array, "Float64Array").value(row),
        )),
        DataType::Boolean => Ok(Value::Bool(
            downcast!(col, BooleanArray, "BooleanArray").value(row),
        )),
        _ => {
            let formatter = datafusion::arrow::util::display::ArrayFormatter::try_new(
                col,
                &datafusion::arrow::util::display::FormatOptions::default(),
            );
            match formatter {
                Ok(fmt) => Ok(Value::String(fmt.value(row).to_string())),
                Err(_) => Ok(Value::Null),
            }
        }
    }
}
