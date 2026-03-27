//! Columnar memtable: in-memory row buffer with typed column vectors.
//!
//! Each column is stored as a typed vector (Vec<i64>, Vec<f64>, etc.) rather
//! than Vec<Value> to avoid enum overhead and enable SIMD-friendly memory layout.
//! The memtable accumulates INSERTs and flushes to a segment when the row count
//! reaches the configured threshold.
//!
//! NOT thread-safe — lives on a single Data Plane core (!Send by design in Origin,
//! Mutex-wrapped in Lite).

use nodedb_types::columnar::{ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

use crate::error::ColumnarError;

/// Default flush threshold: 64K rows per memtable.
///
/// Corresponds to `QueryTuning::columnar_flush_threshold`.
pub const DEFAULT_FLUSH_THRESHOLD: usize = 65_536;

/// A single column's data in the memtable.
///
/// Each variant stores a contiguous Vec of the appropriate primitive type
/// plus a validity bitmap (true = present, false = null). This avoids
/// Option<T> overhead and enables direct handoff to codec pipelines.
#[derive(Debug, Clone)]
pub enum ColumnData {
    Int64 {
        values: Vec<i64>,
        valid: Vec<bool>,
    },
    Float64 {
        values: Vec<f64>,
        valid: Vec<bool>,
    },
    Bool {
        values: Vec<bool>,
        valid: Vec<bool>,
    },
    Timestamp {
        values: Vec<i64>,
        valid: Vec<bool>,
    },
    Decimal {
        /// Stored as 16-byte serialized representations.
        values: Vec<[u8; 16]>,
        valid: Vec<bool>,
    },
    Uuid {
        /// Stored as 16-byte binary representations.
        values: Vec<[u8; 16]>,
        valid: Vec<bool>,
    },
    String {
        /// Concatenated string bytes.
        data: Vec<u8>,
        /// Byte offsets: offset[i] is the start of string i, offset[len] is end sentinel.
        offsets: Vec<u32>,
        valid: Vec<bool>,
    },
    Bytes {
        data: Vec<u8>,
        offsets: Vec<u32>,
        valid: Vec<bool>,
    },
    Geometry {
        /// Stored as JSON-serialized geometry bytes.
        data: Vec<u8>,
        offsets: Vec<u32>,
        valid: Vec<bool>,
    },
    Vector {
        /// Packed f32 values: dim floats per row.
        data: Vec<f32>,
        dim: u32,
        valid: Vec<bool>,
    },
}

impl ColumnData {
    /// Create an empty column for the given type.
    fn new(col_type: &ColumnType) -> Self {
        match col_type {
            ColumnType::Int64 => Self::Int64 {
                values: Vec::new(),
                valid: Vec::new(),
            },
            ColumnType::Float64 => Self::Float64 {
                values: Vec::new(),
                valid: Vec::new(),
            },
            ColumnType::Bool => Self::Bool {
                values: Vec::new(),
                valid: Vec::new(),
            },
            ColumnType::Timestamp => Self::Timestamp {
                values: Vec::new(),
                valid: Vec::new(),
            },
            ColumnType::Decimal => Self::Decimal {
                values: Vec::new(),
                valid: Vec::new(),
            },
            ColumnType::Uuid => Self::Uuid {
                values: Vec::new(),
                valid: Vec::new(),
            },
            ColumnType::String => Self::String {
                data: Vec::new(),
                offsets: vec![0],
                valid: Vec::new(),
            },
            ColumnType::Bytes => Self::Bytes {
                data: Vec::new(),
                offsets: vec![0],
                valid: Vec::new(),
            },
            ColumnType::Geometry => Self::Geometry {
                data: Vec::new(),
                offsets: vec![0],
                valid: Vec::new(),
            },
            ColumnType::Vector(dim) => Self::Vector {
                data: Vec::new(),
                dim: *dim,
                valid: Vec::new(),
            },
        }
    }

    /// Number of rows in this column.
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Int64 { valid, .. }
            | Self::Float64 { valid, .. }
            | Self::Bool { valid, .. }
            | Self::Timestamp { valid, .. }
            | Self::Decimal { valid, .. }
            | Self::Uuid { valid, .. }
            | Self::String { valid, .. }
            | Self::Bytes { valid, .. }
            | Self::Geometry { valid, .. }
            | Self::Vector { valid, .. } => valid.len(),
        }
    }

    /// Append a value. Returns error if type doesn't match.
    fn push(&mut self, value: &Value, col_name: &str) -> Result<(), ColumnarError> {
        match (self, value) {
            // Null for any column type.
            (Self::Int64 { values, valid }, Value::Null) => {
                values.push(0);
                valid.push(false);
            }
            (Self::Float64 { values, valid }, Value::Null) => {
                values.push(0.0);
                valid.push(false);
            }
            (Self::Bool { values, valid }, Value::Null) => {
                values.push(false);
                valid.push(false);
            }
            (Self::Timestamp { values, valid }, Value::Null) => {
                values.push(0);
                valid.push(false);
            }
            (Self::Decimal { values, valid }, Value::Null) => {
                values.push([0u8; 16]);
                valid.push(false);
            }
            (Self::Uuid { values, valid }, Value::Null) => {
                values.push([0u8; 16]);
                valid.push(false);
            }
            (
                Self::String {
                    data: _,
                    offsets,
                    valid,
                },
                Value::Null,
            ) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                valid.push(false);
            }
            (
                Self::Bytes {
                    data: _,
                    offsets,
                    valid,
                },
                Value::Null,
            ) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                valid.push(false);
            }
            (
                Self::Geometry {
                    data: _,
                    offsets,
                    valid,
                },
                Value::Null,
            ) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                valid.push(false);
            }
            (Self::Vector { data, dim, valid }, Value::Null) => {
                data.extend(std::iter::repeat_n(0.0f32, *dim as usize));
                valid.push(false);
            }

            // Typed values.
            (Self::Int64 { values, valid }, Value::Integer(v)) => {
                values.push(*v);
                valid.push(true);
            }
            (Self::Float64 { values, valid }, Value::Float(v)) => {
                values.push(*v);
                valid.push(true);
            }
            (Self::Float64 { values, valid }, Value::Integer(v)) => {
                values.push(*v as f64);
                valid.push(true);
            }
            (Self::Bool { values, valid }, Value::Bool(v)) => {
                values.push(*v);
                valid.push(true);
            }
            (Self::Timestamp { values, valid }, Value::DateTime(dt)) => {
                values.push(dt.micros);
                valid.push(true);
            }
            (Self::Timestamp { values, valid }, Value::Integer(micros)) => {
                values.push(*micros);
                valid.push(true);
            }
            (Self::Decimal { values, valid }, Value::Decimal(d)) => {
                values.push(d.serialize());
                valid.push(true);
            }
            (Self::Uuid { values, valid }, Value::Uuid(s)) => {
                let bytes = uuid::Uuid::parse_str(s)
                    .map(|u| *u.as_bytes())
                    .unwrap_or([0u8; 16]);
                values.push(bytes);
                valid.push(true);
            }
            (
                Self::String {
                    data,
                    offsets,
                    valid,
                },
                Value::String(s),
            ) => {
                data.extend_from_slice(s.as_bytes());
                offsets.push(data.len() as u32);
                valid.push(true);
            }
            (
                Self::Bytes {
                    data,
                    offsets,
                    valid,
                },
                Value::Bytes(b),
            ) => {
                data.extend_from_slice(b);
                offsets.push(data.len() as u32);
                valid.push(true);
            }
            (
                Self::Geometry {
                    data,
                    offsets,
                    valid,
                },
                Value::Geometry(g),
            ) => {
                if let Ok(json) = serde_json::to_vec(g) {
                    data.extend_from_slice(&json);
                }
                offsets.push(data.len() as u32);
                valid.push(true);
            }
            (
                Self::Geometry {
                    data,
                    offsets,
                    valid,
                },
                Value::String(s),
            ) => {
                data.extend_from_slice(s.as_bytes());
                offsets.push(data.len() as u32);
                valid.push(true);
            }
            (Self::Vector { data, dim, valid }, Value::Array(arr)) => {
                let d = *dim as usize;
                for (i, v) in arr.iter().take(d).enumerate() {
                    let f = match v {
                        Value::Float(f) => *f as f32,
                        Value::Integer(n) => *n as f32,
                        _ => 0.0,
                    };
                    if i < d {
                        data.push(f);
                    }
                }
                // Pad with zeros if array is shorter than dim.
                for _ in arr.len()..d {
                    data.push(0.0);
                }
                valid.push(true);
            }

            (other, val) => {
                let type_name = match other {
                    Self::Int64 { .. } => "Int64",
                    Self::Float64 { .. } => "Float64",
                    Self::Bool { .. } => "Bool",
                    Self::Timestamp { .. } => "Timestamp",
                    Self::Decimal { .. } => "Decimal",
                    Self::Uuid { .. } => "Uuid",
                    Self::String { .. } => "String",
                    Self::Bytes { .. } => "Bytes",
                    Self::Geometry { .. } => "Geometry",
                    Self::Vector { .. } => "Vector",
                };
                let _ = val; // Consumed by match.
                return Err(ColumnarError::TypeMismatch {
                    column: col_name.to_string(),
                    expected: type_name.to_string(),
                });
            }
        }
        Ok(())
    }
}

/// In-memory columnar buffer that accumulates INSERTs.
///
/// Each column is stored as a typed vector. The memtable flushes to a
/// compressed segment when the row count reaches the threshold.
pub struct ColumnarMemtable {
    schema: ColumnarSchema,
    columns: Vec<ColumnData>,
    row_count: usize,
    flush_threshold: usize,
}

impl ColumnarMemtable {
    /// Create a new empty memtable for the given schema.
    pub fn new(schema: &ColumnarSchema) -> Self {
        Self::with_threshold(schema, DEFAULT_FLUSH_THRESHOLD)
    }

    /// Create with a custom flush threshold.
    pub fn with_threshold(schema: &ColumnarSchema, flush_threshold: usize) -> Self {
        let columns = schema
            .columns
            .iter()
            .map(|col| ColumnData::new(&col.column_type))
            .collect();
        Self {
            schema: schema.clone(),
            columns,
            row_count: 0,
            flush_threshold,
        }
    }

    /// Append a row of values. Validates types and nullability.
    pub fn append_row(&mut self, values: &[Value]) -> Result<(), ColumnarError> {
        if values.len() != self.schema.columns.len() {
            return Err(ColumnarError::SchemaMismatch {
                expected: self.schema.columns.len(),
                got: values.len(),
            });
        }

        for (i, (col_def, value)) in self.schema.columns.iter().zip(values.iter()).enumerate() {
            if matches!(value, Value::Null) && !col_def.nullable {
                return Err(ColumnarError::NullViolation(col_def.name.clone()));
            }
            self.columns[i].push(value, &col_def.name)?;
        }

        self.row_count += 1;
        debug_assert!(
            self.columns.iter().all(|c| c.len() == self.row_count),
            "column lengths must stay aligned with row_count"
        );
        Ok(())
    }

    /// Number of rows currently buffered.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Whether the memtable has reached its flush threshold.
    pub fn should_flush(&self) -> bool {
        self.row_count >= self.flush_threshold
    }

    /// Whether the memtable is empty.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Access the schema.
    pub fn schema(&self) -> &ColumnarSchema {
        &self.schema
    }

    /// Access the raw column data (for the segment writer).
    pub fn columns(&self) -> &[ColumnData] {
        &self.columns
    }

    /// Drain the memtable: return all column data and reset to empty.
    pub fn drain(&mut self) -> (ColumnarSchema, Vec<ColumnData>, usize) {
        let columns = std::mem::replace(
            &mut self.columns,
            self.schema
                .columns
                .iter()
                .map(|col| ColumnData::new(&col.column_type))
                .collect(),
        );
        let row_count = self.row_count;
        self.row_count = 0;
        (self.schema.clone(), columns, row_count)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};

    use super::*;

    fn test_schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("score", ColumnType::Float64),
        ])
        .expect("valid schema")
    }

    #[test]
    fn append_and_count() {
        let schema = test_schema();
        let mut mt = ColumnarMemtable::new(&schema);

        mt.append_row(&[
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Float(0.75),
        ])
        .expect("append");

        mt.append_row(&[Value::Integer(2), Value::String("Bob".into()), Value::Null])
            .expect("append");

        assert_eq!(mt.row_count(), 2);
        assert!(!mt.is_empty());
    }

    #[test]
    fn null_violation_rejected() {
        let schema = test_schema();
        let mut mt = ColumnarMemtable::new(&schema);

        let err = mt
            .append_row(&[
                Value::Null, // id is NOT NULL
                Value::String("x".into()),
                Value::Null,
            ])
            .unwrap_err();
        assert!(matches!(err, ColumnarError::NullViolation(ref s) if s == "id"));
    }

    #[test]
    fn schema_mismatch_rejected() {
        let schema = test_schema();
        let mut mt = ColumnarMemtable::new(&schema);

        let err = mt.append_row(&[Value::Integer(1)]).unwrap_err();
        assert!(matches!(err, ColumnarError::SchemaMismatch { .. }));
    }

    #[test]
    fn flush_threshold() {
        let schema = test_schema();
        let mut mt = ColumnarMemtable::with_threshold(&schema, 3);

        for i in 0..2 {
            mt.append_row(&[
                Value::Integer(i),
                Value::String(format!("u{i}")),
                Value::Null,
            ])
            .expect("append");
        }
        assert!(!mt.should_flush());

        mt.append_row(&[Value::Integer(2), Value::String("u2".into()), Value::Null])
            .expect("append");
        assert!(mt.should_flush());
    }

    #[test]
    fn drain_resets() {
        let schema = test_schema();
        let mut mt = ColumnarMemtable::new(&schema);

        mt.append_row(&[
            Value::Integer(1),
            Value::String("x".into()),
            Value::Float(0.5),
        ])
        .expect("append");

        let (_schema, columns, row_count) = mt.drain();
        assert_eq!(row_count, 1);
        assert_eq!(columns.len(), 3);
        assert_eq!(mt.row_count(), 0);
        assert!(mt.is_empty());

        // Verify column data.
        match &columns[0] {
            ColumnData::Int64 { values, valid } => {
                assert_eq!(values, &[1]);
                assert_eq!(valid, &[true]);
            }
            _ => panic!("expected Int64"),
        }
        match &columns[1] {
            ColumnData::String {
                data,
                offsets,
                valid,
            } => {
                assert_eq!(std::str::from_utf8(data).unwrap(), "x");
                assert_eq!(offsets, &[0, 1]);
                assert_eq!(valid, &[true]);
            }
            _ => panic!("expected String"),
        }
    }

    #[test]
    fn all_types() {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("i", ColumnType::Int64),
            ColumnDef::required("f", ColumnType::Float64),
            ColumnDef::required("b", ColumnType::Bool),
            ColumnDef::required("ts", ColumnType::Timestamp),
            ColumnDef::required("s", ColumnType::String),
            ColumnDef::required("raw", ColumnType::Bytes),
            ColumnDef::required("vec", ColumnType::Vector(3)),
        ])
        .expect("valid");

        let mut mt = ColumnarMemtable::new(&schema);
        mt.append_row(&[
            Value::Integer(42),
            Value::Float(0.25),
            Value::Bool(true),
            Value::Integer(1_700_000_000), // timestamp as micros
            Value::String("hello".into()),
            Value::Bytes(vec![0xDE, 0xAD]),
            Value::Array(vec![
                Value::Float(1.0),
                Value::Float(2.0),
                Value::Float(3.0),
            ]),
        ])
        .expect("append all types");

        assert_eq!(mt.row_count(), 1);
    }
}
