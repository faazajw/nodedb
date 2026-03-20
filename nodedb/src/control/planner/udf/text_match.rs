//! `text_match(field, query)` — full-text search predicate UDF.
//!
//! Used in WHERE clauses for BM25 full-text search:
//!
//! ```sql
//! SELECT * FROM docs WHERE text_match(body, 'distributed database')
//! ```
//!
//! This is a **marker UDF**: DataFusion evaluates it as a boolean scalar
//! that always returns `true` at the Control Plane. The real text search
//! happens when the plan converter detects `WHERE text_match(...)` in a
//! filter predicate and rewrites it to `PhysicalPlan::TextSearch`, which
//! executes on the Data Plane via the inverted index with BM25 scoring.
//!
//! This is the WHERE-clause counterpart to `bm25_score()` (which is used
//! in ORDER BY). `text_match` is a boolean filter ("does this document
//! match?"), while `bm25_score` produces a float for ranking.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct TextMatch {
    signature: Signature,
}

impl Default for TextMatch {
    fn default() -> Self {
        Self::new()
    }
}

impl TextMatch {
    pub fn new() -> Self {
        Self {
            // text_match(field: Utf8, query: Utf8) → Boolean
            signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for TextMatch {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "text_match"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DfResult<ColumnarValue> {
        // Marker function: returns true for all rows.
        // Real filtering happens via TextSearch on the Data Plane.
        let len = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.len(),
            ColumnarValue::Scalar(_) => 1,
        };
        let trues = BooleanArray::from(vec![true; len]);
        Ok(ColumnarValue::Array(Arc::new(trues) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;

    #[test]
    fn returns_boolean() {
        let udf = TextMatch::new();
        assert_eq!(
            udf.return_type(&[DataType::Utf8, DataType::Utf8]).unwrap(),
            DataType::Boolean
        );
    }

    #[test]
    fn invoke_returns_true() {
        use datafusion::logical_expr::ScalarFunctionArgs;

        let udf = TextMatch::new();
        let field =
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["body", "body"])) as ArrayRef);
        let query =
            ColumnarValue::Array(
                Arc::new(StringArray::from(vec!["test query", "test query"])) as ArrayRef,
            );
        let args = ScalarFunctionArgs {
            args: vec![field, query],
            number_rows: 2,
            return_type: &DataType::Boolean,
        };
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                assert_eq!(bool_arr.len(), 2);
                assert!(bool_arr.value(0));
                assert!(bool_arr.value(1));
            }
            _ => panic!("expected array"),
        }
    }
}
