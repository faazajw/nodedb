//! `CHUNK_TEXT(text, chunk_size, overlap, strategy)` — stub UDF for DataFusion.
//!
//! The DDL router intercepts `SELECT * FROM CHUNK_TEXT(...)` before DataFusion
//! execution. This UDF exists solely for plan validation — it is never invoked
//! at runtime on the Control Plane.

use std::any::Any;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ChunkText {
    signature: Signature,
}

impl ChunkText {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // CHUNK_TEXT(text, chunk_size, overlap, strategy)
                    TypeSignature::Any(4),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for ChunkText {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ChunkText {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "chunk_text"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DfResult<ColumnarValue> {
        // Stub: real execution is intercepted by the DDL router.
        let array = StringArray::from(vec!["[]"; args.number_rows]);
        Ok(ColumnarValue::Array(std::sync::Arc::new(array)))
    }
}
