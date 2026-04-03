//! `multi_vector_score(column, query_vector, mode)` — stub UDF for DataFusion.
//!
//! Enables SQL: `ORDER BY multi_vector_score(token_vectors, ARRAY[...], 'max_sim')`.
//! The PlanConverter recognizes this function name and rewrites to
//! `VectorOp::MultiVectorScoreSearch` on the Data Plane.
//!
//! Note: This is distinct from `multi_vector_search` (which does cross-field
//! RRF fusion). This function does per-row multi-vector aggregated scoring
//! (ColBERT-style MaxSim/AvgSim/SumSim).

use std::any::Any;

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MultiVectorScore {
    signature: Signature,
}

impl MultiVectorScore {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // multi_vector_score(column, query_vector, mode)
                    TypeSignature::Any(3),
                ],
                Volatility::Volatile,
            ),
        }
    }
}

impl Default for MultiVectorScore {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for MultiVectorScore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "multi_vector_score"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DfResult<ColumnarValue> {
        // Stub: real execution is rewritten by PlanConverter.
        let array = Float64Array::from(vec![0.0f64; args.number_rows]);
        Ok(ColumnarValue::Array(std::sync::Arc::new(array)))
    }
}
