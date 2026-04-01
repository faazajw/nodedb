//! `setval('sequence_name', value)` — set current value of a sequence.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DfResult, exec_err};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::control::sequence::SequenceRegistry;

/// `setval(name TEXT, value BIGINT) → BIGINT`
///
/// Sets the named sequence's counter to the given value.
/// Value must be within [MINVALUE, MAXVALUE].
pub struct SetVal {
    signature: Signature,
    registry: Arc<SequenceRegistry>,
    tenant_id: u32,
}

impl SetVal {
    pub fn new(registry: Arc<SequenceRegistry>, tenant_id: u32) -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Int64],
                Volatility::Volatile,
            ),
            registry,
            tenant_id,
        }
    }
}

impl std::fmt::Debug for SetVal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetVal")
            .field("tenant_id", &self.tenant_id)
            .finish()
    }
}

impl PartialEq for SetVal {
    fn eq(&self, other: &Self) -> bool {
        self.tenant_id == other.tenant_id
    }
}

impl Eq for SetVal {}

impl std::hash::Hash for SetVal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.tenant_id.hash(state);
    }
}

impl ScalarUDFImpl for SetVal {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "setval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DfResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("setval requires exactly 2 arguments");
        }

        let name = match &args[0] {
            ColumnarValue::Scalar(s) => s
                .to_string()
                .trim_matches('\'')
                .trim_matches('"')
                .to_lowercase(),
            _ => return exec_err!("setval first argument must be a scalar string"),
        };

        let value = match &args[1] {
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Int64(Some(v))) => *v,
            ColumnarValue::Scalar(s) => {
                return exec_err!("setval second argument must be BIGINT, got: {s}");
            }
            _ => return exec_err!("setval does not support array arguments"),
        };

        let result = self
            .registry
            .setval(self.tenant_id, &name, value)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

        Ok(ColumnarValue::Scalar(
            datafusion::common::ScalarValue::Int64(Some(result)),
        ))
    }
}
