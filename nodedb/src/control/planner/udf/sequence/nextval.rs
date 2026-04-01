//! `nextval('sequence_name')` — advance sequence and return next value.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DfResult, exec_err};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::control::sequence::SequenceRegistry;

/// `nextval(name TEXT) → BIGINT`
///
/// Advances the named sequence and returns the new value.
/// Registered as a DataFusion scalar UDF. Holds an `Arc<SequenceRegistry>`
/// and a fixed `tenant_id` (resolved at QueryContext creation time).
pub struct NextVal {
    signature: Signature,
    registry: Arc<SequenceRegistry>,
    tenant_id: u32,
}

impl NextVal {
    pub fn new(registry: Arc<SequenceRegistry>, tenant_id: u32) -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
            registry,
            tenant_id,
        }
    }
}

impl std::fmt::Debug for NextVal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NextVal")
            .field("tenant_id", &self.tenant_id)
            .finish()
    }
}

impl PartialEq for NextVal {
    fn eq(&self, other: &Self) -> bool {
        self.tenant_id == other.tenant_id
    }
}

impl Eq for NextVal {}

impl std::hash::Hash for NextVal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.tenant_id.hash(state);
    }
}

impl ScalarUDFImpl for NextVal {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "nextval"
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
        if args.len() != 1 {
            return exec_err!("nextval requires exactly 1 argument");
        }

        match &args[0] {
            ColumnarValue::Scalar(scalar) => {
                let name = scalar
                    .to_string()
                    .trim_matches('\'')
                    .trim_matches('"')
                    .to_lowercase();

                let value = self
                    .registry
                    .nextval(self.tenant_id, &name)
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

                Ok(ColumnarValue::Scalar(
                    datafusion::common::ScalarValue::Int64(Some(value)),
                ))
            }
            ColumnarValue::Array(arr) => {
                let names = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "nextval argument must be TEXT".to_string(),
                    )
                })?;

                let mut values = Vec::with_capacity(names.len());
                for i in 0..names.len() {
                    if names.is_null(i) {
                        values.push(None);
                    } else {
                        let name = names.value(i).to_lowercase();
                        let val = self.registry.nextval(self.tenant_id, &name).map_err(|e| {
                            datafusion::error::DataFusionError::Execution(e.to_string())
                        })?;
                        values.push(Some(val));
                    }
                }

                let arr: ArrayRef = Arc::new(Int64Array::from(values));
                Ok(ColumnarValue::Array(arr))
            }
        }
    }
}
