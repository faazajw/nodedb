//! WASM Aggregate UDF: DataFusion AggregateUDFImpl backed by wasmtime.
//!
//! Aggregate WASM modules export four functions:
//! - `agg_init() → state: i64`
//! - `agg_accumulate(state: i64, value: T) → state: i64`
//! - `agg_merge(a: i64, b: i64) → state: i64`
//! - `agg_finalize(state: i64) → result: T`

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};

/// A WASM-backed aggregate function registered with DataFusion.
#[derive(Debug)]
#[allow(dead_code)]
pub struct WasmAggregateUdf {
    name: String,
    input_type: DataType,
    return_type: DataType,
    module_hash: String,
    wasm_bytes: Arc<Vec<u8>>,
    signature: Signature,
    fuel: u64,
}

impl PartialEq for WasmAggregateUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.module_hash == other.module_hash
    }
}
impl Eq for WasmAggregateUdf {}

impl Hash for WasmAggregateUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.module_hash.hash(state);
    }
}

impl WasmAggregateUdf {
    pub fn new(
        name: String,
        input_type: DataType,
        return_type: DataType,
        module_hash: String,
        wasm_bytes: Arc<Vec<u8>>,
        fuel: u64,
    ) -> Self {
        let type_sig = TypeSignature::Exact(vec![input_type.clone()]);
        let signature = Signature::new(type_sig, Volatility::Volatile);
        Self {
            name,
            input_type,
            return_type,
            module_hash,
            wasm_bytes,
            signature,
            fuel,
        }
    }
}

impl AggregateUDFImpl for WasmAggregateUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(self.return_type.clone())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        // Each accumulator holds opaque i64 state from the WASM module.
        // The actual WASM invocation (init/accumulate/merge/finalize) will be
        // wired when the WasmRuntime is accessible from within the accumulator.
        Ok(Box::new(WasmAccumulator {
            state: 0i64,
            return_type: self.return_type.clone(),
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DfResult<Vec<Arc<Field>>> {
        // Single i64 state field for the opaque WASM accumulator handle.
        Ok(vec![Arc::new(
            args.return_field.as_ref().clone().with_name("wasm_state"),
        )])
    }
}

/// Per-group accumulator backed by WASM state.
#[derive(Debug)]
struct WasmAccumulator {
    /// Opaque state handle from WASM agg_init().
    state: i64,
    return_type: DataType,
}

impl Accumulator for WasmAccumulator {
    fn update_batch(&mut self, _values: &[datafusion::arrow::array::ArrayRef]) -> DfResult<()> {
        // Each value calls agg_accumulate(state, value) → new state.
        // Wired to WasmRuntime when SharedState is accessible.
        Ok(())
    }

    fn merge_batch(&mut self, _states: &[datafusion::arrow::array::ArrayRef]) -> DfResult<()> {
        // Each partial state calls agg_merge(self.state, other_state) → merged.
        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<datafusion::common::ScalarValue> {
        // Calls agg_finalize(state) → result.
        match self.return_type {
            DataType::Int32 => Ok(datafusion::common::ScalarValue::Int32(Some(
                self.state as i32,
            ))),
            DataType::Int64 => Ok(datafusion::common::ScalarValue::Int64(Some(self.state))),
            DataType::Float32 => Ok(datafusion::common::ScalarValue::Float32(Some(
                self.state as f32,
            ))),
            DataType::Float64 => Ok(datafusion::common::ScalarValue::Float64(Some(
                self.state as f64,
            ))),
            _ => Ok(datafusion::common::ScalarValue::Int64(Some(self.state))),
        }
    }

    fn state(&mut self) -> DfResult<Vec<datafusion::common::ScalarValue>> {
        Ok(vec![datafusion::common::ScalarValue::Int64(Some(
            self.state,
        ))])
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
