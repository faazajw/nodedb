//! WASM Aggregate UDF: DataFusion AggregateUDFImpl backed by wasmtime.
//!
//! Aggregate WASM modules export four functions:
//! - `agg_init() → state: i64`
//! - `agg_accumulate(state: i64, value: T) → state: i64`
//! - `agg_merge(a: i64, b: i64) → state: i64`
//! - `agg_finalize(state: i64) → result: T`
//!
//! Each accumulator instance owns a wasmtime Store + Instance. The WASM
//! module is compiled once (cached by SHA-256) and shared across accumulators.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};
use wasmtime::{Instance, Store, Val};

use super::runtime::WasmRuntime;
use super::wit;

/// A WASM-backed aggregate function registered with DataFusion.
#[derive(Debug)]
pub struct WasmAggregateUdf {
    name: String,
    input_type: DataType,
    return_type: DataType,
    wasm_bytes: Arc<Vec<u8>>,
    runtime: Arc<WasmRuntime>,
    signature: Signature,
    fuel: u64,
}

impl PartialEq for WasmAggregateUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for WasmAggregateUdf {}

impl Hash for WasmAggregateUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl WasmAggregateUdf {
    pub fn new(
        name: String,
        input_type: DataType,
        return_type: DataType,
        wasm_bytes: Arc<Vec<u8>>,
        runtime: Arc<WasmRuntime>,
        fuel: u64,
    ) -> Self {
        let type_sig = TypeSignature::Exact(vec![input_type.clone()]);
        let signature = Signature::new(type_sig, Volatility::Volatile);
        Self {
            name,
            input_type,
            return_type,
            wasm_bytes,
            runtime,
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
        // Compile module (cache hit is fast).
        let module = self.runtime.get_or_compile(&self.wasm_bytes).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("WASM compile: {e}"))
        })?;

        // Create per-accumulator Store + Instance.
        let mut store = Store::new(self.runtime.engine(), ());
        store
            .set_fuel(self.fuel)
            .map_err(|e| datafusion::error::DataFusionError::Execution(format!("set fuel: {e}")))?;

        let instance = Instance::new(&mut store, &module, &[]).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("WASM instantiate: {e}"))
        })?;

        // Call agg_init() to get initial state.
        let init_fn = instance
            .get_func(&mut store, wit::AGG_INIT)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "WASM module missing export '{}'",
                    wit::AGG_INIT
                ))
            })?;
        let mut init_results = [Val::I64(0)];
        init_fn
            .call(&mut store, &[], &mut init_results)
            .map_err(|e| datafusion::error::DataFusionError::Execution(format!("agg_init: {e}")))?;
        let state = init_results[0].i64().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution("agg_init must return i64".into())
        })?;

        Ok(Box::new(WasmAccumulator {
            store,
            instance,
            state,
            return_type: self.return_type.clone(),
            input_type: self.input_type.clone(),
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DfResult<Vec<Arc<Field>>> {
        Ok(vec![Arc::new(
            args.return_field.as_ref().clone().with_name("wasm_state"),
        )])
    }
}

/// Per-group accumulator backed by a WASM instance.
///
/// Each accumulator owns its own wasmtime Store + Instance, so state is
/// isolated per group. Stores are lightweight (~1KB each).
struct WasmAccumulator {
    store: Store<()>,
    instance: Instance,
    /// Opaque state handle from WASM agg_init().
    state: i64,
    return_type: DataType,
    input_type: DataType,
}

impl std::fmt::Debug for WasmAccumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmAccumulator")
            .field("state", &self.state)
            .field("return_type", &self.return_type)
            .finish()
    }
}

impl WasmAccumulator {
    /// Call agg_accumulate(state, value) → new state.
    fn call_accumulate(&mut self, value: Val) -> DfResult<()> {
        let func = self
            .instance
            .get_func(&mut self.store, wit::AGG_ACCUMULATE)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("missing agg_accumulate".into())
            })?;
        let mut results = [Val::I64(0)];
        func.call(
            &mut self.store,
            &[Val::I64(self.state), value],
            &mut results,
        )
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("agg_accumulate: {e}"))
        })?;
        self.state = results[0].i64().unwrap_or(self.state);
        Ok(())
    }

    /// Call agg_merge(state_a, state_b) → merged state.
    fn call_merge(&mut self, other_state: i64) -> DfResult<()> {
        let func = self
            .instance
            .get_func(&mut self.store, wit::AGG_MERGE)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("missing agg_merge".into())
            })?;
        let mut results = [Val::I64(0)];
        func.call(
            &mut self.store,
            &[Val::I64(self.state), Val::I64(other_state)],
            &mut results,
        )
        .map_err(|e| datafusion::error::DataFusionError::Execution(format!("agg_merge: {e}")))?;
        self.state = results[0].i64().unwrap_or(self.state);
        Ok(())
    }

    /// Call agg_finalize(state) → result value.
    fn call_finalize(&mut self) -> DfResult<Val> {
        let func = self
            .instance
            .get_func(&mut self.store, wit::AGG_FINALIZE)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("missing agg_finalize".into())
            })?;
        // Return type determines the result Val type.
        let mut results = [match self.return_type {
            DataType::Int32 => Val::I32(0),
            DataType::Float32 => Val::F32(0),
            DataType::Float64 => Val::F64(0),
            _ => Val::I64(0),
        }];
        func.call(&mut self.store, &[Val::I64(self.state)], &mut results)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("agg_finalize: {e}"))
            })?;
        Ok(results[0])
    }
}

impl Accumulator for WasmAccumulator {
    fn update_batch(&mut self, values: &[datafusion::arrow::array::ArrayRef]) -> DfResult<()> {
        let arr = values.first().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution("no input array".into())
        })?;
        let len = arr.len();

        // Convert each row to a WASM Val and call agg_accumulate.
        match &self.input_type {
            DataType::Int32 => {
                let a = arr.as_primitive::<datafusion::arrow::datatypes::Int32Type>();
                for i in 0..len {
                    if !a.is_null(i) {
                        self.call_accumulate(Val::I32(a.value(i)))?;
                    }
                }
            }
            DataType::Int64 => {
                let a = arr.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
                for i in 0..len {
                    if !a.is_null(i) {
                        self.call_accumulate(Val::I64(a.value(i)))?;
                    }
                }
            }
            DataType::Float32 => {
                let a = arr.as_primitive::<datafusion::arrow::datatypes::Float32Type>();
                for i in 0..len {
                    if !a.is_null(i) {
                        self.call_accumulate(Val::F32(a.value(i).to_bits()))?;
                    }
                }
            }
            DataType::Float64 => {
                let a = arr.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
                for i in 0..len {
                    if !a.is_null(i) {
                        self.call_accumulate(Val::F64(a.value(i).to_bits()))?;
                    }
                }
            }
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "unsupported WASM aggregate input type: {:?}",
                    self.input_type
                )));
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[datafusion::arrow::array::ArrayRef]) -> DfResult<()> {
        let arr = states.first().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution("no state array for merge".into())
        })?;
        let state_arr = arr.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
        for i in 0..state_arr.len() {
            if !state_arr.is_null(i) {
                self.call_merge(state_arr.value(i))?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<datafusion::common::ScalarValue> {
        let result = self.call_finalize()?;
        match result {
            Val::I32(v) => Ok(datafusion::common::ScalarValue::Int32(Some(v))),
            Val::I64(v) => Ok(datafusion::common::ScalarValue::Int64(Some(v))),
            Val::F32(v) => Ok(datafusion::common::ScalarValue::Float32(Some(
                f32::from_bits(v),
            ))),
            Val::F64(v) => Ok(datafusion::common::ScalarValue::Float64(Some(
                f64::from_bits(v),
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
