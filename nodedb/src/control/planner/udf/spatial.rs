//! Spatial UDFs for DataFusion type checking and planning.
//!
//! Most spatial predicates (ST_DWithin, ST_Contains, etc.) are stub UDFs:
//! they exist so DataFusion can parse them in WHERE clauses, but actual
//! execution is rewritten to PhysicalPlan::SpatialScan by the PlanConverter.

use std::any::Any;

use datafusion::arrow::array::{BooleanArray, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

macro_rules! spatial_stub_bool {
    ($name:ident, $fn_name:expr, $arity:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $name {
            signature: Signature,
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    signature: Signature::one_of(
                        vec![TypeSignature::Any($arity)],
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl ScalarUDFImpl for $name {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                $fn_name
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
                Ok(DataType::Boolean)
            }
            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
                // Stub: rewritten to SpatialScan by PlanConverter.
                let arr = BooleanArray::from(vec![true; args.number_rows]);
                Ok(ColumnarValue::Array(std::sync::Arc::new(arr)))
            }
        }
    };
}

// ST_DWithin(geom, geom, distance) → bool
spatial_stub_bool!(StDwithin, "st_dwithin", 3);
// ST_Contains(geom, geom) → bool
spatial_stub_bool!(StContains, "st_contains", 2);
// ST_Intersects(geom, geom) → bool
spatial_stub_bool!(StIntersects, "st_intersects", 2);
// ST_Within(geom, geom) → bool
spatial_stub_bool!(StWithin, "st_within", 2);

// ── ST_Distance stub (returns Float64) ──

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct StDistance {
    signature: Signature,
}

impl Default for StDistance {
    fn default() -> Self {
        Self::new()
    }
}

impl StDistance {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StDistance {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_distance"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arr = Float64Array::from(vec![0.0f64; args.number_rows]);
        Ok(ColumnarValue::Array(std::sync::Arc::new(arr)))
    }
}

// ── geo_distance(lng1, lat1, lng2, lat2) — executes directly ──

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GeoDistance {
    signature: Signature,
}

impl Default for GeoDistance {
    fn default() -> Self {
        Self::new()
    }
}

impl GeoDistance {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                ])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for GeoDistance {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "geo_distance"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let get_f64 = |idx: usize| -> f64 {
            match &args.args[idx] {
                ColumnarValue::Scalar(datafusion::common::ScalarValue::Float64(Some(v))) => *v,
                _ => 0.0,
            }
        };
        let dist = nodedb_types::geometry::haversine_distance(
            get_f64(0),
            get_f64(1),
            get_f64(2),
            get_f64(3),
        );
        Ok(ColumnarValue::Scalar(
            datafusion::common::ScalarValue::Float64(Some(dist)),
        ))
    }
}
