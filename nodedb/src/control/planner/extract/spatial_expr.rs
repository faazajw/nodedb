//! Spatial predicate extraction from DataFusion `Expr` trees.
//!
//! Detects `ST_DWithin`, `ST_Contains`, `ST_Intersects`, `ST_Within` UDF calls
//! in WHERE clauses and converts them to `SpatialOp::Scan` physical plans.
//!
//! Unlike `spatial_filter.rs` (which operates on `serde_json::Value`), this
//! module works directly on DataFusion `Expr` — the form available in the
//! plan converter's `Filter` handler.

use sonic_rs;

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;

use crate::bridge::physical_plan::{SpatialOp, SpatialPredicate};
use crate::control::planner::physical::PhysicalTask;
use crate::control::planner::search::extract_table_name;
use crate::types::{TenantId, VShardId};

/// Try to extract a spatial predicate from a filter expression.
///
/// Walks through AND conjunctions looking for `ST_*` scalar function calls.
/// Returns a fully-constructed `PhysicalTask` with `SpatialOp::Scan` if found.
///
/// Recognized patterns:
/// - `WHERE ST_DWithin(geom_col, ST_Point(lng, lat), distance)`
/// - `WHERE ST_Contains(geometry_literal, geom_col)`
/// - `WHERE ST_Intersects(geom_col, geometry_literal)`
/// - `WHERE ST_Within(geom_col, geometry_literal)`
///
/// Geometry arguments can be string literals containing GeoJSON, or `ST_Point`
/// function calls that are converted to GeoJSON Point on extraction.
pub(crate) fn try_extract_spatial_scan(
    predicate: &Expr,
    input: &LogicalPlan,
    tenant_id: TenantId,
) -> Option<PhysicalTask> {
    match predicate {
        Expr::ScalarFunction(func) => try_from_scalar_function(func, input, tenant_id),
        Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::And => {
            try_extract_spatial_scan(&binary.left, input, tenant_id)
                .or_else(|| try_extract_spatial_scan(&binary.right, input, tenant_id))
        }
        _ => None,
    }
}

/// Attempt to convert a single ScalarFunction call into a SpatialOp::Scan.
fn try_from_scalar_function(
    func: &datafusion::logical_expr::expr::ScalarFunction,
    input: &LogicalPlan,
    tenant_id: TenantId,
) -> Option<PhysicalTask> {
    let name = func.name().to_lowercase();
    let (predicate, needs_distance) = match name.as_str() {
        "st_dwithin" => (SpatialPredicate::DWithin, true),
        "st_contains" => (SpatialPredicate::Contains, false),
        "st_intersects" => (SpatialPredicate::Intersects, false),
        "st_within" => (SpatialPredicate::Within, false),
        _ => return None,
    };

    let collection = extract_table_name(input)?;
    let vshard = VShardId::from_collection(&collection);

    if needs_distance {
        // ST_DWithin(geom_col_or_geom, geom_or_col, distance)
        if func.args.len() < 3 {
            return None;
        }
        let (field, geojson) = extract_field_and_geometry(&func.args[0], &func.args[1])?;
        let distance = extract_f64_literal(&func.args[2])?;

        Some(PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: crate::bridge::envelope::PhysicalPlan::Spatial(SpatialOp::Scan {
                collection,
                field,
                predicate,
                query_geometry: geojson,
                distance_meters: distance,
                attribute_filters: Vec::new(),
                limit: 1000,
                projection: Vec::new(),
                rls_filters: Vec::new(),
            }),
        })
    } else {
        // ST_Contains/Intersects/Within(arg0, arg1)
        if func.args.len() < 2 {
            return None;
        }
        let (field, geojson) = if predicate == SpatialPredicate::Contains {
            // ST_Contains(container, contained) — first arg is geometry query.
            extract_field_and_geometry_contains(&func.args[0], &func.args[1])?
        } else {
            extract_field_and_geometry(&func.args[0], &func.args[1])?
        };

        Some(PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: crate::bridge::envelope::PhysicalPlan::Spatial(SpatialOp::Scan {
                collection,
                field,
                predicate,
                query_geometry: geojson,
                distance_meters: 0.0,
                attribute_filters: Vec::new(),
                limit: 1000,
                projection: Vec::new(),
                rls_filters: Vec::new(),
            }),
        })
    }
}

/// Determine which argument is a column reference and which is a geometry literal.
///
/// Returns `(field_name, geojson_bytes)`. The field is the column reference,
/// the geometry is materialized as GeoJSON bytes from a string literal or
/// `ST_Point(lng, lat)` call.
fn extract_field_and_geometry(a: &Expr, b: &Expr) -> Option<(String, Vec<u8>)> {
    // a = column, b = geometry
    if let (Some(col), Some(geojson)) = (extract_column_name(a), expr_to_geojson(b)) {
        return Some((col, geojson));
    }
    // b = column, a = geometry (reversed args)
    if let (Some(col), Some(geojson)) = (extract_column_name(b), expr_to_geojson(a)) {
        return Some((col, geojson));
    }
    None
}

/// For `ST_Contains(container, contained)`: the container (first arg) is the
/// query geometry, the contained (second arg) is the document field.
fn extract_field_and_geometry_contains(
    container: &Expr,
    contained: &Expr,
) -> Option<(String, Vec<u8>)> {
    // Standard: container=geometry, contained=column
    if let (Some(geojson), Some(col)) = (expr_to_geojson(container), extract_column_name(contained))
    {
        return Some((col, geojson));
    }
    // Reversed: container=column, contained=geometry (non-standard but handle gracefully)
    if let (Some(col), Some(geojson)) = (extract_column_name(container), expr_to_geojson(contained))
    {
        return Some((col, geojson));
    }
    None
}

/// Extract a column name from an `Expr::Column`.
fn extract_column_name(expr: &Expr) -> Option<String> {
    if let Expr::Column(col) = expr {
        Some(col.name.clone())
    } else {
        None
    }
}

/// Convert an Expr to GeoJSON bytes.
///
/// Handles:
/// - String literals containing GeoJSON: `'{"type":"Point","coordinates":[-73.98,40.75]}'`
/// - `ST_Point(lng, lat)` or `st_point(lng, lat)` function calls → GeoJSON Point
fn expr_to_geojson(expr: &Expr) -> Option<Vec<u8>> {
    match expr {
        Expr::Literal(lit, _) => {
            let s = lit.to_string();
            let trimmed = s.trim_matches('\'').trim_matches('"');
            // Validate it looks like GeoJSON (has "type" key).
            let parsed: serde_json::Value = sonic_rs::from_str(trimmed).ok()?;
            if parsed.get("type").is_some() {
                Some(sonic_rs::to_vec(&parsed).ok()?)
            } else {
                None
            }
        }
        Expr::ScalarFunction(func) if func.name().to_lowercase() == "st_point" => {
            if func.args.len() < 2 {
                return None;
            }
            let lng = extract_f64_literal(&func.args[0])?;
            let lat = extract_f64_literal(&func.args[1])?;
            let geojson = serde_json::json!({
                "type": "Point",
                "coordinates": [lng, lat]
            });
            Some(sonic_rs::to_vec(&geojson).ok()?)
        }
        _ => None,
    }
}

/// Extract an f64 from a literal expression.
fn extract_f64_literal(expr: &Expr) -> Option<f64> {
    if let Expr::Literal(lit, _) = expr {
        let s = lit.to_string();
        s.parse::<f64>().ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::logical_expr::expr::ScalarFunction;

    use crate::control::planner::udf::spatial::StDwithin;

    fn make_table_scan() -> LogicalPlan {
        let schema = std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "geom",
                datafusion::arrow::datatypes::DataType::Utf8,
                true,
            ),
        ]));
        let source = datafusion::logical_expr::builder::LogicalTableSource::new(schema);
        let table_ref = datafusion::common::TableReference::bare("locations");
        LogicalPlan::TableScan(
            datafusion::logical_expr::TableScan::try_new(
                table_ref,
                std::sync::Arc::new(source),
                None,
                vec![],
                None,
            )
            .unwrap(),
        )
    }

    fn geojson_point_literal() -> Expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(
                r#"{"type":"Point","coordinates":[-73.98,40.75]}"#.to_string(),
            )),
            None,
        )
    }

    #[test]
    fn extract_dwithin_from_expr() {
        let input = make_table_scan();
        let udf = ScalarUDF::from(StDwithin::new());
        let predicate = Expr::ScalarFunction(ScalarFunction::new_udf(
            std::sync::Arc::new(udf),
            vec![
                col("geom"),
                geojson_point_literal(),
                Expr::Literal(ScalarValue::Float64(Some(500.0)), None),
            ],
        ));

        let task = try_extract_spatial_scan(&predicate, &input, TenantId::new(1)).unwrap();
        match &task.plan {
            crate::bridge::envelope::PhysicalPlan::Spatial(SpatialOp::Scan {
                collection,
                field,
                predicate,
                distance_meters,
                ..
            }) => {
                assert_eq!(collection, "locations");
                assert_eq!(field, "geom");
                assert_eq!(*predicate, SpatialPredicate::DWithin);
                assert!((distance_meters - 500.0).abs() < 0.01);
            }
            other => panic!("expected SpatialOp::Scan, got {other:?}"),
        }
    }

    #[test]
    fn extract_intersects_from_expr() {
        let input = make_table_scan();
        let udf = ScalarUDF::from(crate::control::planner::udf::spatial::StIntersects::new());
        let predicate = Expr::ScalarFunction(ScalarFunction::new_udf(
            std::sync::Arc::new(udf),
            vec![col("geom"), geojson_point_literal()],
        ));

        let task = try_extract_spatial_scan(&predicate, &input, TenantId::new(1)).unwrap();
        match &task.plan {
            crate::bridge::envelope::PhysicalPlan::Spatial(SpatialOp::Scan {
                predicate,
                distance_meters,
                ..
            }) => {
                assert_eq!(*predicate, SpatialPredicate::Intersects);
                assert_eq!(*distance_meters, 0.0);
            }
            other => panic!("expected SpatialOp::Scan, got {other:?}"),
        }
    }

    #[test]
    fn non_spatial_returns_none() {
        let input = make_table_scan();
        let predicate = col("geom").eq(Expr::Literal(ScalarValue::Utf8(Some("foo".into())), None));
        assert!(try_extract_spatial_scan(&predicate, &input, TenantId::new(1)).is_none());
    }

    #[test]
    fn and_conjunction_finds_spatial() {
        let input = make_table_scan();
        let udf = ScalarUDF::from(crate::control::planner::udf::spatial::StWithin::new());
        let spatial = Expr::ScalarFunction(ScalarFunction::new_udf(
            std::sync::Arc::new(udf),
            vec![col("geom"), geojson_point_literal()],
        ));
        let combined = col("name")
            .eq(Expr::Literal(ScalarValue::Utf8(Some("x".into())), None))
            .and(spatial);

        let task = try_extract_spatial_scan(&combined, &input, TenantId::new(1));
        assert!(task.is_some());
    }
}
