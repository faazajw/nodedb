//! NodeDbQueryParser — pgwire `QueryParser` implementation.
//!
//! Converts incoming SQL (from a Parse message) into a `ParsedStatement`
//! with inferred parameter types and result schema. The actual plan
//! execution happens later in `do_query` when Execute is called.

use std::sync::Arc;

use async_trait::async_trait;
use pgwire::api::results::FieldInfo;
use pgwire::api::stmt::QueryParser;
use pgwire::api::{ClientInfo, Type};
use pgwire::error::PgWireResult;

use crate::control::planner::context::QueryContext;
use crate::control::state::SharedState;

use super::statement::ParsedStatement;

/// Implements pgwire's `QueryParser` trait for NodeDB.
///
/// On Parse message: parses SQL via DataFusion, extracts placeholder types
/// from the analyzed logical plan, and computes the result schema.
///
/// Lives on the Control Plane (Send + Sync).
pub struct NodeDbQueryParser {
    state: Arc<SharedState>,
}

impl NodeDbQueryParser {
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }

    /// Try to infer parameter and result types by parsing through DataFusion.
    ///
    /// For DDL and non-plannable statements, returns empty types (the statement
    /// will be handled by the DDL dispatch path at execute time).
    async fn try_infer_types(
        &self,
        sql: &str,
        client_types: &[Option<Type>],
    ) -> (Vec<Option<Type>>, Vec<FieldInfo>) {
        // Use tenant 1 as default for schema resolution during parse.
        // The actual tenant is resolved at execute time from the connection identity.
        let query_ctx = QueryContext::for_state(&self.state, 1);

        // Try to create a logical plan. If this fails (DDL, non-standard SQL, etc.),
        // fall back to empty types — the statement will be handled as raw SQL at
        // execute time via the same `execute_sql` path as SimpleQuery.
        let plan = match query_ctx.session().state().create_logical_plan(sql).await {
            Ok(plan) => plan,
            Err(_) => return (client_types.to_vec(), Vec::new()),
        };

        // Extract placeholder types from the plan.
        let param_types = infer_param_types(&plan, client_types);

        // Extract result schema.
        let result_fields = plan_to_field_info(&plan);

        (param_types, result_fields)
    }
}

#[async_trait]
impl QueryParser for NodeDbQueryParser {
    type Statement = ParsedStatement;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let (param_types, result_fields) = self.try_infer_types(sql, types).await;

        Ok(ParsedStatement {
            sql: sql.to_owned(),
            param_types,
            result_fields,
        })
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(stmt
            .param_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::UNKNOWN))
            .collect())
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        _column_format: Option<&pgwire::api::portal::Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(stmt.result_fields.clone())
    }
}

/// Infer parameter types from a DataFusion `LogicalPlan`.
///
/// Walks the plan tree looking for `Expr::Placeholder` nodes and extracts
/// their inferred types. Client-provided types take precedence.
fn infer_param_types(
    plan: &datafusion::logical_expr::LogicalPlan,
    client_types: &[Option<Type>],
) -> Vec<Option<Type>> {
    let mut placeholders: Vec<(usize, Option<Type>)> = Vec::new();

    // Walk all expressions in the plan to find placeholders.
    collect_placeholders_from_plan(plan, &mut placeholders);

    if placeholders.is_empty() && client_types.is_empty() {
        return Vec::new();
    }

    // Determine max parameter index.
    let max_from_plan = placeholders.iter().map(|(idx, _)| *idx).max().unwrap_or(0);
    let max_idx = max_from_plan.max(client_types.len());

    let mut result = vec![None; max_idx];

    // Fill from plan inference.
    for (idx, pg_type) in &placeholders {
        if *idx > 0 && *idx <= result.len() && result[idx - 1].is_none() {
            result[idx - 1] = pg_type.clone();
        }
    }

    // Client-provided types override.
    for (i, client_type) in client_types.iter().enumerate() {
        if let Some(t) = client_type {
            result[i] = Some(t.clone());
        }
    }

    result
}

/// Recursively collect placeholder expressions from a logical plan.
fn collect_placeholders_from_plan(
    plan: &datafusion::logical_expr::LogicalPlan,
    out: &mut Vec<(usize, Option<Type>)>,
) {
    // Collect from this plan node's expressions.
    for expr in plan.expressions() {
        collect_placeholders_from_expr(&expr, out);
    }

    // Recurse into child plans.
    for child in plan.inputs() {
        collect_placeholders_from_plan(child, out);
    }
}

/// Extract placeholder index and inferred type from an expression tree.
fn collect_placeholders_from_expr(
    expr: &datafusion::logical_expr::Expr,
    out: &mut Vec<(usize, Option<Type>)>,
) {
    use datafusion::logical_expr::Expr;

    match expr {
        Expr::Placeholder(placeholder) => {
            // Parse "$1", "$2", etc. to get the 1-based index.
            if let Some(idx) = placeholder
                .id
                .strip_prefix('$')
                .and_then(|s| s.parse::<usize>().ok())
            {
                let pg_type = placeholder
                    .field
                    .as_ref()
                    .and_then(|f| arrow_type_to_pg_type(f.data_type()));
                out.push((idx, pg_type));
            }
        }
        // Binary operations, comparisons, etc. — recurse into children.
        Expr::BinaryExpr(b) => {
            collect_placeholders_from_expr(&b.left, out);
            collect_placeholders_from_expr(&b.right, out);
        }
        Expr::Not(e)
        | Expr::IsNotNull(e)
        | Expr::IsNull(e)
        | Expr::IsTrue(e)
        | Expr::IsFalse(e)
        | Expr::IsNotTrue(e)
        | Expr::IsNotFalse(e)
        | Expr::IsUnknown(e)
        | Expr::IsNotUnknown(e)
        | Expr::Negative(e)
        | Expr::Cast(datafusion::logical_expr::Cast { expr: e, .. })
        | Expr::TryCast(datafusion::logical_expr::TryCast { expr: e, .. }) => {
            collect_placeholders_from_expr(e, out);
        }
        Expr::Between(b) => {
            collect_placeholders_from_expr(&b.expr, out);
            collect_placeholders_from_expr(&b.low, out);
            collect_placeholders_from_expr(&b.high, out);
        }
        Expr::Like(l) => {
            collect_placeholders_from_expr(&l.expr, out);
            collect_placeholders_from_expr(&l.pattern, out);
        }
        Expr::InList(i) => {
            collect_placeholders_from_expr(&i.expr, out);
            for e in &i.list {
                collect_placeholders_from_expr(e, out);
            }
        }
        Expr::ScalarFunction(f) => {
            for arg in &f.args {
                collect_placeholders_from_expr(arg, out);
            }
        }
        Expr::AggregateFunction(f) => {
            for arg in &f.params.args {
                collect_placeholders_from_expr(arg, out);
            }
            if let Some(filter) = &f.params.filter {
                collect_placeholders_from_expr(filter, out);
            }
            for ob in &f.params.order_by {
                collect_placeholders_from_expr(&ob.expr, out);
            }
        }
        Expr::Alias(a) => {
            collect_placeholders_from_expr(&a.expr, out);
        }
        Expr::Case(c) => {
            if let Some(e) = &c.expr {
                collect_placeholders_from_expr(e, out);
            }
            for (when, then) in &c.when_then_expr {
                collect_placeholders_from_expr(when, out);
                collect_placeholders_from_expr(then, out);
            }
            if let Some(e) = &c.else_expr {
                collect_placeholders_from_expr(e, out);
            }
        }
        // Leaf expressions with no children — nothing to recurse.
        Expr::Column(_) | Expr::Literal(..) | Expr::OuterReferenceColumn(..) => {}
        // For any other expression variant, skip (safe — we may miss some
        // deeply nested placeholders but won't produce incorrect types).
        _ => {}
    }
}

/// Convert an Arrow DataType to a PostgreSQL Type.
fn arrow_type_to_pg_type(dt: &datafusion::arrow::datatypes::DataType) -> Option<Type> {
    use datafusion::arrow::datatypes::DataType;
    Some(match dt {
        DataType::Boolean => Type::BOOL,
        DataType::Int8 | DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::UInt8 | DataType::UInt16 => Type::INT4,
        DataType::UInt32 => Type::INT8,
        DataType::UInt64 => Type::INT8,
        DataType::Float16 | DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Type::TEXT,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Type::BYTEA,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Timestamp(..) => Type::TIMESTAMP,
        DataType::Time32(..) | DataType::Time64(..) => Type::TIME,
        DataType::Interval(..) => Type::INTERVAL,
        DataType::Decimal128(..) | DataType::Decimal256(..) => Type::NUMERIC,
        DataType::Null => return None,
        _ => Type::TEXT, // fallback for complex types
    })
}

/// Convert a DataFusion `LogicalPlan`'s output schema to pgwire `FieldInfo`.
fn plan_to_field_info(plan: &datafusion::logical_expr::LogicalPlan) -> Vec<FieldInfo> {
    use pgwire::api::results::FieldFormat;

    let schema = plan.schema();
    schema
        .fields()
        .iter()
        .map(|field| {
            let pg_type = arrow_type_to_pg_type(field.data_type()).unwrap_or(Type::TEXT);
            FieldInfo::new(field.name().clone(), None, None, pg_type, FieldFormat::Text)
        })
        .collect()
}
