//! Prepared statement support for NodeDB-Lite.
//!
//! Caches DataFusion LogicalPlans for SQL statements with `$1`, `$2` placeholders.
//! Avoids re-parsing identical SQL on repeated execution with different parameters.

use datafusion::common::ParamValues;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::LogicalPlan;

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

use super::arrow_convert::arrow_value_at;
use super::engine::LiteQueryEngine;

/// A prepared statement holding a cached DataFusion LogicalPlan.
///
/// Created by `LiteQueryEngine::prepare()`. Execute with different
/// parameter values without re-parsing.
pub struct LitePreparedStatement {
    /// Original SQL text.
    pub sql: String,
    /// Cached logical plan (contains Placeholder nodes for `$1`, `$2`, etc.).
    plan: LogicalPlan,
}

impl LitePreparedStatement {
    /// Number of parameters in the prepared statement.
    pub fn param_count(&self) -> usize {
        count_placeholders(&self.sql)
    }
}

impl<S: StorageEngine> LiteQueryEngine<S> {
    /// Parse and cache a SQL statement for repeated execution.
    ///
    /// The SQL may contain `$1`, `$2`, ... placeholders for parameters.
    /// Returns a `LitePreparedStatement` that can be executed multiple times
    /// with different parameter values.
    pub async fn prepare(&self, sql: &str) -> Result<LitePreparedStatement, LiteError> {
        // Auto-register collections so DataFusion can resolve table names.
        self.register_all_collections();

        let plan = self
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| LiteError::Query(format!("prepare: {e}")))?;

        Ok(LitePreparedStatement {
            sql: sql.to_owned(),
            plan,
        })
    }

    /// Execute a prepared statement with parameter values.
    ///
    /// Parameters are positional: `params[0]` binds to `$1`, etc.
    /// Pass an empty slice for statements with no parameters.
    pub async fn execute_prepared(
        &self,
        stmt: &LitePreparedStatement,
        params: &[ScalarValue],
    ) -> Result<QueryResult, LiteError> {
        // Substitute parameters into the plan.
        let plan = if params.is_empty() {
            stmt.plan.clone()
        } else {
            let param_values: ParamValues = params.to_vec().into();
            stmt.plan
                .clone()
                .with_param_values(param_values)
                .map_err(|e| LiteError::Query(format!("parameter bind: {e}")))?
        };

        // Create a DataFrame from the plan and execute.
        let df = self
            .ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|e| LiteError::Query(format!("execute: {e}")))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| LiteError::Query(format!("collect: {e}")))?;

        // Convert Arrow RecordBatches to QueryResult.
        let mut columns: Vec<String> = Vec::new();
        let mut rows: Vec<Vec<Value>> = Vec::new();

        for batch in &batches {
            if columns.is_empty() {
                columns = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
            }

            let num_rows = batch.num_rows();
            for row_idx in 0..num_rows {
                let mut row = Vec::with_capacity(columns.len());
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    let value = arrow_value_at(col, row_idx)?;
                    row.push(value);
                }
                rows.push(row);
            }
        }

        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
        })
    }
}

/// Count `$N` placeholders in SQL (returns the max N).
fn count_placeholders(sql: &str) -> usize {
    let mut max_idx = 0usize;
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            i += 1;
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i > start
                && let Ok(s) = std::str::from_utf8(&bytes[start..i])
                && let Ok(idx) = s.parse::<usize>()
            {
                max_idx = max_idx.max(idx);
            }
        } else {
            i += 1;
        }
    }
    max_idx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_placeholders_basic() {
        assert_eq!(count_placeholders("SELECT $1, $2, $3"), 3);
        assert_eq!(count_placeholders("SELECT 1"), 0);
        assert_eq!(count_placeholders("$10 $2"), 10);
    }
}
