//! Statement executor for procedural SQL blocks with DML.
//!
//! Steps through procedural statements sequentially, dispatching each
//! embedded DML statement through the normal plan+SPSC path. Used by
//! triggers (Tier 3) and stored procedures (Tier 4).
//!
//! Runs on the **Control Plane** (Tokio async). Each DML in the trigger
//! body is planned via DataFusion and dispatched to the Data Plane through
//! the existing SPSC bridge.

use crate::control::planner::procedural::ast::*;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::bindings::RowBindings;

/// Maximum trigger cascade depth (trigger A fires trigger B fires trigger A).
pub const MAX_CASCADE_DEPTH: u32 = 16;

/// Statement executor: steps through procedural SQL blocks with DML.
///
/// Each instance tracks the current cascade depth to prevent infinite loops.
pub struct StatementExecutor<'a> {
    state: &'a SharedState,
    /// Stored for SECURITY INVOKER enforcement on trigger body DML.
    #[allow(dead_code)]
    identity: AuthenticatedIdentity,
    tenant_id: TenantId,
    cascade_depth: u32,
}

impl<'a> StatementExecutor<'a> {
    pub fn new(
        state: &'a SharedState,
        identity: AuthenticatedIdentity,
        tenant_id: TenantId,
        cascade_depth: u32,
    ) -> Self {
        Self {
            state,
            identity,
            tenant_id,
            cascade_depth,
        }
    }

    /// Execute a procedural block with the given row bindings.
    ///
    /// Each DML statement in the body is substituted with NEW/OLD values,
    /// planned via DataFusion, and dispatched to the Data Plane.
    pub async fn execute_block(
        &self,
        block: &ProceduralBlock,
        bindings: &RowBindings,
    ) -> crate::Result<()> {
        for stmt in &block.statements {
            self.execute_statement(stmt, bindings).await?;
        }
        Ok(())
    }

    fn execute_statement<'b>(
        &'b self,
        stmt: &'b Statement,
        bindings: &'b RowBindings,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<()>> + Send + 'b>> {
        Box::pin(async move {
            match stmt {
                Statement::Dml { sql } => self.execute_dml(sql, bindings).await,
                Statement::If {
                    condition,
                    then_block,
                    elsif_branches,
                    else_block,
                } => {
                    // Evaluate condition by substituting bindings.
                    let cond_sql = bindings.substitute(&condition.sql);
                    if self.evaluate_condition(&cond_sql).await? {
                        return self.execute_statements(then_block, bindings).await;
                    }
                    for branch in elsif_branches {
                        let branch_cond = bindings.substitute(&branch.condition.sql);
                        if self.evaluate_condition(&branch_cond).await? {
                            return self.execute_statements(&branch.body, bindings).await;
                        }
                    }
                    if let Some(else_stmts) = else_block {
                        return self.execute_statements(else_stmts, bindings).await;
                    }
                    Ok(())
                }
                Statement::Raise {
                    level: RaiseLevel::Exception,
                    message,
                } => {
                    let msg = bindings.substitute(&message.sql);
                    // Strip surrounding quotes if present.
                    let clean_msg = msg.trim().trim_matches('\'').to_string();
                    Err(crate::Error::BadRequest {
                        detail: format!("trigger raised exception: {clean_msg}"),
                    })
                }
                Statement::Raise { .. } => {
                    // NOTICE/WARNING — log but continue.
                    Ok(())
                }
                Statement::Declare { .. } | Statement::Assign { .. } => {
                    // Variables in trigger bodies are handled by substitution
                    // in the procedural parser. For the statement executor,
                    // we skip DECLARE/ASSIGN (they have no runtime effect
                    // when the body uses NEW/OLD substitution).
                    Ok(())
                }
                Statement::Return { .. } | Statement::ReturnQuery { .. } => {
                    // Triggers don't return values. Ignore.
                    Ok(())
                }
                Statement::Break | Statement::Continue => {
                    // Should not appear outside loops. Ignore gracefully.
                    Ok(())
                }
                Statement::Loop { .. } | Statement::While { .. } | Statement::For { .. } => {
                    // Loops in trigger bodies are uncommon but possible.
                    // For now, reject at runtime. The procedural compiler
                    // handles loops for functions; triggers use the executor.
                    Err(crate::Error::BadRequest {
                        detail: "LOOP/WHILE/FOR not yet supported in trigger bodies".into(),
                    })
                }
                Statement::Commit | Statement::Rollback => Err(crate::Error::BadRequest {
                    detail: "COMMIT/ROLLBACK not allowed in trigger bodies".into(),
                }),
            }
        })
    }

    async fn execute_statements(
        &self,
        stmts: &[Statement],
        bindings: &RowBindings,
    ) -> crate::Result<()> {
        for stmt in stmts {
            self.execute_statement(stmt, bindings).await?;
        }
        Ok(())
    }

    /// Execute a DML statement from the trigger body.
    ///
    /// Substitutes NEW/OLD/TG_* variables, plans via DataFusion, and
    /// dispatches to the Data Plane through the normal path.
    async fn execute_dml(&self, sql: &str, bindings: &RowBindings) -> crate::Result<()> {
        let bound_sql = bindings.substitute(sql);

        // Plan the DML via DataFusion.
        let ctx = crate::control::planner::context::QueryContext::with_catalog(
            std::sync::Arc::clone(&self.state.credentials),
            self.tenant_id.as_u32(),
        );
        let tasks = ctx.plan_sql(&bound_sql, self.tenant_id).await?;

        // Dispatch each task to the Data Plane.
        for task in tasks {
            // WAL append for durability.
            crate::control::server::dispatch_utils::wal_append_if_write(
                &self.state.wal,
                task.tenant_id,
                task.vshard_id,
                &task.plan,
            )?;

            // Dispatch to Data Plane.
            crate::control::server::dispatch_utils::dispatch_to_data_plane(
                self.state,
                task.tenant_id,
                task.vshard_id,
                task.plan,
                0,
            )
            .await?;
        }

        Ok(())
    }

    /// Evaluate a SQL boolean condition by running it as a SELECT query.
    ///
    /// Returns true if the condition evaluates to a truthy value.
    async fn evaluate_condition(&self, condition_sql: &str) -> crate::Result<bool> {
        // For simple constant conditions, avoid DataFusion overhead.
        if let Some(result) = crate::control::trigger::try_eval_simple_condition(condition_sql) {
            return Ok(result);
        }

        // Plan and execute: SELECT (<condition>) as __cond
        let ctx = crate::control::planner::context::QueryContext::with_catalog(
            std::sync::Arc::clone(&self.state.credentials),
            self.tenant_id.as_u32(),
        );
        let select_sql = format!("SELECT ({condition_sql}) as __cond");
        let df = ctx
            .session()
            .sql(&select_sql)
            .await
            .map_err(|e| crate::Error::PlanError {
                detail: format!("condition eval: {e}"),
            })?;
        let batches = df.collect().await.map_err(|e| crate::Error::PlanError {
            detail: format!("condition eval collect: {e}"),
        })?;

        // Extract the boolean result from the first row.
        for batch in &batches {
            if batch.num_rows() > 0 {
                let col = batch.column(0);
                if let Some(bool_arr) = col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                {
                    return Ok(bool_arr.value(0));
                }
                // Fallback: check if numeric truthy.
                if let Some(int_arr) = col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int32Array>()
                {
                    return Ok(int_arr.value(0) != 0);
                }
            }
        }

        // Default to false if evaluation fails.
        Ok(false)
    }

    /// Current cascade depth.
    pub fn cascade_depth(&self) -> u32 {
        self.cascade_depth
    }
}
