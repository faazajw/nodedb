//! Trigger firing logic.
//!
//! Called after DML operations to fire matching AFTER ROW triggers.
//! Matches triggers by (collection, event), evaluates WHEN clauses,
//! and invokes the statement executor for each matching trigger body.

use std::collections::HashMap;

use tracing::warn;

use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::planner::procedural::executor::core::{MAX_CASCADE_DEPTH, StatementExecutor};
use crate::control::security::catalog::trigger_types::TriggerTiming;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::registry::DmlEvent;

/// Fire AFTER ROW triggers for an INSERT operation.
///
/// Called after a successful INSERT dispatch. `new_fields` contains the
/// inserted row's field values.
///
/// The trigger body's DML is dispatched through the normal plan+SPSC path,
/// executing in the same logical transaction context.
pub async fn fire_after_insert(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    collection: &str,
    new_fields: &serde_json::Map<String, serde_json::Value>,
    cascade_depth: u32,
) -> crate::Result<()> {
    let triggers =
        state
            .trigger_registry
            .get_matching(tenant_id.as_u32(), collection, DmlEvent::Insert);

    // Filter to AFTER ROW triggers only (BEFORE not yet supported).
    let after_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::After)
        .collect();

    if after_triggers.is_empty() {
        return Ok(());
    }

    if cascade_depth >= MAX_CASCADE_DEPTH {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "trigger cascade depth exceeded ({MAX_CASCADE_DEPTH}): \
                 possible infinite loop on collection '{collection}'"
            ),
        });
    }

    // Build NEW row bindings.
    let new_row: HashMap<String, serde_json::Value> = new_fields
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let bindings = RowBindings::after_insert(collection, new_row);

    for trigger in &after_triggers {
        // Evaluate WHEN clause if present.
        if let Some(ref when_cond) = trigger.when_condition {
            let bound_cond = bindings.substitute(when_cond);
            if !evaluate_simple_condition(&bound_cond) {
                continue;
            }
        }

        // Parse trigger body.
        let block = match crate::control::planner::procedural::parse_block(&trigger.body_sql) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    trigger = %trigger.name,
                    error = %e,
                    "failed to parse trigger body, skipping"
                );
                continue;
            }
        };

        // Execute via statement executor.
        let executor =
            StatementExecutor::new(state, identity.clone(), tenant_id, cascade_depth + 1);

        if let Err(e) = executor.execute_block(&block, &bindings).await {
            // Trigger exception → propagate (rolls back the transaction).
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "trigger '{}' on '{}' failed: {}",
                    trigger.name, collection, e
                ),
            });
        }
    }

    Ok(())
}

/// Simple condition evaluation for WHEN clauses.
///
/// Handles common patterns without needing DataFusion:
/// - `NEW.field > value` (numeric comparison)
/// - `NEW.field = 'literal'` (string equality)
/// - `NEW.field IS NOT NULL`
///
/// Falls back to `true` (fire the trigger) for complex conditions
/// that require DataFusion evaluation. The trigger body itself will
/// handle the condition via IF blocks.
fn evaluate_simple_condition(condition: &str) -> bool {
    // For simple constants, use the shared evaluator.
    // For complex conditions, default to firing (safe: the trigger body
    // can add its own IF checks). A full evaluation would require
    // DataFusion which is too expensive for a WHEN clause check.
    super::try_eval_simple_condition(condition).unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_condition_true() {
        assert!(evaluate_simple_condition("TRUE"));
        assert!(evaluate_simple_condition("1"));
    }

    #[test]
    fn simple_condition_false() {
        assert!(!evaluate_simple_condition("FALSE"));
        assert!(!evaluate_simple_condition("0"));
        assert!(!evaluate_simple_condition("NULL"));
    }

    #[test]
    fn complex_condition_defaults_true() {
        assert!(evaluate_simple_condition("'ord-1' IS NOT NULL"));
    }
}
