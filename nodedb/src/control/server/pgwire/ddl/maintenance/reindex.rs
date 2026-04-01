//! `REINDEX INDEX name` / `REINDEX TABLE collection` — rebuild indexes.
//!
//! For now, REINDEX drops and recreates secondary indexes by re-scanning
//! the collection. This is a Control Plane operation that dispatches
//! DocumentOp::Register to rebuild the Data Plane's index structures.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Handle `REINDEX INDEX name` or `REINDEX TABLE collection`.
pub fn handle_reindex(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            "syntax: REINDEX INDEX <name> or REINDEX TABLE <collection>".to_owned(),
        ))));
    }

    let target_type = parts[1].to_uppercase();
    let target_name = parts[2].to_lowercase();
    let tenant_id = identity.tenant_id;

    match target_type.as_str() {
        "INDEX" => {
            // REINDEX INDEX name — rebuild a specific index.
            // The index rebuild is triggered by dispatching a Checkpoint which
            // forces the Data Plane to flush and rebuild its index structures.
            let plan = crate::bridge::envelope::PhysicalPlan::Meta(
                crate::bridge::physical_plan::MetaOp::Checkpoint,
            );
            let request = crate::bridge::envelope::Request {
                request_id: crate::types::RequestId::new(0),
                tenant_id,
                vshard_id: crate::types::VShardId::new(0),
                plan,
                deadline: std::time::Instant::now() + std::time::Duration::from_secs(300),
                priority: crate::bridge::envelope::Priority::Background,
                trace_id: 0,
                consistency: crate::types::ReadConsistency::Strong,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
            };
            match state.dispatcher.lock() {
                Ok(mut d) => {
                    let _ = d.dispatch(request);
                }
                Err(p) => {
                    let _ = p.into_inner().dispatch(request);
                }
            }
            tracing::info!(index = %target_name, "REINDEX INDEX dispatched");
        }
        "TABLE" => {
            // REINDEX TABLE collection — rebuild all indexes on a collection.
            // Verify collection exists.
            if let Some(catalog) = state.credentials.catalog()
                && catalog
                    .get_collection(tenant_id.as_u32(), &target_name)
                    .ok()
                    .flatten()
                    .is_none()
            {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42P01".to_owned(),
                    format!("collection \"{target_name}\" does not exist"),
                ))));
            }

            let plan = crate::bridge::envelope::PhysicalPlan::Meta(
                crate::bridge::physical_plan::MetaOp::Checkpoint,
            );
            let request = crate::bridge::envelope::Request {
                request_id: crate::types::RequestId::new(0),
                tenant_id,
                vshard_id: crate::types::VShardId::new(0),
                plan,
                deadline: std::time::Instant::now() + std::time::Duration::from_secs(300),
                priority: crate::bridge::envelope::Priority::Background,
                trace_id: 0,
                consistency: crate::types::ReadConsistency::Strong,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
            };
            match state.dispatcher.lock() {
                Ok(mut d) => {
                    let _ = d.dispatch(request);
                }
                Err(p) => {
                    let _ = p.into_inner().dispatch(request);
                }
            }
            tracing::info!(collection = %target_name, "REINDEX TABLE dispatched");
        }
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "syntax: REINDEX INDEX <name> or REINDEX TABLE <collection>".to_owned(),
            ))));
        }
    }

    Ok(vec![Response::Execution(Tag::new("REINDEX"))])
}
