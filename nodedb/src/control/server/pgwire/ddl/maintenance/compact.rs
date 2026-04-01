//! `COMPACT collection [PARTITION 'name']` — trigger manual compaction.
//!
//! Dispatches a MetaOp::Compact to the Data Plane via the standard
//! dispatch path. The Data Plane merges segments for the receiving core.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Handle `COMPACT collection [PARTITION 'name']`.
pub fn handle_compact(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 2 {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            "COMPACT requires a collection name".to_owned(),
        ))));
    }

    let collection = parts[1].to_lowercase();
    let tenant_id = identity.tenant_id;

    // Verify collection exists.
    if let Some(catalog) = state.credentials.catalog()
        && catalog
            .get_collection(tenant_id.as_u32(), &collection)
            .ok()
            .flatten()
            .is_none()
    {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42P01".to_owned(),
            format!("collection \"{collection}\" does not exist"),
        ))));
    }

    // Dispatch MetaOp::Compact to the Data Plane.
    let plan =
        crate::bridge::envelope::PhysicalPlan::Meta(crate::bridge::physical_plan::MetaOp::Compact);

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

    tracing::info!(%collection, "COMPACT dispatched");

    Ok(vec![Response::Execution(Tag::new("COMPACT"))])
}
