//! `REFRESH MATERIALIZED VIEW` — dispatches a Data Plane meta
//! operation that scans the source collection and writes rows
//! into the view's target.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;

pub async fn refresh_materialized_view(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: REFRESH MATERIALIZED VIEW <name>",
        ));
    }

    let name = parts[3].to_lowercase();
    let tenant_id = identity.tenant_id;

    // Look up view definition to get source collection.
    let view = if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_materialized_view(tenant_id.as_u32(), &name) {
            Ok(Some(v)) => v,
            Ok(None) => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("materialized view '{name}' does not exist"),
                ));
            }
            Err(e) => return Err(sqlstate_error("XX000", &e.to_string())),
        }
    } else {
        return Err(sqlstate_error("XX000", "catalog unavailable"));
    };

    // Dispatch refresh to Data Plane: scan source → write to target.
    let plan = crate::bridge::envelope::PhysicalPlan::Meta(
        crate::bridge::physical_plan::MetaOp::RefreshMaterializedView {
            view_name: name.clone(),
            source_collection: view.source.clone(),
        },
    );

    super::super::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        &view.source,
        plan,
        std::time::Duration::from_secs(30),
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &format!("refresh failed: {e}")))?;

    Ok(vec![Response::Execution(Tag::new(
        "REFRESH MATERIALIZED VIEW",
    ))])
}
