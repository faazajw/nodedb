//! `ANALYZE collection` — collect column statistics.
//!
//! Validates that the collection exists, then returns an explicit error
//! because statistics collection requires a Data Plane scan that is not
//! yet wired into the Control Plane dispatch path.

use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Handle `ANALYZE collection`.
pub fn handle_analyze(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let parts: Vec<&str> = sql.split_whitespace().collect();

    let collection = parts
        .get(1)
        .ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "ANALYZE requires a collection name".to_owned(),
            )))
        })?
        .to_lowercase();

    // Verify collection exists.
    let catalog = state.credentials.catalog().as_ref().ok_or_else(|| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            "catalog not available".to_owned(),
        )))
    })?;

    let _coll = catalog
        .get_collection(tenant_id, &collection)
        .map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("catalog error: {e}"),
            )))
        })?
        .ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P01".to_owned(),
                format!("collection \"{collection}\" does not exist"),
            )))
        })?;

    // ANALYZE requires a Data Plane scan to collect real statistics.
    // Until the Control → Data Plane dispatch for stats collection is wired,
    // return an explicit error rather than storing misleading zero-value stats.
    Err(PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "0A000".to_owned(),
        format!(
            "ANALYZE on \"{collection}\" is not yet supported; \
             statistics collection requires Data Plane integration"
        ),
    ))))
}
