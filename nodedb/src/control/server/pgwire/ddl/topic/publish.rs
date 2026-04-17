//! `PUBLISH TO` pgwire adapter — thin wrapper over the unified SQL dispatcher.
//!
//! Syntax: `PUBLISH TO <topic> '<payload>'`

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::sql_dispatch::dispatch_sql;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;

/// Handle `PUBLISH TO <topic> '<payload>'` from pgwire.
///
/// Delegates parsing, escape handling, and cluster-aware forwarding to the
/// pgwire-agnostic `sql_dispatch::dispatch_sql`.
pub async fn handle_publish(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    match dispatch_sql(state, identity, sql).await {
        Some(Ok(_)) => Ok(vec![Response::Execution(Tag::new("PUBLISH"))]),
        Some(Err(e)) => {
            let sqlstate = match &e {
                crate::Error::CollectionNotFound { .. } => "42704",
                crate::Error::BadRequest { .. } => "42601",
                crate::Error::Dispatch { .. } => "58000",
                _ => "XX000",
            };
            Err(sqlstate_error(sqlstate, &e.to_string()))
        }
        None => Err(sqlstate_error(
            "42601",
            "expected PUBLISH TO <topic> '<payload>'",
        )),
    }
}
