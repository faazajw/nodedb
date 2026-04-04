mod admin;
mod auth;
mod collaborative;
mod dsl;
mod engine_ops;
mod function;
mod helpers;
mod schema;
mod streaming;

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Try to handle a SQL statement as a Control Plane DDL command.
///
/// These execute directly on the Control Plane without going through
/// DataFusion or the Data Plane. Returns `None` if not recognized.
///
/// Async because DSL commands (SEARCH, CRDT) dispatch to the Data Plane
/// and must await the response without blocking the Tokio runtime.
pub async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    let upper = sql.to_uppercase();
    let parts: Vec<&str> = sql.split_whitespace().collect();

    if let Some(r) = auth::dispatch(state, identity, sql, &upper, &parts).await {
        return Some(r);
    }

    if let Some(r) = function::dispatch(state, identity, sql, &upper, &parts).await {
        return Some(r);
    }

    if let Some(r) = streaming::dispatch(state, identity, sql, &upper, &parts).await {
        return Some(r);
    }

    if let Some(r) = engine_ops::dispatch(state, identity, sql, &upper, &parts).await {
        return Some(r);
    }

    if let Some(r) = schema::dispatch(state, identity, sql, &upper, &parts).await {
        return Some(r);
    }

    if let Some(r) = collaborative::dispatch(state, identity, sql, &upper, &parts).await {
        return Some(r);
    }

    if let Some(r) = admin::dispatch(state, identity, sql, &upper, &parts).await {
        return Some(r);
    }

    if let Some(r) = dsl::dispatch(state, identity, sql, &upper, &parts).await {
        return Some(r);
    }

    None
}
