//! Migration DDL commands: SHOW MIGRATIONS.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{int8_field, sqlstate_error, text_field};

/// SHOW MIGRATIONS — list active and recent migrations.
///
/// Superuser only.
pub fn show_migrations(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can view migrations",
        ));
    }

    let tracker = match &state.migration_tracker {
        Some(t) => t,
        None => {
            return Err(sqlstate_error(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };

    let snapshots = tracker.snapshot();

    let schema = Arc::new(vec![
        int8_field("vshard_id"),
        text_field("phase"),
        int8_field("elapsed_ms"),
        text_field("active"),
    ]);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    for s in &snapshots {
        encoder.encode_field(&(s.vshard_id as i64))?;
        encoder.encode_field(&s.phase)?;
        encoder.encode_field(&(s.elapsed_ms as i64))?;
        let active_str = if s.is_active { "yes" } else { "no" };
        encoder.encode_field(&active_str)?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
