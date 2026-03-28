//! Usage metering DDL commands.
//!
//! ```sql
//! DEFINE METERING DIMENSION 'api_calls' UNIT 'calls'
//! SHOW USAGE FOR AUTH USER 'user_42'
//! SHOW USAGE FOR ORG 'acme'
//! SHOW QUOTA FOR AUTH USER 'user_42'
//! ```

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{sqlstate_error, text_field};

/// DEFINE METERING DIMENSION '<name>' UNIT '<unit>'
pub fn define_dimension(
    _state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser",
        ));
    }
    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DEFINE METERING DIMENSION '<name>' UNIT '<unit>'",
        ));
    }
    let _name = parts[3].trim_matches('\'');
    let _unit = parts
        .iter()
        .position(|p| p.to_uppercase() == "UNIT")
        .and_then(|i| parts.get(i + 1))
        .map(|s| s.trim_matches('\''))
        .unwrap_or("tokens");

    // Custom dimensions are stored in config, not in a catalog table.
    // For now, acknowledge the command.
    Ok(vec![Response::Execution(Tag::new(
        "DEFINE METERING DIMENSION",
    ))])
}

/// SHOW USAGE FOR AUTH USER '<id>' / SHOW USAGE FOR ORG '<id>'
pub fn show_usage(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let (user_filter, org_filter) = parse_for_clause(parts);

    let events = state.usage_store.query(
        user_filter.as_deref(),
        org_filter.as_deref(),
        0, // All time.
    );

    let schema = Arc::new(vec![
        text_field("auth_user_id"),
        text_field("org_id"),
        text_field("collection"),
        text_field("operation"),
        text_field("tokens"),
        text_field("timestamp"),
    ]);

    let rows: Vec<_> = events
        .iter()
        .map(|e| {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&e.auth_user_id);
            let _ = enc.encode_field(&e.org_id);
            let _ = enc.encode_field(&e.collection);
            let _ = enc.encode_field(&e.operation);
            let _ = enc.encode_field(&e.tokens.to_string());
            let _ = enc.encode_field(&e.timestamp_secs.to_string());
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// SHOW QUOTA FOR AUTH USER '<id>' / SHOW QUOTA FOR ORG '<id>'
pub fn show_quota(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let (user_filter, _org_filter) = parse_for_clause(parts);
    let grantee_id = user_filter.as_deref().unwrap_or("");

    let quotas = state.quota_manager.list_quotas();

    let schema = Arc::new(vec![
        text_field("scope"),
        text_field("max_tokens"),
        text_field("used_tokens"),
        text_field("remaining"),
        text_field("pct_used"),
        text_field("enforcement"),
        text_field("exceeded"),
    ]);

    let rows: Vec<_> = quotas
        .iter()
        .filter_map(|q| state.quota_manager.get_status(&q.scope_name, grantee_id))
        .map(|s| {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&s.scope_name);
            let _ = enc.encode_field(&s.max_tokens.to_string());
            let _ = enc.encode_field(&s.used_tokens.to_string());
            let _ = enc.encode_field(&s.remaining.to_string());
            let _ = enc.encode_field(&format!("{:.1}%", s.pct_used * 100.0));
            let _ = enc.encode_field(&format!("{:?}", s.enforcement));
            let _ = enc.encode_field(&s.exceeded.to_string());
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Parse FOR AUTH USER '<id>' or FOR ORG '<id>' from parts.
fn parse_for_clause(parts: &[&str]) -> (Option<String>, Option<String>) {
    let for_idx = parts.iter().position(|p| p.to_uppercase() == "FOR");
    let Some(idx) = for_idx else {
        return (None, None);
    };

    let grantee_type = parts
        .get(idx + 1)
        .map(|s| s.to_uppercase())
        .unwrap_or_default();
    match grantee_type.as_str() {
        "AUTH" => {
            // FOR AUTH USER '<id>'
            let id = parts.get(idx + 3).map(|s| s.trim_matches('\'').to_string());
            (id, None)
        }
        "ORG" => {
            let id = parts.get(idx + 2).map(|s| s.trim_matches('\'').to_string());
            (None, id)
        }
        "USER" => {
            let id = parts.get(idx + 2).map(|s| s.trim_matches('\'').to_string());
            (id, None)
        }
        _ => (None, None),
    }
}
