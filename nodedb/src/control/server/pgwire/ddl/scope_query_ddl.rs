//! Scope query DDL commands: ALTER SCOPE, SHOW MY SCOPES, SHOW SCOPES FOR.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{sqlstate_error, text_field};

/// ALTER SCOPE '<name>' SET GRANTS <perm> ON <coll> [, ...] [INCLUDE '<scope>']
pub fn alter_scope(
    state: &SharedState,
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
            "syntax: ALTER SCOPE '<name>' SET GRANTS <perm> ON <coll> [, ...] [INCLUDE '<scope>']",
        ));
    }

    let scope_name = parts[2].trim_matches('\'');
    let set_idx = parts
        .iter()
        .position(|p| p.to_uppercase() == "SET")
        .ok_or_else(|| sqlstate_error("42601", "missing SET keyword"))?;

    let def_parts = &parts[set_idx + 1..];
    let mut grants = Vec::new();
    let mut includes = Vec::new();
    let mut has_grants = false;

    let mut i = 0;
    while i < def_parts.len() {
        let token = def_parts[i].to_uppercase();
        match token.as_str() {
            "GRANTS" => {
                has_grants = true;
                i += 1;
            }
            "INCLUDE" if i + 1 < def_parts.len() => {
                includes.push(
                    def_parts[i + 1]
                        .trim_matches('\'')
                        .trim_end_matches(',')
                        .to_string(),
                );
                i += 2;
            }
            "READ" | "WRITE" | "CREATE" | "DROP" | "ALTER" | "ADMIN"
                if i + 2 < def_parts.len() && def_parts[i + 1].to_uppercase() == "ON" =>
            {
                grants.push((
                    token.to_lowercase(),
                    def_parts[i + 2]
                        .trim_matches('\'')
                        .trim_end_matches(',')
                        .to_string(),
                ));
                i += 3;
            }
            _ => {
                i += 1;
            }
        }
    }

    let grants_opt = if has_grants || !grants.is_empty() {
        Some(grants)
    } else {
        None
    };
    let includes_opt = if !includes.is_empty() {
        Some(includes)
    } else {
        None
    };

    let found = state
        .scope_defs
        .alter(scope_name, grants_opt, includes_opt)
        .map_err(|e| sqlstate_error("42601", &e.to_string()))?;

    if !found {
        return Err(sqlstate_error(
            "42704",
            &format!("scope '{scope_name}' not found"),
        ));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("altered scope '{scope_name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER SCOPE"))])
}

/// SHOW MY SCOPES — show effective scopes for the current user.
pub fn show_my_scopes(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let user_id = identity.user_id.to_string();
    let org_ids = state.orgs.orgs_for_user(&user_id);
    let effective = state.scope_grants.effective_scopes(&user_id, &org_ids);

    let schema = Arc::new(vec![text_field("scope"), text_field("source")]);

    let mut rows = Vec::new();
    for scope_name in &effective {
        let source = if state
            .scope_grants
            .scopes_for("user", &user_id)
            .contains(scope_name)
        {
            "direct"
        } else {
            "org"
        };
        let mut enc = DataRowEncoder::new(schema.clone());
        let _ = enc.encode_field(scope_name);
        let _ = enc.encode_field(&source);
        rows.push(Ok(enc.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// SHOW SCOPES FOR USER '<id>' / SHOW SCOPES FOR ORG '<id>'
pub fn show_scopes_for(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // SHOW SCOPES FOR <USER|ORG> '<id>'
    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: SHOW SCOPES FOR <USER|ORG> '<id>'",
        ));
    }

    let grantee_type = parts[3].to_lowercase();
    let grantee_id = parts[4].trim_matches('\'');

    let scopes = match grantee_type.as_str() {
        "user" => {
            let org_ids = state.orgs.orgs_for_user(grantee_id);
            state.scope_grants.effective_scopes(grantee_id, &org_ids)
        }
        "org" => state
            .scope_grants
            .scopes_for("org", grantee_id)
            .into_iter()
            .collect(),
        _ => return Err(sqlstate_error("42601", "expected USER or ORG")),
    };

    let schema = Arc::new(vec![text_field("scope")]);
    let rows: Vec<_> = scopes
        .iter()
        .map(|s| {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(s);
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
