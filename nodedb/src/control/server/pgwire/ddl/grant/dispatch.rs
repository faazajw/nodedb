//! Top-level routers for `GRANT` and `REVOKE` SQL statements.
//! Decides between role-membership and permission-grant paths
//! based on whether the second token is `ROLE`.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;
use super::permission::{grant_permission, revoke_permission};
use super::role::{grant_role, revoke_role};

/// `GRANT ROLE <role> TO <user>` or
/// `GRANT <perm> ON <collection|FUNCTION name> TO <grantee>`.
pub fn handle_grant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: GRANT ROLE <role> TO <user> | GRANT <perm> ON <collection> TO <grantee>",
        ));
    }

    if parts[1].eq_ignore_ascii_case("ROLE") {
        return grant_role(state, identity, parts);
    }

    grant_permission(state, identity, parts)
}

/// `REVOKE ROLE <role> FROM <user>` or
/// `REVOKE <perm> ON <collection|FUNCTION name> FROM <grantee>`.
pub fn handle_revoke(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: REVOKE ROLE <role> FROM <user> | REVOKE <perm> ON <collection> FROM <grantee>",
        ));
    }

    if parts[1].eq_ignore_ascii_case("ROLE") {
        return revoke_role(state, identity, parts);
    }

    revoke_permission(state, identity, parts)
}
