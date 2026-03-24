//! DEFINE FIELD handler.
//!
//! Parses: DEFINE FIELD <name> ON <collection> [TYPE <type>] [DEFAULT <expr>]
//!         [VALUE <expr>] [ASSERT <expr>] [READONLY]
//!
//! Stores the field definition in the catalog. Applied during writes (DEFAULT,
//! ASSERT, TYPE validation) and reads (VALUE computed fields).

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::catalog::types::FieldDefinition;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::sqlstate_error;
use super::sql_parse::extract_clause;

/// Keywords that delimit DEFINE FIELD clauses.
const FIELD_KEYWORDS: &[&str] = &["TYPE", "DEFAULT", "VALUE", "ASSERT", "READONLY"];

/// Parse and store a DEFINE FIELD statement.
pub fn define_field(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    // Parse: DEFINE FIELD <name> ON <collection> ...
    let parts: Vec<&str> = sql.split_whitespace().collect();
    if parts.len() < 5 || !parts[3].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error(
            "42601",
            "syntax: DEFINE FIELD <name> ON <collection> [TYPE <type>] [DEFAULT <expr>] [VALUE <expr>] [ASSERT <expr>] [READONLY]",
        ));
    }

    let field_name = parts[2].to_lowercase();
    let collection = parts[4].to_lowercase();
    let tenant_id = identity.tenant_id;

    // Parse optional clauses from the remaining SQL.
    let remainder = if sql.len() > parts[..5].iter().map(|p| p.len() + 1).sum::<usize>() {
        &sql[parts[..5].iter().map(|p| p.len() + 1).sum::<usize>()..]
    } else {
        ""
    };
    let upper_rem = remainder.to_uppercase();

    let field_type = extract_clause(&upper_rem, remainder, "TYPE", FIELD_KEYWORDS);
    let default_expr = extract_clause(&upper_rem, remainder, "DEFAULT", FIELD_KEYWORDS);
    let value_expr = extract_clause(&upper_rem, remainder, "VALUE", FIELD_KEYWORDS);
    let assert_expr = extract_clause(&upper_rem, remainder, "ASSERT", FIELD_KEYWORDS);
    let readonly = upper_rem.contains("READONLY");

    let def = FieldDefinition {
        name: field_name.clone(),
        field_type: field_type.unwrap_or_default(),
        default_expr: default_expr.unwrap_or_default(),
        value_expr: value_expr.unwrap_or_default(),
        assert_expr: assert_expr.unwrap_or_default(),
        readonly,
    };

    // Store in catalog.
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(tenant_id.as_u32(), &collection) {
            Ok(Some(mut coll)) => {
                // Remove existing definition for this field if any.
                coll.field_defs.retain(|f| f.name != field_name);
                coll.field_defs.push(def);

                // Also update the simple fields list for backward compat.
                if !coll.fields.iter().any(|(n, _)| n == &field_name) {
                    let ft = coll
                        .field_defs
                        .last()
                        .map(|d| {
                            if d.field_type.is_empty() {
                                "any".to_string()
                            } else {
                                d.field_type.clone()
                            }
                        })
                        .unwrap_or_else(|| "any".to_string());
                    coll.fields.push((field_name.clone(), ft));
                }

                if let Err(e) = catalog.put_collection(&coll) {
                    return Err(sqlstate_error("XX000", &format!("save collection: {e}")));
                }
            }
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{collection}' does not exist"),
                ));
            }
        }
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("defined field '{field_name}' on '{collection}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DEFINE FIELD"))])
}
