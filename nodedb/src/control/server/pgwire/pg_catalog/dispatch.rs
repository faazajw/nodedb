//! pg_catalog query interception and dispatch.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::tables;

/// Try to handle a SQL query as a pg_catalog virtual-table lookup.
///
/// Returns `Some(Ok(response))` if the query targets a known
/// pg_catalog table, `None` if the query should fall through to the
/// normal planner. The `upper` argument is the uppercased SQL.
pub fn try_pg_catalog(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    upper: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    let table = extract_pg_catalog_table(upper)?;
    let result = match table {
        "pg_database" => tables::pg_database(),
        "pg_namespace" => tables::pg_namespace(),
        "pg_type" => tables::pg_type(),
        "pg_class" => tables::pg_class(state, identity),
        "pg_attribute" => tables::pg_attribute(state, identity),
        "pg_index" => tables::pg_index(),
        "pg_authid" => tables::pg_authid(state, identity),
        _ => return None,
    };
    Some(result)
}

/// Extract the first `pg_catalog.<table>` or bare `pg_<table>`
/// reference from a FROM clause. Returns the lowercase table name
/// if found.
fn extract_pg_catalog_table(upper: &str) -> Option<&'static str> {
    let known = [
        "pg_database",
        "pg_namespace",
        "pg_type",
        "pg_class",
        "pg_attribute",
        "pg_index",
        "pg_authid",
    ];
    for table in &known {
        let qualified = format!("PG_CATALOG.{}", table.to_uppercase());
        let bare = table.to_uppercase();
        if upper.contains(&qualified) || upper.contains(&bare) {
            return Some(table);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_qualified_table() {
        let sql = "SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r'";
        assert_eq!(
            extract_pg_catalog_table(&sql.to_uppercase()),
            Some("pg_class")
        );
    }

    #[test]
    fn extracts_bare_table() {
        let sql = "SELECT oid, typname FROM pg_type";
        assert_eq!(
            extract_pg_catalog_table(&sql.to_uppercase()),
            Some("pg_type")
        );
    }

    #[test]
    fn no_match_for_regular_query() {
        let sql = "SELECT * FROM users WHERE id = 1";
        assert_eq!(extract_pg_catalog_table(&sql.to_uppercase()), None);
    }

    #[test]
    fn handles_join_with_pg_catalog() {
        let sql =
            "SELECT c.oid FROM pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid";
        assert_eq!(
            extract_pg_catalog_table(&sql.to_uppercase()),
            Some("pg_namespace")
        );
    }
}
