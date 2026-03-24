//! INSERT INTO dispatch for schemaless collections.
//!
//! Intercepts INSERT for collections without typed schemas, parses
//! column names and values manually, serializes as JSON, and dispatches
//! as PointPut + optional VectorInsert.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::sqlstate_error;
use super::sql_parse::{parse_array_literal, parse_sql_value, split_values};

/// INSERT INTO <collection> (col1, col2, ...) VALUES (val1, val2, ...)
///
/// Intercepts INSERT for schemaless collections. Parses column names
/// and values manually, serializes as JSON, dispatches as PointPut.
/// Returns `None` if the collection has a typed schema (let DataFusion handle it).
pub async fn insert_document(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    let upper = sql.to_uppercase();
    let into_pos = upper.find("INSERT INTO ")?;
    let after_into = sql[into_pos + 12..].trim_start();
    let coll_name_str = after_into.split_whitespace().next()?;
    let coll_name_lower = coll_name_str.to_lowercase();
    let coll_name = coll_name_lower.as_str();

    // Check if collection is schemaless (no declared fields).
    let tenant_id = identity.tenant_id;
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(coll)) = catalog.get_collection(tenant_id.as_u32(), coll_name)
        && !coll.fields.is_empty()
    {
        // Typed collection — let DataFusion handle it.
        return None;
    }

    // Find the column list: first (...) in the SQL.
    let first_open = match sql.find('(') {
        Some(p) => p,
        None => {
            return Some(Err(sqlstate_error(
                "42601",
                "missing column list in INSERT",
            )));
        }
    };
    let values_kw = match upper.find("VALUES") {
        Some(p) => p,
        None => return Some(Err(sqlstate_error("42601", "missing VALUES clause"))),
    };
    let first_close = match sql[first_open..values_kw].rfind(')') {
        Some(p) => first_open + p,
        None => {
            return Some(Err(sqlstate_error(
                "42601",
                "missing closing ) for column list",
            )));
        }
    };
    let cols_str = &sql[first_open + 1..first_close];
    let columns: Vec<&str> = cols_str.split(',').map(|c| c.trim()).collect();

    // Find VALUES (...).
    let after_values = sql[values_kw + 6..].trim_start();
    let vals_open = match after_values.find('(') {
        Some(p) => p,
        None => return Some(Err(sqlstate_error("42601", "missing VALUES (...)"))),
    };
    let vals_close = match after_values.rfind(')') {
        Some(p) => p,
        None => return Some(Err(sqlstate_error("42601", "missing closing ) for VALUES"))),
    };
    let vals_str = &after_values[vals_open + 1..vals_close];
    let values: Vec<&str> = split_values(vals_str);

    if columns.len() != values.len() {
        return Some(Err(sqlstate_error(
            "42601",
            &format!(
                "column count ({}) doesn't match value count ({})",
                columns.len(),
                values.len()
            ),
        )));
    }

    // First column should be 'id'. Build JSON document from the rest.
    let mut doc_id = String::new();
    let mut fields = serde_json::Map::new();

    for (col, val) in columns.iter().zip(values.iter()) {
        let col = col.trim().trim_matches('"');
        let val = val.trim();
        if col.eq_ignore_ascii_case("id") {
            doc_id = val.trim_matches('\'').to_string();
        } else {
            let json_val = parse_sql_value(val);
            fields.insert(col.to_string(), json_val);
        }
    }

    if doc_id.is_empty() {
        // Auto-generate ID.
        doc_id = format!(
            "{:016x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
    }

    // Detect vector fields (ARRAY[...] values) and extract them for VectorInsert.
    let mut vector_fields: Vec<(String, Vec<f32>)> = Vec::new();
    for (col, val) in columns.iter().zip(values.iter()) {
        let col = col.trim().trim_matches('"');
        let val = val.trim();
        if let Some(vec_data) = parse_array_literal(val) {
            vector_fields.push((col.to_string(), vec_data));
        }
    }

    let value_bytes = serde_json::to_vec(&fields).unwrap_or_default();
    let vshard_id = crate::types::VShardId::from_key(doc_id.as_bytes());

    // 1. Store the document via PointPut.
    let plan = crate::bridge::envelope::PhysicalPlan::PointPut {
        collection: coll_name.to_string(),
        document_id: doc_id.clone(),
        value: value_bytes,
    };

    if let Err(e) = crate::control::server::dispatch_utils::wal_append_if_write(
        &state.wal, tenant_id, vshard_id, &plan,
    ) {
        return Some(Err(sqlstate_error("XX000", &e.to_string())));
    }

    if let Err(e) = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard_id, plan, 0,
    )
    .await
    {
        return Some(Err(sqlstate_error("XX000", &e.to_string())));
    }

    // 2. For each vector field, dispatch VectorInsert to HNSW index.
    let vec_vshard = crate::types::VShardId::from_collection(coll_name);
    for (_field_name, vector) in &vector_fields {
        let dim = vector.len();
        let vec_plan = crate::bridge::envelope::PhysicalPlan::VectorInsert {
            collection: coll_name.to_string(),
            vector: vector.clone(),
            dim,
            field_name: String::new(),
            doc_id: Some(doc_id.clone()),
        };

        if let Err(e) = crate::control::server::dispatch_utils::wal_append_if_write(
            &state.wal, tenant_id, vec_vshard, &vec_plan,
        ) {
            return Some(Err(sqlstate_error("XX000", &e.to_string())));
        }

        if let Err(e) = crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state, tenant_id, vec_vshard, vec_plan, 0,
        )
        .await
        {
            return Some(Err(sqlstate_error("XX000", &e.to_string())));
        }
    }

    Some(Ok(vec![Response::Execution(Tag::new("INSERT"))]))
}
