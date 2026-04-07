//! INSERT/UPSERT dispatch for schemaless collections.
//!
//! Intercepts INSERT/UPSERT for collections without typed schemas, parses
//! column names and values manually, serializes as JSON, and dispatches
//! as PointPut (INSERT) or Upsert (UPSERT) + optional VectorInsert.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;
use sonic_rs;

use crate::bridge::physical_plan::{DocumentOp, VectorOp};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::sqlstate_error;
use super::sql_parse::{parse_array_literal, parse_sql_value, split_values};

/// Parsed INSERT/UPSERT statement fields.
struct ParsedInsert {
    coll_name: String,
    doc_id: String,
    fields: std::collections::HashMap<String, nodedb_types::Value>,
    vector_fields: Vec<(String, Vec<f32>)>,
    value_bytes: Vec<u8>,
    has_returning: bool,
}

/// Parse an INSERT/UPSERT SQL statement into structured fields.
///
/// `keyword` is the SQL prefix to match (e.g., "INSERT INTO " or "UPSERT INTO ").
/// Returns `None` if the collection has a typed schema (let DataFusion handle it).
fn parse_write_statement(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    keyword: &str,
) -> Option<PgWireResult<ParsedInsert>> {
    let upper = sql.to_uppercase();
    let kw_pos = upper.find(keyword)?;
    let after_into = sql[kw_pos + keyword.len()..].trim_start();
    let coll_name_str = after_into.split_whitespace().next()?;
    let coll_name = coll_name_str.to_lowercase();

    // Check if collection is schemaless. Let DataFusion handle typed INSERT,
    // but UPSERT must always be handled here (DataFusion doesn't understand UPSERT).
    let tenant_id = identity.tenant_id;
    let is_upsert = keyword.starts_with("UPSERT");
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(coll)) = catalog.get_collection(tenant_id.as_u32(), &coll_name)
    {
        // Skip non-schemaless collections for INSERT (let DataFusion handle).
        // But always handle UPSERT here since sqlparser doesn't parse it.
        if !is_upsert && !coll.collection_type.is_schemaless() {
            return None;
        }
    }

    // Parse column list.
    let first_open = match sql.find('(') {
        Some(p) => p,
        None => {
            return Some(Err(sqlstate_error(
                "42601",
                &format!("missing column list in {}", keyword.trim()),
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

    // Parse VALUES (...).
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

    // Build document fields and extract doc_id.
    let mut doc_id = String::new();
    let mut fields = std::collections::HashMap::new();

    for (col, val) in columns.iter().zip(values.iter()) {
        let col = col.trim().trim_matches('"');
        let val = val.trim();
        if col.eq_ignore_ascii_case("id")
            || col.eq_ignore_ascii_case("document_id")
            || col.eq_ignore_ascii_case("key")
        {
            doc_id = val.trim_matches('\'').to_string();
        }
        // Always include in fields — strict collections need all columns in the value.
        fields.insert(col.to_string(), parse_sql_value(val));
    }

    if doc_id.is_empty() {
        doc_id = nodedb_types::id_gen::uuid_v7();
    }

    // Detect vector fields.
    let mut vector_fields: Vec<(String, Vec<f32>)> = Vec::new();
    for (col, val) in columns.iter().zip(values.iter()) {
        let col = col.trim().trim_matches('"');
        let val = val.trim();
        if let Some(vec_data) = parse_array_literal(val) {
            vector_fields.push((col.to_string(), vec_data));
        }
    }

    let value_bytes = nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(fields.clone()))
        .unwrap_or_default();
    let has_returning = upper.contains("RETURNING");

    Some(Ok(ParsedInsert {
        coll_name,
        doc_id,
        fields,
        vector_fields,
        value_bytes,
        has_returning,
    }))
}

/// Format a RETURNING response from parsed fields.
fn returning_response(
    doc_id: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> PgWireResult<Vec<Response>> {
    use futures::stream;
    use pgwire::api::results::{DataRowEncoder, QueryResponse};

    let mut result_doc = fields.clone();
    result_doc.insert(
        "id".to_string(),
        nodedb_types::Value::String(doc_id.to_string()),
    );
    let json_str =
        sonic_rs::to_string(&nodedb_types::Value::Object(result_doc)).unwrap_or_default();
    let schema = std::sync::Arc::new(vec![super::super::types::text_field("result")]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    let _ = encoder.encode_field(&json_str);
    let row = encoder.take_row();
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}

/// INSERT INTO <collection> (col1, col2, ...) VALUES (val1, val2, ...)
pub async fn insert_document(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    let parsed = match parse_write_statement(state, identity, sql, "INSERT INTO ")? {
        Ok(p) => p,
        Err(e) => return Some(Err(e)),
    };

    let tenant_id = identity.tenant_id;
    // Route by collection name so INSERT and subsequent PointGet/PointUpdate/PointDelete
    // all land on the same core. Routing by doc_id (from_key) would scatter documents
    // across cores while reads always route by collection (from_collection).
    let vshard_id = crate::types::VShardId::from_collection(&parsed.coll_name);

    // Fire INSTEAD OF INSERT triggers — if handled, skip normal dispatch.
    match crate::control::trigger::fire_instead::fire_instead_of_insert(
        state,
        identity,
        tenant_id,
        &parsed.coll_name,
        &parsed.fields,
        0,
    )
    .await
    {
        Ok(crate::control::trigger::fire_instead::InsteadOfResult::Handled) => {
            return Some(Ok(vec![Response::Execution(Tag::new("INSERT"))]));
        }
        Ok(crate::control::trigger::fire_instead::InsteadOfResult::NoTrigger) => {}
        Err(e) => return Some(Err(sqlstate_error("XX000", &format!("trigger error: {e}")))),
    }

    // Fire BEFORE INSERT triggers — may reject via RAISE EXCEPTION, may mutate NEW fields.
    let fields_after_before = match crate::control::trigger::fire_before::fire_before_insert(
        state,
        identity,
        tenant_id,
        &parsed.coll_name,
        &parsed.fields,
        0,
    )
    .await
    {
        Ok(f) => f,
        Err(e) => {
            return Some(Err(sqlstate_error(
                "XX000",
                &format!("BEFORE trigger error: {e}"),
            )));
        }
    };

    // Auto-generate sequence values for fields with sequence_name where the
    // INSERT didn't provide an explicit value. This implements column-level
    // SEQUENCE integration (e.g., `invoice_number STRING SEQUENCE FORMAT '...'`).
    let mut fields = fields_after_before;
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(coll_def)) = catalog.get_collection(tenant_id.as_u32(), &parsed.coll_name)
    {
        for field_def in &coll_def.field_defs {
            if let Some(ref seq_name) = field_def.sequence_name
                && !fields.contains_key(&field_def.name)
            {
                // Field not provided — generate via nextval.
                match state.sequence_registry.nextval_formatted(
                    tenant_id.as_u32(),
                    seq_name,
                    "",
                    &std::collections::HashMap::new(),
                ) {
                    Ok(val) => {
                        let typed_val = match val {
                            crate::control::sequence::registry::SequenceValue::Int(i) => {
                                nodedb_types::Value::Integer(i)
                            }
                            crate::control::sequence::registry::SequenceValue::Formatted(s) => {
                                nodedb_types::Value::String(s)
                            }
                        };
                        fields.insert(field_def.name.clone(), typed_val);
                    }
                    Err(e) => {
                        return Some(Err(sqlstate_error(
                            "XX000",
                            &format!("sequence '{seq_name}' error: {e}"),
                        )));
                    }
                }
            }
        }
    }

    // Rebuild value bytes (sequence injection or BEFORE trigger may have mutated fields).
    let value_bytes = if fields != parsed.fields {
        nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(fields.clone()))
            .unwrap_or(parsed.value_bytes)
    } else {
        parsed.value_bytes
    };

    // Store document via PointPut.
    let plan = crate::bridge::envelope::PhysicalPlan::Document(DocumentOp::PointPut {
        collection: parsed.coll_name.clone(),
        document_id: parsed.doc_id.clone(),
        value: value_bytes,
    });

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

    // Track field names in catalog so DataFusion can resolve them in queries.
    // This makes schemaless fields visible for WHERE, GROUP BY, ORDER BY, etc.
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(mut coll)) = catalog.get_collection(tenant_id.as_u32(), &parsed.coll_name)
    {
        let mut changed = false;
        for (name, val) in &fields {
            if name == "id" {
                continue;
            }
            if !coll.fields.iter().any(|(n, _)| n == name) {
                let type_str = match val {
                    nodedb_types::Value::Float(_) => "FLOAT",
                    nodedb_types::Value::Integer(_) => "INT",
                    nodedb_types::Value::Bool(_) => "BOOL",
                    _ => "TEXT",
                };
                coll.fields.push((name.clone(), type_str.to_string()));
                changed = true;
            }
        }
        if changed {
            let _ = catalog.put_collection(&coll);
        }
    }

    // Fire SYNC AFTER INSERT triggers (execute in write path, same transaction).
    // ASYNC triggers are handled by the Event Plane via WriteEvent dispatch.
    use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
    if let Err(e) = crate::control::trigger::fire::fire_after_insert(
        state,
        identity,
        tenant_id,
        &parsed.coll_name,
        &fields,
        0,
        Some(TriggerExecutionMode::Sync),
    )
    .await
    {
        return Some(Err(sqlstate_error("XX000", &format!("trigger error: {e}"))));
    }

    // Dispatch VectorInsert for vector fields.
    let vec_vshard = crate::types::VShardId::from_collection(&parsed.coll_name);
    for (field_name, vector) in &parsed.vector_fields {
        let dim = vector.len();

        // Enforce strict_dimensions if model metadata is set.
        if let Some(catalog) = state.credentials.catalog() {
            let col = if field_name.is_empty() {
                "embedding"
            } else {
                field_name.as_str()
            };
            if let Ok(Some(entry)) =
                catalog.get_vector_model(tenant_id.as_u32(), &parsed.coll_name, col)
                && entry.metadata.strict_dimensions
                && entry.metadata.dimensions != dim
            {
                return Some(Err(sqlstate_error(
                    "23514",
                    &format!(
                        "strict_dimensions: vector has {} dimensions, model '{}' requires {}",
                        dim, entry.metadata.model, entry.metadata.dimensions
                    ),
                )));
            }
        }
        let vec_plan = crate::bridge::envelope::PhysicalPlan::Vector(VectorOp::Insert {
            collection: parsed.coll_name.clone(),
            vector: vector.clone(),
            dim,
            field_name: String::new(),
            doc_id: Some(parsed.doc_id.clone()),
        });

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

    if parsed.has_returning {
        return Some(returning_response(&parsed.doc_id, &fields));
    }

    Some(Ok(vec![Response::Execution(Tag::new("INSERT"))]))
}

/// UPSERT INTO <collection> (col1, col2, ...) VALUES (val1, val2, ...)
///
/// Same parsing as INSERT but dispatches the `Upsert` plan variant:
/// if a document with the given ID exists, its fields are merged.
pub async fn upsert_document(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    let parsed = match parse_write_statement(state, identity, sql, "UPSERT INTO ")? {
        Ok(p) => p,
        Err(e) => return Some(Err(e)),
    };

    let tenant_id = identity.tenant_id;
    let vshard_id = crate::types::VShardId::from_collection(&parsed.coll_name);

    // Fire INSTEAD OF INSERT triggers (upsert treated as INSERT for triggers).
    match crate::control::trigger::fire_instead::fire_instead_of_insert(
        state,
        identity,
        tenant_id,
        &parsed.coll_name,
        &parsed.fields,
        0,
    )
    .await
    {
        Ok(crate::control::trigger::fire_instead::InsteadOfResult::Handled) => {
            return Some(Ok(vec![Response::Execution(Tag::new("UPSERT"))]));
        }
        Ok(crate::control::trigger::fire_instead::InsteadOfResult::NoTrigger) => {}
        Err(e) => return Some(Err(sqlstate_error("XX000", &format!("trigger error: {e}")))),
    }

    // Fire BEFORE INSERT triggers — may mutate NEW fields.
    let fields = match crate::control::trigger::fire_before::fire_before_insert(
        state,
        identity,
        tenant_id,
        &parsed.coll_name,
        &parsed.fields,
        0,
    )
    .await
    {
        Ok(f) => f,
        Err(e) => {
            return Some(Err(sqlstate_error(
                "XX000",
                &format!("BEFORE trigger error: {e}"),
            )));
        }
    };

    // Rebuild value bytes if BEFORE trigger mutated NEW fields.
    let value_bytes = if fields != parsed.fields {
        nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(fields.clone()))
            .unwrap_or(parsed.value_bytes)
    } else {
        parsed.value_bytes
    };

    let plan = crate::bridge::envelope::PhysicalPlan::Document(DocumentOp::Upsert {
        collection: parsed.coll_name.clone(),
        document_id: parsed.doc_id.clone(),
        value: value_bytes,
    });

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

    // Fire SYNC AFTER INSERT triggers.
    use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
    if let Err(e) = crate::control::trigger::fire::fire_after_insert(
        state,
        identity,
        tenant_id,
        &parsed.coll_name,
        &fields,
        0,
        Some(TriggerExecutionMode::Sync),
    )
    .await
    {
        return Some(Err(sqlstate_error("XX000", &format!("trigger error: {e}"))));
    }

    Some(Ok(vec![Response::Execution(Tag::new("UPSERT"))]))
}
