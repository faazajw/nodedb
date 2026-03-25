//! DataFusion `TableProvider` backed by Loro CRDT documents.
//!
//! Provides full SQL query capability over Lite's document store.
//! Documents are scanned from the in-memory Loro state, converted
//! to Arrow RecordBatches, and fed into DataFusion's execution engine.

use std::any::Any;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::engine::crdt::CrdtEngine;

/// A DataFusion `TableProvider` that reads documents from a Loro collection.
///
/// Each document becomes a row. All fields are stored as JSON strings
/// in a schemaless layout: `(id TEXT, document TEXT)`. DataFusion's
/// JSON functions can extract fields for WHERE/ORDER BY/GROUP BY.
///
/// For typed collections (with known fields), a richer Arrow schema
/// is generated with proper column types.
pub struct LiteTableProvider {
    collection: String,
    schema: SchemaRef,
    crdt: Arc<Mutex<CrdtEngine>>,
}

impl std::fmt::Debug for LiteTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiteTableProvider")
            .field("collection", &self.collection)
            .finish()
    }
}

impl LiteTableProvider {
    /// Create a table provider for a schemaless collection.
    ///
    /// Schema: `(id TEXT NOT NULL, document TEXT)` — all fields stored
    /// as a JSON blob in the `document` column. DataFusion JSON functions
    /// extract individual fields.
    pub fn new(collection: String, crdt: Arc<Mutex<CrdtEngine>>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("document", DataType::Utf8, true),
        ]));
        Self {
            collection,
            schema,
            crdt,
        }
    }

    /// Create a table provider with a known schema (typed collection).
    pub fn with_schema(
        collection: String,
        schema: SchemaRef,
        crdt: Arc<Mutex<CrdtEngine>>,
    ) -> Self {
        Self {
            collection,
            schema,
            crdt,
        }
    }

    /// Scan documents from the Loro collection into Arrow RecordBatches.
    ///
    /// `limit` is pushed down from DataFusion — if set, only reads that
    /// many documents instead of the entire collection.
    fn scan_to_batches(&self, limit: Option<usize>) -> Result<Vec<RecordBatch>, DataFusionError> {
        let crdt = self
            .crdt
            .lock()
            .map_err(|e| DataFusionError::Execution(format!("crdt lock: {e}")))?;

        let mut ids = crdt.list_ids(&self.collection);
        // Apply limit pushdown: don't load more documents than needed.
        if let Some(n) = limit {
            ids.truncate(n);
        }
        if ids.is_empty() {
            let batch = RecordBatch::new_empty(self.schema.clone());
            return Ok(vec![batch]);
        }

        // For schemaless: serialize each document as JSON.
        if self.schema.fields().len() == 2
            && self.schema.field(0).name() == "id"
            && self.schema.field(1).name() == "document"
        {
            return self.scan_schemaless(&crdt, &ids);
        }

        // For typed: extract fields into typed columns.
        self.scan_typed(&crdt, &ids)
    }

    /// Schemaless scan: (id, document_json) pairs.
    fn scan_schemaless(
        &self,
        crdt: &CrdtEngine,
        ids: &[String],
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let mut id_values = Vec::with_capacity(ids.len());
        let mut doc_values = Vec::with_capacity(ids.len());

        for id in ids {
            if let Some(loro_val) = crdt.read(&self.collection, id) {
                let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                let json = serde_json::to_string(&doc.fields).unwrap_or_else(|e| {
                    tracing::warn!(id = %id, error = %e, "JSON serialization failed for document");
                    "{}".to_string()
                });
                id_values.push(id.clone());
                doc_values.push(json);
            }
        }

        let id_array = StringArray::from(id_values);
        let doc_array = StringArray::from(doc_values);
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(id_array), Arc::new(doc_array)],
        )
        .map_err(|e| DataFusionError::Execution(format!("build batch: {e}")))?;

        Ok(vec![batch])
    }

    /// Typed scan: extract known fields into proper Arrow columns.
    fn scan_typed(
        &self,
        crdt: &CrdtEngine,
        ids: &[String],
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        use datafusion::arrow::array::{BooleanArray, Float64Array, Int64Array};

        let field_count = self.schema.fields().len();
        let mut columns: Vec<Vec<Option<nodedb_types::Value>>> =
            vec![Vec::with_capacity(ids.len()); field_count];

        for id in ids {
            if let Some(loro_val) = crdt.read(&self.collection, id) {
                let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                for (i, field) in self.schema.fields().iter().enumerate() {
                    let val = if field.name() == "id" {
                        Some(nodedb_types::Value::String(id.clone()))
                    } else {
                        doc.fields.get(field.name()).cloned()
                    };
                    columns[i].push(val);
                }
            }
        }

        // Build Arrow arrays from Value columns.
        let mut arrow_columns: Vec<Arc<dyn datafusion::arrow::array::Array>> =
            Vec::with_capacity(field_count);

        for (i, field) in self.schema.fields().iter().enumerate() {
            let col = &columns[i];
            let array: Arc<dyn datafusion::arrow::array::Array> = match field.data_type() {
                DataType::Utf8 => {
                    let vals: Vec<Option<String>> = col
                        .iter()
                        .map(|v| match v {
                            Some(nodedb_types::Value::String(s)) => Some(s.clone()),
                            Some(other) => Some(format!("{other:?}")),
                            None => None,
                        })
                        .collect();
                    Arc::new(StringArray::from(vals))
                }
                DataType::Int64 => {
                    let vals: Vec<Option<i64>> = col
                        .iter()
                        .map(|v| match v {
                            Some(nodedb_types::Value::Integer(i)) => Some(*i),
                            Some(nodedb_types::Value::Float(f)) => Some(*f as i64),
                            _ => None,
                        })
                        .collect();
                    Arc::new(Int64Array::from(vals))
                }
                DataType::Float64 => {
                    let vals: Vec<Option<f64>> = col
                        .iter()
                        .map(|v| match v {
                            Some(nodedb_types::Value::Float(f)) => Some(*f),
                            Some(nodedb_types::Value::Integer(i)) => Some(*i as f64),
                            _ => None,
                        })
                        .collect();
                    Arc::new(Float64Array::from(vals))
                }
                DataType::Boolean => {
                    let vals: Vec<Option<bool>> = col
                        .iter()
                        .map(|v| match v {
                            Some(nodedb_types::Value::Bool(b)) => Some(*b),
                            _ => None,
                        })
                        .collect();
                    Arc::new(BooleanArray::from(vals))
                }
                _ => {
                    // Fallback: serialize as string.
                    let vals: Vec<Option<String>> = col
                        .iter()
                        .map(|v| v.as_ref().map(|v| format!("{v:?}")))
                        .collect();
                    Arc::new(StringArray::from(vals))
                }
            };
            arrow_columns.push(array);
        }

        let batch = RecordBatch::try_new(self.schema.clone(), arrow_columns)
            .map_err(|e| DataFusionError::Execution(format!("build typed batch: {e}")))?;

        Ok(vec![batch])
    }
}

#[async_trait]
impl TableProvider for LiteTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let batches = self.scan_to_batches(limit)?;
        // Use MemTable to create a physical plan from in-memory batches.
        let mem_table =
            datafusion::datasource::MemTable::try_new(self.schema.clone(), vec![batches])?;
        mem_table.scan(state, projection, &[], limit).await
    }
}
