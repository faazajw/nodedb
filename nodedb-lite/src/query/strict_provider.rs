//! DataFusion `TableProvider` for strict document collections.
//!
//! Reads Binary Tuples from the StrictEngine, extracts columns into Arrow
//! arrays via `nodedb-strict`'s vectorized extraction, and feeds them to
//! DataFusion as RecordBatches with projection pushdown.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use nodedb_strict::TupleDecoder;
use nodedb_strict::arrow_extract::extract_column_to_arrow;
use nodedb_types::Namespace;
use nodedb_types::columnar::StrictSchema;

use crate::engine::strict::strict_schema_to_arrow;
use crate::storage::engine::StorageEngine;

/// A DataFusion `TableProvider` that reads from a strict document collection.
///
/// Supports column projection pushdown: only the requested columns are
/// decoded from the Binary Tuples. Unneeded columns are never touched.
pub struct StrictTableProvider<S: StorageEngine> {
    collection: String,
    arrow_schema: SchemaRef,
    strict_schema: StrictSchema,
    decoder: TupleDecoder,
    storage: Arc<S>,
}

impl<S: StorageEngine> std::fmt::Debug for StrictTableProvider<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StrictTableProvider")
            .field("collection", &self.collection)
            .finish()
    }
}

impl<S: StorageEngine> StrictTableProvider<S> {
    /// Create a table provider for a strict collection.
    pub fn new(collection: String, schema: &StrictSchema, storage: Arc<S>) -> Self {
        let arrow_schema = strict_schema_to_arrow(schema);
        let decoder = TupleDecoder::new(schema);
        Self {
            collection,
            arrow_schema,
            strict_schema: schema.clone(),
            decoder,
            storage,
        }
    }

    /// Scan tuples and build Arrow RecordBatches.
    ///
    /// `projection` specifies which column indices to decode. If `None`, all
    /// columns are decoded. `limit` caps the number of rows.
    fn scan_to_batches(
        &self,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        // Read raw tuples from storage.
        let prefix = format!("{}:", self.collection);

        // StorageEngine is async, but scan_to_batches is sync (called from
        // DataFusion's async scan). We use tokio::runtime::Handle to block.
        let tuples = tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async {
                self.storage
                    .scan_prefix(Namespace::Strict, prefix.as_bytes())
                    .await
            })
        })
        .map_err(|e| DataFusionError::Execution(format!("storage scan: {e}")))?;

        // Apply limit.
        let tuple_bytes: Vec<Vec<u8>> = if let Some(n) = limit {
            tuples.into_iter().take(n).map(|(_, v)| v).collect()
        } else {
            tuples.into_iter().map(|(_, v)| v).collect()
        };

        if tuple_bytes.is_empty() {
            let batch = RecordBatch::new_empty(self.arrow_schema.clone());
            return Ok(vec![batch]);
        }

        let refs: Vec<&[u8]> = tuple_bytes.iter().map(|t| t.as_slice()).collect();

        // Determine which columns to extract.
        let col_indices: Vec<usize> = match projection {
            Some(proj) => proj.to_vec(),
            None => (0..self.strict_schema.columns.len()).collect(),
        };

        // Build the projected Arrow schema.
        let projected_schema = if projection.is_some() {
            Arc::new(
                self.arrow_schema
                    .project(&col_indices)
                    .map_err(|e| DataFusionError::Execution(format!("schema projection: {e}")))?,
            )
        } else {
            self.arrow_schema.clone()
        };

        // Extract each column into an Arrow array.
        let mut arrays = Vec::with_capacity(col_indices.len());
        for &idx in &col_indices {
            let arr = extract_column_to_arrow(&self.strict_schema, &self.decoder, &refs, idx)
                .map_err(|e| DataFusionError::Execution(format!("extract column: {e}")))?;
            arrays.push(arr);
        }

        let batch = RecordBatch::try_new(projected_schema, arrays)
            .map_err(|e| DataFusionError::Execution(format!("build batch: {e}")))?;

        Ok(vec![batch])
    }
}

#[async_trait]
impl<S: StorageEngine> TableProvider for StrictTableProvider<S> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.arrow_schema)
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
        let batches = self.scan_to_batches(projection, limit)?;
        let schema = if let Some(proj) = projection {
            Arc::new(self.arrow_schema.project(proj)?)
        } else {
            self.arrow_schema.clone()
        };
        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![batches])?;
        mem_table.scan(state, None, &[], limit).await
    }
}
