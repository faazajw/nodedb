//! `RESOLVE_PERMISSION(user_id, resource_id, collection)` SQL scalar function.
//!
//! Walks the permission tree hierarchy for the given resource and returns
//! the effective permission level for the specified user. Useful for
//! debugging and manual permission checks.
//!
//! ```sql
//! SELECT RESOLVE_PERMISSION('user-42', 'doc-123', 'documents');
//! -- Returns: 'editor'
//! ```

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::control::security::permission_tree::{PermissionCache, resolver};

/// `RESOLVE_PERMISSION(user_id, resource_id, collection)` — resolve effective permission.
///
/// Arguments:
/// 1. `user_id` (STRING) — the user to check
/// 2. `resource_id` (STRING) — the resource to check access on
/// 3. `collection` (STRING) — the collection with the permission tree def
///
/// Returns: STRING — the effective permission level name (e.g., "editor", "viewer", "none")
///
/// The function holds an `Arc<tokio::sync::RwLock<PermissionCache>>` for in-memory
/// lookups. Since DataFusion UDFs are synchronous, we use `try_read()` to avoid
/// blocking the async runtime. If the lock can't be acquired, returns "unknown".
#[derive(Debug)]
pub struct ResolvePermission {
    signature: Signature,
    cache: Arc<tokio::sync::RwLock<PermissionCache>>,
    /// Tenant ID for this session (set at planning time).
    tenant_id: u32,
    /// User roles for the current session (for role-based grant matching).
    user_roles: Vec<String>,
}

impl ResolvePermission {
    pub fn new(
        cache: Arc<tokio::sync::RwLock<PermissionCache>>,
        tenant_id: u32,
        user_roles: Vec<String>,
    ) -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8, DataType::Utf8],
                Volatility::Stable,
            ),
            cache,
            tenant_id,
            user_roles,
        }
    }
}

impl std::hash::Hash for ResolvePermission {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.tenant_id.hash(state);
    }
}

impl PartialEq for ResolvePermission {
    fn eq(&self, other: &Self) -> bool {
        self.tenant_id == other.tenant_id
    }
}

impl Eq for ResolvePermission {}

impl ScalarUDFImpl for ResolvePermission {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "resolve_permission"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DfResult<ColumnarValue> {
        let num_rows = args.number_rows;
        let user_ids = extract_string_array(&args.args[0], num_rows)?;
        let resource_ids = extract_string_array(&args.args[1], num_rows)?;
        let collections = extract_string_array(&args.args[2], num_rows)?;

        // Non-blocking read lock — avoids blocking the Tokio runtime.
        let guard = match self.cache.try_read() {
            Ok(g) => g,
            Err(_) => {
                // Lock contention — return "unknown" for all rows.
                let arr = StringArray::from(vec!["unknown"; num_rows]);
                return Ok(ColumnarValue::Array(Arc::new(arr)));
            }
        };

        let mut results: Vec<String> = Vec::with_capacity(num_rows);

        for i in 0..num_rows {
            let user_id = user_ids.value(i);
            let resource_id = resource_ids.value(i);
            let collection = collections.value(i);

            let level = match guard.get_tree_def(self.tenant_id, collection) {
                Some(def) => resolver::resolve_permission(
                    &guard,
                    def,
                    self.tenant_id,
                    user_id,
                    &self.user_roles,
                    resource_id,
                ),
                None => "no_permission_tree".to_owned(),
            };
            results.push(level);
        }

        Ok(ColumnarValue::Array(Arc::new(StringArray::from(results))))
    }
}

/// Extract a StringArray from a ColumnarValue, expanding scalars.
fn extract_string_array(val: &ColumnarValue, num_rows: usize) -> DfResult<StringArray> {
    match val {
        ColumnarValue::Array(arr) => {
            let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "RESOLVE_PERMISSION: expected string argument".into(),
                )
            })?;
            Ok(str_arr.clone())
        }
        ColumnarValue::Scalar(scalar) => {
            let s = scalar.to_string();
            let repeated: Vec<&str> = (0..num_rows).map(|_| s.as_str()).collect();
            Ok(StringArray::from(repeated))
        }
    }
}
