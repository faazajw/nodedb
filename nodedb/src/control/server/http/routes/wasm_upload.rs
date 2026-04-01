//! HTTP endpoint: `PUT /v1/functions/{name}/wasm`
//!
//! Accepts a raw WASM binary as the request body, stores it content-addressed,
//! and updates the function's `wasm_hash` in the catalog.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;

use crate::control::planner::wasm;
use crate::control::state::SharedState;

/// `PUT /v1/functions/:name/wasm` — upload a WASM binary for a function.
///
/// The function must already exist with `language = WASM` in the catalog.
/// The binary replaces the previous one (if any) atomically.
pub async fn upload_wasm(
    State(state): State<Arc<SharedState>>,
    Path(name): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    let name = name.to_lowercase();

    let catalog = match state.credentials.catalog() {
        Some(c) => c,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "system catalog not available".to_string(),
            );
        }
    };

    // Verify the function exists and is a WASM function.
    // Use tenant 0 for now — multi-tenant HTTP auth is handled by middleware.
    let tenant_id = 0u32;
    let mut func = match catalog.get_function(tenant_id, &name) {
        Ok(Some(f)) => f,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                format!("function '{name}' does not exist"),
            );
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("catalog read error: {e}"),
            );
        }
    };

    if func.language != crate::control::security::catalog::function_types::FunctionLanguage::Wasm {
        return (
            StatusCode::BAD_REQUEST,
            format!("function '{name}' is not a WASM function"),
        );
    }

    // Store the binary.
    let config = wasm::WasmConfig::default();
    let hash = match wasm::store::store_wasm_binary(catalog, &body, config.max_binary_size) {
        Ok(h) => h,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("invalid WASM binary: {e}"));
        }
    };

    // Update function metadata.
    func.wasm_hash = Some(hash.clone());
    if let Err(e) = catalog.put_function(&func) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("catalog write error: {e}"),
        );
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        None,
        "_http_upload",
        &format!("WASM binary uploaded for function '{name}' (hash: {hash})"),
    );

    (
        StatusCode::OK,
        serde_json::json!({"hash": hash}).to_string(),
    )
}
