//! Opaque session handle HTTP endpoint.
//!
//! ```text
//! POST /api/auth/session
//! Authorization: Bearer <jwt-or-api-key>
//!
//! Response: { "session_id": "nds_...", "expires_in": 3600 }
//! ```
//!
//! The returned `session_id` can be used with pgwire connection poolers:
//! ```text
//! SET LOCAL nodedb.auth_session = 'nds_...';
//! SELECT * FROM orders;  -- Uses the cached AuthContext from the session handle
//! ```

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::IntoResponse;

use super::super::auth::{ApiError, AppState, resolve_auth};

/// `POST /api/auth/session` — Create an opaque session handle.
///
/// Validates the bearer token (JWT or API key), creates a server-side
/// cached `AuthContext`, and returns a UUID handle the client can use
/// with `SET LOCAL nodedb.auth_session = '<handle>'` on pgwire connections.
pub async fn create_session(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let (_identity, auth_ctx) = resolve_auth(&headers, &state, "http")?;

    let handle = state.shared.session_handles.create(auth_ctx);

    Ok(axum::Json(serde_json::json!({
        "session_id": handle,
        "expires_in": 3600,
    })))
}

/// `DELETE /api/auth/session` — Invalidate a session handle.
///
/// ```text
/// DELETE /api/auth/session
/// X-Session-Id: nds_...
/// ```
pub async fn delete_session(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let handle = headers
        .get("x-session-id")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ApiError::BadRequest("missing X-Session-Id header".into()))?;

    let found = state.shared.session_handles.invalidate(handle);
    if !found {
        return Err(ApiError::BadRequest("session handle not found".into()));
    }

    Ok(axum::Json(serde_json::json!({ "status": "ok" })))
}
