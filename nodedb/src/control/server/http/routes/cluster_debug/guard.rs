//! Shared access gate + JSON response helper for the
//! `/cluster/debug/*` endpoints.

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use super::super::super::auth::{AppState, ResolvedIdentity};

/// Enforce both the superuser role and the
/// `observability.debug_endpoints_enabled` config flag.
///
/// Returns `None` when the caller is allowed. Returns `Some(Response)`
/// pre-built with the correct status when the caller must be refused,
/// letting the handler early-return with a single `?`-style check.
///
/// The flag check is deliberately before the superuser check so a
/// production deployment that left the flag off returns 404 for every
/// caller (including unauthenticated probes) — the endpoints behave as
/// if they don't exist, which is the usual ops hardening expectation.
pub fn ensure_debug_access(state: &AppState, identity: &ResolvedIdentity) -> Option<Response> {
    if !state.shared.debug_endpoints_enabled {
        return Some(json_response(
            StatusCode::NOT_FOUND,
            r#"{"error":"not found"}"#.to_string(),
        ));
    }
    if !identity.0.is_superuser {
        return Some(json_response(
            StatusCode::FORBIDDEN,
            r#"{"error":"superuser required for /cluster/debug/*"}"#.to_string(),
        ));
    }
    None
}

/// Build a JSON response with the given status and pre-serialised
/// body. Mirrors the pattern used by `/cluster/status` so hot-path
/// serialisation stays on `sonic_rs`.
pub fn json_response(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// Serialise `value` with `sonic_rs` and wrap in a 200 response.
/// On serialisation failure returns a 500 with a short JSON error
/// body — the only realistic failure mode for in-memory snapshots is
/// a non-UTF8 key, which would indicate corrupted memory, not a
/// legitimate caller error.
pub fn ok_json<T: serde::Serialize>(value: &T) -> Response {
    match sonic_rs::to_string(value) {
        Ok(body) => json_response(StatusCode::OK, body),
        Err(e) => {
            tracing::warn!(error = %e, "cluster/debug: snapshot serialization failed");
            json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"error":"snapshot serialization failed"}"#.to_string(),
            )
        }
    }
}

/// 503 response used when the cluster subsystem required by a handler
/// is absent (single-node mode). Kept in one place so every endpoint
/// returns the same shape for "feature not wired on this node".
pub fn cluster_disabled() -> Response {
    json_response(
        StatusCode::SERVICE_UNAVAILABLE,
        r#"{"error":"cluster mode not enabled"}"#.to_string(),
    )
}
