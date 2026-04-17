//! `/obsv/api/v1/status/buildinfo` — Grafana data source health check (no auth required).

use axum::http::StatusCode;
use axum::response::IntoResponse;

/// GET `/obsv/api/v1/status/buildinfo`.
pub async fn buildinfo() -> impl IntoResponse {
    let out = format!(
        r#"{{"status":"success","data":{{"version":"{}","revision":"nodedb","branch":"main","buildDate":"","goVersion":"","buildUser":""}}}}"#,
        env!("CARGO_PKG_VERSION")
    );
    (StatusCode::OK, [("content-type", "application/json")], out)
}
