//! POST `/obsv/api/v1/annotations` — Grafana annotation query.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use sonic_rs;

use crate::control::promql;
use crate::control::server::http::auth::{AppState, ResolvedIdentity};

use crate::control::server::http::routes::promql::helpers::{fetch_series_for_query, now_ms};

pub async fn annotations(
    _identity: ResolvedIdentity,
    State(state): State<AppState>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> impl IntoResponse {
    let query = body
        .pointer("/annotation/query")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let from_ms = body
        .pointer("/range/from")
        .and_then(|v| v.as_str())
        .and_then(parse_iso_ms)
        .unwrap_or(0);
    let to_ms = body
        .pointer("/range/to")
        .and_then(|v| v.as_str())
        .and_then(parse_iso_ms)
        .unwrap_or(now_ms());

    if query.is_empty() {
        return (
            StatusCode::OK,
            [("content-type", "application/json")],
            "[]".to_string(),
        );
    }

    let tokens = match promql::lexer::tokenize(query) {
        Ok(t) => t,
        Err(e) => {
            tracing::debug!(error = %e, query, "annotation query tokenize failed");
            return (
                StatusCode::OK,
                [("content-type", "application/json")],
                "[]".to_string(),
            );
        }
    };
    let expr = match promql::parse(&tokens) {
        Ok(e) => e,
        Err(e) => {
            tracing::debug!(error = %e, query, "annotation query parse failed");
            return (
                StatusCode::OK,
                [("content-type", "application/json")],
                "[]".to_string(),
            );
        }
    };

    let series =
        fetch_series_for_query(&state, from_ms - promql::types::DEFAULT_LOOKBACK_MS, to_ms).await;
    let ctx = promql::EvalContext {
        series,
        timestamp_ms: to_ms,
        lookback_ms: promql::types::DEFAULT_LOOKBACK_MS,
    };

    // Step every 60s across the range.
    // Annotations use 60s step — coarser granularity is appropriate for event markers.
    const ANNOTATION_STEP_MS: i64 = 60_000;
    let step_ms = ANNOTATION_STEP_MS;
    let val = promql::evaluate_range(&ctx, &expr, from_ms, to_ms, step_ms);

    let mut result_annotations: Vec<serde_json::Value> = Vec::new();
    if let Ok(promql::Value::Matrix(matrix)) = val {
        for rs in &matrix {
            let title = rs
                .labels
                .get("__name__")
                .cloned()
                .unwrap_or_else(|| "annotation".into());
            let tags: Vec<String> = rs
                .labels
                .iter()
                .filter(|(k, _)| k.as_str() != "__name__")
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            for sample in &rs.samples {
                if sample.value != 0.0 && !sample.value.is_nan() {
                    result_annotations.push(serde_json::json!({
                        "time": sample.timestamp_ms,
                        "title": title,
                        "text": format!("value={}", sample.value),
                        "tags": tags,
                    }));
                }
            }
        }
    }

    (
        StatusCode::OK,
        [("content-type", "application/json")],
        sonic_rs::to_string(&result_annotations).unwrap_or_else(|_| "[]".into()),
    )
}

/// Parse a timestamp as epoch milliseconds or ISO 8601 (RFC 3339).
fn parse_iso_ms(s: &str) -> Option<i64> {
    if let Ok(ms) = s.parse::<i64>() {
        return Some(ms);
    }
    if let Ok(secs) = s.parse::<f64>() {
        return Some((secs * 1000.0) as i64);
    }
    let s = s.trim();
    let (date_part, time_part) = s.split_once('T')?;
    let date_parts: Vec<&str> = date_part.split('-').collect();
    if date_parts.len() != 3 {
        return None;
    }
    let year: i64 = date_parts[0].parse().ok()?;
    let month: i64 = date_parts[1].parse().ok()?;
    let day: i64 = date_parts[2].parse().ok()?;

    let time_clean = time_part
        .trim_end_matches('Z')
        .split('+')
        .next()
        .unwrap_or(time_part);
    let time_parts: Vec<&str> = time_clean.split(':').collect();
    if time_parts.len() < 2 {
        return None;
    }
    let hour: i64 = time_parts[0].parse().ok()?;
    let min: i64 = time_parts[1].parse().ok()?;
    let sec_frac: f64 = time_parts
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);

    let mut days = (year - 1970) * 365 + (year - 1969) / 4;
    let month_days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    if (1..=12).contains(&month) {
        days += month_days[(month - 1) as usize];
    }
    if month > 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
        days += 1;
    }
    days += day - 1;

    let total_ms = days * 86_400_000 + hour * 3_600_000 + min * 60_000 + (sec_frac * 1000.0) as i64;
    Some(total_ms)
}
