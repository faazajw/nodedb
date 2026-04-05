//! PromQL HTTP handler functions.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use sonic_rs;

use crate::control::promql;
use crate::control::server::http::auth::AppState;

use super::helpers::*;
use super::*;

/// GET/POST `/obsv/api/v1/query` — instant query.
pub async fn instant_query(
    State(state): State<AppState>,
    Query(params): Query<InstantQueryParams>,
) -> impl IntoResponse {
    let ts_ms = params.time.map(|t| (t * 1000.0) as i64).unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    });

    let tokens = match promql::lexer::tokenize(&params.query) {
        Ok(t) => t,
        Err(e) => return prom_error("bad_data", &e),
    };
    let expr = match promql::parse(&tokens) {
        Ok(e) => e,
        Err(e) => return prom_error("bad_data", &e),
    };

    let series =
        fetch_series_for_query(&state, ts_ms - promql::types::DEFAULT_LOOKBACK_MS, ts_ms).await;

    let ctx = promql::EvalContext {
        series,
        timestamp_ms: ts_ms,
        lookback_ms: promql::types::DEFAULT_LOOKBACK_MS,
    };

    match promql::evaluate_instant(&ctx, &expr) {
        Ok(value) => prom_success(value),
        Err(e) => prom_error("execution", &e),
    }
}

/// GET/POST `/obsv/api/v1/query_range` — range query.
pub async fn range_query(
    State(state): State<AppState>,
    Query(params): Query<RangeQueryParams>,
) -> impl IntoResponse {
    let start_ms = (params.start * 1000.0) as i64;
    let end_ms = (params.end * 1000.0) as i64;
    let step_ms = parse_step(&params.step).unwrap_or(15_000);

    if step_ms <= 0 {
        return prom_error("bad_data", "step must be positive");
    }
    if end_ms < start_ms {
        return prom_error("bad_data", "end must be >= start");
    }

    let tokens = match promql::lexer::tokenize(&params.query) {
        Ok(t) => t,
        Err(e) => return prom_error("bad_data", &e),
    };
    let expr = match promql::parse(&tokens) {
        Ok(e) => e,
        Err(e) => return prom_error("bad_data", &e),
    };

    let series = fetch_series_for_query(
        &state,
        start_ms - promql::types::DEFAULT_LOOKBACK_MS,
        end_ms,
    )
    .await;

    let ctx = promql::EvalContext {
        series,
        timestamp_ms: start_ms,
        lookback_ms: promql::types::DEFAULT_LOOKBACK_MS,
    };

    match promql::evaluate_range(&ctx, &expr, start_ms, end_ms, step_ms) {
        Ok(value) => prom_success(value),
        Err(e) => prom_error("execution", &e),
    }
}

/// GET `/obsv/api/v1/series` — find series by label matchers.
pub async fn series_query(
    State(state): State<AppState>,
    Query(params): Query<SeriesParams>,
) -> impl IntoResponse {
    let end_ms = params.end.map(|t| (t * 1000.0) as i64).unwrap_or(now_ms());
    let start_ms = params
        .start
        .map(|t| (t * 1000.0) as i64)
        .unwrap_or(end_ms - promql::types::DEFAULT_LOOKBACK_MS);

    let all_series = fetch_series_for_query(&state, start_ms, end_ms).await;

    let filtered: Vec<&promql::Series> = if params.matchers.is_empty() {
        all_series.iter().collect()
    } else {
        all_series
            .iter()
            .filter(|s| {
                params
                    .matchers
                    .iter()
                    .any(|m| match parse_series_matcher(m) {
                        Some(matchers) => promql::label::matches_all(&matchers, &s.labels),
                        None => false,
                    })
            })
            .collect()
    };

    let mut out = String::from(r#"{"status":"success","data":["#);
    for (i, s) in filtered.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        promql::types::write_labels_json(&mut out, &s.labels);
    }
    out.push_str("]}");

    (StatusCode::OK, [("content-type", "application/json")], out)
}

/// GET `/obsv/api/v1/labels` — list all label names.
pub async fn label_names(
    State(state): State<AppState>,
    Query(params): Query<LabelsParams>,
) -> impl IntoResponse {
    let end_ms = params.end.map(|t| (t * 1000.0) as i64).unwrap_or(now_ms());
    let start_ms = params
        .start
        .map(|t| (t * 1000.0) as i64)
        .unwrap_or(end_ms - promql::types::DEFAULT_LOOKBACK_MS);

    let all_series = fetch_series_for_query(&state, start_ms, end_ms).await;

    let mut names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for s in &all_series {
        for k in s.labels.keys() {
            names.insert(k.clone());
        }
    }

    let mut out = String::from(r#"{"status":"success","data":["#);
    for (i, n) in names.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        promql::types::json_escape(&mut out, n);
        out.push('"');
    }
    out.push_str("]}");

    (StatusCode::OK, [("content-type", "application/json")], out)
}

/// GET `/obsv/api/v1/label/:name/values` — list values for a label.
pub async fn label_values(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<LabelsParams>,
) -> impl IntoResponse {
    let end_ms = params.end.map(|t| (t * 1000.0) as i64).unwrap_or(now_ms());
    let start_ms = params
        .start
        .map(|t| (t * 1000.0) as i64)
        .unwrap_or(end_ms - promql::types::DEFAULT_LOOKBACK_MS);

    let all_series = fetch_series_for_query(&state, start_ms, end_ms).await;

    let mut values: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for s in &all_series {
        if let Some(v) = s.labels.get(&name) {
            values.insert(v.clone());
        }
    }

    let mut out = String::from(r#"{"status":"success","data":["#);
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        promql::types::json_escape(&mut out, v);
        out.push('"');
    }
    out.push_str("]}");

    (StatusCode::OK, [("content-type", "application/json")], out)
}

/// GET `/obsv/api/v1/status/buildinfo` — Grafana data source health check.
pub async fn buildinfo() -> impl IntoResponse {
    let out = format!(
        r#"{{"status":"success","data":{{"version":"{}","revision":"nodedb","branch":"main","buildDate":"","goVersion":"","buildUser":""}}}}"#,
        env!("CARGO_PKG_VERSION")
    );
    (StatusCode::OK, [("content-type", "application/json")], out)
}

/// GET `/obsv/api/v1/metadata` — Metric metadata for Grafana metric browser.
pub async fn metadata(State(state): State<AppState>) -> impl IntoResponse {
    let mut out = String::from(r#"{"status":"success","data":{"#);
    let mut first = true;

    if state.shared.system_metrics.is_some() {
        let metrics_meta: &[(&str, &str, &str)] = &[
            ("nodedb_queries_total", "counter", "Total queries executed"),
            ("nodedb_query_errors_total", "counter", "Query errors"),
            (
                "nodedb_active_connections",
                "gauge",
                "Active client connections",
            ),
            (
                "nodedb_wal_fsync_latency_us",
                "gauge",
                "WAL fsync latency in microseconds",
            ),
            ("nodedb_raft_apply_lag", "gauge", "Raft apply lag entries"),
            (
                "nodedb_bridge_utilization",
                "gauge",
                "SPSC bridge utilization percent",
            ),
            (
                "nodedb_compaction_debt",
                "gauge",
                "Pending L1 segments for compaction",
            ),
            (
                "nodedb_vector_searches_total",
                "counter",
                "Vector search operations",
            ),
            (
                "nodedb_graph_traversals_total",
                "counter",
                "Graph traversal operations",
            ),
            (
                "nodedb_text_searches_total",
                "counter",
                "Text search operations",
            ),
            ("nodedb_kv_gets_total", "counter", "KV GET operations"),
            ("nodedb_kv_memory_bytes", "gauge", "KV engine memory usage"),
            (
                "nodedb_pgwire_connections",
                "gauge",
                "Active pgwire connections",
            ),
            (
                "nodedb_slow_queries_total",
                "counter",
                "Queries exceeding 100ms",
            ),
            (
                "nodedb_storage_l0_bytes",
                "gauge",
                "L0 (hot/RAM) storage bytes",
            ),
            (
                "nodedb_storage_l1_bytes",
                "gauge",
                "L1 (warm/NVMe) storage bytes",
            ),
        ];

        for (name, metric_type, help) in metrics_meta {
            if !first {
                out.push(',');
            }
            first = false;
            out.push('"');
            out.push_str(name);
            out.push_str(r#"":[{"type":""#);
            out.push_str(metric_type);
            out.push_str(r#"","help":""#);
            promql::types::json_escape(&mut out, help);
            out.push_str(r#"","unit":""}]"#);
        }
    }

    out.push_str("}}");
    (StatusCode::OK, [("content-type", "application/json")], out)
}

/// POST `/obsv/api/v1/annotations` — Grafana annotation query.
///
/// Grafana sends `{"range":{"from":"...","to":"..."},"annotation":{"query":"..."}}`
/// and expects `[{"time":ms,"title":"...","text":"...","tags":["..."]}]`.
pub async fn annotations(
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

    // Evaluate the PromQL query as a range query over the annotation window.
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

    let mut annotations: Vec<serde_json::Value> = Vec::new();
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
                    annotations.push(serde_json::json!({
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
        sonic_rs::to_string(&annotations).unwrap_or_else(|_| "[]".into()),
    )
}

/// Parse a timestamp as epoch milliseconds or ISO 8601 (RFC 3339).
///
/// Grafana sends timestamps in both formats depending on the data source config.
fn parse_iso_ms(s: &str) -> Option<i64> {
    // Try epoch ms first.
    if let Ok(ms) = s.parse::<i64>() {
        return Some(ms);
    }
    // Try epoch seconds (Grafana sometimes sends these).
    if let Ok(secs) = s.parse::<f64>() {
        return Some((secs * 1000.0) as i64);
    }
    // ISO 8601 / RFC 3339: "2024-01-01T00:00:00.000Z" or "2024-01-01T00:00:00+00:00".
    let s = s.trim();
    let (date_part, time_part) = s.split_once('T')?;
    let date_parts: Vec<&str> = date_part.split('-').collect();
    if date_parts.len() != 3 {
        return None;
    }
    let year: i64 = date_parts[0].parse().ok()?;
    let month: i64 = date_parts[1].parse().ok()?;
    let day: i64 = date_parts[2].parse().ok()?;

    // Strip timezone suffix.
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

    // Days from Unix epoch (1970-01-01) using a simple calendar formula.
    // Accurate for dates 1970-2099 (no leap second correction needed for ms precision).
    let mut days = (year - 1970) * 365 + (year - 1969) / 4;
    let month_days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    if (1..=12).contains(&month) {
        days += month_days[(month - 1) as usize];
    }
    // Leap year correction for current year.
    if month > 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
        days += 1;
    }
    days += day - 1;

    let total_ms = days * 86_400_000 + hour * 3_600_000 + min * 60_000 + (sec_frac * 1000.0) as i64;
    Some(total_ms)
}
