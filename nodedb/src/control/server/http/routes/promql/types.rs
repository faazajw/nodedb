//! Shared query-parameter types for PromQL HTTP handlers.

use serde::Deserialize;

/// Query parameters for `/query`.
#[derive(Debug, Deserialize)]
pub struct InstantQueryParams {
    pub query: String,
    pub time: Option<f64>,
}

/// Query parameters for `/query_range`.
#[derive(Debug, Deserialize)]
pub struct RangeQueryParams {
    pub query: String,
    pub start: f64,
    pub end: f64,
    pub step: String,
}

/// Query parameters for `/series`.
#[derive(Debug, Deserialize)]
pub struct SeriesParams {
    #[serde(rename = "match[]", default)]
    pub matchers: Vec<String>,
    pub start: Option<f64>,
    pub end: Option<f64>,
}

/// Query parameters for `/labels`.
#[derive(Debug, Deserialize)]
pub struct LabelsParams {
    pub start: Option<f64>,
    pub end: Option<f64>,
}
