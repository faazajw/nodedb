//! End-to-end trace_id propagation across all planes.
//!
//! Ensures `trace_id` flows from:
//! 1. Client request (HTTP header `X-Trace-ID` or pgwire parameter)
//! 2. Control Plane (DataFusion planning, routing)
//! 3. SPSC bridge envelope (Request.trace_id field)
//! 4. Data Plane execution (CoreLoop logging)
//! 5. Network hops (cluster transport VShardEnvelope)
//!
//! If the client doesn't supply a trace_id, one is generated from a
//! monotonic counter + timestamp for correlation.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Global trace ID generator for requests that don't carry one.
static TRACE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a trace ID from timestamp + monotonic counter.
///
/// Format: upper 32 bits = epoch seconds, lower 32 bits = counter.
/// This ensures trace IDs are globally unique (within a node) and
/// roughly time-ordered for log correlation.
pub fn generate_trace_id() -> u64 {
    let epoch_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "system clock before Unix epoch, trace IDs may collide");
            e.duration()
        })
        .as_secs() as u32;
    let counter = TRACE_COUNTER.fetch_add(1, Ordering::Relaxed) as u32;
    ((epoch_secs as u64) << 32) | (counter as u64)
}

/// Extract trace_id from an HTTP request header.
///
/// Checks `X-Trace-ID`, `traceparent` (W3C), and `X-Request-ID` headers.
pub fn extract_from_headers(headers: &axum::http::HeaderMap) -> u64 {
    // Check X-Trace-ID (our custom header).
    if let Some(val) = headers.get("x-trace-id")
        && let Ok(s) = val.to_str()
    {
        if let Ok(id) = s.parse::<u64>() {
            return id;
        }
        // Try hex parsing.
        if let Ok(id) = u64::from_str_radix(s.trim_start_matches("0x"), 16) {
            return id;
        }
    }

    // Check W3C traceparent: "00-<trace_id>-<span_id>-<flags>"
    if let Some(val) = headers.get("traceparent")
        && let Ok(s) = val.to_str()
    {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() >= 2 {
            // Take last 16 hex chars of the 32-char trace_id.
            let trace_hex = parts[1];
            if trace_hex.len() >= 16 {
                let suffix = &trace_hex[trace_hex.len() - 16..];
                if let Ok(id) = u64::from_str_radix(suffix, 16) {
                    return id;
                }
            }
        }
    }

    // Check X-Request-ID (common in API gateways).
    if let Some(val) = headers.get("x-request-id")
        && let Ok(s) = val.to_str()
        && let Ok(id) = s.parse::<u64>()
    {
        return id;
    }

    // No trace ID found — generate one.
    generate_trace_id()
}

/// Extract trace_id from a pgwire startup parameter.
///
/// PostgreSQL clients can pass custom parameters during connection:
/// `options=-c trace_id=12345`
pub fn extract_from_pgwire_params(params: &std::collections::HashMap<String, String>) -> u64 {
    if let Some(val) = params.get("trace_id")
        && let Ok(id) = val.parse::<u64>()
    {
        return id;
    }
    generate_trace_id()
}

/// Create a tracing span with the trace_id attached.
///
/// This is the standard way to propagate trace_id through async code:
/// ```ignore
/// let span = trace_context::make_span(trace_id, "query");
/// async { ... }.instrument(span).await;
/// ```
pub fn make_span(trace_id: u64, operation: &str) -> tracing::Span {
    tracing::info_span!("op", trace_id, operation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_unique_ids() {
        let id1 = generate_trace_id();
        let id2 = generate_trace_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn extract_from_x_trace_id() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-trace-id", "42".parse().unwrap());
        assert_eq!(extract_from_headers(&headers), 42);
    }

    #[test]
    fn extract_from_hex_trace_id() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-trace-id", "0xABCD".parse().unwrap());
        assert_eq!(extract_from_headers(&headers), 0xABCD);
    }

    #[test]
    fn extract_generates_when_missing() {
        let headers = axum::http::HeaderMap::new();
        let id = extract_from_headers(&headers);
        assert!(id > 0); // Generated, not zero.
    }

    #[test]
    fn pgwire_param_extraction() {
        let mut params = std::collections::HashMap::new();
        params.insert("trace_id".into(), "9999".into());
        assert_eq!(extract_from_pgwire_params(&params), 9999);
    }
}
