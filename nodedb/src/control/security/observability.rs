//! Auth observability: Prometheus-compatible metrics, anomaly detection, circuit breaker.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Auth system metrics for Prometheus scraping.
///
/// All counters are atomic — safe for concurrent updates from multiple
/// protocol handlers without locking.
pub struct AuthMetrics {
    // ── Duration histograms (approximate via bucket counters) ────────
    /// JWT validation durations: [<1ms, <5ms, <10ms, <50ms, <100ms, <500ms, >=500ms]
    pub jwt_validation_buckets: [AtomicU64; 7],
    /// RLS evaluation durations (same buckets).
    pub rls_evaluation_buckets: [AtomicU64; 7],
    /// Scope resolution durations.
    pub scope_resolution_buckets: [AtomicU64; 7],

    // ── Counters ────────────────────────────────────────────────────
    pub auth_success_password: AtomicU64,
    pub auth_success_api_key: AtomicU64,
    pub auth_success_jwt: AtomicU64,
    pub auth_success_certificate: AtomicU64,
    pub auth_success_trust: AtomicU64,

    pub auth_failure_password: AtomicU64,
    pub auth_failure_api_key: AtomicU64,
    pub auth_failure_jwt: AtomicU64,
    pub auth_failure_expired: AtomicU64,
    pub auth_failure_blacklisted: AtomicU64,

    pub rls_denied_total: AtomicU64,
    pub blacklist_rejected_total: AtomicU64,
    pub rate_limit_rejected_total: AtomicU64,

    pub jwks_cache_hit_total: AtomicU64,
    pub jwks_cache_miss_total: AtomicU64,
    pub jwks_fetch_success_total: AtomicU64,
    pub jwks_fetch_failure_total: AtomicU64,

    // ── Anomaly detection ───────────────────────────────────────────
    /// Failed logins in the current 1-minute window.
    pub failed_login_window: AtomicU64,
    /// Timestamp of the current window start.
    window_start_secs: AtomicU64,

    // ── Circuit breaker ─────────────────────────────────────────────
    /// Whether the JWKS circuit breaker is open (rejecting new JWT validations).
    pub jwks_circuit_open: std::sync::atomic::AtomicBool,
}

impl AuthMetrics {
    pub fn new() -> Self {
        Self {
            jwt_validation_buckets: Default::default(),
            rls_evaluation_buckets: Default::default(),
            scope_resolution_buckets: Default::default(),

            auth_success_password: AtomicU64::new(0),
            auth_success_api_key: AtomicU64::new(0),
            auth_success_jwt: AtomicU64::new(0),
            auth_success_certificate: AtomicU64::new(0),
            auth_success_trust: AtomicU64::new(0),

            auth_failure_password: AtomicU64::new(0),
            auth_failure_api_key: AtomicU64::new(0),
            auth_failure_jwt: AtomicU64::new(0),
            auth_failure_expired: AtomicU64::new(0),
            auth_failure_blacklisted: AtomicU64::new(0),

            rls_denied_total: AtomicU64::new(0),
            blacklist_rejected_total: AtomicU64::new(0),
            rate_limit_rejected_total: AtomicU64::new(0),

            jwks_cache_hit_total: AtomicU64::new(0),
            jwks_cache_miss_total: AtomicU64::new(0),
            jwks_fetch_success_total: AtomicU64::new(0),
            jwks_fetch_failure_total: AtomicU64::new(0),

            failed_login_window: AtomicU64::new(0),
            window_start_secs: AtomicU64::new(now_secs()),

            jwks_circuit_open: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Record a duration in a histogram bucket array.
    pub fn record_duration(buckets: &[AtomicU64; 7], start: Instant) {
        let us = start.elapsed().as_micros() as u64;
        let idx = match us {
            0..=999 => 0,         // <1ms
            1000..=4999 => 1,     // <5ms
            5000..=9999 => 2,     // <10ms
            10000..=49999 => 3,   // <50ms
            50000..=99999 => 4,   // <100ms
            100000..=499999 => 5, // <500ms
            _ => 6,               // >=500ms
        };
        buckets[idx].fetch_add(1, Ordering::Relaxed);
    }

    /// Record auth success by method.
    pub fn record_auth_success(&self, method: &str) {
        match method {
            "password" | "scram" => self.auth_success_password.fetch_add(1, Ordering::Relaxed),
            "api_key" => self.auth_success_api_key.fetch_add(1, Ordering::Relaxed),
            "jwt" => self.auth_success_jwt.fetch_add(1, Ordering::Relaxed),
            "certificate" => self
                .auth_success_certificate
                .fetch_add(1, Ordering::Relaxed),
            "trust" => self.auth_success_trust.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
    }

    /// Record auth failure by reason.
    pub fn record_auth_failure(&self, reason: &str) {
        match reason {
            "password" => self.auth_failure_password.fetch_add(1, Ordering::Relaxed),
            "api_key" => self.auth_failure_api_key.fetch_add(1, Ordering::Relaxed),
            "jwt" => self.auth_failure_jwt.fetch_add(1, Ordering::Relaxed),
            "expired" => self.auth_failure_expired.fetch_add(1, Ordering::Relaxed),
            "blacklisted" => self
                .auth_failure_blacklisted
                .fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };

        // Anomaly: count in current 1-minute window.
        self.rotate_window_if_needed();
        self.failed_login_window.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if failed login rate exceeds threshold (anomaly alert).
    pub fn failed_login_spike(&self, threshold: u64) -> bool {
        self.rotate_window_if_needed();
        self.failed_login_window.load(Ordering::Relaxed) > threshold
    }

    /// Rotate the 1-minute anomaly detection window if expired.
    fn rotate_window_if_needed(&self) {
        let now = now_secs();
        let start = self.window_start_secs.load(Ordering::Relaxed);
        if now - start >= 60 {
            self.window_start_secs.store(now, Ordering::Relaxed);
            self.failed_login_window.store(0, Ordering::Relaxed);
        }
    }

    /// Open the JWKS circuit breaker (JWKS unreachable + cache expired).
    pub fn open_jwks_circuit(&self) {
        self.jwks_circuit_open
            .store(true, std::sync::atomic::Ordering::SeqCst);
        tracing::warn!("JWKS circuit breaker OPEN — rejecting new JWT validations");
    }

    /// Close the JWKS circuit breaker (JWKS fetched successfully).
    pub fn close_jwks_circuit(&self) {
        if self
            .jwks_circuit_open
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            tracing::info!("JWKS circuit breaker CLOSED — JWT validation resumed");
        }
    }

    /// Check if JWKS circuit breaker is open.
    pub fn is_jwks_circuit_open(&self) -> bool {
        self.jwks_circuit_open
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Export all metrics as Prometheus text format.
    pub fn to_prometheus(&self) -> String {
        let mut out = String::with_capacity(2048);

        // Counters.
        append_counter(
            &mut out,
            "nodedb_auth_success_total",
            "method",
            &[
                (
                    "password",
                    self.auth_success_password.load(Ordering::Relaxed),
                ),
                ("api_key", self.auth_success_api_key.load(Ordering::Relaxed)),
                ("jwt", self.auth_success_jwt.load(Ordering::Relaxed)),
                (
                    "certificate",
                    self.auth_success_certificate.load(Ordering::Relaxed),
                ),
                ("trust", self.auth_success_trust.load(Ordering::Relaxed)),
            ],
        );
        append_counter(
            &mut out,
            "nodedb_auth_failure_total",
            "reason",
            &[
                (
                    "password",
                    self.auth_failure_password.load(Ordering::Relaxed),
                ),
                ("api_key", self.auth_failure_api_key.load(Ordering::Relaxed)),
                ("jwt", self.auth_failure_jwt.load(Ordering::Relaxed)),
                ("expired", self.auth_failure_expired.load(Ordering::Relaxed)),
                (
                    "blacklisted",
                    self.auth_failure_blacklisted.load(Ordering::Relaxed),
                ),
            ],
        );

        out.push_str(&format!(
            "nodedb_rls_denied_total {}\n",
            self.rls_denied_total.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "nodedb_blacklist_rejected_total {}\n",
            self.blacklist_rejected_total.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "nodedb_rate_limit_rejected_total {}\n",
            self.rate_limit_rejected_total.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "nodedb_jwks_cache_hit_total {}\n",
            self.jwks_cache_hit_total.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "nodedb_jwks_cache_miss_total {}\n",
            self.jwks_cache_miss_total.load(Ordering::Relaxed)
        ));

        // Histograms.
        append_histogram(
            &mut out,
            "nodedb_jwt_validation_duration_seconds",
            &self.jwt_validation_buckets,
        );
        append_histogram(
            &mut out,
            "nodedb_rls_evaluation_duration_seconds",
            &self.rls_evaluation_buckets,
        );
        append_histogram(
            &mut out,
            "nodedb_scope_resolution_duration_seconds",
            &self.scope_resolution_buckets,
        );

        out
    }
}

impl Default for AuthMetrics {
    fn default() -> Self {
        Self::new()
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn append_counter(out: &mut String, name: &str, label: &str, values: &[(&str, u64)]) {
    for (lv, count) in values {
        out.push_str(&format!("{name}{{{label}=\"{lv}\"}} {count}\n"));
    }
}

fn append_histogram(out: &mut String, name: &str, buckets: &[AtomicU64; 7]) {
    let boundaries = ["0.001", "0.005", "0.01", "0.05", "0.1", "0.5", "+Inf"];
    let mut cumulative = 0u64;
    for (i, boundary) in boundaries.iter().enumerate() {
        cumulative += buckets[i].load(Ordering::Relaxed);
        out.push_str(&format!(
            "{name}_bucket{{le=\"{boundary}\"}} {cumulative}\n"
        ));
    }
    out.push_str(&format!("{name}_count {cumulative}\n"));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_increments() {
        let m = AuthMetrics::new();
        m.record_auth_success("jwt");
        m.record_auth_success("jwt");
        m.record_auth_failure("password");

        assert_eq!(m.auth_success_jwt.load(Ordering::Relaxed), 2);
        assert_eq!(m.auth_failure_password.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn histogram_bucketing() {
        let m = AuthMetrics::new();
        let start = Instant::now();
        // Immediate → <1ms bucket.
        AuthMetrics::record_duration(&m.jwt_validation_buckets, start);
        assert!(m.jwt_validation_buckets[0].load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn anomaly_detection() {
        let m = AuthMetrics::new();
        for _ in 0..50 {
            m.record_auth_failure("password");
        }
        assert!(m.failed_login_spike(10));
        assert!(!m.failed_login_spike(100));
    }

    #[test]
    fn circuit_breaker() {
        let m = AuthMetrics::new();
        assert!(!m.is_jwks_circuit_open());
        m.open_jwks_circuit();
        assert!(m.is_jwks_circuit_open());
        m.close_jwks_circuit();
        assert!(!m.is_jwks_circuit_open());
    }

    #[test]
    fn prometheus_export() {
        let m = AuthMetrics::new();
        m.record_auth_success("jwt");
        m.rls_denied_total.fetch_add(5, Ordering::Relaxed);
        let output = m.to_prometheus();
        assert!(output.contains("nodedb_auth_success_total{method=\"jwt\"} 1"));
        assert!(output.contains("nodedb_rls_denied_total 5"));
        assert!(output.contains("nodedb_jwt_validation_duration_seconds_bucket"));
    }
}
