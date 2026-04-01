//! Per-tenant WASM resource accounting.
//!
//! Tracks fuel consumed and invocation count per (tenant_id, function_name).
//! Used for quota enforcement and audit reporting.

use std::collections::HashMap;
use std::sync::Mutex;

/// Per-function usage counters.
#[derive(Debug, Clone, Default)]
pub struct FunctionUsage {
    /// Total invocations.
    pub invocations: u64,
    /// Total fuel consumed across all invocations.
    pub fuel_consumed: u64,
}

/// Global WASM resource accounting tracker.
pub struct WasmAccounting {
    /// (tenant_id, function_name) → usage counters.
    usage: Mutex<HashMap<(u32, String), FunctionUsage>>,
}

impl WasmAccounting {
    pub fn new() -> Self {
        Self {
            usage: Mutex::new(HashMap::new()),
        }
    }

    /// Record a WASM UDF invocation.
    pub fn record_invocation(&self, tenant_id: u32, function_name: &str, fuel_consumed: u64) {
        let mut map = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        let entry = map
            .entry((tenant_id, function_name.to_string()))
            .or_default();
        entry.invocations += 1;
        entry.fuel_consumed += fuel_consumed;
    }

    /// Get usage for a specific function.
    pub fn get_usage(&self, tenant_id: u32, function_name: &str) -> FunctionUsage {
        let map = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        map.get(&(tenant_id, function_name.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    /// Get all usage entries for a tenant.
    pub fn get_tenant_usage(&self, tenant_id: u32) -> Vec<(String, FunctionUsage)> {
        let map = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        map.iter()
            .filter(|((tid, _), _)| *tid == tenant_id)
            .map(|((_, name), usage)| (name.clone(), usage.clone()))
            .collect()
    }

    /// Reset counters (for testing or periodic reset).
    pub fn clear(&self) {
        self.usage.lock().unwrap_or_else(|p| p.into_inner()).clear();
    }
}

impl Default for WasmAccounting {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_query() {
        let acc = WasmAccounting::new();
        acc.record_invocation(1, "add", 500);
        acc.record_invocation(1, "add", 300);
        acc.record_invocation(1, "mul", 100);

        let add = acc.get_usage(1, "add");
        assert_eq!(add.invocations, 2);
        assert_eq!(add.fuel_consumed, 800);

        let mul = acc.get_usage(1, "mul");
        assert_eq!(mul.invocations, 1);
    }

    #[test]
    fn tenant_isolation() {
        let acc = WasmAccounting::new();
        acc.record_invocation(1, "f", 100);
        acc.record_invocation(2, "f", 200);

        assert_eq!(acc.get_usage(1, "f").fuel_consumed, 100);
        assert_eq!(acc.get_usage(2, "f").fuel_consumed, 200);
    }

    #[test]
    fn tenant_usage_list() {
        let acc = WasmAccounting::new();
        acc.record_invocation(1, "a", 10);
        acc.record_invocation(1, "b", 20);
        acc.record_invocation(2, "c", 30);

        let t1 = acc.get_tenant_usage(1);
        assert_eq!(t1.len(), 2);
    }
}
