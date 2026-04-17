//! Tenant-scoped identifiers for cross-tenant isolation.
//!
//! All tenant data in the Data Plane is scoped by tenant_id using the
//! `"{tid}:{name}"` convention. This module centralizes the construction
//! and parsing of scoped identifiers to ensure consistency and prevent
//! cross-tenant visibility.

/// Construct a tenant-scoped collection key: `"{tid}:{collection}"`.
///
/// Used for config lookups, inverted index scoping, and secondary index keys.
#[inline]
pub fn scoped_collection(tid: u32, collection: &str) -> String {
    format!("{tid}:{collection}")
}

/// Construct a tenant-scoped node ID: `"{tid}:{node_id}"`.
///
/// Used for graph CSR adjacency and edge store lookups.
#[inline]
pub fn scoped_node(tid: u32, node_id: &str) -> String {
    format!("{tid}:{node_id}")
}

/// Strip the `"{tid}:"` prefix from a scoped identifier for client-facing output.
#[inline]
pub fn unscoped(scoped: &str) -> &str {
    scoped.find(':').map(|i| &scoped[i + 1..]).unwrap_or(scoped)
}

/// Conservative variant of `unscoped` that only strips a leading run
/// of ASCII digits followed by `:`. Safe to apply to values whose
/// scoping is not guaranteed (e.g. MATCH binding rows that mix node
/// ids with literal property values containing `:`), because
/// timestamps, URIs, and compound keys without a `{digits}:` prefix
/// pass through unchanged.
#[inline]
pub fn strip_tenant_prefix(s: &str) -> &str {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i > 0 && i < bytes.len() && bytes[i] == b':' {
        &s[i + 1..]
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_collection_format() {
        assert_eq!(scoped_collection(42, "orders"), "42:orders");
    }

    #[test]
    fn scoped_node_format() {
        assert_eq!(scoped_node(1, "doc-123"), "1:doc-123");
    }

    #[test]
    fn unscoped_strips_prefix() {
        assert_eq!(unscoped("42:orders"), "orders");
        assert_eq!(unscoped("1:doc-123"), "doc-123");
    }

    #[test]
    fn unscoped_no_prefix_passthrough() {
        assert_eq!(unscoped("no_prefix"), "no_prefix");
    }

    #[test]
    fn strip_tenant_prefix_strips_numeric_only() {
        assert_eq!(strip_tenant_prefix("1:alice"), "alice");
        assert_eq!(strip_tenant_prefix("42:doc-123"), "doc-123");
    }

    #[test]
    fn strip_tenant_prefix_preserves_non_tenant_colons() {
        assert_eq!(strip_tenant_prefix("alice"), "alice");
        assert_eq!(strip_tenant_prefix("key:value"), "key:value");
        assert_eq!(
            strip_tenant_prefix("2020-01-02T12:00:00"),
            "2020-01-02T12:00:00"
        );
    }
}
