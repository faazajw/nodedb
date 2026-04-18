//! The durable `AuditEntry` record + SHA-256 hash-chain linking.

use std::time::SystemTime;

use crate::types::TenantId;

use super::event::AuditEvent;

/// Security-relevant audit event.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct AuditEntry {
    /// Monotonic sequence number within this node.
    pub seq: u64,
    /// UTC timestamp (microseconds since epoch).
    pub timestamp_us: u64,
    /// Event category.
    pub event: AuditEvent,
    /// Tenant context (if applicable).
    pub tenant_id: Option<TenantId>,
    /// Authenticated user ID (from AuthContext). Empty for unauthenticated.
    #[serde(default)]
    pub auth_user_id: String,
    /// Authenticated username (for display/audit trail).
    #[serde(default)]
    pub auth_user_name: String,
    /// Session ID (for audit correlation across events).
    #[serde(default)]
    pub session_id: String,
    /// Source IP or node identifier.
    pub source: String,
    /// Human-readable detail.
    pub detail: String,
    /// SHA-256 hash of the previous entry (hex). Empty for first entry.
    pub prev_hash: String,
}

/// Compute SHA-256 hash of an audit entry for chain linking.
///
/// Hash covers: prev_hash + seq + timestamp + event + source + detail.
/// This ensures any modification to any field breaks the chain.
pub(super) fn hash_entry(entry: &AuditEntry) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(entry.prev_hash.as_bytes());
    hasher.update(entry.seq.to_le_bytes());
    hasher.update(entry.timestamp_us.to_le_bytes());
    hasher.update(format!("{:?}", entry.event).as_bytes());
    hasher.update(entry.auth_user_id.as_bytes());
    hasher.update(entry.auth_user_name.as_bytes());
    hasher.update(entry.session_id.as_bytes());
    hasher.update(entry.source.as_bytes());
    hasher.update(entry.detail.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub(super) fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}
