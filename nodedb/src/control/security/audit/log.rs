//! Append-only in-memory `AuditLog`.

use std::collections::VecDeque;

use crate::types::TenantId;

use super::auth::AuditAuth;
use super::entry::{AuditEntry, hash_entry, now_us};
use super::event::AuditEvent;
use super::level::AuditLevel;

/// Append-only audit log.
///
/// Entries are immutable once appended. The log supports bounded in-memory
/// retention with overflow to the WAL or a dedicated audit segment file.
///
/// Lives on the Control Plane (Send + Sync).
pub struct AuditLog {
    entries: VecDeque<AuditEntry>,
    /// Separate auth event stream for login/logout/MFA/deactivation.
    auth_events: VecDeque<AuditEntry>,
    /// Maximum entries retained in memory.
    max_entries: usize,
    /// Next sequence number.
    next_seq: u64,
    /// Total entries ever recorded (including evicted).
    total_entries: u64,
    /// Hash of the last recorded entry (for chain continuity).
    last_hash: String,
    /// Current audit level (controls which events are recorded).
    level: AuditLevel,
}

impl AuditLog {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(max_entries.min(10_000)),
            auth_events: VecDeque::with_capacity(1_000),
            max_entries,
            next_seq: 1,
            total_entries: 0,
            last_hash: String::new(),
            level: AuditLevel::default(),
        }
    }

    /// Set the audit level.
    pub fn set_level(&mut self, level: AuditLevel) {
        self.level = level;
    }

    /// Get the current audit level.
    pub fn level(&self) -> AuditLevel {
        self.level
    }

    /// Set the next sequence number (used on startup to resume from catalog).
    pub fn set_next_seq(&mut self, seq: u64) {
        self.next_seq = seq;
    }

    /// Set the last hash (used on startup to continue the chain from catalog).
    pub fn set_last_hash(&mut self, hash: String) {
        self.last_hash = hash;
    }

    /// Record an audit event with hash chain (legacy — no auth context).
    pub fn record(
        &mut self,
        event: AuditEvent,
        tenant_id: Option<TenantId>,
        source: &str,
        detail: &str,
    ) -> u64 {
        self.record_with_auth(event, tenant_id, source, detail, &AuditAuth::default())
    }

    /// Record an audit event with auth context enrichment.
    pub fn record_with_auth(
        &mut self,
        event: AuditEvent,
        tenant_id: Option<TenantId>,
        source: &str,
        detail: &str,
        auth: &AuditAuth,
    ) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        let prev_hash = self.last_hash.clone();

        let entry = AuditEntry {
            seq,
            timestamp_us: now_us(),
            event: event.clone(),
            tenant_id,
            auth_user_id: auth.user_id.clone(),
            auth_user_name: auth.user_name.clone(),
            session_id: auth.session_id.clone(),
            source: source.to_string(),
            detail: detail.to_string(),
            prev_hash,
        };

        self.last_hash = hash_entry(&entry);

        // Route auth events to the separate auth_events stream.
        if event.is_auth_event() {
            if self.auth_events.len() >= 10_000 {
                self.auth_events.pop_front();
            }
            self.auth_events.push_back(entry.clone());
        }

        if self.entries.len() >= self.max_entries {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
        self.total_entries += 1;
        seq
    }

    /// Query entries by event type.
    pub fn query_by_event(&self, event: &AuditEvent) -> Vec<&AuditEntry> {
        self.entries.iter().filter(|e| &e.event == event).collect()
    }

    /// Query entries for a specific tenant.
    pub fn query_by_tenant(&self, tenant_id: TenantId) -> Vec<&AuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.tenant_id == Some(tenant_id))
            .collect()
    }

    /// Query entries since a given sequence number.
    pub fn since(&self, seq: u64) -> Vec<&AuditEntry> {
        self.entries.iter().filter(|e| e.seq >= seq).collect()
    }

    /// Get all entries in memory.
    pub fn all(&self) -> &VecDeque<AuditEntry> {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn total_recorded(&self) -> u64 {
        self.total_entries
    }

    /// Query entries by auth user ID.
    pub fn query_by_user(&self, auth_user_id: &str) -> Vec<&AuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.auth_user_id == auth_user_id)
            .collect()
    }

    /// Get the separate auth events stream (login/logout/MFA/deactivation).
    pub fn auth_events(&self) -> &VecDeque<AuditEntry> {
        &self.auth_events
    }

    /// Drain entries for persistence (WAL or segment file).
    pub fn drain_for_persistence(&mut self) -> Vec<AuditEntry> {
        self.entries.drain(..).collect()
    }

    /// Get the current last hash (for chain continuity on restart).
    pub fn last_hash(&self) -> &str {
        &self.last_hash
    }

    /// Verify the hash chain integrity of in-memory entries.
    /// Returns Ok(()) if chain is valid, Err with the broken sequence number.
    pub fn verify_chain(&self) -> Result<(), u64> {
        let mut expected_prev = String::new();
        for entry in &self.entries {
            if entry.prev_hash != expected_prev {
                return Err(entry.seq);
            }
            expected_prev = hash_entry(entry);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_log() {
        let log = AuditLog::new(100);
        assert!(log.is_empty());
        assert_eq!(log.total_recorded(), 0);
    }

    #[test]
    fn record_and_query() {
        let mut log = AuditLog::new(100);

        log.record(
            AuditEvent::AuthSuccess,
            Some(TenantId::new(1)),
            "10.0.0.1",
            "user login",
        );
        log.record(
            AuditEvent::AuthFailure,
            Some(TenantId::new(2)),
            "10.0.0.2",
            "bad password",
        );
        log.record(
            AuditEvent::AuthSuccess,
            Some(TenantId::new(1)),
            "10.0.0.1",
            "api key auth",
        );

        assert_eq!(log.len(), 3);
        assert_eq!(log.total_recorded(), 3);

        let successes = log.query_by_event(&AuditEvent::AuthSuccess);
        assert_eq!(successes.len(), 2);

        let tenant1 = log.query_by_tenant(TenantId::new(1));
        assert_eq!(tenant1.len(), 2);

        let tenant2 = log.query_by_tenant(TenantId::new(2));
        assert_eq!(tenant2.len(), 1);
    }

    #[test]
    fn sequence_numbers_monotonic() {
        let mut log = AuditLog::new(100);
        let s1 = log.record(AuditEvent::AuthSuccess, None, "src", "");
        let s2 = log.record(AuditEvent::AuthFailure, None, "src", "");
        let s3 = log.record(AuditEvent::AdminAction, None, "src", "");
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);
        assert_eq!(s3, 3);
    }

    #[test]
    fn since_query() {
        let mut log = AuditLog::new(100);
        log.record(AuditEvent::AuthSuccess, None, "src", "a");
        log.record(AuditEvent::AuthFailure, None, "src", "b");
        log.record(AuditEvent::AdminAction, None, "src", "c");

        let since2 = log.since(2);
        assert_eq!(since2.len(), 2);
        assert_eq!(since2[0].detail, "b");
        assert_eq!(since2[1].detail, "c");
    }

    #[test]
    fn bounded_eviction() {
        let mut log = AuditLog::new(3);
        for i in 0..5 {
            log.record(AuditEvent::AuthSuccess, None, "src", &format!("entry-{i}"));
        }

        assert_eq!(log.len(), 3);
        assert_eq!(log.total_recorded(), 5);
        // Oldest entries evicted.
        assert_eq!(log.all()[0].detail, "entry-2");
    }

    #[test]
    fn drain_for_persistence() {
        let mut log = AuditLog::new(100);
        log.record(
            AuditEvent::SnapshotBegin,
            None,
            "node-1",
            "snapshot started",
        );
        log.record(AuditEvent::SnapshotEnd, None, "node-1", "snapshot done");

        let drained = log.drain_for_persistence();
        assert_eq!(drained.len(), 2);
        assert!(log.is_empty());
    }

    #[test]
    fn all_event_types_representable() {
        let events = [
            AuditEvent::AuthSuccess,
            AuditEvent::AuthFailure,
            AuditEvent::AuthzDenied,
            AuditEvent::PrivilegeChange,
            AuditEvent::TenantCreated,
            AuditEvent::TenantDeleted,
            AuditEvent::SnapshotBegin,
            AuditEvent::SnapshotEnd,
            AuditEvent::RestoreBegin,
            AuditEvent::RestoreEnd,
            AuditEvent::CertRotation,
            AuditEvent::CertRotationFailed,
            AuditEvent::KeyRotation,
            AuditEvent::ConfigChange,
            AuditEvent::NodeJoined,
            AuditEvent::NodeLeft,
            AuditEvent::AdminAction,
        ];

        let mut log = AuditLog::new(100);
        for event in events {
            log.record(event, None, "test", "");
        }
        assert_eq!(log.len(), 17);
    }
}
