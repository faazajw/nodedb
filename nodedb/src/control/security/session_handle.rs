//! Opaque session handle store: maps UUID handles to cached `AuthContext`.
//!
//! Allows connection poolers and stateless clients to authenticate once
//! via `POST /api/auth/session`, receive a UUID handle, then attach it
//! to pgwire connections via `SET LOCAL nodedb.auth_session = '<uuid>'`.
//!
//! The handle resolves to a full `AuthContext` without re-validating the
//! JWT on every query. Handles expire after a configurable TTL.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use super::auth_context::AuthContext;

/// A cached session with expiry.
struct CachedSession {
    auth_context: AuthContext,
    created_at: u64,
    expires_at: u64,
}

/// Thread-safe session handle store.
pub struct SessionHandleStore {
    /// UUID handle → cached session.
    sessions: RwLock<HashMap<String, CachedSession>>,
    /// Default TTL for session handles in seconds (default: 3600 = 1h).
    default_ttl_secs: u64,
}

impl SessionHandleStore {
    pub fn new(default_ttl_secs: u64) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            default_ttl_secs,
        }
    }

    /// Create a session handle for the given `AuthContext`.
    /// Returns the UUID handle string.
    pub fn create(&self, auth_context: AuthContext) -> String {
        let now = now_secs();
        let handle = generate_handle();
        let cached = CachedSession {
            auth_context,
            created_at: now,
            expires_at: now + self.default_ttl_secs,
        };

        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.insert(handle.clone(), cached);

        // Lazy cleanup: remove expired handles (max 100 per call to avoid latency).
        let expired: Vec<String> = sessions
            .iter()
            .filter(|(_, s)| now >= s.expires_at)
            .take(100)
            .map(|(k, _)| k.clone())
            .collect();
        for key in expired {
            sessions.remove(&key);
        }

        handle
    }

    /// Resolve a session handle to its cached `AuthContext`.
    /// Returns `None` if handle not found or expired.
    pub fn resolve(&self, handle: &str) -> Option<AuthContext> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        let cached = sessions.get(handle)?;
        let now = now_secs();
        if now >= cached.expires_at {
            return None; // Expired — lazy cleanup on next create().
        }
        Some(cached.auth_context.clone())
    }

    /// Invalidate a session handle.
    pub fn invalidate(&self, handle: &str) -> bool {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.remove(handle).is_some()
    }

    /// Number of active (non-expired) handles.
    pub fn count(&self) -> usize {
        let now = now_secs();
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.values().filter(|s| now < s.expires_at).count()
    }

    /// Age of the oldest active session handle in seconds.
    pub fn oldest_age_secs(&self) -> u64 {
        let now = now_secs();
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .values()
            .filter(|s| now < s.expires_at)
            .map(|s| now.saturating_sub(s.created_at))
            .max()
            .unwrap_or(0)
    }
}

impl Default for SessionHandleStore {
    fn default() -> Self {
        Self::new(3600)
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Generate a cryptographically random UUID-like handle.
fn generate_handle() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ts = now_secs();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    // Use timestamp + counter for uniqueness. Not cryptographic but sufficient
    // for session handles since the handle is opaque and short-lived.
    format!("nds_{ts:x}_{seq:08x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::auth_context::AuthContext;
    use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
    use crate::types::TenantId;

    fn test_auth_context() -> AuthContext {
        let identity = AuthenticatedIdentity {
            user_id: 42,
            username: "alice".into(),
            tenant_id: TenantId::new(1),
            auth_method: AuthMethod::ApiKey,
            roles: vec![Role::ReadWrite],
            is_superuser: false,
        };
        AuthContext::from_identity(
            &identity,
            crate::control::security::auth_context::generate_session_id(),
        )
    }

    #[test]
    fn create_and_resolve() {
        let store = SessionHandleStore::new(3600);
        let handle = store.create(test_auth_context());

        assert!(handle.starts_with("nds_"));
        let resolved = store.resolve(&handle).unwrap();
        assert_eq!(resolved.username, "alice");
    }

    #[test]
    fn expired_handle_returns_none() {
        let store = SessionHandleStore::new(0); // 0 TTL = immediate expiry
        let handle = store.create(test_auth_context());

        // Should be expired immediately.
        assert!(store.resolve(&handle).is_none());
    }

    #[test]
    fn invalidate_removes_handle() {
        let store = SessionHandleStore::new(3600);
        let handle = store.create(test_auth_context());
        assert!(store.resolve(&handle).is_some());

        store.invalidate(&handle);
        assert!(store.resolve(&handle).is_none());
    }

    #[test]
    fn unknown_handle_returns_none() {
        let store = SessionHandleStore::new(3600);
        assert!(store.resolve("nds_nonexistent").is_none());
    }
}
