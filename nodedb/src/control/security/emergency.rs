//! Emergency & incident response: lockdown, break-glass, bulk suspend, two-party auth.

use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Emergency lockdown state.
pub struct EmergencyState {
    /// Whether the system is in lockdown mode.
    locked_down: AtomicBool,
    /// Reason for lockdown (set on EMERGENCY LOCKDOWN).
    lockdown_reason: RwLock<String>,
    /// Break-glass key file path (from config).
    break_glass_key_path: Option<String>,
    /// Two-party authorization tracker.
    two_party: TwoPartyTracker,
}

/// Two-party authorization: requires two separate admins to approve.
struct TwoPartyTracker {
    /// Operations requiring two-party auth.
    required_operations: Vec<String>,
    /// Pending approvals: operation → (first_approver, timestamp).
    pending: RwLock<std::collections::HashMap<String, (String, u64)>>,
}

/// Two-party authorization config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TwoPartyConfig {
    /// Operations requiring two separate admin approvals.
    #[serde(default)]
    pub require_for: Vec<String>,
}

impl Default for TwoPartyConfig {
    fn default() -> Self {
        Self {
            require_for: vec![
                "EMERGENCY LOCKDOWN".into(),
                "IMPERSONATE".into(),
                "DROP TENANT".into(),
            ],
        }
    }
}

impl EmergencyState {
    pub fn new(break_glass_key_path: Option<String>, two_party_config: TwoPartyConfig) -> Self {
        Self {
            locked_down: AtomicBool::new(false),
            lockdown_reason: RwLock::new(String::new()),
            break_glass_key_path,
            two_party: TwoPartyTracker {
                required_operations: two_party_config.require_for,
                pending: RwLock::new(std::collections::HashMap::new()),
            },
        }
    }

    /// Activate emergency lockdown. All non-superuser access is suspended.
    pub fn lockdown(&self, reason: &str) {
        self.locked_down.store(true, Ordering::SeqCst);
        let mut r = self
            .lockdown_reason
            .write()
            .unwrap_or_else(|p| p.into_inner());
        *r = reason.to_string();
        warn!(reason = %reason, "EMERGENCY LOCKDOWN ACTIVATED");
    }

    /// Deactivate emergency lockdown.
    pub fn unlock(&self) {
        self.locked_down.store(false, Ordering::SeqCst);
        let mut r = self
            .lockdown_reason
            .write()
            .unwrap_or_else(|p| p.into_inner());
        r.clear();
        info!("EMERGENCY LOCKDOWN DEACTIVATED");
    }

    /// Check if system is in lockdown.
    pub fn is_locked_down(&self) -> bool {
        self.locked_down.load(Ordering::SeqCst)
    }

    /// Get lockdown reason.
    pub fn lockdown_reason(&self) -> String {
        self.lockdown_reason
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Validate break-glass key from file.
    ///
    /// Returns `true` if the provided key matches the key in the configured file.
    /// Used for emergency access when normal auth is unavailable.
    pub fn validate_break_glass(&self, provided_key: &str) -> bool {
        let Some(ref path) = self.break_glass_key_path else {
            return false;
        };
        let Ok(stored_key) = std::fs::read_to_string(path) else {
            warn!(path = %path, "break-glass key file not readable");
            return false;
        };
        let stored_trimmed = stored_key.trim();
        if stored_trimmed.is_empty() {
            return false;
        }
        // Constant-time comparison.
        provided_key.len() == stored_trimmed.len()
            && provided_key
                .as_bytes()
                .iter()
                .zip(stored_trimmed.as_bytes())
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                == 0
    }

    /// Check if an operation requires two-party authorization.
    pub fn requires_two_party(&self, operation: &str) -> bool {
        self.two_party
            .required_operations
            .iter()
            .any(|op| operation.to_uppercase().starts_with(&op.to_uppercase()))
    }

    /// Submit first approval for a two-party operation.
    /// Returns `true` if this is the first approval (pending second).
    /// Returns `false` if this is the second approval (operation may proceed).
    pub fn submit_two_party_approval(&self, operation: &str, approver: &str) -> bool {
        let now = now_secs();
        let op_key = operation.to_uppercase();

        let mut pending = self
            .two_party
            .pending
            .write()
            .unwrap_or_else(|p| p.into_inner());

        // Check for existing pending approval.
        if let Some((first_approver, ts)) = pending.get(&op_key).cloned() {
            // Expire after 5 minutes.
            if now - ts > 300 {
                pending.remove(&op_key);
                pending.insert(op_key, (approver.into(), now));
                return true;
            }
            if first_approver == approver {
                return true; // Same person — still pending.
            }
            // Different approver — two-party satisfied.
            pending.remove(&op_key);
            info!(
                operation = %operation,
                first = %first_approver,
                second = %approver,
                "two-party authorization satisfied"
            );
            return false; // Proceed.
        }

        // First approval.
        pending.insert(op_key, (approver.into(), now));
        true // Pending second approval.
    }
}

impl Default for EmergencyState {
    fn default() -> Self {
        Self::new(None, TwoPartyConfig::default())
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lockdown_lifecycle() {
        let state = EmergencyState::default();
        assert!(!state.is_locked_down());

        state.lockdown("security incident");
        assert!(state.is_locked_down());
        assert_eq!(state.lockdown_reason(), "security incident");

        state.unlock();
        assert!(!state.is_locked_down());
    }

    #[test]
    fn two_party_auth() {
        let state = EmergencyState::default();

        // First approval — pending.
        assert!(state.submit_two_party_approval("EMERGENCY LOCKDOWN", "admin_alice"));

        // Same person — still pending.
        assert!(state.submit_two_party_approval("EMERGENCY LOCKDOWN", "admin_alice"));

        // Different person — satisfied.
        assert!(!state.submit_two_party_approval("EMERGENCY LOCKDOWN", "admin_bob"));
    }

    #[test]
    fn break_glass_no_file() {
        let state = EmergencyState::default();
        assert!(!state.validate_break_glass("any_key"));
    }

    #[test]
    fn requires_two_party_check() {
        let state = EmergencyState::default();
        assert!(state.requires_two_party("EMERGENCY LOCKDOWN REASON 'test'"));
        assert!(state.requires_two_party("IMPERSONATE AUTH USER 'x'"));
        assert!(!state.requires_two_party("SELECT * FROM users"));
    }
}
