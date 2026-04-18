//! Authenticated identity context attached to every audit entry.

/// Auth context for enriched audit entries.
#[derive(Debug, Clone, Default)]
pub struct AuditAuth {
    /// Authenticated user ID.
    pub user_id: String,
    /// Authenticated username (for display).
    pub user_name: String,
    /// Session ID for correlation.
    pub session_id: String,
}
