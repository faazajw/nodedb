//! Audit-level filter applied to the recording path.

/// Audit level: controls which events are recorded.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum AuditLevel {
    /// Auth success/failure, privilege changes only.
    Minimal = 0,
    /// + admin actions, config changes, DDL (default).
    #[default]
    Standard = 1,
    /// + every query execution, RLS denials.
    Full = 2,
    /// + row-level changes, CRDT deltas, full request/response.
    Forensic = 3,
}
