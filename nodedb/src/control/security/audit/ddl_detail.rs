//! Structured detail body for `AuditEvent::DdlChange`.

/// Structured detail for a `AuditEvent::DdlChange` record. Produced
/// by `MetadataCommitApplier` once a metadata entry reaches the
/// apply watermark on this node. Serialized as JSON into the
/// `AuditEntry::detail` field so existing audit queries that pretty-
/// print the detail keep working.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DdlAuditDetail {
    pub descriptor_kind: String,
    pub descriptor_name: String,
    pub version_before: u64,
    pub version_after: u64,
    /// HLC stamped onto the descriptor at commit time, encoded as the
    /// same string form used elsewhere (`Display` on `Hlc`).
    pub hlc: String,
    /// Raft log index of the committed entry.
    pub raft_index: u64,
    /// Raw SQL statement as sent by the client. Empty when the DDL
    /// originated internally (lease grant, drain proposer) and no
    /// pgwire audit context was installed.
    pub sql_statement: String,
}
