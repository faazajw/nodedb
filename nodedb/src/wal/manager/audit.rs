use super::core::WalManager;

impl WalManager {
    /// Write an audit entry durably to the dedicated audit WAL.
    ///
    /// `data_lsn` is the data WAL LSN this audit entry corresponds to.
    /// The audit entry is serialized, appended, and fsynced before returning.
    /// If the audit WAL is not available, the entry is silently dropped
    /// (logged at warn level).
    pub fn append_audit_durable(&self, audit_bytes: &[u8], data_lsn: u64) -> crate::Result<()> {
        if let Some(ref audit_wal) = self.audit_wal {
            audit_wal.append_durable(audit_bytes, data_lsn)?;
        }
        Ok(())
    }

    /// Recover all audit WAL entries for crash recovery.
    ///
    /// Returns `(data_lsn, audit_entry_bytes)` pairs in LSN order.
    pub fn recover_audit_entries(&self) -> crate::Result<Vec<(u64, Vec<u8>)>> {
        if let Some(ref audit_wal) = self.audit_wal {
            audit_wal.recover()
        } else {
            Ok(Vec::new())
        }
    }

    /// Whether the durable audit WAL is available.
    pub fn has_audit_wal(&self) -> bool {
        self.audit_wal.is_some()
    }
}
