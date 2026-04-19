use tracing::info;

use super::core::WalManager;
use crate::types::Lsn;

impl WalManager {
    /// Truncate old WAL segments that are fully below the checkpoint LSN.
    ///
    /// Deletes sealed segment files whose records are all below `checkpoint_lsn`.
    /// The active segment is never deleted. Safe to call only after a checkpoint
    /// has been confirmed — all engines have flushed their dirty pages.
    pub fn truncate_before(
        &self,
        checkpoint_lsn: Lsn,
    ) -> crate::Result<nodedb_wal::segment::TruncateResult> {
        let wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let result = wal
            .truncate_before(checkpoint_lsn.as_u64())
            .map_err(crate::Error::Wal)?;

        if result.segments_deleted > 0 {
            info!(
                checkpoint_lsn = checkpoint_lsn.as_u64(),
                segments_deleted = result.segments_deleted,
                bytes_reclaimed = result.bytes_reclaimed,
                "WAL truncated"
            );
        }

        Ok(result)
    }

    /// Flush all buffered records to disk (group commit / fsync).
    pub fn sync(&self) -> crate::Result<()> {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        wal.sync().map_err(crate::Error::Wal)
    }

    /// Next LSN that will be assigned.
    pub fn next_lsn(&self) -> Lsn {
        let wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        Lsn::new(wal.next_lsn())
    }

    /// Total WAL size on disk across all segments.
    pub fn total_size_bytes(&self) -> crate::Result<u64> {
        let wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        wal.total_size_bytes().map_err(crate::Error::Wal)
    }

    /// List all WAL segment metadata (for monitoring).
    pub fn list_segments(&self) -> crate::Result<Vec<nodedb_wal::segment::SegmentMeta>> {
        let wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        wal.list_segments().map_err(crate::Error::Wal)
    }
}
