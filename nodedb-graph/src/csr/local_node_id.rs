//! Partition-tagged node identifier for cross-partition safety.
//!
//! Each `CsrIndex` is assigned a unique `partition_tag` at construction
//! from a process-global atomic counter. A `LocalNodeId` carries both
//! a dense node index and the tag of the partition that produced it;
//! using one from partition A with a method on partition B panics.

use std::sync::atomic::{AtomicU32, Ordering};

static PARTITION_COUNTER: AtomicU32 = AtomicU32::new(1);

/// Allocate the next unique partition tag. Called once per `CsrIndex`
/// construction.
pub(crate) fn next_partition_tag() -> u32 {
    PARTITION_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// A dense node index bound to the partition that produced it.
///
/// Constructed only by `CsrIndex` / `CsrSnapshot` read APIs. The
/// partition tag is checked at every consuming API; passing an ID
/// from a different partition panics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LocalNodeId {
    raw: u32,
    partition: u32,
}

impl LocalNodeId {
    /// Construct a tagged node id.
    ///
    /// Callers outside `nodedb-graph` that need to mint a `LocalNodeId`
    /// (e.g. `CsrSnapshot`) must pass the partition tag they inherited
    /// from the source `CsrIndex`. Using a tag from one partition with
    /// the API of another will panic on the first `.raw(expected)` call.
    #[inline]
    pub fn new(raw: u32, partition: u32) -> Self {
        Self { raw, partition }
    }

    /// Partition tag this id belongs to.
    #[inline]
    pub fn partition(self) -> u32 {
        self.partition
    }

    /// Unwrap to the raw dense index, asserting the id was produced by
    /// the expected partition. Panics on tag mismatch — this catches
    /// cross-partition id leakage at the call site.
    #[inline]
    #[track_caller]
    pub fn raw(self, expected_partition: u32) -> u32 {
        assert_eq!(
            self.partition, expected_partition,
            "LocalNodeId from partition {} used on partition {}",
            self.partition, expected_partition
        );
        self.raw
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_counter_is_monotonic() {
        let a = next_partition_tag();
        let b = next_partition_tag();
        assert!(b > a);
    }

    #[test]
    fn raw_with_matching_partition_returns_id() {
        let id = LocalNodeId::new(42, 7);
        assert_eq!(id.raw(7), 42);
    }

    #[test]
    #[should_panic(expected = "partition 7 used on partition 9")]
    fn raw_with_wrong_partition_panics() {
        let id = LocalNodeId::new(42, 7);
        let _ = id.raw(9);
    }

    #[test]
    fn tagged_ids_are_copy_and_eq() {
        let id = LocalNodeId::new(1, 1);
        let copy = id;
        assert_eq!(id, copy);
    }
}
