//! Lifecycle state of a schema descriptor.
//!
//! The `DescriptorState` enum is the one-step-at-a-time state machine that
//! lets in-flight DML observe a safe view of the schema during online DDL.
//! Every descriptor carries its current state; the
//! [`crate::metadata_group::cache::MetadataCache`] exposes the state to
//! planners so they can reject plans that would observe a partially-applied
//! change.

use serde::{Deserialize, Serialize};

/// Lifecycle state of a descriptor.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum DescriptorState {
    /// Fully visible to reads and writes.
    Public,
    /// A new column/field is being added at the given descriptor version.
    AddingField { since_version: u64 },
    /// A column/field is being removed at the given descriptor version.
    DroppingField { since_version: u64 },
    /// The entire descriptor is being dropped; visible only for in-flight
    /// queries planned before `since_version`.
    Dropping { since_version: u64 },
    /// An index or MV is being rebuilt offline.
    OfflineRebuilding,
}

impl DescriptorState {
    pub fn is_public(&self) -> bool {
        matches!(self, DescriptorState::Public)
    }
}
