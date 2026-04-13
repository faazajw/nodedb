//! Descriptor lease record.

use nodedb_types::Hlc;
use serde::{Deserialize, Serialize};

use crate::metadata_group::descriptors::common::DescriptorId;

/// A per-node lease over a specific `(descriptor_id, version)` pair.
///
/// Leases let a node safely cache and plan against a descriptor version
/// for a bounded window (5 minutes by default). Schema changes wait for
/// prior-version leases to drain before bumping the descriptor version.
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
pub struct DescriptorLease {
    pub descriptor_id: DescriptorId,
    pub version: u64,
    pub node_id: u64,
    pub expires_at: Hlc,
}
