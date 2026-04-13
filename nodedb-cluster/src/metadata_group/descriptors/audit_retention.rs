//! Audit retention policy descriptor.

use serde::{Deserialize, Serialize};

use crate::metadata_group::descriptors::common::DescriptorHeader;

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
pub struct AuditRetentionDescriptor {
    pub header: DescriptorHeader,
    pub target: AuditTarget,
    pub retention_days: u32,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum AuditTarget {
    Ddl,
    Auth,
    Query,
    All,
}
