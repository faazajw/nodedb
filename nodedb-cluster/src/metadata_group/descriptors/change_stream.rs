//! Change-stream (CDC) descriptor.

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
pub struct ChangeStreamDescriptor {
    pub header: DescriptorHeader,
    pub collection: String,
    pub op_filter: ChangeOpFilter,
    pub retention_seconds: u64,
    pub max_bytes: u64,
    pub include_before: bool,
    pub include_after: bool,
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
pub enum ChangeOpFilter {
    All,
    InsertOnly,
    UpdateOnly,
    DeleteOnly,
    InsertAndUpdate,
}
