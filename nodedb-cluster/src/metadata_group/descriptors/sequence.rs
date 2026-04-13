//! Sequence descriptor.

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
pub struct SequenceDescriptor {
    pub header: DescriptorHeader,
    pub start_with: i64,
    pub increment_by: i64,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cycle: bool,
    pub cache_size: u32,
}
