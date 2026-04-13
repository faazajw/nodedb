//! Tenant descriptor.

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
pub struct TenantDescriptor {
    pub header: DescriptorHeader,
    pub display_name: String,
    pub quota_bytes: Option<u64>,
    pub quota_row_count: Option<u64>,
    pub region: Option<String>,
    pub enabled: bool,
}
