//! Role descriptor.

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
pub struct RoleDescriptor {
    pub header: DescriptorHeader,
    pub role_name: String,
    pub inherits: Vec<String>,
    pub description: Option<String>,
}
