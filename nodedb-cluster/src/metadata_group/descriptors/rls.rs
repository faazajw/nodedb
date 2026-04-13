//! Row-Level Security policy descriptor.

use serde::{Deserialize, Serialize};

use crate::metadata_group::descriptors::common::DescriptorHeader;
use crate::metadata_group::descriptors::grant::GrantPrincipal;

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
pub struct RlsDescriptor {
    pub header: DescriptorHeader,
    pub collection: String,
    pub policy_name: String,
    pub permissive: bool,
    pub applies_to: Vec<GrantPrincipal>,
    pub using_expr: Option<String>,
    pub with_check_expr: Option<String>,
    pub commands: Vec<RlsCommand>,
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
pub enum RlsCommand {
    Select,
    Insert,
    Update,
    Delete,
    All,
}
