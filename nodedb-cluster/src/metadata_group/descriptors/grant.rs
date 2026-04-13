//! Grant (privilege assignment) descriptor.

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
pub struct GrantDescriptor {
    pub header: DescriptorHeader,
    pub grantee: GrantPrincipal,
    pub privilege: Privilege,
    pub resource: GrantResource,
    pub with_grant_option: bool,
}

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
pub enum GrantPrincipal {
    User(String),
    Role(String),
    Public,
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
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Truncate,
    Usage,
    Execute,
    All,
}

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
pub enum GrantResource {
    Collection(String),
    Tenant,
    Function(String),
    Sequence(String),
    Schema(String),
}
