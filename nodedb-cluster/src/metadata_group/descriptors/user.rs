//! User descriptor.

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
pub struct UserDescriptor {
    pub header: DescriptorHeader,
    pub username: String,
    /// Argon2 password hash or external identity provider reference.
    pub credential: UserCredential,
    pub roles: Vec<String>,
    pub enabled: bool,
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
pub enum UserCredential {
    PasswordHash(String),
    OAuth { issuer: String, subject: String },
    ApiKey { key_id: String },
    External,
}
