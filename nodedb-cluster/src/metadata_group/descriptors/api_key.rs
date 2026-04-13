//! API key descriptor.

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
pub struct ApiKeyDescriptor {
    pub header: DescriptorHeader,
    pub key_id: String,
    /// Argon2 hash of the secret portion.
    pub secret_hash: String,
    pub username: String,
    pub scopes: Vec<String>,
    pub expires_at_wall_ns: Option<u64>,
    pub revoked: bool,
}
