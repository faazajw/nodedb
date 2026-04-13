//! Index descriptor.

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
pub struct IndexDescriptor {
    pub header: DescriptorHeader,
    /// Target collection name within the same tenant.
    pub collection: String,
    pub index_kind: IndexKind,
    pub columns: Vec<String>,
    pub unique: bool,
    pub predicate: Option<String>,
    pub with_options: Vec<(String, String)>,
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
pub enum IndexKind {
    BTree,
    Hash,
    Vector,
    Fts,
    Spatial,
    Graph,
}
