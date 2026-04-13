//! Materialized view descriptor.

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
pub struct MaterializedViewDescriptor {
    pub header: DescriptorHeader,
    pub query_sql: String,
    pub target_collection_type: String,
    pub refresh: MvRefreshPolicy,
    pub source_collections: Vec<String>,
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
pub enum MvRefreshPolicy {
    Manual,
    OnCommit,
    Incremental,
    Scheduled { cron: String },
}
