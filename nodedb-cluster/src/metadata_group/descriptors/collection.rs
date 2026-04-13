//! Collection descriptor.

use serde::{Deserialize, Serialize};

use crate::metadata_group::descriptors::common::DescriptorHeader;

/// Schema-replicated collection descriptor.
///
/// Mirrors the runtime collection shape (engine type, columns, with-options)
/// but is the authoritative source of truth stored in the raft log. The
/// local `SystemCatalog` redb is a materialized view of committed entries.
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
pub struct CollectionDescriptor {
    pub header: DescriptorHeader,
    /// Engine type encoded as the canonical CollectionType string form.
    /// (Stored as string rather than the typed `CollectionType` to avoid a
    /// `nodedb-cluster -> nodedb-types` dependency cycle at the type level;
    /// the nodedb crate decodes this back to `CollectionType` when
    /// materializing into the local catalog.)
    pub collection_type: String,
    /// Column definitions in declaration order.
    pub columns: Vec<ColumnDef>,
    /// WITH (...) options from the CREATE COLLECTION statement.
    pub with_options: Vec<(String, String)>,
    /// Optional primary key column name.
    pub primary_key: Option<String>,
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
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default: Option<String>,
}
