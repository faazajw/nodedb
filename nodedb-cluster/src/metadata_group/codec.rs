//! Serialize / deserialize helpers for [`MetadataEntry`].
//!
//! All entries flow through `zerompk` (MessagePack) for a stable,
//! versioned wire format.

use crate::error::ClusterError;
use crate::metadata_group::entry::MetadataEntry;

pub fn encode_entry(entry: &MetadataEntry) -> Result<Vec<u8>, ClusterError> {
    zerompk::to_msgpack_vec(entry).map_err(|e| ClusterError::Codec {
        detail: format!("metadata encode: {e}"),
    })
}

pub fn decode_entry(data: &[u8]) -> Result<MetadataEntry, ClusterError> {
    zerompk::from_msgpack(data).map_err(|e| ClusterError::Codec {
        detail: format!("metadata decode: {e}"),
    })
}
