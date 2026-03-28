//! KV collection operations for Lite: PUT/GET/DELETE via CRDT engine.
//!
//! KV writes go through the CRDT engine's `upsert`/`delete` path so deltas
//! are produced for edge-to-cloud sync. The KV collection name is prefixed
//! with `_kv_` in the CRDT namespace to separate from document collections.
//! LWW conflict resolution applies by default (latest PUT wins).

use nodedb_types::error::{NodeDbError, NodeDbResult};

use super::super::{LockExt, NodeDbLite};
use crate::storage::engine::StorageEngine;

/// Prefix for KV collection names in the CRDT namespace.
const KV_CRDT_PREFIX: &str = "_kv_";

impl<S: StorageEngine> NodeDbLite<S> {
    /// KV PUT: store a key-value pair with CRDT delta production.
    ///
    /// The value is stored as a hex-encoded string in the CRDT document
    /// (Loro String type syncs cleanly across peers).
    /// The primary key is the CRDT document ID.
    pub fn kv_put(&self, collection: &str, key: &str, value: &[u8]) -> NodeDbResult<()> {
        let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
        let mut crdt = self.crdt.lock_or_recover();

        let value_encoded = bytes_to_hex(value);
        let fields: Vec<(&str, loro::LoroValue)> =
            vec![("value", loro::LoroValue::String(value_encoded.into()))];
        crdt.upsert(&crdt_collection, key, &fields)
            .map_err(NodeDbError::storage)?;

        Ok(())
    }

    /// KV GET: retrieve a value by key.
    pub fn kv_get(&self, collection: &str, key: &str) -> NodeDbResult<Option<Vec<u8>>> {
        let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
        let crdt = self.crdt.lock_or_recover();

        match crdt.read(&crdt_collection, key) {
            Some(loro::LoroValue::Map(map)) => {
                if let Some(loro::LoroValue::String(encoded)) = map.get("value") {
                    let bytes = hex_to_bytes(encoded)
                        .map_err(|e| NodeDbError::storage(format!("kv decode: {e}")))?;
                    Ok(Some(bytes))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    /// KV DELETE: remove a key with CRDT delta production.
    pub fn kv_delete(&self, collection: &str, key: &str) -> NodeDbResult<bool> {
        let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
        let mut crdt = self.crdt.lock_or_recover();
        crdt.delete(&crdt_collection, key)
            .map_err(NodeDbError::storage)?;
        Ok(true)
    }

    /// List all keys in a KV collection.
    pub fn kv_keys(&self, collection: &str) -> NodeDbResult<Vec<String>> {
        let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
        let crdt = self.crdt.lock_or_recover();
        Ok(crdt.list_ids(&crdt_collection))
    }
}

/// Encode bytes as lowercase hex string.
fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(char::from(b"0123456789abcdef"[(b >> 4) as usize]));
        s.push(char::from(b"0123456789abcdef"[(b & 0x0f) as usize]));
    }
    s
}

/// Decode hex string to bytes.
fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, String> {
    if !hex.len().is_multiple_of(2) {
        return Err("odd-length hex string".into());
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let chars: Vec<u8> = hex.bytes().collect();
    for pair in chars.chunks(2) {
        let hi = hex_digit(pair[0]).ok_or_else(|| format!("invalid hex digit: {}", pair[0]))?;
        let lo = hex_digit(pair[1]).ok_or_else(|| format!("invalid hex digit: {}", pair[1]))?;
        bytes.push((hi << 4) | lo);
    }
    Ok(bytes)
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
