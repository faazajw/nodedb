//! `StorageEngine` trait: the async key-value blob interface.
//!
//! All persistent storage on the edge goes through this trait. SQLite
//! (native) and OPFS (WASM) are the two backends. The engines above
//! (HNSW, CSR, Loro) serialize their data to opaque blobs and store them
//! here. SQLite/OPFS never interprets the data.

use async_trait::async_trait;

use crate::error::LiteError;
use nodedb_types::Namespace;

/// A write operation for batch writes.
#[derive(Debug, Clone)]
pub enum WriteOp {
    /// Insert or update a key-value pair.
    Put {
        ns: Namespace,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// Delete a key.
    Delete { ns: Namespace, key: Vec<u8> },
}

/// Async key-value blob storage backend.
///
/// Implementations must be `Send + Sync + 'static` to be shareable across
/// async tasks and engine threads.
///
/// All operations are keyed by `(Namespace, key)`. Values are opaque byte
/// slices — the storage layer never interprets them.
#[async_trait]
pub trait StorageEngine: Send + Sync + 'static {
    /// Get a value by namespace and key.
    ///
    /// Returns `None` if the key does not exist.
    async fn get(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError>;

    /// Put (insert or overwrite) a value.
    async fn put(&self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), LiteError>;

    /// Delete a key. No-op if the key does not exist.
    async fn delete(&self, ns: Namespace, key: &[u8]) -> Result<(), LiteError>;

    /// Scan all keys with a given prefix in a namespace.
    ///
    /// Returns `(key, value)` pairs ordered by key. The prefix match is
    /// bytewise: `key.starts_with(prefix)`.
    ///
    /// If `prefix` is empty, returns all entries in the namespace.
    async fn scan_prefix(
        &self,
        ns: Namespace,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, LiteError>;

    /// Atomically apply a batch of writes.
    ///
    /// All operations in the batch succeed or fail together (transaction).
    /// This is the primary write path for engines that need to persist
    /// multiple related blobs atomically (e.g., HNSW node + metadata).
    async fn batch_write(&self, ops: &[WriteOp]) -> Result<(), LiteError>;

    /// Count the number of entries in a namespace.
    ///
    /// Useful for cold-start progress reporting and memory governor decisions.
    async fn count(&self, ns: Namespace) -> Result<u64, LiteError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_op_debug() {
        let op = WriteOp::Put {
            ns: Namespace::Vector,
            key: vec![1, 2],
            value: vec![3, 4],
        };
        let dbg = format!("{op:?}");
        assert!(dbg.contains("Put"));
        assert!(dbg.contains("Vector"));
    }
}
