//! SQLite-backed `StorageEngine` implementation.
//!
//! Uses SQLite as a dumb, battery-friendly, ACID-compliant blob KV store.
//! The schema is a single `kv` table with `(ns, key) -> val` mapping.
//! SQLite handles file locking, WAL journaling, and crash recovery.
//!
//! All synchronous `rusqlite` calls are dispatched via `spawn_blocking`
//! to avoid stalling the async runtime.

use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::{Connection, OpenFlags, params};

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};
use nodedb_types::Namespace;

/// SQLite-backed blob KV store.
///
/// Thread-safety: `Connection` is `Send` but not `Sync`. We wrap it in
/// `Mutex` and dispatch all I/O through `spawn_blocking` so the async
/// runtime is never blocked by disk I/O or SQLite locks.
pub struct SqliteStorage {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteStorage {
    /// Open or create a SQLite database at the given path.
    ///
    /// Configures WAL journal mode for concurrent reads during writes,
    /// and creates the `kv` table if it doesn't exist.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LiteError> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;

        Self::configure_and_init(conn)
    }

    /// Create an in-memory SQLite database (for testing).
    pub fn open_in_memory() -> Result<Self, LiteError> {
        let conn = Connection::open_in_memory()?;
        Self::configure_and_init(conn)
    }

    /// Configure pragmas and create the schema.
    fn configure_and_init(conn: Connection) -> Result<Self, LiteError> {
        // WAL mode: allows concurrent readers while a writer holds the lock.
        // This is critical for edge devices where the sync thread writes
        // while query threads read.
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = -2000;
             PRAGMA busy_timeout = 5000;",
        )?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS kv (
                ns   INTEGER NOT NULL,
                key  BLOB    NOT NULL,
                val  BLOB    NOT NULL,
                PRIMARY KEY (ns, key)
            ) WITHOUT ROWID;",
        )?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Acquire the connection lock, mapping poison errors.
    fn lock_conn(
        conn: &Mutex<Connection>,
    ) -> Result<std::sync::MutexGuard<'_, Connection>, LiteError> {
        conn.lock().map_err(|_| LiteError::LockPoisoned)
    }

    /// Run a blocking closure on the `spawn_blocking` thread pool.
    #[cfg(not(target_arch = "wasm32"))]
    async fn blocking<F, T>(&self, f: F) -> Result<T, LiteError>
    where
        F: FnOnce(&Connection) -> Result<T, LiteError> + Send + 'static,
        T: Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let guard = Self::lock_conn(&conn)?;
            f(&guard)
        })
        .await
        .map_err(|e| LiteError::JoinError {
            detail: e.to_string(),
        })?
    }
}

#[async_trait]
impl StorageEngine for SqliteStorage {
    async fn get(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
        let ns_u8 = ns as u8;
        let key = key.to_vec();

        self.blocking(move |conn| {
            let mut stmt = conn.prepare_cached("SELECT val FROM kv WHERE ns = ?1 AND key = ?2")?;
            let result = stmt.query_row(params![ns_u8, key], |row| row.get::<_, Vec<u8>>(0));

            match result {
                Ok(val) => Ok(Some(val)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(LiteError::from(e)),
            }
        })
        .await
    }

    async fn put(&self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), LiteError> {
        let ns_u8 = ns as u8;
        let key = key.to_vec();
        let value = value.to_vec();

        self.blocking(move |conn| {
            conn.execute(
                "INSERT OR REPLACE INTO kv (ns, key, val) VALUES (?1, ?2, ?3)",
                params![ns_u8, key, value],
            )?;
            Ok(())
        })
        .await
    }

    async fn delete(&self, ns: Namespace, key: &[u8]) -> Result<(), LiteError> {
        let ns_u8 = ns as u8;
        let key = key.to_vec();

        self.blocking(move |conn| {
            conn.execute(
                "DELETE FROM kv WHERE ns = ?1 AND key = ?2",
                params![ns_u8, key],
            )?;
            Ok(())
        })
        .await
    }

    async fn scan_prefix(
        &self,
        ns: Namespace,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, LiteError> {
        let ns_u8 = ns as u8;
        let prefix = prefix.to_vec();

        self.blocking(move |conn| {
            if prefix.is_empty() {
                // No prefix filter — return all entries in the namespace.
                let mut stmt =
                    conn.prepare_cached("SELECT key, val FROM kv WHERE ns = ?1 ORDER BY key")?;
                let rows = stmt
                    .query_map(params![ns_u8], |row| {
                        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(rows);
            }

            // Prefix scan using range query: key >= prefix AND key < prefix_upper_bound.
            // The upper bound is the prefix with the last byte incremented (with carry).
            // This uses the index efficiently instead of LIKE or full table scan.
            let upper = prefix_upper_bound(&prefix);

            match upper {
                Some(upper) => {
                    let mut stmt = conn.prepare_cached(
                        "SELECT key, val FROM kv WHERE ns = ?1 AND key >= ?2 AND key < ?3 ORDER BY key",
                    )?;
                    let rows = stmt
                        .query_map(params![ns_u8, prefix, upper], |row| {
                            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
                        })?
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(rows)
                }
                None => {
                    // Prefix is all 0xFF bytes — no upper bound, scan to end.
                    let mut stmt = conn.prepare_cached(
                        "SELECT key, val FROM kv WHERE ns = ?1 AND key >= ?2 ORDER BY key",
                    )?;
                    let rows = stmt
                        .query_map(params![ns_u8, prefix], |row| {
                            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
                        })?
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(rows)
                }
            }
        })
        .await
    }

    async fn batch_write(&self, ops: &[WriteOp]) -> Result<(), LiteError> {
        let ops = ops.to_vec();

        self.blocking(move |conn| {
            let tx = conn.unchecked_transaction()?;

            for op in &ops {
                match op {
                    WriteOp::Put { ns, key, value } => {
                        tx.execute(
                            "INSERT OR REPLACE INTO kv (ns, key, val) VALUES (?1, ?2, ?3)",
                            params![*ns as u8, key, value],
                        )?;
                    }
                    WriteOp::Delete { ns, key } => {
                        tx.execute(
                            "DELETE FROM kv WHERE ns = ?1 AND key = ?2",
                            params![*ns as u8, key],
                        )?;
                    }
                }
            }

            tx.commit()?;
            Ok(())
        })
        .await
    }

    async fn count(&self, ns: Namespace) -> Result<u64, LiteError> {
        let ns_u8 = ns as u8;

        self.blocking(move |conn| {
            let mut stmt = conn.prepare_cached("SELECT COUNT(*) FROM kv WHERE ns = ?1")?;
            let count: u64 = stmt.query_row(params![ns_u8], |row| row.get(0))?;
            Ok(count)
        })
        .await
    }
}

/// Compute the exclusive upper bound for a prefix scan.
///
/// Increments the last byte of the prefix. If it overflows (0xFF),
/// carries to the previous byte. Returns `None` if the entire prefix
/// is 0xFF (meaning scan to the end of the keyspace).
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for i in (0..upper.len()).rev() {
        if upper[i] < 0xFF {
            upper[i] += 1;
            upper.truncate(i + 1);
            return Some(upper);
        }
    }
    // All bytes are 0xFF — no upper bound exists.
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_storage() -> SqliteStorage {
        SqliteStorage::open_in_memory().unwrap()
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let s = make_storage();
        s.put(Namespace::Vector, b"v1", b"hello").await.unwrap();

        let val = s.get(Namespace::Vector, b"v1").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"hello".as_slice()));
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let s = make_storage();
        let val = s.get(Namespace::Vector, b"nonexistent").await.unwrap();
        assert!(val.is_none());
    }

    #[tokio::test]
    async fn put_overwrites() {
        let s = make_storage();
        s.put(Namespace::Graph, b"k1", b"first").await.unwrap();
        s.put(Namespace::Graph, b"k1", b"second").await.unwrap();

        let val = s.get(Namespace::Graph, b"k1").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"second".as_slice()));
    }

    #[tokio::test]
    async fn delete_removes_key() {
        let s = make_storage();
        s.put(Namespace::Crdt, b"k1", b"val").await.unwrap();
        s.delete(Namespace::Crdt, b"k1").await.unwrap();

        let val = s.get(Namespace::Crdt, b"k1").await.unwrap();
        assert!(val.is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_is_noop() {
        let s = make_storage();
        // Should not error.
        s.delete(Namespace::Meta, b"ghost").await.unwrap();
    }

    #[tokio::test]
    async fn namespaces_are_isolated() {
        let s = make_storage();
        s.put(Namespace::Vector, b"k1", b"vec").await.unwrap();
        s.put(Namespace::Graph, b"k1", b"graph").await.unwrap();

        let v = s.get(Namespace::Vector, b"k1").await.unwrap();
        let g = s.get(Namespace::Graph, b"k1").await.unwrap();

        assert_eq!(v.as_deref(), Some(b"vec".as_slice()));
        assert_eq!(g.as_deref(), Some(b"graph".as_slice()));
    }

    #[tokio::test]
    async fn scan_prefix_basic() {
        let s = make_storage();
        s.put(Namespace::Vector, b"vec:001", b"a").await.unwrap();
        s.put(Namespace::Vector, b"vec:002", b"b").await.unwrap();
        s.put(Namespace::Vector, b"vec:003", b"c").await.unwrap();
        s.put(Namespace::Vector, b"other:001", b"d").await.unwrap();

        let results = s.scan_prefix(Namespace::Vector, b"vec:").await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, b"vec:001");
        assert_eq!(results[1].0, b"vec:002");
        assert_eq!(results[2].0, b"vec:003");
    }

    #[tokio::test]
    async fn scan_prefix_empty_returns_all() {
        let s = make_storage();
        s.put(Namespace::Meta, b"a", b"1").await.unwrap();
        s.put(Namespace::Meta, b"b", b"2").await.unwrap();
        s.put(Namespace::Vector, b"c", b"3").await.unwrap();

        let results = s.scan_prefix(Namespace::Meta, b"").await.unwrap();
        assert_eq!(results.len(), 2); // Only Meta namespace.
    }

    #[tokio::test]
    async fn scan_prefix_no_match() {
        let s = make_storage();
        s.put(Namespace::Graph, b"edge:1", b"data").await.unwrap();

        let results = s.scan_prefix(Namespace::Graph, b"node:").await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn batch_write_atomic() {
        let s = make_storage();
        s.put(Namespace::Crdt, b"to_delete", b"old").await.unwrap();

        s.batch_write(&[
            WriteOp::Put {
                ns: Namespace::Crdt,
                key: b"new1".to_vec(),
                value: b"val1".to_vec(),
            },
            WriteOp::Put {
                ns: Namespace::Crdt,
                key: b"new2".to_vec(),
                value: b"val2".to_vec(),
            },
            WriteOp::Delete {
                ns: Namespace::Crdt,
                key: b"to_delete".to_vec(),
            },
        ])
        .await
        .unwrap();

        assert!(s.get(Namespace::Crdt, b"new1").await.unwrap().is_some());
        assert!(s.get(Namespace::Crdt, b"new2").await.unwrap().is_some());
        assert!(
            s.get(Namespace::Crdt, b"to_delete")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn batch_write_empty_is_noop() {
        let s = make_storage();
        s.batch_write(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn count_entries() {
        let s = make_storage();
        assert_eq!(s.count(Namespace::Vector).await.unwrap(), 0);

        s.put(Namespace::Vector, b"v1", b"a").await.unwrap();
        s.put(Namespace::Vector, b"v2", b"b").await.unwrap();
        s.put(Namespace::Graph, b"g1", b"c").await.unwrap();

        assert_eq!(s.count(Namespace::Vector).await.unwrap(), 2);
        assert_eq!(s.count(Namespace::Graph).await.unwrap(), 1);
        assert_eq!(s.count(Namespace::Crdt).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn large_value_roundtrip() {
        let s = make_storage();
        // 1 MB blob — simulates a serialized HNSW layer.
        let large = vec![0xABu8; 1_000_000];
        s.put(Namespace::Vector, b"hnsw:layer0", &large)
            .await
            .unwrap();

        let val = s.get(Namespace::Vector, b"hnsw:layer0").await.unwrap();
        assert_eq!(val.unwrap().len(), 1_000_000);
    }

    #[tokio::test]
    async fn binary_keys_work() {
        let s = make_storage();
        // Keys with null bytes and high bytes.
        let key = vec![0x00, 0x01, 0xFF, 0xFE];
        s.put(Namespace::LoroState, &key, b"binary_key_val")
            .await
            .unwrap();

        let val = s.get(Namespace::LoroState, &key).await.unwrap();
        assert_eq!(val.as_deref(), Some(b"binary_key_val".as_slice()));
    }

    #[tokio::test]
    async fn prefix_upper_bound_basic() {
        assert_eq!(prefix_upper_bound(b"abc"), Some(b"abd".to_vec()));
        assert_eq!(prefix_upper_bound(b"a\xff"), Some(b"b".to_vec()));
        assert_eq!(prefix_upper_bound(b"\xff\xff\xff"), None);
        assert_eq!(prefix_upper_bound(b""), None);
    }

    #[tokio::test]
    async fn open_file_based() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Write data, drop, reopen, verify persistence.
        {
            let s = SqliteStorage::open(&path).unwrap();
            s.put(Namespace::Meta, b"key", b"persistent").await.unwrap();
        }
        {
            let s = SqliteStorage::open(&path).unwrap();
            let val = s.get(Namespace::Meta, b"key").await.unwrap();
            assert_eq!(val.as_deref(), Some(b"persistent".as_slice()));
        }
    }
}
