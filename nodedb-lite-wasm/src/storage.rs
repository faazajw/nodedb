//! SQLite WASM storage backend using `sqlite-wasm-rs`.
//!
//! Same blob KV schema as the native `SqliteStorage`:
//! ```sql
//! CREATE TABLE kv (ns INTEGER NOT NULL, key BLOB NOT NULL, val BLOB NOT NULL,
//!                  PRIMARY KEY (ns, key)) WITHOUT ROWID;
//! ```
//!
//! Uses `sqlite-wasm-rs` raw C API (compiled to WASM). On WASM there's
//! only one thread, so no `spawn_blocking` — operations run synchronously.
//!
//! For OPFS persistence, `sqlite-wasm-vfs` provides the SyncAccessHandlePool VFS.

use std::sync::Mutex;

use async_trait::async_trait;
use sqlite_wasm_rs::sqlite3;

use nodedb_lite::error::LiteError;
use nodedb_lite::storage::engine::{StorageEngine, WriteOp};
use nodedb_types::Namespace;

/// SQLite WASM storage backend.
///
/// Single-threaded — `Mutex` is for `Send + Sync` compliance only.
pub struct WasmSqliteStorage {
    db: Mutex<*mut sqlite3>,
}

// SAFETY: WASM is single-threaded. The Mutex is never contended.
unsafe impl Send for WasmSqliteStorage {}
unsafe impl Sync for WasmSqliteStorage {}

impl WasmSqliteStorage {
    /// Open an in-memory database.
    pub fn open_in_memory() -> Result<Self, LiteError> {
        Self::open_raw(c":memory:")
    }

    /// Open a named database (for OPFS persistence, register the VFS first).
    pub fn open(name: &str) -> Result<Self, LiteError> {
        let c_name = std::ffi::CString::new(name).map_err(|e| LiteError::Storage {
            detail: format!("invalid db name: {e}"),
        })?;
        Self::open_raw_cstring(&c_name)
    }

    fn open_raw(name: &std::ffi::CStr) -> Result<Self, LiteError> {
        Self::open_raw_cstring(name)
    }

    fn open_raw_cstring(name: &std::ffi::CStr) -> Result<Self, LiteError> {
        use sqlite_wasm_rs::*;

        let mut db: *mut sqlite3 = std::ptr::null_mut();
        let rc = unsafe {
            sqlite3_open_v2(
                name.as_ptr(),
                &mut db as *mut _,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                std::ptr::null(),
            )
        };
        if rc != SQLITE_OK {
            return Err(LiteError::Storage {
                detail: format!("sqlite3_open_v2 failed: {rc}"),
            });
        }

        // Create schema.
        let sql = c"CREATE TABLE IF NOT EXISTS kv (ns INTEGER NOT NULL, key BLOB NOT NULL, val BLOB NOT NULL, PRIMARY KEY (ns, key)) WITHOUT ROWID";
        let rc = unsafe {
            sqlite3_exec(
                db,
                sql.as_ptr(),
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if rc != SQLITE_OK {
            return Err(LiteError::Storage {
                detail: format!("schema creation failed: {rc}"),
            });
        }

        // WAL mode.
        let pragma = c"PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;";
        unsafe {
            sqlite3_exec(
                db,
                pragma.as_ptr(),
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
        }

        Ok(Self { db: Mutex::new(db) })
    }

    /// Execute a query with blob parameters and return blob results.
    fn exec_get(&self, ns: u8, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
        use sqlite_wasm_rs::*;

        let db = *self.db.lock().map_err(|_| LiteError::LockPoisoned)?;
        let sql = c"SELECT val FROM kv WHERE ns = ?1 AND key = ?2";
        let mut stmt: *mut sqlite3_stmt = std::ptr::null_mut();

        unsafe {
            let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, std::ptr::null_mut());
            if rc != SQLITE_OK {
                return Err(LiteError::Storage {
                    detail: format!("prepare failed: {rc}"),
                });
            }

            sqlite3_bind_int(stmt, 1, ns as i32);
            sqlite3_bind_blob(
                stmt,
                2,
                key.as_ptr().cast(),
                key.len() as i32,
                SQLITE_TRANSIENT(),
            );

            let rc = sqlite3_step(stmt);
            if rc == SQLITE_ROW {
                let blob_ptr = sqlite3_column_blob(stmt, 0);
                let blob_len = sqlite3_column_bytes(stmt, 0) as usize;
                let val = if blob_ptr.is_null() || blob_len == 0 {
                    Vec::new()
                } else {
                    std::slice::from_raw_parts(blob_ptr.cast::<u8>(), blob_len).to_vec()
                };
                sqlite3_finalize(stmt);
                Ok(Some(val))
            } else if rc == SQLITE_DONE {
                sqlite3_finalize(stmt);
                Ok(None)
            } else {
                sqlite3_finalize(stmt);
                Err(LiteError::Storage {
                    detail: format!("step failed: {rc}"),
                })
            }
        }
    }

    fn exec_put(&self, ns: u8, key: &[u8], value: &[u8]) -> Result<(), LiteError> {
        use sqlite_wasm_rs::*;

        let db = *self.db.lock().map_err(|_| LiteError::LockPoisoned)?;
        let sql = c"INSERT OR REPLACE INTO kv (ns, key, val) VALUES (?1, ?2, ?3)";
        let mut stmt: *mut sqlite3_stmt = std::ptr::null_mut();

        unsafe {
            let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, std::ptr::null_mut());
            if rc != SQLITE_OK {
                return Err(LiteError::Storage {
                    detail: format!("prepare failed: {rc}"),
                });
            }

            sqlite3_bind_int(stmt, 1, ns as i32);
            sqlite3_bind_blob(
                stmt,
                2,
                key.as_ptr().cast(),
                key.len() as i32,
                SQLITE_TRANSIENT(),
            );
            sqlite3_bind_blob(
                stmt,
                3,
                value.as_ptr().cast(),
                value.len() as i32,
                SQLITE_TRANSIENT(),
            );

            let rc = sqlite3_step(stmt);
            sqlite3_finalize(stmt);

            if rc != SQLITE_DONE {
                return Err(LiteError::Storage {
                    detail: format!("step failed: {rc}"),
                });
            }
        }
        Ok(())
    }

    fn exec_delete(&self, ns: u8, key: &[u8]) -> Result<(), LiteError> {
        use sqlite_wasm_rs::*;

        let db = *self.db.lock().map_err(|_| LiteError::LockPoisoned)?;
        let sql = c"DELETE FROM kv WHERE ns = ?1 AND key = ?2";
        let mut stmt: *mut sqlite3_stmt = std::ptr::null_mut();

        unsafe {
            let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, std::ptr::null_mut());
            if rc != SQLITE_OK {
                return Err(LiteError::Storage {
                    detail: format!("prepare failed: {rc}"),
                });
            }

            sqlite3_bind_int(stmt, 1, ns as i32);
            sqlite3_bind_blob(
                stmt,
                2,
                key.as_ptr().cast(),
                key.len() as i32,
                SQLITE_TRANSIENT(),
            );

            let rc = sqlite3_step(stmt);
            sqlite3_finalize(stmt);

            if rc != SQLITE_DONE {
                return Err(LiteError::Storage {
                    detail: format!("step failed: {rc}"),
                });
            }
        }
        Ok(())
    }

    fn exec_scan(&self, ns: u8, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, LiteError> {
        use sqlite_wasm_rs::*;

        let db = *self.db.lock().map_err(|_| LiteError::LockPoisoned)?;

        let (sql_cstr, has_prefix) = if prefix.is_empty() {
            (
                c"SELECT key, val FROM kv WHERE ns = ?1 ORDER BY key" as &std::ffi::CStr,
                false,
            )
        } else {
            (
                c"SELECT key, val FROM kv WHERE ns = ?1 AND key >= ?2 AND key < ?3 ORDER BY key"
                    as &std::ffi::CStr,
                true,
            )
        };

        let mut stmt: *mut sqlite3_stmt = std::ptr::null_mut();
        unsafe {
            let rc = sqlite3_prepare_v2(db, sql_cstr.as_ptr(), -1, &mut stmt, std::ptr::null_mut());
            if rc != SQLITE_OK {
                return Err(LiteError::Storage {
                    detail: format!("prepare failed: {rc}"),
                });
            }

            sqlite3_bind_int(stmt, 1, ns as i32);

            if has_prefix {
                sqlite3_bind_blob(
                    stmt,
                    2,
                    prefix.as_ptr().cast(),
                    prefix.len() as i32,
                    SQLITE_TRANSIENT(),
                );

                // Compute upper bound.
                if let Some(upper) = prefix_upper_bound(prefix) {
                    sqlite3_bind_blob(
                        stmt,
                        3,
                        upper.as_ptr().cast(),
                        upper.len() as i32,
                        SQLITE_TRANSIENT(),
                    );
                } else {
                    // All 0xFF — no upper bound; use a different query.
                    sqlite3_finalize(stmt);
                    let alt = c"SELECT key, val FROM kv WHERE ns = ?1 AND key >= ?2 ORDER BY key";
                    let rc =
                        sqlite3_prepare_v2(db, alt.as_ptr(), -1, &mut stmt, std::ptr::null_mut());
                    if rc != SQLITE_OK {
                        return Err(LiteError::Storage {
                            detail: format!("prepare failed: {rc}"),
                        });
                    }
                    sqlite3_bind_int(stmt, 1, ns as i32);
                    sqlite3_bind_blob(
                        stmt,
                        2,
                        prefix.as_ptr().cast(),
                        prefix.len() as i32,
                        SQLITE_TRANSIENT(),
                    );
                }
            }

            let mut results = Vec::new();
            loop {
                let rc = sqlite3_step(stmt);
                if rc == SQLITE_ROW {
                    let key_ptr = sqlite3_column_blob(stmt, 0);
                    let key_len = sqlite3_column_bytes(stmt, 0) as usize;
                    let val_ptr = sqlite3_column_blob(stmt, 1);
                    let val_len = sqlite3_column_bytes(stmt, 1) as usize;

                    let key = if key_ptr.is_null() {
                        Vec::new()
                    } else {
                        std::slice::from_raw_parts(key_ptr.cast::<u8>(), key_len).to_vec()
                    };
                    let val = if val_ptr.is_null() {
                        Vec::new()
                    } else {
                        std::slice::from_raw_parts(val_ptr.cast::<u8>(), val_len).to_vec()
                    };
                    results.push((key, val));
                } else {
                    break;
                }
            }

            sqlite3_finalize(stmt);
            Ok(results)
        }
    }

    fn exec_count(&self, ns: u8) -> Result<u64, LiteError> {
        use sqlite_wasm_rs::*;

        let db = *self.db.lock().map_err(|_| LiteError::LockPoisoned)?;
        let sql = c"SELECT COUNT(*) FROM kv WHERE ns = ?1";
        let mut stmt: *mut sqlite3_stmt = std::ptr::null_mut();

        unsafe {
            let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, std::ptr::null_mut());
            if rc != SQLITE_OK {
                return Err(LiteError::Storage {
                    detail: format!("prepare failed: {rc}"),
                });
            }

            sqlite3_bind_int(stmt, 1, ns as i32);
            let rc = sqlite3_step(stmt);
            let count = if rc == SQLITE_ROW {
                sqlite3_column_int64(stmt, 0) as u64
            } else {
                0
            };
            sqlite3_finalize(stmt);
            Ok(count)
        }
    }
}

impl Drop for WasmSqliteStorage {
    fn drop(&mut self) {
        use sqlite_wasm_rs::*;
        if let Ok(db) = self.db.lock() {
            if !(*db).is_null() {
                unsafe {
                    sqlite3_close(*db);
                }
            }
        }
    }
}

#[async_trait(?Send)]
impl StorageEngine for WasmSqliteStorage {
    async fn get(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
        self.exec_get(ns as u8, key)
    }

    async fn put(&self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), LiteError> {
        self.exec_put(ns as u8, key, value)
    }

    async fn delete(&self, ns: Namespace, key: &[u8]) -> Result<(), LiteError> {
        self.exec_delete(ns as u8, key)
    }

    async fn scan_prefix(
        &self,
        ns: Namespace,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, LiteError> {
        self.exec_scan(ns as u8, prefix)
    }

    async fn batch_write(&self, ops: &[WriteOp]) -> Result<(), LiteError> {
        use sqlite_wasm_rs::*;

        let db = *self.db.lock().map_err(|_| LiteError::LockPoisoned)?;

        unsafe {
            let begin = c"BEGIN TRANSACTION";
            sqlite3_exec(
                db,
                begin.as_ptr(),
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
        }

        for op in ops {
            match op {
                WriteOp::Put { ns, key, value } => {
                    self.exec_put(*ns as u8, key, value)?;
                }
                WriteOp::Delete { ns, key } => {
                    self.exec_delete(*ns as u8, key)?;
                }
            }
        }

        unsafe {
            let commit = c"COMMIT";
            let rc = sqlite3_exec(
                db,
                commit.as_ptr(),
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            if rc != SQLITE_OK {
                let rollback = c"ROLLBACK";
                sqlite3_exec(
                    db,
                    rollback.as_ptr(),
                    None,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                );
                return Err(LiteError::Storage {
                    detail: format!("commit failed: {rc}"),
                });
            }
        }

        Ok(())
    }

    async fn count(&self, ns: Namespace) -> Result<u64, LiteError> {
        self.exec_count(ns as u8)
    }
}

/// Compute exclusive upper bound for prefix scan (same as native SqliteStorage).
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for i in (0..upper.len()).rev() {
        if upper[i] < 0xFF {
            upper[i] += 1;
            upper.truncate(i + 1);
            return Some(upper);
        }
    }
    None
}
