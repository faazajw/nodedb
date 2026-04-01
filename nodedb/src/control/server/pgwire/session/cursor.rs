//! Server-side cursor methods on SessionStore.

use std::net::SocketAddr;

use super::state::CursorState;
use super::store::SessionStore;

impl SessionStore {
    /// Declare a cursor with pre-fetched results.
    pub fn declare_cursor(&self, addr: &SocketAddr, name: String, rows: Vec<String>) {
        self.write_session(addr, |session| {
            session
                .cursors
                .insert(name, CursorState { rows, position: 0 });
        });
    }

    /// Fetch N rows from a cursor. Returns the rows and whether cursor is exhausted.
    pub fn fetch_cursor(
        &self,
        addr: &SocketAddr,
        name: &str,
        count: usize,
    ) -> crate::Result<(Vec<String>, bool)> {
        self.write_session(addr, |session| {
            let cursor = session
                .cursors
                .get_mut(name)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("cursor \"{name}\" does not exist"),
                })?;

            let start = cursor.position;
            let end = (start + count).min(cursor.rows.len());
            let rows: Vec<String> = cursor.rows[start..end].to_vec();
            cursor.position = end;
            let exhausted = end >= cursor.rows.len();
            Ok((rows, exhausted))
        })
        .unwrap_or_else(|| {
            Err(crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })
        })
    }

    /// Close a cursor.
    pub fn close_cursor(&self, addr: &SocketAddr, name: &str) {
        self.write_session(addr, |session| {
            session.cursors.remove(name);
        });
    }
}
