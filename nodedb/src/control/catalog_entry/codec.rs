//! Zerompk encode / decode helpers for [`CatalogEntry`].
//!
//! Kept in its own file so the rest of `catalog_entry/` never has to
//! care about the wire format — all on-the-wire concerns flow through
//! these two functions.

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::error::Error;

pub fn encode(entry: &CatalogEntry) -> Result<Vec<u8>, Error> {
    zerompk::to_msgpack_vec(entry).map_err(|e| Error::Config {
        detail: format!("catalog entry encode: {e}"),
    })
}

pub fn decode(bytes: &[u8]) -> Result<CatalogEntry, Error> {
    zerompk::from_msgpack(bytes).map_err(|e| Error::Config {
        detail: format!("catalog entry decode: {e}"),
    })
}
