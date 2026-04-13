//! The `CatalogEntry` enum itself.
//!
//! Every variant corresponds to a single mutation on the host-side
//! `SystemCatalog` redb and/or an in-memory registry on
//! `SharedState`. Adding a variant forces every consumer to handle
//! it (the apply / post_apply / tests modules use exhaustive
//! matches).

use serde::{Deserialize, Serialize};

use crate::control::security::catalog::{StoredCollection, sequence_types::StoredSequence};

#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub enum CatalogEntry {
    // ── Collection ─────────────────────────────────────────────────
    /// Upsert a collection record. Used by CREATE COLLECTION and by
    /// every ALTER COLLECTION path that ships a full updated record
    /// (strict schema changes, retention / legal_hold / LVC /
    /// append_only toggles, materialized_sum bindings).
    PutCollection(Box<StoredCollection>),
    /// Mark a collection as `is_active = false`. Record is
    /// preserved for audit + undrop.
    DeactivateCollection { tenant_id: u32, name: String },

    // ── Sequence ───────────────────────────────────────────────────
    /// Upsert a sequence record. Used by CREATE SEQUENCE and ALTER
    /// SEQUENCE. Carries the full updated record so followers can
    /// apply the change without shipping a separate diff.
    PutSequence(Box<StoredSequence>),
    /// Delete a sequence record entirely. Used by DROP SEQUENCE and
    /// by the cascade path in DROP COLLECTION that removes implicit
    /// `{coll}_{field}_seq` sequences for SERIAL columns.
    DeleteSequence { tenant_id: u32, name: String },
}

impl CatalogEntry {
    /// Short, human-readable descriptor of this entry — used in
    /// trace / metric labels.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::PutCollection(_) => "put_collection",
            Self::DeactivateCollection { .. } => "deactivate_collection",
            Self::PutSequence(_) => "put_sequence",
            Self::DeleteSequence { .. } => "delete_sequence",
        }
    }
}
