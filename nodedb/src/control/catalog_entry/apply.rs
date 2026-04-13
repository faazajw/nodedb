//! Synchronous host-side application of a [`CatalogEntry`] to
//! `SystemCatalog` redb.
//!
//! Called from the production `MetadataCommitApplier` after a
//! metadata-group entry commits. This is the single writer into
//! `SystemCatalog` on every node — every pgwire DDL handler goes
//! through `propose_catalog_entry` → raft commit → this function,
//! which guarantees every node converges on the same catalog state.
//!
//! The match is exhaustive — adding a variant to [`CatalogEntry`]
//! is a compile error here until this file handles it.

use tracing::{debug, warn};

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::security::catalog::SystemCatalog;

/// Apply `entry` to `catalog`. Logs and swallows per-variant errors
/// (the applier loop treats catalog writes as best-effort so a
/// single write failure doesn't stall the entire raft apply path;
/// startup replay will re-run the entry if needed).
pub fn apply_to(entry: &CatalogEntry, catalog: &SystemCatalog) {
    match entry {
        CatalogEntry::PutCollection(stored) => {
            if let Err(e) = catalog.put_collection(stored) {
                warn!(
                    collection = %stored.name,
                    tenant = stored.tenant_id,
                    error = %e,
                    "catalog_entry: put_collection failed"
                );
            }
        }
        CatalogEntry::DeactivateCollection { tenant_id, name } => {
            match catalog.get_collection(*tenant_id, name) {
                Ok(Some(mut stored)) => {
                    stored.is_active = false;
                    if let Err(e) = catalog.put_collection(&stored) {
                        warn!(
                            collection = %name,
                            tenant = *tenant_id,
                            error = %e,
                            "catalog_entry: deactivate_collection put failed"
                        );
                    }
                }
                Ok(None) => {
                    debug!(
                        collection = %name,
                        tenant = *tenant_id,
                        "catalog_entry: deactivate on missing collection (fresh follower)"
                    );
                }
                Err(e) => warn!(
                    collection = %name,
                    tenant = *tenant_id,
                    error = %e,
                    "catalog_entry: deactivate_collection get failed"
                ),
            }
        }
        CatalogEntry::PutSequence(seq) => {
            if let Err(e) = catalog.put_sequence(seq) {
                warn!(
                    sequence = %seq.name,
                    tenant = seq.tenant_id,
                    error = %e,
                    "catalog_entry: put_sequence failed"
                );
            }
        }
        CatalogEntry::DeleteSequence { tenant_id, name } => {
            if let Err(e) = catalog.delete_sequence(*tenant_id, name) {
                warn!(
                    sequence = %name,
                    tenant = *tenant_id,
                    error = %e,
                    "catalog_entry: delete_sequence failed"
                );
            }
        }
        CatalogEntry::PutSequenceState(state) => {
            if let Err(e) = catalog.put_sequence_state(state) {
                warn!(
                    sequence = %state.name,
                    tenant = state.tenant_id,
                    error = %e,
                    "catalog_entry: put_sequence_state failed"
                );
            }
        }
        CatalogEntry::PutTrigger(trigger) => {
            if let Err(e) = catalog.put_trigger(trigger) {
                warn!(
                    trigger = %trigger.name,
                    tenant = trigger.tenant_id,
                    error = %e,
                    "catalog_entry: put_trigger failed"
                );
            }
        }
        CatalogEntry::DeleteTrigger { tenant_id, name } => {
            if let Err(e) = catalog.delete_trigger(*tenant_id, name) {
                warn!(
                    trigger = %name,
                    tenant = *tenant_id,
                    error = %e,
                    "catalog_entry: delete_trigger failed"
                );
            }
        }
        CatalogEntry::PutFunction(func) => {
            if let Err(e) = catalog.put_function(func) {
                warn!(
                    function = %func.name,
                    tenant = func.tenant_id,
                    error = %e,
                    "catalog_entry: put_function failed"
                );
            }
        }
        CatalogEntry::DeleteFunction { tenant_id, name } => {
            if let Err(e) = catalog.delete_function(*tenant_id, name) {
                warn!(
                    function = %name,
                    tenant = *tenant_id,
                    error = %e,
                    "catalog_entry: delete_function failed"
                );
            }
        }
    }
}
