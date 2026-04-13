//! Asynchronous post-apply side effects for a [`CatalogEntry`].
//!
//! Called from the production `MetadataCommitApplier` **after**
//! `apply_to` has written the redb record. Side effects include:
//!
//! - Data Plane `DocumentOp::Register` dispatch on committed
//!   `PutCollection` (so follower cores know the storage mode
//!   before the first cross-node INSERT arrives).
//! - In-memory `state.sequence_registry` sync on `PutSequence` /
//!   `DeleteSequence` so `NEXTVAL` / `CURRVAL` calls on followers
//!   see the replicated definition immediately.
//! - Future: `block_cache` invalidation for procedure/function
//!   replacement, prepared-statement cache invalidation, CDC
//!   schema-change stream emission.
//!
//! Runs inside a `tokio::spawn` so the raft tick is never blocked
//! on async side effects.

use std::sync::Arc;

use tracing::{debug, warn};

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::state::SharedState;

/// Spawn the post-apply side effects of `entry`. Best-effort: any
/// failure logs a warning but does not unwind the raft apply path.
pub fn spawn_post_apply_side_effects(entry: CatalogEntry, shared: Arc<SharedState>) {
    tokio::spawn(async move {
        match entry {
            CatalogEntry::PutCollection(stored) => {
                crate::control::server::pgwire::ddl::collection::create::dispatch_register_from_stored(
                    &shared,
                    &stored,
                )
                .await;
                debug!(
                    collection = %stored.name,
                    "catalog_entry: Register dispatched to local Data Plane"
                );
            }
            CatalogEntry::DeactivateCollection { tenant_id, name } => {
                // Data Plane Unregister is out of scope for batch
                // 1e — the existing enforcement runtime tolerates an
                // orphan register for an inactive collection until
                // the next collection-level reload. Future batches
                // wire Unregister here.
                debug!(
                    collection = %name,
                    tenant = tenant_id,
                    "catalog_entry: DeactivateCollection post-apply (no Data Plane hook yet)"
                );
            }
            CatalogEntry::PutSequence(seq) => {
                // Create or replace in the in-memory registry so
                // NEXTVAL / CURRVAL on follower nodes sees the
                // replicated record without a restart. The registry
                // is keyed on (tenant_id, name); `create` both
                // inserts new records and overwrites existing ones
                // for in-place ALTER SEQUENCE.
                if let Err(e) = shared.sequence_registry.create((*seq).clone()) {
                    warn!(
                        sequence = %seq.name,
                        tenant = seq.tenant_id,
                        error = %e,
                        "catalog_entry: sequence_registry create failed"
                    );
                }
            }
            CatalogEntry::DeleteSequence { tenant_id, name } => {
                if let Err(e) = shared.sequence_registry.remove(tenant_id, &name) {
                    debug!(
                        sequence = %name,
                        tenant = tenant_id,
                        error = %e,
                        "catalog_entry: sequence_registry remove (ignored)"
                    );
                }
            }
            CatalogEntry::PutSequenceState(state) => {
                // ALTER SEQUENCE RESTART / period reset paths ship
                // a fresh `SequenceState`; replicate it into the
                // in-memory registry handle so `NEXTVAL` on every
                // node returns from the new counter immediately.
                // `restart` is the existing setval path — it takes
                // the next value and atomically updates the handle.
                if let Err(e) = shared.sequence_registry.restart(
                    state.tenant_id,
                    &state.name,
                    state.current_value,
                ) {
                    debug!(
                        sequence = %state.name,
                        tenant = state.tenant_id,
                        error = %e,
                        "catalog_entry: sequence_registry restart (ignored — sequence may be missing on fresh follower)"
                    );
                }
            }
            CatalogEntry::PutTrigger(trigger) => {
                // `register` is an upsert: inserts on new triggers
                // and replaces on OR REPLACE / ALTER ENABLE/DISABLE.
                shared.trigger_registry.register((*trigger).clone());
            }
            CatalogEntry::DeleteTrigger { tenant_id, name } => {
                shared.trigger_registry.unregister(tenant_id, &name);
            }
            CatalogEntry::PutFunction(func) => {
                // The block cache is keyed by body-SQL hash, not
                // by (tenant, name), so a point invalidation isn't
                // possible. Clear the whole cache — it's small,
                // reparsing is cheap, and this mirrors PostgreSQL's
                // "any DDL invalidates prepared plans" behavior.
                shared.block_cache.clear();
                let _ = func;
            }
            CatalogEntry::DeleteFunction { tenant_id, name } => {
                shared.block_cache.clear();
                let _ = (tenant_id, name);
            }
            CatalogEntry::PutProcedure(proc) => {
                // Same body-cache invalidation as functions — the
                // block cache is keyed on body-hash and reparse is
                // cheap, so a full clear mirrors PostgreSQL's
                // "any DDL invalidates prepared plans" behavior.
                shared.block_cache.clear();
                let _ = proc;
            }
            CatalogEntry::DeleteProcedure { tenant_id, name } => {
                shared.block_cache.clear();
                let _ = (tenant_id, name);
            }
            CatalogEntry::PutSchedule(schedule) => {
                // Upsert in the in-memory cron registry so the
                // scheduler executor picks up the new / updated
                // schedule on its next tick.
                shared.schedule_registry.register((*schedule).clone());
            }
            CatalogEntry::DeleteSchedule { tenant_id, name } => {
                shared.schedule_registry.unregister(tenant_id, &name);
            }
            CatalogEntry::PutChangeStream(stream) => {
                // Upsert the CDC stream definition. The Event
                // Plane starts routing matching WriteEvents into
                // this stream's buffer immediately.
                shared.stream_registry.register((*stream).clone());
            }
            CatalogEntry::DeleteChangeStream { tenant_id, name } => {
                shared.stream_registry.unregister(tenant_id, &name);
                shared.cdc_router.remove_buffer(tenant_id, &name);
            }
        }
    });
}
