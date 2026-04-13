//! `release_leases` — synchronous propose-and-wait helper for
//! batched descriptor lease release. Mirrors `propose::acquire_lease`
//! but with `MetadataEntry::DescriptorLeaseRelease`.
//!
//! Used on `SIGTERM` drain to remove this node's leases faster than
//! expiry. Also useful for tests that want to assert the release path
//! independently of the grant path.

use nodedb_cluster::{DescriptorId, MetadataEntry};

use crate::control::state::SharedState;
use crate::error::Error;

/// Release every lease this node currently holds against any of
/// `descriptor_ids`. Empty input is a no-op (returns `Ok` without
/// touching raft).
///
/// **Cluster path**: builds a single `DescriptorLeaseRelease` entry
/// containing all `descriptor_ids` and proposes it through the
/// metadata raft group. Every node's applier removes the matching
/// `(descriptor_id, this_node_id)` entries from its
/// `MetadataCache.leases` map.
///
/// **Single-node fallback**: removes the entries directly from the
/// local cache. Same `Ok(0)` sentinel pattern as
/// `propose_catalog_entry`.
///
/// The `node_id` carried in the variant is always **this** node's
/// id — releasing another node's leases is not supported (and would
/// be a correctness bug because the other node may still be
/// holding active references to the descriptor version).
pub fn release_leases(
    shared: &SharedState,
    descriptor_ids: Vec<DescriptorId>,
) -> Result<(), Error> {
    if descriptor_ids.is_empty() {
        return Ok(());
    }

    // Single-node fallback.
    if shared.metadata_raft.get().is_none() {
        remove_from_local_cache(shared, &descriptor_ids);
        return Ok(());
    }

    let entry = MetadataEntry::DescriptorLeaseRelease {
        node_id: shared.node_id,
        descriptor_ids,
    };
    super::propose_and_wait(shared, &entry, "release")?;
    Ok(())
}

fn remove_from_local_cache(shared: &SharedState, descriptor_ids: &[DescriptorId]) {
    let mut cache = shared
        .metadata_cache
        .write()
        .unwrap_or_else(|p| p.into_inner());
    for id in descriptor_ids {
        cache.leases.remove(&(id.clone(), shared.node_id));
    }
}
