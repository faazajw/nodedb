//! Replicated metadata Raft group (group 0).
//!
//! All cluster-wide state (DDL descriptors, topology, routing, descriptor
//! leases, cluster version) is proposed as a [`MetadataEntry`] against
//! this group and applied on every node via a [`MetadataApplier`].

pub mod actions;
pub mod applier;
pub mod cache;
pub mod codec;
pub mod descriptors;
pub mod entry;
pub mod state;

pub use actions::{
    ApiKeyAction, AuditRetentionAction, ChangeStreamAction, CollectionAction, FunctionAction,
    GrantAction, IndexAction, MaterializedViewAction, ProcedureAction, RlsAction, RoleAction,
    ScheduleAction, SequenceAction, TenantAction, TriggerAction, UserAction,
};
pub use applier::{CacheApplier, MetadataApplier, NoopMetadataApplier};
pub use cache::MetadataCache;
pub use codec::{decode_entry, encode_entry};
pub use descriptors::{
    ApiKeyDescriptor, AuditRetentionDescriptor, ChangeStreamDescriptor, CollectionDescriptor,
    DescriptorHeader, DescriptorId, DescriptorKind, DescriptorLease, FunctionDescriptor,
    GrantDescriptor, IndexDescriptor, MaterializedViewDescriptor, ProcedureDescriptor,
    RlsDescriptor, RoleDescriptor, ScheduleDescriptor, SequenceDescriptor, TenantDescriptor,
    TriggerDescriptor, UserDescriptor,
};
pub use entry::{MetadataEntry, RoutingChange, TopologyChange};
pub use state::DescriptorState;

/// Well-known Raft group ID for the metadata group.
/// Distinct from data vShard groups (which start at group 1).
pub const METADATA_GROUP_ID: u64 = 0;
