//! Schema descriptors.
//!
//! One descriptor type per schema object. Every descriptor embeds a
//! [`DescriptorHeader`] carrying the identity, version, modification time,
//! and lifecycle state required for safe online DDL.

pub mod api_key;
pub mod audit_retention;
pub mod change_stream;
pub mod collection;
pub mod common;
pub mod function;
pub mod grant;
pub mod index;
pub mod lease;
pub mod materialized_view;
pub mod procedure;
pub mod rls;
pub mod role;
pub mod schedule;
pub mod sequence;
pub mod tenant;
pub mod trigger;
pub mod user;

pub use api_key::ApiKeyDescriptor;
pub use audit_retention::AuditRetentionDescriptor;
pub use change_stream::ChangeStreamDescriptor;
pub use collection::CollectionDescriptor;
pub use common::{DescriptorHeader, DescriptorId, DescriptorKind};
pub use function::FunctionDescriptor;
pub use grant::GrantDescriptor;
pub use index::IndexDescriptor;
pub use lease::DescriptorLease;
pub use materialized_view::MaterializedViewDescriptor;
pub use procedure::ProcedureDescriptor;
pub use rls::RlsDescriptor;
pub use role::RoleDescriptor;
pub use schedule::ScheduleDescriptor;
pub use sequence::SequenceDescriptor;
pub use tenant::TenantDescriptor;
pub use trigger::TriggerDescriptor;
pub use user::UserDescriptor;
