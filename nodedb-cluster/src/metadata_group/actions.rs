//! Exhaustive action enums for each schema object type.
//!
//! Every [`crate::metadata_group::MetadataEntry`] DDL variant carries one of
//! these actions: `Create(Descriptor)` to introduce a new descriptor,
//! `Drop { id }` to remove one, and `Alter { id, change }` for in-place
//! modifications that bump the descriptor version.

use serde::{Deserialize, Serialize};

use crate::metadata_group::descriptors::{
    ApiKeyDescriptor, AuditRetentionDescriptor, ChangeStreamDescriptor, CollectionDescriptor,
    DescriptorId, FunctionDescriptor, GrantDescriptor, IndexDescriptor, MaterializedViewDescriptor,
    ProcedureDescriptor, RlsDescriptor, RoleDescriptor, ScheduleDescriptor, SequenceDescriptor,
    TenantDescriptor, TriggerDescriptor, UserDescriptor, collection::ColumnDef,
};

macro_rules! simple_action {
    ($name:ident, $descriptor:ty, $alter:ty) => {
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            Serialize,
            Deserialize,
            zerompk::ToMessagePack,
            zerompk::FromMessagePack,
        )]
        pub enum $name {
            Create(Box<$descriptor>),
            Drop { id: DescriptorId },
            Alter { id: DescriptorId, change: $alter },
        }
    };
}

// ── Collection ──────────────────────────────────────────────────────────

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum CollectionAlter {
    AddColumn(ColumnDef),
    DropColumn {
        name: String,
    },
    RenameColumn {
        from: String,
        to: String,
    },
    SetColumnDefault {
        name: String,
        default: Option<String>,
    },
    SetColumnNullable {
        name: String,
        nullable: bool,
    },
    SetWithOption {
        key: String,
        value: String,
    },
    RemoveWithOption {
        key: String,
    },
}

simple_action!(CollectionAction, CollectionDescriptor, CollectionAlter);

// ── Index ───────────────────────────────────────────────────────────────

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum IndexAlter {
    SetPredicate(Option<String>),
    SetWithOption { key: String, value: String },
}

simple_action!(IndexAction, IndexDescriptor, IndexAlter);

// ── Trigger ─────────────────────────────────────────────────────────────

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum TriggerAlter {
    SetEnabled(bool),
    SetWhenClause(Option<String>),
    SetActionBody(String),
}

simple_action!(TriggerAction, TriggerDescriptor, TriggerAlter);

// ── Sequence ────────────────────────────────────────────────────────────

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum SequenceAlter {
    RestartWith(i64),
    SetIncrementBy(i64),
    SetMinValue(Option<i64>),
    SetMaxValue(Option<i64>),
    SetCycle(bool),
    SetCacheSize(u32),
}

simple_action!(SequenceAction, SequenceDescriptor, SequenceAlter);

// ── User / Role / Grant ─────────────────────────────────────────────────

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum UserAlter {
    SetEnabled(bool),
    ChangeCredential(crate::metadata_group::descriptors::user::UserCredential),
    AddRole(String),
    RemoveRole(String),
}

simple_action!(UserAction, UserDescriptor, UserAlter);

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RoleAlter {
    AddInherit(String),
    RemoveInherit(String),
    SetDescription(Option<String>),
}

simple_action!(RoleAction, RoleDescriptor, RoleAlter);

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum GrantAlter {
    SetWithGrantOption(bool),
}

simple_action!(GrantAction, GrantDescriptor, GrantAlter);

// ── RLS ─────────────────────────────────────────────────────────────────

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RlsAlter {
    SetUsingExpr(Option<String>),
    SetWithCheckExpr(Option<String>),
    SetPermissive(bool),
}

simple_action!(RlsAction, RlsDescriptor, RlsAlter);

// ── Change stream / MV / Schedule ───────────────────────────────────────

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ChangeStreamAlter {
    SetRetentionSeconds(u64),
    SetMaxBytes(u64),
    SetOpFilter(crate::metadata_group::descriptors::change_stream::ChangeOpFilter),
}

simple_action!(
    ChangeStreamAction,
    ChangeStreamDescriptor,
    ChangeStreamAlter
);

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum MaterializedViewAlter {
    Refresh,
    SetRefreshPolicy(crate::metadata_group::descriptors::materialized_view::MvRefreshPolicy),
}

simple_action!(
    MaterializedViewAction,
    MaterializedViewDescriptor,
    MaterializedViewAlter
);

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ScheduleAlter {
    SetCron(String),
    SetTimezone(String),
    SetSqlBody(String),
    SetEnabled(bool),
    SetMaxConcurrent(u32),
}

simple_action!(ScheduleAction, ScheduleDescriptor, ScheduleAlter);

// ── Function / Procedure ────────────────────────────────────────────────

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum FunctionAlter {
    Replace(Box<FunctionDescriptor>),
}

simple_action!(FunctionAction, FunctionDescriptor, FunctionAlter);

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ProcedureAlter {
    Replace(Box<ProcedureDescriptor>),
}

simple_action!(ProcedureAction, ProcedureDescriptor, ProcedureAlter);

// ── Tenant / ApiKey / AuditRetention ────────────────────────────────────

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum TenantAlter {
    SetDisplayName(String),
    SetQuotaBytes(Option<u64>),
    SetQuotaRowCount(Option<u64>),
    SetEnabled(bool),
}

simple_action!(TenantAction, TenantDescriptor, TenantAlter);

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ApiKeyAlter {
    Revoke,
    SetScopes(Vec<String>),
    SetExpiresAtWallNs(Option<u64>),
}

simple_action!(ApiKeyAction, ApiKeyDescriptor, ApiKeyAlter);

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum AuditRetentionAlter {
    SetRetentionDays(u32),
}

simple_action!(
    AuditRetentionAction,
    AuditRetentionDescriptor,
    AuditRetentionAlter
);
