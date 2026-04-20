//! Cross-table referential integrity tests for the catalog apply path.
//!
//! These tests pin the contract between `catalog_entry::apply::apply_to`
//! and `recovery_check::integrity::verify_redb_integrity`:
//!
//!   For every parent-replicated DDL entry type that declares an
//!   `owner` field, the synchronous apply path MUST write a
//!   matching `StoredOwner` row to redb. If it does not, the next
//!   restart's integrity check aborts boot with an `OrphanRow`
//!   divergence.
//!
//! Parent-replicated types covered: Collection, Function, Procedure,
//! Trigger, MaterializedView, Sequence, Schedule, ChangeStream.
//!
//! The verifier-coverage tests assert that `verify_redb_integrity`
//! reports orphans symmetrically for all eight — not just Collection.

use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::catalog_entry::apply::apply_to;
use nodedb::control::cluster::recovery_check::divergence::DivergenceKind;
use nodedb::control::cluster::recovery_check::integrity::verify_redb_integrity;
use nodedb::control::security::catalog::auth_types::{StoredOwner, StoredUser};
use nodedb::control::security::catalog::function_types::{
    FunctionParam, FunctionVolatility, StoredFunction,
};
use nodedb::control::security::catalog::procedure_types::{
    ParamDirection, ProcedureParam, ProcedureRoutability, StoredProcedure,
};
use nodedb::control::security::catalog::sequence_types::StoredSequence;
use nodedb::control::security::catalog::trigger_types::{
    StoredTrigger, TriggerEvents, TriggerGranularity, TriggerTiming,
};
use nodedb::control::security::catalog::{StoredCollection, StoredMaterializedView, SystemCatalog};
use nodedb::event::cdc::stream_def::{ChangeStreamDef, OpFilter, RetentionConfig, StreamFormat};
use nodedb::event::scheduler::types::{MissedPolicy, ScheduleDef, ScheduleScope};

// ── helpers ─────────────────────────────────────────────────────────────────

const ADMIN: &str = "admin";
const TENANT: u32 = 1;

fn make_catalog() -> (tempfile::TempDir, SystemCatalog) {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
    put_admin_user(&catalog);
    (dir, catalog)
}

fn put_admin_user(catalog: &SystemCatalog) {
    let user = StoredUser {
        user_id: 1,
        username: ADMIN.to_string(),
        tenant_id: TENANT,
        password_hash: "argon2id$dummy".to_string(),
        scram_salt: vec![],
        scram_salted_password: vec![],
        roles: vec!["superuser".to_string()],
        is_superuser: true,
        is_active: true,
        is_service_account: false,
        created_at: 0,
        updated_at: 0,
        password_expires_at: 0,
        md5_hash: String::new(),
    };
    catalog.put_user(&user).unwrap();
}

fn make_collection(name: &str) -> StoredCollection {
    StoredCollection::new(TENANT, name, ADMIN)
}

fn make_function(name: &str) -> StoredFunction {
    StoredFunction {
        tenant_id: TENANT,
        name: name.to_string(),
        parameters: vec![FunctionParam {
            name: "x".into(),
            data_type: "INT".into(),
        }],
        return_type: "INT".into(),
        body_sql: "SELECT x".into(),
        compiled_body_sql: None,
        volatility: FunctionVolatility::Immutable,
        security: Default::default(),
        language: Default::default(),
        wasm_hash: None,
        wasm_fuel: 1_000_000,
        wasm_memory: 16 * 1024 * 1024,
        owner: ADMIN.into(),
        created_at: 0,
        descriptor_version: 0,
        modification_hlc: Default::default(),
    }
}

fn make_procedure(name: &str) -> StoredProcedure {
    StoredProcedure {
        tenant_id: TENANT,
        name: name.into(),
        parameters: vec![ProcedureParam {
            name: "cutoff".into(),
            data_type: "INT".into(),
            direction: ParamDirection::In,
        }],
        body_sql: "BEGIN END".into(),
        max_iterations: 1_000_000,
        timeout_secs: 60,
        routability: ProcedureRoutability::default(),
        owner: ADMIN.into(),
        created_at: 0,
        descriptor_version: 0,
        modification_hlc: Default::default(),
    }
}

fn make_trigger(name: &str, collection: &str) -> StoredTrigger {
    StoredTrigger {
        tenant_id: TENANT,
        collection: collection.into(),
        name: name.into(),
        timing: TriggerTiming::After,
        events: TriggerEvents {
            on_insert: true,
            on_update: false,
            on_delete: false,
        },
        granularity: TriggerGranularity::Row,
        when_condition: None,
        body_sql: "BEGIN END".into(),
        priority: 0,
        enabled: true,
        execution_mode: Default::default(),
        security: Default::default(),
        batch_mode: Default::default(),
        owner: ADMIN.into(),
        created_at: 0,
        descriptor_version: 1,
        modification_hlc: Default::default(),
    }
}

fn make_mv(name: &str) -> StoredMaterializedView {
    StoredMaterializedView {
        tenant_id: TENANT,
        name: name.into(),
        source: "source_coll".into(),
        query_sql: "SELECT * FROM source_coll".into(),
        refresh_mode: "auto".into(),
        owner: ADMIN.into(),
        created_at: 0,
        descriptor_version: 0,
        modification_hlc: Default::default(),
    }
}

fn make_sequence(name: &str) -> StoredSequence {
    StoredSequence::new(TENANT, name.into(), ADMIN.into())
}

fn make_schedule(name: &str) -> ScheduleDef {
    ScheduleDef {
        tenant_id: TENANT,
        name: name.into(),
        cron_expr: "*/5 * * * *".into(),
        body_sql: "SELECT 1".into(),
        scope: ScheduleScope::Normal,
        missed_policy: MissedPolicy::Skip,
        allow_overlap: true,
        enabled: true,
        target_collection: None,
        owner: ADMIN.into(),
        created_at: 0,
    }
}

fn make_stream(name: &str) -> ChangeStreamDef {
    ChangeStreamDef {
        tenant_id: TENANT,
        name: name.into(),
        collection: "*".into(),
        op_filter: OpFilter::all(),
        format: StreamFormat::Json,
        retention: RetentionConfig::default(),
        compaction: Default::default(),
        webhook: Default::default(),
        late_data: Default::default(),
        kafka: Default::default(),
        owner: ADMIN.into(),
        created_at: 0,
    }
}

fn owner_row_present(catalog: &SystemCatalog, object_type: &str, name: &str) -> bool {
    let owners = catalog.load_all_owners().unwrap();
    owners.iter().any(|o| {
        o.object_type == object_type
            && o.tenant_id == TENANT
            && o.object_name == name
            && o.owner_username == ADMIN
    })
}

fn find_orphan(catalog: &SystemCatalog, expected_kind: &str) -> Option<DivergenceKind> {
    verify_redb_integrity(catalog)
        .into_iter()
        .map(|d| d.kind)
        .find(|k| {
            matches!(
                k,
                DivergenceKind::OrphanRow { kind, expected_parent_kind: "owner", .. }
                    if *kind == expected_kind
            )
        })
}

// ── applier contract: Put<T> writes StoredOwner row to redb ────────────────

#[test]
fn apply_put_collection_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutCollection(Box::new(make_collection("orders")));
    apply_to(&entry, &catalog);
    assert!(
        owner_row_present(&catalog, "collection", "orders"),
        "PutCollection apply must write a StoredOwner row to redb; \
         missing row causes verify_redb_integrity to abort startup \
         with an OrphanRow(collection) divergence"
    );
}

#[test]
fn apply_put_function_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutFunction(Box::new(make_function("normalize_email")));
    apply_to(&entry, &catalog);
    assert!(
        owner_row_present(&catalog, "function", "normalize_email"),
        "PutFunction apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_procedure_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutProcedure(Box::new(make_procedure("purge_old")));
    apply_to(&entry, &catalog);
    assert!(
        owner_row_present(&catalog, "procedure", "purge_old"),
        "PutProcedure apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_trigger_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    // Write the parent collection first so Check 4 (trigger →
    // collection) doesn't also fire — this test is about the
    // owner-row gap only.
    catalog.put_collection(&make_collection("orders")).unwrap();
    catalog
        .put_owner(&StoredOwner {
            object_type: "collection".into(),
            object_name: "orders".into(),
            tenant_id: TENANT,
            owner_username: ADMIN.into(),
        })
        .unwrap();

    let entry = CatalogEntry::PutTrigger(Box::new(make_trigger("send_email", "orders")));
    apply_to(&entry, &catalog);
    assert!(
        owner_row_present(&catalog, "trigger", "send_email"),
        "PutTrigger apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_materialized_view_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutMaterializedView(Box::new(make_mv("orders_summary")));
    apply_to(&entry, &catalog);
    assert!(
        owner_row_present(&catalog, "materialized_view", "orders_summary"),
        "PutMaterializedView apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_sequence_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutSequence(Box::new(make_sequence("orders_seq")));
    apply_to(&entry, &catalog);
    assert!(
        owner_row_present(&catalog, "sequence", "orders_seq"),
        "PutSequence apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_schedule_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutSchedule(Box::new(make_schedule("nightly")));
    apply_to(&entry, &catalog);
    assert!(
        owner_row_present(&catalog, "schedule", "nightly"),
        "PutSchedule apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_change_stream_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutChangeStream(Box::new(make_stream("orders_cdc")));
    apply_to(&entry, &catalog);
    assert!(
        owner_row_present(&catalog, "change_stream", "orders_cdc"),
        "PutChangeStream apply must write a StoredOwner row to redb"
    );
}

// ── verifier coverage: verify_redb_integrity reports orphans for every ────
//    parent-replicated type, not only Collection. ─────────────────────────

#[test]
fn verify_redb_integrity_flags_orphan_function() {
    let (_dir, catalog) = make_catalog();
    catalog.put_function(&make_function("f1")).unwrap();
    assert!(
        find_orphan(&catalog, "function").is_some(),
        "verify_redb_integrity must report OrphanRow(function) when a \
         StoredFunction exists without a matching StoredOwner row"
    );
}

#[test]
fn verify_redb_integrity_flags_orphan_procedure() {
    let (_dir, catalog) = make_catalog();
    catalog.put_procedure(&make_procedure("p1")).unwrap();
    assert!(
        find_orphan(&catalog, "procedure").is_some(),
        "verify_redb_integrity must report OrphanRow(procedure) when a \
         StoredProcedure exists without a matching StoredOwner row"
    );
}

#[test]
fn verify_redb_integrity_flags_orphan_trigger() {
    let (_dir, catalog) = make_catalog();
    // Provide the parent collection + its owner so the pre-existing
    // Check 4 (trigger → collection) and Check 1 don't fire and mask
    // the orphan-owner signal we are testing.
    catalog.put_collection(&make_collection("orders")).unwrap();
    catalog
        .put_owner(&StoredOwner {
            object_type: "collection".into(),
            object_name: "orders".into(),
            tenant_id: TENANT,
            owner_username: ADMIN.into(),
        })
        .unwrap();
    catalog.put_trigger(&make_trigger("t1", "orders")).unwrap();
    assert!(
        find_orphan(&catalog, "trigger").is_some(),
        "verify_redb_integrity must report OrphanRow(trigger) when a \
         StoredTrigger exists without a matching StoredOwner row"
    );
}

#[test]
fn verify_redb_integrity_flags_orphan_materialized_view() {
    let (_dir, catalog) = make_catalog();
    catalog.put_materialized_view(&make_mv("mv1")).unwrap();
    assert!(
        find_orphan(&catalog, "materialized_view").is_some(),
        "verify_redb_integrity must report OrphanRow(materialized_view) \
         when a StoredMaterializedView exists without a matching \
         StoredOwner row"
    );
}

#[test]
fn verify_redb_integrity_flags_orphan_sequence() {
    let (_dir, catalog) = make_catalog();
    catalog.put_sequence(&make_sequence("s1")).unwrap();
    assert!(
        find_orphan(&catalog, "sequence").is_some(),
        "verify_redb_integrity must report OrphanRow(sequence) when a \
         StoredSequence exists without a matching StoredOwner row"
    );
}

#[test]
fn verify_redb_integrity_flags_orphan_schedule() {
    let (_dir, catalog) = make_catalog();
    catalog.put_schedule(&make_schedule("sch1")).unwrap();
    assert!(
        find_orphan(&catalog, "schedule").is_some(),
        "verify_redb_integrity must report OrphanRow(schedule) when a \
         ScheduleDef exists without a matching StoredOwner row"
    );
}

#[test]
fn verify_redb_integrity_flags_orphan_change_stream() {
    let (_dir, catalog) = make_catalog();
    catalog.put_change_stream(&make_stream("cs1")).unwrap();
    assert!(
        find_orphan(&catalog, "change_stream").is_some(),
        "verify_redb_integrity must report OrphanRow(change_stream) \
         when a ChangeStreamDef exists without a matching StoredOwner row"
    );
}

// ── compile-time guard: exhaustive match on CatalogEntry forces ───────────
//    every new variant to declare its integrity-check status here, so a ──
//    new parent-replicated type cannot land without either being covered
//    by the applier helpers or explicitly marked as exempt. ──────────────

/// Exhaustive classification of every `CatalogEntry` variant for the
/// parent-owner invariant. Adding a new variant to `CatalogEntry` forces
/// this match to grow by one arm; reviewers decide `ParentReplicated`
/// (applier must call `owner::put_parent_owner`) or `Exempt`
/// (standalone objects, registry-only entries).
///
/// This function is never called at runtime — its value is purely the
/// compile-time exhaustiveness check.
#[allow(dead_code)]
enum VariantClass {
    ParentReplicated,
    Exempt,
}

#[allow(dead_code, clippy::match_same_arms)]
fn classify(entry: &CatalogEntry) -> VariantClass {
    match entry {
        // Eight parent-replicated types — owner row written by applier.
        CatalogEntry::PutCollection(_) => VariantClass::ParentReplicated,
        CatalogEntry::PutFunction(_) => VariantClass::ParentReplicated,
        CatalogEntry::PutProcedure(_) => VariantClass::ParentReplicated,
        CatalogEntry::PutTrigger(_) => VariantClass::ParentReplicated,
        CatalogEntry::PutMaterializedView(_) => VariantClass::ParentReplicated,
        CatalogEntry::PutSequence(_) => VariantClass::ParentReplicated,
        CatalogEntry::PutSchedule(_) => VariantClass::ParentReplicated,
        CatalogEntry::PutChangeStream(_) => VariantClass::ParentReplicated,

        // Symmetric delete / deactivate paths — owner row deleted by applier.
        CatalogEntry::DeactivateCollection { .. } => VariantClass::ParentReplicated,
        CatalogEntry::PurgeCollection { .. } => VariantClass::ParentReplicated,
        CatalogEntry::DeleteFunction { .. } => VariantClass::ParentReplicated,
        CatalogEntry::DeleteProcedure { .. } => VariantClass::ParentReplicated,
        CatalogEntry::DeleteTrigger { .. } => VariantClass::ParentReplicated,
        CatalogEntry::DeleteMaterializedView { .. } => VariantClass::ParentReplicated,
        CatalogEntry::DeleteSequence { .. } => VariantClass::ParentReplicated,
        CatalogEntry::DeleteSchedule { .. } => VariantClass::ParentReplicated,
        CatalogEntry::DeleteChangeStream { .. } => VariantClass::ParentReplicated,

        // Standalone ownership — indexes and raw ALTER OWNER paths.
        CatalogEntry::PutOwner(_) => VariantClass::Exempt,
        CatalogEntry::DeleteOwner { .. } => VariantClass::Exempt,

        // Not parent-owned: sequence runtime state, user / role / api_key
        // identity records, permission grants, tenant identity, RLS.
        CatalogEntry::PutSequenceState(_) => VariantClass::Exempt,
        CatalogEntry::PutUser(_) => VariantClass::Exempt,
        CatalogEntry::DeactivateUser { .. } => VariantClass::Exempt,
        CatalogEntry::PutRole(_) => VariantClass::Exempt,
        CatalogEntry::DeleteRole { .. } => VariantClass::Exempt,
        CatalogEntry::PutApiKey(_) => VariantClass::Exempt,
        CatalogEntry::RevokeApiKey { .. } => VariantClass::Exempt,
        CatalogEntry::PutPermission(_) => VariantClass::Exempt,
        CatalogEntry::DeletePermission { .. } => VariantClass::Exempt,
        CatalogEntry::PutTenant(_) => VariantClass::Exempt,
        CatalogEntry::DeleteTenant { .. } => VariantClass::Exempt,
        CatalogEntry::PutRlsPolicy(_) => VariantClass::Exempt,
        CatalogEntry::DeleteRlsPolicy { .. } => VariantClass::Exempt,
    }
}

// ── end-to-end: after applying every parent-replicated Put<T>, the ────────
//    integrity verifier must produce zero violations. ─────────────────────

#[test]
fn apply_all_put_entries_produces_clean_redb_integrity() {
    let (_dir, catalog) = make_catalog();

    apply_to(
        &CatalogEntry::PutCollection(Box::new(make_collection("orders"))),
        &catalog,
    );
    apply_to(
        &CatalogEntry::PutFunction(Box::new(make_function("f1"))),
        &catalog,
    );
    apply_to(
        &CatalogEntry::PutProcedure(Box::new(make_procedure("p1"))),
        &catalog,
    );
    apply_to(
        &CatalogEntry::PutTrigger(Box::new(make_trigger("t1", "orders"))),
        &catalog,
    );
    apply_to(
        &CatalogEntry::PutMaterializedView(Box::new(make_mv("mv1"))),
        &catalog,
    );
    apply_to(
        &CatalogEntry::PutSequence(Box::new(make_sequence("s1"))),
        &catalog,
    );
    apply_to(
        &CatalogEntry::PutSchedule(Box::new(make_schedule("sch1"))),
        &catalog,
    );
    apply_to(
        &CatalogEntry::PutChangeStream(Box::new(make_stream("cs1"))),
        &catalog,
    );

    let violations = verify_redb_integrity(&catalog);
    assert!(
        violations.is_empty(),
        "expected zero integrity violations after applying all \
         parent-replicated Put entries, got: {:?}",
        violations
    );
}
