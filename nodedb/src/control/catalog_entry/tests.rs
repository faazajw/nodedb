//! Unit tests for [`CatalogEntry`]: encode/decode roundtrip and
//! `apply_to` semantics against a real `SystemCatalog` redb.

use std::sync::Arc;

use crate::control::catalog_entry::apply::apply_to;
use crate::control::catalog_entry::codec::{decode, encode};
use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::security::catalog::{StoredCollection, sequence_types::StoredSequence};
use crate::control::security::credential::store::CredentialStore;

fn open_catalog() -> (Arc<CredentialStore>, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let store = Arc::new(
        CredentialStore::open(&tmp.path().join("system.redb")).expect("open credential store"),
    );
    (store, tmp)
}

#[test]
fn roundtrip_put_collection() {
    let stored = StoredCollection::new(7, "orders", "alice");
    let entry = CatalogEntry::PutCollection(Box::new(stored));
    let bytes = encode(&entry).expect("encode");
    let decoded = decode(&bytes).expect("decode");
    match decoded {
        CatalogEntry::PutCollection(s) => {
            assert_eq!(s.tenant_id, 7);
            assert_eq!(s.name, "orders");
            assert_eq!(s.owner, "alice");
        }
        other => panic!("expected PutCollection, got {other:?}"),
    }
}

#[test]
fn roundtrip_deactivate_collection() {
    let entry = CatalogEntry::DeactivateCollection {
        tenant_id: 3,
        name: "legacy".into(),
    };
    let bytes = encode(&entry).unwrap();
    match decode(&bytes).unwrap() {
        CatalogEntry::DeactivateCollection { tenant_id, name } => {
            assert_eq!(tenant_id, 3);
            assert_eq!(name, "legacy");
        }
        other => panic!("expected DeactivateCollection, got {other:?}"),
    }
}

#[test]
fn roundtrip_put_sequence() {
    let seq = StoredSequence::new(1, "counter".into(), "bob".into());
    let entry = CatalogEntry::PutSequence(Box::new(seq));
    let bytes = encode(&entry).unwrap();
    match decode(&bytes).unwrap() {
        CatalogEntry::PutSequence(s) => {
            assert_eq!(s.tenant_id, 1);
            assert_eq!(s.name, "counter");
            assert_eq!(s.owner, "bob");
        }
        other => panic!("expected PutSequence, got {other:?}"),
    }
}

#[test]
fn roundtrip_delete_sequence() {
    let entry = CatalogEntry::DeleteSequence {
        tenant_id: 42,
        name: "gone".into(),
    };
    let bytes = encode(&entry).unwrap();
    match decode(&bytes).unwrap() {
        CatalogEntry::DeleteSequence { tenant_id, name } => {
            assert_eq!(tenant_id, 42);
            assert_eq!(name, "gone");
        }
        other => panic!("expected DeleteSequence, got {other:?}"),
    }
}

#[test]
fn apply_put_collection_writes_redb() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog().as_ref().expect("catalog present");

    let stored = StoredCollection::new(1, "widgets", "carol");
    apply_to(&CatalogEntry::PutCollection(Box::new(stored)), catalog);

    let loaded = catalog
        .get_collection(1, "widgets")
        .unwrap()
        .expect("present");
    assert_eq!(loaded.name, "widgets");
    assert_eq!(loaded.owner, "carol");
    assert!(loaded.is_active);
}

#[test]
fn apply_deactivate_collection_preserves_record() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog().as_ref().expect("catalog present");

    // Seed.
    let stored = StoredCollection::new(1, "archived", "carol");
    catalog.put_collection(&stored).unwrap();

    apply_to(
        &CatalogEntry::DeactivateCollection {
            tenant_id: 1,
            name: "archived".into(),
        },
        catalog,
    );

    let loaded = catalog
        .get_collection(1, "archived")
        .unwrap()
        .expect("record preserved");
    assert!(!loaded.is_active);
}

#[test]
fn apply_deactivate_missing_is_noop() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog().as_ref().expect("catalog present");
    apply_to(
        &CatalogEntry::DeactivateCollection {
            tenant_id: 1,
            name: "ghost".into(),
        },
        catalog,
    );
    assert!(catalog.get_collection(1, "ghost").unwrap().is_none());
}

#[test]
fn apply_put_then_delete_sequence() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog().as_ref().expect("catalog present");

    let seq = StoredSequence::new(1, "orders_id_seq".into(), "alice".into());
    apply_to(&CatalogEntry::PutSequence(Box::new(seq)), catalog);

    let loaded = catalog
        .get_sequence(1, "orders_id_seq")
        .unwrap()
        .expect("present");
    assert_eq!(loaded.name, "orders_id_seq");

    apply_to(
        &CatalogEntry::DeleteSequence {
            tenant_id: 1,
            name: "orders_id_seq".into(),
        },
        catalog,
    );

    assert!(catalog.get_sequence(1, "orders_id_seq").unwrap().is_none());
}

#[test]
fn kind_label_is_stable() {
    assert_eq!(
        CatalogEntry::PutCollection(Box::new(StoredCollection::new(1, "a", "b"))).kind(),
        "put_collection"
    );
    assert_eq!(
        CatalogEntry::DeactivateCollection {
            tenant_id: 1,
            name: "a".into()
        }
        .kind(),
        "deactivate_collection"
    );
    assert_eq!(
        CatalogEntry::PutSequence(Box::new(StoredSequence::new(1, "c".into(), "b".into()))).kind(),
        "put_sequence"
    );
    assert_eq!(
        CatalogEntry::DeleteSequence {
            tenant_id: 1,
            name: "c".into()
        }
        .kind(),
        "delete_sequence"
    );
}
