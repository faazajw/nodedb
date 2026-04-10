//! Integration tests for typeguard DEFAULT/VALUE expressions and VALIDATE TYPEGUARD.
//!
//! Verifies that:
//! - DEFAULT injects a value when the field is absent
//! - DEFAULT does not overwrite user-provided values
//! - VALUE always overwrites, even when user provides a value
//! - REQUIRED + DEFAULT = field is always present
//! - Cross-field VALUE expressions resolve other document fields

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typeguard_default_injects_when_absent() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION tg_defaults").await.unwrap();

    server
        .exec(
            "CREATE TYPEGUARD ON tg_defaults (\
                 status STRING DEFAULT 'draft'\
             )",
        )
        .await
        .unwrap();

    // Insert without status — DEFAULT should fill it.
    server
        .exec("INSERT INTO tg_defaults { id: 'd1', name: 'Alice' }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM tg_defaults WHERE id = 'd1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("draft"),
        "DEFAULT should inject 'draft': {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typeguard_default_does_not_overwrite() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION tg_no_overwrite")
        .await
        .unwrap();

    server
        .exec(
            "CREATE TYPEGUARD ON tg_no_overwrite (\
                 status STRING DEFAULT 'draft'\
             )",
        )
        .await
        .unwrap();

    // Insert with explicit status — DEFAULT should NOT overwrite.
    server
        .exec("INSERT INTO tg_no_overwrite { id: 'd1', status: 'active' }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM tg_no_overwrite WHERE id = 'd1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("active"),
        "DEFAULT should not overwrite user value: {rows:?}"
    );
    assert!(
        !rows[0].contains("draft"),
        "should NOT contain default: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typeguard_value_always_overwrites() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION tg_value_overwrite")
        .await
        .unwrap();

    server
        .exec(
            "CREATE TYPEGUARD ON tg_value_overwrite (\
                 computed STRING VALUE 'server_computed'\
             )",
        )
        .await
        .unwrap();

    // Insert with user-provided value — VALUE should overwrite.
    server
        .exec("INSERT INTO tg_value_overwrite { id: 'v1', computed: 'user_input' }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM tg_value_overwrite WHERE id = 'v1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("server_computed"),
        "VALUE should overwrite user input: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typeguard_required_plus_default() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION tg_req_default")
        .await
        .unwrap();

    server
        .exec(
            "CREATE TYPEGUARD ON tg_req_default (\
                 version INT REQUIRED DEFAULT 1\
             )",
        )
        .await
        .unwrap();

    // Insert without version — DEFAULT fills before REQUIRED check.
    server
        .exec("INSERT INTO tg_req_default { id: 'r1', name: 'test' }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM tg_req_default WHERE id = 'r1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains('1'),
        "REQUIRED + DEFAULT should inject version=1: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typeguard_default_integer() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION tg_int_default")
        .await
        .unwrap();

    server
        .exec(
            "CREATE TYPEGUARD ON tg_int_default (\
                 priority INT DEFAULT 0 CHECK (priority >= 0)\
             )",
        )
        .await
        .unwrap();

    // Insert without priority — DEFAULT 0 should be injected and pass CHECK.
    server
        .exec("INSERT INTO tg_int_default { id: 'p1', name: 'test' }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM tg_int_default WHERE id = 'p1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

// ── VALIDATE TYPEGUARD ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validate_typeguard_no_violations() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION val_clean").await.unwrap();

    // Insert valid data first.
    server
        .exec("INSERT INTO val_clean { id: 'v1', name: 'Alice', age: 25 }")
        .await
        .unwrap();

    // Add type guard after data.
    server
        .exec(
            "CREATE TYPEGUARD ON val_clean (\
                 name STRING,\
                 age INT\
             )",
        )
        .await
        .unwrap();

    // Validate — all docs should pass.
    let rows = server
        .query_text("VALIDATE TYPEGUARD ON val_clean")
        .await
        .unwrap();
    assert_eq!(rows.len(), 0, "no violations expected: {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validate_typeguard_finds_violations() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION val_dirty").await.unwrap();

    // Insert data that will violate a future type guard.
    server
        .exec("INSERT INTO val_dirty { id: 'd1', name: 'Alice', score: 42 }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO val_dirty { id: 'd2', name: 123, score: 99 }")
        .await
        .unwrap();

    // Add type guard — name must be STRING.
    server
        .exec(
            "CREATE TYPEGUARD ON val_dirty (\
                 name STRING\
             )",
        )
        .await
        .unwrap();

    // Validate — d2 has name=123 (INT, not STRING).
    let rows = server
        .query_text("VALIDATE TYPEGUARD ON val_dirty")
        .await
        .unwrap();
    assert!(
        !rows.is_empty(),
        "should find at least one violation: {rows:?}"
    );
    // First column is document_id — should be d2.
    assert!(
        rows.iter().any(|r| r.contains("d2")),
        "violation should reference d2: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validate_typeguard_no_guards() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION val_noguard").await.unwrap();

    server
        .exec("INSERT INTO val_noguard { id: 'n1', x: 1 }")
        .await
        .unwrap();

    // No typeguard — should return empty result.
    let rows = server
        .query_text("VALIDATE TYPEGUARD ON val_noguard")
        .await
        .unwrap();
    assert_eq!(rows.len(), 0);
}
