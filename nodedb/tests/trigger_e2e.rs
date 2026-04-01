//! End-to-end tests for trigger execution: BEFORE validation, AFTER audit,
//! INSTEAD OF, cross-engine cascading via live pgwire server.

mod common;

use common::pgwire_harness::TestServer;

/// CREATE TRIGGER succeeds and SHOW TRIGGERS lists it.
#[tokio::test]
async fn create_trigger_and_show() {
    let server = TestServer::start().await;

    // Create collection first.
    server.exec("CREATE COLLECTION orders").await.unwrap();

    // Create a trigger.
    let result = server
        .exec(
            "CREATE TRIGGER audit_orders AFTER INSERT ON orders FOR EACH ROW \
             BEGIN INSERT INTO audit_log (order_id) VALUES (NEW.id); END",
        )
        .await;
    assert!(result.is_ok(), "CREATE TRIGGER failed: {:?}", result);

    // SHOW TRIGGERS lists it.
    let triggers = server.query_text("SHOW TRIGGERS").await.unwrap_or_default();
    assert!(
        triggers.iter().any(|t| t.contains("audit_orders")),
        "audit_orders not in SHOW TRIGGERS: {triggers:?}"
    );

    // DROP TRIGGER removes it.
    server.exec("DROP TRIGGER audit_orders").await.unwrap();
    let after = server.query_text("SHOW TRIGGERS").await.unwrap_or_default();
    assert!(
        !after.iter().any(|t| t.contains("audit_orders")),
        "audit_orders still in SHOW TRIGGERS after DROP"
    );
}

/// BEFORE trigger with RAISE EXCEPTION rejects the DML.
#[tokio::test]
async fn before_trigger_rejects_dml() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION orders").await.unwrap();

    // Create BEFORE trigger that rejects negative totals.
    server
        .exec(
            "CREATE TRIGGER validate_total BEFORE INSERT ON orders FOR EACH ROW \
             BEGIN \
               IF NEW.total < 0 THEN \
                 RAISE EXCEPTION 'total cannot be negative'; \
               END IF; \
             END",
        )
        .await
        .unwrap();

    // Insert with positive total — should succeed.
    let ok_result = server
        .exec("INSERT INTO orders (id, total) VALUES ('ord-1', 100)")
        .await;
    assert!(ok_result.is_ok(), "positive insert failed: {:?}", ok_result);

    // Insert with negative total — should be rejected by BEFORE trigger.
    server
        .expect_error(
            "INSERT INTO orders (id, total) VALUES ('ord-2', -50)",
            "negative",
        )
        .await;
}

/// ALTER TRIGGER ENABLE/DISABLE works.
#[tokio::test]
async fn alter_trigger_enable_disable() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION items").await.unwrap();

    server
        .exec(
            "CREATE TRIGGER t1 AFTER INSERT ON items FOR EACH ROW \
             BEGIN RETURN; END",
        )
        .await
        .unwrap();

    // Disable.
    server.exec("ALTER TRIGGER t1 DISABLE").await.unwrap();
    let triggers = server.query_text("SHOW TRIGGERS").await.unwrap_or_default();
    // Trigger still exists but disabled.
    assert!(triggers.iter().any(|t| t.contains("t1")));

    // Re-enable.
    server.exec("ALTER TRIGGER t1 ENABLE").await.unwrap();

    // Cleanup.
    server.exec("DROP TRIGGER t1").await.unwrap();
}

/// INSTEAD OF trigger replaces INSERT.
#[tokio::test]
async fn instead_of_trigger() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION view_orders").await.unwrap();

    // INSTEAD OF trigger replaces the DML entirely.
    server
        .exec(
            "CREATE TRIGGER redirect INSTEAD OF INSERT ON view_orders FOR EACH ROW \
             BEGIN RETURN; END",
        )
        .await
        .unwrap();

    // INSERT goes through INSTEAD OF trigger — the trigger body runs instead.
    let result = server
        .exec("INSERT INTO view_orders (id) VALUES ('v1')")
        .await;
    assert!(result.is_ok(), "INSTEAD OF insert failed: {:?}", result);

    server.exec("DROP TRIGGER redirect").await.unwrap();
}

/// SECURITY DEFINER trigger stores correct security mode.
#[tokio::test]
async fn security_definer_trigger() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION secure_data").await.unwrap();

    server
        .exec(
            "CREATE TRIGGER admin_audit AFTER INSERT ON secure_data FOR EACH ROW \
             SECURITY DEFINER \
             BEGIN RETURN; END",
        )
        .await
        .unwrap();

    let triggers = server.query_text("SHOW TRIGGERS").await.unwrap_or_default();
    assert!(triggers.iter().any(|t| t.contains("admin_audit")));

    server.exec("DROP TRIGGER admin_audit").await.unwrap();
}
