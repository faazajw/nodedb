//! End-to-end tests for stored procedure execution: DML, COMMIT/ROLLBACK,
//! exception handling, fuel metering via live pgwire server.

mod common;

use common::pgwire_harness::TestServer;

/// CREATE PROCEDURE and CALL succeed.
#[tokio::test]
async fn create_and_call_procedure() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION orders").await.unwrap();

    let result = server
        .exec(
            "CREATE PROCEDURE cleanup(days INT) AS \
             BEGIN \
               DELETE FROM orders WHERE age > days; \
             END",
        )
        .await;
    assert!(result.is_ok(), "CREATE PROCEDURE failed: {:?}", result);

    // CALL the procedure.
    let call_result = server.exec("CALL cleanup(30)").await;
    assert!(call_result.is_ok(), "CALL failed: {:?}", call_result);

    // SHOW PROCEDURES lists it.
    let procs = server
        .query_text("SHOW PROCEDURES")
        .await
        .unwrap_or_default();
    assert!(
        procs.iter().any(|p| p.contains("cleanup")),
        "cleanup not in SHOW PROCEDURES: {procs:?}"
    );

    server.exec("DROP PROCEDURE cleanup").await.unwrap();
}

/// Procedure with INSERT DML.
#[tokio::test]
async fn procedure_with_insert() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION logs").await.unwrap();

    server
        .exec(
            "CREATE PROCEDURE add_log(msg TEXT) AS \
             BEGIN \
               INSERT INTO logs (id, message) VALUES ('log-1', msg); \
             END",
        )
        .await
        .unwrap();

    let result = server.exec("CALL add_log('hello world')").await;
    assert!(result.is_ok(), "CALL with INSERT failed: {:?}", result);

    server.exec("DROP PROCEDURE add_log").await.unwrap();
}

/// Procedure with RAISE EXCEPTION aborts execution.
#[tokio::test]
async fn procedure_raise_exception() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE PROCEDURE fail_on_purpose() AS \
             BEGIN \
               RAISE EXCEPTION 'intentional failure'; \
             END",
        )
        .await
        .unwrap();

    server
        .expect_error("CALL fail_on_purpose()", "intentional failure")
        .await;

    server.exec("DROP PROCEDURE fail_on_purpose").await.unwrap();
}

/// Procedure with exception handler catches errors.
#[tokio::test]
async fn procedure_exception_handler() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE PROCEDURE safe_proc() AS \
             BEGIN \
               RAISE EXCEPTION 'inner error'; \
             EXCEPTION \
               WHEN OTHERS THEN \
                 RETURN; \
             END",
        )
        .await
        .unwrap();

    // Should NOT raise — exception handler catches it.
    let result = server.exec("CALL safe_proc()").await;
    assert!(
        result.is_ok(),
        "exception handler did not catch: {:?}",
        result
    );

    server.exec("DROP PROCEDURE safe_proc").await.unwrap();
}

/// Procedure with WITH (MAX_ITERATIONS, TIMEOUT) limits.
#[tokio::test]
async fn procedure_fuel_metering() {
    let server = TestServer::start().await;

    // Create a procedure with very low iteration limit.
    server
        .exec(
            "CREATE PROCEDURE infinite_loop() WITH (MAX_ITERATIONS = 10) AS \
             BEGIN \
               LOOP \
                 BREAK; \
               END LOOP; \
             END",
        )
        .await
        .unwrap();

    // CALL should succeed (BREAK exits immediately).
    let result = server.exec("CALL infinite_loop()").await;
    assert!(result.is_ok(), "fuel-limited proc failed: {:?}", result);

    server.exec("DROP PROCEDURE infinite_loop").await.unwrap();
}

/// SAVEPOINT/ROLLBACK TO syntax parses correctly in procedure body.
#[tokio::test]
async fn procedure_savepoint_syntax() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION data").await.unwrap();

    let result = server
        .exec(
            "CREATE PROCEDURE sp_test() AS \
             BEGIN \
               INSERT INTO data (id) VALUES ('a'); \
               SAVEPOINT sp1; \
               INSERT INTO data (id) VALUES ('b'); \
               ROLLBACK TO sp1; \
               COMMIT; \
             END",
        )
        .await;
    assert!(
        result.is_ok(),
        "procedure with SAVEPOINT failed to create: {:?}",
        result
    );

    let call = server.exec("CALL sp_test()").await;
    assert!(call.is_ok(), "CALL sp_test failed: {:?}", call);

    server.exec("DROP PROCEDURE sp_test").await.unwrap();
}
