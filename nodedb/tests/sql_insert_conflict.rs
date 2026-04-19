//! INSERT conflict semantics for document engines.
//!
//! Covers the spec contract that `INSERT` is not `UPSERT`:
//!   * duplicate primary-key / document-id `INSERT` must raise SQLSTATE 23505
//!     (unique_violation) and leave the original row untouched.
//!   * `INSERT ... ON CONFLICT DO NOTHING` must succeed as a no-op on conflict.
//!   * `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` and the `UPSERT` keyword
//!     are the *only* supported paths for intentional overwrite.

mod common;

use common::pgwire_harness::TestServer;

// ── Strict document ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_insert_duplicate_pk_raises_unique_violation() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION t TYPE DOCUMENT STRICT \
             (id STRING NOT NULL PRIMARY KEY, n INT)",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO t (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    // Second INSERT must fail with SQLSTATE 23505 (unique_violation) and the
    // error message must name the conflicting PK value so drivers/users can
    // handle it.
    match server
        .client
        .simple_query("INSERT INTO t (id, n) VALUES ('dup', 2)")
        .await
    {
        Ok(_) => panic!("expected unique_violation, got success"),
        Err(e) => {
            let db_err = e.as_db_error().expect("expected DbError");
            assert_eq!(
                db_err.code().code(),
                "23505",
                "expected SQLSTATE 23505, got {}: {}",
                db_err.code().code(),
                db_err.message()
            );
            let msg = db_err.message().to_lowercase();
            assert!(
                msg.contains("dup"),
                "error message should name the conflicting PK, got: {}",
                db_err.message()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_insert_duplicate_pk_preserves_original_row() {
    // Regression guard against silent overwrite: even if the error surfaces,
    // a future routing regression to `PointPut` would overwrite the row
    // underneath the error. Assert the original n=1 survives.
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION t TYPE DOCUMENT STRICT \
             (id STRING NOT NULL PRIMARY KEY, n INT)",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO t (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    let _ = server
        .client
        .simple_query("INSERT INTO t (id, n) VALUES ('dup', 2)")
        .await;

    let rows = server
        .query_text("SELECT n FROM t WHERE id = 'dup'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected exactly one row, got {rows:?}");
    assert!(
        rows[0].contains("\"n\":1"),
        "duplicate-PK INSERT must not overwrite the original row, got: {}",
        rows[0]
    );
    assert!(
        !rows[0].contains("\"n\":2"),
        "original row was overwritten with the rejected value: {}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_insert_on_conflict_do_nothing_is_noop() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION t TYPE DOCUMENT STRICT \
             (id STRING NOT NULL PRIMARY KEY, n INT)",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO t (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    // Must succeed (no error), must not overwrite.
    server
        .exec("INSERT INTO t (id, n) VALUES ('dup', 2) ON CONFLICT DO NOTHING")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM t WHERE id = 'dup'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("\"n\":1"),
        "ON CONFLICT DO NOTHING must leave the original row intact, got: {}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_insert_on_conflict_do_update_overwrites() {
    // Regression guard: the opt-in overwrite path must keep working so the
    // fix for the default-INSERT path doesn't strand users without an
    // overwrite option.
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION t TYPE DOCUMENT STRICT \
             (id STRING NOT NULL PRIMARY KEY, n INT)",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO t (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    server
        .exec(
            "INSERT INTO t (id, n) VALUES ('dup', 2) \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n",
        )
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM t WHERE id = 'dup'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("\"n\":2"), "got: {}", rows[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_upsert_keyword_overwrites() {
    // Regression guard on the explicit UPSERT grammar path.
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION t TYPE DOCUMENT STRICT \
             (id STRING NOT NULL PRIMARY KEY, n INT)",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO t (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    server
        .exec("UPSERT INTO t (id, n) VALUES ('dup', 2)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM t WHERE id = 'dup'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("\"n\":2"), "got: {}", rows[0]);
}

// ── Schemaless document ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_duplicate_id_raises_unique_violation() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION docs").await.unwrap();

    server
        .exec("INSERT INTO docs (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    match server
        .client
        .simple_query("INSERT INTO docs (id, n) VALUES ('dup', 2)")
        .await
    {
        Ok(_) => panic!("expected unique_violation, got success"),
        Err(e) => {
            let db_err = e.as_db_error().expect("expected DbError");
            assert_eq!(
                db_err.code().code(),
                "23505",
                "expected SQLSTATE 23505, got {}: {}",
                db_err.code().code(),
                db_err.message()
            );
            assert!(
                db_err.message().to_lowercase().contains("dup"),
                "error must name the conflicting id, got: {}",
                db_err.message()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_duplicate_id_preserves_original_row() {
    // Silent-overwrite regression guard on the schemaless path — the
    // DocumentSchemaless arm of `convert_insert` shares the PointPut
    // routing, so the same check must hold.
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION docs").await.unwrap();

    server
        .exec("INSERT INTO docs (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    let _ = server
        .client
        .simple_query("INSERT INTO docs (id, n) VALUES ('dup', 2)")
        .await;

    let rows = server
        .query_text("SELECT n FROM docs WHERE id = 'dup'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected exactly one row, got {rows:?}");
    assert!(
        rows[0].contains("\"n\":1"),
        "duplicate-id INSERT must not overwrite the original row, got: {}",
        rows[0]
    );
    assert!(
        !rows[0].contains("\"n\":2"),
        "original row was overwritten with the rejected value: {}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_on_conflict_do_nothing_is_noop() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION docs").await.unwrap();

    server
        .exec("INSERT INTO docs (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO docs (id, n) VALUES ('dup', 2) ON CONFLICT DO NOTHING")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM docs WHERE id = 'dup'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("\"n\":1"), "got: {}", rows[0]);
}
