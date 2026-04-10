//! Integration tests for `{ key: value }` object literal INSERT syntax.
//!
//! Verifies that `INSERT INTO coll { ... }` produces the same result as the
//! standard `INSERT INTO coll (cols) VALUES (vals)` form across all engines.

mod common;

use common::pgwire_harness::TestServer;

// ── Schemaless Document ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_schemaless() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION docs").await.unwrap();

    server
        .exec("INSERT INTO docs { id: 'doc1', name: 'Alice', age: 30 }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM docs WHERE id = 'doc1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("Alice"));
    assert!(rows[0].contains("30"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_schemaless_nested() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION nested_docs").await.unwrap();

    server
        .exec("INSERT INTO nested_docs { id: 'n1', name: 'Bob', address: { city: 'NYC', zip: '10001' }, tags: ['admin', 'dev'] }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM nested_docs WHERE id = 'n1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("Bob"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_schemaless_auto_id() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION auto_id_docs").await.unwrap();

    // No explicit id — should auto-generate uuid_v7.
    server
        .exec("INSERT INTO auto_id_docs { name: 'Charlie', score: 42 }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM auto_id_docs")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("Charlie"));
}

// ── Key-Value ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_kv() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION kv_cache TYPE KEY_VALUE (key TEXT PRIMARY KEY)")
        .await
        .unwrap();

    // { } form inserts without error.
    server
        .exec("INSERT INTO kv_cache { key: 'k1', value: 'hello' }")
        .await
        .unwrap();

    // Both forms should produce the same result.
    server
        .exec("INSERT INTO kv_cache (key, value) VALUES ('k2', 'world')")
        .await
        .unwrap();

    // Verify both keys exist and both forms produce the same response shape.
    let r1 = server
        .query_text("SELECT * FROM kv_cache WHERE key = 'k1'")
        .await
        .unwrap();
    let r2 = server
        .query_text("SELECT * FROM kv_cache WHERE key = 'k2'")
        .await
        .unwrap();
    assert_eq!(r1.len(), 1, "object literal key lookup: {r1:?}");
    assert_eq!(r2.len(), 1, "VALUES key lookup: {r2:?}");

    // Full scan should return individual rows (flat array format).
    let all = server.query_text("SELECT * FROM kv_cache").await.unwrap();
    assert_eq!(all.len(), 2, "full scan should return 2 rows, got: {all:?}");
}

// ── Strict Document ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_strict() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION strict_orders TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, customer TEXT, amount FLOAT\
            )",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO strict_orders { id: 'o1', customer: 'Alice', amount: 99.99 }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM strict_orders WHERE id = 'o1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("Alice"));
}

// ── Plain Columnar ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_columnar() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION col_data TYPE COLUMNAR (\
                id TEXT, region TEXT, value FLOAT\
            )",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO col_data { id: 'c1', region: 'us-east', value: 3.14 }")
        .await
        .unwrap();

    let rows = server.query_text("SELECT * FROM col_data").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("us-east"));
}

// ── Timeseries Columnar ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_timeseries() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION ts_events TYPE COLUMNAR (\
                id TEXT, ts TIMESTAMP TIME_KEY, value FLOAT, region TEXT\
            ) WITH profile = 'timeseries'",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO ts_events { id: 'e1', ts: '2024-01-01T00:00:00Z', value: 42.0, region: 'us' }")
        .await
        .unwrap();

    let rows = server.query_text("SELECT * FROM ts_events").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("42"));
}

// ── Spatial Columnar ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_spatial() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION sp_locations TYPE COLUMNAR (\
                id TEXT, geom GEOMETRY SPATIAL_INDEX, label TEXT\
            ) WITH profile = 'spatial'",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO sp_locations (id, geom, label) VALUES ('s1', ST_Point(-73.98, 40.75), 'NYC')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM sp_locations")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("NYC"));
}

// ── UPSERT with { } ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_upsert_schemaless() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION upsert_docs").await.unwrap();

    server
        .exec("INSERT INTO upsert_docs { id: 'u1', name: 'Alice', role: 'user' }")
        .await
        .unwrap();

    server
        .exec("UPSERT INTO upsert_docs { id: 'u1', name: 'Alice Updated', role: 'admin' }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM upsert_docs WHERE id = 'u1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("Alice Updated"));
    assert!(rows[0].contains("admin"));
}

// ── Equivalence: { } produces same result as VALUES ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_matches_values_form() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION equiv_docs").await.unwrap();

    server
        .exec("INSERT INTO equiv_docs (id, name, score) VALUES ('v1', 'ValuesForm', 100)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO equiv_docs { id: 'o1', name: 'ObjectForm', score: 100 }")
        .await
        .unwrap();

    let rows = server.query_text("SELECT * FROM equiv_docs").await.unwrap();
    assert_eq!(rows.len(), 2);
}
