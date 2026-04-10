//! Integration tests for index naming: named, unnamed (auto-generated), and FIELDS keyword.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_index_named() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION idx_named").await.unwrap();
    server
        .exec("INSERT INTO idx_named { id: 'a', role: 'admin' }")
        .await
        .unwrap();

    // Named index — standard SQL form.
    server
        .exec("CREATE INDEX my_idx ON idx_named(role)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_index_unnamed_auto_name() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION idx_unnamed").await.unwrap();
    server
        .exec("INSERT INTO idx_unnamed { id: 'a', email: 'a@b.com' }")
        .await
        .unwrap();

    // No name — should auto-generate name and succeed.
    server
        .exec("CREATE INDEX ON idx_unnamed(email)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_index_fields_keyword() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION idx_fields").await.unwrap();
    server
        .exec("INSERT INTO idx_fields { id: 'a', tag: 'rust' }")
        .await
        .unwrap();

    // FIELDS keyword form — should succeed.
    server
        .exec("CREATE INDEX ON idx_fields FIELDS tag")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_unique_index_unnamed() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION idx_unique").await.unwrap();
    server
        .exec("INSERT INTO idx_unique { id: 'a', code: 'ABC' }")
        .await
        .unwrap();

    // Unnamed UNIQUE index.
    server
        .exec("CREATE UNIQUE INDEX ON idx_unique(code)")
        .await
        .unwrap();
}
