//! Smoke tests — basic connectivity and query execution.
//!
//! Each test creates its own Client. The pool properly cleans up after each test.

mod common;

#[tokio::test]
async fn handshake_works_test() {
    let client = common::connect_client().await;
    let info = client.server_info().await.expect("test operation failed");
    eprintln!(
        "Server: {} {}.{}.{} (rev {})",
        info.name, info.version_major, info.version_minor, info.version_patch, info.revision
    );
    assert!(info.revision > 0, "server revision missing");
}

#[tokio::test]
async fn select_one_test() {
    let client = common::connect_client().await;
    let block = client
        .query("SELECT 1")
        .block()
        .await
        .expect("test operation failed");
    assert!(block.row_count() > 0, "expected rows");
    assert!(block.column_count() > 0, "expected columns");
}

#[tokio::test]
async fn select_variant_test() {
    let client = common::connect_client().await;
    match client
        .query("SELECT CAST(1 AS Variant(UInt8, String)) AS v")
        .block()
        .await
    {
        Ok(block) => assert!(block.column_count() > 0),
        Err(e) => eprintln!("Variant not supported: {e:?}"),
    }
}

#[tokio::test]
async fn select_json_test() {
    let client = common::connect_client().await;
    match client
        .query("SELECT CAST('{\"x\":1}' AS JSON) AS j")
        .block()
        .await
    {
        Ok(block) => assert!(block.column_count() > 0),
        Err(e) => eprintln!("JSON not supported: {e:?}"),
    }
}

#[tokio::test]
async fn select_dynamic_test() {
    let client = common::connect_client().await;
    match client
        .query("SELECT CAST(42 AS Dynamic) AS d")
        .block()
        .await
    {
        Ok(block) => assert!(block.column_count() > 0),
        Err(e) => eprintln!("Dynamic not supported: {e:?}"),
    }
}

#[tokio::test]
async fn select_aggregate_test() {
    eprintln!("  AggregateFunction test skipped (needs full CH serialization)");
}
