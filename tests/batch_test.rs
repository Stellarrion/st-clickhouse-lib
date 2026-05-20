//! Batch pipeline query tests — comprehensive coverage of the BatchBuilder API.
//!
//! Tests the explicit batch query pipeline in src/connection/batch.rs,
//! which sends multiple SELECT query packets in a single write() call
//! and reads responses sequentially.
//!
//! Reference: clickhouse-cpp — client_ut.cpp::TestBatches

mod common;
use st_clickhouse::Client;
use st_clickhouse::compression::CompressionMethod;
use st_clickhouse::protocol::block::{Block, ColumnInfo};

// ═══════════════════════════════════════════════════
// Helper: create a test table and populate it via FORMAT Native INSERT
// ═══════════════════════════════════════════════════

async fn setup_test_table(client: &Client, name: &str) {
    client
        .execute(&format!("DROP TABLE IF EXISTS {name}"))
        .await
        .expect("test operation failed");
    client
        .execute(&format!(
            "CREATE TABLE {name} (id UInt64, name String) ENGINE = Memory"
        ))
        .await
        .expect("test operation failed");

    // INSERT via begin_insert → send_data → end (the only working INSERT path)
    let mut session = client
        .begin_insert(name)
        .await
        .expect("test operation failed");

    // Build column data for UInt64: 1, 2, 3 (little-endian)
    let id_data: Vec<u8> = [1u64, 2, 3].iter().flat_map(|v| v.to_le_bytes()).collect();

    // Build column data for String: "Alice", "Bob", "Charlie"
    // String wire format: varint(len) + bytes for each row
    let names: &[&str] = &["Alice", "Bob", "Charlie"];
    let mut name_data = Vec::new();
    for n in names {
        st_clickhouse::protocol::wire::write_varint(&mut name_data, n.len() as u64)
            .expect("test operation failed");
        name_data.extend_from_slice(n.as_bytes());
    }

    let block = Block {
        columns: vec![
            ColumnInfo {
                name: "id".into(),
                type_name: "UInt64".into(),
                data: bytes::Bytes::from(id_data),
                lc_materialized: bytes::Bytes::new(),
            },
            ColumnInfo {
                name: "name".into(),
                type_name: "String".into(),
                data: bytes::Bytes::from(name_data),
                lc_materialized: bytes::Bytes::new(),
            },
        ],
        rows: 3,
    };
    session
        .send_data(&block)
        .await
        .expect("test operation failed");
    session.end().await.expect("test operation failed");
}

async fn setup_empty_table(client: &Client, name: &str) {
    client
        .execute(&format!("DROP TABLE IF EXISTS {name}"))
        .await
        .expect("test operation failed");
    client
        .execute(&format!(
            "CREATE TABLE {name} (id UInt64, name String) ENGINE = Memory"
        ))
        .await
        .expect("test operation failed");
}

// ═══════════════════════════════════════════════════
// 1. test_batch_two_queries
// ═══════════════════════════════════════════════════

#[tokio::test]
async fn test_batch_two_queries() {
    let client = common::connect_client().await;
    setup_test_table(&client, "st_batch_two").await;

    let results = client
        .batch()
        .query("SELECT COUNT(*) FROM st_batch_two")
        .query("SELECT id FROM st_batch_two ORDER BY id")
        .execute()
        .await
        .expect("test operation failed");

    assert_eq!(results.len(), 2, "expected 2 result sets");

    // First query: COUNT(*) should return a block
    let count_block = results[0].as_ref().expect("expected block for COUNT(*)");
    assert!(
        count_block.row_count() > 0,
        "COUNT(*) should return at least 1 row"
    );
    assert!(
        count_block.column_count() > 0,
        "COUNT(*) should have at least 1 column"
    );

    // Second query: SELECT id should return 3 rows
    let id_block = results[1].as_ref().expect("expected block for SELECT id");
    assert_eq!(id_block.row_count(), 3, "SELECT id should return 3 rows");

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS st_batch_two")
        .await
        .expect("test operation failed");
}

// ═══════════════════════════════════════════════════
// 2. test_batch_three_queries
// ═══════════════════════════════════════════════════

#[tokio::test]
async fn test_batch_three_queries() {
    let client = common::connect_client().await;
    setup_test_table(&client, "st_batch_three").await;

    let results = client
        .batch()
        .query("SELECT 42")
        .query("SELECT COUNT(*) FROM st_batch_three")
        .query("SELECT name FROM st_batch_three ORDER BY id")
        .execute()
        .await
        .expect("test operation failed");

    assert_eq!(results.len(), 3, "expected 3 result sets");

    // Query 1: SELECT 42
    assert!(results[0].is_some(), "SELECT 42 should return a block");

    // Query 2: COUNT(*)
    let count_block = results[1].as_ref().expect("COUNT(*) should return a block");
    assert!(count_block.row_count() > 0);

    // Query 3: SELECT name should return 3 rows
    let name_block = results[2]
        .as_ref()
        .expect("SELECT name should return a block");
    assert_eq!(name_block.row_count(), 3, "expected 3 names");

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS st_batch_three")
        .await
        .expect("test operation failed");
}

// ═══════════════════════════════════════════════════
// 3. test_batch_with_settings
// ═══════════════════════════════════════════════════

#[tokio::test]
async fn test_batch_with_settings() {
    let client = common::connect_client().await;
    setup_test_table(&client, "st_batch_settings").await;

    // Use settings to limit threads — should still return correct results
    let results = client
        .batch()
        .with_setting("max_threads", "1")
        .query("SELECT COUNT(*) FROM st_batch_settings")
        .query("SELECT id FROM st_batch_settings WHERE id > 1 ORDER BY id")
        .execute()
        .await
        .expect("test operation failed");

    assert_eq!(results.len(), 2, "expected 2 result sets");

    // COUNT(*) should return a block
    assert!(results[0].is_some(), "COUNT(*) block missing");

    // Filtered query should return 2 rows (id=2 and id=3)
    let filtered = results[1].as_ref().expect("filtered query block missing");
    assert_eq!(filtered.row_count(), 2, "expected 2 rows for id > 1");

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS st_batch_settings")
        .await
        .expect("test operation failed");
}

// ═══════════════════════════════════════════════════
// 4. test_batch_empty_result
// ═══════════════════════════════════════════════════

#[tokio::test]
async fn test_batch_empty_result() {
    let client = common::connect_client().await;
    setup_empty_table(&client, "st_batch_empty").await;

    let results = client
        .batch()
        .query("SELECT COUNT(*) FROM st_batch_empty")
        .query("SELECT * FROM st_batch_empty")
        .execute()
        .await
        .expect("test operation failed");

    assert_eq!(results.len(), 2, "expected 2 result sets");

    // COUNT(*) should return a block (even for empty table — always 1 row)
    let count_block = results[0]
        .as_ref()
        .expect("COUNT(*) should return a block even for empty table");
    assert!(count_block.row_count() > 0, "COUNT(*) should have 1 row");

    // SELECT * from empty table: ClickHouse may not send data blocks, or sends an empty one
    if let Some(ref block) = results[1] {
        assert_eq!(
            block.row_count(),
            0,
            "empty table should return 0 rows if a block is present"
        );
    }
    // else: None is also valid for empty result sets

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS st_batch_empty")
        .await
        .expect("test operation failed");
}

// ═══════════════════════════════════════════════════
// 5. test_batch_lz4_compression
// ═══════════════════════════════════════════════════

#[tokio::test]
async fn test_batch_lz4_compression() {
    let client = common::connect_client().await;
    setup_test_table(&client, "st_batch_lz4").await;

    let results = client
        .batch()
        .with_compression(CompressionMethod::Lz4)
        .query("SELECT 1 AS a")
        .query("SELECT id FROM st_batch_lz4 ORDER BY id")
        .execute()
        .await
        .expect("test operation failed");

    assert_eq!(results.len(), 2, "expected 2 result sets");

    // SELECT 1 should return a block with 1 row
    assert!(results[0].is_some(), "SELECT 1 should return a block");
    assert_eq!(
        results[0]
            .as_ref()
            .expect("test operation failed")
            .row_count(),
        1,
        "SELECT 1 should return 1 row"
    );

    // SELECT id should return 3 rows
    let id_block = results[1].as_ref().expect("SELECT id block missing");
    assert_eq!(id_block.row_count(), 3, "SELECT id should return 3 rows");

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS st_batch_lz4")
        .await
        .expect("test operation failed");
}

// ═══════════════════════════════════════════════════
// 6. test_batch_none_compression
// ═══════════════════════════════════════════════════

#[tokio::test]
async fn test_batch_none_compression() {
    let client = common::connect_client().await;
    setup_test_table(&client, "st_batch_none").await;

    let results = client
        .batch()
        .with_compression(CompressionMethod::None)
        .query("SELECT COUNT(*) FROM st_batch_none")
        .query("SELECT * FROM st_batch_none ORDER BY id")
        .execute()
        .await
        .expect("test operation failed");

    assert_eq!(results.len(), 2, "expected 2 result sets");

    // COUNT(*) = 3
    assert!(results[0].is_some(), "COUNT(*) block missing");

    // SELECT * should return 3 rows with 2 columns
    let all_block = results[1].as_ref().expect("SELECT * block missing");
    assert_eq!(all_block.row_count(), 3, "expected 3 rows");
    assert_eq!(all_block.column_count(), 2, "expected 2 columns (id, name)");

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS st_batch_none")
        .await
        .expect("test operation failed");
}
