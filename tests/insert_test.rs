mod common;
use st_clickhouse::protocol::block::{Block, ColumnInfo};

/// FORMAT Native INSERT: begin_insert → send_data → end.
#[tokio::test]
async fn test_insert_native() {
    let client = common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS st_native_test")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_native_test (id UInt64) ENGINE = Memory")
        .await
        .expect("test operation failed");

    // Get table structure and start INSERT session
    let mut session = client
        .begin_insert("st_native_test")
        .await
        .expect("test operation failed");
    eprintln!("Got table structure");

    // Send data
    let block = Block {
        columns: vec![ColumnInfo {
            name: "id".into(),
            type_name: "UInt64".into(),
            data: bytes::Bytes::copy_from_slice(&42u64.to_le_bytes()),
            lc_materialized: bytes::Bytes::new(),
        }],
        rows: 1,
    };
    session
        .send_data(&block)
        .await
        .expect("test operation failed");
    eprintln!("Data sent");

    // Finalize
    session.end().await.expect("test operation failed");
    eprintln!("Insert done");

    // Verify
    let rows: Vec<(u64,)> = client
        .query("SELECT id FROM st_native_test")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 42);
    eprintln!("SUCCESS: FORMAT Native insert works!");
}
