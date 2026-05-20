//! Edge case, error recovery, and stress tests.
//!
//! All CREATE TABLE statements use TEMPORARY tables with ENGINE = Memory
//! to avoid polluting the server and minimize disk I/O.

mod common;
use st_clickhouse::ClickHouseColumnData;
use st_clickhouse::protocol::block::{Block, ColumnInfo};

// ═══════════════════════════════════════════════════════════════
// 1. test_empty_table: SELECT from empty table returns 0 rows, no error
// ═══════════════════════════════════════════════════════════════
#[tokio::test]
async fn test_empty_table() {
    let client = common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS st_edge_empty")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_edge_empty (id UInt64, name String) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let rows: Vec<(u64, String)> = client
        .query("SELECT id, name FROM st_edge_empty")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 0, "empty table should return 0 rows, no error");
}

// ═══════════════════════════════════════════════════════════════
// 2. test_drop_nonexistent_table: DROP TABLE IF EXISTS doesn't error
// ═══════════════════════════════════════════════════════════════
#[tokio::test]
async fn test_drop_nonexistent_table() {
    let client = common::connect_client().await;
    let result = client
        .execute("DROP TABLE IF EXISTS st_edge_nonexistent_xyzzy_12345")
        .await;
    assert!(
        result.is_ok(),
        "DROP TABLE IF EXISTS on non-existent table should not error"
    );
}

// ═══════════════════════════════════════════════════════════════
// 3. test_special_column_names: backtick-quoted special column names
// ═══════════════════════════════════════════════════════════════
#[tokio::test]
async fn test_special_column_names() {
    let client = common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS st_edge_special_cols")
        .await
        .expect("test operation failed");
    client
        .execute(
            "CREATE TABLE st_edge_special_cols \
             (`col with spaces` UInt64, `col\"quote\"` String, `col\\`backtick\\`` UInt8) \
             ENGINE = Memory",
        )
        .await
        .expect("test operation failed");

    // Insert data
    client
        .execute("INSERT INTO st_edge_special_cols VALUES (42, 'hello', 7)")
        .await
        .expect("test operation failed");

    // Select back via block().column::<T>("name")
    let block = client
        .query("SELECT `col with spaces`, `col\"quote\"`, `col\\`backtick\\`` FROM st_edge_special_cols")
        .block()
        .await
        .expect("test operation failed");
    assert_eq!(block.row_count(), 1);

    let val1: u64 = block
        .column::<u64>("col with spaces")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    assert_eq!(val1, 42);
    let val2: String = block
        .column::<String>("col\"quote\"")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    assert_eq!(val2, "hello");
    let val3: u8 = block
        .column::<u8>("col`backtick`")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    assert_eq!(val3, 7);
}

// ═══════════════════════════════════════════════════════════════
// 4. test_very_long_string: Insert and retrieve a 1 MB string
// ═══════════════════════════════════════════════════════════════
#[tokio::test]
async fn test_very_long_string() {
    let client = common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS st_edge_long_str")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_edge_long_str (data String) ENGINE = Memory")
        .await
        .expect("test operation failed");

    // Insert a 1 MB string via the native block path.
    let payload = "x".repeat(1_048_576);
    let mut data = Vec::with_capacity(payload.len() + 4);
    let mut len = payload.len() as u64;
    loop {
        let byte = (len & 0x7f) as u8;
        len >>= 7;
        data.push(if len == 0 { byte } else { byte | 0x80 });
        if len == 0 {
            break;
        }
    }
    data.extend_from_slice(payload.as_bytes());
    let mut session = client
        .begin_insert("st_edge_long_str")
        .await
        .expect("test operation failed");
    let block = Block {
        columns: vec![ColumnInfo {
            name: "data".into(),
            type_name: "String".into(),
            data: bytes::Bytes::from(data),
            lc_materialized: bytes::Bytes::new(),
        }],
        rows: 1,
    };
    session
        .send_data(&block)
        .await
        .expect("test operation failed");
    session.end().await.expect("test operation failed");

    // Verify length via server-side function
    let rows: Vec<(u64,)> = client
        .query("SELECT length(data) FROM st_edge_long_str")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1048576);

    // Retrieve the actual string and verify client-side
    let block = client
        .query("SELECT data FROM st_edge_long_str")
        .block()
        .await
        .expect("test operation failed");
    let val: String = block
        .column::<String>("data")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    assert_eq!(val.len(), 1_048_576);
    assert!(val.chars().all(|c| c == 'x'));
}

// ═══════════════════════════════════════════════════════════════
// 5. test_many_columns: 120 columns, insert, select back
// ═══════════════════════════════════════════════════════════════
#[tokio::test]
async fn test_many_columns() {
    let client = common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS st_edge_many_cols")
        .await
        .expect("test operation failed");

    // Generate CREATE TABLE with 120 columns
    let col_count = 120usize;
    let col_defs: Vec<String> = (0..col_count).map(|i| format!("col_{i} UInt64")).collect();
    let create_sql = format!(
        "CREATE TABLE st_edge_many_cols ({}) ENGINE = Memory",
        col_defs.join(", ")
    );
    client
        .execute(&create_sql)
        .await
        .expect("test operation failed");

    // Insert one row — each column gets its index as value
    let values: Vec<String> = (0..col_count).map(|i| i.to_string()).collect();
    let insert_sql = format!(
        "INSERT INTO st_edge_many_cols VALUES ({})",
        values.join(", ")
    );
    client
        .execute(&insert_sql)
        .await
        .expect("test operation failed");

    // Select all columns back
    let block = client
        .query("SELECT * FROM st_edge_many_cols")
        .block()
        .await
        .expect("test operation failed");
    assert_eq!(block.row_count(), 1);
    assert_eq!(block.column_count(), col_count);

    // Spot-check first and last column
    let v0: u64 = block
        .column::<u64>("col_0")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    assert_eq!(v0, 0);
    let v_last: u64 = block
        .column::<u64>(&format!("col_{}", col_count - 1))
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    assert_eq!(v_last, (col_count - 1) as u64);
}

// ═══════════════════════════════════════════════════════════════
// 6. test_many_rows: Insert 100K+ rows via INSERT ... SELECT, verify count
// ═══════════════════════════════════════════════════════════════
#[tokio::test]
async fn test_many_rows() {
    let client = common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS st_edge_many_rows")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_edge_many_rows (n UInt64) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let row_count = 100_000u64;
    let insert_sql = format!(
        "INSERT INTO st_edge_many_rows SELECT number FROM system.numbers LIMIT {row_count}"
    );
    client
        .execute(&insert_sql)
        .await
        .expect("test operation failed");

    // Verify count
    let rows: Vec<(u64,)> = client
        .query("SELECT count() FROM st_edge_many_rows")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, row_count);
}

// ═══════════════════════════════════════════════════════════════
// 7. test_tcp_long_running: Connection stays alive after idle period
// ═══════════════════════════════════════════════════════════════
#[tokio::test]
async fn test_tcp_long_running() {
    let client = common::connect_client().await;

    // Sleep for 10 seconds
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    // Connection should still be alive — run a ping and a query
    client.ping().await.expect("ping should succeed after idle");
    let block = client
        .query("SELECT 1 AS val")
        .block()
        .await
        .expect("test operation failed");
    assert_eq!(block.row_count(), 1);
}

// ═══════════════════════════════════════════════════════════════
// 8. test_error_recovery: Bad query → error, then good query succeeds
// ═══════════════════════════════════════════════════════════════
#[tokio::test]
async fn test_error_recovery() {
    let client = common::connect_client().await;

    // Execute a bad query — must return Err
    let result = client
        .query("SELECT * FROM nonexistent_table_xyzzy")
        .block()
        .await;
    assert!(result.is_err(), "bad query should return error");

    // Execute a good query — connection should recover
    let block = client
        .query("SELECT 1 AS ok")
        .block()
        .await
        .expect("test operation failed");
    assert_eq!(block.row_count(), 1);
}

// ═══════════════════════════════════════════════════════════════
// 9. test_consecutive_errors: Multiple bad queries in a row, then a good one
// ═══════════════════════════════════════════════════════════════
#[tokio::test]
async fn test_consecutive_errors() {
    let client = common::connect_client().await;

    // Multiple bad queries
    for i in 0..5 {
        let result = client
            .query("SELECT * FROM nonexistent_xyzzy")
            .block()
            .await;
        assert!(result.is_err(), "bad query #{i} should return error");
    }

    // Then a good query — connection should still be usable
    let block = client
        .query("SELECT 2 AS ok")
        .block()
        .await
        .expect("test operation failed");
    assert_eq!(block.row_count(), 1);
}

// ═══════════════════════════════════════════════════════════════
// 10. test_large_result_set: SELECT 100K rows from system.numbers
// ═══════════════════════════════════════════════════════════════
#[tokio::test]
async fn test_large_result_set() {
    let client = common::connect_client().await;

    let limit = 100_000u64;
    let rows: Vec<(u64,)> = client
        .query(&format!("SELECT number FROM system.numbers LIMIT {limit}"))
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(
        rows.len() as u64,
        limit,
        "should return exactly {limit} rows"
    );
}

// ═══════════════════════════════════════════════════════════════
// 11. test_zero_rows_insert: INSERT with empty block doesn't crash
// ═══════════════════════════════════════════════════════════════
#[tokio::test]
async fn test_zero_rows_insert() {
    let client = common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS st_edge_zero_insert")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_edge_zero_insert (x UInt64) ENGINE = Memory")
        .await
        .expect("test operation failed");

    // Full-text INSERT that produces no rows.
    client
        .execute("INSERT INTO st_edge_zero_insert SELECT toUInt64(1) WHERE 0")
        .await
        .expect("test operation failed");

    // Also test native INSERT with zero rows via the block API
    let mut session = client
        .begin_insert("st_edge_zero_insert")
        .await
        .expect("test operation failed");
    let empty_block = Block {
        columns: vec![ColumnInfo {
            name: "x".into(),
            type_name: "UInt64".into(),
            data: bytes::Bytes::new(),
            lc_materialized: bytes::Bytes::new(),
        }],
        rows: 0,
    };
    session
        .send_data(&empty_block)
        .await
        .expect("test operation failed");
    session.end().await.expect("test operation failed");

    // Verify 0 rows (both inserts sent 0 rows)
    let rows: Vec<(u64,)> = client
        .query("SELECT count() FROM st_edge_zero_insert")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows[0].0, 0, "no rows should have been inserted");
}

// ═══════════════════════════════════════════════════════════════
// 12. test_null_only_column: SELECT from Nullable column where all values are NULL
// ═══════════════════════════════════════════════════════════════
#[tokio::test]
async fn test_null_only_column() {
    let client = common::connect_client().await;

    // 10 rows, all NULL
    let block = client
        .query("SELECT CAST(NULL AS Nullable(UInt64)) AS n FROM system.numbers LIMIT 10")
        .block()
        .await
        .expect("test operation failed");
    assert_eq!(block.row_count(), 10, "should return 10 rows");
    assert!(block.column_count() > 0, "should have at least one column");

    // Read as Option<u64> — all should be None
    let col = block
        .column::<Option<u64>>("n")
        .expect("test operation failed");
    assert_eq!(col.len(), 10);
    for i in 0..col.len() {
        let v: Option<u64> = col.get(i).expect("test operation failed");
        assert!(v.is_none(), "all values should be NULL");
    }
}

// ═══════════════════════════════════════════════════════════════
// 13. test_concurrent_inserts: Concurrent INSERT via pool
// ═══════════════════════════════════════════════════════════════
#[tokio::test]
async fn test_concurrent_inserts() {
    let client = std::sync::Arc::new(common::connect_client_pool(4).await);
    client
        .execute("DROP TABLE IF EXISTS st_edge_concurrent")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_edge_concurrent (id UInt64, val String) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut handles = Vec::new();
    for i in 0..10u64 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let sql = format!("INSERT INTO st_edge_concurrent VALUES ({i}, 'task_{i}')");
            c.execute(&sql).await.expect("test operation failed");
        }));
    }

    for h in handles {
        h.await.expect("test operation failed");
    }

    let count: Vec<(u64,)> = client
        .query("SELECT count() FROM st_edge_concurrent")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(
        count[0].0, 10,
        "should have 10 rows from concurrent inserts"
    );
}

// ═══════════════════════════════════════════════════════════════
// 14. test_insert_special_characters: Unicode, emoji, null bytes, quotes, backslashes
// ═══════════════════════════════════════════════════════════════
#[tokio::test]
async fn test_insert_special_characters() {
    let client = common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS st_edge_special_chars")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_edge_special_chars (data String) ENGINE = Memory")
        .await
        .expect("test operation failed");

    // Build a string with special characters, including null bytes
    let special =
        "Hello 世界 🌍 émoji! \"quotes\" 'single' \\backslash\\ \0null\0 tab\there newline\nhere";
    let data_bytes = special.as_bytes();

    // Encode as String column: [varint_len][raw_bytes]
    let mut raw = Vec::new();
    st_clickhouse::protocol::wire::write_varint(&mut raw, data_bytes.len() as u64)
        .expect("test operation failed");
    raw.extend_from_slice(data_bytes);

    // Send via native INSERT
    let mut session = client
        .begin_insert("st_edge_special_chars")
        .await
        .expect("test operation failed");
    let block = Block {
        columns: vec![ColumnInfo {
            name: "data".into(),
            type_name: "String".into(),
            data: bytes::Bytes::from(raw),
            lc_materialized: bytes::Bytes::new(),
        }],
        rows: 1,
    };
    session
        .send_data(&block)
        .await
        .expect("test operation failed");
    session.end().await.expect("test operation failed");

    // Retrieve and verify byte-for-byte
    let block = client
        .query("SELECT data FROM st_edge_special_chars")
        .block()
        .await
        .expect("test operation failed");
    let val: String = block
        .column::<String>("data")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    assert_eq!(
        val.as_bytes(),
        data_bytes,
        "special characters round-trip failed"
    );
}
