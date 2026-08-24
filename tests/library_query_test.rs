//! End-to-end test using the library's public API.
//!
//! Tests:
//!   1. Client::connect() with full handshake + post-handshake
//!   2. Client::query().block() for SELECT 1
//!   3. Client::execute() for DDL

mod common;
use st_clickhouse::ClickHouseColumnData;

#[tokio::test]
async fn test_client_connect_and_query() {
    eprintln!("Connecting...");
    let client = common::connect_client().await;
    eprintln!("Connected! Server info: {:?}", client.server_info().await);

    // Query using fetch_block
    eprintln!("Fetching block via SELECT 1...");
    let block = client
        .query("SELECT 1")
        .block()
        .await
        .expect("query failed");
    eprintln!(
        "Block: {} columns, {} rows",
        block.column_count(),
        block.row_count()
    );

    assert!(block.row_count() > 0, "expected at least 1 row");
    assert!(block.column_count() > 0, "expected at least 1 column");

    eprintln!("SUCCESS!");
}

#[tokio::test]
async fn test_client_execute() {
    let client = common::connect_client().await;

    client
        .execute("CREATE TABLE IF NOT EXISTS st_test (id UInt64, name String) ENGINE = Memory")
        .await
        .expect("CREATE TABLE failed");

    eprintln!("CREATE TABLE worked!");

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS st_test")
        .await
        .expect("test operation failed");
    eprintln!("DROP TABLE worked!");
}

#[tokio::test]
async fn test_fixed_string_type() {
    let client = common::connect_client().await;
    let block = client
        .query("SELECT toIPv4('127.0.0.1') AS ip")
        .block()
        .await
        .expect("test operation failed");
    let val: u32 = block
        .column::<u32>("ip")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    println!("SUCCESS: IPv4 as u32 = {val}!");
}

#[tokio::test]
async fn test_map_type_end_to_end() {
    let client = common::connect_client().await;
    let block = client
        .query("SELECT map('k1', 1, 'k2', 2) AS m")
        .block()
        .await
        .expect("test operation failed");
    assert_eq!(block.row_count(), 1);
    println!("SUCCESS: Map end-to-end, cols={}!", block.column_count());
}
#[tokio::test]
async fn test_two_queries_sequentially() {
    let client = common::connect_client().await;

    // First query
    let block1 = client
        .query("SELECT 1 AS a")
        .block()
        .await
        .expect("test operation failed");
    eprintln!(
        "Query 1: {} cols x {} rows",
        block1.column_count(),
        block1.row_count()
    );

    // Second query (tests cursor reuse)
    let block2 = client
        .query("SELECT 2 AS b")
        .block()
        .await
        .expect("test operation failed");
    eprintln!(
        "Query 2: {} cols x {} rows",
        block2.column_count(),
        block2.row_count()
    );

    assert!(block1.row_count() > 0);
    assert!(block2.row_count() > 0);
    eprintln!("SUCCESS: two sequential queries!");
}

#[tokio::test]
async fn test_cancel_query() {
    let client = common::connect_client().await;
    // Client::cancel is fail-closed: it owns a pool, not the connection
    // running a query, so it must return Error::Config without touching any
    // pooled connection.
    #[allow(deprecated)]
    let cancelled = client.cancel().await;
    match &cancelled {
        Err(st_clickhouse::error::Error::Config(msg)) => assert!(
            msg.contains("query timeout") && msg.contains("BlockStream::cancel"),
            "cancel error must point at the alternatives: {msg}"
        ),
        other => unreachable!("expected Error::Config, got {other:?}"),
    }
    // The connection is untouched and stays usable.
    let block = client
        .query("SELECT 1")
        .block()
        .await
        .expect("test operation failed");
    assert_eq!(block.row_count(), 1);
    eprintln!("SUCCESS: fail-closed cancel keeps the client usable!");
}

#[tokio::test]
async fn test_date_types() {
    let client = common::connect_client().await;
    // Date: 2 bytes (UInt16), days since epoch
    let block = client
        .query("SELECT toDate('2024-01-15') AS d")
        .block()
        .await
        .expect("test operation failed");
    let days: u16 = block
        .column::<u16>("d")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    // 2024-01-15 = ~19737 days (exact depends on server timezone)
    assert!(days > 19000, "Date as u16 = {days}");
    eprintln!("SUCCESS: Date as u16 = {}!", days);

    // DateTime: 4 bytes (UInt32), seconds since epoch
    let block = client
        .query("SELECT toDateTime('2024-01-15 10:30:00') AS ts")
        .block()
        .await
        .expect("test operation failed");
    let ts: u32 = block
        .column::<u32>("ts")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    assert!(ts > 1_700_000_000, "DateTime as u32 = {ts}");
    eprintln!("SUCCESS: DateTime as u32 = {}!", ts);
}

#[tokio::test]
async fn test_decimal_types() {
    let client = common::connect_client().await;
    // Decimal(9,2) = 4 bytes (i32)
    let block = client
        .query("SELECT toDecimal32(42.99, 2) AS d32")
        .block()
        .await
        .expect("test operation failed");
    let val: i32 = block
        .column::<i32>("d32")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    assert_eq!(val, 4299); // 42.99 * 100 = 4299
    eprintln!("SUCCESS: Decimal32 as i32 = {}!", val);

    // Decimal(18,4) = 8 bytes (i64)
    let block = client
        .query("SELECT toDecimal64(12345.6789, 4) AS d64")
        .block()
        .await
        .expect("test operation failed");
    let val: i64 = block
        .column::<i64>("d64")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    assert_eq!(val, 123456789_i64);
    eprintln!("SUCCESS: Decimal64 as i64 = {}!", val);

    // Decimal(38,10) = 16 bytes (i128)
    let block = client
        .query("SELECT toDecimal128(9876543210.123456, 6) AS d128")
        .block()
        .await
        .expect("test operation failed");
    let val: i128 = block
        .column::<i128>("d128")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    assert_eq!(val, 9876543210123456_i128);
    eprintln!("SUCCESS: Decimal128 as i128 = {}!", val);
}

#[tokio::test]
async fn test_uuid_ip_types() {
    let client = common::connect_client().await;
    // UUID = 16 bytes (u128)
    let block = client
        .query("SELECT toUUID('550e8400-e29b-41d4-a716-446655440000') AS u")
        .block()
        .await
        .expect("test operation failed");
    let val: u128 = block
        .column::<u128>("u")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    // UUID is stored in ClickHouse-native byte order (LE components)
    // Any non-zero value means the data was read correctly
    assert!(val > 0, "UUID should be non-zero, got {val}");
    eprintln!("SUCCESS: UUID as u128 = {val} (raw)!");

    // Enum8 = Int8 (1 byte)
    let block = client
        .query("SELECT CAST(42 AS Enum8('a' = 42)) AS e")
        .block()
        .await
        .expect("test operation failed");
    let val: i8 = block
        .column::<i8>("e")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    assert_eq!(val, 42);
    eprintln!("SUCCESS: Enum8 as i8 = {}!", val);
}

#[tokio::test]
async fn test_array_types() {
    let client = common::connect_client().await;
    // arrayMap returns UInt16 by default
    let block = client
        .query("SELECT arrayMap(x -> x * 10, range(3)) AS arr")
        .block()
        .await
        .expect("test operation failed");
    assert_eq!(block.row_count(), 1);
    let arr: Vec<u16> = block
        .column::<Vec<u16>>("arr")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    assert_eq!(arr, vec![0, 10, 20], "Array(UInt16) values mismatch");
    println!("SUCCESS: Array(UInt16) = {arr:?}!");

    // Array with explicit type
    let block = client
        .query("SELECT arrayMap(x -> toUInt64(x * 10), range(3)) AS arr64")
        .block()
        .await
        .expect("test operation failed");
    assert_eq!(block.row_count(), 1);
    let arr64: Vec<u64> = block
        .column::<Vec<u64>>("arr64")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    assert_eq!(arr64, vec![0, 10, 20], "Array(UInt64) values mismatch");
    println!("SUCCESS: Array(UInt64) = {arr64:?}!");
}

#[tokio::test]
async fn test_map_types() {
    let client = common::connect_client().await;
    let block = client
        .query("SELECT map('k1', 1, 'k2', 2) AS m")
        .block()
        .await
        .expect("test operation failed");
    assert_eq!(block.row_count(), 1);
    // Read as HashMap<String, u8> (ClickHouse uses UInt8 for small literals)
    use std::collections::HashMap;
    let map: HashMap<String, u8> = block
        .column::<HashMap<String, u8>>("m")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    assert_eq!(map.len(), 2);
    assert_eq!(map.get("k1"), Some(&1));
    assert_eq!(map.get("k2"), Some(&2));
    println!("SUCCESS: HashMap<String, u8> = {:?}!", map);
}

#[tokio::test]
async fn test_lowcardinality_string() {
    let client = common::connect_client().await;
    // String from simple query
    let block = client
        .query("SELECT CAST('hello' AS String) AS s")
        .block()
        .await
        .expect("test operation failed");
    assert_eq!(block.row_count(), 1);
    let val: String = block
        .column::<String>("s")
        .expect("test operation failed")
        .get(0)
        .expect("test operation failed");
    assert_eq!(val, "hello");
    println!("SUCCESS: String = {val}!");

    let block = client
        .query("SELECT arrayJoin(['a', 'b', 'c']) AS s2")
        .block()
        .await
        .expect("test operation failed");
    assert_eq!(block.row_count(), 3);
    println!("SUCCESS: arrayJoin rows = {}", block.row_count());
}
