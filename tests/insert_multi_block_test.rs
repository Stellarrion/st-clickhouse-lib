//! Comprehensive multi-block INSERT tests for st-clickhouse-lib.
//!
//! Each test:
//! - Creates a TEMPORARY TABLE (or uses IF NOT EXISTS + DROP)
//! - Uses begin_insert → send_data (one or more blocks) → end flow
//! - Reads back data with .query().all::<(Type,)>() to verify
//! - Tests are sequential but independent (each creates its own table)

mod common;
use st_clickhouse::compression::CompressionMethod;
use st_clickhouse::protocol::block::{Block, ColumnInfo};

// ── helpers ───────────────────────────────────────────────────────

/// Build UInt64 column data from a slice of u64 values (little-endian wire format).
fn u64_column(name: &str, values: &[u64]) -> ColumnInfo {
    let mut data = Vec::with_capacity(values.len() * 8);
    for v in values {
        data.extend_from_slice(&v.to_le_bytes());
    }
    ColumnInfo {
        name: name.into(),
        type_name: "UInt64".into(),
        data: bytes::Bytes::from(data),
        lc_materialized: bytes::Bytes::new(),
    }
}

/// Build a single-column Block of UInt64 rows.
fn u64_block(name: &str, values: &[u64]) -> Block {
    Block {
        columns: vec![u64_column(name, values)],
        rows: values.len(),
    }
}

/// Build Int32 column data.
fn i32_column(name: &str, values: &[i32]) -> ColumnInfo {
    let mut data = Vec::with_capacity(values.len() * 4);
    for v in values {
        data.extend_from_slice(&v.to_le_bytes());
    }
    ColumnInfo {
        name: name.into(),
        type_name: "Int32".into(),
        data: bytes::Bytes::from(data),
        lc_materialized: bytes::Bytes::new(),
    }
}

/// Build Float64 column data.
fn f64_column(name: &str, values: &[f64]) -> ColumnInfo {
    let mut data = Vec::with_capacity(values.len() * 8);
    for v in values {
        data.extend_from_slice(&v.to_le_bytes());
    }
    ColumnInfo {
        name: name.into(),
        type_name: "Float64".into(),
        data: bytes::Bytes::from(data),
        lc_materialized: bytes::Bytes::new(),
    }
}

/// Build String column data (varint-prefixed wire format). Each string is
/// encoded as [varint(len)][bytes...].
fn str_column(name: &str, values: &[&str]) -> ColumnInfo {
    let mut data = Vec::new();
    for s in values {
        let bytes = s.as_bytes();
        // varint length prefix
        let len = bytes.len() as u64;
        // ClickHouse varint encoding
        let mut v = len;
        loop {
            let byte = (v & 0x7F) as u8 | if v > 0x7F { 0x80 } else { 0 };
            data.push(byte);
            v >>= 7;
            if v == 0 {
                break;
            }
        }
        data.extend_from_slice(bytes);
    }
    ColumnInfo {
        name: name.into(),
        type_name: "String".into(),
        data: bytes::Bytes::from(data),
        lc_materialized: bytes::Bytes::new(),
    }
}

// ═══════════════════════════════════════════════════════════════════
// Test 1: Insert 2 blocks with different data, verify all rows
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_insert_two_blocks() {
    let client = common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS st_multi_two_blocks")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_two_blocks (id UInt64) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut session = client
        .begin_insert("st_multi_two_blocks")
        .await
        .expect("test operation failed");

    // Block 1: rows 1, 2
    let block1 = u64_block("id", &[1, 2]);
    session
        .send_data(&block1)
        .await
        .expect("test operation failed");

    // Block 2: rows 3, 4, 5
    let block2 = u64_block("id", &[3, 4, 5]);
    session
        .send_data(&block2)
        .await
        .expect("test operation failed");

    session.end().await.expect("test operation failed");

    // Verify: all 5 rows present
    let rows: Vec<(u64,)> = client
        .query("SELECT id FROM st_multi_two_blocks ORDER BY id")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[1].0, 2);
    assert_eq!(rows[2].0, 3);
    assert_eq!(rows[3].0, 4);
    assert_eq!(rows[4].0, 5);

    eprintln!("SUCCESS: test_insert_two_blocks");
}

// ═══════════════════════════════════════════════════════════════════
// Test 2: Insert 3 blocks
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_insert_three_blocks() {
    let client = common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS st_multi_three_blocks")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_three_blocks (x Int32) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut session = client
        .begin_insert("st_multi_three_blocks")
        .await
        .expect("test operation failed");

    session
        .send_data(&Block {
            columns: vec![i32_column("x", &[10, 20, 30])],
            rows: 3,
        })
        .await
        .expect("test operation failed");

    session
        .send_data(&Block {
            columns: vec![i32_column("x", &[40, 50])],
            rows: 2,
        })
        .await
        .expect("test operation failed");

    session
        .send_data(&Block {
            columns: vec![i32_column("x", &[60])],
            rows: 1,
        })
        .await
        .expect("test operation failed");

    session.end().await.expect("test operation failed");

    let rows: Vec<(i32,)> = client
        .query("SELECT x FROM st_multi_three_blocks ORDER BY x")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 6);
    let values: Vec<i32> = rows.iter().map(|r| r.0).collect();
    assert_eq!(values, vec![10, 20, 30, 40, 50, 60]);

    eprintln!("SUCCESS: test_insert_three_blocks");
}

// ═══════════════════════════════════════════════════════════════════
// Test 3: Insert with LZ4 compression
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_insert_with_lz4() {
    let client = common::connect_client()
        .await
        .with_compression(CompressionMethod::Lz4);

    client
        .execute("DROP TABLE IF EXISTS st_multi_lz4")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_lz4 (id UInt64) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut session = client
        .begin_insert("st_multi_lz4")
        .await
        .expect("test operation failed");

    session
        .send_data(&u64_block("id", &[100, 200, 300]))
        .await
        .expect("test operation failed");
    session
        .send_data(&u64_block("id", &[400, 500]))
        .await
        .expect("test operation failed");

    session.end().await.expect("test operation failed");

    let rows: Vec<(u64,)> = client
        .query("SELECT id FROM st_multi_lz4 ORDER BY id")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 5);
    let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
    assert_eq!(ids, vec![100, 200, 300, 400, 500]);

    eprintln!("SUCCESS: test_insert_with_lz4");
}

// ═══════════════════════════════════════════════════════════════════
// Test 4: Insert without compression
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_insert_with_none_compression() {
    let client = common::connect_client()
        .await
        .with_compression(CompressionMethod::None);

    client
        .execute("DROP TABLE IF EXISTS st_multi_none")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_none (val Float64) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut session = client
        .begin_insert("st_multi_none")
        .await
        .expect("test operation failed");

    session
        .send_data(&Block {
            columns: vec![f64_column("val", &[1.5, 2.5])],
            rows: 2,
        })
        .await
        .expect("test operation failed");
    session
        .send_data(&Block {
            columns: vec![f64_column("val", &[3.5, 4.5, 5.5])],
            rows: 3,
        })
        .await
        .expect("test operation failed");

    session.end().await.expect("test operation failed");

    let rows: Vec<(f64,)> = client
        .query("SELECT val FROM st_multi_none ORDER BY val")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 5);
    // f64 comparison with epsilon
    let vals: Vec<f64> = rows.iter().map(|r| r.0).collect();
    let expected = [1.5, 2.5, 3.5, 4.5, 5.5];
    for (a, b) in vals.iter().zip(expected.iter()) {
        assert!((a - b).abs() < 1e-10, "mismatch: {a} != {b}");
    }

    eprintln!("SUCCESS: test_insert_with_none_compression");
}

// ═══════════════════════════════════════════════════════════════════
// Test 5: Multiple inserts into a single-column table
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_insert_single_column_multiple_blocks() {
    let client = common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS st_multi_single_col")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_single_col (value Float64) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut session = client
        .begin_insert("st_multi_single_col")
        .await
        .expect("test operation failed");

    // 4 blocks of varying sizes
    let batches: &[&[f64]] = &[&[0.1, 0.2], &[1.0, 1.1, 1.2, 1.3], &[], &[99.9]];

    for batch in batches {
        let block = Block {
            columns: vec![f64_column("value", batch)],
            rows: batch.len(),
        };
        session
            .send_data(&block)
            .await
            .expect("test operation failed");
    }

    session.end().await.expect("test operation failed");

    let rows: Vec<(f64,)> = client
        .query("SELECT value FROM st_multi_single_col ORDER BY value")
        .all()
        .await
        .expect("test operation failed");
    // Empty block should be no-op; total = 2 + 4 + 0 + 1 = 7 rows
    assert_eq!(rows.len(), 7);
    let vals: Vec<f64> = rows.iter().map(|r| r.0).collect();
    assert!((vals[0] - 0.1).abs() < 1e-10);
    assert!((vals[1] - 0.2).abs() < 1e-10);
    assert!((vals[2] - 1.0).abs() < 1e-10);
    assert!((vals[3] - 1.1).abs() < 1e-10);
    assert!((vals[4] - 1.2).abs() < 1e-10);
    assert!((vals[5] - 1.3).abs() < 1e-10);
    assert!((vals[6] - 99.9).abs() < 1e-10);

    eprintln!("SUCCESS: test_insert_single_column_multiple_blocks");
}

// ═══════════════════════════════════════════════════════════════════
// Test 6: Insert empty block (should be no-op)
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_insert_empty_block() {
    let client = common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS st_multi_empty_block")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_empty_block (id UInt64) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut session = client
        .begin_insert("st_multi_empty_block")
        .await
        .expect("test operation failed");

    // Send an empty block (0 rows) — should be a no-op
    let empty = Block {
        columns: vec![ColumnInfo {
            name: "id".into(),
            type_name: "UInt64".into(),
            data: bytes::Bytes::new(),
            lc_materialized: bytes::Bytes::new(),
        }],
        rows: 0,
    };
    session
        .send_data(&empty)
        .await
        .expect("test operation failed");

    // Now send real data
    session
        .send_data(&u64_block("id", &[42]))
        .await
        .expect("test operation failed");

    session.end().await.expect("test operation failed");

    let rows: Vec<(u64,)> = client
        .query("SELECT id FROM st_multi_empty_block ORDER BY id")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(
        rows.len(),
        1,
        "empty blocks should not add rows; got {rows:?}"
    );
    assert_eq!(rows[0].0, 42);

    eprintln!("SUCCESS: test_insert_empty_block");
}

// ═══════════════════════════════════════════════════════════════════
// Test 7: Insert String data (variable-length)
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_insert_strings() {
    let client = common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS st_multi_strings")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_strings (name String) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut session = client
        .begin_insert("st_multi_strings")
        .await
        .expect("test operation failed");

    // Block 1: short strings
    session
        .send_data(&Block {
            columns: vec![str_column("name", &["alice", "bob"])],
            rows: 2,
        })
        .await
        .expect("test operation failed");

    // Block 2: empty string + longer string
    session
        .send_data(&Block {
            columns: vec![str_column("name", &["", "charlie-delta-echo"])],
            rows: 2,
        })
        .await
        .expect("test operation failed");

    // Block 3: unicode
    session
        .send_data(&Block {
            columns: vec![str_column("name", &["zażółć", "gęślą", "jaźń"])],
            rows: 3,
        })
        .await
        .expect("test operation failed");

    session.end().await.expect("test operation failed");

    let rows: Vec<(String,)> = client
        .query("SELECT name FROM st_multi_strings ORDER BY name")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 7);
    let names: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "",
            "alice",
            "bob",
            "charlie-delta-echo",
            "gęślą",
            "jaźń",
            "zażółć"
        ]
    );

    eprintln!("SUCCESS: test_insert_strings");
}

// ═══════════════════════════════════════════════════════════════════
// Test 8: Insert all numeric types
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_insert_numeric_types() {
    let client = common::connect_client().await;

    // ── UInt8 ──
    client
        .execute("DROP TABLE IF EXISTS st_multi_num_u8")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_num_u8 (v UInt8) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut session = client
        .begin_insert("st_multi_num_u8")
        .await
        .expect("test operation failed");
    session
        .send_data(&Block {
            columns: vec![ColumnInfo {
                name: "v".into(),
                type_name: "UInt8".into(),
                data: bytes::Bytes::from(vec![0u8, 128, 255]),
                lc_materialized: bytes::Bytes::new(),
            }],
            rows: 3,
        })
        .await
        .expect("test operation failed");
    session.end().await.expect("test operation failed");

    let rows: Vec<(u8,)> = client
        .query("SELECT v FROM st_multi_num_u8 ORDER BY v")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, 0);
    assert_eq!(rows[1].0, 128);
    assert_eq!(rows[2].0, 255);

    // ── Int8 ──
    client
        .execute("DROP TABLE IF EXISTS st_multi_num_i8")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_num_i8 (v Int8) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut session = client
        .begin_insert("st_multi_num_i8")
        .await
        .expect("test operation failed");
    session
        .send_data(&Block {
            columns: vec![ColumnInfo {
                name: "v".into(),
                type_name: "Int8".into(),
                data: bytes::Bytes::from(
                    [-128i8, 0, 127]
                        .iter()
                        .map(|v| u8::from_ne_bytes(v.to_ne_bytes()))
                        .collect::<Vec<u8>>(),
                ),
                lc_materialized: bytes::Bytes::new(),
            }],
            rows: 3,
        })
        .await
        .expect("test operation failed");
    session.end().await.expect("test operation failed");

    let rows: Vec<(i8,)> = client
        .query("SELECT v FROM st_multi_num_i8 ORDER BY v")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, -128);
    assert_eq!(rows[1].0, 0);
    assert_eq!(rows[2].0, 127);

    // ── UInt16 ──
    client
        .execute("DROP TABLE IF EXISTS st_multi_num_u16")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_num_u16 (v UInt16) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut data = Vec::with_capacity(6);
    data.extend_from_slice(&0u16.to_le_bytes());
    data.extend_from_slice(&65535u16.to_le_bytes());
    data.extend_from_slice(&42u16.to_le_bytes());

    let mut session = client
        .begin_insert("st_multi_num_u16")
        .await
        .expect("test operation failed");
    session
        .send_data(&Block {
            columns: vec![ColumnInfo {
                name: "v".into(),
                type_name: "UInt16".into(),
                data: bytes::Bytes::from(data),
                lc_materialized: bytes::Bytes::new(),
            }],
            rows: 3,
        })
        .await
        .expect("test operation failed");
    session.end().await.expect("test operation failed");

    let rows: Vec<(u16,)> = client
        .query("SELECT v FROM st_multi_num_u16 ORDER BY v")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, 0);
    assert_eq!(rows[1].0, 42);
    assert_eq!(rows[2].0, 65535);

    // ── Int16 ──
    client
        .execute("DROP TABLE IF EXISTS st_multi_num_i16")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_num_i16 (v Int16) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut data = Vec::with_capacity(6);
    data.extend_from_slice(&(-32768i16).to_le_bytes());
    data.extend_from_slice(&0i16.to_le_bytes());
    data.extend_from_slice(&32767i16.to_le_bytes());

    let mut session = client
        .begin_insert("st_multi_num_i16")
        .await
        .expect("test operation failed");
    session
        .send_data(&Block {
            columns: vec![ColumnInfo {
                name: "v".into(),
                type_name: "Int16".into(),
                data: bytes::Bytes::from(data),
                lc_materialized: bytes::Bytes::new(),
            }],
            rows: 3,
        })
        .await
        .expect("test operation failed");
    session.end().await.expect("test operation failed");

    let rows: Vec<(i16,)> = client
        .query("SELECT v FROM st_multi_num_i16 ORDER BY v")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, -32768);
    assert_eq!(rows[1].0, 0);
    assert_eq!(rows[2].0, 32767);

    // ── UInt32 ──
    client
        .execute("DROP TABLE IF EXISTS st_multi_num_u32")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_num_u32 (v UInt32) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut session = client
        .begin_insert("st_multi_num_u32")
        .await
        .expect("test operation failed");
    session
        .send_data(&Block {
            columns: vec![ColumnInfo {
                name: "v".into(),
                type_name: "UInt32".into(),
                data: bytes::Bytes::from(
                    [0u32, 42, 0xFFFFFFFF]
                        .iter()
                        .flat_map(|v| v.to_le_bytes())
                        .collect::<Vec<u8>>(),
                ),
                lc_materialized: bytes::Bytes::new(),
            }],
            rows: 3,
        })
        .await
        .expect("test operation failed");
    session.end().await.expect("test operation failed");

    let rows: Vec<(u32,)> = client
        .query("SELECT v FROM st_multi_num_u32 ORDER BY v")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, 0);
    assert_eq!(rows[1].0, 42);
    assert_eq!(rows[2].0, 0xFFFFFFFF);

    // ── Int32 ──
    client
        .execute("DROP TABLE IF EXISTS st_multi_num_i32")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_num_i32 (v Int32) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut session = client
        .begin_insert("st_multi_num_i32")
        .await
        .expect("test operation failed");
    session
        .send_data(&Block {
            columns: vec![i32_column("v", &[-1_000_000, 0, 1_000_000])],
            rows: 3,
        })
        .await
        .expect("test operation failed");
    session.end().await.expect("test operation failed");

    let rows: Vec<(i32,)> = client
        .query("SELECT v FROM st_multi_num_i32 ORDER BY v")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, -1_000_000);
    assert_eq!(rows[1].0, 0);
    assert_eq!(rows[2].0, 1_000_000);

    // ── UInt64 ──
    client
        .execute("DROP TABLE IF EXISTS st_multi_num_u64")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_num_u64 (v UInt64) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut session = client
        .begin_insert("st_multi_num_u64")
        .await
        .expect("test operation failed");
    session
        .send_data(&u64_block("v", &[0, 1, 0xFFFFFFFFFFFFFFFF]))
        .await
        .expect("test operation failed");
    session.end().await.expect("test operation failed");

    let rows: Vec<(u64,)> = client
        .query("SELECT v FROM st_multi_num_u64 ORDER BY v")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, 0);
    assert_eq!(rows[1].0, 1);
    assert_eq!(rows[2].0, 0xFFFFFFFFFFFFFFFF);

    // ── Int64 ──
    client
        .execute("DROP TABLE IF EXISTS st_multi_num_i64")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_num_i64 (v Int64) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut data = Vec::with_capacity(24);
    data.extend_from_slice(&(-9_223_372_036_854_775_808i64).to_le_bytes());
    data.extend_from_slice(&0i64.to_le_bytes());
    data.extend_from_slice(&9_223_372_036_854_775_807i64.to_le_bytes());

    let mut session = client
        .begin_insert("st_multi_num_i64")
        .await
        .expect("test operation failed");
    session
        .send_data(&Block {
            columns: vec![ColumnInfo {
                name: "v".into(),
                type_name: "Int64".into(),
                data: bytes::Bytes::from(data),
                lc_materialized: bytes::Bytes::new(),
            }],
            rows: 3,
        })
        .await
        .expect("test operation failed");
    session.end().await.expect("test operation failed");

    let rows: Vec<(i64,)> = client
        .query("SELECT v FROM st_multi_num_i64 ORDER BY v")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, -9_223_372_036_854_775_808i64);
    assert_eq!(rows[1].0, 0);
    assert_eq!(rows[2].0, 9_223_372_036_854_775_807i64);

    // ── Float32 ──
    client
        .execute("DROP TABLE IF EXISTS st_multi_num_f32")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_num_f32 (v Float32) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut data = Vec::with_capacity(12);
    data.extend_from_slice(&(-1.5f32).to_le_bytes());
    data.extend_from_slice(&0.0f32.to_le_bytes());
    let f32_piish = "3.14".parse::<f32>().expect("test operation failed");
    data.extend_from_slice(&f32_piish.to_le_bytes());

    let mut session = client
        .begin_insert("st_multi_num_f32")
        .await
        .expect("test operation failed");
    session
        .send_data(&Block {
            columns: vec![ColumnInfo {
                name: "v".into(),
                type_name: "Float32".into(),
                data: bytes::Bytes::from(data),
                lc_materialized: bytes::Bytes::new(),
            }],
            rows: 3,
        })
        .await
        .expect("test operation failed");
    session.end().await.expect("test operation failed");

    let rows: Vec<(f32,)> = client
        .query("SELECT v FROM st_multi_num_f32 ORDER BY v")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 3);
    assert!((rows[0].0 - (-1.5f32)).abs() < 1e-6);
    assert_eq!(rows[1].0, 0.0);
    assert!((rows[2].0 - f32_piish).abs() < 1e-6);

    // ── Float64 ──
    client
        .execute("DROP TABLE IF EXISTS st_multi_num_f64")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_num_f64 (v Float64) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut session = client
        .begin_insert("st_multi_num_f64")
        .await
        .expect("test operation failed");
    session
        .send_data(&Block {
            columns: vec![f64_column("v", &[-1e308, 0.0, 1e308])],
            rows: 3,
        })
        .await
        .expect("test operation failed");
    session.end().await.expect("test operation failed");

    let rows: Vec<(f64,)> = client
        .query("SELECT v FROM st_multi_num_f64 ORDER BY v")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 3);
    assert!((rows[0].0 - (-1e308)).abs() < 1e290);
    assert_eq!(rows[1].0, 0.0);
    assert!((rows[2].0 - 1e308).abs() < 1e290);

    eprintln!("SUCCESS: test_insert_numeric_types");
}

// ═══════════════════════════════════════════════════════════════════
// Test 9: Insert, clear block, insert different data
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_insert_after_clear() {
    let client = common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS st_multi_clear")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_multi_clear (id UInt64, name String) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let mut session = client
        .begin_insert("st_multi_clear")
        .await
        .expect("test operation failed");

    // Round 1: insert 2 rows
    let mut block = Block {
        columns: vec![
            u64_column("id", &[1, 2]),
            str_column("name", &["alpha", "beta"]),
        ],
        rows: 2,
    };
    session
        .send_data(&block)
        .await
        .expect("test operation failed");

    // Clear block data (replace with new values, same column names/types)
    block.columns = vec![
        u64_column("id", &[3, 4, 5]),
        str_column("name", &["gamma", "delta", "epsilon"]),
    ];
    block.rows = 3;
    session
        .send_data(&block)
        .await
        .expect("test operation failed");

    // Clear again — single row
    block.columns = vec![u64_column("id", &[6]), str_column("name", &["zeta"])];
    block.rows = 1;
    session
        .send_data(&block)
        .await
        .expect("test operation failed");

    session.end().await.expect("test operation failed");

    let rows: Vec<(u64, String)> = client
        .query("SELECT id, name FROM st_multi_clear ORDER BY id")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 6);
    assert_eq!(rows[0], (1, "alpha".into()));
    assert_eq!(rows[1], (2, "beta".into()));
    assert_eq!(rows[2], (3, "gamma".into()));
    assert_eq!(rows[3], (4, "delta".into()));
    assert_eq!(rows[4], (5, "epsilon".into()));
    assert_eq!(rows[5], (6, "zeta".into()));

    eprintln!("SUCCESS: test_insert_after_clear");
}

#[tokio::test]
async fn dropping_active_insert_session_does_not_poison_pool() {
    let client = common::connect_client_pool(1).await;
    client
        .execute("DROP TABLE IF EXISTS st_drop_active_insert")
        .await
        .expect("drop stale table");
    client
        .execute("CREATE TABLE st_drop_active_insert (id UInt64) ENGINE = Memory")
        .await
        .expect("create table");

    let session = client
        .begin_insert("st_drop_active_insert")
        .await
        .expect("begin insert");
    drop(session); // must close the mid-INSERT socket instead of pooling it

    let probe: u64 = client
        .query("SELECT toUInt64(1)")
        .scalar()
        .await
        .expect("pool reconnects after abandoned INSERT");
    assert_eq!(probe, 1);

    client
        .execute("DROP TABLE st_drop_active_insert")
        .await
        .expect("cleanup table");
}
