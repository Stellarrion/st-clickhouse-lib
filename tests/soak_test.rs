//! Long-run soak tests for st-clickhouse.
//!
//! Tests connection lifecycle, pool reuse, concurrency, cancellation,
//! large datasets, and reconnection behavior.
//!
//! All tests are marked `#[ignore]` by default — run with:
//!   cargo test --test soak_test -- --ignored --test-threads=1

use st_clickhouse::Client;
use st_clickhouse::protocol::block::{Block, ColumnInfo};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn u64_column(name: &str, values: &[u64]) -> ColumnInfo {
    let mut data = Vec::with_capacity(values.len() * 8);
    for value in values {
        data.extend_from_slice(&value.to_le_bytes());
    }
    ColumnInfo {
        name: name.into(),
        type_name: "UInt64".into(),
        data: bytes::Bytes::from(data),
        lc_materialized: bytes::Bytes::new(),
    }
}

fn push_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn string_column(name: &str, values: &[String]) -> ColumnInfo {
    let capacity = values.iter().map(|s| s.len() + 5).sum();
    let mut data = Vec::with_capacity(capacity);
    for value in values {
        push_varint(value.len() as u64, &mut data);
        data.extend_from_slice(value.as_bytes());
    }
    ColumnInfo {
        name: name.into(),
        type_name: "String".into(),
        data: bytes::Bytes::from(data),
        lc_materialized: bytes::Bytes::new(),
    }
}

fn clickhouse_host() -> String {
    std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "127.0.0.1:9000".to_string())
}

async fn connect_soak_client(host: &str) -> Client {
    match Client::connect(host).await {
        Ok(client) => client,
        Err(_) => Client::connect_with_credentials(host, "default", "test")
            .await
            .expect("soak connect should succeed"),
    }
}

/// Repeated connect → query(SELECT 1) → close cycle.
#[tokio::test]
#[ignore]
async fn repeated_connect_query_close() {
    let host = clickhouse_host();
    let n = 100usize;
    for i in 0..n {
        let client = connect_soak_client(&host).await;
        let rows: Vec<(u8,)> = client
            .query("SELECT 1")
            .all()
            .await
            .expect("connect/query/close SELECT 1 should succeed");
        assert_eq!(rows, vec![(1,)]);
        if i % 20 == 19 {
            eprintln!("  connect/query/close: {}/{}", i + 1, n);
        }
    }
}

/// Pool reuse — 500 simple queries on the same Client.
#[tokio::test]
#[ignore]
async fn pool_reuse_500_queries() {
    let host = clickhouse_host();
    let client = connect_soak_client(&host).await;
    let n = 500usize;
    let start = Instant::now();
    for i in 0..n {
        let rows: Vec<(u64,)> = client
            .query("SELECT count() FROM (SELECT number FROM system.numbers LIMIT 100)")
            .all()
            .await
            .expect("pool reuse query should succeed");
        let val = rows[0].0;
        assert_eq!(val, 100, "query {i} expected 100, got {val}");
    }
    let elapsed = start.elapsed();
    eprintln!(
        "  {n} queries in {elapsed:.2?} ({:.0} qps)",
        n as f64 / elapsed.as_secs_f64()
    );
}

/// Concurrent queries — 10 tasks × 50 queries each.
#[tokio::test]
#[ignore]
async fn concurrent_queries_10x50() {
    let host = clickhouse_host();
    let client = Arc::new(connect_soak_client(&host).await);
    let mut handles = Vec::new();
    let tasks = 10;
    let queries_per_task = 50usize;

    for t in 0..tasks {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..queries_per_task {
                let rows: Vec<(u64,)> = c
                    .query("SELECT count()")
                    .all()
                    .await
                    .expect("concurrent query should succeed");
                let val = rows[0].0;
                assert!(val > 0, "task {t} query {i} expected >0, got {val}");
            }
            eprintln!("  task {t}/{tasks} done ({queries_per_task} queries)");
        }));
    }

    for h in handles {
        h.await.expect("concurrent task should join");
    }
    eprintln!("  all {tasks} tasks completed");
}

/// Large SELECT — fetch 100K rows.
#[tokio::test]
#[ignore]
async fn large_select_100k() {
    let host = clickhouse_host();
    let client = connect_soak_client(&host).await;
    let n = 100_000u64;

    let start = Instant::now();
    let rows: Vec<(u64,)> = client
        .query("SELECT number FROM system.numbers LIMIT 100000")
        .all()
        .await
        .expect("large SELECT should succeed");
    let elapsed = start.elapsed();

    assert_eq!(rows.len() as u64, n);
    assert_eq!(rows[0].0, 0);
    assert_eq!(rows[99999].0, 99999);
    eprintln!(
        "  SELECT {n} rows in {elapsed:.2?} ({:.0} rows/sec)",
        n as f64 / elapsed.as_secs_f64()
    );
}

/// Large INSERT — insert 100K rows, SELECT back.
#[tokio::test]
#[ignore]
async fn large_insert_100k() {
    let host = clickhouse_host();
    let client = connect_soak_client(&host).await;
    let table = "soak_large_insert";
    let n = 100_000u64;

    client
        .execute(&format!(
            "CREATE TEMPORARY TABLE {table} (id UInt64, val String)"
        ))
        .await
        .expect("create large INSERT temp table");

    let start = Instant::now();
    let mut batch = client
        .begin_insert(table)
        .await
        .expect("begin large INSERT");
    let chunk = 10_000u64;
    let mut start_id = 0u64;
    while start_id < n {
        let end = (start_id + chunk).min(n);
        let ids: Vec<u64> = (start_id..end).collect();
        let vals: Vec<String> = (start_id..end).map(|i| format!("value_{i}")).collect();
        let block = Block {
            rows: ids.len(),
            columns: vec![u64_column("id", &ids), string_column("val", &vals)],
        };
        batch.send_data(&block).await.expect("send INSERT block");
        start_id = end;
    }
    batch.end().await.expect("finish large INSERT");
    let insert_elapsed = start.elapsed();

    // SELECT + verify
    let select_start = Instant::now();
    let rows: Vec<(u64, String)> = client
        .query(&format!("SELECT id, val FROM {table} ORDER BY id"))
        .all()
        .await
        .expect("select large INSERT rows");
    let select_elapsed = select_start.elapsed();

    assert_eq!(rows.len() as u64, n);
    assert_eq!(rows[0].1, "value_0");
    assert_eq!(rows[99999].1, "value_99999");
    eprintln!(
        "  INSERT {n} rows in {insert_elapsed:.2?} ({:.0} rows/sec)",
        n as f64 / insert_elapsed.as_secs_f64()
    );
    eprintln!(
        "  SELECT {n} rows in {select_elapsed:.2?} ({:.0} rows/sec)",
        n as f64 / select_elapsed.as_secs_f64()
    );
}

/// Cancellation under load — spawn queries, cancel mid-flight.
#[tokio::test]
#[ignore]
async fn cancellation_under_load() {
    let host = clickhouse_host();
    let client = Arc::new(connect_soak_client(&host).await);
    let n_cancels = 10;

    for i in 0..n_cancels {
        // Start a long query
        let c = client.clone();
        let handle = tokio::spawn(async move { c.execute("SELECT sleep(3)").await });

        // Give it a moment to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Cancel it
        client.cancel().await.unwrap_or_else(|e| {
            eprintln!("  cancel {i} result: {e} (expected if query already finished)");
        });

        let _ = handle.await;
        eprintln!("  cancel test {}/{} done", i + 1, n_cancels);
    }
    eprintln!("  all {n_cancels} cancellation tests done");
}

/// Server restart simulation — close stream, reconnect.
#[tokio::test]
#[ignore]
async fn reconnect_after_disconnect() {
    let host = clickhouse_host();
    let client = connect_soak_client(&host).await;

    // Verify we can query
    let rows: Vec<(u8,)> = client
        .query("SELECT 1")
        .all()
        .await
        .expect("initial SELECT 1 should succeed");
    assert_eq!(rows, vec![(1,)]);

    // Simulate connection drop by closing the internal stream.
    // The pool will detect this on next get() and reconnect.
    // We just reconnect with a new Client.
    drop(client);

    // Reconnect
    let client2 = connect_soak_client(&host).await;
    let rows2: Vec<(u8,)> = client2
        .query("SELECT 1")
        .all()
        .await
        .expect("SELECT 1 after reconnect should succeed");
    assert_eq!(rows2, vec![(1,)]);
    eprintln!("  reconnect after disconnect: OK");
}
