//! Benchmark: st-clickhouse vs C++ clickhouse-cpp.
//!
//! Runs the same workload shape as clickhouse-cpp's `bench/bench.cpp`.
//! Requires a ClickHouse server at 127.0.0.1:9000.
//!
//! Run: cargo run --release --bin bench_vs_ch
//!
//! C++ reference (from profile.rs):
//!   cold connect+query:  1.21ms
//!   warm query:          591µs
//!   bulk 100K read:      1.19ms (84M rows/s)

use std::time::{Duration, Instant};

use st_clickhouse::sync::client::SyncClient;

const HOST: &str = "127.0.0.1:9000";
type BenchResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn main() -> BenchResult {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║    st-clickhouse vs clickhouse-cpp — benchmark             ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!("Server:  clickhouse/clickhouse-server (Docker)");
    println!("Host:    {HOST}");
    println!("Client:  st-clickhouse sync v{}", env!("CARGO_PKG_VERSION"));
    println!();

    // ── 1. Connect + handshake ──
    connect_benchmark()?;

    // ── 2. SELECT number, number, number FROM system.numbers LIMIT N ──
    // Matches C++ bench: SelectNumber
    select_benchmark(
        "SELECT number FROM system.numbers LIMIT 1000",
        "SELECT 1 col x 1000 rows",
    )?;
    select_benchmark(
        "SELECT number, number, number FROM system.numbers LIMIT 1000",
        "SELECT 3 cols x 1000 rows (C++ ref)",
    )?;
    select_benchmark(
        "SELECT number, number, number, number, number, number, number, number, number, number FROM system.numbers LIMIT 100",
        "SELECT 10 cols x 100 rows (C++ ref alt)",
    )?;

    // ── 3. Bulk reads ──
    select_benchmark(
        "SELECT number FROM system.numbers LIMIT 100000",
        "SELECT 1 col x 100K rows",
    )?;
    select_benchmark(
        "SELECT number FROM system.numbers LIMIT 1000000",
        "SELECT 1 col x 1M rows",
    )?;

    // ── 4. INSERT benchmark ──
    insert_benchmark()?;

    // ── 5. Large column count ──
    wide_benchmark()?;

    println!();
    println!("── C++ reference (from existing profile) ──");
    println!("cold connect+query:  1.21ms");
    println!("warm query:          591µs");
    println!("bulk 100K read:      1.19ms (84M/s)");
    Ok(())
}

/// Benchmark: connect + handshake (cold start).
fn connect_benchmark() -> BenchResult {
    println!("── Connect + Handshake ──");

    let start = Instant::now();
    let _client = connect()?;
    let elapsed = start.elapsed();
    println!("  connect:         {:.3?}", elapsed);

    // Warm connect
    let start = Instant::now();
    for _ in 0..10 {
        let _c = connect()?;
    }
    let avg = start.elapsed() / 10;
    println!("  connect (warm):  {:.3?}  (avg of 10)", avg);
    Ok(())
}

/// Benchmark: SELECT with warm connection.
fn select_benchmark(query: &str, label: &str) -> BenchResult {
    let mut client = connect()?;

    // Warmup
    for _ in 0..3 {
        let _ = client.query(query);
    }

    // Timed runs
    const RUNS: u32 = 20;
    let mut times = Vec::with_capacity(RUNS as usize);
    let mut total_rows = 0usize;

    for _ in 0..RUNS {
        let start = Instant::now();
        let blocks = client.query(query)?;
        let elapsed = start.elapsed();
        times.push(elapsed);
        total_rows += blocks.iter().map(|b| b.row_count()).sum::<usize>();
    }

    times.sort();
    let avg = times.iter().sum::<Duration>() / RUNS;
    let min = times[0];
    let max = times[times.len() - 1];
    let rows_per_sec = total_rows as f64 / times.iter().sum::<Duration>().as_secs_f64();

    println!("── {label} ──");
    println!("  avg: {avg:.3?}  min: {min:.3?}  max: {max:.3?}  ({RUNS} runs)");
    println!(
        "  throughput: {:.0} rows/s  ({:.2}M rows/s)",
        rows_per_sec,
        rows_per_sec / 1_000_000.0
    );
    Ok(())
}

/// Benchmark: INSERT throughput.
fn insert_benchmark() -> BenchResult {
    println!("── INSERT ──");

    let mut client = connect()?;

    // Create test table
    client.execute(
        "CREATE TABLE IF NOT EXISTS __bench_test (id UInt64, val String) ENGINE = Memory",
    )?;
    client.execute("TRUNCATE TABLE __bench_test")?;

    // Build blocks with 10K rows
    let block = build_test_block(10_000);
    const RUNS: u32 = 10;

    let start = Instant::now();
    for _ in 0..RUNS {
        client.insert(
            "INSERT INTO __bench_test (id, val) VALUES",
            "__bench_test",
            std::slice::from_ref(&block),
        )?;
    }
    let elapsed = start.elapsed();
    let inserted = 10_000 * RUNS as usize;
    let rows_per_sec = inserted as f64 / elapsed.as_secs_f64();

    println!("  inserted {inserted} rows in {elapsed:.3?}");
    println!(
        "  throughput: {:.0} rows/s  ({:.2}M rows/s)",
        rows_per_sec,
        rows_per_sec / 1_000_000.0
    );

    client.execute("DROP TABLE __bench_test")?;
    Ok(())
}

/// Benchmark: wide table (many columns).
fn wide_benchmark() -> BenchResult {
    println!("── Wide SELECT (50 columns) ──");

    let query = format!(
        "SELECT {} FROM system.numbers LIMIT 1000",
        (0..50)
            .map(|i| format!("number AS col{i}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    select_benchmark(&query, "50 cols x 1000 rows")
}

// ══════════════════════════════════════════════════════════════════════════
// Helpers
// ══════════════════════════════════════════════════════════════════════════

fn connect() -> BenchResult<SyncClient> {
    use std::time::Duration;
    let client = SyncClient::connect_with_timeout(HOST, Duration::from_secs(10))?;
    Ok(client)
}

fn build_test_block(rows: usize) -> st_clickhouse::sync::protocol::block::Block {
    use bytes::Bytes;
    use st_clickhouse::sync::protocol::block::{Block, ColumnInfo};

    // id: UInt64
    let mut id_data = Vec::with_capacity(rows * 8);
    for i in 0u64..rows as u64 {
        id_data.extend_from_slice(&i.to_le_bytes());
    }

    // val: String (varint-prefixed)
    let mut val_data = Vec::with_capacity(rows.saturating_mul(16));
    for i in 0u64..rows as u64 {
        let s = format!("val_{i}");
        let len = s.len() as u64;
        let mut v = len;
        loop {
            val_data.push((v & 0x7F) as u8 | if v > 0x7F { 0x80 } else { 0 });
            v >>= 7;
            if v == 0 {
                break;
            }
        }
        val_data.extend_from_slice(s.as_bytes());
    }

    Block {
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                type_name: "UInt64".to_string(),
                data: Bytes::from(id_data),
                lc_materialized: Bytes::new(),
            },
            ColumnInfo {
                name: "val".to_string(),
                type_name: "String".to_string(),
                data: Bytes::from(val_data),
                lc_materialized: Bytes::new(),
            },
        ],
        rows,
    }
}
