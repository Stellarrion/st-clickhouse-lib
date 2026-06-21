//! Rust benchmark harness — all 10 README workloads.
//!
//! Runs the SAME queries as the C++ harness (clickhouse-cpp st_bench.cpp) so the
//! README "Rust vs C++" columns are directly comparable. Uses `numbers(N)` (the
//! C++ client mishandles `system.numbers LIMIT` aggregate plans).
//!
//! Run: CLICKHOUSE_USER=honne CLICKHOUSE_PASSWORD=honne \
//!      cargo run --release --bin bench_all_workloads

use std::hint::black_box;
use std::time::{Duration, Instant};

use bytes::Bytes;
use st_clickhouse::sync::client::SyncClient;
use st_clickhouse::sync::config::ClientConfig;
use st_clickhouse::sync::protocol::block::{Block, ColumnInfo};

const Q1: &str = "SELECT 1";
const Q_COUNT: &str = "SELECT count() FROM numbers(1000000)";
const Q_GROUP: &str = "SELECT g, count() AS c FROM (SELECT number % 1000 AS g FROM numbers(1000000)) \
     GROUP BY g ORDER BY g";
const Q_ORDER: &str = "SELECT number FROM numbers(1000000) ORDER BY number DESC LIMIT 100";
const Q_JSON: &str = "SELECT concat('{\"x\":', toString(number), '}') AS v FROM numbers(1000)";
const Q_UINT64_1M: &str = "SELECT number FROM numbers(1000000)";

fn connect() -> Result<SyncClient, Box<dyn std::error::Error>> {
    let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".into());
    let pass = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
    let cfg = ClientConfig::new()
        .with_host("127.0.0.1")
        .with_port(9000)
        .with_user(&user)
        .with_password(&pass)
        .with_setting("output_format_native_write_json_as_string", "1")
        .with_setting("ratio_of_defaults_for_sparse_serialization", "0");
    Ok(SyncClient::connect_with_config(cfg)?)
}

fn bench<F>(label: &str, warmup: usize, runs: usize, mut f: F)
where
    F: FnMut(&mut SyncClient) -> Result<(), Box<dyn std::error::Error>>,
{
    let mut c = connect().expect("connect");
    for _ in 0..warmup {
        f(&mut c).expect("warmup");
    }
    let mut best = Duration::MAX;
    let mut sum = Duration::ZERO;
    for _ in 0..runs {
        let t0 = Instant::now();
        f(&mut c).expect("run");
        let dt = t0.elapsed();
        best = best.min(dt);
        sum += dt;
    }
    println!(
        "{label:<26} min={:.3}ms  avg={:.3}ms",
        best.as_secs_f64() * 1000.0,
        (sum / runs as u32).as_secs_f64() * 1000.0
    );
}

fn build_u64_block(rows: usize) -> Block {
    let mut data = Vec::with_capacity(rows * 8);
    for i in 0u64..rows as u64 {
        data.extend_from_slice(&i.to_le_bytes());
    }
    Block {
        columns: vec![ColumnInfo {
            name: "id".to_string(),
            type_name: "UInt64".to_string(),
            data: Bytes::from(data),
            lc_materialized: Bytes::new(),
        }],
        rows,
    }
}

fn main() {
    println!("st-clickhouse (Rust) — 10 workloads (same queries as C++ harness)\n");

    // Setup for INSERT / ALTER.
    {
        let mut c = connect().expect("connect");
        c.execute("DROP TABLE IF EXISTS __st_bench_ins")
            .expect("ddl");
        c.execute("CREATE TABLE __st_bench_ins (id UInt64) ENGINE = Memory")
            .expect("ddl");
        c.execute("DROP TABLE IF EXISTS __st_bench_alter")
            .expect("ddl");
        c.execute("CREATE TABLE __st_bench_alter (id UInt64, val UInt64) ENGINE = Memory")
            .expect("ddl");
        let block = build_u64_block(10_000);
        c.insert(
            "INSERT INTO __st_bench_alter (id, val) VALUES",
            "__st_bench_alter",
            std::slice::from_ref(&block),
        )
        .expect("seed");
    }

    bench("SELECT 1", 3, 30, |c| {
        let b = c.query(Q1)?;
        black_box(b);
        Ok(())
    });
    bench("COUNT() 1M", 3, 30, |c| {
        let n: u64 = c.query_scalar(Q_COUNT)?;
        black_box(n);
        Ok(())
    });
    bench("GROUP BY 1K", 3, 25, |c| {
        let blocks = c.query(Q_GROUP)?;
        let rows: usize = blocks.iter().map(|b| b.row_count()).sum();
        black_box(rows);
        Ok(())
    });
    bench("ORDER BY LIMIT 100", 3, 25, |c| {
        let rows: Vec<(u64,)> = c.query_all(Q_ORDER)?;
        black_box(rows.len());
        Ok(())
    });
    bench("JSON 1K", 3, 25, |c| {
        let rows: Vec<(String,)> = c.query_all(Q_JSON)?;
        black_box(rows.len());
        Ok(())
    });
    {
        let q = format!(
            "SELECT {} FROM numbers(1000)",
            (0..50)
                .map(|i| format!("number AS col{i}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        bench("50 cols x 1K", 3, 25, move |c| {
            let blocks = c.query(&q)?;
            let rows: usize = blocks.iter().map(|b| b.row_count()).sum();
            black_box(rows);
            Ok(())
        });
    }
    bench("UInt64 1M owned", 5, 20, |c| {
        let rows: Vec<(u64,)> = c.query_all(Q_UINT64_1M)?;
        black_box(rows.len());
        Ok(())
    });
    bench("UInt64 1M borrowed", 5, 20, |c| {
        let mut n = 0usize;
        c.query_with_block_view(Q_UINT64_1M, |v| {
            n += v.row_count();
            Ok(())
        })?;
        black_box(n);
        Ok(())
    });
    bench("INSERT 10K", 2, 15, |c| {
        c.execute("TRUNCATE TABLE __st_bench_ins")?;
        let block = build_u64_block(10_000);
        c.insert(
            "INSERT INTO __st_bench_ins (id) VALUES",
            "__st_bench_ins",
            std::slice::from_ref(&block),
        )?;
        Ok(())
    });
    bench("ALTER UPDATE 5K/10K", 2, 15, |c| {
        c.execute("ALTER TABLE __st_bench_alter UPDATE val = val + 1 WHERE id < 5000")?;
        Ok(())
    });
}
