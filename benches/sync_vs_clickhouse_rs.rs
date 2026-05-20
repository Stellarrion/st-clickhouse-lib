//! Side-by-side benchmark: st-clickhouse sync native protocol vs the official
//! `clickhouse` Rust crate over HTTP.
//!
//! Conditions:
//! - st-clickhouse sync: native protocol at 127.0.0.1:9000
//! - clickhouse-rs: HTTP protocol at http://127.0.0.1:8123
//! - same user/password/database/query list/warmups/runs
//!
//! Run:
//! `cargo run --profile benchmark -p st-clickhouse-lib --features bench-clickhouse-rs --bin bench_vs_clickhouse_rs`

use std::time::{Duration, Instant};

use serde::Deserialize;
use st_clickhouse::sync::client::SyncClient;
use st_clickhouse::sync::config::ClientConfig;

const NATIVE_HOST: &str = "127.0.0.1";
const NATIVE_PORT: u16 = 9000;
const HTTP_URL: &str = "http://127.0.0.1:8123";
const USER: &str = "default";
const PASSWORD: &str = "test";
const DATABASE: &str = "default";
const WARMUP_RUNS: usize = 3;

type BenchResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    query: &'static str,
    runs: usize,
    rows: usize,
    kind: CaseKind,
}

#[derive(Clone, Copy)]
enum CaseKind {
    U64,
    String,
}

#[derive(clickhouse::Row, Deserialize)]
struct OneU64 {
    v: u64,
}

#[derive(clickhouse::Row, Deserialize)]
struct OneString {
    v: String,
}

const CASES: &[Case] = &[
    Case {
        name: "SELECT 1",
        query: "SELECT toUInt64(1) AS v",
        runs: 100,
        rows: 1,
        kind: CaseKind::U64,
    },
    Case {
        name: "UInt64 1K",
        query: "SELECT number AS v FROM system.numbers LIMIT 1000",
        runs: 50,
        rows: 1000,
        kind: CaseKind::U64,
    },
    Case {
        name: "UInt64 100K",
        query: "SELECT number AS v FROM system.numbers LIMIT 100000",
        runs: 20,
        rows: 100000,
        kind: CaseKind::U64,
    },
    Case {
        name: "UInt64 1M",
        query: "SELECT number AS v FROM system.numbers LIMIT 1000000",
        runs: 10,
        rows: 1000000,
        kind: CaseKind::U64,
    },
    Case {
        name: "String 1K",
        query: "SELECT toString(number) AS v FROM system.numbers LIMIT 1000",
        runs: 50,
        rows: 1000,
        kind: CaseKind::String,
    },
    Case {
        name: "String 100K",
        query: "SELECT toString(number) AS v FROM system.numbers LIMIT 100000",
        runs: 20,
        rows: 100000,
        kind: CaseKind::String,
    },
    Case {
        name: "COUNT 1M",
        query: "SELECT count() AS v FROM (SELECT number FROM system.numbers LIMIT 1000000)",
        runs: 50,
        rows: 1,
        kind: CaseKind::U64,
    },
    Case {
        name: "ORDER BY LIMIT 100",
        query: "SELECT v FROM (SELECT number AS v FROM system.numbers LIMIT 100000) ORDER BY v DESC LIMIT 100",
        runs: 30,
        rows: 100,
        kind: CaseKind::U64,
    },
    Case {
        name: "JSON string 1K",
        query: "SELECT toJSONString(map('n', number)) AS v FROM system.numbers LIMIT 1000",
        runs: 30,
        rows: 1000,
        kind: CaseKind::String,
    },
];

#[tokio::main]
async fn main() -> BenchResult {
    println!(
        "CASE\tclient\truns\tavg_ms\tmin_ms\tmedian_ms\tp99_ms\tmax_ms\tstddev_ms\tcv_pct\trows_per_sec"
    );
    for case in CASES {
        let st = bench_st(*case)?;
        print_stats(case.name, "st-clickhouse sync", &st, case.rows);
        let official = bench_official(*case).await?;
        print_stats(case.name, "clickhouse-rs", &official, case.rows);
    }
    Ok(())
}

fn bench_st(case: Case) -> BenchResult<Vec<Duration>> {
    let mut client = connect_st()?;
    for _ in 0..WARMUP_RUNS {
        let blocks = client.query(case.query)?;
        assert_rows(case, blocks.iter().map(|b| b.row_count()).sum())?;
    }
    let mut times = Vec::with_capacity(case.runs);
    for _ in 0..case.runs {
        let start = Instant::now();
        let blocks = client.query(case.query)?;
        let elapsed = start.elapsed();
        assert_rows(case, blocks.iter().map(|b| b.row_count()).sum())?;
        times.push(elapsed);
    }
    Ok(times)
}

async fn bench_official(case: Case) -> BenchResult<Vec<Duration>> {
    let client = clickhouse::Client::default()
        .with_url(HTTP_URL)
        .with_user(USER)
        .with_password(PASSWORD)
        .with_database(DATABASE);

    for _ in 0..WARMUP_RUNS {
        let rows = fetch_official_rows(&client, case).await?;
        assert_rows(case, rows)?;
    }
    let mut times = Vec::with_capacity(case.runs);
    for _ in 0..case.runs {
        let start = Instant::now();
        let rows = fetch_official_rows(&client, case).await?;
        let elapsed = start.elapsed();
        assert_rows(case, rows)?;
        times.push(elapsed);
    }
    Ok(times)
}

async fn fetch_official_rows(client: &clickhouse::Client, case: Case) -> BenchResult<usize> {
    if matches!(case.kind, CaseKind::String) {
        let rows = client.query(case.query).fetch_all::<OneString>().await?;
        let bytes = rows.iter().map(|r| r.v.len()).sum::<usize>();
        std::hint::black_box(bytes);
        Ok(rows.len())
    } else {
        let rows = client.query(case.query).fetch_all::<OneU64>().await?;
        let sum = rows.iter().fold(0u64, |acc, r| acc.wrapping_add(r.v));
        std::hint::black_box(sum);
        Ok(rows.len())
    }
}

fn connect_st() -> BenchResult<SyncClient> {
    let config = ClientConfig::default()
        .with_host(NATIVE_HOST)
        .with_port(NATIVE_PORT)
        .with_user(USER)
        .with_password(PASSWORD)
        .with_database(DATABASE)
        .with_native_json_as_string(true);
    Ok(SyncClient::connect_with_config(config)?)
}

fn assert_rows(case: Case, rows: usize) -> BenchResult {
    if rows != case.rows {
        return Err(format!("{} returned {rows} rows, expected {}", case.name, case.rows).into());
    }
    Ok(())
}

fn print_stats(name: &str, client: &str, times: &[Duration], rows: usize) {
    let mut sorted = times.to_vec();
    sorted.sort();
    let total = sorted.iter().sum::<Duration>();
    let avg = total / u32::try_from(sorted.len()).expect("benchmark run count fits u32");
    let min = sorted[0];
    let median = sorted[sorted.len() / 2];
    let p99 = sorted[(sorted.len().saturating_mul(99) / 100).min(sorted.len() - 1)];
    let max = sorted[sorted.len() - 1];
    let avg_secs = avg.as_secs_f64();
    let variance = sorted
        .iter()
        .map(|t| {
            let delta = t.as_secs_f64() - avg_secs;
            delta * delta
        })
        .sum::<f64>()
        / sorted.len() as f64;
    let stddev = variance.sqrt();
    let cv_pct = if avg_secs > 0.0 {
        (stddev / avg_secs) * 100.0
    } else {
        0.0
    };
    let rows_per_sec = rows as f64 * sorted.len() as f64 / total.as_secs_f64();
    println!(
        "{name}\t{client}\t{}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{cv_pct:.2}\t{rows_per_sec:.0}",
        sorted.len(),
        avg.as_secs_f64() * 1000.0,
        min.as_secs_f64() * 1000.0,
        median.as_secs_f64() * 1000.0,
        p99.as_secs_f64() * 1000.0,
        max.as_secs_f64() * 1000.0,
        stddev * 1000.0,
    );
}
