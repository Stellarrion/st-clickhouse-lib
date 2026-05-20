#![cfg(test)]

//! Benchmark: st-clickhouse sync vs clickhouse-cpp (C++ reference).
//!
//! Requires a ClickHouse server running at 127.0.0.1:9000 with password "test",
//! or a server described by CLICKHOUSE_HOST / CLICKHOUSE_USER / CLICKHOUSE_PASS.
//!   docker run --rm -p 9000:9000 -e CLICKHOUSE_PASSWORD=test clickhouse/clickhouse-server:26.4.2.10
//!
//! Run: cargo test --test bench_vs_ch --release -- --nocapture

use st_clickhouse::sync::client::SyncClient;
use st_clickhouse::sync::config::ClientConfig;
use std::io::Write;
use std::time::{Duration, Instant};

fn make_config() -> ClientConfig {
    let addr = std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "127.0.0.1:9000".to_owned());
    let (host, port) = addr
        .rsplit_once(':')
        .and_then(|(host, port)| port.parse::<u16>().ok().map(|port| (host.to_owned(), port)))
        .unwrap_or_else(|| ("127.0.0.1".to_owned(), 9000));
    let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_owned());
    let password = std::env::var("CLICKHOUSE_PASS").unwrap_or_else(|_| "test".to_owned());
    ClientConfig::default()
        .with_host(&host)
        .with_port(port)
        .with_user(&user)
        .with_password(&password)
        .with_connect_timeout(Duration::from_secs(5))
        .with_query_timeout(Duration::from_secs(5))
}

fn warmup(client: &mut SyncClient) {
    let _ = client.query("SELECT 1");
}

// ── Connection Latency (10 runs) ──
#[test]
fn bench_connect_latency() {
    let mut out = std::io::stdout();
    const ITER: usize = 10;
    let mut times = Vec::with_capacity(ITER);

    for _ in 0..ITER {
        let start = Instant::now();
        let _client =
            SyncClient::connect_with_config(make_config()).expect("test operation failed");
        times.push(start.elapsed());
    }

    times.sort();
    let min = times[0];
    let median = times[times.len() / 2];
    let mean = times.iter().sum::<Duration>() / ITER as u32;
    let max = times[times.len() - 1];
    let p99 = times[(ITER.saturating_mul(99) / 100).min(times.len() - 1)];

    writeln!(out, "\n── Connect latency ({} runs) ──", ITER).expect("test operation failed");
    writeln!(out, "  min:    {:.3?}", min).expect("test operation failed");
    writeln!(out, "  median: {:.3?}", median).expect("test operation failed");
    writeln!(out, "  mean:   {:.3?}", mean).expect("test operation failed");
    writeln!(out, "  p99:    {:.3?}", p99).expect("test operation failed");
    writeln!(out, "  max:    {:.3?}", max).expect("test operation failed");
    writeln!(
        out,
        "\n  clickhouse-cpp reference: min=0.219ms median=0.343ms mean=2.21ms"
    )
    .expect("test operation failed");
    out.flush().expect("test operation failed");
}

// ── SELECT 1 (100 runs) ──
#[test]
fn bench_select_1() {
    let mut out = std::io::stdout();
    const ITER: usize = 100;
    let mut client = SyncClient::connect_with_config(make_config()).expect("test operation failed");
    warmup(&mut client);

    let mut times = Vec::with_capacity(ITER);
    for _ in 0..ITER {
        let start = Instant::now();
        let _blocks = client.query("SELECT 1").expect("test operation failed");
        times.push(start.elapsed());
    }

    times.sort();
    let min = times[0];
    let median = times[times.len() / 2];
    let mean = times.iter().sum::<Duration>() / ITER as u32;
    let max = times[times.len() - 1];
    let p99 = times[(ITER.saturating_mul(99) / 100).min(times.len() - 1)];

    writeln!(out, "\n── SELECT 1 ({} runs) ──", ITER).expect("test operation failed");
    writeln!(out, "  min:    {:.3?}", min).expect("test operation failed");
    writeln!(out, "  median: {:.3?}", median).expect("test operation failed");
    writeln!(out, "  mean:   {:.3?}", mean).expect("test operation failed");
    writeln!(out, "  p99:    {:.3?}", p99).expect("test operation failed");
    writeln!(out, "  max:    {:.3?}", max).expect("test operation failed");
    writeln!(
        out,
        "\n  clickhouse-cpp reference: min=0.404ms median=0.433ms mean=0.44ms"
    )
    .expect("test operation failed");
    out.flush().expect("test operation failed");
}

// ── SELECT number×3 FROM system.numbers LIMIT 1000 (50 runs) ──
#[test]
fn bench_select_numbers_1000() {
    let mut out = std::io::stdout();
    const ITER: usize = 50;
    let mut client = SyncClient::connect_with_config(make_config()).expect("test operation failed");
    warmup(&mut client);

    let mut times = Vec::with_capacity(ITER);
    for _ in 0..ITER {
        let start = Instant::now();
        let blocks = client
            .query("SELECT number, number, number FROM system.numbers LIMIT 1000")
            .expect("test operation failed");
        let rows: usize = blocks.iter().map(|b| b.row_count()).sum();
        times.push((start.elapsed(), rows));
    }

    times.sort_by_key(|(d, _)| *d);
    let min = times[0].0;
    let median = times[times.len() / 2].0;
    let mean = times.iter().map(|(d, _)| d).sum::<Duration>() / ITER as u32;
    let max = times[times.len() - 1].0;
    let p99 = times[(ITER.saturating_mul(99) / 100).min(times.len() - 1)].0;
    let total_rows: usize = times.iter().map(|(_, r)| r).sum();

    writeln!(
        out,
        "\n── SELECT number×3 LIMIT 1000 ({} runs, {} rows total) ──",
        ITER, total_rows
    )
    .expect("test operation failed");
    writeln!(out, "  min:    {:.3?}", min).expect("test operation failed");
    writeln!(out, "  median: {:.3?}", median).expect("test operation failed");
    writeln!(out, "  mean:   {:.3?}", mean).expect("test operation failed");
    writeln!(out, "  p99:    {:.3?}", p99).expect("test operation failed");
    writeln!(out, "  max:    {:.3?}", max).expect("test operation failed");
    writeln!(
        out,
        "\n  clickhouse-cpp reference: min=0.515ms median=0.564ms mean=0.57ms"
    )
    .expect("test operation failed");
    out.flush().expect("test operation failed");
}

// ── SELECT number×10 FROM system.numbers LIMIT 100 (50 runs) ──
#[test]
fn bench_select_numbers_10col() {
    let mut out = std::io::stdout();
    const ITER: usize = 50;
    let mut client = SyncClient::connect_with_config(make_config()).expect("test operation failed");
    warmup(&mut client);

    let cols: Vec<&str> = std::iter::repeat_n("number", 10).collect();
    let query = format!("SELECT {} FROM system.numbers LIMIT 100", cols.join(", "));

    let mut times = Vec::with_capacity(ITER);
    for _ in 0..ITER {
        let start = Instant::now();
        let blocks = client.query(&query).expect("test operation failed");
        let rows: usize = blocks.iter().map(|b| b.row_count()).sum();
        times.push((start.elapsed(), rows));
    }

    times.sort_by_key(|(d, _)| *d);
    let min = times[0].0;
    let median = times[times.len() / 2].0;
    let mean = times.iter().map(|(d, _)| d).sum::<Duration>() / ITER as u32;
    let max = times[times.len() - 1].0;
    let p99 = times[(ITER.saturating_mul(99) / 100).min(times.len() - 1)].0;

    writeln!(out, "\n── SELECT number×10 LIMIT 100 ({} runs) ──", ITER)
        .expect("test operation failed");
    writeln!(out, "  min:    {:.3?}", min).expect("test operation failed");
    writeln!(out, "  median: {:.3?}", median).expect("test operation failed");
    writeln!(out, "  mean:   {:.3?}", mean).expect("test operation failed");
    writeln!(out, "  p99:    {:.3?}", p99).expect("test operation failed");
    writeln!(out, "  max:    {:.3?}", max).expect("test operation failed");
    writeln!(
        out,
        "\n  clickhouse-cpp reference: min=0.572ms median=0.61ms mean=0.62ms"
    )
    .expect("test operation failed");
    out.flush().expect("test operation failed");
}

// ── Data integrity check ──
#[test]
fn quick_test() {
    let mut out = std::io::stdout();
    let mut client = SyncClient::connect_with_config(make_config()).expect("test operation failed");

    // SELECT 1
    let blocks = client.query("SELECT 1").expect("test operation failed");
    let rows: usize = blocks.iter().map(|b| b.row_count()).sum();
    assert!(!blocks.is_empty(), "should have at least one block");
    assert!(rows > 0, "should have rows");
    for b in &blocks {
        for c in &b.columns {
            writeln!(
                out,
                "  col '{}': type '{}', data {} bytes",
                c.name,
                c.type_name,
                c.data.len()
            )
            .expect("test operation failed");
        }
    }
    writeln!(out, "SELECT 1: {rows} rows in {} blocks ✓", blocks.len())
        .expect("test operation failed");
    out.flush().expect("test operation failed");

    // SELECT number×3 LIMIT 1000
    let blocks = client
        .query("SELECT number, number, number FROM system.numbers LIMIT 1000")
        .expect("test operation failed");
    let rows: usize = blocks.iter().map(|b| b.row_count()).sum();
    writeln!(
        out,
        "SELECT LIMIT 1000: {rows} rows in {} blocks ✓",
        blocks.len()
    )
    .expect("test operation failed");
    assert_eq!(rows, 1000, "LIMIT 1000 should return exactly 1000 rows");
    out.flush().expect("test operation failed");
}

#[test]
fn quick_test_protocol_revision_54464() {
    let mut client = SyncClient::connect_with_config(make_config().with_client_revision(54464))
        .expect("test operation failed");
    assert_eq!(client.server_info().negotiated_revision, 54464);

    let blocks = client.query("SELECT 1").expect("test operation failed");
    let rows: usize = blocks.iter().map(|b| b.row_count()).sum();
    assert_eq!(rows, 1);
}

#[test]
fn quick_test_protocol_revision_54483() {
    let mut client = SyncClient::connect_with_config(make_config().with_client_revision(54483))
        .expect("test operation failed");
    let negotiated_revision = client.server_info().negotiated_revision;
    assert!(negotiated_revision <= 54483);
    if negotiated_revision < 54483 {
        eprintln!(
            "server negotiated protocol revision {negotiated_revision}; skipping exact 54483 assertion"
        );
    } else {
        assert_eq!(negotiated_revision, 54483);
    }

    let blocks = client.query("SELECT 1").expect("test operation failed");
    let rows: usize = blocks.iter().map(|b| b.row_count()).sum();
    assert_eq!(rows, 1);
}
