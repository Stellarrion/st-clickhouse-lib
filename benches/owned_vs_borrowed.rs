//! Owned vs borrowed materialization for 1M UInt64 rows.
//!
//! Isolates the README "1 UInt64 x 1M rows" gap across the three access paths:
//!   - query()            -> `Vec<Block>`         (owned blocks; README "owned")
//!   - query_all::<(u64,)> -> `Vec<(u64,)>`       (owned tuples; row materialization)
//!   - query_with_block_view -> BlockView callback  (borrowed; README "borrowed")
//!
//! Run: CLICKHOUSE_USER=honne CLICKHOUSE_PASSWORD=honne \
//!      cargo run --release --bin owned_vs_borrowed

use std::hint::black_box;
use std::time::{Duration, Instant};

use st_clickhouse::sync::client::SyncClient;
use st_clickhouse::sync::config::ClientConfig;

const Q: &str = "SELECT number AS v FROM system.numbers LIMIT 1000000";

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
        f(&mut c).expect("warmup run");
    }
    let mut times = Vec::with_capacity(runs);
    for _ in 0..runs {
        let start = Instant::now();
        f(&mut c).expect("timed run");
        times.push(start.elapsed());
    }
    times.sort();
    let avg = times.iter().sum::<Duration>() / runs as u32;
    let min = times[0];
    println!(
        "{label:<26} avg={avg:.3?}  min={min:.3?}  (p50={:.3?})",
        times[runs / 2]
    );
}

fn main() {
    println!("1M UInt64 rows — owned vs borrowed materialization\n");

    bench("query() Vec<Block>", 5, 30, |c| {
        let b = c.query(Q)?;
        black_box(b);
        Ok(())
    });
    bench("query_all Vec<(u64,)>", 5, 30, |c| {
        let r: Vec<(u64,)> = c.query_all(Q)?;
        black_box(r.len());
        Ok(())
    });
    bench("query_with_block_view", 5, 30, |c| {
        let mut n = 0usize;
        c.query_with_block_view(Q, |v| {
            n += v.row_count();
            Ok(())
        })?;
        black_box(n);
        Ok(())
    });
    // Simulated optimized materialization: bulk slice-copy per PlainColumn
    // (what a specialization of read_all could do) vs the per-row to_typed path.
    bench("query() + bulk slice copy", 5, 30, |c| {
        let blocks = c.query(Q)?;
        let mut total = 0usize;
        for b in &blocks {
            if b.columns.is_empty() {
                continue;
            }
            let col = b.column_by_index::<u64>(0)?;
            let v: Vec<u64> = match col.as_slice() {
                Some(s) => s.to_vec(),
                None => (0..col.len())
                    .map(|i| col.get(i))
                    .collect::<Result<_, _>>()?,
            };
            total += v.len();
        }
        black_box(total);
        Ok(())
    });
}
