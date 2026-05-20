//! Column decode micro-benchmarks.
//!
//! Benchmarks the byte-level decode of ClickHouse Native protocol columns:
//! UInt64 (zero-copy), String (rowbinary varint), Array(UInt64).
//!
//! Run with: `cargo run --bin column_decode_bench --release`

use std::time::Instant;

use st_clickhouse::column::{ClickHouseColumn, ClickHouseColumnData};
use st_clickhouse::protocol::block::ReadColumnContext;

// ────────────────────────────────────────────────────────────────────
// bench_decode_uint64 — 100K zero-copy UInt64 reads
// ────────────────────────────────────────────────────────────────────

fn bench_decode_uint64() -> st_clickhouse::Result<()> {
    const COUNT: usize = 100_000;
    let total_bytes = COUNT * 8;

    // Build the wire buffer: COUNT u64 values in little-endian
    let buf: Vec<u8> = (0u64..COUNT as u64).flat_map(|v| v.to_le_bytes()).collect();
    assert_eq!(buf.len(), total_bytes);

    let iters = 100;
    let start = Instant::now();
    for _ in 0..iters {
        let mut ctx = ReadColumnContext::new(COUNT, &buf);
        let col = <u64 as ClickHouseColumn>::read_column(&mut ctx)?;
        // Force actual reads
        let sum = (0..col.len()).try_fold(0u64, |acc, i| {
            Ok::<_, st_clickhouse::Error>(acc + col.get(i)?)
        })?;
        std::hint::black_box(sum);
    }
    let elapsed = start.elapsed();
    let per_iter = elapsed / iters;

    println!(
        "bench_decode_uint64  n={COUNT:>7} ({total_bytes:>9}B)  {iters}x  total={elapsed:>8?}  per_iter={per_iter:>8?}"
    );
    Ok(())
}

// ────────────────────────────────────────────────────────────────────
// bench_decode_string — 100K String reads
// ────────────────────────────────────────────────────────────────────

fn bench_decode_string() -> st_clickhouse::Result<()> {
    const COUNT: usize = 100_000;

    // Build rowbinary wire format: [varint(len) + bytes] * COUNT
    let mut buf: Vec<u8> = Vec::new();
    for i in 0..COUNT {
        let s = format!("row_{i:05}");
        write_varint(&mut buf, s.len() as u64);
        buf.extend_from_slice(s.as_bytes());
    }

    let iters = 50;
    let start = Instant::now();
    for _ in 0..iters {
        let mut ctx = ReadColumnContext::new(COUNT, &buf);
        let col = <String as ClickHouseColumn>::read_column(&mut ctx)?;
        // Force actual reads — sum string lengths
        let total = (0..col.len()).try_fold(0usize, |acc, i| {
            Ok::<_, st_clickhouse::Error>(acc + col.get_bytes(i)?.len())
        })?;
        std::hint::black_box(total);
    }
    let elapsed = start.elapsed();
    let per_iter = elapsed / iters;

    println!(
        "bench_decode_string  n={COUNT:>7} ({:>9}B)  {iters}x  total={elapsed:>8?}  per_iter={per_iter:>8?}",
        buf.len()
    );
    Ok(())
}

// ────────────────────────────────────────────────────────────────────
// bench_decode_array — 10K Array(UInt64) reads
// ────────────────────────────────────────────────────────────────────

fn bench_decode_array() -> st_clickhouse::Result<()> {
    const COUNT: usize = 10_000;

    // Build offsets (cumulative, 8 bytes each)
    let mut offsets_buf: Vec<u8> = Vec::with_capacity(COUNT * 8);
    let mut cumulative = 0u64;
    // Distribute elements across rows unevenly
    for i in 0..COUNT {
        cumulative += if i % 3 == 0 {
            3
        } else if i % 3 == 1 {
            7
        } else {
            5
        };
        offsets_buf.extend_from_slice(&cumulative.to_le_bytes());
    }
    let total_elements = cumulative as usize;

    // Build element data: ELEMENTS u64 values
    let elements_buf: Vec<u8> = (0u64..total_elements as u64)
        .flat_map(|v| v.to_le_bytes())
        .collect();

    // Concatenate: offsets + elements
    let mut buf = offsets_buf;
    buf.extend_from_slice(&elements_buf);

    let iters = 50;
    let start = Instant::now();
    for _ in 0..iters {
        let mut ctx = ReadColumnContext::new(COUNT, &buf);
        let col = <Vec<u64> as ClickHouseColumn>::read_column(&mut ctx)?;
        // Force actual reads — sum all elements
        let total = (0..col.len()).try_fold(0u64, |acc, i| {
            Ok::<_, st_clickhouse::Error>(acc + col.get(i)?.iter().sum::<u64>())
        })?;
        std::hint::black_box(total);
    }
    let elapsed = start.elapsed();
    let per_iter = elapsed / iters;

    println!(
        "bench_decode_array   n={COUNT:>7} ({:>9}B)  {iters}x  total={elapsed:>8?}  per_iter={per_iter:>8?}  elements={total_elements}",
        buf.len()
    );
    Ok(())
}

// ────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────

fn write_varint(buf: &mut Vec<u8>, mut val: u64) {
    loop {
        let byte = (val & 0x7F) as u8;
        val >>= 7;
        if val == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

// ────────────────────────────────────────────────────────────────────

fn main() -> st_clickhouse::Result<()> {
    bench_decode_uint64()?;
    bench_decode_string()?;
    bench_decode_array()?;
    Ok(())
}
