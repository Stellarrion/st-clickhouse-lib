//! UInt64 column decode/access breakdown.
//!
//! Isolates where time goes for a UInt64 column. `read_column` is a zero-copy
//! slice borrow (decode is free); the entire cost is *access*. Measures three
//! access paths against a raw `&[u64]` lower bound (the C++ equivalent), with
//! the column decoded once so timing reflects pure access cost.
//!
//! Run with: `cargo run --release --bin uint64_breakdown`

use std::hint::black_box;
use std::time::Instant;

use st_clickhouse::column::{AnyColumnData, ClickHouseColumn, PlainColumnData};
use st_clickhouse::protocol::block::ReadColumnContext;

const COUNT: usize = 100_000;
const ITERS: usize = 500;

fn time<F: FnMut()>(label: &str, mut f: F) {
    for _ in 0..10 {
        f();
    }
    let start = Instant::now();
    for _ in 0..ITERS {
        f();
    }
    let per = start.elapsed() / ITERS as u32;
    let gb_s = (COUNT as f64 * 8.0) / per.as_secs_f64() / 1e9;
    println!("{label:<28} per_iter={per:>10?}  ({gb_s:>5.1} GB/s)");
}

fn main() -> st_clickhouse::Result<()> {
    let buf: Vec<u8> = (0u64..COUNT as u64).flat_map(|v| v.to_le_bytes()).collect();
    let aligned = (buf.as_ptr() as usize).is_multiple_of(8);

    // Decode once: read_column is a zero-copy slice borrow.
    let mut ctx = ReadColumnContext::new(COUNT, &buf);
    let col = <u64 as ClickHouseColumn>::read_column(&mut ctx).expect("decode");
    let as_slice = col.as_slice().expect("aligned");
    let raw = unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u64, COUNT) };
    // AnyColumnData::UInt64 — what `read_all`/cursor pre-extracts; `to_typed` is
    // the per-row accessor the row-materialization fast path actually calls.
    let any = AnyColumnData::UInt64(
        PlainColumnData::<u64>::read_from_bytes(&buf, COUNT).expect("bench buffer sized for COUNT"),
    );

    println!(
        "uint64: {COUNT} elems ({}B), buf 8-aligned={aligned}. Column decoded once; access-only below.\n",
        buf.len()
    );

    // Decode cost, for reference (not access).
    time("decode only (ref)", || {
        let mut c = ReadColumnContext::new(COUNT, &buf);
        let col = <u64 as ClickHouseColumn>::read_column(&mut c).expect("decode");
        black_box(&col);
    });

    // Access paths — same memory, hot in cache, so deltas = compute/vectorization.
    time("raw &[u64] sum", || {
        black_box(raw.iter().sum::<u64>());
    });
    time("as_slice sum", || {
        black_box(as_slice.iter().sum::<u64>());
    });
    time("get(i) loop", || {
        let s: u64 = (0..col.len()).map(|i| col.get(i).expect("in-bounds")).sum();
        black_box(s);
    });
    time("to_typed (row path)", || {
        // The actual `read_all`/cursor per-row call: TypeId ladder + variant
        // match + bounds check + load, resolved fresh every row.
        let s: u64 = (0..COUNT)
            .map(|i| unsafe { any.to_typed::<u64>(i).expect("in-bounds") })
            .sum();
        black_box(s);
    });

    Ok(())
}
