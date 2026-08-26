//! Fuzz the stateful exotic column bodies: LowCardinality dictionary/index
//! framing, Variant discriminators + subcolumns, Dynamic state prefixes and
//! discriminator layouts, JSON subcolumn trees, and Array/Map offset chains
//! wrapping them. Entry point is the public `parse_block` so no `pub(crate)`
//! surface is required.
//!
//! Input layout:
//!   data[0]   selector over the exotic type family (mod table length)
//!   data[1]   row count (0..=255)
//!   data[2..] column body bytes
#![no_main]

use libfuzzer_sys::fuzz_target;
use st_clickhouse::protocol::wire::write_varint;
use st_clickhouse::sync::protocol::response::parse_block;

const TYPES: &[&str] = &[
    "LowCardinality(UInt8)",
    "LowCardinality(String)",
    "LowCardinality(Nullable(String))",
    "LowCardinality(LowCardinality(UInt8))",
    "Array(LowCardinality(UInt8))",
    "Variant(UInt8)",
    "Variant(UInt8, String)",
    "Variant(Nullable(UInt8), Array(String), Dynamic)",
    "Array(Variant(UInt8, String))",
    "Dynamic",
    "Array(Dynamic)",
    "Map(String, Dynamic)",
    "Map(String, Variant(UInt8, String))",
    "JSON",
    "Array(JSON)",
    "Object('json')",
    "Nullable(Dynamic)",
    "Nullable(Variant(UInt8, String))",
];

fn build_block(type_name: &str, rows: u8, body: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(body.len() + type_name.len() + 16);
    buf.push(0); // table name
    buf.push(0); // block info terminator
    write_varint(&mut buf, 1).ok();
    write_varint(&mut buf, u64::from(rows)).ok();
    buf.push(0); // column name
    write_varint(&mut buf, type_name.len() as u64).ok();
    buf.extend_from_slice(type_name.as_bytes());
    buf.push(0); // custom serialization byte
    buf.extend_from_slice(body);
    buf
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }
    let type_name = TYPES[usize::from(data[0]) % TYPES.len()];
    let block_buf = build_block(type_name, data[1], &data[2..]);
    let mut pos = 0usize;
    let _ = parse_block(&block_buf, &mut pos);
});
