//! Fuzz the buffered block parser's column-data paths (`parse_block` ->
//! `parse_block_body` -> shared `skip_column_data` + `read_low_cardinality`).
//!
//! Input layout:
//!   data[0]      selector: 0x00..=0x7f fixed type table, 0x80..=0xff
//!                fuzz-supplied type name (varint-prefixed string read from
//!                the body, exercising `parse_type` and the `Other` fallback)
//!   data[1]      row count (0..=255)
//!   data[2..]    column body bytes (for the fuzz-type path this starts with
//!                the varint-prefixed type name string)
//!
//! The harness frames the body as a one-column Data block so every byte of
//! fuzz data lands inside the column-data parsers.
#![no_main]

use libfuzzer_sys::fuzz_target;
use st_clickhouse::protocol::wire::write_varint;
use st_clickhouse::sync::protocol::response::parse_block;

const TYPES: &[&str] = &[
    "UInt8", "UInt64", "UInt128", "UInt256", "Int8", "Int32", "Int128", "Int256",
    "Float32", "Float64", "Bool", "String", "FixedString(4)", "Date", "Date32",
    "DateTime", "DateTime64(3)", "Decimal(9, 2)", "Decimal(38, 6)", "UUID",
    "IPv4", "IPv6", "Enum8('x' = 1, 'y' = 2)", "Enum16('only' = 42)",
    "Nullable(UInt8)", "Nullable(String)", "Array(UInt8)", "Array(String)",
    "Array(Array(UInt8))", "Array(Nullable(Map(String, Array(UInt8))))",
    "Map(String, UInt64)", "Tuple(UInt8, String)", "Point", "Ring", "Polygon",
    "MultiPolygon", "Nothing", "Time", "Time64(3)", "AggregateFunction",
    "SimpleAggregateFunction", "LowCardinality(UInt8)", "LowCardinality(String)",
    "Variant(UInt8, String)", "Dynamic", "JSON", "Object('json')",
];

fn build_block(type_name: &str, rows: u8, body: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(body.len() + type_name.len() + 16);
    buf.push(0); // table name: empty string
    buf.push(0); // block info: terminator
    write_varint(&mut buf, 1).ok(); // num_columns
    write_varint(&mut buf, u64::from(rows)).ok(); // num_rows
    buf.push(0); // column name: empty string
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
    let selector = data[0];
    let rows = data[1];
    let rest = &data[2..];

    if selector < 0x80 {
        let type_name = TYPES[usize::from(selector) % TYPES.len()];
        let block_buf = build_block(type_name, rows, rest);
        let mut pos = 0usize;
        let _ = parse_block(&block_buf, &mut pos);
    } else {
        // Fuzz-controlled type name: body = <varint-len type name><body...>.
        let mut pos = 0usize;
        let Ok(type_name) = st_clickhouse::sync::protocol::wire::parse_string(rest, &mut pos)
        else {
            return;
        };
        let block_buf = build_block(&type_name, rows, &rest[pos..]);
        let mut pos = 0usize;
        let _ = parse_block(&block_buf, &mut pos);
    }
});
