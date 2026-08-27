//! Parse a complete ClickHouse TCP response buffer into blocks.
//!
//! The response is a sequence of packets. Each packet starts with a varint
//! packet type, followed by type-specific data.
//!
//! Packet types handled:
//!   1 = Data (block)
//!   2 = Exception (error)
//!   3 = Progress (skipped)
//!   5 = End of stream
//!   6 = ProfileInfo (skipped)
//!   7 = Totals (block, skipped)
//!   8 = Extremes (block, skipped)
//!   10 = Log (tag + block, skipped)
//!   11 = TableColumns (skipped)
//!   14 = ProfileEvents (tag + block, skipped)

pub use super::response_packets::{parse_response, parse_response_with_revision};
use crate::sync::error::Result;
use crate::sync::protocol::block::{Block, BlockView, ColumnInfo, ColumnView};
#[cfg(test)]
use crate::sync::protocol::revision;
use crate::sync::protocol::type_parser::{ColumnType, parse_type};
use crate::sync::protocol::wire::{self, parse_bytes, parse_string, parse_varint};
use std::io::Read;
use std::ops::Range;

/// Parse a single Data block from the response buffer.
///
/// Column data is stored as `Bytes::slice()` into `shared` — zero-copy.
pub fn parse_block(buf: &[u8], pos: &mut usize) -> Result<Block> {
    let _table = parse_string(buf, pos)?;
    parse_block_body(buf, pos)
}

pub(crate) fn parse_block_shared(shared: &bytes::Bytes, pos: &mut usize) -> Result<Block> {
    let _table = parse_string(shared, pos)?;
    parse_block_body_shared(shared, pos)
}

/// Parse a block body after the optional packet-level table/tag string has
/// already been consumed.
pub(crate) fn parse_block_body(buf: &[u8], pos: &mut usize) -> Result<Block> {
    skip_block_info(buf, pos)?;
    let num_cols = checked_count(
        parse_varint(buf, pos)?,
        "block column",
        crate::limits::MAX_BLOCK_COLUMNS,
    )?;
    let num_rows = checked_count(
        parse_varint(buf, pos)?,
        "block row",
        crate::limits::MAX_BLOCK_ROWS,
    )?;
    let mut columns = Vec::with_capacity(num_cols);

    for _ in 0..num_cols {
        let name = parse_string(buf, pos)?;
        let type_name = parse_string(buf, pos)?;
        parse_bytes(buf, pos, 1)?; // custom serialization byte

        let ct = parse_type(type_name).map_err(|e| {
            crate::sync::error::Error::Protocol(format!("bad type '{type_name}': {e}"))
        })?;
        let data = if let ColumnType::LowCardinality(inner) = &ct {
            read_low_cardinality_from_buffer(buf, pos, inner, num_rows)?
        } else {
            let col_start = *pos;
            skip_column_data(buf, pos, &ct, num_rows)?;
            let col_end = *pos;
            let col_data_start = materialized_column_data_start(buf, col_start, &ct, num_rows)?;
            buf[col_data_start..col_end].to_vec()
        };

        columns.push(ColumnInfo {
            name: name.to_owned(),
            type_name: type_name.to_owned(),
            data: data.into(), // Vec<u8> → bytes::Bytes
            lc_materialized: bytes::Bytes::new(),
        });
    }

    Ok(Block {
        columns,
        rows: num_rows,
    })
}

pub(crate) fn parse_block_body_shared(shared: &bytes::Bytes, pos: &mut usize) -> Result<Block> {
    let buf: &[u8] = shared;
    skip_block_info(buf, pos)?;
    let num_cols = checked_count(
        parse_varint(buf, pos)?,
        "block column",
        crate::limits::MAX_BLOCK_COLUMNS,
    )?;
    let num_rows = checked_count(
        parse_varint(buf, pos)?,
        "block row",
        crate::limits::MAX_BLOCK_ROWS,
    )?;
    let mut columns = Vec::with_capacity(num_cols);

    for _ in 0..num_cols {
        let name = parse_string(buf, pos)?;
        let type_name = parse_string(buf, pos)?;
        parse_bytes(buf, pos, 1)?; // custom serialization byte

        let ct = parse_column_type(type_name)?;
        let data = if let ColumnType::LowCardinality(inner) = &ct {
            bytes::Bytes::from(read_low_cardinality_from_buffer(buf, pos, inner, num_rows)?)
        } else {
            let col_start = *pos;
            skip_column_data(buf, pos, &ct, num_rows)?;
            let col_end = *pos;
            let col_data_start = materialized_column_data_start(buf, col_start, &ct, num_rows)?;
            shared.slice(col_data_start..col_end)
        };

        columns.push(ColumnInfo {
            name: name.to_owned(),
            type_name: type_name.to_owned(),
            data,
            lc_materialized: bytes::Bytes::new(),
        });
    }

    Ok(Block {
        columns,
        rows: num_rows,
    })
}

fn parse_column_type(type_name: &str) -> Result<ColumnType> {
    let ct = match type_name {
        "UInt8" => ColumnType::UInt8,
        "UInt16" => ColumnType::UInt16,
        "UInt32" => ColumnType::UInt32,
        "UInt64" => ColumnType::UInt64,
        "Int8" => ColumnType::Int8,
        "Int16" => ColumnType::Int16,
        "Int32" => ColumnType::Int32,
        "Int64" => ColumnType::Int64,
        "Float32" => ColumnType::Float32,
        "Float64" => ColumnType::Float64,
        "String" => ColumnType::String,
        "Date" => ColumnType::Date,
        "DateTime" => ColumnType::DateTime,
        "UUID" => ColumnType::UUID,
        "Bool" => ColumnType::Bool,
        "IPv4" => ColumnType::IPv4,
        "IPv6" => ColumnType::IPv6,
        "Date32" => ColumnType::Date32,
        "UInt128" => ColumnType::UInt128,
        "UInt256" => ColumnType::UInt256,
        "Int128" => ColumnType::Int128,
        "Int256" => ColumnType::Int256,
        _ => parse_type(type_name).map_err(|e| {
            crate::sync::error::Error::Protocol(format!("bad type '{type_name}': {e}"))
        })?,
    };
    Ok(ct)
}

fn materialized_column_data_start(
    buf: &[u8], start: usize, ct: &ColumnType, rows: usize,
) -> Result<usize> {
    // JSON nested below the top level keeps its 8-byte string-serialization
    // version inside the sliced data, which the column decoders misread —
    // reject it loudly instead of returning silently wrong data.
    if rows > 0 && crate::sync::protocol::skip_column::contains_nested_json(ct) {
        return Err(crate::sync::error::Error::Protocol(format!(
            "nested JSON columns are not supported in buffered block reads \
             (column type {ct}); use uncompressed reads or query_raw"
        )));
    }
    if rows == 0 || !matches!(ct, ColumnType::JSON) {
        return Ok(start);
    }
    let end = start.checked_add(8).ok_or_else(|| {
        crate::sync::error::Error::Protocol("JSON version offset overflow".into())
    })?;
    if end > buf.len() {
        return Err(crate::sync::error::Error::Protocol(
            "unexpected end of buffer parsing JSON version".into(),
        ));
    }
    Ok(end)
}

pub fn read_block<R: Read>(reader: &mut R) -> Result<Block> {
    let _table = wire::read_string(reader)?;
    read_block_body(reader)
}

pub(crate) fn read_block_view<R, F>(reader: &mut R, visitor: &mut F) -> Result<()>
where
    R: Read,
    F: FnMut(BlockView<'_>) -> Result<()>,
{
    let _table = wire::read_string(reader)?;
    read_block_body_view(reader, visitor)
}

pub(crate) fn read_block_body_view<R, F>(reader: &mut R, visitor: &mut F) -> Result<()>
where
    R: Read,
    F: FnMut(BlockView<'_>) -> Result<()>,
{
    skip_block_info_from_reader(reader)?;
    let num_cols = checked_count(
        wire::read_varint(reader)?,
        "block column",
        crate::limits::MAX_BLOCK_COLUMNS,
    )?;
    let num_rows = checked_count(
        wire::read_varint(reader)?,
        "block row",
        crate::limits::MAX_BLOCK_ROWS,
    )?;

    if num_cols == 0 && num_rows == 0 {
        return visitor(BlockView {
            columns: &[],
            rows: 0,
        });
    }

    struct PendingColumn {
        name: String,
        type_name: String,
        range: Range<usize>,
    }

    let mut pending = Vec::with_capacity(num_cols);
    let mut arena = Vec::new();

    for _ in 0..num_cols {
        let name = wire::read_string(reader)?;
        let type_name = wire::read_string_unchecked(reader)?;
        let mut custom_serialization = [0u8; 1];
        reader.read_exact(&mut custom_serialization)?;
        if custom_serialization[0] != 0 {
            return Err(crate::sync::error::Error::Protocol(format!(
                "unsupported custom serialization for column '{name}'"
            )));
        }

        let ct = parse_column_type(&type_name)?;
        let start = arena.len();
        let mut budget = crate::limits::MAX_COLUMN_BYTES;
        read_column_data_into(reader, &ct, num_rows, &mut arena, &mut budget)?;
        let end = arena.len();

        pending.push(PendingColumn {
            name,
            type_name,
            range: start..end,
        });
    }

    let columns = pending
        .iter()
        .map(|col| ColumnView {
            name: &col.name,
            type_name: &col.type_name,
            data: &arena[col.range.clone()],
        })
        .collect::<Vec<_>>();

    visitor(BlockView {
        columns: &columns,
        rows: num_rows,
    })
}

#[expect(dead_code)]
pub(crate) fn discard_block<R: Read>(reader: &mut R) -> Result<usize> {
    let _table = wire::read_string(reader)?;
    discard_block_body(reader)
}

pub(crate) fn discard_block_body<R: Read>(reader: &mut R) -> Result<usize> {
    skip_block_info_from_reader(reader)?;
    let num_cols = checked_count(
        wire::read_varint(reader)?,
        "block column",
        crate::limits::MAX_BLOCK_COLUMNS,
    )?;
    let num_rows = checked_count(
        wire::read_varint(reader)?,
        "block row",
        crate::limits::MAX_BLOCK_ROWS,
    )?;

    for _ in 0..num_cols {
        let name = wire::read_string(reader)?;
        let type_name = wire::read_string_unchecked(reader)?;
        let mut custom_serialization = [0u8; 1];
        reader.read_exact(&mut custom_serialization)?;
        if custom_serialization[0] != 0 {
            return Err(crate::sync::error::Error::Protocol(format!(
                "unsupported custom serialization for column '{name}'"
            )));
        }

        let ct = parse_column_type(&type_name)?;
        discard_column_data(reader, &ct, num_rows)?;
    }

    Ok(num_rows)
}

pub(crate) fn read_block_body<R: Read>(reader: &mut R) -> Result<Block> {
    skip_block_info_from_reader(reader)?;
    let num_cols = checked_count(
        wire::read_varint(reader)?,
        "block column",
        crate::limits::MAX_BLOCK_COLUMNS,
    )?;
    let num_rows = checked_count(
        wire::read_varint(reader)?,
        "block row",
        crate::limits::MAX_BLOCK_ROWS,
    )?;

    // Fast path: empty block marker — no columns/rows to parse.
    if num_cols == 0 && num_rows == 0 {
        return Ok(Block {
            columns: Vec::new(),
            rows: 0,
        });
    }

    struct PendingColumn {
        name: String,
        type_name: String,
        range: Range<usize>,
    }

    let mut pending = Vec::with_capacity(num_cols);
    let mut arena = Vec::new();

    for _ in 0..num_cols {
        let name = wire::read_string(reader)?;
        let type_name = wire::read_string_unchecked(reader)?;
        let mut custom_serialization = [0u8; 1];
        reader.read_exact(&mut custom_serialization)?;
        if custom_serialization[0] != 0 {
            return Err(crate::sync::error::Error::Protocol(format!(
                "unsupported custom serialization for column '{name}'"
            )));
        }

        let ct = parse_column_type(&type_name)?;
        let start = arena.len();
        let mut budget = crate::limits::MAX_COLUMN_BYTES;
        read_column_data_into(reader, &ct, num_rows, &mut arena, &mut budget)?;
        let end = arena.len();

        pending.push(PendingColumn {
            name,
            type_name,
            range: start..end,
        });
    }

    let shared = bytes::Bytes::from(arena);
    let columns = pending
        .into_iter()
        .map(|col| ColumnInfo {
            name: col.name,
            type_name: col.type_name,
            data: shared.slice(col.range),
            lc_materialized: bytes::Bytes::new(),
        })
        .collect();

    Ok(Block {
        columns,
        rows: num_rows,
    })
}

fn skip_block_info_from_reader<R: Read>(reader: &mut R) -> Result<()> {
    loop {
        let d = wire::read_varint(reader)?;
        match d {
            0 => return Ok(()),
            1 => {
                let mut b = [0u8; 1];
                reader.read_exact(&mut b)?;
            },
            2 => {
                let mut b = [0u8; 4];
                reader.read_exact(&mut b)?;
            },
            3 => {
                wire::read_varint(reader)?;
            },
            _ => return Ok(()),
        }
    }
}

fn read_column_data_into<R: Read>(
    reader: &mut R, ct: &ColumnType, rows: usize, data: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    if rows == 0 {
        return Ok(());
    }

    use ColumnType::*;
    match ct {
        UInt8 | Int8 | Bool | Enum8 => read_exact_into(reader, data, rows, budget)?,
        UInt16 | Int16 | Date | Enum16 => read_exact_into(
            reader,
            data,
            checked_column_len(rows, 2, "fixed-width column")?,
            budget,
        )?,
        UInt32 | Int32 | Float32 | Date32 | DateTime | Time | IPv4 => read_exact_into(
            reader,
            data,
            checked_column_len(rows, 4, "fixed-width column")?,
            budget,
        )?,
        UInt64 | Int64 | Float64 | DateTime64(_) | Time64(_) => read_exact_into(
            reader,
            data,
            checked_column_len(rows, 8, "fixed-width column")?,
            budget,
        )?,
        UInt128 | Int128 | UUID | IPv6 => read_exact_into(
            reader,
            data,
            checked_column_len(rows, 16, "fixed-width column")?,
            budget,
        )?,
        UInt256 | Int256 => read_exact_into(
            reader,
            data,
            checked_column_len(rows, 32, "fixed-width column")?,
            budget,
        )?,
        Decimal(1..=9, _) => read_exact_into(
            reader,
            data,
            checked_column_len(rows, 4, "fixed-width column")?,
            budget,
        )?,
        Decimal(10..=18, _) => read_exact_into(
            reader,
            data,
            checked_column_len(rows, 8, "fixed-width column")?,
            budget,
        )?,
        Decimal(19..=38, _) => read_exact_into(
            reader,
            data,
            checked_column_len(rows, 16, "fixed-width column")?,
            budget,
        )?,
        Decimal(39..=76, _) => read_exact_into(
            reader,
            data,
            checked_column_len(rows, 32, "fixed-width column")?,
            budget,
        )?,
        Decimal(precision, _) => {
            return Err(crate::sync::error::Error::Protocol(format!(
                "unsupported Decimal precision {precision}"
            )));
        },
        Nothing => read_exact_into(reader, data, rows, budget)?,
        String => {
            for _ in 0..rows {
                let len = checked_string_len(
                    read_varint_into(reader, data, budget)?,
                    "string value length",
                )?;
                // read_exact_into charges the budget before reserving, so a
                // lying length fails before this value is allocated or read.
                read_exact_into(reader, data, len, budget)?;
            }
        },
        JSON => {
            let mut ver = [0u8; 8];
            reader.read_exact(&mut ver)?;
            let version = u64::from_le_bytes(ver);
            if version != 1 && version != 4 {
                return Err(crate::sync::error::Error::Protocol(format!(
                    "materialized JSON reads require string serialization version 1 or 4, got {version}; \
                     enable {}=1 or use query_raw",
                    crate::sync::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING
                )));
            }
            for _ in 0..rows {
                let len = checked_string_len(
                    read_varint_into(reader, data, budget)?,
                    "JSON string length",
                )?;
                read_exact_into(reader, data, len, budget)?;
            }
        },
        FixedString(n) => read_exact_into(
            reader,
            data,
            checked_column_len(rows, *n, "FixedString column")?,
            budget,
        )?,
        Nullable(inner) => {
            read_exact_into(reader, data, rows, budget)?;
            read_column_data_into(reader, inner, rows, data, budget)?;
        },
        Array(inner) => {
            let elem_rows = read_offsets_into(reader, data, rows, "array offset", budget)?;
            read_column_data_into(reader, inner, elem_rows, data, budget)?;
        },
        Map(k, v) => {
            let elem_rows = read_offsets_into(reader, data, rows, "map offset", budget)?;
            read_column_data_into(reader, k, elem_rows, data, budget)?;
            read_column_data_into(reader, v, elem_rows, data, budget)?;
        },
        Tuple(elems) => {
            for elem in elems {
                read_column_data_into(reader, elem, rows, data, budget)?;
            }
        },
        LowCardinality(inner) => read_low_cardinality_into(reader, inner, rows, data, budget)?,
        Point => {
            read_column_data_into(reader, &Float64, rows, data, budget)?;
            read_column_data_into(reader, &Float64, rows, data, budget)?;
        },
        Ring => read_column_data_into(reader, &Array(Box::new(Point)), rows, data, budget)?,
        Polygon => read_column_data_into(reader, &Array(Box::new(Ring)), rows, data, budget)?,
        MultiPolygon => {
            read_column_data_into(reader, &Array(Box::new(Polygon)), rows, data, budget)?
        },
        Dynamic => {
            let state = read_dynamic_state_prefix_into(reader, data, budget)?;
            read_dynamic_body_into(reader, &state, rows, data, budget)?;
        },
        Variant(types) => {
            let mut states = Vec::with_capacity(types.len());
            for typ in types {
                states.push(read_column_state_prefix_into(reader, typ, data, budget)?);
            }
            read_variant_body_into(reader, types, &states, rows, data, budget)?;
        },
        _ => {
            return Err(crate::sync::error::Error::Protocol(format!(
                "streaming read unsupported for column type {ct:?}"
            )));
        },
    }
    Ok(())
}

#[derive(Debug, Clone)]
enum RawColumnState {
    None,
    Nullable(Box<RawColumnState>),
    Array(Box<RawColumnState>),
    Map(Box<RawColumnState>, Box<RawColumnState>),
    Tuple(Vec<RawColumnState>),
    LowCardinality(Box<RawColumnState>),
    Variant(Vec<RawColumnState>),
    Dynamic(DynamicRawState),
    Json(JsonRawState),
}

#[derive(Debug, Clone)]
struct DynamicRawState {
    version: u64,
    type_names: Vec<String>,
    type_states: Vec<RawColumnState>,
}

#[derive(Debug, Clone)]
struct JsonRawState {
    version: u64,
    dynamic_paths: Vec<DynamicRawState>,
}

fn read_column_state_prefix_into<R: Read>(
    reader: &mut R, ct: &ColumnType, data: &mut Vec<u8>, budget: &mut usize,
) -> Result<RawColumnState> {
    use ColumnType::*;
    match ct {
        Nullable(inner) => Ok(RawColumnState::Nullable(Box::new(
            read_column_state_prefix_into(reader, inner, data, budget)?,
        ))),
        Array(inner) => Ok(RawColumnState::Array(Box::new(
            read_column_state_prefix_into(reader, inner, data, budget)?,
        ))),
        Map(key, value) => Ok(RawColumnState::Map(
            Box::new(read_column_state_prefix_into(reader, key, data, budget)?),
            Box::new(read_column_state_prefix_into(reader, value, data, budget)?),
        )),
        Tuple(elems) => {
            let mut states = Vec::with_capacity(elems.len());
            for elem in elems {
                states.push(read_column_state_prefix_into(reader, elem, data, budget)?);
            }
            Ok(RawColumnState::Tuple(states))
        },
        LowCardinality(inner) => Ok(RawColumnState::LowCardinality(Box::new(
            read_column_state_prefix_into(reader, inner, data, budget)?,
        ))),
        Variant(types) => {
            let mut states = Vec::with_capacity(types.len());
            for typ in types {
                states.push(read_column_state_prefix_into(reader, typ, data, budget)?);
            }
            Ok(RawColumnState::Variant(states))
        },
        Dynamic => {
            read_dynamic_state_prefix_into(reader, data, budget).map(RawColumnState::Dynamic)
        },
        JSON => read_json_state_prefix_into(reader, data, budget).map(RawColumnState::Json),
        _ => Ok(RawColumnState::None),
    }
}

/// Read `rows` little-endian u64 Array/Map offsets into the arena, validating
/// that they are non-decreasing (cumulative prefix sums) and that the last
/// offset — the inner element row count — stays within MAX_BLOCK_ROWS.
/// Returns the inner element row count.
fn read_offsets_into<R: Read>(
    reader: &mut R, data: &mut Vec<u8>, rows: usize, name: &str, budget: &mut usize,
) -> Result<usize> {
    let nbytes = checked_column_len(rows, 8, name)?;
    let start = data.len();
    read_exact_into(reader, data, nbytes, budget)?;
    let mut total = 0usize;
    for chunk in data[start..].chunks_exact(8) {
        let mut b = [0u8; 8];
        b.copy_from_slice(chunk);
        total = checked_monotonic_offset(total, u64::from_le_bytes(b), name)?;
    }
    Ok(total)
}

fn read_json_state_prefix_into<R: Read>(
    reader: &mut R, data: &mut Vec<u8>, budget: &mut usize,
) -> Result<JsonRawState> {
    let version = read_u64_into(reader, data, budget)?;
    let mut dynamic_paths = Vec::new();
    match version {
        1 | 4 => {},
        3 => {
            let paths_count = checked_count(
                read_varint_into(reader, data, budget)?,
                "JSON path",
                crate::limits::MAX_JSON_DYNAMIC_ITEMS,
            )?;
            for _ in 0..paths_count {
                let _path = read_string_into(reader, data, "JSON path length", budget)?;
            }
            dynamic_paths.reserve(paths_count);
            for _ in 0..paths_count {
                dynamic_paths.push(read_dynamic_state_prefix_into(reader, data, budget)?);
            }
        },
        0 => {
            let _max_dynamic_paths = read_varint_into(reader, data, budget)?;
            let paths_count = checked_count(
                read_varint_into(reader, data, budget)?,
                "JSON path",
                crate::limits::MAX_JSON_DYNAMIC_ITEMS,
            )?;
            for _ in 0..paths_count {
                let _path = read_string_into(reader, data, "JSON path length", budget)?;
            }
            dynamic_paths.reserve(paths_count);
            for _ in 0..paths_count {
                dynamic_paths.push(read_dynamic_state_prefix_into(reader, data, budget)?);
            }
        },
        other => {
            return Err(crate::sync::error::Error::Protocol(format!(
                "unknown JSON serialization version {other}"
            )));
        },
    }
    Ok(JsonRawState {
        version,
        dynamic_paths,
    })
}

fn read_json_body_into<R: Read>(
    reader: &mut R, state: &JsonRawState, rows: usize, data: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    match state.version {
        1 | 4 => {
            for _ in 0..rows {
                let len = checked_string_len(
                    read_varint_into(reader, data, budget)?,
                    "JSON string length",
                )?;
                read_exact_into(reader, data, len, budget)?;
            }
        },
        3 => {
            for dynamic in &state.dynamic_paths {
                read_dynamic_body_into(reader, dynamic, rows, data, budget)?;
            }
        },
        0 => {
            for dynamic in &state.dynamic_paths {
                read_dynamic_body_into(reader, dynamic, rows, data, budget)?;
            }
            read_exact_into(
                reader,
                data,
                checked_column_len(rows, 8, "JSON offsets")?,
                budget,
            )?;
        },
        other => {
            return Err(crate::sync::error::Error::Protocol(format!(
                "unknown JSON serialization version {other}"
            )));
        },
    }
    Ok(())
}

fn read_dynamic_state_prefix_into<R: Read>(
    reader: &mut R, data: &mut Vec<u8>, budget: &mut usize,
) -> Result<DynamicRawState> {
    let version = read_u64_into(reader, data, budget)?;
    let mut type_names = Vec::new();
    let mut type_states = Vec::new();
    match version {
        0 => {},
        1 => {
            let _max_types = read_varint_into(reader, data, budget)?;
            type_names =
                read_dynamic_type_names_into(reader, data, "dynamic subcolumn types", budget)?;
            let _variant_version = read_u64_into(reader, data, budget)?;
        },
        2 | 3 => {
            type_names =
                read_dynamic_type_names_into(reader, data, "dynamic subcolumn types", budget)?;
        },
        other => {
            return Err(crate::sync::error::Error::Protocol(format!(
                "unknown Dynamic subcolumn serialization version {other}"
            )));
        },
    }
    type_states.reserve(type_names.len());
    for type_name in &type_names {
        let ct = crate::sync::protocol::type_parser::parse_type(type_name).map_err(|e| {
            crate::sync::error::Error::Protocol(format!("bad dynamic type '{type_name}': {e}"))
        })?;
        type_states.push(read_column_state_prefix_into(reader, &ct, data, budget)?);
    }
    Ok(DynamicRawState {
        version,
        type_names,
        type_states,
    })
}

fn read_dynamic_body_into<R: Read>(
    reader: &mut R, state: &DynamicRawState, rows: usize, data: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    match state.version {
        0 => Ok(()),
        1 => read_deprecated_dynamic_values_into(
            reader,
            &state.type_names,
            &state.type_states,
            rows,
            data,
            budget,
        ),
        2 | 3 => read_flattened_dynamic_values_into(
            reader,
            &state.type_names,
            &state.type_states,
            rows,
            data,
            budget,
        ),
        other => Err(crate::sync::error::Error::Protocol(format!(
            "unknown Dynamic serialization version {other}"
        ))),
    }
}

fn read_deprecated_dynamic_values_into<R: Read>(
    reader: &mut R, type_names: &[String], type_states: &[RawColumnState], rows: usize,
    data: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    if rows == 0 || type_names.is_empty() {
        return Ok(());
    }
    let start = data.len();
    read_exact_into(reader, data, rows, budget)?;
    let mut counts = vec![0usize; type_names.len()];
    for &discriminator in &data[start..start + rows] {
        let idx = usize::from(discriminator);
        if idx < counts.len() {
            counts[idx] += 1;
        } else if discriminator != u8::MAX {
            return Err(crate::sync::error::Error::Protocol(format!(
                "deprecated Dynamic discriminator {idx} exceeds type count {}",
                type_names.len()
            )));
        }
    }
    read_dynamic_subcolumns_into(reader, type_names, type_states, &counts, data, budget)
}

fn read_flattened_dynamic_values_into<R: Read>(
    reader: &mut R, type_names: &[String], type_states: &[RawColumnState], rows: usize,
    data: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    if rows == 0 || type_names.is_empty() {
        return Ok(());
    }
    let width = dynamic_discriminator_width(type_names.len());
    let len = checked_column_len(rows, width, "Dynamic discriminators")?;
    let start = data.len();
    read_exact_into(reader, data, len, budget)?;
    let mut counts = vec![0usize; type_names.len()];
    for chunk in data[start..start + len].chunks_exact(width) {
        let idx = decode_dynamic_discriminator(chunk)?;
        if idx < counts.len() {
            counts[idx] += 1;
        } else if idx != type_names.len() {
            return Err(crate::sync::error::Error::Protocol(format!(
                "Dynamic discriminator {idx} exceeds type count {}",
                type_names.len()
            )));
        }
    }
    read_dynamic_subcolumns_into(reader, type_names, type_states, &counts, data, budget)
}

fn read_dynamic_subcolumns_into<R: Read>(
    reader: &mut R, type_names: &[String], type_states: &[RawColumnState], counts: &[usize],
    data: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    for (idx, (type_name, count)) in type_names.iter().zip(counts).enumerate() {
        if *count == 0 {
            continue;
        }
        let ct = crate::sync::protocol::type_parser::parse_type(type_name).map_err(|e| {
            crate::sync::error::Error::Protocol(format!("bad dynamic type '{type_name}': {e}"))
        })?;
        let state = type_states.get(idx).unwrap_or(&RawColumnState::None);
        read_column_body_raw_into(reader, &ct, state, *count, data, budget)?;
    }
    Ok(())
}

fn read_variant_body_into<R: Read>(
    reader: &mut R, types: &[ColumnType], type_states: &[RawColumnState], rows: usize,
    data: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    let type_names = types.iter().map(ToString::to_string).collect::<Vec<_>>();
    read_variant_types_body_into(reader, &type_names, type_states, rows, data, false, budget)
}

fn read_variant_types_body_into<R: Read>(
    reader: &mut R, type_names: &[String], type_states: &[RawColumnState], rows: usize,
    data: &mut Vec<u8>, one_based_discriminators: bool, budget: &mut usize,
) -> Result<()> {
    let mode = read_u64_into(reader, data, budget)?;
    if rows == 0 || type_names.is_empty() {
        return Ok(());
    }
    match mode {
        0 => {
            let start = data.len();
            read_exact_into(reader, data, rows, budget)?;
            let mut counts = vec![0usize; type_names.len()];
            for &discriminator in &data[start..start + rows] {
                let idx = if one_based_discriminators {
                    discriminator.checked_sub(1).map(usize::from)
                } else {
                    Some(usize::from(discriminator))
                };
                if let Some(idx) = idx
                    && idx < counts.len()
                {
                    counts[idx] += 1;
                }
            }
            read_dynamic_subcolumns_into(reader, type_names, type_states, &counts, data, budget)
        },
        1 => {
            let discriminator = checked_usize(
                read_u64_into(reader, data, budget)?,
                "Variant compact discriminator",
            )?;
            let discriminator = if one_based_discriminators {
                discriminator.saturating_sub(1)
            } else {
                discriminator
            };
            let compact_rows =
                checked_usize(read_u64_into(reader, data, budget)?, "Variant compact rows")?;
            // A compact granule carries one non-empty variant for at most the
            // outer row count (all-NULL granules legally carry zero rows).
            if compact_rows > rows {
                return Err(crate::sync::error::Error::Protocol(format!(
                    "Variant compact rows {compact_rows} exceeds row count {rows}"
                )));
            }
            if discriminator < type_names.len() && compact_rows > 0 {
                let ct = crate::sync::protocol::type_parser::parse_type(&type_names[discriminator])
                    .map_err(|e| {
                        crate::sync::error::Error::Protocol(format!(
                            "bad variant type '{}': {e}",
                            type_names[discriminator]
                        ))
                    })?;
                let state = type_states
                    .get(discriminator)
                    .unwrap_or(&RawColumnState::None);
                read_column_body_raw_into(reader, &ct, state, compact_rows, data, budget)?;
            }
            Ok(())
        },
        other => Err(crate::sync::error::Error::Protocol(format!(
            "unknown Variant serialization mode {other}"
        ))),
    }
}

fn read_column_body_raw_into<R: Read>(
    reader: &mut R, ct: &ColumnType, state: &RawColumnState, rows: usize, data: &mut Vec<u8>,
    budget: &mut usize,
) -> Result<()> {
    if rows == 0 {
        return Ok(());
    }

    use ColumnType::*;
    match ct {
        Nullable(inner) => {
            read_exact_into(reader, data, rows, budget)?;
            let inner_state = match state {
                RawColumnState::Nullable(inner_state) => inner_state.as_ref(),
                _ => &RawColumnState::None,
            };
            read_column_body_raw_into(reader, inner, inner_state, rows, data, budget)
        },
        Array(inner) => {
            let mut total = 0usize;
            for _ in 0..rows {
                let offset = read_u64_into(reader, data, budget)?;
                // Offsets are cumulative prefix sums: non-decreasing, and the
                // running maximum is the inner row count, capped at
                // MAX_BLOCK_ROWS before the inner column is read.
                total = checked_monotonic_offset(total, offset, "array offset")?;
            }
            if total > 0 {
                let inner_state = match state {
                    RawColumnState::Array(inner_state) => inner_state.as_ref(),
                    _ => &RawColumnState::None,
                };
                read_column_body_raw_into(reader, inner, inner_state, total, data, budget)?;
            }
            Ok(())
        },
        Map(key, value) => {
            let mut total = 0usize;
            for _ in 0..rows {
                let offset = read_u64_into(reader, data, budget)?;
                total = checked_monotonic_offset(total, offset, "map offset")?;
            }
            if total > 0 {
                let (key_state, value_state) = match state {
                    RawColumnState::Map(key_state, value_state) => {
                        (key_state.as_ref(), value_state.as_ref())
                    },
                    _ => (&RawColumnState::None, &RawColumnState::None),
                };
                read_column_body_raw_into(reader, key, key_state, total, data, budget)?;
                read_column_body_raw_into(reader, value, value_state, total, data, budget)?;
            }
            Ok(())
        },
        Tuple(elems) => {
            let states = match state {
                RawColumnState::Tuple(states) => states.as_slice(),
                _ => &[],
            };
            for (idx, elem) in elems.iter().enumerate() {
                let elem_state = states.get(idx).unwrap_or(&RawColumnState::None);
                read_column_body_raw_into(reader, elem, elem_state, rows, data, budget)?;
            }
            Ok(())
        },
        LowCardinality(inner) => {
            let inner_state = match state {
                RawColumnState::LowCardinality(inner_state) => inner_state.as_ref(),
                _ => &RawColumnState::None,
            };
            read_lc_body_raw_into(reader, inner, inner_state, rows, data, budget)
        },
        JSON => {
            let json_state = match state {
                RawColumnState::Json(json_state) => json_state,
                _ => {
                    return Err(crate::sync::error::Error::Protocol(
                        "missing JSON state prefix".into(),
                    ));
                },
            };
            read_json_body_into(reader, json_state, rows, data, budget)
        },
        Dynamic => {
            let dynamic_state = match state {
                RawColumnState::Dynamic(dynamic_state) => dynamic_state,
                _ => {
                    return Err(crate::sync::error::Error::Protocol(
                        "missing Dynamic state prefix".into(),
                    ));
                },
            };
            read_dynamic_body_into(reader, dynamic_state, rows, data, budget)
        },
        Variant(types) => {
            let states = match state {
                RawColumnState::Variant(states) => states.as_slice(),
                _ => &[],
            };
            read_variant_body_into(reader, types, states, rows, data, budget)
        },
        Point => {
            read_column_body_raw_into(reader, &Float64, &RawColumnState::None, rows, data, budget)?;
            read_column_body_raw_into(reader, &Float64, &RawColumnState::None, rows, data, budget)
        },
        Ring => read_column_body_raw_into(
            reader,
            &Array(Box::new(Point)),
            &RawColumnState::None,
            rows,
            data,
            budget,
        ),
        Polygon => read_column_body_raw_into(
            reader,
            &Array(Box::new(Ring)),
            &RawColumnState::None,
            rows,
            data,
            budget,
        ),
        MultiPolygon => read_column_body_raw_into(
            reader,
            &Array(Box::new(Polygon)),
            &RawColumnState::None,
            rows,
            data,
            budget,
        ),
        String | Other(_) => {
            for _ in 0..rows {
                let len = checked_string_len(
                    read_varint_into(reader, data, budget)?,
                    "string value length",
                )?;
                read_exact_into(reader, data, len, budget)?;
            }
            Ok(())
        },
        FixedString(n) => read_exact_into(
            reader,
            data,
            checked_column_len(rows, *n, "FixedString column")?,
            budget,
        ),
        AggregateFunction | SimpleAggregateFunction => Err(crate::sync::error::Error::Protocol(
            format!("raw capture does not support type {ct}"),
        )),
        _ => {
            let width = ct
                .fixed_width()
                .ok_or_else(|| crate::sync::error::Error::Protocol(format!("unknown type {ct}")))?;
            read_exact_into(
                reader,
                data,
                checked_column_len(rows, width, "fixed-width column")?,
                budget,
            )
        },
    }
}

fn read_lc_body_raw_into<R: Read>(
    reader: &mut R, inner: &ColumnType, inner_state: &RawColumnState, rows: usize,
    data: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    let start = data.len();
    read_exact_into(reader, data, 24, budget)?;
    let version = u64::from_le_bytes(data[start..start + 8].try_into().map_err(|_| {
        crate::sync::error::Error::Protocol("LowCardinality version length mismatch".into())
    })?);
    if version != 1 {
        return Err(crate::sync::error::Error::Protocol(format!(
            "unsupported LowCardinality key serialization version {version}"
        )));
    }
    let serial_type = u64::from_le_bytes(data[start + 8..start + 16].try_into().map_err(|_| {
        crate::sync::error::Error::Protocol("LowCardinality metadata length mismatch".into())
    })?);
    let num_keys = checked_count(
        u64::from_le_bytes(data[start + 16..start + 24].try_into().map_err(|_| {
            crate::sync::error::Error::Protocol("LowCardinality key count length mismatch".into())
        })?),
        "LowCardinality key",
        crate::limits::MAX_JSON_DYNAMIC_ITEMS,
    )?;
    let idx_width = 1usize << (serial_type & 0x3);
    if num_keys > 0 {
        read_column_body_raw_into(reader, inner, inner_state, num_keys, data, budget)?;
    }
    let indexes = checked_usize(
        read_u64_into(reader, data, budget)?,
        "LowCardinality indexes",
    )?;
    // The native format writes exactly one index per row of the granule; a
    // different count can only be a malformed or hostile payload.
    if indexes != rows {
        return Err(crate::sync::error::Error::Protocol(format!(
            "LowCardinality index count {indexes} does not match row count {rows}"
        )));
    }
    read_exact_into(
        reader,
        data,
        checked_column_len(indexes, idx_width, "LowCardinality index")?,
        budget,
    )
}

fn read_dynamic_type_names_into<R: Read>(
    reader: &mut R, data: &mut Vec<u8>, count_name: &str, budget: &mut usize,
) -> Result<Vec<String>> {
    let type_count = checked_count(
        read_varint_into(reader, data, budget)?,
        count_name,
        crate::limits::MAX_JSON_DYNAMIC_ITEMS,
    )?;
    let mut type_names = Vec::with_capacity(type_count);
    for _ in 0..type_count {
        type_names.push(read_string_into(
            reader,
            data,
            "dynamic type length",
            budget,
        )?);
    }
    Ok(type_names)
}

fn read_string_into<R: Read>(
    reader: &mut R, data: &mut Vec<u8>, name: &str, budget: &mut usize,
) -> Result<String> {
    let len = checked_string_len(read_varint_into(reader, data, budget)?, name)?;
    let start = data.len();
    read_exact_into(reader, data, len, budget)?;
    std::str::from_utf8(&data[start..start + len])
        .map(str::to_owned)
        .map_err(|e| crate::sync::error::Error::Protocol(format!("invalid UTF-8 string: {e}")))
}

fn read_u64_into<R: Read>(reader: &mut R, data: &mut Vec<u8>, budget: &mut usize) -> Result<u64> {
    let start = data.len();
    read_exact_into(reader, data, 8, budget)?;
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&data[start..start + 8]);
    Ok(u64::from_le_bytes(bytes))
}

fn dynamic_discriminator_width(type_count: usize) -> usize {
    if u8::try_from(type_count).is_ok() {
        1
    } else if u16::try_from(type_count).is_ok() {
        2
    } else if u32::try_from(type_count).is_ok() {
        4
    } else {
        8
    }
}

fn decode_dynamic_discriminator(bytes: &[u8]) -> Result<usize> {
    let value = match bytes.len() {
        1 => u64::from(bytes[0]),
        2 => u64::from(u16::from_le_bytes([bytes[0], bytes[1]])),
        4 => u64::from(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
        8 => u64::from_le_bytes(bytes.try_into().map_err(|_| {
            crate::sync::error::Error::Protocol("Dynamic discriminator length mismatch".into())
        })?),
        _ => {
            return Err(crate::sync::error::Error::Protocol(
                "unsupported Dynamic discriminator width".into(),
            ));
        },
    };
    checked_usize(value, "Dynamic discriminator")
}

fn read_low_cardinality_into<R: Read>(
    reader: &mut R, inner: &ColumnType, rows: usize, data: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    let mut meta = [0u8; 24];
    reader.read_exact(&mut meta)?;
    let version = u64::from_le_bytes(meta[0..8].try_into().map_err(|_| {
        crate::sync::error::Error::Protocol("LowCardinality version length mismatch".into())
    })?);
    let serial_type = u64::from_le_bytes(meta[8..16].try_into().map_err(|_| {
        crate::sync::error::Error::Protocol("LowCardinality metadata length mismatch".into())
    })?);
    let idx_width = lc_idx_width(version, serial_type)?;
    let num_keys = checked_count(
        u64::from_le_bytes(meta[16..24].try_into().map_err(|_| {
            crate::sync::error::Error::Protocol("LowCardinality key count length mismatch".into())
        })?),
        "LowCardinality key",
        crate::limits::MAX_JSON_DYNAMIC_ITEMS,
    )?;
    let mut dict_data = Vec::new();
    let mut dict_budget = crate::limits::MAX_COLUMN_BYTES;
    read_column_data_into(reader, inner, num_keys, &mut dict_data, &mut dict_budget)?;
    let mut count = [0u8; 8];
    reader.read_exact(&mut count)?;
    let indexes = checked_usize(u64::from_le_bytes(count), "LowCardinality indexes")?;
    if indexes != rows {
        return Err(crate::sync::error::Error::Protocol(format!(
            "LowCardinality index count {indexes} does not match row count {rows}"
        )));
    }
    let index_len = checked_column_len(indexes, idx_width, "LowCardinality index")?;
    charge_budget(budget, index_len, "column")?;
    let mut index_data = vec![0u8; index_len];
    reader.read_exact(&mut index_data)?;
    let materialized =
        materialize_low_cardinality_inner(&dict_data, inner, &index_data, idx_width, indexes)?;
    data.extend_from_slice(&materialized);
    Ok(())
}

fn read_low_cardinality_from_buffer(
    buf: &[u8], pos: &mut usize, inner: &ColumnType, rows: usize,
) -> Result<Vec<u8>> {
    // Zero-row blocks (e.g. the header block of every SELECT) carry no column
    // bytes at all, so there is no LowCardinality header to parse.
    if rows == 0 {
        return Ok(Vec::new());
    }
    let meta = parse_bytes(buf, pos, 24)?;
    let version = u64::from_le_bytes(meta[0..8].try_into().map_err(|_| {
        crate::sync::error::Error::Protocol("LowCardinality version length mismatch".into())
    })?);
    let serial_type = u64::from_le_bytes(meta[8..16].try_into().map_err(|_| {
        crate::sync::error::Error::Protocol("LowCardinality metadata length mismatch".into())
    })?);
    let idx_width = lc_idx_width(version, serial_type)?;
    let num_keys = checked_count(
        u64::from_le_bytes(meta[16..24].try_into().map_err(|_| {
            crate::sync::error::Error::Protocol("LowCardinality key count length mismatch".into())
        })?),
        "LowCardinality key",
        crate::limits::MAX_JSON_DYNAMIC_ITEMS,
    )?;
    let dict_start = *pos;
    skip_column_data(buf, pos, inner, num_keys)?;
    let dict_data = &buf[dict_start..*pos];
    let count_bytes = parse_bytes(buf, pos, 8)?;
    let indexes = checked_usize(
        u64::from_le_bytes(count_bytes.try_into().map_err(|_| {
            crate::sync::error::Error::Protocol("LowCardinality index count length mismatch".into())
        })?),
        "LowCardinality indexes",
    )?;
    if indexes != rows {
        return Err(crate::sync::error::Error::Protocol(format!(
            "LowCardinality index count {indexes} does not match row count {rows}"
        )));
    }
    let index_data = parse_bytes(
        buf,
        pos,
        checked_column_len(indexes, idx_width, "LowCardinality index")?,
    )?;
    materialize_low_cardinality_inner(dict_data, inner, index_data, idx_width, indexes)
}

fn read_exact_into<R: Read>(
    reader: &mut R, data: &mut Vec<u8>, len: usize, budget: &mut usize,
) -> Result<()> {
    if len == 0 {
        return Ok(());
    }

    // Single-read backstop: no individual claim may exceed one column's byte
    // budget, and the charge happens before the reserve below so lying
    // lengths fail without allocating.
    if len > crate::limits::MAX_COLUMN_BYTES {
        return Err(crate::sync::error::Error::Protocol(format!(
            "column byte length {len} exceeds limit {}",
            crate::limits::MAX_COLUMN_BYTES
        )));
    }
    charge_budget(budget, len, "column")?;
    let start = data.len();
    let end = start.checked_add(len).ok_or_else(|| {
        crate::sync::error::Error::Protocol("column buffer length overflow".into())
    })?;
    data.reserve(len);
    let spare = &mut data.spare_capacity_mut()[..len];
    // SAFETY: the spare capacity is reserved for exactly `len` bytes and `u8`
    // has no invalid bit patterns. The length is published only after
    // `read_exact` initializes the full range.
    let dst = unsafe { std::slice::from_raw_parts_mut(spare.as_mut_ptr().cast::<u8>(), len) };
    reader.read_exact(dst)?;
    // SAFETY: `read_exact` succeeded, so all bytes in `start..end` are initialized.
    unsafe {
        data.set_len(end);
    }
    Ok(())
}

fn discard_column_data<R: Read>(reader: &mut R, ct: &ColumnType, rows: usize) -> Result<()> {
    if rows == 0 {
        return Ok(());
    }

    use ColumnType::*;
    match ct {
        UInt8 | Int8 | Bool | Enum8 => discard_exact(reader, rows)?,
        UInt16 | Int16 | Date | Enum16 => {
            discard_exact(reader, checked_column_len(rows, 2, "fixed-width column")?)?
        },
        UInt32 | Int32 | Float32 | Date32 | DateTime | Time | IPv4 => {
            discard_exact(reader, checked_column_len(rows, 4, "fixed-width column")?)?
        },
        UInt64 | Int64 | Float64 | DateTime64(_) | Time64(_) => {
            discard_exact(reader, checked_column_len(rows, 8, "fixed-width column")?)?
        },
        UInt128 | Int128 | UUID | IPv6 => {
            discard_exact(reader, checked_column_len(rows, 16, "fixed-width column")?)?
        },
        UInt256 | Int256 => {
            discard_exact(reader, checked_column_len(rows, 32, "fixed-width column")?)?
        },
        Decimal(1..=9, _) => {
            discard_exact(reader, checked_column_len(rows, 4, "fixed-width column")?)?
        },
        Decimal(10..=18, _) => {
            discard_exact(reader, checked_column_len(rows, 8, "fixed-width column")?)?
        },
        Decimal(19..=38, _) => {
            discard_exact(reader, checked_column_len(rows, 16, "fixed-width column")?)?
        },
        Decimal(39..=76, _) => {
            discard_exact(reader, checked_column_len(rows, 32, "fixed-width column")?)?
        },
        Decimal(precision, _) => {
            return Err(crate::sync::error::Error::Protocol(format!(
                "unsupported Decimal precision {precision}"
            )));
        },
        String => {
            for _ in 0..rows {
                let len = checked_string_len(wire::read_varint(reader)?, "string value length")?;
                discard_exact(reader, len)?;
            }
        },
        JSON => {
            let mut ver = [0u8; 8];
            reader.read_exact(&mut ver)?;
            let version = u64::from_le_bytes(ver);
            if version != 1 && version != 4 {
                return Err(crate::sync::error::Error::Protocol(format!(
                    "materialized JSON reads require string serialization version 1 or 4, got {version}; \
                     enable {}=1 or use query_raw",
                    crate::sync::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING
                )));
            }
            for _ in 0..rows {
                let len = checked_string_len(wire::read_varint(reader)?, "JSON string length")?;
                discard_exact(reader, len)?;
            }
        },
        FixedString(n) => {
            discard_exact(reader, checked_column_len(rows, *n, "FixedString column")?)?
        },
        Nullable(inner) => {
            discard_exact(reader, rows)?;
            discard_column_data(reader, inner, rows)?;
        },
        Array(inner) => {
            let elem_rows = discard_offsets(reader, rows, "array offset")?;
            discard_column_data(reader, inner, elem_rows)?;
        },
        Map(k, v) => {
            let elem_rows = discard_offsets(reader, rows, "map offset")?;
            discard_column_data(reader, k, elem_rows)?;
            discard_column_data(reader, v, elem_rows)?;
        },
        Tuple(elems) => {
            for elem in elems {
                discard_column_data(reader, elem, rows)?;
            }
        },
        LowCardinality(inner) => discard_low_cardinality(reader, inner, rows)?,
        Point => {
            discard_column_data(reader, &Float64, rows)?;
            discard_column_data(reader, &Float64, rows)?;
        },
        Ring => discard_column_data(reader, &Array(Box::new(Point)), rows)?,
        Polygon => discard_column_data(reader, &Array(Box::new(Ring)), rows)?,
        MultiPolygon => discard_column_data(reader, &Array(Box::new(Polygon)), rows)?,
        _ => {
            return Err(crate::sync::error::Error::Protocol(format!(
                "discard unsupported for column type {ct:?}"
            )));
        },
    }
    Ok(())
}

fn discard_low_cardinality<R: Read>(reader: &mut R, inner: &ColumnType, rows: usize) -> Result<()> {
    if rows == 0 {
        return Ok(());
    }

    let mut meta = [0u8; 24];
    reader.read_exact(&mut meta)?;
    let version = u64::from_le_bytes(meta[0..8].try_into().map_err(|_| {
        crate::sync::error::Error::Protocol("LowCardinality version length mismatch".into())
    })?);
    let serial_type = u64::from_le_bytes(meta[8..16].try_into().map_err(|_| {
        crate::sync::error::Error::Protocol("LowCardinality metadata length mismatch".into())
    })?);
    let idx_width = lc_idx_width(version, serial_type)?;
    let num_keys = checked_count(
        u64::from_le_bytes(meta[16..24].try_into().map_err(|_| {
            crate::sync::error::Error::Protocol("LowCardinality key count length mismatch".into())
        })?),
        "LowCardinality key",
        crate::limits::MAX_JSON_DYNAMIC_ITEMS,
    )?;
    discard_column_data(reader, inner, num_keys)?;
    let mut count = [0u8; 8];
    reader.read_exact(&mut count)?;
    let indexes = checked_usize(u64::from_le_bytes(count), "LowCardinality indexes")?;
    if indexes != rows {
        return Err(crate::sync::error::Error::Protocol(format!(
            "LowCardinality index count {indexes} does not match row count {rows}"
        )));
    }
    discard_exact(
        reader,
        checked_column_len(indexes, idx_width, "LowCardinality index")?,
    )
}

fn discard_offsets<R: Read>(reader: &mut R, rows: usize, name: &str) -> Result<usize> {
    let mut offset = [0u8; 8];
    let mut total = 0usize;
    for _ in 0..rows {
        reader.read_exact(&mut offset)?;
        // Cumulative prefix sums must be non-decreasing; the last offset is
        // the inner element count, capped at MAX_BLOCK_ROWS.
        total = checked_monotonic_offset(total, u64::from_le_bytes(offset), name)?;
    }
    Ok(total)
}

fn discard_exact<R: Read>(reader: &mut R, mut len: usize) -> Result<()> {
    let mut buf = [0u8; 16 * 1024];
    while len != 0 {
        let n = len.min(buf.len());
        reader.read_exact(&mut buf[..n])?;
        len -= n;
    }
    Ok(())
}

fn materialize_low_cardinality_inner(
    dict_data: &[u8], inner: &ColumnType, indexes: &[u8], idx_width: usize, num_idx: usize,
) -> Result<Vec<u8>> {
    use ColumnType::*;
    match inner {
        UInt8 | Int8 | Bool | Enum8 => {
            materialize_lc_fixed(dict_data, 1, indexes, idx_width, num_idx)
        },
        UInt16 | Int16 | Date | Enum16 => {
            materialize_lc_fixed(dict_data, 2, indexes, idx_width, num_idx)
        },
        UInt32 | Int32 | Float32 | Date32 | DateTime | Time | IPv4 => {
            materialize_lc_fixed(dict_data, 4, indexes, idx_width, num_idx)
        },
        UInt64 | Int64 | Float64 | DateTime64(_) | Time64(_) => {
            materialize_lc_fixed(dict_data, 8, indexes, idx_width, num_idx)
        },
        UInt128 | Int128 | UUID | IPv6 => {
            materialize_lc_fixed(dict_data, 16, indexes, idx_width, num_idx)
        },
        UInt256 | Int256 => materialize_lc_fixed(dict_data, 32, indexes, idx_width, num_idx),
        FixedString(n) => materialize_lc_fixed(dict_data, *n, indexes, idx_width, num_idx),
        String | JSON | Other(_) => materialize_lc_string(dict_data, indexes, idx_width, num_idx),
        Nullable(inner) => materialize_lc_nullable(dict_data, inner, indexes, idx_width, num_idx),
        _ => Err(crate::sync::error::Error::Protocol(format!(
            "LowCardinality({inner}) materialization is not supported"
        ))),
    }
}

fn materialize_lc_fixed(
    dict: &[u8], width: usize, indexes: &[u8], idx_width: usize, num_idx: usize,
) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(checked_column_len(
        num_idx,
        width,
        "LowCardinality materialized column",
    )?);
    for row in 0..num_idx {
        let idx = read_lc_index(indexes, row, idx_width)?;
        let start = idx.checked_mul(width).ok_or_else(|| {
            crate::sync::error::Error::Protocol("LowCardinality index overflow".into())
        })?;
        let end = start.checked_add(width).ok_or_else(|| {
            crate::sync::error::Error::Protocol("LowCardinality index overflow".into())
        })?;
        if end > dict.len() {
            return Err(crate::sync::error::Error::Protocol(
                "LowCardinality dictionary index out of bounds".into(),
            ));
        }
        out.extend_from_slice(&dict[start..end]);
    }
    Ok(out)
}

fn materialize_lc_string(
    dict: &[u8], indexes: &[u8], idx_width: usize, num_idx: usize,
) -> Result<Vec<u8>> {
    let mut entries = Vec::new();
    let mut pos = 0usize;
    while pos < dict.len() {
        let start = pos;
        let len = checked_usize(parse_varint(dict, &mut pos)?, "LowCardinality string")?;
        let end = pos.checked_add(len).ok_or_else(|| {
            crate::sync::error::Error::Protocol("LowCardinality string overflow".into())
        })?;
        if end > dict.len() {
            return Err(crate::sync::error::Error::Protocol(
                "LowCardinality string dictionary is truncated".into(),
            ));
        }
        entries.push(start..end);
        pos = end;
    }

    let mut out = Vec::new();
    let mut total = 0usize;
    for row in 0..num_idx {
        let idx = read_lc_index(indexes, row, idx_width)?;
        let range = entries.get(idx).ok_or_else(|| {
            crate::sync::error::Error::Protocol(
                "LowCardinality dictionary index out of bounds".into(),
            )
        })?;
        // Dictionary expansion repeats wire entries per row; cap the
        // materialized bytes so a tiny dictionary cannot amplify to a huge
        // output (e.g. 10M rows x a 16 MiB entry).
        total = checked_column_bytes(total, range.len(), "LowCardinality materialized column")?;
        out.extend_from_slice(&dict[range.clone()]);
    }
    Ok(out)
}

fn materialize_lc_nullable(
    dict: &[u8], inner: &ColumnType, indexes: &[u8], idx_width: usize, num_idx: usize,
) -> Result<Vec<u8>> {
    let mut pos = 0usize;
    let dict_rows = infer_lc_dictionary_rows(dict, inner)?;
    let nulls = parse_bytes(dict, &mut pos, dict_rows)?;
    let values_start = pos;
    let value_dict = &dict[values_start..];
    let values = materialize_low_cardinality_inner(value_dict, inner, indexes, idx_width, num_idx)?;
    let mut out = Vec::with_capacity(checked_column_bytes(
        num_idx,
        values.len(),
        "LowCardinality materialized column",
    )?);
    for row in 0..num_idx {
        let idx = read_lc_index(indexes, row, idx_width)?;
        out.push(*nulls.get(idx).ok_or_else(|| {
            crate::sync::error::Error::Protocol(
                "LowCardinality nullable index out of bounds".into(),
            )
        })?);
    }
    out.extend_from_slice(&values);
    Ok(out)
}

fn infer_lc_dictionary_rows(dict: &[u8], inner: &ColumnType) -> Result<usize> {
    if let Some(width) = inner.fixed_width() {
        return Ok(dict.len() / (width + 1));
    }
    Err(crate::sync::error::Error::Protocol(
        "LowCardinality Nullable variable-width materialization is not supported".into(),
    ))
}

fn read_lc_index(data: &[u8], row: usize, width: usize) -> Result<usize> {
    let start = row.checked_mul(width).ok_or_else(|| {
        crate::sync::error::Error::Protocol("LowCardinality index overflow".into())
    })?;
    let end = start.checked_add(width).ok_or_else(|| {
        crate::sync::error::Error::Protocol("LowCardinality index overflow".into())
    })?;
    if end > data.len() {
        return Err(crate::sync::error::Error::Protocol(
            "LowCardinality index data is truncated".into(),
        ));
    }
    let value = match width {
        1 => u64::from(data[start]),
        2 => u64::from(u16::from_le_bytes([data[start], data[start + 1]])),
        4 => u64::from(u32::from_le_bytes([
            data[start],
            data[start + 1],
            data[start + 2],
            data[start + 3],
        ])),
        8 => u64::from_le_bytes(data[start..end].try_into().map_err(|_| {
            crate::sync::error::Error::Protocol("LowCardinality index length mismatch".into())
        })?),
        _ => {
            return Err(crate::sync::error::Error::Protocol(format!(
                "unsupported LowCardinality index width {width}"
            )));
        },
    };
    checked_usize(value, "LowCardinality index")
}

/// Validate a server-controlled per-value string length against the shared
/// 16 MiB wire limit before it sizes any buffer.
fn checked_string_len(value: u64, what: &str) -> Result<usize> {
    crate::limits::checked_string_len(value, what).map_err(crate::sync::error::Error::Protocol)
}

/// Validate a column's running byte total (checked add + 64 MiB cap) before a
/// value-driven reserve/resize.
fn checked_column_bytes(acc: usize, add: usize, what: &str) -> Result<usize> {
    crate::limits::checked_column_bytes(acc, add, what).map_err(crate::sync::error::Error::Protocol)
}

/// Validate a fixed-width/offset/index buffer byte length (`rows * width`,
/// checked multiply + 64 MiB cap) before any allocation sized from it.
fn checked_column_len(rows: usize, width: usize, what: &str) -> Result<usize> {
    crate::limits::checked_column_len(rows, width, what)
        .map_err(crate::sync::error::Error::Protocol)
}

/// Validate one Array/Map offset: non-decreasing (cumulative prefix sums) and
/// capped at MAX_BLOCK_ROWS inner elements.
fn checked_monotonic_offset(prev: usize, value: u64, what: &str) -> Result<usize> {
    crate::limits::checked_monotonic_offset(prev, value, what)
        .map_err(crate::sync::error::Error::Protocol)
}

/// Charge `len` claimed bytes against a column's remaining byte budget so
/// lying lengths fail before the matching reserve/resize/read.
fn charge_budget(budget: &mut usize, len: usize, what: &str) -> Result<()> {
    let remaining = budget.checked_sub(len).ok_or_else(|| {
        crate::sync::error::Error::Protocol(format!(
            "{what} cumulative byte length exceeds limit {}",
            crate::limits::MAX_COLUMN_BYTES
        ))
    })?;
    *budget = remaining;
    Ok(())
}

fn checked_usize(value: u64, name: &str) -> Result<usize> {
    usize::try_from(value)
        .map_err(|_| crate::sync::error::Error::Protocol(format!("{name} count too large")))
}

/// Validates a server-controlled item count against a [`crate::limits`] cap
/// before any allocation or loop is sized from it.
fn checked_count(value: u64, what: &str, max: usize) -> Result<usize> {
    crate::limits::checked_count(value, what, max).map_err(crate::sync::error::Error::Protocol)
}

/// Validate a LowCardinality header and derive the per-row index width.
///
/// `version` must be 1; `serial_type` low 2 bits are the index width shift,
/// bit 8 the unsupported "global dictionaries" flag, bit 9 the required
/// "additional keys" flag.
fn lc_idx_width(version: u64, serial_type: u64) -> Result<usize> {
    if version != 1 {
        return Err(crate::sync::error::Error::Protocol(format!(
            "unsupported LowCardinality key serialization version {version}"
        )));
    }
    if (serial_type & (1u64 << 8)) != 0 {
        return Err(crate::sync::error::Error::Protocol(
            "LowCardinality global dictionaries are not supported".into(),
        ));
    }
    if (serial_type & (1u64 << 9)) == 0 {
        return Err(crate::sync::error::Error::Protocol(
            "LowCardinality additional keys flag is missing".into(),
        ));
    }
    Ok(1usize << (serial_type & 0x3))
}

fn read_varint_into<R: Read>(
    reader: &mut R, data: &mut Vec<u8>, budget: &mut usize,
) -> Result<u64> {
    let mut r = 0u64;
    let mut shift = 0;
    loop {
        if shift >= 64 {
            return Err(crate::sync::error::Error::Protocol(
                "varint overflow".into(),
            ));
        }
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        charge_budget(budget, 1, "column")?;
        data.push(byte[0]);
        if shift == 63 && (byte[0] & 0x7F) > 1 {
            return Err(crate::sync::error::Error::Protocol(
                "varint overflow".into(),
            ));
        }
        r |= ((byte[0] & 0x7F) as u64) << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(r);
        }
        shift += 7;
    }
}

/// Skip BlockInfo (varint loop, 0 = end).
fn skip_block_info(buf: &[u8], pos: &mut usize) -> Result<()> {
    loop {
        let d = parse_varint(buf, pos)?;
        match d {
            0 => return Ok(()),
            1 => {
                advance(buf, pos, 1)?;
            },
            2 => {
                advance(buf, pos, 4)?;
            },
            3 => {
                parse_varint(buf, pos)?;
            },
            _ => return Ok(()), // unknown, treat as end
        }
    }
}

/// Skip a single column's data in the buffer.
///
/// Delegates to the shared slice-based skip implementation so the buffered
/// parser frames columns exactly like the streaming raw readers: Array/Map
/// offsets are fixed-width little-endian u64s (never varints) whose last
/// value is the inner row count, materialized JSON carries an 8-byte
/// string-serialization version, LowCardinality carries its header /
/// dictionary / index layout, and Variant/Dynamic carry their per-subcolumn
/// state prefixes.
fn skip_column_data(buf: &[u8], pos: &mut usize, ct: &ColumnType, rows: usize) -> Result<()> {
    crate::sync::protocol::skip_column::skip_column_data(buf, pos, ct, rows)
}

fn advance(buf: &[u8], pos: &mut usize, len: usize) -> Result<()> {
    let end = (*pos)
        .checked_add(len)
        .ok_or_else(|| crate::sync::error::Error::Protocol("buffer position overflow".into()))?;
    if end > buf.len() {
        return Err(crate::sync::error::Error::Protocol(
            "unexpected end of buffer skipping column data".into(),
        ));
    }
    *pos = end;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::column::ClickHouseColumnData;
    use crate::sync::protocol::wire;

    fn empty_data_packet(buf: &mut Vec<u8>) {
        wire::write_varint(buf, 1).expect("test operation failed"); // Data
        wire::write_string(buf, "").expect("test operation failed"); // table
        wire::write_varint(buf, 0).expect("test operation failed"); // BlockInfo terminator
        wire::write_varint(buf, 0).expect("test operation failed"); // columns
        wire::write_varint(buf, 0).expect("test operation failed"); // rows
    }

    // ═══════════════════════════════════════════════
    // Buffered block framing (skip_column_data via parse_block_body)
    // ═══════════════════════════════════════════════

    /// Build a block body: BlockInfo terminator, column and row counts, then
    /// per column name/type/custom-serialization-byte/data.
    fn block_buf(rows: u64, cols: &[(&str, &str, Vec<u8>)]) -> Vec<u8> {
        let mut buf = Vec::new();
        wire::write_varint(&mut buf, 0).expect("test write"); // BlockInfo end
        wire::write_varint(&mut buf, cols.len() as u64).expect("test write");
        wire::write_varint(&mut buf, rows).expect("test write");
        for (name, type_name, data) in cols {
            wire::write_string(&mut buf, name).expect("test write");
            wire::write_string(&mut buf, type_name).expect("test write");
            buf.push(0); // custom serialization
            buf.extend_from_slice(data);
        }
        buf
    }

    /// Array/Map offsets: little-endian u64 per outer row.
    fn offsets(values: &[u64]) -> Vec<u8> {
        let mut buf = Vec::new();
        for v in values {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf
    }

    /// One varint-length-prefixed string value.
    fn string_value(s: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        wire::write_string(&mut buf, s).expect("test write");
        buf
    }

    /// The inner column of an Array column carries exactly the last-offset
    /// rows. When every array is empty the last offset is 0, so the inner
    /// column carries zero rows and zero bytes — using `rows - 1` here would
    /// misframe every later column of the block.
    #[test]
    fn buffered_array_all_empty_offsets_zero_skips_no_inner() {
        let buf = block_buf(
            2,
            &[
                ("a", "Array(UInt8)", offsets(&[0, 0])),
                ("x", "UInt64", offsets(&[7, 8])),
            ],
        );
        let mut pos = 0;
        let block = parse_block_body(&buf, &mut pos).expect("parse block body");
        assert_eq!(pos, buf.len());

        let col = block.column::<Vec<u8>>("a").expect("read array column");
        assert_eq!(col.get(0).expect("row 0"), Vec::<u8>::new());
        assert_eq!(col.get(1).expect("row 1"), Vec::<u8>::new());
        let trailing = block.column::<u64>("x").expect("read trailing column");
        assert_eq!(trailing.get(0).expect("row 0"), 7);
        assert_eq!(trailing.get(1).expect("row 1"), 8);
    }

    /// Normal arrays with embedded empty rows: offsets advance without inner
    /// bytes for the empty row and the trailing column still parses.
    #[test]
    fn buffered_array_uint8_mixed_empty_rows_frame_trailing_column() {
        let data = offsets(&[2, 2, 3]); // row0 [1,2], row1 [], row2 [9]
        let buf = block_buf(
            3,
            &[
                ("a", "Array(UInt8)", [data, vec![1, 2, 9]].concat()),
                ("x", "UInt8", vec![7, 8, 9]),
            ],
        );
        let mut pos = 0;
        let block = parse_block_body(&buf, &mut pos).expect("parse block body");
        assert_eq!(pos, buf.len());

        let col = block.column::<Vec<u8>>("a").expect("read array column");
        assert_eq!(col.get(0).expect("row 0"), vec![1, 2]);
        assert_eq!(col.get(1).expect("row 1"), Vec::<u8>::new());
        assert_eq!(col.get(2).expect("row 2"), vec![9]);
        let trailing = block.column::<u8>("x").expect("read trailing column");
        assert_eq!(trailing.get(2).expect("row 2"), 9);
    }

    /// Map is Array(Tuple(K, V)): offsets first, then the key and value
    /// columns with the last-offset row count each.
    #[test]
    fn buffered_map_offsets_then_keys_and_values() {
        let body = [
            offsets(&[1]),     // one map entry
            string_value("k"), // key column: 1 row
            vec![7u8],         // value column: 1 row
        ]
        .concat();
        let buf = block_buf(
            1,
            &[
                ("m", "Map(String, UInt8)", body),
                ("s", "String", string_value("tail")),
            ],
        );
        let mut pos = 0;
        let block = parse_block_body(&buf, &mut pos).expect("parse block body");
        assert_eq!(pos, buf.len());

        let map = block
            .column::<Vec<(String, u8)>>("m")
            .expect("read map column");
        assert_eq!(map.get(0).expect("row 0"), vec![("k".to_string(), 7u8)]);
        assert_eq!(
            block
                .column::<String>("s")
                .expect("trailing column")
                .get(0)
                .expect("row 0"),
            "tail"
        );
    }

    /// The buffered JSON column data excludes the 8-byte string-serialization
    /// version, matching the streaming materialized reader.
    #[test]
    fn buffered_json_version_prefix_consumed_and_stripped() {
        let body = [
            1u64.to_le_bytes().to_vec(), // string serialization version 1
            string_value(r#"{"x":1}"#),
            string_value(r#"{"y":2}"#),
        ]
        .concat();
        let buf = block_buf(2, &[("j", "JSON", body), ("x", "UInt8", vec![5, 6])]);
        let mut pos = 0;
        let block = parse_block_body(&buf, &mut pos).expect("parse block body");
        assert_eq!(pos, buf.len());

        let expected_data = [string_value(r#"{"x":1}"#), string_value(r#"{"y":2}"#)].concat();
        let json_info = block
            .columns
            .iter()
            .find(|c| c.name == "j")
            .expect("json col");
        assert_eq!(&json_info.data[..], &expected_data[..]);
        let trailing = block.column::<u8>("x").expect("read trailing column");
        assert_eq!(trailing.get(1).expect("row 1"), 6);
    }

    /// Nested JSON keeps its string-serialization version inside the sliced
    /// data, which decoders misread: the buffered parser must reject it
    /// loudly (top-level JSON above stays allowed and version-stripped).
    #[test]
    fn buffered_nested_json_is_rejected_before_slicing() {
        let body = [
            offsets(&[1]),               // one element per outer row
            1u64.to_le_bytes().to_vec(), // inner JSON version (must NOT be sliced)
            string_value(r#"{"a":1}"#),
        ]
        .concat();
        let buf = block_buf(1, &[("j", "Array(JSON)", body), ("x", "UInt8", vec![7])]);
        let mut pos = 0;
        let err = parse_block_body(&buf, &mut pos)
            .err()
            .expect("nested JSON must be rejected (Block lacks Debug; expect_err needs it)");
        assert!(
            err.to_string().contains("nested JSON"),
            "expected nested JSON rejection, got: {err}"
        );
    }

    /// Zero-row blocks (the header block of every SELECT) carry no column
    /// bytes at all — including LowCardinality headers.
    #[test]
    fn buffered_zero_row_block_with_lowcardinality_column_parses() {
        let buf = block_buf(0, &[("lc", "LowCardinality(String)", Vec::new())]);
        let mut pos = 0;
        let block = parse_block_body(&buf, &mut pos).expect("parse block body");
        assert_eq!(pos, buf.len());
        assert_eq!(block.row_count(), 0);
        assert!(block.columns[0].data.is_empty());
    }

    /// Variant mode 0: per-row discriminators plus non-empty subcolumns in
    /// type order; the trailing column must parse.
    #[test]
    fn buffered_variant_subcolumns_frame_trailing_column() {
        let body = [
            0u64.to_le_bytes().to_vec(), // BASIC mode
            vec![0, 1],                  // row 0 -> UInt8, row 1 -> String
            vec![5u8],                   // UInt8 subcolumn: 1 value
            string_value("x"),           // String subcolumn: 1 value
        ]
        .concat();
        let buf = block_buf(
            2,
            &[
                ("v", "Variant(UInt8, String)", body),
                ("t", "UInt8", vec![3, 4]),
            ],
        );
        let mut pos = 0;
        let block = parse_block_body(&buf, &mut pos).expect("parse block body");
        assert_eq!(pos, buf.len());
        let trailing = block.column::<u8>("t").expect("read trailing column");
        assert_eq!(trailing.get(0).expect("row 0"), 3);
        assert_eq!(trailing.get(1).expect("row 1"), 4);
    }

    /// Dynamic flattened (version 2): state prefix with subcolumn type names,
    /// fixed-width discriminators (type count marks NULL), counted
    /// subcolumns; the trailing column must parse.
    #[test]
    fn buffered_dynamic_flattened_frames_trailing_column() {
        let mut state = Vec::new();
        state.extend_from_slice(&2u64.to_le_bytes()); // subcolumn serialization version
        wire::write_varint(&mut state, 1).expect("test write"); // one subcolumn type
        wire::write_string(&mut state, "UInt8").expect("test write");
        let body = [
            state,
            vec![0, 1], // row 0 -> UInt8, row 1 -> NULL (idx == type count)
            vec![9u8],  // UInt8 subcolumn: 1 value
        ]
        .concat();
        let buf = block_buf(2, &[("d", "Dynamic", body), ("t", "UInt8", vec![6, 7])]);
        let mut pos = 0;
        let block = parse_block_body(&buf, &mut pos).expect("parse block body");
        assert_eq!(pos, buf.len());
        let trailing = block.column::<u8>("t").expect("read trailing column");
        assert_eq!(trailing.get(1).expect("row 1"), 7);
    }

    #[test]
    fn read_varint_into_rejects_overlong_and_tenth_byte_overflow() {
        let mut recorded = Vec::new();
        let mut budget = crate::limits::MAX_COLUMN_BYTES;
        let overflow = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02];
        assert!(read_varint_into(&mut &overflow[..], &mut recorded, &mut budget).is_err());

        let mut recorded = Vec::new();
        let mut budget = crate::limits::MAX_COLUMN_BYTES;
        assert!(read_varint_into(&mut &[0x80u8; 11][..], &mut recorded, &mut budget).is_err());
    }

    #[test]
    fn parses_progress_before_data_without_desync() {
        let mut buf = Vec::new();
        wire::write_varint(&mut buf, 3).expect("test operation failed"); // Progress
        for _ in 0..7 {
            wire::write_varint(&mut buf, 0).expect("test operation failed");
        }
        empty_data_packet(&mut buf);
        wire::write_varint(&mut buf, 5).expect("test operation failed"); // EndOfStream

        let blocks = parse_response(buf, revision::DEFAULT_PROTOCOL_REVISION)
            .expect("test operation failed");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].row_count(), 0);
    }

    #[test]
    fn skips_profile_info_and_profile_events_without_desync() {
        let mut buf = Vec::new();
        wire::write_varint(&mut buf, 6).expect("test operation failed"); // ProfileInfo
        wire::write_varint(&mut buf, 0).expect("test operation failed"); // rows
        wire::write_varint(&mut buf, 0).expect("test operation failed"); // blocks
        wire::write_varint(&mut buf, 0).expect("test operation failed"); // bytes
        buf.push(0); // applied_limit
        wire::write_varint(&mut buf, 0).expect("test operation failed"); // rows_before_limit
        buf.push(0); // calculated_rows_before_limit
        buf.push(0); // applied_aggregation
        wire::write_varint(&mut buf, 0).expect("test operation failed"); // rows_before_aggregation

        wire::write_varint(&mut buf, 14).expect("test operation failed"); // ProfileEvents
        wire::write_string(&mut buf, "").expect("test operation failed"); // tag
        wire::write_varint(&mut buf, 0).expect("test operation failed"); // BlockInfo terminator
        wire::write_varint(&mut buf, 0).expect("test operation failed"); // columns
        wire::write_varint(&mut buf, 0).expect("test operation failed"); // rows

        empty_data_packet(&mut buf);
        wire::write_varint(&mut buf, 5).expect("test operation failed"); // EndOfStream

        let blocks = parse_response(buf, revision::DEFAULT_PROTOCOL_REVISION)
            .expect("test operation failed");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].row_count(), 0);
    }

    #[test]
    fn parses_progress_for_revision_54464_without_desync() {
        let mut buf = Vec::new();
        wire::write_varint(&mut buf, 3).expect("test operation failed"); // Progress
        for _ in 0..7 {
            wire::write_varint(&mut buf, 0).expect("test operation failed");
        }
        empty_data_packet(&mut buf);
        wire::write_varint(&mut buf, 5).expect("test operation failed"); // EndOfStream

        let blocks = parse_response_with_revision(buf, 54464).expect("test operation failed");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].row_count(), 0);
    }

    #[test]
    fn skips_part_uuids_and_timezone_update_without_desync() {
        let mut buf = Vec::new();
        wire::write_varint(&mut buf, 12).expect("test operation failed"); // PartUUIDs
        wire::write_varint(&mut buf, 2).expect("test operation failed");
        buf.extend_from_slice(&[1u8; 16]);
        buf.extend_from_slice(&[2u8; 16]);
        wire::write_varint(&mut buf, 17).expect("test operation failed"); // TimezoneUpdate
        wire::write_string(&mut buf, "UTC").expect("test operation failed");
        empty_data_packet(&mut buf);
        wire::write_varint(&mut buf, 5).expect("test operation failed"); // EndOfStream

        let blocks = parse_response(buf, revision::DEFAULT_PROTOCOL_REVISION)
            .expect("test operation failed");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].row_count(), 0);
    }

    #[test]
    fn materializes_low_cardinality_string_like_clickhouse_cpp() {
        let mut buf = Vec::new();
        wire::write_varint(&mut buf, 1).expect("test operation failed"); // Data
        wire::write_string(&mut buf, "").expect("test operation failed"); // table
        wire::write_varint(&mut buf, 0).expect("test operation failed"); // BlockInfo end
        wire::write_varint(&mut buf, 1).expect("test operation failed"); // columns
        wire::write_varint(&mut buf, 3).expect("test operation failed"); // rows
        wire::write_string(&mut buf, "s").expect("test operation failed");
        wire::write_string(&mut buf, "LowCardinality(String)").expect("test operation failed");
        buf.push(0); // custom serialization flag
        buf.extend_from_slice(&1u64.to_le_bytes()); // key serialization version
        buf.extend_from_slice(&(1u64 << 9).to_le_bytes()); // UInt8 indexes + additional keys
        buf.extend_from_slice(&2u64.to_le_bytes()); // dictionary keys
        wire::write_string(&mut buf, "a").expect("test operation failed");
        wire::write_string(&mut buf, "b").expect("test operation failed");
        buf.extend_from_slice(&3u64.to_le_bytes()); // index rows
        buf.extend_from_slice(&[0, 1, 0]);
        wire::write_varint(&mut buf, 5).expect("test operation failed"); // EndOfStream

        let blocks = parse_response_with_revision(buf, revision::DEFAULT_PROTOCOL_REVISION)
            .expect("response should parse");
        let col = blocks[0]
            .column::<String>("s")
            .expect("LowCardinality(String) should materialize");
        assert_eq!(col.get(0).expect("row 0 should decode"), "a");
        assert_eq!(col.get(1).expect("row 1 should decode"), "b");
        assert_eq!(col.get(2).expect("row 2 should decode"), "a");
    }
}

#[cfg(test)]
mod count_limit_tests {
    use super::{
        discard_block_body, parse_block_body, read_block, read_block_view,
        read_dynamic_state_prefix_into, read_json_state_prefix_into,
    };
    use crate::limits;
    use crate::sync::error::Error;
    use crate::sync::protocol::wire;
    use std::io::Cursor;

    fn varint(v: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        wire::write_varint(&mut buf, v).expect("test write");
        buf
    }

    /// BlockInfo terminator + column/row counts.
    fn block_body_bytes(cols: u64, rows: u64) -> Vec<u8> {
        let mut bytes = vec![0x00];
        bytes.extend_from_slice(&varint(cols));
        bytes.extend_from_slice(&varint(rows));
        bytes
    }

    #[test]
    fn parse_block_body_column_count_u64_max_is_protocol_error() {
        let bytes = block_body_bytes(u64::MAX, 0);
        let mut pos = 0;
        let err = parse_block_body(&bytes, &mut pos)
            .err()
            .expect("u64::MAX column count must be rejected");
        match &err {
            Error::Protocol(msg) => assert_eq!(
                msg,
                "block column count 18446744073709551615 exceeds limit 65536"
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn parse_block_body_row_count_cap_plus_one_is_protocol_error() {
        let bytes = block_body_bytes(0, limits::MAX_BLOCK_ROWS as u64 + 1);
        let mut pos = 0;
        let err = parse_block_body(&bytes, &mut pos)
            .err()
            .expect("cap + 1 row count must be rejected");
        match &err {
            Error::Protocol(msg) => {
                assert_eq!(msg, "block row count 10000001 exceeds limit 10000000")
            },
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn parse_block_body_row_cap_boundary_parses() {
        // Exactly MAX_BLOCK_ROWS with zero columns must still parse: the cap
        // bounds a single block, never the total rows of a streamed response.
        let bytes = block_body_bytes(0, limits::MAX_BLOCK_ROWS as u64);
        let mut pos = 0;
        let block = parse_block_body(&bytes, &mut pos).expect("row count at the cap parses");
        assert_eq!(block.row_count(), limits::MAX_BLOCK_ROWS);
        assert!(block.columns.is_empty());
    }

    #[test]
    fn read_block_column_count_u64_max_is_protocol_error() {
        let mut bytes = varint(0); // table name
        bytes.extend_from_slice(&block_body_bytes(u64::MAX, 0));
        let mut reader = Cursor::new(bytes);
        let err = read_block(&mut reader)
            .err()
            .expect("u64::MAX column count must be rejected");
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn read_block_view_row_count_cap_plus_one_is_protocol_error() {
        let mut bytes = varint(0); // table name
        bytes.extend_from_slice(&block_body_bytes(0, limits::MAX_BLOCK_ROWS as u64 + 1));
        let mut reader = Cursor::new(bytes);
        let mut visitor =
            |_view: crate::sync::protocol::block::BlockView<'_>| -> crate::sync::error::Result<()> {
                Ok(())
            };
        let err = read_block_view(&mut reader, &mut visitor)
            .expect_err("cap + 1 row count must be rejected");
        match &err {
            Error::Protocol(msg) => {
                assert_eq!(msg, "block row count 10000001 exceeds limit 10000000")
            },
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn discard_block_body_column_count_u64_max_is_protocol_error() {
        let bytes = block_body_bytes(u64::MAX, 0);
        let mut reader = Cursor::new(bytes);
        let err =
            discard_block_body(&mut reader).expect_err("u64::MAX column count must be rejected");
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn json_state_v3_path_count_u64_max_is_protocol_error() {
        let mut bytes = 3u64.to_le_bytes().to_vec(); // serialization version 3
        bytes.extend_from_slice(&varint(u64::MAX)); // JSON path count
        let mut reader = Cursor::new(bytes);
        let mut data = Vec::new();
        let mut budget = crate::limits::MAX_COLUMN_BYTES;
        let err = read_json_state_prefix_into(&mut reader, &mut data, &mut budget)
            .expect_err("u64::MAX JSON path count must be rejected");
        match &err {
            Error::Protocol(msg) => assert_eq!(
                msg,
                "JSON path count 18446744073709551615 exceeds limit 65536"
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn json_state_v0_path_count_cap_plus_one_is_protocol_error() {
        let mut bytes = 0u64.to_le_bytes().to_vec(); // serialization version 0
        bytes.extend_from_slice(&varint(0)); // max dynamic paths hint
        bytes.extend_from_slice(&varint(65_537)); // JSON path count
        let mut reader = Cursor::new(bytes);
        let mut data = Vec::new();
        let mut budget = crate::limits::MAX_COLUMN_BYTES;
        let err = read_json_state_prefix_into(&mut reader, &mut data, &mut budget)
            .expect_err("cap + 1 JSON path count must be rejected");
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn dynamic_state_v1_type_count_u64_max_is_protocol_error() {
        // Version 1 prefix: version, max-types hint, then the type count that
        // previously fed Vec::with_capacity directly (capacity-overflow panic).
        let mut bytes = 1u64.to_le_bytes().to_vec(); // serialization version 1
        bytes.extend_from_slice(&varint(0)); // max types hint
        bytes.extend_from_slice(&varint(u64::MAX)); // dynamic type count
        let mut reader = Cursor::new(bytes);
        let mut data = Vec::new();
        let mut budget = crate::limits::MAX_COLUMN_BYTES;
        let err = read_dynamic_state_prefix_into(&mut reader, &mut data, &mut budget)
            .expect_err("u64::MAX dynamic type count must be rejected");
        match &err {
            Error::Protocol(msg) => assert_eq!(
                msg,
                "dynamic subcolumn types count 18446744073709551615 exceeds limit 65536"
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn low_cardinality_key_count_u64_max_is_protocol_error() {
        // 24-byte LowCardinality prefix: version 1, serial type 0, then the
        // dictionary key count that previously sized `Vec` growth unbounded.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes()); // key serialization version
        // serial type: "additional keys" flag (bit 9) required by the reader
        bytes.extend_from_slice(&(1u64 << 9).to_le_bytes());
        bytes.extend_from_slice(&u64::MAX.to_le_bytes()); // dictionary key count
        let mut pos = 0;
        let err = super::read_low_cardinality_from_buffer(
            &bytes,
            &mut pos,
            &crate::sync::protocol::type_parser::ColumnType::UInt8,
            1,
        )
        .expect_err("u64::MAX LowCardinality key count must be rejected");
        match &err {
            Error::Protocol(msg) => assert_eq!(
                msg,
                "LowCardinality key count 18446744073709551615 exceeds limit 65536"
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn read_block_within_caps_still_parses() {
        // One UInt8 column, one row: counts well below every cap must keep
        // parsing normally after the cap checks are in place.
        let mut bytes = varint(0); // table name
        bytes.push(0x00); // BlockInfo terminator
        bytes.extend_from_slice(&varint(1)); // columns
        bytes.extend_from_slice(&varint(1)); // rows
        wire::write_string(&mut bytes, "c").expect("test write"); // column name
        wire::write_string(&mut bytes, "UInt8").expect("test write"); // type
        bytes.push(0); // custom serialization
        bytes.push(7); // one UInt8 value
        let mut reader = Cursor::new(bytes);
        let block = read_block(&mut reader).expect("block within caps parses");
        assert_eq!(block.row_count(), 1);
        assert_eq!(block.columns.len(), 1);
        assert_eq!(&block.columns[0].data[..], &[7]);
    }
}

#[cfg(test)]
mod byte_limit_tests {
    use super::read_lc_body_raw_into;
    use super::{
        RawColumnState, discard_block_body, parse_block_body, read_block, read_block_body,
        read_block_view, read_variant_types_body_into,
    };
    use crate::limits::MAX_COLUMN_BYTES;
    use crate::sync::error::Error;
    use crate::sync::protocol::type_parser::ColumnType;
    use crate::sync::protocol::wire;
    use std::io::Cursor;

    fn varint(v: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        wire::write_varint(&mut buf, v).expect("test write");
        buf
    }

    fn column_header(name: &str, type_name: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        wire::write_string(&mut buf, name).expect("test write");
        wire::write_string(&mut buf, type_name).expect("test write");
        buf.push(0); // custom serialization
        buf
    }

    /// table name + BlockInfo terminator + 1 column / 1 row header.
    fn one_column_block(type_name: &str) -> Vec<u8> {
        block_with_rows(type_name, 1)
    }

    fn block_with_rows(type_name: &str, rows: u64) -> Vec<u8> {
        let mut buf = varint(0); // table name
        buf.push(0x00); // BlockInfo terminator
        buf.extend_from_slice(&varint(1)); // columns
        buf.extend_from_slice(&varint(rows)); // rows
        buf.extend_from_slice(&column_header("c", type_name));
        buf
    }

    fn assert_protocol(err: &Error, needle: &str) {
        match err {
            Error::Protocol(msg) => assert!(
                msg.contains(needle),
                "expected {needle:?} in protocol error, got: {msg}"
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn streamed_string_value_length_u64_max_is_rejected_before_read() {
        let mut bytes = one_column_block("String");
        bytes.extend_from_slice(&varint(u64::MAX)); // lying length, no payload
        let mut reader = Cursor::new(bytes);
        let err = read_block(&mut reader)
            .err()
            .expect("lying string length must be rejected");
        assert_protocol(
            &err,
            "string value length 18446744073709551615 exceeds limit 16777215",
        );
    }

    #[test]
    fn view_path_string_value_length_2_pow_40_is_rejected() {
        let mut bytes = one_column_block("String");
        bytes.extend_from_slice(&varint(1u64 << 40));
        let mut reader = Cursor::new(bytes);
        let mut visitor =
            |_view: crate::sync::protocol::block::BlockView<'_>| -> crate::sync::error::Result<()> {
                Ok(())
            };
        let err = read_block_view(&mut reader, &mut visitor)
            .expect_err("2^40 string claim must be rejected on the view path");
        assert_protocol(
            &err,
            "string value length 1099511627776 exceeds limit 16777215",
        );
    }

    #[test]
    fn from_buffer_string_value_length_u64_max_is_rejected() {
        let mut bytes = vec![0x00]; // BlockInfo terminator
        bytes.extend_from_slice(&varint(1)); // columns
        bytes.extend_from_slice(&varint(1)); // rows
        bytes.extend_from_slice(&column_header("c", "String"));
        bytes.extend_from_slice(&varint(u64::MAX));
        let mut pos = 0;
        let err = parse_block_body(&bytes, &mut pos)
            .err()
            .expect("from-buffer lying string length must be rejected");
        assert_protocol(&err, "string value length 18446744073709551615");
    }

    #[test]
    fn discard_string_value_length_u64_max_is_rejected() {
        let mut bytes = vec![0x00]; // BlockInfo terminator
        bytes.extend_from_slice(&varint(1)); // columns
        bytes.extend_from_slice(&varint(1)); // rows
        bytes.extend_from_slice(&column_header("c", "String"));
        bytes.extend_from_slice(&varint(u64::MAX));
        let mut reader = Cursor::new(bytes);
        let err = discard_block_body(&mut reader)
            .expect_err("discard-path lying string length must be rejected");
        assert_protocol(&err, "string value length 18446744073709551615");
    }

    #[test]
    fn streamed_json_string_length_u64_max_is_rejected() {
        let mut bytes = one_column_block("JSON");
        bytes.extend_from_slice(&1u64.to_le_bytes()); // serialization version
        bytes.extend_from_slice(&varint(u64::MAX));
        let mut reader = Cursor::new(bytes);
        let err = read_block(&mut reader)
            .err()
            .expect("lying JSON length must be rejected");
        assert_protocol(&err, "JSON string length 18446744073709551615");
    }

    #[test]
    fn streamed_array_offset_2_pow_60_is_rejected_before_inner_read() {
        let mut bytes = one_column_block("Array(UInt8)");
        bytes.extend_from_slice(&(1u64 << 60).to_le_bytes()); // single offset
        let mut reader = Cursor::new(bytes);
        let err = read_block(&mut reader)
            .err()
            .expect("2^60 array offset must be rejected before the inner read");
        assert_protocol(
            &err,
            "array offset total 1152921504606846976 exceeds limit 10000000",
        );
    }

    #[test]
    fn from_buffer_array_offset_2_pow_60_is_rejected() {
        let mut bytes = vec![0x00];
        bytes.extend_from_slice(&varint(1));
        bytes.extend_from_slice(&varint(1));
        bytes.extend_from_slice(&column_header("c", "Array(UInt8)"));
        bytes.extend_from_slice(&(1u64 << 60).to_le_bytes());
        let mut pos = 0;
        let err = parse_block_body(&bytes, &mut pos)
            .err()
            .expect("2^60 array offset must be rejected in the buffer path");
        assert_protocol(&err, "array offset total 1152921504606846976");
    }

    #[test]
    fn discard_array_offset_2_pow_60_is_rejected() {
        let mut bytes = vec![0x00];
        bytes.extend_from_slice(&varint(1));
        bytes.extend_from_slice(&varint(1));
        bytes.extend_from_slice(&column_header("c", "Array(UInt8)"));
        bytes.extend_from_slice(&(1u64 << 60).to_le_bytes());
        let mut reader = Cursor::new(bytes);
        let err = discard_block_body(&mut reader)
            .expect_err("2^60 array offset must be rejected on the discard path");
        assert_protocol(&err, "array offset total 1152921504606846976");
    }

    #[test]
    fn streamed_map_offset_decrease_is_rejected() {
        let mut bytes = block_with_rows("Map(UInt8, UInt8)", 2);
        bytes.extend_from_slice(&9u64.to_le_bytes());
        bytes.extend_from_slice(&4u64.to_le_bytes()); // decreasing
        let mut reader = Cursor::new(bytes);
        let err = read_block(&mut reader)
            .err()
            .expect("decreasing map offsets must be rejected");
        assert_protocol(&err, "map offset decreased from 9 to 4");
    }

    #[test]
    fn fixed_width_cap_plus_one_is_rejected_before_allocation() {
        // UInt256 rows*32 = 64 MiB + 32 must fail before the arena reserves.
        let mut bytes = vec![0x00];
        bytes.extend_from_slice(&varint(1)); // columns
        bytes.extend_from_slice(&varint((MAX_COLUMN_BYTES / 32 + 1) as u64)); // rows
        bytes.extend_from_slice(&column_header("c", "UInt256"));
        let mut reader = Cursor::new(bytes);
        let err = read_block_body(&mut reader)
            .err()
            .expect("fixed-width cap + 1 must be rejected");
        assert_protocol(
            &err,
            "fixed-width column byte length 67108896 exceeds limit 67108864",
        );
    }

    #[test]
    fn raw_low_cardinality_index_count_u64_max_is_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes()); // key serialization version
        bytes.extend_from_slice(&(1u64 << 9).to_le_bytes()); // additional keys, UInt8 indexes
        bytes.extend_from_slice(&0u64.to_le_bytes()); // no dictionary keys
        bytes.extend_from_slice(&u64::MAX.to_le_bytes()); // index count claim
        let mut reader = Cursor::new(bytes);
        let mut data = Vec::new();
        let mut budget = MAX_COLUMN_BYTES;
        let err = read_lc_body_raw_into(
            &mut reader,
            &ColumnType::UInt8,
            &RawColumnState::None,
            1,
            &mut data,
            &mut budget,
        )
        .expect_err("u64::MAX LowCardinality index count must be rejected");
        assert_protocol(&err, "LowCardinality index count 18446744073709551615");
    }

    #[test]
    fn raw_low_cardinality_index_mismatch_is_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&(1u64 << 9).to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes()); // no dictionary keys
        bytes.extend_from_slice(&5u64.to_le_bytes()); // index count != rows
        let mut reader = Cursor::new(bytes);
        let mut data = Vec::new();
        let mut budget = MAX_COLUMN_BYTES;
        let err = read_lc_body_raw_into(
            &mut reader,
            &ColumnType::UInt8,
            &RawColumnState::None,
            1,
            &mut data,
            &mut budget,
        )
        .expect_err("LowCardinality index mismatch must be rejected");
        assert_protocol(
            &err,
            "LowCardinality index count 5 does not match row count 1",
        );
    }

    #[test]
    fn variant_compact_rows_claim_is_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes()); // mode = COMPACT
        bytes.extend_from_slice(&0u64.to_le_bytes()); // discriminator
        bytes.extend_from_slice(&(1u64 << 60).to_le_bytes()); // compact rows claim
        let mut reader = Cursor::new(bytes);
        let mut data = Vec::new();
        let mut budget = MAX_COLUMN_BYTES;
        let err = read_variant_types_body_into(
            &mut reader,
            &["UInt8".to_string()],
            &[RawColumnState::None],
            1,
            &mut data,
            false,
            &mut budget,
        )
        .expect_err("huge Variant compact rows claim must be rejected");
        assert_protocol(
            &err,
            "Variant compact rows 1152921504606846976 exceeds row count 1",
        );
    }

    #[test]
    fn variant_compact_rows_equal_to_rows_parse() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes()); // mode = COMPACT
        bytes.extend_from_slice(&0u64.to_le_bytes()); // discriminator (UInt8)
        bytes.extend_from_slice(&2u64.to_le_bytes()); // compact rows == rows
        bytes.extend_from_slice(&[7, 9]); // two UInt8 values
        let mut reader = Cursor::new(bytes);
        let mut data = Vec::new();
        let mut budget = MAX_COLUMN_BYTES;
        read_variant_types_body_into(
            &mut reader,
            &["UInt8".to_string()],
            &[RawColumnState::None],
            2,
            &mut data,
            false,
            &mut budget,
        )
        .expect("compact rows == rows parses");
        assert_eq!(&data[data.len() - 2..], &[7, 9]);
    }

    #[test]
    fn low_cardinality_materialization_cap_is_enforced_before_allocating() {
        // 2,097,153 rows of a 32-byte entry would materialize 64 MiB + 32:
        // the capacity claim must fail before with_capacity allocates.
        let dict = vec![0u8; 32];
        let err = super::materialize_lc_fixed(&dict, 32, b"", 4, MAX_COLUMN_BYTES / 32 + 1)
            .expect_err("materialization cap + 1 must be rejected");
        assert_protocol(
            &err,
            "LowCardinality materialized column byte length 67108896 exceeds limit 67108864",
        );
    }
}
