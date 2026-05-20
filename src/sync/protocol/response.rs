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
    let num_cols = checked_usize(parse_varint(buf, pos)?, "columns")?;
    let num_rows = checked_usize(parse_varint(buf, pos)?, "rows")?;
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
    let num_cols = checked_usize(parse_varint(buf, pos)?, "columns")?;
    let num_rows = checked_usize(parse_varint(buf, pos)?, "rows")?;
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
    let num_cols = checked_usize(wire::read_varint(reader)?, "columns")?;
    let num_rows = checked_usize(wire::read_varint(reader)?, "rows")?;

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
        read_column_data_into(reader, &ct, num_rows, &mut arena)?;
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

pub(crate) fn discard_block<R: Read>(reader: &mut R) -> Result<usize> {
    let _table = wire::read_string(reader)?;
    discard_block_body(reader)
}

pub(crate) fn discard_block_body<R: Read>(reader: &mut R) -> Result<usize> {
    skip_block_info_from_reader(reader)?;
    let num_cols = checked_usize(wire::read_varint(reader)?, "columns")?;
    let num_rows = checked_usize(wire::read_varint(reader)?, "rows")?;

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
    let num_cols = checked_usize(wire::read_varint(reader)?, "columns")?;
    let num_rows = checked_usize(wire::read_varint(reader)?, "rows")?;

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
        read_column_data_into(reader, &ct, num_rows, &mut arena)?;
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
    reader: &mut R, ct: &ColumnType, rows: usize, data: &mut Vec<u8>,
) -> Result<()> {
    if rows == 0 {
        return Ok(());
    }

    use ColumnType::*;
    match ct {
        UInt8 | Int8 | Bool | Enum8 => read_exact_into(reader, data, rows)?,
        UInt16 | Int16 | Date | Enum16 => read_exact_into(reader, data, checked_len(rows, 2)?)?,
        UInt32 | Int32 | Float32 | Date32 | DateTime | Time | IPv4 => {
            read_exact_into(reader, data, checked_len(rows, 4)?)?
        },
        UInt64 | Int64 | Float64 | DateTime64(_) | Time64(_) => {
            read_exact_into(reader, data, checked_len(rows, 8)?)?
        },
        UInt128 | Int128 | UUID | IPv6 => read_exact_into(reader, data, checked_len(rows, 16)?)?,
        UInt256 | Int256 => read_exact_into(reader, data, checked_len(rows, 32)?)?,
        Decimal(1..=9, _) => read_exact_into(reader, data, checked_len(rows, 4)?)?,
        Decimal(10..=18, _) => read_exact_into(reader, data, checked_len(rows, 8)?)?,
        Decimal(19..=38, _) => read_exact_into(reader, data, checked_len(rows, 16)?)?,
        Decimal(39..=76, _) => read_exact_into(reader, data, checked_len(rows, 32)?)?,
        Decimal(precision, _) => {
            return Err(crate::sync::error::Error::Protocol(format!(
                "unsupported Decimal precision {precision}"
            )));
        },
        Nothing => read_exact_into(reader, data, rows)?,
        String => {
            for _ in 0..rows {
                let len = checked_usize(read_varint_into(reader, data)?, "string value length")?;
                read_exact_into(reader, data, len)?;
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
                let len = checked_usize(read_varint_into(reader, data)?, "JSON string length")?;
                read_exact_into(reader, data, len)?;
            }
        },
        FixedString(n) => read_exact_into(reader, data, checked_len(rows, *n)?)?,
        Nullable(inner) => {
            read_exact_into(reader, data, rows)?;
            read_column_data_into(reader, inner, rows, data)?;
        },
        Array(inner) => {
            read_exact_into(reader, data, checked_len(rows, 8)?)?;
            let elem_rows = if rows > 0 {
                let start = data.len() - 8;
                let mut offset_bytes = [0u8; 8];
                offset_bytes.copy_from_slice(&data[start..start + 8]);
                usize::try_from(u64::from_le_bytes(offset_bytes)).map_err(|_| {
                    crate::sync::error::Error::Protocol("array offset too large".into())
                })?
            } else {
                0
            };
            read_column_data_into(reader, inner, elem_rows, data)?;
        },
        Map(k, v) => {
            read_exact_into(reader, data, checked_len(rows, 8)?)?;
            let elem_rows = if rows > 0 {
                let start = data.len() - 8;
                let mut offset_bytes = [0u8; 8];
                offset_bytes.copy_from_slice(&data[start..start + 8]);
                usize::try_from(u64::from_le_bytes(offset_bytes)).map_err(|_| {
                    crate::sync::error::Error::Protocol("map offset too large".into())
                })?
            } else {
                0
            };
            read_column_data_into(reader, k, elem_rows, data)?;
            read_column_data_into(reader, v, elem_rows, data)?;
        },
        Tuple(elems) => {
            for elem in elems {
                read_column_data_into(reader, elem, rows, data)?;
            }
        },
        LowCardinality(inner) => read_low_cardinality_into(reader, inner, rows, data)?,
        Point => {
            read_column_data_into(reader, &Float64, rows, data)?;
            read_column_data_into(reader, &Float64, rows, data)?;
        },
        Ring => read_column_data_into(reader, &Array(Box::new(Point)), rows, data)?,
        Polygon => read_column_data_into(reader, &Array(Box::new(Ring)), rows, data)?,
        MultiPolygon => read_column_data_into(reader, &Array(Box::new(Polygon)), rows, data)?,
        Dynamic => {
            let state = read_dynamic_state_prefix_into(reader, data)?;
            read_dynamic_body_into(reader, &state, rows, data)?;
        },
        Variant(types) => {
            let mut states = Vec::with_capacity(types.len());
            for typ in types {
                states.push(read_column_state_prefix_into(reader, typ, data)?);
            }
            read_variant_body_into(reader, types, &states, rows, data)?;
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
    reader: &mut R, ct: &ColumnType, data: &mut Vec<u8>,
) -> Result<RawColumnState> {
    use ColumnType::*;
    match ct {
        Nullable(inner) => Ok(RawColumnState::Nullable(Box::new(
            read_column_state_prefix_into(reader, inner, data)?,
        ))),
        Array(inner) => Ok(RawColumnState::Array(Box::new(
            read_column_state_prefix_into(reader, inner, data)?,
        ))),
        Map(key, value) => Ok(RawColumnState::Map(
            Box::new(read_column_state_prefix_into(reader, key, data)?),
            Box::new(read_column_state_prefix_into(reader, value, data)?),
        )),
        Tuple(elems) => {
            let mut states = Vec::with_capacity(elems.len());
            for elem in elems {
                states.push(read_column_state_prefix_into(reader, elem, data)?);
            }
            Ok(RawColumnState::Tuple(states))
        },
        LowCardinality(inner) => Ok(RawColumnState::LowCardinality(Box::new(
            read_column_state_prefix_into(reader, inner, data)?,
        ))),
        Variant(types) => {
            let mut states = Vec::with_capacity(types.len());
            for typ in types {
                states.push(read_column_state_prefix_into(reader, typ, data)?);
            }
            Ok(RawColumnState::Variant(states))
        },
        Dynamic => read_dynamic_state_prefix_into(reader, data).map(RawColumnState::Dynamic),
        JSON => read_json_state_prefix_into(reader, data).map(RawColumnState::Json),
        _ => Ok(RawColumnState::None),
    }
}

fn read_json_state_prefix_into<R: Read>(
    reader: &mut R, data: &mut Vec<u8>,
) -> Result<JsonRawState> {
    let version = read_u64_into(reader, data)?;
    let mut dynamic_paths = Vec::new();
    match version {
        1 | 4 => {},
        3 => {
            let paths_count = checked_usize(read_varint_into(reader, data)?, "JSON paths")?;
            for _ in 0..paths_count {
                let _path = read_string_into(reader, data, "JSON path length")?;
            }
            dynamic_paths.reserve(paths_count);
            for _ in 0..paths_count {
                dynamic_paths.push(read_dynamic_state_prefix_into(reader, data)?);
            }
        },
        0 => {
            let _max_dynamic_paths = read_varint_into(reader, data)?;
            let paths_count = checked_usize(read_varint_into(reader, data)?, "JSON paths")?;
            for _ in 0..paths_count {
                let _path = read_string_into(reader, data, "JSON path length")?;
            }
            dynamic_paths.reserve(paths_count);
            for _ in 0..paths_count {
                dynamic_paths.push(read_dynamic_state_prefix_into(reader, data)?);
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
    reader: &mut R, state: &JsonRawState, rows: usize, data: &mut Vec<u8>,
) -> Result<()> {
    match state.version {
        1 | 4 => {
            for _ in 0..rows {
                let len = checked_usize(read_varint_into(reader, data)?, "JSON string length")?;
                read_exact_into(reader, data, len)?;
            }
        },
        3 => {
            for dynamic in &state.dynamic_paths {
                read_dynamic_body_into(reader, dynamic, rows, data)?;
            }
        },
        0 => {
            for dynamic in &state.dynamic_paths {
                read_dynamic_body_into(reader, dynamic, rows, data)?;
            }
            read_exact_into(reader, data, checked_len(rows, 8)?)?;
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
    reader: &mut R, data: &mut Vec<u8>,
) -> Result<DynamicRawState> {
    let version = read_u64_into(reader, data)?;
    let mut type_names = Vec::new();
    let mut type_states = Vec::new();
    match version {
        0 => {},
        1 => {
            let _max_types = read_varint_into(reader, data)?;
            type_names = read_dynamic_type_names_into(reader, data, "dynamic subcolumn types")?;
            let _variant_version = read_u64_into(reader, data)?;
        },
        2 | 3 => {
            type_names = read_dynamic_type_names_into(reader, data, "dynamic subcolumn types")?;
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
        type_states.push(read_column_state_prefix_into(reader, &ct, data)?);
    }
    Ok(DynamicRawState {
        version,
        type_names,
        type_states,
    })
}

fn read_dynamic_body_into<R: Read>(
    reader: &mut R, state: &DynamicRawState, rows: usize, data: &mut Vec<u8>,
) -> Result<()> {
    match state.version {
        0 => Ok(()),
        1 => read_deprecated_dynamic_values_into(
            reader,
            &state.type_names,
            &state.type_states,
            rows,
            data,
        ),
        2 | 3 => read_flattened_dynamic_values_into(
            reader,
            &state.type_names,
            &state.type_states,
            rows,
            data,
        ),
        other => Err(crate::sync::error::Error::Protocol(format!(
            "unknown Dynamic serialization version {other}"
        ))),
    }
}

fn read_deprecated_dynamic_values_into<R: Read>(
    reader: &mut R, type_names: &[String], type_states: &[RawColumnState], rows: usize,
    data: &mut Vec<u8>,
) -> Result<()> {
    if rows == 0 || type_names.is_empty() {
        return Ok(());
    }
    let start = data.len();
    read_exact_into(reader, data, rows)?;
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
    read_dynamic_subcolumns_into(reader, type_names, type_states, &counts, data)
}

fn read_flattened_dynamic_values_into<R: Read>(
    reader: &mut R, type_names: &[String], type_states: &[RawColumnState], rows: usize,
    data: &mut Vec<u8>,
) -> Result<()> {
    if rows == 0 || type_names.is_empty() {
        return Ok(());
    }
    let width = dynamic_discriminator_width(type_names.len());
    let len = checked_len(rows, width)?;
    let start = data.len();
    read_exact_into(reader, data, len)?;
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
    read_dynamic_subcolumns_into(reader, type_names, type_states, &counts, data)
}

fn read_dynamic_subcolumns_into<R: Read>(
    reader: &mut R, type_names: &[String], type_states: &[RawColumnState], counts: &[usize],
    data: &mut Vec<u8>,
) -> Result<()> {
    for (idx, (type_name, count)) in type_names.iter().zip(counts).enumerate() {
        if *count == 0 {
            continue;
        }
        let ct = crate::sync::protocol::type_parser::parse_type(type_name).map_err(|e| {
            crate::sync::error::Error::Protocol(format!("bad dynamic type '{type_name}': {e}"))
        })?;
        let state = type_states.get(idx).unwrap_or(&RawColumnState::None);
        read_column_body_raw_into(reader, &ct, state, *count, data)?;
    }
    Ok(())
}

fn read_variant_body_into<R: Read>(
    reader: &mut R, types: &[ColumnType], type_states: &[RawColumnState], rows: usize,
    data: &mut Vec<u8>,
) -> Result<()> {
    let type_names = types.iter().map(ToString::to_string).collect::<Vec<_>>();
    read_variant_types_body_into(reader, &type_names, type_states, rows, data, false)
}

fn read_variant_types_body_into<R: Read>(
    reader: &mut R, type_names: &[String], type_states: &[RawColumnState], rows: usize,
    data: &mut Vec<u8>, one_based_discriminators: bool,
) -> Result<()> {
    let mode = read_u64_into(reader, data)?;
    if rows == 0 || type_names.is_empty() {
        return Ok(());
    }
    match mode {
        0 => {
            let start = data.len();
            read_exact_into(reader, data, rows)?;
            let mut counts = vec![0usize; type_names.len()];
            for &discriminator in &data[start..start + rows] {
                let idx = if one_based_discriminators {
                    discriminator.checked_sub(1).map(usize::from)
                } else {
                    Some(usize::from(discriminator))
                };
                if let Some(idx) = idx {
                    if idx < counts.len() {
                        counts[idx] += 1;
                    }
                }
            }
            read_dynamic_subcolumns_into(reader, type_names, type_states, &counts, data)
        },
        1 => {
            let discriminator = checked_usize(
                read_u64_into(reader, data)?,
                "Variant compact discriminator",
            )?;
            let discriminator = if one_based_discriminators {
                discriminator.saturating_sub(1)
            } else {
                discriminator
            };
            let compact_rows = checked_usize(read_u64_into(reader, data)?, "Variant compact rows")?;
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
                read_column_body_raw_into(reader, &ct, state, compact_rows, data)?;
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
) -> Result<()> {
    if rows == 0 {
        return Ok(());
    }

    use ColumnType::*;
    match ct {
        Nullable(inner) => {
            read_exact_into(reader, data, rows)?;
            let inner_state = match state {
                RawColumnState::Nullable(inner_state) => inner_state.as_ref(),
                _ => &RawColumnState::None,
            };
            read_column_body_raw_into(reader, inner, inner_state, rows, data)
        },
        Array(inner) => {
            let mut total = 0usize;
            for _ in 0..rows {
                let offset = read_u64_into(reader, data)?;
                total = total.max(checked_usize(offset, "array offset")?);
            }
            if total > 0 {
                let inner_state = match state {
                    RawColumnState::Array(inner_state) => inner_state.as_ref(),
                    _ => &RawColumnState::None,
                };
                read_column_body_raw_into(reader, inner, inner_state, total, data)?;
            }
            Ok(())
        },
        Map(key, value) => {
            let mut total = 0usize;
            for _ in 0..rows {
                let offset = read_u64_into(reader, data)?;
                total = total.max(checked_usize(offset, "map offset")?);
            }
            if total > 0 {
                let (key_state, value_state) = match state {
                    RawColumnState::Map(key_state, value_state) => {
                        (key_state.as_ref(), value_state.as_ref())
                    },
                    _ => (&RawColumnState::None, &RawColumnState::None),
                };
                read_column_body_raw_into(reader, key, key_state, total, data)?;
                read_column_body_raw_into(reader, value, value_state, total, data)?;
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
                read_column_body_raw_into(reader, elem, elem_state, rows, data)?;
            }
            Ok(())
        },
        LowCardinality(inner) => {
            let inner_state = match state {
                RawColumnState::LowCardinality(inner_state) => inner_state.as_ref(),
                _ => &RawColumnState::None,
            };
            read_lc_body_raw_into(reader, inner, inner_state, data)
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
            read_json_body_into(reader, json_state, rows, data)
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
            read_dynamic_body_into(reader, dynamic_state, rows, data)
        },
        Variant(types) => {
            let states = match state {
                RawColumnState::Variant(states) => states.as_slice(),
                _ => &[],
            };
            read_variant_body_into(reader, types, states, rows, data)
        },
        Point => {
            read_column_body_raw_into(reader, &Float64, &RawColumnState::None, rows, data)?;
            read_column_body_raw_into(reader, &Float64, &RawColumnState::None, rows, data)
        },
        Ring => read_column_body_raw_into(
            reader,
            &Array(Box::new(Point)),
            &RawColumnState::None,
            rows,
            data,
        ),
        Polygon => read_column_body_raw_into(
            reader,
            &Array(Box::new(Ring)),
            &RawColumnState::None,
            rows,
            data,
        ),
        MultiPolygon => read_column_body_raw_into(
            reader,
            &Array(Box::new(Polygon)),
            &RawColumnState::None,
            rows,
            data,
        ),
        String | Other(_) => {
            for _ in 0..rows {
                let len = checked_usize(read_varint_into(reader, data)?, "string value length")?;
                read_exact_into(reader, data, len)?;
            }
            Ok(())
        },
        FixedString(n) => read_exact_into(reader, data, checked_len(rows, *n)?),
        AggregateFunction | SimpleAggregateFunction => Err(crate::sync::error::Error::Protocol(
            format!("raw capture does not support type {ct}"),
        )),
        _ => {
            let width = ct
                .fixed_width()
                .ok_or_else(|| crate::sync::error::Error::Protocol(format!("unknown type {ct}")))?;
            read_exact_into(reader, data, checked_len(rows, width)?)
        },
    }
}

fn read_lc_body_raw_into<R: Read>(
    reader: &mut R, inner: &ColumnType, inner_state: &RawColumnState, data: &mut Vec<u8>,
) -> Result<()> {
    let start = data.len();
    read_exact_into(reader, data, 24)?;
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
    let num_keys = checked_usize(
        u64::from_le_bytes(data[start + 16..start + 24].try_into().map_err(|_| {
            crate::sync::error::Error::Protocol("LowCardinality key count length mismatch".into())
        })?),
        "LowCardinality keys",
    )?;
    let idx_width = 1usize << (serial_type & 0x3);
    if num_keys > 0 {
        read_column_body_raw_into(reader, inner, inner_state, num_keys, data)?;
    }
    let indexes = checked_usize(read_u64_into(reader, data)?, "LowCardinality indexes")?;
    read_exact_into(reader, data, checked_len(indexes, idx_width)?)
}

fn read_dynamic_type_names_into<R: Read>(
    reader: &mut R, data: &mut Vec<u8>, count_name: &str,
) -> Result<Vec<String>> {
    let type_count = checked_usize(read_varint_into(reader, data)?, count_name)?;
    let mut type_names = Vec::with_capacity(type_count);
    for _ in 0..type_count {
        type_names.push(read_string_into(reader, data, "dynamic type length")?);
    }
    Ok(type_names)
}

fn read_string_into<R: Read>(reader: &mut R, data: &mut Vec<u8>, name: &str) -> Result<String> {
    let len = checked_usize(read_varint_into(reader, data)?, name)?;
    let start = data.len();
    read_exact_into(reader, data, len)?;
    std::str::from_utf8(&data[start..start + len])
        .map(str::to_owned)
        .map_err(|e| crate::sync::error::Error::Protocol(format!("invalid UTF-8 string: {e}")))
}

fn read_u64_into<R: Read>(reader: &mut R, data: &mut Vec<u8>) -> Result<u64> {
    let start = data.len();
    read_exact_into(reader, data, 8)?;
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
    reader: &mut R, inner: &ColumnType, rows: usize, data: &mut Vec<u8>,
) -> Result<()> {
    let mut meta = [0u8; 24];
    reader.read_exact(&mut meta)?;
    let version = u64::from_le_bytes(meta[0..8].try_into().map_err(|_| {
        crate::sync::error::Error::Protocol("LowCardinality version length mismatch".into())
    })?);
    if version != 1 {
        return Err(crate::sync::error::Error::Protocol(format!(
            "unsupported LowCardinality key serialization version {version}"
        )));
    }
    let serial_type = u64::from_le_bytes(meta[8..16].try_into().map_err(|_| {
        crate::sync::error::Error::Protocol("LowCardinality metadata length mismatch".into())
    })?);
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
    let num_keys = checked_usize(
        u64::from_le_bytes(meta[16..24].try_into().map_err(|_| {
            crate::sync::error::Error::Protocol("LowCardinality key count length mismatch".into())
        })?),
        "LowCardinality keys",
    )?;
    let idx_width = 1usize << (serial_type & 0x3);
    let mut dict_data = Vec::new();
    read_column_data_into(reader, inner, num_keys, &mut dict_data)?;
    let mut count = [0u8; 8];
    reader.read_exact(&mut count)?;
    let indexes = checked_usize(u64::from_le_bytes(count), "LowCardinality indexes")?;
    if indexes != rows {
        return Err(crate::sync::error::Error::Protocol(format!(
            "LowCardinality index count {indexes} does not match row count {rows}"
        )));
    }
    let mut index_data = vec![0u8; checked_len(indexes, idx_width)?];
    reader.read_exact(&mut index_data)?;
    let materialized =
        materialize_low_cardinality_inner(&dict_data, inner, &index_data, idx_width, indexes)?;
    data.extend_from_slice(&materialized);
    Ok(())
}

fn read_low_cardinality_from_buffer(
    buf: &[u8], pos: &mut usize, inner: &ColumnType, rows: usize,
) -> Result<Vec<u8>> {
    let meta = parse_bytes(buf, pos, 24)?;
    let version = u64::from_le_bytes(meta[0..8].try_into().map_err(|_| {
        crate::sync::error::Error::Protocol("LowCardinality version length mismatch".into())
    })?);
    if version != 1 {
        return Err(crate::sync::error::Error::Protocol(format!(
            "unsupported LowCardinality key serialization version {version}"
        )));
    }
    let serial_type = u64::from_le_bytes(meta[8..16].try_into().map_err(|_| {
        crate::sync::error::Error::Protocol("LowCardinality metadata length mismatch".into())
    })?);
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
    let num_keys = checked_usize(
        u64::from_le_bytes(meta[16..24].try_into().map_err(|_| {
            crate::sync::error::Error::Protocol("LowCardinality key count length mismatch".into())
        })?),
        "LowCardinality keys",
    )?;
    let idx_width = 1usize << (serial_type & 0x3);
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
    let index_data = parse_bytes(buf, pos, checked_len(indexes, idx_width)?)?;
    materialize_low_cardinality_inner(dict_data, inner, index_data, idx_width, indexes)
}

fn read_exact_into<R: Read>(reader: &mut R, data: &mut Vec<u8>, len: usize) -> Result<()> {
    if len == 0 {
        return Ok(());
    }

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
        UInt16 | Int16 | Date | Enum16 => discard_exact(reader, checked_len(rows, 2)?)?,
        UInt32 | Int32 | Float32 | Date32 | DateTime | Time | IPv4 => {
            discard_exact(reader, checked_len(rows, 4)?)?
        },
        UInt64 | Int64 | Float64 | DateTime64(_) | Time64(_) => {
            discard_exact(reader, checked_len(rows, 8)?)?
        },
        UInt128 | Int128 | UUID | IPv6 => discard_exact(reader, checked_len(rows, 16)?)?,
        UInt256 | Int256 => discard_exact(reader, checked_len(rows, 32)?)?,
        Decimal(1..=9, _) => discard_exact(reader, checked_len(rows, 4)?)?,
        Decimal(10..=18, _) => discard_exact(reader, checked_len(rows, 8)?)?,
        Decimal(19..=38, _) => discard_exact(reader, checked_len(rows, 16)?)?,
        Decimal(39..=76, _) => discard_exact(reader, checked_len(rows, 32)?)?,
        Decimal(precision, _) => {
            return Err(crate::sync::error::Error::Protocol(format!(
                "unsupported Decimal precision {precision}"
            )));
        },
        String => {
            for _ in 0..rows {
                let len = checked_usize(wire::read_varint(reader)?, "string value length")?;
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
                let len = checked_usize(wire::read_varint(reader)?, "JSON string length")?;
                discard_exact(reader, len)?;
            }
        },
        FixedString(n) => discard_exact(reader, checked_len(rows, *n)?)?,
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
    if version != 1 {
        return Err(crate::sync::error::Error::Protocol(format!(
            "unsupported LowCardinality key serialization version {version}"
        )));
    }
    let serial_type = u64::from_le_bytes(meta[8..16].try_into().map_err(|_| {
        crate::sync::error::Error::Protocol("LowCardinality metadata length mismatch".into())
    })?);
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
    let num_keys = checked_usize(
        u64::from_le_bytes(meta[16..24].try_into().map_err(|_| {
            crate::sync::error::Error::Protocol("LowCardinality key count length mismatch".into())
        })?),
        "LowCardinality keys",
    )?;
    let idx_width = 1usize << (serial_type & 0x3);
    discard_column_data(reader, inner, num_keys)?;
    let mut count = [0u8; 8];
    reader.read_exact(&mut count)?;
    let indexes = checked_usize(u64::from_le_bytes(count), "LowCardinality indexes")?;
    if indexes != rows {
        return Err(crate::sync::error::Error::Protocol(format!(
            "LowCardinality index count {indexes} does not match row count {rows}"
        )));
    }
    discard_exact(reader, checked_len(indexes, idx_width)?)
}

fn discard_offsets<R: Read>(reader: &mut R, rows: usize, name: &str) -> Result<usize> {
    let mut offset = [0u8; 8];
    for _ in 0..rows {
        reader.read_exact(&mut offset)?;
    }
    checked_usize(u64::from_le_bytes(offset), name)
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
    let mut out = Vec::with_capacity(checked_len(num_idx, width)?);
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
    for row in 0..num_idx {
        let idx = read_lc_index(indexes, row, idx_width)?;
        let range = entries.get(idx).ok_or_else(|| {
            crate::sync::error::Error::Protocol(
                "LowCardinality dictionary index out of bounds".into(),
            )
        })?;
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
    let mut out = Vec::with_capacity(num_idx.saturating_add(values.len()));
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

fn checked_len(rows: usize, width: usize) -> Result<usize> {
    rows.checked_mul(width)
        .ok_or_else(|| crate::sync::error::Error::Protocol("column byte length overflow".into()))
}

fn checked_usize(value: u64, name: &str) -> Result<usize> {
    usize::try_from(value)
        .map_err(|_| crate::sync::error::Error::Protocol(format!("{name} count too large")))
}

fn read_varint_into<R: Read>(reader: &mut R, data: &mut Vec<u8>) -> Result<u64> {
    let mut r = 0u64;
    let mut shift = 0;
    loop {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        data.push(byte[0]);
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
/// Advances `pos` past the column's wire data based on its type.
fn skip_column_data(buf: &[u8], pos: &mut usize, ct: &ColumnType, rows: usize) -> Result<()> {
    if rows == 0 {
        return Ok(());
    }

    use ColumnType::*;
    match ct {
        // Fixed-width primitives
        UInt8 | Int8 | Bool | Enum8 => advance(buf, pos, rows)?,
        UInt16 | Int16 | Date | Enum16 => advance(buf, pos, checked_len(rows, 2)?)?,
        UInt32 | Int32 | Float32 | Date32 | DateTime | Time | IPv4 => {
            advance(buf, pos, checked_len(rows, 4)?)?
        },
        UInt64 | Int64 | Float64 | DateTime64(_) | Time64(_) => {
            advance(buf, pos, checked_len(rows, 8)?)?
        },
        UInt128 | Int128 | UUID | IPv6 => advance(buf, pos, checked_len(rows, 16)?)?,
        UInt256 | Int256 => advance(buf, pos, checked_len(rows, 32)?)?,

        // Decimal: width depends on precision
        Decimal(1..=9, _) => advance(buf, pos, checked_len(rows, 4)?)?,
        Decimal(10..=18, _) => advance(buf, pos, checked_len(rows, 8)?)?,
        Decimal(19..=38, _) => advance(buf, pos, checked_len(rows, 16)?)?,
        Decimal(39..=76, _) => advance(buf, pos, checked_len(rows, 32)?)?,

        // String: each row is varint-len + data
        String => {
            for _ in 0..rows {
                let l = usize::try_from(parse_varint(buf, pos)?)
                    .map_err(|_| crate::sync::error::Error::Protocol("string too large".into()))?;
                advance(buf, pos, l)?;
            }
        },

        JSON => {
            let version_start = *pos;
            advance(buf, pos, 8)?;
            let mut version_bytes = [0u8; 8];
            version_bytes.copy_from_slice(&buf[version_start..version_start + 8]);
            let version = u64::from_le_bytes(version_bytes);
            if version != 1 && version != 4 {
                return Err(crate::sync::error::Error::Protocol(format!(
                    "materialized JSON reads require string serialization version 1 or 4, got {version}; \
                     enable {}=1 or use query_raw",
                    crate::sync::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING
                )));
            }
            for _ in 0..rows {
                let l = usize::try_from(parse_varint(buf, pos)?).map_err(|_| {
                    crate::sync::error::Error::Protocol("JSON string too large".into())
                })?;
                advance(buf, pos, l)?;
            }
        },

        // FixedString: rows * n
        FixedString(n) => advance(buf, pos, checked_len(rows, *n)?)?,

        Nothing => advance(buf, pos, rows)?,

        // Nullable: null mask (1 byte per row) + inner column
        Nullable(inner) => {
            advance(buf, pos, rows)?; // null mask
            skip_column_data(buf, pos, inner, rows)?;
        },

        // Array: offset array (rows * 8 bytes) + inner column
        Array(inner) => {
            let off_len = checked_len(rows, 8)?;
            let off_end = (*pos).checked_add(off_len).ok_or_else(|| {
                crate::sync::error::Error::Protocol("array offset overflow".into())
            })?;
            if off_end > buf.len() {
                return Err(crate::sync::error::Error::Protocol(
                    "unexpected end of buffer parsing array offsets".into(),
                ));
            }
            let last_off = if rows > 0 {
                let mut offset_bytes = [0u8; 8];
                offset_bytes.copy_from_slice(&buf[off_end - 8..off_end]);
                usize::try_from(u64::from_le_bytes(offset_bytes)).map_err(|_| {
                    crate::sync::error::Error::Protocol("array offset too large".into())
                })?
            } else {
                0
            };
            *pos = off_end;
            // For arrays of depth > 1, the offsets are nested
            let elem_rows = if last_off > 0 {
                last_off
            } else {
                rows.saturating_sub(1)
            };
            skip_column_data(buf, pos, inner, elem_rows)?;
        },

        // Map: keys array + values array
        Map(k, v) => {
            skip_column_data(buf, pos, k, rows)?;
            skip_column_data(buf, pos, v, rows)?;
        },

        // Tuple: each element
        Tuple(elems) => {
            for elem in elems {
                skip_column_data(buf, pos, elem, rows)?;
            }
        },
        Point => {
            skip_column_data(buf, pos, &Float64, rows)?;
            skip_column_data(buf, pos, &Float64, rows)?;
        },
        Ring => skip_column_data(buf, pos, &Array(Box::new(Point)), rows)?,
        Polygon => skip_column_data(buf, pos, &Array(Box::new(Ring)), rows)?,
        MultiPolygon => skip_column_data(buf, pos, &Array(Box::new(Polygon)), rows)?,

        // LowCardinality: version + index serialization type + dictionary + indexes.
        LowCardinality(inner) => {
            let meta = parse_bytes(buf, pos, 24)?;
            let version = u64::from_le_bytes(meta[0..8].try_into().map_err(|_| {
                crate::sync::error::Error::Protocol("LowCardinality version length mismatch".into())
            })?);
            if version != 1 {
                return Err(crate::sync::error::Error::Protocol(format!(
                    "unsupported LowCardinality key serialization version {version}"
                )));
            }
            let serial_type = u64::from_le_bytes(meta[8..16].try_into().map_err(|_| {
                crate::sync::error::Error::Protocol(
                    "LowCardinality metadata length mismatch".into(),
                )
            })?);
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
            let idx_width = 1usize << (serial_type & 0x3);
            let num_keys = checked_usize(
                u64::from_le_bytes(meta[16..24].try_into().map_err(|_| {
                    crate::sync::error::Error::Protocol(
                        "LowCardinality key count length mismatch".into(),
                    )
                })?),
                "LowCardinality keys",
            )?;
            skip_column_data(buf, pos, inner, num_keys)?;
            let count_bytes = parse_bytes(buf, pos, 8)?;
            let indexes = checked_usize(
                u64::from_le_bytes(count_bytes.try_into().map_err(|_| {
                    crate::sync::error::Error::Protocol(
                        "LowCardinality index count length mismatch".into(),
                    )
                })?),
                "LowCardinality indexes",
            )?;
            advance(buf, pos, checked_len(indexes, idx_width)?)?;
        },

        // Variant: mode (8 bytes) + discriminators + sub-columns
        Variant(_types) => {
            advance(buf, pos, 8)?; // mode
            // Read discriminators
            let mut mode_bytes = [0u8; 8];
            mode_bytes.copy_from_slice(&buf[*pos - 8..*pos]);
            let mode = u64::from_le_bytes(mode_bytes);
            match mode {
                0 => {
                    // BASIC: 1 byte per row
                    advance(buf, pos, rows)?;
                },
                1 => {
                    // COMPACT: start_offset(8) + num_rows(8)
                    advance(buf, pos, 16)?;
                },
                _ => {},
            }
            // Skip remaining bytes (sub-columns)
            // Complex: each discriminator maps to a type
            // Simple: skip rest as unknown-size blob
        },

        // Dynamic, AggregateFunction: opaque blobs (skip rest)
        Dynamic | AggregateFunction | SimpleAggregateFunction => {
            // These have complex internal structure — skip to end
            // For simplicity, we just leave pos at current (caller handles)
            // Actually these consume remaining bytes
        },

        // Geo types
        _ => {
            // Unknown types — skip remaining bytes
        },
    }
    Ok(())
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
