use crate::connection::io::{
    checked_column_len, checked_count, checked_monotonic_offset, checked_string_len, checked_usize,
    lc_idx_width,
};
use crate::error::Result;
use crate::protocol::block::RawBlock;
use crate::protocol::type_parser;
use crate::runtime::io::AsyncReadExt;

/// Charge `len` claimed bytes against a column's remaining byte budget. The
/// budget bounds the accumulated wire bytes recorded for one column so lying
/// lengths fail with a deterministic protocol error instead of growing the
/// arena without bound.
fn charge_budget(budget: &mut usize, len: usize, what: &str) -> Result<()> {
    let remaining = budget.checked_sub(len).ok_or_else(|| {
        crate::error::Error::Protocol(format!(
            "{what} cumulative byte length exceeds limit {}",
            crate::limits::MAX_COLUMN_BYTES
        ))
    })?;
    *budget = remaining;
    Ok(())
}

pub(super) async fn read_raw_data_block<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<RawBlock> {
    let mut raw = Vec::with_capacity(1024);
    // Block metadata (table name, BlockInfo, column names/types) gets its own
    // byte budget so header claims cannot grow the arena unbounded either.
    let mut meta_budget = crate::limits::MAX_COLUMN_BYTES;
    let mut name_budget = meta_budget;
    let table_name = read_string_recorded(
        stream,
        &mut Vec::new(),
        "table name length",
        &mut name_budget,
    )
    .await?;

    loop {
        let dim = read_varint_recorded(stream, &mut raw, &mut meta_budget).await?;
        match dim {
            0 => break,
            1 => read_exact_recorded(stream, &mut raw, 1, &mut meta_budget).await?,
            2 => read_exact_recorded(stream, &mut raw, 4, &mut meta_budget).await?,
            3 => {
                let _ = read_varint_recorded(stream, &mut raw, &mut meta_budget).await?;
            },
            _ => {
                return Err(crate::error::Error::Protocol(format!(
                    "unknown BlockInfo field {dim}"
                )));
            },
        }
    }

    let columns = checked_count(
        read_varint_recorded(stream, &mut raw, &mut meta_budget).await?,
        "block column",
        crate::limits::MAX_BLOCK_COLUMNS,
    )?;
    let rows = checked_count(
        read_varint_recorded(stream, &mut raw, &mut meta_budget).await?,
        "block row",
        crate::limits::MAX_BLOCK_ROWS,
    )?;

    for _ in 0..columns {
        let _name =
            read_string_recorded(stream, &mut raw, "column name length", &mut name_budget).await?;
        let type_name =
            read_string_recorded(stream, &mut raw, "column type length", &mut name_budget).await?;
        read_exact_recorded(stream, &mut raw, 1, &mut meta_budget).await?;
        if rows > 0 {
            let mut budget = crate::limits::MAX_COLUMN_BYTES;
            read_column_raw_recorded(stream, &type_name, rows, &mut raw, &mut budget).await?;
        }
    }

    Ok(RawBlock {
        table_name,
        columns,
        rows,
        data: bytes::Bytes::from(raw),
    })
}

async fn read_varint_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, out: &mut Vec<u8>, budget: &mut usize,
) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0;
    loop {
        if shift >= 64 {
            return Err(crate::error::Error::Protocol("varint overflow".into()));
        }
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await?;
        charge_budget(budget, 1, "raw column")?;
        out.push(byte[0]);
        if shift == 63 && (byte[0] & 0x7F) > 1 {
            return Err(crate::error::Error::Protocol("varint overflow".into()));
        }
        result |= u64::from(byte[0] & 0x7F) << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
}

async fn read_string_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, out: &mut Vec<u8>, length_name: &str, budget: &mut usize,
) -> Result<String> {
    let len = checked_string_len(
        read_varint_recorded(stream, out, budget).await?,
        length_name,
    )?;
    let start = out.len();
    read_exact_recorded(stream, out, len, budget).await?;
    String::from_utf8(out[start..].to_vec())
        .map_err(|e| crate::error::Error::Protocol(format!("utf8 in {length_name}: {e}")))
}

async fn read_exact_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, out: &mut Vec<u8>, len: usize, budget: &mut usize,
) -> Result<()> {
    // Single-read backstop: no individual claim may exceed one column's byte
    // budget, so a `rows * width` header can never eager-resize past the cap.
    if len > crate::limits::MAX_COLUMN_BYTES {
        return Err(crate::error::Error::Protocol(format!(
            "raw column byte length {len} exceeds limit {}",
            crate::limits::MAX_COLUMN_BYTES
        )));
    }
    charge_budget(budget, len, "raw column")?;
    let start = out.len();
    let end = start
        .checked_add(len)
        .ok_or_else(|| crate::error::Error::Protocol("raw block length overflow".into()))?;
    out.resize(end, 0);
    stream.read_exact(&mut out[start..]).await?;
    Ok(())
}

pub(super) async fn read_column_raw_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, type_name: &str, rows: usize, out: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    let ct = type_parser::parse_type(type_name)
        .map_err(|e| crate::error::Error::Protocol(format!("bad type '{type_name}': {e}")))?;
    let state = Box::pin(read_column_state_prefix_recorded(stream, &ct, out, budget)).await?;
    Box::pin(read_column_body_raw_recorded(
        stream, &ct, &state, rows, out, budget,
    ))
    .await
}

#[derive(Debug, Clone)]
pub(super) enum RawColumnState {
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
pub(super) struct DynamicRawState {
    version: u64,
    type_names: Vec<String>,
    type_states: Vec<RawColumnState>,
}

#[derive(Debug, Clone)]
pub(super) struct JsonRawState {
    version: u64,
    dynamic_paths: Vec<DynamicRawState>,
}

pub(super) fn variant_states(state: &RawColumnState) -> &[RawColumnState] {
    match state {
        RawColumnState::Variant(states) => states.as_slice(),
        _ => &[],
    }
}

pub(super) async fn read_column_state_prefix_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, ct: &type_parser::ColumnType, out: &mut Vec<u8>, budget: &mut usize,
) -> Result<RawColumnState> {
    use crate::protocol::type_parser::ColumnType::*;
    match ct {
        Nullable(inner) => Ok(RawColumnState::Nullable(Box::new(
            Box::pin(read_column_state_prefix_recorded(
                stream, inner, out, budget,
            ))
            .await?,
        ))),
        Array(inner) => Ok(RawColumnState::Array(Box::new(
            Box::pin(read_column_state_prefix_recorded(
                stream, inner, out, budget,
            ))
            .await?,
        ))),
        Map(key, value) => Ok(RawColumnState::Map(
            Box::new(Box::pin(read_column_state_prefix_recorded(stream, key, out, budget)).await?),
            Box::new(
                Box::pin(read_column_state_prefix_recorded(
                    stream, value, out, budget,
                ))
                .await?,
            ),
        )),
        Tuple(elems) => {
            let mut states = Vec::with_capacity(elems.len());
            for elem in elems {
                states.push(
                    Box::pin(read_column_state_prefix_recorded(stream, elem, out, budget)).await?,
                );
            }
            Ok(RawColumnState::Tuple(states))
        },
        LowCardinality(inner) => Ok(RawColumnState::LowCardinality(Box::new(
            Box::pin(read_column_state_prefix_recorded(
                stream, inner, out, budget,
            ))
            .await?,
        ))),
        Variant(types) => {
            let mut states = Vec::with_capacity(types.len());
            for typ in types {
                states.push(
                    Box::pin(read_column_state_prefix_recorded(stream, typ, out, budget)).await?,
                );
            }
            Ok(RawColumnState::Variant(states))
        },
        Dynamic => read_dynamic_state_prefix_recorded(stream, out, budget)
            .await
            .map(RawColumnState::Dynamic),
        JSON => read_json_state_prefix_recorded(stream, out, budget)
            .await
            .map(RawColumnState::Json),
        _ => Ok(RawColumnState::None),
    }
}

async fn read_column_body_raw_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, ct: &type_parser::ColumnType, state: &RawColumnState, rows: usize,
    out: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    if rows == 0 {
        return Ok(());
    }

    use crate::protocol::type_parser::ColumnType::*;

    match &ct {
        Nullable(inner) => {
            read_exact_recorded(stream, out, rows, budget).await?;
            let inner_state = match state {
                RawColumnState::Nullable(inner_state) => inner_state.as_ref(),
                _ => &RawColumnState::None,
            };
            Box::pin(read_column_body_raw_recorded(
                stream,
                inner,
                inner_state,
                rows,
                out,
                budget,
            ))
            .await
        },
        Array(inner) => {
            // Offsets are `rows` contiguous little-endian u64s on the wire —
            // read them in ONE bulk recorded read (charging rows*8 against the
            // budget exactly as the per-row loop did), then scan the recorded
            // slice for monotonicity like the materialized path does.
            let nbytes = checked_column_len(rows, 8, "array offset")?;
            let start = out.len();
            read_exact_recorded(stream, out, nbytes, budget).await?;
            let mut total = 0usize;
            for chunk in out[start..].chunks_exact(8) {
                let bytes: [u8; 8] = chunk.try_into().map_err(|_| {
                    crate::error::Error::Protocol("array offset length mismatch".into())
                })?;
                // Offsets are cumulative prefix sums: non-decreasing, and the
                // running maximum (the last offset) is the inner element row
                // count, capped at MAX_BLOCK_ROWS before the inner read.
                total = checked_monotonic_offset(total, u64::from_le_bytes(bytes), "array offset")?;
            }
            if total > 0 {
                let inner_state = match state {
                    RawColumnState::Array(inner_state) => inner_state.as_ref(),
                    _ => &RawColumnState::None,
                };
                Box::pin(read_column_body_raw_recorded(
                    stream,
                    inner,
                    inner_state,
                    total,
                    out,
                    budget,
                ))
                .await?;
            }
            Ok(())
        },
        Map(key, value) => {
            // Same bulk conversion as the Array arm: one recorded read of
            // rows*8 bytes, then a monotonicity scan of the recorded slice.
            let nbytes = checked_column_len(rows, 8, "map offset")?;
            let start = out.len();
            read_exact_recorded(stream, out, nbytes, budget).await?;
            let mut total = 0usize;
            for chunk in out[start..].chunks_exact(8) {
                let bytes: [u8; 8] = chunk.try_into().map_err(|_| {
                    crate::error::Error::Protocol("map offset length mismatch".into())
                })?;
                total = checked_monotonic_offset(total, u64::from_le_bytes(bytes), "map offset")?;
            }
            if total > 0 {
                let (key_state, value_state) = match state {
                    RawColumnState::Map(key_state, value_state) => {
                        (key_state.as_ref(), value_state.as_ref())
                    },
                    _ => (&RawColumnState::None, &RawColumnState::None),
                };
                Box::pin(read_column_body_raw_recorded(
                    stream, key, key_state, total, out, budget,
                ))
                .await?;
                Box::pin(read_column_body_raw_recorded(
                    stream,
                    value,
                    value_state,
                    total,
                    out,
                    budget,
                ))
                .await?;
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
                Box::pin(read_column_body_raw_recorded(
                    stream, elem, elem_state, rows, out, budget,
                ))
                .await?;
            }
            Ok(())
        },
        LowCardinality(inner) => {
            let inner_state = match state {
                RawColumnState::LowCardinality(inner_state) => inner_state.as_ref(),
                _ => &RawColumnState::None,
            };
            read_lc_body_raw_recorded(stream, inner, inner_state, rows, out, budget).await
        },
        JSON => {
            let json_state = match state {
                RawColumnState::Json(json_state) => json_state,
                _ => {
                    return Err(crate::error::Error::Protocol(
                        "missing JSON state prefix".into(),
                    ));
                },
            };
            read_json_body_raw_recorded(stream, json_state, rows, out, budget).await
        },
        Dynamic => {
            let dynamic_state = match state {
                RawColumnState::Dynamic(dynamic_state) => dynamic_state,
                _ => {
                    return Err(crate::error::Error::Protocol(
                        "missing Dynamic state prefix".into(),
                    ));
                },
            };
            read_dynamic_body_raw_recorded(stream, dynamic_state, rows, out, budget).await
        },
        Variant(types) => {
            let states = match state {
                RawColumnState::Variant(states) => states.as_slice(),
                _ => &[],
            };
            read_variant_body_raw_recorded(stream, types, states, rows, out, budget).await
        },
        Point => {
            Box::pin(read_column_body_raw_recorded(
                stream,
                &Float64,
                &RawColumnState::None,
                rows,
                out,
                budget,
            ))
            .await?;
            Box::pin(read_column_body_raw_recorded(
                stream,
                &Float64,
                &RawColumnState::None,
                rows,
                out,
                budget,
            ))
            .await
        },
        Ring => {
            Box::pin(read_column_body_raw_recorded(
                stream,
                &Array(Box::new(Point)),
                &RawColumnState::None,
                rows,
                out,
                budget,
            ))
            .await
        },
        Polygon => {
            Box::pin(read_column_body_raw_recorded(
                stream,
                &Array(Box::new(Ring)),
                &RawColumnState::None,
                rows,
                out,
                budget,
            ))
            .await
        },
        MultiPolygon => {
            Box::pin(read_column_body_raw_recorded(
                stream,
                &Array(Box::new(Polygon)),
                &RawColumnState::None,
                rows,
                out,
                budget,
            ))
            .await
        },
        String | Other(_) => {
            for _ in 0..rows {
                let len = checked_string_len(
                    read_varint_recorded(stream, out, budget).await?,
                    "string value length",
                )?;
                read_exact_recorded(stream, out, len, budget).await?;
            }
            Ok(())
        },
        FixedString(n) => {
            read_exact_recorded(
                stream,
                out,
                checked_column_len(rows, *n, "FixedString column")?,
                budget,
            )
            .await
        },
        AggregateFunction | SimpleAggregateFunction => Err(crate::error::Error::Protocol(format!(
            "query_raw does not support raw capture for type {ct}"
        ))),
        _ => {
            let width = ct
                .fixed_width()
                .ok_or_else(|| crate::error::Error::Protocol(format!("unknown type {ct}")))?;
            read_exact_recorded(
                stream,
                out,
                checked_column_len(rows, width, "fixed-width column")?,
                budget,
            )
            .await
        },
    }
}

async fn read_u64_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, out: &mut Vec<u8>, budget: &mut usize,
) -> Result<u64> {
    let start = out.len();
    read_exact_recorded(stream, out, 8, budget).await?;
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&out[start..start + 8]);
    Ok(u64::from_le_bytes(bytes))
}

async fn read_json_state_prefix_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, out: &mut Vec<u8>, budget: &mut usize,
) -> Result<JsonRawState> {
    let version = read_u64_recorded(stream, out, budget).await?;

    let mut dynamic_paths = Vec::new();
    match version {
        1 | 4 => {},
        3 => {
            let paths_count = checked_count(
                read_varint_recorded(stream, out, budget).await?,
                "JSON path",
                crate::limits::MAX_JSON_DYNAMIC_ITEMS,
            )?;
            for _ in 0..paths_count {
                let _path = read_string_recorded(stream, out, "JSON path length", budget).await?;
            }
            dynamic_paths.reserve(paths_count);
            for _ in 0..paths_count {
                dynamic_paths.push(read_dynamic_state_prefix_recorded(stream, out, budget).await?);
            }
        },
        0 => {
            let _max_dynamic_paths = read_varint_recorded(stream, out, budget).await?;
            let paths_count = checked_count(
                read_varint_recorded(stream, out, budget).await?,
                "JSON path",
                crate::limits::MAX_JSON_DYNAMIC_ITEMS,
            )?;
            for _ in 0..paths_count {
                let _path = read_string_recorded(stream, out, "JSON path length", budget).await?;
            }
            dynamic_paths.reserve(paths_count);
            for _ in 0..paths_count {
                dynamic_paths.push(read_dynamic_state_prefix_recorded(stream, out, budget).await?);
            }
        },
        other => {
            return Err(crate::error::Error::Protocol(format!(
                "unknown JSON serialization version {other}"
            )));
        },
    }
    Ok(JsonRawState {
        version,
        dynamic_paths,
    })
}

async fn read_json_body_raw_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, state: &JsonRawState, rows: usize, out: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    match state.version {
        1 | 4 => {
            for _ in 0..rows {
                let len = checked_string_len(
                    read_varint_recorded(stream, out, budget).await?,
                    "JSON string length",
                )?;
                read_exact_recorded(stream, out, len, budget).await?;
            }
        },
        3 => {
            for dynamic in &state.dynamic_paths {
                read_dynamic_body_raw_recorded(stream, dynamic, rows, out, budget).await?;
            }
        },
        0 => {
            for dynamic in &state.dynamic_paths {
                read_dynamic_body_raw_recorded(stream, dynamic, rows, out, budget).await?;
            }
            read_exact_recorded(
                stream,
                out,
                checked_column_len(rows, 8, "JSON offsets")?,
                budget,
            )
            .await?;
        },
        other => {
            return Err(crate::error::Error::Protocol(format!(
                "unknown JSON serialization version {other}"
            )));
        },
    }
    Ok(())
}

async fn read_dynamic_state_prefix_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, out: &mut Vec<u8>, budget: &mut usize,
) -> Result<DynamicRawState> {
    let version = read_u64_recorded(stream, out, budget).await?;
    let mut type_names = Vec::new();
    let mut type_states = Vec::new();
    match version {
        0 => {},
        1 => {
            let _max_types = read_varint_recorded(stream, out, budget).await?;
            type_names =
                read_dynamic_type_names_recorded(stream, out, "dynamic subcolumn types", budget)
                    .await?;
            let _variant_version = read_u64_recorded(stream, out, budget).await?;
        },
        2 | 3 => {
            type_names =
                read_dynamic_type_names_recorded(stream, out, "dynamic subcolumn types", budget)
                    .await?;
        },
        other => Err(crate::error::Error::Protocol(format!(
            "unknown Dynamic subcolumn serialization version {other}"
        )))?,
    }
    type_states.reserve(type_names.len());
    for type_name in &type_names {
        let ct = type_parser::parse_type(type_name).map_err(|e| {
            crate::error::Error::Protocol(format!("bad dynamic type '{type_name}': {e}"))
        })?;
        type_states
            .push(Box::pin(read_column_state_prefix_recorded(stream, &ct, out, budget)).await?);
    }
    Ok(DynamicRawState {
        version,
        type_names,
        type_states,
    })
}

async fn read_dynamic_body_raw_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, state: &DynamicRawState, rows: usize, out: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    match state.version {
        0 => Ok(()),
        1 => {
            read_deprecated_dynamic_values_recorded(
                stream,
                &state.type_names,
                &state.type_states,
                rows,
                out,
                budget,
            )
            .await
        },
        2 | 3 => {
            read_flattened_dynamic_values_recorded(
                stream,
                &state.type_names,
                &state.type_states,
                rows,
                out,
                budget,
            )
            .await
        },
        other => Err(crate::error::Error::Protocol(format!(
            "unknown Dynamic serialization version {other}"
        ))),
    }
}

async fn read_deprecated_dynamic_values_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, type_names: &[String], type_states: &[RawColumnState], rows: usize,
    out: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    if rows == 0 || type_names.is_empty() {
        return Ok(());
    }
    let start = out.len();
    read_exact_recorded(stream, out, rows, budget).await?;
    let mut counts = vec![0usize; type_names.len()];
    for &discriminator in &out[start..start + rows] {
        let idx = usize::from(discriminator);
        if idx < counts.len() {
            counts[idx] += 1;
        } else if discriminator != u8::MAX {
            return Err(crate::error::Error::Protocol(format!(
                "deprecated Dynamic discriminator {idx} exceeds type count {}",
                type_names.len()
            )));
        }
    }
    for (idx, (type_name, count)) in type_names.iter().zip(counts).enumerate() {
        if count > 0 {
            let ct = type_parser::parse_type(type_name).map_err(|e| {
                crate::error::Error::Protocol(format!("bad dynamic type '{type_name}': {e}"))
            })?;
            let state = type_states.get(idx).unwrap_or(&RawColumnState::None);
            Box::pin(read_column_body_raw_recorded(
                stream, &ct, state, count, out, budget,
            ))
            .await?;
        }
    }
    Ok(())
}

async fn read_dynamic_type_names_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, out: &mut Vec<u8>, count_name: &str, budget: &mut usize,
) -> Result<Vec<String>> {
    let type_count = checked_count(
        read_varint_recorded(stream, out, budget).await?,
        count_name,
        crate::limits::MAX_JSON_DYNAMIC_ITEMS,
    )?;
    let mut type_names = Vec::with_capacity(type_count);
    for _ in 0..type_count {
        type_names.push(read_string_recorded(stream, out, "dynamic type length", budget).await?);
    }
    Ok(type_names)
}

async fn read_flattened_dynamic_values_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, type_names: &[String], type_states: &[RawColumnState], rows: usize,
    out: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    if rows == 0 || type_names.is_empty() {
        return Ok(());
    }
    let width = dynamic_discriminator_width(type_names.len());
    let len = checked_column_len(rows, width, "Dynamic discriminators")?;
    let start = out.len();
    read_exact_recorded(stream, out, len, budget).await?;
    let mut counts = vec![0usize; type_names.len()];
    for chunk in out[start..start + len].chunks_exact(width) {
        let idx = decode_dynamic_discriminator(chunk)?;
        if idx < counts.len() {
            counts[idx] += 1;
        } else if idx != type_names.len() {
            return Err(crate::error::Error::Protocol(format!(
                "Dynamic discriminator {idx} exceeds type count {}",
                type_names.len()
            )));
        }
    }
    for (idx, (type_name, count)) in type_names.iter().zip(counts).enumerate() {
        if count > 0 {
            let ct = type_parser::parse_type(type_name).map_err(|e| {
                crate::error::Error::Protocol(format!("bad dynamic type '{type_name}': {e}"))
            })?;
            let state = type_states.get(idx).unwrap_or(&RawColumnState::None);
            Box::pin(read_column_body_raw_recorded(
                stream, &ct, state, count, out, budget,
            ))
            .await?;
        }
    }
    Ok(())
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
            crate::error::Error::Protocol("Dynamic discriminator length mismatch".into())
        })?),
        _ => {
            return Err(crate::error::Error::Protocol(
                "unsupported Dynamic discriminator width".into(),
            ));
        },
    };
    checked_usize(value, "Dynamic discriminator")
}

pub(super) async fn read_variant_body_raw_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, types: &[type_parser::ColumnType], type_states: &[RawColumnState], rows: usize,
    out: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    let type_names = types.iter().map(ToString::to_string).collect::<Vec<_>>();
    read_variant_types_body_raw_recorded(stream, &type_names, type_states, rows, out, false, budget)
        .await
}

async fn read_variant_types_body_raw_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, type_names: &[String], type_states: &[RawColumnState], rows: usize,
    out: &mut Vec<u8>, one_based_discriminators: bool, budget: &mut usize,
) -> Result<()> {
    let mode = read_u64_recorded(stream, out, budget).await?;
    if rows == 0 || type_names.is_empty() {
        return Ok(());
    }
    match mode {
        0 => {
            let start = out.len();
            read_exact_recorded(stream, out, rows, budget).await?;
            let mut counts = vec![0usize; type_names.len()];
            for &discriminator in &out[start..start + rows] {
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
            for (idx, (type_name, count)) in type_names.iter().zip(counts).enumerate() {
                if count > 0 {
                    let ct = type_parser::parse_type(type_name).map_err(|e| {
                        crate::error::Error::Protocol(format!(
                            "bad variant type '{type_name}': {e}"
                        ))
                    })?;
                    let state = type_states.get(idx).unwrap_or(&RawColumnState::None);
                    Box::pin(read_column_body_raw_recorded(
                        stream, &ct, state, count, out, budget,
                    ))
                    .await?;
                }
            }
            Ok(())
        },
        1 => {
            let discriminator = checked_usize(
                read_u64_recorded(stream, out, budget).await?,
                "Variant compact discriminator",
            )?;
            let discriminator = if one_based_discriminators {
                discriminator.saturating_sub(1)
            } else {
                discriminator
            };
            let compact_rows = checked_usize(
                read_u64_recorded(stream, out, budget).await?,
                "Variant compact rows",
            )?;
            // A compact granule carries one non-empty variant for at most the
            // outer row count (all-NULL granules legally carry zero rows).
            if compact_rows > rows {
                return Err(crate::error::Error::Protocol(format!(
                    "Variant compact rows {compact_rows} exceeds row count {rows}"
                )));
            }
            if discriminator < type_names.len() && compact_rows > 0 {
                let ct = type_parser::parse_type(&type_names[discriminator]).map_err(|e| {
                    crate::error::Error::Protocol(format!(
                        "bad variant type '{}': {e}",
                        type_names[discriminator]
                    ))
                })?;
                let state = type_states
                    .get(discriminator)
                    .unwrap_or(&RawColumnState::None);
                Box::pin(read_column_body_raw_recorded(
                    stream,
                    &ct,
                    state,
                    compact_rows,
                    out,
                    budget,
                ))
                .await?;
            }
            Ok(())
        },
        other => Err(crate::error::Error::Protocol(format!(
            "unknown Variant serialization mode {other}"
        ))),
    }
}

async fn read_lc_body_raw_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, inner: &type_parser::ColumnType, inner_state: &RawColumnState, rows: usize,
    out: &mut Vec<u8>, budget: &mut usize,
) -> Result<()> {
    let start = out.len();
    read_exact_recorded(stream, out, 24, budget).await?;
    let serial_type = u64::from_le_bytes(out[start + 8..start + 16].try_into().map_err(|_| {
        crate::error::Error::Protocol("LowCardinality metadata length mismatch".into())
    })?);
    let version = u64::from_le_bytes(out[start..start + 8].try_into().map_err(|_| {
        crate::error::Error::Protocol("LowCardinality version length mismatch".into())
    })?);
    let num_keys = checked_count(
        u64::from_le_bytes(out[start + 16..start + 24].try_into().map_err(|_| {
            crate::error::Error::Protocol("LowCardinality key count length mismatch".into())
        })?),
        "LowCardinality key",
        crate::limits::MAX_JSON_DYNAMIC_ITEMS,
    )?;
    let idx_width = lc_idx_width(version, serial_type)?;

    if num_keys > 0 {
        Box::pin(read_column_body_raw_recorded(
            stream,
            inner,
            inner_state,
            num_keys,
            out,
            budget,
        ))
        .await?;
    }
    let start = out.len();
    read_exact_recorded(stream, out, 8, budget).await?;
    let indexes = checked_usize(
        u64::from_le_bytes(out[start..start + 8].try_into().map_err(|_| {
            crate::error::Error::Protocol("LowCardinality index count length mismatch".into())
        })?),
        "LowCardinality indexes",
    )?;
    // The native format writes exactly one index per row of the granule; a
    // different count can only be a malformed or hostile payload.
    if indexes != rows {
        return Err(crate::error::Error::Protocol(format!(
            "LowCardinality index count {indexes} does not match row count {rows}"
        )));
    }
    read_exact_recorded(
        stream,
        out,
        checked_column_len(indexes, idx_width, "LowCardinality index")?,
        budget,
    )
    .await
}

#[cfg(test)]
mod count_limit_tests {
    use super::{
        read_dynamic_state_prefix_recorded, read_json_state_prefix_recorded, read_raw_data_block,
    };
    use crate::error::Error;
    use crate::protocol::wire;
    use crate::runtime::io::AsyncWriteExt as _;

    fn varint(v: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        wire::write_varint(&mut buf, v).expect("test write");
        buf
    }

    async fn seeded(bytes: Vec<u8>) -> (tokio::io::DuplexStream, tokio::io::DuplexStream) {
        let (mut tx, rx) = tokio::io::duplex(64);
        tx.write_all(&bytes).await.expect("seed bytes");
        (tx, rx)
    }

    #[tokio::test]
    async fn raw_block_column_count_u64_max_is_protocol_error() {
        let mut bytes = varint(0); // table name
        bytes.push(0x00); // BlockInfo terminator
        bytes.extend_from_slice(&varint(u64::MAX)); // columns
        let (_tx, mut rx) = seeded(bytes).await;
        let err = read_raw_data_block(&mut rx)
            .await
            .expect_err("u64::MAX column count must be rejected");
        match &err {
            Error::Protocol(msg) => assert_eq!(
                msg,
                "block column count 18446744073709551615 exceeds limit 65536"
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn raw_block_row_count_cap_plus_one_is_protocol_error() {
        let mut bytes = varint(0); // table name
        bytes.push(0x00); // BlockInfo terminator
        bytes.extend_from_slice(&varint(1)); // columns
        bytes.extend_from_slice(&varint(10_000_001)); // rows
        let (_tx, mut rx) = seeded(bytes).await;
        let err = read_raw_data_block(&mut rx)
            .await
            .expect_err("cap + 1 row count must be rejected");
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn json_state_v3_path_count_u64_max_is_protocol_error() {
        let mut bytes = 3u64.to_le_bytes().to_vec(); // serialization version 3
        bytes.extend_from_slice(&varint(u64::MAX)); // JSON path count
        let (_tx, mut rx) = seeded(bytes).await;
        let mut out = Vec::new();
        let mut budget = crate::limits::MAX_COLUMN_BYTES;
        let err = read_json_state_prefix_recorded(&mut rx, &mut out, &mut budget)
            .await
            .expect_err("u64::MAX JSON path count must be rejected");
        match &err {
            Error::Protocol(msg) => assert_eq!(
                msg,
                "JSON path count 18446744073709551615 exceeds limit 65536"
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn json_state_v0_path_count_cap_plus_one_is_protocol_error() {
        let mut bytes = 0u64.to_le_bytes().to_vec(); // serialization version 0
        bytes.extend_from_slice(&varint(0)); // max dynamic paths hint
        bytes.extend_from_slice(&varint(65_537)); // JSON path count
        let (_tx, mut rx) = seeded(bytes).await;
        let mut out = Vec::new();
        let mut budget = crate::limits::MAX_COLUMN_BYTES;
        let err = read_json_state_prefix_recorded(&mut rx, &mut out, &mut budget)
            .await
            .expect_err("cap + 1 JSON path count must be rejected");
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn dynamic_state_v1_type_count_u64_max_is_protocol_error() {
        // Version 1 prefix: version, max-types hint, then the type count that
        // previously fed Vec::with_capacity directly (capacity-overflow panic).
        let mut bytes = 1u64.to_le_bytes().to_vec(); // serialization version 1
        bytes.extend_from_slice(&varint(0)); // max types hint
        bytes.extend_from_slice(&varint(u64::MAX)); // dynamic type count
        let (_tx, mut rx) = seeded(bytes).await;
        let mut out = Vec::new();
        let mut budget = crate::limits::MAX_COLUMN_BYTES;
        let err = read_dynamic_state_prefix_recorded(&mut rx, &mut out, &mut budget)
            .await
            .expect_err("u64::MAX dynamic type count must be rejected");
        match &err {
            Error::Protocol(msg) => assert_eq!(
                msg,
                "dynamic subcolumn types count 18446744073709551615 exceeds limit 65536"
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn lc_state_key_count_u64_max_is_protocol_error() {
        // 24-byte LowCardinality prefix: version 1, serial type 0, then the
        // dictionary key count that previously sized reads unbounded.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes()); // key serialization version
        // serial type: "additional keys" flag (bit 9) required by the reader
        bytes.extend_from_slice(&(1u64 << 9).to_le_bytes());
        bytes.extend_from_slice(&u64::MAX.to_le_bytes()); // dictionary key count
        let (_tx, mut rx) = seeded(bytes).await;
        let mut out = Vec::new();
        let mut budget = crate::limits::MAX_COLUMN_BYTES;
        let err = super::read_lc_body_raw_recorded(
            &mut rx,
            &crate::protocol::type_parser::ColumnType::UInt8,
            &super::RawColumnState::None,
            1,
            &mut out,
            &mut budget,
        )
        .await
        .expect_err("u64::MAX LowCardinality key count must be rejected");
        match &err {
            Error::Protocol(msg) => assert_eq!(
                msg,
                "LowCardinality key count 18446744073709551615 exceeds limit 65536"
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dynamic_state_v2_type_count_cap_plus_one_is_protocol_error() {
        let mut bytes = 2u64.to_le_bytes().to_vec(); // serialization version 2
        bytes.extend_from_slice(&varint(65_537)); // dynamic type count
        let (_tx, mut rx) = seeded(bytes).await;
        let mut out = Vec::new();
        let mut budget = crate::limits::MAX_COLUMN_BYTES;
        let err = read_dynamic_state_prefix_recorded(&mut rx, &mut out, &mut budget)
            .await
            .expect_err("cap + 1 dynamic type count must be rejected");
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }
}

#[cfg(test)]
mod byte_limit_tests {
    use super::{read_column_raw_recorded, read_lc_body_raw_recorded};
    use crate::error::Error;
    use crate::limits::MAX_COLUMN_BYTES;
    use crate::protocol::type_parser::ColumnType;
    use crate::protocol::wire;
    use crate::runtime::io::AsyncWriteExt as _;

    fn varint(v: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        wire::write_varint(&mut buf, v).expect("test write");
        buf
    }

    async fn seeded(bytes: Vec<u8>) -> tokio::io::DuplexStream {
        let (mut tx, rx) = tokio::io::duplex(64);
        tx.write_all(&bytes).await.expect("seed bytes");
        rx
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

    #[tokio::test]
    async fn raw_string_value_length_u64_max_is_rejected_before_read() {
        // One String row claims u64::MAX bytes; no payload follows.
        let mut rx = seeded(varint(u64::MAX)).await;
        let mut out = Vec::new();
        let mut budget = MAX_COLUMN_BYTES;
        let err = read_column_raw_recorded(&mut rx, "String", 1, &mut out, &mut budget)
            .await
            .expect_err("lying string length must be rejected");
        assert_protocol(
            &err,
            "string value length 18446744073709551615 exceeds limit 16777215",
        );
    }

    #[tokio::test]
    async fn raw_fixed_width_cap_plus_one_is_rejected_before_allocation() {
        // UInt256 rows*32 = 64 MiB + 32 must fail before the arena is resized.
        let rows = MAX_COLUMN_BYTES / 32 + 1;
        let mut rx = seeded(Vec::new()).await;
        let mut out = Vec::new();
        let mut budget = MAX_COLUMN_BYTES;
        let err = read_column_raw_recorded(&mut rx, "UInt256", rows, &mut out, &mut budget)
            .await
            .expect_err("fixed-width cap + 1 must be rejected");
        assert_protocol(
            &err,
            "fixed-width column byte length 67108896 exceeds limit 67108864",
        );
    }

    #[tokio::test]
    async fn raw_fixed_string_cap_plus_one_is_rejected_before_allocation() {
        let rows = MAX_COLUMN_BYTES / 16 + 1;
        let mut rx = seeded(Vec::new()).await;
        let mut out = Vec::new();
        let mut budget = MAX_COLUMN_BYTES;
        let err = read_column_raw_recorded(&mut rx, "FixedString(16)", rows, &mut out, &mut budget)
            .await
            .expect_err("FixedString cap + 1 must be rejected");
        assert_protocol(
            &err,
            "FixedString column byte length 67108880 exceeds limit 67108864",
        );
    }

    #[tokio::test]
    async fn raw_array_offset_2_pow_60_is_rejected_before_inner_read() {
        // One Array row whose single 8-byte offset claims 2^60 inner elements.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(1u64 << 60).to_le_bytes());
        let mut rx = seeded(bytes).await;
        let mut out = Vec::new();
        let mut budget = MAX_COLUMN_BYTES;
        let err = read_column_raw_recorded(&mut rx, "Array(UInt8)", 1, &mut out, &mut budget)
            .await
            .expect_err("2^60 array offset must be rejected");
        assert_protocol(
            &err,
            "array offset total 1152921504606846976 exceeds limit 10000000",
        );
    }

    #[tokio::test]
    async fn raw_array_offset_decrease_is_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&9u64.to_le_bytes());
        bytes.extend_from_slice(&4u64.to_le_bytes());
        let mut rx = seeded(bytes).await;
        let mut out = Vec::new();
        let mut budget = MAX_COLUMN_BYTES;
        let err = read_column_raw_recorded(&mut rx, "Array(UInt8)", 2, &mut out, &mut budget)
            .await
            .expect_err("decreasing array offsets must be rejected");
        assert_protocol(&err, "array offset decreased from 9 to 4");
    }

    #[tokio::test]
    async fn raw_map_offset_2_pow_60_is_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(1u64 << 60).to_le_bytes());
        let mut rx = seeded(bytes).await;
        let mut out = Vec::new();
        let mut budget = MAX_COLUMN_BYTES;
        let err = read_column_raw_recorded(&mut rx, "Map(UInt8, UInt8)", 1, &mut out, &mut budget)
            .await
            .expect_err("2^60 map offset must be rejected");
        assert_protocol(
            &err,
            "map offset total 1152921504606846976 exceeds limit 10000000",
        );
    }

    #[tokio::test]
    async fn raw_low_cardinality_index_mismatch_is_rejected() {
        // Dictionary with 1 UInt8 key, then an index count of 5 for 1 row.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes()); // key serialization version
        bytes.extend_from_slice(&(1u64 << 9).to_le_bytes()); // additional keys, UInt8 indexes
        bytes.extend_from_slice(&1u64.to_le_bytes()); // dictionary keys
        bytes.push(7); // one key value
        bytes.extend_from_slice(&5u64.to_le_bytes()); // index count != rows
        let mut rx = seeded(bytes).await;
        let mut out = Vec::new();
        let mut budget = MAX_COLUMN_BYTES;
        let err = super::read_lc_body_raw_recorded(
            &mut rx,
            &ColumnType::UInt8,
            &super::RawColumnState::None,
            1,
            &mut out,
            &mut budget,
        )
        .await
        .expect_err("LowCardinality index mismatch must be rejected");
        assert_protocol(
            &err,
            "LowCardinality index count 5 does not match row count 1",
        );
    }

    #[tokio::test]
    async fn raw_low_cardinality_index_count_u64_max_is_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes()); // key serialization version
        bytes.extend_from_slice(&(1u64 << 9).to_le_bytes()); // additional keys, UInt8 indexes
        bytes.extend_from_slice(&0u64.to_le_bytes()); // no dictionary keys
        bytes.extend_from_slice(&u64::MAX.to_le_bytes()); // index count
        let mut rx = seeded(bytes).await;
        let mut out = Vec::new();
        let mut budget = MAX_COLUMN_BYTES;
        let err = read_lc_body_raw_recorded(
            &mut rx,
            &ColumnType::UInt8,
            &super::RawColumnState::None,
            1,
            &mut out,
            &mut budget,
        )
        .await
        .expect_err("u64::MAX LowCardinality index count must be rejected");
        assert_protocol(&err, "LowCardinality index count 18446744073709551615");
    }

    #[tokio::test]
    async fn raw_variant_compact_rows_claim_is_rejected() {
        // Compact mode: discriminator 0 (UInt8), compact rows = 2^60 for 1 row.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes()); // mode = COMPACT
        bytes.extend_from_slice(&0u64.to_le_bytes()); // discriminator
        bytes.extend_from_slice(&(1u64 << 60).to_le_bytes()); // compact rows claim
        let mut rx = seeded(bytes).await;
        let mut out = Vec::new();
        let mut budget = MAX_COLUMN_BYTES;
        let err =
            read_column_raw_recorded(&mut rx, "Variant(UInt8, String)", 1, &mut out, &mut budget)
                .await
                .expect_err("huge Variant compact rows claim must be rejected");
        assert_protocol(
            &err,
            "Variant compact rows 1152921504606846976 exceeds row count 1",
        );
    }

    #[tokio::test]
    async fn raw_variant_compact_rows_equal_to_rows_parse() {
        // Boundary: compact rows == rows with real payload stays readable.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes()); // mode = COMPACT
        bytes.extend_from_slice(&0u64.to_le_bytes()); // discriminator (UInt8)
        bytes.extend_from_slice(&2u64.to_le_bytes()); // compact rows == rows
        bytes.extend_from_slice(&[7, 9]); // two UInt8 values
        let mut rx = seeded(bytes).await;
        let mut out = Vec::new();
        let mut budget = MAX_COLUMN_BYTES;
        read_column_raw_recorded(&mut rx, "Variant(UInt8, String)", 2, &mut out, &mut budget)
            .await
            .expect("compact rows == rows parses");
        assert_eq!(&out[out.len() - 2..], &[7, 9]);
    }

    #[tokio::test]
    async fn raw_column_budget_rejects_cumulative_claims() {
        // Three delivered 3-byte values against an 8-byte budget: the third
        // value's claim must exhaust the per-column budget and fail with a
        // deterministic protocol error instead of growing the arena. (With
        // the production 64 MiB budget the same check fires on the claim
        // that crosses the cap, before that value is read.)
        let mut bytes = Vec::new();
        for _ in 0..3 {
            bytes.extend_from_slice(&varint(3));
            bytes.extend_from_slice(b"abc");
        }
        let mut rx = seeded(bytes).await;
        let mut out = Vec::new();
        let mut budget = 8usize;
        let err = read_column_raw_recorded(&mut rx, "String", 3, &mut out, &mut budget)
            .await
            .expect_err("cumulative claims past the column budget must be rejected");
        assert_protocol(&err, "raw column cumulative byte length exceeds limit");
    }
}
