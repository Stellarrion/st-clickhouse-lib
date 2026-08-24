use crate::connection::io::{checked_count, checked_len, checked_usize, lc_idx_width};
use crate::error::Result;
use crate::protocol::block::RawBlock;
use crate::protocol::type_parser;
use crate::runtime::io::AsyncReadExt;

pub(super) async fn read_raw_data_block<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<RawBlock> {
    let mut raw = Vec::with_capacity(1024);
    let table_name = read_string_recorded(stream, &mut Vec::new(), "table name length").await?;

    loop {
        let dim = read_varint_recorded(stream, &mut raw).await?;
        match dim {
            0 => break,
            1 => read_exact_recorded(stream, &mut raw, 1).await?,
            2 => read_exact_recorded(stream, &mut raw, 4).await?,
            3 => {
                let _ = read_varint_recorded(stream, &mut raw).await?;
            },
            _ => {
                return Err(crate::error::Error::Protocol(format!(
                    "unknown BlockInfo field {dim}"
                )));
            },
        }
    }

    let columns = checked_count(
        read_varint_recorded(stream, &mut raw).await?,
        "block column",
        crate::limits::MAX_BLOCK_COLUMNS,
    )?;
    let rows = checked_count(
        read_varint_recorded(stream, &mut raw).await?,
        "block row",
        crate::limits::MAX_BLOCK_ROWS,
    )?;

    for _ in 0..columns {
        let _name = read_string_recorded(stream, &mut raw, "column name length").await?;
        let type_name = read_string_recorded(stream, &mut raw, "column type length").await?;
        read_exact_recorded(stream, &mut raw, 1).await?;
        if rows > 0 {
            read_column_raw_recorded(stream, &type_name, rows, &mut raw).await?;
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
    stream: &mut S, out: &mut Vec<u8>,
) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0;
    loop {
        if shift >= 64 {
            return Err(crate::error::Error::Protocol("varint overflow".into()));
        }
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await?;
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
    stream: &mut S, out: &mut Vec<u8>, length_name: &str,
) -> Result<String> {
    let len = checked_usize(read_varint_recorded(stream, out).await?, length_name)?;
    let start = out.len();
    read_exact_recorded(stream, out, len).await?;
    String::from_utf8(out[start..].to_vec())
        .map_err(|e| crate::error::Error::Protocol(format!("utf8 in {length_name}: {e}")))
}

async fn read_exact_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, out: &mut Vec<u8>, len: usize,
) -> Result<()> {
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
    stream: &mut S, type_name: &str, rows: usize, out: &mut Vec<u8>,
) -> Result<()> {
    let ct = type_parser::parse_type(type_name)
        .map_err(|e| crate::error::Error::Protocol(format!("bad type '{type_name}': {e}")))?;
    let state = Box::pin(read_column_state_prefix_recorded(stream, &ct, out)).await?;
    Box::pin(read_column_body_raw_recorded(
        stream, &ct, &state, rows, out,
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
    stream: &mut S, ct: &type_parser::ColumnType, out: &mut Vec<u8>,
) -> Result<RawColumnState> {
    use crate::protocol::type_parser::ColumnType::*;
    match ct {
        Nullable(inner) => Ok(RawColumnState::Nullable(Box::new(
            Box::pin(read_column_state_prefix_recorded(stream, inner, out)).await?,
        ))),
        Array(inner) => Ok(RawColumnState::Array(Box::new(
            Box::pin(read_column_state_prefix_recorded(stream, inner, out)).await?,
        ))),
        Map(key, value) => Ok(RawColumnState::Map(
            Box::new(Box::pin(read_column_state_prefix_recorded(stream, key, out)).await?),
            Box::new(Box::pin(read_column_state_prefix_recorded(stream, value, out)).await?),
        )),
        Tuple(elems) => {
            let mut states = Vec::with_capacity(elems.len());
            for elem in elems {
                states.push(Box::pin(read_column_state_prefix_recorded(stream, elem, out)).await?);
            }
            Ok(RawColumnState::Tuple(states))
        },
        LowCardinality(inner) => Ok(RawColumnState::LowCardinality(Box::new(
            Box::pin(read_column_state_prefix_recorded(stream, inner, out)).await?,
        ))),
        Variant(types) => {
            let mut states = Vec::with_capacity(types.len());
            for typ in types {
                states.push(Box::pin(read_column_state_prefix_recorded(stream, typ, out)).await?);
            }
            Ok(RawColumnState::Variant(states))
        },
        Dynamic => read_dynamic_state_prefix_recorded(stream, out)
            .await
            .map(RawColumnState::Dynamic),
        JSON => read_json_state_prefix_recorded(stream, out)
            .await
            .map(RawColumnState::Json),
        _ => Ok(RawColumnState::None),
    }
}

async fn read_column_body_raw_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, ct: &type_parser::ColumnType, state: &RawColumnState, rows: usize,
    out: &mut Vec<u8>,
) -> Result<()> {
    if rows == 0 {
        return Ok(());
    }

    use crate::protocol::type_parser::ColumnType::*;

    match &ct {
        Nullable(inner) => {
            read_exact_recorded(stream, out, rows).await?;
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
            ))
            .await
        },
        Array(inner) => {
            let mut total = 0usize;
            for _ in 0..rows {
                let start = out.len();
                read_exact_recorded(stream, out, 8).await?;
                let bytes: [u8; 8] = out[start..start + 8].try_into().map_err(|_| {
                    crate::error::Error::Protocol("array offset length mismatch".into())
                })?;
                let value = checked_usize(u64::from_le_bytes(bytes), "array offset")?;
                total = total.max(value);
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
                ))
                .await?;
            }
            Ok(())
        },
        Map(key, value) => {
            let mut total = 0usize;
            for _ in 0..rows {
                let start = out.len();
                read_exact_recorded(stream, out, 8).await?;
                let bytes: [u8; 8] = out[start..start + 8].try_into().map_err(|_| {
                    crate::error::Error::Protocol("map offset length mismatch".into())
                })?;
                let offset = checked_usize(u64::from_le_bytes(bytes), "map offset")?;
                total = total.max(offset);
            }
            if total > 0 {
                let (key_state, value_state) = match state {
                    RawColumnState::Map(key_state, value_state) => {
                        (key_state.as_ref(), value_state.as_ref())
                    },
                    _ => (&RawColumnState::None, &RawColumnState::None),
                };
                Box::pin(read_column_body_raw_recorded(
                    stream, key, key_state, total, out,
                ))
                .await?;
                Box::pin(read_column_body_raw_recorded(
                    stream,
                    value,
                    value_state,
                    total,
                    out,
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
                    stream, elem, elem_state, rows, out,
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
            read_lc_body_raw_recorded(stream, inner, inner_state, out).await
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
            read_json_body_raw_recorded(stream, json_state, rows, out).await
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
            read_dynamic_body_raw_recorded(stream, dynamic_state, rows, out).await
        },
        Variant(types) => {
            let states = match state {
                RawColumnState::Variant(states) => states.as_slice(),
                _ => &[],
            };
            read_variant_body_raw_recorded(stream, types, states, rows, out).await
        },
        Point => {
            Box::pin(read_column_body_raw_recorded(
                stream,
                &Float64,
                &RawColumnState::None,
                rows,
                out,
            ))
            .await?;
            Box::pin(read_column_body_raw_recorded(
                stream,
                &Float64,
                &RawColumnState::None,
                rows,
                out,
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
            ))
            .await
        },
        String | Other(_) => {
            for _ in 0..rows {
                let len = checked_usize(
                    read_varint_recorded(stream, out).await?,
                    "string value length",
                )?;
                read_exact_recorded(stream, out, len).await?;
            }
            Ok(())
        },
        FixedString(n) => read_exact_recorded(stream, out, checked_len(rows, *n)?).await,
        AggregateFunction | SimpleAggregateFunction => Err(crate::error::Error::Protocol(format!(
            "query_raw does not support raw capture for type {ct}"
        ))),
        _ => {
            let width = ct
                .fixed_width()
                .ok_or_else(|| crate::error::Error::Protocol(format!("unknown type {ct}")))?;
            read_exact_recorded(stream, out, checked_len(rows, width)?).await
        },
    }
}

async fn read_u64_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, out: &mut Vec<u8>,
) -> Result<u64> {
    let start = out.len();
    read_exact_recorded(stream, out, 8).await?;
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&out[start..start + 8]);
    Ok(u64::from_le_bytes(bytes))
}

async fn read_json_state_prefix_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, out: &mut Vec<u8>,
) -> Result<JsonRawState> {
    let version = read_u64_recorded(stream, out).await?;

    let mut dynamic_paths = Vec::new();
    match version {
        1 | 4 => {},
        3 => {
            let paths_count = checked_count(
                read_varint_recorded(stream, out).await?,
                "JSON path",
                crate::limits::MAX_JSON_DYNAMIC_ITEMS,
            )?;
            for _ in 0..paths_count {
                let _path = read_string_recorded(stream, out, "JSON path length").await?;
            }
            dynamic_paths.reserve(paths_count);
            for _ in 0..paths_count {
                dynamic_paths.push(read_dynamic_state_prefix_recorded(stream, out).await?);
            }
        },
        0 => {
            let _max_dynamic_paths = read_varint_recorded(stream, out).await?;
            let paths_count = checked_count(
                read_varint_recorded(stream, out).await?,
                "JSON path",
                crate::limits::MAX_JSON_DYNAMIC_ITEMS,
            )?;
            for _ in 0..paths_count {
                let _path = read_string_recorded(stream, out, "JSON path length").await?;
            }
            dynamic_paths.reserve(paths_count);
            for _ in 0..paths_count {
                dynamic_paths.push(read_dynamic_state_prefix_recorded(stream, out).await?);
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
    stream: &mut S, state: &JsonRawState, rows: usize, out: &mut Vec<u8>,
) -> Result<()> {
    match state.version {
        1 | 4 => {
            for _ in 0..rows {
                let len = checked_usize(
                    read_varint_recorded(stream, out).await?,
                    "JSON string length",
                )?;
                read_exact_recorded(stream, out, len).await?;
            }
        },
        3 => {
            for dynamic in &state.dynamic_paths {
                read_dynamic_body_raw_recorded(stream, dynamic, rows, out).await?;
            }
        },
        0 => {
            for dynamic in &state.dynamic_paths {
                read_dynamic_body_raw_recorded(stream, dynamic, rows, out).await?;
            }
            read_exact_recorded(stream, out, checked_len(rows, 8)?).await?;
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
    stream: &mut S, out: &mut Vec<u8>,
) -> Result<DynamicRawState> {
    let version = read_u64_recorded(stream, out).await?;
    let mut type_names = Vec::new();
    let mut type_states = Vec::new();
    match version {
        0 => {},
        1 => {
            let _max_types = read_varint_recorded(stream, out).await?;
            type_names =
                read_dynamic_type_names_recorded(stream, out, "dynamic subcolumn types").await?;
            let _variant_version = read_u64_recorded(stream, out).await?;
        },
        2 | 3 => {
            type_names =
                read_dynamic_type_names_recorded(stream, out, "dynamic subcolumn types").await?;
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
        type_states.push(Box::pin(read_column_state_prefix_recorded(stream, &ct, out)).await?);
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
    stream: &mut S, state: &DynamicRawState, rows: usize, out: &mut Vec<u8>,
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
    out: &mut Vec<u8>,
) -> Result<()> {
    if rows == 0 || type_names.is_empty() {
        return Ok(());
    }
    let start = out.len();
    read_exact_recorded(stream, out, rows).await?;
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
                stream, &ct, state, count, out,
            ))
            .await?;
        }
    }
    Ok(())
}

async fn read_dynamic_type_names_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, out: &mut Vec<u8>, count_name: &str,
) -> Result<Vec<String>> {
    let type_count = checked_count(
        read_varint_recorded(stream, out).await?,
        count_name,
        crate::limits::MAX_JSON_DYNAMIC_ITEMS,
    )?;
    let mut type_names = Vec::with_capacity(type_count);
    for _ in 0..type_count {
        type_names.push(read_string_recorded(stream, out, "dynamic type length").await?);
    }
    Ok(type_names)
}

async fn read_flattened_dynamic_values_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, type_names: &[String], type_states: &[RawColumnState], rows: usize,
    out: &mut Vec<u8>,
) -> Result<()> {
    if rows == 0 || type_names.is_empty() {
        return Ok(());
    }
    let width = dynamic_discriminator_width(type_names.len());
    let len = checked_len(rows, width)?;
    let start = out.len();
    read_exact_recorded(stream, out, len).await?;
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
                stream, &ct, state, count, out,
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
    out: &mut Vec<u8>,
) -> Result<()> {
    let type_names = types.iter().map(ToString::to_string).collect::<Vec<_>>();
    read_variant_types_body_raw_recorded(stream, &type_names, type_states, rows, out, false).await
}

async fn read_variant_types_body_raw_recorded<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, type_names: &[String], type_states: &[RawColumnState], rows: usize,
    out: &mut Vec<u8>, one_based_discriminators: bool,
) -> Result<()> {
    let mode = read_u64_recorded(stream, out).await?;
    if rows == 0 || type_names.is_empty() {
        return Ok(());
    }
    match mode {
        0 => {
            let start = out.len();
            read_exact_recorded(stream, out, rows).await?;
            let mut counts = vec![0usize; type_names.len()];
            for &discriminator in &out[start..start + rows] {
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
            for (idx, (type_name, count)) in type_names.iter().zip(counts).enumerate() {
                if count > 0 {
                    let ct = type_parser::parse_type(type_name).map_err(|e| {
                        crate::error::Error::Protocol(format!(
                            "bad variant type '{type_name}': {e}"
                        ))
                    })?;
                    let state = type_states.get(idx).unwrap_or(&RawColumnState::None);
                    Box::pin(read_column_body_raw_recorded(
                        stream, &ct, state, count, out,
                    ))
                    .await?;
                }
            }
            Ok(())
        },
        1 => {
            let discriminator = checked_usize(
                read_u64_recorded(stream, out).await?,
                "Variant compact discriminator",
            )?;
            let discriminator = if one_based_discriminators {
                discriminator.saturating_sub(1)
            } else {
                discriminator
            };
            let compact_rows = checked_usize(
                read_u64_recorded(stream, out).await?,
                "Variant compact rows",
            )?;
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
    stream: &mut S, inner: &type_parser::ColumnType, inner_state: &RawColumnState,
    out: &mut Vec<u8>,
) -> Result<()> {
    let start = out.len();
    read_exact_recorded(stream, out, 24).await?;
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
        ))
        .await?;
    }
    let start = out.len();
    read_exact_recorded(stream, out, 8).await?;
    let indexes = checked_usize(
        u64::from_le_bytes(out[start..start + 8].try_into().map_err(|_| {
            crate::error::Error::Protocol("LowCardinality index count length mismatch".into())
        })?),
        "LowCardinality indexes",
    )?;
    read_exact_recorded(stream, out, checked_len(indexes, idx_width)?).await
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
        let err = read_json_state_prefix_recorded(&mut rx, &mut out)
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
        let err = read_json_state_prefix_recorded(&mut rx, &mut out)
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
        let err = read_dynamic_state_prefix_recorded(&mut rx, &mut out)
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
        let err = super::read_lc_body_raw_recorded(
            &mut rx,
            &crate::protocol::type_parser::ColumnType::UInt8,
            &super::RawColumnState::None,
            &mut out,
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
        let err = read_dynamic_state_prefix_recorded(&mut rx, &mut out)
            .await
            .expect_err("cap + 1 dynamic type count must be rejected");
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }
}
