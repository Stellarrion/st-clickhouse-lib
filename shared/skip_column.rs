// Slice-based skipping of native-protocol column data.
//
// Both engines parse buffered (decompressed) blocks by *skipping* each
// column's wire bytes and slicing them out. This module is the single
// implementation of that framing, shared by the async decompressed-block
// parser and the sync buffered-block parser. It mirrors the streaming raw
// readers (`raw_block_reader.rs`, `read_column_data_into`) and the
// materialized readers (`read_column_async`) exactly:
//
// - Array/Map offsets are fixed-width little-endian `UInt64`, one per outer
// row (cumulative prefix sums; the last offset is the inner row count) —
// never varints. The inner column is skipped for `rows = last offset`
// rows, which is zero when every array is empty.
// - A materialized JSON column is an 8-byte little-endian
// string-serialization version (1 or 4) followed by `rows`
// varint-prefixed strings.
// - LowCardinality is a 24-byte header, the dictionary column (`num_keys`
// rows), an 8-byte index count equal to the row count, and the index
// bytes.
// - Variant/Dynamic columns carry per-subcolumn state prefixes that must be
// consumed before the discriminators and subcolumns.
//
// One deliberate asymmetry matches the materialized stream readers: nested
// JSON inside Array/Map/Tuple/Nullable/LowCardinality is skipped with the
// materialized (version 1/4 string) layout because that is what
// `read_column_async` produces, while JSON nested inside Variant/Dynamic
// follows the recorded raw layout with its shared dynamic-path state.

// Shared skip helpers expect Error and Result (from the engine error
// module) in the including module scope.
use super::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING;
use super::type_parser::{ColumnType, parse_type};
use super::wire::{parse_bytes, parse_string_bytes, parse_varint};

/// Whether a column type contains JSON anywhere below the top level.
///
/// Buffered (decompressed) block parsers slice column data with the shape the
/// materialized decoders expect: a top-level JSON column strips its 8-byte
/// string-serialization version, but JSON nested inside Array/Map/Tuple/
/// Nullable keeps the version byte inside the slice and would decode
/// silently wrong — callers reject such columns instead.
pub(crate) fn contains_nested_json(ct: &ColumnType) -> bool {
    nested_json_below(ct, false)
}

/// `is_nested` distinguishes a JSON at the top level (allowed — its version
/// prefix is stripped from the slice) from JSON reached through
/// Array/Map/Tuple/Nullable/Variant (rejected — the version stays inside the
/// slice and decoders misread it). Dynamic is allowed: its subcolumns are
/// carried by the skip-state machine, whose JSON subcolumn state follows the
/// raw layout, not the materialized one.
fn nested_json_below(ct: &ColumnType, is_nested: bool) -> bool {
    use ColumnType::*;
    match ct {
        JSON => is_nested,
        Nullable(inner) | Array(inner) | LowCardinality(inner) => {
            nested_json_below(inner, true)
        },
        Map(k, v) => nested_json_below(k, true) || nested_json_below(v, true),
        Tuple(elems) => elems.iter().any(|e| nested_json_below(e, true)),
        Variant(types) => types.iter().any(|t| nested_json_below(t, true)),
        Dynamic => false,
        _ => false,
    }
}

/// Skip one column's data in a fully buffered block, addressing the column by
/// type name. A type string the parser rejects is skipped as `rows`
/// varint-prefixed strings, matching the unknown-type fallback of the
/// materialized stream readers.
pub(crate) fn skip_column_data_by_name(
    buf: &[u8], pos: &mut usize, type_name: &str, rows: usize,
) -> Result<()> {
    match parse_type(type_name) {
        Ok(ct) => skip_column_data(buf, pos, &ct, rows),
        Err(_) => skip_string_rows(buf, pos, rows, "string value length"),
    }
}

/// Skip one column's data in a fully buffered block.
///
/// Consumes exactly the bytes the materialized stream readers consume for the
/// same column, so the buffer slice between the start and end positions has
/// the layout the column decoders expect.
pub(crate) fn skip_column_data(buf: &[u8], pos: &mut usize, ct: &ColumnType, rows: usize) -> Result<()> {
    if rows == 0 {
        return Ok(());
    }

    use ColumnType::*;
    match ct {
        UInt8 | Int8 | Bool | Enum8 => advance_pos(buf, pos, rows)?,
        UInt16 | Int16 | Date | Enum16 => advance_pos(buf, pos, checked_len(rows, 2)?)?,
        UInt32 | Int32 | Float32 | Date32 | DateTime | Time | IPv4 => {
            advance_pos(buf, pos, checked_len(rows, 4)?)?
        },
        UInt64 | Int64 | Float64 | DateTime64(_) | Time64(_) => {
            advance_pos(buf, pos, checked_len(rows, 8)?)?
        },
        UInt128 | Int128 | UUID | IPv6 => advance_pos(buf, pos, checked_len(rows, 16)?)?,
        UInt256 | Int256 => advance_pos(buf, pos, checked_len(rows, 32)?)?,
        FixedString(n) => advance_pos(buf, pos, checked_len(rows, *n)?)?,
        Decimal(1..=9, _) => advance_pos(buf, pos, checked_len(rows, 4)?)?,
        Decimal(10..=18, _) => advance_pos(buf, pos, checked_len(rows, 8)?)?,
        Decimal(19..=38, _) => advance_pos(buf, pos, checked_len(rows, 16)?)?,
        Decimal(39..=76, _) => advance_pos(buf, pos, checked_len(rows, 32)?)?,
        Decimal(precision, _) => {
            return Err(Error::Protocol(format!(
                "unsupported Decimal precision {precision}"
            )));
        },
        Nothing => advance_pos(buf, pos, rows)?,
        String => skip_string_rows(buf, pos, rows, "string value length")?,
        JSON => skip_materialized_json(buf, pos, rows)?,
        Nullable(inner) => {
            advance_pos(buf, pos, rows)?;
            skip_column_data(buf, pos, inner, rows)?;
        },
        Array(inner) => {
            let total = skip_offsets(buf, pos, rows, "array offset")?;
            if total > 0 {
                skip_column_data(buf, pos, inner, total)?;
            }
        },
        Map(key, value) => {
            let total = skip_offsets(buf, pos, rows, "map offset")?;
            if total > 0 {
                skip_column_data(buf, pos, key, total)?;
                skip_column_data(buf, pos, value, total)?;
            }
        },
        Tuple(elems) => {
            for elem in elems {
                skip_column_data(buf, pos, elem, rows)?;
            }
        },
        Point => {
            skip_column_data(buf, pos, &ColumnType::Float64, rows)?;
            skip_column_data(buf, pos, &ColumnType::Float64, rows)?;
        },
        Ring => skip_column_data(buf, pos, &ColumnType::Array(Box::new(ColumnType::Point)), rows)?,
        Polygon => {
            skip_column_data(buf, pos, &ColumnType::Array(Box::new(ColumnType::Ring)), rows)?
        },
        MultiPolygon => {
            skip_column_data(buf, pos, &ColumnType::Array(Box::new(ColumnType::Polygon)), rows)?
        },
        LowCardinality(inner) => skip_lc_column(buf, pos, inner, rows, &SkipState::None)?,
        Variant(types) => {
            let mut states = Vec::with_capacity(types.len());
            for typ in types {
                states.push(skip_state_prefix(buf, pos, typ)?);
            }
            skip_variant_body(buf, pos, types, &states, rows)?;
        },
        Dynamic => {
            let state = skip_dynamic_state(buf, pos)?;
            skip_dynamic_body(buf, pos, &state, rows)?;
        },
        AggregateFunction | SimpleAggregateFunction => {
            // The materialized stream readers reject these columns outright;
            // skipping them as opaque bytes would silently misframe every
            // later column in the block.
            return Err(Error::Protocol(format!(
                "{ct} columns are not supported in buffered block reads"
            )));
        },
        Other(_) => skip_string_rows(buf, pos, rows, "string value length")?,
    }
    Ok(())
}

/// Per-subcolumn state consumed before a Variant/Dynamic body, mirroring
/// `RawColumnState` in the raw readers.
enum SkipState {
    None,
    Nullable(Box<SkipState>),
    Array(Box<SkipState>),
    Map(Box<SkipState>, Box<SkipState>),
    Tuple(Vec<SkipState>),
    LowCardinality(Box<SkipState>),
    Variant(Vec<SkipState>),
    Dynamic(DynamicSkipState),
    Json(JsonSkipState),
}

/// Dynamic subcolumn state: serialization version, subcolumn type names, and
/// the per-subcolumn states already consumed from the wire.
struct DynamicSkipState {
    version: u64,
    type_names: Vec<String>,
    type_states: Vec<SkipState>,
}

/// JSON shared state: string serialization version and the per-path Dynamic
/// states for the dynamic layouts (versions 0 and 3).
struct JsonSkipState {
    version: u64,
    dynamic_paths: Vec<DynamicSkipState>,
}

/// Skip the state prefix of one subcolumn, mirroring
/// `read_column_state_prefix_recorded`.
fn skip_state_prefix(buf: &[u8], pos: &mut usize, ct: &ColumnType) -> Result<SkipState> {
    use ColumnType::*;
    match ct {
        Nullable(inner) => Ok(SkipState::Nullable(Box::new(skip_state_prefix(
            buf, pos, inner,
        )?))),
        Array(inner) => Ok(SkipState::Array(Box::new(skip_state_prefix(
            buf, pos, inner,
        )?))),
        Map(key, value) => Ok(SkipState::Map(
            Box::new(skip_state_prefix(buf, pos, key)?),
            Box::new(skip_state_prefix(buf, pos, value)?),
        )),
        Tuple(elems) => {
            let mut states = Vec::with_capacity(elems.len());
            for elem in elems {
                states.push(skip_state_prefix(buf, pos, elem)?);
            }
            Ok(SkipState::Tuple(states))
        },
        LowCardinality(inner) => Ok(SkipState::LowCardinality(Box::new(
            skip_state_prefix(buf, pos, inner)?,
        ))),
        Variant(types) => {
            let mut states = Vec::with_capacity(types.len());
            for typ in types {
                states.push(skip_state_prefix(buf, pos, typ)?);
            }
            Ok(SkipState::Variant(states))
        },
        Dynamic => skip_dynamic_state(buf, pos).map(SkipState::Dynamic),
        JSON => skip_json_state(buf, pos).map(SkipState::Json),
        _ => Ok(SkipState::None),
    }
}

/// Skip the body of one subcolumn whose state prefix was already consumed,
/// mirroring `read_column_body_raw_recorded`.
fn skip_body_raw(
    buf: &[u8], pos: &mut usize, ct: &ColumnType, state: &SkipState, rows: usize,
) -> Result<()> {
    if rows == 0 {
        return Ok(());
    }

    use ColumnType::*;
    match ct {
        Nullable(inner) => {
            advance_pos(buf, pos, rows)?;
            let inner_state = match state {
                SkipState::Nullable(inner_state) => inner_state.as_ref(),
                _ => &SkipState::None,
            };
            skip_body_raw(buf, pos, inner, inner_state, rows)
        },
        Array(inner) => {
            let total = skip_offsets(buf, pos, rows, "array offset")?;
            if total == 0 {
                return Ok(());
            }
            let inner_state = match state {
                SkipState::Array(inner_state) => inner_state.as_ref(),
                _ => &SkipState::None,
            };
            skip_body_raw(buf, pos, inner, inner_state, total)
        },
        Map(key, value) => {
            let total = skip_offsets(buf, pos, rows, "map offset")?;
            if total == 0 {
                return Ok(());
            }
            let (key_state, value_state) = match state {
                SkipState::Map(key_state, value_state) => {
                    (key_state.as_ref(), value_state.as_ref())
                },
                _ => (&SkipState::None, &SkipState::None),
            };
            skip_body_raw(buf, pos, key, key_state, total)?;
            skip_body_raw(buf, pos, value, value_state, total)
        },
        Tuple(elems) => {
            let states = match state {
                SkipState::Tuple(states) => states.as_slice(),
                _ => &[],
            };
            for (idx, elem) in elems.iter().enumerate() {
                let elem_state = states.get(idx).unwrap_or(&SkipState::None);
                skip_body_raw(buf, pos, elem, elem_state, rows)?;
            }
            Ok(())
        },
        LowCardinality(inner) => {
            let inner_state = match state {
                SkipState::LowCardinality(inner_state) => inner_state.as_ref(),
                _ => &SkipState::None,
            };
            skip_lc_column(buf, pos, inner, rows, inner_state)
        },
        JSON => {
            let json_state = match state {
                SkipState::Json(json_state) => json_state,
                _ => {
                    return Err(Error::Protocol(
                        "missing JSON state prefix".into(),
                    ));
                },
            };
            skip_json_body_raw(buf, pos, json_state, rows)
        },
        Dynamic => {
            let dynamic_state = match state {
                SkipState::Dynamic(dynamic_state) => dynamic_state,
                _ => {
                    return Err(Error::Protocol(
                        "missing Dynamic state prefix".into(),
                    ));
                },
            };
            skip_dynamic_body(buf, pos, dynamic_state, rows)
        },
        Variant(types) => {
            let states = match state {
                SkipState::Variant(states) => states.as_slice(),
                _ => &[],
            };
            skip_variant_body(buf, pos, types, states, rows)
        },
        Point => {
            skip_body_raw(buf, pos, &ColumnType::Float64, &SkipState::None, rows)?;
            skip_body_raw(buf, pos, &ColumnType::Float64, &SkipState::None, rows)
        },
        Ring => skip_body_raw(
            buf,
            pos,
            &ColumnType::Array(Box::new(ColumnType::Point)),
            &SkipState::None,
            rows,
        ),
        Polygon => skip_body_raw(
            buf,
            pos,
            &ColumnType::Array(Box::new(ColumnType::Ring)),
            &SkipState::None,
            rows,
        ),
        MultiPolygon => skip_body_raw(
            buf,
            pos,
            &ColumnType::Array(Box::new(ColumnType::Polygon)),
            &SkipState::None,
            rows,
        ),
        // Simple columns have no state prefix, so their raw body is the
        // materialized layout.
        _ => skip_column_data(buf, pos, ct, rows),
    }
}

/// A materialized JSON column: 8-byte string-serialization version (1 or 4)
/// followed by `rows` varint-prefixed strings. The version bytes are framing,
/// not column data — the materialized readers strip them.
fn skip_materialized_json(buf: &[u8], pos: &mut usize, rows: usize) -> Result<()> {
    let version = read_u64_le(buf, pos, "JSON version")?;
    if version != 1 && version != 4 {
        return Err(Error::Protocol(format!(
            "materialized JSON reads require string serialization version 1 or 4, got {version}; \
             enable {OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING}=1 or use query_raw"
        )));
    }
    skip_string_rows(buf, pos, rows, "JSON string length")
}

/// Raw JSON state prefix, mirroring `read_json_state_prefix_recorded`:
/// version u64; versions 0 and 3 carry shared dynamic-path state.
fn skip_json_state(buf: &[u8], pos: &mut usize) -> Result<JsonSkipState> {
    let version = read_u64_le(buf, pos, "JSON serialization version")?;
    let mut dynamic_paths = Vec::new();
    match version {
        1 | 4 => {},
        3 => {
            let paths_count = checked_count(
                parse_varint(buf, pos)?,
                "JSON path",
                crate::limits::MAX_JSON_DYNAMIC_ITEMS,
            )?;
            for _ in 0..paths_count {
                parse_string_bytes(buf, pos)?;
            }
            dynamic_paths.reserve(paths_count);
            for _ in 0..paths_count {
                dynamic_paths.push(skip_dynamic_state(buf, pos)?);
            }
        },
        0 => {
            let _max_dynamic_paths = parse_varint(buf, pos)?;
            let paths_count = checked_count(
                parse_varint(buf, pos)?,
                "JSON path",
                crate::limits::MAX_JSON_DYNAMIC_ITEMS,
            )?;
            for _ in 0..paths_count {
                parse_string_bytes(buf, pos)?;
            }
            dynamic_paths.reserve(paths_count);
            for _ in 0..paths_count {
                dynamic_paths.push(skip_dynamic_state(buf, pos)?);
            }
        },
        other => {
            return Err(Error::Protocol(format!(
                "unknown JSON serialization version {other}"
            )));
        },
    }
    Ok(JsonSkipState {
        version,
        dynamic_paths,
    })
}

/// Raw JSON body, mirroring `read_json_body_raw_recorded`.
fn skip_json_body_raw(
    buf: &[u8], pos: &mut usize, state: &JsonSkipState, rows: usize,
) -> Result<()> {
    match state.version {
        1 | 4 => skip_string_rows(buf, pos, rows, "JSON string length"),
        3 => {
            for dynamic in &state.dynamic_paths {
                skip_dynamic_body(buf, pos, dynamic, rows)?;
            }
            Ok(())
        },
        0 => {
            for dynamic in &state.dynamic_paths {
                skip_dynamic_body(buf, pos, dynamic, rows)?;
            }
            advance_pos(buf, pos, checked_len(rows, 8)?)
        },
        other => Err(Error::Protocol(format!(
            "unknown JSON serialization version {other}"
        ))),
    }
}

/// Dynamic state prefix, mirroring `read_dynamic_state_prefix_recorded`.
fn skip_dynamic_state(buf: &[u8], pos: &mut usize) -> Result<DynamicSkipState> {
    let version = read_u64_le(buf, pos, "Dynamic serialization version")?;
    let type_names = match version {
        0 => Vec::new(),
        1 => {
            let _max_types = parse_varint(buf, pos)?;
            let type_names = read_type_names(buf, pos, "dynamic subcolumn types")?;
            let _variant_version = read_u64_le(buf, pos, "Dynamic variant version")?;
            type_names
        },
        2 | 3 => read_type_names(buf, pos, "dynamic subcolumn types")?,
        other => {
            return Err(Error::Protocol(format!(
                "unknown Dynamic subcolumn serialization version {other}"
            )));
        },
    };
    let mut type_states = Vec::with_capacity(type_names.len());
    for type_name in &type_names {
        let ct = parse_type(type_name)
            .map_err(|e| Error::Protocol(format!("bad dynamic type '{type_name}': {e}")))?;
        type_states.push(skip_state_prefix(buf, pos, &ct)?);
    }
    Ok(DynamicSkipState {
        version,
        type_names,
        type_states,
    })
}

/// Dynamic body, mirroring `read_dynamic_body_raw_recorded`.
fn skip_dynamic_body(
    buf: &[u8], pos: &mut usize, state: &DynamicSkipState, rows: usize,
) -> Result<()> {
    if rows == 0 || state.type_names.is_empty() {
        return Ok(());
    }
    match state.version {
        0 => Ok(()),
        1 => {
            // Deprecated layout: 1-byte discriminators, u8::MAX marks NULL.
            let start = *pos;
            advance_pos(buf, pos, rows)?;
            let mut counts = vec![0usize; state.type_names.len()];
            for &discriminator in &buf[start..start + rows] {
                let idx = usize::from(discriminator);
                if idx < counts.len() {
                    counts[idx] += 1;
                } else if discriminator != u8::MAX {
                    return Err(Error::Protocol(format!(
                        "deprecated Dynamic discriminator {idx} exceeds type count {}",
                        state.type_names.len()
                    )));
                }
            }
            skip_counted_subcolumns(buf, pos, state, &counts)
        },
        2 | 3 => {
            // Flattened layout: fixed-width discriminators, the type count
            // itself marks NULL.
            let width = dynamic_discriminator_width(state.type_names.len());
            let start = *pos;
            advance_pos(buf, pos, checked_len(rows, width)?)?;
            let mut counts = vec![0usize; state.type_names.len()];
            for chunk in buf[start..*pos].chunks_exact(width) {
                let idx = decode_dynamic_discriminator(chunk)?;
                if idx < counts.len() {
                    counts[idx] += 1;
                } else if idx != state.type_names.len() {
                    return Err(Error::Protocol(format!(
                        "Dynamic discriminator {idx} exceeds type count {}",
                        state.type_names.len()
                    )));
                }
            }
            skip_counted_subcolumns(buf, pos, state, &counts)
        },
        other => Err(Error::Protocol(format!(
            "unknown Dynamic serialization version {other}"
        ))),
    }
}

/// Skip the per-type subcolumns of a Dynamic body, in type order, each with
/// the row count observed in the discriminator scan.
fn skip_counted_subcolumns(
    buf: &[u8], pos: &mut usize, state: &DynamicSkipState, counts: &[usize],
) -> Result<()> {
    for (idx, (type_name, count)) in state.type_names.iter().zip(counts).enumerate() {
        if *count == 0 {
            continue;
        }
        let ct = parse_type(type_name)
            .map_err(|e| Error::Protocol(format!("bad dynamic type '{type_name}': {e}")))?;
        let sub_state = state.type_states.get(idx).unwrap_or(&SkipState::None);
        skip_body_raw(buf, pos, &ct, sub_state, *count)?;
    }
    Ok(())
}

/// Variant body, mirroring `read_variant_types_body_raw_recorded` with
/// zero-based discriminators: mode u64; mode 0 = per-row discriminators plus
/// subcolumns by observed count; mode 1 = one compact granule.
fn skip_variant_body(
    buf: &[u8], pos: &mut usize, types: &[ColumnType], states: &[SkipState], rows: usize,
) -> Result<()> {
    let mode = read_u64_le(buf, pos, "Variant serialization mode")?;
    if rows == 0 || types.is_empty() {
        return Ok(());
    }
    match mode {
        0 => {
            let start = *pos;
            advance_pos(buf, pos, rows)?;
            let mut counts = vec![0usize; types.len()];
            for &discriminator in &buf[start..start + rows] {
                let idx = usize::from(discriminator);
                if idx < counts.len() {
                    counts[idx] += 1;
                }
            }
            for (idx, (typ, count)) in types.iter().zip(counts).enumerate() {
                if count > 0 {
                    let state = states.get(idx).unwrap_or(&SkipState::None);
                    skip_body_raw(buf, pos, typ, state, count)?;
                }
            }
            Ok(())
        },
        1 => {
            let discriminator = checked_usize(
                read_u64_le(buf, pos, "Variant compact discriminator")?,
                "Variant compact discriminator",
            )?;
            let compact_rows = checked_usize(
                read_u64_le(buf, pos, "Variant compact rows")?,
                "Variant compact rows",
            )?;
            // A compact granule carries one non-empty variant for at most the
            // outer row count (all-NULL granules legally carry zero rows).
            if compact_rows > rows {
                return Err(Error::Protocol(format!(
                    "Variant compact rows {compact_rows} exceeds row count {rows}"
                )));
            }
            if discriminator < types.len() && compact_rows > 0 {
                let state = states.get(discriminator).unwrap_or(&SkipState::None);
                skip_body_raw(buf, pos, &types[discriminator], state, compact_rows)?;
            }
            Ok(())
        },
        other => Err(Error::Protocol(format!(
            "unknown Variant serialization mode {other}"
        ))),
    }
}

/// A LowCardinality column: 24-byte header, dictionary (`num_keys` inner
/// rows), 8-byte index count (must equal the row count), index bytes.
fn skip_lc_column(
    buf: &[u8], pos: &mut usize, inner: &ColumnType, rows: usize, inner_state: &SkipState,
) -> Result<()> {
    let meta = parse_bytes(buf, pos, 24)?;
    let version = u64::from_le_bytes(
        meta[0..8]
            .try_into()
            .map_err(|_| Error::Protocol("LowCardinality version length mismatch".into()))?,
    );
    let serial_type = u64::from_le_bytes(
        meta[8..16]
            .try_into()
            .map_err(|_| Error::Protocol("LowCardinality metadata length mismatch".into()))?,
    );
    let idx_width = lc_idx_width(version, serial_type)?;
    let num_keys = checked_count(
        u64::from_le_bytes(
            meta[16..24].try_into().map_err(|_| {
                Error::Protocol("LowCardinality key count length mismatch".into())
            })?,
        ),
        "LowCardinality key",
        crate::limits::MAX_JSON_DYNAMIC_ITEMS,
    )?;
    if num_keys > 0 {
        skip_body_raw(buf, pos, inner, inner_state, num_keys)?;
    }
    let count_bytes = parse_bytes(buf, pos, 8)?;
    let indexes = checked_usize(
        u64::from_le_bytes(
            count_bytes
                .try_into()
                .map_err(|_| Error::Protocol("LowCardinality index count length mismatch".into()))?,
        ),
        "LowCardinality indexes",
    )?;
    // The native format writes exactly one index per row of the granule; a
    // different count can only be a malformed or hostile payload.
    if indexes != rows {
        return Err(Error::Protocol(format!(
            "LowCardinality index count {indexes} does not match row count {rows}"
        )));
    }
    advance_pos(buf, pos, checked_len(indexes, idx_width)?)
}

/// Read the varint-counted type-name list of a Dynamic state prefix.
fn read_type_names(buf: &[u8], pos: &mut usize, count_name: &str) -> Result<Vec<String>> {
    let type_count = checked_count(
        parse_varint(buf, pos)?,
        count_name,
        crate::limits::MAX_JSON_DYNAMIC_ITEMS,
    )?;
    let mut type_names = Vec::with_capacity(type_count);
    for _ in 0..type_count {
        let bytes = parse_string_bytes(buf, pos)?;
        let name = std::str::from_utf8(bytes)
            .map_err(|e| Error::Protocol(format!("dynamic type name utf8: {e}")))?;
        type_names.push(name.to_owned());
    }
    Ok(type_names)
}

/// Skip `rows` Array/Map offsets (`rows` little-endian u64s), validating that
/// they are non-decreasing cumulative prefix sums and returning the last
/// offset — the inner element row count.
fn skip_offsets(buf: &[u8], pos: &mut usize, rows: usize, name: &str) -> Result<usize> {
    let off_end = (*pos)
        .checked_add(checked_len(rows, 8)?)
        .ok_or_else(|| Error::Protocol("buffer position overflow".into()))?;
    if off_end > buf.len() {
        return Err(Error::Protocol(
            "unexpected end of buffer parsing array offsets".into(),
        ));
    }
    let mut total = 0usize;
    for chunk in buf[*pos..off_end].chunks_exact(8) {
        let mut offset_bytes = [0u8; 8];
        offset_bytes.copy_from_slice(chunk);
        total = checked_monotonic_offset(total, u64::from_le_bytes(offset_bytes), name)?;
    }
    *pos = off_end;
    Ok(total)
}

/// Skip `rows` varint-prefixed strings.
fn skip_string_rows(buf: &[u8], pos: &mut usize, rows: usize, length_name: &str) -> Result<()> {
    for _ in 0..rows {
        let len = checked_string_len(parse_varint(buf, pos)?, length_name)?;
        advance_pos(buf, pos, len)?;
    }
    Ok(())
}

/// Read one little-endian u64, advancing the position by 8.
fn read_u64_le(buf: &[u8], pos: &mut usize, name: &str) -> Result<u64> {
    let bytes = parse_bytes(buf, pos, 8)?;
    Ok(u64::from_le_bytes(
        bytes
            .try_into()
            .map_err(|_| Error::Protocol(format!("{name} length mismatch")))?,
    ))
}

fn advance_pos(buf: &[u8], pos: &mut usize, len: usize) -> Result<()> {
    let end = (*pos)
        .checked_add(len)
        .ok_or_else(|| Error::Protocol("buffer position overflow".into()))?;
    if end > buf.len() {
        return Err(Error::Protocol(
            "unexpected end of buffer skipping column data".into(),
        ));
    }
    *pos = end;
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
            Error::Protocol("Dynamic discriminator length mismatch".into())
        })?),
        _ => {
            return Err(Error::Protocol(
                "unsupported Dynamic discriminator width".into(),
            ));
        },
    };
    checked_usize(value, "Dynamic discriminator")
}

fn checked_len(rows: usize, width: usize) -> Result<usize> {
    crate::limits::checked_column_len(rows, width, "column byte length").map_err(Error::Protocol)
}

fn checked_string_len(value: u64, what: &str) -> Result<usize> {
    crate::limits::checked_string_len(value, what).map_err(Error::Protocol)
}

fn checked_count(value: u64, what: &str, max: usize) -> Result<usize> {
    crate::limits::checked_count(value, what, max).map_err(Error::Protocol)
}

fn checked_usize(value: u64, name: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| Error::Protocol(format!("{name} count too large")))
}

fn checked_monotonic_offset(prev: usize, value: u64, what: &str) -> Result<usize> {
    crate::limits::checked_monotonic_offset(prev, value, what).map_err(Error::Protocol)
}

/// Validate a LowCardinality header and derive the per-row index width.
///
/// The 24-byte header carries a `version` (must be 1) and a `serial_type`
/// whose low 2 bits are the index width shift and whose bits 8/9 carry the
/// "global dictionaries" (unsupported) and "additional keys" (required) flags.
fn lc_idx_width(version: u64, serial_type: u64) -> Result<usize> {
    if version != 1 {
        return Err(Error::Protocol(format!(
            "unsupported LowCardinality key serialization version {version}"
        )));
    }
    if (serial_type & (1u64 << 8)) != 0 {
        return Err(Error::Protocol(
            "LowCardinality global dictionaries are not supported".into(),
        ));
    }
    if (serial_type & (1u64 << 9)) == 0 {
        return Err(Error::Protocol(
            "LowCardinality additional keys flag is missing".into(),
        ));
    }
    Ok(1usize << (serial_type & 0x3))
}
