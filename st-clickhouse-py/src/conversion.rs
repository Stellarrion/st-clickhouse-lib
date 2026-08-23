//! Rust ↔ Python value conversion for ClickHouse types.
//!
//! Converts between column data and Python objects with full type awareness.
//! Date → datetime.date, DateTime → datetime.datetime, UUID → uuid string, etc.

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList, PyString, PyTuple};
use pyo3::{IntoPyObjectExt, Python};

use st_clickhouse::sync::column::OwnedColumnData;
use st_clickhouse::sync::error::{Error, Result};
use st_clickhouse::sync::protocol::block::{Block, ColumnInfo};
use st_clickhouse::sync::protocol::type_parser::{ColumnType, parse_type};

use crate::errors::to_py_err;

// ══════════════════════════════════════════════════════════════════════════
// Block → list[dict] (row-oriented, fully typed)
// ══════════════════════════════════════════════════════════════════════════

/// Convert all blocks into a flat list of row dicts with automatic type conversion.
pub fn blocks_to_py_dicts(blocks: &[Block], py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }

    let mut all_rows: Vec<Py<PyAny>> = Vec::with_capacity(total_materialized_rows(blocks));

    for block in blocks {
        let col_count = block.column_count();
        if col_count == 0 || block.row_count() == 0 {
            continue;
        }

        let mut decoded_cols: Vec<(Py<PyAny>, Vec<Py<PyAny>>)> = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let info = &block.columns[i];
            let values = column_to_py_list_typed(block, i, info, py).map_err(to_py_err)?;
            decoded_cols.push((PyString::new(py, &info.name).into(), values));
        }

        for row_idx in 0..block.row_count() {
            let row = PyDict::new(py);
            for (key, values) in &decoded_cols {
                row.set_item(key, &values[row_idx])?;
            }
            all_rows.push(row.into());
        }
    }

    Ok(all_rows)
}

/// Convert all blocks into a flat list of row tuples with automatic type conversion.
pub fn blocks_to_py_tuples(blocks: &[Block], py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }

    let mut all_rows: Vec<Py<PyAny>> = Vec::with_capacity(total_materialized_rows(blocks));

    for block in blocks {
        let col_count = block.column_count();
        if col_count == 0 || block.row_count() == 0 {
            continue;
        }

        let mut decoded_cols: Vec<Vec<Py<PyAny>>> = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let info = &block.columns[i];
            decoded_cols.push(column_to_py_list_typed(block, i, info, py).map_err(to_py_err)?);
        }

        for row_idx in 0..block.row_count() {
            all_rows.push(
                PyTuple::new(
                    py,
                    decoded_cols
                        .iter()
                        .map(|column| column[row_idx].clone_ref(py)),
                )?
                .into(),
            );
        }
    }

    Ok(all_rows)
}

/// Convert all blocks into a single `{column_name: list[values]}` mapping.
pub fn blocks_to_py_column_map(blocks: &[Block], py: Python<'_>) -> PyResult<Py<PyAny>> {
    let out = PyDict::new(py);
    let Some(first) = blocks.iter().find(|block| block.column_count() > 0) else {
        return Ok(out.into());
    };

    let col_count = first.column_count();
    let non_empty_blocks = blocks
        .iter()
        .filter(|block| block.column_count() > 0 && block.row_count() > 0)
        .count();
    if non_empty_blocks == 1 {
        let Some(block) = blocks
            .iter()
            .find(|block| block.column_count() > 0 && block.row_count() > 0)
        else {
            return Ok(out.into());
        };
        if block.column_count() != col_count {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "inconsistent column count across blocks",
            ));
        }
        for i in 0..col_count {
            let info = &block.columns[i];
            out.set_item(
                &info.name,
                column_to_py_list_object_typed(block, info, py).map_err(to_py_err)?,
            )?;
        }
        return Ok(out.into());
    }

    let total_rows = blocks.iter().map(Block::row_count).sum::<usize>();
    let mut columns: Vec<(String, Vec<Py<PyAny>>)> = first
        .columns
        .iter()
        .map(|info| (info.name.clone(), Vec::with_capacity(total_rows)))
        .collect();

    for block in blocks {
        if block.row_count() == 0 {
            continue;
        }
        if block.column_count() != col_count {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "inconsistent column count across blocks",
            ));
        }
        for (i, (_, values)) in columns.iter_mut().enumerate() {
            let info = &block.columns[i];
            let mut block_values =
                column_to_py_list_typed(block, i, info, py).map_err(to_py_err)?;
            values.append(&mut block_values);
        }
    }

    for (name, values) in columns {
        out.set_item(name, PyList::new(py, values)?)?;
    }

    Ok(out.into())
}

/// Convert blocks to column-oriented dicts.
pub fn blocks_to_py_columns(blocks: &[Block], py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
    let mut result = Vec::with_capacity(blocks.len());
    for block in blocks {
        let block_dict = PyDict::new(py);
        block_dict.set_item("rows", block.row_count())?;

        let cols_list = PyList::empty(py);
        for i in 0..block.column_count() {
            let info = &block.columns[i];
            let col_dict = PyDict::new(py);
            col_dict.set_item("name", &info.name)?;
            col_dict.set_item("type", &info.type_name)?;
            col_dict.set_item(
                "data",
                column_to_py_list_object_typed(block, info, py).map_err(to_py_err)?,
            )?;
            cols_list.append(col_dict)?;
        }
        block_dict.set_item("columns", cols_list)?;
        result.push(block_dict.into());
    }
    Ok(result)
}

// ══════════════════════════════════════════════════════════════════════════
// Type-aware column → Vec<Py<PyAny>>
// ══════════════════════════════════════════════════════════════════════════

pub(crate) fn column_to_py_list_typed(
    block: &Block, _col_idx: usize, info: &ColumnInfo, py: Python<'_>,
) -> Result<Vec<Py<PyAny>>> {
    let ct = parse_type(&info.type_name)
        .map_err(|e| Error::Protocol(format!("bad type '{}': {e}", info.type_name)))?;
    let mut pos = 0usize;
    match &ct {
        ColumnType::LowCardinality(inner) if !info.lc_materialized.is_empty() => {
            decode_column_to_py(
                info.lc_materialized.as_ref(),
                &mut pos,
                inner,
                block.row_count(),
                py,
            )
        },
        _ => decode_column_to_py(info.data.as_ref(), &mut pos, &ct, block.row_count(), py),
    }
}

pub(crate) fn column_to_py_list_object_typed(
    block: &Block, info: &ColumnInfo, py: Python<'_>,
) -> Result<Py<PyAny>> {
    let ct = parse_type(&info.type_name)
        .map_err(|e| Error::Protocol(format!("bad type '{}': {e}", info.type_name)))?;
    let mut pos = 0usize;
    match &ct {
        ColumnType::LowCardinality(inner) if !info.lc_materialized.is_empty() => {
            decode_column_to_py_list_object(
                info.lc_materialized.as_ref(),
                &mut pos,
                inner,
                block.row_count(),
                py,
            )
        },
        _ => decode_column_to_py_list_object(
            info.data.as_ref(),
            &mut pos,
            &ct,
            block.row_count(),
            py,
        ),
    }
}

fn owned_to_py_list_typed(
    owned: &OwnedColumnData, type_name: &str, py: Python<'_>,
) -> Result<Vec<Py<PyAny>>> {
    match owned {
        OwnedColumnData::UInt(values) => values
            .iter()
            .map(|&v| v.into_py_any(py).map_err(py_protocol_err))
            .collect(),
        OwnedColumnData::Int(values) => int_column_to_py(values, type_name, py),
        OwnedColumnData::Float(values) => float_column_to_py(values, type_name, py),
        OwnedColumnData::String(values) => string_column_to_py(values, type_name, py),
        OwnedColumnData::Bool(values) => values
            .iter()
            .map(|&v| v.into_py_any(py).map_err(py_protocol_err))
            .collect(),
        OwnedColumnData::Null(n) => Ok((0..*n).map(|_| py.None()).collect()),
        OwnedColumnData::Unknown => Ok(Vec::new()),
    }
}

// ── Int column dispatch ──

fn int_column_to_py(values: &[i64], type_name: &str, py: Python<'_>) -> Result<Vec<Py<PyAny>>> {
    let base = peel_wrappers(type_name);
    let ct = parse_type(base).unwrap_or(ColumnType::Other(base.to_string()));

    match &ct {
        ColumnType::Date | ColumnType::Date32 => Ok(values
            .iter()
            .map(|&days| date_from_days(days, py))
            .collect()),
        ColumnType::DateTime => Ok(values
            .iter()
            .map(|&ts| datetime_from_timestamp(ts, 0, py))
            .collect()),
        ColumnType::DateTime64(scale) => Ok(values
            .iter()
            .map(|&raw| datetime_from_timestamp(raw, *scale, py))
            .collect()),
        ColumnType::UUID => Ok(values
            .iter()
            .map(|&raw| {
                let hex = format!("{:032x}", u128::from(u64::from_ne_bytes(raw.to_ne_bytes())));
                let uuid_s = format!(
                    "{}-{}-{}-{}-{}",
                    &hex[0..8],
                    &hex[8..12],
                    &hex[12..16],
                    &hex[16..20],
                    &hex[20..32]
                );
                PyString::new(py, &uuid_s).into()
            })
            .collect()),
        ColumnType::IPv4 => Ok(values
            .iter()
            .map(|&raw| {
                let ip_s = format!(
                    "{}.{}.{}.{}",
                    (raw >> 24) & 0xFF,
                    (raw >> 16) & 0xFF,
                    (raw >> 8) & 0xFF,
                    raw & 0xFF
                );
                PyString::new(py, &ip_s).into()
            })
            .collect()),
        ColumnType::IPv6 => Ok(values
            .iter()
            .map(|&raw| {
                let hex = format!("{:016x}", u64::from_ne_bytes(raw.to_ne_bytes()));
                let padded = format!("{:0>32}", hex);
                let groups: Vec<String> = padded
                    .as_bytes()
                    .chunks(4)
                    .map(|c| std::string::String::from_utf8_lossy(c).to_string())
                    .collect();
                PyString::new(py, &groups.join(":")).into()
            })
            .collect()),
        _ => values
            .iter()
            .map(|&v| v.into_py_any(py).map_err(py_protocol_err))
            .collect(),
    }
}

// ── Float column dispatch ──

fn float_column_to_py(values: &[f64], _type_name: &str, py: Python<'_>) -> Result<Vec<Py<PyAny>>> {
    values
        .iter()
        .map(|&v| v.into_py_any(py).map_err(py_protocol_err))
        .collect()
}

// ── String column dispatch ──

fn string_column_to_py(
    values: &[String], _type_name: &str, py: Python<'_>,
) -> Result<Vec<Py<PyAny>>> {
    Ok(values.iter().map(|s| PyString::new(py, s).into()).collect())
}

fn decode_column_to_py(
    data: &[u8], pos: &mut usize, ct: &ColumnType, rows: usize, py: Python<'_>,
) -> Result<Vec<Py<PyAny>>> {
    use ColumnType::*;
    let mut out = Vec::with_capacity(rows);
    match ct {
        UInt8 => {
            for &v in parse_exact(data, pos, rows)? {
                out.push(v.into_py_any(py).map_err(py_protocol_err)?);
            }
        },
        Int8 | Enum8 => {
            for &v in parse_exact(data, pos, rows)? {
                out.push((v as i8).into_py_any(py).map_err(py_protocol_err)?);
            }
        },
        Bool => {
            for &v in parse_exact(data, pos, rows)? {
                out.push((v != 0).into_py_any(py).map_err(py_protocol_err)?);
            }
        },
        UInt16 | Date => {
            for chunk in parse_exact(data, pos, checked_len(rows, 2)?)?.chunks_exact(2) {
                let v = u16::from_le_bytes([chunk[0], chunk[1]]);
                if matches!(ct, Date) {
                    out.push(date_from_days(i64::from(v), py));
                } else {
                    out.push(v.into_py_any(py).map_err(py_protocol_err)?);
                }
            }
        },
        Int16 | Enum16 => {
            for chunk in parse_exact(data, pos, checked_len(rows, 2)?)?.chunks_exact(2) {
                out.push(
                    i16::from_le_bytes([chunk[0], chunk[1]])
                        .into_py_any(py)
                        .map_err(py_protocol_err)?,
                );
            }
        },
        UInt32 | IPv4 => {
            for chunk in parse_exact(data, pos, checked_len(rows, 4)?)?.chunks_exact(4) {
                let v = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                if matches!(ct, IPv4) {
                    out.push(
                        PyString::new(
                            py,
                            &format!(
                                "{}.{}.{}.{}",
                                (v >> 24) & 0xFF,
                                (v >> 16) & 0xFF,
                                (v >> 8) & 0xFF,
                                v & 0xFF
                            ),
                        )
                        .into(),
                    );
                } else {
                    out.push(v.into_py_any(py).map_err(py_protocol_err)?);
                }
            }
        },
        Int32 | Date32 | Time => {
            for chunk in parse_exact(data, pos, checked_len(rows, 4)?)?.chunks_exact(4) {
                let v = i32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                if matches!(ct, Date32) {
                    out.push(date_from_days(i64::from(v), py));
                } else {
                    out.push(v.into_py_any(py).map_err(py_protocol_err)?);
                }
            }
        },
        Float32 => {
            for chunk in parse_exact(data, pos, checked_len(rows, 4)?)?.chunks_exact(4) {
                out.push(
                    f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])
                        .into_py_any(py)
                        .map_err(py_protocol_err)?,
                );
            }
        },
        UInt64 => {
            for chunk in parse_exact(data, pos, checked_len(rows, 8)?)?.chunks_exact(8) {
                out.push(
                    u64::from_le_bytes(
                        chunk
                            .try_into()
                            .map_err(|_| Error::Protocol("UInt64 chunk length mismatch".into()))?,
                    )
                    .into_py_any(py)
                    .map_err(py_protocol_err)?,
                );
            }
        },
        Int64 => {
            for chunk in parse_exact(data, pos, checked_len(rows, 8)?)?.chunks_exact(8) {
                out.push(
                    i64::from_le_bytes(
                        chunk
                            .try_into()
                            .map_err(|_| Error::Protocol("Int64 chunk length mismatch".into()))?,
                    )
                    .into_py_any(py)
                    .map_err(py_protocol_err)?,
                );
            }
        },
        Float64 => {
            for chunk in parse_exact(data, pos, checked_len(rows, 8)?)?.chunks_exact(8) {
                out.push(
                    f64::from_le_bytes(
                        chunk
                            .try_into()
                            .map_err(|_| Error::Protocol("Float64 chunk length mismatch".into()))?,
                    )
                    .into_py_any(py)
                    .map_err(py_protocol_err)?,
                );
            }
        },
        DateTime => {
            for chunk in parse_exact(data, pos, checked_len(rows, 4)?)?.chunks_exact(4) {
                let v = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                out.push(datetime_from_timestamp(i64::from(v), 0, py));
            }
        },
        DateTime64(scale) | Time64(scale) => {
            for chunk in parse_exact(data, pos, checked_len(rows, 8)?)?.chunks_exact(8) {
                let v = i64::from_le_bytes(
                    chunk
                        .try_into()
                        .map_err(|_| Error::Protocol("DateTime64 chunk length mismatch".into()))?,
                );
                if matches!(ct, DateTime64(_)) {
                    out.push(datetime_from_timestamp(v, *scale, py));
                } else {
                    out.push(v.into_py_any(py).map_err(py_protocol_err)?);
                }
            }
        },
        Decimal(1..=9, _) => decode_i32_raw(data, pos, rows, py, &mut out)?,
        Decimal(10..=18, _) => decode_i64_raw(data, pos, rows, py, &mut out)?,
        Decimal(19..=38, _) | Int128 | UInt128 => {
            for chunk in parse_exact(data, pos, checked_len(rows, 16)?)?.chunks_exact(16) {
                let v = u128::from_le_bytes(
                    chunk
                        .try_into()
                        .map_err(|_| Error::Protocol("128-bit chunk length mismatch".into()))?,
                );
                out.push(v.to_string().into_py_any(py).map_err(py_protocol_err)?);
            }
        },
        Decimal(39..=76, _) | Int256 | UInt256 => {
            for chunk in parse_exact(data, pos, checked_len(rows, 32)?)?.chunks_exact(32) {
                out.push(
                    format!("0x{}", hex_lower(chunk))
                        .into_py_any(py)
                        .map_err(py_protocol_err)?,
                );
            }
        },
        UUID => {
            for chunk in parse_exact(data, pos, checked_len(rows, 16)?)?.chunks_exact(16) {
                let hex = hex_lower(chunk);
                out.push(
                    PyString::new(
                        py,
                        &format!(
                            "{}-{}-{}-{}-{}",
                            &hex[0..8],
                            &hex[8..12],
                            &hex[12..16],
                            &hex[16..20],
                            &hex[20..32]
                        ),
                    )
                    .into(),
                );
            }
        },
        IPv6 => {
            for chunk in parse_exact(data, pos, checked_len(rows, 16)?)?.chunks_exact(16) {
                let hex = hex_lower(chunk);
                let groups = hex
                    .as_bytes()
                    .chunks(4)
                    .map(|c| std::string::String::from_utf8_lossy(c).to_string())
                    .collect::<Vec<_>>();
                out.push(PyString::new(py, &groups.join(":")).into());
            }
        },
        String | JSON => {
            for _ in 0..rows {
                out.push(PyString::new(py, &parse_string(data, pos)?).into());
            }
        },
        FixedString(n) => {
            for _ in 0..rows {
                let bytes = parse_exact(data, pos, *n)?;
                out.push(PyString::new(py, &std::string::String::from_utf8_lossy(bytes)).into());
            }
        },
        Nullable(inner) => {
            let nulls = parse_exact(data, pos, rows)?.to_vec();
            let mut values = decode_column_to_py(data, pos, inner, rows, py)?;
            for (idx, is_null) in nulls.into_iter().enumerate() {
                if is_null != 0 {
                    values[idx] = py.None();
                }
            }
            out = values;
        },
        Array(inner) => {
            let offsets = parse_offsets(data, pos, rows)?;
            let total = offsets.last().copied().unwrap_or(0);
            let values = decode_column_to_py(data, pos, inner, total, py)?;
            let mut prev = 0usize;
            for offset in offsets {
                let end = offset.min(values.len());
                let list = PyList::new(py, values[prev..end].iter().map(|v| v.clone_ref(py)))
                    .map_err(|e| Error::Protocol(e.to_string()))?;
                out.push(list.into());
                prev = offset;
            }
        },
        Map(key, value) => {
            let offsets = parse_offsets(data, pos, rows)?;
            let total = offsets.last().copied().unwrap_or(0);
            let keys = decode_column_to_py(data, pos, key, total, py)?;
            let values = decode_column_to_py(data, pos, value, total, py)?;
            let mut prev = 0usize;
            for offset in offsets {
                let dict = PyDict::new(py);
                let end = offset.min(keys.len()).min(values.len());
                for idx in prev..end {
                    dict.set_item(&keys[idx], &values[idx])
                        .map_err(|e| Error::Protocol(e.to_string()))?;
                }
                out.push(dict.into());
                prev = end;
            }
        },
        Tuple(types) => {
            let columns = types
                .iter()
                .map(|inner| decode_column_to_py(data, pos, inner, rows, py))
                .collect::<Result<Vec<_>>>()?;
            for row in 0..rows {
                out.push(
                    PyTuple::new(py, columns.iter().map(|column| column[row].clone_ref(py)))
                        .map_err(|e| Error::Protocol(e.to_string()))?
                        .into(),
                );
            }
        },
        LowCardinality(inner) => {
            out = decode_column_to_py(data, pos, inner, rows, py)?;
        },
        Nothing => {
            out.resize_with(rows, || py.None());
        },
        other => {
            let _ = other;
            out.resize_with(rows, || py.None());
        },
    }
    Ok(out)
}

fn decode_column_to_py_list_object(
    data: &[u8], pos: &mut usize, ct: &ColumnType, rows: usize, py: Python<'_>,
) -> Result<Py<PyAny>> {
    use ColumnType::*;
    match ct {
        UInt8 => Ok(
            PyList::new(py, parse_exact(data, pos, rows)?.iter().copied())
                .map_err(py_protocol_err)?
                .into(),
        ),
        Int8 | Enum8 => Ok(
            PyList::new(py, parse_exact(data, pos, rows)?.iter().map(|&v| v as i8))
                .map_err(py_protocol_err)?
                .into(),
        ),
        Bool => Ok(
            PyList::new(py, parse_exact(data, pos, rows)?.iter().map(|&v| v != 0))
                .map_err(py_protocol_err)?
                .into(),
        ),
        UInt16 => Ok(PyList::new(
            py,
            parse_exact(data, pos, checked_len(rows, 2)?)?
                .chunks_exact(2)
                .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]])),
        )
        .map_err(py_protocol_err)?
        .into()),
        Int16 | Enum16 => Ok(PyList::new(
            py,
            parse_exact(data, pos, checked_len(rows, 2)?)?
                .chunks_exact(2)
                .map(|chunk| i16::from_le_bytes([chunk[0], chunk[1]])),
        )
        .map_err(py_protocol_err)?
        .into()),
        UInt32 => Ok(PyList::new(
            py,
            parse_exact(data, pos, checked_len(rows, 4)?)?
                .chunks_exact(4)
                .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])),
        )
        .map_err(py_protocol_err)?
        .into()),
        Int32 | Time => Ok(PyList::new(
            py,
            parse_exact(data, pos, checked_len(rows, 4)?)?
                .chunks_exact(4)
                .map(|chunk| i32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])),
        )
        .map_err(py_protocol_err)?
        .into()),
        Float32 => Ok(PyList::new(
            py,
            parse_exact(data, pos, checked_len(rows, 4)?)?
                .chunks_exact(4)
                .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])),
        )
        .map_err(py_protocol_err)?
        .into()),
        UInt64 => Ok(PyList::new(
            py,
            parse_exact(data, pos, checked_len(rows, 8)?)?
                .chunks_exact(8)
                .map(|chunk| {
                    u64::from_le_bytes([
                        chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6],
                        chunk[7],
                    ])
                }),
        )
        .map_err(py_protocol_err)?
        .into()),
        Int64 => Ok(PyList::new(
            py,
            parse_exact(data, pos, checked_len(rows, 8)?)?
                .chunks_exact(8)
                .map(|chunk| {
                    i64::from_le_bytes([
                        chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6],
                        chunk[7],
                    ])
                }),
        )
        .map_err(py_protocol_err)?
        .into()),
        Float64 => Ok(PyList::new(
            py,
            parse_exact(data, pos, checked_len(rows, 8)?)?
                .chunks_exact(8)
                .map(|chunk| {
                    f64::from_le_bytes([
                        chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6],
                        chunk[7],
                    ])
                }),
        )
        .map_err(py_protocol_err)?
        .into()),
        Date => Ok(PyList::new(
            py,
            parse_exact(data, pos, checked_len(rows, 2)?)?
                .chunks_exact(2)
                .map(|chunk| {
                    date_from_days(i64::from(u16::from_le_bytes([chunk[0], chunk[1]])), py)
                }),
        )
        .map_err(py_protocol_err)?
        .into()),
        Date32 => Ok(PyList::new(
            py,
            parse_exact(data, pos, checked_len(rows, 4)?)?
                .chunks_exact(4)
                .map(|chunk| {
                    date_from_days(
                        i64::from(i32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])),
                        py,
                    )
                }),
        )
        .map_err(py_protocol_err)?
        .into()),
        DateTime => Ok(PyList::new(
            py,
            parse_exact(data, pos, checked_len(rows, 4)?)?
                .chunks_exact(4)
                .map(|chunk| {
                    datetime_from_timestamp(
                        i64::from(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])),
                        0,
                        py,
                    )
                }),
        )
        .map_err(py_protocol_err)?
        .into()),
        DateTime64(scale) | Time64(scale) => Ok(PyList::new(
            py,
            parse_exact(data, pos, checked_len(rows, 8)?)?
                .chunks_exact(8)
                .map(|chunk| {
                    let raw = i64::from_le_bytes([
                        chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6],
                        chunk[7],
                    ]);
                    if matches!(ct, DateTime64(_)) {
                        datetime_from_timestamp(raw, *scale, py)
                    } else {
                        match raw.into_py_any(py) {
                            Ok(obj) => obj,
                            Err(_) => py.None(),
                        }
                    }
                }),
        )
        .map_err(py_protocol_err)?
        .into()),
        _ => Ok(
            PyList::new(py, decode_column_to_py(data, pos, ct, rows, py)?)
                .map_err(py_protocol_err)?
                .into(),
        ),
    }
}

fn total_materialized_rows(blocks: &[Block]) -> usize {
    blocks
        .iter()
        .filter(|block| block.column_count() > 0)
        .map(Block::row_count)
        .sum()
}

fn decode_i32_raw(
    data: &[u8], pos: &mut usize, rows: usize, py: Python<'_>, out: &mut Vec<Py<PyAny>>,
) -> Result<()> {
    for chunk in parse_exact(data, pos, checked_len(rows, 4)?)?.chunks_exact(4) {
        out.push(
            i32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])
                .into_py_any(py)
                .map_err(py_protocol_err)?,
        );
    }
    Ok(())
}

fn decode_i64_raw(
    data: &[u8], pos: &mut usize, rows: usize, py: Python<'_>, out: &mut Vec<Py<PyAny>>,
) -> Result<()> {
    for chunk in parse_exact(data, pos, checked_len(rows, 8)?)?.chunks_exact(8) {
        out.push(
            i64::from_le_bytes(
                chunk
                    .try_into()
                    .map_err(|_| Error::Protocol("Int64 chunk length mismatch".into()))?,
            )
            .into_py_any(py)
            .map_err(py_protocol_err)?,
        );
    }
    Ok(())
}

fn parse_offsets(data: &[u8], pos: &mut usize, rows: usize) -> Result<Vec<usize>> {
    let mut offsets = Vec::with_capacity(rows);
    for chunk in parse_exact(data, pos, checked_len(rows, 8)?)?.chunks_exact(8) {
        offsets.push(
            usize::try_from(u64::from_le_bytes(
                chunk
                    .try_into()
                    .map_err(|_| Error::Protocol("offset chunk length mismatch".into()))?,
            ))
            .map_err(|_| Error::Protocol("offset too large".into()))?,
        );
    }
    Ok(offsets)
}

fn parse_string(data: &[u8], pos: &mut usize) -> Result<String> {
    let len = usize::try_from(parse_varint(data, pos)?)
        .map_err(|_| Error::Protocol("string length too large".into()))?;
    let bytes = parse_exact(data, pos, len)?;
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|e| Error::Protocol(format!("invalid utf8 string: {e}")))
}

fn parse_varint(data: &[u8], pos: &mut usize) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0;
    for _ in 0..10 {
        if *pos >= data.len() {
            return Err(Error::Protocol("unexpected eof reading varint".into()));
        }
        let b = data[*pos];
        *pos += 1;
        if shift == 63 && (b & 0x7f) > 1 {
            return Err(Error::Protocol("varint overflow".into()));
        }
        result |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(Error::Protocol("varint too long".into()))
}

fn parse_exact<'a>(data: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = (*pos)
        .checked_add(len)
        .ok_or_else(|| Error::Protocol("buffer position overflow".into()))?;
    if end > data.len() {
        return Err(Error::Protocol("unexpected eof reading column".into()));
    }
    let out = &data[*pos..end];
    *pos = end;
    Ok(out)
}

fn checked_len(rows: usize, width: usize) -> Result<usize> {
    rows.checked_mul(width)
        .ok_or_else(|| Error::Protocol("column length overflow".into()))
}

fn py_protocol_err(err: PyErr) -> Error {
    Error::Protocol(err.to_string())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

// ══════════════════════════════════════════════════════════════════════════
// Python object constructors
// ══════════════════════════════════════════════════════════════════════════

pub(crate) fn date_from_days(days: i64, py: Python<'_>) -> Py<PyAny> {
    let ordinal = 719_163i64 + days;
    match py
        .import("datetime")
        .and_then(|mod_dt| mod_dt.getattr("date"))
        .and_then(|cls| cls.call_method("fromordinal", (ordinal,), None))
    {
        Ok(obj) => obj.into(),
        Err(_) => match days.into_py_any(py) {
            Ok(obj) => obj,
            Err(_) => py.None(),
        },
    }
}

pub(crate) fn datetime_from_timestamp(raw: i64, scale: u32, py: Python<'_>) -> Py<PyAny> {
    let divisor = 10i64.pow(scale.min(9));
    let secs = raw / divisor;
    let frac = (raw % divisor) as f64 / divisor as f64;
    let ts = secs as f64 + frac;
    match py
        .import("datetime")
        .and_then(|mod_dt| mod_dt.getattr("datetime"))
        .and_then(|cls| cls.call_method("fromtimestamp", (ts,), None))
    {
        Ok(obj) => obj.into(),
        Err(_) => match secs.into_py_any(py) {
            Ok(obj) => obj,
            Err(_) => py.None(),
        },
    }
}

// ══════════════════════════════════════════════════════════════════════════
// Utilities
// ══════════════════════════════════════════════════════════════════════════

fn peel_wrappers(type_name: &str) -> &str {
    let mut s = type_name;
    loop {
        let trimmed = s
            .strip_prefix("Nullable(")
            .and_then(|r| r.strip_suffix(')'))
            .or_else(|| {
                s.strip_prefix("LowCardinality(")
                    .and_then(|r| r.strip_suffix(')'))
            });
        match trimmed {
            Some(inner) => s = inner,
            None => return s,
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════
// Column → simple Python list (used by PyColumn.to_list)
// ══════════════════════════════════════════════════════════════════════════

pub(crate) fn owned_to_simple_py_list(
    owned: &OwnedColumnData, count: usize, py: Python<'_>,
) -> PyResult<Py<PyAny>> {
    match owned {
        OwnedColumnData::Int(values) => Ok(PyList::new(py, values.iter().copied())?.into()),
        OwnedColumnData::UInt(values) => Ok(PyList::new(py, values.iter().copied())?.into()),
        OwnedColumnData::Float(values) => Ok(PyList::new(py, values.iter().copied())?.into()),
        OwnedColumnData::String(values) => {
            Ok(PyList::new(py, values.iter().map(|s| s.as_str()))?.into())
        },
        OwnedColumnData::Bool(values) => Ok(PyList::new(py, values.iter().copied())?.into()),
        OwnedColumnData::Null(n) => Ok(PyList::new(py, (0..*n).map(|_| py.None()))?.into()),
        OwnedColumnData::Unknown => Ok(PyList::new(py, (0..count).map(|_| py.None()))?.into()),
    }
}

// ══════════════════════════════════════════════════════════════════════════
// Python values → Block (for INSERT)
// ══════════════════════════════════════════════════════════════════════════

/// Build a Block from a list of row dicts for INSERT.
pub fn py_dicts_to_block(
    rows: &[Py<PyAny>], columns: &[(String, String)], py: Python<'_>,
) -> PyResult<Block> {
    let num_rows = rows.len();
    if num_rows == 0 {
        return Ok(Block {
            columns: Vec::new(),
            rows: 0,
        });
    }

    let mut col_data: Vec<Vec<u8>> = Vec::with_capacity(columns.len());

    for (col_name, col_type) in columns {
        let mut buf = Vec::new();
        for row_obj in rows {
            let row_dict = row_obj.downcast_bound::<PyDict>(py)?;
            let val = row_dict.get_item(col_name.as_str())?.ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "missing column '{col_name}' in row"
                ))
            })?;

            write_py_value_to_wire(&val, col_type, &mut buf, py)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        }
        col_data.push(buf);
    }

    let mut block_columns = Vec::with_capacity(columns.len());
    for (i, (name, type_name)) in columns.iter().enumerate() {
        block_columns.push(ColumnInfo {
            name: name.clone(),
            type_name: type_name.clone(),
            data: bytes::Bytes::from(std::mem::take(&mut col_data[i])),
            lc_materialized: bytes::Bytes::new(),
        });
    }

    Ok(Block {
        columns: block_columns,
        rows: num_rows,
    })
}

/// Write a single Python value in ClickHouse wire format.
fn write_py_value_to_wire(
    val: &Bound<'_, PyAny>, ch_type: &str, buf: &mut Vec<u8>, _py: Python<'_>,
) -> std::result::Result<(), String> {
    use st_clickhouse::sync::protocol::wire;

    let base = peel_wrappers(ch_type);

    match base {
        "UInt8" => {
            let v: u8 = val.extract().map_err(|e| format!("expected u8: {e}"))?;
            buf.push(v);
        },
        "UInt16" => {
            let v: u16 = val.extract().map_err(|e| format!("expected u16: {e}"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        },
        "UInt32" | "IPv4" => {
            let v: u32 = val.extract().map_err(|e| format!("expected u32: {e}"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        },
        "UInt64" => {
            let v: u64 = val.extract().map_err(|e| format!("expected u64: {e}"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        },
        "Int8" => {
            let v: i8 = val.extract().map_err(|e| format!("expected i8: {e}"))?;
            buf.push(u8::from_ne_bytes(v.to_ne_bytes()));
        },
        "Int16" => {
            let v: i16 = val.extract().map_err(|e| format!("expected i16: {e}"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        },
        "Int32" | "Date" | "Date32" => {
            let v: i32 = val.extract().map_err(|e| format!("expected i32: {e}"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        },
        "Int64" | "DateTime" => {
            let v: i64 = val.extract().map_err(|e| format!("expected i64: {e}"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        },
        "Float32" => {
            let v: f32 = val.extract().map_err(|e| format!("expected f32: {e}"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        },
        "Float64" => {
            let v: f64 = val.extract().map_err(|e| format!("expected f64: {e}"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        },
        "String" => {
            let s: String = val.extract().map_err(|e| format!("expected string: {e}"))?;
            wire::write_string(buf, &s).map_err(|e| e.to_string())?;
        },
        s if s.starts_with("FixedString(") => {
            let s: String = val.extract().map_err(|e| format!("expected string: {e}"))?;
            wire::write_string(buf, &s).map_err(|e| e.to_string())?;
        },
        s if s.starts_with("DateTime64") => {
            let v: i64 = val
                .extract()
                .map_err(|e| format!("expected i64 for DateTime64: {e}"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        },
        "UUID" => {
            let uuid_str: String = val
                .extract()
                .map_err(|e| format!("expected string for UUID: {e}"))?;
            let clean = uuid_str.replace('-', "");
            let uuid_bytes: Vec<u8> = (0..clean.len())
                .step_by(2)
                .filter_map(|i| u8::from_str_radix(&clean[i..i + 2], 16).ok())
                .collect();
            if uuid_bytes.len() != 16 {
                return Err("invalid UUID: expected 16 bytes".into());
            }
            // Write in big-endian byte order (ClickHouse UUID wire format)
            buf.extend_from_slice(&uuid_bytes);
        },
        "Bool" => {
            let v: bool = val.extract().map_err(|e| format!("expected bool: {e}"))?;
            buf.push(if v { 1 } else { 0 });
        },
        nullable if nullable.starts_with("Nullable(") => {
            if val.is_none() {
                buf.push(1); // null mask = 1
            } else {
                buf.push(0); // null mask = 0
                let inner = nullable
                    .trim_start_matches("Nullable(")
                    .trim_end_matches(')');
                write_py_value_to_wire(val, inner, buf, _py)?;
            }
        },
        _ => {
            let s: String = val
                .str()
                .map_err(|e| format!("cannot convert value: {e}"))?
                .to_string_lossy()
                .into_owned();
            wire::write_string(buf, &s).map_err(|e| e.to_string())?;
        },
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peel_wrappers() {
        assert_eq!(peel_wrappers("UInt64"), "UInt64");
        assert_eq!(peel_wrappers("Nullable(UInt64)"), "UInt64");
        assert_eq!(peel_wrappers("LowCardinality(String)"), "String");
        assert_eq!(peel_wrappers("Nullable(LowCardinality(String))"), "String");
    }

    #[test]
    fn test_uuid_format() {
        let raw: u128 = 0x1234567890abcdef;
        let hex = format!("{:032x}", raw);
        let uuid_s = format!(
            "{}-{}-{}-{}-{}",
            &hex[0..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..32]
        );
        assert_eq!(uuid_s.len(), 36);
        assert_eq!(&uuid_s[8..9], "-");
    }

    #[test]
    fn test_ipv4_format() {
        let raw: u32 = 0x7F000001;
        let ip_s = format!(
            "{}.{}.{}.{}",
            (raw >> 24) & 0xFF,
            (raw >> 16) & 0xFF,
            (raw >> 8) & 0xFF,
            raw & 0xFF
        );
        assert_eq!(ip_s, "127.0.0.1");
    }
}
