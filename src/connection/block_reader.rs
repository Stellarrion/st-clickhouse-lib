use crate::connection::io::{
    checked_column_bytes, checked_column_len, checked_count, checked_len, checked_monotonic_offset,
    checked_string_len, checked_usize, encode_varint, lc_idx_width, read_block_header,
    read_offsets_column, read_string_async, read_string_column_with_prefixes, read_varint_async,
};
use crate::connection::raw_block_reader::{
    read_column_raw_recorded, read_column_state_prefix_recorded, read_variant_body_raw_recorded,
    variant_states,
};
use crate::error::Result;
use crate::protocol::block::{Block, ColumnInfo};
use crate::protocol::type_parser;
use crate::runtime::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// Skip one column of a decompressed (compressed-materialized) block,
/// consuming exactly the bytes the streaming materialized reader consumes.
///
/// Delegates to the shared slice-based skip implementation so the buffered
/// parser and the raw stream readers agree on the wire layout byte for byte:
/// Array/Map offsets are fixed-width little-endian u64s (never varints),
/// materialized JSON carries an 8-byte string-serialization version,
/// LowCardinality carries its 24-byte header/dictionary/index layout, and
/// Variant/Dynamic carry their per-subcolumn state prefixes.
fn skip_col_typed(data: &[u8], pos: &mut usize, tn: &str, rows: usize) -> Result<()> {
    crate::protocol::skip_column::skip_column_data_by_name(data, pos, tn, rows)
}

fn advance_pos(data: &[u8], pos: &mut usize, len: usize) -> Result<()> {
    let end = (*pos)
        .checked_add(len)
        .ok_or_else(|| crate::error::Error::Protocol("buffer position overflow".into()))?;
    if end > data.len() {
        return Err(crate::error::Error::Protocol(
            "unexpected end of buffer skipping column data".into(),
        ));
    }
    *pos = end;
    Ok(())
}

// ═══════════════════════════════════════════════
// Block & column readers (used by both direct and background reads)
// ═══════════════════════════════════════════════

#[tracing::instrument(level = "debug", skip(stream), name = "clickhouse.block.read")]
pub(super) async fn read_data_block<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<Block> {
    use crate::protocol::block::Block;
    let (cols, rows) = read_block_header(stream).await?;
    let mut columns = Vec::with_capacity(cols);
    for _ in 0..cols {
        let name = read_string_async(stream).await?;
        let type_name = read_string_async(stream).await?;
        stream.read_exact(&mut [0u8; 1]).await?;
        let raw = if rows > 0 {
            read_column_async(stream, &type_name, rows).await?
        } else {
            Vec::new()
        };
        columns.push(ColumnInfo {
            name,
            type_name,
            data: bytes::Bytes::from(raw),
            lc_materialized: bytes::Bytes::new(),
        });
    }
    Ok(Block { columns, rows })
}

async fn read_data_block_body<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<Block> {
    use crate::protocol::block::Block;
    loop {
        let d = read_varint_async(stream).await?;
        match d {
            0 => break,
            1 => {
                stream.read_exact(&mut [0u8; 1]).await?;
            },
            2 => {
                stream.read_exact(&mut [0u8; 4]).await?;
            },
            3 => {
                read_varint_async(stream).await?;
            },
            _ => break,
        }
    }
    let cols = checked_count(
        read_varint_async(stream).await?,
        "block column",
        crate::limits::MAX_BLOCK_COLUMNS,
    )?;
    let rows = checked_count(
        read_varint_async(stream).await?,
        "block row",
        crate::limits::MAX_BLOCK_ROWS,
    )?;
    let mut columns = Vec::with_capacity(cols);
    for _ in 0..cols {
        let name = read_string_async(stream).await?;
        let type_name = read_string_async(stream).await?;
        stream.read_exact(&mut [0u8; 1]).await?;
        let raw = if rows > 0 {
            read_column_async(stream, &type_name, rows).await?
        } else {
            Vec::new()
        };
        columns.push(ColumnInfo {
            name,
            type_name,
            data: bytes::Bytes::from(raw),
            lc_materialized: bytes::Bytes::new(),
        });
    }
    Ok(Block { columns, rows })
}

pub(super) async fn read_data_block_maybe_compressed<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, compressed: bool,
) -> Result<Block> {
    if compressed {
        read_data_block_compressed(stream).await
    } else {
        read_data_block(stream).await
    }
}

pub(super) async fn discard_data_block_maybe_compressed<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, compressed: bool,
) -> Result<usize> {
    if compressed {
        discard_data_block_compressed(stream).await
    } else {
        discard_data_block(stream).await
    }
}

async fn discard_data_block<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<usize> {
    let (cols, rows) = read_block_header(stream).await?;
    for _ in 0..cols {
        let name = read_string_async(stream).await?;
        let type_name = read_string_async(stream).await?;
        let mut custom = [0u8; 1];
        stream.read_exact(&mut custom).await?;
        if custom[0] != 0 {
            return Err(crate::error::Error::Protocol(format!(
                "unsupported custom serialization for column '{name}'"
            )));
        }
        discard_column_async(stream, &type_name, rows).await?;
    }
    Ok(rows)
}

async fn discard_data_block_body<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<usize> {
    loop {
        let d = read_varint_async(stream).await?;
        match d {
            0 => break,
            1 => {
                stream.read_exact(&mut [0u8; 1]).await?;
            },
            2 => {
                stream.read_exact(&mut [0u8; 4]).await?;
            },
            3 => {
                read_varint_async(stream).await?;
            },
            _ => break,
        }
    }
    let cols = checked_count(
        read_varint_async(stream).await?,
        "block column",
        crate::limits::MAX_BLOCK_COLUMNS,
    )?;
    let rows = checked_count(
        read_varint_async(stream).await?,
        "block row",
        crate::limits::MAX_BLOCK_ROWS,
    )?;
    for _ in 0..cols {
        let name = read_string_async(stream).await?;
        let type_name = read_string_async(stream).await?;
        let mut custom = [0u8; 1];
        stream.read_exact(&mut custom).await?;
        if custom[0] != 0 {
            return Err(crate::error::Error::Protocol(format!(
                "unsupported custom serialization for column '{name}'"
            )));
        }
        discard_column_async(stream, &type_name, rows).await?;
    }
    Ok(rows)
}

async fn discard_data_block_compressed<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<usize> {
    let _table_name = read_string_async(stream).await?;
    match read_compressed_payload_or_plain_prefix(stream).await? {
        BlockPayload::Compressed(decompressed) => discard_decompressed_block(&decompressed),
        BlockPayload::PlainPrefix(prefix) => {
            let mut prefixed = PrefixedStream::new(prefix, stream);
            discard_data_block_body(&mut prefixed).await
        },
    }
}

async fn read_data_block_compressed<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<Block> {
    let _table_name = read_string_async(stream).await?;
    match read_compressed_payload_or_plain_prefix(stream).await? {
        BlockPayload::Compressed(decompressed) => parse_decompressed_block(decompressed),
        BlockPayload::PlainPrefix(prefix) => {
            let mut prefixed = PrefixedStream::new(prefix, stream);
            read_data_block_body(&mut prefixed).await
        },
    }
}

pub(super) async fn read_table_columns_packet<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, compressed: bool,
) -> Result<()> {
    if !compressed {
        let _name = read_string_async(stream).await?;
        let _types = read_string_async(stream).await?;
        return Ok(());
    }

    match read_compressed_payload_or_plain_prefix(stream).await? {
        BlockPayload::Compressed(payload) => {
            let mut pos = 0usize;
            let _name = parse_string(&payload, &mut pos)?;
            let _types = parse_string(&payload, &mut pos)?;
        },
        BlockPayload::PlainPrefix(prefix) => {
            let mut prefixed = PrefixedStream::new(prefix, stream);
            let _name = read_string_async(&mut prefixed).await?;
            let _types = read_string_async(&mut prefixed).await?;
        },
    }
    Ok(())
}

enum BlockPayload {
    Compressed(bytes::Bytes),
    PlainPrefix(Vec<u8>),
}

async fn read_compressed_payload_or_plain_prefix<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<BlockPayload> {
    const METHOD_OFFSET: usize = 16;
    const HEADER_LEN: usize = 25;
    const COMPRESSED_BODY_HEADER_LEN: usize = 9;

    let mut prefix = [0u8; METHOD_OFFSET + 1];
    stream.read_exact(&mut prefix[..1]).await?;
    match crate::runtime::time::timeout(
        Duration::from_millis(50),
        stream.read_exact(&mut prefix[1..]),
    )
    .await
    {
        Ok(Ok(_)) => {},
        Ok(Err(err)) => return Err(err.into()),
        Err(_) => return Ok(BlockPayload::PlainPrefix(prefix[..1].to_vec())),
    }

    match prefix[METHOD_OFFSET] {
        0x82 | 0x90 | 0x02 => {},
        _ => return Ok(BlockPayload::PlainPrefix(prefix.to_vec())),
    }

    let mut frame = Vec::with_capacity(HEADER_LEN);
    frame.extend_from_slice(&prefix);
    let mut rest = [0u8; HEADER_LEN - METHOD_OFFSET - 1];
    stream.read_exact(&mut rest).await?;
    frame.extend_from_slice(&rest);

    let compressed_size = u32::from_le_bytes([frame[17], frame[18], frame[19], frame[20]]) as usize;
    // The method byte matched, so this is a compressed frame. Sizes below the
    // 9-byte header remain ambiguous with plain payloads and keep the plain
    // fallback, but an oversized claim is rejected outright: it must never
    // reach the resize below, which previously allowed up to 1 GiB.
    if compressed_size < COMPRESSED_BODY_HEADER_LEN {
        return Ok(BlockPayload::PlainPrefix(frame));
    }
    if compressed_size > crate::limits::MAX_FRAME_SIZE {
        return Err(crate::error::Error::Compression(format!(
            "compressed_size {compressed_size} exceeds {} byte frame cap",
            crate::limits::MAX_FRAME_SIZE
        )));
    }

    let body_len = compressed_size - COMPRESSED_BODY_HEADER_LEN;
    let start = frame.len();
    frame.resize(start + body_len, 0);
    stream.read_exact(&mut frame[start..]).await?;

    let decompressed = crate::compression::decode_frame_bytes(&frame)?;
    Ok(BlockPayload::Compressed(bytes::Bytes::from(decompressed)))
}

struct PrefixedStream<'a, S> {
    prefix: Vec<u8>,
    prefix_pos: usize,
    stream: &'a mut S,
}

impl<'a, S> PrefixedStream<'a, S> {
    fn new(prefix: Vec<u8>, stream: &'a mut S) -> Self {
        Self {
            prefix,
            prefix_pos: 0,
            stream,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for PrefixedStream<'_, S> {
    fn poll_read(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.prefix_pos < self.prefix.len() {
            let n = buf
                .remaining()
                .min(self.prefix.len().saturating_sub(self.prefix_pos));
            if n > 0 {
                buf.put_slice(&self.prefix[self.prefix_pos..self.prefix_pos + n]);
                self.prefix_pos += n;
            }
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut *self.stream).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for PrefixedStream<'_, S> {
    fn poll_write(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut *self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.stream).poll_shutdown(cx)
    }
}

fn parse_decompressed_block(shared: bytes::Bytes) -> Result<Block> {
    let mut pos = 0usize;
    parse_block_info(&shared, &mut pos)?;
    let cols = checked_count(
        parse_varint(&shared, &mut pos)?,
        "block column",
        crate::limits::MAX_BLOCK_COLUMNS,
    )?;
    let rows = checked_count(
        parse_varint(&shared, &mut pos)?,
        "block row",
        crate::limits::MAX_BLOCK_ROWS,
    )?;
    let mut columns = Vec::with_capacity(cols);
    for _ in 0..cols {
        let name = parse_string(&shared, &mut pos)?.to_owned();
        let type_name = parse_string(&shared, &mut pos)?.to_owned();
        if pos >= shared.len() {
            return Err(crate::error::Error::Protocol(
                "missing custom serialization byte".into(),
            ));
        }
        let custom = shared[pos];
        pos += 1;
        if custom != 0 {
            return Err(crate::error::Error::Protocol(format!(
                "unsupported custom serialization for column '{name}'"
            )));
        }
        let parsed = type_parser::parse_type(&type_name);
        // LowCardinality columns are materialized (decoded to the inner
        // column layout) like the streaming reader does, so the sliced data
        // matches what `read_column_async` produces for the same column.
        if let Ok(type_parser::ColumnType::LowCardinality(inner)) = &parsed {
            let materialized = lc_materialized_from_buffer(&shared, &mut pos, inner, rows)?;
            columns.push(ColumnInfo {
                name,
                type_name,
                data: materialized,
                lc_materialized: bytes::Bytes::new(),
            });
            continue;
        }
        let start = pos;
        skip_col_typed(&shared, &mut pos, &type_name, rows)?;
        // Top-level JSON columns: the materialized JSON reader strips the
        // 8-byte string-serialization version, so the buffered slice must
        // match it (the type is matched semantically so `Object('json')`
        // counts too). JSON nested inside Array/Map/Tuple/Nullable keeps the
        // version byte inside the slice, which the column decoders misread —
        // reject it loudly instead of returning silently wrong data.
        if rows > 0 {
            match &parsed {
                Ok(type_parser::ColumnType::JSON) => {
                    let data = shared.slice(start + 8..pos);
                    columns.push(ColumnInfo {
                        name,
                        type_name,
                        data,
                        lc_materialized: bytes::Bytes::new(),
                    });
                    continue;
                },
                Ok(ct) if crate::protocol::skip_column::contains_nested_json(ct) => {
                    return Err(crate::error::Error::Protocol(format!(
                        "nested JSON columns are not supported in buffered block reads \
                         (column '{name}' of type {ct}); use uncompressed reads or query_raw"
                    )));
                },
                _ => {},
            }
        }
        columns.push(ColumnInfo {
            name,
            type_name,
            data: shared.slice(start..pos),
            lc_materialized: bytes::Bytes::new(),
        });
    }
    Ok(Block { columns, rows })
}

/// Materialize one LowCardinality column from a decompressed block buffer.
///
/// Mirrors the sync buffered reader's `read_low_cardinality_from_buffer` and
/// the streaming `read_lc_async`: consume the 24-byte header, the dictionary
/// column (`num_keys` inner rows), the 8-byte index count, and the index
/// bytes, then decode the indexes against the dictionary.
fn lc_materialized_from_buffer(
    shared: &bytes::Bytes, pos: &mut usize, inner: &type_parser::ColumnType, rows: usize,
) -> Result<bytes::Bytes> {
    use crate::connection::io::{checked_count, checked_usize, lc_idx_width};

    if rows == 0 {
        return Ok(bytes::Bytes::new());
    }
    let meta = parse_fixed_bytes(shared, pos, 24)?;
    let version = u64::from_le_bytes(meta[0..8].try_into().map_err(|_| {
        crate::error::Error::Protocol("LowCardinality version length mismatch".into())
    })?);
    let serial_type = u64::from_le_bytes(meta[8..16].try_into().map_err(|_| {
        crate::error::Error::Protocol("LowCardinality metadata length mismatch".into())
    })?);
    let idx_width = lc_idx_width(version, serial_type)?;
    let num_keys = checked_count(
        u64::from_le_bytes(meta[16..24].try_into().map_err(|_| {
            crate::error::Error::Protocol("LowCardinality key count length mismatch".into())
        })?),
        "LowCardinality key",
        crate::limits::MAX_JSON_DYNAMIC_ITEMS,
    )?;
    let dict_start = *pos;
    skip_col_typed(shared, pos, &inner.to_string(), num_keys)?;
    let dict_data = &shared[dict_start..*pos];
    let count_bytes = parse_fixed_bytes(shared, pos, 8)?;
    let indexes = checked_usize(
        u64::from_le_bytes(count_bytes.try_into().map_err(|_| {
            crate::error::Error::Protocol("LowCardinality index count length mismatch".into())
        })?),
        "LowCardinality indexes",
    )?;
    if indexes != rows {
        return Err(crate::error::Error::Protocol(format!(
            "LowCardinality index count {indexes} does not match row count {rows}"
        )));
    }
    let index_data = parse_fixed_bytes(
        shared,
        pos,
        crate::connection::io::checked_column_len(indexes, idx_width, "LowCardinality index")?,
    )?;
    crate::cursor::materialize_lc_inner(dict_data, inner, index_data, idx_width, indexes)
        .map(bytes::Bytes::from)
}

/// Read `len` bytes from a buffer at `pos`, advancing the position.
fn parse_fixed_bytes<'a>(data: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = (*pos)
        .checked_add(len)
        .ok_or_else(|| crate::error::Error::Protocol("buffer position overflow".into()))?;
    if end > data.len() {
        return Err(crate::error::Error::Protocol("eof".into()));
    }
    let bytes = &data[*pos..end];
    *pos = end;
    Ok(bytes)
}

fn discard_decompressed_block(shared: &[u8]) -> Result<usize> {
    let mut pos = 0usize;
    parse_block_info(shared, &mut pos)?;
    let cols = checked_count(
        parse_varint(shared, &mut pos)?,
        "block column",
        crate::limits::MAX_BLOCK_COLUMNS,
    )?;
    let rows = checked_count(
        parse_varint(shared, &mut pos)?,
        "block row",
        crate::limits::MAX_BLOCK_ROWS,
    )?;
    for _ in 0..cols {
        let name = parse_string(shared, &mut pos)?;
        let type_name = parse_string(shared, &mut pos)?;
        if pos >= shared.len() {
            return Err(crate::error::Error::Protocol(
                "missing custom serialization byte".into(),
            ));
        }
        let custom = shared[pos];
        pos += 1;
        if custom != 0 {
            return Err(crate::error::Error::Protocol(format!(
                "unsupported custom serialization for column '{name}'"
            )));
        }
        skip_col_typed(shared, &mut pos, type_name, rows)?;
    }
    Ok(rows)
}

fn parse_block_info(data: &[u8], pos: &mut usize) -> Result<()> {
    loop {
        let d = parse_varint(data, pos)?;
        match d {
            0 => return Ok(()),
            1 => advance_pos(data, pos, 1)?,
            2 => advance_pos(data, pos, 4)?,
            3 => {
                let _ = parse_varint(data, pos)?;
            },
            _ => return Ok(()),
        }
    }
}

pub(super) async fn read_column_async<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, type_name: &str, rows: usize,
) -> Result<Vec<u8>> {
    if rows == 0 {
        return Ok(Vec::new());
    }

    use crate::protocol::type_parser::ColumnType::*;
    let ct = match type_parser::parse_type(type_name) {
        Ok(c) => c,
        Err(_) => {
            let mut data = Vec::new();
            let mut total = 0usize;
            for _ in 0..rows {
                let l =
                    checked_string_len(read_varint_async(stream).await?, "string value length")?;
                // Cumulative per-column cap on the claim, before resize/read.
                total = checked_column_bytes(total, l, "string value length")?;
                encode_varint(&mut data, l as u64);
                let start = data.len();
                let end = start.checked_add(l).ok_or_else(|| {
                    crate::error::Error::Protocol("column buffer length overflow".into())
                })?;
                data.resize(end, 0);
                stream.read_exact(&mut data[start..]).await?;
            }
            return Ok(data);
        },
    };
    match &ct {
        Nothing => {
            let mut data = vec![0u8; rows];
            stream.read_exact(&mut data).await?;
            Ok(data)
        },
        Nullable(inner) => {
            let mut data = vec![0u8; rows];
            stream.read_exact(&mut data).await?;
            let inner_data = Box::pin(read_column_async(stream, &inner.to_string(), rows)).await?;
            data.extend(inner_data);
            Ok(data)
        },
        Array(inner) => {
            // Wire format: N * 8 bytes UInt64 offsets + element data
            let (mut obuf, total) = read_offsets_column(stream, rows, "array offset").await?;
            if total > 0 {
                let inner_data =
                    Box::pin(read_column_async(stream, &inner.to_string(), total)).await?;
                obuf.extend(inner_data);
            }
            Ok(obuf)
        },
        Map(k, v) => {
            let (mut obuf, total) = read_offsets_column(stream, rows, "map offset").await?;
            if total > 0 {
                obuf.extend(Box::pin(read_column_async(stream, &k.to_string(), total)).await?);
                obuf.extend(Box::pin(read_column_async(stream, &v.to_string(), total)).await?);
            }
            Ok(obuf)
        },
        Tuple(elems) => {
            let mut raw = Vec::new();
            for elem in elems {
                raw.extend(Box::pin(read_column_async(stream, &elem.to_string(), rows)).await?);
            }
            Ok(raw)
        },
        LowCardinality(inner) => Box::pin(read_lc_async(stream, inner, rows)).await,
        Point => {
            let mut data = Box::pin(read_column_async(stream, "Float64", rows)).await?;
            data.extend(Box::pin(read_column_async(stream, "Float64", rows)).await?);
            Ok(data)
        },
        Ring => Box::pin(read_column_async(stream, "Array(Point)", rows)).await,
        Polygon => Box::pin(read_column_async(stream, "Array(Ring)", rows)).await,
        MultiPolygon => Box::pin(read_column_async(stream, "Array(Polygon)", rows)).await,
        JSON => {
            let mut ver = [0u8; 8];
            stream.read_exact(&mut ver).await?;
            let version = u64::from_le_bytes(ver);
            if version != 1 && version != 4 {
                return Err(crate::error::Error::Protocol(format!(
                    "materialized JSON reads require string serialization version 1 or 4, got {version}; \
                     enable {}=1 or use query_raw",
                    crate::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING
                )));
            }
            read_string_column_with_prefixes(stream, rows, "JSON string length").await
        },
        Dynamic => {
            let mut data = Vec::new();
            let mut budget = crate::limits::MAX_COLUMN_BYTES;
            read_column_raw_recorded(stream, type_name, rows, &mut data, &mut budget).await?;
            Ok(data)
        },
        AggregateFunction | SimpleAggregateFunction => Err(crate::error::Error::Protocol(
            "AggregateFunction type not yet supported in wire reader".into(),
        )),
        Variant(types) => {
            let mut data = Vec::new();
            let mut budget = crate::limits::MAX_COLUMN_BYTES;
            let state =
                read_column_state_prefix_recorded(stream, &ct, &mut data, &mut budget).await?;
            read_variant_body_raw_recorded(
                stream,
                types,
                variant_states(&state),
                rows,
                &mut data,
                &mut budget,
            )
            .await?;
            Ok(data)
        },
        String | Other(_) => {
            read_string_column_with_prefixes(stream, rows, "string value length").await
        },
        FixedString(n) => {
            let mut data = vec![0u8; checked_column_len(rows, *n, "FixedString column")?];
            stream.read_exact(&mut data).await?;
            Ok(data)
        },
        _ => {
            let w = ct
                .fixed_width()
                .ok_or_else(|| crate::error::Error::Protocol(format!("unknown type {ct}")))?;
            let mut data = vec![0u8; checked_column_len(rows, w, "fixed-width column")?];
            stream.read_exact(&mut data).await?;
            Ok(data)
        },
    }
}

async fn discard_column_async<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, type_name: &str, rows: usize,
) -> Result<()> {
    if rows == 0 {
        return Ok(());
    }

    use crate::protocol::type_parser::ColumnType::*;
    let ct = match type_parser::parse_type(type_name) {
        Ok(c) => c,
        Err(_) => {
            for _ in 0..rows {
                let len =
                    checked_string_len(read_varint_async(stream).await?, "string value length")?;
                discard_exact_async(stream, len).await?;
            }
            return Ok(());
        },
    };

    match &ct {
        UInt8 | Int8 | Bool | Enum8 => discard_exact_async(stream, rows).await?,
        UInt16 | Int16 | Date | Enum16 => {
            discard_exact_async(stream, checked_len(rows, 2)?).await?
        },
        UInt32 | Int32 | Float32 | Date32 | DateTime | Time | IPv4 => {
            discard_exact_async(stream, checked_len(rows, 4)?).await?
        },
        UInt64 | Int64 | Float64 | DateTime64(_) | Time64(_) => {
            discard_exact_async(stream, checked_len(rows, 8)?).await?
        },
        UInt128 | Int128 | UUID | IPv6 => {
            discard_exact_async(stream, checked_len(rows, 16)?).await?
        },
        UInt256 | Int256 => discard_exact_async(stream, checked_len(rows, 32)?).await?,
        Decimal(1..=9, _) => discard_exact_async(stream, checked_len(rows, 4)?).await?,
        Decimal(10..=18, _) => discard_exact_async(stream, checked_len(rows, 8)?).await?,
        Decimal(19..=38, _) => discard_exact_async(stream, checked_len(rows, 16)?).await?,
        Decimal(39..=76, _) => discard_exact_async(stream, checked_len(rows, 32)?).await?,
        Decimal(precision, _) => {
            return Err(crate::error::Error::Protocol(format!(
                "unsupported Decimal precision {precision}"
            )));
        },
        String => {
            for _ in 0..rows {
                let len =
                    checked_string_len(read_varint_async(stream).await?, "string value length")?;
                discard_exact_async(stream, len).await?;
            }
        },
        JSON => {
            let mut ver = [0u8; 8];
            stream.read_exact(&mut ver).await?;
            let version = u64::from_le_bytes(ver);
            if version != 1 && version != 4 {
                return Err(crate::error::Error::Protocol(format!(
                    "materialized JSON reads require string serialization version 1 or 4, got {version}; \
                     enable {}=1 or use query_raw",
                    crate::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING
                )));
            }
            for _ in 0..rows {
                let len =
                    checked_string_len(read_varint_async(stream).await?, "JSON string length")?;
                discard_exact_async(stream, len).await?;
            }
        },
        FixedString(n) => discard_exact_async(stream, checked_len(rows, *n)?).await?,
        Nullable(inner) => {
            discard_exact_async(stream, rows).await?;
            Box::pin(discard_column_async(stream, &inner.to_string(), rows)).await?;
        },
        Array(inner) => {
            let total = discard_offsets_async(stream, rows, "array offset").await?;
            Box::pin(discard_column_async(stream, &inner.to_string(), total)).await?;
        },
        Map(k, v) => {
            let total = discard_offsets_async(stream, rows, "map offset").await?;
            Box::pin(discard_column_async(stream, &k.to_string(), total)).await?;
            Box::pin(discard_column_async(stream, &v.to_string(), total)).await?;
        },
        Tuple(elems) => {
            for elem in elems {
                Box::pin(discard_column_async(stream, &elem.to_string(), rows)).await?;
            }
        },
        LowCardinality(inner) => Box::pin(discard_lc_async(stream, inner, rows)).await?,
        Point => {
            Box::pin(discard_column_async(stream, "Float64", rows)).await?;
            Box::pin(discard_column_async(stream, "Float64", rows)).await?;
        },
        Ring => Box::pin(discard_column_async(stream, "Array(Point)", rows)).await?,
        Polygon => Box::pin(discard_column_async(stream, "Array(Ring)", rows)).await?,
        MultiPolygon => Box::pin(discard_column_async(stream, "Array(Polygon)", rows)).await?,
        Dynamic | Variant(_) | AggregateFunction | SimpleAggregateFunction => {
            let _ = Box::pin(read_column_async(stream, type_name, rows)).await?;
        },
        Nothing => discard_exact_async(stream, rows).await?,
        Other(_) => {
            let _ = Box::pin(read_column_async(stream, type_name, rows)).await?;
        },
    }
    Ok(())
}

async fn discard_lc_async<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, inner: &crate::protocol::type_parser::ColumnType, rows: usize,
) -> Result<()> {
    let mut meta = [0u8; 24];
    stream.read_exact(&mut meta).await?;
    let version = u64::from_le_bytes(meta[0..8].try_into().map_err(|_| {
        crate::error::Error::Protocol("LowCardinality version length mismatch".into())
    })?);
    let serial_type = u64::from_le_bytes(meta[8..16].try_into().map_err(|_| {
        crate::error::Error::Protocol("LowCardinality metadata length mismatch".into())
    })?);
    let idx_width = lc_idx_width(version, serial_type)?;
    let num_keys = checked_count(
        u64::from_le_bytes(meta[16..24].try_into().map_err(|_| {
            crate::error::Error::Protocol("LowCardinality key count length mismatch".into())
        })?),
        "LowCardinality key",
        crate::limits::MAX_JSON_DYNAMIC_ITEMS,
    )?;
    Box::pin(discard_column_async(stream, &inner.to_string(), num_keys)).await?;
    let mut count = [0u8; 8];
    stream.read_exact(&mut count).await?;
    let indexes = checked_usize(u64::from_le_bytes(count), "LowCardinality indexes")?;
    if indexes != rows {
        return Err(crate::error::Error::Protocol(format!(
            "LowCardinality index count {indexes} does not match row count {rows}"
        )));
    }
    discard_exact_async(
        stream,
        checked_column_len(indexes, idx_width, "LowCardinality index")?,
    )
    .await
}

async fn discard_offsets_async<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, rows: usize, name: &str,
) -> Result<usize> {
    let mut offset = [0u8; 8];
    let mut total = 0usize;
    for _ in 0..rows {
        stream.read_exact(&mut offset).await?;
        // Cumulative prefix sums must be non-decreasing; the last offset is
        // the inner element count, capped at MAX_BLOCK_ROWS before the inner
        // column is read or skipped.
        total = checked_monotonic_offset(total, u64::from_le_bytes(offset), name)?;
    }
    Ok(total)
}

async fn discard_exact_async<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, mut len: usize,
) -> Result<()> {
    let mut buf = [0u8; 16 * 1024];
    while len != 0 {
        let n = len.min(buf.len());
        stream.read_exact(&mut buf[..n]).await?;
        len -= n;
    }
    Ok(())
}

/// Read a Dynamic column's full data from the wire — used internally.
async fn read_lc_async<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, inner: &type_parser::ColumnType, rows: usize,
) -> Result<Vec<u8>> {
    if rows == 0 {
        return Ok(Vec::new());
    }

    let mut meta = [0u8; 24];
    stream.read_exact(&mut meta).await?;
    let version = u64::from_le_bytes(meta[0..8].try_into().map_err(|_| {
        crate::error::Error::Protocol("LowCardinality version length mismatch".into())
    })?);
    let mut num_keys_bytes = [0u8; 8];
    num_keys_bytes.copy_from_slice(&meta[16..24]);
    let num_keys = checked_count(
        u64::from_le_bytes(num_keys_bytes),
        "LowCardinality key",
        crate::limits::MAX_JSON_DYNAMIC_ITEMS,
    )?;
    let mut serial_type_bytes = [0u8; 8];
    serial_type_bytes.copy_from_slice(&meta[8..16]);
    let serial_type = u64::from_le_bytes(serial_type_bytes);
    let idx_width = lc_idx_width(version, serial_type)?;
    let dict_data = if num_keys > 0 {
        Box::pin(read_column_async(stream, &inner.to_string(), num_keys)).await?
    } else {
        Vec::new()
    };
    let mut il = [0u8; 8];
    stream.read_exact(&mut il).await?;
    let ni = checked_usize(u64::from_le_bytes(il), "LowCardinality indexes")?;
    if ni != rows {
        return Err(crate::error::Error::Protocol(format!(
            "LowCardinality index count {ni} does not match row count {rows}"
        )));
    }
    let mut indexes = vec![0u8; checked_column_len(ni, idx_width, "LowCardinality index")?];
    if ni > 0 {
        stream.read_exact(&mut indexes).await?;
    }
    if ni == 0 || num_keys == 0 {
        return Ok(Vec::new());
    }
    crate::cursor::materialize_lc_inner(&dict_data, inner, &indexes, idx_width, ni)
}

// ═══════════════════════════════════════════════
// Sync parsing helpers
// ═══════════════════════════════════════════════

fn parse_varint(data: &[u8], pos: &mut usize) -> Result<u64> {
    crate::protocol::wire::parse_varint(data, pos)
}

#[allow(dead_code)]
fn parse_string<'a>(data: &'a [u8], pos: &mut usize) -> Result<&'a str> {
    let bytes = parse_bytes(data, pos)?;
    std::str::from_utf8(bytes).map_err(|e| crate::error::Error::Protocol(format!("utf8: {e}")))
}

fn parse_bytes<'a>(data: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
    let len = usize::try_from(parse_varint(data, pos)?)
        .map_err(|_| crate::error::Error::Protocol("byte string too large".into()))?;
    let end = (*pos)
        .checked_add(len)
        .ok_or_else(|| crate::error::Error::Protocol("byte string length overflow".into()))?;
    if end > data.len() {
        return Err(crate::error::Error::Protocol("eof".into()));
    }
    let bytes = &data[*pos..end];
    *pos = end;
    Ok(bytes)
}

#[allow(dead_code)]
fn parse_i32(data: &[u8], pos: &mut usize) -> Result<i32> {
    let end = (*pos)
        .checked_add(4)
        .ok_or_else(|| crate::error::Error::Protocol("i32 length overflow".into()))?;
    if end > data.len() {
        return Err(crate::error::Error::Protocol("eof".into()));
    }
    let v = i32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
    *pos = end;
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClickHouseColumnData as _;
    use crate::protocol::wire;
    use crate::runtime::io::AsyncWriteExt as _;

    // ═══════════════════════════════════════════════
    // Compressed-materialized block framing (parse_decompressed_block)
    // ═══════════════════════════════════════════════

    /// Build a decompressed block body: BlockInfo terminator, column and row
    /// counts, then per column name/type/custom-serialization-byte/data.
    fn decompressed_block_buf(rows: u64, cols: &[(&str, &str, Vec<u8>)]) -> bytes::Bytes {
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
        bytes::Bytes::from(buf)
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

    /// Array offsets are fixed-width little-endian u64s whose last value is
    /// the inner row count; empty array rows advance the offsets without any
    /// inner bytes, and the trailing column still parses.
    #[test]
    fn decompressed_array_uint8_mixed_empty_rows_frame_trailing_column() {
        let data = offsets(&[2, 2, 3]); // row0 [1,2], row1 [], row2 [9]
        let block = parse_decompressed_block(decompressed_block_buf(
            3,
            &[
                ("a", "Array(UInt8)", [data, vec![1, 2, 9]].concat()),
                ("x", "UInt8", vec![7, 8, 9]),
            ],
        ))
        .expect("parse block");

        let col = block.column::<Vec<u8>>("a").expect("read array column");
        assert_eq!(col.get(0).expect("row 0"), vec![1, 2]);
        assert_eq!(col.get(1).expect("row 1"), Vec::<u8>::new());
        assert_eq!(col.get(2).expect("row 2"), vec![9]);

        let trailing = block.column::<u8>("x").expect("read trailing column");
        assert_eq!(trailing.get(0).expect("row 0"), 7);
        assert_eq!(trailing.get(1).expect("row 1"), 8);
        assert_eq!(trailing.get(2).expect("row 2"), 9);
    }

    /// A column whose every array is empty has last offset 0: the inner
    /// column carries zero rows and zero bytes. Skipping must not invent
    /// inner rows, or the trailing column misframes.
    #[test]
    fn decompressed_array_all_empty_offsets_zero_skips_no_inner() {
        let block = parse_decompressed_block(decompressed_block_buf(
            2,
            &[
                ("a", "Array(UInt8)", offsets(&[0, 0])),
                ("x", "UInt64", offsets(&[7, 8])),
            ],
        ))
        .expect("parse block");

        let col = block.column::<Vec<u8>>("a").expect("read array column");
        assert_eq!(col.get(0).expect("row 0"), Vec::<u8>::new());
        assert_eq!(col.get(1).expect("row 1"), Vec::<u8>::new());

        let trailing = block.column::<u64>("x").expect("read trailing column");
        assert_eq!(trailing.get(0).expect("row 0"), 7);
        assert_eq!(trailing.get(1).expect("row 1"), 8);
    }

    /// Array(String) inner strings (including empty strings) are skipped with
    /// the same recursion the streaming reader uses.
    #[test]
    fn decompressed_array_string_with_empty_string_elements() {
        let inner = [string_value("a"), string_value("")].concat();
        let block = parse_decompressed_block(decompressed_block_buf(
            2,
            &[
                ("a", "Array(String)", [offsets(&[1, 2]), inner].concat()),
                ("x", "UInt8", vec![42, 43]),
            ],
        ))
        .expect("parse block");

        let col = block.column::<Vec<String>>("a").expect("read array column");
        assert_eq!(col.get(0).expect("row 0"), vec!["a".to_string()]);
        assert_eq!(col.get(1).expect("row 1"), vec![String::new()]);
        assert_eq!(
            block
                .column::<u8>("x")
                .expect("trailing column")
                .get(1)
                .expect("row 1"),
            43
        );
    }

    /// Map is Array(Tuple(K, V)): offsets first, then the key column and the
    /// value column, each with the last-offset row count.
    #[test]
    fn decompressed_map_offsets_then_keys_and_values() {
        let body = [
            offsets(&[1]),     // one map entry
            string_value("k"), // key column: 1 row
            vec![7u8],         // value column: 1 row
        ]
        .concat();
        let block = parse_decompressed_block(decompressed_block_buf(
            1,
            &[
                ("m", "Map(String, UInt8)", body),
                ("s", "String", string_value("tail")),
            ],
        ))
        .expect("parse block");

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

    /// A materialized JSON column starts with an 8-byte string-serialization
    /// version. The skip must consume it, and the sliced column data must
    /// exclude it so the string decoder sees only the rows.
    #[test]
    fn decompressed_json_version_prefix_consumed_and_stripped() {
        let body = [
            1u64.to_le_bytes().to_vec(), // string serialization version 1
            string_value(r#"{"x":1}"#),
            string_value(r#"{"y":2}"#),
        ]
        .concat();
        let block = parse_decompressed_block(decompressed_block_buf(
            2,
            &[("j", "JSON", body), ("x", "UInt8", vec![5, 6])],
        ))
        .expect("parse block");

        let expected_data = [string_value(r#"{"x":1}"#), string_value(r#"{"y":2}"#)].concat();
        let json_info = block
            .columns
            .iter()
            .find(|c| c.name == "j")
            .expect("json col");
        assert_eq!(&json_info.data[..], &expected_data[..]);

        let col = block
            .column::<crate::column::JsonValue>("j")
            .expect("read json column");
        assert_eq!(col.get(0).expect("row 0").as_str(), r#"{"x":1}"#);
        assert_eq!(col.get(1).expect("row 1").as_str(), r#"{"y":2}"#);
        assert_eq!(
            block
                .column::<u8>("x")
                .expect("trailing column")
                .get(1)
                .expect("row 1"),
            6
        );
    }

    /// JSON versions other than 1/4 are rejected with the same guidance as
    /// the streaming materialized reader instead of misframing the block.
    #[test]
    fn decompressed_json_unsupported_version_is_error() {
        let body = [2u64.to_le_bytes().to_vec(), string_value("x")].concat();
        let err = parse_decompressed_block(decompressed_block_buf(1, &[("j", "JSON", body)]))
            .err()
            .expect("unsupported JSON version must be rejected");
        assert!(
            matches!(&err, crate::error::Error::Protocol(msg) if
                msg.contains("materialized JSON reads require string serialization version 1 or 4")),
            "unexpected error: {err:?}"
        );
    }

    /// LowCardinality carries a 24-byte header, the dictionary column, an
    /// 8-byte index count equal to the row count, and the index bytes. The
    /// buffered parser materializes it like the streaming reader does.
    #[test]
    fn decompressed_lowcardinality_materializes() {
        let mut meta = Vec::new();
        meta.extend_from_slice(&1u64.to_le_bytes()); // key serialization version
        meta.extend_from_slice(&(1u64 << 9).to_le_bytes()); // additional keys, 1-byte indexes
        meta.extend_from_slice(&2u64.to_le_bytes()); // dictionary size
        let body = [
            meta,
            string_value("a"),
            string_value("b"),
            3u64.to_le_bytes().to_vec(), // index count == rows
            vec![0, 1, 0],               // indexes
        ]
        .concat();
        let block = parse_decompressed_block(decompressed_block_buf(
            3,
            &[
                ("lc", "LowCardinality(String)", body),
                ("x", "UInt8", vec![9, 10, 11]),
            ],
        ))
        .expect("parse block");

        let col = block.column::<String>("lc").expect("read lc column");
        assert_eq!(col.get(0).expect("row 0"), "a");
        assert_eq!(col.get(1).expect("row 1"), "b");
        assert_eq!(col.get(2).expect("row 2"), "a");
        assert_eq!(
            block
                .column::<u8>("x")
                .expect("trailing column")
                .get(2)
                .expect("row 2"),
            11
        );
    }

    /// Variant(UInt8, String): mode 0 body = per-row discriminators plus the
    /// non-empty subcolumns in type order. The trailing column must parse.
    #[test]
    fn decompressed_variant_subcolumns_frame_trailing_column() {
        let body = [
            0u64.to_le_bytes().to_vec(), // BASIC mode
            vec![0, 1],                  // row 0 -> UInt8, row 1 -> String
            vec![5u8],                   // UInt8 subcolumn: 1 value
            string_value("x"),           // String subcolumn: 1 value
        ]
        .concat();
        let block = parse_decompressed_block(decompressed_block_buf(
            2,
            &[
                ("v", "Variant(UInt8, String)", body),
                ("t", "UInt8", vec![3, 4]),
            ],
        ))
        .expect("parse block");

        assert_eq!(
            block
                .columns
                .iter()
                .find(|c| c.name == "v")
                .expect("variant col")
                .data
                .len(),
            8 + 2 + 1 + 2
        );
        let trailing = block.column::<u8>("t").expect("read trailing column");
        assert_eq!(trailing.get(0).expect("row 0"), 3);
        assert_eq!(trailing.get(1).expect("row 1"), 4);
    }

    /// Dynamic flattened (version 2) body: fixed-width discriminators where
    /// the type count itself marks NULL, then the counted subcolumn.
    #[test]
    fn decompressed_dynamic_flattened_frames_trailing_column() {
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
        let block = parse_decompressed_block(decompressed_block_buf(
            2,
            &[("d", "Dynamic", body), ("t", "UInt8", vec![6, 7])],
        ))
        .expect("parse block");

        let trailing = block.column::<u8>("t").expect("read trailing column");
        assert_eq!(trailing.get(0).expect("row 0"), 6);
        assert_eq!(trailing.get(1).expect("row 1"), 7);
    }

    /// AggregateFunction columns have no supported wire layout; the buffered
    /// parser must reject them instead of silently misframing later columns.
    #[test]
    fn decompressed_aggregate_function_is_rejected() {
        let err = parse_decompressed_block(decompressed_block_buf(
            1,
            &[("a", "AggregateFunction(any, UInt8)", vec![0; 4])],
        ))
        .err()
        .expect("AggregateFunction must be rejected");
        assert!(
            matches!(&err, crate::error::Error::Protocol(msg) if
                msg.contains("not supported in buffered block reads")),
            "unexpected error: {err:?}"
        );
    }

    /// Offsets are cumulative prefix sums; a decreasing pair is a protocol
    /// error, not a silently misframed block.
    #[test]
    fn decompressed_array_non_monotonic_offsets_rejected() {
        let err = parse_decompressed_block(decompressed_block_buf(
            2,
            &[("a", "Array(UInt8)", offsets(&[3, 2]))],
        ))
        .err()
        .expect("non-monotonic offsets must be rejected");
        assert!(
            matches!(&err, crate::error::Error::Protocol(msg) if msg.contains("array offset")),
            "unexpected error: {err:?}"
        );
    }

    /// The discard path shares the skip framing; it reports the row count of
    /// the discarded block.
    #[test]
    fn discard_decompressed_block_uses_same_array_framing() {
        let data = offsets(&[2, 2, 3]);
        let buf = decompressed_block_buf(
            3,
            &[
                ("a", "Array(UInt8)", [data, vec![1, 2, 9]].concat()),
                ("x", "UInt8", vec![7, 8, 9]),
            ],
        );
        let rows = discard_decompressed_block(&buf).expect("discard block");
        assert_eq!(rows, 3);
    }

    /// An LZ4 frame header (method byte matched) whose compressed_size is
    /// u32::MAX must be rejected by the frame cap BEFORE the body buffer is
    /// resized or read — the previous 1 GiB bound allowed a 4 GiB claim.
    #[tokio::test]
    async fn oversized_compressed_frame_rejected_before_body_read() {
        let (mut server, mut client) = crate::runtime::io::duplex(64);
        // 16 zero checksum bytes + method LZ4 + compressed_size u32::MAX +
        // uncompressed_size 0. No body follows.
        let mut wire = vec![0u8; 16];
        wire.push(0x82);
        wire.extend_from_slice(&u32::MAX.to_le_bytes());
        wire.extend_from_slice(&0u32.to_le_bytes());
        server
            .write_all(&wire)
            .await
            .expect("send hostile frame header");

        match read_compressed_payload_or_plain_prefix(&mut client).await {
            Err(err) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("exceeds") && msg.contains("frame cap"),
                    "expected frame cap error, got: {msg}"
                );
            },
            Ok(BlockPayload::PlainPrefix(p)) => {
                unreachable!(
                    "oversized frame must not fall back to plain, got {} bytes",
                    p.len()
                )
            },
            Ok(BlockPayload::Compressed(_)) => {
                unreachable!("oversized frame must be rejected before body read/allocation")
            },
        }
    }

    /// A well-formed frame below the cap still decompresses through the
    /// block reader after the cap was added.
    #[tokio::test]
    async fn valid_compressed_frame_still_decodes() {
        let payload = b"block-body-bytes".to_vec();
        let frame =
            crate::compression::encode_frame(&payload, crate::compression::CompressionMethod::None)
                .expect("encode test frame");
        let (mut server, mut client) = crate::runtime::io::duplex(64);
        server.write_all(&frame).await.expect("send valid frame");

        match read_compressed_payload_or_plain_prefix(&mut client).await {
            Ok(BlockPayload::Compressed(bytes)) => assert_eq!(&bytes[..], &payload[..]),
            Ok(BlockPayload::PlainPrefix(p)) => {
                unreachable!(
                    "expected compressed payload, got plain prefix of {} bytes",
                    p.len()
                )
            },
            Err(e) => unreachable!("expected compressed payload, got error: {e}"),
        }
    }

    /// The plain-payload fallback survives the cap change: a body whose
    /// 17th byte is not a compression method byte is still returned as a
    /// plain prefix instead of being treated as a compressed frame.
    #[tokio::test]
    async fn non_compressed_prefix_still_falls_back_to_plain() {
        let wire = vec![0x01u8; 17];
        let (mut server, mut client) = crate::runtime::io::duplex(64);
        server.write_all(&wire).await.expect("send plain prefix");

        match read_compressed_payload_or_plain_prefix(&mut client).await {
            Ok(BlockPayload::PlainPrefix(prefix)) => assert_eq!(prefix.len(), 17),
            Ok(BlockPayload::Compressed(_)) => {
                unreachable!("plain prefix must not be decoded as a compressed frame")
            },
            Err(e) => unreachable!("expected plain prefix, got error: {e}"),
        }
    }

    #[test]
    fn skip_col_typed_date32_advances_four_bytes_per_row() {
        // Date32 is Int32 days: 3 rows occupy 12 bytes. A 2-byte stride
        // would leave the stream desynced for every later column.
        let mut data = Vec::new();
        for v in [-1i32, 0, 19_000] {
            data.extend_from_slice(&v.to_le_bytes());
        }
        data.push(7); // one UInt8 row appended after the Date32 column
        let mut pos = 0;
        skip_col_typed(&data, &mut pos, "Date32", 3).expect("skip Date32 column");
        assert_eq!(pos, 12, "Date32 must advance 4 bytes per row");
        skip_col_typed(&data, &mut pos, "UInt8", 1).expect("skip trailing UInt8 column");
        assert_eq!(pos, 13);
    }

    // ── Server-controlled count cap tests ────────────────────────────────

    fn block_body_bytes(cols: u64, rows: u64) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(0x00); // BlockInfo: field 0 terminates the info loop
        crate::protocol::wire::write_varint(&mut data, cols).expect("test write");
        crate::protocol::wire::write_varint(&mut data, rows).expect("test write");
        data
    }

    #[test]
    fn decompressed_block_column_count_u64_max_is_protocol_error() {
        let err = parse_decompressed_block(bytes::Bytes::from(block_body_bytes(u64::MAX, 0)))
            .err()
            .expect("u64::MAX column count must be rejected");
        match &err {
            crate::error::Error::Protocol(msg) => assert_eq!(
                msg,
                "block column count 18446744073709551615 exceeds limit 65536"
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn decompressed_block_row_count_u64_max_is_protocol_error() {
        let err = parse_decompressed_block(bytes::Bytes::from(block_body_bytes(0, u64::MAX)))
            .err()
            .expect("u64::MAX row count must be rejected");
        assert!(
            matches!(err, crate::error::Error::Protocol(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn decompressed_block_row_count_cap_plus_one_is_protocol_error() {
        let err = parse_decompressed_block(bytes::Bytes::from(block_body_bytes(0, 10_000_001)))
            .err()
            .expect("cap + 1 row count must be rejected");
        match &err {
            crate::error::Error::Protocol(msg) => {
                assert_eq!(msg, "block row count 10000001 exceeds limit 10000000")
            },
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn discard_decompressed_block_rejects_oversized_dimensions() {
        for (cols, rows, expected) in [
            (u64::MAX, 0, "block column count"),
            (0, u64::MAX, "block row count"),
        ] {
            let err = discard_decompressed_block(&block_body_bytes(cols, rows))
                .expect_err("discard path must reject an oversized dimension");
            assert!(
                matches!(&err, crate::error::Error::Protocol(msg) if msg.contains(expected)),
                "expected {expected} Protocol error, got {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn discard_low_cardinality_rejects_oversized_key_count() {
        let (mut tx, mut rx) = crate::runtime::io::duplex(64);
        let mut meta = Vec::with_capacity(24);
        meta.extend_from_slice(&1u64.to_le_bytes()); // serialization version
        meta.extend_from_slice(&(1u64 << 9).to_le_bytes()); // additional keys, UInt8 indexes
        meta.extend_from_slice(&u64::MAX.to_le_bytes());
        tx.write_all(&meta)
            .await
            .expect("seed LowCardinality header");

        let err = discard_lc_async(
            &mut rx,
            &crate::protocol::type_parser::ColumnType::String,
            1,
        )
        .await
        .expect_err("discard path must cap dictionary keys");
        assert!(
            matches!(&err, crate::error::Error::Protocol(msg) if msg == "LowCardinality key count 18446744073709551615 exceeds limit 65536"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn decompressed_block_row_cap_boundary_parses() {
        // Exactly MAX_BLOCK_ROWS with zero columns allocates nothing and
        // must still parse: the cap bounds a single block, not totals.
        let block = parse_decompressed_block(bytes::Bytes::from(block_body_bytes(
            0,
            crate::limits::MAX_BLOCK_ROWS as u64,
        )))
        .expect("row count at the cap parses");
        assert_eq!(block.rows, crate::limits::MAX_BLOCK_ROWS);
        assert!(block.columns.is_empty());
    }

    #[tokio::test]
    async fn streamed_block_column_count_u64_max_is_protocol_error() {
        // read_data_block: plain (uncompressed) streaming path.
        let mut wire_bytes = Vec::new();
        crate::protocol::wire::write_varint(&mut wire_bytes, 0).expect("test write"); // table
        wire_bytes.extend_from_slice(&block_body_bytes(u64::MAX, 0));
        let (mut tx, mut rx) = tokio::io::duplex(64);
        tx.write_all(&wire_bytes).await.expect("seed block");

        let err = read_data_block(&mut rx)
            .await
            .err()
            .expect("u64::MAX column count must be rejected");
        assert!(
            matches!(err, crate::error::Error::Protocol(_)),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn streamed_block_body_row_count_cap_plus_one_is_protocol_error() {
        // read_data_block_body: the plain-prefix body path shared by the
        // compressed reader when the payload is not a compression frame.
        let wire_bytes = block_body_bytes(0, 10_000_001);
        let (mut tx, mut rx) = tokio::io::duplex(64);
        tx.write_all(&wire_bytes).await.expect("seed block body");

        let err = read_data_block_body(&mut rx)
            .await
            .err()
            .expect("cap + 1 row count must be rejected");
        match &err {
            crate::error::Error::Protocol(msg) => {
                assert_eq!(msg, "block row count 10000001 exceeds limit 10000000")
            },
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    // ── Server-controlled byte-length cap tests ───────────────────────────

    #[tokio::test]
    async fn discard_string_value_length_u64_max_is_rejected() {
        let mut wire_bytes = Vec::new();
        wire::write_varint(&mut wire_bytes, 0).expect("test write"); // table
        wire_bytes.push(0x00); // BlockInfo terminator
        wire::write_varint(&mut wire_bytes, 1).expect("test write"); // columns
        wire::write_varint(&mut wire_bytes, 1).expect("test write"); // rows
        wire::write_string(&mut wire_bytes, "c").expect("test write");
        wire::write_string(&mut wire_bytes, "String").expect("test write");
        wire_bytes.push(0); // custom serialization
        wire::write_varint(&mut wire_bytes, u64::MAX).expect("test write");
        let (mut tx, mut rx) = tokio::io::duplex(64);
        tx.write_all(&wire_bytes).await.expect("seed block");

        let err = discard_data_block(&mut rx)
            .await
            .expect_err("discard path must reject a lying string length");
        assert!(
            matches!(&err, crate::error::Error::Protocol(msg) if msg == "string value length 18446744073709551615 exceeds limit 16777215"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn materialized_low_cardinality_index_buffer_cap_is_enforced() {
        // rows = 8,388,609 with 8-byte indexes claims 64 MiB + 8 bytes of
        // index buffer; the cap must fire before the vec is allocated.
        let rows = crate::limits::MAX_COLUMN_BYTES / 8 + 1;
        let mut wire_bytes = Vec::new();
        wire_bytes.extend_from_slice(&1u64.to_le_bytes()); // key serialization version
        wire_bytes.extend_from_slice(&((1u64 << 9) | 3).to_le_bytes()); // 8-byte indexes
        wire_bytes.extend_from_slice(&0u64.to_le_bytes()); // no dictionary keys
        wire_bytes.extend_from_slice(&(rows as u64).to_le_bytes()); // indexes == rows
        let (mut tx, mut rx) = tokio::io::duplex(64);
        tx.write_all(&wire_bytes).await.expect("seed column");

        let err = read_column_async(&mut rx, "LowCardinality(String)", rows)
            .await
            .expect_err("oversized LowCardinality index buffer must be rejected");
        assert!(
            matches!(&err, crate::error::Error::Protocol(msg) if msg == "LowCardinality index byte length 67108872 exceeds limit 67108864"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn materialized_variant_compact_rows_claim_is_rejected() {
        let mut wire_bytes = Vec::new();
        wire_bytes.extend_from_slice(&1u64.to_le_bytes()); // mode = COMPACT
        wire_bytes.extend_from_slice(&0u64.to_le_bytes()); // discriminator (UInt8)
        wire_bytes.extend_from_slice(&(1u64 << 60).to_le_bytes()); // rows claim
        let (mut tx, mut rx) = tokio::io::duplex(64);
        tx.write_all(&wire_bytes).await.expect("seed column");

        let err = read_column_async(&mut rx, "Variant(UInt8, String)", 1)
            .await
            .expect_err("huge Variant compact rows claim must be rejected");
        assert!(
            matches!(&err, crate::error::Error::Protocol(msg) if msg == "Variant compact rows 1152921504606846976 exceeds row count 1"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn fixed_width_cap_plus_one_is_rejected_before_allocation() {
        // UInt256 rows*32 = 64 MiB + 32 must fail before the buffer allocates.
        let rows = crate::limits::MAX_COLUMN_BYTES / 32 + 1;
        let (_tx, mut rx) = tokio::io::duplex(64);
        let err = read_column_async(&mut rx, "UInt256", rows)
            .await
            .expect_err("fixed-width cap + 1 must be rejected");
        assert!(
            matches!(&err, crate::error::Error::Protocol(msg) if msg == "fixed-width column byte length 67108896 exceeds limit 67108864"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn parse_decompressed_block_keeps_columns_after_date32_in_sync() {
        // One Date32 column followed by one UInt8 column. The Date32
        // stride decides whether the second column's header lands on bytes
        // or on garbage.
        let mut data = Vec::new();
        data.push(0x00); // BlockInfo: field 0 terminates the info loop
        data.push(2); // columns
        data.push(1); // rows
        // column 1: name "d", type "Date32", custom serialization 0, Int32 payload
        data.extend_from_slice(&[1, b'd']);
        data.extend_from_slice(&[6]);
        data.extend_from_slice(b"Date32");
        data.push(0);
        data.extend_from_slice(&19_000i32.to_le_bytes());
        // column 2: name "f", type "UInt8", custom serialization 0, one byte
        data.extend_from_slice(&[1, b'f']);
        data.extend_from_slice(&[5]);
        data.extend_from_slice(b"UInt8");
        data.push(0);
        data.push(7);

        let block =
            parse_decompressed_block(bytes::Bytes::from(data)).expect("block must parse in sync");
        assert_eq!(block.rows, 1);
        assert_eq!(block.columns.len(), 2);
        assert_eq!(block.columns[0].name, "d");
        assert_eq!(block.columns[1].name, "f");
        assert_eq!(&block.columns[1].data[..], &[7]);
    }
}
