use crate::connection::io::{
    checked_len, checked_usize, encode_varint, lc_idx_width, read_block_header,
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

#[allow(dead_code)]
fn skip_block_body(data: &[u8], pos: &mut usize) -> Result<()> {
    loop {
        let d = parse_varint(data, pos)?;
        match d {
            0 => break,
            1 => advance_pos(data, pos, 1)?,
            2 => advance_pos(data, pos, 4)?,
            3 => {
                parse_varint(data, pos)?;
            },
            _ => break,
        }
    }
    let _cols = parse_varint(data, pos)?;
    let rows = parse_varint(data, pos)? as usize;
    for _ in 0.._cols {
        let _name = parse_bytes(data, pos)?;
        let tn_bytes = parse_bytes(data, pos)?;
        let tn = std::str::from_utf8(tn_bytes).unwrap_or("");
        advance_pos(data, pos, 1)?;
        if rows > 0 {
            skip_col_typed(data, pos, tn, rows)?;
        }
    }
    Ok(())
}

fn skip_col_typed(data: &[u8], pos: &mut usize, tn: &str, rows: usize) -> Result<()> {
    if rows == 0 {
        return Ok(());
    }

    let ct = match type_parser::parse_type(tn) {
        Ok(c) => c,
        Err(_) => {
            for _ in 0..rows {
                let l = usize::try_from(parse_varint(data, pos)?)
                    .map_err(|_| crate::error::Error::Protocol("string too large".into()))?;
                advance_pos(data, pos, l)?;
            }
            return Ok(());
        },
    };
    use type_parser::ColumnType::*;
    match &ct {
        UInt8 | Int8 | Bool | Enum8 => {
            advance_pos(data, pos, rows)?;
            Ok(())
        },
        UInt16 | Int16 | Date | Date32 | Enum16 => {
            advance_pos(data, pos, checked_len(rows, 2)?)?;
            Ok(())
        },
        UInt32 | Int32 | Float32 | DateTime | Time | IPv4 => {
            advance_pos(data, pos, checked_len(rows, 4)?)?;
            Ok(())
        },
        UInt64 | Int64 | Float64 | DateTime64(_) | Time64(_) => {
            advance_pos(data, pos, checked_len(rows, 8)?)?;
            Ok(())
        },
        UInt128 | Int128 | UUID | IPv6 => {
            advance_pos(data, pos, checked_len(rows, 16)?)?;
            Ok(())
        },
        UInt256 | Int256 => {
            advance_pos(data, pos, checked_len(rows, 32)?)?;
            Ok(())
        },
        FixedString(n) => {
            advance_pos(data, pos, checked_len(rows, *n)?)?;
            Ok(())
        },
        Decimal(1..=9, _) => {
            advance_pos(data, pos, checked_len(rows, 4)?)?;
            Ok(())
        },
        Decimal(10..=18, _) => {
            advance_pos(data, pos, checked_len(rows, 8)?)?;
            Ok(())
        },
        Decimal(19..=38, _) => {
            advance_pos(data, pos, checked_len(rows, 16)?)?;
            Ok(())
        },
        Decimal(39..=76, _) => {
            advance_pos(data, pos, checked_len(rows, 32)?)?;
            Ok(())
        },
        Decimal(_, _)
        | String
        | JSON
        | Dynamic
        | AggregateFunction
        | SimpleAggregateFunction
        | Other(_) => {
            for _ in 0..rows {
                if *pos >= data.len() {
                    break;
                }
                let l = usize::try_from(parse_varint(data, pos)?)
                    .map_err(|_| crate::error::Error::Protocol("string too large".into()))?;
                advance_pos(data, pos, l)?;
            }
            Ok(())
        },
        Nothing => {
            advance_pos(data, pos, rows)?;
            Ok(())
        },
        Nullable(inner) => {
            advance_pos(data, pos, rows)?;
            skip_col_typed(data, pos, &inner.to_string(), rows)
        },
        Array(_inner) => {
            for _ in 0..rows {
                if *pos >= data.len() {
                    break;
                }
                let _off = parse_varint(data, pos)?;
            }
            Ok(())
        },
        Map(_k, _v) => {
            for _ in 0..rows {
                if *pos >= data.len() {
                    break;
                }
                let _off = parse_varint(data, pos)?;
            }
            Ok(())
        },
        Tuple(elems) => {
            for elem in elems {
                skip_col_typed(data, pos, &elem.to_string(), rows)?;
            }
            Ok(())
        },
        Point => {
            skip_col_typed(data, pos, "Float64", rows)?;
            skip_col_typed(data, pos, "Float64", rows)
        },
        Ring => skip_col_typed(data, pos, "Array(Point)", rows),
        Polygon => skip_col_typed(data, pos, "Array(Ring)", rows),
        MultiPolygon => skip_col_typed(data, pos, "Array(Polygon)", rows),
        LowCardinality(inner) => skip_col_typed(data, pos, &inner.to_string(), rows),
        Variant(_types) => {
            // Skip mode (8 bytes)
            if (*pos).checked_add(8).is_none_or(|end| end > data.len()) {
                return Ok(());
            }
            advance_pos(data, pos, 8)?;
            // For simplicity, skip all remaining data
            // (correct parsing would require reading discriminators + sub-columns)
            *pos = data.len();
            Ok(())
        },
    }
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
    let cols = checked_usize(read_varint_async(stream).await?, "columns")?;
    let rows = checked_usize(read_varint_async(stream).await?, "rows")?;
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
    let cols = checked_usize(read_varint_async(stream).await?, "columns")?;
    let rows = checked_usize(read_varint_async(stream).await?, "rows")?;
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
    const MAX_COMPRESSED_BLOCK_SIZE: usize = 1 << 30;

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
    if !(COMPRESSED_BODY_HEADER_LEN..=MAX_COMPRESSED_BLOCK_SIZE).contains(&compressed_size) {
        return Ok(BlockPayload::PlainPrefix(frame));
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
    let cols = checked_usize(parse_varint(&shared, &mut pos)?, "columns")?;
    let rows = checked_usize(parse_varint(&shared, &mut pos)?, "rows")?;
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
        let start = pos;
        skip_col_typed(&shared, &mut pos, &type_name, rows)?;
        columns.push(ColumnInfo {
            name,
            type_name,
            data: shared.slice(start..pos),
            lc_materialized: bytes::Bytes::new(),
        });
    }
    Ok(Block { columns, rows })
}

fn discard_decompressed_block(shared: &[u8]) -> Result<usize> {
    let mut pos = 0usize;
    parse_block_info(shared, &mut pos)?;
    let cols = checked_usize(parse_varint(shared, &mut pos)?, "columns")?;
    let rows = checked_usize(parse_varint(shared, &mut pos)?, "rows")?;
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
            for _ in 0..rows {
                let l = checked_usize(read_varint_async(stream).await?, "string value length")?;
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
            read_column_raw_recorded(stream, type_name, rows, &mut data).await?;
            Ok(data)
        },
        AggregateFunction | SimpleAggregateFunction => Err(crate::error::Error::Protocol(
            "AggregateFunction type not yet supported in wire reader".into(),
        )),
        Variant(types) => {
            let mut data = Vec::new();
            let state = read_column_state_prefix_recorded(stream, &ct, &mut data).await?;
            read_variant_body_raw_recorded(stream, types, variant_states(&state), rows, &mut data)
                .await?;
            Ok(data)
        },
        String | Other(_) => {
            read_string_column_with_prefixes(stream, rows, "string value length").await
        },
        FixedString(n) => {
            let mut data = vec![0u8; checked_len(rows, *n)?];
            stream.read_exact(&mut data).await?;
            Ok(data)
        },
        _ => {
            let w = ct
                .fixed_width()
                .ok_or_else(|| crate::error::Error::Protocol(format!("unknown type {ct}")))?;
            let mut data = vec![0u8; checked_len(rows, w)?];
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
                let len = checked_usize(read_varint_async(stream).await?, "string value length")?;
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
                let len = checked_usize(read_varint_async(stream).await?, "string value length")?;
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
                let len = checked_usize(read_varint_async(stream).await?, "JSON string length")?;
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
    let num_keys = checked_usize(
        u64::from_le_bytes(meta[16..24].try_into().map_err(|_| {
            crate::error::Error::Protocol("LowCardinality key count length mismatch".into())
        })?),
        "LowCardinality keys",
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
    discard_exact_async(stream, checked_len(indexes, idx_width)?).await
}

async fn discard_offsets_async<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, rows: usize, name: &str,
) -> Result<usize> {
    let mut offset = [0u8; 8];
    for _ in 0..rows {
        stream.read_exact(&mut offset).await?;
    }
    checked_usize(u64::from_le_bytes(offset), name)
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
    let num_keys = u64::from_le_bytes(num_keys_bytes) as usize;
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
    let ni = u64::from_le_bytes(il) as usize;
    if ni != rows {
        return Err(crate::error::Error::Protocol(format!(
            "LowCardinality index count {ni} does not match row count {rows}"
        )));
    }
    let mut indexes = vec![0u8; checked_len(ni, idx_width)?];
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
    let mut r = 0u64;
    let mut s = 0;
    loop {
        if *pos >= data.len() {
            return Err(crate::error::Error::Protocol("eof".into()));
        }
        let b = data[*pos];
        *pos += 1;
        r |= ((b & 0x7F) as u64) << s;
        if b & 0x80 == 0 {
            return Ok(r);
        }
        s += 7;
        if s >= 64 {
            return Err(crate::error::Error::Protocol("varint overflow".into()));
        }
    }
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
