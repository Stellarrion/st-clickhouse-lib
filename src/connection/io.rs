use crate::client_info::{
    ClientInfoTemplate, build_client_info_template, write_client_info_from_template,
};
use crate::compression::CompressionMethod;
use crate::connection::callbacks::{Profile, Progress};
use crate::error::Result;
use crate::protocol::packet::ClientPacket;
use crate::protocol::revision;
use crate::protocol::wire;
use crate::runtime::io::{AsyncReadExt, AsyncWriteExt};
use crate::runtime::time::Instant;
use std::collections::HashMap;

#[inline]
pub(crate) fn compression_flag(compression: Option<CompressionMethod>) -> u64 {
    match compression {
        Some(CompressionMethod::Lz4 | CompressionMethod::Zstd) => 1,
        Some(CompressionMethod::None) | None => 0,
    }
}

/// Per-read timeout for a packet loop: the smaller of `recv_timeout` and the
/// time remaining until `deadline` (if set).
///
/// Returns `None` when the deadline has already elapsed — the caller must
/// treat the query as timed out (cancel + drain + `Error::Timeout`).
#[inline]
pub(crate) fn packet_read_timeout(
    recv_timeout: std::time::Duration, deadline: Option<Instant>,
) -> Option<std::time::Duration> {
    match deadline {
        None => Some(recv_timeout),
        Some(d) => {
            let remaining = d.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                None
            } else {
                Some(std::cmp::min(recv_timeout, remaining))
            }
        },
    }
}

/// Write the empty Data block marker used after query packets.
///
/// Uses the native BlockInfo format:
/// ```text
/// [varint(2)] Data packet
/// [string("")] table name
/// [BlockInfo]
///   dim=1 [1 byte] is_overflows = 0
///   dim=2 [4 bytes] bucket_num = -1
///   dim=0 [terminator]
/// [varint(0)] num_columns = 0
/// [varint(0)] num_rows = 0
/// ```
pub(crate) fn write_empty_block_body(buf: &mut Vec<u8>) {
    wire::write_varint_to_vec(buf, 1);
    buf.push(0);
    wire::write_varint_to_vec(buf, 2);
    buf.extend_from_slice(&(-1i32).to_le_bytes());
    wire::write_varint_to_vec(buf, 0);
    wire::write_varint_to_vec(buf, 0);
    wire::write_varint_to_vec(buf, 0);
}

pub(crate) fn write_empty_data(buf: &mut Vec<u8>) {
    wire::write_varint_to_vec(buf, 2);
    wire::write_string_to_vec(buf, "");
    write_empty_block_body(buf);
}

pub(crate) fn write_empty_data_for(buf: &mut Vec<u8>, compression: Option<CompressionMethod>) {
    match compression {
        Some(method @ (CompressionMethod::Lz4 | CompressionMethod::Zstd)) => {
            wire::write_varint_to_vec(buf, 2);
            wire::write_string_to_vec(buf, "");
            let mut block = Vec::with_capacity(10);
            write_empty_block_body(&mut block);
            match crate::compression::encode_frame(&block, method) {
                Ok(frame) => buf.extend_from_slice(&frame),
                Err(err) => {
                    debug_assert!(false, "failed to encode empty compressed block: {err}");
                },
            }
        },
        Some(CompressionMethod::None) | None => write_empty_data(buf),
    }
}

pub(crate) fn write_protocol_default_settings(
    buf: &mut Vec<u8>, settings: &HashMap<String, String>, rev: u64,
) {
    if rev >= revision::DBMS_MIN_REVISION_WITH_SPARSE_SERIALIZATION
        && !settings
            .contains_key(crate::protocol::settings::RATIO_OF_DEFAULTS_FOR_SPARSE_SERIALIZATION)
    {
        wire::write_string_to_vec(
            buf,
            crate::protocol::settings::RATIO_OF_DEFAULTS_FOR_SPARSE_SERIALIZATION,
        );
        wire::write_varint_to_vec(buf, 0);
        wire::write_string_to_vec(buf, "1");
    }
    if !settings.contains_key(crate::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING)
    {
        wire::write_string_to_vec(
            buf,
            crate::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING,
        );
        wire::write_varint_to_vec(buf, 0);
        wire::write_string_to_vec(buf, "1");
    }
}

pub(crate) fn merge_settings(
    base: &HashMap<String, String>, overrides: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut merged = base.clone();
    merged.extend(overrides.iter().map(|(k, v)| (k.clone(), v.clone())));
    merged
}

#[derive(Clone, Debug)]
pub(crate) struct QueryPacketCommonTemplate {
    pub(crate) prefix: Vec<u8>,
    pub(crate) client_info: Option<ClientInfoTemplate>,
    pub(crate) before_query: Vec<u8>,
}

pub(crate) fn build_query_packet_common_template(
    settings: &HashMap<String, String>, compression: Option<CompressionMethod>, rev: u64,
    quota_key: &str,
) -> QueryPacketCommonTemplate {
    let mut prefix = Vec::with_capacity(4);
    wire::write_varint_to_vec(&mut prefix, 1); // ClientCode::Query

    let client_info = (rev >= revision::DBMS_MIN_REVISION_WITH_CLIENT_INFO)
        .then(|| build_client_info_template(rev, quota_key));

    let mut before_query = Vec::with_capacity(256);
    write_protocol_default_settings(&mut before_query, settings, rev);
    for (name, value) in settings {
        wire::write_string_to_vec(&mut before_query, name);
        wire::write_varint_to_vec(&mut before_query, 0);
        wire::write_string_to_vec(&mut before_query, value);
    }
    wire::write_string_to_vec(&mut before_query, ""); // settings terminator
    if rev >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES {
        wire::write_string_to_vec(&mut before_query, ""); // externally granted roles
    }
    if rev >= revision::DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
        wire::write_string_to_vec(&mut before_query, ""); // inter-server secret
    }
    wire::write_varint_to_vec(&mut before_query, 2); // stage = Complete
    wire::write_varint_to_vec(&mut before_query, compression_flag(compression));

    QueryPacketCommonTemplate {
        prefix,
        client_info,
        before_query,
    }
}

#[inline]
pub(crate) fn query_packet_common_fixed_capacity(template: &QueryPacketCommonTemplate) -> usize {
    let client_info_len = template
        .client_info
        .as_ref()
        .map(|info| info.before_initial_query_id.len() + info.after_initial_query_id.len() + 1)
        .unwrap_or(0);
    template.prefix.len() + 1 + client_info_len + template.before_query.len()
}

pub(crate) fn write_query_packet_common_from_template(
    buf: &mut Vec<u8>, template: &QueryPacketCommonTemplate, query_id: &[u8],
) {
    buf.extend_from_slice(&template.prefix);
    wire::write_string_bytes_to_vec(buf, query_id);
    if let Some(client_info) = &template.client_info {
        wire::write_varint_to_vec(buf, 1); // query_kind = INITIAL_QUERY
        write_client_info_from_template(buf, client_info, query_id);
    }
    buf.extend_from_slice(&template.before_query);
}

pub(crate) async fn ping_stream(stream: &mut crate::pool::StreamWrapper) -> Result<()> {
    stream.write_packet(&[ClientPacket::Ping as u8]).await?;
    stream.flush().await?;
    let mut pkt = [0u8; 1];
    stream.read_exact(&mut pkt).await?;
    if pkt[0] != 4 {
        return Err(crate::error::Error::Protocol(format!(
            "expected Pong (4), got {}",
            pkt[0]
        )));
    }
    Ok(())
}

pub(crate) async fn read_progress_packet<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<Progress> {
    let rows = read_varint_async(stream).await?;
    let bytes = read_varint_async(stream).await?;
    let total_rows = read_varint_async(stream).await?;
    let (written_rows, written_bytes) = if revision::DEFAULT_PROTOCOL_REVISION
        >= revision::DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO
    {
        (
            read_varint_async(stream).await?,
            read_varint_async(stream).await?,
        )
    } else {
        (0, 0)
    };
    if revision::DEFAULT_PROTOCOL_REVISION
        >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS
    {
        let _elapsed_ns = read_varint_async(stream).await?;
    }
    if revision::DEFAULT_PROTOCOL_REVISION
        >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS
    {
        let _total_bytes_to_read = read_varint_async(stream).await?;
    }
    Ok(Progress {
        rows,
        bytes,
        total_rows,
        written_rows,
        written_bytes,
    })
}

pub(crate) async fn read_profile_info_packet<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<Profile> {
    let rows = read_varint_async(stream).await?;
    let blocks = read_varint_async(stream).await?;
    let bytes = read_varint_async(stream).await?;
    let mut flag = [0u8; 1];
    stream.read_exact(&mut flag).await?;
    let applied_limit = flag[0] != 0;
    let rows_before_limit = read_varint_async(stream).await?;
    stream.read_exact(&mut flag).await?;
    let calculated_rows_before_limit = flag[0] != 0;
    if revision::DEFAULT_PROTOCOL_REVISION
        >= revision::DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION
    {
        stream.read_exact(&mut flag).await?;
        let _applied_aggregation = flag[0] != 0;
        let _rows_before_aggregation = read_varint_async(stream).await?;
    }
    Ok(Profile {
        rows,
        blocks,
        bytes,
        rows_before_limit,
        applied_limit,
        calculated_rows_before_limit,
    })
}

pub(crate) async fn skip_part_uuids_packet<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<()> {
    let _ = read_part_uuids_packet(stream).await?;
    Ok(())
}

pub(crate) async fn read_part_uuids_packet<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<Vec<[u8; 16]>> {
    let count = checked_count(
        read_varint_async(stream).await?,
        "PartUUID",
        crate::limits::MAX_PART_UUIDS,
    )?;
    let mut uuids = Vec::with_capacity(count);
    for _ in 0..count {
        let mut uuid = [0u8; 16];
        stream.read_exact(&mut uuid).await?;
        uuids.push(uuid);
    }
    Ok(uuids)
}

pub(crate) async fn read_tables_status_response<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, protocol_revision: u64,
) -> Result<crate::protocol::table_status::TablesStatusResponse> {
    if protocol_revision < revision::DBMS_MIN_REVISION_WITH_TABLES_STATUS {
        return Err(crate::error::Error::Protocol(format!(
            "TablesStatus requires protocol revision >= {}",
            revision::DBMS_MIN_REVISION_WITH_TABLES_STATUS
        )));
    }
    let count = checked_usize(read_varint_async(stream).await?, "TablesStatus")?;
    if count > 0x00FF_FFFF {
        return Err(crate::error::Error::Protocol(format!(
            "TablesStatus entry count {count} exceeds limit 16777215"
        )));
    }
    let mut table_states_by_id = std::collections::HashMap::with_capacity(count);
    for _ in 0..count {
        let database = read_string_async(stream).await?;
        let table = read_string_async(stream).await?;
        let status = read_table_status(stream, protocol_revision).await?;
        table_states_by_id.insert(
            crate::protocol::table_status::QualifiedTableName::new(database, table),
            status,
        );
    }
    Ok(crate::protocol::table_status::TablesStatusResponse { table_states_by_id })
}

async fn read_table_status<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, protocol_revision: u64,
) -> Result<crate::protocol::table_status::TableStatus> {
    let is_replicated = read_u8_async(stream).await? != 0;
    if !is_replicated {
        return Ok(crate::protocol::table_status::TableStatus::default());
    }
    let absolute_delay = read_varint_async(stream).await?;
    let is_readonly = if protocol_revision >= revision::DBMS_MIN_REVISION_WITH_TABLE_READ_ONLY_CHECK
    {
        read_varint_async(stream).await? != 0
    } else {
        false
    };
    Ok(crate::protocol::table_status::TableStatus {
        is_replicated,
        absolute_delay,
        is_readonly,
    })
}

pub(crate) async fn skip_parallel_read_announcement<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<()> {
    let _version = read_u64_le_async(stream).await?;
    let _mode = read_u8_async(stream).await?;
    skip_ranges_in_data_parts_description(
        stream,
        revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION,
    )
    .await?;
    let _replica_num = read_u64_le_async(stream).await?;
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 4 {
        let _mark_segment_size = read_u64_le_async(stream).await?;
    }
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 6 {
        let _min_marks_per_request = read_varint_async(stream).await?;
    }
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 7 {
        let _stream_id = read_string_async(stream).await?;
    }
    Ok(())
}

pub(crate) async fn read_parallel_read_request_stream_id<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<String> {
    let _version = read_u64_le_async(stream).await?;
    let _mode = read_u8_async(stream).await?;
    let _replica_num = read_u64_le_async(stream).await?;
    let _min_marks_per_request = read_u64_le_async(stream).await?;
    skip_ranges_in_data_parts_description(
        stream,
        revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION,
    )
    .await?;
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 7 {
        read_string_async(stream).await
    } else {
        Ok(String::new())
    }
}

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/shared/read_task_macros.rs"
));
define_read_task_packet_builders!(pub(crate));

async fn skip_ranges_in_data_parts_description<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, parallel_replicas_protocol_version: u64,
) -> Result<()> {
    let count = checked_usize(
        read_varint_async(stream).await?,
        "parallel replica part ranges",
    )?;
    for _ in 0..count {
        skip_merge_tree_part_info(stream).await?;
        skip_mark_ranges(stream).await?;
        let _rows = read_varint_async(stream).await?;
        if parallel_replicas_protocol_version >= 5 {
            let _projection_name = read_string_async(stream).await?;
        }
        if parallel_replicas_protocol_version >= 6 {
            let _min_marks_per_task = read_varint_async(stream).await?;
        }
    }
    Ok(())
}

async fn skip_merge_tree_part_info<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<()> {
    let _version = read_u64_le_async(stream).await?;
    let _partition_id = read_string_async(stream).await?;
    let _min_block = read_u64_le_async(stream).await?;
    let _max_block = read_u64_le_async(stream).await?;
    let _level = read_u64_le_async(stream).await?;
    let _mutation = read_u64_le_async(stream).await?;
    let _use_legacy_max_level = read_u8_async(stream).await?;
    Ok(())
}

async fn skip_mark_ranges<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<()> {
    let count = checked_usize(
        read_u64_le_async(stream).await?,
        "parallel replica mark ranges",
    )?;
    discard_exact(stream, checked_len(count, 16)?).await
}

async fn read_u64_le_async<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<u64> {
    let mut buf = [0u8; 8];
    stream.read_exact(&mut buf).await?;
    Ok(u64::from_le_bytes(buf))
}

async fn read_u8_async<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<u8> {
    let mut buf = [0u8; 1];
    stream.read_exact(&mut buf).await?;
    Ok(buf[0])
}

/// Reads a small batch of bytes at once to reduce `.await` overhead.
/// Most varints are single-byte (<128), so this avoids the per-byte loop
/// in the common case while still handling multi-byte varints correctly.
pub(crate) async fn read_varint_async<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<u64> {
    let mut buf = [0u8; 10];
    stream.read_exact(&mut buf[..1]).await?;
    if buf[0] & 0x80 == 0 {
        return Ok(buf[0] as u64);
    }

    let mut result = (buf[0] & 0x7F) as u64;
    let mut shift = 7;
    loop {
        if shift >= 64 {
            return Err(crate::error::Error::Protocol("varint overflow".into()));
        }
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await?;
        let b = byte[0];
        if shift == 63 && (b & 0x7F) > 1 {
            return Err(crate::error::Error::Protocol("varint overflow".into()));
        }
        result |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
}

pub(crate) async fn read_string_async<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<String> {
    let len = checked_string_len(read_varint_async(stream).await?, "string length")?;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    String::from_utf8(buf).map_err(|e| crate::error::Error::Protocol(format!("utf8: {e}")))
}

pub(crate) async fn read_block_header<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<(usize, usize)> {
    let _table = read_string_async(stream).await?;
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
    let columns = checked_count(
        read_varint_async(stream).await?,
        "block column",
        crate::limits::MAX_BLOCK_COLUMNS,
    )?;
    let rows = checked_count(
        read_varint_async(stream).await?,
        "block row",
        crate::limits::MAX_BLOCK_ROWS,
    )?;
    Ok((columns, rows))
}

/// Values at most this size use the merged body read — the value's bytes plus
/// the next row's first varint byte in ONE `read_exact` — halving the per-row
/// poll count. Every byte of a merged read is already claimed by this column
/// (the lookahead byte is the next row's length varint's first byte, which
/// must exist while rows remain), so the read can never cross the column end
/// into the next column's stream bytes. Larger values stream straight into
/// the column buffer's tail, where the transport's large-read path serves
/// them directly.
const STRING_COL_MERGED_MAX: usize = 4096;

pub(crate) async fn read_string_column_with_prefixes<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, rows: usize, length_name: &str,
) -> Result<Vec<u8>> {
    // Capacity hint only: clamp it so a hostile row count cannot eager-allocate
    // more than one column's byte budget before any data arrives.
    let mut data = Vec::with_capacity(rows.saturating_mul(8).min(crate::limits::MAX_COLUMN_BYTES));
    if rows == 0 {
        return Ok(data);
    }
    let mut total = 0usize;
    let mut scratch = [0u8; STRING_COL_MERGED_MAX + 1];
    // First varint byte of the next row when the previous row's merged body
    // read already pulled it in.
    let mut pending: Option<u8> = None;
    for row_idx in 0..rows {
        // Length varint — same per-byte rules and overflow errors as
        // read_varint_async, starting from `pending` when available.
        let first = match pending.take() {
            Some(b) => b,
            None => {
                let mut byte = [0u8; 1];
                stream.read_exact(&mut byte).await?;
                byte[0]
            },
        };
        let value = if first & 0x80 == 0 {
            u64::from(first)
        } else {
            let mut result = u64::from(first & 0x7F);
            let mut shift = 7u32;
            loop {
                if shift >= 64 {
                    return Err(crate::error::Error::Protocol("varint overflow".into()));
                }
                let mut byte = [0u8; 1];
                stream.read_exact(&mut byte).await?;
                let b = byte[0];
                if shift == 63 && (b & 0x7F) > 1 {
                    return Err(crate::error::Error::Protocol("varint overflow".into()));
                }
                result |= u64::from(b & 0x7F) << shift;
                if b & 0x80 == 0 {
                    break;
                }
                shift += 7;
            }
            result
        };
        let len = checked_string_len(value, length_name)?;
        let needed = varint_len(len as u64)
            .checked_add(len)
            .ok_or_else(|| crate::error::Error::Protocol("column buffer length overflow".into()))?;
        // Cumulative per-column cap fires on the claim, before this value's
        // reserve/resize/read — lying lengths fail without allocating.
        total = checked_column_bytes(total, needed, length_name)?;
        data.reserve(needed);
        encode_varint(&mut data, len as u64);
        let last = row_idx + 1 == rows;
        if len > STRING_COL_MERGED_MAX {
            // Large value: one bulk read into the column buffer's tail.
            let start = data.len();
            let end = start.checked_add(len).ok_or_else(|| {
                crate::error::Error::Protocol("column buffer length overflow".into())
            })?;
            data.resize(end, 0);
            stream.read_exact(&mut data[start..]).await?;
        } else {
            // Merged read: this value's claimed bytes plus, when another row
            // follows, exactly one lookahead byte owned by the next row's
            // length varint. Skipped entirely for the final empty value so no
            // zero-length read is ever issued.
            let n = len + usize::from(!last);
            if n > 0 {
                stream.read_exact(&mut scratch[..n]).await?;
                data.extend_from_slice(&scratch[..len]);
                if !last {
                    pending = Some(scratch[len]);
                }
            }
        }
    }
    Ok(data)
}

pub(crate) async fn read_offsets_column<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, rows: usize, name: &str,
) -> Result<(Vec<u8>, usize)> {
    // Offsets are `rows` contiguous little-endian u64s on the wire — read them
    // in one shot instead of one read_exact per row, then scan for the max.
    let nbytes = checked_column_len(rows, 8, name)?;
    let mut offsets = vec![0u8; nbytes];
    stream.read_exact(&mut offsets).await?;
    let mut total = 0usize;
    for chunk in offsets.as_chunks::<8>().0 {
        let mut b = [0u8; 8];
        b.copy_from_slice(chunk);
        // Cumulative prefix sums must be non-decreasing; the running maximum
        // (the last offset) is the inner element count, capped at MAX_BLOCK_ROWS.
        total = checked_monotonic_offset(total, u64::from_le_bytes(b), name)?;
    }
    Ok((offsets, total))
}

#[inline]
fn varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

pub(crate) fn checked_len(rows: usize, width: usize) -> Result<usize> {
    rows.checked_mul(width)
        .ok_or_else(|| crate::error::Error::Protocol("column byte length overflow".into()))
}

pub(crate) fn checked_usize(value: u64, name: &str) -> Result<usize> {
    usize::try_from(value)
        .map_err(|_| crate::error::Error::Protocol(format!("{name} count too large")))
}

/// Validate a server-controlled per-value string length against the shared
/// 16 MiB wire limit before it sizes any buffer.
pub(crate) fn checked_string_len(value: u64, what: &str) -> Result<usize> {
    crate::limits::checked_string_len(value, what).map_err(crate::error::Error::Protocol)
}

/// Validate a column's running byte total (checked add + 64 MiB cap) before a
/// value-driven reserve/resize.
pub(crate) fn checked_column_bytes(acc: usize, add: usize, what: &str) -> Result<usize> {
    crate::limits::checked_column_bytes(acc, add, what).map_err(crate::error::Error::Protocol)
}

/// Validate a fixed-width/offset/index buffer byte length (`rows * width`,
/// checked multiply + 64 MiB cap) before any allocation sized from it.
pub(crate) fn checked_column_len(rows: usize, width: usize, what: &str) -> Result<usize> {
    crate::limits::checked_column_len(rows, width, what).map_err(crate::error::Error::Protocol)
}

/// Validate one Array/Map offset: non-decreasing (cumulative prefix sums) and
/// capped at MAX_BLOCK_ROWS inner elements.
pub(crate) fn checked_monotonic_offset(prev: usize, value: u64, what: &str) -> Result<usize> {
    crate::limits::checked_monotonic_offset(prev, value, what)
        .map_err(crate::error::Error::Protocol)
}

/// Validates a server-controlled item count against a [`crate::limits`] cap
/// before any allocation or loop is sized from it.
pub(crate) fn checked_count(value: u64, what: &str, max: usize) -> Result<usize> {
    crate::limits::checked_count(value, what, max).map_err(crate::error::Error::Protocol)
}

/// Validate a LowCardinality header and derive the per-row index width.
///
/// The 24-byte header carries a `version` (must be 1) and a `serial_type`
/// whose low 2 bits are the index width shift and whose bits 8/9 carry the
/// "global dictionaries" (unsupported) and "additional keys" (required) flags.
pub(crate) fn lc_idx_width(version: u64, serial_type: u64) -> Result<usize> {
    if version != 1 {
        return Err(crate::error::Error::Protocol(format!(
            "unsupported LowCardinality key serialization version {version}"
        )));
    }
    if (serial_type & (1u64 << 8)) != 0 {
        return Err(crate::error::Error::Protocol(
            "LowCardinality global dictionaries are not supported".into(),
        ));
    }
    if (serial_type & (1u64 << 9)) == 0 {
        return Err(crate::error::Error::Protocol(
            "LowCardinality additional keys flag is missing".into(),
        ));
    }
    Ok(1usize << (serial_type & 0x3))
}

pub(crate) fn encode_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        buf.push((value & 0x7F) as u8 | if value > 0x7F { 0x80 } else { 0 });
        value >>= 7;
        if value == 0 {
            break;
        }
    }
}

#[cold]
pub(crate) async fn read_exception<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<crate::error::Error> {
    let mut root = None;
    let mut messages = Vec::new();
    let mut buf = [0u8; 4];
    let mut depth = 0usize;
    loop {
        if depth >= crate::limits::MAX_EXCEPTION_CHAIN_DEPTH {
            return Err(crate::error::Error::Protocol(format!(
                "exception nesting too deep: more than {} levels",
                crate::limits::MAX_EXCEPTION_CHAIN_DEPTH
            )));
        }
        stream.read_exact(&mut buf).await?;
        let code = i32::from_le_bytes(buf);
        let name = read_string_lossy_async(stream).await?;
        let msg = read_string_lossy_async(stream).await?;
        let _stack = read_string_lossy_async(stream).await?;
        if root.is_none() {
            root = Some((code, name.clone()));
        }
        messages.push(format!("{name} (code {code}): {msg}"));
        depth += 1;
        stream.read_exact(&mut buf[..1]).await?;
        if buf[0] == 0 {
            break;
        }
    }
    let (code, name) = root.unwrap_or((0, "unknown".to_string()));
    Ok(crate::error::Error::ServerError {
        code,
        name,
        message: messages.join(" | nested: "),
    })
}

async fn read_string_lossy_async<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<String> {
    let len = checked_string_len(read_varint_async(stream).await?, "string length")?;
    if len > 1_048_576 {
        discard_exact(stream, len).await?;
        return Ok(format!("<large string {}b>", len));
    }
    if len == 0 {
        return Ok(String::new());
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

async fn discard_exact<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, mut len: usize,
) -> Result<()> {
    let mut buf = [0u8; 8192];
    while len > 0 {
        let n = len.min(buf.len());
        stream.read_exact(&mut buf[..n]).await?;
        len -= n;
    }
    Ok(())
}

#[cfg(test)]
mod timeout_tests {
    use super::packet_read_timeout;
    use crate::runtime::time::Instant;
    use std::time::Duration;

    #[test]
    fn no_deadline_returns_recv_timeout() {
        assert_eq!(
            packet_read_timeout(Duration::from_secs(300), None),
            Some(Duration::from_secs(300))
        );
    }

    #[test]
    fn deadline_far_returns_recv_timeout() {
        let dl = Instant::now() + Duration::from_secs(600);
        assert_eq!(
            packet_read_timeout(Duration::from_secs(300), Some(dl)),
            Some(Duration::from_secs(300))
        );
    }

    #[test]
    fn deadline_near_returns_remaining() {
        let dl = Instant::now() + Duration::from_millis(10);
        let got = packet_read_timeout(Duration::from_secs(300), Some(dl));
        assert!(got.is_some());
        assert!(got.expect("checked is_some above") <= Duration::from_millis(10));
    }

    #[test]
    fn deadline_expired_returns_none() {
        let dl = Instant::now() - Duration::from_millis(1);
        assert_eq!(
            packet_read_timeout(Duration::from_secs(300), Some(dl)),
            None
        );
    }
}

#[cfg(test)]
mod varint_read_tests {
    use super::read_varint_async;
    use crate::runtime::io::AsyncWriteExt;

    #[tokio::test]
    async fn read_varint_async_rejects_tenth_byte_overflow() {
        let (mut tx, mut rx) = tokio::io::duplex(16);
        let bytes = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02];
        tx.write_all(&bytes).await.expect("write varint");
        let result = read_varint_async(&mut rx).await;
        assert!(result.is_err(), "10th-byte overflow must error: {result:?}");
    }
}

#[cfg(test)]
mod exception_chain_depth_tests {
    use super::{encode_varint, read_exception};
    use crate::error::Error;
    use crate::limits::MAX_EXCEPTION_CHAIN_DEPTH;
    use crate::runtime::io::AsyncWriteExt;

    /// Wire body of an exception chain `levels` deep: per level, i32 LE code
    /// plus three length-prefixed strings and the 1-byte has_nested flag.
    fn chain_body(levels: usize) -> Vec<u8> {
        let mut body = Vec::new();
        for i in 0..levels {
            body.extend_from_slice(&46i32.to_le_bytes());
            for field in ["e", "m", ""] {
                encode_varint(&mut body, field.len() as u64);
                body.extend_from_slice(field.as_bytes());
            }
            body.push(u8::from(i + 1 < levels));
        }
        body
    }

    #[tokio::test]
    async fn read_exception_accepts_exactly_cap_chain() {
        let (mut tx, mut rx) = tokio::io::duplex(64);
        let body = chain_body(MAX_EXCEPTION_CHAIN_DEPTH);
        tokio::spawn(async move {
            tx.write_all(&body).await.expect("seed chain");
        });
        let err = read_exception(&mut rx)
            .await
            .expect("chain at cap must parse");
        let Error::ServerError { code, message, .. } = err else {
            unreachable!("expected ServerError");
        };
        assert_eq!(code, 46);
        assert_eq!(
            message.matches(" | nested: ").count(),
            MAX_EXCEPTION_CHAIN_DEPTH - 1
        );
    }

    #[tokio::test]
    async fn read_exception_rejects_cap_plus_one_chain() {
        let (mut tx, mut rx) = tokio::io::duplex(64);
        let body = chain_body(MAX_EXCEPTION_CHAIN_DEPTH + 1);
        tokio::spawn(async move {
            tx.write_all(&body).await.expect("seed chain");
        });
        let err = read_exception(&mut rx)
            .await
            .expect_err("chain deeper than cap must be rejected");
        match err {
            Error::Protocol(msg) => assert_eq!(
                msg,
                format!("exception nesting too deep: more than {MAX_EXCEPTION_CHAIN_DEPTH} levels")
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod offset_read_tests {
    use super::read_offsets_column;
    use crate::runtime::io::AsyncWriteExt;

    #[tokio::test]
    async fn read_offsets_column_reads_all_and_finds_max() {
        let (mut tx, mut rx) = tokio::io::duplex(64);
        // 3 little-endian u64 offsets: 2, 4, 7 — cumulative, so the last is 7.
        let bytes: Vec<u8> = [2u64, 4, 7].iter().flat_map(|v| v.to_le_bytes()).collect();
        tx.write_all(&bytes).await.expect("write offsets");
        let (offsets, total) = read_offsets_column(&mut rx, 3, "test")
            .await
            .expect("read offsets");
        assert_eq!(offsets, bytes);
        assert_eq!(total, 7);
    }

    #[tokio::test]
    async fn read_offsets_column_rejects_decreasing_offset() {
        let (mut tx, mut rx) = tokio::io::duplex(64);
        // Offsets are cumulative prefix sums; 7 followed by 4 is malformed.
        let bytes: Vec<u8> = [7u64, 4].iter().flat_map(|v| v.to_le_bytes()).collect();
        tx.write_all(&bytes).await.expect("write offsets");
        let err = read_offsets_column(&mut rx, 2, "array offset")
            .await
            .expect_err("decreasing offset must be rejected");
        assert!(
            matches!(&err, crate::error::Error::Protocol(msg) if msg == "array offset decreased from 7 to 4"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn read_offsets_column_rejects_huge_last_offset_before_inner_read() {
        let (mut tx, mut rx) = tokio::io::duplex(64);
        // A single 2^60 last offset must fail before any inner column data is
        // read — previously it became the inner row count unbounded.
        let bytes: Vec<u8> = [1u64 << 60].iter().flat_map(|v| v.to_le_bytes()).collect();
        tx.write_all(&bytes).await.expect("write offsets");
        let err = read_offsets_column(&mut rx, 1, "array offset")
            .await
            .expect_err("2^60 offset must be rejected");
        assert!(
            matches!(&err, crate::error::Error::Protocol(msg) if msg == "array offset total 1152921504606846976 exceeds limit 10000000"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn read_offsets_column_rejects_oversized_offset_buffer() {
        // rows * 8 above the 64 MiB column byte cap must fail before the
        // offsets buffer is allocated.
        let (_tx, mut rx) = tokio::io::duplex(64);
        let rows = crate::limits::MAX_COLUMN_BYTES / 8 + 1;
        let err = read_offsets_column(&mut rx, rows, "array offset")
            .await
            .expect_err("oversized offsets buffer must be rejected");
        assert!(
            matches!(&err, crate::error::Error::Protocol(msg) if msg.contains("byte length") && msg.contains("exceeds limit")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn read_offsets_column_zero_rows_is_empty() {
        let (_tx, mut rx) = tokio::io::duplex(8);
        let (offsets, total) = read_offsets_column(&mut rx, 0, "test")
            .await
            .expect("read offsets");
        assert!(offsets.is_empty());
        assert_eq!(total, 0);
    }
}

#[cfg(test)]
mod string_column_limit_tests {
    use super::read_string_column_with_prefixes;
    use crate::error::Error;
    use crate::protocol::wire;
    use crate::runtime::io::{AsyncReadExt as _, AsyncWriteExt as _};

    fn varint(v: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        wire::write_varint(&mut buf, v).expect("test write");
        buf
    }

    #[tokio::test]
    async fn string_value_length_u64_max_is_rejected_before_read() {
        // One row claims a u64::MAX-length value; no payload follows. The
        // per-value cap must fire on the claim, before any resize or read.
        let (mut tx, mut rx) = tokio::io::duplex(64);
        tx.write_all(&varint(u64::MAX))
            .await
            .expect("seed lying length");
        let err = read_string_column_with_prefixes(&mut rx, 1, "string value length")
            .await
            .expect_err("u64::MAX string value length must be rejected");
        match &err {
            Error::Protocol(msg) => assert_eq!(
                msg,
                "string value length 18446744073709551615 exceeds limit 16777215"
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn string_value_length_2_pow_40_is_rejected_before_read() {
        let (mut tx, mut rx) = tokio::io::duplex(64);
        tx.write_all(&varint(1u64 << 40))
            .await
            .expect("seed lying length");
        let err = read_string_column_with_prefixes(&mut rx, 1, "JSON string length")
            .await
            .expect_err("2^40 string value length must be rejected");
        assert!(
            matches!(&err, Error::Protocol(msg) if msg == "JSON string length 1099511627776 exceeds limit 16777215"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn string_column_values_still_parse() {
        let mut wire_bytes = Vec::new();
        wire_bytes.extend_from_slice(&varint(1));
        wire_bytes.push(b'a');
        wire_bytes.extend_from_slice(&varint(0));
        let (mut tx, mut rx) = tokio::io::duplex(64);
        tx.write_all(&wire_bytes).await.expect("seed values");
        let data = read_string_column_with_prefixes(&mut rx, 2, "string value length")
            .await
            .expect("small values parse");
        assert_eq!(data, vec![1, b'a', 0]);
    }

    #[tokio::test]
    async fn string_column_does_not_read_past_the_column() {
        // A String column followed by sentinel bytes on the same stream: the
        // buffered body reader must never consume beyond the column's last
        // claimed byte, or the next reader on this stream would desync. The
        // sentinel is written in the same batch so a greedy refill could grab
        // it; with tx dropped afterwards, any over-read surfaces as EOF here.
        let mut wire_bytes = Vec::new();
        for _ in 0..1000 {
            wire_bytes.extend_from_slice(&varint(3));
            wire_bytes.extend_from_slice(b"abc");
        }
        wire_bytes.extend_from_slice(&[0xABu8; 8]);
        let (mut tx, mut rx) = tokio::io::duplex(8192);
        tx.write_all(&wire_bytes)
            .await
            .expect("seed column + sentinel");
        drop(tx);
        let data = read_string_column_with_prefixes(&mut rx, 1000, "string value length")
            .await
            .expect("column must parse");
        assert_eq!(data.len(), 1000 * 4, "varint + body per row");
        let mut sentinel = [0u8; 8];
        rx.read_exact(&mut sentinel)
            .await
            .expect("sentinel must still be on the stream");
        assert_eq!(sentinel, [0xABu8; 8], "sentinel bytes must be untouched");
    }

    #[tokio::test]
    async fn string_column_multi_byte_varint_lengths_parse() {
        // Length 300 needs a two-byte varint on the wire.
        let mut wire_bytes = Vec::new();
        wire_bytes.extend_from_slice(&varint(300));
        wire_bytes.extend_from_slice(&[b'z'; 300]);
        wire_bytes.extend_from_slice(&varint(0));
        let (mut tx, mut rx) = tokio::io::duplex(1024);
        tx.write_all(&wire_bytes).await.expect("seed values");
        drop(tx);
        let data = read_string_column_with_prefixes(&mut rx, 2, "string value length")
            .await
            .expect("multi-byte varint lengths must parse");
        let mut expected = Vec::new();
        expected.extend_from_slice(&varint(300));
        expected.extend_from_slice(&[b'z'; 300]);
        expected.extend_from_slice(&varint(0));
        assert_eq!(data, expected);
    }

    #[tokio::test]
    async fn string_column_truncated_body_is_unexpected_eof() {
        // Claims 1000 bytes but sends 500, then EOF: the bulk body read must
        // surface the same UnexpectedEof a per-value read_exact would.
        let mut wire_bytes = Vec::new();
        wire_bytes.extend_from_slice(&varint(1000));
        wire_bytes.extend_from_slice(&[7u8; 500]);
        let (mut tx, mut rx) = tokio::io::duplex(1024);
        tx.write_all(&wire_bytes)
            .await
            .expect("seed truncated value");
        drop(tx);
        let err = read_string_column_with_prefixes(&mut rx, 1, "string value length")
            .await
            .expect_err("truncated body must error");
        assert!(
            matches!(
                &err,
                Error::Io(e) if e.kind() == std::io::ErrorKind::UnexpectedEof
            ),
            "expected UnexpectedEof, got {err:?}"
        );
    }

    #[tokio::test]
    async fn string_column_value_larger_than_window_streams_direct() {
        // A value larger than the merged-read limit bypasses it and streams
        // into the column buffer's tail. The duplex capacity exceeds the
        // whole payload and the sender is dropped before the reader runs, so
        // the test never depends on a concurrent writer task being scheduled
        // (send-before-drain would otherwise deadlock at 0% CPU).
        let big_len = 100_000usize;
        let mut wire_bytes = Vec::new();
        wire_bytes.extend_from_slice(&varint(big_len as u64));
        wire_bytes.extend_from_slice(&(0..big_len).map(|i| (i % 251) as u8).collect::<Vec<_>>());
        let (mut tx, mut rx) = tokio::io::duplex(1 << 18);
        tx.write_all(&wire_bytes).await.expect("seed large value");
        drop(tx);
        let data = read_string_column_with_prefixes(&mut rx, 1, "string value length")
            .await
            .expect("large value must parse");
        assert_eq!(data, wire_bytes);
    }
}

#[cfg(test)]
mod part_uuid_limit_tests {
    use super::read_part_uuids_packet;
    use crate::error::Error;
    use crate::protocol::wire;
    use crate::runtime::io::AsyncWriteExt as _;

    fn uuid_packet(count: u64, uuid_bytes: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        wire::write_varint(&mut buf, count).expect("test write");
        buf.extend_from_slice(uuid_bytes);
        buf
    }

    #[tokio::test]
    async fn part_uuid_count_u64_max_is_protocol_error() {
        let (mut tx, mut rx) = tokio::io::duplex(64);
        tx.write_all(&uuid_packet(u64::MAX, &[]))
            .await
            .expect("seed packet");
        let err = read_part_uuids_packet(&mut rx)
            .await
            .expect_err("u64::MAX PartUUID count must be rejected");
        match &err {
            Error::Protocol(msg) => assert_eq!(
                msg,
                "PartUUID count 18446744073709551615 exceeds limit 1048576"
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn part_uuid_count_cap_plus_one_is_protocol_error() {
        let (mut tx, mut rx) = tokio::io::duplex(64);
        tx.write_all(&uuid_packet(1_048_577, &[]))
            .await
            .expect("seed packet");
        let err = read_part_uuids_packet(&mut rx)
            .await
            .expect_err("cap + 1 PartUUID count must be rejected");
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn part_uuids_within_cap_parse() {
        let (mut tx, mut rx) = tokio::io::duplex(64);
        tx.write_all(&uuid_packet(2, &[0x11; 32]))
            .await
            .expect("seed packet");
        let uuids = read_part_uuids_packet(&mut rx)
            .await
            .expect("count within cap parses");
        assert_eq!(uuids.len(), 2);
        assert_eq!(uuids[0], [0x11; 16]);
        assert_eq!(uuids[1], [0x11; 16]);
    }
}
