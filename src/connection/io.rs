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

const MAX_STRING_BYTES: usize = 0x00FF_FFFF;

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
) -> QueryPacketCommonTemplate {
    let mut prefix = Vec::with_capacity(4);
    wire::write_varint_to_vec(&mut prefix, 1); // ClientCode::Query

    let client_info = (rev >= revision::DBMS_MIN_REVISION_WITH_CLIENT_INFO)
        .then(|| build_client_info_template(rev));

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
    let count = checked_usize(read_varint_async(stream).await?, "PartUUIDs")?;
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
    let len = checked_string_len(read_varint_async(stream).await?)?;
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
    let columns = checked_usize(read_varint_async(stream).await?, "columns")?;
    let rows = checked_usize(read_varint_async(stream).await?, "rows")?;
    Ok((columns, rows))
}

pub(crate) async fn read_string_column_with_prefixes<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, rows: usize, length_name: &str,
) -> Result<Vec<u8>> {
    let mut data = Vec::with_capacity(rows.saturating_mul(8));
    for _ in 0..rows {
        let len = checked_usize(read_varint_async(stream).await?, length_name)?;
        let needed = varint_len(len as u64)
            .checked_add(len)
            .ok_or_else(|| crate::error::Error::Protocol("column buffer length overflow".into()))?;
        data.reserve(needed);
        encode_varint(&mut data, len as u64);
        let start = data.len();
        let end = start
            .checked_add(len)
            .ok_or_else(|| crate::error::Error::Protocol("column buffer length overflow".into()))?;
        data.resize(end, 0);
        stream.read_exact(&mut data[start..]).await?;
    }
    Ok(data)
}

pub(crate) async fn read_offsets_column<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, rows: usize, name: &str,
) -> Result<(Vec<u8>, usize)> {
    let mut total = 0usize;
    let mut offsets = Vec::with_capacity(checked_len(rows, 8)?);
    for _ in 0..rows {
        let mut offset = [0u8; 8];
        stream.read_exact(&mut offset).await?;
        let value = checked_usize(u64::from_le_bytes(offset), name)?;
        total = total.max(value);
        offsets.extend_from_slice(&offset);
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
    loop {
        stream.read_exact(&mut buf).await?;
        let code = i32::from_le_bytes(buf);
        let name = read_string_lossy_async(stream).await?;
        let msg = read_string_lossy_async(stream).await?;
        let _stack = read_string_lossy_async(stream).await?;
        if root.is_none() {
            root = Some((code, name.clone()));
        }
        messages.push(format!("{name} (code {code}): {msg}"));
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

fn checked_string_len(value: u64) -> Result<usize> {
    let value = usize::try_from(value)
        .map_err(|_| crate::error::Error::Protocol("string length too large".into()))?;
    if value > MAX_STRING_BYTES {
        return Err(crate::error::Error::Protocol(format!(
            "string length {value} exceeds clickhouse-cpp limit {MAX_STRING_BYTES}"
        )));
    }
    Ok(value)
}

async fn read_string_lossy_async<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S,
) -> Result<String> {
    let len = checked_string_len(read_varint_async(stream).await?)?;
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
        assert!(got.unwrap() <= Duration::from_millis(10));
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
