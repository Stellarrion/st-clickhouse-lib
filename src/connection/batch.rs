//! Batch query execution — pipelined multi-statement queries.
//!
//! Sends multiple SELECT query packets back-to-back in one write() call,
//! then reads all responses sequentially. Saves round-trips for chatty
//! workloads.
//!
//! Only enabled via explicit `client.batch()` — never implicit.

use crate::compression::CompressionMethod;
use crate::connection::io::{
    QueryPacketCommonTemplate, build_empty_cluster_function_read_task_response,
    build_finished_merge_tree_read_task_response, build_query_packet_common_template,
    compression_flag, merge_settings, ping_stream, read_exception,
    read_parallel_read_request_stream_id, read_profile_info_packet, read_progress_packet,
    read_string_async, read_varint_async, skip_parallel_read_announcement, skip_part_uuids_packet,
    write_empty_data_for, write_query_packet_common_from_template,
};
use crate::error::Result;
use crate::metrics::QueryMetricGuard;
use crate::protocol::block::Block;
use crate::protocol::revision;
use crate::protocol::wire;
use crate::query_id::next_query_id_with_prefix;
use crate::runtime::io::AsyncWriteExt;
use crate::runtime::sync::mpsc;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;

static QUERY_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

// ═══════════════════════════════════════════════
// BatchBuilder
// ═══════════════════════════════════════════════

/// Explicitly batches multiple queries into a single pipelined execution.
///
/// ```ignore
/// let blocks = client.batch()
///     .query("SELECT COUNT(*) FROM users")
///     .query("SELECT COUNT(*) FROM orders")
///     .execute().await?;
/// ```
pub struct BatchBuilder<'a> {
    client: &'a crate::connection::Client,
    queries: Vec<String>,
    settings: HashMap<String, String>,
    compression: Option<CompressionMethod>,
    ignored_part_uuids: Vec<[u8; 16]>,
}

impl<'a> BatchBuilder<'a> {
    pub(crate) fn new(client: &'a crate::connection::Client) -> Self {
        Self {
            client,
            queries: Vec::new(),
            settings: HashMap::new(),
            compression: None,
            ignored_part_uuids: Vec::new(),
        }
    }

    /// Add a query to the batch.
    pub fn query(mut self, sql: &str) -> Self {
        self.queries.push(sql.to_owned());
        self
    }

    /// Override compression for this batch.
    pub fn with_compression(mut self, method: CompressionMethod) -> Self {
        self.compression = Some(method);
        self
    }

    /// Override settings for all queries in this batch.
    pub fn with_setting(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.insert(name.into(), value.into());
        self
    }

    /// Control Native JSON serialization for materialized batch results.
    ///
    /// Enabled by default to match clickhouse-cpp. Pass `false` to opt back into
    /// ClickHouse's native JSON/Object serialization.
    pub fn with_native_json_as_string(self, enabled: bool) -> Self {
        self.with_setting(
            crate::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING,
            if enabled { "1" } else { "0" },
        )
    }

    /// Ignore specific replicated part UUIDs before each query in the batch.
    pub fn with_ignored_part_uuid(mut self, uuid: [u8; 16]) -> Self {
        self.ignored_part_uuids.push(uuid);
        self
    }

    /// Ignore specific replicated part UUIDs before each query in the batch.
    pub fn with_ignored_part_uuids<I>(mut self, uuids: I) -> Self
    where
        I: IntoIterator<Item = [u8; 16]>,
    {
        self.ignored_part_uuids.extend(uuids);
        self
    }

    /// Execute all queries, returning the first data block from each result set.
    ///
    /// Sends all query packets in a single `write()` call, then reads responses
    /// sequentially. Returns `None` for queries that produce no data blocks.
    pub async fn execute(self) -> Result<Vec<Option<Block>>> {
        if self.queries.is_empty() {
            return Ok(Vec::new());
        }
        let n = self.queries.len();
        let metric_guard = QueryMetricGuard::new(self.client.metrics(), n as u64);

        // Acquire a connection
        let mut guard = self.client.pool.get().await?;
        let rev = guard.server_info().negotiated_revision;

        // Build and send ALL query packets in one write
        let merged_settings = merge_settings(&self.client.settings, &self.settings);
        let compression = self.compression.or(self.client.compression);
        let response_compressed = compression_flag(compression) == 1;
        let template = build_batch_query_packet_template(
            &merged_settings,
            compression,
            rev,
            self.client.pool.quota_key(),
        );

        let mut all_packets = Vec::new();
        let ignored_part_uuids_packet = (!self.ignored_part_uuids.is_empty()).then(|| {
            crate::protocol::part_uuid::build_ignored_part_uuids_packet(&self.ignored_part_uuids)
        });
        for sql in &self.queries {
            let mut query_id_buf = [0u8; 22];
            let query_id_len = next_query_id(&mut query_id_buf);
            let query_id = &query_id_buf[..query_id_len];
            if let Some(pkt) = &ignored_part_uuids_packet {
                all_packets.extend_from_slice(pkt);
            }
            write_batch_query_packet_from_template(&mut all_packets, &template, sql, query_id);
        }

        // Mark in-flight before the first packet that can leave a response
        // pending (the optional pre-query Ping; otherwise the pipelined query
        // writes): a future dropped before take_stream() must not hand a
        // mid-response socket back to the pool. Taking the stream below moves
        // the connection out of the pool entirely, so the mark stops mattering
        // once the reader task owns the socket.
        guard.mark_response_in_flight();
        let stream = guard.stream_mut();
        if self.client.ping_before_query {
            ping_stream(stream).await?;
        }
        stream.write_packet(&all_packets).await?;
        stream.flush().await?;

        // Take the stream and spawn a reader task. Clear the in-flight mark
        // first: the reader task now owns the socket outright, so dropping
        // this future must not also discard (an empty take would be a no-op,
        // but the flag would survive on a `None` slot semantics audit).
        guard.clear_response_in_flight();
        let stream = guard
            .take_stream()
            .ok_or_else(|| crate::error::Error::Protocol("connection stream taken".into()))?;
        drop(guard);

        let (block_tx, mut block_rx) = mpsc::channel(n * 2);
        crate::runtime::spawn(async move {
            if let Err(e) = read_n_result_sets(stream, n, response_compressed, &block_tx).await {
                let _ = block_tx.send((0, Err(e))).await;
            }
        });

        // Collect results: first non-empty block per query index
        let mut results: Vec<Option<Block>> = (0..n).map(|_| None).collect();
        let mut remaining = n;
        while let Some((idx, result)) = block_rx.recv().await {
            match result {
                Ok(Some(block)) => {
                    if results[idx].is_none() {
                        results[idx] = Some(block);
                    }
                },
                Ok(None) => {
                    remaining -= 1;
                    if remaining == 0 {
                        break;
                    }
                },
                Err(e) => return Err(e),
            }
        }
        metric_guard.succeed();
        Ok(results)
    }
}

// ═══════════════════════════════════════════════
// Background reader: read N sequential result sets
// ═══════════════════════════════════════════════

/// Read `n` sequential result sets from the stream, sending each block
/// tagged with its query index. A result set ends at EndOfStream (type 5).
async fn read_n_result_sets(
    mut stream: crate::pool::StreamWrapper, n: usize, response_compressed: bool,
    block_tx: &mpsc::Sender<(usize, Result<Option<Block>>)>,
) -> Result<()> {
    for query_idx in 0..n {
        // Read packets until EoS for this result set
        loop {
            let packet_type = read_varint_async(&mut stream).await?;
            match packet_type {
                1 => {
                    let block = super::block_reader::read_data_block_maybe_compressed(
                        &mut stream,
                        response_compressed,
                    )
                    .await?;
                    if block.row_count() > 0
                        && block_tx.send((query_idx, Ok(Some(block)))).await.is_err()
                    {
                        return Ok(());
                    }
                },
                2 => {
                    let err = read_exception(&mut stream).await?;
                    let _ = block_tx.send((query_idx, Err(err))).await;
                    return Ok(());
                },
                3 => {
                    read_progress_packet(&mut stream).await?;
                },
                4 => { /* Pong */ },
                5 => {
                    // EndOfStream for this query
                    let _ = block_tx.send((query_idx, Ok(None))).await;
                    break; // move to next query
                },
                6 => {
                    read_profile_info_packet(&mut stream).await?;
                },
                10 => {
                    let _ = super::block_reader::read_data_block(&mut stream).await?;
                },
                14 => {
                    let _ = super::block_reader::read_data_block_maybe_compressed(
                        &mut stream,
                        response_compressed,
                    )
                    .await?;
                },
                12 => {
                    skip_part_uuids_packet(&mut stream).await?;
                },
                17 => {
                    let _timezone = read_string_async(&mut stream).await?;
                },
                13 => {
                    let pkt = build_empty_cluster_function_read_task_response();
                    stream.write_packet(&pkt).await?;
                    stream.flush().await?;
                },
                15 => {
                    skip_parallel_read_announcement(&mut stream).await?;
                },
                16 => {
                    let stream_id = read_parallel_read_request_stream_id(&mut stream).await?;
                    let pkt = build_finished_merge_tree_read_task_response(&stream_id);
                    stream.write_packet(&pkt).await?;
                    stream.flush().await?;
                },
                _ => {
                    let _ = block_tx
                        .send((
                            query_idx,
                            Err(crate::error::Error::Protocol(format!(
                                "unknown packet type: {packet_type}"
                            ))),
                        ))
                        .await;
                    return Ok(());
                },
            }
        }
    }
    Ok(())
}

// ═══════════════════════════════════════════════
// Query packet builder (no trailing empty Data block —
// the batch sends the empty marker as part of the packet)
// ═══════════════════════════════════════════════

struct BatchQueryPacketTemplate {
    common: QueryPacketCommonTemplate,
    suffix: Vec<u8>,
}

fn build_batch_query_packet_template(
    settings: &HashMap<String, String>, compression: Option<CompressionMethod>, rev: u64,
    quota_key: &str,
) -> BatchQueryPacketTemplate {
    let common = build_query_packet_common_template(settings, compression, rev, quota_key);

    let mut suffix = Vec::with_capacity(16);
    if rev >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS {
        wire::write_string_to_vec(&mut suffix, ""); // parameters terminator
    }
    write_empty_data_for(&mut suffix, compression);

    BatchQueryPacketTemplate { common, suffix }
}

fn write_batch_query_packet_from_template(
    buf: &mut Vec<u8>, template: &BatchQueryPacketTemplate, query: &str, query_id: &[u8],
) {
    write_query_packet_common_from_template(buf, &template.common, query_id);
    wire::write_string_to_vec(buf, query);
    buf.extend_from_slice(&template.suffix);
}

fn next_query_id(buf: &mut [u8; 22]) -> usize {
    next_query_id_with_prefix(buf, b"st-b-", &QUERY_ID_COUNTER)
}

#[cfg(test)]
mod packet_template_tests {
    use super::*;

    fn build_batch_query_packet_dynamic(
        query: &str, settings: &HashMap<String, String>, compression: Option<CompressionMethod>,
    ) -> Vec<u8> {
        const REV: u64 = revision::DEFAULT_PROTOCOL_REVISION;
        let mut b = Vec::new();
        wire::write_varint_to_vec(&mut b, 1);
        wire::write_string_to_vec(&mut b, "");
        wire::write_varint_to_vec(&mut b, 1);
        crate::client_info::write_client_info(&mut b, REV, None);
        crate::connection::io::write_protocol_default_settings(&mut b, settings, REV);
        for (name, value) in settings {
            wire::write_string_to_vec(&mut b, name);
            wire::write_varint_to_vec(&mut b, 0);
            wire::write_string_to_vec(&mut b, value);
        }
        wire::write_string_to_vec(&mut b, "");
        if REV >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES {
            wire::write_string_to_vec(&mut b, "");
        }
        if REV >= revision::DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
            wire::write_string_to_vec(&mut b, "");
        }
        wire::write_varint_to_vec(&mut b, 2);
        wire::write_varint_to_vec(&mut b, compression_flag(compression));
        wire::write_string_to_vec(&mut b, query);
        wire::write_string_to_vec(&mut b, "");
        write_empty_data_for(&mut b, compression);
        b
    }

    #[test]
    fn batch_template_matches_dynamic_packet_builder() {
        let mut settings = HashMap::new();
        settings.insert("max_block_size".to_string(), "1024".to_string());
        let compression = Some(CompressionMethod::Lz4);
        let template = build_batch_query_packet_template(
            &settings,
            compression,
            revision::DEFAULT_PROTOCOL_REVISION,
            "",
        );

        let mut templated = Vec::new();
        write_batch_query_packet_from_template(&mut templated, &template, "SELECT 1", b"");
        let dynamic = build_batch_query_packet_dynamic("SELECT 1", &settings, compression);

        assert_eq!(templated, dynamic);
    }
}
