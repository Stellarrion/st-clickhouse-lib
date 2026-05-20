use crate::connection::block_reader::read_data_block;
use crate::connection::callbacks::QueryCallbacks;
use crate::connection::io::{
    ping_stream, read_exception, read_profile_info_packet, read_progress_packet, read_varint_async,
};
use crate::connection::query_packet::{build_query_packet_from_cached_or_revision, query_id_bytes};
use crate::connection::server_packets::{
    handle_coordinator_packet, read_part_uuids_update, read_timezone_update,
    unsupported_server_packet, write_ignored_part_uuids_if_any,
};
use crate::connection::tcp::Client;
use crate::error::Result;
use crate::metrics::QueryMetricGuard;
use crate::protocol::block::Block;
use crate::runtime::io::AsyncWriteExt;
use std::time::Duration;

/// A streaming block-level cursor for interactive SELECT queries.
///
/// Holds a `PoolGuard` with the connection open. The user calls
/// `next_block()` to read blocks one at a time, decides when to
/// stop, and drops the stream (or calls `cancel()`) to finish.
///
/// This is the `BeginSelect` -> `NextBlock()` -> ... -> `Cancel()`
/// interactive pattern from the protocol.
pub struct BlockStream<'a> {
    guard: crate::pool::PoolGuard<'a>,
    done: bool,
    recv_timeout: Duration,
    callbacks: QueryCallbacks,
}

impl Client {
    /// Begin an interactive SELECT query. Returns a [`BlockStream`] that
    /// streams result blocks one at a time under caller control.
    ///
    /// The connection stays open until the `BlockStream` is dropped or
    /// `cancel()` is called.
    pub async fn begin_select(&self, query: &str) -> Result<BlockStream<'_>> {
        self.begin_select_with_ignored_part_uuids(query, &[]).await
    }

    /// Begin an interactive SELECT query while telling ClickHouse to ignore
    /// specific replicated part UUIDs for this query.
    pub async fn begin_select_with_ignored_part_uuids(
        &self, query: &str, ignored_part_uuids: &[[u8; 16]],
    ) -> Result<BlockStream<'_>> {
        let metric_guard = QueryMetricGuard::new(self.metrics(), 1);
        let mut guard = self.pool.get().await?;
        let rev = guard.server_info().negotiated_revision;
        {
            let mut query_id_buf = [0u8; 22];
            let query_id = query_id_bytes(None, &mut query_id_buf);
            let pkt = build_query_packet_from_cached_or_revision(
                &self.query_template,
                &self.settings,
                rev,
                query,
                query_id,
                true,
                &[],
            );
            let stream = guard.stream_mut();
            if self.ping_before_query {
                ping_stream(stream).await?;
            }
            write_ignored_part_uuids_if_any(stream, ignored_part_uuids).await?;
            stream.write_packet(&pkt).await?;
            stream.flush().await?;
        }
        metric_guard.succeed();
        Ok(BlockStream {
            guard,
            done: false,
            recv_timeout: self.recv_timeout,
            callbacks: QueryCallbacks::default(),
        })
    }
}

impl BlockStream<'_> {
    /// Read the next Data block from the server.
    ///
    /// Returns `Ok(Some(block))` for a data block, `Ok(None)` when the
    /// query has completed (EndOfStream), or an error.
    ///
    /// Progress, Profile, Log, and other non-Data packets are silently
    /// consumed.
    pub async fn next_block(&mut self) -> Result<Option<Block>> {
        if self.done {
            return Ok(None);
        }
        let stream = self.guard.stream_mut();
        loop {
            let packet_type =
                match crate::runtime::time::timeout(self.recv_timeout, read_varint_async(stream))
                    .await
                {
                    Ok(Ok(t)) => t,
                    Ok(Err(e)) => {
                        self.done = true;
                        return Err(e);
                    },
                    Err(_) => return Ok(None),
                };
            match packet_type {
                1 => {
                    let block = read_data_block(stream).await?;
                    if block.row_count() > 0 {
                        return Ok(Some(block));
                    }
                },
                2 => {
                    let err = read_exception(stream).await?;
                    self.done = true;
                    return Err(err);
                },
                3 => {
                    let _ = read_progress_packet(stream).await?;
                },
                4 => {},
                5 => {
                    self.done = true;
                    return Ok(None);
                },
                6 => {
                    let _ = read_profile_info_packet(stream).await?;
                },
                10 | 14 => {
                    let log_block = read_data_block(stream).await?;
                    if let Some(ref cb) = self.callbacks.on_log {
                        cb(&log_block);
                    }
                    if let Some(ref cb) = self.callbacks.on_profile_events {
                        cb(&log_block);
                    }
                },
                17 => {
                    read_timezone_update(stream, &self.callbacks).await?;
                },
                12 => {
                    read_part_uuids_update(stream, &self.callbacks).await?;
                },
                _ => {
                    if handle_coordinator_packet(stream, packet_type).await? {
                        continue;
                    }
                    return Err(unsupported_server_packet(stream, packet_type).await?);
                },
            }
        }
    }

    /// Cancel the running query.
    ///
    /// Sends a Cancel packet to the server. The stream is no longer usable
    /// after cancellation.
    pub async fn cancel(&mut self) -> Result<()> {
        if self.done {
            return Ok(());
        }
        self.done = true;
        let stream = self.guard.stream_mut();
        stream.write_packet(&[3]).await?;
        stream.flush().await?;
        Ok(())
    }
}

impl Drop for BlockStream<'_> {
    fn drop(&mut self) {
        if let Some(tcp) = self.guard.stream_mut().raw_tcp() {
            let _ = tcp.try_write(&[3u8]);
        }
    }
}
