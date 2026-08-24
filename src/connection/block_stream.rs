use crate::connection::block_reader::{read_data_block, read_data_block_maybe_compressed};
use crate::connection::callbacks::QueryCallbacks;
use crate::connection::io::{
    compression_flag, packet_read_timeout, ping_stream, read_exception, read_profile_info_packet,
    read_progress_packet, read_varint_async,
};
use crate::connection::query_packet::{build_query_packet_from_cached_or_revision, query_id_bytes};
use crate::connection::server_packets::{
    cancel_and_drain, handle_coordinator_packet, read_part_uuids_update, read_timezone_update,
    unsupported_server_packet, write_ignored_part_uuids_if_any,
};
use crate::connection::tcp::Client;
use crate::error::Result;
use crate::metrics::QueryMetricGuard;
use crate::protocol::block::Block;
use crate::protocol::packet::ClientPacket;
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
    deadline: Option<crate::runtime::time::Instant>,
    callbacks: QueryCallbacks,
    /// Compression negotiated in the query packet: Data and ProfileEvents
    /// blocks are read with it, Log blocks stay protocol-defined uncompressed,
    /// and the cancellation drain skips block bodies accordingly.
    response_compressed: bool,
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
        // Mark in-flight before the first packet that can leave a response
        // pending (the optional pre-query Ping; otherwise the query write).
        // The BlockStream clears the mark at its clean terminal points
        // (EndOfStream / server exception); everything else already discards
        // the socket, so a dropped future in between cannot return a
        // mid-response stream.
        guard.mark_response_in_flight();
        {
            let stream = guard.stream_mut();
            if self.ping_before_query {
                ping_stream(stream).await?;
            }
            write_ignored_part_uuids_if_any(stream, ignored_part_uuids).await?;
            stream.write_packet(&pkt).await?;
            stream.flush().await?;
        }
        let deadline = self
            .query_timeout
            .map(|t| crate::runtime::time::Instant::now() + t);
        metric_guard.succeed();
        Ok(BlockStream {
            guard,
            done: false,
            recv_timeout: self.recv_timeout,
            deadline,
            callbacks: QueryCallbacks::default(),
            response_compressed: compression_flag(self.compression) == 1,
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
            let packet_type = match packet_read_timeout(self.recv_timeout, self.deadline) {
                Some(per_read) => {
                    match crate::runtime::time::timeout(per_read, read_varint_async(stream)).await {
                        Ok(Ok(t)) => t,
                        Ok(Err(e)) => {
                            self.done = true;
                            let _ = self.guard.take_stream();
                            return Err(e);
                        },
                        Err(_) => {
                            if self.deadline.is_some() {
                                self.done = true;
                                let _ = cancel_and_drain(
                                    stream,
                                    self.recv_timeout,
                                    self.response_compressed,
                                )
                                .await;
                                let _ = self.guard.take_stream();
                                return Err(crate::error::Error::Timeout(
                                    "query exceeded deadline".into(),
                                ));
                            }
                            self.done = true;
                            let _ = self.guard.take_stream();
                            return Err(crate::error::Error::Timeout(
                                "receive timeout while reading query response".into(),
                            ));
                        },
                    }
                },
                None => {
                    self.done = true;
                    let _ =
                        cancel_and_drain(stream, self.recv_timeout, self.response_compressed).await;
                    let _ = self.guard.take_stream();
                    return Err(crate::error::Error::Timeout(
                        "query exceeded deadline".into(),
                    ));
                },
            };
            match packet_type {
                1 => {
                    let block =
                        read_data_block_maybe_compressed(stream, self.response_compressed).await?;
                    if block.row_count() > 0 {
                        return Ok(Some(block));
                    }
                },
                2 => {
                    let err = read_exception(stream).await?;
                    // A server exception terminates the response: the
                    // connection stays clean and reusable.
                    self.done = true;
                    self.guard.clear_response_in_flight();
                    return Err(err);
                },
                3 => {
                    let _ = read_progress_packet(stream).await?;
                },
                4 => {},
                5 => {
                    self.done = true;
                    // EndOfStream: the response cycle is complete, so the
                    // pooled connection stays reusable after the stream drops.
                    self.guard.clear_response_in_flight();
                    return Ok(None);
                },
                6 => {
                    let _ = read_profile_info_packet(stream).await?;
                },
                10 => {
                    // Log blocks are always sent uncompressed.
                    let log_block = read_data_block(stream).await?;
                    if let Some(ref cb) = self.callbacks.on_log {
                        cb(&log_block);
                    }
                    if let Some(ref cb) = self.callbacks.on_profile_events {
                        cb(&log_block);
                    }
                },
                14 => {
                    // ProfileEvents follow the response compression flag.
                    let log_block =
                        read_data_block_maybe_compressed(stream, self.response_compressed).await?;
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

    /// Cancel the running query and close its connection.
    ///
    /// Sends a best-effort framed `Cancel`, then discards the socket instead of
    /// waiting behind an arbitrarily large response body. The pool reconnects
    /// lazily on its next acquire, so cancellation stays bounded and no partial
    /// response can poison a reused connection.
    pub async fn cancel(&mut self) -> Result<()> {
        if self.done {
            return Ok(());
        }
        self.done = true;
        let send_budget = self.recv_timeout.min(Duration::from_secs(1));
        let _ = crate::runtime::time::timeout(send_budget, async {
            let stream = self.guard.stream_mut();
            stream.write_packet(&[ClientPacket::Cancel as u8]).await?;
            stream.flush().await?;
            Ok::<(), crate::error::Error>(())
        })
        .await;
        let _ = self.guard.take_stream();
        Ok(())
    }
}

impl Drop for BlockStream<'_> {
    fn drop(&mut self) {
        if !self.done {
            // Async draining is impossible in Drop. Discard the socket rather
            // than inject a raw Cancel byte (which bypasses TLS/chunk framing)
            // or return a partial response to the pool.
            let _ = self.guard.take_stream();
        }
    }
}
