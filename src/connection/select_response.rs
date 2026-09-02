use crate::connection::block_reader::{
    discard_data_block_maybe_compressed, read_data_block, read_data_block_maybe_compressed,
};
use crate::connection::callbacks::QueryCallbacks;
use crate::connection::io::{
    packet_read_timeout, read_exception, read_profile_info_packet, read_progress_packet,
    read_varint_async,
};
use crate::connection::raw_block_reader::read_raw_data_block;
use crate::connection::server_packets::{
    cancel_and_drain, handle_coordinator_packet, read_part_uuids_update, read_timezone_update,
    unsupported_server_packet,
};
use crate::error::Result;
use crate::limits::ResponseBudget;
use crate::protocol::block::{Block, RawBlock};
use std::time::Duration;

/// Charge `block`'s decoded payload bytes against `budget`, mapping a breach
/// to the actionable `ResponseTooLarge` error.
fn charge_block(budget: &mut ResponseBudget, payload_bytes: usize) -> Result<()> {
    budget
        .charge(payload_bytes)
        .map_err(|_| crate::error::Error::response_budget_exceeded(budget))
}

#[allow(async_fn_in_trait)]
pub(super) trait SelectResponseHandler {
    type Output;

    async fn on_data(
        &mut self, stream: &mut crate::pool::StreamWrapper, response_compressed: bool,
    ) -> Result<()>;

    async fn on_log_packet(
        &mut self, stream: &mut crate::pool::StreamWrapper, packet_type: u64,
        response_compressed: bool, callbacks: &QueryCallbacks,
    ) -> Result<()> {
        match packet_type {
            10 => {
                let block = read_data_block(stream).await?;
                if let Some(ref cb) = callbacks.on_log {
                    cb(&block);
                }
            },
            14 => {
                let block = read_data_block_maybe_compressed(stream, response_compressed).await?;
                if let Some(ref cb) = callbacks.on_profile_events {
                    cb(&block);
                }
            },
            _ => unreachable!(),
        }
        Ok(())
    }

    fn finish(self) -> Result<Self::Output>;
}

pub(super) async fn read_select_response<H: SelectResponseHandler>(
    stream: &mut crate::pool::StreamWrapper, recv_timeout: Duration,
    deadline: Option<crate::runtime::time::Instant>, response_compressed: bool,
    callbacks: &QueryCallbacks, mut handler: H,
) -> Result<H::Output> {
    loop {
        let typ = match packet_read_timeout(recv_timeout, deadline) {
            Some(per_read) => {
                match crate::runtime::time::timeout(per_read, read_varint_async(stream)).await {
                    Ok(Ok(t)) => t,
                    Ok(Err(e)) => return Err(e),
                    Err(_) => {
                        if deadline.is_some() {
                            let _ =
                                cancel_and_drain(stream, recv_timeout, response_compressed).await;
                            return Err(crate::error::Error::Timeout(
                                "query exceeded deadline".into(),
                            ));
                        }
                        return Err(crate::error::Error::Timeout(
                            "receive timeout while reading query response".into(),
                        ));
                    },
                }
            },
            None => {
                let _ = cancel_and_drain(stream, recv_timeout, response_compressed).await;
                return Err(crate::error::Error::Timeout(
                    "query exceeded deadline".into(),
                ));
            },
        };
        match typ {
            1 => handler.on_data(stream, response_compressed).await?,
            2 => return Err(read_exception(stream).await?),
            3 => {
                let progress = read_progress_packet(stream).await?;
                if let Some(ref cb) = callbacks.on_progress {
                    cb(progress);
                }
            },
            4 => {},
            5 => return handler.finish(),
            6 => {
                let profile = read_profile_info_packet(stream).await?;
                if let Some(ref cb) = callbacks.on_profile {
                    cb(profile);
                }
            },
            10 | 14 => {
                handler
                    .on_log_packet(stream, typ, response_compressed, callbacks)
                    .await?;
            },
            17 => {
                read_timezone_update(stream, callbacks).await?;
            },
            12 => {
                read_part_uuids_update(stream, callbacks).await?;
            },
            _ => {
                if handle_coordinator_packet(stream, typ).await? {
                    continue;
                }
                return Err(unsupported_server_packet(stream, typ).await?);
            },
        }
    }
}

/// Single-block terminal: returns the query's one non-empty Data block.
///
/// Exact-one-block semantics: a second non-empty Data block is an error, never
/// a silent truncation. Once the first block is captured, later Data blocks
/// are discarded (without materializing their columns) so the response is
/// still read through EndOfStream and the pooled connection stays clean;
/// `finish` then surfaces the error. Backs [`QueryBuilder::block`] and
/// `fetch::<Block>()`.
///
/// The retained block's decoded payload bytes are charged against the
/// response budget (`max_response_size`); discarded extra blocks are not.
pub(super) struct FirstBlockHandler {
    first: Option<Block>,
    extra_blocks: bool,
    budget: ResponseBudget,
}

impl FirstBlockHandler {
    /// Budgeted handler allowing `limit` decoded payload bytes.
    pub(super) fn new(limit: usize) -> Self {
        Self {
            first: None,
            extra_blocks: false,
            budget: ResponseBudget::new(limit),
        }
    }
}

impl SelectResponseHandler for FirstBlockHandler {
    type Output = Block;

    async fn on_data(
        &mut self, stream: &mut crate::pool::StreamWrapper, response_compressed: bool,
    ) -> Result<()> {
        if self.first.is_some() {
            // Keep the stream aligned by consuming (not materializing) the
            // extra block, then flag it so `finish` errors out.
            let rows = discard_data_block_maybe_compressed(stream, response_compressed).await?;
            if rows > 0 {
                self.extra_blocks = true;
            }
            return Ok(());
        }
        let block = read_data_block_maybe_compressed(stream, response_compressed).await?;
        if block.row_count() > 0 {
            charge_block(&mut self.budget, block.payload_bytes())?;
            self.first = Some(block);
        }
        Ok(())
    }

    fn finish(self) -> Result<Self::Output> {
        if self.extra_blocks {
            return Err(crate::error::Error::Protocol(
                "query returned multiple non-empty data blocks; use .blocks() to fetch all of them"
                    .into(),
            ));
        }
        self.first
            .ok_or_else(|| crate::error::Error::Protocol("no data blocks".into()))
    }
}

/// Multi-block terminal: collects every non-empty Data block, preserving
/// block boundaries. Column payloads are moved into the `Block` values, not
/// copied. Backs [`QueryBuilder::blocks`].
///
/// Each retained block's decoded payload bytes are charged against the
/// response budget (`max_response_size`); the cumulative total crossing the
/// limit aborts the read with `Error::ResponseTooLarge` (the mid-response
/// socket is then discarded by the pool).
pub(super) struct BlocksHandler {
    blocks: Vec<Block>,
    budget: ResponseBudget,
}

impl BlocksHandler {
    /// Budgeted handler allowing `limit` decoded payload bytes.
    pub(super) fn new(limit: usize) -> Self {
        Self {
            blocks: Vec::new(),
            budget: ResponseBudget::new(limit),
        }
    }
}

impl SelectResponseHandler for BlocksHandler {
    type Output = Vec<Block>;

    async fn on_data(
        &mut self, stream: &mut crate::pool::StreamWrapper, response_compressed: bool,
    ) -> Result<()> {
        let block = read_data_block_maybe_compressed(stream, response_compressed).await?;
        if block.row_count() > 0 {
            charge_block(&mut self.budget, block.payload_bytes())?;
            self.blocks.push(block);
        }
        Ok(())
    }

    fn finish(self) -> Result<Self::Output> {
        Ok(self.blocks)
    }
}

/// Row-vector terminal backing [`QueryBuilder::all`] and
/// `fetch::<Vec<T>>()`. Every non-empty block is decoded and its payload
/// bytes charged against the response budget (`max_response_size`) before the
/// rows are retained, so the budget metric matches the block-based APIs.
pub(super) struct AllRowsHandler<T> {
    rows: Vec<T>,
    budget: ResponseBudget,
}

impl<T> AllRowsHandler<T> {
    /// Budgeted handler allowing `limit` decoded payload bytes.
    pub(super) fn new(limit: usize) -> Self {
        Self {
            rows: Vec::new(),
            budget: ResponseBudget::new(limit),
        }
    }
}

impl<T: crate::row::Row> SelectResponseHandler for AllRowsHandler<T> {
    type Output = Vec<T>;

    async fn on_data(
        &mut self, stream: &mut crate::pool::StreamWrapper, response_compressed: bool,
    ) -> Result<()> {
        let block = read_data_block_maybe_compressed(stream, response_compressed).await?;
        if block.row_count() > 0 {
            charge_block(&mut self.budget, block.payload_bytes())?;
            self.rows.extend(crate::row::read_all::<T>(&block)?);
        }
        Ok(())
    }

    fn finish(self) -> Result<Self::Output> {
        Ok(self.rows)
    }
}

#[derive(Default)]
pub(super) struct RowCountHandler {
    rows: usize,
}

impl SelectResponseHandler for RowCountHandler {
    type Output = usize;

    async fn on_data(
        &mut self, stream: &mut crate::pool::StreamWrapper, response_compressed: bool,
    ) -> Result<()> {
        let rows = discard_data_block_maybe_compressed(stream, response_compressed).await?;
        self.rows = self
            .rows
            .checked_add(rows)
            .ok_or_else(|| crate::error::Error::Protocol("row count overflow".into()))?;
        Ok(())
    }

    async fn on_log_packet(
        &mut self, stream: &mut crate::pool::StreamWrapper, packet_type: u64,
        response_compressed: bool, callbacks: &QueryCallbacks,
    ) -> Result<()> {
        match packet_type {
            10 => {
                let block = read_data_block(stream).await?;
                if let Some(ref cb) = callbacks.on_log {
                    cb(&block);
                }
            },
            14 if callbacks.on_profile_events.is_some() => {
                let block = read_data_block_maybe_compressed(stream, response_compressed).await?;
                if let Some(ref cb) = callbacks.on_profile_events {
                    cb(&block);
                }
            },
            14 => {
                let _ = discard_data_block_maybe_compressed(stream, response_compressed).await?;
            },
            _ => unreachable!(),
        }
        Ok(())
    }

    fn finish(self) -> Result<Self::Output> {
        Ok(self.rows)
    }
}

/// Raw-capture terminal backing [`QueryBuilder::raw`] and
/// `fetch::<RawBlocks>()`. Each retained `RawBlock`'s native payload length
/// is charged against the response budget (`max_response_size`).
pub(super) struct RawBlocksHandler {
    blocks: Vec<RawBlock>,
    budget: ResponseBudget,
}

impl RawBlocksHandler {
    /// Budgeted handler allowing `limit` decoded payload bytes.
    pub(super) fn new(limit: usize) -> Self {
        Self {
            blocks: Vec::new(),
            budget: ResponseBudget::new(limit),
        }
    }
}

impl SelectResponseHandler for RawBlocksHandler {
    type Output = Vec<RawBlock>;

    async fn on_data(
        &mut self, stream: &mut crate::pool::StreamWrapper, _response_compressed: bool,
    ) -> Result<()> {
        let block = read_raw_data_block(stream).await?;
        if block.rows > 0 {
            charge_block(&mut self.budget, block.payload_bytes())?;
            self.blocks.push(block);
        }
        Ok(())
    }

    async fn on_log_packet(
        &mut self, stream: &mut crate::pool::StreamWrapper, packet_type: u64,
        _response_compressed: bool, callbacks: &QueryCallbacks,
    ) -> Result<()> {
        debug_assert!(matches!(packet_type, 10 | 14));
        let block = read_data_block(stream).await?;
        if let Some(ref cb) = callbacks.on_log {
            cb(&block);
        }
        if let Some(ref cb) = callbacks.on_profile_events {
            cb(&block);
        }
        Ok(())
    }

    fn finish(self) -> Result<Self::Output> {
        Ok(self.blocks)
    }
}
