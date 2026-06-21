use crate::connection::block_reader::{
    read_data_block, read_data_block_maybe_compressed, read_table_columns_packet,
};
use crate::connection::io::{
    packet_read_timeout, read_exception, read_profile_info_packet, read_progress_packet,
    read_string_async, read_varint_async, skip_part_uuids_packet,
};
use crate::connection::server_packets::{cancel_and_drain, unsupported_server_packet};
use crate::error::{Error, Result};
use crate::protocol::block::Block;
use crate::runtime::time::Instant;
use std::time::Duration;
use tracing::debug;

pub(super) async fn read_table_structure(
    stream: &mut crate::pool::StreamWrapper, timeout: Duration, response_compressed: bool,
    deadline: Option<Instant>,
) -> Result<Block> {
    loop {
        let typ = match packet_read_timeout(timeout, deadline) {
            Some(per_read) => {
                match crate::runtime::time::timeout(per_read, read_varint_async(stream)).await {
                    Ok(Ok(t)) => t,
                    Ok(Err(e)) => return Err(e),
                    Err(_) => {
                        if deadline.is_some() {
                            cancel_and_drain(stream, timeout, response_compressed).await?;
                            return Err(Error::Timeout("query exceeded deadline".into()));
                        }
                        return Err(Error::Protocol(
                            "timeout waiting for INSERT table structure".into(),
                        ));
                    },
                }
            },
            None => {
                cancel_and_drain(stream, timeout, response_compressed).await?;
                return Err(Error::Timeout("query exceeded deadline".into()));
            },
        };
        match typ {
            1 => return read_data_block_maybe_compressed(stream, response_compressed).await,
            2 => return Err(read_exception(stream).await?),
            3 => {
                let _ = read_progress_packet(stream).await?;
            },
            4 => {},
            5 => {
                return Err(Error::Protocol(
                    "EndOfStream before INSERT table structure".into(),
                ));
            },
            6 => {
                let _ = read_profile_info_packet(stream).await?;
            },
            11 => {
                read_table_columns_packet(stream, response_compressed).await?;
            },
            10 => {
                let _ = read_data_block(stream).await?;
            },
            14 => {
                let _ = read_data_block_maybe_compressed(stream, response_compressed).await?;
            },
            17 => {
                let _timezone = read_string_async(stream).await?;
            },
            12 => {
                skip_part_uuids_packet(stream).await?;
            },
            _ => return Err(unsupported_server_packet(stream, typ).await?),
        }
    }
}

pub(super) async fn drain_response(
    stream: &mut crate::pool::StreamWrapper, timeout: Duration, response_compressed: bool,
    deadline: Option<Instant>,
) -> Result<()> {
    loop {
        let typ = match packet_read_timeout(timeout, deadline) {
            Some(per_read) => {
                match crate::runtime::time::timeout(per_read, read_varint_async(stream)).await {
                    Ok(Ok(t)) => t,
                    Ok(Err(e)) => return Err(e),
                    Err(_) => {
                        if deadline.is_some() {
                            cancel_and_drain(stream, timeout, response_compressed).await?;
                            return Err(Error::Timeout("query exceeded deadline".into()));
                        }
                        return Ok(()); // recv_timeout floor, no deadline: unchanged
                    },
                }
            },
            None => {
                cancel_and_drain(stream, timeout, response_compressed).await?;
                return Err(Error::Timeout("query exceeded deadline".into()));
            },
        };
        debug!(packet_type = typ, "received packet");
        match typ {
            5 => return Ok(()),
            1 => {
                let _ = read_data_block_maybe_compressed(stream, response_compressed).await;
            },
            2 => {
                let _ = read_exception(stream).await;
                return Ok(());
            },
            3 => {
                let _ = read_progress_packet(stream).await;
            },
            4 => {},
            6 => {
                let _ = read_profile_info_packet(stream).await;
            },
            10 => {
                let _ = read_data_block(stream).await;
            },
            14 => {
                let _ = read_data_block_maybe_compressed(stream, response_compressed).await;
            },
            17 => {
                let _ = read_string_async(stream).await;
            },
            12 => {
                skip_part_uuids_packet(stream).await?;
            },
            11 => {
                read_table_columns_packet(stream, response_compressed).await?;
            },
            _ => return Err(unsupported_server_packet(stream, typ).await?),
        }
    }
}
