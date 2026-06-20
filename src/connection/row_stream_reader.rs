use crate::connection::block_reader::read_data_block;
use crate::connection::callbacks::QueryCallbacks;
use crate::connection::io::{
    read_exception, read_profile_info_packet, read_progress_packet, read_varint_async,
};
use crate::connection::server_packets::{
    handle_coordinator_packet, read_part_uuids_update, read_timezone_update,
    unsupported_server_packet,
};
use crate::error::Result;
use crate::protocol::block::Block;
use crate::protocol::packet::ClientPacket;
use crate::runtime::sync::mpsc;

/// Read packets from the stream, sending Data blocks via the channel.
/// The stream is owned exclusively by this task, so no mutex is needed.
pub(super) async fn read_query_blocks(
    mut stream: crate::pool::StreamWrapper, block_tx: &mpsc::Sender<Result<Option<Block>>>,
    callbacks: &QueryCallbacks, cancel: Option<&std::sync::atomic::AtomicBool>,
) -> Result<()> {
    loop {
        if let Some(c) = cancel {
            if c.load(std::sync::atomic::Ordering::Relaxed) {
                crate::runtime::io::AsyncWriteExt::write_all(
                    &mut stream,
                    &[ClientPacket::Cancel as u8],
                )
                .await
                .ok();
                return Ok(());
            }
        }
        let packet_type = read_varint_async(&mut stream).await?;
        match packet_type {
            1 => {
                let block = read_data_block(&mut stream).await?;
                if block.row_count() > 0 && block_tx.send(Ok(Some(block))).await.is_err() {
                    return Ok(());
                }
            },
            2 => {
                let err = read_exception(&mut stream).await?;
                let _ = block_tx.send(Err(err)).await;
                return Ok(());
            },
            3 => {
                let progress = read_progress_packet(&mut stream).await?;
                if let Some(ref cb) = callbacks.on_progress {
                    cb(progress);
                }
            },
            4 => {},
            5 => {
                let _ = block_tx.send(Ok(None)).await;
                return Ok(());
            },
            6 => {
                let _ = read_profile_info_packet(&mut stream).await?;
            },
            10 | 14 => {
                let _ = read_data_block(&mut stream).await?;
            },
            17 => {
                read_timezone_update(&mut stream, callbacks).await?;
            },
            12 => {
                read_part_uuids_update(&mut stream, callbacks).await?;
            },
            _ => {
                if handle_coordinator_packet(&mut stream, packet_type).await? {
                    continue;
                }
                let err = unsupported_server_packet(&mut stream, packet_type).await?;
                let _ = block_tx.send(Err(err)).await;
                return Ok(());
            },
        }
    }
}
