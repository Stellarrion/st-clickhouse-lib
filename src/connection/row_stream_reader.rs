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
use crate::runtime::io::AsyncWriteExt;
use crate::runtime::sync::mpsc;

/// Read packets from the stream, sending Data blocks via the channel.
/// The stream is owned exclusively by this task, so no mutex is needed.
pub(super) async fn read_query_blocks(
    mut stream: crate::pool::StreamWrapper, block_tx: &mpsc::Sender<Result<Option<Block>>>,
    callbacks: &QueryCallbacks, cancel: Option<&std::sync::atomic::AtomicBool>,
    recv_timeout: std::time::Duration, deadline: Option<crate::runtime::time::Instant>,
) -> Result<()> {
    use crate::connection::io::packet_read_timeout;
    loop {
        if let Some(c) = cancel {
            if c.load(std::sync::atomic::Ordering::Relaxed) {
                stream
                    .write_packet(&[ClientPacket::Cancel as u8])
                    .await
                    .ok();
                stream.flush().await.ok();
                return Ok(());
            }
        }
        let packet_type = match packet_read_timeout(recv_timeout, deadline) {
            Some(per_read) => {
                match crate::runtime::time::timeout(per_read, read_varint_async(&mut stream)).await
                {
                    Ok(Ok(t)) => t,
                    Ok(Err(e)) => {
                        let _ = block_tx.send(Err(e)).await;
                        return Ok(());
                    },
                    Err(_) => {
                        if deadline.is_some() {
                            stream
                                .write_packet(&[ClientPacket::Cancel as u8])
                                .await
                                .ok();
                            stream.flush().await.ok();
                            let _ = block_tx
                                .send(Err(crate::error::Error::Timeout(
                                    "query exceeded deadline".into(),
                                )))
                                .await;
                            return Ok(());
                        }
                        let _ = block_tx
                            .send(Err(crate::error::Error::Protocol("timeout".into())))
                            .await;
                        return Ok(());
                    },
                }
            },
            None => {
                stream
                    .write_packet(&[ClientPacket::Cancel as u8])
                    .await
                    .ok();
                stream.flush().await.ok();
                let _ = block_tx
                    .send(Err(crate::error::Error::Timeout(
                        "query exceeded deadline".into(),
                    )))
                    .await;
                return Ok(());
            },
        };
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
