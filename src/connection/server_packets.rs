use crate::connection::callbacks::QueryCallbacks;
use crate::connection::io::{
    build_empty_cluster_function_read_task_response, build_finished_merge_tree_read_task_response,
    read_parallel_read_request_stream_id, read_part_uuids_packet, read_string_async,
    skip_parallel_read_announcement,
};
use crate::error::{Error, Result};
use crate::runtime::io::AsyncWriteExt;

pub(super) async fn read_timezone_update<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, callbacks: &QueryCallbacks,
) -> Result<()> {
    let timezone = read_string_async(stream).await?;
    if let Some(ref cb) = callbacks.on_timezone_update {
        cb(&timezone);
    }
    Ok(())
}

pub(super) async fn read_part_uuids_update<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, callbacks: &QueryCallbacks,
) -> Result<()> {
    let uuids = read_part_uuids_packet(stream).await?;
    if let Some(ref cb) = callbacks.on_part_uuids {
        cb(&uuids);
    }
    Ok(())
}

pub(super) async fn unsupported_server_packet<
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
>(
    stream: &mut S, packet_type: u64,
) -> Result<Error> {
    Ok(match packet_type {
        9 => Error::Protocol(
            "unexpected TablesStatusResponse packet; table-status requests are not issued by this client".into(),
        ),
        13 => Error::Protocol(
            "server sent ReadTaskRequest; cluster-function read-task responses are not supported by this client".into(),
        ),
        15 => Error::Protocol(
            "server sent MergeTreeAllRangesAnnouncement; parallel-replica coordinator packets are not supported by this client".into(),
        ),
        16 => Error::Protocol(
            "server sent MergeTreeReadTaskRequest; parallel-replica read-task responses are not supported by this client".into(),
        ),
        18 => {
            let challenge = read_string_async(stream).await?;
            Error::Protocol(format!(
                "server sent SSHChallenge{}; SSH-key authentication is not supported by this client",
                if challenge.is_empty() { "" } else { " packet" }
            ))
        },
        other => Error::Protocol(format!("unknown packet type: {other}")),
    })
}

pub(super) async fn handle_coordinator_packet(
    stream: &mut crate::pool::StreamWrapper, packet_type: u64,
) -> Result<bool> {
    match packet_type {
        13 => {
            let pkt = build_empty_cluster_function_read_task_response();
            stream.write_packet(&pkt).await?;
            stream.flush().await?;
            Ok(true)
        },
        15 => {
            skip_parallel_read_announcement(stream).await?;
            Ok(true)
        },
        16 => {
            let stream_id = read_parallel_read_request_stream_id(stream).await?;
            let pkt = build_finished_merge_tree_read_task_response(&stream_id);
            stream.write_packet(&pkt).await?;
            stream.flush().await?;
            Ok(true)
        },
        _ => Ok(false),
    }
}

pub(super) async fn write_ignored_part_uuids_if_any(
    stream: &mut crate::pool::StreamWrapper, uuids: &[[u8; 16]],
) -> Result<()> {
    if uuids.is_empty() {
        return Ok(());
    }
    let pkt = crate::protocol::part_uuid::build_ignored_part_uuids_packet(uuids);
    stream.write_packet(&pkt).await?;
    Ok(())
}

use crate::connection::block_reader::{read_data_block, read_data_block_maybe_compressed};
use crate::connection::io::{
    read_exception, read_profile_info_packet, read_progress_packet, read_varint_async,
    skip_part_uuids_packet,
};
use crate::protocol::packet::ClientPacket;

/// Send a `Cancel` packet and drain the response until `EndOfStream` or
/// `Exception`, bounded by `min(recv_timeout, 5 seconds)` for the whole drain.
///
/// Used when a query deadline elapses. `Cancel` is sent via `write_packet`
/// (not a bare write) so the byte is framed correctly when the connection
/// negotiated chunked-send mode.
///
/// This is a self-contained leaf loop — it intentionally does **not** call
/// [`drain_response`], so the deadline path in `drain_response` /
/// `read_table_structure` / `read_select_response` stays non-recursive
/// (calling back into `drain_response` here would form a mutual async
/// recursion the compiler rejects).
///
/// A timeout, read error, or unexpected packet returns `ConnectionClosed` so
/// the caller can discard the unclean socket rather than return it to the pool.
pub(crate) async fn cancel_and_drain(
    stream: &mut crate::pool::StreamWrapper, recv_timeout: std::time::Duration,
    response_compressed: bool,
) -> crate::error::Result<()> {
    // Cancellation must be bounded as a whole, not only while waiting for the
    // next packet header: a server can keep sending bodies indefinitely or
    // stall midway through one. Five seconds is enough for a graceful drain;
    // after that the caller must discard this connection and reconnect.
    let budget = recv_timeout.min(std::time::Duration::from_secs(5));
    let deadline = crate::runtime::time::Instant::now() + budget;
    match crate::runtime::time::timeout(
        budget,
        cancel_and_drain_inner(stream, response_compressed, deadline),
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(crate::error::Error::ConnectionClosed(format!(
            "cancel drain failed: {error}"
        ))),
        Err(_) => Err(crate::error::Error::ConnectionClosed(format!(
            "cancel drain did not finish within {budget:?}"
        ))),
    }
}

async fn cancel_and_drain_inner(
    stream: &mut crate::pool::StreamWrapper, response_compressed: bool,
    deadline: crate::runtime::time::Instant,
) -> crate::error::Result<()> {
    stream.write_packet(&[ClientPacket::Cancel as u8]).await?;
    stream.flush().await?;
    loop {
        // Tokio timeouts require the inner future to yield. A busy server can
        // keep every socket read immediately ready, so also enforce the wall
        // clock explicitly between packets.
        if crate::runtime::time::Instant::now() >= deadline {
            return Err(crate::error::Error::Timeout(
                "cancel drain deadline exceeded".into(),
            ));
        }
        match read_varint_async(stream).await? {
            5 => return Ok(()),
            2 => {
                // A server exception is terminal for this cancelled query.
                let _ = read_exception(stream).await?;
                return Ok(());
            },
            1 | 14 => {
                let _ = read_data_block_maybe_compressed(stream, response_compressed).await?;
            },
            10 => {
                let _ = read_data_block(stream).await?;
            },
            3 => {
                let _ = read_progress_packet(stream).await?;
            },
            6 => {
                let _ = read_profile_info_packet(stream).await?;
            },
            17 => {
                let _ = read_string_async(stream).await?;
            },
            12 => skip_part_uuids_packet(stream).await?,
            4 => {},
            packet_type => {
                return Err(crate::error::Error::Protocol(format!(
                    "unexpected packet {packet_type} while draining cancelled query"
                )));
            },
        }
    }
}
