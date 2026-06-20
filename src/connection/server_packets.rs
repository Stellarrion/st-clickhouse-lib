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

use crate::connection::response_wait::drain_response;
use crate::protocol::packet::ClientPacket;

/// Send a `Cancel` packet and drain the response until `EndOfStream` /
/// `Exception` (bounded by `recv_timeout`).
///
/// Used when a query deadline elapses. Best-effort: if the server ignores
/// `Cancel` and the drain itself times out, this returns `Ok` and leaves the
/// connection to be reaped by the pool's liveness ping on next acquire.
///
/// `drain_response` is called with `deadline = None` so it never recurses
/// into cancel logic.
#[allow(dead_code)]
pub(crate) async fn cancel_and_drain<S>(
    stream: &mut S, recv_timeout: std::time::Duration, response_compressed: bool,
) -> Result<()>
where
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
{
    use crate::runtime::io::AsyncWriteExt;
    stream.write_all(&[ClientPacket::Cancel as u8]).await.ok();
    stream.flush().await.ok();
    drain_response(stream, recv_timeout, response_compressed, None).await
}
