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
            // A server exception terminates the response for execute()/INSERT
            // end(): surface it (mirrors read_select_response). The
            // best-effort cancellation drain in cancel_and_drain stays
            // lenient on purpose.
            2 => return Err(read_exception(stream).await?),
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

#[cfg(test)]
mod tests {
    use super::drain_response;
    use crate::error::Error;
    use crate::runtime::io::AsyncWriteExt;
    use std::time::Duration;

    fn put_varint(buf: &mut Vec<u8>, v: u64) {
        crate::connection::io::encode_varint(buf, v);
    }

    fn put_string(buf: &mut Vec<u8>, s: &str) {
        put_varint(buf, s.len() as u64);
        buf.extend_from_slice(s.as_bytes());
    }

    /// Wire bytes for an Exception packet (type 2) with no nested exception.
    fn exception_packet(code: i32, name: &str, message: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        put_varint(&mut buf, 2); // ServerPacket::Exception
        buf.extend_from_slice(&code.to_le_bytes());
        put_string(&mut buf, name);
        put_string(&mut buf, message);
        put_string(&mut buf, ""); // stack trace
        buf.push(0); // has_nested = false
        buf
    }

    /// Client-side `StreamWrapper` fed by a one-shot local TCP server that
    /// writes `payload` and holds the socket open briefly.
    async fn stream_with_payload(payload: Vec<u8>) -> crate::pool::StreamWrapper {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("listener local addr");
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.expect("accept client");
            sock.write_all(&payload).await.expect("write payload");
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        let tcp = tokio::net::TcpStream::connect(addr).await.expect("connect");
        crate::pool::StreamWrapper::tcp(tcp)
    }

    #[tokio::test]
    async fn drain_response_propagates_server_exception() {
        let payload = exception_packet(60, "DB::Exception", "unknown function xyz");
        let mut stream = stream_with_payload(payload).await;
        let err = drain_response(&mut stream, Duration::from_secs(5), false, None)
            .await
            .expect_err("server exception must propagate as Err");
        assert!(
            matches!(err, Error::ServerError { code: 60, .. }),
            "expected ServerError code 60, got {err:?}"
        );
        let Error::ServerError { message, .. } = &err else {
            unreachable!("matched ServerError above");
        };
        assert!(
            message.contains("unknown function xyz"),
            "message: {message}"
        );
    }

    #[tokio::test]
    async fn drain_response_end_of_stream_stays_ok() {
        let payload = vec![5u8]; // EndOfStream
        let mut stream = stream_with_payload(payload).await;
        drain_response(&mut stream, Duration::from_secs(5), false, None)
            .await
            .expect("EndOfStream drains to Ok");
    }

    #[tokio::test]
    async fn cancel_and_drain_stays_best_effort_on_exception() {
        // The cancellation drain must keep swallowing exceptions: it runs
        // after a deadline trip where the Timeout error is already decided.
        let mut payload = exception_packet(159, "DB::Exception", "cancelled");
        payload.push(5); // EndOfStream
        let mut stream = stream_with_payload(payload).await;
        crate::connection::server_packets::cancel_and_drain(
            &mut stream,
            Duration::from_secs(5),
            false,
        )
        .await
        .expect("best-effort cancellation drain returns Ok on exception");
    }
}
