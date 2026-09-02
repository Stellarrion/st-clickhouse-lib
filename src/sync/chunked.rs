use crate::sync::config::ClientConfig;
use crate::sync::error::{Error, Result};
use crate::sync::protocol::handshake::ServerInfo;
use crate::sync::protocol::revision;
use std::io::{Read, Write};

pub(super) struct ChunkedNegotiation {
    pub(super) send_mode: &'static str,
    pub(super) recv_mode: &'static str,
    pub(super) send_chunked: bool,
    pub(super) recv_chunked: bool,
}

pub(super) fn negotiate_chunked_transport(
    server_info: &ServerInfo, config: &ClientConfig,
) -> Result<ChunkedNegotiation> {
    if server_info.negotiated_revision < revision::DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS {
        let requested_send_chunked = config.chunked_mode.0 == "chunked";
        let requested_recv_chunked = config.chunked_mode.1 == "chunked";
        if requested_send_chunked || requested_recv_chunked {
            return Err(Error::Protocol(
                "server revision does not support chunked native protocol".into(),
            ));
        }
        return Ok(ChunkedNegotiation {
            send_mode: "notchunked",
            recv_mode: "notchunked",
            send_chunked: false,
            recv_chunked: false,
        });
    }

    let send_chunked = choose_chunked_mode(
        &server_info.proto_recv_chunked_srv,
        &config.chunked_mode.0,
        "send",
    )?;
    let recv_chunked = choose_chunked_mode(
        &server_info.proto_send_chunked_srv,
        &config.chunked_mode.1,
        "recv",
    )?;

    Ok(ChunkedNegotiation {
        send_mode: if send_chunked {
            "chunked"
        } else {
            "notchunked"
        },
        recv_mode: if recv_chunked {
            "chunked"
        } else {
            "notchunked"
        },
        send_chunked,
        recv_chunked,
    })
}

pub(super) fn choose_chunked_mode(
    server_capability: &str, client_capability: &str, direction: &str,
) -> Result<bool> {
    let server_chunked = server_capability.starts_with("chunked");
    let server_optional = server_capability.ends_with("_optional");
    let client_chunked = client_capability.starts_with("chunked");
    let client_optional = client_capability.ends_with("_optional");

    if server_optional {
        return Ok(client_chunked);
    }
    if client_optional {
        return Ok(server_chunked);
    }
    if client_chunked != server_chunked {
        return Err(Error::Protocol(format!(
            "incompatible chunked protocol for {direction}: client requests {}, server requires {}",
            if client_chunked {
                "chunked"
            } else {
                "notchunked"
            },
            if server_chunked {
                "chunked"
            } else {
                "notchunked"
            },
        )));
    }
    Ok(server_chunked)
}

pub(super) fn write_chunk_header<W: Write>(writer: &mut W, len: usize) -> Result<()> {
    let len = u32::try_from(len)
        .map_err(|_| Error::Protocol(format!("chunked packet too large: {len} bytes")))?;
    writer.write_all(&len.to_le_bytes())?;
    Ok(())
}

pub(super) fn write_chunked_packet<W: Write>(writer: &mut W, pkt: &[u8]) -> Result<()> {
    write_chunk_header(writer, pkt.len())?;
    writer.write_all(pkt)?;
    writer.write_all(&0u32.to_le_bytes())?;
    Ok(())
}

/// Send a bare Cancel packet through a writer, chunked-framed when required.
///
/// Used by the response-budget recovery path, which must write through the
/// SAME buffered reader instance that performed the aborted read (dropping it
/// would lose its read-ahead buffer and desynchronize the recovery drain).
pub(super) fn write_cancel_packet<W: Write>(writer: &mut W, chunked_send: bool) -> Result<()> {
    if chunked_send {
        write_chunked_packet(writer, &[3])
    } else {
        writer.write_all(&[3])?;
        writer.flush()?;
        Ok(())
    }
}

pub(super) struct TransportReader<'a> {
    inner: std::io::BufReader<&'a mut crate::sync::transport::Transport>,
}

impl<'a> TransportReader<'a> {
    pub(super) fn new(stream: &'a mut crate::sync::transport::Transport, capacity: usize) -> Self {
        Self {
            inner: std::io::BufReader::with_capacity(capacity, stream),
        }
    }
}

impl Read for TransportReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Write for TransportReader<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.get_mut().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.get_mut().flush()
    }
}

pub(super) struct ChunkedReader<R> {
    inner: R,
    chunk: Vec<u8>,
    pos: usize,
}

impl<R: Read> ChunkedReader<R> {
    pub(super) fn new(inner: R) -> Self {
        Self {
            inner,
            chunk: Vec::new(),
            pos: 0,
        }
    }

    fn read_next_chunk(&mut self) -> std::io::Result<()> {
        loop {
            let mut len_buf = [0u8; 4];
            self.inner.read_exact(&mut len_buf)?;
            let len = u32::from_le_bytes(len_buf) as usize;
            if len == 0 {
                continue;
            }
            // The chunk length is server-controlled; validate it before the
            // resize so a 4-byte header cannot drive a multi-GiB allocation.
            if len > crate::limits::MAX_CHUNK_LEN {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "chunked transport chunk length {len} exceeds maximum {}",
                        crate::limits::MAX_CHUNK_LEN
                    ),
                ));
            }
            self.chunk.resize(len, 0);
            self.inner.read_exact(&mut self.chunk)?;
            self.pos = 0;
            return Ok(());
        }
    }
}

impl<R: Read> Read for ChunkedReader<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if self.pos >= self.chunk.len() {
            self.read_next_chunk()?;
        }
        let n = out.len().min(self.chunk.len() - self.pos);
        out[..n].copy_from_slice(&self.chunk[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

impl<R: Write> Write for ChunkedReader<R> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A server-supplied `u32::MAX` chunk header must fail the length cap
    /// before any buffer is sized, not attempt a 4 GiB read/allocation.
    #[test]
    fn chunked_reader_rejects_oversized_chunk_header() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&u32::MAX.to_le_bytes());
        let mut reader = ChunkedReader::new(std::io::Cursor::new(wire));
        let mut out = [0u8; 8];
        let err = reader
            .read(&mut out)
            .expect_err("oversized chunk header must be rejected");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("chunk length"),
            "expected chunk length error, got: {err}"
        );
    }

    /// The cap boundary itself stays readable: a small well-formed chunk
    /// still round-trips through the reader after the check was added.
    #[test]
    fn chunked_reader_still_reads_small_chunk() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&3u32.to_le_bytes());
        wire.extend_from_slice(b"abc");
        let mut reader = ChunkedReader::new(std::io::Cursor::new(wire));
        let mut out = [0u8; 8];
        let n = reader.read(&mut out).expect("small chunk must decode");
        assert_eq!(&out[..n], b"abc");
    }

    /// Outbound framing keeps its checked conversion: a hypothetical chunk
    /// larger than `u32::MAX` is refused instead of silently truncated.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn chunk_header_writer_refuses_oversized_packet() {
        let mut sink = Vec::new();
        // Use a length beyond the u32 wire field without allocating it.
        let huge = u32::MAX as usize + 1;
        let err = write_chunk_header(&mut sink, huge)
            .expect_err("packet larger than the u32 header must be refused");
        assert!(
            err.to_string().contains("too large"),
            "expected too-large error, got: {err}"
        );
        assert!(sink.is_empty(), "nothing must be written on refusal");
    }
}
