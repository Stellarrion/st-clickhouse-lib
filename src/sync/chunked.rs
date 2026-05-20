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
