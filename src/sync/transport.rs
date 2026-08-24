//! Transport abstraction — plain TCP or TLS-wrapped TCP.
//!
//! `Transport` wraps either a raw `std::net::TcpStream` or a TLS session.
//! This allows `SyncClient` to work over both plain and encrypted connections.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

#[cfg(feature = "tls")]
use crate::sync::error::{Error, Result};

/// A TCP connection that may be TLS-encrypted.
pub enum Transport {
    /// Plain TCP connection.
    Plain(TcpStream),
    /// TLS-encrypted connection.
    #[cfg(feature = "tls")]
    Tls(rustls::StreamOwned<rustls::ClientConnection, TcpStream>),
}

impl Transport {
    /// Create a plain TCP transport.
    pub fn new_plain(stream: TcpStream) -> Self {
        Transport::Plain(stream)
    }

    /// Create a TLS-wrapped transport.
    #[cfg(feature = "tls")]
    pub fn new_tls(
        stream: TcpStream, config: std::sync::Arc<rustls::ClientConfig>, domain: &str,
    ) -> Result<Self> {
        let name = rustls::pki_types::ServerName::try_from(domain.to_owned())
            .map_err(|_| Error::Protocol(format!("invalid TLS domain '{domain}'")))?;
        let conn = rustls::ClientConnection::new(config, name)
            .map_err(|e| Error::Protocol(format!("TLS handshake failed: {e}")))?;
        let tls = rustls::StreamOwned::new(conn, stream);
        Ok(Transport::Tls(tls))
    }

    /// Try to clone the transport for streaming queries.
    ///
    /// Plain TCP can be cloned at the socket level. TLS sessions cannot be
    /// safely cloned because the rustls connection state is part of the stream.
    pub fn try_clone(&self) -> std::io::Result<Self> {
        match self {
            Transport::Plain(s) => s.try_clone().map(Transport::Plain),
            #[cfg(feature = "tls")]
            Transport::Tls(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "streaming query clone is unsupported for TLS transports",
            )),
        }
    }

    /// Get a reference to the underlying TCP stream.
    pub fn raw_tcp(&self) -> &TcpStream {
        match self {
            Transport::Plain(s) => s,
            #[cfg(feature = "tls")]
            Transport::Tls(s) => &s.sock,
        }
    }
}

impl Read for Transport {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Transport::Plain(s) => s.read(buf),
            #[cfg(feature = "tls")]
            Transport::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Transport {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Transport::Plain(s) => s.write(buf),
            #[cfg(feature = "tls")]
            Transport::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Transport::Plain(s) => s.flush(),
            #[cfg(feature = "tls")]
            Transport::Tls(s) => s.flush(),
        }
    }
}

impl Transport {
    /// Set read timeout on the underlying TCP socket.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> std::io::Result<()> {
        self.raw_tcp().set_read_timeout(timeout)
    }

    /// Set write timeout on the underlying TCP socket.
    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> std::io::Result<()> {
        self.raw_tcp().set_write_timeout(timeout)
    }

    /// Set TCP_NODELAY.
    pub fn set_nodelay(&self, nodelay: bool) -> std::io::Result<()> {
        self.raw_tcp().set_nodelay(nodelay)
    }
}
