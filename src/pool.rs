//! Connection pool with per-slot async mutexes and round-robin selection.
//!
//! Architecture:
//!   - Per-slot `crate::runtime::sync::Mutex` — guards each `Option<Connection>`
//!   - Atomic round-robin index (`next_idx`) — assigns slots without a free-list
//!     lock
//!
//! No blocking mutex in any async path. Each concurrent user typically locks a
//! different slot; when more callers contend than there are slots, the wait for
//! a free slot is optionally bounded by `acquire_timeout` (default: unbounded).
//! `PoolGuard::drop` drops the slot guard, waking the next waiter.

use crate::error::Result;
use crate::protocol::handshake;
use crate::protocol::packet::ClientPacket;
use crate::protocol::revision;
use crate::runtime::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use crate::runtime::sync::{Mutex as AsyncMutex, MutexGuard};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tracing::Instrument;
use zeroize::Zeroize;

// ---------------------------------------------------------------------------
// Transport abstraction
// ---------------------------------------------------------------------------

/// Wraps either a raw TCP stream or a TLS-wrapped stream.
pub(crate) enum StreamInner {
    /// Plain TCP connection.
    Tcp(crate::runtime::net::TcpStream),
    /// TLS-wrapped TCP connection (feature = "tokio-tls").
    #[cfg(feature = "tokio-tls")]
    Tls(tokio_rustls::TlsStream<crate::runtime::net::TcpStream>),
}

/// Native transport wrapper.
///
/// In chunked receive mode, `AsyncRead` strips ClickHouse chunk headers and
/// zero-length chunk boundaries so the protocol parsers can continue reading a
/// plain native byte stream. Writes are not implicitly chunked; callers use
/// [`StreamWrapper::write_packet`] for top-level native packets.
pub(crate) struct StreamWrapper {
    inner: StreamInner,
    metrics: Option<&'static crate::metrics::Metrics>,
    use_chunked_send: bool,
    use_chunked_recv: bool,
    send_timeout: Option<std::time::Duration>,
    chunk: Vec<u8>,
    chunk_pos: usize,
    chunk_fill: usize,
    len_buf: [u8; 4],
    len_pos: usize,
    /// Raw-framing prefetch buffer. Bytes read ahead from the socket and served
    /// across many small reads, so byte-wise reads don't each hit the socket.
    /// Only used when `!use_chunked_recv` (the chunked path buffers
    /// length-prefixed frames in `chunk`). Prefetched bytes are later packets
    /// of the same response, consumed in order by subsequent reads; after
    /// EndOfStream the buffer is drained and the server sends nothing more.
    rd_buf: Box<[u8]>,
    rd_pos: usize,
    rd_fill: usize,
}

/// Capacity of the raw-framing read prefetch buffer. Bytes read ahead from
/// the socket are served to many small reads (varints, block headers, string
/// bodies) without each polling the socket.
const READ_BUF_CAP: usize = 8 * 1024;

impl AsyncRead for StreamWrapper {
    fn poll_read(
        self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
        buf: &mut crate::runtime::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if !this.use_chunked_recv {
            return read_buffered(this, cx, buf);
        }

        loop {
            // Serve unconsumed *received* payload bytes. Only `chunk_fill`
            // bytes are real: after `resize(len, 0)` but before the payload
            // read completes, the tail of `chunk` is still zero-fill and must
            // never be handed to the protocol parsers.
            if this.chunk_pos < this.chunk_fill {
                let n = buf.remaining().min(this.chunk_fill - this.chunk_pos);
                if n == 0 {
                    return std::task::Poll::Ready(Ok(()));
                }
                buf.put_slice(&this.chunk[this.chunk_pos..this.chunk_pos + n]);
                this.chunk_pos += n;
                return std::task::Poll::Ready(Ok(()));
            }

            // A chunk is in progress while `chunk.len() > 0`: either fully
            // received and consumed (prepare the next one) or partially
            // received (fall through and resume the payload read — the 4-byte
            // length was already consumed and must not be re-read).
            if !this.chunk.is_empty() && this.chunk_fill == this.chunk.len() {
                this.chunk.clear();
                this.chunk_pos = 0;
                this.chunk_fill = 0;
            }

            if this.chunk.is_empty() {
                while this.len_pos < this.len_buf.len() {
                    let before = this.len_buf.len() - this.len_pos;
                    let mut len_read = ReadBuf::new(&mut this.len_buf[this.len_pos..]);
                    match poll_inner_read(&mut this.inner, cx, &mut len_read) {
                        std::task::Poll::Ready(Ok(())) => {
                            let n = before - len_read.remaining();
                            if n == 0 {
                                return std::task::Poll::Ready(Err(std::io::Error::new(
                                    std::io::ErrorKind::UnexpectedEof,
                                    "unexpected EOF while reading chunk length",
                                )));
                            }
                            this.record_bytes(n);
                            this.len_pos += n;
                        },
                        std::task::Poll::Ready(Err(e)) => return std::task::Poll::Ready(Err(e)),
                        std::task::Poll::Pending => return std::task::Poll::Pending,
                    }
                }

                let len = u32::from_le_bytes(this.len_buf) as usize;
                this.len_pos = 0;
                if len == 0 {
                    continue;
                }
                // The chunk length is server-controlled; validate it before the
                // resize so a 4-byte header cannot drive a multi-GiB allocation.
                if len > crate::limits::MAX_CHUNK_LEN {
                    return std::task::Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "chunked transport chunk length {len} exceeds maximum {}",
                            crate::limits::MAX_CHUNK_LEN
                        ),
                    )));
                }

                this.chunk.resize(len, 0);
            }

            while this.chunk_fill < this.chunk.len() {
                let before = this.chunk.len() - this.chunk_fill;
                let mut chunk_read = ReadBuf::new(&mut this.chunk[this.chunk_fill..]);
                match poll_inner_read(&mut this.inner, cx, &mut chunk_read) {
                    std::task::Poll::Ready(Ok(())) => {
                        let n = before - chunk_read.remaining();
                        if n == 0 {
                            return std::task::Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "unexpected EOF while reading chunk payload",
                            )));
                        }
                        this.record_bytes(n);
                        this.chunk_fill += n;
                    },
                    std::task::Poll::Ready(Err(e)) => return std::task::Poll::Ready(Err(e)),
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                }
            }
        }
    }
}

fn poll_inner_read(
    inner: &mut StreamInner, cx: &mut std::task::Context<'_>, buf: &mut ReadBuf<'_>,
) -> std::task::Poll<std::io::Result<()>> {
    match inner {
        StreamInner::Tcp(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        #[cfg(feature = "tokio-tls")]
        StreamInner::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
    }
}

/// Raw-framing read: serve prefetched bytes first; when drained, bulk-prefetch
/// up to [`READ_BUF_CAP`] bytes from the socket and loop to drain. Prefetched
/// bytes belong to later packets of the same response and are consumed in order
/// by subsequent reads, so nothing is lost across reads.
fn read_buffered(
    this: &mut StreamWrapper, cx: &mut std::task::Context<'_>,
    buf: &mut crate::runtime::io::ReadBuf<'_>,
) -> std::task::Poll<std::io::Result<()>> {
    // Large-read fast path: when the caller wants at least a full window and
    // no prefetched bytes are pending, read straight into the caller's buffer
    // instead of bouncing through the 8 KiB window (mirrors std BufReader).
    // Chunked receive never reaches this function — poll_read routes it to
    // its own frame-serving loop above.
    if this.rd_pos == this.rd_fill && buf.remaining() >= this.rd_buf.len() {
        let before = buf.remaining();
        return match poll_inner_read(&mut this.inner, cx, buf) {
            std::task::Poll::Ready(Ok(())) => {
                let n = before - buf.remaining();
                if n > 0 {
                    this.record_bytes(n);
                }
                std::task::Poll::Ready(Ok(()))
            },
            other => other,
        };
    }
    loop {
        if this.rd_pos < this.rd_fill {
            let n = buf.remaining().min(this.rd_fill - this.rd_pos);
            if n == 0 {
                return std::task::Poll::Ready(Ok(()));
            }
            buf.put_slice(&this.rd_buf[this.rd_pos..this.rd_pos + n]);
            this.rd_pos += n;
            return std::task::Poll::Ready(Ok(()));
        }
        // Buffer fully consumed (rd_pos >= rd_fill) — prefetch a full run from
        // the socket. Only commit rd_pos/rd_fill AFTER a successful read: a
        // Pending or EOF return must leave the (empty) buffer state untouched so
        // the next poll refills instead of re-serving consumed bytes.
        let cap = this.rd_buf.len();
        let filled = {
            let mut rd = crate::runtime::io::ReadBuf::new(&mut this.rd_buf[..]);
            match poll_inner_read(&mut this.inner, cx, &mut rd) {
                std::task::Poll::Ready(Ok(())) => cap - rd.remaining(),
                std::task::Poll::Ready(Err(e)) => return std::task::Poll::Ready(Err(e)),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        };
        this.rd_pos = 0;
        this.rd_fill = filled;
        if filled == 0 {
            // Clean EOF: report it (read_exact callers turn this into UnexpectedEof).
            return std::task::Poll::Ready(Ok(()));
        }
        this.record_bytes(filled);
    }
}

impl AsyncWrite for StreamWrapper {
    fn poll_write(
        self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut self.get_mut().inner {
            StreamInner::Tcp(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tokio-tls")]
            StreamInner::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.get_mut().inner {
            StreamInner::Tcp(s) => std::pin::Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tokio-tls")]
            StreamInner::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.get_mut().inner {
            StreamInner::Tcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tokio-tls")]
            StreamInner::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl StreamWrapper {
    pub(crate) fn tcp(stream: crate::runtime::net::TcpStream) -> Self {
        Self {
            inner: StreamInner::Tcp(stream),
            metrics: None,
            use_chunked_send: false,
            use_chunked_recv: false,
            send_timeout: None,
            chunk: Vec::new(),
            chunk_pos: 0,
            chunk_fill: 0,
            len_buf: [0; 4],
            len_pos: 0,
            rd_buf: vec![0u8; READ_BUF_CAP].into_boxed_slice(),
            rd_pos: 0,
            rd_fill: 0,
        }
    }

    #[cfg(feature = "tokio-tls")]
    pub(crate) fn tls(stream: tokio_rustls::TlsStream<crate::runtime::net::TcpStream>) -> Self {
        Self {
            inner: StreamInner::Tls(stream),
            metrics: None,
            use_chunked_send: false,
            use_chunked_recv: false,
            send_timeout: None,
            chunk: Vec::new(),
            chunk_pos: 0,
            chunk_fill: 0,
            len_buf: [0; 4],
            len_pos: 0,
            rd_buf: vec![0u8; READ_BUF_CAP].into_boxed_slice(),
            rd_pos: 0,
            rd_fill: 0,
        }
    }

    pub(crate) fn set_chunked(&mut self, send: bool, recv: bool) {
        self.use_chunked_send = send;
        self.use_chunked_recv = recv;
        self.chunk.clear();
        self.chunk_pos = 0;
        self.chunk_fill = 0;
        self.len_pos = 0;
        // Drop any raw-framing prefetch so toggling modes mid-life can't serve
        // stale bytes from the other path's buffer.
        self.rd_pos = 0;
        self.rd_fill = 0;
    }

    pub(crate) fn set_metrics(&mut self, metrics: Option<&'static crate::metrics::Metrics>) {
        self.metrics = metrics;
    }

    pub(crate) fn set_send_timeout(&mut self, timeout: Option<std::time::Duration>) {
        self.send_timeout = timeout;
    }

    fn record_bytes(&self, n: usize) {
        if n == 0 {
            return;
        }
        if let Some(metrics) = self.metrics {
            metrics
                .bytes_received
                .fetch_add(n as u64, Ordering::Relaxed);
        }
    }

    pub(crate) async fn write_packet(&mut self, pkt: &[u8]) -> Result<()> {
        if self.use_chunked_send {
            let len = u32::try_from(pkt.len()).map_err(|_| {
                crate::error::Error::Protocol(format!(
                    "chunked packet too large: {} bytes",
                    pkt.len()
                ))
            })?;
            self.write_all(&len.to_le_bytes()).await?;
            self.write_all(pkt).await?;
            self.write_all(&0u32.to_le_bytes()).await?;
            Ok(())
        } else if let Some(dur) = self.send_timeout {
            match crate::runtime::time::timeout(dur, self.write_all(pkt)).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(e.into()),
                Err(_) => Err(crate::error::Error::Timeout(format!(
                    "write timed out after {dur:?}",
                ))),
            }
        } else {
            self.write_all(pkt).await?;
            Ok(())
        }
    }

    /// Get a reference to the underlying raw TcpStream.
    pub(crate) fn raw_tcp(&self) -> Option<&crate::runtime::net::TcpStream> {
        match &self.inner {
            StreamInner::Tcp(s) => Some(s),
            #[cfg(feature = "tokio-tls")]
            StreamInner::Tls(s) => Some(s.get_ref().0),
        }
    }

    /// Whether writing a single raw `Cancel` byte straight to the socket would
    /// be wire-correct: plain TCP transport with chunked sending disabled.
    /// TLS-wrapped streams need TLS records and chunked transports need
    /// length framing — a synchronous `Drop` can provide neither, so those
    /// connections must simply be closed instead.
    pub(crate) fn can_raw_cancel(&self) -> bool {
        !self.use_chunked_send && matches!(self.inner, StreamInner::Tcp(_))
    }
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// A live ClickHouse TCP connection.
pub(crate) struct Connection {
    pub(crate) stream: StreamWrapper,
    pub(crate) server_info: handshake::ServerInfo,
    /// When this connection was established. Used for TTL-based recycling.
    pub(crate) created_at: crate::runtime::time::Instant,
    /// When this connection was last returned to the pool (went idle). Used to
    /// decide whether the next acquire needs a liveness Ping.
    pub(crate) last_used_at: crate::runtime::time::Instant,
    /// Pool configuration generation this connection was opened with.
    config_generation: u64,
    /// A query response is pending: the query packet was written but no
    /// terminal packet (EndOfStream or the end of an Exception chain) has
    /// been read yet. Only the task holding the pool guard touches this flag,
    /// so a plain `bool` needs no synchronization: when that task's future is
    /// dropped mid-response, `PoolGuard::drop` discards the socket instead of
    /// returning a stream parked mid-response to the pool.
    response_in_flight: bool,
}

struct RawConnectConfig<'a> {
    addr: std::net::SocketAddr,
    user: &'a str,
    password: &'a str,
    database: &'a str,
    quota_key: &'a str,
    /// Budget for the whole per-address connect: TCP + TLS + native
    /// handshake + addendum + ping. `None` = unbounded (OS defaults).
    /// DNS resolution is not covered — addresses arrive pre-resolved.
    connect_timeout: Option<std::time::Duration>,
    send_timeout: Option<std::time::Duration>,
    ssh_signer: Option<&'a handshake::SshSigner>,
    #[cfg(feature = "tokio-tls")]
    tls_config: Option<std::sync::Arc<rustls::ClientConfig>>,
    #[cfg(feature = "tokio-tls")]
    tls_domain: &'a str,
}

/// Create a new connection: TCP connect → handshake → addendum → ping.
///
/// The whole per-address future is bounded by `config.connect_timeout` (when
/// set): expiry cancels the in-flight setup and returns
/// [`Error::Timeout`](crate::error::Error::Timeout) naming the address and the
/// budget, so a server that accepts TCP and never sends Hello cannot hang the
/// pool. `None` keeps the unbounded OS default. The caller
/// (`SimplePool::connect_round_robin`) rejects a zero timeout up front.
#[tracing::instrument(level = "debug", skip_all, fields(addr = %config.addr), name = "clickhouse.connect")]
async fn connect_raw(config: RawConnectConfig<'_>) -> Result<Connection> {
    let addr = config.addr;
    match config.connect_timeout {
        Some(budget) => {
            match crate::runtime::time::timeout(budget, connect_raw_inner(config)).await {
                Ok(result) => result,
                Err(_elapsed) => Err(crate::error::Error::Timeout(format!(
                    "connect to {addr} (TCP + TLS + handshake + ping) timed out after {budget:?}"
                ))),
            }
        },
        None => connect_raw_inner(config).await,
    }
}

/// Unbounded per-address connect: TCP connect → TLS → handshake → addendum → ping.
async fn connect_raw_inner(config: RawConnectConfig<'_>) -> Result<Connection> {
    let addr = config.addr;
    let raw = crate::runtime::net::TcpStream::connect(addr).await?;
    configure_tcp(&raw);
    let mut stream = {
        #[cfg(feature = "tokio-tls")]
        {
            if let Some(tls_config) = config.tls_config {
                let server_name = rustls::pki_types::ServerName::try_from(
                    config.tls_domain.to_owned(),
                )
                .map_err(|_| {
                    crate::error::Error::Config(format!(
                        "invalid TLS server name '{}'",
                        config.tls_domain
                    ))
                })?;
                let tls = tokio_rustls::TlsConnector::from(tls_config)
                    .connect(server_name, raw)
                    .await?;
                StreamWrapper::tls(tokio_rustls::TlsStream::Client(tls))
            } else {
                StreamWrapper::tcp(raw)
            }
        }
        #[cfg(not(feature = "tokio-tls"))]
        {
            StreamWrapper::tcp(raw)
        }
    };

    let mut server_info = handshake::handshake(
        &mut stream,
        "st-clickhouse",
        revision::DEFAULT_PROTOCOL_REVISION,
        config.database,
        config.user,
        config.password,
        config.ssh_signer,
    )
    .instrument(tracing::debug_span!("clickhouse.handshake", addr = %addr))
    .await?;
    let chunked = negotiate_chunked_transport(&server_info)?;
    server_info.chunked_send = chunked.send_mode.to_string();
    server_info.chunked_recv = chunked.recv_mode.to_string();
    // Addendum (rev >= 54458)
    if server_info.negotiated_revision >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM {
        let mut buf = Vec::new();
        crate::protocol::wire::write_string(&mut buf, config.quota_key)?; // quota_key
        if server_info.negotiated_revision
            >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS
        {
            crate::protocol::wire::write_string(&mut buf, chunked.send_mode)?;
            crate::protocol::wire::write_string(&mut buf, chunked.recv_mode)?;
        }
        if server_info.negotiated_revision
            >= revision::DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL
        {
            crate::protocol::wire::write_varint(
                &mut buf,
                revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION,
            )?;
        }
        stream.write_all(&buf).await?;
        stream.flush().await?;
    }
    stream.set_chunked(chunked.send_chunked, chunked.recv_chunked);
    if let Some(timeout) = config.send_timeout {
        stream.set_send_timeout(Some(timeout));
    }
    // Ping
    stream.write_packet(&[ClientPacket::Ping as u8]).await?;
    stream.flush().await?;
    let mut pkt = [0u8; 1];
    stream.read_exact(&mut pkt).await?;
    if pkt[0] != 4 {
        return Err(crate::error::Error::Protocol("expected Pong".into()));
    }
    Ok(Connection {
        stream,
        server_info,
        created_at: crate::runtime::time::Instant::now(),
        last_used_at: crate::runtime::time::Instant::now(),
        config_generation: 0,
        response_in_flight: false,
    })
}

fn configure_tcp(raw: &crate::runtime::net::TcpStream) {
    raw.set_nodelay(true).ok();
    let sock = socket2::SockRef::from(raw);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(60))
        .with_interval(Duration::from_secs(15));
    let _ = sock.set_tcp_keepalive(&keepalive);
}

struct ChunkedNegotiation {
    send_mode: &'static str,
    recv_mode: &'static str,
    send_chunked: bool,
    recv_chunked: bool,
}

fn negotiate_chunked_transport(server_info: &handshake::ServerInfo) -> Result<ChunkedNegotiation> {
    if server_info.negotiated_revision < revision::DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS {
        return Ok(ChunkedNegotiation {
            send_mode: "notchunked",
            recv_mode: "notchunked",
            send_chunked: false,
            recv_chunked: false,
        });
    }

    let send_chunked = choose_chunked_mode(
        &server_info.proto_recv_chunked_srv,
        "chunked_optional",
        "send",
    )?;
    let recv_chunked = choose_chunked_mode(
        &server_info.proto_send_chunked_srv,
        "chunked_optional",
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

fn choose_chunked_mode(
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
        return Err(crate::error::Error::Protocol(format!(
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

// ---------------------------------------------------------------------------
// SimplePool
// ---------------------------------------------------------------------------

/// A pool of ClickHouse connections with per-slot async mutexes.
///
/// Slots are assigned round-robin via an atomic counter. Each slot holds
/// `Option<Connection>` and is lazily connected on first use.
pub(crate) struct SimplePool {
    addrs: parking_lot::RwLock<Vec<std::net::SocketAddr>>,
    current_addr: AtomicUsize,
    slots: Vec<AsyncMutex<Option<Connection>>>,
    next_idx: AtomicUsize,
    config_generation: AtomicU64,
    ttl: std::time::Duration,
    connect_timeout: Option<Duration>,
    /// Max wait for a free slot in `get()` (None = unbounded, today's behaviour).
    acquire_timeout: Option<Duration>,
    /// Connections reused within this idle window skip the acquire-time
    /// Ping/Pong. Default 15s; `ZERO` pings on every acquire.
    ping_idle_threshold: Duration,
    user: String,
    password: String,
    database: String,
    /// Quota key sent in ClientInfo and the handshake addendum (rev >= 54458).
    quota_key: String,
    ssh_signer: Option<handshake::SshSigner>,
    /// Addresses marked dead after connection failure, with cooldown expiry.
    dead_addrs: parking_lot::Mutex<HashMap<std::net::SocketAddr, Instant>>,
    /// Consecutive failure count per address for circuit breaker escalation.
    failure_counts: parking_lot::Mutex<HashMap<std::net::SocketAddr, u32>>,
    /// Original hostname for periodic DNS re-resolution.
    dns_hostname: parking_lot::RwLock<Option<String>>,
    /// Last DNS refresh timestamp.
    dns_last_refresh: parking_lot::RwLock<std::time::Instant>,
    /// Interval between DNS refreshes (default: 300s).
    /// How long to wait before retrying a dead address (default: 30s).
    dead_addr_cooldown: Duration,
    send_timeout: Option<Duration>,
    /// Optional metrics for observability.
    metrics: Option<&'static crate::metrics::Metrics>,
    #[cfg(feature = "tokio-tls")]
    tls_config: Option<std::sync::Arc<rustls::ClientConfig>>,
    #[cfg(feature = "tokio-tls")]
    tls_domain: String,
}

impl SimplePool {
    /// Create a pool with `size` slots.
    pub(crate) fn new(addrs: Vec<std::net::SocketAddr>, size: usize) -> Self {
        let size = size.max(1);
        let slots = (0..size).map(|_| AsyncMutex::new(None)).collect();
        Self {
            addrs: parking_lot::RwLock::new(addrs),
            current_addr: AtomicUsize::new(0),
            slots,
            next_idx: AtomicUsize::new(0),
            config_generation: AtomicU64::new(0),
            ttl: std::time::Duration::ZERO,
            connect_timeout: None,
            acquire_timeout: None,
            ping_idle_threshold: Duration::from_secs(15),
            user: String::from("default"),
            password: String::new(),
            database: String::new(),
            quota_key: String::new(),
            ssh_signer: None,
            dead_addrs: parking_lot::Mutex::new(HashMap::new()),
            failure_counts: parking_lot::Mutex::new(HashMap::new()),
            dns_hostname: parking_lot::RwLock::new(None),
            dns_last_refresh: parking_lot::RwLock::new(std::time::Instant::now()),
            dead_addr_cooldown: Duration::from_secs(30),
            send_timeout: None,
            metrics: None,
            #[cfg(feature = "tokio-tls")]
            tls_config: None,
            #[cfg(feature = "tokio-tls")]
            tls_domain: String::new(),
        }
    }

    pub(crate) fn slot_count(&self) -> usize {
        self.slots.len()
    }

    /// Set connection TTL. Connections older than this are recycled on next get().
    pub(crate) fn set_ttl(&mut self, ttl: std::time::Duration) {
        self.ttl = ttl;
    }

    /// Set send timeout (writes fail after this duration).
    pub(crate) fn set_send_timeout(&mut self, t: Option<Duration>) {
        self.send_timeout = t;
        self.bump_config_generation();
    }

    /// Set the max wait for a free pool slot. `None` = unbounded (default).
    pub(crate) fn set_acquire_timeout(&mut self, t: Option<Duration>) {
        self.acquire_timeout = t;
    }

    /// Set the idle threshold for the acquire-time liveness Ping. Connections
    /// reused within `t` skip the Ping; `ZERO` pings on every acquire.
    pub(crate) fn set_ping_idle_threshold(&mut self, t: Duration) {
        self.ping_idle_threshold = t;
    }

    /// Set the hostname for periodic DNS refresh. Pass `None` to disable.
    pub(crate) fn set_hostname(&self, hostname: Option<String>) {
        *self.dns_hostname.write() = hostname;
        *self.dns_last_refresh.write() = Instant::now();
    }

    /// Try to re-resolve DNS and update the address list. Returns true if changed.
    /// Safe to call from async contexts — uses non-blocking `crate::runtime::net::lookup_host`.
    pub(crate) async fn refresh_dns(&self) -> bool {
        let hostname = self.dns_hostname.read().clone();
        let Some(hostname) = hostname else {
            return false;
        };

        // Check if enough time has passed since last refresh
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(*self.dns_last_refresh.read());
        let interval = Duration::from_secs(300); // default 5 minutes
        if elapsed < interval {
            return false;
        }
        *self.dns_last_refresh.write() = now;

        // Non-blocking DNS resolution
        let dns_hostname = hostname.clone();
        match crate::runtime::net::lookup_host(&dns_hostname).await {
            Ok(addrs) => {
                let new_addrs: Vec<_> = addrs.collect();
                if !new_addrs.is_empty() {
                    let mut old = self.addrs.write();
                    *old = new_addrs;
                    // Prune dead/failure tracking for removed addresses
                    self.dead_addrs.lock().retain(|a, _| old.contains(a));
                    self.failure_counts.lock().retain(|a, _| old.contains(a));
                    true
                } else {
                    false
                }
            },
            Err(_) => false,
        }
    }

    /// Set the connect timeout applied to each per-address connect attempt
    /// (TCP + TLS + native handshake + addendum + ping) in
    /// [`SimplePool::get`](Self::get). New connections and reconnects read the
    /// current value at connect time. DNS resolution is not bounded by it.
    ///
    /// [`Duration::ZERO`] is invalid and rejected at connect time with
    /// [`Error::Config`](crate::error::Error::Config) — it cannot mean "no
    /// deadline". Use `None` (the default) for the unbounded OS behaviour.
    pub(crate) fn set_connect_timeout(&mut self, t: Duration) {
        self.connect_timeout = Some(t);
    }

    /// Set credentials for the handshake.
    pub(crate) fn set_credentials(&mut self, user: &str, password: &str) {
        self.user.zeroize();
        self.user = user.to_owned();
        self.password.zeroize();
        self.password = password.to_owned();
        self.ssh_signer = None;
        self.bump_config_generation();
    }

    /// Set default database for the handshake.
    pub(crate) fn set_database(&mut self, database: &str) {
        self.database = database.to_owned();
        self.bump_config_generation();
    }

    /// Set the quota key sent in ClientInfo and the handshake addendum.
    ///
    /// Bumps the config generation so pooled connections reconnect and the
    /// handshake addendum carries the new key (same semantics as
    /// [`set_database`](Self::set_database)).
    pub(crate) fn set_quota_key(&mut self, quota_key: &str) {
        self.quota_key = quota_key.to_owned();
        self.bump_config_generation();
    }

    /// Current quota key (sent in ClientInfo and the handshake addendum).
    pub(crate) fn quota_key(&self) -> &str {
        &self.quota_key
    }

    /// Set SSH-key authentication signer for the handshake.
    pub(crate) fn set_ssh_signer(&mut self, user: &str, signer: handshake::SshSigner) {
        self.user.zeroize();
        self.user = user.to_owned();
        self.password.zeroize();
        self.password.clear();
        self.ssh_signer = Some(signer);
        self.bump_config_generation();
    }

    /// Enable TLS with the given client config and server domain.
    #[cfg(feature = "tokio-tls")]
    pub(crate) fn set_tls(&mut self, config: rustls::ClientConfig, domain: &str) {
        self.tls_config = Some(std::sync::Arc::new(config));
        self.tls_domain = domain.to_owned();
        self.bump_config_generation();
    }

    fn bump_config_generation(&self) {
        self.config_generation.fetch_add(1, Ordering::AcqRel);
    }

    /// Attach metrics for observability. The reference must outlive the pool.
    pub(crate) fn with_metrics(&mut self, metrics: &'static crate::metrics::Metrics) {
        metrics
            .pool_slots
            .store(self.slots.len() as u64, Ordering::Relaxed);
        self.metrics = Some(metrics);
    }

    pub(crate) fn metrics(&self) -> Option<&'static crate::metrics::Metrics> {
        self.metrics
    }

    /// Try addresses in round-robin — returns first successful connection.
    /// Skips addresses marked as dead (within cooldown period).
    async fn connect_round_robin(&self) -> Result<Connection> {
        if self.addrs.read().is_empty() {
            return Err(crate::error::Error::Config(
                "no addresses configured".into(),
            ));
        }
        // A zero budget is deterministic misconfiguration: fail before any
        // address is tried so it is never retried or recorded as a dead
        // address by the circuit breaker.
        if let Some(t) = self.connect_timeout
            && t.is_zero()
        {
            return Err(crate::error::Error::Config(
                "connect_timeout must be greater than zero; Duration::ZERO would remove the connect deadline".into(),
            ));
        }
        // Periodically re-resolve DNS to discover new cluster nodes
        self.refresh_dns().await;

        let n = self.addrs.read().len();
        let start = self.current_addr.fetch_add(1, Ordering::Relaxed) % n;
        let mut last_err = None;

        // Prune expired dead entries
        {
            let mut dead = self.dead_addrs.lock();
            dead.retain(|_, expiry| *expiry > Instant::now());
        }

        for i in 0..n {
            let idx = (start + i) % n;
            let addr = {
                let addrs = self.addrs.read();
                addrs[idx]
            };

            // Skip if currently marked dead
            if self.dead_addrs.lock().contains_key(&addr) {
                continue;
            }

            #[cfg(feature = "tokio-tls")]
            let conn = connect_raw(RawConnectConfig {
                addr,
                user: &self.user,
                password: &self.password,
                database: &self.database,
                quota_key: &self.quota_key,
                connect_timeout: self.connect_timeout,
                send_timeout: self.send_timeout,
                ssh_signer: self.ssh_signer.as_ref(),
                tls_config: self.tls_config.clone(),
                tls_domain: &self.tls_domain,
            })
            .await;
            #[cfg(not(feature = "tokio-tls"))]
            let conn = connect_raw(RawConnectConfig {
                addr,
                user: &self.user,
                password: &self.password,
                database: &self.database,
                quota_key: &self.quota_key,
                connect_timeout: self.connect_timeout,
                send_timeout: self.send_timeout,
                ssh_signer: self.ssh_signer.as_ref(),
            })
            .await;

            match conn {
                Ok(mut conn) => {
                    conn.config_generation = self.config_generation.load(Ordering::Acquire);
                    conn.stream.set_metrics(self.metrics);
                    self.dead_addrs.lock().remove(&addr);
                    self.failure_counts.lock().remove(&addr);
                    return Ok(conn);
                },
                Err(e) => {
                    // Increment failure count for circuit breaker escalation
                    let mut failures = self.failure_counts.lock();
                    let count = failures.entry(addr).or_insert(0);
                    *count = count.saturating_add(1);
                    // Exponential backoff: base * 2^(count-1), capped at 300s
                    let cnt = *count;
                    let factor = if cnt > 1 {
                        let exp = (cnt.saturating_sub(1)).min(10);
                        1u32 << exp
                    } else {
                        1u32
                    };
                    let cooldown = self.dead_addr_cooldown.saturating_mul(factor);
                    let cooldown = cooldown.min(Duration::from_secs(300));
                    drop(failures);
                    self.dead_addrs
                        .lock()
                        .insert(addr, Instant::now() + cooldown);
                    if let Some(m) = self.metrics {
                        m.connection_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    last_err = Some(e);
                },
            }
        }
        Err(last_err
            .unwrap_or_else(|| crate::error::Error::Protocol("all addresses failed".into())))
    }

    /// Acquire a connection slot. Waits if all slots are busy.
    #[tracing::instrument(level = "debug", skip_all, fields(slots = self.slots.len()), name = "clickhouse.pool.acquire")]
    pub(crate) async fn get(&self) -> Result<PoolGuard<'_>> {
        // Entry timestamp for acquire_timeout: the budget is measured across
        // the try-lock sweep below and the final slot wait.
        let entered = Instant::now();
        // Round-robin slot selection
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        // Try-lock sweep from the assigned slot: a busy assigned slot no longer
        // head-of-line blocks the acquire when other slots are free — take the
        // first free one instead. When the pool is idle the sweep takes exactly
        // the assigned slot, so round-robin fairness is unchanged. Locked slots
        // are skipped: they are busy with live queries.
        let swept = (0..self.slots.len())
            .map(|i| (idx + i) % self.slots.len())
            .find_map(|i| self.slots[i].try_lock().ok());
        let mut slot_guard = match swept {
            Some(guard) => guard,
            None => {
                // All slots busy — await the originally assigned slot. The
                // acquire timeout keeps its original meaning: it is measured
                // from get() entry, across the sweep and this wait.
                match self.acquire_timeout {
                    Some(t) => {
                        let remaining = t.saturating_sub(entered.elapsed());
                        match crate::runtime::time::timeout(remaining, self.slots[idx].lock()).await
                        {
                            Ok(g) => g,
                            Err(_) => {
                                if let Some(m) = self.metrics {
                                    m.connection_errors.fetch_add(1, Ordering::Relaxed);
                                }
                                return Err(crate::error::Error::PoolTimeout(format!(
                                    "no connection slot available within {t:?}"
                                )));
                            },
                        }
                    },
                    None => self.slots[idx].lock().await,
                }
            },
        };
        if slot_guard.is_none() {
            *slot_guard = Some(self.connect_round_robin().await?);
        }
        let should_reconnect = if let Some(conn) = slot_guard.as_mut() {
            let expired = Self::connection_expired(conn, self.ttl);
            let stale = self.connection_config_stale(conn);
            if expired || stale {
                true
            } else {
                // Skip the Ping/Pong round-trip for connections reused within
                // the idle threshold: a recently used socket is trusted, with
                // TCP keepalive (set per-socket) and the query itself surfacing
                // any breakage. Idle connections past the threshold are pinged
                // to catch sockets dropped by the server or a proxy.
                should_liveness_ping(conn.last_used_at.elapsed(), self.ping_idle_threshold)
                    && !is_connection_alive(conn).await
            }
        } else {
            true
        };
        if should_reconnect {
            let conn = self.connect_round_robin().await;
            match conn {
                Ok(c) => *slot_guard = Some(c),
                Err(e) => {
                    *slot_guard = None;
                    return Err(e);
                },
            }
        }
        if let Some(metrics) = self.metrics {
            metrics.pool_in_use.fetch_add(1, Ordering::Relaxed);
        }
        Ok(PoolGuard {
            _guard: slot_guard,
            metrics: self.metrics,
        })
    }

    /// Check if a connection has exceeded its TTL.
    fn connection_expired(conn: &Connection, ttl: Duration) -> bool {
        ttl != Duration::ZERO && conn.created_at.elapsed() >= ttl
    }

    fn connection_config_stale(&self, conn: &Connection) -> bool {
        conn.config_generation != self.config_generation.load(Ordering::Acquire)
    }
    #[allow(dead_code)]
    pub(crate) async fn init_all(&self) -> Result<()> {
        let mut guards = Vec::with_capacity(self.slots.len());
        for _ in 0..self.slots.len() {
            guards.push(self.get().await?);
        }
        drop(guards);
        Ok(())
    }
}

impl Drop for SimplePool {
    fn drop(&mut self) {
        // Best-effort shutdown: acquire each slot and close the stream.
        // If a slot is locked, skip it (non-blocking shutdown).
        for slot in &self.slots {
            if let Ok(guard) = slot.try_lock()
                // Send Cancel only when a single raw byte is exactly what the
                // wire expects: plain TCP with chunked sending off. A raw byte
                // through TLS or chunked framing would be protocol garbage, so
                // those sockets are just closed (dropped) instead.
                && let Some(ref conn) = *guard
                && conn.stream.can_raw_cancel()
                && let Some(tcp) = conn.stream.raw_tcp()
            {
                let _: std::io::Result<usize> = tcp.try_write(&[ClientPacket::Cancel as u8]);
            }
        }
        self.user.zeroize();
        self.password.zeroize();
    }
}

/// Whether a pooled connection needs an acquire-time Ping/Pong.
///
/// Returns true only when the connection has been idle for at least `threshold`
/// — recently used sockets are trusted. `threshold == ZERO` always returns true
/// (ping on every acquire).
fn should_liveness_ping(idle: Duration, threshold: Duration) -> bool {
    idle >= threshold
}

async fn is_connection_alive(conn: &mut Connection) -> bool {
    if conn
        .stream
        .write_packet(&[ClientPacket::Ping as u8])
        .await
        .is_err()
    {
        return false;
    }
    if conn.stream.flush().await.is_err() {
        return false;
    }
    let mut pkt = [0u8; 1];
    matches!(
        crate::runtime::time::timeout(Duration::from_secs(1), conn.stream.read_exact(&mut pkt)).await,
        Ok(Ok(_)) if pkt[0] == 4
    )
}

// ---------------------------------------------------------------------------
// PoolGuard
// ---------------------------------------------------------------------------

/// Holds a slot's mutex guard.
/// Dropping the guard releases the slot for the next user.
pub(crate) struct PoolGuard<'a> {
    _guard: MutexGuard<'a, Option<Connection>>,
    metrics: Option<&'static crate::metrics::Metrics>,
}

impl<'a> PoolGuard<'a> {
    pub(crate) fn stream_mut(&mut self) -> &mut StreamWrapper {
        match self._guard.as_mut() {
            Some(conn) => &mut conn.stream,
            None => std::process::abort(),
        }
    }

    /// Take the TcpStream out of the connection.
    /// The slot becomes `None` — next `get()` will reconnect.
    pub(crate) fn take_stream(&mut self) -> Option<StreamWrapper> {
        let conn = (*self._guard).take()?;
        Some(conn.stream)
    }

    pub(crate) fn server_info(&self) -> &handshake::ServerInfo {
        match self._guard.as_ref() {
            Some(conn) => &conn.server_info,
            None => std::process::abort(),
        }
    }

    /// If `result` is a connection-fatal error, drop the underlying connection
    /// so the next `get()` reconnects instead of reusing a broken socket.
    /// Keeps a failed socket from being trusted by the idle-threshold Ping skip.
    pub(crate) fn invalidate_on_err<T>(&mut self, result: &crate::error::Result<T>) {
        if let Err(e) = result
            && e.is_broken_connection()
        {
            let _ = self.take_stream();
        }
    }

    /// Mark that a query response is now in flight. Call before the first
    /// response-triggering packet write (the query packet); if the owning
    /// future is then dropped before its terminal packet is read,
    /// [`PoolGuard::drop`] discards the socket.
    pub(crate) fn mark_response_in_flight(&mut self) {
        if let Some(conn) = self._guard.as_mut() {
            conn.response_in_flight = true;
        }
    }

    /// Whether a query response is still pending on this connection.
    pub(crate) fn response_in_flight(&self) -> bool {
        self._guard
            .as_ref()
            .is_some_and(|conn| conn.response_in_flight)
    }

    /// Clear the in-flight mark without discarding the connection. Call once
    /// the owning future has resolved its response cycle (a terminal packet —
    /// EndOfStream or the end of an Exception chain — was read, or the cycle
    /// ended with a non-fatal error such as a server exception).
    pub(crate) fn clear_response_in_flight(&mut self) {
        if let Some(conn) = self._guard.as_mut() {
            conn.response_in_flight = false;
        }
    }

    /// Finish a response cycle: clear the in-flight mark, then discard the
    /// socket when the result is connection-fatal. Call exactly once, after
    /// the future that reads the response has resolved.
    pub(crate) fn finish_response<T>(&mut self, result: &crate::error::Result<T>) {
        self.clear_response_in_flight();
        self.invalidate_on_err(result);
    }
}

impl Drop for PoolGuard<'_> {
    fn drop(&mut self) {
        if let Some(metrics) = self.metrics {
            metrics.pool_in_use.fetch_sub(1, Ordering::Relaxed);
        }
        // A still-marked connection never saw its terminal packet: the owning
        // future was dropped mid-response (cancellation at an await point).
        // Discard the socket — the slot becomes `None` and the next `get()`
        // reconnects — instead of handing the next pool user a stream parked
        // mid-response, which would surface as a bogus Protocol error and
        // poison the slot until an idle Ping eventually failed.
        //
        // Race analysis: the flag is owned exclusively by the task holding
        // this guard, so no other task can flip it concurrently. The window
        // between reading the terminal packet and `finish_response` clearing
        // the mark can, at worst, cause a needless discard of an already
        // clean connection (one extra reconnect) — never a poisoned reuse.
        if self.response_in_flight() {
            let _ = self.take_stream();
            return;
        }
        // Record when this connection went back to idle so the next acquire
        // can decide whether a liveness Ping is warranted.
        if let Some(conn) = self._guard.as_mut() {
            conn.last_used_at = crate::runtime::time::Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_size_minimum_one() {
        let addr = "127.0.0.1:9000"
            .parse::<std::net::SocketAddr>()
            .expect("test operation failed");
        let pool = SimplePool::new(vec![addr], 0);
        assert_eq!(pool.slots.len(), 1, "pool with size 0 should create 1 slot");
    }

    #[test]
    fn test_pool_new_with_size() {
        let addr = "127.0.0.1:9000"
            .parse::<std::net::SocketAddr>()
            .expect("test operation failed");
        let pool = SimplePool::new(vec![addr], 4);
        assert_eq!(pool.slots.len(), 4);
        assert_eq!(pool.addrs.read().len(), 1, "should have one address");
        assert_eq!(pool.current_addr.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_pool_new_multiple_addresses() {
        let addrs: Vec<std::net::SocketAddr> = vec![
            "127.0.0.1:9000".parse().expect("test address should parse"),
            "127.0.0.2:9000".parse().expect("test address should parse"),
            "127.0.0.3:9000".parse().expect("test address should parse"),
        ];
        let pool = SimplePool::new(addrs, 2);
        assert_eq!(pool.addrs.read().len(), 3);
        assert_eq!(pool.slots.len(), 2);
    }

    #[test]
    fn quota_key_default_empty_and_setter_stores() {
        let addr = "127.0.0.1:9000"
            .parse::<std::net::SocketAddr>()
            .expect("test address should parse");
        let mut pool = SimplePool::new(vec![addr], 1);
        assert!(pool.quota_key().is_empty(), "default quota_key is empty");

        let gen_before = pool.config_generation.load(Ordering::Relaxed);
        pool.set_quota_key("tenant-42");
        assert_eq!(pool.quota_key(), "tenant-42");
        // set_quota_key bumps the generation so pooled connections reconnect.
        assert!(
            pool.config_generation.load(Ordering::Relaxed) > gen_before,
            "set_quota_key must bump config_generation"
        );
    }

    #[test]
    fn test_set_send_timeout() {
        let addr = "127.0.0.1:9000"
            .parse::<std::net::SocketAddr>()
            .expect("test operation failed");
        let mut pool = SimplePool::new(vec![addr], 1);
        assert!(pool.send_timeout.is_none());
        pool.set_send_timeout(Some(Duration::from_secs(10)));
        assert_eq!(pool.send_timeout, Some(Duration::from_secs(10)));
        pool.set_send_timeout(None);
        assert!(pool.send_timeout.is_none());
    }

    #[test]
    fn test_set_connect_timeout() {
        let addr = "127.0.0.1:9000"
            .parse::<std::net::SocketAddr>()
            .expect("test operation failed");
        let mut pool = SimplePool::new(vec![addr], 1);
        assert!(pool.connect_timeout.is_none(), "default is unbounded");
        pool.set_connect_timeout(Duration::from_secs(2));
        assert_eq!(pool.connect_timeout, Some(Duration::from_secs(2)));
    }

    #[test]
    fn test_acquire_timeout_defaults_none() {
        let addr = "127.0.0.1:9000"
            .parse::<std::net::SocketAddr>()
            .expect("test operation failed");
        let pool = SimplePool::new(vec![addr], 2);
        assert!(pool.acquire_timeout.is_none());
    }

    #[test]
    fn test_set_acquire_timeout() {
        let addr = "127.0.0.1:9000"
            .parse::<std::net::SocketAddr>()
            .expect("test operation failed");
        let mut pool = SimplePool::new(vec![addr], 2);
        pool.set_acquire_timeout(Some(Duration::from_millis(50)));
        assert_eq!(pool.acquire_timeout, Some(Duration::from_millis(50)));
        pool.set_acquire_timeout(None);
        assert!(pool.acquire_timeout.is_none());
    }

    #[test]
    fn test_ping_idle_threshold_default_and_setter() {
        let addr = "127.0.0.1:9000"
            .parse::<std::net::SocketAddr>()
            .expect("test operation failed");
        let mut pool = SimplePool::new(vec![addr], 2);
        // Default: trust connections reused within 15s, ping only after idle.
        assert_eq!(pool.ping_idle_threshold, Duration::from_secs(15));
        pool.set_ping_idle_threshold(Duration::from_secs(60));
        assert_eq!(pool.ping_idle_threshold, Duration::from_secs(60));
        // ZERO restores the old always-ping behaviour.
        pool.set_ping_idle_threshold(Duration::ZERO);
        assert_eq!(pool.ping_idle_threshold, Duration::ZERO);
    }

    #[test]
    fn should_liveness_ping_only_after_idle_threshold() {
        // Recently used → trust, skip the round-trip.
        assert!(!should_liveness_ping(
            Duration::ZERO,
            Duration::from_secs(15)
        ));
        assert!(!should_liveness_ping(
            Duration::from_secs(14),
            Duration::from_secs(15)
        ));
        // Idle at/over the threshold → ping.
        assert!(should_liveness_ping(
            Duration::from_secs(15),
            Duration::from_secs(15)
        ));
        assert!(should_liveness_ping(
            Duration::from_secs(60),
            Duration::from_secs(15)
        ));
        // ZERO threshold = always ping (old behaviour).
        assert!(should_liveness_ping(Duration::ZERO, Duration::ZERO));
    }

    #[test]
    fn test_set_credentials() {
        let addr = "127.0.0.1:9000"
            .parse::<std::net::SocketAddr>()
            .expect("test operation failed");
        let mut pool = SimplePool::new(vec![addr], 1);
        assert_eq!(pool.user, "default");
        assert_eq!(pool.password, "");
        pool.set_credentials("admin", "secret");
        assert_eq!(pool.user, "admin");
        assert_eq!(pool.password, "secret");
    }

    #[test]
    fn test_set_hostname_and_initial_refresh_time() {
        let addr = "127.0.0.1:9000"
            .parse::<std::net::SocketAddr>()
            .expect("test operation failed");
        let pool = SimplePool::new(vec![addr], 1);
        assert!(pool.dns_hostname.read().is_none());

        pool.set_hostname(Some("example.com:9000".into()));
        assert_eq!(
            pool.dns_hostname.read().as_deref(),
            Some("example.com:9000")
        );
        let first = *pool.dns_last_refresh.read();
        pool.set_hostname(None);
        assert!(pool.dns_hostname.read().is_none());
        // Call also resets refresh timestamp
        let second = *pool.dns_last_refresh.read();
        assert!(second >= first);
    }

    #[test]
    fn test_circuit_breaker_failure_counting() {
        let addr = "127.0.0.1:9000"
            .parse::<std::net::SocketAddr>()
            .expect("test operation failed");
        let pool = SimplePool::new(vec![addr], 1);

        // Initially no failures
        assert!(pool.failure_counts.lock().is_empty());
        assert!(pool.dead_addrs.lock().is_empty());

        // Simulate a failure by writing directly to the tracking maps
        pool.failure_counts.lock().insert(addr, 1);
        pool.dead_addrs
            .lock()
            .insert(addr, Instant::now() + Duration::from_secs(30));

        assert_eq!(pool.failure_counts.lock().get(&addr), Some(&1));
        assert!(pool.dead_addrs.lock().contains_key(&addr));

        // Increment failure count
        *pool
            .failure_counts
            .lock()
            .get_mut(&addr)
            .expect("failure count should be present") = 2;
        assert_eq!(pool.failure_counts.lock().get(&addr), Some(&2));
    }

    #[test]
    fn test_dead_addr_pruning_expired() {
        let addr = "127.0.0.1:9000"
            .parse::<std::net::SocketAddr>()
            .expect("test operation failed");
        let pool = SimplePool::new(vec![addr], 1);

        // Insert a dead addr with past expiry
        pool.dead_addrs
            .lock()
            .insert(addr, Instant::now() - Duration::from_secs(1));
        assert!(!pool.dead_addrs.lock().is_empty());

        // After acquiring a slot, we can send a "ping" packet to
        // the server.  In this test we only verify that the dead_addrs map
        // can be cleaned.  The connect_round_robin loop itself calls
        // dead.retain(|_, expiry| *expiry > Instant::now()) which prunes
        // expired entries.
        {
            let mut dead = pool.dead_addrs.lock();
            dead.retain(|_, expiry| *expiry > Instant::now());
        }
        assert!(pool.dead_addrs.lock().is_empty());
    }

    #[test]
    fn test_pool_slot_count() {
        let addr = "127.0.0.1:9000"
            .parse::<std::net::SocketAddr>()
            .expect("test operation failed");
        let pool = SimplePool::new(vec![addr], 5);
        assert_eq!(pool.slot_count(), 5);
    }

    /// Bind a local listener that accepts connections and then stays silent —
    /// a server that never sends its Hello.
    async fn silent_listener() -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener address");
        crate::runtime::spawn(async move {
            // Accept and hold every connection open without writing anything.
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        addr
    }

    fn refused_addr() -> std::net::SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind refused-port probe");
        let addr = listener.local_addr().expect("refused-port address");
        drop(listener);
        addr
    }

    /// A silent server must trip the connect timeout — covering TCP accept,
    /// handshake, addendum, and ping — long before anything else could save
    /// it, leaving the slot `None` and the address recorded as failed.
    #[tokio::test]
    async fn test_connect_timeout_bounds_silent_server() {
        let addr = silent_listener().await;
        let mut pool = SimplePool::new(vec![addr], 1);
        pool.set_connect_timeout(Duration::from_millis(200));

        let start = Instant::now();
        let err = match pool.get().await {
            Ok(_) => unreachable!("silent server must not yield a connection"),
            Err(e) => e,
        };
        let elapsed = start.elapsed();

        match &err {
            crate::error::Error::Timeout(msg) => {
                assert!(
                    msg.contains(&addr.to_string()),
                    "message must name the address: {msg}"
                );
                assert!(msg.contains("200ms"), "message must name the budget: {msg}");
            },
            other => unreachable!("expected Timeout, got {other:?}"),
        }
        assert!(
            elapsed < Duration::from_secs(10),
            "connect timeout must fire in ~200ms, took {elapsed:?}"
        );
        // Slot stays empty: the next acquire starts from scratch.
        assert!(
            pool.slots[0].try_lock().expect("slot lockable").is_none(),
            "failed connect must leave the slot None"
        );
        // The circuit breaker saw the failure.
        assert_eq!(pool.failure_counts.lock().get(&addr), Some(&1));
        assert!(pool.dead_addrs.lock().contains_key(&addr));
    }

    /// Fast network errors (connection refused) keep their I/O identity — the
    /// connect timeout must not convert them into Timeout — and failover moves
    /// on to the next address.
    #[tokio::test]
    async fn test_connect_timeout_failover_keeps_io_error_identity() {
        let refused = refused_addr();
        let silent = silent_listener().await;
        let mut pool = SimplePool::new(vec![refused, silent], 1);
        pool.set_connect_timeout(Duration::from_millis(200));

        let err = match pool.get().await {
            Ok(_) => unreachable!("no address here can complete a handshake"),
            Err(e) => e,
        };
        // The final error is the silent address timing out, not the refusal.
        match &err {
            crate::error::Error::Timeout(msg) => {
                assert!(
                    msg.contains(&silent.to_string()),
                    "message must name the address: {msg}"
                );
            },
            other => unreachable!("expected Timeout from the silent address, got {other:?}"),
        }
        // Both attempts were recorded by the circuit breaker.
        assert_eq!(pool.failure_counts.lock().get(&refused), Some(&1));
        assert_eq!(pool.failure_counts.lock().get(&silent), Some(&1));
        assert!(pool.dead_addrs.lock().contains_key(&refused));
        assert!(pool.dead_addrs.lock().contains_key(&silent));
    }

    /// `Duration::ZERO` is deterministic misconfiguration: rejected as Config
    /// before any address is tried, never retried, never marked dead.
    #[tokio::test]
    async fn test_connect_timeout_zero_rejected_as_config() {
        let addr = silent_listener().await;
        let mut pool = SimplePool::new(vec![addr], 1);
        pool.set_connect_timeout(Duration::ZERO);

        let err = match pool.get().await {
            Ok(_) => unreachable!("zero connect_timeout must be rejected"),
            Err(e) => e,
        };
        assert!(
            matches!(err, crate::error::Error::Config(ref msg) if msg.contains("connect_timeout")),
            "expected Config error, got {err:?}"
        );
        assert!(
            pool.failure_counts.lock().is_empty(),
            "config errors are not failures"
        );
        assert!(
            pool.dead_addrs.lock().is_empty(),
            "config errors must not mark addresses dead"
        );
    }

    /// Server-free proof that a hostile `u32::MAX` chunk header is rejected by
    /// the inbound chunk cap before any buffer is sized: the read must fail
    /// with `InvalidData` naming the chunk length, never attempt a 4 GiB
    /// resize (the old behaviour).
    #[tokio::test]
    async fn test_chunked_recv_rejects_oversized_chunk_header() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.expect("accept client");
            use tokio::io::AsyncWriteExt as _;
            sock.write_all(&u32::MAX.to_le_bytes())
                .await
                .expect("send hostile chunk header");
            // Keep the socket open so the client sees data, not EOF.
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect to test listener");
        let mut wrapper = StreamWrapper::tcp(stream);
        wrapper.set_chunked(false, true);

        let mut buf = [0u8; 16];
        let err = wrapper
            .read(&mut buf)
            .await
            .expect_err("oversized chunk header must be rejected");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("chunk length"),
            "expected chunk length error, got: {err}"
        );
        server.abort();
    }

    /// The chunk cap keeps the happy path intact: a small well-formed chunk
    /// still decodes through the chunked receive mode.
    #[tokio::test]
    async fn test_chunked_recv_still_reads_small_chunk() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.expect("accept client");
            use tokio::io::AsyncWriteExt as _;
            let mut wire = Vec::new();
            wire.extend_from_slice(&3u32.to_le_bytes());
            wire.extend_from_slice(b"abc");
            wire.extend_from_slice(&0u32.to_le_bytes());
            sock.write_all(&wire).await.expect("send chunk");
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect to test listener");
        let mut wrapper = StreamWrapper::tcp(stream);
        wrapper.set_chunked(false, true);

        let mut buf = [0u8; 8];
        let n = wrapper
            .read(&mut buf)
            .await
            .expect("small chunk must decode");
        assert_eq!(&buf[..n], b"abc");
        server.abort();
    }

    /// Server-free proof that `get()` honours `acquire_timeout`: hold the only
    /// slot from the test, so `get()` cannot acquire it and must time out.
    /// The outer probe bound makes RED fail fast instead of hanging.
    #[tokio::test]
    async fn test_acquire_timeout_returns_pool_timeout_when_slot_contended() {
        let addr: std::net::SocketAddr = "127.0.0.1:9000".parse().expect("test operation failed");
        let mut pool = SimplePool::new(vec![addr], 1);
        pool.set_acquire_timeout(Some(Duration::from_millis(20)));
        // Occupy the single slot; `get()` (round-robin idx 0) cannot lock it.
        let _held = pool.slots[0].lock().await;

        let res = crate::runtime::time::timeout(Duration::from_secs(2), pool.get()).await;
        // `PoolGuard` isn't `Debug`, so the message is static. Use `assert!`
        // rather than `panic!` (the crate denies `clippy::panic`).
        assert!(
            matches!(res, Ok(Err(crate::error::Error::PoolTimeout(_)))),
            "expected PoolTimeout, got Ok(connection), other error, or probe elapsed"
        );
    }

    // -----------------------------------------------------------------
    // In-flight (drop-mid-response) pool safety
    // -----------------------------------------------------------------

    /// A connection for server-free guard tests: real socket (held open by the
    /// silent listener), placeholder server info.
    async fn test_connection() -> Connection {
        let addr = silent_listener().await;
        let stream = crate::runtime::net::TcpStream::connect(addr)
            .await
            .expect("connect to silent listener");
        Connection {
            stream: StreamWrapper::tcp(stream),
            server_info: handshake::ServerInfo {
                name: "test".into(),
                version_major: 26,
                version_minor: 7,
                version_patch: 1,
                revision: 54471,
                negotiated_revision: 54471,
                timezone: "UTC".into(),
                display_name: "test".into(),
                server_parallel_replicas_protocol_version: 7,
                proto_send_chunked_srv: String::new(),
                proto_recv_chunked_srv: String::new(),
                password_complexity_rules: Vec::new(),
                interserver_secret_nonce: None,
                server_query_plan_serialization_version: None,
                worker_cluster_function_protocol_version: 0,
                chunked_send: String::new(),
                chunked_recv: String::new(),
            },
            created_at: crate::runtime::time::Instant::now(),
            last_used_at: crate::runtime::time::Instant::now(),
            config_generation: 0,
            response_in_flight: false,
        }
    }

    /// Server-free proof of the drop-mid-response contract: a guard dropped
    /// while its connection is marked in-flight must empty the slot (the next
    /// `get()` reconnects), while a clean guard returns the connection.
    #[tokio::test]
    async fn guard_drop_discards_marked_in_flight_connection() {
        let slot = AsyncMutex::new(Some(test_connection().await));
        {
            let guard = slot.lock().await;
            let mut guard = PoolGuard {
                _guard: guard,
                metrics: None,
            };
            guard.mark_response_in_flight();
            assert!(
                guard.response_in_flight(),
                "mark must set the in-flight flag"
            );
        }
        assert!(
            slot.try_lock().expect("slot lockable").is_none(),
            "dropping an in-flight guard must discard the mid-response connection"
        );

        // Clean counterpart: no mark, no discard — the connection is reused.
        let slot = AsyncMutex::new(Some(test_connection().await));
        {
            let guard = slot.lock().await;
            let guard = PoolGuard {
                _guard: guard,
                metrics: None,
            };
            assert!(!guard.response_in_flight());
        }
        assert!(
            slot.try_lock().expect("slot lockable").is_some(),
            "a clean guard drop must keep the connection for reuse"
        );
    }

    /// `finish_response` clears the mark (a resolved response cycle can never
    /// trigger the discard) and still discards only on connection-fatal
    /// errors: a server exception is terminal, so that socket stays pooled.
    #[tokio::test]
    async fn finish_response_clears_mark_and_keeps_terminal_connections() {
        // Ok result: mark cleared, connection kept.
        let slot = AsyncMutex::new(Some(test_connection().await));
        {
            let guard = slot.lock().await;
            let mut guard = PoolGuard {
                _guard: guard,
                metrics: None,
            };
            guard.mark_response_in_flight();
            guard.finish_response(&Ok::<(), crate::error::Error>(()));
            assert!(!guard.response_in_flight());
        }
        assert!(slot.try_lock().expect("slot lockable").is_some());

        // Server exception: terminal packet read — mark cleared, kept.
        let slot = AsyncMutex::new(Some(test_connection().await));
        {
            let guard = slot.lock().await;
            let mut guard = PoolGuard {
                _guard: guard,
                metrics: None,
            };
            guard.mark_response_in_flight();
            guard.finish_response(&Err::<(), _>(crate::error::Error::ServerError {
                code: 159,
                name: "DB::Exception".into(),
                message: "cancelled".into(),
            }));
            assert!(!guard.response_in_flight());
        }
        assert!(
            slot.try_lock().expect("slot lockable").is_some(),
            "a terminal server exception must keep the connection pooled"
        );

        // I/O failure: broken connection — discarded.
        let slot = AsyncMutex::new(Some(test_connection().await));
        {
            let guard = slot.lock().await;
            let mut guard = PoolGuard {
                _guard: guard,
                metrics: None,
            };
            guard.mark_response_in_flight();
            guard.finish_response(&Err::<(), _>(crate::error::Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "reset",
            ))));
        }
        assert!(
            slot.try_lock().expect("slot lockable").is_none(),
            "a connection-fatal result must discard the socket"
        );
    }

    /// `can_raw_cancel` gates the pool-drop Cancel byte on the only transport
    /// where a single raw byte is wire-correct: plain TCP, chunked send off.
    /// (Chunked recv only affects inbound framing and must not matter.)
    #[tokio::test]
    async fn can_raw_cancel_only_for_plain_non_chunked_send() {
        let addr = silent_listener().await;
        let stream = crate::runtime::net::TcpStream::connect(addr)
            .await
            .expect("connect to silent listener");
        let mut wrapper = StreamWrapper::tcp(stream);
        assert!(
            wrapper.can_raw_cancel(),
            "plain non-chunked TCP accepts a raw Cancel byte"
        );
        wrapper.set_chunked(true, true);
        assert!(
            !wrapper.can_raw_cancel(),
            "chunked send framing must not take a raw Cancel byte"
        );
        wrapper.set_chunked(false, true);
        assert!(
            wrapper.can_raw_cancel(),
            "chunked recv must not affect outbound framing"
        );
    }
}
