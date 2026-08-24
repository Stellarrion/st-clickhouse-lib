//! Synchronous ClickHouse client — pure `std::net::TcpStream`, no tokio.
//!
//! Used by `st-clickhouse-py` (PyO3 bindings) and any sync Rust applications.

#[cfg(test)]
use crate::sync::chunked::choose_chunked_mode;
use crate::sync::chunked::{
    ChunkedReader, TransportReader, negotiate_chunked_transport, write_chunk_header,
    write_chunked_packet,
};
use crate::sync::config::ClientConfig;
use crate::sync::error::{Error, Result};
use crate::sync::protocol::block::{Block, BlockView};
use crate::sync::protocol::parameters::{
    QueryParameter, query_parameters_capacity, write_query_parameters_to_vec,
};
use crate::sync::protocol::response_packets::parse_exception_chain;
use crate::sync::schema::{
    TableColumn, TableSchema, query_may_change_schema, quote_identifier_path,
};
use std::collections::HashMap;

thread_local! {
    static BUF_POOL: std::cell::RefCell<Vec<Vec<u8>>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Acquire a buffer from the pool, or create a new one.
fn take_buf(capacity: usize) -> Vec<u8> {
    BUF_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        pool.pop().unwrap_or_else(|| Vec::with_capacity(capacity))
    })
}

/// Upper bound for a merged settings block: varint + bytes per string, plus
/// flags and the terminator per entry. Used only for buffer pre-sizing.
fn serialized_settings_capacity(
    base: &HashMap<String, String>, overlay: &HashMap<String, String>,
) -> usize {
    base.iter()
        .chain(overlay.iter())
        .map(|(name, value)| name.len() + value.len() + 24)
        .sum::<usize>()
        + 24
}

/// Parse a `host:port` string into (host, port) components.
/// Handles IPv6 addresses wrapped in brackets.
pub fn parse_host_port_addr(addr: &str) -> Result<(String, u16)> {
    if let Some((host, port_str)) = addr.rsplit_once(':') {
        let port: u16 = port_str
            .parse()
            .map_err(|_| Error::Protocol(format!("invalid port in '{addr}'")))?;
        let host = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        Ok((host, port))
    } else {
        Err(Error::Protocol(format!(
            "expected 'host:port' format, got '{addr}'"
        )))
    }
}
use crate::sync::protocol::handshake::{self, ServerInfo};
use crate::sync::protocol::response::{parse_block, parse_block_body};
use crate::sync::protocol::revision;
use crate::sync::protocol::table_status::{QualifiedTableName, TableStatus, TablesStatusResponse};
use crate::sync::protocol::wire;
use crate::sync::query_packet::{
    QueryPacketTemplate, build_query_packet_template, next_query_id, write_empty_data_block_to,
};
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;
/// Reject a zero `connect_timeout` clearly: it cannot mean "no deadline"
/// (that is the hang this timeout exists to prevent).
fn validate_connect_timeout(config: &ClientConfig) -> Result<()> {
    if config.connect_timeout.is_zero() {
        return Err(Error::Config(
            "connect_timeout must be greater than zero; Duration::ZERO would remove the connect deadline".into(),
        ));
    }
    Ok(())
}

/// Whether an I/O error is a socket read/write deadline expiry.
///
/// A blocking socket with a timeout reports `TimedOut` on most platforms, but
/// Linux reports `WouldBlock`; both mean the configured deadline expired.
fn is_socket_timeout(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    )
}

/// Classify a failed connection setup: an expired wall-clock deadline (or its
/// socket timeout fallback) becomes a distinct [`Error::Timeout`].
fn classify_setup_error(
    result: Result<ServerInfo>, config: &ClientConfig, deadline_expired: bool,
) -> Result<ServerInfo> {
    match result {
        _ if deadline_expired => Err(Error::Timeout(format!(
            "connection setup to {} did not complete within {:?}",
            config.addr(),
            config.connect_timeout
        ))),
        Err(Error::Io(ref e)) if is_socket_timeout(e) => Err(Error::Timeout(format!(
            "connection setup to {} did not complete within {:?}",
            config.addr(),
            config.connect_timeout
        ))),
        other => other,
    }
}

/// Wall-clock guard for blocking sync setup. Socket timeouts alone reset on
/// every read/write and can be defeated by a peer that drip-feeds bytes. The
/// watchdog shuts down a cloned socket at the absolute deadline, interrupting
/// TLS/native handshake I/O. It is disarmed and joined before a client escapes.
struct SetupWatchdog {
    stop: Option<std::sync::mpsc::Sender<()>>,
    join: Option<std::thread::JoinHandle<()>>,
    expired: std::sync::Arc<std::sync::atomic::AtomicBool>,
    started: std::time::Instant,
    budget: Duration,
}

impl SetupWatchdog {
    fn start(tcp: &TcpStream, budget: Duration) -> Result<Self> {
        let tcp = tcp.try_clone()?;
        let (stop, stopped) = std::sync::mpsc::channel();
        let expired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let expired_in_thread = expired.clone();
        let join = std::thread::Builder::new()
            .name("st-clickhouse-connect-timeout".into())
            .spawn(move || {
                if matches!(
                    stopped.recv_timeout(budget),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout)
                ) {
                    expired_in_thread.store(true, std::sync::atomic::Ordering::Release);
                    let _ = tcp.shutdown(std::net::Shutdown::Both);
                }
            })?;
        Ok(Self {
            stop: Some(stop),
            join: Some(join),
            expired,
            started: std::time::Instant::now(),
            budget,
        })
    }

    fn finish(mut self) -> bool {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        self.expired.load(std::sync::atomic::Ordering::Acquire)
            || self.started.elapsed() >= self.budget
    }
}

impl Drop for SetupWatchdog {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// A synchronous ClickHouse native protocol client.
///
/// Each `SyncClient` holds a single `std::net::TcpStream`. Send and receive
/// are sequenced on the same stream — no cloning needed.
pub struct SyncClient {
    stream: crate::sync::transport::Transport,
    server_info: ServerInfo,
    config: ClientConfig,
    query_template: QueryPacketTemplate,
    schema_cache: HashMap<String, TableSchema>,
}

impl SyncClient {
    /// Create a configurable blocking client builder.
    pub fn builder() -> crate::builder::ClientBuilder<crate::builder::Blocking> {
        crate::builder::ClientBuilder::<crate::builder::Blocking>::new()
    }

    /// Connect using a `clickhouse://` or `clickhouses://` URL.
    pub fn connect_url(addr: &str) -> Result<Self> {
        crate::builder::ClientBuilder::<crate::builder::Blocking>::from_url(addr)
            .map_err(|e| Error::Protocol(e.to_string()))?
            .connect()
    }

    /// Connect to a ClickHouse server using a full [`ClientConfig`].
    ///
    /// Resolves `config.addr()` and tries every socket address in order. Each
    /// address gets one wall-clock `config.connect_timeout` budget shared by
    /// TCP establishment and subsequent TLS/native setup. Transient TCP/setup
    /// I/O and timeout failures move to the next address; deterministic setup
    /// errors surface immediately. See [`SyncClient::connect_stream`]. A
    /// `connect_timeout` of [`Duration::ZERO`](std::time::Duration::ZERO) is
    /// rejected with [`Error::Config`].
    ///
    /// On success the connection is left in normal query mode: the socket read
    /// timeout is `config.query_timeout` and the write timeout is unset.
    pub fn connect_with_config(config: ClientConfig) -> Result<Self> {
        revision::validate_supported_revision(config.client_revision).map_err(Error::Protocol)?;
        validate_connect_timeout(&config)?;

        let timeout = config.connect_timeout;
        let mut last_err = None;
        // Collect first: `ToSocketAddrs` resolves lazily and each `next()` can
        // touch the resolver, which must not happen inside the loop.
        let addrs: Vec<std::net::SocketAddr> = config.addr().to_socket_addrs()?.collect();
        if addrs.is_empty() {
            return Err(Error::Protocol("no address resolved".into()));
        }
        for addr in addrs {
            let attempt_started = std::time::Instant::now();
            // `connect_timeout` returns a plain blocking socket — no
            // non-blocking flags left over from the timed connect.
            match TcpStream::connect_timeout(&addr, timeout) {
                Ok(stream) => {
                    let Some(setup_budget) = timeout.checked_sub(attempt_started.elapsed()) else {
                        last_err = Some(Error::Timeout(format!(
                            "connect to {addr} timed out after {timeout:?}"
                        )));
                        continue;
                    };
                    if setup_budget.is_zero() {
                        last_err = Some(Error::Timeout(format!(
                            "connect to {addr} timed out after {timeout:?}"
                        )));
                        continue;
                    }
                    let transport = crate::sync::transport::Transport::new_plain(stream);
                    match Self::connect_transport_with_budget(
                        transport,
                        config.clone(),
                        setup_budget,
                    ) {
                        Ok(client) => return Ok(client),
                        Err(e @ (Error::Io(_) | Error::Timeout(_))) => last_err = Some(e),
                        Err(e) => return Err(e),
                    }
                },
                Err(e) => {
                    last_err = Some(if is_socket_timeout(&e) {
                        Error::Timeout(format!("TCP connect to {addr} timed out after {timeout:?}"))
                    } else {
                        Error::Io(e)
                    });
                },
            }
        }
        Err(last_err.unwrap_or_else(|| Error::Protocol("no address resolved".into())))
    }

    /// Connect to a ClickHouse server at `host:port`.
    ///
    /// Uses default configuration, filling host and port from `addr`.
    pub fn connect(addr: &str) -> Result<Self> {
        let (host, port) = parse_host_port_addr(addr)?;
        let mut config = ClientConfig::default();
        config.host = host;
        config.port = port;
        SyncClient::connect_with_config(config)
    }

    /// Connect with a custom connection timeout.
    ///
    /// Uses default configuration, filling host and port from `addr`.
    pub fn connect_with_timeout(addr: &str, timeout: Duration) -> Result<Self> {
        let (host, port) = parse_host_port_addr(addr)?;
        let mut config = ClientConfig::default();
        config.host = host;
        config.port = port;
        config.connect_timeout = timeout;
        SyncClient::connect_with_config(config)
    }

    /// Connect using ClickHouse SSH-key authentication.
    ///
    /// The signer receives `protocol_revision + database + user + challenge`
    /// bytes and returns the signature string sent to ClickHouse.
    pub fn connect_with_ssh_signer<F>(addr: &str, user: &str, signer: F) -> Result<Self>
    where
        F: Fn(&[u8]) -> std::result::Result<String, String> + Send + Sync + 'static,
    {
        let (host, port) = parse_host_port_addr(addr)?;
        let config = ClientConfig::default()
            .with_host(&host)
            .with_port(port)
            .with_user(user)
            .with_ssh_signer(signer);
        SyncClient::connect_with_config(config)
    }

    /// Complete the connection setup over an already-established TCP stream.
    ///
    /// The stream must be a fresh blocking socket to a ClickHouse native
    /// endpoint — nothing may have been written to or read from it yet
    /// (`TCP_NODELAY` is set here). The whole setup phase — optional TLS
    /// handshake, native protocol handshake, and the handshake addendum — is
    /// bounded by one absolute `config.connect_timeout` deadline. A watchdog
    /// interrupts the socket at that deadline (temporary socket timeouts are a
    /// fallback), so even a byte-dripping peer cannot extend setup. On success,
    /// the normal query read timeout (`config.query_timeout`) is restored and
    /// writes are unbounded.
    ///
    /// Setup expiry surfaces as [`Error::Timeout`]; a `connect_timeout` of
    /// [`Duration::ZERO`](std::time::Duration::ZERO) is rejected up front with
    /// [`Error::Config`].
    pub fn connect_stream(stream: TcpStream, config: ClientConfig) -> Result<Self> {
        let transport = crate::sync::transport::Transport::new_plain(stream);
        Self::connect_transport(transport, config)
    }

    /// Connect using an already-established transport (plain or TLS).
    fn connect_transport(
        transport: crate::sync::transport::Transport, config: ClientConfig,
    ) -> Result<Self> {
        validate_connect_timeout(&config)?;
        let budget = config.connect_timeout;
        Self::connect_transport_with_budget(transport, config, budget)
    }

    /// Complete setup within one absolute wall-clock budget.
    fn connect_transport_with_budget(
        transport: crate::sync::transport::Transport, config: ClientConfig, budget: Duration,
    ) -> Result<Self> {
        validate_connect_timeout(&config)?;
        if budget.is_zero() {
            return Err(Error::Timeout(format!(
                "connection setup to {} had no remaining connect timeout",
                config.addr()
            )));
        }
        transport.set_nodelay(true)?;
        // Socket deadlines are a fallback for platforms where shutdown does
        // not promptly interrupt a blocking syscall. The watchdog below owns
        // the absolute wall-clock deadline and defeats byte-drip peers.
        transport.set_read_timeout(Some(budget))?;
        transport.set_write_timeout(Some(budget))?;
        let watchdog = SetupWatchdog::start(transport.raw_tcp(), budget)?;

        #[cfg(feature = "tls")]
        let mut transport = if let Some(ref tls_config) = config.tls_config {
            match transport {
                crate::sync::transport::Transport::Plain(s) => {
                    crate::sync::transport::Transport::new_tls(
                        s,
                        tls_config.clone(),
                        &config.tls_domain,
                    )?
                },
                tls @ crate::sync::transport::Transport::Tls(_) => tls,
            }
        } else {
            transport
        };
        #[cfg(not(feature = "tls"))]
        let mut transport = transport;

        let setup = Self::handshake_and_negotiate(&mut transport, &config);
        let deadline_expired = watchdog.finish();
        let setup = classify_setup_error(setup, &config, deadline_expired);

        // Back to normal query semantics. A successful setup is not allowed to
        // escape with stale setup deadlines if either restoration fails.
        if setup.is_ok() {
            transport.set_read_timeout(Some(config.query_timeout))?;
            transport.set_write_timeout(None)?;
        } else {
            // The failed socket is discarded; restoration is best effort only.
            let _ = transport.set_read_timeout(Some(config.query_timeout));
            let _ = transport.set_write_timeout(None);
        }
        let server_info = setup?;

        let query_template = build_query_packet_template(&config, server_info.negotiated_revision);

        Ok(SyncClient {
            stream: transport,
            server_info,
            config,
            query_template,
            schema_cache: HashMap::new(),
        })
    }

    /// Native handshake plus handshake addendum over an established transport.
    fn handshake_and_negotiate(
        transport: &mut crate::sync::transport::Transport, config: &ClientConfig,
    ) -> Result<ServerInfo> {
        let mut server_info = handshake::handshake(transport, config)?;

        if server_info.negotiated_revision >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM {
            let chunked = negotiate_chunked_transport(&server_info, config)?;
            server_info.use_chunked_send = chunked.send_chunked;
            server_info.use_chunked_recv = chunked.recv_chunked;
            let mut buf = Vec::new();
            wire::write_string(&mut buf, &config.quota_key)?;
            if server_info.negotiated_revision
                >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS
            {
                wire::write_string(&mut buf, chunked.send_mode)?;
                wire::write_string(&mut buf, chunked.recv_mode)?;
            }
            if server_info.negotiated_revision
                >= revision::DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL
            {
                wire::write_varint(&mut buf, revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION)?;
            }
            transport.write_all(&buf)?;
            transport.flush()?;
        }
        Ok(server_info)
    }

    // ── Builder methods ──

    pub fn with_setting(mut self, name: &str, value: &str) -> Self {
        self.config
            .settings
            .insert(name.to_owned(), value.to_owned());
        self.refresh_query_template();
        self
    }

    /// Control Native JSON serialization for materialized query results.
    ///
    /// Enabled by default to match clickhouse-cpp. Pass `false` to opt back into
    /// ClickHouse's native JSON/Object serialization.
    pub fn with_native_json_as_string(self, enabled: bool) -> Self {
        self.with_setting(
            crate::sync::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING,
            if enabled { "1" } else { "0" },
        )
    }

    /// Set a session setting at runtime. Rebuilds the query packet template.
    pub fn set_setting(&mut self, name: &str, value: &str) {
        self.config
            .settings
            .insert(name.to_owned(), value.to_owned());
        self.refresh_query_template();
    }

    /// Set Native JSON serialization mode at runtime.
    pub fn set_native_json_as_string(&mut self, enabled: bool) {
        self.set_setting(
            crate::sync::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING,
            if enabled { "1" } else { "0" },
        );
    }

    pub fn with_compression(mut self, method: crate::sync::compression::CompressionMethod) -> Self {
        self.config.compression = Some(method);
        self.refresh_query_template();
        self
    }

    pub fn with_recv_timeout(mut self, timeout: Duration) -> Self {
        self.config.query_timeout = timeout;
        let _ = self.stream.set_read_timeout(Some(timeout));
        self
    }

    pub fn with_schema_validation(mut self, enabled: bool) -> Self {
        self.config.validate_schema = enabled;
        self
    }

    pub fn server_info(&self) -> &ServerInfo {
        &self.server_info
    }

    /// Request ClickHouse replication/read-only status for a set of tables.
    pub fn tables_status(&mut self, tables: &[QualifiedTableName]) -> Result<TablesStatusResponse> {
        let rev = self.server_info.negotiated_revision;
        let pkt = crate::sync::protocol::table_status::build_tables_status_request(tables, rev)?;
        self.write_packet(&pkt)?;
        self.stream.flush()?;

        let packet_type = if self.server_info.use_chunked_recv {
            let mut reader = ChunkedReader::new(&mut self.stream);
            let packet_type = wire::read_varint(&mut reader)?;
            match packet_type {
                2 => return Err(read_exception_packet(&mut reader)),
                9 => {
                    return crate::sync::protocol::table_status::read_tables_status_response(
                        &mut reader,
                        rev,
                    );
                },
                _ => packet_type,
            }
        } else {
            let mut reader = std::io::BufReader::with_capacity(8192, &mut self.stream);
            let packet_type = wire::read_varint(&mut reader)?;
            match packet_type {
                2 => return Err(read_exception_packet(&mut reader)),
                9 => {
                    return crate::sync::protocol::table_status::read_tables_status_response(
                        &mut reader,
                        rev,
                    );
                },
                _ => packet_type,
            }
        };
        Err(Error::Protocol(format!(
            "expected TablesStatusResponse packet, got {packet_type}"
        )))
    }

    /// Request status for one table. Missing tables return `Ok(None)`.
    pub fn table_status(&mut self, database: &str, table: &str) -> Result<Option<TableStatus>> {
        let name = QualifiedTableName::new(database, table);
        let response = self.tables_status(std::slice::from_ref(&name))?;
        Ok(response.table_states_by_id.get(&name).cloned())
    }

    /// Return cached `DESCRIBE TABLE` metadata, fetching it on first use.
    pub fn schema_for_table(&mut self, table: &str) -> Result<TableSchema> {
        if !self.schema_cache.contains_key(table) {
            let schema = self.fetch_schema_for_table(table)?;
            self.schema_cache.insert(table.to_owned(), schema);
        }
        self.schema_cache
            .get(table)
            .cloned()
            .ok_or_else(|| Error::Protocol(format!("schema cache miss for {table}")))
    }

    /// Refresh cached `DESCRIBE TABLE` metadata.
    pub fn refresh_schema_for_table(&mut self, table: &str) -> Result<TableSchema> {
        let schema = self.fetch_schema_for_table(table)?;
        self.schema_cache.insert(table.to_owned(), schema.clone());
        Ok(schema)
    }

    pub fn clear_schema_cache(&mut self) {
        self.schema_cache.clear();
    }

    // ── Query execution ──

    /// Execute a DDL/DML (no result rows).
    pub fn execute(&mut self, query: &str) -> Result<()> {
        self.execute_with_params(query, &[])
    }

    /// Send Client::IgnoredPartUUIDs (8) for the next query on this connection.
    pub fn send_ignored_part_uuids(&mut self, uuids: &[[u8; 16]]) -> Result<()> {
        if uuids.is_empty() {
            return Ok(());
        }
        let pkt = crate::sync::protocol::part_uuid::build_ignored_part_uuids_packet(uuids);
        self.write_packet(&pkt)?;
        self.stream.flush()?;
        Ok(())
    }

    /// Execute a DDL/DML while ignoring replicated parts for this query.
    pub fn execute_with_ignored_part_uuids(
        &mut self, query: &str, uuids: &[[u8; 16]],
    ) -> Result<()> {
        self.execute_with_params_and_ignored_part_uuids(query, &[], uuids)
    }

    /// Execute a DDL/DML with server-side query parameters.
    pub fn execute_with_params(&mut self, query: &str, params: &[QueryParameter]) -> Result<()> {
        self.execute_with_params_and_ignored_part_uuids(query, params, &[])
    }

    /// Execute a DDL/DML with parameters while ignoring replicated parts.
    pub fn execute_with_params_and_ignored_part_uuids(
        &mut self, query: &str, params: &[QueryParameter], uuids: &[[u8; 16]],
    ) -> Result<()> {
        self.execute_with_params_settings_and_ignored_part_uuids(
            query,
            params,
            &HashMap::new(),
            uuids,
        )
    }

    /// Execute a DDL/DML with a per-query settings overlay.
    ///
    /// The overlay is merged into this query's packet only; the connection's
    /// session settings (and every later query) are untouched. An empty
    /// overlay is identical to [`execute`](Self::execute).
    pub fn execute_with_settings(
        &mut self, query: &str, settings: &HashMap<String, String>,
    ) -> Result<()> {
        self.execute_with_params_and_settings(query, &[], settings)
    }

    /// Execute a DDL/DML with parameters and a per-query settings overlay.
    pub fn execute_with_params_and_settings(
        &mut self, query: &str, params: &[QueryParameter], settings: &HashMap<String, String>,
    ) -> Result<()> {
        self.execute_with_params_settings_and_ignored_part_uuids(query, params, settings, &[])
    }

    /// Execute a DDL/DML with parameters, a per-query settings overlay, and
    /// ignored-part UUIDs.
    pub fn execute_with_params_settings_and_ignored_part_uuids(
        &mut self, query: &str, params: &[QueryParameter], settings: &HashMap<String, String>,
        uuids: &[[u8; 16]],
    ) -> Result<()> {
        self.send_ignored_part_uuids(uuids)?;
        let pkt = self.build_query_packet_with_params_and_settings(query, params, settings);
        self.write_packet(&pkt)?;
        self.stream.flush()?;
        self.drain_response()?;
        if query_may_change_schema(query) {
            self.clear_schema_cache();
        }
        Ok(())
    }

    /// Execute a SELECT and return all result blocks.
    ///
    /// Uses streaming reads from a `BufReader`-wrapped socket to minimise
    /// syscall overhead.  Reads packet by packet until EndOfStream (type 5).
    pub fn query(&mut self, query: &str) -> Result<Vec<Block>> {
        self.query_with_params(query, &[])
    }

    /// Execute a SELECT and decode all rows into owned values.
    pub fn query_all<T: crate::sync::row::Row>(&mut self, query: &str) -> Result<Vec<T>> {
        let blocks = self.query(query)?;
        let total_rows = blocks.iter().map(Block::row_count).sum();
        let mut rows = Vec::with_capacity(total_rows);
        for block in &blocks {
            rows.extend(crate::sync::row::read_all::<T>(block)?);
        }
        Ok(rows)
    }

    /// Execute a SELECT and decode exactly one row.
    pub fn query_one<T: crate::sync::row::Row>(&mut self, query: &str) -> Result<T> {
        match self.query_optional::<T>(query)? {
            Some(row) => Ok(row),
            None => Err(Error::Protocol("expected one row, got zero rows".into())),
        }
    }

    /// Execute a SELECT and decode zero or one row.
    pub fn query_optional<T: crate::sync::row::Row>(&mut self, query: &str) -> Result<Option<T>> {
        let mut rows = self.query_all::<T>(query)?;
        match rows.len() {
            0 => Ok(None),
            1 => Ok(rows.pop()),
            n => Err(Error::Protocol(format!(
                "expected zero or one row, got {n} rows"
            ))),
        }
    }

    /// Execute a SELECT and decode exactly one scalar value from the first column.
    pub fn query_scalar<T>(&mut self, query: &str) -> Result<T>
    where
        T: crate::sync::column::ClickHouseColumn + 'static,
    {
        let (value,) = self.query_one::<(T,)>(query)?;
        Ok(value)
    }

    /// Execute a SELECT and visit each data block without constructing owned
    /// [`Block`] / [`ColumnInfo`](crate::sync::protocol::block::ColumnInfo) values.
    ///
    /// Column data is valid only for the duration of the callback. Use
    /// [`query`](Self::query) when the result blocks must be retained.
    pub fn query_with_block_view<F>(&mut self, query: &str, visitor: F) -> Result<()>
    where
        F: FnMut(BlockView<'_>) -> Result<()>,
    {
        self.query_with_params_block_view(query, &[], visitor)
    }

    /// Execute a parameterized SELECT and visit borrowed native blocks.
    pub fn query_with_params_block_view<F>(
        &mut self, query: &str, params: &[QueryParameter], mut visitor: F,
    ) -> Result<()>
    where
        F: FnMut(BlockView<'_>) -> Result<()>,
    {
        let pkt = self.build_query_packet_with_params(query, params);
        self.write_packet(&pkt)?;
        self.stream.flush()?;
        let deadline = std::time::Instant::now() + self.config.query_timeout;
        let rev = self.server_info.negotiated_revision;
        if self.server_info.use_chunked_recv {
            let raw = TransportReader::new(&mut self.stream, self.config.read_buffer_size);
            let mut reader = ChunkedReader::new(raw);
            read_response_block_views(
                &mut reader,
                &mut visitor,
                deadline,
                rev,
                self.server_info.use_chunked_send,
            )
        } else {
            let mut reader = TransportReader::new(&mut self.stream, self.config.read_buffer_size);
            read_response_block_views(
                &mut reader,
                &mut visitor,
                deadline,
                rev,
                self.server_info.use_chunked_send,
            )
        }
    }

    /// Execute a SELECT and return only the total number of rows in Data
    /// packets. Column bytes are consumed and discarded without block
    /// materialization.
    pub fn query_row_count(&mut self, query: &str) -> Result<usize> {
        self.query_with_params_row_count(query, &[])
    }

    /// Execute a parameterized SELECT and count rows without storing columns.
    pub fn query_with_params_row_count(
        &mut self, query: &str, params: &[QueryParameter],
    ) -> Result<usize> {
        let pkt = self.build_query_packet_with_params(query, params);
        self.write_packet(&pkt)?;
        self.stream.flush()?;
        let deadline = std::time::Instant::now() + self.config.query_timeout;
        let rev = self.server_info.negotiated_revision;
        if self.server_info.use_chunked_recv {
            let raw = TransportReader::new(&mut self.stream, self.config.read_buffer_size);
            let mut reader = ChunkedReader::new(raw);
            read_response_row_count(
                &mut reader,
                deadline,
                rev,
                self.server_info.use_chunked_send,
            )
        } else {
            let mut reader = TransportReader::new(&mut self.stream, self.config.read_buffer_size);
            read_response_row_count(
                &mut reader,
                deadline,
                rev,
                self.server_info.use_chunked_send,
            )
        }
    }

    /// Execute a SELECT with server-side query parameters.
    pub fn query_with_params(
        &mut self, query: &str, params: &[QueryParameter],
    ) -> Result<Vec<Block>> {
        self.query_with_params_and_ignored_part_uuids(query, params, &[])
    }

    /// Execute a SELECT while ignoring replicated parts for this query.
    pub fn query_with_ignored_part_uuids(
        &mut self, query: &str, uuids: &[[u8; 16]],
    ) -> Result<Vec<Block>> {
        self.query_with_params_and_ignored_part_uuids(query, &[], uuids)
    }

    /// Execute a SELECT with parameters while ignoring replicated parts.
    pub fn query_with_params_and_ignored_part_uuids(
        &mut self, query: &str, params: &[QueryParameter], uuids: &[[u8; 16]],
    ) -> Result<Vec<Block>> {
        self.query_with_params_settings_and_ignored_part_uuids(
            query,
            params,
            &HashMap::new(),
            uuids,
        )
    }

    /// Execute a SELECT with a per-query settings overlay.
    ///
    /// The overlay is merged into this query's packet only; the connection's
    /// session settings (and every later query) are untouched. An empty
    /// overlay is identical to [`query`](Self::query).
    pub fn query_with_settings(
        &mut self, query: &str, settings: &HashMap<String, String>,
    ) -> Result<Vec<Block>> {
        self.query_with_params_and_settings(query, &[], settings)
    }

    /// Execute a SELECT with parameters and a per-query settings overlay.
    pub fn query_with_params_and_settings(
        &mut self, query: &str, params: &[QueryParameter], settings: &HashMap<String, String>,
    ) -> Result<Vec<Block>> {
        self.query_with_params_settings_and_ignored_part_uuids(query, params, settings, &[])
    }

    /// Execute a SELECT with parameters, a per-query settings overlay, and
    /// ignored-part UUIDs.
    pub fn query_with_params_settings_and_ignored_part_uuids(
        &mut self, query: &str, params: &[QueryParameter], settings: &HashMap<String, String>,
        uuids: &[[u8; 16]],
    ) -> Result<Vec<Block>> {
        self.send_ignored_part_uuids(uuids)?;
        let pkt = self.build_query_packet_with_params_and_settings(query, params, settings);
        self.write_packet(&pkt)?;
        self.stream.flush()?;
        let deadline = std::time::Instant::now() + self.config.query_timeout;
        let rev = self.server_info.negotiated_revision;
        let mut blocks = Vec::with_capacity(8);
        if self.server_info.use_chunked_recv {
            let raw = TransportReader::new(&mut self.stream, self.config.read_buffer_size);
            let mut reader = ChunkedReader::new(raw);
            read_response_blocks(
                &mut reader,
                &mut blocks,
                deadline,
                rev,
                self.server_info.use_chunked_send,
            )?;
        } else {
            let mut reader = TransportReader::new(&mut self.stream, self.config.read_buffer_size);
            read_response_blocks(
                &mut reader,
                &mut blocks,
                deadline,
                rev,
                self.server_info.use_chunked_send,
            )?;
        }
        Ok(blocks)
    }

    /// Consume the response to EndOfStream, surfacing every failure.
    ///
    /// Server exceptions and protocol/parse errors are returned as `Err`; a
    /// failed DDL/DML must never report success. After a server exception the
    /// connection framing is intact and the client stays usable; after a
    /// protocol error the stream position is unknown and the connection must
    /// be dropped.
    pub fn drain_response(&mut self) -> Result<()> {
        let deadline = std::time::Instant::now() + self.config.query_timeout;
        let rev = self.server_info.negotiated_revision;
        let mut blocks = Vec::new();
        if self.server_info.use_chunked_recv {
            let raw = TransportReader::new(&mut self.stream, self.config.read_buffer_size);
            let mut reader = ChunkedReader::new(raw);
            read_response_blocks(
                &mut reader,
                &mut blocks,
                deadline,
                rev,
                self.server_info.use_chunked_send,
            )
        } else {
            let mut reader = TransportReader::new(&mut self.stream, self.config.read_buffer_size);
            read_response_blocks(
                &mut reader,
                &mut blocks,
                deadline,
                rev,
                self.server_info.use_chunked_send,
            )
        }
    }

    /// Ping the server.
    pub fn ping(&mut self) -> Result<()> {
        self.write_packet(&[4])?;
        self.stream.flush()?;
        let mut pkt = [0u8; 1];
        if self.server_info.use_chunked_recv {
            let mut reader = ChunkedReader::new(&mut self.stream);
            reader.read_exact(&mut pkt)?;
        } else {
            self.stream.read_exact(&mut pkt)?;
        }
        if pkt[0] != 4 {
            return Err(Error::Protocol("expected Pong".into()));
        }
        Ok(())
    }

    /// Cancel the running query.
    pub fn cancel(&mut self) -> Result<()> {
        self.write_packet(&[3])?;
        self.stream.flush()?;
        Ok(())
    }

    // ── Raw stream access for INSERT ──

    /// Get a mutable reference to the underlying transport stream.
    pub fn stream_mut(&mut self) -> &mut crate::sync::transport::Transport {
        &mut self.stream
    }

    pub fn get_server_info(&self) -> &ServerInfo {
        &self.server_info
    }

    // ── Packet building ──

    /// Build the wire packet for a query.
    ///
    /// `settings` is a borrowed per-query overlay on top of the connection's
    /// session settings. An empty overlay keeps the fast path: the cached
    /// template bytes (which already contain the serialized session settings)
    /// are extended verbatim. A non-empty overlay serializes the merged
    /// settings block inline — neither `config.settings` nor the cached
    /// template is mutated.
    fn build_query_packet_inner(
        &self, query: &str, include_empty_block: bool, params: &[QueryParameter],
        settings: &HashMap<String, String>, buf: &mut Vec<u8>,
    ) {
        let mut query_id_buf = [0u8; 22];
        let query_id_len = next_query_id(&mut query_id_buf);
        let query_id = &query_id_buf[..query_id_len];
        buf.clear();
        buf.reserve(query.len() + query_id.len() * 2 + query_parameters_capacity(params) + 16);
        buf.extend_from_slice(&self.query_template.prefix);
        wire::write_string_bytes_to_vec(buf, query_id); // query_id
        if let Some(client_info) = &self.query_template.client_info {
            crate::sync::client_info::write_client_info_from_template(buf, client_info, query_id);
        }
        if settings.is_empty() {
            buf.extend_from_slice(&self.query_template.before_query);
        } else {
            crate::sync::query_packet::write_serialized_settings_overlay(
                &self.config.settings,
                settings,
                self.server_info.negotiated_revision,
                buf,
            );
            buf.extend_from_slice(
                &self.query_template.before_query[self.query_template.settings_len..],
            );
        }
        wire::write_string_to_vec(buf, query);
        if params.is_empty() && include_empty_block {
            buf.extend_from_slice(&self.query_template.select_suffix);
        } else if params.is_empty() {
            buf.extend_from_slice(&self.query_template.insert_suffix);
        } else {
            if self.server_info.negotiated_revision
                < revision::DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS
            {
                wire::write_string_to_vec(buf, "");
            } else {
                write_query_parameters_to_vec(buf, params);
            }
            if include_empty_block {
                self.write_empty_data_block(buf);
            }
        }
    }

    /// Build a query packet for SELECT queries.
    /// Includes a trailing empty Data block marker (required by CH 26.4+).
    pub fn build_query_packet(&self, query: &str) -> Vec<u8> {
        let mut buf = take_buf(self.query_template.select_capacity + query.len() + 80);
        self.build_query_packet_inner(query, true, &[], &HashMap::new(), &mut buf);
        buf
    }

    pub fn build_query_packet_with_params(
        &self, query: &str, params: &[QueryParameter],
    ) -> Vec<u8> {
        let mut buf = take_buf(
            self.query_template.select_capacity
                + query.len()
                + query_parameters_capacity(params)
                + 80,
        );
        self.build_query_packet_inner(query, true, params, &HashMap::new(), &mut buf);
        buf
    }

    /// Build a SELECT query packet with a per-query settings overlay.
    pub fn build_query_packet_with_settings(
        &self, query: &str, settings: &HashMap<String, String>,
    ) -> Vec<u8> {
        self.build_query_packet_with_params_and_settings(query, &[], settings)
    }

    /// Build a SELECT query packet with parameters and a settings overlay.
    pub fn build_query_packet_with_params_and_settings(
        &self, query: &str, params: &[QueryParameter], settings: &HashMap<String, String>,
    ) -> Vec<u8> {
        let mut buf = take_buf(
            self.query_template.select_capacity
                + query.len()
                + query_parameters_capacity(params)
                + serialized_settings_capacity(&self.config.settings, settings)
                + 80,
        );
        self.build_query_packet_inner(query, true, params, settings, &mut buf);
        buf
    }

    pub fn build_insert_query_packet(&self, query: &str) -> Vec<u8> {
        let mut buf = take_buf(self.query_template.insert_capacity + query.len() + 80);
        self.build_query_packet_inner(query, false, &[], &HashMap::new(), &mut buf);
        buf
    }

    fn write_empty_data_block(&self, buf: &mut Vec<u8>) {
        write_empty_data_block_to(buf);
    }

    // ── INSERT protocol ──

    pub fn begin_insert(&mut self, query: &str) -> Result<()> {
        let pkt = self.build_query_packet(query);
        self.write_packet(&pkt)?;
        self.stream.flush()?;
        self.wait_for_insert_table_structure()
    }

    pub fn send_data(&mut self, table_name: &str, block: &Block) -> Result<()> {
        self.validate_insert_block(table_name, block)?;
        if let Some(method) = self.config.compression {
            // Compressed path: buf-based (compression produces a single blob).
            let mut buf = Vec::with_capacity(
                crate::sync::protocol::block_writer::data_packet_capacity(table_name, block),
            );
            crate::sync::protocol::block_writer::write_data_packet_compressed(
                &mut buf, table_name, block, method,
            )?;
            self.write_packet(&buf)?;
        } else {
            // Uncompressed path: use writev to avoid copying column data.
            self.send_data_vectored(table_name, block)?;
        }
        self.stream.flush()?;
        Ok(())
    }

    /// Build a small header and send column data via `write_vectored`.
    /// Avoids copying column data into a temp buffer.
    fn send_data_vectored(&mut self, table_name: &str, block: &Block) -> Result<()> {
        // Build the header: packet_type + table_name + block_info + num_cols + num_rows
        let mut header = Vec::new();
        wire::write_varint(&mut header, 2)?; // ClientCode::Data
        wire::write_string(&mut header, table_name)?;
        // BlockInfo: dim=1 (is_overflows=0)
        wire::write_varint(&mut header, 1)?;
        header.push(0);
        // dim=2 (bucket_num = -1)
        wire::write_varint(&mut header, 2)?;
        header.extend_from_slice(&(-1i32).to_le_bytes());
        wire::write_varint(&mut header, 0)?; // terminator
        wire::write_varint(&mut header, block.columns.len() as u64)?;
        wire::write_varint(&mut header, block.rows as u64)?;

        let mut col_meta_chunks = Vec::with_capacity(block.columns.len());
        for col in &block.columns {
            let mut meta = Vec::new();
            wire::write_string(&mut meta, &col.name)?;
            wire::write_string(&mut meta, &col.type_name)?;
            meta.push(0); // custom_serialization
            col_meta_chunks.push(meta);
        }

        let mut col_slices: Vec<std::io::IoSlice<'_>> =
            Vec::with_capacity(1 + block.columns.len() * 2);
        col_slices.push(std::io::IoSlice::new(&header));
        for (idx, col) in block.columns.iter().enumerate() {
            col_slices.push(std::io::IoSlice::new(&col_meta_chunks[idx]));
            if !col.data.is_empty() {
                col_slices.push(std::io::IoSlice::new(&col.data));
            }
        }

        if self.server_info.use_chunked_send {
            let len = col_slices.iter().map(|s| s.len()).sum::<usize>();
            write_chunk_header(&mut self.stream, len)?;
            write_all_vectored(&mut self.stream, &mut col_slices)?;
            self.stream.write_all(&0u32.to_le_bytes())?;
        } else {
            write_all_vectored(&mut self.stream, &mut col_slices)?;
        }
        Ok(())
    }

    pub fn end_insert(&mut self) -> Result<()> {
        let mut buf = Vec::new();
        self.write_empty_data_block(&mut buf);
        self.write_packet(&buf)?;
        self.stream.flush()?;
        self.drain_response()
    }

    pub fn insert(&mut self, query: &str, table_name: &str, blocks: &[Block]) -> Result<()> {
        if self.config.validate_schema && !table_name.is_empty() {
            let schema = self.schema_for_table(table_name)?;
            for block in blocks {
                schema.validate_insert_block(table_name, block)?;
            }
        }
        self.begin_insert(query)?;
        for block in blocks {
            self.send_data("", block)?;
        }
        self.end_insert()
    }

    fn fetch_schema_for_table(&mut self, table: &str) -> Result<TableSchema> {
        let quoted = quote_identifier_path(table)?;
        let blocks = self.query(&format!("DESCRIBE TABLE {quoted}"))?;
        let mut columns = Vec::new();
        for block in &blocks {
            let names = block.column::<String>("name")?;
            let types = block.column::<String>("type")?;
            for row in 0..block.row_count() {
                columns.push(TableColumn {
                    name: names.get_string(row)?,
                    type_name: types.get_string(row)?,
                });
            }
        }
        Ok(TableSchema { columns })
    }

    fn validate_insert_block(&mut self, table_name: &str, block: &Block) -> Result<()> {
        if !self.config.validate_schema || table_name.is_empty() {
            return Ok(());
        }
        let schema = self.schema_for_table(table_name)?;
        schema.validate_insert_block(table_name, block)
    }

    // ── Streaming query ──

    pub fn start_stream(&mut self, query: &str) -> Result<QueryStream> {
        let pkt = self.build_query_packet(query);
        self.write_packet(&pkt)?;
        self.stream.flush()?;

        let cloned = self.stream.try_clone()?;
        Ok(QueryStream {
            buffer: Vec::new(),
            pos: 0,
            stream: cloned,
            read_buffer_size: self.config.read_buffer_size,
            compression: self.config.compression,
            negotiated_revision: self.server_info.negotiated_revision,
            chunked_recv: self.server_info.use_chunked_recv,
            chunked_send: self.server_info.use_chunked_send,
            done: false,
        })
    }

    // ── Helpers ──

    fn wait_for_insert_table_structure(&mut self) -> Result<()> {
        let deadline = std::time::Instant::now() + self.config.query_timeout;
        let rev = self.server_info.negotiated_revision;
        if self.server_info.use_chunked_recv {
            let raw = TransportReader::new(&mut self.stream, self.config.read_buffer_size);
            let mut reader = ChunkedReader::new(raw);
            return wait_for_insert_table_structure_from_reader(
                &mut reader,
                deadline,
                rev,
                self.server_info.use_chunked_send,
            );
        }
        let mut reader = TransportReader::new(&mut self.stream, self.config.read_buffer_size);
        wait_for_insert_table_structure_from_reader(
            &mut reader,
            deadline,
            rev,
            self.server_info.use_chunked_send,
        )
    }

    fn write_packet(&mut self, pkt: &[u8]) -> Result<()> {
        if self.server_info.use_chunked_send {
            write_chunked_packet(&mut self.stream, pkt)
        } else {
            self.stream.write_all(pkt).map_err(Into::into)
        }
    }

    fn refresh_query_template(&mut self) {
        self.query_template =
            build_query_packet_template(&self.config, self.server_info.negotiated_revision);
    }
}

fn wait_for_insert_table_structure_from_reader<R: Read + Write>(
    reader: &mut R, deadline: std::time::Instant, rev: u64, chunked_send: bool,
) -> Result<()> {
    loop {
        if std::time::Instant::now() > deadline {
            return Err(Error::Protocol(
                "insert table-structure deadline exceeded".into(),
            ));
        }
        match wire::read_varint(reader)? {
            1 => {
                let _ = crate::sync::protocol::response::read_block(reader)?;
                return Ok(());
            },
            2 => {
                return Err(read_exception_packet(reader));
            },
            3 => skip_progress_packet(reader, rev)?,
            4 => {},
            5 => {
                return Err(Error::Protocol(
                    "unexpected EndOfStream before insert data".into(),
                ));
            },
            6 => skip_profile_info_packet(reader, rev)?,
            7 | 8 => {
                let _ = crate::sync::protocol::response::read_block(reader)?;
            },
            10 | 14 => {
                let _tag = wire::read_string(reader)?;
                let _ = crate::sync::protocol::response::read_block_body(reader)?;
            },
            12 => skip_part_uuids_packet(reader)?,
            11 => {
                let _table = wire::read_string(reader)?;
                let _columns = wire::read_string(reader)?;
            },
            17 => {
                let _timezone = wire::read_string(reader)?;
            },
            other if handle_coordinator_packet(reader, other, chunked_send)? => {},
            other => {
                return Err(Error::Protocol(format!(
                    "unknown packet type while starting insert: {other}"
                )));
            },
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Response-parsing free functions (generic over R: Read for BufReader support)
// ════════════════════════════════════════════════════════════════════════════

/// Read response packets until EndOfStream (type 5).
/// Pushes Data blocks into `blocks`.
fn read_response_blocks<R: std::io::Read + std::io::Write>(
    reader: &mut R, blocks: &mut Vec<Block>, deadline: std::time::Instant, rev: u64,
    chunked_send: bool,
) -> Result<()> {
    loop {
        if std::time::Instant::now() > deadline {
            return Err(Error::Protocol("response read deadline exceeded".into()));
        }
        let packet_type = match wire::read_varint(reader) {
            Ok(t) => t,
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::TimedOut => {
                return Err(Error::Protocol("response read timed out".into()));
            },
            Err(e) => return Err(e),
        };

        match packet_type {
            1 => {
                let block = crate::sync::protocol::response::read_block(reader)?;
                blocks.push(block);
            },
            2 => return Err(read_exception_packet(reader)),
            3 => skip_progress_packet_for_revision(reader, rev)?,
            4 => {},
            5 => return Ok(()),
            6 => skip_profile_info_packet_for_revision(reader, rev)?,
            7 | 8 => {
                let _ = crate::sync::protocol::response::read_block(reader)?;
            },
            10 | 14 => {
                let _tag = wire::read_string(reader)?;
                let _ = crate::sync::protocol::response::read_block_body(reader)?;
            },
            12 => skip_part_uuids_packet(reader)?,
            11 => {
                let _table = wire::read_string(reader)?;
                let _columns = wire::read_string(reader)?;
            },
            17 => {
                let _timezone = wire::read_string(reader)?;
            },
            other if handle_coordinator_packet(reader, other, chunked_send)? => {},
            other => {
                return Err(Error::Protocol(format!("unknown packet type: {other}")));
            },
        }
    }
}

fn read_response_block_views<R, F>(
    reader: &mut R, visitor: &mut F, deadline: std::time::Instant, rev: u64, chunked_send: bool,
) -> Result<()>
where
    R: std::io::Read + std::io::Write,
    F: FnMut(BlockView<'_>) -> Result<()>,
{
    loop {
        if std::time::Instant::now() > deadline {
            return Err(Error::Protocol("response read deadline exceeded".into()));
        }
        let packet_type = match wire::read_varint(reader) {
            Ok(t) => t,
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::TimedOut => {
                return Err(Error::Protocol("response read timed out".into()));
            },
            Err(e) => return Err(e),
        };

        match packet_type {
            1 => crate::sync::protocol::response::read_block_view(reader, visitor)?,
            2 => return Err(read_exception_packet(reader)),
            3 => skip_progress_packet_for_revision(reader, rev)?,
            4 => {},
            5 => return Ok(()),
            6 => skip_profile_info_packet_for_revision(reader, rev)?,
            7 | 8 => {
                let _ = crate::sync::protocol::response::discard_block(reader)?;
            },
            10 | 14 => {
                let _tag = wire::read_string(reader)?;
                let _ = crate::sync::protocol::response::discard_block_body(reader)?;
            },
            12 => skip_part_uuids_packet(reader)?,
            11 => {
                let _table = wire::read_string(reader)?;
                let _columns = wire::read_string(reader)?;
            },
            17 => {
                let _timezone = wire::read_string(reader)?;
            },
            other if handle_coordinator_packet(reader, other, chunked_send)? => {},
            other => {
                return Err(Error::Protocol(format!("unknown packet type: {other}")));
            },
        }
    }
}

fn read_response_row_count<R: std::io::Read + std::io::Write>(
    reader: &mut R, deadline: std::time::Instant, rev: u64, chunked_send: bool,
) -> Result<usize> {
    let mut rows = 0usize;
    loop {
        if std::time::Instant::now() > deadline {
            return Err(Error::Protocol("response read deadline exceeded".into()));
        }
        let packet_type = match wire::read_varint(reader) {
            Ok(t) => t,
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::TimedOut => {
                return Err(Error::Protocol("response read timed out".into()));
            },
            Err(e) => return Err(e),
        };

        match packet_type {
            1 => {
                rows = rows
                    .checked_add(crate::sync::protocol::response::discard_block(reader)?)
                    .ok_or_else(|| Error::Protocol("row count overflow".into()))?;
            },
            2 => return Err(read_exception_packet(reader)),
            3 => skip_progress_packet_for_revision(reader, rev)?,
            4 => {},
            5 => return Ok(rows),
            6 => skip_profile_info_packet_for_revision(reader, rev)?,
            7 | 8 => {
                let _ = crate::sync::protocol::response::discard_block(reader)?;
            },
            10 | 14 => {
                let _tag = wire::read_string(reader)?;
                let _ = crate::sync::protocol::response::discard_block_body(reader)?;
            },
            12 => skip_part_uuids_packet(reader)?,
            11 => {
                let _table = wire::read_string(reader)?;
                let _columns = wire::read_string(reader)?;
            },
            17 => {
                let _timezone = wire::read_string(reader)?;
            },
            other if handle_coordinator_packet(reader, other, chunked_send)? => {},
            other => {
                return Err(Error::Protocol(format!("unknown packet type: {other}")));
            },
        }
    }
}

fn handle_coordinator_packet<R: Read + Write>(
    reader: &mut R, packet_type: u64, chunked_send: bool,
) -> Result<bool> {
    match packet_type {
        13 => {
            let pkt = build_empty_cluster_function_read_task_response();
            write_native_packet(reader, &pkt, chunked_send)?;
            Ok(true)
        },
        15 => {
            skip_parallel_read_announcement(reader)?;
            Ok(true)
        },
        16 => {
            let stream_id = read_parallel_read_request_stream_id(reader)?;
            let pkt = build_finished_merge_tree_read_task_response(&stream_id);
            write_native_packet(reader, &pkt, chunked_send)?;
            Ok(true)
        },
        _ => Ok(false),
    }
}

fn write_native_packet<W: Write>(writer: &mut W, pkt: &[u8], chunked_send: bool) -> Result<()> {
    if chunked_send {
        write_chunked_packet(writer, pkt)?;
    } else {
        writer.write_all(pkt)?;
    }
    writer.flush()?;
    Ok(())
}

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/shared/read_task_macros.rs"
));
define_read_task_packet_builders!(pub(crate));

fn skip_parallel_read_announcement<R: Read>(reader: &mut R) -> Result<()> {
    let _version = read_u64_le(reader)?;
    let _mode = read_u8(reader)?;
    skip_ranges_in_data_parts_description(
        reader,
        revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION,
    )?;
    let _replica_num = read_u64_le(reader)?;
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 4 {
        let _mark_segment_size = read_u64_le(reader)?;
    }
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 5 {
        let _initial_participating_replicas = wire::read_varint(reader)?;
    }
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 6 {
        let _min_marks_per_request = wire::read_varint(reader)?;
    }
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 7 {
        let _stream_id = wire::read_string(reader)?;
    }
    Ok(())
}

fn read_parallel_read_request_stream_id<R: Read>(reader: &mut R) -> Result<String> {
    let _version = read_u64_le(reader)?;
    let _mode = read_u8(reader)?;
    let _replica_num = read_u64_le(reader)?;
    let _min_marks_per_request = read_u64_le(reader)?;
    skip_ranges_in_data_parts_description(
        reader,
        revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION,
    )?;
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 7 {
        wire::read_string(reader)
    } else {
        Ok(String::new())
    }
}

fn skip_ranges_in_data_parts_description<R: Read>(
    reader: &mut R, parallel_replicas_protocol_version: u64,
) -> Result<()> {
    let count = usize::try_from(wire::read_varint(reader)?)
        .map_err(|_| Error::Protocol("parallel replica part range count too large".into()))?;
    for _ in 0..count {
        skip_merge_tree_part_info(reader)?;
        skip_mark_ranges(reader)?;
        let _rows = wire::read_varint(reader)?;
        if parallel_replicas_protocol_version >= 5 {
            let _projection_name = wire::read_string(reader)?;
        }
        if parallel_replicas_protocol_version >= 6 {
            let _min_marks_per_task = wire::read_varint(reader)?;
        }
    }
    Ok(())
}

fn skip_merge_tree_part_info<R: Read>(reader: &mut R) -> Result<()> {
    let _version = read_u64_le(reader)?;
    let _partition_id = wire::read_string(reader)?;
    let _min_block = read_u64_le(reader)?;
    let _max_block = read_u64_le(reader)?;
    let _level = read_u64_le(reader)?;
    let _mutation = read_u64_le(reader)?;
    let _use_legacy_max_level = read_u8(reader)?;
    Ok(())
}

fn skip_mark_ranges<R: Read>(reader: &mut R) -> Result<()> {
    let count = usize::try_from(read_u64_le(reader)?)
        .map_err(|_| Error::Protocol("parallel replica mark range count too large".into()))?;
    discard_exact(
        reader,
        checked_len(count, 16, "parallel replica mark ranges")?,
    )
}

fn read_u64_le<R: Read>(reader: &mut R) -> Result<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

fn read_u8<R: Read>(reader: &mut R) -> Result<u8> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn discard_exact<R: Read>(reader: &mut R, len: usize) -> Result<()> {
    let mut remaining = len;
    let mut buf = [0u8; 1024];
    while remaining > 0 {
        let n = remaining.min(buf.len());
        reader.read_exact(&mut buf[..n])?;
        remaining -= n;
    }
    Ok(())
}

fn checked_len(count: usize, elem_size: usize, what: &str) -> Result<usize> {
    count
        .checked_mul(elem_size)
        .ok_or_else(|| Error::Protocol(format!("{what} byte length overflow")))
}

#[cold]
/// Read an Exception packet (type 2) body into a structured error.
///
/// A fully parsed chain yields [`Error::ServerError`] with the root
/// exception's code/name and the whole nested chain in `message`. Any read or
/// parse failure inside the packet is returned as-is so malformed protocol
/// stays distinguishable from a terminal server exception.
fn read_exception_packet<R: std::io::Read>(reader: &mut R) -> Error {
    let mut messages = Vec::new();
    let mut root: Option<(i32, String)> = None;
    loop {
        let code = match wire::read_bytes(reader, 4) {
            Ok(bytes) => {
                let mut code_bytes = [0u8; 4];
                code_bytes.copy_from_slice(&bytes);
                i32::from_le_bytes(code_bytes)
            },
            Err(e) => return e,
        };
        // Lossy decode: a server exception must surface even if its message
        // contains bytes that are not valid UTF-8.
        let name = match read_string_lossy(reader) {
            Ok(name) => name,
            Err(e) => return e,
        };
        let msg = match read_string_lossy(reader) {
            Ok(msg) => msg,
            Err(e) => return e,
        };
        if let Err(e) = wire::read_string_bytes(reader) {
            return e; // stack trace still frames the packet; it must parse
        }
        messages.push(format!("{name} (code {code}): {msg}"));
        if root.is_none() {
            root = Some((code, name));
        }
        match wire::read_bytes(reader, 1) {
            Ok(flag) if flag.first().copied().unwrap_or(0) != 0 => {},
            Ok(_) => break,
            Err(e) => return e,
        }
    }
    let (code, name) = root.unwrap_or((0, "unknown".to_string()));
    Error::ServerError {
        code,
        name,
        message: messages.join(" | nested: "),
    }
}

fn read_string_lossy<R: std::io::Read>(reader: &mut R) -> Result<String> {
    wire::read_string_bytes(reader).map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
}

fn skip_progress_packet<R: std::io::Read>(reader: &mut R, rev: u64) -> Result<()> {
    wire::read_varint(reader)?; // rows
    wire::read_varint(reader)?; // bytes
    wire::read_varint(reader)?; // total_rows
    if rev >= revision::DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
        wire::read_varint(reader)?; // written_rows
        wire::read_varint(reader)?; // written_bytes
    }
    if rev >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS {
        wire::read_varint(reader)?; // elapsed_ns
    }
    if rev >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS {
        wire::read_varint(reader)?; // total_bytes_to_read
    }
    Ok(())
}

fn skip_progress_packet_for_revision<R: std::io::Read>(reader: &mut R, rev: u64) -> Result<()> {
    match rev {
        54459 => skip_progress_packet_const::<54459, R>(reader),
        54464 => skip_progress_packet_const::<54464, R>(reader),
        54483 => skip_progress_packet_const::<54483, R>(reader),
        _ => skip_progress_packet(reader, rev),
    }
}

fn skip_progress_packet_const<const REV: u64, R: std::io::Read>(reader: &mut R) -> Result<()> {
    wire::read_varint(reader)?; // rows
    wire::read_varint(reader)?; // bytes
    wire::read_varint(reader)?; // total_rows
    if REV >= revision::DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
        wire::read_varint(reader)?; // written_rows
        wire::read_varint(reader)?; // written_bytes
    }
    if REV >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS {
        wire::read_varint(reader)?; // elapsed_ns
    }
    if REV >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS {
        wire::read_varint(reader)?; // total_bytes_to_read
    }
    Ok(())
}

fn skip_profile_info_packet<R: std::io::Read>(reader: &mut R, rev: u64) -> Result<()> {
    wire::read_varint(reader)?; // rows
    wire::read_varint(reader)?; // blocks
    wire::read_varint(reader)?; // bytes
    let mut b = [0u8; 1];
    reader.read_exact(&mut b)?; // applied_limit
    wire::read_varint(reader)?; // rows_before_limit
    reader.read_exact(&mut b)?; // calculated_rows_before_limit
    if rev >= revision::DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION {
        reader.read_exact(&mut b)?; // applied_aggregation
        wire::read_varint(reader)?; // rows_before_aggregation
    }
    Ok(())
}

fn skip_profile_info_packet_for_revision<R: std::io::Read>(reader: &mut R, rev: u64) -> Result<()> {
    match rev {
        54459 => skip_profile_info_packet_const::<54459, R>(reader),
        54464 => skip_profile_info_packet_const::<54464, R>(reader),
        54483 => skip_profile_info_packet_const::<54483, R>(reader),
        _ => skip_profile_info_packet(reader, rev),
    }
}

fn skip_profile_info_packet_const<const REV: u64, R: std::io::Read>(reader: &mut R) -> Result<()> {
    wire::read_varint(reader)?; // rows
    wire::read_varint(reader)?; // blocks
    wire::read_varint(reader)?; // bytes
    let mut b = [0u8; 1];
    reader.read_exact(&mut b)?; // applied_limit
    wire::read_varint(reader)?; // rows_before_limit
    reader.read_exact(&mut b)?; // calculated_rows_before_limit
    if REV >= revision::DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION {
        reader.read_exact(&mut b)?; // applied_aggregation
        wire::read_varint(reader)?; // rows_before_aggregation
    }
    Ok(())
}

fn skip_part_uuids_packet<R: std::io::Read>(reader: &mut R) -> Result<()> {
    let count = usize::try_from(wire::read_varint(reader)?)
        .map_err(|_| Error::Protocol("PartUUIDs count too large".into()))?;
    let len = count
        .checked_mul(16)
        .ok_or_else(|| Error::Protocol("PartUUIDs byte length overflow".into()))?;
    let mut remaining = len;
    let mut buf = [0u8; 1024];
    while remaining > 0 {
        let n = remaining.min(buf.len());
        reader.read_exact(&mut buf[..n])?;
        remaining -= n;
    }
    Ok(())
}

fn skip_part_uuids_buffer(buf: &[u8], pos: &mut usize) -> Result<()> {
    let count = usize::try_from(wire::parse_varint(buf, pos)?)
        .map_err(|_| Error::Protocol("PartUUIDs count too large".into()))?;
    let len = count
        .checked_mul(16)
        .ok_or_else(|| Error::Protocol("PartUUIDs byte length overflow".into()))?;
    let end = (*pos)
        .checked_add(len)
        .ok_or_else(|| Error::Protocol("PartUUIDs buffer position overflow".into()))?;
    if end > buf.len() {
        return Err(Error::Protocol("unexpected end of PartUUIDs packet".into()));
    }
    *pos = end;
    Ok(())
}

fn skip_parallel_read_announcement_buffer(buf: &[u8], pos: &mut usize) -> Result<()> {
    let _version = parse_u64_le(buf, pos)?;
    let _mode = parse_u8(buf, pos)?;
    skip_ranges_in_data_parts_description_buffer(
        buf,
        pos,
        revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION,
    )?;
    let _replica_num = parse_u64_le(buf, pos)?;
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 4 {
        let _mark_segment_size = parse_u64_le(buf, pos)?;
    }
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 5 {
        let _initial_participating_replicas = wire::parse_varint(buf, pos)?;
    }
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 6 {
        let _min_marks_per_request = wire::parse_varint(buf, pos)?;
    }
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 7 {
        let _stream_id = wire::parse_string(buf, pos)?;
    }
    Ok(())
}

fn read_parallel_read_request_stream_id_buffer(buf: &[u8], pos: &mut usize) -> Result<String> {
    let _version = parse_u64_le(buf, pos)?;
    let _mode = parse_u8(buf, pos)?;
    let _replica_num = parse_u64_le(buf, pos)?;
    let _min_marks_per_request = parse_u64_le(buf, pos)?;
    skip_ranges_in_data_parts_description_buffer(
        buf,
        pos,
        revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION,
    )?;
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 7 {
        wire::parse_string(buf, pos).map(str::to_owned)
    } else {
        Ok(String::new())
    }
}

fn skip_ranges_in_data_parts_description_buffer(
    buf: &[u8], pos: &mut usize, parallel_replicas_protocol_version: u64,
) -> Result<()> {
    let count = usize::try_from(wire::parse_varint(buf, pos)?)
        .map_err(|_| Error::Protocol("parallel replica part range count too large".into()))?;
    for _ in 0..count {
        skip_merge_tree_part_info_buffer(buf, pos)?;
        skip_mark_ranges_buffer(buf, pos)?;
        let _rows = wire::parse_varint(buf, pos)?;
        if parallel_replicas_protocol_version >= 5 {
            let _projection_name = wire::parse_string(buf, pos)?;
        }
        if parallel_replicas_protocol_version >= 6 {
            let _min_marks_per_task = wire::parse_varint(buf, pos)?;
        }
    }
    Ok(())
}

fn skip_merge_tree_part_info_buffer(buf: &[u8], pos: &mut usize) -> Result<()> {
    let _version = parse_u64_le(buf, pos)?;
    let _partition_id = wire::parse_string(buf, pos)?;
    let _min_block = parse_u64_le(buf, pos)?;
    let _max_block = parse_u64_le(buf, pos)?;
    let _level = parse_u64_le(buf, pos)?;
    let _mutation = parse_u64_le(buf, pos)?;
    let _use_legacy_max_level = parse_u8(buf, pos)?;
    Ok(())
}

fn skip_mark_ranges_buffer(buf: &[u8], pos: &mut usize) -> Result<()> {
    let count = usize::try_from(parse_u64_le(buf, pos)?)
        .map_err(|_| Error::Protocol("parallel replica mark range count too large".into()))?;
    let len = checked_len(count, 16, "parallel replica mark ranges")?;
    let end = (*pos)
        .checked_add(len)
        .ok_or_else(|| Error::Protocol("parallel replica mark ranges position overflow".into()))?;
    if end > buf.len() {
        return Err(Error::Protocol(
            "unexpected end of parallel replica mark ranges".into(),
        ));
    }
    *pos = end;
    Ok(())
}

fn parse_u64_le(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let bytes = wire::parse_bytes(buf, pos, 8)?;
    let mut out = [0u8; 8];
    out.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(out))
}

fn parse_u8(buf: &[u8], pos: &mut usize) -> Result<u8> {
    Ok(wire::parse_bytes(buf, pos, 1)?[0])
}

fn write_all_vectored<W: Write>(writer: &mut W, slices: &mut [std::io::IoSlice<'_>]) -> Result<()> {
    let mut slices = slices;
    while !slices.is_empty() {
        let n = writer.write_vectored(slices)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write vectored packet",
            )
            .into());
        }
        std::io::IoSlice::advance_slices(&mut slices, n);
    }
    Ok(())
}

// ════════════════════════════════════════════════════════════════════════════
// QueryStream — streaming query result parser
// ════════════════════════════════════════════════════════════════════════════

pub struct QueryStream {
    buffer: Vec<u8>,
    pos: usize,
    stream: crate::sync::transport::Transport,
    read_buffer_size: usize,
    #[allow(dead_code)]
    compression: Option<crate::sync::compression::CompressionMethod>,
    negotiated_revision: u64,
    chunked_recv: bool,
    chunked_send: bool,
    done: bool,
}

impl QueryStream {
    pub fn read_next_block(&mut self) -> Result<Option<Block>> {
        if self.done {
            return Ok(None);
        }
        loop {
            if self.pos < self.buffer.len() {
                let saved_pos = self.pos;
                let packet_type = match wire::parse_varint(&self.buffer, &mut self.pos) {
                    Ok(v) => v,
                    Err(Error::Protocol(_)) => {
                        self.pos = saved_pos;
                        self.fill_buffer()?;
                        continue;
                    },
                    Err(e) => return Err(e),
                };
                match packet_type {
                    1 => match parse_block(&self.buffer, &mut self.pos) {
                        Ok(block) => return Ok(Some(block)),
                        Err(Error::Protocol(_)) => {
                            self.pos = saved_pos;
                            self.fill_buffer()?;
                            continue;
                        },
                        Err(e) => return Err(e),
                    },
                    5 => {
                        self.done = true;
                        return Ok(None);
                    },
                    2 => match parse_exception_chain(&self.buffer, &mut self.pos) {
                        Ok((code, name, message)) => {
                            self.done = true;
                            return Err(Error::ServerError {
                                code,
                                name,
                                message,
                            });
                        },
                        Err(Error::Protocol(_)) => {
                            self.pos = saved_pos;
                            let buffered = self.buffer.len();
                            self.fill_buffer()?;
                            if self.buffer.len() == buffered {
                                self.done = true;
                                return Err(Error::Protocol(
                                    "truncated exception packet in query stream".into(),
                                ));
                            }
                            continue;
                        },
                        Err(e) => return Err(e),
                    },
                    3 => {
                        let fields = 3
                            + if self.negotiated_revision
                                >= revision::DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO
                            {
                                2
                            } else {
                                0
                            }
                            + if self.negotiated_revision
                                >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS
                            {
                                1
                            } else {
                                0
                            }
                            + if self.negotiated_revision
                                >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS
                            {
                                1
                            } else {
                                0
                            };
                        for _ in 0..fields {
                            match wire::parse_varint(&self.buffer, &mut self.pos) {
                                Err(Error::Protocol(_)) => {
                                    self.pos = saved_pos;
                                    self.fill_buffer()?;
                                    break;
                                },
                                Err(e) => return Err(e),
                                _ => {},
                            }
                        }
                    },
                    4 => {},
                    6 => {
                        let mut ok = true;
                        for _ in 0..3 {
                            if let Err(Error::Protocol(_)) =
                                wire::parse_varint(&self.buffer, &mut self.pos)
                            {
                                ok = false;
                                break;
                            }
                        }
                        if ok && self.pos < self.buffer.len() {
                            self.pos += 1; // applied_limit
                            if let Err(Error::Protocol(_)) =
                                wire::parse_varint(&self.buffer, &mut self.pos)
                            {
                                ok = false;
                            }
                        }
                        if ok && self.pos < self.buffer.len() {
                            self.pos += 1; // calculated_rows_before_limit
                        } else {
                            self.pos = saved_pos;
                            self.fill_buffer()?;
                        }
                        if ok
                            && self.negotiated_revision
                                >= revision::DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION
                        {
                            if self.pos < self.buffer.len() {
                                self.pos += 1; // applied_aggregation
                            } else {
                                self.pos = saved_pos;
                                self.fill_buffer()?;
                                continue;
                            }
                            if let Err(Error::Protocol(_)) =
                                wire::parse_varint(&self.buffer, &mut self.pos)
                            {
                                self.pos = saved_pos;
                                self.fill_buffer()?;
                            }
                        }
                    },
                    7 | 8 => match parse_block(&self.buffer, &mut self.pos) {
                        Ok(_) => {},
                        Err(Error::Protocol(_)) => {
                            self.pos = saved_pos;
                            self.fill_buffer()?;
                            continue;
                        },
                        Err(e) => return Err(e),
                    },
                    10 | 14 => {
                        if wire::parse_string(&self.buffer, &mut self.pos).is_err() {
                            self.pos = saved_pos;
                            self.fill_buffer()?;
                            continue;
                        }
                        match parse_block_body(&self.buffer, &mut self.pos) {
                            Ok(_) => {},
                            Err(Error::Protocol(_)) => {
                                self.pos = saved_pos;
                                self.fill_buffer()?;
                                continue;
                            },
                            Err(e) => return Err(e),
                        }
                    },
                    11 => {
                        if wire::parse_string(&self.buffer, &mut self.pos).is_err()
                            || wire::parse_string(&self.buffer, &mut self.pos).is_err()
                        {
                            self.pos = saved_pos;
                            self.fill_buffer()?;
                        }
                    },
                    12 => {
                        if skip_part_uuids_buffer(&self.buffer, &mut self.pos).is_err() {
                            self.pos = saved_pos;
                            self.fill_buffer()?;
                        }
                    },
                    17 => {
                        if wire::parse_string(&self.buffer, &mut self.pos).is_err() {
                            self.pos = saved_pos;
                            self.fill_buffer()?;
                        }
                    },
                    13 => {
                        let pkt = build_empty_cluster_function_read_task_response();
                        write_native_packet(&mut self.stream, &pkt, self.chunked_send)?;
                    },
                    15 => {
                        if skip_parallel_read_announcement_buffer(&self.buffer, &mut self.pos)
                            .is_err()
                        {
                            self.pos = saved_pos;
                            self.fill_buffer()?;
                        }
                    },
                    16 => {
                        match read_parallel_read_request_stream_id_buffer(
                            &self.buffer,
                            &mut self.pos,
                        ) {
                            Ok(stream_id) => {
                                let pkt = build_finished_merge_tree_read_task_response(&stream_id);
                                write_native_packet(&mut self.stream, &pkt, self.chunked_send)?;
                            },
                            Err(Error::Protocol(_)) => {
                                self.pos = saved_pos;
                                self.fill_buffer()?;
                            },
                            Err(e) => return Err(e),
                        }
                    },
                    other => {
                        return Err(Error::Protocol(format!(
                            "unknown packet type in stream: {other}"
                        )));
                    },
                }
            } else {
                self.fill_buffer()?;
            }
        }
    }

    fn fill_buffer(&mut self) -> Result<()> {
        if self.chunked_recv {
            let mut len_buf = [0u8; 4];
            loop {
                match self.stream.read_exact(&mut len_buf) {
                    Ok(()) => {
                        let len = u32::from_le_bytes(len_buf) as usize;
                        if len == 0 {
                            continue;
                        }
                        let start = self.buffer.len();
                        self.buffer.resize(start + len, 0);
                        self.stream.read_exact(&mut self.buffer[start..])?;
                        return Ok(());
                    },
                    Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                        self.done = true;
                        return Ok(());
                    },
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        self.done = true;
                        return Ok(());
                    },
                    Err(e) => return Err(e.into()),
                }
            }
        }
        let mut buf = vec![0u8; self.read_buffer_size];
        match self.stream.read(&mut buf) {
            Ok(0) => {
                self.done = true;
                Ok(())
            },
            Ok(n) => {
                self.buffer.extend_from_slice(&buf[..n]);
                Ok(())
            },
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                self.done = true;
                Ok(())
            },
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put_string(buf: &mut Vec<u8>, s: &str) {
        wire::write_varint(buf, s.len() as u64).expect("test operation failed");
        buf.extend_from_slice(s.as_bytes());
    }

    /// Body of an Exception packet (after the type varint) for one exception.
    fn exception_body(code: i32, name: &str, msg: &str, nested: bool) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&code.to_le_bytes());
        put_string(&mut buf, name);
        put_string(&mut buf, msg);
        put_string(&mut buf, ""); // stack trace
        buf.push(u8::from(nested)); // has_nested
        buf
    }

    #[test]
    fn read_exception_packet_parses_chain_into_server_error() {
        let mut body = exception_body(60, "DB::Exception", "unknown function xyz", true);
        body.extend(exception_body(48, "DB::Exception", "inner cause", false));
        let err = read_exception_packet(&mut std::io::Cursor::new(body));
        match &err {
            Error::ServerError {
                code,
                name,
                message,
            } => {
                assert_eq!(*code, 60, "root code must be reported");
                assert_eq!(name, "DB::Exception");
                assert!(
                    message.contains("unknown function xyz") && message.contains("inner cause"),
                    "message must carry the whole chain: {message}"
                );
            },
            _other => unreachable!("expected ServerError, got {err:?}"),
        }
    }

    #[test]
    fn read_exception_packet_distinguishes_malformed_body() {
        let mut body = exception_body(60, "DB::Exception", "unknown function xyz", false);
        body.truncate(body.len() - 2); // cut the has_nested flag
        let err = read_exception_packet(&mut std::io::Cursor::new(body));
        assert!(
            matches!(err, Error::Protocol(_) | Error::Io(_)),
            "truncated exception body must not become ServerError, got {err:?}"
        );
    }

    fn query_stream_with_buffer(buffer: Vec<u8>) -> (QueryStream, std::net::TcpStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test listener");
        let client = std::net::TcpStream::connect(listener.local_addr().expect("listener address"))
            .expect("connect test socket");
        let (server, _) = listener.accept().expect("accept test socket");
        client
            .set_read_timeout(Some(std::time::Duration::from_millis(250)))
            .expect("set test timeout");
        (
            QueryStream {
                buffer,
                pos: 0,
                stream: crate::sync::transport::Transport::new_plain(client),
                read_buffer_size: 8192,
                compression: None,
                negotiated_revision: revision::DEFAULT_PROTOCOL_REVISION,
                chunked_recv: false,
                chunked_send: false,
                done: false,
            },
            server,
        )
    }

    #[test]
    fn query_stream_refills_and_parses_nested_exception_chain() {
        let mut packet = vec![2];
        packet.extend(exception_body(1000, "DB::Exception", "outer", true));
        packet.extend(exception_body(48, "DB::Exception", "inner", false));
        let split = 9;
        let (mut stream, mut server) = query_stream_with_buffer(packet[..split].to_vec());
        server
            .write_all(&packet[split..])
            .expect("write remainder of split exception");

        let err = stream
            .read_next_block()
            .err()
            .expect("exception stream must return an error");
        let Error::ServerError {
            code,
            name,
            message,
        } = err
        else {
            unreachable!("expected ServerError");
        };
        assert_eq!(code, 1000);
        assert_eq!(name, "DB::Exception");
        assert!(message.contains("outer") && message.contains("inner"));
    }

    #[test]
    fn query_stream_truncated_exception_terminates_as_protocol_error() {
        let mut packet = vec![2];
        packet.extend_from_slice(&46i32.to_le_bytes());
        let (mut stream, server) = query_stream_with_buffer(packet);
        server
            .shutdown(std::net::Shutdown::Write)
            .expect("close server write side");

        let err = stream
            .read_next_block()
            .err()
            .expect("truncated exception must return an error");
        assert!(
            matches!(err, Error::Protocol(ref message) if message.contains("truncated exception")),
            "expected truncated Protocol error, got {err:?}"
        );
    }

    #[test]
    fn read_response_blocks_surfaces_server_exception() {
        let mut wire_bytes = Vec::new();
        wire::write_varint(&mut wire_bytes, 2).expect("test operation failed");
        wire_bytes.extend(exception_body(
            60,
            "DB::Exception",
            "unknown function xyz",
            false,
        ));

        let mut reader = std::io::Cursor::new(wire_bytes);
        let mut blocks = Vec::new();
        let err = read_response_blocks(
            &mut reader,
            &mut blocks,
            std::time::Instant::now() + std::time::Duration::from_secs(5),
            revision::DEFAULT_PROTOCOL_REVISION,
            false,
        )
        .expect_err("server exception must surface");
        assert!(err.is_server_error(), "expected ServerError, got {err:?}");
    }

    #[test]
    fn read_response_blocks_end_of_stream_is_ok_and_unknown_packet_is_err() {
        let mut reader = std::io::Cursor::new(vec![5u8]);
        let mut blocks = Vec::new();
        read_response_blocks(
            &mut reader,
            &mut blocks,
            std::time::Instant::now() + std::time::Duration::from_secs(5),
            revision::DEFAULT_PROTOCOL_REVISION,
            false,
        )
        .expect("EndOfStream drains to Ok");

        let mut reader = std::io::Cursor::new(vec![99u8, 5]);
        let mut blocks = Vec::new();
        let err = read_response_blocks(
            &mut reader,
            &mut blocks,
            std::time::Instant::now() + std::time::Duration::from_secs(5),
            revision::DEFAULT_PROTOCOL_REVISION,
            false,
        )
        .expect_err("unknown packet type must surface");
        assert!(
            matches!(err, Error::Protocol(ref msg) if msg.contains("unknown packet type")),
            "expected protocol error, got {err:?}"
        );
    }

    #[test]
    fn chunked_reader_removes_native_chunk_markers() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&3u32.to_le_bytes());
        wire.extend_from_slice(b"abc");
        wire.extend_from_slice(&0u32.to_le_bytes());
        wire.extend_from_slice(&2u32.to_le_bytes());
        wire.extend_from_slice(b"de");

        let mut reader = ChunkedReader::new(std::io::Cursor::new(wire));
        let mut out = [0u8; 5];
        reader.read_exact(&mut out).expect("test operation failed");
        assert_eq!(&out, b"abcde");
    }

    #[test]
    fn chunked_negotiation_matches_clickhouse_cpp_rules() {
        assert!(
            choose_chunked_mode("chunked_optional", "chunked_optional", "recv")
                .expect("test operation failed")
        );
        assert!(
            !choose_chunked_mode("notchunked", "chunked_optional", "recv")
                .expect("test operation failed")
        );
        assert!(
            choose_chunked_mode("chunked", "chunked_optional", "recv")
                .expect("test operation failed")
        );
        assert!(
            choose_chunked_mode("notchunked_optional", "chunked", "recv")
                .expect("test operation failed")
        );
        assert!(choose_chunked_mode("chunked", "notchunked", "recv").is_err());
    }
    // ── Per-query settings overlay: server-free packet tests ──────────────

    /// Build a `SyncClient` that never touches the network: packets are
    /// built from the cached template exactly as after a real handshake.
    fn offline_client(settings: &[(&str, &str)], rev: u64) -> SyncClient {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test listener");
        let sock = std::net::TcpStream::connect(listener.local_addr().expect("listener address"))
            .expect("connect test socket");
        drop(listener.accept().expect("accept test socket").0);
        let mut config = ClientConfig::default();
        config.client_revision = rev;
        for (name, value) in settings {
            config
                .settings
                .insert((*name).to_string(), (*value).to_string());
        }
        let server_info = ServerInfo {
            name: "test-server".to_string(),
            major: 26,
            minor: 4,
            patch: 1,
            revision: rev,
            negotiated_revision: rev,
            timezone: None,
            display_name: None,
            server_parallel_replicas_protocol_version: 0,
            proto_send_chunked_srv: String::new(),
            proto_recv_chunked_srv: String::new(),
            use_chunked_send: false,
            use_chunked_recv: false,
            password_complexity_rules: Vec::new(),
            interserver_secret_nonce: None,
            server_query_plan_serialization_version: None,
            worker_cluster_function_protocol_version: 0,
        };
        SyncClient {
            stream: crate::sync::transport::Transport::new_plain(sock),
            server_info,
            query_template: build_query_packet_template(&config, rev),
            config,
            schema_cache: HashMap::new(),
        }
    }

    fn read_varint_at(packet: &[u8], pos: usize) -> (usize, usize) {
        let mut reader = std::io::Cursor::new(&packet[pos..]);
        let value = wire::read_varint(&mut reader).expect("test varint");
        (value as usize, reader.position() as usize)
    }

    /// Byte offset where a query packet's serialized settings block starts.
    fn settings_offset(client: &SyncClient, packet: &[u8]) -> usize {
        let mut pos = client.query_template.prefix.len();
        let (qid_len, n) = read_varint_at(packet, pos);
        pos += n + qid_len;
        if let Some(ci) = &client.query_template.client_info {
            pos += ci.before_initial_query_id.len();
            let (qid_len, n) = read_varint_at(packet, pos);
            pos += n + qid_len;
            pos += ci.after_initial_query_id.len();
        }
        pos
    }

    /// Parse the settings block of a packet. Returns the entries and the
    /// packet offset just past the empty-name terminator.
    fn packet_settings(client: &SyncClient, packet: &[u8]) -> (Vec<(String, String)>, usize) {
        let start = settings_offset(client, packet);
        let mut reader = std::io::Cursor::new(&packet[start..]);
        let mut out = Vec::new();
        loop {
            let name = wire::read_string(&mut reader).expect("setting name");
            if name.is_empty() {
                return (out, start + reader.position() as usize);
            }
            let _flags = wire::read_varint(&mut reader).expect("setting flags");
            let value = wire::read_string(&mut reader).expect("setting value");
            out.push((name, value));
        }
    }

    /// Packet bytes minus the two per-query generated query-id strings, so
    /// consecutive packets are byte-comparable.
    fn strip_query_ids(client: &SyncClient, packet: &[u8]) -> Vec<u8> {
        let mut pos = client.query_template.prefix.len();
        let (qid_len, n) = read_varint_at(packet, pos);
        let mut out = packet[..pos].to_vec();
        pos += n + qid_len;
        if let Some(ci) = &client.query_template.client_info {
            out.extend_from_slice(&ci.before_initial_query_id);
            pos += ci.before_initial_query_id.len();
            let (qid_len, n) = read_varint_at(packet, pos);
            pos += n + qid_len;
            out.extend_from_slice(&ci.after_initial_query_id);
            pos += ci.after_initial_query_id.len();
        }
        out.extend_from_slice(&packet[pos..]);
        out
    }

    #[test]
    fn query_packet_settings_overlay_merges_with_precedence() {
        let client = offline_client(
            &[("max_threads", "4"), ("max_block_size", "1000")],
            revision::DEFAULT_PROTOCOL_REVISION,
        );
        let mut overlay = HashMap::new();
        overlay.insert("max_threads".to_string(), "9".to_string());
        overlay.insert("max_insert_block_size".to_string(), "500".to_string());
        let pkt = client.build_query_packet_with_settings("SELECT 1", &overlay);

        assert_eq!(pkt[0], 1, "ClientCode::Query");
        let (entries, _) = packet_settings(&client, &pkt);
        let by_name: HashMap<_, _> = entries.iter().cloned().collect();
        assert_eq!(
            by_name.get("max_threads").map(String::as_str),
            Some("9"),
            "overlay must win on duplicate keys"
        );
        assert_eq!(
            by_name.get("max_block_size").map(String::as_str),
            Some("1000"),
            "unshadowed baseline must survive"
        );
        assert_eq!(
            by_name.get("max_insert_block_size").map(String::as_str),
            Some("500"),
            "overlay-only keys must be added"
        );
        assert_eq!(
            entries.iter().filter(|(n, _)| n == "max_threads").count(),
            1,
            "duplicate keys must be emitted exactly once"
        );
        // Automatic defaults are still serialized.
        assert!(by_name.contains_key(
            crate::sync::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING
        ));
        assert!(by_name.contains_key(
            crate::sync::protocol::settings::RATIO_OF_DEFAULTS_FOR_SPARSE_SERIALIZATION
        ));
    }

    #[test]
    fn query_packet_settings_overlay_does_not_mutate_state() {
        let client = offline_client(&[("max_threads", "4")], revision::DEFAULT_PROTOCOL_REVISION);
        let template_before = client.query_template.before_query.clone();
        let mut overlay = HashMap::new();
        overlay.insert("max_threads".to_string(), "9".to_string());
        let _ = client.build_query_packet_with_settings("SELECT 1", &overlay);

        assert_eq!(
            client
                .config
                .settings
                .get("max_threads")
                .map(String::as_str),
            Some("4"),
            "config settings must not be mutated"
        );
        assert_eq!(
            client.query_template.before_query, template_before,
            "cached template must not be mutated"
        );
        let (entries, _) = packet_settings(&client, &client.build_query_packet("SELECT 1"));
        assert!(
            entries.iter().any(|(n, v)| n == "max_threads" && v == "4"),
            "next packet must carry the baseline: {entries:?}"
        );
        assert!(
            !entries.iter().any(|(n, v)| n == "max_threads" && v == "9"),
            "overlay must not leak into later packets: {entries:?}"
        );
    }

    #[test]
    fn query_packet_empty_overlay_equals_fast_path() {
        for rev in [
            revision::MIN_SUPPORTED_PROTOCOL_REVISION,
            revision::DEFAULT_PROTOCOL_REVISION,
        ] {
            let client = offline_client(&[("max_threads", "4"), ("x", "y")], rev);
            let fast = client.build_query_packet("SELECT 42");
            let overlay = client.build_query_packet_with_settings("SELECT 42", &HashMap::new());
            assert_eq!(
                strip_query_ids(&client, &fast),
                strip_query_ids(&client, &overlay),
                "empty overlay must keep the cached-template fast path at rev {rev}"
            );
        }
    }

    #[test]
    fn query_packet_overlay_preserves_params_and_framing() {
        let client = offline_client(&[("max_threads", "4")], revision::DEFAULT_PROTOCOL_REVISION);
        let params = vec![
            QueryParameter::new("id", "42"),
            QueryParameter::new("name", "o'brien"),
        ];
        let query = "SELECT {id:UInt64} AS i, {name:String} AS n";
        let mut overlay = HashMap::new();
        overlay.insert("max_threads".to_string(), "9".to_string());
        let pkt = client.build_query_packet_with_params_and_settings(query, &params, &overlay);

        // Deterministic tail: template post-settings bytes + query text +
        // parameters (CUSTOM flag framing) + trailing empty Data block.
        let mut empty_data_block = Vec::new();
        write_empty_data_block_to(&mut empty_data_block);
        let mut expected_tail = Vec::new();
        expected_tail.extend_from_slice(
            &client.query_template.before_query[client.query_template.settings_len..],
        );
        wire::write_string_to_vec(&mut expected_tail, query);
        write_query_parameters_to_vec(&mut expected_tail, &params);
        expected_tail.extend_from_slice(&empty_data_block);
        assert!(
            pkt.ends_with(&expected_tail),
            "overlay packet must preserve query/parameter/suffix framing"
        );

        // The settings block must end exactly where the deterministic tail
        // begins — well-formed and exactly sized.
        let (entries, settings_end) = packet_settings(&client, &pkt);
        assert_eq!(
            settings_end,
            pkt.len() - expected_tail.len(),
            "settings block must end exactly at the framing tail boundary"
        );
        assert!(entries.iter().any(|(n, v)| n == "max_threads" && v == "9"));
    }
}
