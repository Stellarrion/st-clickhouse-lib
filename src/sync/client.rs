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
    /// Handles handshake, addendum, ping/pong, and sets read timeout
    /// from `config.query_timeout`.
    pub fn connect_with_config(config: ClientConfig) -> Result<Self> {
        revision::validate_supported_revision(config.client_revision).map_err(Error::Protocol)?;

        // Native connect() produces a clean blocking socket with no
        // non-blocking flags left over from connect_timeout.
        let addr = config
            .addr()
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| Error::Protocol("no address resolved".into()))?;
        let stream = TcpStream::connect(addr)?;
        Self::connect_stream(stream, config)
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

    pub fn connect_stream(stream: TcpStream, config: ClientConfig) -> Result<Self> {
        let transport = crate::sync::transport::Transport::new_plain(stream);
        Self::connect_transport(transport, config)
    }

    /// Connect using an already-established transport (plain or TLS).
    fn connect_transport(
        transport: crate::sync::transport::Transport, config: ClientConfig,
    ) -> Result<Self> {
        transport.set_nodelay(true)?;
        let _ = transport.set_read_timeout(Some(config.query_timeout));

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

        let mut server_info = handshake::handshake(&mut transport, &config)?;

        if server_info.negotiated_revision >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM {
            let chunked = negotiate_chunked_transport(&server_info, &config)?;
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

        let query_template = build_query_packet_template(&config, server_info.negotiated_revision);

        Ok(SyncClient {
            stream: transport,
            server_info,
            config,
            query_template,
            schema_cache: HashMap::new(),
        })
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
        self.send_ignored_part_uuids(uuids)?;
        let pkt = self.build_query_packet_with_params(query, params);
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
        self.send_ignored_part_uuids(uuids)?;
        let pkt = self.build_query_packet_with_params(query, params);
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

    pub fn drain_response(&mut self) -> Result<()> {
        let deadline = std::time::Instant::now() + self.config.query_timeout;
        let rev = self.server_info.negotiated_revision;
        let mut blocks = Vec::new();
        let res = if self.server_info.use_chunked_recv {
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
        };
        match res {
            Ok(_) => Ok(()),
            Err(Error::Protocol(_)) => Ok(()), // swallow protocol errors in drain
            Err(e) => Err(e),
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

    fn build_query_packet_inner(
        &self, query: &str, include_empty_block: bool, params: &[QueryParameter], buf: &mut Vec<u8>,
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
        buf.extend_from_slice(&self.query_template.before_query);
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
        self.build_query_packet_inner(query, true, &[], &mut buf);
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
        self.build_query_packet_inner(query, true, params, &mut buf);
        buf
    }

    pub fn build_insert_query_packet(&self, query: &str) -> Vec<u8> {
        let mut buf = take_buf(self.query_template.insert_capacity + query.len() + 80);
        self.build_query_packet_inner(query, false, &[], &mut buf);
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
fn read_exception_packet<R: std::io::Read>(reader: &mut R) -> Error {
    let mut parts = Vec::new();
    loop {
        let code = match wire::read_bytes(reader, 4) {
            Ok(bytes) => {
                let mut code_bytes = [0u8; 4];
                code_bytes.copy_from_slice(&bytes);
                i32::from_le_bytes(code_bytes)
            },
            Err(e) => return e,
        };
        let name = wire::read_string(reader).unwrap_or_else(|_| "unknown".to_string());
        let msg = wire::read_string(reader).unwrap_or_default();
        let _stack = wire::read_string(reader);
        parts.push(format!("{name} (code {code}): {msg}"));
        match wire::read_bytes(reader, 1) {
            Ok(flag) if flag.first().copied().unwrap_or(0) != 0 => {},
            Ok(_) => break,
            Err(e) => return e,
        }
    }
    Error::Protocol(format!("server error: {}", parts.join(" | nested: ")))
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
                    2 => {
                        let code = match wire::parse_i32(&self.buffer, &mut self.pos) {
                            Ok(v) => v,
                            Err(Error::Protocol(_)) => {
                                self.pos = saved_pos;
                                self.fill_buffer()?;
                                continue;
                            },
                            Err(e) => return Err(e),
                        };
                        let name = wire::parse_string(&self.buffer, &mut self.pos)
                            .unwrap_or("unknown")
                            .to_owned();
                        let msg = wire::parse_string(&self.buffer, &mut self.pos)
                            .unwrap_or("")
                            .to_owned();
                        let _ = wire::parse_string(&self.buffer, &mut self.pos);
                        if self.pos < self.buffer.len() {
                            self.pos += 1;
                        }
                        self.done = true;
                        return Err(Error::Protocol(format!(
                            "server error (code={code}, name={name}): {msg}"
                        )));
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
}
