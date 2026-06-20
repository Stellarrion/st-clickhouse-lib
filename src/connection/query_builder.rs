use crate::compression::CompressionMethod;
use crate::connection::callbacks::QueryCallbacks;
use crate::connection::io::{compression_flag, ping_stream};
use crate::connection::query_packet::{
    build_query_packet, build_query_packet_template, merge_materialized_settings, query_id_bytes,
};
use crate::connection::query_result::QueryResult;
use crate::connection::row_stream_reader::read_query_blocks;
use crate::connection::select_response::{
    AllRowsHandler, FirstBlockHandler, RawBlocksHandler, RowCountHandler, read_select_response,
};
use crate::connection::server_packets::write_ignored_part_uuids_if_any;
use crate::connection::tcp::Client;
use crate::error::Result;
use crate::metrics::QueryMetricGuard;
use crate::protocol::block::{Block, RawBlock};
use crate::protocol::packet::ClientPacket;
use crate::protocol::parameters::QueryParameter;
use crate::runtime::io::AsyncWriteExt;
use crate::runtime::sync::mpsc;
use std::collections::HashMap;

impl Client {
    /// Start building a SELECT query.
    pub fn query(&self, sql: &str) -> QueryBuilder<'_> {
        QueryBuilder {
            client: self,
            sql: sql.to_owned(),
            settings: HashMap::new(),
            compression: self.compression,
            callbacks: QueryCallbacks::default(),
            query_id: None,
            params: Vec::new(),
            external_tables: Vec::new(),
            ignored_part_uuids: Vec::new(),
            tracing_context: None,
            timeout: None,
        }
    }

    /// Execute a SELECT and return raw native block bodies.
    pub async fn query_raw(&self, sql: &str) -> Result<Vec<RawBlock>> {
        self.query(sql).raw().await
    }
}

pub struct QueryBuilder<'a> {
    client: &'a Client,
    sql: String,
    settings: HashMap<String, String>,
    compression: Option<CompressionMethod>,
    pub callbacks: QueryCallbacks,
    query_id: Option<String>,
    params: Vec<QueryParameter>,
    external_tables: Vec<(String, Block)>,
    ignored_part_uuids: Vec<[u8; 16]>,
    tracing_context: Option<crate::client_info::TracingContext>,
    timeout: Option<std::time::Duration>,
}

enum QuerySettingsMode {
    Materialized,
    RawCapture,
}

impl<'a> QueryBuilder<'a> {
    /// Execute the query and decode it as the requested result shape.
    ///
    /// Examples:
    /// - `fetch::<Vec<MyRow>>()` for all rows
    /// - `fetch::<MyRow>()` for exactly one row
    /// - `fetch::<Option<MyRow>>()` for zero or one row
    /// - `fetch::<Scalar<u64>>()` for one scalar value
    /// - `fetch::<Block>()` for the first data block
    /// - `fetch::<RawBlocks>()` for native block payloads
    /// - `fetch::<RowCount>()` for count-only scans
    pub async fn fetch<T: QueryResult>(self) -> Result<T> {
        T::fetch_from(self).await
    }

    pub fn with_setting(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.insert(name.into(), value.into());
        self
    }

    /// Control Native JSON serialization for this query.
    ///
    /// Enabled by default for materialized reads. Pass `false` to request native
    /// JSON/Object serialization from ClickHouse.
    pub fn with_native_json_as_string(self, enabled: bool) -> Self {
        self.with_setting(
            crate::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING,
            if enabled { "1" } else { "0" },
        )
    }

    pub fn with_compression(mut self, method: CompressionMethod) -> Self {
        self.compression = Some(method);
        self
    }

    /// Set query callbacks.
    pub fn with_callbacks(mut self, cb: QueryCallbacks) -> Self {
        self.callbacks = cb;
        self
    }

    /// Run a callback when the server sends a TimezoneUpdate packet.
    pub fn on_timezone_update<F>(mut self, cb: F) -> Self
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.callbacks.on_timezone_update = Some(Box::new(cb));
        self
    }

    /// Run a callback when the server sends unique part UUIDs.
    pub fn on_part_uuids<F>(mut self, cb: F) -> Self
    where
        F: Fn(&[[u8; 16]]) + Send + Sync + 'static,
    {
        self.callbacks.on_part_uuids = Some(Box::new(cb));
        self
    }

    /// Set a custom query ID (shown in system.query_log).
    pub fn with_query_id(mut self, id: &str) -> Self {
        self.query_id = Some(id.to_owned());
        self
    }

    /// Set a per-query wall-clock timeout that overrides the client-level
    /// [`Client::with_query_timeout`](crate::Client::with_query_timeout).
    /// The deadline starts when the query is first sent.
    pub fn timeout(mut self, t: std::time::Duration) -> Self {
        self.timeout = Some(t);
        self
    }

    /// Bind a server-side query parameter for `{name:Type}` placeholders.
    ///
    /// Values are sent through the native protocol parameter section instead
    /// of substituted into the SQL text.
    pub fn bind(mut self, name: impl Into<String>, value: impl ToString) -> Self {
        self.params
            .push(QueryParameter::new(name.into(), value.to_string()));
        self
    }

    /// Bind a server-side NULL query parameter.
    pub fn bind_null(mut self, name: impl Into<String>) -> Self {
        self.params.push(QueryParameter::null(name));
        self
    }

    /// Add an external table to send alongside the query.
    pub fn with_external_table(mut self, name: &str, block: Block) -> Self {
        self.external_tables.push((name.to_owned(), block));
        self
    }

    /// Ignore specific replicated part UUIDs for this query.
    pub fn with_ignored_part_uuid(mut self, uuid: [u8; 16]) -> Self {
        self.ignored_part_uuids.push(uuid);
        self
    }

    /// Ignore specific replicated part UUIDs for this query.
    pub fn with_ignored_part_uuids<I>(mut self, uuids: I) -> Self
    where
        I: IntoIterator<Item = [u8; 16]>,
    {
        self.ignored_part_uuids.extend(uuids);
        self
    }

    /// Set OpenTelemetry tracing context.
    pub fn with_tracing(mut self, tc: crate::client_info::TracingContext) -> Self {
        self.tracing_context = Some(tc);
        self
    }

    /// Effective whole-query deadline: per-query override else client-level.
    fn effective_deadline(&self) -> Option<crate::runtime::time::Instant> {
        self.timeout
            .or(self.client.query_timeout)
            .map(|t| crate::runtime::time::Instant::now() + t)
    }

    async fn retry<T, F, Fut>(
        &self, deadline: Option<crate::runtime::time::Instant>, mut op: F,
    ) -> Result<T>
    where
        F: FnMut(Option<crate::runtime::time::Instant>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let metric_guard = QueryMetricGuard::new(self.client.metrics(), 1);
        let retries = self.client.send_retries.max(1);
        for attempt in 0..retries {
            match op(deadline).await {
                Ok(value) => {
                    metric_guard.succeed();
                    return Ok(value);
                },
                Err(e) if e.is_retryable() && attempt + 1 < retries => {
                    // A timed-out query under an explicit deadline must not be
                    // re-run (it would just time out again, server-side). With
                    // no deadline configured, behavior is unchanged.
                    if deadline.is_some() && e.is_timeout() {
                        return Err(e);
                    }
                    metric_guard.retry();
                    let base_ms = self.client.retry_timeout.as_millis() as u64;
                    let delay = base_ms.saturating_mul(1u64 << attempt);
                    let jitter = delay / 4;
                    let actual = delay.saturating_add(jitter).max(1);
                    crate::runtime::time::sleep(std::time::Duration::from_millis(actual)).await;
                },
                Err(e) => return Err(e),
            }
        }
        unreachable!()
    }

    async fn send_select_query(
        &self, mode: QuerySettingsMode,
    ) -> Result<(crate::pool::PoolGuard<'_>, bool)> {
        let mut guard = self.client.pool.get().await?;
        let rev = guard.server_info().negotiated_revision;
        let stream = guard.stream_mut();
        if self.client.ping_before_query {
            ping_stream(stream).await?;
        }
        let mut query_id_buf = [0u8; 22];
        let query_id = query_id_bytes(self.query_id.as_deref(), &mut query_id_buf);
        let compression = self.compression.or(self.client.compression);
        let response_compressed = compression_flag(compression) == 1;
        let settings = match mode {
            QuerySettingsMode::Materialized => {
                merge_materialized_settings(&self.client.settings, &self.settings)
            },
            QuerySettingsMode::RawCapture => {
                let mut settings =
                    merge_materialized_settings(&self.client.settings, &self.settings);
                settings
                    .entry(
                        crate::protocol::settings::OUTPUT_FORMAT_NATIVE_USE_FLATTENED_DYNAMIC_AND_JSON_SERIALIZATION
                            .into(),
                    )
                    .or_insert_with(|| "1".into());
                settings
                    .entry(
                        crate::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING.into(),
                    )
                    .or_insert_with(|| "0".into());
                settings
            },
        };
        let template = build_query_packet_template(&settings, compression, rev);
        let pkt = build_query_packet(
            &template,
            &self.sql,
            &self.external_tables,
            query_id,
            &self.params,
        );
        write_ignored_part_uuids_if_any(stream, &self.ignored_part_uuids).await?;
        stream.write_packet(&pkt).await?;
        stream.flush().await?;
        Ok((guard, response_compressed))
    }

    /// Fetch the first result block with retries on retryable errors.
    /// Reads until EoS on `stream_mut()` — connection stays clean.
    pub async fn block(self) -> Result<Block> {
        let deadline = self.effective_deadline();
        self.retry(deadline, |dl| self._try_block(dl)).await
    }

    /// Internal block read — called in retry loop.
    async fn _try_block(&self, deadline: Option<crate::runtime::time::Instant>) -> Result<Block> {
        let (mut guard, response_compressed) = self
            .send_select_query(QuerySettingsMode::Materialized)
            .await?;
        read_select_response(
            guard.stream_mut(),
            self.client.recv_timeout,
            deadline,
            response_compressed,
            &self.callbacks,
            FirstBlockHandler::default(),
        )
        .await
    }

    /// Stream rows via a background task that owns the TcpStream.
    pub async fn rows<T: crate::row::Row>(self) -> Result<crate::cursor::RowCursor<T>> {
        let metric_guard = QueryMetricGuard::new(self.client.metrics(), 1);
        let (mut guard, _) = self
            .send_select_query(QuerySettingsMode::Materialized)
            .await?;

        // Take the stream — pool slot goes empty, will reconnect on next use
        let stream = guard.take_stream().ok_or_else(|| {
            crate::error::Error::Protocol("connection stream already taken".into())
        })?;

        // Drop the guard (releases the slot) — the pool slot is now None
        drop(guard);

        // Cancel signal shared with the background task
        let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let deadline = self.effective_deadline();
        let recv_timeout = self.client.recv_timeout;

        // Spawn a short-lived task that owns the stream and reads all blocks
        let (block_tx, block_rx) = mpsc::channel(4);
        let cancel_clone = cancel.clone();
        crate::runtime::spawn(async move {
            let mut stream = stream;
            if cancel_clone.load(std::sync::atomic::Ordering::Relaxed) {
                stream
                    .write_packet(&[ClientPacket::Cancel as u8])
                    .await
                    .ok();
                stream.flush().await.ok();
                return;
            }
            if let Err(e) = read_query_blocks(
                stream,
                &block_tx,
                &self.callbacks,
                Some(&cancel_clone),
                recv_timeout,
                deadline,
            )
            .await
            {
                let _ = block_tx.send(Err(e)).await;
            }
        });

        metric_guard.succeed();
        Ok(crate::cursor::RowCursor::new(block_rx, cancel))
    }

    /// Fetch all rows into a Vec with retries on retryable errors.
    pub async fn all<T: crate::row::Row>(self) -> Result<Vec<T>> {
        let deadline = self.effective_deadline();
        self.retry(deadline, |dl| self._try_all::<T>(dl)).await
    }

    /// Fetch exactly one row.
    pub async fn one<T: crate::row::Row>(self) -> Result<T> {
        match self.optional::<T>().await? {
            Some(row) => Ok(row),
            None => Err(crate::error::Error::Protocol(
                "expected one row, got zero rows".into(),
            )),
        }
    }

    /// Fetch zero or one row.
    pub async fn optional<T: crate::row::Row>(self) -> Result<Option<T>> {
        let mut rows = self.all::<T>().await?;
        match rows.len() {
            0 => Ok(None),
            1 => Ok(rows.pop()),
            n => Err(crate::error::Error::Protocol(format!(
                "expected zero or one row, got {n} rows"
            ))),
        }
    }

    /// Fetch exactly one scalar value from the first column.
    pub async fn scalar<T>(self) -> Result<T>
    where
        T: crate::column::ClickHouseColumn + 'static,
    {
        let (value,) = self.one::<(T,)>().await?;
        Ok(value)
    }

    /// Count response rows without materializing result blocks.
    pub async fn row_count(self) -> Result<usize> {
        let deadline = self.effective_deadline();
        self.retry(deadline, |dl| self._try_row_count(dl)).await
    }

    async fn _try_row_count(
        &self, deadline: Option<crate::runtime::time::Instant>,
    ) -> Result<usize> {
        let (mut guard, response_compressed) = self
            .send_select_query(QuerySettingsMode::Materialized)
            .await?;
        read_select_response(
            guard.stream_mut(),
            self.client.recv_timeout,
            deadline,
            response_compressed,
            &self.callbacks,
            RowCountHandler::default(),
        )
        .await
    }

    /// Internal all-rows read — called in retry loop.
    async fn _try_all<T: crate::row::Row>(
        &self, deadline: Option<crate::runtime::time::Instant>,
    ) -> Result<Vec<T>> {
        let (mut guard, response_compressed) = self
            .send_select_query(QuerySettingsMode::Materialized)
            .await?;
        read_select_response(
            guard.stream_mut(),
            self.client.recv_timeout,
            deadline,
            response_compressed,
            &self.callbacks,
            AllRowsHandler::<T>::default(),
        )
        .await
    }

    /// Fetch raw native block bodies without materializing parsed [`Block`] values.
    pub async fn raw(self) -> Result<Vec<RawBlock>> {
        let metric_guard = QueryMetricGuard::new(self.client.metrics(), 1);
        let deadline = self.effective_deadline();
        let (mut guard, response_compressed) = self
            .send_select_query(QuerySettingsMode::RawCapture)
            .await?;
        let blocks = read_select_response(
            guard.stream_mut(),
            self.client.recv_timeout,
            deadline,
            response_compressed,
            &self.callbacks,
            RawBlocksHandler::default(),
        )
        .await?;
        metric_guard.succeed();
        Ok(blocks)
    }
}
