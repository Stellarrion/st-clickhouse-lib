use crate::connection::io::{compression_flag, ping_stream};
use crate::connection::query_packet::build_query_packet_from_cached_or_revision;
use crate::connection::response_wait::drain_response;
use crate::connection::server_packets::write_ignored_part_uuids_if_any;
use crate::connection::tcp::Client;
use crate::error::Result;
use crate::metrics::QueryMetricGuard;
use crate::protocol::parameters::QueryParameter;
use crate::query_id::next_query_id;
use crate::runtime::io::AsyncWriteExt;
use crate::schema::query_may_change_schema;
use tracing::{Instrument, info_span};

impl Client {
    /// Execute a DDL/DML (no result rows). Uses the stream directly.
    ///
    /// Retries with exponential backoff + jitter on retryable errors
    /// (connection issues, timeouts). Non-retryable errors (server exceptions,
    /// authentication failures, config errors) are returned immediately.
    pub async fn execute(&self, query: &str) -> Result<()> {
        self.execute_with_params(query, &[]).await
    }

    /// Execute a DDL/DML statement with server-side query parameters.
    pub async fn execute_with_params(&self, query: &str, params: &[QueryParameter]) -> Result<()> {
        self.execute_with_params_and_ignored_part_uuids(query, params, &[])
            .await
    }

    /// Execute a DDL/DML while ignoring replicated parts for this query.
    pub async fn execute_with_ignored_part_uuids(
        &self, query: &str, uuids: &[[u8; 16]],
    ) -> Result<()> {
        self.execute_with_params_and_ignored_part_uuids(query, &[], uuids)
            .await
    }

    /// Execute a DDL/DML with parameters while ignoring replicated parts.
    pub async fn execute_with_params_and_ignored_part_uuids(
        &self, query: &str, params: &[QueryParameter], uuids: &[[u8; 16]],
    ) -> Result<()> {
        let deadline = self
            .query_timeout
            .map(|t| crate::runtime::time::Instant::now() + t);
        let span = info_span!("execute", query = %query, retries = self.send_retries);
        async {
            let metric_guard = QueryMetricGuard::new(self.metrics(), 1);
            let retries = self.send_retries.max(1);
            for attempt in 0..retries {
                match self
                    ._execute_with_params_and_ignored_part_uuids(query, params, uuids, deadline)
                    .await
                {
                    Ok(r) => {
                        metric_guard.succeed();
                        return Ok(r);
                    },
                    Err(e) => {
                        if !e.is_retryable()
                            || attempt + 1 >= retries
                            || (deadline.is_some() && e.is_timeout())
                        {
                            return Err(e);
                        }
                        metric_guard.retry();
                        // Exponential backoff: base * 2^attempt + jitter (+25%)
                        let base_ms = self.retry_timeout.as_millis() as u64;
                        let delay = base_ms.saturating_mul(1u64 << attempt);
                        let jitter_range = delay / 4;
                        let jitter = if jitter_range > 0 {
                            (std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .subsec_nanos() as u64)
                                % (jitter_range * 2 + 1).saturating_sub(jitter_range)
                        } else {
                            0
                        };
                        let actual = delay.saturating_add(jitter).max(1);
                        crate::runtime::time::sleep(std::time::Duration::from_millis(actual)).await;
                    },
                }
            }
            unreachable!()
        }
        .instrument(span)
        .await
    }

    async fn _execute_with_params_and_ignored_part_uuids(
        &self, query: &str, params: &[QueryParameter], uuids: &[[u8; 16]],
        deadline: Option<crate::runtime::time::Instant>,
    ) -> Result<()> {
        let mut guard = self.pool.get().await?;
        let rev = guard.server_info().negotiated_revision;
        let mut query_id_buf = [0u8; 22];
        let query_id_len = next_query_id(&mut query_id_buf);
        let query_id = &query_id_buf[..query_id_len];
        let pkt = build_query_packet_from_cached_or_revision(
            &self.query_template,
            &self.settings,
            rev,
            query,
            query_id,
            true,
            params,
        );
        // Mark in-flight before the first packet that can leave a response
        // pending (the optional pre-query Ping; otherwise the query write): a
        // future dropped before the terminal packet must not return a
        // mid-response socket to the pool.
        guard.mark_response_in_flight();
        {
            let stream = guard.stream_mut();
            if self.ping_before_query {
                ping_stream(stream).await?;
            }
            write_ignored_part_uuids_if_any(stream, uuids).await?;
        }
        let result = {
            let stream = guard.stream_mut();
            stream.write_packet(&pkt).await?;
            stream.flush().await?;
            drain_response(
                stream,
                self.recv_timeout,
                compression_flag(self.compression) == 1,
                deadline,
            )
            .await
        };
        guard.finish_response(&result);
        result?;
        if query_may_change_schema(query) {
            self.clear_schema_cache().await;
        }
        Ok(())
    }

    /// Cancel the running query.
    ///
    /// **This method never cancels anything and always returns
    /// [`Error::Config`](crate::error::Error::Config).** A [`Client`] owns a
    /// connection pool, not the connection running your query: `cancel()` used
    /// to grab an arbitrary idle pooled connection and send `Cancel` there.
    /// The stray packet was silently swallowed by the server (any query
    /// routing), and with a busy single-slot pool `cancel()` blocked until the
    /// query finished anyway — a false-success API. It now fails closed and
    /// touches no connection at all.
    ///
    /// To stop a query, use one of these instead:
    /// - a query deadline — [`Client::with_query_timeout`] or
    ///   [`crate::query::QueryBuilder::timeout`] — which cancels server-side
    ///   and bounds the drain;
    /// - [`crate::BlockStream::cancel`] on the stream returned by
    ///   [`Client::begin_select`];
    /// - dropping the [`crate::cursor::RowCursor`] returned by
    ///   [`crate::query::QueryBuilder::rows`] — its detached reader task sends
    ///   `Cancel` and owns its socket.
    #[deprecated(
        since = "0.2.0",
        note = "Client::cancel cannot reach the connection running the query and always returns Error::Config; use a query timeout, BlockStream::cancel, or drop the RowCursor"
    )]
    pub async fn cancel(&self) -> Result<()> {
        Err(crate::error::Error::Config(
            concat!(
                "Client::cancel cannot cancel a query: the Client owns a pool, not the ",
                "connection running the query, so no connection is touched. Use a query ",
                "timeout (Client::with_query_timeout / QueryBuilder::timeout), ",
                "BlockStream::cancel on a begin_select stream, or drop the RowCursor ",
                "returned by QueryBuilder::rows"
            )
            .into(),
        ))
    }
}
