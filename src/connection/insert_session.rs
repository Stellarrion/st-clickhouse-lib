use crate::compression::CompressionMethod;
use crate::connection::io::{compression_flag, ping_stream};
use crate::connection::query_packet::{build_query_packet_from_cached_or_revision, next_query_id};
use crate::connection::response_wait::{drain_response, read_table_structure};
use crate::connection::tcp::Client;
use crate::error::Result;
use crate::metrics::QueryMetricGuard;
use crate::protocol::block::Block;
use crate::runtime::io::AsyncWriteExt;
use crate::schema::{TableSchema, quote_identifier_path};
use std::time::Duration;

pub struct InsertSession<'a> {
    guard: crate::pool::PoolGuard<'a>,
    block: Option<Block>,
    active: bool,
    recv_timeout: Duration,
    deadline: Option<crate::runtime::time::Instant>,
    compression: Option<CompressionMethod>,
    table_name: String,
    schema: Option<TableSchema>,
}

impl Client {
    /// Begin an INSERT. Uses the stream directly.
    pub async fn begin_insert(&self, table: &str) -> Result<InsertSession<'_>> {
        let metric_guard = QueryMetricGuard::new(self.metrics(), 1);
        let schema = if self.validate_schema {
            Some(self.schema_for_table(table).await?)
        } else {
            None
        };
        let quoted = quote_identifier_path(table)?;
        let query = format!("INSERT INTO {quoted} FORMAT Native");
        let mut guard = self.pool.get().await?;
        let rev = guard.server_info().negotiated_revision;
        let mut query_id_buf = [0u8; 22];
        let query_id_len = next_query_id(&mut query_id_buf);
        let query_id = &query_id_buf[..query_id_len];
        let pkt = build_query_packet_from_cached_or_revision(
            &self.query_template,
            &self.settings,
            rev,
            &query,
            query_id,
            true,
            &[],
        );
        let stream = guard.stream_mut();
        if self.ping_before_query {
            ping_stream(stream).await?;
        }
        stream.write_packet(&pkt).await?;
        stream.flush().await?;
        let response_compressed = compression_flag(self.compression) == 1;
        let deadline = self
            .query_timeout
            .map(|t| crate::runtime::time::Instant::now() + t);
        let block_result =
            read_table_structure(stream, self.recv_timeout, response_compressed, deadline).await;
        guard.invalidate_on_err(&block_result);
        let block = block_result?;
        metric_guard.succeed();
        Ok(InsertSession {
            guard,
            block: Some(block),
            active: true,
            recv_timeout: self.recv_timeout,
            deadline,
            compression: self.compression,
            table_name: table.to_owned(),
            schema,
        })
    }
}

impl Drop for InsertSession<'_> {
    fn drop(&mut self) {
        if self.active {
            // Async cleanup is impossible in Drop. Closing the socket aborts
            // the unfinished INSERT and prevents its pending protocol state
            // from being handed to the next pool user.
            let _ = self.guard.take_stream();
        }
    }
}

impl InsertSession<'_> {
    pub fn table_structure(&self) -> Option<&Block> {
        self.block.as_ref()
    }

    pub async fn send_data(&mut self, block: &Block) -> Result<()> {
        if !self.active {
            return Err(crate::error::Error::Protocol("INSERT session ended".into()));
        }
        if let Some(schema) = &self.schema {
            schema.validate_insert_block(&self.table_name, block)?;
        }
        use crate::protocol::block_writer;
        let stream = self.guard.stream_mut();
        let mut buf = Vec::with_capacity(block_writer::data_packet_capacity("", block));
        match self.compression {
            Some(method @ (CompressionMethod::Lz4 | CompressionMethod::Zstd)) => {
                block_writer::write_data_packet_compressed(&mut buf, "", block, method)?;
            },
            Some(CompressionMethod::None) | None => {
                block_writer::write_data_packet(&mut buf, "", block)?;
            },
        }
        stream.write_packet(&buf).await?;
        stream.flush().await?;
        Ok(())
    }

    pub async fn end(mut self) -> Result<()> {
        use crate::protocol::block_writer;
        let stream = self.guard.stream_mut();
        let empty = Block {
            columns: Vec::new(),
            rows: 0,
        };
        let mut buf = Vec::with_capacity(block_writer::data_packet_capacity("", &empty));
        match self.compression {
            Some(method @ (CompressionMethod::Lz4 | CompressionMethod::Zstd)) => {
                block_writer::write_data_packet_compressed(&mut buf, "", &empty, method)?;
            },
            Some(CompressionMethod::None) | None => {
                block_writer::write_data_packet(&mut buf, "", &empty)?;
            },
        }
        stream.write_packet(&buf).await?;
        stream.flush().await?;
        let result = drain_response(
            stream,
            self.recv_timeout,
            compression_flag(self.compression) == 1,
            self.deadline,
        )
        .await;
        if result.is_ok() {
            self.active = false;
        }
        self.guard.invalidate_on_err(&result);
        result
    }
}
