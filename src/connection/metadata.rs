use crate::connection::io::{read_exception, read_tables_status_response, read_varint_async};
use crate::connection::server_packets::unsupported_server_packet;
use crate::connection::tcp::Client;
use crate::error::Result;
use crate::protocol::table_status::{QualifiedTableName, TableStatus, TablesStatusResponse};
use crate::runtime::io::AsyncWriteExt;
use crate::schema::{TableColumn, TableSchema, quote_identifier_path};

impl Client {
    /// Request ClickHouse replication/read-only status for a set of tables.
    pub async fn tables_status(
        &self, tables: &[QualifiedTableName],
    ) -> Result<TablesStatusResponse> {
        let mut guard = self.pool.get().await?;
        let rev = guard.server_info().negotiated_revision;
        let pkt = crate::protocol::table_status::build_tables_status_request(tables, rev)?;
        let stream = guard.stream_mut();
        stream.write_packet(&pkt).await?;
        stream.flush().await?;

        loop {
            let typ =
                match crate::runtime::time::timeout(self.recv_timeout, read_varint_async(stream))
                    .await
                {
                    Ok(result) => result?,
                    Err(_) => {
                        return Err(crate::error::Error::Timeout(format!(
                            "timeout waiting for TablesStatusResponse after {:?}",
                            self.recv_timeout
                        )));
                    },
                };
            match typ {
                2 => return Err(read_exception(stream).await?),
                4 => continue,
                9 => return read_tables_status_response(stream, rev).await,
                _ => return Err(unsupported_server_packet(stream, typ).await?),
            }
        }
    }

    /// Request status for one table. Missing tables return `Ok(None)`.
    pub async fn table_status(&self, database: &str, table: &str) -> Result<Option<TableStatus>> {
        let name = QualifiedTableName::new(database, table);
        let response = self.tables_status(std::slice::from_ref(&name)).await?;
        Ok(response.table_states_by_id.get(&name).cloned())
    }

    /// Return cached `DESCRIBE TABLE` metadata, fetching it on first use.
    pub async fn schema_for_table(&self, table: &str) -> Result<TableSchema> {
        if let Some(schema) = self.schema_cache.read().await.get(table).cloned() {
            return Ok(schema);
        }
        let schema = self.fetch_schema_for_table(table).await?;
        self.schema_cache
            .write()
            .await
            .insert(table.to_owned(), schema.clone());
        Ok(schema)
    }

    /// Refresh cached `DESCRIBE TABLE` metadata.
    pub async fn refresh_schema_for_table(&self, table: &str) -> Result<TableSchema> {
        let schema = self.fetch_schema_for_table(table).await?;
        self.schema_cache
            .write()
            .await
            .insert(table.to_owned(), schema.clone());
        Ok(schema)
    }

    pub async fn clear_schema_cache(&self) {
        self.schema_cache.write().await.clear();
    }

    async fn fetch_schema_for_table(&self, table: &str) -> Result<TableSchema> {
        let quoted = quote_identifier_path(table)?;
        let block = self
            .query(&format!("DESCRIBE TABLE {quoted}"))
            .block()
            .await?;
        let names = block.column::<String>("name")?;
        let types = block.column::<String>("type")?;
        let mut columns = Vec::with_capacity(block.row_count());
        for row in 0..block.row_count() {
            columns.push(TableColumn {
                name: names.get_string(row)?,
                type_name: types.get_string(row)?,
            });
        }
        Ok(TableSchema { columns })
    }
}
