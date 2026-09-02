//! High-level query builder with parameter binding.
//!
//! Wraps [`Client`] and adds SQL helpers for server-side
//! `{name:Type}` placeholders (ClickHouse 54459+).
//!
//! This builder is a thin convenience shim over the richer
//! [`crate::connection::QueryBuilder`] returned by [`Client::query`], which
//! additionally supports settings,
//! compression, callbacks, query IDs, timeouts, external tables, and
//! streaming reads.

use crate::Client;
use crate::error::Result;
use crate::protocol::block::Block;
use crate::protocol::parameters::QueryParameter;

/// A simple SQL query builder with parameter binding.
///
/// # Example
///
/// ```ignore
/// // ignore: needs a connected `Client` (live server); the shim is deprecated
/// let block = QueryBuilder::new(&client)
///     .query("SELECT * FROM users WHERE id = {uid:UInt64}")
///     .bind("uid", "42")
///     .block()
///     .await?;
/// ```
#[deprecated(
    since = "0.3.0",
    note = "thin shim over the richer builder returned by Client::query; use st_clickhouse::connection::QueryBuilder (settings, timeouts, callbacks, streaming) or Client::execute_with_params for parameterized DDL"
)]
pub struct QueryBuilder<'a> {
    client: &'a Client,
    sql: String,
    params: Vec<QueryParameter>,
}

// The shim's own methods reference the deprecated struct's fields; the
// deprecation is aimed at external users, not at this delegation code.
#[allow(deprecated)]
impl<'a> QueryBuilder<'a> {
    /// Create a new empty query builder for the given client.
    pub fn new(client: &'a Client) -> Self {
        Self {
            client,
            sql: String::new(),
            params: Vec::new(),
        }
    }

    /// Set the SQL query text.
    ///
    /// Use `{name:Type}` for server-side parameters (ClickHouse 54459+).
    /// Bind values with [`bind`](Self::bind).
    pub fn query(mut self, sql: &str) -> Self {
        self.sql = sql.to_owned();
        self
    }

    /// Bind a parameter value.
    ///
    /// The name must correspond to a `{name:Type}` placeholder in the SQL.
    /// Values are sent through the native protocol parameter section.
    pub fn bind(mut self, name: &str, value: impl ToString) -> Self {
        self.params
            .push(QueryParameter::new(name.to_owned(), value.to_string()));
        self
    }

    /// Bind a server-side NULL value.
    pub fn bind_null(mut self, name: &str) -> Self {
        self.params.push(QueryParameter::null(name.to_owned()));
        self
    }

    /// Execute a DDL/DML statement (no result rows returned).
    ///
    pub async fn execute(self) -> Result<()> {
        self.client
            .execute_with_params(&self.sql, &self.params)
            .await
    }

    /// Execute a SELECT that must return exactly one non-empty data block.
    ///
    /// Returns an error instead of silently dropping rows if the server emits
    /// more than one block. Use [`blocks`](Self::blocks) for general results.
    pub async fn block(self) -> Result<Block> {
        self.into_connection_query().block().await
    }

    /// Execute a SELECT and return every non-empty server block.
    pub async fn blocks(self) -> Result<Vec<Block>> {
        self.into_connection_query().blocks().await
    }

    fn into_connection_query(self) -> crate::connection::QueryBuilder<'a> {
        let mut query = self.client.query(&self.sql);
        for param in self.params {
            query = match param.value {
                Some(value) => query.bind(param.name, value),
                None => query.bind_null(param.name),
            };
        }
        query
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(deprecated)]
    fn test_query_builder_bind_chaining() {
        // Test that the builder API compiles and chains correctly.
        // We can't construct a real Client here, but the type system
        // test verifies the API shape.
        let builder: Option<QueryBuilder<'_>> = None;
        // These lines must compile:
        let _ = builder.map(|qb| qb.query("SELECT 1").bind("x", 1).bind_null("y"));
    }
}
