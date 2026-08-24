pub mod builder;
#[cfg(feature = "tokio")]
pub mod client_info;
pub mod column;
pub mod compression;
#[cfg(feature = "tokio")]
pub mod connection;
#[cfg(feature = "tokio")]
pub mod cursor;
pub mod error;
pub(crate) mod limits;
#[cfg(feature = "tokio")]
pub mod metrics;
#[cfg(feature = "tokio")]
pub(crate) mod pool;
pub mod protocol;
#[cfg(feature = "tokio")]
pub mod query;
pub(crate) mod query_id;
pub mod row;
pub mod runtime;
pub mod schema;
pub mod sync;

pub mod prelude {
    pub use crate::column::{ClickHouseColumn, ClickHouseColumnData, ClickHouseValue};
    pub use crate::compression::CompressionMethod;
    pub use crate::error::Result;
    pub use crate::protocol::block::{Block, ColumnInfo, RawBlock};
    pub use crate::row::Row;
    pub use crate::schema::{TableColumn, TableSchema};

    #[cfg(feature = "tokio")]
    pub use crate::connection::{Client, QueryResult, RawBlocks, RowCount, Scalar};
    #[cfg(feature = "tokio")]
    pub use crate::protocol::handshake::SshSigner;
    #[cfg(feature = "tokio")]
    pub use crate::protocol::parameters::QueryParameter;
    #[cfg(feature = "tokio")]
    pub use crate::protocol::table_status::{
        QualifiedTableName, TableStatus, TablesStatusResponse,
    };
    #[cfg(feature = "tokio")]
    pub use crate::query::QueryBuilder;

    #[cfg(feature = "derive")]
    pub use st_clickhouse_derive::Row;
}

pub use builder::{Async, Blocking, ClientBuilder};
pub use column::{
    BoolColumnData, ClickHouseColumn, ClickHouseColumnData, ClickHouseValue, Date, DateTime,
    DynamicFieldValue, DynamicTypedValue, DynamicValue, Ipv4, Ipv6, JsonValue, Uuid, VariantValue,
};
pub use compression::CompressionMethod;
pub use error::{Error, Result};
pub use protocol::block::{Block, ColumnInfo, RawBlock};
pub use protocol::settings;
pub use row::Row;
pub use schema::{TableColumn, TableSchema};

#[cfg(feature = "tokio")]
pub use client_info::TracingContext;
#[cfg(feature = "tokio")]
pub use connection::{BlockStream, Client, QueryResult, RawBlocks, RowCount, Scalar, TokioClient};
#[cfg(feature = "tokio")]
pub use protocol::handshake::SshSigner;
#[cfg(feature = "tokio")]
pub use protocol::parameters::QueryParameter;
#[cfg(feature = "tokio")]
pub use protocol::table_status::{QualifiedTableName, TableStatus, TablesStatusResponse};
#[cfg(feature = "tokio")]
pub use query::QueryBuilder;

#[cfg(feature = "derive")]
pub use st_clickhouse_derive::Row;
