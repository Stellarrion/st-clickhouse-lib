//! Sync ClickHouse native protocol engine.
//!
//! This module is part of the `st-clickhouse-lib` crate so users do not need a
//! separate public core crate. It intentionally keeps the synchronous protocol
//! implementation available for Python bindings, benchmarks, and applications
//! that want to provide their own scheduling/runtime around blocking I/O.

pub(crate) mod chunked;
pub mod client;
pub mod client_info;
pub mod column;
pub mod compression;
pub mod config;
pub mod error;
pub mod protocol;
pub(crate) mod query_packet;
pub mod row;
pub mod schema;
pub mod transport;

pub mod prelude {
    pub use crate::sync::client::SyncClient;
    pub use crate::sync::column::{ClickHouseColumn, ClickHouseColumnData, ClickHouseValue};
    pub use crate::sync::config::ClientConfig;
    pub use crate::sync::error::Result;
    pub use crate::sync::protocol::block::{Block, ColumnInfo};
    pub use crate::sync::protocol::handshake::SshSigner;
    pub use crate::sync::protocol::parameters::QueryParameter;
    pub use crate::sync::protocol::table_status::{
        QualifiedTableName, TableStatus, TablesStatusResponse,
    };
    pub use crate::sync::row::Row;
    pub use crate::sync::schema::{TableColumn, TableSchema};
}

pub use crate::sync::client::SyncClient;
pub use crate::sync::column::{
    BoolColumnData, ClickHouseColumn, ClickHouseColumnData, ClickHouseValue, Date, DateTime,
    DynamicFieldValue, DynamicTypedValue, DynamicValue, Ipv4, Ipv6, JsonValue, Uuid, VariantValue,
};
pub use crate::sync::config::ClientConfig;
pub use crate::sync::error::{Error, Result};
pub use crate::sync::protocol::block::{Block, ColumnInfo};
pub use crate::sync::protocol::handshake::SshSigner;
pub use crate::sync::protocol::parameters::QueryParameter;
pub use crate::sync::protocol::response::parse_response;
pub use crate::sync::protocol::settings;
pub use crate::sync::protocol::table_status::{
    QualifiedTableName, TableStatus, TablesStatusResponse,
};
pub use crate::sync::row::Row;
pub use crate::sync::schema::{TableColumn, TableSchema};
