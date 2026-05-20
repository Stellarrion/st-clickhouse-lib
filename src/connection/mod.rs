//! Connection management — Client, TCP/TLS, pool, and query execution.
//!
//! SELECT queries extract the `TcpStream` from the pool and hand it to a
//! short-lived tokio task. No mutexes on the stream — the task owns it
//! exclusively until all data is read.
//!
//! Batch queries (`Client::batch()`) send multiple SELECT statement packets
//! in a single `write()` call, saving round-trips for chatty workloads.
//! Only usable via explicit `.batch()` — never implicit.

pub mod batch;
pub(crate) mod block_reader;
pub(crate) mod block_stream;
pub(crate) mod callbacks;
pub(crate) mod commands;
pub(crate) mod config;
pub(crate) mod connect;
pub(crate) mod insert_session;
pub(crate) mod io;
pub(crate) mod metadata;
pub(crate) mod ops;
pub(crate) mod query_builder;
pub(crate) mod query_packet;
pub(crate) mod query_result;
pub(crate) mod raw_block_reader;
pub(crate) mod response_wait;
pub(crate) mod row_stream_reader;
pub(crate) mod select_response;
pub(crate) mod server_packets;
mod tcp;
#[cfg(test)]
mod tcp_tests;

pub use block_stream::BlockStream;
pub use callbacks::{Profile, Progress, QueryCallbacks};
pub use insert_session::InsertSession;
pub use query_builder::QueryBuilder;
pub use query_result::{QueryResult, RawBlocks, RowCount, Scalar};
pub use tcp::{Client, TokioClient};
