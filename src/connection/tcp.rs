//! ClickHouse native TCP protocol client.
//!
//! For SELECT queries, the `TcpStream` is extracted from the pool via
//! `PoolGuard::take_stream()` and moved into a short-lived tokio task
//! that reads all response blocks. The pool slot reconnects on next use.
//! For EXECUTE/CANCEL/INSERT, the stream is used directly via `stream_mut()`.

use crate::compression::CompressionMethod;
use crate::connection::callbacks::QueryCallbacks;
use crate::connection::query_packet::QueryPacketTemplate;
use crate::runtime::sync::RwLock;
use crate::schema::TableSchema;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// ═══════════════════════════════════════════════
// Client
// ═══════════════════════════════════════════════

/// A ClickHouse native protocol client backed by a connection pool.
pub struct Client {
    pub(crate) pool: crate::pool::SimplePool,
    pub(crate) settings: HashMap<String, String>,
    pub(crate) query_template: QueryPacketTemplate,
    pub(crate) compression: Option<CompressionMethod>,
    pub(crate) ping_before_query: bool,
    pub callbacks: QueryCallbacks,
    pub(crate) send_retries: u32,
    pub(crate) retry_timeout: Duration,
    pub(crate) connect_timeout: Duration,
    pub(crate) recv_timeout: Duration,
    pub(crate) schema_cache: Arc<RwLock<HashMap<String, TableSchema>>>,
    pub(crate) validate_schema: bool,
}

/// Explicit name for the default async client implementation.
///
/// `Client` remains the ergonomic public type. This alias documents that the
/// current async transport is backed by the crate's Tokio runtime engine.
pub type TokioClient = Client;

// ═══════════════════════════════════════════════
// BlockStream — interactive SELECT (BeginSelect pattern)
// ═══════════════════════════════════════════════

// ═══════════════════════════════════════════════
// Drop implementations
// ═══════════════════════════════════════════════

impl Drop for Client {
    fn drop(&mut self) {
        // The pool handles connection cleanup in SimplePool::drop().
        // Active BlockStream instances should be cancelled by the user.
    }
}
