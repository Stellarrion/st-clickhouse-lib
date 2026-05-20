# Changelog

All notable changes to st-clickhouse are documented here.

## [0.1.0] — 2026-05-18

### Added
- **Error types**: `ServerError { code, name, message }`, `Timeout`, `ConnectionClosed`, `Config` variants
- **Convenience methods**: `is_timeout()`, `is_server_error()`, `is_retryable()` on `Error`
- **Exponential backoff with jitter** for retries (base × 2^attempt ±25%)
- **Credential support**: `Client::with_credentials(user, pass)`, `Client::with_user(user)`
- **TLS infrastructure**: `StreamWrapper` enum (`Tcp`/`Tls`) with `AsyncRead` + `AsyncWrite` impls
- **Graceful shutdown**: `Drop` impls for `Client`, `BlockStream`, `SimplePool`
- **CI pipeline**: GitHub Actions (check, lint, unit/integration tests, feature matrix, coverage)
- **Testcontainers**: All integration tests use `tests/common/mod.rs` to start ClickHouse dynamically
- **Comprehensive test suite**: 19 files, 132 tests (was 11 files, ~45 tests)

### Fixed
- **TTL bug**: `with_ttl()` now actually recycles expired connections (per-connection `created_at`)
- **Connect timeout**: `with_connect_timeout()` now wraps TCP connect with `tokio::time::timeout`
- **Server error detection**: `drain_response` now propagates server exceptions via `Error::ServerError`
- **`write_empty_data_marker`**: Fixed broken BlockInfo encoding → proper clickhouse-arrow format

### Changed
- **`tests/cpp_compat_test.rs`** renamed to `tests/compat_test.rs`
- **Connection.stream** type changed from `tokio::net::TcpStream` to `crate::pool::StreamWrapper`
- **Batch module I/O functions** generified to `S: AsyncRead + AsyncWrite + Unpin`

## [0.1.0] — Initial release

- ClickHouse native TCP protocol client
- Connection pool with semaphore-based concurrency control
- Columnar zero-copy reads
- Row-level streaming via `RowCursor`
- INSERT via `begin_insert` / `send_data` / `end`
- Batch query pipelining
- LZ4 / ZSTD compression
- Query callbacks (progress, profile, logs)
- Type system: UInt*, Int*, Float*, String, FixedString, Date, DateTime, DateTime64,
  Decimal32/64/128, UUID, IPv4, IPv6, Nullable, Array, Map, LowCardinality, Enum, Tuple, Bool,
  Point, Ring, Polygon, MultiPolygon (geo types)
