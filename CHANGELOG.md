# Changelog

All notable changes to st-clickhouse are documented here.

## [Unreleased]

### Added
- **Query timeout**: opt-in hard wall-clock deadline via `Client::with_query_timeout(d)`
  and per-query `QueryBuilder::timeout(d)`. On expiry the query is cancelled server-side
  (`Cancel` packet) and the pooled connection is drained and reused. Default `None` —
  no behaviour change for existing users. Async client only; sync core already
  supported `query_timeout`.
- **Pool acquire timeout**: bound the wait for a free pool slot via
  `Client::with_acquire_timeout(d)` / `ClientBuilder::acquire_timeout(d)` / URL
  `acquire_timeout=`. Returns the retryable `Error::PoolTimeout` when no slot is
  free in time. Default `None` (unbounded — unchanged). Async client only.
  An acquire timeout also bumps the `connection_errors` metric.
- **Quota key**: configurable `quota_key` sent in ClientInfo and the connection
  handshake addendum, via `Client::with_quota_key(s)` /
  `ClientBuilder::quota_key(s)` / URL `?quota_key=`. Parity with the sync core,
  which previously hardcoded `""` on the async path. Default `""` — no wire
  change for existing users. Setting it (or changing it) bumps the pool's config
  generation so pooled connections reconnect carrying the new key. Note: URL
  `?quota_key=` is now the protocol ClientInfo field (previously it fell through
  to the ClickHouse `settings` map).

## [0.2.0] — 2026-06-21

### Changed (BREAKING)
- **`StringColumnData` now borrows the block buffer** (`StringColumnData<'a>`) — true
  zero-copy String column reads. `get_bytes` / `get_str` return `&'a`. Previously the
  column owned a copy of every string body; the read path is now a single varint scan
  over the borrowed buffer (~3.3× faster decode of 100K strings). Mirrors the existing
  `FixedStringColumnData<'a>` shape.

### Performance
- **Row materialization fast path**: `read_all` / `query_all` materialize all-PlainColumn
  tuples by indexing native per-column slices instead of dispatching `to_typed` per row
  (~33% faster: 1M `UInt64` tuples 4.2ms → 2.8ms, to within ~0.5ms of the block floor).
  Mixed tuples (String/Array fields) silently fall back to the per-row path.
- **Zero-copy String column** (see Breaking) + single-pass varint scan.
- **Stream-level read prefetch buffer** in `StreamWrapper` (8 KiB) — all byte-wise reads
  (varints, headers, string bodies, cancel/drain) hit a persistent buffer drained at
  EndOfStream. Transparent to every read path.
- **Bulk-read Array/Map offset columns** in one `read_exact`.
- **Reuse the cached query template** on the common SELECT path (no per-query HashMap
  clone + re-serialize).
- **Skip the acquire-time Ping** for recently-used connections (idle-gated), with
  invalidate-on-broken-connection so it is strictly as safe as before.
- **Streaming cursor** decodes each block via the column-pre-extraction fast path.

### Added
- `AnyColumnData::plain_slice::<T>() -> Option<&[T]>` — native slice view for
  PlainColumn types (powers the row fast path).
- `Row::from_columns_collect` — bulk materialization hook (default loops; tuple impls
  override with the PlainColumn fast path).
- Benchmarks: `column_decode_bench`, `uint64_breakdown` (decode/access breakdown),
  `owned_vs_borrowed` (1M-row materialization).

### Fixed (security)
- TLS root-store hardening + type-parser recursion cap.
- Harden untrusted-input parsing + credential redaction.

### Refactor
- Dedup async/sync columns via `shared/` + `include!`.
- Extract LowCardinality header validation.

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
