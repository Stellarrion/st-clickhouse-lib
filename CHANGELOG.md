# Changelog

All notable changes to st-clickhouse are documented here.

## [Unreleased]

### Added
- **Real connect timeouts (sync)**: `SyncClient::connect_with_config` /
  `connect` / `connect_with_timeout` now resolve every address and enforce
  `connect_timeout` per TCP connect attempt (previously only the first
  resolved address was tried, with no timeout). The whole setup phase — TLS
  handshake, native handshake, addendum — runs under one absolute wall-clock
  deadline enforced by a socket-shutdown watchdog (with temporary socket I/O
  timeouts as a fallback), restored to the normal `query_timeout` read deadline
  after success. Silent and byte-dripping peers cannot extend setup. Setup
  expiry surfaces as the new `sync::Error::Timeout` (Linux `WouldBlock` /
  `TimedOut` both classified); `SyncClient::connect_stream` documents the same
  setup-bound semantics for pre-established sockets. `connect_timeout == 0`
  is rejected up front with the new `sync::Error::Config`.
- **Real connect timeouts (async)**: `SimplePool.connect_timeout` (set via
  `ClientBuilder::connect_timeout`, URL `?connect_timeout=`, or
  `Client::with_connect_timeout`) now bounds the whole per-address
  `connect_raw` future — TCP + TLS + native handshake + addendum + Ping —
  through the runtime timeout helper. Expiry returns `Error::Timeout` with the
  address and budget, leaves the slot empty, and feeds failover /
  circuit-breaker bookkeeping exactly like any other connect error.
  `Duration::ZERO` is rejected as `Error::Config` before any address is tried
  (never retried, never marked dead). DNS resolution remains unbounded by it
  and `acquire_timeout` still bounds only the slot wait. Reconnects read the
  current setting at connect time.
- **Python timeout/config mapping**: native connect/setup timeouts raise the
  built-in `TimeoutError`, and the high-level `map_error` translates it to
  `st_clickhouse.TimeoutError` (not a generic `ClickHouseError`); native
  configuration errors (`connect_timeout=0`) map to `st_clickhouse.ConfigError`.
- **True per-query settings overlays**: `SyncClient::{execute,query}_with_settings`,
  `_{with_params_and_settings}`, and `_{with_params_settings_and_ignored_part_uuids}`
  build each Query packet from the persistent `ClientConfig.settings` overlaid by a
  borrowed per-query map. The overlay wins on duplicate keys, automatic
  JSON/sparse defaults and query-parameter/ignored-part-UUID framing are preserved,
  and neither the config nor the cached packet template is mutated — so nothing
  leaks to later queries. An empty overlay keeps the cached-template fast path; the
  pre-existing methods delegate with an empty overlay and behave unchanged.
- **Python per-query settings**: the native `_Client.execute/query/query_tuples/
  query_columns/query_blocks` accept a keyword-only `settings` dict (parsed to owned
  strings before the GIL is released). Python `Client` passes `settings` straight
  through, and `AsyncClient.execute/query/query_tuples/query_columns/query_blocks`
  gained an explicit `settings` parameter threaded through the pool worker, and
  `AsyncSession.execute/query/query_blocks` route the same overlay on their pinned
  connection. Async `settings=...` is no longer swallowed by `**kwargs` and merged
  into query parameters.

### Fixed
- **Per-query settings no longer leak** (`st_clickhouse`): `with_per_query_settings`
  mutated the native client's session settings (`set_setting`) for every query and its
  `finally` restore loop was a no-op, so a per-query setting persisted on the
  connection forever and keys absent from the constructor baseline could never be
  restored. Per-query settings are now merged into that query's packet only; the
  connection baseline is structurally untouched and pooled connections are never
  reconfigured. The dead helper was removed.
- **Query timeout**: opt-in hard wall-clock deadline via `Client::with_query_timeout(d)`
  and per-query `QueryBuilder::timeout(d)`. On expiry the query is cancelled server-side
  (`Cancel` packet); the connection is discarded after a timeout so a partial
  response can never return to the pool. Default `None` — no behaviour change for
  queries that finish within their deadline. Async client only; sync core already
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
- `QueryBuilder::blocks()` returns every non-empty result block while preserving
  server block boundaries and moving, rather than copying, column payloads.
- **Query-ID collisions between sync and async clients**: the sync
  (`crate::sync`) and tokio (`crate::connection`) packet builders each owned a
  `st-ch-` counter, so a sync query and an async query issued from the same
  process minted identical IDs and ClickHouse rejected one with
  `QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING` (216). Standard `st-ch-` IDs now come
  from one process-wide generator in `crate::query_id` used by both builders;
  the wire format is unchanged and the Query packet's ClientInfo still repeats
  the same ID by design. Batch IDs keep their distinct `st-b-` prefix and
  their own counter.

### Changed (BREAKING)
- `PlainColumnData::read_from_bytes` now returns `Result<Self>` and rejects a logical
  element count that exceeds the backing bytes. This closes a safe-constructor
  soundness hole that could lead to an out-of-bounds unsafe read.
- Async `execute()` and `InsertSession::end()` now return ClickHouse server exceptions
  instead of reporting silent success.
- Sync `drain_response()` (backing `SyncClient::execute*` and `end_insert`) no longer
  swallows protocol errors: a failed DDL/DML response is always `Err`. The sync error
  model gained a structured `Error::ServerError { code, name, message }` populated by
  the native Exception parser (root code/name plus the nested chain in `message`),
  with malformed/truncated packets still reported as protocol or I/O errors. The
  Python binding maps it to `QueryError`.
- `.block()` and `fetch::<Block>()` now require exactly one non-empty server block and
  return an error on multi-block results instead of silently dropping later rows. Use
  `.blocks()` when the result can span blocks.

### Fixed
- **Python connection pool correctness** (`st_clickhouse._pool`): the pool now runs an
  explicit lending state machine (`_all` / `_available` / `_lent` / `_creating`). Fixes
  double-lending of freshly grown connections (the new client was both returned and left
  available), duplicate deque entries from double `release()`, slot resurrection after
  `close()` cleared state outside the lock (releases and in-flight health checks could
  re-add slots; the reaper could then "reap" a lent connection), and zombie slots when a
  health-check replacement failed. Factory calls and pings now run outside the condition
  lock (a slow health check no longer blocks other acquires), concurrent growth can never
  exceed `max_size` and discards its client if the pool closes mid-create, `release()`
  is idempotent and a no-op on a closed pool, and pool metrics gained a truthful
  `in_use` plus a new `creating` counter for in-progress growth calls (in-progress
  creates are excluded from `total`/`in_use` until they commit). Pool size/time
  configuration is validated at construction. No public API change.
- Derived rows map fields by column name even when the SELECT order differs, while
  tuples and existing manual `Row` implementations remain positional. The ordered
  fast path stays allocation-free.
- `Date32` consistently uses its signed 32-bit, four-byte wire representation in
  compressed block parsing, dynamic typed decode, and LowCardinality dictionaries.
- All native-protocol varint readers reject encodings outside the `u64` range instead
  of panicking or silently discarding high bits.
- Malformed LowCardinality dictionaries and indexes now return protocol errors instead
  of panicking, allocating from unchecked counts, or zero-filling invalid entries.
- `rows()` and `begin_select()` now decode LZ4/Zstd Data and ProfileEvents blocks using
  the query's negotiated compression mode; compressed streams no longer lose rows or
  fail as uncompressed input.
- Dropping or explicitly cancelling a `BlockStream` discards its socket when a clean
  asynchronous drain is not possible, preventing TLS framing violations and pooled
  connection desynchronization.
- Protocol/decode errors are no longer retried as if transient, and timed-out sockets
  are always removed from the pool before a later query can reuse partial responses.
- Dropping an unfinished `InsertSession` closes its socket instead of returning a
  connection that is still waiting for INSERT data to the pool.

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

### Python
- `st-clickhouse-py` version-aligned to `0.2.0`; rebuilt against the 0.2.0 Rust core.
  No Python binding API change.

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
