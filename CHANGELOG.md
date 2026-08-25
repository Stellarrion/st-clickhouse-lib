# Changelog

All notable changes to st-clickhouse are documented here.

## [Unreleased]

### Changed (BREAKING, Python bindings)
- **Python `cancel()` is now fail-closed** (`st_clickhouse`): `Client.cancel()`,
  `AsyncClient.cancel()`, and `AsyncSession.cancel()` always raise `RuntimeError`
  with guidance. The old implementations could not cancel anything: the native
  `_Client.cancel` borrows the client mutably, so it raised "Already borrowed"
  while a query was running (swallowed by the async wrapper), and idle pooled
  connections received stray no-op `Cancel` packets. `AsyncClient.cancel`
  additionally iterated the pool issuing per-client cancels — borrowed clients
  raised (cancelled nothing), idle ones got poisoned. This mirrors the Rust
  core's fail-closed `Client::cancel`.
- **Cancelling a task now stops its server-side query** (`st_clickhouse`): the
  dead `await self.cancel()` in every `CancelledError` handler is replaced by a
  deterministic kill. New native `_Client.discard()` is borrow-free (frozen
  pyclass + duplicated socket fd): it shuts the connection down from any thread
  in O(1), which unblocks the executor thread mid-query, makes the server abort
  the query (previously `SELECT sleep(3)` ran to completion as a zombie), and
  marks the client dead for any later use. The pool slot is destroyed and
  transparently replaced by the next acquire; pool capacity and metrics stay
  truthful. Acquire and query are now separate executor steps so the wrapper
  knows which pooled client to kill, and an acquire interrupted by cancellation
  can no longer leak its connection.
- **Abandoning a stream no longer desyncs the pool** (`st_clickhouse`):
  breaking out of `query_stream` (async or sync) previously returned the
  connection to the pool while the server was still streaming the old
  response — the next query on that connection failed with
  `unknown packet type: 0`. The native `_QueryStream` now tracks whether the
  response reached a terminal packet (EndOfStream or server exception,
  exposed as `.eos`); on any other exit the connection is killed (server
  aborts the query) and the async pool destroys the slot instead of
  recycling it. Fully-consumed streams still release cleanly. The sync
  `Client.query_stream` returns a new `QueryStream` wrapper implementing the
  same contract: early abandon closes the client (recreate it); server-error
  streams leave the client usable; dropping a mid-response stream kills its
  reader thread deterministically. `AsyncSession` cancellation destroys the
  pinned connection (the session must be reopened), and a cancelled
  `AsyncInsertStream.send`/`close` destroys its connection instead of
  recycling a socket left mid-INSERT.

### Security
- **Cumulative response-size budget is now enforced (`max_response_size`)**:
  the accumulating query APIs bound the total decoded payload bytes they
  retain — async `fetch`/`all`/`blocks`/`block` (first result block)/raw block
  capture and each `batch()` result set, and sync `query`/`query_all` (with a
  checked row-count sum replacing the overflow-panicking `usize` sum). The
  `ResponseTooLarge { limit, received }` error names the configured limit and
  the decoded total at breach and points to the streaming APIs
  (`rows()`/`RowCursor`, `BlockStream`, `QueryStream`), which stay unbudgeted
  by design — their memory is bounded per block. On breach the connection is
  discarded (async pool slot) or deterministically recovered via Cancel plus a
  bounded discard through the same buffered reader (sync), so the next query
  on the same client/pool succeeds. Configuration: async
  `ClientBuilder::with_max_response_size` and sync
  `ClientConfig::max_response_size` (default 256 MiB, previously dead
  config); the Python bindings surface the error as `QueryError` with the
  same guidance.
- **Capped server-controlled item counts (P0)**: server-controlled list and
  block dimensions are now validated against generous internal caps in
  `src/limits.rs` before they size any `Vec::with_capacity`/`reserve`
  allocation or bound any read loop, in both the async (Tokio) and sync
  engines. Counts only — per-string byte lengths, Array/Map offset totals,
  timeouts, and cancellation are unchanged and were addressed separately. A
  hostile or compromised server previously could
  panic the client with a capacity overflow (or drive an oversized
  allocation) from a small varint count; it now gets a deterministic
  `Error::Protocol` naming the field, the received count, and the limit.
  - Password complexity rule count in the server Hello (async and sync):
    capped at 65,536 rules (previously `u64::MAX` → capacity-overflow panic).
  - Ignored PartUUID count (async read path and sync response skip): capped
    at 1,048,576 UUIDs.
  - JSON path count and Dynamic subcolumn type-name count in column state
    prefixes (async raw reader, sync raw/materialized readers): capped at
    65,536 items each.
  - LowCardinality dictionary key count (async and sync readers, all
    raw/materialized/discard variants): capped at 65,536 keys.
  - Native block columns (≤ 65,536) and rows (≤ 10,000,000) per block across
    every parser variant — async streamed (plain and compressed), async
    decompressed-buffer, async raw capture, and the sync buffer, streamed,
    view, and discard readers. The row cap bounds a single block only; total
    rows across a streamed multi-block response are intentionally not
    limited.
  - Deterministic server-free regressions added for all of the above with
    `u64::MAX` and cap+1 counts (asserting `Protocol` errors, never panics),
    plus boundary (exact-cap) and within-cap happy paths.
- **Bounded server-controlled column byte lengths, nested totals, and
  count-derived eager allocations (P0)**: every inbound native-protocol column
  parser in both engines now validates server-controlled byte lengths and
  nested element counts against shared internal caps in `src/limits.rs`
  *before* any `resize`/`reserve`/`with_capacity` or read loop is sized from
  them: `MAX_STRING_BYTES` (16 MiB - 1, unchanged clickhouse-cpp wire limit)
  per String/JSON column value, and `MAX_COLUMN_BYTES` (64 MiB) per column for
  accumulated string values, fixed-width/offset/LowCardinality-index buffers,
  raw-capture arenas, and LowCardinality materialization. Array and Map
  offsets must be non-decreasing (cumulative prefix sums) and their last
  value — the inner element row count — is capped at 10,000,000 before the
  inner column is read. LowCardinality raw readers now require the index
  count to equal the outer row count (as the native format guarantees, and as
  the materialized readers already did); Variant compact-mode granules may
  carry at most the outer row count of the single non-empty variant (zero is
  legal for all-NULL granules, so equality is deliberately not enforced).
  Trusted outbound inserts are untouched. Previously a hostile or compromised
  server could drive multi-TiB eager allocations from a few bytes of wire
  (a varint string length, an 8-byte Array/Map offset of 2^60, a
  `rows * width` product, a LowCardinality index count, a Variant compact row
  count, or dictionary expansion that repeats one 16 MiB entry across 10M
  rows); it now gets a deterministic `Error::Protocol` naming the field, the
  received value, and the limit.
  - Covered paths: async `read_string_column_with_prefixes`, the streamed and
    decompressed-buffer block readers (read/skip), `read_offsets_column`,
    materialized and discard column readers, the raw-capture reader (per
    column byte budget incl. state prefixes), LowCardinality materialization
    (async and sync), and the sync buffer, streamed, view, raw, and discard
    readers. Compressed and plain payloads share the same column-level caps.
  - Compatibility: a single column whose wire data (or materialized output)
    exceeds 64 MiB now fails deterministically instead of allocating; blocks
    up to 10M rows remain accepted when each column stays within the budget.
    Non-monotonic Array/Map offsets are rejected (valid servers always emit
    cumulative prefix sums).
  - Deterministic server-free regressions added for lying string/JSON lengths
    (`2^40`, `u64::MAX`), cumulative column cap + 1, `2^60` Array/Map offsets,
    decreasing offsets, fixed-width and LowCardinality index buffers at
    cap + 1, LowCardinality index-count mismatch and `u64::MAX`, and Variant
    compact row claims — all asserting `Protocol` errors before allocation or
    read, with boundary (equality and within-cap) paths retained.
- **Bounded transport chunk and compression-frame allocations (P0)**: all
  server-controlled chunk and compression-frame lengths are now validated
  against shared internal caps (`64 MiB`) before any buffer is sized, in both
  the async (Tokio) and sync
  engines. Previously a single 4-byte chunk header or a 25-byte compression
  frame header could drive up to a 4 GiB (chunked transport, sync decoder) or
  1 GiB (async decoder, block reader) allocation — a denial-of-service vector
  against clients connected to a hostile or compromised server.
  - Chunked native transport (async `StreamWrapper` and sync `ChunkedReader`):
    a chunk length above the cap fails fast with `InvalidData` before the
    chunk buffer is resized.
  - Compression frames (async and sync `decode_frame`): `compressed_size` is
    checked against the 9-byte mandatory header and the cap, and
    `uncompressed_size` against the cap, before any allocation; a frame whose
    checksum is valid but whose declared sizes exceed the cap is still
    rejected. The checksum is now computed in place over a single
    checksum+header+body buffer instead of duplicating the body, and zstd
    output is bounded during decompression to the declared (capped) size — a
    frame that expands beyond its declaration fails at decompression instead
    of decoding first and validating only afterwards (LZ4 was already bounded
    by its capacity hint).
  - Async block reader: after the compression method byte matches, an
    oversized `compressed_size` is rejected before the frame body buffer is
    resized or read (previously up to 1 GiB); sub-header sizes keep the
    existing plain-payload fallback.
  - Encode paths now use checked `u32` conversions for the wire size fields:
    a payload that cannot be represented is refused with a clear error
    instead of silently truncating `usize -> u32` into a corrupt frame.
    Trusted outbound writes stay allowed up to the 4 GiB wire limit.
  - Deterministic server-free regressions added for all of the above
    (`u32::MAX` chunk headers on both transports, `u32::MAX`
    compressed/uncompressed declarations, valid-checksum over-cap frames,
    block-reader oversized frames, zstd expansion beyond the declared size);
    None/LZ4/Zstd roundtrips are retained.

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
- **Buffered block framing now mirrors the wire format for Array/Map/JSON/
  LowCardinality/Variant/Dynamic columns** (both engines): the async
  compressed-materialized parser (`parse_decompressed_block`/`discard_decompressed_block`
  in `connection/block_reader.rs`) and the sync buffered parser
  (`skip_column_data` in `sync/protocol/response.rs`, used by `QueryStream`
  and totals/extremes blocks) previously framed columns with ad-hoc skip code
  that desynced whole blocks and lost the connection. Both now share one
  slice-based skip implementation (`shared/skip_column.rs`) that mirrors the
  raw stream readers byte for byte. Fixed layouts: Array/Map offsets are
  fixed-width little-endian `UInt64` per outer row (previously parsed as
  varints on the async path, and skipped entirely on the sync path), the inner
  columns carry exactly the last-offset rows — zero rows and zero bytes when
  every array is empty (the sync path previously skipped `rows - 1` inner
  rows); a materialized JSON column's 8-byte string-serialization version is
  consumed as framing and stripped from the sliced data (previously missing on
  the async path); LowCardinality columns are framed and materialized through
  their 24-byte header/dictionary/index layout (previously skipped as a bare
  inner column on the async path, and the sync buffered parser hung on the
  zero-row header block of every SELECT with a LowCardinality column);
  Variant/Dynamic columns consume their per-subcolumn state prefixes,
  discriminators, and counted subcolumns instead of jumping to the end of the
  buffer; and `AggregateFunction` columns are rejected like the streaming
  readers instead of silently misframing later columns.
- **Dropping a query future no longer poisons the pool** (`st_clickhouse`, async
  engine): every pooled connection now carries an in-flight response mark, set before
  the first response-triggering packet write (query, pre-query Ping, TablesStatus,
  batch) and cleared after the terminal packet (EndOfStream / end of an Exception
  chain) or a resolved response cycle. When a future is dropped at an await point
  mid-response (timeout wrapper, `tokio::spawn` + `abort`, `select!`), the guard now
  discards that socket so the next `pool.get()` makes exactly one clean reconnect —
  previously the mid-response socket returned to the pool and the next user hit a
  bogus `Protocol` error that kept the slot poisoned until an idle Ping eventually
  failed. The mark is owned by the task holding the guard, so there is no
  cross-task race; the clear window can at worst cause one needless reconnect.
  `BlockStream`/`InsertSession` keep their own session-level discard and clear the
  mark at their clean terminal points.
- **`SimplePool::drop` no longer writes an unframed Cancel byte through chunked or
  TLS transports**: the best-effort raw `try_write(Cancel)` is now sent only when the
  transport is plain TCP with chunked sending off — the only case where a single raw
  byte is wire-correct. Chunked and TLS connections are simply closed instead of
  emitting protocol garbage.
- **Chunked-receive reentrancy** (async engine): a poll that returned `Pending`
  between reading a chunk's 4-byte length and its payload made the next poll serve
  the zero-fill bytes of the resized buffer (and re-read a length from payload
  bytes). The chunked reader now serves only received bytes (`chunk_fill`) and
  resumes the in-progress chunk instead of restarting the frame. Found by the new
  chunked-transport pool-drop regression test, whose mock writes frame parts in
  separate TCP segments — exactly the split a real server or proxy can produce.
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
- **Sync `QueryStream` chunked refill**: the chunked-transport receive path in
  `SyncClient::start_stream` now rejects a server-claimed chunk length above
  the shared 64 MiB transport cap before resizing its buffer (previously an
  eager zeroed allocation of up to 4 GiB-1 from a 4-byte header).
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
- **Async `Client::cancel()` is now fail-closed** (`st_clickhouse`): it used to grab an
  arbitrary idle pooled connection and send `Cancel` there — the stray packet was
  silently swallowed by the server (cancelling nothing), and with a busy single-slot
  pool it blocked until the query finished. A `Client` owns a pool, not the connection
  running your query, so `cancel()` now returns `Error::Config` explaining the
  query-scoped alternatives without touching any connection, and is marked
  `#[deprecated]` (signature unchanged). Use a query deadline
  (`Client::with_query_timeout` / `QueryBuilder::timeout`), `BlockStream::cancel()` on
  a `begin_select` stream, or drop the `RowCursor` returned by `QueryBuilder::rows()`.
  The sync engine's `SyncClient::cancel` is unchanged.
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

### Fixed (CI)
- **The `ci` profile is now passed to nextest as a cargo profile**: the test
  job ran `cargo nextest run --profile ci`, which selects a *nextest* profile
  that no `.config/nextest.toml` in the repo defines. It now runs with
  `--cargo-profile ci`, so the `[profile.ci]` cargo profile from `Cargo.toml`
  applies while nextest keeps its default profile.
- **The crates.io wait now tracks the derive crate's real version**: the
  publish job polled the index for a hardcoded `st-clickhouse-derive =
  "0.1.0"` while the crate is at 0.2.0, so every release would have timed out
  after publishing derive. The expected version is now parsed from
  `derive/Cargo.toml` (pure shell, no new runner dependencies) and the timeout
  message reports it.
- **MSRV is now enforced in CI, and the declared MSRV is now true**: a
  bounded `msrv` job runs `cargo check --workspace --all-features` using the
  `rust-version` parsed live from `Cargo.toml`, so the check cannot drift from
  the declaration; `publish-crates` and `build-wheels` depend on it. Enforcing
  this exposed that 1.85 was never buildable: `src/pool.rs` uses let-chains
  (stable since Rust 1.88) and `--all-features` enables `bench-clickhouse-rs`,
  whose `clickhouse` 0.15 dependency requires Rust 1.89. The declared
  `rust-version` is corrected from 1.85 to 1.89 (also in `fuzz/Cargo.toml`);
  verified locally: the check passes on 1.89 and fails on 1.85/1.88.
- **Crate versions are guarded before publishing**: a `version-check` job
  asserts that `st-clickhouse-lib`, `st-clickhouse-derive`, and
  `st-clickhouse-py` (both its `Cargo.toml` and `pyproject.toml`) carry the
  same version, failing fast with a clear message; both release jobs depend
  on it.

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
