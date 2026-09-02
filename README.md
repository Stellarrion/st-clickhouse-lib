# st-clickhouse-lib

> **Zero-copy ClickHouse native protocol client** — Rust + Python.  
> Native TCP, typed Rust rows, columnar Python output shapes, TLS, compression, pooling, and protocol coverage for ClickHouse 24.x onward.

[![CI](https://github.com/Stellarrion/st-clickhouse-lib/actions/workflows/ci.yml/badge.svg)](https://github.com/Stellarrion/st-clickhouse-lib/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/st-clickhouse-lib.svg)](https://crates.io/crates/st-clickhouse-lib)
[![PyPI](https://img.shields.io/pypi/v/st-clickhouse-py.svg)](https://pypi.org/project/st-clickhouse-py/)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](#license)

---

## Table of Contents

- [Quick Start](#quick-start)
- [Python Quick Start](#python-quick-start)
- [When to Use Which Output Shape](#when-to-use-which-output-shape)
- [API Reference](#api-reference)
  - [Rust](#rust-client)
  - [Python](#python-client)
- [Security & Authentication](#security--authentication)
- [ClickHouse Type Mappings](#clickhouse-type-mappings)
- [Architecture](#architecture)
- [Performance](#performance)
- [Benchmarks](#benchmarks)
- [Features](#features)
- [Compatibility](#compatibility)
- [Release](#release)
- [Contributing](#contributing)
- [License](#license)

---

## Quick Start

```toml
[dependencies]
st-clickhouse-lib = { version = "0.3", features = ["derive"] }
```

```rust
use st_clickhouse::Client;
use st_clickhouse::connection::{QueryResult, RowCount, Scalar};
use st_clickhouse::protocol::block::{Block, ColumnInfo};

let client = Client::connect("127.0.0.1:9000").await?;

// Blocks — all server blocks, preserving zero-copy columnar payloads
let blocks = client
    .query("SELECT number FROM system.numbers LIMIT 100000")
    .blocks()
    .await?;
for block in &blocks {
    let nums = block.column::<u64>("number")?;
    if let Some(slice) = nums.as_slice() {
        println!("received {} values", slice.len());
    }
}

// Vec — owned rows, ergonomic
let rows: Vec<(u64, String)> = client
    .query("SELECT number, toString(number) FROM system.numbers LIMIT 10")
    .fetch().await?;

// One — single row
let one: (u64,) = client.query("SELECT toUInt64(1)").fetch().await?;

// Optional — 0-or-1 rows
let maybe: Option<(u64,)> = client
    .query("SELECT toUInt64(1) WHERE 0")
    .fetch().await?;

// Scalar — single value, wrapped for type safety
let count: Scalar<u64> = client
    .query("SELECT count() FROM system.numbers LIMIT 1000000")
    .fetch().await?;
println!("{}", count.into_inner());

// RowCount — scan without materializing columns
let scanned: RowCount = client
    .query("SELECT number FROM system.numbers LIMIT 1000000")
    .fetch().await?;
println!("{} rows", scanned.get());

// Streaming rows — constant memory, background I/O prefetch
let mut cursor = client
    .query("SELECT number FROM system.numbers LIMIT 100")
    .rows::<(u64,)>().await?;
while let Some((n,)) = cursor.next().await? {
    println!("{n}");
}

// INSERT — FORMAT Native blocks (begin_insert → send_data → end)
client.execute("CREATE TABLE IF NOT EXISTS events (ts DateTime, value Float64) ENGINE = Memory").await?;
let mut insert = client.begin_insert("events").await?;
let block = Block {
    columns: vec![
        ColumnInfo {
            name: "ts".into(),
            type_name: "DateTime".into(),
            data: bytes::Bytes::copy_from_slice(&1_700_000_000u32.to_le_bytes()),
            lc_materialized: bytes::Bytes::new(),
        },
        ColumnInfo {
            name: "value".into(),
            type_name: "Float64".into(),
            data: bytes::Bytes::copy_from_slice(&1.5f64.to_le_bytes()),
            lc_materialized: bytes::Bytes::new(),
        },
    ],
    rows: 1,
};
insert.send_data(&block).await?;
insert.end().await?;
```

---

## Python Quick Start

```bash
pip install st-clickhouse-py
```

```python
from st_clickhouse import Client

client = Client("localhost:9000")

# Row dicts — ergonomic, best for <100K rows
rows = client.query("SELECT count() AS cnt FROM system.tables")
print(rows[0]["cnt"])  # 42

# With server-side parameters
rows = client.query(
    "SELECT {id:UInt64} AS val, {name:String} AS label",
    params={"id": 1, "name": "hello"},
)

# With per-query settings (applied to that query only)
rows = client.query("SELECT * FROM big_table", settings={"max_threads": "8"})

# Tuples — faster than dicts for large results
rows = client.query_tuples("SELECT number, toString(number) LIMIT 10")

# Columns — columnar access, 41M rows/s
col_nums, col_strs = client.query_columns(
    "SELECT number, toString(number) FROM system.numbers LIMIT 100000"
)

# Blocks — rawest/fastest, 147M rows/s
blocks = client.query_blocks(
    "SELECT number FROM system.numbers LIMIT 100000"
)
for block in blocks:
    col = block["number"]         # Column object
    values = col.to_list()        # list[int]

# Streaming — low memory for large results
for block in client.query_stream("SELECT number FROM system.numbers"):
    for row in block.rows():
        print(row)

# INSERT
client.execute("CREATE TABLE IF NOT EXISTS test (x Int32) ENGINE Memory")
client.insert("INSERT INTO test VALUES", [{"x": 1}, {"x": 2}, {"x": 3}])

# TLS
client = Client(
    "clickhouse.example.com:9440",
    tls=True,
    tls_domain="clickhouse.example.com",
    tls_ca_file="/path/to/ca.crt",
)

# Cleanup
client.close()
```

---

## When to Use Which Output Shape

### Rust

| Method | Returns | Rows/s (1M rows) | Memory | Best For |
|--------|---------|-----------------|--------|----------|
| `.block()` | `Block` | 60M+ | Zero-copy (borrowed) | Results guaranteed to contain exactly one server block |
| `.blocks()` | `Vec<Block>` | 60M+ | Zero-copy payloads | Multi-block columnar results without dropped rows |
| `.all::<T>()` | `Vec<T>` | 20M+ | Owned rows | Small results, ergonomic access |
| `.rows::<T>()` | `RowCursor<T>` | 10M+ | Streaming | Large results, low memory |
| `.execute(sql)` | `()` | N/A | N/A | DDL/DML with no result rows: `CREATE`, `ALTER`, `DROP`, `INSERT ... VALUES` (for native-block INSERT use `begin_insert()` + `send_data()` + `end()`) |

**Rule of thumb:**
- **Result < 10K rows** → `.all::<T>()` — ergonomic, no borrow lifetime issues
- **Result 10K–1M rows** → `.blocks()` — all columnar blocks, fastest materialized path
- **Result > 1M rows** → `.rows::<T>()` — streaming, constant memory
- **Need one known server block** → `.block()` — errors rather than truncating if another block arrives
- **Need column slices** → iterate `.blocks()` and call `block.column::<T>("name")`
- **Need owned rows** → `.all::<(u64, String)>()`

### Python

| Method | Returns | Rows/s | Best For |
|--------|---------|--------|----------|
| `query()` | `list[dict]` | 7.6M | Ergonomics, small results |
| `query_tuples()` | `list[tuple]` | 14.1M | Large flat results |
| `query_columns()` | `list[list]` | 47.4M | Columnar processing |
| `query_blocks()` | `list[Block]` | 136.7M | Rawest, least allocation |
| `query_stream()` | `Iterator[Block]` | 349.8M | Very large, streaming |

**Rule of thumb:**
- **Result < 10K rows** → `query()` — dicts are convenient
- **Result 10K–1M rows** → `query_columns()` or `query_blocks()`
- **Result > 1M rows** → `query_stream()` — constant memory
- **Need column slices** → `query_columns()`
- **Need dicts** → `query()` (but beware: 2x overhead vs tuples)

---

## API Reference

### Rust Client

```rust
use st_clickhouse::Client;

// ── Builder ──────────────────────────────────────────────────
let client = Client::builder()
    .hosts(["ch-1:9000", "ch-2:9000"])
    .pool_size(8)
    .user("default")
    .password("secret")
    .compression(CompressionMethod::Lz4)
    .connect()
    .await?;

// ── Simple connect ───────────────────────────────────────────
let client = Client::connect("127.0.0.1:9000").await?;

// ── Builder chain ────────────────────────────────────────────
let client = Client::connect("127.0.0.1:9000")
    .await?
    .with_compression(CompressionMethod::Lz4)
    .with_ping_before_query(true)
    .with_send_retries(3)
    .with_retry_timeout(Duration::from_secs(5))
    .with_send_timeout(Duration::from_secs(30))
    .with_recv_timeout(Duration::from_secs(120))
    .with_setting("max_threads", "8");

// ── Connection pool ──────────────────────────────────────────
let client = Client::connect_with_pool("127.0.0.1:9000", 8).await?;

// ── DNS rotation (multi-node clusters) ───────────────────────
// Resolves ALL A/AAAA records, round-robins, refreshes every 300s
let client = Client::connect_with_hostname("ch-cluster.example.com", 9000, "default", "").await?;

// ── TLS ──────────────────────────────────────────────────────
// Enable the `tls` feature in Cargo.toml
let client = Client::connect("clickhouse.example.com:9440")
    .await?
    .with_tls("clickhouse.example.com")?;

// ── SSH authentication ───────────────────────────────────────
let client = Client::connect_with_ssh_signer(
    "127.0.0.1:9000",
    "ssh_user",
    |challenge: &[u8]| {
        // Sign challenge with SSH key
        sign_ssh_challenge(challenge)
    },
).await?;

// ── External tables ──────────────────────────────────────────
let block: Block = /* ... */;
let rows = client
    .query("SELECT * FROM ext WHERE id IN (SELECT id FROM external_table)")
    .with_external_table("external_table", block)
    .fetch::<Vec<(u64, String)>>()
    .await?;

// ── Query callbacks ──────────────────────────────────────────
let block = client
    .query("SELECT number FROM system.numbers LIMIT 10")
    .with_callbacks(QueryCallbacks {
        on_progress: Some(Box::new(|p| println!("Progress: {p:?}"))),
        on_log: Some(Box::new(|l| println!("Log: {l:?}"))),
        ..Default::default()
    })
    .fetch::<Block>()
    .await?;

// ── Batch queries (single round-trip) ────────────────────────
let results = client.batch()
    .query("SELECT 1")
    .query("SELECT 2")
    .query("SELECT 3")
    .execute()
    .await?;
```

#### Connection timeouts

`connect_timeout` (builder option, URL `?connect_timeout=`, or
`Client::with_connect_timeout` / sync `ClientConfig::with_connect_timeout`)
bounds **each per-address connection attempt end to end**: TCP establishment,
TLS handshake, native protocol handshake, addendum, and (async) the
connect-time Ping. A server that accepts TCP and then never sends its Hello
fails with `Error::Timeout` instead of hanging until the query timeout.

- Async: each resolved address gets the full budget; expiry returns
  `Error::Timeout` naming the address and budget, the pool slot stays empty,
  and failover/circuit-breaker bookkeeping treats it like any connect failure.
- Sync: resolved addresses are tried in order, each with one full end-to-end
  budget shared by TCP and setup. An absolute socket-shutdown watchdog (plus
  socket I/O timeouts as fallback) defeats byte-drip peers; the normal
  `query_timeout` read deadline is restored after success.
- `Duration::ZERO` is rejected with `Error::Config` — it cannot mean "no
  deadline". Async pools are unbounded when the option is unset; sync
  `ClientConfig` defaults to 10 seconds.
- DNS resolution is not bounded by `connect_timeout`. It is independent of
  `acquire_timeout`, which bounds only the wait for a free pool slot.

### Python Client

```python
from st_clickhouse import Client, AsyncClient

# ── Sync Client ───────────────────────────────────────────────

# Simple
client = Client("localhost:9000")

# With options
client = Client(
    "localhost:9000",
    user="default",
    password="",
    database="default",
    settings={"max_threads": "8"},
    compression="lz4",          # "lz4", "zstd", or None
    connect_timeout=30,         # bounds TCP + handshake setup
    query_timeout=300,          # hard deadline per query (server-side cancel)
    max_response_size=256 * 1024 * 1024,  # response budget in bytes
    tls=True,
    tls_domain="clickhouse.local",
    tls_ca_file="/etc/ssl/certs/ca.crt",
    tls_client_cert="/path/to/client.crt",
    tls_client_key="/path/to/client.key",
)

client.query("SELECT count() AS cnt FROM system.tables")
client.query_tuples("SELECT number FROM system.numbers LIMIT 10")
client.query_columns("SELECT number FROM system.numbers LIMIT 10")
client.query_blocks("SELECT number FROM system.numbers LIMIT 10")

# Streaming
for block in client.query_stream("SELECT number FROM system.numbers"):
    for row in block.rows():
        print(row)

client.execute("CREATE TABLE test (x Int32) ENGINE Memory")
client.insert("INSERT INTO test VALUES", [{"x": 1}, {"x": 2}])

# Per-query settings (applied to that query only)
rows = client.query("SELECT * FROM big_table", settings={"max_threads": "8"})

# Parameterized queries
rows = client.query(
    "SELECT {id:UInt64} AS val",
    params={"id": 42},
)

# Pool metrics (AsyncClient only)
metrics = async_client.metrics
# => {pool_slots: 4, pool_in_use: 1, connection_errors: 0, ...}

client.close()

# ── Async Client ──────────────────────────────────────────────

async def main():
    async with AsyncClient("localhost:9000") as client:
        rows = await client.query("SELECT count() AS cnt FROM system.tables")
        async for block in client.query_stream("SELECT number FROM system.numbers"):
            for row in block.rows():
                print(row)
```

---

## Security & Authentication

### TLS (Native TCP)

TLS is provided by `rustls` (no OpenSSL dependency). Enable with the `tls` feature:

```toml
[dependencies]
st-clickhouse-lib = { version = "0.3", features = ["tls"] }
```

```rust
// CA-verified (default — uses system root store)
use st_clickhouse::Client;

let client = Client::connect("ch.example.com:9440")
    .await?
    .with_tls("ch.example.com")?;

// Custom CA
let mut root_store = rustls::RootCertStore::empty();
root_store.add_parquet_certificate_file("ca.crt")?;
let config = rustls::ClientConfig::builder()
    .with_root_certificates(root_store)
    .with_no_client_auth();
let client = Client::connect("ch.example.com:9440")
    .await?
    .with_tls_config(config, "ch.example.com")?;
```

**⚠️ Security note:** Without the `tls` feature, credentials are sent in cleartext over TCP.

### SSH Key Authentication

ClickHouse supports SSH public-key authentication for protocol revisions ≥ 54483:

```rust
let client = Client::connect_with_ssh_signer(
    "127.0.0.1:9000",
    "ssh_user",
    |msg: &[u8]| {
        // msg = "{revision}{database}{user}{challenge}"
        ssh_key_sign(msg, "/path/to/id_ed25519")
    },
).await?;
```

The signer receives the challenge payload and must return a signature string.
Typical usage with `ssh-keygen -Y sign`:

```rust
use std::process::Command;
let sig = String::from_utf8(
    Command::new("ssh-keygen")
        .args(["-Y", "sign", "-f", key_path, "-n", "clickhouse", &msg_path])
        .output()?
        .stdout
)?;
```

### Security

Top critical risks addressed in v0.1:
- ✅ TLS support (optional feature, `rustls`-based)
- ✅ Connection circuit breaker (exponential backoff)
- ✅ Write timeouts with configurable `send_timeout`
- ✅ Query retry (exponential backoff + jitter)
- ✅ `overflow-checks = true` in release profile
- ✅ `unwrap_used = "deny"` and `panic = "deny"` clippy lints

---

## ClickHouse Type Mappings

| ClickHouse | Rust Type | Python Type | Columnar Access |
|-----------|-----------|-------------|-----------------|
| `UInt8/16/32/64` | `u8`..`u64` | `int` | `&[T]` |
| `UInt128/256` | `u128`, `UInt256` | `int` (via `bytes`) | `&[T]` |
| `Int8/16/32/64` | `i8`..`i64` | `int` | `&[T]` |
| `Int128/256` | `i128`, `Int256` | `int` | `&[T]` |
| `Float32/64` | `f32`, `f64` | `float` | `&[T]` |
| `String` | `String` | `str` | offsets + data |
| `FixedString(N)` | `FixedStringBytes` | `bytes` | `&[u8]` |
| `Date` / `Date32` | `Date` | `datetime.date` | `&[Date]` |
| `DateTime` | `DateTime` | `datetime.datetime` | `&[DateTime]` |
| `DateTime64(N)` | `DateTime64Value` | `datetime.datetime` | `&[DateTime64Value]` |
| `Decimal(S)` | `Decimal32/64/128/256` | `Decimal` | `&[T]` |
| `Bool` | `bool` | `bool` | via `get(index)` |
| `UUID` | `Uuid` | `uuid.UUID` | `&[Uuid]` |
| `IPv4` | `Ipv4` | `str` (dotted) | `&[Ipv4]` |
| `IPv6` | `Ipv6` | `str` (colon) | `&[Ipv6]` |
| `Enum8` / `Enum16` | `i8`/`i16` | `str` (label) | via runtime dispatch |
| `Array(T)` | `Vec<T>` | `list` | via block column API |
| `Map(K, V)` | `HashMap<K, V>` | `dict` | via block column API |
| `Tuple(T...)` | `(T1, T2, ...)` | `tuple` | via block column API |
| `Nullable(T)` | `Option<T>` | `None` or `T` | via `get(index)` |
| `LowCardinality(T)` | same as `T` | same as `T` | auto-materialized |
| `JSON` | `JsonValue` | `dict` | via `get(index)` |
| `Variant(T...)` | `VariantValue` | `dict` (tagged) | via `get(index)` |
| `Dynamic` | `DynamicValue` | `dict`/`list`/scalar | via `get(index)` |
| `Point` | `Point` | `tuple[float, float]` | `&[Point]` |
| `Ring` | `Ring` | `list[tuple]` | via block column API |
| `Polygon` | `Polygon` | `list[list[tuple]]` | via block column API |
| `MultiPolygon` | `MultiPolygon` | `list[list[list[tuple]]]` | via block column API |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                  st-clickhouse-lib (async)                   │
│  Client · QueryBuilder · InsertSession · BatchBuilder        │
│  RowCursor · Pool · Metrics · Callbacks                      │
│  Tokio async I/O · Connection pool · DNS rotation            │
│  Circuit breaker · Write timeouts · Query retry              │
└───────────────────────┬──────────────────────────────────────┘
                        │ depends on
┌───────────────────────▼──────────────────────────────────────┐
│                st-clickhouse-lib sync core                   │
│  SyncClient · ClientConfig · Transport (TCP/TLS)             │
│  Handshake (password + SSH) · Protocol negotiation           │
│  Wire format (varint · string · checksummed blocks)          │
│  LZ4/ZSTD compression with CityHash128 integrity             │
└───────────────────────┬──────────────────────────────────────┘
                        │
┌───────────────────────▼──────────────────────────────────────┐
│               st-clickhouse-py (PyO3)                        │
│  Client · AsyncClient · Block · InsertSession                │
│  Thread-per-query bridge · asyncio integration               │
│  dict/tuple/column/block output shapes                       │
└──────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

- **Two engines, one wire format** — The async client speaks native tokio I/O directly (`tokio::net::TcpStream`, pool, cancellation-safe futures). The independent sync core (`std::net::TcpStream`) backs the Python bindings; both share the wire codec and column code in `shared/`.
- **Lockless pool** — Per-slot `tokio::sync::Mutex`, no blocking mutex in async path. Round-robin via `AtomicUsize`.
- **Zero-copy columns** — `PlainColumnData<T>` provides `&[T]` directly into the decompression buffer when aligned, falling back to `read_unaligned` when misaligned (Nullable mask before data).
- **DNS rotation** — `resolve_all()` returns every A/AAAA record. Pool rotates through all IPs round-robin. Periodic DNS refresh (300s default) picks up new cluster nodes.
- **Circuit breaker** — Per-address exponential backoff on failure, cleared on successful reconnect.

---

## Performance

How these numbers were produced:

- Both clients run **identical** `numbers(N)` queries against the same local
  ClickHouse over native TCP. Latencies are the minimum of ~15–30 runs —
  lower is better.
- Re-run them yourself: `cargo run --release --bin bench_all_workloads`
  (Rust) and `benches/cpp/st_bench.cpp` built against `clickhouse-cpp -O3`;
  see `benches/README.md`.
- `numbers(N)` is used instead of `system.numbers LIMIT` because
  `clickhouse-cpp` mishandles that aggregate plan and blocks.

### Rust vs C++

The **C++ / Rust** column is `clickhouse-cpp` latency divided by
`st-clickhouse` latency — above `1.00x` means Rust is faster, and the
number reads directly as "how many times faster". These are local
benchmark-harness results, not universal guarantees; the
owned-materialization row is cache-sensitive and varies a few percent.

| Workload | st-clickhouse | clickhouse-cpp `-O3` | C++ / Rust |
|----------|---------------|----------------------|------------|
| `SELECT 1` | 0.400ms | 0.416ms | **1.04x** |
| `COUNT()` over 1M rows | 0.805ms | 0.796ms | 0.99x |
| `GROUP BY` 1K groups | 2.664ms | 3.011ms | **1.13x** |
| `ORDER BY ... LIMIT 100` | 1.255ms | 1.337ms | **1.07x** |
| JSON materialization (1K) | 0.551ms | 0.526ms | 0.95x |
| 50 columns × 1K rows | 0.920ms | 0.914ms | 0.99x |
| 1 UInt64 × 1M rows (owned) | 5.452ms | 10.071ms | **1.85x** |
| 1 UInt64 × 1M rows (borrowed) | 1.676ms | 1.713ms | **1.02x** |
| INSERT Memory 10K rows † | 0.549ms | 0.534ms | 0.97x |
| ALTER UPDATE 5K/10K rows † | 0.525ms | 0.518ms | 0.99x |

The **UInt64 owned-materialization row** is where st-clickhouse's PlainColumn bulk-slice fast path (`read_all` / `query_all`, since 0.2.0) shows: 5.452ms vs `clickhouse-cpp`'s 10.071ms — **1.85x faster** — its per-value column access can't match a vectorized slice copy. Most other rows are network/server-bound and within ~5% of C++.

† *Server-version drift:* these numbers were measured on ClickHouse **26.4**. On **26.7+** the server itself charges a flat ~60 ms per mutation (raw HTTP inserts that bypass the client cost the same), so both rows track the server's mutation path, not client overhead — the client still adds ~0.3 ms or less. All other rows were re-measured on 26.7 at or better than the values shown.

### Python Materialization (Multiple Output Shapes)

| Workload | Sync | Async | Rows/s |
|----------|------|-------|--------|
| `SELECT 1` (row dict) | 0.465ms | 0.569ms | — |
| 100K rows as dicts | 13.208ms | 12.796ms | 7.6M |
| 100K rows as **tuples** | 9.485ms | 7.085ms | 14.1M |
| 100K rows as **columns** | 2.426ms | 2.111ms | 47.4M |
| 100K rows as **blocks** | 0.679ms | 0.732ms | 147.3M |
| 1M rows as blocks | 2.391ms | 2.141ms | 467.1M |
| 32 concurrent `SELECT 1` | N/A | 5.027ms | — |

Throughput has been **stable from 0.1.0 onward** (within run-to-run variance): the 0.2.0-era optimizations live in the Rust materialization core (see *Rust vs C++* above — the UInt64 owned row), which is a small slice of Python end-to-end time, where PyO3 FFI + Python object construction dominate.

### Python vs Official `clickhouse-connect` (HTTP)

| Workload | st-click sync | official sync | st-click async | official async |
|----------|:--------:|:---------:|:---------:|:----------:|
| Connect | **0.178ms** | 3.973ms | **0.276ms** | 4.748ms |
| `SELECT 1` | **0.465ms** | 0.712ms | **0.569ms** | 0.646ms |
| 100K rows (tuples) | **9.485ms** | 10.001ms | **7.085ms** | 10.058ms |
| 1M rows (blocks/columns) | **2.391ms** | 61.350ms | **2.141ms** | 173.246ms |
| 32 concurrent | N/A | N/A | **5.027ms** | 7.078ms |

### Rust vs Official `clickhouse-rs` (HTTP)

| Workload | st-clickhouse sync | clickhouse-rs | Speedup |
|----------|:------------------:|:-------------:|:-------:|
| `SELECT 1` | 0.515ms | 0.537ms | 1.04× |
| UInt64 100K rows | **0.791ms** | 3.424ms | **4.3×** |
| UInt64 1M rows | **3.183ms** | 17.467ms | **5.5×** |
| String 100K rows | **2.050ms** | 5.903ms | **2.9×** |

<details>
<summary>Reproducing benchmarks</summary>

```bash
cargo run -p st-clickhouse-lib --release --bin bench_vs_ch
cargo run -p st-clickhouse-lib --profile benchmark --bin bench_vs_ch
cargo run -p st-clickhouse-lib --profile benchmark \
  --features bench-clickhouse-rs --bin bench_vs_clickhouse_rs
cd st-clickhouse-py
uv run python bench/python_bench.py
uv run --with 'clickhouse-connect[async]' python bench/python_bench.py
```

For profiling:
```bash
cargo rustc -p st-clickhouse-lib --profile benchmark \
  --bin profile_core_workload -- -C debuginfo=1 -C strip=none
perf record -F 997 -g -- target/benchmark/profile_core_workload scan-1m-view 500
```
</details>

---

## Features

### Core
- ✅ **Zero-copy columnar access** — `&[u64]`, `&[f64]`, `&[Date]` directly into decompression buffer
- ✅ **Broad ClickHouse type coverage** — primitives, decimals, geo, JSON, Variant, Dynamic, aggregate functions
- ✅ **Modern server packet coverage** — Data, Progress, Profile, Log, ProfileEvents, Exception, EndOfStream,
      PartUUIDs (skipped), TimezoneUpdate (skipped), SSHChallenge
- ✅ **Connection pool** — lockless per-slot mutex, automatic reconnect
- ✅ **DNS rotation** — resolve all A/AAAA records, round-robin, periodic refresh
- ✅ **Circuit breaker** — per-address exponential backoff on failure
- ✅ **Query retry** — configurable retries with exponential backoff + jitter
- ✅ **Write timeouts** — configurable per-operation timeout
- ✅ **Query deadlines** — `Client::with_query_timeout` / per-query `QueryBuilder::timeout`; expiry cancels the query server-side and discards the connection
- ✅ **Fail-closed `cancel()`** — async `Client::cancel` and sync `SyncClient::cancel` never cancel anything: they return `Error::Config` with guidance (deadlines and stream teardown are the real mechanisms)
- ✅ **Pool acquire timeout** — bounded wait for a free pool slot (`Error::PoolTimeout`)
- ✅ **Response-size budgets** — `max_response_size` caps the bytes retained by accumulating reads; streaming paths stay unbudgeted by design
- ✅ **Bounded protocol framing** — every server-controlled count/length is validated before it sizes an allocation (hostile-server safe)
- ✅ **Batch queries** — multiple queries, single round-trip
- ✅ **Streaming** — `RowCursor` with background I/O prefetch
- ✅ **INSERT** — native protocol with `FORMAT Native`, streaming insert
- ✅ **Parameterized queries** — `{name:Type}` placeholders with server-side binding
- ✅ **External tables** — query-level table data for JOINs
- ✅ **Query callbacks** — progress, profile, log, profile_events
- ✅ **Compression** — LZ4, ZSTD, or none
- ✅ **TLS** — rustls-based, optional feature, CA-verified or custom roots
- ✅ **SSH authentication** — public-key challenge/response
- ✅ **Prometheus metrics** — queries, errors, pool utilization, retries

### Safety
- ✅ **`unsafe` used only where necessary** — unsafe blocks are documented with `// SAFETY:` comments
- ✅ **No unchecked `from_utf8`** — all server-provided strings checked for valid UTF-8
- ✅ **`overflow-checks = true`** in all profiles (including release)
- ✅ **`unwrap_used = "deny"`**, **`panic = "deny"`** clippy lints
- ✅ **`cargo-deny`** — checks advisories, sources, and licenses in CI
- ✅ **`cargo-fuzz`** — protocol parsers fuzzed (varint, block headers, exception chains)
- ✅ **Security review** — TLS, parser bounds, UTF-8 validation, credential zeroing, and panic-free paths reviewed

### Python Bindings
- ✅ **Sync + Async** — `Client` (sync) and `AsyncClient` via thread-per-query
- ✅ **4 output shapes** — `query()` (dicts), `query_tuples()`, `query_columns()`, `query_blocks()`
- ✅ **Streaming** — `query_stream()` for large results
- ✅ **Cancellation that works** — `cancel()` fails closed with guidance; cancelling the awaiting task or abandoning a stream kills the query's connection and the server aborts the query
- ✅ **TLS** — `tls=True` with CA file and client certificate
- ✅ **Per-query settings** — `settings={"key": "val"}` overlaid per query, never leaking onto the connection
- ✅ **Server-side parameters** — `params={"id": 42}`
- ✅ **Connection pool** — health checks, idle reaper, metrics
- ✅ **Error hierarchy** — `ProtocolError`, `ConnectionError`, `AuthenticationError`, `QueryError`, etc.
- ✅ **Type hints** — `py.typed` marker included for IDE support

---

## Compatibility

Tested against ClickHouse **24.8**, **25.8**, **26.4**, and **`latest`** in CI.

End-to-end coverage spans the native client surface: connection and
authentication (TCP, TLS, SSH-key), the full type matrix, bidirectional
LZ4/Zstd compression (including multi-frame blocks above 1 MiB), streaming
reads and block insertions, batched and parameterized queries, per-query
settings, progress/log/profile callbacks, connection pooling with failover
and circuit breaking, connect/query/acquire timeouts, and query cancellation
— across both Rust engines and the Python bindings. Not covered: external
tables (API present, e2e test pending), Kerberos/interserver
authentication, and the HTTP interface (native protocol only by design).

Run locally:
```bash
./tests/compatibility/run.sh
```

## Release

Rust users depend on one public crate:

```toml
st-clickhouse-lib = { version = "0.3", features = ["derive", "tls", "lz4"] }
```

The Rust import path is `st_clickhouse`. The `st-clickhouse-derive` crate is an implementation detail required by Rust's proc-macro model and is pulled in by the `derive` feature.

Release tags (`v*`) publish:

- `st-clickhouse-derive` to crates.io first.
- `st-clickhouse-lib` to crates.io after the derive crate appears in the index.
- `st-clickhouse-py` wheels to PyPI via Trusted Publishing / OIDC.

---

## Contributing

```bash
# Build and test
cargo test --workspace --all-features

# Run integration tests (requires ClickHouse on :9000)
docker run -d --name ch -p 9000:9000 \
  clickhouse/clickhouse-server:26.4
cargo test --test '*'

# Python tests
cd st-clickhouse-py
uv run --extra test maturin develop --release
uv run --extra test python -m pytest

# Fuzz testing
cd fuzz
cargo fuzz run varint
cargo fuzz run block
cargo fuzz run exception

# Soak testing
cargo test --test soak_test -- --ignored --test-threads=1
```

---

## License

Licensed under the [Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE).
