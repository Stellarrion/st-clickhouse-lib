# st-clickhouse-py

[![PyPI](https://img.shields.io/pypi/v/st-clickhouse-py.svg?style=flat-square)](https://pypi.org/project/st-clickhouse-py/)
[![PyPI downloads](https://img.shields.io/pypi/dm/st-clickhouse-py.svg?style=flat-square)](https://pypi.org/project/st-clickhouse-py/)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg?style=flat-square)](https://pypi.org/project/st-clickhouse-py/)
[![Free Threading 3.14t+](https://img.shields.io/badge/free--threading-3.14t%2B-green.svg?style=flat-square)](#free-threaded-python-314t)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg?style=flat-square)](https://github.com/Stellarrion/st-clickhouse-lib/blob/main/NOTICE)
[![CI](https://github.com/Stellarrion/st-clickhouse-lib/actions/workflows/ci.yml/badge.svg)](https://github.com/Stellarrion/st-clickhouse-lib/actions/workflows/ci.yml)
[![Releases](https://img.shields.io/github/v/release/Stellarrion/st-clickhouse-lib.svg?style=flat-square)](https://github.com/Stellarrion/st-clickhouse-lib/releases)

**Python bindings for the ClickHouse native TCP protocol - 100% Rust core via PyO3.**

A high-performance ClickHouse client that speaks the native protocol directly.
No HTTP, no REST, no SQL parsing in Python - the entire protocol is implemented
in Rust with Python bindings via PyO3.

## Features

- **Native TCP protocol** - direct connection to ClickHouse, no HTTP overhead
- **Sync + Async** - `Client` (sync) and `AsyncClient` (async) APIs
- **Connection pooling** - transparent pool (2-8 connections, health checks, idle reaper)
- **True streaming** - Rust reader thread + bounded channel, zero thread pool overhead
- **Type-aware conversion** - `Date` to `datetime.date`, `DateTime` to `datetime.datetime`,
  `UUID` to string, `IPv4/IPv6` to strings
- **Column-oriented access** - `Block` and `Column` objects for efficient columnar data access
- **GIL-free I/O** - all blocking operations release the GIL during network reads/writes
- **Cancellation** - cancelling a task aborts its server-side query (the pooled
  connection is killed and transparently replaced); `cancel()` fails closed
  with guidance
- **uvloop compatible** - standard asyncio APIs only
- **Python 3.12+** (GIL builds, abi3 wheels) and **free-threaded Python 3.14t+** (version-specific `cp3XXt` wheels). Free threading is supported from 3.14 onward; 3.13 free-threaded builds are not supported (pyo3 0.29 dropped them).
- **Compression** - LZ4 and ZSTD support

## Quick Start

```python
import st_clickhouse as ch

# Sync
with ch.connect("127.0.0.1:9000") as client:
    rows = client.query("SELECT number FROM system.numbers LIMIT 5")
    for row in rows:
        print(row)

# Async
import asyncio

async def main():
    async with ch.connect_async(
        "127.0.0.1:9000",
        pool_min_size=2,
        pool_max_size=8,
    ) as client:
        rows = await client.query("SELECT 1 AS x")
        print(rows)

asyncio.run(main())

# Streaming (sync)
for block in client.query_stream("SELECT * FROM huge_table"):
    col_a = block["a"]
    values = col_a.to_list()  # → [1, 2, 3, ...]

# Streaming (async) — zero thread pool threads blocked
async for block in client.query_stream("SELECT * FROM huge_table"):
    process(block)
```

## Installation

```bash
pip install st-clickhouse-py
```

Or build from source:

```bash
pip install maturin
cd st-clickhouse-py
maturin build --release
pip install target/wheels/st_clickhouse_py-*.whl
```

## API Overview

### Sync Client

| Method | Description |
|--------|-------------|
| `execute(query)` | DDL/DML, no result rows |
| `query(query)` | SELECT → list of dicts |
| `query_blocks(query)` | SELECT → list of `Block` objects |
| `query_stream(query)` | SELECT → iterator of `Block` objects |
| `insert(query, rows)` | INSERT from list of dicts |
| `insert_blocks(query, table, blocks)` | INSERT from `Block` objects |
| `insert_stream(query)` → `InsertStream` | Streaming INSERT session |
| `ping()` | Health check |
| `cancel()` | Always raises `RuntimeError` — see [Cancellation](#cancellation) |
| `server_info()` | Server metadata (cached) |
| `set_setting(name, value)` | Session setting |

### Async Client

Same methods prefixed with `async`/`await`, plus connection pooling:

```python
client = AsyncClient(
    "127.0.0.1:9000",
    pool_min_size=2,
    pool_max_size=8,
    pool_acquire_timeout=30.0,
    pool_health_check_interval=30.0,
    pool_max_idle_time=300.0,
)
```

### Cancellation

`cancel()` on `Client`, `AsyncClient`, and `AsyncSession` always raises
`RuntimeError`. A Cancel packet can only be delivered over the connection
running the query, and that connection is blocked inside the query call;
sending it anywhere else poisons idle connections. Use the real mechanisms
instead:

- **Cancel the awaiting task** (`task.cancel()`): the pooled connection
  running the query is killed immediately — the server aborts the query —
  and the pool transparently creates a replacement on the next acquire. The
  task unwinds in O(1) instead of waiting for the query.
- **Abandon a stream**: `break` out of `query_stream` (sync or async). If
  the response never reached its terminal packet (EndOfStream or server
  exception), the connection is killed the same way. The async pool replaces
  it; the sync `Client` is closed and must be recreated (its single
  connection was left mid-response).
- **Query deadline**: `Client(..., query_timeout=...)` bounds every query.

A stream that is fully consumed releases its connection cleanly and keeps
the client/pool usable.

### Connection Pool

The pool manages TCP connections transparently:

- **Acquire/release**: each operation gets a connection, uses it, returns it
- **Lazy growth**: connections created on demand up to `pool_max_size`
- **Health checks**: idle connections are pinged before being served
- **Idle reaper**: background thread closes stale connections above `min_size`
- **Backpressure**: `asyncio.Queue` + Rust channel — if consumer is slow, the
  reader thread blocks on TCP write, telling ClickHouse to slow down

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     PYTHON                                  │
│  AsyncClient                                                │
│    ├── query/execute → Pool.acquire() → client.query()      │
│    │                              (GIL released during I/O) │
│    └── query_stream → Pool.acquire() → [held for stream]    │
│         └── Rust Reader Thread → mpsc Channel → Forwarder   │
│             (TCP I/O, no GIL)    (bounded 32)  → Queue      │
│                                                    ↓        │
│                                              async for       │
└─────────────────────────────────────────────────────────────┘
```

## Running Tests

Requires a running ClickHouse server:

```bash
# Using Docker
docker run -d -p 9000:9000 --name ch-test clickhouse/clickhouse-server

uv run --extra test maturin develop --release
uv run --extra test python -m pytest
```

## Performance

- **Single query**: ~50μs overhead over raw TCP (PyO3 FFI + type conversion)
- **Streaming**: zero copy per block (Rust reader thread → Python `async for`)
- **Concurrent**: up to `pool_max_size` parallel queries on separate TCP connections
- **GIL release**: all I/O-bound operations release the GIL

## License

Apache-2.0
