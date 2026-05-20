"""
st-clickhouse — ClickHouse native protocol client.

A high-performance ClickHouse client using the native TCP protocol.
100% Rust core with Python bindings via PyO3.

Quick start:

    import st_clickhouse as ch

    # Sync
    with ch.connect("127.0.0.1:9000") as client:
        rows = client.query("SELECT number, number * 2 AS double FROM system.numbers LIMIT 5")
        for row in rows:
            print(row["number"], row["double"])

    # Async — connection pool (2-8 connections, transparent)
    import asyncio

    async def main():
        async with ch.connect_async("127.0.0.1:9000") as client:
            rows = await client.query("SELECT 1 AS x")
            print(rows)

    asyncio.run(main())

"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from typing import Any, Callable, Dict, Iterable, List, Optional, Iterator, AsyncIterator, Tuple

from ._errors import (
    AuthenticationError,
    ClickHouseError,
    CompressionError,
    ConfigError,
    ConnectionError,
    ProtocolError,
    QueryError,
    TimeoutError,
    map_error as _map_error,
)
from ._pool import ConnectionPool as _ConnectionPool
from ._session import AsyncSession
from ._streams import AsyncInsertStream, InsertStream
from ._utils import (
    merge_query_params as _merge_query_params,
    parse_connect_args as _parse_connect_args,
    with_per_query_settings as _with_per_query_settings,
)

# Import the native Rust extension
from st_clickhouse._native import (  # pyrefly: ignore
    _Client as _NativeClient,
    _Block as _NativeBlock,
    _Column as _NativeColumn,
    _RowIterator as _NativeRowIterator,
    _QueryStream as _NativeQueryStream,
    blocks_to_dicts,
    dicts_to_block,
)

# Re-export native types
Block = _NativeBlock
Column = _NativeColumn

__version__ = _NativeClient.__module__  # will be overridden below

__all__ = [
    "connect",
    "connect_async",
    "Client",
    "AsyncClient",
    "AsyncSession",
    "Block",
    "Column",
    "ClickHouseError",
    "ProtocolError",
    "ConnectionError",
    "QueryError",
    "AuthenticationError",
    "TimeoutError",
    "CompressionError",
    "ConfigError",
    "blocks_to_dicts",
    "dicts_to_block",
]

# ══════════════════════════════════════════════════════════════════════════
# Version
# ══════════════════════════════════════════════════════════════════════════

try:
    from st_clickhouse._native import __version__ as _native_version
    __version__ = _native_version
except ImportError:
    __version__ = "0.1.0"


# ══════════════════════════════════════════════════════════════════════════
# Sync Client — high-level wrapper around native _Client
# ══════════════════════════════════════════════════════════════════════════

class Client:
    """High-performance synchronous ClickHouse client.

    Args:
        addr: Host and port (e.g., ``"127.0.0.1:9000"``).
        user: ClickHouse username (default: "default").
        password: ClickHouse password (default: "").
        database: Default database (default: "").
        settings: ClickHouse session settings dict.
        compression: Compression method — ``"lz4"``, ``"zstd"``, or None.
        connect_timeout: Connect timeout in seconds (default: 10.0).
        query_timeout: Query timeout in seconds (default: 300.0).
        tls: Enable TLS encryption (default: False). Uses system CA store.
        tls_domain: TLS SNI hostname override (default: parsed from addr).
        tls_ca_file: Path to custom CA certificate file.
        tls_client_cert: Path to client certificate for mutual TLS.
        tls_client_key: Path to client private key for mutual TLS.
        ssh_signer: Callable receiving ClickHouse challenge bytes and returning
            an SSH-key signature string for native SSH authentication.
        validate_schema: Validate native INSERT blocks against cached
            ``DESCRIBE TABLE`` metadata.

    Usage:

        with Client("127.0.0.1:9000") as client:
            rows = client.query("SELECT 1 AS x")
            print(rows)  # [{"x": 1}]

        with Client("example.com:9440", tls=True) as client:
            rows = client.query("SELECT 1")
    """

    def __init__(
        self,
        addr: str,
        *,
        user: str = "default",
        password: str = "",
        database: str = "",
        settings: Optional[Dict[str, str]] = None,
        compression: Optional[str] = None,
        connect_timeout: float = 10.0,
        query_timeout: float = 300.0,
        max_response_size: int = 256 * 1024 * 1024,
        tls: bool = False,
        tls_domain: Optional[str] = None,
        tls_ca_file: Optional[str] = None,
        tls_client_cert: Optional[str] = None,
        tls_client_key: Optional[str] = None,
        ssh_signer: Optional[Callable[[bytes], str]] = None,
        validate_schema: bool = False,
    ):
        self._closed: bool = True
        # Build kwargs, filtering out None TLS params
        native_kwargs = dict(
            addr=addr,
            user=user,
            password=password,
            database=database,
            settings=settings or {},
            compression=compression,
            connect_timeout=connect_timeout,
            query_timeout=query_timeout,
            max_response_size=max_response_size,
            ssh_signer=ssh_signer,
            validate_schema=validate_schema,
        )
        if tls:
            native_kwargs['tls'] = True
            if tls_domain:
                native_kwargs['tls_domain'] = tls_domain
            if tls_ca_file:
                native_kwargs['tls_ca_file'] = tls_ca_file
            if tls_client_cert:
                native_kwargs['tls_client_cert'] = tls_client_cert
            if tls_client_key:
                native_kwargs['tls_client_key'] = tls_client_key
        try:
            self._client = _NativeClient(**native_kwargs)
        except Exception as e:
            raise _map_error(e) from e
        self._closed: bool = False

    def execute(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, str]] = None,
        ignored_part_uuids: Optional[Iterable[Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Execute a DDL/DML query. No result rows.

        Args:
            query: SQL statement to execute (CREATE, ALTER, INSERT, etc.)
            settings: Per-query ClickHouse settings (applied temporarily).

        Raises:
            QueryError: If the server returns an error.
            ConnectionError: If the connection is lost.
        """
        self._check_open()
        bound_params = _merge_query_params(params, kwargs)
        _with_per_query_settings(
            self._client,
            settings or {},
            lambda: self._client.execute(query, bound_params, ignored_part_uuids),
        )

    def query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, str]] = None,
        ignored_part_uuids: Optional[Iterable[Any]] = None,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """Execute a SELECT query. Returns list of row dicts.

        Types are automatically converted:
            - ``Date`` / ``Date32`` → ``datetime.date``
            - ``DateTime`` / ``DateTime64`` → ``datetime.datetime`` (UTC)
            - ``UUID`` → ``uuid.UUID``
            - ``IPv4`` / ``IPv6`` → ``str`` (dot/colon notation)
            - ``Decimal`` → ``decimal.Decimal``
            - ``Enum`` → ``str``
            - ``Nullable(T)`` → ``None`` or converted value

        Args:
            query: SELECT SQL statement.
                Use ``{name:Type}`` placeholders for server-side parameters:
                ``query("SELECT {id:UInt64} AS val", params={"id": 42})``
            params: Optional dict mapping parameter names to values.
            settings: Per-query ClickHouse settings (applied temporarily).
                Example: ``{"max_threads": "8", "optimize_if_chain_to_multiif": "0"}``
            ignored_part_uuids: Optional iterable of UUIDs to ignore during
                replicated INSERT deduplication.

        Returns:
            List of dictionaries (one per row, column names as keys).

        Example:
            Basic usage::

                client = Client("localhost:9000")
                rows = client.query("SELECT count() AS cnt FROM system.tables")
                print(rows[0]["cnt"])  # 42

            With server-side parameters::

                rows = client.query(
                    "SELECT {id:UInt64} AS val, {name:String} AS label",
                    params={"id": 1, "name": "hello"},
                )

            With per-query settings::

                rows = client.query(
                    "SELECT * FROM big_table",
                    settings={"max_threads": "8"},
                )

            ``query()`` is best for small to medium result sets (up to ~100K rows).
            For larger results, use ``query_blocks()`` or ``query_tuples()``
            to avoid building a large list of dicts.
        """""
        self._check_open()
        bound_params = _merge_query_params(params, kwargs)
        return _with_per_query_settings(
            self._client,
            settings or {},
            lambda: self._client.query(query, bound_params, ignored_part_uuids),
        )

    def query_blocks(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, str]] = None,
        ignored_part_uuids: Optional[Iterable[Any]] = None,
        **kwargs: Any,
    ) -> List[Block]:
        """Execute a SELECT query. Returns list of Block objects.

        More efficient for large result sets — access columns lazily.
        """
        self._check_open()
        bound_params = _merge_query_params(params, kwargs)
        return _with_per_query_settings(
            self._client,
            settings or {},
            lambda: self._client.query_blocks(query, bound_params, ignored_part_uuids),
        )

    def query_tuples(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, str]] = None,
        ignored_part_uuids: Optional[Iterable[Any]] = None,
        **kwargs: Any,
    ) -> List[Tuple[Any, ...]]:
        """Execute a SELECT query. Returns rows as tuples.

        This avoids one Python dict allocation per row and is faster than
        :meth:`query` when column names are not needed on every row.
        """
        self._check_open()
        bound_params = _merge_query_params(params, kwargs)
        return _with_per_query_settings(
            self._client,
            settings or {},
            lambda: self._client.query_tuples(query, bound_params, ignored_part_uuids),
        )

    def query_columns(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, str]] = None,
        ignored_part_uuids: Optional[Iterable[Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, List[Any]]:
        """Execute a SELECT query. Returns ``{column_name: list[values]}``.

        This is the fastest fully materialized Python representation. For the
        lowest allocation path, use :meth:`query_blocks` and access columns
        lazily.
        """
        self._check_open()
        bound_params = _merge_query_params(params, kwargs)
        return _with_per_query_settings(
            self._client,
            settings or {},
            lambda: self._client.query_columns(query, bound_params, ignored_part_uuids),
        )

    def query_stream(self, query: str) -> Iterator[Block]:
        """Stream query results block by block.

        Uses a Rust background reader thread + channel internally.
        The reader thread holds no Python objects — pure Rust I/O.
        """
        self._check_open()
        return self._client.query_stream(query)

    def insert(self, query: str, rows: List[Dict[str, Any]]) -> None:
        """Insert rows into a table from a list of dicts.

        Automatically infers column types from the server via DESCRIBE TABLE,
        builds native protocol blocks, and inserts in one batch.
        """
        self._check_open()
        if not rows:
            return

        import re
        col_names = list(rows[0].keys())
        table_match = re.search(r"INSERT\s+INTO\s+(\S+)", query, re.IGNORECASE)

        columns: list[tuple[str, str]] = []
        if table_match:
            try:
                desc = self.query(f"DESCRIBE TABLE {table_match.group(1)}")
                type_map = {row["name"]: row["type"] for row in desc}
                columns = [(n, type_map.get(n, "String")) for n in col_names]
            except Exception:
                columns = [(n, "String") for n in col_names]
        else:
            columns = [(n, "String") for n in col_names]

        block = dicts_to_block(rows, columns)
        self._client.insert(query, "", [block])

    def insert_blocks(self, query: str, table_name: str, blocks: List[Block]) -> None:
        """Insert blocks into a table using native protocol."""
        self._check_open()
        try:
            self._client.insert(query, table_name, blocks)
        except Exception as e:
            raise _map_error(e) from e

    def ping(self) -> bool:
        """Ping the server. Returns True on success."""
        self._check_open()
        try:
            return self._client.ping()
        except Exception as e:
            raise _map_error(e) from e

    def cancel(self) -> None:
        """Cancel the currently running query."""
        self._check_open()
        try:
            self._client.cancel()
        except Exception as e:
            raise _map_error(e) from e

    def tables_status(
        self, tables: Iterable[Tuple[str, str]]
    ) -> Dict[Tuple[str, str], Dict[str, Any]]:
        """Get replication/read-only status for tables.

        Args:
            tables: Iterable of ``(database, table)`` pairs.
        """
        self._check_open()
        try:
            return self._client.tables_status(list(tables))
        except Exception as e:
            raise _map_error(e) from e

    def table_status(
        self, database: str, table: str
    ) -> Optional[Dict[str, Any]]:
        """Get replication/read-only status for one table."""
        self._check_open()
        try:
            return self._client.table_status(database, table)
        except Exception as e:
            raise _map_error(e) from e

    def schema_for_table(self, table: str) -> Dict[str, Any]:
        """Return cached table schema metadata from ``DESCRIBE TABLE``."""
        self._check_open()
        try:
            return self._client.schema_for_table(table)
        except Exception as e:
            raise _map_error(e) from e

    def refresh_schema_for_table(self, table: str) -> Dict[str, Any]:
        """Refresh and return table schema metadata from ``DESCRIBE TABLE``."""
        self._check_open()
        try:
            return self._client.refresh_schema_for_table(table)
        except Exception as e:
            raise _map_error(e) from e

    def clear_schema_cache(self) -> None:
        """Clear cached table schema metadata."""
        self._check_open()
        self._client.clear_schema_cache()

    def server_info(self) -> Dict[str, Any]:
        """Get server information (cached from handshake, no I/O).

        Returns:
            Dict with keys: ``name``, ``version_major``, ``version_minor``,
            ``revision``, ``timezone``, ``display_name``.
        """
        self._check_open()
        try:
            return self._client.server_info()
        except Exception as e:
            raise _map_error(e) from e

    def insert_stream(self, query: str, table_name: str = "") -> InsertStream:
        """Start a streaming INSERT session (sync)."""
        self._check_open()
        self._client.begin_insert_stream(query)
        return InsertStream(self._client, table_name)

    def set_setting(self, name: str, value: str) -> None:
        """Set a ClickHouse session setting at runtime."""
        self._check_open()
        self._client.set_setting(name, value)

    @property
    def closed(self) -> bool:
        return getattr(self, "_closed", True)

    def close(self) -> None:
        if not getattr(self, "_closed", True):
            self._closed = True
            # Immediately drop native client to close the TCP connection.
            if hasattr(self, '_client') and self._client is not None:
                del self._client

    def __del__(self) -> None:
        self.close()

    def _check_open(self) -> None:
        if getattr(self, "_closed", True):
            raise ConnectionError("Connection is closed")

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        try:
            return f"<Client {self._client}>"
        except Exception:
            return "<Client (closed)>"


# ══════════════════════════════════════════════════════════════════════════
# Async Client — connection pool + sync Rust core + asyncio bridge
# ══════════════════════════════════════════════════════════════════════════

class AsyncClient:
    """Async ClickHouse client with transparent connection pooling.

    Architecture:

    ::

        asyncio tasks
            │
            ├── query/execute/ping ──► Pool.acquire() ──► client.query() ──► Pool.release()
            │                               (thread pool, GIL released)
            │
            └── query_stream ──► Pool.acquire() ──► [Rust Reader] ──► [Forwarder] ──► Queue ──► async for
                                     (held for stream)   (no GIL)    (lock held)

    - A connection pool (``pool_min_size`` .. ``pool_max_size``) manages
      multiple TCP connections to ClickHouse.
    - Each one-shot operation (query, execute, ping) acquires a connection
      from the pool, uses it (GIL released), and returns it.
    - Each stream operation (query_stream) acquires and HOLDS a connection
      for the stream duration — the forwarder thread holds it.
    - Backpressure: Rust mpsc channel (32) → asyncio.Queue (32).
      If consumer is slow, the Rust reader thread blocks on TCP write,
      telling the ClickHouse server to slow down.
    - Cancellation: ``asyncio.CancelledError`` sets a threading ``Event``,
      the forwarder thread polls it during backpressure waits, and the
      stream is cancelled via ``_QueryStream.cancel()``.
    - ``GeneratorExit`` (from ``async for`` break or GC): same cleanup path.
    - Compatible with ``uvloop`` (standard asyncio APIs only).

    Args:
        addr: Host and port (e.g., ``"127.0.0.1:9000"``).
        user: ClickHouse username (default: "default").
        password: ClickHouse password (default: "").
        database: Default database.
        settings: ClickHouse session settings dict.
        compression: Compression method — ``"lz4"``, ``"zstd"``, or ``None``.
        connect_timeout: Connect timeout in seconds (default: 10.0).
        query_timeout: Query timeout in seconds (default: 300.0).
        ssh_signer: Callable receiving ClickHouse challenge bytes and returning
            an SSH-key signature string for native SSH authentication.
        pool_min_size: Minimum connections in pool (default: 2).
        pool_max_size: Maximum connections in pool (default: 8).
        pool_acquire_timeout: Max seconds to wait for a connection (default: 30.0).

    Usage:

        # Simple queries — pool managed transparently
        async with AsyncClient("127.0.0.1:9000") as client:
            rows = await client.query("SELECT 1 AS x")

            # Streaming — ZERO thread pool threads blocked
            async for block in client.query_stream("SELECT * FROM huge_table"):
                process(block)

        # In a web framework, share one AsyncClient across all requests.
        # The pool handles concurrent requests up to pool_max_size.
    """

    def __init__(
        self,
        addr: str,
        *,
        user: str = "default",
        password: str = "",
        database: str = "",
        settings: Optional[Dict[str, str]] = None,
        compression: Optional[str] = None,
        connect_timeout: float = 10.0,
        query_timeout: float = 300.0,
        max_response_size: int = 256 * 1024 * 1024,
        pool_min_size: int = 2,
        pool_max_size: int = 8,
        pool_acquire_timeout: float = 30.0,
        pool_health_check_interval: float = 30.0,
        pool_max_idle_time: float = 300.0,
        pool_reaper_interval: float = 60.0,
        ssh_signer: Optional[Callable[[bytes], str]] = None,
        validate_schema: bool = False,
    ):
        self._closed: bool = True
        self._addr = addr
        self._create_kwargs = {
            "user": user,
            "password": password,
            "database": database,
            "settings": settings,
            "compression": compression,
            "connect_timeout": connect_timeout,
            "query_timeout": query_timeout,
            "max_response_size": max_response_size,
            "ssh_signer": ssh_signer,
            "validate_schema": validate_schema,
        }

        def _make():
            try:
                return _NativeClient(addr, **self._create_kwargs)
            except Exception as e:
                raise _map_error(e) from e

        try:
            self._pool = _ConnectionPool(
                _make,
                min_size=pool_min_size,
                max_size=pool_max_size,
                acquire_timeout=pool_acquire_timeout,
                health_check_interval=pool_health_check_interval,
                max_idle_time=pool_max_idle_time,
                reaper_interval=pool_reaper_interval,
            )
        except Exception as e:
            raise _map_error(e) from e
        self._closed: bool = False

    # ── One-shot operations ─────────────────────────────────────────

    async def execute(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        ignored_part_uuids: Optional[Iterable[Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Execute a DDL/DML query. No result rows.

        Acquires a connection from the pool, releases after execution.
        GIL released during TCP I/O.
        """
        self._check_open()
        loop = asyncio.get_running_loop()
        bound_params = _merge_query_params(params, kwargs)
        fut = loop.run_in_executor(
            None, self._sync_execute, query, bound_params, ignored_part_uuids
        )
        try:
            await fut
        except asyncio.CancelledError:
            await self.cancel()
            raise
        except Exception as e:
            raise _map_error(e) from e

    def _sync_execute(
        self,
        query: str,
        params: Dict[str, Any],
        ignored_part_uuids: Optional[Iterable[Any]],
    ) -> None:
        client = self._pool.acquire()
        try:
            client.execute(query, params, ignored_part_uuids)
        finally:
            self._pool.release(client)

    async def query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        ignored_part_uuids: Optional[Iterable[Any]] = None,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """Execute a SELECT query. Returns list of row dicts.

        Acquires and releases a connection from the pool.
        """
        self._check_open()
        loop = asyncio.get_running_loop()
        bound_params = _merge_query_params(params, kwargs)
        fut = loop.run_in_executor(
            None, self._sync_query, query, bound_params, ignored_part_uuids
        )
        try:
            return await fut
        except asyncio.CancelledError:
            await self.cancel()
            raise
        except Exception as e:
            raise _map_error(e) from e

    def _sync_query(
        self,
        query: str,
        params: Dict[str, Any],
        ignored_part_uuids: Optional[Iterable[Any]],
    ) -> List[Dict[str, Any]]:
        client = self._pool.acquire()
        try:
            return client.query(query, params, ignored_part_uuids)
        finally:
            self._pool.release(client)

    async def query_tuples(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        ignored_part_uuids: Optional[Iterable[Any]] = None,
        **kwargs: Any,
    ) -> List[Tuple[Any, ...]]:
        """Execute a SELECT query. Returns rows as tuples."""
        self._check_open()
        loop = asyncio.get_running_loop()
        bound_params = _merge_query_params(params, kwargs)
        fut = loop.run_in_executor(
            None, self._sync_query_tuples, query, bound_params, ignored_part_uuids
        )
        try:
            return await fut
        except asyncio.CancelledError:
            await self.cancel()
            raise
        except Exception as e:
            raise _map_error(e) from e

    def _sync_query_tuples(
        self,
        query: str,
        params: Dict[str, Any],
        ignored_part_uuids: Optional[Iterable[Any]],
    ) -> List[Tuple[Any, ...]]:
        client = self._pool.acquire()
        try:
            return client.query_tuples(query, params, ignored_part_uuids)
        finally:
            self._pool.release(client)

    async def query_columns(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        ignored_part_uuids: Optional[Iterable[Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, List[Any]]:
        """Execute a SELECT query. Returns ``{column_name: list[values]}``."""
        self._check_open()
        loop = asyncio.get_running_loop()
        bound_params = _merge_query_params(params, kwargs)
        fut = loop.run_in_executor(
            None, self._sync_query_columns, query, bound_params, ignored_part_uuids
        )
        try:
            return await fut
        except asyncio.CancelledError:
            await self.cancel()
            raise
        except Exception as e:
            raise _map_error(e) from e

    def _sync_query_columns(
        self,
        query: str,
        params: Dict[str, Any],
        ignored_part_uuids: Optional[Iterable[Any]],
    ) -> Dict[str, List[Any]]:
        client = self._pool.acquire()
        try:
            return client.query_columns(query, params, ignored_part_uuids)
        finally:
            self._pool.release(client)

    async def query_blocks(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        ignored_part_uuids: Optional[Iterable[Any]] = None,
        **kwargs: Any,
    ) -> List[Block]:
        """Execute a SELECT query. Returns list of Block objects.

        Acquires and releases a connection from the pool.
        """
        self._check_open()
        loop = asyncio.get_running_loop()
        bound_params = _merge_query_params(params, kwargs)
        fut = loop.run_in_executor(
            None, self._sync_query_blocks, query, bound_params, ignored_part_uuids
        )
        try:
            return await fut
        except asyncio.CancelledError:
            await self.cancel()
            raise
        except Exception as e:
            raise _map_error(e) from e

    def _sync_query_blocks(
        self,
        query: str,
        params: Dict[str, Any],
        ignored_part_uuids: Optional[Iterable[Any]],
    ) -> List[Block]:
        client = self._pool.acquire()
        try:
            return client.query_blocks(query, params, ignored_part_uuids)
        finally:
            self._pool.release(client)

    async def insert(self, query: str, rows: List[Dict[str, Any]]) -> None:
        """Insert rows into a table from a list of dicts.

        Acquires and releases a connection from the pool.
        """
        self._check_open()
        if not rows:
            return

        import re
        col_names = list(rows[0].keys())
        table_match = re.search(r"INSERT\s+INTO\s+(\S+)", query, re.IGNORECASE)

        columns: list[tuple[str, str]] = []
        if table_match:
            try:
                desc = await self.query(f"DESCRIBE TABLE {table_match.group(1)}")
                type_map = {row["name"]: row["type"] for row in desc}
                columns = [(n, type_map.get(n, "String")) for n in col_names]
            except Exception:
                columns = [(n, "String") for n in col_names]
        else:
            columns = [(n, "String") for n in col_names]

        block = dicts_to_block(rows, columns)
        await self.insert_blocks(query, "", [block])

    async def insert_blocks(
        self, query: str, table_name: str, blocks: List[Block]
    ) -> None:
        """Insert blocks into a table using native protocol.

        Acquires and releases a connection from the pool.
        """
        self._check_open()
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(
            None, self._sync_insert_blocks, query, table_name, blocks
        )
        try:
            await fut
        except asyncio.CancelledError:
            await self.cancel()
            raise
        except Exception as e:
            raise _map_error(e) from e

    def _sync_insert_blocks(
        self, query: str, table_name: str, blocks: List[Block]
    ) -> None:
        client = self._pool.acquire()
        try:
            client.insert(query, table_name, blocks)
        finally:
            self._pool.release(client)

    async def ping(self) -> bool:
        """Ping the server. Returns True on success.

        Acquires and releases a connection from the pool.
        """
        self._check_open()
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(None, self._sync_ping)
        except Exception as e:
            raise _map_error(e) from e

    def _sync_ping(self) -> bool:
        client = self._pool.acquire()
        try:
            return client.ping()
        finally:
            self._pool.release(client)

    async def cancel(self) -> None:
        """Cancel the currently running query on all pool connections.

        Sends cancel to each connection. The pool is NOT released
        during cancel (pool management is independent).
        """
        self._check_open()
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self._sync_cancel_all)
        except Exception as e:
            raise _map_error(e) from e

    def _sync_cancel_all(self) -> None:
        """Send cancel on all pool connections (by iterating allocated clients)."""
        for pc in self._pool._all[:]:
            try:
                pc.client.cancel()
            except Exception:
                pass  # Best-effort cancel

    async def tables_status(
        self, tables: Iterable[Tuple[str, str]]
    ) -> Dict[Tuple[str, str], Dict[str, Any]]:
        """Get replication/read-only status for tables."""
        self._check_open()
        loop = asyncio.get_running_loop()
        table_list = list(tables)
        try:
            return await loop.run_in_executor(
                None, self._sync_tables_status, table_list
            )
        except Exception as e:
            raise _map_error(e) from e

    def _sync_tables_status(
        self, tables: List[Tuple[str, str]]
    ) -> Dict[Tuple[str, str], Dict[str, Any]]:
        client = self._pool.acquire()
        try:
            return client.tables_status(tables)
        finally:
            self._pool.release(client)

    async def table_status(
        self, database: str, table: str
    ) -> Optional[Dict[str, Any]]:
        """Get replication/read-only status for one table."""
        self._check_open()
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(
                None, self._sync_table_status, database, table
            )
        except Exception as e:
            raise _map_error(e) from e

    def _sync_table_status(
        self, database: str, table: str
    ) -> Optional[Dict[str, Any]]:
        client = self._pool.acquire()
        try:
            return client.table_status(database, table)
        finally:
            self._pool.release(client)

    async def schema_for_table(self, table: str) -> Dict[str, Any]:
        """Return cached table schema metadata from one pool connection."""
        self._check_open()
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(None, self._sync_schema_for_table, table)
        except Exception as e:
            raise _map_error(e) from e

    def _sync_schema_for_table(self, table: str) -> Dict[str, Any]:
        client = self._pool.acquire()
        try:
            return client.schema_for_table(table)
        finally:
            self._pool.release(client)

    async def refresh_schema_for_table(self, table: str) -> Dict[str, Any]:
        """Refresh table schema metadata on one pool connection."""
        self._check_open()
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(None, self._sync_refresh_schema_for_table, table)
        except Exception as e:
            raise _map_error(e) from e

    def _sync_refresh_schema_for_table(self, table: str) -> Dict[str, Any]:
        client = self._pool.acquire()
        try:
            return client.refresh_schema_for_table(table)
        finally:
            self._pool.release(client)

    async def clear_schema_cache(self) -> None:
        """Clear schema metadata caches on all currently allocated pool connections."""
        self._check_open()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._sync_clear_schema_cache)

    def _sync_clear_schema_cache(self) -> None:
        for pc in self._pool._all[:]:
            pc.client.clear_schema_cache()

    def session(self) -> AsyncSession:
        """Return an async context manager with connection affinity.

        Queries executed through the session use one pinned native connection,
        so temporary tables and connection-local settings remain visible until
        the context exits.
        """
        self._check_open()
        return AsyncSession(self)

    async def set_setting(self, name: str, value: str) -> None:
        """Set a ClickHouse session setting on the next acquired connection.

        Note: settings are per-connection. This sets on one connection
        from the pool.
        """
        self._check_open()
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self._sync_set_setting, name, value)
        except Exception as e:
            raise _map_error(e) from e

    def _sync_set_setting(self, name: str, value: str) -> None:
        client = self._pool.acquire()
        try:
            client.set_setting(name, value)
        finally:
            self._pool.release(client)

    async def server_info(self) -> Dict[str, Any]:
        """Get server information from one pool connection.

        Returns:
            Dict with keys: ``name``, ``version_major``, ``version_minor``,
            ``revision``, ``timezone``, ``display_name``.
        """
        self._check_open()
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(None, self._sync_server_info)
        except Exception as e:
            raise _map_error(e) from e

    def _sync_server_info(self) -> Dict[str, Any]:
        client = self._pool.acquire()
        try:
            return client.server_info()
        finally:
            self._pool.release(client)

    async def insert_stream(
        self, query: str, table_name: str = ""
    ) -> AsyncInsertStream:
        """Start a streaming INSERT session (async).

        Acquires and HOLDS a connection for the stream duration.

        Usage:
            async with client.insert_stream("INSERT INTO events VALUES") as stream:
                for chunk in event_stream:
                    block = dicts_to_block(chunk, columns)
                    await stream.send(block)
        """
        self._check_open()
        loop = asyncio.get_running_loop()
        # Acquire a connection and start the insert stream
        client = await loop.run_in_executor(None, self._pool.acquire)
        try:
            await loop.run_in_executor(
                None, lambda: client.begin_insert_stream(query)
            )
        except BaseException:
            self._pool.release(client)
            raise
        return AsyncInsertStream(self, client, query, table_name)

    # ── Streaming ───────────────────────────────────────────────────

    async def query_stream(self, query: str) -> AsyncIterator[Block]:
        """Stream query results block by block.

        **Zero thread pool threads are blocked** during streaming.

        Architecture:
        1. A connection is acquired from the pool and HELD for the stream
        2. A Rust reader thread reads TCP data and pushes blocks through
           an mpsc channel (capacity 32, backpressure)
        3. A single Python forwarder thread reads from the channel and
           pushes to ``asyncio.Queue`` via ``run_coroutine_threadsafe``
        4. The async generator yields from the queue
        5. On any exit (normal end, break, CancelledError, GeneratorExit,
           exception), the connection is returned to the pool

        Args:
            query: SELECT SQL statement.

        Yields:
            :class:`Block` objects one at a time.

        Usage:
            async for block in client.query_stream("SELECT a, b FROM t"):
                col_a = block["a"]
                values = col_a.to_list()
        """
        self._check_open()
        loop = asyncio.get_running_loop()

        # Acquire a connection from pool — HELD until stream ends
        client = await loop.run_in_executor(None, self._pool.acquire)

        # Start the stream (Rust reader thread + channel)
        try:
            stream = await loop.run_in_executor(
                None, lambda: client.query_stream(query)
            )
        except BaseException:
            self._pool.release(client)
            raise

        # Bridge: forwarder thread → asyncio.Queue → async generator
        _SENTINEL = object()
        queue: asyncio.Queue = asyncio.Queue(maxsize=32)
        cancel_event = threading.Event()
        forwarder_exc: list[BaseException] = []

        def _forwarder() -> None:
            """Dedicated forwarder thread.

            Holds the connection for the entire stream.
            Reads from the Rust channel, pushes to asyncio.Queue.
            Polls cancel_event during backpressure waits.
            """
            try:
                for block in stream:
                    if cancel_event.is_set():
                        return
                    # Non-blocking push with cancel polling
                    while True:
                        if cancel_event.is_set():
                            return
                        fut = asyncio.run_coroutine_threadsafe(
                            queue.put(block), loop
                        )
                        try:
                            # 100ms timeout allows cancel polling
                            fut.result(timeout=0.1)
                            break  # Put succeeded
                        except concurrent.futures.TimeoutError:
                            continue  # Retry (also re-checks cancel)
                        except BaseException:
                            return  # Queue closed / loop stopped
            except BaseException as e:
                forwarder_exc.append(e)
            finally:
                # Always signal end-of-stream
                try:
                    asyncio.run_coroutine_threadsafe(
                        queue.put(_SENTINEL), loop
                    ).result(timeout=5)
                except BaseException:
                    pass
                # Return connection to pool
                self._pool.release(client)

        thread = threading.Thread(target=_forwarder, daemon=True)
        thread.start()

        # Async generator body
        try:
            while True:
                item = await queue.get()
                if item is _SENTINEL:
                    break
                yield item

            # Re-raise any forwarder exception
            if forwarder_exc:
                raise forwarder_exc[0]

        except GeneratorExit:
            # async for break / early return / GC — clean up
            cancel_event.set()
            stream.cancel()
            raise

        except asyncio.CancelledError:
            cancel_event.set()
            stream.cancel()
            raise

        finally:
            # Safety net: ensure cleanup on ANY exit path
            if not cancel_event.is_set():
                cancel_event.set()
                stream.cancel()
            # stream.__del__() cleans up the Rust reader thread

    # ── Lifecycle ────────────────────────────────────────────────────

    @property
    def closed(self) -> bool:
        return getattr(self, "_closed", True)

    def close(self) -> None:
        if not getattr(self, "_closed", True):
            self._closed = True
            self._pool.close()

    def __del__(self) -> None:
        self.close()

    def _check_open(self) -> None:
        if getattr(self, "_closed", True):
            raise ConnectionError("Connection is closed")

    @property
    def metrics(self) -> Dict[str, Any]:
        self._check_open()
        return self._pool.metrics

    async def __aenter__(self) -> AsyncClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        try:
            m = self._pool.metrics
            return f"<AsyncClient pool={m['total']} active={m['in_use']}>"
        except Exception:
            return "<AsyncClient (closed)>"


# ══════════════════════════════════════════════════════════════════════════
# Connection helpers
# ══════════════════════════════════════════════════════════════════════════

def connect(addr: str, **kwargs) -> Client:
    """Connect to a ClickHouse server (sync).

    Args:
        addr: Host and port (e.g., ``"127.0.0.1:9000"``).
        **kwargs: Passed to :class:`Client`.

    Returns:
        :class:`Client` instance.
    """
    addr, kwargs = _parse_connect_args(addr, kwargs)
    return Client(addr, **kwargs)


def connect_async(addr: str, **kwargs) -> AsyncClient:
    """Connect to a ClickHouse server (async).

    Args:
        addr: Host and port (e.g., ``"127.0.0.1:9000"``).
        **kwargs: Passed to :class:`AsyncClient`.

    Returns:
        :class:`AsyncClient` instance.

    Usage:
        async with connect_async("127.0.0.1:9000") as client:
            rows = await client.query("SELECT 1")
    """
    addr, kwargs = _parse_connect_args(addr, kwargs)
    return AsyncClient(addr, **kwargs)
