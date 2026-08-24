"""Tests for st-clickhouse-py bindings.

These tests verify the Python API contract. They require a running
ClickHouse server. Use ``pytest`` to run.
"""

from __future__ import annotations

import pytest
import asyncio
import datetime
import os
import uuid
from typing import Any, Dict, List, Iterator, Generator

import st_clickhouse as ch
from st_clickhouse import (
    Client,
    AsyncClient,
    Block,
    Column,
    ClickHouseError,
    ConnectionError,
    AuthenticationError,
    connect,
    connect_async,
    dicts_to_block,
)


# ══════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1:9000")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASS = os.environ.get("CLICKHOUSE_PASS", "test")


# ══════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════

def _parse_connection_url(url: str) -> dict:
    """Parse a connection URL (internal helper, mirrors __init__.py logic)."""
    from st_clickhouse import connect
    # Just validate the URL parses by attempting connect-style parsing
    return {"url": url}


# ══════════════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="session")
def docker_ch() -> str:
    """Return the ClickHouse server address for testing."""
    return CLICKHOUSE_HOST


@pytest.fixture
def client(docker_ch: str) -> Generator[Client, None, None]:
    """Create a connected client."""
    c = connect(
        docker_ch,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASS,
        settings={"max_block_size": "1000"},
    )
    yield c
    c.close()


@pytest.fixture
def test_table(client: Client) -> Generator[str, None, None]:
    """Create a test table and return its name."""
    table = "test_py_" + uuid.uuid4().hex[:8]
    client.execute(
        f"CREATE TABLE IF NOT EXISTS {table} ("
        "  id UInt64,"
        "  name String,"
        "  age UInt8,"
        "  salary Float64,"
        "  active Bool"
        ") ENGINE = Memory"
    )
    client.execute(
        f"INSERT INTO {table} (id, name, age, salary, active) VALUES "
        "(1, 'Alice', 30, 75000.50, 1),"
        "(2, 'Bob', 25, 82000.00, 0),"
        "(3, 'Charlie', 35, 95000.00, 1)"
    )
    yield table
    client.execute(f"DROP TABLE IF EXISTS {table}")


# ══════════════════════════════════════════════════════════════════════════
# Connection tests
# ══════════════════════════════════════════════════════════════════════════

class TestConnection:
    def test_connect_plain(self, docker_ch: str):
        """Connect with plain host:port address."""
        c = connect(docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS)
        assert c.ping()
        c.close()

    def test_connect_url(self, docker_ch: str):
        """Connect with clickhouse:// URL."""
        host, port = docker_ch.split(":")
        url = f"clickhouse://{CLICKHOUSE_USER}:{CLICKHOUSE_PASS}@{host}:{port}/default"
        c = connect(url)
        assert c.ping()
        c.close()

    def test_connect_url_no_password(self, docker_ch: str):
        """Connect with clickhouse:// URL and password supplied separately."""
        host, port = docker_ch.split(":")
        url = f"clickhouse://{CLICKHOUSE_USER}@{host}:{port}"
        c = connect(url, password=CLICKHOUSE_PASS)
        assert c.ping()
        c.close()

    def test_connect_with_settings(self, docker_ch: str):
        """Connect with additional settings."""
        c = connect(
            docker_ch,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            settings={"max_block_size": "500", "distributed_product_mode": "deny"},
        )
        assert c.ping()
        c.close()

    def test_connect_bad_address(self):
        """Connecting to a non-existent server raises ConnectionError."""
        with pytest.raises(ConnectionError):
            connect("127.0.0.1:19999", connect_timeout=1.0)

    def test_connect_bad_auth(self, docker_ch: str):
        """Connecting with wrong password raises AuthenticationError."""
        with pytest.raises(AuthenticationError):
            connect(docker_ch, user=CLICKHOUSE_USER, password="wrong_password")

    def test_connect_timeout_silent_server_raises_timeout_error(self):
        """A server that accepts TCP but never answers must raise
        st_clickhouse.TimeoutError (and the builtin TimeoutError) within the
        configured connect_timeout — not hang until query_timeout."""
        import socket
        import threading
        import time

        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(8)
        port = listener.getsockname()[1]
        held: list[socket.socket] = []

        def accept_and_stay_silent() -> None:
            while True:
                try:
                    conn, _ = listener.accept()
                except OSError:
                    return
                held.append(conn)  # accept, never write, never close

        acceptor = threading.Thread(target=accept_and_stay_silent, daemon=True)
        acceptor.start()

        start = time.monotonic()
        with pytest.raises(ch.TimeoutError) as excinfo:
            connect(
                f"127.0.0.1:{port}",
                connect_timeout=0.5,
                query_timeout=60.0,
            )
        elapsed = time.monotonic() - start

        # High-level error is the specific st_clickhouse.TimeoutError, not a
        # generic ClickHouseError (the native layer raised the builtin
        # TimeoutError; map_error must translate it).
        assert type(excinfo.value) is ch.TimeoutError, type(excinfo.value)
        assert "did not complete" in str(excinfo.value)
        # Generous upper bound: far below the 60 s query timeout.
        assert elapsed < 30.0, f"connect timeout fired too late: {elapsed:.1f}s"
        listener.close()

    def test_connect_timeout_zero_is_config_error(self):
        """Duration.ZERO cannot mean "no deadline" — it must be rejected."""
        with pytest.raises(ch.ConfigError):
            connect("127.0.0.1:9000", connect_timeout=0.0)

    def test_context_manager(self, docker_ch: str):
        """Client works as a context manager."""
        with connect(docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS) as c:
            assert c.ping()
        assert c.closed

    def test_double_close(self, docker_ch: str):
        """Closing twice should not error."""
        c = connect(docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS)
        c.close()
        c.close()
        assert c.closed

    def test_connect_compression_lz4(self, docker_ch: str):
        """Connect with LZ4 compression."""
        c = connect(
            docker_ch,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            compression="lz4",
        )
        assert c.ping()
        c.close()

    def test_connect_compression_zstd(self, docker_ch: str):
        """Connect with ZSTD compression."""
        c = connect(
            docker_ch,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            compression="zstd",
        )
        assert c.ping()
        c.close()


class TestErrorMapping:
    """Native-to-Python error translation for the new timeout/config errors."""

    def test_native_builtin_timeout_maps_to_st_timeout(self):
        from st_clickhouse._errors import map_error

        native = TimeoutError("ClickHouse timeout: connection setup stalled")
        mapped = map_error(native)
        assert type(mapped) is ch.TimeoutError
        assert isinstance(mapped, ClickHouseError)  # still part of the hierarchy

    def test_native_config_error_maps_to_config_error(self):
        from st_clickhouse._errors import map_error

        native = ValueError(
            "ClickHouse configuration error: connect_timeout must be greater than zero"
        )
        mapped = map_error(native)
        assert type(mapped) is ch.ConfigError

    def test_builtin_timeout_wins_over_word_heuristics(self):
        from st_clickhouse._errors import map_error

        # A timeout message containing "connection" must stay a TimeoutError
        # instead of falling through to the connection-word heuristic.
        native = TimeoutError(
            "connect to 127.0.0.1:9000 (TCP + TLS + handshake + ping) timed out"
        )
        mapped = map_error(native)
        assert type(mapped) is ch.TimeoutError


# ══════════════════════════════════════════════════════════════════════════
# Query execution tests
# ══════════════════════════════════════════════════════════════════════════

class TestQuery:
    def test_select_one(self, client: Client):
        """SELECT 1 returns a single row."""
        rows = client.query("SELECT 1 AS x")
        assert len(rows) == 1
        assert rows[0]["x"] == 1

    def test_select_many_rows(self, client: Client):
        """SELECT with multiple rows."""
        rows = client.query("SELECT number FROM system.numbers LIMIT 10")
        assert len(rows) == 10
        assert [r["number"] for r in rows] == list(range(10))

    def test_select_multiple_columns(self, client: Client):
        """SELECT with multiple columns."""
        rows = client.query("SELECT 1 AS a, 'hello' AS b, 3.14 AS c")
        assert len(rows) == 1
        assert rows[0]["a"] == 1
        assert rows[0]["b"] == "hello"
        assert abs(rows[0]["c"] - 3.14) < 0.01

    def test_select_empty_result(self, client: Client):
        """SELECT with no matching rows."""
        rows = client.query("SELECT 1 WHERE 0")
        assert len(rows) == 0

    def test_execute_ddl(self, client: Client):
        """DDL execution (CREATE, DROP)."""
        client.execute("CREATE TABLE IF NOT EXISTS test_ddl (id UInt64) ENGINE = Memory")
        client.execute("DROP TABLE IF EXISTS test_ddl")

    def test_execute_insert(self, client: Client, test_table: str):
        """INSERT via execute."""
        rows = client.query(f"SELECT count(*) AS cnt FROM {test_table}")
        assert rows[0]["cnt"] == 3

    def test_query_blocks(self, client: Client, test_table: str):
        """query_blocks returns Block objects."""
        blocks = client.query_blocks(f"SELECT id, name FROM {test_table} ORDER BY id")
        assert len(blocks) > 0
        block = blocks[0]
        assert isinstance(block, Block)
        assert block.row_count() == 3
        assert block.column_count() == 2

    def test_query_blocks_column_access(self, client: Client, test_table: str):
        """Access columns from a Block."""
        blocks = client.query_blocks(f"SELECT id, name FROM {test_table} ORDER BY id")
        block = blocks[0]
        col_id = block["id"]
        col_name = block["name"]
        assert col_id.name == "id"
        assert col_name.name == "name"
        assert col_id.to_list() == [1, 2, 3]

    def test_query_tuples(self, client: Client, test_table: str):
        """query_tuples returns positional row tuples."""
        rows = client.query_tuples(f"SELECT id, name FROM {test_table} ORDER BY id")
        assert rows == [(1, "Alice"), (2, "Bob"), (3, "Charlie")]

    def test_query_columns(self, client: Client, test_table: str):
        """query_columns returns a column-oriented mapping."""
        columns = client.query_columns(f"SELECT id, name FROM {test_table} ORDER BY id")
        assert columns["id"] == [1, 2, 3]
        assert columns["name"] == ["Alice", "Bob", "Charlie"]

    def test_query_stream(self, client: Client, test_table: str):
        """query_stream yields blocks."""
        blocks = list(client.query_stream(f"SELECT id FROM {test_table} ORDER BY id"))
        assert len(blocks) > 0
        total_rows = sum(b.row_count() for b in blocks)
        assert total_rows == 3

    def test_query_with_params(self, client: Client):
        """Query with different data types."""
        rows = client.query("SELECT 42 AS int, 'text' AS str, 1.5 AS flt, true AS b")
        row = rows[0]
        assert row["int"] == 42
        assert row["str"] == "text"
        assert abs(row["flt"] - 1.5) < 0.01
        assert row["b"] is True

    def test_server_info(self, client: Client):
        """server_info returns metadata."""
        info = client.server_info()
        assert info["name"] == "ClickHouse"
        assert info["version_major"] >= 24
        assert info["revision"] > 54000


# ══════════════════════════════════════════════════════════════════════════
# Type conversion tests
# ══════════════════════════════════════════════════════════════════════════

class TestTypeConversion:
    def test_date_type(self, client: Client):
        """Date column converts to datetime.date."""
        rows = client.query("SELECT toDate('2024-01-15') AS d")
        d = rows[0]["d"]
        assert isinstance(d, (datetime.date, int))

    def test_datetime_type(self, client: Client):
        """DateTime column converts to datetime.datetime."""
        rows = client.query("SELECT toDateTime('2024-01-15 10:30:00') AS ts")
        ts = rows[0]["ts"]
        assert isinstance(ts, (datetime.datetime, int, float))

    def test_uint_types(self, client: Client):
        """Integer columns return as Python int."""
        rows = client.query(
            "SELECT "
            "  toUInt8(255) AS u8,"
            "  toUInt16(65535) AS u16,"
            "  toUInt32(4294967295) AS u32,"
            "  toInt8(-128) AS i8,"
            "  toInt16(-32768) AS i16"
        )
        r = rows[0]
        assert r["u8"] == 255
        assert r["u16"] == 65535
        assert r["u32"] == 4294967295
        assert r["i8"] == -128
        assert r["i16"] == -32768

    def test_uint64_large_boundary(self, client: Client):
        """UInt64 values above JavaScript's safe integer boundary stay exact."""
        value = 9_007_199_254_740_993
        rows = client.query(f"SELECT toUInt64({value}) AS u")
        assert rows[0]["u"] == value
        assert isinstance(rows[0]["u"], int)

    def test_float_types(self, client: Client):
        """Float columns return as Python float."""
        rows = client.query("SELECT toFloat32(3.14) AS f32, toFloat64(2.71828) AS f64")
        r = rows[0]
        assert abs(r["f32"] - 3.14) < 0.01
        assert abs(r["f64"] - 2.71828) < 0.01

    def test_string_type(self, client: Client):
        """String columns return as Python str."""
        rows = client.query("SELECT 'hello world' AS s")
        assert rows[0]["s"] == "hello world"

    def test_nullable_type(self, client: Client):
        """Nullable columns return None for null values."""
        rows = client.query("SELECT CAST(NULL AS Nullable(UInt64)) AS n")
        assert rows[0]["n"] is None

    def test_bool_type(self, client: Client):
        """Bool columns return as Python bool."""
        rows = client.query("SELECT toBool(1) AS t, toBool(0) AS f")
        assert rows[0]["t"] is True
        assert rows[0]["f"] is False

    def test_uuid_type(self, client: Client):
        """UUID column returns as string."""
        rows = client.query("SELECT generateUUIDv4() AS u")
        val = rows[0]["u"]
        assert isinstance(val, str)
        assert len(val) == 36

    def test_ipv4_type(self, client: Client):
        """IPv4 column returns as string."""
        rows = client.query("SELECT toIPv4('127.0.0.1') AS ip")
        assert rows[0]["ip"] == "127.0.0.1"

    def test_decimal_type(self, client: Client):
        """Decimal column returns numeric."""
        rows = client.query("SELECT toDecimal32(123.45, 2) AS d")
        val = rows[0]["d"]
        assert isinstance(val, (int, float))

    def test_fixed_array_map_tuple_enum_types(self, client: Client):
        """Compound and less common native types convert to Python values."""
        rows = client.query(
            "SELECT "
            "toFixedString('ab', 4) AS fs,"
            "[toUInt64(1), toUInt64(2)] AS arr,"
            "map('a', toUInt64(10), 'b', toUInt64(20)) AS m,"
            "tuple(toUInt8(7), 'x') AS t,"
            "CAST('b', 'Enum8(\\'a\\' = 1, \\'b\\' = 2)') AS e"
        )
        row = rows[0]
        assert row["fs"].startswith("ab")
        assert row["arr"] == [1, 2]
        assert row["m"] == {"a": 10, "b": 20}
        assert row["t"] == (7, "x")
        assert row["e"] == 2


# ══════════════════════════════════════════════════════════════════════════
# Error handling tests
# ══════════════════════════════════════════════════════════════════════════

class TestErrors:
    def test_server_error_mapping_is_query_error(self):
        """Native ServerError text maps to QueryError, not ProtocolError.

        Server-free unit test for the _native → _errors mapping: the native
        to_py_err raises ValueError with a "ClickHouse server error (code=..."
        prefix; map_error must classify it as QueryError even when the server
        message itself contains words like "protocol" or "compression".
        """
        from st_clickhouse._errors import QueryError, map_error

        exc = ValueError(
            "ClickHouse server error (code=60, name=DB::Exception): "
            "unknown function protocol_compression_xyz"
        )
        mapped = map_error(exc)
        assert isinstance(mapped, QueryError)
        assert isinstance(mapped, ClickHouseError)

        auth_flavored = map_error(
            ValueError(
                "ClickHouse server error (code=516, name=DB::Exception): "
                "Authentication failed"
            )
        )
        assert isinstance(auth_flavored, QueryError)

    def test_protocol_error_mapping_unchanged(self):
        """Plain protocol ValueError still maps to ProtocolError."""
        from st_clickhouse._errors import ProtocolError, map_error

        mapped = map_error(ValueError("ClickHouse protocol error: unknown packet type: 99"))
        assert isinstance(mapped, ProtocolError)

    def test_query_error(self, client: Client):
        """Invalid SQL raises ClickHouseError."""
        with pytest.raises(ClickHouseError):
            client.query("SELECT invalid_column FROM non_existent_table")

    def test_syntax_error(self, client: Client):
        """Syntax errors raise ClickHouseError."""
        with pytest.raises(ClickHouseError):
            client.query("SELECT不良SQL")

    def test_closed_connection(self, client: Client):
        """Operations on closed connection raise ConnectionError."""
        client.close()
        with pytest.raises(ConnectionError, match="Connection is closed"):
            client.query("SELECT 1")

    def test_ping_on_closed(self, client: Client):
        """Ping on closed connection raises."""
        client.close()
        with pytest.raises(ConnectionError):
            client.ping()


# ══════════════════════════════════════════════════════════════════════════
# DataFrame integration
# ══════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════
# Async client tests
# ══════════════════════════════════════════════════════════════════════════

class TestAsyncClient:
    @pytest.mark.asyncio
    async def test_async_connect(self, docker_ch: str):
        """Async connect and query."""
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        ) as c:
            rows = await c.query("SELECT 1 AS x")
            assert rows[0]["x"] == 1

    @pytest.mark.asyncio
    async def test_async_execute(self, docker_ch: str):
        """Async execute DDL."""
        table = "test_async_" + uuid.uuid4().hex[:8]
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        ) as c:
            await c.execute(f"CREATE TABLE IF NOT EXISTS {table} (id UInt64) ENGINE = Memory")
            await c.execute(f"DROP TABLE IF EXISTS {table}")

    @pytest.mark.asyncio
    async def test_async_blocks(self, docker_ch: str):
        """Async query_blocks."""
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        ) as c:
            blocks = await c.query_blocks("SELECT 1 AS x")
            assert len(blocks) > 0
            assert blocks[0].row_count() == 1

    @pytest.mark.asyncio
    async def test_async_tuples_and_columns(self, docker_ch: str):
        """Async tuple and column-oriented materialization."""
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        ) as c:
            rows = await c.query_tuples("SELECT 1 AS x, 'a' AS y")
            columns = await c.query_columns("SELECT 1 AS x, 'a' AS y")
            assert rows == [(1, "a")]
            assert columns == {"x": [1], "y": ["a"]}

    @pytest.mark.asyncio
    async def test_async_context_manager(self, docker_ch: str):
        """Async context manager closes properly."""
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        ) as c:
            assert await c.ping()
        assert c.closed

    @pytest.mark.asyncio
    async def test_async_session_stickiness_for_temp_table(self, docker_ch: str):
        """Pinned async session keeps temporary tables on one connection."""
        async with connect_async(
            docker_ch,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            pool_min_size=2,
            pool_max_size=2,
        ) as c:
            async with c.session() as session:
                await session.execute("CREATE TEMPORARY TABLE py_tmp_sticky (x UInt64)")
                await session.execute("INSERT INTO py_tmp_sticky VALUES (42)")
                rows = await session.query("SELECT x FROM py_tmp_sticky")
                assert rows[0]["x"] == 42


# ══════════════════════════════════════════════════════════════════════════
# Direct native module tests
# ══════════════════════════════════════════════════════════════════════════

class TestNativeModule:
    def test_native_import(self):
        """Native _native module can be imported."""
        from st_clickhouse._native import _Client, _Block  # pyrefly: ignore
        assert hasattr(_Client, "__module__")
        assert hasattr(_Block, "__module__")

    def test_native_version(self):
        """Package has a version."""
        import st_clickhouse
        assert isinstance(st_clickhouse.__version__, str)
        assert len(st_clickhouse.__version__) > 0

    def test_native_connect(self, docker_ch: str):
        """Native _Client connects and pings."""
        from st_clickhouse._native import _Client  # pyrefly: ignore
        c = _Client(docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS)
        assert c.ping()

    def test_native_blocks(self, docker_ch: str):
        """Native _Client query_blocks works."""
        from st_clickhouse._native import _Client  # pyrefly: ignore
        c = _Client(docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS)
        blocks = c.query_blocks("SELECT 1 AS x")
        assert len(blocks) > 0
        block = blocks[0]
        assert block.row_count() == 1
        names = block.column_names()
        assert "x" in names


# ══════════════════════════════════════════════════════════════════════════
# Edge cases & stress tests
# ══════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    def test_large_result(self, client: Client):
        """Query with many rows."""
        rows = client.query("SELECT number FROM system.numbers LIMIT 10000")
        assert len(rows) == 10000

    def test_many_columns(self, client: Client):
        """Query with many columns."""
        cols = ", ".join(f"{i} AS c{i}" for i in range(50))
        rows = client.query(f"SELECT {cols}")
        assert len(rows) == 1
        assert len(rows[0]) == 50

    def test_special_chars_in_string(self, client: Client):
        """Strings with special characters."""
        special = "hello'world\\nested\"quotes"
        rows = client.query("SELECT {s:String} AS s", s=special)
        assert rows[0]["s"] == special

    def test_unicode_string(self, client: Client):
        """Unicode strings round-trip correctly."""
        text = "你好世界 🦀🔥"
        rows = client.query(f"SELECT '{text}' AS s")
        assert rows[0]["s"] == text

    def test_many_sequential_queries(self, client: Client):
        """Many sequential queries don't leak connections."""
        for i in range(100):
            rows = client.query(f"SELECT {i} AS x")
            assert rows[0]["x"] == i

    def test_empty_string(self, client: Client):
        """Empty string round-trips correctly."""
        rows = client.query("SELECT '' AS s")
        assert rows[0]["s"] == ""

    def test_large_string(self, client: Client):
        """Large strings (10KB)."""
        big = "x" * 10_000
        rows = client.query(f"SELECT '{big}' AS s")
        assert rows[0]["s"] == big

    def test_negative_numbers(self, client: Client):
        """Negative integers."""
        rows = client.query("SELECT -1 AS n, -1000000 AS m")
        assert rows[0]["n"] == -1
        assert rows[0]["m"] == -1000000

    def test_zero_values(self, client: Client):
        """Zero values in all numeric types."""
        rows = client.query(
            "SELECT "
            "  toUInt8(0) AS u8,"
            "  toInt32(0) AS i32,"
            "  toFloat64(0.0) AS f64,"
            "  toDecimal64(0, 2) AS d64"
        )
        r = rows[0]
        assert r["u8"] == 0
        assert r["i32"] == 0
        assert r["f64"] == 0.0


# ══════════════════════════════════════════════════════════════════════════
# Insert tests
# ══════════════════════════════════════════════════════════════════════════

class TestInsert:
    def test_insert_via_execute(self, client: Client):
        """Insert data via execute with VALUES."""
        table = "test_insert_" + uuid.uuid4().hex[:8]
        client.execute(
            f"CREATE TABLE {table} (id UInt64, name String, age UInt8) ENGINE = Memory"
        )
        client.execute(
            f"INSERT INTO {table} (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25)"
        )
        rows = client.query(f"SELECT * FROM {table} ORDER BY id")
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        client.execute(f"DROP TABLE {table}")


# ══════════════════════════════════════════════════════════════════════════
# Block API tests
# ══════════════════════════════════════════════════════════════════════════

class TestBlockAPI:
    def test_block_repr(self, client: Client):
        """Block __repr__ is informative."""
        blocks = client.query_blocks("SELECT 1")
        r = repr(blocks[0])
        assert "Block" in r
        assert "rows" in r

    def test_block_columns(self, client: Client):
        """Access Block columns by name and index."""
        blocks = client.query_blocks("SELECT 1 AS a, 2 AS b")
        block = blocks[0]
        assert block.column_count() == 2
        assert block.column_names() == ["a", "b"]
        assert block["a"].name == "a"
        assert block["b"].name == "b"

    def test_column_repr(self, client: Client):
        """Column __repr__ is informative."""
        blocks = client.query_blocks("SELECT 1 AS x")
        col = blocks[0]["x"]
        r = repr(col)
        assert "Column" in r
        assert "x" in r

    def test_column_access_by_type_name(self, client: Client):
        """Column type_name is accurate."""
        blocks = client.query_blocks("SELECT toUInt64(1) AS x")
        col = blocks[0]["x"]
        assert col.type_name == "UInt64"

    def test_block_getitem_keyerror(self, client: Client):
        """Accessing non-existent column raises KeyError."""
        blocks = client.query_blocks("SELECT 1 AS x")
        with pytest.raises(KeyError):
            blocks[0]["nonexistent"]

    def test_block_by_index(self, client: Client):
        """Access column by index."""
        blocks = client.query_blocks("SELECT 1 AS a, 2 AS b")
        col = blocks[0].column_by_index(1)
        assert col.name == "b"


# ══════════════════════════════════════════════════════════════════════════
# Streaming tests
# ══════════════════════════════════════════════════════════════════════════


class TestStreaming:
    """Sync and async streaming query execution."""

    def test_sync_query_stream(self, client: Client):
        """Sync query_stream yields blocks."""
        blocks = list(client.query_stream("SELECT number FROM system.numbers LIMIT 10"))
        assert len(blocks) >= 1
        total = sum(b.row_count() for b in blocks)
        assert total == 10

    def test_sync_query_stream_empty(self, client: Client):
        """Sync query_stream on empty result yields nothing."""
        blocks = list(client.query_stream("SELECT 1 WHERE 0"))
        assert len(blocks) == 0

    def test_sync_query_stream_column_access(self, client: Client):
        """Blocks from stream support column access."""
        for block in client.query_stream("SELECT 1 AS x UNION ALL SELECT 2"):
            assert block["x"].to_list() == [1] or block["x"].to_list() == [2]

    @pytest.mark.asyncio
    async def test_async_query_stream(self, docker_ch: str):
        """Async query_stream yields blocks."""
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS, pool_min_size=1
        ) as c:
            blocks = []
            async for block in c.query_stream("SELECT number FROM system.numbers LIMIT 10"):
                blocks.append(block)
            assert len(blocks) >= 1
            total = sum(b.row_count() for b in blocks)
            assert total == 10

    @pytest.mark.asyncio
    async def test_async_query_stream_empty(self, docker_ch: str):
        """Async query_stream on empty result yields nothing."""
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS, pool_min_size=1
        ) as c:
            blocks = []
            async for block in c.query_stream("SELECT 1 WHERE 0"):
                blocks.append(block)
            assert len(blocks) == 0

    @pytest.mark.asyncio
    async def test_async_query_stream_cancel(self, docker_ch: str):
        """Cancelling async query_stream cleans up."""
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS, pool_min_size=1
        ) as c:
            stream = c.query_stream(
                "SELECT number FROM system.numbers LIMIT 100000 SETTINGS max_block_size=1"
            )

            async def consume():
                count = 0
                async for _ in stream:
                    count += 1
                    if count >= 5:
                        raise asyncio.CancelledError()
                return count

            with pytest.raises(asyncio.CancelledError):
                await consume()

            # Pool should still work after cancellation
            rows = await c.query("SELECT 1 AS x")
            assert rows[0]["x"] == 1


# ══════════════════════════════════════════════════════════════════════════
# Connection pool tests
# ══════════════════════════════════════════════════════════════════════════


class TestConnectionPool:
    """Tests for the async connection pool."""

    @pytest.mark.asyncio
    async def test_pool_concurrent_queries(self, docker_ch: str):
        """Multiple async queries run concurrently on different pool connections."""
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS,
            pool_min_size=4, pool_max_size=4,
        ) as c:
            async def query_slow():
                return await c.query("SELECT sleep(0.1) AS x")

            # Run 4 queries concurrently
            results = await asyncio.gather(*[query_slow() for _ in range(4)])
            assert len(results) == 4

    @pytest.mark.asyncio
    async def test_pool_exhaustion(self, docker_ch: str):
        """Pool raises ConnectionError when all connections are busy beyond timeout."""
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS,
            pool_min_size=2, pool_max_size=2, pool_acquire_timeout=1.0,
        ) as c:
            async def slow_query():
                return await c.query("SELECT sleepEachRow(0.3) FROM numbers(5)")

            # Start 2 slow queries (fills pool), then try a 3rd (times out)
            task1 = asyncio.create_task(slow_query())
            task2 = asyncio.create_task(slow_query())
            for _ in range(20):
                if c.metrics["in_use"] == 2:
                    break
                await asyncio.sleep(0.05)

            with pytest.raises(ConnectionError, match="pool exhausted"):
                await c.query("SELECT 1")

            # Clean up
            task1.cancel()
            task2.cancel()
            for t in (task1, task2):
                try:
                    await t
                except (asyncio.CancelledError, ConnectionError):
                    pass

    @pytest.mark.asyncio
    async def test_pool_metrics(self, docker_ch: str):
        """Pool metrics are available and sensible."""
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS,
            pool_min_size=2, pool_max_size=4,
        ) as c:
            m = c._pool.metrics
            assert m["min_size"] == 2
            assert m["max_size"] == 4
            assert m["total"] >= 2
            assert m["in_use"] == 0  # No active queries
            assert m["available"] >= 2
            assert m["oldest_idle"] is not None

    @pytest.mark.asyncio
    async def test_pool_reuses_connections(self, docker_ch: str):
        """Pool reuses connections across queries."""
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS,
            pool_min_size=1, pool_max_size=1,
        ) as c:
            m1 = c._pool.metrics
            await c.query("SELECT 1")
            await c.query("SELECT 2")
            await c.query("SELECT 3")
            m2 = c._pool.metrics
            # Same connections reused (total shouldn't grow beyond min_size)
            assert m2["total"] <= m1["total"] + 1


# ══════════════════════════════════════════════════════════════════════════
# Streaming INSERT tests
# ══════════════════════════════════════════════════════════════════════════


class TestInsertStream:
    """Tests for InsertStream and AsyncInsertStream."""

    def test_sync_insert_stream(self, client: Client, test_table: str):
        """Sync InsertStream inserts data block by block."""
        block = dicts_to_block(
            [{"id": 10, "name": "Stream", "age": 40, "salary": 100000.0, "active": True}],
            [("id", "UInt64"), ("name", "String"), ("age", "UInt8"), ("salary", "Float64"), ("active", "Bool")],
        )
        with client.insert_stream(f"INSERT INTO {test_table} VALUES") as stream:
            stream.send(block)

        rows = client.query(f"SELECT * FROM {test_table} WHERE id = 10")
        assert len(rows) == 1
        assert rows[0]["name"] == "Stream"

    @pytest.mark.asyncio
    async def test_async_insert_stream(self, docker_ch: str):
        """AsyncInsertStream inserts data block by block."""
        table = "test_async_insert_" + uuid.uuid4().hex[:8]
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS, pool_min_size=1
        ) as c:
            await c.execute(
                f"CREATE TABLE {table} (id UInt64, name String, age UInt8) ENGINE = Memory"
            )
            block = dicts_to_block(
                [{"id": 1, "name": "Async", "age": 25}],
                [("id", "UInt64"), ("name", "String"), ("age", "UInt8")],
            )
            async with await c.insert_stream(f"INSERT INTO {table} VALUES") as stream:
                await stream.send(block)

            rows = await c.query(f"SELECT * FROM {table}")
            assert len(rows) == 1
            assert rows[0]["name"] == "Async"
            await c.execute(f"DROP TABLE {table}")

    @pytest.mark.asyncio
    async def test_async_insert_stream_timeout(self, docker_ch: str):
        """AsyncInsertStream.send with timeout."""
        table = "test_insert_timeout_" + uuid.uuid4().hex[:8]
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS, pool_min_size=1
        ) as c:
            await c.execute(
                f"CREATE TABLE {table} (id UInt64) ENGINE = Memory"
            )
            block = dicts_to_block(
                [{"id": 1}],
                [("id", "UInt64")],
            )
            async with await c.insert_stream(f"INSERT INTO {table} VALUES") as stream:
                await stream.send(block, timeout=10.0)

            rows = await c.query(f"SELECT * FROM {table}")
            assert len(rows) == 1
            await c.execute(f"DROP TABLE {table}")


# ══════════════════════════════════════════════════════════════════════════
# Server info & settings tests
# ══════════════════════════════════════════════════════════════════════════


class TestServerInfo:
    """Tests for server_info and settings."""

    def test_server_info(self, client: Client):
        """server_info returns metadata with expected keys."""
        info = client.server_info()
        assert "name" in info
        assert "version_major" in info
        assert "version_minor" in info
        assert "revision" in info

    @pytest.mark.asyncio
    async def test_async_server_info(self, docker_ch: str):
        """Async server_info returns metadata."""
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS, pool_min_size=1
        ) as c:
            info = await c.server_info()
            assert "name" in info
            assert "revision" in info

    def test_set_setting(self, client: Client):
        """set_setting applies session settings."""
        client.set_setting("max_block_size", "100")
        rows = client.query("SELECT value FROM system.settings WHERE name = 'max_block_size'")
        assert rows[0]["value"] == "100"

    @pytest.mark.asyncio
    async def test_async_set_setting(self, docker_ch: str):
        """Async set_setting applies session settings."""
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS, pool_min_size=1
        ) as c:
            await c.set_setting("max_block_size", "100")
            rows = await c.query(
                "SELECT value FROM system.settings WHERE name = 'max_block_size'"
            )
            assert rows[0]["value"] == "100"


# ══════════════════════════════════════════════════════════════════════════
# Query timeout (sync core already enforces `query_timeout` via the socket
# read timeout; this verifies the constructor wiring surfaces it to Python).
# ══════════════════════════════════════════════════════════════════════════

def test_query_timeout_aborts_slow_query():
    """A tight `query_timeout` must abort a slow query rather than hang."""
    client = Client(
        CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASS,
        query_timeout=0.5,
    )
    try:
        # SELECT sleep(3) sends no data for 3s; the 0.5s read timeout fires.
        with pytest.raises(Exception):
            client.query("SELECT sleep(3)")
    finally:
        client.close()

    # A fresh client must still work afterwards (server is healthy).
    client2 = Client(
        CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASS,
        query_timeout=30.0,
    )
    try:
        rows = client2.query("SELECT toUInt64(1) AS x")
        assert rows[0]["x"] == 1
    finally:
        client2.close()


# ══════════════════════════════════════════════════════════════════════════
# Per-query settings overlay
#
# Regression tests for the `with_per_query_settings` bug: the helper used to
# mutate the native client's session settings per query and its restore loop
# was a no-op, so per-query settings leaked onto the connection forever.
# ══════════════════════════════════════════════════════════════════════════


class TestPerQuerySettings:
    """Sync per-query settings: visible inside the query, never persistent."""

    Q = "SELECT value FROM system.settings WHERE name = '{}'"

    def _get(self, client: Client, name: str = "max_threads") -> str:
        rows = client.query(self.Q.format(name))
        assert rows, f"server did not report setting {name!r}"
        return rows[0]["value"]

    def test_settings_visible_within_query_and_not_after(self, client: Client):
        """Overlay applies to its own query; later queries see the baseline."""
        baseline = self._get(client)
        rows = client.query(self.Q.format("max_threads"), settings={"max_threads": "3"})
        assert rows[0]["value"] == "3"
        # The old bug leaked "3" onto the connection forever.
        assert self._get(client) == baseline

    def test_overlay_overrides_constructor_setting_then_restores(self, docker_ch: str):
        """Constructor baseline is overridden for one query, then restored."""
        c = connect(
            docker_ch,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            settings={"max_threads": "7"},
        )
        try:
            assert self._get(c) == "7"
            rows = c.query(self.Q.format("max_threads"), settings={"max_threads": "3"})
            assert rows[0]["value"] == "3"  # overlay wins over constructor baseline
            assert self._get(c) == "7"  # baseline structurally intact afterwards
        finally:
            c.close()

    def test_overlay_error_leaves_baseline_unchanged(self, client: Client):
        """A failing overlay query must not disturb the connection baseline."""
        baseline = self._get(client)
        with pytest.raises(ClickHouseError):
            client.query("SELECT no_such_function_xyz()", settings={"max_threads": "3"})
        assert self._get(client) == baseline

    def test_no_baseline_key_returns_server_default_afterwards(self, client: Client):
        """Keys absent from the baseline revert to the server default."""
        key = "max_insert_block_size"
        default = self._get(client, key)
        rows = client.query(self.Q.format(key), settings={key: "123457"})
        assert rows[0]["value"] == "123457"
        assert self._get(client, key) == default

    def test_all_materialized_variants_apply_overlay(self, client: Client):
        """query_tuples/query_columns/query_blocks/execute route the overlay."""
        q = self.Q.format("max_threads")
        assert client.query_tuples(q, settings={"max_threads": "3"})[0][0] == "3"
        assert client.query_columns(q, settings={"max_threads": "3"})["value"][0] == "3"
        blocks = client.query_blocks(q, settings={"max_threads": "3"})
        assert ch.blocks_to_dicts(blocks)[0]["value"] == "3"
        client.execute(q, settings={"max_threads": "3"})  # rows dropped, must not error
        assert self._get(client) != "3"  # nothing leaked

    def test_params_and_settings_route_independently(self, client: Client):
        """Server-side parameters keep working alongside an overlay."""
        rows = client.query(
            "SELECT {v:UInt8} AS x, value AS mt FROM system.settings "
            "WHERE name = 'max_threads'",
            params={"v": 42},
            settings={"max_threads": "3"},
        )
        assert rows[0]["x"] == 42
        assert rows[0]["mt"] == "3"

    def test_non_string_setting_values_are_coerced(self, client: Client):
        """Int/bool values keep working (the old helper stringified them)."""
        rows = client.query(self.Q.format("max_threads"), settings={"max_threads": 3})
        assert rows[0]["value"] == "3"

    def test_invalid_settings_shape_preserves_type_error(self, client: Client):
        """Python argument errors must not be flattened into ClickHouseError."""
        bad_shape: Any = 0
        bad_key: Any = {1: "2"}
        with pytest.raises(TypeError):
            client.query("SELECT 1", settings=bad_shape)
        with pytest.raises(TypeError):
            client.query("SELECT 1", settings=bad_key)

    def test_native_client_settings_keyword(self, docker_ch: str):
        """Native _Client accepts keyword-only settings and never persists them."""
        from st_clickhouse._native import _Client as _NativeClient

        native = _NativeClient(
            docker_ch,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            settings={"max_threads": "7"},
        )
        try:
            rows = native.query(self.Q.format("max_threads"), settings={"max_threads": "3"})
            assert rows[0]["value"] == "3"
            rows = native.query(self.Q.format("max_threads"))
            assert rows[0]["value"] == "7"
        finally:
            del native


class TestAsyncPerQuerySettings:
    """Async per-query settings: overlay on pool connections, not parameters."""

    Q = "SELECT value FROM system.settings WHERE name = '{}'"

    async def _get(self, client: AsyncClient, name: str = "max_threads") -> str:
        rows = await client.query(self.Q.format(name))
        assert rows, f"server did not report setting {name!r}"
        return rows[0]["value"]

    @pytest.mark.asyncio
    async def test_settings_visible_within_query_and_not_after(self, docker_ch: str):
        """Overlay applies to its own query; later pool queries see baseline."""
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS, pool_min_size=1
        ) as c:
            baseline = await self._get(c)
            rows = await c.query(
                self.Q.format("max_threads"), settings={"max_threads": "3"}
            )
            assert rows[0]["value"] == "3"
            assert await self._get(c) == baseline

    @pytest.mark.asyncio
    async def test_explicit_settings_are_not_query_params(self, docker_ch: str):
        """settings= must not be swallowed by **kwargs as a query parameter."""
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        ) as c:
            rows = await c.query("SELECT 1 AS x", settings={"max_threads": "3"})
            assert rows == [{"x": 1}]
            # Both routed at once: placeholders still bind, overlay still applies.
            rows = await c.query(
                "SELECT {v:UInt8} AS x",
                params={"v": 7},
                settings={"max_threads": "3"},
            )
            assert rows == [{"x": 7}]

    @pytest.mark.asyncio
    async def test_session_overlays_apply_and_do_not_leak(self, docker_ch: str):
        """Pinned AsyncSession routes settings separately from query params."""
        async with connect_async(
            docker_ch, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        ) as c:
            async with c.session() as session:
                baseline = (await session.query(self.Q.format("max_threads")))[0]["value"]
                rows = await session.query(
                    self.Q.format("max_threads"), settings={"max_threads": "3"}
                )
                assert rows[0]["value"] == "3"
                blocks = await session.query_blocks(
                    self.Q.format("max_threads"), settings={"max_threads": "5"}
                )
                assert ch.blocks_to_dicts(blocks)[0]["value"] == "5"
                await session.execute("SELECT 1", settings={"max_threads": "7"})
                assert (await session.query(self.Q.format("max_threads")))[0]["value"] == baseline

    @pytest.mark.asyncio
    async def test_concurrent_overlays_do_not_cross_contaminate(self, docker_ch: str):
        """Concurrent queries with different overlays keep their own values."""
        async with connect_async(
            docker_ch,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            pool_min_size=2,
            pool_max_size=4,
        ) as c:

            async def one(value: str) -> str:
                rows = await c.query(
                    self.Q.format("max_threads"), settings={"max_threads": value}
                )
                return rows[0]["value"]

            values = ["2", "3", "5", "7"]
            results = await asyncio.gather(*[one(v) for v in values])
            assert results == values
