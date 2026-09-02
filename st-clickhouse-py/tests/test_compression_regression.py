"""Regression tests for multi-frame response compression (P0 fix).

Round 3 volume testing showed the Python wheel inherited both defects:
  * a 20,000-row query under compression wedged ~300 s (server read timeout)
    and killed the connection, because the sync read path never decompressed
    and the trailing empty Data block was sent uncompressed;
  * the async multi-frame reader left the second compression frame's bytes
    in the stream.

The existing compression tests only ping() (Pong is uncompressed), which is
why this went unnoticed.
"""

from __future__ import annotations

import os

import pytest

import st_clickhouse as ch
from st_clickhouse import connect

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1:9000")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASS = os.environ.get("CLICKHOUSE_PASS", "test")

VOLUME_SQL = "SELECT number, repeat('x', 64) FROM system.numbers LIMIT 20000 SETTINGS max_block_size = 20000"


@pytest.fixture(scope="module")
def docker_ch() -> str:
    return CLICKHOUSE_HOST


def _check_rows(rows: list) -> None:
    assert len(rows) == 20000
    first, last = rows[0], rows[19999]
    if isinstance(first, dict):
        string_col = "repeat('x', 64)"
        assert first["number"] == 0
        assert first[string_col] == "x" * 64
        assert last["number"] == 19999
        assert last[string_col] == "x" * 64
    else:
        assert first[0] == 0
        assert first[1] == "x" * 64
        assert last[0] == 19999
        assert last[1] == "x" * 64


@pytest.mark.parametrize("method", ["lz4"])
class TestCompressedVolumeQueries:
    def test_query_20k_rows(self, docker_ch: str, method: str) -> None:
        """The exact failing shape: one ~1.4 MiB block spanning two frames."""
        c = connect(
            docker_ch,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            compression=method,
        )
        rows = c.query(VOLUME_SQL)
        _check_rows(rows)
        # The connection must stay usable afterwards.
        assert c.query("SELECT toUInt64(7)")[0]["toUInt64(7)"] == 7
        c.close()

    def test_query_stream_20k_rows(self, docker_ch: str, method: str) -> None:
        """query_stream through the same multi-frame shape (Block objects)."""
        c = connect(
            docker_ch,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            compression=method,
        )
        blocks = list(c.query_stream(VOLUME_SQL))
        assert sum(b.row_count() for b in blocks) == 20000
        rows = list(blocks[0].rows())
        string_col = "repeat('x', 64)"
        assert rows[0]["number"] == 0
        assert rows[0][string_col] == "x" * 64
        assert rows[-1]["number"] == 19999
        assert c.query("SELECT toUInt64(3)")[0]["toUInt64(3)"] == 3
        c.close()

    def test_query_15k_boundary(self, docker_ch: str, method: str) -> None:
        """The natural single-block boundary (~1.09 MiB, two frames)."""
        c = connect(
            docker_ch,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            compression=method,
        )
        rows = c.query(
            "SELECT number, repeat('x', 64) FROM system.numbers LIMIT 15000"
        )
        assert len(rows) == 15000
        assert rows[0]["repeat('x', 64)"] == "x" * 64
        c.close()


@pytest.mark.parametrize("method", ["lz4"])
class TestCompressedAsyncVolumeQueries:
    def test_async_query_20k_rows(self, docker_ch: str, method: str) -> None:
        """AsyncClient through the same shape (pool + streaming reads)."""
        import asyncio

        async def run() -> None:
            c = ch.connect_async(
                docker_ch,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASS,
                compression=method,
            )
            rows = await c.query(VOLUME_SQL)
            assert len(rows) == 20000
            assert rows[0]["number"] == 0
            assert rows[0]["repeat('x', 64)"] == "x" * 64
            assert rows[19999]["number"] == 19999
            assert (await c.query("SELECT toUInt64(5)"))[0]["toUInt64(5)"] == 5
            await asyncio.to_thread(c.close)

        asyncio.run(run())
