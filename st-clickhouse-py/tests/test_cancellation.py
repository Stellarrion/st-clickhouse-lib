"""Cancellation and pool-release safety tests.

These tests require a running ClickHouse server (same gating as
``test_client.py``: ``CLICKHOUSE_HOST`` / ``CLICKHOUSE_USER`` /
``CLICKHOUSE_PASS``). They verify the cancellation contract:

- cancelling a task that awaits a one-shot query stops the server-side
  query (the pooled connection is killed and replaced), verified through
  ``system.query_log`` when accessible;
- abandoning ``query_stream`` early never recycles the connection (the old
  behavior desynced the next query with ``unknown packet type: 0``);
- ``cancel()`` fails closed with guidance;
- fully-consumed streams release their connection cleanly and the pool
  never leaks lent slots.

No test sleeps past generous query timeouts; waits are bounded polls.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any, List

import pytest

from st_clickhouse import ConnectionError, connect, connect_async

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1:9000")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASS = os.environ.get("CLICKHOUSE_PASS", "test")

# Hard safety net against deadlocks; every wait below is a bounded poll.
pytestmark = pytest.mark.timeout(120)

# Bounds: sleep(3) queries must be cut short well before this.
ABORT_BUDGET_S = 2.0


async def _wait_until(predicate, timeout: float = 10.0, interval: float = 0.02) -> bool:
    """Bounded poll; returns whether predicate() became truthy."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


def _wait_until_sync(predicate, timeout: float = 10.0, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _pool_settled(client: "AsyncClient") -> bool:
    m = client.metrics
    return m["in_use"] == 0


async def _query_log_durations(client: Any, tag: str) -> List[dict]:
    """Server-side durations for queries tagged with ``tag`` (best effort).

    Columns are cast: stored ``system.query_log`` columns can arrive with
    custom (sparse) serialization depending on data, which the sync engine
    does not decode; casts produce standard serializations.
    """
    try:
        await client.execute("SYSTEM FLUSH LOGS")
    except Exception:
        pass  # no flush rights: query_log flushes on its own schedule
    return await client.query(
        "SELECT toUInt8(type) AS type, toUInt64(query_duration_ms) AS dur_ms "
        "FROM system.query_log "
        f"WHERE query LIKE '%{tag}%' AND type IN (2, 4)"
    )


# ══════════════════════════════════════════════════════════════════════════
# Task cancellation stops the server-side query
# ══════════════════════════════════════════════════════════════════════════


class TestTaskCancellation:
    async def test_cancel_stops_server_query(self):
        """task.cancel() aborts the running SELECT sleep(3) server-side."""
        tag = uuid.uuid4().hex[:8]
        async with connect_async(
            CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            pool_min_size=1,
            pool_max_size=2,
        ) as c:
            task = asyncio.create_task(
                c.query(f"SELECT sleep(3) AS x /* cancel_{tag} */")
            )
            # Deterministic setup: wait until the query holds a connection,
            # then let it reach the server before cancelling.
            assert await _wait_until(lambda: c.metrics["in_use"] == 1)
            await asyncio.sleep(0.3)

            t0 = time.monotonic()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            elapsed = time.monotonic() - t0
            # The awaiting task unwinds in O(1), not after the query ends.
            assert elapsed < 1.0, f"cancel took {elapsed:.2f}s"

            # Pool stays usable immediately (replacement on next acquire).
            rows = await c.query("SELECT 42 AS x")
            assert rows[0]["x"] == 42

            # Server-side evidence: the query was aborted well under 3s.
            async def durations():
                try:
                    return await _query_log_durations(c, f"cancel_{tag}")
                except Exception:
                    return []

            rows = []
            for _ in range(80):  # bounded: log flush latency
                try:
                    rows = await durations()
                except Exception:
                    rows = []  # query_log not readable on this server
                    break
                if rows:
                    break
                await asyncio.sleep(0.1)
            if not rows:
                pytest.skip("system.query_log finish entry not observable")
            for row in rows:
                assert row["dur_ms"] < ABORT_BUDGET_S * 1000, row

            # No leaked lent slots.
            assert await _wait_until(lambda: _pool_settled(c), timeout=15.0)
            assert c.metrics["total"] <= c.metrics["max_size"]

    async def test_cancel_repeated_pool_stable(self):
        """Repeated cancel-then-query rounds keep the pool leak-free."""
        async with connect_async(
            CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            pool_min_size=1,
            pool_max_size=3,
        ) as c:
            for _ in range(5):
                task = asyncio.create_task(c.query("SELECT sleep(3) AS x"))
                assert await _wait_until(lambda: c.metrics["in_use"] >= 1)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                rows = await c.query("SELECT 1 AS x")
                assert rows[0]["x"] == 1
            assert await _wait_until(lambda: _pool_settled(c), timeout=15.0)
            assert c.metrics["total"] <= c.metrics["max_size"]

    async def test_cancel_during_acquire_does_not_leak(self):
        """Cancelling a task blocked on pool acquire frees nothing extra."""
        async with connect_async(
            CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            pool_min_size=1,
            pool_max_size=1,
        ) as c:
            first = asyncio.create_task(c.query("SELECT sleep(0.5) AS x"))
            assert await _wait_until(lambda: c.metrics["in_use"] == 1)

            blocked = asyncio.create_task(c.query("SELECT 2 AS x"))
            await asyncio.sleep(0.05)  # let it enter the acquire wait
            blocked.cancel()
            with pytest.raises(asyncio.CancelledError):
                await blocked

            first_rows = await first
            assert first_rows == [{"x": 0}]  # sleep() returns 0
            # The late acquire result must not leak or duplicate a slot.
            assert await _wait_until(lambda: _pool_settled(c), timeout=15.0)
            assert c.metrics["total"] <= 1
            rows = await c.query("SELECT 3 AS x")
            assert rows[0]["x"] == 3

    async def test_cancel_session_destroys_session_not_pool(self):
        """Cancelling a session query kills the pinned connection only."""
        async with connect_async(
            CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            pool_min_size=1,
            pool_max_size=2,
        ) as c:
            async with c.session() as s:
                task = asyncio.create_task(s.execute("SELECT sleep(3)"))
                await asyncio.sleep(0.3)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                # The session's pinned connection identity is gone.
                with pytest.raises(ConnectionError, match="destroyed"):
                    await s.query("SELECT 1")
            # The pool itself keeps working.
            rows = await c.query("SELECT 4 AS x")
            assert rows[0]["x"] == 4
            assert await _wait_until(lambda: _pool_settled(c), timeout=15.0)


# ══════════════════════════════════════════════════════════════════════════
# Stream abandonment
# ══════════════════════════════════════════════════════════════════════════


class TestAsyncStreamAbandon:
    async def test_break_then_query_no_desync(self):
        """Early break recycles nothing: the next query must succeed.

        Regression: the connection used to be returned to the pool while the
        server still streamed the old response, desyncing the next query
        with ``unknown packet type: 0``.
        """
        async with connect_async(
            CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            pool_min_size=1,
            pool_max_size=2,
        ) as c:
            for _ in range(3):
                # > 32 blocks: the reader cannot buffer the whole response,
                # so the abandon path must destroy, not recycle.
                async for _block in c.query_stream(
                    "SELECT number FROM numbers(10000000)"
                ):
                    break  # abandon mid-response
                rows = await c.query("SELECT 42 AS x")
                assert rows[0]["x"] == 42
            assert await _wait_until(lambda: _pool_settled(c), timeout=15.0)
            assert c.metrics["total"] <= c.metrics["max_size"]

    async def test_break_stops_server_query(self):
        """Breaking a slow stream aborts the server-side query.

        Full run is ~3s (3000 blocks x sleepEachRow(0.001)); the abandon must
        cut it far short.
        """
        tag = uuid.uuid4().hex[:8]
        async with connect_async(
            CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            pool_min_size=1,
            pool_max_size=2,
        ) as c:
            async for _block in c.query_stream(
                f"SELECT number, sleepEachRow(0.001) FROM numbers(3000) "
                f"SETTINGS max_block_size=1 /* stream_{tag} */"
            ):
                break
            rows = await c.query("SELECT 1 AS x")
            assert rows[0]["x"] == 1

            log_rows = []
            for _ in range(80):
                try:
                    log_rows = await _query_log_durations(c, f"stream_{tag}")
                except Exception:
                    log_rows = []  # query_log not readable on this server
                    break
                if log_rows:
                    break
                await asyncio.sleep(0.1)
            if not log_rows:
                pytest.skip("system.query_log finish entry not observable")
            for row in log_rows:
                assert row["dur_ms"] < 1500, row

    async def test_task_cancel_during_stream(self):
        """task.cancel() inside an async-for cleans up like a break."""
        async with connect_async(
            CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            pool_min_size=1,
            pool_max_size=2,
        ) as c:
            async def consume():
                async for _block in c.query_stream(
                    "SELECT number FROM numbers(10000000)"
                ):
                    await asyncio.sleep(3600)  # park with the stream open

            task = asyncio.create_task(consume())
            await asyncio.sleep(0.3)  # let the stream start and park
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            rows = await c.query("SELECT 43 AS x")
            assert rows[0]["x"] == 43
            assert await _wait_until(lambda: _pool_settled(c), timeout=15.0)

    async def test_full_stream_releases_cleanly(self):
        """A fully-consumed stream keeps the pool size constant (no leak)."""
        async with connect_async(
            CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            pool_min_size=1,
            pool_max_size=2,
        ) as c:
            total_before = c.metrics["total"]
            for _ in range(3):
                count = 0
                async for block in c.query_stream(
                    "SELECT number FROM numbers(1000)"
                ):
                    count += block.row_count()
                assert count == 1000
                rows = await c.query("SELECT 1 AS x")
                assert rows[0]["x"] == 1
            assert await _wait_until(lambda: _pool_settled(c), timeout=15.0)
            # The pool may have grown by one if the follow-up query raced the
            # forwarder's (asynchronous) clean release — that is healthy.
            # What must hold: bounded size, everything available, nothing lent.
            m = c.metrics
            assert m["total"] <= m["max_size"]
            assert m["available"] == m["total"]
            assert m["total"] >= total_before  # never shrank below min fill

    async def test_server_error_stream_leaves_pool_usable(self):
        """A stream that ends in a server exception releases cleanly."""
        async with connect_async(
            CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            pool_min_size=1,
            pool_max_size=2,
        ) as c:
            with pytest.raises(Exception):
                async for _block in c.query_stream(
                    "SELECT nonexistent_column_xyz FROM system.numbers LIMIT 1"
                ):
                    pass
            rows = await c.query("SELECT 44 AS x")
            assert rows[0]["x"] == 44
            assert await _wait_until(lambda: _pool_settled(c), timeout=15.0)


class TestSyncStreamAbandon:
    def test_break_discards_client_no_desync(self):
        """Early break closes the client instead of desyncing it.

        Regression: the follow-up query used to fail with
        ``unknown packet type: 0``; now the abandoned client reports a clear
        ConnectionError and a fresh client works.
        """
        c = connect(
            CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        )
        # > 32 blocks: the reader cannot buffer the whole response.
        for _block in c.query_stream("SELECT number FROM numbers(10000000)"):
            break
        with pytest.raises(ConnectionError, match="discarded"):
            c.query("SELECT 1")
        c.close()

        c2 = connect(
            CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        )
        assert c2.query("SELECT 45 AS x")[0]["x"] == 45
        c2.close()

    def test_full_stream_keeps_client_usable(self):
        c = connect(
            CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        )
        count = 0
        for block in c.query_stream("SELECT number FROM numbers(1000)"):
            count += block.row_count()
        assert count == 1000
        assert c.query("SELECT 46 AS x")[0]["x"] == 46
        c.close()

    def test_server_error_stream_keeps_client_usable(self):
        c = connect(
            CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        )
        with pytest.raises(Exception):
            for _block in c.query_stream(
                "SELECT nonexistent_column_xyz FROM system.numbers LIMIT 1"
            ):
                pass
        assert c.query("SELECT 47 AS x")[0]["x"] == 47
        c.close()

    def test_stream_cancel_discards_client(self):
        c = connect(
            CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        )
        stream = c.query_stream("SELECT number FROM numbers(10000000)")
        next(iter(stream))
        stream.cancel()
        with pytest.raises(ConnectionError, match="discarded"):
            c.query("SELECT 1")
        c.close()


# ══════════════════════════════════════════════════════════════════════════
# cancel() fails closed
# ══════════════════════════════════════════════════════════════════════════


class TestCancelFailsClosed:
    def test_sync_client_cancel_raises(self):
        c = connect(
            CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        )
        with pytest.raises(RuntimeError, match="cannot cancel a running query"):
            c.cancel()
        # The client is unaffected by the failed cancel.
        assert c.query("SELECT 48 AS x")[0]["x"] == 48
        c.close()

    async def test_async_client_cancel_raises(self):
        async with connect_async(
            CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        ) as c:
            with pytest.raises(RuntimeError, match="cannot cancel a running query"):
                await c.cancel()
            rows = await c.query("SELECT 49 AS x")
            assert rows[0]["x"] == 49

    async def test_async_session_cancel_raises(self):
        async with connect_async(
            CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        ) as c:
            async with c.session() as s:
                with pytest.raises(RuntimeError, match="cannot cancel a running query"):
                    await s.cancel()
                rows = await s.query("SELECT 50 AS x")
                assert rows[0]["x"] == 50

    def test_sync_cancel_while_idle_matches_documented_error(self):
        c = connect(
            CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        )
        with pytest.raises(RuntimeError, match="query_timeout"):
            c.cancel()
        c.close()
