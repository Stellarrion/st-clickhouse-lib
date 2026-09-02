"""Regression tests: pool admission under high concurrency (v0.3 bench bug).

Symptom (before the fix): 32 concurrent ``SELECT 1`` on the bench pool
(``pool_min_size=4, pool_max_size=4``) made every acquire that did not
immediately get a slot fail at exactly ``pool_acquire_timeout`` (30s),
while the four in-flight queries succeeded. 8/16/24 concurrency passed.

Root cause: ``AsyncClient._acquire`` ran ``pool.acquire()`` on the loop's
*default* executor. Acquire waiters block on the pool Condition; the query
work items that release slots and wake them share the same bounded FIFO
executor. With 28 default-executor threads (``min(32, cpu+4)``) and
``pool_max_size=4``, 32 concurrent queries park 28 waiters on every thread,
so the four queued query items can never start, no slot ever frees, and
every waiter times out. The fix routes acquires to a dedicated bounded
executor ("ch-pool-acquire"), leaving default-executor threads free for
query work.

The same change made ``_run_pooled._run``'s ``finally`` the only release
site: the previous second, op-level release could race with the slot being
re-acquired and steal it from its new owner (observed as a flaky
"Pool is closed" failure ~2ms into 24-concurrency bursts).

These tests require a running ClickHouse server (same gating as
``test_client.py``: ``CLICKHOUSE_HOST`` / ``CLICKHOUSE_USER`` /
``CLICKHOUSE_PASS``).
"""

from __future__ import annotations

import asyncio
import os
import threading
import time

import pytest

from st_clickhouse import AsyncClient, ConnectionError

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1:9000")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASS = os.environ.get("CLICKHOUSE_PASS", "test")

pytestmark = pytest.mark.timeout(120)


def _client(**pool_kwargs) -> AsyncClient:
    return AsyncClient(
        CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASS,
        **pool_kwargs,
    )


async def _burst(client: AsyncClient, n: int, budget_s: float = 5.0) -> float:
    """Run ``n`` concurrent SELECT 1; assert correctness inside the budget.

    ``wait_for(30)`` is the generous hard safety net; the regression bound
    itself is ``budget_s`` (the bug failed at exactly 30.0s — an order of
    magnitude above this budget).
    """
    t0 = time.monotonic()
    rows = await asyncio.wait_for(
        asyncio.gather(*(client.query("SELECT 1 AS x") for _ in range(n))),
        timeout=30.0,
    )
    elapsed = time.monotonic() - t0
    assert len(rows) == n, f"expected {n} results, got {len(rows)}"
    assert all(r == [{"x": 1}] for r in rows), f"wrong rows in burst results: {rows[:2]!r}"
    assert elapsed < budget_s, (
        f"{n} concurrent SELECT 1 took {elapsed:.2f}s "
        f"(pool-admission regression: expected well under {budget_s}s)"
    )
    return elapsed


class TestPoolBurstConcurrency:
    async def test_32_concurrent_default_pool(self):
        """32 concurrent SELECT 1 on the default pool stay well under 10s."""
        async with _client() as client:
            await _burst(client, 32)

    async def test_32_concurrent_bench_pool(self):
        """The v0.3 bench config: min4/max4 — the tightest admission case.

        With a 28-wide default executor, 32 concurrent queries on max=4
        park exactly 28 waiters: the pre-fix self-starvation point.
        """
        async with _client(pool_min_size=4, pool_max_size=4) as client:
            await _burst(client, 32)

    async def test_64_concurrent_beyond_executor_width(self):
        """Bursts past the acquire-executor width still drain via FIFO."""
        async with _client(pool_min_size=4, pool_max_size=4) as client:
            await _burst(client, 64)


class TestAcquireExecutorLifecycle:
    async def test_close_reaps_acquire_executor_threads(self):
        """close() drains the dedicated acquire executor: no thread leak."""
        client = _client(pool_min_size=4, pool_max_size=4)
        await _burst(client, 32)
        assert any(
            t.name.startswith("ch-pool-acquire") for t in threading.enumerate()
        ), "expected parked acquire threads after a burst"
        client.close()
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if not any(
                t.name.startswith("ch-pool-acquire") for t in threading.enumerate()
            ):
                break
            await asyncio.sleep(0.05)
        assert not any(
            t.name.startswith("ch-pool-acquire") for t in threading.enumerate()
        ), "acquire executor threads survived close()"
        with pytest.raises(ConnectionError):
            await client.query("SELECT 1")
