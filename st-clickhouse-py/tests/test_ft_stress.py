"""Free-threaded (CPython 3.14t) stress tests for shared-state safety.

These tests run ONLY on a free-threaded interpreter — detected via
``sysconfig.get_config_var("Py_GIL_DISABLED")`` — and skip everywhere else
(GIL 3.12/3.13 builds are exercised by the regular suite; the GIL already
serializes the code paths stressed here, so these tests add no signal
there).

Stress axes, all against a live ClickHouse server:

1. One shared sync ``Client`` hammered by 8 threads × 200 ``SELECT 1``
   loops. The native ``_Client`` serializes calls through an internal
   mutex; this proves the Python wrapper and the Rust extension stay
   correct when bytecode truly runs in parallel.
2. Raw acquire/release churn on a real ``ConnectionPool`` backed by real
   native clients: 8 threads rapidly check connections out and back in,
   forcing pool growth from ``min_size`` under contention.
3. ``AsyncClient`` burst: 32 concurrent one-shot queries through the
   pooled async path (dedicated acquire executor + default-executor
   query workers).

Aftermath checks everywhere: every result correct, no exceptions, and
pool metrics settle to ``in_use == 0`` with ``total <= max_size``.

Requires a running ClickHouse server (same gating as ``test_client.py``:
``CLICKHOUSE_HOST`` / ``CLICKHOUSE_USER`` / ``CLICKHOUSE_PASS``).
"""

from __future__ import annotations

import asyncio
import os
import sys
import sysconfig
import threading
import time
from typing import Any, Callable, Dict, List

import pytest

from st_clickhouse import AsyncClient, connect
from st_clickhouse._native import _Client as NativeClient
from st_clickhouse._pool import ConnectionPool

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1:9000")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASS = os.environ.get("CLICKHOUSE_PASS", "test")

THREADS = 8
ITERATIONS = 200
ASYNC_BURST = 32

IS_FREE_THREADED = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))

pytestmark = [
    pytest.mark.timeout(300),
    pytest.mark.skipif(
        sys.version_info < (3, 14) or not IS_FREE_THREADED,
        reason="free-threading stress: requires CPython 3.14t+ "
        "(Py_GIL_DISABLED); GIL builds are covered by the regular suite",
    ),
]


# ══════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════

def _assert_settled(metrics: Dict[str, Any], max_size: int) -> Dict[str, Any]:
    """Assert the documented settle state of pool metrics."""
    assert metrics["in_use"] == 0, f"pool did not settle: {metrics}"
    assert metrics["creating"] == 0, f"factory calls still in flight: {metrics}"
    assert metrics["total"] <= max_size, f"pool exceeded max_size: {metrics}"
    assert metrics["total"] == metrics["available"] + metrics["in_use"], (
        f"slot accounting broken: {metrics}"
    )
    return metrics


def _wait_settled(metrics_fn: Callable[[], Dict[str, Any]], max_size: int,
                  deadline_s: float = 30.0) -> Dict[str, Any]:
    """Poll sync metrics until in_use/creating drain to zero, then assert."""
    deadline = time.monotonic() + deadline_s
    metrics = metrics_fn()
    while metrics["in_use"] or metrics["creating"]:
        if time.monotonic() >= deadline:
            break
        time.sleep(0.05)
        metrics = metrics_fn()
    return _assert_settled(metrics, max_size)


async def _await_settled(client: AsyncClient, max_size: int,
                         deadline_s: float = 30.0) -> Dict[str, Any]:
    """Async variant of :func:`_wait_settled` (never blocks the loop)."""
    deadline = time.monotonic() + deadline_s
    metrics = client.metrics
    while metrics["in_use"] or metrics["creating"]:
        if time.monotonic() >= deadline:
            break
        await asyncio.sleep(0.05)
        metrics = client.metrics
    return _assert_settled(metrics, max_size)


def _spawn_and_join(name_prefix: str, workers: List[threading.Thread],
                    join_timeout: float = 240.0) -> None:
    """Start every worker, then join each with a generous timeout."""
    for t in workers:
        t.start()
    for t in workers:
        t.join(timeout=join_timeout)
        assert not t.is_alive(), f"{name_prefix} thread {t.name} did not finish"


# ══════════════════════════════════════════════════════════════════════════
# 1. Shared sync Client under an 8-thread hammer
# ══════════════════════════════════════════════════════════════════════════

class TestSyncSharedClientHammer:
    def test_8_threads_hammer_one_shared_client(self):
        """8 threads × 200 SELECT 1 on ONE shared sync Client.

        The wrapper object, its native mutex and the result conversion
        all run under true parallelism; any result corruption, lost
        wakeup, or cross-thread state bleed shows up here.
        """
        client = connect(
            CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
        )
        try:
            errors: List[BaseException] = []
            wrong: List[Any] = []
            counts = [0] * THREADS
            barrier = threading.Barrier(THREADS)

            def hammer(idx: int) -> None:
                try:
                    barrier.wait(timeout=30.0)
                    for _ in range(ITERATIONS):
                        rows = client.query("SELECT 1 AS x")
                        if rows != [{"x": 1}]:
                            wrong.append((idx, rows))
                            return
                        counts[idx] += 1
                except BaseException as e:  # recorded, asserted below
                    errors.append(e)

            workers = [
                threading.Thread(
                    target=hammer, args=(i,), name=f"ft-hammer-{i}", daemon=True
                )
                for i in range(THREADS)
            ]
            _spawn_and_join("hammer", workers)

            assert not errors, f"hammer threads raised: {errors!r}"
            assert not wrong, f"hammer threads saw wrong rows: {wrong[:2]!r}"
            assert sum(counts) == THREADS * ITERATIONS, (
                f"completed iteration counts {counts} != {THREADS}x{ITERATIONS}"
            )
        finally:
            client.close()


# ══════════════════════════════════════════════════════════════════════════
# 2. Concurrent pool acquire/release churn
# ══════════════════════════════════════════════════════════════════════════

class TestPoolAcquireReleaseChurn:
    def test_concurrent_acquire_release_churn(self):
        """8 threads rapidly check real connections out of / into the pool.

        Mirrors ``AsyncClient``'s own factory (native ``_Client``), so the
        churn exercises the real lending state machine: growth from
        ``min_size=2`` under 8-way contention, ping health checks on
        freshly handed-out slots, and clean release bookkeeping.
        """
        pool = ConnectionPool(
            lambda: NativeClient(
                CLICKHOUSE_HOST,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASS,
            ),
            min_size=2,
            max_size=8,
            acquire_timeout=30.0,
        )
        try:
            errors: List[BaseException] = []
            bad_ping: List[Any] = []
            barrier = threading.Barrier(THREADS)

            def churn(idx: int) -> None:
                try:
                    barrier.wait(timeout=30.0)
                    for i in range(ITERATIONS):
                        native = pool.acquire()
                        try:
                            # Occasional health check on the lent slot —
                            # validates the connection is really usable.
                            if i % 16 == 0:
                                if native.ping() is not True:
                                    bad_ping.append((idx, i))
                                    return
                        finally:
                            pool.release(native)
                except BaseException as e:  # recorded, asserted below
                    errors.append(e)

            workers = [
                threading.Thread(
                    target=churn, args=(i,), name=f"ft-churn-{i}", daemon=True
                )
                for i in range(THREADS)
            ]
            _spawn_and_join("churn", workers)

            assert not errors, f"churn threads raised: {errors!r}"
            assert not bad_ping, f"ping on lent slot failed: {bad_ping!r}"
            settled = _wait_settled(lambda: pool.metrics, max_size=8)
            assert settled["total"] >= 2, (
                f"pool below min_size after churn: {settled}"
            )
        finally:
            pool.close()


# ══════════════════════════════════════════════════════════════════════════
# 3. AsyncClient burst of 32 concurrent queries
# ══════════════════════════════════════════════════════════════════════════

class TestAsyncBurst32:
    async def test_32_concurrent_queries_one_async_client(self):
        """32 concurrent SELECT 1 through one pooled AsyncClient.

        Drives the full async path — dedicated acquire executor,
        default-executor query workers, slot release in ``finally`` —
        with real parallelism between the Python threads involved.
        """
        async with AsyncClient(
            CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            pool_min_size=4,
            pool_max_size=8,
        ) as client:
            rows = await asyncio.wait_for(
                asyncio.gather(
                    *(client.query("SELECT 1 AS x") for _ in range(ASYNC_BURST))
                ),
                timeout=120.0,
            )
            assert len(rows) == ASYNC_BURST
            assert all(r == [{"x": 1}] for r in rows), (
                f"wrong rows in burst results: {rows[:2]!r}"
            )
            await _await_settled(client, max_size=8)
