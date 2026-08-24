"""Deterministic, server-free unit tests for the ConnectionPool state machine.

These tests never touch a ClickHouse server or the native extension: the
pool is driven with fake factories and fake clients. Concurrency is
coordinated with ``threading.Event`` / ``Barrier`` — no timing-based sleeps —
and every worker thread is joined with a timeout and checked for liveness.
"""

from __future__ import annotations

import threading
import time
from typing import Any, List, Optional

import pytest

from st_clickhouse._errors import ConnectionError
from st_clickhouse._pool import ConnectionPool, PooledConnection

# Hard safety net: if a pool operation deadlocks while holding the condition
# lock, the whole test run would hang; fail instead.
pytestmark = pytest.mark.timeout(60)


# ══════════════════════════════════════════════════════════════════════════
# Fakes
# ══════════════════════════════════════════════════════════════════════════


class FakeClient:
    """Stand-in for a native client. Ping behaviour is scriptable."""

    def __init__(self, name: str):
        self.name = name
        self.ping_calls = 0
        self.touched_by_reaper = False
        self.ping_started: Optional[threading.Event] = None
        self.ping_gate: Optional[threading.Event] = None
        self.ping_error: Optional[BaseException] = None

    def ping(self) -> bool:
        self.ping_calls += 1
        if self.ping_started is not None:
            self.ping_started.set()
        if self.ping_gate is not None:
            if not self.ping_gate.wait(timeout=10):
                raise RuntimeError("ping gate never opened")
        if self.ping_error is not None:
            raise self.ping_error
        return True


class Gated:
    """Script item wrapper: block this specific factory call until released."""

    def __init__(
        self,
        item: Any,
        on_enter: Optional[threading.Event] = None,
        gate: Optional[threading.Event] = None,
    ):
        self.item = item
        self.on_enter = on_enter if on_enter is not None else threading.Event()
        self.gate = gate if gate is not None else threading.Event()


class Factory:
    """Scriptable client factory recording every call.

    Each call consumes one script item: a :class:`FakeClient` (served
    as-is), a name (wrapped in a new FakeClient), an exception (raised), a
    callable (invoked with the call number), or a :class:`Gated` wrapper.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.calls = 0
        self.created: List[FakeClient] = []
        self.script: List[Any] = []

    def __call__(self) -> Any:
        with self.lock:
            self.calls += 1
            call_no = self.calls
            item = self.script.pop(0) if self.script else f"c{call_no}"
        if isinstance(item, Gated):
            if item.on_enter is not None:
                item.on_enter.set()
            if item.gate is not None and not item.gate.wait(timeout=10):
                raise RuntimeError("factory gate never opened")
            item = item.item
        if isinstance(item, BaseException):
            raise item
        if callable(item):
            item = item(call_no)
        if isinstance(item, FakeClient):
            client = item
        else:
            client = FakeClient(str(item))
        with self.lock:
            self.created.append(client)
        return client


def make_pool(factory: Factory, **kwargs: Any) -> ConnectionPool:
    kwargs.setdefault("min_size", 0)
    kwargs.setdefault("max_size", 4)
    kwargs.setdefault("acquire_timeout", 5.0)
    kwargs.setdefault("reaper_interval", 0)  # no background reaper; steps run by hand
    return ConnectionPool(factory, **kwargs)


def assert_invariants(pool: ConnectionPool) -> None:
    """The documented state-machine invariants must hold whenever observed."""
    with pool._cond:
        assert len(pool._all) == len(pool._available) + len(pool._lent), (
            f"total={len(pool._all)} available={len(pool._available)} "
            f"lent={len(pool._lent)}"
        )
        assert set(pool._available).isdisjoint(pool._lent)
        assert len(pool._all) + pool._creating <= pool._max
        assert all(pc in pool._all for pc in pool._available)
        assert all(pc in pool._all for pc in pool._lent)


def join(threads: List[threading.Thread], timeout: float = 10.0) -> None:
    for t in threads:
        t.join(timeout=timeout)
    alive = [t.name for t in threads if t.is_alive()]
    assert not alive, f"threads did not terminate: {alive}"


# ══════════════════════════════════════════════════════════════════════════
# Growth and lending
# ══════════════════════════════════════════════════════════════════════════


class TestGrowth:
    def test_min_zero_growth_lends_distinct_clients_no_availability(self):
        factory = Factory()
        pool = make_pool(factory, min_size=0, max_size=4)

        assert pool.metrics["total"] == 0
        assert pool.metrics["available"] == 0

        clients = [pool.acquire() for _ in range(3)]
        assert len({id(c) for c in clients}) == 3  # distinct, never double-lent

        m = pool.metrics
        assert m["total"] == 3
        assert m["available"] == 0  # everything lent, nothing reusable
        assert m["in_use"] == 3
        assert m["creating"] == 0
        assert factory.calls == 3
        assert_invariants(pool)
        pool.close()

    def test_min_size_prefilled_available(self):
        factory = Factory()
        pool = make_pool(factory, min_size=2, max_size=4)
        m = pool.metrics
        assert m["total"] == 2
        assert m["available"] == 2
        assert m["in_use"] == 0
        assert factory.calls == 2
        assert_invariants(pool)
        pool.close()

    def test_growth_never_exceeds_max_under_concurrency(self):
        factory = Factory()
        pool = make_pool(factory, min_size=0, max_size=4, acquire_timeout=1.0)

        workers = 12
        barrier = threading.Barrier(workers)
        hold = threading.Event()  # successful acquirers keep their client
        outcome_lock = threading.Lock()
        outcomes: List[Any] = []  # client or exception, one per worker
        all_done = threading.Condition()

        def worker() -> None:
            barrier.wait(timeout=10)
            client: Any = None
            try:
                client = pool.acquire()
                result: Any = client
            except BaseException as e:  # noqa: BLE001 - recorded, asserted below
                result = e
            # Record the outcome first: the main thread counts outcomes to
            # decide when the acquire phase is over, and winners must still
            # be holding their client at that point.
            with outcome_lock:
                outcomes.append(result)
            with all_done:
                all_done.notify_all()
            if client is not None:
                hold.wait(timeout=10)
                pool.release(client)

        threads = [
            threading.Thread(target=worker, name=f"acq-{i}", daemon=True)
            for i in range(workers)
        ]
        for t in threads:
            t.start()

        deadline = time.monotonic() + 15
        with all_done:
            while len(outcomes) < workers:
                assert time.monotonic() < deadline, "acquirers never finished"
                all_done.wait(timeout=deadline - time.monotonic() + 1)

        ok = [r for r in outcomes if not isinstance(r, BaseException)]
        errs = [r for r in outcomes if isinstance(r, BaseException)]
        # Exactly max_size acquirers win; the rest time out while blocked.
        assert len(ok) == 4, f"unexpected success count: {len(ok)}"
        assert len(errs) == 8
        assert len({id(c) for c in ok}) == 4  # never double-lent
        assert all(isinstance(e, ConnectionError) for e in errs)
        assert all("exhausted" in str(e) for e in errs)
        # No overshoot: the factory ran exactly max_size times, once per slot.
        assert factory.calls == 4
        assert len(pool._all) == 4

        hold.set()
        join(threads)

        m = pool.metrics
        assert m["total"] == 4
        assert m["available"] == 4
        assert m["in_use"] == 0
        assert m["creating"] == 0
        assert_invariants(pool)
        pool.close()

    def test_metrics_create_in_progress_visible(self):
        factory = Factory()
        creating = Gated("c1", threading.Event(), threading.Event())
        factory.script = [creating]
        pool = make_pool(factory, min_size=0, max_size=2)

        got: List[Any] = []
        t = threading.Thread(target=lambda: got.append(pool.acquire()), daemon=True)
        t.start()
        assert creating.on_enter.wait(timeout=10)  # factory in flight

        m = pool.metrics  # must not block: metrics take the lock briefly
        assert m["creating"] == 1
        assert m["total"] == 0  # not committed yet
        assert m["in_use"] == 0  # creates are not "in use" until they commit
        assert_invariants(pool)

        creating.gate.set()
        join([t])
        assert got and isinstance(got[0], FakeClient)
        m = pool.metrics
        assert m["creating"] == 0
        assert m["total"] == 1
        assert m["in_use"] == 1
        assert_invariants(pool)
        pool.close()

    def test_factory_failure_rolls_back_capacity(self):
        factory = Factory()
        factory.script = [RuntimeError("no server")]
        pool = make_pool(factory, min_size=0, max_size=1)

        with pytest.raises(RuntimeError, match="no server"):
            pool.acquire()
        m = pool.metrics
        assert m["creating"] == 0  # reservation rolled back
        assert m["total"] == 0
        assert_invariants(pool)

        # Capacity is free again: a later acquire can create a connection.
        client = pool.acquire()
        assert isinstance(client, FakeClient)
        assert pool.metrics["in_use"] == 1
        assert_invariants(pool)
        pool.close()


# ══════════════════════════════════════════════════════════════════════════
# Release idempotence
# ══════════════════════════════════════════════════════════════════════════


class TestRelease:
    def test_double_release_does_not_duplicate_slot(self):
        factory = Factory()
        pool = make_pool(factory, min_size=1, max_size=2)

        client = pool.acquire()
        assert pool.metrics["available"] == 0

        pool.release(client)
        pool.release(client)  # double release: must be a no-op

        m = pool.metrics
        assert m["available"] == 1
        assert m["in_use"] == 0
        assert m["total"] == 1
        assert len(pool._available) == 1  # no deque duplication
        assert_invariants(pool)
        pool.close()

    def test_unknown_release_ignored(self):
        factory = Factory()
        pool = make_pool(factory, min_size=1, max_size=2)
        before = pool.metrics

        pool.release(FakeClient("stranger"))
        pool.release("not a client")
        pool.release(None)

        after = pool.metrics
        for key in ("total", "available", "in_use", "creating"):
            assert after[key] == before[key]
        assert_invariants(pool)
        pool.close()

    def test_release_after_close_is_noop(self):
        factory = Factory()
        pool = make_pool(factory, min_size=1, max_size=2)
        client = pool.acquire()
        pool.close()

        pool.release(client)  # must not resurrect anything

        assert pool._all == []
        assert len(pool._available) == 0
        assert len(pool._lent) == 0
        m = pool.metrics
        assert (m["total"], m["available"], m["in_use"]) == (0, 0, 0)
        with pytest.raises(ConnectionError, match="Pool is closed"):
            pool.acquire()
        assert_invariants(pool)

    def test_close_is_idempotent(self):
        factory = Factory()
        pool = make_pool(factory, min_size=2, max_size=2)
        pool.close()
        pool.close()  # second close must not raise
        assert pool.metrics["total"] == 0


# ══════════════════════════════════════════════════════════════════════════
# Close semantics
# ══════════════════════════════════════════════════════════════════════════


class TestClose:
    def test_close_wakes_blocked_waiters(self):
        factory = Factory()
        pool = make_pool(factory, min_size=0, max_size=1, acquire_timeout=30.0)

        held = pool.acquire()  # pool now full
        errors: List[BaseException] = []
        started = threading.Event()

        def waiter() -> None:
            started.set()
            try:
                pool.acquire()
            except BaseException as e:  # noqa: BLE001
                errors.append(e)

        t = threading.Thread(target=waiter, daemon=True, name="blocked-waiter")
        t.start()
        assert started.wait(timeout=10)

        pool.close()
        join([t])
        assert len(errors) == 1
        assert isinstance(errors[0], ConnectionError)
        assert "closed" in str(errors[0])
        pool.release(held)  # late release on a closed pool: no-op

    def test_close_during_create_discards_connection(self):
        factory = Factory()
        creating = Gated("doomed", threading.Event(), threading.Event())
        factory.script = [creating]
        pool = make_pool(factory, min_size=0, max_size=2)

        results: List[Any] = []

        def creator() -> None:
            try:
                results.append(pool.acquire())
            except BaseException as e:  # noqa: BLE001
                results.append(e)

        t = threading.Thread(target=creator, daemon=True, name="creator")
        t.start()
        assert creating.on_enter.wait(timeout=10)  # inside factory, post-reserve

        pool.close()
        creating.gate.set()  # factory returns a client into a closed pool
        join([t])

        assert len(results) == 1
        assert isinstance(results[0], ConnectionError)
        assert "closed" in str(results[0])
        # The created client was NOT added to the pool.
        assert pool._all == []
        assert pool.metrics["creating"] == 0
        assert pool.metrics["total"] == 0
        assert factory.created  # a client really was produced and discarded
        assert_invariants(pool)


# ══════════════════════════════════════════════════════════════════════════
# Health checks
# ══════════════════════════════════════════════════════════════════════════


class TestHealthCheck:
    def test_failed_replacement_frees_capacity_and_later_acquire_recovers(self):
        factory = Factory()
        dead = FakeClient("dead")
        dead.ping_error = RuntimeError("server gone")
        factory.script = [dead, RuntimeError("still down"), "fresh"]
        pool = make_pool(
            factory, min_size=0, max_size=1, health_check_interval=0.0
        )

        first = pool.acquire()  # growth path: no health check on fresh slots
        assert first is dead
        pool.release(first)

        # Idle enough (interval 0): ping fails AND replacement fails.
        with pytest.raises(ConnectionError) as exc_info:
            pool.acquire()
        msg = str(exc_info.value)
        assert "Health check failed" in msg
        assert "replacement" in msg
        assert "server gone" in msg
        assert "still down" in msg

        # The dead slot is gone entirely — capacity is free again.
        m = pool.metrics
        assert (m["total"], m["available"], m["in_use"], m["creating"]) == (0, 0, 0, 0)
        assert len(pool._all) == 0
        assert_invariants(pool)

        # Recovery: a later acquire grows a brand-new connection.
        recovered = pool.acquire()
        assert isinstance(recovered, FakeClient)
        assert recovered is not dead
        assert recovered.name == "fresh"
        assert factory.calls == 3
        assert pool.metrics["in_use"] == 1
        assert_invariants(pool)
        pool.close()

    def test_base_exception_during_ping_drops_reserved_slot(self):
        factory = Factory()
        dead = FakeClient("dead")
        dead.ping_error = KeyboardInterrupt()
        factory.script = [dead]
        pool = make_pool(
            factory, min_size=0, max_size=1, health_check_interval=0.0
        )

        assert pool.acquire() is dead
        pool.release(dead)
        with pytest.raises(KeyboardInterrupt):
            pool.acquire()

        assert pool.metrics["total"] == 0
        assert pool.metrics["in_use"] == 0
        assert_invariants(pool)
        recovered = pool.acquire()
        assert recovered is not dead
        pool.close()

    def test_base_exception_during_replacement_drops_reserved_slot(self):
        factory = Factory()
        dead = FakeClient("dead")
        dead.ping_error = RuntimeError("server gone")
        factory.script = [dead, KeyboardInterrupt()]
        pool = make_pool(
            factory, min_size=0, max_size=1, health_check_interval=0.0
        )

        assert pool.acquire() is dead
        pool.release(dead)
        with pytest.raises(KeyboardInterrupt):
            pool.acquire()

        assert pool.metrics["total"] == 0
        assert pool.metrics["in_use"] == 0
        assert_invariants(pool)
        recovered = pool.acquire()
        assert recovered is not dead
        pool.close()

    def test_successful_replacement_stays_lent(self):
        factory = Factory()
        stale = FakeClient("stale")
        stale.ping_error = RuntimeError("stale socket")
        replacement = FakeClient("replacement")
        factory.script = [stale, replacement]
        pool = make_pool(
            factory, min_size=0, max_size=1, health_check_interval=0.0
        )

        assert pool.acquire() is stale  # fresh slot: no check
        pool.release(stale)

        served = pool.acquire()  # ping fails -> replacement created and served
        assert served is replacement
        assert stale.ping_calls == 1

        m = pool.metrics
        assert m["total"] == 1  # same slot, swapped client
        assert m["in_use"] == 1  # still lent, not available
        assert m["available"] == 0
        assert len(pool._available) == 0
        assert_invariants(pool)

        # The old client object is no longer tracked: releasing it is a no-op.
        pool.release(stale)
        assert pool.metrics["in_use"] == 1
        assert pool.metrics["available"] == 0

        # Releasing the replacement returns the (single) slot normally.
        pool.release(replacement)
        assert pool.metrics["in_use"] == 0
        assert pool.metrics["available"] == 1
        assert_invariants(pool)
        pool.close()

    def test_healthy_ping_returns_same_client(self):
        factory = Factory()
        pool = make_pool(
            factory, min_size=1, max_size=2, health_check_interval=0.0
        )
        client = pool.acquire()  # available slot: ping succeeds
        assert client is factory.created[0]
        assert factory.created[0].ping_calls == 1
        m = pool.metrics
        assert m["in_use"] == 1
        assert m["total"] == 1
        assert_invariants(pool)
        pool.release(client)
        pool.close()

    def test_slow_health_check_does_not_block_other_acquires(self):
        factory = Factory()
        slow = FakeClient("slow")
        slow.ping_started = threading.Event()
        slow.ping_gate = threading.Event()
        factory.script = [slow, "second"]
        pool = make_pool(
            factory, min_size=0, max_size=2, health_check_interval=0.0,
            acquire_timeout=10.0,
        )

        assert pool.acquire() is slow  # growth: creates `slow`
        pool.release(slow)  # now available, and stale enough to be checked

        checker_results: List[Any] = []
        checker_errors: List[BaseException] = []

        def checker() -> None:
            try:
                checker_results.append(pool.acquire())
            except BaseException as e:  # noqa: BLE001
                checker_errors.append(e)

        t1 = threading.Thread(target=checker, daemon=True, name="health-checker")
        t1.start()
        # Wait until the health check is actually inside ping().
        assert slow.ping_started.wait(timeout=10)

        # While ping() blocks, another acquire must still succeed by growing:
        # ping runs outside the condition lock, so the pool stays usable.
        second_results: List[Any] = []
        t2_done = threading.Event()

        def second() -> None:
            second_results.append(pool.acquire())
            t2_done.set()

        t2 = threading.Thread(target=second, daemon=True, name="second-acquirer")
        t2.start()
        assert t2_done.wait(timeout=10), "acquire blocked behind a slow health check"

        assert len(second_results) == 1
        second_client = second_results[0]
        assert isinstance(second_client, FakeClient)
        assert second_client is not slow
        assert factory.calls == 2

        m = pool.metrics  # slow slot lent (reserved for its health check)
        assert m["in_use"] == 2
        assert m["available"] == 0
        assert_invariants(pool)

        slow.ping_gate.set()  # let the health check finish: ping succeeds
        join([t1, t2])
        assert not checker_errors, checker_errors
        assert checker_results == [slow]

        assert_invariants(pool)
        pool.close()

    def test_close_during_health_check_does_not_resurrect(self):
        factory = Factory()
        stale = FakeClient("stale")
        stale.ping_error = RuntimeError("stale socket")
        stale.ping_started = threading.Event()
        stale.ping_gate = threading.Event()
        replacement = FakeClient("replacement")
        replacement_entered = threading.Event()
        replacement_gate = threading.Event()
        factory.script = [stale, Gated(replacement, replacement_entered, replacement_gate)]
        pool = make_pool(
            factory, min_size=0, max_size=1, health_check_interval=0.0
        )

        pool.acquire()
        pool.release(stale)

        errors: List[BaseException] = []

        def checker() -> None:
            try:
                pool.acquire()
            except BaseException as e:  # noqa: BLE001
                errors.append(e)

        t = threading.Thread(target=checker, daemon=True, name="health-checker")
        t.start()
        assert stale.ping_started.wait(timeout=10)  # inside ping()

        pool.close()
        stale.ping_gate.set()  # ping fails -> replacement factory runs
        assert replacement_entered.wait(timeout=10)  # inside replacement factory
        replacement_gate.set()  # client produced into a closed pool
        join([t])

        assert len(errors) == 1
        assert isinstance(errors[0], ConnectionError)
        assert "closed" in str(errors[0])
        # The replacement was never added; state stays empty.
        assert pool._all == []
        assert pool.metrics["total"] == 0
        assert_invariants(pool)


# ══════════════════════════════════════════════════════════════════════════
# Reaper
# ══════════════════════════════════════════════════════════════════════════


class TestReaper:
    def test_reaper_never_touches_lent_connections(self):
        factory = Factory()
        pool = make_pool(factory, min_size=0, max_size=3, max_idle_time=100.0)

        held = pool.acquire()  # lent and idle — but invisible to the reaper
        idle = pool.acquire()
        pool.release(idle)
        # Backdate the available slot so it is reaping-eligible.
        with pool._cond:
            for pc in pool._available:
                pc.last_used -= 1000.0

        reaped = pool._reap_once()
        assert reaped == 1
        assert held.ping_calls == 0  # lent client untouched
        m = pool.metrics
        assert m["total"] == 1
        assert m["in_use"] == 1
        assert m["available"] == 0
        assert_invariants(pool)
        pool.close()

    def test_reaper_respects_min_size(self):
        factory = Factory()
        pool = make_pool(factory, min_size=2, max_size=4, max_idle_time=100.0)

        a, b, c = pool.acquire(), pool.acquire(), pool.acquire()
        for client in (a, b, c):
            pool.release(client)
        with pool._cond:
            for pc in pool._available:
                pc.last_used -= 1000.0

        reaped = pool._reap_once()
        assert reaped == 1  # 3 available, min 2 -> at most 1 reaped
        m = pool.metrics
        assert m["total"] == 2
        assert m["available"] == 2
        assert_invariants(pool)

        assert pool._reap_once() == 0  # already at min_size
        assert_invariants(pool)
        pool.close()

    def test_reap_frees_capacity_for_blocked_acquirer(self):
        factory = Factory()
        pool = make_pool(factory, min_size=0, max_size=1, max_idle_time=0.0)

        first = pool.acquire()
        pool.release(first)  # idle beyond max_idle_time=0 -> reaping-eligible

        holder = pool.acquire()  # pool full again: total == max
        assert pool.metrics["in_use"] == 1

        reaped = pool._reap_once()  # nothing available: must not reap the lent one
        assert reaped == 0
        assert pool._all  # the lent slot survived
        pool.release(holder)

        # Now it is available and idle; a reaper pass frees the capacity ...
        with pool._cond:
            for pc in pool._available:
                pc.last_used -= 10.0
        assert pool._reap_once() == 1
        assert pool.metrics["total"] == 0
        # ... so a later acquire can grow again instead of waiting forever.
        recovered = pool.acquire()
        assert isinstance(recovered, FakeClient)
        assert pool.metrics["total"] == 1
        assert_invariants(pool)
        pool.close()


# ══════════════════════════════════════════════════════════════════════════
# Configuration validation
# ══════════════════════════════════════════════════════════════════════════


class TestConfigValidation:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"min_size": -1},
            {"max_size": 0},
            {"min_size": 4, "max_size": 2},
            {"acquire_timeout": -1.0},
            {"health_check_interval": -1.0},
            {"max_idle_time": -1.0},
            {"reaper_interval": -1.0},
        ],
    )
    def test_invalid_config_rejected(self, kwargs: Any) -> None:
        with pytest.raises(ValueError):
            ConnectionPool(Factory(), **kwargs)

    def test_zero_acquire_timeout_fails_fast_when_full(self):
        factory = Factory()
        pool = make_pool(factory, min_size=0, max_size=1, acquire_timeout=0.0)
        pool.acquire()
        with pytest.raises(ConnectionError, match="exhausted"):
            pool.acquire()
        assert_invariants(pool)
        pool.close()
