"""Thread-safe pool of native clients with an explicit lending state machine.

State (every field is guarded by ``self._cond``):

- ``_all``: every live slot — available plus lent.
- ``_available``: deque of slots ready to hand out.
- ``_lent``: set of slots (identity-hashed) currently checked out.
- ``_creating``: growth factory calls in flight; their slots are not in ``_all`` yet.

Invariants. They hold after every completed public operation and between the
locked steps of any in-flight operation:

1. Exactly one lending state per slot:
   ``len(_all) == len(_available) + len(_lent)`` and the two collections are
   disjoint. A client can therefore never be lent twice.
2. Bounded growth: ``len(_all) + _creating <= _max`` at all times, including
   while factories run concurrently.
3. A closed pool has empty ``_all`` / ``_available`` / ``_lent``. ``_creating``
   drains to zero as in-flight factories finish and discard their client.

Blocking work (factory calls, pings) never runs while the lock is held: the
pool books capacity or lends a slot under the lock, performs the blocking
call outside it, then commits or rolls back under the lock again.

Metrics semantics: ``in_use`` counts lent slots only. In-progress growth
calls appear as ``creating`` and are not part of ``total`` or ``in_use`` until
the factory result commits; replacement factories reuse an existing lent slot.
"""

from __future__ import annotations

import collections
import threading
import time
from typing import Any, Callable, Dict, Optional

from ._errors import ConnectionError


class PooledConnection:
    """A connection slot in the pool with metadata.

    Deliberately keeps default identity semantics (no ``__eq__``), so slots
    hash by identity and are safe to keep in the ``_lent`` set.
    """

    __slots__ = ("client", "last_used", "created")

    def __init__(self, client: Any):
        self.client = client
        self.last_used: float = time.monotonic()
        self.created: float = self.last_used


class ConnectionPool:
    """Thread-safe pool of native clients.

    ``client_factory`` must return a fresh client object on every call.
    """

    def __init__(
        self,
        client_factory: Callable[[], Any],
        min_size: int = 2,
        max_size: int = 8,
        acquire_timeout: float = 30.0,
        health_check_interval: float = 30.0,
        max_idle_time: float = 300.0,
        reaper_interval: float = 60.0,
    ):
        if min_size < 0:
            raise ValueError(f"min_size must be >= 0, got {min_size!r}")
        if max_size < 1:
            raise ValueError(f"max_size must be >= 1, got {max_size!r}")
        if min_size > max_size:
            raise ValueError(
                f"min_size ({min_size!r}) must be <= max_size ({max_size!r})"
            )
        if acquire_timeout < 0:
            raise ValueError(f"acquire_timeout must be >= 0, got {acquire_timeout!r}")
        if health_check_interval < 0:
            raise ValueError(
                f"health_check_interval must be >= 0, got {health_check_interval!r}"
            )
        if max_idle_time < 0:
            raise ValueError(f"max_idle_time must be >= 0, got {max_idle_time!r}")
        if reaper_interval < 0:
            raise ValueError(f"reaper_interval must be >= 0, got {reaper_interval!r}")

        self._factory = client_factory
        self._min = min_size
        self._max = max_size
        self._acquire_timeout = acquire_timeout
        self._health_check_interval = health_check_interval
        self._max_idle_time = max_idle_time
        self._reaper_interval = reaper_interval
        self._all: list[PooledConnection] = []
        self._available: collections.deque[PooledConnection] = collections.deque()
        self._lent: set[PooledConnection] = set()
        self._creating: int = 0
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._closed = False
        self._reaper_wake = threading.Event()

        for _ in range(min_size):
            # No other thread can observe the pool yet, so initial fill needs
            # no reservation dance. A factory failure propagates unchanged.
            pc = PooledConnection(self._factory())
            self._all.append(pc)
            self._available.append(pc)

        if reaper_interval > 0:
            t = threading.Thread(
                target=self._reaper_loop,
                daemon=True,
                name="ch-pool-reaper",
            )
            t.start()

    @property
    def metrics(self) -> Dict[str, Any]:
        """Pool metrics for observability.

        ``in_use`` counts lent slots; ``creating`` counts growth factory calls
        in flight that are not yet part of ``total``.
        """
        with self._cond:
            now = time.monotonic()
            return {
                "total": len(self._all),
                "available": len(self._available),
                "in_use": len(self._lent),
                "creating": self._creating,
                "min_size": self._min,
                "max_size": self._max,
                "acquire_timeout": self._acquire_timeout,
                "health_check_interval": self._health_check_interval,
                "max_idle_time": self._max_idle_time,
                "oldest_idle": (
                    (now - self._available[0].last_used) if self._available else None
                ),
            }

    def acquire(self) -> Any:
        """Acquire a connection from the pool. Blocks until one is available.

        Growth capacity is booked under the lock, the factory runs outside it,
        and the new connection is committed under the lock again, so the pool
        never exceeds ``max_size`` even under concurrent creation.

        Raises:
            ConnectionError: If the pool is closed, if the acquire timeout
                expires while every slot is busy, or if a health check fails
                and creating a replacement also fails. In the last case the
                dead slot is dropped first, so the freed capacity lets a
                later acquire create a fresh connection.
        """
        deadline = time.monotonic() + self._acquire_timeout
        pc = self._reserve(deadline)
        if pc is None:
            # _reserve booked one unit of growth capacity for us.
            return self._grow()
        return self._hand_out(pc)

    def release(self, client: Any) -> None:
        """Return a connection to the pool.

        Idempotent and safe on a closed pool: unknown clients, double
        releases, and releases after ``close()`` are silently ignored and
        never mutate pool accounting.
        """
        with self._cond:
            if self._closed:
                return
            for pc in self._all:
                if pc.client is client:
                    if pc in self._lent:
                        self._lent.discard(pc)
                        pc.last_used = time.monotonic()
                        self._available.append(pc)
                        self._cond.notify()
                    # else: double release — the slot is already available.
                    return
            # Unknown client — ignore.

    def close(self) -> None:
        """Close the pool. All pending and future acquires will fail.

        Clears every state collection while holding the lock, so a release or
        health check that commits afterwards cannot resurrect a slot. Factory
        calls already in flight discard their client when they commit.
        """
        with self._cond:
            self._closed = True
            self._all.clear()
            self._available.clear()
            self._lent.clear()
            self._cond.notify_all()
        self._reaper_wake.set()

    # ── Private: acquire pipeline ────────────────────────────────────

    def _reserve(self, deadline: float) -> Optional[PooledConnection]:
        """Book a slot to hand out, or growth capacity. Never blocks on I/O.

        Returns a slot already marked lent, or ``None`` when one unit of
        growth capacity was booked for the caller (``_creating`` incremented),
        which keeps ``len(_all) + _creating <= _max`` under concurrency.
        """
        with self._cond:
            while True:
                if self._closed:
                    raise ConnectionError("Pool is closed")
                if self._available:
                    pc = self._available.popleft()
                    self._lent.add(pc)
                    return pc
                if len(self._all) + self._creating < self._max:
                    self._creating += 1
                    return None
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ConnectionError(
                        f"Connection pool exhausted: all {self._max} connections "
                        f"busy after {self._acquire_timeout}s timeout. "
                        f"Increase pool_max_size or reduce concurrent queries."
                    )
                self._cond.wait(timeout=remaining)

    def _grow(self) -> Any:
        """Create one new connection using capacity booked by ``_reserve``.

        The factory runs outside the lock. If the pool closes while the
        factory runs, the new connection is discarded. On factory failure the
        booked capacity is rolled back so waiters can reuse it.
        """
        try:
            client = self._factory()
        except BaseException:
            with self._cond:
                self._creating -= 1
                self._cond.notify_all()
            raise
        with self._cond:
            self._creating -= 1
            if self._closed:
                self._cond.notify_all()
                raise ConnectionError("Pool is closed")
            pc = PooledConnection(client)
            self._all.append(pc)
            self._lent.add(pc)
            return client

    def _hand_out(self, pc: PooledConnection) -> Any:
        """Hand out a reserved (already lent) slot after its health check.

        The caller owns ``pc``: it is in ``_lent``, so no other thread can
        lend it, reap it, or see it as available while we work. Ping and the
        replacement factory run outside the lock. A successful replacement
        keeps the slot lent under the same identity.
        """
        idle = time.monotonic() - pc.last_used
        if idle >= self._health_check_interval:
            try:
                pc.client.ping()
            except BaseException as ping_exc:
                if not isinstance(ping_exc, Exception):
                    # KeyboardInterrupt/SystemExit/cancellation must not strand
                    # the reserved slot as permanently lent.
                    self._drop_slot(pc)
                    raise
                try:
                    replacement = self._factory()
                except BaseException as factory_exc:
                    self._drop_slot(pc)
                    if not isinstance(factory_exc, Exception):
                        raise
                    raise ConnectionError(
                        f"Health check failed for a connection idle {idle:.0f}s "
                        f"(ping error: {ping_exc!r}) and creating a replacement "
                        f"failed (error: {factory_exc!r})"
                    ) from factory_exc
                with self._cond:
                    if self._closed or pc not in self._lent:
                        # The pool closed while we were checking. The slot was
                        # dropped by close(); do not resurrect it.
                        raise ConnectionError("Pool is closed")
                    pc.client = replacement
                    pc.last_used = time.monotonic()
                    return replacement
        with self._cond:
            if self._closed or pc not in self._lent:
                # The pool closed while we were checking. The slot was dropped
                # by close(); do not resurrect it.
                raise ConnectionError("Pool is closed")
            pc.last_used = time.monotonic()
            return pc.client

    def _drop_slot(self, pc: PooledConnection) -> None:
        """Remove a dead lent slot, freeing its capacity for later growth."""
        with self._cond:
            self._lent.discard(pc)
            try:
                self._all.remove(pc)
            except ValueError:
                pass
            # Freed capacity: blocked acquires may be able to grow now.
            self._cond.notify_all()

    # ── Private: idle reaper ─────────────────────────────────────────

    def _reap_once(self) -> int:
        """Drop idle available connections above ``min_size``. One reaper step.

        Only truly available slots are considered — lent slots can never be
        reaped. Returns the number of reaped slots.
        """
        with self._cond:
            if self._closed:
                return 0
            now = time.monotonic()
            target = len(self._available) - self._min
            if target <= 0:
                return 0
            reaped: list[PooledConnection] = []
            for pc in list(self._available):
                if len(reaped) >= target:
                    break
                if now - pc.last_used > self._max_idle_time:
                    reaped.append(pc)
            for pc in reaped:
                self._available.remove(pc)
                self._all.remove(pc)
            if reaped:
                # Freed capacity: blocked acquires may be able to grow now.
                self._cond.notify_all()
            return len(reaped)

    def _reaper_loop(self) -> None:
        """Background thread: close idle connections above min_size."""
        while not self._reaper_wake.wait(self._reaper_interval):
            if self._closed:
                return
            self._reap_once()
