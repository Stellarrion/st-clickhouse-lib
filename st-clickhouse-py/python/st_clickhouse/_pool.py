from __future__ import annotations

import collections
import threading
import time
from typing import Any, Callable, Dict

from ._errors import ConnectionError


class PooledConnection:
    """A connection in the pool with metadata."""

    __slots__ = ("client", "last_used", "created")

    def __init__(self, client: Any):
        self.client = client
        self.last_used: float = time.monotonic()
        self.created: float = self.last_used


class ConnectionPool:
    """Thread-safe pool of native clients."""

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
        self._factory = client_factory
        self._min = min_size
        self._max = max_size
        self._acquire_timeout = acquire_timeout
        self._health_check_interval = health_check_interval
        self._max_idle_time = max_idle_time
        self._reaper_interval = reaper_interval
        self._all: list[PooledConnection] = []
        self._available: collections.deque[PooledConnection] = collections.deque()
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._closed = False

        for _ in range(min_size):
            self._add_new()

        if reaper_interval > 0:
            t = threading.Thread(
                target=self._reaper_loop,
                daemon=True,
                name="ch-pool-reaper",
            )
            t.start()

    @property
    def metrics(self) -> Dict[str, Any]:
        """Pool metrics for observability."""
        with self._cond:
            now = time.monotonic()
            total = len(self._all)
            available = len(self._available)
            return {
                "total": total,
                "available": available,
                "in_use": total - available,
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
        """Acquire a connection from the pool. Blocks until one is available."""
        deadline = time.monotonic() + self._acquire_timeout
        with self._cond:
            while True:
                if self._closed:
                    raise ConnectionError("Pool is closed")
                if self._available:
                    pc = self._available.popleft()
                    self._maybe_health_check(pc)
                    pc.last_used = time.monotonic()
                    return pc.client
                if len(self._all) < self._max:
                    pc = self._add_new()
                    return pc.client
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ConnectionError(
                        f"Connection pool exhausted: all {self._max} connections "
                        f"busy after {self._acquire_timeout}s timeout. "
                        f"Increase pool_max_size or reduce concurrent queries."
                    )
                self._cond.wait(timeout=remaining)

    def release(self, client: Any) -> None:
        """Return a connection to the pool."""
        with self._cond:
            for pc in self._all:
                if pc.client is client:
                    pc.last_used = time.monotonic()
                    self._available.append(pc)
                    self._cond.notify()
                    return

    def close(self) -> None:
        """Close the pool. All pending and future acquires will fail."""
        with self._cond:
            if self._closed:
                return
            self._closed = True
            self._cond.notify_all()
        self._all.clear()
        self._available.clear()

    def _add_new(self) -> PooledConnection:
        """Create a new connection and add to pool."""
        c = self._factory()
        pc = PooledConnection(c)
        self._all.append(pc)
        self._available.append(pc)
        return pc

    def _maybe_health_check(self, pc: PooledConnection) -> None:
        """Ping if idle too long. Replace dead connections."""
        idle = time.monotonic() - pc.last_used
        if idle < self._health_check_interval:
            return
        try:
            pc.client.ping()
        except Exception:
            try:
                pc.client = self._factory()
            except Exception:
                raise ConnectionError(
                    f"Health check failed and replacement failed: "
                    f"connection idle {idle:.0f}s"
                )

    def _reaper_loop(self) -> None:
        """Background thread: close idle connections above min_size."""
        while True:
            time.sleep(self._reaper_interval)
            with self._cond:
                if self._closed:
                    return
                now = time.monotonic()
                target_idle = len(self._available) - self._min
                if target_idle <= 0:
                    continue
                reaped: list[PooledConnection] = []
                for pc in list(self._available):
                    if len(reaped) >= target_idle:
                        break
                    if now - pc.last_used > self._max_idle_time:
                        reaped.append(pc)
                for pc in reaped:
                    self._available.remove(pc)
                    self._all.remove(pc)
                if reaped:
                    self._cond.notify_all()
