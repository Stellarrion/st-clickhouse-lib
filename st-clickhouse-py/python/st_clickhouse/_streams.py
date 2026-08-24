from __future__ import annotations

import asyncio
from typing import Any, Optional

from ._errors import ClickHouseError, TimeoutError


class QueryStream:
    """Iterator over a streaming query's blocks with safe abandonment.

    Wraps the native ``_QueryStream`` and links it to its owning
    :class:`~st_clickhouse.Client`. When the response reaches its terminal
    packet (EndOfStream or a server exception — the ``eos`` flag), the
    connection is clean and the client stays usable. Abandoning the
    iteration before that (``break``, exception, ``close()``, or GC) kills
    the connection instead: the Rust reader thread unblocks, the server sees
    the disconnect and stops the query, and the client is closed — its
    socket was left mid-response and cannot be reused safely.
    """

    def __init__(self, native_stream: Any, owner: Any = None) -> None:
        self._stream = native_stream
        self._owner = owner
        self._closed = False

    def __iter__(self) -> QueryStream:
        return self

    def __next__(self) -> Any:
        try:
            return next(self._stream)
        except Exception:
            # StopIteration (end) or error: the native stream is finished;
            # no abandonment needed. Only Exception subclasses are caught so a
            # KeyboardInterrupt landing at the boundary cannot silently skip
            # the not-at-eos kill in _abandon.
            self._closed = True
            raise

    @property
    def eos(self) -> bool:
        """Whether the response reached its terminal packet."""
        return bool(self._stream.eos)

    @property
    def finished(self) -> bool:
        """Whether the reader thread exited."""
        return bool(self._stream.finished)

    def cancel(self) -> None:
        """Stop the stream: the owning client is discarded unless the
        response already reached its terminal packet."""
        self._abandon()

    def close(self) -> None:
        """Same as :meth:`cancel` — safe to call repeatedly."""
        self._abandon()

    def _abandon(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stream.cancel()
        if not self._stream.eos and self._owner is not None:
            # Kill the shared socket: unblocks the reader thread, aborts the
            # server-side query, and closes the client for further use.
            self._owner._discard()

    def __del__(self) -> None:
        try:
            self._abandon()
        except Exception:
            pass

    def __repr__(self) -> str:
        return f"<QueryStream eos={self.eos} finished={self.finished}>"


class InsertStream:
    """Streaming INSERT session for continuous/batch ingestion."""

    def __init__(self, native_client: Any, table_name: str = "") -> None:
        self._client = native_client
        self._table = table_name
        self._active = True

    def send(self, block: Any) -> None:
        """Send a single data block."""
        if not self._active:
            raise ClickHouseError("INSERT stream is closed")
        self._client.send_data(self._table, block)

    def close(self) -> None:
        """End the INSERT stream (sends empty terminator block)."""
        if self._active:
            self._active = False
            self._client.end_insert_stream()

    def __enter__(self) -> InsertStream:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


class AsyncInsertStream:
    """Async streaming INSERT session."""

    def __init__(
        self,
        async_client: Any,
        native_client: Any,
        query: str,
        table_name: str = "",
    ) -> None:
        self._async_client = async_client
        self._client = native_client
        self._table = table_name
        self._active = True

    async def send(self, block: Any, timeout: Optional[float] = None) -> None:
        """Send a single data block.

        If the awaiting task is cancelled mid-send, the connection is killed
        and destroyed: an INSERT abandoned between blocks cannot be resumed,
        and the pool must not recycle the desynced socket.
        """
        if not self._active:
            raise ClickHouseError("INSERT stream is closed")
        loop = asyncio.get_running_loop()
        coro = loop.run_in_executor(
            None, lambda: self._client.send_data(self._table, block)
        )
        try:
            if timeout is not None:
                try:
                    await asyncio.wait_for(coro, timeout=timeout)
                except asyncio.TimeoutError:
                    raise TimeoutError(f"Insert stream send timed out after {timeout}s")
            else:
                await coro
        except asyncio.CancelledError:
            self._active = False
            self._async_client._pool.discard(self._client)
            raise

    async def close(self) -> None:
        """End the INSERT stream and return connection to pool."""
        if self._active:
            self._active = False
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(None, lambda: self._client.end_insert_stream())
            except asyncio.CancelledError:
                # Cancelled mid-terminator: the INSERT state is unknown.
                self._async_client._pool.release(self._client, destroy=True)
                raise
            except Exception:
                self._async_client._pool.release(self._client)
                raise
            else:
                self._async_client._pool.release(self._client)

    async def __aenter__(self) -> AsyncInsertStream:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()
