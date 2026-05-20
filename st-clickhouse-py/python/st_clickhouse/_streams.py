from __future__ import annotations

import asyncio
from typing import Any, Optional

from ._errors import ClickHouseError, TimeoutError


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
        """Send a single data block."""
        if not self._active:
            raise ClickHouseError("INSERT stream is closed")
        loop = asyncio.get_running_loop()
        coro = loop.run_in_executor(
            None, lambda: self._client.send_data(self._table, block)
        )
        if timeout is not None:
            try:
                await asyncio.wait_for(coro, timeout=timeout)
            except asyncio.TimeoutError:
                raise TimeoutError(f"Insert stream send timed out after {timeout}s")
        else:
            await coro

    async def close(self) -> None:
        """End the INSERT stream and return connection to pool."""
        if self._active:
            self._active = False
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(None, lambda: self._client.end_insert_stream())
            except asyncio.CancelledError:
                self._async_client._pool.release(self._client)
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
