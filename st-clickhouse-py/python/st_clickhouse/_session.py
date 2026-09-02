from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from ._errors import ConnectionError, map_error
from ._utils import merge_query_params

_CANCEL_MESSAGE_SESSION = (
    "AsyncSession.cancel() cannot cancel a running query: the pinned "
    "connection is blocked inside the query call. Cancel the awaiting task "
    "instead — the pinned connection is destroyed (the server aborts the "
    "query) and the session must be reopened. For a hard deadline use "
    "query_timeout."
)


class AsyncSession:
    """Pinned async connection context returned by ``AsyncClient.session``."""

    def __init__(self, async_client: Any) -> None:
        self._async_client = async_client
        self._client: Optional[Any] = None
        self._closed = False
        self._destroyed = False

    async def __aenter__(self) -> AsyncSession:
        self._async_client._check_open()
        # Route through _acquire (not a raw run_in_executor of pool.acquire)
        # so a task cancelled while blocked on pool admission cannot drop the
        # acquire worker's eventual result and leak the lent slot forever.
        self._client = await self._async_client._acquire(asyncio.get_running_loop())
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._client is not None:
            self._async_client._pool.release(self._client)
            self._client = None

    def _require_client(self) -> Any:
        if self._destroyed:
            raise ConnectionError(
                "AsyncSession was destroyed: a query on this session was "
                "cancelled, so its pinned connection was killed. Open a new "
                "session."
            )
        if self._closed or self._client is None:
            raise ConnectionError("AsyncSession is closed")
        return self._client

    def _destroy(self, client: Any) -> None:
        """Kill the pinned connection after task cancellation.

        The server aborts the running query, the executor thread unblocks,
        and the pool slot is destroyed (a replacement is created for other
        pool users). The session itself becomes unusable — its pinned
        connection identity is gone.
        """
        self._destroyed = True
        self._async_client._pool.discard(client)

    async def execute(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> None:
        client = self._require_client()
        loop = asyncio.get_running_loop()
        bound_params = merge_query_params(params, kwargs)
        fut = loop.run_in_executor(
            None, lambda: client.execute(query, bound_params, settings=settings)
        )
        try:
            await fut
        except asyncio.CancelledError:
            self._destroy(client)
            raise
        except Exception as e:
            raise map_error(e) from e

    async def query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        client = self._require_client()
        loop = asyncio.get_running_loop()
        bound_params = merge_query_params(params, kwargs)
        fut = loop.run_in_executor(
            None, lambda: client.query(query, bound_params, settings=settings)
        )
        try:
            return await fut
        except asyncio.CancelledError:
            self._destroy(client)
            raise
        except Exception as e:
            raise map_error(e) from e

    async def query_blocks(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> List[Any]:
        client = self._require_client()
        loop = asyncio.get_running_loop()
        bound_params = merge_query_params(params, kwargs)
        fut = loop.run_in_executor(
            None, lambda: client.query_blocks(query, bound_params, settings=settings)
        )
        try:
            return await fut
        except asyncio.CancelledError:
            self._destroy(client)
            raise
        except Exception as e:
            raise map_error(e) from e

    async def ping(self) -> bool:
        client = self._require_client()
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(None, client.ping)
        except Exception as e:
            raise map_error(e) from e

    async def cancel(self) -> None:
        """Fail closed: this method cannot cancel a running query.

        The session's connection is pinned and blocked inside the query
        call; a Cancel packet cannot be delivered over it. Cancel the
        awaiting task instead — the pinned connection is destroyed (the
        server aborts the query) and the session must be reopened.
        """
        self._require_client()
        raise RuntimeError(_CANCEL_MESSAGE_SESSION)

    async def set_setting(self, name: str, value: str) -> None:
        client = self._require_client()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, client.set_setting, name, value)
