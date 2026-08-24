from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from ._errors import ConnectionError, map_error
from ._utils import merge_query_params


class AsyncSession:
    """Pinned async connection context returned by ``AsyncClient.session``."""

    def __init__(self, async_client: Any) -> None:
        self._async_client = async_client
        self._client: Optional[Any] = None
        self._closed = False

    async def __aenter__(self) -> AsyncSession:
        self._async_client._check_open()
        loop = asyncio.get_running_loop()
        self._client = await loop.run_in_executor(None, self._async_client._pool.acquire)
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
        if self._closed or self._client is None:
            raise ConnectionError("AsyncSession is closed")
        return self._client

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
            await self.cancel()
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
            await self.cancel()
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
            await self.cancel()
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
        client = self._require_client()
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, client.cancel)
        except Exception as e:
            raise map_error(e) from e

    async def set_setting(self, name: str, value: str) -> None:
        client = self._require_client()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, client.set_setting, name, value)
