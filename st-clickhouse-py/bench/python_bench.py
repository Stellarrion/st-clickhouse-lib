from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Iterable, Optional

import st_clickhouse as ch

try:
    import clickhouse_connect
except ImportError:
    clickhouse_connect = None


ADDR = "127.0.0.1:9000"
HTTP_HOST = "127.0.0.1"
HTTP_PORT = 8123
USER = "default"
PASSWORD = "test"
SETTINGS = {
    "output_format_native_write_json_as_string": "1",
    "ratio_of_defaults_for_sparse_serialization": "0",
    "max_block_size": "1000000",
}
OFFICIAL_SETTINGS = {
    "max_block_size": "1000000",
}


@dataclass
class Stats:
    name: str
    runs: int
    avg_ms: float
    min_ms: float
    median_ms: float
    p99_ms: float
    max_ms: float
    stddev_ms: float
    cv_pct: float
    rows: Optional[int] = None


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    idx = math.ceil((pct / 100.0) * len(values)) - 1
    return values[max(0, min(idx, len(values) - 1))]


def summarize(name: str, samples_ms: list[float], rows: Optional[int] = None) -> Stats:
    ordered = sorted(samples_ms)
    avg = sum(ordered) / len(ordered)
    variance = sum((v - avg) * (v - avg) for v in ordered) / len(ordered)
    stddev = math.sqrt(variance)
    return Stats(
        name=name,
        runs=len(ordered),
        avg_ms=avg,
        min_ms=ordered[0],
        median_ms=ordered[len(ordered) // 2],
        p99_ms=percentile(ordered, 99.0),
        max_ms=ordered[-1],
        stddev_ms=stddev,
        cv_pct=(stddev / avg * 100.0) if avg else 0.0,
        rows=rows,
    )


def print_stats(stats: Stats) -> None:
    print(
        f"CASE\t{stats.name}"
        f"\truns={stats.runs}"
        f"\tavg_ms={stats.avg_ms:.6f}"
        f"\tmin_ms={stats.min_ms:.6f}"
        f"\tmedian_ms={stats.median_ms:.6f}"
        f"\tp99_ms={stats.p99_ms:.6f}"
        f"\tmax_ms={stats.max_ms:.6f}"
        f"\tstddev_ms={stats.stddev_ms:.6f}"
        f"\tcv_pct={stats.cv_pct:.2f}"
    )
    if stats.rows is not None and stats.avg_ms > 0:
        rows_per_sec = stats.rows / (stats.avg_ms / 1000.0)
        print(
            f"ROWS\t{stats.name}"
            f"\trows={stats.rows}"
            f"\trows_per_sec={rows_per_sec:.0f}"
            f"\tmrows_per_sec={rows_per_sec / 1_000_000.0:.3f}"
        )


def time_sync(
    name: str,
    runs: int,
    fn: Callable[[], object],
    *,
    rows: Optional[int] = None,
    warmup: int = 3,
) -> Stats:
    for _ in range(warmup):
        fn()
    samples = []
    for _ in range(runs):
        start = time.perf_counter_ns()
        fn()
        samples.append((time.perf_counter_ns() - start) / 1_000_000.0)
    stats = summarize(name, samples, rows)
    print_stats(stats)
    return stats


async def time_async(
    name: str,
    runs: int,
    fn: Callable[[], Awaitable[object]],
    *,
    rows: Optional[int] = None,
    warmup: int = 3,
) -> Stats:
    for _ in range(warmup):
        await fn()
    samples = []
    for _ in range(runs):
        start = time.perf_counter_ns()
        await fn()
        samples.append((time.perf_counter_ns() - start) / 1_000_000.0)
    stats = summarize(name, samples, rows)
    print_stats(stats)
    return stats


def count_block_rows(blocks: Iterable[object]) -> int:
    return sum(block.row_count() for block in blocks)


def run_sync() -> None:
    print("== sync ==")
    time_sync(
        "sync connect",
        10,
        lambda: ch.connect(ADDR, user=USER, password=PASSWORD, settings=SETTINGS).close(),
    )

    with ch.connect(ADDR, user=USER, password=PASSWORD, settings=SETTINGS) as client:
        time_sync("sync SELECT 1 rows", 100, lambda: client.query("SELECT 1 AS x"), rows=1)
        time_sync(
            "sync SELECT 1 blocks",
            100,
            lambda: count_block_rows(client.query_blocks("SELECT 1 AS x")),
            rows=1,
        )
        time_sync(
            "sync COUNT 1M rows",
            50,
            lambda: client.query("SELECT count() AS c FROM numbers(1000000)"),
            rows=1,
        )
        time_sync(
            "sync 100K rows as dicts",
            10,
            lambda: client.query("SELECT number FROM numbers(100000)"),
            rows=100_000,
        )
        time_sync(
            "sync 100K rows as tuples",
            10,
            lambda: client.query_tuples("SELECT number FROM numbers(100000)"),
            rows=100_000,
        )
        time_sync(
            "sync 100K rows as columns",
            10,
            lambda: client.query_columns("SELECT number FROM numbers(100000)"),
            rows=100_000,
        )
        time_sync(
            "sync 100K rows as blocks",
            20,
            lambda: count_block_rows(client.query_blocks("SELECT number FROM numbers(100000)")),
            rows=100_000,
        )
        time_sync(
            "sync 1M rows as blocks",
            20,
            lambda: count_block_rows(client.query_blocks("SELECT number FROM numbers(1000000)")),
            rows=1_000_000,
        )
        time_sync(
            "sync 50 cols x 1000 blocks",
            20,
            lambda: count_block_rows(
                client.query_blocks(
                    "SELECT " + ", ".join(f"number + {i} AS c{i}" for i in range(50))
                    + " FROM numbers(1000)"
                )
            ),
            rows=1_000,
        )
        time_sync(
            "sync 1M rows stream",
            10,
            lambda: count_block_rows(client.query_stream("SELECT number FROM numbers(1000000)")),
            rows=1_000_000,
        )


def run_clickhouse_connect_sync() -> None:
    if clickhouse_connect is None:
        print("== clickhouse-connect sync skipped: package is not installed ==")
        return

    print("== clickhouse-connect sync ==")
    time_sync(
        "official sync connect",
        10,
        lambda: clickhouse_connect.get_client(
            host=HTTP_HOST,
            port=HTTP_PORT,
            username=USER,
            password=PASSWORD,
            settings=OFFICIAL_SETTINGS,
        ).close(),
    )

    client = clickhouse_connect.get_client(
        host=HTTP_HOST,
        port=HTTP_PORT,
        username=USER,
        password=PASSWORD,
        settings=OFFICIAL_SETTINGS,
    )
    try:
        time_sync(
            "official sync SELECT 1 rows",
            100,
            lambda: client.query("SELECT 1 AS x").result_rows,
            rows=1,
        )
        time_sync(
            "official sync SELECT 1 columns",
            100,
            lambda: client.query("SELECT 1 AS x").result_columns,
            rows=1,
        )
        time_sync(
            "official sync COUNT 1M rows",
            50,
            lambda: client.query("SELECT count() AS c FROM numbers(1000000)").result_rows,
            rows=1,
        )
        time_sync(
            "official sync 100K rows",
            10,
            lambda: client.query("SELECT number FROM numbers(100000)").result_rows,
            rows=100_000,
        )
        time_sync(
            "official sync 100K columns",
            10,
            lambda: client.query("SELECT number FROM numbers(100000)").result_columns,
            rows=100_000,
        )
        time_sync(
            "official sync 1M columns",
            20,
            lambda: client.query("SELECT number FROM numbers(1000000)").result_columns,
            rows=1_000_000,
        )
        time_sync(
            "official sync 50 cols x 1000 columns",
            20,
            lambda: client.query(
                "SELECT " + ", ".join(f"number + {i} AS c{i}" for i in range(50))
                + " FROM numbers(1000)"
            ).result_columns,
            rows=1_000,
        )
    finally:
        client.close()


async def run_async() -> None:
    print("== async ==")
    await time_async(
        "async connect pool1",
        10,
        lambda: connect_and_close_async(),
    )

    async with ch.connect_async(
        ADDR,
        user=USER,
        password=PASSWORD,
        settings=OFFICIAL_SETTINGS,
        pool_min_size=4,
        pool_max_size=4,
    ) as client:
        await time_async("async SELECT 1 rows", 100, lambda: client.query("SELECT 1 AS x"), rows=1)
        await time_async(
            "async SELECT 1 blocks",
            100,
            lambda: async_count_blocks(client.query_blocks("SELECT 1 AS x")),
            rows=1,
        )
        await time_async(
            "async COUNT 1M rows",
            50,
            lambda: client.query("SELECT count() AS c FROM numbers(1000000)"),
            rows=1,
        )
        await time_async(
            "async 100K rows as dicts",
            10,
            lambda: client.query("SELECT number FROM numbers(100000)"),
            rows=100_000,
        )
        await time_async(
            "async 100K rows as tuples",
            10,
            lambda: client.query_tuples("SELECT number FROM numbers(100000)"),
            rows=100_000,
        )
        await time_async(
            "async 100K rows as columns",
            10,
            lambda: client.query_columns("SELECT number FROM numbers(100000)"),
            rows=100_000,
        )
        await time_async(
            "async 100K rows as blocks",
            20,
            lambda: async_count_blocks(client.query_blocks("SELECT number FROM numbers(100000)")),
            rows=100_000,
        )
        await time_async(
            "async 1M rows as blocks",
            20,
            lambda: async_count_blocks(client.query_blocks("SELECT number FROM numbers(1000000)")),
            rows=1_000_000,
        )
        await time_async(
            "async 50 cols x 1000 blocks",
            20,
            lambda: async_count_blocks(
                client.query_blocks(
                    "SELECT " + ", ".join(f"number + {i} AS c{i}" for i in range(50))
                    + " FROM numbers(1000)"
                )
            ),
            rows=1_000,
        )
        await time_async(
            "async 1M rows stream",
            10,
            lambda: async_count_stream(client.query_stream("SELECT number FROM numbers(1000000)")),
            rows=1_000_000,
        )
        await time_async(
            "async 32 concurrent SELECT 1",
            20,
            lambda: asyncio.gather(*(client.query("SELECT 1 AS x") for _ in range(32))),
            rows=32,
        )


async def run_clickhouse_connect_async() -> None:
    if clickhouse_connect is None:
        print("== clickhouse-connect async skipped: package is not installed ==")
        return
    if not hasattr(clickhouse_connect, "get_async_client"):
        print("== clickhouse-connect async skipped: get_async_client is unavailable ==")
        return

    print("== clickhouse-connect async ==")
    await time_async(
        "official async connect",
        10,
        lambda: official_connect_and_close_async(),
    )

    client = await clickhouse_connect.get_async_client(
        host=HTTP_HOST,
        port=HTTP_PORT,
        username=USER,
        password=PASSWORD,
        settings=OFFICIAL_SETTINGS,
    )
    try:
        await time_async(
            "official async SELECT 1 rows",
            100,
            lambda: official_query_rows_async(client, "SELECT 1 AS x"),
            rows=1,
        )
        await time_async(
            "official async SELECT 1 columns",
            100,
            lambda: official_query_columns_async(client, "SELECT 1 AS x"),
            rows=1,
        )
        await time_async(
            "official async COUNT 1M rows",
            50,
            lambda: official_query_rows_async(
                client, "SELECT count() AS c FROM numbers(1000000)"
            ),
            rows=1,
        )
        await time_async(
            "official async 100K rows",
            10,
            lambda: official_query_rows_async(client, "SELECT number FROM numbers(100000)"),
            rows=100_000,
        )
        await time_async(
            "official async 100K columns",
            10,
            lambda: official_query_columns_async(client, "SELECT number FROM numbers(100000)"),
            rows=100_000,
        )
        await time_async(
            "official async 1M columns",
            20,
            lambda: official_query_columns_async(client, "SELECT number FROM numbers(1000000)"),
            rows=1_000_000,
        )
        await time_async(
            "official async 50 cols x 1000 columns",
            20,
            lambda: official_query_columns_async(
                client,
                "SELECT " + ", ".join(f"number + {i} AS c{i}" for i in range(50))
                + " FROM numbers(1000)",
            ),
            rows=1_000,
        )
        await time_async(
            "official async 32 concurrent SELECT 1",
            20,
            lambda: asyncio.gather(
                *(official_query_rows_async(client, "SELECT 1 AS x") for _ in range(32))
            ),
            rows=32,
        )
    finally:
        result = client.close()
        if hasattr(result, "__await__"):
            await result


async def connect_and_close_async() -> None:
    client = ch.connect_async(
        ADDR,
        user=USER,
        password=PASSWORD,
        settings=SETTINGS,
        pool_min_size=1,
        pool_max_size=1,
    )
    client.close()


async def official_connect_and_close_async() -> None:
    assert clickhouse_connect is not None
    client = await clickhouse_connect.get_async_client(
        host=HTTP_HOST,
        port=HTTP_PORT,
        username=USER,
        password=PASSWORD,
        settings=OFFICIAL_SETTINGS,
    )
    result = client.close()
    if hasattr(result, "__await__"):
        await result


async def official_query_rows_async(client: object, query: str) -> object:
    result = await client.query(query)
    return result.result_rows


async def official_query_columns_async(client: object, query: str) -> object:
    result = await client.query(query)
    return result.result_columns


async def async_count_blocks(awaitable: Awaitable[list[object]]) -> int:
    return count_block_rows(await awaitable)


async def async_count_stream(stream: object) -> int:
    total = 0
    async for block in stream:
        total += block.row_count()
    return total


def main() -> None:
    print("st-clickhouse-py benchmark")
    print(f"server={ADDR} user={USER} client=st-clickhouse-py")
    if clickhouse_connect is not None:
        print(
            f"official_client=clickhouse-connect {clickhouse_connect.__version__} "
            f"http={HTTP_HOST}:{HTTP_PORT}"
        )
    print(
        "settings: output_format_native_write_json_as_string=1, "
        "ratio_of_defaults_for_sparse_serialization=0, max_block_size=1000000"
    )
    run_sync()
    run_clickhouse_connect_sync()
    asyncio.run(run_async())
    asyncio.run(run_clickhouse_connect_async())


if __name__ == "__main__":
    main()
