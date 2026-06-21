# Benchmarks

Two harnesses produce the README "Rust vs C++" table. Both run the **same** `numbers(N)`
queries against the same ClickHouse so the columns are directly comparable.

## Rust — `bench_all_workloads`

All 10 README workloads via the sync client (`query`, `query_all`, `query_with_block_view`,
`insert`, `execute`).

```
CLICKHOUSE_USER=honne CLICKHOUSE_PASSWORD=honne \
  cargo run --release --bin bench_all_workloads
```

(Env creds default to `default`/empty; set `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD` for your server.)

## C++ — `cpp/st_bench.cpp` (clickhouse-cpp `-O3`)

Same 10 workloads via [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp).

```
# 1. build clickhouse-cpp (bundled deps; no submodules needed)
git clone https://github.com/ClickHouse/clickhouse-cpp.git /tmp/clickhouse-cpp
cmake -S /tmp/clickhouse-cpp -B /tmp/clickhouse-cpp/build \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=OFF -DBUILD_BENCHMARK=OFF
cmake --build /tmp/clickhouse-cpp/build -j$(nproc)

# 2. compile the harness against it (-O3)
g++ -O3 -std=c++17 benches/cpp/st_bench.cpp -I/tmp/clickhouse-cpp \
    $(find /tmp/clickhouse-cpp/build -name '*.a' | tr '\n' ' ') -lpthread -o /tmp/st_bench

# 3. run (same env creds)
CLICKHOUSE_USER=honne CLICKHOUSE_PASSWORD=honne CLICKHOUSE_HOST=127.0.0.1 /tmp/st_bench
```

## Notes

- **`numbers(N)` vs `system.numbers LIMIT`** — both harnesses use the `numbers(N)` table
  function because `clickhouse-cpp` mishandles the `system.numbers LIMIT` aggregate plan
  (it blocks). st-clickhouse handles both; `numbers(N)` is the common ground.
- Numbers are the **min** of ~15–30 runs. The owned-materialization row allocates an 8 MiB
  `Vec` and is cache/page-fault sensitive, so expect a few % variance.
- Other diagnostic benches: `column_decode_bench` (column decode micro-bench),
  `uint64_breakdown` (decode/access path breakdown), `owned_vs_borrowed` (1M-row
  materialization isolation).
