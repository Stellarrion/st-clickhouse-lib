// C++ benchmark harness — st-clickhouse-lib vs clickhouse-cpp.
// Runs the same 10 workloads as the Rust harness (benches/bench_all_workloads.rs)
// against the same ClickHouse, so the README table columns are directly comparable.
//
// Build: see bench_cpp.sh. Auth via env CLICKHOUSE_USER/PASSWORD/HOST/PORT/DB.
#include <clickhouse/client.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

using namespace clickhouse;
using clk = std::chrono::steady_clock;

static std::string env(const char* k, const char* d) {
    const char* v = std::getenv(k);
    return v ? std::string(v) : std::string(d);
}

static std::unique_ptr<Client> make_client() {
    return std::make_unique<Client>(ClientOptions()
        .SetHost(env("CLICKHOUSE_HOST", "localhost"))
        .SetPort(static_cast<uint16_t>(std::stoi(env("CLICKHOUSE_PORT", "9000"))))
        .SetUser(env("CLICKHOUSE_USER", "default"))
        .SetPassword(env("CLICKHOUSE_PASSWORD", ""))
        .SetDefaultDatabase(env("CLICKHOUSE_DB", "default"))
        .SetPingBeforeQuery(false));
}

template <typename F>
void bench(const char* label, int warmup, int runs, F f) {
    std::fprintf(stderr, ">> %-26s ", label);
    std::fflush(stderr);
    auto c = make_client();
    try {
        for (int i = 0; i < warmup; ++i) f(*c);
        double best = 1e18, sum = 0;
        for (int i = 0; i < runs; ++i) {
            auto t0 = clk::now();
            f(*c);
            double us = std::chrono::duration<double, std::micro>(clk::now() - t0).count();
            best = std::min(best, us);
            sum += us;
        }
        std::fprintf(stderr, "min=%.3fms  avg=%.3fms\n", best / 1000.0, (sum / runs) / 1000.0);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "ERROR: %s\n", e.what());
    }
}

static const char* Q1 = "SELECT 1";
static const char* Q_COUNT = "SELECT count() FROM numbers(1000000)";
static const char* Q_GROUP =
    "SELECT g, count() AS c FROM (SELECT number % 1000 AS g FROM numbers(1000000)) "
    "GROUP BY g ORDER BY g";
static const char* Q_ORDER =
    "SELECT number FROM numbers(1000000) ORDER BY number DESC LIMIT 100";
static const char* Q_JSON =
    "SELECT concat('{\"x\":', toString(number), '}') AS v FROM numbers(1000)";
static const char* Q_UINT64_1M = "SELECT number FROM numbers(1000000)";

int main() {
    // Setup tables for INSERT / ALTER workloads.
    {
        auto c = make_client();
        c->Execute("DROP TABLE IF EXISTS __st_bench_ins");
        c->Execute("CREATE TABLE __st_bench_ins (id UInt64) ENGINE = Memory");
        c->Execute("DROP TABLE IF EXISTS __st_bench_alter");
        c->Execute("CREATE TABLE __st_bench_alter (id UInt64, val UInt64) ENGINE = Memory");
        auto col = std::make_shared<ColumnUInt64>();
        auto val = std::make_shared<ColumnUInt64>();
        for (uint64_t i = 0; i < 10000; ++i) { col->Append(i); val->Append(0); }
        Block b;
        b.AppendColumn("id", col);
        b.AppendColumn("val", val);
        c->Insert("__st_bench_alter", b);
    }

    bench("SELECT 1", 3, 25, [&](Client& c) {
        c.Select(Q1, [](const Block&) {});
    });
    bench("COUNT() 1M", 3, 20, [&](Client& c) {
        uint64_t n = 0;
        c.Select(Q_COUNT, [&](const Block& b) { if (b.GetRowCount()==0) return;
            auto col = b[0]->As<ColumnUInt64>();
            for (size_t i = 0; i < col->Size(); ++i) n = (*col)[i];
        });
        (void)n;
    });
    bench("GROUP BY 1K", 3, 15, [&](Client& c) {
        size_t rows = 0;
        c.Select(Q_GROUP, [&](const Block& b) { rows += b.GetRowCount(); });
        (void)rows;
    });
    bench("ORDER BY LIMIT 100", 3, 15, [&](Client& c) {
        uint64_t sum = 0;
        c.Select(Q_ORDER, [&](const Block& b) { if (b.GetRowCount()==0) return;
            auto col = b[0]->As<ColumnUInt64>();
            for (size_t i = 0; i < col->Size(); ++i) sum += (*col)[i];
        });
        (void)sum;
    });
    bench("JSON 1K", 3, 20, [&](Client& c) {
        size_t bytes = 0;
        c.Select(Q_JSON, [&](const Block& b) { if (b.GetRowCount()==0) return;
            auto col = b[0]->As<ColumnString>();
            for (size_t i = 0; i < col->Size(); ++i) bytes += col->At(i).size();
        });
        (void)bytes;
    });
    {
        std::string q = "SELECT ";
        for (int i = 0; i < 50; ++i) {
            q += "number AS col";
            q += std::to_string(i);
            q += (i + 1 < 50) ? ", " : " ";
        }
        q += "FROM numbers(1000)";
        bench("50 cols x 1K", 3, 20, [&](Client& c) {
            size_t rows = 0;
            c.Select(q.c_str(), [&](const Block& b) { rows += b.GetRowCount(); });
            (void)rows;
        });
    }
    bench("UInt64 1M owned", 5, 15, [&](Client& c) {
        std::vector<uint64_t> v;
        c.Select(Q_UINT64_1M, [&](const Block& b) { if (b.GetRowCount()==0) return;
            auto col = b[0]->As<ColumnUInt64>();
            size_t n = col->Size();
            v.reserve(v.size() + n);
            for (size_t i = 0; i < n; ++i) v.push_back((*col)[i]);
        });
        (void)v;
    });
    bench("UInt64 1M borrowed", 5, 15, [&](Client& c) {
        size_t rows = 0;
        c.Select(Q_UINT64_1M, [&](const Block& b) { if (b.GetRowCount()==0) return; rows += b.GetRowCount(); });
        (void)rows;
    });
    bench("INSERT 10K", 3, 15, [&](Client& c) {
        c.Execute("TRUNCATE TABLE __st_bench_ins");
        auto col = std::make_shared<ColumnUInt64>();
        for (uint64_t i = 0; i < 10000; ++i) col->Append(i);
        Block b;
        b.AppendColumn("id", col);
        c.Insert("__st_bench_ins", b);
    });
    bench("ALTER UPDATE 5K/10K", 3, 15, [&](Client& c) {
        c.Execute("ALTER TABLE __st_bench_alter UPDATE val = val + 1 WHERE id < 5000");
    });

    return 0;
}
