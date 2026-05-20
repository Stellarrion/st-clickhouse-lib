//! Isolated async-client profiling workloads.
//!
//! Build with symbols for perf:
//! `cargo rustc -p st-clickhouse-lib --profile benchmark --bin profile_async_workload -- -C debuginfo=1 -C strip=none`
//!
//! Example:
//! `perf record -F 997 -g -- target/benchmark/profile_async_workload scan-1m-blocks 200`

use std::time::{Duration, Instant};

use st_clickhouse::Client;

const ADDR: &str = "127.0.0.1:9000";
const USER: &str = "default";
const PASSWORD: &str = "test";
const DEFAULT_RUNS: usize = 1_000;
const DEFAULT_WARMUP: usize = 20;

type ProfileResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy)]
enum Workload {
    Connect,
    Select1Rows,
    Select1Block,
    Scan100kRows,
    Scan100kBlocks,
    Scan100kCount,
    Scan1mBlocks,
    Scan1mCount,
    RawJson,
}

impl Workload {
    fn parse(value: &str) -> ProfileResult<Self> {
        match value {
            "connect" => Ok(Self::Connect),
            "select1-rows" => Ok(Self::Select1Rows),
            "select1-block" => Ok(Self::Select1Block),
            "scan-100k-rows" => Ok(Self::Scan100kRows),
            "scan-100k-blocks" => Ok(Self::Scan100kBlocks),
            "scan-100k-count" => Ok(Self::Scan100kCount),
            "scan-1m-blocks" => Ok(Self::Scan1mBlocks),
            "scan-1m-count" => Ok(Self::Scan1mCount),
            "raw-json" => Ok(Self::RawJson),
            "help" | "--help" | "-h" => Err(usage().into()),
            other => Err(format!("unknown workload '{other}'\n{}", usage()).into()),
        }
    }

    fn default_runs(self) -> usize {
        match self {
            Self::Connect | Self::Select1Rows | Self::Select1Block | Self::RawJson => DEFAULT_RUNS,
            Self::Scan100kRows => 100,
            Self::Scan100kBlocks | Self::Scan100kCount => 400,
            Self::Scan1mBlocks => 200,
            Self::Scan1mCount => 200,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::Select1Rows => "select1-rows",
            Self::Select1Block => "select1-block",
            Self::Scan100kRows => "scan-100k-rows",
            Self::Scan100kBlocks => "scan-100k-blocks",
            Self::Scan100kCount => "scan-100k-count",
            Self::Scan1mBlocks => "scan-1m-blocks",
            Self::Scan1mCount => "scan-1m-count",
            Self::RawJson => "raw-json",
        }
    }
}

fn main() -> ProfileResult {
    let mut args = std::env::args().skip(1);
    let workload = args
        .next()
        .map(|value| Workload::parse(&value))
        .unwrap_or_else(|| Err(usage().into()))?;
    let runs = parse_optional_usize(args.next(), workload.default_runs(), "runs")?;
    let warmup = parse_optional_usize(args.next(), DEFAULT_WARMUP, "warmup")?;
    if args.next().is_some() {
        return Err(format!("too many arguments\n{}", usage()).into());
    }

    let rt = tokio::runtime::Runtime::new()?;
    let avg = match workload {
        Workload::Connect => bench_async(&rt, warmup, runs, || async {
            let client = Client::connect_with_credentials(ADDR, USER, PASSWORD).await?;
            std::hint::black_box(client);
            Ok(())
        })?,
        _ => {
            let client = rt
                .block_on(async { Client::connect_with_credentials(ADDR, USER, PASSWORD).await })?;
            bench_async(&rt, warmup, runs, || run_workload(&client, workload))?
        },
    };

    println!(
        "WORKLOAD\t{}\truns={runs}\twarmup={warmup}\tavg_ns={}\tavg_us={:.3}\tavg_ms={:.6}",
        workload.name(),
        avg.as_nanos(),
        avg.as_secs_f64() * 1_000_000.0,
        avg.as_secs_f64() * 1_000.0
    );
    Ok(())
}

async fn run_workload(client: &Client, workload: Workload) -> ProfileResult {
    match workload {
        Workload::Connect => unreachable!("connect is handled before client reuse"),
        Workload::Select1Rows => {
            let rows: Vec<(u8,)> = client.query("SELECT 1 AS v").all().await?;
            std::hint::black_box(rows);
        },
        Workload::Select1Block => {
            let block = client.query("SELECT 1 AS v").block().await?;
            std::hint::black_box(block);
        },
        Workload::Scan100kRows => {
            let rows: Vec<(u64,)> = client
                .query("SELECT number AS v FROM system.numbers LIMIT 100000")
                .all()
                .await?;
            std::hint::black_box(rows);
        },
        Workload::Scan100kBlocks => {
            let block = client
                .query("SELECT number AS v FROM system.numbers LIMIT 100000")
                .block()
                .await?;
            std::hint::black_box(block);
        },
        Workload::Scan100kCount => {
            let rows = client
                .query("SELECT number AS v FROM system.numbers LIMIT 100000")
                .row_count()
                .await?;
            std::hint::black_box(rows);
        },
        Workload::Scan1mBlocks => {
            let block = client
                .query("SELECT number AS v FROM system.numbers LIMIT 1000000")
                .block()
                .await?;
            std::hint::black_box(block);
        },
        Workload::Scan1mCount => {
            let rows = client
                .query("SELECT number AS v FROM system.numbers LIMIT 1000000")
                .row_count()
                .await?;
            std::hint::black_box(rows);
        },
        Workload::RawJson => {
            let blocks = client
                .query("SELECT CAST(concat('{\"x\":', toString(number), '}'), 'JSON') AS v FROM system.numbers LIMIT 1000 SETTINGS allow_experimental_json_type = 1")
                .raw()
                .await?;
            std::hint::black_box(blocks);
        },
    }
    Ok(())
}

fn bench_async<F, Fut>(
    rt: &tokio::runtime::Runtime, warmup: usize, runs: usize, f: F,
) -> ProfileResult<Duration>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = ProfileResult>,
{
    if runs == 0 {
        return Err("runs must be greater than zero".into());
    }
    for _ in 0..warmup {
        rt.block_on(f())?;
    }
    let start = Instant::now();
    for _ in 0..runs {
        rt.block_on(f())?;
    }
    let divisor = u32::try_from(runs).map_err(|_| "runs exceeds u32::MAX")?;
    Ok(start.elapsed() / divisor)
}

fn parse_optional_usize(
    value: Option<String>, default: usize, name: &'static str,
) -> ProfileResult<usize> {
    value.map_or(Ok(default), |raw| {
        raw.parse::<usize>()
            .map_err(|err| format!("invalid {name} '{raw}': {err}").into())
    })
}

fn usage() -> &'static str {
    "usage: profile_async_workload <workload> [runs] [warmup]\n\
     workloads: connect, select1-rows, select1-block, scan-100k-rows, \
     scan-100k-blocks, scan-100k-count, scan-1m-blocks, scan-1m-count, raw-json"
}
