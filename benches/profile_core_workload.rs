//! Isolated sync/core profiling workloads.
//!
//! Build with symbols for perf:
//! `cargo rustc -p st-clickhouse-lib --profile benchmark --bin profile_core_workload -- -C debuginfo=1 -C strip=none`
//!
//! Example:
//! `perf record -F 997 -g -- target/benchmark/profile_core_workload scan-1m-view 500`

use std::time::{Duration, Instant};

use st_clickhouse::sync::client::SyncClient;
use st_clickhouse::sync::config::ClientConfig;

const HOST: &str = "127.0.0.1";
const PORT: u16 = 9000;
const USER: &str = "default";
const PASSWORD: &str = "test";
const DEFAULT_RUNS: usize = 1_000;
const DEFAULT_WARMUP: usize = 20;

type ProfileResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy)]
enum Workload {
    Connect,
    Select1,
    Scan100kOwned,
    Scan1mOwned,
    Scan1mView,
    Scan1mDiscard,
    Wide50,
    Json,
}

impl Workload {
    fn parse(value: &str) -> ProfileResult<Self> {
        match value {
            "connect" => Ok(Self::Connect),
            "select1" => Ok(Self::Select1),
            "scan-100k-owned" => Ok(Self::Scan100kOwned),
            "scan-1m-owned" => Ok(Self::Scan1mOwned),
            "scan-1m-view" => Ok(Self::Scan1mView),
            "scan-1m-discard" => Ok(Self::Scan1mDiscard),
            "wide-50" => Ok(Self::Wide50),
            "json" => Ok(Self::Json),
            "help" | "--help" | "-h" => Err(usage().into()),
            other => Err(format!("unknown workload '{other}'\n{}", usage()).into()),
        }
    }

    fn default_runs(self) -> usize {
        match self {
            Self::Connect | Self::Select1 => DEFAULT_RUNS,
            Self::Scan100kOwned | Self::Wide50 | Self::Json => 400,
            Self::Scan1mOwned | Self::Scan1mView | Self::Scan1mDiscard => 300,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::Select1 => "select1",
            Self::Scan100kOwned => "scan-100k-owned",
            Self::Scan1mOwned => "scan-1m-owned",
            Self::Scan1mView => "scan-1m-view",
            Self::Scan1mDiscard => "scan-1m-discard",
            Self::Wide50 => "wide-50",
            Self::Json => "json",
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
    if runs == 0 {
        return Err("runs must be greater than zero".into());
    }

    let avg = match workload {
        Workload::Connect => bench(warmup, runs, || {
            let client = make_client()?;
            std::hint::black_box(client);
            Ok(())
        })?,
        _ => {
            let mut client = make_client()?;
            bench(warmup, runs, || run_workload(&mut client, workload))?
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

fn run_workload(client: &mut SyncClient, workload: Workload) -> ProfileResult {
    match workload {
        Workload::Connect => unreachable!("connect is handled before client reuse"),
        Workload::Select1 => {
            let blocks = client.query("SELECT 1")?;
            std::hint::black_box(blocks);
        },
        Workload::Scan100kOwned => {
            let blocks = client.query("SELECT number AS v FROM system.numbers LIMIT 100000")?;
            std::hint::black_box(blocks);
        },
        Workload::Scan1mOwned => {
            let blocks = client.query("SELECT number AS v FROM system.numbers LIMIT 1000000")?;
            std::hint::black_box(blocks);
        },
        Workload::Scan1mView => {
            let mut rows = 0usize;
            client.query_with_block_view(
                "SELECT number AS v FROM system.numbers LIMIT 1000000",
                |block| {
                    rows = rows.saturating_add(block.row_count());
                    Ok(())
                },
            )?;
            std::hint::black_box(rows);
        },
        Workload::Scan1mDiscard => {
            let rows =
                client.query_row_count("SELECT number AS v FROM system.numbers LIMIT 1000000")?;
            std::hint::black_box(rows);
        },
        Workload::Wide50 => {
            let blocks = client.query(
                "SELECT number, number, number, number, number, number, number, number, number, number, \
                 number, number, number, number, number, number, number, number, number, number, \
                 number, number, number, number, number, number, number, number, number, number, \
                 number, number, number, number, number, number, number, number, number, number, \
                 number, number, number, number, number, number, number, number, number, number \
                 FROM system.numbers LIMIT 1000",
            )?;
            std::hint::black_box(blocks);
        },
        Workload::Json => {
            let blocks = client.query(
                "SELECT CAST(concat('{\"x\":', toString(number), '}'), 'JSON') AS v \
                 FROM system.numbers LIMIT 1000 SETTINGS allow_experimental_json_type = 1",
            )?;
            std::hint::black_box(blocks);
        },
    }
    Ok(())
}

fn make_client() -> ProfileResult<SyncClient> {
    let config = ClientConfig::new()
        .with_host(HOST)
        .with_port(PORT)
        .with_user(USER)
        .with_password(PASSWORD)
        .with_setting("output_format_native_write_json_as_string", "1")
        .with_setting("ratio_of_defaults_for_sparse_serialization", "0");
    Ok(SyncClient::connect_with_config(config)?)
}

fn bench(
    warmup: usize, runs: usize, mut f: impl FnMut() -> ProfileResult,
) -> ProfileResult<Duration> {
    for _ in 0..warmup {
        f()?;
    }
    let start = Instant::now();
    for _ in 0..runs {
        f()?;
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
    "usage: profile_core_workload <workload> [runs] [warmup]\n\
     workloads: connect, select1, scan-100k-owned, scan-1m-owned, \
     scan-1m-view, scan-1m-discard, wide-50, json"
}
