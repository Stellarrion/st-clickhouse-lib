//! Profile each step of st-clickhouse query pipeline.
//! Run: cargo run --bin profile --profile benchmark

use std::time::Instant;

type BenchResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn main() -> BenchResult {
    let rt = tokio::runtime::Runtime::new()?;

    // ── Warmup: connect once ──
    rt.block_on(async {
        let _ =
            st_clickhouse::Client::connect_with_credentials("127.0.0.1:9000", "default", "test")
                .await;
    });

    // ── 1. Connect + handshake (no query) ──
    let t = bench_async(
        &rt,
        || async {
            let c = st_clickhouse::Client::connect_with_credentials(
                "127.0.0.1:9000",
                "default",
                "test",
            )
            .await?;
            drop(c);
            Ok(())
        },
        2,
        10,
    )?;
    println!("1. connect+handshake  {:?}", t);

    // ── 2. tokio Runtime::new() cost ──
    let t = bench(
        || {
            let rt = tokio::runtime::Runtime::new()?;
            std::hint::black_box(rt);
            Ok(())
        },
        2,
        10,
    )?;
    println!("2. tokio Runtime::new {:?}", t);

    // ── 3. Full cold query (including Runtime::new) ──
    let t = bench(
        || {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async {
                let c = st_clickhouse::Client::connect_with_credentials(
                    "127.0.0.1:9000",
                    "default",
                    "test",
                )
                .await?;
                let r: Vec<(u8,)> = c.query("SELECT 1").all().await?;
                std::hint::black_box(r);
                Ok::<_, Box<dyn std::error::Error>>(())
            })
        },
        2,
        5,
    )?;
    println!("3. cold connect+query {:?}  (incl Runtime::new)", t);

    // ── 4. Cold WITHOUT Runtime::new ──
    let rt_cold = tokio::runtime::Runtime::new()?;
    let t = bench_async(
        &rt_cold,
        || async {
            let c = st_clickhouse::Client::connect_with_credentials(
                "127.0.0.1:9000",
                "default",
                "test",
            )
            .await?;
            let r: Vec<(u8,)> = c.query("SELECT 1").all().await?;
            std::hint::black_box(r);
            Ok(())
        },
        2,
        10,
    )?;
    println!("4. cold (reuse rt)    {:?}", t);

    // ── 5. Warm query (pooled) ──
    let client = rt.block_on(async {
        st_clickhouse::Client::connect_with_credentials("127.0.0.1:9000", "default", "test").await
    })?;
    let t = bench_async(
        &rt,
        || async {
            let r: Vec<(u8,)> = client.query("SELECT 1").all().await?;
            std::hint::black_box(r);
            Ok(())
        },
        3,
        10,
    )?;
    println!("5. warm query         {:?}", t);

    let t = bench_async(
        &rt,
        || async {
            let r: Vec<(u8,)> = client.query("SELECT 1 AS v").all::<(u8,)>().await?;
            std::hint::black_box(r);
            Ok(())
        },
        0,
        1000,
    )?;
    println!("   (per-query avg)    {:?}", t);

    let json_default_client = rt.block_on(async {
        st_clickhouse::Client::connect_with_credentials("127.0.0.1:9000", "default", "test").await
    })?;
    let json_disabled_client = rt
        .block_on(async {
            st_clickhouse::Client::connect_with_credentials("127.0.0.1:9000", "default", "test")
                .await
        })?
        .with_native_json_as_string(false);
    let t_json_default = bench_async(
        &rt,
        || async {
            let r: Vec<(u8,)> = json_default_client
                .query("SELECT 1 AS v")
                .all::<(u8,)>()
                .await?;
            std::hint::black_box(r);
            Ok(())
        },
        10,
        2000,
    )?;
    let t_json_disabled = bench_async(
        &rt,
        || async {
            let r: Vec<(u8,)> = json_disabled_client
                .query("SELECT 1 AS v")
                .all::<(u8,)>()
                .await?;
            std::hint::black_box(r);
            Ok(())
        },
        10,
        2000,
    )?;
    let delta = t_json_default.as_secs_f64() - t_json_disabled.as_secs_f64();
    println!("   json setting=1     {:?}", t_json_default);
    println!("   json setting=0     {:?}", t_json_disabled);
    println!("   json setting delta {:+.3}µs", delta * 1_000_000.0);

    // ── 6. Pure sum 100K u64 (Rust baseline) ──
    let data: Vec<u64> = (0..100_000).collect();
    let t = bench(
        || {
            let sum: u64 = data.iter().sum();
            std::hint::black_box(sum);
            Ok(())
        },
        100,
        1000,
    )?;
    let tp = 100_000.0 / t.as_secs_f64() / 1_000_000.0;
    println!("6. iter().sum()       {:?}  ({:.0}M/s)", t, tp);

    // ── 7. Indexed sum (bounds-checked) ──
    let t = bench(
        || {
            let slice: &[u64] = &data;
            let mut sum = 0u64;
            for &v in slice {
                sum += v;
            }
            std::hint::black_box(sum);
            Ok(())
        },
        100,
        1000,
    )?;
    let tp = 100_000.0 / t.as_secs_f64() / 1_000_000.0;
    println!("7. indexed sum        {:?}  ({:.0}M/s)", t, tp);

    // ── 8. Unchecked ptr sum (unsafe) ──
    let t = bench(
        || {
            let slice: &[u64] = &data;
            let ptr = slice.as_ptr();
            let mut sum = 0u64;
            for i in 0..slice.len() {
                sum += unsafe { *ptr.add(i) };
            }
            std::hint::black_box(sum);
            Ok(())
        },
        100,
        1000,
    )?;
    let tp = 100_000.0 / t.as_secs_f64() / 1_000_000.0;
    println!("8. unsafe ptr sum     {:?}  ({:.0}M/s)", t, tp);

    // ── 9. mpsc send/recv ──
    let t = bench_async(
        &rt,
        || async {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(1);
            tx.send(42).await?;
            let v = rx.recv().await.ok_or("mpsc channel closed")?;
            std::hint::black_box(v);
            Ok(())
        },
        100,
        1000,
    )?;
    println!("9. mpsc send/recv     {:?}", t);

    // ── 10. pool.get overhead (warm) ──
    // We can't directly measure pool.get(), but we can compare
    // full query vs minimal operations
    println!();
    println!("--- C++ reference ---");
    println!("cold connect+query:  1.21ms");
    println!("warm query:          591µs");
    println!("bulk 100K read:      1.19ms (84M/s)");
    Ok(())
}

fn bench(
    mut f: impl FnMut() -> BenchResult, warmup: usize, runs: usize,
) -> BenchResult<std::time::Duration> {
    for _ in 0..warmup {
        f()?;
    }
    let s = Instant::now();
    for _ in 0..runs {
        f()?;
    }
    Ok(s.elapsed() / runs as u32)
}

fn bench_async<F, Fut>(
    rt: &tokio::runtime::Runtime, f: F, warmup: usize, runs: usize,
) -> BenchResult<std::time::Duration>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = BenchResult>,
{
    for _ in 0..warmup {
        rt.block_on(f())?;
    }
    let s = Instant::now();
    for _ in 0..runs {
        rt.block_on(f())?;
    }
    Ok(s.elapsed() / runs as u32)
}
