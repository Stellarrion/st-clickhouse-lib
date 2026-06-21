mod common;

use st_clickhouse::error::Error;
use std::sync::Arc;
use std::time::Duration;

/// A size-1 pool with a 50 ms acquire timeout: a slow query occupies the only
/// slot, so a concurrent acquire must fail fast with `PoolTimeout`. Afterwards
/// the pool is not starved — a fresh query succeeds.
#[tokio::test]
#[ignore]
async fn acquire_timeout_fires_under_contention() {
    let client = Arc::new(
        common::connect_client_pool(1)
            .await
            .with_acquire_timeout(Duration::from_millis(50)),
    );

    // Slow query grabs and holds the single slot for ~2 s.
    let slow = {
        let c = client.clone();
        tokio::spawn(async move { c.query("SELECT sleep(2)").fetch::<(u8,)>().await })
    };
    // Let the slow query acquire the slot first.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Concurrent acquire on the same single-slot pool must time out.
    // (`assert!`, not `panic!` — the crate denies `clippy::panic`.)
    let probe = client.query("SELECT 1").fetch::<(u8,)>().await;
    assert!(
        matches!(&probe, Err(Error::PoolTimeout(_))),
        "expected PoolTimeout, got {probe:?}"
    );

    // After the slow query releases the slot, a fresh query must succeed.
    let _ = slow.await.expect("slow task panicked");
    let one: (u8,) = client
        .query("SELECT toUInt8(1)")
        .fetch()
        .await
        .expect("pool usable after slow query finishes");
    assert_eq!(one.0, 1);
}

/// Regression guard: with no `acquire_timeout` (default), concurrent queries on
/// a tiny pool simply queue — never a spurious `PoolTimeout`.
#[tokio::test]
#[ignore]
async fn no_acquire_timeout_queues_instead_of_failing() {
    let client = Arc::new(common::connect_client_pool(1).await);

    let a = {
        let c = client.clone();
        tokio::spawn(async move { c.query("SELECT toUInt8(7)").fetch::<(u8,)>().await })
    };
    let b = {
        let c = client.clone();
        tokio::spawn(async move { c.query("SELECT toUInt8(8)").fetch::<(u8,)>().await })
    };

    let ra = a.await.expect("task a panicked");
    let rb = b.await.expect("task b panicked");
    assert!(ra.is_ok(), "no spurious PoolTimeout: {ra:?}");
    assert!(rb.is_ok(), "no spurious PoolTimeout: {rb:?}");
}
