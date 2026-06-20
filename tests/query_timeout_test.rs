mod common;

use st_clickhouse::error::Error;
use std::time::Duration;

fn assert_timeout<T: std::fmt::Debug>(r: st_clickhouse::error::Result<T>) {
    assert!(
        matches!(&r, Err(Error::Timeout(_))),
        "expected Error::Timeout, got {r:?}"
    );
}

#[tokio::test]
#[ignore]
async fn per_query_timeout_fires_and_connection_is_reused() {
    let client = common::connect_client().await;
    // 1s deadline, 3s server sleep -> must time out.
    assert_timeout(
        client
            .query("SELECT sleep(3), number FROM system.numbers LIMIT 1")
            .timeout(Duration::from_secs(1))
            .fetch::<(u8, u64)>()
            .await,
    );
    // The connection must still be usable (drained + reused, or reaped + reconnected).
    let one: (u64,) = client
        .query("SELECT toUInt64(1)")
        .fetch()
        .await
        .expect("connection reusable after timeout");
    assert_eq!(one.0, 1);
}

#[tokio::test]
#[ignore]
async fn client_level_query_timeout_applies() {
    let client = common::connect_client().await;
    let client = client.with_query_timeout(Duration::from_secs(1));
    assert_timeout(client.query("SELECT sleep(3)").fetch::<(u8,)>().await);
}

#[tokio::test]
#[ignore]
async fn per_query_override_beats_client_level() {
    let client = common::connect_client().await;
    // Tight client deadline, generous per-query override -> must succeed.
    let client = client.with_query_timeout(Duration::from_millis(100));
    let val: (u8,) = client
        .query("SELECT sleep(0.3)")
        .timeout(Duration::from_secs(5))
        .fetch()
        .await
        .expect("per-query override should win");
    assert_eq!(val.0, 0);
}

#[tokio::test]
#[ignore]
async fn no_timeout_long_query_completes() {
    // Regression guard: with no deadline, a sub-second query completes normally.
    let client = common::connect_client().await;
    let val: (u64,) = client
        .query("SELECT toUInt64(42) WHERE sleep(0.2) = 0")
        .fetch()
        .await
        .expect("no timeout configured -> must complete");
    assert_eq!(val.0, 42);
}

#[tokio::test]
#[ignore]
async fn execute_timeout_fires() {
    let client = common::connect_client().await;
    let client = client.with_query_timeout(Duration::from_secs(1));
    assert_timeout(client.execute("SELECT sleep(3)").await);
}
