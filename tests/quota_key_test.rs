mod common;

/// A client configured with `with_quota_key` must still run queries: the
/// quota_key is sent both in the per-query ClientInfo block and in the
/// connection handshake addendum (the setter bumps the config generation, so
/// the next acquire reconnects carrying the new key).
///
/// Quota accounting itself is not asserted — observing it requires server-side
/// quota configuration — but a clean round-trip proves the extra wire fields
/// don't break the protocol.
#[tokio::test]
#[ignore]
async fn with_quota_key_query_round_trips() {
    let client = common::connect_client()
        .await
        .with_quota_key("st-clickhouse-quota-key-test");
    let val: (u8,) = client
        .query("SELECT toUInt8(1)")
        .fetch()
        .await
        .expect("query with quota_key must round-trip");
    assert_eq!(val.0, 1);

    // A second query reuses the (reconnected) connection and must still work.
    let again: (u8,) = client
        .query("SELECT toUInt8(2)")
        .fetch()
        .await
        .expect("reused connection must still work with quota_key");
    assert_eq!(again.0, 2);
}
