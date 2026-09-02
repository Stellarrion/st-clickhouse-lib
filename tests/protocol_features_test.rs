mod common;

use st_clickhouse::{ClickHouseColumnData, Client, QualifiedTableName, QueryParameter};

async fn connect() -> Client {
    let addr = common::clickhouse_addr();
    // Env override for non-default users (e.g. CLICKHOUSE_USER=honne CLICKHOUSE_PASSWORD=honne).
    if let (Ok(user), Ok(password)) = (
        std::env::var("CLICKHOUSE_USER"),
        std::env::var("CLICKHOUSE_PASSWORD"),
    ) {
        return Client::connect_with_credentials(addr, &user, &password)
            .await
            .expect("test operation failed");
    }
    match Client::connect(addr).await {
        Ok(client) => client,
        Err(_) => Client::connect_with_credentials(addr, "default", "test")
            .await
            .expect("test operation failed"),
    }
}

#[tokio::test]
async fn server_side_parameters_roundtrip() {
    let client = connect().await;
    let block = client
        .query("SELECT {id:UInt64} AS id, {name:String} AS name")
        .bind("id", 42)
        .bind("name", "alice")
        .block()
        .await
        .expect("test operation failed");

    assert_eq!(
        block
            .column::<u64>("id")
            .expect("test operation failed")
            .get(0)
            .expect("test operation failed"),
        42
    );
    assert_eq!(
        block
            .column::<String>("name")
            .expect("test operation failed")
            .get(0)
            .expect("test operation failed"),
        "alice".to_owned()
    );
}

#[tokio::test]
async fn execute_with_parameter_packet_roundtrip() {
    let client = connect().await;
    client
        .execute_with_params("SELECT {id:UInt64}", &[QueryParameter::new("id", "42")])
        .await
        .expect("test operation failed");
}

#[tokio::test]
async fn tables_status_roundtrip() {
    let client = connect().await;
    let response = client
        .tables_status(&[QualifiedTableName::new("system", "one")])
        .await
        .expect("test operation failed");

    assert!(
        response
            .table_states_by_id
            .contains_key(&QualifiedTableName::new("system", "one"))
    );
}

/// Whether the server still accepts `Client::IgnoredPartUUIDs`.
///
/// ClickHouse removed query deduplication server-side (UNSUPPORTED_METHOD
/// "Received IgnoredPartUUIDs packet, but query deduplication ... is no
/// longer supported" — TCPHandler::processObsoleteIgnoredPartUUIDs, observed
/// live on 26.7.3). On such servers the packet closes the connection, so the
/// ignored-part-UUID tests below can only run where the feature exists.
async fn server_supports_ignored_part_uuids() -> bool {
    let client = connect().await;
    let block = client.query("SELECT version()").block().await;
    match block {
        Ok(block) => {
            let version = block
                .column::<String>("version()")
                .expect("test operation failed")
                .get(0)
                .expect("test operation failed");
            // Removal landed in 25.x (26.7 rejects it outright). Exact cutoff
            // unverified; 24.x is the last line known to accept it.
            let major: u32 = version
                .split('.')
                .next()
                .and_then(|m| m.parse().ok())
                .unwrap_or(0);
            major <= 24
        },
        Err(_) => false,
    }
}

#[tokio::test]
async fn ignored_part_uuids_packet_is_accepted_before_query() {
    if !server_supports_ignored_part_uuids().await {
        eprintln!("server dropped IgnoredPartUUIDs support; skipping");
        return;
    }
    let client = connect().await;
    let block = client
        .query("SELECT 1 AS x")
        .with_query_id("st-test-ignored-part-query")
        .with_ignored_part_uuid([7u8; 16])
        .block()
        .await
        .expect("test operation failed");

    assert_eq!(
        block
            .column::<u8>("x")
            .expect("test operation failed")
            .get(0)
            .expect("test operation failed"),
        1
    );
}

#[tokio::test]
async fn ignored_part_uuids_packet_is_accepted_before_begin_select() {
    if !server_supports_ignored_part_uuids().await {
        eprintln!("server dropped IgnoredPartUUIDs support; skipping");
        return;
    }
    let client = connect().await.with_setting("replace_running_query", "1");
    let mut stream = client
        .begin_select_with_ignored_part_uuids("SELECT 1 AS x", &[[8u8; 16]])
        .await
        .expect("test operation failed");
    let block = stream
        .next_block()
        .await
        .expect("test operation failed")
        .expect("test operation failed");

    assert_eq!(
        block
            .column::<u8>("x")
            .expect("test operation failed")
            .get(0)
            .expect("test operation failed"),
        1
    );
    stream.cancel().await.expect("test operation failed");
}

#[tokio::test]
async fn ignored_part_uuids_packet_is_accepted_before_batch_queries() {
    if !server_supports_ignored_part_uuids().await {
        eprintln!("server dropped IgnoredPartUUIDs support; skipping");
        return;
    }
    let client = connect().await;
    let blocks = client
        .batch()
        .with_ignored_part_uuid([9u8; 16])
        .query("SELECT 1 AS x")
        .execute()
        .await
        .expect("test operation failed");

    assert_eq!(blocks.len(), 1);
    let first = blocks[0].as_ref().expect("test operation failed");
    assert_eq!(
        first
            .column::<u8>("x")
            .expect("test operation failed")
            .get(0)
            .expect("test operation failed"),
        1
    );
}
