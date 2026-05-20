mod common;

use st_clickhouse::{ClickHouseColumnData, Client, QualifiedTableName, QueryParameter};

async fn connect() -> Client {
    match Client::connect(common::clickhouse_addr()).await {
        Ok(client) => client,
        Err(_) => Client::connect_with_credentials(common::clickhouse_addr(), "default", "test")
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

#[tokio::test]
async fn ignored_part_uuids_packet_is_accepted_before_query() {
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
