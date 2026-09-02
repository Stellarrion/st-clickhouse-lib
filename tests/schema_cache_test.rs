mod common;

use bytes::Bytes;
use st_clickhouse::protocol::block::{Block, ColumnInfo};

#[tokio::test]
async fn schema_cache_fetches_refreshes_and_validates_inserts() {
    let client = common::connect_client().await.with_schema_validation(true);

    client
        .execute("DROP TABLE IF EXISTS st_schema_cache_test")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_schema_cache_test (id UInt64, name String) ENGINE = Memory")
        .await
        .expect("test operation failed");

    let schema = client
        .schema_for_table("st_schema_cache_test")
        .await
        .expect("schema should load");
    assert_eq!(schema.columns[0].name, "id");
    assert_eq!(schema.columns[0].type_name, "UInt64");

    client
        .execute("ALTER TABLE st_schema_cache_test ADD COLUMN active UInt8 DEFAULT 1")
        .await
        .expect("test operation failed");
    let refreshed = client
        .schema_for_table("st_schema_cache_test")
        .await
        .expect("schema should reload after ALTER invalidates cache");
    assert!(refreshed.columns.iter().any(|c| c.name == "active"));

    let block = Block {
        columns: vec![ColumnInfo {
            name: "id".to_owned(),
            type_name: "String".to_owned(),
            data: Bytes::from_static(b"\x011"),
            lc_materialized: Bytes::new(),
        }],
        rows: 1,
    };
    let mut session = client
        .begin_insert("st_schema_cache_test")
        .await
        .expect("insert should start");
    let err = session
        .send_data(&block)
        .await
        .expect_err("schema validation should reject wrong column type");
    assert!(format!("{err:?}").contains("expected 'UInt64'"));
    // The rejected block was never sent, but the server is still waiting for
    // the INSERT to finish: end the session so the pooled connection is not
    // left mid-INSERT (otherwise the next Query draws "Unexpected packet
    // Query received from client", which execute() now reports instead of
    // swallowing it).
    session.end().await.expect("insert session should end");

    client
        .execute("DROP TABLE IF EXISTS st_schema_cache_test")
        .await
        .expect("test operation failed");
}
