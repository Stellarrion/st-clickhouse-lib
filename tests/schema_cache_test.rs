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
    let err = client
        .begin_insert("st_schema_cache_test")
        .await
        .expect("insert should start")
        .send_data(&block)
        .await
        .expect_err("schema validation should reject wrong column type");
    assert!(format!("{err:?}").contains("expected 'UInt64'"));

    client
        .execute("DROP TABLE IF EXISTS st_schema_cache_test")
        .await
        .expect("test operation failed");
}
