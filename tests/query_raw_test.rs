mod common;

use st_clickhouse::{Client, DynamicFieldValue, RawBlocks, RowCount};

#[tokio::test]
async fn query_raw_returns_native_block_body() {
    let client = match Client::connect(common::clickhouse_addr()).await {
        Ok(client) => client,
        Err(_) => Client::connect_with_credentials(common::clickhouse_addr(), "default", "test")
            .await
            .expect("test operation failed"),
    };
    let blocks = client
        .query_raw("SELECT number FROM system.numbers LIMIT 3")
        .await
        .expect("test operation failed");

    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].rows, 3);
    assert_eq!(blocks[0].columns, 1);
    assert!(!blocks[0].data.is_empty());

    let typed_raw = client
        .query("SELECT number FROM system.numbers LIMIT 3")
        .fetch::<RawBlocks>()
        .await
        .expect("test operation failed");
    assert_eq!(typed_raw.len(), 1);
    assert_eq!(typed_raw[0].rows, 3);

    let count = client
        .query("SELECT number FROM system.numbers LIMIT 3")
        .fetch::<RowCount>()
        .await
        .expect("test operation failed");
    assert_eq!(count.get(), 3);
}

#[tokio::test]
async fn materialized_variant_and_dynamic_have_typed_accessors() {
    let client = match Client::connect(common::clickhouse_addr()).await {
        Ok(client) => client,
        Err(_) => Client::connect_with_credentials(common::clickhouse_addr(), "default", "test")
            .await
            .expect("test operation failed"),
    };

    match client
        .query("SELECT CAST(1 AS Variant(UInt8, String)) AS v")
        .block()
        .await
    {
        Ok(block) => {
            let col = block.variant_column("v").expect("Variant should decode");
            let value = col
                .typed_value(0)
                .expect("Variant row should have a typed value");
            assert_eq!(value.type_name, "UInt8");
            assert_eq!(value.value, DynamicFieldValue::UInt8(1));
        },
        Err(e) => {
            eprintln!("server does not support materialized Variant fixture: {e:?}");
        },
    }

    match client
        .query("SELECT CAST(42 AS Dynamic) AS d")
        .with_setting(
            st_clickhouse::settings::OUTPUT_FORMAT_NATIVE_USE_FLATTENED_DYNAMIC_AND_JSON_SERIALIZATION,
            "1",
        )
        .block()
        .await
    {
        Ok(block) => {
            let col = block.dynamic_column("d").expect("Dynamic should decode");
            let value = col.typed_value(0).expect("Dynamic row should have a typed value");
            assert_eq!(value.value.as_u64(), Some(42));
        }
        Err(e) => {
            eprintln!("server does not support materialized Dynamic fixture: {e:?}");
        }
    }
}

#[tokio::test]
async fn query_raw_handles_nested_json_dynamic_layouts_when_server_supports_them() {
    let client = match Client::connect(common::clickhouse_addr()).await {
        Ok(client) => client,
        Err(_) => Client::connect_with_credentials(common::clickhouse_addr(), "default", "test")
            .await
            .expect("test operation failed"),
    };

    for sql in [
        "SELECT CAST([toUInt8(1), toUInt8(2)] AS Dynamic) AS d",
        "SELECT CAST(map('a', toUInt8(1)) AS Dynamic) AS d",
        "SELECT CAST(tuple(toUInt8(1), 'x') AS Dynamic) AS d",
        "SELECT [CAST('{\"x\":1}' AS JSON)] AS j",
        "SELECT tuple(CAST('{\"x\":1}' AS JSON), CAST(42 AS Dynamic)) AS t",
    ] {
        match client.query_raw(sql).await {
            Ok(blocks) => {
                assert_eq!(blocks.len(), 1);
                assert_eq!(blocks[0].rows, 1);
                assert_eq!(blocks[0].columns, 1);
                assert!(!blocks[0].data.is_empty());
            },
            Err(e) => {
                let msg = format!("{e:?}");
                assert!(
                    !msg.contains("query_raw does not support raw capture")
                        && !msg.contains("missing JSON state prefix")
                        && !msg.contains("missing Dynamic state prefix"),
                    "query_raw rejected a nested complex layout after parser support was added: {msg}",
                );
                eprintln!("server does not support nested query_raw fixture {sql:?}: {msg}");
            },
        }
    }
}

#[tokio::test]
async fn query_raw_handles_json_dynamic_variant_when_server_supports_them() {
    let client = match Client::connect(common::clickhouse_addr()).await {
        Ok(client) => client,
        Err(_) => Client::connect_with_credentials(common::clickhouse_addr(), "default", "test")
            .await
            .expect("test operation failed"),
    };

    for sql in [
        "SELECT CAST(1 AS Variant(UInt8, String)) AS v",
        "SELECT CAST('{\"x\":1}' AS JSON) AS j",
        "SELECT CAST(42 AS Dynamic) AS d",
    ] {
        match client.query_raw(sql).await {
            Ok(blocks) => {
                assert_eq!(blocks.len(), 1);
                assert_eq!(blocks[0].rows, 1);
                assert_eq!(blocks[0].columns, 1);
                assert!(!blocks[0].data.is_empty());
            },
            Err(e) => {
                let msg = format!("{e:?}");
                assert!(
                    !msg.contains("query_raw does not support raw capture"),
                    "query_raw rejected a complex type after parser support was added: {msg}",
                );
                eprintln!("server does not support query_raw complex fixture {sql:?}: {msg}");
            },
        }
    }
}
