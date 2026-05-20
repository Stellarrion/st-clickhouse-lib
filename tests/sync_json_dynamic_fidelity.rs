use st_clickhouse::sync::client::SyncClient;
use st_clickhouse::sync::config::ClientConfig;
use st_clickhouse::sync::{DynamicFieldValue, settings};

fn connect() -> st_clickhouse::sync::Result<SyncClient> {
    let config = ClientConfig::default()
        .with_host("127.0.0.1")
        .with_port(9000)
        .with_user("default")
        .with_password("test")
        .with_setting(
            settings::OUTPUT_FORMAT_NATIVE_USE_FLATTENED_DYNAMIC_AND_JSON_SERIALIZATION,
            "1",
        );
    SyncClient::connect_with_config(config)
}

#[test]
fn materialized_dynamic_nested_values_decode_when_server_supports_them()
-> Result<(), Box<dyn std::error::Error>> {
    let Ok(mut client) = connect() else {
        eprintln!("ClickHouse test server is not available with default/test credentials");
        return Ok(());
    };

    for (sql, expected) in [
        (
            "SELECT CAST([toUInt8(1), toUInt8(2)] AS Dynamic) AS d",
            "Array",
        ),
        (
            "SELECT CAST([CAST([toUInt8(1)] AS Array(UInt8)), CAST([toUInt8(2)] AS Array(UInt8))] AS Dynamic) AS d",
            "Array",
        ),
        ("SELECT CAST(map('a', toUInt8(1)) AS Dynamic) AS d", "Map"),
        (
            "SELECT CAST(map('nested', [toUInt8(1), toUInt8(2)]) AS Dynamic) AS d",
            "Map",
        ),
        (
            "SELECT CAST(tuple(toUInt8(1), 'x') AS Dynamic) AS d",
            "Tuple",
        ),
        (
            "SELECT CAST(tuple(map('a', toUInt8(1)), [toUInt8(2)]) AS Dynamic) AS d",
            "Tuple",
        ),
    ] {
        match client.query(sql) {
            Ok(blocks) => {
                let block = blocks
                    .iter()
                    .find(|block| block.row_count() > 0)
                    .ok_or_else(|| format!("server returned no data rows for {sql:?}"))?;
                let col = block.dynamic_column("d").map_err(|e| {
                    let bytes = block
                        .columns
                        .iter()
                        .find(|c| c.name == "d")
                        .map(|c| format!("{:02x?}", c.data.as_ref()))
                        .unwrap_or_else(|| "<missing column>".to_owned());
                    format!("server accepted Dynamic fixture but typed decode failed for {sql:?}: {e}; bytes={bytes}")
                })?;
                let value = col
                    .typed_value(0)
                    .map(|value| value.value.clone())
                    .ok_or_else(|| {
                        format!("Dynamic typed accessor has no row value for {sql:?}")
                    })?;
                match (expected, value) {
                    ("Array", DynamicFieldValue::Array(values)) => assert_eq!(values.len(), 2),
                    ("Map", DynamicFieldValue::Map(values)) => assert_eq!(values.len(), 1),
                    ("Tuple", DynamicFieldValue::Tuple(values)) => assert_eq!(values.len(), 2),
                    (name, other) => {
                        return Err(format!("expected {name}, got {other:?}").into());
                    },
                }
            },
            Err(e) => {
                eprintln!("server does not support Dynamic fixture {sql:?}: {e:?}");
            },
        }
    }
    Ok(())
}
