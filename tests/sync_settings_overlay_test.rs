//! Live sync-client per-query settings overlay tests.
//!
//! Requires a ClickHouse native TCP endpoint. Configure with the standard
//! environment variables used by the other sync integration tests:
//!   CLICKHOUSE_HOST (default 127.0.0.1:9000), CLICKHOUSE_USER (default
//!   "default"), CLICKHOUSE_PASS (default "test").
//!
//! Skips (returns Ok) when the server is not reachable.

use std::collections::HashMap;
use std::time::Duration;

use st_clickhouse::sync::client::SyncClient;
use st_clickhouse::sync::config::ClientConfig;

fn connect(settings: &[(&str, &str)]) -> st_clickhouse::sync::Result<SyncClient> {
    let addr = std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "127.0.0.1:9000".to_owned());
    let (host, port) = addr
        .rsplit_once(':')
        .and_then(|(host, port)| port.parse::<u16>().ok().map(|port| (host.to_owned(), port)))
        .unwrap_or_else(|| ("127.0.0.1".to_owned(), 9000));
    let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_owned());
    let password = std::env::var("CLICKHOUSE_PASS").unwrap_or_else(|_| "test".to_owned());
    let mut config = ClientConfig::default()
        .with_host(&host)
        .with_port(port)
        .with_user(&user)
        .with_password(&password)
        .with_connect_timeout(Duration::from_secs(5))
        .with_query_timeout(Duration::from_secs(10));
    for (name, value) in settings {
        config = config.with_setting(name, value);
    }
    SyncClient::connect_with_config(config)
}

fn setting_rows(client: &mut SyncClient, name: &str) -> String {
    let query = format!("SELECT value FROM system.settings WHERE name = '{name}'");
    let blocks = client.query(&query).expect("settings query failed");
    blocks
        .iter()
        .filter(|b| b.row_count() > 0)
        .find_map(|b| {
            b.column::<String>("value")
                .ok()
                .and_then(|c| c.get_string(0).ok())
        })
        .expect("no value for setting")
}

fn overlay(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[test]
fn live_sync_per_query_settings_overlay() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(mut client) = connect(&[("max_threads", "7")]) else {
        eprintln!("skipping: ClickHouse server not reachable");
        return Ok(());
    };

    // Baseline comes from the constructor settings.
    assert_eq!(setting_rows(&mut client, "max_threads"), "7");

    // Overlay applies to its own query and wins over the baseline.
    let rows = client.query_with_settings(
        "SELECT value FROM system.settings WHERE name = 'max_threads'",
        &overlay(&[("max_threads", "3")]),
    )?;
    let value = rows
        .iter()
        .filter(|b| b.row_count() > 0)
        .find_map(|b| {
            b.column::<String>("value")
                .ok()
                .and_then(|c| c.get_string(0).ok())
        })
        .expect("overlay query returned no rows");
    assert_eq!(value, "3");

    // Later queries see the untouched baseline.
    assert_eq!(setting_rows(&mut client, "max_threads"), "7");

    // Keys absent from the baseline revert to the server default afterwards.
    let key = "max_insert_block_size";
    let default = setting_rows(&mut client, key);
    client.query_with_settings(
        &format!("SELECT value FROM system.settings WHERE name = '{key}'"),
        &overlay(&[(key, "123457")]),
    )?;
    assert_eq!(setting_rows(&mut client, key), default);

    // Server-side parameters still work alongside an overlay.
    let blocks = client.query_with_params_and_settings(
        "SELECT {v:UInt8} AS x",
        &[st_clickhouse::sync::protocol::parameters::QueryParameter::new("v", "42")],
        &overlay(&[("max_threads", "3")]),
    )?;
    let x = blocks
        .iter()
        .filter(|b| b.row_count() > 0)
        .find_map(|b| b.column::<u8>("x").ok().and_then(|c| c.get(0).ok()))
        .expect("parameterized overlay query returned no rows");
    assert_eq!(x, 42);
    assert_eq!(setting_rows(&mut client, "max_threads"), "7");

    Ok(())
}

#[test]
fn live_sync_execute_with_settings_does_not_persist() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(mut client) = connect(&[]) else {
        eprintln!("skipping: ClickHouse server not reachable");
        return Ok(());
    };

    let baseline = setting_rows(&mut client, "max_threads");
    client.execute_with_settings(
        "SELECT value FROM system.settings WHERE name = 'max_threads'",
        &overlay(&[("max_threads", "3")]),
    )?;
    assert_eq!(setting_rows(&mut client, "max_threads"), baseline);
    Ok(())
}
