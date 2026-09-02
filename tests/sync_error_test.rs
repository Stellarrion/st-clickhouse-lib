//! Live sync-client server-error propagation tests.
//!
//! Requires a ClickHouse native TCP endpoint. Configure with the standard
//! environment variables used by the other sync integration tests:
//!   CLICKHOUSE_HOST (default 127.0.0.1:9000), CLICKHOUSE_USER (default
//!   "default"), CLICKHOUSE_PASS (default "test").
//!
//! Skips (returns Ok) when the server is not reachable.

use st_clickhouse::sync::client::SyncClient;
use st_clickhouse::sync::config::ClientConfig;
use st_clickhouse::sync::error::Error;
use std::time::Duration;

fn connect() -> st_clickhouse::sync::Result<SyncClient> {
    let addr = std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "127.0.0.1:9000".to_owned());
    let (host, port) = addr
        .rsplit_once(':')
        .and_then(|(host, port)| port.parse::<u16>().ok().map(|port| (host.to_owned(), port)))
        .unwrap_or_else(|| ("127.0.0.1".to_owned(), 9000));
    let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_owned());
    let password = std::env::var("CLICKHOUSE_PASS").unwrap_or_else(|_| "test".to_owned());
    let config = ClientConfig::default()
        .with_host(&host)
        .with_port(port)
        .with_user(&user)
        .with_password(&password)
        .with_connect_timeout(Duration::from_secs(5))
        .with_query_timeout(Duration::from_secs(10));
    SyncClient::connect_with_config(config)
}

/// Assert the error is a structured server exception carrying the expected
/// code/name/message triple, then verify the same connection still serves a
/// valid query — server exceptions must not desync the framing.
fn assert_server_error_then_reusable(
    client: &mut SyncClient, err: Error,
) -> Result<(), Box<dyn std::error::Error>> {
    let Error::ServerError {
        code,
        name,
        message,
    } = &err
    else {
        unreachable!("expected ServerError, got: {err:?}");
    };
    // 46 = UNKNOWN_FUNCTION on current servers (60 is UNKNOWN_IDENTIFIER).
    assert_eq!(*code, 46, "expected UNKNOWN_FUNCTION (46), got: {err}");
    assert_eq!(name, "DB::Exception");
    assert!(
        message.contains("unknown_function_xyz"),
        "message must name the failing function: {message}"
    );
    assert!(err.is_server_error());

    let blocks = client.query("SELECT toUInt8(42) AS v")?;
    let block = blocks
        .iter()
        .find(|block| block.row_count() > 0)
        .ok_or("valid query returned no data rows")?;
    let value: u8 = block.column::<u8>("v")?.get(0)?;
    assert_eq!(
        value, 42,
        "connection must remain usable after a server exception"
    );
    Ok(())
}

#[test]
fn execute_invalid_query_errs_then_connection_still_works() -> Result<(), Box<dyn std::error::Error>>
{
    let Ok(mut client) = connect() else {
        eprintln!("ClickHouse test server is not available; skipping live test");
        return Ok(());
    };
    let err = client
        .execute("SELECT unknown_function_xyz()")
        .expect_err("invalid execute must surface the server exception");
    assert_server_error_then_reusable(&mut client, err)
}

#[test]
fn cancel_is_fail_closed_and_keeps_client_usable() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(mut client) = connect() else {
        eprintln!("ClickHouse test server is not available; skipping live test");
        return Ok(());
    };
    // SyncClient::cancel is fail-closed: a bare Cancel write cannot produce
    // a cancelled-and-drained connection, so it must return Error::Config
    // with guidance and without touching the socket.
    #[allow(deprecated)]
    let cancelled = client.cancel();
    match cancelled {
        Err(Error::Config(msg)) => assert!(
            msg.contains("with_query_timeout") && msg.contains("shutdown_handle"),
            "cancel error must point at the alternatives: {msg}"
        ),
        other => unreachable!("expected Error::Config, got {other:?}"),
    }
    // The connection is untouched and stays usable.
    let blocks = client.query("SELECT toUInt8(42) AS v")?;
    let block = blocks
        .iter()
        .find(|block| block.row_count() > 0)
        .ok_or("valid query returned no data rows")?;
    let value: u8 = block.column::<u8>("v")?.get(0)?;
    assert_eq!(value, 42, "connection must remain usable after cancel()");
    Ok(())
}

#[test]
fn insert_into_missing_table_errs_then_connection_still_works()
-> Result<(), Box<dyn std::error::Error>> {
    let Ok(mut client) = connect() else {
        eprintln!("ClickHouse test server is not available; skipping live test");
        return Ok(());
    };
    let err = client
        .insert(
            "INSERT INTO no_such_table_xyz (id) VALUES",
            "no_such_table_xyz",
            &[],
        )
        .expect_err("INSERT into a missing table must surface the server exception");
    assert!(err.is_server_error(), "expected ServerError, got: {err:?}");
    let blocks = client.query("SELECT toUInt8(7) AS v")?;
    let block = blocks
        .iter()
        .find(|block| block.row_count() > 0)
        .ok_or("valid query returned no data rows")?;
    let value: u8 = block.column::<u8>("v")?.get(0)?;
    assert_eq!(
        value, 7,
        "connection must remain usable after a failed insert"
    );
    Ok(())
}
