//! Live sync-client cumulative response-budget tests.
//!
//! Requires a ClickHouse native TCP endpoint. Configure with the standard
//! environment variables used by the other sync integration tests:
//!   CLICKHOUSE_HOST (default 127.0.0.1:9000), CLICKHOUSE_USER (default
//!   "default"), CLICKHOUSE_PASS (default "test").
//!
//! Skips (returns Ok) when the server is not reachable.

use std::time::Duration;

use st_clickhouse::sync::client::SyncClient;
use st_clickhouse::sync::config::ClientConfig;
use st_clickhouse::sync::error::Error;

fn connect(max_response_size: usize) -> st_clickhouse::sync::Result<SyncClient> {
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
        .with_query_timeout(Duration::from_secs(10))
        .with_max_response_size(max_response_size);
    SyncClient::connect_with_config(config)
}

const BIG_QUERY: &str = "SELECT number, repeat('x', 1000) FROM system.numbers LIMIT 100000";

/// A budget breach returns `ResponseTooLarge`, and the recovery (Cancel plus
/// a bounded discard through the SAME buffered reader) must be deterministic:
/// the very next query on the same client succeeds every time. Repeated
/// because the pre-fix bug dropped up to 64 KiB of read-ahead and killed the
/// connection on roughly 1 run in 12.
#[test]
fn live_sync_budget_breach_recovery_is_deterministic() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(mut client) = connect(8 * 1024) else {
        eprintln!("live server not available; skipping");
        return Ok(());
    };
    for run in 0..12 {
        match client.query(BIG_QUERY) {
            Err(Error::ResponseTooLarge { .. }) => {},
            Ok(_) => {
                return Err(format!("run {run}: expected ResponseTooLarge, got Ok").into());
            },
            Err(other) => {
                return Err(format!("run {run}: expected ResponseTooLarge, got {other}").into());
            },
        }
        client
            .query("SELECT 1")
            .map_err(|e| format!("run {run}: follow-up after breach failed: {e}"))?;
    }
    Ok(())
}

/// A budget at or above the result size never fires; normal queries are
/// unaffected by the limit.
#[test]
fn live_sync_budget_large_enough_does_not_fire() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(mut client) = connect(256 * 1024 * 1024) else {
        eprintln!("live server not available; skipping");
        return Ok(());
    };
    let blocks = client.query("SELECT number FROM system.numbers LIMIT 10")?;
    let total: usize = blocks.iter().map(|b| b.row_count()).sum();
    assert!(total > 0);
    Ok(())
}
