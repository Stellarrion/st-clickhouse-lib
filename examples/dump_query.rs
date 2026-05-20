//! Dump the raw query packet bytes for debugging.
#[tokio::main]
async fn main() {
    use st_clickhouse::connection::Client;
    use std::time::Duration;

    // Build the same query packet that block() would send
    let client = Client::connect_with_credentials("127.0.0.1:9000", "default", "test")
        .await
        .expect("test operation failed")
        .with_recv_timeout(Duration::from_secs(2));

    // We need access to build_query_packet which is private.
    // Let's just use the high-level API and check it works.
    eprintln!("Client connected, sending query...");
    match client.query("SELECT 1 AS x").block().await {
        Ok(block) => {
            eprintln!(
                "SUCCESS: {} rows, {} cols",
                block.row_count(),
                block.column_count()
            );
        },
        Err(e) => {
            eprintln!("ERROR: {e:?}");
        },
    }
}
