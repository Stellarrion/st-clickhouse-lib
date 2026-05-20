use st_clickhouse::connection::Client;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let client = Client::connect("127.0.0.1:9000")
        .await
        .expect("test operation failed");
    eprintln!("connected");
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        client.query("SELECT 1 AS x").block(),
    )
    .await;
    match result {
        Ok(Ok(block)) => eprintln!("SUCCESS: {} rows", block.row_count()),
        Ok(Err(e)) => eprintln!("ERROR: {:?}", e),
        Err(_) => eprintln!("TIMEOUT"),
    }
}
