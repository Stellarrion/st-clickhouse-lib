//! Minimal connection debug

#[tokio::main]
async fn main() {
    eprintln!("Connecting...");
    match st_clickhouse::Client::connect("127.0.0.1:9000").await {
        Ok(client) => {
            eprintln!("Connected! Running SELECT 1...");
            match client.query("SELECT 1").block().await {
                Ok(block) => {
                    eprintln!(
                        "Got block: {} rows, {} cols",
                        block.row_count(),
                        block.column_count()
                    );
                },
                Err(e) => {
                    eprintln!("Query error: {e:?}");
                },
            }
        },
        Err(e) => {
            eprintln!("Connect error: {e:?}");
        },
    }
}
