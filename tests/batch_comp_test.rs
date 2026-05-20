mod common;
use st_clickhouse::compression::CompressionMethod;

#[tokio::test]
async fn test_batch_bare_lz4() {
    eprintln!("connecting...");
    let client = common::connect_client().await;
    eprintln!("running batch with LZ4...");
    let results = client
        .batch()
        .with_compression(CompressionMethod::Lz4)
        .query("SELECT 1 AS a")
        .query("SELECT 2 AS b")
        .execute()
        .await;
    match results {
        Ok(r) => eprintln!("OK: {} results", r.len()),
        Err(e) => eprintln!("ERR: {:?}", e),
    }
}
