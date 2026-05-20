mod common;
use st_clickhouse::compression::CompressionMethod;

#[tokio::test]
async fn test_regular_query_lz4() {
    eprintln!("connecting...");
    let client = common::connect_client().await;
    eprintln!("query with LZ4...");
    match client
        .query("SELECT 1")
        .with_compression(CompressionMethod::Lz4)
        .block()
        .await
    {
        Ok(block) => eprintln!("OK: {} rows", block.row_count()),
        Err(e) => eprintln!("ERR: {:?}", e),
    }
}

#[tokio::test]
async fn test_regular_query_none() {
    eprintln!("connecting...");
    let client = common::connect_client().await;
    eprintln!("query with None...");
    match client
        .query("SELECT 1")
        .with_compression(CompressionMethod::None)
        .block()
        .await
    {
        Ok(block) => eprintln!("OK: {} rows", block.row_count()),
        Err(e) => eprintln!("ERR: {:?}", e),
    }
}
