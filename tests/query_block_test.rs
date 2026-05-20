mod common;
#[tokio::test]
async fn test_query_block() {
    eprintln!("connecting...");
    let client = common::connect_client().await;
    eprintln!("connected");
    eprintln!("sending query...");
    match client.query("SELECT 1").block().await {
        Ok(block) => {
            eprintln!("OK: {} rows", block.row_count());
        },
        Err(e) => {
            eprintln!("ERROR: {:?}", e);
        },
    }
}

#[tokio::test]
async fn test_query_row_count_fast_path() {
    let client = common::connect_client().await;
    let rows = client
        .query("SELECT number, toString(number), [number] FROM system.numbers LIMIT 128")
        .row_count()
        .await
        .expect("row count query should succeed");

    assert_eq!(rows, 128);
}
