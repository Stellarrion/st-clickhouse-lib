mod common;
#[tokio::test]
async fn test_exec() {
    eprintln!("connecting...");
    let client = common::connect_client().await;
    eprintln!("connected! executing SELECT 1...");
    client
        .execute("SELECT 1")
        .await
        .expect("test operation failed");
    eprintln!("execute OK");
}
