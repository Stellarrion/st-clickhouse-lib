mod common;
#[tokio::test]
async fn test_minimal() {
    eprintln!("connecting...");
    let client = common::connect_client().await;
    eprintln!("connected!");
    let info = client.server_info().await.expect("test operation failed");
    eprintln!("server: {:?}", info);
}
