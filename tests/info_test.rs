mod common;
#[tokio::test]
async fn test_server_info() {
    let client = common::connect_client().await;
    let info = client.server_info().await.expect("test operation failed");
    eprintln!("rev={}", info.revision);
}
