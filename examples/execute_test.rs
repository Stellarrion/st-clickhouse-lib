use st_clickhouse::connection::Client;
#[tokio::main]
async fn main() {
    let client = Client::connect("127.0.0.1:9000")
        .await
        .expect("test operation failed");
    eprintln!("connected, executing...");
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.execute("SELECT 1"),
    )
    .await
    {
        Ok(Ok(())) => eprintln!("execute OK"),
        Ok(Err(e)) => eprintln!("execute ERR: {e:?}"),
        Err(_) => eprintln!("execute TIMEOUT"),
    }
}
