use st_clickhouse::sync::{ClientConfig, SyncClient};

fn main() -> st_clickhouse::sync::Result<()> {
    let config = ClientConfig::new().with_host("127.0.0.1").with_port(9000);

    let mut client = SyncClient::connect_with_config(config)?;
    let blocks = client.query("SELECT 1")?;
    let rows: usize = blocks.iter().map(|block| block.rows).sum();
    println!("rows={rows}");

    Ok(())
}
