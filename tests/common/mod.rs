//! Shared test utilities for locating a ClickHouse native TCP endpoint.
//!
//! Connection setup and DNS resolution stay in the client code under test.

pub fn clickhouse_addr() -> &'static str {
    "127.0.0.1:9000"
}

#[allow(dead_code)]
pub async fn connect_client() -> st_clickhouse::Client {
    match st_clickhouse::Client::connect(clickhouse_addr()).await {
        Ok(client) => client,
        Err(_) => {
            st_clickhouse::Client::connect_with_credentials(clickhouse_addr(), "default", "test")
                .await
                .expect("test operation failed")
        },
    }
}

#[allow(dead_code)]
pub async fn connect_client_pool(size: usize) -> st_clickhouse::Client {
    match st_clickhouse::Client::connect_with_pool(clickhouse_addr(), size).await {
        Ok(client) => client,
        Err(_) => st_clickhouse::Client::connect_with_pool_credentials(
            clickhouse_addr(),
            size,
            "default",
            "test",
        )
        .await
        .expect("test operation failed"),
    }
}
