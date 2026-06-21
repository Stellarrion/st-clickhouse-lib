//! Shared test utilities for locating a ClickHouse native TCP endpoint.
//!
//! Connection setup and DNS resolution stay in the client code under test.

pub fn clickhouse_addr() -> &'static str {
    "127.0.0.1:9000"
}

#[allow(dead_code)]
pub async fn connect_client() -> st_clickhouse::Client {
    let addr = clickhouse_addr();
    // Env override for non-default users (e.g. CLICKHOUSE_USER=honne CLICKHOUSE_PASSWORD=honne).
    if let (Ok(user), Ok(password)) = (
        std::env::var("CLICKHOUSE_USER"),
        std::env::var("CLICKHOUSE_PASSWORD"),
    ) {
        return st_clickhouse::Client::connect_with_credentials(addr, &user, &password)
            .await
            .expect("test operation failed");
    }
    match st_clickhouse::Client::connect(addr).await {
        Ok(client) => client,
        Err(_) => st_clickhouse::Client::connect_with_credentials(addr, "default", "test")
            .await
            .expect("test operation failed"),
    }
}

#[allow(dead_code)]
pub async fn connect_client_pool(size: usize) -> st_clickhouse::Client {
    let addr = clickhouse_addr();
    if let (Ok(user), Ok(password)) = (
        std::env::var("CLICKHOUSE_USER"),
        std::env::var("CLICKHOUSE_PASSWORD"),
    ) {
        return st_clickhouse::Client::connect_with_pool_credentials(addr, size, &user, &password)
            .await
            .expect("test operation failed");
    }
    match st_clickhouse::Client::connect_with_pool(addr, size).await {
        Ok(client) => client,
        Err(_) => {
            st_clickhouse::Client::connect_with_pool_credentials(addr, size, "default", "test")
                .await
                .expect("test operation failed")
        },
    }
}
