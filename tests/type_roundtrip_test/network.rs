use st_clickhouse::column::{Ipv4, Ipv6, Uuid};

#[tokio::test]
async fn test_uuid_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_uuid")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_uuid (val UUID) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_uuid VALUES ('550e8400-e29b-41d4-a716-446655440000'), ('00000000-0000-0000-0000-000000000000')")
        .await
        .expect("test operation failed");

    let block = client
        .query("SELECT val FROM rt_uuid")
        .block()
        .await
        .expect("test operation failed");
    let col = block.column::<Uuid>("val").expect("test operation failed");
    assert_eq!(col.len(), 2);
    let u0 = col.get(0).expect("test operation failed");
    let u1 = col.get(1).expect("test operation failed");
    assert_eq!(u1.as_u128(), 0u128);
    let s0 = u0.to_hyphenated();
    assert!(
        s0.contains("550e8400"),
        "expected 550e8400 in uuid string, got {}",
        s0
    );
    eprintln!("SUCCESS: UUID roundtrip ({} / zero)", s0);
}

#[tokio::test]
async fn test_ipv4_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_ipv4")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_ipv4 (val IPv4) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_ipv4 VALUES ('192.168.1.1'), ('0.0.0.0'), ('255.255.255.255')")
        .await
        .expect("test operation failed");

    let block = client
        .query("SELECT val FROM rt_ipv4 ORDER BY val")
        .block()
        .await
        .expect("test operation failed");
    let col = block.column::<Ipv4>("val").expect("test operation failed");
    assert_eq!(col.len(), 3);
    assert_eq!(col.get(0).expect("test operation failed").as_u32(), 0);
    assert_eq!(
        col.get(1).expect("test operation failed").to_std(),
        std::net::Ipv4Addr::new(192, 168, 1, 1)
    );
    assert_eq!(
        col.get(2).expect("test operation failed").to_std(),
        std::net::Ipv4Addr::new(255, 255, 255, 255)
    );
    eprintln!("SUCCESS: IPv4 roundtrip");
}

#[tokio::test]
async fn test_ipv6_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_ipv6")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_ipv6 (val IPv6) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_ipv6 VALUES ('2001:db8::1'), ('::')")
        .await
        .expect("test operation failed");

    let block = client
        .query("SELECT val FROM rt_ipv6 ORDER BY val")
        .block()
        .await
        .expect("test operation failed");
    let col = block.column::<Ipv6>("val").expect("test operation failed");
    assert_eq!(col.len(), 2);
    assert_eq!(col.get(0).expect("test operation failed").as_u128(), 0);
    let addr = col.get(1).expect("test operation failed").to_std();
    assert_eq!(
        addr,
        std::net::Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 1)
    );
    eprintln!("SUCCESS: IPv6 roundtrip");
}
