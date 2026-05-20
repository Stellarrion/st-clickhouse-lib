use st_clickhouse::column::{Date, DateTime, DateTime64Value, Decimal32, Decimal64, Decimal128};

#[tokio::test]
async fn test_date_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_date")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_date (val Date) ENGINE = Memory")
        .await
        .expect("test operation failed");
    let expected = Date::from_days(crate::days_since_epoch(2024, 1, 15));
    client
        .execute("INSERT INTO rt_date VALUES ('2024-01-15')")
        .await
        .expect("test operation failed");

    let rows: Vec<(Date,)> = client
        .query("SELECT val FROM rt_date")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, expected);
    eprintln!("SUCCESS: Date roundtrip (days={})", expected.as_days());
}

#[tokio::test]
async fn test_datetime_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_dt")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_dt (val DateTime) ENGINE = Memory")
        .await
        .expect("test operation failed");
    let expected = DateTime::from_secs(crate::dt_secs(2024, 6, 15, 13, 45, 30));
    client
        .execute("INSERT INTO rt_dt VALUES ('2024-06-15 13:45:30')")
        .await
        .expect("test operation failed");

    let rows: Vec<(DateTime,)> = client
        .query("SELECT val FROM rt_dt")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, expected);
    eprintln!("SUCCESS: DateTime roundtrip (secs={})", expected.as_secs());
}

#[tokio::test]
async fn test_datetime64_roundtrip() {
    let client = crate::common::connect_client().await;

    client
        .execute("DROP TABLE IF EXISTS rt_dt64_3")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_dt64_3 (val DateTime64(3)) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_dt64_3 VALUES ('2024-01-15 12:30:00.123')")
        .await
        .expect("test operation failed");
    let block = client
        .query("SELECT val FROM rt_dt64_3")
        .block()
        .await
        .expect("test operation failed");
    let col = block
        .column::<DateTime64Value>("val")
        .expect("test operation failed");
    assert_eq!(col.len(), 1);
    let v = col.get(0).expect("test operation failed");
    let expected_ts = crate::days_since_epoch(2024, 1, 15) as i64 * 86400 + 12 * 3600 + 30 * 60;
    assert_eq!(v.to_timestamp(3), expected_ts);
    assert_eq!(v.0, expected_ts * 1000 + 123);
    eprintln!("SUCCESS: DateTime64(3) roundtrip");

    client
        .execute("DROP TABLE IF EXISTS rt_dt64_6")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_dt64_6 (val DateTime64(6)) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_dt64_6 VALUES ('2024-01-15 12:30:00.123456')")
        .await
        .expect("test operation failed");
    let block = client
        .query("SELECT val FROM rt_dt64_6")
        .block()
        .await
        .expect("test operation failed");
    let col = block
        .column::<DateTime64Value>("val")
        .expect("test operation failed");
    assert_eq!(col.len(), 1);
    let v = col.get(0).expect("test operation failed");
    assert_eq!(v.0, expected_ts * 1_000_000 + 123456);
    eprintln!("SUCCESS: DateTime64(6) roundtrip");
}

crate::scalar_roundtrip_test!(
    test_decimal32_roundtrip,
    "rt_dec32",
    "Decimal32(2)",
    "(123.45), (-99.99)",
    Decimal32,
    vec![(Decimal32(-9999),), (Decimal32(12345),)],
    "SUCCESS: Decimal32(2) roundtrip"
);

crate::scalar_roundtrip_test!(
    test_decimal64_roundtrip,
    "rt_dec64",
    "Decimal64(4)",
    "(123.4567), (-1.0001)",
    Decimal64,
    vec![(Decimal64(-10001),), (Decimal64(1234567),)],
    "SUCCESS: Decimal64(4) roundtrip"
);

crate::scalar_roundtrip_test!(
    test_decimal128_roundtrip,
    "rt_dec128",
    "Decimal128(10)",
    "(123.4567890123), (0.0)",
    Decimal128,
    vec![(Decimal128(0),), (Decimal128(1234567890123),)],
    "SUCCESS: Decimal128(10) roundtrip"
);
