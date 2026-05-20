crate::scalar_roundtrip_test!(
    test_uint8_roundtrip,
    "rt_uint8",
    "UInt8",
    "(0), (1), (127), (255)",
    u8,
    vec![(0,), (1,), (127,), (255,)],
    "SUCCESS: UInt8 roundtrip"
);

crate::scalar_roundtrip_test!(
    test_uint64_roundtrip,
    "rt_uint64",
    "UInt64",
    "(0), (42), (18446744073709551615)",
    u64,
    vec![(0,), (42,), (u64::MAX,)],
    "SUCCESS: UInt64 roundtrip"
);

crate::scalar_roundtrip_test!(
    test_int8_roundtrip,
    "rt_int8",
    "Int8",
    "(-128), (-1), (0), (127)",
    i8,
    vec![(-128,), (-1,), (0,), (127,)],
    "SUCCESS: Int8 roundtrip"
);

crate::scalar_roundtrip_test!(
    test_int64_roundtrip,
    "rt_int64",
    "Int64",
    "(-9223372036854775808), (-1), (0), (9223372036854775807)",
    i64,
    vec![(i64::MIN,), (-1,), (0,), (i64::MAX,)],
    "SUCCESS: Int64 roundtrip"
);

#[tokio::test]
async fn test_float32_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_float32")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_float32 (val Float32) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_float32 VALUES (0.0), (-3.14), (1.5), (3.4028235e38)")
        .await
        .expect("test operation failed");

    let block = client
        .query("SELECT val FROM rt_float32 ORDER BY val")
        .block()
        .await
        .expect("test operation failed");
    let col = block.column::<f32>("val").expect("test operation failed");
    let mut sorted: Vec<f32> = (0..col.len())
        .map(|i| col.get(i).expect("test operation failed"))
        .collect();
    sorted.sort_by(|a, b| a.partial_cmp(b).expect("test operation failed"));

    assert_eq!(sorted.len(), 4);
    let f32_piish = "3.14".parse::<f32>().expect("test operation failed");
    assert!(
        (sorted[0] - (-f32_piish)).abs() < 0.01,
        "expected -3.14, got {}",
        sorted[0],
    );
    assert_eq!(sorted[1], 0.0);
    assert!((sorted[2] - 1.5).abs() < 0.01);
    assert!(
        (sorted[3] - 3.4028235e38).abs() / 3.4028235e38 < 1e-6,
        "expected Float32 max-ish value, got {}",
        sorted[3],
    );
    eprintln!("SUCCESS: Float32 roundtrip");
}

#[tokio::test]
async fn test_float64_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_float64")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_float64 (val Float64) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_float64 VALUES (0.0), (-1.5), (3.141592653589793), (1.7976931348623157e308)")
        .await
        .expect("test operation failed");
    let rows: Vec<(f64,)> = client
        .query("SELECT val FROM rt_float64 ORDER BY val")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].0, -1.5);
    assert_eq!(rows[1].0, 0.0);
    assert!((rows[2].0 - std::f64::consts::PI).abs() < 1e-14);
    assert_eq!(rows[3].0, 1.7976931348623157e308);
    eprintln!("SUCCESS: Float64 roundtrip");
}

crate::scalar_roundtrip_test!(
    test_bool_roundtrip,
    "rt_bool",
    "Bool",
    "(true), (false), (true)",
    bool,
    vec![(false,), (true,), (true,)],
    "SUCCESS: Bool roundtrip"
);
