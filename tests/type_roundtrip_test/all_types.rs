use st_clickhouse::ClickHouseColumnData;
use st_clickhouse::column::{
    Date, DateTime, DateTime64Value, Decimal32, Decimal64, Decimal128, FixedStringBytes, Ipv4,
    Ipv6, Uuid,
};
use std::collections::HashMap;

#[tokio::test]
async fn test_all_types_single_row() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_all")
        .await
        .expect("test operation failed");
    client
        .execute(
            "CREATE TABLE rt_all (\
            u8 UInt8,\
            u64 UInt64,\
            i8 Int8,\
            i64 Int64,\
            f32 Float32,\
            f64 Float64,\
            s String,\
            fs FixedString(5),\
            d Date,\
            dt DateTime,\
            dt64_3 DateTime64(3),\
            dec32 Decimal32(2),\
            dec64 Decimal64(4),\
            dec128 Decimal128(10),\
            uid UUID,\
            ip4 IPv4,\
            ip6 IPv6,\
            b Bool,\
            e8 Enum8('x'=5),\
            e16 Enum16('y'=1000),\
            lc LowCardinality(String),\
            null_u8 Nullable(UInt8),\
            null_str Nullable(String),\
            arr_u64 Array(UInt64),\
            arr_str Array(String),\
            arr_null Array(Nullable(UInt64)),\
            m Map(String, UInt64),\
            tup Tuple(UInt64, String)\
        ) ENGINE = Memory",
        )
        .await
        .expect("test operation failed");

    client
        .execute(
            "INSERT INTO rt_all VALUES (\
            42,\
            9876543210,\
            -128,\
            -1000000,\
            3.14,\
            2.718281828459045,\
            'multi-type-row',\
            'FIXED',\
            '2024-05-18',\
            '2024-05-18 08:30:00',\
            '2024-05-18 08:30:00.500',\
            99.99,\
            123.4567,\
            1.2345678901,\
            'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',\
            '10.0.0.1',\
            '::1',\
            true,\
            'x',\
            'y',\
            'lowcard',\
            100,\
            'not null',\
            [1, 2, 3],\
            ['a', 'b'],\
            [10, NULL, 20],\
            {'k': 999},\
            (77, 'tupval')\
        )",
        )
        .await
        .expect("test operation failed");

    let block = client
        .query("SELECT * FROM rt_all")
        .block()
        .await
        .expect("test operation failed");
    assert_eq!(block.row_count(), 1);

    let col_u8 = block.column::<u8>("u8").expect("test operation failed");
    assert_eq!(col_u8.get(0).expect("test operation failed"), 42u8);

    let col_u64 = block.column::<u64>("u64").expect("test operation failed");
    assert_eq!(
        col_u64.get(0).expect("test operation failed"),
        9876543210u64
    );

    let col_i8 = block.column::<i8>("i8").expect("test operation failed");
    assert_eq!(col_i8.get(0).expect("test operation failed"), -128i8);

    let col_i64 = block.column::<i64>("i64").expect("test operation failed");
    assert_eq!(col_i64.get(0).expect("test operation failed"), -1000000i64);

    let col_f32 = block.column::<f32>("f32").expect("test operation failed");
    let f32_piish = "3.14".parse::<f32>().expect("test operation failed");
    assert!((col_f32.get(0).expect("test operation failed") - f32_piish).abs() < 0.01);

    let col_f64 = block.column::<f64>("f64").expect("test operation failed");
    assert!((col_f64.get(0).expect("test operation failed") - std::f64::consts::E).abs() < 1e-14);

    let col_s = block.column::<String>("s").expect("test operation failed");
    assert_eq!(
        col_s.get(0).expect("test operation failed"),
        "multi-type-row"
    );

    let col_fs = block
        .column::<FixedStringBytes>("fs")
        .expect("test operation failed");
    assert_eq!(
        &col_fs.get(0).expect("test operation failed").0[..],
        b"FIXED"
    );

    let col_d = block.column::<Date>("d").expect("test operation failed");
    let expected_d = Date::from_days(crate::days_since_epoch(2024, 5, 18));
    assert_eq!(col_d.get(0).expect("test operation failed"), expected_d);

    let col_dt = block
        .column::<DateTime>("dt")
        .expect("test operation failed");
    let expected_dt = DateTime::from_secs(crate::dt_secs(2024, 5, 18, 8, 30, 0));
    assert_eq!(col_dt.get(0).expect("test operation failed"), expected_dt);

    let col_dt64 = block
        .column::<DateTime64Value>("dt64_3")
        .expect("test operation failed");
    let v = col_dt64.get(0).expect("test operation failed");
    assert_eq!(v.to_timestamp(3), expected_dt.as_secs() as i64);
    assert_eq!(v.0 % 1000, 500);

    let col_dec32 = block
        .column::<Decimal32>("dec32")
        .expect("test operation failed");
    assert_eq!(
        col_dec32.get(0).expect("test operation failed"),
        Decimal32(9999)
    );

    let col_dec64 = block
        .column::<Decimal64>("dec64")
        .expect("test operation failed");
    assert_eq!(
        col_dec64.get(0).expect("test operation failed"),
        Decimal64(1234567)
    );

    let col_dec128 = block
        .column::<Decimal128>("dec128")
        .expect("test operation failed");
    assert_eq!(
        col_dec128.get(0).expect("test operation failed"),
        Decimal128(12345678901)
    );

    let col_uid = block.column::<Uuid>("uid").expect("test operation failed");
    let uid_s = col_uid
        .get(0)
        .expect("test operation failed")
        .to_hyphenated();
    assert!(uid_s.contains("a0eebc99"));

    let col_ip4 = block.column::<Ipv4>("ip4").expect("test operation failed");
    assert_eq!(
        col_ip4.get(0).expect("test operation failed").to_std(),
        std::net::Ipv4Addr::new(10, 0, 0, 1)
    );

    let col_ip6 = block.column::<Ipv6>("ip6").expect("test operation failed");
    assert_eq!(
        col_ip6.get(0).expect("test operation failed").to_std(),
        std::net::Ipv6Addr::LOCALHOST
    );

    let col_b = block.column::<bool>("b").expect("test operation failed");
    assert!(col_b.get(0).expect("test operation failed"));

    let col_e8 = block.column::<i8>("e8").expect("test operation failed");
    assert_eq!(col_e8.get(0).expect("test operation failed"), 5i8);

    let col_e16 = block.column::<i16>("e16").expect("test operation failed");
    assert_eq!(col_e16.get(0).expect("test operation failed"), 1000i16);

    let col_lc = block.column::<String>("lc").expect("test operation failed");
    assert_eq!(col_lc.get(0).expect("test operation failed"), "lowcard");

    let col_null_u8 = block
        .column::<Option<u8>>("null_u8")
        .expect("test operation failed");
    assert_eq!(
        col_null_u8.get(0).expect("test operation failed"),
        Some(100u8)
    );

    let col_null_str = block
        .column::<Option<String>>("null_str")
        .expect("test operation failed");
    assert_eq!(
        col_null_str.get(0).expect("test operation failed"),
        Some("not null".to_string())
    );

    let col_arr_u64 = block
        .column::<Vec<u64>>("arr_u64")
        .expect("test operation failed");
    assert_eq!(
        col_arr_u64.get(0).expect("test operation failed"),
        vec![1u64, 2, 3]
    );

    let col_arr_str = block
        .column::<Vec<String>>("arr_str")
        .expect("test operation failed");
    assert_eq!(
        col_arr_str.get(0).expect("test operation failed"),
        vec!["a".to_string(), "b".to_string()]
    );

    let col_arr_null = block
        .column::<Vec<Option<u64>>>("arr_null")
        .expect("test operation failed");
    assert_eq!(
        col_arr_null.get(0).expect("test operation failed"),
        vec![Some(10u64), None, Some(20u64)]
    );

    let col_map = block
        .column::<HashMap<String, u64>>("m")
        .expect("test operation failed");
    let m = col_map.get(0).expect("test operation failed");
    assert_eq!(m.get("k"), Some(&999u64));

    let col_tup = block
        .column::<(u64, String)>("tup")
        .expect("test operation failed");
    assert_eq!(
        col_tup.get(0).expect("test operation failed"),
        (77u64, "tupval".to_string())
    );

    eprintln!("SUCCESS: all types single row roundtrip!");
}
