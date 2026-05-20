use st_clickhouse::ClickHouseColumnData;
use std::collections::HashMap;

#[tokio::test]
async fn test_nullable_uint8_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_null_u8")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_null_u8 (val Nullable(UInt8)) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_null_u8 VALUES (42), (NULL), (0), (NULL), (255)")
        .await
        .expect("test operation failed");

    let block = client
        .query("SELECT val FROM rt_null_u8")
        .block()
        .await
        .expect("test operation failed");
    let col = block
        .column::<Option<u8>>("val")
        .expect("test operation failed");
    assert_eq!(col.len(), 5);
    assert_eq!(col.get(0).expect("test operation failed"), Some(42u8));
    assert_eq!(col.get(1).expect("test operation failed"), None);
    assert_eq!(col.get(2).expect("test operation failed"), Some(0u8));
    assert_eq!(col.get(3).expect("test operation failed"), None);
    assert_eq!(col.get(4).expect("test operation failed"), Some(255u8));
    eprintln!("SUCCESS: Nullable(UInt8) roundtrip");
}

#[tokio::test]
async fn test_nullable_string_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_null_str")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_null_str (val Nullable(String)) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_null_str VALUES ('hello'), (NULL), (''), (NULL), ('world')")
        .await
        .expect("test operation failed");

    let block = client
        .query("SELECT val FROM rt_null_str")
        .block()
        .await
        .expect("test operation failed");
    let col = block
        .column::<Option<String>>("val")
        .expect("test operation failed");
    assert_eq!(col.len(), 5);
    assert_eq!(
        col.get(0).expect("test operation failed"),
        Some("hello".to_string())
    );
    assert_eq!(col.get(1).expect("test operation failed"), None);
    assert_eq!(
        col.get(2).expect("test operation failed"),
        Some("".to_string())
    );
    assert_eq!(col.get(3).expect("test operation failed"), None);
    assert_eq!(
        col.get(4).expect("test operation failed"),
        Some("world".to_string())
    );
    eprintln!("SUCCESS: Nullable(String) roundtrip");
}

#[tokio::test]
async fn test_array_uint64_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_arr_u64")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_arr_u64 (val Array(UInt64)) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_arr_u64 VALUES ([1, 2, 3]), ([]), ([42])")
        .await
        .expect("test operation failed");

    let block = client
        .query("SELECT val FROM rt_arr_u64 ORDER BY length(val)")
        .block()
        .await
        .expect("test operation failed");
    let col = block
        .column::<Vec<u64>>("val")
        .expect("test operation failed");
    assert_eq!(col.len(), 3);
    assert_eq!(
        col.get(0).expect("test operation failed"),
        vec![] as Vec<u64>
    );
    assert_eq!(col.get(1).expect("test operation failed"), vec![42u64]);
    assert_eq!(col.get(2).expect("test operation failed"), vec![1u64, 2, 3]);
    eprintln!("SUCCESS: Array(UInt64) roundtrip");
}

#[tokio::test]
async fn test_array_string_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_arr_str")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_arr_str (val Array(String)) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_arr_str VALUES (['hello', 'world']), ([]), ([''])")
        .await
        .expect("test operation failed");

    let block = client
        .query("SELECT val FROM rt_arr_str ORDER BY length(val)")
        .block()
        .await
        .expect("test operation failed");
    let col = block
        .column::<Vec<String>>("val")
        .expect("test operation failed");
    assert_eq!(col.len(), 3);
    assert_eq!(
        col.get(0).expect("test operation failed"),
        vec![] as Vec<String>
    );
    assert_eq!(
        col.get(1).expect("test operation failed"),
        vec!["".to_string()]
    );
    assert_eq!(
        col.get(2).expect("test operation failed"),
        vec!["hello".to_string(), "world".to_string()]
    );
    eprintln!("SUCCESS: Array(String) roundtrip");
}

#[tokio::test]
async fn test_array_nullable_uint64_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_arr_null_u64")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_arr_null_u64 (val Array(Nullable(UInt64))) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_arr_null_u64 VALUES ([1, NULL, 3]), ([]), ([NULL])")
        .await
        .expect("test operation failed");

    let block = client
        .query("SELECT val FROM rt_arr_null_u64 ORDER BY length(val)")
        .block()
        .await
        .expect("test operation failed");
    let col = block
        .column::<Vec<Option<u64>>>("val")
        .expect("test operation failed");
    assert_eq!(col.len(), 3);
    assert_eq!(
        col.get(0).expect("test operation failed"),
        vec![] as Vec<Option<u64>>
    );
    assert_eq!(col.get(1).expect("test operation failed"), vec![None]);
    assert_eq!(
        col.get(2).expect("test operation failed"),
        vec![Some(1u64), None, Some(3u64)]
    );
    eprintln!("SUCCESS: Array(Nullable(UInt64)) roundtrip");
}

#[tokio::test]
async fn test_map_string_uint64_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_map")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_map (val Map(String, UInt64)) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_map VALUES ({}), ({'key1': 100, 'key2': 200})")
        .await
        .expect("test operation failed");

    let block = client
        .query("SELECT val FROM rt_map ORDER BY length(val)")
        .block()
        .await
        .expect("test operation failed");
    let col = block
        .column::<HashMap<String, u64>>("val")
        .expect("test operation failed");
    assert_eq!(col.len(), 2);
    let m0 = col.get(0).expect("test operation failed");
    assert!(m0.is_empty());
    let m1 = col.get(1).expect("test operation failed");
    assert_eq!(m1.len(), 2);
    assert_eq!(m1.get("key1"), Some(&100));
    assert_eq!(m1.get("key2"), Some(&200));
    eprintln!("SUCCESS: Map(String, UInt64) roundtrip");
}

#[tokio::test]
async fn test_enum8_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_enum8")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_enum8 (val Enum8('x' = 5, 'y' = 10, 'z' = 15)) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_enum8 VALUES ('x'), ('z'), ('y')")
        .await
        .expect("test operation failed");

    let rows: Vec<(i8,)> = client
        .query("SELECT val FROM rt_enum8 ORDER BY val")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, 5);
    assert_eq!(rows[1].0, 10);
    assert_eq!(rows[2].0, 15);
    eprintln!("SUCCESS: Enum8 roundtrip");
}

#[tokio::test]
async fn test_enum16_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_enum16")
        .await
        .expect("test operation failed");
    client.execute("CREATE TABLE rt_enum16 (val Enum16('a' = 100, 'b' = 20000, 'c' = 300)) ENGINE = Memory").await.expect("test operation failed");
    client
        .execute("INSERT INTO rt_enum16 VALUES ('a'), ('c'), ('b')")
        .await
        .expect("test operation failed");

    let rows: Vec<(i16,)> = client
        .query("SELECT val FROM rt_enum16 ORDER BY val")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, 100);
    assert_eq!(rows[1].0, 300);
    assert_eq!(rows[2].0, 20000);
    eprintln!("SUCCESS: Enum16 roundtrip");
}

#[tokio::test]
async fn test_tuple_uint64_string_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_tuple")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_tuple (val Tuple(UInt64, String)) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_tuple VALUES ((42, 'hello')), ((99, 'world'))")
        .await
        .expect("test operation failed");

    let block = client
        .query("SELECT val FROM rt_tuple ORDER BY val")
        .block()
        .await
        .expect("test operation failed");
    let col = block
        .column::<(u64, String)>("val")
        .expect("test operation failed");
    assert_eq!(col.len(), 2);
    assert_eq!(
        col.get(0).expect("test operation failed"),
        (42u64, "hello".to_string())
    );
    assert_eq!(
        col.get(1).expect("test operation failed"),
        (99u64, "world".to_string())
    );
    eprintln!("SUCCESS: Tuple(UInt64, String) roundtrip");
}
