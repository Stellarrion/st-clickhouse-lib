use st_clickhouse::ClickHouseColumnData;
use st_clickhouse::column::FixedStringBytes;

#[tokio::test]
async fn test_string_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_string")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_string (val String) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_string VALUES (''), ('hello'), ('it''s'), ('a\\nb'), ('cafe\\'s'), ('日本語')")
        .await
        .expect("test operation failed");

    let block = client
        .query("SELECT val FROM rt_string")
        .block()
        .await
        .expect("test operation failed");
    let col = block
        .column::<String>("val")
        .expect("test operation failed");
    let mut vals: Vec<String> = (0..col.len())
        .map(|i| col.get(i).expect("test operation failed"))
        .collect();
    vals.sort();
    assert_eq!(vals[0], "");
    assert_eq!(vals[1], "a\nb");
    assert_eq!(vals[2], "cafe's");
    assert_eq!(vals[3], "hello");
    assert_eq!(vals[4], "it's");
    assert_eq!(vals[5], "日本語");
    eprintln!("SUCCESS: String roundtrip");
}

#[tokio::test]
async fn test_fixed_string_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_fstr")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_fstr (val FixedString(5)) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_fstr VALUES ('abcde'), ('ab')")
        .await
        .expect("test operation failed");

    let block = client
        .query("SELECT val FROM rt_fstr ORDER BY val")
        .block()
        .await
        .expect("test operation failed");
    let col = block
        .column::<FixedStringBytes>("val")
        .expect("test operation failed");
    assert_eq!(col.len(), 2);
    assert_eq!(
        &col.get(0).expect("test operation failed").0[..],
        b"ab\0\0\0"
    );
    assert_eq!(&col.get(1).expect("test operation failed").0[..], b"abcde");
    eprintln!("SUCCESS: FixedString(5) roundtrip");
}

#[tokio::test]
async fn test_low_cardinality_string_roundtrip() {
    let client = crate::common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS rt_lc")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE rt_lc (val LowCardinality(String)) ENGINE = Memory")
        .await
        .expect("test operation failed");
    client
        .execute("INSERT INTO rt_lc VALUES ('foo'), ('bar'), ('foo'), ('baz')")
        .await
        .expect("test operation failed");

    let rows: Vec<(String,)> = client
        .query("SELECT val FROM rt_lc ORDER BY val")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].0, "bar");
    assert_eq!(rows[1].0, "baz");
    assert_eq!(rows[2].0, "foo");
    assert_eq!(rows[3].0, "foo");
    eprintln!("SUCCESS: LowCardinality(String) roundtrip");
}
