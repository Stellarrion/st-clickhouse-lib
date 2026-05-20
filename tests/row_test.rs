mod common;

#[tokio::test]
async fn test_cursor_streaming() {
    let client = common::connect_client().await;
    let block = client
        .query("SELECT number FROM system.numbers LIMIT 10")
        .block()
        .await
        .expect("test operation failed");
    let col = block
        .column::<u64>("number")
        .expect("test operation failed");
    let mut count = 0u64;
    for i in 0..col.len() {
        assert_eq!(col.get(i).expect("test operation failed"), count);
        count += 1;
    }
    assert_eq!(count, 10);
    eprintln!("SUCCESS: columnar streaming 10 rows!");
}

#[tokio::test]
async fn test_cursor_collect() {
    let client = common::connect_client().await;
    let block = client
        .query("SELECT number FROM system.numbers LIMIT 5")
        .block()
        .await
        .expect("test operation failed");
    let col = block
        .column::<u64>("number")
        .expect("test operation failed");
    assert_eq!(col.len(), 5);
    for (i, row) in col
        .as_slice()
        .expect("test operation failed")
        .iter()
        .enumerate()
    {
        assert_eq!(*row, i as u64);
    }
    eprintln!("SUCCESS: columnar collect 5 rows!");
}

#[derive(st_clickhouse::Row)]
struct OneU64 {
    v: u64,
}

#[derive(st_clickhouse::Row)]
struct OneString {
    name: String,
}

#[derive(st_clickhouse::Row)]
struct TwoFields {
    id: u64,
    value: u64,
}

#[derive(st_clickhouse::Row)]
struct RenamedDefaultSkipped {
    #[clickhouse(rename = "user_id")]
    id: u64,
    #[clickhouse(default)]
    missing: u64,
    #[clickhouse(skip)]
    skipped: String,
}

#[tokio::test]
async fn test_fetch_all_derive_one_u64() {
    let client = common::connect_client().await;
    let rows: Vec<OneU64> = client
        .query("SELECT toUInt64(1) AS v")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].v, 1);
    eprintln!("SUCCESS: derive one u64!");
}

#[tokio::test]
async fn test_fetch_all_derive_one_string() {
    let client = common::connect_client().await;
    let rows: Vec<OneString> = client
        .query("SELECT 'hello' AS name")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "hello");
    eprintln!("SUCCESS: derive one string!");
}

#[tokio::test]
async fn test_fetch_all_derive_two_fields() {
    let client = common::connect_client().await;
    let rows: Vec<TwoFields> = client
        .query("SELECT number AS id, number * 10 AS value FROM system.numbers LIMIT 3")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 3);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.id, i as u64);
        assert_eq!(row.value, (i * 10) as u64);
    }
    eprintln!("SUCCESS: derive two fields!");
}

#[tokio::test]
async fn test_derive_rename_default_skip() {
    let client = common::connect_client().await;
    let rows: Vec<RenamedDefaultSkipped> = client
        .query("SELECT toUInt64(7) AS user_id")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 7);
    assert_eq!(rows[0].missing, 0);
    assert_eq!(rows[0].skipped, "");
}

#[tokio::test]
async fn test_query_one_optional_scalar_helpers() {
    let client = common::connect_client().await;
    let one: OneU64 = client
        .query("SELECT toUInt64(9) AS v")
        .one()
        .await
        .expect("test operation failed");
    assert_eq!(one.v, 9);

    let none: Option<OneU64> = client
        .query("SELECT toUInt64(9) AS v WHERE 0")
        .optional()
        .await
        .expect("test operation failed");
    assert!(none.is_none());

    let scalar: u64 = client
        .query("SELECT toUInt64(11)")
        .scalar()
        .await
        .expect("test operation failed");
    assert_eq!(scalar, 11);

    let fetched_one: OneU64 = client
        .query("SELECT toUInt64(12) AS v")
        .fetch()
        .await
        .expect("test operation failed");
    assert_eq!(fetched_one.v, 12);

    let fetched_rows: Vec<OneU64> = client
        .query("SELECT number AS v FROM system.numbers LIMIT 2")
        .fetch()
        .await
        .expect("test operation failed");
    assert_eq!(fetched_rows.len(), 2);
    assert_eq!(fetched_rows[1].v, 1);

    let fetched_scalar = client
        .query("SELECT toUInt64(13)")
        .fetch::<st_clickhouse::Scalar<u64>>()
        .await
        .expect("test operation failed");
    assert_eq!(fetched_scalar.into_inner(), 13);
}

#[tokio::test]
async fn test_fetch_all_tuple() {
    let client = common::connect_client().await;
    let rows: Vec<(u8,)> = client
        .query("SELECT 1 AS v")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1u8);
    eprintln!("SUCCESS: single tuple!");
}

#[tokio::test]
async fn test_fetch_all_numbers() {
    let client = common::connect_client().await;
    let rows: Vec<(u64,)> = client
        .query("SELECT number FROM system.numbers LIMIT 5")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 5);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.0, i as u64);
    }
    eprintln!("SUCCESS: 5 numbers!");
}

#[tokio::test]
async fn test_fetch_nullable() {
    let client = common::connect_client().await;
    let rows: Vec<(Option<u8>,)> = client
        .query("SELECT nullIf(number % 2, 1) FROM system.numbers LIMIT 4")
        .all()
        .await
        .expect("test operation failed");
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].0, Some(0));
    assert_eq!(rows[1].0, None);
    assert_eq!(rows[2].0, Some(0));
    assert_eq!(rows[3].0, None);
    eprintln!("SUCCESS: Nullable<UInt8> via fetch_all!");
}
