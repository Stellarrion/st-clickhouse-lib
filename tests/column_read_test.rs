mod common;

#[tokio::test]
async fn test_read_uint8_column() {
    let client = common::connect_client().await;
    let block = client
        .query("SELECT 1 AS val")
        .block()
        .await
        .expect("test operation failed");
    assert_eq!(block.row_count(), 1);
    let col = block.column::<u8>("val").expect("test operation failed");
    assert_eq!(col.get(0).expect("test operation failed"), 1u8);
    eprintln!("SUCCESS: UInt8 read!");
}

#[tokio::test]
async fn test_read_uint64_column() {
    let client = common::connect_client().await;
    let block = client
        .query("SELECT number FROM system.numbers LIMIT 10")
        .block()
        .await
        .expect("test operation failed");
    let col = block
        .column::<u64>("number")
        .expect("test operation failed");
    assert_eq!(col.len(), 10);
    for i in 0..10 {
        assert_eq!(col.get(i).expect("test operation failed"), i as u64);
    }
    eprintln!("SUCCESS: UInt64 10 rows read!");
}

#[tokio::test]
#[allow(clippy::approx_constant)]
async fn test_multi_column() {
    let client = common::connect_client().await;
    let block = client
        .query("SELECT 42 AS num, 3.14 AS pi")
        .block()
        .await
        .expect("test operation failed");
    assert_eq!(block.row_count(), 1);
    let nums = block.column::<u8>("num").expect("test operation failed");
    assert_eq!(nums.get(0).expect("test operation failed"), 42u8);
    let pis = block.column::<f64>("pi").expect("test operation failed");
    assert_eq!(pis.get(0).expect("test operation failed"), 3.14_f64);
    eprintln!("SUCCESS: multi-column read!");
}
