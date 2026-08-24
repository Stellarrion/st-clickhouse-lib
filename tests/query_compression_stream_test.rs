// Live integration tests for negotiated response compression on the
// streaming read paths (`QueryBuilder::rows`, `Client::begin_select`) and
// for multi-block result semantics (`blocks()` / `block()`).

mod common;

use st_clickhouse::compression::CompressionMethod;

#[cfg(any(feature = "lz4", feature = "zstd"))]
fn enabled_compression_methods() -> impl Iterator<Item = CompressionMethod> {
    [
        #[cfg(feature = "lz4")]
        CompressionMethod::Lz4,
        #[cfg(feature = "zstd")]
        CompressionMethod::Zstd,
    ]
    .into_iter()
}

#[tokio::test]
#[cfg(any(feature = "lz4", feature = "zstd"))]
async fn rows_matches_all_under_enabled_compression() {
    let client = common::connect_client_pool(1).await;
    let sql = "SELECT number FROM system.numbers LIMIT 5000";
    let expected: Vec<(u64,)> = client.query(sql).all().await.expect("plain all()");

    for method in enabled_compression_methods() {
        let cursor = client
            .query(sql)
            .with_compression(method)
            .rows::<(u64,)>()
            .await
            .expect("rows() with enabled compression");
        let streamed = cursor.collect().await.expect("collect compressed rows");
        assert_eq!(
            streamed, expected,
            "rows() must match all() under {method:?}"
        );
        // The one-slot pool must keep serving queries after the stream.
        let probe: u64 = client
            .query("SELECT toUInt64(7)")
            .scalar()
            .await
            .expect("pool must stay usable after rows()");
        assert_eq!(probe, 7);
    }
}

#[tokio::test]
#[cfg(feature = "lz4")]
async fn rows_cancel_mid_stream_keeps_pool_usable() {
    let client = common::connect_client_pool(1).await;
    {
        let mut cursor = client
            .query("SELECT number FROM system.numbers LIMIT 1000000")
            .with_compression(CompressionMethod::Lz4)
            .rows::<(u64,)>()
            .await
            .expect("rows() with LZ4");
        let first = cursor
            .next()
            .await
            .expect("next()")
            .expect("at least one row");
        assert_eq!(first.0, 0);
    } // drop → Cancel is sent server-side
    let probe: u64 = client
        .query("SELECT toUInt64(3)")
        .scalar()
        .await
        .expect("pool must stay usable after cursor drop");
    assert_eq!(probe, 3);
}

#[tokio::test]
#[cfg(any(feature = "lz4", feature = "zstd"))]
async fn begin_select_with_compression_streams_blocks_and_reuses_connection() {
    for method in enabled_compression_methods() {
        let client = common::connect_client_pool(1)
            .await
            .with_compression(method);
        let mut stream = client
            .begin_select("SELECT number FROM system.numbers LIMIT 3000")
            .await
            .expect("begin_select with enabled compression");

        let mut total = 0usize;
        let mut first_value = None;
        while let Some(block) = stream.next_block().await.expect("next compressed block") {
            let col = block.column::<u64>("number").expect("number column");
            if first_value.is_none() {
                first_value = Some(col.get(0).expect("value"));
            }
            total += block.row_count();
        }
        assert_eq!(total, 3000, "all rows must arrive under {method:?}");
        assert_eq!(
            first_value,
            Some(0),
            "values must decode correctly under {method:?}"
        );
        drop(stream);

        // Same pooled connection (one slot) must serve the next query cleanly.
        let probe: u64 = client
            .query("SELECT toUInt64(11)")
            .scalar()
            .await
            .expect("connection reusable after compressed drain");
        assert_eq!(probe, 11);
    }
}

#[tokio::test]
#[cfg(feature = "lz4")]
async fn begin_select_cancel_mid_stream_keeps_connection_usable() {
    let client = common::connect_client_pool(1)
        .await
        .with_compression(CompressionMethod::Lz4);
    let mut stream = client
        .begin_select("SELECT number FROM system.numbers")
        .await
        .expect("begin_select with LZ4");
    let block = stream
        .next_block()
        .await
        .expect("next_block")
        .expect("first block");
    let col = block.column::<u64>("number").expect("number column");
    assert_eq!(col.get(0).expect("value"), 0);

    // Cancel must drain the compressed response without desyncing the wire.
    stream.cancel().await.expect("cancel");
    drop(stream); // releases the one-slot pool guard

    let probe: u64 = client
        .query("SELECT toUInt64(5)")
        .scalar()
        .await
        .expect("connection must stay usable after compressed cancel/drain");
    assert_eq!(probe, 5);
}

#[tokio::test]
async fn blocks_returns_multiple_blocks_with_boundaries() {
    let client = common::connect_client().await;
    let blocks = client
        .query("SELECT number FROM system.numbers LIMIT 2500")
        .with_setting("max_block_size", "1000")
        .blocks()
        .await
        .expect("blocks()");
    assert!(
        blocks.len() >= 2,
        "expected multiple blocks with max_block_size=1000, got {}",
        blocks.len()
    );
    assert!(
        blocks.iter().all(|b| b.row_count() <= 1000),
        "server block boundaries must be preserved"
    );

    let mut next = 0u64;
    for block in &blocks {
        let col = block.column::<u64>("number").expect("number column");
        for i in 0..col.len() {
            assert_eq!(col.get(i).expect("value"), next);
            next += 1;
        }
    }
    assert_eq!(next, 2500, "no rows may be dropped between blocks");
}

#[tokio::test]
#[cfg(feature = "zstd")]
async fn blocks_under_compression_matches_plain() {
    let client = common::connect_client().await;
    let plain = client
        .query("SELECT number FROM system.numbers LIMIT 2500")
        .with_setting("max_block_size", "1000")
        .blocks()
        .await
        .expect("plain blocks()");
    let compressed = client
        .query("SELECT number FROM system.numbers LIMIT 2500")
        .with_setting("max_block_size", "1000")
        .with_compression(CompressionMethod::Zstd)
        .blocks()
        .await
        .expect("blocks() with Zstd");

    let shape =
        |blocks: &[st_clickhouse::Block]| blocks.iter().map(|b| b.row_count()).collect::<Vec<_>>();
    assert_eq!(
        shape(&plain),
        shape(&compressed),
        "same block split expected"
    );
    assert_eq!(
        compressed.iter().map(|b| b.row_count()).sum::<usize>(),
        2500
    );
}

#[tokio::test]
async fn block_errors_on_multi_block_response_and_stays_usable() {
    let client = common::connect_client_pool(1).await;
    let multi = client
        .query("SELECT number FROM system.numbers LIMIT 2500")
        .with_setting("max_block_size", "1000");
    let err = multi
        .block()
        .await
        .err()
        .expect("multi-block query must error on .block(), not truncate");
    assert!(
        err.to_string().contains("multiple non-empty data blocks"),
        "unexpected error: {err}"
    );

    // The failed read still drained the response: the connection stays clean.
    let probe: u64 = client
        .query("SELECT toUInt64(2)")
        .scalar()
        .await
        .expect("connection must stay usable after the multi-block error");
    assert_eq!(probe, 2);

    // fetch::<Block>() keeps exact-one-block semantics on single-block results.
    let single = client
        .query("SELECT toUInt64(42) AS v")
        .fetch::<st_clickhouse::Block>()
        .await
        .expect("fetch::<Block>() on a single-block result");
    assert_eq!(single.row_count(), 1);
    let col = single.column::<u64>("v").expect("v column");
    assert_eq!(col.get(0).expect("value"), 42);
}
