//! Live regression tests for multi-frame response compression (P0 fix).
//!
//! ClickHouse flushes its ~1 MiB `CompressedWriteBuffer` mid-packet, so any
//! Data packet whose serialized body exceeds ~1 MiB arrives as a *sequence*
//! of compression frames. Round 3 volume testing (target/r3-compression-*.log)
//! showed two defects:
//!
//! 1. ASYNC: the compressed reader consumed exactly ONE frame per Data
//!    packet, leaving the second frame's bytes in the stream -> "unexpected
//!    end of buffer skipping column data" or downstream desync. Deterministic
//!    at >= 15000 rows x ~73 B (the failing frame decompresses to exactly
//!    1,048,576 bytes).
//! 2. SYNC: the query packet set the compression flag but the read path
//!    never decompressed — ANY compressed SELECT failed.
//!
//! These tests pin the exact failing shapes: a 20000-row (forced
//! max_block_size=20000) single-block response spanning multiple frames, the
//! 15000-row natural boundary, and — for the sync client — the same shapes
//! through `query` and `start_stream`.
//!
//! Skips (early-return) when the server is unreachable. Server-free unit
//! tests for the two-frame wrapper live in `src/connection/block_reader.rs`
//! and `src/sync/compression/mod.rs`.

mod common;

use st_clickhouse::ClickHouseColumnData;
use st_clickhouse::compression::CompressionMethod;

/// The exact repro shape: `number` (UInt64, 8 B) + `repeat('x', 64)` (String:
/// 1 B length varint + 64 B) ≈ 73 B per row.
const VOLUME_SQL: &str = "SELECT number, repeat('x', 64) FROM system.numbers LIMIT ";

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

async fn server_reachable() -> bool {
    // Same credential resolution as connect_client: a reachable server with
    // non-default credentials must not look "unreachable".
    let addr = common::clickhouse_addr();
    let attempt = match (
        std::env::var("CLICKHOUSE_USER"),
        std::env::var("CLICKHOUSE_PASSWORD"),
    ) {
        (Ok(user), Ok(password)) => {
            st_clickhouse::Client::connect_with_credentials(addr, &user, &password).await
        },
        _ => match st_clickhouse::Client::connect(addr).await {
            Ok(client) => Ok(client),
            Err(_) => {
                st_clickhouse::Client::connect_with_credentials(addr, "default", "test").await
            },
        },
    };
    attempt.is_ok()
}

/// Map the crate-level method enum onto the sync client's own enum.
#[cfg(any(feature = "lz4", feature = "zstd"))]
fn sync_method(method: CompressionMethod) -> st_clickhouse::sync::compression::CompressionMethod {
    match method {
        CompressionMethod::Lz4 => st_clickhouse::sync::compression::CompressionMethod::Lz4,
        CompressionMethod::Zstd => st_clickhouse::sync::compression::CompressionMethod::Zstd,
        CompressionMethod::None => st_clickhouse::sync::compression::CompressionMethod::None,
    }
}

/// Collect `(number, string)` rows from a two-column block set.
fn collect_pairs(blocks: &[st_clickhouse::Block]) -> Vec<(u64, String)> {
    let mut rows = Vec::new();
    for block in blocks {
        if block.row_count() == 0 {
            continue; // header/trailing empty blocks carry no columns
        }
        let numbers = block.column::<u64>("number").expect("number column");
        let strings = block
            .column::<String>("repeat('x', 64)")
            .expect("string column");
        for i in 0..block.row_count() {
            rows.push((
                numbers.get(i).expect("number value"),
                strings.get(i).expect("string value"),
            ));
        }
    }
    rows
}

/// Collect `(number, string)` rows from sync blocks.
fn collect_sync_pairs(
    blocks: &[st_clickhouse::sync::protocol::block::Block],
) -> Vec<(u64, String)> {
    use st_clickhouse::sync::column::ClickHouseColumnData;
    let mut rows = Vec::new();
    for block in blocks {
        if block.row_count() == 0 {
            continue; // header/trailing empty blocks carry no columns
        }
        let numbers = block.column::<u64>("number").expect("number column");
        let strings = block
            .column::<String>("repeat('x', 64)")
            .expect("string column");
        for i in 0..block.row_count() {
            rows.push((
                numbers.get(i).expect("number value"),
                strings.get(i).expect("string value"),
            ));
        }
    }
    rows
}

// ── Async: the exact failing shapes ─────────────────────────────────────────

/// 20000 rows x 73 B with max_block_size forced to 20000: ONE Data packet
/// whose ~1.4 MiB body spans two frames. Failed deterministically before the
/// fix with "unexpected end of buffer skipping column data".
#[tokio::test]
#[cfg(any(feature = "lz4", feature = "zstd"))]
async fn async_20000_row_single_block_multiframe_matches_plain() {
    if !server_reachable().await {
        eprintln!("skipping: server unreachable");
        return;
    }
    let client = common::connect_client().await;
    let sql = format!("{VOLUME_SQL}20000 SETTINGS max_block_size = 20000");
    let expected = collect_pairs(&client.query(&sql).blocks().await.expect("plain baseline"));

    for method in enabled_compression_methods() {
        let blocks = client
            .query(&sql)
            .with_compression(method)
            .blocks()
            .await
            .expect("20000-row block query under {method:?} failed: {e}");
        let got = collect_pairs(&blocks);
        assert_eq!(got.len(), 20000, "row count under {method:?}");
        assert_eq!(got, expected, "rows must equal plain under {method:?}");

        // The connection must stay clean for the next query.
        let probe: u64 = client
            .query("SELECT toUInt64(9)")
            .scalar()
            .await
            .expect("connection reusable after multi-frame read");
        assert_eq!(probe, 9);
    }
}

/// The natural 15000-row boundary: default settings already produce a single
/// ~1.09 MiB block that spans two frames (>= 13500 rows fails without
/// max_block_size tuning; this is the smallest deterministic repro).
#[tokio::test]
#[cfg(any(feature = "lz4", feature = "zstd"))]
async fn async_15000_row_boundary_multiframe_matches_plain() {
    if !server_reachable().await {
        eprintln!("skipping: server unreachable");
        return;
    }
    let client = common::connect_client().await;
    let sql = format!("{VOLUME_SQL}15000");
    let expected = collect_pairs(&client.query(&sql).blocks().await.expect("plain baseline"));

    for method in enabled_compression_methods() {
        let got = collect_pairs(
            &client
                .query(&sql)
                .with_compression(method)
                .blocks()
                .await
                .expect("15000-row query under {method:?} failed: {e}"),
        );
        assert_eq!(got.len(), 15000, "row count under {method:?}");
        assert_eq!(got, expected, "rows must equal plain under {method:?}");
    }
}

/// The streaming cursor path (rows()) through the same multi-frame shape.
#[tokio::test]
#[cfg(any(feature = "lz4", feature = "zstd"))]
async fn async_rows_stream_20000_row_multiframe_matches_plain() {
    if !server_reachable().await {
        eprintln!("skipping: server unreachable");
        return;
    }
    let client = common::connect_client().await;
    let sql = format!("{VOLUME_SQL}20000 SETTINGS max_block_size = 20000");
    let expected: Vec<(u64, String)> = client.query(&sql).all().await.expect("plain all()");

    for method in enabled_compression_methods() {
        let cursor = client
            .query(&sql)
            .with_compression(method)
            .rows::<(u64, String)>()
            .await
            .expect("rows() under {method:?} failed: {e}");
        let got: Vec<(u64, String)> = cursor
            .collect()
            .await
            .expect("collect under {method:?}: {e}");
        assert_eq!(got.len(), 20000, "row count under {method:?}");
        assert_eq!(got, expected, "rows must equal plain under {method:?}");
    }
}

// ── Sync client: the compression flag now decompresses responses ───────────

mod sync_shapes {
    use super::*;
    use st_clickhouse::sync::client::SyncClient;
    use st_clickhouse::sync::config::ClientConfig;
    use std::time::Duration;

    fn connect(
        method: st_clickhouse::sync::compression::CompressionMethod,
    ) -> st_clickhouse::sync::Result<SyncClient> {
        let addr = std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "127.0.0.1:9000".to_owned());
        let (host, port) = addr
            .rsplit_once(':')
            .and_then(|(host, port)| port.parse::<u16>().ok().map(|port| (host.to_owned(), port)))
            .unwrap_or_else(|| ("127.0.0.1".to_owned(), 9000));
        let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_owned());
        let password = std::env::var("CLICKHOUSE_PASS").unwrap_or_else(|_| "test".to_owned());
        let config = ClientConfig::default()
            .with_host(&host)
            .with_port(port)
            .with_user(&user)
            .with_password(&password)
            .with_connect_timeout(Duration::from_secs(5))
            .with_query_timeout(Duration::from_secs(30))
            .with_compression(method);
        SyncClient::connect_with_config(config)
    }

    fn sync_reachable() -> bool {
        let addr = std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "127.0.0.1:9000".to_owned());
        std::net::TcpStream::connect(&addr).is_ok()
    }

    /// The headline sync repro: ANY compressed SELECT failed outright before
    /// the fix (the flag was set but the read path never decompressed), and
    /// a 20000-row query wedged until the server's 300 s read timeout.
    #[test]
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn sync_query_decompresses_selects_multiframe() {
        if !sync_reachable() {
            eprintln!("skipping: server unreachable");
            return;
        }
        for method in enabled_compression_methods() {
            let sync_method = sync_method(method);
            let mut client = connect(sync_method).expect("sync connect under {method:?}: {e}");

            // Small SELECT first — the pre-fix failure shape ("SELECT 1"
            // failed with a fill-buffer I/O error).
            let blocks = client
                .query("SELECT toUInt64(42) AS v")
                .expect("small SELECT under {method:?}: {e}");
            assert_eq!(blocks.iter().map(|b| b.row_count()).sum::<usize>(), 1);

            // The multi-frame shape: 20000 rows x 73 B, one forced block.
            let sql = format!("{VOLUME_SQL}20000 SETTINGS max_block_size = 20000");
            let blocks = client
                .query(&sql)
                .expect("20000-row sync query under {method:?}: {e}");
            let rows = collect_sync_pairs(&blocks);
            assert_eq!(rows.len(), 20000, "row count under {method:?}");
            assert_eq!(rows[0].0, 0);
            assert_eq!(rows[0].1, "x".repeat(64));
            assert_eq!(rows[19999].0, 19999);

            // The 15000-row natural boundary.
            let sql = format!("{VOLUME_SQL}15000");
            let rows = collect_sync_pairs(
                &client
                    .query(&sql)
                    .expect("15000-row sync query under {method:?}: {e}"),
            );
            assert_eq!(rows.len(), 15000, "boundary row count under {method:?}");

            // Connection stays usable afterwards.
            let blocks = client
                .query("SELECT toUInt64(7) AS v")
                .expect("connection reusable");
            assert_eq!(blocks.iter().map(|b| b.row_count()).sum::<usize>(), 1);
        }
    }

    /// QueryStream path: fill_buffer feeds parse_block from decompressed
    /// bytes through the per-packet wrapper.
    #[test]
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn sync_query_stream_decompresses_multiframe_selects() {
        if !sync_reachable() {
            eprintln!("skipping: server unreachable");
            return;
        }
        for method in enabled_compression_methods() {
            let sync_method = sync_method(method);
            let mut client = connect(sync_method).expect("sync connect under {method:?}: {e}");
            let sql = format!("{VOLUME_SQL}20000 SETTINGS max_block_size = 20000");
            let mut stream = client
                .start_stream(&sql)
                .expect("start_stream under {method:?}: {e}");
            let mut count = 0usize;
            let mut first = None;
            while let Some(block) = stream
                .read_next_block()
                .expect("read_next_block under {method:?}: {e}")
            {
                if first.is_none() && block.row_count() > 0 {
                    let numbers = block.column::<u64>("number").expect("number column");
                    first = Some(numbers.get(0).expect("first number value"));
                }
                count += block.row_count();
            }
            assert_eq!(count, 20000, "streamed row count under {method:?}");
            assert_eq!(first, Some(0));
            drop(stream);
            // The client must still work after the stream drained to EOS.
            let blocks = client.query("SELECT toUInt64(3) AS v").expect("reusable");
            assert_eq!(blocks.iter().map(|b| b.row_count()).sum::<usize>(), 1);
        }
    }

    /// Sync INSERT round-trip under compression: the table-structure Data packet
    /// arrives compressed (the query packet's flag is set), so this exercises the
    /// compressed `wait_for_insert_table_structure` read path plus the (already
    /// compressed) client→server data blocks.
    #[test]
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn sync_insert_roundtrip_under_compression() {
        if !sync_reachable() {
            eprintln!("skipping: server unreachable");
            return;
        }
        for method in enabled_compression_methods() {
            let sync_method = sync_method(method);
            let mut client =
                connect(sync_method).expect("sync connect under compression for insert");
            client
                .execute("DROP TABLE IF EXISTS st_multi_comp_sync")
                .expect("drop table");
            client
                .execute("CREATE TABLE st_multi_comp_sync (id UInt64, s String) ENGINE = Memory")
                .expect("create table");

            // INSERT via VALUES goes through the same compressed query packet and
            // compressed table-structure read.
            client
                .execute("INSERT INTO st_multi_comp_sync VALUES (1, 'one'), (2, 'two')")
                .expect("compressed insert");

            let rows = collect_sync_pairs(
            &client
                .query("SELECT id AS number, s AS \"repeat('x', 64)\" FROM st_multi_comp_sync ORDER BY id")
                .expect("read back"),
        );
            assert_eq!(rows, vec![(1, "one".to_owned()), (2, "two".to_owned())]);

            client
                .execute("DROP TABLE st_multi_comp_sync")
                .expect("cleanup");
        }
    }

    /// Block-INSERT (begin/send_data/end) under compression: the trailing empty
    /// Data block must be compressed like the query packet, and the connection
    /// must stay usable afterwards (ping + follow-up query). Pre-fix this wedged
    /// the server and desynced the connection.
    #[test]
    fn sync_block_insert_roundtrip_under_compression() {
        for method in enabled_compression_methods() {
            let sync_method = sync_method(method);
            let mut client = connect(sync_method).expect("sync connect");
            client
                .execute("DROP TABLE IF EXISTS st_multi_comp_blockins")
                .expect("drop table");
            client
                .execute("CREATE TABLE st_multi_comp_blockins (id UInt64) ENGINE = Memory")
                .expect("create table");

            let id_data: Vec<u8> = 42u64.to_le_bytes().to_vec();
            let block = st_clickhouse::sync::Block {
                columns: vec![st_clickhouse::sync::ColumnInfo {
                    name: "id".into(),
                    type_name: "UInt64".into(),
                    data: bytes::Bytes::from(id_data),
                    lc_materialized: bytes::Bytes::new(),
                }],
                rows: 1,
            };
            client
                .insert(
                    "INSERT INTO st_multi_comp_blockins (id) VALUES",
                    "st_multi_comp_blockins",
                    &[block],
                )
                .expect("block insert under compression");

            assert!(
                client.ping().is_ok(),
                "connection must survive block insert"
            );
            let rows: usize = client
                .query("SELECT id AS number FROM st_multi_comp_blockins")
                .expect("follow-up query")
                .iter()
                .map(|b| b.row_count())
                .sum();
            assert_eq!(rows, 1);

            client
                .execute("DROP TABLE st_multi_comp_blockins")
                .expect("cleanup");
        }
    }

    /// Parameterized queries under compression: the params branch of the query
    /// packet must compress its trailing empty Data block too.
    #[test]
    fn sync_params_under_compression() {
        use st_clickhouse::sync::QueryParameter;
        for method in enabled_compression_methods() {
            let mut client = connect(sync_method(method)).expect("sync connect");
            let rows: usize = client
                .query_with_params(
                    "SELECT {n:UInt64} AS number",
                    &[QueryParameter::new("n", "7")],
                )
                .expect("parameterized query under compression")
                .iter()
                .map(|b| b.row_count())
                .sum();
            assert_eq!(rows, 1);
            assert!(client.ping().is_ok());
        }
    }
}
