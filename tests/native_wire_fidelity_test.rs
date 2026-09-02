// Live integration tests for buffered/decompressed block framing fidelity.
//
// The compressed-materialized async path (parse_decompressed_block) and the
// sync buffered path (QueryStream -> parse_block) frame blocks by *skipping*
// column data. These tests exercise that framing against a real server for
// the wire layouts where it historically desynced: Array/Map fixed-width
// u64 offsets (including all-empty-array columns), materialized JSON with
// its 8-byte string-serialization version, and LowCardinality columns.
//
// Requires a ClickHouse native TCP server on 127.0.0.1:9000 (see
// tests/common/mod.rs for credentials).

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

/// Compressed-materialized reads must equal uncompressed reads for Array
/// columns whose rows mix values with empty arrays, plus a trailing column.
#[tokio::test]
#[cfg(feature = "lz4")]
async fn compressed_materialized_array_with_empty_rows_matches_plain() {
    let client = common::connect_client().await;
    let sql = "SELECT a, s FROM (SELECT [1, 2, 3] AS a, 'x' AS s UNION ALL SELECT cast([], 'Array(UInt8)') AS a, 'y' AS s UNION ALL SELECT [4] AS a, 'z' AS s) ORDER BY s";
    let expected: Vec<(Vec<u8>, String)> = client
        .query(sql)
        .all()
        .await
        .expect("plain all() over Array(UInt8)");

    for method in enabled_compression_methods() {
        let rows = client
            .query(sql)
            .with_compression(method)
            .all::<(Vec<u8>, String)>()
            .await
            .expect("compressed-materialized read must succeed");
        assert_eq!(rows, expected, "compressed {method:?} must match plain");
        assert_eq!(rows[1].0, Vec::<u8>::new(), "empty array row must decode");
    }
}

/// A column whose every array is empty has last offset 0 and a zero-row
/// inner column; the trailing column must still decode.
#[tokio::test]
#[cfg(feature = "lz4")]
async fn compressed_materialized_all_empty_arrays_column_matches_plain() {
    let client = common::connect_client().await;
    let sql =
        "SELECT cast([], 'Array(UInt8)') AS a, toUInt64(number) AS x FROM system.numbers LIMIT 3";
    let expected: Vec<(Vec<u8>, u64)> = client
        .query(sql)
        .all()
        .await
        .expect("plain all() over all-empty Array(UInt8)");

    for method in enabled_compression_methods() {
        let rows = client
            .query(sql)
            .with_compression(method)
            .all::<(Vec<u8>, u64)>()
            .await
            .expect("compressed-materialized read must succeed");
        assert_eq!(rows, expected, "compressed {method:?} must match plain");
        assert!(rows.iter().all(|(a, _)| a.is_empty()));
    }
}

/// Array(String) with empty strings exercises the string recursion.
#[tokio::test]
#[cfg(feature = "lz4")]
async fn compressed_materialized_array_string_matches_plain() {
    let client = common::connect_client().await;
    let sql = "SELECT ['a', '', 'bc'] AS a, 'tail' AS s";
    let expected: Vec<(Vec<String>, String)> = client
        .query(sql)
        .all()
        .await
        .expect("plain all() over Array(String)");

    for method in enabled_compression_methods() {
        let rows = client
            .query(sql)
            .with_compression(method)
            .all::<(Vec<String>, String)>()
            .await
            .expect("compressed-materialized read must succeed");
        assert_eq!(rows, expected, "compressed {method:?} must match plain");
    }
}

/// Map is Array(Tuple(K, V)): offsets first, then keys and values columns.
#[tokio::test]
#[cfg(feature = "lz4")]
async fn compressed_materialized_map_matches_plain() {
    let client = common::connect_client().await;
    let sql = "SELECT m, s FROM (SELECT map('k', toUInt8(7)) AS m, 'x' AS s UNION ALL SELECT cast(map(), 'Map(String, UInt8)') AS m, 'y' AS s) ORDER BY s";
    let expected: Vec<(Vec<(String, u8)>, String)> = client
        .query(sql)
        .all()
        .await
        .expect("plain all() over Map(String, UInt8)");

    for method in enabled_compression_methods() {
        let rows = client
            .query(sql)
            .with_compression(method)
            .all::<(Vec<(String, u8)>, String)>()
            .await
            .expect("compressed-materialized read must succeed");
        assert_eq!(rows, expected, "compressed {method:?} must match plain");
        assert!(rows[1].0.is_empty(), "empty map row must decode");
    }
}

/// Materialized JSON carries an 8-byte string-serialization version that the
/// compressed parser must consume as framing (not column data). Requires
/// native JSON type support (ClickHouse 25.x+; 24.8 rejects the cast).
#[tokio::test]
#[cfg(feature = "lz4")]
async fn compressed_materialized_json_matches_plain() {
    let client = common::connect_client().await;
    // Skip on servers without native JSON support (24.8: "Cannot create column")
    let version: Vec<(String,)> = client
        .query("SELECT version()")
        .all()
        .await
        .expect("read version");
    let major: u32 = version[0]
        .0
        .split('.')
        .next()
        .and_then(|m| m.parse().ok())
        .unwrap_or(0);
    if major < 25 {
        eprintln!("server lacks native JSON type (major {major}); skipping");
        return;
    }
    let sql = "SELECT cast('{\"x\":1}', 'JSON') AS j, 'tail' AS s";
    let expected: Vec<(st_clickhouse::column::JsonValue, String)> = client
        .query(sql)
        .with_setting(
            st_clickhouse::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING,
            "1",
        )
        .all()
        .await
        .expect("plain materialized JSON read");

    for method in enabled_compression_methods() {
        let rows = client
            .query(sql)
            .with_setting(
                st_clickhouse::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING,
                "1",
            )
            .with_compression(method)
            .all::<(st_clickhouse::column::JsonValue, String)>()
            .await
            .expect("compressed-materialized read must succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.as_str(), expected[0].0.as_str());
        assert_eq!(rows[0].1, "tail");
    }
}

/// LowCardinality columns are materialized on the compressed path with the
/// same layout the streaming reader produces.
#[tokio::test]
#[cfg(feature = "lz4")]
async fn compressed_materialized_lowcardinality_matches_plain() {
    let client = common::connect_client().await;
    let sql = "SELECT toLowCardinality('v') AS lc, 'tail' AS s";
    let expected: Vec<(String, String)> = client
        .query(sql)
        .all()
        .await
        .expect("plain all() over LowCardinality(String)");

    for method in enabled_compression_methods() {
        let rows = client
            .query(sql)
            .with_compression(method)
            .all::<(String, String)>()
            .await
            .expect("compressed-materialized read must succeed");
        assert_eq!(rows, expected, "compressed {method:?} must match plain");
    }
}

/// The sync buffered QueryStream path (parse_block) must frame Array/Map/
/// LowCardinality blocks, including the all-empty-arrays case whose last
/// offset is zero.
#[test]
fn sync_buffered_stream_frames_array_map_and_lowcardinality() {
    use st_clickhouse::sync::client::SyncClient;
    use st_clickhouse::sync::column::ClickHouseColumnData as _;
    use st_clickhouse::sync::config::ClientConfig;

    let mut config = ClientConfig::default()
        .with_host("127.0.0.1")
        .with_port(9000);
    if let (Ok(user), Ok(password)) = (
        std::env::var("CLICKHOUSE_USER"),
        std::env::var("CLICKHOUSE_PASSWORD"),
    ) {
        config = config.with_user(&user).with_password(&password);
    } else {
        config = config.with_user("default").with_password("test");
    }
    let Ok(mut client) = SyncClient::connect_with_config(config) else {
        eprintln!("ClickHouse test server is not available");
        return;
    };

    // Two rows of all-empty arrays: last offset 0 with rows > 1 previously
    // skipped one bogus inner byte and desynced the stream.
    {
        let mut stream = client
            .start_stream(
                "SELECT cast([], 'Array(UInt8)') AS a, toUInt64(number) AS x FROM system.numbers LIMIT 2",
            )
            .expect("start all-empty-array stream");
        let mut seen = 0usize;
        while let Some(block) = stream.read_next_block().expect("read block") {
            if block.row_count() == 0 {
                continue;
            }
            let arrays = block.column::<Vec<u8>>("a").expect("array column");
            let trailing = block.column::<u64>("x").expect("trailing column");
            for row in 0..block.row_count() {
                assert_eq!(arrays.get(row).expect("array row"), Vec::<u8>::new());
                assert_eq!(trailing.get(row).expect("trailing row"), seen as u64);
                seen += 1;
            }
        }
        assert_eq!(seen, 2, "stream must deliver both all-empty-array rows");
    }

    // Mixed arrays with a trailing column.
    {
        let mut stream = client
            .start_stream(
                "SELECT [toUInt8(1), toUInt8(2)] AS a, 'x' AS s UNION ALL SELECT cast([], 'Array(UInt8)') AS a, 'y' AS s ORDER BY s",
            )
            .expect("start mixed-array stream");
        let mut seen = 0usize;
        while let Some(block) = stream.read_next_block().expect("read block") {
            if block.row_count() == 0 {
                continue;
            }
            let arrays = block.column::<Vec<u8>>("a").expect("array column");
            seen += block.row_count();
            assert_eq!(arrays.len(), block.row_count());
        }
        assert_eq!(seen, 2);
    }

    // Map column with a trailing column.
    {
        let mut stream = client
            .start_stream("SELECT map('k', toUInt8(7)) AS m, 'tail' AS s")
            .expect("start map stream");
        let mut seen = 0usize;
        while let Some(block) = stream.read_next_block().expect("read block") {
            if block.row_count() == 0 {
                continue;
            }
            let maps = block.column::<Vec<(String, u8)>>("m").expect("map column");
            assert_eq!(maps.get(0).expect("map row"), vec![("k".to_string(), 7u8)]);
            seen += block.row_count();
        }
        assert_eq!(seen, 1);
    }

    // LowCardinality column: the zero-row header block must parse without
    // trying to read a 24-byte dictionary header that is not on the wire.
    {
        let mut stream = client
            .start_stream("SELECT toLowCardinality('v') AS lc, 'tail' AS s")
            .expect("start lc stream");
        let mut seen = 0usize;
        while let Some(block) = stream.read_next_block().expect("read block") {
            if block.row_count() == 0 {
                continue;
            }
            let lc = block.column::<String>("lc").expect("lc column");
            assert_eq!(lc.get(0).expect("lc row"), "v");
            seen += block.row_count();
        }
        assert_eq!(seen, 1);
    }
}

/// Round-trip through a real table: insert Array columns including empty
/// arrays, read back through the compressed-materialized path and the sync
/// buffered path, and require equality with the plain path.
#[tokio::test]
#[cfg(feature = "lz4")]
async fn array_round_trip_table_compressed_and_plain() {
    let client = common::connect_client().await;
    client
        .execute("DROP TABLE IF EXISTS wire_fidelity_arr")
        .await
        .expect("drop table");
    client
        .execute("CREATE TABLE wire_fidelity_arr (a Array(UInt8), b Array(String), s String) ENGINE = Memory")
        .await
        .expect("create table");
    client
        .execute(
            "INSERT INTO wire_fidelity_arr VALUES ([1, 2], ['x', ''], 'one'), ([], [], 'two'), ([3], ['y'], 'three')",
        )
        .await
        .expect("insert rows");

    let sql = "SELECT a, b, s FROM wire_fidelity_arr ORDER BY s";
    let expected: Vec<(Vec<u8>, Vec<String>, String)> =
        client.query(sql).all().await.expect("plain round trip");
    assert_eq!(expected.len(), 3);
    // ORDER BY s: 'one', 'three', 'two' — the last row is the all-empty one.
    assert_eq!(expected[2].0, Vec::<u8>::new());
    assert_eq!(expected[2].1, Vec::<String>::new());

    for method in enabled_compression_methods() {
        let rows = client
            .query(sql)
            .with_compression(method)
            .all::<(Vec<u8>, Vec<String>, String)>()
            .await
            .expect("compressed round trip must succeed");
        assert_eq!(rows, expected, "compressed {method:?} must match plain");
    }

    client
        .execute("DROP TABLE wire_fidelity_arr")
        .await
        .expect("cleanup table");
}

/// Nested JSON inside Array now decodes correctly under compression: the
/// multi-frame fix replaced the buffered compressed-block parser with the
/// streaming reader (the same code path uncompressed reads use), which
/// handles the nested string-serialization version the buffered parser
/// could not. The result must match the plain (uncompressed) query instead
/// of being rejected.
#[tokio::test]
#[cfg(feature = "lz4")]
async fn nested_json_array_compressed_matches_plain() {
    let client = common::connect_client().await;
    let sql = "SELECT [cast('{\"a\":1}','JSON')] AS j";
    let expected: Vec<(Vec<String>,)> = client.query(sql).all().await.expect("plain nested JSON");
    let compressed = client
        .query(sql)
        .with_compression(CompressionMethod::Lz4)
        .all::<(Vec<String>,)>()
        .await
        .expect("nested JSON under compression must decode via the streaming reader");
    assert_eq!(
        compressed, expected,
        "compressed nested JSON must match plain"
    );
}
