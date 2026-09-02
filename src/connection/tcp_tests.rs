use crate::compression::CompressionMethod;
use crate::connection::block_reader::read_column_async;
use crate::connection::callbacks::QueryCallbacks;
use crate::connection::query_packet::{
    build_query_packet, build_query_packet_from_template, build_query_packet_template,
};
use crate::connection::raw_block_reader::read_column_raw_recorded;
use crate::connection::tcp::Client;
use crate::protocol::parameters::QueryParameter;
use crate::protocol::revision;
use crate::protocol::wire;
use crate::query_id::next_query_id;
use crate::runtime::io::AsyncWriteExt;
use crate::runtime::sync::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn query_template_matches_dynamic_packet_builder() {
    let mut settings = HashMap::new();
    settings.insert("max_block_size".to_string(), "1024".to_string());
    let compression = Some(CompressionMethod::Lz4);

    let template = build_query_packet_template(
        &settings,
        compression,
        revision::DEFAULT_PROTOCOL_REVISION,
        "",
    );
    let templated = build_query_packet_from_template(&template, "SELECT 1", b"", true, &[]);
    let dynamic = build_query_packet(&template, "SELECT 1", &[], b"", &[]);

    assert_eq!(templated, dynamic);
}

#[test]
fn query_packet_defaults_json_to_string_serialization() {
    let settings = HashMap::new();
    let template =
        build_query_packet_template(&settings, None, revision::DEFAULT_PROTOCOL_REVISION, "");
    let packet = build_query_packet(&template, "SELECT 1", &[], b"", &[]);
    let setting_name =
        crate::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING.as_bytes();

    assert!(
        packet
            .windows(setting_name.len())
            .any(|window| window == setting_name)
    );
}

#[test]
fn query_packet_serializes_server_side_parameters() {
    let settings = HashMap::new();
    let query = "SELECT {id:UInt64}, {name:String}";
    let template =
        build_query_packet_template(&settings, None, revision::DEFAULT_PROTOCOL_REVISION, "");
    let packet = build_query_packet(
        &template,
        query,
        &[],
        b"",
        &[
            QueryParameter::new("id", "42"),
            QueryParameter::new("name", "O'Reilly"),
        ],
    );
    let query_start = packet
        .windows(query.len())
        .position(|window| window == query.as_bytes())
        .expect("query text in packet");
    let mut rd = &packet[query_start + query.len()..];

    assert_eq!(wire::read_string(&mut rd).expect("param name"), "id");
    assert_eq!(wire::read_varint(&mut rd).expect("param flag"), 2);
    assert_eq!(wire::read_string(&mut rd).expect("param value"), "'42'");
    assert_eq!(wire::read_string(&mut rd).expect("param name"), "name");
    assert_eq!(wire::read_varint(&mut rd).expect("param flag"), 2);
    assert_eq!(
        wire::read_string(&mut rd).expect("param value"),
        r"'O\x27Reilly'"
    );
    assert_eq!(wire::read_string(&mut rd).expect("params terminator"), "");
    assert_eq!(wire::read_varint(&mut rd).expect("data packet"), 2);
}

#[test]
fn explicit_json_serialization_setting_is_not_duplicated() {
    let mut settings = HashMap::new();
    settings.insert(
        crate::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING.to_string(),
        "0".to_string(),
    );
    let template =
        build_query_packet_template(&settings, None, revision::DEFAULT_PROTOCOL_REVISION, "");
    let packet = build_query_packet(&template, "SELECT 1", &[], b"", &[]);
    let setting_name =
        crate::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING.as_bytes();
    let occurrences = packet
        .windows(setting_name.len())
        .filter(|window| *window == setting_name)
        .count();

    assert_eq!(occurrences, 1);
}

async fn capture_raw_column(type_name: &str, rows: usize, wire_data: &[u8]) -> Vec<u8> {
    let (mut writer, mut reader) = crate::runtime::io::duplex(wire_data.len().max(64));
    writer
        .write_all(wire_data)
        .await
        .expect("test operation failed");
    drop(writer);

    let mut out = Vec::new();
    let mut budget = crate::limits::MAX_COLUMN_BYTES;
    read_column_raw_recorded(&mut reader, type_name, rows, &mut out, &mut budget)
        .await
        .expect("test operation failed");
    out
}

#[tokio::test]
async fn materialized_json_string_serialization_strips_version_prefix() {
    let mut wire_data = Vec::new();
    wire_data.extend_from_slice(&4u64.to_le_bytes());
    wire::write_string_to_vec(&mut wire_data, r#"{"x":1}"#);
    wire::write_string_to_vec(&mut wire_data, r#"{"x":2}"#);

    let (mut writer, mut reader) = crate::runtime::io::duplex(wire_data.len().max(64));
    writer
        .write_all(&wire_data)
        .await
        .expect("test operation failed");
    drop(writer);

    let out = read_column_async(&mut reader, "JSON", 2)
        .await
        .expect("test operation failed");
    let mut expected = Vec::new();
    wire::write_string_to_vec(&mut expected, r#"{"x":1}"#);
    wire::write_string_to_vec(&mut expected, r#"{"x":2}"#);

    assert_eq!(out, expected);
}

#[tokio::test]
async fn raw_capture_records_json_string_bytes() {
    let mut wire_data = Vec::new();
    wire_data.extend_from_slice(&4u64.to_le_bytes());
    wire::write_string_to_vec(&mut wire_data, r#"{"x":1}"#);
    wire::write_string_to_vec(&mut wire_data, r#"{"x":2}"#);

    let out = capture_raw_column("JSON", 2, &wire_data).await;

    assert_eq!(out, wire_data);
}

#[tokio::test]
async fn raw_capture_records_json_flattened_bytes() {
    let mut wire_data = Vec::new();
    wire_data.extend_from_slice(&0u64.to_le_bytes());
    wire::write_varint_to_vec(&mut wire_data, 1);
    wire::write_varint_to_vec(&mut wire_data, 1);
    wire::write_string_to_vec(&mut wire_data, "x");
    wire_data.extend_from_slice(&1u64.to_le_bytes());
    wire::write_varint_to_vec(&mut wire_data, 1);
    wire::write_varint_to_vec(&mut wire_data, 1);
    wire::write_string_to_vec(&mut wire_data, "Int64");
    wire_data.extend_from_slice(&0u64.to_le_bytes());
    wire_data.push(0);
    wire_data.extend_from_slice(&1i64.to_le_bytes());
    wire_data.extend_from_slice(&0u64.to_le_bytes());

    let out = capture_raw_column("JSON", 1, &wire_data).await;

    assert_eq!(out, wire_data);
}

#[tokio::test]
async fn raw_capture_records_json_v3_flattened_bytes() {
    let mut wire_data = Vec::new();
    wire_data.extend_from_slice(&3u64.to_le_bytes());
    wire::write_varint_to_vec(&mut wire_data, 1);
    wire::write_string_to_vec(&mut wire_data, "x");
    wire_data.extend_from_slice(&3u64.to_le_bytes());
    wire::write_varint_to_vec(&mut wire_data, 1);
    wire::write_string_to_vec(&mut wire_data, "Int64");
    wire_data.push(0);
    wire_data.extend_from_slice(&1i64.to_le_bytes());

    let out = capture_raw_column("JSON", 1, &wire_data).await;

    assert_eq!(out, wire_data);
}

#[tokio::test]
async fn raw_capture_records_json_v3_headers_before_bodies() {
    let mut wire_data = Vec::new();
    wire_data.extend_from_slice(&3u64.to_le_bytes());
    wire::write_varint_to_vec(&mut wire_data, 2);
    wire::write_string_to_vec(&mut wire_data, "a");
    wire::write_string_to_vec(&mut wire_data, "b");

    wire_data.extend_from_slice(&3u64.to_le_bytes());
    wire::write_varint_to_vec(&mut wire_data, 1);
    wire::write_string_to_vec(&mut wire_data, "UInt8");
    wire_data.extend_from_slice(&3u64.to_le_bytes());
    wire::write_varint_to_vec(&mut wire_data, 1);
    wire::write_string_to_vec(&mut wire_data, "String");

    wire_data.push(0);
    wire_data.push(7);
    wire_data.push(0);
    wire::write_string_to_vec(&mut wire_data, "seven");

    let out = capture_raw_column("JSON", 1, &wire_data).await;

    assert_eq!(out, wire_data);
}

#[tokio::test]
async fn raw_capture_records_dynamic_variant_bytes() {
    let mut wire_data = Vec::new();
    wire_data.extend_from_slice(&3u64.to_le_bytes());
    wire::write_varint_to_vec(&mut wire_data, 2);
    wire::write_string_to_vec(&mut wire_data, "UInt8");
    wire::write_string_to_vec(&mut wire_data, "String");
    wire_data.extend_from_slice(&[0, 1]);
    wire_data.push(7);
    wire::write_string_to_vec(&mut wire_data, "abc");

    let out = capture_raw_column("Dynamic", 2, &wire_data).await;

    assert_eq!(out, wire_data);
}

#[tokio::test]
async fn raw_capture_records_variant_basic_and_compact_bytes() {
    let mut basic = Vec::new();
    basic.extend_from_slice(&0u64.to_le_bytes());
    basic.extend_from_slice(&[0, 1]);
    basic.push(7);
    wire::write_string_to_vec(&mut basic, "abc");

    let basic_out = capture_raw_column("Variant(UInt8, String)", 2, &basic).await;
    assert_eq!(basic_out, basic);

    let mut compact = Vec::new();
    compact.extend_from_slice(&1u64.to_le_bytes());
    compact.extend_from_slice(&1u64.to_le_bytes());
    compact.extend_from_slice(&2u64.to_le_bytes());
    wire::write_string_to_vec(&mut compact, "abc");
    wire::write_string_to_vec(&mut compact, "def");

    let compact_out = capture_raw_column("Variant(UInt8, String)", 2, &compact).await;
    assert_eq!(compact_out, compact);
}

#[test]
fn test_client_builder_with_settings() {
    let client = test_client()
        .with_setting("max_block_size", "1024")
        .with_compression(CompressionMethod::Lz4)
        .with_ping_before_query(true)
        .with_send_retries(3)
        .with_retry_timeout(Duration::from_secs(10))
        .with_connect_timeout(Duration::from_secs(5))
        .with_send_timeout(Duration::from_secs(30))
        .with_recv_timeout(Duration::from_secs(120));
    // Only check builder methods that don't need a running server
    assert_eq!(
        client.settings.get("max_block_size").map(|s| s.as_str()),
        Some("1024")
    );
    assert_eq!(client.compression, Some(CompressionMethod::Lz4));
    assert!(client.ping_before_query);
    assert_eq!(client.send_retries, 3);
    assert_eq!(client.retry_timeout, Duration::from_secs(10));
    assert_eq!(client.connect_timeout, Duration::from_secs(5));
    assert_eq!(client.recv_timeout, Duration::from_secs(120));
}

#[test]
fn test_client_builder_send_retries_minimum_one() {
    let client = test_client().with_send_retries(0);
    assert_eq!(client.send_retries, 1);
    let client = test_client().with_send_retries(1);
    assert_eq!(client.send_retries, 1);
    let client = test_client().with_send_retries(5);
    assert_eq!(client.send_retries, 5);
    let client = test_client().with_send_retries(u32::MAX);
    assert_eq!(client.send_retries, u32::MAX);
}

#[test]
fn test_error_is_retryable_io() {
    use crate::error::Error;
    assert!(
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "conn refused"
        ))
        .is_retryable()
    );
    assert!(Error::Timeout("timed out".into()).is_retryable());
    assert!(Error::ConnectionClosed("closed".into()).is_retryable());
    assert!(!Error::Protocol("protocol err".into()).is_retryable());
}

#[test]
fn test_error_is_not_retryable() {
    use crate::error::Error;
    assert!(!Error::Authentication("bad auth".into()).is_retryable());
    assert!(!Error::Config("bad config".into()).is_retryable());
    assert!(
        !Error::ServerError {
            code: 1,
            name: "Exception".into(),
            message: "server exception".into(),
        }
        .is_retryable()
    );
}

#[test]
fn test_next_query_id_format() {
    let mut buf = [0u8; 22];
    let len1 = next_query_id(&mut buf);
    let id1 = std::str::from_utf8(&buf[..len1])
        .expect("query id is ASCII")
        .to_owned();
    let len2 = next_query_id(&mut buf);
    let id2 = std::str::from_utf8(&buf[..len2])
        .expect("query id is ASCII")
        .to_owned();
    assert!(id1.starts_with("st-ch-"));
    assert!(id2.starts_with("st-ch-"));
    // IDs should be different
    assert_ne!(id1, id2);
    // ID suffix should be hex-only (0-9, a-f)
    for c in id1.trim_start_matches("st-ch-").chars() {
        assert!(c.is_ascii_hexdigit(), "query ID char '{c}' not hex");
    }
}

#[test]
fn test_build_query_packet_template_returns_valid_structure() {
    let mut settings = HashMap::new();
    settings.insert("max_threads".to_string(), "4".to_string());
    let template =
        build_query_packet_template(&settings, None, revision::DEFAULT_PROTOCOL_REVISION, "");
    let packet = build_query_packet_from_template(&template, "SELECT 1", b"test-id", true, &[]);
    // Should contain the query text
    assert!(packet.windows(8).any(|w| w == b"SELECT 1"));
    // Should contain the query_id
    assert!(packet.windows(7).any(|w| w == b"test-id"));
    // Should contain the setting
    let setting_bytes = b"max_threads";
    assert!(
        packet
            .windows(setting_bytes.len())
            .any(|w| w == setting_bytes)
    );
}

#[test]
fn test_build_query_packet_with_empty_query() {
    let settings = HashMap::new();
    let template =
        build_query_packet_template(&settings, None, revision::DEFAULT_PROTOCOL_REVISION, "");
    let packet = build_query_packet(&template, "", &[], b"", &[]);
    assert!(!packet.is_empty());
    assert!(packet.contains(&2)); // trailing empty Data packet
}

#[test]
fn test_query_kind_detection_create() {
    let settings = HashMap::new();
    let template =
        build_query_packet_template(&settings, None, revision::DEFAULT_PROTOCOL_REVISION, "");
    let pkt = build_query_packet(
        &template,
        "CREATE TABLE foo (x UInt64) ENGINE = Memory",
        &[],
        b"",
        &[],
    );
    assert!(!pkt.is_empty());
}

fn test_client() -> Client {
    let addr = "127.0.0.1:9000".parse().expect("test address should parse");
    Client {
        pool: crate::pool::SimplePool::new(vec![addr], 1),
        settings: HashMap::new(),
        query_template: build_query_packet_template(
            &HashMap::new(),
            None,
            revision::DEFAULT_PROTOCOL_REVISION,
            "",
        ),
        compression: None,
        ping_before_query: false,
        callbacks: QueryCallbacks::default(),
        send_retries: 1,
        retry_timeout: Duration::from_secs(5),
        connect_timeout: Duration::from_secs(30),
        recv_timeout: Duration::from_secs(300),
        query_timeout: None,
        schema_cache: Arc::new(RwLock::new(HashMap::new())),
        validate_schema: false,
        max_response_size: crate::limits::DEFAULT_MAX_RESPONSE_SIZE,
    }
}

// ---------------------------------------------------------------------------
// StreamWrapper raw-framing read buffer
// ---------------------------------------------------------------------------

async fn spawn_server(
    payload: Vec<u8>, trickle: Option<std::time::Duration>,
) -> std::net::SocketAddr {
    use crate::runtime::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        if let Some(delay) = trickle {
            for b in &payload {
                let _ = sock.write_all(&[*b]).await;
                tokio::time::sleep(delay).await;
            }
        } else {
            let _ = sock.write_all(&payload).await;
        }
        // hold the socket briefly so reads don't race ahead of writes
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    });
    addr
}

#[tokio::test]
async fn stream_wrapper_serves_bytes_across_tiny_reads() {
    use crate::runtime::io::AsyncReadExt;
    let payload: Vec<u8> = (0..300u32).flat_map(|v| v.to_le_bytes()).collect();
    let addr = spawn_server(payload.clone(), None).await;
    let tcp = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let mut sw = crate::pool::StreamWrapper::tcp(tcp);
    let mut got = vec![0u8; payload.len()];
    for i in 0..payload.len() {
        sw.read_exact(&mut got[i..i + 1]).await.expect("read byte");
    }
    assert_eq!(got, payload);
}

#[tokio::test]
async fn stream_wrapper_read_exact_spans_refills() {
    use crate::runtime::io::AsyncReadExt;
    // Larger than the 8 KiB prefetch buffer → exercises multiple refills.
    let payload = vec![0xA5u8; 50_000];
    let addr = spawn_server(payload.clone(), None).await;
    let tcp = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let mut sw = crate::pool::StreamWrapper::tcp(tcp);
    let mut got = vec![0u8; payload.len()];
    sw.read_exact(&mut got).await.expect("read");
    assert_eq!(got, payload);
}

#[tokio::test]
async fn stream_wrapper_handles_trickled_writes() {
    // Server writes one byte at a time → client refills return partial data and
    // Pending, exercising the refill state machine. A state bug (committing
    // rd_pos before a successful read) would re-serve consumed bytes here.
    use crate::runtime::io::AsyncReadExt;
    let payload: Vec<u8> = (0..100u8).collect();
    let addr = spawn_server(payload.clone(), Some(std::time::Duration::from_millis(2))).await;
    let tcp = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let mut sw = crate::pool::StreamWrapper::tcp(tcp);
    let mut got = vec![0u8; payload.len()];
    sw.read_exact(&mut got).await.expect("read");
    assert_eq!(got, payload);
}

#[tokio::test]
async fn stream_wrapper_propagates_eof() {
    use crate::runtime::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        let _ = sock.write_all(&[1, 2, 3]).await;
        let _ = sock.shutdown().await;
    });
    let tcp = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let mut sw = crate::pool::StreamWrapper::tcp(tcp);
    let mut got = [0u8; 3];
    sw.read_exact(&mut got).await.expect("read 3");
    assert_eq!(&got, &[1, 2, 3]);
    // After EOF the next read must error, not re-serve consumed bytes.
    let err = sw
        .read_exact(&mut [0u8; 1])
        .await
        .expect_err("expected EOF");
    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
}

// ---------------------------------------------------------------------------
// Fake select-response payloads (deterministic, server-free handler tests)
// ---------------------------------------------------------------------------

/// Body of one data block (BlockInfo + header + a single `UInt64` column).
fn u64_block_body(vals: &[u64]) -> Vec<u8> {
    let mut b = Vec::new();
    // BlockInfo: field 1 (is_overflows), field 2 (bucket_num), terminator.
    wire::write_varint_to_vec(&mut b, 1);
    b.push(0);
    wire::write_varint_to_vec(&mut b, 2);
    b.extend_from_slice(&(-1i32).to_le_bytes());
    wire::write_varint_to_vec(&mut b, 0);
    wire::write_varint_to_vec(&mut b, 1); // num_columns
    wire::write_varint_to_vec(&mut b, vals.len() as u64);
    wire::write_string_to_vec(&mut b, "v");
    wire::write_string_to_vec(&mut b, "UInt64");
    b.push(0); // custom serialization = none
    for val in vals {
        b.extend_from_slice(&val.to_le_bytes());
    }
    b
}

/// Uncompressed server `Data` packet carrying one UInt64 column.
fn data_packet(vals: &[u64]) -> Vec<u8> {
    let mut p = Vec::new();
    wire::write_varint_to_vec(&mut p, crate::protocol::packet::ServerPacket::Data as u64);
    wire::write_string_to_vec(&mut p, ""); // table name — never compressed
    p.extend_from_slice(&u64_block_body(vals));
    p
}

/// LZ4/ZSTD-framed server `Data` packet (table name outside the frame).
#[cfg(any(feature = "lz4", feature = "zstd"))]
fn compressed_data_packet(vals: &[u64], method: CompressionMethod) -> Vec<u8> {
    let body = u64_block_body(vals);
    let frame = crate::compression::encode_frame(&body, method).expect("test operation failed");
    let mut p = Vec::new();
    wire::write_varint_to_vec(&mut p, crate::protocol::packet::ServerPacket::Data as u64);
    wire::write_string_to_vec(&mut p, "");
    p.extend_from_slice(&frame);
    p
}

/// ProfileEvents packet; framed like a Data packet. When response compression
/// is negotiated the body arrives in a compression frame.
fn profile_events_packet(vals: &[u64], method: Option<CompressionMethod>) -> Vec<u8> {
    let mut p = Vec::new();
    wire::write_varint_to_vec(
        &mut p,
        crate::protocol::packet::ServerPacket::ProfileEvents as u64,
    );
    wire::write_string_to_vec(&mut p, "");
    match method {
        Some(m) => {
            let body = u64_block_body(vals);
            let frame = crate::compression::encode_frame(&body, m).expect("test operation failed");
            p.extend_from_slice(&frame);
        },
        None => p.extend_from_slice(&u64_block_body(vals)),
    }
    p
}

fn end_of_stream_packet() -> Vec<u8> {
    let mut p = Vec::new();
    wire::write_varint_to_vec(
        &mut p,
        crate::protocol::packet::ServerPacket::EndOfStream as u64,
    );
    p
}

async fn fake_stream(payload: Vec<u8>) -> crate::pool::StreamWrapper {
    let addr = spawn_server(payload, None).await;
    let tcp = tokio::net::TcpStream::connect(addr).await.expect("connect");
    crate::pool::StreamWrapper::tcp(tcp)
}

fn block_values(block: &crate::protocol::block::Block) -> Vec<u64> {
    (0..block.row_count())
        .map(|i| {
            block
                .column::<u64>("v")
                .expect("test operation failed")
                .get(i)
                .expect("test operation failed")
        })
        .collect()
}

#[tokio::test]
async fn first_block_handler_errors_on_second_non_empty_block() {
    use crate::connection::select_response::{FirstBlockHandler, read_select_response};
    // A second non-empty Data block must be an error — never a silent
    // truncation. The trailing third block proves the response is still
    // drained to EndOfStream (the connection stays clean) before failing.
    let mut payload = data_packet(&[1]);
    payload.extend_from_slice(&data_packet(&[2, 3]));
    payload.extend_from_slice(&data_packet(&[4, 5, 6]));
    payload.extend_from_slice(&end_of_stream_packet());

    let mut stream = fake_stream(payload).await;
    let result = read_select_response(
        &mut stream,
        Duration::from_secs(5),
        None,
        false,
        &QueryCallbacks::default(),
        FirstBlockHandler::new(usize::MAX),
    )
    .await;
    let err = result
        .err()
        .expect("second non-empty block must error, not truncate");
    assert!(
        err.to_string().contains("multiple non-empty data blocks"),
        "unexpected error: {err}"
    );
    // Everything up to EndOfStream was consumed — the next read hits EOF.
    assert!(
        crate::connection::io::read_varint_async(&mut stream)
            .await
            .is_err(),
        "response must be drained before the error is returned"
    );
}

#[tokio::test]
async fn first_block_handler_accepts_single_block_and_skips_empty_ones() {
    use crate::connection::select_response::{FirstBlockHandler, read_select_response};
    let mut payload = data_packet(&[]);
    payload.extend_from_slice(&data_packet(&[7]));
    payload.extend_from_slice(&data_packet(&[]));
    payload.extend_from_slice(&end_of_stream_packet());

    let mut stream = fake_stream(payload).await;
    let block = read_select_response(
        &mut stream,
        Duration::from_secs(5),
        None,
        false,
        &QueryCallbacks::default(),
        FirstBlockHandler::new(usize::MAX),
    )
    .await
    .expect("single non-empty block must succeed");
    assert_eq!(block_values(&block), vec![7]);
}

#[tokio::test]
async fn blocks_handler_collects_all_blocks_and_preserves_boundaries() {
    use crate::connection::select_response::{BlocksHandler, read_select_response};
    // ProfileEvents blocks are log traffic: read and discarded, never part of
    // the result — even when they carry rows.
    let mut payload = data_packet(&[1, 2]);
    payload.extend_from_slice(&data_packet(&[3]));
    payload.extend_from_slice(&profile_events_packet(&[99], None));
    payload.extend_from_slice(&data_packet(&[4, 5, 6]));
    payload.extend_from_slice(&end_of_stream_packet());

    let mut stream = fake_stream(payload).await;
    let blocks = read_select_response(
        &mut stream,
        Duration::from_secs(5),
        None,
        false,
        &QueryCallbacks::default(),
        BlocksHandler::new(usize::MAX),
    )
    .await
    .expect("multi-block response must succeed");
    assert_eq!(
        blocks.iter().map(|b| b.row_count()).collect::<Vec<_>>(),
        vec![2, 1, 3],
        "block boundaries must be preserved"
    );
    assert_eq!(block_values(&blocks[0]), vec![1, 2]);
    assert_eq!(block_values(&blocks[1]), vec![3]);
    assert_eq!(block_values(&blocks[2]), vec![4, 5, 6]);
}

// ---------------------------------------------------------------------------
// Response-size budget (max_response_size) — accumulating handlers only
// ---------------------------------------------------------------------------

/// One `data_packet(&[a, b])` block decodes to a single UInt64 column of
/// `2 * 8 = 16` payload bytes — the unit of the response budget.
fn two_block_payload() -> Vec<u8> {
    let mut payload = data_packet(&[1, 2]);
    payload.extend_from_slice(&data_packet(&[3, 4]));
    payload.extend_from_slice(&end_of_stream_packet());
    payload
}

#[tokio::test]
async fn blocks_handler_tiny_cap_breaches_on_second_block() {
    use crate::connection::select_response::{BlocksHandler, read_select_response};
    // Budget 16 bytes: the first block (16 payload bytes) fits exactly, the
    // second breaches at a block boundary.
    let mut stream = fake_stream(two_block_payload()).await;
    let result = read_select_response(
        &mut stream,
        Duration::from_secs(5),
        None,
        false,
        &QueryCallbacks::default(),
        BlocksHandler::new(16),
    )
    .await;
    let err = result
        .err()
        .expect("second block must breach the 16-byte budget");
    match &err {
        crate::error::Error::ResponseTooLarge { limit, received } => {
            assert_eq!(*limit, 16);
            assert_eq!(*received, 32, "breach reports the decoded total");
        },
        other => unreachable!("expected ResponseTooLarge, got {other:?}"),
    }
    assert!(
        err.to_string().contains("max_response_size 16")
            && err.to_string().contains("with_max_response_size"),
        "error must name the limit and the remedy: {err}"
    );
    assert!(
        err.is_broken_connection(),
        "the mid-response socket must be discarded"
    );
}

#[tokio::test]
async fn blocks_handler_exactly_at_cap_passes() {
    use crate::connection::select_response::{BlocksHandler, read_select_response};
    // Two blocks of 16 payload bytes each: exactly 32 stays within a
    // 32-byte budget (a strict-greater-than check, never off-by-one).
    let mut stream = fake_stream(two_block_payload()).await;
    let blocks = read_select_response(
        &mut stream,
        Duration::from_secs(5),
        None,
        false,
        &QueryCallbacks::default(),
        BlocksHandler::new(32),
    )
    .await
    .expect("cumulative payload exactly at the cap must pass");
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0].payload_bytes(), 16);
    assert_eq!(blocks[1].payload_bytes(), 16);
}

#[tokio::test]
async fn all_rows_handler_charges_decoded_block_payload() {
    use crate::connection::select_response::{AllRowsHandler, read_select_response};
    // Row-vector APIs charge the same decoded block payload metric: 16-byte
    // blocks, budget 16 → the second block breaches.
    let mut stream = fake_stream(two_block_payload()).await;
    let result = read_select_response(
        &mut stream,
        Duration::from_secs(5),
        None,
        false,
        &QueryCallbacks::default(),
        AllRowsHandler::<(u64,)>::new(16),
    )
    .await;
    let err = match result {
        Err(e) => e,
        Ok(rows) => unreachable!("row accumulation must respect the budget, got {rows:?} rows"),
    };
    assert!(
        matches!(
            err,
            crate::error::Error::ResponseTooLarge {
                limit: 16,
                received: 32
            }
        ),
        "expected ResponseTooLarge(16, 32), got {err:?}"
    );

    // Exactly at cap: all rows materialize.
    let mut stream = fake_stream(two_block_payload()).await;
    let rows = read_select_response(
        &mut stream,
        Duration::from_secs(5),
        None,
        false,
        &QueryCallbacks::default(),
        AllRowsHandler::<(u64,)>::new(32),
    )
    .await
    .expect("exactly-at-cap row read must pass");
    assert_eq!(rows.as_slice(), &[(1u64,), (2u64,), (3u64,), (4u64,)]);
}

#[tokio::test]
async fn first_block_handler_charges_only_the_retained_block() {
    use crate::connection::select_response::{FirstBlockHandler, read_select_response};
    // The retained first block (16 payload bytes) is budgeted; later blocks
    // are discarded un-materialized and never charged.
    let mut payload = data_packet(&[1, 2]);
    payload.extend_from_slice(&data_packet(&[]));
    payload.extend_from_slice(&end_of_stream_packet());

    let mut stream = fake_stream(payload.clone()).await;
    let block = read_select_response(
        &mut stream,
        Duration::from_secs(5),
        None,
        false,
        &QueryCallbacks::default(),
        FirstBlockHandler::new(16),
    )
    .await
    .expect("retained block exactly at cap passes");
    assert_eq!(block_values(&block), vec![1, 2]);

    let mut stream = fake_stream(payload).await;
    let err = read_select_response(
        &mut stream,
        Duration::from_secs(5),
        None,
        false,
        &QueryCallbacks::default(),
        FirstBlockHandler::new(15),
    )
    .await
    .err()
    .expect("a single block larger than the budget must breach");
    assert!(
        matches!(
            err,
            crate::error::Error::ResponseTooLarge {
                limit: 15,
                received: 16
            }
        ),
        "expected ResponseTooLarge(15, 16), got {err:?}"
    );
}

#[tokio::test]
async fn raw_blocks_handler_budgets_native_payload_bytes() {
    use crate::connection::select_response::{RawBlocksHandler, read_select_response};
    // Raw capture charges the native block body length (RawBlock::payload_bytes),
    // which is larger than the materialized column bytes.
    let mut payload = Vec::new();
    payload.extend_from_slice(&data_packet(&[1]));
    payload.extend_from_slice(&end_of_stream_packet());
    let mut stream = fake_stream(payload).await;
    let blocks = read_select_response(
        &mut stream,
        Duration::from_secs(5),
        None,
        false,
        &QueryCallbacks::default(),
        RawBlocksHandler::new(usize::MAX),
    )
    .await
    .expect("unbudgeted raw read must pass");
    let raw_len = blocks[0].payload_bytes();
    assert!(raw_len > 8, "raw body includes framing beyond column bytes");

    let mut payload = Vec::new();
    payload.extend_from_slice(&data_packet(&[1]));
    payload.extend_from_slice(&data_packet(&[2]));
    payload.extend_from_slice(&end_of_stream_packet());
    let mut stream = fake_stream(payload).await;
    let result = read_select_response(
        &mut stream,
        Duration::from_secs(5),
        None,
        false,
        &QueryCallbacks::default(),
        RawBlocksHandler::new(raw_len),
    )
    .await;
    let err = match result {
        Err(e) => e,
        Ok(_) => unreachable!("second raw block must breach the per-block-sized budget"),
    };
    assert!(
        matches!(err, crate::error::Error::ResponseTooLarge { limit, received } if limit == raw_len && received == 2 * raw_len),
        "expected ResponseTooLarge(raw_len, 2*raw_len), got {err:?}"
    );
}

#[tokio::test]
async fn streaming_reader_is_not_budgeted_while_blocks_would_breach() {
    // The streaming path (rows()/RowCursor, BlockStream) takes no budget by
    // design: the same payload that breaches a tiny cap on the accumulating
    // handler streams through fine.
    use crate::connection::row_stream_reader::read_query_blocks;
    use crate::connection::select_response::{BlocksHandler, read_select_response};

    let mut payload = Vec::new();
    for _ in 0..32 {
        payload.extend_from_slice(&data_packet(&[1, 2]));
    }
    payload.extend_from_slice(&end_of_stream_packet());

    // Sanity: the accumulating handler breaches a 16-byte budget on it.
    let mut stream = fake_stream(payload.clone()).await;
    let result = read_select_response(
        &mut stream,
        Duration::from_secs(5),
        None,
        false,
        &QueryCallbacks::default(),
        BlocksHandler::new(16),
    )
    .await;
    assert!(
        matches!(result, Err(crate::error::Error::ResponseTooLarge { .. })),
        "accumulating read of the same payload must breach"
    );

    // The streaming reader carries every block with no cap. Drain the
    // channel concurrently: the reader's `send` parks once the bounded
    // channel is full, so awaiting the read before receiving would deadlock.
    let stream = fake_stream(payload).await;
    let (tx, mut rx) = crate::runtime::sync::mpsc::channel(4);
    let reader_tx = tx.clone();
    let reader = crate::runtime::spawn(async move {
        read_query_blocks(
            stream,
            &reader_tx,
            &QueryCallbacks::default(),
            None,
            Duration::from_secs(5),
            None,
            false,
        )
        .await
    });
    let mut blocks = 0;
    let mut reader_err: Option<crate::error::Error> = None;
    loop {
        match rx.recv().await {
            Some(Ok(Some(_))) => blocks += 1,
            Some(Ok(None)) | None => break,
            Some(Err(e)) => {
                reader_err = Some(e);
                break;
            },
        }
    }
    drop(tx);
    reader
        .await
        .expect("reader task joins")
        .expect("streaming read must not be budgeted");
    assert!(
        reader_err.is_none(),
        "streaming read failed: {:?}",
        reader_err
    );
    assert_eq!(blocks, 32, "every streamed block must arrive");
}

#[tokio::test]
#[cfg(feature = "lz4")]
async fn blocks_handler_reads_lz4_compressed_blocks() {
    use crate::connection::select_response::{BlocksHandler, read_select_response};
    let mut payload = compressed_data_packet(&[1, 2], CompressionMethod::Lz4);
    payload.extend_from_slice(&compressed_data_packet(&[3, 4], CompressionMethod::Lz4));
    // ProfileEvents follow the response-compression flag too.
    payload.extend_from_slice(&profile_events_packet(&[9], Some(CompressionMethod::Lz4)));
    payload.extend_from_slice(&end_of_stream_packet());

    let mut stream = fake_stream(payload).await;
    let blocks = read_select_response(
        &mut stream,
        Duration::from_secs(5),
        None,
        true,
        &QueryCallbacks::default(),
        BlocksHandler::new(usize::MAX),
    )
    .await
    .expect("compressed multi-block response must succeed");
    assert_eq!(block_values(&blocks[0]), vec![1, 2]);
    assert_eq!(block_values(&blocks[1]), vec![3, 4]);
}

#[tokio::test]
#[cfg(feature = "lz4")]
async fn lz4_framed_block_parsed_as_plain_must_fail() {
    // Guards the compression flag plumbing: LZ4-framed data decoded on the
    // uncompressed path must fail loudly, not "succeed" with garbage rows.
    use crate::connection::select_response::{BlocksHandler, read_select_response};
    let mut payload = compressed_data_packet(&[1, 2], CompressionMethod::Lz4);
    payload.extend_from_slice(&end_of_stream_packet());

    let mut stream = fake_stream(payload).await;
    let result = read_select_response(
        &mut stream,
        Duration::from_secs(5),
        None,
        false,
        &QueryCallbacks::default(),
        BlocksHandler::new(usize::MAX),
    )
    .await;
    assert!(
        result.is_err(),
        "incorrectly flagged LZ4 frame must not decode as a plain block"
    );
}

#[tokio::test]
#[cfg(feature = "zstd")]
async fn blocks_handler_reads_zstd_compressed_blocks() {
    use crate::connection::select_response::{BlocksHandler, read_select_response};
    let mut payload = compressed_data_packet(&[10, 20], CompressionMethod::Zstd);
    payload.extend_from_slice(&profile_events_packet(&[9], Some(CompressionMethod::Zstd)));
    payload.extend_from_slice(&end_of_stream_packet());

    let mut stream = fake_stream(payload).await;
    let blocks = read_select_response(
        &mut stream,
        Duration::from_secs(5),
        None,
        true,
        &QueryCallbacks::default(),
        BlocksHandler::new(usize::MAX),
    )
    .await
    .expect("zstd multi-block response must succeed");
    assert_eq!(block_values(&blocks[0]), vec![10, 20]);
}

#[tokio::test]
#[cfg(feature = "lz4")]
async fn read_query_blocks_streams_compressed_data_blocks() {
    // The rows() background reader must honor the negotiated response
    // compression for Data blocks and ProfileEvents alike.
    use crate::connection::row_stream_reader::read_query_blocks;
    let mut payload = compressed_data_packet(&[1, 2], CompressionMethod::Lz4);
    payload.extend_from_slice(&compressed_data_packet(&[3], CompressionMethod::Lz4));
    payload.extend_from_slice(&profile_events_packet(&[9], Some(CompressionMethod::Lz4)));
    payload.extend_from_slice(&end_of_stream_packet());

    let stream = fake_stream(payload).await;
    let (tx, mut rx) = crate::runtime::sync::mpsc::channel(4);
    read_query_blocks(
        stream,
        &tx,
        &QueryCallbacks::default(),
        None,
        Duration::from_secs(5),
        None,
        true,
    )
    .await
    .expect("compressed stream read must succeed");
    drop(tx);

    let mut rows = Vec::new();
    while let Some(msg) = rx.recv().await {
        match msg.expect("streamed block result") {
            Some(block) => rows.extend(block_values(&block)),
            None => break,
        }
    }
    assert_eq!(rows, vec![1, 2, 3]);
}

#[tokio::test]
async fn read_query_blocks_streams_plain_data_blocks() {
    use crate::connection::row_stream_reader::read_query_blocks;
    let mut payload = data_packet(&[5, 6]);
    payload.extend_from_slice(&profile_events_packet(&[9], None));
    payload.extend_from_slice(&end_of_stream_packet());

    let stream = fake_stream(payload).await;
    let (tx, mut rx) = crate::runtime::sync::mpsc::channel(4);
    read_query_blocks(
        stream,
        &tx,
        &QueryCallbacks::default(),
        None,
        Duration::from_secs(5),
        None,
        false,
    )
    .await
    .expect("plain stream read must succeed");
    drop(tx);

    let mut rows = Vec::new();
    while let Some(msg) = rx.recv().await {
        match msg.expect("streamed block result") {
            Some(block) => rows.extend(block_values(&block)),
            None => break,
        }
    }
    assert_eq!(rows, vec![5, 6]);
}
