use crate::compression::CompressionMethod;
use crate::connection::block_reader::read_column_async;
use crate::connection::callbacks::QueryCallbacks;
use crate::connection::query_packet::{
    build_query_packet, build_query_packet_from_template, build_query_packet_template,
    next_query_id,
};
use crate::connection::raw_block_reader::read_column_raw_recorded;
use crate::connection::tcp::Client;
use crate::protocol::parameters::QueryParameter;
use crate::protocol::revision;
use crate::protocol::wire;
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
    read_column_raw_recorded(&mut reader, type_name, rows, &mut out)
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
    assert!(Error::Protocol("protocol err".into()).is_retryable());
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
