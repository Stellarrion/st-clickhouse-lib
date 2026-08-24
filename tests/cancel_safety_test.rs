//! Cancellation and abort-safety tests.
//!
//! `Client::cancel` is fail-closed (a `Client` owns a pool, not the connection
//! running the query), and a future dropped mid-response must convert into
//! exactly one clean reconnect instead of poisoning a pooled socket. The
//! mock-server tests below prove both against a scripted native-protocol
//! server; the live test proves the abort path against a real ClickHouse.

mod common;

use st_clickhouse::error::Error;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ══════════════════════════════════════════════════════════════════════
// Minimal native-protocol mock server
// ══════════════════════════════════════════════════════════════════════

/// Server revision the mock advertises. 54471 exercises the addendum, chunked
/// negotiation, and versioned-parallel-replicas fields while staying below the
/// server-settings gate (54474) the mock would otherwise have to encode.
const SERVER_REVISION: u64 = 54471;

/// Marker scanned for in the client's byte stream to detect a sent query.
/// The query text travels verbatim inside the query packet.
const QUERY_MARKER: &[u8] = b"SELECT";

#[derive(Default)]
struct MockState {
    /// Completed native handshakes (one per TCP connection).
    handshakes: AtomicUsize,
    /// The first query of the first connection arrived (and was answered).
    first_answered: AtomicBool,
    /// Per-connection: all bytes received after the handshake Ping. Used to
    /// detect unframed garbage (e.g. a raw Cancel byte from pool drop).
    received: Mutex<Vec<(usize, Vec<u8>)>>,
}

fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut b = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            b |= 0x80;
        }
        buf.push(b);
        if v == 0 {
            break;
        }
    }
}

fn put_string(buf: &mut Vec<u8>, s: &str) {
    put_varint(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}

async fn read_varint(sock: &mut TcpStream) -> std::io::Result<u64> {
    let mut v = 0u64;
    let mut shift = 0u32;
    loop {
        let mut b = [0u8; 1];
        sock.read_exact(&mut b).await?;
        v |= u64::from(b[0] & 0x7f) << shift;
        if b[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(v)
}

async fn read_string(sock: &mut TcpStream) -> std::io::Result<String> {
    let len = read_varint(sock).await? as usize;
    let mut buf = vec![0u8; len];
    sock.read_exact(&mut buf).await?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Read one chunked frame (`u32 len + payload + u32 zero`) — the transport the
/// client uses for everything after the addendum when chunked is negotiated.
async fn read_frame(sock: &mut TcpStream) -> Option<Vec<u8>> {
    let mut lenb = [0u8; 4];
    sock.read_exact(&mut lenb).await.ok()?;
    let len = u32::from_le_bytes(lenb) as usize;
    let mut payload = vec![0u8; len];
    sock.read_exact(&mut payload).await.ok()?;
    let mut z = [0u8; 4];
    sock.read_exact(&mut z).await.ok()?;
    (u32::from_le_bytes(z) == 0).then_some(payload)
}

/// Server Hello at revision 54471, mirroring the client's field order:
/// name, versions, revision, parallel-replicas protocol, timezone, display
/// name, version patch, chunked capabilities, password rules, interserver
/// nonce. `caps` is advertised for both chunked directions.
fn server_hello(caps: &str) -> Vec<u8> {
    let mut b = Vec::new();
    put_varint(&mut b, 0); // ServerPacket::Hello
    put_string(&mut b, "ClickHouse");
    put_varint(&mut b, 26); // version_major
    put_varint(&mut b, 7); // version_minor
    put_varint(&mut b, SERVER_REVISION);
    put_varint(&mut b, 7); // parallel replicas protocol version
    put_string(&mut b, "UTC"); // timezone (rev >= 54058)
    put_string(&mut b, "mock"); // display name (rev >= 54372)
    put_varint(&mut b, 1); // version patch (rev >= 54401)
    put_string(&mut b, caps); // proto_send_chunked_srv (rev >= 54470)
    put_string(&mut b, caps); // proto_recv_chunked_srv
    put_varint(&mut b, 0); // password complexity rules: none (rev >= 54461)
    b.extend_from_slice(&0i64.to_le_bytes()); // interserver nonce (rev >= 54462)
    b
}

/// A Progress packet (type 3) with all-zero counters: seven varints, as the
/// client's reader expects at the default revision.
fn progress_packet() -> Vec<u8> {
    let mut b = vec![3u8];
    for _ in 0..7 {
        put_varint(&mut b, 0);
    }
    b
}

/// Write one native packet, applying chunked framing (`u32 len + payload +
/// u32 zero`) when the transport negotiated chunked sending.
async fn write_packet(sock: &mut TcpStream, payload: &[u8], chunked: bool) {
    let res = if chunked {
        let len = u32::try_from(payload.len()).unwrap_or(u32::MAX);
        async {
            sock.write_all(&len.to_le_bytes()).await?;
            sock.write_all(payload).await?;
            sock.write_all(&0u32.to_le_bytes()).await
        }
        .await
    } else {
        sock.write_all(payload).await
    };
    let _ = res;
}

/// Read one byte at a time until `received` ends with `marker`. Returns false
/// when the connection closed before the marker arrived.
async fn read_until_marker(sock: &mut TcpStream, received: &mut Vec<u8>, marker: &[u8]) -> bool {
    loop {
        let mut b = [0u8; 1];
        match sock.read(&mut b).await {
            Ok(0) | Err(_) => return false,
            Ok(_) => {
                received.push(b[0]);
                if received.ends_with(marker) {
                    return true;
                }
            },
        }
    }
}

/// One scripted connection: handshake → addendum → ping/pong, then wait for
/// the query marker and answer.
///
/// - `stall_first` and this is the first connection: answer with Progress and
///   never send EndOfStream (a mid-response stall), or
/// - answer with Progress + EndOfStream, then capture everything the client
///   sends until close, so the tests can detect unframed garbage.
async fn serve_connection(
    mut sock: TcpStream, state: Arc<MockState>, conn_idx: usize, caps: &'static str,
    stall_first: bool,
) {
    // Client Hello: type, name, major, minor, revision, database, user, password.
    let hello = async {
        let _typ = read_varint(&mut sock).await?;
        let _name = read_string(&mut sock).await?;
        let _major = read_varint(&mut sock).await?;
        let _minor = read_varint(&mut sock).await?;
        let _revision = read_varint(&mut sock).await?;
        let _database = read_string(&mut sock).await?;
        let _user = read_string(&mut sock).await?;
        read_string(&mut sock).await // password
    };
    if hello.await.is_err() {
        return;
    }
    let _ = sock.write_all(&server_hello(caps)).await;

    // Addendum (rev >= 54458): quota key, chunked send/recv modes (>= 54470),
    // parallel-replicas protocol version (>= 54471).
    let addendum = async {
        read_string(&mut sock).await?;
        read_string(&mut sock).await?;
        read_string(&mut sock).await?;
        read_varint(&mut sock).await
    };
    if addendum.await.is_err() {
        return;
    }

    // "chunked_optional" caps make the client negotiate chunked both ways;
    // "notchunked" keeps the plain transport. Everything after the addendum is
    // framed accordingly.
    let chunked = caps.starts_with("chunked");
    state.handshakes.fetch_add(1, Ordering::SeqCst);

    // Ping (framed per the negotiated transport) → Pong.
    let ping_ok = if chunked {
        matches!(
            read_frame(&mut sock).await.as_deref(),
            Some(payload) if payload == [4u8]
        )
    } else {
        let mut b = [0u8; 1];
        sock.read_exact(&mut b).await.is_ok() && b[0] == 4
    };
    if !ping_ok {
        return;
    }
    write_packet(&mut sock, &[4], chunked).await;

    // Wait for the query, capturing every byte the client sends.
    let mut received: Vec<u8> = Vec::new();
    if !read_until_marker(&mut sock, &mut received, QUERY_MARKER).await {
        state
            .received
            .lock()
            .expect("received lock")
            .push((conn_idx, received));
        return;
    }

    if stall_first && conn_idx == 0 {
        // Partial response, then stall: no EndOfStream ever comes.
        write_packet(&mut sock, &progress_packet(), chunked).await;
        state.first_answered.store(true, Ordering::SeqCst);
        // Hold the socket open without further writes.
        tokio::time::sleep(Duration::from_secs(120)).await;
        return;
    }

    let mut response = progress_packet();
    response.push(5); // EndOfStream
    write_packet(&mut sock, &response, chunked).await;

    // Capture everything until close: the tail of the query packet plus any
    // trailing bytes (garbage) the client may still write.
    let mut buf = [0u8; 512];
    loop {
        match sock.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => received.extend_from_slice(&buf[..n]),
        }
    }
    state
        .received
        .lock()
        .expect("received lock")
        .push((conn_idx, received));
}

/// Spawn a mock server. `stall_first` makes the first connection stall
/// mid-response after its first query; every later connection answers
/// Progress + EndOfStream immediately.
async fn spawn_mock(
    caps: &'static str, stall_first: bool,
) -> (std::net::SocketAddr, Arc<MockState>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock server");
    let addr = listener.local_addr().expect("mock server address");
    let state = Arc::new(MockState::default());
    let server_state = state.clone();
    tokio::spawn(async move {
        let mut conn_idx = 0usize;
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                return;
            };
            let st = server_state.clone();
            let idx = conn_idx;
            conn_idx += 1;
            tokio::spawn(serve_connection(sock, st, idx, caps, stall_first));
        }
    });
    (addr, state)
}

/// Wait until the mock confirms the stalled query arrived and was answered.
async fn wait_first_answered(state: &MockState) {
    for _ in 0..500 {
        if state.first_answered.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        state.first_answered.load(Ordering::SeqCst),
        "mock never answered the first query"
    );
}

/// All bytes the mock received from `conn_idx` after the handshake Ping.
/// Yields while polling so the mock task (same test runtime) can record.
async fn received_from(state: &MockState, conn_idx: usize) -> Vec<u8> {
    for _ in 0..200 {
        {
            let guard = state.received.lock().expect("received lock");
            if let Some((_, bytes)) = guard.iter().find(|(idx, _)| *idx == conn_idx) {
                return bytes.clone();
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Vec::new()
}

/// Walk complete chunked frames (`u32 len + payload + u32 zero`) from the
/// start of `bytes` and return the count of bytes that belong to no complete
/// frame — i.e. unframed garbage such as a raw Cancel byte.
fn chunked_leftover(bytes: &[u8]) -> usize {
    let mut off = 0usize;
    while bytes.len() >= off + 4 {
        let len = u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]])
            as usize;
        off += 4;
        if bytes.len() < off + len + 4 || len > 1 << 20 {
            return bytes.len() - (off - 4);
        }
        off += len + 4;
    }
    bytes.len() - off
}

// ══════════════════════════════════════════════════════════════════════
// Mock-server tests
// ══════════════════════════════════════════════════════════════════════

/// Abort an `execute()` mid-response: the dropped future must discard the
/// pooled socket, the next `get()` reconnects (handshake #2), and the first
/// query after the abort succeeds on the fresh connection.
#[tokio::test]
async fn aborting_execute_mid_response_reconnects_cleanly() {
    let (addr, state) = spawn_mock("notchunked", true).await;
    let client = st_clickhouse::Client::connect_with_pool_credentials(addr, 1, "default", "")
        .await
        .expect("connect to mock");
    // Bound every query so a poisoned-reuse regression fails in seconds
    // instead of hanging on the stalled mock connection.
    let client = Arc::new(client.with_query_timeout(Duration::from_secs(5)));
    assert_eq!(state.handshakes.load(Ordering::SeqCst), 1);

    // First query: the mock answers Progress then stalls (no EndOfStream).
    let c = client.clone();
    let handle = tokio::spawn(async move { c.execute("SELECT sleep(2)").await });
    wait_first_answered(&state).await;

    // Abort: the future drops at the drain await point, mid-response.
    handle.abort();
    let _ = handle.await;

    // The mid-response socket must have been discarded, not pooled: the next
    // query reconnects (handshake #2) and succeeds through EndOfStream.
    client
        .execute("SELECT toUInt8(1)")
        .await
        .expect("first query after abort must succeed on a fresh connection");
    assert_eq!(
        state.handshakes.load(Ordering::SeqCst),
        2,
        "exactly one reconnect: no poisoned reuse, no extra handshakes"
    );
}

/// `Client::cancel` fails closed: while the only pool slot is busy with an
/// in-flight query it returns `Error::Config` immediately (no `pool.get()`
/// block, no false success) and opens no connection.
#[tokio::test]
async fn cancel_fails_closed_without_pool_side_effects() {
    let (addr, state) = spawn_mock("notchunked", true).await;
    let client = Arc::new(
        st_clickhouse::Client::connect_with_pool_credentials(addr, 1, "default", "")
            .await
            .expect("connect to mock"),
    );
    assert_eq!(state.handshakes.load(Ordering::SeqCst), 1);

    // Occupy the only slot with a stalled query.
    let c = client.clone();
    let holder = tokio::spawn(async move { c.execute("SELECT sleep(2)").await });
    wait_first_answered(&state).await;

    // cancel() must not wait behind the busy slot (the old behaviour with a
    // single-slot pool) — the 1s probe fails the test if it blocks.
    let cancel_result = tokio::time::timeout(Duration::from_secs(1), async {
        #[allow(deprecated)]
        client.cancel().await
    })
    .await
    .expect("cancel must return without waiting for the busy slot");

    match &cancel_result {
        Err(Error::Config(msg)) => assert!(
            msg.contains("query timeout")
                && msg.contains("BlockStream::cancel")
                && msg.contains("RowCursor"),
            "cancel error must name the query-scoped alternatives: {msg}"
        ),
        other => unreachable!("expected Error::Config, got {other:?}"),
    }

    // No connection was opened or touched: handshake count unchanged.
    assert_eq!(state.handshakes.load(Ordering::SeqCst), 1);

    holder.abort();
    let _ = holder.await;
}

/// Pool drop under chunked negotiation must not emit garbage: with chunked
/// framing active a raw Cancel byte is protocol noise, so the socket is just
/// closed. On the plain transport the best-effort Cancel byte is still sent.
#[tokio::test]
async fn pool_drop_emits_no_garbage_under_chunked_negotiation() {
    // Chunked transport: every byte the client sent must belong to a complete
    // chunked frame — no unframed Cancel on pool drop.
    let (addr, state) = spawn_mock("chunked_optional", false).await;
    {
        let client = st_clickhouse::Client::connect_with_pool_credentials(addr, 1, "default", "")
            .await
            .expect("connect to chunked mock");
        client
            .execute("SELECT 1")
            .await
            .expect("query under chunked negotiation");
        drop(client);
    }
    let received = received_from(&state, 0).await;
    assert!(
        !received.is_empty(),
        "mock must have captured the client's framed query"
    );
    assert_eq!(
        chunked_leftover(&received),
        0,
        "pool drop must not write unframed bytes on a chunked connection"
    );
    assert_eq!(state.handshakes.load(Ordering::SeqCst), 1);

    // Plain transport: the best-effort Cancel byte is still sent — exactly one
    // raw byte, which is wire-correct there. The client's own query packet
    // ends with the empty-block terminator (three zero varints), so a trailing
    // 0x03 can only be the pool-drop Cancel.
    let (addr, state) = spawn_mock("notchunked", false).await;
    {
        let client = st_clickhouse::Client::connect_with_pool_credentials(addr, 1, "default", "")
            .await
            .expect("connect to plain mock");
        client
            .execute("SELECT 1")
            .await
            .expect("query on plain transport");
        drop(client);
    }
    let received = received_from(&state, 0).await;
    assert!(
        received.ends_with(&[CLIENT_PACKET_CANCEL]),
        "plain-transport pool drop should still send its best-effort Cancel byte"
    );
}

/// Raw client Cancel packet type, for the plain-transport drop assertion.
const CLIENT_PACKET_CANCEL: u8 = 3;

// ══════════════════════════════════════════════════════════════════════
// Live server
// ══════════════════════════════════════════════════════════════════════

/// Live proof: aborting an `execute()` on `SELECT sleep(2)` mid-response
/// leaves the pool usable — the very next execute and SELECT both succeed on
/// a clean connection.
#[tokio::test]
async fn live_abort_execute_leaves_pool_usable() {
    let client = Arc::new(common::connect_client_pool(1).await);

    let c = client.clone();
    let handle = tokio::spawn(async move { c.execute("SELECT sleep(2)").await });
    // Let the query start and its response cycle begin.
    tokio::time::sleep(Duration::from_millis(300)).await;
    handle.abort();
    let _ = handle.await;

    client
        .execute("SELECT toUInt8(1)")
        .await
        .expect("pool must be usable immediately after an aborted execute");
    let one: (u8,) = client
        .query("SELECT toUInt8(1)")
        .fetch()
        .await
        .expect("SELECT path must work on the post-abort pool");
    assert_eq!(one.0, 1);
}
