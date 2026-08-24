//! Connect-timeout correctness tests (deterministic, server-free by default).
//!
//! A server that accepts TCP and then never sends its Hello must trip the
//! configured `connect_timeout` — in both clients — long before the much
//! larger `query_timeout` could mask the stall. Live-server tests at the
//! bottom skip when no ClickHouse is reachable.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use st_clickhouse::error::Error as AsyncError;
use st_clickhouse::sync::config::ClientConfig;
use st_clickhouse::sync::error::Error as SyncError;

// ══════════════════════════════════════════════════════════════════════════
// Helpers
// ══════════════════════════════════════════════════════════════════════════

/// Spawn a listener that accepts connections and then never writes a byte —
/// ClickHouse that accepts TCP but never sends Hello.
fn silent_listener() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
    let addr = listener.local_addr().expect("listener address").to_string();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    // Drain close notifications so the accept loop keeps going;
                    // never write anything back.
                    held.push(stream);
                },
                Err(_) => break,
            }
        }
    });
    addr
}

/// Reserve and release an ephemeral loopback port, yielding an address that
/// is refused unless another process races to bind it.
fn refused_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind refused-port probe");
    let addr = listener
        .local_addr()
        .expect("refused-port address")
        .to_string();
    drop(listener);
    addr
}

fn sync_config(addr: &str, connect_timeout: Duration, query_timeout: Duration) -> ClientConfig {
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    ClientConfig::default()
        .with_host(host)
        .with_port(port.parse().expect("u16 port"))
        .with_connect_timeout(connect_timeout)
        .with_query_timeout(query_timeout)
}

// ══════════════════════════════════════════════════════════════════════════
// Sync client
// ══════════════════════════════════════════════════════════════════════════

/// A silent server must fail the *setup* phase within `connect_timeout`, not
/// hang until `query_timeout` (60 s here) — the exact bug being fixed.
#[test]
fn sync_silent_server_trips_connect_timeout_not_query_timeout() {
    let addr = silent_listener();
    let config = sync_config(&addr, Duration::from_millis(400), Duration::from_secs(60));

    let start = Instant::now();
    let err = match st_clickhouse::sync::client::SyncClient::connect_with_config(config) {
        Ok(_) => unreachable!("silent server must fail the handshake"),
        Err(e) => e,
    };
    let elapsed = start.elapsed();

    match &err {
        SyncError::Timeout(msg) => {
            assert!(msg.contains("did not complete"), "message: {msg}");
        },
        other => unreachable!("expected Timeout, got {other:?}"),
    }
    assert!(
        elapsed < Duration::from_secs(20),
        "setup must trip in ~400ms, not wait for the 60s query_timeout; took {elapsed:?}"
    );
    assert!(err.is_timeout());
}

/// A peer that sends one continuation byte before each socket timeout cannot
/// reset the setup budget indefinitely: the absolute watchdog still expires.
#[test]
fn sync_byte_drip_cannot_extend_connect_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
    let addr = listener.local_addr().expect("listener address");
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().expect("accept test socket");
        let mut hello = [0u8; 64];
        let _ = socket.read(&mut hello);
        loop {
            std::thread::sleep(Duration::from_millis(80));
            if socket.write_all(&[0x80]).is_err() {
                break;
            }
        }
    });
    let config = sync_config(
        &addr.to_string(),
        Duration::from_millis(350),
        Duration::from_secs(60),
    );

    let start = Instant::now();
    let err = st_clickhouse::sync::client::SyncClient::connect_with_config(config)
        .err()
        .expect("byte-drip peer must not complete setup");
    let elapsed = start.elapsed();
    assert!(
        matches!(err, SyncError::Timeout(_)),
        "absolute deadline must win over drip-fed bytes, got {err:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "350ms wall deadline stretched to {elapsed:?}"
    );
    server.join().expect("server thread");
}

/// Zero is rejected up front with a clear Config error — never "no deadline".
#[test]
fn sync_zero_connect_timeout_is_rejected() {
    let addr = silent_listener();
    let config = sync_config(&addr, Duration::ZERO, Duration::from_secs(5));

    let start = Instant::now();
    let err = match st_clickhouse::sync::client::SyncClient::connect_with_config(config) {
        Ok(_) => unreachable!("zero connect_timeout must be rejected"),
        Err(e) => e,
    };
    assert!(
        matches!(&err, SyncError::Config(msg) if msg.contains("connect_timeout")),
        "expected Config, got {err:?}"
    );
    assert!(start.elapsed() < Duration::from_secs(2));
}

/// Fast network errors keep their I/O identity — refused stays `Io`, not
/// `Timeout`, and returns immediately.
#[test]
fn sync_refused_port_stays_io_error() {
    let config = sync_config(
        &refused_addr(),
        Duration::from_secs(5),
        Duration::from_secs(5),
    );
    let start = Instant::now();
    let err = match st_clickhouse::sync::client::SyncClient::connect_with_config(config) {
        Ok(_) => unreachable!("refused port must fail"),
        Err(e) => e,
    };
    assert!(matches!(err, SyncError::Io(_)), "expected Io, got {err:?}");
    assert!(start.elapsed() < Duration::from_secs(5));
}

/// `connect_stream` semantics: a pre-established socket to a silent server
/// still gets its setup bounded by `connect_timeout`.
#[test]
fn sync_connect_stream_setup_is_bounded() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
    let addr = listener.local_addr().expect("listener address");
    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().expect("accept test socket");
        let mut scratch = [0u8; 64];
        // Read the client hello, never answer, and hold the socket open well
        // past the 300 ms setup budget so the client sees a stall, not EOF.
        let _ = sock.read(&mut scratch);
        std::thread::sleep(Duration::from_secs(3));
    });
    let stream = TcpStream::connect(addr).expect("connect test socket");
    let config = sync_config(
        "unused:9000",
        Duration::from_millis(300),
        Duration::from_secs(60),
    );

    let start = Instant::now();
    let err = match st_clickhouse::sync::client::SyncClient::connect_stream(stream, config) {
        Ok(_) => unreachable!("silent peer must fail setup"),
        Err(e) => e,
    };
    assert!(
        matches!(&err, SyncError::Timeout(msg) if msg.contains("did not complete")),
        "expected setup Timeout, got {err:?}"
    );
    assert!(start.elapsed() < Duration::from_secs(20));
    server.join().expect("server thread");
}

// ══════════════════════════════════════════════════════════════════════════
// Async client (builder → pool → connect_raw wiring)
// ══════════════════════════════════════════════════════════════════════════

/// The builder option must reach real connection attempts: connecting to a
/// silent server with `connect_timeout` fails fast with `Error::Timeout`.
#[tokio::test]
async fn async_builder_connect_timeout_reaches_new_connections() {
    let addr = silent_listener();
    let start = Instant::now();
    let err = match st_clickhouse::ClientBuilder::<st_clickhouse::Async>::new()
        .host(addr)
        .connect_timeout(Duration::from_millis(400))
        .connect()
        .await
    {
        Ok(_) => unreachable!("silent server must fail the connect"),
        Err(e) => e,
    };
    let elapsed = start.elapsed();

    match &err {
        AsyncError::Timeout(msg) => {
            assert!(msg.contains("timed out after 400ms"), "message: {msg}");
        },
        other => unreachable!("expected Timeout, got {other:?}"),
    }
    assert!(
        elapsed < Duration::from_secs(20),
        "connect must trip in ~400ms; took {elapsed:?}"
    );
}

/// The URL option takes the same path: `?connect_timeout=` bounds the connect.
#[tokio::test]
async fn async_url_connect_option_reaches_new_connections() {
    let addr = silent_listener();
    let url = format!("clickhouse://{addr}/?connect_timeout=400ms");
    let err = match st_clickhouse::ClientBuilder::<st_clickhouse::Async>::from_url(&url)
        .expect("url parses")
        .connect()
        .await
    {
        Ok(_) => unreachable!("silent server must fail the connect"),
        Err(e) => e,
    };
    match &err {
        AsyncError::Timeout(msg) => {
            assert!(msg.contains("timed out after 400ms"), "message: {msg}")
        },
        other => unreachable!("expected Timeout, got {other:?}"),
    }
}

/// Failover stays intact: a refused address fails fast as `Io`, the pool moves
/// on, and the silent second address surfaces as `Timeout` with its address.
#[tokio::test]
async fn async_connect_timeout_failover_preserved() {
    let silent = silent_listener();
    let host_port = silent.clone();
    let (host, port) = host_port.rsplit_once(':').expect("host:port");
    let start = Instant::now();
    let err = match st_clickhouse::ClientBuilder::<st_clickhouse::Async>::new()
        .hosts([refused_addr(), silent])
        .connect_timeout(Duration::from_millis(300))
        .connect()
        .await
    {
        Ok(_) => unreachable!("no address can complete a handshake"),
        Err(e) => e,
    };
    let elapsed = start.elapsed();

    match &err {
        AsyncError::Timeout(msg) => {
            assert!(
                msg.contains(host) && msg.contains(port),
                "message must name the silent address: {msg}"
            );
        },
        other => unreachable!("expected Timeout from the silent address, got {other:?}"),
    }
    assert!(elapsed < Duration::from_secs(20), "took {elapsed:?}");
}

/// Zero is rejected as deterministic Config — not retried across addresses.
#[tokio::test]
async fn async_zero_connect_timeout_is_rejected() {
    let addr = silent_listener();
    let err = match st_clickhouse::ClientBuilder::<st_clickhouse::Async>::new()
        .host(addr)
        .connect_timeout(Duration::ZERO)
        .connect()
        .await
    {
        Ok(_) => unreachable!("zero connect_timeout must be rejected"),
        Err(e) => e,
    };
    assert!(
        matches!(&err, AsyncError::Config(msg) if msg.contains("connect_timeout")),
        "expected Config, got {err:?}"
    );
}

// ══════════════════════════════════════════════════════════════════════════
// Live server (skipped when ClickHouse is not reachable)
// ══════════════════════════════════════════════════════════════════════════

fn live_addr() -> String {
    std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "127.0.0.1:9000".to_owned())
}

fn live_creds() -> Vec<(String, String)> {
    match (
        std::env::var("CLICKHOUSE_USER"),
        std::env::var("CLICKHOUSE_PASSWORD"),
    ) {
        (Ok(user), Ok(password)) => vec![(user, password)],
        _ => vec![
            ("honne".to_owned(), "honne".to_owned()),
            ("default".to_owned(), String::new()),
            ("default".to_owned(), "test".to_owned()),
        ],
    }
}

/// A configured connect timeout must not break healthy connects: both clients
/// still complete the handshake and serve a query.
#[test]
fn live_sync_connect_with_timeout_still_connects() {
    let addr = live_addr();
    let mut last: Option<st_clickhouse::sync::error::Error> = None;
    for (user, password) in live_creds() {
        let (host, port) = addr.rsplit_once(':').expect("host:port");
        let config = ClientConfig::default()
            .with_host(host)
            .with_port(port.parse().expect("u16 port"))
            .with_user(&user)
            .with_password(&password)
            .with_connect_timeout(Duration::from_secs(5))
            .with_query_timeout(Duration::from_secs(10));
        match st_clickhouse::sync::client::SyncClient::connect_with_config(config) {
            Ok(mut client) => {
                client
                    .query("SELECT toUInt8(1) AS v")
                    .expect("live query must succeed");
                return;
            },
            Err(e) => last = Some(e),
        }
    }
    // No live server — skip instead of failing.
    eprintln!("skipping: no ClickHouse at {addr} ({last:?})");
}

#[tokio::test]
async fn live_async_url_connect_timeout_still_connects() {
    let addr = live_addr();
    for (user, password) in live_creds() {
        let url = format!("clickhouse://{user}:{password}@{addr}/?connect_timeout=5s");
        // Ping (not a query) so the sync live test's query ID can never
        // collide with this one when both run in parallel.
        let pinged = match st_clickhouse::ClientBuilder::<st_clickhouse::Async>::from_url(&url)
            .expect("url parses")
            .connect()
            .await
        {
            Ok(client) => client.ping().await,
            Err(_) => continue,
        };
        pinged.expect("live ping must succeed within the connect timeout config");
        return;
    }
    eprintln!("skipping: no ClickHouse at {addr}");
}
