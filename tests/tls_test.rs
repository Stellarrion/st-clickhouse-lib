//! TLS integration tests for st-clickhouse native protocol.
//!
//! Requires ClickHouse with TLS enabled on port 9440.
//! Set env vars:
//!   CLICKHOUSE_TLS_HOST=127.0.0.1:9440
//!   CLICKHOUSE_TLS_CA=/path/to/ca.crt
//!   CLICKHOUSE_TLS_DOMAIN=localhost

use st_clickhouse::Client;
#[cfg(feature = "tls")]
use st_clickhouse::compression::CompressionMethod;

#[cfg(feature = "tls")]
use rustls::pki_types::pem::PemObject;

#[cfg(feature = "tls")]
struct TlsTarget {
    host: String,
    domain: String,
    ca_path: String,
}

#[cfg(feature = "tls")]
fn tls_target() -> Option<TlsTarget> {
    let host = std::env::var("CLICKHOUSE_TLS_HOST").ok()?;
    let ca_path = std::env::var("CLICKHOUSE_TLS_CA").ok()?;
    let domain = std::env::var("CLICKHOUSE_TLS_DOMAIN").unwrap_or_else(|_| "localhost".to_string());
    Some(TlsTarget {
        host,
        domain,
        ca_path,
    })
}

#[cfg(feature = "tls")]
fn ca_verified_config(ca_path: &str) -> rustls::ClientConfig {
    let ca_pem = std::fs::read(ca_path).expect("open CLICKHOUSE_TLS_CA");
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls::pki_types::CertificateDer::pem_slice_iter(&ca_pem) {
        roots
            .add(cert.expect("parse CA certificate"))
            .expect("add CA");
    }
    rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
}

#[cfg(feature = "tls")]
async fn tls_client() -> Option<Client> {
    let target = match tls_target() {
        Some(target) => target,
        None => {
            eprintln!("Skipping TLS test: set CLICKHOUSE_TLS_HOST and CLICKHOUSE_TLS_CA");
            return None;
        },
    };
    let config = ca_verified_config(&target.ca_path);
    Some(
        Client::connect_tls_with_config(
            target.host.as_str(),
            "default",
            "",
            config,
            &target.domain,
        )
        .await
        .expect("TLS connect should succeed"),
    )
}

/// Connect over TLS with an explicit CA and run SELECT 1.
#[cfg(feature = "tls")]
#[tokio::test]
async fn test_tls_connect_ca_verified() {
    let Some(client) = tls_client().await else {
        return;
    };
    let rows: Vec<(u8,)> = client
        .query("SELECT 1")
        .all()
        .await
        .expect("SELECT 1 over TLS should succeed");
    assert_eq!(rows, vec![(1,)]);
}

/// Connect over TLS with explicit builder options.
#[cfg(feature = "tls")]
#[tokio::test]
async fn test_tls_connect_custom_config() {
    let Some(target) = tls_target() else {
        eprintln!("Skipping TLS test: set CLICKHOUSE_TLS_HOST and CLICKHOUSE_TLS_CA");
        return;
    };
    let config = ca_verified_config(&target.ca_path);
    let client = Client::connect_tls_with_config(
        target.host.as_str(),
        "default",
        "",
        config,
        &target.domain,
    )
    .await
    .expect("TLS connect with custom config should succeed");
    let rows: Vec<(u8,)> = client
        .query("SELECT 1")
        .all()
        .await
        .expect("SELECT 1 over custom TLS config should succeed");
    assert_eq!(rows, vec![(1,)]);
}

/// Insert + SELECT over TLS connection.
#[cfg(feature = "tls")]
#[tokio::test]
async fn test_tls_insert_select() {
    let Some(client) = tls_client().await else {
        return;
    };

    client
        .execute("DROP TABLE IF EXISTS test_tls")
        .await
        .expect("drop TLS table");
    client
        .execute("CREATE TABLE test_tls (x UInt64, s String) ENGINE = Memory")
        .await
        .expect("create TLS temp table");

    client
        .execute("INSERT INTO test_tls VALUES (42, 'hello'), (99, 'world')")
        .await
        .expect("insert over TLS");

    let rows: Vec<(u64, String)> = client
        .query("SELECT x, s FROM test_tls ORDER BY x")
        .all()
        .await
        .expect("select inserted TLS rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (42, "hello".to_string()));
    assert_eq!(rows[1], (99, "world".to_string()));
}

/// TLS + compression: LZ4 over TLS.
#[cfg(feature = "tls")]
#[tokio::test]
async fn test_tls_with_lz4_compression() {
    let Some(client) = tls_client().await else {
        return;
    };
    let client = client.with_compression(CompressionMethod::Lz4);

    client
        .execute("DROP TABLE IF EXISTS test_tls_lz4")
        .await
        .expect("drop TLS LZ4 table");
    client
        .execute("CREATE TABLE test_tls_lz4 (x UInt64) ENGINE = Memory")
        .await
        .expect("create TLS LZ4 temp table");
    client
        .execute("INSERT INTO test_tls_lz4 VALUES (1), (2), (3)")
        .await
        .expect("insert over TLS LZ4");
    let rows: Vec<(u64,)> = client
        .query("SELECT x FROM test_tls_lz4 ORDER BY x")
        .all()
        .await
        .expect("select TLS LZ4 rows");
    assert_eq!(rows, vec![(1,), (2,), (3,)]);
}

/// Connecting to TLS port without TLS support should fail gracefully.
#[cfg(not(feature = "tls"))]
#[tokio::test]
async fn test_tls_without_tls_feature_fails() {
    let Ok(host) = std::env::var("CLICKHOUSE_TLS_HOST") else {
        eprintln!("Skipping TLS test: set CLICKHOUSE_TLS_HOST");
        return;
    };
    let result = Client::connect(&host).await;
    assert!(
        result.is_err(),
        "connecting to TLS port without tls feature should fail"
    );
}
