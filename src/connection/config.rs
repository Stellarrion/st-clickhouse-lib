use crate::compression::CompressionMethod;
use crate::connection::query_packet::build_query_packet_template;
use crate::connection::tcp::Client;
#[cfg(feature = "tokio-tls")]
use crate::error::Result;
use crate::protocol::revision;
use std::time::Duration;

impl Client {
    /// Set a ClickHouse setting for subsequent queries.
    pub fn with_setting(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.insert(name.into(), value.into());
        self.refresh_query_template();
        self
    }

    /// Control Native JSON serialization for materialized query results.
    ///
    /// Enabled by default to match clickhouse-cpp. Pass `false` to opt back into
    /// ClickHouse's native JSON/Object serialization.
    pub fn with_native_json_as_string(self, enabled: bool) -> Self {
        self.with_setting(
            crate::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING,
            if enabled { "1" } else { "0" },
        )
    }

    /// Enable compression.
    pub fn with_compression(mut self, method: CompressionMethod) -> Self {
        self.compression = Some(method);
        self.refresh_query_template();
        self
    }

    /// Enable Ping/Pong before each query.
    pub fn with_ping_before_query(mut self, enabled: bool) -> Self {
        self.ping_before_query = enabled;
        self
    }

    /// Set the number of times to retry a query on failure.
    pub fn with_send_retries(mut self, n: u32) -> Self {
        self.send_retries = n.max(1);
        self
    }

    /// Set the delay between retry attempts.
    pub fn with_retry_timeout(mut self, t: Duration) -> Self {
        self.retry_timeout = t;
        self
    }

    /// Set connect timeout.
    pub fn with_connect_timeout(mut self, t: Duration) -> Self {
        self.connect_timeout = t;
        self.pool.set_connect_timeout(t);
        self
    }

    /// Set send timeout. Writes fail after this duration (default: 300s).
    pub fn with_send_timeout(mut self, t: Duration) -> Self {
        self.pool.set_send_timeout(Some(t));
        self
    }

    /// Set receive timeout.
    pub fn with_recv_timeout(mut self, t: Duration) -> Self {
        self.recv_timeout = t;
        self
    }

    /// Set a whole-query wall-clock timeout.
    ///
    /// When set, a query that has not fully completed (read through
    /// `EndOfStream`) within `t` is cancelled server-side and returns
    /// [`Error::Timeout`](crate::error::Error::Timeout). The connection is
    /// drained and returned to the pool alive. `None` by default.
    pub fn with_query_timeout(mut self, t: Duration) -> Self {
        self.query_timeout = Some(t);
        self
    }

    /// Attach a metrics sink shared with the connection pool.
    pub fn with_metrics(mut self, metrics: &'static crate::metrics::Metrics) -> Self {
        self.pool.with_metrics(metrics);
        self
    }

    /// Validate INSERT blocks against cached `DESCRIBE TABLE` metadata.
    pub fn with_schema_validation(mut self, enabled: bool) -> Self {
        self.validate_schema = enabled;
        self
    }

    pub(crate) fn refresh_query_template(&mut self) {
        self.query_template = build_query_packet_template(
            &self.settings,
            self.compression,
            revision::DEFAULT_PROTOCOL_REVISION,
        );
    }

    pub(crate) fn metrics(&self) -> Option<&'static crate::metrics::Metrics> {
        self.pool.metrics()
    }

    /// Set connection TTL. Connections older than this are recycled.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.pool.set_ttl(ttl);
        self
    }

    /// Set ClickHouse credentials.
    pub fn with_credentials(mut self, user: &str, password: &str) -> Self {
        self.pool.set_credentials(user, password);
        self
    }

    /// Set ClickHouse user (no password).
    pub fn with_user(mut self, user: &str) -> Self {
        self.pool.set_credentials(user, "");
        self
    }

    /// Enable TLS using the system certificate store.
    ///
    /// Existing pooled sockets are recycled on their next checkout so future
    /// handshakes use TLS. Prefer [`Client::connect_tls`] or
    /// [`Client::connect_tls_with_config`] when the first handshake must also be
    /// encrypted.
    #[cfg(feature = "tokio-tls")]
    pub fn with_tls(mut self, domain: &str) -> Result<Self> {
        // Use platform-native certificate store
        let mut root_store = rustls::RootCertStore::empty();
        let cert_result = rustls_native_certs::load_native_certs();
        if !cert_result.errors.is_empty() {
            // Log but don't fail — system may have partial certs
            eprintln!("rustls-native-certs warnings: {:?}", cert_result.errors);
        }
        for cert in cert_result.certs {
            let _ = root_store.add(cert);
        }
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        self.pool.set_tls(config, domain);
        Ok(self)
    }

    /// Enable TLS with a custom client config.
    ///
    /// Existing pooled sockets are recycled on their next checkout so future
    /// handshakes use TLS. Prefer [`Client::connect_tls_with_config`] when the
    /// first handshake must also be encrypted.
    #[cfg(feature = "tokio-tls")]
    pub fn with_tls_config(mut self, config: rustls::ClientConfig, domain: &str) -> Self {
        self.pool.set_tls(config, domain);
        self
    }
}
