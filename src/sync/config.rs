//! Connection configuration for the ClickHouse native protocol client.
//!
//! [`ClientConfig`] holds ALL connection parameters with sensible defaults.
//! Use the builder pattern to override only what you need:
//!
//! ```ignore
//! let config = ClientConfig::default()
//!     .with_host("clickhouse.example.com")
//!     .with_user("analytics")
//!     .with_password("secret")
//!     .with_compression(CompressionMethod::Lz4)
//!     .with_setting("max_block_size", "8192");
//! let client = SyncClient::connect_with_config(config)?;
//! ```

use std::collections::HashMap;
use std::fmt;
#[cfg_attr(not(feature = "tls"), allow(unused_imports))]
use std::sync::Arc;
use std::time::Duration;

use crate::sync::compression::CompressionMethod;
use crate::sync::protocol::handshake::SshSigner;
use crate::sync::protocol::revision;
use zeroize::Zeroize;
/// All connection parameters for a ClickHouse native protocol client.
///
/// Every field has a sensible default. Override with the builder methods
/// (`.with_host()`, `.with_user()`, etc.) or set fields directly.
#[derive(Clone)]
pub struct ClientConfig {
    /// Server hostname or IP address.
    pub host: String,
    /// Server TCP port (default: 9000 for native protocol).
    pub port: u16,
    /// ClickHouse user name.
    pub user: String,
    /// ClickHouse user password.
    pub password: String,
    /// Default database.
    pub database: String,
    /// Client name sent during handshake and in ClientInfo.
    pub client_name: String,
    /// OS user name sent in ClientInfo.
    pub os_user: String,
    /// Client hostname sent in ClientInfo.
    pub client_hostname: String,
    /// Client version major.
    pub client_version_major: u64,
    /// Client version minor.
    pub client_version_minor: u64,
    /// Client version patch.
    pub client_version_patch: u64,
    /// Protocol revision (defines which features the server enables).
    pub client_revision: u64,
    /// Timeout for each per-address connection attempt: TCP establishment
    /// plus the whole setup phase (TLS handshake, native handshake,
    /// addendum). Must be greater than zero; see
    /// [`SyncClient::connect_with_config`](crate::sync::SyncClient::connect_with_config).
    pub connect_timeout: Duration,
    /// Read timeout for queries (applied to the TCP stream).
    pub query_timeout: Duration,
    /// Compression method for data packets (None = no compression).
    pub compression: Option<CompressionMethod>,
    /// Session-level settings (key-value pairs sent with every query).
    pub settings: HashMap<String, String>,
    /// Quota key sent in the handshake addendum.
    pub quota_key: String,
    /// Chunked transfer modes (send, recv) — \"notchunked\" or \"chunked\".
    pub chunked_mode: (String, String),
    /// Parallel replicas version sent in the handshake addendum.
    pub parallel_replicas_version: u64,
    /// Initial query ID sent in every ClientInfo block.
    pub initial_query_id: String,
    /// Initial user sent in every ClientInfo block.
    pub initial_user: String,
    /// Initial address sent in every ClientInfo block.
    pub initial_address: String,
    /// Maximum response size before truncation (default: 256 MiB).
    pub max_response_size: usize,
    /// Size of the internal read buffer for streaming (default: 64 KiB).
    pub read_buffer_size: usize,
    /// Whether to send a Ping before the first query (CH 26.4+ doesn't need it).
    pub ping_before_query: bool,
    /// Validate insert blocks against cached `DESCRIBE TABLE` metadata.
    pub validate_schema: bool,
    /// Optional SSH-key authentication signer.
    pub ssh_signer: Option<SshSigner>,
    /// TLS client configuration (enables TLS when set).
    #[cfg(feature = "tls")]
    pub tls_config: Option<std::sync::Arc<rustls::ClientConfig>>,
    /// TLS server domain for SNI.
    #[cfg(feature = "tls")]
    pub tls_domain: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 9000,
            user: "default".to_string(),
            password: String::new(),
            database: String::new(),
            client_name: "st-clickhouse-sync".to_string(),
            os_user: String::new(),
            client_hostname: "localhost".to_string(),
            client_version_major: 26,
            client_version_minor: 4,
            client_version_patch: 1, // Match C++ CLICKHOUSE_CPP_VERSION_PATCH
            client_revision: revision::DEFAULT_PROTOCOL_REVISION,
            connect_timeout: Duration::from_secs(10),
            query_timeout: Duration::from_secs(30),
            compression: None,
            settings: HashMap::new(),
            quota_key: String::new(),
            chunked_mode: (
                "chunked_optional".to_string(),
                "chunked_optional".to_string(),
            ),
            parallel_replicas_version: 7,
            initial_query_id: String::new(),
            initial_user: String::new(),
            initial_address: "0.0.0.0:0".to_string(),
            max_response_size: 256 * 1024 * 1024,
            read_buffer_size: 65536,
            ping_before_query: false,
            validate_schema: false,
            ssh_signer: None,
            #[cfg(feature = "tls")]
            tls_config: None,
            #[cfg(feature = "tls")]
            tls_domain: String::new(),
        }
    }
}

impl ClientConfig {
    /// Build the `host:port` address string.
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Create a new config with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    // ── Builder methods ──

    pub fn with_host(mut self, host: &str) -> Self {
        self.host = host.to_string();
        self
    }
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
    pub fn with_user(mut self, user: &str) -> Self {
        self.user.zeroize();
        self.user = user.to_string();
        self
    }
    pub fn with_password(mut self, password: &str) -> Self {
        self.password.zeroize();
        self.password = password.to_string();
        self.ssh_signer = None;
        self
    }
    pub fn with_database(mut self, database: &str) -> Self {
        self.database = database.to_string();
        self
    }
    pub fn with_client_name(mut self, name: &str) -> Self {
        self.client_name = name.to_string();
        self
    }
    pub fn with_client_hostname(mut self, hostname: &str) -> Self {
        self.client_hostname = hostname.to_string();
        self
    }
    pub fn with_client_version(mut self, major: u64, minor: u64, patch: u64) -> Self {
        self.client_version_major = major;
        self.client_version_minor = minor;
        self.client_version_patch = patch;
        self
    }
    pub fn with_client_revision(mut self, rev: u64) -> Self {
        self.client_revision = rev;
        self
    }
    /// Set the connect timeout (see [`ClientConfig::connect_timeout`]).
    ///
    /// Each resolved address gets the full timeout for TCP establishment and
    /// connection setup; expiry returns
    /// [`Error::Timeout`](crate::sync::error::Error::Timeout) for setup or TCP
    /// stalls. `Duration::ZERO` is rejected with
    /// [`Error::Config`](crate::sync::error::Error::Config) at connect time.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }
    pub fn with_query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = timeout;
        self
    }
    pub fn with_compression(mut self, method: CompressionMethod) -> Self {
        self.compression = Some(method);
        self
    }
    pub fn with_setting(mut self, name: &str, value: &str) -> Self {
        self.settings.insert(name.to_string(), value.to_string());
        self
    }
    /// Control Native JSON serialization for materialized query results.
    ///
    /// Enabled by default to match clickhouse-cpp. Pass `false` to opt back into
    /// ClickHouse's native JSON/Object serialization.
    pub fn with_native_json_as_string(self, enabled: bool) -> Self {
        self.with_setting(
            crate::sync::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING,
            if enabled { "1" } else { "0" },
        )
    }
    pub fn with_max_response_size(mut self, size: usize) -> Self {
        self.max_response_size = size;
        self
    }
    pub fn with_read_buffer_size(mut self, size: usize) -> Self {
        self.read_buffer_size = size;
        self
    }
    pub fn with_initial_user(mut self, user: &str) -> Self {
        self.initial_user.zeroize();
        self.initial_user = user.to_string();
        self
    }
    pub fn with_initial_query_id(mut self, id: &str) -> Self {
        self.initial_query_id = id.to_string();
        self
    }
    pub fn with_initial_address(mut self, addr: &str) -> Self {
        self.initial_address = addr.to_string();
        self
    }
    pub fn with_quota_key(mut self, key: &str) -> Self {
        self.quota_key = key.to_string();
        self
    }
    pub fn with_chunked_mode(mut self, send: &str, recv: &str) -> Self {
        self.chunked_mode = (send.to_string(), recv.to_string());
        self
    }
    pub fn with_ping_before_query(mut self, ping: bool) -> Self {
        self.ping_before_query = ping;
        self
    }
    pub fn with_schema_validation(mut self, enabled: bool) -> Self {
        self.validate_schema = enabled;
        self
    }
    /// Enable ClickHouse SSH-key authentication.
    ///
    /// The signer receives the exact challenge payload ClickHouse expects:
    /// `protocol_revision + database + user + challenge`. It must return the
    /// signature string sent in `SSHChallengeResponse`.
    pub fn with_ssh_signer<F>(mut self, signer: F) -> Self
    where
        F: Fn(&[u8]) -> std::result::Result<String, String> + Send + Sync + 'static,
    {
        self.password.zeroize();
        self.password.clear();
        self.ssh_signer = Some(Arc::new(signer));
        self
    }
    /// Enable TLS with the given client config and domain.
    #[cfg(feature = "tls")]
    pub fn with_tls(mut self, config: rustls::ClientConfig, domain: &str) -> Self {
        self.tls_config = Some(Arc::new(config));
        self.tls_domain = domain.to_string();
        self
    }
}

impl fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("ClientConfig");
        debug
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .field("database", &self.database)
            .field("client_name", &self.client_name)
            .field("os_user", &self.os_user)
            .field("client_hostname", &self.client_hostname)
            .field("client_version_major", &self.client_version_major)
            .field("client_version_minor", &self.client_version_minor)
            .field("client_version_patch", &self.client_version_patch)
            .field("client_revision", &self.client_revision)
            .field("connect_timeout", &self.connect_timeout)
            .field("query_timeout", &self.query_timeout)
            .field("compression", &self.compression)
            .field("settings", &self.settings)
            .field("quota_key", &self.quota_key)
            .field("chunked_mode", &self.chunked_mode)
            .field("parallel_replicas_version", &self.parallel_replicas_version)
            .field("initial_query_id", &self.initial_query_id)
            .field("initial_user", &self.initial_user)
            .field("initial_address", &self.initial_address)
            .field("max_response_size", &self.max_response_size)
            .field("read_buffer_size", &self.read_buffer_size)
            .field("ping_before_query", &self.ping_before_query)
            .field("ssh_signer", &self.ssh_signer.is_some());
        #[cfg(feature = "tls")]
        debug
            .field("tls_config", &self.tls_config.is_some())
            .field("tls_domain", &self.tls_domain);
        debug.finish()
    }
}

impl Drop for ClientConfig {
    fn drop(&mut self) {
        self.user.zeroize();
        self.initial_user.zeroize();
        self.password.zeroize();
    }
}
