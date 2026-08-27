use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::Duration;

use crate::compression::CompressionMethod;

/// Async client builder mode.
#[derive(Debug, Clone, Copy)]
pub struct Async;

/// Blocking sync client builder mode.
#[derive(Debug, Clone, Copy)]
pub struct Blocking;

/// Unified ClickHouse client builder.
///
/// Use `Client::builder()` for the async Tokio-backed client (requires the
/// `tokio` feature, enabled by default) or
/// [`SyncClient::builder`](crate::sync::SyncClient::builder) for the blocking
/// client.
#[derive(Debug, Clone)]
pub struct ClientBuilder<M = Async> {
    opts: BuilderOptions,
    _mode: PhantomData<M>,
}

#[derive(Clone)]
struct BuilderOptions {
    hosts: Vec<HostEndpoint>,
    user: String,
    password: String,
    database: String,
    quota_key: String,
    pool_size: usize,
    compression: Option<CompressionMethod>,
    settings: HashMap<String, String>,
    connect_timeout: Option<Duration>,
    recv_timeout: Option<Duration>,
    send_timeout: Option<Duration>,
    retry_timeout: Option<Duration>,
    query_timeout: Option<Duration>,
    acquire_timeout: Option<Duration>,
    send_retries: Option<u32>,
    ping_before_query: bool,
    validate_schema: bool,
    secure: bool,
    tls_domain: Option<String>,
}

impl std::fmt::Debug for BuilderOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never emit the cleartext password (mirrors sync config.rs).
        f.debug_struct("BuilderOptions")
            .field("hosts", &self.hosts)
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .field("database", &self.database)
            .field("quota_key", &self.quota_key)
            .field("pool_size", &self.pool_size)
            .field("compression", &self.compression)
            .field("settings", &self.settings)
            .field("connect_timeout", &self.connect_timeout)
            .field("recv_timeout", &self.recv_timeout)
            .field("send_timeout", &self.send_timeout)
            .field("retry_timeout", &self.retry_timeout)
            .field("query_timeout", &self.query_timeout)
            .field("acquire_timeout", &self.acquire_timeout)
            .field("send_retries", &self.send_retries)
            .field("ping_before_query", &self.ping_before_query)
            .field("validate_schema", &self.validate_schema)
            .field("secure", &self.secure)
            .field("tls_domain", &self.tls_domain)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HostEndpoint {
    host: String,
    port: u16,
}

#[cfg(feature = "tokio")]
impl HostEndpoint {
    fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl Default for BuilderOptions {
    fn default() -> Self {
        Self {
            hosts: vec![HostEndpoint {
                host: "127.0.0.1".to_owned(),
                port: 9000,
            }],
            user: "default".to_owned(),
            password: String::new(),
            database: String::new(),
            quota_key: String::new(),
            pool_size: 1,
            compression: None,
            settings: HashMap::new(),
            connect_timeout: None,
            recv_timeout: None,
            send_timeout: None,
            retry_timeout: None,
            query_timeout: None,
            acquire_timeout: None,
            send_retries: None,
            ping_before_query: false,
            validate_schema: false,
            secure: false,
            tls_domain: None,
        }
    }
}

impl<M> ClientBuilder<M> {
    pub fn host(mut self, host: impl Into<String>) -> Self {
        let default_port = self.primary_port();
        self.opts.hosts = vec![
            parse_host_endpoint_lossy(&host.into(), default_port).unwrap_or(HostEndpoint {
                host: "127.0.0.1".to_owned(),
                port: default_port,
            }),
        ];
        self
    }

    pub fn hosts<I, S>(mut self, hosts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let default_port = self.primary_port();
        self.opts.hosts = hosts
            .into_iter()
            .filter_map(|host| parse_host_endpoint_lossy(&host.into(), default_port))
            .collect::<Vec<_>>();
        if self.opts.hosts.is_empty() {
            self.opts.hosts = BuilderOptions::default().hosts;
        }
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        if self.opts.hosts.len() == 1 {
            self.opts.hosts[0].port = port;
        } else {
            for host in &mut self.opts.hosts {
                if host.port == 9000 {
                    host.port = port;
                }
            }
        }
        self
    }

    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.opts.user = user.into();
        self
    }

    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.opts.password = password.into();
        self
    }

    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.opts.database = database.into();
        self
    }

    /// Set the quota key sent in ClientInfo and the handshake addendum.
    pub fn quota_key(mut self, key: impl Into<String>) -> Self {
        self.opts.quota_key = key.into();
        self
    }

    pub fn pool_size(mut self, size: usize) -> Self {
        self.opts.pool_size = size.max(1);
        self
    }

    pub fn compression(mut self, method: CompressionMethod) -> Self {
        self.opts.compression = Some(method);
        self
    }

    pub fn setting(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.opts.settings.insert(name.into(), value.into());
        self
    }

    pub fn native_json_as_string(self, enabled: bool) -> Self {
        self.setting(
            crate::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING,
            if enabled { "1" } else { "0" },
        )
    }

    /// Set the connect timeout for each per-address connection attempt
    /// (TCP + TLS + native handshake + ping). Also accepted as the URL option
    /// `?connect_timeout=`. `Duration::ZERO` is rejected at connect time.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.opts.connect_timeout = Some(timeout);
        self
    }

    pub fn recv_timeout(mut self, timeout: Duration) -> Self {
        self.opts.recv_timeout = Some(timeout);
        self
    }

    pub fn send_timeout(mut self, timeout: Duration) -> Self {
        self.opts.send_timeout = Some(timeout);
        self
    }

    pub fn retry_timeout(mut self, timeout: Duration) -> Self {
        self.opts.retry_timeout = Some(timeout);
        self
    }

    pub fn query_timeout(mut self, timeout: Duration) -> Self {
        self.opts.query_timeout = Some(timeout);
        self
    }

    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.opts.acquire_timeout = Some(timeout);
        self
    }

    pub fn send_retries(mut self, retries: u32) -> Self {
        self.opts.send_retries = Some(retries.max(1));
        self
    }

    pub fn ping_before_query(mut self, enabled: bool) -> Self {
        self.opts.ping_before_query = enabled;
        self
    }

    pub fn schema_validation(mut self, enabled: bool) -> Self {
        self.opts.validate_schema = enabled;
        self
    }

    pub fn secure(mut self, enabled: bool) -> Self {
        self.opts.secure = enabled;
        self
    }

    pub fn tls_domain(mut self, domain: impl Into<String>) -> Self {
        self.opts.tls_domain = Some(domain.into());
        self
    }

    pub fn from_url(url: &str) -> crate::Result<Self> {
        let opts = parse_clickhouse_url(url).map_err(crate::Error::Config)?;
        Ok(Self {
            opts,
            _mode: PhantomData,
        })
    }

    #[cfg(feature = "tokio")]
    fn addrs(&self) -> Vec<String> {
        self.opts.hosts.iter().map(HostEndpoint::addr).collect()
    }

    fn primary_host(&self) -> &str {
        self.opts
            .hosts
            .first()
            .map(|host| host.host.as_str())
            .unwrap_or("127.0.0.1")
    }

    fn primary_port(&self) -> u16 {
        self.opts.hosts.first().map_or(9000, |host| host.port)
    }
}

impl ClientBuilder<Async> {
    pub fn new() -> Self {
        Self {
            opts: BuilderOptions::default(),
            _mode: PhantomData,
        }
    }

    #[cfg(feature = "tokio")]
    pub async fn connect(self) -> crate::Result<crate::Client> {
        let logical_addrs = self.addrs();
        let mut addrs = Vec::new();
        for addr in &logical_addrs {
            addrs.extend(
                crate::runtime::net::lookup_host(addr)
                    .await
                    .map_err(crate::Error::Io)?,
            );
        }
        if addrs.is_empty() {
            return Err(crate::Error::Config(format!(
                "no addresses resolved for {}",
                logical_addrs.join(",")
            )));
        }

        let mut pool = crate::pool::SimplePool::new(addrs, self.opts.pool_size);
        pool.set_credentials(&self.opts.user, &self.opts.password);
        pool.set_database(&self.opts.database);
        pool.set_quota_key(&self.opts.quota_key);
        if logical_addrs.len() == 1 {
            pool.set_hostname(logical_addrs.into_iter().next());
        }
        if let Some(timeout) = self.opts.connect_timeout {
            pool.set_connect_timeout(timeout);
        }
        if let Some(timeout) = self.opts.send_timeout {
            pool.set_send_timeout(Some(timeout));
        }
        if let Some(timeout) = self.opts.acquire_timeout {
            pool.set_acquire_timeout(Some(timeout));
        }
        #[cfg(feature = "tokio-tls")]
        if self.opts.secure {
            let domain = self.tls_server_name();
            pool.set_tls(default_rustls_config(), &domain);
        }
        #[cfg(not(feature = "tokio-tls"))]
        if self.opts.secure {
            return Err(crate::Error::Config(
                "secure ClickHouse URL requires the 'tokio-tls' feature".into(),
            ));
        }

        let mut client = crate::Client::new_connected(pool).await?;
        client.settings = self.opts.settings;
        client.compression = self.opts.compression;
        client.ping_before_query = self.opts.ping_before_query;
        client.validate_schema = self.opts.validate_schema;
        if let Some(timeout) = self.opts.recv_timeout {
            client.recv_timeout = timeout;
        }
        if let Some(timeout) = self.opts.retry_timeout {
            client.retry_timeout = timeout;
        }
        if let Some(timeout) = self.opts.query_timeout {
            client.query_timeout = Some(timeout);
        }
        if let Some(retries) = self.opts.send_retries {
            client.send_retries = retries;
        }
        client.refresh_query_template();
        Ok(client)
    }

    #[cfg(feature = "tokio-tls")]
    fn tls_server_name(&self) -> String {
        self.opts
            .tls_domain
            .clone()
            .unwrap_or_else(|| self.primary_host().to_owned())
    }
}

impl Default for ClientBuilder<Async> {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBuilder<Blocking> {
    pub fn new() -> Self {
        Self {
            opts: BuilderOptions::default(),
            _mode: PhantomData,
        }
    }

    pub fn connect(self) -> crate::sync::Result<crate::sync::SyncClient> {
        let mut config = crate::sync::ClientConfig::new()
            .with_host(self.primary_host())
            .with_port(self.primary_port())
            .with_user(&self.opts.user)
            .with_password(&self.opts.password)
            .with_database(&self.opts.database)
            .with_ping_before_query(self.opts.ping_before_query)
            .with_schema_validation(self.opts.validate_schema);
        if let Some(timeout) = self.opts.connect_timeout {
            config = config.with_connect_timeout(timeout);
        }
        if let Some(timeout) = self.opts.recv_timeout {
            config = config.with_query_timeout(timeout);
        }
        if let Some(method) = self.opts.compression {
            config = config.with_compression(to_sync_compression(method));
        }
        for (name, value) in &self.opts.settings {
            config = config.with_setting(name, value);
        }
        #[cfg(feature = "tls")]
        if self.opts.secure {
            let domain = self
                .opts
                .tls_domain
                .as_deref()
                .unwrap_or(self.primary_host());
            config = config.with_tls(default_rustls_config(), domain);
        }
        #[cfg(not(feature = "tls"))]
        if self.opts.secure {
            return Err(crate::sync::Error::Protocol(
                "secure ClickHouse URL requires the 'tls' feature".into(),
            ));
        }
        crate::sync::SyncClient::connect_with_config(config)
    }
}

impl Default for ClientBuilder<Blocking> {
    fn default() -> Self {
        Self::new()
    }
}

fn to_sync_compression(method: CompressionMethod) -> crate::sync::compression::CompressionMethod {
    match method {
        CompressionMethod::None => crate::sync::compression::CompressionMethod::None,
        CompressionMethod::Lz4 => crate::sync::compression::CompressionMethod::Lz4,
        CompressionMethod::Zstd => crate::sync::compression::CompressionMethod::Zstd,
    }
}

fn parse_clickhouse_url(url: &str) -> std::result::Result<BuilderOptions, String> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| "ClickHouse URL must include a scheme".to_owned())?;
    let secure = match scheme {
        "clickhouse" | "clickhouse+native" => false,
        "clickhouses" | "clickhouse+tls" | "clickhouse+native+tls" => true,
        _ => return Err(format!("unsupported ClickHouse URL scheme '{scheme}'")),
    };
    let mut opts = BuilderOptions {
        secure,
        ..BuilderOptions::default()
    };

    let (without_query, query) = split_once_char(rest, '?');
    let (authority, path) = split_once_char(without_query, '/');
    parse_authority(authority, &mut opts)?;
    if !path.is_empty() {
        opts.database = percent_decode(path)?;
    }
    parse_query(query, &mut opts)?;
    Ok(opts)
}

fn split_once_char(input: &str, needle: char) -> (&str, &str) {
    input
        .split_once(needle)
        .map_or((input, ""), |(left, right)| (left, right))
}

fn parse_authority(authority: &str, opts: &mut BuilderOptions) -> std::result::Result<(), String> {
    if authority.is_empty() {
        return Err("ClickHouse URL host is empty".into());
    }
    let (userinfo, host_port) = authority
        .rsplit_once('@')
        .map_or(("", authority), |(left, right)| (left, right));
    if !userinfo.is_empty() {
        let (user, password) = split_once_char(userinfo, ':');
        opts.user = percent_decode(user)?;
        opts.password = percent_decode(password)?;
    }
    opts.hosts = host_port
        .split(',')
        .map(str::trim)
        .filter(|host| !host.is_empty())
        .map(|host| parse_host_endpoint(host, 9000))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if opts.hosts.is_empty() {
        return Err("ClickHouse URL host is empty".into());
    }
    Ok(())
}

fn parse_host_endpoint_lossy(input: &str, default_port: u16) -> Option<HostEndpoint> {
    parse_host_endpoint(input, default_port).ok().or_else(|| {
        let host = input.trim();
        (!host.is_empty()).then(|| HostEndpoint {
            host: host.to_owned(),
            port: default_port,
        })
    })
}

fn parse_host_endpoint(
    input: &str, default_port: u16,
) -> std::result::Result<HostEndpoint, String> {
    if input.is_empty() {
        return Err("ClickHouse host is empty".into());
    }
    if let Some(rest) = input.strip_prefix('[') {
        let (host, after_host) = rest
            .split_once(']')
            .ok_or_else(|| "unterminated IPv6 host".to_owned())?;
        let port = if let Some(port) = after_host.strip_prefix(':') {
            parse_port(port)?
        } else {
            default_port
        };
        return Ok(HostEndpoint {
            host: host.to_owned(),
            port,
        });
    }
    let (host, port) = input
        .rsplit_once(':')
        .map_or((input, ""), |(host, port)| (host, port));
    if host.is_empty() {
        return Err("ClickHouse host is empty".into());
    }
    Ok(HostEndpoint {
        host: host.to_owned(),
        port: if port.is_empty() {
            default_port
        } else {
            parse_port(port)?
        },
    })
}

fn parse_port(port: &str) -> std::result::Result<u16, String> {
    port.parse::<u16>()
        .map_err(|_| format!("invalid ClickHouse URL port '{port}'"))
}

fn parse_query(query: &str, opts: &mut BuilderOptions) -> std::result::Result<(), String> {
    if query.is_empty() {
        return Ok(());
    }
    for pair in query.split('&').filter(|s| !s.is_empty()) {
        let (raw_name, raw_value) = split_once_char(pair, '=');
        let name = percent_decode(raw_name)?;
        let value = percent_decode(raw_value)?;
        match name.as_str() {
            "user" => opts.user = value,
            "password" => opts.password = value,
            "database" | "db" => opts.database = value,
            "quota_key" => opts.quota_key = value,
            "compression" => opts.compression = Some(parse_compression(&value)?),
            "secure" | "tls" => opts.secure = parse_bool(&value)?,
            "tls_domain" | "sni" => opts.tls_domain = Some(value),
            "pool_size" => {
                opts.pool_size = value
                    .parse::<usize>()
                    .map_err(|_| format!("invalid pool_size '{value}'"))?
                    .max(1);
            },
            "connect_timeout" => opts.connect_timeout = Some(parse_duration(&value)?),
            "recv_timeout" | "query_timeout" => opts.recv_timeout = Some(parse_duration(&value)?),
            "send_timeout" => opts.send_timeout = Some(parse_duration(&value)?),
            "acquire_timeout" => opts.acquire_timeout = Some(parse_duration(&value)?),
            "retry_timeout" => opts.retry_timeout = Some(parse_duration(&value)?),
            "send_retries" => {
                opts.send_retries = Some(
                    value
                        .parse::<u32>()
                        .map_err(|_| format!("invalid send_retries '{value}'"))?
                        .max(1),
                );
            },
            "ping_before_query" => opts.ping_before_query = parse_bool(&value)?,
            "schema_validation" | "validate_schema" => opts.validate_schema = parse_bool(&value)?,
            _ => {
                opts.settings.insert(name, value);
            },
        }
    }
    Ok(())
}

fn parse_bool(value: &str) -> std::result::Result<bool, String> {
    match value {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!("invalid boolean value '{value}'")),
    }
}

fn parse_compression(value: &str) -> std::result::Result<CompressionMethod, String> {
    match value {
        "none" | "off" | "0" => Ok(CompressionMethod::None),
        "lz4" => Ok(CompressionMethod::Lz4),
        "zstd" => Ok(CompressionMethod::Zstd),
        _ => Err(format!("unsupported compression '{value}'")),
    }
}

fn parse_duration(value: &str) -> std::result::Result<Duration, String> {
    let (number, multiplier) = if let Some(n) = value.strip_suffix("ms") {
        (n, 1)
    } else if let Some(n) = value.strip_suffix('s') {
        (n, 1_000)
    } else if let Some(n) = value.strip_suffix('m') {
        (n, 60_000)
    } else {
        (value, 1_000)
    };
    let n = number
        .parse::<u64>()
        .map_err(|_| format!("invalid duration '{value}'"))?;
    Ok(Duration::from_millis(n.saturating_mul(multiplier)))
}

fn percent_decode(input: &str) -> std::result::Result<String, String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(format!("invalid percent escape in '{input}'"));
            }
            let hi = hex_val(bytes[i + 1])
                .ok_or_else(|| format!("invalid percent escape in '{input}'"))?;
            let lo = hex_val(bytes[i + 2])
                .ok_or_else(|| format!("invalid percent escape in '{input}'"))?;
            out.push((hi << 4) | lo);
            i += 3;
        } else if bytes[i] == b'+' {
            out.push(b' ');
            i += 1;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| format!("invalid UTF-8 in percent-decoded '{input}'"))
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(10 + b - b'a'),
        b'A'..=b'F' => Some(10 + b - b'A'),
        _ => None,
    }
}

#[cfg(any(feature = "tls", feature = "tokio-tls"))]
fn default_rustls_config() -> rustls::ClientConfig {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    // Also honor the OS trust store so internal/private CAs — accepted by
    // Client::with_tls() — validate consistently under clickhouses:// / secure.
    #[cfg(feature = "tokio-tls")]
    for cert in rustls_native_certs::load_native_certs().certs {
        let _ = root_store.add(cert);
    }
    rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_password() {
        let builder = ClientBuilder::<Async>::default().password("hunter2-secret");
        let s = format!("{:?}", builder);
        assert!(!s.contains("hunter2-secret"), "Debug leaked password: {s}");
        assert!(
            s.contains("<redacted>"),
            "password should be redacted in Debug: {s}"
        );
    }

    #[test]
    fn parses_clickhouse_url() {
        let builder = ClientBuilder::<Async>::from_url(
            "clickhouses://alice:s3%20cret@ch-1.example.com:9440,ch-2.example.com:9441/analytics?compression=lz4&pool_size=4&max_block_size=1000",
        )
        .expect("url should parse");
        assert_eq!(
            builder.opts.hosts,
            vec![
                HostEndpoint {
                    host: "ch-1.example.com".to_owned(),
                    port: 9440,
                },
                HostEndpoint {
                    host: "ch-2.example.com".to_owned(),
                    port: 9441,
                },
            ]
        );
        assert_eq!(builder.opts.user, "alice");
        assert_eq!(builder.opts.password, "s3 cret");
        assert_eq!(builder.opts.database, "analytics");
        assert_eq!(builder.opts.pool_size, 4);
        assert_eq!(builder.opts.compression, Some(CompressionMethod::Lz4));
        assert_eq!(
            builder
                .opts
                .settings
                .get("max_block_size")
                .map(String::as_str),
            Some("1000")
        );
        assert!(builder.opts.secure);
    }
}

#[cfg(test)]
mod query_timeout_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn builder_stores_query_timeout() {
        let b = ClientBuilder::<Async>::new().query_timeout(Duration::from_secs(12));
        assert_eq!(b.opts.query_timeout, Some(Duration::from_secs(12)));
    }

    #[test]
    fn builder_default_has_no_query_timeout() {
        let b = ClientBuilder::<Async>::new();
        assert_eq!(b.opts.query_timeout, None);
    }
}

#[cfg(test)]
mod acquire_timeout_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn builder_stores_acquire_timeout() {
        let b = ClientBuilder::<Async>::new().acquire_timeout(Duration::from_millis(250));
        assert_eq!(b.opts.acquire_timeout, Some(Duration::from_millis(250)));
    }

    #[test]
    fn builder_default_has_no_acquire_timeout() {
        let b = ClientBuilder::<Async>::new();
        assert_eq!(b.opts.acquire_timeout, None);
    }

    #[test]
    fn url_parses_acquire_timeout() {
        let b = ClientBuilder::<Async>::from_url(
            "clickhouse://honne:honne@127.0.0.1:9000?acquire_timeout=50ms",
        )
        .expect("url should parse");
        assert_eq!(b.opts.acquire_timeout, Some(Duration::from_millis(50)));
    }
}

#[cfg(test)]
mod quota_key_tests {
    use super::*;

    #[test]
    fn builder_stores_quota_key() {
        let b = ClientBuilder::<Async>::new().quota_key("tenant-42");
        assert_eq!(b.opts.quota_key, "tenant-42");
    }

    #[test]
    fn builder_default_has_empty_quota_key() {
        let b = ClientBuilder::<Async>::new();
        assert!(b.opts.quota_key.is_empty());
    }

    #[test]
    fn url_parses_quota_key() {
        let b = ClientBuilder::<Async>::from_url(
            "clickhouse://honne:honne@127.0.0.1:9000?quota_key=tenant-42",
        )
        .expect("url should parse");
        assert_eq!(b.opts.quota_key, "tenant-42");
    }
}
