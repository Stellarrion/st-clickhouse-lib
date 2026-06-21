use crate::connection::callbacks::QueryCallbacks;
use crate::connection::query_packet::build_query_packet_template;
use crate::connection::tcp::Client;
use crate::error::Result;
use crate::protocol::revision;
use crate::runtime::sync::RwLock;
use crate::schema::TableSchema;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

impl Client {
    /// Create a configurable client builder.
    pub fn builder() -> crate::builder::ClientBuilder<crate::builder::Async> {
        crate::builder::ClientBuilder::<crate::builder::Async>::new()
    }

    /// Connect using a `clickhouse://` or `clickhouses://` URL.
    pub async fn connect_url(url: &str) -> Result<Self> {
        crate::builder::ClientBuilder::<crate::builder::Async>::from_url(url)?
            .connect()
            .await
    }

    pub(crate) fn from_pool(pool: crate::pool::SimplePool) -> Self {
        let quota_key = pool.quota_key().to_owned();
        Self {
            pool,
            settings: HashMap::new(),
            query_template: build_query_packet_template(
                &HashMap::new(),
                None,
                revision::DEFAULT_PROTOCOL_REVISION,
                &quota_key,
            ),
            compression: None,
            ping_before_query: false,
            callbacks: QueryCallbacks::default(),
            send_retries: 1,
            retry_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(30),
            recv_timeout: Duration::from_secs(300),
            query_timeout: None,
            schema_cache: Arc::new(RwLock::new(HashMap::<String, TableSchema>::new())),
            validate_schema: false,
        }
    }

    pub(crate) async fn new_connected(pool: crate::pool::SimplePool) -> Result<Self> {
        {
            let _guard = pool.get().await?;
        }
        Ok(Self::from_pool(pool))
    }

    fn pool_from_addrs(addrs: Vec<SocketAddr>, size: usize) -> Result<crate::pool::SimplePool> {
        if addrs.is_empty() {
            return Err(crate::error::Error::Protocol("no address resolved".into()));
        }
        Ok(crate::pool::SimplePool::new(addrs, size))
    }

    /// Connect with a single-connection pool (pool size 1).
    pub async fn connect(addr: impl crate::runtime::net::ToSocketAddrs) -> Result<Self> {
        Self::connect_with_credentials(addr, "default", "").await
    }

    /// Connect with a single-connection pool, storing the hostname for DNS refresh.
    pub async fn connect_with_hostname(
        hostname: &str, port: u16, user: &str, password: &str,
    ) -> Result<Self> {
        let addrs = resolve_all((hostname, port)).await?;
        let mut pool = Self::pool_from_addrs(addrs, 1)?;
        pool.set_credentials(user, password);
        pool.set_hostname(Some(format!("{hostname}:{port}")));
        Self::new_connected(pool).await
    }

    /// Connect with explicit ClickHouse credentials.
    pub async fn connect_with_credentials(
        addr: impl crate::runtime::net::ToSocketAddrs, user: &str, password: &str,
    ) -> Result<Self> {
        let addrs = resolve_all(addr).await?;
        let mut pool = Self::pool_from_addrs(addrs, 1)?;
        pool.set_credentials(user, password);
        Self::new_connected(pool).await
    }

    /// Connect over TLS with explicit ClickHouse credentials and a custom
    /// rustls client config.
    ///
    /// Unlike [`Client::with_tls_config`], TLS is configured before the initial
    /// handshake, so credentials and the first server hello are protected.
    #[cfg(feature = "tokio-tls")]
    pub async fn connect_tls_with_config(
        addr: impl crate::runtime::net::ToSocketAddrs, user: &str, password: &str,
        config: rustls::ClientConfig, domain: &str,
    ) -> Result<Self> {
        let addrs = resolve_all(addr).await?;
        let mut pool = Self::pool_from_addrs(addrs, 1)?;
        pool.set_credentials(user, password);
        pool.set_tls(config, domain);
        Self::new_connected(pool).await
    }

    /// Connect over TLS using the system certificate store.
    #[cfg(feature = "tokio-tls")]
    pub async fn connect_tls(
        addr: impl crate::runtime::net::ToSocketAddrs, domain: &str,
    ) -> Result<Self> {
        let mut root_store = rustls::RootCertStore::empty();
        let cert_result = rustls_native_certs::load_native_certs();
        if !cert_result.errors.is_empty() {
            eprintln!("rustls-native-certs warnings: {:?}", cert_result.errors);
        }
        for cert in cert_result.certs {
            let _ = root_store.add(cert);
        }
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        Self::connect_tls_with_config(addr, "default", "", config, domain).await
    }

    /// Connect using ClickHouse SSH-key authentication.
    ///
    /// The signer receives the exact challenge payload ClickHouse expects:
    /// `protocol_revision + database + user + challenge`. It must return the
    /// signature string to send in `SSHChallengeResponse`.
    pub async fn connect_with_ssh_signer<F>(
        addr: impl crate::runtime::net::ToSocketAddrs, user: &str, signer: F,
    ) -> Result<Self>
    where
        F: Fn(&[u8]) -> std::result::Result<String, String> + Send + Sync + 'static,
    {
        let addrs = resolve_all(addr).await?;
        let mut pool = Self::pool_from_addrs(addrs, 1)?;
        pool.set_ssh_signer(user, Arc::new(signer));
        Self::new_connected(pool).await
    }

    /// Connect with a pool of `size` connections.
    pub async fn connect_with_pool(
        addr: impl crate::runtime::net::ToSocketAddrs, size: usize,
    ) -> Result<Self> {
        let addrs = resolve_all(addr).await?;
        let pool = Self::pool_from_addrs(addrs, size)?;
        Self::new_connected(pool).await
    }

    /// Connect with a pool of `size` connections and explicit credentials.
    pub async fn connect_with_pool_credentials(
        addr: impl crate::runtime::net::ToSocketAddrs, size: usize, user: &str, password: &str,
    ) -> Result<Self> {
        let addrs = resolve_all(addr).await?;
        let mut pool = Self::pool_from_addrs(addrs, size)?;
        pool.set_credentials(user, password);
        Self::new_connected(pool).await
    }

    /// Eagerly connect all pool slots.
    pub async fn warmup(&self) -> Result<()> {
        for _ in 0..self.pool.slot_count() {
            let g = self.pool.get().await?;
            drop(g);
        }
        Ok(())
    }
}

/// Resolve all IP addresses for a given `host:port`.
async fn resolve_all(addr: impl crate::runtime::net::ToSocketAddrs) -> Result<Vec<SocketAddr>> {
    Ok(crate::runtime::net::lookup_host(addr)
        .await?
        .collect::<Vec<_>>())
}

/// Resolve a single IP address — legacy wrapper.
#[allow(dead_code)]
async fn resolve_one(addr: impl crate::runtime::net::ToSocketAddrs) -> Result<SocketAddr> {
    let mut addrs = crate::runtime::net::lookup_host(addr).await?;
    addrs
        .next()
        .ok_or_else(|| crate::error::Error::Protocol("no address resolved".into()))
}
