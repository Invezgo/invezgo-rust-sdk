//! Client configuration and the main entry point.

use std::sync::Arc;
use std::time::Duration;

use crate::error::InvezgoError;
use crate::transport::Transport;

/// Default API base URL.
pub const DEFAULT_BASE_URL: &str = "https://api.invezgo.com";

/// L1 in-memory LRU cache settings. Only GET endpoints are cached; the raw body is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheConfig {
    /// TTL for normal endpoints (default 60 s). `Duration::ZERO` disables this tier.
    pub ttl: Duration,
    /// TTL for `[REALTIME]` endpoints (default 5 s). `Duration::ZERO` disables this tier.
    pub realtime_ttl: Duration,
    /// Maximum number of cached responses (default 1000).
    pub max_entries: usize,
    /// Maximum total size of cached bodies in bytes (default 32 MiB).
    pub max_bytes: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(60),
            realtime_ttl: Duration::from_secs(5),
            max_entries: 1000,
            max_bytes: 32 * 1024 * 1024,
        }
    }
}

/// Invezgo API client. Cheap to clone (shares the HTTP pool, cache and key state).
///
/// ```no_run
/// # async fn run() -> Result<(), invezgo_sdk::InvezgoError> {
/// let client = invezgo_sdk::InvezgoClient::new("YOUR_API_KEY")?;
/// let stocks = client.analysis().get_stock_list().await?;
/// println!("{} stocks", stocks.len());
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct InvezgoClient {
    inner: Arc<Transport>,
}

impl std::fmt::Debug for InvezgoClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // never print API keys
        f.debug_struct("InvezgoClient")
            .field("base_url", &self.inner.base_url)
            .field("api_keys", &self.inner.keys.len())
            .field("cache", &self.inner.cache.as_ref().map(|(c, _)| c))
            .finish()
    }
}

impl InvezgoClient {
    /// Client with one API key and default settings (cache on).
    pub fn new(api_key: impl Into<String>) -> Result<Self, InvezgoError> {
        Self::builder().api_key(api_key).build()
    }

    /// Start configuring a client.
    pub fn builder() -> InvezgoClientBuilder {
        InvezgoClientBuilder::default()
    }

    /// Drop every cached response.
    pub fn clear_cache(&self) {
        self.inner.clear_cache();
    }

    pub(crate) fn transport(&self) -> &Transport {
        &self.inner
    }
}

/// Builder for [`InvezgoClient`].
#[derive(Debug)]
pub struct InvezgoClientBuilder {
    api_key: Option<String>,
    additional_api_keys: Vec<String>,
    base_url: String,
    timeout: Duration,
    cache: Option<CacheConfig>,
}

impl Default for InvezgoClientBuilder {
    fn default() -> Self {
        Self {
            api_key: None,
            additional_api_keys: Vec::new(),
            base_url: DEFAULT_BASE_URL.to_string(),
            timeout: Duration::from_secs(30),
            cache: Some(CacheConfig::default()),
        }
    }
}

impl InvezgoClientBuilder {
    /// Primary API key (required).
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    /// Fallback keys, tried in order when a key gets HTTP 429.
    pub fn additional_api_keys<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.additional_api_keys = keys.into_iter().map(Into::into).collect();
        self
    }

    /// Override the API base URL (default `https://api.invezgo.com`).
    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    /// Timeout per HTTP attempt (default 30 s).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Custom cache settings (the cache is on by default).
    pub fn cache(mut self, config: CacheConfig) -> Self {
        self.cache = Some(config);
        self
    }

    /// Disable the response cache.
    pub fn no_cache(mut self) -> Self {
        self.cache = None;
        self
    }

    /// Build the client. Fails when the primary API key is missing or empty.
    pub fn build(self) -> Result<InvezgoClient, InvezgoError> {
        let primary = self.api_key.unwrap_or_default();
        if primary.trim().is_empty() {
            return Err(InvezgoError::Config("api_key is required".into()));
        }
        let mut keys: Vec<String> = Vec::new();
        for k in std::iter::once(primary).chain(self.additional_api_keys) {
            let k = k.trim().to_string();
            if !k.is_empty() && !keys.contains(&k) {
                keys.push(k);
            }
        }
        let http = reqwest::Client::builder()
            .timeout(self.timeout)
            .user_agent(concat!("invezgo-rust-sdk/", env!("CARGO_PKG_VERSION")))
            .build()?;
        let base_url = self.base_url.trim_end_matches('/').to_string();
        Ok(InvezgoClient { inner: Arc::new(Transport::new(http, base_url, keys, self.cache)) })
    }
}
