//! # Client builder
//!
//! The one way SignalDB's consumers (CLI, MCP server, integration tests)
//! construct a [`Client`]: it owns the underlying `reqwest::ClientBuilder`
//! (default headers, timeouts) and installs the [`RetryPolicy`], so
//! cross-cutting client policy is defined once here and cannot drift per
//! consumer (see `client-surface-parity`).
//!
//! ```rust,ignore
//! let client = signaldb_sdk::ClientBuilder::new("http://localhost:3000")
//!     .bearer("sk-acme-key")
//!     .tenant("acme")
//!     .dataset("production")
//!     .build()?;
//! ```

use std::time::Duration;

use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};

use crate::Client;
use crate::retry::RetryPolicy;

/// Default end-to-end request timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);
/// Default connect timeout.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Header carrying the tenant id.
pub const TENANT_HEADER: &str = "x-tenant-id";
/// Header carrying the dataset id.
pub const DATASET_HEADER: &str = "x-dataset-id";

/// Why a [`ClientBuilder`] could not produce a client.
#[derive(Debug, thiserror::Error)]
pub enum ClientBuildError {
    /// A header value (bearer token, tenant, dataset, custom header) is not a
    /// valid HTTP header value.
    #[error("invalid value for header `{header}`")]
    InvalidHeaderValue {
        /// The header whose value was rejected.
        header: String,
    },
    /// A header name is not a valid HTTP header name.
    #[error("invalid header name `{0}`")]
    InvalidHeaderName(String),
    /// The HTTP client itself could not be built.
    #[error("failed to build HTTP client: {0}")]
    Http(#[from] reqwest::Error),
}

/// Builder for a [`Client`] with SignalDB's default policy applied.
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    base_url: String,
    headers: Vec<(String, String)>,
    timeout: Duration,
    connect_timeout: Duration,
    retry: RetryPolicy,
}

impl ClientBuilder {
    /// Start a builder for the router at `base_url` (scheme, host, port; a
    /// trailing slash is trimmed because generated operation paths are
    /// absolute).
    pub fn new(base_url: impl AsRef<str>) -> Self {
        Self {
            base_url: base_url.as_ref().trim_end_matches('/').to_string(),
            headers: Vec::new(),
            timeout: DEFAULT_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            retry: RetryPolicy::default(),
        }
    }

    /// Send `Authorization: Bearer <token>` on every request.
    pub fn bearer(self, token: impl AsRef<str>) -> Self {
        self.header(AUTHORIZATION.as_str(), format!("Bearer {}", token.as_ref()))
    }

    /// Send `X-Tenant-ID` on every request.
    pub fn tenant(self, tenant: impl AsRef<str>) -> Self {
        self.header(TENANT_HEADER, tenant.as_ref())
    }

    /// Send `X-Dataset-ID` on every request.
    pub fn dataset(self, dataset: impl AsRef<str>) -> Self {
        self.header(DATASET_HEADER, dataset.as_ref())
    }

    /// Send an arbitrary default header on every request (later calls for
    /// the same name replace earlier ones).
    pub fn header(mut self, name: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        let name = name.as_ref().to_ascii_lowercase();
        self.headers.retain(|(n, _)| n != &name);
        self.headers.push((name, value.as_ref().to_string()));
        self
    }

    /// End-to-end timeout per request (default [`DEFAULT_TIMEOUT`]). Retries
    /// each get their own timeout; the retry policy's `total_cap` bounds the
    /// waits between them.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Connect timeout (default [`DEFAULT_CONNECT_TIMEOUT`]).
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// The retry policy (default [`RetryPolicy::default`]; use
    /// [`RetryPolicy::disabled`] for fail-fast).
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }

    /// The configured base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Build the client.
    ///
    /// # Errors
    ///
    /// Fails when a header name/value is invalid or the HTTP client cannot
    /// be constructed.
    pub fn build(self) -> Result<Client, ClientBuildError> {
        let mut headers = HeaderMap::with_capacity(self.headers.len());
        for (name, value) in &self.headers {
            let header_name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| ClientBuildError::InvalidHeaderName(name.clone()))?;
            let header_value =
                HeaderValue::from_str(value).map_err(|_| ClientBuildError::InvalidHeaderValue {
                    header: name.clone(),
                })?;
            headers.insert(header_name, header_value);
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(self.timeout)
            .connect_timeout(self.connect_timeout)
            .build()?;
        Ok(Client::new_with_client(&self.base_url, http, self.retry))
    }
}
