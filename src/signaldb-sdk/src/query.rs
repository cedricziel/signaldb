//! # Flight SQL query transport
//!
//! The generated HTTP client ([`crate::Client`]) covers the admin/management
//! surface and the PromQL/LogQL/TraceQL compat endpoints. SQL, however, is
//! served over Arrow Flight (gRPC), which OpenAPI cannot describe — so this
//! module is hand-written (see `design.md` D1). It is the single non-generated
//! piece of the SDK; everything else comes from `generated.rs`.
//!
//! [`QueryClient`] connects to the router's Flight endpoint, sends a SQL string
//! as the ticket, and decodes the response into Arrow [`RecordBatch`]es. Output
//! formatting (table/CSV/NDJSON) is the caller's concern, keeping this layer a
//! pure transport that the CLI and the MCP server share.

use std::time::Duration;

use arrow::record_batch::RecordBatch;
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use futures::{StreamExt, TryStreamExt};
use tonic::metadata::MetadataValue;
use tonic::transport::Endpoint;

/// Errors returned by the Flight SQL query transport.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// The provided Flight URL could not be parsed.
    #[error("invalid Flight URL '{url}': {source}")]
    InvalidUrl {
        url: String,
        source: tonic::transport::Error,
    },
    /// The connection to the Flight endpoint could not be established.
    #[error("failed to connect to SignalDB at {url}: {source}\n  Is the server running?")]
    Connect {
        url: String,
        source: tonic::transport::Error,
    },
    /// A tenant/dataset/authorization header value was not valid.
    #[error("invalid request metadata: {0}")]
    Metadata(String),
    /// The server rejected or failed the query. The message is already
    /// formatted for human consumption.
    #[error("{0}")]
    Server(String),
    /// The Flight response stream could not be decoded.
    #[error("failed to decode Flight response: {0}")]
    Decode(String),
}

/// A connection factory for SQL queries over the router's Arrow Flight
/// endpoint. Cheap to construct; a fresh channel is opened per [`sql`] call.
///
/// [`sql`]: QueryClient::sql
#[derive(Clone)]
pub struct QueryClient {
    flight_url: String,
    api_key: Option<String>,
    tenant_id: Option<String>,
    dataset_id: Option<String>,
    timeout: Duration,
    connect_timeout: Duration,
}

// Manual `Debug` so the API key is never printed (e.g. in a log line or a
// panic message); everything else is shown.
impl std::fmt::Debug for QueryClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryClient")
            .field("flight_url", &self.flight_url)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("tenant_id", &self.tenant_id)
            .field("dataset_id", &self.dataset_id)
            .field("timeout", &self.timeout)
            .field("connect_timeout", &self.connect_timeout)
            .finish()
    }
}

impl QueryClient {
    /// Create a client targeting the router Flight URL (e.g.
    /// `http://localhost:50053`).
    pub fn new(flight_url: impl Into<String>) -> Self {
        Self {
            flight_url: flight_url.into(),
            api_key: None,
            tenant_id: None,
            dataset_id: None,
            timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
        }
    }

    /// Set the API key sent as `authorization: Bearer <key>`.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Set the `x-tenant-id` header.
    pub fn with_tenant(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    /// Set the `x-dataset-id` header.
    pub fn with_dataset(mut self, dataset_id: impl Into<String>) -> Self {
        self.dataset_id = Some(dataset_id.into());
        self
    }

    /// Override the per-request timeout (default 30s).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Execute a SQL query and collect the full result as Arrow record batches.
    pub async fn sql(&self, sql: &str) -> Result<Vec<RecordBatch>, QueryError> {
        let endpoint = Endpoint::from_shared(self.flight_url.clone())
            .map_err(|source| QueryError::InvalidUrl {
                url: self.flight_url.clone(),
                source,
            })?
            .timeout(self.timeout)
            .connect_timeout(self.connect_timeout);

        let channel = endpoint
            .connect()
            .await
            .map_err(|source| QueryError::Connect {
                url: self.flight_url.clone(),
                source,
            })?;
        let mut client = FlightServiceClient::new(channel);

        let ticket = Ticket {
            ticket: sql.as_bytes().to_vec().into(),
        };
        let mut request = tonic::Request::new(ticket);
        if let Some(key) = &self.api_key {
            let value = MetadataValue::try_from(format!("Bearer {key}"))
                .map_err(|e| QueryError::Metadata(e.to_string()))?;
            request.metadata_mut().insert("authorization", value);
        }
        if let Some(tenant) = &self.tenant_id {
            let value = MetadataValue::try_from(tenant.as_str())
                .map_err(|e| QueryError::Metadata(e.to_string()))?;
            request.metadata_mut().insert("x-tenant-id", value);
        }
        if let Some(dataset) = &self.dataset_id {
            let value = MetadataValue::try_from(dataset.as_str())
                .map_err(|e| QueryError::Metadata(e.to_string()))?;
            request.metadata_mut().insert("x-dataset-id", value);
        }

        let response = client
            .do_get(request)
            .await
            .map_err(|status| QueryError::Server(format_flight_error(&status)))?;
        let flight_stream = response.into_inner();
        let mut batch_stream = FlightRecordBatchStream::new_from_flight_data(
            flight_stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

        let mut batches = Vec::new();
        while let Some(result) = batch_stream.next().await {
            batches.push(result.map_err(|e| QueryError::Decode(e.to_string()))?);
        }
        Ok(batches)
    }
}

/// Render a tonic `Status` from the Flight endpoint into an operator-facing
/// message. Mirrors the classification the CLI previously did inline.
fn format_flight_error(status: &tonic::Status) -> String {
    let msg = status.message().trim();
    match status.code() {
        tonic::Code::Unavailable => {
            format!("Service unavailable: {msg}\n  No query service is reachable.")
        }
        tonic::Code::Internal => {
            if let Some(detail) = msg.strip_prefix("Query execution failed: ") {
                format!("Query failed: {detail}")
            } else {
                format!("Server error: {msg}")
            }
        }
        tonic::Code::InvalidArgument => format!("Invalid query: {msg}"),
        tonic::Code::NotFound => format!("Not found: {msg}"),
        tonic::Code::Unauthenticated => {
            format!("Authentication required: {msg}")
        }
        tonic::Code::PermissionDenied => {
            format!("Permission denied: {msg}\n  Check your API key and tenant access.")
        }
        _ => format!("{}: {msg}", status.code()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_sets_context() {
        let c = QueryClient::new("http://localhost:50053")
            .with_api_key("k")
            .with_tenant("acme")
            .with_dataset("prod");
        assert_eq!(c.api_key.as_deref(), Some("k"));
        assert_eq!(c.tenant_id.as_deref(), Some("acme"));
        assert_eq!(c.dataset_id.as_deref(), Some("prod"));
    }

    #[test]
    fn invalid_url_is_reported() {
        // `Endpoint::from_shared` rejects a malformed scheme/URI.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let err = rt
            .block_on(QueryClient::new("not a url").sql("SELECT 1"))
            .unwrap_err();
        assert!(matches!(err, QueryError::InvalidUrl { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn unreachable_endpoint_reports_connect_error() {
        // Port 1 is not listenable; connect must fail deterministically.
        let err = QueryClient::new("http://127.0.0.1:1")
            .with_api_key("k")
            .with_tenant("t")
            .sql("SELECT 1")
            .await
            .unwrap_err();
        assert!(matches!(err, QueryError::Connect { .. }), "got {err:?}");
    }
}
