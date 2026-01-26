mod conversion;
mod flight_client;

use flight_client::{AuthContext, SignalDBFlightClient};
use grafana_plugin_sdk::{backend, data, prelude::*};
use serde::Deserialize;
use std::sync::Arc;
use thiserror::Error;

/// Default Flight service URL
const DEFAULT_FLIGHT_URL: &str = "http://localhost:50053";
/// Default query timeout in seconds
const DEFAULT_TIMEOUT_SECS: u32 = 30;

/// Configuration from frontend datasource settings (JSON data).
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DataSourceConfig {
    /// Router URL for Flight service (e.g., "http://localhost:50053")
    pub router_url: Option<String>,
    /// Protocol to use (currently only "flight" is supported)
    pub protocol: Option<String>,
    /// Query timeout in seconds
    pub timeout: Option<u32>,
    /// Tenant ID for multi-tenancy
    pub tenant_id: Option<String>,
    /// Dataset ID for data isolation
    pub dataset_id: Option<String>,
}

/// Secure JSON data (decrypted by Grafana before passing to plugin).
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SignalDBSecureJsonData {
    /// API key for authentication
    pub api_key: Option<String>,
}

/// SignalDB datasource plugin for Grafana
#[derive(Clone, Debug, Default, GrafanaPlugin)]
#[grafana_plugin(
    plugin_type = "datasource",
    json_data = DataSourceConfig,
    secure_json_data = SignalDBSecureJsonData
)]
pub struct SignalDBDataSource {
    /// Router URL (e.g., "http://localhost:3001" for HTTP or "http://localhost:50053" for Flight)
    router_url: Arc<str>,
    /// Query timeout in seconds
    timeout_secs: u32,
}

/// Query error type
#[derive(Debug, Error)]
#[error("Error querying SignalDB for query {}", .ref_id)]
pub struct QueryError {
    ref_id: String,
    #[source]
    source: anyhow::Error,
}

impl backend::DataQueryError for QueryError {
    fn ref_id(self) -> String {
        self.ref_id
    }
}

/// Query definition matching frontend types
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalDBQuery {
    signal_type: String,
    query_text: String,
    #[allow(dead_code)]
    limit: Option<u32>,
}

impl SignalDBDataSource {
    pub fn new() -> Self {
        let router_url =
            std::env::var("SIGNALDB_ROUTER_URL").unwrap_or_else(|_| DEFAULT_FLIGHT_URL.to_string());

        let timeout_secs = std::env::var("SIGNALDB_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_TIMEOUT_SECS);

        tracing::info!(
            "SignalDB DataSource initialized with router: {router_url}, timeout: {timeout_secs}s"
        );

        Self {
            router_url: Arc::from(router_url.as_str()),
            timeout_secs,
        }
    }

    /// Create a new instance with configuration from frontend settings.
    #[allow(dead_code)]
    pub fn with_config(config: DataSourceConfig) -> Self {
        let router_url = config
            .router_url
            .unwrap_or_else(|| DEFAULT_FLIGHT_URL.to_string());

        let timeout_secs = config.timeout.unwrap_or(DEFAULT_TIMEOUT_SECS);

        tracing::info!(
            "SignalDB DataSource initialized with router: {router_url}, timeout: {timeout_secs}s"
        );

        Self {
            router_url: Arc::from(router_url.as_str()),
            timeout_secs,
        }
    }

    /// Create a new Flight client connection.
    ///
    /// Each call creates a fresh connection. Connection pooling can be added
    /// in the future if needed for performance.
    async fn create_flight_client(&self) -> anyhow::Result<SignalDBFlightClient> {
        tracing::debug!("Creating Flight client connection to {}", self.router_url);
        Ok(SignalDBFlightClient::connect(&self.router_url, self.timeout_secs).await?)
    }
}

#[async_trait::async_trait]
impl backend::DataService for SignalDBDataSource {
    type Query = SignalDBQuery;
    type QueryError = QueryError;
    type Stream = backend::BoxDataResponseStream<Self::QueryError>;

    async fn query_data(
        &self,
        request: backend::QueryDataRequest<Self::Query, Self>,
    ) -> Self::Stream {
        tracing::debug!(
            "Received query_data request with {} queries",
            request.queries.len()
        );

        // Extract authentication context from plugin settings
        let auth = request
            .plugin_context
            .instance_settings
            .as_ref()
            .and_then(|settings| {
                let api_key = settings.decrypted_secure_json_data.api_key.clone();
                let tenant_id = settings.json_data.tenant_id.clone();
                let dataset_id = settings.json_data.dataset_id.clone();

                // Only return auth context if we have at least some auth info
                if api_key.is_some() || tenant_id.is_some() || dataset_id.is_some() {
                    Some(AuthContext {
                        api_key,
                        tenant_id,
                        dataset_id,
                    })
                } else {
                    None
                }
            });

        let datasource = self.clone();

        Box::pin(
            request
                .queries
                .into_iter()
                .map(|query| {
                    let ds = datasource.clone();
                    let auth = auth.clone();
                    async move {
                        let ref_id = query.ref_id.clone();
                        match ds.handle_query(&query.query, auth.as_ref()).await {
                            Ok(frame) => match frame.check() {
                                Ok(validated_frame) => {
                                    Ok(backend::DataResponse::new(ref_id, vec![validated_frame]))
                                }
                                Err(check_err) => {
                                    tracing::error!(
                                        "Frame validation failed for ref_id {}: {:?}",
                                        ref_id,
                                        check_err
                                    );
                                    Err(QueryError {
                                        ref_id,
                                        source: check_err.into(),
                                    })
                                }
                            },
                            Err(e) => {
                                tracing::error!("Query failed for ref_id {}: {:?}", ref_id, e);
                                Err(QueryError { ref_id, source: e })
                            }
                        }
                    }
                })
                .collect::<futures::stream::FuturesOrdered<_>>(),
        )
    }
}

impl SignalDBDataSource {
    /// Handle a single query.
    async fn handle_query(
        &self,
        query: &SignalDBQuery,
        auth: Option<&AuthContext>,
    ) -> anyhow::Result<data::Frame> {
        tracing::debug!(
            "Processing {} query: {}",
            query.signal_type,
            query.query_text
        );

        match query.signal_type.as_str() {
            "traces" => self.query_traces(query, auth).await,
            "metrics" => self.query_metrics(query, auth).await,
            "logs" => self.query_logs(query, auth).await,
            _ => Err(anyhow::anyhow!(
                "Unknown signal type '{}'. Supported types: traces, metrics, logs",
                query.signal_type
            )),
        }
    }

    /// Query traces via Flight.
    async fn query_traces(
        &self,
        query: &SignalDBQuery,
        auth: Option<&AuthContext>,
    ) -> anyhow::Result<data::Frame> {
        // Build the Flight ticket
        // Format: "traces" for listing, "trace_by_id?id=<trace_id>" for specific trace
        let ticket = if query.query_text.is_empty() {
            "traces".to_string()
        } else {
            // If query_text contains a trace ID, use trace_by_id format
            format!("trace_by_id?id={}", query.query_text)
        };

        tracing::debug!("Executing Flight query with ticket: {ticket}");

        // Connect and execute query
        let mut client = self.create_flight_client().await?;
        let (batches, schema) = client.query_with_auth(&ticket, auth).await?;

        if batches.is_empty() {
            tracing::debug!("No trace data returned from Flight query");
            return Ok(create_empty_traces_frame());
        }

        // Convert Arrow batches to Grafana Frame with time field
        let frame = conversion::batches_to_frame_with_time(
            &batches,
            &schema,
            "traces",
            "start_time_unix_nano",
            "time",
        )?;

        Ok(frame)
    }

    /// Query metrics via Flight.
    async fn query_metrics(
        &self,
        _query: &SignalDBQuery,
        auth: Option<&AuthContext>,
    ) -> anyhow::Result<data::Frame> {
        let ticket = "metrics".to_string();

        tracing::debug!("Executing Flight query for metrics");

        let mut client = self.create_flight_client().await?;
        let (batches, schema) = client.query_with_auth(&ticket, auth).await?;

        if batches.is_empty() {
            tracing::debug!("No metrics data returned from Flight query");
            return Ok(create_empty_metrics_frame());
        }

        // Convert Arrow batches to Grafana Frame with time field
        let frame = conversion::batches_to_frame_with_time(
            &batches,
            &schema,
            "metrics",
            "time_unix_nano",
            "time",
        )?;

        Ok(frame)
    }

    /// Query logs via Flight.
    async fn query_logs(
        &self,
        _query: &SignalDBQuery,
        auth: Option<&AuthContext>,
    ) -> anyhow::Result<data::Frame> {
        let ticket = "logs".to_string();

        tracing::debug!("Executing Flight query for logs");

        let mut client = self.create_flight_client().await?;
        let (batches, schema) = client.query_with_auth(&ticket, auth).await?;

        if batches.is_empty() {
            tracing::debug!("No logs data returned from Flight query");
            return Ok(create_empty_logs_frame());
        }

        // Convert Arrow batches to Grafana Frame with time field
        let frame = conversion::batches_to_frame_with_time(
            &batches,
            &schema,
            "logs",
            "time_unix_nano",
            "time",
        )?;

        Ok(frame)
    }
}

/// Create an empty traces frame with expected columns.
fn create_empty_traces_frame() -> data::Frame {
    use chrono::prelude::*;
    [
        Vec::<DateTime<Utc>>::new().into_field("time"),
        Vec::<String>::new().into_field("traceId"),
        Vec::<u64>::new().into_field("duration"),
    ]
    .into_frame("traces")
}

/// Create an empty metrics frame with expected columns.
fn create_empty_metrics_frame() -> data::Frame {
    use chrono::prelude::*;
    [
        Vec::<DateTime<Utc>>::new().into_field("time"),
        Vec::<String>::new().into_field("name"),
        Vec::<String>::new().into_field("metric_type"),
    ]
    .into_frame("metrics")
}

/// Create an empty logs frame with expected columns.
fn create_empty_logs_frame() -> data::Frame {
    use chrono::prelude::*;
    [
        Vec::<DateTime<Utc>>::new().into_field("time"),
        Vec::<String>::new().into_field("body"),
        Vec::<String>::new().into_field("severity_text"),
    ]
    .into_frame("logs")
}

#[grafana_plugin_sdk::main(
    services(data),
    init_subscriber = true,
    shutdown_handler = "0.0.0.0:10000"
)]
async fn plugin() -> SignalDBDataSource {
    SignalDBDataSource::new()
}
