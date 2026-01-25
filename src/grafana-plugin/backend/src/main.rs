mod conversion;
mod flight_client;

use flight_client::SignalDBFlightClient;
use grafana_plugin_sdk::{backend, data, prelude::*};
use serde::Deserialize;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

/// Default Flight service URL
const DEFAULT_FLIGHT_URL: &str = "http://localhost:50053";
/// Default query timeout in seconds
const DEFAULT_TIMEOUT_SECS: u32 = 30;

/// Configuration from frontend datasource settings.
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DataSourceConfig {
    /// Router URL for Flight service (e.g., "http://localhost:50053")
    pub router_url: Option<String>,
    /// Protocol to use (currently only "flight" is supported)
    pub protocol: Option<String>,
    /// Query timeout in seconds
    pub timeout: Option<u32>,
}

/// SignalDB datasource plugin for Grafana
#[derive(Clone, Debug, Default, GrafanaPlugin)]
#[grafana_plugin(plugin_type = "datasource")]
pub struct SignalDBDataSource {
    /// Router URL (e.g., "http://localhost:3001" for HTTP or "http://localhost:50053" for Flight)
    router_url: Arc<str>,
    /// Query timeout in seconds
    timeout_secs: u32,
    /// Flight client (lazily initialized)
    flight_client: Arc<Mutex<Option<SignalDBFlightClient>>>,
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
            flight_client: Arc::new(Mutex::new(None)),
        }
    }

    /// Create a new instance with configuration from frontend settings.
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
            flight_client: Arc::new(Mutex::new(None)),
        }
    }

    /// Get or create a Flight client connection.
    async fn get_flight_client(&self) -> anyhow::Result<SignalDBFlightClient> {
        let mut client_guard = self.flight_client.lock().await;

        if client_guard.is_none() {
            tracing::debug!(
                "Creating new Flight client connection to {}",
                self.router_url
            );
            let client = SignalDBFlightClient::connect(&self.router_url, self.timeout_secs).await?;
            *client_guard = Some(client);
        }

        // Clone the client for use (we can't return a reference held by the lock)
        // For now, create a new connection each time - connection pooling can be added later
        drop(client_guard);
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

        let datasource = self.clone();

        Box::pin(
            request
                .queries
                .into_iter()
                .map(|query| {
                    let ds = datasource.clone();
                    async move {
                        let ref_id = query.ref_id.clone();
                        match ds.handle_query(&query.query).await {
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
    async fn handle_query(&self, query: &SignalDBQuery) -> anyhow::Result<data::Frame> {
        tracing::debug!(
            "Processing {} query: {}",
            query.signal_type,
            query.query_text
        );

        match query.signal_type.as_str() {
            "traces" => self.query_traces(query).await,
            "metrics" => self.query_metrics(query).await,
            "logs" => self.query_logs(query).await,
            _ => Err(anyhow::anyhow!(
                "Unknown signal type: {}",
                query.signal_type
            )),
        }
    }

    /// Query traces via Flight.
    async fn query_traces(&self, query: &SignalDBQuery) -> anyhow::Result<data::Frame> {
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
        let mut client = self.get_flight_client().await?;
        let (batches, schema) = client.query(&ticket).await?;

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

    /// Query metrics via Flight (placeholder - returns mock data for now).
    async fn query_metrics(&self, _query: &SignalDBQuery) -> anyhow::Result<data::Frame> {
        // TODO: Implement metrics query via Flight
        use chrono::prelude::*;
        Ok([
            [
                Utc.with_ymd_and_hms(2021, 1, 1, 12, 0, 0).single().unwrap(),
                Utc.with_ymd_and_hms(2021, 1, 1, 12, 0, 1).single().unwrap(),
                Utc.with_ymd_and_hms(2021, 1, 1, 12, 0, 2).single().unwrap(),
            ]
            .into_field("time"),
            [1.0_f64, 2.0, 3.0].into_field("value"),
        ]
        .into_frame("metrics"))
    }

    /// Query logs via Flight (placeholder - returns mock data for now).
    async fn query_logs(&self, _query: &SignalDBQuery) -> anyhow::Result<data::Frame> {
        // TODO: Implement logs query via Flight
        use chrono::prelude::*;
        Ok([
            [
                Utc.with_ymd_and_hms(2021, 1, 1, 12, 0, 0).single().unwrap(),
                Utc.with_ymd_and_hms(2021, 1, 1, 12, 0, 1).single().unwrap(),
                Utc.with_ymd_and_hms(2021, 1, 1, 12, 0, 2).single().unwrap(),
            ]
            .into_field("time"),
            ["log line 1", "log line 2", "log line 3"].into_field("line"),
        ]
        .into_frame("logs"))
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

#[grafana_plugin_sdk::main(
    services(data),
    init_subscriber = true,
    shutdown_handler = "0.0.0.0:10000"
)]
async fn plugin() -> SignalDBDataSource {
    SignalDBDataSource::new()
}
