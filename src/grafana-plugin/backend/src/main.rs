use grafana_plugin_sdk::{backend, data, prelude::*};
use serde::Deserialize;
use std::sync::Arc;
use thiserror::Error;

/// SignalDB datasource plugin for Grafana
#[derive(Clone, Debug, Default, GrafanaPlugin)]
#[grafana_plugin(plugin_type = "datasource")]
pub struct SignalDBDataSource {
    /// Router URL (e.g., "http://localhost:3001" for HTTP or "http://localhost:50053" for Flight)
    router_url: Arc<str>,
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
        let router_url = std::env::var("SIGNALDB_ROUTER_URL")
            .unwrap_or_else(|_| "http://localhost:3001".to_string());

        tracing::info!(
            "SignalDB DataSource initialized with router: {}",
            router_url
        );

        Self {
            router_url: Arc::from(router_url.as_str()),
        }
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

        Box::pin(
            request
                .queries
                .into_iter()
                .map(|query| {
                    let router_url = self.router_url.clone();
                    async move {
                        let ref_id = query.ref_id.clone();
                        match handle_query(&router_url, &query.query).await {
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

/// Handle a single query
async fn handle_query(router_url: &str, query: &SignalDBQuery) -> anyhow::Result<data::Frame> {
    tracing::debug!(
        "Processing {} query: {}",
        query.signal_type,
        query.query_text
    );

    // For now, return a placeholder frame
    // TODO: Implement actual query logic for each signal type via Router API
    match query.signal_type.as_str() {
        "traces" => query_traces(router_url, query).await,
        "metrics" => query_metrics(router_url, query).await,
        "logs" => query_logs(router_url, query).await,
        _ => Err(anyhow::anyhow!(
            "Unknown signal type: {}",
            query.signal_type
        )),
    }
}

async fn query_traces(_router_url: &str, _query: &SignalDBQuery) -> anyhow::Result<data::Frame> {
    // TODO: Implement traces query via Router's Tempo API
    use chrono::prelude::*;
    Ok([
        [
            Utc.with_ymd_and_hms(2021, 1, 1, 12, 0, 0).single().unwrap(),
            Utc.with_ymd_and_hms(2021, 1, 1, 12, 0, 1).single().unwrap(),
            Utc.with_ymd_and_hms(2021, 1, 1, 12, 0, 2).single().unwrap(),
        ]
        .into_field("time"),
        ["trace1", "trace2", "trace3"].into_field("traceId"),
        [100_u32, 200, 300].into_field("duration"),
    ]
    .into_frame("traces"))
}

async fn query_metrics(_router_url: &str, _query: &SignalDBQuery) -> anyhow::Result<data::Frame> {
    // TODO: Implement metrics query
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

async fn query_logs(_router_url: &str, _query: &SignalDBQuery) -> anyhow::Result<data::Frame> {
    // TODO: Implement logs query
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

#[grafana_plugin_sdk::main(
    services(data),
    init_subscriber = true,
    shutdown_handler = "0.0.0.0:10000"
)]
async fn plugin() -> SignalDBDataSource {
    SignalDBDataSource::new()
}
