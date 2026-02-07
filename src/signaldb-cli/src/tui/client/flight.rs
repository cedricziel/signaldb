//! Flight SQL client for TUI data access.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use futures::{StreamExt, TryStreamExt};
use tonic::metadata::MetadataValue;
use tonic::transport::Endpoint;

use super::models::{SpanInfo, TraceDetail, TraceResult, TraceSearchParams};

/// Errors returned by [`FlightSqlClient`].
#[derive(Debug, thiserror::Error)]
pub enum FlightClientError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("authentication error: {0}")]
    Auth(String),
    #[error("query error: {0}")]
    Query(String),
}

/// Flight SQL client for querying SignalDB from the TUI.
#[derive(Debug, Clone)]
pub struct FlightSqlClient {
    flight_url: String,
    api_key: Option<String>,
    tenant_id: Option<String>,
    dataset_id: Option<String>,
}

impl FlightSqlClient {
    pub fn new(
        flight_url: String,
        api_key: Option<String>,
        tenant_id: Option<String>,
        dataset_id: Option<String>,
    ) -> Self {
        Self {
            flight_url,
            api_key,
            tenant_id,
            dataset_id,
        }
    }

    /// Execute a SQL query and return raw record batches.
    pub async fn query_sql(&self, sql: &str) -> Result<Vec<RecordBatch>, FlightClientError> {
        let endpoint = Endpoint::from_shared(self.flight_url.clone())
            .map_err(|e| {
                FlightClientError::Connection(format!(
                    "invalid flight URL '{}': {e}",
                    self.flight_url
                ))
            })?
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10));

        let channel = endpoint.connect().await.map_err(|e| {
            FlightClientError::Connection(format!(
                "failed to connect to SignalDB at {}: {e}",
                self.flight_url
            ))
        })?;

        let mut client = FlightServiceClient::new(channel);

        let ticket = Ticket {
            ticket: sql.as_bytes().to_vec().into(),
        };
        let mut request = tonic::Request::new(ticket);

        if let Some(key) = &self.api_key {
            let value = MetadataValue::try_from(format!("Bearer {key}"))
                .map_err(|e| FlightClientError::Auth(format!("invalid API key: {e}")))?;
            request.metadata_mut().insert("authorization", value);
        }
        if let Some(tenant) = &self.tenant_id {
            let value = MetadataValue::try_from(tenant.as_str())
                .map_err(|e| FlightClientError::Auth(format!("invalid tenant ID: {e}")))?;
            request.metadata_mut().insert("x-tenant-id", value);
        }
        if let Some(dataset) = &self.dataset_id {
            let value = MetadataValue::try_from(dataset.as_str())
                .map_err(|e| FlightClientError::Auth(format!("invalid dataset ID: {e}")))?;
            request.metadata_mut().insert("x-dataset-id", value);
        }

        let response = client
            .do_get(request)
            .await
            .map_err(|status| match status.code() {
                tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
                    FlightClientError::Auth(status.message().to_string())
                }
                tonic::Code::Unavailable => {
                    FlightClientError::Connection(status.message().to_string())
                }
                _ => FlightClientError::Query(status.message().to_string()),
            })?;

        let flight_stream = response.into_inner();
        let mut batch_stream = FlightRecordBatchStream::new_from_flight_data(
            flight_stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

        let mut batches = Vec::new();
        while let Some(result) = batch_stream.next().await {
            let batch = result.map_err(|e| FlightClientError::Query(e.to_string()))?;
            batches.push(batch);
        }

        Ok(batches)
    }

    /// Search for traces matching the given parameters.
    pub async fn search_traces(
        &self,
        params: &TraceSearchParams,
    ) -> Result<Vec<TraceResult>, FlightClientError> {
        let mut conditions = Vec::new();

        if let Some(tags) = &params.tags {
            conditions.push(format!("span_attributes LIKE '%{tags}%'"));
        }
        if let Some(min_dur) = &params.min_duration
            && let Some(nanos) = parse_duration_to_nanos(min_dur)
        {
            conditions.push(format!("duration_nanos >= {nanos}"));
        }
        if let Some(max_dur) = &params.max_duration
            && let Some(nanos) = parse_duration_to_nanos(max_dur)
        {
            conditions.push(format!("duration_nanos <= {nanos}"));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let limit = params.limit.unwrap_or(20);
        let sql = format!(
            "SELECT trace_id, service_name, span_name, duration_nanos, \
             start_time_unix_nano, is_root \
             FROM traces {where_clause} \
             ORDER BY start_time_unix_nano DESC LIMIT {limit}"
        );

        let batches = self.query_sql(&sql).await?;
        let mut results = Vec::new();

        for batch in &batches {
            let trace_ids = batch
                .column_by_name("trace_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let services = batch
                .column_by_name("service_name")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let operations = batch
                .column_by_name("span_name")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let durations = batch.column_by_name("duration_nanos");
            let start_times = batch.column_by_name("start_time_unix_nano");

            let (trace_ids, services, operations) = match (trace_ids, services, operations) {
                (Some(t), Some(s), Some(o)) => (t, s, o),
                _ => continue,
            };

            for i in 0..batch.num_rows() {
                let duration_nanos = extract_numeric_value(durations, i).unwrap_or(0);
                let start_nanos = extract_numeric_value(start_times, i).unwrap_or(0);

                results.push(TraceResult {
                    trace_id: trace_ids.value(i).to_string(),
                    root_service: services.value(i).to_string(),
                    root_operation: operations.value(i).to_string(),
                    duration_ms: duration_nanos as f64 / 1_000_000.0,
                    span_count: 0,
                    start_time: format_nanos_timestamp(start_nanos),
                });
            }
        }

        Ok(results)
    }

    /// Fetch all spans for a specific trace.
    pub async fn get_trace(&self, trace_id: &str) -> Result<TraceDetail, FlightClientError> {
        let sql = format!(
            "SELECT span_id, parent_span_id, span_name, service_name, \
             start_time_unix_nano, duration_nanos, status_code, span_attributes \
             FROM traces WHERE trace_id = '{trace_id}' \
             ORDER BY start_time_unix_nano ASC"
        );

        let batches = self.query_sql(&sql).await?;

        let trace_start = batches
            .iter()
            .flat_map(|b| {
                let col = b.column_by_name("start_time_unix_nano");
                (0..b.num_rows()).map(move |i| extract_numeric_value(col, i).unwrap_or(u64::MAX))
            })
            .min()
            .unwrap_or(0);

        let mut spans = Vec::new();

        for batch in &batches {
            let span_ids = batch
                .column_by_name("span_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let parent_ids = batch
                .column_by_name("parent_span_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let operations = batch
                .column_by_name("span_name")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let services = batch
                .column_by_name("service_name")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let start_times = batch.column_by_name("start_time_unix_nano");
            let durations = batch.column_by_name("duration_nanos");
            let statuses = batch
                .column_by_name("status_code")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let attributes = batch
                .column_by_name("span_attributes")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            let (span_ids, operations, services) = match (span_ids, operations, services) {
                (Some(s), Some(o), Some(sv)) => (s, o, sv),
                _ => continue,
            };

            for i in 0..batch.num_rows() {
                let start_nanos = extract_numeric_value(start_times, i).unwrap_or(0);
                let dur_nanos = extract_numeric_value(durations, i).unwrap_or(0);

                let parent = parent_ids.and_then(|p| {
                    if p.is_null(i) {
                        None
                    } else {
                        let v = p.value(i);
                        if v.is_empty() {
                            None
                        } else {
                            Some(v.to_string())
                        }
                    }
                });

                let attrs = attributes
                    .and_then(|a| {
                        if a.is_null(i) {
                            None
                        } else {
                            serde_json::from_str(a.value(i)).ok()
                        }
                    })
                    .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

                let status = statuses.map(|s| s.value(i).to_string()).unwrap_or_default();

                spans.push(SpanInfo {
                    span_id: span_ids.value(i).to_string(),
                    parent_span_id: parent,
                    operation: operations.value(i).to_string(),
                    service: services.value(i).to_string(),
                    start_time_ms: (start_nanos.saturating_sub(trace_start)) as f64 / 1_000_000.0,
                    duration_ms: dur_nanos as f64 / 1_000_000.0,
                    status,
                    attributes: attrs,
                });
            }
        }

        Ok(TraceDetail {
            trace_id: trace_id.to_string(),
            spans,
        })
    }

    /// Execute an arbitrary SQL query for system metrics / admin dashboards.
    pub async fn query_system_metrics(
        &self,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, FlightClientError> {
        self.query_sql(sql).await
    }
}

fn extract_numeric_value(col: Option<&Arc<dyn Array>>, row: usize) -> Option<u64> {
    let col = col?.as_ref();
    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        if arr.is_null(row) {
            None
        } else {
            Some(arr.value(row))
        }
    } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        if arr.is_null(row) {
            None
        } else {
            Some(arr.value(row) as u64)
        }
    } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        if arr.is_null(row) {
            None
        } else {
            Some(arr.value(row) as u64)
        }
    } else {
        None
    }
}

fn parse_duration_to_nanos(s: &str) -> Option<u64> {
    let s = s.trim();
    if let Some(ms) = s.strip_suffix("ms") {
        ms.trim()
            .parse::<f64>()
            .ok()
            .map(|v| (v * 1_000_000.0) as u64)
    } else if let Some(secs) = s.strip_suffix('s') {
        secs.trim()
            .parse::<f64>()
            .ok()
            .map(|v| (v * 1_000_000_000.0) as u64)
    } else if let Some(mins) = s.strip_suffix('m') {
        mins.trim()
            .parse::<f64>()
            .ok()
            .map(|v| (v * 60_000_000_000.0) as u64)
    } else {
        s.parse::<u64>().ok()
    }
}

fn format_nanos_timestamp(nanos: u64) -> String {
    let secs = (nanos / 1_000_000_000) as i64;
    let nsec = (nanos % 1_000_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, nsec)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
        .unwrap_or_else(|| nanos.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_new_stores_fields() {
        let client = FlightSqlClient::new(
            "http://localhost:50053".into(),
            Some("sk-test".into()),
            Some("acme".into()),
            Some("prod".into()),
        );
        assert_eq!(client.flight_url, "http://localhost:50053");
        assert_eq!(client.api_key.as_deref(), Some("sk-test"));
        assert_eq!(client.tenant_id.as_deref(), Some("acme"));
        assert_eq!(client.dataset_id.as_deref(), Some("prod"));
    }

    #[test]
    fn client_new_optional_fields() {
        let client = FlightSqlClient::new("http://localhost:50053".into(), None, None, None);
        assert!(client.api_key.is_none());
        assert!(client.tenant_id.is_none());
        assert!(client.dataset_id.is_none());
    }

    #[test]
    fn error_display_connection() {
        let err = FlightClientError::Connection("refused".into());
        assert_eq!(err.to_string(), "connection error: refused");
    }

    #[test]
    fn error_display_auth() {
        let err = FlightClientError::Auth("bad token".into());
        assert_eq!(err.to_string(), "authentication error: bad token");
    }

    #[test]
    fn error_display_query() {
        let err = FlightClientError::Query("syntax error".into());
        assert_eq!(err.to_string(), "query error: syntax error");
    }

    #[tokio::test]
    async fn connection_to_unreachable_returns_connection_error() {
        let client = FlightSqlClient::new("http://127.0.0.1:19999".into(), None, None, None);
        let result = client.query_sql("SELECT 1").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            FlightClientError::Connection(_) => {}
            other => panic!("expected ConnectionError, got {other:?}"),
        }
    }

    #[test]
    fn parse_duration_ms() {
        assert_eq!(parse_duration_to_nanos("100ms"), Some(100_000_000));
    }

    #[test]
    fn parse_duration_s() {
        assert_eq!(parse_duration_to_nanos("2s"), Some(2_000_000_000));
    }

    #[test]
    fn parse_duration_m() {
        assert_eq!(parse_duration_to_nanos("1m"), Some(60_000_000_000));
    }

    #[test]
    fn parse_duration_raw_nanos() {
        assert_eq!(parse_duration_to_nanos("500"), Some(500));
    }

    #[test]
    fn parse_duration_invalid() {
        assert_eq!(parse_duration_to_nanos("abc"), None);
    }

    #[test]
    fn format_timestamp_zero() {
        let s = format_nanos_timestamp(0);
        assert!(s.contains("1970-01-01"));
    }
}
