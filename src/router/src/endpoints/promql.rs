//! # Prometheus-Compatible HTTP API (PromQL)
//!
//! Query endpoints for the metrics signal in the format Grafana's
//! Prometheus datasource expects, nested under `/prometheus`:
//!
//! - `GET|POST /api/v1/query_range` — range query → matrix
//! - `GET|POST /api/v1/query` — instant query → vector
//! - `GET /api/v1/labels`, `/api/v1/label/{name}/values`, `/api/v1/series`
//!
//! Handlers build a `query_promql` Flight ticket, execute it against a
//! querier, and convert the returned matrix RecordBatches into Prometheus
//! JSON. Metadata endpoints are minimal stubs for now (#339).

use std::collections::HashMap;

use crate::RouterState;
use arrow_flight::{Ticket, utils::flight_data_to_batches};
use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
};
use common::auth::TenantContextExtractor;
use common::flight::transport::ServiceCapability;
use datafusion::arrow::array::{
    Array, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use futures::StreamExt;
use prometheus_api::{
    InstantVector, LabelsResponse, QueryResponse, QueryResult, RangeVector, Sample, SeriesResponse,
};
use serde::Deserialize;

pub fn router<S: RouterState>() -> Router<S> {
    Router::new()
        .route("/api/v1/query", get(query::<S>).post(query::<S>))
        .route(
            "/api/v1/query_range",
            get(query_range::<S>).post(query_range::<S>),
        )
        .route("/api/v1/labels", get(labels::<S>))
        .route("/api/v1/label/{name}/values", get(label_values::<S>))
        .route("/api/v1/series", get(series::<S>))
}

/// One hour in nanoseconds, the default range-query lookback.
const HOUR_NS: i64 = 3_600_000_000_000;

/// Parameters for `/api/v1/query` (instant queries).
#[derive(Debug, Deserialize)]
pub struct InstantParams {
    pub query: Option<String>,
    /// Evaluation timestamp (unix seconds or RFC3339).
    pub time: Option<String>,
}

/// Parameters for `/api/v1/query_range`.
#[derive(Debug, Deserialize)]
pub struct RangeParams {
    pub query: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    /// Resolution step (Go duration or seconds).
    pub step: Option<String>,
}

/// Parameters for the metadata endpoints.
#[derive(Debug, Default, Deserialize)]
pub struct MetadataParams {
    pub start: Option<String>,
    pub end: Option<String>,
    #[serde(rename = "match[]")]
    pub matcher: Option<String>,
}

/// GET|POST /prometheus/api/v1/query_range.
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(tenant_id = %tenant_ctx.0.tenant_id, dataset_id = %tenant_ctx.0.dataset_id)
)]
pub async fn query_range<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<RangeParams>,
) -> Result<axum::Json<QueryResponse>, StatusCode> {
    let Some(promql) = non_empty(&params.query) else {
        return Ok(axum::Json(QueryResponse::error(
            "bad_data",
            "missing or empty 'query'",
        )));
    };
    let end = parse_timestamp_ns(params.end.as_deref()).unwrap_or_else(now_ns);
    let start = parse_timestamp_ns(params.start.as_deref()).unwrap_or(end - HOUR_NS);
    let step = parse_step_ns(params.step.as_deref()).unwrap_or_else(|| default_step_ns(start, end));

    match run_promql(&state, &tenant_ctx, &promql, start, end, step).await {
        Ok(batches) => Ok(axum::Json(QueryResponse::success(QueryResult::Matrix(
            batches_to_matrix(&batches),
        )))),
        Err(status) => Err(status),
    }
}

/// GET|POST /prometheus/api/v1/query — instant query.
///
/// Evaluated as a one-bucket range at `time`, returning the latest sample
/// per series as a vector.
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(tenant_id = %tenant_ctx.0.tenant_id, dataset_id = %tenant_ctx.0.dataset_id)
)]
pub async fn query<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<InstantParams>,
) -> Result<axum::Json<QueryResponse>, StatusCode> {
    let Some(promql) = non_empty(&params.query) else {
        return Ok(axum::Json(QueryResponse::error(
            "bad_data",
            "missing or empty 'query'",
        )));
    };
    let at = parse_timestamp_ns(params.time.as_deref()).unwrap_or_else(now_ns);
    let start = at - HOUR_NS;
    // One bucket spanning the lookback so each series yields one sample.
    let step = HOUR_NS;

    match run_promql(&state, &tenant_ctx, &promql, start, at, step).await {
        Ok(batches) => {
            let vector = matrix_to_vector(batches_to_matrix(&batches));
            Ok(axum::Json(QueryResponse::success(QueryResult::Vector(
                vector,
            ))))
        }
        Err(status) => Err(status),
    }
}

/// GET /prometheus/api/v1/labels — stub (metadata discovery lands in #339).
pub async fn labels<S: RouterState>(
    State(_state): State<S>,
    _tenant_ctx: TenantContextExtractor,
    Query(_params): Query<MetadataParams>,
) -> axum::Json<LabelsResponse> {
    axum::Json(LabelsResponse::success(vec![]))
}

/// GET /prometheus/api/v1/label/{name}/values — stub (#339).
pub async fn label_values<S: RouterState>(
    State(_state): State<S>,
    _tenant_ctx: TenantContextExtractor,
    Path(_name): Path<String>,
    Query(_params): Query<MetadataParams>,
) -> axum::Json<LabelsResponse> {
    axum::Json(LabelsResponse::success(vec![]))
}

/// GET /prometheus/api/v1/series — stub (#339).
pub async fn series<S: RouterState>(
    State(_state): State<S>,
    _tenant_ctx: TenantContextExtractor,
    Query(_params): Query<MetadataParams>,
) -> axum::Json<SeriesResponse> {
    axum::Json(SeriesResponse::success(vec![]))
}

// ---- execution + conversion ----

/// Build and execute a `query_promql` ticket.
async fn run_promql<S: RouterState>(
    state: &S,
    tenant_ctx: &TenantContextExtractor,
    promql: &str,
    start: i64,
    end: i64,
    step: i64,
) -> Result<Vec<RecordBatch>, StatusCode> {
    let payload = serde_json::json!({
        "query": promql,
        "start": start,
        "end": end,
        "step": step,
    });
    let ticket = format!(
        "query_promql:{}:{}:{payload}",
        tenant_ctx.0.tenant_slug, tenant_ctx.0.dataset_slug
    );
    execute_ticket(state, ticket).await
}

/// Send a Flight ticket to a querier and collect the result batches.
async fn execute_ticket<S: RouterState>(
    state: &S,
    ticket_content: String,
) -> Result<Vec<RecordBatch>, StatusCode> {
    let mut client = state
        .service_registry()
        .get_flight_client_for_capability(ServiceCapability::QueryExecution)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to get Flight client for PromQL query");
            StatusCode::SERVICE_UNAVAILABLE
        })?;

    let ticket = Ticket::new(ticket_content);
    let mut flight_request = tonic::Request::new(ticket);
    common::flight::trace_context::inject_context_into_request(&mut flight_request);
    if let Some(key) = &state.config().auth.internal_service_key {
        common::flight::auth::attach_internal_auth(&mut flight_request, key);
    }

    let mut stream = client
        .do_get(flight_request)
        .await
        .map_err(|e| flight_status_to_http(&e))?
        .into_inner();

    let mut data = Vec::new();
    while let Some(flight_data) = stream.next().await {
        data.push(flight_data.map_err(|e| flight_status_to_http(&e))?);
    }

    flight_data_to_batches(&data).map_err(|e| {
        tracing::error!(error = %e, "Failed to decode PromQL Flight data");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

fn flight_status_to_http(status: &tonic::Status) -> StatusCode {
    match status.code() {
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        tonic::Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        tonic::Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        _ => {
            tracing::error!(error = %status, "PromQL Flight query failed");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Group matrix rows (`bucket`, `metric_name`, label columns, `value`)
/// into Prometheus range vectors. `bucket` is nanoseconds; Prometheus
/// samples use unix seconds.
fn batches_to_matrix(batches: &[RecordBatch]) -> Vec<RangeVector> {
    let mut order: Vec<String> = Vec::new();
    let mut series: HashMap<String, RangeVector> = HashMap::new();

    for batch in batches {
        let Some(buckets) = timestamps_ns(batch, "bucket") else {
            continue;
        };
        let value = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());

        let schema = batch.schema();
        let label_cols: Vec<(String, &StringArray)> = schema
            .fields()
            .iter()
            .filter_map(|f| {
                let name = f.name();
                if name == "bucket" || name == "value" {
                    return None;
                }
                str_col(batch, name).map(|c| (name.clone(), c))
            })
            .collect();

        for i in 0..batch.num_rows() {
            let mut metric: HashMap<String, String> = HashMap::new();
            for (name, col) in &label_cols {
                if col.is_null(i) || col.value(i).is_empty() {
                    continue;
                }
                // `metric_name` is Prometheus's `__name__`.
                let key = if name == "metric_name" {
                    "__name__"
                } else {
                    name.as_str()
                };
                metric.insert(key.to_string(), col.value(i).to_string());
            }
            let key = label_key(&metric);
            let seconds = if buckets.is_null(i) {
                0.0
            } else {
                buckets.value(i) as f64 / 1_000_000_000.0
            };
            let v = value
                .map(|c| if c.is_null(i) { f64::NAN } else { c.value(i) })
                .unwrap_or(f64::NAN);
            series
                .entry(key.clone())
                .or_insert_with(|| {
                    order.push(key.clone());
                    RangeVector {
                        metric,
                        values: Vec::new(),
                    }
                })
                .values
                .push(Sample::new(seconds, format_value(v)));
        }
    }

    order
        .into_iter()
        .filter_map(|k| series.remove(&k))
        .collect()
}

/// Reduce a matrix to an instant vector: each series' last sample.
fn matrix_to_vector(matrix: Vec<RangeVector>) -> Vec<InstantVector> {
    matrix
        .into_iter()
        .filter_map(|series| {
            series.values.into_iter().last().map(|value| InstantVector {
                metric: series.metric,
                value,
            })
        })
        .collect()
}

fn format_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.fract() == 0.0 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

fn label_key(labels: &HashMap<String, String>) -> String {
    let mut pairs: Vec<_> = labels.iter().collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn str_col<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
}

/// Read a timestamp column as nanoseconds, casting from the storage unit.
fn timestamps_ns(batch: &RecordBatch, name: &str) -> Option<TimestampNanosecondArray> {
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    let column = batch.column_by_name(name)?;
    let nanos = cast(column, &DataType::Timestamp(TimeUnit::Nanosecond, None)).ok()?;
    nanos
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .cloned()
}

fn non_empty(value: &Option<String>) -> Option<String> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn now_ns() -> i64 {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() * 1_000_000)
}

/// Parse a Prometheus timestamp (unix seconds float, or RFC3339) → ns.
fn parse_timestamp_ns(value: Option<&str>) -> Option<i64> {
    let value = value.map(str::trim).filter(|s| !s.is_empty())?;
    if let Ok(seconds) = value.parse::<f64>() {
        return Some((seconds * 1_000_000_000.0) as i64);
    }
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .and_then(|dt| dt.timestamp_nanos_opt())
}

/// Parse `step` (Go duration or seconds) → nanoseconds.
fn parse_step_ns(value: Option<&str>) -> Option<i64> {
    let value = value.map(str::trim).filter(|s| !s.is_empty())?;
    if let Ok(seconds) = value.parse::<f64>() {
        return Some((seconds * 1_000_000_000.0) as i64);
    }
    // Reuse the LogQL lexer for durations like `30s`, `5m`.
    let tokens = logql::tokenize(value).ok()?;
    match tokens.first().map(|t| &t.token) {
        Some(logql::Token::Duration(d)) => Some(d.as_nanos() as i64),
        _ => None,
    }
}

fn default_step_ns(start: i64, end: i64) -> i64 {
    let span = (end - start).max(1);
    (span / 250).max(1_000_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn matrix_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "bucket",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_000_000_000,
                    2_000_000_000,
                    1_000_000_000,
                ])),
                Arc::new(StringArray::from(vec!["reqs", "reqs", "reqs"])),
                Arc::new(StringArray::from(vec!["api", "api", "web"])),
                Arc::new(Float64Array::from(vec![2.0, 3.0, 5.5])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn matrix_groups_rows_and_maps_name() {
        let matrix = batches_to_matrix(&[matrix_batch()]);
        assert_eq!(matrix.len(), 2);
        let api = matrix
            .iter()
            .find(|s| s.metric.get("service_name") == Some(&"api".to_string()))
            .unwrap();
        assert_eq!(api.metric.get("__name__"), Some(&"reqs".to_string()));
        assert_eq!(
            api.values,
            vec![Sample::new(1.0, "2"), Sample::new(2.0, "3")]
        );
        let web = matrix
            .iter()
            .find(|s| s.metric.get("service_name") == Some(&"web".to_string()))
            .unwrap();
        assert_eq!(web.values, vec![Sample::new(1.0, "5.5")]);
    }

    #[test]
    fn instant_vector_takes_last_sample() {
        let vector = matrix_to_vector(batches_to_matrix(&[matrix_batch()]));
        let api = vector
            .iter()
            .find(|s| s.metric.get("service_name") == Some(&"api".to_string()))
            .unwrap();
        assert_eq!(api.value, Sample::new(2.0, "3"));
    }

    #[test]
    fn value_formatting() {
        assert_eq!(format_value(3.0), "3");
        assert_eq!(format_value(2.5), "2.5");
        assert_eq!(format_value(f64::NAN), "NaN");
    }

    #[test]
    fn step_and_timestamp_parsing() {
        assert_eq!(parse_step_ns(Some("30")), Some(30_000_000_000));
        assert_eq!(parse_step_ns(Some("5m")), Some(300_000_000_000));
        assert_eq!(
            parse_timestamp_ns(Some("1700000000")),
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(
            parse_timestamp_ns(Some("2023-11-14T22:13:20Z")),
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(parse_timestamp_ns(None), None);
    }
}
