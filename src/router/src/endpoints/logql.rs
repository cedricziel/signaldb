//! # Loki-Compatible HTTP API (LogQL)
//!
//! Query endpoints for the logs signal in the format Grafana's Loki
//! datasource expects, nested under `/loki`:
//!
//! - `GET /api/v1/query` — instant query
//! - `GET /api/v1/query_range` — range query
//! - `GET /api/v1/labels` — label name discovery
//! - `GET /api/v1/label/{name}/values` — label value discovery
//! - `GET /api/v1/series` — series discovery
//!
//! Handlers currently return empty stub responses in the correct wire
//! format; query execution lands with the LogQL transpiler and querier
//! log service (#373–#377). The `/api/v1/tail` WebSocket endpoint is
//! tracked separately (#380).

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
use datafusion::arrow::array::{Array, RecordBatch, StringArray, TimestampNanosecondArray};
use futures::StreamExt;
use loki_api::{LabelsResponse, LogEntry, QueryResponse, QueryResult, SeriesResponse, Stream};
use serde::Deserialize;

pub fn router<S: RouterState>() -> Router<S> {
    Router::new()
        .route("/api/v1/query", get(query::<S>))
        .route("/api/v1/query_range", get(query_range::<S>))
        .route("/api/v1/labels", get(labels::<S>))
        .route("/api/v1/label/{name}/values", get(label_values::<S>))
        .route("/api/v1/series", get(series::<S>))
}

fn default_limit() -> u32 {
    100
}

fn default_direction() -> String {
    "backward".to_string()
}

/// Query parameters for `/api/v1/query` (instant queries).
#[derive(Debug, Deserialize)]
pub struct InstantQueryParams {
    /// LogQL query string.
    pub query: Option<String>,
    /// Evaluation timestamp (unix epoch ns, s, or RFC3339).
    pub time: Option<String>,
    /// Maximum number of entries for log queries.
    #[serde(default = "default_limit")]
    pub limit: u32,
    /// `forward` or `backward`.
    #[serde(default = "default_direction")]
    pub direction: String,
}

/// Query parameters for `/api/v1/query_range`.
#[derive(Debug, Deserialize)]
pub struct RangeQueryParams {
    /// LogQL query string.
    pub query: Option<String>,
    /// Range start (unix epoch ns, s, or RFC3339).
    pub start: Option<String>,
    /// Range end (unix epoch ns, s, or RFC3339).
    pub end: Option<String>,
    /// Evaluation interval for metric queries (duration or seconds).
    pub step: Option<String>,
    /// Maximum number of entries for log queries.
    #[serde(default = "default_limit")]
    pub limit: u32,
    /// `forward` or `backward`.
    #[serde(default = "default_direction")]
    pub direction: String,
}

/// Query parameters for the metadata endpoints (`/labels`,
/// `/label/{name}/values`, `/series`).
#[derive(Debug, Default, Deserialize)]
pub struct MetadataParams {
    /// Range start (unix epoch ns, s, or RFC3339).
    pub start: Option<String>,
    /// Range end (unix epoch ns, s, or RFC3339).
    pub end: Option<String>,
    /// Stream selector to restrict results, e.g. `{service_name="api"}`.
    #[serde(rename = "match[]")]
    pub matcher: Option<String>,
    /// `query` is accepted as an alias for `match[]` (Grafana sends it
    /// on the labels/values endpoints).
    pub query: Option<String>,
}

/// Reject requests without a `query` parameter, mirroring Loki's
/// "empty query" error status.
fn require_query(query: &Option<String>) -> Result<String, StatusCode> {
    query
        .as_deref()
        .map(str::trim)
        .filter(|q| !q.is_empty())
        .map(str::to_string)
        .ok_or(StatusCode::BAD_REQUEST)
}

/// Validate the `direction` parameter.
fn validate_direction(direction: &str) -> Result<(), StatusCode> {
    match direction {
        "forward" | "backward" => Ok(()),
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

/// GET /loki/api/v1/query — instant query.
///
/// For a log selector this returns the most recent lines in the window;
/// the instant `time` (or now) is treated as the window end with a
/// default one-hour lookback, matching how Grafana renders instant log
/// panels.
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn query<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<InstantQueryParams>,
) -> Result<axum::Json<QueryResponse>, StatusCode> {
    let logql = require_query(&params.query)?;
    validate_direction(&params.direction)?;

    let end = parse_timestamp_ns(params.time.as_deref()).unwrap_or_else(now_ns);
    let start = end - HOUR_NS;
    let streams = run_log_query(
        &state,
        &tenant_ctx,
        &logql,
        start,
        end,
        params.limit,
        &params.direction,
    )
    .await?;
    Ok(axum::Json(QueryResponse::success(QueryResult::Streams(
        streams,
    ))))
}

/// GET /loki/api/v1/query_range — range query.
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn query_range<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<RangeQueryParams>,
) -> Result<axum::Json<QueryResponse>, StatusCode> {
    let logql = require_query(&params.query)?;
    validate_direction(&params.direction)?;

    let end = parse_timestamp_ns(params.end.as_deref()).unwrap_or_else(now_ns);
    let start = parse_timestamp_ns(params.start.as_deref()).unwrap_or(end - HOUR_NS);

    // A metric query returns a matrix; a log query returns streams.
    if is_metric_query(&logql) {
        let step =
            parse_step_ns(params.step.as_deref()).unwrap_or_else(|| default_step_ns(start, end));
        let matrix = run_metric_query(&state, &tenant_ctx, &logql, start, end, step).await?;
        return Ok(axum::Json(QueryResponse::success(QueryResult::Matrix(
            matrix,
        ))));
    }

    let streams = run_log_query(
        &state,
        &tenant_ctx,
        &logql,
        start,
        end,
        params.limit,
        &params.direction,
    )
    .await?;
    Ok(axum::Json(QueryResponse::success(QueryResult::Streams(
        streams,
    ))))
}

/// GET /loki/api/v1/labels — list label names.
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn labels<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<MetadataParams>,
) -> Result<axum::Json<LabelsResponse>, StatusCode> {
    let (start, end) = metadata_window(&params);
    let ticket = format!(
        "query_logs_labels:{}:{}:{start}:{end}",
        tenant_ctx.0.tenant_slug, tenant_ctx.0.dataset_slug
    );
    let batches = execute_ticket(&state, ticket).await?;
    Ok(axum::Json(LabelsResponse::success(string_column(
        &batches, "label",
    ))))
}

/// GET /loki/api/v1/label/{name}/values — list values of one label.
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id,
        label = %name
    )
)]
pub async fn label_values<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Path(name): Path<String>,
    Query(params): Query<MetadataParams>,
) -> Result<axum::Json<LabelsResponse>, StatusCode> {
    if name.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let (start, end) = metadata_window(&params);
    let ticket = format!(
        "query_logs_label_values:{}:{}:{name}:{start}:{end}",
        tenant_ctx.0.tenant_slug, tenant_ctx.0.dataset_slug
    );
    let batches = execute_ticket(&state, ticket).await?;
    Ok(axum::Json(LabelsResponse::success(string_column(
        &batches, "value",
    ))))
}

/// GET /loki/api/v1/series — list series matching a selector.
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn series<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<MetadataParams>,
) -> Result<axum::Json<SeriesResponse>, StatusCode> {
    let selector = params
        .matcher
        .as_deref()
        .or(params.query.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let (start, end) = metadata_window(&params);

    let payload = serde_json::json!({ "selector": selector, "start": start, "end": end });
    let ticket = format!(
        "query_logs_series:{}:{}:{payload}",
        tenant_ctx.0.tenant_slug, tenant_ctx.0.dataset_slug
    );
    let batches = execute_ticket(&state, ticket).await?;
    Ok(axum::Json(SeriesResponse::success(series_from_batches(
        &batches,
    ))))
}

// ---- execution + conversion ----

/// One hour in nanoseconds, the default lookback window.
const HOUR_NS: i64 = 3_600_000_000_000;

/// Build and execute a `query_logs` ticket, converting the result batches
/// into Loki streams.
async fn run_log_query<S: RouterState>(
    state: &S,
    tenant_ctx: &TenantContextExtractor,
    logql: &str,
    start: i64,
    end: i64,
    limit: u32,
    direction: &str,
) -> Result<Vec<Stream>, StatusCode> {
    let payload = serde_json::json!({
        "query": logql,
        "start": start,
        "end": end,
        "limit": limit,
        "direction": direction,
    });
    let ticket = format!(
        "query_logs:{}:{}:{payload}",
        tenant_ctx.0.tenant_slug, tenant_ctx.0.dataset_slug
    );
    let batches = execute_ticket(state, ticket).await?;
    Ok(batches_to_streams(&batches))
}

/// Whether a LogQL string is a metric query (returns samples) rather than
/// a log query (returns lines).
fn is_metric_query(logql: &str) -> bool {
    matches!(logql::parse(logql), Ok(logql::Expr::Metric(_)))
}

/// Build and execute a `query_metric` ticket, converting the result into a
/// Loki matrix.
async fn run_metric_query<S: RouterState>(
    state: &S,
    tenant_ctx: &TenantContextExtractor,
    logql: &str,
    start: i64,
    end: i64,
    step: i64,
) -> Result<Vec<loki_api::MetricSeries>, StatusCode> {
    let payload = serde_json::json!({
        "query": logql,
        "start": start,
        "end": end,
        "step": step,
    });
    let ticket = format!(
        "query_metric:{}:{}:{payload}",
        tenant_ctx.0.tenant_slug, tenant_ctx.0.dataset_slug
    );
    let batches = execute_ticket(state, ticket).await?;
    Ok(batches_to_matrix(&batches))
}

/// Group matrix rows (`bucket`, label columns, `value`) into Loki metric
/// series. The `bucket` column is nanoseconds; Loki matrix samples use
/// unix seconds.
fn batches_to_matrix(batches: &[RecordBatch]) -> Vec<loki_api::MetricSeries> {
    use loki_api::{MetricSeries, Sample};

    let mut order: Vec<String> = Vec::new();
    let mut series: HashMap<String, MetricSeries> = HashMap::new();

    for batch in batches {
        let Some(buckets) = timestamps_ns(batch, "bucket") else {
            continue;
        };
        let value = batch.column_by_name("value").and_then(|c| {
            c.as_any()
                .downcast_ref::<datafusion::arrow::array::Float64Array>()
        });

        // Every string column other than the value is a series label;
        // `severity_text` is presented as Loki's `level`.
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
            let mut labels: HashMap<String, String> = HashMap::new();
            for (name, col) in &label_cols {
                if !col.is_null(i) && !col.value(i).is_empty() {
                    let key = if name == "severity_text" {
                        "level"
                    } else {
                        name.as_str()
                    };
                    labels.insert(key.to_string(), col.value(i).to_string());
                }
            }
            let key = label_key(&labels);
            let seconds = if buckets.is_null(i) {
                0.0
            } else {
                buckets.value(i) as f64 / 1_000_000_000.0
            };
            let v = value
                .map(|c| if c.is_null(i) { 0.0 } else { c.value(i) })
                .unwrap_or(0.0);
            series
                .entry(key.clone())
                .or_insert_with(|| {
                    order.push(key.clone());
                    MetricSeries {
                        metric: labels,
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

/// Render a matrix value the way Loki does: an integer when whole.
fn format_value(v: f64) -> String {
    if v.fract() == 0.0 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

/// Parse the `step` parameter (Go duration or seconds) into nanoseconds.
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

/// A sensible default step when the caller omits one: aim for ~250 points
/// across the window, at least one second.
fn default_step_ns(start: i64, end: i64) -> i64 {
    let span = (end - start).max(1);
    (span / 250).max(1_000_000_000)
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
            tracing::error!(error = %e, "Failed to get Flight client for log query");
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
        tracing::error!(error = %e, "Failed to decode log Flight data");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

fn flight_status_to_http(status: &tonic::Status) -> StatusCode {
    match status.code() {
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        tonic::Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        tonic::Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        _ => {
            tracing::error!(error = %status, "Log Flight query failed");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Group the projected log rows into Loki streams by their label set,
/// preserving row order (the querier already sorts by direction).
fn batches_to_streams(batches: &[RecordBatch]) -> Vec<Stream> {
    // Preserve first-seen label-set order for stable output.
    let mut order: Vec<String> = Vec::new();
    let mut streams: HashMap<String, Stream> = HashMap::new();

    for batch in batches {
        let Some(timestamps) = timestamps_ns(batch, "timestamp") else {
            continue;
        };
        let body = str_col(batch, "body");
        let service = str_col(batch, "service_name");
        let severity = str_col(batch, "severity_text");

        for i in 0..batch.num_rows() {
            let mut label_set: HashMap<String, String> = HashMap::new();
            if let Some(v) = value_at(&service, i) {
                label_set.insert("service_name".to_string(), v);
            }
            if let Some(v) = value_at(&severity, i) {
                label_set.insert("level".to_string(), v);
            }
            let key = label_key(&label_set);
            let entry = LogEntry::new(
                if timestamps.is_null(i) {
                    0
                } else {
                    timestamps.value(i)
                },
                value_at(&body, i).unwrap_or_default(),
            );
            streams
                .entry(key.clone())
                .or_insert_with(|| {
                    order.push(key.clone());
                    Stream {
                        stream: label_set,
                        values: Vec::new(),
                    }
                })
                .values
                .push(entry);
        }
    }

    order
        .into_iter()
        .filter_map(|k| streams.remove(&k))
        .collect()
}

/// A deterministic key for a label set (sorted `k=v` pairs).
fn label_key(labels: &HashMap<String, String>) -> String {
    let mut pairs: Vec<_> = labels.iter().collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Decode a `series` JSON batch (from `query_logs_series`) into label maps.
fn series_from_batches(batches: &[RecordBatch]) -> Vec<HashMap<String, String>> {
    let mut out = Vec::new();
    for value in string_column(batches, "series") {
        if let Ok(series) = serde_json::from_str::<Vec<HashMap<String, String>>>(&value) {
            out.extend(series);
        }
    }
    out
}

/// Collect the values of a single-string-column result batch.
fn string_column(batches: &[RecordBatch], column: &str) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let col = str_col(batch, column);
        if let Some(col) = col {
            for i in 0..col.len() {
                if !col.is_null(i) {
                    out.push(col.value(i).to_string());
                }
            }
        }
    }
    out
}

fn str_col<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
}

/// Read a timestamp column as nanoseconds, casting from whatever unit the
/// storage uses (Iceberg stores microsecond timestamps, so the raw column
/// is not necessarily nanosecond).
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

fn value_at(col: &Option<&StringArray>, i: usize) -> Option<String> {
    col.filter(|c| !c.is_null(i) && !c.value(i).is_empty())
        .map(|c| c.value(i).to_string())
}

/// Resolve a metadata endpoint's `[start, end]` window in nanoseconds,
/// defaulting to the last hour.
fn metadata_window(params: &MetadataParams) -> (i64, i64) {
    let end = parse_timestamp_ns(params.end.as_deref()).unwrap_or_else(now_ns);
    let start = parse_timestamp_ns(params.start.as_deref()).unwrap_or(end - HOUR_NS);
    (start, end)
}

/// Current time as unix-epoch nanoseconds.
fn now_ns() -> i64 {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() * 1_000_000)
}

/// Parse a Loki timestamp parameter into unix-epoch nanoseconds. Accepts
/// a nanosecond integer, a smaller unix-seconds integer, or an RFC3339
/// string. Returns `None` when absent or unparseable.
fn parse_timestamp_ns(value: Option<&str>) -> Option<i64> {
    let value = value.map(str::trim).filter(|s| !s.is_empty())?;
    if let Ok(n) = value.parse::<i64>() {
        // Heuristic: values below ~year 2286 in seconds are seconds.
        return Some(if n < 10_000_000_000 {
            n.saturating_mul(1_000_000_000)
        } else {
            n
        });
    }
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .and_then(|dt| dt.timestamp_nanos_opt())
}

#[cfg(test)]
mod tests {
    use crate::{InMemoryStateImpl, create_router};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use common::catalog::Catalog;
    use common::config::{ApiKeyConfig, Configuration, TenantConfig};
    use tower::ServiceExt;

    fn test_config() -> Configuration {
        let mut config = Configuration::default();
        config.auth = common::config::AuthConfig {
            tenants: vec![TenantConfig {
                id: "acme".to_string(),
                slug: "acme".to_string(),
                name: "Acme".to_string(),
                default_dataset: Some("default".to_string()),
                datasets: vec![],
                api_keys: vec![ApiKeyConfig {
                    key: "sk-test-key".to_string(),
                    name: Some("test".to_string()),
                }],
                schema_config: None,
                limits: None,
            }],
            ..Default::default()
        };
        config
    }

    async fn test_app() -> axum::Router {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        create_router(InMemoryStateImpl::new(catalog, test_config()))
    }

    async fn get_json(app: &axum::Router, uri: &str) -> (StatusCode, Option<serde_json::Value>) {
        let request = Request::builder()
            .uri(uri)
            .header("authorization", "Bearer sk-test-key")
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).ok())
    }

    #[tokio::test]
    async fn query_without_query_param_is_bad_request() {
        let app = test_app().await;
        let (status, _) = get_json(&app, "/loki/api/v1/query").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = get_json(&app, "/loki/api/v1/query_range?query=").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn invalid_direction_is_bad_request() {
        let app = test_app().await;
        let (status, _) = get_json(
            &app,
            "/loki/api/v1/query_range?query=%7Bjob%3D%22x%22%7D&direction=sideways",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn series_without_selector_is_bad_request() {
        let app = test_app().await;
        let (status, _) = get_json(&app, "/loki/api/v1/series").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn query_without_a_querier_is_service_unavailable() {
        // A valid query with no discovered querier surfaces 503, not 200.
        let app = test_app().await;
        let (status, _) = get_json(&app, "/loki/api/v1/query?query=%7Bjob%3D%22x%22%7D").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn logql_endpoints_require_authentication() {
        let app = test_app().await;

        // Missing Authorization header is a 400 (middleware contract),
        // a wrong key a 401.
        let request = Request::builder()
            .uri("/loki/api/v1/labels")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let request = Request::builder()
            .uri("/loki/api/v1/labels")
            .header("authorization", "Bearer sk-wrong-key")
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    // ---- Arrow -> Loki conversion (#377) ----

    mod convert {
        use super::super::{
            batches_to_matrix, batches_to_streams, default_step_ns, is_metric_query, parse_step_ns,
            parse_timestamp_ns, series_from_batches, string_column,
        };
        use datafusion::arrow::array::{
            Float64Array, RecordBatch, StringArray, TimestampNanosecondArray,
        };
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use std::sync::Arc;

        fn log_batch() -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("body", DataType::Utf8, true),
                Field::new("service_name", DataType::Utf8, true),
                Field::new("severity_text", DataType::Utf8, true),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(TimestampNanosecondArray::from(vec![100, 200, 300])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                    Arc::new(StringArray::from(vec!["api", "api", "web"])),
                    Arc::new(StringArray::from(vec!["error", "error", "info"])),
                ],
            )
            .unwrap()
        }

        #[test]
        fn groups_rows_into_streams_by_label_set() {
            let streams = batches_to_streams(&[log_batch()]);
            // {api,error} has two entries; {web,info} has one.
            assert_eq!(streams.len(), 2);
            let api = streams
                .iter()
                .find(|s| s.stream.get("service_name") == Some(&"api".to_string()))
                .unwrap();
            assert_eq!(api.stream.get("level"), Some(&"error".to_string()));
            assert_eq!(api.values.len(), 2);
            assert_eq!(api.values[0], loki_api::LogEntry::new(100, "a"));
            assert_eq!(api.values[1], loki_api::LogEntry::new(200, "b"));

            let web = streams
                .iter()
                .find(|s| s.stream.get("service_name") == Some(&"web".to_string()))
                .unwrap();
            assert_eq!(web.values, vec![loki_api::LogEntry::new(300, "c")]);
        }

        #[test]
        fn string_column_collects_non_null_values() {
            let schema = Arc::new(Schema::new(vec![Field::new("label", DataType::Utf8, true)]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("b"),
                ]))],
            )
            .unwrap();
            assert_eq!(string_column(&[batch], "label"), vec!["a", "b"]);
        }

        #[test]
        fn series_json_is_decoded() {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "series",
                DataType::Utf8,
                true,
            )]));
            let json = r#"[{"service_name":"api","level":"error"}]"#;
            let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![json]))])
                .unwrap();
            let series = series_from_batches(&[batch]);
            assert_eq!(series.len(), 1);
            assert_eq!(series[0].get("service_name"), Some(&"api".to_string()));
        }

        #[test]
        fn timestamp_parsing_handles_ns_seconds_and_rfc3339() {
            assert_eq!(
                parse_timestamp_ns(Some("1700000000000000000")),
                Some(1_700_000_000_000_000_000)
            );
            // Seconds are scaled up to nanoseconds.
            assert_eq!(
                parse_timestamp_ns(Some("1700000000")),
                Some(1_700_000_000_000_000_000)
            );
            assert_eq!(
                parse_timestamp_ns(Some("2023-11-14T22:13:20Z")),
                Some(1_700_000_000_000_000_000)
            );
            assert_eq!(parse_timestamp_ns(None), None);
            assert_eq!(parse_timestamp_ns(Some("")), None);
            assert_eq!(parse_timestamp_ns(Some("garbage")), None);
        }

        fn matrix_batch() -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "bucket",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("service_name", DataType::Utf8, true),
                Field::new("severity_text", DataType::Utf8, true),
                Field::new("value", DataType::Float64, false),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    // Two buckets for the api/error series, one for web/info.
                    Arc::new(TimestampNanosecondArray::from(vec![
                        1_000_000_000,
                        2_000_000_000,
                        1_000_000_000,
                    ])),
                    Arc::new(StringArray::from(vec!["api", "api", "web"])),
                    Arc::new(StringArray::from(vec!["error", "error", "info"])),
                    Arc::new(Float64Array::from(vec![2.0, 3.0, 1.5])),
                ],
            )
            .unwrap()
        }

        #[test]
        fn groups_matrix_rows_into_series() {
            let series = batches_to_matrix(&[matrix_batch()]);
            assert_eq!(series.len(), 2);
            let api = series
                .iter()
                .find(|s| s.metric.get("service_name") == Some(&"api".to_string()))
                .unwrap();
            // severity_text is presented as Loki's `level`.
            assert_eq!(api.metric.get("level"), Some(&"error".to_string()));
            // Samples: (seconds, value string); whole values render as ints.
            assert_eq!(api.values.len(), 2);
            assert_eq!(api.values[0], loki_api::Sample::new(1.0, "2"));
            assert_eq!(api.values[1], loki_api::Sample::new(2.0, "3"));

            let web = series
                .iter()
                .find(|s| s.metric.get("service_name") == Some(&"web".to_string()))
                .unwrap();
            assert_eq!(web.values, vec![loki_api::Sample::new(1.0, "1.5")]);
        }

        #[test]
        fn detects_metric_vs_log_queries() {
            assert!(is_metric_query(r#"rate({app="x"}[5m])"#));
            assert!(is_metric_query(
                r#"sum by (level) (count_over_time({a="b"}[1m]))"#
            ));
            assert!(!is_metric_query(r#"{app="x"}"#));
            assert!(!is_metric_query(r#"{app="x"} |= "err""#));
        }

        #[test]
        fn step_parsing_handles_seconds_and_durations() {
            assert_eq!(parse_step_ns(Some("30")), Some(30_000_000_000));
            assert_eq!(parse_step_ns(Some("5m")), Some(300_000_000_000));
            assert_eq!(parse_step_ns(Some("1h")), Some(3_600_000_000_000));
            assert_eq!(parse_step_ns(None), None);
            // ~250 points across a 1000s window, floored at one second.
            assert_eq!(default_step_ns(0, 1_000_000_000_000), 4_000_000_000);
            assert_eq!(default_step_ns(0, 10), 1_000_000_000);
        }
    }
}
