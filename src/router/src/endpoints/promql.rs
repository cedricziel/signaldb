//! # Prometheus-Compatible HTTP API (PromQL)
//!
//! Query endpoints for the metrics signal in the format Grafana's
//! Prometheus datasource expects, nested under `/prometheus`:
//!
//! - `GET|POST /api/v1/query_range` — range query → matrix
//! - `GET|POST /api/v1/query` — instant query → vector
//! - `GET /api/v1/labels`, `/api/v1/label/{name}/values`, `/api/v1/series`
//!
//! The query handlers lower PromQL to a Query IR document
//! (`ql_ir::promql_to_ir`), run it the way `POST /api/v1/query` runs one, and
//! shape the metric Series or Scalar result into Prometheus JSON. Metadata
//! endpoints (labels/values/series) query the metrics tables via the querier.

use std::collections::{BTreeMap, HashMap};
use tracing::Instrument;

use super::api_error::ApiError;
use super::query::DecodedSeries;
use crate::RouterAppState;
use arrow_flight::Ticket;
use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
};
use common::auth::TenantContextExtractor;
use common::catalog::{AttributeStatsRecord, Catalog};
use common::flight::transport::ServiceCapability;
use common::query_ir::ResultEnvelope;
use datafusion::arrow::array::{Array, Float64Array, RecordBatch, StringArray};
use futures::StreamExt;
use prometheus_api::{
    InstantVector, LabelStat, LabelStatsResponse, LabelsResponse, QueryResponse, QueryResult,
    RangeVector, Sample, SeriesResponse,
};
use serde::Deserialize;

pub fn router() -> Router<RouterAppState> {
    Router::new()
        .route("/api/v1/query", get(query).post(query))
        .route("/api/v1/query_range", get(query_range).post(query_range))
        .route("/api/v1/labels", get(labels))
        .route("/api/v1/label/{name}/values", get(label_values))
        .route("/api/v1/label_stats", get(label_stats))
        .route("/api/v1/series", get(series))
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
#[utoipa::path(
    get,
    path = "/prometheus/api/v1/query_range",
    operation_id = "promql_query_range",
    tag = "metrics",
    security(("bearerAuth" = [])),
    params(
        ("query" = String, Query, description = "PromQL expression"),
        ("start" = Option<String>, Query, description = "Range start (unix seconds or RFC3339)"),
        ("end" = Option<String>, Query, description = "Range end (unix seconds or RFC3339)"),
        ("step" = Option<String>, Query, description = "Resolution step (Go duration or seconds)"),
    ),
    responses(
        (status = 200, description = "Prometheus range-query response (matrix)", body = serde_json::Value),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
    )
)]
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(signaldb.tenant.id = %tenant_ctx.0.tenant_id, signaldb.dataset.id = %tenant_ctx.0.dataset_id)
)]
pub async fn query_range(
    State(state): State<RouterAppState>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<RangeParams>,
) -> Result<axum::Json<QueryResponse>, ApiError> {
    let Some(promql) = non_empty(&params.query) else {
        return Ok(axum::Json(QueryResponse::error(
            "bad_data",
            "missing or empty 'query'",
        )));
    };
    let end = parse_timestamp_ns(params.end.as_deref()).unwrap_or_else(super::now_ns);
    let start = parse_timestamp_ns(params.start.as_deref()).unwrap_or(end - HOUR_NS);
    let step = parse_step_ns(params.step.as_deref()).unwrap_or_else(|| default_step_ns(start, end));

    let params = ql_ir::PromqlParams::range(start, end, step);
    // A range query answers a matrix even for a scalar expression: one
    // label-less series.
    let (_, series) = run_promql(&state, &tenant_ctx, &promql, &params).await?;
    Ok(axum::Json(QueryResponse::success(QueryResult::Matrix(
        series.into_iter().map(range_vector).collect(),
    ))))
}

/// GET|POST /prometheus/api/v1/query — instant query.
///
/// Evaluated as a one-bucket range at `time`, returning the latest sample
/// per series as a vector.
#[utoipa::path(
    get,
    path = "/prometheus/api/v1/query",
    operation_id = "promql_query",
    tag = "metrics",
    security(("bearerAuth" = [])),
    params(
        ("query" = String, Query, description = "PromQL expression"),
        ("time" = Option<String>, Query, description = "Evaluation timestamp (unix seconds or RFC3339)"),
    ),
    responses(
        (status = 200, description = "Prometheus instant-query response (vector)", body = serde_json::Value),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
    )
)]
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(signaldb.tenant.id = %tenant_ctx.0.tenant_id, signaldb.dataset.id = %tenant_ctx.0.dataset_id)
)]
pub async fn query(
    State(state): State<RouterAppState>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<InstantParams>,
) -> Result<axum::Json<QueryResponse>, ApiError> {
    let Some(promql) = non_empty(&params.query) else {
        return Ok(axum::Json(QueryResponse::error(
            "bad_data",
            "missing or empty 'query'",
        )));
    };
    // Evaluated once, at `time`: each series' value at that instant (its
    // latest point in the lookback), or a scalar for a scalar expression.
    let at = parse_timestamp_ns(params.time.as_deref()).unwrap_or_else(super::now_ns);
    let params = ql_ir::PromqlParams::instant(at);
    let (envelope, series) = run_promql(&state, &tenant_ctx, &promql, &params).await?;
    let result = match envelope {
        ResultEnvelope::Scalar => {
            let value = series
                .into_iter()
                .next()
                .and_then(|(_, points)| value_at(points, at))
                .unwrap_or(f64::NAN);
            QueryResult::Scalar(sample(at, value))
        }
        _ => QueryResult::Vector(
            series
                .into_iter()
                .filter_map(|(labels, points)| {
                    Some(InstantVector {
                        metric: prometheus_labels(labels),
                        value: sample(at, value_at(points, at)?),
                    })
                })
                .collect(),
        ),
    };
    Ok(axum::Json(QueryResponse::success(result)))
}

/// GET /prometheus/api/v1/labels — metric label names.
#[utoipa::path(
    get,
    path = "/prometheus/api/v1/labels",
    operation_id = "promql_labels",
    tag = "metrics",
    security(("bearerAuth" = [])),
    params(
        ("start" = Option<String>, Query, description = "Range start (unix seconds or RFC3339)"),
        ("end" = Option<String>, Query, description = "Range end (unix seconds or RFC3339)"),
    ),
    responses(
        (status = 200, description = "Known metric label names", body = serde_json::Value),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
    )
)]
pub async fn labels(
    State(state): State<RouterAppState>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<MetadataParams>,
) -> Result<axum::Json<LabelsResponse>, ApiError> {
    let (start, end) = metadata_window(&params);
    let ticket = format!(
        "query_metric_labels:{}:{}:{start}:{end}",
        tenant_ctx.0.tenant_slug, tenant_ctx.0.dataset_slug
    );
    let batches = execute_ticket(&state, ticket).await?;
    Ok(axum::Json(LabelsResponse::success(string_column(
        &batches, "label",
    ))))
}

/// GET /prometheus/api/v1/label/{name}/values — distinct values of a label.
#[utoipa::path(
    get,
    path = "/prometheus/api/v1/label/{name}/values",
    operation_id = "promql_label_values",
    tag = "metrics",
    security(("bearerAuth" = [])),
    params(
        ("name" = String, Path, description = "Label name to list values for"),
        ("start" = Option<String>, Query, description = "Range start (unix seconds or RFC3339)"),
        ("end" = Option<String>, Query, description = "Range end (unix seconds or RFC3339)"),
    ),
    responses(
        (status = 200, description = "Distinct values for the label", body = serde_json::Value),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
    )
)]
pub async fn label_values(
    State(state): State<RouterAppState>,
    tenant_ctx: TenantContextExtractor,
    Path(name): Path<String>,
    Query(params): Query<MetadataParams>,
) -> Result<axum::Json<LabelsResponse>, ApiError> {
    let name = name.trim();
    if name.is_empty() {
        return Err(ApiError::bad_request("label name must not be empty"));
    }
    let (start, end) = metadata_window(&params);
    let ticket = format!(
        "query_metric_label_values:{}:{}:{name}:{start}:{end}",
        tenant_ctx.0.tenant_slug, tenant_ctx.0.dataset_slug
    );
    let batches = execute_ticket(&state, ticket).await?;
    Ok(axum::Json(LabelsResponse::success(string_column(
        &batches, "value",
    ))))
}

/// GET /prometheus/api/v1/series — series matching a selector.
pub async fn series(
    State(state): State<RouterAppState>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<MetadataParams>,
) -> Result<axum::Json<SeriesResponse>, ApiError> {
    let selector = params
        .matcher
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ApiError::bad_request("missing or empty 'match[]' selector"))?;
    let (start, end) = metadata_window(&params);
    let payload = serde_json::json!({ "selector": selector, "start": start, "end": end });
    let ticket = format!(
        "query_metric_series:{}:{}:{payload}",
        tenant_ctx.0.tenant_slug, tenant_ctx.0.dataset_slug
    );
    let batches = execute_ticket(&state, ticket).await?;
    Ok(axum::Json(SeriesResponse::success(series_from_batches(
        &batches,
    ))))
}

/// The signal attribute stats for metric labels are recorded under.
const METRICS_SIGNAL: &str = "metrics";

/// GET /prometheus/api/v1/label_stats — per-label cardinality stats.
///
/// Reads the compactor's advisory attribute statistics straight from the
/// catalog (no querier round-trip), so the metrics explorer can warn before a
/// user groups by a high-cardinality label. Names match `/api/v1/labels`.
pub async fn label_stats(
    State(state): State<RouterAppState>,
    tenant_ctx: TenantContextExtractor,
) -> Result<axum::Json<LabelStatsResponse>, ApiError> {
    let stats = fetch_label_stats(
        state.catalog(),
        &tenant_ctx.0.tenant_slug,
        &tenant_ctx.0.dataset_slug,
    )
    .await
    .map_err(|error| {
        tracing::error!(?error, "failed to read attribute stats");
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to read attribute stats: {error}"),
        )
    })?;
    Ok(axum::Json(LabelStatsResponse::success(stats)))
}

/// Read and shape the metric-signal attribute stats for a tenant/dataset.
async fn fetch_label_stats(
    catalog: &Catalog,
    tenant_slug: &str,
    dataset_slug: &str,
) -> anyhow::Result<Vec<LabelStat>> {
    let records = catalog
        .get_attribute_stats(tenant_slug, dataset_slug, METRICS_SIGNAL)
        .await?;
    Ok(records.into_iter().map(label_stat_from_record).collect())
}

/// Shape one catalog record into the API's `LabelStat`, deriving presence.
fn label_stat_from_record(record: AttributeStatsRecord) -> LabelStat {
    let presence = record.coverage().unwrap_or(0.0);
    LabelStat {
        name: record.attr_key,
        distinct_estimate: record.distinct_estimate,
        presence,
        capped: record.capped,
    }
}

// ---- execution + conversion ----

/// Lower PromQL to an IR document and run it the way `POST /api/v1/query`
/// runs one. Every lowering failure is the caller's (400).
async fn run_promql(
    state: &RouterAppState,
    tenant_ctx: &TenantContextExtractor,
    promql: &str,
    params: &ql_ir::PromqlParams,
) -> Result<(ResultEnvelope, Vec<DecodedSeries<f64>>), ApiError> {
    let document =
        ql_ir::promql_to_ir(promql, params).map_err(|e| ApiError::bad_request(e.to_string()))?;
    let ticket = super::query::query_ir_ticket(&tenant_ctx.0, &document, super::now_ns())?;
    let (batches, _correlate_truncated) = super::query::execute_ticket(state, ticket).await?;
    let series = super::query::decode_series(&batches, |array, row| {
        array
            .as_any()
            .downcast_ref::<Float64Array>()
            .filter(|values| values.is_valid(row))
            .map_or(f64::NAN, |values| values.value(row))
    })?;
    Ok((document.result, series))
}

/// Send a Flight ticket to a querier and collect the result batches.
async fn execute_ticket(
    state: &RouterAppState,
    ticket_content: String,
) -> Result<Vec<RecordBatch>, ApiError> {
    let (mut client, server_address) = state
        .service_registry()
        .get_flight_client_and_address_for_capability(ServiceCapability::QueryExecution)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to get Flight client for PromQL query");
            ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "no querier available")
        })?;

    let verb = common::self_monitoring::spans::ticket_verb(&ticket_content).map(str::to_owned);
    let ticket = Ticket::new(ticket_content);
    let mut flight_request = tonic::Request::new(ticket);
    let rpc_span = common::flight::trace_context::do_get_client_span(
        verb.as_deref(),
        &mut flight_request,
        Some(&server_address),
    );
    if let Some(key) = &state.config().auth.internal_service_key {
        common::flight::auth::attach_internal_auth(&mut flight_request, key);
    }

    let mut stream = client
        .do_get(flight_request)
        .instrument(rpc_span.clone())
        .await
        .map_err(|e| rpc_span.in_scope(|| ApiError::from_flight(&e, "promql")))?
        .into_inner();

    let mut data = Vec::new();
    while let Some(flight_data) = stream.next().await {
        data.push(flight_data.map_err(|e| ApiError::from_flight(&e, "promql"))?);
    }

    super::flight_decode::decode_flight_batches(data, "promql")
        .await
        .map_err(ApiError::from)
}

fn range_vector((labels, points): DecodedSeries<f64>) -> RangeVector {
    RangeVector {
        metric: prometheus_labels(labels),
        values: points
            .into_iter()
            .filter_map(|(t, v)| Some(sample(t.as_i64()?, v)))
            .collect(),
    }
}

/// A series' value at the evaluation instant `at_ns`.
fn value_at(points: Vec<(serde_json::Value, f64)>, at_ns: i64) -> Option<f64> {
    points
        .into_iter()
        .find_map(|(t, v)| (t.as_i64() == Some(at_ns)).then_some(v))
}

fn sample(t_ns: i64, v: f64) -> Sample {
    Sample::new(t_ns as f64 / 1_000_000_000.0, format_value(v))
}

/// A Series label set under Prometheus label names: `metric.name` is
/// `__name__` and `service.name` is `service_name`; every other label keeps
/// its IR name. A label literally named `service_name` wins over the
/// renamed `service.name`.
fn prometheus_labels(labels: BTreeMap<String, String>) -> HashMap<String, String> {
    let mut metric = HashMap::with_capacity(labels.len());
    let mut renamed = Vec::new();
    for (name, value) in labels {
        match name.as_str() {
            "metric.name" => renamed.push(("__name__", value)),
            "service.name" => renamed.push(("service_name", value)),
            _ => {
                metric.insert(name, value);
            }
        }
    }
    for (name, value) in renamed {
        metric.entry(name.to_string()).or_insert(value);
    }
    metric
}

/// A sample value as Prometheus renders it: Go's shortest float text, with
/// `NaN`, `+Inf` and `-Inf` spelled out.
fn format_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 { "+Inf" } else { "-Inf" }.to_string()
    } else {
        format!("{v}")
    }
}

fn str_col<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
}

/// Collect the values of a single-string-column result batch.
fn string_column(batches: &[RecordBatch], column: &str) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        if let Some(col) = str_col(batch, column) {
            for i in 0..col.len() {
                if !col.is_null(i) {
                    out.push(col.value(i).to_string());
                }
            }
        }
    }
    out
}

/// Decode a `series` JSON batch into label maps.
fn series_from_batches(batches: &[RecordBatch]) -> Vec<HashMap<String, String>> {
    let mut out = Vec::new();
    for value in string_column(batches, "series") {
        if let Ok(series) = serde_json::from_str::<Vec<HashMap<String, String>>>(&value) {
            out.extend(series);
        }
    }
    out
}

/// Resolve a metadata endpoint's `[start, end]` window in nanoseconds,
/// defaulting to the last hour.
fn metadata_window(params: &MetadataParams) -> (i64, i64) {
    let end = parse_timestamp_ns(params.end.as_deref()).unwrap_or_else(super::now_ns);
    let start = parse_timestamp_ns(params.start.as_deref()).unwrap_or(end - HOUR_NS);
    (start, end)
}

fn non_empty(value: &Option<String>) -> Option<String> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
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
    use datafusion::arrow::array::TimestampNanosecondArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    /// An IR metric Series frame: `bucket`, `__labels`, `value`.
    fn series_batch(rows: Vec<(i64, &str, f64)>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "bucket",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("__labels", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn decode(batch: RecordBatch) -> Vec<DecodedSeries<f64>> {
        super::super::query::decode_series(&[batch], |array, row| {
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row)
        })
        .unwrap()
    }

    const API: &str = r#"{"code":"200","metric.name":"reqs","service.name":"api"}"#;
    const WEB: &str = r#"{"code":"500","service.name":"web"}"#;

    #[test]
    fn a_series_frame_becomes_a_matrix_under_prometheus_label_names() {
        let matrix: Vec<RangeVector> = decode(series_batch(vec![
            (1_000_000_000, API, 2.0),
            (2_000_000_000, API, f64::NAN),
            (1_000_000_000, WEB, f64::INFINITY),
            (2_000_000_000, WEB, f64::NEG_INFINITY),
        ]))
        .into_iter()
        .map(range_vector)
        .collect();

        assert_eq!(matrix.len(), 2);
        let api = &matrix[0];
        assert_eq!(
            api.metric,
            HashMap::from([
                ("__name__".to_string(), "reqs".to_string()),
                ("service_name".to_string(), "api".to_string()),
                ("code".to_string(), "200".to_string()),
            ])
        );
        assert_eq!(
            api.values,
            vec![Sample::new(1.0, "2"), Sample::new(2.0, "NaN")]
        );
        // The lowering dropped `metric.name`; it stays dropped.
        let web = &matrix[1];
        assert!(!web.metric.contains_key("__name__"));
        assert_eq!(
            web.values,
            vec![Sample::new(1.0, "+Inf"), Sample::new(2.0, "-Inf")]
        );
    }

    #[test]
    fn an_instant_value_is_the_point_at_the_evaluation_time() {
        let series = decode(series_batch(vec![
            (1_000_000_000, API, 2.0),
            (2_000_000_000, API, 3.0),
        ]));
        let (_, points) = series.into_iter().next().unwrap();
        assert_eq!(value_at(points.clone(), 1_000_000_000), Some(2.0));
        assert_eq!(value_at(points, 3_000_000_000), None);
    }

    #[test]
    fn a_point_label_named_service_name_wins_over_the_renamed_service() {
        let labels = BTreeMap::from([
            ("service.name".to_string(), "api".to_string()),
            ("service_name".to_string(), "own".to_string()),
        ]);
        assert_eq!(
            prometheus_labels(labels),
            HashMap::from([("service_name".to_string(), "own".to_string())])
        );
    }

    #[test]
    fn value_formatting() {
        assert_eq!(format_value(3.0), "3");
        assert_eq!(format_value(2.5), "2.5");
        assert_eq!(format_value(-0.25), "-0.25");
        assert_eq!(format_value(1e20), "100000000000000000000");
        assert_eq!(format_value(f64::NAN), "NaN");
        assert_eq!(format_value(f64::INFINITY), "+Inf");
        assert_eq!(format_value(f64::NEG_INFINITY), "-Inf");
    }

    async fn get_status(uri: &str) -> (StatusCode, serde_json::Value) {
        use tower::ServiceExt;
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let mut config = common::config::Configuration::default();
        config.auth.tenants = vec![common::config::TenantConfig {
            id: "acme".into(),
            slug: "acme".into(),
            name: "Acme".into(),
            default_dataset: Some("default".into()),
            datasets: vec![],
            api_keys: vec![common::config::ApiKeyConfig {
                key: "sk-test-key".into(),
                name: Some("test".into()),
            }],
            schema_config: None,
            limits: None,
        }];
        let app = crate::create_router(RouterAppState::new(catalog, config));
        let request = axum::http::Request::builder()
            .uri(uri)
            .header("authorization", "Bearer sk-test-key")
            .header("x-tenant-id", "acme")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    #[tokio::test]
    async fn invalid_or_inexpressible_promql_is_bad_data_before_any_querier() {
        // No querier is registered: a 400 proves the lowering rejected the
        // query before execution was attempted.
        for query in ["sum(", "requests%5B5m%5D", "requests%20offset%20-5m"] {
            for uri in [
                format!("/prometheus/api/v1/query?query={query}&time=1700000000"),
                format!(
                    "/prometheus/api/v1/query_range?query={query}&start=1700000000&end=1700000060&step=15"
                ),
            ] {
                let (status, body) = get_status(&uri).await;
                assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
                assert_eq!(body["status"], "error", "{uri}: {body}");
                assert_eq!(body["errorType"], "bad_data", "{uri}: {body}");
            }
        }
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

    #[test]
    fn label_stat_derives_presence_and_passes_through() {
        let stat = label_stat_from_record(AttributeStatsRecord {
            tenant_id: "acme".into(),
            dataset_id: "prod".into(),
            signal: "metrics".into(),
            attr_key: "http.route".into(),
            present_rows: 3,
            total_rows: 4,
            distinct_estimate: 86,
            capped: false,
            query_hits: 0,
            promote_streak: 0,
            updated_at: "2026-08-17 09:00:00".into(),
        });
        assert_eq!(stat.name, "http.route");
        assert_eq!(stat.distinct_estimate, 86);
        assert_eq!(stat.presence, 0.75);
        assert!(!stat.capped);
    }

    #[test]
    fn label_stat_presence_is_zero_when_no_rows_scanned() {
        let stat = label_stat_from_record(AttributeStatsRecord {
            tenant_id: "acme".into(),
            dataset_id: "prod".into(),
            signal: "metrics".into(),
            attr_key: "k8s.pod".into(),
            present_rows: 0,
            total_rows: 0,
            distinct_estimate: 0,
            capped: false,
            query_hits: 0,
            promote_streak: 0,
            updated_at: "2026-08-17 09:00:00".into(),
        });
        assert_eq!(stat.presence, 0.0);
    }

    #[tokio::test]
    async fn fetch_label_stats_returns_only_the_metrics_signal() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        // Two metrics keys — one a high-cardinality, capped label.
        catalog
            .upsert_attribute_scan_stats("acme", "prod", "metrics", "service", 100, 100, 12, false)
            .await
            .unwrap();
        catalog
            .upsert_attribute_scan_stats(
                "acme", "prod", "metrics", "k8s.pod", 90, 100, 10_000, true,
            )
            .await
            .unwrap();
        // A logs key and another dataset must be excluded.
        catalog
            .upsert_attribute_scan_stats("acme", "prod", "logs", "trace.id", 100, 100, 9000, true)
            .await
            .unwrap();
        catalog
            .upsert_attribute_scan_stats("acme", "staging", "metrics", "region", 10, 10, 3, false)
            .await
            .unwrap();

        let stats = fetch_label_stats(&catalog, "acme", "prod").await.unwrap();

        // Ordered by attr_key (catalog ORDER BY): k8s.pod, service.
        let names: Vec<_> = stats.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["k8s.pod", "service"]);

        let pod = &stats[0];
        assert_eq!(pod.distinct_estimate, 10_000);
        assert!(pod.capped);
        assert_eq!(pod.presence, 0.9);

        let service = &stats[1];
        assert_eq!(service.distinct_estimate, 12);
        assert!(!service.capped);
        assert_eq!(service.presence, 1.0);
    }

    #[tokio::test]
    async fn fetch_label_stats_is_empty_for_unknown_dataset() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let stats = fetch_label_stats(&catalog, "nobody", "nowhere")
            .await
            .unwrap();
        assert!(stats.is_empty());
    }
}
