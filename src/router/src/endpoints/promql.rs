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
//! JSON. Metadata endpoints (labels/values/series) query the metrics tables via the querier.

use std::collections::HashMap;
use tracing::Instrument;

use super::api_error::ApiError;
use crate::RouterState;
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
use datafusion::arrow::array::{
    Array, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use futures::StreamExt;
use prometheus_api::{
    InstantVector, LabelStat, LabelStatsResponse, LabelsResponse, QueryResponse, QueryResult,
    RangeVector, Sample, SeriesResponse,
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
        .route("/api/v1/label_stats", get(label_stats::<S>))
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
    )
)]
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(signaldb.tenant.id = %tenant_ctx.0.tenant_id, signaldb.dataset.id = %tenant_ctx.0.dataset_id)
)]
pub async fn query_range<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<RangeParams>,
) -> Result<axum::Json<QueryResponse>, ApiError> {
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
    )
)]
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(signaldb.tenant.id = %tenant_ctx.0.tenant_id, signaldb.dataset.id = %tenant_ctx.0.dataset_id)
)]
pub async fn query<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<InstantParams>,
) -> Result<axum::Json<QueryResponse>, ApiError> {
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
    )
)]
pub async fn labels<S: RouterState>(
    State(state): State<S>,
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
    )
)]
pub async fn label_values<S: RouterState>(
    State(state): State<S>,
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
pub async fn series<S: RouterState>(
    State(state): State<S>,
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
pub async fn label_stats<S: RouterState>(
    State(state): State<S>,
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
    let presence = if record.total_rows > 0 {
        record.present_rows as f64 / record.total_rows as f64
    } else {
        0.0
    };
    LabelStat {
        name: record.attr_key,
        distinct_estimate: record.distinct_estimate,
        presence,
        capped: record.capped,
    }
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
) -> Result<Vec<RecordBatch>, ApiError> {
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
                // `metric_name` is Prometheus's `__name__`; materialized
                // `label_<key>` columns surface under their label name.
                let key = if name == "metric_name" {
                    "__name__"
                } else {
                    name.strip_prefix("label_").unwrap_or(name.as_str())
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
    let end = parse_timestamp_ns(params.end.as_deref()).unwrap_or_else(now_ns);
    let start = parse_timestamp_ns(params.start.as_deref()).unwrap_or(end - HOUR_NS);
    (start, end)
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
