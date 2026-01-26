use crate::RouterState;
use arrow_flight::{FlightData, Ticket};
use axum::{
    Router,
    extract::{Path, Query, State},
    routing::get,
};
use common::auth::TenantContextExtractor;
use common::flight::transport::ServiceCapability;
use datafusion::arrow::{
    array::{BooleanArray, StringArray, UInt64Array},
    ipc::reader::StreamReader,
    record_batch::RecordBatch,
};
use futures::StreamExt;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Write;
use tempo_api::{
    self, MetricSeries, MetricsData, MetricsQueryParams, MetricsRangeQueryParams, MetricsResponse,
    TraceQueryParams,
};

/// Query parameters for v2 tag search
#[derive(Debug, Deserialize)]
pub struct TagSearchV2Params {
    pub scope: Option<tempo_api::TagScope>,
}

/// Query parameters for v2 tag value search
#[derive(Debug, Deserialize)]
pub struct TagValueSearchV2Params {
    pub start: Option<i32>,
    pub end: Option<i32>,
    pub q: Option<String>,
}

pub fn router<S: RouterState>() -> Router<S> {
    Router::new()
        .route("/api/echo", get(echo))
        .route("/api/traces/{trace_id}", get(query_single_trace::<S>))
        .route("/api/search", get(search::<S>))
        .route("/api/search/tags", get(search_tags))
        .route("/api/search/tag/{tag_name}/values", get(search_tag_values))
        // v2 routes
        .route("/api/v2/traces/{trace_id}", get(query_single_trace::<S>)) // V2 uses same handler for now
        .route("/api/v2/search/tags", get(search_tags_v2))
        .route(
            "/api/v2/search/tag/{tag_name}/values",
            get(search_tag_values_v2),
        )
        // metrics endpoints
        .route("/api/metrics/query", get(metrics_query::<S>))
        .route("/api/metrics/query_range", get(metrics_query_range::<S>))
}

/// Convert Arrow FlightData to internal trace model, then to Tempo API format
fn flight_data_to_tempo_trace(
    flight_data: Vec<FlightData>,
    trace_id: &str,
) -> Result<Option<tempo_api::Trace>, Box<dyn std::error::Error + Send + Sync>> {
    if flight_data.is_empty() {
        return Ok(None);
    }

    // Convert FlightData to RecordBatches
    let mut cursor = std::io::Cursor::new(Vec::new());
    for data in &flight_data {
        cursor.write_all(&data.data_body)?;
    }
    cursor.set_position(0);

    let reader = StreamReader::try_new(cursor, None)?;
    let batches: Result<Vec<RecordBatch>, _> = reader.collect();
    let batches = batches?;

    if batches.is_empty() {
        return Ok(None);
    }

    // Convert RecordBatches to internal trace model
    let trace = record_batches_to_trace(batches, trace_id)?;

    // Convert internal trace model to Tempo API format
    let tempo_trace = internal_trace_to_tempo(&trace);

    Ok(Some(tempo_trace))
}

/// Convert Arrow RecordBatches to internal trace model
fn record_batches_to_trace(
    batches: Vec<RecordBatch>,
    trace_id: &str,
) -> Result<common::model::trace::Trace, Box<dyn std::error::Error + Send + Sync>> {
    let mut span_map: HashMap<String, common::model::span::Span> = HashMap::new();

    // Process all batches and collect spans
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let span_trace_id = batch
                .column_by_name("trace_id")
                .ok_or("Missing trace_id column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Invalid trace_id column type")?
                .value(row_index)
                .to_string();

            // Only include spans that match the requested trace_id
            if span_trace_id != trace_id {
                continue;
            }

            let span_id = batch
                .column_by_name("span_id")
                .ok_or("Missing span_id column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Invalid span_id column type")?
                .value(row_index)
                .to_string();

            let parent_span_id = batch
                .column_by_name("parent_span_id")
                .ok_or("Missing parent_span_id column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Invalid parent_span_id column type")?
                .value(row_index)
                .to_string();

            let name = batch
                .column_by_name("span_name")
                .ok_or("Missing span_name column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Invalid span_name column type")?
                .value(row_index)
                .to_string();

            let service_name = batch
                .column_by_name("service_name")
                .ok_or("Missing service_name column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Invalid service_name column type")?
                .value(row_index)
                .to_string();

            let span_kind_str = batch
                .column_by_name("span_kind")
                .ok_or("Missing span_kind column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Invalid span_kind column type")?
                .value(row_index);

            let start_time_unix_nano = batch
                .column_by_name("start_time_unix_nano")
                .ok_or("Missing start_time_unix_nano column")?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or("Invalid start_time_unix_nano column type")?
                .value(row_index);

            let duration_nano = batch
                .column_by_name("duration_nano")
                .ok_or("Missing duration_nano column")?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or("Invalid duration_nano column type")?
                .value(row_index);

            let status_str = batch
                .column_by_name("status_code")
                .ok_or("Missing status_code column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Invalid status_code column type")?
                .value(row_index);

            let is_root = batch
                .column_by_name("is_root")
                .ok_or("Missing is_root column")?
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("Invalid is_root column type")?
                .value(row_index);

            let span = common::model::span::Span {
                trace_id: span_trace_id,
                span_id: span_id.clone(),
                parent_span_id,
                status: status_str
                    .parse()
                    .unwrap_or(common::model::span::SpanStatus::Unspecified),
                is_root,
                name,
                service_name,
                span_kind: span_kind_str
                    .parse()
                    .unwrap_or(common::model::span::SpanKind::Internal),
                start_time_unix_nano,
                duration_nano,
                attributes: HashMap::new(), // TODO: Extract attributes if available
                resource: HashMap::new(),   // TODO: Extract resource if available
                children: Vec::new(),
            };

            span_map.insert(span_id, span);
        }
    }

    // Build hierarchical structure
    let mut root_spans = Vec::new();
    let mut child_spans: HashMap<String, Vec<common::model::span::Span>> = HashMap::new();

    for span in span_map.values() {
        if span.is_root || span.parent_span_id == "00000000" || span.parent_span_id.is_empty() {
            root_spans.push(span.clone());
        } else {
            child_spans
                .entry(span.parent_span_id.clone())
                .or_default()
                .push(span.clone());
        }
    }

    // Add children to parents
    for (parent_id, children) in child_spans {
        if let Some(parent) = span_map.get_mut(&parent_id) {
            parent.children.extend(children);
        }
    }

    Ok(common::model::trace::Trace {
        trace_id: trace_id.to_string(),
        spans: root_spans,
    })
}

/// Convert internal trace model to Tempo API format
fn internal_trace_to_tempo(trace: &common::model::trace::Trace) -> tempo_api::Trace {
    use std::collections::HashMap;

    // Find the earliest start time and calculate total duration
    let mut earliest_start = u64::MAX;
    let mut latest_end = 0u64;
    let mut root_service_name = "unknown".to_string();
    let mut root_trace_name = "unknown".to_string();

    // Collect all spans including children
    let mut all_spans = Vec::new();
    fn collect_all_spans(
        spans: &[common::model::span::Span],
        all_spans: &mut Vec<common::model::span::Span>,
    ) {
        for span in spans {
            all_spans.push(span.clone());
            collect_all_spans(&span.children, all_spans);
        }
    }
    collect_all_spans(&trace.spans, &mut all_spans);

    // Find root span and calculate timing info
    for span in &all_spans {
        if span.is_root {
            root_service_name = span.service_name.clone();
            root_trace_name = span.name.clone();
        }
        if span.start_time_unix_nano < earliest_start {
            earliest_start = span.start_time_unix_nano;
        }
        let end_time = span.start_time_unix_nano + span.duration_nano;
        if end_time > latest_end {
            latest_end = end_time;
        }
    }

    let duration_ms = if earliest_start != u64::MAX && latest_end > earliest_start {
        (latest_end - earliest_start) / 1_000_000 // Convert nanoseconds to milliseconds
    } else {
        0
    };

    // Convert spans to Tempo format
    let tempo_spans: Vec<tempo_api::Span> = all_spans
        .iter()
        .map(|span| {
            let mut attributes = HashMap::new();

            // Add span attributes
            for (key, value) in &span.attributes {
                let tempo_value = match value {
                    serde_json::Value::String(s) => tempo_api::Value::StringValue(s.clone()),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            tempo_api::Value::IntValue(i)
                        } else if let Some(f) = n.as_f64() {
                            tempo_api::Value::DoubleValue(f)
                        } else {
                            tempo_api::Value::StringValue(n.to_string())
                        }
                    }
                    serde_json::Value::Bool(b) => tempo_api::Value::BoolValue(*b),
                    _ => tempo_api::Value::StringValue(value.to_string()),
                };

                attributes.insert(
                    key.clone(),
                    tempo_api::Attribute {
                        key: key.clone(),
                        value: tempo_value,
                    },
                );
            }

            // Add resource attributes
            for (key, value) in &span.resource {
                let tempo_value = match value {
                    serde_json::Value::String(s) => tempo_api::Value::StringValue(s.clone()),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            tempo_api::Value::IntValue(i)
                        } else if let Some(f) = n.as_f64() {
                            tempo_api::Value::DoubleValue(f)
                        } else {
                            tempo_api::Value::StringValue(n.to_string())
                        }
                    }
                    serde_json::Value::Bool(b) => tempo_api::Value::BoolValue(*b),
                    _ => tempo_api::Value::StringValue(value.to_string()),
                };

                attributes.insert(
                    format!("resource.{key}"),
                    tempo_api::Attribute {
                        key: format!("resource.{key}"),
                        value: tempo_value,
                    },
                );
            }

            tempo_api::Span {
                span_id: span.span_id.clone(),
                start_time_unix_nano: span.start_time_unix_nano.to_string(),
                duration_nanos: span.duration_nano.to_string(),
                attributes,
            }
        })
        .collect();

    let span_set = tempo_api::SpanSet {
        spans: tempo_spans,
        matched: all_spans.len() as u16,
    };

    tempo_api::Trace {
        trace_id: trace.trace_id.clone(),
        root_service_name,
        root_trace_name,
        start_time_unix_nano: earliest_start.to_string(),
        duration_ms,
        span_sets: vec![span_set],
    }
}

/// Convert Arrow FlightData to Tempo search results
fn flight_data_to_search_results(
    flight_data: Vec<FlightData>,
) -> Result<tempo_api::SearchResult, Box<dyn std::error::Error + Send + Sync>> {
    if flight_data.is_empty() {
        return Ok(tempo_api::SearchResult {
            traces: vec![],
            metrics: HashMap::new(),
        });
    }

    // Convert FlightData to RecordBatches
    let mut cursor = std::io::Cursor::new(Vec::new());
    for data in &flight_data {
        cursor.write_all(&data.data_body)?;
    }
    cursor.set_position(0);

    let reader = StreamReader::try_new(cursor, None)?;
    let batches: Result<Vec<RecordBatch>, _> = reader.collect();
    let batches = batches?;

    if batches.is_empty() {
        return Ok(tempo_api::SearchResult {
            traces: vec![],
            metrics: HashMap::new(),
        });
    }

    // Group spans by trace_id
    let mut traces_map: HashMap<String, Vec<common::model::span::Span>> = HashMap::new();

    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let trace_id = batch
                .column_by_name("trace_id")
                .ok_or("Missing trace_id column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Invalid trace_id column type")?
                .value(row_index)
                .to_string();

            let span_id = batch
                .column_by_name("span_id")
                .ok_or("Missing span_id column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Invalid span_id column type")?
                .value(row_index)
                .to_string();

            let parent_span_id = batch
                .column_by_name("parent_span_id")
                .ok_or("Missing parent_span_id column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Invalid parent_span_id column type")?
                .value(row_index)
                .to_string();

            let name = batch
                .column_by_name("span_name")
                .ok_or("Missing span_name column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Invalid span_name column type")?
                .value(row_index)
                .to_string();

            let service_name = batch
                .column_by_name("service_name")
                .ok_or("Missing service_name column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Invalid service_name column type")?
                .value(row_index)
                .to_string();

            let span_kind_str = batch
                .column_by_name("span_kind")
                .ok_or("Missing span_kind column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Invalid span_kind column type")?
                .value(row_index);

            let start_time_unix_nano = batch
                .column_by_name("start_time_unix_nano")
                .ok_or("Missing start_time_unix_nano column")?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or("Invalid start_time_unix_nano column type")?
                .value(row_index);

            let duration_nano = batch
                .column_by_name("duration_nano")
                .ok_or("Missing duration_nano column")?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or("Invalid duration_nano column type")?
                .value(row_index);

            let status_str = batch
                .column_by_name("status_code")
                .ok_or("Missing status_code column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Invalid status_code column type")?
                .value(row_index);

            let is_root = batch
                .column_by_name("is_root")
                .ok_or("Missing is_root column")?
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("Invalid is_root column type")?
                .value(row_index);

            let span = common::model::span::Span {
                trace_id: trace_id.clone(),
                span_id,
                parent_span_id,
                status: status_str
                    .parse()
                    .unwrap_or(common::model::span::SpanStatus::Unspecified),
                is_root,
                name,
                service_name,
                span_kind: span_kind_str
                    .parse()
                    .unwrap_or(common::model::span::SpanKind::Internal),
                start_time_unix_nano,
                duration_nano,
                attributes: HashMap::new(),
                resource: HashMap::new(),
                children: Vec::new(),
            };

            traces_map.entry(trace_id).or_default().push(span);
        }
    }

    // Convert each trace to Tempo format
    let mut traces = Vec::new();
    for (trace_id, spans) in traces_map {
        let trace = common::model::trace::Trace { trace_id, spans };
        traces.push(internal_trace_to_tempo(&trace));
    }

    let metrics = HashMap::new(); // TODO: Add metrics if needed

    Ok(tempo_api::SearchResult { traces, metrics })
}

/// GET /api/echo
///
/// See https://grafana.com/docs/tempo/latest/api_docs/#query-echo-endpoint
#[tracing::instrument]
pub async fn echo() -> &'static str {
    "echo"
}

/// GET /api/traces/<traceid>?start=<start>&end=<end>
///
/// See https://grafana.com/docs/tempo/latest/api_docs/#query
#[tracing::instrument(skip(tenant_ctx))]
pub async fn query_single_trace<S: RouterState>(
    state: State<S>,
    tenant_ctx: TenantContextExtractor,
    Path(trace_id): Path<String>,
    Query(params): Query<TraceQueryParams>,
) -> Result<axum::Json<tempo_api::Trace>, axum::http::StatusCode> {
    log::info!(
        "Querying for trace_id: {trace_id} (tenant={}, dataset={})",
        tenant_ctx.0.tenant_id,
        tenant_ctx.0.dataset_id
    );

    // Use service registry to find available services for routing
    let services = state.service_registry().get_services().await;
    log::info!(
        "Available services for trace query: {} services found",
        services.len()
    );

    // Get a Flight client for a querier service
    let mut client = match state
        .service_registry()
        .get_flight_client_for_capability(ServiceCapability::QueryExecution)
        .await
    {
        Ok(client) => client,
        Err(e) => {
            log::error!("Failed to get Flight client for query execution: {e}");
            return Err(axum::http::StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    // Create Flight query for trace lookup with tenant context (using slugs for Iceberg namespace)
    let ticket = Ticket::new(format!(
        "find_trace:{}:{}:{trace_id}",
        tenant_ctx.0.tenant_slug, tenant_ctx.0.dataset_slug
    ));

    match client.do_get(ticket).await {
        Ok(response) => {
            let mut stream = response.into_inner();
            let mut trace_data = Vec::new();

            // Collect all flight data
            while let Some(flight_data) = stream.next().await {
                match flight_data {
                    Ok(data) => trace_data.push(data),
                    Err(e) => {
                        log::error!("Error reading flight data: {e}");
                        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                    }
                }
            }

            // Convert flight data to trace format
            match flight_data_to_tempo_trace(trace_data, &trace_id) {
                Ok(Some(trace)) => {
                    log::info!("Successfully converted trace {trace_id} to Tempo format");
                    return Ok(axum::Json(trace));
                }
                Ok(None) => {
                    log::info!("No trace data found for trace {trace_id}");
                }
                Err(e) => {
                    log::error!("Failed to convert flight data to trace: {e}");
                    return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                }
            }
        }
        Err(e) => {
            log::error!("Flight query failed for trace {trace_id}: {e}");
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    // Return empty trace if no data found
    let trace = tempo_api::Trace {
        trace_id,
        root_service_name: "unknown".to_string(),
        root_trace_name: "unknown".to_string(),
        start_time_unix_nano: "0".to_string(),
        duration_ms: 0u64,
        span_sets: vec![],
    };

    Ok(axum::Json(trace))
}

/// GET https://grafana.com/docs/tempo/latest/api_docs/#search
#[tracing::instrument(skip(tenant_ctx))]
pub async fn search<S: RouterState>(
    state: State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(query): Query<tempo_api::SearchQueryParams>,
) -> Result<axum::Json<tempo_api::SearchResult>, axum::http::StatusCode> {
    log::info!(
        "Searching for traces with params: {query:?} (tenant={}, dataset={})",
        tenant_ctx.0.tenant_id,
        tenant_ctx.0.dataset_id
    );

    // Use service registry to find available services for routing
    let services = state.service_registry().get_services().await;
    log::info!(
        "Available services for trace search: {} services found",
        services.len()
    );

    // Get a Flight client for a querier service
    let mut client = match state
        .service_registry()
        .get_flight_client_for_capability(ServiceCapability::QueryExecution)
        .await
    {
        Ok(client) => client,
        Err(e) => {
            log::error!("Failed to get Flight client for query execution: {e}");
            return Err(axum::http::StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    // Create Flight query for trace search with tenant context
    let search_params = serde_json::to_string(&query).map_err(|e| {
        log::error!("Failed to serialize search parameters: {e}");
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let ticket = Ticket::new(format!(
        "search_traces:{}:{}:{search_params}",
        tenant_ctx.0.tenant_slug, tenant_ctx.0.dataset_slug
    ));

    match client.do_get(ticket).await {
        Ok(response) => {
            let mut stream = response.into_inner();
            let mut search_results = Vec::new();

            // Collect all flight data
            while let Some(flight_data) = stream.next().await {
                match flight_data {
                    Ok(data) => search_results.push(data),
                    Err(e) => {
                        log::error!("Error reading flight data for search: {e}");
                        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                    }
                }
            }

            // Convert flight data to search results
            match flight_data_to_search_results(search_results) {
                Ok(search_result) => {
                    log::info!(
                        "Successfully converted {} traces to Tempo search format",
                        search_result.traces.len()
                    );
                    return Ok(axum::Json(search_result));
                }
                Err(e) => {
                    log::error!("Failed to convert flight data to search results: {e}");
                    return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                }
            }
        }
        Err(e) => {
            log::error!("Flight search query failed: {e}");
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    }
}

/// GET /api/search/tags?scope=<resource|span|intrinsic>
///
/// See https://grafana.com/docs/tempo/latest/api_docs/#search-tags
#[tracing::instrument]
pub async fn search_tags()
-> Result<axum::Json<tempo_api::TagSearchResponse>, axum::http::StatusCode> {
    let response = tempo_api::TagSearchResponse { tag_names: vec![] };
    Ok(axum::Json(response))
}

/// GET /api/search/tag/:tag_name/values
#[tracing::instrument]
pub async fn search_tag_values(
    Path(_tag_name): Path<String>,
) -> Result<axum::Json<tempo_api::TagValuesResponse>, axum::http::StatusCode> {
    let response = tempo_api::TagValuesResponse { tag_values: vec![] };
    Ok(axum::Json(response))
}

/// GET /api/v2/search/tags?scope=<resource|span|intrinsic>
#[tracing::instrument]
pub async fn search_tags_v2(
    Query(_params): Query<TagSearchV2Params>,
) -> Result<axum::Json<tempo_api::v2::TagSearchResponse>, axum::http::StatusCode> {
    let response = tempo_api::v2::TagSearchResponse { scopes: vec![] };
    Ok(axum::Json(response))
}

/// GET /api/v2/search/tag/{tag_name}/values
#[tracing::instrument]
pub async fn search_tag_values_v2(
    Path(_scoped_tag): Path<String>,
    Query(_params): Query<TagValueSearchV2Params>,
) -> Result<axum::Json<tempo_api::v2::TagValuesResponse>, axum::http::StatusCode> {
    let response = tempo_api::v2::TagValuesResponse { tag_values: vec![] };
    Ok(axum::Json(response))
}

/// GET /api/metrics/query - Instant TraceQL metrics query
#[tracing::instrument]
pub async fn metrics_query<S: RouterState>(
    _state: State<S>,
    Query(params): Query<MetricsQueryParams>,
) -> Result<axum::Json<MetricsResponse>, axum::http::StatusCode> {
    log::info!("Metrics instant query: {:?}", params);

    // For now, return a simple response indicating the endpoint is available
    // Full implementation will parse TraceQL and execute queries

    // Parse the TraceQL query to extract selector and metric function
    // Example: "{service.name='api'}|count()" -> selector: service.name='api', function: count

    let response = MetricsResponse {
        status: "success".to_string(),
        data: MetricsData {
            result_type: "vector".to_string(),
            result: vec![
                // Example response structure
                MetricSeries {
                    metric: HashMap::new(),
                    values: vec![(chrono::Utc::now().timestamp(), "0".to_string())],
                },
            ],
        },
    };

    Ok(axum::Json(response))
}

/// GET /api/metrics/query_range - Range TraceQL metrics query with time series
#[tracing::instrument]
pub async fn metrics_query_range<S: RouterState>(
    _state: State<S>,
    Query(params): Query<MetricsRangeQueryParams>,
) -> Result<axum::Json<MetricsResponse>, axum::http::StatusCode> {
    log::info!("Metrics range query: {:?}", params);

    // For now, return a simple response indicating the endpoint is available
    // Full implementation will:
    // 1. Parse TraceQL query
    // 2. Execute time-bucketed SQL queries
    // 3. Return time series data

    let response = MetricsResponse {
        status: "success".to_string(),
        data: MetricsData {
            result_type: "matrix".to_string(),
            result: vec![
                // Example time series response
                MetricSeries {
                    metric: HashMap::new(),
                    values: vec![
                        // Multiple timestamp-value pairs for time series
                        (chrono::Utc::now().timestamp() - 300, "10".to_string()),
                        (chrono::Utc::now().timestamp(), "15".to_string()),
                    ],
                },
            ],
        },
    };

    Ok(axum::Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use common::catalog::Catalog;

    async fn create_test_catalog() -> Catalog {
        // For testing, we'll need to use a real PostgreSQL connection or mock
        // Since we don't want to require a DB for unit tests, let's skip the catalog tests for now
        // and focus on testing the service registry logic separately

        // This is a placeholder - in a real test we'd either:
        // 1. Use a test database
        // 2. Create a mock catalog implementation
        // 3. Use dependency injection for testability
        panic!("Catalog tests require database setup - skipping for now")
    }

    #[tokio::test]
    #[ignore = "Requires database setup"]
    async fn test_search_result() {
        // This test is disabled because it requires a real database connection
        // To enable this test, set up a test database and update the catalog creation logic

        // Create a mock state
        let catalog = create_test_catalog().await;
        let state =
            crate::InMemoryStateImpl::new(catalog, common::config::Configuration::default());

        let query = tempo_api::SearchQueryParams {
            start: None,
            end: None,
            limit: None,
            tags: None,
            min_duration: None,
            max_duration: None,
            q: None,
            spss: None,
        };

        // Create a test tenant context
        let tenant_ctx = common::auth::TenantContext::new(
            "test-tenant".to_string(),
            "test-dataset".to_string(),
            "test-tenant".to_string(),
            "test-dataset".to_string(),
            None,
            common::auth::TenantSource::Config,
        );

        let result = search(
            State(state),
            common::auth::TenantContextExtractor(tenant_ctx),
            Query(query),
        )
        .await
        .unwrap();
        assert_eq!(result.0.traces.len(), 0);
        assert_eq!(result.0.metrics.len(), 0);
    }
}
