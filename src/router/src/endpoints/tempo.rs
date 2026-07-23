use crate::RouterState;
use arrow_flight::{FlightData, Ticket, utils::flight_data_to_batches};
use axum::{
    Router,
    extract::{Path, Query, State},
    routing::get,
};
use common::auth::TenantContextExtractor;
use common::flight::transport::ServiceCapability;
use datafusion::arrow::{
    array::{Array, BooleanArray, StringArray, UInt64Array},
    record_batch::RecordBatch,
};
use futures::StreamExt;
use serde::Deserialize;
use std::collections::HashMap;
use tempo_api::{
    self, MetricsQueryParams, MetricsRangeQueryParams, MetricsResponse, TraceQueryParams,
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
        .route(
            "/api/search/tag/{tag_name}/values",
            get(search_tag_values::<S>),
        )
        // v2 routes
        .route("/api/v2/traces/{trace_id}", get(query_single_trace::<S>)) // V2 uses same handler for now
        .route("/api/v2/search/tags", get(search_tags_v2))
        .route(
            "/api/v2/search/tag/{tag_name}/values",
            get(search_tag_values_v2::<S>),
        )
        // metrics endpoints
        .route("/api/metrics/query", get(metrics_query))
        .route("/api/metrics/query_range", get(metrics_query_range))
}

/// Convert Arrow FlightData to internal trace model, then to Tempo API format
fn flight_data_to_tempo_trace(
    flight_data: Vec<FlightData>,
    trace_id: &str,
) -> Result<Option<tempo_api::Trace>, Box<dyn std::error::Error + Send + Sync>> {
    if flight_data.is_empty() {
        return Ok(None);
    }

    // Convert FlightData to RecordBatches using Arrow Flight utilities
    let batches = flight_data_to_batches(&flight_data)?;

    if batches.is_empty() {
        return Ok(None);
    }

    // Convert RecordBatches to internal trace model
    let trace = record_batches_to_trace(batches, trace_id)?;

    // Convert internal trace model to Tempo API format
    let tempo_trace = internal_trace_to_tempo(&trace, None);

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
        // Extract typed column references once per batch to avoid repeated lookups
        let trace_id_col = batch
            .column_by_name("trace_id")
            .ok_or("Missing trace_id column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("Invalid trace_id column type")?;
        let span_id_col = batch
            .column_by_name("span_id")
            .ok_or("Missing span_id column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("Invalid span_id column type")?;
        let parent_span_id_col = batch
            .column_by_name("parent_span_id")
            .ok_or("Missing parent_span_id column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("Invalid parent_span_id column type")?;
        let span_name_col = batch
            .column_by_name("span_name")
            .ok_or("Missing span_name column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("Invalid span_name column type")?;
        let service_name_col = batch
            .column_by_name("service_name")
            .ok_or("Missing service_name column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("Invalid service_name column type")?;
        let span_kind_col = batch
            .column_by_name("span_kind")
            .ok_or("Missing span_kind column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("Invalid span_kind column type")?;
        let start_time_col = batch
            .column_by_name("start_time_unix_nano")
            .ok_or("Missing start_time_unix_nano column")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or("Invalid start_time_unix_nano column type")?;
        let duration_col = batch
            .column_by_name("duration_nano")
            .ok_or("Missing duration_nano column")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or("Invalid duration_nano column type")?;
        let status_col = batch
            .column_by_name("status_code")
            .ok_or("Missing status_code column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("Invalid status_code column type")?;
        let is_root_col = batch
            .column_by_name("is_root")
            .ok_or("Missing is_root column")?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or("Invalid is_root column type")?;
        // Optional attribute columns (may not exist in older data)
        let span_attrs_col = batch
            .column_by_name("span_attributes")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let resource_attrs_col = batch
            .column_by_name("resource_attributes")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        for row_index in 0..batch.num_rows() {
            let span_trace_id = trace_id_col.value(row_index).to_string();

            // Only include spans that match the requested trace_id
            if span_trace_id != trace_id {
                continue;
            }

            let span_id = span_id_col.value(row_index).to_string();

            let attributes = span_attrs_col
                .and_then(|arr| {
                    if arr.is_null(row_index) {
                        None
                    } else {
                        serde_json::from_str(arr.value(row_index)).ok()
                    }
                })
                .unwrap_or_default();

            let resource = resource_attrs_col
                .and_then(|arr| {
                    if arr.is_null(row_index) {
                        None
                    } else {
                        serde_json::from_str(arr.value(row_index)).ok()
                    }
                })
                .unwrap_or_default();

            let span = common::model::span::Span {
                trace_id: span_trace_id,
                span_id: span_id.clone(),
                parent_span_id: parent_span_id_col.value(row_index).to_string(),
                status: status_col
                    .value(row_index)
                    .parse()
                    .unwrap_or(common::model::span::SpanStatus::Unspecified),
                is_root: is_root_col.value(row_index),
                name: span_name_col.value(row_index).to_string(),
                service_name: service_name_col.value(row_index).to_string(),
                span_kind: span_kind_col
                    .value(row_index)
                    .parse()
                    .unwrap_or(common::model::span::SpanKind::Internal),
                start_time_unix_nano: start_time_col.value(row_index),
                duration_nano: duration_col.value(row_index),
                attributes,
                resource,
                children: Vec::new(),
            };

            span_map.insert(span_id, span);
        }
    }

    // Build hierarchical structure
    let root_spans = common::model::span::build_span_hierarchy(span_map);

    Ok(common::model::trace::Trace {
        trace_id: trace_id.to_string(),
        spans: root_spans,
    })
}

/// Convert internal trace model to Tempo API format.
///
/// `span_cap` limits how many spans are included in the returned span set
/// (Tempo's `spss`, spans-per-spanset); `matched` still reports the full
/// span count. `None` includes every span.
fn internal_trace_to_tempo(
    trace: &common::model::trace::Trace,
    span_cap: Option<usize>,
) -> tempo_api::Trace {
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

    // Convert spans to Tempo format, capped at spss when requested
    let tempo_spans: Vec<tempo_api::Span> = all_spans
        .iter()
        .take(span_cap.unwrap_or(usize::MAX))
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

/// Convert Arrow FlightData to Tempo search results.
///
/// `spss` is Tempo's spans-per-spanset limit; non-positive values are
/// ignored. When absent, every matched span is returned (Tempo itself
/// defaults to 3, but SignalDB preserves its historical full-span
/// responses unless the client asks for a cap).
fn flight_data_to_search_results(
    flight_data: Vec<FlightData>,
    spss: Option<i32>,
) -> Result<tempo_api::SearchResult, Box<dyn std::error::Error + Send + Sync>> {
    let span_cap = spss
        .and_then(|v| usize::try_from(v).ok())
        .filter(|v| *v > 0);
    if flight_data.is_empty() {
        return Ok(tempo_api::SearchResult {
            traces: vec![],
            metrics: HashMap::new(),
        });
    }

    // Convert FlightData to RecordBatches using Arrow Flight utilities
    let batches = flight_data_to_batches(&flight_data)?;

    if batches.is_empty() {
        return Ok(tempo_api::SearchResult {
            traces: vec![],
            metrics: HashMap::new(),
        });
    }

    // Group spans by trace_id
    let mut traces_map: HashMap<String, Vec<common::model::span::Span>> = HashMap::new();

    for batch in batches {
        // Extract typed column references once per batch to avoid repeated lookups
        let trace_id_col = batch
            .column_by_name("trace_id")
            .ok_or("Missing trace_id column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("Invalid trace_id column type")?;
        let span_id_col = batch
            .column_by_name("span_id")
            .ok_or("Missing span_id column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("Invalid span_id column type")?;
        let parent_span_id_col = batch
            .column_by_name("parent_span_id")
            .ok_or("Missing parent_span_id column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("Invalid parent_span_id column type")?;
        let span_name_col = batch
            .column_by_name("span_name")
            .ok_or("Missing span_name column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("Invalid span_name column type")?;
        let service_name_col = batch
            .column_by_name("service_name")
            .ok_or("Missing service_name column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("Invalid service_name column type")?;
        let span_kind_col = batch
            .column_by_name("span_kind")
            .ok_or("Missing span_kind column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("Invalid span_kind column type")?;
        let start_time_col = batch
            .column_by_name("start_time_unix_nano")
            .ok_or("Missing start_time_unix_nano column")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or("Invalid start_time_unix_nano column type")?;
        let duration_col = batch
            .column_by_name("duration_nano")
            .ok_or("Missing duration_nano column")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or("Invalid duration_nano column type")?;
        let status_col = batch
            .column_by_name("status_code")
            .ok_or("Missing status_code column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("Invalid status_code column type")?;
        let is_root_col = batch
            .column_by_name("is_root")
            .ok_or("Missing is_root column")?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or("Invalid is_root column type")?;
        // Optional attribute columns (may not exist in older data)
        let span_attrs_col = batch
            .column_by_name("span_attributes")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let resource_attrs_col = batch
            .column_by_name("resource_attributes")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        for row_index in 0..batch.num_rows() {
            let trace_id = trace_id_col.value(row_index).to_string();
            let span_id = span_id_col.value(row_index).to_string();

            let attributes = span_attrs_col
                .and_then(|arr| {
                    if arr.is_null(row_index) {
                        None
                    } else {
                        serde_json::from_str(arr.value(row_index)).ok()
                    }
                })
                .unwrap_or_default();

            let resource = resource_attrs_col
                .and_then(|arr| {
                    if arr.is_null(row_index) {
                        None
                    } else {
                        serde_json::from_str(arr.value(row_index)).ok()
                    }
                })
                .unwrap_or_default();

            let span = common::model::span::Span {
                trace_id: trace_id.clone(),
                span_id,
                parent_span_id: parent_span_id_col.value(row_index).to_string(),
                status: status_col
                    .value(row_index)
                    .parse()
                    .unwrap_or(common::model::span::SpanStatus::Unspecified),
                is_root: is_root_col.value(row_index),
                name: span_name_col.value(row_index).to_string(),
                service_name: service_name_col.value(row_index).to_string(),
                span_kind: span_kind_col
                    .value(row_index)
                    .parse()
                    .unwrap_or(common::model::span::SpanKind::Internal),
                start_time_unix_nano: start_time_col.value(row_index),
                duration_nano: duration_col.value(row_index),
                attributes,
                resource,
                children: Vec::new(),
            };

            traces_map.entry(trace_id).or_default().push(span);
        }
    }

    // Convert each trace to Tempo format
    let mut traces = Vec::new();
    for (trace_id, spans) in traces_map {
        let trace = common::model::trace::Trace { trace_id, spans };
        traces.push(internal_trace_to_tempo(&trace, span_cap));
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
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn query_single_trace<S: RouterState>(
    state: State<S>,
    tenant_ctx: TenantContextExtractor,
    Path(trace_id): Path<String>,
    Query(params): Query<TraceQueryParams>,
) -> Result<axum::Json<tempo_api::Trace>, axum::http::StatusCode> {
    tracing::info!(
        trace_id = %trace_id,
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id,
        start = ?params.start,
        end = ?params.end,
        "Querying for trace"
    );

    // Use service registry to find available services for routing
    let services = state.service_registry().get_services().await;
    tracing::info!(
        service_count = services.len(),
        "Available services for trace query"
    );

    // Get a Flight client for a querier service
    let mut client = match state
        .service_registry()
        .get_flight_client_for_capability(ServiceCapability::QueryExecution)
        .await
    {
        Ok(client) => client,
        Err(e) => {
            tracing::error!(error = %e, "Failed to get Flight client for query execution");
            return Err(axum::http::StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    // Create Flight query for trace lookup with tenant context (using slugs
    // for the Iceberg namespace). Time-hint segments are only appended when
    // present so tickets without hints keep the legacy 3-part form.
    let ticket_content = match (params.start, params.end) {
        (None, None) => format!(
            "find_trace:{}:{}:{trace_id}",
            tenant_ctx.0.tenant_slug, tenant_ctx.0.dataset_slug
        ),
        (start, end) => format!(
            "find_trace:{}:{}:{trace_id}:{}:{}",
            tenant_ctx.0.tenant_slug,
            tenant_ctx.0.dataset_slug,
            start.map(|v| v.to_string()).unwrap_or_default(),
            end.map(|v| v.to_string()).unwrap_or_default()
        ),
    };
    let ticket = Ticket::new(ticket_content);
    let mut flight_request = tonic::Request::new(ticket);
    common::flight::trace_context::inject_context_into_request(&mut flight_request);
    if let Some(key) = &state.config().auth.internal_service_key {
        common::flight::auth::attach_internal_auth(&mut flight_request, key);
    }

    match client.do_get(flight_request).await {
        Ok(response) => {
            let mut stream = response.into_inner();
            let mut trace_data = Vec::new();

            // Collect all flight data
            while let Some(flight_data) = stream.next().await {
                match flight_data {
                    Ok(data) => trace_data.push(data),
                    Err(e) => {
                        tracing::error!(error = %e, "Error reading flight data");
                        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                    }
                }
            }

            // Convert flight data to trace format
            match flight_data_to_tempo_trace(trace_data, &trace_id) {
                Ok(Some(trace)) => {
                    tracing::info!(trace_id = %trace_id, "Successfully converted trace to Tempo format");
                    return Ok(axum::Json(trace));
                }
                Ok(None) => {
                    tracing::info!(trace_id = %trace_id, "No trace data found");
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to convert flight data to trace");
                    return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                }
            }
        }
        Err(e) => {
            tracing::error!(trace_id = %trace_id, error = %e, "Flight query failed for trace");
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    // Return 404 when no trace data is found
    tracing::info!(trace_id = %trace_id, "Trace not found");
    Err(axum::http::StatusCode::NOT_FOUND)
}

/// GET https://grafana.com/docs/tempo/latest/api_docs/#search
#[tracing::instrument(
    skip(state, tenant_ctx, query),
    fields(
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn search<S: RouterState>(
    state: State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(query): Query<tempo_api::SearchQueryParams>,
) -> Result<axum::Json<tempo_api::SearchResult>, axum::http::StatusCode> {
    tracing::info!(
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id,
        "Searching for traces"
    );

    // Use service registry to find available services for routing
    let services = state.service_registry().get_services().await;
    tracing::info!(
        service_count = services.len(),
        "Available services for trace search"
    );

    // Get a Flight client for a querier service
    let mut client = match state
        .service_registry()
        .get_flight_client_for_capability(ServiceCapability::QueryExecution)
        .await
    {
        Ok(client) => client,
        Err(e) => {
            tracing::error!(error = %e, "Failed to get Flight client for query execution");
            return Err(axum::http::StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    // Create Flight query for trace search with tenant context
    let search_params = serde_json::to_string(&query).map_err(|e| {
        tracing::error!(error = %e, "Failed to serialize search parameters");
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let ticket = Ticket::new(format!(
        "search_traces:{}:{}:{search_params}",
        tenant_ctx.0.tenant_slug, tenant_ctx.0.dataset_slug
    ));
    let mut flight_request = tonic::Request::new(ticket);
    common::flight::trace_context::inject_context_into_request(&mut flight_request);
    if let Some(key) = &state.config().auth.internal_service_key {
        common::flight::auth::attach_internal_auth(&mut flight_request, key);
    }

    match client.do_get(flight_request).await {
        Ok(response) => {
            let mut stream = response.into_inner();
            let mut search_results = Vec::new();

            // Collect all flight data
            while let Some(flight_data) = stream.next().await {
                match flight_data {
                    Ok(data) => search_results.push(data),
                    Err(e) => {
                        tracing::error!(error = %e, "Error reading flight data for search");
                        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                    }
                }
            }

            // Convert flight data to search results
            match flight_data_to_search_results(search_results, query.spss) {
                Ok(search_result) => {
                    tracing::info!(
                        trace_count = search_result.traces.len(),
                        "Successfully converted traces to Tempo search format"
                    );
                    return Ok(axum::Json(search_result));
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to convert flight data to search results");
                    return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                }
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "Flight search query failed");
            // Surface caller errors honestly: bad selectors are 400,
            // unsupported query features are 501, everything else 500.
            return Err(match e.code() {
                tonic::Code::InvalidArgument => axum::http::StatusCode::BAD_REQUEST,
                tonic::Code::Unimplemented => axum::http::StatusCode::NOT_IMPLEMENTED,
                tonic::Code::ResourceExhausted => axum::http::StatusCode::TOO_MANY_REQUESTS,
                tonic::Code::DeadlineExceeded => axum::http::StatusCode::GATEWAY_TIMEOUT,
                _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            });
        }
    }
}

/// Tag names trace search can actually filter on today (see the
/// querier's search_filter module). Returned instead of an empty stub so
/// Grafana autocomplete reflects real capability without fabricating
/// unqueryable tags.
const RESOURCE_TAGS: &[&str] = &["service.name"];
const INTRINSIC_TAGS: &[&str] = &["name", "status"];

/// Map a (possibly scoped) tag name to the traces column that backs it.
fn tag_value_column(tag_name: &str) -> Option<&'static str> {
    let unscoped = tag_name
        .strip_prefix("resource.")
        .or_else(|| tag_name.strip_prefix("span."))
        .unwrap_or(tag_name)
        .trim_start_matches('.');
    match unscoped {
        "service.name" => Some("service_name"),
        "name" => Some("span_name"),
        _ => None,
    }
}

/// Fetch distinct values of a traces column for the tenant via the
/// querier's Flight SQL path.
async fn distinct_column_values<S: RouterState>(
    state: &S,
    tenant_ctx: &common::auth::TenantContext,
    column: &str,
) -> Result<Vec<String>, axum::http::StatusCode> {
    let mut client = state
        .service_registry()
        .get_flight_client_for_capability(ServiceCapability::QueryExecution)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to get Flight client for tag values");
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        })?;

    // Slugs are validated at authentication time; quote identifiers so
    // hyphenated slugs parse.
    let sql = format!(
        "SELECT DISTINCT \"{column}\" FROM \"{}\".\"{}\".\"traces\" ORDER BY 1 LIMIT 1000",
        tenant_ctx.tenant_slug, tenant_ctx.dataset_slug
    );
    let mut flight_request = tonic::Request::new(Ticket::new(sql));
    common::flight::trace_context::inject_context_into_request(&mut flight_request);
    if let Some(key) = &state.config().auth.internal_service_key {
        common::flight::auth::attach_internal_auth(&mut flight_request, key);
    }

    let mut stream = client
        .do_get(flight_request)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "Tag values query failed");
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        })?
        .into_inner();

    let mut flight_data = Vec::new();
    while let Some(data) = stream.next().await {
        flight_data.push(data.map_err(|e| {
            tracing::error!(error = %e, "Error reading tag values flight data");
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        })?);
    }
    if flight_data.is_empty() {
        return Ok(vec![]);
    }

    let batches = flight_data_to_batches(&flight_data).map_err(|e| {
        tracing::error!(error = %e, "Failed to decode tag values flight data");
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let mut values = Vec::new();
    for batch in batches {
        if batch.num_columns() == 0 {
            continue;
        }
        if let Some(column) = batch.column(0).as_any().downcast_ref::<StringArray>() {
            for i in 0..column.len() {
                if !column.is_null(i) {
                    values.push(column.value(i).to_string());
                }
            }
        }
    }
    Ok(values)
}

/// GET /api/search/tags?scope=<resource|span|intrinsic>
///
/// See https://grafana.com/docs/tempo/latest/api_docs/#search-tags
#[tracing::instrument]
pub async fn search_tags()
-> Result<axum::Json<tempo_api::TagSearchResponse>, axum::http::StatusCode> {
    let response = tempo_api::TagSearchResponse {
        tag_names: RESOURCE_TAGS
            .iter()
            .chain(INTRINSIC_TAGS)
            .map(|t| t.to_string())
            .collect(),
    };
    Ok(axum::Json(response))
}

/// GET /api/search/tag/:tag_name/values
///
/// Backed by real data: distinct values from the tenant's traces table
/// for supported tags, static status values for `status`, and an
/// explicit 501 for tags that are not queryable yet.
#[tracing::instrument(skip(state, tenant_ctx))]
pub async fn search_tag_values<S: RouterState>(
    state: State<S>,
    tenant_ctx: TenantContextExtractor,
    Path(tag_name): Path<String>,
) -> Result<axum::Json<tempo_api::TagValuesResponse>, axum::http::StatusCode> {
    let tag_values = tag_values_for(&state, &tenant_ctx.0, &tag_name).await?;
    Ok(axum::Json(tempo_api::TagValuesResponse { tag_values }))
}

async fn tag_values_for<S: RouterState>(
    state: &State<S>,
    tenant_ctx: &common::auth::TenantContext,
    tag_name: &str,
) -> Result<Vec<String>, axum::http::StatusCode> {
    if let Some(column) = tag_value_column(tag_name) {
        return distinct_column_values(&state.0, tenant_ctx, column).await;
    }
    let unscoped = tag_name.trim_start_matches('.');
    if unscoped == "status" || unscoped == "intrinsic.status" {
        return Ok(vec![
            "ok".to_string(),
            "error".to_string(),
            "unset".to_string(),
        ]);
    }
    // Attribute tag values require an index (#411); saying so beats an
    // empty list that looks like "no data".
    tracing::debug!(tag_name = %tag_name, "Tag value lookup not implemented for this tag");
    Err(axum::http::StatusCode::NOT_IMPLEMENTED)
}

/// GET /api/v2/search/tags?scope=<resource|span|intrinsic>
#[tracing::instrument]
pub async fn search_tags_v2(
    Query(_params): Query<TagSearchV2Params>,
) -> Result<axum::Json<tempo_api::v2::TagSearchResponse>, axum::http::StatusCode> {
    let response = tempo_api::v2::TagSearchResponse {
        scopes: vec![
            tempo_api::v2::TagSearchScope {
                scope: "resource".to_string(),
                tags: RESOURCE_TAGS.iter().map(|t| t.to_string()).collect(),
            },
            tempo_api::v2::TagSearchScope {
                scope: "intrinsic".to_string(),
                tags: INTRINSIC_TAGS.iter().map(|t| t.to_string()).collect(),
            },
        ],
    };
    Ok(axum::Json(response))
}

/// GET /api/v2/search/tag/{tag_name}/values
#[tracing::instrument(skip(state, tenant_ctx))]
pub async fn search_tag_values_v2<S: RouterState>(
    state: State<S>,
    tenant_ctx: TenantContextExtractor,
    Path(scoped_tag): Path<String>,
    Query(_params): Query<TagValueSearchV2Params>,
) -> Result<axum::Json<tempo_api::v2::TagValuesResponse>, axum::http::StatusCode> {
    let values = tag_values_for(&state, &tenant_ctx.0, &scoped_tag).await?;
    Ok(axum::Json(tempo_api::v2::TagValuesResponse {
        tag_values: values
            .into_iter()
            .map(|value| tempo_api::v2::TagWithValue {
                tag: scoped_tag.clone(),
                value,
            })
            .collect(),
    }))
}

/// GET /api/metrics/query - Instant TraceQL metrics query
///
/// TraceQL metrics are not implemented. Answer 501 instead of the
/// fabricated series this endpoint used to return (issue #552).
#[tracing::instrument]
pub async fn metrics_query(
    Query(_params): Query<MetricsQueryParams>,
) -> Result<axum::Json<MetricsResponse>, axum::http::StatusCode> {
    tracing::debug!("TraceQL metrics instant query not implemented");
    Err(axum::http::StatusCode::NOT_IMPLEMENTED)
}

/// GET /api/metrics/query_range - Range TraceQL metrics query with time series
///
/// TraceQL metrics are not implemented. Answer 501 instead of the
/// fabricated series this endpoint used to return (issue #552).
#[tracing::instrument]
pub async fn metrics_query_range(
    Query(_params): Query<MetricsRangeQueryParams>,
) -> Result<axum::Json<MetricsResponse>, axum::http::StatusCode> {
    tracing::debug!("TraceQL metrics range query not implemented");
    Err(axum::http::StatusCode::NOT_IMPLEMENTED)
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

    fn make_span(span_id: &str, start: u64) -> common::model::span::Span {
        common::model::span::Span {
            trace_id: "trace-1".to_string(),
            span_id: span_id.to_string(),
            parent_span_id: String::new(),
            status: common::model::span::SpanStatus::Unspecified,
            is_root: span_id == "root",
            name: format!("span-{span_id}"),
            service_name: "svc".to_string(),
            span_kind: common::model::span::SpanKind::Internal,
            start_time_unix_nano: start,
            duration_nano: 1_000,
            attributes: Default::default(),
            resource: Default::default(),
            children: Vec::new(),
        }
    }

    #[test]
    fn span_cap_limits_returned_spans_but_not_matched_count() {
        let trace = common::model::trace::Trace {
            trace_id: "trace-1".to_string(),
            spans: (0..5)
                .map(|i| make_span(if i == 0 { "root" } else { "child" }, 1_000 + i))
                .collect(),
        };

        let capped = internal_trace_to_tempo(&trace, Some(3));
        assert_eq!(capped.span_sets.len(), 1);
        assert_eq!(capped.span_sets[0].spans.len(), 3);
        assert_eq!(capped.span_sets[0].matched, 5);

        let uncapped = internal_trace_to_tempo(&trace, None);
        assert_eq!(uncapped.span_sets[0].spans.len(), 5);
        assert_eq!(uncapped.span_sets[0].matched, 5);
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
