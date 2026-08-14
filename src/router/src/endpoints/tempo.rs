use super::api_error::ApiError;
use crate::RouterState;
use arrow_flight::{FlightData, Ticket};
use axum::{
    Router,
    extract::{Path, Query, State},
    routing::get,
};
use common::auth::TenantContextExtractor;
use common::flight::decode::flight_data_vec_to_batches;
use common::flight::transport::ServiceCapability;
use datafusion::arrow::{
    array::{Array, BooleanArray, StringArray, UInt64Array},
    record_batch::RecordBatch,
};
use futures::StreamExt;
use serde::Deserialize;
use std::collections::HashMap;
use tempo_api::{self, MetricsResponse, TraceQueryParams};
use tracing::Instrument;

/// Query parameters for v2 tag search
#[derive(Debug, Deserialize)]
pub struct TagSearchV2Params {
    pub scope: Option<tempo_api::TagScope>,
}

/// Query parameters for v1 tag value search. `start`/`end` are unix
/// seconds, per the Tempo API.
#[derive(Debug, Deserialize)]
pub struct TagValueSearchParams {
    pub start: Option<i64>,
    pub end: Option<i64>,
}

/// Query parameters for v2 tag value search. `start`/`end` are unix
/// seconds, per the Tempo API.
#[derive(Debug, Deserialize)]
pub struct TagValueSearchV2Params {
    pub start: Option<i64>,
    pub end: Option<i64>,
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
async fn flight_data_to_tempo_trace(
    flight_data: Vec<FlightData>,
    trace_id: &str,
) -> Result<Option<tempo_api::Trace>, Box<dyn std::error::Error + Send + Sync>> {
    if flight_data.is_empty() {
        return Ok(None);
    }

    // Convert FlightData to RecordBatches, honoring any dictionary batches
    // the querier sent (#951).
    let batches = flight_data_vec_to_batches(flight_data).await?;

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
        let events_col = batch
            .column_by_name("events")
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
                events: events_col
                    .filter(|arr| !arr.is_null(row_index))
                    .map(|arr| common::model::span::parse_span_events(arr.value(row_index)))
                    .unwrap_or_default(),
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

/// Map a JSON attribute value to the Tempo API's typed value.
fn json_to_tempo_value(value: &serde_json::Value) -> tempo_api::Value {
    match value {
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
    }
}

/// Map model span events to the Tempo API span-event shape.
fn model_events_to_tempo(events: &[common::model::span::SpanEvent]) -> Vec<tempo_api::SpanEvent> {
    events
        .iter()
        .map(|event| tempo_api::SpanEvent {
            name: event.name.clone(),
            time_unix_nano: event.timestamp_unix_nano.to_string(),
            attributes: event
                .attributes
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        tempo_api::Attribute {
                            key: key.clone(),
                            value: json_to_tempo_value(value),
                        },
                    )
                })
                .collect(),
        })
        .collect()
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

    // Collect all spans including children, iteratively so deep hierarchies
    // cannot overflow the stack.
    let mut all_spans = Vec::new();
    let mut stack: Vec<&common::model::span::Span> = trace.spans.iter().rev().collect();
    while let Some(span) = stack.pop() {
        stack.extend(span.children.iter().rev());
        all_spans.push(span.clone_without_children());
    }

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
                name: Some(span.name.clone()),
                parent_span_id: if span.parent_span_id.is_empty() {
                    None
                } else {
                    Some(span.parent_span_id.clone())
                },
                service_name: Some(span.service_name.clone()),
                status: Some(
                    match span.status {
                        common::model::span::SpanStatus::Ok => "ok",
                        common::model::span::SpanStatus::Error => "error",
                        common::model::span::SpanStatus::Unspecified => "unset",
                    }
                    .to_string(),
                ),
                attributes,
                events: model_events_to_tempo(&span.events),
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
        profiles: None,
    }
}

/// Convert Arrow FlightData to Tempo search results.
///
/// `spss` is Tempo's spans-per-spanset limit; non-positive values are
/// ignored. When absent, every matched span is returned (Tempo itself
/// defaults to 3, but SignalDB preserves its historical full-span
/// responses unless the client asks for a cap).
async fn flight_data_to_search_results(
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

    // Convert FlightData to RecordBatches, honoring any dictionary batches
    // the querier sent (#951).
    let batches = flight_data_vec_to_batches(flight_data).await?;

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
                events: Vec::new(),
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
#[tracing::instrument(skip_all)]
pub async fn echo() -> &'static str {
    "echo"
}

/// GET /api/traces/<traceid>?start=<start>&end=<end>
///
/// See https://grafana.com/docs/tempo/latest/api_docs/#query
#[utoipa::path(
    get,
    path = "/tempo/api/traces/{trace_id}",
    tag = "traces",
    security(("bearerAuth" = [])),
    params(
        ("trace_id" = String, Path, description = "Trace ID to fetch"),
        tempo_api::TraceQueryParams,
    ),
    responses(
        (status = 200, description = "The reconstructed trace", body = tempo_api::Trace),
        (status = 404, description = "Trace not found"),
    )
)]
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(
        signaldb.tenant.id = %tenant_ctx.0.tenant_id,
        signaldb.dataset.id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn query_single_trace<S: RouterState>(
    state: State<S>,
    tenant_ctx: TenantContextExtractor,
    Path(trace_id): Path<String>,
    Query(params): Query<TraceQueryParams>,
) -> Result<
    (
        axum::Extension<common::self_monitoring::ServerTimings>,
        axum::Json<tempo_api::Trace>,
    ),
    ApiError,
> {
    tracing::info!(
        trace_id = %trace_id,
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id,
        start = ?params.start,
        end = ?params.end,
        "Querying for trace"
    );

    // Get a Flight client for a querier service
    let (mut client, server_address) = match state
        .service_registry()
        .get_flight_client_and_address_for_capability(ServiceCapability::QueryExecution)
        .await
    {
        Ok(result) => result,
        Err(e) => {
            tracing::error!(error = %e, "Failed to get Flight client for query execution");
            return Err(ApiError::new(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "no querier service available",
            ));
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

    let querier_started = std::time::Instant::now();
    match client
        .do_get(flight_request)
        .instrument(rpc_span.clone())
        .await
    {
        Ok(response) => {
            let mut stream = response.into_inner();
            let mut trace_data = Vec::new();

            // Collect all flight data. The querier's terminal status can
            // surface here rather than at do_get, so map it in both places.
            while let Some(flight_data) = stream.next().await {
                match flight_data {
                    Ok(data) => trace_data.push(data),
                    Err(e) => {
                        return Err(
                            rpc_span.in_scope(|| trace_lookup_status_to_http(&trace_id, &e))
                        );
                    }
                }
            }
            let querier_elapsed = querier_started.elapsed();
            let convert_started = std::time::Instant::now();

            // Convert flight data to trace format
            match flight_data_to_tempo_trace(trace_data, &trace_id).await {
                Ok(Some(mut trace)) => {
                    tracing::info!(trace_id = %trace_id, "Successfully converted trace to Tempo format");
                    // Optionally attach linked profile summaries. A failed
                    // profile lookup must not fail the trace response.
                    if params.include_profiles.unwrap_or(false) {
                        match super::pyroscope::fetch_profiles_for_trace(
                            &state.0,
                            &tenant_ctx.0.tenant_slug,
                            &tenant_ctx.0.dataset_slug,
                            &trace_id,
                        )
                        .await
                        {
                            Ok(profiles) => trace.profiles = Some(profiles),
                            Err(err) => {
                                tracing::warn!(
                                    trace_id = %trace_id,
                                    status = %err.status,
                                    error = %err.message,
                                    "Failed to fetch linked profiles for trace"
                                );
                            }
                        }
                    }
                    return Ok(timed_trace_response(
                        trace,
                        querier_elapsed,
                        convert_started.elapsed(),
                    ));
                }
                Ok(None) => {
                    tracing::info!(trace_id = %trace_id, "No trace data found");
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to convert flight data to trace");
                    return Err(ApiError::new(
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        "failed to decode trace data returned by the querier",
                    ));
                }
            }
        }
        Err(e) => {
            return Err(rpc_span.in_scope(|| trace_lookup_status_to_http(&trace_id, &e)));
        }
    }

    // Return 404 when no trace data is found
    tracing::info!(trace_id = %trace_id, "Trace not found");
    Err(ApiError::new(
        axum::http::StatusCode::NOT_FOUND,
        format!("trace {trace_id} not found"),
    ))
}

/// Assemble the single-trace response together with its stage timings —
/// `querier` (Flight round-trip to the querier, including streaming the
/// result) and `convert` (Flight data → Tempo JSON) — which the trace-context
/// middleware surfaces as `Server-Timing` entries.
fn timed_trace_response(
    trace: tempo_api::Trace,
    querier: std::time::Duration,
    convert: std::time::Duration,
) -> (
    axum::Extension<common::self_monitoring::ServerTimings>,
    axum::Json<tempo_api::Trace>,
) {
    let mut timings = common::self_monitoring::ServerTimings::new();
    timings.push("querier", querier);
    timings.push("convert", convert);
    (axum::Extension(timings), axum::Json(trace))
}

/// Map the querier's Flight status for a trace lookup onto an HTTP error
/// carrying the querier's message (#921). Not-found is an expected outcome
/// and logged at info; everything else is an error.
fn trace_lookup_status_to_http(trace_id: &str, status: &tonic::Status) -> ApiError {
    common::self_monitoring::spans::record_rpc_result(
        &tracing::Span::current(),
        common::self_monitoring::spans::RpcBoundary::Client,
        status.code(),
    );
    let code = match status.code() {
        tonic::Code::NotFound => {
            tracing::info!(trace_id = %trace_id, "Trace not found");
            axum::http::StatusCode::NOT_FOUND
        }
        tonic::Code::InvalidArgument => {
            tracing::warn!(trace_id = %trace_id, error = %status, "Invalid trace query");
            axum::http::StatusCode::BAD_REQUEST
        }
        tonic::Code::ResourceExhausted => {
            tracing::warn!(trace_id = %trace_id, error = %status, "Trace query throttled");
            axum::http::StatusCode::TOO_MANY_REQUESTS
        }
        // `Cancelled` is what a client-side channel deadline looks like, so
        // it means the same thing to the caller as `DeadlineExceeded`.
        tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => {
            tracing::error!(trace_id = %trace_id, error = %status, "Trace query timed out");
            axum::http::StatusCode::GATEWAY_TIMEOUT
        }
        _ => {
            tracing::error!(trace_id = %trace_id, error = %status, "Flight query failed for trace");
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    ApiError::new(code, status.message())
}

/// GET https://grafana.com/docs/tempo/latest/api_docs/#search
#[utoipa::path(
    get,
    path = "/tempo/api/search",
    tag = "traces",
    security(("bearerAuth" = [])),
    params(tempo_api::SearchQueryParams),
    responses(
        (status = 200, description = "TraceQL search results", body = tempo_api::SearchResult),
        (status = 400, description = "Invalid query"),
        (status = 429, description = "Rate limited"),
    )
)]
#[tracing::instrument(
    skip(state, tenant_ctx, query),
    fields(
        signaldb.tenant.id = %tenant_ctx.0.tenant_id,
        signaldb.dataset.id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn search<S: RouterState>(
    state: State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(query): Query<tempo_api::SearchQueryParams>,
) -> Result<axum::Json<tempo_api::SearchResult>, ApiError> {
    tracing::info!(
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id,
        "Searching for traces"
    );

    // Get a Flight client for a querier service
    let (mut client, server_address) = match state
        .service_registry()
        .get_flight_client_and_address_for_capability(ServiceCapability::QueryExecution)
        .await
    {
        Ok(result) => result,
        Err(e) => {
            tracing::error!(error = %e, "Failed to get Flight client for query execution");
            return Err(ApiError::new(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "no querier service available",
            ));
        }
    };

    // Create Flight query for trace search with tenant context
    let search_params = serde_json::to_string(&query).map_err(|e| {
        tracing::error!(error = %e, "Failed to serialize search parameters");
        ApiError::new(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "failed to serialize search parameters",
        )
    })?;
    let ticket = Ticket::new(format!(
        "search_traces:{}:{}:{search_params}",
        tenant_ctx.0.tenant_slug, tenant_ctx.0.dataset_slug
    ));
    let mut flight_request = tonic::Request::new(ticket);
    let rpc_span = common::flight::trace_context::do_get_client_span(
        Some("search_traces"),
        &mut flight_request,
        Some(&server_address),
    );
    if let Some(key) = &state.config().auth.internal_service_key {
        common::flight::auth::attach_internal_auth(&mut flight_request, key);
    }

    match client
        .do_get(flight_request)
        .instrument(rpc_span.clone())
        .await
    {
        Ok(response) => {
            let mut stream = response.into_inner();
            let mut search_results = Vec::new();

            // Collect all flight data. As on the trace-lookup path, the
            // querier's terminal status usually surfaces here rather than at
            // `do_get` — a timeout in particular — so map it in both places.
            while let Some(flight_data) = stream.next().await {
                match flight_data {
                    Ok(data) => search_results.push(data),
                    Err(e) => {
                        tracing::error!(error = %e, "Error reading flight data for search");
                        return Err(search_status_to_http(&e));
                    }
                }
            }

            // Convert flight data to search results
            match flight_data_to_search_results(search_results, query.spss).await {
                Ok(search_result) => {
                    tracing::info!(
                        trace_count = search_result.traces.len(),
                        "Successfully converted traces to Tempo search format"
                    );
                    return Ok(axum::Json(search_result));
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to convert flight data to search results");
                    return Err(ApiError::new(
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        "failed to decode search results returned by the querier",
                    ));
                }
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "Flight search query failed");
            return Err(search_status_to_http(&e));
        }
    }
}

/// Map the querier's Flight status for a trace search onto an HTTP error
/// carrying the querier's message (#921). Surface caller errors honestly:
/// bad selectors are 400, unsupported query features are 501, exhausted
/// timeouts are 504, everything else 500.
fn search_status_to_http(status: &tonic::Status) -> ApiError {
    let code = match status.code() {
        tonic::Code::InvalidArgument => axum::http::StatusCode::BAD_REQUEST,
        tonic::Code::Unimplemented => axum::http::StatusCode::NOT_IMPLEMENTED,
        tonic::Code::ResourceExhausted => axum::http::StatusCode::TOO_MANY_REQUESTS,
        // `Cancelled` is what a client-side channel deadline looks like.
        tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => {
            axum::http::StatusCode::GATEWAY_TIMEOUT
        }
        _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
    };
    ApiError::new(code, status.message())
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

/// Fallback lookback for tag-value discovery when the caller sends no
/// `start`/`end`: bound the scan to the recent window instead of reading
/// the whole traces table (#929).
const DEFAULT_TAG_VALUES_WINDOW_SECS: i64 = 24 * 60 * 60;

/// Cap on distinct values returned per tag.
const TAG_VALUES_LIMIT: usize = 1000;

/// Resolve the caller's optional `start`/`end` (unix seconds) into a
/// concrete window: `end` defaults to now, `start` trails `end` by
/// [`DEFAULT_TAG_VALUES_WINDOW_SECS`].
fn resolve_tag_values_window(start: Option<i64>, end: Option<i64>, now_secs: i64) -> (i64, i64) {
    let end = end.unwrap_or(now_secs);
    let start = start.unwrap_or_else(|| end.saturating_sub(DEFAULT_TAG_VALUES_WINDOW_SECS));
    (start, end)
}

/// Convert a caller-supplied unix-**second** timestamp into nanoseconds.
///
/// Tempo's `start`/`end` query parameters are unix seconds; anything that
/// overflows the conversion is not a unix-second timestamp — most often
/// milliseconds from a client that guessed the unit (the #920 class of
/// bug). Mirrors the querier's guard so the router answers 400 and names
/// the problem instead of running a query that matches nothing.
fn tag_window_seconds_to_nanos(name: &str, seconds: i64) -> Result<i64, String> {
    seconds.checked_mul(1_000_000_000).ok_or_else(|| {
        format!(
            "`{name}` ({seconds}) is out of range: expected a unix timestamp in seconds \
             (did you send milliseconds?)"
        )
    })
}

/// Build the DISTINCT-values SQL for one traces column, bounded to
/// `[start_secs, end_secs]`.
///
/// The window is applied twice, mirroring the querier's trace lookup path:
/// a precise `start_time_unix_nano` row bound, plus an equivalent bound on
/// the `timestamp` partition column so Iceberg can prune whole hour
/// partitions (the partition transform is `Hour(timestamp)`, so a filter
/// on `start_time_unix_nano` alone never engages partition pruning —
/// without it, `LIMIT` above `DISTINCT` reads every Parquet file).
fn distinct_values_sql(
    tenant_slug: &str,
    dataset_slug: &str,
    column: &str,
    start_secs: i64,
    end_secs: i64,
) -> Result<String, String> {
    let start_nanos = tag_window_seconds_to_nanos("start", start_secs)?;
    let end_nanos = tag_window_seconds_to_nanos("end", end_secs)?;
    // Slugs are validated at authentication time; quote identifiers so
    // hyphenated slugs parse.
    Ok(format!(
        "SELECT DISTINCT \"{column}\" FROM \"{tenant_slug}\".\"{dataset_slug}\".\"traces\" \
         WHERE \"timestamp\" >= to_timestamp_seconds({start_secs}) \
         AND \"timestamp\" <= to_timestamp_seconds({end_secs}) \
         AND start_time_unix_nano >= {start_nanos} \
         AND start_time_unix_nano <= {end_nanos} \
         ORDER BY 1 LIMIT {TAG_VALUES_LIMIT}"
    ))
}

/// Fetch distinct values of a traces column for the tenant via the
/// querier's Flight SQL path, bounded to the caller's time window.
async fn distinct_column_values<S: RouterState>(
    state: &S,
    tenant_ctx: &common::auth::TenantContext,
    column: &str,
    start: Option<i64>,
    end: Option<i64>,
) -> Result<Vec<String>, ApiError> {
    let (start_secs, end_secs) =
        resolve_tag_values_window(start, end, chrono::Utc::now().timestamp());
    let sql = distinct_values_sql(
        &tenant_ctx.tenant_slug,
        &tenant_ctx.dataset_slug,
        column,
        start_secs,
        end_secs,
    )
    .map_err(|e| {
        tracing::warn!(error = %e, "Rejecting tag values query with invalid time bounds");
        ApiError::bad_request(e)
    })?;

    let (mut client, server_address) = state
        .service_registry()
        .get_flight_client_and_address_for_capability(ServiceCapability::QueryExecution)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to get Flight client for tag values");
            ApiError::new(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "no querier service available",
            )
        })?;
    let mut flight_request = tonic::Request::new(Ticket::new(sql));
    let rpc_span = common::flight::trace_context::do_get_client_span(
        None,
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
        .map_err(|e| {
            common::self_monitoring::spans::record_rpc_result(
                &rpc_span,
                common::self_monitoring::spans::RpcBoundary::Client,
                e.code(),
            );
            tracing::error!(error = %e, "Tag values query failed");
            ApiError::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.message())
        })?
        .into_inner();

    let mut flight_data = Vec::new();
    while let Some(data) = stream.next().await {
        flight_data.push(data.map_err(|e| {
            tracing::error!(error = %e, "Error reading tag values flight data");
            ApiError::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.message())
        })?);
    }
    if flight_data.is_empty() {
        return Ok(vec![]);
    }

    // Honor any dictionary batches the querier sent (#951).
    let batches = flight_data_vec_to_batches(flight_data).await.map_err(|e| {
        tracing::error!(error = %e, "Failed to decode tag values flight data");
        ApiError::new(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "failed to decode tag values returned by the querier",
        )
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
#[utoipa::path(
    get,
    path = "/tempo/api/search/tags",
    tag = "traces",
    security(("bearerAuth" = [])),
    responses(
        (status = 200, description = "Searchable tag names", body = tempo_api::TagSearchResponse),
    )
)]
#[tracing::instrument(skip_all)]
pub async fn search_tags() -> Result<axum::Json<tempo_api::TagSearchResponse>, ApiError> {
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
#[utoipa::path(
    get,
    path = "/tempo/api/search/tag/{tag_name}/values",
    tag = "traces",
    security(("bearerAuth" = [])),
    params(
        ("tag_name" = String, Path, description = "Tag name to fetch values for"),
        ("start" = Option<i64>, Query, description = "Window start (unix seconds)"),
        ("end" = Option<i64>, Query, description = "Window end (unix seconds)"),
    ),
    responses(
        (status = 200, description = "Values for the tag", body = tempo_api::TagValuesResponse),
        (status = 400, description = "start/end are not unix-second timestamps"),
        (status = 501, description = "Tag not queryable yet"),
    )
)]
#[tracing::instrument(skip(state, tenant_ctx, params))]
pub async fn search_tag_values<S: RouterState>(
    state: State<S>,
    tenant_ctx: TenantContextExtractor,
    Path(tag_name): Path<String>,
    Query(params): Query<TagValueSearchParams>,
) -> Result<axum::Json<tempo_api::TagValuesResponse>, ApiError> {
    let tag_values =
        tag_values_for(&state, &tenant_ctx.0, &tag_name, params.start, params.end).await?;
    Ok(axum::Json(tempo_api::TagValuesResponse { tag_values }))
}

async fn tag_values_for<S: RouterState>(
    state: &State<S>,
    tenant_ctx: &common::auth::TenantContext,
    tag_name: &str,
    start: Option<i64>,
    end: Option<i64>,
) -> Result<Vec<String>, ApiError> {
    if let Some(column) = tag_value_column(tag_name) {
        return distinct_column_values(&state.0, tenant_ctx, column, start, end).await;
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
    Err(ApiError::new(
        axum::http::StatusCode::NOT_IMPLEMENTED,
        format!("tag value lookup is not implemented for tag '{tag_name}'"),
    ))
}

/// GET /api/v2/search/tags?scope=<resource|span|intrinsic>
#[tracing::instrument(skip_all)]
pub async fn search_tags_v2(
    Query(_params): Query<TagSearchV2Params>,
) -> Result<axum::Json<tempo_api::v2::TagSearchResponse>, ApiError> {
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
#[tracing::instrument(skip(state, tenant_ctx, params))]
pub async fn search_tag_values_v2<S: RouterState>(
    state: State<S>,
    tenant_ctx: TenantContextExtractor,
    Path(scoped_tag): Path<String>,
    Query(params): Query<TagValueSearchV2Params>,
) -> Result<axum::Json<tempo_api::v2::TagValuesResponse>, ApiError> {
    let values =
        tag_values_for(&state, &tenant_ctx.0, &scoped_tag, params.start, params.end).await?;
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
/// fabricated series this endpoint used to return (issue #552). No
/// parameter extraction: a `Query<MetricsQueryParams>` rejection would
/// answer a plain-text 400 about a missing `q` before the handler runs,
/// hiding the honest "not implemented" from the caller (#921).
#[tracing::instrument(skip_all)]
pub async fn metrics_query() -> Result<axum::Json<MetricsResponse>, ApiError> {
    tracing::debug!("TraceQL metrics instant query not implemented");
    Err(ApiError::new(
        axum::http::StatusCode::NOT_IMPLEMENTED,
        "TraceQL metrics queries are not implemented",
    ))
}

/// GET /api/metrics/query_range - Range TraceQL metrics query with time series
///
/// TraceQL metrics are not implemented. Answer 501 instead of the
/// fabricated series this endpoint used to return (issue #552). As with
/// `metrics_query`, no parameter extraction so the 501 always answers.
#[tracing::instrument(skip_all)]
pub async fn metrics_query_range() -> Result<axum::Json<MetricsResponse>, ApiError> {
    tracing::debug!("TraceQL metrics range query not implemented");
    Err(ApiError::new(
        axum::http::StatusCode::NOT_IMPLEMENTED,
        "TraceQL metrics queries are not implemented",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

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
            events: Vec::new(),
        }
    }

    /// A client-side deadline on the Flight channel surfaces as
    /// `Cancelled`, not `DeadlineExceeded`. Both mean "the query ran out of
    /// time", so both must read as 504 — leaving `Cancelled` in the
    /// catch-all arm turns every slow trace lookup into a bodyless 500.
    #[test]
    fn timeout_codes_map_to_gateway_timeout() {
        for code in [tonic::Code::DeadlineExceeded, tonic::Code::Cancelled] {
            let status = tonic::Status::new(code, "Timeout expired");
            assert_eq!(
                trace_lookup_status_to_http("abc123", &status).status,
                axum::http::StatusCode::GATEWAY_TIMEOUT,
                "trace lookup: {code:?} must map to 504"
            );
            assert_eq!(
                search_status_to_http(&status).status,
                axum::http::StatusCode::GATEWAY_TIMEOUT,
                "search: {code:?} must map to 504"
            );
        }
    }

    #[test]
    fn non_timeout_codes_keep_their_mapping() {
        let cases = [
            (tonic::Code::NotFound, axum::http::StatusCode::NOT_FOUND),
            (
                tonic::Code::InvalidArgument,
                axum::http::StatusCode::BAD_REQUEST,
            ),
            (
                tonic::Code::ResourceExhausted,
                axum::http::StatusCode::TOO_MANY_REQUESTS,
            ),
            (
                tonic::Code::Internal,
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ];
        for (code, expected) in cases {
            let status = tonic::Status::new(code, "boom");
            assert_eq!(
                trace_lookup_status_to_http("abc123", &status).status,
                expected
            );
        }
    }

    /// The querier's diagnostic message (e.g. #920's "did you send
    /// milliseconds?" hint) must survive the mapping so the caller sees it
    /// in the response body instead of a bodyless status code (#921).
    #[test]
    fn status_mappers_preserve_the_querier_message() {
        let hint = "start/end look like unix milliseconds; did you send milliseconds \
                    where unix seconds were expected?";
        let status = tonic::Status::invalid_argument(hint);
        assert_eq!(trace_lookup_status_to_http("abc123", &status).message, hint);
        assert_eq!(search_status_to_http(&status).message, hint);
    }

    // ---- Router-level: error responses carry a JSON body (#921) ----

    mod error_bodies {
        use crate::{RouterAppState, create_router};
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
            create_router(RouterAppState::new(catalog, test_config()))
        }

        async fn get_error(uri: &str) -> (StatusCode, serde_json::Value) {
            let app = test_app().await;
            let request = Request::builder()
                .uri(uri)
                .header("authorization", "Bearer sk-test-key")
                .header("x-tenant-id", "acme")
                .body(Body::empty())
                .unwrap();
            let response = app.oneshot(request).await.unwrap();
            let status = response.status();
            let content_type = response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert!(
                !body.is_empty(),
                "error response for {uri} must carry a body"
            );
            assert!(
                content_type.starts_with("application/json"),
                "error response for {uri} must be JSON, got {content_type:?}"
            );
            let json: serde_json::Value =
                serde_json::from_slice(&body).expect("error body must be valid JSON");
            assert!(
                json["error"]
                    .as_str()
                    .is_some_and(|message| !message.is_empty()),
                "error body for {uri} must carry a non-empty 'error' message: {json}"
            );
            (status, json)
        }

        #[tokio::test]
        async fn trace_lookup_without_a_querier_explains_itself() {
            let (status, _) = get_error("/tempo/api/traces/0123456789abcdef").await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        }

        #[tokio::test]
        async fn search_without_a_querier_explains_itself() {
            let (status, _) = get_error("/tempo/api/search").await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        }

        #[tokio::test]
        async fn tag_values_without_a_querier_explains_itself() {
            let (status, _) = get_error("/tempo/api/search/tag/service.name/values").await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        }

        /// #920/#979: a millisecond `start` is rejected before any querier
        /// contact — and the 400 must carry the diagnostic in the envelope,
        /// not an empty body (#921).
        #[tokio::test]
        async fn millisecond_tag_window_is_rejected_with_an_explanation() {
            let (status, json) = get_error(
                "/tempo/api/search/tag/service.name/values?start=1700000000000&end=1700000001000",
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(
                json["error"].as_str().unwrap().contains("milliseconds"),
                "message should carry the unit hint: {json}"
            );
        }

        #[tokio::test]
        async fn unqueryable_tag_values_explain_themselves() {
            let (status, json) = get_error("/tempo/api/search/tag/http.method/values").await;
            assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
            assert!(
                json["error"].as_str().unwrap().contains("http.method"),
                "message should name the tag: {json}"
            );
        }

        #[tokio::test]
        async fn metrics_queries_explain_they_are_not_implemented() {
            let (status, _) = get_error("/tempo/api/metrics/query").await;
            assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
            let (status, _) = get_error("/tempo/api/metrics/query_range").await;
            assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        }
    }

    /// Tag-value discovery must never scan the whole traces table (#929):
    /// the SQL needs a bound on the `timestamp` partition column (so
    /// Iceberg hour partitions prune) plus the precise
    /// `start_time_unix_nano` row bound, mirroring the trace lookup path.
    #[test]
    fn distinct_values_sql_bounds_scan_by_time_window() {
        let sql = distinct_values_sql("acme", "prod", "service_name", 1_000, 2_000).unwrap();
        assert!(
            sql.contains("SELECT DISTINCT \"service_name\" FROM \"acme\".\"prod\".\"traces\""),
            "unexpected projection/table: {sql}"
        );
        assert!(
            sql.contains("\"timestamp\" >= to_timestamp_seconds(1000)")
                && sql.contains("\"timestamp\" <= to_timestamp_seconds(2000)"),
            "missing partition-column bounds: {sql}"
        );
        assert!(
            sql.contains("start_time_unix_nano >= 1000000000000")
                && sql.contains("start_time_unix_nano <= 2000000000000"),
            "missing precise row bounds: {sql}"
        );
        assert!(
            sql.ends_with("ORDER BY 1 LIMIT 1000"),
            "missing order/limit: {sql}"
        );
    }

    /// #920 class of bug: a caller that sends milliseconds must get a 400,
    /// not a silently empty (or absurd) window. Mirrors the querier's
    /// `unix_seconds_to_nanos` guard.
    #[test]
    fn distinct_values_sql_rejects_millisecond_timestamps() {
        let err = distinct_values_sql(
            "acme",
            "prod",
            "service_name",
            1_753_000_000_000, // unix millis, not seconds
            1_753_000_060_000,
        )
        .unwrap_err();
        assert!(
            err.contains("seconds"),
            "error must name the expected unit: {err}"
        );
    }

    /// Absent `start`/`end` must fall back to a bounded lookback window,
    /// never an unbounded full-table scan.
    #[test]
    fn tag_values_window_defaults_to_bounded_lookback() {
        let now = 1_754_000_000;
        assert_eq!(
            resolve_tag_values_window(None, None, now),
            (now - DEFAULT_TAG_VALUES_WINDOW_SECS, now)
        );
        assert_eq!(resolve_tag_values_window(Some(5), Some(10), now), (5, 10));
        // end-only: the default start trails the supplied end
        assert_eq!(
            resolve_tag_values_window(None, Some(1_000_000), now),
            (1_000_000 - DEFAULT_TAG_VALUES_WINDOW_SECS, 1_000_000)
        );
        // start-only: end defaults to now
        assert_eq!(resolve_tag_values_window(Some(7), None, now), (7, now));
    }

    #[test]
    fn single_trace_response_carries_stage_timings() {
        let trace = tempo_api::Trace {
            trace_id: "trace-1".to_string(),
            root_service_name: String::new(),
            root_trace_name: String::new(),
            start_time_unix_nano: "0".to_string(),
            duration_ms: 0,
            span_sets: Vec::new(),
            profiles: None,
        };
        let (axum::Extension(timings), _json) = timed_trace_response(
            trace,
            std::time::Duration::from_millis(5),
            std::time::Duration::from_millis(1),
        );
        let names: Vec<_> = timings.entries().iter().map(|(name, _)| *name).collect();
        assert_eq!(names, ["querier", "convert"]);
    }

    #[test]
    fn record_batches_to_trace_surfaces_span_events() {
        use common::model::span::{SpanEvent, serialize_span_events};
        use datafusion::arrow::array::{BooleanArray, RecordBatch, StringArray, UInt64Array};
        use std::sync::Arc;

        let events_json = serialize_span_events(&[SpanEvent {
            name: "exception".to_string(),
            timestamp_unix_nano: 5,
            attributes: HashMap::from([(
                "exception.message".to_string(),
                serde_json::Value::String("boom".to_string()),
            )]),
        }]);

        // A single-span wire batch (create_span_batch_schema order) with the
        // events column carrying the exception.
        let batch = RecordBatch::try_new(
            Arc::new(common::flight::schema::create_span_batch_schema()),
            vec![
                Arc::new(StringArray::from(vec!["trace-1"])),
                Arc::new(StringArray::from(vec!["root"])),
                Arc::new(StringArray::from(vec![""])),
                Arc::new(StringArray::from(vec!["Error"])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(StringArray::from(vec!["op"])),
                Arc::new(StringArray::from(vec!["svc"])),
                Arc::new(StringArray::from(vec!["Server"])),
                Arc::new(UInt64Array::from(vec![1_000u64])),
                Arc::new(UInt64Array::from(vec![10u64])),
                Arc::new(StringArray::from(vec![Some("{}")])),
                Arc::new(StringArray::from(vec![Some("{}")])),
                Arc::new(StringArray::from(vec![Some(events_json.as_str())])),
            ],
        )
        .unwrap();

        let trace = record_batches_to_trace(vec![batch], "trace-1").unwrap();
        let tempo = internal_trace_to_tempo(&trace, None);
        let span = &tempo.span_sets[0].spans[0];
        assert_eq!(span.events.len(), 1);
        assert_eq!(span.events[0].name, "exception");
        assert_eq!(
            span.events[0]
                .attributes
                .get("exception.message")
                .map(|a| &a.value),
            Some(&tempo_api::Value::StringValue("boom".to_string()))
        );
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

    // ---- Router-level tests: the routed handlers, not just the helpers ----
    //
    // No real database/catalog setup is needed: `Catalog::new("sqlite::memory:")`
    // is sufficient, matching the pattern used in admin.rs/session.rs/logql.rs.

    mod handlers {
        use crate::{RouterAppState, create_router};
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
            create_router(RouterAppState::new(catalog, test_config()))
        }

        async fn authed_get(app: &axum::Router, uri: &str) -> StatusCode {
            let request = Request::builder()
                .uri(uri)
                .header("authorization", "Bearer sk-test-key")
                .header("x-tenant-id", "acme")
                .body(Body::empty())
                .unwrap();
            app.clone().oneshot(request).await.unwrap().status()
        }

        #[tokio::test]
        async fn search_without_a_querier_is_service_unavailable() {
            // A valid search with no discovered querier surfaces 503, not a
            // panic or a fabricated empty 200.
            let app = test_app().await;
            assert_eq!(
                authed_get(&app, "/tempo/api/search").await,
                StatusCode::SERVICE_UNAVAILABLE
            );
        }

        #[tokio::test]
        async fn query_single_trace_without_a_querier_is_service_unavailable() {
            let app = test_app().await;
            assert_eq!(
                authed_get(&app, "/tempo/api/traces/abc123").await,
                StatusCode::SERVICE_UNAVAILABLE
            );
        }

        #[tokio::test]
        async fn search_tag_values_for_unsupported_tag_is_not_implemented() {
            // "duration" has no queryable column and isn't the synthetic
            // "status" tag, so it must 501 without ever reaching a querier.
            let app = test_app().await;
            assert_eq!(
                authed_get(&app, "/tempo/api/search/tag/duration/values").await,
                StatusCode::NOT_IMPLEMENTED
            );
        }

        #[tokio::test]
        async fn search_tag_values_for_supported_tag_without_a_querier_is_service_unavailable() {
            // "service.name" maps to a real column and requires a querier
            // round-trip, unlike the unsupported-tag case above.
            let app = test_app().await;
            assert_eq!(
                authed_get(&app, "/tempo/api/search/tag/service.name/values").await,
                StatusCode::SERVICE_UNAVAILABLE
            );
        }

        #[tokio::test]
        async fn metrics_query_range_is_not_implemented() {
            let app = test_app().await;
            // `q` is a required param: without it the Query extractor answers
            // 400 before the handler runs. A well-formed request must get the
            // handler's 501 (TraceQL metrics unimplemented, #552).
            assert_eq!(
                authed_get(&app, "/tempo/api/metrics/query_range?q=%7B%7D").await,
                StatusCode::NOT_IMPLEMENTED
            );
        }

        #[tokio::test]
        async fn tempo_endpoints_require_authentication() {
            let app = test_app().await;

            let request = Request::builder()
                .uri("/tempo/api/search")
                .body(Body::empty())
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

            let request = Request::builder()
                .uri("/tempo/api/search")
                .header("authorization", "Bearer sk-wrong-key")
                .header("x-tenant-id", "acme")
                .body(Body::empty())
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
    }
}
