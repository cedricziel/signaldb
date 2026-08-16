//! # Pyroscope-Compatible HTTP API
//!
//! Query endpoints for the profiles signal in the format Grafana's
//! Pyroscope datasource expects:
//!
//! - `GET /render` — flamegraph for a query and time range
//! - `GET /render-diff` — differential flamegraph (baseline vs comparison)
//! - `GET /label-names`, `GET /label-values` — label discovery
//! - `GET /profile-types` — available profile types
//!
//! Handlers forward tenant-scoped Flight tickets to the querier and shape
//! the JSON results into `pyroscope-api` types.

use super::api_error::ApiError;
use crate::RouterState;
use arrow_flight::Ticket;
use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    routing::get,
};
use common::auth::TenantContextExtractor;
use common::flight::transport::ServiceCapability;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use pyroscope_api::{
    Flamebearer, FlamebearerMetadata, LabelsResponse, ProfileType, RenderResponse,
};
use serde::Deserialize;
use tracing::Instrument;
use utoipa::IntoParams;

pub fn router<S: RouterState>() -> Router<S> {
    Router::new()
        .route("/render", get(render::<S>))
        .route("/render-diff", get(render_diff::<S>))
        .route("/label-names", get(label_names::<S>))
        .route("/label-values", get(label_values::<S>))
        .route("/profile-types", get(profile_types::<S>))
}

/// Routes for trace-to-profile correlation, nested at `/api/profiles`.
pub fn profiles_router<S: RouterState>() -> Router<S> {
    Router::new().route("/trace/{trace_id}", get(profiles_by_trace::<S>))
}

/// Query parameters for `/render` and `/render-diff`.
#[derive(Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct RenderParams {
    /// Pyroscope query: `{app}` or `{type}{label="value",...}`.
    pub query: Option<String>,
    /// Range start: unix seconds, unix milliseconds, or `now[-<N><s|m|h|d>]`.
    pub from: Option<String>,
    /// Range end, same forms as `from`.
    pub until: Option<String>,
    /// Diff-only: baseline range start.
    #[serde(rename = "leftFrom")]
    pub left_from: Option<String>,
    /// Diff-only: baseline range end.
    #[serde(rename = "leftUntil")]
    pub left_until: Option<String>,
    /// Diff-only: comparison range start.
    #[serde(rename = "rightFrom")]
    pub right_from: Option<String>,
    /// Diff-only: comparison range end.
    #[serde(rename = "rightUntil")]
    pub right_until: Option<String>,
}

/// Query parameters for the discovery endpoints.
#[derive(Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct DiscoveryParams {
    /// Range start: unix seconds, unix milliseconds, or `now[-<N><s|m|h|d>]`.
    pub from: Option<String>,
    /// Range end, same forms as `from`.
    pub until: Option<String>,
    /// Label name for `/label-values`.
    pub label: Option<String>,
}

/// The selector part of a Pyroscope query, resolved to querier filters.
#[derive(Debug, Default, PartialEq)]
struct ParsedQuery {
    sample_type: Option<String>,
    service_name: Option<String>,
}

/// Parse a Pyroscope query like `process_cpu:cpu:nanoseconds{service_name="checkout"}`
/// or a bare app/type name. The first ID segment maps to our sample type;
/// a `service_name` matcher narrows the service.
fn parse_query(query: &str) -> ParsedQuery {
    let query = query.trim();
    if query.is_empty() {
        return ParsedQuery::default();
    }

    let (id_part, selector_part) = match query.split_once('{') {
        Some((id, rest)) => (id.trim(), rest.strip_suffix('}').unwrap_or(rest)),
        None => (query, ""),
    };

    // Profile type IDs are colon-separated; our sample_type is the second
    // segment when present (`{name}:{sample_type}:{unit}...`), else the
    // whole ID.
    let sample_type = if id_part.is_empty() {
        None
    } else {
        let segments: Vec<&str> = id_part.split(':').collect();
        Some(
            segments
                .get(1)
                .filter(|s| !s.is_empty())
                .unwrap_or(&segments[0])
                .to_string(),
        )
    };

    let mut service_name = None;
    for matcher in selector_part.split(',') {
        let Some((key, value)) = matcher.split_once('=') else {
            continue;
        };
        if key.trim() == "service_name" {
            service_name = Some(value.trim().trim_matches('"').to_string());
        }
    }

    ParsedQuery {
        sample_type,
        service_name,
    }
}

/// Parse a Pyroscope time parameter: unix seconds, unix milliseconds, or
/// relative `now[-<N><s|m|h|d>]`. Returns unix seconds.
fn parse_time(value: &str) -> Option<i64> {
    parse_time_at(value, chrono::Utc::now().timestamp())
}

/// [`parse_time`] with the `now` anchor for the relative case injected,
/// so the relative-time parsing logic can be asserted exactly instead of
/// against the wall clock.
fn parse_time_at(value: &str, now: i64) -> Option<i64> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Ok(number) = value.parse::<i64>() {
        // Values this large are unix milliseconds.
        return Some(if number > 100_000_000_000 {
            number / 1000
        } else {
            number
        });
    }
    if let Some(rest) = value.strip_prefix("now") {
        if rest.is_empty() {
            return Some(now);
        }
        let rest = rest.strip_prefix('-')?;
        // split_at takes a byte index; a trailing multi-byte character (e.g.
        // "now-1é") would land mid-character and panic in a handler that
        // receives client-controlled input. Relative units are ASCII-only.
        if !rest.is_ascii() {
            return None;
        }
        let (digits, unit) = rest.split_at(rest.len().saturating_sub(1));
        let amount: i64 = digits.parse().ok()?;
        let seconds = match unit {
            "s" => amount,
            "m" => amount * 60,
            "h" => amount * 3600,
            "d" => amount * 86400,
            _ => return None,
        };
        return Some(now - seconds);
    }
    None
}

/// Build the JSON search-params object for the querier's profile tickets.
fn search_params_json(parsed: &ParsedQuery, from: Option<i64>, until: Option<i64>) -> String {
    serde_json::json!({
        "service_name": parsed.service_name,
        "sample_type": parsed.sample_type,
        "start": from,
        "end": until,
    })
    .to_string()
}

/// Execute a Flight ticket against a querier and collect the batches.
async fn execute_ticket<S: RouterState>(
    state: &S,
    ticket_content: String,
) -> Result<Vec<RecordBatch>, ApiError> {
    let (mut client, server_address) = state
        .service_registry()
        .get_flight_client_and_address_for_capability(ServiceCapability::QueryExecution)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to get Flight client for profile query");
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "no querier service available",
            )
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
        .map_err(|e| rpc_span.in_scope(|| flight_status_to_http(&e)))?
        .into_inner();

    let mut data = Vec::new();
    while let Some(flight_data) = stream.next().await {
        data.push(flight_data.map_err(|e| flight_status_to_http(&e))?);
    }

    super::flight_decode::decode_flight_batches(data, "profiles")
        .await
        .map_err(ApiError::from)
}

/// Map the querier's Flight status onto an HTTP error carrying the
/// querier's message (#921).
fn flight_status_to_http(status: &tonic::Status) -> ApiError {
    common::self_monitoring::spans::record_rpc_result(
        &tracing::Span::current(),
        common::self_monitoring::spans::RpcBoundary::Client,
        status.code(),
    );
    let code = match status.code() {
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        tonic::Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        _ => {
            tracing::error!(error = %status, "Profile Flight query failed");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    ApiError::new(code, status.message())
}

/// Collect every value of the (single) string column across batches.
fn string_values(batches: &[RecordBatch]) -> Vec<String> {
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
    values
}

/// Decode the single JSON document returned by flamegraph/diff tickets.
fn decode_json_result<T: serde::de::DeserializeOwned>(
    batches: &[RecordBatch],
) -> Result<T, ApiError> {
    let values = string_values(batches);
    let json = values.first().ok_or_else(|| {
        tracing::error!("Profile query returned no result document");
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "profile query returned no result document",
        )
    })?;
    serde_json::from_str(json).map_err(|e| {
        tracing::error!(error = %e, "Failed to parse profile query result");
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to parse the profile query result",
        )
    })
}

/// GET /pyroscope/render — aggregate profiles into a flamegraph.
#[utoipa::path(
    get,
    path = "/pyroscope/render",
    operation_id = "pyroscope_render",
    tag = "profiles",
    security(("bearerAuth" = [])),
    params(RenderParams),
    responses(
        (status = 200, description = "Aggregated flame graph (flamebearer encoding)", body = pyroscope_api::RenderResponse),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
    )
)]
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(
        signaldb.tenant.id = %tenant_ctx.0.tenant_id,
        signaldb.dataset.id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn render<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<RenderParams>,
) -> Result<axum::Json<RenderResponse>, ApiError> {
    let parsed = parse_query(params.query.as_deref().unwrap_or(""));
    let from = params.from.as_deref().and_then(parse_time);
    let until = params.until.as_deref().and_then(parse_time);

    let ticket = format!(
        "profile_flamegraph:{}:{}:{}",
        tenant_ctx.0.tenant_slug,
        tenant_ctx.0.dataset_slug,
        search_params_json(&parsed, from, until)
    );
    let batches = execute_ticket(&state, ticket).await?;
    let flamegraph: common::profile::Flamegraph = decode_json_result(&batches)?;

    Ok(axum::Json(RenderResponse {
        flamebearer: Flamebearer {
            names: flamegraph.names,
            levels: flamegraph.levels,
            num_ticks: flamegraph.total,
            max_self: flamegraph.max_self,
        },
        metadata: FlamebearerMetadata {
            format: "single".to_string(),
            spy_name: None,
            sample_rate: 100,
            units: "samples".to_string(),
            name: params.query.unwrap_or_default(),
        },
        timeline: None,
        left_ticks: None,
        right_ticks: None,
    }))
}

/// GET /pyroscope/render-diff — differential flamegraph between two ranges.
#[utoipa::path(
    get,
    path = "/pyroscope/render-diff",
    operation_id = "pyroscope_render_diff",
    tag = "profiles",
    security(("bearerAuth" = [])),
    params(RenderParams),
    responses(
        (status = 200, description = "Differential flame graph (baseline vs comparison)", body = pyroscope_api::RenderResponse),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
    )
)]
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(
        signaldb.tenant.id = %tenant_ctx.0.tenant_id,
        signaldb.dataset.id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn render_diff<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<RenderParams>,
) -> Result<axum::Json<RenderResponse>, ApiError> {
    let parsed = parse_query(params.query.as_deref().unwrap_or(""));
    let left_from = params.left_from.as_deref().and_then(parse_time);
    let left_until = params.left_until.as_deref().and_then(parse_time);
    let right_from = params.right_from.as_deref().and_then(parse_time);
    let right_until = params.right_until.as_deref().and_then(parse_time);

    let diff_params = serde_json::json!({
        "baseline": serde_json::from_str::<serde_json::Value>(
            &search_params_json(&parsed, left_from, left_until)
        ).unwrap_or_default(),
        "comparison": serde_json::from_str::<serde_json::Value>(
            &search_params_json(&parsed, right_from, right_until)
        ).unwrap_or_default(),
    });

    let ticket = format!(
        "profile_diff:{}:{}:{}",
        tenant_ctx.0.tenant_slug, tenant_ctx.0.dataset_slug, diff_params
    );
    let batches = execute_ticket(&state, ticket).await?;
    let diff: common::profile::DiffFlamegraph = decode_json_result(&batches)?;

    Ok(axum::Json(RenderResponse {
        flamebearer: Flamebearer {
            names: diff.names,
            levels: diff.levels,
            num_ticks: diff.total,
            max_self: diff.max_self,
        },
        metadata: FlamebearerMetadata {
            format: "double".to_string(),
            spy_name: None,
            sample_rate: 100,
            units: "samples".to_string(),
            name: params.query.unwrap_or_default(),
        },
        timeline: None,
        left_ticks: Some(diff.left_ticks),
        right_ticks: Some(diff.right_ticks),
    }))
}

/// Build the discovery-window JSON tail for discovery tickets.
fn discovery_params_json(params: &DiscoveryParams) -> String {
    serde_json::json!({
        "start": params.from.as_deref().and_then(parse_time),
        "end": params.until.as_deref().and_then(parse_time),
    })
    .to_string()
}

/// GET /pyroscope/label-names
#[utoipa::path(
    get,
    path = "/pyroscope/label-names",
    operation_id = "pyroscope_label_names",
    tag = "profiles",
    security(("bearerAuth" = [])),
    params(DiscoveryParams),
    responses(
        (status = 200, description = "Known profile label names", body = pyroscope_api::LabelsResponse),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
    )
)]
#[tracing::instrument(skip(state, tenant_ctx, params))]
pub async fn label_names<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<DiscoveryParams>,
) -> Result<axum::Json<LabelsResponse>, ApiError> {
    let ticket = format!(
        "label_names:{}:{}:{}",
        tenant_ctx.0.tenant_slug,
        tenant_ctx.0.dataset_slug,
        discovery_params_json(&params)
    );
    let batches = execute_ticket(&state, ticket).await?;
    Ok(axum::Json(LabelsResponse {
        names: string_values(&batches),
    }))
}

/// GET /pyroscope/label-values?label=<name>
#[utoipa::path(
    get,
    path = "/pyroscope/label-values",
    operation_id = "pyroscope_label_values",
    tag = "profiles",
    security(("bearerAuth" = [])),
    params(DiscoveryParams),
    responses(
        (status = 200, description = "Known values for the requested label", body = pyroscope_api::LabelsResponse),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
    )
)]
#[tracing::instrument(skip(state, tenant_ctx, params))]
pub async fn label_values<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<DiscoveryParams>,
) -> Result<axum::Json<LabelsResponse>, ApiError> {
    let label = params.label.clone().filter(|l| !l.is_empty()).ok_or_else(
        // Pyroscope requires the label parameter here.
        || ApiError::bad_request("missing or empty 'label' parameter"),
    )?;
    let ticket = format!(
        "label_values:{}:{}:{}:{}",
        tenant_ctx.0.tenant_slug,
        tenant_ctx.0.dataset_slug,
        label,
        discovery_params_json(&params)
    );
    let batches = execute_ticket(&state, ticket).await?;
    Ok(axum::Json(LabelsResponse {
        names: string_values(&batches),
    }))
}

/// GET /pyroscope/profile-types
#[utoipa::path(
    get,
    path = "/pyroscope/profile-types",
    operation_id = "pyroscope_profile_types",
    tag = "profiles",
    security(("bearerAuth" = [])),
    params(DiscoveryParams),
    responses(
        (status = 200, description = "Profile types with data in the requested window", body = [pyroscope_api::ProfileType]),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
    )
)]
#[tracing::instrument(skip(state, tenant_ctx, params))]
pub async fn profile_types<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<DiscoveryParams>,
) -> Result<axum::Json<Vec<ProfileType>>, ApiError> {
    let ticket = format!(
        "profile_types:{}:{}:{}",
        tenant_ctx.0.tenant_slug,
        tenant_ctx.0.dataset_slug,
        discovery_params_json(&params)
    );
    let batches = execute_ticket(&state, ticket).await?;
    let types = string_values(&batches)
        .iter()
        .map(|entry| {
            let (sample_type, sample_unit) = entry.split_once(':').unwrap_or((entry.as_str(), ""));
            ProfileType::from_type_unit(sample_type, sample_unit)
        })
        .collect();
    Ok(axum::Json(types))
}

/// Decode profile summary batches (the querier's search/by-trace column
/// set) into API summaries.
pub(crate) fn batches_to_profile_summaries(
    batches: &[RecordBatch],
) -> Vec<tempo_api::ProfileSummary> {
    use datafusion::arrow::array::{Int64Array, TimestampNanosecondArray};

    let mut summaries = Vec::new();
    for batch in batches {
        let get_string = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        };
        let Some(profile_ids) = get_string("profile_id") else {
            continue;
        };
        let timestamps = batch
            .column_by_name("timestamp")
            .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>());
        let durations = batch
            .column_by_name("duration_nano")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        let sample_types = get_string("sample_type");
        let sample_units = get_string("sample_unit");
        let service_names = get_string("service_name");
        let span_ids = get_string("span_id");

        let value = |col: Option<&StringArray>, i: usize| -> String {
            col.map(|c| {
                if c.is_null(i) {
                    String::new()
                } else {
                    c.value(i).to_string()
                }
            })
            .unwrap_or_default()
        };

        for i in 0..batch.num_rows() {
            summaries.push(tempo_api::ProfileSummary {
                profile_id: value(Some(profile_ids), i),
                time_unix_nano: timestamps
                    .map(|t| if t.is_null(i) { 0 } else { t.value(i) })
                    .unwrap_or(0)
                    .to_string(),
                duration_nano: durations
                    .map(|d| if d.is_null(i) { 0 } else { d.value(i) })
                    .unwrap_or(0)
                    .to_string(),
                sample_type: value(sample_types, i),
                sample_unit: value(sample_units, i),
                service_name: value(service_names, i),
                span_id: span_ids.and_then(|c| {
                    if c.is_null(i) || c.value(i).is_empty() {
                        None
                    } else {
                        Some(c.value(i).to_string())
                    }
                }),
            });
        }
    }
    summaries
}

/// Fetch summaries of profiles linked to a trace via the querier.
pub(crate) async fn fetch_profiles_for_trace<S: RouterState>(
    state: &S,
    tenant_slug: &str,
    dataset_slug: &str,
    trace_id: &str,
) -> Result<Vec<tempo_api::ProfileSummary>, ApiError> {
    let ticket = format!("profiles_by_trace:{tenant_slug}:{dataset_slug}:{trace_id}");
    let batches = execute_ticket(state, ticket).await?;
    Ok(batches_to_profile_summaries(&batches))
}

/// GET /api/profiles/trace/{trace_id} — profiles linked to a trace.
#[utoipa::path(
    get,
    path = "/api/profiles/trace/{trace_id}",
    operation_id = "profiles_by_trace",
    tag = "profiles",
    security(("bearerAuth" = [])),
    params(
        ("trace_id" = String, Path, description = "Trace ID to fetch correlated profiles for"),
    ),
    responses(
        (status = 200, description = "Summaries of the profiles linked to the trace", body = [tempo_api::ProfileSummary]),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
    )
)]
#[tracing::instrument(
    skip(state, tenant_ctx),
    fields(
        signaldb.tenant.id = %tenant_ctx.0.tenant_id,
        signaldb.dataset.id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn profiles_by_trace<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    axum::extract::Path(trace_id): axum::extract::Path<String>,
) -> Result<axum::Json<Vec<tempo_api::ProfileSummary>>, ApiError> {
    let summaries = fetch_profiles_for_trace(
        &state,
        &tenant_ctx.0.tenant_slug,
        &tenant_ctx.0.dataset_slug,
        &trace_id,
    )
    .await?;
    Ok(axum::Json(summaries))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- OpenAPI wire-format fixtures (task 1.1): the typed response
    // structs' `Serialize` output must stay byte-identical to the fixed
    // fixtures below, guarding against an accidental field rename/reorder
    // when the structs gained `ToSchema` for the OpenAPI document. ----

    #[test]
    fn render_response_wire_format_is_stable() {
        let response = RenderResponse {
            flamebearer: Flamebearer {
                names: vec!["total".to_string(), "main".to_string()],
                levels: vec![vec![0, 100, 0, 0], vec![0, 100, 40, 1]],
                num_ticks: 100,
                max_self: 40,
            },
            metadata: FlamebearerMetadata {
                format: "single".to_string(),
                spy_name: None,
                sample_rate: 100,
                units: "samples".to_string(),
                name: "cpu".to_string(),
            },
            timeline: None,
            left_ticks: None,
            right_ticks: None,
        };
        let expected = serde_json::json!({
            "flamebearer": {
                "names": ["total", "main"],
                "levels": [[0, 100, 0, 0], [0, 100, 40, 1]],
                "numTicks": 100,
                "maxSelf": 40
            },
            "metadata": {
                "format": "single",
                "sampleRate": 100,
                "units": "samples",
                "name": "cpu"
            }
        });
        assert_eq!(serde_json::to_value(&response).unwrap(), expected);
    }

    #[test]
    fn render_diff_response_wire_format_is_stable() {
        let response = RenderResponse {
            metadata: FlamebearerMetadata {
                format: "double".to_string(),
                ..FlamebearerMetadata::default()
            },
            left_ticks: Some(175),
            right_ticks: Some(250),
            ..RenderResponse::default()
        };
        let expected = serde_json::json!({
            "flamebearer": {
                "names": [],
                "levels": [],
                "numTicks": 0,
                "maxSelf": 0
            },
            "metadata": {
                "format": "double",
                "sampleRate": 0,
                "units": "",
                "name": ""
            },
            "leftTicks": 175,
            "rightTicks": 250
        });
        assert_eq!(serde_json::to_value(&response).unwrap(), expected);
    }

    #[test]
    fn labels_response_wire_format_is_stable() {
        let response = LabelsResponse {
            names: vec!["service_name".to_string(), "env".to_string()],
        };
        let expected = serde_json::json!({ "names": ["service_name", "env"] });
        assert_eq!(serde_json::to_value(&response).unwrap(), expected);
    }

    #[test]
    fn profile_type_wire_format_is_stable() {
        let types = vec![ProfileType::from_type_unit("cpu", "nanoseconds")];
        let expected = serde_json::json!([{
            "ID": "cpu:cpu:nanoseconds",
            "name": "cpu",
            "sampleType": "cpu",
            "sampleUnit": "nanoseconds"
        }]);
        assert_eq!(serde_json::to_value(&types).unwrap(), expected);
    }

    #[test]
    fn profile_summary_wire_format_is_stable() {
        let summaries = vec![tempo_api::ProfileSummary {
            profile_id: "p1".to_string(),
            time_unix_nano: "1700000000000000000".to_string(),
            duration_nano: "1000000".to_string(),
            sample_type: "cpu".to_string(),
            sample_unit: "nanoseconds".to_string(),
            service_name: "checkout".to_string(),
            span_id: Some("abc123".to_string()),
        }];
        let expected = serde_json::json!([{
            "profileID": "p1",
            "timeUnixNano": "1700000000000000000",
            "durationNano": "1000000",
            "sampleType": "cpu",
            "sampleUnit": "nanoseconds",
            "serviceName": "checkout",
            "spanID": "abc123"
        }]);
        assert_eq!(serde_json::to_value(&summaries).unwrap(), expected);
    }

    #[test]
    fn parses_full_pyroscope_query() {
        let parsed =
            parse_query(r#"process_cpu:cpu:nanoseconds{service_name="checkout",env="prod"}"#);
        assert_eq!(parsed.sample_type.as_deref(), Some("cpu"));
        assert_eq!(parsed.service_name.as_deref(), Some("checkout"));
    }

    #[test]
    fn parses_bare_app_query() {
        let parsed = parse_query("cpu");
        assert_eq!(parsed.sample_type.as_deref(), Some("cpu"));
        assert_eq!(parsed.service_name, None);
    }

    #[test]
    fn parses_empty_query() {
        assert_eq!(parse_query(""), ParsedQuery::default());
    }

    #[test]
    fn parses_absolute_unix_seconds() {
        assert_eq!(parse_time("1700000000"), Some(1_700_000_000));
    }

    #[test]
    fn parses_absolute_unix_milliseconds() {
        assert_eq!(parse_time("1700000000000"), Some(1_700_000_000));
    }

    #[test]
    fn parses_relative_time_against_a_fixed_anchor() {
        // Anchored to a fixed `now` so the expected value is exact rather
        // than a wall-clock tolerance window.
        let now = 1_700_100_000;
        assert_eq!(parse_time_at("now", now), Some(now));
        assert_eq!(parse_time_at("now-1h", now), Some(now - 3600));
        assert_eq!(parse_time_at("now-30m", now), Some(now - 1800));
        assert_eq!(parse_time_at("now-45s", now), Some(now - 45));
        assert_eq!(parse_time_at("now-2d", now), Some(now - 172_800));
    }

    #[test]
    fn rejects_unparseable_time_values() {
        assert_eq!(parse_time("later"), None);
        assert_eq!(parse_time_at("now-1x", 1_700_100_000), None);
    }

    #[test]
    fn rejects_non_ascii_relative_time_without_panicking() {
        // Regression: split_at on a byte index panicked on a trailing
        // multi-byte character in this client-controlled parameter (DoS).
        assert_eq!(parse_time_at("now-1é", 1_700_100_000), None);
        assert_eq!(parse_time_at("now-é", 1_700_100_000), None);
    }

    // ---- Router-level tests: the routed handlers, not just the helpers ----

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
        async fn label_values_without_label_param_is_bad_request() {
            let app = test_app().await;
            assert_eq!(
                authed_get(&app, "/pyroscope/label-values").await,
                StatusCode::BAD_REQUEST
            );
        }

        #[tokio::test]
        async fn render_without_a_querier_is_service_unavailable() {
            // A valid render request with no discovered querier surfaces
            // 503, not a panic or a 200 with empty data.
            let app = test_app().await;
            assert_eq!(
                authed_get(&app, "/pyroscope/render?query=cpu").await,
                StatusCode::SERVICE_UNAVAILABLE
            );
        }

        #[tokio::test]
        async fn profile_types_without_a_querier_is_service_unavailable() {
            let app = test_app().await;
            assert_eq!(
                authed_get(&app, "/pyroscope/profile-types").await,
                StatusCode::SERVICE_UNAVAILABLE
            );
        }

        #[tokio::test]
        async fn pyroscope_endpoints_require_authentication() {
            let app = test_app().await;

            let request = Request::builder()
                .uri("/pyroscope/profile-types")
                .body(Body::empty())
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

            let request = Request::builder()
                .uri("/pyroscope/profile-types")
                .header("authorization", "Bearer sk-wrong-key")
                .header("x-tenant-id", "acme")
                .body(Body::empty())
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
    }

    /// The querier's Flight message must survive into the HTTP error so
    /// callers see why a profile query failed (#921).
    #[test]
    fn flight_status_mapping_preserves_the_querier_message() {
        let status = tonic::Status::invalid_argument("unknown sample type 'wall'");
        let err = flight_status_to_http(&status);
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.message, "unknown sample type 'wall'");
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

        async fn get_error(uri: &str) -> (StatusCode, serde_json::Value) {
            let catalog = Catalog::new("sqlite::memory:").await.unwrap();
            let app = create_router(RouterAppState::new(catalog, test_config()));
            let request = Request::builder()
                .uri(uri)
                .header("authorization", "Bearer sk-test-key")
                .header("x-tenant-id", "acme")
                .body(Body::empty())
                .unwrap();
            let response = app.oneshot(request).await.unwrap();
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert!(
                !body.is_empty(),
                "error response for {uri} must carry a body"
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
        async fn render_without_a_querier_explains_itself() {
            let (status, _) = get_error("/pyroscope/render?query=cpu").await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        }

        #[tokio::test]
        async fn label_values_without_label_explains_itself() {
            let (status, json) = get_error("/pyroscope/label-values").await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(
                json["error"].as_str().unwrap().contains("label"),
                "message should name the missing parameter: {json}"
            );
        }

        #[tokio::test]
        async fn profiles_by_trace_without_a_querier_explains_itself() {
            let (status, _) = get_error("/api/profiles/trace/0123456789abcdef").await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        }
    }
}
