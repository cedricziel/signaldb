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

use crate::RouterState;
use arrow_flight::{Ticket, utils::flight_data_to_batches};
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

pub fn router<S: RouterState>() -> Router<S> {
    Router::new()
        .route("/render", get(render::<S>))
        .route("/render-diff", get(render_diff::<S>))
        .route("/label-names", get(label_names::<S>))
        .route("/label-values", get(label_values::<S>))
        .route("/profile-types", get(profile_types::<S>))
}

/// Query parameters for `/render` and `/render-diff`.
#[derive(Debug, Default, Deserialize)]
pub struct RenderParams {
    /// Pyroscope query: `{app}` or `{type}{label="value",...}`.
    pub query: Option<String>,
    pub from: Option<String>,
    pub until: Option<String>,
    /// Diff-only: baseline range.
    #[serde(rename = "leftFrom")]
    pub left_from: Option<String>,
    #[serde(rename = "leftUntil")]
    pub left_until: Option<String>,
    /// Diff-only: comparison range.
    #[serde(rename = "rightFrom")]
    pub right_from: Option<String>,
    #[serde(rename = "rightUntil")]
    pub right_until: Option<String>,
}

/// Query parameters for the discovery endpoints.
#[derive(Debug, Default, Deserialize)]
pub struct DiscoveryParams {
    pub from: Option<String>,
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
        let now = chrono::Utc::now().timestamp();
        if rest.is_empty() {
            return Some(now);
        }
        let rest = rest.strip_prefix('-')?;
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
) -> Result<Vec<RecordBatch>, StatusCode> {
    let mut client = state
        .service_registry()
        .get_flight_client_for_capability(ServiceCapability::QueryExecution)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to get Flight client for profile query");
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
        tracing::error!(error = %e, "Failed to decode profile Flight data");
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
        _ => {
            tracing::error!(error = %status, "Profile Flight query failed");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
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
) -> Result<T, StatusCode> {
    let values = string_values(batches);
    let json = values.first().ok_or_else(|| {
        tracing::error!("Profile query returned no result document");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    serde_json::from_str(json).map_err(|e| {
        tracing::error!(error = %e, "Failed to parse profile query result");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

/// GET /pyroscope/render — aggregate profiles into a flamegraph.
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn render<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<RenderParams>,
) -> Result<axum::Json<RenderResponse>, StatusCode> {
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
#[tracing::instrument(
    skip(state, tenant_ctx, params),
    fields(
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn render_diff<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<RenderParams>,
) -> Result<axum::Json<RenderResponse>, StatusCode> {
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
#[tracing::instrument(skip(state, tenant_ctx, params))]
pub async fn label_names<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<DiscoveryParams>,
) -> Result<axum::Json<LabelsResponse>, StatusCode> {
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
#[tracing::instrument(skip(state, tenant_ctx, params))]
pub async fn label_values<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<DiscoveryParams>,
) -> Result<axum::Json<LabelsResponse>, StatusCode> {
    let label = params.label.clone().filter(|l| !l.is_empty()).ok_or(
        // Pyroscope requires the label parameter here.
        StatusCode::BAD_REQUEST,
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
#[tracing::instrument(skip(state, tenant_ctx, params))]
pub async fn profile_types<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<DiscoveryParams>,
) -> Result<axum::Json<Vec<ProfileType>>, StatusCode> {
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn parses_absolute_and_relative_times() {
        assert_eq!(parse_time("1700000000"), Some(1_700_000_000));
        // Milliseconds are detected and converted.
        assert_eq!(parse_time("1700000000000"), Some(1_700_000_000));
        let now = chrono::Utc::now().timestamp();
        let one_hour_ago = parse_time("now-1h").expect("relative time");
        assert!((now - 3600 - one_hour_ago).abs() <= 2);
        assert!(parse_time("later").is_none());
    }
}
