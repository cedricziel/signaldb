//! Operational control (ops) API.
//!
//! Proxies `/api/v1/ops/*` HTTP requests to the compactor's Flight `do_action`
//! control surface (service capability `StorageMaintenance`), so operators can
//! trigger and inspect compaction through the router — and, in turn, through the
//! SDK, CLI, and MCP server.
//!
//! Only **compaction** control is exposed today (`compact_now`,
//! `compact_status`, `compact_dry_run`). Retention enforcement, snapshot
//! expiration, and orphan cleanup run as compactor background loops with no
//! `do_action` surface yet; exposing them here needs matching compactor actions
//! (tracked follow-up).

use arrow_flight::Action;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router, extract::State};
use bytes::Bytes;
use common::flight::transport::ServiceCapability;
use futures::StreamExt;
use serde_json::Value;
use tonic::Request;
use tracing::Instrument;

use super::api_error::ApiError;
use crate::RouterState;
use std::time::Duration;

/// Upper bound on a single compactor round-trip.
const OPS_TIMEOUT: Duration = Duration::from_secs(30);

/// The `/api/v1/ops/*` routes. Mounted behind the admin-auth layer.
pub fn router<S: RouterState>() -> Router<S> {
    Router::new()
        .route("/compact", post(compact::<S>))
        .route("/compact/status", get(compact_status::<S>))
        .route("/compact/dry-run", post(compact_dry_run::<S>))
}

/// POST /api/v1/ops/compact — trigger a compaction pass now.
#[utoipa::path(
    post,
    path = "/api/v1/ops/compact",
    operation_id = "ops_compact",
    tag = "ops",
    security(("bearerAuth" = [])),
    responses(
        (status = 200, description = "Compaction run summary", body = serde_json::Value),
        (status = 502, description = "The compactor returned an unusable response"),
        (status = 503, description = "No compactor service is reachable"),
        (status = 504, description = "The compactor did not respond in time"),
    )
)]
pub async fn compact<S: RouterState>(State(state): State<S>) -> Result<Json<Value>, ApiError> {
    Ok(Json(do_compactor_action(&state, "compact_now").await?))
}

/// GET /api/v1/ops/compact/status — active leases and compaction metrics.
#[utoipa::path(
    get,
    path = "/api/v1/ops/compact/status",
    operation_id = "ops_compact_status",
    tag = "ops",
    security(("bearerAuth" = [])),
    responses(
        (status = 200, description = "Compaction status snapshot", body = serde_json::Value),
        (status = 502, description = "The compactor returned an unusable response"),
        (status = 503, description = "No compactor service is reachable"),
        (status = 504, description = "The compactor did not respond in time"),
    )
)]
pub async fn compact_status<S: RouterState>(
    State(state): State<S>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(do_compactor_action(&state, "compact_status").await?))
}

/// POST /api/v1/ops/compact/dry-run — plan compaction candidates without
/// executing (the read-only preview of what `compact` would do).
#[utoipa::path(
    post,
    path = "/api/v1/ops/compact/dry-run",
    operation_id = "ops_compact_dry_run",
    tag = "ops",
    security(("bearerAuth" = [])),
    responses(
        (status = 200, description = "Compaction candidates", body = serde_json::Value),
        (status = 502, description = "The compactor returned an unusable response"),
        (status = 503, description = "No compactor service is reachable"),
        (status = 504, description = "The compactor did not respond in time"),
    )
)]
pub async fn compact_dry_run<S: RouterState>(
    State(state): State<S>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(do_compactor_action(&state, "compact_dry_run").await?))
}

/// Forward a control action to the compactor's Flight `do_action` surface and
/// return its JSON result body verbatim.
async fn do_compactor_action<S: RouterState>(
    state: &S,
    action_type: &str,
) -> Result<Value, ApiError> {
    let (mut client, server_address) = state
        .service_registry()
        .get_flight_client_and_address_for_capability(ServiceCapability::StorageMaintenance)
        .await
        .map_err(|e| {
            // Discovery collapses "nothing registered with the capability" and
            // "registered but unreachable" into one failure. Both surface as
            // 503, so the cause only survives if it is carried out here.
            tracing::warn!(
                error = %e,
                action = action_type,
                "No compactor reachable for StorageMaintenance"
            );
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("no compactor service is reachable: {e}"),
            )
        })?;

    let mut request = Request::new(Action {
        r#type: action_type.to_string(),
        body: Bytes::new(),
    });
    // RPC CLIENT boundary span, named by the fully-qualified Flight method
    // plus the low-cardinality action type, propagated to the compactor's
    // SERVER span via the injected trace context.
    let rpc_span = common::self_monitoring::spans::rpc_client_span(
        common::self_monitoring::spans::FLIGHT_DO_ACTION,
        Some(action_type),
        Some(&server_address),
    );
    rpc_span.in_scope(|| common::flight::trace_context::inject_context_into_request(&mut request));
    let record_span = rpc_span.clone();

    // Bound the round-trip so a hung compactor can't hang the request.
    let result = tokio::time::timeout(
        OPS_TIMEOUT,
        async {
            let mut stream = client
                .do_action(request)
                .await
                .map_err(|s| ApiError::from_flight(&s, action_type))?
                .into_inner();
            let result = stream.next().await.ok_or_else(|| {
                ApiError::new(StatusCode::BAD_GATEWAY, "compactor returned no result")
            })?;
            let result = result.map_err(|s| ApiError::from_flight(&s, action_type))?;
            common::self_monitoring::spans::record_rpc_result(
                &record_span,
                common::self_monitoring::spans::RpcBoundary::Client,
                tonic::Code::Ok,
            );
            Ok::<_, ApiError>(result)
        }
        .instrument(rpc_span),
    )
    .await
    .map_err(|_| {
        ApiError::new(
            StatusCode::GATEWAY_TIMEOUT,
            format!(
                "compactor did not respond within {}s",
                OPS_TIMEOUT.as_secs()
            ),
        )
    })??;

    serde_json::from_slice(&result.body).map_err(|e| {
        ApiError::new(
            StatusCode::BAD_GATEWAY,
            format!("invalid compactor response: {e}"),
        )
    })
}
