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

use crate::RouterState;
use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
};
use common::auth::TenantContextExtractor;
use loki_api::{LabelsResponse, QueryResponse, QueryResult, SeriesResponse};
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
#[tracing::instrument(
    skip(_state, tenant_ctx, params),
    fields(
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn query<S: RouterState>(
    State(_state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<InstantQueryParams>,
) -> Result<axum::Json<QueryResponse>, StatusCode> {
    let logql = require_query(&params.query)?;
    validate_direction(&params.direction)?;
    tracing::debug!(query = %logql, "LogQL instant query (stub)");

    // Instant queries over log selectors return streams; execution is
    // wired up in #376. Until then, answer with an empty stream set.
    Ok(axum::Json(QueryResponse::success(QueryResult::Streams(
        vec![],
    ))))
}

/// GET /loki/api/v1/query_range — range query.
#[tracing::instrument(
    skip(_state, tenant_ctx, params),
    fields(
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn query_range<S: RouterState>(
    State(_state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(params): Query<RangeQueryParams>,
) -> Result<axum::Json<QueryResponse>, StatusCode> {
    let logql = require_query(&params.query)?;
    validate_direction(&params.direction)?;
    tracing::debug!(query = %logql, "LogQL range query (stub)");

    Ok(axum::Json(QueryResponse::success(QueryResult::Streams(
        vec![],
    ))))
}

/// GET /loki/api/v1/labels — list label names.
#[tracing::instrument(
    skip(_state, tenant_ctx, _params),
    fields(
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn labels<S: RouterState>(
    State(_state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(_params): Query<MetadataParams>,
) -> Result<axum::Json<LabelsResponse>, StatusCode> {
    Ok(axum::Json(LabelsResponse::success(vec![])))
}

/// GET /loki/api/v1/label/{name}/values — list values of one label.
#[tracing::instrument(
    skip(_state, tenant_ctx, _params),
    fields(
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id,
        label = %name
    )
)]
pub async fn label_values<S: RouterState>(
    State(_state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Path(name): Path<String>,
    Query(_params): Query<MetadataParams>,
) -> Result<axum::Json<LabelsResponse>, StatusCode> {
    if name.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(axum::Json(LabelsResponse::success(vec![])))
}

/// GET /loki/api/v1/series — list series matching a selector.
#[tracing::instrument(
    skip(_state, tenant_ctx, _params),
    fields(
        tenant_id = %tenant_ctx.0.tenant_id,
        dataset_id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn series<S: RouterState>(
    State(_state): State<S>,
    tenant_ctx: TenantContextExtractor,
    Query(_params): Query<MetadataParams>,
) -> Result<axum::Json<SeriesResponse>, StatusCode> {
    Ok(axum::Json(SeriesResponse::success(vec![])))
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
    async fn query_range_returns_empty_streams_stub() {
        let app = test_app().await;
        let (status, body) = get_json(
            &app,
            "/loki/api/v1/query_range?query=%7Bservice_name%3D%22api%22%7D&limit=50",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body.unwrap(),
            serde_json::json!({
                "status": "success",
                "data": {"resultType": "streams", "result": []}
            })
        );
    }

    #[tokio::test]
    async fn instant_query_returns_empty_streams_stub() {
        let app = test_app().await;
        let (status, body) = get_json(&app, "/loki/api/v1/query?query=%7Bjob%3D%22x%22%7D").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.unwrap()["data"]["resultType"], "streams");
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
    async fn metadata_endpoints_return_empty_stubs() {
        let app = test_app().await;

        let (status, body) = get_json(&app, "/loki/api/v1/labels").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body.unwrap(),
            serde_json::json!({"status": "success", "data": []})
        );

        let (status, body) = get_json(&app, "/loki/api/v1/label/service_name/values").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.unwrap()["status"], "success");

        let (status, body) =
            get_json(&app, "/loki/api/v1/series?match%5B%5D=%7Bjob%3D%22x%22%7D").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body.unwrap(),
            serde_json::json!({"status": "success", "data": []})
        );
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
}
