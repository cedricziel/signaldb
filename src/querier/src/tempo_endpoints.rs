use std::collections::HashMap;

use axum::{
    extract::{Path, Query},
    routing::get,
    Router,
};

use crate::query::{trace::TraceService, FindTraceByIdParams, TraceQuerier};

pub fn router() -> Router {
    Router::new()
        .route("/api/echo", get(echo))
        .route("/api/traces/:trace_id", get(query_single_trace))
        .route("/api/search", get(search))
        .route("/api/search/tags", get(search_tags))
        .route("/api/search/tag/:tag_name/values", get(search_tag_values))
        // v2 routes
        .route("/api/v2/search/tags", get(search_tags_v2))
        .route(
            "/api/v2/search/tag/:tag_name/values",
            get(search_tag_values_v2),
        )
}

/// GET /api/echo
///
/// See https://grafana.com/docs/tempo/latest/api_docs/#query-echo-endpoint
#[tracing::instrument]
pub async fn echo() -> &'static str {
    log::info!("Echoing back the request");

    "echo"
}

/// GET /api/traces/<traceid>?start=<start>&end=<end>
///
/// See https://grafana.com/docs/tempo/latest/api_docs/#query
#[tracing::instrument]
pub async fn query_single_trace(
    Path(trace_id): Path<String>,
    start: Option<Query<String>>,
    end: Option<Query<String>>,
) -> Result<axum::Json<tempo_api::Trace>, axum::http::StatusCode> {
    log::info!("Querying for trace_id: {}", trace_id);

    let trace_service = TraceService::new();

    match trace_service
        .find_by_id(FindTraceByIdParams {
            trace_id,
            start: start.map(|q| q.0),
            end: end.map(|q| q.0),
        })
        .await
    {
        Ok(trace) => match trace {
            Some(trace) => Ok(axum::Json(trace.into())),
            None => return Err(axum::http::StatusCode::NOT_FOUND),
        },
        Err(_) => return Err(axum::http::StatusCode::NOT_FOUND),
    }
}

/// GET https://grafana.com/docs/tempo/latest/api_docs/#search
#[tracing::instrument]
pub async fn search(
    Query(query): Query<tempo_api::SearchQueryParams>,
) -> Result<axum::Json<tempo_api::SearchResult>, axum::http::StatusCode> {
    let response = tempo_api::SearchResult {
        traces: vec![],
        metrics: HashMap::new(),
    };

    Ok(axum::Json(response))
}

/// GET /api/search/tags?scope=<resource|span|intrinsic>
///
/// See https://grafana.com/docs/tempo/latest/api_docs/#search-tags
#[tracing::instrument]
pub async fn search_tags(
) -> Result<axum::Json<tempo_api::TagSearchResponse>, axum::http::StatusCode> {
    let response = tempo_api::TagSearchResponse { tag_names: vec![] };

    Ok(axum::Json(response))
}

/// GET /api/search/tag/:tag_name/values
///
/// See https://grafana.com/docs/tempo/latest/api_docs/#search-tag-values
#[tracing::instrument]
pub async fn search_tag_values(
    Path(_tag_name): Path<String>,
    Query(start): Query<i32>,
    Query(end): Query<i32>,
) -> Result<axum::Json<tempo_api::TagValuesResponse>, axum::http::StatusCode> {
    let response = tempo_api::TagValuesResponse { tag_values: vec![] };

    Ok(axum::Json(response))
}

/// GET /api/v2/search/tags?scope=<resource|span|intrinsic>
#[tracing::instrument]
pub async fn search_tags_v2(
    _scope: Option<Query<tempo_api::TagScope>>,
) -> Result<axum::Json<tempo_api::v2::TagSearchResponse>, axum::http::StatusCode> {
    let response = tempo_api::v2::TagSearchResponse {
        scopes: vec![tempo_api::v2::TagSearchScope {
            scope: "resource".to_string(),
            tags: vec!["service.name".to_string()],
        }],
    };

    Ok(axum::Json(response))
}

/// GET /api/v2/search/tag/.service.name/values
#[tracing::instrument]
pub async fn search_tag_values_v2(
    scoped_tag: Path<String>,
    start: Option<Query<i32>>,
    end: Option<Query<i32>>,
    q: Option<Query<String>>,
) -> Result<axum::Json<tempo_api::v2::TagValuesResponse>, axum::http::StatusCode> {
    let response = tempo_api::v2::TagValuesResponse {
        tag_values: vec![
            tempo_api::v2::TagWithValue {
                tag: "service.name".to_string(),
                value: "shop-backend".to_string(),
            },
            tempo_api::v2::TagWithValue {
                tag: "service.name".to_string(),
                value: "shop-frontend".to_string(),
            },
        ],
    };

    Ok(axum::Json(response))
}
