use crate::RouterState;
use axum::{
    extract::{Path, Query, State},
    routing::get,
    Router,
};
use std::collections::HashMap;
use tempo_api;

pub fn router<S: RouterState>() -> Router<S> {
    Router::new()
        .route("/api/echo", get(echo))
        .route("/api/traces/:trace_id", get(query_single_trace::<S>))
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
    "echo"
}

/// GET /api/traces/<traceid>?start=<start>&end=<end>
///
/// See https://grafana.com/docs/tempo/latest/api_docs/#query
#[tracing::instrument]
pub async fn query_single_trace<S: RouterState>(
    _state: State<S>,
    Path(trace_id): Path<String>,
    _start: Option<Query<String>>,
    _end: Option<Query<String>>,
) -> Result<axum::Json<tempo_api::Trace>, axum::http::StatusCode> {
    log::info!("Querying for trace_id: {}", trace_id);
    Err(axum::http::StatusCode::NOT_IMPLEMENTED)
}

/// GET https://grafana.com/docs/tempo/latest/api_docs/#search
#[tracing::instrument]
pub async fn search(
    Query(_query): Query<tempo_api::SearchQueryParams>,
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
    _scope: Option<Query<tempo_api::TagScope>>,
) -> Result<axum::Json<tempo_api::v2::TagSearchResponse>, axum::http::StatusCode> {
    let response = tempo_api::v2::TagSearchResponse { scopes: vec![] };
    Ok(axum::Json(response))
}

/// GET /api/v2/search/tag/:tag_name/values
#[tracing::instrument]
pub async fn search_tag_values_v2(
    Path(_scoped_tag): Path<String>,
    _start: Option<Query<i32>>,
    _end: Option<Query<i32>>,
    _q: Option<Query<String>>,
) -> Result<axum::Json<tempo_api::v2::TagValuesResponse>, axum::http::StatusCode> {
    let response = tempo_api::v2::TagValuesResponse { tag_values: vec![] };
    Ok(axum::Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_search_result() {
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

        let result = search(Query(query)).await.unwrap();
        assert_eq!(result.0.traces.len(), 0);
        assert_eq!(result.0.metrics.len(), 0);
    }
}
