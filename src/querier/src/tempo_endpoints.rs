use std::collections::HashMap;

use axum::{
    extract::{Path, Query, State},
    routing::get,
    Router,
};
use datafusion::prelude::SessionContext;

use crate::query::{trace::TraceService, FindTraceByIdParams, TraceQuerier};

#[derive(Clone, Debug)]
struct QuerierState {
    trace_querier: TraceService,
}

pub fn router() -> Router {
    let session_context = SessionContext::new();
    let state = QuerierState {
        trace_querier: TraceService::new(session_context),
    };

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
        .with_state(state)
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
    state: State<QuerierState>,
    Path(trace_id): Path<String>,
    start: Option<Query<String>>,
    end: Option<Query<String>>,
) -> Result<axum::Json<tempo_api::Trace>, axum::http::StatusCode> {
    log::info!("Querying for trace_id: {}", trace_id);

    match state
        .trace_querier
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

mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_search_result() {
        let traces = vec![tempo_api::Trace {
            trace_id: "2f3e0cee77ae5dc9c17ade3689eb2e54".to_string(),
            root_service_name: "shop-backend".to_string(),
            root_trace_name: "GET /api/orders".to_string(),
            start_time_unix_nano: "1684778327699392724".to_string(),
            duration_ms: 557,
            span_sets: vec![tempo_api::SpanSet {
                spans: vec![tempo_api::Span {
                    span_id: "563d623c76514f8e".to_string(),
                    start_time_unix_nano: "1684778327699392724".to_string(),
                    duration_nanos: "1234".to_string(),
                    attributes: HashMap::new(),
                }],
                matched: 123,
            }],
        }];

        let search_result = tempo_api::SearchResult {
            traces,
            metrics: HashMap::new(),
        };

        let json = serde_json::to_string(&search_result).unwrap();
        let deserialized: tempo_api::SearchResult = serde_json::from_str(&json).unwrap();

        assert_eq!(search_result, deserialized);
    }
}
