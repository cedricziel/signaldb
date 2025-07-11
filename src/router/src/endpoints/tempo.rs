use crate::RouterState;
use arrow_flight::Ticket;
use axum::{
    Router,
    extract::{Path, Query, State},
    routing::get,
};
use common::flight::transport::ServiceCapability;
use futures::StreamExt;
use std::collections::HashMap;
use tempo_api::{self, TraceQueryParams};

pub fn router<S: RouterState>() -> Router<S> {
    Router::new()
        .route("/api/echo", get(echo))
        .route("/api/traces/:trace_id", get(query_single_trace::<S>))
        .route("/api/search", get(search::<S>))
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
    state: State<S>,
    Path(trace_id): Path<String>,
    Query(params): Query<TraceQueryParams>,
) -> Result<axum::Json<tempo_api::Trace>, axum::http::StatusCode> {
    log::info!("Querying for trace_id: {trace_id}");

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

    // Create Flight query for trace lookup
    let ticket = Ticket::new(format!("find_trace:{trace_id}"));

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
            if !trace_data.is_empty() {
                log::info!("Successfully queried trace {trace_id} from querier service");
                // TODO: Convert Arrow data to Tempo trace format
                // For now, return a trace indicating data was found
            } else {
                log::info!("No trace data found for trace {trace_id}");
            }
        }
        Err(e) => {
            log::error!("Flight query failed for trace {trace_id}: {e}");
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    // Create a mock trace
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
#[tracing::instrument]
pub async fn search<S: RouterState>(
    state: State<S>,
    Query(query): Query<tempo_api::SearchQueryParams>,
) -> Result<axum::Json<tempo_api::SearchResult>, axum::http::StatusCode> {
    log::info!("Searching for traces with params: {query:?}");

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

    // Create Flight query for trace search
    let search_params = serde_json::to_string(&query).map_err(|e| {
        log::error!("Failed to serialize search parameters: {e}");
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let ticket = Ticket::new(format!("search_traces:{search_params}"));

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

            if !search_results.is_empty() {
                log::info!("Successfully executed trace search via Flight protocol");
                // TODO: Convert Arrow data to Tempo search result format
            } else {
                log::info!("No search results found");
            }
        }
        Err(e) => {
            log::error!("Flight search query failed: {e}");
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    // In a real implementation, you would:
    // 1. Subscribe to the arrow-traces topic
    // 2. Filter traces based on query parameters
    // 3. Convert Arrow to OTLP using arrow_to_otlp_traces
    // 4. Convert OTLP to Tempo API format

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

        let result = search(State(state), Query(query)).await.unwrap();
        assert_eq!(result.0.traces.len(), 0);
        assert_eq!(result.0.metrics.len(), 0);
    }
}
