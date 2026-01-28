use crate::RouterState;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use common::auth::Authenticator;
use signaldb_api::{
    ApiError, ApiKeyResponse, CreateApiKeyRequest, CreateApiKeyResponse, CreateDatasetRequest,
    CreateTenantRequest, DatasetResponse, ListApiKeysResponse, ListDatasetsResponse,
    ListTenantsResponse, TenantResponse, UpdateTenantRequest,
};
use uuid::Uuid;

// ── Tenant endpoints ────────────────────────────────────────────────────

/// List all tenants
#[utoipa::path(
    get,
    path = "/tenants",
    tag = "tenants",
    responses(
        (status = 200, description = "List of tenants", body = ListTenantsResponse),
    )
)]
pub async fn list_tenants<S: RouterState>(state: State<S>) -> impl IntoResponse {
    match state.catalog().list_tenants().await {
        Ok(tenants) => {
            let response = ListTenantsResponse {
                tenants: tenants.into_iter().map(tenant_record_to_response).collect(),
            };
            (
                StatusCode::OK,
                Json(serde_json::to_value(response).unwrap()),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
        )
            .into_response(),
    }
}

/// Create a new tenant
#[utoipa::path(
    post,
    path = "/tenants",
    tag = "tenants",
    request_body = CreateTenantRequest,
    responses(
        (status = 201, description = "Tenant created", body = TenantResponse),
        (status = 400, description = "Validation error", body = ApiError),
        (status = 409, description = "Tenant already exists", body = ApiError),
    )
)]
pub async fn create_tenant<S: RouterState>(
    state: State<S>,
    Json(request): Json<CreateTenantRequest>,
) -> impl IntoResponse {
    if request.id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                serde_json::to_value(ApiError::new(
                    "validation_error",
                    "Tenant ID must not be empty",
                ))
                .unwrap(),
            ),
        )
            .into_response();
    }

    if request.name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                serde_json::to_value(ApiError::new(
                    "validation_error",
                    "Tenant name must not be empty",
                ))
                .unwrap(),
            ),
        )
            .into_response();
    }

    // Check if tenant already exists
    match state.catalog().get_tenant(&request.id).await {
        Ok(Some(_)) => {
            return (
                StatusCode::CONFLICT,
                Json(
                    serde_json::to_value(ApiError::new(
                        "conflict",
                        format!("Tenant '{}' already exists", request.id),
                    ))
                    .unwrap(),
                ),
            )
                .into_response();
        }
        Ok(None) => {}
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
            )
                .into_response();
        }
    }

    // Create tenant in database
    if let Err(e) = state
        .catalog()
        .upsert_tenant(
            &request.id,
            &request.name,
            request.default_dataset.as_deref(),
            "database",
        )
        .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
        )
            .into_response();
    }

    // Fetch back the created tenant to get timestamps
    match state.catalog().get_tenant(&request.id).await {
        Ok(Some(record)) => (
            StatusCode::CREATED,
            Json(serde_json::to_value(tenant_record_to_response(record)).unwrap()),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(
                serde_json::to_value(ApiError::new(
                    "internal_error",
                    "Tenant created but not found",
                ))
                .unwrap(),
            ),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
        )
            .into_response(),
    }
}

/// Get a tenant by ID
#[utoipa::path(
    get,
    path = "/tenants/{tenant_id}",
    tag = "tenants",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier")
    ),
    responses(
        (status = 200, description = "Tenant details", body = TenantResponse),
        (status = 404, description = "Tenant not found", body = ApiError),
    )
)]
pub async fn get_tenant<S: RouterState>(
    state: State<S>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    match state.catalog().get_tenant(&tenant_id).await {
        Ok(Some(record)) => (
            StatusCode::OK,
            Json(serde_json::to_value(tenant_record_to_response(record)).unwrap()),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(
                serde_json::to_value(ApiError::new(
                    "not_found",
                    format!("Tenant '{tenant_id}' not found"),
                ))
                .unwrap(),
            ),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
        )
            .into_response(),
    }
}

/// Update a tenant
#[utoipa::path(
    put,
    path = "/tenants/{tenant_id}",
    tag = "tenants",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier")
    ),
    request_body = UpdateTenantRequest,
    responses(
        (status = 200, description = "Tenant updated", body = TenantResponse),
        (status = 403, description = "Config-sourced tenants cannot be modified", body = ApiError),
        (status = 404, description = "Tenant not found", body = ApiError),
    )
)]
pub async fn update_tenant<S: RouterState>(
    state: State<S>,
    Path(tenant_id): Path<String>,
    Json(request): Json<UpdateTenantRequest>,
) -> impl IntoResponse {
    // Get existing tenant
    let existing = match state.catalog().get_tenant(&tenant_id).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(
                    serde_json::to_value(ApiError::new(
                        "not_found",
                        format!("Tenant '{tenant_id}' not found"),
                    ))
                    .unwrap(),
                ),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
            )
                .into_response();
        }
    };

    // Protect config-sourced tenants
    if existing.source == "config" {
        return (
            StatusCode::FORBIDDEN,
            Json(
                serde_json::to_value(ApiError::new(
                    "forbidden",
                    "Config-sourced tenants cannot be modified via API",
                ))
                .unwrap(),
            ),
        )
            .into_response();
    }

    // Merge updates
    let name = request.name.as_deref().unwrap_or(&existing.name);
    let default_dataset = match &request.default_dataset {
        Some(ds) => Some(ds.as_str()),
        None => existing.default_dataset.as_deref(),
    };

    if let Err(e) = state
        .catalog()
        .upsert_tenant(&tenant_id, name, default_dataset, "database")
        .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
        )
            .into_response();
    }

    match state.catalog().get_tenant(&tenant_id).await {
        Ok(Some(record)) => (
            StatusCode::OK,
            Json(serde_json::to_value(tenant_record_to_response(record)).unwrap()),
        )
            .into_response(),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(
                serde_json::to_value(ApiError::new(
                    "internal_error",
                    "Tenant updated but not found",
                ))
                .unwrap(),
            ),
        )
            .into_response(),
    }
}

/// Delete a tenant
#[utoipa::path(
    delete,
    path = "/tenants/{tenant_id}",
    tag = "tenants",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier")
    ),
    responses(
        (status = 204, description = "Tenant deleted"),
        (status = 403, description = "Config-sourced tenants cannot be deleted", body = ApiError),
        (status = 404, description = "Tenant not found", body = ApiError),
    )
)]
pub async fn delete_tenant<S: RouterState>(
    state: State<S>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    // Check tenant exists and source
    match state.catalog().get_tenant(&tenant_id).await {
        Ok(Some(record)) => {
            if record.source == "config" {
                return (
                    StatusCode::FORBIDDEN,
                    Json(
                        serde_json::to_value(ApiError::new(
                            "forbidden",
                            "Config-sourced tenants cannot be deleted via API",
                        ))
                        .unwrap(),
                    ),
                )
                    .into_response();
            }
        }
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(
                    serde_json::to_value(ApiError::new(
                        "not_found",
                        format!("Tenant '{tenant_id}' not found"),
                    ))
                    .unwrap(),
                ),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
            )
                .into_response();
        }
    }

    match state.catalog().delete_tenant(&tenant_id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(
                serde_json::to_value(ApiError::new(
                    "not_found",
                    format!("Tenant '{tenant_id}' not found or is config-sourced"),
                ))
                .unwrap(),
            ),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
        )
            .into_response(),
    }
}

// ── API Key endpoints ───────────────────────────────────────────────────

/// List API keys for a tenant
#[utoipa::path(
    get,
    path = "/tenants/{tenant_id}/api-keys",
    tag = "api-keys",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier")
    ),
    responses(
        (status = 200, description = "List of API keys", body = ListApiKeysResponse),
        (status = 404, description = "Tenant not found", body = ApiError),
    )
)]
pub async fn list_api_keys<S: RouterState>(
    state: State<S>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    // Verify tenant exists
    match state.catalog().get_tenant(&tenant_id).await {
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(
                    serde_json::to_value(ApiError::new(
                        "not_found",
                        format!("Tenant '{tenant_id}' not found"),
                    ))
                    .unwrap(),
                ),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
            )
                .into_response();
        }
        Ok(Some(_)) => {}
    }

    match state.catalog().list_api_keys(&tenant_id).await {
        Ok(keys) => {
            let response = ListApiKeysResponse {
                api_keys: keys
                    .into_iter()
                    .map(|k| ApiKeyResponse {
                        id: k.id,
                        name: k.name,
                        created_at: k.created_at.to_rfc3339(),
                        revoked_at: k.revoked_at.map(|t| t.to_rfc3339()),
                    })
                    .collect(),
            };
            (
                StatusCode::OK,
                Json(serde_json::to_value(response).unwrap()),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
        )
            .into_response(),
    }
}

/// Create a new API key for a tenant
#[utoipa::path(
    post,
    path = "/tenants/{tenant_id}/api-keys",
    tag = "api-keys",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier")
    ),
    request_body = CreateApiKeyRequest,
    responses(
        (status = 201, description = "API key created (raw key shown once)", body = CreateApiKeyResponse),
        (status = 404, description = "Tenant not found", body = ApiError),
    )
)]
pub async fn create_api_key<S: RouterState>(
    state: State<S>,
    Path(tenant_id): Path<String>,
    Json(request): Json<CreateApiKeyRequest>,
) -> impl IntoResponse {
    // Verify tenant exists
    match state.catalog().get_tenant(&tenant_id).await {
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(
                    serde_json::to_value(ApiError::new(
                        "not_found",
                        format!("Tenant '{tenant_id}' not found"),
                    ))
                    .unwrap(),
                ),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
            )
                .into_response();
        }
        Ok(Some(_)) => {}
    }

    // Generate a new raw API key
    let raw_key = format!("sk-{}-{}", tenant_id, Uuid::new_v4());
    let key_hash = Authenticator::hash_api_key(&raw_key);

    match state
        .catalog()
        .upsert_api_key(&tenant_id, &key_hash, request.name.as_deref())
        .await
    {
        Ok(key_id) => {
            // Fetch the record for timestamps
            let created_at = match state.catalog().get_api_key(&key_id).await {
                Ok(Some(record)) => record.created_at.to_rfc3339(),
                _ => chrono::Utc::now().to_rfc3339(),
            };

            let response = CreateApiKeyResponse {
                id: key_id,
                key: raw_key,
                name: request.name,
                created_at,
            };
            (
                StatusCode::CREATED,
                Json(serde_json::to_value(response).unwrap()),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
        )
            .into_response(),
    }
}

/// Revoke an API key
#[utoipa::path(
    delete,
    path = "/tenants/{tenant_id}/api-keys/{key_id}",
    tag = "api-keys",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("key_id" = String, Path, description = "API key identifier"),
    ),
    responses(
        (status = 204, description = "API key revoked"),
        (status = 404, description = "API key not found", body = ApiError),
    )
)]
pub async fn revoke_api_key<S: RouterState>(
    state: State<S>,
    Path((tenant_id, key_id)): Path<(String, String)>,
) -> impl IntoResponse {
    // Verify the key exists and belongs to this tenant
    match state.catalog().get_api_key(&key_id).await {
        Ok(Some(record)) => {
            if record.tenant_id != tenant_id {
                return (
                    StatusCode::NOT_FOUND,
                    Json(
                        serde_json::to_value(ApiError::new(
                            "not_found",
                            format!("API key '{key_id}' not found for tenant '{tenant_id}'"),
                        ))
                        .unwrap(),
                    ),
                )
                    .into_response();
            }
        }
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(
                    serde_json::to_value(ApiError::new(
                        "not_found",
                        format!("API key '{key_id}' not found"),
                    ))
                    .unwrap(),
                ),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
            )
                .into_response();
        }
    }

    match state.catalog().revoke_api_key(&key_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
        )
            .into_response(),
    }
}

// ── Dataset endpoints ───────────────────────────────────────────────────

/// List datasets for a tenant
#[utoipa::path(
    get,
    path = "/tenants/{tenant_id}/datasets",
    tag = "datasets",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier")
    ),
    responses(
        (status = 200, description = "List of datasets", body = ListDatasetsResponse),
        (status = 404, description = "Tenant not found", body = ApiError),
    )
)]
pub async fn list_datasets<S: RouterState>(
    state: State<S>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    // Verify tenant exists
    match state.catalog().get_tenant(&tenant_id).await {
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(
                    serde_json::to_value(ApiError::new(
                        "not_found",
                        format!("Tenant '{tenant_id}' not found"),
                    ))
                    .unwrap(),
                ),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
            )
                .into_response();
        }
        Ok(Some(_)) => {}
    }

    match state.catalog().get_datasets(&tenant_id).await {
        Ok(datasets) => {
            let response = ListDatasetsResponse {
                datasets: datasets
                    .into_iter()
                    .map(|d| DatasetResponse {
                        id: d.id,
                        name: d.name,
                        tenant_id: d.tenant_id,
                        created_at: d.created_at.to_rfc3339(),
                    })
                    .collect(),
            };
            (
                StatusCode::OK,
                Json(serde_json::to_value(response).unwrap()),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
        )
            .into_response(),
    }
}

/// Create a new dataset for a tenant
#[utoipa::path(
    post,
    path = "/tenants/{tenant_id}/datasets",
    tag = "datasets",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier")
    ),
    request_body = CreateDatasetRequest,
    responses(
        (status = 201, description = "Dataset created", body = DatasetResponse),
        (status = 404, description = "Tenant not found", body = ApiError),
    )
)]
pub async fn create_dataset<S: RouterState>(
    state: State<S>,
    Path(tenant_id): Path<String>,
    Json(request): Json<CreateDatasetRequest>,
) -> impl IntoResponse {
    if request.name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                serde_json::to_value(ApiError::new(
                    "validation_error",
                    "Dataset name must not be empty",
                ))
                .unwrap(),
            ),
        )
            .into_response();
    }

    // Verify tenant exists
    match state.catalog().get_tenant(&tenant_id).await {
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(
                    serde_json::to_value(ApiError::new(
                        "not_found",
                        format!("Tenant '{tenant_id}' not found"),
                    ))
                    .unwrap(),
                ),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
            )
                .into_response();
        }
        Ok(Some(_)) => {}
    }

    match state
        .catalog()
        .create_dataset(&tenant_id, &request.name)
        .await
    {
        Ok(dataset_id) => {
            let response = DatasetResponse {
                id: dataset_id,
                name: request.name,
                tenant_id,
                created_at: chrono::Utc::now().to_rfc3339(),
            };
            (
                StatusCode::CREATED,
                Json(serde_json::to_value(response).unwrap()),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
        )
            .into_response(),
    }
}

/// Delete a dataset
#[utoipa::path(
    delete,
    path = "/tenants/{tenant_id}/datasets/{dataset_id}",
    tag = "datasets",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("dataset_id" = String, Path, description = "Dataset identifier"),
    ),
    responses(
        (status = 204, description = "Dataset deleted"),
        (status = 404, description = "Dataset not found", body = ApiError),
    )
)]
pub async fn delete_dataset<S: RouterState>(
    state: State<S>,
    Path((_tenant_id, dataset_id)): Path<(String, String)>,
) -> impl IntoResponse {
    match state.catalog().delete_dataset(&dataset_id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(
                serde_json::to_value(ApiError::new(
                    "not_found",
                    format!("Dataset '{dataset_id}' not found"),
                ))
                .unwrap(),
            ),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::to_value(ApiError::new("internal_error", e.to_string())).unwrap()),
        )
            .into_response(),
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn tenant_record_to_response(record: common::catalog::TenantRecord) -> TenantResponse {
    TenantResponse {
        id: record.id,
        name: record.name,
        default_dataset: record.default_dataset,
        source: record.source,
        created_at: record.created_at.to_rfc3339(),
        updated_at: record.updated_at.to_rfc3339(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryStateImpl;
    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::{delete, get, post, put},
    };
    use common::catalog::Catalog;
    use common::config::Configuration;
    use tower::ServiceExt;

    async fn create_admin_test_state() -> InMemoryStateImpl {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let config = Configuration::default();
        InMemoryStateImpl::new(catalog, config)
    }

    fn admin_router(state: InMemoryStateImpl) -> Router {
        Router::new()
            .route("/tenants", get(list_tenants::<InMemoryStateImpl>))
            .route("/tenants", post(create_tenant::<InMemoryStateImpl>))
            .route("/tenants/{tenant_id}", get(get_tenant::<InMemoryStateImpl>))
            .route(
                "/tenants/{tenant_id}",
                put(update_tenant::<InMemoryStateImpl>),
            )
            .route(
                "/tenants/{tenant_id}",
                delete(delete_tenant::<InMemoryStateImpl>),
            )
            .route(
                "/tenants/{tenant_id}/api-keys",
                get(list_api_keys::<InMemoryStateImpl>),
            )
            .route(
                "/tenants/{tenant_id}/api-keys",
                post(create_api_key::<InMemoryStateImpl>),
            )
            .route(
                "/tenants/{tenant_id}/api-keys/{key_id}",
                delete(revoke_api_key::<InMemoryStateImpl>),
            )
            .route(
                "/tenants/{tenant_id}/datasets",
                get(list_datasets::<InMemoryStateImpl>),
            )
            .route(
                "/tenants/{tenant_id}/datasets",
                post(create_dataset::<InMemoryStateImpl>),
            )
            .route(
                "/tenants/{tenant_id}/datasets/{dataset_id}",
                delete(delete_dataset::<InMemoryStateImpl>),
            )
            .with_state(state)
    }

    #[tokio::test]
    async fn test_tenant_crud_lifecycle() {
        let state = create_admin_test_state().await;
        let app = admin_router(state);

        // Create tenant
        let request = Request::builder()
            .method("POST")
            .uri("/tenants")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"id": "test", "name": "Test Corp"}"#))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        // Get tenant
        let request = Request::builder()
            .uri("/tenants/test")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // List tenants
        let request = Request::builder()
            .uri("/tenants")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Update tenant
        let request = Request::builder()
            .method("PUT")
            .uri("/tenants/test")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"name": "Updated Corp"}"#))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Delete tenant
        let request = Request::builder()
            .method("DELETE")
            .uri("/tenants/test")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Verify gone
        let request = Request::builder()
            .uri("/tenants/test")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_duplicate_tenant_creation() {
        let state = create_admin_test_state().await;
        let app = admin_router(state);

        // Create tenant
        let request = Request::builder()
            .method("POST")
            .uri("/tenants")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"id": "dup", "name": "Duplicate"}"#))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        // Attempt duplicate
        let request = Request::builder()
            .method("POST")
            .uri("/tenants")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"id": "dup", "name": "Duplicate Again"}"#))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_config_sourced_tenant_protection() {
        let state = create_admin_test_state().await;

        // Insert a config-sourced tenant directly
        state
            .catalog()
            .upsert_tenant("config-tenant", "Config Tenant", None, "config")
            .await
            .unwrap();

        let app = admin_router(state);

        // Try to update config-sourced tenant
        let request = Request::builder()
            .method("PUT")
            .uri("/tenants/config-tenant")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"name": "Hacked"}"#))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        // Try to delete config-sourced tenant
        let request = Request::builder()
            .method("DELETE")
            .uri("/tenants/config-tenant")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_api_key_lifecycle() {
        let state = create_admin_test_state().await;

        // Create tenant first
        state
            .catalog()
            .upsert_tenant("acme", "Acme Corp", None, "database")
            .await
            .unwrap();

        let app = admin_router(state);

        // Create API key
        let request = Request::builder()
            .method("POST")
            .uri("/tenants/acme/api-keys")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"name": "Production Key"}"#))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let created: CreateApiKeyResponse = serde_json::from_slice(&body).unwrap();
        assert!(created.key.starts_with("sk-acme-"));
        assert_eq!(created.name, Some("Production Key".to_string()));

        // List API keys
        let request = Request::builder()
            .uri("/tenants/acme/api-keys")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let list: ListApiKeysResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(list.api_keys.len(), 1);

        // Revoke API key
        let key_id = &created.id;
        let request = Request::builder()
            .method("DELETE")
            .uri(format!("/tenants/acme/api-keys/{key_id}"))
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_dataset_lifecycle() {
        let state = create_admin_test_state().await;

        // Create tenant first
        state
            .catalog()
            .upsert_tenant("acme", "Acme Corp", None, "database")
            .await
            .unwrap();

        let app = admin_router(state);

        // Create dataset
        let request = Request::builder()
            .method("POST")
            .uri("/tenants/acme/datasets")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"name": "production"}"#))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let created: DatasetResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(created.name, "production");

        // List datasets
        let request = Request::builder()
            .uri("/tenants/acme/datasets")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Delete dataset
        let dataset_id = &created.id;
        let request = Request::builder()
            .method("DELETE")
            .uri(format!("/tenants/acme/datasets/{dataset_id}"))
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_validation_errors() {
        let state = create_admin_test_state().await;
        let app = admin_router(state);

        // Empty tenant ID
        let request = Request::builder()
            .method("POST")
            .uri("/tenants")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"id": "", "name": "Test"}"#))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Empty tenant name
        let request = Request::builder()
            .method("POST")
            .uri("/tenants")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"id": "test", "name": ""}"#))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_api_key_for_nonexistent_tenant() {
        let state = create_admin_test_state().await;
        let app = admin_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/tenants/ghost/api-keys")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"name": "Key"}"#))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
