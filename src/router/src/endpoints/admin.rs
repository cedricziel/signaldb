use crate::RouterState;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use common::auth::{Authenticator, hash_password, validate_scopes};
use common::catalog::MembershipRole;
use signaldb_api::{
    ApiError, ApiKeyResponse, CreateApiKeyRequest, CreateApiKeyResponse, CreateDatasetRequest,
    CreateTenantRequest, CreateUserRequest, DatasetResponse, ListApiKeysResponse,
    ListDatasetsResponse, ListTenantsResponse, TenantResponse, UpdateApiKeyRequest,
    UpdateTenantRequest, UserResponse,
};
use std::str::FromStr;
use uuid::Uuid;

/// Build a `429` for an exhausted per-tenant count quota (`max_api_keys`,
/// `max_datasets`): the existing `quota_exceeded` JSON body plus the
/// `Retry-After` / `X-RateLimit-Limit` headers every SignalDB rate-limit
/// rejection carries (see `common::ratelimit`). A count quota has no token
/// bucket to refill from, so `Retry-After` is a fixed floor of 1 second —
/// enough to tell the client "wait and retry", which is the header's whole
/// job here; there is no burst allowance to report.
fn quota_exceeded_response(limit: u32, message: String) -> axum::response::Response {
    common::self_monitoring::record_rate_limit_rejection("admin", "quota");
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        Json(serde_json::to_value(ApiError::new("quota_exceeded", message)).unwrap()),
    )
        .into_response();
    let headers = response.headers_mut();
    headers.insert(
        axum::http::header::RETRY_AFTER,
        axum::http::HeaderValue::from(1u64),
    );
    headers.insert(
        axum::http::HeaderName::from_static("x-ratelimit-limit"),
        axum::http::HeaderValue::from(u64::from(limit)),
    );
    response
}

// ── Tenant endpoints ────────────────────────────────────────────────────

/// List all tenants
#[utoipa::path(
    get,
    path = "/api/v1/admin/tenants",
    tag = "tenants",
    security(("bearerAuth" = [])),
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
    path = "/api/v1/admin/tenants",
    tag = "tenants",
    security(("bearerAuth" = [])),
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

    // Create the tenant and materialize its default dataset together. Without
    // a dataset row the tenant cannot authenticate — `resolve_database_tenant`
    // requires one and fails closed — and it is invisible to everything that
    // enumerates dataset rows (issue #1066). One transaction, so a failure
    // cannot leave a tenant that a retry could not repair: creation rejects an
    // existing id with 409.
    if let Err(e) = state
        .catalog()
        .upsert_tenant_with_default_dataset(
            &request.id,
            &request.name,
            request.default_dataset.as_deref(),
            "database",
        )
        .await
    {
        tracing::error!(
            tenant_id = %request.id,
            default_dataset = ?request.default_dataset,
            error = %e,
            "Tenant creation failed"
        );
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
    path = "/api/v1/admin/tenants/{tenant_id}",
    tag = "tenants",
    security(("bearerAuth" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    responses(
        (status = 200, description = "Tenant found", body = TenantResponse),
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
    path = "/api/v1/admin/tenants/{tenant_id}",
    tag = "tenants",
    security(("bearerAuth" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
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

    // Validate: reject explicit empty string for name
    if request.name.as_deref() == Some("") {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                serde_json::to_value(ApiError::new(
                    "validation_error",
                    "Tenant name cannot be empty",
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

    // Repointing the default at a dataset with no row would recreate the state
    // creation avoids, so materialize it in the same transaction. The previous
    // default is deliberately left in place — it may hold data.
    if let Err(e) = state
        .catalog()
        .upsert_tenant_with_default_dataset(&tenant_id, name, default_dataset, "database")
        .await
    {
        tracing::error!(
            tenant_id = %tenant_id,
            default_dataset = ?default_dataset,
            error = %e,
            "Tenant update failed"
        );
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
    path = "/api/v1/admin/tenants/{tenant_id}",
    tag = "tenants",
    security(("bearerAuth" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
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
    path = "/api/v1/admin/tenants/{tenant_id}/api-keys",
    tag = "api-keys",
    security(("bearerAuth" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
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
                api_keys: keys.into_iter().map(api_key_record_to_response).collect(),
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
    path = "/api/v1/admin/tenants/{tenant_id}/api-keys",
    tag = "api-keys",
    security(("bearerAuth" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = CreateApiKeyRequest,
    responses(
        (status = 201, description = "API key created", body = CreateApiKeyResponse),
        (status = 404, description = "Tenant not found", body = ApiError),
        (status = 429, description = "Tenant API key quota exceeded", body = ApiError,
            headers(
                ("Retry-After" = i64, description = "Fixed at 1 second: a count quota has no token bucket to refill from"),
                ("X-RateLimit-Limit" = i64, description = "The tenant's max_api_keys quota"),
            )),
        (status = 400, description = "Dataset does not exist", body = ApiError),
        (status = 422, description = "Invalid or empty scopes", body = ApiError),
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

    // Enforce the tenant's API key quota (active keys only; revoked keys
    // do not count against it).
    if let Some(max_keys) = state.config().auth.limits_for(&tenant_id).max_api_keys {
        match state.catalog().list_api_keys(&tenant_id).await {
            Ok(keys) => {
                let active = keys.iter().filter(|k| k.revoked_at.is_none()).count();
                if active as u64 >= u64::from(max_keys) {
                    return quota_exceeded_response(
                        max_keys,
                        format!(
                            "Tenant '{tenant_id}' already has {active} active API keys \
                             (limit {max_keys}); revoke a key or raise max_api_keys"
                        ),
                    );
                }
            }
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(
                        serde_json::to_value(ApiError::new("internal_error", e.to_string()))
                            .unwrap(),
                    ),
                )
                    .into_response();
            }
        }
    }

    if let Err(response) = validate_scopes_response(&request.scopes) {
        return *response;
    }
    if let Some(dataset_id) = &request.dataset_id
        && let Err(response) = validate_dataset_exists(&*state, &tenant_id, dataset_id).await
    {
        return *response;
    }

    // Generate a new raw API key
    let raw_key = format!("sk-{}-{}", tenant_id, Uuid::new_v4());
    let key_hash = Authenticator::hash_api_key(&raw_key);

    match state
        .catalog()
        .upsert_scoped_api_key(
            &tenant_id,
            &key_hash,
            request.name.as_deref(),
            request.dataset_id.as_deref(),
            Some(&request.scopes),
            None,
        )
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
                scopes: request.scopes,
                dataset_id: request.dataset_id,
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

/// Update the scopes and/or dataset restriction of a live API key
#[utoipa::path(
    patch,
    path = "/api/v1/admin/tenants/{tenant_id}/api-keys/{key_id}",
    tag = "api-keys",
    security(("bearerAuth" = [])),
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("key_id" = String, Path, description = "API key identifier"),
    ),
    request_body = UpdateApiKeyRequest,
    responses(
        (status = 200, description = "API key updated", body = ApiKeyResponse),
        (status = 400, description = "Dataset does not exist", body = ApiError),
        (status = 404, description = "API key not found", body = ApiError),
        (status = 409, description = "API key is revoked", body = ApiError),
        (status = 422, description = "Invalid scopes", body = ApiError),
    )
)]
pub async fn update_api_key<S: RouterState>(
    state: State<S>,
    Path((tenant_id, key_id)): Path<(String, String)>,
    Json(request): Json<UpdateApiKeyRequest>,
) -> impl IntoResponse {
    if let Some(scopes) = &request.scopes
        && let Err(response) = validate_scopes_response(scopes)
    {
        return *response;
    }
    if let Some(dataset_id) = &request.dataset_id
        && let Err(response) = validate_dataset_exists(&*state, &tenant_id, dataset_id).await
    {
        return *response;
    }
    match state.catalog().get_api_key(&key_id).await {
        Ok(Some(record)) if record.tenant_id == tenant_id => {
            if record.revoked_at.is_some() {
                return api_error(
                    StatusCode::CONFLICT,
                    "revoked",
                    format!("API key '{key_id}' is revoked and cannot be updated"),
                );
            }
        }
        Ok(_) => {
            return api_error(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("API key '{key_id}' not found for tenant '{tenant_id}'"),
            );
        }
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                e.to_string(),
            );
        }
    }
    match state
        .catalog()
        .update_api_key_scopes(
            &key_id,
            request.scopes.as_deref(),
            request.dataset_id.as_deref(),
        )
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            return api_error(
                StatusCode::CONFLICT,
                "revoked",
                format!("API key '{key_id}' is revoked and cannot be updated"),
            );
        }
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                e.to_string(),
            );
        }
    }
    match state.catalog().get_api_key(&key_id).await {
        Ok(Some(record)) => (
            StatusCode::OK,
            Json(serde_json::to_value(api_key_record_to_response(record)).unwrap()),
        )
            .into_response(),
        Ok(None) => api_error(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("API key '{key_id}' not found"),
        ),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            e.to_string(),
        ),
    }
}

fn api_error(
    status: StatusCode,
    code: &str,
    message: impl Into<String>,
) -> axum::response::Response {
    (
        status,
        Json(serde_json::to_value(ApiError::new(code, message)).unwrap()),
    )
        .into_response()
}

fn api_key_record_to_response(record: common::catalog::ApiKeyRecord) -> ApiKeyResponse {
    ApiKeyResponse {
        id: record.id,
        name: record.name,
        scopes: record.scopes,
        dataset_id: record.dataset_id,
        created_at: record.created_at.to_rfc3339(),
        revoked_at: record.revoked_at.map(|t| t.to_rfc3339()),
    }
}

/// `422 invalid_scope` naming the offending scope (or the empty list).
fn validate_scopes_response(scopes: &[String]) -> Result<(), Box<axum::response::Response>> {
    validate_scopes(scopes).map_err(|error| {
        Box::new(api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_scope",
            error.to_string(),
        ))
    })
}

/// `400 invalid_dataset` unless `dataset_id` exists in the tenant.
async fn validate_dataset_exists<S: RouterState>(
    state: &S,
    tenant_id: &str,
    dataset_id: &str,
) -> Result<(), Box<axum::response::Response>> {
    match state.catalog().get_datasets(tenant_id).await {
        Ok(datasets) if datasets.iter().any(|d| d.name == dataset_id) => Ok(()),
        Ok(_) => Err(Box::new(api_error(
            StatusCode::BAD_REQUEST,
            "invalid_dataset",
            format!("Dataset '{dataset_id}' does not exist in tenant '{tenant_id}'"),
        ))),
        Err(e) => Err(Box::new(api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            e.to_string(),
        ))),
    }
}

/// Revoke an API key
#[utoipa::path(
    delete,
    path = "/api/v1/admin/tenants/{tenant_id}/api-keys/{key_id}",
    tag = "api-keys",
    security(("bearerAuth" = [])),
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
    path = "/api/v1/admin/tenants/{tenant_id}/datasets",
    tag = "datasets",
    security(("bearerAuth" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
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
    path = "/api/v1/admin/tenants/{tenant_id}/datasets",
    tag = "datasets",
    security(("bearerAuth" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = CreateDatasetRequest,
    responses(
        (status = 201, description = "Dataset created", body = DatasetResponse),
        (status = 404, description = "Tenant not found", body = ApiError),
        (status = 429, description = "Tenant dataset quota exceeded", body = ApiError,
            headers(
                ("Retry-After" = i64, description = "Fixed at 1 second: a count quota has no token bucket to refill from"),
                ("X-RateLimit-Limit" = i64, description = "The tenant's max_datasets quota"),
            )),
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

    // Enforce the tenant's dataset quota.
    if let Some(max_datasets) = state.config().auth.limits_for(&tenant_id).max_datasets {
        match state.catalog().get_datasets(&tenant_id).await {
            Ok(datasets) => {
                let count = datasets.len();
                if count as u64 >= u64::from(max_datasets) {
                    return quota_exceeded_response(
                        max_datasets,
                        format!(
                            "Tenant '{tenant_id}' already has {count} datasets \
                             (limit {max_datasets}); delete a dataset or raise \
                             max_datasets"
                        ),
                    );
                }
            }
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(
                        serde_json::to_value(ApiError::new("internal_error", e.to_string()))
                            .unwrap(),
                    ),
                )
                    .into_response();
            }
        }
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
    path = "/api/v1/admin/tenants/{tenant_id}/datasets/{dataset_id}",
    tag = "datasets",
    security(("bearerAuth" = [])),
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("dataset_id" = String, Path, description = "Dataset identifier"),
    ),
    responses(
        (status = 204, description = "Dataset deleted"),
        (status = 403, description = "Config-sourced datasets cannot be deleted", body = ApiError),
        (status = 404, description = "Dataset not found", body = ApiError),
    )
)]
pub async fn delete_dataset<S: RouterState>(
    state: State<S>,
    Path((tenant_id, dataset_id)): Path<(String, String)>,
) -> impl IntoResponse {
    match state
        .catalog()
        .delete_dataset_for_tenant(&tenant_id, &dataset_id)
        .await
    {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(
                serde_json::to_value(ApiError::new(
                    "not_found",
                    format!("Dataset '{dataset_id}' not found for tenant '{tenant_id}'"),
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

// ── User endpoints ──────────────────────────────────────────────────────

/// Create a human user and grant an initial tenant membership.
#[utoipa::path(
    post,
    path = "/api/v1/admin/users",
    tag = "users",
    security(("bearerAuth" = [])),
    request_body = CreateUserRequest,
    responses(
        (status = 201, description = "User created", body = UserResponse),
        (status = 400, description = "Validation error", body = ApiError),
        (status = 404, description = "Tenant not found", body = ApiError),
        (status = 409, description = "User already exists", body = ApiError),
    )
)]
pub async fn create_user<S: RouterState>(
    state: State<S>,
    Json(request): Json<CreateUserRequest>,
) -> impl IntoResponse {
    fn bad(code: &str, msg: impl Into<String>) -> (StatusCode, Json<serde_json::Value>) {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::to_value(ApiError::new(code, msg.into())).unwrap()),
        )
    }

    if request.email.trim().is_empty() {
        return bad("validation_error", "email must not be empty").into_response();
    }
    if request.password.len() < 12 {
        return bad(
            "validation_error",
            "password must be at least 12 characters",
        )
        .into_response();
    }
    let role = match MembershipRole::from_str(&request.role) {
        Ok(role) => role,
        Err(_) => {
            return bad(
                "validation_error",
                format!(
                    "invalid role '{}': expected admin, member, or viewer",
                    request.role
                ),
            )
            .into_response();
        }
    };

    // The tenant must exist in the catalog before a membership can be granted.
    match state.catalog().get_tenant(&request.tenant).await {
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(
                    serde_json::to_value(ApiError::new(
                        "not_found",
                        format!("Tenant '{}' not found", request.tenant),
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

    // Hash the password off the async runtime; the plaintext never touches the
    // catalog.
    let password = request.password.clone();
    let password_hash = match tokio::task::spawn_blocking(move || hash_password(&password)).await {
        Ok(Ok(hash)) => hash,
        Ok(Err(e)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(
                    serde_json::to_value(ApiError::new(
                        "internal_error",
                        format!("password hashing failed: {e}"),
                    ))
                    .unwrap(),
                ),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(
                    serde_json::to_value(ApiError::new(
                        "internal_error",
                        format!("password hashing task failed: {e}"),
                    ))
                    .unwrap(),
                ),
            )
                .into_response();
        }
    };

    let user = match state
        .catalog()
        .create_user(
            &request.email,
            request.display_name.as_deref(),
            &password_hash,
            request.instance_admin,
        )
        .await
    {
        Ok(user) => user,
        Err(e) => {
            let msg = e.to_string();
            // A unique-constraint violation on email is a conflict, not a 500.
            let (status, code) = if msg.contains("UNIQUE") || msg.contains("duplicate") {
                (StatusCode::CONFLICT, "conflict")
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal_error")
            };
            return (
                status,
                Json(serde_json::to_value(ApiError::new(code, msg)).unwrap()),
            )
                .into_response();
        }
    };

    if let Err(e) = state
        .catalog()
        .upsert_tenant_membership(&user.id, &request.tenant, role)
        .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(
                serde_json::to_value(ApiError::new(
                    "internal_error",
                    format!("user created but membership grant failed: {e}"),
                ))
                .unwrap(),
            ),
        )
            .into_response();
    }

    let response = UserResponse {
        id: user.id,
        email: user.email,
        display_name: user.display_name,
        instance_admin: user.is_instance_admin,
        created_at: user.created_at.to_rfc3339(),
    };
    (
        StatusCode::CREATED,
        Json(serde_json::to_value(response).unwrap()),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RouterAppState;
    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::{delete, get, post, put},
    };
    use common::catalog::Catalog;
    use common::config::Configuration;
    use tower::ServiceExt;

    async fn create_admin_test_state() -> RouterAppState {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let config = Configuration::default();
        RouterAppState::new(catalog, config)
    }

    fn admin_router(state: RouterAppState) -> Router {
        Router::new()
            .route("/tenants", get(list_tenants::<RouterAppState>))
            .route("/tenants", post(create_tenant::<RouterAppState>))
            .route("/tenants/{tenant_id}", get(get_tenant::<RouterAppState>))
            .route("/tenants/{tenant_id}", put(update_tenant::<RouterAppState>))
            .route(
                "/tenants/{tenant_id}",
                delete(delete_tenant::<RouterAppState>),
            )
            .route(
                "/tenants/{tenant_id}/api-keys",
                get(list_api_keys::<RouterAppState>),
            )
            .route(
                "/tenants/{tenant_id}/api-keys",
                post(create_api_key::<RouterAppState>),
            )
            .route(
                "/tenants/{tenant_id}/api-keys/{key_id}",
                delete(revoke_api_key::<RouterAppState>).patch(update_api_key::<RouterAppState>),
            )
            .route(
                "/tenants/{tenant_id}/datasets",
                get(list_datasets::<RouterAppState>),
            )
            .route(
                "/tenants/{tenant_id}/datasets",
                post(create_dataset::<RouterAppState>),
            )
            .route(
                "/tenants/{tenant_id}/datasets/{dataset_id}",
                delete(delete_dataset::<RouterAppState>),
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

    /// A tenant created with a `default_dataset` must get a real `datasets`
    /// row, not just the column on the tenant row. Without one it cannot
    /// authenticate at all — `resolve_database_tenant` fails closed with
    /// `403 Dataset '<name>' not found` — and it is invisible to everything
    /// that enumerates dataset rows (issue #1066). The management API already
    /// materializes the row; the admin API did not.
    #[tokio::test]
    async fn creating_a_tenant_materializes_its_default_dataset() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let state = RouterAppState::new(catalog.clone(), Configuration::default());
        let app = admin_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/tenants")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"id": "acme", "name": "Acme", "default_dataset": "production"}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        assert_eq!(
            catalog
                .get_datasets("acme")
                .await
                .unwrap()
                .iter()
                .map(|d| d.name.as_str())
                .collect::<Vec<_>>(),
            vec!["production"],
            "the default dataset must exist as a row"
        );

        // And it is visible on the dataset listing the UI reads.
        let request = Request::builder()
            .uri("/tenants/acme/datasets")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(
            String::from_utf8_lossy(&body).contains("production"),
            "the default dataset must be listed"
        );
    }

    /// Repointing `default_dataset` at a name with no row would recreate
    /// exactly the state creation now avoids.
    #[tokio::test]
    async fn updating_the_default_dataset_materializes_it() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let state = RouterAppState::new(catalog.clone(), Configuration::default());
        let app = admin_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/tenants")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"id": "acme", "name": "Acme", "default_dataset": "production"}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::CREATED
        );

        let request = Request::builder()
            .method("PUT")
            .uri("/tenants/acme")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"default_dataset": "staging"}"#))
            .unwrap();
        assert_eq!(app.oneshot(request).await.unwrap().status(), StatusCode::OK);

        let names: Vec<String> = catalog
            .get_datasets("acme")
            .await
            .unwrap()
            .into_iter()
            .map(|d| d.name)
            .collect();
        assert!(
            names.contains(&"staging".to_string()),
            "new default materialized"
        );
        assert!(
            names.contains(&"production".to_string()),
            "the previous default is not removed — it may hold data"
        );
    }

    /// A tenant created without a default dataset gains no dataset row.
    #[tokio::test]
    async fn creating_a_tenant_without_a_default_dataset_writes_no_row() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let state = RouterAppState::new(catalog.clone(), Configuration::default());
        let app = admin_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/tenants")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"id": "acme", "name": "Acme"}"#))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        assert!(catalog.get_datasets("acme").await.unwrap().is_empty());
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

    async fn create_quota_test_state(limits: common::config::TenantLimits) -> RouterAppState {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let mut config = Configuration::default();
        config.auth.default_limits = limits;
        RouterAppState::new(catalog, config)
    }

    #[tokio::test]
    async fn api_key_quota_rejects_creation_beyond_limit() {
        let state = create_quota_test_state(common::config::TenantLimits {
            max_api_keys: Some(2),
            ..Default::default()
        })
        .await;
        state
            .catalog()
            .upsert_tenant("acme", "Acme Corp", None, "database")
            .await
            .unwrap();
        let app = admin_router(state);

        let create = |name: &str| {
            Request::builder()
                .method("POST")
                .uri("/tenants/acme/api-keys")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"name": "{name}", "scopes": ["traces:write"]}}"#
                )))
                .unwrap()
        };

        let mut first_key_id = String::new();
        for name in ["one", "two"] {
            let response = app.clone().oneshot(create(name)).await.unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
            if name == "one" {
                let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
                    .await
                    .unwrap();
                let created: CreateApiKeyResponse = serde_json::from_slice(&body).unwrap();
                first_key_id = created.id;
            }
        }

        // Third key exceeds the quota.
        let response = app.clone().oneshot(create("three")).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );
        assert_eq!(
            response
                .headers()
                .get("x-ratelimit-limit")
                .and_then(|v| v.to_str().ok()),
            Some("2")
        );
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let error: ApiError = serde_json::from_slice(&body).unwrap();
        assert_eq!(error.error, "quota_exceeded");

        // Revoking a key frees quota: creation succeeds again.
        let request = Request::builder()
            .method("DELETE")
            .uri(format!("/tenants/acme/api-keys/{first_key_id}"))
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app.clone().oneshot(create("three")).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn dataset_quota_rejects_creation_beyond_limit() {
        let state = create_quota_test_state(common::config::TenantLimits {
            max_datasets: Some(1),
            ..Default::default()
        })
        .await;
        state
            .catalog()
            .upsert_tenant("acme", "Acme Corp", None, "database")
            .await
            .unwrap();
        let app = admin_router(state);

        let create = |name: &str| {
            Request::builder()
                .method("POST")
                .uri("/tenants/acme/datasets")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"name": "{name}", "scopes": ["traces:write"]}}"#
                )))
                .unwrap()
        };

        let response = app.clone().oneshot(create("production")).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app.clone().oneshot(create("staging")).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );
        assert_eq!(
            response
                .headers()
                .get("x-ratelimit-limit")
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let error: ApiError = serde_json::from_slice(&body).unwrap();
        assert_eq!(error.error, "quota_exceeded");
    }

    #[tokio::test]
    async fn quotas_unlimited_by_default() {
        let state = create_admin_test_state().await;
        state
            .catalog()
            .upsert_tenant("acme", "Acme Corp", None, "database")
            .await
            .unwrap();
        let app = admin_router(state);

        for i in 0..20 {
            let request = Request::builder()
                .method("POST")
                .uri("/tenants/acme/api-keys")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"name": "key-{i}", "scopes": ["logs:write"]}}"#
                )))
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
        }
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
            .body(Body::from(
                r#"{"name": "Production Key", "scopes": ["traces:write", "schema:read"]}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let created: CreateApiKeyResponse = serde_json::from_slice(&body).unwrap();
        assert!(created.key.starts_with("sk-acme-"));
        assert_eq!(created.name, Some("Production Key".to_string()));
        assert_eq!(created.scopes, vec!["traces:write", "schema:read"]);
        assert_eq!(created.dataset_id, None);

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
        assert_eq!(
            list.api_keys[0].scopes,
            Some(vec!["traces:write".to_string(), "schema:read".to_string()])
        );

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

    async fn create_key(app: &Router, body: &str) -> (StatusCode, serde_json::Value) {
        let request = Request::builder()
            .method("POST")
            .uri("/tenants/acme/api-keys")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    async fn patch_key(app: &Router, key_id: &str, body: &str) -> (StatusCode, serde_json::Value) {
        let request = Request::builder()
            .method("PATCH")
            .uri(format!("/tenants/acme/api-keys/{key_id}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let value = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, value)
    }

    #[tokio::test]
    async fn api_key_creation_rejects_empty_and_unknown_scopes() {
        let state = create_admin_test_state().await;
        state
            .catalog()
            .upsert_tenant("acme", "Acme Corp", None, "database")
            .await
            .unwrap();
        let app = admin_router(state);

        let (status, body) = create_key(&app, r#"{"name": "none", "scopes": []}"#).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert!(
            body["message"]
                .as_str()
                .unwrap()
                .contains("at least one scope")
        );

        let (status, body) = create_key(
            &app,
            r#"{"name": "bogus", "scopes": ["traces:write", "schema:admin"]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert!(body["message"].as_str().unwrap().contains("schema:admin"));

        // `scopes` is required: a body without it is rejected by deserialization.
        let request = Request::builder()
            .method("POST")
            .uri("/tenants/acme/api-keys")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"name": "legacy"}"#))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn api_key_creation_validates_dataset() {
        let state = create_admin_test_state().await;
        state
            .catalog()
            .upsert_tenant("acme", "Acme Corp", None, "database")
            .await
            .unwrap();
        state
            .catalog()
            .create_dataset("acme", "production")
            .await
            .unwrap();
        let app = admin_router(state);

        let (status, body) = create_key(
            &app,
            r#"{"name": "ghost", "scopes": ["schema:read"], "dataset_id": "nope"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

        let (status, body) = create_key(
            &app,
            r#"{"name": "prod", "scopes": ["schema:read"], "dataset_id": "production"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(body["dataset_id"], "production");
    }

    #[tokio::test]
    async fn patch_api_key_updates_scopes_and_dataset_but_not_revoked_keys() {
        let state = create_admin_test_state().await;
        state
            .catalog()
            .upsert_tenant("acme", "Acme Corp", None, "database")
            .await
            .unwrap();
        state
            .catalog()
            .create_dataset("acme", "production")
            .await
            .unwrap();
        let app = admin_router(state);

        let (status, created) =
            create_key(&app, r#"{"name": "k", "scopes": ["schema:read"]}"#).await;
        assert_eq!(status, StatusCode::CREATED);
        let key_id = created["id"].as_str().unwrap().to_string();

        // Add schema:write.
        let (status, body) = patch_key(
            &app,
            &key_id,
            r#"{"scopes": ["schema:read", "schema:write"]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["scopes"],
            serde_json::json!(["schema:read", "schema:write"])
        );
        assert_eq!(body["dataset_id"], serde_json::Value::Null);

        // Dataset only, scopes preserved.
        let (status, body) = patch_key(&app, &key_id, r#"{"dataset_id": "production"}"#).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["scopes"],
            serde_json::json!(["schema:read", "schema:write"])
        );
        assert_eq!(body["dataset_id"], "production");

        // Validation.
        let (status, body) = patch_key(&app, &key_id, r#"{"scopes": []}"#).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        let (status, body) = patch_key(&app, &key_id, r#"{"scopes": ["schema:admin"]}"#).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert!(body["message"].as_str().unwrap().contains("schema:admin"));
        let (status, _) = patch_key(&app, &key_id, r#"{"dataset_id": "nope"}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // Unknown key.
        let (status, _) = patch_key(&app, "no-such-key", r#"{"scopes": ["schema:read"]}"#).await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Revoked key is immutable.
        let request = Request::builder()
            .method("DELETE")
            .uri(format!("/tenants/acme/api-keys/{key_id}"))
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::NO_CONTENT
        );
        let (status, body) = patch_key(&app, &key_id, r#"{"scopes": ["schema:read"]}"#).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
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
            .body(Body::from(r#"{"name": "Key", "scopes": ["traces:write"]}"#))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
