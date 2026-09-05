//! Tenant management endpoints (`/api/v1/manage/*`).
//!
//! Used by the web UI (session/OAuth principals with the tenant-admin role or
//! instance-admin flag) and by automation holding an API key that carries the
//! `tenant:manage` scope. Both act only on the tenant of the caller's context;
//! tenant creation stays instance-admin-only.

use crate::RouterState;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use common::{
    auth::{Authenticator, TenantContext, TenantContextExtractor, validate_id, validate_scopes},
    catalog::MembershipRole,
    schema::{
        SCHEMA_DEFINITIONS,
        logical::{AttributeLevel, Filterability, LogicalFieldKind, LogicalSchema, LogicalType},
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

pub fn router<S: RouterState>() -> Router<S> {
    Router::new()
        .route("/tenants", post(create_tenant::<S>))
        .route(
            "/tenants/{tenant_id}/datasets",
            get(list_datasets::<S>).post(create_dataset::<S>),
        )
        .route(
            "/tenants/{tenant_id}/datasets/{dataset_name}",
            delete(delete_dataset::<S>),
        )
        .route(
            "/tenants/{tenant_id}/api-keys",
            get(list_api_keys::<S>).post(create_api_key::<S>),
        )
        .route(
            "/tenants/{tenant_id}/api-keys/{key_id}",
            delete(revoke_api_key::<S>).patch(update_api_key::<S>),
        )
        .route(
            "/tenants/{tenant_id}/memberships",
            get(list_memberships::<S>).put(upsert_membership::<S>),
        )
        .route(
            "/tenants/{tenant_id}/memberships/{user_id}",
            delete(remove_membership::<S>),
        )
        .route("/schema", get(get_schema::<S>))
}

/// Error returned when the principal may not manage the tenant.
const MANAGE_FORBIDDEN: &str = "Tenant administrator role or tenant:manage scope required";

/// Whether `ctx` may manage its own tenant: a human principal (session or
/// OAuth token) with the tenant-admin role or instance-admin flag, or an API
/// key explicitly scoped with `tenant:manage`. Legacy unscoped keys do NOT
/// qualify — see [`TenantContext::can_manage_via_key`].
fn can_manage(ctx: &TenantContext) -> bool {
    (ctx.user_id.is_some() && ctx.can_manage_tenant()) || ctx.can_manage_via_key()
}

fn authorize_tenant(
    ctx: &TenantContext,
    tenant_id: &str,
) -> Result<(), (StatusCode, &'static str)> {
    if ctx.tenant_id != tenant_id {
        return Err((StatusCode::FORBIDDEN, "Tenant context does not match path"));
    }
    if !can_manage(ctx) {
        return Err((StatusCode::FORBIDDEN, MANAGE_FORBIDDEN));
    }
    Ok(())
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}

/// Whether `target_user_id` is the tenant's sole remaining administrator —
/// used to block demoting or removing the last admin membership.
fn is_last_remaining_admin(
    members: &[common::catalog::TenantMembershipRecord],
    target_user_id: &str,
) -> bool {
    let target_is_admin = members.iter().any(|membership| {
        membership.user_id == target_user_id && membership.role == MembershipRole::Admin
    });
    let admin_count = members
        .iter()
        .filter(|membership| membership.role == MembershipRole::Admin)
        .count();
    target_is_admin && admin_count == 1
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[schema(as = ManageCreateTenantRequest)]
pub struct CreateTenantRequest {
    pub id: String,
    pub name: String,
    pub default_dataset: Option<String>,
}

/// 201 response body for tenant creation via the management API.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct ManageCreatedTenant {
    id: String,
}

/// Error response body for the management API.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct ManageError {
    error: String,
}

#[utoipa::path(
    post,
    path = "/api/v1/manage/tenants",
    tag = "tenants",
    operation_id = "manage_create_tenant",
    request_body = CreateTenantRequest,
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 201, description = "Tenant created", body = ManageCreatedTenant),
        (status = 400, description = "Validation error", body = ManageError),
        (status = 403, description = "Instance administrator required", body = ManageError),
        (status = 409, description = "Tenant already exists", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn create_tenant<S: RouterState>(
    State(state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
    Json(request): Json<CreateTenantRequest>,
) -> Response {
    if !ctx.is_instance_admin {
        return error(StatusCode::FORBIDDEN, "Instance administrator required");
    }
    let tenant_id = match validate_id(&request.id) {
        Ok(value) => value,
        Err(error_value) => return error(StatusCode::BAD_REQUEST, error_value.to_string()),
    };
    let default_dataset = match request.default_dataset.as_deref() {
        Some(value) => match validate_id(value) {
            Ok(value) => Some(value),
            Err(error_value) => return error(StatusCode::BAD_REQUEST, error_value.to_string()),
        },
        None => None,
    };
    if request.name.trim().is_empty() {
        return error(StatusCode::BAD_REQUEST, "Tenant name is required");
    }
    if state
        .config()
        .auth
        .tenants
        .iter()
        .any(|tenant| tenant.id == tenant_id)
    {
        return error(
            StatusCode::CONFLICT,
            "A configuration-backed tenant already uses this ID",
        );
    }
    match state.catalog().get_tenant(&tenant_id).await {
        Ok(Some(_)) => return error(StatusCode::CONFLICT, "Tenant already exists"),
        Ok(None) => {}
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, "tenant existence check failed");
            return error(StatusCode::INTERNAL_SERVER_ERROR, "Unable to create tenant");
        }
    }
    // Tenant row and default dataset row in one transaction: a tenant whose
    // `default_dataset` has no row fails authentication closed, and creation
    // rejects an existing id with 409, so a retry could not repair it.
    if let Err(catalog_error) = state
        .catalog()
        .upsert_tenant_with_default_dataset(
            &tenant_id,
            request.name.trim(),
            default_dataset.as_deref(),
            "database",
        )
        .await
    {
        tracing::error!(error = %catalog_error, tenant_id, "tenant creation failed");
        return error(StatusCode::INTERNAL_SERVER_ERROR, "Unable to create tenant");
    }
    if let Some(user_id) = &ctx.user_id
        && let Err(catalog_error) = state
            .catalog()
            .upsert_tenant_membership(user_id, &tenant_id, MembershipRole::Admin)
            .await
    {
        tracing::error!(error = %catalog_error, tenant_id, user_id, "creator membership failed");
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Tenant was created but creator access could not be recorded",
        );
    }
    tracing::info!(actor_user_id = ?ctx.user_id, tenant_id, "tenant created via UX");
    (
        StatusCode::CREATED,
        Json(ManageCreatedTenant { id: tenant_id }),
    )
        .into_response()
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[schema(as = ManageDatasetResponse)]
pub(crate) struct DatasetResponse {
    id: String,
    name: String,
}

#[utoipa::path(
    get,
    path = "/api/v1/manage/tenants/{tenant_id}/datasets",
    tag = "datasets",
    operation_id = "manage_list_datasets",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "List of datasets", body = [DatasetResponse]),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn list_datasets<S: RouterState>(
    State(state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
    Path(tenant_id): Path<String>,
) -> Response {
    if let Err((status, message)) = authorize_tenant(&ctx, &tenant_id) {
        return error(status, message);
    }
    match state.catalog().get_datasets(&tenant_id).await {
        Ok(datasets) => Json(
            datasets
                .into_iter()
                .map(|dataset| DatasetResponse {
                    id: dataset.id,
                    name: dataset.name,
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, "dataset listing failed");
            error(StatusCode::INTERNAL_SERVER_ERROR, "Unable to list datasets")
        }
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[schema(as = ManageCreateDatasetRequest)]
pub(crate) struct CreateDatasetRequest {
    name: String,
}

#[utoipa::path(
    post,
    path = "/api/v1/manage/tenants/{tenant_id}/datasets",
    tag = "datasets",
    operation_id = "manage_create_dataset",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = CreateDatasetRequest,
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 201, description = "Dataset created", body = DatasetResponse),
        (status = 400, description = "Validation error", body = ManageError),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 409, description = "Unable to create dataset", body = ManageError),
    )
)]
pub(crate) async fn create_dataset<S: RouterState>(
    State(state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
    Path(tenant_id): Path<String>,
    Json(request): Json<CreateDatasetRequest>,
) -> Response {
    if let Err((status, message)) = authorize_tenant(&ctx, &tenant_id) {
        return error(status, message);
    }
    let name = match validate_id(&request.name) {
        Ok(value) => value,
        Err(error_value) => return error(StatusCode::BAD_REQUEST, error_value.to_string()),
    };
    match state.catalog().create_dataset(&tenant_id, &name).await {
        Ok(id) => {
            tracing::info!(actor_user_id = ?ctx.user_id, tenant_id, dataset = name, "dataset created via UX");
            crate::endpoints::provision_dataset_tables(
                state.config(),
                state.catalog(),
                &tenant_id,
                &name,
            )
            .await;
            (StatusCode::CREATED, Json(DatasetResponse { id, name })).into_response()
        }
        Err(catalog_error) => {
            tracing::warn!(error = %catalog_error, tenant_id, dataset = name, "dataset creation failed");
            error(StatusCode::CONFLICT, "Unable to create dataset")
        }
    }
}

#[utoipa::path(
    delete,
    path = "/api/v1/manage/tenants/{tenant_id}/datasets/{dataset_name}",
    tag = "datasets",
    operation_id = "manage_delete_dataset",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("dataset_name" = String, Path, description = "Dataset name"),
    ),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 204, description = "Dataset deleted"),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 404, description = "Dataset not found", body = ManageError),
        (status = 409, description = "Dataset cannot be deleted", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn delete_dataset<S: RouterState>(
    State(state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
    Path((tenant_id, dataset_name)): Path<(String, String)>,
) -> Response {
    if let Err((status, message)) = authorize_tenant(&ctx, &tenant_id) {
        return error(status, message);
    }
    if state
        .config()
        .auth
        .tenants
        .iter()
        .find(|tenant| tenant.id == tenant_id)
        .is_some_and(|tenant| {
            tenant
                .datasets
                .iter()
                .any(|dataset| dataset.id == dataset_name)
        })
    {
        return error(
            StatusCode::CONFLICT,
            "Configuration-backed datasets cannot be deleted in the UI",
        );
    }
    match state.catalog().get_tenant(&tenant_id).await {
        Ok(Some(tenant)) if tenant.default_dataset.as_deref() == Some(dataset_name.as_str()) => {
            return error(
                StatusCode::CONFLICT,
                "The default dataset cannot be deleted",
            );
        }
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, "tenant lookup failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to delete dataset",
            );
        }
        _ => {}
    }
    let datasets = match state.catalog().get_datasets(&tenant_id).await {
        Ok(value) => value,
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, "dataset lookup failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to delete dataset",
            );
        }
    };
    let Some(dataset) = datasets
        .into_iter()
        .find(|dataset| dataset.name == dataset_name)
    else {
        return error(StatusCode::NOT_FOUND, "Dataset not found");
    };
    match state
        .catalog()
        .delete_dataset_for_tenant(&tenant_id, &dataset.id)
        .await
    {
        Ok(true) => {
            tracing::info!(actor_user_id = ?ctx.user_id, tenant_id, dataset = dataset_name, "dataset deleted via UX");
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => error(StatusCode::NOT_FOUND, "Dataset not found"),
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, dataset = dataset_name, "dataset deletion failed");
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to delete dataset",
            )
        }
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[schema(as = ManageCreateApiKeyRequest)]
pub(crate) struct CreateApiKeyRequest {
    name: Option<String>,
    dataset_id: Option<String>,
    scopes: Vec<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[schema(as = ManageApiKeyResponse)]
pub(crate) struct ApiKeyResponse {
    id: String,
    name: Option<String>,
    dataset_id: Option<String>,
    scopes: Option<Vec<String>>,
    revoked: bool,
    created_at: String,
}

/// 201 response body for API key creation via the management API.
///
/// Fields mirror the previous `json!` body exactly (including `null` for
/// absent `name`/`dataset_id`), preserving the wire format.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct ManageCreatedApiKey {
    id: String,
    key: String,
    name: Option<String>,
    dataset_id: Option<String>,
    scopes: Vec<String>,
}

#[utoipa::path(
    get,
    path = "/api/v1/manage/tenants/{tenant_id}/api-keys",
    tag = "api-keys",
    operation_id = "manage_list_api_keys",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "List of API keys", body = [ApiKeyResponse]),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn list_api_keys<S: RouterState>(
    State(state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
    Path(tenant_id): Path<String>,
) -> Response {
    if let Err((status, message)) = authorize_tenant(&ctx, &tenant_id) {
        return error(status, message);
    }
    match state.catalog().list_api_keys(&tenant_id).await {
        Ok(keys) => Json(
            keys.into_iter()
                .map(|key| ApiKeyResponse {
                    id: key.id,
                    name: key.name,
                    dataset_id: key.dataset_id,
                    scopes: key.scopes,
                    revoked: key.revoked_at.is_some(),
                    created_at: key.created_at.to_rfc3339(),
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, "API key listing failed");
            error(StatusCode::INTERNAL_SERVER_ERROR, "Unable to list API keys")
        }
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/manage/tenants/{tenant_id}/api-keys",
    tag = "api-keys",
    operation_id = "manage_create_api_key",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = CreateApiKeyRequest,
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 201, description = "API key created", body = ManageCreatedApiKey),
        (status = 400, description = "Dataset does not exist", body = ManageError),
        (status = 422, description = "Invalid or empty scopes", body = ManageError),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 409, description = "Unable to create API key", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn create_api_key<S: RouterState>(
    State(state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
    Path(tenant_id): Path<String>,
    Json(request): Json<CreateApiKeyRequest>,
) -> Response {
    if let Err((status, message)) = authorize_tenant(&ctx, &tenant_id) {
        return error(status, message);
    }
    if let Err(validation_error) = validate_scopes(&request.scopes) {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            validation_error.to_string(),
        );
    }
    if let Some(dataset_id) = &request.dataset_id
        && let Err(response) = ensure_dataset_exists(&state, &tenant_id, dataset_id).await
    {
        return *response;
    }
    let secret = format!("sdbk_{}", Uuid::new_v4().simple());
    let key_hash = Authenticator::hash_api_key(&secret);
    // TODO(multi-dataset-key-restriction phase 2): `request.dataset_id`
    // becomes `dataset_ids: Vec<String>` on the request DTO; this
    // single-to-slice bridge goes away once that lands.
    let dataset_ids = request.dataset_id.as_ref().map(std::slice::from_ref);
    match state
        .catalog()
        .upsert_scoped_api_key(
            &tenant_id,
            &key_hash,
            request.name.as_deref(),
            dataset_ids,
            Some(&request.scopes),
            ctx.user_id.as_deref(),
        )
        .await
    {
        Ok(id) => {
            tracing::info!(actor_user_id = ?ctx.user_id, tenant_id, key_id = id, "scoped API key created via UX");
            (
                StatusCode::CREATED,
                Json(ManageCreatedApiKey {
                    id,
                    key: secret,
                    name: request.name,
                    dataset_id: request.dataset_id,
                    scopes: request.scopes,
                }),
            )
                .into_response()
        }
        Err(catalog_error) => {
            tracing::warn!(error = %catalog_error, tenant_id, "API key creation failed");
            error(StatusCode::CONFLICT, "Unable to create API key")
        }
    }
}

/// `400` unless `dataset_id` exists in the tenant.
async fn ensure_dataset_exists<S: RouterState>(
    state: &S,
    tenant_id: &str,
    dataset_id: &str,
) -> Result<(), Box<Response>> {
    match state.catalog().get_datasets(tenant_id).await {
        Ok(datasets) if datasets.iter().any(|dataset| dataset.name == dataset_id) => Ok(()),
        Ok(_) => Err(Box::new(error(
            StatusCode::BAD_REQUEST,
            "Dataset does not exist",
        ))),
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, "dataset validation failed");
            Err(Box::new(error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to validate dataset",
            )))
        }
    }
}

/// Body for `PATCH /api/v1/manage/tenants/{tenant_id}/api-keys/{key_id}`.
/// Absent fields are left untouched.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[schema(as = ManageUpdateApiKeyRequest)]
pub(crate) struct UpdateApiKeyRequest {
    /// Replacement scope list (non-empty, drawn from the shared vocabulary).
    scopes: Option<Vec<String>>,
    /// Replacement dataset restriction.
    dataset_id: Option<String>,
}

#[utoipa::path(
    patch,
    path = "/api/v1/manage/tenants/{tenant_id}/api-keys/{key_id}",
    tag = "api-keys",
    operation_id = "manage_update_api_key",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("key_id" = String, Path, description = "API key identifier"),
    ),
    request_body = UpdateApiKeyRequest,
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "API key updated", body = ApiKeyResponse),
        (status = 400, description = "Dataset does not exist", body = ManageError),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 404, description = "API key not found", body = ManageError),
        (status = 409, description = "API key is revoked", body = ManageError),
        (status = 422, description = "Invalid or empty scopes", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn update_api_key<S: RouterState>(
    State(state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
    Path((tenant_id, key_id)): Path<(String, String)>,
    Json(request): Json<UpdateApiKeyRequest>,
) -> Response {
    if let Err((status, message)) = authorize_tenant(&ctx, &tenant_id) {
        return error(status, message);
    }
    if let Some(scopes) = &request.scopes
        && let Err(validation_error) = validate_scopes(scopes)
    {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            validation_error.to_string(),
        );
    }
    if let Some(dataset_id) = &request.dataset_id
        && let Err(response) = ensure_dataset_exists(&state, &tenant_id, dataset_id).await
    {
        return *response;
    }
    match state.catalog().get_api_key(&key_id).await {
        Ok(Some(record)) if record.tenant_id == tenant_id => {
            if record.revoked_at.is_some() {
                return error(StatusCode::CONFLICT, "API key is revoked");
            }
        }
        Ok(_) => return error(StatusCode::NOT_FOUND, "API key not found"),
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, key_id, "API key lookup failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to update API key",
            );
        }
    }
    // TODO(multi-dataset-key-restriction phase 2): `request.dataset_id`
    // becomes `dataset_ids`/`clear_dataset_restriction` on the request DTO,
    // constructing the full DatasetRestrictionUpdate tri-state; this
    // two-state bridge (matching today's COALESCE semantics exactly) goes
    // away once that lands.
    let dataset_update = match request.dataset_id.clone() {
        Some(id) => common::catalog::DatasetRestrictionUpdate::Set(vec![id]),
        None => common::catalog::DatasetRestrictionUpdate::Keep,
    };
    match state
        .catalog()
        .update_api_key_scopes(&key_id, request.scopes.as_deref(), dataset_update)
        .await
    {
        Ok(true) => {}
        Ok(false) => return error(StatusCode::CONFLICT, "API key is revoked"),
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, key_id, "API key update failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to update API key",
            );
        }
    }
    tracing::info!(actor_user_id = ?ctx.user_id, tenant_id, key_id, "API key scopes updated via UX");
    match state.catalog().get_api_key(&key_id).await {
        Ok(Some(key)) => Json(ApiKeyResponse {
            id: key.id,
            name: key.name,
            dataset_id: key.dataset_id,
            scopes: key.scopes,
            revoked: key.revoked_at.is_some(),
            created_at: key.created_at.to_rfc3339(),
        })
        .into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "API key not found"),
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, key_id, "API key reload failed");
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to update API key",
            )
        }
    }
}

#[utoipa::path(
    delete,
    path = "/api/v1/manage/tenants/{tenant_id}/api-keys/{key_id}",
    tag = "api-keys",
    operation_id = "manage_revoke_api_key",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("key_id" = String, Path, description = "API key identifier"),
    ),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 204, description = "API key revoked"),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 404, description = "API key not found", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn revoke_api_key<S: RouterState>(
    State(state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
    Path((tenant_id, key_id)): Path<(String, String)>,
) -> Response {
    if let Err((status, message)) = authorize_tenant(&ctx, &tenant_id) {
        return error(status, message);
    }
    match state.catalog().get_api_key(&key_id).await {
        Ok(Some(key)) if key.tenant_id == tenant_id => {}
        Ok(_) => return error(StatusCode::NOT_FOUND, "API key not found"),
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, key_id, "API key lookup failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to revoke API key",
            );
        }
    }
    match state.catalog().revoke_api_key(&key_id).await {
        Ok(()) => {
            tracing::info!(actor_user_id = ?ctx.user_id, tenant_id, key_id, "API key revoked via UX");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, key_id, "API key revocation failed");
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to revoke API key",
            )
        }
    }
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct MembershipResponse {
    user_id: String,
    email: String,
    role: MembershipRole,
}

#[utoipa::path(
    get,
    path = "/api/v1/manage/tenants/{tenant_id}/memberships",
    tag = "memberships",
    operation_id = "manage_list_memberships",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "List of memberships", body = [MembershipResponse]),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn list_memberships<S: RouterState>(
    State(state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
    Path(tenant_id): Path<String>,
) -> Response {
    if let Err((status, message)) = authorize_tenant(&ctx, &tenant_id) {
        return error(status, message);
    }
    let memberships = match state.catalog().list_members_for_tenant(&tenant_id).await {
        Ok(value) => value,
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, "membership listing failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to list memberships",
            );
        }
    };
    let mut response = Vec::with_capacity(memberships.len());
    for membership in memberships {
        let user = match state.catalog().get_user(&membership.user_id).await {
            Ok(Some(value)) => value,
            Ok(None) => continue,
            Err(catalog_error) => {
                tracing::error!(error = %catalog_error, tenant_id, "membership user lookup failed");
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Unable to list memberships",
                );
            }
        };
        response.push(MembershipResponse {
            user_id: user.id,
            email: user.email,
            role: membership.role,
        });
    }
    Json(response).into_response()
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub(crate) struct UpsertMembershipRequest {
    email: String,
    role: MembershipRole,
}

#[utoipa::path(
    put,
    path = "/api/v1/manage/tenants/{tenant_id}/memberships",
    tag = "memberships",
    operation_id = "manage_upsert_membership",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = UpsertMembershipRequest,
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Membership updated", body = MembershipResponse),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 404, description = "User not found", body = ManageError),
        (status = 409, description = "Last administrator cannot be demoted", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn upsert_membership<S: RouterState>(
    State(state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
    Path(tenant_id): Path<String>,
    Json(request): Json<UpsertMembershipRequest>,
) -> Response {
    if let Err((status, message)) = authorize_tenant(&ctx, &tenant_id) {
        return error(status, message);
    }
    let user = match state.catalog().get_user_by_email(&request.email).await {
        Ok(Some(value)) if value.disabled_at.is_none() => value,
        Ok(_) => return error(StatusCode::NOT_FOUND, "Active user not found"),
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, "membership user lookup failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to update membership",
            );
        }
    };
    if request.role != MembershipRole::Admin {
        let members = match state.catalog().list_members_for_tenant(&tenant_id).await {
            Ok(value) => value,
            Err(catalog_error) => {
                tracing::error!(error = %catalog_error, tenant_id, "administrator count failed");
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Unable to update membership",
                );
            }
        };
        if is_last_remaining_admin(&members, &user.id) {
            return error(
                StatusCode::CONFLICT,
                "The last tenant administrator cannot be demoted",
            );
        }
    }
    match state
        .catalog()
        .upsert_tenant_membership(&user.id, &tenant_id, request.role)
        .await
    {
        Ok(()) => {
            tracing::info!(actor_user_id = ?ctx.user_id, tenant_id, target_user_id = user.id, role = %request.role, "membership updated via UX");
            Json(MembershipResponse {
                user_id: user.id,
                email: user.email,
                role: request.role,
            })
            .into_response()
        }
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, "membership update failed");
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to update membership",
            )
        }
    }
}

#[utoipa::path(
    delete,
    path = "/api/v1/manage/tenants/{tenant_id}/memberships/{user_id}",
    tag = "memberships",
    operation_id = "manage_remove_membership",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("user_id" = String, Path, description = "User identifier"),
    ),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 204, description = "Membership removed"),
        (status = 400, description = "Cannot remove own membership", body = ManageError),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 409, description = "Last administrator cannot be removed", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn remove_membership<S: RouterState>(
    State(state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
    Path((tenant_id, user_id)): Path<(String, String)>,
) -> Response {
    if let Err((status, message)) = authorize_tenant(&ctx, &tenant_id) {
        return error(status, message);
    }
    if ctx.user_id.as_deref() == Some(user_id.as_str()) {
        return error(
            StatusCode::BAD_REQUEST,
            "You cannot remove your own active membership",
        );
    }
    let members = match state.catalog().list_members_for_tenant(&tenant_id).await {
        Ok(value) => value,
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, "administrator count failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to remove membership",
            );
        }
    };
    if is_last_remaining_admin(&members, &user_id) {
        return error(
            StatusCode::CONFLICT,
            "The last tenant administrator cannot be removed",
        );
    }
    match state
        .catalog()
        .remove_tenant_membership(&user_id, &tenant_id)
        .await
    {
        Ok(()) => {
            tracing::info!(actor_user_id = ?ctx.user_id, tenant_id, target_user_id = user_id, "membership removed via UX");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, user_id, "membership removal failed");
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to remove membership",
            )
        }
    }
}

/// One logical (client-visible, OTel-native) field, as registered in
/// [`common::schema::logical::LogicalSchema`].
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct ManageLogicalField {
    source: String,
    /// `resource` | `scope` | `record`, absent when the field isn't
    /// attribute-scoped (a plain `String` here, not `Option<AttributeLevel>`
    /// — utoipa emits a nullable `$ref` enum as `oneOf: [{type: null}, ref]`,
    /// which the progenitor-generated Rust SDK client can't parse).
    level: Option<String>,
    name: String,
    value_type: LogicalType,
    filterability: Filterability,
    kind: LogicalFieldKind,
    non_native: bool,
}

fn attribute_level_str(level: Option<AttributeLevel>) -> Option<String> {
    level.map(|level| {
        match level {
            AttributeLevel::Resource => "resource",
            AttributeLevel::Scope => "scope",
            AttributeLevel::Record => "record",
        }
        .to_string()
    })
}

/// One physical (storage) column, as resolved from `schemas.toml`.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct ManagePhysicalField {
    name: String,
    field_type: String,
    required: bool,
    computed: Option<String>,
    physical_only: bool,
}

/// One resolved table-schema version for one signal source.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct ManagePhysicalSchema {
    source: String,
    version: String,
    is_current: bool,
    description: String,
    partition_by: Vec<String>,
    fields: Vec<ManagePhysicalField>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct ManageSchemaResponse {
    logical_schema_version: String,
    logical: Vec<ManageLogicalField>,
    physical: Vec<ManagePhysicalSchema>,
}

fn physical_schemas_for_source(
    source: &str,
    versions: &std::collections::HashMap<
        String,
        common::schema::schema_parser::TableSchemaDefinition,
    >,
    current_version: &str,
) -> Vec<ManagePhysicalSchema> {
    let mut names: Vec<&String> = versions.keys().collect();
    names.sort();
    names
        .into_iter()
        .filter_map(|version| {
            SCHEMA_DEFINITIONS
                .resolve_table_schema(versions, version)
                .ok()
                .map(|resolved| ManagePhysicalSchema {
                    source: source.to_string(),
                    version: resolved.version.clone(),
                    is_current: resolved.version == current_version,
                    description: resolved.description,
                    partition_by: resolved.partition_by,
                    fields: resolved
                        .fields
                        .into_iter()
                        .map(|f| ManagePhysicalField {
                            name: f.name,
                            field_type: f.field_type,
                            required: f.required,
                            computed: f.computed,
                            physical_only: f.physical_only,
                        })
                        .collect(),
                })
        })
        .collect()
}

/// GET /api/v1/manage/schema
///
/// The registered logical (OTel-native, client-visible) schema and the
/// resolved physical (storage) schema for every version of every signal
/// source — read-only and not tenant-scoped (the schema is global, not
/// per-tenant). Readable by a tenant administrator, an instance
/// administrator, or an API key carrying `tenant:manage`.
#[utoipa::path(
    get,
    path = "/api/v1/manage/schema",
    tag = "schema",
    operation_id = "manage_get_schema",
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Logical and physical schema", body = ManageSchemaResponse),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required", body = ManageError),
    )
)]
pub(crate) async fn get_schema<S: RouterState>(
    State(_state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
) -> Response {
    if !can_manage(&ctx) {
        return error(StatusCode::FORBIDDEN, MANAGE_FORBIDDEN);
    }

    let mut logical: Vec<ManageLogicalField> = LogicalSchema::core()
        .fields()
        .map(|field| ManageLogicalField {
            source: field.id.source.clone(),
            level: attribute_level_str(field.id.level),
            name: field.id.name.clone(),
            value_type: field.value_type,
            filterability: field.filterability,
            kind: field.kind,
            non_native: field.non_native,
        })
        .collect();
    logical.sort_by(|a, b| (&a.source, &a.name).cmp(&(&b.source, &b.name)));

    let mut physical = Vec::new();
    physical.extend(physical_schemas_for_source(
        "traces",
        &SCHEMA_DEFINITIONS.traces,
        SCHEMA_DEFINITIONS.current_trace_version(),
    ));
    physical.extend(physical_schemas_for_source(
        "logs",
        &SCHEMA_DEFINITIONS.logs,
        &SCHEMA_DEFINITIONS.metadata.current_log_version,
    ));
    for (source, versions) in [
        ("metrics_gauge", &SCHEMA_DEFINITIONS.metrics_gauge),
        ("metrics_sum", &SCHEMA_DEFINITIONS.metrics_sum),
        ("metrics_histogram", &SCHEMA_DEFINITIONS.metrics_histogram),
    ] {
        physical.extend(physical_schemas_for_source(
            source,
            versions,
            &SCHEMA_DEFINITIONS.metadata.current_metric_version,
        ));
    }

    Json(ManageSchemaResponse {
        logical_schema_version: SCHEMA_DEFINITIONS.logical_schema_version().to_string(),
        logical,
        physical,
    })
    .into_response()
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn physical_schemas_for_source_resolves_every_version_sorted_and_flags_current() {
        let schemas = physical_schemas_for_source(
            "traces",
            &SCHEMA_DEFINITIONS.traces,
            SCHEMA_DEFINITIONS.current_trace_version(),
        );

        // schemas.toml registers physical-v1, physical-v2, and physical-v3
        // for traces (#1208: span_kind_number/status_code_number/dropped
        // counts).
        assert_eq!(schemas.len(), 3);
        let versions: Vec<&str> = schemas.iter().map(|s| s.version.as_str()).collect();
        assert_eq!(
            versions,
            vec!["physical-v1", "physical-v2", "physical-v3"],
            "sorted by version name"
        );

        let current: Vec<&str> = schemas
            .iter()
            .filter(|s| s.is_current)
            .map(|s| s.version.as_str())
            .collect();
        assert_eq!(current, vec![SCHEMA_DEFINITIONS.current_trace_version()]);

        for schema in &schemas {
            assert_eq!(schema.source, "traces");
            assert!(!schema.fields.is_empty());
            assert!(schema.fields.iter().any(|f| f.name == "trace_id"));
        }
    }

    #[test]
    fn get_schema_dto_covers_every_signal_source() {
        let logical: Vec<ManageLogicalField> = LogicalSchema::core()
            .fields()
            .map(|field| ManageLogicalField {
                source: field.id.source.clone(),
                level: attribute_level_str(field.id.level),
                name: field.id.name.clone(),
                value_type: field.value_type,
                filterability: field.filterability,
                kind: field.kind,
                non_native: field.non_native,
            })
            .collect();

        let sources: std::collections::HashSet<&str> =
            logical.iter().map(|f| f.source.as_str()).collect();
        assert!(sources.contains("traces"));
        assert!(sources.contains("logs"));
    }
}

#[cfg(test)]
mod key_scope_authorization_tests {
    //! `tenant:manage` API keys reach the management API for their own
    //! tenant; ingest-only, legacy-unscoped, and cross-tenant keys do not.

    use crate::{RouterAppState, create_router};
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use common::auth::{Authenticator, TENANT_MANAGE_SCOPE};
    use common::catalog::{Catalog, MembershipRole};
    use common::config::{ApiKeyConfig, AuthConfig, Configuration, DatasetConfig, TenantConfig};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    const MANAGE_KEY: &str = "sdbk_acme_manage";
    const INGEST_KEY: &str = "sdbk_acme_ingest";
    /// Config-backed key: predates scopes, i.e. unscoped/legacy.
    const LEGACY_KEY: &str = "acme-legacy-key";

    fn tenant(id: &str, key: &str) -> TenantConfig {
        TenantConfig {
            id: id.to_string(),
            slug: id.to_string(),
            name: format!("{id} Inc"),
            default_dataset: Some("production".to_string()),
            datasets: vec![DatasetConfig {
                id: "production".to_string(),
                slug: "production".to_string(),
                is_default: true,
                storage: None,
            }],
            api_keys: vec![ApiKeyConfig {
                key: key.to_string(),
                name: Some("legacy".to_string()),
            }],
            schema_config: None,
            limits: None,
        }
    }

    async fn scoped_key(catalog: &Catalog, tenant_id: &str, secret: &str, scopes: &[&str]) {
        let scopes: Vec<String> = scopes.iter().map(|s| s.to_string()).collect();
        catalog
            .upsert_scoped_api_key(
                tenant_id,
                &Authenticator::hash_api_key(secret),
                Some(secret),
                None,
                Some(&scopes),
                None,
            )
            .await
            .unwrap();
    }

    async fn test_app() -> axum::Router {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let config = Configuration {
            auth: AuthConfig {
                tenants: vec![tenant("acme", LEGACY_KEY), tenant("globex", "globex-key")],
                ..Default::default()
            },
            ..Default::default()
        };
        catalog.sync_config_tenants(&config.auth).await.unwrap();
        scoped_key(
            &catalog,
            "acme",
            MANAGE_KEY,
            &["traces:write", TENANT_MANAGE_SCOPE],
        )
        .await;
        scoped_key(&catalog, "acme", INGEST_KEY, &["traces:write"]).await;
        let hash = common::auth::hash_password("member password").unwrap();
        let user = catalog
            .create_user("member@example.com", Some("Member"), &hash, false)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&user.id, "acme", MembershipRole::Member)
            .await
            .unwrap();
        let admin = catalog
            .create_user("admin@example.com", Some("Admin"), &hash, false)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&admin.id, "acme", MembershipRole::Admin)
            .await
            .unwrap();
        create_router(RouterAppState::new(catalog, config))
    }

    async fn call(
        app: &axum::Router,
        key: &str,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", format!("Bearer {key}"))
            .header("x-tenant-id", "acme");
        let body = match body {
            Some(value) => {
                builder = builder.header("content-type", "application/json");
                Body::from(value.to_string())
            }
            None => Body::empty(),
        };
        let response = app
            .clone()
            .oneshot(builder.body(body).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        (status, json)
    }

    #[tokio::test]
    async fn tenant_manage_key_manages_datasets_keys_memberships_and_schema() {
        let app = test_app().await;

        // Datasets: list, create, delete.
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::GET,
            "/api/v1/manage/tenants/acme/datasets",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/manage/tenants/acme/datasets",
            Some(json!({ "name": "staging" })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::GET,
            "/api/v1/manage/tenants/acme/datasets",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body.as_array()
                .unwrap()
                .iter()
                .any(|d| d["name"] == "staging"),
            "{body}"
        );
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::DELETE,
            "/api/v1/manage/tenants/acme/datasets/staging",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");

        // API keys: list, create, update, revoke.
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::GET,
            "/api/v1/manage/tenants/acme/api-keys",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/manage/tenants/acme/api-keys",
            Some(json!({ "name": "ci", "scopes": ["traces:write"] })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let key_id = body["id"].as_str().unwrap().to_string();
        assert!(body["key"].as_str().unwrap().starts_with("sdbk_"));
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::PATCH,
            &format!("/api/v1/manage/tenants/acme/api-keys/{key_id}"),
            Some(json!({ "scopes": ["traces:write", "logs:write"] })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::DELETE,
            &format!("/api/v1/manage/tenants/acme/api-keys/{key_id}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");

        // Memberships: list, upsert, remove.
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::GET,
            "/api/v1/manage/tenants/acme/memberships",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::PUT,
            "/api/v1/manage/tenants/acme/memberships",
            Some(json!({ "email": "member@example.com", "role": "admin" })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let user_id = body["user_id"].as_str().unwrap().to_string();
        // Demote back so the removal is not blocked by the last-admin guard.
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::PUT,
            "/api/v1/manage/tenants/acme/memberships",
            Some(json!({ "email": "member@example.com", "role": "member" })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::DELETE,
            &format!("/api/v1/manage/tenants/acme/memberships/{user_id}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");

        // Schema view.
        let (status, body) =
            call(&app, MANAGE_KEY, Method::GET, "/api/v1/manage/schema", None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body["logical"].is_array(), "{body}");
    }

    #[tokio::test]
    async fn ingest_only_key_is_refused_on_every_management_endpoint() {
        let app = test_app().await;
        for (method, uri, body) in [
            (Method::GET, "/api/v1/manage/tenants/acme/datasets", None),
            (
                Method::POST,
                "/api/v1/manage/tenants/acme/datasets",
                Some(json!({ "name": "staging" })),
            ),
            (Method::GET, "/api/v1/manage/tenants/acme/api-keys", None),
            (Method::GET, "/api/v1/manage/tenants/acme/memberships", None),
            (Method::GET, "/api/v1/manage/schema", None),
        ] {
            let (status, json) = call(&app, INGEST_KEY, method.clone(), uri, body).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{method} {uri}: {json}");
            assert!(
                json["error"]
                    .as_str()
                    .unwrap_or("")
                    .contains("tenant:manage"),
                "{method} {uri}: error must name the required scope: {json}"
            );
        }
    }

    #[tokio::test]
    async fn legacy_unscoped_key_is_refused_because_management_is_opt_in() {
        let app = test_app().await;
        let (status, json) = call(
            &app,
            LEGACY_KEY,
            Method::GET,
            "/api/v1/manage/tenants/acme/datasets",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{json}");
        let (status, json) =
            call(&app, LEGACY_KEY, Method::GET, "/api/v1/manage/schema", None).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{json}");
    }

    #[tokio::test]
    async fn tenant_manage_key_cannot_cross_tenants() {
        let app = test_app().await;
        let (status, json) = call(
            &app,
            MANAGE_KEY,
            Method::GET,
            "/api/v1/manage/tenants/globex/datasets",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{json}");
        let (status, json) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/manage/tenants/globex/api-keys",
            Some(json!({ "name": "evil", "scopes": [TENANT_MANAGE_SCOPE] })),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{json}");
    }

    #[tokio::test]
    async fn tenant_manage_key_cannot_create_tenants() {
        let app = test_app().await;
        let (status, json) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/manage/tenants",
            Some(json!({ "id": "newco", "name": "NewCo" })),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{json}");
    }
}

#[cfg(test)]
mod dataset_provisioning_tests {
    //! Creating a dataset through the management API must provision its
    //! enabled signal tables synchronously, so it is usable before the
    //! writer's periodic reconciler ever ticks and without anyone calling the
    //! manual `POST .../tables/create` trigger.

    use crate::{RouterAppState, create_router};
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use common::CatalogManager;
    use common::auth::{Authenticator, TENANT_MANAGE_SCOPE};
    use common::catalog::Catalog;
    use common::config::{ApiKeyConfig, AuthConfig, Configuration, DatasetConfig, TenantConfig};
    use serde_json::json;
    use tower::ServiceExt;

    const MANAGE_KEY: &str = "sdbk_acme_manage";

    #[tokio::test]
    async fn creating_a_dataset_provisions_its_tables_immediately() {
        // A file-backed Iceberg catalog: the handler and this test's
        // assertion each build their own `CatalogManager`/connection pool,
        // and a named in-memory database only lives while a connection to it
        // is open (see `common::testing::TempCatalog`).
        let temp_catalog = common::testing::TempCatalog::new();
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let mut config = Configuration {
            auth: AuthConfig {
                tenants: vec![TenantConfig {
                    id: "acme".to_string(),
                    slug: "acme".to_string(),
                    name: "Acme".to_string(),
                    default_dataset: Some("production".to_string()),
                    datasets: vec![DatasetConfig {
                        id: "production".to_string(),
                        slug: "production".to_string(),
                        is_default: true,
                        storage: None,
                    }],
                    api_keys: vec![ApiKeyConfig {
                        key: "legacy".to_string(),
                        name: Some("legacy".to_string()),
                    }],
                    schema_config: None,
                    limits: None,
                }],
                ..Default::default()
            },
            ..Configuration::default()
        };
        config.schema.catalog_uri = temp_catalog.uri().to_string();
        catalog.sync_config_tenants(&config.auth).await.unwrap();
        catalog
            .upsert_scoped_api_key(
                "acme",
                &Authenticator::hash_api_key(MANAGE_KEY),
                Some(MANAGE_KEY),
                None,
                Some(&[TENANT_MANAGE_SCOPE.to_string()]),
                None,
            )
            .await
            .unwrap();

        let app = create_router(RouterAppState::new(catalog, config.clone()));
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/manage/tenants/acme/datasets")
            .header("authorization", format!("Bearer {MANAGE_KEY}"))
            .header("x-tenant-id", "acme")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "name": "staging" }).to_string()))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        // Without ever running the reconciler or the manual `tables/create`
        // trigger, the new dataset's tables must already exist.
        let manager = CatalogManager::new(config).await.unwrap();
        let tables = crate::endpoints::tabular_names_in(&manager, "acme", "staging").await;
        assert!(
            !tables.is_empty(),
            "expected the new dataset's signal tables to be provisioned immediately, found none"
        );
    }
}
