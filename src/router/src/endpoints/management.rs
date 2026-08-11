//! Session-authenticated tenant management endpoints used by the web UI.

use crate::RouterState;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use common::{
    auth::{Authenticator, TenantContext, TenantContextExtractor, validate_id},
    catalog::MembershipRole,
    schema::{
        SCHEMA_DEFINITIONS,
        logical::{AttributeLevel, Filterability, LogicalFieldKind, LogicalSchema, LogicalType},
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

const INGEST_SCOPES: [&str; 4] = [
    "metrics:write",
    "logs:write",
    "traces:write",
    "profiles:write",
];

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
            delete(revoke_api_key::<S>),
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

fn authorize_tenant(
    ctx: &TenantContext,
    tenant_id: &str,
) -> Result<(), (StatusCode, &'static str)> {
    if ctx.user_id.is_none() {
        return Err((
            StatusCode::FORBIDDEN,
            "Human user session required for management",
        ));
    }
    if ctx.tenant_id != tenant_id {
        return Err((StatusCode::FORBIDDEN, "Tenant context does not match path"));
    }
    if !ctx.can_manage_tenant() {
        return Err((StatusCode::FORBIDDEN, "Tenant administrator role required"));
    }
    Ok(())
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
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
        (status = 200, description = "List of datasets", body = [DatasetResponse]),
        (status = 403, description = "Forbidden", body = ManageError),
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
        (status = 201, description = "Dataset created", body = DatasetResponse),
        (status = 400, description = "Validation error", body = ManageError),
        (status = 403, description = "Forbidden", body = ManageError),
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
        (status = 204, description = "Dataset deleted"),
        (status = 403, description = "Forbidden", body = ManageError),
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
        (status = 200, description = "List of API keys", body = [ApiKeyResponse]),
        (status = 403, description = "Forbidden", body = ManageError),
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
        (status = 201, description = "API key created", body = ManageCreatedApiKey),
        (status = 400, description = "Validation error", body = ManageError),
        (status = 403, description = "Forbidden", body = ManageError),
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
    if request.scopes.is_empty()
        || request
            .scopes
            .iter()
            .any(|scope| !INGEST_SCOPES.contains(&scope.as_str()))
    {
        return error(
            StatusCode::BAD_REQUEST,
            "At least one valid ingestion scope is required",
        );
    }
    if let Some(dataset_id) = &request.dataset_id {
        let datasets = match state.catalog().get_datasets(&tenant_id).await {
            Ok(value) => value,
            Err(catalog_error) => {
                tracing::error!(error = %catalog_error, tenant_id, "dataset validation failed");
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Unable to validate dataset",
                );
            }
        };
        if !datasets.iter().any(|dataset| dataset.name == *dataset_id) {
            return error(StatusCode::BAD_REQUEST, "Dataset does not exist");
        }
    }
    let secret = format!("sdbk_{}", Uuid::new_v4().simple());
    let key_hash = Authenticator::hash_api_key(&secret);
    match state
        .catalog()
        .upsert_scoped_api_key(
            &tenant_id,
            &key_hash,
            request.name.as_deref(),
            request.dataset_id.as_deref(),
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
        (status = 204, description = "API key revoked"),
        (status = 403, description = "Forbidden", body = ManageError),
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
        (status = 200, description = "List of memberships", body = [MembershipResponse]),
        (status = 403, description = "Forbidden", body = ManageError),
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
        (status = 200, description = "Membership updated", body = MembershipResponse),
        (status = 403, description = "Forbidden", body = ManageError),
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
        let target_is_admin = members.iter().any(|membership| {
            membership.user_id == user.id && membership.role == MembershipRole::Admin
        });
        let admin_count = members
            .iter()
            .filter(|membership| membership.role == MembershipRole::Admin)
            .count();
        if target_is_admin && admin_count == 1 {
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
        (status = 204, description = "Membership removed"),
        (status = 400, description = "Cannot remove own membership", body = ManageError),
        (status = 403, description = "Forbidden", body = ManageError),
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
    let target_is_admin = members.iter().any(|membership| {
        membership.user_id == user_id && membership.role == MembershipRole::Admin
    });
    let admin_count = members
        .iter()
        .filter(|membership| membership.role == MembershipRole::Admin)
        .count();
    if target_is_admin && admin_count == 1 {
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
/// source — read-only, instance-admin-gated, and not tenant-scoped (the
/// schema is global, not per-tenant).
#[utoipa::path(
    get,
    path = "/api/v1/manage/schema",
    tag = "schema",
    operation_id = "manage_get_schema",
    responses(
        (status = 200, description = "Logical and physical schema", body = ManageSchemaResponse),
        (status = 403, description = "Instance administrator required", body = ManageError),
    )
)]
pub(crate) async fn get_schema<S: RouterState>(
    State(_state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
) -> Response {
    if !ctx.is_instance_admin {
        return error(StatusCode::FORBIDDEN, "Instance administrator required");
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

        // schemas.toml registers physical-v1 and physical-v2 for traces.
        assert_eq!(schemas.len(), 2);
        let versions: Vec<&str> = schemas.iter().map(|s| s.version.as_str()).collect();
        assert_eq!(
            versions,
            vec!["physical-v1", "physical-v2"],
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
