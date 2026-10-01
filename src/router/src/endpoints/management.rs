//! Tenant-scoped resource endpoints: datasets, API keys, and memberships
//! under `/api/v1/tenants/{tenant_id}/...`.
//!
//! Reachable by the web UI (session/OAuth principals with the tenant-admin
//! role or instance-admin flag), by automation holding an API key that
//! carries the `tenant:manage` scope, or by the break-glass admin key with
//! no tenant at all (see [`crate::endpoints::authz`]).

use crate::RouterAppState;
use crate::endpoints::authz::authorize_tenant_or_admin_key;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get},
};
use chrono::{DateTime, Utc};
use common::{
    auth::{Authenticator, TenantContext, validate_id, validate_scopes},
    catalog::{GrantSource, MembershipRole},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

pub fn router() -> Router<RouterAppState> {
    Router::new()
        .route(
            "/tenants/{tenant_id}/datasets",
            get(list_datasets).post(create_dataset),
        )
        .route(
            "/tenants/{tenant_id}/datasets/{dataset_name}",
            delete(delete_dataset),
        )
        .route(
            "/tenants/{tenant_id}/api-keys",
            get(list_api_keys).post(create_api_key),
        )
        .route(
            "/tenants/{tenant_id}/api-keys/{key_id}",
            delete(revoke_api_key).patch(update_api_key),
        )
        .route(
            "/tenants/{tenant_id}/memberships",
            get(list_memberships).put(upsert_membership),
        )
        .route(
            "/tenants/{tenant_id}/memberships/{user_id}",
            delete(remove_membership),
        )
}

/// Error returned when the principal may not manage the tenant.
const MANAGE_FORBIDDEN: &str = "Tenant administrator role or tenant:manage scope required";

/// Error returned when the principal's own credential carries a dataset-set
/// restriction (D9): such a credential gets the data-plane access it asked
/// for and nothing that administers the tenant, regardless of role or scope.
const MANAGE_FORBIDDEN_DATASET_RESTRICTED: &str =
    "A dataset-restricted credential cannot use the tenant management API";

/// Whether `ctx` may manage its own tenant: a human principal (session or
/// OAuth token) with the tenant-admin role or instance-admin flag, or an API
/// key explicitly scoped with `tenant:manage`. Legacy unscoped keys do NOT
/// qualify — see [`TenantContext::can_manage_via_key`].
fn can_manage(ctx: &TenantContext) -> bool {
    (ctx.user_id.is_some() && ctx.can_manage_tenant()) || ctx.can_manage_via_key()
}

/// Whether `ctx`'s own credential carries a non-empty dataset-set
/// restriction (D9). Checked generically here rather than per-endpoint so no
/// management operation can accidentally skip it: a dataset-restricted API
/// key or OAuth grant is refused the entire management API, regardless of
/// `tenant:manage` or an admin role.
fn is_dataset_restricted(ctx: &TenantContext) -> bool {
    ctx.api_key_dataset_ids
        .as_ref()
        .is_some_and(|ids| !ids.is_empty())
}

pub(crate) fn authorize_tenant(
    ctx: &TenantContext,
    tenant_id: &str,
) -> Result<(), (StatusCode, &'static str)> {
    if ctx.tenant_id != tenant_id {
        return Err((StatusCode::FORBIDDEN, "Tenant context does not match path"));
    }
    if is_dataset_restricted(ctx) {
        return Err((StatusCode::FORBIDDEN, MANAGE_FORBIDDEN_DATASET_RESTRICTED));
    }
    if !can_manage(ctx) {
        return Err((StatusCode::FORBIDDEN, MANAGE_FORBIDDEN));
    }
    Ok(())
}

pub(crate) fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}

/// Whether `ctx` is the instance-admin principal — stricter than
/// [`can_manage`]/[`authorize_tenant`], which also accept a tenant-scoped
/// `tenant:manage` grant. Used by the handful of operations too privileged
/// for that: creating a tenant, and attaching a GitHub App installation
/// with no ownership check to fall back on (see
/// `endpoints::github::attach_github_installation`'s doc comment).
pub(crate) fn authorize_instance_admin(
    ctx: &TenantContext,
) -> Result<(), (StatusCode, &'static str)> {
    if !ctx.is_instance_admin {
        return Err((StatusCode::FORBIDDEN, "Instance administrator required"));
    }
    Ok(())
}

/// Whether `target_user_id` is the tenant's sole remaining administrator —
/// used to block demoting or removing the last admin membership.
///
/// Only counts `local` grants: an `oidc_mapping`-sourced admin membership is
/// derived from IdP group mapping and can vanish the moment that mapping
/// changes, so it must not be allowed to "protect" — or be double-counted
/// alongside — a `local` admin row.
fn is_last_remaining_admin(
    members: &[common::catalog::TenantMembershipRecord],
    target_user_id: &str,
) -> bool {
    fn is_local_admin(membership: &common::catalog::TenantMembershipRecord) -> bool {
        membership.role == MembershipRole::Admin && membership.granted_by == GrantSource::Local
    }
    let target_is_admin = members
        .iter()
        .any(|membership| membership.user_id == target_user_id && is_local_admin(membership));
    let admin_count = members.iter().filter(|m| is_local_admin(m)).count();
    target_is_admin && admin_count == 1
}

/// Error response body for the tenant-scoped resource endpoints.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct ManageError {
    error: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[schema(as = ManageDatasetResponse)]
pub(crate) struct DatasetResponse {
    id: String,
    name: String,
}

#[utoipa::path(
    get,
    path = "/api/v1/tenants/{tenant_id}/datasets",
    tag = "datasets",
    operation_id = "list_datasets",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "List of datasets", body = [DatasetResponse]),
        (status = 401, description = "Missing or invalid credentials", body = ManageError),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn list_datasets(
    State(state): State<RouterAppState>,
    headers: axum::http::HeaderMap,
    ctx: Option<axum::Extension<TenantContext>>,
    Path(tenant_id): Path<String>,
) -> Response {
    if let Err(response) =
        authorize_tenant_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0), &tenant_id)
            .await
    {
        return *response;
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
    path = "/api/v1/tenants/{tenant_id}/datasets",
    tag = "datasets",
    operation_id = "create_dataset",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = CreateDatasetRequest,
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 201, description = "Dataset created", body = DatasetResponse),
        (status = 400, description = "Validation error", body = ManageError),
        (status = 401, description = "Missing or invalid credentials", body = ManageError),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 409, description = "Unable to create dataset", body = ManageError),
    )
)]
pub(crate) async fn create_dataset(
    State(state): State<RouterAppState>,
    headers: axum::http::HeaderMap,
    ctx: Option<axum::Extension<TenantContext>>,
    Path(tenant_id): Path<String>,
    Json(request): Json<CreateDatasetRequest>,
) -> Response {
    if let Err(response) =
        authorize_tenant_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0), &tenant_id)
            .await
    {
        return *response;
    }
    let actor_user_id = ctx.as_ref().and_then(|e| e.0.user_id.clone());
    let name = match validate_id(&request.name) {
        Ok(value) => value,
        Err(error_value) => return error(StatusCode::BAD_REQUEST, error_value.to_string()),
    };
    match state.catalog().create_dataset(&tenant_id, &name).await {
        Ok(id) => {
            tracing::info!(actor_user_id = ?actor_user_id, tenant_id, dataset = name, "dataset created via UX");
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
    path = "/api/v1/tenants/{tenant_id}/datasets/{dataset_name}",
    tag = "datasets",
    operation_id = "delete_dataset",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("dataset_name" = String, Path, description = "Dataset name"),
    ),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 204, description = "Dataset deleted"),
        (status = 401, description = "Missing or invalid credentials", body = ManageError),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 404, description = "Dataset not found", body = ManageError),
        (status = 409, description = "Dataset cannot be deleted", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn delete_dataset(
    State(state): State<RouterAppState>,
    headers: axum::http::HeaderMap,
    ctx: Option<axum::Extension<TenantContext>>,
    Path((tenant_id, dataset_name)): Path<(String, String)>,
) -> Response {
    if let Err(response) =
        authorize_tenant_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0), &tenant_id)
            .await
    {
        return *response;
    }
    let actor_user_id = ctx.as_ref().and_then(|e| e.0.user_id.clone());
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
            tracing::info!(actor_user_id = ?actor_user_id, tenant_id, dataset = dataset_name, "dataset deleted via UX");
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

/// `dataset_ids` mirrors [`signaldb_api::CreateApiKeyRequest`] (D1a): omitted
/// or `null` creates an unrestricted key, a non-empty array restricts it,
/// and an explicit empty array or duplicate name is rejected. The legacy
/// singular `dataset_id` field is not accepted — `deny_unknown_fields`
/// rejects a request body still sending it, rather than silently dropping
/// it and creating an unrestricted key when the caller asked for a
/// restricted one.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[schema(as = ManageCreateApiKeyRequest)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateApiKeyRequest {
    name: Option<String>,
    #[schema(min_items = 1)]
    dataset_ids: Option<Vec<String>>,
    #[schema(min_items = 1)]
    allowed_origins: Option<Vec<String>>,
    scopes: Vec<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[schema(as = ManageApiKeyResponse)]
pub(crate) struct ApiKeyResponse {
    id: String,
    name: Option<String>,
    dataset_ids: Option<Vec<String>>,
    allowed_origins: Option<Vec<String>>,
    scopes: Option<Vec<String>>,
    revoked: bool,
    created_at: DateTime<Utc>,
}

/// 201 response body for API key creation via the management API.
///
/// Fields mirror the previous `json!` body exactly (including `null` for
/// absent `name`), preserving the wire format.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct ManageCreatedApiKey {
    id: String,
    key: String,
    name: Option<String>,
    dataset_ids: Option<Vec<String>>,
    allowed_origins: Option<Vec<String>>,
    scopes: Vec<String>,
}

#[utoipa::path(
    get,
    path = "/api/v1/tenants/{tenant_id}/api-keys",
    tag = "api-keys",
    operation_id = "list_api_keys",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "List of API keys", body = [ApiKeyResponse]),
        (status = 401, description = "Missing or invalid credentials", body = ManageError),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn list_api_keys(
    State(state): State<RouterAppState>,
    headers: axum::http::HeaderMap,
    ctx: Option<axum::Extension<TenantContext>>,
    Path(tenant_id): Path<String>,
) -> Response {
    if let Err(response) =
        authorize_tenant_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0), &tenant_id)
            .await
    {
        return *response;
    }
    match state.catalog().list_api_keys(&tenant_id).await {
        Ok(keys) => Json(
            keys.into_iter()
                .map(|key| ApiKeyResponse {
                    id: key.id,
                    name: key.name,
                    dataset_ids: key.dataset_ids,
                    allowed_origins: key.allowed_origins,
                    scopes: key.scopes,
                    revoked: key.revoked_at.is_some(),
                    created_at: key.created_at,
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
    path = "/api/v1/tenants/{tenant_id}/api-keys",
    tag = "api-keys",
    operation_id = "create_api_key",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = CreateApiKeyRequest,
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 201, description = "API key created", body = ManageCreatedApiKey),
        (status = 400, description = "Dataset does not exist", body = ManageError),
        (status = 422, description = "Invalid or empty scopes", body = ManageError),
        (status = 401, description = "Missing or invalid credentials", body = ManageError),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 409, description = "Unable to create API key", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn create_api_key(
    State(state): State<RouterAppState>,
    headers: axum::http::HeaderMap,
    ctx: Option<axum::Extension<TenantContext>>,
    Path(tenant_id): Path<String>,
    Json(request): Json<CreateApiKeyRequest>,
) -> Response {
    if let Err(response) =
        authorize_tenant_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0), &tenant_id)
            .await
    {
        return *response;
    }
    let actor_user_id = ctx.as_ref().and_then(|e| e.0.user_id.clone());
    if let Err(validation_error) = validate_scopes(&request.scopes) {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            validation_error.to_string(),
        );
    }
    let dataset_ids = match common::catalog::validate_create_dataset_ids(request.dataset_ids) {
        Ok(ids) => ids,
        Err(validation_error) => {
            return error(StatusCode::BAD_REQUEST, validation_error.to_string());
        }
    };
    if let Some(ids) = &dataset_ids
        && let Err(response) =
            validate_dataset_restriction_gate_and_membership(&state, &tenant_id, ids).await
    {
        return *response;
    }
    let allowed_origins =
        match common::catalog::validate_create_allowed_origins(request.allowed_origins) {
            Ok(origins) => origins,
            Err(validation_error) => {
                return error(StatusCode::BAD_REQUEST, validation_error.to_string());
            }
        };
    let secret = format!("sdbk_{}", Uuid::new_v4().simple());
    let key_hash = Authenticator::hash_api_key(&secret);
    match state
        .catalog()
        .upsert_scoped_api_key(
            &tenant_id,
            &key_hash,
            request.name.as_deref(),
            dataset_ids.as_deref(),
            allowed_origins.as_deref(),
            Some(&request.scopes),
            actor_user_id.as_deref(),
        )
        .await
    {
        Ok(id) => {
            tracing::info!(actor_user_id = ?actor_user_id, tenant_id, key_id = id, "scoped API key created via UX");
            let response = ManageCreatedApiKey {
                id,
                key: secret,
                name: request.name,
                dataset_ids,
                allowed_origins,
                scopes: request.scopes,
            };
            (StatusCode::CREATED, Json(response)).into_response()
        }
        Err(catalog_error) => {
            tracing::warn!(error = %catalog_error, tenant_id, "API key creation failed");
            error(StatusCode::CONFLICT, "Unable to create API key")
        }
    }
}

/// `400` unless every dataset in `dataset_ids` exists in the tenant.
async fn ensure_datasets_exist(
    state: &RouterAppState,
    tenant_id: &str,
    dataset_ids: &[String],
) -> Result<(), Box<Response>> {
    match state.catalog().get_datasets(tenant_id).await {
        Ok(datasets) => {
            let existing: std::collections::HashSet<&str> = datasets
                .iter()
                .map(|dataset| dataset.name.as_str())
                .collect();
            match dataset_ids
                .iter()
                .find(|dataset_id| !existing.contains(dataset_id.as_str()))
            {
                None => Ok(()),
                Some(_) => Err(Box::new(error(
                    StatusCode::BAD_REQUEST,
                    "Dataset does not exist",
                ))),
            }
        }
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, "dataset validation failed");
            Err(Box::new(error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to validate dataset",
            )))
        }
    }
}

/// `400` when `dataset_ids` names two or more datasets while the
/// mixed-version rollout gate (`[auth].dataset_restriction_rollout_complete`)
/// is not yet `true` (D2), or when any element does not belong to
/// `tenant_id`.
async fn validate_dataset_restriction_gate_and_membership(
    state: &RouterAppState,
    tenant_id: &str,
    dataset_ids: &[String],
) -> Result<(), Box<Response>> {
    common::catalog::check_dataset_restriction_rollout_gate(
        dataset_ids,
        state.config().auth.dataset_restriction_rollout_complete,
    )
    .map_err(|message| Box::new(error(StatusCode::BAD_REQUEST, message)))?;
    ensure_datasets_exist(state, tenant_id, dataset_ids).await
}

/// Body for `PATCH /api/v1/tenants/{tenant_id}/api-keys/{key_id}`.
/// Absent fields are left untouched. `dataset_ids`/`clear_dataset_restriction`
/// mirror [`signaldb_api::UpdateApiKeyRequest`] (D1a); the legacy singular
/// `dataset_id` field is rejected via `deny_unknown_fields` rather than
/// silently dropped.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[schema(as = ManageUpdateApiKeyRequest)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateApiKeyRequest {
    /// Replacement scope list (non-empty, drawn from the shared vocabulary).
    scopes: Option<Vec<String>>,
    /// Replacement dataset set (non-empty; an explicit empty array is
    /// rejected). Omitted/`null` leaves the current restriction unchanged.
    /// Mutually exclusive with `clear_dataset_restriction: true`.
    #[schema(min_items = 1)]
    dataset_ids: Option<Vec<String>>,
    /// Clear an existing dataset restriction back to unrestricted. Must not
    /// be combined with a non-empty `dataset_ids` in the same request.
    #[serde(default)]
    clear_dataset_restriction: bool,
    /// Replacement allowed-origins set (non-empty; an explicit empty array
    /// is rejected). Omitted/`null` leaves the current restriction
    /// unchanged. Mutually exclusive with `clear_allowed_origins: true`.
    #[schema(min_items = 1)]
    allowed_origins: Option<Vec<String>>,
    /// Clear an existing allowed-origins restriction back to unrestricted.
    /// Must not be combined with a non-empty `allowed_origins` in the same
    /// request.
    #[serde(default)]
    clear_allowed_origins: bool,
}

#[utoipa::path(
    patch,
    path = "/api/v1/tenants/{tenant_id}/api-keys/{key_id}",
    tag = "api-keys",
    operation_id = "update_api_key",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("key_id" = String, Path, description = "API key identifier"),
    ),
    request_body = UpdateApiKeyRequest,
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "API key updated", body = ApiKeyResponse),
        (status = 400, description = "Dataset does not exist", body = ManageError),
        (status = 401, description = "Missing or invalid credentials", body = ManageError),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 404, description = "API key not found", body = ManageError),
        (status = 409, description = "API key is revoked", body = ManageError),
        (status = 422, description = "Invalid or empty scopes", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn update_api_key(
    State(state): State<RouterAppState>,
    headers: axum::http::HeaderMap,
    ctx: Option<axum::Extension<TenantContext>>,
    Path((tenant_id, key_id)): Path<(String, String)>,
    Json(request): Json<UpdateApiKeyRequest>,
) -> Response {
    if let Err(response) =
        authorize_tenant_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0), &tenant_id)
            .await
    {
        return *response;
    }
    let actor_user_id = ctx.as_ref().and_then(|e| e.0.user_id.clone());
    if let Some(scopes) = &request.scopes
        && let Err(validation_error) = validate_scopes(scopes)
    {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            validation_error.to_string(),
        );
    }
    let dataset_update = match common::catalog::DatasetRestrictionUpdate::from_request(
        request.dataset_ids,
        request.clear_dataset_restriction,
    ) {
        Ok(update) => update,
        Err(validation_error) => {
            return error(StatusCode::BAD_REQUEST, validation_error.to_string());
        }
    };
    if let common::catalog::DatasetRestrictionUpdate::Set(ids) = &dataset_update
        && let Err(response) =
            validate_dataset_restriction_gate_and_membership(&state, &tenant_id, ids).await
    {
        return *response;
    }
    let origin_update = match common::catalog::OriginRestrictionUpdate::from_request(
        request.allowed_origins,
        request.clear_allowed_origins,
    ) {
        Ok(update) => update,
        Err(validation_error) => {
            return error(StatusCode::BAD_REQUEST, validation_error.to_string());
        }
    };
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
    match state
        .catalog()
        .update_api_key_scopes(
            &key_id,
            request.scopes.as_deref(),
            dataset_update,
            origin_update,
        )
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
    tracing::info!(actor_user_id = ?actor_user_id, tenant_id, key_id, "API key scopes updated via UX");
    match state.catalog().get_api_key(&key_id).await {
        Ok(Some(key)) => {
            let response = ApiKeyResponse {
                id: key.id,
                name: key.name,
                dataset_ids: key.dataset_ids,
                allowed_origins: key.allowed_origins,
                scopes: key.scopes,
                revoked: key.revoked_at.is_some(),
                created_at: key.created_at,
            };
            Json(response).into_response()
        }
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
    path = "/api/v1/tenants/{tenant_id}/api-keys/{key_id}",
    tag = "api-keys",
    operation_id = "revoke_api_key",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("key_id" = String, Path, description = "API key identifier"),
    ),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 204, description = "API key revoked"),
        (status = 401, description = "Missing or invalid credentials", body = ManageError),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 404, description = "API key not found", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn revoke_api_key(
    State(state): State<RouterAppState>,
    headers: axum::http::HeaderMap,
    ctx: Option<axum::Extension<TenantContext>>,
    Path((tenant_id, key_id)): Path<(String, String)>,
) -> Response {
    if let Err(response) =
        authorize_tenant_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0), &tenant_id)
            .await
    {
        return *response;
    }
    let actor_user_id = ctx.as_ref().and_then(|e| e.0.user_id.clone());
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
            tracing::info!(actor_user_id = ?actor_user_id, tenant_id, key_id, "API key revoked via UX");
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
    /// `"local"` (granted via this API/CLI/MCP) or `"oidc_mapping"` (synced
    /// from an OIDC group claim, change: oidc-login). A local and a mapped
    /// row can coexist for the same user, yielding two response rows that
    /// differ only by this field — the UI keys on `user_id` + `granted_by`.
    granted_by: String,
}

#[utoipa::path(
    get,
    path = "/api/v1/tenants/{tenant_id}/memberships",
    tag = "memberships",
    operation_id = "list_memberships",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "List of memberships", body = [MembershipResponse]),
        (status = 401, description = "Missing or invalid credentials", body = ManageError),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 404, description = "Tenant not found", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn list_memberships(
    State(state): State<RouterAppState>,
    headers: axum::http::HeaderMap,
    ctx: Option<axum::Extension<TenantContext>>,
    Path(tenant_id): Path<String>,
) -> Response {
    if let Err(response) =
        authorize_tenant_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0), &tenant_id)
            .await
    {
        return *response;
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
            granted_by: membership.granted_by.to_string(),
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
    path = "/api/v1/tenants/{tenant_id}/memberships",
    tag = "memberships",
    operation_id = "upsert_membership",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = UpsertMembershipRequest,
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Membership updated", body = MembershipResponse),
        (status = 401, description = "Missing or invalid credentials", body = ManageError),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 404, description = "User or tenant not found", body = ManageError),
        (status = 409, description = "Last administrator cannot be demoted", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn upsert_membership(
    State(state): State<RouterAppState>,
    headers: axum::http::HeaderMap,
    ctx: Option<axum::Extension<TenantContext>>,
    Path(tenant_id): Path<String>,
    Json(request): Json<UpsertMembershipRequest>,
) -> Response {
    if let Err(response) =
        authorize_tenant_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0), &tenant_id)
            .await
    {
        return *response;
    }
    let actor_user_id = ctx.as_ref().and_then(|e| e.0.user_id.clone());
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
            tracing::info!(actor_user_id = ?actor_user_id, tenant_id, target_user_id = user.id, role = %request.role, "membership updated via UX");
            Json(MembershipResponse {
                user_id: user.id,
                email: user.email,
                role: request.role,
                granted_by: GrantSource::Local.to_string(),
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
    path = "/api/v1/tenants/{tenant_id}/memberships/{user_id}",
    tag = "memberships",
    operation_id = "remove_membership",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("user_id" = String, Path, description = "User identifier"),
    ),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 204, description = "Membership removed"),
        (status = 400, description = "Cannot remove own membership", body = ManageError),
        (status = 401, description = "Missing or invalid credentials", body = ManageError),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 404, description = "Tenant not found", body = ManageError),
        (status = 409, description = "Last administrator cannot be removed", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn remove_membership(
    State(state): State<RouterAppState>,
    headers: axum::http::HeaderMap,
    ctx: Option<axum::Extension<TenantContext>>,
    Path((tenant_id, user_id)): Path<(String, String)>,
) -> Response {
    if let Err(response) =
        authorize_tenant_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0), &tenant_id)
            .await
    {
        return *response;
    }
    let actor_user_id = ctx.as_ref().and_then(|e| e.0.user_id.clone());
    if actor_user_id.as_deref() == Some(user_id.as_str()) {
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
            tracing::info!(actor_user_id = ?actor_user_id, tenant_id, target_user_id = user_id, "membership removed via UX");
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

#[cfg(test)]
mod last_remaining_admin_tests {
    use super::*;
    use common::catalog::TenantMembershipRecord;

    fn membership(
        user_id: &str,
        role: MembershipRole,
        granted_by: GrantSource,
    ) -> TenantMembershipRecord {
        TenantMembershipRecord {
            user_id: user_id.to_string(),
            tenant_id: "acme".to_string(),
            role,
            granted_by,
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn another_local_admin_allows_removal() {
        let members = vec![
            membership("alice", MembershipRole::Admin, GrantSource::Local),
            membership("bob", MembershipRole::Admin, GrantSource::Local),
        ];
        assert!(!is_last_remaining_admin(&members, "alice"));
    }

    #[test]
    fn sole_local_admin_is_protected() {
        let members = vec![
            membership("alice", MembershipRole::Admin, GrantSource::Local),
            membership("bob", MembershipRole::Member, GrantSource::Local),
        ];
        assert!(is_last_remaining_admin(&members, "alice"));
    }

    #[test]
    fn mapped_admin_row_alone_does_not_satisfy_the_guard() {
        // `alice` holds only an oidc_mapping-sourced admin row; she is not a
        // *local* admin, so she isn't "the last remaining admin" and removing
        // any other membership must not be blocked by her presence.
        let members = vec![membership(
            "alice",
            MembershipRole::Admin,
            GrantSource::OidcMapping,
        )];
        assert!(!is_last_remaining_admin(&members, "alice"));
    }

    #[test]
    fn local_admin_is_not_protected_by_a_coexisting_mapped_admin_row() {
        // `alice` has both a `local` admin row and an `oidc_mapping` admin
        // row for the same tenant (change: oidc-login design decision 5).
        // Only the local row should count toward the "last remaining admin"
        // guard, so removing it must not be blocked by the mapped row.
        let members = vec![
            membership("alice", MembershipRole::Admin, GrantSource::Local),
            membership("alice", MembershipRole::Admin, GrantSource::OidcMapping),
        ];
        assert!(is_last_remaining_admin(&members, "alice"));
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
            .create_user("member@example.com", Some("Member"), Some(&hash), false)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&user.id, "acme", MembershipRole::Member)
            .await
            .unwrap();
        let admin = catalog
            .create_user("admin@example.com", Some("Admin"), Some(&hash), false)
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
            "/api/v1/tenants/acme/datasets",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/acme/datasets",
            Some(json!({ "name": "staging" })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::GET,
            "/api/v1/tenants/acme/datasets",
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
            "/api/v1/tenants/acme/datasets/staging",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");

        // API keys: list, create, update, revoke.
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::GET,
            "/api/v1/tenants/acme/api-keys",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/acme/api-keys",
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
            &format!("/api/v1/tenants/acme/api-keys/{key_id}"),
            Some(json!({ "scopes": ["traces:write", "logs:write"] })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::DELETE,
            &format!("/api/v1/tenants/acme/api-keys/{key_id}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");

        // Memberships: list, upsert, remove.
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::GET,
            "/api/v1/tenants/acme/memberships",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::PUT,
            "/api/v1/tenants/acme/memberships",
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
            "/api/v1/tenants/acme/memberships",
            Some(json!({ "email": "member@example.com", "role": "member" })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::DELETE,
            &format!("/api/v1/tenants/acme/memberships/{user_id}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");

        // Schema view.
        let (status, body) = call(&app, MANAGE_KEY, Method::GET, "/api/v1/schema", None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body["logical"].is_array(), "{body}");
    }

    #[tokio::test]
    async fn list_memberships_includes_granted_by() {
        let app = test_app().await;
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::GET,
            "/api/v1/tenants/acme/memberships",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows = body.as_array().unwrap();
        assert!(!rows.is_empty());
        assert!(
            rows.iter().all(|row| row["granted_by"] == "local"),
            "every membership from `upsert_tenant_membership` must be granted_by=local: {body}"
        );
    }

    /// Task 4.3/4.4 (change: oidc-login): a `local` and an `oidc_mapping`
    /// row can coexist for the same `(user_id, tenant_id)`. The handler must
    /// not collapse them — it emits one `MembershipResponse` per row, so the
    /// same `user_id` appears twice, distinguished only by `granted_by`.
    #[tokio::test]
    async fn list_memberships_shows_coexisting_local_and_mapped_rows_for_same_user() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let config = Configuration {
            auth: AuthConfig {
                tenants: vec![tenant("acme", LEGACY_KEY)],
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
        let hash = common::auth::hash_password("member password").unwrap();
        let user = catalog
            .create_user("dual@example.com", Some("Dual"), Some(&hash), false)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&user.id, "acme", MembershipRole::Viewer)
            .await
            .unwrap();
        catalog
            .sync_oidc_memberships(&user.id, &[("acme".to_string(), MembershipRole::Member)])
            .await
            .unwrap();
        let app = create_router(RouterAppState::new(catalog, config));

        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::GET,
            "/api/v1/tenants/acme/memberships",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows: Vec<&Value> = body
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["user_id"] == user.id)
            .collect();
        assert_eq!(
            rows.len(),
            2,
            "expected one row per (user, granted_by): {body}"
        );
        let sources: std::collections::HashSet<&str> = rows
            .iter()
            .map(|row| row["granted_by"].as_str().unwrap())
            .collect();
        assert!(sources.contains("local"));
        assert!(sources.contains("oidc_mapping"));
    }

    #[tokio::test]
    async fn ingest_only_key_is_refused_on_every_management_endpoint() {
        let app = test_app().await;
        for (method, uri, body) in [
            (Method::GET, "/api/v1/tenants/acme/datasets", None),
            (
                Method::POST,
                "/api/v1/tenants/acme/datasets",
                Some(json!({ "name": "staging" })),
            ),
            (Method::GET, "/api/v1/tenants/acme/api-keys", None),
            (Method::GET, "/api/v1/tenants/acme/memberships", None),
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
            "/api/v1/tenants/acme/datasets",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{json}");
    }

    #[tokio::test]
    async fn tenant_manage_key_cannot_cross_tenants() {
        let app = test_app().await;
        let (status, json) = call(
            &app,
            MANAGE_KEY,
            Method::GET,
            "/api/v1/tenants/globex/datasets",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{json}");
        let (status, json) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/globex/api-keys",
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
            "/api/v1/tenants",
            Some(json!({ "id": "newco", "name": "NewCo" })),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{json}");
    }
}

/// The break-glass `admin_api_key`, with no `X-Tenant-ID` at all, reaches
/// the tenant-scoped API-key/dataset admin surface for any tenant (issue
/// #1561 part 2) — the same surface `key_scope_authorization_tests` above
/// exercises with a `tenant:manage`-scoped key.
#[cfg(test)]
mod admin_key_bypass_for_tenant_scoped_admin_surface_tests {
    use crate::{RouterAppState, create_router};
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use common::catalog::Catalog;
    use common::config::Configuration;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    const ADMIN_KEY: &str = "sk-admin-break-glass";

    async fn test_app() -> axum::Router {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let mut config = Configuration::default();
        config.auth.admin_api_key = Some(ADMIN_KEY.to_string());
        catalog
            .upsert_tenant_with_default_dataset("acme", "Acme Corp", Some("production"), "database")
            .await
            .unwrap();
        create_router(RouterAppState::new(catalog, config))
    }

    /// A bearer-only request: no `X-Tenant-ID`, no session cookie.
    async fn call(
        app: &axum::Router,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", format!("Bearer {ADMIN_KEY}"));
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
    async fn admin_key_manages_datasets_for_any_tenant() {
        let app = test_app().await;

        let (status, body) = call(&app, Method::GET, "/api/v1/tenants/acme/datasets", None).await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let (status, body) = call(
            &app,
            Method::POST,
            "/api/v1/tenants/acme/datasets",
            Some(json!({ "name": "staging" })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");

        let (status, body) = call(
            &app,
            Method::DELETE,
            "/api/v1/tenants/acme/datasets/staging",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    }

    #[tokio::test]
    async fn admin_key_manages_api_keys_for_any_tenant() {
        let app = test_app().await;

        let (status, body) = call(
            &app,
            Method::POST,
            "/api/v1/tenants/acme/api-keys",
            Some(json!({ "name": "ci", "scopes": ["traces:write"] })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let key_id = body["id"].as_str().unwrap().to_string();

        let (status, body) = call(&app, Method::GET, "/api/v1/tenants/acme/api-keys", None).await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let (status, body) = call(
            &app,
            Method::PATCH,
            &format!("/api/v1/tenants/acme/api-keys/{key_id}"),
            Some(json!({ "scopes": ["traces:write", "logs:write"] })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let (status, body) = call(
            &app,
            Method::DELETE,
            &format!("/api/v1/tenants/acme/api-keys/{key_id}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    }

    /// Memberships are a tenant-scoped resource like datasets and API keys:
    /// the break-glass admin key reaches them for any tenant too.
    #[tokio::test]
    async fn admin_key_manages_memberships_for_any_tenant() {
        let app = test_app().await;
        let (status, body) =
            call(&app, Method::GET, "/api/v1/tenants/acme/memberships", None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    /// An unknown tenant returns 404 on the admin-key path rather than
    /// silently operating on nothing.
    #[tokio::test]
    async fn admin_key_gets_404_for_unknown_tenant() {
        let app = test_app().await;
        let (status, body) = call(&app, Method::GET, "/api/v1/tenants/ghost/datasets", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    }
}

#[cfg(test)]
mod dataset_restriction_tests {
    //! Multi-dataset-key-restriction, phase 2: the management API's
    //! `dataset_ids`/`clear_dataset_restriction` create/update surface, the
    //! `[auth].dataset_restriction_rollout_complete` gate, and D9 (a
    //! dataset-restricted credential is refused the entire management API).

    use crate::{RouterAppState, create_router};
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use common::auth::{Authenticator, TENANT_MANAGE_SCOPE};
    use common::catalog::Catalog;
    use common::config::{AuthConfig, Configuration};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    const MANAGE_KEY: &str = "sdbk_acme_manage";
    /// Carries `tenant:manage` *and* a dataset restriction — D9 must refuse
    /// this combination outright, regardless of the scope.
    const RESTRICTED_MANAGE_KEY: &str = "sdbk_acme_restricted_manage";

    async fn scoped_key(
        catalog: &Catalog,
        tenant_id: &str,
        secret: &str,
        scopes: &[&str],
        dataset_ids: Option<&[String]>,
    ) {
        let scopes: Vec<String> = scopes.iter().map(|s| s.to_string()).collect();
        catalog
            .upsert_scoped_api_key(
                tenant_id,
                &Authenticator::hash_api_key(secret),
                Some(secret),
                dataset_ids,
                None,
                Some(&scopes),
                None,
            )
            .await
            .unwrap();
    }

    async fn test_app(rollout_complete: bool) -> (axum::Router, Catalog) {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let config = Configuration {
            auth: AuthConfig {
                dataset_restriction_rollout_complete: rollout_complete,
                ..Default::default()
            },
            ..Default::default()
        };
        catalog
            .upsert_tenant("acme", "Acme Corp", Some("production"), "database")
            .await
            .unwrap();
        catalog.create_dataset("acme", "production").await.unwrap();
        catalog.create_dataset("acme", "staging").await.unwrap();
        scoped_key(
            &catalog,
            "acme",
            MANAGE_KEY,
            &["traces:write", TENANT_MANAGE_SCOPE],
            None,
        )
        .await;
        scoped_key(
            &catalog,
            "acme",
            RESTRICTED_MANAGE_KEY,
            &["traces:write", TENANT_MANAGE_SCOPE],
            Some(&["production".to_string()]),
        )
        .await;
        (
            create_router(RouterAppState::new(catalog.clone(), config)),
            catalog,
        )
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
    async fn create_and_update_accept_dataset_ids_and_authenticate_within_the_set() {
        let (app, catalog) = test_app(true).await;

        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/acme/api-keys",
            Some(json!({ "name": "multi", "scopes": ["traces:read"], "dataset_ids": ["production", "staging"] })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(
            body["dataset_ids"],
            serde_json::json!(["production", "staging"])
        );
        let raw_key = body["key"].as_str().unwrap().to_string();
        let key_id = body["id"].as_str().unwrap().to_string();

        let authenticator = Authenticator::new(AuthConfig::default(), std::sync::Arc::new(catalog));
        assert!(
            authenticator
                .authenticate(&raw_key, "acme", Some("production"))
                .await
                .is_ok()
        );
        assert!(
            authenticator
                .authenticate(&raw_key, "acme", Some("other"))
                .await
                .is_err()
        );

        // Clear the restriction back to unrestricted.
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::PATCH,
            &format!("/api/v1/tenants/acme/api-keys/{key_id}"),
            Some(json!({ "clear_dataset_restriction": true })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["dataset_ids"], Value::Null);
    }

    /// D8: the deprecated singular `dataset_id` field is removed entirely
    /// from API-key response bodies — not `null`, but absent from the JSON
    /// object (task 2.1).
    #[tokio::test]
    async fn api_key_response_omits_deprecated_dataset_id_key() {
        let (app, _catalog) = test_app(true).await;

        let (status, created) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/acme/api-keys",
            Some(json!({ "scopes": ["traces:read"] })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{created}");
        assert!(
            !created.as_object().unwrap().contains_key("dataset_id"),
            "create response must not carry the removed dataset_id field: {created}"
        );

        let (status, list) = call(
            &app,
            MANAGE_KEY,
            Method::GET,
            "/api/v1/tenants/acme/api-keys",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{list}");
        let keys = list.as_array().unwrap();
        assert!(!keys.is_empty());
        assert!(
            !keys[0].as_object().unwrap().contains_key("dataset_id"),
            "list response must not carry the removed dataset_id field: {}",
            keys[0]
        );
    }

    #[tokio::test]
    async fn create_and_update_reject_empty_dataset_ids_and_contradictory_clear() {
        let (app, _catalog) = test_app(true).await;

        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/acme/api-keys",
            Some(json!({ "scopes": ["traces:read"], "dataset_ids": [] })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/acme/api-keys",
            Some(json!({ "name": "k", "scopes": ["traces:read"], "dataset_ids": ["production"] })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let key_id = body["id"].as_str().unwrap().to_string();

        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::PATCH,
            &format!("/api/v1/tenants/acme/api-keys/{key_id}"),
            Some(json!({ "dataset_ids": [], "clear_dataset_restriction": true })),
        )
        .await;
        // Both combinations are invalid; either is an acceptable rejection
        // reason, but the request must not succeed.
        assert_ne!(status, StatusCode::OK, "{body}");

        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::PATCH,
            &format!("/api/v1/tenants/acme/api-keys/{key_id}"),
            Some(json!({ "dataset_ids": ["staging"], "clear_dataset_restriction": true })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }

    #[tokio::test]
    async fn create_and_update_accept_allowed_origins() {
        let (app, _catalog) = test_app(true).await;

        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/acme/api-keys",
            Some(
                json!({ "name": "origin-restricted", "scopes": ["traces:read"], "allowed_origins": ["https://example.com"] }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(
            body["allowed_origins"],
            serde_json::json!(["https://example.com"])
        );
        let key_id = body["id"].as_str().unwrap().to_string();

        // Replace the restriction.
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::PATCH,
            &format!("/api/v1/tenants/acme/api-keys/{key_id}"),
            Some(json!({ "allowed_origins": ["https://other.example"] })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["allowed_origins"],
            serde_json::json!(["https://other.example"])
        );

        // Clear the restriction back to unrestricted.
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::PATCH,
            &format!("/api/v1/tenants/acme/api-keys/{key_id}"),
            Some(json!({ "clear_allowed_origins": true })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["allowed_origins"], Value::Null);
    }

    #[tokio::test]
    async fn create_and_update_reject_empty_allowed_origins_and_contradictory_clear() {
        let (app, _catalog) = test_app(true).await;

        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/acme/api-keys",
            Some(json!({ "scopes": ["traces:read"], "allowed_origins": [] })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/acme/api-keys",
            Some(
                json!({ "name": "k", "scopes": ["traces:read"], "allowed_origins": ["https://example.com"] }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let key_id = body["id"].as_str().unwrap().to_string();

        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::PATCH,
            &format!("/api/v1/tenants/acme/api-keys/{key_id}"),
            Some(
                json!({ "allowed_origins": ["https://other.example"], "clear_allowed_origins": true }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }

    #[tokio::test]
    async fn create_and_update_reject_legacy_dataset_id_field() {
        let (app, _catalog) = test_app(true).await;

        let (status, _) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/acme/api-keys",
            Some(json!({ "scopes": ["traces:read"], "dataset_id": "production" })),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/acme/api-keys",
            Some(json!({ "name": "k", "scopes": ["traces:read"] })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let key_id = body["id"].as_str().unwrap().to_string();

        let (status, _) = call(
            &app,
            MANAGE_KEY,
            Method::PATCH,
            &format!("/api/v1/tenants/acme/api-keys/{key_id}"),
            Some(json!({ "dataset_id": "production" })),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn dataset_ids_must_belong_to_the_target_tenant() {
        let (app, _catalog) = test_app(true).await;
        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/acme/api-keys",
            Some(json!({ "scopes": ["traces:read"], "dataset_ids": ["production", "ghost"] })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }

    #[tokio::test]
    async fn multi_dataset_restriction_is_gated_by_the_rollout_flag() {
        let (app, _catalog) = test_app(false).await;

        let (status, body) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/acme/api-keys",
            Some(json!({ "scopes": ["traces:read"], "dataset_ids": ["production", "staging"] })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("dataset_restriction_rollout_complete"),
            "{body}"
        );

        // Single-dataset and unrestricted are unaffected by the flag.
        let (status, _) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/acme/api-keys",
            Some(json!({ "scopes": ["traces:read"], "dataset_ids": ["production"] })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, _) = call(
            &app,
            MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/acme/api-keys",
            Some(json!({ "scopes": ["traces:read"] })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    /// D9: a key carrying `tenant:manage` *and* a dataset restriction is
    /// refused by every management-API endpoint, asserted here against
    /// `manage_create_api_key` and `manage_delete_dataset` specifically —
    /// enforcement is generic (one place), not per-endpoint.
    #[tokio::test]
    async fn dataset_restricted_manage_key_is_refused_on_create_api_key_and_delete_dataset() {
        let (app, _catalog) = test_app(true).await;

        let (status, json) = call(
            &app,
            RESTRICTED_MANAGE_KEY,
            Method::POST,
            "/api/v1/tenants/acme/api-keys",
            Some(json!({ "name": "evil", "scopes": ["traces:read"] })),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{json}");

        let (status, json) = call(
            &app,
            RESTRICTED_MANAGE_KEY,
            Method::DELETE,
            "/api/v1/tenants/acme/datasets/staging",
            None,
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
                None,
                Some(&[TENANT_MANAGE_SCOPE.to_string()]),
                None,
            )
            .await
            .unwrap();

        let app = create_router(RouterAppState::new(catalog, config.clone()));
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/tenants/acme/datasets")
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
