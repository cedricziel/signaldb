//! The tenant identity resource (`/api/v1/tenants`, `/api/v1/tenants/{id}`)
//! and human users (`/api/v1/users`).
//!
//! Merges what used to be two separate surfaces — a tenant's self-service
//! view of itself, and the instance-admin tenant registry — into one
//! handler per verb (issue #1561 follow-up: no `/manage`/`/manage/admin`
//! path segment names a privilege scope; every handler checks the caller's
//! privilege on the specific resource instead):
//!
//! - `GET /tenants`: an instance admin or the break-glass admin key lists
//!   every tenant; anyone else sees only the tenant(s) their own credential
//!   belongs to (its own tenant for an API key, every tenant the
//!   authenticated user holds a membership in for a session/OAuth token).
//! - `GET /tenants/{id}`: an instance admin or the admin key can read any
//!   tenant; anyone else only their own.
//! - `POST /tenants`, `PATCH /tenants/{id}`, `DELETE /tenants/{id}`:
//!   instance admin or the admin key only.
//! - `POST /users`: instance admin or the admin key only. `GET /users`:
//!   an instance admin (or the admin key) lists every user; anyone else
//!   sees only themselves.

use crate::RouterAppState;
use crate::endpoints::authz::require_instance_admin_or_admin_key;
use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use common::auth::{TenantContext, hash_password, validate_id};
use common::catalog::{MembershipRole, TenantRecord};
use serde::{Deserialize, Serialize};
use signaldb_api::{ApiError, CreateUserRequest, UpdateTenantRequest, UserResponse};
use std::str::FromStr;

pub fn router() -> Router<RouterAppState> {
    Router::new()
        .route("/tenants", get(list_tenants).post(create_tenant))
        .route(
            "/tenants/{tenant_id}",
            get(get_tenant).patch(update_tenant).delete(delete_tenant),
        )
        .route("/users", get(list_users).post(create_user))
}

/// Superset tenant response: the catalog-backed identity fields
/// (`id`/`name`/`default_dataset`/`source`/timestamps) every caller of the
/// old admin surface relied on, unchanged.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct TenantResponse {
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_dataset: Option<String>,
    pub source: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ListTenantsResponse {
    pub tenants: Vec<TenantResponse>,
}

fn tenant_record_to_response(record: TenantRecord) -> TenantResponse {
    TenantResponse {
        id: record.id,
        name: record.name,
        default_dataset: record.default_dataset,
        source: record.source,
        created_at: record.created_at,
        updated_at: record.updated_at,
    }
}

fn json_error(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(serde_json::to_value(ApiError::new(code, message)).unwrap()),
    )
        .into_response()
}

fn internal_error(e: impl std::fmt::Display) -> Response {
    json_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
        e.to_string(),
    )
}

/// Renders a [`require_instance_admin_or_admin_key`] failure as the
/// `signaldb_api::ApiError` shape this module's handlers declare, unlike
/// `management::error`'s bare `{"error"}` body.
fn admin_auth_error(status: StatusCode, message: &'static str) -> Response {
    let code = if status == StatusCode::FORBIDDEN {
        "forbidden"
    } else {
        "unauthorized"
    };
    json_error(status, code, message)
}

// ── Tenants ─────────────────────────────────────────────────────────────

/// List tenants: every tenant for an instance admin or the break-glass
/// admin key, otherwise only the tenant(s) the caller's own credential
/// belongs to.
#[utoipa::path(
    get,
    path = "/api/v1/tenants",
    tag = "tenants",
    operation_id = "list_tenants",
    summary = "List tenants visible to the caller",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    responses(
        (status = 200, description = "Tenants visible to the caller", body = ListTenantsResponse),
        (status = 401, description = "Missing or invalid credentials", body = ApiError),
    )
)]
pub async fn list_tenants(
    state: State<RouterAppState>,
    headers: HeaderMap,
    ctx: Option<Extension<TenantContext>>,
) -> impl IntoResponse {
    let ctx = ctx.map(|e| e.0);
    if is_admin(&state, &headers, ctx.as_ref()) {
        return match state.catalog().list_tenants().await {
            Ok(tenants) => Json(ListTenantsResponse {
                tenants: tenants.into_iter().map(tenant_record_to_response).collect(),
            })
            .into_response(),
            Err(e) => internal_error(e),
        };
    }
    let Some(ctx) = ctx else {
        return json_error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Missing or invalid credentials",
        );
    };
    // A human principal sees every tenant it holds a membership in; an
    // API-key-only credential (no `user_id`) sees just the one tenant it
    // authenticated against, matching the old self-service behavior.
    let tenant_ids: Vec<String> = if let Some(user_id) = &ctx.user_id {
        match state
            .catalog()
            .list_effective_memberships_for_user(user_id)
            .await
        {
            Ok(memberships) => memberships.into_iter().map(|m| m.tenant_id).collect(),
            Err(e) => return internal_error(e),
        }
    } else {
        vec![ctx.tenant_id.clone()]
    };
    let mut tenants = Vec::with_capacity(tenant_ids.len());
    for tenant_id in tenant_ids {
        match state.catalog().get_tenant(&tenant_id).await {
            Ok(Some(record)) => tenants.push(tenant_record_to_response(record)),
            Ok(None) => {}
            Err(e) => return internal_error(e),
        }
    }
    Json(ListTenantsResponse { tenants }).into_response()
}

/// Whether the caller is authorized to act on every tenant: an
/// instance-admin tenant credential, or the break-glass admin key.
fn is_admin(state: &RouterAppState, headers: &HeaderMap, ctx: Option<&TenantContext>) -> bool {
    if let Some(ctx) = ctx {
        return ctx.is_instance_admin;
    }
    require_instance_admin_or_admin_key(state, headers, None).is_ok()
}

/// Get a tenant by ID: any tenant for an instance admin or the admin key,
/// the caller's own tenant otherwise.
#[utoipa::path(
    get,
    path = "/api/v1/tenants/{tenant_id}",
    tag = "tenants",
    operation_id = "get_tenant",
    summary = "Get a tenant by ID",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    responses(
        (status = 200, description = "Tenant found", body = TenantResponse),
        (status = 401, description = "Missing or invalid credentials", body = ApiError),
        (status = 403, description = "Requested tenant does not match the authenticated tenant", body = ApiError),
        (status = 404, description = "Tenant not found", body = ApiError),
    )
)]
pub async fn get_tenant(
    state: State<RouterAppState>,
    headers: HeaderMap,
    ctx: Option<Extension<TenantContext>>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    let ctx = ctx.map(|e| e.0);
    if !is_admin(&state, &headers, ctx.as_ref()) {
        match &ctx {
            Some(ctx) if ctx.tenant_id == tenant_id => {}
            Some(_) => {
                return json_error(
                    StatusCode::FORBIDDEN,
                    "forbidden",
                    "Requested tenant does not match the authenticated tenant",
                );
            }
            None => {
                return json_error(
                    StatusCode::UNAUTHORIZED,
                    "unauthorized",
                    "Missing or invalid credentials",
                );
            }
        }
    }
    match state.catalog().get_tenant(&tenant_id).await {
        Ok(Some(record)) => Json(tenant_record_to_response(record)).into_response(),
        Ok(None) => json_error(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("Tenant '{tenant_id}' not found"),
        ),
        Err(e) => internal_error(e),
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct CreateTenantRequest {
    pub id: String,
    pub name: String,
    pub default_dataset: Option<String>,
}

#[utoipa::path(
    post,
    path = "/api/v1/tenants",
    tag = "tenants",
    operation_id = "create_tenant",
    summary = "Create a new tenant",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    request_body = CreateTenantRequest,
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 201, description = "Tenant created", body = TenantResponse),
        (status = 400, description = "Validation error", body = ApiError),
        (status = 401, description = "Missing or invalid credentials", body = ApiError),
        (status = 403, description = "Instance administrator required", body = ApiError),
        (status = 409, description = "Tenant already exists", body = ApiError),
        (status = 500, description = "Internal error", body = ApiError),
    )
)]
pub async fn create_tenant(
    State(state): State<RouterAppState>,
    headers: HeaderMap,
    ctx: Option<Extension<TenantContext>>,
    Json(request): Json<CreateTenantRequest>,
) -> Response {
    if let Err((status, message)) =
        require_instance_admin_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0))
    {
        return admin_auth_error(status, message);
    }
    let actor_user_id = ctx.as_ref().and_then(|e| e.0.user_id.clone());
    let tenant_id = match validate_id(&request.id) {
        Ok(value) => value,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, "validation_error", e.to_string()),
    };
    let default_dataset = match request.default_dataset.as_deref() {
        Some(value) => match validate_id(value) {
            Ok(value) => Some(value),
            Err(e) => {
                return json_error(StatusCode::BAD_REQUEST, "validation_error", e.to_string());
            }
        },
        None => None,
    };
    if request.name.trim().is_empty() {
        return json_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "Tenant name is required",
        );
    }
    if state
        .config()
        .auth
        .tenants
        .iter()
        .any(|tenant| tenant.id == tenant_id)
    {
        return json_error(
            StatusCode::CONFLICT,
            "conflict",
            "A configuration-backed tenant already uses this ID",
        );
    }
    match state.catalog().get_tenant(&tenant_id).await {
        Ok(Some(_)) => {
            return json_error(StatusCode::CONFLICT, "conflict", "Tenant already exists");
        }
        Ok(None) => {}
        Err(e) => {
            tracing::error!(error = %e, tenant_id, "tenant existence check failed");
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "Unable to create tenant",
            );
        }
    }
    // Tenant row and default dataset row in one transaction: a tenant whose
    // `default_dataset` has no row fails authentication closed, and creation
    // rejects an existing id with 409, so a retry could not repair it.
    if let Err(e) = state
        .catalog()
        .upsert_tenant_with_default_dataset(
            &tenant_id,
            request.name.trim(),
            default_dataset.as_deref(),
            "database",
        )
        .await
    {
        tracing::error!(error = %e, tenant_id, "tenant creation failed");
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "Unable to create tenant",
        );
    }
    if let Some(user_id) = &actor_user_id
        && let Err(e) = state
            .catalog()
            .upsert_tenant_membership(user_id, &tenant_id, MembershipRole::Admin)
            .await
    {
        tracing::error!(error = %e, tenant_id, user_id, "creator membership failed");
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "Tenant was created but creator access could not be recorded",
        );
    }
    tracing::info!(actor_user_id = ?actor_user_id, tenant_id, "tenant created");
    match state.catalog().get_tenant(&tenant_id).await {
        Ok(Some(record)) => {
            (StatusCode::CREATED, Json(tenant_record_to_response(record))).into_response()
        }
        _ => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "Tenant created but not found",
        ),
    }
}

#[utoipa::path(
    patch,
    path = "/api/v1/tenants/{tenant_id}",
    tag = "tenants",
    operation_id = "update_tenant",
    summary = "Update a tenant's name or default dataset",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = UpdateTenantRequest,
    responses(
        (status = 200, description = "Tenant updated", body = TenantResponse),
        (status = 400, description = "Validation error", body = ApiError),
        (status = 401, description = "Missing or invalid credentials", body = ApiError),
        (status = 403, description = "Config-sourced tenants cannot be modified, or instance administrator required", body = ApiError),
        (status = 404, description = "Tenant not found", body = ApiError),
    )
)]
pub async fn update_tenant(
    state: State<RouterAppState>,
    headers: HeaderMap,
    ctx: Option<Extension<TenantContext>>,
    Path(tenant_id): Path<String>,
    Json(request): Json<UpdateTenantRequest>,
) -> impl IntoResponse {
    if let Err((status, message)) =
        require_instance_admin_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0))
    {
        return admin_auth_error(status, message);
    }
    let existing = match state.catalog().get_tenant(&tenant_id).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            return json_error(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("Tenant '{tenant_id}' not found"),
            );
        }
        Err(e) => return internal_error(e),
    };
    if existing.source == "config" {
        return json_error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "Config-sourced tenants cannot be modified via API",
        );
    }
    let name = match request.name.as_deref() {
        Some(name) if name.trim().is_empty() => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "validation_error",
                "Tenant name cannot be empty",
            );
        }
        Some(name) => name.trim(),
        None => &existing.name,
    };
    let default_dataset = match request.default_dataset.as_deref() {
        Some(ds) => match validate_id(ds) {
            Ok(value) => Some(value),
            Err(e) => {
                return json_error(StatusCode::BAD_REQUEST, "validation_error", e.to_string());
            }
        },
        None => existing.default_dataset.clone(),
    };
    let default_dataset = default_dataset.as_deref();
    if let Err(e) = state
        .catalog()
        .upsert_tenant_with_default_dataset(&tenant_id, name, default_dataset, "database")
        .await
    {
        tracing::error!(tenant_id = %tenant_id, error = %e, "tenant update failed");
        return internal_error(e);
    }
    match state.catalog().get_tenant(&tenant_id).await {
        Ok(Some(record)) => Json(tenant_record_to_response(record)).into_response(),
        _ => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "Tenant updated but not found",
        ),
    }
}

#[utoipa::path(
    delete,
    path = "/api/v1/tenants/{tenant_id}",
    tag = "tenants",
    operation_id = "delete_tenant",
    summary = "Delete a database-sourced tenant",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    responses(
        (status = 204, description = "Tenant deleted"),
        (status = 401, description = "Missing or invalid credentials", body = ApiError),
        (status = 403, description = "Config-sourced tenants cannot be deleted, or instance administrator required", body = ApiError),
        (status = 404, description = "Tenant not found", body = ApiError),
    )
)]
pub async fn delete_tenant(
    state: State<RouterAppState>,
    headers: HeaderMap,
    ctx: Option<Extension<TenantContext>>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    if let Err((status, message)) =
        require_instance_admin_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0))
    {
        return admin_auth_error(status, message);
    }
    match state.catalog().get_tenant(&tenant_id).await {
        Ok(Some(record)) => {
            if record.source == "config" {
                return json_error(
                    StatusCode::FORBIDDEN,
                    "forbidden",
                    "Config-sourced tenants cannot be deleted via API",
                );
            }
        }
        Ok(None) => {
            return json_error(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("Tenant '{tenant_id}' not found"),
            );
        }
        Err(e) => return internal_error(e),
    }
    match state.catalog().delete_tenant(&tenant_id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => json_error(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("Tenant '{tenant_id}' not found or is config-sourced"),
        ),
        Err(e) => internal_error(e),
    }
}

// ── Users ───────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ListUsersResponse {
    pub users: Vec<UserResponse>,
}

/// List human users: every user for an instance admin or the admin key;
/// otherwise just the caller's own user record, when the credential is a
/// human session (an API-key-only credential has no user to list).
#[utoipa::path(
    get,
    path = "/api/v1/users",
    tag = "users",
    operation_id = "list_users",
    summary = "List human users",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    responses(
        (status = 200, description = "Users visible to the caller", body = ListUsersResponse),
        (status = 401, description = "Missing or invalid credentials", body = ApiError),
        (status = 500, description = "Internal error", body = ApiError),
    )
)]
pub async fn list_users(
    state: State<RouterAppState>,
    headers: HeaderMap,
    ctx: Option<Extension<TenantContext>>,
) -> impl IntoResponse {
    let ctx = ctx.map(|e| e.0);
    if is_admin(&state, &headers, ctx.as_ref()) {
        return match state.catalog().list_users().await {
            Ok(users) => Json(ListUsersResponse {
                users: users.into_iter().map(user_record_to_response).collect(),
            })
            .into_response(),
            Err(e) => internal_error(e),
        };
    }
    let Some(ctx) = ctx else {
        return json_error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Missing or invalid credentials",
        );
    };
    let Some(user_id) = &ctx.user_id else {
        // An API-key-only credential has no user of its own to list.
        return Json(ListUsersResponse { users: Vec::new() }).into_response();
    };
    match state.catalog().get_user(user_id).await {
        Ok(Some(user)) => Json(ListUsersResponse {
            users: vec![user_record_to_response(user)],
        })
        .into_response(),
        Ok(None) => Json(ListUsersResponse { users: Vec::new() }).into_response(),
        Err(e) => internal_error(e),
    }
}

fn user_record_to_response(user: common::catalog::UserRecord) -> UserResponse {
    UserResponse {
        id: user.id,
        email: user.email,
        display_name: user.display_name,
        instance_admin: user.is_instance_admin,
        created_at: user.created_at,
    }
}

/// Create a human user and grant an initial tenant membership.
#[utoipa::path(
    post,
    path = "/api/v1/users",
    tag = "users",
    operation_id = "create_user",
    summary = "Create a user outright and grant an initial tenant membership",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    request_body = CreateUserRequest,
    responses(
        (status = 201, description = "User created", body = UserResponse),
        (status = 400, description = "Validation error", body = ApiError),
        (status = 401, description = "Missing or invalid credentials", body = ApiError),
        (status = 403, description = "Instance administrator required", body = ApiError),
        (status = 404, description = "Tenant not found", body = ApiError),
        (status = 409, description = "User already exists", body = ApiError),
    )
)]
pub async fn create_user(
    state: State<RouterAppState>,
    headers: HeaderMap,
    ctx: Option<Extension<TenantContext>>,
    Json(request): Json<CreateUserRequest>,
) -> impl IntoResponse {
    if let Err((status, message)) =
        require_instance_admin_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0))
    {
        return admin_auth_error(status, message);
    }
    if request.email.trim().is_empty() {
        return json_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "email must not be empty",
        );
    }
    if request.password.len() < 12 {
        return json_error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "password must be at least 12 characters",
        );
    }
    let role = match MembershipRole::from_str(&request.role) {
        Ok(role) => role,
        Err(_) => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "validation_error",
                format!(
                    "invalid role '{}': expected admin, member, or viewer",
                    request.role
                ),
            );
        }
    };
    match state.catalog().get_tenant(&request.tenant).await {
        Ok(None) => {
            return json_error(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("Tenant '{}' not found", request.tenant),
            );
        }
        Err(e) => return internal_error(e),
        Ok(Some(_)) => {}
    }
    let password = request.password.clone();
    let password_hash = match tokio::task::spawn_blocking(move || hash_password(&password)).await {
        Ok(Ok(hash)) => hash,
        Ok(Err(e)) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                format!("password hashing failed: {e}"),
            );
        }
        Err(e) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                format!("password hashing task failed: {e}"),
            );
        }
    };
    let user = match state
        .catalog()
        .create_user(
            &request.email,
            request.display_name.as_deref(),
            Some(&password_hash),
            request.instance_admin,
        )
        .await
    {
        Ok(user) => user,
        Err(e) => {
            let msg = e.to_string();
            let (status, code) = if msg.contains("UNIQUE") || msg.contains("duplicate") {
                (StatusCode::CONFLICT, "conflict")
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal_error")
            };
            return json_error(status, code, msg);
        }
    };
    if let Err(e) = state
        .catalog()
        .upsert_tenant_membership(&user.id, &request.tenant, role)
        .await
    {
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("user created but membership grant failed: {e}"),
        );
    }
    (StatusCode::CREATED, Json(user_record_to_response(user))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RouterAppState;
    use crate::create_router;
    use axum::body::Body;
    use axum::http::Request;
    use common::catalog::Catalog;
    use common::config::{AuthConfig, Configuration};
    use tower::ServiceExt;

    const ADMIN_KEY: &str = "sk-admin-test-key";

    async fn router_with_database_tenant() -> Router {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_tenant_with_default_dataset("acme", "Acme", Some("prod"), "database")
            .await
            .unwrap();
        let config = Configuration {
            auth: AuthConfig {
                admin_api_key: Some(ADMIN_KEY.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        create_router(RouterAppState::new(catalog, config))
    }

    async fn patch_tenant(app: &Router, body: serde_json::Value) -> axum::response::Response {
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/tenants/acme")
                    .header("authorization", format!("Bearer {ADMIN_KEY}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn update_tenant_rejects_a_whitespace_only_name_and_trims_an_accepted_one() {
        let app = router_with_database_tenant().await;

        let res = patch_tenant(&app, serde_json::json!({"name": "   "})).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        let res = patch_tenant(&app, serde_json::json!({"name": "  Acme Two  "})).await;
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["name"], "Acme Two");
    }

    #[tokio::test]
    async fn update_tenant_validates_and_keeps_default_dataset() {
        let app = router_with_database_tenant().await;

        // An invalid `default_dataset` is rejected as a validation error.
        let res = patch_tenant(&app, serde_json::json!({"default_dataset": "Not Valid!"})).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "validation_error");

        // Omitting it keeps the existing default_dataset.
        let res = patch_tenant(&app, serde_json::json!({"name": "Acme"})).await;
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["default_dataset"], "prod");
    }
}
