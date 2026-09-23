//! Instance-admin endpoints (`/api/v1/manage/admin/*`).
//!
//! Operations here have no tenant-scoped equivalent under `/api/v1/manage`:
//! listing/reading/updating/deleting *any* tenant, and creating a user
//! outright rather than within a tenant's own membership. Access control does
//! not depend on the path prefix: every handler below checks the caller's
//! privilege itself, via [`require_instance_admin_or_admin_key`], rather than
//! relying on a router-level layer.
//!
//! Two credentials authorize a caller here: a normal tenant credential
//! (session or API key) whose `TenantContext.is_instance_admin` is set, or
//! the break-glass `[auth].admin_api_key` bearer with no tenant at all.
//! `create_router`'s `auth_layer` (see `lib.rs`) lets the latter past the
//! normal per-tenant header requirements — every other `/api/v1` route
//! requires `X-Tenant-ID` for any bearer credential — so it reaches here as
//! a request with no `TenantContext` attached; the handlers below are what
//! actually decides whether that bearer is the configured admin key.
//!
//! Moved from the removed `/api/v1/admin` API (issue #1561); handler bodies
//! are otherwise unchanged from their `admin.rs` originals.

use crate::RouterAppState;
use crate::endpoints::management::error;
use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
    routing::{delete, get, put},
};
use common::auth::{Authenticator, TenantContext, hash_password};
use common::catalog::MembershipRole;
use signaldb_api::{
    ApiError, CreateUserRequest, ListTenantsResponse, TenantResponse, UpdateTenantRequest,
    UserResponse,
};
use std::str::FromStr;

pub fn router() -> Router<RouterAppState> {
    Router::new()
        .route("/tenants", get(list_tenants))
        .route("/tenants/{tenant_id}", get(get_tenant))
        .route("/tenants/{tenant_id}", put(update_tenant))
        .route("/tenants/{tenant_id}", delete(delete_tenant))
        .route("/users", axum::routing::post(create_user))
}

/// Authorizes a `/api/v1/manage/admin/*` caller. `ctx` is `Some` whenever
/// `auth_layer` resolved a normal tenant credential (session, API key,
/// OAuth token); such a caller is authorized only when it carries the
/// instance-admin flag — the same flag
/// [`common::auth::admin_auth_middleware`] grants a session authenticated
/// via `authenticate_instance_admin_session`, so the auth behaviour the old
/// `/api/v1/admin` API offered through a session is unchanged here.
///
/// `ctx` is `None` for the break-glass path: `auth_layer` lets a
/// tenant-less request through untouched when its bearer token matches the
/// configured `admin_api_key`, so a `None` here is authorized precisely
/// when `headers` still carries that same matching bearer (re-checked here
/// rather than trusted from the layer, since the layer's job was only to
/// let the request past the tenant requirement, not to authorize it) — a
/// request with neither a resolved tenant context nor a matching admin key
/// should not reach this module at all under normal routing, but is
/// rejected with `401` defensively if it does.
///
/// Called explicitly at the top of every handler in this module rather than
/// via a path-keyed layer, so privilege never depends on where a route
/// happens to be mounted. Also reused by
/// [`crate::endpoints::management::create_tenant`]: tenant creation has no
/// duplicate under `/api/v1/manage/admin` (it already lives at
/// `POST /api/v1/manage/tenants`), but is exactly as instance-admin-only as
/// every operation in this module, so the CLI's `admin tenant create` needs
/// the same break-glass path as `admin tenant list/get/update/delete`.
pub(crate) fn require_instance_admin_or_admin_key(
    state: &RouterAppState,
    headers: &HeaderMap,
    ctx: Option<&TenantContext>,
) -> Result<(), Box<Response>> {
    if let Some(ctx) = ctx {
        return if ctx.is_instance_admin {
            Ok(())
        } else {
            Err(Box::new(error(
                StatusCode::FORBIDDEN,
                "Instance administrator required",
            )))
        };
    }
    let bearer = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if let (Some(expected), Some(token)) = (state.config().auth.admin_api_key.as_deref(), bearer)
        && Authenticator::hash_api_key(token) == Authenticator::hash_api_key(expected)
    {
        return Ok(());
    }
    Err(Box::new(error(
        StatusCode::UNAUTHORIZED,
        "Missing administrator credentials",
    )))
}

// ── Tenant endpoints ────────────────────────────────────────────────────

/// List all tenants
#[utoipa::path(
    get,
    path = "/api/v1/manage/admin/tenants",
    tag = "tenants",
    operation_id = "manage_admin_list_tenants",
    summary = "List every tenant on the instance",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    responses(
        (status = 200, description = "List of tenants", body = ListTenantsResponse),
        (status = 403, description = "Instance administrator required", body = ApiError),
    )
)]
pub async fn list_tenants(
    state: State<RouterAppState>,
    headers: HeaderMap,
    ctx: Option<Extension<TenantContext>>,
) -> impl IntoResponse {
    if let Err(response) =
        require_instance_admin_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0))
    {
        return *response;
    }
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

/// Get a tenant by ID
#[utoipa::path(
    get,
    path = "/api/v1/manage/admin/tenants/{tenant_id}",
    tag = "tenants",
    operation_id = "manage_admin_get_tenant",
    summary = "Get any tenant by ID",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    responses(
        (status = 200, description = "Tenant found", body = TenantResponse),
        (status = 403, description = "Instance administrator required", body = ApiError),
        (status = 404, description = "Tenant not found", body = ApiError),
    )
)]
pub async fn get_tenant(
    state: State<RouterAppState>,
    headers: HeaderMap,
    ctx: Option<Extension<TenantContext>>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    if let Err(response) =
        require_instance_admin_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0))
    {
        return *response;
    }
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
    path = "/api/v1/manage/admin/tenants/{tenant_id}",
    tag = "tenants",
    operation_id = "manage_admin_update_tenant",
    summary = "Update any tenant's name or default dataset",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = UpdateTenantRequest,
    responses(
        (status = 200, description = "Tenant updated", body = TenantResponse),
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
    if let Err(response) =
        require_instance_admin_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0))
    {
        return *response;
    }
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
    path = "/api/v1/manage/admin/tenants/{tenant_id}",
    tag = "tenants",
    operation_id = "manage_admin_delete_tenant",
    summary = "Delete any database-sourced tenant",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    responses(
        (status = 204, description = "Tenant deleted"),
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
    if let Err(response) =
        require_instance_admin_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0))
    {
        return *response;
    }
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

// ── Helpers ─────────────────────────────────────────────────────────────

fn tenant_record_to_response(record: common::catalog::TenantRecord) -> TenantResponse {
    TenantResponse {
        id: record.id,
        name: record.name,
        default_dataset: record.default_dataset,
        source: record.source,
        created_at: record.created_at,
        updated_at: record.updated_at,
    }
}

// ── User endpoints ──────────────────────────────────────────────────────

/// Create a human user and grant an initial tenant membership.
#[utoipa::path(
    post,
    path = "/api/v1/manage/admin/users",
    tag = "users",
    operation_id = "manage_admin_create_user",
    summary = "Create a user outright and grant an initial tenant membership",
    security(("bearerAuth" = []), ("adminApiKey" = [])),
    request_body = CreateUserRequest,
    responses(
        (status = 201, description = "User created", body = UserResponse),
        (status = 400, description = "Validation error", body = ApiError),
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
    if let Err(response) =
        require_instance_admin_or_admin_key(&state, &headers, ctx.as_ref().map(|e| &e.0))
    {
        return *response;
    }
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
            Some(&password_hash),
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
        created_at: user.created_at,
    };
    (
        StatusCode::CREATED,
        Json(serde_json::to_value(response).unwrap()),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use crate::{RouterAppState, create_router};
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use common::catalog::{Catalog, MembershipRole};
    use common::config::Configuration;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    async fn call(
        app: &axum::Router,
        session_token: Option<&str>,
        method: Method,
        uri: &str,
        tenant_id: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header("x-tenant-id", tenant_id);
        if let Some(token) = session_token {
            builder = builder.header(
                "cookie",
                format!("{}={token}", common::auth::SESSION_COOKIE),
            );
        }
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

    /// Builds a router with one tenant, a non-instance-admin member with an
    /// active session, and an instance-admin user with an active session.
    /// Returns `(app, tenant_id, member_session, admin_session)`.
    async fn test_app() -> (axum::Router, String, String, String) {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let config = Configuration::default();
        // A default dataset is required: without one, `resolve_user_tenant`
        // rejects every request with `400` for missing `X-Dataset-ID`.
        catalog
            .upsert_tenant_with_default_dataset("acme", "Acme Corp", Some("production"), "database")
            .await
            .unwrap();
        let hash = common::auth::hash_password("member password 123").unwrap();

        let member = catalog
            .create_user("member@example.com", Some("Member"), Some(&hash), false)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&member.id, "acme", MembershipRole::Member)
            .await
            .unwrap();
        let member_session = common::auth::generate_session_token();
        catalog
            .create_user_session(
                &member.id,
                &common::auth::hash_session_token(&member_session),
                chrono::Utc::now() + chrono::Duration::hours(1),
            )
            .await
            .unwrap();

        let admin = catalog
            .create_user("admin@example.com", Some("Admin"), Some(&hash), true)
            .await
            .unwrap();
        let admin_session = common::auth::generate_session_token();
        catalog
            .create_user_session(
                &admin.id,
                &common::auth::hash_session_token(&admin_session),
                chrono::Utc::now() + chrono::Duration::hours(1),
            )
            .await
            .unwrap();

        let app = create_router(RouterAppState::new(catalog, config));
        (app, "acme".to_string(), member_session, admin_session)
    }

    #[tokio::test]
    async fn non_instance_admin_gets_403_on_every_manage_admin_route() {
        let (app, tenant_id, member_session, _admin_session) = test_app().await;

        let (status, _) = call(
            &app,
            Some(&member_session),
            Method::GET,
            "/api/v1/manage/admin/tenants",
            &tenant_id,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        let (status, _) = call(
            &app,
            Some(&member_session),
            Method::GET,
            "/api/v1/manage/admin/tenants/acme",
            &tenant_id,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        let (status, _) = call(
            &app,
            Some(&member_session),
            Method::PUT,
            "/api/v1/manage/admin/tenants/acme",
            &tenant_id,
            Some(json!({ "name": "Hacked" })),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        let (status, _) = call(
            &app,
            Some(&member_session),
            Method::DELETE,
            "/api/v1/manage/admin/tenants/acme",
            &tenant_id,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        let (status, _) = call(
            &app,
            Some(&member_session),
            Method::POST,
            "/api/v1/manage/admin/users",
            &tenant_id,
            Some(json!({
                "email": "new@example.com",
                "password": "at least twelve chars",
                "tenant": "acme",
                "role": "member",
            })),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn instance_admin_succeeds_on_every_manage_admin_route() {
        let (app, tenant_id, _member_session, admin_session) = test_app().await;

        let (status, body) = call(
            &app,
            Some(&admin_session),
            Method::GET,
            "/api/v1/manage/admin/tenants",
            &tenant_id,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let (status, body) = call(
            &app,
            Some(&admin_session),
            Method::GET,
            "/api/v1/manage/admin/tenants/acme",
            &tenant_id,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let (status, body) = call(
            &app,
            Some(&admin_session),
            Method::PUT,
            "/api/v1/manage/admin/tenants/acme",
            &tenant_id,
            Some(json!({ "name": "Renamed" })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let (status, body) = call(
            &app,
            Some(&admin_session),
            Method::POST,
            "/api/v1/manage/admin/users",
            &tenant_id,
            Some(json!({
                "email": "new@example.com",
                "password": "at least twelve chars",
                "tenant": "acme",
                "role": "member",
            })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");

        let (status, body) = call(
            &app,
            Some(&admin_session),
            Method::DELETE,
            "/api/v1/manage/admin/tenants/acme",
            &tenant_id,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    }

    const ADMIN_KEY: &str = "sk-admin-break-glass";

    /// Builds a router configured with [`ADMIN_KEY`] as `[auth].admin_api_key`,
    /// plus the same tenant/member/admin fixtures as [`test_app`]. Returns
    /// `(app, tenant_id)`.
    async fn test_app_with_admin_key() -> (axum::Router, String) {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let mut config = Configuration::default();
        config.auth.admin_api_key = Some(ADMIN_KEY.to_string());
        catalog
            .upsert_tenant_with_default_dataset("acme", "Acme Corp", Some("production"), "database")
            .await
            .unwrap();
        let app = create_router(RouterAppState::new(catalog, config));
        (app, "acme".to_string())
    }

    /// A request carrying only a bearer token, no `X-Tenant-ID` and no
    /// session cookie — the break-glass admin-key shape.
    async fn call_with_bearer(
        app: &axum::Router,
        bearer: &str,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", format!("Bearer {bearer}"));
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

    /// The break-glass `admin_api_key`, with no `X-Tenant-ID` at all,
    /// succeeds on every `/api/v1/manage/admin/*` route and on
    /// `POST /api/v1/manage/tenants` (issue #1561 part 2).
    #[tokio::test]
    async fn admin_key_succeeds_with_no_tenant_on_every_bypass_route() {
        let (app, _tenant_id) = test_app_with_admin_key().await;

        let (status, body) = call_with_bearer(
            &app,
            ADMIN_KEY,
            Method::GET,
            "/api/v1/manage/admin/tenants",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let (status, body) = call_with_bearer(
            &app,
            ADMIN_KEY,
            Method::GET,
            "/api/v1/manage/admin/tenants/acme",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let (status, body) = call_with_bearer(
            &app,
            ADMIN_KEY,
            Method::PUT,
            "/api/v1/manage/admin/tenants/acme",
            Some(json!({ "name": "Renamed" })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let (status, body) = call_with_bearer(
            &app,
            ADMIN_KEY,
            Method::POST,
            "/api/v1/manage/admin/users",
            Some(json!({
                "email": "break-glass@example.com",
                "password": "at least twelve chars",
                "tenant": "acme",
                "role": "member",
            })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");

        let (status, body) = call_with_bearer(
            &app,
            ADMIN_KEY,
            Method::POST,
            "/api/v1/manage/tenants",
            Some(json!({ "id": "globex", "name": "Globex" })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");

        let (status, body) = call_with_bearer(
            &app,
            ADMIN_KEY,
            Method::DELETE,
            "/api/v1/manage/admin/tenants/acme",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    }

    /// A bearer token that does not match the configured admin key, with no
    /// `X-Tenant-ID`, is rejected `401` — the same outcome a request with no
    /// credentials at all gets on every other `/api/v1` route.
    #[tokio::test]
    async fn invalid_admin_key_gets_401() {
        let (app, _tenant_id) = test_app_with_admin_key().await;

        let (status, _) = call_with_bearer(
            &app,
            "sk-not-the-admin-key",
            Method::GET,
            "/api/v1/manage/admin/tenants",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let (status, _) = call_with_bearer(
            &app,
            "sk-not-the-admin-key",
            Method::POST,
            "/api/v1/manage/tenants",
            Some(json!({ "id": "globex", "name": "Globex" })),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
}
