//! # UI session and whoami endpoints
//!
//! Browser-facing authentication for the embedded explore UI:
//!
//! - `POST /ui/session` validates a human user's email/password and tenant
//!   membership, then issues an opaque server-side session token.
//! - `DELETE /ui/session` revokes that session and clears its cookie.
//! - `GET /api/v1/whoami` (behind the tenant auth middleware) returns the
//!   authenticated tenant and its datasets, strictly scoped to that tenant.

use crate::RouterState;
use axum::{
    Json, Router,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use chrono::{Duration, Utc};
use common::auth::{
    SESSION_COOKIE, TenantContextExtractor, generate_session_token, hash_session_token,
    session_token_from_headers, validate_dataset_id, validate_tenant_id, verify_password,
};
use common::catalog::MembershipRole;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Routes mounted at the router root (absolute `/ui/session` paths, so the
/// session endpoint coexists with the `/ui` static-asset service).
pub fn router<S: RouterState>() -> Router<S> {
    Router::new().route(
        "/ui/session",
        post(create_session::<S>).delete(delete_session::<S>),
    )
}

#[derive(Debug, Deserialize)]
pub struct CreateSessionRequest {
    pub email: String,
    pub password: String,
    pub tenant: String,
    #[serde(default)]
    pub dataset: Option<String>,
}

/// POST /ui/session
///
/// Validates the credentials and sets the session cookie. 200 on success,
/// 401/403 with a JSON error body on invalid credentials, 400 on malformed
/// tenant/dataset IDs.
pub async fn create_session<S: RouterState>(
    State(state): State<S>,
    Json(body): Json<CreateSessionRequest>,
) -> Response {
    let tenant = match validate_tenant_id(&body.tenant) {
        Ok(t) => t,
        Err(e) => return error_response(400, e.to_string()),
    };
    let dataset = match body.dataset.as_deref().map(str::trim) {
        Some("") | None => None,
        Some(d) => match validate_dataset_id(d) {
            Ok(d) => Some(d),
            Err(e) => return error_response(400, e.to_string()),
        },
    };

    let user = match state.catalog().get_user_by_email(&body.email).await {
        Ok(Some(user)) if user.disabled_at.is_none() => user,
        Ok(_) => return error_response(401, "Invalid email or password".to_string()),
        Err(error) => {
            tracing::error!(error = %error, "UI session user lookup failed");
            return error_response(500, "Unable to create session".to_string());
        }
    };
    let password = body.password;
    let password_hash = user.password_hash.clone();
    let password_matches = match tokio::task::spawn_blocking(move || {
        verify_password(&password, &password_hash)
    })
    .await
    {
        Ok(Ok(matches)) => matches,
        Ok(Err(error)) => {
            tracing::error!(user_id = %user.id, error = %error, "Stored password hash is invalid");
            return error_response(500, "Unable to create session".to_string());
        }
        Err(error) => {
            tracing::error!(error = %error, "Password verification task failed");
            return error_response(500, "Unable to create session".to_string());
        }
    };
    if !password_matches {
        return error_response(401, "Invalid email or password".to_string());
    }

    let membership_role = if user.is_instance_admin {
        MembershipRole::Admin
    } else {
        match state
            .catalog()
            .get_tenant_membership(&user.id, &tenant)
            .await
        {
            Ok(Some(membership)) => membership.role,
            Ok(None) => {
                return error_response(403, "User is not a member of this tenant".to_string());
            }
            Err(error) => {
                tracing::error!(user_id = %user.id, error = %error, "Membership lookup failed");
                return error_response(500, "Unable to create session".to_string());
            }
        }
    };

    let token = generate_session_token();
    let token_hash = hash_session_token(&token);
    let expires_at = Utc::now() + Duration::hours(12);
    let session = match state
        .catalog()
        .create_user_session(&user.id, &token_hash, expires_at)
        .await
    {
        Ok(session) => session,
        Err(error) => {
            tracing::error!(user_id = %user.id, error = %error, "Session persistence failed");
            return error_response(500, "Unable to create session".to_string());
        }
    };

    match state
        .authenticator()
        .authenticate_session(&token, &tenant, dataset.as_deref())
        .await
    {
        Ok(ctx) => {
            tracing::info!(
                user_id = %user.id,
                tenant_id = %ctx.tenant_id,
                dataset = %ctx.dataset_id,
                role = %membership_role,
                "UI session created"
            );
            (
                StatusCode::OK,
                [(
                    header::SET_COOKIE,
                    format!(
                        "{SESSION_COOKIE}={token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=43200"
                    ),
                )],
                Json(json!({
                    "tenant": ctx.tenant_id,
                    "dataset": ctx.dataset_id,
                })),
            )
                .into_response()
        }
        Err(err) => {
            if let Err(error) = state.catalog().revoke_session(&session.id).await {
                tracing::error!(session_id = %session.id, error = %error, "Failed to revoke rejected session");
            }
            tracing::warn!(tenant_id = %tenant, "UI session login failed: {}", err.message);
            error_response(err.status_code, err.message)
        }
    }
}

/// DELETE /ui/session
///
/// Clears the session cookie (logout).
pub async fn delete_session<S: RouterState>(
    State(state): State<S>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Some(token) = session_token_from_headers(&headers) {
        match state
            .catalog()
            .get_valid_session(&hash_session_token(&token))
            .await
        {
            Ok(Some(session)) => {
                if let Err(error) = state.catalog().revoke_session(&session.id).await {
                    tracing::error!(session_id = %session.id, error = %error, "Session revocation failed");
                    return error_response(500, "Unable to revoke session".to_string());
                }
            }
            Ok(None) => {}
            Err(error) => {
                tracing::error!(error = %error, "Session lookup during logout failed");
                return error_response(500, "Unable to revoke session".to_string());
            }
        }
    }
    (
        StatusCode::NO_CONTENT,
        [(
            header::SET_COOKIE,
            format!("{SESSION_COOKIE}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0"),
        )],
    )
        .into_response()
}

fn error_response(status: u16, message: String) -> Response {
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::UNAUTHORIZED),
        Json(json!({ "error": message })),
    )
        .into_response()
}

#[derive(Debug, Serialize)]
pub struct WhoamiTenant {
    pub id: String,
    pub slug: String,
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct WhoamiDataset {
    pub id: String,
    pub slug: String,
    pub is_default: bool,
}

#[derive(Debug, Serialize)]
pub struct WhoamiResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<WhoamiUser>,
    pub memberships: Vec<WhoamiMembership>,
    pub tenant: WhoamiTenant,
    pub datasets: Vec<WhoamiDataset>,
    pub default_dataset: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct WhoamiUser {
    pub id: String,
    pub email: String,
    pub display_name: Option<String>,
    pub is_instance_admin: bool,
}

#[derive(Debug, Serialize)]
pub struct WhoamiMembership {
    pub tenant_id: String,
    pub role: MembershipRole,
}

/// GET /api/v1/whoami
///
/// Returns the authenticated tenant with its datasets and default dataset,
/// resolved from the same sources the [`common::auth::Authenticator`] uses
/// (config tenants first, then catalog tenants). Strictly scoped to the
/// tenant in the request's [`common::auth::TenantContext`].
pub async fn whoami<S: RouterState>(
    State(state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
) -> Response {
    let (user, memberships) = match &ctx.user_id {
        Some(user_id) => {
            let user = match state.catalog().get_user(user_id).await {
                Ok(Some(user)) => user,
                Ok(None) => return error_response(401, "Session user not found".to_string()),
                Err(error) => {
                    tracing::error!(user_id = %user_id, error = %error, "whoami: user lookup failed");
                    return error_response(500, "Failed to resolve user".to_string());
                }
            };
            let memberships = if user.is_instance_admin {
                match state.catalog().list_tenants().await {
                    Ok(tenants) => tenants
                        .into_iter()
                        .map(|tenant| WhoamiMembership {
                            tenant_id: tenant.id,
                            role: MembershipRole::Admin,
                        })
                        .collect(),
                    Err(error) => {
                        tracing::error!(user_id = %user_id, error = %error, "whoami: tenant lookup failed");
                        return error_response(500, "Failed to resolve tenants".to_string());
                    }
                }
            } else {
                match state.catalog().list_memberships_for_user(user_id).await {
                    Ok(memberships) => memberships
                        .into_iter()
                        .map(|membership| WhoamiMembership {
                            tenant_id: membership.tenant_id,
                            role: membership.role,
                        })
                        .collect(),
                    Err(error) => {
                        tracing::error!(user_id = %user_id, error = %error, "whoami: membership lookup failed");
                        return error_response(500, "Failed to resolve memberships".to_string());
                    }
                }
            };
            (
                Some(WhoamiUser {
                    id: user.id,
                    email: user.email,
                    display_name: user.display_name,
                    is_instance_admin: user.is_instance_admin,
                }),
                memberships,
            )
        }
        None => (None, Vec::new()),
    };

    // Config-defined tenants first (mirrors Authenticator precedence).
    if let Some(tc) = state
        .config()
        .auth
        .tenants
        .iter()
        .find(|t| t.id == ctx.tenant_id)
    {
        let default_dataset = tc.default_dataset.clone().or_else(|| {
            tc.datasets
                .iter()
                .find(|d| d.is_default)
                .map(|d| d.id.clone())
        });
        let mut datasets = tc
            .datasets
            .iter()
            .map(|d| WhoamiDataset {
                id: d.id.clone(),
                slug: d.slug.clone(),
                is_default: d.is_default,
            })
            .collect::<Vec<_>>();
        match state.catalog().get_datasets(&ctx.tenant_id).await {
            Ok(catalog_datasets) => {
                for dataset in catalog_datasets {
                    if !datasets.iter().any(|existing| existing.id == dataset.name) {
                        datasets.push(WhoamiDataset {
                            id: dataset.name.clone(),
                            slug: dataset.name,
                            is_default: false,
                        });
                    }
                }
            }
            Err(error) => {
                tracing::error!(tenant_id = %ctx.tenant_id, error = %error, "whoami: catalog dataset overlay failed");
                return error_response(500, "Failed to resolve datasets".to_string());
            }
        }
        let response = WhoamiResponse {
            user,
            memberships,
            tenant: WhoamiTenant {
                id: tc.id.clone(),
                slug: tc.slug.clone(),
                name: tc.name.clone(),
            },
            datasets,
            default_dataset,
        };
        return Json(response).into_response();
    }

    // Database-defined tenants (created via the admin API).
    let tenant = match state.catalog().get_tenant(&ctx.tenant_id).await {
        Ok(Some(t)) => t,
        Ok(None) => {
            return error_response(404, format!("Tenant '{}' not found", ctx.tenant_id));
        }
        Err(e) => {
            tracing::error!(tenant_id = %ctx.tenant_id, error = %e, "whoami: tenant lookup failed");
            return error_response(500, "Failed to resolve tenant".to_string());
        }
    };
    let datasets = match state.catalog().get_datasets(&ctx.tenant_id).await {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(tenant_id = %ctx.tenant_id, error = %e, "whoami: dataset lookup failed");
            return error_response(500, "Failed to resolve datasets".to_string());
        }
    };

    let response = WhoamiResponse {
        user,
        memberships,
        tenant: WhoamiTenant {
            // DB tenants use IDs as slugs, matching the Authenticator.
            id: tenant.id.clone(),
            slug: tenant.id.clone(),
            name: tenant.name,
        },
        datasets: datasets
            .into_iter()
            .map(|d| WhoamiDataset {
                is_default: Some(d.name.as_str()) == tenant.default_dataset.as_deref(),
                slug: d.name.clone(),
                id: d.name,
            })
            .collect(),
        default_dataset: tenant.default_dataset,
    };
    Json(response).into_response()
}

#[cfg(test)]
mod tests {
    use crate::{InMemoryStateImpl, create_router};
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use common::catalog::{Catalog, MembershipRole};
    use common::config::{ApiKeyConfig, AuthConfig, Configuration, DatasetConfig, TenantConfig};
    use serde_json::Value;
    use tower::ServiceExt;

    fn tenant(
        id: &str,
        key: &str,
        datasets: &[(&str, bool)],
        default_dataset: Option<&str>,
    ) -> TenantConfig {
        TenantConfig {
            id: id.to_string(),
            slug: format!("{id}-slug"),
            name: format!("{id} Inc"),
            default_dataset: default_dataset.map(str::to_string),
            datasets: datasets
                .iter()
                .map(|(name, is_default)| DatasetConfig {
                    id: name.to_string(),
                    slug: name.to_string(),
                    is_default: *is_default,
                    storage: None,
                })
                .collect(),
            api_keys: vec![ApiKeyConfig {
                key: key.to_string(),
                name: Some("test".to_string()),
            }],
            schema_config: None,
            limits: None,
        }
    }

    async fn test_app() -> axum::Router {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let config = Configuration {
            auth: AuthConfig {
                tenants: vec![
                    tenant(
                        "acme",
                        "acme-key",
                        &[("production", true), ("staging", false)],
                        Some("production"),
                    ),
                    tenant("globex", "globex-key", &[("main", true)], Some("main")),
                ],
                ..Default::default()
            },
            ..Default::default()
        };
        catalog.sync_config_tenants(&config.auth).await.unwrap();
        let password_hash = common::auth::hash_password("correct horse battery staple").unwrap();
        let user = catalog
            .create_user("alice@example.com", Some("Alice"), &password_hash, true)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&user.id, "acme", MembershipRole::Admin)
            .await
            .unwrap();
        let viewer_hash = common::auth::hash_password("viewer password").unwrap();
        let viewer = catalog
            .create_user("viewer@example.com", Some("Viewer"), &viewer_hash, false)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&viewer.id, "acme", MembershipRole::Viewer)
            .await
            .unwrap();
        let member_hash = common::auth::hash_password("member password").unwrap();
        let member = catalog
            .create_user("member@example.com", Some("Member"), &member_hash, false)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&member.id, "acme", MembershipRole::Member)
            .await
            .unwrap();
        create_router(InMemoryStateImpl::new(catalog, config))
    }

    async fn create_session(app: &axum::Router, body: Value) -> axum::response::Response {
        let request = Request::builder()
            .method("POST")
            .uri("/ui/session")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        app.clone().oneshot(request).await.unwrap()
    }

    async fn json_body(res: axum::response::Response) -> Value {
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// The Set-Cookie value minus attributes, e.g. `signaldb_session=abc`.
    fn cookie_pair(res: &axum::response::Response) -> String {
        let set_cookie = res
            .headers()
            .get(header::SET_COOKIE)
            .expect("Set-Cookie present")
            .to_str()
            .unwrap();
        set_cookie
            .split(';')
            .next()
            .expect("cookie name=value")
            .to_string()
    }

    #[tokio::test]
    async fn password_login_sets_opaque_secure_cookie() {
        let app = test_app().await;
        let res = create_session(
            &app,
            serde_json::json!({
                "email": "Alice@Example.com",
                "password": "correct horse battery staple",
                "tenant": "acme",
                "dataset": "staging"
            }),
        )
        .await;

        assert_eq!(res.status(), StatusCode::OK);
        let set_cookie = res
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(set_cookie.starts_with("signaldb_session="));
        assert!(set_cookie.contains("HttpOnly"));
        assert!(set_cookie.contains("Secure"));
        assert!(set_cookie.contains("SameSite=Strict"));
        assert!(set_cookie.contains("Path=/"));
        let value = cookie_pair(&res);
        let value = value.strip_prefix("signaldb_session=").unwrap();
        assert!(value.starts_with(common::auth::SESSION_TOKEN_PREFIX));
        assert!(!value.contains("correct horse battery staple"));
    }

    #[tokio::test]
    async fn password_login_with_invalid_password_returns_generic_401() {
        let app = test_app().await;
        let res = create_session(
            &app,
            serde_json::json!({
                "email": "alice@example.com",
                "password": "wrong password",
                "tenant": "acme"
            }),
        )
        .await;

        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert!(res.headers().get(header::SET_COOKIE).is_none());
        let body = json_body(res).await;
        assert_eq!(body["error"], "Invalid email or password");
    }

    #[tokio::test]
    async fn create_session_with_wrong_dataset_is_rejected() {
        let app = test_app().await;
        let res = create_session(
            &app,
            serde_json::json!({
                "email": "alice@example.com",
                "password": "correct horse battery staple",
                "tenant": "acme",
                "dataset": "nope"
            }),
        )
        .await;
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        assert!(res.headers().get(header::SET_COOKIE).is_none());
    }

    #[tokio::test]
    async fn session_cookie_authenticates_query_route() {
        let app = test_app().await;
        let res = create_session(
            &app,
            serde_json::json!({
                "email": "alice@example.com",
                "password": "correct horse battery staple",
                "tenant": "acme"
            }),
        )
        .await;
        let cookie = cookie_pair(&res);

        // A query route accepts the cookie in place of the auth headers.
        let request = Request::builder()
            .uri("/tempo/api/echo")
            .header(header::COOKIE, &cookie)
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(request).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // Without cookie or headers the same route rejects the request as
        // unauthenticated (401 — what the UI login gate keys on).
        let request = Request::builder()
            .uri("/tempo/api/echo")
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(request).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn user_whoami_returns_identity_memberships_and_role() {
        let app = test_app().await;
        let login = create_session(
            &app,
            serde_json::json!({
                "email": "alice@example.com",
                "password": "correct horse battery staple",
                "tenant": "acme"
            }),
        )
        .await;
        let cookie = cookie_pair(&login);
        let request = Request::builder()
            .uri("/api/v1/whoami")
            .header(header::COOKIE, cookie)
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["user"]["email"], "alice@example.com");
        assert_eq!(body["user"]["display_name"], "Alice");
        assert_eq!(body["user"]["is_instance_admin"], true);
        assert_eq!(body["memberships"][0]["tenant_id"], "acme");
        assert_eq!(body["memberships"][0]["role"], "admin");
    }

    #[tokio::test]
    async fn instance_admin_can_select_tenant_without_membership() {
        let app = test_app().await;
        let login = create_session(
            &app,
            serde_json::json!({
                "email": "alice@example.com",
                "password": "correct horse battery staple",
                "tenant": "acme"
            }),
        )
        .await;
        let cookie = cookie_pair(&login);
        let request = Request::builder()
            .uri("/api/v1/whoami")
            .header(header::COOKIE, cookie)
            .header("x-tenant-id", "globex")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["tenant"]["id"], "globex");
        assert_eq!(body["memberships"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn instance_admin_can_login_directly_to_tenant_without_membership() {
        let app = test_app().await;
        let response = create_session(
            &app,
            serde_json::json!({
                "email": "alice@example.com",
                "password": "correct horse battery staple",
                "tenant": "globex"
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn management_creates_dataset_and_dataset_scoped_key() {
        let app = test_app().await;
        let login = create_session(
            &app,
            serde_json::json!({
                "email": "alice@example.com",
                "password": "correct horse battery staple",
                "tenant": "acme"
            }),
        )
        .await;
        let cookie = cookie_pair(&login);

        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/manage/tenants/acme/datasets")
            .header(header::COOKIE, &cookie)
            .header("x-tenant-id", "acme")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"name":"analytics"}"#))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/manage/tenants/acme/api-keys")
            .header(header::COOKIE, &cookie)
            .header("x-tenant-id", "acme")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"name":"metrics-only","dataset_id":"analytics","scopes":["metrics:write"]}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = json_body(response).await;
        assert!(body["key"].as_str().unwrap().starts_with("sdbk_"));
        assert_eq!(body["dataset_id"], "analytics");
        assert_eq!(body["scopes"][0], "metrics:write");
    }

    #[tokio::test]
    async fn instance_admin_creates_and_immediately_accesses_tenant() {
        let app = test_app().await;
        let login = create_session(
            &app,
            serde_json::json!({
                "email": "alice@example.com",
                "password": "correct horse battery staple",
                "tenant": "acme"
            }),
        )
        .await;
        let cookie = cookie_pair(&login);
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/manage/tenants")
            .header(header::COOKIE, &cookie)
            .header("x-tenant-id", "acme")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"id":"newco","name":"New Co","default_dataset":"production"}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let request = Request::builder()
            .uri("/api/v1/whoami")
            .header(header::COOKIE, cookie)
            .header("x-tenant-id", "newco")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["tenant"]["id"], "newco");
        assert_eq!(body["default_dataset"], "production");
    }

    #[tokio::test]
    async fn management_rejects_cross_tenant_path() {
        let app = test_app().await;
        let login = create_session(
            &app,
            serde_json::json!({
                "email": "alice@example.com",
                "password": "correct horse battery staple",
                "tenant": "acme"
            }),
        )
        .await;
        let cookie = cookie_pair(&login);
        let request = Request::builder()
            .uri("/api/v1/manage/tenants/globex/api-keys")
            .header(header::COOKIE, cookie)
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn viewer_cannot_use_management_endpoints_or_other_tenants() {
        let app = test_app().await;
        let login = create_session(
            &app,
            serde_json::json!({
                "email": "viewer@example.com",
                "password": "viewer password",
                "tenant": "acme"
            }),
        )
        .await;
        let cookie = cookie_pair(&login);
        let request = Request::builder()
            .uri("/api/v1/manage/tenants/acme/api-keys")
            .header(header::COOKIE, &cookie)
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let request = Request::builder()
            .uri("/api/v1/whoami")
            .header(header::COOKIE, cookie)
            .header("x-tenant-id", "globex")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn ingestion_api_key_cannot_use_human_management_endpoints() {
        let app = test_app().await;
        let request = Request::builder()
            .uri("/api/v1/manage/tenants/acme/api-keys")
            .header("authorization", "Bearer acme-key")
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn member_can_query_but_cannot_administer_tenant() {
        let app = test_app().await;
        let login = create_session(
            &app,
            serde_json::json!({
                "email": "member@example.com",
                "password": "member password",
                "tenant": "acme"
            }),
        )
        .await;
        let cookie = cookie_pair(&login);
        let query = Request::builder()
            .uri("/tempo/api/echo")
            .header(header::COOKIE, &cookie)
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(query).await.unwrap().status(),
            StatusCode::OK
        );

        let management = Request::builder()
            .uri("/api/v1/manage/tenants/acme/api-keys")
            .header(header::COOKIE, cookie)
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(management).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn explicit_bearer_headers_override_session_cookie() {
        let app = test_app().await;
        let res = create_session(
            &app,
            serde_json::json!({
                "email": "alice@example.com",
                "password": "correct horse battery staple",
                "tenant": "acme"
            }),
        )
        .await;
        let cookie = cookie_pair(&res);

        // Full explicit headers for another tenant win over the cookie.
        let request = Request::builder()
            .uri("/api/v1/whoami")
            .header(header::COOKIE, &cookie)
            .header("authorization", "Bearer globex-key")
            .header("x-tenant-id", "globex")
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(request).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = json_body(res).await;
        assert_eq!(body["tenant"]["id"], "globex");
        assert!(body.get("user").is_none());
    }

    #[tokio::test]
    async fn whoami_returns_only_own_tenant_and_datasets() {
        let app = test_app().await;
        let request = Request::builder()
            .uri("/api/v1/whoami")
            .header("authorization", "Bearer acme-key")
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(request).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = json_body(res).await;
        assert_eq!(body["tenant"]["id"], "acme");
        assert_eq!(body["tenant"]["slug"], "acme-slug");
        assert_eq!(body["tenant"]["name"], "acme Inc");
        assert_eq!(body["default_dataset"], "production");
        assert!(body.get("user").is_none());
        assert_eq!(body["memberships"].as_array().unwrap().len(), 0);
        let datasets = body["datasets"].as_array().unwrap();
        assert_eq!(datasets.len(), 2);
        assert_eq!(datasets[0]["id"], "production");
        assert_eq!(datasets[0]["is_default"], true);
        assert_eq!(datasets[1]["id"], "staging");
        assert_eq!(datasets[1]["is_default"], false);
        // No cross-tenant data leaks into the response.
        assert!(!body.to_string().contains("globex"));
    }

    #[tokio::test]
    async fn whoami_requires_authentication() {
        let app = test_app().await;
        let request = Request::builder()
            .uri("/api/v1/whoami")
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(request).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn logout_revokes_and_clears_session_cookie() {
        let app = test_app().await;
        let login = create_session(
            &app,
            serde_json::json!({
                "email": "alice@example.com",
                "password": "correct horse battery staple",
                "tenant": "acme"
            }),
        )
        .await;
        let cookie = cookie_pair(&login);
        let request = Request::builder()
            .method("DELETE")
            .uri("/ui/session")
            .header(header::COOKIE, &cookie)
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(request).await.unwrap();

        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        let set_cookie = res
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(set_cookie.starts_with("signaldb_session=;"));
        assert!(set_cookie.contains("Max-Age=0"));
        assert!(set_cookie.contains("HttpOnly"));

        let request = Request::builder()
            .uri("/tempo/api/echo")
            .header(header::COOKIE, &cookie)
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(request).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }
}
