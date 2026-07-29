//! # UI session and whoami endpoints
//!
//! Browser-facing authentication for the embedded explore UI:
//!
//! - `POST /ui/session` accepts either credential shape:
//!   - **API key + tenant (+ optional dataset)**: validated with the same
//!     [`Authenticator`] path the auth middleware uses; the cookie stores
//!     the base64-JSON credential triple.
//!   - **Email + password**: verified against the catalog's `users` table
//!     (Argon2id); on success a server-side session row is created and the
//!     cookie stores the raw opaque `sdbs_` token (only its SHA-256 hash
//!     is persisted).
//! - `DELETE /ui/session` revokes the server-side session (for `sdbs_`
//!   cookies) and clears the cookie.
//! - `GET /api/v1/whoami` (behind the tenant auth middleware) returns the
//!   authenticated tenant and its datasets, strictly scoped to that
//!   tenant; for user sessions it additionally reports the user identity
//!   and all tenant memberships.

use crate::RouterState;
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use common::auth::{
    SESSION_COOKIE, SESSION_TOKEN_PREFIX, SessionData, TenantContextExtractor, encode_session,
    generate_session_token, hash_session_token, session_cookie_value, validate_dataset_id,
    validate_tenant_id, verify_password,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Absolute session lifetime for user (email/password) sessions.
///
/// Phase 1 default: 24h absolute expiry from login. An idle timeout
/// (sliding expiration) is deferred to a later phase; until then the
/// session simply ends 24h after it was created regardless of activity.
const USER_SESSION_LIFETIME_HOURS: i64 = 24;

/// Routes mounted at the router root (absolute `/ui/session` paths, so the
/// session endpoint coexists with the `/ui` static-asset service).
pub fn router<S: RouterState>() -> Router<S> {
    Router::new().route(
        "/ui/session",
        post(create_session::<S>).delete(delete_session::<S>),
    )
}

/// Body of `POST /ui/session` — either an API-key login (the original
/// shape, kept byte-for-byte compatible) or an email/password login.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum CreateSessionRequest {
    /// API key + tenant (+ optional dataset) login.
    ApiKey {
        api_key: String,
        tenant: String,
        #[serde(default)]
        dataset: Option<String>,
    },
    /// Email + password login producing a server-side user session.
    Password { email: String, password: String },
}

/// POST /ui/session
///
/// Validates the credentials and sets the session cookie. 204 on success,
/// 401/403 with a JSON error body on invalid credentials, 400 on malformed
/// tenant/dataset IDs.
pub async fn create_session<S: RouterState>(
    State(state): State<S>,
    Json(body): Json<CreateSessionRequest>,
) -> Response {
    match body {
        CreateSessionRequest::ApiKey {
            api_key,
            tenant,
            dataset,
        } => create_api_key_session(&state, api_key, tenant, dataset).await,
        CreateSessionRequest::Password { email, password } => {
            create_password_session(&state, email, password).await
        }
    }
}

/// API-key login: validates with the [`common::auth::Authenticator`] and
/// stores the credential triple in the cookie (original behavior).
async fn create_api_key_session<S: RouterState>(
    state: &S,
    api_key: String,
    tenant: String,
    dataset: Option<String>,
) -> Response {
    let tenant = match validate_tenant_id(&tenant) {
        Ok(t) => t,
        Err(e) => return error_response(400, e.to_string()),
    };
    let dataset = match dataset.as_deref().map(str::trim) {
        Some("") | None => None,
        Some(d) => match validate_dataset_id(d) {
            Ok(d) => Some(d),
            Err(e) => return error_response(400, e.to_string()),
        },
    };

    match state
        .authenticator()
        .authenticate(&api_key, &tenant, dataset.as_deref())
        .await
    {
        Ok(ctx) => {
            tracing::info!(tenant_id = %ctx.tenant_id, dataset = %ctx.dataset_id, "UI session created");
            let cookie = encode_session(&SessionData {
                api_key,
                tenant,
                dataset,
            });
            set_session_cookie_response(&cookie)
        }
        Err(err) => {
            tracing::warn!(tenant_id = %tenant, "UI session login failed: {}", err.message);
            error_response(err.status_code, err.message)
        }
    }
}

/// The uniform 401 for any failed email/password login. Deliberately
/// identical for unknown email, wrong password, and disabled account, so
/// the response does not leak which of them applied.
fn invalid_credentials() -> Response {
    error_response(401, "Invalid email or password".to_string())
}

/// Email/password login: verifies the password (Argon2id) against the
/// catalog's `users` table and issues an opaque server-side session token.
async fn create_password_session<S: RouterState>(
    state: &S,
    email: String,
    password: String,
) -> Response {
    let user = match state.catalog().get_user_by_email(&email).await {
        Ok(Some(user)) => user,
        Ok(None) => {
            tracing::warn!("UI login failed: unknown email");
            return invalid_credentials();
        }
        Err(e) => {
            tracing::error!(error = %e, "UI login: user lookup failed");
            return error_response(500, "Failed to process login".to_string());
        }
    };

    if user.disabled_at.is_some() {
        tracing::warn!(user_id = %user.id, "UI login failed: account disabled");
        return invalid_credentials();
    }

    // Argon2id verification is CPU-bound (deliberately slow); keep it off
    // the async worker threads.
    let password_hash = user.password_hash.clone();
    let verified = tokio::task::spawn_blocking(move || verify_password(&password, &password_hash))
        .await
        .map_err(anyhow::Error::from)
        .and_then(|r| r.map_err(anyhow::Error::from));
    match verified {
        Ok(true) => {}
        Ok(false) => {
            tracing::warn!(user_id = %user.id, "UI login failed: wrong password");
            return invalid_credentials();
        }
        Err(e) => {
            tracing::error!(user_id = %user.id, error = %e, "UI login: password verification failed");
            return error_response(500, "Failed to process login".to_string());
        }
    }

    let token = generate_session_token();
    let expires_at = chrono::Utc::now() + chrono::Duration::hours(USER_SESSION_LIFETIME_HOURS);
    if let Err(e) = state
        .catalog()
        .create_user_session(&user.id, &hash_session_token(&token), expires_at)
        .await
    {
        tracing::error!(user_id = %user.id, error = %e, "UI login: session creation failed");
        return error_response(500, "Failed to process login".to_string());
    }

    tracing::info!(user_id = %user.id, "UI user session created");
    // The cookie holds the RAW token; only its hash is stored server-side.
    set_session_cookie_response(&token)
}

/// 204 response setting the session cookie with the shared attributes.
fn set_session_cookie_response(value: &str) -> Response {
    (
        StatusCode::NO_CONTENT,
        [(
            header::SET_COOKIE,
            format!("{SESSION_COOKIE}={value}; HttpOnly; SameSite=Strict; Path=/"),
        )],
    )
        .into_response()
}

/// DELETE /ui/session
///
/// Logout. When the cookie holds an `sdbs_` user-session token the
/// server-side session row is revoked first; the cookie is cleared
/// unconditionally either way.
pub async fn delete_session<S: RouterState>(
    State(state): State<S>,
    headers: HeaderMap,
) -> Response {
    if let Some(value) = session_cookie_value(&headers)
        && value.starts_with(SESSION_TOKEN_PREFIX)
    {
        match state
            .catalog()
            .get_valid_session(&hash_session_token(&value))
            .await
        {
            Ok(Some(session)) => {
                if let Err(e) = state.catalog().revoke_session(&session.id).await {
                    tracing::error!(session_id = %session.id, error = %e, "Logout: session revocation failed");
                } else {
                    tracing::info!(user_id = %session.user_id, "UI user session revoked");
                }
            }
            Ok(None) => {} // already expired/revoked — nothing to do
            Err(e) => {
                tracing::error!(error = %e, "Logout: session lookup failed");
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

/// Authenticated human user (present only for user sessions).
#[derive(Debug, Serialize)]
pub struct WhoamiUser {
    pub id: String,
    pub email: String,
    pub display_name: Option<String>,
}

/// One tenant the authenticated user belongs to.
#[derive(Debug, Serialize)]
pub struct WhoamiMembership {
    pub tenant_id: String,
    pub role: String,
}

#[derive(Debug, Serialize)]
pub struct WhoamiResponse {
    pub tenant: WhoamiTenant,
    pub datasets: Vec<WhoamiDataset>,
    pub default_dataset: Option<String>,
    /// Present only when the request was authenticated with a user
    /// session; API-key responses are unchanged.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<WhoamiUser>,
    /// All tenants the user belongs to (user sessions only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memberships: Option<Vec<WhoamiMembership>>,
}

/// Look up the user identity and memberships for a user-session request.
///
/// Returns `(None, None)` for API-key authentication so the response
/// shape stays exactly as it was.
async fn whoami_user_fields<S: RouterState>(
    state: &S,
    ctx: &common::auth::TenantContext,
) -> (Option<WhoamiUser>, Option<Vec<WhoamiMembership>>) {
    let Some(identity) = ctx.user.as_ref() else {
        return (None, None);
    };

    let display_name = match state.catalog().get_user(&identity.user_id).await {
        Ok(Some(user)) => user.display_name,
        Ok(None) => None,
        Err(e) => {
            tracing::error!(user_id = %identity.user_id, error = %e, "whoami: user lookup failed");
            None
        }
    };

    let memberships = match state
        .catalog()
        .list_memberships_for_user(&identity.user_id)
        .await
    {
        Ok(memberships) => memberships
            .into_iter()
            .map(|m| WhoamiMembership {
                tenant_id: m.tenant_id,
                role: m.role.to_string(),
            })
            .collect(),
        Err(e) => {
            tracing::error!(user_id = %identity.user_id, error = %e, "whoami: membership lookup failed");
            // Fall back to the membership the request was authenticated
            // with rather than dropping the field entirely.
            vec![WhoamiMembership {
                tenant_id: ctx.tenant_id.clone(),
                role: identity.role.to_string(),
            }]
        }
    };

    (
        Some(WhoamiUser {
            id: identity.user_id.clone(),
            email: identity.email.clone(),
            display_name,
        }),
        Some(memberships),
    )
}

/// GET /api/v1/whoami
///
/// Returns the authenticated tenant with its datasets and default dataset,
/// resolved from the same sources the [`common::auth::Authenticator`] uses
/// (config tenants first, then catalog tenants). Strictly scoped to the
/// tenant in the request's [`common::auth::TenantContext`].
///
/// For user sessions the response additionally carries the `user` identity
/// and the user's `memberships`.
pub async fn whoami<S: RouterState>(
    State(state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
) -> Response {
    let (user, memberships) = whoami_user_fields(&state, &ctx).await;

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
        let response = WhoamiResponse {
            tenant: WhoamiTenant {
                id: tc.id.clone(),
                slug: tc.slug.clone(),
                name: tc.name.clone(),
            },
            datasets: tc
                .datasets
                .iter()
                .map(|d| WhoamiDataset {
                    id: d.id.clone(),
                    slug: d.slug.clone(),
                    is_default: d.is_default,
                })
                .collect(),
            default_dataset,
            user,
            memberships,
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
        user,
        memberships,
    };
    Json(response).into_response()
}

#[cfg(test)]
mod tests {
    use crate::{InMemoryStateImpl, create_router};
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use common::catalog::Catalog;
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
    async fn create_session_with_valid_key_sets_httponly_cookie() {
        let app = test_app().await;
        let res = create_session(
            &app,
            serde_json::json!({"api_key": "acme-key", "tenant": "acme", "dataset": "staging"}),
        )
        .await;

        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        let set_cookie = res
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(set_cookie.starts_with("signaldb_session="));
        assert!(set_cookie.contains("HttpOnly"));
        assert!(set_cookie.contains("SameSite=Strict"));
        assert!(set_cookie.contains("Path=/"));

        // The cookie value round-trips through the shared session codec.
        let value = cookie_pair(&res);
        let value = value.strip_prefix("signaldb_session=").unwrap();
        let session = common::auth::decode_session(value).expect("decodable session");
        assert_eq!(session.api_key, "acme-key");
        assert_eq!(session.tenant, "acme");
        assert_eq!(session.dataset.as_deref(), Some("staging"));
    }

    #[tokio::test]
    async fn create_session_with_invalid_key_returns_401_json() {
        let app = test_app().await;
        let res = create_session(
            &app,
            serde_json::json!({"api_key": "wrong-key", "tenant": "acme"}),
        )
        .await;

        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert!(res.headers().get(header::SET_COOKIE).is_none());
        let body = json_body(res).await;
        assert!(body["error"].as_str().unwrap().contains("API key"));
    }

    #[tokio::test]
    async fn create_session_with_wrong_dataset_is_rejected() {
        let app = test_app().await;
        let res = create_session(
            &app,
            serde_json::json!({"api_key": "acme-key", "tenant": "acme", "dataset": "nope"}),
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
            serde_json::json!({"api_key": "acme-key", "tenant": "acme"}),
        )
        .await;
        let cookie = cookie_pair(&res);

        // A query route accepts the cookie in place of the auth headers.
        let request = Request::builder()
            .uri("/tempo/api/echo")
            .header(header::COOKIE, &cookie)
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
    async fn explicit_headers_override_session_cookie() {
        let app = test_app().await;
        let res = create_session(
            &app,
            serde_json::json!({"api_key": "acme-key", "tenant": "acme"}),
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

    mod user_login {
        use super::*;
        use common::auth::hash_password;
        use common::catalog::MembershipRole;

        /// App plus its catalog, so tests can seed users and tenants. The
        /// config tenants are the same ones `test_app` uses.
        async fn app_with_catalog() -> (axum::Router, Catalog) {
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
            // Memberships FK to `tenants`, so the config tenants need rows
            // too — what `sync_config_tenants` does on a real startup.
            catalog
                .sync_config_tenants(&config.auth)
                .await
                .expect("config tenants synced");
            let app = create_router(InMemoryStateImpl::new(catalog.clone(), config));
            (app, catalog)
        }

        /// Seed a user with the given password and memberships.
        async fn seed_user(
            catalog: &Catalog,
            email: &str,
            password: &str,
            memberships: &[(&str, MembershipRole)],
        ) -> String {
            let hash = hash_password(password).unwrap();
            let user = catalog
                .create_user(email, Some("Test User"), &hash, false)
                .await
                .unwrap();
            for (tenant_id, role) in memberships {
                catalog
                    .upsert_tenant_membership(&user.id, tenant_id, *role)
                    .await
                    .unwrap();
            }
            user.id
        }

        #[tokio::test]
        async fn email_password_login_sets_sdbs_cookie_and_stores_session() {
            let (app, catalog) = app_with_catalog().await;
            let user_id = seed_user(
                &catalog,
                "alice@example.com",
                "hunter2hunter2",
                &[("acme", MembershipRole::Member)],
            )
            .await;

            let res = create_session(
                &app,
                serde_json::json!({"email": "Alice@Example.com", "password": "hunter2hunter2"}),
            )
            .await;
            assert_eq!(res.status(), StatusCode::NO_CONTENT);

            let set_cookie = res
                .headers()
                .get(header::SET_COOKIE)
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            assert!(set_cookie.contains("HttpOnly"));
            assert!(set_cookie.contains("SameSite=Strict"));
            assert!(set_cookie.contains("Path=/"));

            let pair = cookie_pair(&res);
            let token = pair.strip_prefix("signaldb_session=").unwrap();
            assert!(token.starts_with("sdbs_"));

            // The raw token is not stored; its hash resolves the session.
            let session = catalog
                .get_valid_session(&common::auth::hash_session_token(token))
                .await
                .unwrap()
                .expect("session row created");
            assert_eq!(session.user_id, user_id);
            assert!(session.expires_at > chrono::Utc::now() + chrono::Duration::hours(23));
            assert!(session.expires_at < chrono::Utc::now() + chrono::Duration::hours(25));
        }

        #[tokio::test]
        async fn wrong_password_and_unknown_email_are_identical_401s() {
            let (app, catalog) = app_with_catalog().await;
            seed_user(
                &catalog,
                "alice@example.com",
                "hunter2hunter2",
                &[("acme", MembershipRole::Member)],
            )
            .await;

            let wrong_password = create_session(
                &app,
                serde_json::json!({"email": "alice@example.com", "password": "nope"}),
            )
            .await;
            assert_eq!(wrong_password.status(), StatusCode::UNAUTHORIZED);
            assert!(wrong_password.headers().get(header::SET_COOKIE).is_none());

            let unknown_email = create_session(
                &app,
                serde_json::json!({"email": "nobody@example.com", "password": "hunter2hunter2"}),
            )
            .await;
            assert_eq!(unknown_email.status(), StatusCode::UNAUTHORIZED);
            assert!(unknown_email.headers().get(header::SET_COOKIE).is_none());

            // Same body — the response must not reveal which factor failed.
            assert_eq!(
                json_body(wrong_password).await,
                json_body(unknown_email).await
            );
        }

        #[tokio::test]
        async fn disabled_user_cannot_log_in() {
            let (app, catalog) = app_with_catalog().await;
            let user_id = seed_user(
                &catalog,
                "alice@example.com",
                "hunter2hunter2",
                &[("acme", MembershipRole::Member)],
            )
            .await;
            catalog.set_user_disabled(&user_id, true).await.unwrap();

            let res = create_session(
                &app,
                serde_json::json!({"email": "alice@example.com", "password": "hunter2hunter2"}),
            )
            .await;
            assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
            assert!(res.headers().get(header::SET_COOKIE).is_none());
        }

        #[tokio::test]
        async fn user_session_cookie_authenticates_and_whoami_reports_memberships() {
            let (app, catalog) = app_with_catalog().await;
            let user_id = seed_user(
                &catalog,
                "alice@example.com",
                "hunter2hunter2",
                &[
                    ("acme", MembershipRole::Admin),
                    ("globex", MembershipRole::Viewer),
                ],
            )
            .await;

            let res = create_session(
                &app,
                serde_json::json!({"email": "alice@example.com", "password": "hunter2hunter2"}),
            )
            .await;
            let cookie = cookie_pair(&res);

            // Two memberships: the tenant header selects which one applies.
            let request = Request::builder()
                .uri("/api/v1/whoami")
                .header(header::COOKIE, &cookie)
                .header("x-tenant-id", "acme")
                .body(Body::empty())
                .unwrap();
            let res = app.clone().oneshot(request).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);

            let body = json_body(res).await;
            // Existing tenant/dataset fields are unchanged.
            assert_eq!(body["tenant"]["id"], "acme");
            assert_eq!(body["tenant"]["slug"], "acme-slug");
            assert_eq!(body["default_dataset"], "production");
            // User identity and memberships are added.
            assert_eq!(body["user"]["id"], user_id);
            assert_eq!(body["user"]["email"], "alice@example.com");
            assert_eq!(body["user"]["display_name"], "Test User");
            let memberships = body["memberships"].as_array().unwrap();
            assert_eq!(memberships.len(), 2);
            assert_eq!(memberships[0]["tenant_id"], "acme");
            assert_eq!(memberships[0]["role"], "admin");
            assert_eq!(memberships[1]["tenant_id"], "globex");
            assert_eq!(memberships[1]["role"], "viewer");
        }

        #[tokio::test]
        async fn whoami_with_api_key_has_no_user_fields() {
            let app = test_app().await;
            let request = Request::builder()
                .uri("/api/v1/whoami")
                .header("authorization", "Bearer acme-key")
                .header("x-tenant-id", "acme")
                .body(Body::empty())
                .unwrap();
            let res = app.clone().oneshot(request).await.unwrap();
            let body = json_body(res).await;
            assert!(body.get("user").is_none());
            assert!(body.get("memberships").is_none());
        }

        #[tokio::test]
        async fn logout_revokes_the_user_session() {
            let (app, catalog) = app_with_catalog().await;
            seed_user(
                &catalog,
                "alice@example.com",
                "hunter2hunter2",
                &[("acme", MembershipRole::Member)],
            )
            .await;

            let res = create_session(
                &app,
                serde_json::json!({"email": "alice@example.com", "password": "hunter2hunter2"}),
            )
            .await;
            let cookie = cookie_pair(&res);

            // The cookie authenticates before logout.
            let request = Request::builder()
                .uri("/api/v1/whoami")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap();
            let res = app.clone().oneshot(request).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);

            // Logout revokes the session server-side and clears the cookie.
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
            assert!(set_cookie.contains("Max-Age=0"));

            // Replaying the same cookie no longer authenticates.
            let request = Request::builder()
                .uri("/api/v1/whoami")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap();
            let res = app.clone().oneshot(request).await.unwrap();
            assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        }

        #[tokio::test]
        async fn malformed_login_body_is_rejected() {
            let (app, _catalog) = app_with_catalog().await;
            // Neither credential shape matches.
            let res = create_session(&app, serde_json::json!({"email": "a@example.com"})).await;
            assert!(res.status().is_client_error());
        }
    }

    #[tokio::test]
    async fn logout_clears_session_cookie() {
        let app = test_app().await;
        let request = Request::builder()
            .method("DELETE")
            .uri("/ui/session")
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
    }
}
