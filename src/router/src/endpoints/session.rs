//! # UI session and whoami endpoints
//!
//! Browser-facing authentication for the embedded explore UI:
//!
//! - `POST /ui/session` validates an API key + tenant (+ optional dataset)
//!   with the same [`Authenticator`] path the auth middleware uses and, on
//!   success, sets an `HttpOnly` session cookie the middleware accepts in
//!   place of the auth headers.
//! - `DELETE /ui/session` clears that cookie.
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
use common::auth::{
    SESSION_COOKIE, SessionData, TenantContextExtractor, encode_session, validate_dataset_id,
    validate_tenant_id,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Routes mounted at the router root (absolute `/ui/session` paths, so the
/// session endpoint coexists with the `/ui` static-asset service).
pub fn router<S: RouterState>() -> Router<S> {
    Router::new().route(
        "/ui/session",
        post(create_session::<S>).delete(delete_session),
    )
}

#[derive(Debug, Deserialize)]
pub struct CreateSessionRequest {
    pub api_key: String,
    pub tenant: String,
    #[serde(default)]
    pub dataset: Option<String>,
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

    match state
        .authenticator()
        .authenticate(&body.api_key, &tenant, dataset.as_deref())
        .await
    {
        Ok(ctx) => {
            tracing::info!(tenant_id = %ctx.tenant_id, dataset = %ctx.dataset_id, "UI session created");
            let cookie = encode_session(&SessionData {
                api_key: body.api_key,
                tenant,
                dataset,
            });
            (
                StatusCode::NO_CONTENT,
                [(
                    header::SET_COOKIE,
                    format!("{SESSION_COOKIE}={cookie}; HttpOnly; SameSite=Strict; Path=/"),
                )],
            )
                .into_response()
        }
        Err(err) => {
            tracing::warn!(tenant_id = %tenant, "UI session login failed: {}", err.message);
            error_response(err.status_code, err.message)
        }
    }
}

/// DELETE /ui/session
///
/// Clears the session cookie (logout).
pub async fn delete_session() -> Response {
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
    pub tenant: WhoamiTenant,
    pub datasets: Vec<WhoamiDataset>,
    pub default_dataset: Option<String>,
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
