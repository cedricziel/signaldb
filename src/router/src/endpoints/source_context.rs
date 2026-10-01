//! Stack-frame source-context snippet lookup endpoint (change:
//! `github-app-source-context`).
//!
//! `POST /api/v1/tenants/{tenant_id}/source-context` is a runtime read
//! available to any authenticated tenant caller with read access to at
//! least one signal — not an admin/management operation, and not part of
//! the Query IR (see the design's "Source-context lookup is an internal
//! service call, not a new Query IR source" decision). It always answers
//! `200` for a well-formed request, carrying `status: "unavailable"` rather
//! than an error status when no snippet could be served, so a caller never
//! has to special-case this endpoint's failure modes to render a frame.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use common::auth::TenantContextExtractor;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::RouterAppState;
use crate::endpoints::api_error::ApiError;
use crate::source_context::{
    DEFAULT_CONTEXT_LINES, SourceLookup, SourceRequest, SourceSnippet, UnavailableReason,
};

/// Request body for [`source_context`].
#[derive(Debug, Deserialize, ToSchema)]
pub struct SourceContextRequest {
    /// `owner/name`, or a GitHub URL naming the repository (`https://github.com/owner/name`,
    /// `owner/name.git`, ...). Omit it to probe every repository covered by
    /// the tenant's linked GitHub installations by path alone.
    pub repository: Option<String>,
    /// The ref (branch, tag, or commit SHA) to read the file at. Omit it to
    /// read the repository's default branch.
    #[serde(rename = "ref")]
    pub git_ref: Option<String>,
    /// The file path within the repository.
    pub path: String,
    /// The 1-based line number to center the snippet on.
    pub line: u32,
    /// Lines of context on each side of `line`; defaults to
    /// [`DEFAULT_CONTEXT_LINES`] and is clamped to
    /// [`crate::source_context::MAX_CONTEXT_LINES`].
    pub context_lines: Option<u32>,
}

/// Whether [`source_context`] served a snippet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceContextStatus {
    /// `snippet` is populated.
    Available,
    /// `reason` is populated; `snippet` is `None`.
    Unavailable,
}

/// Response body for [`source_context`]. Always `200` for a well-formed
/// request, whether or not a snippet could be served.
#[derive(Debug, Serialize, ToSchema)]
pub struct SourceContextResponse {
    /// Whether a snippet was served.
    pub status: SourceContextStatus,
    /// Why no snippet was served; present only when `status` is
    /// `"unavailable"`.
    pub reason: Option<UnavailableReason>,
    /// The resolved snippet; present only when `status` is `"available"`.
    pub snippet: Option<SourceSnippet>,
}

impl From<SourceLookup> for SourceContextResponse {
    fn from(lookup: SourceLookup) -> Self {
        match lookup {
            SourceLookup::Available(snippet) => SourceContextResponse {
                status: SourceContextStatus::Available,
                reason: None,
                snippet: Some(snippet),
            },
            SourceLookup::Unavailable(reason) => SourceContextResponse {
                status: SourceContextStatus::Unavailable,
                reason: Some(reason),
                snippet: None,
            },
        }
    }
}

/// The source-context route, mounted at `/api/v1` beside
/// [`crate::endpoints::tenant::router`] (inside the auth and query-rate
/// layers applied to that nest in `create_router`).
pub fn router() -> Router<RouterAppState> {
    Router::new().route(
        "/tenants/{tenant_id}/source-context",
        get(source_context_availability).post(source_context),
    )
}

/// Whether source context can be served for a tenant at all — the UI's
/// read-level probe for showing or hiding "View source" (the installation
/// *list* is a management-only endpoint that ordinary readers cannot call).
#[derive(Debug, Serialize, ToSchema)]
pub struct SourceContextAvailability {
    /// `[github]` is configured on this deployment.
    pub configured: bool,
    /// The tenant has linked at least one GitHub App installation.
    pub linked: bool,
}

/// The read-access check every source-context call shares: the path tenant
/// must be the authenticated tenant, and the principal must be able to read
/// at least one signal (an ingest-only key is refused).
// The error *is* the response we would send — see `session.rs`'s
// `list_session_memberships` for why this stays unboxed.
#[allow(clippy::result_large_err)]
fn authorize_reader(ctx: &common::auth::TenantContext, tenant_id: &str) -> Result<(), Response> {
    if tenant_id != ctx.tenant_id {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "Requested tenant does not match authenticated tenant",
        )
        .into_response());
    }
    if !(ctx.can_read("traces") || ctx.can_read("logs") || ctx.can_read("profiles")) {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "Read access to traces, logs, or profiles is required",
        )
        .into_response());
    }
    Ok(())
}

/// `GET /api/v1/tenants/{tenant_id}/source-context`
///
/// Reports whether `[github]` is configured and whether the tenant has
/// linked any installation, so a read-only caller can decide to offer
/// source lookups without the management-scoped installation list.
#[utoipa::path(
    get,
    path = "/api/v1/tenants/{tenant_id}/source-context",
    tag = "github",
    operation_id = "source_context_availability",
    security(("bearerAuth" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier (must match the authenticated tenant)")),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Whether source context can be served for this tenant", body = SourceContextAvailability),
        (status = 403, description = "Requested tenant does not match the authenticated tenant, or the caller has no read access to any signal", body = crate::endpoints::api_error::ApiErrorBody),
        (status = 500, description = "Internal error", body = crate::endpoints::api_error::ApiErrorBody),
    )
)]
#[tracing::instrument(skip_all, fields(signaldb.tenant.id = %tenant_id))]
pub async fn source_context_availability(
    State(state): State<RouterAppState>,
    Path(tenant_id): Path<String>,
    TenantContextExtractor(ctx): TenantContextExtractor,
) -> Response {
    if let Err(response) = authorize_reader(&ctx, &tenant_id) {
        return response;
    }
    if state.source_context().is_none() {
        return Json(SourceContextAvailability {
            configured: false,
            linked: false,
        })
        .into_response();
    }
    match state.catalog().list_github_installations(&tenant_id).await {
        Ok(installations) => Json(SourceContextAvailability {
            configured: true,
            linked: !installations.is_empty(),
        })
        .into_response(),
        Err(error) => {
            tracing::error!(error = %error, tenant_id, "GitHub installation listing failed");
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to check GitHub installations",
            )
            .into_response()
        }
    }
}

/// `POST /api/v1/tenants/{tenant_id}/source-context`
///
/// Resolves a repo/ref/file/line reference to a bounded, cached source
/// snippet through the tenant's linked GitHub App installation(s). `400` on
/// a malformed request (empty `path`, `line: 0`, or an unsafe path —
/// containing a `..`, `.`, empty, or absolute segment — see
/// `crate::source_context::SourceRequest::new`); `403` when the path tenant
/// doesn't match the caller's authenticated tenant, or the caller has no
/// read access to any signal. Otherwise always `200`: an unconfigured
/// deployment, no covering installation, a missing file, or an out-of-range
/// line all answer `status: "unavailable"` with a `reason`, never an error —
/// the surrounding trace or profile view must render without the source
/// panel rather than fail.
#[utoipa::path(
    post,
    path = "/api/v1/tenants/{tenant_id}/source-context",
    tag = "github",
    operation_id = "source_context",
    security(("bearerAuth" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier (must match the authenticated tenant)")),
    request_body = SourceContextRequest,
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Snippet lookup result (available or unavailable, never an error for a well-formed request)", body = SourceContextResponse),
        (status = 400, description = "Empty path, line is zero, or an unsafe (traversal/absolute) path", body = crate::endpoints::api_error::ApiErrorBody),
        (status = 403, description = "Requested tenant does not match the authenticated tenant, or the caller has no read access to any signal", body = crate::endpoints::api_error::ApiErrorBody),
    )
)]
#[tracing::instrument(skip_all, fields(signaldb.tenant.id = %tenant_id))]
pub async fn source_context(
    State(state): State<RouterAppState>,
    Path(tenant_id): Path<String>,
    TenantContextExtractor(ctx): TenantContextExtractor,
    Json(body): Json<SourceContextRequest>,
) -> Response {
    if let Err(response) = authorize_reader(&ctx, &tenant_id) {
        return response;
    }

    let request = match SourceRequest::new(
        body.repository,
        body.git_ref,
        body.path,
        body.line,
        body.context_lines.unwrap_or(DEFAULT_CONTEXT_LINES),
    ) {
        Ok(request) => request,
        Err(error) => return ApiError::bad_request(error.to_string()).into_response(),
    };

    let Some(service) = state.source_context() else {
        return Json(SourceContextResponse::from(SourceLookup::Unavailable(
            UnavailableReason::NotConfigured,
        )))
        .into_response();
    };
    Json(SourceContextResponse::from(
        service.lookup(state.catalog(), &tenant_id, &request).await,
    ))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RouterAppState;
    use crate::create_router;
    use crate::github::test_support::{contents_file_body, mount_installation_token};
    use axum::body::Body;
    use axum::http::Request;
    use common::catalog::Catalog;
    use common::config::{
        ApiKeyConfig, AuthConfig, Configuration, DatasetConfig, GitHubAppConfig, TenantConfig,
    };
    use serde_json::{Value, json};
    use tower::ServiceExt;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ACME_KEY: &str = "acme-legacy-key";
    const ACME_MANAGE_KEY: &str = "acme-manage-key";

    fn tenant(id: &str) -> TenantConfig {
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
                key: format!("{id}-legacy-key"),
                name: Some("legacy".to_string()),
            }],
            schema_config: None,
            limits: None,
        }
    }

    fn github_config(server: &MockServer) -> GitHubAppConfig {
        common::testing::github_test_config(&server.uri())
    }

    /// Builds a test app with tenants `acme`/`globex`, an ingest-only key
    /// for `acme`, and (when `github` is `Some`) an installation-token mock
    /// for the two installations `seed_installations` links: 777 (acme,
    /// covering `octo/api`) and 888 (globex, covering `globex/secret`).
    async fn test_app(github: Option<GitHubAppConfig>) -> (axum::Router, Catalog) {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let config = Configuration {
            auth: AuthConfig {
                tenants: vec![tenant("acme"), tenant("globex")],
                ..Default::default()
            },
            github,
            ..Default::default()
        };
        catalog.sync_config_tenants(&config.auth).await.unwrap();
        catalog
            .upsert_scoped_api_key(
                "acme",
                &common::auth::Authenticator::hash_api_key("acme-ingest-only"),
                Some("acme-ingest-only"),
                None,
                None,
                Some(&["traces:write".to_string()]),
                None,
            )
            .await
            .unwrap();
        catalog
            .upsert_scoped_api_key(
                "acme",
                &common::auth::Authenticator::hash_api_key(ACME_MANAGE_KEY),
                Some("manage"),
                None,
                None,
                Some(&[common::auth::TENANT_MANAGE_SCOPE.to_string()]),
                None,
            )
            .await
            .unwrap();
        let app = create_router(RouterAppState::new(catalog.clone(), config));
        (app, catalog)
    }

    /// Links installation 777 (`octo/api`) to `acme` and 888
    /// (`globex/secret`) to `globex`, directly through the catalog (as the
    /// completed callback would).
    async fn seed_installations(catalog: &Catalog) {
        for (tenant_id, installation_id, repo) in
            [("acme", 777, "octo/api"), ("globex", 888, "globex/secret")]
        {
            let state_hash = format!("state-{installation_id}");
            catalog
                .create_github_link_state(
                    &state_hash,
                    tenant_id,
                    None,
                    std::time::Duration::from_secs(600),
                )
                .await
                .unwrap();
            catalog
                .complete_github_link(
                    &state_hash,
                    &common::catalog::NewGitHubInstallation {
                        installation_id,
                        account_login: "octo".to_string(),
                        account_type: "Organization".to_string(),
                        account_id: 1,
                        repositories: vec![repo.to_string()],
                        linked_by_user_id: None,
                        linked_by_github_login: None,
                    },
                )
                .await
                .unwrap();
        }
    }

    /// Mounts an installation-token mock for both installations
    /// `seed_installations` links (777 acme, 888 globex).
    async fn mount_installation_tokens(server: &MockServer) {
        for installation_id in [777, 888] {
            mount_installation_token(server, installation_id).await;
        }
    }

    async fn call(
        app: &axum::Router,
        tenant_id: &str,
        key: &str,
        body: serde_json::Value,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/tenants/{tenant_id}/source-context"))
            .header("authorization", format!("Bearer {key}"))
            .header("x-tenant-id", tenant_id)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, json)
    }

    async fn availability(app: &axum::Router, tenant_id: &str, key: &str) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/tenants/{tenant_id}/source-context"))
            .header("authorization", format!("Bearer {key}"))
            .header("x-tenant-id", tenant_id)
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    #[tokio::test]
    async fn availability_reports_configured_and_linked() {
        let server = MockServer::start().await;
        let (app, catalog) = test_app(Some(github_config(&server))).await;
        let (status, body) = availability(&app, "acme", ACME_KEY).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body, json!({ "configured": true, "linked": false }));

        seed_installations(&catalog).await;
        let (_, body) = availability(&app, "acme", ACME_KEY).await;
        assert_eq!(body, json!({ "configured": true, "linked": true }));

        // Wrong tenant in the path and an unconfigured deployment.
        let (status, _) = availability(&app, "globex", ACME_KEY).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (app, _) = test_app(None).await;
        let (status, body) = availability(&app, "acme", ACME_KEY).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({ "configured": false, "linked": false }));
    }

    #[tokio::test]
    async fn available_with_explicit_repository_ref_and_line() {
        let server = MockServer::start().await;
        let (app, catalog) = test_app(Some(github_config(&server))).await;
        seed_installations(&catalog).await;
        mount_installation_tokens(&server).await;

        let text = "one\ntwo\nthree\nfour\nfive\n";
        Mock::given(method("GET"))
            .and(path("/repos/octo/api/contents/src/f.rs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(contents_file_body(
                text,
                "sha-abc",
                "https://github.com/octo/api/blob/main/src/f.rs",
            )))
            .mount(&server)
            .await;

        let (status, body) = call(
            &app,
            "acme",
            ACME_KEY,
            json!({
                "repository": "octo/api",
                "ref": "main",
                "path": "src/f.rs",
                "line": 3,
                "context_lines": 1,
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["status"], "available");
        let snippet = &body["snippet"];
        assert_eq!(snippet["start_line"], 2);
        assert_eq!(snippet["lines"], json!(["two", "three", "four"]));
        assert_eq!(
            snippet["html_url"],
            "https://github.com/octo/api/blob/main/src/f.rs#L3"
        );
    }

    #[tokio::test]
    async fn available_with_github_url_repository() {
        let server = MockServer::start().await;
        let (app, catalog) = test_app(Some(github_config(&server))).await;
        seed_installations(&catalog).await;
        mount_installation_tokens(&server).await;

        Mock::given(method("GET"))
            .and(path("/repos/octo/api/contents/f.rs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(contents_file_body(
                "a\nb\nc\n",
                "sha",
                "https://github.com/octo/api/blob/main/f.rs",
            )))
            .mount(&server)
            .await;

        let (status, body) = call(
            &app,
            "acme",
            ACME_KEY,
            json!({
                "repository": "https://github.com/octo/api.git",
                "path": "f.rs",
                "line": 1,
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["status"], "available");
    }

    #[tokio::test]
    async fn path_only_resolution_finds_the_covering_repository() {
        let server = MockServer::start().await;
        let (app, catalog) = test_app(Some(github_config(&server))).await;
        seed_installations(&catalog).await;
        mount_installation_tokens(&server).await;

        Mock::given(method("GET"))
            .and(path("/repos/octo/api/contents/f.rs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(contents_file_body(
                "a\nb\n",
                "sha",
                "https://github.com/octo/api/blob/main/f.rs",
            )))
            .mount(&server)
            .await;

        let (status, body) =
            call(&app, "acme", ACME_KEY, json!({ "path": "f.rs", "line": 1 })).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["status"], "available");
        assert_eq!(body["snippet"]["repository"], "octo/api");
    }

    #[tokio::test]
    async fn cross_tenant_repository_is_indistinguishable_from_no_installation() {
        let server = MockServer::start().await;
        let (app, catalog) = test_app(Some(github_config(&server))).await;
        seed_installations(&catalog).await;
        mount_installation_tokens(&server).await;

        let (status, cross_tenant) = call(
            &app,
            "acme",
            ACME_KEY,
            json!({ "repository": "globex/secret", "path": "f.rs", "line": 1 }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, made_up) = call(
            &app,
            "acme",
            ACME_KEY,
            json!({ "repository": "nobody/nothing", "path": "f.rs", "line": 1 }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(cross_tenant, made_up);
        assert_eq!(cross_tenant["status"], "unavailable");
        assert_eq!(cross_tenant["reason"], "no_installation");
    }

    #[tokio::test]
    async fn unknown_path_is_not_found() {
        let server = MockServer::start().await;
        let (app, catalog) = test_app(Some(github_config(&server))).await;
        seed_installations(&catalog).await;
        mount_installation_tokens(&server).await;

        Mock::given(method("GET"))
            .and(path("/repos/octo/api/contents/missing.rs"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({"message": "Not Found"})))
            .mount(&server)
            .await;

        let (status, body) = call(
            &app,
            "acme",
            ACME_KEY,
            json!({ "repository": "octo/api", "path": "missing.rs", "line": 1 }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "unavailable");
        assert_eq!(body["reason"], "not_found");
    }

    #[tokio::test]
    async fn unconfigured_app_answers_not_configured() {
        let (app, _catalog) = test_app(None).await;

        let (status, body) = call(
            &app,
            "acme",
            ACME_KEY,
            json!({ "repository": "octo/api", "path": "f.rs", "line": 1 }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "unavailable");
        assert_eq!(body["reason"], "not_configured");
    }

    #[tokio::test]
    async fn ingest_only_key_is_forbidden() {
        let server = MockServer::start().await;
        let (app, catalog) = test_app(Some(github_config(&server))).await;
        seed_installations(&catalog).await;

        let (status, _) = call(
            &app,
            "acme",
            "acme-ingest-only",
            json!({ "repository": "octo/api", "path": "f.rs", "line": 1 }),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn wrong_path_tenant_is_forbidden() {
        let server = MockServer::start().await;
        let (app, catalog) = test_app(Some(github_config(&server))).await;
        seed_installations(&catalog).await;

        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/tenants/globex/source-context")
            .header("authorization", format!("Bearer {ACME_KEY}"))
            .header("x-tenant-id", "acme")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "repository": "octo/api", "path": "f.rs", "line": 1 }).to_string(),
            ))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn zero_line_is_bad_request() {
        let server = MockServer::start().await;
        let (app, catalog) = test_app(Some(github_config(&server))).await;
        seed_installations(&catalog).await;

        let (status, _) = call(
            &app,
            "acme",
            ACME_KEY,
            json!({ "repository": "octo/api", "path": "f.rs", "line": 0 }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn unsafe_path_is_bad_request() {
        let server = MockServer::start().await;
        let (app, catalog) = test_app(Some(github_config(&server))).await;
        seed_installations(&catalog).await;

        let (status, _) = call(
            &app,
            "acme",
            ACME_KEY,
            json!({ "repository": "octo/api", "path": "../x", "line": 1 }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn removing_installation_forgets_the_cache() {
        let server = MockServer::start().await;
        let (app, catalog) = test_app(Some(github_config(&server))).await;
        seed_installations(&catalog).await;
        mount_installation_tokens(&server).await;

        Mock::given(method("GET"))
            .and(path("/repos/octo/api/contents/f.rs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(contents_file_body(
                "a\nb\n",
                "sha",
                "https://github.com/octo/api/blob/main/f.rs",
            )))
            .mount(&server)
            .await;

        let (status, body) = call(
            &app,
            "acme",
            ACME_KEY,
            json!({ "repository": "octo/api", "path": "f.rs", "line": 1 }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "available");

        let request = Request::builder()
            .method("DELETE")
            .uri("/api/v1/tenants/acme/github-installations/777")
            .header("authorization", format!("Bearer {ACME_MANAGE_KEY}"))
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // The mock still serves the file (the installation row and token
        // are gone, but nothing stops a fresh mint attempt from working) —
        // this must now be `no_installation`, not `available`, because the
        // catalog no longer resolves `octo/api` to any of acme's
        // installations.
        let (status, body) = call(
            &app,
            "acme",
            ACME_KEY,
            json!({ "repository": "octo/api", "path": "f.rs", "line": 1 }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "unavailable");
        assert_eq!(body["reason"], "no_installation");
    }
}
