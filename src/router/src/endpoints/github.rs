//! GitHub App management endpoints and browser callback (change:
//! github-app-source-context).
//!
//! Two surfaces live here:
//!
//! - The tenant-management endpoints (`start_github_link`,
//!   `list_github_installations`, `remove_github_installation`), mounted
//!   under `/api/v1/manage/tenants/{tenant_id}/github-installations` beside
//!   the rest of [`crate::endpoints::management`] and subject to the same
//!   `authorize_tenant` check and rate-limit layers.
//! - [`callback`], the App's registered callback URL
//!   (`GET /ui/github/callback`), which completes a link started by
//!   [`start_github_link`]. It is mounted at the router root (public: no
//!   tenant auth layer) because the browser arrives here straight from
//!   GitHub, carrying no `Authorization`/`X-Tenant-ID` headers — only a
//!   `signaldb_session` cookie and the query parameters GitHub appends.
//!
//! # Two independent checks bind the callback
//!
//! Neither check alone is sufficient (see the design's "Install linking is
//! bound to a CSRF-style state token" and "The link completes on a
//! server-handled browser redirect" decisions):
//!
//! 1. **State token + session binding** (the SignalDB side): the state
//!    token minted by `start_github_link` proves *which admin and tenant*
//!    asked; the `signaldb_session` cookie on the callback request proves
//!    *this browser* is that admin's (or, for an API-key-started flow, an
//!    admin of the state's tenant). Both are checked before any GitHub call
//!    is made.
//! 2. **Ownership via the user token** (the GitHub side): the callback
//!    exchanges GitHub's `code` for a user-to-server token and checks that
//!    the callback's `installation_id` actually appears in that user's own
//!    `GET /user/installations` — otherwise a learned or guessed
//!    installation id from another organization could be linked.
//!
//! # Redirect contract
//!
//! Every outcome is a 302. Success redirects to
//! `/integrations/github?github=linked&installation_id=<id>`. Every failure
//! redirects to `/integrations/github?github=error&reason=<reason>` with
//! `reason` drawn from a fixed, non-disclosing vocabulary (`state`,
//! `session`, `forbidden`, `github`, `permissions`, `internal`); the
//! specific cause is logged server-side at `warn` (or `error` for a catalog
//! failure) and never sent to the client.

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use chrono::Utc;
use common::auth::{TenantContextExtractor, generate_prefixed_token, sha256_hex};
use common::catalog::{GitHubLinkOutcome, MembershipRole, NewGitHubInstallation};
use serde::{Deserialize, Serialize};

use crate::RouterState;
use crate::endpoints::management::{ManageError, authorize_tenant, error};
use crate::endpoints::session;
use crate::github::write_permissions;

/// The three tenant-management routes, nested under `/manage` (final paths
/// `/api/v1/manage/tenants/{tenant_id}/github-installations...`) beside
/// [`crate::endpoints::management::router`].
pub fn manage_router<S: RouterState>() -> Router<S> {
    Router::new()
        .route(
            "/tenants/{tenant_id}/github-installations/link",
            post(start_github_link::<S>),
        )
        .route(
            "/tenants/{tenant_id}/github-installations",
            get(list_github_installations::<S>),
        )
        .route(
            "/tenants/{tenant_id}/github-installations/{installation_id}",
            delete(remove_github_installation::<S>),
        )
}

/// The App's install-flow callback, mounted at the router root (public —
/// see the module docs for why it carries no tenant auth layer).
pub fn callback_router<S: RouterState>() -> Router<S> {
    Router::new().route("/ui/github/callback", get(callback::<S>))
}

/// 201 response body for [`start_github_link`].
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct GitHubLinkStartResponse {
    /// GitHub's install page to redirect the admin's browser to. Carries
    /// the single-use state token as its `state` query parameter.
    pub install_url: String,
    /// RFC 3339 timestamp naming when the state token (and so this link
    /// attempt) expires.
    pub expires_at: String,
}

/// One linked installation, as reported by [`list_github_installations`].
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct GitHubInstallationResponse {
    pub installation_id: i64,
    pub account_login: String,
    pub account_type: String,
    /// The installation's covered repositories, as `"owner/name"` full
    /// names.
    pub repositories: Vec<String>,
    /// RFC 3339 timestamp of the last successful repository-list refresh.
    pub repositories_synced_at: String,
    /// `true` when the live GitHub refresh failed and `repositories` is the
    /// last successfully fetched copy rather than a fresh one.
    pub stale: bool,
    pub linked_by_github_login: Option<String>,
    /// GitHub's own installation-settings page for this installation.
    pub manage_url: String,
    /// RFC 3339 timestamp.
    pub created_at: String,
    /// RFC 3339 timestamp.
    pub updated_at: String,
}

/// 200 response body for [`list_github_installations`].
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct GitHubInstallationsResponse {
    /// `false` when `[github]` is absent (or failed to build) — the
    /// endpoint answers 200 rather than 404 so a caller can render "GitHub
    /// is not set up" without special-casing an error status.
    pub configured: bool,
    /// The App's URL slug, present only when `configured`.
    pub app_slug: Option<String>,
    pub installations: Vec<GitHubInstallationResponse>,
}

/// `POST /api/v1/manage/tenants/{tenant_id}/github-installations/link`
///
/// Mints a single-use, tenant-and-admin-bound state token and returns the
/// GitHub install-flow URL carrying it. 404 when `[github]` is not
/// configured.
#[utoipa::path(
    post,
    path = "/api/v1/manage/tenants/{tenant_id}/github-installations/link",
    tag = "github",
    operation_id = "manage_start_github_link",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 201, description = "Link flow started", body = GitHubLinkStartResponse),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 404, description = "GitHub integration is not configured", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn start_github_link<S: RouterState>(
    State(state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
    Path(tenant_id): Path<String>,
) -> Response {
    if let Err((status, message)) = authorize_tenant(&ctx, &tenant_id) {
        return error(status, message);
    }
    let Some(app) = state.github() else {
        return error(
            StatusCode::NOT_FOUND,
            "GitHub integration is not configured",
        );
    };
    let token = generate_prefixed_token("ghls_", 32);
    let record = match state
        .catalog()
        .create_github_link_state(
            &sha256_hex(&token),
            &tenant_id,
            ctx.user_id.as_deref(),
            app.config().link_state_ttl,
        )
        .await
    {
        Ok(record) => record,
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, "GitHub link-state creation failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to start GitHub link",
            );
        }
    };
    tracing::info!(tenant_id, "GitHub install link started");
    (
        StatusCode::CREATED,
        Json(GitHubLinkStartResponse {
            install_url: app.config().install_url(&token),
            expires_at: record.expires_at.to_rfc3339(),
        }),
    )
        .into_response()
}

/// `GET /api/v1/manage/tenants/{tenant_id}/github-installations`
///
/// Always 200, even when `[github]` is unconfigured (`configured: false`).
/// When configured, refreshes each installation's repository list from
/// GitHub; a refresh failure falls back to the last stored list, marked
/// `stale: true`, rather than failing the whole request.
#[utoipa::path(
    get,
    path = "/api/v1/manage/tenants/{tenant_id}/github-installations",
    tag = "github",
    operation_id = "manage_list_github_installations",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Linked GitHub installations", body = GitHubInstallationsResponse),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn list_github_installations<S: RouterState>(
    State(state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
    Path(tenant_id): Path<String>,
) -> Response {
    if let Err((status, message)) = authorize_tenant(&ctx, &tenant_id) {
        return error(status, message);
    }
    let Some(app) = state.github() else {
        return Json(GitHubInstallationsResponse {
            configured: false,
            app_slug: None,
            installations: Vec::new(),
        })
        .into_response();
    };
    let stored = match state.catalog().list_github_installations(&tenant_id).await {
        Ok(installations) => installations,
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, "GitHub installation listing failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to list GitHub installations",
            );
        }
    };

    // Each installation's refresh is an independent GitHub round trip, so
    // run them concurrently rather than serially.
    let catalog = state.catalog();
    let installations = futures::future::join_all(stored.into_iter().map(|installation| {
        let tenant_id = tenant_id.as_str();
        async move {
            let (repositories, repositories_synced_at, stale) = match app
                .installation_repositories(installation.installation_id)
                .await
            {
                Ok(repos) => {
                    if let Err(catalog_error) = catalog
                        .update_github_installation_repositories(
                            tenant_id,
                            installation.installation_id,
                            &repos,
                        )
                        .await
                    {
                        tracing::warn!(
                            error = %catalog_error,
                            tenant_id,
                            installation_id = installation.installation_id,
                            "failed to persist refreshed GitHub repository list"
                        );
                    }
                    (repos, Utc::now(), false)
                }
                Err(github_error) => {
                    tracing::warn!(
                        error = %github_error,
                        tenant_id,
                        installation_id = installation.installation_id,
                        "GitHub repository refresh failed; serving the last known list"
                    );
                    (
                        installation.repositories.clone(),
                        installation.repositories_synced_at,
                        true,
                    )
                }
            };
            let manage_url = installation_manage_url(
                app.config().web_base(),
                &installation.account_type,
                &installation.account_login,
                installation.installation_id,
            );
            GitHubInstallationResponse {
                installation_id: installation.installation_id,
                account_login: installation.account_login,
                account_type: installation.account_type,
                repositories,
                repositories_synced_at: repositories_synced_at.to_rfc3339(),
                stale,
                linked_by_github_login: installation.linked_by_github_login,
                manage_url,
                created_at: installation.created_at.to_rfc3339(),
                updated_at: installation.updated_at.to_rfc3339(),
            }
        }
    }))
    .await;

    Json(GitHubInstallationsResponse {
        configured: true,
        app_slug: Some(app.config().app_slug.clone()),
        installations,
    })
    .into_response()
}

/// GitHub's own settings page for one installation: the org variant for an
/// `Organization` account, the personal-settings variant otherwise.
fn installation_manage_url(
    web_base: &str,
    account_type: &str,
    account_login: &str,
    installation_id: i64,
) -> String {
    if account_type == "Organization" {
        format!("{web_base}/organizations/{account_login}/settings/installations/{installation_id}")
    } else {
        format!("{web_base}/settings/installations/{installation_id}")
    }
}

/// `DELETE /api/v1/manage/tenants/{tenant_id}/github-installations/{installation_id}`
///
/// Removes the tenant's link and drops any cached installation token, so
/// token minting for that installation stops immediately (spec: "removal
/// takes effect immediately").
#[utoipa::path(
    delete,
    path = "/api/v1/manage/tenants/{tenant_id}/github-installations/{installation_id}",
    tag = "github",
    operation_id = "manage_remove_github_installation",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("installation_id" = i64, Path, description = "GitHub installation identifier"),
    ),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 204, description = "Installation link removed"),
        (status = 403, description = "Tenant administrator role or tenant:manage scope required, and the tenant must match the caller", body = ManageError),
        (status = 404, description = "GitHub installation not found for this tenant, or GitHub integration is not configured", body = ManageError),
        (status = 500, description = "Internal error", body = ManageError),
    )
)]
pub(crate) async fn remove_github_installation<S: RouterState>(
    State(state): State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
    Path((tenant_id, installation_id)): Path<(String, i64)>,
) -> Response {
    if let Err((status, message)) = authorize_tenant(&ctx, &tenant_id) {
        return error(status, message);
    }
    let Some(app) = state.github() else {
        return error(
            StatusCode::NOT_FOUND,
            "GitHub integration is not configured",
        );
    };
    let deleted = match state
        .catalog()
        .delete_github_installation(&tenant_id, installation_id)
        .await
    {
        Ok(deleted) => deleted,
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, tenant_id, installation_id, "GitHub installation removal failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to remove GitHub installation",
            );
        }
    };
    if !deleted {
        return error(StatusCode::NOT_FOUND, "GitHub installation not found");
    }
    // The source-context service (present whenever `app` is, since both
    // come from the same `Arc<GitHubApp>` — see `crate::build_github`)
    // forgets the installation's own cached token in addition to its
    // snippet cache entries; fall back to `app` directly on the off chance
    // it isn't.
    match state.source_context() {
        Some(source_context) => source_context.forget_installation(installation_id).await,
        None => app.forget_installation(installation_id).await,
    }
    tracing::info!(tenant_id, installation_id, "GitHub installation removed");
    StatusCode::NO_CONTENT.into_response()
}

/// Query parameters GitHub appends to the install-flow callback redirect
/// (`setup_action` is also sent but not needed here; serde ignores it).
#[derive(Debug, Deserialize)]
pub(crate) struct CallbackParams {
    /// OAuth-on-install authorization code, exchanged for a user token to
    /// verify installation ownership.
    #[serde(default)]
    pub code: Option<String>,
    /// The installation id GitHub reports, as a decimal string.
    #[serde(default)]
    pub installation_id: Option<String>,
    /// The state token [`start_github_link`] minted.
    #[serde(default)]
    pub state: Option<String>,
}

/// `GET /ui/github/callback`
///
/// The App's registered callback URL. See the module docs for the two
/// checks this performs and the redirect contract. Plain 404 (no redirect)
/// when `[github]` is not configured — there is nowhere safe to send the
/// browser back to in that case.
#[utoipa::path(
    get,
    path = "/ui/github/callback",
    tag = "github",
    operation_id = "github_callback",
    security(()),
    params(
        ("code" = Option<String>, Query, description = "OAuth-on-install authorization code"),
        ("installation_id" = Option<String>, Query, description = "The installation id GitHub reports"),
        ("setup_action" = Option<String>, Query, description = "GitHub's setup_action (install/update/request)"),
        ("state" = Option<String>, Query, description = "The state token issued by the link-start endpoint"),
    ),
    responses(
        (status = 302, description = "Redirects to /integrations/github?github=linked&installation_id=<id> on success, or /integrations/github?github=error&reason=<state|session|forbidden|github|permissions|internal> on failure"),
        (status = 404, description = "GitHub integration is not configured"),
    )
)]
pub(crate) async fn callback<S: RouterState>(
    State(state): State<S>,
    headers: HeaderMap,
    Query(params): Query<CallbackParams>,
) -> Response {
    let Some(app) = state.github() else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let (_token, user, _session) = match session::resolve_session_user(&state, &headers).await {
        Ok(resolved) => resolved,
        Err(_) => return redirect_error("session", "no valid session cookie"),
    };

    let Some(state_token) = params.state.as_deref().filter(|s| !s.is_empty()) else {
        return redirect_error("state", "missing state parameter");
    };
    let Some(installation_id) = params
        .installation_id
        .as_deref()
        .and_then(|raw| raw.parse::<i64>().ok())
    else {
        return redirect_error("state", "missing or non-numeric installation_id");
    };

    let state_hash = sha256_hex(state_token);
    let link = match state.catalog().get_github_link_state(&state_hash).await {
        Ok(Some(link)) => link,
        Ok(None) => return redirect_error("state", "unknown state token"),
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, "GitHub link-state lookup failed");
            return redirect_error("internal", "link-state lookup failed");
        }
    };
    if link.consumed_at.is_some() {
        return redirect_error("state", "state token already consumed");
    }
    if link.expires_at < Utc::now() {
        return redirect_error("state", "state token expired");
    }

    match &link.user_id {
        Some(started_by) if started_by == &user.id => {}
        Some(_) => {
            return redirect_error(
                "forbidden",
                "session user does not match the admin who started the flow",
            );
        }
        None => {
            let is_tenant_admin = match state
                .catalog()
                .get_tenant_membership(&user.id, &link.tenant_id)
                .await
            {
                Ok(Some(membership)) => membership.role == MembershipRole::Admin,
                Ok(None) => false,
                Err(catalog_error) => {
                    tracing::error!(error = %catalog_error, "GitHub link membership lookup failed");
                    return redirect_error("internal", "membership lookup failed");
                }
            };
            if !user.is_instance_admin && !is_tenant_admin {
                return redirect_error(
                    "forbidden",
                    "session user is not an admin of the state's tenant",
                );
            }
        }
    }

    let Some(code) = params.code.as_deref().filter(|c| !c.is_empty()) else {
        return redirect_error("state", "missing code parameter");
    };

    let user_token = match app.exchange_user_code(code).await {
        Ok(token) => token,
        Err(github_error) => {
            tracing::warn!(error = %github_error, "GitHub code exchange failed");
            return redirect_error("github", "code exchange failed");
        }
    };
    let github_user = match app.user(&user_token).await {
        Ok(user) => user,
        Err(github_error) => {
            tracing::warn!(error = %github_error, "GitHub user lookup failed");
            return redirect_error("github", "user lookup failed");
        }
    };
    let user_installations = match app.user_installations(&user_token).await {
        Ok(installations) => installations,
        Err(github_error) => {
            tracing::warn!(error = %github_error, "GitHub user-installations lookup failed");
            return redirect_error("github", "user-installations lookup failed");
        }
    };
    let Some(summary) = user_installations.iter().find(|installation| {
        installation.id == installation_id && installation.app_id == app.config().app_id as i64
    }) else {
        return redirect_error(
            "forbidden",
            "installation not owned by the authorizing GitHub user",
        );
    };

    let write_perms = write_permissions(&summary.permissions);
    if !write_perms.is_empty() {
        tracing::warn!(
            permissions = ?write_perms,
            "GitHub installation carries write-capable permissions; refusing link"
        );
        return redirect_error(
            "permissions",
            "installation carries a write-capable permission",
        );
    }
    let extra_read: Vec<&String> = summary
        .permissions
        .keys()
        .filter(|name| name.as_str() != "contents" && name.as_str() != "metadata")
        .collect();
    if !extra_read.is_empty() {
        tracing::warn!(
            permissions = ?extra_read,
            "GitHub installation carries unexpected read permissions"
        );
    }

    let repositories = match app.installation_repositories(installation_id).await {
        Ok(repositories) => repositories,
        Err(github_error) => {
            tracing::warn!(error = %github_error, "GitHub repository listing failed");
            return redirect_error("github", "repository listing failed");
        }
    };

    let new_installation = NewGitHubInstallation {
        installation_id,
        account_login: summary.account.login.clone(),
        account_type: summary.account.kind.clone(),
        account_id: summary.account.id,
        repositories,
        linked_by_user_id: Some(user.id.clone()),
        linked_by_github_login: Some(github_user.login.clone()),
    };
    match state
        .catalog()
        .complete_github_link(&state_hash, &new_installation)
        .await
    {
        Ok(GitHubLinkOutcome::Linked(record)) => {
            tracing::info!(
                tenant_id = %record.tenant_id,
                installation_id = record.installation_id,
                account = %record.account_login,
                "GitHub installation linked"
            );
            redirect(&format!(
                "/integrations/github?github=linked&installation_id={installation_id}"
            ))
        }
        Ok(GitHubLinkOutcome::StateRejected) => {
            redirect_error("state", "state token rejected at completion")
        }
        Err(catalog_error) => {
            tracing::error!(error = %catalog_error, "GitHub link completion failed");
            redirect_error("internal", "link completion failed")
        }
    }
}

/// A 302 to `location` (the same status the OIDC callback answers; an
/// unencodable header value would surface as axum's own 500).
fn redirect(location: &str) -> Response {
    (
        StatusCode::FOUND,
        [(header::LOCATION, location.to_string())],
    )
        .into_response()
}

/// Every callback failure funnels through here: logs `detail` (the specific
/// cause) at `warn`, then 302s to
/// `/integrations/github?github=error&reason=<reason>` with the generic,
/// non-disclosing `reason` code.
fn redirect_error(reason: &str, detail: &str) -> Response {
    tracing::warn!(reason, detail, "GitHub install-link callback rejected");
    redirect(&format!(
        "/integrations/github?github=error&reason={reason}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RouterAppState;
    use crate::create_router;
    use axum::body::Body;
    use axum::http::{Request, header};
    use common::auth::{Authenticator, TENANT_MANAGE_SCOPE};
    use common::catalog::{Catalog, MembershipRole};
    use common::config::{
        ApiKeyConfig, AuthConfig, Configuration, DatasetConfig, GitHubAppConfig, TenantConfig,
    };
    use serde_json::{Value, json};
    use tower::ServiceExt;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const MANAGE_KEY: &str = "sdbk_acme_manage";

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

    /// Builds a test app with tenant `acme` (an admin user, a member user,
    /// and a `tenant:manage`-scoped key) and tenant `globex`. `github` is
    /// `None` for the "unconfigured" tests and `Some` (pointed at a
    /// wiremock server) otherwise.
    async fn test_app(github: Option<GitHubAppConfig>) -> (axum::Router, Catalog, String, String) {
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
                &Authenticator::hash_api_key(MANAGE_KEY),
                Some(MANAGE_KEY),
                None,
                None,
                Some(&[TENANT_MANAGE_SCOPE.to_string()]),
                None,
            )
            .await
            .unwrap();
        let hash = common::auth::hash_password("test password").unwrap();
        let admin = catalog
            .create_user("admin@example.com", Some("Admin"), Some(&hash), false)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&admin.id, "acme", MembershipRole::Admin)
            .await
            .unwrap();
        let member = catalog
            .create_user("member@example.com", Some("Member"), Some(&hash), false)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&member.id, "acme", MembershipRole::Member)
            .await
            .unwrap();
        let app = create_router(RouterAppState::new(catalog.clone(), config));
        (app, catalog, admin.id, member.id)
    }

    async fn login(app: &axum::Router, email: &str) -> String {
        let request = Request::builder()
            .method("POST")
            .uri("/ui/session")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "email": email, "password": "test password", "tenant": "acme" })
                    .to_string(),
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let set_cookie = response
            .headers()
            .get(header::SET_COOKIE)
            .expect("Set-Cookie present")
            .to_str()
            .unwrap();
        set_cookie.split(';').next().unwrap().to_string()
    }

    async fn call_manage(
        app: &axum::Router,
        method_: axum::http::Method,
        uri: &str,
        credential: Credential<'_>,
    ) -> (StatusCode, Value) {
        let mut builder = Request::builder()
            .method(method_)
            .uri(uri)
            .header("x-tenant-id", "acme");
        builder = match credential {
            Credential::ApiKey(key) => builder.header("authorization", format!("Bearer {key}")),
            Credential::Cookie(cookie) => builder.header(header::COOKIE, cookie),
        };
        let response = app
            .clone()
            .oneshot(builder.body(Body::empty()).unwrap())
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

    enum Credential<'a> {
        ApiKey(&'a str),
        Cookie(String),
    }

    /// Starts a link as `credential` (expecting 201) and returns the state
    /// token carried by the install URL.
    async fn start_link(app: &axum::Router, credential: Credential<'_>) -> String {
        let (status, body) = call_manage(
            app,
            axum::http::Method::POST,
            "/api/v1/manage/tenants/acme/github-installations/link",
            credential,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        state_from_install_url(body["install_url"].as_str().unwrap())
    }

    /// Extracts the `state=` query value from an `install_url`.
    fn state_from_install_url(install_url: &str) -> String {
        url::Url::parse(install_url)
            .unwrap()
            .query_pairs()
            .find(|(k, _)| k == "state")
            .map(|(_, v)| v.into_owned())
            .expect("install_url carries a state parameter")
    }

    async fn callback_request(app: &axum::Router, cookie: Option<&str>, query: &str) -> Response {
        let mut builder = Request::builder()
            .uri(format!("/ui/github/callback?{query}"))
            .method("GET");
        if let Some(cookie) = cookie {
            builder = builder.header(header::COOKIE, cookie);
        }
        app.clone()
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    fn location(response: &Response) -> String {
        response
            .headers()
            .get(header::LOCATION)
            .expect("Location header present")
            .to_str()
            .unwrap()
            .to_string()
    }

    /// Mounts the token-exchange and `/user` mocks every callback test
    /// needs regardless of outcome (code exchange succeeds, resolving to
    /// `octocat`); callers mount their own `/user/installations` response on
    /// top to vary the ownership/permissions check.
    async fn mount_token_exchange_and_user(server: &MockServer) {
        Mock::given(method("POST"))
            .and(path("/login/oauth/access_token"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "access_token": "ghu_token" })),
            )
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/user"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "login": "octocat", "id": 1 })),
            )
            .mount(server)
            .await;
    }

    /// Mounts the default set of GitHub API mocks a happy-path link needs:
    /// token exchange, `/user`, `/user/installations` (installation 777,
    /// read-only permissions), an installation access token, and the
    /// installation's covered repositories.
    async fn mount_happy_path_mocks(server: &MockServer) {
        mount_token_exchange_and_user(server).await;
        Mock::given(method("GET"))
            .and(path("/user/installations"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "installations": [{
                    "id": 777,
                    "app_id": 4242,
                    "account": { "login": "octo-org", "id": 9, "type": "Organization" },
                    "permissions": { "contents": "read", "metadata": "read" },
                }],
            })))
            .mount(server)
            .await;
        crate::github::test_support::mount_installation_token(server, 777).await;
        Mock::given(method("GET"))
            .and(path("/installation/repositories"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "repositories": [
                    { "full_name": "octo-org/api" },
                    { "full_name": "octo-org/web" },
                ],
            })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn unconfigured_app_answers_404_and_list_reports_not_configured() {
        let (app, _catalog, admin_id, _member_id) = test_app(None).await;
        let cookie = login(&app, "admin@example.com").await;
        let _ = admin_id;

        let (status, _) = call_manage(
            &app,
            axum::http::Method::POST,
            "/api/v1/manage/tenants/acme/github-installations/link",
            Credential::Cookie(cookie.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (status, body) = call_manage(
            &app,
            axum::http::Method::GET,
            "/api/v1/manage/tenants/acme/github-installations",
            Credential::Cookie(cookie.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["configured"], false);

        let (status, _) = call_manage(
            &app,
            axum::http::Method::DELETE,
            "/api/v1/manage/tenants/acme/github-installations/777",
            Credential::Cookie(cookie),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let response = callback_request(&app, None, "state=x&installation_id=777&code=c").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn start_link_authorization_and_url_shape() {
        let server = MockServer::start().await;
        let (app, _catalog, _admin_id, _member_id) = test_app(Some(github_config(&server))).await;
        let admin_cookie = login(&app, "admin@example.com").await;
        let member_cookie = login(&app, "member@example.com").await;

        let (status, body) = call_manage(
            &app,
            axum::http::Method::POST,
            "/api/v1/manage/tenants/acme/github-installations/link",
            Credential::Cookie(admin_cookie),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let install_url = body["install_url"].as_str().unwrap();
        assert!(
            install_url.starts_with(&format!(
                "{}/apps/signaldb-test/installations/new?state=ghls_",
                server.uri()
            )),
            "{install_url}"
        );

        let (status, _) = call_manage(
            &app,
            axum::http::Method::POST,
            "/api/v1/manage/tenants/acme/github-installations/link",
            Credential::Cookie(member_cookie),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        let (status, _) = call_manage(
            &app,
            axum::http::Method::POST,
            "/api/v1/manage/tenants/acme/github-installations/link",
            Credential::ApiKey(MANAGE_KEY),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);

        // Mismatched path tenant: the caller's own tenant is `acme` (from
        // `X-Tenant-ID`); asking for `globex` in the path is forbidden.
        let (status, _) = call_manage(
            &app,
            axum::http::Method::POST,
            "/api/v1/manage/tenants/globex/github-installations/link",
            Credential::ApiKey(MANAGE_KEY),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn happy_path_link_list_remove() {
        let server = MockServer::start().await;
        mount_happy_path_mocks(&server).await;
        let (app, _catalog, _admin_id, _member_id) = test_app(Some(github_config(&server))).await;
        let admin_cookie = login(&app, "admin@example.com").await;

        let state_token = start_link(&app, Credential::Cookie(admin_cookie.clone())).await;

        let response = callback_request(
            &app,
            Some(&admin_cookie),
            &format!("state={state_token}&installation_id=777&code=abc&setup_action=install"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(
            location(&response),
            "/integrations/github?github=linked&installation_id=777"
        );

        let (status, body) = call_manage(
            &app,
            axum::http::Method::GET,
            "/api/v1/manage/tenants/acme/github-installations",
            Credential::Cookie(admin_cookie.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["configured"], true);
        let installations = body["installations"].as_array().unwrap();
        assert_eq!(installations.len(), 1);
        let installation = &installations[0];
        assert_eq!(installation["installation_id"], 777);
        assert_eq!(installation["stale"], false);
        assert_eq!(installation["linked_by_github_login"], "octocat");
        assert_eq!(
            installation["manage_url"],
            format!(
                "{}/organizations/octo-org/settings/installations/777",
                server.uri()
            )
        );
        let repos: Vec<String> = installation["repositories"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(repos, vec!["octo-org/api", "octo-org/web"]);

        let (status, _) = call_manage(
            &app,
            axum::http::Method::DELETE,
            "/api/v1/manage/tenants/acme/github-installations/777",
            Credential::Cookie(admin_cookie.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (status, body) = call_manage(
            &app,
            axum::http::Method::GET,
            "/api/v1/manage/tenants/acme/github-installations",
            Credential::Cookie(admin_cookie.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["installations"].as_array().unwrap().is_empty());

        let (status, _) = call_manage(
            &app,
            axum::http::Method::DELETE,
            "/api/v1/manage/tenants/acme/github-installations/777",
            Credential::Cookie(admin_cookie),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn callback_without_session_leaves_state_usable() {
        let server = MockServer::start().await;
        mount_happy_path_mocks(&server).await;
        let (app, _catalog, _admin_id, _member_id) = test_app(Some(github_config(&server))).await;
        let admin_cookie = login(&app, "admin@example.com").await;

        let (_, body) = call_manage(
            &app,
            axum::http::Method::POST,
            "/api/v1/manage/tenants/acme/github-installations/link",
            Credential::Cookie(admin_cookie.clone()),
        )
        .await;
        let state_token = state_from_install_url(body["install_url"].as_str().unwrap());
        let query = format!("state={state_token}&installation_id=777&code=abc");

        let response = callback_request(&app, None, &query).await;
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(
            location(&response),
            "/integrations/github?github=error&reason=session"
        );

        // The state token was never consumed, so completing it correctly
        // afterwards still succeeds.
        let response = callback_request(&app, Some(&admin_cookie), &query).await;
        assert_eq!(
            location(&response),
            "/integrations/github?github=linked&installation_id=777"
        );
    }

    #[tokio::test]
    async fn callback_in_foreign_session_is_forbidden_and_links_nothing() {
        let server = MockServer::start().await;
        mount_happy_path_mocks(&server).await;
        let (app, catalog, _admin_id, _member_id) = test_app(Some(github_config(&server))).await;
        let admin_cookie = login(&app, "admin@example.com").await;
        let member_cookie = login(&app, "member@example.com").await;

        let (_, body) = call_manage(
            &app,
            axum::http::Method::POST,
            "/api/v1/manage/tenants/acme/github-installations/link",
            Credential::Cookie(admin_cookie),
        )
        .await;
        let state_token = state_from_install_url(body["install_url"].as_str().unwrap());
        let query = format!("state={state_token}&installation_id=777&code=abc");

        let response = callback_request(&app, Some(&member_cookie), &query).await;
        assert_eq!(
            location(&response),
            "/integrations/github?github=error&reason=forbidden"
        );
        assert!(
            catalog
                .list_github_installations("acme")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn api_key_started_state_completed_by_admin_links_by_member_forbidden() {
        let server = MockServer::start().await;
        mount_happy_path_mocks(&server).await;
        let (app, _catalog, _admin_id, _member_id) = test_app(Some(github_config(&server))).await;
        let admin_cookie = login(&app, "admin@example.com").await;
        let member_cookie = login(&app, "member@example.com").await;

        // API-key-started: no session user, so the state token's user_id is
        // None.
        let (_, body) = call_manage(
            &app,
            axum::http::Method::POST,
            "/api/v1/manage/tenants/acme/github-installations/link",
            Credential::ApiKey(MANAGE_KEY),
        )
        .await;
        let state_token = state_from_install_url(body["install_url"].as_str().unwrap());
        let query = format!("state={state_token}&installation_id=777&code=abc");

        let response = callback_request(&app, Some(&admin_cookie), &query).await;
        assert_eq!(
            location(&response),
            "/integrations/github?github=linked&installation_id=777"
        );

        // A second API-key-started state, completed by the non-admin member.
        let (_, body) = call_manage(
            &app,
            axum::http::Method::POST,
            "/api/v1/manage/tenants/acme/github-installations/link",
            Credential::ApiKey(MANAGE_KEY),
        )
        .await;
        let state_token = state_from_install_url(body["install_url"].as_str().unwrap());
        let query = format!("state={state_token}&installation_id=777&code=abc");
        let response = callback_request(&app, Some(&member_cookie), &query).await;
        assert_eq!(
            location(&response),
            "/integrations/github?github=error&reason=forbidden"
        );
    }

    #[tokio::test]
    async fn reused_unknown_and_codeless_states_are_rejected() {
        let server = MockServer::start().await;
        mount_happy_path_mocks(&server).await;
        let (app, _catalog, _admin_id, _member_id) = test_app(Some(github_config(&server))).await;
        let admin_cookie = login(&app, "admin@example.com").await;

        let (_, body) = call_manage(
            &app,
            axum::http::Method::POST,
            "/api/v1/manage/tenants/acme/github-installations/link",
            Credential::Cookie(admin_cookie.clone()),
        )
        .await;
        let state_token = state_from_install_url(body["install_url"].as_str().unwrap());
        let query = format!("state={state_token}&installation_id=777&code=abc");

        let first = callback_request(&app, Some(&admin_cookie), &query).await;
        assert_eq!(
            location(&first),
            "/integrations/github?github=linked&installation_id=777"
        );
        let second = callback_request(&app, Some(&admin_cookie), &query).await;
        assert_eq!(
            location(&second),
            "/integrations/github?github=error&reason=state"
        );

        let unknown = callback_request(
            &app,
            Some(&admin_cookie),
            "state=never-issued&installation_id=777&code=abc",
        )
        .await;
        assert_eq!(
            location(&unknown),
            "/integrations/github?github=error&reason=state"
        );

        let (_, body) = call_manage(
            &app,
            axum::http::Method::POST,
            "/api/v1/manage/tenants/acme/github-installations/link",
            Credential::Cookie(admin_cookie.clone()),
        )
        .await;
        let state_token = state_from_install_url(body["install_url"].as_str().unwrap());
        let missing_code = callback_request(
            &app,
            Some(&admin_cookie),
            &format!("state={state_token}&installation_id=777"),
        )
        .await;
        assert_eq!(
            location(&missing_code),
            "/integrations/github?github=error&reason=state"
        );
    }

    #[tokio::test]
    async fn installation_not_owned_is_forbidden() {
        let server = MockServer::start().await;
        mount_token_exchange_and_user(&server).await;
        Mock::given(method("GET"))
            .and(path("/user/installations"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "installations": [] })))
            .mount(&server)
            .await;
        let (app, catalog, _admin_id, _member_id) = test_app(Some(github_config(&server))).await;
        let admin_cookie = login(&app, "admin@example.com").await;

        let (_, body) = call_manage(
            &app,
            axum::http::Method::POST,
            "/api/v1/manage/tenants/acme/github-installations/link",
            Credential::Cookie(admin_cookie.clone()),
        )
        .await;
        let state_token = state_from_install_url(body["install_url"].as_str().unwrap());
        let response = callback_request(
            &app,
            Some(&admin_cookie),
            &format!("state={state_token}&installation_id=777&code=abc"),
        )
        .await;
        assert_eq!(
            location(&response),
            "/integrations/github?github=error&reason=forbidden"
        );
        assert!(
            catalog
                .list_github_installations("acme")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn write_permission_is_refused() {
        let server = MockServer::start().await;
        mount_token_exchange_and_user(&server).await;
        Mock::given(method("GET"))
            .and(path("/user/installations"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "installations": [{
                    "id": 777,
                    "app_id": 4242,
                    "account": { "login": "octo-org", "id": 9, "type": "Organization" },
                    "permissions": { "contents": "write" },
                }],
            })))
            .mount(&server)
            .await;
        let (app, catalog, _admin_id, _member_id) = test_app(Some(github_config(&server))).await;
        let admin_cookie = login(&app, "admin@example.com").await;

        let (_, body) = call_manage(
            &app,
            axum::http::Method::POST,
            "/api/v1/manage/tenants/acme/github-installations/link",
            Credential::Cookie(admin_cookie.clone()),
        )
        .await;
        let state_token = state_from_install_url(body["install_url"].as_str().unwrap());
        let response = callback_request(
            &app,
            Some(&admin_cookie),
            &format!("state={state_token}&installation_id=777&code=abc"),
        )
        .await;
        assert_eq!(
            location(&response),
            "/integrations/github?github=error&reason=permissions"
        );
        assert!(
            catalog
                .list_github_installations("acme")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn token_exchange_failure_maps_to_github_reason() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/login/oauth/access_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "error": "bad_verification_code",
            })))
            .mount(&server)
            .await;
        let (app, _catalog, _admin_id, _member_id) = test_app(Some(github_config(&server))).await;
        let admin_cookie = login(&app, "admin@example.com").await;

        let (_, body) = call_manage(
            &app,
            axum::http::Method::POST,
            "/api/v1/manage/tenants/acme/github-installations/link",
            Credential::Cookie(admin_cookie.clone()),
        )
        .await;
        let state_token = state_from_install_url(body["install_url"].as_str().unwrap());
        let response = callback_request(
            &app,
            Some(&admin_cookie),
            &format!("state={state_token}&installation_id=777&code=abc"),
        )
        .await;
        assert_eq!(
            location(&response),
            "/integrations/github?github=error&reason=github"
        );
    }

    #[tokio::test]
    async fn list_refresh_failure_serves_stale_stored_repositories() {
        let server = MockServer::start().await;
        mount_happy_path_mocks(&server).await;
        let (app, _catalog, _admin_id, _member_id) = test_app(Some(github_config(&server))).await;
        let admin_cookie = login(&app, "admin@example.com").await;

        let (_, body) = call_manage(
            &app,
            axum::http::Method::POST,
            "/api/v1/manage/tenants/acme/github-installations/link",
            Credential::Cookie(admin_cookie.clone()),
        )
        .await;
        let state_token = state_from_install_url(body["install_url"].as_str().unwrap());
        callback_request(
            &app,
            Some(&admin_cookie),
            &format!("state={state_token}&installation_id=777&code=abc"),
        )
        .await;

        // Re-mount `/installation/repositories` at a higher priority than
        // the happy-path 200 mock (equal-priority mocks match in mount
        // order, so a plain re-mount would still hit the first one), so the
        // next refresh attempt fails while the installation token mint (a
        // separate mock, still healthy) is untouched.
        Mock::given(method("GET"))
            .and(path("/installation/repositories"))
            .respond_with(ResponseTemplate::new(500))
            .with_priority(1)
            .mount(&server)
            .await;

        let (status, body) = call_manage(
            &app,
            axum::http::Method::GET,
            "/api/v1/manage/tenants/acme/github-installations",
            Credential::Cookie(admin_cookie),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let installation = &body["installations"][0];
        assert_eq!(installation["stale"], true);
        let repos: Vec<String> = installation["repositories"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(repos, vec!["octo-org/api", "octo-org/web"]);
    }
}
