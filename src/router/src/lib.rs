use axum::{Router, http::StatusCode, middleware, response::IntoResponse, routing::get};
use common::auth::{Authenticator, TenantContext, admin_auth_middleware, auth_middleware};
use common::catalog::Catalog;
use common::config::Configuration;
use common::ratelimit::TenantRateLimiter;
use common::schema_registry::SchemaResolver;
use std::sync::Arc;

pub mod cli;
pub mod demo_guard;
pub mod discovery;
pub mod endpoints;
pub mod github;
pub mod oidc;
pub mod openapi;
pub mod read_scope;
pub mod source_context;
pub mod ui;

/// Mount prefixes for the Tempo/Loki/Prometheus/Pyroscope compatibility
/// dialects and the native query surface, shared between the `.nest()` calls
/// in [`create_router`] and `endpoints::session::connection_info`'s
/// `ConnectionQuery`/`ConnectionCompat` response, so the two can never drift.
pub(crate) const TEMPO_PREFIX: &str = "/tempo";
pub(crate) const LOKI_PREFIX: &str = "/loki";
pub(crate) const PROMETHEUS_PREFIX: &str = "/prometheus";
pub(crate) const PYROSCOPE_PREFIX: &str = "/pyroscope";
pub(crate) const QUERY_IR_PATH: &str = "/api/v1/query";
pub(crate) const OPENAPI_JSON_PATH: &str = "/api/v1/openapi.json";

/// Concrete state that every route handler depends on: the catalog, service
/// registry, configuration, and authenticator, plus the shared handles built
/// from them.
type SharedCatalogManager = tokio::sync::OnceCell<Arc<common::CatalogManager>>;

/// The router's `CatalogManager`, carrying the catalog as tenant source.
///
/// Built once on first use and reused, so per-request callers stop opening a
/// new connection pool each time. Nothing in it needs invalidating: the
/// router's config is fixed at startup, and tenants/datasets are read live
/// from the tenant source on every lookup. A failed build is not cached.
async fn catalog_manager(state: &RouterAppState) -> anyhow::Result<Arc<common::CatalogManager>> {
    state
        .catalog_manager
        .get_or_try_init(|| async {
            let manager = common::CatalogManager::new(state.config().clone()).await?;
            Ok(Arc::new(
                manager.with_tenant_source(Arc::new(state.catalog().clone())),
            ))
        })
        .await
        .cloned()
}

/// A [`common::tenant_api::TenantApi`] over the shared `CatalogManager`.
pub(crate) async fn tenant_api(
    state: &RouterAppState,
) -> anyhow::Result<common::tenant_api::TenantApi> {
    Ok(common::tenant_api::TenantApi::new(state.config().clone())
        .with_catalog_manager(catalog_manager(state).await?))
}

#[derive(Clone)]
pub struct RouterAppState {
    catalog: Catalog,
    service_registry: discovery::ServiceRegistry,
    config: Configuration,
    authenticator: Arc<Authenticator>,
    schema_resolver: SchemaResolver,
    processor_registry: Arc<common::processors::ProcessorRegistry>,
    oidc: Option<Arc<oidc::OidcRuntime>>,
    github: Option<Arc<github::GitHubApp>>,
    source_context: Option<Arc<source_context::SourceContextService>>,
    catalog_manager: Arc<SharedCatalogManager>,
}

impl std::fmt::Debug for RouterAppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterAppState")
            .field("catalog", &"Catalog")
            .field("service_registry", &self.service_registry)
            .field("config", &"Configuration")
            .field("authenticator", &"Authenticator")
            .field("oidc", &self.oidc.is_some())
            .field("github", &self.github.is_some())
            .field("source_context", &self.source_context.is_some())
            .finish()
    }
}

impl RouterAppState {
    pub fn new(catalog: Catalog, config: Configuration) -> Self {
        let mut service_registry = discovery::ServiceRegistry::new(catalog.clone());
        if let Some(discovery_config) = &config.discovery {
            service_registry = service_registry.with_discovery_ttl(discovery_config.ttl);
        }
        let authenticator = Arc::new(
            Authenticator::new(config.auth.clone(), Arc::new(catalog.clone()))
                .with_mcp_resource(config.mcp.oauth.resource_url.clone()),
        );
        let oidc = spawn_oidc_runtime(&config);
        let (github, source_context) = build_github(&config);

        Self {
            schema_resolver: SchemaResolver::new(catalog.clone()),
            processor_registry: Arc::new(common::processors::ProcessorRegistry::new(
                Arc::new(catalog.clone()),
                &config.processors,
            )),
            catalog,
            service_registry,
            config,
            authenticator,
            oidc,
            github,
            source_context,
            catalog_manager: Arc::default(),
        }
    }

    pub fn new_with_flight_transport(
        catalog: Catalog,
        config: Configuration,
        flight_transport: common::flight::transport::InMemoryFlightTransport,
    ) -> Self {
        let mut service_registry =
            discovery::ServiceRegistry::with_flight_transport(catalog.clone(), flight_transport);
        if let Some(discovery_config) = &config.discovery {
            service_registry = service_registry.with_discovery_ttl(discovery_config.ttl);
        }
        let authenticator = Arc::new(
            Authenticator::new(config.auth.clone(), Arc::new(catalog.clone()))
                .with_mcp_resource(config.mcp.oauth.resource_url.clone()),
        );
        let oidc = spawn_oidc_runtime(&config);
        let (github, source_context) = build_github(&config);

        Self {
            schema_resolver: SchemaResolver::new(catalog.clone()),
            processor_registry: Arc::new(common::processors::ProcessorRegistry::new(
                Arc::new(catalog.clone()),
                &config.processors,
            )),
            catalog,
            service_registry,
            config,
            authenticator,
            oidc,
            github,
            source_context,
            catalog_manager: Arc::default(),
        }
    }
}

/// Spawn the OIDC runtime for a configured `[auth.oidc]` section (change:
/// oidc-login). Discovery runs entirely in the background (design decision
/// 10), so this never blocks or fails construction — an unreachable issuer
/// leaves the runtime in its `unavailable` state until a retry succeeds.
fn spawn_oidc_runtime(config: &Configuration) -> Option<Arc<oidc::OidcRuntime>> {
    config.auth.oidc.clone().map(oidc::OidcRuntime::spawn)
}

/// Build the GitHub App client, and the source-context service wrapping it,
/// for a configured `[github]` section (change: github-app-source-context).
/// `GitHubApp::new` fails only on a private key that can't be parsed; since
/// [`common::config::GitHubAppConfig::validate`] already parses that key at
/// config-validation time (startup), this should never fail in practice —
/// but if it somehow does, this logs the failure and degrades both handles
/// to `None` rather than panicking: the GitHub endpoints then answer 404,
/// same as when `[github]` is absent. The two handles always agree (`Some`
/// together or `None` together) because the second is built from the
/// first — the one place that invariant needs to hold.
fn build_github(
    config: &Configuration,
) -> (
    Option<Arc<github::GitHubApp>>,
    Option<Arc<source_context::SourceContextService>>,
) {
    let Some(github_config) = config.github.clone() else {
        return (None, None);
    };
    let app = match github::GitHubApp::new(github_config) {
        Ok(app) => Arc::new(app),
        Err(error) => {
            tracing::error!(%error, "[github] is configured but the GitHub App client failed to build; GitHub endpoints will answer 404");
            return (None, None);
        }
    };
    let source_context = Arc::new(source_context::SourceContextService::new(app.clone()));
    (Some(app), Some(source_context))
}

impl RouterAppState {
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn service_registry(&self) -> &discovery::ServiceRegistry {
        &self.service_registry
    }

    pub fn config(&self) -> &Configuration {
        &self.config
    }

    pub fn authenticator(&self) -> &Arc<Authenticator> {
        &self.authenticator
    }

    pub fn schema_resolver(&self) -> SchemaResolver {
        self.schema_resolver.clone()
    }

    pub fn processor_registry(&self) -> Arc<common::processors::ProcessorRegistry> {
        self.processor_registry.clone()
    }

    /// Share an externally built processor registry, e.g. the one the
    /// acceptor's ingest handlers use, so processors created through the
    /// router are visible on the ingest path.
    pub fn with_processor_registry(
        mut self,
        registry: Arc<common::processors::ProcessorRegistry>,
    ) -> Self {
        self.processor_registry = registry;
        self
    }

    pub fn oidc(&self) -> Option<&Arc<oidc::OidcRuntime>> {
        self.oidc.as_ref()
    }

    pub fn github(&self) -> Option<&Arc<github::GitHubApp>> {
        self.github.as_ref()
    }

    pub fn source_context(&self) -> Option<&Arc<source_context::SourceContextService>> {
        self.source_context.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn catalog_manager_cell(&self) -> &SharedCatalogManager {
        &self.catalog_manager
    }
}

/// Whether `path` (relative to the `/api/v1` nest — `Router::nest` strips
/// that prefix from the request `Uri` before an inner layer like
/// `auth_layer` below ever sees it) is eligible for the break-glass
/// `admin_api_key` bypass: the tenant-identity resource
/// (`/tenants`, `/tenants/{id}`), human users (`/users`), or the
/// tenant-scoped API-key/dataset/membership admin surface under
/// `/tenants/{id}/...` — matched by route, not by any privilege-scope path
/// segment (issue #1561 follow-up: there is no `/manage` or `/manage/admin`
/// prefix left to match on). Every other `/tenants/{id}/...` path (`tables`,
/// `schemas`, `source-context`, and `github-installations`) is deliberately
/// excluded: those stay tenant-credential-only. `github-installations` in
/// particular must stay out of this bypass even though its handlers accept
/// the admin key — they take `TenantContextExtractor`, which this bypass
/// leaves unset, and 500 without it (issue #1685 follow-up).
fn is_admin_key_bypass_path(path: &str) -> bool {
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    matches!(
        segments.as_slice(),
        ["tenants"]
            | ["tenants", _]
            | ["tenants", _, "api-keys", ..]
            | ["tenants", _, "datasets", ..]
            | ["tenants", _, "memberships", ..]
            | ["users"]
    )
}

/// Create a new router instance with all routes configured
pub fn create_router(state: RouterAppState) -> Router {
    // The break-glass admin key, hashed once, shared by the auth layer's
    // admin-key bypass below and by `ops_auth_layer`.
    let admin_key_hash = state
        .config()
        .auth
        .admin_api_key
        .as_ref()
        .map(|key| Authenticator::hash_api_key(key));

    // Create auth middleware layer. `admin_key_hash` lets a break-glass
    // request through untouched (no TenantContext attached) when it targets
    // one of `is_admin_key_bypass_path`'s routes and its bearer token
    // matches the configured admin key — regardless of whether `X-Tenant-ID`
    // is *also* present (the MCP server forwards both) — the normal auth
    // flow this layer otherwise runs requires a tenant for every bearer
    // credential, which the admin key deliberately has none of. The actual
    // authorization decision (does this request in fact carry that key, or
    // an instance-admin tenant credential) stays in each handler via
    // `endpoints::authz`, not here.
    let authenticator = state.authenticator().clone();
    let admin_key_bypass_hash = admin_key_hash.clone();
    let auth_layer =
        middleware::from_fn(move |req: axum::extract::Request, next: middleware::Next| {
            let authenticator = authenticator.clone();
            let admin_key_hash = admin_key_bypass_hash.clone();
            async move {
                if is_admin_key_bypass_path(req.uri().path())
                    && let Some(expected_hash) = admin_key_hash.as_deref()
                    && let Some(token) = req
                        .headers()
                        .get(axum::http::header::AUTHORIZATION)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.strip_prefix("Bearer "))
                    && Authenticator::hash_api_key(token) == expected_hash
                {
                    return next.run(req).await;
                }
                auth_middleware(authenticator, req, next).await
            }
        });

    // Per-tenant query request-rate limiting, applied after authentication
    // (it reads the TenantContext the auth layer inserts). Tenants without
    // a max_query_requests_per_sec limit are unaffected.
    let query_limiter = Arc::new(TenantRateLimiter::from_auth_config(&state.config().auth));
    let query_rate_layer =
        middleware::from_fn(move |req: axum::extract::Request, next: middleware::Next| {
            let limiter = query_limiter.clone();
            async move {
                if let Some(ctx) = req.extensions().get::<TenantContext>()
                    && let Err(e) = limiter.check_query(&ctx.tenant_id)
                {
                    let retry_after_ms = e.retry_after_secs().saturating_mul(1_000);
                    tracing::warn!(
                        tenant_id = %ctx.tenant_id,
                        surface = "query",
                        retry_after_ms,
                        "Query request rate limited"
                    );
                    common::self_monitoring::record_rate_limit_rejection("query", e.kind.as_str());
                    return endpoints::api_error::ApiError::rate_limited(&e).into_response();
                }
                next.run(req).await
            }
        });

    // Create admin auth middleware layer for operational control (compaction).
    // The old dedicated admin API was removed (issue #1561): instance-admin
    // operations live on the same plain resource paths as everything else
    // (`/api/v1/tenants`, `/api/v1/users`, ...), authenticated per-handler via
    // `endpoints::authz::require_instance_admin_or_admin_key` (which also
    // accepts this same admin key, tenant-less — see `auth_layer` above).
    let admin_authenticator = state.authenticator().clone();
    let ops_auth_layer = middleware::from_fn(move |req, next| {
        admin_auth_middleware(
            admin_key_hash.clone(),
            admin_authenticator.clone(),
            req,
            next,
        )
    });

    // Serialize the OpenAPI spec once at startup; served as pre-encoded bytes
    // so each request only bumps a refcount instead of re-serializing (and
    // previously deep-cloning) the whole document.
    let openapi_spec = load_openapi_spec();

    // OAuth 2.1 authorization-server surface (change: mcp-oauth-dcr). Mounted
    // only when enabled, so a plain deployment exposes no OAuth endpoints.
    let oauth_routes = if state.config().mcp.oauth.enabled {
        endpoints::oauth::router()
    } else {
        Router::new()
    };

    Router::new()
        // Public health check endpoint (no authentication)
        .route("/health", get(health_check))
        // OpenAPI spec endpoint (public)
        .route(
            OPENAPI_JSON_PATH,
            get(move || {
                let spec = openapi_spec.clone();
                async move {
                    (
                        [(axum::http::header::CONTENT_TYPE, "application/json")],
                        spec,
                    )
                }
            }),
        )
        // Protected routes with authentication
        .nest(
            TEMPO_PREFIX,
            endpoints::tempo::router()
                .layer(middleware::from_fn(|req, next| {
                    read_scope::require_read_scope("traces", req, next)
                }))
                .layer(query_rate_layer.clone())
                .layer(auth_layer.clone()),
        )
        // Pyroscope-compatible profile query API
        .nest(
            PYROSCOPE_PREFIX,
            endpoints::pyroscope::router()
                .layer(middleware::from_fn(|req, next| {
                    read_scope::require_read_scope("profiles", req, next)
                }))
                .layer(query_rate_layer.clone())
                .layer(auth_layer.clone()),
        )
        // Loki-compatible log query API (LogQL)
        .nest(
            LOKI_PREFIX,
            endpoints::logql::router()
                .layer(middleware::from_fn(|req, next| {
                    read_scope::require_read_scope("logs", req, next)
                }))
                .layer(query_rate_layer.clone())
                .layer(auth_layer.clone()),
        )
        // Prometheus-compatible metrics query API (PromQL)
        .nest(
            PROMETHEUS_PREFIX,
            endpoints::promql::router()
                .layer(middleware::from_fn(|req, next| {
                    read_scope::require_read_scope("metrics", req, next)
                }))
                .layer(query_rate_layer.clone())
                .layer(auth_layer.clone()),
        )
        // Trace-to-profile correlation
        .nest(
            "/api/profiles",
            endpoints::pyroscope::profiles_router()
                .layer(middleware::from_fn(|req, next| {
                    read_scope::require_read_scope("profiles", req, next)
                }))
                .layer(query_rate_layer.clone())
                .layer(auth_layer.clone()),
        )
        // UI session login/logout (public; sets/clears the HttpOnly session
        // cookie the auth middleware accepts in place of auth headers)
        .merge(endpoints::session::router())
        // OIDC SSO login (public; change: oidc-login) — 404s on every route
        // when `[auth.oidc]` is absent.
        .merge(endpoints::oidc::router())
        // GitHub App install-flow callback (public; change:
        // github-app-source-context) — 404s when `[github]` is absent (see
        // `endpoints::github::callback`).
        .merge(endpoints::github::callback_router())
        // OAuth 2.1 authorization-server endpoints (public: discovery + DCR are
        // unauthenticated by spec; empty unless mcp.oauth.enabled)
        .merge(oauth_routes)
        // Admin routes with admin authentication
        // Operational control (compaction), admin-authenticated, proxied to the
        // compactor's Flight do_action surface.
        .nest(
            "/api/v1/ops",
            endpoints::ops::router().layer(ops_auth_layer),
        )
        .nest(
            "/api/v1",
            endpoints::tenant::router()
                .merge(endpoints::tenants::router())
                .merge(endpoints::source_context::router())
                .merge(endpoints::management::router())
                .merge(endpoints::github::manage_router())
                .route("/schema", get(endpoints::schema::get_schema))
                .nest("/schema", endpoints::schema::router())
                .merge(endpoints::processors::router())
                .route("/whoami", get(endpoints::session::whoami))
                .route("/connection", get(endpoints::session::connection_info))
                .merge(endpoints::query::router())
                .layer(query_rate_layer)
                .layer(auth_layer),
        )
        // Explore UI at root (SPA fallback): every request not matched by an
        // API route above serves the UI — its `/runtime-config.js` route and,
        // for any other path, `index.html` so SPA deep links (incl.
        // `/oauth/consent`) boot the app. Served from SIGNALDB_UI_DIR.
        .fallback_service(ui::service_from_env(
            &state.config().self_monitoring.frontend,
            &state.config().self_monitoring.environment,
        ))
        // Demo-account write guard (change: demo-mode): a no-op unless
        // `[demo]` is enabled, and a no-op for every session but the demo
        // user's own — see `demo_guard` for the allowlist.
        .layer(middleware::from_fn_with_state(
            state.clone(),
            demo_guard::demo_write_guard,
        ))
        // OTel HTTP server metrics for all routes (no-op unless
        // self-monitoring is enabled)
        .layer(middleware::from_fn(
            common::self_monitoring::http_metrics_middleware,
        ))
        // Outermost: root each request in a server span parented to the
        // caller's W3C trace context, so external callers' traces join
        // SignalDB's query trace (no-op unless self-monitoring is enabled).
        .layer(middleware::from_fn(
            common::self_monitoring::http_trace_context_middleware,
        ))
        .with_state(state)
}

/// Create a new Flight service instance
pub fn create_flight_service(state: RouterAppState) -> endpoints::flight::SignalDBFlightService {
    endpoints::flight::SignalDBFlightService::new(state)
}

/// Basic health check endpoint
async fn health_check() -> impl IntoResponse {
    StatusCode::OK
}

/// Serve the code-first OpenAPI document assembled from the handler annotations
/// (see [`crate::openapi`]). This is the same document checked into
/// `api/signaldb-api.json` by the golden test, so the served spec can never
/// drift from the code.
fn load_openapi_spec() -> axum::body::Bytes {
    let json = serde_json::to_vec(&crate::openapi::openapi_document())
        .expect("OpenAPI document must serialize to JSON");
    axum::body::Bytes::from(json)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use common::config::{ApiKeyConfig, TenantConfig, TenantLimits};
    use tower::ServiceExt;

    #[test]
    fn admin_key_bypass_path_covers_plain_tenant_and_user_resource_paths() {
        assert!(is_admin_key_bypass_path("/tenants"));
        assert!(is_admin_key_bypass_path("/tenants/acme"));
        assert!(is_admin_key_bypass_path("/users"));
        assert!(is_admin_key_bypass_path("/tenants/acme/api-keys"));
        assert!(is_admin_key_bypass_path("/tenants/acme/api-keys/key-1"));
        assert!(is_admin_key_bypass_path("/tenants/acme/datasets"));
        assert!(is_admin_key_bypass_path("/tenants/acme/datasets/staging"));
        assert!(is_admin_key_bypass_path("/tenants/acme/memberships"));
        assert!(is_admin_key_bypass_path("/tenants/acme/memberships/u1"));

        // Data-plane tenant paths, and github-installations (whose handlers
        // take a TenantContextExtractor and 500 without one — issue #1685
        // follow-up), stay tenant-credential-only.
        assert!(!is_admin_key_bypass_path("/tenants/acme/tables"));
        assert!(!is_admin_key_bypass_path("/tenants/acme/schemas"));
        assert!(!is_admin_key_bypass_path("/tenants/acme/source-context"));
        assert!(!is_admin_key_bypass_path(
            "/tenants/acme/github-installations"
        ));
        assert!(!is_admin_key_bypass_path("/query"));
    }

    fn test_config(query_limit: Option<u32>) -> Configuration {
        let mut config = Configuration::default();
        config.auth = common::config::AuthConfig {
            default_limits: TenantLimits {
                max_query_requests_per_sec: query_limit,
                burst_seconds: 1.0,
                ..Default::default()
            },
            tenants: vec![TenantConfig {
                id: "acme".to_string(),
                slug: "acme".to_string(),
                name: "Acme".to_string(),
                default_dataset: Some("default".to_string()),
                datasets: vec![],
                api_keys: vec![ApiKeyConfig {
                    key: "sk-test-key".to_string(),
                    name: Some("test".to_string()),
                }],
                schema_config: None,
                limits: None,
            }],
            ..Default::default()
        };
        config
    }

    fn echo_request_builder() -> Request<Body> {
        Request::builder()
            .uri("/tempo/api/echo")
            .header("authorization", "Bearer sk-test-key")
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap()
    }

    async fn echo_request(app: &Router) -> StatusCode {
        app.clone()
            .oneshot(echo_request_builder())
            .await
            .unwrap()
            .status()
    }

    async fn echo_request_response(app: &Router) -> axum::response::Response {
        app.clone().oneshot(echo_request_builder()).await.unwrap()
    }

    #[tokio::test]
    async fn query_requests_are_rate_limited_per_tenant() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let state = RouterAppState::new(catalog, test_config(Some(2)));
        let app = create_router(state);

        assert_eq!(echo_request(&app).await, StatusCode::OK);
        assert_eq!(echo_request(&app).await, StatusCode::OK);
        assert_eq!(echo_request(&app).await, StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn query_rate_limit_answers_the_structured_429_envelope_with_headers() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let state = RouterAppState::new(catalog, test_config(Some(1)));
        let app = create_router(state);

        assert_eq!(echo_request(&app).await, StatusCode::OK);
        let response = echo_request_response(&app).await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
        let retry_after: u64 = response
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .expect("Retry-After header present and numeric");
        assert!(retry_after >= 1);
        assert_eq!(
            response
                .headers()
                .get("x-ratelimit-limit")
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );
        assert!(response.headers().get("x-ratelimit-burst").is_some());

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "error");
        assert_eq!(json["errorType"], "rate_limited");
        assert!(json["retryAfterMs"].as_u64().unwrap() >= 1_000);
    }

    #[tokio::test]
    async fn query_requests_unlimited_without_configured_limit() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let state = RouterAppState::new(catalog, test_config(None));
        let app = create_router(state);

        for _ in 0..50 {
            assert_eq!(echo_request(&app).await, StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn generous_default_burst_admits_an_interactive_fan_out() {
        // Regression guard for the "generous defaults" requirement: a
        // tenant limited to 100 req/s with the default burst (10s of
        // budget) admits a 40-request fan-out (an Explore page load or an
        // agent's multi-tool investigation) in one instant.
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let mut config = test_config(Some(100));
        config.auth.default_limits.burst_seconds =
            common::config::TenantLimits::default().burst_seconds;
        let app = create_router(RouterAppState::new(catalog, config));

        for _ in 0..40 {
            assert_eq!(echo_request(&app).await, StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn admin_key_request_to_github_installations_route_is_unauthorized_not_500() {
        // github-installations handlers take a TenantContextExtractor, which
        // 500s when no TenantContext is attached — so the admin-key bypass
        // must not cover this route (issue #1685 follow-up).
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let mut config = test_config(None);
        config.auth.admin_api_key = Some("admin-secret".to_string());
        let app = create_router(RouterAppState::new(catalog, config));

        let res = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/tenants/acme/github-installations")
                    .header("authorization", "Bearer admin-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn serves_frontend_runtime_config_from_self_monitoring() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let mut config = test_config(None);
        config.self_monitoring.frontend = common::config::FrontendMonitoringConfig {
            enabled: true,
            endpoint: "http://signaldb.example:4318".to_string(),
            api_key: Some("sk-ingest".to_string()),
            ..Default::default()
        };
        let app = create_router(RouterAppState::new(catalog, config));

        // Public route (no auth headers), served at root by the UI fallback.
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/runtime-config.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains("window.__SIGNALDB_RUNTIME_CONFIG__"));
        assert!(body.contains("http://signaldb.example:4318"));
        assert!(body.contains("sk-ingest"));
    }
}
