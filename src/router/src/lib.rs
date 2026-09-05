use axum::{
    Router,
    http::StatusCode,
    middleware,
    response::IntoResponse,
    routing::{delete, get, post, put},
};
use common::auth::{Authenticator, TenantContext, admin_auth_middleware, auth_middleware};
use common::catalog::Catalog;
use common::config::Configuration;
use common::ratelimit::TenantRateLimiter;
use common::schema_registry::SchemaResolver;
use std::sync::Arc;

pub mod cli;
pub mod discovery;
pub mod endpoints;
pub mod openapi;
pub mod read_scope;
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

/// The shared state that route handlers depend on.
///
/// This is the narrow interface every handler needs from the router's state —
/// the catalog, service registry, configuration, and authenticator. Handlers
/// are generic over this trait so they can be exercised against any state that
/// satisfies the contract; [`RouterAppState`] is the concrete implementation
/// used in production and tests.
pub trait RouterState: std::fmt::Debug + Clone + Send + Sync + 'static {
    fn catalog(&self) -> &Catalog;
    fn service_registry(&self) -> &discovery::ServiceRegistry;
    fn config(&self) -> &Configuration;
    fn authenticator(&self) -> &Arc<Authenticator>;
    /// The schema-registry resolver. The default builds a fresh (uncached)
    /// resolver over the catalog, which is fine for tests; the app state
    /// overrides it with a shared instance whose per-tenant index persists
    /// across requests.
    fn schema_resolver(&self) -> SchemaResolver {
        SchemaResolver::new(self.catalog().clone())
    }
}

/// Concrete [`RouterState`] holding the router's shared handles.
#[derive(Clone)]
pub struct RouterAppState {
    catalog: Catalog,
    service_registry: discovery::ServiceRegistry,
    config: Configuration,
    authenticator: Arc<Authenticator>,
    schema_resolver: SchemaResolver,
}

impl std::fmt::Debug for RouterAppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterAppState")
            .field("catalog", &"Catalog")
            .field("service_registry", &self.service_registry)
            .field("config", &"Configuration")
            .field("authenticator", &"Authenticator")
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

        Self {
            schema_resolver: SchemaResolver::new(catalog.clone()),
            catalog,
            service_registry,
            config,
            authenticator,
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

        Self {
            schema_resolver: SchemaResolver::new(catalog.clone()),
            catalog,
            service_registry,
            config,
            authenticator,
        }
    }
}

impl RouterState for RouterAppState {
    fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    fn service_registry(&self) -> &discovery::ServiceRegistry {
        &self.service_registry
    }

    fn config(&self) -> &Configuration {
        &self.config
    }

    fn authenticator(&self) -> &Arc<Authenticator> {
        &self.authenticator
    }

    fn schema_resolver(&self) -> SchemaResolver {
        self.schema_resolver.clone()
    }
}

/// Create a new router instance with all routes configured
pub fn create_router<S: RouterState>(state: S) -> Router {
    // Create auth middleware layer
    let authenticator = state.authenticator().clone();
    let auth_layer =
        middleware::from_fn(move |req, next| auth_middleware(authenticator.clone(), req, next));

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

    // Create admin auth middleware layer
    let admin_key_hash = state
        .config()
        .auth
        .admin_api_key
        .as_ref()
        .map(|key| Authenticator::hash_api_key(key));
    let admin_authenticator = state.authenticator().clone();
    // Operational control uses the same administrative authentication as the
    // admin API; clone the inputs before the admin layer moves them.
    let ops_key_hash = admin_key_hash.clone();
    let ops_authenticator = admin_authenticator.clone();
    let admin_auth_layer = middleware::from_fn(move |req, next| {
        admin_auth_middleware(
            admin_key_hash.clone(),
            admin_authenticator.clone(),
            req,
            next,
        )
    });
    let ops_auth_layer = middleware::from_fn(move |req, next| {
        admin_auth_middleware(ops_key_hash.clone(), ops_authenticator.clone(), req, next)
    });

    // Build admin routes
    let admin_router = Router::new()
        .route("/tenants", get(endpoints::admin::list_tenants::<S>))
        .route("/tenants", post(endpoints::admin::create_tenant::<S>))
        .route(
            "/tenants/{tenant_id}",
            get(endpoints::admin::get_tenant::<S>),
        )
        .route(
            "/tenants/{tenant_id}",
            put(endpoints::admin::update_tenant::<S>),
        )
        .route(
            "/tenants/{tenant_id}",
            delete(endpoints::admin::delete_tenant::<S>),
        )
        .route(
            "/tenants/{tenant_id}/api-keys",
            get(endpoints::admin::list_api_keys::<S>),
        )
        .route(
            "/tenants/{tenant_id}/api-keys",
            post(endpoints::admin::create_api_key::<S>),
        )
        .route(
            "/tenants/{tenant_id}/api-keys/{key_id}",
            delete(endpoints::admin::revoke_api_key::<S>)
                .patch(endpoints::admin::update_api_key::<S>),
        )
        .route(
            "/tenants/{tenant_id}/datasets",
            get(endpoints::admin::list_datasets::<S>),
        )
        .route(
            "/tenants/{tenant_id}/datasets",
            post(endpoints::admin::create_dataset::<S>),
        )
        .route(
            "/tenants/{tenant_id}/datasets/{dataset_id}",
            delete(endpoints::admin::delete_dataset::<S>),
        )
        .route("/users", post(endpoints::admin::create_user::<S>))
        .layer(admin_auth_layer);

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
        // OAuth 2.1 authorization-server endpoints (public: discovery + DCR are
        // unauthenticated by spec; empty unless mcp.oauth.enabled)
        .merge(oauth_routes)
        // Admin routes with admin authentication
        .nest("/api/v1/admin", admin_router)
        // Operational control (compaction), admin-authenticated, proxied to the
        // compactor's Flight do_action surface.
        .nest(
            "/api/v1/ops",
            endpoints::ops::router::<S>().layer(ops_auth_layer),
        )
        .nest(
            "/api/v1",
            endpoints::tenant::router()
                .nest("/manage", endpoints::management::router())
                .nest("/schema", endpoints::schema::router())
                .route("/whoami", get(endpoints::session::whoami::<S>))
                .route("/connection", get(endpoints::session::connection_info::<S>))
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
pub fn create_flight_service<S: RouterState>(
    state: S,
) -> endpoints::flight::SignalDBFlightService<S> {
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
