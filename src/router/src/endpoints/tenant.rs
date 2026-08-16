use crate::RouterState;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use common::auth::TenantContextExtractor;
use common::tenant_api::{
    ListTablesResponse, ListTenantsResponse, TableInfo, TenantApi, TenantInfo,
};
use serde::Serialize;
use serde_json::json;
use utoipa::ToSchema;

/// Response body for `POST /tenants/{tenant_id}/tables/create`.
#[derive(Debug, Serialize, ToSchema)]
pub struct CreateTenantTablesResponse {
    /// Human-readable confirmation message.
    pub message: String,
    /// The tenant the tables were created for.
    pub tenant_id: String,
}

/// Response body for `GET /schemas/available`.
#[derive(Debug, Serialize, ToSchema)]
pub struct AvailableSchemasResponse {
    /// All table schema types SignalDB knows how to provision.
    pub schemas: Vec<TableInfo>,
}

/// Create tenant management routes
pub fn router<S: RouterState>() -> Router<S> {
    Router::new()
        .route("/tenants", get(list_tenants::<S>))
        .route("/tenants/{tenant_id}", get(get_tenant::<S>))
        .route("/tenants/{tenant_id}/tables", get(list_tenant_tables::<S>))
        .route(
            "/tenants/{tenant_id}/tables/create",
            post(create_tenant_tables::<S>),
        )
        .route(
            "/tenants/{tenant_id}/schemas",
            get(list_tenant_schemas::<S>),
        )
        .route("/schemas/available", get(list_available_schemas))
}

/// GET /tenants
///
/// List all configured tenants
#[utoipa::path(
    get,
    path = "/api/v1/tenants",
    tag = "tenants",
    operation_id = "list_tenants_self",
    security(("bearerAuth" = [])),
    responses(
        (status = 200, description = "The caller's own tenant, as a single-entry list", body = ListTenantsResponse),
    )
)]
#[tracing::instrument(skip_all)]
pub async fn list_tenants<S: RouterState>(
    state: State<S>,
    TenantContextExtractor(ctx): TenantContextExtractor,
) -> impl IntoResponse {
    let api = TenantApi::new(state.config().clone());
    let mut response = api.list_tenants();
    response
        .tenants
        .retain(|tenant| tenant.tenant_id == ctx.tenant_id);
    response.default_tenant = ctx.tenant_id;
    Json(response)
}

/// GET /tenants/:tenant_id
///
/// Get information about a specific tenant
#[utoipa::path(
    get,
    path = "/api/v1/tenants/{tenant_id}",
    tag = "tenants",
    operation_id = "get_tenant_self",
    security(("bearerAuth" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier (must match the authenticated tenant)")),
    responses(
        (status = 200, description = "Tenant information", body = TenantInfo),
        (status = 403, description = "Requested tenant does not match the authenticated tenant"),
        (status = 404, description = "Tenant not found"),
    )
)]
#[tracing::instrument(skip_all, fields(signaldb.tenant.id = %tenant_id))]
pub async fn get_tenant<S: RouterState>(
    state: State<S>,
    Path(tenant_id): Path<String>,
    TenantContextExtractor(ctx): TenantContextExtractor,
) -> impl IntoResponse {
    if tenant_id != ctx.tenant_id {
        return forbidden_tenant().into_response();
    }
    let api = TenantApi::new(state.config().clone());

    match api.get_tenant(&tenant_id) {
        Ok(tenant_info) => (
            StatusCode::OK,
            Json(serde_json::to_value(tenant_info).unwrap()),
        )
            .into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": "Tenant not found",
                "message": e.to_string()
            })),
        )
            .into_response(),
    }
}

/// GET /tenants/:tenant_id/tables
///
/// List all tables for a specific tenant
#[utoipa::path(
    get,
    path = "/api/v1/tenants/{tenant_id}/tables",
    tag = "tenants",
    operation_id = "list_tenant_tables",
    security(("bearerAuth" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier (must match the authenticated tenant)")),
    responses(
        (status = 200, description = "The tenant's provisioned signal tables", body = ListTablesResponse),
        (status = 403, description = "Requested tenant does not match the authenticated tenant"),
        (status = 500, description = "Failed to list tables"),
    )
)]
#[tracing::instrument(skip_all, fields(signaldb.tenant.id = %tenant_id))]
pub async fn list_tenant_tables<S: RouterState>(
    state: State<S>,
    Path(tenant_id): Path<String>,
    TenantContextExtractor(ctx): TenantContextExtractor,
) -> impl IntoResponse {
    if tenant_id != ctx.tenant_id {
        return forbidden_tenant().into_response();
    }
    // Attach the SQL catalog so a tenant created through the admin API — one
    // with no `[[auth.tenants]]` block — resolves here too, the same as
    // `create_tenant_tables`; otherwise a listing right after provisioning
    // such a tenant would see nothing.
    let mut api = TenantApi::new(state.config().clone())
        .with_tenant_source(std::sync::Arc::new(state.catalog().clone()));

    match api.list_tables(&tenant_id).await {
        Ok(response) => (
            StatusCode::OK,
            Json(serde_json::to_value(response).unwrap()),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "Failed to list tables",
                "message": e.to_string()
            })),
        )
            .into_response(),
    }
}

/// POST /tenants/:tenant_id/tables/create
///
/// Create default tables for a tenant
#[utoipa::path(
    post,
    path = "/api/v1/tenants/{tenant_id}/tables/create",
    tag = "tenants",
    operation_id = "create_tenant_tables",
    security(("bearerAuth" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier (must match the authenticated tenant)")),
    responses(
        (status = 201, description = "The tenant's enabled signal tables were created", body = CreateTenantTablesResponse),
        (status = 403, description = "Requested tenant mismatch, or tenant administrator privileges required"),
        (status = 500, description = "Failed to create tables"),
    )
)]
#[tracing::instrument(skip_all, fields(signaldb.tenant.id = %tenant_id))]
pub async fn create_tenant_tables<S: RouterState>(
    state: State<S>,
    Path(tenant_id): Path<String>,
    TenantContextExtractor(ctx): TenantContextExtractor,
) -> impl IntoResponse {
    if tenant_id != ctx.tenant_id {
        return forbidden_tenant().into_response();
    }
    if !ctx.can_manage_tenant() {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Tenant administrator privileges required"})),
        )
            .into_response();
    }
    // Attach the SQL catalog so a tenant created through the admin API — one
    // with no `[[auth.tenants]]` block — resolves here too.
    let mut api = TenantApi::new(state.config().clone())
        .with_tenant_source(std::sync::Arc::new(state.catalog().clone()));

    match api.create_default_tables(&tenant_id).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(CreateTenantTablesResponse {
                message: format!("Default tables created for tenant '{tenant_id}'"),
                tenant_id,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "Failed to create tables",
                "message": e.to_string()
            })),
        )
            .into_response(),
    }
}

/// GET /tenants/:tenant_id/schemas
///
/// List available table schemas for a tenant
#[utoipa::path(
    get,
    path = "/api/v1/tenants/{tenant_id}/schemas",
    tag = "tenants",
    operation_id = "list_tenant_schemas",
    security(("bearerAuth" = [])),
    params(("tenant_id" = String, Path, description = "Tenant identifier (must match the authenticated tenant)")),
    responses(
        (status = 200, description = "The tenant's configured table schemas", body = ListTablesResponse),
        (status = 403, description = "Requested tenant does not match the authenticated tenant"),
        (status = 500, description = "Failed to list schemas"),
    )
)]
#[tracing::instrument(skip_all, fields(signaldb.tenant.id = %tenant_id))]
pub async fn list_tenant_schemas<S: RouterState>(
    state: State<S>,
    Path(tenant_id): Path<String>,
    TenantContextExtractor(ctx): TenantContextExtractor,
) -> impl IntoResponse {
    if tenant_id != ctx.tenant_id {
        return forbidden_tenant().into_response();
    }
    let api = TenantApi::new(state.config().clone());

    match api.list_table_schemas(&tenant_id) {
        Ok(response) => (
            StatusCode::OK,
            Json(serde_json::to_value(response).unwrap()),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "Failed to list schemas",
                "message": e.to_string()
            })),
        )
            .into_response(),
    }
}

fn forbidden_tenant() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::FORBIDDEN,
        Json(json!({"error": "Requested tenant does not match authenticated tenant"})),
    )
}

/// GET /schemas/available
///
/// List all available table schema types
#[utoipa::path(
    get,
    path = "/api/v1/schemas/available",
    tag = "tenants",
    operation_id = "list_available_schemas",
    security(("bearerAuth" = [])),
    responses(
        (status = 200, description = "All table schema types SignalDB knows how to provision", body = AvailableSchemasResponse),
    )
)]
#[tracing::instrument(skip_all)]
pub async fn list_available_schemas() -> Json<AvailableSchemasResponse> {
    let schemas = TenantApi::get_available_table_schemas();
    Json(AvailableSchemasResponse { schemas })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RouterAppState, create_router};
    use axum::body::Body;
    use axum::http::{Request, header};
    use common::catalog::{Catalog, MembershipRole};
    use common::config::{
        ApiKeyConfig, AuthConfig, Configuration, DefaultSchemas, SchemaConfig, TenantConfig,
        TenantSchemaConfig, TenantsConfig,
    };
    use common::tenant_api::TenantApi;
    use std::collections::HashMap;
    use tower::ServiceExt;

    async fn create_test_state() -> RouterAppState {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        // Create configuration with test tenant
        let tenant_config = TenantSchemaConfig {
            schema: Some(SchemaConfig {
                catalog_type: "memory".to_string(),
                catalog_uri: "memory://test".to_string(),
                default_schemas: DefaultSchemas::default(),
                materialized_labels: Default::default(),
            }),
            ..TenantSchemaConfig::default()
        };

        let mut tenants = HashMap::new();
        tenants.insert("test-tenant".to_string(), tenant_config);

        let config = Configuration {
            tenants: TenantsConfig {
                default_tenant: "test-tenant".to_string(),
                tenants,
            },
            ..Configuration::default()
        };

        RouterAppState::new(catalog, config)
    }

    #[tokio::test]
    async fn test_tenant_api_integration() {
        let state = create_test_state().await;
        let api = TenantApi::new(state.config().clone());

        // Test listing tenants
        let tenants = api.list_tenants();
        assert_eq!(tenants.default_tenant, "test-tenant");
        assert_eq!(tenants.tenants.len(), 1);
        assert_eq!(tenants.tenants[0].tenant_id, "test-tenant");

        // Test getting existing tenant
        let tenant_info = api.get_tenant("test-tenant").unwrap();
        assert_eq!(tenant_info.tenant_id, "test-tenant");
        assert!(tenant_info.enabled);
        assert!(tenant_info.schema.is_some());

        // Test getting non-existent tenant
        assert!(api.get_tenant("unknown-tenant").is_err());
    }

    #[tokio::test]
    async fn test_list_available_schemas() {
        let schemas = TenantApi::get_available_table_schemas();

        // Should include at least the basic schema types
        let schema_names: Vec<String> = schemas.into_iter().map(|s| s.name).collect();
        assert!(schema_names.contains(&"traces".to_string()));
        assert!(schema_names.contains(&"logs".to_string()));
        assert!(schema_names.contains(&"metrics_gauge".to_string()));
        assert!(schema_names.contains(&"metrics_sum".to_string()));
        assert!(schema_names.contains(&"metrics_histogram".to_string()));
    }

    #[tokio::test]
    async fn test_tenant_schema_listing() {
        let state = create_test_state().await;
        let api = TenantApi::new(state.config().clone());

        // Test listing schemas for existing tenant
        let schemas_result = api.list_table_schemas("test-tenant");
        assert!(schemas_result.is_ok());

        // Test listing schemas for non-existent tenant
        let schemas_result = api.list_table_schemas("unknown-tenant");
        assert!(schemas_result.is_ok()); // Should still work but return default schemas
    }

    #[tokio::test]
    async fn test_tenant_configuration_access() {
        let state = create_test_state().await;

        // Test that the state provides access to configuration
        let config = state.config();
        assert_eq!(config.get_default_tenant(), "test-tenant");
        assert!(config.is_tenant_enabled("test-tenant"));
        assert!(!config.is_tenant_enabled("unknown-tenant"));

        // Test tenant schema config access
        let tenant_schema = config.get_tenant_schema_config("test-tenant");
        assert_eq!(tenant_schema.catalog_type, "memory");
        assert_eq!(tenant_schema.catalog_uri, "memory://test");
    }

    /// GET /tenants/:tenant_id rejects a path tenant_id that doesn't match
    /// the authenticated tenant, regardless of whether that tenant exists.
    /// This is the handler-level guard (`forbidden_tenant`), not something
    /// `TenantApi` itself enforces.
    #[tokio::test]
    async fn get_tenant_with_mismatched_path_tenant_is_forbidden() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let config = Configuration {
            auth: AuthConfig {
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
            },
            ..Configuration::default()
        };
        let app = create_router(RouterAppState::new(catalog, config));

        let request = Request::builder()
            .uri("/api/v1/tenants/other-tenant")
            .header("authorization", "Bearer sk-test-key")
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json["error"],
            "Requested tenant does not match authenticated tenant"
        );
    }

    /// POST /tenants/:tenant_id/tables/create must actually create the
    /// tenant's signal tables. It used to return 201 having created nothing
    /// (`SchemaRegistry::create_default_tables_for_tenant` only logged
    /// "Would create table ...").
    #[tokio::test]
    async fn create_tenant_tables_actually_creates_them() {
        // A file-backed catalog: a named in-memory database lives only
        // while a connection is open, and the code under test builds and
        // drops its own pool. Holding a second manager does not help —
        // sqlx pools are lazy and reap idle connections.
        let temp_catalog = common::testing::TempCatalog::new();
        let catalog_uri = temp_catalog.uri().to_string();
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let mut config = Configuration {
            auth: AuthConfig {
                tenants: vec![TenantConfig {
                    id: "acme".to_string(),
                    slug: "acme".to_string(),
                    name: "Acme".to_string(),
                    default_dataset: Some("production".to_string()),
                    datasets: vec![],
                    api_keys: vec![ApiKeyConfig {
                        key: "sk-test-key".to_string(),
                        name: Some("test".to_string()),
                    }],
                    schema_config: None,
                    limits: None,
                }],
                ..Default::default()
            },
            ..Configuration::default()
        };
        config.schema.catalog_uri = catalog_uri.clone();

        let manager = common::CatalogManager::new(config.clone()).await.unwrap();

        let app = create_router(RouterAppState::new(catalog, config.clone()));
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/tenants/acme/tables/create")
            .header("authorization", "Bearer sk-test-key")
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();

        // The response contract is unchanged: 201 with the same message.
        assert_eq!(response.status(), StatusCode::CREATED);

        // ...and the tables are really there.
        let namespace = manager.build_namespace("acme", "production").unwrap();
        let mut tables: Vec<String> = manager
            .catalog()
            .list_tabulars(&namespace)
            .await
            .unwrap()
            .iter()
            .map(|identifier| identifier.name().to_string())
            .collect();
        tables.sort();
        assert_eq!(
            tables,
            vec![
                "logs",
                "metrics_exponential_histogram",
                "metrics_gauge",
                "metrics_histogram",
                "metrics_sum",
                "metrics_summary",
                "profiles",
                "traces",
            ]
        );
    }

    /// GET /tenants/:tenant_id/tables reads from the same place
    /// POST …/tables/create writes to: listing right after provisioning
    /// shows exactly what was created, grouped by dataset.
    #[tokio::test]
    async fn list_tenant_tables_reflects_provisioning_grouped_by_dataset() {
        let temp_catalog = common::testing::TempCatalog::new();
        let catalog_uri = temp_catalog.uri().to_string();
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let mut config = Configuration {
            auth: AuthConfig {
                tenants: vec![TenantConfig {
                    id: "acme".to_string(),
                    slug: "acme".to_string(),
                    name: "Acme".to_string(),
                    default_dataset: Some("production".to_string()),
                    datasets: vec![],
                    api_keys: vec![ApiKeyConfig {
                        key: "sk-test-key".to_string(),
                        name: Some("test".to_string()),
                    }],
                    schema_config: None,
                    limits: None,
                }],
                ..Default::default()
            },
            ..Configuration::default()
        };
        config.schema.catalog_uri = catalog_uri;
        let app = create_router(RouterAppState::new(catalog, config.clone()));

        // Nothing provisioned yet: the flat list is empty, not an error, but
        // the tenant's known dataset ("production", its implicit default)
        // still appears in the grouping with an empty `tables` array — a
        // caller should be able to see the dataset exists before anything
        // is provisioned in it.
        let request = Request::builder()
            .method("GET")
            .uri("/api/v1/tenants/acme/tables")
            .header("authorization", "Bearer sk-test-key")
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let listed: common::tenant_api::ListTablesResponse = serde_json::from_slice(&body).unwrap();
        assert!(listed.tables.is_empty());
        assert_eq!(listed.datasets.len(), 1, "{:?}", listed.datasets);
        assert_eq!(listed.datasets[0].dataset, "production");
        assert!(listed.datasets[0].tables.is_empty());

        // Provision, then list again.
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/tenants/acme/tables/create")
            .header("authorization", "Bearer sk-test-key")
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let request = Request::builder()
            .method("GET")
            .uri("/api/v1/tenants/acme/tables")
            .header("authorization", "Bearer sk-test-key")
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let listed: common::tenant_api::ListTablesResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(listed.tables.len(), 8, "{:?}", listed.tables);
        assert!(listed.tables.iter().all(|t| t.dataset == "production"));
        assert!(listed.tables.iter().any(|t| t.name == "traces"));
        let profiles = listed
            .tables
            .iter()
            .find(|t| t.name == "profiles")
            .expect("profiles table listed");
        assert_eq!(profiles.schema_type, "profiles");

        assert_eq!(listed.datasets.len(), 1, "{:?}", listed.datasets);
        assert_eq!(listed.datasets[0].dataset, "production");
        assert_eq!(listed.datasets[0].tables.len(), 8);
    }

    /// The endpoint must work for a tenant that exists ONLY in the database —
    /// created through the admin API, with no `[[auth.tenants]]` block. This
    /// is what the `with_tenant_source` wiring on the handler is for; the
    /// config-tenant case above passes without it.
    #[tokio::test]
    async fn create_tenant_tables_for_a_database_only_tenant() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let mut config = Configuration::default();
        // A file-backed catalog: a named in-memory database lives only while a
        // connection to it is open, and the handler builds and drops its own
        // pool. Holding a second manager open does not help — sqlx pools are
        // lazy and reap idle connections.
        let temp_catalog = common::testing::TempCatalog::new();
        config.schema.catalog_uri = temp_catalog.uri().to_string();
        // Deliberately empty: the tenant is registered only in the catalog.
        config.auth.tenants = vec![];

        let manager = common::CatalogManager::new(config.clone()).await.unwrap();

        catalog
            .upsert_tenant("dbonly", "DB Only", Some("production"), "database")
            .await
            .unwrap();
        // The authenticator rejects a tenant whose resolved dataset has no
        // dataset row, so an HTTP-reachable database tenant always has one.
        // (The reconciler covers the tenant-row-only case, which no
        // authenticated request can reach — see `TableReconciler`.)
        catalog
            .create_dataset("dbonly", "production")
            .await
            .unwrap();
        catalog
            .upsert_api_key(
                "dbonly",
                &common::auth::Authenticator::hash_api_key("sk-db-key"),
                Some("test"),
            )
            .await
            .unwrap();

        let app = create_router(RouterAppState::new(catalog, config.clone()));
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/tenants/dbonly/tables/create")
            .header("authorization", "Bearer sk-db-key")
            .header("x-tenant-id", "dbonly")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            status,
            StatusCode::CREATED,
            "body: {}",
            String::from_utf8_lossy(&body)
        );

        let namespace = manager.build_namespace("dbonly", "production").unwrap();
        assert_eq!(
            manager
                .catalog()
                .list_tabulars(&namespace)
                .await
                .unwrap()
                .len(),
            8,
            "a database-only tenant must be provisioned too"
        );

        // `GET .../tables` must see it too — proving the handler's
        // `with_tenant_source` wiring, not just `create_tenant_tables`'s.
        let request = Request::builder()
            .method("GET")
            .uri("/api/v1/tenants/dbonly/tables")
            .header("authorization", "Bearer sk-db-key")
            .header("x-tenant-id", "dbonly")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let listed: common::tenant_api::ListTablesResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(listed.tables.len(), 8, "{:?}", listed.tables);
        assert!(listed.tables.iter().all(|t| t.dataset == "production"));
        assert_eq!(listed.datasets.len(), 1, "{:?}", listed.datasets);
        assert_eq!(listed.datasets[0].dataset, "production");
        assert_eq!(listed.datasets[0].tables.len(), 8);
    }

    /// POST /tenants/:tenant_id/tables/create requires the authenticated
    /// principal to be able to manage the tenant. A signed-in user with a
    /// non-admin membership role must be rejected before `TenantApi` is
    /// ever invoked.
    #[tokio::test]
    async fn create_tenant_tables_without_admin_role_is_forbidden() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let config = Configuration {
            auth: AuthConfig {
                tenants: vec![TenantConfig {
                    id: "acme".to_string(),
                    slug: "acme".to_string(),
                    name: "Acme".to_string(),
                    default_dataset: Some("default".to_string()),
                    datasets: vec![],
                    api_keys: vec![],
                    schema_config: None,
                    limits: None,
                }],
                ..Default::default()
            },
            ..Configuration::default()
        };
        catalog.sync_config_tenants(&config.auth).await.unwrap();

        let password_hash = common::auth::hash_password("member password").unwrap();
        let member = catalog
            .create_user("member@example.com", Some("Member"), &password_hash, false)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&member.id, "acme", MembershipRole::Member)
            .await
            .unwrap();

        let app = create_router(RouterAppState::new(catalog, config));

        let login_request = Request::builder()
            .method("POST")
            .uri("/ui/session")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "email": "member@example.com",
                    "password": "member password",
                    "tenant": "acme"
                })
                .to_string(),
            ))
            .unwrap();
        let login_response = app.clone().oneshot(login_request).await.unwrap();
        assert_eq!(login_response.status(), StatusCode::OK);
        let cookie = login_response
            .headers()
            .get(header::SET_COOKIE)
            .expect("Set-Cookie present")
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .expect("cookie name=value")
            .to_string();

        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/tenants/acme/tables/create")
            .header(header::COOKIE, &cookie)
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "Tenant administrator privileges required");
    }
}
