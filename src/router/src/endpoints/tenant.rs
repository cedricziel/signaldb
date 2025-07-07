use crate::RouterState;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, put},
};
use common::tenant_api::{CreateTenantRequest, TenantApi, TenantInfo, UpdateTenantRequest};
use serde_json::json;

/// Create tenant management routes
pub fn router<S: RouterState>() -> Router<S> {
    Router::new()
        .route("/tenants", get(list_tenants::<S>))
        .route("/tenants", post(create_tenant::<S>))
        .route("/tenants/:tenant_id", get(get_tenant::<S>))
        .route("/tenants/:tenant_id", put(update_tenant::<S>))
        .route("/tenants/:tenant_id/tables", get(list_tenant_tables::<S>))
        .route(
            "/tenants/:tenant_id/tables/create",
            post(create_tenant_tables::<S>),
        )
        .route("/tenants/:tenant_id/schemas", get(list_tenant_schemas::<S>))
        .route("/schemas/available", get(list_available_schemas))
}

/// GET /tenants
///
/// List all configured tenants
#[tracing::instrument]
pub async fn list_tenants<S: RouterState>(state: State<S>) -> impl IntoResponse {
    let api = TenantApi::new(state.config().clone());
    let response = api.list_tenants();
    Json(response)
}

/// POST /tenants
///
/// Create a new tenant
#[tracing::instrument]
pub async fn create_tenant<S: RouterState>(
    state: State<S>,
    Json(request): Json<CreateTenantRequest>,
) -> impl IntoResponse {
    let api = TenantApi::new(state.config().clone());

    // Validate the request
    if let Err(e) = api.validate_create_tenant_request(&request) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Validation failed",
                "message": e.to_string()
            })),
        );
    }

    // For now, just return the tenant info that would be created
    // TODO: Actually persist the tenant configuration
    let tenant_info = TenantInfo {
        tenant_id: request.tenant_id,
        schema: request.schema,
        custom_schemas: request.custom_schemas,
        enabled: request.enabled.unwrap_or(true),
    };

    (
        StatusCode::CREATED,
        Json(serde_json::to_value(tenant_info).unwrap()),
    )
}

/// GET /tenants/:tenant_id
///
/// Get information about a specific tenant
#[tracing::instrument]
pub async fn get_tenant<S: RouterState>(
    state: State<S>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
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

/// PUT /tenants/:tenant_id
///
/// Update an existing tenant
#[tracing::instrument]
pub async fn update_tenant<S: RouterState>(
    state: State<S>,
    Path(tenant_id): Path<String>,
    Json(request): Json<UpdateTenantRequest>,
) -> impl IntoResponse {
    let api = TenantApi::new(state.config().clone());

    // Validate the request
    if let Err(e) = api.validate_update_tenant_request(&tenant_id, &request) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Validation failed",
                "message": e.to_string()
            })),
        )
            .into_response();
    }

    // Get existing tenant and update it
    match api.get_tenant(&tenant_id) {
        Ok(mut tenant_info) => {
            // Update fields if provided
            if let Some(schema) = request.schema {
                tenant_info.schema = Some(schema);
            }
            if let Some(custom_schemas) = request.custom_schemas {
                tenant_info.custom_schemas = Some(custom_schemas);
            }
            if let Some(enabled) = request.enabled {
                tenant_info.enabled = enabled;
            }

            // TODO: Actually persist the updated tenant configuration
            (
                StatusCode::OK,
                Json(serde_json::to_value(tenant_info).unwrap()),
            )
                .into_response()
        }
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
#[tracing::instrument]
pub async fn list_tenant_tables<S: RouterState>(
    state: State<S>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    let mut api = TenantApi::new(state.config().clone());

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
#[tracing::instrument]
pub async fn create_tenant_tables<S: RouterState>(
    state: State<S>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    let mut api = TenantApi::new(state.config().clone());

    match api.create_default_tables(&tenant_id).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(json!({
                "message": format!("Default tables created for tenant '{}'", tenant_id),
                "tenant_id": tenant_id
            })),
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
#[tracing::instrument]
pub async fn list_tenant_schemas<S: RouterState>(
    state: State<S>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
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

/// GET /schemas/available
///
/// List all available table schema types
#[tracing::instrument]
pub async fn list_available_schemas() -> Json<serde_json::Value> {
    let schemas = TenantApi::get_available_table_schemas();
    Json(json!({
        "schemas": schemas
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryStateImpl;
    use common::catalog::Catalog;
    use common::config::{
        Configuration, DefaultSchemas, SchemaConfig, TenantSchemaConfig, TenantsConfig,
    };
    use common::tenant_api::{CreateTenantRequest, TenantApi, UpdateTenantRequest};
    use std::collections::HashMap;

    async fn create_test_state() -> InMemoryStateImpl {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        // Create configuration with test tenant
        let tenant_config = TenantSchemaConfig {
            schema: Some(SchemaConfig {
                catalog_type: "memory".to_string(),
                catalog_uri: "memory://test".to_string(),
                default_schemas: DefaultSchemas::default(),
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

        InMemoryStateImpl::new(catalog, config)
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

        // Test validation for create request
        let valid_request = CreateTenantRequest {
            tenant_id: "new-tenant".to_string(),
            schema: None,
            custom_schemas: None,
            enabled: Some(true),
        };
        assert!(api.validate_create_tenant_request(&valid_request).is_ok());

        // Test validation failure for empty tenant ID
        let invalid_request = CreateTenantRequest {
            tenant_id: "".to_string(),
            schema: None,
            custom_schemas: None,
            enabled: Some(true),
        };
        assert!(
            api.validate_create_tenant_request(&invalid_request)
                .is_err()
        );

        // Test validation for update request
        let update_request = UpdateTenantRequest {
            schema: Some(SchemaConfig {
                catalog_type: "sql".to_string(),
                catalog_uri: "sqlite:///test.db".to_string(),
                default_schemas: DefaultSchemas::default(),
            }),
            custom_schemas: None,
            enabled: Some(false),
        };
        assert!(
            api.validate_update_tenant_request("test-tenant", &update_request)
                .is_ok()
        );
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
}
