use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Request body for creating a new tenant
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateTenantRequest {
    /// Unique tenant identifier
    pub id: String,
    /// Human-readable tenant name
    pub name: String,
    /// Default dataset name (optional)
    pub default_dataset: Option<String>,
}

/// Request body for updating an existing tenant
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateTenantRequest {
    /// Updated tenant name
    pub name: Option<String>,
    /// Updated default dataset
    pub default_dataset: Option<String>,
}

/// Tenant information returned by the API
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TenantResponse {
    /// Unique tenant identifier
    pub id: String,
    /// Human-readable tenant name
    pub name: String,
    /// Default dataset name
    pub default_dataset: Option<String>,
    /// Source of the tenant record (config or database)
    pub source: String,
    /// ISO 8601 creation timestamp
    pub created_at: String,
    /// ISO 8601 last-updated timestamp
    pub updated_at: String,
}

/// Response containing a list of tenants
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListTenantsResponse {
    /// List of tenant records
    pub tenants: Vec<TenantResponse>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_tenant_request_serde_roundtrip() {
        let req = CreateTenantRequest {
            id: "acme".to_string(),
            name: "Acme Corp".to_string(),
            default_dataset: Some("production".to_string()),
        };
        let json = serde_json::to_string(&req).unwrap();
        let deserialized: CreateTenantRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "acme");
        assert_eq!(deserialized.name, "Acme Corp");
        assert_eq!(deserialized.default_dataset, Some("production".to_string()));
    }

    #[test]
    fn test_update_tenant_request_partial() {
        let json = r#"{"name": "Updated Name"}"#;
        let req: UpdateTenantRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, Some("Updated Name".to_string()));
        assert_eq!(req.default_dataset, None);
    }

    #[test]
    fn test_tenant_response_serde_roundtrip() {
        let resp = TenantResponse {
            id: "acme".to_string(),
            name: "Acme Corp".to_string(),
            default_dataset: Some("production".to_string()),
            source: "database".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            updated_at: "2024-01-01T00:00:00Z".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: TenantResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "acme");
        assert_eq!(deserialized.source, "database");
    }

    #[test]
    fn test_list_tenants_response_serde() {
        let resp = ListTenantsResponse {
            tenants: vec![TenantResponse {
                id: "acme".to_string(),
                name: "Acme Corp".to_string(),
                default_dataset: None,
                source: "config".to_string(),
                created_at: "2024-01-01T00:00:00Z".to_string(),
                updated_at: "2024-01-01T00:00:00Z".to_string(),
            }],
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: ListTenantsResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.tenants.len(), 1);
    }
}
