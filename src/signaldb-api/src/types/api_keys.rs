use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Request body for creating a new API key
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateApiKeyRequest {
    /// Optional human-readable name for the key
    pub name: Option<String>,
}

/// Response returned when a new API key is created (includes the raw key)
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateApiKeyResponse {
    /// Unique key identifier
    pub id: String,
    /// The raw API key (only shown once at creation time)
    pub key: String,
    /// Optional human-readable name
    pub name: Option<String>,
    /// ISO 8601 creation timestamp
    pub created_at: String,
}

/// API key information (without the raw key)
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiKeyResponse {
    /// Unique key identifier
    pub id: String,
    /// Optional human-readable name
    pub name: Option<String>,
    /// ISO 8601 creation timestamp
    pub created_at: String,
    /// ISO 8601 revocation timestamp (if revoked)
    pub revoked_at: Option<String>,
}

/// Response containing a list of API keys
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListApiKeysResponse {
    /// List of API key records (without raw keys)
    pub api_keys: Vec<ApiKeyResponse>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_api_key_request_serde() {
        let req = CreateApiKeyRequest {
            name: Some("Production Key".to_string()),
        };
        let json = serde_json::to_string(&req).unwrap();
        let deserialized: CreateApiKeyRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, Some("Production Key".to_string()));
    }

    #[test]
    fn test_create_api_key_request_empty_name() {
        let json = r#"{"name": null}"#;
        let req: CreateApiKeyRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, None);
    }

    #[test]
    fn test_create_api_key_response_serde() {
        let resp = CreateApiKeyResponse {
            id: "key-123".to_string(),
            key: "sk-secret-key".to_string(),
            name: Some("Test Key".to_string()),
            created_at: "2024-01-01T00:00:00Z".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: CreateApiKeyResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "key-123");
        assert_eq!(deserialized.key, "sk-secret-key");
    }

    #[test]
    fn test_api_key_response_with_revocation() {
        let resp = ApiKeyResponse {
            id: "key-123".to_string(),
            name: None,
            created_at: "2024-01-01T00:00:00Z".to_string(),
            revoked_at: Some("2024-06-01T00:00:00Z".to_string()),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: ApiKeyResponse = serde_json::from_str(&json).unwrap();
        assert!(deserialized.revoked_at.is_some());
    }

    #[test]
    fn test_list_api_keys_response_serde() {
        let resp = ListApiKeysResponse {
            api_keys: vec![ApiKeyResponse {
                id: "key-1".to_string(),
                name: Some("Key One".to_string()),
                created_at: "2024-01-01T00:00:00Z".to_string(),
                revoked_at: None,
            }],
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: ListApiKeysResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.api_keys.len(), 1);
    }
}
