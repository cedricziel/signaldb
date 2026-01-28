use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Request body for creating a new dataset
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateDatasetRequest {
    /// Dataset name
    pub name: String,
}

/// Dataset information returned by the API
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DatasetResponse {
    /// Unique dataset identifier
    pub id: String,
    /// Dataset name
    pub name: String,
    /// Tenant that owns this dataset
    pub tenant_id: String,
    /// ISO 8601 creation timestamp
    pub created_at: String,
}

/// Response containing a list of datasets
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListDatasetsResponse {
    /// List of dataset records
    pub datasets: Vec<DatasetResponse>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_dataset_request_serde() {
        let req = CreateDatasetRequest {
            name: "production".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let deserialized: CreateDatasetRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, "production");
    }

    #[test]
    fn test_dataset_response_serde() {
        let resp = DatasetResponse {
            id: "ds-123".to_string(),
            name: "production".to_string(),
            tenant_id: "acme".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: DatasetResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "ds-123");
        assert_eq!(deserialized.tenant_id, "acme");
    }

    #[test]
    fn test_list_datasets_response_serde() {
        let resp = ListDatasetsResponse {
            datasets: vec![DatasetResponse {
                id: "ds-1".to_string(),
                name: "default".to_string(),
                tenant_id: "acme".to_string(),
                created_at: "2024-01-01T00:00:00Z".to_string(),
            }],
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: ListDatasetsResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.datasets.len(), 1);
    }
}
