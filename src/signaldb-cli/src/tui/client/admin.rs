//! HTTP admin API client for TUI

use signaldb_sdk::Client;
use thiserror::Error;

/// Admin API client error types
#[derive(Error, Debug)]
pub enum AdminClientError {
    #[error("Unauthorized: invalid or missing admin API key")]
    Unauthorized,

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("API error: {0}")]
    ApiError(String),
}

/// HTTP admin API client wrapper
pub struct AdminClient {
    client: Client,
}

impl AdminClient {
    /// Create a new admin client
    ///
    /// # Arguments
    /// * `base_url` - Base URL of the SignalDB router (e.g., "http://localhost:3000")
    /// * `admin_key` - Admin API key for authentication
    ///
    /// # Returns
    /// Result with AdminClient or error
    pub fn new(base_url: &str, admin_key: &str) -> Result<Self, AdminClientError> {
        let base_url = format!("{}/api/v1/admin", base_url.trim_end_matches('/'));

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {admin_key}"))
                .map_err(|e| AdminClientError::ConnectionError(e.to_string()))?,
        );

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(|e| AdminClientError::ConnectionError(e.to_string()))?;

        let client = Client::new_with_client(&base_url, http);

        Ok(Self { client })
    }

    /// Probe admin access to verify credentials
    ///
    /// # Arguments
    /// * `base_url` - Base URL of the SignalDB router
    /// * `key` - Admin API key to test
    ///
    /// # Returns
    /// Ok(true) if access is granted, Ok(false) if unauthorized, Err if connection fails
    pub async fn probe_admin_access(base_url: &str, key: &str) -> Result<bool, AdminClientError> {
        let client = Self::new(base_url, key)?;
        match client.list_tenants().await {
            Ok(_) => Ok(true),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("401") || err_str.contains("403") {
                    Ok(false)
                } else {
                    Err(AdminClientError::ConnectionError(err_str))
                }
            }
        }
    }

    /// List all tenants
    pub async fn list_tenants(&self) -> Result<Vec<serde_json::Value>, AdminClientError> {
        let response = self
            .client
            .list_tenants()
            .send()
            .await
            .map_err(|e| self.map_error(&e))?;

        let body = response.into_inner();
        Ok(serde_json::to_value(&body.tenants)
            .ok()
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default())
    }

    /// Create a new tenant
    pub async fn create_tenant(
        &self,
        id: &str,
        name: &str,
    ) -> Result<serde_json::Value, AdminClientError> {
        let request = signaldb_sdk::types::CreateTenantRequest {
            id: id.to_string(),
            name: name.to_string(),
            default_dataset: None,
        };

        let response = self
            .client
            .create_tenant()
            .body(request)
            .send()
            .await
            .map_err(|e| self.map_error(&e))?;

        serde_json::to_value(response.into_inner())
            .map_err(|e| AdminClientError::ApiError(e.to_string()))
    }

    /// Get a tenant by ID
    pub async fn get_tenant(&self, id: &str) -> Result<serde_json::Value, AdminClientError> {
        let response = self
            .client
            .get_tenant()
            .tenant_id(id)
            .send()
            .await
            .map_err(|e| self.map_error(&e))?;

        serde_json::to_value(response.into_inner())
            .map_err(|e| AdminClientError::ApiError(e.to_string()))
    }

    /// Update a tenant
    pub async fn update_tenant(
        &self,
        id: &str,
        name: &str,
    ) -> Result<serde_json::Value, AdminClientError> {
        let request = signaldb_sdk::types::UpdateTenantRequest {
            name: Some(name.to_string()),
            default_dataset: None,
        };

        let response = self
            .client
            .update_tenant()
            .tenant_id(id)
            .body(request)
            .send()
            .await
            .map_err(|e| self.map_error(&e))?;

        serde_json::to_value(response.into_inner())
            .map_err(|e| AdminClientError::ApiError(e.to_string()))
    }

    /// Delete a tenant
    pub async fn delete_tenant(&self, id: &str) -> Result<(), AdminClientError> {
        self.client
            .delete_tenant()
            .tenant_id(id)
            .send()
            .await
            .map_err(|e| self.map_error(&e))?;

        Ok(())
    }

    /// List API keys for a tenant
    pub async fn list_api_keys(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<serde_json::Value>, AdminClientError> {
        let response = self
            .client
            .list_api_keys()
            .tenant_id(tenant_id)
            .send()
            .await
            .map_err(|e| self.map_error(&e))?;

        let body = response.into_inner();
        Ok(serde_json::to_value(&body.api_keys)
            .ok()
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default())
    }

    /// Create a new API key for a tenant
    pub async fn create_api_key(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<serde_json::Value, AdminClientError> {
        let request = signaldb_sdk::types::CreateApiKeyRequest {
            name: Some(name.to_string()),
        };

        let response = self
            .client
            .create_api_key()
            .tenant_id(tenant_id)
            .body(request)
            .send()
            .await
            .map_err(|e| self.map_error(&e))?;

        serde_json::to_value(response.into_inner())
            .map_err(|e| AdminClientError::ApiError(e.to_string()))
    }

    /// Revoke an API key
    pub async fn revoke_api_key(
        &self,
        tenant_id: &str,
        key_id: &str,
    ) -> Result<(), AdminClientError> {
        self.client
            .revoke_api_key()
            .tenant_id(tenant_id)
            .key_id(key_id)
            .send()
            .await
            .map_err(|e| self.map_error(&e))?;

        Ok(())
    }

    /// List datasets for a tenant
    pub async fn list_datasets(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<serde_json::Value>, AdminClientError> {
        let response = self
            .client
            .list_datasets()
            .tenant_id(tenant_id)
            .send()
            .await
            .map_err(|e| self.map_error(&e))?;

        let body = response.into_inner();
        Ok(serde_json::to_value(&body.datasets)
            .ok()
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default())
    }

    /// Create a new dataset for a tenant
    pub async fn create_dataset(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<serde_json::Value, AdminClientError> {
        let request = signaldb_sdk::types::CreateDatasetRequest {
            name: id.to_string(),
        };

        let response = self
            .client
            .create_dataset()
            .tenant_id(tenant_id)
            .body(request)
            .send()
            .await
            .map_err(|e| self.map_error(&e))?;

        serde_json::to_value(response.into_inner())
            .map_err(|e| AdminClientError::ApiError(e.to_string()))
    }

    /// Delete a dataset
    pub async fn delete_dataset(
        &self,
        tenant_id: &str,
        dataset_id: &str,
    ) -> Result<(), AdminClientError> {
        self.client
            .delete_dataset()
            .tenant_id(tenant_id)
            .dataset_id(dataset_id)
            .send()
            .await
            .map_err(|e| self.map_error(&e))?;

        Ok(())
    }

    fn map_error<T: std::fmt::Debug>(&self, error: &signaldb_sdk::Error<T>) -> AdminClientError {
        let err_str = format!("{:?}", error);

        if err_str.contains("401") || err_str.contains("403") {
            AdminClientError::Unauthorized
        } else if err_str.contains("404") {
            AdminClientError::NotFound(err_str)
        } else if err_str.contains("409") {
            AdminClientError::Conflict(err_str)
        } else if err_str.contains("connection") || err_str.contains("timeout") {
            AdminClientError::ConnectionError(err_str)
        } else {
            AdminClientError::ApiError(err_str)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_admin_client_error_display() {
        let err = AdminClientError::Unauthorized;
        assert_eq!(
            err.to_string(),
            "Unauthorized: invalid or missing admin API key"
        );

        let err = AdminClientError::NotFound("tenant not found".to_string());
        assert_eq!(err.to_string(), "Not found: tenant not found");

        let err = AdminClientError::Conflict("already exists".to_string());
        assert_eq!(err.to_string(), "Conflict: already exists");

        let err = AdminClientError::ConnectionError("timeout".to_string());
        assert_eq!(err.to_string(), "Connection error: timeout");

        let err = AdminClientError::ApiError("internal error".to_string());
        assert_eq!(err.to_string(), "API error: internal error");
    }
}
