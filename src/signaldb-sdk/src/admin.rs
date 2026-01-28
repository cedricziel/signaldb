use crate::{SdkError, SignalDbClient};
use signaldb_api::{
    CreateApiKeyRequest, CreateApiKeyResponse, CreateDatasetRequest, CreateTenantRequest,
    DatasetResponse, ListApiKeysResponse, ListDatasetsResponse, ListTenantsResponse,
    TenantResponse, UpdateTenantRequest,
};

impl SignalDbClient {
    // ── Tenant operations ──────────────────────────────────────────

    /// List all tenants
    pub async fn list_tenants(&self) -> Result<ListTenantsResponse, SdkError> {
        self.get("/api/v1/admin/tenants").await
    }

    /// Create a new tenant
    pub async fn create_tenant(
        &self,
        req: CreateTenantRequest,
    ) -> Result<TenantResponse, SdkError> {
        self.post("/api/v1/admin/tenants", &req).await
    }

    /// Get a tenant by ID
    pub async fn get_tenant(&self, id: &str) -> Result<TenantResponse, SdkError> {
        self.get(&format!("/api/v1/admin/tenants/{id}")).await
    }

    /// Update a tenant
    pub async fn update_tenant(
        &self,
        id: &str,
        req: UpdateTenantRequest,
    ) -> Result<TenantResponse, SdkError> {
        self.put(&format!("/api/v1/admin/tenants/{id}"), &req).await
    }

    /// Delete a tenant
    pub async fn delete_tenant(&self, id: &str) -> Result<(), SdkError> {
        self.delete(&format!("/api/v1/admin/tenants/{id}")).await
    }

    // ── API key operations ─────────────────────────────────────────

    /// List API keys for a tenant
    pub async fn list_api_keys(&self, tenant_id: &str) -> Result<ListApiKeysResponse, SdkError> {
        self.get(&format!("/api/v1/admin/tenants/{tenant_id}/api-keys"))
            .await
    }

    /// Create a new API key for a tenant
    pub async fn create_api_key(
        &self,
        tenant_id: &str,
        req: CreateApiKeyRequest,
    ) -> Result<CreateApiKeyResponse, SdkError> {
        self.post(&format!("/api/v1/admin/tenants/{tenant_id}/api-keys"), &req)
            .await
    }

    /// Revoke an API key
    pub async fn revoke_api_key(&self, tenant_id: &str, key_id: &str) -> Result<(), SdkError> {
        self.delete(&format!(
            "/api/v1/admin/tenants/{tenant_id}/api-keys/{key_id}"
        ))
        .await
    }

    // ── Dataset operations ─────────────────────────────────────────

    /// List datasets for a tenant
    pub async fn list_datasets(&self, tenant_id: &str) -> Result<ListDatasetsResponse, SdkError> {
        self.get(&format!("/api/v1/admin/tenants/{tenant_id}/datasets"))
            .await
    }

    /// Create a new dataset for a tenant
    pub async fn create_dataset(
        &self,
        tenant_id: &str,
        req: CreateDatasetRequest,
    ) -> Result<DatasetResponse, SdkError> {
        self.post(&format!("/api/v1/admin/tenants/{tenant_id}/datasets"), &req)
            .await
    }

    /// Delete a dataset
    pub async fn delete_dataset(&self, tenant_id: &str, dataset_id: &str) -> Result<(), SdkError> {
        self.delete(&format!(
            "/api/v1/admin/tenants/{tenant_id}/datasets/{dataset_id}"
        ))
        .await
    }
}
