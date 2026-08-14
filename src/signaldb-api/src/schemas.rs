//! Data-transfer types for the SignalDB admin HTTP API.
//!
//! These structs are the source of truth for the admin API's OpenAPI component
//! schemas: they derive [`utoipa::ToSchema`], and the `router` crate assembles
//! them into the emitted spec (`api/signaldb-api.json`). Field names and serde
//! attributes define the JSON wire format exactly — optional fields are omitted
//! from responses via `skip_serializing_if`, matching the documented schema.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Standard API error response.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiError {
    /// Error category.
    pub error: String,
    /// Human-readable error description.
    pub message: String,
}

impl ApiError {
    /// Create a new API error
    pub fn new(error: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            message: message.into(),
        }
    }
}

/// Request body for creating a new tenant.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateTenantRequest {
    /// Unique tenant identifier.
    pub id: String,
    /// Human-readable tenant name.
    pub name: String,
    /// Default dataset name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_dataset: Option<String>,
}

/// Request body for updating an existing tenant.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateTenantRequest {
    /// Updated tenant name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Updated default dataset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_dataset: Option<String>,
}

/// Tenant information returned by the API.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TenantResponse {
    /// Unique tenant identifier.
    pub id: String,
    /// Human-readable tenant name.
    pub name: String,
    /// Default dataset name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_dataset: Option<String>,
    /// Source of the tenant record (config or database).
    pub source: String,
    /// ISO 8601 creation timestamp.
    pub created_at: String,
    /// ISO 8601 last-updated timestamp.
    pub updated_at: String,
}

/// Response containing a list of tenants.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListTenantsResponse {
    /// List of tenant records.
    pub tenants: Vec<TenantResponse>,
}

/// Request body for creating a new API key.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateApiKeyRequest {
    /// Optional human-readable name for the key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Response returned when a new API key is created (includes the raw key).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateApiKeyResponse {
    /// Unique key identifier.
    pub id: String,
    /// The raw API key (only shown once at creation time).
    pub key: String,
    /// Optional human-readable name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// ISO 8601 creation timestamp.
    pub created_at: String,
}

/// API key information (without the raw key).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiKeyResponse {
    /// Unique key identifier.
    pub id: String,
    /// Optional human-readable name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// ISO 8601 creation timestamp.
    pub created_at: String,
    /// ISO 8601 revocation timestamp (if revoked).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<String>,
}

/// Response containing a list of API keys.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListApiKeysResponse {
    /// List of API key records (without raw keys).
    pub api_keys: Vec<ApiKeyResponse>,
}

/// Request body for creating a new dataset.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateDatasetRequest {
    /// Dataset name.
    pub name: String,
}

/// Dataset information returned by the API.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DatasetResponse {
    /// Unique dataset identifier.
    pub id: String,
    /// Dataset name.
    pub name: String,
    /// Tenant that owns this dataset.
    pub tenant_id: String,
    /// ISO 8601 creation timestamp.
    pub created_at: String,
}

/// Response containing a list of datasets.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListDatasetsResponse {
    /// List of dataset records.
    pub datasets: Vec<DatasetResponse>,
}

/// Request body for creating a human user with an initial tenant membership.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateUserRequest {
    /// Login email address.
    pub email: String,
    /// Optional display name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Password (hashed server-side; must be at least 12 characters).
    pub password: String,
    /// Grant instance-administrator status.
    #[serde(default)]
    pub instance_admin: bool,
    /// Tenant to grant the initial membership in.
    pub tenant: String,
    /// Initial tenant role: `admin`, `member`, or `viewer`.
    #[serde(default = "default_user_role")]
    pub role: String,
}

fn default_user_role() -> String {
    "admin".to_string()
}

/// Response returned when a user is created (never includes the password hash).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserResponse {
    /// Unique user identifier.
    pub id: String,
    /// Login email address.
    pub email: String,
    /// Optional display name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Whether the user is an instance administrator.
    pub instance_admin: bool,
    /// ISO 8601 creation timestamp.
    pub created_at: String,
}
