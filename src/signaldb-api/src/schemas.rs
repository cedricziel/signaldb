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
///
/// `scopes` is required and non-empty: a key's permissions are always
/// explicit. The vocabulary is `metrics:write`, `logs:write`, `traces:write`,
/// `profiles:write`, `traces:read`, `logs:read`, `metrics:read`,
/// `profiles:read`, `schema:read`, `schema:write`.
///
/// The legacy singular `dataset_id` field is not accepted here (removed in
/// the multi-dataset-key-restriction change): a request body carrying it is
/// rejected with a validation error rather than silently ignored, since
/// dropping it would create an unrestricted key when the caller asked for a
/// restricted one.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateApiKeyRequest {
    /// Optional human-readable name for the key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Scopes the key carries (required, at least one).
    pub scopes: Vec<String>,
    /// Dataset set the key is restricted to. Omitted or `null` creates an
    /// unrestricted key; a non-empty array restricts it to exactly that set.
    /// An explicit empty array, or a duplicate name within the set, is
    /// rejected.
    #[schema(min_items = 1)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_ids: Option<Vec<String>>,
}

/// Request body for updating a live API key's scopes and/or dataset restriction.
///
/// Absent fields are left untouched. Revoked keys cannot be updated. The
/// legacy singular `dataset_id` field is not accepted (see
/// [`CreateApiKeyRequest`]).
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateApiKeyRequest {
    /// New scope list (replaces the current one; must be non-empty).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scopes: Option<Vec<String>>,
    /// Replacement dataset set (non-empty; an explicit empty array is
    /// rejected). Omitted/`null` leaves the current restriction unchanged.
    /// Mutually exclusive with `clear_dataset_restriction: true`.
    #[schema(min_items = 1)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_ids: Option<Vec<String>>,
    /// Clear an existing dataset restriction back to unrestricted. Must not
    /// be combined with a non-empty `dataset_ids` in the same request.
    #[serde(default)]
    pub clear_dataset_restriction: bool,
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
    /// Scopes the key carries.
    pub scopes: Vec<String>,
    /// Dataset set the key is restricted to, if any; `null` is unrestricted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_ids: Option<Vec<String>>,
    /// Deprecated: the single dataset the key is restricted to, derived from
    /// `dataset_ids` as `Some` only when it names exactly one dataset (and
    /// `None` for both unrestricted and multi-dataset keys). Response-only —
    /// no request body accepts this field anymore. Prefer `dataset_ids`.
    #[deprecated(
        note = "derived from dataset_ids; None for multi-dataset restrictions. Use dataset_ids."
    )]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_id: Option<String>,
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
    /// Scopes the key carries; `null` for a legacy unrestricted key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scopes: Option<Vec<String>>,
    /// Dataset set the key is restricted to, if any; `null` is unrestricted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_ids: Option<Vec<String>>,
    /// Deprecated: see [`CreateApiKeyResponse::dataset_id`].
    #[deprecated(
        note = "derived from dataset_ids; None for multi-dataset restrictions. Use dataset_ids."
    )]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_id: Option<String>,
    /// ISO 8601 creation timestamp.
    pub created_at: String,
    /// ISO 8601 revocation timestamp (if revoked).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<String>,
}

/// Derive the deprecated single-dataset legacy field from a dataset-id set
/// (D8): `Some` for the single-dataset case a pre-this-change reader already
/// understood, `None` for both "unrestricted" and a genuinely new
/// multi-element restriction it had no way to represent.
pub fn derive_legacy_dataset_id(dataset_ids: Option<&[String]>) -> Option<String> {
    match dataset_ids {
        Some([single]) => Some(single.clone()),
        _ => None,
    }
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
