//! Code-first OpenAPI document for the SignalDB HTTP API.
//!
//! The document is assembled from the `#[utoipa::path]` annotations on the
//! admin (`/api/v1/admin/...`) and management (`/api/v1/manage/...`) handlers
//! plus the `ToSchema`-deriving DTOs in [`signaldb_api`], this crate's
//! management module, and [`common::catalog::MembershipRole`]. The generated
//! spec is checked into `api/signaldb-api.json` and kept current by the golden
//! test in this module.

use utoipa::{
    Modify, OpenApi,
    openapi::security::{Http, HttpAuthScheme, SecurityScheme},
};

/// Registers the `bearerAuth` HTTP bearer security scheme in the components.
struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi
            .components
            .get_or_insert_with(utoipa::openapi::Components::default);
        components.add_security_scheme(
            "bearerAuth",
            SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "SignalDB API",
        version = "1.0.0",
        description = "SignalDB admin and tenant-management HTTP API"
    ),
    servers((url = "/")),
    modifiers(&SecurityAddon),
    tags(
        (name = "tenants", description = "Tenant lifecycle operations"),
        (name = "api-keys", description = "API key management"),
        (name = "datasets", description = "Dataset management"),
        (name = "memberships", description = "Tenant membership management"),
    ),
    paths(
        crate::endpoints::admin::list_tenants,
        crate::endpoints::admin::create_tenant,
        crate::endpoints::admin::get_tenant,
        crate::endpoints::admin::update_tenant,
        crate::endpoints::admin::delete_tenant,
        crate::endpoints::admin::list_api_keys,
        crate::endpoints::admin::create_api_key,
        crate::endpoints::admin::revoke_api_key,
        crate::endpoints::admin::list_datasets,
        crate::endpoints::admin::create_dataset,
        crate::endpoints::admin::delete_dataset,
        crate::endpoints::management::create_tenant,
        crate::endpoints::management::list_datasets,
        crate::endpoints::management::create_dataset,
        crate::endpoints::management::delete_dataset,
        crate::endpoints::management::list_api_keys,
        crate::endpoints::management::create_api_key,
        crate::endpoints::management::revoke_api_key,
        crate::endpoints::management::list_memberships,
        crate::endpoints::management::upsert_membership,
        crate::endpoints::management::remove_membership,
    ),
    components(schemas(
        // signaldb-api admin DTOs
        signaldb_api::ApiError,
        signaldb_api::CreateTenantRequest,
        signaldb_api::UpdateTenantRequest,
        signaldb_api::TenantResponse,
        signaldb_api::ListTenantsResponse,
        signaldb_api::CreateApiKeyRequest,
        signaldb_api::CreateApiKeyResponse,
        signaldb_api::ApiKeyResponse,
        signaldb_api::ListApiKeysResponse,
        signaldb_api::CreateDatasetRequest,
        signaldb_api::DatasetResponse,
        signaldb_api::ListDatasetsResponse,
        // management (session-authenticated) DTOs
        crate::endpoints::management::CreateTenantRequest,
        crate::endpoints::management::ManageCreatedTenant,
        crate::endpoints::management::ManageError,
        crate::endpoints::management::DatasetResponse,
        crate::endpoints::management::CreateDatasetRequest,
        crate::endpoints::management::CreateApiKeyRequest,
        crate::endpoints::management::ApiKeyResponse,
        crate::endpoints::management::ManageCreatedApiKey,
        crate::endpoints::management::MembershipResponse,
        crate::endpoints::management::UpsertMembershipRequest,
        // shared enums
        common::catalog::MembershipRole,
    )),
)]
struct ApiDoc;

/// Returns the assembled OpenAPI document for the SignalDB HTTP API.
pub fn openapi_document() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openapi_spec_is_up_to_date() {
        let generated = openapi_document().to_pretty_json().unwrap() + "\n";
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../api/signaldb-api.json");
        if std::env::var("UPDATE_OPENAPI").is_ok() {
            std::fs::write(path, &generated).unwrap();
        } else {
            let on_disk = std::fs::read_to_string(path).expect(
                "api/signaldb-api.json missing — run UPDATE_OPENAPI=1 cargo test -p router openapi_spec_is_up_to_date",
            );
            assert_eq!(
                on_disk, generated,
                "api/signaldb-api.json is stale — run UPDATE_OPENAPI=1 cargo test -p router openapi_spec_is_up_to_date"
            );
        }
    }
}
