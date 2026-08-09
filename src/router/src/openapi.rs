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
    openapi::security::{Http, HttpAuthScheme, SecurityRequirement, SecurityScheme},
};

/// Registers the `bearerAuth` HTTP bearer security scheme and requires it
/// globally, so every operation (admin and management) is documented as
/// authenticated. Admin handlers also restate it per-path; management handlers
/// inherit this default.
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
        openapi.security = Some(vec![SecurityRequirement::new(
            "bearerAuth",
            Vec::<String>::new(),
        )]);
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "SignalDB API",
        version = "1.0.0",
        description = "SignalDB admin, tenant-management, and query HTTP API\n\nEvery response whose request was traced carries the server's W3C trace context back to the caller: `Server-Timing: traceparent;desc=\"00-<trace-id>-<span-id>-<flags>\"` (readable in browsers via the Performance API) and the equivalent `traceresponse` header, plus `Server-Timing` `dur` entries with server-side stage timings (always `total`, endpoint-specific stages where available) and `Timing-Allow-Origin: *` so cross-origin pages can read the timing entries. The headers are omitted when self-monitoring tracing is disabled."
    ),
    servers((url = "/")),
    modifiers(&SecurityAddon),
    tags(
        (name = "tenants", description = "Tenant lifecycle operations"),
        (name = "api-keys", description = "API key management"),
        (name = "datasets", description = "Dataset management"),
        (name = "users", description = "Human user management"),
        (name = "memberships", description = "Tenant membership management"),
        (name = "traces", description = "Tempo-compatible trace search and retrieval"),
        (name = "query", description = "Native structured Query IR"),
        (name = "metrics", description = "Prometheus-compatible metrics query (PromQL)"),
        (name = "logs", description = "Loki-compatible log query (LogQL)"),
        (name = "ops", description = "Operational control (compaction)"),
        (name = "oauth", description = "OAuth 2.1 connector consent flow"),
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
        crate::endpoints::admin::create_user,
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
        crate::endpoints::session::whoami,
        // Tempo-compatible trace query endpoints
        crate::endpoints::tempo::search,
        crate::endpoints::tempo::query_single_trace,
        crate::endpoints::tempo::search_tags,
        crate::endpoints::tempo::search_tag_values,
        // Native Query IR
        crate::endpoints::query::query_ir,
        // Prometheus-compatible metrics query endpoints (PromQL)
        crate::endpoints::promql::query,
        crate::endpoints::promql::query_range,
        crate::endpoints::promql::labels,
        crate::endpoints::promql::label_values,
        // Loki-compatible log query endpoints (LogQL)
        crate::endpoints::logql::query,
        crate::endpoints::logql::query_range,
        crate::endpoints::logql::labels,
        crate::endpoints::logql::label_values,
        // Operational control (compaction) endpoints
        crate::endpoints::ops::compact,
        crate::endpoints::ops::compact_status,
        crate::endpoints::ops::compact_dry_run,
        // OAuth 2.1 connector consent flow (change: mcp-oauth-dcr)
        crate::endpoints::oauth::consent_context,
        crate::endpoints::oauth::authorize_decision,
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
        signaldb_api::CreateUserRequest,
        signaldb_api::UserResponse,
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
        // authenticated identity
        crate::endpoints::session::WhoamiIdentityResponse,
        crate::endpoints::session::WhoamiTenant,
        // shared enums
        common::catalog::MembershipRole,
        // Tempo-compatible trace query DTOs
        tempo_api::SearchResult,
        tempo_api::Trace,
        tempo_api::SpanSet,
        tempo_api::Span,
        tempo_api::SpanEvent,
        tempo_api::Attribute,
        tempo_api::ProfileSummary,
        tempo_api::TagSearchResponse,
        tempo_api::TagValuesResponse,
        // Native Query IR request/response DTOs
        crate::endpoints::query::QueryIrRequest,
        crate::endpoints::query::QueryRange,
        crate::endpoints::query::QueryIrResponse,
        crate::endpoints::query::ResolvedWindow,
        crate::endpoints::query::ResultColumn,
        crate::endpoints::query::ResultSeries,
        // OAuth 2.1 connector consent DTOs
        crate::endpoints::oauth::ConsentDecision,
        crate::endpoints::oauth::ConsentDecisionResponse,
        crate::endpoints::oauth::ConsentContextResponse,
        crate::endpoints::oauth::ConsentTenant,
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
