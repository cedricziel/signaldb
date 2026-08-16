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
        (name = "schema", description = "Read-only logical and physical schema introspection"),
        (name = "traces", description = "Tempo-compatible trace search and retrieval"),
        (name = "query", description = "Native structured Query IR"),
        (name = "metrics", description = "Prometheus-compatible metrics query (PromQL)"),
        (name = "logs", description = "Loki-compatible log query (LogQL)"),
        (name = "ops", description = "Operational control (compaction)"),
        (name = "oauth", description = "OAuth 2.1 connector consent flow"),
        (name = "schema", description = "Schema registry: semantic-convention registries, attribute/entity/metric resolution"),
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
        crate::endpoints::admin::update_api_key,
        crate::endpoints::admin::list_datasets,
        crate::endpoints::admin::create_dataset,
        crate::endpoints::admin::delete_dataset,
        crate::endpoints::admin::create_user,
        // Tenant self-service endpoints (the caller's own tenant, via API key)
        crate::endpoints::tenant::list_tenants,
        crate::endpoints::tenant::get_tenant,
        crate::endpoints::tenant::list_tenant_tables,
        crate::endpoints::tenant::create_tenant_tables,
        crate::endpoints::tenant::list_tenant_schemas,
        crate::endpoints::tenant::list_available_schemas,
        crate::endpoints::management::create_tenant,
        crate::endpoints::management::list_datasets,
        crate::endpoints::management::create_dataset,
        crate::endpoints::management::delete_dataset,
        crate::endpoints::management::list_api_keys,
        crate::endpoints::management::create_api_key,
        crate::endpoints::management::revoke_api_key,
        crate::endpoints::management::update_api_key,
        crate::endpoints::management::list_memberships,
        crate::endpoints::management::upsert_membership,
        crate::endpoints::management::remove_membership,
        crate::endpoints::management::get_schema,
        crate::endpoints::session::whoami,
        // Tempo-compatible trace query endpoints
        crate::endpoints::tempo::search,
        crate::endpoints::tempo::query_single_trace,
        crate::endpoints::tempo::search_tags,
        crate::endpoints::tempo::search_tag_values,
        crate::endpoints::tempo::search_tags_v2,
        crate::endpoints::tempo::search_tag_values_v2,
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
        // Schema registry (change: schema-registry)
        crate::endpoints::schema::list_registries,
        crate::endpoints::schema::create_registry,
        crate::endpoints::schema::validate_registry,
        crate::endpoints::schema::get_registry,
        crate::endpoints::schema::replace_registry,
        crate::endpoints::schema::delete_registry,
        crate::endpoints::schema::search_attributes,
        crate::endpoints::schema::resolve_attribute,
        crate::endpoints::schema::search_entities,
        crate::endpoints::schema::resolve_entity,
        crate::endpoints::schema::search_metrics,
        crate::endpoints::schema::resolve_metric,
    ),
    components(schemas(
        // signaldb-api admin DTOs
        signaldb_api::ApiError,
        signaldb_api::CreateTenantRequest,
        signaldb_api::UpdateTenantRequest,
        signaldb_api::TenantResponse,
        signaldb_api::ListTenantsResponse,
        signaldb_api::CreateApiKeyRequest,
        signaldb_api::UpdateApiKeyRequest,
        signaldb_api::CreateApiKeyResponse,
        signaldb_api::ApiKeyResponse,
        signaldb_api::ListApiKeysResponse,
        signaldb_api::CreateDatasetRequest,
        signaldb_api::DatasetResponse,
        signaldb_api::ListDatasetsResponse,
        signaldb_api::CreateUserRequest,
        signaldb_api::UserResponse,
        // Tenant self-service DTOs
        common::tenant_api::TenantInfo,
        common::tenant_api::ListTenantsResponse,
        common::tenant_api::TableInfo,
        common::tenant_api::ListTablesResponse,
        crate::endpoints::tenant::CreateTenantTablesResponse,
        crate::endpoints::tenant::AvailableSchemasResponse,
        // management (session-authenticated) DTOs
        crate::endpoints::management::CreateTenantRequest,
        crate::endpoints::management::ManageCreatedTenant,
        crate::endpoints::management::ManageError,
        crate::endpoints::management::DatasetResponse,
        crate::endpoints::management::CreateDatasetRequest,
        crate::endpoints::management::CreateApiKeyRequest,
        crate::endpoints::management::UpdateApiKeyRequest,
        crate::endpoints::management::ApiKeyResponse,
        crate::endpoints::management::ManageCreatedApiKey,
        crate::endpoints::management::MembershipResponse,
        crate::endpoints::management::UpsertMembershipRequest,
        crate::endpoints::management::ManageLogicalField,
        crate::endpoints::management::ManagePhysicalField,
        crate::endpoints::management::ManagePhysicalSchema,
        crate::endpoints::management::ManageSchemaResponse,
        common::schema::logical::LogicalType,
        common::schema::logical::Filterability,
        common::schema::logical::LogicalFieldKind,
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
        tempo_api::TagScope,
        tempo_api::TagSearchResponse,
        tempo_api::TagValuesResponse,
        tempo_api::v2::TagSearchResponse,
        tempo_api::v2::TagSearchScope,
        tempo_api::v2::TagValuesResponse,
        tempo_api::v2::TagWithValue,
        // Native Query IR request/response DTOs
        crate::endpoints::query::QueryIrRequest,
        crate::endpoints::query::QueryRange,
        crate::endpoints::query::QueryIrResponse,
        crate::endpoints::query::ResolvedWindow,
        crate::endpoints::query::ResultColumn,
        crate::endpoints::query::ResultSeries,
        crate::endpoints::query::HeatmapAxisX,
        crate::endpoints::query::HeatmapAxisY,
        crate::endpoints::query::HeatmapCell,
        crate::endpoints::query::HeatmapResult,
        // OAuth 2.1 connector consent DTOs
        crate::endpoints::oauth::ConsentDecision,
        crate::endpoints::oauth::ConsentDecisionResponse,
        crate::endpoints::oauth::ConsentContextResponse,
        crate::endpoints::oauth::ConsentTenant,
        // Schema registry DTOs
        crate::endpoints::schema::SchemaError,
        crate::endpoints::schema::RegistryListResponse,
        crate::endpoints::schema::RegistryResponse,
        crate::endpoints::schema::AttributeSearchResponse,
        crate::endpoints::schema::EntitySearchResponse,
        crate::endpoints::schema::MetricSearchResponse,
        crate::endpoints::schema::AttributeResolution,
        crate::endpoints::schema::EntityResolution,
        crate::endpoints::schema::MetricResolution,
        common::schema_registry::RegistrySource,
        common::schema_registry::RegistrySummary,
        common::schema_registry::ValidationReport,
        common::schema_registry::AttributeHit,
        common::schema_registry::QualifiedEntityRole,
        common::schema_registry::EntityHit,
        common::schema_registry::MetricHit,
        schema_model::ValidationError,
        schema_model::DeprecatedInfo,
        schema_model::AttributeDef,
        schema_model::EntityAttribute,
        schema_model::EntityDef,
        schema_model::MetricAttribute,
        schema_model::MetricDef,
        schema_model::EnumMember,
        schema_model::Role,
        // Rate-limit rejection envelope (change: query-throttle-signalling)
        crate::endpoints::api_error::ApiErrorBody,
    ), responses(
        crate::endpoints::api_error::RateLimited,
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

    /// The tenant self-service endpoints (`endpoints/tenant.rs`) used to have
    /// no `#[utoipa::path]` annotations at all, so they were invisible to the
    /// OpenAPI document and therefore to every generated client (SDK, CLI,
    /// MCP, UI). This is the failing-test-first guard for
    /// `mcp-admin-tool-parity` task 1.1: every one of the six routes must have
    /// an operation in the document.
    #[test]
    fn tenant_self_service_routes_are_in_openapi_document() {
        let doc = openapi_document();
        let paths = &doc.paths.paths;

        for path in [
            "/api/v1/tenants",
            "/api/v1/tenants/{tenant_id}",
            "/api/v1/tenants/{tenant_id}/tables",
            "/api/v1/tenants/{tenant_id}/tables/create",
            "/api/v1/tenants/{tenant_id}/schemas",
            "/api/v1/schemas/available",
        ] {
            assert!(
                paths.contains_key(path),
                "OpenAPI document is missing operation for {path}"
            );
        }
    }

    /// Route-vs-OpenAPI drift guard (design D4). axum 0.8 does not expose a
    /// public route-introspection API, so this mirrors the route nesting
    /// declared in `lib.rs::create_router` by hand for the endpoint modules
    /// whose `router()` function is a plain, greppable list of
    /// `.route("path", ...)` calls under one fixed mount prefix.
    /// `known_routes_match_router_fn_source` below extracts the actual route
    /// paths straight out of those source files and cross-checks them
    /// against `KNOWN_ROUTES` / `ALLOWLISTED_ROUTES` bidirectionally, so a
    /// route added to one of those files without updating this list fails
    /// loudly instead of silently escaping the guard. `admin.rs` (assembled
    /// inline in `lib.rs`, not a standalone `router()` fn) and the
    /// public/infra routes (`/health`, `/api/v1/openapi.json`, session,
    /// OAuth) are out of scope for the extraction and are trusted by
    /// inspection instead.
    const KNOWN_ROUTES: &[&str] = &[
        // endpoints/tempo.rs, mounted at /tempo
        "/tempo/api/search",
        "/tempo/api/traces/{trace_id}",
        "/tempo/api/search/tags",
        "/tempo/api/search/tag/{tag_name}/values",
        "/tempo/api/v2/search/tags",
        "/tempo/api/v2/search/tag/{tag_name}/values",
        // endpoints/loki (logql.rs), mounted at /loki
        "/loki/api/v1/query",
        "/loki/api/v1/query_range",
        "/loki/api/v1/labels",
        "/loki/api/v1/label/{name}/values",
        // endpoints/promql.rs, mounted at /prometheus
        "/prometheus/api/v1/query",
        "/prometheus/api/v1/query_range",
        "/prometheus/api/v1/labels",
        "/prometheus/api/v1/label/{name}/values",
        // endpoints/admin.rs, mounted at /api/v1/admin (assembled inline in
        // lib.rs, not extracted — see `known_routes_match_router_fn_source`)
        "/api/v1/admin/tenants",
        "/api/v1/admin/tenants/{tenant_id}",
        "/api/v1/admin/tenants/{tenant_id}/api-keys",
        "/api/v1/admin/tenants/{tenant_id}/api-keys/{key_id}",
        "/api/v1/admin/tenants/{tenant_id}/datasets",
        "/api/v1/admin/tenants/{tenant_id}/datasets/{dataset_id}",
        "/api/v1/admin/users",
        // endpoints/ops.rs, mounted at /api/v1/ops
        "/api/v1/ops/compact",
        "/api/v1/ops/compact/status",
        "/api/v1/ops/compact/dry-run",
        // endpoints/tenant.rs, mounted at /api/v1
        "/api/v1/tenants",
        "/api/v1/tenants/{tenant_id}",
        "/api/v1/tenants/{tenant_id}/tables",
        "/api/v1/tenants/{tenant_id}/tables/create",
        "/api/v1/tenants/{tenant_id}/schemas",
        "/api/v1/schemas/available",
        // endpoints/query.rs, mounted at /api/v1
        "/api/v1/query",
        // endpoints/management.rs, mounted at /api/v1/manage
        "/api/v1/manage/tenants",
        "/api/v1/manage/tenants/{tenant_id}/datasets",
        "/api/v1/manage/tenants/{tenant_id}/datasets/{dataset_name}",
        "/api/v1/manage/tenants/{tenant_id}/api-keys",
        "/api/v1/manage/tenants/{tenant_id}/api-keys/{key_id}",
        "/api/v1/manage/tenants/{tenant_id}/memberships",
        "/api/v1/manage/tenants/{tenant_id}/memberships/{user_id}",
        "/api/v1/manage/schema",
        // endpoints/schema.rs, mounted at /api/v1/schema
        "/api/v1/schema/registries",
        "/api/v1/schema/registries:validate",
        "/api/v1/schema/registries/{namespace}/{version}",
        "/api/v1/schema/attributes",
        "/api/v1/schema/attributes/{key}",
        "/api/v1/schema/entities",
        "/api/v1/schema/entities/{name}",
        "/api/v1/schema/metrics",
        "/api/v1/schema/metrics/{name}",
    ];

    /// Routes registered by the auto-extracted files (see
    /// `known_routes_match_router_fn_source`) that are deliberately outside
    /// the OpenAPI document: pre-existing Tempo/Loki/Prometheus compat
    /// surface not yet in the API contract (tracked separately — not part of
    /// this change's scope). `/pyroscope/**`, `/api/profiles/**`, session
    /// login/logout, OAuth 2.1 endpoints, `/health`, and
    /// `/api/v1/openapi.json` are outside the extraction entirely (assembled
    /// directly in `lib.rs`, not through one of the extracted `router()`
    /// functions) and need no allowlist entry.
    const ALLOWLISTED_ROUTES: &[&str] = &[
        "/tempo/api/echo",
        "/tempo/api/v2/traces/{trace_id}",
        "/tempo/api/metrics/query",
        "/tempo/api/metrics/query_range",
        "/loki/api/v1/series",
        "/loki/api/v1/detected_fields",
        "/prometheus/api/v1/label_stats",
        "/prometheus/api/v1/series",
    ];

    /// Extracts the literal path strings passed to `.route(` in a router
    /// source file, e.g. `.route("/tenants", get(...))` or the multi-line
    /// form `.route(\n    "/tenants/{id}",\n    get(...),\n)`. Chained verbs
    /// (`.route("/x", get(a).post(b))`) contribute the path once, matching
    /// how axum treats them as one route with multiple methods.
    fn extract_route_paths(content: &str) -> Vec<String> {
        let mut paths = Vec::new();
        let mut rest = content;
        while let Some(idx) = rest.find(".route(") {
            let after = &rest[idx + ".route(".len()..];
            let quote_start = after.find('"').unwrap_or_else(|| {
                panic!("`.route(` not followed by a quoted path literal near: {after:.80}")
            });
            let after_quote = &after[quote_start + 1..];
            let quote_end = after_quote
                .find('"')
                .expect("unterminated string literal after `.route(`");
            paths.push(after_quote[..quote_end].to_string());
            rest = &after_quote[quote_end + 1..];
        }
        paths
    }

    /// Cross-checks `KNOWN_ROUTES`/`ALLOWLISTED_ROUTES` against the actual
    /// `.route(...)` calls in the endpoint modules whose `router()` function
    /// registers routes under one fixed mount prefix, catching drift in both
    /// directions: a route added to source without a list entry, or a list
    /// entry for a route that no longer exists.
    #[test]
    fn known_routes_match_router_fn_source() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let files_with_prefix = [
            ("src/endpoints/tempo.rs", "/tempo"),
            ("src/endpoints/logql.rs", "/loki"),
            ("src/endpoints/promql.rs", "/prometheus"),
            ("src/endpoints/ops.rs", "/api/v1/ops"),
            ("src/endpoints/tenant.rs", "/api/v1"),
            ("src/endpoints/query.rs", "/api/v1"),
            ("src/endpoints/management.rs", "/api/v1/manage"),
            ("src/endpoints/schema.rs", "/api/v1/schema"),
        ];

        let known: std::collections::HashSet<&str> = KNOWN_ROUTES.iter().copied().collect();
        let allowlisted: std::collections::HashSet<&str> =
            ALLOWLISTED_ROUTES.iter().copied().collect();

        let mut actual: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (file, prefix) in files_with_prefix {
            let path = format!("{manifest_dir}/{file}");
            let content = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("failed to read {path}: {e}"));
            for relative in extract_route_paths(&content) {
                actual.insert(format!("{prefix}{relative}"));
            }
        }

        let mut undeclared: Vec<&String> = actual
            .iter()
            .filter(|route| {
                !known.contains(route.as_str()) && !allowlisted.contains(route.as_str())
            })
            .collect();
        undeclared.sort();
        assert!(
            undeclared.is_empty(),
            "routes registered in source but missing from KNOWN_ROUTES/ALLOWLISTED_ROUTES \
             (add an OpenAPI operation and a KNOWN_ROUTES entry, or an ALLOWLISTED_ROUTES entry \
             with a reason): {undeclared:?}"
        );

        // admin.rs routes are assembled inline in lib.rs (not one of the
        // extracted `router()` functions above) and are trusted by
        // inspection rather than extracted; exclude them from the
        // stale-entry check.
        let mut stale: Vec<&str> = known
            .iter()
            .chain(allowlisted.iter())
            .filter(|route| !route.starts_with("/api/v1/admin/"))
            .filter(|route| !actual.contains(**route))
            .copied()
            .collect();
        stale.sort();
        assert!(
            stale.is_empty(),
            "KNOWN_ROUTES/ALLOWLISTED_ROUTES entries no longer present in source: {stale:?}"
        );
    }

    /// `/pyroscope/**`, `/api/profiles/**` (trace-to-profile correlation, not
    /// yet in the API contract — tracked separately), session login/logout,
    /// OAuth 2.1 connector endpoints, `/health`, and `/api/v1/openapi.json`
    /// itself are deliberately outside the OpenAPI document: they are either
    /// not part of the tenant/admin HTTP contract (Pyroscope compat, health)
    /// or are public infrastructure endpoints the SDK has no business calling
    /// (session cookies, OAuth redirects, the spec document itself). Routes
    /// in `ALLOWLISTED_ROUTES` are pre-existing gaps out of this change's
    /// scope, not required to have an operation.
    #[test]
    fn every_known_route_has_an_openapi_operation() {
        let doc = openapi_document();
        let paths = &doc.paths.paths;

        let mut missing = Vec::new();
        for route in KNOWN_ROUTES {
            if !paths.contains_key(*route) {
                missing.push(*route);
            }
        }
        assert!(
            missing.is_empty(),
            "routes registered in the router but missing an OpenAPI operation: {missing:?}"
        );
    }

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

    /// Drift guard (change: query-throttle-signalling): every operation
    /// mounted behind the router's rate limiters — the query budget
    /// (`query_rate_layer`, see `lib.rs`) and the admin per-tenant quotas
    /// (`endpoints::admin`) — must declare a `429` response carrying at
    /// least the `Retry-After` header, so a new rate-limited endpoint can't
    /// silently ship without the retry contract.
    #[test]
    fn every_rate_limited_path_declares_429_with_retry_after() {
        let spec: serde_json::Value =
            serde_json::from_str(&openapi_document().to_pretty_json().unwrap()).unwrap();
        let components_responses = spec
            .pointer("/components/responses")
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        // (path, method) pairs mounted under `query_rate_layer` in
        // `create_router` (tempo/pyroscope/loki/prometheus/api-profiles and
        // the `/api/v1` tenant-scoped nest: query IR, whoami, management,
        // schema) plus the admin per-tenant count quotas, which answer 429
        // via the same header contract (see `endpoints::admin`).
        let rate_limited: &[(&str, &str)] = &[
            ("/tempo/api/search", "get"),
            ("/tempo/api/traces/{trace_id}", "get"),
            ("/tempo/api/search/tags", "get"),
            ("/tempo/api/search/tag/{tag_name}/values", "get"),
            ("/prometheus/api/v1/query", "get"),
            ("/prometheus/api/v1/query_range", "get"),
            ("/prometheus/api/v1/labels", "get"),
            ("/prometheus/api/v1/label/{name}/values", "get"),
            ("/loki/api/v1/query", "get"),
            ("/loki/api/v1/query_range", "get"),
            ("/loki/api/v1/labels", "get"),
            ("/loki/api/v1/label/{name}/values", "get"),
            ("/api/v1/query", "post"),
            ("/api/v1/whoami", "get"),
            ("/api/v1/admin/tenants/{tenant_id}/api-keys", "post"),
            ("/api/v1/admin/tenants/{tenant_id}/datasets", "post"),
        ];

        for (path, method) in rate_limited {
            let response = spec
                .pointer(&format!(
                    "/paths/{}/{method}/responses/429",
                    path.replace('/', "~1")
                ))
                .unwrap_or_else(|| panic!("{method} {path}: no 429 response declared"));
            let resolved = match response.get("$ref").and_then(|v| v.as_str()) {
                Some(r) => {
                    let name = r.rsplit('/').next().unwrap();
                    components_responses
                        .get(name)
                        .unwrap_or_else(|| panic!("{method} {path}: unresolved $ref {r}"))
                }
                None => response,
            };
            assert!(
                resolved.pointer("/headers/Retry-After").is_some(),
                "{method} {path}: 429 response is missing the Retry-After header"
            );
        }
    }
}
