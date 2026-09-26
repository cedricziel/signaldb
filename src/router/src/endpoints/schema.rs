//! # Schema registry API (`/api/v1/schema/*`)
//!
//! Registry listing and documents, tenant custom-registry management, and the
//! resolved lookup / prefix search surface over the tenant's visible
//! registries (change: `schema-registry`). Reads require `schema:read`;
//! mutations and `:validate` require `schema:write` (see
//! [`common::auth::TenantContext::can_read_schema`] /
//! [`can_write_schema`](common::auth::TenantContext::can_write_schema)).
//! Bundled registries additionally refuse mutation with `409`.

use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use common::auth::{TenantContext, dataset_allowed};
use common::schema::type_authority::AttributeTypeRecord;
use common::schema_registry::{
    AttributeHit, EntityHit, MetricHit, RegistrySummary, Resolution, StoreError, ValidationReport,
};
use schema_model::{RegistryDocument, ValidationError};
use serde::{Deserialize, Serialize};

use crate::RouterAppState;

/// Upper bound on `limit` for prefix searches.
pub const MAX_SEARCH_LIMIT: usize = 200;
const DEFAULT_SEARCH_LIMIT: usize = 50;

pub fn router() -> Router<RouterAppState> {
    Router::new()
        .route("/registries", get(list_registries).post(create_registry))
        .route("/registries:validate", post(validate_registry))
        .route(
            "/registries/{namespace}/{version}",
            get(get_registry)
                .put(replace_registry)
                .delete(delete_registry),
        )
        .route("/attributes", get(search_attributes))
        .route("/attributes/{key}", get(resolve_attribute))
        .route("/entities", get(search_entities))
        .route("/entities/{name}", get(resolve_entity))
        .route("/metrics", get(search_metrics))
        .route("/metrics/{name}", get(resolve_metric))
}

// ---- DTOs -----------------------------------------------------------------

/// Error body for the schema API.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SchemaError {
    pub error: String,
    /// Validation errors with document paths (422 only).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<ValidationError>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct RegistryListResponse {
    pub registries: Vec<RegistrySummary>,
}

/// A registry with its document (the uploaded Weaver-model file, verbatim).
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct RegistryResponse {
    #[serde(flatten)]
    pub summary: RegistrySummary,
    /// The registry document in the OpenTelemetry Weaver semantic-convention
    /// model (`name`, `version`, `schema_url`, `dependencies`, `groups`).
    #[schema(value_type = Object)]
    pub document: RegistryDocument,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AttributeSearchResponse {
    pub hits: Vec<AttributeHit>,
    /// Present when `keys=` was given: one resolution per requested key.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolutions: Vec<AttributeResolution>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct EntitySearchResponse {
    pub hits: Vec<EntityHit>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct MetricSearchResponse {
    pub hits: Vec<MetricHit>,
    /// Present when `keys=` was given: one resolution per requested name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolutions: Vec<MetricResolution>,
}

/// Every definition of one attribute key across the visible registries, in
/// precedence order; `primary` is the first.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AttributeResolution {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary: Option<AttributeHit>,
    pub hits: Vec<AttributeHit>,
    /// The canonical type the type authority committed for this key, per
    /// dataset/signal/level it has been observed in, and how many values
    /// arrived with a different type (kept, but not typed-queryable). Absent
    /// when no type has been established yet.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub canonical_types: Vec<AttributeTypeRecord>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct EntityResolution {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary: Option<EntityHit>,
    pub hits: Vec<EntityHit>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct MetricResolution {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary: Option<MetricHit>,
    pub hits: Vec<MetricHit>,
}

impl From<Resolution<AttributeHit>> for AttributeResolution {
    fn from(r: Resolution<AttributeHit>) -> Self {
        AttributeResolution {
            key: r.key,
            primary: r.primary,
            hits: r.hits,
            canonical_types: Vec::new(),
        }
    }
}
impl From<Resolution<EntityHit>> for EntityResolution {
    fn from(r: Resolution<EntityHit>) -> Self {
        EntityResolution {
            key: r.key,
            primary: r.primary,
            hits: r.hits,
        }
    }
}
impl From<Resolution<MetricHit>> for MetricResolution {
    fn from(r: Resolution<MetricHit>) -> Self {
        MetricResolution {
            key: r.key,
            primary: r.primary,
            hits: r.hits,
        }
    }
}

/// Query parameters for prefix search / batch resolution (attributes and
/// metrics — see [`EntitySearchParams`] for entities, which do not resolve
/// an exact key set).
#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct SearchParams {
    /// Name prefix (empty lists from the top).
    #[serde(default)]
    pub prefix: String,
    /// Maximum hits (default 50, max 200).
    pub limit: Option<usize>,
    /// Comma-separated exact keys to resolve in one call (attributes and
    /// metrics only).
    pub keys: Option<String>,
}

/// Query parameters for entity prefix search. Entities have no `keys=`
/// batch-resolution parameter: unlike attributes and metrics, there is no
/// endpoint support for resolving an exact entity-type name set in one call.
#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct EntitySearchParams {
    /// Name prefix (empty lists from the top).
    #[serde(default)]
    pub prefix: String,
    /// Maximum hits (default 50, max 200).
    pub limit: Option<usize>,
}

// ---- helpers --------------------------------------------------------------

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(SchemaError {
            error: message.into(),
            errors: Vec::new(),
        }),
    )
        .into_response()
}

fn store_error(err: StoreError) -> Response {
    match err {
        StoreError::ReservedNamespace(_) | StoreError::IdentityMismatch { .. } => {
            error(StatusCode::UNPROCESSABLE_ENTITY, err.to_string())
        }
        StoreError::ReadOnly { .. } | StoreError::AlreadyExists { .. } => {
            error(StatusCode::CONFLICT, err.to_string())
        }
        StoreError::NotFound { .. } => error(StatusCode::NOT_FOUND, err.to_string()),
        StoreError::Invalid(errors) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(SchemaError {
                error: "registry document is invalid".into(),
                errors,
            }),
        )
            .into_response(),
        StoreError::Corrupt(_) | StoreError::Database(_) => {
            tracing::error!(error = %err, "schema registry store failure");
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "schema registry store failure",
            )
        }
    }
}

fn require_read(ctx: &TenantContext) -> Result<(), Box<Response>> {
    if ctx.can_read_schema() {
        Ok(())
    } else {
        Err(Box::new(error(
            StatusCode::FORBIDDEN,
            "missing schema:read scope",
        )))
    }
}

fn require_write(ctx: &TenantContext) -> Result<(), Box<Response>> {
    if ctx.can_write_schema() {
        Ok(())
    } else {
        Err(Box::new(error(
            StatusCode::FORBIDDEN,
            "schema:write scope (and tenant admin role for sessions) required",
        )))
    }
}

/// Parse a registry document from the request body: JSON by default, YAML
/// when the content type says so.
fn parse_document(headers: &HeaderMap, body: &Bytes) -> Result<RegistryDocument, Box<Response>> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_ascii_lowercase();
    let text = std::str::from_utf8(body)
        .map_err(|_| Box::new(error(StatusCode::BAD_REQUEST, "request body is not UTF-8")))?;
    let parsed = if content_type.contains("yaml") || content_type.contains("yml") {
        RegistryDocument::from_yaml(text)
    } else {
        RegistryDocument::from_json(text)
    };
    parsed.map_err(|e| {
        Box::new(error(
            StatusCode::BAD_REQUEST,
            format!("cannot parse registry document: {e}"),
        ))
    })
}

fn clamp_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(DEFAULT_SEARCH_LIMIT)
        .clamp(1, MAX_SEARCH_LIMIT)
}

/// Split a `keys=` query value into trimmed, non-empty names, capped at
/// [`MAX_SEARCH_LIMIT`]. `None` when the value is absent or blank.
fn split_keys(raw: &Option<String>) -> Option<impl Iterator<Item = &str>> {
    raw.as_deref().filter(|k| !k.trim().is_empty()).map(|keys| {
        keys.split(',')
            .map(str::trim)
            .filter(|k| !k.is_empty())
            .take(MAX_SEARCH_LIMIT)
    })
}

// ---- registries -----------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/schema/registries",
    tag = "schema",
    operation_id = "schema_list_registries",
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Registries visible to the tenant, precedence order", body = RegistryListResponse),
        (status = 403, description = "Missing schema:read scope", body = SchemaError),
    ),
    security(("bearer" = []))
)]
pub async fn list_registries(
    State(state): State<RouterAppState>,
    Extension(ctx): Extension<TenantContext>,
) -> Response {
    if let Err(r) = require_read(&ctx) {
        return *r;
    }
    match state.schema_resolver().list(&ctx.tenant_id).await {
        Ok(registries) => Json(RegistryListResponse { registries }).into_response(),
        Err(e) => store_error(e),
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/schema/registries",
    tag = "schema",
    operation_id = "schema_create_registry",
    request_body(content = Object, description = "Registry document (Weaver semantic-convention model) as JSON, or YAML with a yaml content type", content_type = "application/json"),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 201, description = "Registry created", body = RegistrySummary),
        (status = 400, description = "Unparseable document", body = SchemaError),
        (status = 403, description = "Missing schema:write scope", body = SchemaError),
        (status = 409, description = "Registry already exists", body = SchemaError),
        (status = 422, description = "Invalid document (errors carry paths)", body = SchemaError),
    ),
    security(("bearer" = []))
)]
pub async fn create_registry(
    State(state): State<RouterAppState>,
    Extension(ctx): Extension<TenantContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(r) = require_write(&ctx) {
        return *r;
    }
    let doc = match parse_document(&headers, &body) {
        Ok(d) => d,
        Err(r) => return *r,
    };
    match state.schema_resolver().create(&ctx.tenant_id, &doc).await {
        Ok(summary) => {
            tracing::info!(tenant_id = %ctx.tenant_id, namespace = %summary.namespace, version = %summary.version, "schema registry created");
            (StatusCode::CREATED, Json(summary)).into_response()
        }
        Err(e) => store_error(e),
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/schema/registries:validate",
    tag = "schema",
    operation_id = "schema_validate_registry",
    request_body(content = Object, description = "Registry document to validate (JSON, or YAML with a yaml content type); nothing is stored", content_type = "application/json"),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Validation outcome (errors with paths, or resulting counts)", body = ValidationReport),
        (status = 400, description = "Unparseable document", body = SchemaError),
        (status = 403, description = "Missing schema:write scope", body = SchemaError),
    ),
    security(("bearer" = []))
)]
pub async fn validate_registry(
    State(state): State<RouterAppState>,
    Extension(ctx): Extension<TenantContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(r) = require_write(&ctx) {
        return *r;
    }
    let doc = match parse_document(&headers, &body) {
        Ok(d) => d,
        Err(r) => return *r,
    };
    match state.schema_resolver().validate(&ctx.tenant_id, &doc).await {
        Ok(report) => Json(report).into_response(),
        Err(e) => store_error(e),
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/schema/registries/{namespace}/{version}",
    tag = "schema",
    operation_id = "schema_get_registry",
    params(("namespace" = String, Path, description = "Registry namespace"), ("version" = String, Path, description = "Registry version")),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Registry summary and document", body = RegistryResponse),
        (status = 403, description = "Missing schema:read scope", body = SchemaError),
        (status = 404, description = "No such registry visible to the tenant", body = SchemaError),
    ),
    security(("bearer" = []))
)]
pub async fn get_registry(
    State(state): State<RouterAppState>,
    Extension(ctx): Extension<TenantContext>,
    Path((namespace, version)): Path<(String, String)>,
) -> Response {
    if let Err(r) = require_read(&ctx) {
        return *r;
    }
    match state
        .schema_resolver()
        .get(&ctx.tenant_id, &namespace, &version)
        .await
    {
        Ok(Some((summary, document))) => {
            Json(RegistryResponse { summary, document }).into_response()
        }
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            format!("registry {namespace}@{version} not found"),
        ),
        Err(e) => store_error(e),
    }
}

#[utoipa::path(
    put,
    path = "/api/v1/schema/registries/{namespace}/{version}",
    tag = "schema",
    operation_id = "schema_replace_registry",
    params(("namespace" = String, Path, description = "Registry namespace"), ("version" = String, Path, description = "Registry version")),
    request_body(content = Object, description = "Replacement registry document; its name/version must match the path", content_type = "application/json"),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Registry replaced", body = RegistrySummary),
        (status = 400, description = "Unparseable document", body = SchemaError),
        (status = 403, description = "Missing schema:write scope", body = SchemaError),
        (status = 404, description = "No such custom registry", body = SchemaError),
        (status = 409, description = "Registry is bundled and read-only", body = SchemaError),
        (status = 422, description = "Invalid document or identity mismatch", body = SchemaError),
    ),
    security(("bearer" = []))
)]
pub async fn replace_registry(
    State(state): State<RouterAppState>,
    Extension(ctx): Extension<TenantContext>,
    Path((namespace, version)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(r) = require_write(&ctx) {
        return *r;
    }
    let doc = match parse_document(&headers, &body) {
        Ok(d) => d,
        Err(r) => return *r,
    };
    match state
        .schema_resolver()
        .replace(&ctx.tenant_id, &namespace, &version, &doc)
        .await
    {
        Ok(summary) => {
            tracing::info!(tenant_id = %ctx.tenant_id, namespace = %namespace, version = %version, "schema registry replaced");
            Json(summary).into_response()
        }
        Err(e) => store_error(e),
    }
}

#[utoipa::path(
    delete,
    path = "/api/v1/schema/registries/{namespace}/{version}",
    tag = "schema",
    operation_id = "schema_delete_registry",
    params(("namespace" = String, Path, description = "Registry namespace"), ("version" = String, Path, description = "Registry version")),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 204, description = "Registry deleted"),
        (status = 403, description = "Missing schema:write scope", body = SchemaError),
        (status = 404, description = "No such custom registry", body = SchemaError),
        (status = 409, description = "Registry is bundled and read-only", body = SchemaError),
    ),
    security(("bearer" = []))
)]
pub async fn delete_registry(
    State(state): State<RouterAppState>,
    Extension(ctx): Extension<TenantContext>,
    Path((namespace, version)): Path<(String, String)>,
) -> Response {
    if let Err(r) = require_write(&ctx) {
        return *r;
    }
    match state
        .schema_resolver()
        .delete(&ctx.tenant_id, &namespace, &version)
        .await
    {
        Ok(true) => {
            tracing::info!(tenant_id = %ctx.tenant_id, namespace = %namespace, version = %version, "schema registry deleted");
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => error(
            StatusCode::NOT_FOUND,
            format!("registry {namespace}@{version} not found"),
        ),
        Err(e) => store_error(e),
    }
}

// ---- type authority ---------------------------------------------------------

/// The canonical types the type authority has committed for `key`, filtered
/// to the datasets `ctx`'s credential may see. `Err` is a ready-to-return
/// error response (a store failure, mapped to `500`).
async fn canonical_types(
    state: &RouterAppState,
    ctx: &TenantContext,
    key: &str,
) -> Result<Vec<AttributeTypeRecord>, Box<Response>> {
    match state
        .catalog()
        .list_attribute_types(&ctx.tenant_id, key)
        .await
    {
        Ok(records) => Ok(records
            .into_iter()
            .filter(|r| dataset_allowed(ctx.api_key_dataset_ids.as_deref(), &r.dataset))
            .collect()),
        Err(e) => {
            tracing::error!(error = %e, "attribute type authority lookup failed");
            Err(Box::new(error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "attribute type authority lookup failed",
            )))
        }
    }
}

// ---- resolution / search ---------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/schema/attributes",
    tag = "schema",
    operation_id = "schema_search_attributes",
    params(SearchParams),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Prefix hits (and per-key resolutions when keys= is given)", body = AttributeSearchResponse),
        (status = 403, description = "Missing schema:read scope", body = SchemaError),
    ),
    security(("bearer" = []))
)]
pub async fn search_attributes(
    State(state): State<RouterAppState>,
    Extension(ctx): Extension<TenantContext>,
    Query(params): Query<SearchParams>,
) -> Response {
    if let Err(r) = require_read(&ctx) {
        return *r;
    }
    let resolver = state.schema_resolver();
    if let Some(keys) = split_keys(&params.keys) {
        let mut resolutions = Vec::new();
        for key in keys {
            let (resolved, types) = tokio::join!(
                resolver.resolve_attribute(&ctx.tenant_id, key),
                canonical_types(&state, &ctx, key)
            );
            let mut resolution: AttributeResolution = match resolved {
                Ok(r) => r.into(),
                Err(e) => return store_error(e),
            };
            resolution.canonical_types = match types {
                Ok(records) => records,
                Err(resp) => return *resp,
            };
            resolutions.push(resolution);
        }
        return Json(AttributeSearchResponse {
            hits: Vec::new(),
            resolutions,
        })
        .into_response();
    }
    match resolver
        .search_attributes(&ctx.tenant_id, &params.prefix, clamp_limit(params.limit))
        .await
    {
        Ok(hits) => Json(AttributeSearchResponse {
            hits,
            resolutions: Vec::new(),
        })
        .into_response(),
        Err(e) => store_error(e),
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/schema/attributes/{key}",
    tag = "schema",
    operation_id = "schema_resolve_attribute",
    params(("key" = String, Path, description = "Attribute wire key, e.g. k8s.pod.uid")),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Every definition of the key across visible registries (empty when unknown)", body = AttributeResolution),
        (status = 403, description = "Missing schema:read scope", body = SchemaError),
    ),
    security(("bearer" = []))
)]
pub async fn resolve_attribute(
    State(state): State<RouterAppState>,
    Extension(ctx): Extension<TenantContext>,
    Path(key): Path<String>,
) -> Response {
    if let Err(r) = require_read(&ctx) {
        return *r;
    }
    let resolver = state.schema_resolver();
    let (resolved, types) = tokio::join!(
        resolver.resolve_attribute(&ctx.tenant_id, &key),
        canonical_types(&state, &ctx, &key)
    );
    let mut resolution = match resolved {
        Ok(r) => AttributeResolution::from(r),
        Err(e) => return store_error(e),
    };
    resolution.canonical_types = match types {
        Ok(records) => records,
        Err(resp) => return *resp,
    };
    Json(resolution).into_response()
}

#[utoipa::path(
    get,
    path = "/api/v1/schema/entities",
    tag = "schema",
    operation_id = "schema_search_entities",
    params(EntitySearchParams),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Entity types whose name starts with the prefix", body = EntitySearchResponse),
        (status = 403, description = "Missing schema:read scope", body = SchemaError),
    ),
    security(("bearer" = []))
)]
pub async fn search_entities(
    State(state): State<RouterAppState>,
    Extension(ctx): Extension<TenantContext>,
    Query(params): Query<EntitySearchParams>,
) -> Response {
    if let Err(r) = require_read(&ctx) {
        return *r;
    }
    match state
        .schema_resolver()
        .search_entities(&ctx.tenant_id, &params.prefix, clamp_limit(params.limit))
        .await
    {
        Ok(hits) => Json(EntitySearchResponse { hits }).into_response(),
        Err(e) => store_error(e),
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/schema/entities/{name}",
    tag = "schema",
    operation_id = "schema_resolve_entity",
    params(("name" = String, Path, description = "Entity type name, e.g. k8s.pod")),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Every definition of the entity across visible registries (empty when unknown)", body = EntityResolution),
        (status = 403, description = "Missing schema:read scope", body = SchemaError),
    ),
    security(("bearer" = []))
)]
pub async fn resolve_entity(
    State(state): State<RouterAppState>,
    Extension(ctx): Extension<TenantContext>,
    Path(name): Path<String>,
) -> Response {
    if let Err(r) = require_read(&ctx) {
        return *r;
    }
    match state
        .schema_resolver()
        .resolve_entity(&ctx.tenant_id, &name)
        .await
    {
        Ok(r) => Json(EntityResolution::from(r)).into_response(),
        Err(e) => store_error(e),
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/schema/metrics",
    tag = "schema",
    operation_id = "schema_search_metrics",
    params(SearchParams),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Prefix hits (and per-name resolutions when keys= is given)", body = MetricSearchResponse),
        (status = 403, description = "Missing schema:read scope", body = SchemaError),
    ),
    security(("bearer" = []))
)]
pub async fn search_metrics(
    State(state): State<RouterAppState>,
    Extension(ctx): Extension<TenantContext>,
    Query(params): Query<SearchParams>,
) -> Response {
    if let Err(r) = require_read(&ctx) {
        return *r;
    }
    let resolver = state.schema_resolver();
    if let Some(keys) = split_keys(&params.keys) {
        let mut resolutions = Vec::new();
        for name in keys {
            match resolver.resolve_metric(&ctx.tenant_id, name).await {
                Ok(r) => resolutions.push(r.into()),
                Err(e) => return store_error(e),
            }
        }
        return Json(MetricSearchResponse {
            hits: Vec::new(),
            resolutions,
        })
        .into_response();
    }
    match resolver
        .search_metrics(&ctx.tenant_id, &params.prefix, clamp_limit(params.limit))
        .await
    {
        Ok(hits) => Json(MetricSearchResponse {
            hits,
            resolutions: Vec::new(),
        })
        .into_response(),
        Err(e) => store_error(e),
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/schema/metrics/{name}",
    tag = "schema",
    operation_id = "schema_resolve_metric",
    params(("name" = String, Path, description = "Metric name, e.g. k8s.pod.cpu.time")),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Every definition of the metric across visible registries (empty when unknown)", body = MetricResolution),
        (status = 403, description = "Missing schema:read scope", body = SchemaError),
    ),
    security(("bearer" = []))
)]
pub async fn resolve_metric(
    State(state): State<RouterAppState>,
    Extension(ctx): Extension<TenantContext>,
    Path(name): Path<String>,
) -> Response {
    if let Err(r) = require_read(&ctx) {
        return *r;
    }
    match state
        .schema_resolver()
        .resolve_metric(&ctx.tenant_id, &name)
        .await
    {
        Ok(r) => Json(MetricResolution::from(r)).into_response(),
        Err(e) => store_error(e),
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use common::auth::Authenticator;
    use common::catalog::{Catalog, MembershipRole};
    use common::config::{ApiKeyConfig, AuthConfig, Configuration, DatasetConfig, TenantConfig};
    use common::schema::logical::{AttributeLevel, LogicalFieldId};
    use common::schema::type_authority::{CanonicalType, Resolution, TypeSource};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use crate::{RouterAppState, create_router};

    const ACME: &str = include_str!("../../../schema-model/tests/fixtures/acme.yaml");

    /// The record-level attribute field the canonical-types tests establish
    /// a type for.
    fn order_id_field() -> LogicalFieldId {
        LogicalFieldId {
            source: "traces".to_string(),
            level: Some(AttributeLevel::Record),
            name: "acme.order.id".to_string(),
        }
    }

    /// An `Observed`-sourced resolution, the type authority's default
    /// outcome when neither config nor a semconv hint decided the type.
    fn observed(canonical: CanonicalType) -> Resolution<'static> {
        Resolution {
            canonical,
            source: TypeSource::Observed,
            hint_schema_url: None,
        }
    }

    fn tenant(id: &str, key: &str) -> TenantConfig {
        TenantConfig {
            id: id.to_string(),
            slug: id.to_string(),
            name: format!("{id} Inc"),
            default_dataset: Some("production".to_string()),
            datasets: vec![DatasetConfig {
                id: "production".to_string(),
                slug: "production".to_string(),
                is_default: true,
                storage: None,
            }],
            api_keys: vec![ApiKeyConfig {
                key: key.to_string(),
                name: Some("legacy".to_string()),
            }],
            schema_config: None,
            limits: None,
        }
    }

    /// App with two tenants. `acme-key`/`globex-key` are legacy unscoped
    /// keys; scoped DB keys `sk-read` (schema:read), `sk-write`
    /// (schema:read + schema:write), `sk-ingest` (traces:write) belong to
    /// acme; sessions: alice (Admin), vera (Viewer).
    async fn app() -> (axum::Router, Catalog) {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let config = Configuration {
            auth: AuthConfig {
                tenants: vec![tenant("acme", "acme-key"), tenant("globex", "globex-key")],
                ..Default::default()
            },
            ..Default::default()
        };
        catalog.sync_config_tenants(&config.auth).await.unwrap();
        for (key, scopes) in [
            ("sk-read", vec!["schema:read".to_string()]),
            (
                "sk-write",
                vec!["schema:read".to_string(), "schema:write".to_string()],
            ),
            ("sk-ingest", vec!["traces:write".to_string()]),
        ] {
            catalog
                .upsert_scoped_api_key(
                    "acme",
                    &Authenticator::hash_api_key(key),
                    Some(key),
                    None,
                    None,
                    Some(&scopes),
                    None,
                )
                .await
                .unwrap();
        }
        let hash = common::auth::hash_password("pw").unwrap();
        let alice = catalog
            .create_user("alice@example.com", Some("Alice"), Some(&hash), false)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&alice.id, "acme", MembershipRole::Admin)
            .await
            .unwrap();
        let vera = catalog
            .create_user("vera@example.com", Some("Vera"), Some(&hash), false)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&vera.id, "acme", MembershipRole::Viewer)
            .await
            .unwrap();
        (
            create_router(RouterAppState::new(catalog.clone(), config)),
            catalog,
        )
    }

    async fn login(app: &axum::Router, email: &str) -> String {
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ui/session")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"email": email, "password": "pw", "tenant_id": "acme"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 200, "login {email}");
        res.headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string()
    }

    enum Auth<'a> {
        Key(&'a str, &'a str),
        Cookie(&'a str),
    }

    async fn call(
        app: &axum::Router,
        auth: Auth<'_>,
        method: &str,
        uri: &str,
        body: Option<(&str, String)>,
    ) -> (u16, Value) {
        let mut req = Request::builder().method(method).uri(uri);
        match auth {
            Auth::Key(key, tenant) => {
                req = req
                    .header("authorization", format!("Bearer {key}"))
                    .header("x-tenant-id", tenant);
            }
            Auth::Cookie(cookie) => {
                req = req.header("cookie", cookie).header("x-tenant-id", "acme");
            }
        }
        let body = match body {
            Some((ct, text)) => {
                req = req.header("content-type", ct);
                Body::from(text)
            }
            None => Body::empty(),
        };
        let res = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
        let status = res.status().as_u16();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let json = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or(Value::String(String::from_utf8_lossy(&bytes).to_string()))
        };
        (status, json)
    }

    fn acme_json() -> String {
        let doc = schema_model::RegistryDocument::from_yaml(ACME).unwrap();
        serde_json::to_string(&doc).unwrap()
    }

    // ---- 5.1 registries -------------------------------------------------

    #[tokio::test]
    async fn list_and_get_registries_require_schema_read() {
        let (app, _) = app().await;
        let (status, body) = call(
            &app,
            Auth::Key("acme-key", "acme"),
            "GET",
            "/api/v1/schema/registries",
            None,
        )
        .await;
        assert_eq!(status, 200, "{body}");
        let regs = body["registries"].as_array().unwrap();
        assert!(regs.iter().any(|r| r["namespace"] == "otel"
            && r["source"] == "bundled"
            && r["read_only"] == true
            && r["attribute_count"].as_u64().unwrap() > 800));
        assert!(regs.iter().any(|r| r["namespace"] == "signaldb"));

        let (status, _) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/schema/registries",
            None,
        )
        .await;
        assert_eq!(status, 200);
        let (status, _) = call(
            &app,
            Auth::Key("sk-ingest", "acme"),
            "GET",
            "/api/v1/schema/registries",
            None,
        )
        .await;
        assert_eq!(status, 403, "ingest-only key cannot read the registry");

        let (status, body) = call(
            &app,
            Auth::Key("acme-key", "acme"),
            "GET",
            "/api/v1/schema/registries/otel/1.43.0",
            None,
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["namespace"], "otel");
        assert!(body["document"]["groups"].as_array().unwrap().len() > 900);
        let (status, _) = call(
            &app,
            Auth::Key("acme-key", "acme"),
            "GET",
            "/api/v1/schema/registries/nope/1.0.0",
            None,
        )
        .await;
        assert_eq!(status, 404);
    }

    #[tokio::test]
    async fn custom_registry_lifecycle_json_and_yaml() {
        let (app, _) = app().await;
        // read-only key cannot create
        let (status, _) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "POST",
            "/api/v1/schema/registries",
            Some(("application/json", acme_json())),
        )
        .await;
        assert_eq!(status, 403);

        // create via YAML upload
        let (status, body) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "POST",
            "/api/v1/schema/registries",
            Some(("application/yaml", ACME.to_string())),
        )
        .await;
        assert_eq!(status, 201, "{body}");
        assert_eq!(body["namespace"], "acme");
        assert_eq!(body["source"], "custom");
        assert_eq!(body["entity_count"], 2);

        // duplicate → 409
        let (status, _) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "POST",
            "/api/v1/schema/registries",
            Some(("application/json", acme_json())),
        )
        .await;
        assert_eq!(status, 409);

        // visible to acme, not to globex
        let (status, body) = call(
            &app,
            Auth::Key("acme-key", "acme"),
            "GET",
            "/api/v1/schema/registries/acme/1.0.0",
            None,
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["document"]["name"], "acme");
        let (status, _) = call(
            &app,
            Auth::Key("globex-key", "globex"),
            "GET",
            "/api/v1/schema/registries/acme/1.0.0",
            None,
        )
        .await;
        assert_eq!(status, 404);

        // replace with an invalid doc → 422 with paths; old doc still served
        let mut bad: Value = serde_json::from_str(&acme_json()).unwrap();
        bad["groups"][0]["attributes"]
            .as_array_mut()
            .unwrap()
            .push(json!({"ref": "acme.nonexistent"}));
        let (status, body) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "PUT",
            "/api/v1/schema/registries/acme/1.0.0",
            Some(("application/json", bad.to_string())),
        )
        .await;
        assert_eq!(status, 422, "{body}");
        assert_eq!(body["errors"][0]["path"], "groups[0].attributes[3].ref");
        // identity mismatch → 422
        let (status, _) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "PUT",
            "/api/v1/schema/registries/acme/9.9.9",
            Some(("application/json", acme_json())),
        )
        .await;
        assert_eq!(status, 422);
        // valid replace → 200
        let mut edited: Value = serde_json::from_str(&acme_json()).unwrap();
        edited["description"] = json!("edited");
        let (status, body) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "PUT",
            "/api/v1/schema/registries/acme/1.0.0",
            Some(("application/json", edited.to_string())),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["description"], "edited");

        // bundled: mutation → 409 even with write scope
        let (status, _) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "DELETE",
            "/api/v1/schema/registries/otel/1.43.0",
            None,
        )
        .await;
        assert_eq!(status, 409);
        // reserved namespace on create → 422
        let mut reserved: Value = serde_json::from_str(&acme_json()).unwrap();
        reserved["name"] = json!("otel");
        let (status, _) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "POST",
            "/api/v1/schema/registries",
            Some(("application/json", reserved.to_string())),
        )
        .await;
        assert_eq!(status, 422);
        // unparseable → 400
        let (status, _) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "POST",
            "/api/v1/schema/registries",
            Some(("application/json", "{not json".to_string())),
        )
        .await;
        assert_eq!(status, 400);

        // delete → 204, then 404
        let (status, _) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "DELETE",
            "/api/v1/schema/registries/acme/1.0.0",
            None,
        )
        .await;
        assert_eq!(status, 204);
        let (status, _) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "DELETE",
            "/api/v1/schema/registries/acme/1.0.0",
            None,
        )
        .await;
        assert_eq!(status, 404);
    }

    #[tokio::test]
    async fn sessions_viewer_reads_admin_writes() {
        let (app, _) = app().await;
        let alice = login(&app, "alice@example.com").await;
        let vera = login(&app, "vera@example.com").await;
        let (status, _) = call(
            &app,
            Auth::Cookie(&vera),
            "GET",
            "/api/v1/schema/attributes/k8s.pod.uid",
            None,
        )
        .await;
        assert_eq!(status, 200);
        let (status, _) = call(
            &app,
            Auth::Cookie(&vera),
            "POST",
            "/api/v1/schema/registries",
            Some(("application/json", acme_json())),
        )
        .await;
        assert_eq!(status, 403);
        let (status, body) = call(
            &app,
            Auth::Cookie(&alice),
            "POST",
            "/api/v1/schema/registries",
            Some(("application/json", acme_json())),
        )
        .await;
        assert_eq!(status, 201, "{body}");
    }

    // ---- 5.1b validate ---------------------------------------------------

    #[tokio::test]
    async fn validate_reports_and_stores_nothing() {
        let (app, _) = app().await;
        let (status, body) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "POST",
            "/api/v1/schema/registries:validate",
            Some(("application/yaml", ACME.to_string())),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["errors"].as_array().unwrap().len(), 0);
        assert_eq!(body["entity_count"], 2);
        let (status, _) = call(
            &app,
            Auth::Key("acme-key", "acme"),
            "GET",
            "/api/v1/schema/registries/acme/1.0.0",
            None,
        )
        .await;
        assert_eq!(status, 404, "validate must not store");

        let mut bad: Value = serde_json::from_str(&acme_json()).unwrap();
        bad["groups"][0]["attributes"]
            .as_array_mut()
            .unwrap()
            .push(json!({"ref": "acme.nonexistent"}));
        let (status, body) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "POST",
            "/api/v1/schema/registries:validate",
            Some(("application/json", bad.to_string())),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["errors"][0]["path"], "groups[0].attributes[3].ref");
        // read-only key may not validate
        let (status, _) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "POST",
            "/api/v1/schema/registries:validate",
            Some(("application/json", acme_json())),
        )
        .await;
        assert_eq!(status, 403);
    }

    // ---- 5.2 resolve / search --------------------------------------------

    #[tokio::test]
    async fn resolve_and_search_endpoints() {
        let (app, _) = app().await;
        call(
            &app,
            Auth::Key("sk-write", "acme"),
            "POST",
            "/api/v1/schema/registries",
            Some(("application/json", acme_json())),
        )
        .await;

        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/schema/attributes/service.name",
            None,
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["primary"]["namespace"], "acme");
        assert_eq!(body["hits"].as_array().unwrap().len(), 2);
        assert_eq!(body["hits"][1]["namespace"], "otel");
        assert_eq!(body["hits"][1]["source"], "bundled");
        assert!(
            body["hits"][1]["entity_roles"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["entity"] == "service" && r["role"] == "identifying")
        );

        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/schema/attributes/http.status_code",
            None,
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(
            body["primary"]["deprecated"]["renamed_to"],
            "http.response.status_code"
        );

        // unknown → 200 empty
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/schema/attributes/no.such",
            None,
        )
        .await;
        assert_eq!(status, 200);
        assert!(body["primary"].is_null());
        assert_eq!(body["hits"].as_array().unwrap().len(), 0);

        // batch keys=
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/schema/attributes?keys=k8s.pod.uid,acme.order.id,no.such",
            None,
        )
        .await;
        assert_eq!(status, 200, "{body}");
        let res = body["resolutions"].as_array().unwrap();
        assert_eq!(res.len(), 3);
        assert_eq!(res[0]["key"], "k8s.pod.uid");
        assert_eq!(res[1]["primary"]["namespace"], "acme");
        assert!(res[2]["hits"].as_array().unwrap().is_empty());

        // prefix + limit
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/schema/attributes?prefix=k8s.pod.&limit=3",
            None,
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["hits"].as_array().unwrap().len(), 3);
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/schema/attributes?prefix=k8s.pod.&limit=100000",
            None,
        )
        .await;
        assert_eq!(status, 200);
        assert!(body["hits"].as_array().unwrap().len() <= super::MAX_SEARCH_LIMIT);

        // entities
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/schema/entities/k8s.pod",
            None,
        )
        .await;
        assert_eq!(status, 200, "{body}");
        let pod = &body["primary"];
        assert!(
            pod["identifying"]
                .as_array()
                .unwrap()
                .iter()
                .any(|a| a["key"] == "k8s.pod.uid")
        );
        assert!(
            pod["metrics"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m == "k8s.pod.cpu.time")
        );
        assert!(
            pod["extended_by"]
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e == "acme/acme.k8s.pod")
        );
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/schema/entities?prefix=acme.",
            None,
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["hits"].as_array().unwrap().len(), 2);

        // metrics
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/schema/metrics/k8s.pod.cpu.time",
            None,
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["primary"]["instrument"], "counter");
        assert_eq!(body["primary"]["entity_associations"][0], "k8s.pod");
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/schema/metrics?prefix=acme.",
            None,
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["hits"][0]["name"], "acme.checkout.latency");

        // batch keys= on metrics
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/schema/metrics?keys=k8s.pod.cpu.time,acme.checkout.latency,no.such",
            None,
        )
        .await;
        assert_eq!(status, 200, "{body}");
        let res = body["resolutions"].as_array().unwrap();
        assert_eq!(res.len(), 3);
        assert_eq!(res[0]["key"], "k8s.pod.cpu.time");
        assert_eq!(res[0]["primary"]["namespace"], "otel");
        assert_eq!(res[1]["primary"]["namespace"], "acme");
        assert!(res[2]["hits"].as_array().unwrap().is_empty());

        // ingest-only key is refused on reads
        let (status, _) = call(
            &app,
            Auth::Key("sk-ingest", "acme"),
            "GET",
            "/api/v1/schema/entities/k8s.pod",
            None,
        )
        .await;
        assert_eq!(status, 403);
        // no auth → 401
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/schema/attributes/k8s.pod.uid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 401);
    }

    // ---- canonical types (type authority discoverability) ----------------

    #[tokio::test]
    async fn resolve_attribute_includes_canonical_types_when_established() {
        let (app, catalog) = app().await;
        // an unestablished key omits the field entirely
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/schema/attributes/no.type.established",
            None,
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert!(
            body.get("canonical_types").is_none(),
            "no canonical_types field when nothing established: {body}"
        );

        let field = order_id_field();
        catalog
            .establish_attribute_type(
                "acme",
                "production",
                &field,
                observed(CanonicalType::String),
            )
            .await
            .unwrap();
        catalog
            .record_off_type("acme", "production", &field, 4)
            .await
            .unwrap();

        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/schema/attributes/acme.order.id",
            None,
        )
        .await;
        assert_eq!(status, 200, "{body}");
        let types = body["canonical_types"].as_array().unwrap();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0]["dataset"], "production");
        assert_eq!(types[0]["signal"], "traces");
        assert_eq!(types[0]["level"], "record");
        assert_eq!(types[0]["canonical_type"], "string");
        assert_eq!(types[0]["source"], "observed");
        assert_eq!(types[0]["off_type_count"], 4);
    }

    #[tokio::test]
    async fn resolve_attribute_canonical_types_respects_dataset_restriction() {
        let (app, catalog) = app().await;

        let field = order_id_field();
        catalog
            .establish_attribute_type(
                "acme",
                "production",
                &field,
                observed(CanonicalType::String),
            )
            .await
            .unwrap();
        catalog
            .establish_attribute_type("acme", "staging", &field, observed(CanonicalType::Int64))
            .await
            .unwrap();
        catalog.create_dataset("acme", "staging").await.unwrap();

        catalog
            .upsert_scoped_api_key(
                "acme",
                &Authenticator::hash_api_key("sk-staging-only"),
                Some("staging-only"),
                Some(&["staging".to_string()]),
                None,
                Some(&["schema:read".to_string()]),
                None,
            )
            .await
            .unwrap();

        let (status, body) = call(
            &app,
            Auth::Key("sk-staging-only", "acme"),
            "GET",
            "/api/v1/schema/attributes/acme.order.id",
            None,
        )
        .await;
        assert_eq!(status, 200, "{body}");
        let types = body["canonical_types"].as_array().unwrap();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0]["dataset"], "staging");

        // the unrestricted key still sees both datasets
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/schema/attributes/acme.order.id",
            None,
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["canonical_types"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn search_attributes_batch_keys_includes_canonical_types() {
        let (app, catalog) = app().await;
        let field = order_id_field();
        catalog
            .establish_attribute_type(
                "acme",
                "production",
                &field,
                observed(CanonicalType::String),
            )
            .await
            .unwrap();

        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/schema/attributes?keys=acme.order.id,no.such",
            None,
        )
        .await;
        assert_eq!(status, 200, "{body}");
        let res = body["resolutions"].as_array().unwrap();
        assert_eq!(res[0]["key"], "acme.order.id");
        let types = res[0]["canonical_types"].as_array().unwrap();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0]["dataset"], "production");
        assert!(
            res[1].get("canonical_types").is_none(),
            "no canonical_types field for an unestablished key: {res:?}"
        );
    }
}

// ── Logical/physical schema introspection (`GET /api/v1/schema`) ──────────
//
// Global, read-only, not tenant-scoped: the registered logical (OTel-native)
// schema and the resolved physical (storage) schema for every version of
// every signal source. Readable by any authenticated tenant credential.

/// One logical (client-visible, OTel-native) field, as registered in
/// [`common::schema::logical::LogicalSchema`].
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct LogicalField {
    source: String,
    /// `resource` | `scope` | `record`, absent when the field isn't
    /// attribute-scoped (a plain `String` here, not `Option<AttributeLevel>`
    /// — utoipa emits a nullable `$ref` enum as `oneOf: [{type: null}, ref]`,
    /// which the progenitor-generated Rust SDK client can't parse).
    level: Option<String>,
    name: String,
    value_type: common::schema::logical::LogicalType,
    filterability: common::schema::logical::Filterability,
    kind: common::schema::logical::LogicalFieldKind,
    non_native: bool,
}

fn attribute_level_str(level: Option<common::schema::logical::AttributeLevel>) -> Option<String> {
    level.map(|level| {
        match level {
            common::schema::logical::AttributeLevel::Resource => "resource",
            common::schema::logical::AttributeLevel::Scope => "scope",
            common::schema::logical::AttributeLevel::Record => "record",
        }
        .to_string()
    })
}

/// One physical (storage) column, as resolved from `schemas.toml`.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct PhysicalField {
    name: String,
    field_type: String,
    required: bool,
    computed: Option<String>,
    physical_only: bool,
}

/// One resolved table-schema version for one signal source.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct PhysicalSchema {
    source: String,
    version: String,
    is_current: bool,
    description: String,
    partition_by: Vec<String>,
    fields: Vec<PhysicalField>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct SchemaResponse {
    logical_schema_version: String,
    logical: Vec<LogicalField>,
    physical: Vec<PhysicalSchema>,
}

fn physical_schemas_for_source(
    source: &str,
    versions: &std::collections::HashMap<
        String,
        common::schema::schema_parser::TableSchemaDefinition,
    >,
    current_version: &str,
) -> Vec<PhysicalSchema> {
    use common::schema::SCHEMA_DEFINITIONS;
    let mut names: Vec<&String> = versions.keys().collect();
    names.sort();
    names
        .into_iter()
        .filter_map(|version| {
            SCHEMA_DEFINITIONS
                .resolve_table_schema(versions, version)
                .ok()
                .map(|resolved| PhysicalSchema {
                    source: source.to_string(),
                    version: resolved.version.clone(),
                    is_current: resolved.version == current_version,
                    description: resolved.description,
                    partition_by: resolved.partition_by,
                    fields: resolved
                        .fields
                        .into_iter()
                        .map(|f| PhysicalField {
                            name: f.name,
                            field_type: f.field_type,
                            required: f.required,
                            computed: f.computed,
                            physical_only: f.physical_only,
                        })
                        .collect(),
                })
        })
        .collect()
}

/// GET /api/v1/schema
///
/// The registered logical (OTel-native, client-visible) schema and the
/// resolved physical (storage) schema for every version of every signal
/// source — read-only and not tenant-scoped (the schema is global, not
/// per-tenant). Readable by any authenticated tenant credential.
#[utoipa::path(
    get,
    path = "/api/v1/schema",
    tag = "schema",
    operation_id = "get_schema",
    summary = "Get the registered logical and physical schema for every signal source",
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Logical and physical schema", body = SchemaResponse),
        (status = 401, description = "Missing or invalid credentials"),
    )
)]
pub(crate) async fn get_schema(Extension(_ctx): Extension<TenantContext>) -> Response {
    use common::schema::SCHEMA_DEFINITIONS;
    use common::schema::logical::LogicalSchema;

    let mut logical: Vec<LogicalField> = LogicalSchema::core()
        .fields()
        .map(|field| LogicalField {
            source: field.id.source.clone(),
            level: attribute_level_str(field.id.level),
            name: field.id.name.clone(),
            value_type: field.value_type,
            filterability: field.filterability,
            kind: field.kind,
            non_native: field.non_native,
        })
        .collect();
    logical.sort_by(|a, b| (&a.source, &a.name).cmp(&(&b.source, &b.name)));

    let mut physical = Vec::new();
    physical.extend(physical_schemas_for_source(
        "traces",
        &SCHEMA_DEFINITIONS.traces,
        SCHEMA_DEFINITIONS.current_trace_version(),
    ));
    physical.extend(physical_schemas_for_source(
        "logs",
        &SCHEMA_DEFINITIONS.logs,
        &SCHEMA_DEFINITIONS.metadata.current_log_version,
    ));
    for (source, versions) in [
        ("metrics_gauge", &SCHEMA_DEFINITIONS.metrics_gauge),
        ("metrics_sum", &SCHEMA_DEFINITIONS.metrics_sum),
        ("metrics_histogram", &SCHEMA_DEFINITIONS.metrics_histogram),
    ] {
        physical.extend(physical_schemas_for_source(
            source,
            versions,
            &SCHEMA_DEFINITIONS.metadata.current_metric_version,
        ));
    }

    Json(SchemaResponse {
        logical_schema_version: SCHEMA_DEFINITIONS.logical_schema_version().to_string(),
        logical,
        physical,
    })
    .into_response()
}

#[cfg(test)]
mod core_schema_tests {
    use super::*;
    use common::schema::SCHEMA_DEFINITIONS;
    use common::schema::logical::LogicalSchema;

    #[test]
    fn physical_schemas_for_source_resolves_every_version_sorted_and_flags_current() {
        let schemas = physical_schemas_for_source(
            "traces",
            &SCHEMA_DEFINITIONS.traces,
            SCHEMA_DEFINITIONS.current_trace_version(),
        );

        // schemas.toml registers physical-v1, physical-v2, physical-v3
        // (#1208: span_kind_number/status_code_number/dropped counts), and
        // physical-v4 (#1340: resource_identity) for traces.
        assert_eq!(schemas.len(), 4);
        let versions: Vec<&str> = schemas.iter().map(|s| s.version.as_str()).collect();
        assert_eq!(
            versions,
            vec!["physical-v1", "physical-v2", "physical-v3", "physical-v4"],
            "sorted by version name"
        );

        let current: Vec<&str> = schemas
            .iter()
            .filter(|s| s.is_current)
            .map(|s| s.version.as_str())
            .collect();
        assert_eq!(current, vec![SCHEMA_DEFINITIONS.current_trace_version()]);

        for schema in &schemas {
            assert_eq!(schema.source, "traces");
            assert!(!schema.fields.is_empty());
            assert!(schema.fields.iter().any(|f| f.name == "trace_id"));
        }
    }

    #[test]
    fn get_schema_dto_covers_every_signal_source() {
        let logical: Vec<LogicalField> = LogicalSchema::core()
            .fields()
            .map(|field| LogicalField {
                source: field.id.source.clone(),
                level: attribute_level_str(field.id.level),
                name: field.id.name.clone(),
                value_type: field.value_type,
                filterability: field.filterability,
                kind: field.kind,
                non_native: field.non_native,
            })
            .collect();

        let sources: std::collections::HashSet<&str> =
            logical.iter().map(|f| f.source.as_str()).collect();
        assert!(sources.contains("traces"));
        assert!(sources.contains("logs"));
    }
}
