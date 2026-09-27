//! # Eval sets API (`/api/v1/eval-sets/*`)
//!
//! CRUD over named eval sets for offline agent evaluation, plus appending
//! cases to an existing set (change: agent-offline-evals, design D7). Every
//! set belongs to the caller's tenant and dataset (`TenantContext`); another
//! tenant or dataset sees `404`. Reads require `evals:read`; writes require
//! `evals:write` and, for a user session, the tenant-admin role, checked by
//! the [`EvalsRead`] / [`EvalsWrite`] extractors before any body is read.
//! Errors use the shared [`ApiError`] envelope. Cases can also be appended
//! from a trace query run through the Query IR ([`from_traces`]).

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, FromRequestParts, Path, State},
    http::{StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use common::auth::TenantContext;
use common::eval_sets::{
    AppendCasesOutcome, EvalCase, EvalCaseSource, EvalSetRecord, EvalSetSpec, EvalSetSummary,
    MAX_CASES_PER_SET, StoreError,
};
use serde::{Deserialize, Serialize};

use crate::RouterAppState;
use crate::endpoints::api_error::{ApiError, ApiErrorBody, ApiJson};
use crate::endpoints::links::{API_V1, Link};

mod from_traces;

pub use from_traces::{AppendCasesFromTracesOutcome, AppendCasesFromTracesRequest};
use from_traces::{Options, Row, invalid};

/// Request-body cap for this API. A set holds up to
/// [`common::eval_sets::MAX_CASES_PER_SET`] cases, and a few thousand
/// cases with multi-kilobyte inputs outgrow axum's 2 MiB default.
pub const EVAL_SET_BODY_LIMIT: usize = 32 * 1024 * 1024;

/// The collection's path below [`API_V1`].
const EVAL_SETS: &str = "/eval-sets";

pub fn router() -> Router<RouterAppState> {
    Router::new()
        .route("/eval-sets", get(list_eval_sets).post(create_eval_set))
        .route(
            "/eval-sets/{name}",
            get(get_eval_set)
                .put(replace_eval_set)
                .delete(delete_eval_set),
        )
        .route("/eval-sets/{name}/cases", post(append_eval_cases))
        .route(
            "/eval-sets/{name}/cases/from-traces",
            post(append_eval_cases_from_traces),
        )
        .layer(DefaultBodyLimit::max(EVAL_SET_BODY_LIMIT))
}

// ---- DTOs -------------------------------------------------------------

/// Links on one eval set. The mutation links appear only when the caller
/// may write.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct EvalSetLinks {
    #[serde(rename = "self")]
    pub self_: Link,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replace: Option<Link>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete: Option<Link>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub append_cases: Option<Link>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub append_cases_from_traces: Option<Link>,
}

/// Links on the eval-set collection.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct EvalSetListLinks {
    #[serde(rename = "self")]
    pub self_: Link,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create: Option<Link>,
}

/// An eval set with its cases in order.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct EvalSetResponse {
    #[serde(flatten)]
    pub record: EvalSetRecord,
    #[serde(rename = "_links")]
    pub links: EvalSetLinks,
}

/// An eval set as listed, without its cases.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct EvalSetSummaryResponse {
    #[serde(flatten)]
    pub summary: EvalSetSummary,
    #[serde(rename = "_links")]
    pub links: EvalSetLinks,
}

/// Every eval set in the caller's dataset, ordered by name.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct EvalSetListResponse {
    pub items: Vec<EvalSetSummaryResponse>,
    #[serde(rename = "_links")]
    pub links: EvalSetListLinks,
}

/// Body of `POST /api/v1/eval-sets/{name}/cases`.
#[derive(Debug, Deserialize, Serialize, utoipa::ToSchema)]
pub struct AppendEvalCasesRequest {
    pub cases: Vec<EvalCase>,
}

// ---- helpers ------------------------------------------------------------

fn collection_href() -> String {
    format!("{API_V1}{EVAL_SETS}")
}

fn set_href(name: &str) -> String {
    format!("{API_V1}{EVAL_SETS}/{name}")
}

fn set_links(name: &str, can_write: bool) -> EvalSetLinks {
    let href = set_href(name);
    EvalSetLinks {
        self_: Link::get(href.clone()),
        replace: can_write.then(|| Link::with_method(href.clone(), "PUT")),
        delete: can_write.then(|| Link::with_method(href.clone(), "DELETE")),
        append_cases: can_write.then(|| Link::with_method(format!("{href}/cases"), "POST")),
        append_cases_from_traces: can_write
            .then(|| Link::with_method(format!("{href}/cases/from-traces"), "POST")),
    }
}

fn set_response(record: EvalSetRecord, ctx: &TenantContext) -> EvalSetResponse {
    let links = set_links(&record.summary.name, ctx.can_write_evals());
    EvalSetResponse { record, links }
}

fn tenant_context(parts: &Parts) -> Result<TenantContext, ApiError> {
    parts
        .extensions
        .get::<TenantContext>()
        .cloned()
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "TenantContext not found in request extensions",
            )
        })
}

/// The caller's [`TenantContext`], admitted only with `evals:read`
/// ([`TenantContext::can_read_evals`]).
pub struct EvalsRead(pub TenantContext);

impl<S: Send + Sync> FromRequestParts<S> for EvalsRead {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let ctx = tenant_context(parts)?;
        if !ctx.can_read_evals() {
            return Err(ApiError::new(
                StatusCode::FORBIDDEN,
                "missing evals:read scope",
            ));
        }
        Ok(Self(ctx))
    }
}

/// The caller's [`TenantContext`], admitted only with `evals:write` and,
/// for a session, the tenant-admin role
/// ([`TenantContext::can_write_evals`]). Placed before the body extractor,
/// so a caller without write access gets `403` before the body is read.
pub struct EvalsWrite(pub TenantContext);

impl<S: Send + Sync> FromRequestParts<S> for EvalsWrite {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let ctx = tenant_context(parts)?;
        if !ctx.can_write_evals() {
            return Err(ApiError::new(
                StatusCode::FORBIDDEN,
                "evals:write scope (and tenant admin role for sessions) required",
            ));
        }
        Ok(Self(ctx))
    }
}

impl From<StoreError> for ApiError {
    fn from(err: StoreError) -> Self {
        let status = match &err {
            StoreError::NotFound(_) => StatusCode::NOT_FOUND,
            StoreError::Conflict(_) => StatusCode::CONFLICT,
            StoreError::UnknownDataset(_) | StoreError::Invalid(_) => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            StoreError::Database(_) => {
                tracing::error!(error = %err, "eval set store failure");
                return ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "eval set store failure");
            }
        };
        ApiError::new(status, err.to_string())
    }
}

// ---- handlers -------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/eval-sets",
    tag = "eval-sets",
    operation_id = "list_eval_sets",
    summary = "List the eval sets in the caller's dataset",
    responses(
        (status = 200, description = "Eval sets without their cases, ordered by name", body = EvalSetListResponse),
        (status = 403, description = "Missing evals:read scope", body = ApiErrorBody),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn list_eval_sets(
    State(state): State<RouterAppState>,
    EvalsRead(ctx): EvalsRead,
) -> Result<Json<EvalSetListResponse>, ApiError> {
    let can_write = ctx.can_write_evals();
    let summaries = state
        .catalog()
        .list_eval_sets(&ctx.tenant_id, &ctx.dataset_id)
        .await?;
    Ok(Json(EvalSetListResponse {
        items: summaries
            .into_iter()
            .map(|summary| EvalSetSummaryResponse {
                links: set_links(&summary.name, can_write),
                summary,
            })
            .collect(),
        links: EvalSetListLinks {
            self_: Link::get(collection_href()),
            create: can_write.then(|| Link::with_method(collection_href(), "POST")),
        },
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/eval-sets",
    tag = "eval-sets",
    operation_id = "create_eval_set",
    summary = "Create an eval set in the caller's dataset",
    request_body = EvalSetSpec,
    responses(
        (status = 201, description = "Eval set created", body = EvalSetResponse,
            headers(("Location" = String, description = "URL of the new eval set"))),
        (status = 400, description = "Malformed JSON body", body = ApiErrorBody),
        (status = 403, description = "Missing evals:write scope, or a session without the tenant-admin role", body = ApiErrorBody),
        (status = 409, description = "An eval set with this name already exists in the dataset", body = ApiErrorBody),
        (status = 413, description = "Body exceeds the 32 MiB limit", body = ApiErrorBody),
        (status = 422, description = "Invalid name, empty agent, duplicate or invalid case ids, or an invalid trace id", body = ApiErrorBody),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn create_eval_set(
    State(state): State<RouterAppState>,
    EvalsWrite(ctx): EvalsWrite,
    ApiJson(spec): ApiJson<EvalSetSpec>,
) -> Result<Response, ApiError> {
    let record = state
        .catalog()
        .insert_eval_set(&ctx.tenant_id, &ctx.dataset_id, spec)
        .await?;
    tracing::info!(
        tenant_id = %ctx.tenant_id,
        dataset = %ctx.dataset_id,
        name = %record.summary.name,
        cases = record.summary.case_count,
        "eval set created"
    );
    let location = set_href(&record.summary.name);
    Ok((
        StatusCode::CREATED,
        [(header::LOCATION, location)],
        Json(set_response(record, &ctx)),
    )
        .into_response())
}

#[utoipa::path(
    get,
    path = "/api/v1/eval-sets/{name}",
    tag = "eval-sets",
    operation_id = "get_eval_set",
    summary = "Get an eval set with its cases in order",
    params(("name" = String, Path, description = "Eval set name")),
    responses(
        (status = 200, description = "The eval set and its cases", body = EvalSetResponse),
        (status = 403, description = "Missing evals:read scope", body = ApiErrorBody),
        (status = 404, description = "No such eval set in the caller's dataset", body = ApiErrorBody),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn get_eval_set(
    State(state): State<RouterAppState>,
    EvalsRead(ctx): EvalsRead,
    Path(name): Path<String>,
) -> Result<Json<EvalSetResponse>, ApiError> {
    let record = state
        .catalog()
        .get_eval_set(&ctx.tenant_id, &ctx.dataset_id, &name)
        .await?
        .ok_or(StoreError::NotFound(name))?;
    Ok(Json(set_response(record, &ctx)))
}

#[utoipa::path(
    put,
    path = "/api/v1/eval-sets/{name}",
    tag = "eval-sets",
    operation_id = "replace_eval_set",
    summary = "Replace an eval set's agent, description and cases",
    params(("name" = String, Path, description = "Eval set name")),
    request_body = EvalSetSpec,
    responses(
        (status = 200, description = "Eval set replaced", body = EvalSetResponse),
        (status = 400, description = "Malformed JSON body", body = ApiErrorBody),
        (status = 403, description = "Missing evals:write scope, or a session without the tenant-admin role", body = ApiErrorBody),
        (status = 404, description = "No such eval set (PUT never creates)", body = ApiErrorBody),
        (status = 413, description = "Body exceeds the 32 MiB limit", body = ApiErrorBody),
        (status = 422, description = "Invalid spec, or the body name differs from the path name", body = ApiErrorBody),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn replace_eval_set(
    State(state): State<RouterAppState>,
    EvalsWrite(ctx): EvalsWrite,
    Path(name): Path<String>,
    ApiJson(spec): ApiJson<EvalSetSpec>,
) -> Result<Json<EvalSetResponse>, ApiError> {
    let record = state
        .catalog()
        .replace_eval_set(&ctx.tenant_id, &ctx.dataset_id, &name, spec)
        .await?;
    tracing::info!(
        tenant_id = %ctx.tenant_id,
        dataset = %ctx.dataset_id,
        name = %record.summary.name,
        cases = record.summary.case_count,
        "eval set replaced"
    );
    Ok(Json(set_response(record, &ctx)))
}

#[utoipa::path(
    delete,
    path = "/api/v1/eval-sets/{name}",
    tag = "eval-sets",
    operation_id = "delete_eval_set",
    summary = "Delete an eval set and its cases",
    params(("name" = String, Path, description = "Eval set name")),
    responses(
        (status = 204, description = "Eval set deleted"),
        (status = 403, description = "Missing evals:write scope, or a session without the tenant-admin role", body = ApiErrorBody),
        (status = 404, description = "No such eval set in the caller's dataset", body = ApiErrorBody),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn delete_eval_set(
    State(state): State<RouterAppState>,
    EvalsWrite(ctx): EvalsWrite,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let deleted = state
        .catalog()
        .delete_eval_set(&ctx.tenant_id, &ctx.dataset_id, &name)
        .await?;
    if !deleted {
        return Err(StoreError::NotFound(name).into());
    }
    tracing::info!(
        tenant_id = %ctx.tenant_id,
        dataset = %ctx.dataset_id,
        name = %name,
        "eval set deleted"
    );
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/api/v1/eval-sets/{name}/cases",
    tag = "eval-sets",
    operation_id = "append_eval_cases",
    summary = "Append cases to an eval set, skipping ids it already holds",
    params(("name" = String, Path, description = "Eval set name")),
    request_body = AppendEvalCasesRequest,
    responses(
        (status = 200, description = "Cases appended; ids already in the set are reported, not overwritten", body = AppendCasesOutcome),
        (status = 400, description = "Malformed JSON body", body = ApiErrorBody),
        (status = 403, description = "Missing evals:write scope, or a session without the tenant-admin role", body = ApiErrorBody),
        (status = 404, description = "No such eval set in the caller's dataset", body = ApiErrorBody),
        (status = 413, description = "Body exceeds the 32 MiB limit", body = ApiErrorBody),
        (status = 422, description = "Duplicate or invalid case ids, an invalid trace id, or the set would exceed its case limit", body = ApiErrorBody),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn append_eval_cases(
    State(state): State<RouterAppState>,
    EvalsWrite(ctx): EvalsWrite,
    Path(name): Path<String>,
    ApiJson(request): ApiJson<AppendEvalCasesRequest>,
) -> Result<Json<AppendCasesOutcome>, ApiError> {
    let outcome = state
        .catalog()
        .append_eval_cases(&ctx.tenant_id, &ctx.dataset_id, &name, request.cases)
        .await?;
    tracing::info!(
        tenant_id = %ctx.tenant_id,
        dataset = %ctx.dataset_id,
        name = %name,
        added = outcome.added,
        already_present = outcome.already_present,
        "eval cases appended"
    );
    Ok(Json(outcome))
}

/// Runs one IR document for the caller; a document the query engine
/// rejects (e.g. a filter naming an unknown or physical field) is a `422`
/// on this endpoint's options.
async fn run_trace_query(
    state: &RouterAppState,
    ctx: &TenantContext,
    document: &common::query_ir::Document,
    now_ns: i64,
) -> Result<(Vec<super::query::ResultColumn>, Vec<Vec<serde_json::Value>>), ApiError> {
    super::query::execute_document_rows(state, ctx, document, now_ns)
        .await
        .map_err(|e| match e.status {
            StatusCode::BAD_REQUEST => invalid(format!("trace query rejected: {}", e.message)),
            _ => e,
        })
}

/// Runs one IR document and decodes its rows, so a "run → rows → decode"
/// query is one call at the use site.
async fn run_and_decode<T>(
    state: &RouterAppState,
    ctx: &TenantContext,
    document: &common::query_ir::Document,
    now_ns: i64,
    decode: impl FnOnce(&[Row<'_>]) -> T,
) -> Result<T, ApiError> {
    let (columns, rows) = run_trace_query(state, ctx, document, now_ns).await?;
    Ok(decode(&Row::all(&columns, &rows)))
}

#[utoipa::path(
    post,
    path = "/api/v1/eval-sets/{name}/cases/from-traces",
    tag = "eval-sets",
    operation_id = "append_eval_cases_from_traces",
    summary = "Append one case per matching agent trace not already in the set",
    description = "Runs a trace query through the Query IR and appends one case per matching \
agent trace the set does not hold yet, newest traces first, up to `sample` (default 50, at most \
1000). See docs/users/eval-sets.md (\"Build cases from traces\") for the full matching, \
selection and case-building rules.",
    params(("name" = String, Path, description = "Eval set name")),
    request_body = AppendCasesFromTracesRequest,
    responses(
        (status = 200, description = "Matching, already-present and added counts, with the new case ids", body = AppendCasesFromTracesOutcome),
        (status = 400, description = "Malformed JSON body", body = ApiErrorBody),
        (status = 403, description = "Missing evals:write scope (or, for a session, the tenant-admin role), or missing traces:read (logs:read with failing_evaluator)", body = ApiErrorBody),
        (status = 404, description = "No such eval set in the caller's dataset", body = ApiErrorBody),
        (status = 422, description = "Invalid options or range, a filter the query engine rejects, or the set would exceed its case limit", body = ApiErrorBody),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 500, description = "Internal error", body = ApiErrorBody),
        (status = 503, description = "No querier service available", body = ApiErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn append_eval_cases_from_traces(
    State(state): State<RouterAppState>,
    EvalsWrite(ctx): EvalsWrite,
    Path(name): Path<String>,
    ApiJson(request): ApiJson<AppendCasesFromTracesRequest>,
) -> Result<Json<AppendCasesFromTracesOutcome>, ApiError> {
    let now = super::now_ns();
    let window = super::query::resolve_window(&request.range, now)
        .map_err(|e| invalid(format!("`range`: {}", e.message)))?;
    if window.start_ns >= window.end_ns {
        return Err(invalid("`range.from` must be before `range.to`"));
    }
    let mut options = Options::new(request)?;
    super::query::source_read_scope(&ctx, "traces")?;
    if options.failing_evaluator.is_some() {
        super::query::source_read_scope(&ctx, "logs")?;
    }
    let record = state
        .catalog()
        .get_eval_set(&ctx.tenant_id, &ctx.dataset_id, &name)
        .await?
        .ok_or_else(|| StoreError::NotFound(name.clone()))?;
    options
        .agent
        .get_or_insert_with(|| record.summary.agent.clone());
    let present: std::collections::HashSet<String> = record
        .cases
        .iter()
        .filter_map(|c| match &c.source {
            EvalCaseSource::Trace { trace_id } => Some(trace_id.clone()),
            _ => None,
        })
        .collect();
    let (start, end) = (window.start_ns, window.end_ns);

    // Independent reads: run them concurrently rather than one after the
    // other.
    let matches_document = from_traces::matches_document(&options, start, end);
    let matches_fut = run_and_decode(
        &state,
        &ctx,
        &matches_document,
        now,
        from_traces::decode_candidates,
    );
    let failing_fut = async {
        match &options.failing_evaluator {
            Some(evaluator) => {
                let document = from_traces::results_document(evaluator, start, end);
                let failing =
                    run_and_decode(&state, &ctx, &document, now, from_traces::failing_traces)
                        .await?;
                Ok(Some(failing))
            }
            None => Ok(None),
        }
    };
    let (candidates, failing) = tokio::try_join!(matches_fut, failing_fut)?;
    let selection = from_traces::select(&candidates, failing.as_ref(), &present, options.sample);
    if record.cases.len() + selection.trace_ids.len() > MAX_CASES_PER_SET {
        return Err(invalid(format!(
            "adding {} cases to {} would exceed the limit of {MAX_CASES_PER_SET} per set",
            selection.trace_ids.len(),
            record.cases.len()
        )));
    }

    let mut outcome = AppendCasesFromTracesOutcome {
        matches: selection.matches,
        already_present: selection.already_present,
        added: 0,
        added_ids: Vec::new(),
    };
    if !selection.trace_ids.is_empty() {
        let document = from_traces::spans_document(&options, &selection.trace_ids, start, end);
        let cases = run_and_decode(&state, &ctx, &document, now, |rows| {
            from_traces::build_cases(rows, &selection.trace_ids, &options)
        })
        .await?;
        let appended = state
            .catalog()
            .append_eval_cases(&ctx.tenant_id, &ctx.dataset_id, &name, cases)
            .await?;
        // A derived id the set already holds under another source.
        outcome.already_present += appended.already_present;
        outcome.added = appended.added;
        outcome.added_ids = appended.added_ids;
    }
    tracing::info!(
        tenant_id = %ctx.tenant_id,
        dataset = %ctx.dataset_id,
        name = %name,
        matches = outcome.matches,
        added = outcome.added,
        already_present = outcome.already_present,
        failing_evaluator = options.failing_evaluator.is_some(),
        "eval cases appended from traces"
    );
    Ok(Json(outcome))
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use common::auth::Authenticator;
    use common::catalog::{Catalog, MembershipRole};
    use common::config::{ApiKeyConfig, AuthConfig, Configuration, DatasetConfig, TenantConfig};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use crate::{RouterAppState, create_router};

    fn dataset(id: &str, is_default: bool) -> DatasetConfig {
        DatasetConfig {
            id: id.to_string(),
            slug: id.to_string(),
            is_default,
            storage: None,
        }
    }

    fn tenant(id: &str, key: &str) -> TenantConfig {
        TenantConfig {
            id: id.to_string(),
            slug: id.to_string(),
            name: format!("{id} Inc"),
            default_dataset: Some("production".to_string()),
            datasets: vec![dataset("production", true), dataset("staging", false)],
            api_keys: vec![ApiKeyConfig {
                key: key.to_string(),
                name: Some("legacy".to_string()),
            }],
            schema_config: None,
            limits: None,
        }
    }

    /// App with two tenants. `acme-key`/`globex-key` are legacy unscoped
    /// keys; scoped keys `sk-read` (evals:read), `sk-write` (evals:read +
    /// evals:write), `sk-other` (processors:read + processors:write) and
    /// `sk-traces` (evals read/write + traces:read + logs:read) belong to
    /// acme; sessions: alice (Admin), vera (Viewer).
    async fn app() -> axum::Router {
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
            ("sk-read", vec!["evals:read"]),
            ("sk-write", vec!["evals:read", "evals:write"]),
            ("sk-other", vec!["processors:read", "processors:write"]),
            (
                "sk-traces",
                vec!["evals:read", "evals:write", "traces:read", "logs:read"],
            ),
        ] {
            let scopes: Vec<String> = scopes.into_iter().map(String::from).collect();
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
        for (email, role) in [
            ("alice@example.com", MembershipRole::Admin),
            ("vera@example.com", MembershipRole::Viewer),
        ] {
            let user = catalog
                .create_user(email, None, Some(&hash), false)
                .await
                .unwrap();
            catalog
                .upsert_tenant_membership(&user.id, "acme", role)
                .await
                .unwrap();
        }
        create_router(RouterAppState::new(catalog.clone(), config))
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

    #[derive(Clone, Copy)]
    enum Auth<'a> {
        /// API key, tenant, and an optional `X-Dataset-ID`.
        Key(&'a str, &'a str, Option<&'a str>),
        Cookie(&'a str),
    }

    struct Reply {
        status: u16,
        location: Option<String>,
        body: Value,
    }

    async fn send(
        app: &axum::Router,
        auth: Auth<'_>,
        method: &str,
        uri: &str,
        body: Option<Body>,
    ) -> Reply {
        let mut req = Request::builder().method(method).uri(uri);
        match auth {
            Auth::Key(key, tenant, dataset) => {
                req = req
                    .header("authorization", format!("Bearer {key}"))
                    .header("x-tenant-id", tenant);
                if let Some(dataset) = dataset {
                    req = req.header("x-dataset-id", dataset);
                }
            }
            Auth::Cookie(cookie) => {
                req = req.header("cookie", cookie).header("x-tenant-id", "acme");
            }
        }
        let body = match body {
            Some(b) => {
                req = req.header("content-type", "application/json");
                b
            }
            None => Body::empty(),
        };
        let res = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
        let status = res.status().as_u16();
        let location = res
            .headers()
            .get("location")
            .map(|v| v.to_str().unwrap().to_string());
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or(Value::String(String::from_utf8_lossy(&bytes).to_string()))
        };
        Reply {
            status,
            location,
            body,
        }
    }

    async fn call(
        app: &axum::Router,
        auth: Auth<'_>,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> Reply {
        send(
            app,
            auth,
            method,
            uri,
            body.map(|v| Body::from(v.to_string())),
        )
        .await
    }

    fn case(id: &str) -> Value {
        json!({
            "id": id,
            "input": format!("customer asks about {id}"),
            "expected_tools": ["lookup_order", "issue_refund"],
            "reference": "refund issued",
            "tags": ["refunds"],
            "source": {"kind": "trace", "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"},
        })
    }

    fn set(name: &str, ids: &[&str]) -> Value {
        json!({
            "name": name,
            "agent": "support-triage",
            "description": "refund edge cases",
            "cases": ids.iter().map(|id| case(id)).collect::<Vec<_>>(),
        })
    }

    /// Creates `name` with cases `ids` and asserts `201`.
    async fn create(app: &axum::Router, auth: Auth<'_>, name: &str, ids: &[&str]) -> Reply {
        let r = call(app, auth, "POST", "/api/v1/eval-sets", Some(set(name, ids))).await;
        assert_eq!(r.status, 201, "create {name}: {}", r.body);
        r
    }

    fn case_ids(body: &Value) -> Vec<String> {
        body["cases"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["id"].as_str().unwrap().to_string())
            .collect()
    }

    const WRITER: Auth<'static> = Auth::Key("sk-write", "acme", None);
    const READER: Auth<'static> = Auth::Key("sk-read", "acme", None);

    #[tokio::test]
    async fn crud_lifecycle_with_location_conflict_and_not_found() {
        let app = app().await;

        let r = create(&app, WRITER, "refund-edge-cases-40", &["edge-2", "edge-1"]).await;
        assert_eq!(
            r.location.as_deref(),
            Some("/api/v1/eval-sets/refund-edge-cases-40")
        );
        assert_eq!(r.body["tenant_id"], "acme");
        assert_eq!(r.body["dataset"], "production");
        assert_eq!(r.body["case_count"], 2);
        assert_eq!(case_ids(&r.body), vec!["edge-2", "edge-1"]);
        assert_eq!(r.body["cases"][0], case("edge-2"));
        assert!(r.body["created_at"].as_str().unwrap().ends_with('Z'));
        assert_eq!(
            r.body["_links"]["self"]["href"],
            "/api/v1/eval-sets/refund-edge-cases-40"
        );
        assert_eq!(r.body["_links"]["delete"]["method"], "DELETE");

        let r = call(
            &app,
            WRITER,
            "POST",
            "/api/v1/eval-sets",
            Some(set("refund-edge-cases-40", &[])),
        )
        .await;
        assert_eq!(r.status, 409, "{}", r.body);
        assert_eq!(r.body["status"], "error");
        assert_eq!(r.body["errorType"], "conflict");

        let r = call(&app, READER, "GET", "/api/v1/eval-sets", None).await;
        assert_eq!(r.status, 200, "{}", r.body);
        let items = r.body["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["name"], "refund-edge-cases-40");
        assert_eq!(items[0]["agent"], "support-triage");
        assert_eq!(items[0]["case_count"], 2);
        assert!(items[0].get("cases").is_none(), "list omits cases");
        assert!(
            items[0]["_links"].get("delete").is_none(),
            "a read-only caller gets no mutation links"
        );
        assert_eq!(r.body["_links"]["self"]["href"], "/api/v1/eval-sets");

        let r = call(
            &app,
            READER,
            "GET",
            "/api/v1/eval-sets/refund-edge-cases-40",
            None,
        )
        .await;
        assert_eq!(r.status, 200, "{}", r.body);
        assert_eq!(case_ids(&r.body), vec!["edge-2", "edge-1"]);

        let r = call(&app, READER, "GET", "/api/v1/eval-sets/nope", None).await;
        assert_eq!(r.status, 404);
        assert_eq!(r.body["errorType"], "not_found");

        let mut replacement = set("refund-edge-cases-40", &["edge-9"]);
        replacement["agent"] = json!("support-triage-v2");
        let r = call(
            &app,
            WRITER,
            "PUT",
            "/api/v1/eval-sets/refund-edge-cases-40",
            Some(replacement),
        )
        .await;
        assert_eq!(r.status, 200, "{}", r.body);
        assert_eq!(r.body["agent"], "support-triage-v2");
        assert_eq!(case_ids(&r.body), vec!["edge-9"]);

        let r = call(
            &app,
            WRITER,
            "PUT",
            "/api/v1/eval-sets/absent",
            Some(set("absent", &[])),
        )
        .await;
        assert_eq!(r.status, 404, "PUT never creates: {}", r.body);

        let r = call(
            &app,
            WRITER,
            "DELETE",
            "/api/v1/eval-sets/refund-edge-cases-40",
            None,
        )
        .await;
        assert_eq!(r.status, 204);
        assert_eq!(r.body, Value::Null);

        let r = call(
            &app,
            READER,
            "GET",
            "/api/v1/eval-sets/refund-edge-cases-40",
            None,
        )
        .await;
        assert_eq!(r.status, 404);
        let r = call(
            &app,
            WRITER,
            "DELETE",
            "/api/v1/eval-sets/refund-edge-cases-40",
            None,
        )
        .await;
        assert_eq!(r.status, 404);
    }

    #[tokio::test]
    async fn append_reports_added_and_already_present_cases() {
        let app = app().await;
        create(&app, WRITER, "golden", &["edge-39", "edge-40"]).await;

        let r = call(
            &app,
            WRITER,
            "POST",
            "/api/v1/eval-sets/golden/cases",
            Some(json!({"cases": [case("edge-40"), case("edge-41")]})),
        )
        .await;
        assert_eq!(r.status, 200, "{}", r.body);
        assert_eq!(
            r.body,
            json!({
                "added": 1,
                "already_present": 1,
                "added_ids": ["edge-41"],
                "already_present_ids": ["edge-40"],
            })
        );

        let r = call(&app, READER, "GET", "/api/v1/eval-sets/golden", None).await;
        assert_eq!(case_ids(&r.body), vec!["edge-39", "edge-40", "edge-41"]);

        let r = call(
            &app,
            WRITER,
            "POST",
            "/api/v1/eval-sets/missing/cases",
            Some(json!({"cases": [case("a")]})),
        )
        .await;
        assert_eq!(r.status, 404, "{}", r.body);
    }

    #[tokio::test]
    async fn invalid_bodies_are_rejected_with_the_error_envelope() {
        let app = app().await;

        for (body, why) in [
            (set("Bad Name", &[]), "bad name"),
            (set("dup-ids", &["a", "a"]), "duplicate case ids"),
            (
                json!({"name": "no-agent", "agent": "", "cases": []}),
                "empty agent",
            ),
            (
                json!({"name": "bad-trace", "agent": "a", "cases": [
                    {"id": "a", "input": "x", "source": {"kind": "trace", "trace_id": "xyz"}}
                ]}),
                "bad trace id",
            ),
            (json!({"name": "no-agent-field"}), "missing agent"),
            (
                json!({"name": "x", "agent": "a", "cases": "nope"}),
                "wrong type",
            ),
            (
                json!({"name": "x", "agent": "a", "cases": [{"id": "a", "input": "x", "source": {"kind": "carrier-pigeon"}}]}),
                "unknown source kind",
            ),
        ] {
            let r = call(&app, WRITER, "POST", "/api/v1/eval-sets", Some(body)).await;
            assert_eq!(r.status, 422, "{why}: {}", r.body);
            assert_eq!(r.body["status"], "error", "{why}");
            assert_eq!(r.body["errorType"], "invalid", "{why}");
        }

        let r = send(
            &app,
            WRITER,
            "POST",
            "/api/v1/eval-sets",
            Some(Body::from("{not json")),
        )
        .await;
        assert_eq!(r.status, 400, "{}", r.body);
        assert_eq!(r.body["errorType"], "bad_data");

        create(&app, WRITER, "one", &[]).await;
        let r = call(
            &app,
            WRITER,
            "PUT",
            "/api/v1/eval-sets/one",
            Some(set("two", &[])),
        )
        .await;
        assert_eq!(r.status, 422, "body name must match the path: {}", r.body);

        let r = call(
            &app,
            WRITER,
            "POST",
            "/api/v1/eval-sets/one/cases",
            Some(json!({"cases": [case("a"), case("a")]})),
        )
        .await;
        assert_eq!(r.status, 422, "{}", r.body);
    }

    #[tokio::test]
    async fn a_set_of_thousands_of_cases_fits_the_body_limit() {
        let app = app().await;
        let input = "x".repeat(1_000);
        let cases: Vec<Value> = (0..3_000)
            .map(|i| json!({"id": format!("case-{i}"), "input": input}))
            .collect();
        let body = json!({"name": "big", "agent": "a", "cases": cases});
        assert!(
            body.to_string().len() > 2 * 1024 * 1024,
            "beyond axum's default"
        );
        let r = call(&app, WRITER, "POST", "/api/v1/eval-sets", Some(body)).await;
        assert_eq!(r.status, 201, "{}", r.body);
        assert_eq!(r.body["case_count"], 3_000);
        assert_eq!(r.body["cases"][2_999]["id"], "case-2999");
    }

    #[tokio::test]
    async fn an_oversized_body_is_413_in_the_error_envelope() {
        let app = app().await;
        let body = Body::from(vec![b' '; super::EVAL_SET_BODY_LIMIT + 1]);
        let r = send(&app, WRITER, "POST", "/api/v1/eval-sets", Some(body)).await;
        assert_eq!(r.status, 413, "{}", r.body);
        assert_eq!(r.body["errorType"], "payload_too_large");
    }

    #[tokio::test]
    async fn read_only_key_is_forbidden_on_every_write_route() {
        let app = app().await;
        create(&app, WRITER, "golden", &["a"]).await;

        for (method, uri, body) in [
            ("POST", "/api/v1/eval-sets", Some(set("other", &[]))),
            ("PUT", "/api/v1/eval-sets/golden", Some(set("golden", &[]))),
            ("DELETE", "/api/v1/eval-sets/golden", None),
            (
                "POST",
                "/api/v1/eval-sets/golden/cases",
                Some(json!({"cases": [case("b")]})),
            ),
            (
                "POST",
                "/api/v1/eval-sets/golden/cases/from-traces",
                Some(from_traces_body()),
            ),
        ] {
            let r = call(&app, READER, method, uri, body).await;
            assert_eq!(r.status, 403, "{method} {uri}: {}", r.body);
            assert_eq!(r.body["errorType"], "forbidden");
        }

        let r = call(&app, READER, "GET", "/api/v1/eval-sets/golden", None).await;
        assert_eq!(r.status, 200, "the set still exists");
        assert_eq!(case_ids(&r.body), vec!["a"]);
    }

    #[tokio::test]
    async fn keys_without_eval_scopes_cannot_read() {
        let app = app().await;
        for uri in ["/api/v1/eval-sets", "/api/v1/eval-sets/golden"] {
            let r = call(&app, Auth::Key("sk-other", "acme", None), "GET", uri, None).await;
            assert_eq!(r.status, 403, "{uri}: {}", r.body);
        }
        let r = call(
            &app,
            Auth::Key("sk-other", "acme", None),
            "POST",
            "/api/v1/eval-sets",
            Some(set("x", &[])),
        )
        .await;
        assert_eq!(r.status, 403);

        // A legacy unscoped key is unrestricted.
        create(&app, Auth::Key("acme-key", "acme", None), "x", &[]).await;
    }

    #[tokio::test]
    async fn viewer_session_reads_but_only_admin_session_writes() {
        let app = app().await;
        let vera = login(&app, "vera@example.com").await;
        let alice = login(&app, "alice@example.com").await;

        let r = call(
            &app,
            Auth::Cookie(&vera),
            "POST",
            "/api/v1/eval-sets",
            Some(set("golden", &["a"])),
        )
        .await;
        assert_eq!(r.status, 403, "{}", r.body);

        create(&app, Auth::Cookie(&alice), "golden", &["a"]).await;

        for (method, uri, body) in [
            ("PUT", "/api/v1/eval-sets/golden", Some(set("golden", &[]))),
            (
                "POST",
                "/api/v1/eval-sets/golden/cases",
                Some(json!({"cases": [case("b")]})),
            ),
            ("DELETE", "/api/v1/eval-sets/golden", None),
        ] {
            let r = call(&app, Auth::Cookie(&vera), method, uri, body).await;
            assert_eq!(r.status, 403, "viewer {method} {uri}: {}", r.body);
        }

        let r = call(
            &app,
            Auth::Cookie(&vera),
            "GET",
            "/api/v1/eval-sets/golden",
            None,
        )
        .await;
        assert_eq!(r.status, 200, "{}", r.body);
        assert!(r.body["_links"].get("replace").is_none());

        let r = call(
            &app,
            Auth::Cookie(&alice),
            "POST",
            "/api/v1/eval-sets/golden/cases",
            Some(json!({"cases": [case("b")]})),
        )
        .await;
        assert_eq!(r.status, 200, "{}", r.body);
        let r = call(
            &app,
            Auth::Cookie(&alice),
            "PUT",
            "/api/v1/eval-sets/golden",
            Some(set("golden", &[])),
        )
        .await;
        assert_eq!(r.status, 200, "{}", r.body);
        let r = call(
            &app,
            Auth::Cookie(&alice),
            "DELETE",
            "/api/v1/eval-sets/golden",
            None,
        )
        .await;
        assert_eq!(r.status, 204);
    }

    fn from_traces_body() -> Value {
        json!({
            "range": {"from": "now-7d", "to": "now"},
            "failing_evaluator": "Correctness",
            "sample": 50,
            "expected_tools": true,
        })
    }

    const TRACES: Auth<'static> = Auth::Key("sk-traces", "acme", None);
    const FROM_TRACES: &str = "/api/v1/eval-sets/golden/cases/from-traces";

    #[tokio::test]
    async fn from_traces_needs_an_existing_set_and_trace_read_access() {
        let app = app().await;

        let r = call(
            &app,
            TRACES,
            "POST",
            "/api/v1/eval-sets/missing/cases/from-traces",
            Some(from_traces_body()),
        )
        .await;
        assert_eq!(r.status, 404, "{}", r.body);
        assert_eq!(r.body["errorType"], "not_found");

        create(&app, WRITER, "golden", &["a"]).await;
        let r = call(&app, WRITER, "POST", FROM_TRACES, Some(from_traces_body())).await;
        assert_eq!(
            r.status, 403,
            "evals:write alone cannot read traces: {}",
            r.body
        );
        assert!(
            r.body["error"].as_str().unwrap().contains("traces:read"),
            "{}",
            r.body
        );

        let set = call(&app, READER, "GET", "/api/v1/eval-sets/golden", None).await;
        assert_eq!(set.body["_links"].get("append_cases_from_traces"), None);
        let set = call(&app, WRITER, "GET", "/api/v1/eval-sets/golden", None).await;
        assert_eq!(
            set.body["_links"]["append_cases_from_traces"],
            json!({"href": FROM_TRACES, "method": "POST"})
        );

        // Every check passes; with no querier registered the trace query
        // itself is what fails.
        let r = call(&app, TRACES, "POST", FROM_TRACES, Some(from_traces_body())).await;
        assert_eq!(r.status, 503, "{}", r.body);
        let r = call(&app, READER, "GET", "/api/v1/eval-sets/golden", None).await;
        assert_eq!(case_ids(&r.body), vec!["a"], "nothing appended");
    }

    #[tokio::test]
    async fn from_traces_rejects_invalid_options_with_422() {
        let app = app().await;
        create(&app, WRITER, "golden", &[]).await;

        let with = |extra: Value| {
            let mut body = json!({"range": {"from": "now-7d", "to": "now"}});
            body.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            body
        };
        for (body, why) in [
            (with(json!({"sample": 0})), "sample 0"),
            (with(json!({"sample": 1001})), "sample over 1000"),
            (with(json!({"agent": ""})), "empty agent"),
            (with(json!({"failing_evaluator": " "})), "blank evaluator"),
            (with(json!({"failing_evaluater": "x"})), "unknown option"),
            (
                with(json!({"filters": [{"field": "a", "op": "near", "value": 1}]})),
                "unknown predicate op",
            ),
            (
                json!({"range": {"from": "yesterday", "to": "now"}}),
                "unparseable range",
            ),
            (
                json!({"range": {"from": "now", "to": "now-7d"}}),
                "inverted range",
            ),
            (json!({"sample": 5}), "missing range"),
        ] {
            let r = call(&app, TRACES, "POST", FROM_TRACES, Some(body)).await;
            assert_eq!(r.status, 422, "{why}: {}", r.body);
            assert_eq!(r.body["errorType"], "invalid", "{why}");
        }
    }

    #[tokio::test]
    async fn another_tenant_sees_404() {
        let app = app().await;
        create(&app, WRITER, "triage-golden-200", &["a"]).await;

        let globex = Auth::Key("globex-key", "globex", None);
        let r = call(
            &app,
            globex,
            "GET",
            "/api/v1/eval-sets/triage-golden-200",
            None,
        )
        .await;
        assert_eq!(r.status, 404, "{}", r.body);
        let r = call(&app, globex, "GET", "/api/v1/eval-sets", None).await;
        assert_eq!(r.body["items"], json!([]));
        let r = call(
            &app,
            globex,
            "DELETE",
            "/api/v1/eval-sets/triage-golden-200",
            None,
        )
        .await;
        assert_eq!(r.status, 404);
        let r = call(
            &app,
            globex,
            "POST",
            "/api/v1/eval-sets/triage-golden-200/cases",
            Some(json!({"cases": [case("b")]})),
        )
        .await;
        assert_eq!(r.status, 404);

        let r = call(
            &app,
            READER,
            "GET",
            "/api/v1/eval-sets/triage-golden-200",
            None,
        )
        .await;
        assert_eq!(case_ids(&r.body), vec!["a"], "owner's set untouched");
    }

    #[tokio::test]
    async fn another_dataset_sees_404() {
        let app = app().await;
        create(&app, WRITER, "golden", &["a"]).await;

        let staging = Auth::Key("sk-write", "acme", Some("staging"));
        let r = call(&app, staging, "GET", "/api/v1/eval-sets/golden", None).await;
        assert_eq!(r.status, 404, "{}", r.body);
        let r = call(&app, staging, "GET", "/api/v1/eval-sets", None).await;
        assert_eq!(r.body["items"], json!([]));

        let r = create(&app, staging, "golden", &["s"]).await;
        assert_eq!(r.body["dataset"], "staging");

        let r = call(&app, READER, "GET", "/api/v1/eval-sets/golden", None).await;
        assert_eq!(case_ids(&r.body), vec!["a"]);
    }
}
