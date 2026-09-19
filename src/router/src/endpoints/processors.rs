//! # Tenant OTTL processors API (`/api/v1/processors/*`)
//!
//! CRUD over per-tenant OTTL processors (change: tenant-ottl-processors,
//! design D3/D6/D7), plus `:validate` (compile-only, never stored) and
//! `:test` (apply against an inline OTLP payload, never touches the WAL or
//! catalog). Reads require `processors:read`; mutations and `:validate`
//! require `processors:write` (tenant-admin sessions, or a key scoped
//! accordingly — see [`common::auth::TenantContext::can_read_processors`] /
//! [`can_write_processors`](common::auth::TenantContext::can_write_processors)).

use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use common::auth::TenantContext;
use common::processors::{ProcessorRecord, ProcessorSpec, StoreError};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use serde::{Deserialize, Serialize};

use crate::RouterState;

/// Absolute routes (merged directly under `/api/v1`, like `endpoints::tenant`)
/// rather than nested under a `/processors` prefix, so `/processors:validate`
/// and `/processors:test` stay one path segment instead of gaining a
/// spurious leading slash from nesting at the bare resource root.
pub fn router<S: RouterState>() -> Router<S> {
    Router::new()
        .route(
            "/processors",
            get(list_processors::<S>).post(create_processor::<S>),
        )
        .route("/processors:validate", post(validate_processor::<S>))
        .route("/processors:test", post(test_processor::<S>))
        .route(
            "/processors/{name}",
            get(get_processor::<S>)
                .put(replace_processor::<S>)
                .delete(delete_processor::<S>),
        )
}

// ---- DTOs -------------------------------------------------------------

/// Error body for the processors API.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ProcessorError {
    pub error: String,
    /// Positional compile errors (422 only).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<StatementError>,
}

/// One compile error, positioned to a statement and (where known) a column.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct StatementError {
    pub statement: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<usize>,
    pub message: String,
}

impl From<ottl::CompileError> for StatementError {
    fn from(e: ottl::CompileError) -> Self {
        StatementError {
            statement: e.statement,
            column: e.column,
            message: e.message,
        }
    }
}

/// A processor row plus its compiled status.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ProcessorResponse {
    #[serde(flatten)]
    pub record: ProcessorRecord,
    /// `"invalid"` when the stored statements currently fail to compile
    /// (skipped at apply time, never blocking ingest); `"ok"` otherwise.
    pub status: &'static str,
}

fn processor_status(record: &ProcessorRecord, limits: &ottl::Limits) -> &'static str {
    let signal = match record.signal.as_str() {
        "traces" => ottl::Signal::Traces,
        "logs" => ottl::Signal::Logs,
        "metrics" => ottl::Signal::Metrics,
        _ => return "invalid",
    };
    if ottl::compile(signal, &record.statements, limits).is_ok() {
        "ok"
    } else {
        "invalid"
    }
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ProcessorListResponse {
    pub processors: Vec<ProcessorResponse>,
}

/// Every write response carries the cross-process propagation bound for the
/// change: the `ProcessorRegistry` cache TTL, in seconds.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ProcessorWriteResponse {
    #[serde(flatten)]
    pub record: ProcessorRecord,
    pub status: &'static str,
    pub applies_within_seconds: u64,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ValidateRequest {
    pub signal: String,
    pub statements: Vec<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ValidateResponse {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<StatementError>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct TestRequest {
    pub signal: String,
    #[serde(default)]
    pub dataset: Option<String>,
    /// Processors to apply, in the given order; when omitted, the tenant's
    /// stored processors for `signal`/`dataset` are used instead.
    #[serde(default)]
    pub processors: Option<Vec<ProcessorSpec>>,
    /// The OTLP export request (OTLP/JSON), for `signal`.
    pub payload: serde_json::Value,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct TestResponse {
    pub payload: serde_json::Value,
    pub statements: Vec<TestStatementResult>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct TestStatementResult {
    pub index: usize,
    pub matched: u64,
    pub errors: u64,
}

// ---- helpers ------------------------------------------------------------

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(ProcessorError {
            error: message.into(),
            errors: Vec::new(),
        }),
    )
        .into_response()
}

fn compile_errors(
    status: StatusCode,
    message: impl Into<String>,
    errs: Vec<ottl::CompileError>,
) -> Response {
    (
        status,
        Json(ProcessorError {
            error: message.into(),
            errors: errs.into_iter().map(StatementError::from).collect(),
        }),
    )
        .into_response()
}

fn store_error(err: StoreError) -> Response {
    match err {
        StoreError::NotFound(name) => error(
            StatusCode::NOT_FOUND,
            format!("processor `{name}` not found"),
        ),
        StoreError::Conflict(name) => error(
            StatusCode::CONFLICT,
            format!("processor `{name}` already exists"),
        ),
        StoreError::UnknownDataset(_) | StoreError::Invalid(_) => {
            error(StatusCode::UNPROCESSABLE_ENTITY, err.to_string())
        }
        StoreError::Database(_) => {
            tracing::error!(error = %err, "processor store failure");
            error(StatusCode::INTERNAL_SERVER_ERROR, "processor store failure")
        }
    }
}

fn require_read(ctx: &TenantContext) -> Result<(), Box<Response>> {
    if ctx.can_read_processors() {
        Ok(())
    } else {
        Err(Box::new(error(
            StatusCode::FORBIDDEN,
            "missing processors:read scope",
        )))
    }
}

fn require_write(ctx: &TenantContext) -> Result<(), Box<Response>> {
    if ctx.can_write_processors() {
        Ok(())
    } else {
        Err(Box::new(error(
            StatusCode::FORBIDDEN,
            "processors:write scope (and tenant admin role for sessions) required",
        )))
    }
}

fn limits_for<S: RouterState>(state: &S) -> ottl::Limits {
    let cfg = &state.config().processors;
    ottl::Limits {
        max_statements: cfg.max_statements,
        max_regex_len: cfg.max_regex_len,
        ..ottl::Limits::default()
    }
}

/// Compiles `spec.statements` for `spec.signal` and rejects with 422 +
/// positional errors on failure — the spec's "OTTL subset is validated at
/// write time" requirement: create/replace/`:validate` all reject an
/// uncompilable program up front. A stored row's `status` can therefore only
/// go `invalid` later, if a `Limits`/config change makes it stop compiling
/// (design D5) — never at the moment it was written.
fn compile_check<S: RouterState>(state: &S, spec: &ProcessorSpec) -> Result<(), Box<Response>> {
    let signal = signal_of(&spec.signal)?;
    let limits = limits_for(state);
    match ottl::compile(signal, &spec.statements, &limits) {
        Ok(_) => Ok(()),
        Err(errs) => Err(Box::new(compile_errors(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("processor `{}` failed to compile", spec.name),
            errs,
        ))),
    }
}

fn signal_of(signal: &str) -> Result<ottl::Signal, Box<Response>> {
    match signal {
        "traces" => Ok(ottl::Signal::Traces),
        "logs" => Ok(ottl::Signal::Logs),
        "metrics" => Ok(ottl::Signal::Metrics),
        other => Err(Box::new(error(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("signal `{other}` must be one of traces, logs, metrics"),
        ))),
    }
}

// ---- CRUD -----------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/processors",
    tag = "processors",
    operation_id = "processors_list",
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "This tenant's processors", body = ProcessorListResponse),
        (status = 403, description = "Missing processors:read scope", body = ProcessorError),
    ),
    security(("bearer" = []))
)]
pub async fn list_processors<S: RouterState>(
    State(state): State<S>,
    Extension(ctx): Extension<TenantContext>,
) -> Response {
    if let Err(r) = require_read(&ctx) {
        return *r;
    }
    let limits = limits_for(&state);
    match state.catalog().list_processors(&ctx.tenant_id).await {
        Ok(records) => Json(ProcessorListResponse {
            processors: records
                .into_iter()
                .map(|record| {
                    let status = processor_status(&record, &limits);
                    ProcessorResponse { record, status }
                })
                .collect(),
        })
        .into_response(),
        Err(e) => store_error(e),
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/processors",
    tag = "processors",
    operation_id = "processors_create",
    request_body = ProcessorSpec,
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 201, description = "Processor created", body = ProcessorWriteResponse),
        (status = 400, description = "Unparseable body", body = ProcessorError),
        (status = 403, description = "Missing processors:write scope", body = ProcessorError),
        (status = 409, description = "Processor already exists", body = ProcessorError),
        (status = 422, description = "Invalid spec or unknown dataset", body = ProcessorError),
    ),
    security(("bearer" = []))
)]
pub async fn create_processor<S: RouterState>(
    State(state): State<S>,
    Extension(ctx): Extension<TenantContext>,
    body: Bytes,
) -> Response {
    if let Err(r) = require_write(&ctx) {
        return *r;
    }
    let spec: ProcessorSpec = match serde_json::from_slice(&body) {
        Ok(spec) => spec,
        Err(e) => {
            return error(
                StatusCode::BAD_REQUEST,
                format!("invalid processor spec: {e}"),
            );
        }
    };
    if let Err(r) = compile_check(&state, &spec) {
        return *r;
    }
    match state
        .catalog()
        .insert_processor(&ctx.tenant_id, &spec)
        .await
    {
        Ok(record) => {
            state.processor_registry().invalidate(&ctx.tenant_id);
            tracing::info!(tenant_id = %ctx.tenant_id, name = %record.name, "processor created");
            (StatusCode::CREATED, Json(write_response(&state, record))).into_response()
        }
        Err(e) => store_error(e),
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/processors:validate",
    tag = "processors",
    operation_id = "processors_validate",
    request_body = ValidateRequest,
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Validation outcome; nothing is stored", body = ValidateResponse),
        (status = 400, description = "Unparseable body", body = ProcessorError),
        (status = 403, description = "Missing processors:read scope", body = ProcessorError),
    ),
    security(("bearer" = []))
)]
pub async fn validate_processor<S: RouterState>(
    State(state): State<S>,
    Extension(ctx): Extension<TenantContext>,
    body: Bytes,
) -> Response {
    if let Err(r) = require_read(&ctx) {
        return *r;
    }
    let req: ValidateRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return error(StatusCode::BAD_REQUEST, format!("invalid request: {e}")),
    };
    let signal = match signal_of(&req.signal) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    let limits = limits_for(&state);
    match ottl::compile(signal, &req.statements, &limits) {
        Ok(_) => Json(ValidateResponse { errors: Vec::new() }).into_response(),
        Err(errs) => Json(ValidateResponse {
            errors: errs.into_iter().map(StatementError::from).collect(),
        })
        .into_response(),
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/processors/{name}",
    tag = "processors",
    operation_id = "processors_get",
    params(("name" = String, Path, description = "Processor name")),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "The processor", body = ProcessorResponse),
        (status = 403, description = "Missing processors:read scope", body = ProcessorError),
        (status = 404, description = "No such processor", body = ProcessorError),
    ),
    security(("bearer" = []))
)]
pub async fn get_processor<S: RouterState>(
    State(state): State<S>,
    Extension(ctx): Extension<TenantContext>,
    Path(name): Path<String>,
) -> Response {
    if let Err(r) = require_read(&ctx) {
        return *r;
    }
    match state.catalog().get_processor(&ctx.tenant_id, &name).await {
        Ok(Some(record)) => {
            let status = processor_status(&record, &limits_for(&state));
            Json(ProcessorResponse { record, status }).into_response()
        }
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            format!("processor `{name}` not found"),
        ),
        Err(e) => store_error(e),
    }
}

#[utoipa::path(
    put,
    path = "/api/v1/processors/{name}",
    tag = "processors",
    operation_id = "processors_replace",
    params(("name" = String, Path, description = "Processor name")),
    request_body = ProcessorSpec,
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "Processor replaced", body = ProcessorWriteResponse),
        (status = 400, description = "Unparseable body", body = ProcessorError),
        (status = 403, description = "Missing processors:write scope", body = ProcessorError),
        (status = 404, description = "No such processor (PUT never upserts)", body = ProcessorError),
        (status = 422, description = "Invalid spec or unknown dataset", body = ProcessorError),
    ),
    security(("bearer" = []))
)]
pub async fn replace_processor<S: RouterState>(
    State(state): State<S>,
    Extension(ctx): Extension<TenantContext>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(r) = require_write(&ctx) {
        return *r;
    }
    let spec: ProcessorSpec = match serde_json::from_slice(&body) {
        Ok(spec) => spec,
        Err(e) => {
            return error(
                StatusCode::BAD_REQUEST,
                format!("invalid processor spec: {e}"),
            );
        }
    };
    if let Err(r) = compile_check(&state, &spec) {
        return *r;
    }
    match state
        .catalog()
        .replace_processor(&ctx.tenant_id, &name, &spec)
        .await
    {
        Ok(record) => {
            state.processor_registry().invalidate(&ctx.tenant_id);
            tracing::info!(tenant_id = %ctx.tenant_id, name = %record.name, "processor replaced");
            Json(write_response(&state, record)).into_response()
        }
        Err(e) => store_error(e),
    }
}

#[utoipa::path(
    delete,
    path = "/api/v1/processors/{name}",
    tag = "processors",
    operation_id = "processors_delete",
    params(("name" = String, Path, description = "Processor name")),
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 204, description = "Processor deleted"),
        (status = 403, description = "Missing processors:write scope", body = ProcessorError),
        (status = 404, description = "No such processor", body = ProcessorError),
    ),
    security(("bearer" = []))
)]
pub async fn delete_processor<S: RouterState>(
    State(state): State<S>,
    Extension(ctx): Extension<TenantContext>,
    Path(name): Path<String>,
) -> Response {
    if let Err(r) = require_write(&ctx) {
        return *r;
    }
    match state
        .catalog()
        .delete_processor(&ctx.tenant_id, &name)
        .await
    {
        Ok(true) => {
            state.processor_registry().invalidate(&ctx.tenant_id);
            tracing::info!(tenant_id = %ctx.tenant_id, name = %name, "processor deleted");
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => error(
            StatusCode::NOT_FOUND,
            format!("processor `{name}` not found"),
        ),
        Err(e) => store_error(e),
    }
}

fn write_response<S: RouterState>(state: &S, record: ProcessorRecord) -> ProcessorWriteResponse {
    let status = processor_status(&record, &limits_for(state));
    ProcessorWriteResponse {
        record,
        status,
        applies_within_seconds: state.config().processors.reload_interval.as_secs(),
    }
}

// ---- :test ------------------------------------------------------------

#[utoipa::path(
    post,
    path = "/api/v1/processors:test",
    tag = "processors",
    operation_id = "processors_test",
    request_body = TestRequest,
    responses(
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 200, description = "The transformed payload and per-statement counts", body = TestResponse),
        (status = 400, description = "Unparseable body or payload", body = ProcessorError),
        (status = 403, description = "Missing processors:read scope", body = ProcessorError),
        (status = 413, description = "Payload exceeds processors.test_payload_max_bytes", body = ProcessorError),
        (status = 422, description = "Inline processors failed to compile", body = ProcessorError),
    ),
    security(("bearer" = []))
)]
pub async fn test_processor<S: RouterState>(
    State(state): State<S>,
    Extension(ctx): Extension<TenantContext>,
    body: Bytes,
) -> Response {
    if let Err(r) = require_read(&ctx) {
        return *r;
    }
    let max_bytes = state.config().processors.test_payload_max_bytes;
    if body.len() > max_bytes {
        return error(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("payload exceeds the {max_bytes}-byte test limit"),
        );
    }
    let req: TestRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return error(StatusCode::BAD_REQUEST, format!("invalid request: {e}")),
    };
    let signal = match signal_of(&req.signal) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    let limits = limits_for(&state);

    // Resolve the programs to run: inline specs (compiled fresh, in order)
    // or the tenant's stored processors for this signal/dataset.
    let dataset = req.dataset.as_deref().unwrap_or(&ctx.dataset_id);
    let programs: Vec<(String, ottl::CompiledProgram, ottl::ErrorMode)> = match req.processors {
        Some(specs) => {
            let mut programs = Vec::with_capacity(specs.len());
            for spec in specs {
                if spec.signal != req.signal {
                    return error(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        format!(
                            "processor `{}` signal `{}` does not match test signal `{}`",
                            spec.name, spec.signal, req.signal
                        ),
                    );
                }
                let mode = match spec.error_mode.parse::<ottl::ErrorMode>() {
                    Ok(mode) => mode,
                    Err(e) => {
                        return compile_errors(
                            StatusCode::UNPROCESSABLE_ENTITY,
                            format!("processor `{}`: {e}", spec.name),
                            Vec::new(),
                        );
                    }
                };
                match ottl::compile(signal, &spec.statements, &limits) {
                    Ok(program) => programs.push((spec.name, program, mode)),
                    Err(errs) => {
                        return compile_errors(
                            StatusCode::UNPROCESSABLE_ENTITY,
                            format!("processor `{}` failed to compile", spec.name),
                            errs,
                        );
                    }
                }
            }
            programs
        }
        None => {
            let signal_str = req.signal.as_str();
            let compiled = match state
                .processor_registry()
                .for_request(&ctx.tenant_id, dataset, signal_str)
                .await
            {
                Ok(compiled) => compiled,
                Err(e) => {
                    return error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!(
                            "failed to load processors for tenant `{}`: {e}",
                            ctx.tenant_id
                        ),
                    );
                }
            };
            compiled
                .iter()
                .filter_map(|p| {
                    let program = p.program.clone()?;
                    // Stored rows are validated at write time; an
                    // unparseable value here can only come from drift, so
                    // fail open to `Ignore` rather than reject the request.
                    let mode = p
                        .record
                        .error_mode
                        .parse()
                        .unwrap_or(ottl::ErrorMode::Ignore);
                    Some((p.record.name.clone(), program, mode))
                })
                .collect()
        }
    };

    let mut stats: Vec<TestStatementResult> = Vec::new();
    let payload = match signal {
        ottl::Signal::Traces => {
            let mut req: ExportTraceServiceRequest = match serde_json::from_value(req.payload) {
                Ok(r) => r,
                Err(e) => {
                    return error(
                        StatusCode::BAD_REQUEST,
                        format!("invalid OTLP traces payload: {e}"),
                    );
                }
            };
            for (name, program, mode) in &programs {
                match program.apply_traces(&mut req, *mode) {
                    Ok(report) => push_stats(&mut stats, &report),
                    Err(e) => {
                        return error(
                            StatusCode::UNPROCESSABLE_ENTITY,
                            format!("processor `{name}` failed: {e}"),
                        );
                    }
                }
            }
            match serde_json::to_value(&req) {
                Ok(v) => v,
                Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            }
        }
        ottl::Signal::Logs => {
            let mut req: ExportLogsServiceRequest = match serde_json::from_value(req.payload) {
                Ok(r) => r,
                Err(e) => {
                    return error(
                        StatusCode::BAD_REQUEST,
                        format!("invalid OTLP logs payload: {e}"),
                    );
                }
            };
            for (name, program, mode) in &programs {
                match program.apply_logs(&mut req, *mode) {
                    Ok(report) => push_stats(&mut stats, &report),
                    Err(e) => {
                        return error(
                            StatusCode::UNPROCESSABLE_ENTITY,
                            format!("processor `{name}` failed: {e}"),
                        );
                    }
                }
            }
            match serde_json::to_value(&req) {
                Ok(v) => v,
                Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            }
        }
        ottl::Signal::Metrics => {
            let mut req: ExportMetricsServiceRequest = match serde_json::from_value(req.payload) {
                Ok(r) => r,
                Err(e) => {
                    return error(
                        StatusCode::BAD_REQUEST,
                        format!("invalid OTLP metrics payload: {e}"),
                    );
                }
            };
            for (name, program, mode) in &programs {
                match program.apply_metrics(&mut req, *mode) {
                    Ok(report) => push_stats(&mut stats, &report),
                    Err(e) => {
                        return error(
                            StatusCode::UNPROCESSABLE_ENTITY,
                            format!("processor `{name}` failed: {e}"),
                        );
                    }
                }
            }
            match serde_json::to_value(&req) {
                Ok(v) => v,
                Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            }
        }
    };

    Json(TestResponse {
        payload,
        statements: stats,
    })
    .into_response()
}

/// Accumulates one program's `ApplyReport` into the flat, cross-processor
/// statement result list `:test` returns (each processor's statements are
/// appended in program order).
fn push_stats(out: &mut Vec<TestStatementResult>, report: &ottl::ApplyReport) {
    for (i, s) in report.statements.iter().enumerate() {
        out.push(TestStatementResult {
            index: i,
            matched: s.matched,
            errors: s.errors,
        });
    }
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

    /// The two spec-verbatim example statements (design D1/D2, `docs/users/processors.md`).
    fn redact_url() -> String {
        r#"replace_pattern(attributes["url.full"], "\\?.*$", "")"#.to_string()
    }
    fn hash_email() -> String {
        r#"set(attributes["user.email"], SHA256(attributes["user.email"])) where attributes["user.email"] != nil"#.to_string()
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
    /// keys; scoped keys `sk-read` (processors:read), `sk-write`
    /// (processors:read + processors:write), `sk-ingest` (traces:write)
    /// belong to acme; sessions: alice (Admin), vera (Viewer).
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
            ("sk-read", vec!["processors:read".to_string()]),
            (
                "sk-write",
                vec![
                    "processors:read".to_string(),
                    "processors:write".to_string(),
                ],
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

    enum Auth<'a> {
        Key(&'a str, &'a str),
        Cookie(&'a str),
    }

    async fn call(
        app: &axum::Router,
        auth: Auth<'_>,
        method: &str,
        uri: &str,
        body: Option<Value>,
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
            Some(v) => {
                req = req.header("content-type", "application/json");
                Body::from(v.to_string())
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

    fn spec(name: &str, statements: Vec<String>) -> Value {
        json!({
            "name": name,
            "signal": "traces",
            "statements": statements,
        })
    }

    #[tokio::test]
    async fn crud_lifecycle_with_404_409_and_applies_within_seconds() {
        let app = app().await;

        // create
        let (status, body) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "POST",
            "/api/v1/processors",
            Some(spec("redact", vec![redact_url()])),
        )
        .await;
        assert_eq!(status, 201, "{body}");
        assert_eq!(body["name"], "redact");
        assert_eq!(body["status"], "ok");
        assert!(body["applies_within_seconds"].as_u64().is_some());

        // duplicate -> 409
        let (status, _) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "POST",
            "/api/v1/processors",
            Some(spec("redact", vec![redact_url()])),
        )
        .await;
        assert_eq!(status, 409);

        // list
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/processors",
            None,
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["processors"].as_array().unwrap().len(), 1);

        // get
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/processors/redact",
            None,
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["name"], "redact");

        // get missing -> 404
        let (status, _) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/processors/nope",
            None,
        )
        .await;
        assert_eq!(status, 404);

        // replace
        let (status, body) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "PUT",
            "/api/v1/processors/redact",
            Some(spec("redact", vec![redact_url(), hash_email()])),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["statements"].as_array().unwrap().len(), 2);
        assert!(body["applies_within_seconds"].as_u64().is_some());

        // replace missing -> 404 (never upserts)
        let (status, _) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "PUT",
            "/api/v1/processors/nope",
            Some(spec("nope", vec![redact_url()])),
        )
        .await;
        assert_eq!(status, 404);

        // replace with an uncompilable statement -> 422 with positional
        // errors, rejected up front rather than stored (the OTTL subset is
        // validated at write time; `status: "invalid"` is reserved for a row
        // that compiled when written but stops compiling later, e.g. after a
        // Limits/config change — design D5).
        let (status, body) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "PUT",
            "/api/v1/processors/redact",
            Some(spec("redact", vec!["not a valid statement (".to_string()])),
        )
        .await;
        assert_eq!(status, 422, "{body}");
        let errs = body["errors"].as_array().unwrap();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0]["statement"], 0);

        // unknown dataset reference -> 422
        let (status, _) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "POST",
            "/api/v1/processors",
            Some(json!({
                "name": "scoped",
                "signal": "traces",
                "dataset": "no-such-dataset",
                "statements": [redact_url()],
            })),
        )
        .await;
        assert_eq!(status, 422);

        // delete
        let (status, _) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "DELETE",
            "/api/v1/processors/redact",
            None,
        )
        .await;
        assert_eq!(status, 204);
        let (status, _) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "DELETE",
            "/api/v1/processors/redact",
            None,
        )
        .await;
        assert_eq!(status, 404);
    }

    #[tokio::test]
    async fn create_rejects_unsupported_statement_with_positional_error() {
        let app = app().await;
        let (status, body) = call(
            &app,
            Auth::Key("sk-write", "acme"),
            "POST",
            "/api/v1/processors",
            Some(spec(
                "merge",
                vec![r#"merge_maps(attributes, resource.attributes, "insert")"#.to_string()],
            )),
        )
        .await;
        assert_eq!(status, 422, "{body}");
        assert_eq!(body["error"], "processor `merge` failed to compile");
        let errs = body["errors"].as_array().unwrap();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0]["statement"], 0);

        // nothing was stored
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/processors",
            None,
        )
        .await;
        assert_eq!(status, 200);
        assert!(body["processors"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn scopes_and_roles_enforced() {
        let app = app().await;
        // missing read scope
        let (status, _) = call(
            &app,
            Auth::Key("sk-ingest", "acme"),
            "GET",
            "/api/v1/processors",
            None,
        )
        .await;
        assert_eq!(status, 403);
        // missing write scope
        let (status, _) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "POST",
            "/api/v1/processors",
            Some(spec("redact", vec![redact_url()])),
        )
        .await;
        assert_eq!(status, 403);

        // viewer session may read, not write
        let alice = login(&app, "alice@example.com").await;
        let vera = login(&app, "vera@example.com").await;
        let (status, _) = call(&app, Auth::Cookie(&vera), "GET", "/api/v1/processors", None).await;
        assert_eq!(status, 200);
        let (status, _) = call(
            &app,
            Auth::Cookie(&vera),
            "POST",
            "/api/v1/processors",
            Some(spec("redact", vec![redact_url()])),
        )
        .await;
        assert_eq!(status, 403);
        let (status, body) = call(
            &app,
            Auth::Cookie(&alice),
            "POST",
            "/api/v1/processors",
            Some(spec("redact", vec![redact_url()])),
        )
        .await;
        assert_eq!(status, 201, "{body}");
    }

    #[tokio::test]
    async fn tenant_scoping_isolates_processors() {
        let app = app().await;
        call(
            &app,
            Auth::Key("sk-write", "acme"),
            "POST",
            "/api/v1/processors",
            Some(spec("redact", vec![redact_url()])),
        )
        .await;

        let (status, body) = call(
            &app,
            Auth::Key("globex-key", "globex"),
            "GET",
            "/api/v1/processors",
            None,
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert!(body["processors"].as_array().unwrap().is_empty());

        let (status, _) = call(
            &app,
            Auth::Key("globex-key", "globex"),
            "GET",
            "/api/v1/processors/redact",
            None,
        )
        .await;
        assert_eq!(status, 404, "tenant B cannot see tenant A's processor");

        let (status, _) = call(
            &app,
            Auth::Key("globex-key", "globex"),
            "DELETE",
            "/api/v1/processors/redact",
            None,
        )
        .await;
        assert_eq!(status, 404, "tenant B cannot delete tenant A's processor");
    }

    #[tokio::test]
    async fn validate_reports_positional_errors_and_stores_nothing() {
        let app = app().await;
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "POST",
            "/api/v1/processors:validate",
            Some(json!({"signal": "traces", "statements": [redact_url(), hash_email()]})),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert!(
            body["errors"].as_array().is_none_or(|e| e.is_empty()),
            "{body}"
        );

        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "POST",
            "/api/v1/processors:validate",
            Some(json!({"signal": "traces", "statements": ["not a valid statement ("]})),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        let errs = body["errors"].as_array().unwrap();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0]["statement"], 0);

        // never stores
        let (status, _) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/processors",
            None,
        )
        .await;
        assert_eq!(status, 200);
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/processors",
            None,
        )
        .await;
        assert_eq!(status, 200);
        assert!(body["processors"].as_array().unwrap().is_empty());
    }

    fn traces_payload(email: &str, url: &str) -> Value {
        json!({
            "resourceSpans": [{
                "resource": {"attributes": []},
                "scopeSpans": [{
                    "scope": {},
                    "spans": [{
                        "traceId": "0102030405060708090a0b0c0d0e0f10",
                        "spanId": "0102030405060708",
                        "name": "GET /checkout",
                        "startTimeUnixNano": "1",
                        "endTimeUnixNano": "2",
                        "attributes": [
                            {"key": "user.email", "value": {"stringValue": email}},
                            {"key": "url.full", "value": {"stringValue": url}}
                        ]
                    }]
                }]
            }]
        })
    }

    fn traces_payload_with_int_attr(key: &str, value: i64) -> Value {
        json!({
            "resourceSpans": [{
                "resource": {"attributes": []},
                "scopeSpans": [{
                    "scope": {},
                    "spans": [{
                        "traceId": "0102030405060708090a0b0c0d0e0f10",
                        "spanId": "0102030405060708",
                        "name": "GET /checkout",
                        "startTimeUnixNano": "1",
                        "endTimeUnixNano": "2",
                        "attributes": [
                            {"key": key, "value": {"intValue": value.to_string()}}
                        ]
                    }]
                }]
            }]
        })
    }

    #[tokio::test]
    async fn test_endpoint_runs_inline_processors_and_never_writes() {
        let app = app().await;
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "POST",
            "/api/v1/processors:test",
            Some(json!({
                "signal": "traces",
                "processors": [{
                    "name": "inline",
                    "signal": "traces",
                    "statements": [redact_url(), hash_email()],
                }],
                "payload": traces_payload("alice@example.com", "https://example.com/checkout?token=abc"),
            })),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        let statements = body["statements"].as_array().unwrap();
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0]["matched"], 1);
        assert_eq!(statements[1]["matched"], 1);
        let span = &body["payload"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        let attrs = span["attributes"].as_array().unwrap();
        let url = attrs.iter().find(|a| a["key"] == "url.full").unwrap()["value"]["stringValue"]
            .as_str()
            .unwrap();
        assert_eq!(url, "https://example.com/checkout");
        let email =
            attrs.iter().find(|a| a["key"] == "user.email").unwrap()["value"]["stringValue"]
                .as_str()
                .unwrap();
        assert_ne!(email, "alice@example.com");

        // no processor was stored
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "GET",
            "/api/v1/processors",
            None,
        )
        .await;
        assert_eq!(status, 200);
        assert!(body["processors"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_endpoint_runs_stored_processors_when_omitted() {
        let app = app().await;
        call(
            &app,
            Auth::Key("sk-write", "acme"),
            "POST",
            "/api/v1/processors",
            Some(spec("redact", vec![redact_url()])),
        )
        .await;

        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "POST",
            "/api/v1/processors:test",
            Some(json!({
                "signal": "traces",
                "payload": traces_payload("alice@example.com", "https://example.com/checkout?token=abc"),
            })),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        let statements = body["statements"].as_array().unwrap();
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0]["matched"], 1);
    }

    #[tokio::test]
    async fn test_endpoint_returns_422_when_propagate_mode_errors() {
        let app = app().await;
        // `count` is an int; assigning it to `name` (a string field) requires
        // coercion that fails, and isn't guarded by a `where`, so it errors on
        // every span. With `error_mode: propagate` that error must abort the
        // test and surface as a 422 naming the failing processor, not a 200
        // with partial stats.
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "POST",
            "/api/v1/processors:test",
            Some(json!({
                "signal": "traces",
                "processors": [{
                    "name": "bad-processor",
                    "signal": "traces",
                    "statements": [r#"set(name, attributes["count"])"#],
                    "error_mode": "propagate",
                }],
                "payload": traces_payload_with_int_attr("count", 42),
            })),
        )
        .await;
        assert_eq!(status, 422, "{body}");
        assert!(
            body["error"].as_str().unwrap().contains("bad-processor"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn test_endpoint_rejects_inline_processor_signal_mismatch() {
        let app = app().await;
        let (status, body) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "POST",
            "/api/v1/processors:test",
            Some(json!({
                "signal": "traces",
                "processors": [{
                    "name": "wrong-signal",
                    "signal": "logs",
                    "statements": [redact_url()],
                }],
                "payload": traces_payload("alice@example.com", "https://example.com/checkout?token=abc"),
            })),
        )
        .await;
        assert_eq!(status, 422, "{body}");
        let error = body["error"].as_str().unwrap();
        assert!(error.contains("wrong-signal"), "{body}");
        assert!(error.contains("logs"), "{body}");
        assert!(error.contains("traces"), "{body}");
    }

    #[tokio::test]
    async fn test_endpoint_rejects_oversized_payload() {
        let app = app().await;
        let big_url = "x".repeat(2 * 1024 * 1024);
        let (status, _) = call(
            &app,
            Auth::Key("sk-read", "acme"),
            "POST",
            "/api/v1/processors:test",
            Some(json!({
                "signal": "traces",
                "processors": [],
                "payload": traces_payload("a@example.com", &big_url),
            })),
        )
        .await;
        assert_eq!(status, 413);
    }
}
