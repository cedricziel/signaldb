pub mod cli;
pub mod handler;
pub mod middleware;
pub mod services;

use std::{net::SocketAddr, sync::Arc};

use anyhow::Context;
use axum::{
    Extension, Router,
    routing::{get, post},
};
use opentelemetry_proto::tonic::collector::{
    logs::v1::logs_service_server::LogsServiceServer,
    metrics::v1::metrics_service_server::MetricsServiceServer,
    profiles::v1development::profiles_service_server::ProfilesServiceServer,
    trace::v1::trace_service_server::TraceServiceServer,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::codec::CompressionEncoding;
// Service bootstrap and configuration
use common::config::Configuration;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
// Flight protocol and transport
use common::flight::transport::InMemoryFlightTransport;
// WAL for durability
use common::wal::WalConfig;

use crate::handler::otlp_grpc::TraceHandler;
use crate::handler::otlp_log_handler::LogHandler;
use crate::handler::otlp_metrics_handler::MetricsHandler;
use crate::handler::otlp_profiles_handler::ProfileHandler;
use crate::handler::{PrometheusHandler, PrometheusHandlerState};
use crate::handler::{WalManager, WalRetryConsumer};
use crate::middleware::auth_middleware;
use crate::services::{
    otlp_log_service::LogAcceptorService, otlp_metric_service::MetricsAcceptorService,
    otlp_profile_service::ProfileAcceptorService, otlp_trace_service::TraceAcceptorService,
};
use common::auth::Authenticator;

/// Shared resources for acceptor services (gRPC and HTTP)
#[derive(Clone)]
pub struct AcceptorResources {
    pub flight_transport: Arc<InMemoryFlightTransport>,
    pub wal_manager: Arc<WalManager>,
    pub authenticator: Arc<Authenticator>,
    pub rate_limiter: Arc<common::ratelimit::TenantRateLimiter>,
    pub storage_usage: Arc<common::storage_usage::StorageUsageTracker>,
}

/// Initialize shared resources for acceptor services
pub async fn init_acceptor_resources(
    config: Configuration,
    advertise_addr: String,
    wal_dir: std::path::PathBuf,
) -> Result<AcceptorResources, anyhow::Error> {
    // Keep a copy for the storage usage refresher, which needs the full
    // configuration to open the Iceberg catalog.
    let full_config = config.clone();

    // Initialize service bootstrap for catalog-based discovery
    let service_bootstrap = ServiceBootstrap::new(config, ServiceType::Acceptor, advertise_addr)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize service bootstrap: {e}"))?;

    // Extract catalog and auth config BEFORE moving service_bootstrap into InMemoryFlightTransport
    let catalog = Arc::new(service_bootstrap.catalog().clone());
    let auth_config = service_bootstrap.config().auth.clone();

    // Initialize Flight transport with catalog-aware discovery
    let flight_transport = Arc::new(InMemoryFlightTransport::new(service_bootstrap));

    // Start background connection cleanup
    flight_transport.start_connection_cleanup(std::time::Duration::from_secs(60));

    // Initialize WalManager with separate base configurations per signal
    // type, tuned for that signal's typical volume and payload size.
    fn wal_config(
        wal_dir: &std::path::Path,
        max_segment_mb: u64,
        max_buffer_entries: usize,
        flush_interval_secs: u64,
    ) -> WalConfig {
        let mut config = WalConfig::with_defaults(wal_dir.to_path_buf());
        config.max_segment_size = max_segment_mb * 1024 * 1024;
        config.max_buffer_entries = max_buffer_entries;
        config.flush_interval_secs = flush_interval_secs;
        config
    }

    let wal_manager = Arc::new(WalManager::new(
        // traces - baseline configuration
        wal_config(&wal_dir, 64, 1000, 30),
        // logs - higher volume, more frequent flushes
        wal_config(&wal_dir, 64, 2000, 15),
        // metrics - highest volume, most aggressive flushing
        wal_config(&wal_dir, 128, 5000, 10),
        // profiles - large payloads, lower entry count
        wal_config(&wal_dir, 256, 500, 60),
    ));

    tracing::info!(
        wal_dir = %wal_dir.display(),
        "Initialized WalManager for multi-tenant WAL isolation"
    );

    // Open WALs left on disk by previous runs so their unprocessed entries
    // get retried even before new traffic arrives for those tenants.
    match wal_manager.discover_existing_wals().await {
        Ok(discovered) if discovered > 0 => {
            tracing::info!(discovered, "Discovered existing WALs from previous runs");
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(error = %e, "Failed to discover existing WALs");
        }
    }

    // Background retry consumer: re-forwards unprocessed WAL entries whose
    // inline forward to the writer failed, and marks them processed so
    // segments can be reclaimed.
    WalRetryConsumer::new(wal_manager.clone(), flight_transport.clone()).spawn();
    tracing::info!("Started WAL retry consumer");

    // Per-tenant ingest rate limiter (unlimited unless configured via
    // [auth].default_limits / [[auth.tenants]].limits)
    let rate_limiter = Arc::new(common::ratelimit::TenantRateLimiter::from_auth_config(
        &auth_config,
    ));

    // Per-tenant storage quota enforcement. The tracker itself is a cheap
    // cache; the Iceberg-metadata accounting loop only runs when at least
    // one max_storage_bytes quota is configured.
    let storage_usage =
        Arc::new(common::storage_usage::StorageUsageTracker::from_auth_config(&auth_config));
    if storage_usage.quotas_configured() {
        let refresh_interval = full_config.auth.storage_usage_refresh_interval;
        // Failing to build the catalog here must fail startup: the
        // configuration promises storage quotas, and starting without
        // accounting would silently not enforce them.
        let catalog_manager = Arc::new(
            common::catalog_manager::CatalogManager::new(full_config)
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "max_storage_bytes is configured but the Iceberg catalog for \
                         storage usage accounting could not be initialized: {e:#}"
                    )
                })?,
        );
        common::storage_usage::spawn_usage_refresher(
            catalog_manager,
            storage_usage.clone(),
            refresh_interval,
        );
        tracing::info!(
            refresh_interval = ?refresh_interval,
            "Started per-tenant storage usage refresher for quota enforcement"
        );
    }

    // Create Authenticator for multi-tenant authentication
    let authenticator = Arc::new(Authenticator::new(auth_config, catalog));

    tracing::info!("Initialized Authenticator for multi-tenant authentication");

    Ok(AcceptorResources {
        flight_transport,
        wal_manager,
        authenticator,
        rate_limiter,
        storage_usage,
    })
}

/// Configuration for the gRPC acceptor server
pub struct GrpcAcceptorConfig {
    pub addr: SocketAddr,
    pub resources: AcceptorResources,
    /// `max_decoding_message_size` applied to every OTLP/gRPC service.
    /// Shared with the HTTP body limit via `[acceptor].max_request_body_bytes`
    /// so the two transports enforce the same ceiling.
    pub max_decoding_message_size: usize,
}

pub async fn serve_otlp_grpc(
    config: GrpcAcceptorConfig,
    init_tx: oneshot::Sender<()>,
    shutdown_rx: oneshot::Receiver<()>,
    stopped_tx: oneshot::Sender<()>,
) -> Result<(), anyhow::Error> {
    tracing::info!(address = %config.addr, "Starting OTLP/gRPC acceptor");

    let max_decoding_message_size = config.max_decoding_message_size;

    let AcceptorResources {
        flight_transport,
        wal_manager,
        authenticator,
        rate_limiter,
        storage_usage,
    } = config.resources;

    // Set up OTLP/gRPC services with handler pattern and WAL Manager
    // integration. Authentication is a single tower layer applied once
    // around the whole server below (crate::middleware::GrpcAuthLayer),
    // not a per-service interceptor.
    let log_handler = LogHandler::new(flight_transport.clone(), wal_manager.clone());
    let log_service = LogAcceptorService::new(log_handler)
        .with_rate_limiter(rate_limiter.clone())
        .with_storage_quota(storage_usage.clone());
    let log_server = LogsServiceServer::new(log_service)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(max_decoding_message_size);

    let trace_handler = TraceHandler::new(flight_transport.clone(), wal_manager.clone());
    let trace_service = TraceAcceptorService::new(trace_handler)
        .with_rate_limiter(rate_limiter.clone())
        .with_storage_quota(storage_usage.clone());
    let trace_server = TraceServiceServer::new(trace_service)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(max_decoding_message_size);

    let metrics_handler = MetricsHandler::new(flight_transport.clone(), wal_manager.clone());
    let metrics_service = MetricsAcceptorService::new(metrics_handler)
        .with_rate_limiter(rate_limiter.clone())
        .with_storage_quota(storage_usage.clone());
    let metric_server = MetricsServiceServer::new(metrics_service)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(max_decoding_message_size);

    let profile_handler = ProfileHandler::new(flight_transport.clone(), wal_manager.clone());
    let profile_service = ProfileAcceptorService::new(profile_handler)
        .with_rate_limiter(rate_limiter.clone())
        .with_storage_quota(storage_usage.clone());
    let profile_server = ProfilesServiceServer::new(profile_service)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(max_decoding_message_size);

    // Bind before signaling init: sending init_tx first would let the CLI
    // log "listening" for a port that turned out to be unbindable (e.g.
    // already in use), and a bind failure surfaced only as a panic in the
    // spawned task instead of a startup error the CLI could fail fast on.
    let listener = tokio::net::TcpListener::bind(config.addr)
        .await
        .with_context(|| format!("Failed to bind OTLP/gRPC listener on {}", config.addr))?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    // A dropped receiver means the CLI already gave up waiting on us — most
    // often because the *other* server (gRPC/HTTP) failed to start first and
    // the CLI's sequential `grpc_init_rx.await?` / `http_init_rx.await?`
    // returned early, dropping this receiver before we got here. That is a
    // real (if unlikely) race, not a programming error, so it must not
    // panic: propagate it like any other startup failure.
    init_tx.send(()).map_err(|_| {
        anyhow::anyhow!("Unable to send init signal for OTLP/gRPC: receiver dropped")
    })?;

    tonic::transport::Server::builder()
        .layer(crate::middleware::GrpcTraceLayer)
        .layer(crate::middleware::GrpcAuthLayer::new(authenticator))
        .add_service(log_server)
        .add_service(trace_server)
        .add_service(metric_server)
        .add_service(profile_server)
        .serve_with_incoming_shutdown(incoming, async {
            shutdown_rx.await.ok();
            tracing::info!("Shutting down OTLP/gRPC acceptor");
        })
        .await
        .context("OTLP/gRPC server error")?;

    stopped_tx
        .send(())
        .expect("Unable to send stopped signal for OTLP/gRPC");
    Ok(())
}

pub fn acceptor_router() -> Router {
    Router::new()
        .route("/health", get(health))
        .layer(axum::middleware::from_fn(
            common::self_monitoring::http_metrics_middleware,
        ))
        .layer(axum::middleware::from_fn(
            common::self_monitoring::http_trace_context_middleware,
        ))
}

/// Create a router for Prometheus remote_write endpoint with authentication
///
/// This router handles:
/// - POST /api/v1/write - Prometheus remote_write ingestion
///
/// Authentication is handled by middleware that extracts tenant context from headers.
///
/// # Example
///
/// ```ignore
/// let authenticator = Arc::new(Authenticator::new(auth_config, catalog));
/// let prometheus_handler = Arc::new(PrometheusHandler::new(flight_transport, wal_manager));
/// let router = prometheus_router(authenticator, prometheus_handler);
/// ```
pub fn prometheus_router(
    authenticator: Arc<Authenticator>,
    prometheus_handler: Arc<PrometheusHandler>,
) -> Router {
    use axum::middleware;

    let state = PrometheusHandlerState {
        handler: prometheus_handler,
    };

    // Use Extension instead of State for simpler type handling
    Router::new()
        .route("/api/v1/write", post(handle_prometheus_write_with_ext))
        .layer(Extension(state))
        .layer(middleware::from_fn(move |req, next| {
            let auth = authenticator.clone();
            async move { auth_middleware(auth, req, next).await }
        }))
        .layer(middleware::from_fn(
            common::self_monitoring::http_metrics_middleware,
        ))
        .layer(middleware::from_fn(
            common::self_monitoring::http_trace_context_middleware,
        ))
}

/// Shared per-signal state for OTLP/HTTP export endpoints
pub struct OtlpHttpState<H> {
    pub handler: Arc<H>,
    pub rate_limiter: Arc<common::ratelimit::TenantRateLimiter>,
    pub storage_quota: Arc<common::storage_usage::StorageUsageTracker>,
}

// Manual impl: `#[derive(Clone)]` would require `H: Clone`, but only the
// `Arc`s are cloned.
impl<H> Clone for OtlpHttpState<H> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            rate_limiter: self.rate_limiter.clone(),
            storage_quota: self.storage_quota.clone(),
        }
    }
}

/// Shared state for the OTLP/HTTP traces endpoint
pub type TracesHandlerState = OtlpHttpState<TraceHandler>;
/// Shared state for the OTLP/HTTP logs endpoint
pub type LogsHandlerState = OtlpHttpState<LogHandler>;
/// Shared state for the OTLP/HTTP metrics endpoint
pub type MetricsHandlerState = OtlpHttpState<MetricsHandler>;
/// Shared state for the OTLP/HTTP profiles endpoint
pub type ProfilesHandlerState = OtlpHttpState<ProfileHandler>;

/// Mount a single OTLP/HTTP export route behind the shared auth middleware
/// and HTTP self-monitoring metrics.
/// Build the CORS layer that lets the browser export telemetry cross-origin
/// to the OTLP/HTTP endpoints.
///
/// The browser sends `Authorization` + `X-Tenant-ID` / `X-Dataset-ID`, which
/// make the export a non-simple request; the browser first issues an
/// unauthenticated `OPTIONS` preflight. This layer sits outermost so it answers
/// the preflight before the auth middleware can reject it for missing
/// credentials. An empty origin list allows any origin (trusted-network
/// homelab default); a non-empty list restricts to those exact origins.
fn otlp_cors_layer(allowed_origins: &[String]) -> tower_http::cors::CorsLayer {
    use axum::http::{HeaderName, Method, header};
    use tower_http::cors::{Any, CorsLayer};

    let cors = CorsLayer::new()
        .allow_methods([Method::POST, Method::OPTIONS])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            HeaderName::from_static("x-tenant-id"),
            HeaderName::from_static("x-dataset-id"),
        ]);

    if allowed_origins.is_empty() {
        cors.allow_origin(Any)
    } else {
        let origins: Vec<axum::http::HeaderValue> = allowed_origins
            .iter()
            .filter_map(|o| match o.parse() {
                Ok(value) => Some(value),
                Err(e) => {
                    tracing::warn!(
                        origin = %o,
                        error = %e,
                        "Dropping unparseable entry from self_monitoring.frontend.allowed_origins"
                    );
                    None
                }
            })
            .collect();
        cors.allow_origin(origins)
    }
}

fn otlp_signal_router<H: Send + Sync + 'static>(
    path: &str,
    method_router: axum::routing::MethodRouter,
    authenticator: Arc<Authenticator>,
    state: OtlpHttpState<H>,
) -> Router {
    use axum::middleware;

    Router::new()
        .route(path, method_router)
        .layer(Extension(state))
        .layer(middleware::from_fn(move |req, next| {
            let auth = authenticator.clone();
            async move { auth_middleware(auth, req, next).await }
        }))
        .layer(middleware::from_fn(
            common::self_monitoring::http_metrics_middleware,
        ))
        .layer(middleware::from_fn(
            common::self_monitoring::http_trace_context_middleware,
        ))
}

/// Create a router for the OTLP/HTTP traces ingestion endpoint with authentication
///
/// Handles `POST /v1/traces` with protobuf (`application/x-protobuf`) or JSON
/// (`application/json`, protojson encoding with hex trace/span ids) request
/// bodies, matching the OTLP/HTTP specification. Per-tenant ingest rate
/// limits and storage quotas are enforced with HTTP 429; both are unlimited
/// unless configured.
pub fn traces_http_router(
    authenticator: Arc<Authenticator>,
    trace_handler: Arc<TraceHandler>,
    rate_limiter: Arc<common::ratelimit::TenantRateLimiter>,
    storage_quota: Arc<common::storage_usage::StorageUsageTracker>,
) -> Router {
    otlp_signal_router(
        "/v1/traces",
        post(handle_http_traces),
        authenticator,
        OtlpHttpState {
            handler: trace_handler,
            rate_limiter,
            storage_quota,
        },
    )
}

/// Create a router for the OTLP/HTTP logs ingestion endpoint with authentication
///
/// Handles `POST /v1/logs` with protobuf (`application/x-protobuf`) or JSON
/// (`application/json`, protojson encoding) request bodies, matching the
/// OTLP/HTTP specification. Per-tenant ingest rate limits and storage quotas
/// are enforced with HTTP 429; both are unlimited unless configured.
pub fn logs_http_router(
    authenticator: Arc<Authenticator>,
    log_handler: Arc<LogHandler>,
    rate_limiter: Arc<common::ratelimit::TenantRateLimiter>,
    storage_quota: Arc<common::storage_usage::StorageUsageTracker>,
) -> Router {
    otlp_signal_router(
        "/v1/logs",
        post(handle_http_logs),
        authenticator,
        OtlpHttpState {
            handler: log_handler,
            rate_limiter,
            storage_quota,
        },
    )
}

/// Create a router for the OTLP/HTTP metrics ingestion endpoint with authentication
///
/// Handles `POST /v1/metrics` with protobuf (`application/x-protobuf`) or
/// JSON (`application/json`, protojson encoding) request bodies, matching the
/// OTLP/HTTP specification. Per-tenant ingest rate limits and storage quotas
/// are enforced with HTTP 429; both are unlimited unless configured.
pub fn metrics_http_router(
    authenticator: Arc<Authenticator>,
    metrics_handler: Arc<MetricsHandler>,
    rate_limiter: Arc<common::ratelimit::TenantRateLimiter>,
    storage_quota: Arc<common::storage_usage::StorageUsageTracker>,
) -> Router {
    otlp_signal_router(
        "/v1/metrics",
        post(handle_http_metrics),
        authenticator,
        OtlpHttpState {
            handler: metrics_handler,
            rate_limiter,
            storage_quota,
        },
    )
}

/// Create a router for the OTLP/HTTP profiles ingestion endpoint with authentication
///
/// Handles `POST /v1development/profiles` (the OTLP development endpoint for
/// the profiles signal) with protobuf or JSON request bodies, matching the
/// OTLP/HTTP specification like every other signal — including answering in
/// the request's own encoding (finding M4: this used to always answer with
/// `application/json`, breaking OTLP/HTTP's same-encoding rule for
/// protobuf requests). Per-tenant ingest rate limits and storage quotas are
/// enforced with HTTP 429; both are unlimited unless configured.
pub fn profiles_http_router(
    authenticator: Arc<Authenticator>,
    profile_handler: Arc<ProfileHandler>,
    rate_limiter: Arc<common::ratelimit::TenantRateLimiter>,
    storage_quota: Arc<common::storage_usage::StorageUsageTracker>,
) -> Router {
    otlp_signal_router(
        "/v1development/profiles",
        post(handle_http_profiles),
        authenticator,
        OtlpHttpState {
            handler: profile_handler,
            rate_limiter,
            storage_quota,
        },
    )
}

/// Shared OTLP/HTTP export plumbing: enforce per-tenant rate limits and
/// storage quotas, decode the body by content type (protobuf or protojson),
/// dispatch to the per-signal handler (WAL durability + Flight forward), and
/// answer with an empty `Export*ServiceResponse` in the request's encoding.
async fn handle_otlp_http_export<Req, Resp, F, Fut>(
    signal: &'static str,
    rate_limiter: &common::ratelimit::TenantRateLimiter,
    storage_quota: &common::storage_usage::StorageUsageTracker,
    tenant_context: &common::auth::TenantContext,
    headers: &axum::http::HeaderMap,
    body: axum::body::Bytes,
    dispatch: F,
) -> axum::response::Response<axum::body::Body>
where
    Req: prost::Message + Default + serde::de::DeserializeOwned,
    Resp: prost::Message + Default,
    F: FnOnce(Req) -> Fut,
    Fut: std::future::Future<Output = Result<(), crate::handler::IngestError>>,
{
    // Per-tenant ingest rate limiting (HTTP 429 with the reason)
    if let Err(e) = rate_limiter.check_ingest(&tenant_context.tenant_id, body.len()) {
        return otlp_http_rate_limit_error(&e);
    }

    // Per-tenant storage quota: a tenant at or over max_storage_bytes
    // must free space (or get a raised quota) before ingesting more.
    if let Err(e) = storage_quota.check_ingest(&tenant_context.tenant_id) {
        return otlp_http_error(axum::http::StatusCode::TOO_MANY_REQUESTS, e.to_string());
    }

    let is_json = otlp_http_content_type_is_json(headers);

    let request = if is_json {
        match serde_json::from_slice::<Req>(&body) {
            Ok(request) => request,
            Err(e) => {
                return otlp_http_error(
                    axum::http::StatusCode::BAD_REQUEST,
                    format!("invalid OTLP/JSON {signal} payload: {e}"),
                );
            }
        }
    } else {
        match Req::decode(body.as_ref()) {
            Ok(request) => request,
            Err(e) => {
                return otlp_http_error(
                    axum::http::StatusCode::BAD_REQUEST,
                    format!("invalid OTLP/protobuf {signal} payload: {e}"),
                );
            }
        }
    };

    match dispatch(request).await {
        Ok(()) => {
            // Per OTLP/HTTP spec the response body is a full
            // Export*ServiceResponse in the same encoding as the request.
            let builder = axum::response::Response::builder().status(axum::http::StatusCode::OK);
            let response = if is_json {
                builder
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from("{}"))
            } else {
                builder
                    .header(axum::http::header::CONTENT_TYPE, "application/x-protobuf")
                    .body(axum::body::Body::from(Resp::default().encode_to_vec()))
            };
            match response {
                Ok(response) => response,
                Err(e) => {
                    tracing::error!(error = %e, signal, "Failed to build OTLP/HTTP export response");
                    otlp_http_error(
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        "failed to build export response".to_string(),
                    )
                }
            }
        }
        // Classify the failure (finding M3): a deterministic conversion
        // failure is 400 Bad Request (retrying the same bytes will fail
        // again), while a WAL/durability failure stays 503 Service
        // Unavailable so the client retries instead of dropping its copy.
        Err(crate::handler::IngestError::Invalid(e)) => {
            tracing::warn!(error = %e, signal, "Rejecting export via HTTP: invalid payload");
            otlp_http_error(
                axum::http::StatusCode::BAD_REQUEST,
                format!("invalid {signal} payload: {e:#}"),
            )
        }
        Err(crate::handler::IngestError::Unavailable(e)) => {
            tracing::error!(error = %e, signal, "Failed to durably accept export via HTTP");
            otlp_http_error(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                format!("failed to durably accept {signal} export: {e:#}"),
            )
        }
    }
}

/// OTLP/HTTP traces export: decode by content type, hand off to the
/// trace handler (WAL durability + Flight forward), and answer with an
/// `ExportTraceServiceResponse` in the request's encoding.
#[tracing::instrument(
    skip_all,
    fields(
        signaldb.tenant.id = %tenant_context.tenant_id,
        signaldb.dataset.id = %tenant_context.dataset_id
    )
)]
async fn handle_http_traces(
    Extension(state): Extension<TracesHandlerState>,
    headers: axum::http::HeaderMap,
    crate::middleware::TenantContextExtractor(tenant_context): crate::middleware::TenantContextExtractor,
    body: axum::body::Bytes,
) -> axum::response::Response<axum::body::Body> {
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceResponse;

    handle_otlp_http_export::<_, ExportTraceServiceResponse, _, _>(
        "traces",
        &state.rate_limiter,
        &state.storage_quota,
        &tenant_context,
        &headers,
        body,
        |request| {
            state
                .handler
                .handle_grpc_otlp_traces(&tenant_context, request)
        },
    )
    .await
}

/// OTLP/HTTP logs export: decode by content type, hand off to the
/// log handler (WAL durability + Flight forward), and answer with an
/// `ExportLogsServiceResponse` in the request's encoding.
#[tracing::instrument(
    skip_all,
    fields(
        signaldb.tenant.id = %tenant_context.tenant_id,
        signaldb.dataset.id = %tenant_context.dataset_id
    )
)]
async fn handle_http_logs(
    Extension(state): Extension<LogsHandlerState>,
    headers: axum::http::HeaderMap,
    crate::middleware::TenantContextExtractor(tenant_context): crate::middleware::TenantContextExtractor,
    body: axum::body::Bytes,
) -> axum::response::Response<axum::body::Body> {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceResponse;

    handle_otlp_http_export::<_, ExportLogsServiceResponse, _, _>(
        "logs",
        &state.rate_limiter,
        &state.storage_quota,
        &tenant_context,
        &headers,
        body,
        |request| {
            state
                .handler
                .handle_grpc_otlp_logs(&tenant_context, request)
        },
    )
    .await
}

/// OTLP/HTTP metrics export: decode by content type, hand off to the
/// metrics handler (WAL durability + Flight forward), and answer with an
/// `ExportMetricsServiceResponse` in the request's encoding.
#[tracing::instrument(
    skip_all,
    fields(
        signaldb.tenant.id = %tenant_context.tenant_id,
        signaldb.dataset.id = %tenant_context.dataset_id
    )
)]
async fn handle_http_metrics(
    Extension(state): Extension<MetricsHandlerState>,
    headers: axum::http::HeaderMap,
    crate::middleware::TenantContextExtractor(tenant_context): crate::middleware::TenantContextExtractor,
    body: axum::body::Bytes,
) -> axum::response::Response<axum::body::Body> {
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse;

    handle_otlp_http_export::<_, ExportMetricsServiceResponse, _, _>(
        "metrics",
        &state.rate_limiter,
        &state.storage_quota,
        &tenant_context,
        &headers,
        body,
        |request| {
            state
                .handler
                .handle_grpc_otlp_metrics(&tenant_context, request)
        },
    )
    .await
}

/// Whether an OTLP/HTTP request body is JSON-encoded.
///
/// OTLP/HTTP defaults to protobuf when no content type is present.
fn otlp_http_content_type_is_json(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/x-protobuf")
        .starts_with("application/json")
}

/// OTLP/HTTP profiles export: decode by content type, hand off to the
/// profile handler (WAL durability + Flight forward), and answer with an
/// `ExportProfilesServiceResponse` in the request's encoding — same shared
/// plumbing as traces/logs/metrics (finding M4: this used to hand-roll its
/// own request/response handling and always answered `application/json`
/// with an empty `{}` body, which is a protobuf-encoding response to a
/// protobuf-encoded request, violating the OTLP/HTTP same-encoding rule).
#[tracing::instrument(
    skip_all,
    fields(
        signaldb.tenant.id = %tenant_context.tenant_id,
        signaldb.dataset.id = %tenant_context.dataset_id
    )
)]
async fn handle_http_profiles(
    Extension(state): Extension<ProfilesHandlerState>,
    headers: axum::http::HeaderMap,
    crate::middleware::TenantContextExtractor(tenant_context): crate::middleware::TenantContextExtractor,
    body: axum::body::Bytes,
) -> axum::response::Response<axum::body::Body> {
    use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceResponse;

    handle_otlp_http_export::<_, ExportProfilesServiceResponse, _, _>(
        "profiles",
        &state.rate_limiter,
        &state.storage_quota,
        &tenant_context,
        &headers,
        body,
        |request| {
            state
                .handler
                .handle_grpc_otlp_profiles(&tenant_context, request)
        },
    )
    .await
}

fn otlp_http_error(
    status: axum::http::StatusCode,
    message: String,
) -> axum::response::Response<axum::body::Body> {
    axum::response::Response::builder()
        .status(status)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(
            serde_json::json!({ "message": message }).to_string(),
        ))
        .expect("error response must build")
}

/// A `429` for an ingest rate-limit rejection: same body shape as
/// [`otlp_http_error`], plus the `Retry-After` / `X-RateLimit-Limit` /
/// `X-RateLimit-Burst` headers computed from the rejected bucket's actual
/// state (`common::ratelimit::retry_headers`, shared with the router so
/// every SignalDB 429 answers identically). Also records the
/// `signaldb_rate_limit_rejections_total{surface="otlp_http",kind}` counter.
fn otlp_http_rate_limit_error(
    err: &common::ratelimit::RateLimitExceeded,
) -> axum::response::Response<axum::body::Body> {
    common::self_monitoring::record_rate_limit_rejection("otlp_http", err.kind.as_str());
    let mut response = otlp_http_error(axum::http::StatusCode::TOO_MANY_REQUESTS, err.to_string());
    let headers = response.headers_mut();
    for (name, value) in common::ratelimit::retry_headers(err) {
        headers.insert(name, value);
    }
    response
}

/// Handler variant using Extension instead of State for simpler router composition
#[tracing::instrument(
    skip_all,
    fields(
        signaldb.tenant.id = %tenant_context.tenant_id,
        signaldb.dataset.id = %tenant_context.dataset_id
    )
)]
async fn handle_prometheus_write_with_ext(
    Extension(state): Extension<PrometheusHandlerState>,
    headers: axum::http::HeaderMap,
    crate::middleware::TenantContextExtractor(tenant_context): crate::middleware::TenantContextExtractor,
    body: axum::body::Bytes,
) -> Result<axum::http::StatusCode, crate::handler::prometheus_handler::PrometheusError> {
    state
        .handler
        .handle_remote_write(&tenant_context, body, &headers)
        .await?;

    Ok(axum::http::StatusCode::NO_CONTENT)
}

async fn health() -> &'static str {
    "ok"
}

/// Configuration for the HTTP acceptor server
pub struct HttpAcceptorConfig {
    pub addr: SocketAddr,
    pub flight_transport: Arc<InMemoryFlightTransport>,
    pub wal_manager: Arc<WalManager>,
    pub authenticator: Arc<Authenticator>,
    pub rate_limiter: Arc<common::ratelimit::TenantRateLimiter>,
    pub storage_usage: Arc<common::storage_usage::StorageUsageTracker>,
    /// Origins the browser may export telemetry from (CORS). `None` disables
    /// cross-origin access entirely; `Some(empty)` allows any origin. Set from
    /// `[self_monitoring.frontend]` when browser telemetry export is enabled.
    pub cors_allowed_origins: Option<Vec<String>>,
    /// Maximum decoded request body size, in bytes, for every OTLP/HTTP and
    /// Prometheus remote_write route. From `[acceptor].max_request_body_bytes`,
    /// shared with the gRPC side's `max_decoding_message_size`.
    pub max_request_body_bytes: usize,
}

/// Apply the acceptor's OTLP/HTTP transport limits to a router: accept
/// gzip/zstd-encoded request bodies, then cap the decoded body size.
///
/// The OTel Collector's `otlphttp` exporter defaults to `compression:
/// gzip`, so without decompression support a default-configured collector
/// gets a 400 and drops the batch permanently (acceptor whole-crate review,
/// finding H1). `RequestDecompressionLayer` must sit *outside*
/// `DefaultBodyLimit` (added first here, so it is the inner layer): the
/// limit is enforced against the decompressed byte stream, not the wire
/// size, so a small compressed body cannot bypass it (zip-bomb guard,
/// finding H2). Everything downstream of these layers — including the
/// per-tenant rate limiter's `body.len()` ingest accounting in
/// `handle_otlp_http_export` — therefore sees decompressed bytes, which is
/// the deliberate choice: it matches the WAL/Iceberg bytes the tenant is
/// actually charged for, not the bytes that happened to cross the wire.
///
/// Exposed (not just inlined into [`serve_otlp_http`]) so integration tests
/// can exercise the exact same wiring production uses.
pub fn with_http_transport_limits(app: Router, max_request_body_bytes: usize) -> Router {
    app.layer(axum::extract::DefaultBodyLimit::max(max_request_body_bytes))
        .layer(tower_http::decompression::RequestDecompressionLayer::new())
}

pub async fn serve_otlp_http(
    config: HttpAcceptorConfig,
    init_tx: oneshot::Sender<()>,
    shutdown_rx: oneshot::Receiver<()>,
    stopped_tx: oneshot::Sender<()>,
) -> Result<(), anyhow::Error> {
    tracing::info!(address = %config.addr, "Starting OTLP/HTTP acceptor");

    // Create Prometheus handler with shared resources
    let prometheus_handler = Arc::new(
        PrometheusHandler::new(config.flight_transport.clone(), config.wal_manager.clone())
            .with_rate_limiter(config.rate_limiter.clone())
            .with_storage_quota(config.storage_usage.clone()),
    );

    // Create profiles handler with shared resources
    let profile_handler = Arc::new(ProfileHandler::new(
        config.flight_transport.clone(),
        config.wal_manager.clone(),
    ));

    // Create trace handler with shared resources (same WAL + Flight path as gRPC)
    let trace_handler = Arc::new(TraceHandler::new(
        config.flight_transport.clone(),
        config.wal_manager.clone(),
    ));

    // Create log handler with shared resources (same WAL + Flight path as gRPC)
    let log_handler = Arc::new(LogHandler::new(
        config.flight_transport.clone(),
        config.wal_manager.clone(),
    ));

    // Create metrics handler with shared resources (same WAL + Flight path as gRPC)
    let metrics_handler = Arc::new(MetricsHandler::new(
        config.flight_transport.clone(),
        config.wal_manager.clone(),
    ));

    // Build combined router with health, traces, logs, metrics, Prometheus,
    // and profiles endpoints
    let app = acceptor_router()
        .merge(traces_http_router(
            config.authenticator.clone(),
            trace_handler,
            config.rate_limiter.clone(),
            config.storage_usage.clone(),
        ))
        .merge(logs_http_router(
            config.authenticator.clone(),
            log_handler,
            config.rate_limiter.clone(),
            config.storage_usage.clone(),
        ))
        .merge(metrics_http_router(
            config.authenticator.clone(),
            metrics_handler,
            config.rate_limiter.clone(),
            config.storage_usage.clone(),
        ))
        .merge(prometheus_router(
            config.authenticator.clone(),
            prometheus_handler,
        ))
        .merge(profiles_http_router(
            config.authenticator.clone(),
            profile_handler,
            config.rate_limiter.clone(),
            config.storage_usage.clone(),
        ));

    let app = with_http_transport_limits(app, config.max_request_body_bytes);

    // Browser telemetry export is cross-origin (UI on the router, ingest on
    // the acceptor), so add CORS outermost when it is enabled — it must answer
    // the preflight before auth runs.
    let app = match &config.cors_allowed_origins {
        Some(origins) => {
            tracing::info!(
                allowed_origins = ?origins,
                "CORS enabled for browser telemetry export"
            );
            app.layer(otlp_cors_layer(origins))
        }
        None => app,
    };

    tracing::info!("OTLP traces endpoint enabled at POST /v1/traces");
    tracing::info!("OTLP logs endpoint enabled at POST /v1/logs");
    tracing::info!("OTLP metrics endpoint enabled at POST /v1/metrics");
    tracing::info!("Prometheus remote_write endpoint enabled at POST /api/v1/write");
    tracing::info!("OTLP profiles endpoint enabled at POST /v1development/profiles");

    // Bind before signaling init: sending init_tx first would let the CLI
    // log "listening" for a port that turned out to be unbindable (e.g.
    // already in use).
    let listener = TcpListener::bind(config.addr).await?;

    // A dropped receiver means the CLI already gave up waiting on us — see
    // the matching comment in serve_otlp_grpc. Propagate, don't panic.
    init_tx.send(()).map_err(|_| {
        anyhow::anyhow!("Unable to send init signal for OTLP/HTTP: receiver dropped")
    })?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            shutdown_rx.await.ok();
            tracing::info!("Shutting down OTLP/HTTP acceptor");
        })
        .await?;

    stopped_tx
        .send(())
        .expect("Unable to send stopped signal for OTLP/HTTP");

    Ok(())
}

#[cfg(test)]
mod cors_tests {
    use super::otlp_cors_layer;
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode, header};
    use axum::routing::post;
    use axum::{Router, response::IntoResponse};
    use tower::ServiceExt;

    fn app_with_cors(allowed: &[String]) -> Router {
        // A stand-in for the auth-gated /v1/traces route: it 401s without
        // credentials, exactly like the real endpoint, so the test proves the
        // CORS layer answers the preflight *before* auth would reject it.
        async fn guarded() -> impl IntoResponse {
            (StatusCode::UNAUTHORIZED, "missing auth")
        }
        Router::new()
            .route("/v1/traces", post(guarded))
            .layer(otlp_cors_layer(allowed))
    }

    fn preflight(origin: &str) -> Request<Body> {
        Request::builder()
            .method(Method::OPTIONS)
            .uri("/v1/traces")
            .header(header::ORIGIN, origin)
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .header(
                header::ACCESS_CONTROL_REQUEST_HEADERS,
                "authorization,x-tenant-id",
            )
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn preflight_allowed_for_any_origin_when_unrestricted() {
        let app = app_with_cors(&[]);
        let res = app
            .oneshot(preflight("http://ui.example:3000"))
            .await
            .unwrap();
        // Preflight is answered by the CORS layer, never reaching the 401 route.
        assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            res.headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .map(|v| v.to_str().unwrap().to_string()),
            Some("*".to_string())
        );
    }

    #[tokio::test]
    async fn preflight_echoes_listed_origin_and_omits_others() {
        let allowed = vec!["http://ui.example:3000".to_string()];

        let res = app_with_cors(&allowed)
            .oneshot(preflight("http://ui.example:3000"))
            .await
            .unwrap();
        assert_eq!(
            res.headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .map(|v| v.to_str().unwrap().to_string()),
            Some("http://ui.example:3000".to_string())
        );

        let res = app_with_cors(&allowed)
            .oneshot(preflight("http://evil.example"))
            .await
            .unwrap();
        assert!(
            res.headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .is_none(),
            "unlisted origin must not be granted CORS access"
        );
    }

    /// Finding L3: an unparseable entry in `allowed_origins` must not take
    /// down CORS for the *valid* entries alongside it — it's dropped (with
    /// a warning, not exercised by this test) and the rest still work.
    #[tokio::test]
    async fn preflight_still_allows_valid_origins_alongside_an_unparseable_one() {
        let allowed = vec![
            "not a valid header value\n".to_string(),
            "http://ui.example:3000".to_string(),
        ];

        let res = app_with_cors(&allowed)
            .oneshot(preflight("http://ui.example:3000"))
            .await
            .unwrap();
        assert_eq!(
            res.headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .map(|v| v.to_str().unwrap().to_string()),
            Some("http://ui.example:3000".to_string()),
            "the valid origin must still be allowed despite the unparseable entry"
        );
    }
}

/// Finding M3: `handle_otlp_http_export` must classify `IngestError`
/// into the right HTTP status — Invalid to 400, Unavailable to 503 — not
/// flatten every handler failure into 503.
#[cfg(test)]
mod otlp_http_export_classification_tests {
    use super::handle_otlp_http_export;
    use crate::handler::IngestError;
    use common::config::AuthConfig;
    use common::ratelimit::TenantRateLimiter;
    use common::storage_usage::StorageUsageTracker;
    use opentelemetry_proto::tonic::collector::trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
    };
    use prost::Message;

    fn test_tenant_context() -> common::auth::TenantContext {
        common::auth::TenantContext {
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            tenant_slug: "test-tenant".to_string(),
            dataset_slug: "test-dataset".to_string(),
            api_key_name: Some("test-key".to_string()),
            api_key_scopes: None,
            api_key_dataset_id: None,
            user_id: None,
            role: None,
            is_instance_admin: false,
            session_id: None,
            source: common::auth::TenantSource::Config,
        }
    }

    #[tokio::test]
    async fn invalid_payload_maps_to_400() {
        let rate_limiter = TenantRateLimiter::from_auth_config(&AuthConfig::default());
        let storage_quota = StorageUsageTracker::from_auth_config(&AuthConfig::default());
        let tenant_context = test_tenant_context();
        let headers = axum::http::HeaderMap::new();
        let body = axum::body::Bytes::from(ExportTraceServiceRequest::default().encode_to_vec());

        let response =
            handle_otlp_http_export::<ExportTraceServiceRequest, ExportTraceServiceResponse, _, _>(
                "traces",
                &rate_limiter,
                &storage_quota,
                &tenant_context,
                &headers,
                body,
                |_req| async {
                    Err(IngestError::Invalid(anyhow::anyhow!(
                        "OTLP to Arrow conversion failed"
                    )))
                },
            )
            .await;

        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn unavailable_backend_maps_to_503() {
        let rate_limiter = TenantRateLimiter::from_auth_config(&AuthConfig::default());
        let storage_quota = StorageUsageTracker::from_auth_config(&AuthConfig::default());
        let tenant_context = test_tenant_context();
        let headers = axum::http::HeaderMap::new();
        let body = axum::body::Bytes::from(ExportTraceServiceRequest::default().encode_to_vec());

        let response =
            handle_otlp_http_export::<ExportTraceServiceRequest, ExportTraceServiceResponse, _, _>(
                "traces",
                &rate_limiter,
                &storage_quota,
                &tenant_context,
                &headers,
                body,
                |_req| async { Err(IngestError::Unavailable(anyhow::anyhow!("WAL unavailable"))) },
            )
            .await;

        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
