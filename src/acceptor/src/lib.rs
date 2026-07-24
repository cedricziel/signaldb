pub mod handler;
pub mod middleware;
pub mod services;

use std::{net::SocketAddr, sync::Arc, time::SystemTime};

use axum::{
    Extension, Router,
    routing::{get, post},
};
use common::dataset::DataSet;
use datafusion::arrow::datatypes::Schema;
use datafusion::parquet::{
    arrow::AsyncArrowWriter,
    file::properties::{WriterProperties, WriterVersion},
};
use opentelemetry_proto::tonic::collector::{
    logs::v1::logs_service_server::LogsServiceServer,
    metrics::v1::metrics_service_server::MetricsServiceServer,
    profiles::v1development::profiles_service_server::ProfilesServiceServer,
    trace::v1::trace_service_server::TraceServiceServer,
};
use tokio::net::TcpListener;
use tokio::{
    fs::{File, create_dir_all},
    sync::oneshot,
};
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
use crate::middleware::grpc_auth::grpc_auth_interceptor;
use crate::services::{
    otlp_log_service::LogAcceptorService, otlp_metric_service::MetricsAcceptorService,
    otlp_profile_service::ProfileAcceptorService, otlp_trace_service::TraceAcceptorService,
};
use common::auth::Authenticator;

pub async fn get_parquet_writer(
    data_set: DataSet,
    schema: Schema,
    config: &Configuration,
) -> AsyncArrowWriter<File> {
    tracing::info!("get_parquet_writer");

    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();

    // Get storage path from configuration DSN
    let storage_dsn = &config.storage.dsn;
    let base_path = if let Some(path) = storage_dsn.strip_prefix("file://") {
        path.to_string()
    } else {
        // Fallback to .data/ds if not a file:// URL
        ".data/ds".to_string()
    };

    let dir_path = format!("{}/{}", base_path, data_set.data_type);
    create_dir_all(&dir_path)
        .await
        .expect("Error creating directory");

    let file_path = format!(
        "{}/{}.parquet",
        dir_path,
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );

    tracing::info!(path = %file_path, "Writing parquet file");

    AsyncArrowWriter::try_new(
        File::create(file_path)
            .await
            .expect("Error creating parquet file"),
        Arc::new(schema),
        Some(props),
    )
    .expect("Error creating parquet writer")
}

/// Shared resources for acceptor services (gRPC and HTTP)
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

    // Initialize WalManager with separate base configurations for each signal type

    // Base WAL config for traces - baseline configuration
    let mut traces_wal_config = WalConfig::with_defaults(wal_dir.clone());
    traces_wal_config.max_segment_size = 64 * 1024 * 1024; // 64MB
    traces_wal_config.max_buffer_entries = 1000;
    traces_wal_config.flush_interval_secs = 30;

    // Base WAL config for logs - higher volume, more frequent flushes
    let mut logs_wal_config = WalConfig::with_defaults(wal_dir.clone());
    logs_wal_config.max_segment_size = 64 * 1024 * 1024; // 64MB
    logs_wal_config.max_buffer_entries = 2000;
    logs_wal_config.flush_interval_secs = 15;

    // Base WAL config for metrics - highest volume, most aggressive flushing
    let mut metrics_wal_config = WalConfig::with_defaults(wal_dir.clone());
    metrics_wal_config.max_segment_size = 128 * 1024 * 1024; // 128MB
    metrics_wal_config.max_buffer_entries = 5000;
    metrics_wal_config.flush_interval_secs = 10;

    // Base WAL config for profiles - large payloads, lower entry count
    let mut profiles_wal_config = WalConfig::with_defaults(wal_dir.clone());
    profiles_wal_config.max_segment_size = 256 * 1024 * 1024; // 256MB
    profiles_wal_config.max_buffer_entries = 500;
    profiles_wal_config.flush_interval_secs = 60;

    let wal_manager = Arc::new(WalManager::new(
        traces_wal_config,
        logs_wal_config,
        metrics_wal_config,
        profiles_wal_config,
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
}

pub async fn serve_otlp_grpc(
    config: GrpcAcceptorConfig,
    init_tx: oneshot::Sender<()>,
    shutdown_rx: oneshot::Receiver<()>,
    stopped_tx: oneshot::Sender<()>,
) -> Result<(), anyhow::Error> {
    tracing::info!(address = %config.addr, "Starting OTLP/gRPC acceptor");

    let AcceptorResources {
        flight_transport,
        wal_manager,
        authenticator,
        rate_limiter,
        storage_usage,
    } = config.resources;

    // Set up OTLP/gRPC services with handler pattern, WAL Manager integration, and auth interceptor
    let log_handler = LogHandler::new(flight_transport.clone(), wal_manager.clone());
    let log_service = LogAcceptorService::new(log_handler)
        .with_rate_limiter(rate_limiter.clone())
        .with_storage_quota(storage_usage.clone());
    let auth_for_logs = authenticator.clone();
    let log_server = LogsServiceServer::with_interceptor(log_service, move |req| {
        grpc_auth_interceptor(auth_for_logs.clone(), req)
    });

    let trace_handler = TraceHandler::new(flight_transport.clone(), wal_manager.clone());
    let trace_service = TraceAcceptorService::new(trace_handler)
        .with_rate_limiter(rate_limiter.clone())
        .with_storage_quota(storage_usage.clone());
    let auth_for_traces = authenticator.clone();
    let trace_server = TraceServiceServer::with_interceptor(trace_service, move |req| {
        grpc_auth_interceptor(auth_for_traces.clone(), req)
    });

    let metrics_handler = MetricsHandler::new(flight_transport.clone(), wal_manager.clone());
    let metrics_service = MetricsAcceptorService::new(metrics_handler)
        .with_rate_limiter(rate_limiter.clone())
        .with_storage_quota(storage_usage.clone());
    let auth_for_metrics = authenticator.clone();
    let metric_server = MetricsServiceServer::with_interceptor(metrics_service, move |req| {
        grpc_auth_interceptor(auth_for_metrics.clone(), req)
    });

    let profile_handler = ProfileHandler::new(flight_transport.clone(), wal_manager.clone());
    let profile_service = ProfileAcceptorService::new(profile_handler)
        .with_rate_limiter(rate_limiter.clone())
        .with_storage_quota(storage_usage.clone());
    let auth_for_profiles = authenticator.clone();
    let profile_server = ProfilesServiceServer::with_interceptor(profile_service, move |req| {
        grpc_auth_interceptor(auth_for_profiles.clone(), req)
    });

    init_tx
        .send(())
        .expect("Unable to send init signal for OTLP/gRPC");

    tonic::transport::Server::builder()
        .add_service(log_server)
        .add_service(trace_server)
        .add_service(metric_server)
        .add_service(profile_server)
        .serve_with_shutdown(config.addr, async {
            shutdown_rx.await.ok();
            tracing::info!("Shutting down OTLP/gRPC acceptor");
        })
        .await
        .expect("Unable to start OTLP acceptor");

    stopped_tx
        .send(())
        .expect("Unable to send stopped signal for OTLP/gRPC");
    Ok(())
}

pub fn acceptor_router() -> Router {
    Router::new()
        .route("/v1/traces", post(handle_traces))
        .route("/health", get(health))
        .layer(axum::middleware::from_fn(
            common::self_monitoring::http_metrics_middleware,
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
}

/// Shared state for the OTLP/HTTP profiles endpoint
#[derive(Clone)]
pub struct ProfilesHandlerState {
    pub handler: Arc<ProfileHandler>,
    pub rate_limiter: Arc<common::ratelimit::TenantRateLimiter>,
    pub storage_quota: Arc<common::storage_usage::StorageUsageTracker>,
}

/// Create a router for the OTLP/HTTP profiles ingestion endpoint with authentication
///
/// Handles `POST /v1development/profiles` (the OTLP development endpoint for
/// the profiles signal) with protobuf or JSON request bodies. Per-tenant
/// ingest rate limits and storage quotas are enforced with HTTP 429; both
/// are unlimited unless configured.
pub fn profiles_http_router(
    authenticator: Arc<Authenticator>,
    profile_handler: Arc<ProfileHandler>,
    rate_limiter: Arc<common::ratelimit::TenantRateLimiter>,
    storage_quota: Arc<common::storage_usage::StorageUsageTracker>,
) -> Router {
    use axum::middleware;

    let state = ProfilesHandlerState {
        handler: profile_handler,
        rate_limiter,
        storage_quota,
    };

    Router::new()
        .route("/v1development/profiles", post(handle_http_profiles))
        .layer(Extension(state))
        .layer(middleware::from_fn(move |req, next| {
            let auth = authenticator.clone();
            async move { auth_middleware(auth, req, next).await }
        }))
        .layer(middleware::from_fn(
            common::self_monitoring::http_metrics_middleware,
        ))
}

/// OTLP/HTTP profiles export: decode by content type, hand off to the
/// profile handler, and answer with an empty export response.
#[tracing::instrument(
    skip_all,
    fields(
        tenant_id = %tenant_context.tenant_id,
        dataset_id = %tenant_context.dataset_id
    )
)]
async fn handle_http_profiles(
    Extension(state): Extension<ProfilesHandlerState>,
    headers: axum::http::HeaderMap,
    crate::middleware::TenantContextExtractor(tenant_context): crate::middleware::TenantContextExtractor,
    body: axum::body::Bytes,
) -> axum::response::Response<axum::body::Body> {
    use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest;
    use prost::Message;

    // Per-tenant ingest rate limiting (HTTP 429 with the reason)
    if let Err(e) = state
        .rate_limiter
        .check_ingest(&tenant_context.tenant_id, body.len())
    {
        return otlp_http_error(axum::http::StatusCode::TOO_MANY_REQUESTS, e.to_string());
    }

    // Per-tenant storage quota: a tenant at or over max_storage_bytes
    // must free space (or get a raised quota) before ingesting more.
    if let Err(e) = state.storage_quota.check_ingest(&tenant_context.tenant_id) {
        return otlp_http_error(axum::http::StatusCode::TOO_MANY_REQUESTS, e.to_string());
    }

    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/x-protobuf");

    let request = if content_type.starts_with("application/json") {
        match serde_json::from_slice::<ExportProfilesServiceRequest>(&body) {
            Ok(request) => request,
            Err(e) => {
                return otlp_http_error(
                    axum::http::StatusCode::BAD_REQUEST,
                    format!("invalid OTLP/JSON profiles payload: {e}"),
                );
            }
        }
    } else {
        match ExportProfilesServiceRequest::decode(body.as_ref()) {
            Ok(request) => request,
            Err(e) => {
                return otlp_http_error(
                    axum::http::StatusCode::BAD_REQUEST,
                    format!("invalid OTLP/protobuf profiles payload: {e}"),
                );
            }
        }
    };

    match state
        .handler
        .handle_grpc_otlp_profiles(&tenant_context, request)
        .await
    {
        Ok(()) => axum::response::Response::builder()
            .status(axum::http::StatusCode::OK)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from("{}"))
            .expect("static response must build"),
        Err(e) => {
            tracing::error!(error = %e, "Failed to durably accept profiles export via HTTP");
            otlp_http_error(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                format!("failed to durably accept profiles export: {e:#}"),
            )
        }
    }
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

/// Handler variant using Extension instead of State for simpler router composition
#[tracing::instrument(
    skip_all,
    fields(
        tenant_id = %tenant_context.tenant_id,
        dataset_id = %tenant_context.dataset_id
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

#[tracing::instrument(skip_all)]
async fn handle_traces(
    axum::extract::Json(payload): axum::extract::Json<serde_json::Value>,
) -> axum::response::Response<axum::body::Body> {
    tracing::info!(
        payload_size = payload.to_string().len(),
        "Got traces via HTTP"
    );
    axum::response::Response::builder()
        .status(200)
        .body(axum::body::Body::empty())
        .unwrap()
}

/// Configuration for the HTTP acceptor server
pub struct HttpAcceptorConfig {
    pub addr: SocketAddr,
    pub flight_transport: Arc<InMemoryFlightTransport>,
    pub wal_manager: Arc<WalManager>,
    pub authenticator: Arc<Authenticator>,
    pub rate_limiter: Arc<common::ratelimit::TenantRateLimiter>,
    pub storage_usage: Arc<common::storage_usage::StorageUsageTracker>,
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

    // Build combined router with health, traces, Prometheus, and profiles endpoints
    let app = acceptor_router()
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

    tracing::info!("Prometheus remote_write endpoint enabled at POST /api/v1/write");
    tracing::info!("OTLP profiles endpoint enabled at POST /v1development/profiles");

    init_tx
        .send(())
        .expect("Unable to send init signal for OTLP/HTTP");

    let listener = TcpListener::bind(config.addr).await?;
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
