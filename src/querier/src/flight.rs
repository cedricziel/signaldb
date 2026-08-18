use anyhow::Context;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult,
    SchemaResult, Ticket,
};
use bytes::Bytes;
use common::CatalogManager;
use common::config::QuerierConfig;
use common::flight::batches_to_compressed_flight_data;
use common::flight::schema::create_span_batch_schema;
use common::flight::transport::InMemoryFlightTransport;
use common::storage::create_object_store_from_dsn;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionConfig;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use object_store::ObjectStore;
use std::collections::HashSet;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::Instrument;

use crate::query::ir_planner::IrService;
use crate::query::logs::LogsService;
use crate::query::metrics::MetricsService;
use crate::query::profile::{
    FindProfileByIdParams, ProfileDiffParams, ProfileDiscoveryParams, ProfileSearchParams,
    ProfileService,
};
use crate::query::trace::TraceService;
use crate::query::{
    DetectedFieldsParams, FindTraceByIdParams, IrQueryParams, LogQueryParams, LogSeriesParams,
    MetricQueryParams, MetricSeriesParams, PromQlQueryParams, SearchQueryParams,
    TraceTagValuesParams, TraceTagsParams,
};

/// Queries the Iceberg catalog directly, bypassing `datafusion_iceberg`'s
/// stale `Mirror` cache so newly-created tables are immediately visible.
struct LiveIcebergSchema {
    namespace: iceberg_rust::catalog::namespace::Namespace,
    catalog: Arc<dyn iceberg_rust::catalog::Catalog>,
}

impl std::fmt::Debug for LiveIcebergSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveIcebergSchema")
            .field("namespace", &self.namespace)
            .finish()
    }
}

#[async_trait::async_trait]
impl SchemaProvider for LiveIcebergSchema {
    fn table_names(&self) -> Vec<String> {
        vec![]
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::error::Result<Option<Arc<dyn datafusion::datasource::TableProvider>>> {
        use iceberg_rust::catalog::identifier::Identifier;
        use iceberg_rust::catalog::tabular::Tabular;
        use std::ops::Deref;

        let ident = Identifier::try_new(
            &[self.namespace.deref(), &[name.to_string()]].concat(),
            None,
        )
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        match self.catalog.clone().load_tabular(&ident).await {
            Ok(tabular) => {
                let table = match tabular {
                    Tabular::Table(t) => Arc::new(datafusion_iceberg::DataFusionTable::new(
                        Tabular::Table(t),
                        None,
                        None,
                        None,
                    ))
                        as Arc<dyn datafusion::datasource::TableProvider>,
                    other => Arc::new(datafusion_iceberg::DataFusionTable::new(
                        other, None, None, None,
                    ))
                        as Arc<dyn datafusion::datasource::TableProvider>,
                };
                Ok(Some(table))
            }
            Err(iceberg_rust::error::Error::CatalogNotFound) => Ok(None),
            Err(e) => Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }
    }

    fn table_exist(&self, _name: &str) -> bool {
        true
    }
}

/// A DataFusion `CatalogProvider` scoped to a single tenant.
///
struct TenantCatalog {
    tenant_slug: String,
    catalog: Arc<dyn iceberg_rust::catalog::Catalog>,
}

impl std::fmt::Debug for TenantCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantCatalog")
            .field("tenant_slug", &self.tenant_slug)
            .finish()
    }
}

impl CatalogProvider for TenantCatalog {
    fn schema_names(&self) -> Vec<String> {
        vec![]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let namespace = iceberg_rust::catalog::namespace::Namespace::try_new(&[
            self.tenant_slug.clone(),
            name.to_string(),
        ])
        .ok()?;

        Some(Arc::new(LiveIcebergSchema {
            namespace,
            catalog: self.catalog.clone(),
        }))
    }

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(None)
    }

    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> datafusion::error::Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(None)
    }
}

/// Represents different types of ticket requests
#[derive(Debug)]
enum TicketRequest {
    FindTrace {
        tenant_slug: String,
        dataset_slug: String,
        trace_id: String,
        /// Optional unix-second time hints bracketing the expected trace
        start: Option<i64>,
        end: Option<i64>,
    },
    SearchTraces {
        tenant_slug: String,
        dataset_slug: String,
        params: SearchQueryParams,
    },
    SqlQuery {
        sql: String,
    },
    FindProfile {
        tenant_slug: String,
        dataset_slug: String,
        profile_id: String,
    },
    SearchProfiles {
        tenant_slug: String,
        dataset_slug: String,
        params: ProfileSearchParams,
    },
    ProfileTypes {
        tenant_slug: String,
        dataset_slug: String,
        params: ProfileDiscoveryParams,
    },
    ProfileLabelNames {
        tenant_slug: String,
        dataset_slug: String,
        params: ProfileDiscoveryParams,
    },
    ProfileLabelValues {
        tenant_slug: String,
        dataset_slug: String,
        label_name: String,
        params: ProfileDiscoveryParams,
    },
    ProfileFlamegraph {
        tenant_slug: String,
        dataset_slug: String,
        params: ProfileSearchParams,
    },
    ProfileDiff {
        tenant_slug: String,
        dataset_slug: String,
        params: ProfileDiffParams,
    },
    SqlProfiles {
        tenant_slug: String,
        dataset_slug: String,
        sql: String,
    },
    ProfilesByTrace {
        tenant_slug: String,
        dataset_slug: String,
        trace_id: String,
        span_id: Option<String>,
    },
    QueryLogs {
        tenant_slug: String,
        dataset_slug: String,
        params: LogQueryParams,
    },
    /// Native Query IR ticket: `query_ir:{tenant}:{dataset}:{json IrQueryParams}`.
    QueryIr {
        tenant_slug: String,
        dataset_slug: String,
        params: IrQueryParams,
    },
    QueryLogsLabels {
        tenant_slug: String,
        dataset_slug: String,
        start: i64,
        end: i64,
    },
    QueryLogsLabelValues {
        tenant_slug: String,
        dataset_slug: String,
        label: String,
        start: i64,
        end: i64,
    },
    QueryLogsSeries {
        tenant_slug: String,
        dataset_slug: String,
        params: LogSeriesParams,
    },
    QueryLogsDetectedFields {
        tenant_slug: String,
        dataset_slug: String,
        params: DetectedFieldsParams,
    },
    QueryMetric {
        tenant_slug: String,
        dataset_slug: String,
        params: MetricQueryParams,
    },
    QueryPromql {
        tenant_slug: String,
        dataset_slug: String,
        params: PromQlQueryParams,
    },
    QueryMetricLabels {
        tenant_slug: String,
        dataset_slug: String,
        start: i64,
        end: i64,
    },
    QueryMetricLabelValues {
        tenant_slug: String,
        dataset_slug: String,
        label: String,
        start: i64,
        end: i64,
    },
    QueryMetricSeries {
        tenant_slug: String,
        dataset_slug: String,
        params: MetricSeriesParams,
    },
    /// Trace tag-name discovery: `trace_tags:{tenant}:{dataset}:{json TraceTagsParams}`.
    TraceTags {
        tenant_slug: String,
        dataset_slug: String,
        params: TraceTagsParams,
    },
    /// Trace tag-value discovery:
    /// `trace_tag_values:{tenant}:{dataset}:{tag}:{json TraceTagValuesParams}`.
    TraceTagValues {
        tenant_slug: String,
        dataset_slug: String,
        tag: String,
        params: TraceTagValuesParams,
    },
}

/// Flight service for query execution against stored data
pub struct QuerierFlightService {
    _flight_transport: Arc<InMemoryFlightTransport>,
    session_ctx: Arc<SessionContext>,
    trace_service: TraceService,
    profile_service: ProfileService,
    logs_service: LogsService,
    metrics_service: MetricsService,
    ir_service: IrService,
    #[allow(dead_code)]
    iceberg_catalog: Option<Arc<dyn iceberg_rust::catalog::Catalog>>,
    limits: QuerierConfig,
    /// Per-tenant concurrent-query permits, populated lazily. Bounded by
    /// the number of distinct tenants.
    query_permits: dashmap::DashMap<String, Arc<tokio::sync::Semaphore>>,
    /// Tenant registry, used to resolve and lazily register catalogs for
    /// tenants created after startup (e.g. via the admin API). `None` for the
    /// legacy single-object-store constructor used in tests.
    catalog_manager: Option<Arc<CatalogManager>>,
    /// Slugs whose DataFusion catalog is already registered in `session_ctx`.
    registered_tenants: dashmap::DashSet<String>,
    /// Per-tenant registration locks so concurrent first-queries for the *same*
    /// tenant register exactly once, while different tenants register
    /// concurrently (no single global lock on the query path).
    tenant_reg_locks: dashmap::DashMap<String, Arc<tokio::sync::Mutex<()>>>,
    /// Cache of per-request `SessionContext`s keyed by `(tenant_slug,
    /// dataset_slug)`, populated lazily by `session_for_request`. Avoids
    /// rebuilding the DataFusion function/UDF registry (via
    /// `SessionStateBuilder::new_from_existing`) on every single request —
    /// see the safety comment on `session_for_request` for why sharing one
    /// context per tenant+dataset is sound.
    session_cache: dashmap::DashMap<(String, String), Arc<SessionContext>>,
}

/// Build the querier's SessionConfig with DataFusion scan/pushdown options
/// from `[querier.datafusion]` applied. Only mutates `SessionConfig` options;
/// it deliberately leaves `create_default_catalog_and_schema` untouched — the
/// per-request session builder (`session_for_request`) relies on the default
/// catalog behavior of the shared context and disables it itself when
/// cloning state.
fn session_config_from(limits: &QuerierConfig) -> SessionConfig {
    let mut config = SessionConfig::new();
    let options = config.options_mut();
    options.execution.split_file_groups_by_statistics =
        limits.datafusion.split_file_groups_by_statistics;
    options.execution.parquet.pushdown_filters = limits.datafusion.pushdown_filters;
    options.execution.parquet.reorder_filters = limits.datafusion.reorder_filters;
    config
}

/// Build a SessionContext whose RuntimeEnv enforces the configured memory
/// limit (spilling operators use the default disk manager). Falls back to
/// an unlimited-memory context if the runtime cannot be built, which is
/// logged as an error and practically cannot happen with default settings.
///
/// The pool is the shared [`common::datafusion_runtime::bounded_memory_pool`]
/// — a `FairSpillPool`, so one tenant's heavy sort cannot take the whole
/// pool first-come and starve the rest (#941).
fn session_context_with_limits(limits: &QuerierConfig) -> SessionContext {
    let session_config = session_config_from(limits);
    let mut builder = RuntimeEnvBuilder::new();
    match limits.memory_limit_mb {
        Some(mb) => {
            builder = builder.with_memory_pool(common::datafusion_runtime::bounded_memory_pool(
                (mb as usize) * 1024 * 1024,
                limits.memory_pool_fraction,
            ));
            tracing::info!(
                memory_limit_mb = mb,
                memory_pool_fraction = limits.memory_pool_fraction,
                "Querier memory pool configured"
            );
        }
        None => {
            tracing::warn!(
                "Querier memory is UNBOUNDED ([querier].memory_limit_mb is not set); \
                 a single heavy query can exhaust process memory"
            );
        }
    }
    match builder.build() {
        Ok(runtime_env) => {
            SessionContext::new_with_config_rt(session_config, Arc::new(runtime_env))
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                "Failed to build limited RuntimeEnv; falling back to unlimited memory"
            );
            SessionContext::new_with_config(session_config)
        }
    }
}

/// Register the object store for a dataset's storage DSN on `session_ctx`.
///
/// An unparseable DSN is logged and skipped; a store-construction failure is
/// propagated. Shared by startup and lazy on-demand registration so both paths
/// handle storage-DSN failures identically.
fn register_dataset_object_store(
    session_ctx: &SessionContext,
    url_str: &str,
    tenant_id: &str,
    dataset_id: &str,
) -> anyhow::Result<()> {
    match url::Url::parse(url_str) {
        Ok(url) => {
            let store = create_object_store_from_dsn(url_str).with_context(|| {
                format!("Failed to create object store for dataset DSN: {url_str}")
            })?;
            session_ctx.runtime_env().register_object_store(&url, store);
            tracing::debug!(scheme = %url.scheme(), url = %url_str, "Registered object store");
        }
        Err(e) => {
            tracing::warn!(
                dsn = %url_str,
                tenant_id = %tenant_id,
                dataset_id = %dataset_id,
                error = %e,
                "Skipping invalid storage DSN"
            );
        }
    }
    Ok(())
}

impl QuerierFlightService {
    /// Build a Tempo gRPC querier backed by this service's trace query
    /// engine, for serving `tempopb.Querier` alongside Flight.
    pub fn tempo_querier(&self) -> crate::services::tempo::SignalDBQuerier {
        crate::services::tempo::SignalDBQuerier::new(self.trace_service.clone())
    }

    /// Create a new QuerierFlightService with default resource limits
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        flight_transport: Arc<InMemoryFlightTransport>,
    ) -> Self {
        Self::new_with_limits(object_store, flight_transport, QuerierConfig::default())
    }

    /// Create a new QuerierFlightService with explicit resource limits
    pub fn new_with_limits(
        object_store: Arc<dyn ObjectStore>,
        flight_transport: Arc<InMemoryFlightTransport>,
        limits: QuerierConfig,
    ) -> Self {
        let session_ctx = Arc::new(session_context_with_limits(&limits));

        // Register object store with DataFusion for querying Parquet files
        // This allows querying files like: SELECT * FROM 'batch/file.parquet'
        let url = url::Url::parse("file://").unwrap();
        session_ctx
            .runtime_env()
            .register_object_store(&url, object_store.clone());

        // Create trace service for specialized trace queries
        let trace_service = TraceService::new(session_ctx.as_ref().clone(), "traces".to_string())
            .with_max_search_limit(limits.max_search_limit);
        let profile_service = ProfileService::new(session_ctx.as_ref().clone())
            .with_max_search_limit(limits.max_search_limit);
        let logs_service = LogsService::new(session_ctx.as_ref().clone());
        let metrics_service = MetricsService::new(session_ctx.as_ref().clone());
        let ir_service = IrService::new(session_ctx.as_ref().clone());

        Self {
            _flight_transport: flight_transport,
            session_ctx,
            trace_service,
            profile_service,
            logs_service,
            metrics_service,
            ir_service,
            iceberg_catalog: None,
            limits,
            query_permits: dashmap::DashMap::new(),
            catalog_manager: None,
            registered_tenants: dashmap::DashSet::new(),
            tenant_reg_locks: dashmap::DashMap::new(),
            session_cache: dashmap::DashMap::new(),
        }
    }

    /// Create a new QuerierFlightService with CatalogManager and per-tenant catalogs
    ///
    /// This constructor registers each enabled tenant as a separate DataFusion catalog,
    /// allowing queries like `SELECT * FROM tenant.dataset.traces` to work correctly.
    pub async fn new_with_catalog_manager(
        flight_transport: Arc<InMemoryFlightTransport>,
        catalog_manager: Arc<CatalogManager>,
        limits: QuerierConfig,
    ) -> anyhow::Result<Self> {
        let session_ctx = Arc::new(session_context_with_limits(&limits));

        // Track registered storage URLs to avoid duplicates
        let mut registered_urls: HashSet<String> = HashSet::new();

        // Enumerate active tenants through the source-agnostic registry so that
        // database-created (admin-API) tenants are registered alongside
        // config-defined ones. Falls back to config-only when the catalog
        // manager has no database tenant source attached.
        let tenants = catalog_manager
            .list_active_tenants()
            .await
            .context("Failed to enumerate active tenants for catalog registration")?;

        // Register object stores for all configured storage backends, each
        // unique DSN once.
        for tenant in &tenants {
            for dataset in &tenant.datasets {
                if registered_urls.insert(dataset.storage_dsn.clone()) {
                    register_dataset_object_store(
                        &session_ctx,
                        &dataset.storage_dsn,
                        &tenant.id,
                        &dataset.id,
                    )?;
                }
            }
        }

        let iceberg_catalog = catalog_manager.catalog();

        let registered_tenants: dashmap::DashSet<String> = dashmap::DashSet::new();
        for tenant in &tenants {
            let tenant_catalog = TenantCatalog {
                tenant_slug: tenant.slug.clone(),
                catalog: iceberg_catalog.clone(),
            };

            session_ctx.register_catalog(&tenant.slug, Arc::new(tenant_catalog));
            registered_tenants.insert(tenant.slug.clone());
            tracing::info!(
                catalog = %tenant.slug,
                tenant_id = %tenant.id,
                "Registered DataFusion catalog"
            );
        }

        // Create trace service for specialized trace queries
        let trace_service = TraceService::new(session_ctx.as_ref().clone(), "traces".to_string())
            .with_max_search_limit(limits.max_search_limit);
        let profile_service = ProfileService::new(session_ctx.as_ref().clone())
            .with_max_search_limit(limits.max_search_limit);
        let logs_service = LogsService::new(session_ctx.as_ref().clone());
        let metrics_service = MetricsService::new(session_ctx.as_ref().clone());
        let ir_service = IrService::new(session_ctx.as_ref().clone());

        Ok(Self {
            _flight_transport: flight_transport,
            session_ctx,
            trace_service,
            profile_service,
            logs_service,
            metrics_service,
            ir_service,
            iceberg_catalog: Some(iceberg_catalog),
            limits,
            query_permits: dashmap::DashMap::new(),
            catalog_manager: Some(catalog_manager),
            registered_tenants,
            tenant_reg_locks: dashmap::DashMap::new(),
            session_cache: dashmap::DashMap::new(),
        })
    }

    /// Ensure the DataFusion catalog for `tenant_slug` is registered in the
    /// running session, resolving it from the tenant registry on demand.
    ///
    /// This makes a tenant created after startup (e.g. via the admin API)
    /// queryable on its first query with no restart. Registration is
    /// idempotent and guarded so that concurrent first-queries for the same
    /// tenant register exactly once. A tenant that is not found in the registry
    /// is left unregistered (the query then resolves nothing, as before).
    async fn ensure_tenant_registered(&self, tenant_slug: &str) -> anyhow::Result<()> {
        // Fast path: already registered in the session.
        if self.session_ctx.catalog(tenant_slug).is_some() {
            return Ok(());
        }
        let Some(catalog_manager) = &self.catalog_manager else {
            return Ok(());
        };

        // Serialize only same-tenant first-queries via a per-tenant lock, so
        // distinct tenants register concurrently. The DashMap ref is dropped
        // before awaiting the lock (never held across an await point).
        let lock = self
            .tenant_reg_locks
            .entry(tenant_slug.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        if self.registered_tenants.contains(tenant_slug)
            || self.session_ctx.catalog(tenant_slug).is_some()
        {
            return Ok(());
        }

        let Some(tenant) = catalog_manager
            .resolve_tenant_by_slug(tenant_slug)
            .await
            .with_context(|| format!("Failed to resolve tenant '{tenant_slug}' from registry"))?
        else {
            // Unknown tenant — nothing to register; let the query proceed.
            return Ok(());
        };

        // Register object stores for the tenant's datasets (deduplicated by the
        // runtime env; re-registering the same URL is harmless).
        for dataset in &tenant.datasets {
            register_dataset_object_store(
                &self.session_ctx,
                &dataset.storage_dsn,
                &tenant.id,
                &dataset.id,
            )?;
        }

        let iceberg_catalog = match &self.iceberg_catalog {
            Some(c) => c.clone(),
            None => return Ok(()),
        };
        let tenant_catalog = TenantCatalog {
            tenant_slug: tenant.slug.clone(),
            catalog: iceberg_catalog,
        };
        self.session_ctx
            .register_catalog(&tenant.slug, Arc::new(tenant_catalog));
        self.registered_tenants.insert(tenant.slug.clone());
        tracing::info!(
            catalog = %tenant.slug,
            tenant_id = %tenant.id,
            "Registered DataFusion catalog on demand"
        );
        Ok(())
    }

    /// Reserve a concurrent-query slot for `tenant`, or reject with
    /// RESOURCE_EXHAUSTED when the tenant is already at its cap. Returns
    /// `None` (no permit needed) when no cap is configured.
    #[allow(clippy::result_large_err)]
    fn try_acquire_query_permit(
        &self,
        tenant: &str,
    ) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, Status> {
        let Some(cap) = self.limits.max_concurrent_queries_per_tenant else {
            return Ok(None);
        };
        let semaphore = self
            .query_permits
            .entry(tenant.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(cap)))
            .clone();
        match semaphore.try_acquire_owned() {
            Ok(permit) => Ok(Some(permit)),
            Err(_) => {
                tracing::warn!(
                    tenant_id = %tenant,
                    limit = cap,
                    "Rejecting query: tenant is at its concurrent-query limit"
                );
                Err(Status::resource_exhausted(format!(
                    "tenant '{tenant}' has reached its concurrent-query limit ({cap}); retry later"
                )))
            }
        }
    }

    /// Parse ticket content to determine query type and parameters
    #[allow(clippy::result_large_err)]
    fn parse_ticket(&self, ticket_content: &str) -> Result<TicketRequest, Status> {
        // Format: find_trace:{tenant_slug}:{dataset_slug}:{trace_id}[:{start}:{end}]
        // The optional trailing segments are unix-second time hints; either
        // may be empty. Routers only append them when a hint is present, so
        // the short form remains valid.
        if let Some(remainder) = ticket_content.strip_prefix("find_trace:") {
            let parts: Vec<&str> = remainder.splitn(5, ':').collect();
            if parts.len() == 3 || parts.len() == 5 {
                let parse_hint = |name: &str, value: &str| -> Result<Option<i64>, Status> {
                    if value.is_empty() {
                        return Ok(None);
                    }
                    value.parse::<i64>().map(Some).map_err(|_| {
                        Status::invalid_argument(format!(
                            "Invalid find_trace ticket: {name} '{value}' is not a unix timestamp"
                        ))
                    })
                };
                let (start, end) = if parts.len() == 5 {
                    (parse_hint("start", parts[3])?, parse_hint("end", parts[4])?)
                } else {
                    (None, None)
                };
                tracing::info!(
                    tenant_slug = %parts[0],
                    dataset_slug = %parts[1],
                    trace_id = %parts[2],
                    start = ?start,
                    end = ?end,
                    "Parsing find_trace ticket"
                );
                return Ok(TicketRequest::FindTrace {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    trace_id: parts[2].to_string(),
                    start,
                    end,
                });
            } else {
                return Err(Status::invalid_argument(
                    "Invalid find_trace ticket format. Expected: find_trace:tenant_slug:dataset_slug:trace_id[:start:end]",
                ));
            }
        }

        // New format: search_traces:{tenant_slug}:{dataset_slug}:{search_params_json}
        if let Some(remainder) = ticket_content.strip_prefix("search_traces:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() == 3 {
                tracing::info!(
                    tenant_slug = %parts[0],
                    dataset_slug = %parts[1],
                    params = %parts[2],
                    "Parsing search_traces ticket"
                );
                let params: SearchQueryParams = serde_json::from_str(parts[2]).map_err(|e| {
                    Status::invalid_argument(format!("Invalid search parameters: {e}"))
                })?;
                return Ok(TicketRequest::SearchTraces {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    params,
                });
            } else {
                return Err(Status::invalid_argument(
                    "Invalid search_traces ticket format. Expected: search_traces:tenant_slug:dataset_slug:params",
                ));
            }
        }

        if let Some(remainder) = ticket_content.strip_prefix("find_profile:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() == 3 {
                return Ok(TicketRequest::FindProfile {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    profile_id: parts[2].to_string(),
                });
            }
            return Err(Status::invalid_argument(
                "Invalid find_profile ticket format. Expected: find_profile:tenant_slug:dataset_slug:profile_id",
            ));
        }

        if let Some(remainder) = ticket_content.strip_prefix("search_profiles:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() == 3 {
                let params: ProfileSearchParams = serde_json::from_str(parts[2]).map_err(|e| {
                    Status::invalid_argument(format!("Invalid profile search parameters: {e}"))
                })?;
                return Ok(TicketRequest::SearchProfiles {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    params,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid search_profiles ticket format. Expected: search_profiles:tenant_slug:dataset_slug:params",
            ));
        }

        if let Some(remainder) = ticket_content.strip_prefix("profile_types:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() >= 2 {
                let params = parse_discovery_params(parts.get(2).copied())?;
                return Ok(TicketRequest::ProfileTypes {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    params,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid profile_types ticket format. Expected: profile_types:tenant_slug:dataset_slug[:params]",
            ));
        }

        if let Some(remainder) = ticket_content.strip_prefix("label_names:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() >= 2 {
                let params = parse_discovery_params(parts.get(2).copied())?;
                return Ok(TicketRequest::ProfileLabelNames {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    params,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid label_names ticket format. Expected: label_names:tenant_slug:dataset_slug[:params]",
            ));
        }

        if let Some(remainder) = ticket_content.strip_prefix("profile_flamegraph:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() == 3 {
                let params: ProfileSearchParams = serde_json::from_str(parts[2]).map_err(|e| {
                    Status::invalid_argument(format!("Invalid flamegraph parameters: {e}"))
                })?;
                return Ok(TicketRequest::ProfileFlamegraph {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    params,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid profile_flamegraph ticket format. Expected: profile_flamegraph:tenant_slug:dataset_slug:params",
            ));
        }

        if let Some(remainder) = ticket_content.strip_prefix("profile_diff:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() == 3 {
                let params: ProfileDiffParams = serde_json::from_str(parts[2]).map_err(|e| {
                    Status::invalid_argument(format!("Invalid profile diff parameters: {e}"))
                })?;
                return Ok(TicketRequest::ProfileDiff {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    params,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid profile_diff ticket format. Expected: profile_diff:tenant_slug:dataset_slug:params",
            ));
        }

        if let Some(remainder) = ticket_content.strip_prefix("label_values:") {
            let parts: Vec<&str> = remainder.splitn(4, ':').collect();
            if parts.len() >= 3 {
                let params = parse_discovery_params(parts.get(3).copied())?;
                return Ok(TicketRequest::ProfileLabelValues {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    label_name: parts[2].to_string(),
                    params,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid label_values ticket format. Expected: label_values:tenant_slug:dataset_slug:label_name[:params]",
            ));
        }

        if let Some(remainder) = ticket_content.strip_prefix("profiles_by_trace:") {
            let parts: Vec<&str> = remainder.splitn(4, ':').collect();
            if parts.len() >= 3 {
                return Ok(TicketRequest::ProfilesByTrace {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    trace_id: parts[2].to_string(),
                    span_id: parts
                        .get(3)
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string()),
                });
            }
            return Err(Status::invalid_argument(
                "Invalid profiles_by_trace ticket format. Expected: profiles_by_trace:tenant_slug:dataset_slug:trace_id[:span_id]",
            ));
        }

        if let Some(remainder) = ticket_content.strip_prefix("sql_profiles:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() == 3 {
                let sql = parts[2].trim();
                let lowered = sql.trim_start().to_ascii_lowercase();
                // Read-only entry point: only SELECT/WITH statements.
                if !(lowered.starts_with("select") || lowered.starts_with("with")) {
                    return Err(Status::invalid_argument(
                        "sql_profiles only accepts SELECT or WITH statements",
                    ));
                }
                return Ok(TicketRequest::SqlProfiles {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    sql: sql.to_string(),
                });
            }
            return Err(Status::invalid_argument(
                "Invalid sql_profiles ticket format. Expected: sql_profiles:tenant_slug:dataset_slug:sql",
            ));
        }

        // LogQL log query: query_logs:{tenant}:{dataset}:{json LogQueryParams}
        if let Some(remainder) = ticket_content.strip_prefix("query_logs:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() == 3 {
                let params: LogQueryParams = serde_json::from_str(parts[2]).map_err(|e| {
                    Status::invalid_argument(format!("Invalid query_logs parameters: {e}"))
                })?;
                return Ok(TicketRequest::QueryLogs {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    params,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid query_logs ticket format. Expected: query_logs:tenant:dataset:{json}",
            ));
        }

        // Native Query IR: query_ir:{tenant}:{dataset}:{json IrQueryParams}
        if let Some(remainder) = ticket_content.strip_prefix("query_ir:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() == 3 {
                let params: IrQueryParams = serde_json::from_str(parts[2]).map_err(|e| {
                    Status::invalid_argument(format!("Invalid query_ir parameters: {e}"))
                })?;
                return Ok(TicketRequest::QueryIr {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    params,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid query_ir ticket format. Expected: query_ir:tenant:dataset:{json}",
            ));
        }

        // Label names: query_logs_labels:{tenant}:{dataset}:{start}:{end}
        if let Some(remainder) = ticket_content.strip_prefix("query_logs_labels:") {
            let parts: Vec<&str> = remainder.splitn(4, ':').collect();
            if parts.len() == 4 {
                let (start, end) = parse_ns_window(parts[2], parts[3])?;
                return Ok(TicketRequest::QueryLogsLabels {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    start,
                    end,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid query_logs_labels ticket format. Expected: query_logs_labels:tenant:dataset:start:end",
            ));
        }

        // Label values: query_logs_label_values:{tenant}:{dataset}:{label}:{start}:{end}
        if let Some(remainder) = ticket_content.strip_prefix("query_logs_label_values:") {
            let parts: Vec<&str> = remainder.splitn(5, ':').collect();
            if parts.len() == 5 {
                let (start, end) = parse_ns_window(parts[3], parts[4])?;
                return Ok(TicketRequest::QueryLogsLabelValues {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    label: parts[2].to_string(),
                    start,
                    end,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid query_logs_label_values ticket format. Expected: query_logs_label_values:tenant:dataset:label:start:end",
            ));
        }

        // Series: query_logs_series:{tenant}:{dataset}:{json LogSeriesParams}
        if let Some(remainder) = ticket_content.strip_prefix("query_logs_series:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() == 3 {
                let params: LogSeriesParams = serde_json::from_str(parts[2]).map_err(|e| {
                    Status::invalid_argument(format!("Invalid query_logs_series parameters: {e}"))
                })?;
                return Ok(TicketRequest::QueryLogsSeries {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    params,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid query_logs_series ticket format. Expected: query_logs_series:tenant:dataset:{json}",
            ));
        }

        // Field discovery:
        // query_logs_detected_fields:{tenant}:{dataset}:{json DetectedFieldsParams}
        if let Some(remainder) = ticket_content.strip_prefix("query_logs_detected_fields:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() == 3 {
                let params: DetectedFieldsParams = serde_json::from_str(parts[2]).map_err(|e| {
                    Status::invalid_argument(format!(
                        "Invalid query_logs_detected_fields parameters: {e}"
                    ))
                })?;
                return Ok(TicketRequest::QueryLogsDetectedFields {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    params,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid query_logs_detected_fields ticket format. Expected: query_logs_detected_fields:tenant:dataset:{json}",
            ));
        }

        // LogQL metric query: query_metric:{tenant}:{dataset}:{json MetricQueryParams}
        if let Some(remainder) = ticket_content.strip_prefix("query_metric:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() == 3 {
                let params: MetricQueryParams = serde_json::from_str(parts[2]).map_err(|e| {
                    Status::invalid_argument(format!("Invalid query_metric parameters: {e}"))
                })?;
                return Ok(TicketRequest::QueryMetric {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    params,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid query_metric ticket format. Expected: query_metric:tenant:dataset:{json}",
            ));
        }

        // PromQL query: query_promql:{tenant}:{dataset}:{json PromQlQueryParams}
        if let Some(remainder) = ticket_content.strip_prefix("query_promql:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() == 3 {
                let params: PromQlQueryParams = serde_json::from_str(parts[2]).map_err(|e| {
                    Status::invalid_argument(format!("Invalid query_promql parameters: {e}"))
                })?;
                return Ok(TicketRequest::QueryPromql {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    params,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid query_promql ticket format. Expected: query_promql:tenant:dataset:{json}",
            ));
        }

        // Metric label names: query_metric_labels:{tenant}:{dataset}:{start}:{end}
        if let Some(remainder) = ticket_content.strip_prefix("query_metric_labels:") {
            let parts: Vec<&str> = remainder.splitn(4, ':').collect();
            if parts.len() == 4 {
                let (start, end) = parse_ns_window(parts[2], parts[3])?;
                return Ok(TicketRequest::QueryMetricLabels {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    start,
                    end,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid query_metric_labels ticket format. Expected: query_metric_labels:tenant:dataset:start:end",
            ));
        }

        // Metric label values: query_metric_label_values:{tenant}:{dataset}:{label}:{start}:{end}
        if let Some(remainder) = ticket_content.strip_prefix("query_metric_label_values:") {
            let parts: Vec<&str> = remainder.splitn(5, ':').collect();
            if parts.len() == 5 {
                let (start, end) = parse_ns_window(parts[3], parts[4])?;
                return Ok(TicketRequest::QueryMetricLabelValues {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    label: parts[2].to_string(),
                    start,
                    end,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid query_metric_label_values ticket format. Expected: query_metric_label_values:tenant:dataset:label:start:end",
            ));
        }

        // Metric series: query_metric_series:{tenant}:{dataset}:{json MetricSeriesParams}
        if let Some(remainder) = ticket_content.strip_prefix("query_metric_series:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() == 3 {
                let params: MetricSeriesParams = serde_json::from_str(parts[2]).map_err(|e| {
                    Status::invalid_argument(format!("Invalid query_metric_series parameters: {e}"))
                })?;
                return Ok(TicketRequest::QueryMetricSeries {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    params,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid query_metric_series ticket format. Expected: query_metric_series:tenant:dataset:{json}",
            ));
        }

        // Trace tag names: trace_tags:{tenant}:{dataset}:{json TraceTagsParams}
        if let Some(remainder) = ticket_content.strip_prefix("trace_tags:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() == 3 {
                let params: TraceTagsParams = serde_json::from_str(parts[2]).map_err(|e| {
                    Status::invalid_argument(format!("Invalid trace_tags parameters: {e}"))
                })?;
                return Ok(TicketRequest::TraceTags {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    params,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid trace_tags ticket format. Expected: trace_tags:tenant:dataset:{json}",
            ));
        }

        // Trace tag values: trace_tag_values:{tenant}:{dataset}:{tag}:{json TraceTagValuesParams}
        if let Some(remainder) = ticket_content.strip_prefix("trace_tag_values:") {
            let parts: Vec<&str> = remainder.splitn(4, ':').collect();
            if parts.len() == 4 {
                let params: TraceTagValuesParams = serde_json::from_str(parts[3]).map_err(|e| {
                    Status::invalid_argument(format!("Invalid trace_tag_values parameters: {e}"))
                })?;
                return Ok(TicketRequest::TraceTagValues {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    tag: parts[2].to_string(),
                    params,
                });
            }
            return Err(Status::invalid_argument(
                "Invalid trace_tag_values ticket format. Expected: trace_tag_values:tenant:dataset:tag:{json}",
            ));
        }

        // Fall back to raw SQL query
        Ok(TicketRequest::SqlQuery {
            sql: ticket_content.to_string(),
        })
    }

    /// Convert internal trace model to Arrow RecordBatches
    async fn trace_to_record_batches(
        &self,
        trace: &common::model::trace::Trace,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        use datafusion::arrow::array::{BooleanArray, StringArray, UInt64Array};

        // Create schema matching the span batch schema
        let schema = create_span_batch_schema();

        // Collect all spans from the trace (including nested children),
        // iteratively so deep hierarchies cannot overflow the stack. Kept as
        // borrows: nothing here needs an owned, child-stripped copy of each
        // span, only read access to its fields.
        let mut all_spans: Vec<&common::model::span::Span> = Vec::new();
        let mut stack: Vec<&common::model::span::Span> = trace.spans.iter().rev().collect();
        while let Some(span) = stack.pop() {
            stack.extend(span.children.iter().rev());
            all_spans.push(span);
        }

        if all_spans.is_empty() {
            return Ok(vec![]);
        }

        // Build arrays for each column (order must match create_span_batch_schema)
        let span_count = all_spans.len();
        let mut trace_ids = Vec::with_capacity(span_count);
        let mut span_ids = Vec::with_capacity(span_count);
        let mut parent_span_ids = Vec::with_capacity(span_count);
        let mut statuses = Vec::with_capacity(span_count);
        let mut is_roots = Vec::with_capacity(span_count);
        let mut names = Vec::with_capacity(span_count);
        let mut service_names = Vec::with_capacity(span_count);
        let mut span_kinds = Vec::with_capacity(span_count);
        let mut start_times = Vec::with_capacity(span_count);
        let mut duration_nanos = Vec::with_capacity(span_count);
        let mut span_attributes_json = Vec::with_capacity(span_count);
        let mut resource_json = Vec::with_capacity(span_count);
        let mut events_json = Vec::with_capacity(span_count);

        for span in &all_spans {
            trace_ids.push(span.trace_id.clone());
            span_ids.push(span.span_id.clone());
            parent_span_ids.push(span.parent_span_id.clone());
            statuses.push(format!("{:?}", span.status));
            is_roots.push(span.is_root);
            names.push(span.name.clone());
            service_names.push(span.service_name.clone());
            span_kinds.push(format!("{:?}", span.span_kind));
            start_times.push(span.start_time_unix_nano);
            duration_nanos.push(span.duration_nano);
            span_attributes_json.push(serde_json::to_string(&span.attributes).unwrap_or_default());
            resource_json.push(serde_json::to_string(&span.resource).unwrap_or_default());
            events_json.push(common::model::span::serialize_span_events(&span.events));
        }

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(trace_ids)),
                Arc::new(StringArray::from(span_ids)),
                Arc::new(StringArray::from(parent_span_ids)),
                Arc::new(StringArray::from(statuses)),
                Arc::new(BooleanArray::from(is_roots)),
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(service_names)),
                Arc::new(StringArray::from(span_kinds)),
                Arc::new(UInt64Array::from(start_times)),
                Arc::new(UInt64Array::from(duration_nanos)),
                Arc::new(StringArray::from(span_attributes_json)),
                Arc::new(StringArray::from(resource_json)),
                Arc::new(StringArray::from(events_json)),
            ],
        )?;

        Ok(vec![batch])
    }

    /// Build (or reuse) a per-tenant/dataset SessionContext with tenant/dataset
    /// defaults.
    ///
    /// Per-request session state used to be rebuilt from scratch on every
    /// call via `SessionStateBuilder::new_from_existing`, which clones the
    /// full function registry (scalar/aggregate/window UDFs, table
    /// functions, file formats) — expensive to do on every single request.
    /// Instead, the derived `SessionContext` is built once per distinct
    /// `(tenant_slug, dataset_slug)` pair and cached; subsequent calls for
    /// the same pair return a cheap `Arc::clone` of the cached context.
    ///
    /// This is safe to share across *concurrent* requests for the *same*
    /// tenant+dataset because they have identical defaults — the original
    /// bug this design avoids (see below) was only about *different*
    /// tenants observing each other's defaults, which per-tenant caching
    /// still fully prevents. Registered catalogs and the runtime
    /// environment are shared through Arcs inside the DataFusion
    /// `SessionState` (`catalog_list` is `Arc<dyn CatalogProviderList>`
    /// backed by an internally-mutable `DashMap`), so a cached context still
    /// observes catalogs/tables registered on the shared `session_ctx`
    /// *after* the cache entry was created (e.g. tenants registered lazily
    /// by `ensure_tenant_registered`).
    ///
    /// Mutating the shared context directly (the previous `SET
    /// datafusion.catalog.default_catalog` approach) let two concurrent
    /// queries for different tenants execute against the wrong tenant's
    /// catalog; per-(tenant, dataset) cached contexts keep that isolation
    /// while avoiding a full state rebuild per request.
    fn session_for_request(
        &self,
        tenant_slug: Option<&str>,
        dataset_slug: Option<&str>,
    ) -> Arc<SessionContext> {
        let Some(tenant) = tenant_slug else {
            // No tenant to scope defaults to: return a fresh context sharing
            // the shared state's Arcs. Not cached since there is no stable
            // cache key and this path carries no per-tenant defaults to
            // amortize.
            return Arc::new(SessionContext::new_with_state(self.session_ctx.state()));
        };

        let dataset = dataset_slug.unwrap_or("default");
        let cache_key = (tenant.to_string(), dataset.to_string());

        if let Some(cached) = self.session_cache.get(&cache_key) {
            return Arc::clone(cached.value());
        }

        let state = self.session_ctx.state();

        // create_default_catalog_and_schema must be off: with it on,
        // SessionStateBuilder::build() would register a fresh EMPTY catalog
        // under the tenant's name on the shared catalog list, shadowing the
        // real tenant catalog for every session.
        let mut config = state
            .config()
            .clone()
            .with_create_default_catalog_and_schema(false);
        let options = config.options_mut();
        options.catalog.default_catalog = tenant.to_string();
        options.catalog.default_schema = dataset.to_string();

        let state = SessionStateBuilder::new_from_existing(state)
            .with_config(config)
            .build();
        let ctx = Arc::new(SessionContext::new_with_state(state));

        // On a race, both racers build a context and one insert wins; the
        // loser's Arc is simply dropped. Both are behaviorally identical, so
        // returning whichever ended up in the cache (not necessarily the
        // one this call just built) is correct.
        self.session_cache.entry(cache_key).or_insert(ctx).clone()
    }

    /// Execute a SQL query and return results as RecordBatches
    async fn execute_query(
        &self,
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        // Literals sanitized before the text reaches logs or spans — raw
        // SQL can carry PII in string/numeric literals.
        let sanitized = common::self_monitoring::sanitize::sanitize_query_text(sql);
        tracing::info!(sql = %sanitized, "Executing query");

        let df = ctx
            .sql(sql)
            .instrument(tracing::info_span!(
                "signaldb.query.plan",
                signaldb.query.text = %sanitized,
            ))
            .await?;
        // Cap the number of rows a raw SQL query can materialize; the
        // client controls the SQL, so an unbounded SELECT could otherwise
        // buffer arbitrarily many rows in memory.
        let df = df.limit(0, Some(self.limits.max_sql_rows))?;
        let exec_span = tracing::info_span!(
            "signaldb.query.execute",
            signaldb.query.rows = tracing::field::Empty,
            signaldb.query.batches = tracing::field::Empty,
        );
        let batches = df.collect().instrument(exec_span.clone()).await?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        exec_span.record("signaldb.query.rows", rows as i64);
        exec_span.record("signaldb.query.batches", batches.len() as i64);

        Ok(batches)
    }

    /// Execute a query against the object store
    async fn execute_distributed_query(
        &self,
        ctx: &SessionContext,
        query: &str,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        // Query only the object store - data at rest
        // Writers are responsible for persisting data to object store
        // Querier should not depend on or know about writers

        match self.execute_query(ctx, query).await {
            Ok(batches) => {
                tracing::debug!(
                    batch_count = batches.len(),
                    "Retrieved batches from object store"
                );
                Ok(batches)
            }
            Err(e) => {
                tracing::error!(error = %e, "Error querying object store");
                Err(e)
            }
        }
    }
}

#[tonic::async_trait]
impl FlightService for QuerierFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let resp = HandshakeResponse {
            protocol_version: 0,
            payload: Bytes::new(),
        };
        let out = stream::once(async move { Ok(resp) }).boxed();
        Ok(Response::new(out))
    }

    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;

    async fn list_flights(
        &self,
        _request: Request<arrow_flight::Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        // Return available query endpoints
        let flights = vec![FlightInfo {
            schema: Bytes::new(),
            flight_descriptor: Some(FlightDescriptor {
                r#type: arrow_flight::flight_descriptor::DescriptorType::Cmd as i32,
                cmd: b"SELECT * FROM traces".to_vec().into(),
                path: vec![],
            }),
            endpoint: vec![],
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: Bytes::new(),
        }];

        let out = stream::iter(flights.into_iter().map(Ok)).boxed();
        Ok(Response::new(out))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info not supported"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();

        // Return appropriate schema based on the descriptor
        let schema =
            if descriptor.cmd == b"traces".as_slice() || descriptor.cmd.starts_with(b"SELECT") {
                create_span_batch_schema()
            } else {
                return Err(Status::not_found("Unknown schema"));
            };

        // Serialize schema to Flight format
        let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
        let mut dict_tracker = datafusion::arrow::ipc::writer::DictionaryTracker::new(false);
        let data_gen = datafusion::arrow::ipc::writer::IpcDataGenerator::default();
        let schema_bytes = data_gen
            .schema_to_bytes_with_dictionary_tracker(&schema, &mut dict_tracker, &options)
            .ipc_message;

        let schema_result = SchemaResult {
            schema: schema_bytes.into(),
        };

        Ok(Response::new(schema_result))
    }

    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;

    async fn do_put(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        // Querier doesn't accept writes - this is read-only
        Err(Status::unimplemented("Querier is read-only"))
    }

    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let metadata = request.metadata().clone();
        let remote_addr = request.remote_addr();
        // Tenant-scoped caller identity, inserted by the Flight auth
        // interceptor. None for internal-service callers and for
        // deployments without Flight auth configured.
        let caller_tenant = request
            .extensions()
            .get::<common::auth::TenantContext>()
            .cloned();
        let ticket = request.into_inner();
        let ticket_content = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid ticket: {e}")))?;

        // Anti-loop guard (#760): executing a query over the _system
        // tenant's own telemetry must not emit logs/spans that get exported
        // and re-ingested as _system telemetry. The tenant is the
        // authenticated caller when present, else the one named in the
        // ticket (every prefixed ticket form is `op:tenant_slug:...`, so a
        // cheap segment scan suffices before parsing — parsing itself
        // logs), else (raw SQL from internal callers) the x-tenant-id
        // header.
        let suppress = caller_tenant
            .as_ref()
            .map(|ctx| ctx.tenant_id.as_str())
            .or_else(|| metadata.get("x-tenant-id").and_then(|v| v.to_str().ok()))
            .is_some_and(common::self_monitoring::is_self_monitoring_tenant)
            || ticket_content
                .split(':')
                .nth(1)
                .is_some_and(common::self_monitoring::is_self_monitoring_tenant);

        // Process within a semconv RPC SERVER span that joins the caller's
        // distributed trace (e.g. Router -> Querier); the parent must be set
        // before the span is first entered. The span is created under the
        // suppression scope so it is itself not exported for _system queries.
        //
        // The ticket verb disambiguates the span name only when it looks
        // like a verb: raw-SQL tickets have no `op:` prefix, so their first
        // `:`-segment is query text and must stay out of the span name.
        let span_ticket_verb =
            common::self_monitoring::spans::ticket_verb(&ticket_content).map(str::to_owned);
        let make_span = || {
            common::self_monitoring::spans::rpc_server_span(
                common::self_monitoring::spans::FLIGHT_DO_GET,
                span_ticket_verb.as_deref(),
            )
        };
        let span = if suppress {
            common::self_monitoring::suppress_self_telemetry_sync(make_span)
        } else {
            make_span()
        };
        common::flight::trace_context::set_parent_from_metadata(&span, &metadata);
        common::self_monitoring::spans::record_network_peer_from_addr(&span, remote_addr);
        // Boxed: the state machine is large, and nesting it by value inside
        // the suppression wrapper overflows rustc's layout-query depth.
        common::self_monitoring::maybe_suppress_self_telemetry(
            suppress,
            Box::pin(
                async move {
                    // Single error boundary for the whole request. Every `?` and
                    // `return Err` in the body below flattens into a transport
                    // `Status` that the router strips down to a bare HTTP code,
                    // losing the reason. Recording it here — inside the
                    // instrumented DoGet server span (the current span) — as an
                    // OTel `exception` event is the one place that cause survives
                    // for after-the-fact diagnosis, and covers every internal
                    // failure path (ticket parsing, tenant checks, execution,
                    // conversion) at once.
                    let result: Result<Response<Self::DoGetStream>, Status> = async move {
                        // Log only the ticket verb at info — the full ticket body
                        // (e.g. a `query_ir` IR document) can carry query literals
                        // with PII, so it stays at debug.
                        let ticket_verb = ticket_content.split(':').next().unwrap_or("");
                        tracing::info!(ticket_verb = %ticket_verb, "Processing Flight ticket");
                        tracing::debug!(ticket = %ticket_content, "Flight ticket body");

                        // Parse ticket to determine request type
                        let ticket_request = self.parse_ticket(&ticket_content)?;

                        // The tenant named in the ticket, when it names one (raw
                        // SQL tickets carry the tenant out of band).
                        let ticket_tenant_slug: Option<String> = match &ticket_request {
                            TicketRequest::FindTrace { tenant_slug, .. }
                            | TicketRequest::SearchTraces { tenant_slug, .. }
                            | TicketRequest::FindProfile { tenant_slug, .. }
                            | TicketRequest::SearchProfiles { tenant_slug, .. }
                            | TicketRequest::ProfileTypes { tenant_slug, .. }
                            | TicketRequest::ProfileLabelNames { tenant_slug, .. }
                            | TicketRequest::ProfileLabelValues { tenant_slug, .. }
                            | TicketRequest::ProfileFlamegraph { tenant_slug, .. }
                            | TicketRequest::ProfileDiff { tenant_slug, .. }
                            | TicketRequest::SqlProfiles { tenant_slug, .. }
                            | TicketRequest::ProfilesByTrace { tenant_slug, .. }
                            | TicketRequest::QueryLogs { tenant_slug, .. }
                            | TicketRequest::QueryIr { tenant_slug, .. }
                            | TicketRequest::QueryLogsLabels { tenant_slug, .. }
                            | TicketRequest::QueryLogsLabelValues { tenant_slug, .. }
                            | TicketRequest::QueryLogsSeries { tenant_slug, .. }
                            | TicketRequest::QueryLogsDetectedFields { tenant_slug, .. }
                            | TicketRequest::QueryMetric { tenant_slug, .. }
                            | TicketRequest::QueryPromql { tenant_slug, .. }
                            | TicketRequest::QueryMetricLabels { tenant_slug, .. }
                            | TicketRequest::QueryMetricLabelValues { tenant_slug, .. }
                            | TicketRequest::QueryMetricSeries { tenant_slug, .. }
                            | TicketRequest::TraceTags { tenant_slug, .. }
                            | TicketRequest::TraceTagValues { tenant_slug, .. } => {
                                Some(tenant_slug.clone())
                            }
                            TicketRequest::SqlQuery { .. } => None,
                        };

                        // Tenant-scoped callers may only touch their own tenant's data,
                        // regardless of what the ticket claims.
                        if let Some(ctx) = &caller_tenant
                            && let Some(ticket_tenant) = ticket_tenant_slug.as_deref()
                            && ticket_tenant != ctx.tenant_slug
                        {
                            tracing::warn!(
                                caller_tenant = %ctx.tenant_slug,
                                ticket_tenant = %ticket_tenant,
                                "Rejecting cross-tenant Flight ticket"
                            );
                            return Err(Status::permission_denied(
                                "ticket tenant does not match authenticated tenant",
                            ));
                        }

                        // Lazily register the tenant's catalog if it was created
                        // after startup (e.g. via the admin API), so it is queryable
                        // with no restart. Bound by the query timeout so a stuck
                        // registration (slow catalog/object-store) cannot hang the
                        // request unbounded.
                        if let Some(slug) = ticket_tenant_slug.as_deref() {
                            tokio::time::timeout(
                                self.limits.query_timeout,
                                self.ensure_tenant_registered(slug),
                            )
                            .await
                            .map_err(|_| {
                                Status::deadline_exceeded("tenant catalog registration timed out")
                            })?
                            .map_err(|e| {
                                Status::internal(format!("Failed to register tenant catalog: {e}"))
                            })?;
                        }

                        let query_type = match &ticket_request {
                            TicketRequest::FindTrace { .. } => "trace_by_id",
                            TicketRequest::SearchTraces { .. } => "trace_search",
                            TicketRequest::FindProfile { .. } => "profile_by_id",
                            TicketRequest::SearchProfiles { .. } => "profile_search",
                            TicketRequest::ProfileTypes { .. } => "profile_types",
                            TicketRequest::ProfileLabelNames { .. } => "profile_label_names",
                            TicketRequest::ProfileLabelValues { .. } => "profile_label_values",
                            TicketRequest::ProfileFlamegraph { .. } => "profile_flamegraph",
                            TicketRequest::ProfileDiff { .. } => "profile_diff",
                            TicketRequest::SqlProfiles { .. } => "sql_profiles",
                            TicketRequest::ProfilesByTrace { .. } => "profiles_by_trace",
                            TicketRequest::QueryLogs { .. } => "query_logs",
                            TicketRequest::QueryIr { .. } => "query_ir",
                            TicketRequest::QueryLogsLabels { .. } => "query_logs_labels",
                            TicketRequest::QueryLogsLabelValues { .. } => "query_logs_label_values",
                            TicketRequest::QueryLogsSeries { .. } => "query_logs_series",
                            TicketRequest::QueryLogsDetectedFields { .. } => {
                                "query_logs_detected_fields"
                            }
                            TicketRequest::QueryMetric { .. } => "query_metric",
                            TicketRequest::QueryPromql { .. } => "query_promql",
                            TicketRequest::QueryMetricLabels { .. } => "query_metric_labels",
                            TicketRequest::QueryMetricLabelValues { .. } => {
                                "query_metric_label_values"
                            }
                            TicketRequest::QueryMetricSeries { .. } => "query_metric_series",
                            TicketRequest::TraceTags { .. } => "trace_tags",
                            TicketRequest::TraceTagValues { .. } => "trace_tag_values",
                            TicketRequest::SqlQuery { .. } => "sql",
                        };

                        // Per-tenant concurrent-query cap. The tenant is the
                        // authenticated caller when present, else the tenant named in
                        // the ticket (internal callers proxy on behalf of tenants).
                        // Internal raw-SQL callers carry no tenant and are exempt.
                        let permit_tenant = caller_tenant
                            .as_ref()
                            .map(|ctx| ctx.tenant_id.clone())
                            .or_else(|| ticket_tenant_slug.clone());
                        // Held until the query's batches are fully computed.
                        let _query_permit = match &permit_tenant {
                            Some(tenant) => self.try_acquire_query_permit(tenant)?,
                            None => None,
                        };
                        let query_start = std::time::Instant::now();
                        let query_future = async {
                            Ok(match ticket_request {
                                TicketRequest::FindTrace {
                                    tenant_slug,
                                    dataset_slug,
                                    trace_id,
                                    start,
                                    end,
                                } => {
                                    tracing::info!(
                                        tenant_slug = %tenant_slug,
                                        dataset_slug = %dataset_slug,
                                        trace_id = %trace_id,
                                        start = ?start,
                                        end = ?end,
                                        "Executing find_trace"
                                    );

                                    let params = FindTraceByIdParams {
                                        trace_id,
                                        start,
                                        end,
                                    };

                                    match self
                                        .trace_service
                                        .find_by_id_with_tenant(params, &tenant_slug, &dataset_slug)
                                        .await
                                    {
                                        Ok(Some(trace)) => {
                                            tracing::info!(
                                                span_count = trace.spans.len(),
                                                "Found trace"
                                            );
                                            self.trace_to_record_batches(&trace).await.map_err(
                                                |e| {
                                                    Status::internal(format!(
                                                        "Failed to convert trace to batches: {e}"
                                                    ))
                                                },
                                            )?
                                        }
                                        Ok(None) => {
                                            tracing::info!("No trace found");
                                            let e =
                                                crate::query::error::QuerierError::TraceNotFound;
                                            return Err(Status::not_found(e.to_string()));
                                        }
                                        Err(e) => {
                                            tracing::error!(error = ?e, "Error querying trace");
                                            // Mirror the search arm: caller errors are
                                            // surfaced as such instead of a blanket 500.
                                            return Err(trace_error_to_status("Trace query")(e));
                                        }
                                    }
                                }
                                TicketRequest::SearchTraces {
                                    tenant_slug,
                                    dataset_slug,
                                    params,
                                } => {
                                    tracing::info!(
                                        tenant_slug = %tenant_slug,
                                        dataset_slug = %dataset_slug,
                                        params = ?params,
                                        "Executing search_traces"
                                    );

                                    match self
                                        .trace_service
                                        .find_traces_with_tenant(
                                            params,
                                            &tenant_slug,
                                            &dataset_slug,
                                        )
                                        .await
                                    {
                                        Ok(traces) => {
                                            tracing::info!(
                                                trace_count = traces.len(),
                                                "Found traces"
                                            );

                                            let mut all_batches = Vec::new();
                                            for trace in &traces {
                                                let trace_batches = self
                                                .trace_to_record_batches(trace)
                                                .await
                                                .map_err(|e| {
                                                    Status::internal(format!(
                                                        "Failed to convert trace to batches: {e}"
                                                    ))
                                                })?;
                                                all_batches.extend(trace_batches);
                                            }
                                            all_batches
                                        }
                                        Err(e) => {
                                            tracing::error!(error = ?e, "Error searching traces");
                                            // Distinguish caller errors from server
                                            // errors so bad or unsupported selectors
                                            // surface as 400/501, not 500.
                                            return Err(trace_error_to_status("Trace search")(e));
                                        }
                                    }
                                }
                                TicketRequest::FindProfile {
                                    tenant_slug,
                                    dataset_slug,
                                    profile_id,
                                } => {
                                    tracing::info!(
                                        tenant_slug = %tenant_slug,
                                        dataset_slug = %dataset_slug,
                                        profile_id = %profile_id,
                                        "Executing find_profile"
                                    );
                                    let batches = self
                                        .profile_service
                                        .find_by_id_with_tenant(
                                            FindProfileByIdParams { profile_id },
                                            &tenant_slug,
                                            &dataset_slug,
                                        )
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_PROFILES))?;
                                    if batches.is_empty() {
                                        return Err(Status::not_found("Profile not found"));
                                    }
                                    batches
                                }
                                TicketRequest::SearchProfiles {
                                    tenant_slug,
                                    dataset_slug,
                                    params,
                                } => {
                                    tracing::info!(
                                        tenant_slug = %tenant_slug,
                                        dataset_slug = %dataset_slug,
                                        params = ?params,
                                        "Executing search_profiles"
                                    );
                                    self.profile_service
                                        .search_with_tenant(params, &tenant_slug, &dataset_slug)
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_PROFILES))?
                                }
                                TicketRequest::ProfileTypes {
                                    tenant_slug,
                                    dataset_slug,
                                    params,
                                } => {
                                    let types = self
                                        .profile_service
                                        .profile_types_with_tenant(
                                            params,
                                            &tenant_slug,
                                            &dataset_slug,
                                        )
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_PROFILES))?;
                                    vec![strings_to_batch("profile_type", types)?]
                                }
                                TicketRequest::ProfileLabelNames {
                                    tenant_slug,
                                    dataset_slug,
                                    params,
                                } => {
                                    let names = self
                                        .profile_service
                                        .label_names_with_tenant(
                                            params,
                                            &tenant_slug,
                                            &dataset_slug,
                                        )
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_PROFILES))?;
                                    vec![strings_to_batch("label_name", names)?]
                                }
                                TicketRequest::ProfileLabelValues {
                                    tenant_slug,
                                    dataset_slug,
                                    label_name,
                                    params,
                                } => {
                                    let values = self
                                        .profile_service
                                        .label_values_with_tenant(
                                            &label_name,
                                            params,
                                            &tenant_slug,
                                            &dataset_slug,
                                        )
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_PROFILES))?;
                                    vec![strings_to_batch("label_value", values)?]
                                }
                                TicketRequest::ProfileFlamegraph {
                                    tenant_slug,
                                    dataset_slug,
                                    params,
                                } => {
                                    let flamegraph = self
                                        .profile_service
                                        .flamegraph_with_tenant(params, &tenant_slug, &dataset_slug)
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_PROFILES))?;
                                    vec![json_to_batch("flamegraph", &flamegraph)?]
                                }
                                TicketRequest::ProfileDiff {
                                    tenant_slug,
                                    dataset_slug,
                                    params,
                                } => {
                                    let diff = self
                                        .profile_service
                                        .diff_with_tenant(params, &tenant_slug, &dataset_slug)
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_PROFILES))?;
                                    vec![json_to_batch("diff", &diff)?]
                                }
                                TicketRequest::ProfilesByTrace {
                                    tenant_slug,
                                    dataset_slug,
                                    trace_id,
                                    span_id,
                                } => {
                                    tracing::info!(
                                        tenant_slug = %tenant_slug,
                                        dataset_slug = %dataset_slug,
                                        trace_id = %trace_id,
                                        span_id = ?span_id,
                                        "Executing profiles_by_trace"
                                    );
                                    self.profile_service
                                        .find_by_trace_with_tenant(
                                            &trace_id,
                                            span_id.as_deref(),
                                            &tenant_slug,
                                            &dataset_slug,
                                        )
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_PROFILES))?
                                }
                                TicketRequest::QueryLogs {
                                    tenant_slug,
                                    dataset_slug,
                                    params,
                                } => {
                                    tracing::info!(
                                        tenant_slug = %tenant_slug,
                                        dataset_slug = %dataset_slug,
                                        query = %params.query,
                                        "Executing query_logs"
                                    );
                                    self.logs_service
                                        .query_logs(&params, &tenant_slug, &dataset_slug)
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_LOGS))?
                                }
                                TicketRequest::QueryIr {
                                    tenant_slug,
                                    dataset_slug,
                                    params,
                                } => {
                                    tracing::info!(
                                        tenant_slug = %tenant_slug,
                                        dataset_slug = %dataset_slug,
                                        "Executing query_ir"
                                    );
                                    let (batches, _window) = self
                                        .ir_service
                                        .query(&params, &tenant_slug, &dataset_slug)
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_QUERY_IR))?;
                                    batches
                                }
                                TicketRequest::QueryLogsLabels {
                                    tenant_slug,
                                    dataset_slug,
                                    start,
                                    end,
                                } => {
                                    let labels = self
                                        .logs_service
                                        .get_labels(start, end, &tenant_slug, &dataset_slug)
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_LOGS))?;
                                    vec![strings_to_batch("label", labels)?]
                                }
                                TicketRequest::QueryLogsLabelValues {
                                    tenant_slug,
                                    dataset_slug,
                                    label,
                                    start,
                                    end,
                                } => {
                                    let values = self
                                        .logs_service
                                        .get_label_values(
                                            &label,
                                            start,
                                            end,
                                            &tenant_slug,
                                            &dataset_slug,
                                        )
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_LOGS))?;
                                    vec![strings_to_batch("value", values)?]
                                }
                                TicketRequest::QueryLogsSeries {
                                    tenant_slug,
                                    dataset_slug,
                                    params,
                                } => {
                                    let series = self
                                        .logs_service
                                        .get_series(
                                            &params.selector,
                                            params.start,
                                            params.end,
                                            &tenant_slug,
                                            &dataset_slug,
                                        )
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_LOGS))?;
                                    vec![json_to_batch("series", &series)?]
                                }
                                TicketRequest::QueryLogsDetectedFields {
                                    tenant_slug,
                                    dataset_slug,
                                    params,
                                } => {
                                    let fields = self
                                        .logs_service
                                        .detected_fields(&params, &tenant_slug, &dataset_slug)
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_LOGS))?;
                                    vec![json_to_batch("fields", &fields)?]
                                }
                                TicketRequest::QueryMetric {
                                    tenant_slug,
                                    dataset_slug,
                                    params,
                                } => {
                                    tracing::info!(
                                        tenant_slug = %tenant_slug,
                                        dataset_slug = %dataset_slug,
                                        query = %params.query,
                                        "Executing query_metric"
                                    );
                                    self.logs_service
                                        .query_metric(&params, &tenant_slug, &dataset_slug)
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_LOGS))?
                                }
                                TicketRequest::QueryPromql {
                                    tenant_slug,
                                    dataset_slug,
                                    params,
                                } => {
                                    tracing::info!(
                                        tenant_slug = %tenant_slug,
                                        dataset_slug = %dataset_slug,
                                        query = %params.query,
                                        "Executing query_promql"
                                    );
                                    self.metrics_service
                                        .query_range(
                                            &params.query,
                                            params.start,
                                            params.end,
                                            params.step,
                                            &tenant_slug,
                                            &dataset_slug,
                                        )
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_METRICS))?
                                }
                                TicketRequest::QueryMetricLabels {
                                    tenant_slug,
                                    dataset_slug,
                                    start,
                                    end,
                                } => {
                                    let labels = self
                                        .metrics_service
                                        .get_labels(start, end, &tenant_slug, &dataset_slug)
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_METRICS))?;
                                    vec![strings_to_batch("label", labels)?]
                                }
                                TicketRequest::QueryMetricLabelValues {
                                    tenant_slug,
                                    dataset_slug,
                                    label,
                                    start,
                                    end,
                                } => {
                                    let values = self
                                        .metrics_service
                                        .get_label_values(
                                            &label,
                                            start,
                                            end,
                                            &tenant_slug,
                                            &dataset_slug,
                                        )
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_METRICS))?;
                                    vec![strings_to_batch("value", values)?]
                                }
                                TicketRequest::QueryMetricSeries {
                                    tenant_slug,
                                    dataset_slug,
                                    params,
                                } => {
                                    let series = self
                                        .metrics_service
                                        .get_series(
                                            &params.selector,
                                            params.start,
                                            params.end,
                                            &tenant_slug,
                                            &dataset_slug,
                                        )
                                        .await
                                        .map_err(querier_error_to_status(SIGNAL_METRICS))?;
                                    vec![json_to_batch("series", &series)?]
                                }
                                TicketRequest::TraceTags {
                                    tenant_slug,
                                    dataset_slug,
                                    params,
                                } => {
                                    let tags = self
                                        .trace_service
                                        .get_tags(&params, &tenant_slug, &dataset_slug)
                                        .await
                                        .map_err(trace_error_to_status("Trace tag discovery"))?;
                                    vec![json_to_batch("tags", &tags)?]
                                }
                                TicketRequest::TraceTagValues {
                                    tenant_slug,
                                    dataset_slug,
                                    tag,
                                    params,
                                } => {
                                    let values = self
                                        .trace_service
                                        .get_tag_values(
                                            &tag,
                                            params.start,
                                            params.end,
                                            &tenant_slug,
                                            &dataset_slug,
                                        )
                                        .await
                                        .map_err(trace_error_to_status(
                                            "Trace tag value discovery",
                                        ))?;
                                    vec![strings_to_batch("value", values)?]
                                }
                                TicketRequest::SqlProfiles {
                                    tenant_slug,
                                    dataset_slug,
                                    sql,
                                } => {
                                    tracing::info!(
                                        tenant_slug = %tenant_slug,
                                        dataset_slug = %dataset_slug,
                                        sql = %sql,
                                        "Executing sql_profiles"
                                    );
                                    // Pin the session defaults to the ticket's
                                    // tenant/dataset so unqualified table names like
                                    // `profiles` resolve inside the tenant's catalog.
                                    let request_ctx = self.session_for_request(
                                        Some(&tenant_slug),
                                        Some(&dataset_slug),
                                    );
                                    self.execute_distributed_query(&request_ctx, &sql)
                                        .await
                                        .map_err(|e| {
                                            Status::internal(format!(
                                                "Profiles SQL query failed: {e}"
                                            ))
                                        })?
                                }
                                TicketRequest::SqlQuery { sql } => {
                                    // Tenant-scoped callers are pinned to their
                                    // authenticated tenant/dataset; only internal or
                                    // unauthenticated callers may scope via headers.
                                    let (tenant_slug, dataset_slug) = match &caller_tenant {
                                        Some(ctx) => (
                                            Some(ctx.tenant_slug.clone()),
                                            Some(ctx.dataset_slug.clone()),
                                        ),
                                        None => (
                                            metadata
                                                .get("x-tenant-id")
                                                .and_then(|v| v.to_str().ok())
                                                .map(|s| s.to_string()),
                                            metadata
                                                .get("x-dataset-id")
                                                .and_then(|v| v.to_str().ok())
                                                .map(|s| s.to_string()),
                                        ),
                                    };

                                    tracing::info!(
                                        tenant_id = ?tenant_slug,
                                        dataset_id = ?dataset_slug,
                                        sql = %sql,
                                        "Executing SQL query"
                                    );

                                    // Per-request context: tenant/dataset defaults must
                                    // never be applied to the shared session (see
                                    // session_for_request).
                                    let request_ctx = self.session_for_request(
                                        tenant_slug.as_deref(),
                                        dataset_slug.as_deref(),
                                    );

                                    self.execute_distributed_query(&request_ctx, &sql)
                                        .await
                                        .map_err(|e| {
                                            Status::internal(format!("Query execution failed: {e}"))
                                        })?
                                }
                            })
                        };
                        // Bound every query's wall-clock time so a heavy scan cannot
                        // occupy the querier indefinitely.
                        let batches_result: Result<Vec<_>, Status> =
                            match tokio::time::timeout(self.limits.query_timeout, query_future)
                                .await
                            {
                                Ok(result) => result,
                                Err(_) => Err(Status::deadline_exceeded(format!(
                                    "query exceeded the configured timeout of {:?}",
                                    self.limits.query_timeout
                                ))),
                            };

                        let app_metrics = common::self_monitoring::app_metrics();
                        let query_attrs = [opentelemetry::KeyValue::new("query_type", query_type)];
                        app_metrics
                            .query_duration
                            .record(query_start.elapsed().as_secs_f64(), &query_attrs);
                        app_metrics.flight_request_duration.record(
                            query_start.elapsed().as_secs_f64(),
                            &[opentelemetry::KeyValue::new("rpc.method", "do_get")],
                        );
                        let batches = match batches_result {
                            Ok(batches) => batches,
                            Err(status) => {
                                app_metrics.query_errors.add(1, &query_attrs);
                                return Err(status);
                            }
                        };
                        let rows_returned: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                        app_metrics
                            .query_rows_returned
                            .record(rows_returned, &query_attrs);

                        if batches.is_empty() {
                            let out = stream::empty().boxed();
                            return Ok(Response::new(out));
                        }

                        // Convert results to Flight data
                        let schema = batches[0].schema();
                        let flight_data = batches_to_compressed_flight_data(&schema, batches)
                            .map_err(|e| {
                                Status::internal(format!("Failed to convert results: {e}"))
                            })?;

                        let out = stream::iter(flight_data.into_iter().map(Ok)).boxed();
                        Ok(Response::new(out))
                    }
                    .await;
                    if let Err(status) = &result {
                        common::self_monitoring::record_span_exception(status);
                    }
                    let code = result
                        .as_ref()
                        .err()
                        .map(|s| s.code())
                        .unwrap_or(tonic::Code::Ok);
                    common::self_monitoring::spans::record_rpc_result(
                        &tracing::Span::current(),
                        common::self_monitoring::spans::RpcBoundary::Server,
                        code,
                    );
                    result
                }
                .instrument(span),
            ),
        )
        .await
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not supported"))
    }

    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not supported"))
    }

    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;

    async fn do_action(
        &self,
        _request: Request<arrow_flight::Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not supported"))
    }

    type ListActionsStream = BoxStream<'static, Result<arrow_flight::ActionType, Status>>;

    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let out = stream::empty().boxed();
        Ok(Response::new(out))
    }
}

/// Parse a `start:end` nanosecond window from ticket segments.
#[allow(clippy::result_large_err)]
fn parse_ns_window(start: &str, end: &str) -> Result<(i64, i64), Status> {
    let parse = |name: &str, value: &str| -> Result<i64, Status> {
        value.parse::<i64>().map_err(|_| {
            Status::invalid_argument(format!(
                "{name} '{value}' is not a unix-nanosecond timestamp"
            ))
        })
    };
    Ok((parse("start", start)?, parse("end", end)?))
}

/// Parse the optional JSON tail of a profile discovery ticket.
#[allow(clippy::result_large_err)]
fn parse_discovery_params(raw: Option<&str>) -> Result<ProfileDiscoveryParams, Status> {
    match raw {
        None | Some("") => Ok(ProfileDiscoveryParams::default()),
        Some(json) => serde_json::from_str(json).map_err(|e| {
            Status::invalid_argument(format!("Invalid profile discovery parameters: {e}"))
        }),
    }
}

/// Encode a list of strings as a single-column RecordBatch.
#[allow(clippy::result_large_err)]
fn strings_to_batch(column_name: &str, values: Vec<String>) -> Result<RecordBatch, Status> {
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![Field::new(
        column_name,
        DataType::Utf8,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))])
        .map_err(|e| Status::internal(format!("Failed to build result batch: {e}")))
}

/// Encode a serializable value as a single-row, single-column JSON batch.
#[allow(clippy::result_large_err)]
fn json_to_batch<T: serde::Serialize>(column_name: &str, value: &T) -> Result<RecordBatch, Status> {
    let json = serde_json::to_string(value)
        .map_err(|e| Status::internal(format!("Failed to serialize result: {e}")))?;
    strings_to_batch(column_name, vec![json])
}

/// Read-path labels for [`querier_error_to_status`]. A failure names the
/// signal it came from, so one signal's failure is never attributed to
/// another.
const SIGNAL_LOGS: &str = "Logs";
const SIGNAL_METRICS: &str = "Metrics";
const SIGNAL_PROFILES: &str = "Profile";
const SIGNAL_QUERY_IR: &str = "Query IR";

/// Map the caller-error variants every signal's error-to-status conversion
/// agrees on (`InvalidInput`/`Unsupported`); a variant this doesn't cover
/// comes back via `Err` un-consumed so the caller can add its own arms
/// without cloning.
pub(crate) fn common_error_status(
    err: crate::query::error::QuerierError,
) -> Result<Status, crate::query::error::QuerierError> {
    match err {
        crate::query::error::QuerierError::InvalidInput(msg) => Ok(Status::invalid_argument(msg)),
        crate::query::error::QuerierError::Unsupported(msg) => Ok(Status::unimplemented(msg)),
        other => Err(other),
    }
}

/// Map one signal's querier errors onto gRPC statuses: caller errors surface
/// as INVALID_ARGUMENT/UNIMPLEMENTED instead of a blanket internal error, and
/// server errors name `signal` as their origin.
fn querier_error_to_status(
    signal: &'static str,
) -> impl Fn(crate::query::error::QuerierError) -> Status {
    move |e| {
        common_error_status(e).unwrap_or_else(|e| match e {
            too_many @ crate::query::error::QuerierError::TooManyGroups { .. } => {
                Status::invalid_argument(too_many.to_string())
            }
            other => Status::internal(format!("{signal} query failed: {other:?}")),
        })
    }
}

/// Map trace lookup/search errors onto gRPC statuses: same caller-error
/// arms as [`querier_error_to_status`], but keeps each call site's existing
/// message wording (`context` names the action, e.g. "Trace query").
fn trace_error_to_status(
    context: &'static str,
) -> impl Fn(crate::query::error::QuerierError) -> Status {
    move |e| {
        common_error_status(e)
            .unwrap_or_else(|other| Status::internal(format!("{context} failed: {other:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::{Configuration, DatabaseConfig, DiscoveryConfig};
    use common::service_bootstrap::{ServiceBootstrap, ServiceType};
    use object_store::memory::InMemory;
    use std::time::Duration;

    #[tokio::test]
    async fn test_querier_flight_service_creation() {
        let object_store = Arc::new(InMemory::new());

        let config = Configuration {
            database: DatabaseConfig {
                dsn: "sqlite::memory:".to_string(),
            },
            discovery: Some(DiscoveryConfig {
                dsn: "sqlite::memory:".to_string(),
                heartbeat_interval: Duration::from_secs(5),
                poll_interval: Duration::from_secs(10),
                ttl: Duration::from_secs(60),
            }),
            ..Default::default()
        };

        let bootstrap =
            ServiceBootstrap::new(config, ServiceType::Querier, "localhost:50054".to_string())
                .await
                .unwrap();

        let flight_transport = Arc::new(InMemoryFlightTransport::new(bootstrap));
        let _service = QuerierFlightService::new(object_store, flight_transport);
    }

    /// Build a querier backed by a catalog manager whose tenant source is
    /// `source`, so database-created tenants participate in registration.
    async fn make_catalog_service(source: Arc<common::catalog::Catalog>) -> QuerierFlightService {
        let config = Configuration {
            database: DatabaseConfig {
                dsn: "sqlite::memory:".to_string(),
            },
            discovery: Some(DiscoveryConfig {
                dsn: "sqlite::memory:".to_string(),
                heartbeat_interval: Duration::from_secs(5),
                poll_interval: Duration::from_secs(10),
                ttl: Duration::from_secs(60),
            }),
            ..Default::default()
        };
        let bootstrap =
            ServiceBootstrap::new(config, ServiceType::Querier, "localhost:50054".to_string())
                .await
                .unwrap();
        let flight_transport = Arc::new(InMemoryFlightTransport::new(bootstrap));
        let catalog_manager = Arc::new(
            CatalogManager::new_in_memory()
                .await
                .unwrap()
                .with_tenant_source(source),
        );
        QuerierFlightService::new_with_catalog_manager(
            flight_transport,
            catalog_manager,
            QuerierConfig::default(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn database_tenant_catalog_registered_at_startup() {
        let source = Arc::new(common::catalog::Catalog::new_in_memory().await.unwrap());
        source
            .upsert_tenant("gamma", "Gamma", Some("production"), "database")
            .await
            .unwrap();
        source.create_dataset("gamma", "production").await.unwrap();

        let service = make_catalog_service(source).await;

        // The database-created tenant has a DataFusion catalog registered even
        // though it has no `[[auth.tenants]]` config block.
        assert!(
            service.session_ctx.catalog("gamma").is_some(),
            "database tenant should be registered at startup"
        );
    }

    #[tokio::test]
    async fn database_tenant_registered_lazily_without_restart() {
        // Start with an empty tenant source: nothing registered at startup.
        let source = Arc::new(common::catalog::Catalog::new_in_memory().await.unwrap());
        let service = make_catalog_service(source.clone()).await;
        assert!(
            service.session_ctx.catalog("delta").is_none(),
            "tenant absent before creation"
        );

        // Create the tenant AFTER the querier is built (as the admin API would,
        // against the shared catalog) — no restart / rebuild of the service.
        source
            .upsert_tenant("delta", "Delta", Some("production"), "database")
            .await
            .unwrap();
        source.create_dataset("delta", "production").await.unwrap();

        service.ensure_tenant_registered("delta").await.unwrap();

        assert!(
            service.session_ctx.catalog("delta").is_some(),
            "tenant should be registered on demand without restart"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lazy_registration_is_concurrency_safe() {
        let source = Arc::new(common::catalog::Catalog::new_in_memory().await.unwrap());
        // Empty at startup, so nothing is registered until the concurrent
        // first-queries race below.
        let service = Arc::new(make_catalog_service(source.clone()).await);
        source
            .upsert_tenant("epsilon", "Epsilon", Some("production"), "database")
            .await
            .unwrap();
        source
            .create_dataset("epsilon", "production")
            .await
            .unwrap();

        // Many simultaneous first-queries for the same not-yet-registered
        // tenant, spawned across worker threads so they genuinely race.
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let s = service.clone();
                tokio::spawn(async move { s.ensure_tenant_registered("epsilon").await })
            })
            .collect();
        for h in handles {
            h.await
                .expect("task did not panic")
                .expect("concurrent lazy registration should succeed");
        }

        assert!(service.session_ctx.catalog("epsilon").is_some());
        // Registered exactly once despite the race: the only registered slug is
        // epsilon (startup registered none, since the source was empty then).
        assert!(service.registered_tenants.contains("epsilon"));
        assert_eq!(
            service.registered_tenants.len(),
            1,
            "the tenant must be registered exactly once under concurrency"
        );
    }

    #[tokio::test]
    async fn test_query_execution() {
        let object_store = Arc::new(InMemory::new());

        let config = Configuration {
            database: DatabaseConfig {
                dsn: "sqlite::memory:".to_string(),
            },
            discovery: Some(DiscoveryConfig {
                dsn: "sqlite::memory:".to_string(),
                heartbeat_interval: Duration::from_secs(5),
                poll_interval: Duration::from_secs(10),
                ttl: Duration::from_secs(60),
            }),
            ..Default::default()
        };

        let bootstrap =
            ServiceBootstrap::new(config, ServiceType::Querier, "localhost:50054".to_string())
                .await
                .unwrap();

        let flight_transport = Arc::new(InMemoryFlightTransport::new(bootstrap));
        let service = QuerierFlightService::new(object_store, flight_transport);

        // Test basic query execution (will fail due to no data, but tests the path)
        let result = service
            .execute_query(&service.session_ctx, "SELECT 1 as test_col")
            .await;
        assert!(result.is_ok());
    }

    async fn make_service_with_limits(limits: QuerierConfig) -> QuerierFlightService {
        let object_store = Arc::new(InMemory::new());

        let config = Configuration {
            database: DatabaseConfig {
                dsn: "sqlite::memory:".to_string(),
            },
            discovery: Some(DiscoveryConfig {
                dsn: "sqlite::memory:".to_string(),
                heartbeat_interval: Duration::from_secs(5),
                poll_interval: Duration::from_secs(10),
                ttl: Duration::from_secs(60),
            }),
            ..Default::default()
        };

        let bootstrap =
            ServiceBootstrap::new(config, ServiceType::Querier, "localhost:50054".to_string())
                .await
                .unwrap();

        let flight_transport = Arc::new(InMemoryFlightTransport::new(bootstrap));
        QuerierFlightService::new_with_limits(object_store, flight_transport, limits)
    }

    #[tokio::test]
    async fn sql_row_cap_bounds_raw_sql_results() {
        let service = make_service_with_limits(QuerierConfig {
            max_sql_rows: 10,
            ..QuerierConfig::default()
        })
        .await;

        let batches = service
            .execute_query(
                &service.session_ctx,
                "SELECT * FROM generate_series(1, 1000)",
            )
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 10, "raw SQL results must be capped at max_sql_rows");
    }

    #[tokio::test]
    async fn concurrent_query_cap_is_enforced_per_tenant() {
        let service = make_service_with_limits(QuerierConfig {
            max_concurrent_queries_per_tenant: Some(1),
            ..QuerierConfig::default()
        })
        .await;

        let held = service
            .try_acquire_query_permit("acme")
            .expect("first query is within the cap")
            .expect("a permit is issued when a cap is configured");

        // Same tenant at the cap: rejected with the gRPC analog of 429.
        let status = service
            .try_acquire_query_permit("acme")
            .expect_err("second concurrent query must be rejected");
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);

        // A different tenant has its own budget.
        let other = service.try_acquire_query_permit("globex").unwrap();
        assert!(other.is_some());

        // Releasing the permit frees the slot.
        drop(held);
        assert!(service.try_acquire_query_permit("acme").is_ok());
    }

    #[tokio::test]
    async fn no_cap_means_no_permits_needed() {
        let service = make_service_with_limits(QuerierConfig::default()).await;
        for _ in 0..100 {
            assert!(service.try_acquire_query_permit("acme").unwrap().is_none());
        }
    }

    #[test]
    fn session_enables_datafusion_scan_options_by_default() {
        let ctx = session_context_with_limits(&QuerierConfig::default());
        let options = ctx.state().config_options().clone();
        assert!(
            options.execution.split_file_groups_by_statistics,
            "statistics-based file grouping must be on by default"
        );
        assert!(
            options.execution.parquet.pushdown_filters,
            "Parquet row-level filter pushdown must be on by default"
        );
        assert!(
            options.execution.parquet.reorder_filters,
            "Parquet filter reordering must be on by default"
        );
    }

    #[test]
    fn session_honors_disabled_datafusion_scan_options() {
        let limits = QuerierConfig {
            datafusion: common::config::QuerierDataFusionConfig {
                split_file_groups_by_statistics: false,
                pushdown_filters: false,
                reorder_filters: false,
            },
            ..QuerierConfig::default()
        };
        let ctx = session_context_with_limits(&limits);
        let options = ctx.state().config_options().clone();
        assert!(!options.execution.split_file_groups_by_statistics);
        assert!(!options.execution.parquet.pushdown_filters);
        assert!(!options.execution.parquet.reorder_filters);
    }

    #[test]
    fn memory_pool_is_bounded_when_configured() {
        use datafusion::execution::memory_pool::MemoryConsumer;

        let ctx = session_context_with_limits(&QuerierConfig {
            memory_limit_mb: Some(1),
            memory_pool_fraction: 1.0,
            ..QuerierConfig::default()
        });
        let reservation = MemoryConsumer::new("test").register(&ctx.runtime_env().memory_pool);
        assert!(
            reservation.try_grow(10 * 1024 * 1024).is_err(),
            "allocations beyond the configured limit must be refused"
        );

        // Without a configured limit the pool is unbounded (legacy behavior).
        let ctx = session_context_with_limits(&QuerierConfig::default());
        let reservation = MemoryConsumer::new("test").register(&ctx.runtime_env().memory_pool);
        assert!(reservation.try_grow(10 * 1024 * 1024).is_ok());
    }

    /// A shared querier must not let one heavy sort take the whole pool
    /// and starve every other tenant (#941) — the same greedy-pool shape
    /// that broke compaction in #1064.
    #[test]
    fn memory_pool_fair_shares_between_spilling_consumers() {
        use datafusion::execution::memory_pool::MemoryConsumer;

        let ctx = session_context_with_limits(&QuerierConfig {
            memory_limit_mb: Some(100),
            memory_pool_fraction: 1.0,
            ..QuerierConfig::default()
        });
        let pool = &ctx.runtime_env().memory_pool;

        let heavy = MemoryConsumer::new("tenant-a-sort")
            .with_can_spill(true)
            .register(pool);
        let _other = MemoryConsumer::new("tenant-b-sort")
            .with_can_spill(true)
            .register(pool);

        assert!(
            heavy.try_grow(90 * 1024 * 1024).is_err(),
            "one tenant's sort must not take 90% of a pool shared with another"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn query_timeout_returns_deadline_exceeded() {
        let service = make_service_with_limits(QuerierConfig {
            query_timeout: Duration::from_millis(50),
            ..QuerierConfig::default()
        })
        .await;

        // A cross join over 1e10 combinations cannot finish in 50ms.
        let ticket = Ticket {
            ticket: Bytes::from(
                "SELECT count(*) FROM generate_series(1, 100000000) t1(a) \
                 CROSS JOIN generate_series(1, 100) t2(b)",
            ),
        };
        let status = match service.do_get(Request::new(ticket)).await {
            Ok(_) => panic!("query must be aborted by the timeout"),
            Err(status) => status,
        };
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    }

    async fn make_service() -> QuerierFlightService {
        let object_store = Arc::new(InMemory::new());

        let config = Configuration {
            database: DatabaseConfig {
                dsn: "sqlite::memory:".to_string(),
            },
            discovery: Some(DiscoveryConfig {
                dsn: "sqlite::memory:".to_string(),
                heartbeat_interval: Duration::from_secs(5),
                poll_interval: Duration::from_secs(10),
                ttl: Duration::from_secs(60),
            }),
            ..Default::default()
        };

        let bootstrap =
            ServiceBootstrap::new(config, ServiceType::Querier, "localhost:50054".to_string())
                .await
                .unwrap();

        let flight_transport = Arc::new(InMemoryFlightTransport::new(bootstrap));
        QuerierFlightService::new(object_store, flight_transport)
    }

    #[tokio::test]
    async fn parse_find_trace_ticket_legacy_form_has_no_hints() {
        let service = make_service().await;
        match service.parse_ticket("find_trace:acme:prod:abc123").unwrap() {
            TicketRequest::FindTrace {
                tenant_slug,
                dataset_slug,
                trace_id,
                start,
                end,
            } => {
                assert_eq!(tenant_slug, "acme");
                assert_eq!(dataset_slug, "prod");
                assert_eq!(trace_id, "abc123");
                assert_eq!(start, None);
                assert_eq!(end, None);
            }
            other => panic!("expected FindTrace, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn parse_find_trace_ticket_with_time_hints() {
        let service = make_service().await;
        match service
            .parse_ticket("find_trace:acme:prod:abc123:1700000000:1700003600")
            .unwrap()
        {
            TicketRequest::FindTrace { start, end, .. } => {
                assert_eq!(start, Some(1_700_000_000));
                assert_eq!(end, Some(1_700_003_600));
            }
            other => panic!("expected FindTrace, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn parse_query_logs_ticket() {
        let service = make_service().await;
        let ticket = r#"query_logs:acme:prod:{"query":"{service_name=\"api\"}","start":10,"end":20,"limit":50,"direction":"forward"}"#;
        match service.parse_ticket(ticket).unwrap() {
            TicketRequest::QueryLogs {
                tenant_slug,
                dataset_slug,
                params,
            } => {
                assert_eq!(tenant_slug, "acme");
                assert_eq!(dataset_slug, "prod");
                assert_eq!(params.query, r#"{service_name="api"}"#);
                assert_eq!((params.start, params.end, params.limit), (10, 20, 50));
                assert_eq!(params.direction.as_deref(), Some("forward"));
            }
            other => panic!("expected QueryLogs, got {other:?}"),
        }
    }

    // Task 5.1 — the query_ir ticket is dispatched; a malformed ticket is
    // rejected with invalid_argument.
    #[tokio::test]
    async fn parse_query_ir_ticket() {
        let service = make_service().await;
        let ticket = r#"query_ir:acme:prod:{"document":{"irVersion":1,"from":"logs","range":{"from":"now-1h","to":"now"},"result":"rows","pipeline":[]},"now_ns":1700000000000000000}"#;
        match service.parse_ticket(ticket).unwrap() {
            TicketRequest::QueryIr {
                tenant_slug,
                dataset_slug,
                params,
            } => {
                assert_eq!(tenant_slug, "acme");
                assert_eq!(dataset_slug, "prod");
                assert_eq!(params.now_ns, 1_700_000_000_000_000_000);
                assert_eq!(params.document["from"], "logs");
            }
            other => panic!("expected QueryIr, got {other:?}"),
        }

        // A malformed IR ticket (bad JSON payload) is invalid_argument.
        let err = service
            .parse_ticket("query_ir:acme:prod:{not json}")
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn parse_query_logs_metadata_tickets() {
        let service = make_service().await;

        match service
            .parse_ticket("query_logs_labels:acme:prod:10:20")
            .unwrap()
        {
            TicketRequest::QueryLogsLabels {
                start,
                end,
                tenant_slug,
                ..
            } => {
                assert_eq!((start, end), (10, 20));
                assert_eq!(tenant_slug, "acme");
            }
            other => panic!("expected QueryLogsLabels, got {other:?}"),
        }

        match service
            .parse_ticket("query_logs_label_values:acme:prod:service_name:10:20")
            .unwrap()
        {
            TicketRequest::QueryLogsLabelValues {
                label, start, end, ..
            } => {
                assert_eq!(label, "service_name");
                assert_eq!((start, end), (10, 20));
            }
            other => panic!("expected QueryLogsLabelValues, got {other:?}"),
        }

        let series = r#"query_logs_series:acme:prod:{"selector":"{service_name=\"api\"}","start":10,"end":20}"#;
        match service.parse_ticket(series).unwrap() {
            TicketRequest::QueryLogsSeries { params, .. } => {
                assert_eq!(params.selector, r#"{service_name="api"}"#);
                assert_eq!((params.start, params.end), (10, 20));
            }
            other => panic!("expected QueryLogsSeries, got {other:?}"),
        }

        let detected =
            r#"query_logs_detected_fields:acme:prod:{"query":null,"start":10,"end":20,"limit":50}"#;
        match service.parse_ticket(detected).unwrap() {
            TicketRequest::QueryLogsDetectedFields { params, .. } => {
                assert_eq!(params.query, None);
                assert_eq!((params.start, params.end), (10, 20));
                assert_eq!(params.limit, 50);
            }
            other => panic!("expected QueryLogsDetectedFields, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn parse_query_promql_ticket() {
        let service = make_service().await;
        let ticket =
            r#"query_promql:acme:prod:{"query":"sum(rate(up[5m]))","start":10,"end":20,"step":15}"#;
        match service.parse_ticket(ticket).unwrap() {
            TicketRequest::QueryPromql {
                tenant_slug,
                dataset_slug,
                params,
            } => {
                assert_eq!(tenant_slug, "acme");
                assert_eq!(dataset_slug, "prod");
                assert_eq!(params.query, "sum(rate(up[5m]))");
                assert_eq!((params.start, params.end, params.step), (10, 20, 15));
            }
            other => panic!("expected QueryPromql, got {other:?}"),
        }
        assert!(
            service
                .parse_ticket("query_promql:acme:prod:not-json")
                .is_err()
        );
    }

    #[tokio::test]
    async fn parse_trace_tags_ticket() {
        let service = make_service().await;
        let ticket = r#"trace_tags:acme:prod:{"start":10,"end":20,"scope":"span"}"#;
        match service.parse_ticket(ticket).unwrap() {
            TicketRequest::TraceTags {
                tenant_slug,
                dataset_slug,
                params,
            } => {
                assert_eq!(tenant_slug, "acme");
                assert_eq!(dataset_slug, "prod");
                assert_eq!((params.start, params.end), (10, 20));
                assert_eq!(params.scope, Some(tempo_api::TagScope::Span));
            }
            other => panic!("expected TraceTags, got {other:?}"),
        }

        // Scope is optional.
        let ticket = r#"trace_tags:acme:prod:{"start":10,"end":20}"#;
        match service.parse_ticket(ticket).unwrap() {
            TicketRequest::TraceTags { params, .. } => assert_eq!(params.scope, None),
            other => panic!("expected TraceTags, got {other:?}"),
        }

        assert!(
            service
                .parse_ticket("trace_tags:acme:prod:not-json")
                .is_err()
        );
    }

    #[tokio::test]
    async fn parse_trace_tag_values_ticket() {
        let service = make_service().await;
        let ticket = r#"trace_tag_values:acme:prod:http.route:{"start":10,"end":20}"#;
        match service.parse_ticket(ticket).unwrap() {
            TicketRequest::TraceTagValues {
                tenant_slug,
                dataset_slug,
                tag,
                params,
            } => {
                assert_eq!(tenant_slug, "acme");
                assert_eq!(dataset_slug, "prod");
                assert_eq!(tag, "http.route");
                assert_eq!((params.start, params.end), (10, 20));
            }
            other => panic!("expected TraceTagValues, got {other:?}"),
        }

        assert!(
            service
                .parse_ticket("trace_tag_values:acme:prod:http.route:not-json")
                .is_err()
        );
    }

    #[tokio::test]
    async fn malformed_query_logs_tickets_are_rejected() {
        let service = make_service().await;
        assert!(
            service
                .parse_ticket("query_logs:acme:prod:not-json")
                .is_err()
        );
        assert!(
            service
                .parse_ticket("query_logs_labels:acme:prod:notanumber:20")
                .is_err()
        );
    }

    #[tokio::test]
    async fn parse_find_trace_ticket_allows_empty_hint_segments() {
        let service = make_service().await;
        match service
            .parse_ticket("find_trace:acme:prod:abc123::1700003600")
            .unwrap()
        {
            TicketRequest::FindTrace { start, end, .. } => {
                assert_eq!(start, None);
                assert_eq!(end, Some(1_700_003_600));
            }
            other => panic!("expected FindTrace, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn parse_find_trace_ticket_rejects_non_numeric_hint() {
        let service = make_service().await;
        let status = service
            .parse_ticket("find_trace:acme:prod:abc123:soon:later")
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    /// Register a catalog `name` with schema `schema` containing a
    /// single-row table `t` whose column `owner` holds `name`.
    fn register_tenant_catalog(service: &QuerierFlightService, name: &str, schema: &str) {
        use datafusion::arrow::array::StringArray;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::catalog::MemoryCatalogProvider;
        use datafusion::catalog::MemorySchemaProvider;
        use datafusion::datasource::MemTable;

        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "owner",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            table_schema.clone(),
            vec![Arc::new(StringArray::from(vec![name.to_string()]))],
        )
        .unwrap();
        let table = MemTable::try_new(table_schema, vec![vec![batch]]).unwrap();

        let schema_provider = MemorySchemaProvider::new();
        schema_provider
            .register_table("t".to_string(), Arc::new(table))
            .unwrap();

        let catalog = MemoryCatalogProvider::new();
        catalog
            .register_schema(schema, Arc::new(schema_provider))
            .unwrap();

        service
            .session_ctx
            .register_catalog(name, Arc::new(catalog));
    }

    #[tokio::test]
    async fn per_request_sessions_isolate_tenant_defaults() {
        use datafusion::arrow::array::StringArray;

        let service = make_service().await;
        register_tenant_catalog(&service, "tenant_a", "prod");
        register_tenant_catalog(&service, "tenant_b", "prod");

        // Two per-request contexts for different tenants, both alive at once
        let ctx_a = service.session_for_request(Some("tenant_a"), Some("prod"));
        let ctx_b = service.session_for_request(Some("tenant_b"), Some("prod"));

        // Building per-request contexts must not shadow the real tenant
        // catalogs on the shared catalog list (SessionStateBuilder would
        // register an empty default catalog if not explicitly disabled)
        let cat = service.session_ctx.catalog("tenant_a").unwrap();
        assert_eq!(
            cat.schema("prod").unwrap().table_names(),
            vec!["t".to_string()],
            "shared tenant catalog must remain intact"
        );

        // Each context must resolve the unqualified table in its own catalog,
        // regardless of the other context existing concurrently
        for (ctx, expected) in [(&ctx_a, "tenant_a"), (&ctx_b, "tenant_b")] {
            let batches = ctx
                .sql("SELECT owner FROM t")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let owners = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(owners.value(0), expected);
        }

        // The shared context's defaults must be untouched
        let state = service.session_ctx.state();
        assert_eq!(
            state.config().options().catalog.default_catalog,
            "datafusion"
        );
    }

    #[tokio::test]
    async fn session_for_request_caches_per_tenant_dataset() {
        let service = make_service().await;
        register_tenant_catalog(&service, "tenant_a", "prod");
        register_tenant_catalog(&service, "tenant_b", "prod");

        // Two sequential calls for the same (tenant, dataset) must return the
        // *same* cached context (not merely an equivalent one), proving the
        // expensive SessionStateBuilder rebuild only happens once.
        let first = service.session_for_request(Some("tenant_a"), Some("prod"));
        let second = service.session_for_request(Some("tenant_a"), Some("prod"));
        assert!(
            Arc::ptr_eq(&first, &second),
            "second call for the same (tenant, dataset) must reuse the cached context"
        );

        // A different dataset for the same tenant is a distinct cache entry.
        let other_dataset = service.session_for_request(Some("tenant_a"), Some("staging"));
        assert!(
            !Arc::ptr_eq(&first, &other_dataset),
            "different dataset must get its own cached context"
        );

        // A different tenant must get a distinct cached context with its own
        // defaults, never the other tenant's.
        let other_tenant = service.session_for_request(Some("tenant_b"), Some("prod"));
        assert!(
            !Arc::ptr_eq(&first, &other_tenant),
            "different tenant must get its own cached context"
        );
        assert_eq!(
            first.state().config().options().catalog.default_catalog,
            "tenant_a"
        );
        assert_eq!(
            other_tenant
                .state()
                .config()
                .options()
                .catalog
                .default_catalog,
            "tenant_b"
        );

        // Exactly the three distinct (tenant, dataset) pairs requested above
        // are cached — proves the cache doesn't grow per-request.
        assert_eq!(service.session_cache.len(), 3);
    }

    #[tokio::test]
    async fn session_for_request_cache_sees_catalogs_registered_after_caching() {
        // A tenant's catalog registered on the shared session_ctx *after* a
        // request already cached that tenant's derived context must still
        // be visible through the cached context — catalog_list is an
        // Arc<dyn CatalogProviderList> shared (not deep-cloned) across
        // derived SessionStates.
        let service = make_service().await;
        register_tenant_catalog(&service, "tenant_c", "prod");

        // Populate the cache before the second catalog exists.
        let cached = service.session_for_request(Some("tenant_c"), Some("prod"));
        assert!(cached.catalog("tenant_c").is_some());

        // Register a brand-new tenant catalog on the shared context after
        // caching.
        register_tenant_catalog(&service, "tenant_d", "prod");

        // The cached context for tenant_c (built before tenant_d existed)
        // must still resolve the newly-registered tenant_d catalog, because
        // both share the same underlying catalog list.
        assert!(
            cached.catalog("tenant_d").is_some(),
            "cached context must observe catalogs registered on the shared session_ctx later"
        );
    }

    #[tokio::test]
    async fn tenant_scoped_caller_cannot_use_cross_tenant_ticket() {
        let service = make_service().await;

        // Authenticated as tenant_a, but the ticket names tenant_b
        let mut request = Request::new(Ticket::new("find_trace:tenant_b:prod:abc123"));
        request
            .extensions_mut()
            .insert(common::auth::TenantContext::new(
                "tenant_a".to_string(),
                "prod".to_string(),
                "tenant_a".to_string(),
                "prod".to_string(),
                None,
                common::auth::TenantSource::Config,
            ));

        match service.do_get(request).await {
            Ok(_) => panic!("cross-tenant ticket must be rejected"),
            Err(status) => assert_eq!(status.code(), tonic::Code::PermissionDenied),
        }
    }

    #[tokio::test]
    async fn system_tenant_query_is_suppressed_from_otel_export() {
        // Regression test for issue #760: executing a query for the
        // _system tenant must not emit spans/log records that pass the
        // OTel export filter — they would be exported and re-ingested as
        // _system telemetry (feedback loop). A normal tenant's query
        // telemetry must still export.
        let service = make_service().await;

        let query = |tenant: &str| {
            let mut request = Request::new(Ticket::new(format!(
                "find_trace:{tenant}:_monitoring:abc123"
            )));
            request
                .extensions_mut()
                .insert(common::auth::TenantContext::new(
                    tenant.to_string(),
                    "_monitoring".to_string(),
                    tenant.to_string(),
                    "_monitoring".to_string(),
                    None,
                    common::auth::TenantSource::Config,
                ));
            request
        };

        let probe = common::testing::OtelExportProbe::new();
        {
            let _guard = probe.install();
            let _ = service.do_get(query("_system")).await;
        }
        assert_eq!(
            probe.exported_events(),
            0,
            "_system query processing must not export log records"
        );
        assert_eq!(
            probe.exported_spans(),
            0,
            "_system query processing must not export spans"
        );

        let probe = common::testing::OtelExportProbe::new();
        {
            let _guard = probe.install();
            let _ = service.do_get(query("tenant_a")).await;
        }
        assert!(
            probe.exported_events() > 0,
            "normal tenant query processing must still export telemetry"
        );
    }

    #[tokio::test]
    async fn tenant_scoped_caller_may_use_own_tenant_ticket() {
        let service = make_service().await;

        // Same tenant in context and ticket: passes authorization and
        // proceeds to execution (no data registered, so an empty result or
        // a non-permission error are both acceptable here)
        let mut request = Request::new(Ticket::new("find_trace:tenant_a:prod:abc123"));
        request
            .extensions_mut()
            .insert(common::auth::TenantContext::new(
                "tenant_a".to_string(),
                "prod".to_string(),
                "tenant_a".to_string(),
                "prod".to_string(),
                None,
                common::auth::TenantSource::Config,
            ));

        match service.do_get(request).await {
            Ok(_) => {}
            Err(status) => {
                assert_ne!(
                    status.code(),
                    tonic::Code::PermissionDenied,
                    "same-tenant ticket must not be rejected for authorization"
                );
            }
        }
    }

    /// The Flight boundary emits a semconv RPC SERVER span: fully-qualified
    /// method name disambiguated by the ticket verb, `rpc.*` attributes,
    /// and the server-side status asymmetry (a client-fault gRPC code
    /// leaves span status unset).
    #[tokio::test]
    async fn do_get_emits_semconv_rpc_server_span() {
        use opentelemetry::trace::{SpanKind, Status as OtelStatus, TracerProvider as _};
        use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::prelude::*;

        let object_store = Arc::new(InMemory::new());
        let config = Configuration {
            database: DatabaseConfig {
                dsn: "sqlite::memory:".to_string(),
            },
            discovery: Some(DiscoveryConfig {
                dsn: "sqlite::memory:".to_string(),
                heartbeat_interval: Duration::from_secs(5),
                poll_interval: Duration::from_secs(10),
                ttl: Duration::from_secs(60),
            }),
            ..Default::default()
        };
        let bootstrap =
            ServiceBootstrap::new(config, ServiceType::Querier, "localhost:50054".to_string())
                .await
                .unwrap();
        let flight_transport = Arc::new(InMemoryFlightTransport::new(bootstrap));
        let service = QuerierFlightService::new(object_store, flight_transport);

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        async {
            // Unknown verb: parse fails with a client-fault code.
            let ticket = Ticket {
                ticket: bytes::Bytes::from("bogus_op:sometenant:xyz"),
            };
            let result = service.do_get(Request::new(ticket)).await;
            assert!(result.is_err());
        }
        .with_subscriber(subscriber)
        .await;

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let names: Vec<_> = spans.iter().map(|s| s.name.to_string()).collect();
        let span = spans
            .iter()
            .find(|s| {
                s.name
                    .starts_with("arrow.flight.protocol.FlightService/DoGet")
            })
            .unwrap_or_else(|| panic!("no RPC server span; exported = {names:?}"));

        assert_eq!(
            span.name,
            "arrow.flight.protocol.FlightService/DoGet bogus_op"
        );
        assert_eq!(span.span_kind, SpanKind::Server);
        let attr = |key: &str| {
            span.attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.as_str().to_string())
        };
        assert_eq!(attr("rpc.system.name").as_deref(), Some("grpc"));
        assert_eq!(
            attr("rpc.method").as_deref(),
            Some("arrow.flight.protocol.FlightService/DoGet")
        );
        assert_eq!(
            attr("signaldb.flight.ticket_verb").as_deref(),
            Some("bogus_op")
        );
        // The unparseable ticket surfaces as INTERNAL — a server-fault
        // code, so the span is marked failed (the client-fault-stays-unset
        // asymmetry is pinned by the factory conformance tests).
        assert_eq!(
            attr("rpc.response.status_code").as_deref(),
            Some("INTERNAL")
        );
        assert!(matches!(span.status, OtelStatus::Error { .. }));
    }

    /// Issue #972 companion: the shared status mapper used to label every
    /// signal's failure "Profile query failed", so a logs or metrics failure
    /// was attributed to profiles. Each read path names its own signal.
    #[test]
    fn error_status_names_the_signal_it_came_from() {
        use crate::query::error::QuerierError;
        use datafusion::error::DataFusionError;

        let message_for = |signal| {
            querier_error_to_status(signal)(QuerierError::QueryFailed(DataFusionError::Internal(
                "boom".to_string(),
            )))
            .message()
            .to_string()
        };

        for signal in [
            SIGNAL_LOGS,
            SIGNAL_METRICS,
            SIGNAL_PROFILES,
            SIGNAL_QUERY_IR,
        ] {
            let message = message_for(signal);
            assert!(
                message.starts_with(signal),
                "{signal} failure reported as: {message}"
            );
        }
        assert!(!message_for(SIGNAL_LOGS).contains("Profile"));
        assert!(!message_for(SIGNAL_METRICS).contains("Profile"));
    }

    /// Caller errors keep their own gRPC codes regardless of signal.
    #[test]
    fn caller_errors_keep_their_codes() {
        use crate::query::error::QuerierError;

        let status =
            querier_error_to_status(SIGNAL_LOGS)(QuerierError::InvalidInput("bad".to_string()));
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let status =
            querier_error_to_status(SIGNAL_LOGS)(QuerierError::Unsupported("nope".to_string()));
        assert_eq!(status.code(), tonic::Code::Unimplemented);
    }
}
