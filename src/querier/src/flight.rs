use anyhow::Context;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult,
    SchemaResult, Ticket,
};
use bytes::Bytes;
use common::CatalogManager;
use common::config::QuerierConfig;
use common::flight::schema::{FlightSchemas, create_span_batch_schema};
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

use crate::query::trace::TraceService;
use crate::query::{FindTraceByIdParams, SearchQueryParams};

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
    },
    SearchTraces {
        tenant_slug: String,
        dataset_slug: String,
        params: SearchQueryParams,
    },
    SqlQuery {
        sql: String,
    },
}

/// Flight service for query execution against stored data
pub struct QuerierFlightService {
    #[allow(dead_code)]
    object_store: Arc<dyn ObjectStore>,
    _flight_transport: Arc<InMemoryFlightTransport>,
    #[allow(dead_code)]
    schemas: FlightSchemas,
    session_ctx: Arc<SessionContext>,
    trace_service: TraceService,
    #[allow(dead_code)]
    iceberg_catalog: Option<Arc<dyn iceberg_rust::catalog::Catalog>>,
    limits: QuerierConfig,
    /// Per-tenant concurrent-query permits, populated lazily. Bounded by
    /// the number of distinct tenants.
    query_permits: dashmap::DashMap<String, Arc<tokio::sync::Semaphore>>,
}

/// Build a SessionContext whose RuntimeEnv enforces the configured memory
/// limit (spilling operators use the default disk manager). Falls back to
/// an unlimited default context if the runtime cannot be built, which is
/// logged as an error and practically cannot happen with default settings.
fn session_context_with_limits(limits: &QuerierConfig) -> SessionContext {
    let mut builder = RuntimeEnvBuilder::new();
    match limits.memory_limit_mb {
        Some(mb) => {
            builder =
                builder.with_memory_limit((mb as usize) * 1024 * 1024, limits.memory_pool_fraction);
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
            SessionContext::new_with_config_rt(SessionConfig::new(), Arc::new(runtime_env))
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                "Failed to build limited RuntimeEnv; falling back to unlimited defaults"
            );
            SessionContext::new()
        }
    }
}

impl QuerierFlightService {
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

        Self {
            object_store,
            _flight_transport: flight_transport,
            schemas: FlightSchemas::new(),
            session_ctx,
            trace_service,
            iceberg_catalog: None,
            limits,
            query_permits: dashmap::DashMap::new(),
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

        // Collect enabled tenants once to avoid duplicating the filter logic
        let enabled_tenants = catalog_manager.get_enabled_tenants();

        // Register object stores for all configured storage backends
        for tenant in &enabled_tenants {
            for dataset in &tenant.datasets {
                let storage_config =
                    catalog_manager.get_dataset_storage_config(&tenant.id, &dataset.id);
                let url_str = &storage_config.dsn;

                // Register each unique storage URL with a scheme-appropriate object store
                if !registered_urls.contains(url_str) {
                    match url::Url::parse(url_str) {
                        Ok(url) => {
                            let store =
                                create_object_store_from_dsn(url_str).with_context(|| {
                                    format!(
                                        "Failed to create object store for dataset DSN: {url_str}"
                                    )
                                })?;
                            session_ctx.runtime_env().register_object_store(&url, store);
                            registered_urls.insert(url_str.clone());
                            tracing::info!(
                                scheme = %url.scheme(),
                                url = %url_str,
                                "Registered object store"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                dsn = %url_str,
                                tenant_id = %tenant.id,
                                dataset_id = %dataset.id,
                                error = %e,
                                "Skipping invalid storage DSN"
                            );
                        }
                    }
                }
            }
        }

        // Struct field placeholder — not used at runtime (all per-dataset stores
        // are registered above). InMemory avoids coupling to any specific DSN.
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());

        let iceberg_catalog = catalog_manager.catalog();

        for tenant in &enabled_tenants {
            let tenant_catalog = TenantCatalog {
                tenant_slug: tenant.slug.clone(),
                catalog: iceberg_catalog.clone(),
            };

            session_ctx.register_catalog(&tenant.slug, Arc::new(tenant_catalog));
            tracing::info!(
                catalog = %tenant.slug,
                tenant_id = %tenant.id,
                "Registered DataFusion catalog"
            );
        }

        // Create trace service for specialized trace queries
        let trace_service = TraceService::new(session_ctx.as_ref().clone(), "traces".to_string())
            .with_max_search_limit(limits.max_search_limit);

        Ok(Self {
            object_store,
            _flight_transport: flight_transport,
            schemas: FlightSchemas::new(),
            session_ctx,
            trace_service,
            iceberg_catalog: Some(iceberg_catalog),
            limits,
            query_permits: dashmap::DashMap::new(),
        })
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
        // New format: find_trace:{tenant_slug}:{dataset_slug}:{trace_id}
        if let Some(remainder) = ticket_content.strip_prefix("find_trace:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() == 3 {
                tracing::info!(
                    tenant_slug = %parts[0],
                    dataset_slug = %parts[1],
                    trace_id = %parts[2],
                    "Parsing find_trace ticket"
                );
                return Ok(TicketRequest::FindTrace {
                    tenant_slug: parts[0].to_string(),
                    dataset_slug: parts[1].to_string(),
                    trace_id: parts[2].to_string(),
                });
            } else {
                return Err(Status::invalid_argument(
                    "Invalid find_trace ticket format. Expected: find_trace:tenant_slug:dataset_slug:trace_id",
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

        // Collect all spans from the trace (including nested children)
        let mut all_spans = Vec::new();
        fn collect_spans(
            spans: &[common::model::span::Span],
            all_spans: &mut Vec<common::model::span::Span>,
        ) {
            for span in spans {
                all_spans.push(span.clone());
                collect_spans(&span.children, all_spans);
            }
        }
        collect_spans(&trace.spans, &mut all_spans);

        if all_spans.is_empty() {
            return Ok(vec![]);
        }

        // Build arrays for each column (order must match create_span_batch_schema)
        let mut trace_ids = Vec::new();
        let mut span_ids = Vec::new();
        let mut parent_span_ids = Vec::new();
        let mut statuses = Vec::new();
        let mut is_roots = Vec::new();
        let mut names = Vec::new();
        let mut service_names = Vec::new();
        let mut span_kinds = Vec::new();
        let mut start_times = Vec::new();
        let mut duration_nanos = Vec::new();
        let mut span_attributes_json = Vec::new();
        let mut resource_json = Vec::new();

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
            ],
        )?;

        Ok(vec![batch])
    }

    /// Build a per-request SessionContext with tenant/dataset defaults.
    ///
    /// The shared context's session state is cloned per request so that
    /// concurrent queries can never observe each other's default
    /// catalog/schema — mutating the shared context (the previous `SET
    /// datafusion.catalog.default_catalog` approach) let two concurrent
    /// queries for different tenants execute against the wrong tenant's
    /// catalog. Registered catalogs and the runtime environment remain
    /// shared through Arcs inside the cloned state.
    fn session_for_request(
        &self,
        tenant_slug: Option<&str>,
        dataset_slug: Option<&str>,
    ) -> SessionContext {
        let state = self.session_ctx.state();

        let Some(tenant) = tenant_slug else {
            return SessionContext::new_with_state(state);
        };

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
        options.catalog.default_schema = dataset_slug.unwrap_or("default").to_string();

        let state = SessionStateBuilder::new_from_existing(state)
            .with_config(config)
            .build();
        SessionContext::new_with_state(state)
    }

    /// Execute a SQL query and return results as RecordBatches
    async fn execute_query(
        &self,
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!(sql = %sql, "Executing query");

        let df = ctx.sql(sql).await?;
        // Cap the number of rows a raw SQL query can materialize; the
        // client controls the SQL, so an unbounded SELECT could otherwise
        // buffer arbitrarily many rows in memory.
        let df = df.limit(0, Some(self.limits.max_sql_rows))?;
        let batches = df.collect().await?;

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
        // Process within a span that joins the caller's distributed trace
        // (e.g. Router -> Querier); the parent must be set before the span
        // is first entered.
        let span = tracing::info_span!("flight_do_get");
        common::flight::trace_context::set_parent_from_request(&span, &request);
        async move {
            let metadata = request.metadata().clone();
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

            tracing::info!(ticket = %ticket_content, "Processing Flight ticket");

            // Parse ticket to determine request type
            let ticket_request = self.parse_ticket(&ticket_content)?;

            // Tenant-scoped callers may only touch their own tenant's data,
            // regardless of what the ticket claims.
            if let Some(ctx) = &caller_tenant {
                let ticket_tenant = match &ticket_request {
                    TicketRequest::FindTrace { tenant_slug, .. }
                    | TicketRequest::SearchTraces { tenant_slug, .. } => Some(tenant_slug),
                    TicketRequest::SqlQuery { .. } => None,
                };
                if let Some(ticket_tenant) = ticket_tenant
                    && ticket_tenant != &ctx.tenant_slug
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
            }
            let query_type = match &ticket_request {
                TicketRequest::FindTrace { .. } => "trace_by_id",
                TicketRequest::SearchTraces { .. } => "trace_search",
                TicketRequest::SqlQuery { .. } => "sql",
            };

            // Per-tenant concurrent-query cap. The tenant is the
            // authenticated caller when present, else the tenant named in
            // the ticket (internal callers proxy on behalf of tenants).
            // Internal raw-SQL callers carry no tenant and are exempt.
            let permit_tenant = caller_tenant
                .as_ref()
                .map(|ctx| ctx.tenant_id.clone())
                .or_else(|| match &ticket_request {
                    TicketRequest::FindTrace { tenant_slug, .. }
                    | TicketRequest::SearchTraces { tenant_slug, .. } => Some(tenant_slug.clone()),
                    TicketRequest::SqlQuery { .. } => None,
                });
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
                    } => {
                        tracing::info!(
                            tenant_slug = %tenant_slug,
                            dataset_slug = %dataset_slug,
                            trace_id = %trace_id,
                            "Executing find_trace"
                        );

                        let params = FindTraceByIdParams {
                            trace_id,
                            start: None,
                            end: None,
                        };

                        match self
                            .trace_service
                            .find_by_id_with_tenant(params, &tenant_slug, &dataset_slug)
                            .await
                        {
                            Ok(Some(trace)) => {
                                tracing::info!(span_count = trace.spans.len(), "Found trace");
                                self.trace_to_record_batches(&trace).await.map_err(|e| {
                                    Status::internal(format!(
                                        "Failed to convert trace to batches: {e}"
                                    ))
                                })?
                            }
                            Ok(None) => {
                                tracing::info!("No trace found");
                                vec![]
                            }
                            Err(e) => {
                                tracing::error!(error = ?e, "Error querying trace");
                                return Err(Status::internal(format!("Trace query failed: {e:?}")));
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
                            .find_traces_with_tenant(params, &tenant_slug, &dataset_slug)
                            .await
                        {
                            Ok(traces) => {
                                tracing::info!(trace_count = traces.len(), "Found traces");

                                let mut all_batches = Vec::new();
                                for trace in &traces {
                                    let trace_batches =
                                        self.trace_to_record_batches(trace).await.map_err(|e| {
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
                                return Err(Status::internal(format!(
                                    "Trace search failed: {e:?}"
                                )));
                            }
                        }
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
                        let request_ctx = self
                            .session_for_request(tenant_slug.as_deref(), dataset_slug.as_deref());

                        self.execute_distributed_query(&request_ctx, &sql)
                            .await
                            .map_err(|e| Status::internal(format!("Query execution failed: {e}")))?
                    }
                })
            };
            // Bound every query's wall-clock time so a heavy scan cannot
            // occupy the querier indefinitely.
            let batches_result: Result<Vec<_>, Status> =
                match tokio::time::timeout(self.limits.query_timeout, query_future).await {
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
            let flight_data = batches_to_flight_data(&schema, batches)
                .map_err(|e| Status::internal(format!("Failed to convert results: {e}")))?;

            let out = stream::iter(flight_data.into_iter().map(Ok)).boxed();
            Ok(Response::new(out))
        }
        .instrument(span)
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
}
