use anyhow::Context;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult,
    SchemaResult, Ticket,
};
use bytes::Bytes;
use common::CatalogManager;
use common::flight::schema::{FlightSchemas, create_span_batch_schema};
use common::flight::transport::InMemoryFlightTransport;
use common::storage::create_object_store_from_dsn;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use object_store::ObjectStore;
use std::collections::HashSet;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::query::trace::TraceService;
use crate::query::{FindTraceByIdParams, SearchQueryParams};

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
}

impl QuerierFlightService {
    /// Create a new QuerierFlightService
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        flight_transport: Arc<InMemoryFlightTransport>,
    ) -> Self {
        let session_ctx = Arc::new(SessionContext::new());

        // Register object store with DataFusion for querying Parquet files
        // This allows querying files like: SELECT * FROM 'batch/file.parquet'
        let url = url::Url::parse("file://").unwrap();
        session_ctx
            .runtime_env()
            .register_object_store(&url, object_store.clone());

        // Create trace service for specialized trace queries
        let trace_service = TraceService::new(session_ctx.as_ref().clone(), "traces".to_string());

        Self {
            object_store,
            _flight_transport: flight_transport,
            schemas: FlightSchemas::new(),
            session_ctx,
            trace_service,
            iceberg_catalog: None,
        }
    }

    /// Create a new QuerierFlightService with Iceberg catalog
    pub async fn new_with_iceberg(
        object_store: Arc<dyn ObjectStore>,
        flight_transport: Arc<InMemoryFlightTransport>,
        config: &common::config::Configuration,
    ) -> anyhow::Result<Self> {
        let session_ctx = Arc::new(SessionContext::new());

        // Register object store with DataFusion for querying Parquet files
        let url = url::Url::parse("file://").unwrap();
        session_ctx
            .runtime_env()
            .register_object_store(&url, object_store.clone());

        // Create Iceberg SQL catalog using common module to ensure proper storage configuration
        let iceberg_catalog = common::schema::create_catalog_with_config(config).await?;

        // Create datafusion_iceberg catalog wrapper
        let datafusion_catalog = datafusion_iceberg::catalog::catalog::IcebergCatalog::new(
            iceberg_catalog.clone(),
            None, // No specific branch
        )
        .await?;

        // Register the iceberg catalog with DataFusion
        session_ctx.register_catalog("iceberg", Arc::new(datafusion_catalog));
        log::info!("Registered Iceberg catalog with DataFusion");

        // Create trace service for specialized trace queries
        let trace_service = TraceService::new(session_ctx.as_ref().clone(), "traces".to_string());

        Ok(Self {
            object_store,
            _flight_transport: flight_transport,
            schemas: FlightSchemas::new(),
            session_ctx,
            trace_service,
            iceberg_catalog: Some(iceberg_catalog),
        })
    }

    /// Create a new QuerierFlightService with CatalogManager and per-tenant catalogs
    ///
    /// This constructor registers each enabled tenant as a separate DataFusion catalog,
    /// allowing queries like `SELECT * FROM tenant.dataset.traces` to work correctly.
    pub async fn new_with_catalog_manager(
        flight_transport: Arc<InMemoryFlightTransport>,
        catalog_manager: Arc<CatalogManager>,
    ) -> anyhow::Result<Self> {
        let config = catalog_manager.config();
        let session_ctx = Arc::new(SessionContext::new());

        // Track registered storage URLs to avoid duplicates
        let mut registered_urls: HashSet<String> = HashSet::new();

        // Register object stores for all configured storage backends
        for tenant in &config.auth.tenants {
            // Check if tenant is enabled via schema_config
            if let Some(ref schema_config) = tenant.schema_config
                && !schema_config.enabled
            {
                continue;
            }

            for dataset in &tenant.datasets {
                let storage_config =
                    catalog_manager.get_dataset_storage_config(&tenant.id, &dataset.id);
                let url_str = &storage_config.dsn;

                // Register each unique storage URL with a scheme-appropriate object store
                if !registered_urls.contains(url_str)
                    && let Ok(url) = url::Url::parse(url_str)
                {
                    let store = create_object_store_from_dsn(url_str).with_context(|| {
                        format!("Failed to create object store for dataset DSN: {url_str}")
                    })?;
                    session_ctx.runtime_env().register_object_store(&url, store);
                    registered_urls.insert(url_str.clone());
                    log::info!(
                        "Registered object store for scheme '{}': {url_str}",
                        url.scheme()
                    );
                }
            }
        }

        // Struct field placeholder — not used at runtime (all per-dataset stores
        // are registered above). InMemory avoids coupling to any specific DSN.
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());

        // Use the shared catalog from CatalogManager
        let iceberg_catalog = catalog_manager.catalog();

        // Register per-tenant DataFusion catalogs (each tenant slug as a catalog name)
        for tenant in &config.auth.tenants {
            // Check if tenant is enabled
            if let Some(ref schema_config) = tenant.schema_config
                && !schema_config.enabled
            {
                continue;
            }

            // Create DataFusion catalog wrapper for this tenant
            // Note: All tenants share the same underlying Iceberg catalog
            let datafusion_catalog = datafusion_iceberg::catalog::catalog::IcebergCatalog::new(
                iceberg_catalog.clone(),
                None, // No specific branch
            )
            .await?;

            session_ctx.register_catalog(&tenant.slug, Arc::new(datafusion_catalog));
            log::info!(
                "Registered DataFusion catalog '{}' for tenant '{}'",
                tenant.slug,
                tenant.id
            );
        }

        // Also register under the "iceberg" name for backward compatibility
        let compat_catalog = datafusion_iceberg::catalog::catalog::IcebergCatalog::new(
            iceberg_catalog.clone(),
            None,
        )
        .await?;
        session_ctx.register_catalog("iceberg", Arc::new(compat_catalog));
        log::info!("Registered backward-compatible 'iceberg' catalog");

        // Create trace service for specialized trace queries
        let trace_service = TraceService::new(session_ctx.as_ref().clone(), "traces".to_string());

        Ok(Self {
            object_store,
            _flight_transport: flight_transport,
            schemas: FlightSchemas::new(),
            session_ctx,
            trace_service,
            iceberg_catalog: Some(iceberg_catalog),
        })
    }

    /// Parse ticket content to determine query type and parameters
    #[allow(clippy::result_large_err)]
    fn parse_ticket(&self, ticket_content: &str) -> Result<TicketRequest, Status> {
        // New format: find_trace:{tenant_slug}:{dataset_slug}:{trace_id}
        if let Some(remainder) = ticket_content.strip_prefix("find_trace:") {
            let parts: Vec<&str> = remainder.splitn(3, ':').collect();
            if parts.len() == 3 {
                log::info!(
                    "Parsing find_trace ticket for tenant_slug={}, dataset_slug={}, trace_id={}",
                    parts[0],
                    parts[1],
                    parts[2]
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
                log::info!(
                    "Parsing search_traces ticket for tenant_slug={}, dataset_slug={}, params={}",
                    parts[0],
                    parts[1],
                    parts[2]
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

    /// Execute a SQL query and return results as RecordBatches
    async fn execute_query(
        &self,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        log::info!("Executing query: {sql}");

        let df = self.session_ctx.sql(sql).await?;
        let batches = df.collect().await?;

        Ok(batches)
    }

    /// Execute a query against the object store
    async fn execute_distributed_query(
        &self,
        query: &str,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        // Query only the object store - data at rest
        // Writers are responsible for persisting data to object store
        // Querier should not depend on or know about writers

        match self.execute_query(query).await {
            Ok(batches) => {
                log::debug!("Retrieved {} batches from object store", batches.len());
                Ok(batches)
            }
            Err(e) => {
                log::error!("Error querying object store: {e}");
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
        let ticket = request.into_inner();
        let ticket_content = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid ticket: {e}")))?;

        log::info!("Processing Flight ticket: {}", ticket_content);

        // Parse ticket to determine request type
        let ticket_request = self.parse_ticket(&ticket_content)?;
        let batches = match ticket_request {
            TicketRequest::FindTrace {
                tenant_slug,
                dataset_slug,
                trace_id,
            } => {
                log::info!(
                    "Executing find_trace for tenant_slug={tenant_slug}, dataset_slug={dataset_slug}, trace_id={trace_id}"
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
                        log::info!("Found trace with {} root spans", trace.spans.len());
                        self.trace_to_record_batches(&trace).await.map_err(|e| {
                            Status::internal(format!("Failed to convert trace to batches: {e}"))
                        })?
                    }
                    Ok(None) => {
                        log::info!("No trace found");
                        vec![]
                    }
                    Err(e) => {
                        log::error!("Error querying trace: {e:?}");
                        return Err(Status::internal(format!("Trace query failed: {e:?}")));
                    }
                }
            }
            TicketRequest::SearchTraces {
                tenant_slug,
                dataset_slug,
                params,
            } => {
                log::info!(
                    "Executing search_traces for tenant_slug={tenant_slug}, dataset_slug={dataset_slug}, params={params:?}"
                );

                match self
                    .trace_service
                    .find_traces_with_tenant(params, &tenant_slug, &dataset_slug)
                    .await
                {
                    Ok(traces) => {
                        log::info!("Found {} traces", traces.len());

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
                        log::error!("Error searching traces: {e:?}");
                        return Err(Status::internal(format!("Trace search failed: {e:?}")));
                    }
                }
            }
            TicketRequest::SqlQuery { sql } => {
                log::info!("Executing SQL query: {sql}");

                self.execute_distributed_query(&sql)
                    .await
                    .map_err(|e| Status::internal(format!("Query execution failed: {e}")))?
            }
        };

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
        let result = service.execute_query("SELECT 1 as test_col").await;
        assert!(result.is_ok());
    }
}
