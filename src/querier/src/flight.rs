use arrow_flight::flight_service_server::FlightService;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult,
    SchemaResult, Ticket,
};
use bytes::Bytes;
use common::flight::schema::{create_span_batch_schema, FlightSchemas};
use common::flight::transport::InMemoryFlightTransport;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use object_store::ObjectStore;
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// Flight service for query execution against stored data
pub struct QuerierFlightService {
    #[allow(dead_code)]
    object_store: Arc<dyn ObjectStore>,
    _flight_transport: Arc<InMemoryFlightTransport>,
    #[allow(dead_code)]
    schemas: FlightSchemas,
    session_ctx: Arc<SessionContext>,
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

        Self {
            object_store,
            _flight_transport: flight_transport,
            schemas: FlightSchemas::new(),
            session_ctx,
        }
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
        let query = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid query: {e}")))?;

        log::info!("Executing query via Flight: {}", query);

        // Execute distributed query
        let batches = self
            .execute_distributed_query(&query)
            .await
            .map_err(|e| Status::internal(format!("Query execution failed: {e}")))?;

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
