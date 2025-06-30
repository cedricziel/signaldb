use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use bytes::Bytes;
use common::flight::schema::FlightSchemas;
use common::flight::transport::ServiceCapability;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::arrow::{ipc, record_batch::RecordBatch};
use futures::stream::BoxStream;
use futures::{stream, StreamExt};
use std::collections::HashMap;
use tonic::{Request, Response, Status, Streaming};

use crate::RouterState;

/// Query result types for Flight operations
#[derive(Debug)]
enum QueryResult {
    #[allow(dead_code)] // Will be used in full implementation
    Traces(Vec<RecordBatch>, Schema),
    Empty(Schema),
}

/// Parsed query parameters
#[derive(Debug)]
struct ParsedQuery {
    query_type: String,
    parameters: HashMap<String, String>,
}

/// SignalDBFlightService is a Flight service implementation for SignalDB
pub struct SignalDBFlightService<S: RouterState> {
    #[allow(dead_code)]
    state: S,
    #[allow(dead_code)]
    schemas: FlightSchemas,
}

impl<S: RouterState> SignalDBFlightService<S> {
    /// Create a new SignalDBFlightService with the given state
    pub fn new(state: S) -> Self {
        Self {
            state,
            schemas: FlightSchemas::new(),
        }
    }

    /// Create a FlightInfo for a given query type
    #[allow(clippy::result_large_err)] // tonic::Status is inherently large
    fn create_flight_info(
        &self,
        query_type: &str,
        description: &str,
        schema: &Schema,
    ) -> Result<FlightInfo, Status> {
        let options = IpcWriteOptions::default();
        let mut dict_tracker = ipc::writer::DictionaryTracker::new(false);
        let data_gen = ipc::writer::IpcDataGenerator::default();
        let schema_bytes = data_gen
            .schema_to_bytes_with_dictionary_tracker(schema, &mut dict_tracker, &options)
            .ipc_message;

        let flight_descriptor = FlightDescriptor {
            r#type: 1, // CMD
            cmd: query_type.as_bytes().to_vec().into(),
            path: vec![],
        };

        let endpoint = arrow_flight::FlightEndpoint {
            ticket: Some(arrow_flight::Ticket {
                ticket: query_type.as_bytes().to_vec().into(),
            }),
            location: vec![], // Use default location
            expiration_time: None,
            app_metadata: vec![].into(),
        };

        let flight_info = FlightInfo {
            schema: schema_bytes.into(),
            flight_descriptor: Some(flight_descriptor),
            endpoint: vec![endpoint],
            total_records: -1, // Unknown record count
            total_bytes: -1,   // Unknown byte count
            ordered: false,
            app_metadata: description.as_bytes().to_vec().into(),
        };

        Ok(flight_info)
    }

    /// Parse a query string into structured parameters
    #[allow(clippy::result_large_err)] // tonic::Status is inherently large
    fn parse_query(&self, query: &str) -> Result<ParsedQuery, Status> {
        // Simple query parsing - expecting format like "traces" or "trace_by_id?id=12345"
        let parts: Vec<&str> = query.split('?').collect();
        let query_type = parts[0].to_string();

        let mut parameters = HashMap::new();
        if parts.len() > 1 {
            for param in parts[1].split('&') {
                let kv: Vec<&str> = param.split('=').collect();
                if kv.len() == 2 {
                    parameters.insert(kv[0].to_string(), kv[1].to_string());
                }
            }
        }

        Ok(ParsedQuery {
            query_type,
            parameters,
        })
    }

    /// Execute a query and return results
    async fn execute_query(&self, query: &str) -> Result<QueryResult, Status> {
        let parsed = self.parse_query(query)?;

        log::info!("Executing parsed query: {parsed:?}");

        match parsed.query_type.as_str() {
            "traces" => {
                // Route to querier services for trace data
                let querier_services = self
                    .state
                    .service_registry()
                    .get_flight_services_by_capability(ServiceCapability::QueryExecution)
                    .await;

                if querier_services.is_empty() {
                    log::warn!("No querier services available");
                    return Ok(QueryResult::Empty(self.schemas.trace_schema.clone()));
                }

                // For now, return empty results - in a full implementation, we would
                // call the querier services via Flight and aggregate results
                Ok(QueryResult::Empty(self.schemas.trace_schema.clone()))
            }
            "trace_by_id" => {
                if let Some(trace_id) = parsed.parameters.get("id") {
                    log::info!("Querying for trace ID: {trace_id}");
                    // Route to querier services with specific trace ID
                    Ok(QueryResult::Empty(self.schemas.trace_schema.clone()))
                } else {
                    Err(Status::invalid_argument(
                        "trace_by_id query requires 'id' parameter",
                    ))
                }
            }
            "logs" => Ok(QueryResult::Empty(self.schemas.log_schema.clone())),
            "metrics" => Ok(QueryResult::Empty(self.schemas.metric_schema.clone())),
            _ => Err(Status::invalid_argument(format!(
                "Unknown query type: {}",
                parsed.query_type
            ))),
        }
    }

    /// Create a Flight data stream from record batches
    async fn create_flight_data_stream(
        batches: Vec<RecordBatch>,
        schema: Schema,
    ) -> Result<BoxStream<'static, Result<FlightData, Status>>, Status> {
        let options = IpcWriteOptions::default();
        let mut flight_data = Vec::new();
        let data_gen = ipc::writer::IpcDataGenerator::default();

        // First, send the schema
        let mut dict_tracker = ipc::writer::DictionaryTracker::new(false);
        let schema_bytes = data_gen
            .schema_to_bytes_with_dictionary_tracker(&schema, &mut dict_tracker, &options)
            .ipc_message;
        flight_data.push(FlightData {
            data_header: schema_bytes.into(),
            data_body: vec![].into(),
            app_metadata: vec![].into(),
            flight_descriptor: None,
        });

        // Then send each batch
        for batch in batches {
            let mut batch_dict_tracker = ipc::writer::DictionaryTracker::new(false);
            let (_, batch_data) = data_gen
                .encoded_batch(&batch, &mut batch_dict_tracker, &options)
                .map_err(|e| Status::internal(format!("Failed to encode batch: {e}")))?;

            flight_data.push(FlightData {
                data_header: vec![].into(),
                data_body: batch_data.ipc_message.into(),
                app_metadata: vec![].into(),
                flight_descriptor: None,
            });
        }

        let stream = stream::iter(flight_data.into_iter().map(Ok)).boxed();
        Ok(stream)
    }
}

#[tonic::async_trait]
impl<S: RouterState> FlightService for SignalDBFlightService<S> {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        // Simple handshake implementation - no authentication for now
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: Bytes::new(),
        };

        let output = stream::once(async move { Ok(response) }).boxed();
        Ok(Response::new(output))
    }

    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        // Return available query types as flights
        let flights = vec![
            self.create_flight_info("traces", "List all traces", &self.schemas.trace_schema)?,
            self.create_flight_info("trace_by_id", "Get trace by ID", &self.schemas.trace_schema)?,
            self.create_flight_info("logs", "List all logs", &self.schemas.log_schema)?,
            self.create_flight_info("metrics", "List all metrics", &self.schemas.metric_schema)?,
        ];

        let output = stream::iter(flights.into_iter().map(Ok)).boxed();
        Ok(Response::new(output))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();

        let query_type = if !descriptor.cmd.is_empty() {
            String::from_utf8(descriptor.cmd.to_vec())
                .map_err(|e| Status::invalid_argument(format!("Invalid command: {e}")))?
        } else {
            return Err(Status::invalid_argument("No command provided"));
        };

        let (schema, description) = match query_type.as_str() {
            "traces" => (&self.schemas.trace_schema, "List all traces"),
            "trace_by_id" => (&self.schemas.trace_schema, "Get trace by ID"),
            "logs" => (&self.schemas.log_schema, "List all logs"),
            "metrics" => (&self.schemas.metric_schema, "List all metrics"),
            _ => {
                return Err(Status::invalid_argument(format!(
                    "Unknown query type: {query_type}"
                )))
            }
        };

        let flight_info = self.create_flight_info(&query_type, description, schema)?;
        Ok(Response::new(flight_info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        // Not implemented for now
        Err(Status::unimplemented("poll_flight_info is not implemented"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();

        let query_type = if !descriptor.cmd.is_empty() {
            String::from_utf8(descriptor.cmd.to_vec())
                .map_err(|e| Status::invalid_argument(format!("Invalid command: {e}")))?
        } else {
            return Err(Status::invalid_argument("No command provided"));
        };

        let schema = match query_type.as_str() {
            "traces" | "trace_by_id" => &self.schemas.trace_schema,
            "logs" => &self.schemas.log_schema,
            "metrics" => &self.schemas.metric_schema,
            _ => {
                return Err(Status::invalid_argument(format!(
                    "Unknown query type: {query_type}"
                )))
            }
        };

        let options = IpcWriteOptions::default();
        let mut dict_tracker = ipc::writer::DictionaryTracker::new(false);
        let data_gen = ipc::writer::IpcDataGenerator::default();
        let schema_bytes = data_gen
            .schema_to_bytes_with_dictionary_tracker(schema, &mut dict_tracker, &options)
            .ipc_message;

        let schema_result = SchemaResult {
            schema: schema_bytes.into(),
        };
        Ok(Response::new(schema_result))
    }

    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let query = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid ticket: {e}")))?;

        log::info!("Executing Flight query: {query}");

        // Parse the query to determine type and parameters
        let query_result = self.execute_query(&query).await?;

        match query_result {
            QueryResult::Traces(batches, schema) => {
                let stream = Self::create_flight_data_stream(batches, schema).await?;
                Ok(Response::new(stream))
            }
            QueryResult::Empty(schema) => {
                // Return empty stream with schema
                let stream = Self::create_flight_data_stream(vec![], schema).await?;
                Ok(Response::new(stream))
            }
        }
    }

    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        // Not implemented for now
        Err(Status::unimplemented("do_put is not implemented"))
    }

    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        // Not implemented for now
        Err(Status::unimplemented("do_exchange is not implemented"))
    }

    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        // Not implemented for now
        Err(Status::unimplemented("do_action is not implemented"))
    }

    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        // Return an empty list of actions for now
        let output = stream::empty().boxed();
        Ok(Response::new(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryStateImpl;
    use arrow_flight::{Criteria, Ticket};
    use common::catalog::Catalog;
    use tokio;
    use tonic::Request;

    async fn create_test_state() -> InMemoryStateImpl {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        InMemoryStateImpl::new(catalog)
    }

    #[tokio::test]
    async fn test_flight_service_creation() {
        let state = create_test_state().await;
        let service = SignalDBFlightService::new(state);

        // Verify service was created with schemas
        assert!(!service.schemas.trace_schema.fields().is_empty());
        assert!(!service.schemas.log_schema.fields().is_empty());
        assert!(!service.schemas.metric_schema.fields().is_empty());
    }

    #[tokio::test]
    async fn test_list_flights() {
        let state = create_test_state().await;
        let service = SignalDBFlightService::new(state);

        let criteria = Criteria {
            expression: vec![].into(),
        };
        let request = Request::new(criteria);
        let response = service.list_flights(request).await;

        assert!(response.is_ok());
        let mut stream = response.unwrap().into_inner();

        let mut flight_count = 0;
        while let Some(flight_result) = stream.next().await {
            assert!(flight_result.is_ok());
            flight_count += 1;
        }

        // Should have 4 flights: traces, trace_by_id, logs, metrics
        assert_eq!(flight_count, 4);
    }

    #[tokio::test]
    async fn test_do_get_traces() {
        let state = create_test_state().await;
        let service = SignalDBFlightService::new(state);

        let ticket = Ticket {
            ticket: b"traces".to_vec().into(),
        };
        let request = Request::new(ticket);
        let response = service.do_get(request).await;

        assert!(response.is_ok());
        let mut stream = response.unwrap().into_inner();

        // Should get at least a schema message
        let first_message = stream.next().await;
        assert!(first_message.is_some());
        assert!(first_message.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_parse_query() {
        let state = create_test_state().await;
        let service = SignalDBFlightService::new(state);

        // Test simple query
        let parsed = service.parse_query("traces").unwrap();
        assert_eq!(parsed.query_type, "traces");
        assert!(parsed.parameters.is_empty());

        // Test query with parameters
        let parsed = service
            .parse_query("trace_by_id?id=12345&start=2023-01-01")
            .unwrap();
        assert_eq!(parsed.query_type, "trace_by_id");
        assert_eq!(parsed.parameters.get("id").unwrap(), "12345");
        assert_eq!(parsed.parameters.get("start").unwrap(), "2023-01-01");
    }
}
