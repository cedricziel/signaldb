use acceptor::{
    handler::otlp_grpc::TraceHandler, services::otlp_trace_service::TraceAcceptorService,
};
use arrow_flight::client::FlightClient;
use opentelemetry_proto::tonic::{
    collector::trace::v1::{trace_service_server::TraceServiceServer, ExportTraceServiceRequest},
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
};
use std::{sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::sleep};
use tonic::{transport::Channel, transport::Server};

#[tokio::test]
async fn test_trace_ingestion_via_flight() {
    // Create a mock Flight client for the acceptor service
    let channel = Channel::from_static("http://localhost:8080")
        .connect()
        .await
        .unwrap();
    let flight_client = Arc::new(Mutex::new(FlightClient::new(channel)));

    // Set up the OTLP gRPC handler with Flight client
    let trace_handler = TraceHandler::new(flight_client);
    let handler = TraceAcceptorService::new(trace_handler);

    // Start the gRPC server
    let addr = "[::1]:4317".parse().unwrap();
    let server = Server::builder()
        .add_service(TraceServiceServer::new(handler))
        .serve(addr);

    tokio::spawn(server);

    // Allow server to start
    sleep(Duration::from_millis(100)).await;

    // Create test trace data
    let trace_id = vec![1; 16];
    let span_id = vec![2; 8];

    let trace_request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: None,
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: trace_id.clone(),
                    span_id: span_id.clone(),
                    parent_span_id: vec![],
                    name: "test-span".to_string(),
                    kind: 1, // Server
                    start_time_unix_nano: 1_000_000_000,
                    end_time_unix_nano: 2_000_000_000,
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: Some(Status {
                        code: 1, // Ok
                        message: "".to_string(),
                    }),
                    trace_state: String::new(),
                    flags: 0,
                }],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }],
    };

    // Send trace data
    let mut client = opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient::connect("http://[::1]:4317")
        .await
        .unwrap();

    let response = client.export(trace_request).await;

    // Verify the request was processed successfully
    assert!(response.is_ok());

    // In a real test, we would verify that the data was forwarded via Flight protocol
    // For now, we just verify the service accepts the request
}
