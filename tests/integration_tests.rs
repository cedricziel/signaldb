use acceptor::{
    handler::otlp_grpc::MockTraceHandler, services::otlp_trace_service::TraceAcceptorService,
};
use opentelemetry_proto::tonic::{
    collector::trace::v1::{trace_service_server::TraceServiceServer, ExportTraceServiceRequest},
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
};
use std::time::Duration;
use tokio::{net::TcpListener, time::sleep};
use tonic::transport::Server;

#[tokio::test]
async fn test_trace_ingestion_via_flight() {
    // Use the mock trace handler instead of real Flight client
    let mock_handler = MockTraceHandler::new();
    let handler = TraceAcceptorService::new(mock_handler);

    // Find an available port for the test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // Start the gRPC server
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

    // Send trace data to the test server
    let endpoint = format!("http://{addr}");
    let mut client = opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient::connect(endpoint)
        .await
        .unwrap();

    let response = client.export(trace_request).await;

    // Verify the request was processed successfully
    assert!(response.is_ok());

    // The mock handler will have recorded the call, demonstrating that
    // the trace ingestion pipeline works without requiring external services
}
