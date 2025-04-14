use acceptor::{
    handler::otlp_grpc::TraceHandler, services::otlp_trace_service::TraceAcceptorService,
};
use common::config::QueueConfig;
use futures::StreamExt;
use hex;
use messaging::{
    backend::memory::InMemoryStreamingBackend, messages::trace::Trace, Message, MessagingBackend,
};
use opentelemetry_proto::tonic::{
    collector::trace::v1::{trace_service_server::TraceServiceServer, ExportTraceServiceRequest},
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
};
use std::{sync::Arc, time::Duration};
use tempfile::tempdir;
use tokio::{sync::Mutex, time::sleep};
use tonic::transport::Server;
use writer::storage::{LocalStorage, Storage};

#[tokio::test]
async fn test_trace_ingestion_to_storage() {
    // Create temporary directory for storage
    let temp_dir = tempdir().unwrap();
    let storage = Arc::new(LocalStorage::new(temp_dir.path()));

    // Set up memory queue and writer
    let queue = Arc::new(Mutex::new(InMemoryStreamingBackend::new(10)));
    let queue_clone = queue.clone();

    // Set up writer to process queue messages
    let storage_clone = storage.clone();
    tokio::spawn(async move {
        // Create a stream for the traces topic
        let mut stream = queue_clone.lock().await.stream("traces").await;

        // Process messages from the stream
        while let Some(message) = stream.next().await {
            if let Message::Trace(trace) = message {
                storage_clone.store_trace(&trace).await.unwrap();
            }
        }
    });

    // Set up the OTLP gRPC handler
    let handler = TraceAcceptorService::new(TraceHandler::new(queue));

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

    client.export(trace_request).await.unwrap();

    // Allow time for processing
    sleep(Duration::from_millis(1500)).await;

    let stored_traces = storage.list_traces().await.unwrap();
    assert_eq!(stored_traces.len(), 1);

    let stored_trace = storage.get_trace(&stored_traces[0]).await.unwrap();
    assert_eq!(stored_trace.spans.len(), 1);

    let stored_span = &stored_trace.spans[0];
    assert_eq!(stored_span.trace_id, hex::encode(&trace_id));
    assert_eq!(stored_span.span_id, hex::encode(&span_id));
    assert_eq!(stored_span.name, "test-span");
}
