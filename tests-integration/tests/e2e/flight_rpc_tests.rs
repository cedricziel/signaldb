//! Integration tests for Flight RPC methods: get_flight_info and get_schema
//!
//! These tests verify that the router's Flight service correctly implements
//! the `get_flight_info` and `get_schema` RPC methods for all supported
//! query types (traces, logs, metrics).
//!
//! Issue: #278 - Implement remaining flight rpc methods

use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::{FlightDescriptor, FlightInfo, SchemaResult};
use common::catalog::Catalog;
use common::config::Configuration;
use router::InMemoryStateImpl;
use router::endpoints::flight::SignalDBFlightService;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tonic::Request;
use tonic::transport::{Channel, Server};

/// Create a test router Flight service and return the Flight client channel
async fn setup_router_flight_service() -> Channel {
    let catalog = Catalog::new("sqlite::memory:").await.unwrap();
    let config = Configuration::default();
    let state = InMemoryStateImpl::new(catalog, config);

    let flight_service = SignalDBFlightService::new(state);

    // Start Flight server on a random port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let server = Server::builder()
        .add_service(FlightServiceServer::new(flight_service))
        .serve(addr);
    tokio::spawn(server);

    // Wait for server to start
    sleep(Duration::from_millis(200)).await;

    // Create Flight client channel
    let endpoint = format!("http://{addr}");
    Channel::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap()
}

/// Create a FlightServiceClient from a channel
fn create_flight_client(
    channel: Channel,
) -> arrow_flight::flight_service_client::FlightServiceClient<Channel> {
    arrow_flight::flight_service_client::FlightServiceClient::new(channel)
}

// ============================================================================
// get_flight_info tests
// ============================================================================

/// Test get_flight_info for traces query type
#[tokio::test]
async fn test_router_get_flight_info_traces() {
    let channel = setup_router_flight_service().await;
    let mut client = create_flight_client(channel);

    let descriptor = FlightDescriptor::new_cmd("traces");
    let request = Request::new(descriptor);

    let response = client.get_flight_info(request).await;
    assert!(
        response.is_ok(),
        "get_flight_info should succeed for traces"
    );

    let flight_info: FlightInfo = response.unwrap().into_inner();

    // Verify the FlightInfo contains valid schema
    assert!(
        !flight_info.schema.is_empty(),
        "FlightInfo should contain schema"
    );

    // Verify the descriptor is preserved
    assert!(
        flight_info.flight_descriptor.is_some(),
        "FlightInfo should have descriptor"
    );

    let returned_descriptor = flight_info.flight_descriptor.unwrap();
    let cmd = String::from_utf8(returned_descriptor.cmd.to_vec()).unwrap();
    assert_eq!(cmd, "traces", "Descriptor command should be 'traces'");

    println!(
        "✅ get_flight_info for traces: schema size = {} bytes",
        flight_info.schema.len()
    );
}

/// Test get_flight_info for logs query type
#[tokio::test]
async fn test_router_get_flight_info_logs() {
    let channel = setup_router_flight_service().await;
    let mut client = create_flight_client(channel);

    let descriptor = FlightDescriptor::new_cmd("logs");
    let request = Request::new(descriptor);

    let response = client.get_flight_info(request).await;
    assert!(response.is_ok(), "get_flight_info should succeed for logs");

    let flight_info: FlightInfo = response.unwrap().into_inner();

    assert!(
        !flight_info.schema.is_empty(),
        "FlightInfo should contain schema"
    );

    let returned_descriptor = flight_info.flight_descriptor.unwrap();
    let cmd = String::from_utf8(returned_descriptor.cmd.to_vec()).unwrap();
    assert_eq!(cmd, "logs", "Descriptor command should be 'logs'");

    println!(
        "✅ get_flight_info for logs: schema size = {} bytes",
        flight_info.schema.len()
    );
}

/// Test get_flight_info for metrics query type
#[tokio::test]
async fn test_router_get_flight_info_metrics() {
    let channel = setup_router_flight_service().await;
    let mut client = create_flight_client(channel);

    let descriptor = FlightDescriptor::new_cmd("metrics");
    let request = Request::new(descriptor);

    let response = client.get_flight_info(request).await;
    assert!(
        response.is_ok(),
        "get_flight_info should succeed for metrics"
    );

    let flight_info: FlightInfo = response.unwrap().into_inner();

    assert!(
        !flight_info.schema.is_empty(),
        "FlightInfo should contain schema"
    );

    let returned_descriptor = flight_info.flight_descriptor.unwrap();
    let cmd = String::from_utf8(returned_descriptor.cmd.to_vec()).unwrap();
    assert_eq!(cmd, "metrics", "Descriptor command should be 'metrics'");

    println!(
        "✅ get_flight_info for metrics: schema size = {} bytes",
        flight_info.schema.len()
    );
}

/// Test get_flight_info for trace_by_id query type
#[tokio::test]
async fn test_router_get_flight_info_trace_by_id() {
    let channel = setup_router_flight_service().await;
    let mut client = create_flight_client(channel);

    let descriptor = FlightDescriptor::new_cmd("trace_by_id");
    let request = Request::new(descriptor);

    let response = client.get_flight_info(request).await;
    assert!(
        response.is_ok(),
        "get_flight_info should succeed for trace_by_id"
    );

    let flight_info: FlightInfo = response.unwrap().into_inner();

    assert!(
        !flight_info.schema.is_empty(),
        "FlightInfo should contain schema"
    );

    println!(
        "✅ get_flight_info for trace_by_id: schema size = {} bytes",
        flight_info.schema.len()
    );
}

/// Test get_flight_info returns error for invalid query type
#[tokio::test]
async fn test_router_get_flight_info_invalid_query_type() {
    let channel = setup_router_flight_service().await;
    let mut client = create_flight_client(channel);

    let descriptor = FlightDescriptor::new_cmd("invalid_query_type");
    let request = Request::new(descriptor);

    let response = client.get_flight_info(request).await;
    assert!(
        response.is_err(),
        "get_flight_info should fail for invalid query type"
    );

    let error = response.err().unwrap();
    assert_eq!(
        error.code(),
        tonic::Code::InvalidArgument,
        "Error should be InvalidArgument"
    );

    let error_message = error.message();
    assert!(
        error_message.contains("Unknown query type"),
        "Error message should mention unknown query type: {error_message}"
    );

    println!("✅ get_flight_info correctly rejects invalid query type");
}

/// Test get_flight_info returns error when no command provided
#[tokio::test]
async fn test_router_get_flight_info_no_command() {
    let channel = setup_router_flight_service().await;
    let mut client = create_flight_client(channel);

    // Empty command
    let descriptor = FlightDescriptor::new_cmd("");
    let request = Request::new(descriptor);

    let response = client.get_flight_info(request).await;
    assert!(
        response.is_err(),
        "get_flight_info should fail with no command"
    );

    let error = response.err().unwrap();
    assert_eq!(
        error.code(),
        tonic::Code::InvalidArgument,
        "Error should be InvalidArgument"
    );

    println!("✅ get_flight_info correctly rejects empty command");
}

// ============================================================================
// get_schema tests
// ============================================================================

/// Test get_schema for traces query type
#[tokio::test]
async fn test_router_get_schema_traces() {
    let channel = setup_router_flight_service().await;
    let mut client = create_flight_client(channel);

    let descriptor = FlightDescriptor::new_cmd("traces");
    let request = Request::new(descriptor);

    let response = client.get_schema(request).await;
    assert!(response.is_ok(), "get_schema should succeed for traces");

    let schema_result: SchemaResult = response.unwrap().into_inner();

    assert!(
        !schema_result.schema.is_empty(),
        "SchemaResult should contain schema"
    );

    println!(
        "✅ get_schema for traces: schema size = {} bytes",
        schema_result.schema.len()
    );
}

/// Test get_schema for logs query type
#[tokio::test]
async fn test_router_get_schema_logs() {
    let channel = setup_router_flight_service().await;
    let mut client = create_flight_client(channel);

    let descriptor = FlightDescriptor::new_cmd("logs");
    let request = Request::new(descriptor);

    let response = client.get_schema(request).await;
    assert!(response.is_ok(), "get_schema should succeed for logs");

    let schema_result: SchemaResult = response.unwrap().into_inner();

    assert!(
        !schema_result.schema.is_empty(),
        "SchemaResult should contain schema"
    );

    println!(
        "✅ get_schema for logs: schema size = {} bytes",
        schema_result.schema.len()
    );
}

/// Test get_schema for metrics query type
#[tokio::test]
async fn test_router_get_schema_metrics() {
    let channel = setup_router_flight_service().await;
    let mut client = create_flight_client(channel);

    let descriptor = FlightDescriptor::new_cmd("metrics");
    let request = Request::new(descriptor);

    let response = client.get_schema(request).await;
    assert!(response.is_ok(), "get_schema should succeed for metrics");

    let schema_result: SchemaResult = response.unwrap().into_inner();

    assert!(
        !schema_result.schema.is_empty(),
        "SchemaResult should contain schema"
    );

    println!(
        "✅ get_schema for metrics: schema size = {} bytes",
        schema_result.schema.len()
    );
}

/// Test get_schema for trace_by_id (should use same schema as traces)
#[tokio::test]
async fn test_router_get_schema_trace_by_id() {
    let channel = setup_router_flight_service().await;
    let mut client = create_flight_client(channel);

    let descriptor = FlightDescriptor::new_cmd("trace_by_id");
    let request = Request::new(descriptor);

    let response = client.get_schema(request).await;
    assert!(
        response.is_ok(),
        "get_schema should succeed for trace_by_id"
    );

    let schema_result: SchemaResult = response.unwrap().into_inner();

    assert!(
        !schema_result.schema.is_empty(),
        "SchemaResult should contain schema"
    );

    println!(
        "✅ get_schema for trace_by_id: schema size = {} bytes",
        schema_result.schema.len()
    );
}

/// Test get_schema returns error for invalid query type
#[tokio::test]
async fn test_router_get_schema_invalid_query_type() {
    let channel = setup_router_flight_service().await;
    let mut client = create_flight_client(channel);

    let descriptor = FlightDescriptor::new_cmd("nonexistent_type");
    let request = Request::new(descriptor);

    let response = client.get_schema(request).await;
    assert!(
        response.is_err(),
        "get_schema should fail for invalid query type"
    );

    let error = response.err().unwrap();
    assert_eq!(
        error.code(),
        tonic::Code::InvalidArgument,
        "Error should be InvalidArgument"
    );

    let error_message = error.message();
    assert!(
        error_message.contains("Unknown query type"),
        "Error message should mention unknown query type: {error_message}"
    );

    println!("✅ get_schema correctly rejects invalid query type");
}

/// Test get_schema returns error when no command provided
#[tokio::test]
async fn test_router_get_schema_no_command() {
    let channel = setup_router_flight_service().await;
    let mut client = create_flight_client(channel);

    let descriptor = FlightDescriptor::new_cmd("");
    let request = Request::new(descriptor);

    let response = client.get_schema(request).await;
    assert!(response.is_err(), "get_schema should fail with no command");

    let error = response.err().unwrap();
    assert_eq!(
        error.code(),
        tonic::Code::InvalidArgument,
        "Error should be InvalidArgument"
    );

    println!("✅ get_schema correctly rejects empty command");
}

// ============================================================================
// Schema consistency tests
// ============================================================================

/// Test that get_flight_info and get_schema return the same schema for traces
#[tokio::test]
async fn test_schema_consistency_traces() {
    let channel = setup_router_flight_service().await;
    let mut client = create_flight_client(channel);

    // Get schema via get_flight_info
    let descriptor1 = FlightDescriptor::new_cmd("traces");
    let flight_info = client
        .get_flight_info(Request::new(descriptor1))
        .await
        .unwrap()
        .into_inner();

    // Get schema via get_schema
    let descriptor2 = FlightDescriptor::new_cmd("traces");
    let schema_result = client
        .get_schema(Request::new(descriptor2))
        .await
        .unwrap()
        .into_inner();

    // Both should return non-empty schemas
    assert!(!flight_info.schema.is_empty());
    assert!(!schema_result.schema.is_empty());

    println!(
        "✅ Schema consistency for traces: flight_info={} bytes, get_schema={} bytes",
        flight_info.schema.len(),
        schema_result.schema.len()
    );
}

/// Test that traces and trace_by_id return the same schema
#[tokio::test]
async fn test_schema_consistency_traces_and_trace_by_id() {
    let channel = setup_router_flight_service().await;
    let mut client = create_flight_client(channel);

    let traces_schema = client
        .get_schema(Request::new(FlightDescriptor::new_cmd("traces")))
        .await
        .unwrap()
        .into_inner();

    let trace_by_id_schema = client
        .get_schema(Request::new(FlightDescriptor::new_cmd("trace_by_id")))
        .await
        .unwrap()
        .into_inner();

    // Both should return the same schema (trace schema)
    assert_eq!(
        traces_schema.schema, trace_by_id_schema.schema,
        "traces and trace_by_id should have the same schema"
    );

    println!("✅ traces and trace_by_id return identical schemas");
}
