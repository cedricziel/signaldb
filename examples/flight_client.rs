/// Example Flight client for SignalDB
///
/// This example demonstrates how to connect to SignalDB's Flight service
/// and perform basic queries for traces, logs, and metrics.
///
/// Usage:
/// ```bash
/// # Start SignalDB router with Flight service on port 50053
/// cargo run --bin router
///
/// # Run this example
/// cargo run --example flight_client
/// ```
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Criteria, FlightDescriptor, Ticket};
use std::error::Error;
use tonic::transport::Channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    env_logger::init();

    println!("SignalDB Flight Client Example");
    println!("==============================");

    // Connect to SignalDB Flight service
    let endpoint = "http://127.0.0.1:50053";
    println!("Connecting to SignalDB Flight service at {endpoint}");

    let channel = Channel::from_static("http://127.0.0.1:50053")
        .connect()
        .await?;
    let mut client = FlightServiceClient::new(channel);

    // Example 1: List available flights
    println!("\n1. Listing available flights...");
    let criteria = Criteria {
        expression: vec![].into(),
    };
    let request = tonic::Request::new(criteria);

    match client.list_flights(request).await {
        Ok(response) => {
            let mut stream = response.into_inner();
            println!("Available flights:");
            while let Some(flight) = stream.message().await? {
                if let Some(descriptor) = &flight.flight_descriptor {
                    let cmd = String::from_utf8_lossy(&descriptor.cmd);
                    let description = String::from_utf8_lossy(&flight.app_metadata);
                    println!("  - {cmd} ({description})");
                }
            }
        }
        Err(e) => {
            println!("Error listing flights: {e}");
        }
    }

    // Example 2: Get flight info for traces
    println!("\n2. Getting flight info for 'traces'...");
    let descriptor = FlightDescriptor {
        r#type: 1, // CMD
        cmd: b"traces".to_vec().into(),
        path: vec![],
    };
    let request = tonic::Request::new(descriptor);

    match client.get_flight_info(request).await {
        Ok(response) => {
            let flight_info = response.into_inner();
            println!("Flight info received:");
            println!("  - Schema size: {} bytes", flight_info.schema.len());
            println!("  - Endpoints: {}", flight_info.endpoint.len());
        }
        Err(e) => {
            println!("Error getting flight info: {e}");
        }
    }

    // Example 3: Get schema for traces
    println!("\n3. Getting schema for 'traces'...");
    let descriptor = FlightDescriptor {
        r#type: 1, // CMD
        cmd: b"traces".to_vec().into(),
        path: vec![],
    };
    let request = tonic::Request::new(descriptor);

    match client.get_schema(request).await {
        Ok(response) => {
            let schema_result = response.into_inner();
            println!("Schema received: {} bytes", schema_result.schema.len());
        }
        Err(e) => {
            println!("Error getting schema: {e}");
        }
    }

    // Example 4: Execute a query for all traces
    println!("\n4. Executing query for all traces...");
    let ticket = Ticket {
        ticket: b"traces".to_vec().into(),
    };
    let request = tonic::Request::new(ticket);

    match client.do_get(request).await {
        Ok(response) => {
            let mut stream = response.into_inner();
            let mut record_count = 0;
            println!("Query results:");
            while let Some(flight_data) = stream.message().await? {
                if !flight_data.data_body.is_empty() {
                    record_count += 1;
                    println!(
                        "  - Received data batch {} ({} bytes)",
                        record_count,
                        flight_data.data_body.len()
                    );
                } else if !flight_data.data_header.is_empty() {
                    println!(
                        "  - Received schema ({} bytes)",
                        flight_data.data_header.len()
                    );
                }
            }
            if record_count == 0 {
                println!("  - No data returned (empty result set)");
            }
        }
        Err(e) => {
            println!("Error executing query: {e}");
        }
    }

    // Example 5: Execute a query for specific trace by ID
    println!("\n5. Executing query for trace by ID...");
    let ticket = Ticket {
        ticket: b"trace_by_id?id=12345".to_vec().into(),
    };
    let request = tonic::Request::new(ticket);

    match client.do_get(request).await {
        Ok(response) => {
            let mut stream = response.into_inner();
            let mut record_count = 0;
            println!("Query results:");
            while let Some(flight_data) = stream.message().await? {
                if !flight_data.data_body.is_empty() {
                    record_count += 1;
                    println!(
                        "  - Received data batch {} ({} bytes)",
                        record_count,
                        flight_data.data_body.len()
                    );
                } else if !flight_data.data_header.is_empty() {
                    println!(
                        "  - Received schema ({} bytes)",
                        flight_data.data_header.len()
                    );
                }
            }
            if record_count == 0 {
                println!("  - No data returned (trace not found or empty result)");
            }
        }
        Err(e) => {
            println!("Error executing trace query: {e}");
        }
    }

    // Example 6: Query logs
    println!("\n6. Executing query for logs...");
    let ticket = Ticket {
        ticket: b"logs".to_vec().into(),
    };
    let request = tonic::Request::new(ticket);

    match client.do_get(request).await {
        Ok(response) => {
            let mut stream = response.into_inner();
            let mut record_count = 0;
            println!("Query results:");
            while let Some(flight_data) = stream.message().await? {
                if !flight_data.data_body.is_empty() {
                    record_count += 1;
                    println!(
                        "  - Received data batch {} ({} bytes)",
                        record_count,
                        flight_data.data_body.len()
                    );
                } else if !flight_data.data_header.is_empty() {
                    println!(
                        "  - Received schema ({} bytes)",
                        flight_data.data_header.len()
                    );
                }
            }
            if record_count == 0 {
                println!("  - No data returned (empty result set)");
            }
        }
        Err(e) => {
            println!("Error executing logs query: {e}");
        }
    }

    println!("\nFlight client example completed successfully!");
    Ok(())
}
